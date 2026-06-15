use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use crc32fast::Hasher;
use serde::{Deserialize, Serialize};

use crate::common::error::{Result, TaibaiError};
use crate::types::scalar::ScalarValue;

const DEFAULT_RECENT_EVENTS: usize = 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChangeOperation {
    Insert,
    Update,
    Delete,
    Truncate,
    CreateTable,
    DropTable,
    AlterTable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangeEvent {
    #[serde(default = "default_change_event_version")]
    pub version: u16,
    pub sequence: u64,
    pub timestamp_micros: i64,
    pub database: String,
    pub table: String,
    pub operation: ChangeOperation,
    pub rows_affected: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rows: Vec<ChangeRow>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub row_images_truncated: bool,
}

fn default_change_event_version() -> u16 {
    1
}

fn is_false(value: &bool) -> bool {
    !*value
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangeRow {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub primary_key: BTreeMap<String, ScalarValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before: Option<BTreeMap<String, ScalarValue>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<BTreeMap<String, ScalarValue>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangePublishRequest {
    pub database: String,
    pub table: String,
    pub operation: ChangeOperation,
    pub rows_affected: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rows: Vec<ChangeRow>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub row_images_truncated: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum ChangeOutboxState {
    Prepared,
    Committed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ChangeOutboxEntry {
    id: u64,
    state: ChangeOutboxState,
    requests: Vec<ChangePublishRequest>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ChangeLogRecord {
    event: ChangeEvent,
    checksum: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangeStreamStatus {
    pub next_sequence: u64,
    pub earliest_sequence: u64,
    pub latest_sequence: u64,
    pub retained_events: usize,
    pub retention_events: usize,
    pub recent_capacity: usize,
    pub consumer_count: usize,
    pub outbox_prepared: usize,
    pub outbox_committed: usize,
    pub persistent: bool,
    pub log_path: Option<PathBuf>,
}

pub struct ChangeStream {
    next_sequence: AtomicU64,
    sender: tokio::sync::broadcast::Sender<ChangeEvent>,
    recent: Mutex<VecDeque<ChangeEvent>>,
    recent_capacity: usize,
    log_path: Option<PathBuf>,
    log_file: Option<Mutex<File>>,
    log_retention_events: usize,
    consumers_path: Option<PathBuf>,
    consumers: Mutex<HashMap<String, u64>>,
    outbox_path: Option<PathBuf>,
    outbox: Mutex<Vec<ChangeOutboxEntry>>,
    next_outbox_id: AtomicU64,
}

impl ChangeStream {
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_RECENT_EVENTS)
    }

    pub fn with_capacity(recent_capacity: usize) -> Self {
        let channel_capacity = recent_capacity.max(16);
        let (sender, _) = tokio::sync::broadcast::channel(channel_capacity);
        Self {
            next_sequence: AtomicU64::new(1),
            sender,
            recent: Mutex::new(VecDeque::with_capacity(recent_capacity)),
            recent_capacity,
            log_path: None,
            log_file: None,
            log_retention_events: 0,
            consumers_path: None,
            consumers: Mutex::new(HashMap::new()),
            outbox_path: None,
            outbox: Mutex::new(Vec::new()),
            next_outbox_id: AtomicU64::new(1),
        }
    }

    pub fn persistent_with_options(
        data_dir: &Path,
        recent_capacity: usize,
        log_retention_events: usize,
    ) -> Result<Self> {
        let meta_dir = data_dir.join("meta");
        std::fs::create_dir_all(&meta_dir)?;
        let log_path = meta_dir.join("change_stream.log");
        let consumers_path = meta_dir.join("change_stream_consumers.json");
        let outbox_path = meta_dir.join("change_stream_outbox.json");
        let consumers = load_consumers(&consumers_path)?;
        let outbox = load_outbox(&outbox_path)?;
        let mut loaded = load_events_with_repair(&log_path, true)?;
        if log_retention_events > 0 && loaded.len() > log_retention_events {
            loaded = loaded
                .into_iter()
                .rev()
                .take(log_retention_events)
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect();
            rewrite_events(&log_path, &loaded)?;
        }
        let next_sequence = loaded.iter().map(|event| event.sequence).max().unwrap_or(0) + 1;
        let channel_capacity = recent_capacity.max(16);
        let (sender, _) = tokio::sync::broadcast::channel(channel_capacity);
        let mut recent = VecDeque::with_capacity(recent_capacity);
        if recent_capacity > 0 {
            for event in loaded
                .into_iter()
                .rev()
                .take(recent_capacity)
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
            {
                recent.push_back(event);
            }
        }
        let log_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)?;

        let stream = Self {
            next_sequence: AtomicU64::new(next_sequence),
            sender,
            recent: Mutex::new(recent),
            recent_capacity,
            log_path: Some(log_path),
            log_file: Some(Mutex::new(log_file)),
            log_retention_events,
            consumers_path: Some(consumers_path),
            consumers: Mutex::new(consumers),
            outbox_path: Some(outbox_path),
            next_outbox_id: AtomicU64::new(
                outbox.iter().map(|entry| entry.id).max().unwrap_or(0) + 1,
            ),
            outbox: Mutex::new(outbox),
        };
        stream.drain_committed_outbox()?;
        Ok(stream)
    }

    pub fn subscribe(&self) -> tokio::sync::broadcast::Receiver<ChangeEvent> {
        self.sender.subscribe()
    }

    #[allow(dead_code)]
    pub fn publish(
        &self,
        database: &str,
        table: &str,
        operation: ChangeOperation,
        rows_affected: u64,
    ) -> ChangeEvent {
        self.publish_with_rows(database, table, operation, rows_affected, Vec::new(), false)
    }

    #[allow(dead_code)]
    pub fn publish_with_rows(
        &self,
        database: &str,
        table: &str,
        operation: ChangeOperation,
        rows_affected: u64,
        rows: Vec<ChangeRow>,
        row_images_truncated: bool,
    ) -> ChangeEvent {
        let request = ChangePublishRequest {
            database: database.to_string(),
            table: table.to_string(),
            operation,
            rows_affected,
            rows,
            row_images_truncated,
        };
        match self.publish_request(&request) {
            Ok(event) => event,
            Err(e) => {
                tracing::warn!(error = %e, "failed to publish change stream event");
                self.build_event(&request)
            }
        }
    }

    pub fn publish_request(&self, request: &ChangePublishRequest) -> Result<ChangeEvent> {
        let event = self.build_event(request);
        self.persist_and_broadcast(event.clone())?;
        Ok(event)
    }

    fn build_event(&self, request: &ChangePublishRequest) -> ChangeEvent {
        let event = ChangeEvent {
            version: 1,
            sequence: self.next_sequence.fetch_add(1, Ordering::AcqRel),
            timestamp_micros: now_micros(),
            database: request.database.clone(),
            table: request.table.clone(),
            operation: request.operation,
            rows_affected: request.rows_affected,
            rows: request.rows.clone(),
            row_images_truncated: request.row_images_truncated,
        };
        event
    }

    fn persist_and_broadcast(&self, event: ChangeEvent) -> Result<()> {
        if let Some(log_file) = &self.log_file {
            let mut file = log_file.lock().map_err(|e| {
                TaibaiError::Internal(format!("failed to lock change stream log: {}", e))
            })?;
            let line = encode_log_record(&event)?;
            writeln!(file, "{}", line)?;
            file.flush()?;
            if self.log_retention_events > 0
                && event.sequence % self.log_retention_events.max(1024) as u64 == 0
            {
                drop(file);
                if let Err(e) = self.compact_log() {
                    tracing::warn!(error = %e, "failed to compact change stream log");
                }
            }
        }

        self.push_recent(event.clone());
        let _ = self.sender.send(event.clone());
        Ok(())
    }

    pub fn prepare_outbox(&self, requests: Vec<ChangePublishRequest>) -> Result<Option<u64>> {
        if requests.is_empty() {
            return Ok(None);
        }
        let Some(_) = &self.outbox_path else {
            for request in requests {
                self.publish_request(&request)?;
            }
            return Ok(None);
        };
        let id = self.next_outbox_id.fetch_add(1, Ordering::AcqRel);
        let mut outbox = self.outbox.lock().unwrap();
        outbox.push(ChangeOutboxEntry {
            id,
            state: ChangeOutboxState::Prepared,
            requests,
        });
        self.save_outbox(&outbox)?;
        Ok(Some(id))
    }

    pub fn commit_outbox(&self, id: u64) -> Result<()> {
        let Some(_) = &self.outbox_path else {
            return Ok(());
        };
        let mut outbox = self.outbox.lock().unwrap();
        let entry = outbox
            .iter_mut()
            .find(|entry| entry.id == id)
            .ok_or_else(|| {
                TaibaiError::Storage(format!("change stream outbox {} not found", id))
            })?;
        entry.state = ChangeOutboxState::Committed;
        self.save_outbox(&outbox)
    }

    pub fn publish_outbox(&self, id: u64) -> Result<usize> {
        self.publish_outbox_entry(Some(id))
    }

    pub fn drain_committed_outbox(&self) -> Result<usize> {
        self.publish_outbox_entry(None)
    }

    fn publish_outbox_entry(&self, id: Option<u64>) -> Result<usize> {
        let entries = {
            let outbox = self.outbox.lock().unwrap();
            outbox
                .iter()
                .filter(|entry| {
                    entry.state == ChangeOutboxState::Committed
                        && id.is_none_or(|target| target == entry.id)
                })
                .cloned()
                .collect::<Vec<_>>()
        };
        let mut published = 0;
        for entry in entries {
            for request in &entry.requests {
                self.publish_request(request)?;
                published += 1;
            }
            let mut outbox = self.outbox.lock().unwrap();
            outbox.retain(|candidate| candidate.id != entry.id);
            self.save_outbox(&outbox)?;
        }
        Ok(published)
    }

    pub fn recent(&self, database: Option<&str>, table: Option<&str>) -> Vec<ChangeEvent> {
        self.recent
            .lock()
            .unwrap()
            .iter()
            .filter(|event| change_event_matches(event, database, table))
            .cloned()
            .collect()
    }

    pub fn since(
        &self,
        sequence: u64,
        database: Option<&str>,
        table: Option<&str>,
    ) -> Result<Vec<ChangeEvent>> {
        let Some(log_path) = &self.log_path else {
            return Ok(self
                .recent(database, table)
                .into_iter()
                .filter(|event| event.sequence >= sequence)
                .collect());
        };
        let events = load_events(log_path)?;
        if let Some(first) = events.first() {
            if sequence < first.sequence {
                return Err(TaibaiError::Sql(format!(
                    "change stream sequence {} is no longer available, earliest available sequence is {}",
                    sequence, first.sequence
                )));
            }
        }
        Ok(events
            .into_iter()
            .filter(|event| {
                event.sequence >= sequence && change_event_matches(event, database, table)
            })
            .collect())
    }

    pub fn events(
        &self,
        from_sequence: Option<u64>,
        limit: Option<usize>,
        database: Option<&str>,
        table: Option<&str>,
    ) -> Result<Vec<ChangeEvent>> {
        let mut events = if let Some(log_path) = &self.log_path {
            load_events(log_path)?
        } else {
            self.recent(database, table)
        };
        events.retain(|event| {
            from_sequence.is_none_or(|sequence| event.sequence >= sequence)
                && change_event_matches(event, database, table)
        });
        if let Some(limit) = limit {
            events.truncate(limit);
        }
        Ok(events)
    }

    pub fn status(&self) -> Result<ChangeStreamStatus> {
        let events = if let Some(log_path) = &self.log_path {
            load_events(log_path)?
        } else {
            self.recent(None, None)
        };
        let earliest_sequence = events.first().map(|event| event.sequence).unwrap_or(0);
        let latest_sequence = events.last().map(|event| event.sequence).unwrap_or(0);
        let consumer_count = self.consumers.lock().unwrap().len();
        let (outbox_prepared, outbox_committed) = {
            let outbox = self.outbox.lock().unwrap();
            (
                outbox
                    .iter()
                    .filter(|entry| entry.state == ChangeOutboxState::Prepared)
                    .count(),
                outbox
                    .iter()
                    .filter(|entry| entry.state == ChangeOutboxState::Committed)
                    .count(),
            )
        };
        Ok(ChangeStreamStatus {
            next_sequence: self.next_sequence.load(Ordering::Acquire),
            earliest_sequence,
            latest_sequence,
            retained_events: events.len(),
            retention_events: self.log_retention_events,
            recent_capacity: self.recent_capacity,
            consumer_count,
            outbox_prepared,
            outbox_committed,
            persistent: self.log_path.is_some(),
            log_path: self.log_path.clone(),
        })
    }

    fn push_recent(&self, event: ChangeEvent) {
        if self.recent_capacity == 0 {
            return;
        }
        let mut recent = self.recent.lock().unwrap();
        if recent.len() == self.recent_capacity {
            recent.pop_front();
        }
        recent.push_back(event);
    }

    pub fn ack_consumer(&self, group: &str, sequence: u64) -> Result<u64> {
        validate_consumer_group(group)?;
        let mut consumers = self.consumers.lock().unwrap();
        let current = consumers.get(group).copied().unwrap_or(0);
        let stored = current.max(sequence);
        consumers.insert(group.to_string(), stored);
        self.save_consumers(&consumers)?;
        Ok(stored)
    }

    pub fn reset_consumer(&self, group: &str, sequence: u64) -> Result<u64> {
        validate_consumer_group(group)?;
        let mut consumers = self.consumers.lock().unwrap();
        consumers.insert(group.to_string(), sequence);
        self.save_consumers(&consumers)?;
        Ok(sequence)
    }

    pub fn drop_consumer(&self, group: &str) -> Result<bool> {
        validate_consumer_group(group)?;
        let mut consumers = self.consumers.lock().unwrap();
        let removed = consumers.remove(group).is_some();
        if removed {
            self.save_consumers(&consumers)?;
        }
        Ok(removed)
    }

    pub fn consumer_sequence(&self, group: &str) -> u64 {
        self.consumers
            .lock()
            .unwrap()
            .get(group)
            .copied()
            .unwrap_or(0)
    }

    pub fn consumers(&self) -> Vec<(String, u64)> {
        let mut consumers = self
            .consumers
            .lock()
            .unwrap()
            .iter()
            .map(|(group, sequence)| (group.clone(), *sequence))
            .collect::<Vec<_>>();
        consumers.sort_by(|(left, _), (right, _)| left.cmp(right));
        consumers
    }

    fn save_consumers(&self, consumers: &HashMap<String, u64>) -> Result<()> {
        let Some(path) = &self.consumers_path else {
            return Ok(());
        };
        let data = serde_json::to_vec_pretty(consumers).map_err(|e| {
            TaibaiError::Internal(format!("failed to encode change stream consumers: {}", e))
        })?;
        write_file_atomically(path, &data)?;
        Ok(())
    }

    fn save_outbox(&self, outbox: &[ChangeOutboxEntry]) -> Result<()> {
        let Some(path) = &self.outbox_path else {
            return Ok(());
        };
        let data = serde_json::to_vec_pretty(outbox)
            .map_err(|e| TaibaiError::Internal(format!("failed to encode change outbox: {}", e)))?;
        write_file_atomically(path, &data)
    }

    fn compact_log(&self) -> Result<()> {
        let Some(log_path) = &self.log_path else {
            return Ok(());
        };
        let mut events = load_events(log_path)?;
        if self.log_retention_events == 0 || events.len() <= self.log_retention_events {
            return Ok(());
        }
        events = events
            .into_iter()
            .rev()
            .take(self.log_retention_events)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect();
        rewrite_events(log_path, &events)
    }
}

impl Default for ChangeStream {
    fn default() -> Self {
        Self::new()
    }
}

fn load_events(path: &Path) -> Result<Vec<ChangeEvent>> {
    load_events_with_repair(path, false)
}

fn load_events_with_repair(path: &Path, repair: bool) -> Result<Vec<ChangeEvent>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut events = Vec::new();
    let mut corrupted = false;
    for (line_idx, line) in reader.lines().enumerate() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        match decode_log_line(&line) {
            Ok(event) => events.push(event),
            Err(e) => {
                corrupted = true;
                tracing::warn!(
                    error = %e,
                    line = line_idx + 1,
                    path = %path.display(),
                    "detected corrupt change stream log entry"
                );
                break;
            }
        }
    }
    if corrupted {
        if repair {
            rewrite_events(path, &events)?;
            tracing::warn!(
                retained_events = events.len(),
                path = %path.display(),
                "repaired change stream log by truncating at first corrupt entry"
            );
        } else {
            return Err(TaibaiError::Storage(format!(
                "change stream log '{}' contains corrupt entries",
                path.display()
            )));
        }
    }
    Ok(events)
}

fn load_consumers(path: &Path) -> Result<HashMap<String, u64>> {
    if !path.exists() {
        return Ok(HashMap::new());
    }
    let data = std::fs::read(path)?;
    serde_json::from_slice(&data).map_err(|e| {
        TaibaiError::Storage(format!("failed to parse change stream consumers: {}", e))
    })
}

fn load_outbox(path: &Path) -> Result<Vec<ChangeOutboxEntry>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let data = std::fs::read(path)?;
    serde_json::from_slice(&data)
        .map_err(|e| TaibaiError::Storage(format!("failed to parse change stream outbox: {}", e)))
}

fn rewrite_events(path: &Path, events: &[ChangeEvent]) -> Result<()> {
    let tmp_path = temp_path_for(path);
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&tmp_path)?;
    for event in events {
        let line = encode_log_record(event)?;
        writeln!(file, "{}", line)?;
    }
    file.flush()?;
    drop(file);
    replace_file(&tmp_path, path)?;
    Ok(())
}

fn encode_log_record(event: &ChangeEvent) -> Result<String> {
    let checksum = checksum_event(event)?;
    let record = ChangeLogRecord {
        event: event.clone(),
        checksum,
    };
    serde_json::to_string(&record)
        .map_err(|e| TaibaiError::Internal(format!("failed to encode change log record: {}", e)))
}

fn decode_log_line(line: &str) -> Result<ChangeEvent> {
    let value: serde_json::Value = serde_json::from_str(line).map_err(|e| {
        TaibaiError::Storage(format!("failed to parse change stream log entry: {}", e))
    })?;

    if value.get("event").is_some() && value.get("checksum").is_some() {
        let record: ChangeLogRecord = serde_json::from_value(value).map_err(|e| {
            TaibaiError::Storage(format!("failed to parse change stream log record: {}", e))
        })?;
        let actual = checksum_event(&record.event)?;
        if actual != record.checksum {
            return Err(TaibaiError::Storage(format!(
                "change stream checksum mismatch for sequence {}: expected {}, got {}",
                record.event.sequence, record.checksum, actual
            )));
        }
        return Ok(record.event);
    }

    serde_json::from_value(value).map_err(|e| {
        TaibaiError::Storage(format!(
            "failed to parse legacy change stream log entry: {}",
            e
        ))
    })
}

fn checksum_event(event: &ChangeEvent) -> Result<u32> {
    let data = serde_json::to_vec(event)
        .map_err(|e| TaibaiError::Internal(format!("failed to encode change event: {}", e)))?;
    let mut hasher = Hasher::new();
    hasher.update(&data);
    Ok(hasher.finalize())
}

fn change_event_matches(event: &ChangeEvent, database: Option<&str>, table: Option<&str>) -> bool {
    database.is_none_or(|db| event.database == db)
        && table.is_none_or(|table_name| event.table == table_name)
}

fn validate_consumer_group(group: &str) -> Result<()> {
    if group.trim().is_empty() {
        return Err(TaibaiError::Sql("consumer group is required".into()));
    }
    Ok(())
}

fn write_file_atomically(path: &Path, data: &[u8]) -> Result<()> {
    let tmp_path = temp_path_for(path);
    {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)?;
        file.write_all(data)?;
        file.flush()?;
    }
    replace_file(&tmp_path, path)
}

fn temp_path_for(path: &Path) -> PathBuf {
    let mut tmp_path = path.to_path_buf();
    let extension = path.extension().and_then(|s| s.to_str()).unwrap_or("");
    let tmp_extension = if extension.is_empty() {
        "tmp".to_string()
    } else {
        format!("{}.tmp", extension)
    };
    tmp_path.set_extension(tmp_extension);
    tmp_path
}

fn replace_file(tmp_path: &Path, path: &Path) -> Result<()> {
    if path.exists() {
        let _ = std::fs::remove_file(path);
    }
    std::fs::rename(tmp_path, path)?;
    Ok(())
}

fn now_micros() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as i64
}
