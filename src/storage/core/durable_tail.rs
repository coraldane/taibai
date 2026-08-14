use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crc32fast::Hasher;
use uuid::Uuid;

use crate::common::error::{Result, TaibaiError};
use crate::sql::execution::wal_helper::{
    compress_prepared_batch_for_wal_with, deserialize_batch_from_wal, prepare_batch_for_wal,
    serialize_batch_for_wal,
};
use crate::storage::segment::SegmentReader;
use crate::types::batch::{RecordBatch, Schema};

const TAIL_MAGIC: u32 = 0x5441_494C;
const TAIL_LOG_MAGIC: u32 = 0x544C_4F47;
/// V2 tail log frames carry the row count in the frame header so recovery can
/// index a tail without decompressing any payload.
const TAIL_LOG_MAGIC_V2: u32 = 0x544C_4F48;
const FLUSH_MAGIC: u32 = 0x464C_5553;
const TAIL_DIR: &str = "memtable_tail";
static TAIL_SEQUENCE: AtomicU64 = AtomicU64::new(0);

const GROUP_COMMIT_MAX_DELAY: Duration = Duration::from_micros(500);
const GROUP_COMMIT_MAX_BYTES: usize = 16 * 1024 * 1024;
const GROUP_COMMIT_MAX_REQUESTS: usize = 128;

/// A WAL frame prepared by the request thread. Serialization and zstd level 3
/// compression therefore happen in parallel and outside the table mutation
/// lock; the tail writer only performs sequential file I/O and durability.
pub(crate) struct PreparedTailAppend {
    wal_payload: Vec<u8>,
    frame: Option<Vec<u8>>,
    schema: Schema,
    rows: u64,
}

impl PreparedTailAppend {
    pub(crate) fn new(batch: &RecordBatch) -> Self {
        Self {
            wal_payload: prepare_batch_for_wal(batch),
            frame: None,
            schema: batch.schema.clone(),
            rows: batch.num_rows() as u64,
        }
    }

    fn encoded_len(&self) -> usize {
        self.frame
            .as_ref()
            .map(Vec::len)
            .unwrap_or(self.wal_payload.len())
    }

    fn finish(&mut self, compressor: &mut zstd::bulk::Compressor<'_>) {
        if self.frame.is_none() {
            let raw = std::mem::take(&mut self.wal_payload);
            let payload = compress_prepared_batch_for_wal_with(compressor, raw);
            self.frame = Some(encode_tail_frame_payload(&payload, self.rows));
        }
    }

    fn frame(&self) -> &[u8] {
        self.frame
            .as_deref()
            .expect("tail append must be compressed by commit thread")
    }
}

type CommitOutcome = std::result::Result<(), String>;

struct CommitCompletion {
    outcome: Mutex<Option<CommitOutcome>>,
    ready: Condvar,
}

impl CommitCompletion {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            outcome: Mutex::new(None),
            ready: Condvar::new(),
        })
    }

    fn finish(&self, outcome: CommitOutcome) {
        let mut guard = self.outcome.lock().unwrap();
        if guard.is_none() {
            *guard = Some(outcome);
            self.ready.notify_all();
        }
    }

    fn wait(&self) -> Result<()> {
        let mut guard = self.outcome.lock().unwrap();
        while guard.is_none() {
            guard = self.ready.wait(guard).unwrap();
        }
        match guard.as_ref().unwrap() {
            Ok(()) => Ok(()),
            Err(message) => Err(TaibaiError::Storage(message.clone())),
        }
    }
}

pub(crate) struct TailCommitTicket {
    completion: Arc<CommitCompletion>,
}

impl TailCommitTicket {
    pub(crate) fn wait(self) -> Result<()> {
        self.completion.wait()
    }
}

struct AppendCommand {
    table_dir: PathBuf,
    id: Uuid,
    partition_key: String,
    prepared: PreparedTailAppend,
    completion: Arc<CommitCompletion>,
}

enum CommitCommand {
    Append(AppendCommand),
    Barrier {
        table_dir: PathBuf,
        completion: Arc<CommitCompletion>,
    },
    Shutdown {
        completion: Arc<CommitCompletion>,
    },
}

/// Single-owner tail WAL writer. Requests keep their normal SQL semantics: a
/// ticket is released only after the file containing that request has been
/// synced. The small collection window amortizes one storage barrier over all
/// concurrently arriving INSERT statements.
///
/// In async-fsync mode the ticket is released once the frame bytes reach the
/// page cache; the fsync still runs for every group inside this thread, just
/// off the acknowledgement path. A crash between the ack and the background
/// fsync can lose acknowledged rows — that is the documented trade selected by
/// `storage.tail_async_fsync`.
pub(crate) struct TailCommitter {
    sender: crossbeam_channel::Sender<CommitCommand>,
    handle: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl TailCommitter {
    pub(crate) fn start(async_fsync: bool) -> Arc<Self> {
        let (sender, receiver) = crossbeam_channel::unbounded();
        let handle = std::thread::Builder::new()
            .name("taibai-tail-commit".into())
            .spawn(move || tail_commit_loop(receiver, async_fsync))
            .expect("failed to spawn durable tail commit thread");
        Arc::new(Self {
            sender,
            handle: Mutex::new(Some(handle)),
        })
    }

    pub(crate) fn submit(
        &self,
        table_dir: PathBuf,
        id: Uuid,
        partition_key: String,
        prepared: PreparedTailAppend,
    ) -> TailCommitTicket {
        let completion = CommitCompletion::new();
        if self
            .sender
            .send(CommitCommand::Append(AppendCommand {
                table_dir,
                id,
                partition_key,
                prepared,
                completion: Arc::clone(&completion),
            }))
            .is_err()
        {
            completion.finish(Err("durable tail commit thread stopped".into()));
        }
        TailCommitTicket { completion }
    }

    /// Wait for every previously submitted append for this table, then close
    /// its cached handles so flush/rename/delete operations are safe on Windows.
    pub(crate) fn barrier(&self, table_dir: &Path) -> Result<()> {
        let completion = CommitCompletion::new();
        if self
            .sender
            .send(CommitCommand::Barrier {
                table_dir: table_dir.to_path_buf(),
                completion: Arc::clone(&completion),
            })
            .is_err()
        {
            return Err(TaibaiError::Storage(
                "durable tail commit thread stopped".into(),
            ));
        }
        completion.wait()
    }

    pub(crate) fn stop(&self) {
        let completion = CommitCompletion::new();
        if self
            .sender
            .send(CommitCommand::Shutdown {
                completion: Arc::clone(&completion),
            })
            .is_ok()
        {
            let _ = completion.wait();
        }
        if let Some(handle) = self.handle.lock().unwrap().take() {
            let _ = handle.join();
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct TailKey {
    table_dir: PathBuf,
    id: Uuid,
}

struct OpenTail {
    file: fs::File,
    path: PathBuf,
    partition_key: String,
    pending_rename: Option<PathBuf>,
}

#[derive(Default)]
struct TailFileSet {
    open: HashMap<TailKey, OpenTail>,
    poisoned: HashSet<TailKey>,
}

impl TailFileSet {
    fn append(&mut self, command: &AppendCommand) -> Result<TailKey> {
        let key = TailKey {
            table_dir: command.table_dir.clone(),
            id: command.id,
        };
        if self.poisoned.contains(&key) {
            return Err(TaibaiError::Storage(
                "durable tail is unavailable after an earlier write failure".into(),
            ));
        }

        if !self.open.contains_key(&key) {
            match open_tail_for_group_append(command)? {
                OpenTailResult::Ready(tail) => {
                    self.open.insert(key.clone(), tail);
                }
                OpenTailResult::FrameAlreadyDurable(tail) => {
                    self.open.insert(key.clone(), tail);
                    return Ok(key);
                }
            }
        }

        let tail = self.open.get_mut(&key).unwrap();
        if tail.partition_key != command.partition_key {
            return Err(TaibaiError::Storage(
                "durable tail partition key changed while appending".into(),
            ));
        }
        if let Err(error) = tail.file.write_all(command.prepared.frame()) {
            self.poisoned.insert(key.clone());
            return Err(error.into());
        }
        Ok(key)
    }

    fn sync(&mut self, key: &TailKey) -> Result<()> {
        let Some(tail) = self.open.get_mut(key) else {
            return Err(TaibaiError::Storage(
                "durable tail handle disappeared before sync".into(),
            ));
        };
        if let Err(error) = tail.file.sync_all() {
            self.poisoned.insert(key.clone());
            return Err(error.into());
        }
        if let Some(final_path) = tail.pending_rename.take() {
            if let Err(error) = fs::rename(&tail.path, &final_path) {
                self.poisoned.insert(key.clone());
                return Err(error.into());
            }
            tail.path = final_path;
        }
        Ok(())
    }

    fn evict_table(&mut self, table_dir: &Path) {
        self.open.retain(|key, _| key.table_dir != table_dir);
        self.poisoned.retain(|key| key.table_dir != table_dir);
    }
}

enum OpenTailResult {
    Ready(OpenTail),
    FrameAlreadyDurable(OpenTail),
}

fn open_tail_for_group_append(command: &AppendCommand) -> Result<OpenTailResult> {
    let tail_dir = command.table_dir.join(TAIL_DIR);
    fs::create_dir_all(&tail_dir)?;
    if let Some(path) = newest_live_tail_path(&tail_dir, command.id)? {
        let is_v2 = read_tail_log_version(&path)? == Some(TAIL_LOG_MAGIC_V2);
        if is_v2 && read_tail_log_partition_key(&path)?.as_deref() == Some(&command.partition_key) {
            let file = fs::OpenOptions::new().append(true).open(&path)?;
            return Ok(OpenTailResult::Ready(OpenTail {
                file,
                path,
                partition_key: command.partition_key.clone(),
                pending_rename: None,
            }));
        }

        // Legacy snapshot and v1 log tails are migrated to the v2 log format
        // synchronously once. Keeping this compatibility path means every live
        // tail becomes row-indexable without a decode.
        let bytes = fs::read(&path)?;
        let (old_partition_key, old_batch) = decode_tail(&bytes, &command.prepared.schema)?;
        if old_partition_key != command.partition_key {
            return Err(TaibaiError::Storage(
                "durable tail partition key changed while appending".into(),
            ));
        }
        let sequence = next_sequence();
        let stem = format!("{}.{}", command.id.simple(), sequence);
        let tmp_path = tail_dir.join(format!(".{stem}.tmp"));
        let final_path = tail_dir.join(format!("{stem}.tail"));
        let mut payload = encode_tail_log(&command.partition_key, &old_batch);
        payload.extend_from_slice(command.prepared.frame());
        write_new_synced(&tmp_path, &payload)?;
        fs::rename(&tmp_path, &final_path)?;
        remove_other_generations(&tail_dir, command.id, &final_path)?;
        let file = fs::OpenOptions::new().append(true).open(&final_path)?;
        return Ok(OpenTailResult::FrameAlreadyDurable(OpenTail {
            file,
            path: final_path,
            partition_key: command.partition_key.clone(),
            pending_rename: None,
        }));
    }

    let sequence = next_sequence();
    let stem = format!("{}.{}", command.id.simple(), sequence);
    let tmp_path = tail_dir.join(format!(".{stem}.tmp"));
    let final_path = tail_dir.join(format!("{stem}.tail"));
    let mut file = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .append(true)
        .open(&tmp_path)?;
    file.write_all(&encode_tail_log_header(&command.partition_key))?;
    Ok(OpenTailResult::Ready(OpenTail {
        file,
        path: tmp_path,
        partition_key: command.partition_key.clone(),
        pending_rename: Some(final_path),
    }))
}

fn tail_commit_loop(receiver: crossbeam_channel::Receiver<CommitCommand>, async_fsync: bool) {
    let mut files = TailFileSet::default();
    let mut compressor = zstd::bulk::Compressor::new(3)
        .expect("zstd level 3 compressor must initialize for durable tail writer");
    let mut deferred = None;
    loop {
        let command = match deferred.take() {
            Some(command) => command,
            None => match receiver.recv() {
                Ok(command) => command,
                Err(_) => break,
            },
        };
        match command {
            CommitCommand::Append(first) => {
                let mut bytes = first.prepared.encoded_len();
                let mut group = vec![first];
                let deadline = Instant::now() + GROUP_COMMIT_MAX_DELAY;
                while group.len() < GROUP_COMMIT_MAX_REQUESTS && bytes < GROUP_COMMIT_MAX_BYTES {
                    let now = Instant::now();
                    if now >= deadline {
                        break;
                    }
                    match receiver.recv_timeout(deadline.saturating_duration_since(now)) {
                        Ok(CommitCommand::Append(next)) => {
                            bytes = bytes.saturating_add(next.prepared.encoded_len());
                            group.push(next);
                        }
                        Ok(other) => {
                            deferred = Some(other);
                            break;
                        }
                        Err(crossbeam_channel::RecvTimeoutError::Timeout) => break,
                        Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
                    }
                }
                commit_append_group(&mut files, &mut compressor, group, async_fsync);
            }
            CommitCommand::Barrier {
                table_dir,
                completion,
            } => {
                files.evict_table(&table_dir);
                completion.finish(Ok(()));
            }
            CommitCommand::Shutdown { completion } => {
                files.open.clear();
                completion.finish(Ok(()));
                break;
            }
        }
    }
}

fn commit_append_group(
    files: &mut TailFileSet,
    compressor: &mut zstd::bulk::Compressor<'_>,
    group: Vec<AppendCommand>,
    async_fsync: bool,
) {
    if crate::common::config::insert_profile_enabled() {
        let bytes = group
            .iter()
            .map(|command| command.prepared.encoded_len())
            .sum::<usize>();
        eprintln!(
            "[insert-profile] stage=tail_group requests={} bytes={}",
            group.len(),
            bytes
        );
    }
    let mut by_tail = HashMap::<TailKey, Vec<Arc<CommitCompletion>>>::new();
    for mut command in group {
        command.prepared.finish(compressor);
        match files.append(&command) {
            Ok(key) => by_tail.entry(key).or_default().push(command.completion),
            Err(error) => command.completion.finish(Err(error.to_string())),
        }
    }
    for (key, completions) in by_tail {
        if async_fsync {
            // Frames are in the page cache: release the acknowledgements now
            // and keep the barrier off the INSERT latency path. Ordering with
            // later commands (barriers, flush intents) is preserved because
            // this thread processes the channel serially.
            for completion in &completions {
                completion.finish(Ok(()));
            }
            if let Err(error) = files.sync(&key) {
                tracing::error!(
                    tail = ?key,
                    error = %error,
                    "background durable tail fsync failed after acknowledgement"
                );
            }
        } else {
            let outcome = files.sync(&key).map_err(|error| error.to_string());
            for completion in completions {
                completion.finish(outcome.clone());
            }
        }
    }
}

#[allow(dead_code)] // still used by crash-recovery tests
pub struct RecoveredTail {
    pub id: Uuid,
    pub partition_key: String,
    pub batch: RecordBatch,
}

/// Metadata-only view of a recovered tail: the rows stay on disk until a
/// query, DML, or flush materializes them.
pub struct RecoveredTailIndex {
    pub id: Uuid,
    pub partition_key: String,
    pub rows: u64,
    pub path: PathBuf,
}

pub struct FlushIntent {
    id: Uuid,
    marker_path: PathBuf,
}

#[derive(Clone)]
enum GenerationKind {
    Tail,
    Clear,
    Flush,
}

#[derive(Clone)]
struct Generation {
    sequence: u64,
    path: PathBuf,
    kind: GenerationKind,
}

pub fn persist(table_dir: &Path, id: Uuid, partition_key: &str, batch: &RecordBatch) -> Result<()> {
    let tail_dir = table_dir.join(TAIL_DIR);
    fs::create_dir_all(&tail_dir)?;
    let sequence = next_sequence();
    let payload = encode_tail_log(partition_key, batch);
    let stem = format!("{}.{}", id.simple(), sequence);
    let tmp_path = tail_dir.join(format!(".{stem}.tmp"));
    let final_path = tail_dir.join(format!("{stem}.tail"));

    write_new_synced(&tmp_path, &payload)?;
    fs::rename(&tmp_path, &final_path)?;
    remove_other_generations(&tail_dir, id, &final_path)?;
    Ok(())
}

/// Durably append only the newly inserted rows to an existing memtable tail.
///
/// The previous implementation rewrote the complete accumulated memtable for
/// every INSERT. Repeated single-row INSERTs therefore produced quadratic I/O
/// and one fsync/rename/delete cycle per ever-growing snapshot. A tail log keeps
/// the same acknowledgement durability while writing only the statement delta.
pub fn append(table_dir: &Path, id: Uuid, partition_key: &str, delta: &RecordBatch) -> Result<()> {
    if delta.num_rows() == 0 {
        return Ok(());
    }
    let tail_dir = table_dir.join(TAIL_DIR);
    fs::create_dir_all(&tail_dir)?;
    let existing = newest_live_tail_path(&tail_dir, id)?;
    let frame = encode_tail_frame(delta);

    if let Some(path) = existing {
        if read_tail_log_partition_key(&path)?.as_deref() == Some(partition_key) {
            let mut file = fs::OpenOptions::new().append(true).open(&path)?;
            file.write_all(&frame)?;
            file.sync_all()?;
            return Ok(());
        }

        // One-time migration for tails written by versions that stored a full
        // snapshot. Keep the old generation until the new log is durable.
        let bytes = fs::read(&path)?;
        let (old_partition_key, old_batch) = decode_tail(&bytes, &delta.schema)?;
        if old_partition_key != partition_key {
            return Err(TaibaiError::Storage(
                "durable tail partition key changed while appending".into(),
            ));
        }
        let sequence = next_sequence();
        let stem = format!("{}.{}", id.simple(), sequence);
        let tmp_path = tail_dir.join(format!(".{stem}.tmp"));
        let final_path = tail_dir.join(format!("{stem}.tail"));
        let mut payload = encode_tail_log(partition_key, &old_batch);
        payload.extend_from_slice(&frame);
        write_new_synced(&tmp_path, &payload)?;
        fs::rename(&tmp_path, &final_path)?;
        remove_other_generations(&tail_dir, id, &final_path)?;
        return Ok(());
    }

    let sequence = next_sequence();
    let stem = format!("{}.{}", id.simple(), sequence);
    let tmp_path = tail_dir.join(format!(".{stem}.tmp"));
    let final_path = tail_dir.join(format!("{stem}.tail"));
    write_new_synced(&tmp_path, &encode_tail_log(partition_key, delta))?;
    fs::rename(&tmp_path, &final_path)?;
    Ok(())
}

/// Persist a tombstone before removing a tail. Keeping the tombstone makes a
/// failed cleanup fail closed: an older tail cannot be resurrected on restart.
pub fn clear(table_dir: &Path, id: Uuid) -> Result<()> {
    let tail_dir = table_dir.join(TAIL_DIR);
    if !tail_dir.exists() {
        return Ok(());
    }
    let sequence = next_sequence();
    let marker = tail_dir.join(format!("{}.{}.clear", id.simple(), sequence));
    write_new_synced(&marker, &TAIL_MAGIC.to_le_bytes())?;
    remove_other_generations(&tail_dir, id, &marker)?;
    Ok(())
}

pub fn has_tail(table_dir: &Path, id: Uuid) -> bool {
    let Ok(mut generations) = generations_for_id(&table_dir.join(TAIL_DIR), id) else {
        return false;
    };
    generations.sort_by_key(|generation| std::cmp::Reverse(generation.sequence));
    for generation in generations {
        match generation.kind {
            GenerationKind::Tail => return true,
            GenerationKind::Clear => return false,
            GenerationKind::Flush => {
                let Ok(bytes) = fs::read(&generation.path) else {
                    continue;
                };
                if flush_intent_satisfied(table_dir, &bytes).unwrap_or(false) {
                    return false;
                }
            }
        }
    }
    false
}

pub fn generation_file_count(table_dir: &Path) -> Result<usize> {
    let tail_dir = table_dir.join(TAIL_DIR);
    if !tail_dir.exists() {
        return Ok(0);
    }
    let mut count = 0usize;
    for entry in fs::read_dir(tail_dir)? {
        if parse_generation(&entry?.path()).is_some() {
            count += 1;
        }
    }
    Ok(count)
}

/// Batch form of `has_tail`: a single directory scan answers for every
/// requested id, applying the same generation arbitration (newest tail wins,
/// clear tombstones, flush intents). With thousands of live tails, per-id
/// scans made flush grouping quadratic in the size of the tail directory.
pub fn has_tail_batch(table_dir: &Path, ids: &[Uuid]) -> Result<HashMap<Uuid, bool>> {
    let tail_dir = table_dir.join(TAIL_DIR);
    let mut by_id = generations_by_id(&tail_dir)?;
    let mut answers = HashMap::with_capacity(ids.len());
    for id in ids {
        let Some(mut generations) = by_id.remove(id) else {
            answers.insert(*id, false);
            continue;
        };
        generations.sort_by_key(|generation| std::cmp::Reverse(generation.sequence));
        let mut has = false;
        for generation in generations {
            match generation.kind {
                GenerationKind::Tail => {
                    has = true;
                    break;
                }
                GenerationKind::Clear => break,
                GenerationKind::Flush => {
                    let Ok(bytes) = fs::read(&generation.path) else {
                        continue;
                    };
                    if flush_intent_satisfied(table_dir, &bytes).unwrap_or(false) {
                        break;
                    }
                }
            }
        }
        answers.insert(*id, has);
    }
    Ok(answers)
}

/// Batch form of `live_tail_path`: one directory scan resolves the newest
/// live tail file of every requested id with the same arbitration rules.
pub fn live_tail_paths_batch(
    table_dir: &Path,
    ids: &[Uuid],
) -> Result<HashMap<Uuid, Option<PathBuf>>> {
    let tail_dir = table_dir.join(TAIL_DIR);
    let mut by_id = generations_by_id(&tail_dir)?;
    let parent_dir = tail_dir.parent().unwrap_or(&tail_dir);
    let mut answers = HashMap::with_capacity(ids.len());
    for id in ids {
        let Some(mut generations) = by_id.remove(id) else {
            answers.insert(*id, None);
            continue;
        };
        generations.sort_by_key(|generation| std::cmp::Reverse(generation.sequence));
        let mut live = None;
        for generation in generations {
            match generation.kind {
                GenerationKind::Tail => {
                    live = Some(generation.path);
                    break;
                }
                GenerationKind::Clear => break,
                GenerationKind::Flush => {
                    let Ok(bytes) = fs::read(&generation.path) else {
                        continue;
                    };
                    if flush_intent_satisfied(parent_dir, &bytes).unwrap_or(false) {
                        break;
                    }
                }
            }
        }
        answers.insert(*id, live);
    }
    Ok(answers)
}

/// Record the destination before a durable tail is copied into a segment.
/// Recovery can then distinguish "rows durable in the segment" from "flush
/// never completed", avoiding duplicate rows. `expected_rows` is the row
/// count the segment footer must reach for the intent to be satisfied; this
/// allows the target to be a shared active segment that also holds rows from
/// other flushes.
pub fn begin_flush(
    table_dir: &Path,
    id: Uuid,
    segment_file_name: &str,
    expected_rows: u64,
) -> Result<FlushIntent> {
    if Path::new(segment_file_name)
        .file_name()
        .and_then(|name| name.to_str())
        != Some(segment_file_name)
    {
        return Err(TaibaiError::Storage(
            "durable tail flush target must be a file name".into(),
        ));
    }
    let tail_dir = table_dir.join(TAIL_DIR);
    fs::create_dir_all(&tail_dir)?;
    let marker_path = tail_dir.join(format!("{}.{}.flush", id.simple(), next_sequence()));
    write_new_synced(
        &marker_path,
        &encode_flush_target(segment_file_name, expected_rows),
    )?;
    Ok(FlushIntent { id, marker_path })
}

/// Finish a flush pass by cleaning up every intent with a single directory
/// scan. Per-intent scans made group cleanup quadratic in the number of live
/// tail files. Ordering per intent is preserved exactly: source tails are
/// removed first and intent markers last, so an interrupted batch still fails
/// closed during recovery (the surviving marker points at the durable segment).
pub fn finish_flush_batch(table_dir: &Path, intents: Vec<FlushIntent>) -> Result<()> {
    if intents.is_empty() {
        return Ok(());
    }
    let tail_dir = table_dir.join(TAIL_DIR);
    if !tail_dir.exists() {
        return Ok(());
    }
    let keep: HashSet<PathBuf> = intents
        .iter()
        .map(|intent| intent.marker_path.clone())
        .collect();
    let ids: HashSet<Uuid> = intents.iter().map(|intent| intent.id).collect();
    for entry in fs::read_dir(&tail_dir)? {
        let path = entry?.path();
        let Some((id, _)) = parse_generation(&path) else {
            continue;
        };
        if ids.contains(&id) && !keep.contains(&path) {
            remove_file_if_exists(&path)?;
        }
    }
    for intent in intents {
        remove_file_if_exists(&intent.marker_path)?;
    }
    remove_empty_dir(&tail_dir);
    Ok(())
}

pub fn cancel_flush(intent: FlushIntent) -> Result<()> {
    remove_file_if_exists(&intent.marker_path)
}

/// Apply generation arbitration (newest tail wins, clear tombstones, flush
/// intents) and return the live tail file for every recoverable memtable id.
/// Shared by `recover` (eager decode) and `recover_index` (metadata only);
/// performs the same on-disk cleanup as before.
fn resolve_live_tail_paths(table_dir: &Path) -> Result<Vec<(Uuid, PathBuf)>> {
    let tail_dir = table_dir.join(TAIL_DIR);
    if !tail_dir.exists() {
        return Ok(Vec::new());
    }
    let mut by_id = HashMap::<Uuid, Vec<Generation>>::new();
    for entry in fs::read_dir(&tail_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) == Some("tmp") {
            let _ = fs::remove_file(path);
            continue;
        }
        let Some((id, generation)) = parse_generation(&path) else {
            continue;
        };
        by_id.entry(id).or_default().push(generation);
    }

    let mut resolved = Vec::new();
    for (id, mut generations) in by_id {
        generations.sort_by_key(|generation| std::cmp::Reverse(generation.sequence));
        let mut live_path = None;
        for generation in &generations {
            match generation.kind {
                GenerationKind::Tail => {
                    live_path = Some(generation.path.clone());
                    break;
                }
                GenerationKind::Clear => break,
                GenerationKind::Flush => {
                    if flush_intent_satisfied(table_dir, &fs::read(&generation.path)?)? {
                        remove_other_generations(&tail_dir, id, &generation.path)?;
                        remove_file_if_exists(&generation.path)?;
                        break;
                    }
                    // The rows never became durable in the target segment.
                    // Ignore this intent and continue to the preceding tail.
                    let _ = remove_file_if_exists(&generation.path);
                }
            }
        }
        if let Some(path) = live_path {
            remove_other_generations(&tail_dir, id, &path)?;
            resolved.push((id, path));
        }
    }
    remove_empty_dir(&tail_dir);
    Ok(resolved)
}

#[allow(dead_code)] // still used by crash-recovery tests
pub fn recover(table_dir: &Path, schema: &Schema) -> Result<Vec<RecoveredTail>> {
    let mut recovered = Vec::new();
    for (id, path) in resolve_live_tail_paths(table_dir)? {
        let (partition_key, batch) = decode_tail_file(&path, schema)?;
        recovered.push(RecoveredTail {
            id,
            partition_key,
            batch,
        });
    }
    Ok(recovered)
}

/// Lazy counterpart of `recover`: v2 tails are indexed from frame headers
/// without decoding; legacy tails are decoded once only to count rows, and
/// their payloads remain on disk either way.
pub fn recover_index(table_dir: &Path, schema: &Schema) -> Result<Vec<RecoveredTailIndex>> {
    let mut recovered = Vec::new();
    for (id, path) in resolve_live_tail_paths(table_dir)? {
        let bytes = fs::read(&path)?;
        if let Some(rows) = scan_tail_log_rows(&bytes) {
            let partition_key = read_tail_log_partition_key(&path)?.ok_or_else(|| {
                TaibaiError::Storage("v2 durable tail is missing its partition key".into())
            })?;
            recovered.push(RecoveredTailIndex {
                id,
                partition_key,
                rows,
                path,
            });
            continue;
        }
        let (partition_key, batch) = decode_tail(&bytes, schema)?;
        recovered.push(RecoveredTailIndex {
            id,
            partition_key,
            rows: batch.num_rows() as u64,
            path,
        });
    }
    Ok(recovered)
}

/// Newest live tail file for a memtable, applying the same clear/flush-intent
/// arbitration as crash recovery. Used by the spill path.
#[allow(dead_code)] // still used by spill/recovery tests
pub fn live_tail_path(table_dir: &Path, id: Uuid) -> Result<Option<PathBuf>> {
    newest_live_tail_path(&table_dir.join(TAIL_DIR), id)
}

/// Row count of a v2 tail log without decompressing any frame. Returns `None`
/// for legacy formats, which are not eligible for spilling.
pub fn tail_log_rows(path: &Path) -> Result<Option<u64>> {
    Ok(scan_tail_log_rows(&fs::read(path)?))
}

pub(crate) fn decode_tail_file(path: &Path, schema: &Schema) -> Result<(String, RecordBatch)> {
    decode_tail(&fs::read(path)?, schema)
}

fn write_new_synced(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

fn encode_tail_log(partition_key: &str, batch: &RecordBatch) -> Vec<u8> {
    let mut bytes = encode_tail_log_header(partition_key);
    bytes.extend_from_slice(&encode_tail_frame(batch));
    bytes
}

fn encode_tail_log_header(partition_key: &str) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(8 + partition_key.len());
    bytes.extend_from_slice(&TAIL_LOG_MAGIC_V2.to_le_bytes());
    bytes.extend_from_slice(&(partition_key.len() as u32).to_le_bytes());
    bytes.extend_from_slice(partition_key.as_bytes());
    bytes
}

fn encode_tail_frame(batch: &RecordBatch) -> Vec<u8> {
    let payload = serialize_batch_for_wal(batch);
    encode_tail_frame_payload(&payload, batch.num_rows() as u64)
}

/// V2 frame layout: `[u64 payload_len][u64 rows][payload][crc32]`, with the
/// CRC covering both header fields and the payload. V1 frames lacked the
/// `rows` field; both versions remain decodable.
fn encode_tail_frame_payload(payload: &[u8], rows: u64) -> Vec<u8> {
    let mut frame = Vec::with_capacity(20 + payload.len());
    frame.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    frame.extend_from_slice(&rows.to_le_bytes());
    frame.extend_from_slice(payload);
    let mut hasher = Hasher::new();
    hasher.update(&frame);
    frame.extend_from_slice(&hasher.finalize().to_le_bytes());
    frame
}

fn encode_flush_target(segment_file_name: &str, expected_rows: u64) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(20 + segment_file_name.len());
    bytes.extend_from_slice(&FLUSH_MAGIC.to_le_bytes());
    bytes.extend_from_slice(&(segment_file_name.len() as u32).to_le_bytes());
    bytes.extend_from_slice(segment_file_name.as_bytes());
    bytes.extend_from_slice(&expected_rows.to_le_bytes());
    append_crc(bytes)
}

fn append_crc(mut bytes: Vec<u8>) -> Vec<u8> {
    let mut hasher = Hasher::new();
    hasher.update(&bytes);
    bytes.extend_from_slice(&hasher.finalize().to_le_bytes());
    bytes
}

fn verify_crc(bytes: &[u8]) -> Result<&[u8]> {
    if bytes.len() < 4 {
        return Err(TaibaiError::Storage(
            "durable tail record is truncated".into(),
        ));
    }
    let body_end = bytes.len() - 4;
    let stored_crc = u32::from_le_bytes(bytes[body_end..].try_into().unwrap());
    let mut hasher = Hasher::new();
    hasher.update(&bytes[..body_end]);
    if stored_crc != hasher.finalize() {
        return Err(TaibaiError::Storage(
            "durable tail checksum mismatch".into(),
        ));
    }
    Ok(&bytes[..body_end])
}

fn decode_tail(bytes: &[u8], schema: &Schema) -> Result<(String, RecordBatch)> {
    if bytes.len() >= 4 {
        let magic = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
        if magic == TAIL_LOG_MAGIC || magic == TAIL_LOG_MAGIC_V2 {
            return decode_tail_log(bytes, schema);
        }
    }
    let body = verify_crc(bytes)?;
    if body.len() < 16 {
        return Err(TaibaiError::Storage("durable tail is truncated".into()));
    }
    let magic = u32::from_le_bytes(body[0..4].try_into().unwrap());
    if magic != TAIL_MAGIC {
        return Err(TaibaiError::Storage("invalid durable tail magic".into()));
    }
    let partition_len = u32::from_le_bytes(body[4..8].try_into().unwrap()) as usize;
    let partition_end = 8usize.saturating_add(partition_len);
    if partition_end.saturating_add(8) > body.len() {
        return Err(TaibaiError::Storage("invalid durable tail header".into()));
    }
    let partition_key = std::str::from_utf8(&body[8..partition_end])
        .map_err(|_| TaibaiError::Storage("invalid durable tail partition key".into()))?
        .to_string();
    let batch_len =
        u64::from_le_bytes(body[partition_end..partition_end + 8].try_into().unwrap()) as usize;
    let batch_start = partition_end + 8;
    let batch_end = batch_start.saturating_add(batch_len);
    if batch_end != body.len() {
        return Err(TaibaiError::Storage(
            "invalid durable tail payload length".into(),
        ));
    }
    let batch = deserialize_batch_from_wal(&body[batch_start..batch_end], schema)?;
    Ok((partition_key, batch))
}

fn decode_tail_log(bytes: &[u8], schema: &Schema) -> Result<(String, RecordBatch)> {
    if bytes.len() < 8 {
        return Err(TaibaiError::Storage("durable tail log is truncated".into()));
    }
    let v2 = match u32::from_le_bytes(bytes[0..4].try_into().unwrap()) {
        TAIL_LOG_MAGIC => false,
        TAIL_LOG_MAGIC_V2 => true,
        _ => {
            return Err(TaibaiError::Storage(
                "invalid durable tail log magic".into(),
            ));
        }
    };
    let frame_header_len = if v2 { 16usize } else { 8usize };
    let partition_len = u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize;
    let mut offset = 8usize.saturating_add(partition_len);
    if offset > bytes.len() {
        return Err(TaibaiError::Storage(
            "invalid durable tail log header".into(),
        ));
    }
    let partition_key = std::str::from_utf8(&bytes[8..offset])
        .map_err(|_| TaibaiError::Storage("invalid durable tail partition key".into()))?
        .to_string();
    let mut recovered = RecordBatch::new(schema.clone());
    let mut complete_frames = 0usize;
    while offset < bytes.len() {
        // A crash may leave the final append incomplete. All preceding frames
        // were individually synced and remain a valid recovery prefix.
        if bytes.len().saturating_sub(offset) < frame_header_len + 4 {
            break;
        }
        let payload_len =
            u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap()) as usize;
        let payload_start = offset + frame_header_len;
        let payload_end = payload_start.saturating_add(payload_len);
        let frame_end = payload_end.saturating_add(4);
        if frame_end > bytes.len() {
            break;
        }
        let mut hasher = Hasher::new();
        hasher.update(&bytes[offset..payload_end]);
        let stored_crc = u32::from_le_bytes(bytes[payload_end..frame_end].try_into().unwrap());
        if stored_crc != hasher.finalize() {
            return Err(TaibaiError::Storage(
                "durable tail log frame checksum mismatch".into(),
            ));
        }
        let batch = deserialize_batch_from_wal(&bytes[payload_start..payload_end], schema)?;
        append_batch(&mut recovered, &batch)?;
        complete_frames += 1;
        offset = frame_end;
    }
    if complete_frames == 0 {
        return Err(TaibaiError::Storage(
            "durable tail log contains no complete frame".into(),
        ));
    }
    Ok((partition_key, recovered))
}

fn append_batch(target: &mut RecordBatch, batch: &RecordBatch) -> Result<()> {
    for col_idx in 0..batch.num_columns() {
        let source = batch
            .column(col_idx)
            .ok_or_else(|| TaibaiError::Storage("durable tail delta is missing a column".into()))?;
        let destination = target
            .column_mut(col_idx)
            .ok_or_else(|| TaibaiError::Storage("durable tail schema column mismatch".into()))?;
        destination.extend(source)?;
    }
    Ok(())
}

fn read_tail_log_version(path: &Path) -> Result<Option<u32>> {
    let mut file = fs::File::open(path)?;
    let mut header = [0u8; 4];
    if let Err(error) = file.read_exact(&mut header) {
        if error.kind() == std::io::ErrorKind::UnexpectedEof {
            return Ok(None);
        }
        return Err(error.into());
    }
    let magic = u32::from_le_bytes(header);
    Ok((magic == TAIL_LOG_MAGIC || magic == TAIL_LOG_MAGIC_V2).then_some(magic))
}

/// Row count of a v2 tail log obtained by walking frame headers only — no
/// payload is decompressed. Returns `None` for legacy (v1 log / snapshot)
/// tails or on checksum mismatch, so callers can fall back to decoding.
fn scan_tail_log_rows(bytes: &[u8]) -> Option<u64> {
    if bytes.len() < 8 {
        return None;
    }
    if u32::from_le_bytes(bytes[0..4].try_into().unwrap()) != TAIL_LOG_MAGIC_V2 {
        return None;
    }
    let partition_len = u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize;
    let mut offset = 8usize.saturating_add(partition_len);
    if offset > bytes.len() {
        return None;
    }
    let mut rows = 0u64;
    while offset < bytes.len() {
        // Mirror decode_tail_log: a crash-truncated final frame ends the
        // valid prefix without failing the scan.
        if bytes.len().saturating_sub(offset) < 20 {
            break;
        }
        let payload_len =
            u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap()) as usize;
        let frame_rows = u64::from_le_bytes(bytes[offset + 8..offset + 16].try_into().unwrap());
        let payload_start = offset + 16;
        let payload_end = payload_start.saturating_add(payload_len);
        let frame_end = payload_end.saturating_add(4);
        if frame_end > bytes.len() {
            break;
        }
        let mut hasher = Hasher::new();
        hasher.update(&bytes[offset..payload_end]);
        let stored_crc = u32::from_le_bytes(bytes[payload_end..frame_end].try_into().unwrap());
        if stored_crc != hasher.finalize() {
            return None;
        }
        rows = rows.saturating_add(frame_rows);
        offset = frame_end;
    }
    Some(rows)
}

fn read_tail_log_partition_key(path: &Path) -> Result<Option<String>> {
    let mut file = fs::File::open(path)?;
    let mut header = [0u8; 8];
    if let Err(error) = file.read_exact(&mut header) {
        if error.kind() == std::io::ErrorKind::UnexpectedEof {
            return Ok(None);
        }
        return Err(error.into());
    }
    let magic = u32::from_le_bytes(header[0..4].try_into().unwrap());
    if magic != TAIL_LOG_MAGIC && magic != TAIL_LOG_MAGIC_V2 {
        return Ok(None);
    }
    let partition_len = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;
    let mut partition = vec![0u8; partition_len];
    file.read_exact(&mut partition)?;
    String::from_utf8(partition)
        .map(Some)
        .map_err(|_| TaibaiError::Storage("invalid durable tail partition key".into()))
}

fn decode_flush_target(bytes: &[u8]) -> Result<(String, Option<u64>)> {
    let body = verify_crc(bytes)?;
    if body.len() < 8 {
        return Err(TaibaiError::Storage("flush intent is truncated".into()));
    }
    if u32::from_le_bytes(body[0..4].try_into().unwrap()) != FLUSH_MAGIC {
        return Err(TaibaiError::Storage("invalid flush intent magic".into()));
    }
    let name_len = u32::from_le_bytes(body[4..8].try_into().unwrap()) as usize;
    let name_end = 8usize.saturating_add(name_len);
    let expected_rows = if name_end == body.len() {
        None
    } else if name_end.saturating_add(8) == body.len() {
        Some(u64::from_le_bytes(
            body[name_end..name_end + 8].try_into().unwrap(),
        ))
    } else {
        return Err(TaibaiError::Storage("invalid flush intent length".into()));
    };
    let name = std::str::from_utf8(&body[8..name_end])
        .map_err(|_| TaibaiError::Storage("invalid flush intent target".into()))?
        .to_string();
    if Path::new(&name)
        .file_name()
        .and_then(|value| value.to_str())
        != Some(name.as_str())
    {
        return Err(TaibaiError::Storage("unsafe flush intent target".into()));
    }
    Ok((name, expected_rows))
}

/// A recorded flush intent proves its tail is durable once the named segment
/// is readable and, for intents carrying a row-count watermark, holds at
/// least that many rows. Legacy intents without a watermark keep the old
/// existence check; their target was a standalone segment written atomically.
fn flush_intent_satisfied(table_dir: &Path, bytes: &[u8]) -> Result<bool> {
    let (segment_name, expected_rows) = decode_flush_target(bytes)?;
    let segment_path = table_dir.join(segment_name);
    if !segment_path.is_file() {
        return Ok(false);
    }
    let reader = match SegmentReader::open(&segment_path) {
        Ok(reader) => reader,
        Err(_) => return Ok(false),
    };
    Ok(match expected_rows {
        Some(expected) => reader.row_count() >= expected,
        None => true,
    })
}

fn next_sequence() -> u64 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(u64::MAX as u128) as u64;
    let counter = TAIL_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    nanos.saturating_add(counter & 0xFFFF)
}

fn parse_generation(path: &Path) -> Option<(Uuid, Generation)> {
    let name = path.file_name()?.to_str()?;
    let mut parts = name.split('.');
    let id = Uuid::parse_str(parts.next()?).ok()?;
    let sequence = parts.next()?.parse::<u64>().ok()?;
    let kind = match parts.next()? {
        "tail" => GenerationKind::Tail,
        "clear" => GenerationKind::Clear,
        "flush" => GenerationKind::Flush,
        _ => return None,
    };
    if parts.next().is_some() {
        return None;
    }
    Some((
        id,
        Generation {
            sequence,
            path: path.to_path_buf(),
            kind,
        },
    ))
}

fn generations_for_id(tail_dir: &Path, id: Uuid) -> Result<Vec<Generation>> {
    if !tail_dir.exists() {
        return Ok(Vec::new());
    }
    let mut generations = Vec::new();
    for entry in fs::read_dir(tail_dir)? {
        let path = entry?.path();
        if let Some((entry_id, generation)) = parse_generation(&path) {
            if entry_id == id {
                generations.push(generation);
            }
        }
    }
    Ok(generations)
}

/// Index the whole tail directory in one `read_dir` pass, grouped by memtable
/// id. Shared by the batched flush/spill helpers so that answering for N
/// memtables costs one scan instead of N.
fn generations_by_id(tail_dir: &Path) -> Result<HashMap<Uuid, Vec<Generation>>> {
    let mut by_id = HashMap::new();
    if !tail_dir.exists() {
        return Ok(by_id);
    }
    for entry in fs::read_dir(tail_dir)? {
        let path = entry?.path();
        if let Some((id, generation)) = parse_generation(&path) {
            by_id.entry(id).or_default().push(generation);
        }
    }
    Ok(by_id)
}

fn newest_live_tail_path(tail_dir: &Path, id: Uuid) -> Result<Option<PathBuf>> {
    let mut generations = generations_for_id(tail_dir, id)?;
    generations.sort_by_key(|generation| std::cmp::Reverse(generation.sequence));
    for generation in generations {
        match generation.kind {
            GenerationKind::Tail => return Ok(Some(generation.path)),
            GenerationKind::Clear => return Ok(None),
            GenerationKind::Flush => {
                let Ok(bytes) = fs::read(&generation.path) else {
                    continue;
                };
                let table_dir = tail_dir.parent().unwrap_or(tail_dir);
                if flush_intent_satisfied(table_dir, &bytes).unwrap_or(false) {
                    return Ok(None);
                }
            }
        }
    }
    Ok(None)
}

fn remove_other_generations(tail_dir: &Path, id: Uuid, keep_path: &Path) -> Result<()> {
    for generation in generations_for_id(tail_dir, id)? {
        if generation.path != keep_path {
            remove_file_if_exists(&generation.path)?;
        }
    }
    Ok(())
}

fn remove_file_if_exists(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn remove_empty_dir(path: &Path) {
    if fs::read_dir(path)
        .ok()
        .is_some_and(|mut entries| entries.next().is_none())
    {
        let _ = fs::remove_dir(path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::types::DataType;
    use crate::storage::segment::constants::DEFAULT_ROW_GROUP_SIZE;
    use crate::storage::segment::SegmentWriter;
    use crate::types::batch::ColumnDef;
    use crate::types::scalar::ScalarValue;

    fn test_batch(schema: &Schema, values: &[i64]) -> Result<RecordBatch> {
        let mut batch = RecordBatch::new(schema.clone());
        for value in values {
            batch
                .column_mut(0)
                .unwrap()
                .push(&ScalarValue::Int64(Some(*value)))?;
        }
        Ok(batch)
    }

    #[test]
    fn latest_tail_generation_round_trips_and_clear_tombstones_it() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
        let id = Uuid::new_v4();
        persist(temp.path(), id, "2026", &test_batch(&schema, &[1])?)?;
        persist(temp.path(), id, "2026", &test_batch(&schema, &[1, 2])?)?;
        let recovered = recover(temp.path(), &schema)?;
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].id, id);
        assert_eq!(recovered[0].partition_key, "2026");
        assert_eq!(recovered[0].batch.num_rows(), 2);

        clear(temp.path(), id)?;
        assert!(recover(temp.path(), &schema)?.is_empty());
        assert!(!has_tail(temp.path(), id));
        Ok(())
    }

    #[test]
    fn appended_tail_deltas_recover_as_one_batch() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
        let id = Uuid::new_v4();
        append(temp.path(), id, "2026", &test_batch(&schema, &[1])?)?;
        append(temp.path(), id, "2026", &test_batch(&schema, &[2, 3])?)?;

        let recovered = recover(temp.path(), &schema)?;
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].partition_key, "2026");
        assert_eq!(recovered[0].batch.num_rows(), 3);
        assert_eq!(
            recovered[0].batch.scalar(2, 0),
            Some(ScalarValue::Int64(Some(3)))
        );
        Ok(())
    }

    #[test]
    fn group_committer_acknowledges_only_recoverable_appends() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
        let id = Uuid::new_v4();
        let first = test_batch(&schema, &[1, 2])?;
        let second = test_batch(&schema, &[3, 4])?;
        let committer = TailCommitter::start(false);
        let first_ticket = committer.submit(
            temp.path().to_path_buf(),
            id,
            "2026".into(),
            PreparedTailAppend::new(&first),
        );
        let second_ticket = committer.submit(
            temp.path().to_path_buf(),
            id,
            "2026".into(),
            PreparedTailAppend::new(&second),
        );
        first_ticket.wait()?;
        second_ticket.wait()?;
        committer.barrier(temp.path())?;

        let recovered = recover(temp.path(), &schema)?;
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].batch.num_rows(), 4);
        assert_eq!(
            recovered[0].batch.scalar(3, 0),
            Some(ScalarValue::Int64(Some(4)))
        );
        committer.stop();
        Ok(())
    }

    #[test]
    fn async_fsync_committer_still_leaves_recoverable_tail() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
        let id = Uuid::new_v4();
        let batch = test_batch(&schema, &[7, 8, 9])?;
        let committer = TailCommitter::start(true);
        let ticket = committer.submit(
            temp.path().to_path_buf(),
            id,
            "2026".into(),
            PreparedTailAppend::new(&batch),
        );
        ticket.wait()?;
        committer.barrier(temp.path())?;

        let recovered = recover(temp.path(), &schema)?;
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].batch.num_rows(), 3);
        assert_eq!(
            recovered[0].batch.scalar(2, 0),
            Some(ScalarValue::Int64(Some(9)))
        );
        committer.stop();
        Ok(())
    }

    #[test]
    fn appending_migrates_legacy_snapshot_tail() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
        let id = Uuid::new_v4();
        let partition_key = "legacy";
        let legacy_batch = test_batch(&schema, &[1])?;
        let batch_payload = serialize_batch_for_wal(&legacy_batch);
        let mut legacy = Vec::new();
        legacy.extend_from_slice(&TAIL_MAGIC.to_le_bytes());
        legacy.extend_from_slice(&(partition_key.len() as u32).to_le_bytes());
        legacy.extend_from_slice(partition_key.as_bytes());
        legacy.extend_from_slice(&(batch_payload.len() as u64).to_le_bytes());
        legacy.extend_from_slice(&batch_payload);
        let legacy = append_crc(legacy);
        let tail_dir = temp.path().join(TAIL_DIR);
        fs::create_dir_all(&tail_dir)?;
        fs::write(tail_dir.join(format!("{}.1.tail", id.simple())), legacy)?;

        append(temp.path(), id, partition_key, &test_batch(&schema, &[2])?)?;
        let recovered = recover(temp.path(), &schema)?;
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].batch.num_rows(), 2);
        assert_eq!(
            recovered[0].batch.scalar(1, 0),
            Some(ScalarValue::Int64(Some(2)))
        );
        Ok(())
    }

    #[test]
    fn recovery_ignores_incomplete_final_tail_frame() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
        let id = Uuid::new_v4();
        append(temp.path(), id, "", &test_batch(&schema, &[1])?)?;
        let tail_path =
            newest_live_tail_path(&temp.path().join(TAIL_DIR), id)?.expect("tail should exist");
        let mut file = fs::OpenOptions::new().append(true).open(tail_path)?;
        file.write_all(&[0x08, 0x00, 0x00, 0x00])?;
        drop(file);

        let recovered = recover(temp.path(), &schema)?;
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].batch.num_rows(), 1);
        Ok(())
    }

    #[test]
    fn incomplete_flush_recovers_tail_and_completed_flush_suppresses_it() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
        let batch = test_batch(&schema, &[1, 2])?;
        let id = Uuid::new_v4();
        persist(temp.path(), id, "", &batch)?;

        let incomplete = begin_flush(temp.path(), id, "seg_00001.dat", 2)?;
        assert_eq!(recover(temp.path(), &schema)?.len(), 1);
        let _ = cancel_flush(incomplete);

        let _completed = begin_flush(temp.path(), id, "seg_00001.dat", 2)?;
        let mut writer = SegmentWriter::create(
            &temp.path().join("seg_00001.dat"),
            schema.clone(),
            DEFAULT_ROW_GROUP_SIZE,
        )?;
        writer.write_batch(&batch)?;
        writer.seal()?;
        assert!(recover(temp.path(), &schema)?.is_empty());
        assert!(!has_tail(temp.path(), id));
        Ok(())
    }

    #[test]
    fn flush_intent_watermark_requires_segment_row_count() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
        let batch = test_batch(&schema, &[1, 2])?;
        let id = Uuid::new_v4();
        persist(temp.path(), id, "", &batch)?;

        let _intent = begin_flush(temp.path(), id, "seg_00001.dat", 6)?;
        let mut writer = SegmentWriter::create(
            &temp.path().join("seg_00001.dat"),
            schema.clone(),
            DEFAULT_ROW_GROUP_SIZE,
        )?;
        writer.write_batch(&batch)?;
        writer.seal()?;

        let recovered = recover(temp.path(), &schema)?;
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].batch.num_rows(), 2);
        assert!(has_tail(temp.path(), id));
        Ok(())
    }

    #[test]
    fn flush_intent_watermark_satisfied_by_shared_segment_growth() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
        let first = test_batch(&schema, &[1, 2])?;
        let second = test_batch(&schema, &[3, 4])?;
        let id = Uuid::new_v4();
        persist(temp.path(), id, "", &second)?;

        let _intent = begin_flush(temp.path(), id, "seg_00001.dat", 4)?;
        let mut writer = SegmentWriter::create(
            &temp.path().join("seg_00001.dat"),
            schema.clone(),
            DEFAULT_ROW_GROUP_SIZE,
        )?;
        writer.write_batch(&first)?;
        writer.write_batch(&second)?;
        writer.seal()?;

        assert!(recover(temp.path(), &schema)?.is_empty());
        assert!(!has_tail(temp.path(), id));
        Ok(())
    }

    #[test]
    fn batched_tail_queries_match_single_id_semantics() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
        let live = Uuid::new_v4();
        let cleared = Uuid::new_v4();
        let flushed = Uuid::new_v4();
        let unknown = Uuid::new_v4();
        persist(temp.path(), live, "2026", &test_batch(&schema, &[1])?)?;
        persist(temp.path(), cleared, "2026", &test_batch(&schema, &[2])?)?;
        clear(temp.path(), cleared)?;
        let flushed_batch = test_batch(&schema, &[3, 4])?;
        persist(temp.path(), flushed, "", &flushed_batch)?;
        let _intent = begin_flush(temp.path(), flushed, "seg_00001.dat", 2)?;
        let mut writer = SegmentWriter::create(
            &temp.path().join("seg_00001.dat"),
            schema.clone(),
            DEFAULT_ROW_GROUP_SIZE,
        )?;
        writer.write_batch(&flushed_batch)?;
        writer.seal()?;

        let answers = has_tail_batch(temp.path(), &[live, cleared, flushed, unknown])?;
        assert!(answers[&live]);
        assert!(!answers[&cleared]);
        assert!(!answers[&flushed]);
        assert!(!answers[&unknown]);

        let paths = live_tail_paths_batch(temp.path(), &[live, cleared, unknown])?;
        assert!(paths[&live].is_some());
        assert!(paths[&cleared].is_none());
        assert!(paths[&unknown].is_none());
        Ok(())
    }

    #[test]
    fn finish_flush_batch_removes_tails_and_markers_in_one_pass() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
        let ids = [Uuid::new_v4(), Uuid::new_v4()];
        for id in ids {
            persist(temp.path(), id, "", &test_batch(&schema, &[1])?)?;
        }
        let intents = ids
            .iter()
            .map(|id| begin_flush(temp.path(), *id, "seg_00001.dat", 1))
            .collect::<Result<Vec<_>>>()?;

        finish_flush_batch(temp.path(), intents)?;

        for id in ids {
            assert!(!has_tail(temp.path(), id));
        }
        assert!(!temp.path().join(TAIL_DIR).exists());
        Ok(())
    }

    #[test]
    fn v2_tail_log_reports_rows_without_decode() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
        let id = Uuid::new_v4();
        append(temp.path(), id, "2026", &test_batch(&schema, &[1])?)?;
        append(temp.path(), id, "2026", &test_batch(&schema, &[2, 3])?)?;

        let path = live_tail_path(temp.path(), id)?.expect("tail should exist");
        assert_eq!(tail_log_rows(&path)?, Some(3));

        // A crash-truncated final frame still yields the valid prefix count.
        let mut bytes = fs::read(&path)?;
        bytes.extend_from_slice(&[0x08, 0x00, 0x00, 0x00]);
        assert_eq!(scan_tail_log_rows(&bytes), Some(3));
        Ok(())
    }

    #[test]
    fn recover_index_counts_rows_and_keeps_tail_on_disk() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
        let id = Uuid::new_v4();
        append(temp.path(), id, "2026", &test_batch(&schema, &[1, 2])?)?;
        append(temp.path(), id, "2026", &test_batch(&schema, &[3])?)?;

        let index = recover_index(temp.path(), &schema)?;
        assert_eq!(index.len(), 1);
        assert_eq!(index[0].id, id);
        assert_eq!(index[0].partition_key, "2026");
        assert_eq!(index[0].rows, 3);
        assert!(index[0].path.is_file(), "indexing must keep the tail file");

        let (partition_key, batch) = decode_tail_file(&index[0].path, &schema)?;
        assert_eq!(partition_key, "2026");
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.scalar(2, 0), Some(ScalarValue::Int64(Some(3))));

        // Arbitration cleanup is idempotent across repeated index passes.
        assert_eq!(recover_index(temp.path(), &schema)?.len(), 1);
        Ok(())
    }

    #[test]
    fn recover_index_counts_legacy_snapshot_tail_rows() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
        let id = Uuid::new_v4();
        let partition_key = "legacy";
        let legacy_batch = test_batch(&schema, &[7, 8])?;
        let batch_payload = serialize_batch_for_wal(&legacy_batch);
        let mut legacy = Vec::new();
        legacy.extend_from_slice(&TAIL_MAGIC.to_le_bytes());
        legacy.extend_from_slice(&(partition_key.len() as u32).to_le_bytes());
        legacy.extend_from_slice(partition_key.as_bytes());
        legacy.extend_from_slice(&(batch_payload.len() as u64).to_le_bytes());
        legacy.extend_from_slice(&batch_payload);
        let legacy = append_crc(legacy);
        let tail_dir = temp.path().join(TAIL_DIR);
        fs::create_dir_all(&tail_dir)?;
        fs::write(tail_dir.join(format!("{}.1.tail", id.simple())), legacy)?;

        let index = recover_index(temp.path(), &schema)?;
        assert_eq!(index.len(), 1);
        assert_eq!(index[0].partition_key, "legacy");
        assert_eq!(index[0].rows, 2);
        assert!(index[0].path.is_file());

        let (_, batch) = decode_tail_file(&index[0].path, &schema)?;
        assert_eq!(batch.num_rows(), 2);
        Ok(())
    }
}
