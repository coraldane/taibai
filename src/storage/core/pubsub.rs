use std::collections::{HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crossbeam_channel::{bounded, RecvTimeoutError, TrySendError};
use tokio::sync::mpsc::Sender as FrameSender;
use tokio::sync::Notify;
use uuid::Uuid;

use crate::common::config::PubSubSettings;
use crate::common::error::{Result, TaibaiError};
use crate::types::batch::RecordBatch;

const PUBLISH_RETRY: Duration = Duration::from_millis(2);
const PUBLISHER_TICK: Duration = Duration::from_millis(200);
const BOOKMARK_FLUSH_INTERVAL: Duration = Duration::from_millis(500);

#[derive(Clone)]
pub(crate) struct CommitNotice {
    pub db: Arc<str>,
    pub table: Arc<str>,
    pub batch: Arc<RecordBatch>,
}

pub struct LogEntry {
    pub seq: u64,
    pub batch: Arc<RecordBatch>,
    approx_bytes: u64,
}

pub enum FrameKind {
    Data {
        seq_start: u64,
        seq_end: u64,
        batches: Vec<Arc<RecordBatch>>,
    },
    ResyncRequired {
        floor: u64,
    },
}

pub struct PublishedFrame {
    pub db: String,
    pub table: String,
    pub sub_id: u64,
    pub kind: FrameKind,
}

#[derive(Clone, Debug, PartialEq)]
pub enum SubscribeMode {
    FromNow,
    FromSeq(u64),
}

pub struct SubscribeRequest {
    pub db: String,
    pub table: String,
    pub mode: SubscribeMode,
    pub filter_column: Option<String>,
    pub filter_values: Option<HashSet<String>>,
    pub named: Option<String>,
    pub session_id: Uuid,
    pub frame_tx: FrameSender<PublishedFrame>,
}

#[derive(Debug)]
pub struct SubscribeGrant {
    pub sub_id: u64,
    pub epoch: u64,
    pub granted_from: u64,
    pub floor: u64,
}

#[derive(Debug)]
pub enum DrainResult {
    Data {
        seq_start: u64,
        seq_end: u64,
        batches: Vec<Arc<RecordBatch>>,
    },
    Resync {
        floor: u64,
    },
    Empty,
}

pub struct SubscriberState {
    pub id: u64,
    pub session_id: Uuid,
    pub db: String,
    pub table: String,
    next_delivered: AtomicU64,
    pub last_acked: AtomicU64,
    pub last_sent: AtomicU64,
    pub lagging: AtomicBool,
    pub needs_resync: AtomicBool,
    closed: AtomicBool,
    pub filter_column: Option<String>,
    pub filter_values: Option<HashSet<String>>,
    pub named: Option<String>,
    frame_tx: FrameSender<PublishedFrame>,
}

impl SubscriberState {
    pub fn filter_column(&self) -> Option<&str> {
        self.filter_column.as_deref()
    }

    pub fn filter_values(&self) -> Option<&HashSet<String>> {
        self.filter_values.as_ref()
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst) || self.needs_resync.load(Ordering::SeqCst)
    }
}

struct LogState {
    entries: VecDeque<LogEntry>,
    bytes: u64,
}

pub struct SubscriptionHandle {
    channel: Arc<TableChannel>,
    sub: Arc<SubscriberState>,
    poll_interval: Duration,
}

impl std::fmt::Debug for SubscriptionHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SubscriptionHandle")
            .field("sub_id", &self.sub.id)
            .finish()
    }
}

impl SubscriptionHandle {
    pub fn sub_id(&self) -> u64 {
        self.sub.id
    }

    pub fn table(&self) -> (&str, &str) {
        (&self.channel.db, &self.channel.table)
    }

    pub fn poll_interval(&self) -> Duration {
        self.poll_interval
    }

    pub fn filter(&self) -> Option<(&str, &HashSet<String>)> {
        let col = self.sub.filter_column.as_deref()?;
        let vals = self.sub.filter_values.as_ref()?;
        Some((col, vals))
    }

    pub fn is_closed(&self) -> bool {
        self.sub.is_closed()
    }

    pub async fn wait_changed(&self) {
        let notified = self.channel.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if self.has_pending() || self.sub.is_closed() {
            return;
        }
        notified.await;
    }

    fn has_pending(&self) -> bool {
        if self.sub.needs_resync.load(Ordering::SeqCst) {
            return true;
        }
        let next = self.sub.next_delivered.load(Ordering::SeqCst);
        let log = self.channel.log.lock().unwrap();
        log.entries.back().map_or(false, |e| e.seq >= next)
    }

    pub fn drain(&self, max_entries: usize) -> DrainResult {
        if self.sub.needs_resync.load(Ordering::SeqCst) {
            self.sub.closed.store(true, Ordering::SeqCst);
            let floor = self.channel.floor();
            self.channel.remove_sub(self.sub.id);
            return DrainResult::Resync { floor };
        }
        let next = self.sub.next_delivered.load(Ordering::SeqCst);
        let mut seq_start = 0;
        let mut seq_end = 0;
        let mut batches = Vec::new();
        {
            let log = self.channel.log.lock().unwrap();
            for entry in log.entries.iter() {
                if entry.seq < next {
                    continue;
                }
                if !batches.is_empty() && batches.len() >= max_entries {
                    break;
                }
                if batches.is_empty() {
                    seq_start = entry.seq;
                }
                seq_end = entry.seq;
                batches.push(Arc::clone(&entry.batch));
            }
        }
        if batches.is_empty() {
            return DrainResult::Empty;
        }
        self.sub.last_sent.store(seq_end, Ordering::SeqCst);
        DrainResult::Data {
            seq_start,
            seq_end,
            batches,
        }
    }

    pub fn advance_delivered(&self, seq_end: u64) {
        self.sub.next_delivered.store(seq_end + 1, Ordering::SeqCst);
    }

    pub async fn send_frame(&self, kind: FrameKind) -> bool {
        let frame = PublishedFrame {
            db: self.channel.db.clone(),
            table: self.channel.table.clone(),
            sub_id: self.sub.id,
            kind,
        };
        self.sub.frame_tx.send(frame).await.is_ok()
    }
}

#[derive(Debug, Clone)]
struct BookmarkMeta {
    name: String,
    db: String,
    table: String,
    dir: PathBuf,
    last_write: Option<SystemTime>,
}

pub struct TableChannel {
    db: String,
    table: String,
    last_seq: AtomicU64,
    gap: AtomicBool,
    log: Mutex<LogState>,
    subs: Mutex<HashMap<u64, Arc<SubscriberState>>>,
    notify: Notify,
    log_max_batches: usize,
    log_max_bytes: u64,
    committed_batches: AtomicU64,
    committed_rows: AtomicU64,
    resyncs: AtomicU64,
    /// Manager-wide live subscriber counter shared by every channel, so the
    /// write-path gate stays a single relaxed atomic load.
    live_subs: Arc<AtomicU64>,
    /// Last append or subscription change; drives the idle log reclaim.
    last_activity: Mutex<std::time::Instant>,
    /// Set by the idle reaper: the log was reclaimed and the write path may
    /// stop mirroring commits into this channel. A fresh subscribe un-retires
    /// it. `last_seq` is kept either way so sequences never renumber.
    retired: AtomicBool,
}

/// Subscriber-less channels keep their retained log for this long so a
/// reconnecting named subscription can resume, then it is reclaimed.
const IDLE_LOG_RETENTION: Duration = Duration::from_secs(60);

impl TableChannel {
    fn new(db: &str, table: &str, settings: &PubSubSettings, live_subs: Arc<AtomicU64>) -> Self {
        Self {
            db: db.to_string(),
            table: table.to_string(),
            last_seq: AtomicU64::new(0),
            gap: AtomicBool::new(false),
            log: Mutex::new(LogState {
                entries: VecDeque::new(),
                bytes: 0,
            }),
            subs: Mutex::new(HashMap::new()),
            notify: Notify::new(),
            log_max_batches: settings.log_max_batches.max(8),
            log_max_bytes: settings.log_max_bytes_mb.max(1) * 1024 * 1024,
            committed_batches: AtomicU64::new(0),
            committed_rows: AtomicU64::new(0),
            resyncs: AtomicU64::new(0),
            live_subs,
            last_activity: Mutex::new(std::time::Instant::now()),
            retired: AtomicBool::new(false),
        }
    }

    /// Drop every retained entry of the log. Only valid while the channel
    /// has no subscribers: nobody can resume from evicted sequences anymore,
    /// and `floor()` falls back to `last_seq + 1` so late `FromSeq` grants
    /// are rejected instead of silently replaying renumbered sequences.
    fn clear_log(&self) {
        let mut log = self.log.lock().unwrap();
        log.entries.clear();
        log.bytes = 0;
    }

    pub fn last_seq(&self) -> u64 {
        self.last_seq.load(Ordering::SeqCst)
    }

    fn is_retired(&self) -> bool {
        self.retired.load(Ordering::Relaxed)
    }

    pub fn floor(&self) -> u64 {
        let log = self.log.lock().unwrap();
        log.entries
            .front()
            .map(|e| e.seq)
            .unwrap_or_else(|| self.last_seq() + 1)
    }

    fn subscriber_count(&self) -> usize {
        self.subs.lock().unwrap().len()
    }

    pub fn stats(&self) -> (u64, u64, u64, usize) {
        let log = self.log.lock().unwrap();
        (
            self.committed_batches.load(Ordering::Relaxed),
            self.committed_rows.load(Ordering::Relaxed),
            log.bytes,
            log.entries.len(),
        )
    }

    fn append(&self, batch: Arc<RecordBatch>) {
        let seq = self.last_seq.fetch_add(1, Ordering::SeqCst) + 1;
        let approx = batch.estimated_size_bytes().max(64) as u64;
        let rows = batch.num_rows() as u64;
        {
            let subs_snapshot: Vec<Arc<SubscriberState>> = {
                let subs = self.subs.lock().unwrap();
                subs.values().cloned().collect()
            };
            let mut log = self.log.lock().unwrap();
            log.entries.push_back(LogEntry {
                seq,
                batch: Arc::clone(&batch),
                approx_bytes: approx,
            });
            log.bytes += approx;
            while log.entries.len() > 1
                && (log.entries.len() > self.log_max_batches || log.bytes > self.log_max_bytes)
            {
                let evicted = log.entries.pop_front().unwrap();
                log.bytes -= evicted.approx_bytes;
                for sub in &subs_snapshot {
                    if sub.next_delivered.load(Ordering::SeqCst) <= evicted.seq {
                        sub.needs_resync.store(true, Ordering::SeqCst);
                        self.resyncs.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }
        self.committed_batches.fetch_add(1, Ordering::Relaxed);
        self.committed_rows.fetch_add(rows, Ordering::Relaxed);
        *self.last_activity.lock().unwrap() = std::time::Instant::now();
        self.notify.notify_waiters();
    }

    fn mark_gap(&self) {
        self.gap.store(true, Ordering::SeqCst);
    }

    fn resolve_gap(&self) {
        if self.gap.swap(false, Ordering::SeqCst) {
            self.resyncs.fetch_add(1, Ordering::Relaxed);
            let subs: Vec<Arc<SubscriberState>> =
                self.subs.lock().unwrap().values().cloned().collect();
            for sub in subs {
                sub.needs_resync.store(true, Ordering::SeqCst);
            }
        }
    }

    fn add_sub(
        &self,
        id: u64,
        session_id: Uuid,
        from: u64,
        filter_column: Option<String>,
        filter_values: Option<HashSet<String>>,
        named: Option<String>,
        frame_tx: FrameSender<PublishedFrame>,
    ) -> Arc<SubscriberState> {
        let sub = Arc::new(SubscriberState {
            id,
            session_id,
            db: self.db.clone(),
            table: self.table.clone(),
            next_delivered: AtomicU64::new(from.saturating_add(1)),
            last_acked: AtomicU64::new(from),
            last_sent: AtomicU64::new(from),
            lagging: AtomicBool::new(false),
            needs_resync: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            filter_column,
            filter_values,
            named,
            frame_tx,
        });
        self.subs.lock().unwrap().insert(id, Arc::clone(&sub));
        self.live_subs.fetch_add(1, Ordering::Relaxed);
        sub
    }

    fn remove_sub(&self, sub_id: u64) -> Option<Arc<SubscriberState>> {
        if let Some(sub) = self.subs.lock().unwrap().remove(&sub_id) {
            sub.closed.store(true, Ordering::SeqCst);
            self.live_subs.fetch_sub(1, Ordering::Relaxed);
            *self.last_activity.lock().unwrap() = std::time::Instant::now();
            self.notify.notify_waiters();
            Some(sub)
        } else {
            None
        }
    }

    fn remove_session(&self, session_id: &Uuid) -> Vec<Arc<SubscriberState>> {
        let targets: Vec<u64> = self
            .subs
            .lock()
            .unwrap()
            .values()
            .filter(|s| s.session_id == *session_id)
            .map(|s| s.id)
            .collect();
        targets
            .into_iter()
            .filter_map(|id| self.remove_sub(id))
            .collect()
    }

    fn ack(&self, sub_id: u64, seq: u64) -> Result<()> {
        let subs = self.subs.lock().unwrap();
        let Some(sub) = subs.get(&sub_id) else {
            return Err(TaibaiError::Protocol(format!(
                "unknown subscription id {sub_id} for {}.{}",
                self.db, self.table
            )));
        };
        let last = sub.last_acked.load(Ordering::SeqCst);
        if seq < last {
            return Err(TaibaiError::Protocol(format!(
                "subscription ack sequence {seq} is below acknowledged {last}"
            )));
        }
        sub.last_acked.store(seq, Ordering::SeqCst);
        if sub.lagging.load(Ordering::SeqCst) && seq >= sub.last_sent.load(Ordering::SeqCst) {
            sub.lagging.store(false, Ordering::SeqCst);
        }
        Ok(())
    }
}

#[derive(Debug, Default, Clone)]
pub struct PubsubStatsSnapshot {
    pub enabled: bool,
    pub channel_count: usize,
    pub subscriber_count: usize,
    pub log_entry_count: usize,
    pub log_bytes: u64,
    pub committed_batches: u64,
    pub committed_rows: u64,
    pub missed_notices: u64,
    pub resyncs: u64,
}

pub struct PubSubManager {
    settings: PubSubSettings,
    enabled: bool,
    epoch: u64,
    channels: RwLock<HashMap<(String, String), Arc<TableChannel>>>,
    commit_tx: Mutex<Option<crossbeam_channel::Sender<CommitNotice>>>,
    next_sub_id: AtomicU64,
    bookmark_dir: Option<PathBuf>,
    bookmarks: Mutex<HashMap<u64, BookmarkMeta>>,
    missed_notices: AtomicU64,
    /// Live subscriptions across every channel. The hub mirror gate reads
    /// this with one relaxed load on the write path.
    live_subs: Arc<AtomicU64>,
}

impl PubSubManager {
    pub fn disabled() -> Arc<Self> {
        Arc::new(Self {
            settings: PubSubSettings::default(),
            enabled: false,
            epoch: 0,
            channels: RwLock::new(HashMap::new()),
            commit_tx: Mutex::new(None),
            next_sub_id: AtomicU64::new(1),
            bookmark_dir: None,
            bookmarks: Mutex::new(HashMap::new()),
            missed_notices: AtomicU64::new(0),
            live_subs: Arc::new(AtomicU64::new(0)),
        })
    }

    pub fn start(settings: PubSubSettings, bookmark_dir: PathBuf) -> Arc<Self> {
        let (tx, rx) = bounded::<CommitNotice>(settings.commit_ring_capacity.max(64));
        let epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(1);
        let manager = Arc::new(Self {
            settings,
            enabled: true,
            epoch,
            channels: RwLock::new(HashMap::new()),
            commit_tx: Mutex::new(Some(tx)),
            next_sub_id: AtomicU64::new(1),
            bookmark_dir: Some(bookmark_dir),
            bookmarks: Mutex::new(HashMap::new()),
            missed_notices: AtomicU64::new(0),
            live_subs: Arc::new(AtomicU64::new(0)),
        });
        let worker = Arc::downgrade(&manager);
        std::thread::Builder::new()
            .name("pubsub-publisher".into())
            .spawn(move || publisher_loop(worker, rx))
            .expect("spawn pubsub publisher thread");
        manager
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Live subscriptions across every channel; one relaxed atomic load.
    /// Cheap enough for the write-path gate.
    pub fn total_subscriber_count(&self) -> u64 {
        self.live_subs.load(Ordering::Relaxed)
    }

    /// True while at least one table channel is still active (subscribed to
    /// at some point, not yet retired by the idle reaper). The hub uses this
    /// to decide whether committed inserts must keep being mirrored into the
    /// retention logs -- that is what makes a reconnecting subscription
    /// resume instead of silently skipping commits.
    pub fn has_active_channels(&self) -> bool {
        if !self.enabled {
            return false;
        }
        self.channels
            .read()
            .unwrap()
            .values()
            .any(|channel| !channel.is_retired())
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    pub fn settings(&self) -> &PubSubSettings {
        &self.settings
    }

    pub fn clone_for_publish(
        &self,
        db: &str,
        table: &str,
        batch: &RecordBatch,
    ) -> Option<Arc<RecordBatch>> {
        if !self.enabled {
            return None;
        }
        self.existing_channel(db, table)
            .filter(|c| c.subscriber_count() > 0)?;
        Some(Arc::new(batch.clone()))
    }

    pub fn notify_committed_arc(&self, db: &str, table: &str, batch: Arc<RecordBatch>) {
        if !self.enabled {
            return;
        }
        // Notices are retained per existing channel even while no subscriber
        // is attached: that is what lets a reconnecting named subscription
        // resume without silently skipping commits. The log is bounded, so
        // retention costs stay proportional to the configured window.
        let Some(channel) = self.existing_channel(db, table) else {
            return;
        };
        let notice = CommitNotice {
            db: Arc::from(db),
            table: Arc::from(table),
            batch,
        };
        self.send_notice(channel, notice);
    }

    /// Non-blocking variant used by the hub mirror on the write path: a full
    /// commit ring must never stall an INSERT, so the notice is dropped and
    /// the gap flag makes subscribers resync.
    pub fn try_notify_committed_arc(&self, db: &str, table: &str, batch: Arc<RecordBatch>) {
        if !self.enabled {
            return;
        }
        let Some(channel) = self.existing_channel(db, table) else {
            return;
        };
        let notice = CommitNotice {
            db: Arc::from(db),
            table: Arc::from(table),
            batch,
        };
        let guard = self.commit_tx.lock().unwrap();
        let Some(tx) = guard.as_ref() else {
            return;
        };
        if tx.try_send(notice).is_err() {
            self.missed_notices.fetch_add(1, Ordering::Relaxed);
            channel.mark_gap();
        }
    }

    pub fn notify_commit(&self, db: &str, table: &str, batch: &RecordBatch) {
        if !self.enabled {
            return;
        }
        let Some(channel) = self.existing_channel(db, table) else {
            return;
        };
        let notice = CommitNotice {
            db: Arc::from(db),
            table: Arc::from(table),
            batch: Arc::new(batch.clone()),
        };
        self.send_notice(channel, notice);
    }

    fn send_notice(&self, channel: Arc<TableChannel>, notice: CommitNotice) {
        let guard = self.commit_tx.lock().unwrap();
        let Some(tx) = guard.as_ref() else {
            return;
        };
        match tx.try_send(notice) {
            Ok(()) => {}
            Err(TrySendError::Full(notice)) => match tx.send_timeout(notice, PUBLISH_RETRY) {
                Ok(()) => {}
                Err(_) => {
                    self.missed_notices.fetch_add(1, Ordering::Relaxed);
                    channel.mark_gap();
                }
            },
            Err(TrySendError::Disconnected(_)) => {}
        }
    }

    fn existing_channel(&self, db: &str, table: &str) -> Option<Arc<TableChannel>> {
        self.channels
            .read()
            .unwrap()
            .get(&(db.to_string(), table.to_string()))
            .cloned()
    }

    fn ensure_channel(&self, db: &str, table: &str) -> Arc<TableChannel> {
        if let Some(ch) = self.existing_channel(db, table) {
            return ch;
        }
        let mut channels = self.channels.write().unwrap();
        channels
            .entry((db.to_string(), table.to_string()))
            .or_insert_with(|| {
                Arc::new(TableChannel::new(
                    db,
                    table,
                    &self.settings,
                    Arc::clone(&self.live_subs),
                ))
            })
            .clone()
    }

    pub fn subscribe(&self, req: SubscribeRequest) -> Result<(SubscribeGrant, SubscriptionHandle)> {
        if !self.enabled {
            return Err(TaibaiError::Protocol(
                "pubsub is disabled on this server".into(),
            ));
        }
        if self.total_subscriber_count() >= self.settings.max_subscriptions.max(1) as u64 {
            return Err(TaibaiError::Protocol(format!(
                "subscription limit reached ({})",
                self.settings.max_subscriptions
            )));
        }
        let channel = self.ensure_channel(&req.db, &req.table);
        channel.retired.store(false, Ordering::Relaxed);
        channel.resolve_gap();
        let floor = channel.floor();
        let last = channel.last_seq();
        let mut from = match &req.mode {
            SubscribeMode::FromNow => last,
            SubscribeMode::FromSeq(seq) => {
                if *seq + 1 < floor {
                    return Err(TaibaiError::Protocol(format!(
                        "requested sequence {} is no longer retained; floor is {}",
                        *seq, floor
                    )));
                }
                (*seq).min(last)
            }
        };
        let mut named = req.named.clone();
        if let Some(name) = named.clone() {
            if let Some(saved) = self.load_bookmark(&req.db, &req.table, &name) {
                let saved_from = saved;
                if saved_from + 1 >= floor && saved_from < from {
                    from = saved_from;
                }
            } else {
                named = Some(name.clone());
            }
        }
        Ok(self.register_sub(channel, req, named, from, floor))
    }

    fn register_sub(
        &self,
        channel: Arc<TableChannel>,
        req: SubscribeRequest,
        named: Option<String>,
        from: u64,
        floor: u64,
    ) -> (SubscribeGrant, SubscriptionHandle) {
        let sub_id = self.next_sub_id.fetch_add(1, Ordering::SeqCst);
        let sub = channel.add_sub(
            sub_id,
            req.session_id,
            from,
            req.filter_column,
            req.filter_values,
            named.clone(),
            req.frame_tx,
        );
        if let (Some(name), Some(base)) = (named, self.bookmark_dir.clone()) {
            self.bookmarks.lock().unwrap().insert(
                sub_id,
                BookmarkMeta {
                    name,
                    db: channel.db.clone(),
                    table: channel.table.clone(),
                    dir: base,
                    last_write: None,
                },
            );
        }
        let grant = SubscribeGrant {
            sub_id,
            epoch: self.epoch,
            granted_from: from,
            floor,
        };
        let handle = SubscriptionHandle {
            channel,
            sub,
            poll_interval: Duration::from_millis(self.settings.poll_interval_ms.max(1)),
        };
        (grant, handle)
    }

    pub fn ack(&self, db: &str, table: &str, sub_id: u64, seq: u64) -> Result<()> {
        let channel = self
            .existing_channel(db, table)
            .ok_or_else(|| TaibaiError::Protocol(format!("unknown table {db}.{table}")))?;
        channel.ack(sub_id, seq)?;
        self.flush_bookmark_if_due(sub_id, seq);
        Ok(())
    }

    pub fn unsubscribe(&self, db: &str, table: &str, sub_id: u64) -> bool {
        let Some(channel) = self.existing_channel(db, table) else {
            return false;
        };
        let removed = channel.remove_sub(sub_id);
        if let Some(sub) = &removed {
            if sub.named.is_some() {
                let acked = sub.last_acked.load(Ordering::SeqCst);
                self.write_bookmark(sub_id, acked);
            }
        }
        self.bookmarks.lock().unwrap().remove(&sub_id);
        removed.is_some()
    }

    pub fn unsubscribe_session(&self, session_id: &Uuid) -> usize {
        let mut removed = 0;
        let channels: Vec<Arc<TableChannel>> =
            self.channels.read().unwrap().values().cloned().collect();
        for channel in channels {
            for sub in channel.remove_session(session_id) {
                if sub.named.is_some() {
                    let acked = sub.last_acked.load(Ordering::SeqCst);
                    self.write_bookmark(sub.id, acked);
                }
                self.bookmarks.lock().unwrap().remove(&sub.id);
                removed += 1;
            }
        }
        removed
    }

    pub fn session_subscription_count(&self, session_id: &Uuid) -> usize {
        self.channels
            .read()
            .unwrap()
            .values()
            .map(|c| {
                c.subs
                    .lock()
                    .unwrap()
                    .values()
                    .filter(|s| s.session_id == *session_id)
                    .count()
            })
            .sum()
    }

    fn flush_bookmark_if_due(&self, sub_id: u64, acked: u64) {
        let due = {
            let bookmarks = self.bookmarks.lock().unwrap();
            match bookmarks.get(&sub_id) {
                None => return,
                Some(meta) => meta
                    .last_write
                    .map(|t| {
                        SystemTime::now().duration_since(t).unwrap_or_default()
                            >= BOOKMARK_FLUSH_INTERVAL
                    })
                    .unwrap_or(true),
            }
        };
        if due {
            self.write_bookmark(sub_id, acked);
        }
    }

    fn write_bookmark(&self, sub_id: u64, acked: u64) {
        let mut bookmarks = self.bookmarks.lock().unwrap();
        let Some(meta) = bookmarks.get_mut(&sub_id) else {
            return;
        };
        let dir = meta
            .dir
            .join(sanitize(&meta.db))
            .join(sanitize(&meta.table))
            .join(sanitize(&meta.name));
        if std::fs::create_dir_all(&dir).is_err() {
            return;
        }
        let doc = serde_json::json!({
            "name": meta.name,
            "db": meta.db,
            "table": meta.table,
            "last_acked": acked,
            "updated_at": SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
        });
        let path = dir.join("bookmark.json");
        let tmp = dir.join("bookmark.json.tmp");
        if std::fs::write(&tmp, doc.to_string()).is_ok() {
            let _ = std::fs::rename(&tmp, &path);
        }
        meta.last_write = Some(SystemTime::now());
    }

    fn load_bookmark(&self, db: &str, table: &str, name: &str) -> Option<u64> {
        let dir = self.bookmark_dir.as_ref()?;
        let path = dir
            .join(sanitize(db))
            .join(sanitize(table))
            .join(sanitize(name))
            .join("bookmark.json");
        let text = std::fs::read_to_string(path).ok()?;
        let value: serde_json::Value = serde_json::from_str(&text).ok()?;
        value.get("last_acked").and_then(|v| v.as_u64())
    }

    pub fn stats(&self) -> PubsubStatsSnapshot {
        let channels = self.channels.read().unwrap();
        let mut snap = PubsubStatsSnapshot {
            enabled: self.enabled,
            channel_count: channels.len(),
            ..Default::default()
        };
        snap.missed_notices = self.missed_notices.load(Ordering::Relaxed);
        for channel in channels.values() {
            let (committed, rows, bytes, entries) = channel.stats();
            snap.subscriber_count += channel.subscriber_count();
            snap.log_entry_count += entries;
            snap.log_bytes += bytes;
            snap.committed_batches += committed;
            snap.committed_rows += rows;
            snap.resyncs += channel.resyncs.load(Ordering::Relaxed);
        }
        snap
    }

    fn reap_idle_channels(&self) {
        // Channels are never removed from the map (dropping one would reset
        // `last_seq` and renumber future changes). Their retained log is
        // reclaimed once no subscriber has been attached for a while:
        // without this, write-once tables would pin committed batches
        // forever. `floor()` falls back to `last_seq + 1` for an empty log,
        // so late `FromSeq` grants are rejected instead of replaying gaps.
        let channels = self.channels.read().unwrap();
        for channel in channels.values() {
            if channel.subscriber_count() > 0 {
                continue;
            }
            let idle = channel.last_activity.lock().unwrap().elapsed();
            if idle >= IDLE_LOG_RETENTION {
                channel.clear_log();
                channel.retired.store(true, Ordering::Relaxed);
            }
        }
    }
}

fn sanitize(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

fn publisher_loop(
    manager: std::sync::Weak<PubSubManager>,
    receiver: crossbeam_channel::Receiver<CommitNotice>,
) {
    loop {
        match receiver.recv_timeout(PUBLISHER_TICK) {
            Ok(notice) => {
                let Some(manager) = manager.upgrade() else {
                    return;
                };
                let channel = manager.ensure_channel(&notice.db, &notice.table);
                channel.resolve_gap();
                channel.append(notice.batch);
            }
            Err(RecvTimeoutError::Timeout) => {
                let Some(manager) = manager.upgrade() else {
                    return;
                };
                manager.reap_idle_channels();
            }
            Err(RecvTimeoutError::Disconnected) => return,
        }
    }
}

#[cfg(test)]
#[path = "pubsub_test.rs"]
mod tests;
