use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crc32fast::Hasher;
use uuid::Uuid;

use crate::common::error::{Result, TaibaiError};
use crate::protocol::types::DataType;
use crate::sql::execution::dml_update::rebuild_update_column;
use crate::sql::execution::wal_helper::{
    compress_prepared_batch_for_wal_with, deserialize_batch_from_wal, prepare_batch_for_wal,
    serialize_batch_for_wal,
};
use crate::storage::segment::SegmentReader;
use crate::types::batch::{RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

const TAIL_MAGIC: u32 = 0x5441_494C;
/// V2 tail log frames carry the row count in the frame header so recovery can
/// index a tail without decompressing any payload.
const TAIL_LOG_MAGIC_V2: u32 = 0x544C_4F48;
/// V3 tail log frames extend the V2 header with a globally allocated LSN and
/// a commit timestamp (micros since the Unix epoch), which replication site
/// tracking and point-in-time recovery replay both need. The file header
/// magic distinguishes the frame layouts; V2 files stay decodable.
const TAIL_LOG_MAGIC_V3: u32 = 0x544C_4F56;
/// Monotonic high-water for tail LSNs, seeded from the wall clock at thread
/// start and never allowed to regress within the process.
static TAIL_LSN_HIGH_WATER: AtomicU64 = AtomicU64::new(0);
const FLUSH_MAGIC: u32 = 0x464C_5553;
const TAIL_DIR: &str = "memtable_tail";
static TAIL_SEQUENCE: AtomicU64 = AtomicU64::new(0);
/// Monotonic high-water for generation sequences. Wall-clock nanos alone let
/// an NTP step backwards make new generations sort before existing ones,
/// resurrecting deleted tails during arbitration. Seeded from sequences seen
/// on disk and never allowed to regress within the process.
static TAIL_SEQUENCE_HIGH_WATER: AtomicU64 = AtomicU64::new(0);

const GROUP_COMMIT_MAX_DELAY: Duration = Duration::from_micros(500);
/// Once the previous group held several statements the workload is
/// concurrent: widen the collection window so one storage barrier amortizes
/// over the whole in-flight burst instead of splitting into singleton fsyncs.
const GROUP_COMMIT_CONCURRENT_DELAY: Duration = Duration::from_millis(2);
/// Async-fsync mode bounds the acknowledged-but-not-yet-fsynced window by
/// bytes and time; the commit thread also syncs after the write stream stays
/// quiet even if neither bound is reached. The quiescence period must exceed
/// the natural gap between bursts of a busy workload, otherwise every
/// inter-group gap would trigger a sync. It also scales with the observed
/// fsync cost: paying a 30ms stall for 5ms of silence is a bad trade, so the
/// effective quiet period is `max(base, 2 x moving-average sync cost)`,
/// capped at `ASYNC_FSYNC_MAX_DELAY` to respect the durability deadline.
const ASYNC_FSYNC_MAX_DIRTY_BYTES: usize = 8 * 1024 * 1024;
const ASYNC_FSYNC_MAX_DELAY: Duration = Duration::from_millis(200);
const ASYNC_FSYNC_QUIESCE_DELAY: Duration = Duration::from_millis(5);
const GROUP_COMMIT_MAX_BYTES: usize = 16 * 1024 * 1024;
const GROUP_COMMIT_MAX_REQUESTS: usize = 128;

fn observe_sync_cost(previous: Duration, observed: Duration) -> Duration {
    if previous.is_zero() {
        observed
    } else {
        (previous * 3 + observed) / 4
    }
}

fn async_quiesce_delay(sync_cost: Duration) -> Duration {
    ASYNC_FSYNC_QUIESCE_DELAY
        .max(sync_cost.saturating_mul(2))
        .min(ASYNC_FSYNC_MAX_DELAY)
}

/// A WAL frame heading for the tail log. Two preparation variants feed the
/// same commit thread, and both keep the design intent that serialization and
/// zstd level 3 compression never run under the table mutation lock; the tail
/// writer only performs sequential file I/O and durability:
///
/// * eager (`new`): the request thread serializes before acquiring the lock
///   (single auto-commit INSERTs), so the lock only sees the ready payload;
/// * lazy (`from_shared_batch`): the COMMIT fan-out already holds the table
///   mutation lock, so it only clones the memtable batch's `Arc` and defers
///   serialize + compress to the commit thread's `finish`.
pub(crate) struct PreparedTailAppend {
    /// Eager variant's serialized payload, consumed by `finish`.
    wal_payload: Option<Vec<u8>>,
    /// Lazy variant's shared source batch, dropped once the frame exists.
    batch: Option<Arc<RecordBatch>>,
    frame: Option<Vec<u8>>,
    rows: u64,
}

impl PreparedTailAppend {
    /// Eager variant: serialize now, on the request thread, before the table
    /// mutation lock is taken.
    pub(crate) fn new(batch: &RecordBatch) -> Result<Self> {
        Ok(Self {
            wal_payload: Some(prepare_batch_for_wal(batch)?),
            batch: None,
            frame: None,
            rows: batch.num_rows() as u64,
        })
    }

    /// Lazy variant: hold a shared reference to the memtable's batch and
    /// encode on the commit thread. Infallible by design — the caller holding
    /// the table mutation lock only pays an `Arc` clone here, and an encoding
    /// failure is reported later against this single command (it must fail
    /// the command, not poison the commit thread).
    pub(crate) fn from_shared_batch(batch: Arc<RecordBatch>) -> Self {
        Self {
            wal_payload: None,
            rows: batch.num_rows() as u64,
            batch: Some(batch),
            frame: None,
        }
    }

    /// Byte size charged against the group-commit window. Before the frame is
    /// encoded this is an estimate: the raw (uncompressed) payload, which
    /// upper-bounds the compressed frame, so the window's byte bound never
    /// under-estimates. After `finish` it is the exact frame size.
    fn encoded_len(&self) -> usize {
        if let Some(frame) = self.frame.as_ref() {
            return frame.len();
        }
        if let Some(payload) = self.wal_payload.as_ref() {
            return payload.len();
        }
        self.batch
            .as_ref()
            .map(|batch| batch.estimated_size_bytes())
            .unwrap_or(0)
    }

    fn finish(
        &mut self,
        compressor: &mut zstd::bulk::Compressor<'_>,
        lsn: u64,
        commit_ts_micros: i64,
    ) -> Result<()> {
        if self.frame.is_none() {
            // Serialize the lazy variant here, on the commit thread, then
            // compress either variant once. Runs at most once per command.
            let raw = match self.wal_payload.take() {
                Some(raw) => raw,
                None => {
                    let batch = self.batch.as_deref().ok_or_else(|| {
                        TaibaiError::Internal(
                            "tail append carries neither payload nor source batch".into(),
                        )
                    })?;
                    prepare_batch_for_wal(batch)?
                }
            };
            let payload = compress_prepared_batch_for_wal_with(compressor, raw)?;
            self.frame = Some(encode_tail_frame_payload(
                &payload,
                self.rows,
                lsn,
                commit_ts_micros,
            ));
            // The encoded frame now owns the durable representation; drop the
            // shared batch reference.
            self.batch = None;
        }
        Ok(())
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

    fn wait_bounded(&self, timeout: Duration, warn_after: Duration) -> Result<()> {
        let start = Instant::now();
        let mut guard = self.outcome.lock().unwrap();
        while guard.is_none() {
            let elapsed = start.elapsed();
            if elapsed >= timeout {
                return Err(TaibaiError::Storage(format!(
                    "durable tail commit timed out after {}ms",
                    timeout.as_millis()
                )));
            }
            let (new_guard, _) = self.ready.wait_timeout(guard, timeout - elapsed).unwrap();
            guard = new_guard;
        }
        let waited = start.elapsed();
        if waited >= warn_after {
            tracing::warn!(
                wait_ms = waited.as_millis() as u64,
                "durable tail commit acknowledgement was slow"
            );
        }
        match guard.as_ref().unwrap() {
            Ok(()) => Ok(()),
            Err(message) => Err(TaibaiError::Storage(message.clone())),
        }
    }
}

fn tail_commit_timeouts() -> (Duration, Duration) {
    let timeouts = crate::common::config::storage_timeouts();
    (
        Duration::from_secs(timeouts.tail_commit_timeout_secs.max(1)),
        Duration::from_secs(timeouts.tail_commit_warn_secs.max(1)),
    )
}

/// Upper bound on in-flight commit commands (frames are batch-sized; 8192 is
/// thousands of statements of slack before backpressure engages).
const TAIL_COMMIT_QUEUE_CAPACITY: usize = 8192;

pub(crate) struct TailCommitTicket {
    completion: Arc<CommitCompletion>,
}

impl TailCommitTicket {
    pub(crate) fn wait(self) -> Result<()> {
        let (timeout, warn_after) = tail_commit_timeouts();
        self.completion.wait_bounded(timeout, warn_after)
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
    Replicate(crate::storage::replication::ReplicationEvent),
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
    queued: Arc<AtomicUsize>,
    /// Replication log, wired after construction by the engine. Shared with
    /// the commit loop through an `Arc<OnceLock<_>>` because the loop is
    /// spawned in `start`, before the engine exists.
    rep_log: Arc<OnceLock<Arc<crate::storage::replication::RepLog>>>,
}

impl TailCommitter {
    pub(crate) fn start(async_fsync: bool) -> Arc<Self> {
        // Bounded: every queued command holds a batch-sized compressed WAL
        // frame, and the old unbounded channel grew without limit when the
        // disk could not keep up (the backlog warning did not slow anyone
        // down). A full queue now applies backpressure at `submit` for the
        // configured tail-commit timeout, then fails that statement's
        // durability wait instead of buffering without bound.
        let (sender, receiver) = crossbeam_channel::bounded(TAIL_COMMIT_QUEUE_CAPACITY);
        let queued = Arc::new(AtomicUsize::new(0));
        let loop_queued = Arc::clone(&queued);
        let rep_log: Arc<OnceLock<Arc<crate::storage::replication::RepLog>>> =
            Arc::new(OnceLock::new());
        let loop_rep_log = Arc::clone(&rep_log);
        let handle = std::thread::Builder::new()
            .name("taibai-tail-commit".into())
            .spawn(move || tail_commit_loop(receiver, async_fsync, loop_queued, loop_rep_log))
            .expect("failed to spawn durable tail commit thread");
        Arc::new(Self {
            sender,
            handle: Mutex::new(Some(handle)),
            queued,
            rep_log,
        })
    }

    /// Attach the replication log. Called once by the engine after both
    /// objects exist; events are silently dropped until then.
    pub(crate) fn set_rep_log(&self, log: Arc<crate::storage::replication::RepLog>) {
        let _ = self.rep_log.set(log);
    }

    /// Enqueue a structural replication event (segment updates and later
    /// DML/DDL events). It is seq-stamped when the commit thread dequeues
    /// it, after every tail append that causally precedes it (their flush
    /// paths hold the tail barrier first). Bounded backpressure, same as
    /// regular appends.
    pub(crate) fn submit_event(&self, event: crate::storage::replication::ReplicationEvent) {
        self.queued.fetch_add(1, Ordering::Relaxed);
        let (timeout, _warn) = tail_commit_timeouts();
        match self
            .sender
            .send_timeout(CommitCommand::Replicate(event), timeout)
        {
            Ok(()) => {}
            Err(crossbeam_channel::SendTimeoutError::Timeout(_)) => {
                self.queued.fetch_sub(1, Ordering::Relaxed);
                tracing::error!(
                    "replication event dropped: commit queue stayed full for the tail-commit timeout"
                );
            }
            Err(crossbeam_channel::SendTimeoutError::Disconnected(_)) => {
                self.queued.fetch_sub(1, Ordering::Relaxed);
            }
        }
    }

    pub(crate) fn submit(
        &self,
        table_dir: PathBuf,
        id: Uuid,
        partition_key: String,
        prepared: PreparedTailAppend,
    ) -> TailCommitTicket {
        let completion = CommitCompletion::new();
        self.queued.fetch_add(1, Ordering::Relaxed);
        let command = CommitCommand::Append(AppendCommand {
            table_dir,
            id,
            partition_key,
            prepared,
            completion: Arc::clone(&completion),
        });
        let (timeout, _warn) = tail_commit_timeouts();
        match self.sender.send_timeout(command, timeout) {
            Ok(()) => {}
            Err(crossbeam_channel::SendTimeoutError::Timeout(_)) => {
                self.queued.fetch_sub(1, Ordering::Relaxed);
                completion.finish(Err(
                    "durable tail commit backlog exceeded the queue and the fsync wait timed out; the write was not acknowledged".into(),
                ));
            }
            Err(crossbeam_channel::SendTimeoutError::Disconnected(_)) => {
                self.queued.fetch_sub(1, Ordering::Relaxed);
                completion.finish(Err("durable tail commit thread stopped".into()));
            }
        }
        TailCommitTicket { completion }
    }

    /// Wait for every previously submitted append for this table, then close
    /// its cached handles so flush/rename/delete operations are safe on Windows.
    pub(crate) fn barrier(&self, table_dir: &Path) -> Result<()> {
        let completion = CommitCompletion::new();
        let backlog = self.queued.fetch_add(1, Ordering::Relaxed) + 1;
        if backlog > 1024 {
            tracing::warn!(
                backlog,
                "durable tail committer backlog is large; fsync throughput cannot keep up with writes"
            );
        }
        if self
            .sender
            .send(CommitCommand::Barrier {
                table_dir: table_dir.to_path_buf(),
                completion: Arc::clone(&completion),
            })
            .is_err()
        {
            self.queued.fetch_sub(1, Ordering::Relaxed);
            return Err(TaibaiError::Storage(
                "durable tail commit thread stopped".into(),
            ));
        }
        let (timeout, warn_after) = tail_commit_timeouts();
        completion.wait_bounded(timeout, warn_after)
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
            let OpenTailResult::Ready(tail) = open_tail_for_group_append(command)?;
            self.open.insert(key.clone(), tail);
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
}

fn open_tail_for_group_append(command: &AppendCommand) -> Result<OpenTailResult> {
    let tail_dir = command.table_dir.join(TAIL_DIR);
    fs::create_dir_all(&tail_dir)?;
    if let Some(path) = newest_live_tail_path(&tail_dir, command.id)? {
        let is_current_format = read_tail_log_version(&path)?.is_some();
        if is_current_format
            && read_tail_log_partition_key(&path)?.as_deref() == Some(&command.partition_key)
        {
            let file = fs::OpenOptions::new().append(true).open(&path)?;
            return Ok(OpenTailResult::Ready(OpenTail {
                file,
                path,
                partition_key: command.partition_key.clone(),
                pending_rename: None,
            }));
        }

        return Err(TaibaiError::Storage(
            "unsupported durable tail version".into(),
        ));
    }

    let sequence = next_sequence();
    let stem = format!("{}.{}", command.id.simple(), sequence);
    let tmp_path = tail_dir.join(format!(".{stem}.tmp"));
    let final_path = tail_dir.join(format!("{stem}.tail"));
    let mut file = fs::OpenOptions::new()
        .create_new(true)
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

fn unqueue(queued: &AtomicUsize, counted: bool) {
    if counted {
        queued.fetch_sub(1, Ordering::Relaxed);
    }
}

fn tail_commit_loop(
    receiver: crossbeam_channel::Receiver<CommitCommand>,
    async_fsync: bool,
    queued: Arc<AtomicUsize>,
    rep_log: Arc<OnceLock<Arc<crate::storage::replication::RepLog>>>,
) {
    let mut files = TailFileSet::default();
    let mut compressor = zstd::bulk::Compressor::new(3)
        .expect("zstd level 3 compressor must initialize for durable tail writer");
    let mut deferred = None;
    let mut last_group_size = 0usize;
    let mut dirty_tails: HashSet<TailKey> = HashSet::new();
    let mut dirty_bytes = 0usize;
    let mut last_sync = Instant::now();
    let mut last_activity = Instant::now();
    let mut sync_cost = Duration::ZERO;
    // Seed the LSN high-water from the wall clock so LSNs minted after a
    // restart sort after every LSN already on disk. There is deliberately NO
    // thread-local counter on top of this: every frame — commit-thread and
    // direct-write alike — allocates from `TAIL_LSN_HIGH_WATER.fetch_add`,
    // the single LSN channel.
    {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;
        TAIL_LSN_HIGH_WATER.fetch_max(now, Ordering::AcqRel);
    }
    loop {
        if async_fsync && !dirty_tails.is_empty() {
            let quiesced = last_activity.elapsed() >= async_quiesce_delay(sync_cost);
            if quiesced
                || dirty_bytes >= ASYNC_FSYNC_MAX_DIRTY_BYTES
                || last_sync.elapsed() >= ASYNC_FSYNC_MAX_DELAY
            {
                let sync_started = Instant::now();
                sync_dirty_tails(
                    &mut files,
                    &mut dirty_tails,
                    &mut dirty_bytes,
                    &mut last_sync,
                );
                sync_cost = observe_sync_cost(sync_cost, sync_started.elapsed());
            }
        }
        let command = match deferred.take() {
            Some(command) => {
                last_activity = Instant::now();
                command
            }
            None => {
                // With a deferred fsync backlog the loop must wake up before
                // the durability deadline even when the channel goes quiet.
                let dirty_wait = if async_fsync && !dirty_tails.is_empty() {
                    let time_bound = ASYNC_FSYNC_MAX_DELAY.saturating_sub(last_sync.elapsed());
                    let quiesce_bound = async_quiesce_delay(sync_cost)
                        .saturating_sub(last_activity.elapsed())
                        .max(Duration::from_micros(200));
                    Some(time_bound.min(quiesce_bound))
                } else {
                    None
                };
                match dirty_wait {
                    Some(wait) => match receiver.recv_timeout(wait) {
                        Ok(command) => {
                            unqueue(&queued, !matches!(command, CommitCommand::Shutdown { .. }));
                            last_activity = Instant::now();
                            command
                        }
                        Err(crossbeam_channel::RecvTimeoutError::Timeout) => continue,
                        Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                            sync_dirty_tails(
                                &mut files,
                                &mut dirty_tails,
                                &mut dirty_bytes,
                                &mut last_sync,
                            );
                            files.open.clear();
                            break;
                        }
                    },
                    None => match receiver.recv() {
                        Ok(command) => {
                            unqueue(&queued, !matches!(command, CommitCommand::Shutdown { .. }));
                            last_activity = Instant::now();
                            command
                        }
                        Err(_) => {
                            sync_dirty_tails(
                                &mut files,
                                &mut dirty_tails,
                                &mut dirty_bytes,
                                &mut last_sync,
                            );
                            files.open.clear();
                            break;
                        }
                    },
                }
            }
        };
        match command {
            CommitCommand::Append(first) => {
                let window = if last_group_size > 1 {
                    GROUP_COMMIT_CONCURRENT_DELAY
                } else {
                    GROUP_COMMIT_MAX_DELAY
                };
                let mut bytes = first.prepared.encoded_len();
                let mut group = vec![first];
                let deadline = Instant::now() + window;
                while group.len() < GROUP_COMMIT_MAX_REQUESTS && bytes < GROUP_COMMIT_MAX_BYTES {
                    let now = Instant::now();
                    if now >= deadline {
                        break;
                    }
                    match receiver.recv_timeout(deadline.saturating_duration_since(now)) {
                        Ok(CommitCommand::Append(next)) => {
                            unqueue(&queued, true);
                            last_activity = Instant::now();
                            bytes = bytes.saturating_add(next.prepared.encoded_len());
                            group.push(next);
                        }
                        Ok(other) => {
                            unqueue(&queued, matches!(other, CommitCommand::Barrier { .. }));
                            deferred = Some(other);
                            break;
                        }
                        Err(crossbeam_channel::RecvTimeoutError::Timeout) => break,
                        Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
                    }
                }
                last_group_size = group.len();

                let written = write_append_group(&mut files, &mut compressor, group);

                if async_fsync {
                    // Tail-append replication events are published INLINE on
                    // this thread: seq order then equals the frame-write
                    // order. (They must not re-enter the bounded channel -
                    // this thread is its only drainer, and a full queue would
                    // deadlock.) Frames are in the page cache: release the
                    // acknowledgements now and keep the storage barrier off
                    // the INSERT latency path. The dirty tails are synced on
                    // quiescence, byte, or time bounds above; ordering with
                    // later commands (barriers, flush intents) is preserved
                    // because this thread processes the channel serially.
                    let events = build_tail_append_events(rep_log.get(), &written);
                    if let Some(log) = rep_log.get() {
                        for event in events {
                            log.publish(event);
                        }
                    }
                    for group in written {
                        dirty_tails.insert(group.key);
                        dirty_bytes = dirty_bytes.saturating_add(group.bytes);
                        for completion in group.completions {
                            completion.finish(Ok(()));
                        }
                    }
                } else {
                    // Sync mode publishes only after the frames are durable:
                    // an fsync failure poisons the tail and errors every
                    // completion, so the primary reports the INSERT as
                    // failed — a standby that had already applied a
                    // publish-before-fsync event would hold rows the primary
                    // may not have. Publishing here keeps seq order (this
                    // thread is the only publisher) and keeps the replicated
                    // set a subset of the durable set.
                    let events = build_tail_append_events(rep_log.get(), &written);
                    sync_append_group(&mut files, written);
                    if let Some(log) = rep_log.get() {
                        for event in events {
                            log.publish(event);
                        }
                    }
                }
            }
            CommitCommand::Barrier {
                table_dir,
                completion,
            } => {
                let sync_started = Instant::now();
                sync_dirty_tails_for_table(&mut files, &mut dirty_tails, &table_dir);
                if async_fsync {
                    sync_cost = observe_sync_cost(sync_cost, sync_started.elapsed());
                }
                files.evict_table(&table_dir);
                completion.finish(Ok(()));
            }
            CommitCommand::Replicate(event) => {
                if let Some(log) = rep_log.get() {
                    log.publish(event);
                }
            }
            CommitCommand::Shutdown { completion } => {
                sync_dirty_tails(
                    &mut files,
                    &mut dirty_tails,
                    &mut dirty_bytes,
                    &mut last_sync,
                );
                files.open.clear();
                completion.finish(Ok(()));
                break;
            }
        }
    }
}

struct WrittenTailGroup {
    key: TailKey,
    completions: Vec<Arc<CommitCompletion>>,
    bytes: usize,
    /// Frames written for this tail in this group, for the replication
    /// event. `None` when the group wrote nothing.
    rep_payload: Option<TailAppendPayload>,
}

/// Everything the replication event needs about one group's appends to one
/// tail. db/table are derived from the table dir (`<data>/<db>/<table>`).
struct TailAppendPayload {
    db: String,
    table: String,
    partition: String,
    memtable_id: Uuid,
    first_lsn: u64,
    last_lsn: u64,
    frames: Vec<u8>,
}

/// Build the tail-append replication events for a written group batch.
/// Called only from the tail-commit thread; the events are prebuilt so the
/// sync-mode branch can construct them before the group is consumed by the
/// fsync and publish them once durability is confirmed.
fn build_tail_append_events(
    rep_log: Option<&std::sync::Arc<crate::storage::replication::RepLog>>,
    written: &[WrittenTailGroup],
) -> Vec<crate::storage::replication::ReplicationEvent> {
    if rep_log.is_none() {
        return Vec::new();
    }
    written
        .iter()
        .filter_map(|group| {
            group.rep_payload.as_ref().map(|payload| {
                crate::storage::replication::ReplicationEvent::TailAppend {
                    db: payload.db.clone(),
                    table: payload.table.clone(),
                    partition: payload.partition.clone(),
                    memtable_id: payload.memtable_id,
                    first_lsn: payload.first_lsn,
                    last_lsn: payload.last_lsn,
                    frames: payload.frames.clone(),
                }
            })
        })
        .collect()
}

fn write_append_group(
    files: &mut TailFileSet,
    compressor: &mut zstd::bulk::Compressor<'_>,
    group: Vec<AppendCommand>,
) -> Vec<WrittenTailGroup> {
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
    // Frames of one group commit together, so they share the commit
    // timestamp (taken at write time, the durable-commit moment). The LSN is
    // per frame, allocated from the same global high-water the direct-write
    // helpers use — the single LSN channel — so a direct append racing this
    // group-commit window can never observe a duplicate LSN (only a gap).
    let commit_ts_micros = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0);
    let mut by_tail = HashMap::<TailKey, TailGroupAccumulator>::new();
    for mut command in group {
        // Frame-encoding failure (payload beyond the WAL u32 length field)
        // fails this request like a write error instead of poisoning the
        // committer thread.
        let lsn = TAIL_LSN_HIGH_WATER.fetch_add(1, Ordering::AcqRel) + 1;
        if let Err(error) = command.prepared.finish(compressor, lsn, commit_ts_micros) {
            command.completion.finish(Err(error.to_string()));
            continue;
        }
        // Charge the exact frame size only after encoding: the pre-encoding
        // estimate is the raw payload, which would over-state the bytes the
        // async-fsync window must bound.
        let frame_bytes = command.prepared.encoded_len();
        match files.append(&command) {
            Ok(key) => {
                let (db, table) = db_table_from_dir(&command.table_dir);
                let entry = by_tail.entry(key).or_insert_with(|| TailGroupAccumulator {
                    db,
                    table,
                    partition: command.partition_key.clone(),
                    memtable_id: command.id,
                    completions: Vec::new(),
                    bytes: 0,
                    frames: Vec::new(),
                    first_lsn: lsn,
                    last_lsn: lsn,
                });
                entry.completions.push(command.completion);
                entry.bytes += frame_bytes;
                entry.frames.extend_from_slice(command.prepared.frame());
                entry.last_lsn = lsn;
            }
            Err(error) => command.completion.finish(Err(error.to_string())),
        }
    }
    by_tail
        .into_iter()
        .map(|(key, acc)| WrittenTailGroup {
            rep_payload: (!acc.frames.is_empty()).then(|| TailAppendPayload {
                db: acc.db,
                table: acc.table,
                partition: acc.partition,
                memtable_id: acc.memtable_id,
                first_lsn: acc.first_lsn,
                last_lsn: acc.last_lsn,
                frames: acc.frames,
            }),
            key,
            completions: acc.completions,
            bytes: acc.bytes,
        })
        .collect()
}

/// Accumulated state of one group's appends to one tail file.
struct TailGroupAccumulator {
    db: String,
    table: String,
    partition: String,
    memtable_id: Uuid,
    completions: Vec<Arc<CommitCompletion>>,
    bytes: usize,
    frames: Vec<u8>,
    first_lsn: u64,
    last_lsn: u64,
}

/// `<data>/<db>/<table>` tail dir -> `(db, table)`.
fn db_table_from_dir(table_dir: &Path) -> (String, String) {
    let table = table_dir
        .file_name()
        .map(|name| name.to_string_lossy().to_string())
        .unwrap_or_default();
    let db = table_dir
        .parent()
        .and_then(|parent| parent.file_name())
        .map(|name| name.to_string_lossy().to_string())
        .unwrap_or_default();
    (db, table)
}

fn sync_append_group(files: &mut TailFileSet, written: Vec<WrittenTailGroup>) {
    for group in written {
        let sync_start = Instant::now();
        let outcome = files.sync(&group.key).map_err(|error| error.to_string());
        let sync_elapsed = sync_start.elapsed();
        let warn_after = tail_commit_timeouts().1;
        if sync_elapsed >= warn_after {
            tracing::warn!(
                tail = ?group.key,
                fsync_ms = sync_elapsed.as_millis() as u64,
                "durable tail fsync was slow"
            );
        }
        for completion in group.completions {
            completion.finish(outcome.clone());
        }
    }
}

fn sync_dirty_tails(
    files: &mut TailFileSet,
    dirty_tails: &mut HashSet<TailKey>,
    dirty_bytes: &mut usize,
    last_sync: &mut Instant,
) {
    for key in std::mem::take(dirty_tails) {
        if let Err(error) = files.sync(&key) {
            tracing::error!(
                tail = ?key,
                error = %error,
                "deferred durable tail fsync failed"
            );
        }
    }
    *dirty_bytes = 0;
    *last_sync = Instant::now();
}

fn sync_dirty_tails_for_table(
    files: &mut TailFileSet,
    dirty_tails: &mut HashSet<TailKey>,
    table_dir: &Path,
) {
    let keys: Vec<TailKey> = dirty_tails
        .iter()
        .filter(|key| key.table_dir == table_dir)
        .cloned()
        .collect();
    for key in keys {
        dirty_tails.remove(&key);
        if let Err(error) = files.sync(&key) {
            tracing::error!(
                tail = ?key,
                error = %error,
                "durable tail fsync before barrier evict failed"
            );
        }
    }
}

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
    /// Delete sidecar: positions to remove from the tail generations BELOW
    /// it. Appended by DELETE/REPLACE so removing memtable rows costs
    /// O(affected rows) instead of rewriting the whole tail snapshot (which
    /// made repeated small deletes quadratic in the memtable size).
    Delete,
    /// Update sidecar: matched row positions plus the constant value each
    /// assigned column takes on them. Appended by UPDATE so changing memtable
    /// rows costs O(affected rows) instead of a whole-tail rewrite (UPDATE
    /// never changes row count or row order, so positions stay stable under
    /// it — the property that makes the sidecar chain replayable).
    Update,
}

/// The two sidecar kinds that can sit above a live tail.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SidecarKind {
    Delete,
    Update,
}

/// A sidecar generation seen above the live tail, in walk order
/// (newest-first). Callers decode and apply them oldest-first.
#[derive(Clone)]
struct SidecarFile {
    kind: SidecarKind,
    path: PathBuf,
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
    let payload = encode_tail_log(partition_key, batch)?;
    let stem = format!("{}.{}", id.simple(), sequence);
    let tmp_path = tail_dir.join(format!(".{stem}.tmp"));
    let final_path = tail_dir.join(format!("{stem}.tail"));

    write_new_synced(&tmp_path, &payload)?;
    fs::rename(&tmp_path, &final_path)?;
    // The rename must be durable BEFORE the older generation is removed,
    // otherwise a crash on a filesystem that has not yet persisted the
    // rename's directory entry leaves no tail at all — every acknowledged
    // row of this memtable would vanish on recovery. Failure to fsync a
    // directory (not supported everywhere) degrades to the previous
    // behavior rather than failing the write.
    sync_dir_best_effort(&tail_dir);
    remove_other_generations(&tail_dir, id, &final_path)?;
    Ok(())
}

/// Best-effort `fsync` of a directory entry so a completed rename survives a
/// crash before the old generation is removed. Unix needs this for
/// durability; Windows either no-ops or refuses, which we tolerate.
fn sync_dir_best_effort(dir: &Path) {
    let result = fs::File::open(dir).and_then(|handle| handle.sync_all());
    if let Err(error) = result {
        tracing::debug!(
            dir = %dir.display(),
            error = %error,
            "directory fsync unsupported; rename durability relies on the filesystem journal"
        );
    }
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
    let frame = encode_tail_frame(delta)?;

    if let Some(path) = existing {
        if read_tail_log_partition_key(&path)?.as_deref() == Some(partition_key) {
            let mut file = fs::OpenOptions::new().append(true).open(&path)?;
            file.write_all(&frame)?;
            file.sync_all()?;
            return Ok(());
        }

        return Err(TaibaiError::Storage(
            "unsupported durable tail version".into(),
        ));
    }

    let sequence = next_sequence();
    let stem = format!("{}.{}", id.simple(), sequence);
    let tmp_path = tail_dir.join(format!(".{stem}.tmp"));
    let final_path = tail_dir.join(format!("{stem}.tail"));
    write_new_synced(&tmp_path, &encode_tail_log(partition_key, delta)?)?;
    fs::rename(&tmp_path, &final_path)?;
    sync_dir_best_effort(&tail_dir);
    Ok(())
}

const DELETE_GEN_MAGIC: u32 = 0x5441_4944; // "TAID"

fn encode_delete_positions(positions: &[u32]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(8 + positions.len() * 4);
    bytes.extend_from_slice(&DELETE_GEN_MAGIC.to_le_bytes());
    bytes.extend_from_slice(&(positions.len() as u32).to_le_bytes());
    for position in positions {
        bytes.extend_from_slice(&position.to_le_bytes());
    }
    append_crc(bytes)
}

fn decode_delete_positions(bytes: &[u8]) -> Result<Vec<u32>> {
    let body = verify_crc(bytes)?;
    if body.len() < 8 || u32::from_le_bytes(body[0..4].try_into().unwrap()) != DELETE_GEN_MAGIC {
        return Err(TaibaiError::Storage(
            "durable tail delete sidecar is corrupt".into(),
        ));
    }
    let count = u32::from_le_bytes(body[4..8].try_into().unwrap()) as usize;
    if body.len() != 8 + count * 4 {
        return Err(TaibaiError::Storage(
            "durable tail delete sidecar length mismatch".into(),
        ));
    }
    Ok(body[8..]
        .chunks_exact(4)
        .map(|chunk| u32::from_le_bytes(chunk.try_into().unwrap()))
        .collect())
}

const UPDATE_GEN_MAGIC: u32 = 0x5441_4955; // "TAIU"

/// One memtable UPDATE's durable image: the ascending tail-row positions a
/// statement matched, and the constant value each assigned column takes on
/// those rows (UPDATE assignments are statement constants, evaluated without
/// row context). Applied to the tail generations BELOW the sidecar, replayed
/// oldest sidecar first, exactly like the delete sidecars it shares the
/// generation arbitration with.
#[derive(Debug, Clone)]
pub(crate) struct TailUpdateSet {
    /// `(physical column index, new value)` pairs in the tail batch's column
    /// space, ascending by column index.
    pub assigned: Vec<(usize, ScalarValue)>,
    /// Ascending, deduplicated row positions in the sidecar's coordinate
    /// space: the batch state produced by applying every OLDER generation.
    pub positions: Vec<u32>,
}

fn read_le_u32(body: &[u8], offset: &mut usize) -> Result<u32> {
    if body.len() < *offset + 4 {
        return Err(TaibaiError::Storage(
            "durable tail update sidecar is truncated".into(),
        ));
    }
    let value = u32::from_le_bytes(body[*offset..*offset + 4].try_into().unwrap());
    *offset += 4;
    Ok(value)
}

fn read_len_prefixed<'a>(body: &'a [u8], offset: &mut usize) -> Result<&'a [u8]> {
    let len = read_le_u32(body, offset)? as usize;
    if body.len() < *offset + len {
        return Err(TaibaiError::Storage(
            "durable tail update sidecar is truncated".into(),
        ));
    }
    let slice = &body[*offset..*offset + len];
    *offset += len;
    Ok(slice)
}

/// One assigned cell on disk: `[u8 dtype][u8 null][payload]`, payload absent
/// when the null flag is set and length-prefixed for the variable-width
/// types. Fixed-width payload layout follows [`ScalarValue`]'s inner repr.
fn encode_scalar_cell(bytes: &mut Vec<u8>, value: &ScalarValue) {
    bytes.push(value.data_type() as u8);
    if value.is_null() {
        bytes.push(1u8);
        return;
    }
    bytes.push(0u8);
    match value {
        ScalarValue::Bool(Some(v)) => bytes.push(*v as u8),
        ScalarValue::Int8(Some(v)) => bytes.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::Int16(Some(v)) => bytes.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::Int32(Some(v)) => bytes.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::Int64(Some(v)) => bytes.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::UInt8(Some(v)) => bytes.push(*v),
        ScalarValue::UInt16(Some(v)) => bytes.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::UInt32(Some(v)) => bytes.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::UInt64(Some(v)) => bytes.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::Float32(Some(v)) => bytes.extend_from_slice(&v.to_bits().to_le_bytes()),
        ScalarValue::Float64(Some(v)) => bytes.extend_from_slice(&v.to_bits().to_le_bytes()),
        ScalarValue::Decimal(Some(v)) => bytes.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::Date(Some(v)) => bytes.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::Time(Some(v)) => bytes.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::Timestamp(Some(v)) => bytes.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::Interval(Some(v)) => bytes.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::VarChar(Some(v)) => {
            bytes.extend_from_slice(&(v.len() as u32).to_le_bytes());
            bytes.extend_from_slice(v.as_bytes());
        }
        ScalarValue::Blob(Some(v)) => {
            bytes.extend_from_slice(&(v.len() as u32).to_le_bytes());
            bytes.extend_from_slice(v);
        }
        ScalarValue::Vector(Some(v)) => {
            bytes.extend_from_slice(&(v.len() as u32).to_le_bytes());
            for element in v {
                bytes.extend_from_slice(&element.to_bits().to_le_bytes());
            }
        }
        // Nulls returned early above; the None arms are unreachable.
        _ => unreachable!("non-null scalar cell matched a None payload"),
    }
}

fn decode_scalar_cell(body: &[u8], offset: &mut usize) -> Result<ScalarValue> {
    if body.len() < *offset + 2 {
        return Err(TaibaiError::Storage(
            "durable tail update sidecar is truncated".into(),
        ));
    }
    let data_type = DataType::from_u8(body[*offset])?;
    *offset += 1;
    let is_null = body[*offset] == 1;
    *offset += 1;
    if is_null {
        return Ok(ScalarValue::null_for_type(data_type));
    }
    // Fixed-width payloads are raw little-endian bytes; variable-width ones
    // are u32-length-prefixed (mirroring encode_scalar_cell exactly).
    let value = match data_type {
        DataType::Bool => ScalarValue::Bool(Some(read_raw(body, offset, 1)?[0] != 0)),
        DataType::Int8 => ScalarValue::Int8(Some(read_raw(body, offset, 1)?[0] as i8)),
        DataType::Int16 => ScalarValue::Int16(Some(i16::from_le_bytes(
            read_raw(body, offset, 2)?.try_into().unwrap(),
        ))),
        DataType::Int32 => ScalarValue::Int32(Some(i32::from_le_bytes(
            read_raw(body, offset, 4)?.try_into().unwrap(),
        ))),
        DataType::Int64 => ScalarValue::Int64(Some(i64::from_le_bytes(
            read_raw(body, offset, 8)?.try_into().unwrap(),
        ))),
        DataType::UInt8 => ScalarValue::UInt8(Some(read_raw(body, offset, 1)?[0])),
        DataType::UInt16 => ScalarValue::UInt16(Some(u16::from_le_bytes(
            read_raw(body, offset, 2)?.try_into().unwrap(),
        ))),
        DataType::UInt32 => ScalarValue::UInt32(Some(u32::from_le_bytes(
            read_raw(body, offset, 4)?.try_into().unwrap(),
        ))),
        DataType::UInt64 => ScalarValue::UInt64(Some(u64::from_le_bytes(
            read_raw(body, offset, 8)?.try_into().unwrap(),
        ))),
        DataType::Float32 => ScalarValue::Float32(Some(f32::from_bits(u32::from_le_bytes(
            read_raw(body, offset, 4)?.try_into().unwrap(),
        )))),
        DataType::Float64 => ScalarValue::Float64(Some(f64::from_bits(u64::from_le_bytes(
            read_raw(body, offset, 8)?.try_into().unwrap(),
        )))),
        DataType::Decimal => ScalarValue::Decimal(Some(i64::from_le_bytes(
            read_raw(body, offset, 8)?.try_into().unwrap(),
        ))),
        DataType::Date => ScalarValue::Date(Some(i32::from_le_bytes(
            read_raw(body, offset, 4)?.try_into().unwrap(),
        ))),
        DataType::Time => ScalarValue::Time(Some(i64::from_le_bytes(
            read_raw(body, offset, 8)?.try_into().unwrap(),
        ))),
        DataType::Timestamp => ScalarValue::Timestamp(Some(i64::from_le_bytes(
            read_raw(body, offset, 8)?.try_into().unwrap(),
        ))),
        DataType::Interval => ScalarValue::Interval(Some(i64::from_le_bytes(
            read_raw(body, offset, 8)?.try_into().unwrap(),
        ))),
        DataType::VarChar => ScalarValue::VarChar(Some(String::from_utf8(
            read_len_prefixed(body, offset)?.to_vec(),
        )
        .map_err(|_| TaibaiError::Storage("update sidecar VarChar is not UTF-8".into()))?)),
        DataType::Blob => ScalarValue::Blob(Some(read_len_prefixed(body, offset)?.to_vec())),
        DataType::Vector => {
            let count = read_le_u32(body, offset)? as usize;
            if body.len() < *offset + count * 4 {
                return Err(TaibaiError::Storage(
                    "durable tail update sidecar is truncated".into(),
                ));
            }
            let elements = body[*offset..*offset + count * 4]
                .chunks_exact(4)
                .map(|chunk| f32::from_bits(u32::from_le_bytes(chunk.try_into().unwrap())))
                .collect();
            *offset += count * 4;
            ScalarValue::Vector(Some(elements))
        }
    };
    Ok(value)
}

/// A raw fixed-width payload read that verifies the exact width, so a
/// corrupt-but-CRC-bearing file fails loudly instead of misreading bytes.
fn read_raw<'a>(body: &'a [u8], offset: &mut usize, width: usize) -> Result<&'a [u8]> {
    if body.len() < *offset + width {
        return Err(TaibaiError::Storage(
            "durable tail update sidecar is truncated".into(),
        ));
    }
    let slice = &body[*offset..*offset + width];
    *offset += width;
    Ok(slice)
}

fn encode_update_set(set: &TailUpdateSet) -> Result<Vec<u8>> {
    let mut bytes = Vec::with_capacity(16 + set.assigned.len() * 24 + set.positions.len() * 4);
    bytes.extend_from_slice(&UPDATE_GEN_MAGIC.to_le_bytes());
    bytes.extend_from_slice(&(set.assigned.len() as u32).to_le_bytes());
    for (col_idx, value) in &set.assigned {
        let col_idx = u32::try_from(*col_idx).map_err(|_| {
            TaibaiError::Storage("update sidecar column index overflows u32".into())
        })?;
        bytes.extend_from_slice(&col_idx.to_le_bytes());
        encode_scalar_cell(&mut bytes, value);
    }
    bytes.extend_from_slice(&(set.positions.len() as u32).to_le_bytes());
    for position in &set.positions {
        bytes.extend_from_slice(&position.to_le_bytes());
    }
    Ok(append_crc(bytes))
}

fn decode_update_set(raw: &[u8]) -> Result<TailUpdateSet> {
    let body = verify_crc(raw)?;
    if body.len() < 8 || u32::from_le_bytes(body[0..4].try_into().unwrap()) != UPDATE_GEN_MAGIC {
        return Err(TaibaiError::Storage(
            "durable tail update sidecar is corrupt".into(),
        ));
    }
    let mut offset = 4usize;
    let assigned_count = read_le_u32(body, &mut offset)? as usize;
    let mut assigned = Vec::with_capacity(assigned_count);
    for _ in 0..assigned_count {
        let col_idx = read_le_u32(body, &mut offset)? as usize;
        let value = decode_scalar_cell(body, &mut offset)?;
        assigned.push((col_idx, value));
    }
    let position_count = read_le_u32(body, &mut offset)? as usize;
    if body.len() != offset + position_count * 4 {
        return Err(TaibaiError::Storage(
            "durable tail update sidecar length mismatch".into(),
        ));
    }
    let positions = body[offset..]
        .chunks_exact(4)
        .map(|chunk| u32::from_le_bytes(chunk.try_into().unwrap()))
        .collect();
    Ok(TailUpdateSet { assigned, positions })
}

/// Why a generation walk stopped without finding a live tail.
enum Suppression {
    /// A clear tombstone: the tail was explicitly dropped.
    Clear,
    /// A satisfied flush intent: the tail's rows are durable in a segment.
    Flush(PathBuf),
}

/// Result of walking one id's generations under the shared arbitration: the
/// newest usable tail wins, clear tombstones and satisfied flush intents
/// suppress everything below, delete/update sidecars are transparent.
enum LiveTail {
    Found(PathBuf),
    Suppressed(Suppression),
    Absent,
}

/// Walk `generations` (must be pre-sorted newest→oldest) applying the
/// generation arbitration shared by every live-tail resolution.
///
/// * `validate_tails` discards tail files without a usable header and keeps
///   walking (the mutation paths); read-only views skip the check.
/// * `prune_intents` removes unsatisfied flush intents encountered along
///   the walk (they prove nothing about the tail below); read-only views
///   leave them for the mutation paths.
/// * `sidecars` collects the delete/update sidecar paths seen above the
///   live tail, newest-first.
fn resolve_live_tail(
    table_dir: &Path,
    generations: &[Generation],
    validate_tails: bool,
    prune_intents: bool,
    mut sidecars: Option<&mut Vec<SidecarFile>>,
) -> LiveTail {
    for generation in generations {
        match generation.kind {
            GenerationKind::Tail => {
                if validate_tails && !tail_file_has_header(&generation.path) {
                    discard_unusable_tail(&generation.path, "header shorter than the log header");
                    continue;
                }
                return LiveTail::Found(generation.path.clone());
            }
            GenerationKind::Clear => return LiveTail::Suppressed(Suppression::Clear),
            GenerationKind::Delete | GenerationKind::Update => {
                if let Some(list) = sidecars.as_deref_mut() {
                    list.push(SidecarFile {
                        kind: match generation.kind {
                            GenerationKind::Delete => SidecarKind::Delete,
                            _ => SidecarKind::Update,
                        },
                        path: generation.path.clone(),
                    });
                }
            }
            GenerationKind::Flush => {
                // A torn or corrupt intent marker proves nothing about the
                // tail: treat it as unsatisfied so the preceding tail stays
                // authoritative.
                let satisfied = match fs::read(&generation.path) {
                    Ok(bytes) => flush_intent_satisfied(table_dir, &bytes).unwrap_or(false),
                    Err(error) => {
                        report_intent_unreadable(&generation.path, &error.to_string());
                        false
                    }
                };
                if satisfied {
                    return LiveTail::Suppressed(Suppression::Flush(generation.path.clone()));
                }
                if prune_intents {
                    let _ = fs::remove_file(&generation.path);
                }
            }
        }
    }
    LiveTail::Absent
}

/// Durably record row deletions for a memtable without rewriting its tail.
///
/// The sidecar generation (`<id>.<seq>.del`) applies to the tail below it:
/// recovery loads the tail, then removes the recorded positions oldest
/// sidecar first. Appends to the tail only extend it, so positions recorded
/// against an earlier prefix stay valid. Returns `false` when the memtable
/// has no live tail (caller falls back to a full `persist`).
pub fn delete_rows(table_dir: &Path, id: Uuid, positions: &[u32]) -> Result<bool> {
    if positions.is_empty() {
        return Ok(true);
    }
    let mut positions = positions.to_vec();
    positions.sort_unstable();
    positions.dedup();
    let tail_dir = table_dir.join(TAIL_DIR);
    if !tail_dir.exists() {
        return Ok(false);
    }
    let mut generations = generations_for_id(&tail_dir, id)?;
    generations.sort_by_key(|generation| std::cmp::Reverse(generation.sequence));
    if !matches!(
        resolve_live_tail(table_dir, &generations, true, true, None),
        LiveTail::Found(_)
    ) {
        return Ok(false);
    }
    let sequence = next_sequence();
    let tmp_path = tail_dir.join(format!(".{}.{}.del.tmp", id.simple(), sequence));
    let final_path = tail_dir.join(format!("{}.{}.del", id.simple(), sequence));
    write_new_synced(&tmp_path, &encode_delete_positions(&positions))?;
    fs::rename(&tmp_path, &final_path)?;
    // The sidecar must be durable before the statement acks the delete;
    // otherwise a crash restores rows the caller believes are gone.
    sync_dir_best_effort(&tail_dir);
    Ok(true)
}

/// Durably record a memtable UPDATE without rewriting its tail.
///
/// The sidecar generation (`<id>.<seq>.upd`) applies to the tail generations
/// BELOW it: recovery decodes the tail and replays every generation
/// oldest-first — appends extend the tail, updates overwrite values in place
/// at stable positions, deletes remove rows. Each sidecar's positions refer
/// to the batch state its statement observed, which the ordered replay
/// reproduces exactly. Repeated small UPDATEs on a large memtable therefore
/// cost O(affected rows) per statement instead of a whole-tail rewrite
/// (quadratic). Returns `false` when the memtable has no live tail (caller
/// falls back to a full `persist` of the updated batch).
pub fn update_rows(
    table_dir: &Path,
    id: Uuid,
    positions: &[u32],
    assigned: &[(usize, ScalarValue)],
) -> Result<bool> {
    if positions.is_empty() || assigned.is_empty() {
        return Ok(true);
    }
    let mut positions = positions.to_vec();
    positions.sort_unstable();
    positions.dedup();
    // Later assignments to the same column never win (the statement planner
    // already keeps only the first); normalize to ascending column order so
    // the on-disk layout is canonical.
    let mut assigned = assigned.to_vec();
    assigned.sort_by_key(|(col_idx, _)| *col_idx);
    assigned.dedup_by_key(|(col_idx, _)| *col_idx);
    let tail_dir = table_dir.join(TAIL_DIR);
    if !tail_dir.exists() {
        return Ok(false);
    }
    let mut generations = generations_for_id(&tail_dir, id)?;
    generations.sort_by_key(|generation| std::cmp::Reverse(generation.sequence));
    if !matches!(
        resolve_live_tail(table_dir, &generations, true, true, None),
        LiveTail::Found(_)
    ) {
        return Ok(false);
    }
    let set = TailUpdateSet {
        assigned,
        positions,
    };
    let sequence = next_sequence();
    let tmp_path = tail_dir.join(format!(".{}.{}.upd.tmp", id.simple(), sequence));
    let final_path = tail_dir.join(format!("{}.{}.upd", id.simple(), sequence));
    write_new_synced(&tmp_path, &encode_update_set(&set)?)?;
    fs::rename(&tmp_path, &final_path)?;
    // The sidecar must be durable before the statement acks the UPDATE;
    // otherwise a crash restores the pre-update values.
    sync_dir_best_effort(&tail_dir);
    Ok(true)
}

/// True when no sidecars pend above the memtable's live tail. A spill (which
/// hands the memtable a lazily-materialized view of the RAW tail file) must
/// not run while sidecars pend: `ensure_resident` decodes only the tail, so
/// pending delete/update generations would be silently dropped.
///
/// Not expressed through [`resolve_live_tail`]: a sidecar is terminal here
/// (answers "plain?"), not transparent as in the live-tail searches.
pub fn tail_is_plain(table_dir: &Path, id: Uuid) -> Result<bool> {
    let tail_dir = table_dir.join(TAIL_DIR);
    if !tail_dir.exists() {
        return Ok(true);
    }
    let mut generations = generations_for_id(&tail_dir, id)?;
    generations.sort_by_key(|generation| std::cmp::Reverse(generation.sequence));
    for generation in generations {
        match generation.kind {
            GenerationKind::Tail => return Ok(true),
            GenerationKind::Clear => return Ok(true),
            GenerationKind::Delete | GenerationKind::Update => return Ok(false),
            GenerationKind::Flush => {
                let satisfied = fs::read(&generation.path)
                    .ok()
                    .and_then(|bytes| flush_intent_satisfied(table_dir, &bytes).ok())
                    .unwrap_or(false);
                if satisfied {
                    return Ok(true);
                }
                let _ = fs::remove_file(&generation.path);
            }
        }
    }
    Ok(true)
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
    // Make the marker's directory entry durable before the tails it
    // tombstones are removed (same ordering rule as `persist`).
    sync_dir_best_effort(&tail_dir);
    remove_other_generations(&tail_dir, id, &marker)?;
    Ok(())
}

pub fn has_tail(table_dir: &Path, id: Uuid) -> bool {
    let Ok(mut generations) = generations_for_id(&table_dir.join(TAIL_DIR), id) else {
        return false;
    };
    generations.sort_by_key(|generation| std::cmp::Reverse(generation.sequence));
    // Read-only view: no header validation, no intent pruning.
    matches!(
        resolve_live_tail(table_dir, &generations, false, false, None),
        LiveTail::Found(_)
    )
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
        // Read-only view, same arbitration as `has_tail`.
        let has = matches!(
            resolve_live_tail(table_dir, &generations, false, false, None),
            LiveTail::Found(_)
        );
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
    let mut answers = HashMap::with_capacity(ids.len());
    for id in ids {
        let Some(mut generations) = by_id.remove(id) else {
            answers.insert(*id, None);
            continue;
        };
        generations.sort_by_key(|generation| std::cmp::Reverse(generation.sequence));
        // Read-only view, same arbitration as `live_tail_path`.
        let live = match resolve_live_tail(table_dir, &generations, false, false, None) {
            LiveTail::Found(path) => Some(path),
            _ => None,
        };
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
            archive_or_remove(&tail_dir, &path);
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
/// One pending sidecar decoded from disk, in application order.
enum DecodedSidecar {
    Delete(Vec<u32>),
    Update(TailUpdateSet),
}

fn decode_sidecar(file: &SidecarFile) -> Result<DecodedSidecar> {
    let bytes = fs::read(&file.path)?;
    match file.kind {
        SidecarKind::Delete => Ok(DecodedSidecar::Delete(decode_delete_positions(&bytes)?)),
        SidecarKind::Update => Ok(DecodedSidecar::Update(decode_update_set(&bytes)?)),
    }
}

/// Replay decoded sidecars (oldest-first) onto a decoded tail batch: deletes
/// remove rows, updates overwrite values in place. Each sidecar's positions
/// refer to the batch state produced by the generations before it, so the
/// ordered replay reproduces exactly what the statements left in memory.
fn apply_sidecars(mut batch: RecordBatch, sidecars: &[DecodedSidecar]) -> Result<RecordBatch> {
    for sidecar in sidecars {
        batch = match sidecar {
            DecodedSidecar::Delete(positions) => filter_out_positions(&batch, positions)?,
            DecodedSidecar::Update(set) => apply_update_set(&batch, set)?,
        };
    }
    Ok(batch)
}

/// Apply one update sidecar to a decoded tail batch: matched rows take the
/// assigned constant values, every other row is untouched. Uses the same
/// image constructor as the UPDATE statement itself so the recovered batch
/// cannot diverge from the one the statement swapped into memory.
fn apply_update_set(batch: &RecordBatch, set: &TailUpdateSet) -> Result<RecordBatch> {
    let mut mask = vec![false; batch.num_rows()];
    for &position in &set.positions {
        let position = position as usize;
        if position >= batch.num_rows() {
            return Err(TaibaiError::Storage(
                "durable tail update sidecar references a row outside the tail".into(),
            ));
        }
        mask[position] = true;
    }
    let all_rows: Vec<u32> = (0..batch.num_rows() as u32).collect();
    let mut columns = batch.columns.clone();
    for (col_idx, value) in &set.assigned {
        if *col_idx >= columns.len() {
            return Err(TaibaiError::Storage(format!(
                "durable tail update sidecar references column {col_idx} outside the tail"
            )));
        }
        if columns[*col_idx].data_type() != value.data_type() {
            return Err(TaibaiError::Storage(
                "durable tail update sidecar value type does not match the tail column".into(),
            ));
        }
        columns[*col_idx] =
            rebuild_update_column(&columns[*col_idx], Some(value), &all_rows, &mask)?;
    }
    RecordBatch::try_new(batch.schema.clone(), columns)
}

/// Resolve the live tail for one id, collecting the sidecars above it and
/// decoding them oldest-first. A corrupt sidecar is an error (fail closed:
/// applying a partial chain would resurrect rows or drop updates).
fn live_tail_with_sidecars(
    tail_dir: &Path,
    table_dir: &Path,
    id: Uuid,
    generations: &mut Vec<Generation>,
) -> Result<Option<(PathBuf, Vec<DecodedSidecar>)>> {
    generations.sort_by_key(|generation| std::cmp::Reverse(generation.sequence));
    let mut sidecars_rev = Vec::new();
    match resolve_live_tail(table_dir, generations, true, true, Some(&mut sidecars_rev)) {
        LiveTail::Found(path) => {
            let mut applied = Vec::with_capacity(sidecars_rev.len());
            for file in sidecars_rev.iter().rev() {
                applied.push(decode_sidecar(file)?);
            }
            Ok(Some((path, applied)))
        }
        // The tail's rows are durable in the target segment: retire the
        // id's remaining generations (sidecars preserved — see
        // `remove_other_generations_ex`) and the satisfied intent itself.
        LiveTail::Suppressed(Suppression::Flush(intent)) => {
            remove_other_generations_ex(tail_dir, id, &intent, true)?;
            remove_file_if_exists(&intent)?;
            Ok(None)
        }
        LiveTail::Suppressed(Suppression::Clear) | LiveTail::Absent => Ok(None),
    }
}

fn resolve_live_tail_paths(table_dir: &Path) -> Result<Vec<(Uuid, PathBuf, Vec<DecodedSidecar>)>> {
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
        // Seed the monotonic high-water so sequences minted after a clock
        // rollback still sort after every generation already on disk.
        TAIL_SEQUENCE_HIGH_WATER.fetch_max(generation.sequence, Ordering::AcqRel);
        by_id.entry(id).or_default().push(generation);
    }

    let mut resolved = Vec::new();
    for (id, mut generations) in by_id {
        let Some((path, sidecars)) =
            live_tail_with_sidecars(&tail_dir, table_dir, id, &mut generations)?
        else {
            continue;
        };
        // Sidecars must survive this cleanup: they are only applied (and
        // then superseded) by the caller, and a crash in between must find
        // them again on the next recovery.
        remove_other_generations_ex(&tail_dir, id, &path, true)?;
        resolved.push((id, path, sidecars));
    }
    remove_empty_dir(&tail_dir);
    Ok(resolved)
}

pub fn recover(table_dir: &Path, schema: &Schema) -> Result<Vec<RecoveredTail>> {
    let mut recovered = Vec::new();
    for (id, path, sidecars) in resolve_live_tail_paths(table_dir)? {
        let (partition_key, batch) = decode_tail_file(&path, schema)?;
        // Replay the pending sidecars oldest-first: each one's positions
        // refer to the batch state its statement observed.
        let batch = apply_sidecars(batch, &sidecars)?;
        recovered.push(RecoveredTail {
            id,
            partition_key,
            batch,
        });
    }
    Ok(recovered)
}

/// Recover tail metadata: current-format tails are indexed from frame
/// headers without decoding their payloads.
pub fn recover_index(table_dir: &Path, schema: &Schema) -> Result<Vec<RecoveredTailIndex>> {
    let mut recovered = Vec::new();
    for (id, mut path, sidecars) in resolve_live_tail_paths(table_dir)? {
        // The lazy materialization path reads only the tail file, so pending
        // sidecars must be folded into a fresh full tail HERE. The decode
        // cost is confined to memtables that restarted with unflushed
        // DELETE/UPDATE statements; plain tails stay lazy.
        if !sidecars.is_empty() {
            let (partition_key, batch) = match decode_tail_file(&path, schema) {
                Ok(decoded) => decoded,
                // A layout that predates an ALTER COLUMN (legacy data from an
                // older version) cannot fold: quarantine it — the tail and
                // sidecars stay on disk, the memtable is skipped — instead of
                // failing the whole startup or silently folding garbage.
                Err(error) if is_schema_drift_error(&error) => {
                    report_tail_schema_drift(&path, &error);
                    continue;
                }
                Err(error) => return Err(error),
            };
            let batch = apply_sidecars(batch, &sidecars)?;
            persist(table_dir, id, &partition_key, &batch)?;
            path = newest_live_tail_path(&table_dir.join(TAIL_DIR), id)?.ok_or_else(|| {
                TaibaiError::Storage("durable tail vanished after sidecar consolidation".into())
            })?;
        }
        let bytes = fs::read(&path)?;
        if bytes.len() < TAIL_LOG_HEADER_LEN as usize {
            report_tail_discarded(
                &path,
                "file shorter than the log header",
                bytes.len() as u64,
            );
            continue;
        }
        let rows = match scan_tail_log_rows(&bytes) {
            Some(rows) => rows,
            None => {
                // A crash can tear the file inside the variable-length
                // header (magic + key-length present, key bytes missing).
                // Like a sub-header file, nothing was fsynced into it, so
                // treat it as absent instead of failing the whole table's
                // recovery.
                let partition_len = u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize;
                if bytes.len() < 8usize.saturating_add(partition_len) {
                    report_tail_discarded(
                        &path,
                        "torn inside the variable-length header",
                        bytes.len() as u64,
                    );
                    continue;
                }
                return Err(TaibaiError::Storage(
                    "unsupported or corrupt durable tail version".into(),
                ));
            }
        };
        let partition_key = read_tail_log_partition_key(&path)?.ok_or_else(|| {
            TaibaiError::Storage("durable tail is missing its partition key".into())
        })?;
        recovered.push(RecoveredTailIndex {
            id,
            partition_key,
            rows,
            path,
        });
    }
    Ok(recovered)
}

/// Newest live tail file for a memtable, applying the same clear/flush-intent
/// arbitration as crash recovery. Used by the spill path.
/// Standby replication apply: append already-encoded V3 frames to the
/// memtable's tail file, creating the generation on first append. Durable
/// before returning (fsync), matching the standby's strict write semantics.
pub fn append_raw_frames(
    table_dir: &Path,
    id: Uuid,
    partition_key: &str,
    frames: &[u8],
) -> Result<()> {
    let tail_dir = table_dir.join(TAIL_DIR);
    fs::create_dir_all(&tail_dir)?;
    if let Some(path) = live_tail_path(table_dir, id)? {
        let mut file = fs::OpenOptions::new().append(true).open(&path)?;
        file.write_all(frames)?;
        file.sync_all()?;
        return Ok(());
    }
    let sequence = next_sequence();
    let stem = format!("{}.{}", id.simple(), sequence);
    let tmp_path = tail_dir.join(format!(".{stem}.tmp"));
    let final_path = tail_dir.join(format!("{stem}.tail"));
    let mut file = fs::OpenOptions::new()
        .create_new(true)
        .append(true)
        .open(&tmp_path)?;
    file.write_all(&encode_tail_log_header(partition_key))?;
    file.write_all(frames)?;
    file.sync_all()?;
    drop(file);
    fs::rename(&tmp_path, &final_path)?;
    Ok(())
}

/// Sum of the `rows` fields across V3 frames in a buffer (header walk only).
pub fn count_v3_frame_rows(frames: &[u8]) -> u64 {
    let mut rows = 0u64;
    let mut offset = 0usize;
    while offset + 32 <= frames.len() {
        let payload_len =
            u64::from_le_bytes(frames[offset..offset + 8].try_into().unwrap()) as usize;
        let frame_rows = u64::from_le_bytes(frames[offset + 8..offset + 16].try_into().unwrap());
        let frame_end = offset + 32 + payload_len + 4;
        if frame_end > frames.len() {
            break;
        }
        rows = rows.saturating_add(frame_rows);
        offset = frame_end;
    }
    rows
}

pub fn live_tail_path(table_dir: &Path, id: Uuid) -> Result<Option<PathBuf>> {
    newest_live_tail_path(&table_dir.join(TAIL_DIR), id)
}

/// Row count of a current-format tail log without decompressing any frame.
/// Returns `None` when the file is invalid or corrupt.
pub fn tail_log_rows(path: &Path) -> Result<Option<u64>> {
    Ok(scan_tail_log_rows(&fs::read(path)?))
}

/// LSN of the last complete frame in a current-format tail log, again by
/// walking frame headers only (LSNs are allocated in append order, so the
/// last frame carries the maximum). `Ok(None)` for an invalid/corrupt file or
/// a V2 (LSN-less) log.
///
/// The replication applier uses this to make replay idempotent: its applied
/// watermark persists only AFTER a tail append is fsynced, so a standby crash
/// in that window replays the event — and without this check the same frames
/// would be appended a second time, doubling the tail's rows.
pub fn tail_last_lsn(path: &Path) -> Result<Option<u64>> {
    // A tail removed concurrently (compaction/flush) reads as "no lsn" —
    // the applier treats that as not-yet-applied and re-appends.
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let format = match bytes.len() {
        len if len < 8 => return Ok(None),
        _ => match TailFormat::from_magic(u32::from_le_bytes(
            bytes[0..4].try_into().unwrap(),
        )) {
            Some(format) => format,
            None => return Ok(None),
        },
    };
    if !format.has_commit_metadata() {
        // V2 frames carry no LSN: nothing to dedup on.
        return Ok(None);
    }
    let partition_len = u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize;
    let mut offset = 8usize.saturating_add(partition_len);
    if offset > bytes.len() {
        return Ok(None);
    }
    let mut last_lsn = None;
    let frame_min_len = format.frame_header_len + 4;
    while offset < bytes.len() {
        // Mirror decode_tail_log / scan_tail_log_rows: a crash-truncated
        // final frame ends the valid prefix instead of failing the scan.
        if bytes.len().saturating_sub(offset) < frame_min_len {
            break;
        }
        let payload_len =
            u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap()) as usize;
        let payload_start = offset + format.frame_header_len;
        let payload_end = payload_start.saturating_add(payload_len);
        let frame_end = payload_end.saturating_add(4);
        if frame_end > bytes.len() {
            break;
        }
        let mut hasher = Hasher::new();
        hasher.update(&bytes[offset..payload_end]);
        let stored_crc = u32::from_le_bytes(bytes[payload_end..frame_end].try_into().unwrap());
        if stored_crc != hasher.finalize() {
            return Ok(None);
        }
        last_lsn = Some(u64::from_le_bytes(
            bytes[offset + 16..offset + 24].try_into().unwrap(),
        ));
        offset = frame_end;
    }
    Ok(last_lsn)
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

fn encode_tail_log(partition_key: &str, batch: &RecordBatch) -> Result<Vec<u8>> {
    let mut bytes = encode_tail_log_header(partition_key);
    bytes.extend_from_slice(&encode_tail_frame(batch)?);
    Ok(bytes)
}

fn encode_tail_log_header(partition_key: &str) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(8 + partition_key.len());
    bytes.extend_from_slice(&TAIL_LOG_MAGIC_V3.to_le_bytes());
    bytes.extend_from_slice(&(partition_key.len() as u32).to_le_bytes());
    bytes.extend_from_slice(partition_key.as_bytes());
    bytes
}

fn encode_tail_frame(batch: &RecordBatch) -> Result<Vec<u8>> {
    let payload = serialize_batch_for_wal(batch)?;
    // Direct-write helpers (persist/append) allocate from the same
    // monotonic counter as the commit thread, so every durable frame
    // carries a unique, increasing LSN.
    let lsn = TAIL_LSN_HIGH_WATER.fetch_add(1, Ordering::AcqRel) + 1;
    let commit_ts_micros = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0);
    Ok(encode_tail_frame_payload(
        &payload,
        batch.num_rows() as u64,
        lsn,
        commit_ts_micros,
    ))
}

/// V3 frame layout: `[u64 payload_len][u64 rows][u64 lsn][i64 commit_ts_micros]
/// [payload][crc32]`, the CRC covering the four header fields and the payload.
/// V2 frames (`[u64 payload_len][u64 rows][payload][crc32]`) and V1 frames
/// (no `rows` field) remain decodable via the file-header magic.
fn encode_tail_frame_payload(
    payload: &[u8],
    rows: u64,
    lsn: u64,
    commit_ts_micros: i64,
) -> Vec<u8> {
    let mut frame = Vec::with_capacity(32 + payload.len());
    frame.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    frame.extend_from_slice(&rows.to_le_bytes());
    frame.extend_from_slice(&lsn.to_le_bytes());
    frame.extend_from_slice(&commit_ts_micros.to_le_bytes());
    frame.extend_from_slice(payload);
    let mut hasher = Hasher::new();
    hasher.update(&frame);
    frame.extend_from_slice(&hasher.finalize().to_le_bytes());
    frame
}

/// Per-frame metadata surfaced to recovery, replication and PITR consumers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TailFrameMeta {
    pub lsn: u64,
    pub commit_ts_micros: i64,
    pub rows: u64,
}

/// A tail log's frame layout, resolved once from the file-header magic.
/// V3 frames carry LSN/commit-timestamp fields; V2 files decode with zeros
/// there; V1 frames (no `rows` field) stay decodable through the same
/// header-length arithmetic. All version dispatch lives here.
#[derive(Debug, Clone, Copy)]
struct TailFormat {
    frame_header_len: usize,
}

impl TailFormat {
    fn from_magic(magic: u32) -> Option<Self> {
        match magic {
            TAIL_LOG_MAGIC_V3 => Some(Self {
                frame_header_len: 32,
            }),
            TAIL_LOG_MAGIC_V2 => Some(Self {
                frame_header_len: 16,
            }),
            _ => None,
        }
    }

    /// V3+ frames carry the LSN and commit timestamp in the header.
    fn has_commit_metadata(self) -> bool {
        self.frame_header_len >= 32
    }
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
    decode_tail_with_meta(bytes, schema).map(|(partition, batch, _)| (partition, batch))
}

/// Decode a tail log and surface per-frame metadata (LSN, commit timestamp).
/// V2 files decode with zero LSNs/timestamps.
fn decode_tail_with_meta(
    bytes: &[u8],
    schema: &Schema,
) -> Result<(String, RecordBatch, Vec<TailFrameMeta>)> {
    if bytes.len() < 4 {
        return Err(TaibaiError::Storage(
            "unsupported durable tail version".into(),
        ));
    }
    let magic = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
    let format = TailFormat::from_magic(magic)
        .ok_or_else(|| TaibaiError::Storage("unsupported durable tail version".into()))?;
    decode_tail_log_inner(bytes, schema, format)
}

fn decode_tail_log_inner(
    bytes: &[u8],
    schema: &Schema,
    format: TailFormat,
) -> Result<(String, RecordBatch, Vec<TailFrameMeta>)> {
    if bytes.len() < 8 {
        return Err(TaibaiError::Storage("durable tail log is truncated".into()));
    }
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
    let mut frames = Vec::new();
    let mut complete_frames = 0usize;
    while offset < bytes.len() {
        // A crash may leave the final append incomplete. All preceding frames
        // were individually synced and remain a valid recovery prefix.
        if bytes.len().saturating_sub(offset) < format.frame_header_len + 4 {
            break;
        }
        let payload_len =
            u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap()) as usize;
        let payload_start = offset + format.frame_header_len;
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
        // V2 frames have no LSN/timestamp fields; report zeros there.
        let (lsn, commit_ts_micros) = if format.has_commit_metadata() {
            (
                u64::from_le_bytes(bytes[offset + 16..offset + 24].try_into().unwrap()),
                i64::from_le_bytes(bytes[offset + 24..offset + 32].try_into().unwrap()),
            )
        } else {
            (0, 0)
        };
        frames.push(TailFrameMeta {
            lsn,
            commit_ts_micros,
            rows: batch_rows_hint(&bytes[offset..payload_start]),
        });
        complete_frames += 1;
        offset = frame_end;
    }
    if complete_frames == 0 {
        return Err(TaibaiError::Storage(
            "durable tail log contains no complete frame".into(),
        ));
    }
    Ok((partition_key, recovered, frames))
}

/// The `rows` field of a frame header (V2 and V3 share its position).
fn batch_rows_hint(frame_head: &[u8]) -> u64 {
    if frame_head.len() >= 16 {
        u64::from_le_bytes(frame_head[8..16].try_into().unwrap())
    } else {
        0
    }
}

/// Remove the given positions from a batch (columnar take, no per-cell
/// scalar round-trip). Local copy of the deferred-tombstone filter so the
/// tail codec does not depend on the transaction registry.
fn filter_out_positions(batch: &RecordBatch, positions: &[u32]) -> Result<RecordBatch> {
    let mut doomed = positions.to_vec();
    doomed.sort_unstable();
    doomed.dedup();
    if doomed.is_empty() {
        return Ok(batch.clone());
    }
    let mut keep = Vec::with_capacity(batch.num_rows().saturating_sub(doomed.len()));
    let mut doomed_iter = doomed.iter();
    let mut next = doomed_iter.next();
    for row in 0..batch.num_rows() as u32 {
        match next {
            Some(&position) if position == row => {
                next = doomed_iter.next();
            }
            _ => keep.push(row),
        }
    }
    if keep.len() == batch.num_rows() {
        return Ok(batch.clone());
    }
    let columns = batch
        .columns
        .iter()
        .map(|column| column.take(&keep))
        .collect();
    RecordBatch::try_new(batch.schema.clone(), columns)
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
    Ok(TailFormat::from_magic(magic).map(|_| magic))
}

/// Row count of a current-format tail log obtained by walking frame headers
/// only. Returns `None` when the header, frame layout, or checksum is invalid.
fn scan_tail_log_rows(bytes: &[u8]) -> Option<u64> {
    if bytes.len() < 8 {
        return None;
    }
    let magic = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
    let format = TailFormat::from_magic(magic)?;
    let frame_min_len = format.frame_header_len + 4;
    let partition_len = u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize;
    let mut offset = 8usize.saturating_add(partition_len);
    if offset > bytes.len() {
        return None;
    }
    let mut rows = 0u64;
    while offset < bytes.len() {
        // Mirror decode_tail_log: a crash-truncated final frame ends the
        // valid prefix without failing the scan.
        if bytes.len().saturating_sub(offset) < frame_min_len {
            break;
        }
        let payload_len =
            u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap()) as usize;
        let frame_rows = u64::from_le_bytes(bytes[offset + 8..offset + 16].try_into().unwrap());
        let payload_start = offset + format.frame_header_len;
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
    if TailFormat::from_magic(magic).is_none() {
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
    let expected_rows = if name_end.saturating_add(8) == body.len() {
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
/// is readable and holds at least the recorded row-count watermark.
fn flush_intent_satisfied(table_dir: &Path, bytes: &[u8]) -> Result<bool> {
    let (segment_name, expected_rows) = match decode_flush_target(bytes) {
        Ok(target) => target,
        // A torn marker proves nothing about the tail: unsatisfied, and the
        // preceding tail stays authoritative. Counted so the debris stays
        // visible in aggregate.
        Err(error) => {
            INTENTS_UNREADABLE.fetch_add(1, Ordering::Relaxed);
            tracing::debug!(
                error = %error,
                "flush intent marker undecodable; treated as unsatisfied"
            );
            return Ok(false);
        }
    };
    let segment_path = table_dir.join(segment_name);
    if !segment_path.is_file() {
        return Ok(false);
    }
    let reader = match SegmentReader::open(&segment_path) {
        Ok(reader) => reader,
        // The segment exists but is damaged: the intent legitimately stays
        // unsatisfied (the tail stays authoritative), but a damaged segment
        // is worth more than a debug line — surfaces at warn, counted.
        Err(error) => {
            report_segment_unreadable(&segment_path, &error);
            return Ok(false);
        }
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
    let wall_based = nanos.saturating_add(counter & 0xFFFF);
    // Strictly increasing under concurrency: a compare-exchange loop bumps
    // the high-water past every previously issued or observed sequence, so
    // arbitration within a memtable id can never see a tie or a regression
    // (e.g. after an NTP clock step backwards).
    let mut current = TAIL_SEQUENCE_HIGH_WATER.load(Ordering::Acquire);
    loop {
        let candidate = wall_based.max(current.saturating_add(1));
        match TAIL_SEQUENCE_HIGH_WATER.compare_exchange_weak(
            current,
            candidate,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => return candidate,
            Err(observed) => current = observed,
        }
    }
}

/// A tail file shorter than the V2 log header cannot hold even one committed
/// frame: nothing was fsynced into it, so recovery must treat it as absent
/// instead of failing the whole table's recovery. The header is
/// variable-length (magic + partition-key bytes), so both the magic and the
/// full header extent are validated against the file size.
const TAIL_LOG_HEADER_LEN: u64 = 8;

// ---------------------------------------------------------------------------
// Damage observability
// ---------------------------------------------------------------------------
//
// Recovery discards damaged durability artifacts by design: a tail torn
// before its first fsync holds no committed rows, a torn flush-intent marker
// proves nothing. The policies are correct — but they used to be silent, so
// data loss after a crash was undiscoverable. Every discard now bumps a
// counter and emits a structured event; `tail_damage_snapshot()` aggregates
// for tests and health reporting.

static TAILS_DISCARDED: AtomicU64 = AtomicU64::new(0);
static TAIL_BYTES_DISCARDED: AtomicU64 = AtomicU64::new(0);
static INTENTS_UNREADABLE: AtomicU64 = AtomicU64::new(0);
static SEGMENTS_UNREADABLE: AtomicU64 = AtomicU64::new(0);
static ROWCOUNT_MISMATCHES: AtomicU64 = AtomicU64::new(0);
static TAILS_SCHEMA_DRIFT: AtomicU64 = AtomicU64::new(0);

/// Point-in-time aggregate of durability damage seen since process start.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TailDamageSnapshot {
    /// Tail files removed because they could not hold a committed frame
    /// (torn header / truncated body before first fsync).
    pub tails_discarded: u64,
    /// Their combined size in bytes.
    pub tail_bytes_discarded: u64,
    /// Flush-intent markers whose bytes were unreadable (torn marker).
    pub intents_unreadable: u64,
    /// Named flush-target segments that exist but failed to open (damaged
    /// segment — the intent stays unsatisfied, the tail stays authoritative).
    pub segments_unreadable: u64,
    /// Spilled memtables whose decoded row count disagreed with the recorded
    /// one (the decoded content is authoritative and kept).
    pub rowcount_mismatches: u64,
    /// Durable tails whose frame layout predates an ALTER TABLE column
    /// change (data written by an older version, before width-changing DDL
    /// converged hot state). Quarantined: the file stays on disk, the
    /// memtable's rows are unavailable until the table is reloaded.
    pub tails_schema_drift: u64,
}

/// Aggregate of every durability damage event since process start.
pub fn tail_damage_snapshot() -> TailDamageSnapshot {
    TailDamageSnapshot {
        tails_discarded: TAILS_DISCARDED.load(Ordering::Relaxed),
        tail_bytes_discarded: TAIL_BYTES_DISCARDED.load(Ordering::Relaxed),
        intents_unreadable: INTENTS_UNREADABLE.load(Ordering::Relaxed),
        segments_unreadable: SEGMENTS_UNREADABLE.load(Ordering::Relaxed),
        rowcount_mismatches: ROWCOUNT_MISMATCHES.load(Ordering::Relaxed),
        tails_schema_drift: TAILS_SCHEMA_DRIFT.load(Ordering::Relaxed),
    }
}

/// Whether a tail decode error is the schema-drift signature (frames written
/// before an ALTER TABLE column change) rather than plain corruption. Only
/// drift gets the quarantine-and-continue recovery treatment; corruption
/// keeps failing closed.
pub(crate) fn is_schema_drift_error(error: &TaibaiError) -> bool {
    error.to_string().contains("column count mismatch")
}

fn report_tail_schema_drift(path: &Path, error: &TaibaiError) {
    TAILS_SCHEMA_DRIFT.fetch_add(1, Ordering::Relaxed);
    tracing::error!(
        tail = %path.display(),
        error = %error,
        "recovery quarantined a durable tail whose layout predates an ALTER TABLE column change (data written by an older version); its rows stay on disk but are unavailable — reload the table from a backup taken before the ALTER"
    );
}

/// Counted for a spilled memtable whose tail cannot materialize against the
/// current catalog because its layout predates an ALTER COLUMN.
pub(crate) fn note_tail_schema_drift(path: &Path, error: &TaibaiError) {
    report_tail_schema_drift(path, error);
}

fn report_tail_discarded(path: &Path, reason: &str, bytes: u64) {
    TAILS_DISCARDED.fetch_add(1, Ordering::Relaxed);
    TAIL_BYTES_DISCARDED.fetch_add(bytes, Ordering::Relaxed);
    tracing::warn!(
        tail = %path.display(),
        bytes,
        reason,
        "recovery discarded a tail file that could not hold a committed frame (torn before first fsync); its rows were never visible"
    );
}

fn report_intent_unreadable(path: &Path, reason: &str) {
    INTENTS_UNREADABLE.fetch_add(1, Ordering::Relaxed);
    tracing::debug!(
        intent = %path.display(),
        reason,
        "flush intent marker unreadable after crash; treated as unsatisfied and the tail stays authoritative"
    );
}

fn report_segment_unreadable(path: &Path, error: &TaibaiError) {
    SEGMENTS_UNREADABLE.fetch_add(1, Ordering::Relaxed);
    tracing::warn!(
        segment = %path.display(),
        error = %error,
        "flush intent names a segment that failed to open; the intent stays unsatisfied and the tail stays authoritative — the segment may be damaged"
    );
}

/// Counter hook for the spilled-memtable row-count correction in
/// `MemTable::ensure_resident` (which logs its own error with the details).
pub(crate) fn note_rowcount_mismatch() {
    ROWCOUNT_MISMATCHES.fetch_add(1, Ordering::Relaxed);
}

fn tail_file_has_header(path: &Path) -> bool {
    use std::io::Read;
    let mut file = match fs::File::open(path) {
        Ok(file) => file,
        Err(_) => return false,
    };
    let mut header = [0u8; TAIL_LOG_HEADER_LEN as usize];
    if file.read_exact(&mut header).is_err() {
        return false;
    }
    let magic = u32::from_le_bytes(header[0..4].try_into().unwrap());
    if TailFormat::from_magic(magic).is_none() {
        return false;
    }
    let partition_len = u32::from_le_bytes(header[4..8].try_into().unwrap()) as u64;
    fs::metadata(path)
        .map(|meta| meta.len() >= TAIL_LOG_HEADER_LEN.saturating_add(partition_len))
        .unwrap_or(false)
}

fn discard_unusable_tail(path: &Path, reason: &str) {
    let bytes = fs::metadata(path).map(|meta| meta.len()).unwrap_or(0);
    let _ = fs::remove_file(path);
    report_tail_discarded(path, reason, bytes);
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
        "del" => GenerationKind::Delete,
        "upd" => GenerationKind::Update,
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
    let table_dir = tail_dir.parent().unwrap_or(tail_dir);
    // Appending insert deltas below a delete sidecar is safe: appends only
    // extend the batch, so recorded positions keep referring to the same
    // prefix — hence deletions are not collected here.
    Ok(
        match resolve_live_tail(table_dir, &generations, true, true, None) {
            LiveTail::Found(path) => Some(path),
            _ => None,
        },
    )
}

fn remove_other_generations(tail_dir: &Path, id: Uuid, keep_path: &Path) -> Result<()> {
    remove_other_generations_ex(tail_dir, id, keep_path, false)
}

/// `preserve_sidecars` keeps delete AND update sidecars: callers that
/// already collected their contents but have not applied them yet (or
/// crashed before doing so) must find them again on the next recovery. A
/// fresh full tail written by `persist` supersedes them instead
/// (`preserve_sidecars = false`): the snapshot already contains their effect.
fn remove_other_generations_ex(
    tail_dir: &Path,
    id: Uuid,
    keep_path: &Path,
    preserve_sidecars: bool,
) -> Result<()> {
    for generation in generations_for_id(tail_dir, id)? {
        if generation.path != keep_path {
            if matches!(generation.kind, GenerationKind::Tail) {
                archive_or_remove(tail_dir, &generation.path);
            } else if matches!(
                generation.kind,
                GenerationKind::Delete | GenerationKind::Update
            ) && preserve_sidecars
            {
                continue;
            } else {
                remove_file_if_exists(&generation.path)?;
            }
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

/// Dispose of a durable-tail generation file after its rows became durable
/// in a segment. With `storage.tail_archive` enabled, `.tail` files are
/// MOVED into `<data>/wal/<db>/<table>/memtable_tail/` instead of deleted:
/// the archive is the incremental-log source for point-in-time recovery and
/// replication bootstrap. The top-level `wal` directory is reserved (never
/// scanned as a database), so archived logs stay invisible to SQL. Archive
/// failures degrade to deletion with a warning - losing an archive copy is
/// recoverable from a fresh backup, while failing the clear would resurrect
/// rows.
fn archive_or_remove(tail_dir: &Path, path: &Path) {
    dispose_tail_file(
        tail_dir,
        path,
        crate::common::config::tail_archive_enabled(),
    );
}

/// Testable core of [`archive_or_remove`]: `archive` selects between the
/// move-to-archive and delete behaviors.
fn dispose_tail_file(tail_dir: &Path, path: &Path, archive: bool) {
    let is_tail = path.extension().and_then(|value| value.to_str()) == Some("tail");
    if !is_tail || !archive {
        if let Err(error) = remove_file_if_exists(path) {
            tracing::warn!(file = %path.display(), error = %error, "tail cleanup failed");
        }
        return;
    }
    if let Some(archive_dir) = archive_dir_for(tail_dir) {
        if fs::create_dir_all(&archive_dir).is_ok() {
            let file_name = path
                .file_name()
                .map(|name| name.to_os_string())
                .unwrap_or_default();
            let destination = archive_dir.join(file_name);
            match fs::rename(path, &destination) {
                Ok(()) => return,
                Err(error) => {
                    tracing::warn!(
                        file = %path.display(),
                        error = %error,
                        "tail archive move failed; deleting instead"
                    );
                }
            }
        }
    }
    let _ = remove_file_if_exists(path);
}

/// `<data>/wal/<db>/<table>/memtable_tail` for a tail dir of
/// `<data>/<db>/<table>/memtable_tail`. `None` when the layout is
/// unexpected (custom data roots in tests).
fn archive_dir_for(tail_dir: &Path) -> Option<std::path::PathBuf> {
    let table_dir = tail_dir.parent()?;
    let table_name = table_dir.file_name()?;
    let db_dir = table_dir.parent()?;
    let db_name = db_dir.file_name()?;
    let data_dir = db_dir.parent()?;
    Some(
        data_dir
            .join("wal")
            .join(db_name)
            .join(table_name)
            .join(TAIL_DIR),
    )
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
#[path = "durable_tail_test.rs"]
mod tests;
