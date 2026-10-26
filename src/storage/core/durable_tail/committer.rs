//! Group-commit background machinery: the TailCommitter thread, append
//! grouping, sync strategy, and sequence/LSN watermarks.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use uuid::Uuid;

use crate::common::error::{Result, TaibaiError};
use crate::sql::execution::wal_helper::{
    compress_prepared_batch_for_wal_with, prepare_batch_for_wal,
};
use crate::types::batch::RecordBatch;

use super::api::*;
use super::format::*;
use super::generations::*;

/// Monotonic high-water for tail LSNs, seeded from the wall clock at thread
/// start and never allowed to regress within the process.
pub(super) static TAIL_LSN_HIGH_WATER: AtomicU64 = AtomicU64::new(0);

pub(super) static TAIL_SEQUENCE: AtomicU64 = AtomicU64::new(0);
/// Monotonic high-water for generation sequences. Wall-clock nanos alone let
/// an NTP step backwards make new generations sort before existing ones,
/// resurrecting deleted tails during arbitration. Seeded from sequences seen
/// on disk and never allowed to regress within the process.
pub(super) static TAIL_SEQUENCE_HIGH_WATER: AtomicU64 = AtomicU64::new(0);

pub(super) const GROUP_COMMIT_MAX_DELAY: Duration = Duration::from_micros(500);
/// Once the previous group held several statements the workload is
/// concurrent: widen the collection window so one storage barrier amortizes
/// over the whole in-flight burst instead of splitting into singleton fsyncs.
pub(super) const GROUP_COMMIT_CONCURRENT_DELAY: Duration = Duration::from_millis(2);
/// Async-fsync mode bounds the acknowledged-but-not-yet-fsynced window by
/// bytes and time; the commit thread also syncs after the write stream stays
/// quiet even if neither bound is reached. The quiescence period must exceed
/// the natural gap between bursts of a busy workload, otherwise every
/// inter-group gap would trigger a sync. It also scales with the observed
/// fsync cost: paying a 30ms stall for 5ms of silence is a bad trade, so the
/// effective quiet period is `max(base, 2 x moving-average sync cost)`,
/// capped at `ASYNC_FSYNC_MAX_DELAY` to respect the durability deadline.
pub(super) const ASYNC_FSYNC_MAX_DIRTY_BYTES: usize = 8 * 1024 * 1024;
pub(super) const ASYNC_FSYNC_MAX_DELAY: Duration = Duration::from_millis(200);
pub(super) const ASYNC_FSYNC_QUIESCE_DELAY: Duration = Duration::from_millis(5);
pub(super) const GROUP_COMMIT_MAX_BYTES: usize = 16 * 1024 * 1024;
pub(super) const GROUP_COMMIT_MAX_REQUESTS: usize = 128;

pub(super) fn observe_sync_cost(previous: Duration, observed: Duration) -> Duration {
    if previous.is_zero() {
        observed
    } else {
        (previous * 3 + observed) / 4
    }
}

pub(super) fn async_quiesce_delay(sync_cost: Duration) -> Duration {
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
    pub(super) fn encoded_len(&self) -> usize {
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

    pub(super) fn finish(
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

    pub(super) fn frame(&self) -> &[u8] {
        self.frame
            .as_deref()
            .expect("tail append must be compressed by commit thread")
    }
}

pub(super) type CommitOutcome = std::result::Result<(), String>;

pub(super) struct CommitCompletion {
    outcome: Mutex<Option<CommitOutcome>>,
    ready: Condvar,
}

impl CommitCompletion {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            outcome: Mutex::new(None),
            ready: Condvar::new(),
        })
    }

    pub(super) fn finish(&self, outcome: CommitOutcome) {
        let mut guard = self.outcome.lock().unwrap();
        if guard.is_none() {
            *guard = Some(outcome);
            self.ready.notify_all();
        }
    }

    pub(super) fn wait(&self) -> Result<()> {
        self.wait_bounded(Duration::from_secs(60), Duration::from_secs(10))
    }

    pub(super) fn wait_bounded(&self, timeout: Duration, warn_after: Duration) -> Result<()> {
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

pub(super) fn tail_commit_timeouts() -> (Duration, Duration) {
    let timeouts = crate::common::config::storage_timeouts();
    (
        Duration::from_secs(timeouts.tail_commit_timeout_secs.max(1)),
        Duration::from_secs(timeouts.tail_commit_warn_secs.max(1)),
    )
}

/// Upper bound on in-flight commit commands (frames are batch-sized; 8192 is
/// thousands of statements of slack before backpressure engages).
pub(super) const TAIL_COMMIT_QUEUE_CAPACITY: usize = 8192;

pub(crate) struct TailCommitTicket {
    completion: Arc<CommitCompletion>,
}

impl TailCommitTicket {
    pub(crate) fn wait(self) -> Result<()> {
        let (timeout, warn_after) = tail_commit_timeouts();
        self.completion.wait_bounded(timeout, warn_after)
    }

    /// Returns `true` when the submit already failed synchronously (queue
    /// full or committer stopped). The caller can reject the write before
    /// appending to the memtable, preventing undurable rows.
    pub(crate) fn is_immediate_failure(&self) -> bool {
        let guard = self.completion.outcome.lock().unwrap();
        matches!(guard.as_ref(), Some(Err(_)))
    }
}

pub(super) struct AppendCommand {
    table_dir: PathBuf,
    id: Uuid,
    partition_key: String,
    prepared: PreparedTailAppend,
    completion: Arc<CommitCompletion>,
}

pub(super) enum CommitCommand {
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
        // Bounded send, matching submit(): callers of barrier (DDL, DELETE
        // conflict resolution, UPDATE sidecar) may hold the table mutation
        // lock, and an unbounded blocking send on a full queue would stall
        // the whole table while the commit thread grinds through a slow
        // device. Failing the barrier is always safe — the caller either
        // retries or falls back to the segment flush protocol.
        let send_deadline = std::time::Duration::from_secs(30);
        match self.sender.send_timeout(
            CommitCommand::Barrier {
                table_dir: table_dir.to_path_buf(),
                completion: Arc::clone(&completion),
            },
            send_deadline,
        ) {
            Ok(()) => {}
            Err(crossbeam_channel::SendTimeoutError::Timeout(_)) => {
                self.queued.fetch_sub(1, Ordering::Relaxed);
                return Err(TaibaiError::Storage(format!(
                    "durable tail commit queue stalled for {send_deadline:?}; barrier abandoned"
                )));
            }
            Err(crossbeam_channel::SendTimeoutError::Disconnected(_)) => {
                self.queued.fetch_sub(1, Ordering::Relaxed);
                return Err(TaibaiError::Storage(
                    "durable tail commit thread stopped".into(),
                ));
            }
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
pub(super) struct TailKey {
    table_dir: PathBuf,
    id: Uuid,
}

pub(super) struct OpenTail {
    file: fs::File,
    path: PathBuf,
    partition_key: String,
    pending_rename: Option<PathBuf>,
}

#[derive(Default)]
pub(super) struct TailFileSet {
    open: HashMap<TailKey, OpenTail>,
    poisoned: HashSet<TailKey>,
}

impl TailFileSet {
    pub(super) fn append(&mut self, command: &AppendCommand) -> Result<TailKey> {
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

    pub(super) fn sync(&mut self, key: &TailKey) -> Result<()> {
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

    pub(super) fn evict_table(&mut self, table_dir: &Path) {
        self.open.retain(|key, _| key.table_dir != table_dir);
        self.poisoned.retain(|key| key.table_dir != table_dir);
    }
}

pub(super) enum OpenTailResult {
    Ready(OpenTail),
}

/// Create a fresh tail log generation inside `tail_dir`: mint a sequence,
/// create the hidden tmp file (header already written) and return the open
/// file plus its rename pair.
///
/// The create races components that legitimately remove the (empty) tail
/// directory during normal operation — flush cleanup after a table's last
/// live generation is consumed, and recovery readers tidying an empty dir.
/// A lost race surfaces between this function's `create_dir_all` call site
/// and the open as a transient create error (macOS reports EEXIST/EINVAL
/// there, not ENOENT), so the directory is restored and a fresh sequence
/// minted, bounded to three attempts before the last error is reported.
pub(super) fn create_tail_log_file(
    tail_dir: &Path,
    id: Uuid,
    partition_key: &str,
) -> Result<(fs::File, PathBuf, PathBuf)> {
    let mut last_error: Option<std::io::Error> = None;
    for _ in 0..3 {
        let sequence = next_sequence();
        let stem = format!("{}.{}", id.simple(), sequence);
        let tmp_path = tail_dir.join(format!(".{stem}.tmp"));
        let final_path = tail_dir.join(format!("{stem}.tail"));
        match fs::OpenOptions::new()
            .create_new(true)
            .append(true)
            .open(&tmp_path)
        {
            Ok(mut file) => {
                file.write_all(&encode_tail_log_header(partition_key))?;
                return Ok((file, tmp_path, final_path));
            }
            Err(error) => {
                last_error = Some(error);
                fs::create_dir_all(tail_dir)?;
            }
        }
    }
    let error =
        last_error.unwrap_or_else(|| std::io::Error::other("tail log create attempts exhausted"));
    Err(error.into())
}

pub(super) fn open_tail_for_group_append(command: &AppendCommand) -> Result<OpenTailResult> {
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

    let (file, tmp_path, final_path) =
        create_tail_log_file(&tail_dir, command.id, &command.partition_key)?;
    Ok(OpenTailResult::Ready(OpenTail {
        file,
        path: tmp_path,
        partition_key: command.partition_key.clone(),
        pending_rename: Some(final_path),
    }))
}

pub(super) fn unqueue(queued: &AtomicUsize, counted: bool) {
    if counted {
        queued.fetch_sub(1, Ordering::Relaxed);
    }
}

pub(super) fn tail_commit_loop(
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

                let has_rep = rep_log.get().is_some();
                let written =
                    write_append_group(&mut files, &mut compressor, group, has_rep);

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

pub(super) struct WrittenTailGroup {
    key: TailKey,
    completions: Vec<Arc<CommitCompletion>>,
    bytes: usize,
    /// Frames written for this tail in this group, for the replication
    /// event. `None` when the group wrote nothing.
    rep_payload: Option<TailAppendPayload>,
}

/// Everything the replication event needs about one group's appends to one
/// tail. db/table are derived from the table dir (`<data>/<db>/<table>`).
pub(super) struct TailAppendPayload {
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
pub(super) fn build_tail_append_events(
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

pub(super) fn write_append_group(
    files: &mut TailFileSet,
    compressor: &mut zstd::bulk::Compressor<'_>,
    group: Vec<AppendCommand>,
    has_replication: bool,
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
                if has_replication {
                    entry.frames.extend_from_slice(command.prepared.frame());
                }
                entry.last_lsn = lsn;
            }
            Err(error) => command.completion.finish(Err(error.to_string())),
        }
    }
    by_tail
        .into_iter()
        .map(|(key, acc)| WrittenTailGroup {
            rep_payload: (!acc.frames.is_empty()).then_some(TailAppendPayload {
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
pub(super) struct TailGroupAccumulator {
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
pub(super) fn db_table_from_dir(table_dir: &Path) -> (String, String) {
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

pub(super) fn sync_append_group(files: &mut TailFileSet, written: Vec<WrittenTailGroup>) {
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

pub(super) fn sync_dirty_tails(
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

pub(super) fn sync_dirty_tails_for_table(
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

pub(super) fn next_sequence() -> u64 {
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
