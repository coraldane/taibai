use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use crate::common::error::{Result, TaibaiError};
use crate::storage::core::durable_tail::{PreparedTailAppend, TailCommitTicket, TailCommitter};
use crate::storage::core::memtable::{
    align_batch_to_schema, batch_matches_schema, MemTable, MemTableManager, MemTableState,
};
use crate::storage::segment::meta::remove_sidecar_meta;
use crate::storage::segment_manager::SegmentManager;
use crate::types::batch::{RecordBatch, Schema};

const DEFAULT_MAX_ROWS_PER_MEMTABLE: u64 = 500_000;
const DEFAULT_FLUSH_INTERVAL_SECS: u64 = 30;
const DEFAULT_MAX_TOTAL_MEMORY: u64 = 512 * 1024 * 1024;
const DEFAULT_LOW_RATE_MAX_ROWS: u64 = 1024;
const DEFAULT_LOW_RATE_MAX_AGE_SECS: u64 = 3600;
const DEFAULT_SPILL_MIN_AGE_SECS: u64 = 60;
const MAX_TAIL_FILES_PER_TABLE: usize = 256;

fn insert_profile_enabled() -> bool {
    crate::common::config::insert_profile_enabled()
}

fn log_insert_profile(stage: &str, rows: usize, elapsed: std::time::Duration) {
    if insert_profile_enabled() {
        eprintln!(
            "[insert-profile] stage={} rows={} elapsed_us={}",
            stage,
            rows,
            elapsed.as_micros()
        );
    }
}

enum WriteCommand {
    Flush { db: String, table: String },
    Shutdown,
}

/// Serializes the out-of-lock half of memtable flushes per table.
///
/// The split flush freezes memtables under the partition mutation lock but
/// performs the segment append, durable-tail intents and tail cleanup after
/// releasing it, so a slow disk write no longer stalls concurrent INSERTs.
/// Writers of unrelated tables keep running in parallel. The gate preserves
/// the invariants that used to come from the partition lock: two flushes of
/// the same table (background flusher vs a writer's error-path flush or a
/// DELETE-driven `flush_partition`) cannot interleave the
/// watermark-read/begin-flush/append sequence, and DDL paths that drain or
/// clear memtable state (`clear_memtables`, `rename_memtables`) exclude
/// in-flight flush I/O.
pub(crate) struct PartitionFlushGate {
    locks: std::sync::Mutex<HashMap<(String, String), Arc<std::sync::Mutex<()>>>>,
}

impl PartitionFlushGate {
    fn new() -> Self {
        Self {
            locks: std::sync::Mutex::new(HashMap::new()),
        }
    }

    /// Return the per-table flush I/O mutex, creating it on first use. Callers
    /// lock the returned mutex for the duration of the flush's phase 2; the
    /// guard and the Arc live in the same scope, so the lock releases when the
    /// scope ends. A panicking holder must not poison the gate for every
    /// future flush, hence `into_inner` on poison.
    fn mutex_for(&self, db: &str, table: &str) -> Arc<std::sync::Mutex<()>> {
        let key = (db.to_string(), table.to_string());
        let mut locks = self.locks.lock().unwrap();
        locks
            .entry(key)
            .or_insert_with(|| Arc::new(std::sync::Mutex::new(())))
            .clone()
    }
}

pub struct BatchWriterConfig {
    pub max_rows_per_memtable: u64,
    pub flush_interval_secs: u64,
    pub max_total_memory: u64,
    pub worker_count: usize,
    pub low_rate_max_rows: u64,
    pub low_rate_max_age_secs: u64,
    /// Relieve memtable memory pressure by spilling cold committed memtables
    /// to their durable tails instead of flushing small row groups.
    pub spill_enabled: bool,
    pub spill_min_age_secs: u64,
    /// Ack durable-tail appends after the page-cache write; the tail commit
    /// thread performs the fsync for every group in the background.
    pub tail_async_fsync: bool,
}

impl Default for BatchWriterConfig {
    fn default() -> Self {
        Self {
            max_rows_per_memtable: DEFAULT_MAX_ROWS_PER_MEMTABLE,
            flush_interval_secs: DEFAULT_FLUSH_INTERVAL_SECS,
            max_total_memory: DEFAULT_MAX_TOTAL_MEMORY,
            worker_count: 1,
            low_rate_max_rows: DEFAULT_LOW_RATE_MAX_ROWS,
            low_rate_max_age_secs: DEFAULT_LOW_RATE_MAX_AGE_SECS,
            spill_enabled: true,
            spill_min_age_secs: DEFAULT_SPILL_MIN_AGE_SECS,
            tail_async_fsync: false,
        }
    }
}

/// Failure cleanup for a frozen flush group. Held across the fallible steps
/// of a flush, it accumulates the begun tail intents and, on drop — any `?`,
/// early return, or panic — cancels the intents and returns still-`Flushing`
/// members to `Pending`. Success paths either [`FlushGroupGuard::take_intents`]
/// (handing them to `finish_flush_batch`) or have already `mark_flushed` the
/// members, which makes the restore a no-op. This replaces the hand-written
/// cancel+restore pair that every failure exit used to need: one forgotten
/// copy left a memtable stuck in `Flushing` — unflushable and unrestorable —
/// until recovery.
struct FlushGroupGuard {
    memtables: Vec<Arc<crate::storage::core::memtable::MemTable>>,
    intents: Vec<crate::storage::core::durable_tail::FlushIntent>,
}

impl FlushGroupGuard {
    fn new<'m>(
        memtables: impl IntoIterator<Item = &'m Arc<crate::storage::core::memtable::MemTable>>,
    ) -> Self {
        Self {
            memtables: memtables.into_iter().cloned().collect(),
            intents: Vec::new(),
        }
    }

    fn take_intents(&mut self) -> Vec<crate::storage::core::durable_tail::FlushIntent> {
        std::mem::take(&mut self.intents)
    }
}

impl Drop for FlushGroupGuard {
    fn drop(&mut self) {
        BatchWriter::cancel_flush_intents(self.intents.drain(..));
        BatchWriter::restore_flushing_to_pending(self.memtables.iter());
    }
}

pub struct BatchWriter {
    config: BatchWriterConfig,
    memtable_mgr: Arc<MemTableManager>,
    segment_mgr: Arc<SegmentManager>,
    sender: crossbeam_channel::Sender<WriteCommand>,
    stop: Arc<AtomicBool>,
    workers: Vec<std::thread::JoinHandle<()>>,
    flush_handle: Option<std::thread::JoinHandle<()>>,
    tail_committer: Arc<TailCommitter>,
    write_epoch: Arc<AtomicU64>,
    /// Serializes out-of-lock flush I/O per table; see `PartitionFlushGate`.
    flush_gate: Arc<PartitionFlushGate>,
    /// Change-notification hub; `None` when pubsub is disabled. Publishing
    /// from commit is a lock-free `try_send` guarded by an atomic count.
    pubsub: Option<Arc<crate::pubsub::PubSubHub>>,
}

type LockedWriteOutcome = (
    u64,
    Option<(
        TailCommitTicket,
        Arc<crate::storage::core::memtable::MemTable>,
    )>,
);

/// A committed transaction whose durable tail frame has been handed to the
/// group committer but whose fsync acknowledgement is still pending. The
/// table mutation lock is released before `wait` runs, so consecutive
/// writers pipeline through one fsync group instead of serializing on it.
pub(crate) struct PendingTailCommit {
    ticket: TailCommitTicket,
    memtable: Arc<MemTable>,
}

impl BatchWriter {
    pub fn new(
        config: BatchWriterConfig,
        segment_mgr: Arc<SegmentManager>,
        pubsub: Option<Arc<crate::pubsub::PubSubHub>>,
    ) -> Self {
        let memtable_mgr = Arc::new(MemTableManager::new(
            config.max_rows_per_memtable,
            config.flush_interval_secs,
            config.low_rate_max_rows,
            config.low_rate_max_age_secs,
        ));
        let (sender, receiver) = crossbeam_channel::bounded(8192);
        let stop = Arc::new(AtomicBool::new(false));
        let receiver = Arc::new(receiver);
        let tail_committer = TailCommitter::start(config.tail_async_fsync);
        let write_epoch = Arc::new(AtomicU64::new(0));

        let worker_count = config.worker_count.max(1);
        let flush_gate = Arc::new(PartitionFlushGate::new());
        let mut workers = Vec::with_capacity(worker_count);
        for i in 0..worker_count {
            let rx = Arc::clone(&receiver);
            let stop_clone = Arc::clone(&stop);
            let mgr_clone = Arc::clone(&segment_mgr);
            let mt_mgr_clone = Arc::clone(&memtable_mgr);
            let tail_committer_clone = Arc::clone(&tail_committer);
            let write_epoch_clone = Arc::clone(&write_epoch);
            let gate_clone = Arc::clone(&flush_gate);
            let max_rows_per_memtable = config.max_rows_per_memtable;
            workers.push(
                std::thread::Builder::new()
                    .name(format!("taibai-bw-worker-{}", i))
                    .spawn(move || {
                        Self::worker_loop(
                            rx,
                            stop_clone,
                            mgr_clone,
                            mt_mgr_clone,
                            tail_committer_clone,
                            write_epoch_clone,
                            gate_clone,
                            max_rows_per_memtable,
                        );
                    })
                    .expect("failed to spawn batch writer worker"),
            );
        }

        let flush_stop = Arc::clone(&stop);
        let flush_interval = config.flush_interval_secs;
        let flush_memory_high_water = config.max_total_memory.max(1);
        let flush_spill_enabled = config.spill_enabled;
        let flush_spill_min_age_secs = config.spill_min_age_secs;
        let flush_mgr = Arc::clone(&segment_mgr);
        let flush_mt_mgr = Arc::clone(&memtable_mgr);
        let flush_tail_committer = Arc::clone(&tail_committer);
        let flush_gate_for_thread = Arc::clone(&flush_gate);
        let flush_handle = std::thread::Builder::new()
            .name("taibai-bw-flush".into())
            .spawn(move || {
                while !flush_stop.load(Ordering::Acquire) {
                    for _ in 0..flush_interval {
                        if flush_stop.load(Ordering::Acquire) {
                            break;
                        }
                        if flush_mt_mgr.total_estimated_memory_bytes() >= flush_memory_high_water {
                            // Spilling cold committed memtables to their
                            // durable tails is the first relief valve: it
                            // frees memory without cutting small row groups.
                            if flush_spill_enabled {
                                let spilled = Self::spill_pressure_pass(
                                    &flush_mgr,
                                    &flush_mt_mgr,
                                    &flush_tail_committer,
                                    flush_memory_high_water,
                                    flush_spill_min_age_secs,
                                );
                                if spilled > 0 {
                                    tracing::info!(
                                        spilled,
                                        "memory pressure relieved by spilling memtables to durable tails"
                                    );
                                }
                            }
                            if flush_mt_mgr.total_estimated_memory_bytes()
                                >= flush_memory_high_water
                            {
                                if let Err(e) = Self::flush_all_pending(
                                    &flush_mgr,
                                    &flush_mt_mgr,
                                    &flush_tail_committer,
                                    &flush_gate_for_thread,
                                ) {
                                    tracing::warn!(error = %e, "pressure memtable flush failed");
                                }
                            }
                        }
                        if let Err(error) = Self::flush_tail_file_pressure(
                            &flush_mgr,
                            &flush_mt_mgr,
                            &flush_tail_committer,
                            &flush_gate_for_thread,
                            MAX_TAIL_FILES_PER_TABLE,
                        ) {
                            tracing::warn!(
                                error = %error,
                                "durable tail file pressure flush failed"
                            );
                        }
                        std::thread::sleep(std::time::Duration::from_secs(1));
                    }
                    if flush_stop.load(Ordering::Acquire) {
                        break;
                    }
                    // Flush due memtables every interval regardless of write
                    // activity. Gating this on "no writes since the last
                    // flush" starved tail->segment conversion during
                    // sustained imports and let tail files pile up in the
                    // memtable_tail directory.
                    if let Err(e) = Self::flush_pending_memtables(
                        &flush_mgr,
                        &flush_mt_mgr,
                        &flush_tail_committer,
                        &flush_gate_for_thread,
                    ) {
                        tracing::warn!(error = %e, "periodic memtable flush failed");
                    }
                }
            })
            .expect("failed to spawn flush thread");

        Self {
            config,
            memtable_mgr,
            segment_mgr,
            sender,
            stop,
            workers,
            flush_handle: Some(flush_handle),
            tail_committer,
            write_epoch,
            flush_gate,
            pubsub,
        }
    }

    pub fn memtable_manager(&self) -> &Arc<MemTableManager> {
        &self.memtable_mgr
    }

    #[cfg(test)]
    pub(crate) fn flush_gate_for_test(&self) -> &PartitionFlushGate {
        &self.flush_gate
    }

    /// Block until any in-flight split flush (phase 2) of `table` completes.
    /// Callers get the old pre-split guarantee back: once a synchronous
    /// `flush_*` entry point returns, every row written before the call is in
    /// a segment (or still in a Pending memtable this call will drain), never
    /// half-way through a background flush.
    fn wait_out_table_flush_io(&self, db: &str, table: &str) {
        let io_mutex = self.flush_gate.mutex_for(db, table);
        let _gate = io_mutex.lock().unwrap_or_else(|p| p.into_inner());
    }

    pub fn segment_manager(&self) -> &Arc<SegmentManager> {
        &self.segment_mgr
    }

    /// Drain all earlier durable-tail appends and close cached file handles for
    /// a table. DDL calls this while holding the table mutation lock before a
    /// Windows directory rename or removal.
    pub(crate) fn drain_table_tail(&self, db: &str, table: &str) -> Result<()> {
        // Wait out any in-flight split flush first: phase 2 keeps segment
        // file handles open, and this drain runs before a Windows directory
        // rename or removal that would fail against open handles.
        let io_mutex = self.flush_gate.mutex_for(db, table);
        let _io_gate = io_mutex.lock().unwrap_or_else(|p| p.into_inner());
        self.tail_committer
            .barrier(&self.segment_mgr.table_dir(db, table))
    }

    pub fn write_columns(
        &self,
        db: &str,
        table: &str,
        partition_key: &str,
        batch: RecordBatch,
        schema: Schema,
        hot_fed: bool,
    ) -> Result<u64> {
        self.write_columns_with_txn(db, table, partition_key, batch, schema, 0, hot_fed)
    }

    pub fn write_columns_with_txn(
        &self,
        db: &str,
        table: &str,
        partition_key: &str,
        batch: RecordBatch,
        schema: Schema,
        txn_id: u64,
        hot_fed: bool,
    ) -> Result<u64> {
        let row_count = batch.num_rows() as u64;
        if row_count == 0 {
            return Ok(0);
        }
        if txn_id == 0 {
            self.write_epoch.fetch_add(1, Ordering::Release);
        }
        let prepared = (txn_id == 0)
            .then(|| PreparedTailAppend::new(&batch))
            .transpose()?;
        let lock_start = std::time::Instant::now();
        let (rows, pending) =
            self.segment_mgr
                .with_partition_mutation_lock(db, table, partition_key, || {
                    self.write_columns_grouped_under_table_lock(
                        db,
                        table,
                        partition_key,
                        batch,
                        schema,
                        txn_id,
                        prepared,
                        hot_fed,
                    )
                })?;
        // Profile-gated like the other per-statement instrumentation: this
        // used to be an unconditional info!, spamming the log and paying the
        // elapsed/formatting cost on every single-row INSERT.
        if crate::common::config::insert_profile_enabled() {
            tracing::info!(
                table = %table,
                rows,
                lock_ms = lock_start.elapsed().as_millis() as u64,
                "insert lock timing"
            );
        }
        self.settle_pending_tail_commit(db, table, pending)?;
        Ok(rows)
    }

    #[allow(clippy::too_many_arguments)]
    fn write_columns_grouped_under_table_lock(
        &self,
        db: &str,
        table: &str,
        partition_key: &str,
        batch: RecordBatch,
        schema: Schema,
        txn_id: u64,
        prepared: Option<PreparedTailAppend>,
        hot_fed: bool,
    ) -> Result<LockedWriteOutcome> {
        let row_count = batch.num_rows() as u64;
        if !self.segment_mgr.table_dir(db, table).is_dir() {
            return Err(TaibaiError::Sql(format!(
                "table '{}.{}' does not exist",
                db, table
            )));
        }
        let get_start = Instant::now();
        let mt =
            self.memtable_mgr
                .get_or_create_with_txn(db, table, partition_key, &schema, txn_id);
        // An in-flight statement can plan against the pre-ALTER schema and
        // reach this lock after a width-changing ALTER published the new
        // layout: align the batch — added columns take their DEFAULT — so
        // the memtable, its durable tail and crash recovery all see one
        // width. The eager tail frame is rebuilt in this rare path; the
        // encoding cost moves under the lock only for the raced statement.
        let (batch, prepared) = if !batch_matches_schema(&batch, &mt.schema()) {
            let aligned = align_batch_to_schema(&batch, &mt.schema())?;
            let prepared = if txn_id == 0 {
                Some(PreparedTailAppend::new(&aligned)?)
            } else {
                prepared
            };
            (aligned, prepared)
        } else {
            (batch, prepared)
        };
        log_insert_profile("memtable_get", row_count as usize, get_start.elapsed());
        let append_start = Instant::now();
        let insert_bytes = (txn_id > 0).then(|| batch.estimated_size_bytes() as u64);
        let pending = if txn_id == 0 {
            // `PreparedTailAppend` already owns the durable representation of
            // this delta, so the materialized column buffers can move into the
            // memtable. Borrowing here cloned every column once per INSERT.
            mt.append_owned(batch)?;
            let prepared = prepared.ok_or_else(|| {
                TaibaiError::Internal("missing prepared durable tail append".into())
            })?;
            let ticket = self.tail_committer.submit(
                self.segment_mgr.table_dir(db, table),
                mt.id(),
                partition_key.to_string(),
                prepared,
            );
            Some((ticket, Arc::clone(&mt)))
        } else {
            mt.append_owned(batch)?;
            None
        };
        if let Some(insert_bytes) = insert_bytes {
            self.charge_txn_insert_bytes(txn_id, db, table, insert_bytes)?;
        }
        log_insert_profile(
            "memtable_append",
            row_count as usize,
            append_start.elapsed(),
        );
        // Mirror accounting: auto-commit rows appended without a hot-store
        // feed make the partition unprovable for the resident fast path.
        // Transaction deltas are accounted at COMMIT instead.
        if txn_id == 0 && !hot_fed {
            self.memtable_mgr
                .note_hot_unfed_rows(db, table, partition_key, row_count);
        }
        if txn_id == 0 && mt.row_count() >= self.config.max_rows_per_memtable {
            let _ = self.sender.try_send(WriteCommand::Flush {
                db: db.to_string(),
                table: table.to_string(),
            });
        }
        Ok((row_count, pending))
    }

    /// Entry for callers that already hold the table mutation lock (REPLACE,
    /// delayed UPDATE materialization). Same protocol as
    /// [`Self::write_columns_with_txn`]: the durable-tail delta is prepared
    /// before the memtable append and the group committer owns it afterwards
    /// — this path used to append the tail synchronously and fall back to a
    /// flush on failure, which kept a second copy of the insert protocol.
    pub(crate) fn write_columns_with_txn_under_table_lock(
        &self,
        db: &str,
        table: &str,
        partition_key: &str,
        batch: RecordBatch,
        schema: Schema,
        txn_id: u64,
        hot_fed: bool,
    ) -> Result<u64> {
        let row_count = batch.num_rows() as u64;
        if row_count == 0 {
            return Ok(0);
        }
        if txn_id == 0 {
            self.write_epoch.fetch_add(1, Ordering::Release);
        }
        let prepared = (txn_id == 0)
            .then(|| PreparedTailAppend::new(&batch))
            .transpose()?;
        let (rows, pending) = self.write_columns_grouped_under_table_lock(
            db,
            table,
            partition_key,
            batch,
            schema,
            txn_id,
            prepared,
            hot_fed,
        )?;
        self.settle_pending_tail_commit(db, table, pending)?;
        Ok(rows)
    }

    /// Wait out a submitted durable-tail append. A failed group commit falls
    /// back to flushing the target memtable, so the rows still become
    /// durable in a segment instead of living only in memory.
    fn settle_pending_tail_commit(
        &self,
        db: &str,
        table: &str,
        pending: Option<(
            TailCommitTicket,
            Arc<crate::storage::core::memtable::MemTable>,
        )>,
    ) -> Result<()> {
        if let Some((ticket, memtable)) = pending {
            if let Err(error) = ticket.wait() {
                // The tail write failed midway, so the tail file (if any was
                // created) may be torn. Route the fallback flush through the
                // segment protocol explicitly instead of re-probing the disk:
                // a torn tail would still answer `has_tail` and the durable
                // protocol would replay possibly-corrupt frames.
                memtable.set_needs_segment_flush(true);
                tracing::warn!(
                    db,
                    table,
                    error = %error,
                    "group-committed durable tail write failed; flushing target memtable via segment protocol"
                );
                Self::flush_memtable(
                    &self.segment_mgr,
                    &memtable,
                    &self.tail_committer,
                    &self.flush_gate,
                )?;
            }
        }
        Ok(())
    }

    pub(crate) fn set_rep_log(&self, log: Arc<crate::storage::replication::RepLog>) {
        log.set_tail_committer(Arc::clone(&self.tail_committer));
        self.tail_committer.set_rep_log(log);
    }

    /// Charge a transactional INSERT's buffered bytes against the
    /// transaction's in-memory budget (no-op when the registry is not wired,
    /// e.g. unit-test writers). Transactional memtables are invisible to the
    /// background relief valves until COMMIT, so this is the only bound on
    /// how much memory one open transaction can accumulate.
    fn charge_txn_insert_bytes(
        &self,
        txn_id: u64,
        db: &str,
        table: &str,
        bytes: u64,
    ) -> Result<()> {
        match self.memtable_mgr.txn_mutations() {
            Some(registry) => registry
                .charge_txn_insert_bytes(txn_id, db, table, bytes)
                .map(|_| ()),
            None => Ok(()),
        }
    }

    pub fn commit_txn(&self, txn_id: u64) -> Result<()> {
        let pending = self.commit_txn_submit_inner(txn_id, false)?;
        self.wait_pending_tail_commits(pending)
    }

    /// Re-drive durability for memtables a failed `commit_txn` left committed
    /// but unpersisted. The visibility flip inside `commit_txn` clears each
    /// memtable's txn_id, so retrying `commit_txn(txn_id)` finds no memtables
    /// and would report success with the rows still only in memory. Flushing
    /// the memtables to segments makes the rows durable even when the durable
    /// tail lost its frames. Ordinary auto-commit memtables in the same state
    /// are flushed too — harmless, the background flusher does the same.
    pub fn redrive_commit_durability(&self) -> Result<()> {
        let mts = self.memtable_mgr.committed_pending_memtables();
        for group in Self::group_memtables_for_flush(mts) {
            Self::flush_memtable_group(
                &self.segment_mgr,
                &group,
                false,
                &self.tail_committer,
                &self.flush_gate,
            )?;
        }
        Ok(())
    }

    pub(crate) fn commit_txn_submit_under_table_lock(
        &self,
        txn_id: u64,
    ) -> Result<Vec<PendingTailCommit>> {
        self.commit_txn_submit_inner(txn_id, true)
    }

    fn commit_txn_submit_inner(
        &self,
        txn_id: u64,
        table_lock_held: bool,
    ) -> Result<Vec<PendingTailCommit>> {
        let committed_mts = self.memtable_mgr.commit_txn(txn_id);
        // Publish committed transactional deltas after the visibility flip in
        // `commit_txn`. Sharing the memtable's batch via `Arc` keeps this off
        // the copy path; the fan-out thread does the encoding work.
        if let Some(pubsub) = &self.pubsub {
            if pubsub.has_any_subscribers() {
                for memtable in &committed_mts {
                    if memtable.row_count() == 0 {
                        continue;
                    }
                    match memtable.read_snapshot_arc() {
                        Some(snapshot) => {
                            pubsub.publish_shared(memtable.db(), memtable.table(), snapshot)
                        }
                        None => tracing::warn!(
                            db = %memtable.db(),
                            table = %memtable.table(),
                            "committed memtable snapshot unavailable; change notification skipped"
                        ),
                    }
                }
            }
        }
        let mut pending = Vec::new();
        for memtable in &committed_mts {
            if table_lock_held {
                if let Some(submitted) = self.submit_memtable_tail(memtable)? {
                    pending.push(submitted);
                }
            } else {
                let submitted = self.segment_mgr.with_partition_mutation_lock(
                    memtable.db(),
                    memtable.table(),
                    memtable.partition_key(),
                    || self.submit_memtable_tail(memtable),
                )?;
                if let Some(submitted) = submitted {
                    pending.push(submitted);
                }
            }
        }
        let rows_committed = committed_mts.iter().map(|mt| mt.row_count()).sum::<u64>();
        self.segment_mgr.commit_txn(txn_id)?;
        tracing::debug!(
            txn_id,
            rows_committed,
            "transaction committed to visible memtables"
        );
        Ok(pending)
    }

    fn submit_memtable_tail(&self, memtable: &Arc<MemTable>) -> Result<Option<PendingTailCommit>> {
        if memtable.state() != MemTableState::Pending
            || memtable.is_transactional()
            || memtable.visibility() != crate::storage::core::memtable::TxnVisibility::Committed
        {
            return Ok(None);
        }
        // Share the memtable's batch instead of deep-copying it, and let the
        // commit thread do the encoding: this runs under the table mutation
        // lock (COMMIT fan-out), so the lock only pays an `Arc` clone plus a
        // channel send — serialize+zstd happen in the committer thread (see
        // `PreparedTailAppend`'s design comment). An encoding failure later
        // fails this command's ticket, never the commit thread.
        let Some(batch) = memtable.read_snapshot_arc() else {
            return Ok(None);
        };
        let prepared = PreparedTailAppend::from_shared_batch(batch);
        let ticket = self.tail_committer.submit(
            self.segment_mgr.table_dir(memtable.db(), memtable.table()),
            memtable.id(),
            memtable.partition_key().to_string(),
            prepared,
        );
        Ok(Some(PendingTailCommit {
            ticket,
            memtable: Arc::clone(memtable),
        }))
    }

    pub(crate) fn wait_pending_tail_commits(&self, pending: Vec<PendingTailCommit>) -> Result<()> {
        for pending_commit in pending {
            if let Err(error) = pending_commit.ticket.wait() {
                tracing::warn!(
                    db = pending_commit.memtable.db(),
                    table = pending_commit.memtable.table(),
                    error = %error,
                    "group-committed durable tail write failed; flushing target memtable"
                );
                Self::flush_memtable(
                    &self.segment_mgr,
                    &pending_commit.memtable,
                    &self.tail_committer,
                    &self.flush_gate,
                )?;
            }
        }
        Ok(())
    }

    pub fn rollback_txn(&self, txn_id: u64) -> Result<u64> {
        let rolled_back_rows = self.memtable_mgr.rollback_txn(txn_id);
        self.segment_mgr.rollback_txn(txn_id)?;
        tracing::debug!(
            txn_id,
            rows_discarded = rolled_back_rows,
            "transaction rolled back"
        );
        Ok(rolled_back_rows)
    }

    pub fn flush_table(&self, db: &str, table: &str) -> Result<()> {
        self.wait_out_table_flush_io(db, table);
        // Transactional memtables reach segments only through COMMIT and
        // deferred-DML pins must keep their positions stable (see
        // get_flushable_by_table); flushing either would break another
        // session's transaction.
        let mts = self.memtable_mgr.get_flushable_by_table(db, table);
        for group in Self::group_memtables_for_flush(mts) {
            Self::flush_memtable_group(
                &self.segment_mgr,
                &group,
                false,
                &self.tail_committer,
                &self.flush_gate,
            )?;
        }
        Ok(())
    }

    pub fn flush_partition(&self, db: &str, table: &str, partition_key: &str) -> Result<()> {
        self.wait_out_table_flush_io(db, table);
        let mts = self
            .memtable_mgr
            .get_flushable_by_table(db, table)
            .into_iter()
            .filter(|memtable| memtable.partition_key() == partition_key)
            .collect::<Vec<_>>();
        for group in Self::group_memtables_for_flush(mts) {
            Self::flush_memtable_group(
                &self.segment_mgr,
                &group,
                false,
                &self.tail_committer,
                &self.flush_gate,
            )?;
        }
        self.memtable_mgr.remove_flushed();
        Ok(())
    }

    /// Flush one partition's memtables while the caller already holds that
    /// partition's mutation lock (or a superset like the TTL purge's
    /// partition-set lock). The held lock already excludes appenders for
    /// this partition, so the freeze needs no further synchronization;
    /// segment I/O still runs under the flush I/O gate per the split-flush
    /// contract, never under the caller's partition lock. Callers must NOT
    /// use this while holding only the wide table gate for a DIFFERENT
    /// purpose — `flush_table_under_table_lock` is the wide-gate variant.
    pub(crate) fn flush_partition_under_partition_lock(
        &self,
        db: &str,
        table: &str,
        partition_key: &str,
    ) -> Result<()> {
        self.wait_out_table_flush_io(db, table);
        let mts = self
            .memtable_mgr
            .get_flushable_by_table(db, table)
            .into_iter()
            .filter(|memtable| memtable.partition_key() == partition_key)
            .collect::<Vec<_>>();
        for group in Self::group_memtables_for_flush(mts) {
            Self::flush_memtable_group(
                &self.segment_mgr,
                &group,
                true,
                &self.tail_committer,
                &self.flush_gate,
            )?;
        }
        self.memtable_mgr.remove_flushed();
        Ok(())
    }

    pub(crate) fn flush_table_under_table_lock(&self, db: &str, table: &str) -> Result<()> {
        self.wait_out_table_flush_io(db, table);
        let mts = self.memtable_mgr.get_flushable_by_table(db, table);
        for group in Self::group_memtables_for_flush(mts) {
            Self::flush_memtable_group(
                &self.segment_mgr,
                &group,
                true,
                &self.tail_committer,
                &self.flush_gate,
            )?;
        }
        self.memtable_mgr.remove_flushed();
        Ok(())
    }

    /// Flush large committed pending tails before a row rewrite. Callers must
    /// NOT hold the table mutation lock: the flush acquires it per memtable
    /// group internally, keeping rewrite critical sections free of segment
    /// write I/O. Rows arriving after the checkpoint are handled by the
    /// normal memtable conflict path under the lock.
    pub fn checkpoint_before_rewrite(
        &self,
        db: &str,
        table: &str,
        affected_rows: usize,
    ) -> Result<bool> {
        let pending_rows = self.pending_committed_rows(db, table);
        if !Self::should_checkpoint_before_rewrite(pending_rows, affected_rows) {
            return Ok(false);
        }

        self.flush_table(db, table)?;
        tracing::info!(
            db,
            table,
            pending_rows,
            affected_rows,
            "checkpointed large pending tail before row rewrite"
        );
        Ok(true)
    }

    fn pending_committed_rows(&self, db: &str, table: &str) -> u64 {
        self.memtable_mgr
            .get_by_table(db, table)
            .into_iter()
            .filter(|memtable| {
                memtable.state() == MemTableState::Pending
                    && !memtable.is_transactional()
                    && memtable.visibility()
                        == crate::storage::core::memtable::TxnVisibility::Committed
            })
            .map(|memtable| memtable.row_count())
            .sum()
    }

    fn should_checkpoint_before_rewrite(pending_rows: u64, affected_rows: usize) -> bool {
        const MIN_REWRITE_ROWS: u64 = 4096;
        const MAX_AFFECTED_RATIO: u64 = 4;
        pending_rows >= MIN_REWRITE_ROWS
            && (affected_rows as u64).saturating_mul(MAX_AFFECTED_RATIO) < pending_rows
    }

    pub fn flush_all(&self) -> Result<()> {
        let mts = self.memtable_mgr.get_all_pending();
        for group in Self::group_memtables_for_flush(mts) {
            Self::flush_memtable_group(
                &self.segment_mgr,
                &group,
                false,
                &self.tail_committer,
                &self.flush_gate,
            )?;
        }
        self.memtable_mgr.remove_flushed();
        Ok(())
    }

    /// Flush only pending memtables that have no durable tail. Tailed
    /// memtables are restart-safe (recovered from their tails), so a routine
    /// shutdown leaves them resident instead of cutting tiny row groups.
    pub fn flush_untailed_pending(&self) -> Result<()> {
        let mts = self
            .memtable_mgr
            .get_all_pending()
            .into_iter()
            .filter(|mt| {
                // A failed tail commit also needs the segment protocol here,
                // regardless of what the (possibly torn) tail looks like.
                if mt.needs_segment_flush() {
                    return true;
                }
                let table_dir = self.segment_mgr.table_dir(mt.db(), mt.table());
                let _ = self.tail_committer.barrier(&table_dir);
                !crate::storage::core::durable_tail::has_tail(&table_dir, mt.id())
            })
            .collect();
        for group in Self::group_memtables_for_flush(mts) {
            Self::flush_memtable_group(
                &self.segment_mgr,
                &group,
                false,
                &self.tail_committer,
                &self.flush_gate,
            )?;
        }
        self.memtable_mgr.remove_flushed();
        Ok(())
    }

    pub fn clear_memtables(&self, db: &str, table: &str) -> u64 {
        // Exclude any in-flight split flush (phase 2 runs outside the table
        // mutation lock) before draining memtable and tail state.
        let io_mutex = self.flush_gate.mutex_for(db, table);
        let _io_gate = io_mutex.lock().unwrap_or_else(|p| p.into_inner());
        let table_dir = self.segment_mgr.table_dir(db, table);
        if let Err(error) = self.tail_committer.barrier(&table_dir) {
            tracing::warn!(db, table, error = %error, "failed to drain durable tail before clear");
            return 0;
        }
        let memtables = self.memtable_mgr.get_by_table(db, table);
        let cleared = self.memtable_mgr.clear_table(db, table);
        for memtable in memtables {
            let _ = crate::storage::core::durable_tail::clear(&table_dir, memtable.id());
        }
        cleared
    }

    pub fn clear_partition_memtables(&self, db: &str, table: &str, partition_key: &str) -> u64 {
        let io_mutex = self.flush_gate.mutex_for(db, table);
        let _io_gate = io_mutex.lock().unwrap_or_else(|p| p.into_inner());
        let table_dir = self.segment_mgr.table_dir(db, table);
        if let Err(error) = self.tail_committer.barrier(&table_dir) {
            tracing::warn!(db, table, error = %error, "failed to drain durable tail before partition clear");
            return 0;
        }
        let memtables = self
            .memtable_mgr
            .get_by_table(db, table)
            .into_iter()
            .filter(|memtable| memtable.partition_key() == partition_key)
            .collect::<Vec<_>>();
        let cleared = self.memtable_mgr.clear_partition(db, table, partition_key);
        for memtable in memtables {
            let _ = crate::storage::core::durable_tail::clear(&table_dir, memtable.id());
        }
        cleared
    }

    pub fn rename_memtables(&self, db: &str, old_table: &str, new_table: &str) -> u64 {
        let io_mutex = self.flush_gate.mutex_for(db, old_table);
        let _io_gate = io_mutex.lock().unwrap_or_else(|p| p.into_inner());
        let old_dir = self.segment_mgr.table_dir(db, old_table);
        if let Err(error) = self.tail_committer.barrier(&old_dir) {
            tracing::warn!(db, table = old_table, error = %error, "failed to drain durable tail before rename");
            return 0;
        }
        self.memtable_mgr.rename_table(db, old_table, new_table)
    }

    /// Persist an explicit batch as the memtable's durable tail — same guards
    /// as [`Self::persist_memtable_tail`], but the caller supplies the
    /// content. UPDATE/DELETE use this to make the post-statement batch
    /// durable BEFORE the in-memory swap: the statement's segment rewrites
    /// are already committed at that point, and a crash between the tail
    /// write and the swap must not let recovery restore the pre-statement
    /// tail next to them (resurrecting old rows).
    pub(crate) fn persist_memtable_tail_batch(
        &self,
        memtable: &crate::storage::core::memtable::MemTable,
        batch: &RecordBatch,
    ) -> Result<()> {
        if memtable.state() != MemTableState::Pending {
            return Ok(());
        }
        if memtable.is_transactional()
            || memtable.visibility() != crate::storage::core::memtable::TxnVisibility::Committed
        {
            return Ok(());
        }
        let table_dir = self.segment_mgr.table_dir(memtable.db(), memtable.table());
        // Drain the commit thread's queued appends for this table and close
        // its cached tail handles first — the same protocol as the
        // clear/rename paths. `persist` writes a fresh full-tail generation
        // and removes the older ones, so frames still queued for those
        // generations must be written (or failed) before the removal, and
        // their open handles must be gone or the Windows rename/removal
        // races the commit thread. The barrier only waits for the commit
        // thread to drain its queue — it acquires no table lock — so calling
        // it under the caller's table mutation lock cannot deadlock; a slow
        // drain surfaces as the barrier's bounded-timeout error.
        self.tail_committer.barrier(&table_dir)?;
        crate::storage::core::durable_tail::persist(
            &table_dir,
            memtable.id(),
            memtable.partition_key(),
            batch,
        )
    }

    /// Durable side of the auto-commit UPDATE's memtable pass: record the
    /// statement's matched rows and assigned constants as an incremental
    /// `.upd` sidecar (O(affected rows)) instead of rewriting the memtable's
    /// whole tail, which made repeated small UPDATEs quadratic in the
    /// memtable size. Falls back to a full `persist` of the updated batch
    /// when no live tail exists to hang the sidecar on. The in-memory batch
    /// swap remains the caller's, after this returns.
    pub(crate) fn update_memtable_tail_rows(
        &self,
        memtable: &MemTable,
        positions: &[u32],
        assigned: &[(usize, crate::types::scalar::ScalarValue)],
        updated_batch: &RecordBatch,
    ) -> Result<()> {
        if memtable.state() != MemTableState::Pending {
            return Ok(());
        }
        if memtable.is_transactional()
            || memtable.visibility() != crate::storage::core::memtable::TxnVisibility::Committed
        {
            return Ok(());
        }
        let table_dir = self.segment_mgr.table_dir(memtable.db(), memtable.table());
        // A memtable recovered from a pre-ALTER tail keeps its insert-time
        // layout; the UPDATE plan produced a CATALOG-aligned replacement.
        // The `.upd` sidecar's `assigned` column indices are catalog-space,
        // but recovery replays them against the OLD-layout tail file — a
        // positional mismatch that would corrupt a neighboring column. Skip
        // the incremental sidecar for drifted layouts and persist the full
        // aligned batch as a fresh tail generation instead (positions are
        // row indices and stay valid either way).
        let layouts_match = memtable
            .read_snapshot_arc()
            .is_some_and(|current| current.schema == updated_batch.schema);
        if !layouts_match {
            return self.persist_memtable_tail_batch(memtable, updated_batch);
        }
        // Drain the commit thread first: the sidecar's positions may cover
        // rows whose INSERT frames are still queued for fsync. Persisting
        // the sidecar before those frames, then crashing in the window,
        // leaves recovery with a tail shorter than the sidecar references —
        // a hard "row outside the tail" error that aborts engine startup.
        // The barrier acquires no table lock (see persist_memtable_tail_batch),
        // so holding the caller's mutation lock here cannot deadlock.
        self.tail_committer.barrier(&table_dir)?;
        match crate::storage::core::durable_tail::update_rows(
            &table_dir,
            memtable.id(),
            positions,
            assigned,
        ) {
            Ok(true) => Ok(()),
            // No live tail to hang the sidecar on: the snapshot write also
            // drains the commit thread first, so in-flight INSERT frames
            // cannot land after it.
            Ok(false) => self.persist_memtable_tail_batch(memtable, updated_batch),
            Err(error) => Err(error),
        }
    }

    pub fn stop(&mut self) {
        self.stop.store(true, Ordering::Release);
        let _ = self.sender.send(WriteCommand::Shutdown);
        for handle in self.workers.drain(..) {
            let _ = handle.join();
        }
        if let Some(h) = self.flush_handle.take() {
            let _ = h.join();
        }
        self.tail_committer.stop();
    }

    fn worker_loop(
        rx: Arc<crossbeam_channel::Receiver<WriteCommand>>,
        stop: Arc<AtomicBool>,
        segment_mgr: Arc<SegmentManager>,
        memtable_mgr: Arc<MemTableManager>,
        tail_committer: Arc<TailCommitter>,
        write_epoch: Arc<AtomicU64>,
        flush_gate: Arc<PartitionFlushGate>,
        max_rows_per_memtable: u64,
    ) {
        let mut pending_tables = std::collections::HashSet::<(String, String)>::new();
        let mut last_write_epoch = write_epoch.load(Ordering::Acquire);
        let mut checkpoint_idle = false;
        loop {
            if stop.load(Ordering::Acquire) {
                break;
            }
            let command = if checkpoint_idle {
                rx.try_recv().map_err(|error| match error {
                    crossbeam_channel::TryRecvError::Empty => {
                        crossbeam_channel::RecvTimeoutError::Timeout
                    }
                    crossbeam_channel::TryRecvError::Disconnected => {
                        crossbeam_channel::RecvTimeoutError::Disconnected
                    }
                })
            } else {
                rx.recv_timeout(std::time::Duration::from_secs(1))
            };
            match command {
                Ok(WriteCommand::Flush { db, table }) => {
                    last_write_epoch = write_epoch.load(Ordering::Acquire);
                    pending_tables.remove(&(db.clone(), table.clone()));
                    if Self::checkpoint_full_memtables(
                        &segment_mgr,
                        &memtable_mgr,
                        &tail_committer,
                        &flush_gate,
                        max_rows_per_memtable,
                        &db,
                        &table,
                    ) {
                        pending_tables.insert((db, table));
                    }
                    checkpoint_idle = false;
                }
                Ok(WriteCommand::Shutdown) => break,
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                    let current_write_epoch = write_epoch.load(Ordering::Acquire);
                    if current_write_epoch != last_write_epoch {
                        last_write_epoch = current_write_epoch;
                        checkpoint_idle = false;
                        continue;
                    }

                    checkpoint_idle = true;
                    let Some((db, table)) = pending_tables.iter().next().cloned() else {
                        checkpoint_idle = false;
                        continue;
                    };
                    pending_tables.remove(&(db.clone(), table.clone()));
                    if Self::checkpoint_full_memtables(
                        &segment_mgr,
                        &memtable_mgr,
                        &tail_committer,
                        &flush_gate,
                        max_rows_per_memtable,
                        &db,
                        &table,
                    ) {
                        pending_tables.insert((db, table));
                    }
                    let after_checkpoint_epoch = write_epoch.load(Ordering::Acquire);
                    if after_checkpoint_epoch != last_write_epoch {
                        last_write_epoch = after_checkpoint_epoch;
                        checkpoint_idle = false;
                    }
                }
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
            }
        }
    }

    fn checkpoint_full_memtables(
        segment_mgr: &Arc<SegmentManager>,
        memtable_mgr: &MemTableManager,
        tail_committer: &Arc<TailCommitter>,
        flush_gate: &PartitionFlushGate,
        max_rows_per_memtable: u64,
        db: &str,
        table: &str,
    ) -> bool {
        // Collect through the shared flush choke point: a transactional
        // memtable here would publish uncommitted rows as plain segment rows
        // (ROLLBACK cannot retract them), and a deferred-DML pin must keep
        // its positions stable until that transaction COMMITs. A full
        // transactional memtable simply stays Pending until its COMMIT.
        let full = memtable_mgr
            .get_flushable_by_table(db, table)
            .into_iter()
            .filter(|mt| mt.row_count() >= max_rows_per_memtable)
            .collect::<Vec<_>>();
        if full.is_empty() {
            return false;
        }
        let mut failed = false;
        for group in Self::group_memtables_for_flush(full) {
            if let Err(error) =
                Self::flush_memtable_group(segment_mgr, &group, false, tail_committer, flush_gate)
            {
                failed = true;
                tracing::warn!(error = %error, "failed to flush memtables for {}.{}", db, table);
            }
        }
        if !failed {
            memtable_mgr.remove_flushed();
        }
        failed
    }

    fn flush_pending_memtables(
        segment_mgr: &Arc<SegmentManager>,
        memtable_mgr: &MemTableManager,
        tail_committer: &Arc<TailCommitter>,
        flush_gate: &PartitionFlushGate,
    ) -> Result<()> {
        let flushable = memtable_mgr.get_flushable();
        for group in Self::group_memtables_for_flush(flushable) {
            Self::flush_memtable_group(segment_mgr, &group, false, tail_committer, flush_gate)?;
        }
        memtable_mgr.remove_flushed();
        Ok(())
    }

    fn flush_tail_file_pressure(
        segment_mgr: &Arc<SegmentManager>,
        memtable_mgr: &MemTableManager,
        tail_committer: &Arc<TailCommitter>,
        flush_gate: &PartitionFlushGate,
        max_files_per_table: usize,
    ) -> Result<usize> {
        // get_all_pending already applies the shared flush eligibility
        // contract (non-transactional, unpinned).
        let mut tables = std::collections::HashSet::new();
        for memtable in memtable_mgr.get_all_pending() {
            tables.insert((memtable.db().to_string(), memtable.table().to_string()));
        }
        let mut flushed_tables = 0usize;
        for (db, table) in tables {
            let table_dir = segment_mgr.table_dir(&db, &table);
            let file_count = crate::storage::core::durable_tail::generation_file_count(&table_dir)?;
            if file_count < max_files_per_table {
                continue;
            }
            let memtables = memtable_mgr.get_flushable_by_table(&db, &table);
            if memtables.is_empty() {
                continue;
            }
            for group in Self::group_memtables_for_flush(memtables) {
                Self::flush_memtable_group(segment_mgr, &group, false, tail_committer, flush_gate)?;
            }
            flushed_tables += 1;
            tracing::info!(
                db,
                table,
                file_count,
                max_files_per_table,
                "forced memtable flush to bound durable tail file count"
            );
        }
        if flushed_tables > 0 {
            memtable_mgr.remove_flushed();
        }
        Ok(flushed_tables)
    }

    /// Spill cold committed memtables onto their durable tails until resident
    /// memory drops to 80% of the high-water mark, or candidates run out.
    /// Returns the number of memtables spilled. Spilling is metadata-only:
    /// the rows are already durable in the tail, so this performs no I/O
    /// beyond a tail barrier and a frame-header scan.
    fn spill_pressure_pass(
        segment_mgr: &Arc<SegmentManager>,
        memtable_mgr: &Arc<MemTableManager>,
        tail_committer: &Arc<TailCommitter>,
        high_water: u64,
        min_age_secs: u64,
    ) -> usize {
        let target = high_water.saturating_sub(high_water / 5);
        let mut candidates = memtable_mgr.spill_candidates(min_age_secs);
        candidates.sort_by_key(|mt| std::cmp::Reverse(mt.age_secs()));
        let mut by_table = Vec::<(
            (String, String),
            Vec<Arc<crate::storage::core::memtable::MemTable>>,
        )>::new();
        'outer: for mt in candidates {
            let key = (mt.db().to_string(), mt.table().to_string());
            for (existing_key, group) in by_table.iter_mut() {
                if *existing_key == key {
                    group.push(mt);
                    continue 'outer;
                }
            }
            by_table.push((key, vec![mt]));
        }
        let mut spilled = 0usize;
        for ((db, table), mts) in by_table {
            if memtable_mgr.total_estimated_memory_bytes() <= target {
                break;
            }
            let table_dir = segment_mgr.table_dir(&db, &table);
            let mut partition_keys = mts
                .iter()
                .map(|mt| mt.partition_key().to_string())
                .collect::<Vec<_>>();
            partition_keys.sort();
            partition_keys.dedup();
            let partition_refs = partition_keys
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>();
            let result =
                segment_mgr.with_partition_set_mutation_lock(&db, &table, &partition_refs, || {
                    tail_committer.barrier(&table_dir)?;
                    let ids = mts.iter().map(|mt| mt.id()).collect::<Vec<_>>();
                    let live_paths = crate::storage::core::durable_tail::live_tail_paths_batch(
                        &table_dir, &ids,
                    )?;
                    let mut count = 0usize;
                    for mt in &mts {
                        if memtable_mgr.total_estimated_memory_bytes() <= target {
                            break;
                        }
                        if Self::try_spill_memtable(
                            live_paths.get(&mt.id()).and_then(|path| path.clone()),
                            mt,
                        ) {
                            count += 1;
                        }
                    }
                    Ok(count)
                });
            match result {
                Ok(count) => spilled += count,
                Err(error) => {
                    tracing::warn!(db, table, error = %error, "memtable spill pass failed for table")
                }
            }
        }
        spilled
    }

    /// Spill one memtable after proving its durable tail covers exactly the
    /// memtable's rows. Holding the table mutation lock plus the preceding
    /// tail barrier makes that proof race-free against concurrent inserts.
    /// The caller resolves the live tail paths in one batched directory scan.
    fn try_spill_memtable(
        live_tail: Option<std::path::PathBuf>,
        mt: &Arc<crate::storage::core::memtable::MemTable>,
    ) -> bool {
        if mt.state() != MemTableState::Pending
            || mt.is_transactional()
            || mt.visibility() != crate::storage::core::memtable::TxnVisibility::Committed
        {
            return false;
        }
        let Some(path) = live_tail else {
            return false;
        };
        // A pending delete sidecar makes the raw tail exceed the memtable's
        // rows, and spilling a fresh tail under it would strand the
        // deletions (they would re-apply to unrelated rows). Refuse; the
        // normal flush path supersedes tail and sidecars together.
        let table_dir = match path.parent().and_then(|tail_dir| tail_dir.parent()) {
            Some(dir) => dir.to_path_buf(),
            None => return false,
        };
        match crate::storage::core::durable_tail::tail_is_plain(&table_dir, mt.id()) {
            Ok(true) => {}
            Ok(false) => return false,
            Err(_) => return false,
        }
        // Current-format tails expose their row count without decoding.
        let Ok(Some(tail_rows)) = crate::storage::core::durable_tail::tail_log_rows(&path) else {
            return false;
        };
        if tail_rows != mt.row_count() {
            return false;
        }
        mt.spill_to_tail(path)
    }

    /// Flush *every* pending memtable regardless of age or size. Used under
    /// memory pressure, in contrast
    /// to `flush_pending_memtables` which only drains memtables that are full or
    /// old. Transactional memtables are left untouched.
    fn flush_all_pending(
        segment_mgr: &Arc<SegmentManager>,
        memtable_mgr: &MemTableManager,
        tail_committer: &Arc<TailCommitter>,
        flush_gate: &PartitionFlushGate,
    ) -> Result<()> {
        // get_all_pending already excludes transactional and pinned
        // memtables — the shared flush eligibility contract.
        let mts = memtable_mgr.get_all_pending();
        for group in Self::group_memtables_for_flush(mts) {
            Self::flush_memtable_group(segment_mgr, &group, false, tail_committer, flush_gate)?;
        }
        memtable_mgr.remove_flushed();
        Ok(())
    }

    fn flush_memtable(
        segment_mgr: &Arc<SegmentManager>,
        mt: &Arc<crate::storage::core::memtable::MemTable>,
        tail_committer: &Arc<TailCommitter>,
        flush_gate: &PartitionFlushGate,
    ) -> Result<()> {
        // A single memtable is a length-1 group; the group protocol is the
        // single protocol generalized.
        Self::flush_memtable_group(
            segment_mgr,
            &[Arc::clone(mt)],
            false,
            tail_committer,
            flush_gate,
        )
    }

    /// Freeze a memtable while the table mutation lock is already held: the
    /// lock excludes every appender, so flipping to `Flushing` and sharing the
    /// batch needs no further synchronization.
    fn freeze_memtable_locked(
        mt: &Arc<crate::storage::core::memtable::MemTable>,
    ) -> Option<Arc<RecordBatch>> {
        if mt.state() != MemTableState::Pending {
            return None;
        }
        mt.set_state(MemTableState::Flushing);
        match mt.read_snapshot_arc() {
            Some(b) if b.num_rows() > 0 => Some(b),
            _ => {
                mt.set_state(MemTableState::Flushed);
                None
            }
        }
    }

    /// Rollback fallback: when restoring a memtable's durable tail fails, flush
    /// the memtable to segments under the already-held table lock and drop the
    /// stale tail, so crash recovery cannot replay partially-restored rows.
    pub(crate) fn flush_memtable_restore_fallback_under_table_lock(
        &self,
        memtable: &Arc<crate::storage::core::memtable::MemTable>,
    ) -> Result<()> {
        Self::flush_memtable_group(
            &self.segment_mgr,
            &[Arc::clone(memtable)],
            true,
            &self.tail_committer,
            &self.flush_gate,
        )?;
        let _ = crate::storage::core::durable_tail::clear(
            &self.segment_mgr.table_dir(memtable.db(), memtable.table()),
            memtable.id(),
        );
        Ok(())
    }

    /// Group memtables that would be flushed to the same active segment
    /// (same db/table/partition and the same transaction), preserving the
    /// original chronological order inside each group. Flushing such a group
    /// as one merged append cuts a single row group instead of one small row
    /// group per memtable (close-of-day / aging / FLUSH TABLE paths).
    fn group_memtables_for_flush(
        mts: Vec<Arc<crate::storage::core::memtable::MemTable>>,
    ) -> Vec<Vec<Arc<crate::storage::core::memtable::MemTable>>> {
        let mut groups: Vec<Vec<Arc<crate::storage::core::memtable::MemTable>>> = Vec::new();
        'outer: for mt in mts {
            for group in groups.iter_mut() {
                let first = &group[0];
                if first.db() == mt.db()
                    && first.table() == mt.table()
                    && first.partition_key() == mt.partition_key()
                    && first.txn_id() == mt.txn_id()
                {
                    group.push(mt);
                    continue 'outer;
                }
            }
            groups.push(vec![mt]);
        }
        groups
    }

    fn flush_memtable_group(
        segment_mgr: &Arc<SegmentManager>,
        group: &[Arc<crate::storage::core::memtable::MemTable>],
        table_lock_held: bool,
        tail_committer: &Arc<TailCommitter>,
        flush_gate: &PartitionFlushGate,
    ) -> Result<()> {
        let db = group[0].db().to_string();
        let table = group[0].table().to_string();
        let had_rows = group
            .iter()
            .any(|mt| mt.state() == MemTableState::Pending && mt.row_count() > 0);
        let result = (|| -> Result<()> {
            // Phase 1: freeze every still-Pending member and share its batch.
            // Under the table mutation lock (close-of-day / FLUSH TABLE) the
            // caller already excludes appenders; otherwise the partition
            // mutation lock must be taken for the freeze.
            let frozen = if table_lock_held {
                group
                    .iter()
                    .filter_map(|mt| Self::freeze_memtable_locked(mt).map(|b| (mt.clone(), b)))
                    .collect::<Vec<_>>()
            } else {
                Self::freeze_memtable_group_under_partition_lock(segment_mgr, group)?
            };
            if frozen.is_empty() {
                return Ok(());
            }
            // Phase 2: segment I/O with the mutation lock released; the gate
            // keeps same-table flushes (and DDL) serialized.
            let io_mutex = flush_gate.mutex_for(&db, &table);
            let _io_gate = io_mutex.lock().unwrap_or_else(|p| p.into_inner());
            Self::flush_frozen_memtable_group(segment_mgr, &frozen, tail_committer)
        })();
        if result.is_ok() && had_rows {
            segment_mgr.note_segments_changed(&db, &table);
        }
        result
    }

    /// Group-freeze counterpart of [`Self::freeze_memtable_under_partition_lock`]:
    /// one partition-lock acquisition flips every still-`Pending` member of
    /// the (same-partition, same-transaction) group to `Flushing` and shares
    /// its batch.
    fn freeze_memtable_group_under_partition_lock(
        segment_mgr: &Arc<SegmentManager>,
        group: &[Arc<crate::storage::core::memtable::MemTable>],
    ) -> Result<
        Vec<(
            Arc<crate::storage::core::memtable::MemTable>,
            Arc<RecordBatch>,
        )>,
    > {
        // Pin re-check INSIDE the partition lock: the caller's eligibility
        // snapshot (get_flushable_by_table / get_all_pending) races a
        // concurrent deferred-DML statement registering its pin under the
        // same partition (or table) mutation lock, so the lock is the
        // mutual-exclusion point. A memtable pinned between snapshot and
        // freeze must stay Pending — flushing it would make that
        // transaction's COMMIT unrecoverably fail.
        let txn_mutations = segment_mgr.txn_mutations();
        segment_mgr.with_partition_mutation_lock(
            group[0].db(),
            group[0].table(),
            group[0].partition_key(),
            || {
                Ok(group
                    .iter()
                    .filter(|mt| {
                        !txn_mutations.is_some_and(|registry| registry.is_memtable_pinned(mt.id()))
                    })
                    .filter_map(|mt| Self::freeze_memtable_locked(mt).map(|b| (mt.clone(), b)))
                    .collect::<Vec<_>>())
            },
        )
    }

    /// Flush a same-segment memtable group as one merged append. Mixed
    /// tailed/untailed groups use sequential appends.
    /// Phase 2 of the split group flush: every member is already frozen
    /// (`Flushing`, batch shared) by the freeze phase, and the caller holds
    /// the table's flush I/O gate. Segment appends, durable-tail intents and
    /// tail cleanup run here without the partition mutation lock.
    fn flush_frozen_memtable_group(
        segment_mgr: &Arc<SegmentManager>,
        frozen: &[(
            Arc<crate::storage::core::memtable::MemTable>,
            Arc<RecordBatch>,
        )],
        tail_committer: &Arc<TailCommitter>,
    ) -> Result<()> {
        let db = frozen[0].0.db().to_string();
        let table = frozen[0].0.table().to_string();
        let partition_key = frozen[0].0.partition_key().to_string();
        let txn_id = frozen[0].0.txn_id();
        let table_dir = segment_mgr.table_dir(&db, &table);
        // Every fallible step below (barrier, tail probing, merging, the
        // protocol-specific flushes) restores the frozen members on the way
        // out.
        let _guard = FlushGroupGuard::new(frozen.iter().map(|(mt, _)| mt));
        tail_committer.barrier(&table_dir)?;

        let ids = frozen.iter().map(|(mt, _)| mt.id()).collect::<Vec<_>>();
        let tailed_map = crate::storage::core::durable_tail::has_tail_batch(&table_dir, &ids)?;
        let tailed: Vec<bool> = frozen
            .iter()
            .map(|(mt, _)| {
                // A memtable whose tail commit failed must take the segment
                // protocol even if a (torn) tail file exists on disk.
                !mt.needs_segment_flush() && tailed_map.get(&mt.id()).copied().unwrap_or(false)
            })
            .collect();
        let all_tailed = !tailed.is_empty() && tailed.iter().all(|t| *t);
        let none_tailed = tailed.iter().all(|t| !*t);
        // Mixed tailed/untailed groups have no uniform merged protocol: flush
        // each member as its own length-1 group. The group protocol re-routes
        // per member (tailed members take the durable-tail path, the rest the
        // segment append), so there is exactly one flush protocol — a single
        // memtable is a length-1 group — and no separately maintained single
        // flow to drift from.
        if !all_tailed && !none_tailed {
            for (mt, batch) in frozen {
                let single = [(Arc::clone(mt), Arc::clone(batch))];
                Self::flush_frozen_memtable_group(segment_mgr, &single, tail_committer)?;
            }
            return Ok(());
        }

        if !table_dir.is_dir() {
            // Table was dropped while this flush waited on the I/O gate.
            Self::mark_flushed(frozen.iter().map(|(mt, _)| mt));
            return Ok(());
        }

        let merged = Self::concat_flush_group_batches(frozen)?;
        if all_tailed {
            return Self::flush_durable_memtable_group(
                segment_mgr,
                frozen,
                &merged,
                &db,
                &table,
                &partition_key,
                txn_id,
                &table_dir,
            );
        }

        let flush_result = segment_mgr.flush_memtable_batch_if_under_table_lock(
            &db,
            &table,
            &partition_key,
            &merged,
            txn_id,
            || {
                frozen
                    .iter()
                    .all(|(mt, _)| mt.state() == MemTableState::Flushing)
            },
            || Self::mark_flushed(frozen.iter().map(|(mt, _)| mt)),
        );
        let wrote = match flush_result {
            Ok(wrote) => wrote,
            Err(e) => {
                return Err(e);
            }
        };
        if !wrote {
            // A concurrent winner already carried these rows; drop the copies.
            Self::mark_flushed(frozen.iter().map(|(mt, _)| mt));
            return Ok(());
        }
        for (mt, _) in frozen {
            crate::storage::core::durable_tail::clear(&table_dir, mt.id())?;
        }
        let group_ids: Vec<uuid::Uuid> = frozen.iter().map(|(mt, _)| mt.id()).collect();
        segment_mgr.emit_segment_update(&db, &table, &partition_key, group_ids, None);
        tracing::debug!(
            table = %table,
            rows = merged.num_rows(),
            memtables = frozen.len(),
            txn_id,
            "memtable group flushed to active segment as one row group"
        );
        Ok(())
    }

    fn concat_flush_group_batches(
        locked: &[(
            Arc<crate::storage::core::memtable::MemTable>,
            Arc<RecordBatch>,
        )],
    ) -> Result<Arc<RecordBatch>> {
        let mut iter = locked.iter();
        let (_, first) = iter
            .next()
            .ok_or_else(|| TaibaiError::Storage("flush group is empty".into()))?;
        // Single memtable in the group: flush its shared batch as-is instead
        // of copying it into a merged buffer.
        if iter.len() == 0 {
            return Ok(Arc::clone(first));
        }
        let mut merged = (**first).clone();
        for (_, batch) in iter {
            for col_idx in 0..batch.num_columns() {
                let source = batch.column(col_idx).ok_or_else(|| {
                    TaibaiError::Storage("flush group batch is missing a column".into())
                })?;
                let destination = merged.column_mut(col_idx).ok_or_else(|| {
                    TaibaiError::Storage("flush group merged batch is missing a column".into())
                })?;
                destination.extend(source)?;
            }
        }
        Ok(Arc::new(merged))
    }

    /// Return still-`Flushing` members of a failed flush to `Pending`. A
    /// memtable left in `Flushing` would never resolve again:
    /// `get_or_create_with_txn` only hands out `Pending` memtables.
    fn restore_flushing_to_pending<'m>(
        memtables: impl IntoIterator<Item = &'m Arc<crate::storage::core::memtable::MemTable>>,
    ) {
        for mt in memtables {
            if mt.state() == MemTableState::Flushing {
                mt.set_state(MemTableState::Pending);
            }
        }
    }

    /// Cancel flush intents that were begun before a flush failed. Failures
    /// are ignored on purpose: recovery treats a leftover intent marker as
    /// unsatisfied and re-flushes the tail.
    fn cancel_flush_intents(
        intents: impl IntoIterator<Item = crate::storage::core::durable_tail::FlushIntent>,
    ) {
        for intent in intents {
            let _ = crate::storage::core::durable_tail::cancel_flush(intent);
        }
    }

    /// Drop the resident batches of memtables whose rows are now durable
    /// (in a segment or carried by a concurrent winner flush).
    fn mark_flushed<'m>(
        memtables: impl IntoIterator<Item = &'m Arc<crate::storage::core::memtable::MemTable>>,
    ) {
        for mt in memtables {
            mt.clear_batch();
            mt.set_state(MemTableState::Flushed);
        }
    }

    /// Durable (tailed) group flush: one flush intent per tail with cumulative
    /// row-count watermarks over the shared target segment. The single merged
    /// append makes every watermark true at once; a crash before the footer
    /// reaches a watermark leaves that tail's intent for recovery, exactly as
    /// in the single-memtable protocol.
    #[allow(clippy::too_many_arguments)]
    fn flush_durable_memtable_group(
        segment_mgr: &Arc<SegmentManager>,
        locked: &[(
            Arc<crate::storage::core::memtable::MemTable>,
            Arc<RecordBatch>,
        )],
        merged: &RecordBatch,
        db: &str,
        table: &str,
        partition_key: &str,
        txn_id: u64,
        table_dir: &std::path::Path,
    ) -> Result<()> {
        let mut guard = FlushGroupGuard::new(locked.iter().map(|(mt, _)| mt));
        let watermark = segment_mgr.durable_active_segment_watermark_under_table_lock(
            db,
            table,
            partition_key,
            &merged.schema,
            merged.num_rows() as u64,
        )?;
        let Some((segment_file_name, expected_total_rows)) = watermark else {
            return Self::flush_durable_memtable_group_as_new_segment(
                segment_mgr,
                locked,
                merged,
                db,
                table,
                partition_key,
                txn_id,
                table_dir,
            );
        };
        let base_rows = expected_total_rows - merged.num_rows() as u64;
        let mut cumulative = 0u64;
        for (mt, batch) in locked {
            cumulative += batch.num_rows() as u64;
            match crate::storage::core::durable_tail::begin_flush(
                table_dir,
                mt.id(),
                &segment_file_name,
                base_rows + cumulative,
            ) {
                Ok(intent) => guard.intents.push(intent),
                Err(error) => {
                    return Err(error);
                }
            }
        }
        let flush_result = segment_mgr.flush_memtable_batch_if_under_table_lock(
            db,
            table,
            partition_key,
            merged,
            0,
            || {
                locked
                    .iter()
                    .all(|(mt, _)| mt.state() == MemTableState::Flushing)
            },
            || Self::mark_flushed(locked.iter().map(|(mt, _)| mt)),
        );
        let wrote = match flush_result {
            Ok(wrote) => wrote,
            Err(error) => {
                return Err(error);
            }
        };
        if !wrote {
            // A concurrent winner already carried these rows; drop the
            // copies and the intents (the guard's drop cancels them).
            Self::mark_flushed(locked.iter().map(|(mt, _)| mt));
            return Ok(());
        }
        if let Err(error) =
            crate::storage::core::durable_tail::finish_flush_batch(table_dir, guard.take_intents())
        {
            tracing::warn!(
                db,
                table,
                error = %error,
                "durable tail group flush cleanup deferred to recovery"
            );
        }
        let group_ids: Vec<uuid::Uuid> = locked.iter().map(|(mt, _)| mt.id()).collect();
        segment_mgr.emit_segment_update(
            db,
            table,
            partition_key,
            group_ids,
            Some(segment_file_name.as_str()),
        );
        tracing::debug!(
            table,
            rows = merged.num_rows(),
            memtables = locked.len(),
            txn_id,
            segment = %segment_file_name,
            expected_total_rows,
            "durable memtable group appended to active segment as one row group"
        );
        Ok(())
    }

    /// Durable group flush with no appendable active segment yet: write the
    /// merged rows as one standalone segment via tmp+rename (mirrors
    /// `flush_durable_memtable_as_new_segment` for a group).
    #[allow(clippy::too_many_arguments)]
    fn flush_durable_memtable_group_as_new_segment(
        segment_mgr: &Arc<SegmentManager>,
        locked: &[(
            Arc<crate::storage::core::memtable::MemTable>,
            Arc<RecordBatch>,
        )],
        merged: &RecordBatch,
        db: &str,
        table: &str,
        partition_key: &str,
        txn_id: u64,
        table_dir: &std::path::Path,
    ) -> Result<()> {
        let final_path = if partition_key.is_empty() {
            segment_mgr.next_segment_path(table_dir, "")?
        } else {
            segment_mgr.next_partition_segment_path(table_dir, partition_key)?
        };
        let segment_file_name = final_path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| {
                TaibaiError::Storage("durable tail segment path has no file name".into())
            })?
            .to_string();
        let mut guard = FlushGroupGuard::new(locked.iter().map(|(mt, _)| mt));
        let mut cumulative = 0u64;
        for (mt, batch) in locked {
            cumulative += batch.num_rows() as u64;
            match crate::storage::core::durable_tail::begin_flush(
                table_dir,
                mt.id(),
                &segment_file_name,
                cumulative,
            ) {
                Ok(intent) => guard.intents.push(intent),
                Err(error) => {
                    return Err(error);
                }
            }
        }
        let tmp_path = table_dir.join(format!(".tail_flush_{}.tmp", locked[0].0.id().simple()));

        let write_result = (|| {
            let _ = std::fs::remove_file(&tmp_path);
            let _ = remove_sidecar_meta(&tmp_path);
            segment_mgr.write_standalone_active_encoded_segment(&tmp_path, merged)?;
            Ok(())
        })();

        if let Err(error) = write_result {
            let _ = std::fs::remove_file(&tmp_path);
            let _ = remove_sidecar_meta(&tmp_path);
            return Err(error);
        }

        let transition_result = segment_mgr.with_flush_transition(db, table, || {
            std::fs::rename(&tmp_path, &final_path)?;
            let _ = remove_sidecar_meta(&tmp_path);
            segment_mgr.open_reader(&final_path)?;
            crate::storage::segment_manager::SegmentManager::sleep_flush_transition_delay();
            Self::mark_flushed(locked.iter().map(|(mt, _)| mt));
            Ok::<(), TaibaiError>(())
        });
        if let Err(error) = transition_result {
            let _ = std::fs::remove_file(&tmp_path);
            let _ = remove_sidecar_meta(&tmp_path);
            return Err(error);
        }
        if let Err(error) =
            crate::storage::core::durable_tail::finish_flush_batch(table_dir, guard.take_intents())
        {
            tracing::warn!(
                db,
                table,
                error = %error,
                "durable tail group flush cleanup deferred to recovery"
            );
        }
        let group_ids: Vec<uuid::Uuid> = locked.iter().map(|(mt, _)| mt.id()).collect();
        segment_mgr.emit_segment_update(
            db,
            table,
            partition_key,
            group_ids,
            Some(segment_file_name.as_str()),
        );
        tracing::debug!(
            table,
            rows = merged.num_rows(),
            memtables = locked.len(),
            txn_id,
            segment = %segment_file_name,
            "durable memtable group materialized as new segment"
        );
        Ok(())
    }
}

#[cfg(test)]
#[path = "batch_writer_test.rs"]
mod tests;
