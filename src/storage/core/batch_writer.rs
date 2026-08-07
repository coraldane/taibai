#![allow(dead_code)]

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use crate::common::error::{Result, TaibaiError};
use crate::storage::core::durable_tail::{PreparedTailAppend, TailCommitTicket, TailCommitter};
use crate::storage::core::memtable::{MemTableManager, MemTableState};
use crate::storage::segment::meta::remove_sidecar_meta;
use crate::storage::segment_manager::SegmentManager;
use crate::types::batch::{RecordBatch, Schema};

const DEFAULT_MAX_ROWS_PER_MEMTABLE: u64 = 500_000;
const DEFAULT_FLUSH_INTERVAL_SECS: u64 = 30;
const DEFAULT_MAX_TOTAL_MEMORY: u64 = 512 * 1024 * 1024;
const DEFAULT_LOW_RATE_MAX_ROWS: u64 = 1024;
const DEFAULT_LOW_RATE_MAX_AGE_SECS: u64 = 7 * 24 * 60 * 60;

fn insert_profile_enabled() -> bool {
    matches!(std::env::var("TAIBAI_INSERT_PROFILE"), Ok(v) if v == "1" || v.eq_ignore_ascii_case("true"))
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
    WriteColumns {
        db: String,
        table: String,
        partition_key: String,
        batch: RecordBatch,
        schema: Schema,
        txn_id: u64,
    },
    Flush {
        db: String,
        table: String,
    },
    TxnCommit {
        txn_id: u64,
    },
    TxnRollback {
        txn_id: u64,
    },
    Shutdown,
}

pub struct BatchWriterConfig {
    pub max_rows_per_memtable: u64,
    pub flush_interval_secs: u64,
    pub max_total_memory: u64,
    pub worker_count: usize,
    pub low_rate_max_rows: u64,
    pub low_rate_max_age_secs: u64,
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
        }
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
}

impl BatchWriter {
    pub fn new(config: BatchWriterConfig, segment_mgr: Arc<SegmentManager>) -> Self {
        let memtable_mgr = Arc::new(MemTableManager::new(
            config.max_rows_per_memtable,
            config.flush_interval_secs,
            config.low_rate_max_rows,
            config.low_rate_max_age_secs,
        ));
        let (sender, receiver) = crossbeam_channel::bounded(8192);
        let stop = Arc::new(AtomicBool::new(false));
        let receiver = Arc::new(receiver);
        let tail_committer = TailCommitter::start();
        let write_epoch = Arc::new(AtomicU64::new(0));

        let worker_count = config.worker_count.max(1);
        let checkpoint_backlog_bytes = config
            .max_total_memory
            .saturating_div(4)
            .clamp(512 * 1024 * 1024, 2 * 1024 * 1024 * 1024);
        let mut workers = Vec::with_capacity(worker_count);
        for i in 0..worker_count {
            let rx = Arc::clone(&receiver);
            let stop_clone = Arc::clone(&stop);
            let mgr_clone = Arc::clone(&segment_mgr);
            let mt_mgr_clone = Arc::clone(&memtable_mgr);
            let tail_committer_clone = Arc::clone(&tail_committer);
            let write_epoch_clone = Arc::clone(&write_epoch);
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
                            max_rows_per_memtable,
                            checkpoint_backlog_bytes,
                        );
                    })
                    .expect("failed to spawn batch writer worker"),
            );
        }

        let flush_stop = Arc::clone(&stop);
        let flush_interval = config.flush_interval_secs;
        let flush_memory_high_water = config.max_total_memory.max(1);
        let flush_mgr = Arc::clone(&segment_mgr);
        let flush_mt_mgr = Arc::clone(&memtable_mgr);
        let flush_tail_committer = Arc::clone(&tail_committer);
        let flush_write_epoch = Arc::clone(&write_epoch);
        let flush_handle = std::thread::Builder::new()
            .name("taibai-bw-flush".into())
            .spawn(move || {
                let mut last_periodic_write_epoch = flush_write_epoch.load(Ordering::Acquire);
                while !flush_stop.load(Ordering::Acquire) {
                    for _ in 0..flush_interval {
                        if flush_stop.load(Ordering::Acquire) {
                            break;
                        }
                        if flush_mt_mgr.total_estimated_memory_bytes() >= flush_memory_high_water {
                            if let Err(e) = Self::flush_all_pending(
                                &flush_mgr,
                                &flush_mt_mgr,
                                &flush_tail_committer,
                            ) {
                                tracing::warn!(error = %e, "pressure memtable flush failed");
                            }
                        }
                        std::thread::sleep(std::time::Duration::from_secs(1));
                    }
                    if flush_stop.load(Ordering::Acquire) {
                        break;
                    }
                    let current_write_epoch = flush_write_epoch.load(Ordering::Acquire);
                    if current_write_epoch != last_periodic_write_epoch {
                        last_periodic_write_epoch = current_write_epoch;
                        continue;
                    }
                    if let Err(e) = Self::flush_pending_memtables(
                        &flush_mgr,
                        &flush_mt_mgr,
                        &flush_tail_committer,
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
        }
    }

    pub fn memtable_manager(&self) -> &Arc<MemTableManager> {
        &self.memtable_mgr
    }

    pub fn segment_manager(&self) -> &Arc<SegmentManager> {
        &self.segment_mgr
    }

    /// Drain all earlier durable-tail appends and close cached file handles for
    /// a table. DDL calls this while holding the table mutation lock before a
    /// Windows directory rename or removal.
    pub(crate) fn drain_table_tail(&self, db: &str, table: &str) -> Result<()> {
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
    ) -> Result<u64> {
        self.write_columns_with_txn(db, table, partition_key, batch, schema, 0)
    }

    pub fn write_columns_with_txn(
        &self,
        db: &str,
        table: &str,
        partition_key: &str,
        batch: RecordBatch,
        schema: Schema,
        txn_id: u64,
    ) -> Result<u64> {
        let row_count = batch.num_rows() as u64;
        if row_count == 0 {
            return Ok(0);
        }
        if txn_id == 0 {
            self.write_epoch.fetch_add(1, Ordering::Release);
        }
        let prepared = (txn_id == 0).then(|| PreparedTailAppend::new(&batch));
        let (rows, pending) = self.segment_mgr.with_table_mutation_lock(db, table, || {
            self.write_columns_grouped_under_table_lock(
                db,
                table,
                partition_key,
                batch,
                schema,
                txn_id,
                prepared,
            )
        })?;
        if let Some((ticket, memtable)) = pending {
            if let Err(error) = ticket.wait() {
                tracing::warn!(
                    db,
                    table,
                    error = %error,
                    "group-committed durable tail write failed; flushing target memtable"
                );
                Self::flush_memtable(&self.segment_mgr, &memtable, &self.tail_committer)?;
            }
        }
        Ok(rows)
    }

    fn write_columns_grouped_under_table_lock(
        &self,
        db: &str,
        table: &str,
        partition_key: &str,
        batch: RecordBatch,
        schema: Schema,
        txn_id: u64,
        prepared: Option<PreparedTailAppend>,
    ) -> Result<(
        u64,
        Option<(
            TailCommitTicket,
            Arc<crate::storage::core::memtable::MemTable>,
        )>,
    )> {
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
        log_insert_profile("memtable_get", row_count as usize, get_start.elapsed());
        let append_start = Instant::now();
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
        log_insert_profile(
            "memtable_append",
            row_count as usize,
            append_start.elapsed(),
        );
        if txn_id == 0 && mt.row_count() >= self.config.max_rows_per_memtable {
            let _ = self.sender.try_send(WriteCommand::Flush {
                db: db.to_string(),
                table: table.to_string(),
            });
        }
        Ok((row_count, pending))
    }

    pub(crate) fn write_columns_with_txn_under_table_lock(
        &self,
        db: &str,
        table: &str,
        partition_key: &str,
        batch: RecordBatch,
        schema: Schema,
        txn_id: u64,
    ) -> Result<u64> {
        let row_count = batch.num_rows() as u64;
        if row_count == 0 {
            return Ok(0);
        }
        if txn_id == 0 {
            self.write_epoch.fetch_add(1, Ordering::Release);
        }
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
        log_insert_profile("memtable_get", row_count as usize, get_start.elapsed());
        let append_start = Instant::now();
        if txn_id == 0 {
            // Keep the input batch alive so only this INSERT's delta is added
            // to the durable tail. Rewriting mt.read_snapshot() here made a
            // stream of small INSERTs quadratic in both serialization and I/O.
            mt.append(&batch)?;
            if let Err(error) = self.append_memtable_tail(&mt, &batch) {
                tracing::warn!(
                    db,
                    table,
                    error = %error,
                    "durable tail write failed; flushing target memtable"
                );
                Self::flush_memtable_inner(&self.segment_mgr, &mt, true, &self.tail_committer)?;
            }
        } else {
            mt.append_owned(batch)?;
        }
        log_insert_profile(
            "memtable_append",
            row_count as usize,
            append_start.elapsed(),
        );

        if txn_id == 0 && mt.row_count() >= self.config.max_rows_per_memtable {
            let _ = self.sender.try_send(WriteCommand::Flush {
                db: db.to_string(),
                table: table.to_string(),
            });
        }
        Ok(row_count)
    }

    pub fn commit_txn(&self, txn_id: u64) -> Result<()> {
        self.commit_txn_inner(txn_id, false)
    }

    pub(crate) fn commit_txn_under_table_lock(&self, txn_id: u64) -> Result<()> {
        self.commit_txn_inner(txn_id, true)
    }

    fn commit_txn_inner(&self, txn_id: u64, table_lock_held: bool) -> Result<()> {
        let committed_mts = self.memtable_mgr.commit_txn(txn_id);
        for memtable in &committed_mts {
            let persist = || {
                if let Err(error) = self.persist_memtable_tail(memtable) {
                    tracing::warn!(
                        db = memtable.db(),
                        table = memtable.table(),
                        error = %error,
                        "committed durable tail write failed; flushing target memtable"
                    );
                    Self::flush_memtable_inner(
                        &self.segment_mgr,
                        memtable,
                        true,
                        &self.tail_committer,
                    )?;
                }
                Ok(())
            };
            if table_lock_held {
                persist()?;
            } else {
                self.segment_mgr.with_table_mutation_lock(
                    memtable.db(),
                    memtable.table(),
                    persist,
                )?;
            }
        }
        let rows_committed = committed_mts.iter().map(|mt| mt.row_count()).sum::<u64>();
        self.segment_mgr.commit_txn(txn_id)?;
        tracing::debug!(
            txn_id,
            rows_committed,
            "transaction committed to visible memtables"
        );
        Ok(())
    }

    pub fn rollback_txn(&self, txn_id: u64) -> Result<u64> {
        let rolled_back_rows = self.memtable_mgr.rollback_txn(txn_id);
        let tables = self.memtable_mgr.txn_tables(txn_id);
        for ((db, table), _) in tables {
            let table_dir = self.segment_mgr.table_dir(&db, &table);
            if table_dir.exists() {
                self.segment_mgr.clear_txn_wal(&table_dir, txn_id)?;
            }
        }
        self.segment_mgr.rollback_txn(txn_id)?;
        tracing::debug!(
            txn_id,
            rows_discarded = rolled_back_rows,
            "transaction rolled back"
        );
        Ok(rolled_back_rows)
    }

    pub fn flush_table(&self, db: &str, table: &str) -> Result<()> {
        let mts = self.memtable_mgr.get_by_table(db, table);
        for group in Self::group_memtables_for_flush(mts) {
            Self::flush_memtable_group(&self.segment_mgr, &group, false, &self.tail_committer)?;
        }
        Ok(())
    }

    pub fn flush_partition(&self, db: &str, table: &str, partition_key: &str) -> Result<()> {
        let mts = self
            .memtable_mgr
            .get_by_table(db, table)
            .into_iter()
            .filter(|memtable| memtable.partition_key() == partition_key)
            .collect::<Vec<_>>();
        for group in Self::group_memtables_for_flush(mts) {
            Self::flush_memtable_group(&self.segment_mgr, &group, false, &self.tail_committer)?;
        }
        self.memtable_mgr.remove_flushed();
        Ok(())
    }

    pub(crate) fn flush_table_under_table_lock(&self, db: &str, table: &str) -> Result<()> {
        let mts = self.memtable_mgr.get_by_table(db, table);
        for group in Self::group_memtables_for_flush(mts) {
            Self::flush_memtable_group(&self.segment_mgr, &group, true, &self.tail_committer)?;
        }
        self.memtable_mgr.remove_flushed();
        Ok(())
    }

    pub fn flush_all(&self) -> Result<()> {
        let mts = self.memtable_mgr.get_all_pending();
        for group in Self::group_memtables_for_flush(mts) {
            Self::flush_memtable_group(&self.segment_mgr, &group, false, &self.tail_committer)?;
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
                let table_dir = self.segment_mgr.table_dir(mt.db(), mt.table());
                let _ = self.tail_committer.barrier(&table_dir);
                !crate::storage::core::durable_tail::has_tail(&table_dir, mt.id())
            })
            .collect();
        for group in Self::group_memtables_for_flush(mts) {
            Self::flush_memtable_group(&self.segment_mgr, &group, false, &self.tail_committer)?;
        }
        self.memtable_mgr.remove_flushed();
        Ok(())
    }

    pub fn clear_memtables(&self, db: &str, table: &str) -> u64 {
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
        let old_dir = self.segment_mgr.table_dir(db, old_table);
        if let Err(error) = self.tail_committer.barrier(&old_dir) {
            tracing::warn!(db, table = old_table, error = %error, "failed to drain durable tail before rename");
            return 0;
        }
        self.memtable_mgr.rename_table(db, old_table, new_table)
    }

    pub(crate) fn persist_memtable_tail(
        &self,
        memtable: &crate::storage::core::memtable::MemTable,
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
        let Some(batch) = memtable.read_snapshot() else {
            return Ok(());
        };
        crate::storage::core::durable_tail::persist(
            &table_dir,
            memtable.id(),
            memtable.partition_key(),
            &batch,
        )
    }

    fn append_memtable_tail(
        &self,
        memtable: &crate::storage::core::memtable::MemTable,
        delta: &RecordBatch,
    ) -> Result<()> {
        if memtable.state() != MemTableState::Pending
            || memtable.is_transactional()
            || memtable.visibility() != crate::storage::core::memtable::TxnVisibility::Committed
        {
            return Ok(());
        }
        crate::storage::core::durable_tail::append(
            &self.segment_mgr.table_dir(memtable.db(), memtable.table()),
            memtable.id(),
            memtable.partition_key(),
            delta,
        )
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
        max_rows_per_memtable: u64,
        checkpoint_backlog_bytes: u64,
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
                Ok(WriteCommand::WriteColumns { .. }) => {}
                Ok(WriteCommand::Flush { db, table }) => {
                    last_write_epoch = write_epoch.load(Ordering::Acquire);
                    pending_tables.insert((db.clone(), table.clone()));
                    if memtable_mgr.total_estimated_memory_bytes() >= checkpoint_backlog_bytes {
                        pending_tables.remove(&(db.clone(), table.clone()));
                        if Self::checkpoint_one_full_memtable(
                            &segment_mgr,
                            &memtable_mgr,
                            &tail_committer,
                            max_rows_per_memtable,
                            &db,
                            &table,
                        ) {
                            pending_tables.insert((db, table));
                        }
                    } else {
                        checkpoint_idle = false;
                    }
                }
                Ok(WriteCommand::TxnCommit { txn_id }) => {
                    memtable_mgr.commit_txn(txn_id);
                    if let Err(e) = segment_mgr.commit_txn(txn_id) {
                        tracing::warn!(error = %e, txn_id, "failed to commit transaction in storage");
                    }
                }
                Ok(WriteCommand::TxnRollback { txn_id }) => {
                    let rolled_back_rows = memtable_mgr.rollback_txn(txn_id);
                    tracing::debug!(
                        txn_id,
                        rows_discarded = rolled_back_rows,
                        "transaction rolled back in worker"
                    );
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
                    if Self::checkpoint_one_full_memtable(
                        &segment_mgr,
                        &memtable_mgr,
                        &tail_committer,
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

    /// Checkpoint one full memtable and report whether the same table still
    /// has additional full memtables waiting. Limiting each foreground-active
    /// turn to one keeps the write-behind queue bounded without a long burst.
    fn checkpoint_one_full_memtable(
        segment_mgr: &Arc<SegmentManager>,
        memtable_mgr: &MemTableManager,
        tail_committer: &Arc<TailCommitter>,
        max_rows_per_memtable: u64,
        db: &str,
        table: &str,
    ) -> bool {
        let mut full = memtable_mgr
            .get_by_table(db, table)
            .into_iter()
            .filter(|mt| mt.row_count() >= max_rows_per_memtable)
            .collect::<Vec<_>>();
        if let Some(mt) = full.pop() {
            if let Err(error) = Self::flush_memtable(segment_mgr, &mt, tail_committer) {
                tracing::warn!(error = %error, "failed to flush memtable for {}.{}", db, table);
            }
        }
        !full.is_empty()
    }

    fn flush_pending_memtables(
        segment_mgr: &Arc<SegmentManager>,
        memtable_mgr: &MemTableManager,
        tail_committer: &Arc<TailCommitter>,
    ) -> Result<()> {
        let flushable = memtable_mgr.get_flushable();
        for group in Self::group_memtables_for_flush(flushable) {
            Self::flush_memtable_group(segment_mgr, &group, false, tail_committer)?;
        }
        memtable_mgr.remove_flushed();
        Ok(())
    }

    /// Flush *every* pending memtable regardless of age or size. Used under
    /// memory pressure (design §4.1 WAL/resident-memory high-water), in contrast
    /// to `flush_pending_memtables` which only drains memtables that are full or
    /// old. Transactional memtables are left untouched.
    fn flush_all_pending(
        segment_mgr: &Arc<SegmentManager>,
        memtable_mgr: &MemTableManager,
        tail_committer: &Arc<TailCommitter>,
    ) -> Result<()> {
        let mts = memtable_mgr
            .get_all_pending()
            .into_iter()
            .filter(|mt| !mt.is_transactional())
            .collect();
        for group in Self::group_memtables_for_flush(mts) {
            Self::flush_memtable_group(segment_mgr, &group, false, tail_committer)?;
        }
        memtable_mgr.remove_flushed();
        Ok(())
    }

    fn flush_memtable(
        segment_mgr: &Arc<SegmentManager>,
        mt: &Arc<crate::storage::core::memtable::MemTable>,
        tail_committer: &Arc<TailCommitter>,
    ) -> Result<()> {
        Self::flush_memtable_inner(segment_mgr, mt, false, tail_committer)
    }

    fn flush_memtable_inner(
        segment_mgr: &Arc<SegmentManager>,
        mt: &Arc<crate::storage::core::memtable::MemTable>,
        table_lock_held: bool,
        tail_committer: &Arc<TailCommitter>,
    ) -> Result<()> {
        let db = mt.db().to_string();
        let table = mt.table().to_string();
        let had_rows = mt.state() == MemTableState::Pending && mt.row_count() > 0;
        let result = if table_lock_held {
            Self::flush_memtable_under_table_lock(segment_mgr, mt, tail_committer)
        } else {
            segment_mgr.with_table_mutation_lock(&db, &table, || {
                Self::flush_memtable_under_table_lock(segment_mgr, mt, tail_committer)
            })
        };
        if result.is_ok() && had_rows {
            // Rows may have landed in segments (or been carried by a concurrent
            // winner flush); advance the epoch so unique-index snapshots stamped
            // earlier are rebuilt before probing segment-side locations.
            segment_mgr.note_memtable_flush(&db, &table);
        }
        result
    }

    /// Rollback fallback: when restoring a memtable's durable tail fails, flush
    /// the memtable to segments under the already-held table lock and drop the
    /// stale tail, so crash recovery cannot replay partially-restored rows.
    pub(crate) fn flush_memtable_restore_fallback_under_table_lock(
        &self,
        memtable: &Arc<crate::storage::core::memtable::MemTable>,
    ) -> Result<()> {
        Self::flush_memtable_inner(&self.segment_mgr, memtable, true, &self.tail_committer)?;
        let _ = crate::storage::core::durable_tail::clear(
            &self.segment_mgr.table_dir(memtable.db(), memtable.table()),
            memtable.id(),
        );
        Ok(())
    }

    fn flush_memtable_under_table_lock(
        segment_mgr: &Arc<SegmentManager>,
        mt: &Arc<crate::storage::core::memtable::MemTable>,
        tail_committer: &Arc<TailCommitter>,
    ) -> Result<()> {
        tail_committer.barrier(&segment_mgr.table_dir(mt.db(), mt.table()))?;
        if mt.state() != MemTableState::Pending {
            return Ok(());
        }
        mt.set_state(MemTableState::Flushing);
        let batch = match mt.read_snapshot() {
            Some(b) if b.num_rows() > 0 => b,
            _ => {
                mt.set_state(MemTableState::Flushed);
                return Ok(());
            }
        };
        let db = mt.db().to_string();
        let table = mt.table().to_string();
        let partition_key = mt.partition_key().to_string();
        let txn_id = mt.txn_id();
        let table_dir = segment_mgr.table_dir(&db, &table);
        if !table_dir.is_dir() {
            let _ = mt.take_batch();
            mt.set_state(MemTableState::Flushed);
            return Ok(());
        }
        if crate::storage::core::durable_tail::has_tail(&table_dir, mt.id()) {
            return Self::flush_durable_memtable_under_table_lock(
                segment_mgr,
                mt,
                &batch,
                &db,
                &table,
                &partition_key,
                txn_id,
            );
        }
        let flush_result = segment_mgr.flush_memtable_batch_if_under_table_lock(
            &db,
            &table,
            &partition_key,
            &batch,
            txn_id,
            || mt.state() == MemTableState::Flushing,
            || {
                let _ = mt.take_batch();
                mt.set_state(MemTableState::Flushed);
            },
        );
        let wrote = match flush_result {
            Ok(wrote) => wrote,
            Err(e) => {
                if mt.state() == MemTableState::Flushing {
                    mt.set_state(MemTableState::Pending);
                }
                return Err(e);
            }
        };
        if !wrote {
            let _ = mt.take_batch();
            mt.set_state(MemTableState::Flushed);
            return Ok(());
        }

        crate::storage::core::durable_tail::clear(&segment_mgr.table_dir(&db, &table), mt.id())?;

        tracing::debug!(
            table = %table,
            rows = mt.row_count(),
            txn_id,
            "memtable flushed to active segment"
        );
        Ok(())
    }

    /// Group memtables that would be flushed to the same active segment
    /// (same db/table/partition and the same txn WAL scope), preserving the
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
    ) -> Result<()> {
        if group.len() == 1 {
            return Self::flush_memtable_inner(segment_mgr, &group[0], table_lock_held, tail_committer);
        }
        let db = group[0].db().to_string();
        let table = group[0].table().to_string();
        let had_rows = group
            .iter()
            .any(|mt| mt.state() == MemTableState::Pending && mt.row_count() > 0);
        let result = if table_lock_held {
            Self::flush_memtable_group_under_table_lock(segment_mgr, group, tail_committer)
        } else {
            segment_mgr.with_table_mutation_lock(&db, &table, || {
                Self::flush_memtable_group_under_table_lock(segment_mgr, group, tail_committer)
            })
        };
        if result.is_ok() && had_rows {
            segment_mgr.note_memtable_flush(&db, &table);
        }
        result
    }

    /// Flush a same-segment memtable group as ONE merged append. Mixed
    /// tailed/untailed groups keep the legacy sequential semantics.
    fn flush_memtable_group_under_table_lock(
        segment_mgr: &Arc<SegmentManager>,
        group: &[Arc<crate::storage::core::memtable::MemTable>],
        tail_committer: &Arc<TailCommitter>,
    ) -> Result<()> {
        let db = group[0].db().to_string();
        let table = group[0].table().to_string();
        let partition_key = group[0].partition_key().to_string();
        let txn_id = group[0].txn_id();
        let table_dir = segment_mgr.table_dir(&db, &table);
        tail_committer.barrier(&table_dir)?;

        let pending: Vec<&Arc<crate::storage::core::memtable::MemTable>> = group
            .iter()
            .filter(|mt| mt.state() == MemTableState::Pending)
            .collect();
        let tailed: Vec<bool> = pending
            .iter()
            .map(|mt| crate::storage::core::durable_tail::has_tail(&table_dir, mt.id()))
            .collect();
        let all_tailed = !tailed.is_empty() && tailed.iter().all(|t| *t);
        let none_tailed = tailed.iter().all(|t| !*t);
        if pending.len() <= 1 || (!all_tailed && !none_tailed) {
            for mt in pending {
                Self::flush_memtable_under_table_lock(segment_mgr, mt, tail_committer)?;
            }
            return Ok(());
        }

        // Lock the group in Flushing state and snapshot every batch.
        let mut locked: Vec<(Arc<crate::storage::core::memtable::MemTable>, RecordBatch)> =
            Vec::with_capacity(pending.len());
        for mt in pending {
            mt.set_state(MemTableState::Flushing);
            match mt.read_snapshot() {
                Some(b) if b.num_rows() > 0 => locked.push((mt.clone(), b)),
                _ => mt.set_state(MemTableState::Flushed),
            }
        }
        if locked.is_empty() {
            return Ok(());
        }
        if !table_dir.is_dir() {
            // Table was dropped while the group waited on the table lock.
            for (mt, _) in &locked {
                let _ = mt.take_batch();
                mt.set_state(MemTableState::Flushed);
            }
            return Ok(());
        }

        let merged = Self::concat_flush_group_batches(&locked)?;
        if all_tailed {
            return Self::flush_durable_memtable_group_under_table_lock(
                segment_mgr,
                &locked,
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
            || locked.iter().all(|(mt, _)| mt.state() == MemTableState::Flushing),
            || {
                for (mt, _) in &locked {
                    let _ = mt.take_batch();
                    mt.set_state(MemTableState::Flushed);
                }
            },
        );
        let wrote = match flush_result {
            Ok(wrote) => wrote,
            Err(e) => {
                for (mt, _) in &locked {
                    if mt.state() == MemTableState::Flushing {
                        mt.set_state(MemTableState::Pending);
                    }
                }
                return Err(e);
            }
        };
        if !wrote {
            // A concurrent winner already carried these rows; drop the copies.
            for (mt, _) in &locked {
                let _ = mt.take_batch();
                mt.set_state(MemTableState::Flushed);
            }
            return Ok(());
        }
        for (mt, _) in &locked {
            crate::storage::core::durable_tail::clear(&table_dir, mt.id())?;
        }
        tracing::debug!(
            table = %table,
            rows = merged.num_rows(),
            memtables = locked.len(),
            txn_id,
            "memtable group flushed to active segment as one row group"
        );
        Ok(())
    }

    fn concat_flush_group_batches(
        locked: &[(Arc<crate::storage::core::memtable::MemTable>, RecordBatch)],
    ) -> Result<RecordBatch> {
        let mut iter = locked.iter();
        let (_, first) = iter
            .next()
            .ok_or_else(|| TaibaiError::Storage("flush group is empty".into()))?;
        let mut merged = first.clone();
        for (_, batch) in iter {
            for col_idx in 0..batch.num_columns() {
                let source = batch
                    .column(col_idx)
                    .ok_or_else(|| TaibaiError::Storage("flush group batch is missing a column".into()))?;
                let destination = merged.column_mut(col_idx).ok_or_else(|| {
                    TaibaiError::Storage("flush group merged batch is missing a column".into())
                })?;
                destination.extend(source)?;
            }
        }
        Ok(merged)
    }

    /// Durable (tailed) group flush: one flush intent per tail with cumulative
    /// row-count watermarks over the shared target segment. The single merged
    /// append makes every watermark true at once; a crash before the footer
    /// reaches a watermark leaves that tail's intent for recovery, exactly as
    /// in the single-memtable protocol.
    #[allow(clippy::too_many_arguments)]
    fn flush_durable_memtable_group_under_table_lock(
        segment_mgr: &Arc<SegmentManager>,
        locked: &[(Arc<crate::storage::core::memtable::MemTable>, RecordBatch)],
        merged: &RecordBatch,
        db: &str,
        table: &str,
        partition_key: &str,
        txn_id: u64,
        table_dir: &std::path::Path,
    ) -> Result<()> {
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
        let mut intents = Vec::with_capacity(locked.len());
        let mut cumulative = 0u64;
        for (mt, batch) in locked {
            cumulative += batch.num_rows() as u64;
            match crate::storage::core::durable_tail::begin_flush(
                table_dir,
                mt.id(),
                &segment_file_name,
                base_rows + cumulative,
            ) {
                Ok(intent) => intents.push(intent),
                Err(error) => {
                    for intent in intents {
                        let _ = crate::storage::core::durable_tail::cancel_flush(intent);
                    }
                    for (mt, _) in locked {
                        if mt.state() == MemTableState::Flushing {
                            mt.set_state(MemTableState::Pending);
                        }
                    }
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
            || locked.iter().all(|(mt, _)| mt.state() == MemTableState::Flushing),
            || {
                for (mt, _) in locked {
                    let _ = mt.take_batch();
                    mt.set_state(MemTableState::Flushed);
                }
            },
        );
        let wrote = match flush_result {
            Ok(wrote) => wrote,
            Err(error) => {
                for intent in intents {
                    let _ = crate::storage::core::durable_tail::cancel_flush(intent);
                }
                for (mt, _) in locked {
                    if mt.state() == MemTableState::Flushing {
                        mt.set_state(MemTableState::Pending);
                    }
                }
                return Err(error);
            }
        };
        if !wrote {
            for (mt, _) in locked {
                let _ = mt.take_batch();
                mt.set_state(MemTableState::Flushed);
            }
            for intent in intents {
                let _ = crate::storage::core::durable_tail::cancel_flush(intent);
            }
            return Ok(());
        }
        for intent in intents {
            if let Err(error) = crate::storage::core::durable_tail::finish_flush(table_dir, intent) {
                tracing::warn!(
                    db,
                    table,
                    error = %error,
                    "durable tail group flush cleanup deferred to recovery"
                );
            }
        }
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
        locked: &[(Arc<crate::storage::core::memtable::MemTable>, RecordBatch)],
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
        let mut intents = Vec::with_capacity(locked.len());
        let mut cumulative = 0u64;
        for (mt, batch) in locked {
            cumulative += batch.num_rows() as u64;
            match crate::storage::core::durable_tail::begin_flush(
                table_dir,
                mt.id(),
                &segment_file_name,
                cumulative,
            ) {
                Ok(intent) => intents.push(intent),
                Err(error) => {
                    for intent in intents {
                        let _ = crate::storage::core::durable_tail::cancel_flush(intent);
                    }
                    for (mt, _) in locked {
                        if mt.state() == MemTableState::Flushing {
                            mt.set_state(MemTableState::Pending);
                        }
                    }
                    return Err(error);
                }
            }
        }
        let tmp_path = table_dir.join(format!(".tail_flush_{}.tmp", locked[0].0.id().simple()));

        let write_result = (|| {
            let _ = std::fs::remove_file(&tmp_path);
            let _ = remove_sidecar_meta(&tmp_path);
            segment_mgr.write_standalone_active_encoded_segment(&tmp_path, merged)?;
            std::fs::rename(&tmp_path, &final_path)?;
            let _ = remove_sidecar_meta(&tmp_path);
            segment_mgr.open_reader(&final_path)?;
            Ok(())
        })();

        if let Err(error) = write_result {
            let _ = std::fs::remove_file(&tmp_path);
            let _ = remove_sidecar_meta(&tmp_path);
            for intent in intents {
                let _ = crate::storage::core::durable_tail::cancel_flush(intent);
            }
            for (mt, _) in locked {
                if mt.state() == MemTableState::Flushing {
                    mt.set_state(MemTableState::Pending);
                }
            }
            return Err(error);
        }

        for (mt, _) in locked {
            let _ = mt.take_batch();
            mt.set_state(MemTableState::Flushed);
        }
        for intent in intents {
            if let Err(error) = crate::storage::core::durable_tail::finish_flush(table_dir, intent) {
                tracing::warn!(
                    db,
                    table,
                    error = %error,
                    "durable tail group flush cleanup deferred to recovery"
                );
            }
        }
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
    #[allow(clippy::too_many_arguments)]
    fn flush_durable_memtable_under_table_lock(
        segment_mgr: &Arc<SegmentManager>,
        mt: &Arc<crate::storage::core::memtable::MemTable>,
        batch: &RecordBatch,
        db: &str,
        table: &str,
        partition_key: &str,
        txn_id: u64,
    ) -> Result<()> {
        let table_dir = segment_mgr.table_dir(db, table);
        let watermark = segment_mgr.durable_active_segment_watermark_under_table_lock(
            db,
            table,
            partition_key,
            &batch.schema,
            batch.num_rows() as u64,
        )?;
        let Some((segment_file_name, expected_rows)) = watermark else {
            return Self::flush_durable_memtable_as_new_segment(
                segment_mgr,
                mt,
                batch,
                db,
                table,
                partition_key,
                txn_id,
                &table_dir,
            );
        };
        let intent = crate::storage::core::durable_tail::begin_flush(
            &table_dir,
            mt.id(),
            &segment_file_name,
            expected_rows,
        )?;
        // The durable tail already protects these rows, so the shared active
        // segment append runs without the extra transactional WAL write
        // (txn_id = 0). The intent above carries the row-count watermark that
        // lets recovery tell "appended and footer-synced" apart from "crash
        // before the segment footer reached the watermark".
        let flush_result = segment_mgr.flush_memtable_batch_if_under_table_lock(
            db,
            table,
            partition_key,
            batch,
            0,
            || mt.state() == MemTableState::Flushing,
            || {
                let _ = mt.take_batch();
                mt.set_state(MemTableState::Flushed);
            },
        );
        let wrote = match flush_result {
            Ok(wrote) => wrote,
            Err(error) => {
                let _ = crate::storage::core::durable_tail::cancel_flush(intent);
                if mt.state() == MemTableState::Flushing {
                    mt.set_state(MemTableState::Pending);
                }
                return Err(error);
            }
        };
        if !wrote {
            let _ = mt.take_batch();
            mt.set_state(MemTableState::Flushed);
            let _ = crate::storage::core::durable_tail::cancel_flush(intent);
            return Ok(());
        }

        if let Err(error) = crate::storage::core::durable_tail::finish_flush(&table_dir, intent) {
            tracing::warn!(
                db,
                table,
                error = %error,
                "durable tail flush cleanup deferred to recovery"
            );
        }
        tracing::debug!(
            table,
            rows = batch.num_rows(),
            txn_id,
            segment = %segment_file_name,
            expected_rows,
            "durable memtable tail appended to active segment"
        );
        Ok(())
    }

    /// First durable flush for a partition (no appendable active segment
    /// exists yet): write the rows as a complete standalone segment via
    /// tmp+rename so the file only becomes discoverable with a valid embedded
    /// footer. The segment stays unsealed, so later durable flushes find it
    /// through the watermark lookup and append to it in place.
    #[allow(clippy::too_many_arguments)]
    fn flush_durable_memtable_as_new_segment(
        segment_mgr: &Arc<SegmentManager>,
        mt: &Arc<crate::storage::core::memtable::MemTable>,
        batch: &RecordBatch,
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
        let intent = crate::storage::core::durable_tail::begin_flush(
            table_dir,
            mt.id(),
            &segment_file_name,
            batch.num_rows() as u64,
        )?;
        let tmp_path = table_dir.join(format!(".tail_flush_{}.tmp", mt.id().simple()));

        let write_result = (|| {
            let _ = std::fs::remove_file(&tmp_path);
            let _ = remove_sidecar_meta(&tmp_path);
            segment_mgr.write_standalone_active_encoded_segment(&tmp_path, batch)?;
            std::fs::rename(&tmp_path, &final_path)?;
            let _ = remove_sidecar_meta(&tmp_path);
            segment_mgr.open_reader(&final_path)?;
            Ok(())
        })();

        if let Err(error) = write_result {
            let _ = std::fs::remove_file(&tmp_path);
            let _ = remove_sidecar_meta(&tmp_path);
            let _ = crate::storage::core::durable_tail::cancel_flush(intent);
            if mt.state() == MemTableState::Flushing {
                mt.set_state(MemTableState::Pending);
            }
            return Err(error);
        }

        let _ = mt.take_batch();
        mt.set_state(MemTableState::Flushed);
        if let Err(error) = crate::storage::core::durable_tail::finish_flush(table_dir, intent) {
            tracing::warn!(
                db,
                table,
                error = %error,
                "durable tail flush cleanup deferred to recovery"
            );
        }
        tracing::debug!(
            table,
            rows = batch.num_rows(),
            txn_id,
            segment = %segment_file_name,
            "durable memtable tail materialized as new segment"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::types::DataType;
    use crate::storage::segment::SegmentReader;
    use crate::types::batch::ColumnDef;
    use crate::types::scalar::ScalarValue;
    use std::path::{Path, PathBuf};

    const TEST_DB: &str = "main";
    const TEST_TABLE: &str = "t";

    fn test_schema() -> Schema {
        Schema::new(vec![ColumnDef::new("id", DataType::Int64)])
    }

    fn make_batch(schema: &Schema, start_id: i64, rows: usize) -> Result<RecordBatch> {
        let mut batch = RecordBatch::new(schema.clone());
        for offset in 0..rows {
            batch
                .column_mut(0)
                .unwrap()
                .push(&ScalarValue::Int64(Some(start_id + offset as i64)))?;
        }
        Ok(batch)
    }

    fn test_config(max_rows_per_memtable: u64) -> BatchWriterConfig {
        BatchWriterConfig {
            max_rows_per_memtable,
            flush_interval_secs: 3600,
            max_total_memory: 1 << 30,
            worker_count: 1,
            low_rate_max_rows: 1024,
            low_rate_max_age_secs: 3600,
        }
    }

    fn segment_files(table_dir: &Path) -> Vec<PathBuf> {
        let mut files: Vec<PathBuf> = std::fs::read_dir(table_dir)
            .unwrap()
            .flatten()
            .map(|entry| entry.path())
            .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("dat"))
            .collect();
        files.sort();
        files
    }

    fn total_segment_rows(table_dir: &Path) -> u64 {
        segment_files(table_dir)
            .iter()
            .map(|path| SegmentReader::open(path).unwrap().row_count())
            .sum()
    }

    fn tail_entries(table_dir: &Path) -> usize {
        let tail_dir = table_dir.join("memtable_tail");
        if !tail_dir.is_dir() {
            return 0;
        }
        std::fs::read_dir(tail_dir).unwrap().flatten().count()
    }

    /// Repeated durable memtable flushes must append into one shared active
    /// segment instead of sealing one small segment per memtable.
    #[test]
    fn durable_flushes_share_one_active_segment() -> Result<()> {
        let temp = tempfile::tempdir().unwrap();
        let table_dir = temp.path().join(TEST_DB).join(TEST_TABLE);
        std::fs::create_dir_all(&table_dir)?;

        let schema = test_schema();
        let segment_mgr = Arc::new(SegmentManager::new(temp.path())?);
        let mut writer = BatchWriter::new(test_config(4), Arc::clone(&segment_mgr));

        for batch_idx in 0..3 {
            let batch = make_batch(&schema, batch_idx * 4, 4)?;
            writer.write_columns(TEST_DB, TEST_TABLE, "", batch, schema.clone())?;
            writer.flush_table(TEST_DB, TEST_TABLE)?;
        }
        // A final partial memtable also lands in the same active segment.
        let batch = make_batch(&schema, 12, 2)?;
        writer.write_columns(TEST_DB, TEST_TABLE, "", batch, schema.clone())?;
        writer.flush_table(TEST_DB, TEST_TABLE)?;
        writer.stop();

        let files = segment_files(&table_dir);
        assert_eq!(
            files.len(),
            1,
            "durable flushes should share one active segment, got {files:?}"
        );
        assert_eq!(total_segment_rows(&table_dir), 14);
        assert_eq!(tail_entries(&table_dir), 0, "flushed tails must be cleared");
        Ok(())
    }

    /// A crash before any flush leaves the durable tail on disk. Startup
    /// recovery replays it exactly once into the active segment.
    #[test]
    fn crash_before_flush_replays_tail_exactly_once() -> Result<()> {
        let temp = tempfile::tempdir().unwrap();
        let table_dir = temp.path().join(TEST_DB).join(TEST_TABLE);
        std::fs::create_dir_all(&table_dir)?;
        let schema = test_schema();

        {
            let segment_mgr = Arc::new(SegmentManager::new(temp.path())?);
            let mut writer = BatchWriter::new(test_config(1024), Arc::clone(&segment_mgr));
            let batch = make_batch(&schema, 100, 5)?;
            writer.write_columns(TEST_DB, TEST_TABLE, "", batch, schema.clone())?;
            // No flush: stop only halts the background threads, leaving the
            // group-committed tail behind like a crash would.
            writer.stop();
        }
        assert!(tail_entries(&table_dir) > 0, "tail must survive the crash");
        assert_eq!(total_segment_rows(&table_dir), 0);

        let segment_mgr = Arc::new(SegmentManager::new(temp.path())?);
        let mut writer = BatchWriter::new(test_config(1024), Arc::clone(&segment_mgr));
        let recovered = crate::storage::core::durable_tail::recover(&table_dir, &schema)?;
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].batch.num_rows(), 5);
        for tail in recovered {
            writer.memtable_manager().restore_committed_tail(
                tail.id,
                TEST_DB,
                TEST_TABLE,
                &tail.partition_key,
                tail.batch,
            );
        }
        writer.flush_table(TEST_DB, TEST_TABLE)?;
        writer.stop();

        assert_eq!(total_segment_rows(&table_dir), 5);
        assert!(
            crate::storage::core::durable_tail::recover(&table_dir, &schema)?.is_empty(),
            "replayed tail must not be recovered a second time"
        );
        Ok(())
    }

    /// A crash after the rows reached the segment but before tail cleanup is
    /// detected through the flush-intent watermark: no replay, no duplicates.
    #[test]
    fn crash_after_segment_append_suppresses_tail_replay() -> Result<()> {
        let temp = tempfile::tempdir().unwrap();
        let table_dir = temp.path().join(TEST_DB).join(TEST_TABLE);
        std::fs::create_dir_all(&table_dir)?;
        let schema = test_schema();

        let segment_mgr = Arc::new(SegmentManager::new(temp.path())?);
        let mut writer = BatchWriter::new(test_config(1024), Arc::clone(&segment_mgr));
        let batch = make_batch(&schema, 200, 6)?;
        writer.write_columns(TEST_DB, TEST_TABLE, "", batch, schema.clone())?;
        writer.flush_table(TEST_DB, TEST_TABLE)?;
        writer.stop();
        assert_eq!(total_segment_rows(&table_dir), 6);
        assert_eq!(tail_entries(&table_dir), 0);

        // Rebuild the crash window artificially: a tail plus a flush intent
        // whose watermark is already satisfied by the segment contents.
        let id = uuid::Uuid::new_v4();
        let batch = make_batch(&schema, 200, 6)?;
        crate::storage::core::durable_tail::persist(&table_dir, id, "", &batch)?;
        let segment_name = segment_files(&table_dir)[0]
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        let _intent =
            crate::storage::core::durable_tail::begin_flush(&table_dir, id, &segment_name, 6)?;

        let recovered = crate::storage::core::durable_tail::recover(&table_dir, &schema)?;
        assert!(
            recovered.is_empty(),
            "satisfied watermark must suppress replay; got {} tail(s)",
            recovered.len()
        );
        assert_eq!(tail_entries(&table_dir), 0, "recovery cleans up the intent");
        assert_eq!(total_segment_rows(&table_dir), 6);
        Ok(())
    }

    /// A crash in the middle of the append (segment below the watermark) must
    /// replay the tail so no acknowledged rows are lost.
    #[test]
    fn crash_mid_append_replays_tail() -> Result<()> {
        let temp = tempfile::tempdir().unwrap();
        let table_dir = temp.path().join(TEST_DB).join(TEST_TABLE);
        std::fs::create_dir_all(&table_dir)?;
        let schema = test_schema();

        let segment_mgr = Arc::new(SegmentManager::new(temp.path())?);
        let mut writer = BatchWriter::new(test_config(1024), Arc::clone(&segment_mgr));
        let batch = make_batch(&schema, 300, 3)?;
        writer.write_columns(TEST_DB, TEST_TABLE, "", batch, schema.clone())?;
        writer.flush_table(TEST_DB, TEST_TABLE)?;
        writer.stop();
        assert_eq!(total_segment_rows(&table_dir), 3);

        // A second memtable (3 more rows) began flushing: intent demands 6
        // rows in the shared segment, but the crash left only 3 durable.
        let id = uuid::Uuid::new_v4();
        let batch = make_batch(&schema, 303, 3)?;
        crate::storage::core::durable_tail::persist(&table_dir, id, "", &batch)?;
        let segment_name = segment_files(&table_dir)[0]
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        let _intent =
            crate::storage::core::durable_tail::begin_flush(&table_dir, id, &segment_name, 6)?;

        let recovered = crate::storage::core::durable_tail::recover(&table_dir, &schema)?;
        assert_eq!(recovered.len(), 1, "unsatisfied watermark must replay");
        assert_eq!(recovered[0].batch.num_rows(), 3);
        Ok(())
    }
}
