#![allow(dead_code)]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use crate::common::error::{Result, TaibaiError};
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
            worker_count: 2,
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

        let worker_count = config.worker_count.max(1);
        let mut workers = Vec::with_capacity(worker_count);
        for i in 0..worker_count {
            let rx = Arc::clone(&receiver);
            let stop_clone = Arc::clone(&stop);
            let mgr_clone = Arc::clone(&segment_mgr);
            let mt_mgr_clone = Arc::clone(&memtable_mgr);
            workers.push(
                std::thread::Builder::new()
                    .name(format!("taibai-bw-worker-{}", i))
                    .spawn(move || {
                        Self::worker_loop(rx, stop_clone, mgr_clone, mt_mgr_clone);
                    })
                    .expect("failed to spawn batch writer worker"),
            );
        }

        let flush_stop = Arc::clone(&stop);
        let flush_interval = config.flush_interval_secs;
        const NOMINAL_BYTES_PER_ROW: u64 = 64;
        let flush_row_high_water = (config.max_total_memory / NOMINAL_BYTES_PER_ROW).max(1);
        let flush_mgr = Arc::clone(&segment_mgr);
        let flush_mt_mgr = Arc::clone(&memtable_mgr);
        let flush_handle = std::thread::Builder::new()
            .name("taibai-bw-flush".into())
            .spawn(move || {
                while !flush_stop.load(Ordering::Acquire) {
                    for _ in 0..flush_interval {
                        if flush_stop.load(Ordering::Acquire) {
                            break;
                        }
                        if flush_mt_mgr.total_row_count() >= flush_row_high_water {
                            if let Err(e) = Self::flush_all_pending(&flush_mgr, &flush_mt_mgr) {
                                tracing::warn!(error = %e, "pressure memtable flush failed");
                            }
                        }
                        std::thread::sleep(std::time::Duration::from_secs(1));
                    }
                    if flush_stop.load(Ordering::Acquire) {
                        break;
                    }
                    if let Err(e) = Self::flush_pending_memtables(&flush_mgr, &flush_mt_mgr) {
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
        }
    }

    pub fn memtable_manager(&self) -> &Arc<MemTableManager> {
        &self.memtable_mgr
    }

    pub fn segment_manager(&self) -> &Arc<SegmentManager> {
        &self.segment_mgr
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
        self.segment_mgr.with_table_mutation_lock(db, table, || {
            self.write_columns_with_txn_under_table_lock(
                db,
                table,
                partition_key,
                batch,
                schema,
                txn_id,
            )
        })
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
                Self::flush_memtable_inner(&self.segment_mgr, &mt, true)?;
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
                    Self::flush_memtable_inner(&self.segment_mgr, memtable, true)?;
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
        for mt in mts {
            Self::flush_memtable(&self.segment_mgr, &mt)?;
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
        for mt in mts {
            Self::flush_memtable(&self.segment_mgr, &mt)?;
        }
        self.memtable_mgr.remove_flushed();
        Ok(())
    }

    pub(crate) fn flush_table_under_table_lock(&self, db: &str, table: &str) -> Result<()> {
        let mts = self.memtable_mgr.get_by_table(db, table);
        for mt in mts {
            Self::flush_memtable_inner(&self.segment_mgr, &mt, true)?;
        }
        self.memtable_mgr.remove_flushed();
        Ok(())
    }

    pub fn flush_all(&self) -> Result<()> {
        let mts = self.memtable_mgr.get_all_pending();
        for mt in mts {
            Self::flush_memtable(&self.segment_mgr, &mt)?;
        }
        self.memtable_mgr.remove_flushed();
        Ok(())
    }

    /// Flush buffers that have no durable low-rate tail. Durable tails remain
    /// resident-on-disk and are restored as memtables at the next startup, so
    /// a routine service restart does not manufacture a tiny row group.
    pub fn flush_all_for_shutdown(&self) -> Result<()> {
        let mts = self.memtable_mgr.get_all_pending();
        for mt in mts {
            let table_dir = self.segment_mgr.table_dir(mt.db(), mt.table());
            if crate::storage::core::durable_tail::has_tail(&table_dir, mt.id()) {
                continue;
            }
            Self::flush_memtable(&self.segment_mgr, &mt)?;
        }
        self.memtable_mgr.remove_flushed();
        Ok(())
    }

    pub fn clear_memtables(&self, db: &str, table: &str) -> u64 {
        let memtables = self.memtable_mgr.get_by_table(db, table);
        let cleared = self.memtable_mgr.clear_table(db, table);
        let table_dir = self.segment_mgr.table_dir(db, table);
        for memtable in memtables {
            let _ = crate::storage::core::durable_tail::clear(&table_dir, memtable.id());
        }
        cleared
    }

    pub fn clear_partition_memtables(&self, db: &str, table: &str, partition_key: &str) -> u64 {
        let memtables = self
            .memtable_mgr
            .get_by_table(db, table)
            .into_iter()
            .filter(|memtable| memtable.partition_key() == partition_key)
            .collect::<Vec<_>>();
        let cleared = self.memtable_mgr.clear_partition(db, table, partition_key);
        let table_dir = self.segment_mgr.table_dir(db, table);
        for memtable in memtables {
            let _ = crate::storage::core::durable_tail::clear(&table_dir, memtable.id());
        }
        cleared
    }

    pub fn rename_memtables(&self, db: &str, old_table: &str, new_table: &str) -> u64 {
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
    }

    fn worker_loop(
        rx: Arc<crossbeam_channel::Receiver<WriteCommand>>,
        stop: Arc<AtomicBool>,
        segment_mgr: Arc<SegmentManager>,
        memtable_mgr: Arc<MemTableManager>,
    ) {
        loop {
            if stop.load(Ordering::Acquire) {
                break;
            }
            match rx.recv_timeout(std::time::Duration::from_secs(1)) {
                Ok(WriteCommand::WriteColumns { .. }) => {}
                Ok(WriteCommand::Flush { db, table }) => {
                    let mts = memtable_mgr.get_by_table(&db, &table);
                    for mt in mts {
                        if let Err(e) = Self::flush_memtable(&segment_mgr, &mt) {
                            tracing::warn!(error = %e, "failed to flush memtable for {}.{}", db, table);
                        }
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
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => continue,
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
            }
        }
    }

    fn flush_pending_memtables(
        segment_mgr: &Arc<SegmentManager>,
        memtable_mgr: &MemTableManager,
    ) -> Result<()> {
        let flushable = memtable_mgr.get_flushable();
        for mt in flushable {
            Self::flush_memtable(segment_mgr, &mt)?;
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
    ) -> Result<()> {
        for mt in memtable_mgr.get_all_pending() {
            if !mt.is_transactional() {
                Self::flush_memtable(segment_mgr, &mt)?;
            }
        }
        memtable_mgr.remove_flushed();
        Ok(())
    }

    fn flush_memtable(
        segment_mgr: &Arc<SegmentManager>,
        mt: &Arc<crate::storage::core::memtable::MemTable>,
    ) -> Result<()> {
        Self::flush_memtable_inner(segment_mgr, mt, false)
    }

    fn flush_memtable_inner(
        segment_mgr: &Arc<SegmentManager>,
        mt: &Arc<crate::storage::core::memtable::MemTable>,
        table_lock_held: bool,
    ) -> Result<()> {
        let db = mt.db().to_string();
        let table = mt.table().to_string();
        if table_lock_held {
            return Self::flush_memtable_under_table_lock(segment_mgr, mt);
        }
        segment_mgr.with_table_mutation_lock(&db, &table, || {
            Self::flush_memtable_under_table_lock(segment_mgr, mt)
        })
    }

    fn flush_memtable_under_table_lock(
        segment_mgr: &Arc<SegmentManager>,
        mt: &Arc<crate::storage::core::memtable::MemTable>,
    ) -> Result<()> {
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
        let final_path = if partition_key.is_empty() {
            segment_mgr.next_segment_path(&table_dir, "")?
        } else {
            segment_mgr.next_partition_segment_path(&table_dir, partition_key)?
        };
        let segment_file_name = final_path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| {
                crate::common::error::TaibaiError::Storage(
                    "durable tail segment path has no file name".into(),
                )
            })?;
        let intent = crate::storage::core::durable_tail::begin_flush(
            &table_dir,
            mt.id(),
            segment_file_name,
        )?;
        let tmp_path = table_dir.join(format!(".tail_flush_{}.tmp", mt.id().simple()));

        let write_result = (|| {
            let _ = std::fs::remove_file(&tmp_path);
            let _ = remove_sidecar_meta(&tmp_path);
            segment_mgr.write_standalone_active_encoded_segment(&tmp_path, batch)?;
            std::fs::rename(&tmp_path, &final_path)?;
            // The sealed segment contains its footer. The temporary sidecar is
            // unnecessary and is deliberately not renamed to a shared
            // partition sidecar path.
            let _ = remove_sidecar_meta(&tmp_path);
            segment_mgr.open_reader(&final_path)?;
            Ok(())
        })();

        if let Err(error) = write_result {
            let _ = std::fs::remove_file(&tmp_path);
            let _ = remove_sidecar_meta(&tmp_path);
            let _ = crate::storage::core::durable_tail::cancel_flush(intent);
            mt.set_state(MemTableState::Pending);
            return Err(error);
        }

        let _ = mt.take_batch();
        mt.set_state(MemTableState::Flushed);
        if let Err(error) = crate::storage::core::durable_tail::finish_flush(&table_dir, intent) {
            // The segment and intent are both durable. Recovery will suppress
            // the source tail even if this best-effort cleanup is interrupted.
            tracing::warn!(db, table, error = %error, "durable tail flush cleanup deferred to recovery");
        }
        tracing::debug!(
            table,
            rows = batch.num_rows(),
            txn_id,
            path = %final_path.display(),
            "durable memtable tail flushed to sealed segment"
        );
        Ok(())
    }
}
