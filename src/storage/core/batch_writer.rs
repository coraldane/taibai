#![allow(dead_code)]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::common::error::Result;
use crate::storage::core::memtable::{MemTableManager, MemTableState};
use crate::storage::segment_manager::SegmentManager;
use crate::types::batch::{RecordBatch, Schema};

const DEFAULT_MAX_ROWS_PER_MEMTABLE: u64 = 500_000;
const DEFAULT_FLUSH_INTERVAL_SECS: u64 = 30;
const DEFAULT_MAX_TOTAL_MEMORY: u64 = 512 * 1024 * 1024;

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
}

impl Default for BatchWriterConfig {
    fn default() -> Self {
        Self {
            max_rows_per_memtable: DEFAULT_MAX_ROWS_PER_MEMTABLE,
            flush_interval_secs: DEFAULT_FLUSH_INTERVAL_SECS,
            max_total_memory: DEFAULT_MAX_TOTAL_MEMORY,
            worker_count: 2,
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
        let flush_mgr = Arc::clone(&segment_mgr);
        let flush_mt_mgr = Arc::clone(&memtable_mgr);
        let flush_handle = std::thread::Builder::new()
            .name("taibai-bw-flush".into())
            .spawn(move || {
                while !flush_stop.load(Ordering::Acquire) {
                    std::thread::sleep(std::time::Duration::from_secs(flush_interval));
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
        let mt =
            self.memtable_mgr
                .get_or_create_with_txn(db, table, partition_key, &schema, txn_id);
        mt.append(&batch)?;

        if txn_id == 0 && mt.row_count() >= self.config.max_rows_per_memtable {
            let _ = self.sender.try_send(WriteCommand::Flush {
                db: db.to_string(),
                table: table.to_string(),
            });
        }
        Ok(row_count)
    }

    pub fn commit_txn(&self, txn_id: u64) -> Result<()> {
        let committed_mts = self.memtable_mgr.commit_txn(txn_id);
        let rows_flushed = committed_mts.len();
        for mt in &committed_mts {
            if mt.row_count() > 0 {
                Self::flush_memtable(&self.segment_mgr, mt)?;
            }
        }
        self.segment_mgr.commit_txn(txn_id)?;
        tracing::debug!(txn_id, rows_flushed, "transaction committed and flushed");
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

    pub fn flush_all(&self) -> Result<()> {
        let mts = self.memtable_mgr.get_all_pending();
        for mt in mts {
            Self::flush_memtable(&self.segment_mgr, &mt)?;
        }
        self.memtable_mgr.remove_flushed();
        Ok(())
    }

    pub fn clear_memtables(&self, db: &str, table: &str) -> u64 {
        self.memtable_mgr.clear_table(db, table)
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
                    let committed_mts = memtable_mgr.commit_txn(txn_id);
                    for mt in committed_mts {
                        if mt.row_count() > 0 {
                            if let Err(e) = Self::flush_memtable(&segment_mgr, &mt) {
                                tracing::warn!(error = %e, txn_id, "failed to flush memtable on commit");
                            }
                        }
                    }
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

    fn flush_memtable(
        segment_mgr: &Arc<SegmentManager>,
        mt: &Arc<crate::storage::core::memtable::MemTable>,
    ) -> Result<()> {
        if mt.state() != MemTableState::Pending {
            return Ok(());
        }
        mt.set_state(MemTableState::Flushing);
        let batch = match mt.take_batch() {
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
        std::fs::create_dir_all(&table_dir)?;

        if txn_id > 0 {
            segment_mgr.write_txn_wal(&table_dir, txn_id, &batch)?;
        }

        if partition_key.is_empty() {
            segment_mgr.bulk_insert_batch(&db, &table, batch, true)?;
        } else {
            segment_mgr.bulk_insert_batch_with_partition(&db, &table, &partition_key, batch)?;
        }

        if txn_id > 0 {
            segment_mgr.sync_table_wal(&table_dir)?;
        }

        tracing::debug!(
            table = %table,
            rows = mt.row_count(),
            txn_id,
            "memtable flushed to active segment"
        );
        mt.set_state(MemTableState::Flushed);
        Ok(())
    }
}
