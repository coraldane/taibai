#![allow(dead_code)]
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Instant;

use crate::common::error::Result;
use crate::types::batch::{RecordBatch, Schema};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum MemTableState {
    Pending,
    Flushing,
    Flushed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnVisibility {
    Uncommitted(u64),
    Committed,
    Aborted,
}

pub struct MemTable {
    db: String,
    table: String,
    partition_key: String,
    batch: RwLock<Option<RecordBatch>>,
    state: AtomicU8,
    row_count: AtomicU64,
    created_at: Instant,
    txn_id: AtomicU64,
    visibility: RwLock<TxnVisibility>,
}

impl MemTable {
    pub fn new(
        db: String,
        table: String,
        partition_key: String,
        schema: Schema,
        ) -> Self {
        Self {
            db,
            table,
            partition_key,
            batch: RwLock::new(Some(RecordBatch::new(schema))),
            state: AtomicU8::new(MemTableState::Pending as u8),
            row_count: AtomicU64::new(0),
            created_at: Instant::now(),
            txn_id: AtomicU64::new(0),
            visibility: RwLock::new(TxnVisibility::Committed),
        }
    }

    pub fn with_txn(
        db: String,
        table: String,
        partition_key: String,
        schema: Schema,
        txn_id: u64,
        ) -> Self {
        Self {
            db,
            table,
            partition_key,
            batch: RwLock::new(Some(RecordBatch::new(schema))),
            state: AtomicU8::new(MemTableState::Pending as u8),
            row_count: AtomicU64::new(0),
            created_at: Instant::now(),
            txn_id: AtomicU64::new(txn_id),
            visibility: RwLock::new(TxnVisibility::Uncommitted(txn_id)),
        }
    }

    pub fn db(&self) -> &str {
        &self.db
    }

    pub fn table(&self) -> &str {
        &self.table
    }

    pub fn partition_key(&self) -> &str {
        &self.partition_key
    }

    pub fn state(&self) -> MemTableState {
        match self.state.load(Ordering::Acquire) {
            0 => MemTableState::Pending,
            1 => MemTableState::Flushing,
            2 => MemTableState::Flushed,
            _ => MemTableState::Pending,
        }
    }

    pub fn set_state(&self, new_state: MemTableState) {
        self.state.store(new_state as u8, Ordering::Release);
    }

    pub fn row_count(&self) -> u64 {
        self.row_count.load(Ordering::Acquire)
    }

    pub fn txn_id(&self) -> u64 {
        self.txn_id.load(Ordering::Acquire)
    }

    pub fn is_transactional(&self) -> bool {
        self.txn_id() != 0
    }

    pub fn visibility(&self) -> TxnVisibility {
        let guard = self.visibility.read().unwrap();
        *guard
    }

    pub fn mark_committed(&self) {
        let mut guard = self.visibility.write().unwrap();
        *guard = TxnVisibility::Committed;
        self.txn_id.store(0, Ordering::Release);
    }

    pub fn mark_aborted(&self) {
        let mut guard = self.visibility.write().unwrap();
        *guard = TxnVisibility::Aborted;
    }

    pub fn is_visible_to(&self, reader_txn_id: u64) -> bool {
        match self.visibility() {
            TxnVisibility::Committed => true,
            TxnVisibility::Aborted => false,
            TxnVisibility::Uncommitted(writer_txn_id) => writer_txn_id == reader_txn_id,
        }
    }

    pub fn append(&self, batch: &RecordBatch) -> Result<u64> {
        let added = batch.num_rows() as u64;
        if added == 0 {
            return Ok(0);
        }
        let mut guard = self.batch.write().unwrap();
        match guard.as_mut() {
            Some(existing) => {
                for col_idx in 0..batch.num_columns() {
                    if let Some(src) = batch.column(col_idx) {
                        if let Some(dst) = existing.column_mut(col_idx) {
                            dst.extend(src)?;
                        }
                    }
                }
            }
            None => {
                *guard = Some(batch.clone());
            }
        }
        self.row_count.fetch_add(added, Ordering::Release);
        Ok(added)
    }

    pub fn append_owned(&self, batch: RecordBatch) -> Result<u64> {
        let added = batch.num_rows() as u64;
        if added == 0 {
            return Ok(0);
        }
        let mut guard = self.batch.write().unwrap();
        match guard.as_mut() {
            Some(existing) if existing.num_rows() > 0 => {
                for col_idx in 0..batch.num_columns() {
                    if let Some(src) = batch.column(col_idx) {
                        if let Some(dst) = existing.column_mut(col_idx) {
                            dst.extend(src)?;
                        }
                    }
                }
            }
            _ => {
                *guard = Some(batch);
            }
        }
        self.row_count.fetch_add(added, Ordering::Release);
        Ok(added)
    }

    pub fn read(&self) -> Option<RecordBatch> {
        let guard = self.batch.read().unwrap();
        guard.clone()
    }

    pub fn take_batch(&self) -> Option<RecordBatch> {
        let mut guard = self.batch.write().unwrap();
        guard.take()
    }

    pub fn restore_batch(&self, batch: RecordBatch) {
        let rows = batch.num_rows() as u64;
        let mut guard = self.batch.write().unwrap();
        *guard = Some(batch);
        self.row_count.store(rows, Ordering::Release);
    }

    pub fn schema(&self) -> Option<Schema> {
        let guard = self.batch.read().unwrap();
        guard.as_ref().map(|b| b.schema.clone())
    }

    pub fn age_secs(&self) -> u64 {
        self.created_at.elapsed().as_secs()
    }
}


pub struct MemTableManager {
    memtables: RwLock<Vec<Arc<MemTable>>>,
    max_rows_per_memtable: u64,
    max_age_secs: u64,
}

impl MemTableManager {
    pub fn new(max_rows_per_memtable: u64, max_age_secs: u64) -> Self {
        Self {
            memtables: RwLock::new(Vec::new()),
            max_rows_per_memtable,
            max_age_secs,
        }
    }

    pub fn get_or_create(
        &self,
        db: &str,
        table: &str,
        partition_key: &str,
        schema: &Schema,
    ) -> Arc<MemTable> {
        self.get_or_create_with_txn(db, table, partition_key, schema, 0)
    }

    pub fn get_or_create_with_txn(
        &self,
        db: &str,
        table: &str,
        partition_key: &str,
        schema: &Schema,
        txn_id: u64,
    ) -> Arc<MemTable> {
        {
            let memtables = self.memtables.read().unwrap();
            for mt in memtables.iter() {
                if mt.db() == db
                    && mt.table() == table
                    && mt.partition_key() == partition_key
                    && mt.state() == MemTableState::Pending
                    && mt.txn_id() == txn_id
                    && mt.row_count() < self.max_rows_per_memtable
                {
                    return Arc::clone(mt);
                }
            }
        }

        let mut memtables = self.memtables.write().unwrap();
        for mt in memtables.iter() {
            if mt.db() == db
                && mt.table() == table
                && mt.partition_key() == partition_key
                && mt.state() == MemTableState::Pending
                && mt.txn_id() == txn_id
                && mt.row_count() < self.max_rows_per_memtable
            {
                return Arc::clone(mt);
            }
        }
        let mt = if txn_id > 0 {
            Arc::new(MemTable::with_txn(
                db.to_string(),
                table.to_string(),
                partition_key.to_string(),
                schema.clone(),
                txn_id,
                ))
        } else {
            Arc::new(MemTable::new(
                db.to_string(),
                table.to_string(),
                partition_key.to_string(),
                schema.clone(),
                ))
        };
        memtables.push(Arc::clone(&mt));
        mt
    }

    pub fn get_by_table(&self, db: &str, table: &str) -> Vec<Arc<MemTable>> {
        let memtables = self.memtables.read().unwrap();
        memtables
            .iter()
            .filter(|mt| {
                mt.db() == db && mt.table() == table && mt.state() == MemTableState::Pending
            })
            .cloned()
            .collect()
    }

    pub fn get_readable_by_table(&self, db: &str, table: &str) -> Vec<Arc<MemTable>> {
        let memtables = self.memtables.read().unwrap();
        memtables
            .iter()
            .filter(|mt| {
                mt.db() == db
                    && mt.table() == table
                    && matches!(mt.state(), MemTableState::Pending | MemTableState::Flushing)
            })
            .cloned()
            .collect()
    }

    pub fn get_by_txn(&self, txn_id: u64) -> Vec<Arc<MemTable>> {
        let memtables = self.memtables.read().unwrap();
        memtables
            .iter()
            .filter(|mt| mt.txn_id() == txn_id && mt.state() == MemTableState::Pending)
            .cloned()
            .collect()
    }

    pub fn get_visible_to(&self, db: &str, table: &str, reader_txn_id: u64) -> Vec<Arc<MemTable>> {
        let memtables = self.memtables.read().unwrap();
        memtables
            .iter()
            .filter(|mt| {
                mt.db() == db
                    && mt.table() == table
                    && mt.state() == MemTableState::Pending
                    && mt.is_visible_to(reader_txn_id)
            })
            .cloned()
            .collect()
    }

    pub fn get_flushable(&self) -> Vec<Arc<MemTable>> {
        let memtables = self.memtables.read().unwrap();
        memtables
            .iter()
            .filter(|mt| {
                mt.state() == MemTableState::Pending
                    && !mt.is_transactional()
                    && (mt.row_count() >= self.max_rows_per_memtable
                        || mt.age_secs() >= self.max_age_secs)
            })
            .cloned()
            .collect()
    }

    pub fn get_all_pending(&self) -> Vec<Arc<MemTable>> {
        let memtables = self.memtables.read().unwrap();
        memtables
            .iter()
            .filter(|mt| mt.state() == MemTableState::Pending)
            .cloned()
            .collect()
    }

    pub fn remove_flushed(&self) {
        let mut memtables = self.memtables.write().unwrap();
        memtables.retain(|mt| mt.state() != MemTableState::Flushed);
    }

    pub fn clear_table(&self, db: &str, table: &str) -> u64 {
        let mut memtables = self.memtables.write().unwrap();
        let mut cleared = 0u64;
        memtables.retain(|mt| {
            if mt.db() == db && mt.table() == table {
                cleared += mt.row_count();
                false
            } else {
                true
            }
        });
        cleared
    }

    pub fn rollback_txn(&self, txn_id: u64) -> u64 {
        let mut memtables = self.memtables.write().unwrap();
        let mut rolled_back = 0u64;
        memtables.retain(|mt| {
            if mt.txn_id() == txn_id {
                mt.mark_aborted();
                rolled_back += mt.row_count();
                false
            } else {
                true
            }
        });
        rolled_back
    }

    pub fn commit_txn(&self, txn_id: u64) -> Vec<Arc<MemTable>> {
        let memtables = self.memtables.read().unwrap();
        let committed: Vec<Arc<MemTable>> = memtables
            .iter()
            .filter(|mt| mt.txn_id() == txn_id && mt.state() == MemTableState::Pending)
            .cloned()
            .collect();
        for mt in &committed {
            mt.mark_committed();
        }
        committed
    }

    pub fn total_row_count(&self) -> u64 {
        let memtables = self.memtables.read().unwrap();
        memtables
            .iter()
            .filter(|mt| mt.state() == MemTableState::Pending)
            .map(|mt| mt.row_count())
            .sum()
    }

    pub fn txn_row_count(&self, txn_id: u64) -> u64 {
        let memtables = self.memtables.read().unwrap();
        memtables
            .iter()
            .filter(|mt| mt.txn_id() == txn_id && mt.state() == MemTableState::Pending)
            .map(|mt| mt.row_count())
            .sum()
    }

    pub fn txn_tables(&self, txn_id: u64) -> HashMap<(String, String), u64> {
        let memtables = self.memtables.read().unwrap();
        let mut result = HashMap::new();
        for mt in memtables.iter() {
            if mt.txn_id() == txn_id && mt.state() == MemTableState::Pending {
                let key = (mt.db().to_string(), mt.table().to_string());
                *result.entry(key).or_insert(0) += mt.row_count();
            }
        }
        result
    }
}
