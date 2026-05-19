use std::collections::HashMap;
use std::sync::{Arc, RwLock, Mutex};
use std::time::{Duration, Instant};

use crate::common::error::{MonaxError, Result};
use crate::types::batch::{RecordBatch, Schema};
use crate::types::array::ColumnArray;
use crate::types::scalar::ScalarValue;

const MAX_MEMTABLE_ROWS: usize = 50000;
const MAX_MEMTABLE_MEMORY: usize = 100 * 1024 * 1024;
const FLUSH_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub struct MemTableConfig {
    pub max_rows: usize,
    pub max_memory: usize,
    pub flush_interval: Duration,
}

impl Default for MemTableConfig {
    fn default() -> Self {
        Self {
            max_rows: MAX_MEMTABLE_ROWS,
            max_memory: MAX_MEMTABLE_MEMORY,
            flush_interval: FLUSH_INTERVAL,
        }
    }
}

pub struct MemTable {
    key: String,
    schema: Schema,
    columns: Vec<Vec<ScalarValue>>,
    row_count: usize,
    estimated_memory: usize,
    config: MemTableConfig,
    state: RwLock<MemTableState>,
    created_at: Instant,
}

#[derive(Clone, Copy, PartialEq)]
#[allow(dead_code)]
enum MemTableState {
    Active,
    Flushing,
    Flushed,
}

impl MemTable {
    pub fn new(key: String, schema: Schema, config: MemTableConfig) -> Self {
        let columns = schema.columns.iter()
            .map(|_| Vec::with_capacity(config.max_rows))
            .collect();
        Self {
            key,
            schema,
            columns,
            row_count: 0,
            estimated_memory: 0,
            config,
            state: RwLock::new(MemTableState::Active),
            created_at: Instant::now(),
        }
    }

    pub fn append(&mut self, batch: &RecordBatch) -> Result<usize> {
        {
            let state = self.state.read().unwrap();
            if *state != MemTableState::Active {
                return Err(MonaxError::Internal("memtable not active".into()));
            }
        }

        let rows = batch.num_rows();
        for col_idx in 0..batch.num_columns() {
            let col = batch.column(col_idx).unwrap();
            for row_idx in 0..rows {
                let val = col.scalar(row_idx);
                self.columns[col_idx].push(val.clone());
            }
        }

        self.row_count += rows;
        self.estimated_memory += self.estimate_batch_memory(batch);

        Ok(rows)
    }

    fn estimate_batch_memory(&self, batch: &RecordBatch) -> usize {
        let mut size = 0;
        for col_idx in 0..batch.num_columns() {
            let col = batch.column(col_idx).unwrap();
            size += estimate_column_memory(col);
        }
        size
    }

    pub fn should_flush(&self) -> bool {
        self.row_count >= self.config.max_rows
            || self.estimated_memory >= self.config.max_memory
            || self.created_at.elapsed() >= self.config.flush_interval
    }

    pub fn to_batch(&self) -> Result<RecordBatch> {
        let mut columns: Vec<ColumnArray> = Vec::with_capacity(self.schema.columns.len());
        for (col_idx, col_def) in self.schema.columns.iter().enumerate() {
            let vals = &self.columns[col_idx];
            let arr = ColumnArray::from_scalar_values(col_def.data_type, vals)?;
            columns.push(arr);
        }
        RecordBatch::try_new(self.schema.clone(), columns)
    }

    pub fn row_count(&self) -> usize {
        self.row_count
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    #[allow(private_interfaces)]
    pub fn state(&self) -> MemTableState {
        *self.state.read().unwrap()
    }

    #[allow(private_interfaces)]
    pub fn set_state(&self, state: MemTableState) {
        *self.state.write().unwrap() = state;
    }

    pub fn clear(&mut self) {
        for col in &mut self.columns {
            col.clear();
        }
        self.row_count = 0;
        self.estimated_memory = 0;
        self.created_at = Instant::now();
        *self.state.write().unwrap() = MemTableState::Active;
    }
}

fn estimate_column_memory(col: &ColumnArray) -> usize {
    match col {
        ColumnArray::Bool { data, .. } => data.len(),
        ColumnArray::Int8 { data, .. } => data.len(),
        ColumnArray::Int16 { data, .. } => data.len() * 2,
        ColumnArray::Int32 { data, .. } => data.len() * 4,
        ColumnArray::Int64 { data, .. } => data.len() * 8,
        ColumnArray::UInt8 { data, .. } => data.len(),
        ColumnArray::UInt16 { data, .. } => data.len() * 2,
        ColumnArray::UInt32 { data, .. } => data.len() * 4,
        ColumnArray::UInt64 { data, .. } => data.len() * 8,
        ColumnArray::Float32 { data, .. } => data.len() * 4,
        ColumnArray::Float64 { data, .. } => data.len() * 8,
        ColumnArray::Decimal { data, .. } => data.len() * 8,
        ColumnArray::Date { data, .. } => data.len() * 4,
        ColumnArray::Time { data, .. } => data.len() * 8,
        ColumnArray::Timestamp { data, .. } => data.len() * 8,
        ColumnArray::Interval { data, .. } => data.len() * 8,
        ColumnArray::VarChar { data, .. } => data.iter().map(|s| s.len()).sum(),
        ColumnArray::Blob { data, .. } => data.iter().map(|b| b.len()).sum(),
        ColumnArray::Vector { data, .. } => data.iter().map(|v| v.len() * 4).sum(),
    }
}

pub struct MemTableManager {
    tables: RwLock<HashMap<String, Arc<Mutex<MemTable>>>>,
    config: MemTableConfig,
    table_schema_getter: RwLock<HashMap<String, Schema>>,
}

impl MemTableManager {
    pub fn new(config: MemTableConfig) -> Self {
        Self {
            tables: RwLock::new(HashMap::new()),
            config,
            table_schema_getter: RwLock::new(HashMap::new()),
        }
    }

    pub fn register_schema(&self, key: &str, schema: Schema) {
        let mut getter = self.table_schema_getter.write().unwrap();
        getter.insert(key.to_string(), schema);
    }

    pub fn get_or_create(&self, key: &str, schema: Schema) -> Result<Arc<Mutex<MemTable>>> {
        let tables = self.tables.read().unwrap();
        if let Some(mt) = tables.get(key) {
            return Ok(mt.clone());
        }
        drop(tables);

        let mut tables = self.tables.write().unwrap();
        let mt = Arc::new(Mutex::new(MemTable::new(
            key.to_string(),
            schema,
            self.config.clone(),
        )));
        tables.insert(key.to_string(), mt.clone());
        Ok(mt)
    }

    pub fn get(&self, key: &str) -> Option<Arc<Mutex<MemTable>>> {
        self.tables.read().unwrap().get(key).cloned()
    }

    pub fn remove(&self, key: &str) {
        self.tables.write().unwrap().remove(key);
    }

    pub fn get_all(&self) -> Vec<Arc<Mutex<MemTable>>> {
        self.tables.read().unwrap().values().cloned().collect()
    }

    pub fn get_flushable(&self) -> Vec<Arc<Mutex<MemTable>>> {
        self.tables.read().unwrap().values()
            .filter(|mt| {
                let mt_lock = mt.lock().unwrap();
                mt_lock.should_flush() && mt_lock.state() == MemTableState::Active
            })
            .cloned()
            .collect()
    }
}