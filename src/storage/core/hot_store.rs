#![allow(dead_code)]
//! In-memory hot acceleration store.
//!
//! An append-only columnar buffer that indexes the **leading cluster-key
//! column** of a table with a per-value inverted index, so that point lookups
//! of the form `WHERE <key> = ?` gather only the matching rows instead of
//! scanning the whole resident set.
//!
//! This is a *generic* mechanism: the index column is whatever the table
//! declares as its first `CLUSTER BY` column, of any comparable scalar type
//! (integer, float, boolean, varchar, date/time/...). No business-specific
//! assumptions are baked in.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use crate::common::error::{Result, TaibaiError};
use crate::protocol::types::DataType;
use crate::storage::catalog::schema::TableMeta;
use crate::types::array::ColumnArray;
use crate::types::batch::{ColumnDef, RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

/// Interning pool for variable-length key values. A distinct string is stored
/// once and addressed by a `u32` id, so that building the inverted index never
/// re-clones a string for every row (only on first sight of a new value).
pub struct StringPool {
    forward: HashMap<String, u32>,
    reverse: Vec<String>,
}

impl StringPool {
    pub fn new() -> Self {
        Self {
            forward: HashMap::new(),
            reverse: Vec::new(),
        }
    }

    /// Insert-or-return for a string value. Borrows `s` (no allocation on a
    /// cache hit).
    pub fn intern(&mut self, s: &str) -> u32 {
        if let Some(&id) = self.forward.get(s) {
            return id;
        }
        let id = self.reverse.len() as u32;
        self.reverse.push(s.to_string());
        self.forward.insert(s.to_string(), id);
        id
    }

    /// Immutable lookup used on the read/query path. `None` means the value has
    /// never been appended, hence there can be no matching rows.
    pub fn lookup(&self, s: &str) -> Option<u32> {
        self.forward.get(s).copied()
    }

    pub fn lookup_value(&self, id: u32) -> Option<&str> {
        self.reverse.get(id as usize).map(|s| s.as_str())
    }

    pub fn len(&self) -> usize {
        self.reverse.len()
    }
}

impl Default for StringPool {
    fn default() -> Self {
        Self::new()
    }
}

/// Compact, hashable key derived from a scalar value of the index column, used
/// as the inverted-index key. Fixed-width values are carried inline; strings are
/// represented by their interned id (`Str(u32)`) so hashing/eq are cheap.
///
/// `Int`/`UInt`/`Float` are kept as distinct variants so that values of
/// different source types never collide in the map (e.g. an `Int64` value and a
/// `UInt64` value are not merged even if they share a bit pattern).
#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq)]
enum ValueKey {
    Null,
    Bool(bool),
    Int(i64),
    UInt(u64),
    Float(u64), // IEEE-754 bit pattern; NaN folds to a single canonical entry
    Str(u32),   // interned id, resolved via StringPool
}

/// Resolve the index column (leading cluster-key column) for a table. Returns
/// `None` for tables without a cluster key, or whose leading key column is a
/// binary/vector type that cannot be meaningfully indexed.
pub fn resolve_key_column(table_meta: &TableMeta) -> Option<usize> {
    table_meta.hot_time_column.as_ref()?;
    let key = table_meta
        .hot_index_column
        .as_deref()
        .or_else(|| table_meta.cluster_key.first().map(String::as_str))?;
    let idx = (0..table_meta.columns.len())
        .find(|i| table_meta.columns[*i].name.eq_ignore_ascii_case(key))?;
    match table_meta.columns[idx].data_type {
        DataType::Blob | DataType::Vector => None,
        _ => Some(idx),
    }
}

/// Convert a predicate scalar to an index key on the read path. Variable-length
/// strings are resolved against the pool; an unseen string yields `Ok(None)`
/// (no matching rows). Binary/vector values are unsupported.
fn scalar_to_key(sv: &ScalarValue, pool: &StringPool) -> Result<Option<ValueKey>> {
    use ScalarValue::*;
    let key = match sv {
        Blob(_) | Vector(_) => {
            return Err(TaibaiError::Internal(
                "hot store: Blob/Vector are not supported as a cluster-key column".into(),
            ));
        }
        VarChar(Some(s)) => match pool.lookup(s) {
            Some(id) => ValueKey::Str(id),
            None => return Ok(None),
        },
        Bool(Some(b)) => ValueKey::Bool(*b),
        Int8(Some(v)) => ValueKey::Int(*v as i64),
        Int16(Some(v)) => ValueKey::Int(*v as i64),
        Int32(Some(v)) => ValueKey::Int(*v as i64),
        Int64(Some(v)) => ValueKey::Int(*v),
        UInt8(Some(v)) => ValueKey::Int(*v as i64),
        UInt16(Some(v)) => ValueKey::Int(*v as i64),
        UInt32(Some(v)) => ValueKey::Int(*v as i64),
        UInt64(Some(v)) => ValueKey::UInt(*v),
        Float32(Some(v)) => ValueKey::Float((*v as f64).to_bits()),
        Float64(Some(v)) => ValueKey::Float(v.to_bits()),
        Decimal(Some(v)) => ValueKey::Int(*v),
        Date(Some(v)) => ValueKey::Int(*v as i64),
        Time(Some(v)) | Timestamp(Some(v)) | Interval(Some(v)) => ValueKey::Int(*v),
        // Any remaining variant carries `None` -> null key (matches NULL rows).
        _ => ValueKey::Null,
    };
    Ok(Some(key))
}

/// Derive index keys for the rows of a column directly from the native columnar
/// data, avoiding a per-row `ScalarValue` round-trip (which would clone every
/// string). Strings are interned through `pool`.
fn collect_keys(col: &ColumnArray, pool: &mut StringPool) -> Result<Vec<ValueKey>> {
    let n = col.len();
    let bm = col.bitmap();
    let mut out = Vec::with_capacity(n);

    macro_rules! num_keys {
        ($data:expr, $conv:expr) => {{
            let data = $data;
            for i in 0..n {
                if bm.is_null(i) {
                    out.push(ValueKey::Null);
                } else {
                    out.push(($conv)(data[i]));
                }
            }
        }};
    }

    match col {
        ColumnArray::Bool { data, .. } => {
            for i in 0..n {
                out.push(if bm.is_null(i) {
                    ValueKey::Null
                } else {
                    ValueKey::Bool(data[i] != 0)
                });
            }
        }
        ColumnArray::Int8 { data, .. } => num_keys!(data, |v: i8| ValueKey::Int(v as i64)),
        ColumnArray::Int16 { data, .. } => num_keys!(data, |v: i16| ValueKey::Int(v as i64)),
        ColumnArray::Int32 { data, .. } => num_keys!(data, |v: i32| ValueKey::Int(v as i64)),
        ColumnArray::Int64 { data, .. } => num_keys!(data, |v: i64| ValueKey::Int(v)),
        ColumnArray::UInt8 { data, .. } => num_keys!(data, |v: u8| ValueKey::Int(v as i64)),
        ColumnArray::UInt16 { data, .. } => num_keys!(data, |v: u16| ValueKey::Int(v as i64)),
        ColumnArray::UInt32 { data, .. } => num_keys!(data, |v: u32| ValueKey::Int(v as i64)),
        ColumnArray::UInt64 { data, .. } => num_keys!(data, |v: u64| ValueKey::UInt(v)),
        ColumnArray::Float32 { data, .. } => {
            num_keys!(data, |v: f32| ValueKey::Float((v as f64).to_bits()))
        }
        ColumnArray::Float64 { data, .. } => num_keys!(data, |v: f64| ValueKey::Float(v.to_bits())),
        ColumnArray::Decimal { data, .. } => num_keys!(data, |v: i64| ValueKey::Int(v)),
        ColumnArray::Date { data, .. } => num_keys!(data, |v: i32| ValueKey::Int(v as i64)),
        ColumnArray::Time { data, .. } => num_keys!(data, |v: i64| ValueKey::Int(v)),
        ColumnArray::Timestamp { data, .. } => num_keys!(data, |v: i64| ValueKey::Int(v)),
        ColumnArray::Interval { data, .. } => num_keys!(data, |v: i64| ValueKey::Int(v)),
        ColumnArray::VarChar { data, .. } => {
            for i in 0..n {
                if bm.is_null(i) {
                    out.push(ValueKey::Null);
                } else {
                    out.push(ValueKey::Str(pool.intern(&data[i])));
                }
            }
        }
        ColumnArray::Blob { .. } | ColumnArray::Vector { .. } => {
            return Err(TaibaiError::Internal(format!(
                "hot store: column type {} is not supported as a cluster-key column",
                col.data_type().display_name()
            )));
        }
    }
    Ok(out)
}

/// An in-memory, append-only acceleration table.
///
/// Columns are stored in their declared (native) types. A per-value inverted
/// index maps each distinct value of the leading cluster-key column to the row
/// offsets that carry it, enabling O(matched) gather for point lookups.
pub struct HotStore {
    schema: Schema,
    key_column_index: usize,
    columns: RwLock<Vec<ColumnArray>>,
    row_count: AtomicU64,
    fully_resident: AtomicBool,
    resident_partitions: RwLock<HashSet<String>>,
    string_pool: RwLock<StringPool>,
    value_index: RwLock<HashMap<ValueKey, Vec<u32>>>,
    memory_soft_limit: u64,
    memory_hard_limit: u64,
}

impl HotStore {
    pub fn new(schema: Schema, key_column_index: usize) -> Self {
        let columns = build_initial_columns(&schema);
        Self {
            schema,
            key_column_index,
            columns: RwLock::new(columns),
            row_count: AtomicU64::new(0),
            fully_resident: AtomicBool::new(false),
            resident_partitions: RwLock::new(HashSet::new()),
            string_pool: RwLock::new(StringPool::new()),
            value_index: RwLock::new(HashMap::new()),
            memory_soft_limit: 6 * 1024 * 1024 * 1024,
            memory_hard_limit: 8 * 1024 * 1024 * 1024,
        }
    }

    pub fn key_column_index(&self) -> usize {
        self.key_column_index
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn row_count(&self) -> u64 {
        self.row_count.load(Ordering::Acquire)
    }

    pub fn mark_full_resident(&self) {
        self.fully_resident.store(true, Ordering::Release);
    }

    pub fn mark_partition_resident(&self, partition_key: String) {
        self.resident_partitions
            .write()
            .unwrap()
            .insert(partition_key);
    }

    pub fn clear_resident_coverage(&self) {
        self.fully_resident.store(false, Ordering::Release);
        self.resident_partitions.write().unwrap().clear();
    }

    pub fn is_resident_for_partition(&self, partition_key: Option<&str>) -> bool {
        if self.fully_resident.load(Ordering::Acquire) {
            return true;
        }
        let Some(partition_key) = partition_key else {
            return false;
        };
        self.resident_partitions
            .read()
            .unwrap()
            .contains(partition_key)
    }

    /// Number of distinct key values currently indexed.
    pub fn distinct_key_count(&self) -> usize {
        self.value_index.read().unwrap().len()
    }

    /// Append a batch, incrementally maintaining the inverted index by scanning
    /// only the newly added rows (O(added) rather than O(total)).
    pub fn append(&self, batch: &RecordBatch) -> Result<()> {
        let added = batch.num_rows();
        if added == 0 {
            return Ok(());
        }
        if batch.num_columns() != self.schema.num_columns() {
            return Err(TaibaiError::Internal(format!(
                "hot store append: expected {} columns but got {}",
                self.schema.num_columns(),
                batch.num_columns()
            )));
        }

        // Backpressure valve (design §3.1): reject the append before mutating if
        // it would push resident memory past the hard limit. The caller (insert
        // path) reacts by checkpoint-flushing + compacting the persisted prefix,
        // then retrying once.
        let projected = self
            .estimated_memory_bytes()
            .saturating_add(estimate_batch_memory(batch));
        if projected > self.memory_hard_limit {
            tracing::error!(
                used_bytes = projected,
                limit = self.memory_hard_limit,
                "hot store hard memory limit would be exceeded; rejecting append (backpressure)"
            );
            return Err(TaibaiError::ResourceBusy(format!(
                "resident hot store hard memory limit exceeded ({} > {}); flush and retry",
                projected, self.memory_hard_limit
            )));
        }

        let base = self.row_count.load(Ordering::Acquire);
        if base + added as u64 > u32::MAX as u64 {
            return Err(TaibaiError::Internal(
                "hot store: row offset overflow (exceeds u32 range)".into(),
            ));
        }
        let base_u32 = base as u32;

        // Derive index keys for the new rows (interning strings without cloning).
        let key_col = batch.column(self.key_column_index).ok_or_else(|| {
            TaibaiError::Internal("hot store: missing key column in batch".into())
        })?;
        let keys = {
            let mut pool = self.string_pool.write().unwrap();
            collect_keys(key_col, &mut pool)?
        };

        // Extend every column in its native type.
        {
            let mut cols = self.columns.write().unwrap();
            for i in 0..self.schema.num_columns() {
                if let Some(src) = batch.column(i) {
                    cols[i].extend(src)?;
                }
            }
        }

        // Update the inverted index.
        {
            let mut idx = self.value_index.write().unwrap();
            for (j, key) in keys.iter().enumerate() {
                idx.entry(*key).or_default().push(base_u32 + j as u32);
            }
        }

        self.row_count.store(base + added as u64, Ordering::Release);

        let used = self.estimated_memory_bytes();
        if used > self.memory_hard_limit {
            tracing::error!(
                used_bytes = used,
                limit = self.memory_hard_limit,
                "hot store exceeded hard memory limit; backpressure recommended"
            );
        } else if used > self.memory_soft_limit {
            tracing::warn!(
                used_bytes = used,
                limit = self.memory_soft_limit,
                "hot store exceeded soft memory limit"
            );
        }

        Ok(())
    }

    /// Gather all rows whose index-column value equals `key_value`, optionally
    /// projected to a subset of columns. Returns `None` only when the value has
    /// never been seen (caller may treat that as an empty result).
    ///
    /// `min_offset` excludes rows whose append offset is below it. The scan
    /// layer derives `min_offset` as a watermark (= total appended rows minus
    /// rows still resident in live memtables) so that rows already persisted to
    /// segments are not gathered here (the segment scan covers those), avoiding
    /// double-counting. This relies on the single-active-partition invariant:
    /// persisted rows form a contiguous append-prefix of the store.
    pub fn lookup(
        &self,
        key_value: &ScalarValue,
        projection: Option<&[usize]>,
        min_offset: u64,
    ) -> Option<RecordBatch> {
        let key = {
            let pool = self.string_pool.read().unwrap();
            match scalar_to_key(key_value, &pool).ok()? {
                Some(k) => k,
                None => return None,
            }
        };
        let rows = {
            let idx = self.value_index.read().unwrap();
            let raw = idx.get(&key)?;
            if min_offset == 0 {
                raw.clone()
            } else {
                raw.iter()
                    .filter(|&&o| (o as u64) >= min_offset)
                    .copied()
                    .collect()
            }
        };

        let proj: Vec<usize> = match projection {
            Some(p) => p.to_vec(),
            None => (0..self.schema.num_columns()).collect(),
        };

        if rows.is_empty() {
            let col_defs: Vec<ColumnDef> = proj
                .iter()
                .map(|&i| self.schema.columns[i].clone())
                .collect();
            return Some(RecordBatch::new(Schema::new(col_defs)));
        }

        let cols = self.columns.read().unwrap();
        let mut out_cols = Vec::with_capacity(proj.len());
        let mut out_defs = Vec::with_capacity(proj.len());
        for &ci in &proj {
            out_defs.push(self.schema.columns[ci].clone());
            out_cols.push(cols[ci].take(&rows));
        }
        drop(cols);

        RecordBatch::try_new(Schema::new(out_defs), out_cols).ok()
    }

    /// Count rows whose index-column value equals `key_value` without
    /// materialising the batch.
    pub fn count_by_key(&self, key_value: &ScalarValue) -> Option<u64> {
        self.count_by_key_from(key_value, 0)
    }

    pub fn count_by_key_from(&self, key_value: &ScalarValue, min_offset: u64) -> Option<u64> {
        let pool = self.string_pool.read().unwrap();
        let key = scalar_to_key(key_value, &pool).ok()??;
        drop(pool);
        self.value_index.read().unwrap().get(&key).map(|v| {
            if min_offset == 0 {
                v.len() as u64
            } else {
                v.iter().filter(|&&o| (o as u64) >= min_offset).count() as u64
            }
        })
    }

    /// Rough resident-memory estimate in bytes.
    pub fn estimated_memory_bytes(&self) -> u64 {
        let cols = self.columns.read().unwrap();
        let mut total: u64 = 0;
        for (i, col) in cols.iter().enumerate() {
            total += (col.len() as u64) * approx_type_width(&self.schema.columns[i].data_type);
        }
        drop(cols);
        total += (self.string_pool.read().unwrap().len() as u64) * 24; // interned strings
        total += self.row_count() * 4; // inverted-index row-offset storage
        total
    }

    pub fn soft_limit_exceeded(&self) -> bool {
        self.estimated_memory_bytes() > self.memory_soft_limit
    }

    pub fn hard_limit_exceeded(&self) -> bool {
        self.estimated_memory_bytes() > self.memory_hard_limit
    }

    /// Drop the leading `requested` rows (the append-prefix already persisted to
    /// segments) to reclaim resident memory under backpressure. Returns the
    /// number of rows actually dropped. Remaining rows keep their relative
    /// order; the inverted index is rewritten with shifted offsets. This is the
    /// "release the oldest archived interval" safety valve from design §3.1.
    pub fn compact_prefix(&self, requested: u64) -> u64 {
        let total = self.row_count();
        if requested == 0 || total == 0 {
            return 0;
        }
        let n = (requested.min(total)) as usize;
        {
            let mut cols = self.columns.write().unwrap();
            for i in 0..cols.len() {
                let len = cols[i].len();
                if n >= len {
                    cols[i] = ColumnArray::empty(self.schema.columns[i].data_type);
                } else {
                    cols[i] = cols[i].slice_fast(n, len);
                }
            }
        }
        {
            let mut idx = self.value_index.write().unwrap();
            let n_u32 = n as u32;
            for offsets in idx.values_mut() {
                let mut write_pos = 0usize;
                let len = offsets.len();
                for read_pos in 0..len {
                    let o = offsets[read_pos];
                    if o >= n_u32 {
                        offsets[write_pos] = o - n_u32;
                        write_pos += 1;
                    }
                }
                offsets.truncate(write_pos);
            }
            idx.retain(|_, v| !v.is_empty());
        }
        self.row_count.fetch_sub(n as u64, Ordering::Release);
        n as u64
    }

    /// How many appended rows have already been persisted to segments (the
    /// reclaimable prefix), derived from total appended minus live memtable rows
    /// supplied by the caller. Used by the engine to size `compact_prefix`.
    pub fn reclaimable_prefix(&self, live_memtable_rows: u64) -> u64 {
        self.row_count().saturating_sub(live_memtable_rows)
    }
}

fn estimate_batch_memory(batch: &RecordBatch) -> u64 {
    let mut total: u64 = 0;
    for i in 0..batch.num_columns() {
        if let Some(col) = batch.column(i) {
            total += (col.len() as u64) * approx_type_width(&col.data_type());
        }
    }
    total += (batch.num_rows() as u64) * 4; // inverted-index row offsets
    total
}

fn approx_type_width(dt: &DataType) -> u64 {
    match dt {
        DataType::Bool | DataType::Int8 | DataType::UInt8 => 1,
        DataType::Int16 | DataType::UInt16 => 2,
        DataType::Int32 | DataType::UInt32 | DataType::Date | DataType::Float32 => 4,
        DataType::Int64
        | DataType::UInt64
        | DataType::Float64
        | DataType::Decimal
        | DataType::Time
        | DataType::Timestamp
        | DataType::Interval => 8,
        DataType::VarChar => 24,
        DataType::Blob => 32,
        DataType::Vector => 64,
    }
}

fn build_initial_columns(schema: &Schema) -> Vec<ColumnArray> {
    schema
        .columns
        .iter()
        .map(|c| ColumnArray::empty(c.data_type))
        .collect()
}

/// Registry of resident hot stores, keyed by `(db, table)`.
pub struct HotStoreManager {
    stores: RwLock<HashMap<(String, String), Arc<HotStore>>>,
}

impl HotStoreManager {
    pub fn new() -> Self {
        Self {
            stores: RwLock::new(HashMap::new()),
        }
    }

    pub fn get(&self, db: &str, table: &str) -> Option<Arc<HotStore>> {
        let stores = self.stores.read().unwrap();
        stores.get(&(db.to_string(), table.to_string())).cloned()
    }

    /// Return the hot store for a table, creating it lazily if the table is
    /// eligible (i.e. has an indexable leading cluster-key column). Returns
    /// `None` for ineligible tables.
    pub fn get_or_create_for_table(
        &self,
        db: &str,
        table: &str,
        table_meta: &TableMeta,
    ) -> Option<Arc<HotStore>> {
        let key_idx = resolve_key_column(table_meta)?;
        {
            let stores = self.stores.read().unwrap();
            if let Some(existing) = stores.get(&(db.to_string(), table.to_string())) {
                return Some(Arc::clone(existing));
            }
        }
        let mut stores = self.stores.write().unwrap();
        if let Some(existing) = stores.get(&(db.to_string(), table.to_string())) {
            return Some(Arc::clone(existing));
        }
        let schema = Schema::new(table_meta.columns.clone());
        let store = Arc::new(HotStore::new(schema, key_idx));
        stores.insert((db.to_string(), table.to_string()), Arc::clone(&store));
        Some(store)
    }

    /// Feed a batch into the resident store. No-op for ineligible tables.
    pub fn try_append(
        &self,
        db: &str,
        table: &str,
        table_meta: &TableMeta,
        batch: &RecordBatch,
    ) -> Result<()> {
        let Some(store) = self.get_or_create_for_table(db, table, table_meta) else {
            return Ok(());
        };
        store.append(batch)
    }

    /// Drop every resident store (eviction handoff hook).
    pub fn evict_all(&self) {
        let mut stores = self.stores.write().unwrap();
        stores.clear();
    }

    /// Reclaim the persisted prefix of one table's resident store (backpressure).
    pub fn compact_prefix(&self, db: &str, table: &str, n: u64) -> u64 {
        let stores = self.stores.read().unwrap();
        stores
            .get(&(db.to_string(), table.to_string()))
            .map(|s| s.compact_prefix(n))
            .unwrap_or(0)
    }

    /// True when any resident store is past its soft memory limit.
    pub fn any_soft_limit_exceeded(&self) -> bool {
        let stores = self.stores.read().unwrap();
        stores.values().any(|s| s.soft_limit_exceeded())
    }

    pub fn soft_limit_exceeded_for(&self, db: &str, table: &str) -> bool {
        let stores = self.stores.read().unwrap();
        stores
            .get(&(db.to_string(), table.to_string()))
            .map(|s| s.soft_limit_exceeded())
            .unwrap_or(false)
    }

    pub fn remove(&self, db: &str, table: &str) {
        let mut stores = self.stores.write().unwrap();
        stores.remove(&(db.to_string(), table.to_string()));
    }

    pub fn clear_resident_coverage(&self, db: &str, table: &str) {
        if let Some(store) = self.get(db, table) {
            store.clear_resident_coverage();
        }
    }
}

impl Default for HotStoreManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::batch::ColumnDef;

    /// A table clustered by a varchar leading key, plus a secondary key.
    fn varchar_key_schema() -> Schema {
        Schema::new(vec![
            ColumnDef::new("category", DataType::VarChar),
            ColumnDef::new("ts", DataType::Timestamp),
            ColumnDef::new("price", DataType::Float64),
            ColumnDef::new("volume", DataType::Int64),
        ])
    }

    fn varchar_key_meta(schema: &Schema) -> TableMeta {
        let mut meta = TableMeta::new("main", "events", schema.columns.clone());
        meta.cluster_key = vec!["category".to_string(), "ts".to_string()];
        meta.hot_time_column = Some("ts".to_string());
        meta
    }

    fn build_varchar_batch(
        category: &str,
        ts: &[i64],
        prices: &[f64],
        vols: &[i64],
    ) -> RecordBatch {
        let mut batch = RecordBatch::new(varchar_key_schema());
        let c0 = batch.column_mut(0).unwrap();
        for _ in 0..ts.len() {
            c0.push(&ScalarValue::VarChar(Some(category.to_string())))
                .unwrap();
        }
        let c1 = batch.column_mut(1).unwrap();
        for v in ts {
            c1.push(&ScalarValue::Timestamp(Some(*v))).unwrap();
        }
        let c2 = batch.column_mut(2).unwrap();
        for v in prices {
            c2.push(&ScalarValue::Float64(Some(*v))).unwrap();
        }
        let c3 = batch.column_mut(3).unwrap();
        for v in vols {
            c3.push(&ScalarValue::Int64(Some(*v))).unwrap();
        }
        batch
    }

    #[test]
    fn append_then_lookup_varchar_key() {
        let schema = varchar_key_schema();
        let meta = varchar_key_meta(&schema);
        let mgr = HotStoreManager::new();
        let batch = build_varchar_batch("alpha", &[1, 2, 3], &[10.0, 10.5, 11.0], &[100, 200, 300]);
        mgr.try_append("main", "events", &meta, &batch).unwrap();

        let store = mgr.get("main", "events").unwrap();
        assert_eq!(store.row_count(), 3);

        let result = store
            .lookup(&ScalarValue::VarChar(Some("alpha".into())), None, 0)
            .unwrap();
        assert_eq!(result.num_rows(), 3);
        assert_eq!(result.num_columns(), 4);
        match result.column(0).unwrap() {
            ColumnArray::VarChar { data, .. } => {
                assert_eq!(data, &vec!["alpha".to_string(); 3]);
            }
            _ => panic!("expected varchar key column"),
        }
        match result.column(3).unwrap() {
            ColumnArray::Int64 { data, .. } => assert_eq!(data, &vec![100, 200, 300]),
            _ => panic!("expected int64 volume"),
        }
    }

    #[test]
    fn lookup_unknown_value_returns_none() {
        let schema = varchar_key_schema();
        let meta = varchar_key_meta(&schema);
        let mgr = HotStoreManager::new();
        let batch = build_varchar_batch("alpha", &[1], &[10.0], &[100]);
        mgr.try_append("main", "events", &meta, &batch).unwrap();

        let store = mgr.get("main", "events").unwrap();
        assert!(store
            .lookup(&ScalarValue::VarChar(Some("zzz".into())), None, 0)
            .is_none());
        assert_eq!(
            store.count_by_key(&ScalarValue::VarChar(Some("zzz".into()))),
            None
        );
    }

    #[test]
    fn incremental_index_across_appends() {
        let schema = varchar_key_schema();
        let meta = varchar_key_meta(&schema);
        let mgr = HotStoreManager::new();
        mgr.try_append(
            "main",
            "events",
            &meta,
            &build_varchar_batch("A", &[1, 2], &[1.0, 2.0], &[1, 2]),
        )
        .unwrap();
        mgr.try_append(
            "main",
            "events",
            &meta,
            &build_varchar_batch("B", &[3], &[3.0], &[3]),
        )
        .unwrap();
        mgr.try_append(
            "main",
            "events",
            &meta,
            &build_varchar_batch("A", &[4], &[4.0], &[4]),
        )
        .unwrap();

        let store = mgr.get("main", "events").unwrap();
        assert_eq!(store.row_count(), 4);
        assert_eq!(store.distinct_key_count(), 2);
        assert_eq!(
            store.count_by_key(&ScalarValue::VarChar(Some("A".into()))),
            Some(3)
        );
        let a = store
            .lookup(&ScalarValue::VarChar(Some("A".into())), Some(&[1, 3]), 0)
            .unwrap();
        assert_eq!(a.num_rows(), 3);
        assert_eq!(a.num_columns(), 2);
    }

    #[test]
    fn integer_key_point_lookup() {
        // Demonstrates generality: an integer cluster-key column is indexed too.
        let schema = Schema::new(vec![
            ColumnDef::new("id", DataType::Int64),
            ColumnDef::new("payload", DataType::VarChar),
        ]);
        let mut meta = TableMeta::new("main", "kv", schema.columns.clone());
        meta.cluster_key = vec!["id".to_string()];
        meta.hot_time_column = Some("id".to_string());

        let mgr = HotStoreManager::new();
        let mut batch = RecordBatch::new(schema.clone());
        let c0 = batch.column_mut(0).unwrap();
        for v in [10i64, 20, 10, 30] {
            c0.push(&ScalarValue::Int64(Some(v))).unwrap();
        }
        let c1 = batch.column_mut(1).unwrap();
        for s in ["a", "b", "c", "d"] {
            c1.push(&ScalarValue::VarChar(Some(s.into()))).unwrap();
        }
        mgr.try_append("main", "kv", &meta, &batch).unwrap();

        let store = mgr.get("main", "kv").unwrap();
        assert_eq!(store.count_by_key(&ScalarValue::Int64(Some(10))), Some(2));
        let r = store
            .lookup(&ScalarValue::Int64(Some(10)), None, 0)
            .unwrap();
        assert_eq!(r.num_rows(), 2);
        match r.column(0).unwrap() {
            ColumnArray::Int64 { data, .. } => assert_eq!(data, &vec![10, 10]),
            _ => panic!("expected int64 key column"),
        }
    }

    #[test]
    fn lookup_watermark_excludes_flushed_prefix() {
        let schema = varchar_key_schema();
        let meta = varchar_key_meta(&schema);
        let mgr = HotStoreManager::new();
        // Append order -> offsets: A@0, A@1, B@2, A@3  (A at [0,1,3]).
        mgr.try_append(
            "main",
            "events",
            &meta,
            &build_varchar_batch("A", &[1, 2], &[1.0, 2.0], &[1, 2]),
        )
        .unwrap();
        mgr.try_append(
            "main",
            "events",
            &meta,
            &build_varchar_batch("B", &[3], &[3.0], &[3]),
        )
        .unwrap();
        mgr.try_append(
            "main",
            "events",
            &meta,
            &build_varchar_batch("A", &[4], &[4.0], &[4]),
        )
        .unwrap();

        let store = mgr.get("main", "events").unwrap();
        assert_eq!(store.row_count(), 4);

        // No watermark: all three A rows.
        let all = store
            .lookup(&ScalarValue::VarChar(Some("A".into())), Some(&[3]), 0)
            .unwrap();
        assert_eq!(all.num_rows(), 3);

        // Watermark 3 simulates "offsets [0,3) already persisted to segments";
        // only the live A row at offset 3 is gathered (volume = 4).
        let live = store
            .lookup(&ScalarValue::VarChar(Some("A".into())), Some(&[3]), 3)
            .unwrap();
        assert_eq!(live.num_rows(), 1);
        match live.column(0).unwrap() {
            ColumnArray::Int64 { data, .. } => assert_eq!(data, &vec![4]),
            _ => panic!("expected volume column"),
        }

        // Watermark past every A offset -> empty result (segment scan covers them).
        let none = store
            .lookup(&ScalarValue::VarChar(Some("A".into())), Some(&[3]), 4)
            .unwrap();
        assert_eq!(none.num_rows(), 0);
    }

    #[test]
    fn ineligible_table_is_noop() {
        // No cluster key -> not eligible.
        let schema = Schema::new(vec![
            ColumnDef::new("id", DataType::Int64),
            ColumnDef::new("v", DataType::Float64),
        ]);
        let meta = TableMeta::new("main", "t", schema.columns.clone());
        let mgr = HotStoreManager::new();
        assert!(resolve_key_column(&meta).is_none());
        assert!(mgr.get("main", "t").is_none());

        // Blob leading cluster key -> not eligible.
        let schema2 = Schema::new(vec![
            ColumnDef::new("raw", DataType::Blob),
            ColumnDef::new("v", DataType::Float64),
        ]);
        let mut meta2 = TableMeta::new("main", "t2", schema2.columns.clone());
        meta2.cluster_key = vec!["raw".to_string()];
        assert!(resolve_key_column(&meta2).is_none());
    }

    #[test]
    fn resolve_key_column_prefers_hot_index_column() {
        let schema = Schema::new(vec![
            ColumnDef::new("trade_time", DataType::Timestamp),
            ColumnDef::new("security_id", DataType::VarChar),
        ]);
        let mut meta = TableMeta::new("main", "tick", schema.columns.clone());
        meta.cluster_key = vec!["trade_time".to_string()];
        meta.hot_time_column = Some("trade_time".to_string());
        meta.hot_index_column = Some("security_id".to_string());
        let idx = resolve_key_column(&meta).expect("resolvable");
        assert_eq!(
            idx, 1,
            "must prefer hot_index_column over cluster_key leading column"
        );

        meta.hot_index_column = None;
        let idx = resolve_key_column(&meta).expect("resolvable");
        assert_eq!(idx, 0, "must fall back to cluster_key leading column");
    }
}
