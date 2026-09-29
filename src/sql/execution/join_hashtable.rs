/// Join Hash Table implementation inspired by DuckDB's columnar JoinHashTable.
/// Uses a two-phase approach: Build (insert keys) -> Probe (lookup matches).
/// Stores data in columnar format using RecordBatch for cache efficiency.
use std::collections::HashMap;

use crate::types::array::ColumnArray;
use crate::types::batch::RecordBatch;
use crate::types::scalar::ScalarValue;

/// Key for hash table: a vector of scalar values representing join columns.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct JoinKey(Vec<ScalarValueHashable>);

impl JoinKey {
    pub fn from_scalars(values: &[ScalarValue]) -> Self {
        Self(
            values
                .iter()
                .map(ScalarValueHashable::from_scalar)
                .collect(),
        )
    }

    pub fn to_scalars(&self) -> Vec<ScalarValue> {
        self.0.iter().map(ScalarValueHashable::to_scalar).collect()
    }

    pub fn from_batch(batch: &RecordBatch, row: usize, columns: &[usize]) -> Option<Self> {
        let mut values = Vec::with_capacity(columns.len());
        for &column in columns {
            let array = batch.column(column)?;
            if array.bitmap().is_null(row) {
                return None;
            }
            values.push(ScalarValueHashable::from_column(array, row));
        }
        Some(Self(values))
    }
}

/// Wrapper to make ScalarValue hashable for use in HashMap.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ScalarValueHashable {
    Bool(Option<bool>),
    Int8(Option<i8>),
    Int16(Option<i16>),
    Int32(Option<i32>),
    Int64(Option<i64>),
    UInt8(Option<u8>),
    UInt16(Option<u16>),
    UInt32(Option<u32>),
    UInt64(Option<u64>),
    FloatToI64(Option<i64>), // Floats hashed via ordered-float, stored as bits
    VarChar(Option<String>),
    Date(Option<i32>),
    Time(Option<i64>),
    Timestamp(Option<i64>),
    Null,
}

impl ScalarValueHashable {
    pub fn from_column(column: &ColumnArray, row: usize) -> Self {
        match column {
            ColumnArray::Selected { .. } | ColumnArray::Rle { .. } => {
                Self::from_column(column.flattened(), row)
            }
            ColumnArray::Dictionary { .. } => Self::from_column(&column.materialize_varchar(), row),
            ColumnArray::Bool { data, .. } => Self::Bool(Some(data[row] != 0)),
            ColumnArray::Int8 { data, .. } => Self::Int8(Some(data[row])),
            ColumnArray::Int16 { data, .. } => Self::Int16(Some(data[row])),
            ColumnArray::Int32 { data, .. } => Self::Int32(Some(data[row])),
            ColumnArray::Int64 { data, .. } | ColumnArray::Decimal { data, .. } => {
                Self::Int64(Some(data[row]))
            }
            ColumnArray::UInt8 { data, .. } => Self::UInt8(Some(data[row])),
            ColumnArray::UInt16 { data, .. } => Self::UInt16(Some(data[row])),
            ColumnArray::UInt32 { data, .. } => Self::UInt32(Some(data[row])),
            ColumnArray::UInt64 { data, .. } => Self::UInt64(Some(data[row])),
            ColumnArray::Float32 { data, .. } => Self::FloatToI64(Some(data[row].to_bits() as i64)),
            ColumnArray::Float64 { data, .. } => Self::FloatToI64(Some(data[row].to_bits() as i64)),
            ColumnArray::VarChar { data, .. } => Self::VarChar(Some(data.value(row).to_owned())),
            ColumnArray::Date { data, .. } => Self::Date(Some(data[row])),
            ColumnArray::Time { data, .. } => Self::Time(Some(data[row])),
            ColumnArray::Timestamp { data, .. } => Self::Timestamp(Some(data[row])),
            ColumnArray::Interval { data, .. } => Self::Int64(Some(data[row])),
            ColumnArray::Blob { .. } | ColumnArray::Vector { .. } => Self::Null,
        }
    }

    pub fn from_scalar(v: &ScalarValue) -> Self {
        match v {
            ScalarValue::Bool(b) => ScalarValueHashable::Bool(*b),
            ScalarValue::Int8(i) => ScalarValueHashable::Int8(*i),
            ScalarValue::Int16(i) => ScalarValueHashable::Int16(*i),
            ScalarValue::Int32(i) => ScalarValueHashable::Int32(*i),
            ScalarValue::Int64(i) => ScalarValueHashable::Int64(*i),
            ScalarValue::UInt8(i) => ScalarValueHashable::UInt8(*i),
            ScalarValue::UInt16(i) => ScalarValueHashable::UInt16(*i),
            ScalarValue::UInt32(i) => ScalarValueHashable::UInt32(*i),
            ScalarValue::UInt64(i) => ScalarValueHashable::UInt64(*i),
            ScalarValue::Float32(f) => {
                ScalarValueHashable::FloatToI64(f.map(|v| v.to_bits() as i64))
            }
            ScalarValue::Float64(f) => {
                ScalarValueHashable::FloatToI64(f.map(|v| v.to_bits() as i64))
            }
            ScalarValue::VarChar(s) => ScalarValueHashable::VarChar(s.clone()),
            ScalarValue::Date(d) => ScalarValueHashable::Date(*d),
            ScalarValue::Time(t) => ScalarValueHashable::Time(*t),
            ScalarValue::Timestamp(t) => ScalarValueHashable::Timestamp(*t),
            ScalarValue::Decimal(d) => ScalarValueHashable::Int64(*d),
            _ => ScalarValueHashable::Null,
        }
    }

    pub fn to_scalar(&self) -> ScalarValue {
        match self {
            ScalarValueHashable::Bool(v) => ScalarValue::Bool(*v),
            ScalarValueHashable::Int8(v) => ScalarValue::Int8(*v),
            ScalarValueHashable::Int16(v) => ScalarValue::Int16(*v),
            ScalarValueHashable::Int32(v) => ScalarValue::Int32(*v),
            ScalarValueHashable::Int64(v) => ScalarValue::Int64(*v),
            ScalarValueHashable::UInt8(v) => ScalarValue::UInt8(*v),
            ScalarValueHashable::UInt16(v) => ScalarValue::UInt16(*v),
            ScalarValueHashable::UInt32(v) => ScalarValue::UInt32(*v),
            ScalarValueHashable::UInt64(v) => ScalarValue::UInt64(*v),
            ScalarValueHashable::FloatToI64(v) => {
                ScalarValue::Float64(v.map(|bits| f64::from_bits(bits as u64)))
            }
            ScalarValueHashable::VarChar(v) => ScalarValue::VarChar(v.clone()),
            ScalarValueHashable::Date(v) => ScalarValue::Date(*v),
            ScalarValueHashable::Time(v) => ScalarValue::Time(*v),
            ScalarValueHashable::Timestamp(v) => ScalarValue::Timestamp(*v),
            ScalarValueHashable::Null => ScalarValue::Int64(None),
        }
    }
}

/// Row pointer: identifies a specific row within a specific batch.
#[derive(Debug, Clone, Copy)]
pub struct RowPointer {
    pub batch_idx: usize,
    pub row_idx: usize,
}

/// The Join Hash Table used in the Build phase of hash join.
pub struct JoinHashTable {
    /// Maps join key -> list of row pointers that have this key.
    table: HashMap<JoinKey, Vec<RowPointer>>,
    /// The stored build-side data (collected from all input batches).
    pub build_data: Vec<RecordBatch>,
    /// Column indices used for the join key on the build side.
    key_columns: Vec<usize>,
    /// Track which build rows were matched (for LEFT/FULL JOIN null emission).
    pub matched: Vec<bool>,
    /// Total row count across all build batches.
    total_rows: usize,
    /// Prefix sum of build-batch row counts. `row_offset` is called per
    /// matched row during LEFT/RIGHT/FULL probes; the old re-summing made
    /// marking O(matched × batches).
    batch_offsets: Vec<usize>,
}

impl JoinHashTable {
    pub fn new(key_columns: Vec<usize>) -> Self {
        Self {
            table: HashMap::new(),
            build_data: Vec::new(),
            key_columns,
            matched: Vec::new(),
            total_rows: 0,
            batch_offsets: vec![0],
        }
    }

    /// Build phase: consume a batch and insert its rows into the hash table.
    pub fn insert_batch(&mut self, batch: RecordBatch) {
        let batch_idx = self.build_data.len();
        let num_rows = batch.num_rows();

        for row_idx in 0..num_rows {
            // Build typed keys directly from column buffers. NULL keys do not
            // participate in SQL equality joins.
            let Some(key) = JoinKey::from_batch(&batch, row_idx, &self.key_columns) else {
                self.total_rows += 1;
                continue;
            };
            let ptr = RowPointer { batch_idx, row_idx };
            self.table.entry(key).or_default().push(ptr);
            self.total_rows += 1;
        }

        self.build_data.push(batch);
        let last = self.batch_offsets.last().copied().unwrap_or(0);
        self.batch_offsets.push(last + num_rows);
    }

    /// Finalize the build phase: initialize matched tracking.
    pub fn finalize(&mut self) {
        self.matched = vec![false; self.total_rows];
    }

    /// Probe phase: find matching build rows for a given probe key.
    /// Returns list of (build_batch_idx, build_row_idx) pairs.
    pub fn probe(&self, key_values: &[ScalarValue]) -> Vec<RowPointer> {
        let key = JoinKey::from_scalars(key_values);
        self.table.get(&key).cloned().unwrap_or_default()
    }

    /// Probe one row of a probe batch directly from column storage, without
    /// materializing the row as scalars. The typed key is built from the key
    /// columns only. Returns `None` when the row has a NULL join key (NULL
    /// keys never match, mirroring `insert_batch`) or when there is no match.
    pub fn probe_batch_row<'a>(
        &'a self,
        batch: &RecordBatch,
        row: usize,
        key_columns: &[usize],
    ) -> Option<&'a [RowPointer]> {
        let key = JoinKey::from_batch(batch, row, key_columns)?;
        self.table.get(&key).map(Vec::as_slice)
    }

    /// Mark a build row as matched.
    pub fn mark_matched(&mut self, ptr: &RowPointer) {
        let offset = self.row_offset(ptr.batch_idx, ptr.row_idx);
        if offset < self.matched.len() {
            self.matched[offset] = true;
        }
    }

    /// Whether a build row is already marked matched (semi-join dedup).
    pub fn is_marked(&self, ptr: &RowPointer) -> bool {
        let offset = self.row_offset(ptr.batch_idx, ptr.row_idx);
        offset < self.matched.len() && self.matched[offset]
    }

    /// Get all unmatched build row pointers (for LEFT/FULL JOIN).
    pub fn unmatched_rows(&self) -> Vec<RowPointer> {
        let mut result = Vec::new();
        let mut offset = 0;
        for (batch_idx, batch) in self.build_data.iter().enumerate() {
            for row_idx in 0..batch.num_rows() {
                if !self.matched[offset] {
                    result.push(RowPointer { batch_idx, row_idx });
                }
                offset += 1;
            }
        }
        result
    }

    /// Get a scalar value from a build row.
    pub fn get_scalar(&self, ptr: &RowPointer, col_idx: usize) -> ScalarValue {
        self.build_data[ptr.batch_idx]
            .scalar(ptr.row_idx, col_idx)
            .unwrap_or(ScalarValue::Int64(None))
    }

    /// Number of rows in the hash table.
    pub fn row_count(&self) -> usize {
        self.total_rows
    }

    pub fn unique_key_values(&self) -> Vec<Vec<ScalarValue>> {
        self.table.keys().map(|key| key.to_scalars()).collect()
    }

    fn row_offset(&self, batch_idx: usize, row_idx: usize) -> usize {
        // `batch_offsets` holds the prefix sum (len = batches + 1); the old
        // re-sum over build batches ran once per matched row.
        self.batch_offsets
            .get(batch_idx)
            .copied()
            .unwrap_or(0)
            + row_idx
    }
}

/// Time-key match direction for ASOF joins, derived from the ON-clause
/// inequality. `l.ts >= r.ts` means "most recent right row at or before the
/// left timestamp" (the kdb `aj` flavor).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AsofOp {
    /// `l.ts >= r.ts` — rightmost entry with `entry_ts <= probe_ts`.
    AtOrBefore,
    /// `l.ts > r.ts` — rightmost entry with `entry_ts < probe_ts`.
    Before,
    /// `l.ts <= r.ts` — leftmost entry with `entry_ts >= probe_ts`.
    AtOrAfter,
    /// `l.ts < r.ts` — leftmost entry with `entry_ts > probe_ts`.
    After,
}

/// Asof Join index: per equality-key partition, the right-side rows sorted by
/// the time key, plus the stored right-side batches so matched rows can be
/// fetched. Rows with a NULL equality key or NULL time value never match.
pub struct AsofJoinIndex {
    /// equality key -> entries sorted ascending by time key.
    partitions: HashMap<JoinKey, Vec<(ScalarValue, RowPointer)>>,
    /// The stored right-side data (collected from all input batches).
    pub build_data: Vec<RecordBatch>,
    total_rows: usize,
}

impl Default for AsofJoinIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl AsofJoinIndex {
    pub fn new() -> Self {
        Self {
            partitions: HashMap::new(),
            build_data: Vec::new(),
            total_rows: 0,
        }
    }

    /// Build the partitioned index from the right-side batches.
    pub fn build(&mut self, batches: Vec<RecordBatch>, eq_key_cols: &[usize], ts_col: usize) {
        for (batch_idx, batch) in batches.into_iter().enumerate() {
            for row_idx in 0..batch.num_rows() {
                self.total_rows += 1;
                let Some(key) = JoinKey::from_batch(&batch, row_idx, eq_key_cols) else {
                    continue;
                };
                let ts = batch
                    .scalar(row_idx, ts_col)
                    .unwrap_or(ScalarValue::Int64(None));
                if ts.is_null() {
                    continue;
                }
                self.partitions
                    .entry(key)
                    .or_default()
                    .push((ts, RowPointer { batch_idx, row_idx }));
            }
            self.build_data.push(batch);
        }
        for entries in self.partitions.values_mut() {
            entries.sort_by(|a, b| compare_timestamps(&a.0, &b.0));
        }
    }

    /// Find the matching build row for `probe_ts` inside one partition
    /// (obtained from `partition`), honoring the match direction.
    pub fn find_asof(
        &self,
        entries: &[(ScalarValue, RowPointer)],
        probe_ts: &ScalarValue,
        op: AsofOp,
    ) -> Option<RowPointer> {
        if entries.is_empty() {
            return None;
        }
        match op {
            AsofOp::AtOrBefore | AsofOp::Before => {
                // Rightmost entry with entry_ts <= (or <) probe_ts.
                let mut lo = 0usize;
                let mut hi = entries.len();
                while lo < hi {
                    let mid = lo + (hi - lo) / 2;
                    let ord = compare_timestamps(&entries[mid].0, probe_ts);
                    let go_right = match op {
                        AsofOp::AtOrBefore => ord != std::cmp::Ordering::Greater,
                        _ => ord == std::cmp::Ordering::Less,
                    };
                    if go_right {
                        lo = mid + 1;
                    } else {
                        hi = mid;
                    }
                }
                lo.checked_sub(1).map(|idx| entries[idx].1)
            }
            AsofOp::AtOrAfter | AsofOp::After => {
                // Leftmost entry with entry_ts >= (or >) probe_ts.
                let mut lo = 0usize;
                let mut hi = entries.len();
                while lo < hi {
                    let mid = lo + (hi - lo) / 2;
                    let ord = compare_timestamps(&entries[mid].0, probe_ts);
                    let go_right = match op {
                        AsofOp::AtOrAfter => ord == std::cmp::Ordering::Less,
                        _ => ord != std::cmp::Ordering::Greater,
                    };
                    if go_right {
                        lo = mid + 1;
                    } else {
                        hi = mid;
                    }
                }
                entries.get(lo).map(|(_, ptr)| *ptr)
            }
        }
    }

    /// Look up the partition from already-materialized probe row values,
    /// indexed by the same equality-key columns used at build time. A NULL
    /// equality value yields `None` (no match).
    pub fn partition_for_scalars(
        &self,
        values: &[ScalarValue],
        eq_key_cols: &[usize],
    ) -> Option<&[(ScalarValue, RowPointer)]> {
        let mut hashable = Vec::with_capacity(eq_key_cols.len());
        for &col in eq_key_cols {
            let value = values.get(col)?;
            if value.is_null() {
                return None;
            }
            hashable.push(ScalarValueHashable::from_scalar(value));
        }
        self.partitions.get(&JoinKey(hashable)).map(Vec::as_slice)
    }

    /// Get a scalar value from a stored right-side row.
    pub fn get_scalar(&self, ptr: &RowPointer, col_idx: usize) -> ScalarValue {
        self.build_data[ptr.batch_idx]
            .scalar(ptr.row_idx, col_idx)
            .unwrap_or(ScalarValue::Int64(None))
    }
}

fn compare_timestamps(a: &ScalarValue, b: &ScalarValue) -> std::cmp::Ordering {
    match (a, b) {
        (ScalarValue::Timestamp(Some(l)), ScalarValue::Timestamp(Some(r))) => l.cmp(r),
        (ScalarValue::Int64(Some(l)), ScalarValue::Int64(Some(r))) => l.cmp(r),
        (ScalarValue::Float64(Some(l)), ScalarValue::Float64(Some(r))) => {
            l.partial_cmp(r).unwrap_or(std::cmp::Ordering::Equal)
        }
        _ => std::cmp::Ordering::Equal,
    }
}
