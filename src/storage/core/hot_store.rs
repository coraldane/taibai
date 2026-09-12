//! In-memory hot acceleration store.
//!
//! The layout follows the kdb+ HDB/RDB split:
//!
//! * The resident set is chopped into fixed-size **chunks** (default 64Ki
//!   rows). A chunk whose columns and inverted index are complete is *sealed*
//!   and published as an immutable `Arc`, exactly like an immutable q HDB
//!   partition: readers gather from sealed chunks with **no data locks at
//!   all** (only a shared hold on the chunk-list snapshot).
//! * A single writable **open chunk** is the RDB analogue. Appends are bulk
//!   `memcpy` into its column vectors plus incremental inverted-index upkeep
//!   for the new rows only, under one writer mutex. A sealed chunk carries its
//!   own copy of the `g#`-style group index (per distinct key value -> row
//!   offsets), so the write path never revisits old data.
//! * `compact_prefix` releases whole chunks below the reclaimed prefix by
//!   advancing a `front` cursor, so reclaim cost is O(chunks) rather than
//!   O(rows) and surviving rows are not re-indexed. Row ids are interpreted
//!   relative to `front`, which keeps the scan layer's watermark arithmetic
//!   (`row_count - live memtable rows`) exact.
//!
//! The index column is the leading `CLUSTER BY` column (or the explicit
//! `hot_index_column`), of any comparable scalar type. No business-specific
//! assumptions are baked in.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use crate::common::error::{Result, TaibaiError};
use crate::protocol::types::DataType;
use crate::storage::catalog::schema::TableMeta;
use crate::storage::partition::pruning::{RangeClause, RangeOp};
use crate::types::array::{BinaryBuffer, ColumnArray, Utf8Buffer};
use crate::types::batch::{ColumnDef, RecordBatch, Schema};
use crate::types::bitmap::NullBitmap;
use crate::types::scalar::ScalarValue;

/// Rows per sealed chunk. Sealed chunks are immutable `Arc`s, so this is the
/// granularity of lock-free reads and of prefix reclamation.
const DEFAULT_CHUNK_ROWS: usize = 65_536;

#[derive(Debug, Clone, Default)]
pub struct ResidentCoverageSnapshot {
    fully_resident: bool,
    resident_partitions: HashSet<String>,
}

/// Interning pool for variable-length key values. A distinct string is stored
/// once and addressed by a `u32` id, so that building the inverted index never
/// re-clones a string for every row (only on first sight of a new value).
///
/// This mirrors q's enumerated `` `sym$ `` symbols: the index compares interned
/// ids, never string bytes.
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

    pub fn len(&self) -> usize {
        self.reverse.len()
    }

    pub fn is_empty(&self) -> bool {
        self.reverse.is_empty()
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
            for (i, &row_value) in data.iter().enumerate().take(n) {
                if bm.is_null(i) {
                    out.push(ValueKey::Null);
                } else {
                    out.push(($conv)(row_value));
                }
            }
        }};
    }

    match col {
        ColumnArray::Bool { data, .. } => {
            for (i, &raw) in data.iter().enumerate().take(n) {
                out.push(if bm.is_null(i) {
                    ValueKey::Null
                } else {
                    ValueKey::Bool(raw != 0)
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

// ---------------------------------------------------------------------------
// Typed predicate pre-compilation.
//
// `ColumnArray::compare_scalar_at` re-matches the (column, scalar) enum pair on
// every row. The lookup loops run one tight filter pass over each chunk's
// posting list, so each predicate is resolved once per chunk into a typed
// comparator and the per-row work is a direct slice index + `Ord::cmp`.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CmpOp {
    Eq,
    Lt,
    Le,
    Gt,
    Ge,
}

impl CmpOp {
    #[inline]
    fn matches(self, ord: std::cmp::Ordering) -> bool {
        match self {
            CmpOp::Eq => ord.is_eq(),
            CmpOp::Lt => ord.is_lt(),
            CmpOp::Le => !ord.is_gt(),
            CmpOp::Gt => ord.is_gt(),
            CmpOp::Ge => !ord.is_lt(),
        }
    }
}

/// A predicate bound to one chunk's typed column storage.
enum FastPred<'a> {
    Bool { data: &'a [u8], v: bool },
    I8 { data: &'a [i8], v: i8 },
    I16 { data: &'a [i16], v: i16 },
    I32 { data: &'a [i32], v: i32 },
    I64 { data: &'a [i64], v: i64 },
    U8 { data: &'a [u8], v: u8 },
    U16 { data: &'a [u16], v: u16 },
    U32 { data: &'a [u32], v: u32 },
    U64 { data: &'a [u64], v: u64 },
    F32 { data: &'a [f32], v: f32 },
    F64 { data: &'a [f64], v: f64 },
    Str { data: &'a Utf8Buffer, v: &'a str },
    Bytes { data: &'a BinaryBuffer, v: &'a [u8] },
}

impl FastPred<'_> {
    #[inline]
    fn compare(&self, row: usize) -> Option<std::cmp::Ordering> {
        match *self {
            FastPred::Bool { data, v } => Some((data[row] != 0).cmp(&v)),
            FastPred::I8 { data, v } => Some(data[row].cmp(&v)),
            FastPred::I16 { data, v } => Some(data[row].cmp(&v)),
            FastPred::I32 { data, v } => Some(data[row].cmp(&v)),
            FastPred::I64 { data, v } => Some(data[row].cmp(&v)),
            FastPred::U8 { data, v } => Some(data[row].cmp(&v)),
            FastPred::U16 { data, v } => Some(data[row].cmp(&v)),
            FastPred::U32 { data, v } => Some(data[row].cmp(&v)),
            FastPred::U64 { data, v } => Some(data[row].cmp(&v)),
            FastPred::F32 { data, v } => data[row].partial_cmp(&v),
            FastPred::F64 { data, v } => data[row].partial_cmp(&v),
            FastPred::Str { data, v } => Some(data.value(row).cmp(v)),
            FastPred::Bytes { data, v } => Some(data.value(row).cmp(v)),
        }
    }
}

/// A fully-resolved row predicate: either a typed fast comparator or the
/// canonical scalar path (cross-type comparisons and any other pair).
enum RowPred<'c, 'v> {
    Fast {
        bitmap: &'c NullBitmap,
        pred: FastPred<'c>,
        op: CmpOp,
    },
    Generic {
        column: &'c ColumnArray,
        value: &'v ScalarValue,
        op: CmpOp,
    },
    AlwaysFalse,
}

impl RowPred<'_, '_> {
    #[inline]
    fn matches(&self, row: usize) -> bool {
        match self {
            RowPred::Fast { bitmap, pred, op } => {
                if bitmap.is_null(row) {
                    return false;
                }
                pred.compare(row).is_some_and(|ord| op.matches(ord))
            }
            RowPred::Generic { column, value, op } => column
                .compare_scalar_at(row, value)
                .is_some_and(|ord| op.matches(ord)),
            RowPred::AlwaysFalse => false,
        }
    }
}

fn compile_one<'c, 'v>(
    column: Option<&'c ColumnArray>,
    value: &'v ScalarValue,
    op: CmpOp,
) -> RowPred<'c, 'v>
where
    'v: 'c,
{
    use ScalarValue as SV;
    let Some(col) = column else {
        return RowPred::AlwaysFalse;
    };
    macro_rules! typed {
        ($variant:ident, $scalar:ident, $field:ident) => {
            if let (ColumnArray::$variant { data, .. }, SV::$scalar(Some(v))) = (col, value) {
                return RowPred::Fast {
                    bitmap: col.bitmap(),
                    pred: FastPred::$field {
                        data,
                        v: v.clone(),
                    },
                    op,
                };
            }
        };
    }
    typed!(Bool, Bool, Bool);
    typed!(Int8, Int8, I8);
    typed!(Int16, Int16, I16);
    typed!(Int32, Int32, I32);
    typed!(Int64, Int64, I64);
    typed!(UInt8, UInt8, U8);
    typed!(UInt16, UInt16, U16);
    typed!(UInt32, UInt32, U32);
    typed!(UInt64, UInt64, U64);
    typed!(Float32, Float32, F32);
    typed!(Float64, Float64, F64);
    typed!(Decimal, Decimal, I64);
    typed!(Date, Date, I32);
    typed!(Time, Time, I64);
    typed!(Timestamp, Timestamp, I64);
    typed!(Interval, Interval, I64);
    if let (ColumnArray::VarChar { data, .. }, SV::VarChar(Some(v))) = (col, value) {
        return RowPred::Fast {
            bitmap: col.bitmap(),
            pred: FastPred::Str {
                data,
                v: v.as_str(),
            },
            op,
        };
    }
    if let (ColumnArray::Blob { data, .. }, SV::Blob(Some(v))) = (col, value) {
        return RowPred::Fast {
            bitmap: col.bitmap(),
            pred: FastPred::Bytes {
                data,
                v: v.as_slice(),
            },
            op,
        };
    }
    RowPred::Generic {
        column: col,
        value,
        op,
    }
}

/// Pre-resolve the pushed-down predicates against one chunk's columns. The
/// index-covered key column (and the partition column when the composite index
/// was used) are skipped, mirroring the guarantee that every candidate row
/// already satisfies them.
fn compile_predicates<'c, 'v>(
    columns: &'c [ColumnArray],
    eq_predicates: &'v [(usize, ScalarValue)],
    range_predicates: &'v [RangeClause],
    covered_key_column: usize,
    covered_partition_column: Option<usize>,
) -> Vec<RowPred<'c, 'v>>
where
    'v: 'c,
{
    let mut preds = Vec::new();
    for (column, value) in eq_predicates {
        if *column == covered_key_column || Some(*column) == covered_partition_column {
            continue;
        }
        preds.push(compile_one(columns.get(*column), value, CmpOp::Eq));
    }
    for clause in range_predicates {
        let op = match clause.op {
            RangeOp::Eq => CmpOp::Eq,
            RangeOp::Lt => CmpOp::Lt,
            RangeOp::Le => CmpOp::Le,
            RangeOp::Gt => CmpOp::Gt,
            RangeOp::Ge => CmpOp::Ge,
        };
        preds.push(compile_one(
            columns.get(clause.column_index),
            &clause.value,
            op,
        ));
    }
    preds
}

// ---------------------------------------------------------------------------
// Chunks
// ---------------------------------------------------------------------------

/// Group index of one chunk: per distinct key value -> ascending local row
/// offsets (the `g#` attribute analogue), plus the composite key/partition
/// index used when a lookup pins the partition column.
struct GroupIndex {
    key: HashMap<ValueKey, Vec<u32>>,
    key_partition: Option<HashMap<(ValueKey, ValueKey), Vec<u32>>>,
}

impl GroupIndex {
    fn empty(partitioned: bool) -> Self {
        Self {
            key: HashMap::new(),
            key_partition: partitioned.then(HashMap::new),
        }
    }

    fn estimate_bytes(&self) -> u64 {
        let key_entry = std::mem::size_of::<ValueKey>() as u64 + 24; // slot + Vec header
        let mut bytes = self.key.len() as u64 * key_entry;
        bytes += self
            .key
            .values()
            .map(|v| v.capacity() as u64 * 4)
            .sum::<u64>();
        if let Some(kp) = &self.key_partition {
            let entry = (std::mem::size_of::<ValueKey>() as u64) * 2 + 24;
            bytes += kp.len() as u64 * entry;
            bytes += kp.values().map(|v| v.capacity() as u64 * 4).sum::<u64>();
        }
        bytes
    }
}

/// A sealed (immutable) chunk. `base` is the global row id of local offset 0;
/// global ids only move relative to the store's `front` cursor.
struct Chunk {
    base: u64,
    rows: u32,
    columns: Vec<ColumnArray>,
    index: GroupIndex,
    memory_bytes: u64,
}

/// Read-only view shared by sealed chunks and the open chunk, so the lookup
/// walk can treat both uniformly.
trait ChunkView {
    fn base(&self) -> u64;
    fn num_rows(&self) -> u64;
    fn columns(&self) -> &[ColumnArray];
    fn key_postings(&self, key: &ValueKey) -> Option<&Vec<u32>>;
    fn key_partition_postings(&self, key: &ValueKey, partition: &ValueKey) -> Option<&Vec<u32>>;
}

impl ChunkView for Chunk {
    fn base(&self) -> u64 {
        self.base
    }
    fn num_rows(&self) -> u64 {
        self.rows as u64
    }
    fn columns(&self) -> &[ColumnArray] {
        &self.columns
    }
    fn key_postings(&self, key: &ValueKey) -> Option<&Vec<u32>> {
        self.index.key.get(key)
    }
    fn key_partition_postings(&self, key: &ValueKey, partition: &ValueKey) -> Option<&Vec<u32>> {
        self.index.key_partition.as_ref()?.get(&(*key, *partition))
    }
}

/// The single writable chunk. Appends bulk-copy columns and maintain the group
/// index for the new rows only; readers take this chunk's lock briefly, so they
/// never observe a half-extended batch.
struct OpenChunk {
    base: u64,
    rows: usize,
    columns: Vec<ColumnArray>,
    index: GroupIndex,
}

impl OpenChunk {
    fn new(schema: &Schema, base: u64, chunk_rows: usize, partitioned: bool) -> Self {
        Self {
            base,
            rows: 0,
            columns: schema
                .columns
                .iter()
                .map(|c| ColumnArray::with_capacity(c.data_type, chunk_rows))
                .collect(),
            index: GroupIndex::empty(partitioned),
        }
    }

    /// Freeze a copy of this chunk's current content into an immutable chunk.
    /// The open chunk keeps its data so readers already holding the lock (or
    /// about to acquire it) still observe every published row.
    fn freeze_at(&self, base: u64) -> Chunk {
        let memory_bytes = self.columns.iter().map(|c| c.estimated_size_bytes() as u64).sum::<u64>()
            + self.index.estimate_bytes();
        Chunk {
            base,
            rows: self.rows as u32,
            columns: self.columns.clone(),
            index: GroupIndex {
                key: self.index.key.clone(),
                key_partition: self.index.key_partition.clone(),
            },
            memory_bytes,
        }
    }

    fn estimated_memory_bytes(&self) -> u64 {
        self.columns.iter().map(|c| c.estimated_size_bytes() as u64).sum::<u64>()
            + self.index.estimate_bytes()
    }
}

impl ChunkView for OpenChunk {
    fn base(&self) -> u64 {
        self.base
    }
    fn num_rows(&self) -> u64 {
        self.rows as u64
    }
    fn columns(&self) -> &[ColumnArray] {
        &self.columns
    }
    fn key_postings(&self, key: &ValueKey) -> Option<&Vec<u32>> {
        self.index.key.get(key)
    }
    fn key_partition_postings(&self, key: &ValueKey, partition: &ValueKey) -> Option<&Vec<u32>> {
        self.index.key_partition.as_ref()?.get(&(*key, *partition))
    }
}

/// Reader-visible snapshot: the `front` cursor, the sealed chunk list and the
/// open chunk handle. Lookups hold the `RwLock` read guard for their whole
/// walk (readers never block readers; only seal/compact take the write side),
/// which keeps a query's view atomic against prefix reclamation.
///
/// `summary` aggregates the per-chunk group indexes so a watermark-free COUNT
/// probes one hash instead of every chunk. It is maintained under the writer
/// lock at seal/compact time.
struct StoreState {
    front: u64,
    sealed: Vec<Arc<Chunk>>,
    summary: SealedSummary,
    open: Arc<RwLock<OpenChunk>>,
}

/// Seal-time rollup of the sealed chunks' group indexes: rows per key value,
/// and rows per (key, partition) pair when the table is partitioned.
#[derive(Default, Clone)]
struct SealedSummary {
    key_rows: HashMap<ValueKey, u64>,
    key_partition_rows: HashMap<(ValueKey, ValueKey), u64>,
}

impl SealedSummary {
    fn add_chunk(&mut self, index: &GroupIndex) {
        for (key, offsets) in &index.key {
            *self.key_rows.entry(*key).or_insert(0) += offsets.len() as u64;
        }
        if let Some(kp) = &index.key_partition {
            for (key, offsets) in kp {
                *self.key_partition_rows.entry(*key).or_insert(0) += offsets.len() as u64;
            }
        }
    }

    /// Remove the contribution of `index`: either every row (`None`) or only
    /// those at local offsets below `cut` (prefix reclaimed from a boundary
    /// chunk).
    fn remove_chunk(&mut self, index: &GroupIndex, cut: Option<usize>) {
        for (key, offsets) in &index.key {
            let removed = match cut {
                Some(c) => offsets.partition_point(|&o| (o as usize) < c) as u64,
                None => offsets.len() as u64,
            };
            if removed > 0 {
                if let Some(slot) = self.key_rows.get_mut(key) {
                    *slot = slot.saturating_sub(removed);
                }
            }
        }
        if let Some(kp) = &index.key_partition {
            for (key, offsets) in kp {
                let removed = match cut {
                    Some(c) => offsets.partition_point(|&o| (o as usize) < c) as u64,
                    None => offsets.len() as u64,
                };
                if removed > 0 {
                    if let Some(slot) = self.key_partition_rows.get_mut(key) {
                        *slot = slot.saturating_sub(removed);
                    }
                }
            }
        }
    }
}

/// An in-memory, append-only acceleration table.
///
/// Columns are stored in their declared (native) types across contiguous
/// chunks. A per-chunk group index maps each distinct value of the leading
/// cluster-key column to the row offsets that carry it, enabling O(matched)
/// gather for point lookups.
pub struct HotStore {
    schema: Schema,
    key_column_index: usize,
    partition_column_index: Option<usize>,
    chunk_rows: usize,
    state: RwLock<Arc<StoreState>>,
    /// Serializes append/seal/compact. Readers never take this.
    writer: Mutex<()>,
    string_pool: RwLock<StringPool>,
    row_count: AtomicU64,
    memory_bytes: AtomicU64,
    fully_resident: AtomicBool,
    resident_partitions: RwLock<HashSet<String>>,
    memory_soft_limit: u64,
    memory_hard_limit: u64,
}

impl HotStore {
    pub fn new(
        schema: Schema,
        key_column_index: usize,
        partition_column_index: Option<usize>,
        memory_limit_bytes: u64,
    ) -> Self {
        Self::with_chunk_rows(
            schema,
            key_column_index,
            partition_column_index,
            memory_limit_bytes,
            DEFAULT_CHUNK_ROWS,
        )
    }

    fn with_chunk_rows(
        schema: Schema,
        key_column_index: usize,
        partition_column_index: Option<usize>,
        memory_limit_bytes: u64,
        chunk_rows: usize,
    ) -> Self {
        let chunk_rows = chunk_rows.max(1);
        let open = Arc::new(RwLock::new(OpenChunk::new(
            &schema,
            0,
            chunk_rows,
            partition_column_index.is_some(),
        )));
        Self {
            memory_soft_limit: memory_limit_bytes,
            memory_hard_limit: memory_limit_bytes,
            schema,
            key_column_index,
            partition_column_index,
            chunk_rows,
            state: RwLock::new(Arc::new(StoreState {
                front: 0,
                sealed: Vec::new(),
                summary: SealedSummary::default(),
                open,
            })),
            writer: Mutex::new(()),
            string_pool: RwLock::new(StringPool::new()),
            row_count: AtomicU64::new(0),
            memory_bytes: AtomicU64::new(0),
            fully_resident: AtomicBool::new(false),
            resident_partitions: RwLock::new(HashSet::new()),
        }
    }

    pub fn key_column_index(&self) -> usize {
        self.key_column_index
    }

    pub fn partition_column_index(&self) -> Option<usize> {
        self.partition_column_index
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

    pub fn resident_coverage_snapshot(&self) -> ResidentCoverageSnapshot {
        ResidentCoverageSnapshot {
            fully_resident: self.fully_resident.load(Ordering::Acquire),
            resident_partitions: self.resident_partitions.read().unwrap().clone(),
        }
    }

    pub fn restore_resident_coverage(&self, snapshot: ResidentCoverageSnapshot) {
        self.fully_resident
            .store(snapshot.fully_resident, Ordering::Release);
        *self.resident_partitions.write().unwrap() = snapshot.resident_partitions;
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

    /// Number of distinct key values currently indexed. Sealed chunks are
    /// unioned on demand; this is a diagnostic, not a query-path primitive.
    pub fn distinct_key_count(&self) -> usize {
        let st = self.state.read().unwrap();
        let mut distinct: HashSet<ValueKey> = HashSet::new();
        for chunk in &st.sealed {
            distinct.extend(chunk.index.key.keys().copied());
        }
        let open = st.open.read().unwrap();
        distinct.extend(open.index.key.keys().copied());
        distinct.len()
    }

    /// Append a batch. The columns are bulk-copied into the open chunk and the
    /// group index is extended for the new rows; a full open chunk is sealed
    /// into an immutable `Arc` (one copy per chunk, amortized over
    /// `chunk_rows` rows).
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

        // Backpressure valve (design 搂3.1): reject the append before mutating if
        // it would push resident memory past the hard limit. The caller (insert
        // path) reacts by checkpoint-flushing + compacting the persisted prefix,
        // then retrying once.
        let projected = self
            .memory_bytes
            .load(Ordering::Relaxed)
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

        let _writer = self.writer.lock().unwrap();

        // Derive index keys for the new rows (interning strings without
        // cloning). Done once per batch; the per-slice copy below slices into
        // this buffer.
        let (keys, partition_keys) = {
            let mut pool = self.string_pool.write().unwrap();
            let key_col = batch.column(self.key_column_index).ok_or_else(|| {
                TaibaiError::Internal("hot store: missing key column in batch".into())
            })?;
            let keys = collect_keys(key_col, &mut pool)?;
            let partition_keys = match self.partition_column_index {
                Some(partition_idx) => {
                    let partition_col = batch.column(partition_idx).ok_or_else(|| {
                        TaibaiError::Internal(
                            "hot store: missing partition column in batch".into(),
                        )
                    })?;
                    Some(collect_keys(partition_col, &mut pool)?)
                }
                None => None,
            };
            (keys, partition_keys)
        };

        let num_cols = self.schema.num_columns();
        let mut consumed = 0usize;
        while consumed < added {
            let open_arc = self.state.read().unwrap().open.clone();
            let mut open = open_arc.write().unwrap();
            let space = self.chunk_rows - open.rows;
            let take = space.min(added - consumed);
            let start = consumed;
            let end = consumed + take;
            for i in 0..num_cols {
                let src = batch.column(i).ok_or_else(|| {
                    TaibaiError::Internal("hot store: missing column in batch".into())
                })?;
                if start == 0 && end == added {
                    open.columns[i].extend(src)?;
                } else {
                    let sliced = src.slice_fast(start, end);
                    open.columns[i].extend(&sliced)?;
                }
            }
            let local_base = open.rows as u32;
            for (j, key) in keys[start..end].iter().enumerate() {
                open.index
                    .key
                    .entry(*key)
                    .or_default()
                    .push(local_base + j as u32);
            }
            if let (Some(kp), Some(pkeys)) =
                (open.index.key_partition.as_mut(), partition_keys.as_ref())
            {
                for (j, (key, pkey)) in keys[start..end]
                    .iter()
                    .zip(pkeys[start..end].iter())
                    .enumerate()
                {
                    kp.entry((*key, *pkey))
                        .or_default()
                        .push(local_base + j as u32);
                }
            }
            open.rows += take;
            let full = open.rows == self.chunk_rows;
            drop(open);
            consumed = end;
            if full {
                self.seal_open();
            }
        }

        // Publish the new rows only after their data and index are in place.
        self.row_count.fetch_add(added as u64, Ordering::AcqRel);
        self.memory_bytes
            .fetch_add(estimate_batch_memory(batch), Ordering::Relaxed);

        let used = self.memory_bytes.load(Ordering::Relaxed);
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

    /// Seal the current open chunk: freeze it into the sealed list and install
    /// a fresh open chunk. Caller must hold the writer mutex.
    fn seal_open(&self) {
        let st = self.state.read().unwrap().clone();
        let frozen = {
            let open = st.open.read().unwrap();
            if open.rows == 0 {
                return;
            }
            Arc::new(open.freeze_at(open.base))
        };
        let fresh = Arc::new(RwLock::new(OpenChunk::new(
            &self.schema,
            frozen.base + frozen.rows as u64,
            self.chunk_rows,
            self.partition_column_index.is_some(),
        )));
        let mut sealed = Vec::with_capacity(st.sealed.len() + 1);
        sealed.extend(st.sealed.iter().cloned());
        sealed.push(Arc::clone(&frozen));
        let mut summary = st.summary.clone();
        summary.add_chunk(&frozen.index);
        // The write guard must not span the recompute below (same-thread
        // RwLock re-acquisition would self-deadlock).
        *self.state.write().unwrap() = Arc::new(StoreState {
            front: st.front,
            sealed,
            summary,
            open: fresh,
        });
        self.recompute_memory_bytes();
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
        partition_value: Option<&ScalarValue>,
        projection: Option<&[usize]>,
        min_offset: u64,
        reverse: bool,
    ) -> Option<RecordBatch> {
        self.lookup_filtered(
            key_value,
            partition_value,
            projection,
            min_offset,
            reverse,
            None,
            &[],
            &[],
        )
    }

    /// Index lookup with predicate/limit pushdown. Candidate row offsets are
    /// filtered against resident typed columns before projected values are
    /// gathered, so a selective time range does not copy every table column.
    #[allow(clippy::too_many_arguments)]
    pub fn lookup_filtered(
        &self,
        key_value: &ScalarValue,
        partition_value: Option<&ScalarValue>,
        projection: Option<&[usize]>,
        min_offset: u64,
        reverse: bool,
        row_limit: Option<usize>,
        eq_predicates: &[(usize, ScalarValue)],
        range_predicates: &[RangeClause],
    ) -> Option<RecordBatch> {
        let (key, partition_key) = self.resolve_query_keys(key_value, partition_value)?;
        // Hold the snapshot for the whole walk so a query never mixes chunks
        // from before and after a prefix reclamation.
        let st = self.state.read().unwrap();
        let open_guard = st.open.read().unwrap();
        let front = st.front;

        // Visit chunks in append order (ascending ids), or newest-first for
        // reverse lookups, so LIMIT can terminate early.
        let mut visit: Vec<&dyn ChunkView> = Vec::with_capacity(st.sealed.len() + 1);
        if reverse {
            visit.push(&*open_guard);
            visit.extend(st.sealed.iter().rev().map(|c| c.as_ref() as &dyn ChunkView));
        } else {
            visit.extend(st.sealed.iter().map(|c| c.as_ref() as &dyn ChunkView));
            visit.push(&*open_guard);
        }

        let proj: Vec<usize> = match projection {
            Some(p) => p.to_vec(),
            None => (0..self.schema.num_columns()).collect(),
        };
        let target = row_limit.unwrap_or(usize::MAX);
        let mut gathered: Option<(Vec<ColumnDef>, Vec<ColumnArray>)> = None;
        let mut found: usize = 0;
        let mut seen = false;
        let mut skipped_by_watermark = false;
        let mut rows_buf: Vec<u32> = Vec::new();

        for chunk in visit {
            let base = chunk.base() - front;
            let chunk_end = base + chunk.num_rows();
            if chunk_end <= min_offset {
                // Fully below the watermark. Coverage is an append-order
                // prefix, so a newest-first (reverse) walk can stop here.
                skipped_by_watermark = true;
                if reverse {
                    break;
                }
                continue;
            }
            if found >= target {
                break;
            }
            let postings = match partition_key {
                Some(pk) => chunk.key_partition_postings(&key, &pk),
                None => chunk.key_postings(&key),
            };
            let Some(postings) = postings else {
                continue;
            };
            seen = true;
            let cut = if min_offset > base {
                ((min_offset - base).min(u32::MAX as u64)) as u32
            } else {
                0
            };
            let start = postings.partition_point(|&o| o < cut);
            let slice = &postings[start..];
            if slice.is_empty() {
                continue;
            }
            let covered_partition = partition_key.and(self.partition_column_index);
            let preds = compile_predicates(
                chunk.columns(),
                eq_predicates,
                range_predicates,
                self.key_column_index,
                covered_partition,
            );
            rows_buf.clear();
            let remaining = target - found;
            if reverse {
                for &row in slice.iter().rev() {
                    if preds.iter().all(|p| p.matches(row as usize)) {
                        rows_buf.push(row);
                        if rows_buf.len() >= remaining {
                            break;
                        }
                    }
                }
            } else {
                for &row in slice {
                    if preds.iter().all(|p| p.matches(row as usize)) {
                        rows_buf.push(row);
                        if rows_buf.len() >= remaining {
                            break;
                        }
                    }
                }
            }
            if rows_buf.is_empty() {
                continue;
            }
            found += rows_buf.len();
            let columns: Vec<ColumnArray> = proj
                .iter()
                .map(|&ci| chunk.columns()[ci].take(&rows_buf))
                .collect();
            match &mut gathered {
                Some((_, acc)) => {
                    for (dst, src) in acc.iter_mut().zip(columns) {
                        dst.extend(&src).ok();
                    }
                }
                slot @ None => {
                    let defs = proj
                        .iter()
                        .map(|&i| self.schema.columns[i].clone())
                        .collect();
                    *slot = Some((defs, columns));
                }
            }
        }
        drop(open_guard);

        if !seen {
            if skipped_by_watermark {
                // The key was never probed above the watermark. Keep the
                // None-vs-empty contract: a key that exists only under the
                // watermark yields Some(empty), never None.
                if !self.key_exists_anywhere(&st, &key, partition_key.as_ref()) {
                    return None;
                }
            } else {
                return None;
            }
        }
        let Some((defs, columns)) = gathered else {
            let col_defs: Vec<ColumnDef> = proj
                .iter()
                .map(|&i| self.schema.columns[i].clone())
                .collect();
            return Some(RecordBatch::new(Schema::new(col_defs)));
        };
        RecordBatch::try_new(Schema::new(defs), columns).ok()
    }

    /// Existence probe used to distinguish "never seen" (`None`) from "seen
    /// but everything filtered" (`Some(empty)`) when the watermark skipped
    /// chunks without probing their postings.
    fn key_exists_anywhere(
        &self,
        st: &StoreState,
        key: &ValueKey,
        partition_key: Option<&ValueKey>,
    ) -> bool {
        let sealed_hit = st.sealed.iter().any(|c| match partition_key {
            Some(pk) => c.key_partition_postings(key, pk).is_some(),
            None => c.key_postings(key).is_some(),
        });
        if sealed_hit {
            return true;
        }
        let open = st.open.read().unwrap();
        match partition_key {
            Some(pk) => open.key_partition_postings(key, pk).is_some(),
            None => open.key_postings(key).is_some(),
        }
    }

    /// Count rows whose index-column value equals `key_value` without
    /// materialising the batch.
    pub fn count_by_key_partition_from(
        &self,
        key_value: &ScalarValue,
        partition_value: Option<&ScalarValue>,
        min_offset: u64,
    ) -> Option<u64> {
        let (key, partition_key) = self.resolve_query_keys(key_value, partition_value)?;
        let st = self.state.read().unwrap();
        let open = st.open.read().unwrap();
        let front = st.front;

        if min_offset == 0 {
            // Resident fast path: every sealed row is visible, so the
            // seal-time summary answers for all chunks in one probe.
            let sealed_total = match partition_key {
                None => st.summary.key_rows.get(&key).copied().unwrap_or(0),
                Some(pk) => st
                    .summary
                    .key_partition_rows
                    .get(&(key, pk))
                    .copied()
                    .unwrap_or(0),
            };
            let mut total = sealed_total;
            let mut seen = sealed_total > 0;
            let open_postings = match partition_key {
                Some(pk) => open.key_partition_postings(&key, &pk),
                None => open.key_postings(&key),
            };
            if let Some(offsets) = open_postings {
                seen = true;
                total += offsets.len() as u64;
            }
            return seen.then_some(total);
        }

        let mut seen = false;
        let mut skipped = false;
        let mut total = 0u64;
        for chunk in sealed_then_open(&st, &open) {
            let base = chunk.base() - front;
            if base + chunk.num_rows() <= min_offset {
                skipped = true;
                continue;
            }
            let postings = match partition_key {
                Some(pk) => chunk.key_partition_postings(&key, &pk),
                None => chunk.key_postings(&key),
            };
            let Some(postings) = postings else {
                continue;
            };
            seen = true;
            let cut = if min_offset > base {
                ((min_offset - base).min(u32::MAX as u64)) as u32
            } else {
                0
            };
            total += (postings.len() - postings.partition_point(|&o| o < cut)) as u64;
        }
        if !seen && skipped && !self.key_exists_anywhere(&st, &key, partition_key.as_ref()) {
            return None;
        }
        (seen || skipped).then_some(total)
    }

    /// Count matching index candidates without gathering a RecordBatch.
    pub fn count_filtered(
        &self,
        key_value: &ScalarValue,
        partition_value: Option<&ScalarValue>,
        min_offset: u64,
        eq_predicates: &[(usize, ScalarValue)],
        range_predicates: &[RangeClause],
    ) -> Option<u64> {
        let (key, partition_key) = self.resolve_query_keys(key_value, partition_value)?;
        let st = self.state.read().unwrap();
        let open = st.open.read().unwrap();
        let front = st.front;
        let mut seen = false;
        let mut skipped = false;
        let mut total = 0u64;
        for chunk in sealed_then_open(&st, &open) {
            let base = chunk.base() - front;
            if base + chunk.num_rows() <= min_offset {
                skipped = true;
                continue;
            }
            let postings = match partition_key {
                Some(pk) => chunk.key_partition_postings(&key, &pk),
                None => chunk.key_postings(&key),
            };
            let Some(postings) = postings else {
                continue;
            };
            seen = true;
            let cut = if min_offset > base {
                ((min_offset - base).min(u32::MAX as u64)) as u32
            } else {
                0
            };
            let start = postings.partition_point(|&o| o < cut);
            if start >= postings.len() {
                continue;
            }
            let covered_partition = partition_key.and(self.partition_column_index);
            let preds = compile_predicates(
                chunk.columns(),
                eq_predicates,
                range_predicates,
                self.key_column_index,
                covered_partition,
            );
            total += postings[start..]
                .iter()
                .filter(|&&row| preds.iter().all(|p| p.matches(row as usize)))
                .count() as u64;
        }
        if !seen && skipped && !self.key_exists_anywhere(&st, &key, partition_key.as_ref()) {
            return None;
        }
        (seen || skipped).then_some(total)
    }

    /// Resolve the query key (and optional partition key) through the intern
    /// pool. `None` means the value has never been appended.
    fn resolve_query_keys(
        &self,
        key_value: &ScalarValue,
        partition_value: Option<&ScalarValue>,
    ) -> Option<(ValueKey, Option<ValueKey>)> {
        let pool = self.string_pool.read().unwrap();
        let key = scalar_to_key(key_value, &pool).ok()??;
        let partition_key = match partition_value {
            Some(value) => scalar_to_key(value, &pool).ok()?,
            None => None,
        };
        Some((key, partition_key))
    }

    /// Rough resident-memory estimate in bytes (maintained incrementally and
    /// recomputed at seal/compact boundaries).
    pub fn estimated_memory_bytes(&self) -> u64 {
        self.memory_bytes.load(Ordering::Relaxed)
    }

    pub fn soft_limit_exceeded(&self) -> bool {
        self.memory_bytes.load(Ordering::Relaxed) > self.memory_soft_limit
    }

    fn recompute_memory_bytes(&self) {
        let st = self.state.read().unwrap().clone();
        let mut total: u64 = st.sealed.iter().map(|c| c.memory_bytes).sum();
        total += st.open.read().unwrap().estimated_memory_bytes();
        total += (self.string_pool.read().unwrap().len() as u64) * 24;
        self.memory_bytes.store(total, Ordering::Relaxed);
    }

    /// Drop the leading `requested` rows (the append-prefix already persisted to
    /// segments) to reclaim resident memory under backpressure. Returns the
    /// number of rows actually dropped.
    ///
    /// Whole chunks below the cut are released as-is (their frozen indexes go
    /// with them); only the chunk straddling the cut is trimmed and re-indexed.
    /// The `front` cursor advances instead of renumbering survivors, so the
    /// relative offsets the scan layer uses as watermarks stay exact. This is
    /// the "release the oldest archived interval" safety valve from design
    /// 搂3.1.
    pub fn compact_prefix(&self, requested: u64) -> u64 {
        if requested == 0 {
            return 0;
        }
        let _writer = self.writer.lock().unwrap();
        let total = self.row_count();
        if total == 0 {
            return 0;
        }
        let n = requested.min(total);
        let st = self.state.read().unwrap().clone();
        let cut = st.front + n;
        let mut summary = st.summary.clone();

        let mut sealed: Vec<Arc<Chunk>> = Vec::with_capacity(st.sealed.len());
        for chunk in &st.sealed {
            let end = chunk.base + chunk.rows as u64;
            if end <= cut {
                // Entirely below the cut: the frozen chunk (columns + index)
                // is released as one allocation group.
                summary.remove_chunk(&chunk.index, None);
                continue;
            }
            if chunk.base >= cut {
                sealed.push(Arc::clone(chunk));
            } else {
                // Boundary chunk: keep [cut, end) as a fresh frozen chunk.
                let local_cut = (cut - chunk.base) as usize;
                let rows = chunk.rows as usize;
                summary.remove_chunk(&chunk.index, Some(local_cut));
                let columns: Vec<ColumnArray> = chunk
                    .columns
                    .iter()
                    .map(|c| c.slice_fast(local_cut, rows))
                    .collect();
                let index = self.rebuild_index(&columns);
                let memory_bytes = columns
                    .iter()
                    .map(|c| c.estimated_size_bytes() as u64)
                    .sum::<u64>()
                    + index.estimate_bytes();
                sealed.push(Arc::new(Chunk {
                    base: cut,
                    rows: (rows - local_cut) as u32,
                    columns,
                    index,
                    memory_bytes,
                }));
            }
        }

        // Open chunk: shared when untouched, trimmed when the cut reaches into
        // it, replaced when fully reclaimed.
        let open_arc = {
            let open = st.open.read().unwrap();
            let end = open.base + open.rows as u64;
            if open.rows == 0 || open.base >= cut {
                Arc::clone(&st.open)
            } else if end <= cut {
                Arc::new(RwLock::new(OpenChunk::new(
                    &self.schema,
                    end,
                    self.chunk_rows,
                    self.partition_column_index.is_some(),
                )))
            } else {
                let local_cut = (cut - open.base) as usize;
                let columns: Vec<ColumnArray> = open
                    .columns
                    .iter()
                    .map(|c| c.slice_fast(local_cut, open.rows))
                    .collect();
                let index = self.rebuild_index(&columns);
                Arc::new(RwLock::new(OpenChunk {
                    base: cut,
                    rows: open.rows - local_cut,
                    columns,
                    index,
                }))
            }
        };

        *self.state.write().unwrap() = Arc::new(StoreState {
            front: cut,
            sealed,
            summary,
            open: open_arc,
        });

        self.row_count.fetch_sub(n, Ordering::AcqRel);
        self.recompute_memory_bytes();
        n
    }

    /// Rebuild the group index of a trimmed chunk directly from its columns
    /// (string values are already interned, so this is a lookup-only pass).
    fn rebuild_index(&self, columns: &[ColumnArray]) -> GroupIndex {
        let mut pool = self.string_pool.write().unwrap();
        let keys = match columns.get(self.key_column_index) {
            Some(col) => collect_keys(col, &mut pool).unwrap_or_default(),
            None => Vec::new(),
        };
        let partition_keys = match self.partition_column_index.and_then(|i| columns.get(i)) {
            Some(col) => collect_keys(col, &mut pool).unwrap_or_default(),
            None => Vec::new(),
        };
        let mut index = GroupIndex::empty(self.partition_column_index.is_some());
        for (row, key) in keys.iter().enumerate() {
            index.key.entry(*key).or_default().push(row as u32);
        }
        if let (Some(kp), Some(pkeys)) = (
            index.key_partition.as_mut(),
            (!partition_keys.is_empty()).then_some(&partition_keys),
        ) {
            for (row, (key, pkey)) in keys.iter().zip(pkeys.iter()).enumerate() {
                kp.entry((*key, *pkey)).or_default().push(row as u32);
            }
        }
        index
    }
}

/// Iterate the sealed chunks in append order followed by the open chunk.
fn sealed_then_open<'a>(
    st: &'a StoreState,
    open: &'a OpenChunk,
) -> impl Iterator<Item = &'a dyn ChunkView> {
    st.sealed
        .iter()
        .map(|c| c.as_ref() as &dyn ChunkView)
        .chain(std::iter::once(open as &dyn ChunkView))
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

/// Registry of resident hot stores, keyed by `(db, table)`.
pub struct HotStoreManager {
    stores: RwLock<HashMap<(String, String), Arc<HotStore>>>,
    invalidated: RwLock<HashSet<(String, String)>>,
    memory_limit_bytes: u64,
}

impl HotStoreManager {
    pub fn new() -> Self {
        Self::with_memory_limit_bytes(8 * 1024 * 1024 * 1024)
    }

    pub fn with_memory_limit_bytes(memory_limit_bytes: u64) -> Self {
        Self {
            stores: RwLock::new(HashMap::new()),
            invalidated: RwLock::new(HashSet::new()),
            memory_limit_bytes,
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
        let table_key = (db.to_string(), table.to_string());
        if self.invalidated.read().unwrap().contains(&table_key) {
            return None;
        }
        let key_idx = resolve_key_column(table_meta)?;
        {
            let stores = self.stores.read().unwrap();
            if let Some(existing) = stores.get(&table_key) {
                return Some(Arc::clone(existing));
            }
        }
        let mut stores = self.stores.write().unwrap();
        if let Some(existing) = stores.get(&table_key) {
            return Some(Arc::clone(existing));
        }
        let schema = Schema::new(table_meta.columns.clone());
        let partition_idx = table_meta.get_partition_column_index_cached();
        let store = Arc::new(HotStore::new(
            schema,
            key_idx,
            partition_idx,
            self.memory_limit_bytes,
        ));
        stores.insert(table_key, Arc::clone(&store));
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

    /// Reclaim the persisted prefix of one table's resident store (backpressure).
    pub fn compact_prefix(&self, db: &str, table: &str, n: u64) -> u64 {
        let stores = self.stores.read().unwrap();
        stores
            .get(&(db.to_string(), table.to_string()))
            .map(|s| s.compact_prefix(n))
            .unwrap_or(0)
    }

    pub fn soft_limit_exceeded_for(&self, db: &str, table: &str) -> bool {
        let stores = self.stores.read().unwrap();
        stores
            .get(&(db.to_string(), table.to_string()))
            .map(|s| s.soft_limit_exceeded())
            .unwrap_or(false)
    }

    pub fn remove(&self, db: &str, table: &str) {
        let table_key = (db.to_string(), table.to_string());
        let mut stores = self.stores.write().unwrap();
        stores.remove(&table_key);
        self.invalidated.write().unwrap().remove(&table_key);
    }

    /// Drop an append-only hot store after an in-place mutation and keep it
    /// disabled until restart. Recreating it from only subsequent INSERTs
    /// would hide older live memtable rows behind an incomplete fast path.
    pub fn invalidate(&self, db: &str, table: &str) {
        let table_key = (db.to_string(), table.to_string());
        self.stores.write().unwrap().remove(&table_key);
        self.invalidated.write().unwrap().insert(table_key);
    }

    pub fn clear_resident_coverage(&self, db: &str, table: &str) {
        if let Some(store) = self.get(db, table) {
            store.clear_resident_coverage();
        }
    }

    pub fn resident_coverage_snapshot(
        &self,
        db: &str,
        table: &str,
    ) -> Option<ResidentCoverageSnapshot> {
        self.get(db, table)
            .map(|store| store.resident_coverage_snapshot())
    }

    pub fn restore_resident_coverage(
        &self,
        db: &str,
        table: &str,
        snapshot: ResidentCoverageSnapshot,
    ) {
        if let Some(store) = self.get(db, table) {
            store.restore_resident_coverage(snapshot);
        }
    }
}

impl Default for HotStoreManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[path = "hot_store_test.rs"]
mod tests;
