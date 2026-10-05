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

// ---------------------------------------------------------------------------
// Hashing.
//
// The append path hashes the key value of every row twice (string intern +
// group-index update) and the read path probes once per query. SipHash costs
// more than the surrounding work for these short keys, so every hot map uses
// the multiply-xor hasher below (FxHash family).
// ---------------------------------------------------------------------------

use crate::common::fast_hash::{FastBuild, FastHasher};

/// Watermark cut for one chunk: how many of its appended rows fall below the
/// scan's `min_offset` watermark and are therefore no longer resident.
#[inline]
fn watermark_cut(min_offset: u64, base: u64) -> u32 {
    if min_offset > base {
        (min_offset - base).min(u32::MAX as u64) as u32
    } else {
        0
    }
}

/// The postings at or above the watermark cut (rows still held in memory).
#[inline]
fn resident_postings(postings: &[u32], cut: u32) -> &[u32] {
    &postings[postings.partition_point(|&o| o < cut)..]
}

#[derive(Debug, Clone, Default)]
pub struct ResidentCoverageSnapshot {
    fully_resident: bool,
    resident_partitions: HashSet<String>,
}

/// Direct-mapped cache slot for the intern pool: `(hash, id)` with `hash == 0`
/// meaning empty. Hashes are forced nonzero, so a slot match is either the
/// interned value or a (verified) 64-bit collision.
#[derive(Clone, Copy)]
struct PoolCacheSlot {
    hash: u64,
    id: u32,
    _pad: u32,
}

const POOL_CACHE_SLOTS: usize = 4096;

/// Interning pool for variable-length key values. A distinct string is stored
/// once and addressed by a `u32` id, so that building the inverted index never
/// re-clones a string for every row (only on first sight of a new value).
///
/// This mirrors q's enumerated `` `sym$ `` symbols: the index compares interned
/// ids, never string bytes. A direct-mapped hash cache sits in front of the
/// map so repeated symbols within a batch skip the full lookup entirely.
///
/// Released ids are tombstoned (`reverse[i] = None`) rather than removed:
/// ids are positions in `reverse`, and postings in surviving chunk indexes
/// embed them. Tombstoned ids are gone from `forward`, so a later intern of
/// the same string allocates a fresh id — safe because no live postings
/// reference a released one.
pub struct StringPool {
    forward: HashMap<String, u32, FastBuild>,
    reverse: Vec<Option<String>>,
    cache: Vec<PoolCacheSlot>,
}

impl StringPool {
    pub fn new() -> Self {
        Self {
            forward: HashMap::with_hasher(FastBuild::default()),
            reverse: Vec::new(),
            cache: vec![
                PoolCacheSlot {
                    hash: 0,
                    id: 0,
                    _pad: 0
                };
                POOL_CACHE_SLOTS
            ],
        }
    }

    #[inline]
    fn cached(&self, s: &str) -> Option<(usize, PoolCacheSlot)> {
        let hash = FastHasher::hash_str(s);
        let idx = (hash as usize) & (POOL_CACHE_SLOTS - 1);
        let slot = self.cache[idx];
        if slot.hash == hash
            && self
                .reverse
                .get(slot.id as usize)
                .and_then(|entry| entry.as_deref())
                == Some(s)
        {
            Some((idx, slot))
        } else {
            None
        }
    }

    /// Insert-or-return for a string value. Borrows `s` (no allocation on a
    /// cache hit).
    pub fn intern(&mut self, s: &str) -> u32 {
        if let Some((_idx, slot)) = self.cached(s) {
            return slot.id;
        }
        let id = match self.forward.get(s) {
            Some(&id) => id,
            None => {
                let id = self.reverse.len() as u32;
                self.reverse.push(Some(s.to_string()));
                self.forward.insert(s.to_string(), id);
                id
            }
        };
        let hash = FastHasher::hash_str(s);
        let idx = (hash as usize) & (POOL_CACHE_SLOTS - 1);
        self.cache[idx] = PoolCacheSlot { hash, id, _pad: 0 };
        id
    }

    /// Immutable lookup used on the read/query path. `None` means the value has
    /// never been appended, hence there can be no matching rows.
    pub fn lookup(&self, s: &str) -> Option<u32> {
        match self.cached(s) {
            Some((_idx, slot)) => Some(slot.id),
            None => self.forward.get(s).copied(),
        }
    }

    /// Drop every id outside `live`: removes its string from `forward` and
    /// tombstones its `reverse` slot. Called after a prefix compaction so the
    /// pool tracks the rows actually resident instead of growing without
    /// bound on high-cardinality string keys. The direct-mapped cache is
    /// cleared wholesale — its (hash, id) slots may name released ids.
    pub fn retain_ids(&mut self, live: &HashSet<u32>) {
        for (id, slot) in self.reverse.iter_mut().enumerate() {
            if slot.is_some() && !live.contains(&(id as u32)) {
                if let Some(s) = slot.take() {
                    self.forward.remove(s.as_str());
                }
            }
        }
        for slot in self.cache.iter_mut() {
            slot.hash = 0;
            slot.id = 0;
        }
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
    Float(u64), // canonical bits via `float_key_bits` (-0.0/NaN folded)
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
        Float32(Some(v)) => ValueKey::Float(crate::types::scalar::float_key_bits(*v as f64)),
        Float64(Some(v)) => ValueKey::Float(crate::types::scalar::float_key_bits(*v)),
        Decimal(Some(v)) => ValueKey::Int(*v),
        Date(Some(v)) => ValueKey::Int(*v as i64),
        Time(Some(v)) | Timestamp(Some(v)) | Interval(Some(v)) => ValueKey::Int(*v),
        // Any remaining variant carries `None` -> null key (matches NULL rows).
        _ => ValueKey::Null,
    };
    Ok(Some(key))
}

/// Row accessor that yields the inverted-index `ValueKey` for a column without
/// materializing a `Vec<ValueKey>`: the append path groups keys straight from
/// the column range in one pass. Variable-width values dedup through a
/// batch-local map, so the pool interns each distinct string once per slice
/// (the pool's own 1024-slot direct-mapped cache thrashes beyond ~1000
/// distinct symbols).
enum KeySource<'a> {
    Bool {
        data: &'a [u8],
        bm: &'a NullBitmap,
    },
    Int8 {
        data: &'a [i8],
        bm: &'a NullBitmap,
    },
    Int16 {
        data: &'a [i16],
        bm: &'a NullBitmap,
    },
    Int32 {
        data: &'a [i32],
        bm: &'a NullBitmap,
    },
    Int64 {
        data: &'a [i64],
        bm: &'a NullBitmap,
    },
    UInt8 {
        data: &'a [u8],
        bm: &'a NullBitmap,
    },
    UInt16 {
        data: &'a [u16],
        bm: &'a NullBitmap,
    },
    UInt32 {
        data: &'a [u32],
        bm: &'a NullBitmap,
    },
    UInt64 {
        data: &'a [u64],
        bm: &'a NullBitmap,
    },
    Float32 {
        data: &'a [f32],
        bm: &'a NullBitmap,
    },
    Float64 {
        data: &'a [f64],
        bm: &'a NullBitmap,
    },
    Decimal {
        data: &'a [i64],
        bm: &'a NullBitmap,
    },
    Date {
        data: &'a [i32],
        bm: &'a NullBitmap,
    },
    Time {
        data: &'a [i64],
        bm: &'a NullBitmap,
    },
    Timestamp {
        data: &'a [i64],
        bm: &'a NullBitmap,
    },
    Interval {
        data: &'a [i64],
        bm: &'a NullBitmap,
    },
    VarChar {
        data: &'a Utf8Buffer,
        bm: &'a NullBitmap,
        dedup: HashMap<&'a str, ValueKey, FastBuild>,
    },
}

/// Materialize view-like key columns (dictionary/RLE) once so grouping reads
/// owned arrays.
fn materialized_key_column<'a>(
    owned: &'a mut Option<ColumnArray>,
    col: &'a ColumnArray,
) -> &'a ColumnArray {
    match col {
        ColumnArray::Dictionary { .. } => {
            *owned = Some(col.materialize_varchar());
            owned.as_ref().expect("just inserted")
        }
        ColumnArray::Rle { .. } | ColumnArray::Selected { .. } => {
            *owned = Some(col.flattened().clone());
            owned.as_ref().expect("just inserted")
        }
        other => other,
    }
}

impl<'a> KeySource<'a> {
    fn new(col: &'a ColumnArray) -> Result<Self> {
        Ok(match col {
            ColumnArray::Bool { data, bitmap } => Self::Bool { data, bm: bitmap },
            ColumnArray::Int8 { data, bitmap } => Self::Int8 { data, bm: bitmap },
            ColumnArray::Int16 { data, bitmap } => Self::Int16 { data, bm: bitmap },
            ColumnArray::Int32 { data, bitmap } => Self::Int32 { data, bm: bitmap },
            ColumnArray::Int64 { data, bitmap } => Self::Int64 { data, bm: bitmap },
            ColumnArray::UInt8 { data, bitmap } => Self::UInt8 { data, bm: bitmap },
            ColumnArray::UInt16 { data, bitmap } => Self::UInt16 { data, bm: bitmap },
            ColumnArray::UInt32 { data, bitmap } => Self::UInt32 { data, bm: bitmap },
            ColumnArray::UInt64 { data, bitmap } => Self::UInt64 { data, bm: bitmap },
            ColumnArray::Float32 { data, bitmap } => Self::Float32 { data, bm: bitmap },
            ColumnArray::Float64 { data, bitmap } => Self::Float64 { data, bm: bitmap },
            ColumnArray::Decimal { data, bitmap } => Self::Decimal { data, bm: bitmap },
            ColumnArray::Date { data, bitmap } => Self::Date { data, bm: bitmap },
            ColumnArray::Time { data, bitmap } => Self::Time { data, bm: bitmap },
            ColumnArray::Timestamp { data, bitmap } => Self::Timestamp { data, bm: bitmap },
            ColumnArray::Interval { data, bitmap } => Self::Interval { data, bm: bitmap },
            ColumnArray::VarChar { data, bitmap } => Self::VarChar {
                data,
                bm: bitmap,
                dedup: HashMap::with_capacity_and_hasher(256, FastBuild::default()),
            },
            other => {
                return Err(TaibaiError::Internal(format!(
                    "hot store: column type {} is not supported as a cluster-key column",
                    other.data_type().display_name()
                )));
            }
        })
    }

    #[inline]
    fn key_at(&mut self, i: usize, pool: &mut StringPool) -> ValueKey {
        match self {
            Self::Bool { data, bm } => key_or_null(bm, i, || ValueKey::Bool(data[i] != 0)),
            Self::Int8 { data, bm } => key_or_null(bm, i, || ValueKey::Int(data[i] as i64)),
            Self::Int16 { data, bm } => key_or_null(bm, i, || ValueKey::Int(data[i] as i64)),
            Self::Int32 { data, bm } => key_or_null(bm, i, || ValueKey::Int(data[i] as i64)),
            Self::Int64 { data, bm } => key_or_null(bm, i, || ValueKey::Int(data[i])),
            Self::UInt8 { data, bm } => key_or_null(bm, i, || ValueKey::Int(data[i] as i64)),
            Self::UInt16 { data, bm } => key_or_null(bm, i, || ValueKey::Int(data[i] as i64)),
            Self::UInt32 { data, bm } => key_or_null(bm, i, || ValueKey::Int(data[i] as i64)),
            Self::UInt64 { data, bm } => key_or_null(bm, i, || ValueKey::UInt(data[i])),
            Self::Float32 { data, bm } => key_or_null(bm, i, || {
                ValueKey::Float(crate::types::scalar::float_key_bits(data[i] as f64))
            }),
            Self::Float64 { data, bm } => key_or_null(bm, i, || {
                ValueKey::Float(crate::types::scalar::float_key_bits(data[i]))
            }),
            Self::Decimal { data, bm } => key_or_null(bm, i, || ValueKey::Int(data[i])),
            Self::Date { data, bm } => key_or_null(bm, i, || ValueKey::Int(data[i] as i64)),
            Self::Time { data, bm } => key_or_null(bm, i, || ValueKey::Int(data[i])),
            Self::Timestamp { data, bm } => key_or_null(bm, i, || ValueKey::Int(data[i])),
            Self::Interval { data, bm } => key_or_null(bm, i, || ValueKey::Int(data[i])),
            Self::VarChar { data, bm, dedup } => {
                if bm.is_null(i) {
                    return ValueKey::Null;
                }
                let s: &'a str = &data[i];
                if let Some(key) = dedup.get(s) {
                    return *key;
                }
                let key = ValueKey::Str(pool.intern(s));
                dedup.insert(s, key);
                key
            }
        }
    }
}

/// Shared null-check for the fixed-width `key_at` arms: every one of them
/// reads `ValueKey::Null` for a null row and a type-specific key otherwise.
#[inline]
fn key_or_null(bm: &NullBitmap, i: usize, value: impl FnOnce() -> ValueKey) -> ValueKey {
    if bm.is_null(i) {
        ValueKey::Null
    } else {
        value()
    }
}

// ---------------------------------------------------------------------------
// Batch grouping.
// ---------------------------------------------------------------------------

/// Intern-id sentinel marking a NULL key row in the dense fast path.
const NULL_RUN_ID: u32 = u32::MAX;

/// Groups one batch's rows into a chunk's [`GroupIndex`], slice by slice
/// (chunk boundaries split a batch). The strategy is picked once per batch
/// from the key/partition column shapes, and all scratch buffers live here
/// so they are reused across slices instead of reallocated per chunk.
enum BatchGrouper<'a> {
    /// VarChar key column without partition grouping: intern ids make per-row
    /// work one pool probe plus a flat pair push, and one sort lays the slice
    /// out as per-key runs — the chunk index sees one entry + one contiguous
    /// extend per distinct symbol, with no per-row hash-table probe.
    DenseIds {
        data: &'a Utf8Buffer,
        bm: &'a NullBitmap,
        pending: Vec<(u32, u32)>,
    },
    /// Every other shape: per-row key extraction into batch-local scratch
    /// maps, then one contiguous extend per distinct key. The per-key vectors
    /// are cleared (buffers kept) between slices — group shapes are stable
    /// across batches of a table, so the capacity survives.
    Keys {
        key: KeySource<'a>,
        partition: Option<KeySource<'a>>,
        key_scratch: HashMap<ValueKey, Vec<u32>, FastBuild>,
        pkey_scratch: HashMap<(ValueKey, ValueKey), Vec<u32>, FastBuild>,
    },
}

impl<'a> BatchGrouper<'a> {
    /// Validate that the key (and partition) columns are groupable and pick
    /// the strategy for this batch. `rows` reserves scratch up front.
    fn new(
        key_col: &'a ColumnArray,
        partition_col: Option<&'a ColumnArray>,
        rows: usize,
    ) -> Result<Self> {
        if partition_col.is_none() {
            if let ColumnArray::VarChar { data, bitmap } = key_col {
                return Ok(Self::DenseIds {
                    data,
                    bm: bitmap,
                    pending: Vec::with_capacity(rows),
                });
            }
        }
        Ok(Self::Keys {
            key: KeySource::new(key_col)?,
            partition: match partition_col {
                Some(col) => Some(KeySource::new(col)?),
                None => None,
            },
            key_scratch: HashMap::with_capacity_and_hasher(256, FastBuild::default()),
            pkey_scratch: HashMap::with_capacity_and_hasher(256, FastBuild::default()),
        })
    }

    /// Group rows `[start, end)` of the batch into `index` at local offsets
    /// `base + (i - start)`. Offsets within one key's postings stay ascending
    /// by construction.
    fn group(
        &mut self,
        pool: &mut StringPool,
        index: &mut GroupIndex,
        start: usize,
        end: usize,
        base: u32,
    ) {
        match self {
            Self::DenseIds { data, bm, pending } => {
                pending.clear();
                for i in start..end {
                    let offset = base + (i - start) as u32;
                    if bm.is_null(i) {
                        pending.push((NULL_RUN_ID, offset));
                    } else {
                        pending.push((pool.intern(&data[i]), offset));
                    }
                }
                // Sort by (id, offset): per-key runs become contiguous with
                // ascending offsets, so each distinct key extends its
                // postings exactly once. Symbol-grouped ingestion is nearly
                // pre-sorted, which the unstable sort detects.
                pending.sort_unstable();
                let mut i = 0usize;
                while i < pending.len() {
                    let id = pending[i].0;
                    let mut j = i + 1;
                    while j < pending.len() && pending[j].0 == id {
                        j += 1;
                    }
                    let key = if id == NULL_RUN_ID {
                        ValueKey::Null
                    } else {
                        ValueKey::Str(id)
                    };
                    index
                        .key
                        .entry(key)
                        .or_default()
                        .extend(pending[i..j].iter().map(|p| p.1));
                    i = j;
                }
            }
            Self::Keys {
                key,
                partition,
                key_scratch,
                pkey_scratch,
            } => {
                // Clear the per-key vectors but keep their buffers: reusing
                // them avoids ~one Vec allocation per distinct key per batch.
                if key_scratch.len() > 8192 {
                    key_scratch.clear();
                } else {
                    for offsets in key_scratch.values_mut() {
                        offsets.clear();
                    }
                }
                match partition.as_mut() {
                    Some(psrc) => {
                        if pkey_scratch.len() > 8192 {
                            pkey_scratch.clear();
                        } else {
                            for offsets in pkey_scratch.values_mut() {
                                offsets.clear();
                            }
                        }
                        for i in start..end {
                            let k = key.key_at(i, pool);
                            let pk = psrc.key_at(i, pool);
                            let offset = base + (i - start) as u32;
                            key_scratch.entry(k).or_default().push(offset);
                            pkey_scratch.entry((k, pk)).or_default().push(offset);
                        }
                    }
                    None => {
                        for i in start..end {
                            let k = key.key_at(i, pool);
                            key_scratch
                                .entry(k)
                                .or_default()
                                .push(base + (i - start) as u32);
                        }
                    }
                }
                for (key, offsets) in key_scratch.iter() {
                    index
                        .key
                        .entry(*key)
                        .or_default()
                        .extend_from_slice(offsets);
                }
                if let Some(kp) = index.key_partition.as_mut() {
                    for ((key, pkey), offsets) in pkey_scratch.iter() {
                        kp.entry((*key, *pkey))
                            .or_default()
                            .extend_from_slice(offsets);
                    }
                }
            }
        }
    }
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
                    pred: FastPred::$field { data, v: v.clone() },
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
    columns: &'c [std::sync::Arc<ColumnArray>],
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
        preds.push(compile_one(
            columns.get(*column).map(std::sync::Arc::as_ref),
            value,
            CmpOp::Eq,
        ));
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
            columns.get(clause.column_index).map(std::sync::Arc::as_ref),
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
    key: HashMap<ValueKey, Vec<u32>, FastBuild>,
    key_partition: Option<HashMap<(ValueKey, ValueKey), Vec<u32>, FastBuild>>,
}

impl GroupIndex {
    fn empty(partitioned: bool) -> Self {
        Self {
            key: HashMap::with_hasher(FastBuild::default()),
            key_partition: partitioned.then(|| HashMap::with_hasher(FastBuild::default())),
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
    columns: Vec<std::sync::Arc<ColumnArray>>,
    index: GroupIndex,
    memory_bytes: u64,
}

/// Read-only view shared by sealed chunks and the open chunk, so the lookup
/// walk can treat both uniformly. `Send + Sync` lets the parallel gather walk
/// chunks on the rayon pool.
trait ChunkView: Sync {
    fn base(&self) -> u64;
    fn num_rows(&self) -> u64;
    fn columns(&self) -> &[std::sync::Arc<ColumnArray>];
    fn key_postings(&self, key: &ValueKey) -> Option<&Vec<u32>>;
    fn key_partition_postings(&self, key: &ValueKey, partition: &ValueKey) -> Option<&Vec<u32>>;
}

/// Both chunk kinds carry the same field names, so one macro serves the two
/// views; `rows` differs (u32 vs usize) and widens through `as u64`.
macro_rules! impl_chunk_view {
    ($t:ty) => {
        impl ChunkView for $t {
            fn base(&self) -> u64 {
                self.base
            }
            fn num_rows(&self) -> u64 {
                self.rows as u64
            }
            fn columns(&self) -> &[std::sync::Arc<ColumnArray>] {
                &self.columns
            }
            fn key_postings(&self, key: &ValueKey) -> Option<&Vec<u32>> {
                self.index.key.get(key)
            }
            fn key_partition_postings(
                &self,
                key: &ValueKey,
                partition: &ValueKey,
            ) -> Option<&Vec<u32>> {
                self.index.key_partition.as_ref()?.get(&(*key, *partition))
            }
        }
    };
}

impl_chunk_view!(Chunk);

/// The single writable chunk. Appends bulk-copy columns and maintain the group
/// index for the new rows only; readers take this chunk's lock briefly, so they
/// never observe a half-extended batch.
struct OpenChunk {
    base: u64,
    rows: usize,
    columns: Vec<std::sync::Arc<ColumnArray>>,
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
                .map(|c| std::sync::Arc::new(ColumnArray::with_capacity(c.data_type, chunk_rows)))
                .collect(),
            index: GroupIndex::empty(partitioned),
        }
    }

    /// Freeze a copy of this chunk's current content into an immutable chunk.
    /// The open chunk keeps its data so readers already holding the lock (or
    /// about to acquire it) still observe every published row. Columns are
    /// shared by `Arc` clone — the immutable chunk and the (soon replaced)
    /// open chunk reference the same buffers, so sealing is a refcount bump
    /// instead of a full-column copy.
    fn freeze_at(&self, base: u64) -> Chunk {
        let memory_bytes = self
            .columns
            .iter()
            .map(|c| c.estimated_size_bytes() as u64)
            .sum::<u64>()
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
        self.columns
            .iter()
            .map(|c| c.estimated_size_bytes() as u64)
            .sum::<u64>()
            + self.index.estimate_bytes()
    }
}

impl_chunk_view!(OpenChunk);

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
    key_rows: HashMap<ValueKey, u64, FastBuild>,
    key_partition_rows: HashMap<(ValueKey, ValueKey), u64, FastBuild>,
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
        // A3: zeroed-entry reclamation (the two `retain` sweeps) is NOT done
        // here — `compact_prefix` reclaims many chunks per pass and running
        // the retains per chunk made one compact O(chunks × distinct keys
        // ever seen). The caller retains once after its loop; lookups treat
        // a missing key as 0, so intermediate zero entries are invisible.
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
        let mut distinct: HashSet<ValueKey, FastBuild> = HashSet::with_hasher(FastBuild::default());
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
        // then retrying once. Checked UNDER the writer lock and against the
        // up-to-date counter: with the check outside, N concurrent appends all
        // passed the same stale projection and over-committed the limit by N-1
        // batches.
        let _writer = self.writer.lock().unwrap();
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

        // Grouping sources: keys are derived straight from each column range
        // in one fused pass per slice — no intermediate Vec<ValueKey> and no
        // second traversal. View-like key columns (dictionary/RLE) are
        // materialized once up front, and `BatchGrouper::new` validates the
        // shapes before any state is mutated.
        let mut owned_key_col: Option<ColumnArray> = None;
        let mut owned_partition_col: Option<ColumnArray> = None;
        let key_col = materialized_key_column(
            &mut owned_key_col,
            batch.column(self.key_column_index).ok_or_else(|| {
                TaibaiError::Internal("hot store: missing key column in batch".into())
            })?,
        );
        let partition_col = match self.partition_column_index {
            Some(idx) => Some(materialized_key_column(
                &mut owned_partition_col,
                batch.column(idx).ok_or_else(|| {
                    TaibaiError::Internal("hot store: missing partition column in batch".into())
                })?,
            )),
            None => None,
        };
        let mut grouper = BatchGrouper::new(key_col, partition_col, added)?;

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
                let dst = std::sync::Arc::get_mut(&mut open.columns[i])
                    .expect("open chunk columns are exclusively owned");
                if start == 0 && end == added {
                    dst.extend(src)?;
                } else {
                    dst.extend_range(src, start, end)?;
                }
            }
            let local_base = open.rows as u32;
            {
                // The pool write guard must drop before seal_open:
                // recompute_memory_bytes takes string_pool.read(), and
                // same-thread write→read on std's RwLock self-deadlocks.
                let mut pool = self.string_pool.write().unwrap();
                grouper.group(&mut pool, &mut open.index, start, end, local_base);
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
        let open_visit_pos = if reverse {
            visit.push(&*open_guard);
            visit.extend(st.sealed.iter().rev().map(|c| c.as_ref() as &dyn ChunkView));
            0usize
        } else {
            visit.extend(st.sealed.iter().map(|c| c.as_ref() as &dyn ChunkView));
            let pos = visit.len();
            visit.push(&*open_guard);
            pos
        };

        let proj: Vec<usize> = match projection {
            Some(p) => p.to_vec(),
            None => (0..self.schema.num_columns()).collect(),
        };
        let target = row_limit.unwrap_or(usize::MAX);

        // Large gathers (analytics over one key's full history) parallelize
        // across chunks on the rayon pool; point lookups and limited reads
        // stay on the serial walk with its early exit.
        if row_limit.is_none()
            && visit.len() > 1
            && estimate_candidates(&visit, &key, partition_key.as_ref(), min_offset, front)
                >= PARALLEL_GATHER_MIN_ROWS
        {
            use rayon::prelude::*;
            // (probed, skipped below watermark, gathered part)
            type Part = (bool, bool, Option<(Vec<ColumnDef>, Vec<ColumnArray>)>);
            let parts: Vec<Part> = visit
                .par_iter()
                .map(|&chunk| {
                    let base = chunk.base() - front;
                    if base + chunk.num_rows() <= min_offset {
                        return (false, true, None);
                    }
                    let postings = match partition_key {
                        Some(pk) => chunk.key_partition_postings(&key, &pk),
                        None => chunk.key_postings(&key),
                    };
                    let Some(postings) = postings else {
                        return (false, false, None);
                    };
                    let cut = watermark_cut(min_offset, base);
                    let slice = resident_postings(postings, cut);
                    if slice.is_empty() {
                        return (true, false, None);
                    }
                    let covered_partition = partition_key.and(self.partition_column_index);
                    let preds = compile_predicates(
                        chunk.columns(),
                        eq_predicates,
                        range_predicates,
                        self.key_column_index,
                        covered_partition,
                    );
                    let mut rows = Vec::with_capacity(slice.len());
                    if reverse {
                        for &row in slice.iter().rev() {
                            if preds.iter().all(|p| p.matches(row as usize)) {
                                rows.push(row);
                            }
                        }
                    } else {
                        for &row in slice {
                            if preds.iter().all(|p| p.matches(row as usize)) {
                                rows.push(row);
                            }
                        }
                    }
                    if rows.is_empty() {
                        return (true, false, None);
                    }
                    let columns: Vec<ColumnArray> = proj
                        .iter()
                        .map(|&ci| chunk.columns()[ci].take(&rows))
                        .collect();
                    let defs = proj
                        .iter()
                        .map(|&i| self.schema.columns[i].clone())
                        .collect();
                    (true, false, Some((defs, columns)))
                })
                .collect();
            let mut gathered: Option<(Vec<ColumnDef>, Vec<ColumnArray>)> = None;
            let mut seen = false;
            let mut skipped_by_watermark = false;
            for (probed, was_skipped, part) in parts {
                seen |= probed;
                skipped_by_watermark |= was_skipped;
                if let Some((defs, columns)) = part {
                    accumulate_part(&mut gathered, defs, columns);
                }
            }
            if !seen && skipped_by_watermark {
                // open_guard is held for the whole walk — consult the halves
                // directly instead of re-acquiring the read lock.
                if !(Self::key_in_sealed(&st, &key, partition_key.as_ref())
                    || Self::key_in_open(&open_guard, &key, partition_key.as_ref()))
                {
                    return None;
                }
            } else if !seen {
                return None;
            }
            let Some((defs, columns)) = gathered else {
                let col_defs: Vec<ColumnDef> = proj
                    .iter()
                    .map(|&i| self.schema.columns[i].clone())
                    .collect();
                return Some(RecordBatch::new(Schema::new(col_defs)));
            };
            return RecordBatch::try_new(Schema::new(defs), columns).ok();
        }

        let mut found: usize = 0;
        let mut seen = false;
        let mut skipped_by_watermark = false;
        let mut rows_buf: Vec<u32> = Vec::new();
        // Per contributing chunk: the row selection plus, for each projected
        // column, either the shared sealed source (`View`) or an already
        // gathered owned column for the open chunk (`Owned`).
        let mut chunk_selections: Vec<(Vec<u32>, Vec<SelectionSource>)> = Vec::new();

        for (visit_idx, chunk) in visit.iter().enumerate() {
            let chunk: &dyn ChunkView = *chunk;
            let is_open = visit_idx == open_visit_pos;
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
            let cut = watermark_cut(min_offset, base);
            let slice = resident_postings(postings, cut);
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
            let rows = std::mem::take(&mut rows_buf);
            let sources: Vec<SelectionSource> = proj
                .iter()
                .map(|&ci| {
                    if is_open {
                        // Open-chunk columns mutate in place; views cannot
                        // share them, so take only the selected rows.
                        SelectionSource::Owned(chunk.columns()[ci].take(&rows))
                    } else {
                        SelectionSource::Shared(chunk.columns()[ci].clone())
                    }
                })
                .collect();
            chunk_selections.push((rows, sources));
        }
        drop(open_guard);

        // Small results containing sealed-chunk rows become zero-copy views;
        // everything else keeps the fused memcpy gather, which beats
        // indirection for consumers.
        let has_shared_part = chunk_selections.iter().any(|(_, sources)| {
            sources
                .iter()
                .any(|s| matches!(s, SelectionSource::Shared(_)))
        });
        if seen && has_shared_part && found <= SELECTED_MAX_ROWS {
            let defs: Vec<ColumnDef> = proj
                .iter()
                .map(|&i| self.schema.columns[i].clone())
                .collect();
            let columns: Vec<ColumnArray> = proj
                .iter()
                .enumerate()
                .map(|(col_pos, &ci)| {
                    let data_type = self.schema.columns[ci].data_type;
                    let parts: Vec<(std::sync::Arc<ColumnArray>, std::sync::Arc<[u32]>)> =
                        chunk_selections
                            .iter()
                            .map(|(rows, sources)| match &sources[col_pos] {
                                SelectionSource::Shared(source) => {
                                    (source.clone(), std::sync::Arc::from(rows.clone()))
                                }
                                SelectionSource::Owned(owned) => {
                                    let identity: Vec<u32> = (0..owned.len() as u32).collect();
                                    (
                                        std::sync::Arc::new(owned.clone()),
                                        std::sync::Arc::from(identity),
                                    )
                                }
                            })
                            .collect();
                    ColumnArray::selected(std::sync::Arc::from(parts), data_type, found)
                })
                .collect();
            return RecordBatch::try_new(Schema::new(defs), columns).ok();
        }
        if seen && !has_shared_part && found <= SELECTED_MAX_ROWS && chunk_selections.len() == 1 {
            // Single open-chunk hit: the taken columns ARE the result; move
            // them without a copy.
            let (_, sources) = chunk_selections.remove(0);
            let defs: Vec<ColumnDef> = proj
                .iter()
                .map(|&i| self.schema.columns[i].clone())
                .collect();
            let columns: Vec<ColumnArray> = sources
                .into_iter()
                .map(|s| match s {
                    SelectionSource::Owned(col) => col,
                    SelectionSource::Shared(_) => unreachable!("checked above"),
                })
                .collect();
            return RecordBatch::try_new(Schema::new(defs), columns).ok();
        }
        let mut gathered: Option<(Vec<ColumnDef>, Vec<ColumnArray>)> = None;
        for (rows, sources) in chunk_selections {
            if gathered.is_none() {
                let defs = proj
                    .iter()
                    .map(|&i| self.schema.columns[i].clone())
                    .collect();
                let columns = proj
                    .iter()
                    .map(|&i| ColumnArray::empty(self.schema.columns[i].data_type))
                    .collect();
                gathered = Some((defs, columns));
            }
            let mut sources = sources.into_iter();
            if let Some((_, acc)) = gathered.as_mut() {
                for col in acc.iter_mut() {
                    match sources.next() {
                        Some(SelectionSource::Shared(source)) => {
                            // A gather failure abandons the whole answer:
                            // partial rows would under-count the group.
                            source.gather_into(&rows, col).ok()?;
                        }
                        Some(SelectionSource::Owned(owned)) => {
                            // Rows were already gathered from the open chunk;
                            // append them (buffer move for variable-width).
                            col.append_owned(owned).ok();
                        }
                        None => {}
                    }
                }
            }
        }

        if !seen {
            if skipped_by_watermark {
                // The key was never probed above the watermark. Keep the
                // None-vs-empty contract: a key that exists only under the
                // watermark yields Some(empty), never None. The open read
                // guard was dropped before the gather above, so this
                // re-acquisition is not recursive.
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
        if Self::key_in_sealed(st, key, partition_key) {
            return true;
        }
        let open = st.open.read().unwrap();
        Self::key_in_open(&open, key, partition_key)
    }

    fn key_in_sealed(st: &StoreState, key: &ValueKey, partition_key: Option<&ValueKey>) -> bool {
        st.sealed.iter().any(|c| match partition_key {
            Some(pk) => c.key_partition_postings(key, pk).is_some(),
            None => c.key_postings(key).is_some(),
        })
    }

    fn key_in_open(
        open: &std::sync::RwLockReadGuard<'_, OpenChunk>,
        key: &ValueKey,
        partition_key: Option<&ValueKey>,
    ) -> bool {
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

        let mut total = 0u64;
        let (probed, skipped) = walk_resident_postings(
            &st,
            &open,
            &key,
            partition_key.as_ref(),
            min_offset,
            |slice, _| total += slice.len() as u64,
        );
        if !probed && skipped {
            // The `open` read guard is held — avoid re-acquiring it.
            if !(Self::key_in_sealed(&st, &key, partition_key.as_ref())
                || Self::key_in_open(&open, &key, partition_key.as_ref()))
            {
                return None;
            }
        }
        (probed || skipped).then_some(total)
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
        let covered_partition = partition_key.and(self.partition_column_index);
        let mut total = 0u64;
        let (probed, skipped) = walk_resident_postings(
            &st,
            &open,
            &key,
            partition_key.as_ref(),
            min_offset,
            |slice, chunk| {
                let preds = compile_predicates(
                    chunk.columns(),
                    eq_predicates,
                    range_predicates,
                    self.key_column_index,
                    covered_partition,
                );
                total += slice
                    .iter()
                    .filter(|&&row| preds.iter().all(|p| p.matches(row as usize)))
                    .count() as u64;
            },
        );
        if !probed && skipped {
            // The `open` read guard is held — avoid re-acquiring it.
            if !(Self::key_in_sealed(&st, &key, partition_key.as_ref())
                || Self::key_in_open(&open, &key, partition_key.as_ref()))
            {
                return None;
            }
        }
        (probed || skipped).then_some(total)
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
                let columns: Vec<std::sync::Arc<ColumnArray>> = chunk
                    .columns
                    .iter()
                    .map(|c| std::sync::Arc::new(c.slice_fast(local_cut, rows)))
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

        // A3: reclaim the summary's zeroed entries ONCE per compact pass
        // instead of once per reclaimed chunk. A high-cardinality key stream
        // (one new key per row) otherwise grows the summary by "distinct
        // keys ever seen" forever — multi-GB over months, and invisible to
        // the memory accounting that gates appends. Lookups treat a missing
        // key as 0, so dropping the zero entries is observationally
        // identical.
        summary.key_rows.retain(|_, rows| *rows > 0);
        summary.key_partition_rows.retain(|_, rows| *rows > 0);

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
                let columns: Vec<std::sync::Arc<ColumnArray>> = open
                    .columns
                    .iter()
                    .map(|c| std::sync::Arc::new(c.slice_fast(local_cut, open.rows)))
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
        // Reclaim interned strings the surviving chunks no longer reference:
        // without this the pool grows with every distinct key ever appended,
        // even after its rows were reclaimed under backpressure.
        {
            let new_state = self.state.read().unwrap().clone();
            let live = {
                let open = new_state.open.read().unwrap();
                collect_live_pool_ids(&new_state, &open)
            };
            self.string_pool.write().unwrap().retain_ids(&live);
        }
        self.recompute_memory_bytes();
        n
    }

    /// Rebuild the group index of a trimmed chunk directly from its columns
    /// through the same `KeySource` accessor the append path uses (string
    /// values are already interned, so this is a lookup-only pass).
    fn rebuild_index(&self, columns: &[std::sync::Arc<ColumnArray>]) -> GroupIndex {
        let mut pool = self.string_pool.write().unwrap();
        let mut index = GroupIndex::empty(self.partition_column_index.is_some());
        let Some(key_col) = columns.get(self.key_column_index) else {
            return index;
        };
        // Sliced columns may still be dictionary/RLE views; materialize the
        // same way the append path does. An unsupported shape yields an empty
        // index, matching the previous per-column collect fallback.
        let mut owned_key: Option<ColumnArray> = None;
        let mut key_src = match KeySource::new(materialized_key_column(&mut owned_key, key_col)) {
            Ok(src) => src,
            Err(_) => return index,
        };
        let mut owned_partition: Option<ColumnArray> = None;
        let mut part_src = match self.partition_column_index.and_then(|i| columns.get(i)) {
            Some(col) => KeySource::new(materialized_key_column(&mut owned_partition, col)).ok(),
            None => None,
        };
        let rows = key_col.len();
        for row in 0..rows {
            let key = key_src.key_at(row, &mut pool);
            index.key.entry(key).or_default().push(row as u32);
            if let (Some(kp), Some(psrc)) = (index.key_partition.as_mut(), part_src.as_mut()) {
                let pkey = psrc.key_at(row, &mut pool);
                kp.entry((key, pkey)).or_default().push(row as u32);
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

/// Intern ids still referenced by any surviving chunk's group indexes (the
/// key index plus both components of the composite key/partition index).
fn collect_live_pool_ids(st: &StoreState, open: &OpenChunk) -> HashSet<u32> {
    fn collect(index: &GroupIndex, live: &mut HashSet<u32>) {
        for key in index.key.keys() {
            if let ValueKey::Str(id) = key {
                live.insert(*id);
            }
        }
        if let Some(kp) = &index.key_partition {
            for (key, partition) in kp.keys() {
                if let ValueKey::Str(id) = key {
                    live.insert(*id);
                }
                if let ValueKey::Str(id) = partition {
                    live.insert(*id);
                }
            }
        }
    }
    let mut live = HashSet::new();
    for chunk in &st.sealed {
        collect(&chunk.index, &mut live);
    }
    collect(&open.index, &mut live);
    live
}

/// Walk all chunks in append order (sealed, then open) and hand each chunk's
/// resident postings of `key` (watermark cut applied) to `visit`. Returns
/// `(probed, skipped_below_watermark)` for the None-vs-empty contract:
/// `probed` means at least one chunk had postings for the key, `skipped`
/// means at least one chunk sat entirely below the watermark.
fn walk_resident_postings(
    st: &StoreState,
    open: &OpenChunk,
    key: &ValueKey,
    partition_key: Option<&ValueKey>,
    min_offset: u64,
    mut visit: impl FnMut(&[u32], &dyn ChunkView),
) -> (bool, bool) {
    let front = st.front;
    let mut probed = false;
    let mut skipped = false;
    for chunk in sealed_then_open(st, open) {
        let base = chunk.base() - front;
        if base + chunk.num_rows() <= min_offset {
            skipped = true;
            continue;
        }
        let postings = match partition_key {
            Some(pk) => chunk.key_partition_postings(key, pk),
            None => chunk.key_postings(key),
        };
        let Some(postings) = postings else {
            continue;
        };
        probed = true;
        let cut = watermark_cut(min_offset, base);
        let slice = resident_postings(postings, cut);
        if !slice.is_empty() {
            visit(slice, chunk);
        }
    }
    (probed, skipped)
}

/// One chunk's contribution to a lookup result, per projected column: either
/// the sealed chunk's shared column (rows selected by index) or an owned
/// gather of the open chunk's selected rows.
enum SelectionSource {
    Shared(std::sync::Arc<ColumnArray>),
    Owned(ColumnArray),
}

/// Result rows at or below this count are returned as zero-copy `Selected`
/// views; larger results use the fused memcpy gather.
const SELECTED_MAX_ROWS: usize = 512;

/// Minimum visible candidate rows before a gather is worth spreading across
/// the rayon pool. Below this the serial walk wins (no task-spawn overhead).
const PARALLEL_GATHER_MIN_ROWS: u64 = 32_768;

/// Visible posting entries across all chunks, used to size up a gather before
/// deciding between the serial and parallel walks. Early-outs at the parallel
/// threshold so point lookups pay for at most one extra chunk probe.
fn estimate_candidates(
    visit: &[&dyn ChunkView],
    key: &ValueKey,
    partition_key: Option<&ValueKey>,
    min_offset: u64,
    front: u64,
) -> u64 {
    let mut total = 0u64;
    for &chunk in visit {
        let base = chunk.base() - front;
        if base + chunk.num_rows() <= min_offset {
            continue;
        }
        let postings = match partition_key {
            Some(pk) => chunk.key_partition_postings(key, pk),
            None => chunk.key_postings(key),
        };
        if let Some(postings) = postings {
            let cut = watermark_cut(min_offset, base);
            total += resident_postings(postings, cut).len() as u64;
            if total >= PARALLEL_GATHER_MIN_ROWS {
                break;
            }
        }
    }
    total
}

/// Fold one chunk's gathered columns into the output accumulator, preserving
/// visit order. `append_owned` moves variable-width buffers instead of
/// re-copying them row by row.
fn accumulate_part(
    gathered: &mut Option<(Vec<ColumnDef>, Vec<ColumnArray>)>,
    defs: Vec<ColumnDef>,
    columns: Vec<ColumnArray>,
) {
    match gathered {
        Some((_, acc)) => {
            for (dst, src) in acc.iter_mut().zip(columns) {
                dst.append_owned(src).ok();
            }
        }
        slot @ None => {
            *slot = Some((defs, columns));
        }
    }
}

fn estimate_batch_memory(batch: &RecordBatch) -> u64 {
    let mut total: u64 = 0;
    for i in 0..batch.num_columns() {
        if let Some(col) = batch.column(i) {
            // Capacity-exact accounting per column. The old fixed-width table
            // (VarChar 24 / Blob 32 / Vector 64 bytes per row) undercounted
            // wide strings and vector columns by 10-50x, so the soft/hard
            // memory gates let those tables blow far past their limits before
            // the next seal-time recount noticed.
            total += col.estimated_size_bytes() as u64;
        }
    }
    total += (batch.num_rows() as u64) * 4; // inverted-index row offsets
    total
}

/// Registry of resident hot stores, keyed by `(db, table)`.
pub struct HotStoreManager {
    stores: RwLock<HashMap<(String, String), Arc<HotStore>>>,
    invalidated: RwLock<HashSet<(String, String)>>,
    /// Per-table feed accounting for the backpressure reclaim: `in_flight`
    /// counts rows appended to the store whose memtable staging has not
    /// completed yet (they must never be treated as persisted prefix), and
    /// `epoch` is bumped whenever a reclaim/invalidation shortens the store
    /// so stale residency marks are not applied afterwards.
    feed_accounting: Mutex<HashMap<(String, String), FeedAccounting>>,
    memory_limit_bytes: u64,
}

#[derive(Default)]
struct FeedAccounting {
    in_flight: u64,
    epoch: u64,
}

impl HotStoreManager {
    pub fn new() -> Self {
        Self::with_memory_limit_bytes(8 * 1024 * 1024 * 1024)
    }

    pub fn with_memory_limit_bytes(memory_limit_bytes: u64) -> Self {
        Self {
            stores: RwLock::new(HashMap::new()),
            invalidated: RwLock::new(HashSet::new()),
            feed_accounting: Mutex::new(HashMap::new()),
            memory_limit_bytes,
        }
    }

    /// Open a hot-store feed: accounts the batch as in flight until the
    /// caller's memtable staging completes (see `HotFeedGuard`) and returns
    /// the current feed epoch for stale-mark detection.
    pub fn begin_feed(&self, db: &str, table: &str) -> u64 {
        let mut accounting = self.feed_accounting.lock().unwrap();
        let state = accounting
            .entry((db.to_string(), table.to_string()))
            .or_default();
        state.in_flight = state.in_flight.saturating_add(1);
        state.epoch
    }

    /// Close a hot-store feed opened by [`Self::begin_feed`].
    pub fn end_feed(&self, db: &str, table: &str) {
        let mut accounting = self.feed_accounting.lock().unwrap();
        if let Some(state) = accounting.get_mut(&(db.to_string(), table.to_string())) {
            state.in_flight = state.in_flight.saturating_sub(1);
        }
    }

    /// Rows currently between hot-store append and memtable staging.
    pub fn in_flight(&self, db: &str, table: &str) -> u64 {
        self.feed_accounting
            .lock()
            .unwrap()
            .get(&(db.to_string(), table.to_string()))
            .map(|state| state.in_flight)
            .unwrap_or(0)
    }

    /// Current feed epoch; bumped by every reclaim/invalidation below.
    pub fn feed_epoch(&self, db: &str, table: &str) -> u64 {
        self.feed_accounting
            .lock()
            .unwrap()
            .get(&(db.to_string(), table.to_string()))
            .map(|state| state.epoch)
            .unwrap_or(0)
    }

    fn bump_feed_epoch(&self, db: &str, table: &str) {
        let mut accounting = self.feed_accounting.lock().unwrap();
        accounting
            .entry((db.to_string(), table.to_string()))
            .or_default()
            .epoch += 1;
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
        self.bump_feed_epoch(db, table);
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

    /// Drop an append-only hot store after an in-place mutation and mark the
    /// table STALE. A stale table is not served (`get` finds nothing, readers
    /// fall back to the generic scan) and not fed from a single batch:
    /// [`Self::rebuild_from_live_state`] — called by the engine at the next
    /// feed — is the only sound recreation, because the scan-side watermark
    /// derivation (`row_count - live_memtable_rows`) requires EVERY live
    /// memtable row to be in the store. Recreating from subsequent INSERTs
    /// alone would hide older live memtable rows behind an incomplete fast
    /// path, which is why this used to disable the table until restart.
    pub fn invalidate(&self, db: &str, table: &str) {
        let table_key = (db.to_string(), table.to_string());
        self.bump_feed_epoch(db, table);
        self.stores.write().unwrap().remove(&table_key);
        self.invalidated.write().unwrap().insert(table_key);
    }

    /// True while the table's store was dropped by [`Self::invalidate`] and
    /// awaits a full-live-state rebuild.
    pub fn is_stale(&self, db: &str, table: &str) -> bool {
        self.invalidated
            .read()
            .unwrap()
            .contains(&(db.to_string(), table.to_string()))
    }

    /// Rebuild a stale table's store from its FULL live state — every
    /// reader-visible memtable batch — clearing the stale marker. This is
    /// the only sound recreation after an in-place mutation (see
    /// [`Self::invalidate`]); it coalesces any number of invalidating
    /// mutations into one rebuild. Returns the fresh store, or `None` for
    /// ineligible tables. No-op (returns the existing store) when not stale.
    pub fn rebuild_from_live_state(
        &self,
        db: &str,
        table: &str,
        table_meta: &TableMeta,
        live_batches: &[std::sync::Arc<RecordBatch>],
    ) -> Result<Option<std::sync::Arc<HotStore>>> {
        let table_key = (db.to_string(), table.to_string());
        if !self.invalidated.read().unwrap().contains(&table_key) {
            return Ok(self.get(db, table));
        }
        let Some(key_idx) = resolve_key_column(table_meta) else {
            // Ineligible after all (e.g. the key column was dropped): stop
            // marking it stale so feeds stop attempting rebuilds.
            self.invalidated.write().unwrap().remove(&table_key);
            return Ok(None);
        };
        let schema = Schema::new(table_meta.columns.clone());
        let partition_idx = table_meta.get_partition_column_index_cached();
        let store = std::sync::Arc::new(HotStore::new(
            schema,
            key_idx,
            partition_idx,
            self.memory_limit_bytes,
        ));
        for batch in live_batches {
            if batch.num_rows() > 0 {
                store.append(batch)?;
            }
        }
        self.bump_feed_epoch(db, table);
        {
            let mut stores = self.stores.write().unwrap();
            stores.insert(table_key.clone(), std::sync::Arc::clone(&store));
        }
        self.invalidated.write().unwrap().remove(&table_key);
        Ok(Some(store))
    }

    pub fn clear_resident_coverage(&self, db: &str, table: &str) {
        self.bump_feed_epoch(db, table);
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
