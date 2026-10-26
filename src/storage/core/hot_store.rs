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

//! Implementation modules separate string pooling, key grouping, predicate
//! compilation, chunk storage, reads and the store-manager lifecycle.

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

mod chunks;
mod keys;
mod manager;
mod pool;
mod predicates;
mod read;
use chunks::*;
pub use keys::resolve_key_column;
use keys::*;
pub use manager::HotStoreManager;
pub use pool::StringPool;
use predicates::*;

/// Borrow the cached dense key representation without cloning its buffers.
fn materialized_key_column(column: &ColumnArray) -> &ColumnArray {
    column.flattened()
}

#[derive(Debug, Clone, Default)]
pub struct ResidentCoverageSnapshot {
    fully_resident: bool,
    resident_partitions: HashSet<String>,
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
        let key_col =
            materialized_key_column(batch.column(self.key_column_index).ok_or_else(|| {
                TaibaiError::Internal("hot store: missing key column in batch".into())
            })?);
        let partition_col = match self.partition_column_index {
            Some(idx) => Some(materialized_key_column(batch.column(idx).ok_or_else(
                || TaibaiError::Internal("hot store: missing partition column in batch".into()),
            )?)),
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
        let mut key_src = match KeySource::new(materialized_key_column(key_col)) {
            Ok(src) => src,
            Err(_) => return index,
        };
        let mut part_src = match self.partition_column_index.and_then(|i| columns.get(i)) {
            Some(col) => KeySource::new(materialized_key_column(col)).ok(),
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

/// Result rows at or below this count are returned as zero-copy `Selected`
/// views; larger results use the fused memcpy gather.
const SELECTED_MAX_ROWS: usize = 512;

/// Minimum visible candidate rows before a gather is worth spreading across
/// the rayon pool. Below this the serial walk wins (no task-spawn overhead).
const PARALLEL_GATHER_MIN_ROWS: u64 = 32_768;

fn estimate_batch_memory(batch: &RecordBatch) -> u64 {
    // Include index overhead: each row may insert into the group HashMap
    // (key pointer + vec entry ~ 40 bytes per row for high-cardinality keys).
    batch.estimated_size_bytes() as u64 + batch.num_rows() as u64 * 44
}

#[cfg(test)]
#[path = "hot_store_test.rs"]
mod tests;

#[cfg(test)]
#[path = "hot_store/regression_test.rs"]
mod regression_tests;
