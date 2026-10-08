//! Chunk storage, postings and sealed summaries.

use super::*;

// ---------------------------------------------------------------------------
// Chunks
// ---------------------------------------------------------------------------

/// Group index of one chunk: per distinct key value -> ascending local row
/// offsets (the `g#` attribute analogue), plus the composite key/partition
/// index used when a lookup pins the partition column.
pub(super) struct GroupIndex {
    pub(super) key: HashMap<ValueKey, Vec<u32>, FastBuild>,
    pub(super) key_partition: Option<HashMap<(ValueKey, ValueKey), Vec<u32>, FastBuild>>,
}

impl GroupIndex {
    pub(super) fn empty(partitioned: bool) -> Self {
        Self {
            key: HashMap::with_hasher(FastBuild::default()),
            key_partition: partitioned.then(|| HashMap::with_hasher(FastBuild::default())),
        }
    }

    pub(super) fn estimate_bytes(&self) -> u64 {
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
pub(super) struct Chunk {
    pub(super) base: u64,
    pub(super) rows: u32,
    pub(super) columns: Vec<std::sync::Arc<ColumnArray>>,
    pub(super) index: GroupIndex,
    pub(super) memory_bytes: u64,
}

/// Read-only view shared by sealed chunks and the open chunk, so the lookup
/// walk can treat both uniformly. `Send + Sync` lets the parallel gather walk
/// chunks on the rayon pool.
pub(super) trait ChunkView: Sync {
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
pub(super) struct OpenChunk {
    pub(super) base: u64,
    pub(super) rows: usize,
    pub(super) columns: Vec<std::sync::Arc<ColumnArray>>,
    pub(super) index: GroupIndex,
}

impl OpenChunk {
    pub(super) fn new(schema: &Schema, base: u64, chunk_rows: usize, partitioned: bool) -> Self {
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
    pub(super) fn freeze_at(&self, base: u64) -> Chunk {
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

    pub(super) fn estimated_memory_bytes(&self) -> u64 {
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
pub(super) struct StoreState {
    pub(super) front: u64,
    pub(super) sealed: Vec<Arc<Chunk>>,
    pub(super) summary: SealedSummary,
    pub(super) open: Arc<RwLock<OpenChunk>>,
}

/// Seal-time rollup of the sealed chunks' group indexes: rows per key value,
/// and rows per (key, partition) pair when the table is partitioned.
#[derive(Default, Clone)]
pub(super) struct SealedSummary {
    pub(super) key_rows: HashMap<ValueKey, u64, FastBuild>,
    pub(super) key_partition_rows: HashMap<(ValueKey, ValueKey), u64, FastBuild>,
}

impl SealedSummary {
    pub(super) fn add_chunk(&mut self, index: &GroupIndex) {
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
    pub(super) fn remove_chunk(&mut self, index: &GroupIndex, cut: Option<usize>) {
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
