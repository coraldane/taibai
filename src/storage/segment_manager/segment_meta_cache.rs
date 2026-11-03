use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::storage::segment::meta::SegmentMetaSnapshot;

use super::byte_lru::ByteLru;

/// Parsed segment-metadata cache.
///
/// # Bound
///
/// Byte-budgeted, not entry-counted. The previous implementation kept
/// `capacity` entries (default 1024, from `storage.reader_cache_capacity`) and
/// never looked at their size. A sealed 2 GiB segment's footer parses into
/// ~24 MiB of row-group metadata — bloom filters for every column of every
/// row group dominate — so the same knob silently authorized a 24 GiB
/// resident set. Nothing reported it: the cache was absent from
/// `SHOW MEMORY`, and an idle server never calls `put`, so the eviction path
/// never ran either.
///
/// Entries are charged [`SegmentMetaSnapshot::estimated_heap_bytes`], an
/// explicit walk of the owned buffers. An entry bigger than the whole budget
/// is handed back to the caller but not cached, so one outsized segment
/// cannot flush the working set.
pub(super) struct SegmentMetaCache {
    inner: ByteLru<PathBuf, Arc<SegmentMetaSnapshot>>,
}

impl SegmentMetaCache {
    /// The floor (`>= 1 MiB`) is applied where the config value is installed,
    /// so tests can drive eviction with budgets too small for production.
    pub(super) fn new(budget_bytes: u64) -> Self {
        Self {
            inner: ByteLru::new(budget_bytes),
        }
    }

    pub(super) fn get(&mut self, path: &Path) -> Option<Arc<SegmentMetaSnapshot>> {
        self.inner.get(&path.to_path_buf()).cloned()
    }

    pub(super) fn put(
        &mut self,
        path: PathBuf,
        meta: SegmentMetaSnapshot,
    ) -> Arc<SegmentMetaSnapshot> {
        let meta = Arc::new(meta);
        let cost_bytes = meta.estimated_heap_bytes();
        self.inner.put(path, Arc::clone(&meta), cost_bytes);
        meta
    }

    pub(super) fn invalidate(&mut self, path: &Path) {
        self.inner.invalidate(&path.to_path_buf());
    }

    pub(super) fn invalidate_under(&mut self, dir: &Path) {
        self.inner.retain(|path| !path.starts_with(dir));
    }

    /// `(entries, charged heap bytes, budget bytes)` for `SHOW MEMORY`.
    pub(super) fn stats(&self) -> (usize, u64, u64) {
        self.inner.stats()
    }
}

#[cfg(test)]
#[path = "segment_meta_cache_test.rs"]
mod tests;
