use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::storage::segment::SegmentReader;

/// Conservative average heap bytes charged per cached reader. Serves two
/// purposes:
///
/// 1. Deriving the cache's byte budget from the configured entry count
///    (`storage.reader_cache_capacity`, default 1024):
///    `budget = capacity * AVG_READER_COST_BYTES`, i.e. 256 MiB by default.
/// 2. The per-entry cost charged for an mmap-backed reader: the mapping
///    itself is page-cache backed and consumes no heap, so only the parsed
///    footer (row-group metadata, zone maps, delete bitmap) and the index
///    vectors count. Charging the same constant keeps the worst-case
///    mmap-entry count at the configured capacity, matching the old
///    count-based cache for large sealed segments.
///
/// Owned readers (full heap copies, used for sealed segments below
/// `min_seal_size` and for mutable segments) are charged their real file
/// length instead, so a cache full of them is bounded by the byte budget
/// rather than by `capacity` entries of up to ~255 MB each.
pub(super) const AVG_READER_COST_BYTES: u64 = 256 * 1024;

pub(super) struct ReaderCache {
    budget_bytes: u64,
    used_bytes: u64,
    /// Cached reader plus the budget bytes charged for it. The charge is
    /// remembered per entry so `invalidate`/eviction can release exactly what
    /// was reserved, including after a same-path re-put with a new cost.
    entries: HashMap<PathBuf, (Arc<SegmentReader>, u64)>,
    order: VecDeque<PathBuf>,
}

impl ReaderCache {
    /// Build a cache with a byte budget of `capacity * AVG_READER_COST_BYTES`
    /// (at least one entry's worth). The count-based config knob stays the
    /// only tunable so existing deployments keep working unchanged; only the
    /// internal bound switches from "N entries" to "N * 256 KiB bytes".
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            budget_bytes: capacity.max(1) as u64 * AVG_READER_COST_BYTES,
            used_bytes: 0,
            entries: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    pub(super) fn get(&mut self, path: &Path) -> Option<Arc<SegmentReader>> {
        let key = path.to_path_buf();
        let (reader, _) = self.entries.get(&key)?.clone();
        self.touch(&key);
        Some(reader)
    }

    /// Cache `reader` under `path`, charging `cost_bytes` of budget for it
    /// (file length for owned readers, `AVG_READER_COST_BYTES` for mmap
    /// readers). Evicts least-recently-used entries until the cache fits its
    /// budget. An entry whose cost alone exceeds the whole budget is NOT
    /// cached: inserting it would evict every other entry and still leave the
    /// cache over budget, so it is handed straight back to the caller.
    pub(super) fn put(
        &mut self,
        path: PathBuf,
        reader: SegmentReader,
        cost_bytes: u64,
    ) -> Arc<SegmentReader> {
        let reader = Arc::new(reader);
        if cost_bytes > self.budget_bytes {
            return reader;
        }
        match self.entries.insert(path.clone(), (Arc::clone(&reader), cost_bytes)) {
            // Same-path re-put replaces the reader in place (keeping its LRU
            // position, as before) and must release the old entry's charge.
            Some((_, previous_cost)) => self.used_bytes -= previous_cost,
            None => self.order.push_back(path),
        }
        self.used_bytes += cost_bytes;
        self.evict_if_needed();
        reader
    }

    pub(super) fn invalidate(&mut self, path: &Path) {
        let key = path.to_path_buf();
        if let Some((_, cost)) = self.entries.remove(&key) {
            self.used_bytes -= cost;
        }
        if let Some(pos) = self.order.iter().position(|p| p == &key) {
            self.order.remove(pos);
        }
    }

    pub(super) fn invalidate_under(&mut self, dir: &Path) {
        let keys: Vec<PathBuf> = self
            .entries
            .keys()
            .filter(|path| path.starts_with(dir))
            .cloned()
            .collect();
        for key in keys {
            self.invalidate(&key);
        }
    }

    fn touch(&mut self, path: &PathBuf) {
        if let Some(pos) = self.order.iter().position(|p| p == path) {
            self.order.remove(pos);
        }
        self.order.push_back(path.clone());
    }

    fn evict_if_needed(&mut self) {
        // The newest entry sits at the back of `order` and costs at most the
        // whole budget (oversized entries never reach the map), so this loop
        // always terminates with the just-inserted entry surviving.
        while self.used_bytes > self.budget_bytes {
            if let Some(oldest) = self.order.pop_front() {
                if let Some((_, cost)) = self.entries.remove(&oldest) {
                    self.used_bytes -= cost;
                }
            } else {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::types::DataType;
    use crate::types::batch::{ColumnDef, RecordBatch, Schema};
    use crate::types::scalar::ScalarValue;

    const KIB: u64 = 1024;

    /// Capacity 4 -> budget 4 * 256 KiB = 1 MiB, small enough to exercise
    /// eviction with hand-picked costs.
    fn cache() -> ReaderCache {
        ReaderCache::new(4)
    }

    fn write_segment(dir: &Path, name: &str) -> PathBuf {
        let path = dir.join(name);
        let schema = Schema::new(vec![ColumnDef::new("v", DataType::Int64)]);
        let mut writer =
            crate::storage::segment::SegmentWriter::create(&path, schema.clone(), 8192, 1).unwrap();
        let mut batch = RecordBatch::new(schema);
        batch
            .column_mut(0)
            .unwrap()
            .push(&ScalarValue::Int64(Some(7)))
            .unwrap();
        writer.write_batch(&batch).unwrap();
        writer.seal().unwrap();
        path
    }

    fn owned_reader(path: &Path) -> SegmentReader {
        SegmentReader::open_owned(path).unwrap()
    }

    #[test]
    fn budget_derives_from_configured_capacity() {
        assert_eq!(
            ReaderCache::new(1024).budget_bytes,
            1024 * AVG_READER_COST_BYTES
        );
        // A degenerate capacity still buys one average entry's worth.
        assert_eq!(ReaderCache::new(0).budget_bytes, AVG_READER_COST_BYTES);
    }

    #[test]
    fn entries_within_budget_coexist() {
        let temp = tempfile::tempdir().unwrap();
        let mut cache = cache();
        let paths: Vec<PathBuf> = (0..3)
            .map(|i| write_segment(temp.path(), &format!("seg_{i}.seg")))
            .collect();
        for (path, cost) in paths.iter().zip([100 * KIB, 200 * KIB, 300 * KIB]) {
            cache.put(path.clone(), owned_reader(path), cost);
        }
        for path in &paths {
            assert!(
                cache.get(path).is_some(),
                "entry {} should stay cached while the total fits the budget",
                path.display()
            );
        }
    }

    #[test]
    fn putting_over_budget_evicts_oldest_first() {
        let temp = tempfile::tempdir().unwrap();
        let mut cache = cache(); // 1 MiB budget
        let a = write_segment(temp.path(), "a.seg");
        let b = write_segment(temp.path(), "b.seg");
        let c = write_segment(temp.path(), "c.seg");
        let d = write_segment(temp.path(), "d.seg");

        cache.put(a.clone(), owned_reader(&a), 400 * KIB);
        cache.put(b.clone(), owned_reader(&b), 300 * KIB);
        cache.put(c.clone(), owned_reader(&c), 300 * KIB); // 1000 KiB == budget
        for path in [&a, &b, &c] {
            assert!(cache.get(path).is_some());
        }

        // 1000 + 300 > 1024 KiB: the oldest entry (a) must go, not arbitrary
        // ones — previously the cache only bounded the entry count, so 4
        // entries of 255 MB each would all have been kept.
        cache.put(d.clone(), owned_reader(&d), 300 * KIB);
        assert!(cache.get(&a).is_none(), "oldest entry must be evicted");
        for path in [&b, &c, &d] {
            assert!(cache.get(path).is_some());
        }
    }

    #[test]
    fn entry_larger_than_total_budget_is_not_cached() {
        let temp = tempfile::tempdir().unwrap();
        let mut cache = cache(); // 1 MiB budget
        let small = write_segment(temp.path(), "small.seg");
        let huge = write_segment(temp.path(), "huge.seg");

        cache.put(small.clone(), owned_reader(&small), 100 * KIB);
        let returned = cache.put(huge.clone(), owned_reader(&huge), 2 * 1024 * KIB);

        assert!(
            cache.get(&huge).is_none(),
            "an entry costing more than the whole budget must not be cached"
        );
        assert!(
            cache.get(&small).is_some(),
            "an oversized put must not disturb existing entries"
        );
        // The caller still receives a usable reader for the oversized entry.
        assert_eq!(returned.row_count(), 1);
    }

    #[test]
    fn get_refreshes_lru_order_on_touch() {
        let temp = tempfile::tempdir().unwrap();
        let mut cache = cache(); // 1 MiB budget
        let a = write_segment(temp.path(), "a.seg");
        let b = write_segment(temp.path(), "b.seg");
        let c = write_segment(temp.path(), "c.seg");
        let d = write_segment(temp.path(), "d.seg");

        cache.put(a.clone(), owned_reader(&a), 300 * KIB);
        cache.put(b.clone(), owned_reader(&b), 300 * KIB);
        cache.put(c.clone(), owned_reader(&c), 300 * KIB); // 900 KiB

        // Touch a: b becomes the least recently used entry.
        assert!(cache.get(&a).is_some());

        cache.put(d.clone(), owned_reader(&d), 300 * KIB); // 1200 > 1024 KiB
        assert!(cache.get(&b).is_none(), "untouched entry must be evicted first");
        for path in [&a, &c, &d] {
            assert!(cache.get(path).is_some());
        }
    }

    #[test]
    fn same_path_reput_releases_the_previous_charge() {
        let temp = tempfile::tempdir().unwrap();
        let mut cache = cache(); // 1 MiB budget
        let a = write_segment(temp.path(), "a.seg");
        let b = write_segment(temp.path(), "b.seg");

        cache.put(a.clone(), owned_reader(&a), 900 * KIB);
        // Re-put the same path with a much smaller charge: the 900 KiB must
        // be released, not double-counted.
        cache.put(a.clone(), owned_reader(&a), 100 * KIB);
        cache.put(b.clone(), owned_reader(&b), 900 * KIB); // 1000 KiB <= budget

        assert!(cache.get(&a).is_some());
        assert!(cache.get(&b).is_some());
        assert_eq!(cache.used_bytes, 1000 * KIB);
    }

    #[test]
    fn invalidate_releases_budget_for_reuse() {
        let temp = tempfile::tempdir().unwrap();
        let mut cache = cache(); // 1 MiB budget
        let a = write_segment(temp.path(), "a.seg");
        let b = write_segment(temp.path(), "b.seg");

        cache.put(a.clone(), owned_reader(&a), 900 * KIB);
        cache.invalidate(&a);
        assert!(cache.get(&a).is_none());
        assert_eq!(cache.used_bytes, 0);

        // Only fits because a's bytes were released.
        cache.put(b.clone(), owned_reader(&b), 900 * KIB);
        assert!(cache.get(&b).is_some());
    }
}
