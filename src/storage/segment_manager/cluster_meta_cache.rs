use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};

use crate::storage::partition::pruning::ZoneMap;

const CAPACITY: usize = 16384;

#[derive(Default)]
pub(super) struct ClusterMetaCache {
    entries: HashMap<(PathBuf, usize), Option<ZoneMap>>,
    /// Insertion order for INCREMENTAL eviction. The previous policy —
    /// clearing the whole map at capacity — made every subsequent ordered
    /// scan re-read and re-parse each segment footer it touched (a
    /// stampede), then rebuild the cache from scratch. Evicting the oldest
    /// HALF keeps the recent working set warm and bounds the churn to a
    /// fraction of the map per cycle.
    order: VecDeque<(PathBuf, usize)>,
}

impl ClusterMetaCache {
    pub(super) fn new() -> Self {
        Self {
            entries: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    pub(super) fn get(&self, path: &Path, col_idx: usize) -> Option<Option<ZoneMap>> {
        self.entries.get(&(path.to_path_buf(), col_idx)).cloned()
    }

    pub(super) fn put(&mut self, path: PathBuf, col_idx: usize, zone_map: Option<ZoneMap>) {
        // Bounded: entries are only removed when a segment is deleted, so
        // long-lived tables with heavy compaction churn would grow this
        // map without limit.
        let key = (path, col_idx);
        if !self.entries.contains_key(&key) {
            self.order.push_back(key.clone());
            if self.entries.len() >= CAPACITY {
                let target = CAPACITY / 2;
                while self.entries.len() > target {
                    let Some(oldest) = self.order.pop_front() else {
                        break;
                    };
                    self.entries.remove(&oldest);
                }
            }
        }
        self.entries.insert(key, zone_map);
    }

    pub(super) fn invalidate(&mut self, path: &Path) {
        self.entries.retain(|(p, _), _| p != path);
        self.order.retain(|(p, _)| p != path);
    }

    pub(super) fn invalidate_under(&mut self, dir: &Path) {
        self.entries.retain(|(p, _), _| !p.starts_with(dir));
        self.order.retain(|(p, _)| !p.starts_with(dir));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn map(min: i64) -> Option<ZoneMap> {
        Some(ZoneMap {
            min_value: crate::types::scalar::ScalarValue::Int64(Some(min)),
            max_value: crate::types::scalar::ScalarValue::Int64(Some(min + 10)),
            null_count: 0,
            row_count: 1,
        })
    }

    #[test]
    fn eviction_removes_only_the_oldest_half() {
        let mut cache = ClusterMetaCache::new();
        let paths: Vec<PathBuf> = (0..CAPACITY)
            .map(|i| PathBuf::from(format!("seg_{i}.dat")))
            .collect();
        for path in &paths {
            cache.put(path.clone(), 0, map(1));
        }
        assert_eq!(cache.entries.len(), CAPACITY);

        // One more entry: the oldest HALF must go, the newest half stays.
        cache.put(PathBuf::from("seg_new.dat"), 0, map(2));
        assert_eq!(
            cache.entries.len(),
            CAPACITY / 2 + 1,
            "eviction keeps the recent half instead of clearing everything"
        );
        assert!(
            cache.get(&paths[0], 0).is_none(),
            "the oldest entry was evicted"
        );
        assert!(
            cache.get(&paths[CAPACITY - 1], 0).is_some(),
            "the newest entries stay warm across the eviction cycle"
        );
        assert!(cache.get(Path::new("seg_new.dat"), 0).is_some());
    }

    #[test]
    fn invalidate_prunes_order_too() {
        let mut cache = ClusterMetaCache::new();
        let path = PathBuf::from("seg_1.dat");
        cache.put(path.clone(), 0, map(1));
        cache.invalidate(&path);
        cache.put(path.clone(), 0, map(2));
        assert_eq!(cache.order.len(), 1, "stale order entries do not accumulate");
        assert!(cache.get(&path, 0).is_some());
    }
}
