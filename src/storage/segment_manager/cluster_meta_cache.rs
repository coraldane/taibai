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
#[path = "cluster_meta_cache_test.rs"]
mod tests;
