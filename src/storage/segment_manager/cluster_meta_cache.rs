use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::storage::partition::pruning::ZoneMap;

#[derive(Default)]
pub(super) struct ClusterMetaCache {
    entries: HashMap<(PathBuf, usize), Option<ZoneMap>>,
}

impl ClusterMetaCache {
    pub(super) fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    pub(super) fn get(&self, path: &Path, col_idx: usize) -> Option<Option<ZoneMap>> {
        self.entries.get(&(path.to_path_buf(), col_idx)).cloned()
    }

    pub(super) fn put(&mut self, path: PathBuf, col_idx: usize, zone_map: Option<ZoneMap>) {
        // Bounded: entries are only removed when a segment is deleted, so
        // long-lived tables with heavy compaction churn would grow this
        // map without limit.
        if self.entries.len() >= 16384 {
            self.entries.clear();
        }
        self.entries.insert((path, col_idx), zone_map);
    }

    pub(super) fn invalidate(&mut self, path: &Path) {
        self.entries.retain(|(p, _), _| p != path);
    }

    pub(super) fn invalidate_under(&mut self, dir: &Path) {
        self.entries.retain(|(p, _), _| !p.starts_with(dir));
    }
}
