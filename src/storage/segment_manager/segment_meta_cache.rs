use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::storage::segment::meta::SegmentMetaSnapshot;

pub(super) struct SegmentMetaCache {
    capacity: usize,
    entries: HashMap<PathBuf, Arc<SegmentMetaSnapshot>>,
    order: VecDeque<PathBuf>,
}

impl SegmentMetaCache {
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            entries: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    pub(super) fn get(&mut self, path: &Path) -> Option<Arc<SegmentMetaSnapshot>> {
        let key = path.to_path_buf();
        let meta = self.entries.get(&key).cloned()?;
        self.touch(&key);
        Some(meta)
    }

    pub(super) fn put(
        &mut self,
        path: PathBuf,
        meta: SegmentMetaSnapshot,
    ) -> Arc<SegmentMetaSnapshot> {
        let meta = Arc::new(meta);
        if self
            .entries
            .insert(path.clone(), Arc::clone(&meta))
            .is_none()
        {
            self.order.push_back(path);
        }
        self.evict_if_needed();
        meta
    }

    pub(super) fn invalidate(&mut self, path: &Path) {
        let key = path.to_path_buf();
        self.entries.remove(&key);
        if let Some(pos) = self.order.iter().position(|p| p == &key) {
            self.order.remove(pos);
        }
    }

    pub(super) fn invalidate_under(&mut self, dir: &Path) {
        let keys = self
            .entries
            .keys()
            .filter(|path| path.starts_with(dir))
            .cloned()
            .collect::<Vec<_>>();
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
        while self.entries.len() > self.capacity {
            if let Some(oldest) = self.order.pop_front() {
                self.entries.remove(&oldest);
            } else {
                break;
            }
        }
    }
}
