use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::storage::segment::SegmentReader;

pub(super) struct ReaderCache {
    capacity: usize,
    entries: HashMap<PathBuf, Arc<SegmentReader>>,
    order: VecDeque<PathBuf>,
}

impl ReaderCache {
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            entries: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    pub(super) fn get(&mut self, path: &Path) -> Option<Arc<SegmentReader>> {
        let key = path.to_path_buf();
        let reader = self.entries.get(&key).cloned()?;
        self.touch(&key);
        Some(reader)
    }

    pub(super) fn put(&mut self, path: PathBuf, reader: SegmentReader) -> Arc<SegmentReader> {
        let reader = Arc::new(reader);
        if self
            .entries
            .insert(path.clone(), Arc::clone(&reader))
            .is_none()
        {
            self.order.push_back(path);
        }
        self.evict_if_needed();
        reader
    }

    pub(super) fn invalidate(&mut self, path: &Path) {
        let key = path.to_path_buf();
        self.entries.remove(&key);
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
        while self.entries.len() > self.capacity {
            if let Some(oldest) = self.order.pop_front() {
                self.entries.remove(&oldest);
            } else {
                break;
            }
        }
    }
}
