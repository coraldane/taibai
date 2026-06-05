use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use lru::LruCache;

use crate::common::error::Result;
use crate::storage::metrics::CacheMetrics;
use crate::storage::segment::SegmentReader;

const DEFAULT_CACHE_CAPACITY: usize = 64;

pub struct SegmentReaderCache {
    cache: Mutex<LruCache<PathBuf, Arc<SegmentReader>>>,
    metrics: CacheMetrics,
    capacity: usize,
}

impl SegmentReaderCache {
    pub fn new(capacity: usize) -> Self {
        let cap = NonZeroUsize::new(capacity.max(1)).unwrap();
        Self {
            cache: Mutex::new(LruCache::new(cap)),
            metrics: CacheMetrics::new(),
            capacity: capacity.max(1),
        }
    }

    pub fn with_default_capacity() -> Self {
        Self::new(DEFAULT_CACHE_CAPACITY)
    }

    pub fn get(&self, path: &Path) -> Option<Arc<SegmentReader>> {
        let mut cache = self.cache.lock().unwrap();
        if let Some(reader) = cache.get(&path.to_path_buf()) {
            self.metrics.record_hit();
            return Some(Arc::clone(reader));
        }
        self.metrics.record_miss();
        None
    }

    pub fn put(&self, path: &Path, reader: SegmentReader) -> Arc<SegmentReader> {
        let arc_reader = Arc::new(reader);
        let mut cache = self.cache.lock().unwrap();
        let key = path.to_path_buf();
        if cache.put(key.clone(), Arc::clone(&arc_reader)).is_some() {
            self.metrics.record_eviction();
        }
        arc_reader
    }

    pub fn get_or_insert(&self, path: &Path, f: impl FnOnce() -> Result<SegmentReader>) -> Result<Arc<SegmentReader>> {
        if let Some(reader) = self.get(path) {
            return Ok(reader);
        }
        let reader = f()?;
        Ok(self.put(path, reader))
    }

    pub fn invalidate(&self, path: &Path) {
        let mut cache = self.cache.lock().unwrap();
        cache.pop(&path.to_path_buf());
    }

    pub fn clear(&self) {
        let mut cache = self.cache.lock().unwrap();
        cache.clear();
    }

    pub fn len(&self) -> usize {
        self.cache.lock().unwrap().len()
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn metrics(&self) -> &CacheMetrics {
        &self.metrics
    }

    pub fn hit_rate(&self) -> f64 {
        self.metrics.hit_rate()
    }

    pub fn hits(&self) -> u64 {
        self.metrics.hits()
    }

    pub fn misses(&self) -> u64 {
        self.metrics.misses()
    }

    pub fn evictions(&self) -> u64 {
        self.metrics.evictions()
    }
}

impl Default for SegmentReaderCache {
    fn default() -> Self {
        Self::with_default_capacity()
    }
}