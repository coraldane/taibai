use std::collections::HashMap;
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

/// O(1) intrusive LRU order over paths: a slab-backed doubly-linked list
/// with a path→slot index. `touch`, `push`, `pop_front` and `remove` are all
/// constant time. This replaces the previous `VecDeque` whose `touch` and
/// `invalidate` scanned up to `capacity` `PathBuf`s per operation — on the
/// scan hot path, once per segment lookup.
struct LruOrder {
    index: HashMap<PathBuf, u32>,
    slots: Vec<Slot>,
    free: Vec<u32>,
    head: Option<u32>,
    tail: Option<u32>,
}

struct Slot {
    path: PathBuf,
    prev: Option<u32>,
    next: Option<u32>,
}

impl LruOrder {
    fn new() -> Self {
        Self {
            index: HashMap::new(),
            slots: Vec::new(),
            free: Vec::new(),
            head: None,
            tail: None,
        }
    }

    fn unlink(&mut self, id: u32) {
        let (prev, next) = {
            let slot = &self.slots[id as usize];
            (slot.prev, slot.next)
        };
        match prev {
            Some(prev) => self.slots[prev as usize].next = next,
            None => self.head = next,
        }
        match next {
            Some(next) => self.slots[next as usize].prev = prev,
            None => self.tail = prev,
        }
    }

    fn push_back(&mut self, path: PathBuf) {
        // A re-push of a live path is a touch (the old slot moves to the
        // back); callers otherwise remove before pushing a fresh key.
        if self.index.contains_key(&path) {
            self.touch(&path);
            return;
        }
        let id = match self.free.pop() {
            Some(id) => id,
            None => {
                self.slots.push(Slot {
                    path: path.clone(),
                    prev: None,
                    next: None,
                });
                let id = (self.slots.len() - 1) as u32;
                self.link_at_tail(id);
                self.index.insert(path, id);
                return;
            }
        };
        let slot = &mut self.slots[id as usize];
        slot.path = path.clone();
        self.link_at_tail(id);
        self.index.insert(path, id);
    }

    fn link_at_tail(&mut self, id: u32) {
        let old_tail = self.tail;
        self.slots[id as usize].prev = old_tail;
        self.slots[id as usize].next = None;
        match old_tail {
            Some(old_tail) => self.slots[old_tail as usize].next = Some(id),
            None => self.head = Some(id),
        }
        self.tail = Some(id);
    }

    fn touch(&mut self, path: &Path) {
        let Some(&id) = self.index.get(path) else {
            return;
        };
        if self.tail == Some(id) {
            return;
        }
        self.unlink(id);
        self.link_at_tail(id);
    }

    fn pop_front(&mut self) -> Option<PathBuf> {
        let head = self.head?;
        self.unlink(head);
        let path = self.slots[head as usize].path.clone();
        self.index.remove(&path);
        self.free.push(head);
        Some(path)
    }

    fn remove(&mut self, path: &Path) {
        let Some(id) = self.index.remove(path) else {
            return;
        };
        self.unlink(id);
        self.free.push(id);
    }

    fn shrink(&mut self) {
        self.index.shrink_to(self.index.len() * 2);
        self.free.shrink_to(self.free.len() * 2);
    }
}

pub(super) struct ReaderCache {
    budget_bytes: u64,
    used_bytes: u64,
    /// Cached reader plus the budget bytes charged for it. The charge is
    /// remembered per entry so `invalidate`/eviction can release exactly what
    /// was reserved, including after a same-path re-put with a new cost.
    entries: HashMap<PathBuf, (Arc<SegmentReader>, u64)>,
    order: LruOrder,
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
            order: LruOrder::new(),
        }
    }

    pub(super) fn get(&mut self, path: &Path) -> Option<Arc<SegmentReader>> {
        let (reader, _) = self.entries.get(path)?.clone();
        self.order.touch(path);
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
        match self
            .entries
            .insert(path.clone(), (Arc::clone(&reader), cost_bytes))
        {
            // Same-path re-put replaces the reader in place (keeping its LRU
            // position, as before) and must release the old entry's charge.
            Some((_, previous_cost)) => self.used_bytes -= previous_cost,
            None => self.order.push_back(path),
        }
        self.used_bytes += cost_bytes;
        self.evict_if_needed();
        reader
    }

    /// `(entries, charged heap bytes, budget bytes)` for `SHOW MEMORY`. The
    /// charge covers owned readers' heap copies; mmap readers are
    /// page-cache-backed and only their metadata is charged.
    pub(super) fn stats(&self) -> (usize, u64, u64) {
        (self.entries.len(), self.used_bytes, self.budget_bytes)
    }

    pub(super) fn invalidate(&mut self, path: &Path) {
        if let Some((_, cost)) = self.entries.remove(path) {
            self.used_bytes -= cost;
        }
        self.order.remove(path);
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

    fn evict_if_needed(&mut self) {
        while self.used_bytes > self.budget_bytes {
            if let Some(oldest) = self.order.pop_front() {
                if let Some((_, cost)) = self.entries.remove(&oldest) {
                    self.used_bytes -= cost;
                }
            } else {
                break;
            }
        }
        if self.entries.capacity() > self.entries.len().max(16) * 4 {
            self.entries.shrink_to(self.entries.len() * 2);
            self.order.shrink();
        }
    }
}

#[cfg(test)]
#[path = "reader_cache_test.rs"]
mod tests;
