//! A byte-budgeted LRU map: the one container every metadata cache in the
//! segment manager is expected to use.
//!
//! # Why this exists
//!
//! The reader caches were byte-bounded (see `reader_cache.rs`,
//! `storage.mutable_reader_cache_mb`), but `SegmentMetaCache` counted
//! ENTRIES instead: `capacity = 1024` entries. When a sealed segment's
//! parsed footer grew to ~24 MiB (row-group metadata, zone maps and bloom
//! filters for a multi-GiB segment), the same configuration silently became
//! a 24 GiB resident set — with no byte metric anywhere to notice it, and no
//! eviction at all on an idle server because `evict_if_needed` only ran from
//! `put`.
//!
//! The rule this type enforces: **a cache is bounded in bytes, not in
//! entries**. Callers supply a per-entry cost; the container evicts LRU
//! entries until the total fits, and refuses an entry that is larger than
//! the entire budget rather than thrashing everything else out for it.
//!
//! The LRU order is a `VecDeque` with O(n) `touch`. That is deliberate: a
//! byte budget divided by a per-entry cost of hundreds of KiB to tens of MiB
//! yields tens to a few hundred live entries, so the scan is cheaper than
//! maintaining an intrusive list. Caches that are sized in the thousands of
//! tiny entries (readers) keep their own intrusive order for that reason.

use std::collections::{HashMap, VecDeque};
use std::hash::Hash;

struct Entry<V> {
    value: V,
    /// Bytes charged when the entry was inserted. Remembered per entry so a
    /// same-key replacement (or invalidation) releases exactly what it
    /// reserved, even if the new value has a different cost.
    cost_bytes: u64,
}

pub(super) struct ByteLru<K, V> {
    budget_bytes: u64,
    used_bytes: u64,
    entries: HashMap<K, Entry<V>>,
    /// LRU order, front = least recently used.
    order: VecDeque<K>,
}

impl<K: Eq + Hash + Clone, V> ByteLru<K, V> {
    pub(super) fn new(budget_bytes: u64) -> Self {
        Self {
            budget_bytes: budget_bytes.max(1),
            used_bytes: 0,
            entries: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    pub(super) fn get(&mut self, key: &K) -> Option<&V> {
        if !self.entries.contains_key(key) {
            return None;
        }
        self.touch(key);
        self.entries.get(key).map(|entry| &entry.value)
    }

    /// Store `value` under `key` charged `cost_bytes` against the budget.
    ///
    /// Returns `false` when the entry was refused because it alone exceeds
    /// the whole budget; the caller keeps ownership of `value` in that case
    /// (it is not inserted, and nothing else is evicted for it).
    pub(super) fn put(&mut self, key: K, value: V, cost_bytes: u64) -> bool {
        if cost_bytes > self.budget_bytes {
            return false;
        }
        match self.entries.get_mut(&key) {
            // Same-key replacement keeps the LRU position (callers treat a
            // re-put as a refresh, not an access) and swaps the charge.
            Some(entry) => {
                self.used_bytes -= entry.cost_bytes;
                entry.value = value;
                entry.cost_bytes = cost_bytes;
            }
            None => {
                self.entries
                    .insert(key.clone(), Entry { value, cost_bytes });
                self.order.push_back(key);
            }
        }
        self.used_bytes += cost_bytes;
        self.evict_if_needed();
        true
    }

    /// Drop `key`; returns whether it was present.
    pub(super) fn invalidate(&mut self, key: &K) -> bool {
        match self.entries.remove(key) {
            Some(entry) => {
                self.used_bytes -= entry.cost_bytes;
                if let Some(pos) = self.order.iter().position(|candidate| candidate == key) {
                    self.order.remove(pos);
                }
                true
            }
            None => false,
        }
    }

    /// Drop every entry whose key does not satisfy `keep` (directory-scoped
    /// invalidation: `retain(|k| !k.starts_with(dir))`).
    pub(super) fn retain<F: FnMut(&K) -> bool>(&mut self, mut keep: F) {
        let victims: Vec<K> = self
            .entries
            .keys()
            .filter(|key| !keep(key))
            .cloned()
            .collect();
        for key in victims {
            self.invalidate(&key);
        }
    }

    /// `(entries, charged bytes, budget bytes)` for `SHOW MEMORY`.
    pub(super) fn stats(&self) -> (usize, u64, u64) {
        (self.entries.len(), self.used_bytes, self.budget_bytes)
    }

    fn touch(&mut self, key: &K) {
        if let Some(pos) = self.order.iter().position(|candidate| candidate == key) {
            self.order.remove(pos);
        }
        self.order.push_back(key.clone());
    }

    fn evict_if_needed(&mut self) {
        while self.used_bytes > self.budget_bytes {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            if let Some(entry) = self.entries.remove(&oldest) {
                self.used_bytes -= entry.cost_bytes;
            }
        }
        // A burst of large entries can leave a HashMap allocated for far more
        // than it holds; hand the slack back rather than pinning it.
        if self.entries.capacity() > self.entries.len().max(16) * 4 {
            self.entries.shrink_to(self.entries.len() * 2);
            self.order.shrink_to(self.order.len().max(16) * 2);
        }
    }
}
