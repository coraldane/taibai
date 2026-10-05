//! String interning and stable pool identifiers.

use super::*;

/// Direct-mapped cache slot for the intern pool: `(hash, id)` with `hash == 0`
/// meaning empty. Hashes are forced nonzero, so a slot match is either the
/// interned value or a (verified) 64-bit collision.
#[derive(Clone, Copy)]
pub(super) struct PoolCacheSlot {
    pub(super) hash: u64,
    pub(super) id: u32,
    pub(super) _pad: u32,
}

pub(super) const POOL_CACHE_SLOTS: usize = 4096;

/// Interning pool for variable-length key values. A distinct string is stored
/// once and addressed by a `u32` id, so that building the inverted index never
/// re-clones a string for every row (only on first sight of a new value).
///
/// This mirrors q's enumerated `` `sym$ `` symbols: the index compares interned
/// ids, never string bytes. A direct-mapped hash cache sits in front of the
/// map so repeated symbols within a batch skip the full lookup entirely.
///
/// Released ids are tombstoned (`reverse[i] = None`) rather than removed:
/// ids are positions in `reverse`, and postings in surviving chunk indexes
/// embed them. Tombstoned ids are gone from `forward`, so a later intern of
/// the same string allocates a fresh id — safe because no live postings
/// reference a released one.
pub struct StringPool {
    pub(super) forward: HashMap<String, u32, FastBuild>,
    pub(super) reverse: Vec<Option<String>>,
    pub(super) cache: Vec<PoolCacheSlot>,
}

impl StringPool {
    pub fn new() -> Self {
        Self {
            forward: HashMap::with_hasher(FastBuild::default()),
            reverse: Vec::new(),
            cache: vec![
                PoolCacheSlot {
                    hash: 0,
                    id: 0,
                    _pad: 0
                };
                POOL_CACHE_SLOTS
            ],
        }
    }

    #[inline]
    pub(super) fn cached(&self, s: &str) -> Option<(usize, PoolCacheSlot)> {
        let hash = FastHasher::hash_str(s);
        let idx = (hash as usize) & (POOL_CACHE_SLOTS - 1);
        let slot = self.cache[idx];
        if slot.hash == hash
            && self
                .reverse
                .get(slot.id as usize)
                .and_then(|entry| entry.as_deref())
                == Some(s)
        {
            Some((idx, slot))
        } else {
            None
        }
    }

    /// Insert-or-return for a string value. Borrows `s` (no allocation on a
    /// cache hit).
    pub fn intern(&mut self, s: &str) -> u32 {
        if let Some((_idx, slot)) = self.cached(s) {
            return slot.id;
        }
        let id = match self.forward.get(s) {
            Some(&id) => id,
            None => {
                let id = self.reverse.len() as u32;
                self.reverse.push(Some(s.to_string()));
                self.forward.insert(s.to_string(), id);
                id
            }
        };
        let hash = FastHasher::hash_str(s);
        let idx = (hash as usize) & (POOL_CACHE_SLOTS - 1);
        self.cache[idx] = PoolCacheSlot { hash, id, _pad: 0 };
        id
    }

    /// Immutable lookup used on the read/query path. `None` means the value has
    /// never been appended, hence there can be no matching rows.
    pub fn lookup(&self, s: &str) -> Option<u32> {
        match self.cached(s) {
            Some((_idx, slot)) => Some(slot.id),
            None => self.forward.get(s).copied(),
        }
    }

    /// Drop every id outside `live`: removes its string from `forward` and
    /// tombstones its `reverse` slot. Called after a prefix compaction so the
    /// pool tracks the rows actually resident instead of growing without
    /// bound on high-cardinality string keys. The direct-mapped cache is
    /// cleared wholesale — its (hash, id) slots may name released ids.
    pub fn retain_ids(&mut self, live: &HashSet<u32>) {
        for (id, slot) in self.reverse.iter_mut().enumerate() {
            if slot.is_some() && !live.contains(&(id as u32)) {
                if let Some(s) = slot.take() {
                    self.forward.remove(s.as_str());
                }
            }
        }
        for slot in self.cache.iter_mut() {
            slot.hash = 0;
            slot.id = 0;
        }
    }

    pub fn len(&self) -> usize {
        self.reverse.len()
    }

    pub fn is_empty(&self) -> bool {
        self.reverse.is_empty()
    }
}

impl Default for StringPool {
    fn default() -> Self {
        Self::new()
    }
}
