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
/// Released ids are tombstoned (`reverse[i] = None`) rather than reused:
/// postings in surviving chunk indexes embed ids, so an id must never come
/// back naming a different string. Tombstoned ids are gone from `forward`, so
/// a later intern of the same string allocates a fresh id.
///
/// Ids are `base + position in reverse`. After a release, the leading run of
/// tombstones is trimmed off and `base` advances past it, so a long-running
/// pool over high-cardinality keys keeps memory proportional to the resident
/// window instead of one slot per string ever seen.
pub struct StringPool {
    pub(super) forward: HashMap<String, u32, FastBuild>,
    pub(super) reverse: Vec<Option<String>>,
    /// Id of `reverse[0]`; every id below it has been released.
    pub(super) base: u32,
    pub(super) cache: Vec<PoolCacheSlot>,
}

impl StringPool {
    pub fn new() -> Self {
        Self {
            forward: HashMap::with_hasher(FastBuild::default()),
            reverse: Vec::new(),
            base: 0,
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
    fn entry(&self, id: u32) -> Option<&str> {
        let pos = id.checked_sub(self.base)? as usize;
        self.reverse.get(pos).and_then(|entry| entry.as_deref())
    }

    #[inline]
    pub(super) fn cached(&self, s: &str) -> Option<(usize, PoolCacheSlot)> {
        let hash = FastHasher::hash_str(s);
        let idx = (hash as usize) & (POOL_CACHE_SLOTS - 1);
        let slot = self.cache[idx];
        if slot.hash == hash && self.entry(slot.id) == Some(s) {
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
                // Ids are never reused; wrapping would silently alias a new
                // string onto ids still embedded in live postings.
                let next = self.base as u64 + self.reverse.len() as u64;
                let id = u32::try_from(next)
                    .expect("string pool id space exhausted (u32)");
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
    /// tombstones its `reverse` slot, then trims the leading tombstone run so
    /// the backing vector actually shrinks. Called after a prefix compaction
    /// so the pool tracks the rows actually resident instead of growing
    /// without bound on high-cardinality string keys. The direct-mapped cache
    /// is cleared wholesale — its (hash, id) slots may name released ids.
    pub fn retain_ids(&mut self, live: &HashSet<u32>) {
        let base = self.base;
        for (pos, slot) in self.reverse.iter_mut().enumerate() {
            let id = base + pos as u32;
            if slot.is_some() && !live.contains(&id) {
                if let Some(s) = slot.take() {
                    self.forward.remove(s.as_str());
                }
            }
        }
        let dead_prefix = self
            .reverse
            .iter()
            .position(|slot| slot.is_some())
            .unwrap_or(self.reverse.len());
        if dead_prefix > 0 {
            self.reverse.drain(..dead_prefix);
            self.base += dead_prefix as u32;
            if self.reverse.capacity() > 2 * self.reverse.len().max(64) {
                self.reverse.shrink_to_fit();
            }
        }
        if self.forward.capacity() > 2 * self.forward.len().max(64) {
            self.forward.shrink_to_fit();
        }
        for slot in self.cache.iter_mut() {
            slot.hash = 0;
            slot.id = 0;
        }
    }

    /// Number of resident slots (live strings plus interior tombstones).
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
