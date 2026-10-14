use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, RwLock};
use std::time::Duration;

use crate::common::error::{Result, TaibaiError};
use crate::storage::segment_manager::active_writer::ActiveSegmentWriter;

/// Mutation mutex with a FIFO admission queue.
///
/// A bare `std::sync::Mutex` grants `try_lock` to whichever thread polls it
/// while the mutex happens to be free, so under sustained contention a
/// steady stream of newcomers can push an older waiter past its timeout.
/// The wrapper keeps the plain mutex and its `MutexGuard` but adds a ticket
/// queue: a waiter registers before it starts polling, and only the oldest
/// registered waiter polls `try_lock`. Later arrivals join the queue instead
/// of polling, so they cannot overtake the head; abandoned waiters (cancel,
/// timeout) remove their ticket so the successor takes over. The unlocked
/// fast path is only taken while the queue is empty, which keeps the
/// uncontended case at a single `try_lock` and stops fresh arrivals from
/// barging ahead of registered waiters.
pub(crate) struct FairMutex {
    inner: Mutex<()>,
    /// Tickets of waiters currently queued, in arrival order. A ticket is
    /// removed when its waiter acquires the mutex or abandons the wait.
    queue: Mutex<BTreeSet<u64>>,
    next_ticket: AtomicU64,
}

impl FairMutex {
    pub(super) fn new() -> Self {
        Self {
            inner: Mutex::new(()),
            queue: Mutex::new(BTreeSet::new()),
            next_ticket: AtomicU64::new(0),
        }
    }

    pub(super) fn queue_empty(&self) -> bool {
        self.queue
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_empty()
    }

    pub(super) fn enter_queue(&self) -> QueuedTicket<'_> {
        let ticket = self.next_ticket.fetch_add(1, Ordering::Relaxed);
        self.queue
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(ticket);
        QueuedTicket {
            fair: self,
            ticket,
            active: true,
        }
    }

    fn leave_queue(&self, ticket: u64) {
        self.queue
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&ticket);
    }

    pub(super) fn is_queue_head(&self, queued: &QueuedTicket<'_>) -> bool {
        self.queue.lock().unwrap_or_else(|e| e.into_inner()).first() == Some(&queued.ticket)
    }

    pub(super) fn try_lock(&self) -> std::sync::TryLockResult<MutexGuard<'_, ()>> {
        self.inner.try_lock()
    }

    /// Blocking hold for tests that stage contention. Production waiters
    /// must go through the `acquire_*` helpers so they queue fairly; this
    /// bypasses the ticket queue on purpose and is compiled out of release
    /// builds to keep that path test-only.
    #[cfg(test)]
    pub(crate) fn lock_for_test(&self) -> MutexGuard<'_, ()> {
        self.inner.lock().unwrap()
    }

    /// Try-once admission for the lock-free mutation attempts: `None` means
    /// busy (the caller reports "not now" and retries later); a poisoned
    /// lock still panics so the failure stays loud instead of being
    /// silently skipped. May run ahead of queued waiters when the mutex
    /// happens to be free; try-once callers never wait, so they cannot
    /// starve the queue.
    pub(super) fn try_lock_or_none(&self, what: &str) -> Option<MutexGuard<'_, ()>> {
        match self.inner.try_lock() {
            Ok(guard) => Some(guard),
            Err(std::sync::TryLockError::WouldBlock) => None,
            Err(std::sync::TryLockError::Poisoned(poisoned)) => {
                panic!("{} poisoned: {}", what, poisoned)
            }
        }
    }
}

/// A registered place in a [`FairMutex`] queue. Dropping it removes the
/// ticket, so a cancelled, timed-out, or panicking waiter cannot strand the
/// waiters queued behind it; `release` hands the head position over before
/// the acquired guard is returned.
pub(super) struct QueuedTicket<'a> {
    fair: &'a FairMutex,
    ticket: u64,
    active: bool,
}

impl QueuedTicket<'_> {
    pub(super) fn is_head(&self) -> bool {
        self.fair.is_queue_head(self)
    }

    pub(super) fn release(&mut self) {
        if self.active {
            self.fair.leave_queue(self.ticket);
            self.active = false;
        }
    }
}

impl Drop for QueuedTicket<'_> {
    fn drop(&mut self) {
        self.release();
    }
}

pub(super) struct TableSegmentState {
    pub(super) seq: AtomicU32,
    /// Per-table zstd level override (`COMPRESSION_LEVEL`), refreshed by DDL
    /// and the engine write paths. `0` means unset — writes fall back to the
    /// global `bulk_compression_level()`.
    pub(super) compression_level_override: AtomicI32,
    pub(super) partition_seq: Mutex<HashMap<String, u32>>,
    pub(super) active_segments: Mutex<HashMap<String, ActiveSegmentWriter>>,
    pub(super) mutation_lock: FairMutex,
    /// Per-partition mutation locks. Partition-scoped writers (memtable
    /// INSERT, single-partition bulk append, memtable flush/seal) take only
    /// their partition's mutex; table-wide mutation ops take the wide gate
    /// (`with_table_mutation_lock`), which freezes this registry and locks
    /// every partition mutex, so the two levels stay mutually exclusive.
    /// Lock order: `mutation_lock` → `partition_locks` (write) → partition
    /// mutexes in sorted key order. Partition writers take one partition
    /// mutex and must never re-enter `mutation_lock` or this registry.
    pub(super) partition_locks: RwLock<HashMap<String, Arc<FairMutex>>>,
    /// Set while a table-wide mutation operation holds the wide gate.
    /// Partition writers re-check it after acquiring their mutex and retry
    /// until it clears, so the registry write guard only needs to be held
    /// briefly for the partition snapshot.
    pub(super) partition_gate: AtomicBool,
    /// Segments removed from `active_segments` whose seal (footer + fsync)
    /// still runs outside every lock. Registered atomically with the removal
    /// while the partition mutex is held, so a wide gate that has collected
    /// all partition mutexes can wait for the registry to drain and then see
    /// a fully quiesced table. Readers/discovery must skip these paths until
    /// the seal publishes the footer.
    pub(super) sealing_segments: Mutex<Vec<(String, PathBuf)>>,
    pub(super) sealing_condvar: Condvar,
    pub(super) flush_visibility: RwLock<()>,
    pub(super) retired: AtomicBool,
}

impl TableSegmentState {
    pub(super) fn new(initial_seq: u32) -> Self {
        Self::with_partition_seq(initial_seq, HashMap::new())
    }

    pub(super) fn with_partition_seq(
        initial_seq: u32,
        partition_seq: HashMap<String, u32>,
    ) -> Self {
        Self {
            seq: AtomicU32::new(initial_seq),
            compression_level_override: AtomicI32::new(0),
            partition_seq: Mutex::new(partition_seq),
            active_segments: Mutex::new(HashMap::new()),
            mutation_lock: FairMutex::new(),
            partition_locks: RwLock::new(HashMap::new()),
            partition_gate: AtomicBool::new(false),
            sealing_segments: Mutex::new(Vec::new()),
            sealing_condvar: Condvar::new(),
            flush_visibility: RwLock::new(()),
            retired: AtomicBool::new(false),
        }
    }

    /// Per-table `COMPRESSION_LEVEL` override; `None` while the table follows
    /// the global configured level.
    pub(super) fn compression_level_override(&self) -> Option<i32> {
        let level = self.compression_level_override.load(Ordering::Relaxed);
        (level != 0).then_some(level)
    }

    pub(super) fn set_compression_level_override(&self, level: Option<i32>) {
        self.compression_level_override
            .store(level.unwrap_or(0), Ordering::Relaxed);
    }

    /// Record that `path` is being sealed outside the locks. Must be called
    /// while the partition mutex is still held, together with removing the
    /// segment from `active_segments`.
    pub(super) fn register_sealing_segment(&self, partition: &str, path: PathBuf) {
        self.sealing_segments
            .lock()
            .unwrap()
            .push((partition.to_string(), path));
    }

    /// Mark the seal of `path` as finished and wake any wide gate waiting for
    /// the registry to drain. Idempotent.
    pub(super) fn finish_sealing_segment(&self, path: &Path) {
        let mut sealing = self.sealing_segments.lock().unwrap();
        sealing.retain(|(_, sealing_path)| sealing_path != path);
        drop(sealing);
        self.sealing_condvar.notify_all();
    }

    /// Block until no seal is in flight, bounded by `timeout` so a stalled
    /// disk surfaces as a timeout error instead of an indefinite hang.
    pub(super) fn wait_sealing_segments_drained(&self, timeout: Duration) -> Result<()> {
        let mut sealing = self.sealing_segments.lock().unwrap();
        if sealing.is_empty() {
            return Ok(());
        }
        let deadline = std::time::Instant::now()
            .checked_add(timeout)
            .unwrap_or_else(std::time::Instant::now);
        while !sealing.is_empty() {
            let now = std::time::Instant::now();
            if now >= deadline {
                return Err(TaibaiError::ResourceBusy(format!(
                    "in-flight segment seals did not finish within {} ms; the sealing disk I/O is stalled or slow",
                    timeout.as_millis()
                )));
            }
            let (guard, _) = self
                .sealing_condvar
                .wait_timeout(sealing, deadline - now)
                .unwrap();
            sealing = guard;
        }
        Ok(())
    }

    /// Paths currently being sealed outside the locks; discovery and
    /// mutable-reader lookups must treat them as not yet published.
    pub(super) fn sealing_paths(&self) -> HashSet<PathBuf> {
        self.sealing_segments
            .lock()
            .unwrap()
            .iter()
            .map(|(_, path)| path.clone())
            .collect()
    }

    /// Cheap non-blocking probe used by the try-lock wide-gate variants: a
    /// table with an in-flight seal falls back to the blocking path, which
    /// drains the registry and keeps the gate's quiescence guarantee.
    pub(super) fn has_sealing_segments(&self) -> bool {
        !self.sealing_segments.lock().unwrap().is_empty()
    }
}
