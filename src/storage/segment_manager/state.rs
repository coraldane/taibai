use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, RwLock};
use std::time::{Duration, Instant};

use crate::common::error::{Result, TaibaiError};
use crate::storage::segment_manager::active_writer::ActiveSegmentWriter;
use crate::storage::segment_manager::catalog::{CatalogEntryRecord, SegmentPlanMeta};

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

/// One maintenance-tracked segment path: its revision/retired state (the
/// Phase 1 protocol) plus the advisory plan metadata of the segment catalog.
/// `plan` is `None` while the segment is mutable, freshly published, or
/// invalidated — the next maintenance round rebuilds it from the footer.
pub(super) struct MaintenanceEntry {
    revision: u64,
    retired: bool,
    plan: Option<Arc<SegmentPlanMeta>>,
}

/// Minimum interval between catalog file writes; persistence is also gated on
/// the generation counter, so a burst of rounds coalesces into one write.
const CATALOG_PERSIST_MIN_INTERVAL: Duration = Duration::from_secs(1);

pub(super) struct TableSegmentState {
    pub(super) seq: AtomicU32,
    /// Per-table zstd level override (`COMPRESSION_LEVEL`), refreshed by DDL
    /// and the engine write paths. `0` means unset — writes fall back to the
    /// global `bulk_compression_level()`.
    pub(super) compression_level_override: AtomicI32,
    pub(super) partition_seq: Mutex<HashMap<String, u32>>,
    pub(super) active_segments: Mutex<HashMap<String, ActiveSegmentWriter>>,
    pub(super) maintenance_entries: Mutex<HashMap<PathBuf, MaintenanceEntry>>,
    next_maintenance_revision: AtomicU64,
    /// Bumped on every maintenance-entry content change (plan fill/clear,
    /// publish, retire, prune); persistence compares it against the last
    /// persisted generation.
    catalog_generation: AtomicU64,
    catalog_last_persisted: AtomicU64,
    catalog_last_persist_at: Mutex<Option<Instant>>,
    /// The on-disk catalog file was loaded (or found absent) for this state.
    catalog_loaded: AtomicBool,
    /// First maintenance round after startup must reconcile the catalog
    /// against the directory: files may have changed out-of-band while the
    /// process was down (crash window, replication apply, external tools).
    catalog_needs_reconcile: AtomicBool,
    catalog_rounds: AtomicU32,
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
            maintenance_entries: Mutex::new(HashMap::new()),
            next_maintenance_revision: AtomicU64::new(1),
            catalog_generation: AtomicU64::new(0),
            catalog_last_persisted: AtomicU64::new(0),
            catalog_last_persist_at: Mutex::new(None),
            catalog_loaded: AtomicBool::new(false),
            catalog_needs_reconcile: AtomicBool::new(true),
            catalog_rounds: AtomicU32::new(0),
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

    pub(super) fn maintenance_revision(&self, path: &Path) -> (u64, bool) {
        let mut entries = self.maintenance_entries.lock().unwrap();
        let entry = entries
            .entry(path.to_path_buf())
            .or_insert_with(|| MaintenanceEntry {
                revision: self
                    .next_maintenance_revision
                    .fetch_add(1, Ordering::Relaxed),
                retired: false,
                plan: None,
            });
        (entry.revision, entry.retired)
    }

    pub(super) fn maintenance_plan(&self, path: &Path) -> Option<Arc<SegmentPlanMeta>> {
        self.maintenance_entries
            .lock()
            .unwrap()
            .get(path)
            .and_then(|entry| entry.plan.clone())
    }

    /// Attach freshly captured plan metadata. Entries retired in the meantime
    /// keep their tombstone and stay planless.
    pub(super) fn set_maintenance_plan(&self, path: &Path, plan: Arc<SegmentPlanMeta>) {
        let mut entries = self.maintenance_entries.lock().unwrap();
        let entry = entries
            .entry(path.to_path_buf())
            .or_insert_with(|| MaintenanceEntry {
                revision: self
                    .next_maintenance_revision
                    .fetch_add(1, Ordering::Relaxed),
                retired: false,
                plan: None,
            });
        if !entry.retired {
            entry.plan = Some(plan);
            self.touch_catalog_generation();
        }
    }

    /// Seed an entry from the persisted catalog file. Never overwrites a live
    /// entry: in-process state wins over the advisory file.
    pub(super) fn seed_maintenance_plan(&self, path: PathBuf, plan: Arc<SegmentPlanMeta>) {
        let mut entries = self.maintenance_entries.lock().unwrap();
        if let std::collections::hash_map::Entry::Vacant(slot) = entries.entry(path) {
            slot.insert(MaintenanceEntry {
                revision: self
                    .next_maintenance_revision
                    .fetch_add(1, Ordering::Relaxed),
                retired: false,
                plan: Some(plan),
            });
            self.touch_catalog_generation();
        }
    }

    /// Drop the entry for a segment file that no longer exists on disk.
    pub(super) fn remove_maintenance_path(&self, path: &Path) {
        if self
            .maintenance_entries
            .lock()
            .unwrap()
            .remove(path)
            .is_some()
        {
            self.touch_catalog_generation();
        }
    }

    /// Reconcile diff: drop entries for paths the directory scan no longer
    /// sees. Retired entries stay until their plan lease prunes them.
    pub(super) fn retain_maintenance_paths(&self, keep: &HashSet<PathBuf>) {
        let mut entries = self.maintenance_entries.lock().unwrap();
        let before = entries.len();
        entries.retain(|path, entry| entry.retired || keep.contains(path));
        if entries.len() != before {
            drop(entries);
            self.touch_catalog_generation();
        }
    }

    /// Non-retired entry paths: the steady-state candidate list for a
    /// catalog-driven maintenance round.
    pub(super) fn maintenance_paths(&self) -> Vec<PathBuf> {
        self.maintenance_entries
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, entry)| !entry.retired)
            .map(|(path, _)| path.clone())
            .collect()
    }

    pub(super) fn catalog_records(&self) -> Vec<CatalogEntryRecord> {
        self.maintenance_entries
            .lock()
            .unwrap()
            .iter()
            .filter_map(|(path, entry)| {
                let plan = entry.plan.as_ref()?;
                (!entry.retired).then(|| {
                    CatalogEntryRecord::from_plan(
                        path.file_name()
                            .map(|name| name.to_string_lossy().into_owned())
                            .unwrap_or_default(),
                        plan,
                    )
                })
            })
            .collect()
    }

    /// Throttled persistence gate: `Some(generation)` when a write is due,
    /// carrying the generation to mark persisted after it succeeds.
    pub(super) fn catalog_persist_due(&self) -> Option<u64> {
        let generation = self.catalog_generation.load(Ordering::Relaxed);
        if generation == self.catalog_last_persisted.load(Ordering::Relaxed) {
            return None;
        }
        let mut last = self.catalog_last_persist_at.lock().unwrap();
        let due = last.is_none_or(|instant| instant.elapsed() >= CATALOG_PERSIST_MIN_INTERVAL);
        if due {
            *last = Some(Instant::now());
            return Some(generation);
        }
        None
    }

    pub(super) fn mark_catalog_persisted(&self, generation: u64) {
        self.catalog_last_persisted
            .store(generation, Ordering::Relaxed);
    }

    pub(super) fn take_catalog_reconcile_request(&self) -> bool {
        self.catalog_needs_reconcile.swap(false, Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(super) fn request_catalog_reconcile(&self) {
        self.catalog_needs_reconcile.store(true, Ordering::Relaxed);
    }

    pub(super) fn next_catalog_round(&self) -> u32 {
        self.catalog_rounds.fetch_add(1, Ordering::Relaxed)
    }

    pub(super) fn catalog_load_guard(&self) -> bool {
        !self.catalog_loaded.swap(true, Ordering::SeqCst)
    }

    fn touch_catalog_generation(&self) {
        self.catalog_generation.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn bump_maintenance_revision(&self, path: &Path) {
        let revision = self
            .next_maintenance_revision
            .fetch_add(1, Ordering::Relaxed);
        self.maintenance_entries
            .lock()
            .unwrap()
            .entry(path.to_path_buf())
            .and_modify(|entry| {
                entry.revision = revision;
                entry.plan = None;
            })
            .or_insert(MaintenanceEntry {
                revision,
                retired: false,
                plan: None,
            });
        self.touch_catalog_generation();
    }

    pub(super) fn publish_maintenance_segment(&self, path: &Path) {
        let revision = self
            .next_maintenance_revision
            .fetch_add(1, Ordering::Relaxed);
        self.maintenance_entries.lock().unwrap().insert(
            path.to_path_buf(),
            MaintenanceEntry {
                revision,
                retired: false,
                plan: None,
            },
        );
        self.touch_catalog_generation();
    }

    pub(super) fn retire_maintenance_segment(&self, path: &Path) {
        let revision = self
            .next_maintenance_revision
            .fetch_add(1, Ordering::Relaxed);
        self.maintenance_entries.lock().unwrap().insert(
            path.to_path_buf(),
            MaintenanceEntry {
                revision,
                retired: true,
                plan: None,
            },
        );
        self.touch_catalog_generation();
    }

    pub(super) fn prune_retired_maintenance_segments(&self) {
        let mut entries = self.maintenance_entries.lock().unwrap();
        let removed = entries.values().any(|entry| entry.retired);
        if removed {
            entries.retain(|_, entry| !entry.retired);
            drop(entries);
            self.touch_catalog_generation();
        }
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
