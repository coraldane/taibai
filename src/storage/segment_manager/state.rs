use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU32, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use crate::storage::segment_manager::active_writer::ActiveSegmentWriter;

pub(super) struct TableSegmentState {
    pub(super) seq: AtomicU32,
    /// Per-table zstd level override (`COMPRESSION_LEVEL`), refreshed by DDL
    /// and the engine write paths. `0` means unset — writes fall back to the
    /// global `bulk_compression_level()`.
    pub(super) compression_level_override: AtomicI32,
    pub(super) partition_seq: Mutex<HashMap<String, u32>>,
    pub(super) active_segments: Mutex<HashMap<String, ActiveSegmentWriter>>,
    pub(super) mutation_lock: Mutex<()>,
    /// Per-partition mutation locks. Partition-scoped writers (memtable
    /// INSERT, single-partition bulk append, memtable flush/seal) take only
    /// their partition's mutex; table-wide mutation ops take the wide gate
    /// (`with_table_mutation_lock`), which freezes this registry and locks
    /// every partition mutex, so the two levels stay mutually exclusive.
    /// Lock order: `mutation_lock` → `partition_locks` (write) → partition
    /// mutexes in sorted key order. Partition writers take one partition
    /// mutex and must never re-enter `mutation_lock` or this registry.
    pub(super) partition_locks: RwLock<HashMap<String, Arc<Mutex<()>>>>,
    /// Set while a table-wide mutation operation holds the wide gate.
    /// Partition writers re-check it after acquiring their mutex and retry
    /// until it clears, so the registry write guard only needs to be held
    /// briefly for the partition snapshot.
    pub(super) partition_gate: AtomicBool,
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
            mutation_lock: Mutex::new(()),
            partition_locks: RwLock::new(HashMap::new()),
            partition_gate: AtomicBool::new(false),
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
}
