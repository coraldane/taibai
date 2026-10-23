//! Store registry, feed accounting and rebuild lifecycle.

use super::*;

/// Registry of resident hot stores, keyed by `(db, table)`.
pub struct HotStoreManager {
    pub(super) stores: RwLock<HashMap<(String, String), Arc<HotStore>>>,
    pub(super) invalidated: RwLock<HashSet<(String, String)>>,
    /// Per-table feed accounting for the backpressure reclaim: `in_flight`
    /// counts rows appended to the store whose memtable staging has not
    /// completed yet (they must never be treated as persisted prefix), and
    /// `epoch` is bumped whenever a reclaim/invalidation shortens the store
    /// so stale residency marks are not applied afterwards.
    pub(super) feed_accounting: Mutex<HashMap<(String, String), FeedAccounting>>,
    pub(super) memory_limit_bytes: u64,
}

#[derive(Default)]
pub(super) struct FeedAccounting {
    pub(super) in_flight: u64,
    pub(super) epoch: u64,
}

impl HotStoreManager {
    pub fn new() -> Self {
        Self::with_memory_limit_bytes(8 * 1024 * 1024 * 1024)
    }

    pub fn with_memory_limit_bytes(memory_limit_bytes: u64) -> Self {
        Self {
            stores: RwLock::new(HashMap::new()),
            invalidated: RwLock::new(HashSet::new()),
            feed_accounting: Mutex::new(HashMap::new()),
            memory_limit_bytes,
        }
    }

    /// Open a hot-store feed: accounts the batch as in flight until the
    /// caller's memtable staging completes (see `HotFeedGuard`) and returns
    /// the current feed epoch for stale-mark detection.
    pub fn begin_feed(&self, db: &str, table: &str) -> u64 {
        let mut accounting = self.feed_accounting.lock().unwrap();
        let state = accounting
            .entry((db.to_string(), table.to_string()))
            .or_default();
        state.in_flight = state.in_flight.saturating_add(1);
        state.epoch
    }

    /// Close a hot-store feed opened by [`Self::begin_feed`].
    pub fn end_feed(&self, db: &str, table: &str) {
        let mut accounting = self.feed_accounting.lock().unwrap();
        if let Some(state) = accounting.get_mut(&(db.to_string(), table.to_string())) {
            state.in_flight = state.in_flight.saturating_sub(1);
        }
    }

    /// Rows currently between hot-store append and memtable staging.
    pub fn in_flight(&self, db: &str, table: &str) -> u64 {
        self.feed_accounting
            .lock()
            .unwrap()
            .get(&(db.to_string(), table.to_string()))
            .map(|state| state.in_flight)
            .unwrap_or(0)
    }

    /// Current feed epoch; bumped by every reclaim/invalidation below.
    pub fn feed_epoch(&self, db: &str, table: &str) -> u64 {
        self.feed_accounting
            .lock()
            .unwrap()
            .get(&(db.to_string(), table.to_string()))
            .map(|state| state.epoch)
            .unwrap_or(0)
    }

    pub(super) fn bump_feed_epoch(&self, db: &str, table: &str) {
        let mut accounting = self.feed_accounting.lock().unwrap();
        accounting
            .entry((db.to_string(), table.to_string()))
            .or_default()
            .epoch += 1;
    }

    pub fn get(&self, db: &str, table: &str) -> Option<Arc<HotStore>> {
        let stores = self.stores.read().unwrap();
        stores.get(&(db.to_string(), table.to_string())).cloned()
    }

    /// Return the hot store for a table, creating it lazily if the table is
    /// eligible (i.e. has an indexable leading cluster-key column). Returns
    /// `None` for ineligible tables.
    pub fn get_or_create_for_table(
        &self,
        db: &str,
        table: &str,
        table_meta: &TableMeta,
    ) -> Option<Arc<HotStore>> {
        let table_key = (db.to_string(), table.to_string());
        if self.invalidated.read().unwrap().contains(&table_key) {
            return None;
        }
        let key_idx = resolve_key_column(table_meta)?;
        {
            let stores = self.stores.read().unwrap();
            if let Some(existing) = stores.get(&table_key) {
                return Some(Arc::clone(existing));
            }
        }
        let mut stores = self.stores.write().unwrap();
        if let Some(existing) = stores.get(&table_key) {
            return Some(Arc::clone(existing));
        }
        let schema = Schema::new(table_meta.columns.clone());
        let partition_idx = table_meta.get_partition_column_index_cached();
        let store = Arc::new(HotStore::new(
            schema,
            key_idx,
            partition_idx,
            self.memory_limit_bytes,
        ));
        stores.insert(table_key, Arc::clone(&store));
        Some(store)
    }

    /// Feed a batch into the resident store. No-op for ineligible tables.
    pub fn try_append(
        &self,
        db: &str,
        table: &str,
        table_meta: &TableMeta,
        batch: &RecordBatch,
    ) -> Result<()> {
        let Some(store) = self.get_or_create_for_table(db, table, table_meta) else {
            return Ok(());
        };
        store.append(batch)
    }

    /// Reclaim the persisted prefix of one table's resident store (backpressure).
    pub fn compact_prefix(&self, db: &str, table: &str, n: u64) -> u64 {
        self.bump_feed_epoch(db, table);
        let stores = self.stores.read().unwrap();
        stores
            .get(&(db.to_string(), table.to_string()))
            .map(|s| s.compact_prefix(n))
            .unwrap_or(0)
    }

    pub fn soft_limit_exceeded_for(&self, db: &str, table: &str) -> bool {
        let stores = self.stores.read().unwrap();
        stores
            .get(&(db.to_string(), table.to_string()))
            .map(|s| s.soft_limit_exceeded())
            .unwrap_or(false)
    }

    pub fn remove(&self, db: &str, table: &str) {
        let table_key = (db.to_string(), table.to_string());
        let mut stores = self.stores.write().unwrap();
        stores.remove(&table_key);
        self.invalidated.write().unwrap().remove(&table_key);
    }

    /// Per-store accounting for `SHOW MEMORY`: one `((db, table), bytes)`
    /// pair per resident store, plus the store count. Uses the same
    /// estimate the append backpressure enforces.
    pub fn memory_snapshot(&self) -> Vec<((String, String), u64)> {
        let stores = self.stores.read().unwrap();
        stores
            .iter()
            .map(|(key, store)| (key.clone(), store.estimated_memory_bytes()))
            .collect()
    }

    /// Tables whose store was dropped by [`Self::invalidate`] and await a
    /// rebuild (sticky markers, one map entry each).
    pub fn stale_table_count(&self) -> usize {
        self.invalidated.read().unwrap().len()
    }

    /// Drop an append-only hot store after an in-place mutation and mark the
    /// table STALE. A stale table is not served (`get` finds nothing, readers
    /// fall back to the generic scan) and not fed from a single batch:
    /// [`Self::rebuild_from_live_state`] — called by the engine at the next
    /// feed — is the only sound recreation, because the scan-side watermark
    /// derivation (`row_count - live_memtable_rows`) requires EVERY live
    /// memtable row to be in the store. Recreating from subsequent INSERTs
    /// alone would hide older live memtable rows behind an incomplete fast
    /// path, which is why this used to disable the table until restart.
    pub fn invalidate(&self, db: &str, table: &str) {
        let table_key = (db.to_string(), table.to_string());
        self.bump_feed_epoch(db, table);
        self.stores.write().unwrap().remove(&table_key);
        self.invalidated.write().unwrap().insert(table_key);
    }

    /// True while the table's store was dropped by [`Self::invalidate`] and
    /// awaits a full-live-state rebuild.
    pub fn is_stale(&self, db: &str, table: &str) -> bool {
        self.invalidated
            .read()
            .unwrap()
            .contains(&(db.to_string(), table.to_string()))
    }

    /// Rebuild a stale table's store from its FULL live state — every
    /// reader-visible memtable batch — clearing the stale marker. This is
    /// the only sound recreation after an in-place mutation (see
    /// [`Self::invalidate`]); it coalesces any number of invalidating
    /// mutations into one rebuild. Returns the fresh store, or `None` for
    /// ineligible tables. No-op (returns the existing store) when not stale.
    pub fn rebuild_from_live_state(
        &self,
        db: &str,
        table: &str,
        table_meta: &TableMeta,
        live_batches: &[std::sync::Arc<RecordBatch>],
    ) -> Result<Option<std::sync::Arc<HotStore>>> {
        let table_key = (db.to_string(), table.to_string());
        if !self.invalidated.read().unwrap().contains(&table_key) {
            return Ok(self.get(db, table));
        }
        let Some(key_idx) = resolve_key_column(table_meta) else {
            // Ineligible after all (e.g. the key column was dropped): stop
            // marking it stale so feeds stop attempting rebuilds.
            self.invalidated.write().unwrap().remove(&table_key);
            return Ok(None);
        };
        let schema = Schema::new(table_meta.columns.clone());
        let partition_idx = table_meta.get_partition_column_index_cached();
        let store = std::sync::Arc::new(HotStore::new(
            schema,
            key_idx,
            partition_idx,
            self.memory_limit_bytes,
        ));
        for batch in live_batches {
            if batch.num_rows() > 0 {
                store.append(batch)?;
            }
        }
        self.bump_feed_epoch(db, table);
        {
            let mut stores = self.stores.write().unwrap();
            stores.insert(table_key.clone(), std::sync::Arc::clone(&store));
        }
        self.invalidated.write().unwrap().remove(&table_key);
        Ok(Some(store))
    }

    pub fn clear_resident_coverage(&self, db: &str, table: &str) {
        self.bump_feed_epoch(db, table);
        if let Some(store) = self.get(db, table) {
            store.clear_resident_coverage();
        }
    }

    pub fn resident_coverage_snapshot(
        &self,
        db: &str,
        table: &str,
    ) -> Option<ResidentCoverageSnapshot> {
        self.get(db, table)
            .map(|store| store.resident_coverage_snapshot())
    }

    pub fn restore_resident_coverage(
        &self,
        db: &str,
        table: &str,
        snapshot: ResidentCoverageSnapshot,
    ) {
        if let Some(store) = self.get(db, table) {
            store.restore_resident_coverage(snapshot);
        }
    }
}

impl Default for HotStoreManager {
    fn default() -> Self {
        Self::new()
    }
}
