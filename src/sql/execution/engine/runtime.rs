/// Held for the duration of one hot-store feed: from the append until the
/// batch's memtable staging completes. Dropping it releases the in-flight
/// accounting; the captured epoch lets `feed_hot_store` detect that a
/// concurrent reclaim invalidated the residency mark it computed.
pub struct HotFeedGuard<'a> {
    mgr: &'a crate::storage::core::hot_store::HotStoreManager,
    db: String,
    table: String,
    epoch: u64,
}

impl HotFeedGuard<'_> {
    pub fn epoch(&self) -> u64 {
        self.epoch
    }
}

impl Drop for HotFeedGuard<'_> {
    fn drop(&mut self) {
        self.mgr.end_feed(&self.db, &self.table);
    }
}

impl Engine {
    pub(crate) fn performance_baselines(
        &self,
    ) -> &Arc<crate::common::performance_baseline::PerformanceBaselineStore> {
        &self.performance_baselines
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    pub fn segment_mgr(&self) -> &Arc<SegmentManager> {
        &self.segment_mgr
    }

    pub fn batch_writer(&self) -> &BatchWriter {
        &self.batch_writer
    }

    /// Change-notification hub backing the SUBSCRIBE protocol messages. The
    /// write path reaches it through `BatchWriter` (transaction commits) and
    /// the insert executor (autocommit inserts); connections reach it through
    /// the request handler.
    pub fn pubsub(&self) -> &Arc<crate::pubsub::PubSubHub> {
        &self.pubsub
    }

    pub fn memtable_manager(&self) -> &Arc<MemTableManager> {
        self.batch_writer.memtable_manager()
    }

    pub fn hot_store_manager(&self) -> &Arc<crate::storage::core::hot_store::HotStoreManager> {
        &self.hot_store_mgr
    }

    pub fn feed_hot_store(
        &self,
        db: &str,
        table: &str,
        table_meta: &crate::storage::catalog::schema::TableMeta,
        batch: &crate::types::batch::RecordBatch,
        feed: &HotFeedGuard<'_>,
    ) -> crate::common::error::Result<()> {
        let resident_mark = self.hot_resident_mark_for_append(db, table, table_meta, batch);
        self.hot_store_mgr
            .try_append(db, table, table_meta, batch)?;
        // A concurrent backpressure reclaim may have compacted the store
        // between the mark computation and the append (feed epoch bump):
        // applying the now-stale positive residency mark would let the
        // resident fast path answer from the shortened store.
        if self.hot_store_mgr.feed_epoch(db, table) == feed.epoch() {
            self.apply_hot_resident_mark(db, table, resident_mark);
        }
        Ok(())
    }

    /// Account a hot-store feed for the whole write: the guard must be held
    /// from the append until the batch's memtable staging completes, so a
    /// concurrent [`Self::reclaim_hot_store_pressure`] never mistakes the
    /// in-flight rows for persisted prefix.
    pub fn begin_hot_feed(&self, db: &str, table: &str) -> HotFeedGuard<'_> {
        let epoch = self.hot_store_mgr.begin_feed(db, table);
        HotFeedGuard {
            mgr: &self.hot_store_mgr,
            db: db.to_string(),
            table: table.to_string(),
            epoch,
        }
    }

    fn hot_resident_mark_for_append(
        &self,
        db: &str,
        table: &str,
        table_meta: &crate::storage::catalog::schema::TableMeta,
        batch: &crate::types::batch::RecordBatch,
    ) -> HotResidentMark {
        if crate::storage::core::hot_store::resolve_key_column(table_meta).is_none()
            || batch.num_rows() == 0
        {
            return HotResidentMark::None;
        }

        let table_dir = self.segment_mgr.table_dir(db, table);
        if !table_meta.is_partitioned() {
            return if !Self::table_has_any_segment(&table_dir) {
                HotResidentMark::FullTable
            } else {
                HotResidentMark::None
            };
        }

        let partitions = Self::batch_partition_keys(table_meta, batch);
        if partitions.is_empty() {
            return HotResidentMark::None;
        }

        // Consult the positive segment cache before hitting the filesystem:
        // once a partition is known to own a segment, it never needs marking
        // resident again, and skip-list decisions must stay conservative.
        let markable = {
            let mut cache = self.hot_partition_segments.lock().unwrap();
            let known = cache.entry(table_dir.clone()).or_default();
            partitions
                .into_iter()
                .filter(|partition| {
                    if known.contains(partition.as_str()) {
                        return false;
                    }
                    if Self::partition_has_segment(&table_dir, partition) {
                        known.insert(partition.clone());
                        return false;
                    }
                    true
                })
                .collect::<Vec<_>>()
        };
        if markable.is_empty() {
            HotResidentMark::None
        } else {
            HotResidentMark::Partitions(markable)
        }
    }


    /// Distinct partition keys of a batch (empty for non-partitioned tables
    /// or when no key can be derived). Typed, memoized key derivation
    /// straight from column storage; the scalar-based
    /// `hot_partition_key_for_value` is the semantic reference. NULL rows and
    /// unsupported partition types are skipped.
    pub fn batch_partition_keys(
        table_meta: &crate::storage::catalog::schema::TableMeta,
        batch: &crate::types::batch::RecordBatch,
    ) -> Vec<String> {
        if !table_meta.is_partitioned() {
            return Vec::new();
        }
        let Some(partition_col_idx) =
            table_meta.get_partition_column_index_cached().or_else(|| {
                table_meta
                    .columns
                    .iter()
                    .position(|col| col.name.eq_ignore_ascii_case(&table_meta.partition_key))
            })
        else {
            return Vec::new();
        };
        use crate::storage::catalog::schema::PartitionType;
        use crate::storage::partition::partition_function::{
            PartitionKeyKind, PartitionKeysAt,
        };
        let partition_column = batch.column(partition_col_idx);
        // Derive the kind through `partition_strategy()` so every scheme
        // (Hash/Mod included) gets per-partition mirror accounting, matching
        // the write path's `partition_keys_computer`.
        let kind = match table_meta.partition_type {
            PartitionType::Value => Some(PartitionKeyKind::ValueText),
            PartitionType::None => None,
            _ => Some(PartitionKeyKind::Strategy(table_meta.partition_strategy())),
        };
        let Some((kind, partition_column)) = kind.zip(partition_column) else {
            return Vec::new();
        };
        let mut keys = PartitionKeysAt::new(kind, partition_column);
        let mut partitions = std::collections::HashSet::new();
        for row_idx in 0..batch.num_rows() {
            if partition_column.bitmap().is_null(row_idx) {
                continue;
            }
            let key = keys.key_at(row_idx);
            if matches!(
                kind,
                PartitionKeyKind::Strategy(
                    crate::storage::partition::partition_function::PartitionFunction::Day,
                ) | PartitionKeyKind::Strategy(
                    crate::storage::partition::partition_function::PartitionFunction::Month,
                ) | PartitionKeyKind::Strategy(
                    crate::storage::partition::partition_function::PartitionFunction::Year,
                )
            ) && key.is_empty()
            {
                // Temporal derivation failed for this value; the scalar path
                // returned None here.
                continue;
            }
            partitions.insert(key.to_string());
        }
        partitions.into_iter().collect()
    }

    /// Mirror accounting for a batch written outside the regular auto-commit
    /// INSERT path (bulk / relaxed direct writes). `hot_fed` must reflect
    /// whether the batch was actually mirrored into the hot store.
    pub fn note_mirror_batch(
        &self,
        db: &str,
        table: &str,
        table_meta: &crate::storage::catalog::schema::TableMeta,
        batch: &crate::types::batch::RecordBatch,
        hot_fed: bool,
    ) {
        if hot_fed || batch.num_rows() == 0 {
            return;
        }
        if crate::storage::core::hot_store::resolve_key_column(table_meta).is_none() {
            // Table has no hot store; nothing to account for.
            return;
        }
        let mt_mgr = self.batch_writer.memtable_manager();
        if table_meta.is_partitioned() {
            for partition in Self::batch_partition_keys(table_meta, batch) {
                mt_mgr.note_hot_unfed_rows(db, table, &partition, batch.num_rows() as u64);
            }
        } else {
            mt_mgr.note_hot_unfed_rows(db, table, "", batch.num_rows() as u64);
        }
    }

    fn apply_hot_resident_mark(&self, db: &str, table: &str, mark: HotResidentMark) {
        let Some(store) = self.hot_store_mgr.get(db, table) else {
            return;
        };
        match mark {
            HotResidentMark::None => {}
            HotResidentMark::FullTable => store.mark_full_resident(),
            HotResidentMark::Partitions(partitions) => {
                for partition in partitions {
                    store.mark_partition_resident(partition);
                }
            }
        }
    }

    /// Drop the cached partition-segment knowledge for one table directory.
    pub fn clear_hot_partition_segments(&self, table_dir: &std::path::Path) {
        self.hot_partition_segments
            .lock()
            .unwrap()
            .remove(table_dir);
    }

    pub(crate) fn table_has_any_segment(table_dir: &std::path::Path) -> bool {
        SegmentManager::discover_segments(table_dir)
            .map(|segments| !segments.is_empty())
            .unwrap_or(true)
    }

    fn partition_has_segment(table_dir: &std::path::Path, partition_key: &str) -> bool {
        if !table_dir.exists() {
            return false;
        }
        let Ok(entries) = std::fs::read_dir(table_dir) else {
            return true;
        };
        for entry in entries.flatten() {
            let Some(name) = entry.file_name().to_str().map(str::to_string) else {
                continue;
            };
            if parse_partition_key(&name).as_deref() == Some(partition_key) {
                return true;
            }
        }
        false
    }

    /// Reclaim resident memory under hot-store backpressure (design 鎼?.1 hard
    /// valve): drop the already-persisted append-prefix from the store, then
    /// checkpoint-flush (seal active segments + clear WAL). Persisted rows stay
    /// queryable via the segment scan; only the in-memory mirror is released.
    pub fn reclaim_hot_store_pressure(&self, db: &str, table: &str) -> bool {
        let store_rows = self
            .hot_store_mgr
            .get(db, table)
            .map(|s| s.row_count())
            .unwrap_or(0);
        let live = self
            .batch_writer
            .memtable_manager()
            .get_by_table(db, table)
            .iter()
            .map(|mt| mt.row_count())
            .sum::<u64>();
        let prefix = store_rows
            .saturating_sub(live)
            // Rows appended to the store but not staged into the memtables
            // yet are neither persisted nor counted in `live`: subtracting
            // them keeps the persisted-prefix estimate stable while their
            // write is in progress.
            .saturating_sub(self.hot_store_mgr.in_flight(db, table))
            // Rows sitting in the active segment without a published footer
            // are not queryable yet; dropping their mirror here would hide
            // them from every query until the next footer publish.
            .saturating_sub(self.segment_mgr.unpublished_active_rows(db, table));
        if prefix > 0 {
            let dropped = self.hot_store_mgr.compact_prefix(db, table, prefix);
            // Rows below the new front are gone from the store: retract
            // residency and flip the mirror accounting so resident-partition
            // scans fall back to the segment scan instead of under-counting.
            self.hot_store_mgr.clear_resident_coverage(db, table);
            self.batch_writer
                .memtable_manager()
                .mark_hot_unfed_all_partitions(db, table);
            tracing::warn!(
                db,
                table,
                dropped,
                "hot store backpressure: reclaimed persisted prefix"
            );
        }
        if let Err(e) = self.flush_table_buffers(db, table) {
            tracing::warn!(db, table, error = %e, "hot store backpressure: checkpoint flush failed");
        }
        prefix > 0
    }

    pub fn auth_manager(&self) -> &Arc<AuthManager> {
        &self.auth_manager
    }

    /// Drop the hot store of a table together with its mirror accounting.
    /// Every row-mutation path that invalidates the store (UPDATE/DELETE,
    /// tail recovery) must go through here so the resident fast path and the
    /// mirror bookkeeping stay consistent.
    pub fn invalidate_hot_store(&self, db: &str, table: &str) {
        self.hot_store_mgr.invalidate(db, table);
        self.batch_writer
            .memtable_manager()
            .clear_hot_unfed(db, table);
    }

    pub fn auto_increment_manager(&self) -> &Arc<AutoIncrementManager> {
        &self.auto_increment_mgr
    }

    pub fn index_manager(&self) -> &Arc<IndexManager> {
        &self.index_manager
    }

    pub fn commit_session_txn(&self, session: &mut Session) -> Result<()> {
        if !session.in_txn() {
            return Err(TaibaiError::Sql("no active transaction".into()));
        }
        let txn_id = session.active_txn_id();
        let had_writes = session
            .active_txn
            .as_ref()
            .map(|t| t.had_writes)
            .unwrap_or(false);

        if had_writes {
            self.batch_writer().commit_txn(txn_id)?;
        }

        session.commit_txn();
        Ok(())
    }

    pub fn get_table_meta(
        &self,
        db: &str,
        table: &str,
    ) -> Option<std::sync::Arc<crate::storage::catalog::schema::TableMeta>> {
        let catalogs = self.catalogs.read().unwrap();
        catalogs
            .get(db)
            .and_then(|c| c.get_table_shared(table).cloned())
    }

    /// Run one pass of partition-level TTL purge across all candidate
    /// tables (temporal partitions whose TTL column is the partition
    /// column). Invoked periodically by the close-of-day scheduler; see
    /// `ttl_purge`.
    pub fn run_ttl_partition_purge(&self) -> Result<u64> {
        crate::sql::execution::ttl_purge::purge_expired_partitions(self)
    }

    pub fn database_exists(&self, db: &str) -> bool {
        self.catalogs.read().unwrap().contains_key(db)
    }

    pub fn begin_bulk_import(&self) -> usize {
        let active = self.bulk_mode_sessions.fetch_add(1, Ordering::AcqRel) + 1;
        if active == 1 {
            self.index_manager.pause_unique_index_compaction();
        }
        active
    }

    pub fn end_bulk_import(&self) -> usize {
        self.bulk_mode_sessions
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                Some(count.saturating_sub(1))
            })
            .unwrap_or(0)
            .saturating_sub(1)
    }

    pub fn is_bulk_mode(&self) -> bool {
        self.bulk_mode_sessions.load(Ordering::Acquire) > 0
    }

    pub fn bulk_mode_session_count(&self) -> usize {
        self.bulk_mode_sessions.load(Ordering::Acquire)
    }

    pub fn flush_bulk_dirty_tables(&self) -> Result<u64> {
        let tables = self
            .bulk_dirty_tables
            .lock()
            .unwrap()
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        let mut total = 0u64;
        for (db, table) in tables {
            total = total.saturating_add(self.flush_table_buffers(&db, &table)?);
            self.bulk_dirty_tables
                .lock()
                .unwrap()
                .remove(&(db, table));
        }
        if !self.is_bulk_mode() {
            self.index_manager.resume_unique_index_compaction();
        }
        Ok(total)
    }

    fn begin_operation(&self) -> EngineOperationGuard<'_> {
        self.in_flight_operations.fetch_add(1, Ordering::AcqRel);
        EngineOperationGuard { engine: self }
    }

    pub fn alloc_txn_id(&self) -> u64 {
        self.segment_mgr.alloc_txn_id()
    }

    pub fn flush_table_buffers(&self, db: &str, table: &str) -> Result<u64> {
        self.batch_writer.flush_table(db, table)?;

        let rows = self.segment_mgr.flush_table_active(db, table)?;

        {
            let mut catalogs = self.catalogs.write().unwrap();
            if let Some(catalog) = catalogs.get_mut(db) {
                let meta_dir = self.data_dir.join("meta");
                let _ = catalog.save(&meta_dir);
            }
        }
        {
            let mut state = self.catalog_save_state.lock().unwrap();
            state.pending_insert_rows = 0;
            state.last_save_at = Instant::now();
        }

        Ok(rows)
    }

    pub fn flush_partition_buffers(
        &self,
        db: &str,
        table: &str,
        partition_key: &str,
    ) -> Result<u64> {
        self.batch_writer
            .flush_partition(db, table, partition_key)?;
        let rows = self
            .segment_mgr
            .flush_partition_active(db, table, partition_key)?;
        let mut catalogs = self.catalogs.write().unwrap();
        if let Some(catalog) = catalogs.get_mut(db) {
            catalog.save(&self.data_dir.join("meta"))?;
        }
        Ok(rows)
    }

    pub fn flush_all_buffers(&self) -> Result<u64> {
        self.batch_writer.flush_all()?;

        self.segment_mgr.flush_all_active()?;

        {
            let mut catalogs = self.catalogs.write().unwrap();
            let meta_dir = self.data_dir.join("meta");
            for (_, catalog) in catalogs.iter_mut() {
                let _ = catalog.save(&meta_dir);
            }
        }
        {
            let mut state = self.catalog_save_state.lock().unwrap();
            state.pending_insert_rows = 0;
            state.last_save_at = Instant::now();
        }

        Ok(0)
    }

    /// Graceful process checkpoint. Memtables backed by a durable tail stay
    /// resident (recovered from their tails on restart), so routine restarts
    /// do not cut tiny row groups; only untailed memtables are materialized.
    pub fn flush_shutdown_buffers(&self) -> Result<u64> {
        self.batch_writer.flush_untailed_pending()?;

        self.segment_mgr.flush_all_active()?;
        {
            let mut catalogs = self.catalogs.write().unwrap();
            let meta_dir = self.data_dir.join("meta");
            for (_, catalog) in catalogs.iter_mut() {
                let _ = catalog.save(&meta_dir);
            }
        }
        Ok(0)
    }

    pub fn close_of_day_flush_table(&self, db: &str, table: &str) -> Result<()> {
        tracing::info!(
            db,
            table,
            "close-of-day: flushing table resident data to history"
        );
        let table_meta = {
            let catalogs = self.catalogs.read().unwrap();
            let catalog = catalogs
                .get(db)
                .ok_or_else(|| TaibaiError::Sql(format!("database '{}' does not exist", db)))?;
            catalog
                .get_table(table)
                .ok_or_else(|| {
                    TaibaiError::Sql(format!("table '{}.{}' does not exist", db, table))
                })?
                .clone()
        };

        // Capture the write set before flush/seal retires its active writers.
        // A table-level OPTIMIZE here used to rewrite every historical
        // partition on every close, even when only today's partition changed.
        let mut dirty_partitions = std::collections::BTreeSet::new();
        dirty_partitions.extend(self.segment_mgr.active_partition_keys(db, table));
        dirty_partitions.extend(
            self.batch_writer
                .memtable_manager()
                .get_readable_by_table(db, table)
                .into_iter()
                .map(|memtable| memtable.partition_key().to_string()),
        );
        self.flush_table_buffers(db, table)?;
        // flush_table_buffers intentionally keeps small segments active during
        // normal operation. At close-of-day they must be sealed so compaction
        // sees the complete write set for each dirty partition.
        dirty_partitions.extend(self.segment_mgr.active_partition_keys(db, table));
        self.segment_mgr.seal_table_active(db, table)?;
        // Once flush succeeds, segments are the source of truth. Release the
        // in-memory mirror before OPTIMIZE so clustered compaction can use the
        // same memory budget to decode every source row group exactly once.
        // Queries remain correct and fall back to the still-live old segments
        // until the atomic replacement completes.
        self.hot_store_mgr.remove(db, table);
        self.batch_writer
            .memtable_manager()
            .remove_archived(db, table);
        tracing::info!(
            db,
            table,
            dirty_partitions = ?dirty_partitions,
            "close-of-day: resident state evicted before optimization"
        );
        let compactor = Compactor::new(Arc::clone(&self.segment_mgr), self.data_dir.clone());
        let mut stats = crate::storage::core::compactor::CompactStats::default();
        if table_meta.is_partitioned() {
            for partition_key in &dirty_partitions {
                let partition_stats =
                    compactor.optimize_partition(db, table, &table_meta, partition_key)?;
                stats.segments_read += partition_stats.segments_read;
                stats.segments_written += partition_stats.segments_written;
                stats.rows_written += partition_stats.rows_written;
                stats.rows_dropped += partition_stats.rows_dropped;
            }
        } else if !dirty_partitions.is_empty() {
            stats = compactor.optimize_table(db, table, &table_meta)?;
        }
        if stats.segments_written > 0 || stats.rows_dropped > 0 {
            let schema = Schema::new(table_meta.columns.clone());
            let rebuilt = self.segment_mgr.with_table_mutation_lock(db, table, || {
                super::dml_insert::rebuild_table_location_indexes(
                    self,
                    db,
                    table,
                    &table_meta,
                    &schema,
                )
            });
            if let Err(error) = rebuilt {
                tracing::warn!(
                    db,
                    table,
                    error = %error,
                    "close-of-day index auto-rebuild failed; falling back to snapshot invalidation"
                );
                let unique_index_names = table_meta
                    .unique_indexes
                    .iter()
                    .map(|index| index.name.clone())
                    .collect::<Vec<_>>();
                self.index_manager.invalidate_table_unique_index_snapshots(
                    db,
                    table,
                    &unique_index_names,
                );
            }
        }
        let now = chrono::Local::now();
        let today = now.date_naive().to_string();
        {
            let mut catalogs = self.catalogs.write().unwrap();
            if let Some(catalog) = catalogs.get_mut(db) {
                if let Some(meta) = catalog.get_table_mut(table) {
                    meta.hot_last_close_date = Some(today);
                    meta.hot_last_close_at = Some(now.timestamp_millis());
                }
                let meta_dir = self.data_dir.join("meta");
                catalog.save(&meta_dir)?;
            }
        }
        tracing::info!(
            db,
            table,
            rows_written = stats.rows_written,
            rows_dropped = stats.rows_dropped,
            partitions_optimized = dirty_partitions.len(),
            "close-of-day: table resident state evicted and optimized"
        );
        Ok(())
    }

    /// Rebuild resident hot stores from on-disk segments after a restart (design
    /// gap fix). Without this, already-flushed data would not be resident and
    /// point lookups would fall back to the segment scan until new writes refill
    /// the store. For each hot-eligible table we load the most recently written
    /// segments (up to a row budget) so the inverted index is warm again.
    /// Best-effort: unreadable segments are skipped.
    ///
    /// Close-aware: segments archived by a close-of-day flush are NOT
    /// reloaded. Only segments modified after the recorded close completion
    /// (`hot_last_close_at`) are eligible, because a continuously-running
    /// server would hold nothing older resident — the close evicted it. A
    /// table closed by a legacy binary that recorded only
    /// `hot_last_close_date` stays cold until its next close.
    pub fn warmup_hot_stores(&self) {
        use crate::storage::core::hot_store::resolve_key_column;
        use crate::storage::partition::parse_partition_key;
        use crate::storage::segment::delete_bitmap;
        use crate::storage::segment::reader::SegmentReader;
        use std::collections::{HashMap, HashSet};

        const WARMUP_ROW_BUDGET: u64 = 20_000_000;

        let catalogs = self.catalogs.read().unwrap();
        for (db, catalog) in catalogs.iter() {
            for (table, table_meta) in catalog.tables.iter() {
                if resolve_key_column(table_meta).is_none() {
                    continue;
                }
                let table_dir = self.segment_mgr.table_dir(db, table);
                if !table_dir.exists() {
                    continue;
                }
                let segments = match SegmentManager::discover_segments(&table_dir) {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::warn!(db, table, error = %e, "warmup: discover segments failed");
                        continue;
                    }
                };
                if segments.is_empty() {
                    continue;
                }

                // Close-aware recency cutoff. A close-of-day flush archives
                // the resident mirror and evicts it, so a continuously-running
                // server holds nothing for the table until new writes arrive.
                // Reloading every segment on restart would undo that memory
                // release, so only segments written after the recorded close
                // completion are eligible for the rebuild.
                let close_cutoff_ms: Option<i64> = if let Some(at) = table_meta.hot_last_close_at {
                    Some(at)
                } else if table_meta.hot_last_close_date.is_some() {
                    // Legacy catalog state: closed before this binary began
                    // recording the completion instant. The archived set
                    // cannot be separated from post-close writes, so keep the
                    // table cold; the next close records the cutoff.
                    tracing::info!(
                        db,
                        table,
                        last_close_date = ?table_meta.hot_last_close_date,
                        "warmup: table closed without a recorded close instant, skipping hot store rebuild"
                    );
                    continue;
                } else {
                    None
                };

                let mut mtimed: Vec<(std::time::SystemTime, std::path::PathBuf)> = Vec::new();
                for seg in &segments {
                    if let Ok(meta) = std::fs::metadata(seg) {
                        if let Ok(mtime) = meta.modified() {
                            if let Some(cutoff) = close_cutoff_ms {
                                let mtime_ms = mtime
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .map(|d| d.as_millis() as i64)
                                    .unwrap_or(i64::MAX);
                                if mtime_ms <= cutoff {
                                    continue;
                                }
                            }
                            mtimed.push((mtime, seg.clone()));
                        }
                    }
                }
                mtimed.sort_by(|a, b| b.cmp(a));
                if mtimed.is_empty() {
                    if close_cutoff_ms.is_some() {
                        tracing::info!(
                            db,
                            table,
                            "warmup: no segments written after the last close, hot store stays cold"
                        );
                    }
                    continue;
                }

                let mut loaded_rows: u64 = 0;
                let mut loaded_segments: HashSet<std::path::PathBuf> = HashSet::new();
                let mut failed_segments: HashSet<std::path::PathBuf> = HashSet::new();
                let mut budget_exhausted = false;
                let eligible: HashSet<std::path::PathBuf> =
                    mtimed.iter().map(|(_, path)| path.clone()).collect();
                for (_, seg_path) in &mtimed {
                    if loaded_rows >= WARMUP_ROW_BUDGET {
                        budget_exhausted = true;
                        break;
                    }
                    let reader = match SegmentReader::open(seg_path) {
                        Ok(r) => r,
                        Err(e) => {
                            tracing::warn!(path = %seg_path.display(), error = %e, "warmup: open segment failed");
                            failed_segments.insert(seg_path.clone());
                            continue;
                        }
                    };
                    let num_cols = reader.schema().num_columns();
                    let projection: Vec<usize> = (0..num_cols).collect();
                    let del_bitmap = reader.delete_bitmap();
                    let has_deletes = !del_bitmap.is_empty();
                    let mut segment_loaded = true;
                    for rg in 0..reader.num_row_groups() {
                        if loaded_rows >= WARMUP_ROW_BUDGET {
                            budget_exhausted = true;
                            segment_loaded = false;
                            break;
                        }
                        let batch = match reader.read_row_group_with_projection(rg, &projection) {
                            Ok(b) => b,
                            Err(e) => {
                                tracing::warn!(error = %e, "warmup: read row group failed");
                                segment_loaded = false;
                                continue;
                            }
                        };
                        let first_row_id = reader.row_group_start_row(rg);
                        let batch = if has_deletes {
                            match delete_bitmap::filter_batch(&batch, first_row_id, del_bitmap) {
                                Ok(b) => b,
                                Err(_) => batch,
                            }
                        } else {
                            batch
                        };
                        if batch.num_rows() == 0 {
                            continue;
                        }
                        loaded_rows = loaded_rows.saturating_add(batch.num_rows() as u64);
                        if let Err(e) = self.hot_store_mgr.try_append(db, table, table_meta, &batch)
                        {
                            tracing::warn!(db, table, error = %e, "warmup: feed hot store failed");
                            segment_loaded = false;
                        }
                    }
                    if segment_loaded {
                        loaded_segments.insert(seg_path.clone());
                    } else {
                        failed_segments.insert(seg_path.clone());
                    }
                }

                if let Some(store) = self.hot_store_mgr.get(db, table) {
                    if !budget_exhausted
                        && failed_segments.is_empty()
                        && loaded_segments.len() == segments.len()
                    {
                        store.mark_full_resident();
                    } else if table_meta.is_partitioned() {
                        let mut by_partition: HashMap<String, Vec<std::path::PathBuf>> =
                            HashMap::new();
                        for seg in &segments {
                            let Some(name) = seg.file_name().and_then(|name| name.to_str()) else {
                                continue;
                            };
                            let Some(partition_key) = parse_partition_key(name) else {
                                continue;
                            };
                            by_partition
                                .entry(partition_key)
                                .or_default()
                                .push(seg.clone());
                        }
                        for (partition_key, partition_segments) in by_partition {
                            // The store may answer a partition alone only when
                            // it holds the partition's complete row set: every
                            // segment must be inside the close-recency window
                            // and fully loaded. A partition with any archived
                            // (pre-cutoff) segment stays cold, matching a
                            // server that never restarted.
                            if partition_segments
                                .iter()
                                .all(|seg| {
                                    eligible.contains(seg)
                                        && loaded_segments.contains(seg)
                                        && !failed_segments.contains(seg)
                                })
                            {
                                store.mark_partition_resident(partition_key);
                            }
                        }
                    }
                }
                if loaded_rows > 0 {
                    tracing::info!(
                        db,
                        table,
                        loaded_rows,
                        "warmup: rebuilt resident hot store from segments"
                    );
                }
            }
        }
    }

    /// Graceful shutdown checkpoint.
    /// Physical writers are sealed, while restart-safe low-rate tails remain
    /// in memtable form so routine restarts do not create tiny segments.
    pub fn shutdown(&mut self) -> Result<()> {
        tracing::info!("engine shutdown: checkpointing resident data");
        if let Err(e) = self.flush_shutdown_buffers() {
            tracing::warn!(error = %e, "engine shutdown checkpoint failed");
        }
        self.batch_writer.stop();
        Ok(())
    }
}
