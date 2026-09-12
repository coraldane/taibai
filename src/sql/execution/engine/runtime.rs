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
    ) -> crate::common::error::Result<()> {
        let resident_mark = self.hot_resident_mark_for_append(db, table, table_meta, batch);
        self.hot_store_mgr
            .try_append(db, table, table_meta, batch)?;
        self.apply_hot_resident_mark(db, table, resident_mark);
        Ok(())
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

        let Some(partition_col_idx) =
            table_meta.get_partition_column_index_cached().or_else(|| {
                table_meta
                    .columns
                    .iter()
                    .position(|col| col.name.eq_ignore_ascii_case(&table_meta.partition_key))
            })
        else {
            return HotResidentMark::None;
        };

        let mut partitions = std::collections::HashSet::new();
        for row_idx in 0..batch.num_rows() {
            let Some(value) = batch.scalar(row_idx, partition_col_idx) else {
                continue;
            };
            let Some(key) = Self::hot_partition_key_for_value(table_meta, &value) else {
                continue;
            };
            partitions.insert(key);
        }
        if partitions.is_empty() {
            return HotResidentMark::None;
        }

        let markable = partitions
            .into_iter()
            .filter(|partition| !Self::partition_has_segment(&table_dir, partition))
            .collect::<Vec<_>>();
        if markable.is_empty() {
            HotResidentMark::None
        } else {
            HotResidentMark::Partitions(markable)
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

    fn hot_partition_key_for_value(
        table_meta: &crate::storage::catalog::schema::TableMeta,
        value: &crate::types::scalar::ScalarValue,
    ) -> Option<String> {
        match table_meta.partition_type {
            crate::storage::catalog::schema::PartitionType::Value => value.to_text(),
            crate::storage::catalog::schema::PartitionType::Day => {
                crate::storage::partition::compute_partition_key(value, PartitionStrategy::Day).ok()
            }
            crate::storage::catalog::schema::PartitionType::Month => {
                crate::storage::partition::compute_partition_key(value, PartitionStrategy::Month)
                    .ok()
            }
            crate::storage::catalog::schema::PartitionType::Year => {
                crate::storage::partition::compute_partition_key(value, PartitionStrategy::Year)
                    .ok()
            }
            _ => None,
        }
    }

    fn table_has_any_segment(table_dir: &std::path::Path) -> bool {
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
        let prefix = store_rows.saturating_sub(live);
        if prefix > 0 {
            let dropped = self.hot_store_mgr.compact_prefix(db, table, prefix);
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
    ) -> Option<crate::storage::catalog::schema::TableMeta> {
        let catalogs = self.catalogs.read().unwrap();
        catalogs.get(db).and_then(|c| c.get_table(table).cloned())
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
        let today = chrono::Local::now().date_naive().to_string();
        {
            let mut catalogs = self.catalogs.write().unwrap();
            if let Some(catalog) = catalogs.get_mut(db) {
                if let Some(meta) = catalog.get_table_mut(table) {
                    meta.hot_last_close_date = Some(today);
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

                let mut mtimed: Vec<(std::time::SystemTime, std::path::PathBuf)> = Vec::new();
                for seg in &segments {
                    if let Ok(meta) = std::fs::metadata(seg) {
                        if let Ok(mtime) = meta.modified() {
                            mtimed.push((mtime, seg.clone()));
                        }
                    }
                }
                mtimed.sort_by(|a, b| b.cmp(a));
                if mtimed.is_empty() {
                    continue;
                }

                let mut loaded_rows: u64 = 0;
                let mut loaded_segments: HashSet<std::path::PathBuf> = HashSet::new();
                let mut failed_segments: HashSet<std::path::PathBuf> = HashSet::new();
                let mut budget_exhausted = false;
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
                            if partition_segments
                                .iter()
                                .all(|seg| loaded_segments.contains(seg))
                                && partition_segments
                                    .iter()
                                    .all(|seg| !failed_segments.contains(seg))
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
