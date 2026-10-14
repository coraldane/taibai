use super::*;

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

/// C13: process-wide positive cache of non-partitioned table directories
/// known to own at least one segment file. "Has a segment" is monotone —
/// once a table owns a segment it keeps owning one while its table state
/// exists — so a cached entry short-circuits the `discover_segments`
/// directory listing that every non-partitioned HOT INSERT would otherwise
/// pay. Entries are dropped by [`Engine::clear_hot_partition_segments`] and
/// [`Engine::invalidate_hot_segment_hint`] when the table's data is removed
/// or moved (DROP / TRUNCATE / RENAME); a stale entry is only ever
/// conservative (it skips the FullTable resident mark, it never fakes one).
static HOT_TABLES_WITH_SEGMENTS: std::sync::OnceLock<Mutex<HashSet<PathBuf>>> =
    std::sync::OnceLock::new();

fn hot_tables_with_segments() -> &'static Mutex<HashSet<PathBuf>> {
    HOT_TABLES_WITH_SEGMENTS.get_or_init(|| Mutex::new(HashSet::new()))
}

impl Engine {
    pub(crate) fn performance_baselines(
        &self,
    ) -> &Arc<crate::common::performance_baseline::PerformanceBaselineStore> {
        &self.performance_baselines
    }

    /// Running-query registry backing `SHOW PROCESSLIST` and `KILL QUERY`.
    /// The request handler registers every heavy request here.
    pub fn query_registry(&self) -> Arc<crate::sql::execution::query_registry::QueryRegistry> {
        Arc::clone(&self.query_registry)
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

    /// Reliable-subscription manager backing the SUBSCRIBE_V2 protocol
    /// messages (resumable sequences, acks, named bookmarks, column
    /// filters). Mirrored through [`Self::pubsub`] on every committed insert.
    pub fn pubsub_manager(&self) -> &Arc<crate::storage::core::pubsub::PubSubManager> {
        &self.pubsub_manager
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
        // A dropped-and-stale mirror (an UPDATE/DELETE invalidated it) is
        // rebuilt from the FULL live state before this feed: recreating it
        // from this batch alone would hide older live memtable rows behind
        // the watermark derivation. Rebuilding here coalesces any number of
        // invalidating mutations into one rebuild; until then readers fall
        // back to the generic scan, which is correct.
        if self.hot_store_mgr.is_stale(db, table) {
            self.rebuild_stale_hot_store(db, table, table_meta);
        }
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

    /// Full-live-state rebuild of a stale hot store: every reader-visible
    /// memtable batch, plus closing the `hot_mirror_covers_from` gate — the
    /// rebuilt mirror holds no flushed-prefix rows, so any pre-invalidation
    /// coverage bound must stop answering. Best-effort: a failed rebuild
    /// leaves the table stale (generic-scan reads stay correct).
    fn rebuild_stale_hot_store(
        &self,
        db: &str,
        table: &str,
        table_meta: &crate::storage::catalog::schema::TableMeta,
    ) {
        let live_batches = self
            .memtable_manager()
            .get_readable_by_table(db, table)
            .into_iter()
            .filter(|mt| mt.is_visible_to(0))
            .filter_map(|mt| mt.read_snapshot_arc())
            .collect::<Vec<_>>();
        match self
            .hot_store_mgr
            .rebuild_from_live_state(db, table, table_meta, &live_batches)
        {
            Ok(_) => {
                let mut catalogs = self.catalogs.write().unwrap();
                if let Some(catalog) = catalogs.get_mut(db) {
                    if let Some(meta) = catalog.get_table_mut(table) {
                        meta.hot_mirror_covers_from = None;
                    }
                    let meta_dir = self.data_dir.join("meta");
                    if let Err(error) = catalog.save(&meta_dir) {
                        tracing::warn!(
                            db,
                            table,
                            error = %error,
                            "hot store rebuild: persisting cleared mirror coverage failed"
                        );
                    }
                }
            }
            Err(error) => tracing::warn!(
                db,
                table,
                error = %error,
                "hot store rebuild from live state failed; table stays stale (generic-scan reads remain correct)"
            ),
        }
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
            // C13: consult the positive "known to have a segment" cache
            // before the filesystem: the miss path is a full
            // `discover_segments` directory listing, paid by every INSERT
            // of a non-partitioned HOT table once segments exist.
            if hot_tables_with_segments()
                .lock()
                .unwrap()
                .contains(&table_dir)
            {
                return HotResidentMark::None;
            }
            if Self::table_has_any_segment(&table_dir) {
                hot_tables_with_segments()
                    .lock()
                    .unwrap()
                    .insert(table_dir.clone());
                return HotResidentMark::None;
            }
            return HotResidentMark::FullTable;
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
        use crate::storage::partition::partition_function::{PartitionKeyKind, PartitionKeysAt};
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

    pub(super) fn apply_hot_resident_mark(&self, db: &str, table: &str, mark: HotResidentMark) {
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
    /// Also drops the C13 table-level "known to have a segment" hint, so a
    /// same-name recreate re-probes the filesystem (DROP TABLE and the TTL
    /// purge land here).
    pub fn clear_hot_partition_segments(&self, table_dir: &std::path::Path) {
        self.hot_partition_segments
            .lock()
            .unwrap()
            .remove(table_dir);
        hot_tables_with_segments().lock().unwrap().remove(table_dir);
    }

    /// Drop the C13 "known to have a segment" hint for one table. Wire this
    /// wherever a table's segment files are removed or moved out from under
    /// its directory (TRUNCATE TABLE, RENAME TABLE) — paths that do not pass
    /// through [`Self::clear_hot_partition_segments`]. A missed call is only
    /// a lost FullTable fast path for the recreated table, never a wrong
    /// mark, so wiring is an optimization, not a correctness requirement.
    pub fn invalidate_hot_segment_hint(&self, db: &str, table: &str) {
        let table_dir = self.segment_mgr.table_dir(db, table);
        hot_tables_with_segments()
            .lock()
            .unwrap()
            .remove(&table_dir);
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
                .retract_hot_mirror_coverage(db, table);
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

    /// Standby replication apply: replace an in-memory catalog from its
    /// on-disk JSON (the snapshot was written by the applier).
    pub fn reload_catalog(&self, db: &str) -> Result<()> {
        let meta_dir = self.data_dir.join("meta");
        let catalog =
            crate::storage::catalog::schema::DatabaseCatalog::load_or_create(&meta_dir, db)?;
        self.catalogs
            .write()
            .unwrap()
            .insert(db.to_string(), catalog);
        Ok(())
    }

    /// Standby replication apply: DROP DATABASE.
    pub fn remove_catalog(&self, db: &str) {
        self.catalogs.write().unwrap().remove(db);
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
            // Deferred in-transaction DELETE/UPDATE must be durable (footer
            // rewrites, memtable tail rewrites) before the visibility flip
            // inside commit_txn exposes the transaction's new rows. Every
            // tombstone leg runs under the DEFERRED journal lifecycle with
            // the transaction's own memtables as the image watermark, so a
            // crash before COMMIT's tail fsync rolls the tombstones back at
            // startup instead of deleting rows whose images were lost.
            let materialization =
                crate::sql::execution::txn_dml::materialize_txn_mutations_at_commit(self, txn_id)?;
            if let Err(first_error) = self.batch_writer().commit_txn(txn_id) {
                // By the time commit_txn reports an error, the visibility flip
                // has usually already happened (the failure comes from the
                // durable-tail fsync wait) — and the flip clears each
                // memtable's txn_id, so retrying commit_txn(txn_id) would find
                // no memtables and report success with the rows still only in
                // memory. Re-drive durability for the committed memtables
                // directly instead: flushing them to segments persists the rows
                // even when the tail lost its frames. If that also fails, fail
                // with an explicit "fate ambiguous" error — the changes are
                // visible and ROLLBACK cannot undo them.
                std::thread::sleep(std::time::Duration::from_millis(50));
                if let Err(retry_error) = self.batch_writer().redrive_commit_durability() {
                    tracing::error!(
                        txn_id,
                        first_error = %first_error,
                        retry_error = %retry_error,
                        "COMMIT durable-tail wait failed and durability redrive failed; transaction fate ambiguous"
                    );
                    // The journals and guards stay on disk: startup recovery
                    // judges each leg against the watermark and keeps only
                    // the tombstones whose images provably survived.
                    return Err(TaibaiError::Storage(format!(
                        "COMMIT applied but the durable-tail wait failed twice ({retry_error}); \
                         the transaction's changes are visible and ROLLBACK cannot undo them — \
                         verify the data with SELECT"
                    )));
                }
                // The redrive persisted the images as segments: that also
                // satisfies every watermark, so fall through to finish
                // materialization like the success path.
            }
            crate::sql::execution::txn_dml::finish_materialization(self, materialization);
        }

        // The published index entries are authoritative now; drop the
        // rollback record.
        self.index_manager().forget_txn_published_entries(txn_id);
        session.commit_txn();
        Ok(())
    }

    /// Roll back the session's open transaction: drop deferred DML plans,
    /// roll the batch writer back, retract the unique/PK index entries the
    /// transaction published at statement time, and clear the session's
    /// transaction state. This is the single rollback protocol — the SQL
    /// ROLLBACK statement, the wire ROLLBACK message, and the disconnect
    /// path all go through it, so the cleanup steps cannot drift apart.
    ///
    /// On failure the session's transaction state is left intact so the
    /// caller can retry or report; callers that cannot propagate (disconnect)
    /// log instead.
    pub fn rollback_session_txn(&self, session: &mut Session) -> Result<u64> {
        if !session.in_txn() {
            return Err(TaibaiError::Sql("no active transaction".into()));
        }
        let txn_id = session.active_txn_id();
        // Retract the catalog row_count of rows INSERT statements already
        // counted at statement time. Without this, a rolled-back
        // transaction's rows stayed in the stats forever (only DELETE's
        // COMMIT path ever decremented). The retractions are memory-only
        // inside the loop; one forced save per touched database afterwards
        // persists all of them — a rollback is rare and can retract a large
        // count, while saving per table would re-serialize the whole catalog
        // JSON once per table for the same bytes.
        let mut retracted_dbs: Vec<String> = Vec::new();
        for (db, table, rows) in self.txn_mutations().take_inserted_rows_by_table(txn_id) {
            if let Err(error) = self.record_inserted_rows(&db, &table, -(rows as i64), false, false)
            {
                tracing::warn!(
                    db,
                    table,
                    rows,
                    error = %error,
                    "failed to retract rolled-back rows from catalog row_count"
                );
                continue;
            }
            if !retracted_dbs.iter().any(|seen| seen == &db) {
                retracted_dbs.push(db);
            }
        }
        for db in &retracted_dbs {
            let mut catalogs = self.catalogs.write().unwrap();
            if let Some(catalog) = catalogs.get_mut(db) {
                let meta_dir = self.data_dir.join("meta");
                if let Err(error) = catalog.save(&meta_dir) {
                    tracing::warn!(
                        db,
                        error = %error,
                        "failed to persist rolled-back row_count retraction"
                    );
                }
            }
        }
        if !retracted_dbs.is_empty() {
            self.mark_catalog_stats_saved();
        }
        // Deferred DML plans are dropped first: no durable state was
        // written at statement time, so discarding them restores the
        // pre-statement state.
        let discarded_dml =
            crate::sql::execution::txn_dml::discard_txn_mutations_at_rollback(self, txn_id);
        let rolled_back = self.batch_writer().rollback_txn(txn_id)?;
        // Retract the unique/PK index entries INSERT statements published at
        // statement time; otherwise the next INSERT of the same key sees a
        // phantom duplicate until the next snapshot refresh.
        self.index_manager()
            .discard_txn_published_entries_at_rollback(txn_id);
        let rows = rolled_back + discarded_dml;
        tracing::debug!(txn_id, rows_rolled_back = rows, "transaction rolled back");
        session.rollback_txn();
        Ok(rows)
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
            .try_update(Ordering::AcqRel, Ordering::Acquire, |count| {
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
        // The resume must run even when a table's flush errors: skipping it
        // left unique-index compaction paused FOREVER (the error paths of
        // both callers only log). Flush failures keep the table queued for
        // the next end-of-bulk attempt; the pause does not get a second
        // chance.
        let flush_all = |tables: &[(String, String)]| -> Result<u64> {
            let mut total = 0u64;
            for (db, table) in tables {
                total = total.saturating_add(self.flush_table_buffers(db, table)?);
                self.bulk_dirty_tables
                    .lock()
                    .unwrap()
                    .remove(&(db.clone(), table.clone()));
            }
            Ok(total)
        };
        let result = flush_all(&tables);
        if !self.is_bulk_mode() {
            self.index_manager.resume_unique_index_compaction();
        }
        result
    }

    pub(super) fn begin_operation(&self) -> EngineOperationGuard<'_> {
        self.in_flight_operations.fetch_add(1, Ordering::AcqRel);
        EngineOperationGuard { engine: self }
    }

    pub fn alloc_txn_id(&self) -> u64 {
        self.segment_mgr.alloc_txn_id()
    }

    pub fn flush_table_buffers(&self, db: &str, table: &str) -> Result<u64> {
        // The end-of-bulk-import flush can write gigabytes per table; when it
        // takes this long, every bulk commit of the last session (and the
        // client behind it) waited just as long — worth a permanent WARN.
        const SLOW_TABLE_FLUSH_WARN: std::time::Duration = std::time::Duration::from_secs(30);
        let start = std::time::Instant::now();
        self.batch_writer.flush_table(db, table)?;

        let rows = self.segment_mgr.flush_table_active(db, table)?;

        {
            let mut catalogs = self.catalogs.write().unwrap();
            if let Some(catalog) = catalogs.get_mut(db) {
                let meta_dir = self.data_dir.join("meta");
                let _ = catalog.save(&meta_dir);
            }
        }
        self.mark_catalog_stats_saved();

        let elapsed = start.elapsed();
        if elapsed >= SLOW_TABLE_FLUSH_WARN {
            tracing::warn!(
                db,
                table,
                rows,
                elapsed_ms = elapsed.as_millis() as u64,
                "slow table buffer flush; the requesting bulk client waited for the whole duration"
            );
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
        drop(catalogs);
        self.mark_catalog_stats_saved();
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
        self.mark_catalog_stats_saved();

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
                let result = compactor.optimize_partition(db, table, &table_meta, partition_key);
                super::adjust_row_count_for_ttl_drops(
                    &self.catalogs,
                    &self.data_dir,
                    db,
                    table,
                    compactor.committed_stats().rows_dropped_ttl,
                );
                let partition_stats = result?;
                stats.segments_read += partition_stats.segments_read;
                stats.segments_written += partition_stats.segments_written;
                stats.rows_written += partition_stats.rows_written;
                stats.rows_dropped += partition_stats.rows_dropped;
            }
        } else if !dirty_partitions.is_empty() {
            let result = compactor.optimize_table(db, table, &table_meta);
            super::adjust_row_count_for_ttl_drops(
                &self.catalogs,
                &self.data_dir,
                db,
                table,
                compactor.committed_stats().rows_dropped_ttl,
            );
            stats = result?;
        }
        if stats.segments_written > 0 {
            // Compaction freed segment path numbers that later writers can
            // reuse; drop this table's cached SUM metadata (see drop table).
            crate::sql::execution::scan::aggregate_meta::invalidate_segment_sum_cache_under(
                &self.data_dir.join(db).join(table),
            );
        }
        if stats.segments_written > 0 || stats.rows_dropped > 0 {
            let schema = Schema::new(table_meta.columns.clone());
            let rebuilt = self.segment_mgr.with_table_mutation_lock(db, table, || {
                crate::sql::execution::dml_insert::rebuild_table_location_indexes(
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
        // Establish the partial-residency coverage bound. The mirror was just
        // emptied, so every row whose partition-column value exceeds the
        // archived maximum can only come from post-close writes, which the
        // insert path mirrors; any later bypass re-closes the gate through
        // `hot_partition_mirrored`.
        let covers_from = match SegmentManager::discover_segments(
            &self.segment_mgr.table_dir(db, table),
        ) {
            // An empty table legitimately yields i64::MIN (nothing archived,
            // every future row is post-close). A discovery FAILURE must not:
            // the mirror is empty here, so claiming full coverage would hide
            // every archived row — decline the feature for this cycle.
            Ok(segments) => self.max_partition_value_of_segments(&table_meta, &segments),
            Err(e) => {
                tracing::warn!(db, table, error = %e, "close-of-day: segment discovery failed, hybrid mirror coverage disabled");
                None
            }
        };
        {
            let mut catalogs = self.catalogs.write().unwrap();
            if let Some(catalog) = catalogs.get_mut(db) {
                if let Some(meta) = catalog.get_table_mut(table) {
                    meta.hot_last_close_date = Some(today);
                    meta.hot_last_close_at = Some(now.timestamp_millis());
                    meta.hot_mirror_covers_from = covers_from;
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
            hot_mirror_covers_from = ?covers_from,
            "close-of-day: table resident state evicted and optimized"
        );
        Ok(())
    }

    /// Max partition-column value across `segments`' zone maps, as i64 in the
    /// column's own value domain (integer partition columns only). `None`
    /// means the mirror-coverage claim cannot be proven (unreadable segment
    /// metadata or a non-integer partition column); `Some(i64::MIN)` when
    /// there is nothing archived, so every future row is post-close.
    fn max_partition_value_of_segments(
        &self,
        table_meta: &crate::storage::catalog::schema::TableMeta,
        segments: &[std::path::PathBuf],
    ) -> Option<i64> {
        use crate::types::scalar::ScalarValue;
        let pk_idx = table_meta.get_partition_column_index()?;
        let column = table_meta.columns.get(pk_idx)?;
        if !matches!(
            column.data_type,
            crate::protocol::types::DataType::Int32 | crate::protocol::types::DataType::Int64
        ) {
            return None;
        }
        if segments.is_empty() {
            return Some(i64::MIN);
        }
        let mut max: Option<i64> = None;
        for seg in segments {
            let meta = self.segment_mgr.segment_meta(seg).ok().flatten()?;
            // `pk_idx` is a CATALOG index; the snapshot's zone maps are in the
            // segment's own footer order. Resolve by name (current +
            // previous), the same translation every prune path performs —
            // positional access read the wrong column's zone map after any
            // DDL drift.
            let mut names = Vec::with_capacity(column.previous_names.len() + 1);
            names.push(column.name.as_str());
            names.extend(column.previous_names.iter().map(String::as_str));
            let footer_idx = names
                .iter()
                .find_map(|name| meta.footer_schema().index_of(name))?;
            let zone = meta.footer_zone_maps().get(footer_idx)?;
            if zone.row_count == 0 {
                continue;
            }
            let value = match zone.max_value.clone() {
                ScalarValue::Int32(Some(v)) => v as i64,
                ScalarValue::Int64(Some(v)) => v,
                _ => return None,
            };
            max = Some(max.map_or(value, |m| m.max(value)));
        }
        Some(max.unwrap_or(i64::MIN))
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

        let mut covers_updates: Vec<(String, String, i64)> = Vec::new();
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
                // Old-layout segments feed through the catalog alignment:
                // the store is always catalog-width, so a table whose
                // segments predate an ALTER still warms up after restart.
                let warmup_schema = Schema::new(table_meta.columns.clone());
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
                            match delete_bitmap::filter_batch(batch, first_row_id, del_bitmap) {
                                Ok(b) => b,
                                Err(e) => {
                                    // Cannot guarantee which rows are live:
                                    // skip this row group instead of feeding
                                    // possibly-deleted rows into the hot store.
                                    tracing::warn!(error = %e, "warmup: delete-bitmap filter failed");
                                    segment_loaded = false;
                                    continue;
                                }
                            }
                        } else {
                            batch
                        };
                        // Segments predating an ALTER carry the old layout:
                        // align to the catalog — added columns fill with
                        // their DEFAULT — so the store is always
                        // catalog-width and old segments still warm it.
                        let batch = if !crate::storage::core::memtable::batch_matches_schema(
                            &batch,
                            &warmup_schema,
                        ) {
                            match crate::storage::core::memtable::align_batch_to_schema(
                                &batch,
                                &warmup_schema,
                            ) {
                                Ok(b) => b,
                                Err(e) => {
                                    tracing::warn!(error = %e, "warmup: schema alignment failed");
                                    segment_loaded = false;
                                    continue;
                                }
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
                            if partition_segments.iter().all(|seg| {
                                eligible.contains(seg)
                                    && loaded_segments.contains(seg)
                                    && !failed_segments.contains(seg)
                            }) {
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

                // Re-establish the partial-residency bound: when every
                // post-close segment was fully loaded, the mirror holds all
                // rows above the archived maximum, so hybrid scans may serve
                // that range without full-partition residency. Applied to the
                // catalog after the read lock is released.
                if close_cutoff_ms.is_some()
                    && !budget_exhausted
                    && eligible
                        .iter()
                        .all(|seg| loaded_segments.contains(seg) && !failed_segments.contains(seg))
                {
                    let archived: Vec<std::path::PathBuf> = segments
                        .iter()
                        .filter(|seg| !eligible.contains(*seg))
                        .cloned()
                        .collect();
                    if let Some(covers) =
                        self.max_partition_value_of_segments(table_meta, &archived)
                    {
                        covers_updates.push((db.clone(), table.clone(), covers));
                    }
                }
            }
        }
        drop(catalogs);
        if !covers_updates.is_empty() {
            let mut catalogs = self.catalogs.write().unwrap();
            for (db, table, covers) in covers_updates {
                if let Some(catalog) = catalogs.get_mut(&db) {
                    if let Some(meta) = catalog.get_table_mut(&table) {
                        meta.hot_mirror_covers_from = Some(covers);
                    }
                    let meta_dir = self.data_dir.join("meta");
                    if let Err(e) = catalog.save(&meta_dir) {
                        tracing::warn!(db, table, error = %e, "warmup: persisting mirror coverage failed");
                    }
                }
            }
        }
    }

    /// Graceful shutdown checkpoint.
    /// Physical writers are sealed, while restart-safe low-rate tails remain
    /// in memtable form so routine restarts do not create tiny segments.
    pub fn shutdown(&mut self) -> Result<()> {
        tracing::info!("engine shutdown: checkpointing resident data");
        // Supervised background schedulers (auto-compact, close-of-day) stop
        // first so no compaction or archival flush races the final
        // checkpoint. Idempotent: a taken handle is gone on the next call.
        for handle in [
            self.auto_compact_supervisor
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .take(),
            self.close_of_day_supervisor
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .take(),
        ]
        .into_iter()
        .flatten()
        {
            handle.shutdown();
        }
        if let Err(e) = self.flush_shutdown_buffers() {
            tracing::warn!(error = %e, "engine shutdown checkpoint failed");
        }
        self.batch_writer.stop();
        Ok(())
    }
}
