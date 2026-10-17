use super::runtime::*;
use super::*;

/// Upper bound for decompressing a bulk payload: the ratio cap mirrors the
/// codec's message ceiling and bounds decompression-bomb amplification.
fn max_bulk_decompressed_len(compressed_len: usize) -> usize {
    compressed_len
        .saturating_mul(8)
        .saturating_add(64 * 1024 * 1024)
        .min(usize::MAX / 2)
}

fn log_direct_bulk_profile(stage: &str, rows: usize, elapsed: std::time::Duration) {
    if crate::common::config::insert_profile_enabled() {
        println!(
            "[insert-profile] stage=direct_bulk_{} rows={} elapsed_us={}",
            stage,
            rows,
            elapsed.as_micros()
        );
    }
}

type DirectBulkConstraintEntries = (
    Vec<Vec<crate::types::scalar::ScalarValue>>,
    Vec<(
        String,
        Vec<crate::storage::index::index_manager::UniqueIndexEntry>,
    )>,
);

fn prepare_direct_bulk_constraint_entries(
    engine: &Engine,
    db: &str,
    table: &str,
    req: &crate::storage::bulk_payload::DirectBulkRequest<'_>,
    table_meta: &crate::storage::catalog::schema::TableMeta,
    schema: &Schema,
) -> Result<DirectBulkConstraintEntries> {
    use crate::storage::index::index_manager::{
        UniqueIndexEntry, UniqueRowLocation, PRIMARY_LOCATION_INDEX_NAME,
    };

    let primary_indices = crate::sql::execution::dml_insert::primary_key_indices(schema);
    let unique_constraints =
        crate::sql::execution::dml_insert::unique_constraints(table_meta, schema)?;
    let mut primary_keys = Vec::with_capacity(req.row_count);
    let mut primary_seen = HashSet::with_capacity(req.row_count);
    if !primary_indices.is_empty() {
        for row_idx in 0..req.row_count {
            let mut key = Vec::with_capacity(primary_indices.len());
            for &col_idx in &primary_indices {
                let value = crate::storage::segment_manager::direct_bulk_column_scalar_at(
                    &req.columns[col_idx],
                    row_idx,
                )?;
                if value.is_null() {
                    return Err(TaibaiError::Sql(format!(
                        "primary key column '{}' cannot be NULL",
                        schema.columns[col_idx].name
                    )));
                }
                key.push(value);
            }
            if !primary_seen.insert(key.clone()) {
                return Err(TaibaiError::Sql(format!(
                    "duplicate primary key for table '{}'",
                    table
                )));
            }
            primary_keys.push(key);
        }
        // The staleness-triggered synchronous refresh inside
        // `unique_keys_exist` can cost a full-table scan on large tables
        // and stall the import for minutes. Bulk imports keep uniqueness
        // tracked by the post-write index refresh, so when the in-memory
        // state is stale the precheck is skipped instead of blocking;
        // REPLACE-style multi-location entries remain the documented
        // outcome of a client that does send duplicates.
        if let Some(true) = engine.index_manager().unique_keys_exist_if_current(
            db,
            table,
            PRIMARY_LOCATION_INDEX_NAME,
            &primary_keys,
        )? {
            return Err(TaibaiError::Sql(format!(
                "duplicate primary key for table '{}'",
                table
            )));
        }
    }

    let mut entries_by_index = Vec::with_capacity(unique_constraints.len());
    for (index_name, indices) in unique_constraints {
        let mut seen = HashSet::with_capacity(req.row_count);
        let mut entries = Vec::with_capacity(req.row_count);
        for row_idx in 0..req.row_count {
            let mut key = Vec::with_capacity(indices.len());
            let mut has_null = false;
            for &col_idx in &indices {
                let value = crate::storage::segment_manager::direct_bulk_column_scalar_at(
                    &req.columns[col_idx],
                    row_idx,
                )?;
                if value.is_null() {
                    has_null = true;
                    break;
                }
                key.push(value);
            }
            if has_null {
                continue;
            }
            // Probe first and move on success so each row pays one key
            // clone instead of two.
            if seen.contains(&key) {
                return Err(TaibaiError::Sql(format!(
                    "duplicate unique key '{}' for table '{}'",
                    index_name, table
                )));
            }
            seen.insert(key.clone());
            entries.push(UniqueIndexEntry {
                key,
                location: UniqueRowLocation {
                    segment_path: String::new(),
                    row_id: row_idx as u64,
                },
            });
        }
        // `seen` holds exactly the deduplicated keys; move them out instead
        // of re-cloning per row.
        let keys: Vec<Vec<crate::types::scalar::ScalarValue>> = seen.into_iter().collect();
        match engine
            .index_manager()
            .unique_keys_exist(db, table, &index_name, &keys)?
        {
            Some(true) => {
                return Err(TaibaiError::Sql(format!(
                    "duplicate unique key '{}' for table '{}'",
                    index_name, table
                )))
            }
            Some(false) => {}
            None => {
                let batch =
                    crate::storage::segment_manager::direct_bulk_request_to_batch(req, schema)?;
                crate::sql::execution::dml_insert::validate_unique_key_uniqueness(
                    engine,
                    db,
                    table,
                    schema,
                    &indices,
                    &index_name,
                    Some(&batch),
                )?;
            }
        }
        entries_by_index.push((index_name, entries));
    }

    Ok((primary_keys, entries_by_index))
}

/// Coverage ownership for one bulk write. [`Engine::begin_hot_bulk_write`]
/// snapshots and clears the resident coverage; the guard carries the
/// pre-write snapshot through the write and the feed, and every exit is
/// explicit:
/// - [`HotBulkCoverageGuard::restore_pre_write`] — the write did not commit
///   (or committed zero rows); put the pre-write coverage back.
/// - [`HotBulkCoverageGuard::settle`] — a successor step took over the final
///   state (the restore-or-mark decision after a successful feed).
///
/// Dropping without either clears the coverage: the fail-closed default. A
/// path that leaves without a decision must not leave the store claiming a
/// coverage it cannot prove (the ghost-row posture). This replaces the
/// snapshot/clear/restore-or-clear sequence that used to be hand-written at
/// every bulk entry and feed tail.
struct HotBulkCoverageGuard<'e> {
    engine: &'e Engine,
    db: String,
    table: String,
    snapshot: Option<crate::storage::core::hot_store::ResidentCoverageSnapshot>,
    settled: bool,
}

impl HotBulkCoverageGuard<'_> {
    /// Whether a pre-write coverage existed to defend (the table has a hot
    /// key column). When false, every operation is a no-op.
    fn active(&self) -> bool {
        self.snapshot.is_some()
    }

    fn restore_pre_write(&mut self) {
        if let Some(snapshot) = self.snapshot.take() {
            self.engine
                .hot_store_mgr
                .restore_resident_coverage(&self.db, &self.table, snapshot);
        }
        self.settled = true;
    }

    fn settle(&mut self) {
        self.settled = true;
    }
}

impl Drop for HotBulkCoverageGuard<'_> {
    fn drop(&mut self) {
        if !self.settled {
            self.engine
                .hot_store_mgr
                .clear_resident_coverage(&self.db, &self.table);
        }
    }
}

impl Engine {
    fn mark_bulk_dirty_table(&self, db: &str, table: &str) {
        self.bulk_dirty_tables
            .lock()
            .unwrap()
            .insert((db.to_string(), table.to_string()));
    }

    pub(crate) fn catalogs(&self) -> &Arc<RwLock<HashMap<String, DatabaseCatalog>>> {
        &self.catalogs
    }

    pub(crate) fn record_inserted_rows(
        &self,
        db: &str,
        table: &str,
        rows: i64,
        save_immediately: bool,
        save_deferred: bool,
    ) -> Result<()> {
        let mut should_save = save_immediately;
        if save_deferred && !save_immediately {
            should_save = self.catalog_stats_save_due(rows.max(0) as u64, 0);
        }

        let mut catalogs = self.catalogs.write().unwrap();
        if let Some(catalog) = catalogs.get_mut(db) {
            if let Some(meta) = catalog.get_table_mut(table) {
                meta.row_count += rows;
                if let Some(counter) = self.auto_increment_mgr.get(db, table) {
                    meta.auto_increment_next_value = counter.current_value();
                }
            }
            if should_save {
                let meta_dir = self.data_dir.join("meta");
                catalog.save(&meta_dir)?;
            }
        }
        Ok(())
    }

    /// Account rows removed by DELETE/UPDATE rewrites against the catalog
    /// row_count. The in-memory decrement always runs under the catalog write
    /// lock inside the caller's table mutation lock (the C22 ordering), and
    /// the whole-catalog save shares the INSERT path's debounce gate, so a
    /// burst of DELETE statements serializes the catalog JSON once per window
    /// instead of once per statement. A skipped save only leaves disk behind
    /// memory until the next due save, flush checkpoint, or DDL save — every
    /// save is a full-catalog snapshot of current memory, so no lost update
    /// is possible.
    pub(crate) fn retract_deleted_rows(
        &self,
        db: &str,
        table: &str,
        rows: u64,
        save_deferred: bool,
    ) -> Result<()> {
        let mut should_save = !save_deferred;
        if save_deferred {
            should_save = self.catalog_stats_save_due(0, rows);
        }

        let mut catalogs = self.catalogs.write().unwrap();
        if let Some(catalog) = catalogs.get_mut(db) {
            if let Some(meta) = catalog.get_table_mut(table) {
                meta.row_count = (meta.row_count - rows as i64).max(0);
            }
            if should_save {
                let meta_dir = self.data_dir.join("meta");
                catalog.save(&meta_dir)?;
            }
        }
        Ok(())
    }

    /// Shared debounce gate for catalog stats saves: accumulates pending
    /// insert/delete rows and returns whether a save is due (row threshold
    /// reached, or the save interval elapsed since the last save with any
    /// pending change). Resetting the accumulators here — before the caller
    /// actually saves — matches the existing INSERT gate's contract: a failed
    /// save reports its error while the next due window retries.
    fn catalog_stats_save_due(&self, insert_rows: u64, delete_rows: u64) -> bool {
        let mut state = self.catalog_save_state.lock().unwrap();
        state.pending_insert_rows = state.pending_insert_rows.saturating_add(insert_rows);
        state.pending_delete_rows = state.pending_delete_rows.saturating_add(delete_rows);
        if state.pending_insert_rows + state.pending_delete_rows >= CATALOG_SAVE_INSERT_ROWS
            || state.last_save_at.elapsed() >= CATALOG_SAVE_INTERVAL
        {
            state.pending_insert_rows = 0;
            state.pending_delete_rows = 0;
            state.last_save_at = Instant::now();
            return true;
        }
        false
    }

    /// Record that a full-catalog save just happened outside the gate (flush
    /// checkpoints, forced rollback save), so the next DML statement does not
    /// immediately re-serialize what was just persisted.
    pub(crate) fn mark_catalog_stats_saved(&self) {
        let mut state = self.catalog_save_state.lock().unwrap();
        state.pending_insert_rows = 0;
        state.pending_delete_rows = 0;
        state.last_save_at = Instant::now();
    }

    pub(crate) fn persist_auto_increment_next_value(
        &self,
        db: &str,
        table: &str,
        save_immediately: bool,
    ) -> Result<()> {
        let Some(counter) = self.auto_increment_mgr.get(db, table) else {
            return Ok(());
        };
        let mut catalogs = self.catalogs.write().unwrap();
        let Some(catalog) = catalogs.get_mut(db) else {
            return Ok(());
        };
        let Some(meta) = catalog.get_table_mut(table) else {
            return Ok(());
        };
        meta.auto_increment_next_value = counter.current_value();
        if save_immediately {
            let meta_dir = self.data_dir.join("meta");
            catalog.save(&meta_dir)?;
        }
        Ok(())
    }

    fn validate_batch_unique_constraints(
        &self,
        db: &str,
        table: &str,
        batch: &RecordBatch,
    ) -> Result<()> {
        let table_meta = {
            let catalogs = self.catalogs.read().unwrap();
            catalogs
                .get(db)
                .and_then(|catalog| catalog.get_table(table))
                .cloned()
        };
        let Some(table_meta) = table_meta else {
            return Ok(());
        };
        let schema = Schema::new(table_meta.columns.clone());
        for index in &table_meta.unique_indexes {
            let mut indices = Vec::with_capacity(index.columns.len());
            for column in &index.columns {
                indices.push(schema.index_of(column).ok_or_else(|| {
                    TaibaiError::Sql(format!("unique index column '{}' not found", column))
                })?);
            }
            crate::sql::execution::dml_insert::validate_unique_key_uniqueness(
                self,
                db,
                table,
                &schema,
                &indices,
                &index.name,
                Some(batch),
            )?;
        }
        let primary_key_indices = schema
            .columns
            .iter()
            .enumerate()
            .filter(|(_, column)| column.primary_key)
            .map(|(idx, _)| idx)
            .collect::<Vec<_>>();
        if !primary_key_indices.is_empty() {
            crate::sql::execution::dml_insert::validate_primary_key_uniqueness(
                self,
                db,
                table,
                batch,
                &schema,
                &primary_key_indices,
            )?;
        }
        Ok(())
    }

    pub fn exec_bulk_insert_partitioned(
        &self,
        db: &str,
        table: &str,
        table_meta: &crate::storage::catalog::schema::TableMeta,
        batch: RecordBatch,
        force_deferred: bool,
    ) -> Result<u64> {
        self.segment_mgr
            .set_table_compression_level(db, table, table_meta.compression_level);
        self.validate_batch_unique_constraints(db, table, &batch)?;
        let mut coverage = self.begin_hot_bulk_write(db, table, table_meta);
        let had_segments_before = coverage.active()
            && !Self::table_has_any_segment(&self.segment_mgr.table_dir(db, table));
        // Load-bearing copy: the segment writers consume the batch by value
        // (append_batch_owned moves columns into row-group buffers), and the
        // hot mirror must only be fed AFTER the write commits — feeding first
        // would expose rows to resident-path scans before the write they
        // depend on exists, and a failed write would strand them. The regular
        // INSERT path can feed first because its rows land in memtables under
        // the same statement, not in segment files.
        let hot_batch = if coverage.active() {
            Some(batch.clone())
        } else {
            None
        };
        let deferred = self.is_bulk_mode() || force_deferred;
        let result = match if deferred {
            self.segment_mgr
                .bulk_insert_batch_partitioned_deferred(db, table, table_meta, batch)
        } else if table_meta.is_partitioned() {
            self.segment_mgr
                .bulk_insert_batch_partitioned(db, table, table_meta, batch)
        } else {
            self.segment_mgr.bulk_insert_batch(db, table, batch)
        } {
            Ok(result) => result,
            Err(e) => {
                coverage.restore_pre_write();
                return Err(e);
            }
        };
        if result > 0 {
            if deferred {
                self.mark_bulk_dirty_table(db, table);
            }
            if let Some(batch) = hot_batch.as_ref() {
                self.feed_hot_store_after_batch(
                    db,
                    table,
                    table_meta,
                    batch,
                    &mut coverage,
                    had_segments_before,
                );
            }
            // No hot batch (no key column): the guard's drop keeps the
            // coverage cleared, as the explicit clear here used to.
        } else {
            coverage.restore_pre_write();
        }
        Ok(result)
    }

    pub fn exec_bulk_insert_payload_with_options(
        &self,
        db: &str,
        payload: &[u8],
        bulk_import_mode: bool,
    ) -> Result<u64> {
        // Internal/test entry point: no authenticated user, so no per-table
        // permission check. The network path must use
        // `exec_bulk_insert_payload_as`.
        self.exec_bulk_insert_payload_as(db, payload, bulk_import_mode, None)
    }

    pub fn exec_bulk_insert_payload_as(
        &self,
        db: &str,
        payload: &[u8],
        bulk_import_mode: bool,
        username: Option<&str>,
    ) -> Result<u64> {
        let _op_guard = self.begin_operation();
        // Direct segment writes bypass the durable tail entirely, so a
        // replication primary would stream standbys that never see these
        // rows. Every replicated write must go through the WAL.
        if crate::common::config::replication_settings().role_is_primary() {
            return Err(TaibaiError::Sql(
                "columnar bulk import is not allowed on a replication primary;                  use regular INSERTs (DURABLE)"
                    .into(),
            ));
        }
        let total_start = std::time::Instant::now();
        // Bulk clients may ship the columnar payload zstd-compressed (level-1
        // frames keep 40-90MB batches within loopback/LAN transfer budgets).
        // The magic is unambiguous (0x28 B5 2F FD) and old uncompressed
        // payloads — which start with the u16 table name length — never
        // collide with it.
        let decompressed;
        let payload: &[u8] = if payload.len() >= 4 && payload[..4] == [0x28, 0xB5, 0x2F, 0xFD] {
            decompressed =
                zstd::bulk::decompress(payload, max_bulk_decompressed_len(payload.len())).map_err(
                    |e| {
                        TaibaiError::Protocol(format!("bulk payload zstd decompress failed: {}", e))
                    },
                )?;
            &decompressed
        } else {
            payload
        };
        let table_name = bulk_payload_table_name(payload)?.to_string();
        if let Some(username) = username {
            // The columnar bulk message bypasses the SQL layer entirely, so
            // it used to bypass the INSERT permission check that every SQL
            // insert path performs.
            use crate::storage::catalog::auth::{PermissionObjectType, PermissionType};
            if !self.auth_manager.check_permission(
                username,
                PermissionType::Insert,
                PermissionObjectType::Table,
                &format!("{}.{}", db, table_name),
            ) {
                return Err(TaibaiError::Sql(format!(
                    "access denied for user '{}' to {:?} on {:?} '{}.{}'",
                    username,
                    PermissionType::Insert,
                    PermissionObjectType::Table,
                    db,
                    table_name
                )));
            }
        }
        let catalog = {
            let catalogs = self.catalogs.read().unwrap();
            catalogs
                .get(db)
                .cloned()
                .ok_or_else(|| TaibaiError::Sql(format!("database '{}' does not exist", db)))?
        };

        // One zero-copy parse shared by the constraint scan, the auto-increment
        // max and the post-write hot-store feed. The request borrows the
        // payload, so keeping it alive is free; re-parsing the payload inside
        // the hot feed used to decode the whole frame a second time.
        let req = crate::storage::bulk_payload::parse_direct_bulk_payload(payload)?;
        // Validate before indexing payload columns for constraints or auto-increment.
        // Malformed requests must return a protocol error, never panic or mutate indexes.
        let table_meta = catalog.get_table(&table_name).ok_or_else(|| {
            TaibaiError::Sql(format!("table '{}.{}' does not exist", db, table_name))
        })?;
        self.segment_mgr
            .validate_direct_bulk_request(&req, &Schema::new(table_meta.columns.clone()))?;
        let mut explicit_auto_increment_max = None;
        let mut unique_index_entries = Vec::new();
        let mut primary_keys = Vec::new();
        let table_meta_for_hot = catalog.get_table_shared(&table_name).cloned();
        let hot_payload_should_feed = !bulk_import_mode
            && table_meta_for_hot.as_ref().is_some_and(|meta| {
                crate::storage::core::hot_store::resolve_key_column(meta).is_some()
            });
        let table_has_primary_key = catalog
            .get_table(&table_name)
            .is_some_and(|meta| meta.columns.iter().any(|column| column.primary_key));
        let has_unique_constraints = table_meta_for_hot.as_ref().is_some_and(|meta| {
            !meta.unique_indexes.is_empty() || meta.columns.iter().any(|column| column.primary_key)
        });
        let _constraint_guard = if has_unique_constraints {
            Some(
                crate::sql::execution::dml_insert::acquire_constraint_insert_lock(db, &table_name)?,
            )
        } else {
            None
        };
        if let Some(table_meta) = catalog.get_table(&table_name) {
            if !table_meta.unique_indexes.is_empty()
                || table_meta.has_auto_increment()
                || table_has_primary_key
            {
                let schema = Schema::new(table_meta.columns.clone());
                if !table_meta.unique_indexes.is_empty() || table_has_primary_key {
                    let constraint_start = std::time::Instant::now();
                    (primary_keys, unique_index_entries) = prepare_direct_bulk_constraint_entries(
                        self,
                        db,
                        &table_name,
                        &req,
                        table_meta,
                        &schema,
                    )?;
                    log_direct_bulk_profile(
                        "constraints",
                        req.row_count,
                        constraint_start.elapsed(),
                    );
                }
                if let Some((col_idx, _)) = table_meta
                    .columns
                    .iter()
                    .enumerate()
                    .find(|(_, col)| col.auto_increment)
                {
                    let mut max_value = None;
                    for row_idx in 0..req.row_count {
                        let value = crate::storage::segment_manager::direct_bulk_column_scalar_at(
                            &req.columns[col_idx],
                            row_idx,
                        )?;
                        let Some(int_value) = value.integer_i128() else {
                            continue;
                        };
                        let Ok(unsigned) = u64::try_from(int_value) else {
                            continue;
                        };
                        max_value =
                            Some(max_value.map_or(unsigned, |current: u64| current.max(unsigned)));
                    }
                    explicit_auto_increment_max = max_value;
                }
            }
        }

        let mut coverage = if hot_payload_should_feed {
            Some(self.begin_hot_bulk_write(
                db,
                &table_name,
                table_meta_for_hot.as_ref().expect("checked hot table meta"),
            ))
        } else {
            None
        };
        let had_segments_before = hot_payload_should_feed
            && !Self::table_has_any_segment(&self.segment_mgr.table_dir(db, &table_name));
        let write_start = std::time::Instant::now();
        let result = match self.segment_mgr.bulk_insert_payload_with_catalog(
            db,
            payload,
            &catalog,
            !bulk_import_mode,
        ) {
            Ok(result) => result,
            Err(e) => {
                if let Some(coverage) = coverage.as_mut() {
                    coverage.restore_pre_write();
                }
                return Err(e);
            }
        };
        log_direct_bulk_profile("write", result as usize, write_start.elapsed());
        if result > 0 && (bulk_import_mode || self.is_bulk_mode()) {
            self.mark_bulk_dirty_table(db, &table_name);
        }
        let hot_feed_start = std::time::Instant::now();
        log_direct_bulk_profile(
            "should_feed",
            if hot_payload_should_feed { 1 } else { 0 },
            hot_feed_start.elapsed(),
        );
        if result > 0 && hot_payload_should_feed {
            if let Some(table_meta) = table_meta_for_hot.as_ref() {
                if let Some(coverage) = coverage.as_mut() {
                    self.feed_hot_store_after_direct_bulk(
                        db,
                        &table_name,
                        table_meta,
                        &req,
                        coverage,
                        had_segments_before,
                    );
                }
            }
        } else if result > 0 {
            let else_start = std::time::Instant::now();
            // The guard's drop clears the coverage. Bulk-mode writes skip the
            // hot-store feed entirely and their partitions are not derived
            // here; retire the fast path for the table until the store is
            // rebuilt (restart warmup / invalidate).
            if table_meta_for_hot.as_ref().is_some_and(|meta| {
                crate::storage::core::hot_store::resolve_key_column(meta).is_some()
            }) {
                self.batch_writer
                    .memtable_manager()
                    .note_hot_unfed_table(db, &table_name, result);
            }
            log_direct_bulk_profile("else_branch", result as usize, else_start.elapsed());
        }
        drop(coverage);

        if result > 0 {
            let index_start = std::time::Instant::now();
            if !primary_keys.is_empty() {
                self.index_manager.refresh_unique_index_state(
                    db,
                    &table_name,
                    crate::storage::index::index_manager::PRIMARY_LOCATION_INDEX_NAME,
                )?;
                let _ = self
                    .index_manager
                    .append_primary_keys(db, &table_name, &primary_keys)?;
            }
            for (index_name, _entries) in &unique_index_entries {
                self.index_manager
                    .refresh_unique_index_state(db, &table_name, index_name)?;
            }
            log_direct_bulk_profile("hot_feed", result as usize, hot_feed_start.elapsed());
            log_direct_bulk_profile("index_refresh", result as usize, index_start.elapsed());
        }

        if let Some(max_value) = explicit_auto_increment_max {
            self.auto_increment_mgr
                .advance_past(db, &table_name, max_value)?;
        }

        if !bulk_import_mode {
            self.record_inserted_rows(db, &table_name, result as i64, true, false)?;
            log_direct_bulk_profile("total", result as usize, total_start.elapsed());
            return Ok(result);
        }

        if explicit_auto_increment_max.is_some() {
            self.persist_auto_increment_next_value(db, &table_name, false)?;
        }

        log_direct_bulk_profile("total", result as usize, total_start.elapsed());
        Ok(result)
    }

    fn begin_hot_bulk_write(
        &self,
        db: &str,
        table: &str,
        table_meta: &crate::storage::catalog::schema::TableMeta,
    ) -> HotBulkCoverageGuard<'_> {
        let snapshot = if crate::storage::core::hot_store::resolve_key_column(table_meta).is_some()
        {
            let snapshot = self
                .hot_store_mgr
                .resident_coverage_snapshot(db, table)
                .unwrap_or_default();
            self.hot_store_mgr.clear_resident_coverage(db, table);
            Some(snapshot)
        } else {
            None
        };
        HotBulkCoverageGuard {
            engine: self,
            db: db.to_string(),
            table: table.to_string(),
            snapshot,
            settled: false,
        }
    }

    /// After a successful hot-store feed: when the written rows are the
    /// table's entire row set (no segments existed before the write), the
    /// store now provably covers every row, so mark it fully resident — this
    /// is what lets intraday point queries answer from the in-memory
    /// inverted index instead of scanning segments. Otherwise restore the
    /// pre-write coverage snapshot as before. The marker is additionally
    /// gated on zero unmirrored-row accounting.
    fn restore_or_mark_full_resident(
        &self,
        db: &str,
        table: &str,
        coverage: &mut HotBulkCoverageGuard<'_>,
        had_segments_before: bool,
    ) {
        if !had_segments_before
            && self
                .batch_writer
                .memtable_manager()
                .hot_partition_mirrored(db, table, "")
        {
            let store = self.hot_store_mgr.get(db, table);
            if store.is_some_and(|s| s.row_count() > 0) {
                self.apply_hot_resident_mark(db, table, HotResidentMark::FullTable);
                coverage.settle();
                return;
            }
        }
        coverage.restore_pre_write();
    }

    /// Feed one batch into the hot store, reclaiming resident memory once on
    /// backpressure before retrying. The single feed step shared by every
    /// bulk feed tail.
    fn feed_hot_store_with_reclaim(
        &self,
        db: &str,
        table: &str,
        table_meta: &crate::storage::catalog::schema::TableMeta,
        batch: &RecordBatch,
        feed: &HotFeedGuard<'_>,
    ) -> crate::common::error::Result<()> {
        match self.feed_hot_store(db, table, table_meta, batch, feed) {
            Ok(()) => Ok(()),
            Err(crate::common::error::TaibaiError::ResourceBusy(_)) => {
                self.reclaim_hot_store_pressure(db, table);
                self.feed_hot_store(db, table, table_meta, batch, feed)
            }
            Err(e) => Err(e),
        }
    }

    fn feed_hot_store_after_batch(
        &self,
        db: &str,
        table: &str,
        table_meta: &crate::storage::catalog::schema::TableMeta,
        batch: &RecordBatch,
        coverage: &mut HotBulkCoverageGuard<'_>,
        had_segments_before: bool,
    ) {
        let feed = self.begin_hot_feed(db, table);
        match self.feed_hot_store_with_reclaim(db, table, table_meta, batch, &feed) {
            Ok(()) => {
                Self::restore_or_mark_full_resident(self, db, table, coverage, had_segments_before);
            }
            Err(e) => {
                // The rows are persisted but not mirrored; the coverage
                // guard's drop clears the fast path, and the unmirrored-row
                // accounting keeps it down for these rows.
                self.note_mirror_batch(db, table, table_meta, batch, false);
                tracing::warn!(
                    db,
                    table,
                    error = %e,
                    "bulk insert persisted, but feeding hot store failed; resident fast path disabled"
                );
            }
        }
    }

    fn feed_hot_store_after_direct_bulk(
        &self,
        db: &str,
        table: &str,
        table_meta: &crate::storage::catalog::schema::TableMeta,
        req: &crate::storage::bulk_payload::DirectBulkRequest<'_>,
        coverage: &mut HotBulkCoverageGuard<'_>,
        had_segments_before: bool,
    ) {
        // The request was parsed once before the write; only the batch
        // materialization for the mirror happens here.
        let hot_parse_start = std::time::Instant::now();
        let parse_result = {
            let schema = Schema::new(table_meta.columns.clone());
            crate::storage::segment_manager::direct_bulk_request_to_batch(req, &schema)
        };
        log_direct_bulk_profile("hot_feed_parse", 0, hot_parse_start.elapsed());
        let mut parsed_batch: Option<RecordBatch> = None;
        let feed = self.begin_hot_feed(db, table);
        let outcome = match parse_result {
            Ok(batch) => {
                let result = self.feed_hot_store_with_reclaim(db, table, table_meta, &batch, &feed);
                parsed_batch = Some(batch);
                result
            }
            Err(e) => Err(e),
        };

        match outcome {
            Ok(()) => {
                Self::restore_or_mark_full_resident(self, db, table, coverage, had_segments_before);
            }
            Err(e) => {
                // The payload rows are already persisted but not mirrored;
                // the coverage guard's drop clears, and the accounting below
                // stands the fast path down for what we can name. A parse
                // failure names no rows, so only the table-level marker goes
                // up.
                match parsed_batch.as_ref() {
                    Some(batch) => self.note_mirror_batch(db, table, table_meta, batch, false),
                    None => self
                        .batch_writer
                        .memtable_manager()
                        .note_hot_unfed_table(db, table, 1),
                }
                tracing::warn!(
                    db,
                    table,
                    error = %e,
                    "direct bulk insert persisted, but feeding hot store failed; resident fast path disabled"
                );
            }
        }
    }
}
