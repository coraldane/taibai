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

    let primary_indices = super::dml_insert::primary_key_indices(schema);
    let unique_constraints = super::dml_insert::unique_constraints(table_meta, schema)?;
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
        match engine.index_manager().unique_keys_exist_if_current(
            db,
            table,
            PRIMARY_LOCATION_INDEX_NAME,
            &primary_keys,
        )? {
            Some(true) => {
                return Err(TaibaiError::Sql(format!(
                    "duplicate primary key for table '{}'",
                    table
                )))
            }
            Some(false) | None => {}
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
                super::dml_insert::validate_unique_key_uniqueness(
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
            let mut state = self.catalog_save_state.lock().unwrap();
            state.pending_insert_rows =
                state.pending_insert_rows.saturating_add(rows.max(0) as u64);
            if state.pending_insert_rows >= CATALOG_SAVE_INSERT_ROWS
                || state.last_save_at.elapsed() >= CATALOG_SAVE_INTERVAL
            {
                state.pending_insert_rows = 0;
                state.last_save_at = Instant::now();
                should_save = true;
            }
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
            super::dml_insert::validate_unique_key_uniqueness(
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
            super::dml_insert::validate_primary_key_uniqueness(
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
        let resident_coverage_before_bulk = self.begin_hot_bulk_write(db, table, table_meta);
        let had_segments_before =
            resident_coverage_before_bulk.is_some()
                && !Self::table_has_any_segment(&self.segment_mgr.table_dir(db, table));
        // Load-bearing copy: the segment writers consume the batch by value
        // (append_batch_owned moves columns into row-group buffers), and the
        // hot mirror must only be fed AFTER the write commits — feeding first
        // would expose rows to resident-path scans before the write they
        // depend on exists, and a failed write would strand them. The regular
        // INSERT path can feed first because its rows land in memtables under
        // the same statement, not in segment files.
        let hot_batch = if resident_coverage_before_bulk.is_some() {
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
                if let Some(snapshot) = resident_coverage_before_bulk {
                    self.hot_store_mgr
                        .restore_resident_coverage(db, table, snapshot);
                }
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
                    resident_coverage_before_bulk,
                    had_segments_before,
                );
            } else {
                self.hot_store_mgr.clear_resident_coverage(db, table);
            }
        } else if let Some(snapshot) = resident_coverage_before_bulk {
            self.hot_store_mgr
                .restore_resident_coverage(db, table, snapshot);
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
        let _constraint_guard = has_unique_constraints.then(|| {
            super::dml_insert::constraint_insert_lock(db, &table_name)
                .lock()
                .unwrap()
        });
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

        let resident_coverage_before_bulk = if hot_payload_should_feed {
            self.begin_hot_bulk_write(
                db,
                &table_name,
                table_meta_for_hot.as_ref().expect("checked hot table meta"),
            )
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
                if let Some(snapshot) = resident_coverage_before_bulk {
                    self.hot_store_mgr
                        .restore_resident_coverage(db, &table_name, snapshot);
                }
                return Err(e);
            }
        };
        log_direct_bulk_profile("write", result as usize, write_start.elapsed());
        if result > 0 && (bulk_import_mode || self.is_bulk_mode()) {
            self.mark_bulk_dirty_table(db, &table_name);
        }
        let hot_feed_start = std::time::Instant::now();
        log_direct_bulk_profile("should_feed", if hot_payload_should_feed { 1 } else { 0 }, hot_feed_start.elapsed());
        if result > 0 && hot_payload_should_feed {
            if let Some(table_meta) = table_meta_for_hot.as_ref() {
                self.feed_hot_store_after_direct_bulk(
                    db,
                    &table_name,
                    table_meta,
                    &req,
                    resident_coverage_before_bulk,
                    had_segments_before,
                );
            }
        } else if result > 0 {
            let else_start = std::time::Instant::now();
            self.hot_store_mgr.clear_resident_coverage(db, &table_name);
            // Bulk-mode writes skip the hot-store feed entirely and their
            // partitions are not derived here; retire the fast path for the
            // table until the store is rebuilt (restart warmup / invalidate).
            if table_meta_for_hot.as_ref().is_some_and(|meta| {
                crate::storage::core::hot_store::resolve_key_column(meta).is_some()
            }) {
                self.batch_writer
                    .memtable_manager()
                    .note_hot_unfed_table(db, &table_name, result);
            }
            log_direct_bulk_profile("else_branch", result as usize, else_start.elapsed());
        }

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
    ) -> Option<crate::storage::core::hot_store::ResidentCoverageSnapshot> {
        crate::storage::core::hot_store::resolve_key_column(table_meta)?;
        let snapshot = self
            .hot_store_mgr
            .resident_coverage_snapshot(db, table)
            .unwrap_or_default();
        self.hot_store_mgr.clear_resident_coverage(db, table);
        Some(snapshot)
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
        resident_coverage_before_bulk: Option<
            crate::storage::core::hot_store::ResidentCoverageSnapshot,
        >,
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
                return;
            }
        }
        if let Some(snapshot) = resident_coverage_before_bulk {
            self.hot_store_mgr
                .restore_resident_coverage(db, table, snapshot);
        }
    }

    fn feed_hot_store_after_batch(
        &self,
        db: &str,
        table: &str,
        table_meta: &crate::storage::catalog::schema::TableMeta,
        batch: &RecordBatch,
        resident_coverage_before_bulk: Option<
            crate::storage::core::hot_store::ResidentCoverageSnapshot,
        >,
        had_segments_before: bool,
    ) {
        let feed = self.begin_hot_feed(db, table);
        let feed_result = match self.feed_hot_store(db, table, table_meta, batch, &feed) {
            Ok(()) => Ok(()),
            Err(crate::common::error::TaibaiError::ResourceBusy(_)) => {
                self.reclaim_hot_store_pressure(db, table);
                self.feed_hot_store(db, table, table_meta, batch, &feed)
            }
            Err(e) => Err(e),
        };

        match feed_result {
            Ok(()) => {
                Self::restore_or_mark_full_resident(
                    self,
                    db,
                    table,
                    resident_coverage_before_bulk,
                    had_segments_before,
                );
            }
            Err(e) => {
                self.hot_store_mgr.clear_resident_coverage(db, table);
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
        resident_coverage_before_bulk: Option<
            crate::storage::core::hot_store::ResidentCoverageSnapshot,
        >,
        had_segments_before: bool,
    ) {
        // The request was parsed once before the write; only the batch
        // materialization for the mirror happens here.
        let hot_parse_start = std::time::Instant::now();
        let parse_result = (|| {
            let schema = Schema::new(table_meta.columns.clone());
            crate::storage::segment_manager::direct_bulk_request_to_batch(req, &schema)
        })();
        let mut feed_batch: Option<RecordBatch> = None;
        let feed = self.begin_hot_feed(db, table);
        let feed_result = match parse_result {
            Ok(batch) => {
            log_direct_bulk_profile("hot_feed_parse", 0, hot_parse_start.elapsed());
                let fed = match self.feed_hot_store(db, table, table_meta, &batch, &feed) {
                    Ok(()) => Ok(()),
                    Err(crate::common::error::TaibaiError::ResourceBusy(_)) => {
                        self.reclaim_hot_store_pressure(db, table);
                        self.feed_hot_store(db, table, table_meta, &batch, &feed)
                    }
                    Err(e) => Err(e),
                };
                match fed {
                    Ok(()) => {
                        feed_batch = Some(batch);
                        Ok(())
                    }
                    Err(e) => {
                        feed_batch = Some(batch);
                        Err(e)
                    }
                }
            }
            Err(e) => Err(e),
        };

        match feed_result {
            Ok(()) => {
                Self::restore_or_mark_full_resident(
                    self,
                    db,
                    table,
                    resident_coverage_before_bulk,
                    had_segments_before,
                );
            }
            Err(e) => {
                self.hot_store_mgr.clear_resident_coverage(db, table);
                // The payload rows are already persisted but not mirrored;
                // mark what we can so the resident fast path stands down.
                match feed_batch.as_ref() {
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
