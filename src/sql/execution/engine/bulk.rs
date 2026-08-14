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

    fn validate_batch_unique_indexes(
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
        Ok(())
    }

    pub fn exec_bulk_insert_with_wal_control(
        &self,
        db: &str,
        table: &str,
        batch: RecordBatch,
        disable_wal: bool,
    ) -> Result<u64> {
        self.validate_batch_unique_indexes(db, table, &batch)?;
        let table_meta_for_hot = {
            let catalogs = self.catalogs.read().unwrap();
            catalogs
                .get(db)
                .and_then(|catalog| catalog.get_table(table))
                .cloned()
        };
        let resident_coverage_before_bulk = table_meta_for_hot
            .as_ref()
            .and_then(|meta| self.begin_hot_bulk_write(db, table, meta));
        let hot_batch = if resident_coverage_before_bulk.is_some() {
            Some(batch.clone())
        } else {
            None
        };
        let deferred = self.is_bulk_mode();
        let result = match if deferred {
            self.segment_mgr
                .bulk_insert_batch_deferred(db, table, batch, disable_wal)
        } else {
            self.segment_mgr
                .bulk_insert_batch(db, table, batch, disable_wal)
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
            if let (Some(table_meta), Some(batch)) =
                (table_meta_for_hot.as_ref(), hot_batch.as_ref())
            {
                self.feed_hot_store_after_batch(
                    db,
                    table,
                    table_meta,
                    batch,
                    resident_coverage_before_bulk,
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

    pub fn exec_bulk_insert_partitioned_with_wal_control(
        &self,
        db: &str,
        table: &str,
        table_meta: &crate::storage::catalog::schema::TableMeta,
        batch: RecordBatch,
        disable_wal: bool,
        force_deferred: bool,
    ) -> Result<u64> {
        self.validate_batch_unique_indexes(db, table, &batch)?;
        let resident_coverage_before_bulk = self.begin_hot_bulk_write(db, table, table_meta);
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
            self.segment_mgr
                .bulk_insert_batch(db, table, batch, disable_wal)
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

    pub fn exec_bulk_insert_payload_with_wal_control(
        &self,
        db: &str,
        payload: &[u8],
        disable_wal: bool,
    ) -> Result<u64> {
        self.exec_bulk_insert_payload_with_options(db, payload, disable_wal, false)
    }

    pub fn exec_bulk_insert_payload_with_options(
        &self,
        db: &str,
        payload: &[u8],
        disable_wal: bool,
        bulk_import_mode: bool,
    ) -> Result<u64> {
        let _op_guard = self.begin_operation();
        let table_name = bulk_payload_table_name(payload)?.to_string();
        let catalog = {
            let catalogs = self.catalogs.read().unwrap();
            catalogs
                .get(db)
                .cloned()
                .ok_or_else(|| TaibaiError::Sql(format!("database '{}' does not exist", db)))?
        };

        let mut explicit_auto_increment_max = None;
        let mut unique_index_names = Vec::new();
        let table_meta_for_hot = catalog.get_table(&table_name).cloned();
        let hot_payload_should_feed = !bulk_import_mode
            && table_meta_for_hot.as_ref().is_some_and(|meta| {
                crate::storage::core::hot_store::resolve_key_column(meta).is_some()
            });
        if let Some(table_meta) = catalog.get_table(&table_name) {
            unique_index_names = table_meta
                .unique_indexes
                .iter()
                .map(|index| index.name.clone())
                .collect();
            if !table_meta.unique_indexes.is_empty() || table_meta.has_auto_increment() {
                let req = crate::storage::bulk_payload::parse_direct_bulk_payload(payload)?;
                let schema = Schema::new(table_meta.columns.clone());
                let batch =
                    crate::storage::segment_manager::direct_bulk_request_to_batch(&req, &schema)?;
                if !table_meta.unique_indexes.is_empty() {
                    self.validate_batch_unique_indexes(db, &table_name, &batch)?;
                }
                if let Some((col_idx, _)) = table_meta
                    .columns
                    .iter()
                    .enumerate()
                    .find(|(_, col)| col.auto_increment)
                {
                    let mut max_value = None;
                    for row_idx in 0..batch.num_rows() {
                        let Some(value) = batch.scalar(row_idx, col_idx) else {
                            continue;
                        };
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
        let result = match self.segment_mgr.bulk_insert_payload_with_catalog(
            db,
            payload,
            disable_wal,
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
        if result > 0 && (bulk_import_mode || self.is_bulk_mode()) {
            self.mark_bulk_dirty_table(db, &table_name);
        }
        if result > 0 && hot_payload_should_feed {
            if let Some(table_meta) = table_meta_for_hot.as_ref() {
                self.feed_hot_store_after_direct_bulk(
                    db,
                    &table_name,
                    table_meta,
                    payload,
                    resident_coverage_before_bulk,
                );
            }
        } else if result > 0 {
            self.hot_store_mgr.clear_resident_coverage(db, &table_name);
        }

        if result > 0 && !unique_index_names.is_empty() {
            self.index_manager.remove_table_unique_index_snapshots(
                db,
                &table_name,
                &unique_index_names,
            );
        }

        if let Some(max_value) = explicit_auto_increment_max {
            self.auto_increment_mgr
                .advance_past(db, &table_name, max_value)?;
        }

        if !disable_wal && !bulk_import_mode {
            self.record_inserted_rows(db, &table_name, result as i64, true, false)?;
            return Ok(result);
        }

        if explicit_auto_increment_max.is_some() {
            self.persist_auto_increment_next_value(db, &table_name, false)?;
        }
        if result > 0 {}
        Ok(result)
    }

    fn begin_hot_bulk_write(
        &self,
        db: &str,
        table: &str,
        table_meta: &crate::storage::catalog::schema::TableMeta,
    ) -> Option<crate::storage::core::hot_store::ResidentCoverageSnapshot> {
        if crate::storage::core::hot_store::resolve_key_column(table_meta).is_none() {
            return None;
        }
        let snapshot = self
            .hot_store_mgr
            .resident_coverage_snapshot(db, table)
            .unwrap_or_default();
        self.hot_store_mgr.clear_resident_coverage(db, table);
        Some(snapshot)
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
    ) {
        let feed_result = match self.feed_hot_store(db, table, table_meta, batch) {
            Ok(()) => Ok(()),
            Err(crate::common::error::TaibaiError::ResourceBusy(_)) => {
                self.reclaim_hot_store_pressure(db, table);
                self.feed_hot_store(db, table, table_meta, batch)
            }
            Err(e) => Err(e),
        };

        match feed_result {
            Ok(()) => {
                if let Some(snapshot) = resident_coverage_before_bulk {
                    self.hot_store_mgr
                        .restore_resident_coverage(db, table, snapshot);
                }
            }
            Err(e) => {
                self.hot_store_mgr.clear_resident_coverage(db, table);
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
        payload: &[u8],
        resident_coverage_before_bulk: Option<
            crate::storage::core::hot_store::ResidentCoverageSnapshot,
        >,
    ) {
        let feed_result = (|| -> Result<()> {
            let req = crate::storage::bulk_payload::parse_direct_bulk_payload(payload)?;
            let schema = Schema::new(table_meta.columns.clone());
            let batch =
                crate::storage::segment_manager::direct_bulk_request_to_batch(&req, &schema)?;
            match self.feed_hot_store(db, table, table_meta, &batch) {
                Ok(()) => Ok(()),
                Err(crate::common::error::TaibaiError::ResourceBusy(_)) => {
                    self.reclaim_hot_store_pressure(db, table);
                    self.feed_hot_store(db, table, table_meta, &batch)
                }
                Err(e) => Err(e),
            }
        })();

        match feed_result {
            Ok(()) => {
                if let Some(snapshot) = resident_coverage_before_bulk {
                    self.hot_store_mgr
                        .restore_resident_coverage(db, table, snapshot);
                }
            }
            Err(e) => {
                self.hot_store_mgr.clear_resident_coverage(db, table);
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
