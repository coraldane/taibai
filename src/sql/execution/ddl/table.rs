#[allow(clippy::too_many_arguments)]
pub fn exec_create_table(
    engine: &Engine,
    db: &str,
    name: &str,
    columns: &[ColumnDefAst],
    if_not_exists: bool,
    primary_key: &[String],
    unique_keys: &[UniqueKeyDef],
    cluster_by: &Option<Vec<String>>,
    partition_by: &Option<PartitionDef>,
    ttl: &Option<TtlDef>,
    hot: &Option<HotDef>,
    durability: &Option<DurabilityLevel>,
) -> Result<ExecResult> {
    let mut catalogs = engine.catalogs().write().unwrap();
    let catalog = get_existing_catalog(&mut catalogs, db)?;

    if catalog.get_table(name).is_some() {
        if if_not_exists {
            return Ok(ExecResult::Ok { rows_affected: 0 });
        }
        return Err(TaibaiError::Sql(format!(
            "table '{}.{}' already exists",
            db, name
        )));
    }

    let mut col_defs: Vec<ColumnDef> = columns
        .iter()
        .map(|c| {
            let default_value = c.default_value.as_ref().map(|dv| match dv {
                DefaultValue::CurrentTimestamp => ColumnDefaultValue::CurrentTimestamp,
                DefaultValue::Literal(s) => ColumnDefaultValue::Literal(s.clone()),
            });
            ColumnDef::new(&c.name, c.data_type)
                .with_nullable(c.nullable)
                .with_scale(c.scale)
                .with_precision(c.precision)
                .with_auto_increment(c.auto_increment)
                .with_primary_key(c.primary_key)
                .with_default_value(default_value)
        })
        .collect();

    apply_table_primary_key(&mut col_defs, primary_key)?;
    validate_column_constraints(&mut col_defs)?;
    let table_unique_indexes = build_table_unique_indexes(columns, &col_defs, unique_keys)?;
    if let Some(ttl_def) = ttl {
        validate_ttl_columns(&col_defs, ttl_def)?;
    }

    if let Some(hot_def) = hot {
        validate_hot_def(&col_defs, name, hot_def)?;
    }

    catalog.create_table(name, col_defs)?;

    if let Some(meta) = catalog.get_table_mut(name) {
        for (index_name, columns) in table_unique_indexes {
            meta.add_unique_index(&index_name, columns)?;
        }
        if let Some(ref keys) = cluster_by {
            meta.cluster_key = keys.clone();
        }
        if let Some(hot_def) = hot {
            meta.hot_time_column = Some(hot_def.time_column.clone());
            meta.hot_close_time = hot_def.close_time.clone();
            meta.hot_index_column = hot_def.index_column.clone();
        }
        if let Some(ref pdef) = partition_by {
            meta.partition_key = pdef.column.clone();
            meta.partition_type = match pdef.strategy {
                PartitionStrategy::Day => PartitionType::Day,
                PartitionStrategy::Year => PartitionType::Year,
                PartitionStrategy::Month => PartitionType::Month,
                PartitionStrategy::Value => PartitionType::Value,
                PartitionStrategy::Hash(buckets) => {
                    meta.partition_param = buckets;
                    PartitionType::Hash
                }
                PartitionStrategy::Mod(divisor) => {
                    meta.partition_param = divisor;
                    PartitionType::Mod
                }
            };
            meta.update_partition_key_idx();
        }
        if let Some(ttl_def) = ttl {
            meta.set_ttl(&ttl_def.column, ttl_def.duration_secs);
        }
        if let Some(level) = durability {
            meta.durability = match level {
                DurabilityLevel::Strict => TableDurability::Strict,
                DurabilityLevel::Relaxed => TableDurability::Relaxed,
            };
        }
    }

    let auto_inc_col = columns.iter().find(|c| c.auto_increment);
    if let Some(ai_col) = auto_inc_col {
        if let Some(meta) = catalog.get_table_mut(name) {
            meta.auto_increment_next_value = 1;
        }
        engine
            .auto_increment_manager()
            .register(db, name, &ai_col.name, 1)?;
    }

    let table_dir = engine.data_dir().join(db).join(name);
    std::fs::create_dir_all(&table_dir)?;

    // A UNIQUE KEY declared with CREATE TABLE starts from an empty, valid
    // snapshot.  Without this file, INSERT validation falls back to scanning
    // every segment and resident memtable and later appends cannot bootstrap
    // the index because they do not contain the pre-existing row locations.
    if let Some(meta) = catalog.get_table(name) {
        for index in &meta.unique_indexes {
            engine.index_manager().persist_unique_index_entries(
                db,
                name,
                &index.name,
                &index.columns,
                Vec::new(),
                Some(engine.segment_mgr().memtable_flush_epoch(db, name)),
                None,
            )?;
        }
        let primary_columns = meta
            .columns
            .iter()
            .filter(|column| column.primary_key)
            .map(|column| column.name.clone())
            .collect::<Vec<_>>();
        if !primary_columns.is_empty() {
            engine.index_manager().persist_unique_index_entries(
                db,
                name,
                crate::storage::index::index_manager::PRIMARY_LOCATION_INDEX_NAME,
                &primary_columns,
                Vec::new(),
                Some(engine.segment_mgr().memtable_flush_epoch(db, name)),
                None,
            )?;
        }
    }

    let meta_dir = engine.data_dir().join("meta");
    catalog.save(&meta_dir)?;

    Ok(ExecResult::Ok { rows_affected: 0 })
}

pub fn exec_drop_index(
    engine: &Engine,
    db: &str,
    table: &str,
    index_name: &str,
    if_exists: bool,
) -> Result<ExecResult> {
    let mut catalogs = engine.catalogs().write().unwrap();
    let catalog = get_existing_catalog(&mut catalogs, db)?;
    let table_meta = catalog
        .get_table(table)
        .ok_or_else(|| TaibaiError::Sql(format!("table '{}.{}' does not exist", db, table)))?;
    let has_unique_index = table_meta
        .unique_indexes
        .iter()
        .any(|idx| idx.name.eq_ignore_ascii_case(index_name));
    let has_secondary_index = table_meta
        .secondary_indexes
        .iter()
        .any(|idx| idx.name.eq_ignore_ascii_case(index_name));
    if !has_unique_index && !has_secondary_index {
        if if_exists {
            return Ok(ExecResult::Ok { rows_affected: 0 });
        }
        return Err(TaibaiError::Sql(format!(
            "index '{}' does not exist",
            index_name
        )));
    }
    let table_meta = catalog
        .get_table_mut(table)
        .ok_or_else(|| TaibaiError::Sql(format!("table '{}.{}' does not exist", db, table)))?;
    if has_unique_index {
        table_meta.drop_unique_index(index_name)?;
        engine
            .index_manager()
            .remove_unique_index_snapshot(db, table, index_name);
        engine.index_manager().mark_index_stale(db, table);
    } else {
        table_meta.drop_secondary_index(index_name)?;
        engine.index_manager().mark_index_stale(db, table);
    }
    let meta_dir = engine.data_dir().join("meta");
    catalog.save(&meta_dir)?;
    Ok(ExecResult::Ok { rows_affected: 0 })
}

#[allow(clippy::too_many_arguments)]
pub fn exec_create_index(
    engine: &Engine,
    db: &str,
    table: &str,
    index_name: &str,
    columns: &[String],
    unique: bool,
    if_not_exists: bool,
    index_type: Option<&str>,
) -> Result<ExecResult> {
    let mut catalogs = engine.catalogs().write().unwrap();
    let catalog = get_existing_catalog(&mut catalogs, db)?;
    let table_meta = catalog
        .get_table(table)
        .ok_or_else(|| TaibaiError::Sql(format!("table '{}.{}' does not exist", db, table)))?
        .clone();
    let schema = Schema::new(table_meta.columns.clone());
    let indices = resolve_unique_index_columns(&schema, columns)?;
    let table_meta = catalog
        .get_table_mut(table)
        .ok_or_else(|| TaibaiError::Sql(format!("table '{}.{}' does not exist", db, table)))?;
    if table_meta.has_index(index_name) {
        if if_not_exists {
            return Ok(ExecResult::Ok { rows_affected: 0 });
        }
        return Err(TaibaiError::Sql(format!(
            "index '{}' already exists",
            index_name
        )));
    }
    if unique {
        super::dml_insert::validate_unique_key_uniqueness(
            engine, db, table, &schema, &indices, index_name, None,
        )?;
        table_meta.add_unique_index(index_name, columns.to_vec())?;
    } else {
        let index_type = parse_secondary_index_type(index_type)?;
        table_meta.add_secondary_index(index_name, columns.to_vec(), index_type)?;
    }
    let meta_dir = engine.data_dir().join("meta");
    catalog.save(&meta_dir)?;
    Ok(ExecResult::Ok { rows_affected: 0 })
}

fn parse_secondary_index_type(index_type: Option<&str>) -> Result<SecondaryIndexType> {
    match index_type.unwrap_or("bloom").to_ascii_lowercase().as_str() {
        "bloom" => Ok(SecondaryIndexType::Bloom),
        "minmax" | "min_max" => Ok(SecondaryIndexType::MinMax),
        "set" => Ok(SecondaryIndexType::Set),
        other => Err(TaibaiError::Sql(format!(
            "unsupported index type '{}'; supported types: bloom, minmax, set",
            other
        ))),
    }
}

fn resolve_unique_index_columns(schema: &Schema, columns: &[String]) -> Result<Vec<usize>> {
    if columns.is_empty() {
        return Err(TaibaiError::Sql(
            "index must contain at least one column".into(),
        ));
    }
    let mut seen = std::collections::HashSet::new();
    let mut indices = Vec::with_capacity(columns.len());
    for column in columns {
        let idx = schema
            .index_of(column)
            .ok_or_else(|| TaibaiError::Sql(format!("index column '{}' not found", column)))?;
        if !seen.insert(idx) {
            return Err(TaibaiError::Sql(format!(
                "duplicate index column '{}'",
                column
            )));
        }
        indices.push(idx);
    }
    Ok(indices)
}

pub fn exec_show_unique_index_status(engine: &Engine, db: &str, table: &str) -> Result<ExecResult> {
    let catalogs = engine.catalogs().read().unwrap();
    let catalog = catalogs
        .get(db)
        .ok_or_else(|| TaibaiError::Sql(format!("database '{}' does not exist", db)))?;
    let table_meta = catalog
        .get_table(table)
        .ok_or_else(|| TaibaiError::Sql(format!("table '{}.{}' does not exist", db, table)))?
        .clone();
    drop(catalogs);

    let schema = Schema::new(vec![
        ColumnDef::new("table", DataType::VarChar),
        ColumnDef::new("index_name", DataType::VarChar),
        ColumnDef::new("columns", DataType::VarChar),
        ColumnDef::new("status", DataType::VarChar),
        ColumnDef::new("message", DataType::VarChar),
    ]);
    let mut batch = RecordBatch::new(schema.clone());
    let table_schema = Schema::new(table_meta.columns.clone());

    for index in &table_meta.unique_indexes {
        let indices = resolve_unique_index_columns(&table_schema, &index.columns);
        let status = indices.and_then(|indices| {
            super::dml_insert::validate_unique_key_uniqueness(
                engine,
                db,
                table,
                &table_schema,
                &indices,
                &index.name,
                None,
            )?;
            engine
                .index_manager()
                .load_unique_index_snapshot(db, table, &index.name)
        });
        batch
            .column_mut(0)
            .unwrap()
            .push(&ScalarValue::VarChar(Some(table.to_string())))?;
        batch
            .column_mut(1)
            .unwrap()
            .push(&ScalarValue::VarChar(Some(index.name.clone())))?;
        batch
            .column_mut(2)
            .unwrap()
            .push(&ScalarValue::VarChar(Some(index.columns.join(","))))?;
        match status {
            Ok(snapshot) => {
                let message = snapshot
                    .map(|snapshot| format!("persisted keys={}", snapshot.key_count))
                    .unwrap_or_else(|| "not persisted; run REBUILD UNIQUE INDEX".to_string());
                batch
                    .column_mut(3)
                    .unwrap()
                    .push(&ScalarValue::VarChar(Some("READY".to_string())))?;
                batch
                    .column_mut(4)
                    .unwrap()
                    .push(&ScalarValue::VarChar(Some(message)))?;
            }
            Err(err) => {
                batch
                    .column_mut(3)
                    .unwrap()
                    .push(&ScalarValue::VarChar(Some("INVALID".to_string())))?;
                batch
                    .column_mut(4)
                    .unwrap()
                    .push(&ScalarValue::VarChar(Some(err.to_string())))?;
            }
        }
    }

    Ok(ExecResult::Query {
        schema,
        batches: vec![batch],
    })
}

pub fn exec_rebuild_unique_index(
    engine: &Engine,
    db: &str,
    table: &str,
    index_name: Option<&str>,
) -> Result<ExecResult> {
    let catalogs = engine.catalogs().read().unwrap();
    let catalog = catalogs
        .get(db)
        .ok_or_else(|| TaibaiError::Sql(format!("database '{}' does not exist", db)))?;
    let table_meta = catalog
        .get_table(table)
        .ok_or_else(|| TaibaiError::Sql(format!("table '{}.{}' does not exist", db, table)))?
        .clone();
    drop(catalogs);

    let table_schema = Schema::new(table_meta.columns.clone());
    engine.flush_table_buffers(db, table)?;
    let mut rebuilt = 0u64;
    engine
        .segment_mgr()
        .with_table_mutation_lock(db, table, || {
            for index in &table_meta.unique_indexes {
                if index_name
                    .map(|name| !index.name.eq_ignore_ascii_case(name))
                    .unwrap_or(false)
                {
                    continue;
                }
                let indices = resolve_unique_index_columns(&table_schema, &index.columns)?;
                let entries = super::dml_insert::collect_unique_index_entries(
                    engine,
                    db,
                    table,
                    &indices,
                    &index.name,
                )?;
                engine.index_manager().persist_unique_index_entries(
                    db,
                    table,
                    &index.name,
                    &index.columns,
                    entries,
                    Some(engine.segment_mgr().memtable_flush_epoch(db, table)),
                    None,
                )?;
                rebuilt += 1;
            }
            Ok(())
        })?;

    if rebuilt == 0 {
        if let Some(index_name) = index_name {
            return Err(TaibaiError::Sql(format!(
                "unique index '{}' does not exist",
                index_name
            )));
        }
    }
    engine.index_manager().mark_index_stale(db, table);
    Ok(ExecResult::Ok {
        rows_affected: rebuilt,
    })
}

pub fn exec_drop_table(
    engine: &Engine,
    db: &str,
    name: &str,
    if_exists: bool,
) -> Result<ExecResult> {
    let total_start = Instant::now();
    tracing::info!(db, table = name, "drop table: start");
    let table_dir = engine.data_dir().join(db).join(name);
    let mut catalogs = engine.catalogs().write().unwrap();
    tracing::info!(db, table = name, path = %table_dir.display(), "drop table: waiting for table mutation lock");
    let mut lock_attempts = 0u32;
    let (tombstone, table_meta) = loop {
        let catalog = get_existing_catalog(&mut catalogs, db)?;
        let table_meta = match catalog.get_table(name) {
            Some(table_meta) => table_meta.clone(),
            None if cleanup_pending_table_drop(engine.data_dir(), db, name)? => {
                tracing::info!(
                    db,
                    table = name,
                    "drop table: completed pending physical cleanup"
                );
                return Ok(ExecResult::Ok { rows_affected: 0 });
            }
            None if if_exists => {
                tracing::info!(
                    db,
                    table = name,
                    total_elapsed_ms = total_start.elapsed().as_millis(),
                    "drop table: table missing, if exists ignored"
                );
                return Ok(ExecResult::Ok { rows_affected: 0 });
            }
            None => {
                return Err(TaibaiError::Sql(format!(
                    "table '{}.{}' does not exist",
                    db, name
                )))
            }
        };

        let stage_start = Instant::now();
        let mut op = || {
            tracing::info!(
                db,
                table = name,
                elapsed_ms = stage_start.elapsed().as_millis(),
                "drop table: acquired table mutation lock"
            );

            engine.batch_writer().drain_table_tail(db, name)?;

            let step_start = Instant::now();
            tracing::info!(
                db,
                table = name,
                elapsed_ms = step_start.elapsed().as_millis(),
                "drop table: drained table tail"
            );

            let step_start = Instant::now();
            engine.segment_mgr().close_table_state_handles(db, name);
            tracing::info!(
                db,
                table = name,
                elapsed_ms = step_start.elapsed().as_millis(),
                "drop table: closed table state handles"
            );

            let step_start = Instant::now();
            engine.segment_mgr().invalidate_readers_under(&table_dir);
            tracing::info!(
                db,
                table = name,
                elapsed_ms = step_start.elapsed().as_millis(),
                "drop table: invalidated readers"
            );

            let step_start = Instant::now();
            tracing::info!(db, table = name, path = %table_dir.display(), "drop table: staging table directory");
            let tombstone = stage_table_directory(&table_dir).map_err(|error| {
                tracing::error!(db, table = name, path = %table_dir.display(), error = %error, elapsed_ms = step_start.elapsed().as_millis(), "drop table: failed to stage table directory");
                error
            })?;
            tracing::info!(db, table = name, path = %table_dir.display(), elapsed_ms = step_start.elapsed().as_millis(), "drop table: staged table directory");

            catalog.drop_table(name)?;
            let meta_dir = engine.data_dir().join("meta");
            if let Err(save_error) = catalog.save(&meta_dir) {
                catalog.tables.insert(name.to_string(), table_meta.clone());
                if let Some(tombstone) = tombstone.as_deref() {
                    if let Err(restore_error) =
                        restore_staged_table_directory(tombstone, &table_dir)
                    {
                        return Err(TaibaiError::Storage(format!(
                            "failed to save catalog after staging table '{}.{}': {}; additionally failed to restore '{}' to '{}': {}",
                            db,
                            name,
                            save_error,
                            tombstone.display(),
                            table_dir.display(),
                            restore_error
                        )));
                    }
                }
                return Err(save_error);
            }

            let step_start = Instant::now();
            engine.hot_store_manager().remove(db, name);
            tracing::info!(
                db,
                table = name,
                elapsed_ms = step_start.elapsed().as_millis(),
                "drop table: removed resident hot store"
            );

            let step_start = Instant::now();
            let cleared_rows = engine.batch_writer().clear_memtables(db, name);
            tracing::info!(
                db,
                table = name,
                cleared_rows,
                elapsed_ms = step_start.elapsed().as_millis(),
                "drop table: cleared memtables"
            );

            engine.segment_mgr().remove_table_state(db, name);

            Ok(tombstone)
        };
        let attempt = if lock_attempts >= DDL_TABLE_LOCK_BLOCK_AFTER_ATTEMPTS {
            Some(
                engine
                    .segment_mgr()
                    .with_table_mutation_lock(db, name, &mut op)?,
            )
        } else {
            engine
                .segment_mgr()
                .try_with_table_mutation_lock(db, name, &mut op)?
        };
        match attempt {
            Some(tombstone) => break (tombstone, table_meta),
            None => {
                lock_attempts += 1;
                if lock_attempts == 1 || lock_attempts.is_multiple_of(200) {
                    tracing::warn!(
                        db,
                        table = name,
                        lock_attempts,
                        "drop table: table mutation lock busy; retrying without the catalog lock"
                    );
                }
                drop(catalogs);
                thread::sleep(DDL_TABLE_LOCK_RETRY_INTERVAL);
                catalogs = engine.catalogs().write().unwrap();
            }
        }
    };
    drop(catalogs);

    engine.auto_increment_manager().unregister(db, name);
    engine.index_manager().remove_index(db, name);
    let unique_index_names = table_meta
        .unique_indexes
        .iter()
        .map(|index| index.name.clone())
        .collect::<Vec<_>>();
    engine
        .index_manager()
        .remove_table_unique_index_snapshots(db, name, &unique_index_names);

    let mut cleanup_error = None;
    if let Some(tombstone) = tombstone {
        let step_start = Instant::now();
        tracing::info!(db, table = name, path = %tombstone.display(), "drop table: removing staged table directory");
        if let Err(error) = remove_staged_table_directory(&tombstone) {
            tracing::warn!(
                db,
                table = name,
                path = %tombstone.display(),
                error = %error,
                elapsed_ms = step_start.elapsed().as_millis(),
                "drop table: logical drop committed; staged directory cleanup deferred"
            );
            cleanup_error = Some(TaibaiError::Storage(format!(
                "table '{}.{}' was removed from the catalog, but physical data cleanup failed for '{}': {}; cleanup will be retried when the engine restarts",
                db,
                name,
                tombstone.display(),
                error
            )));
        } else {
            tracing::info!(db, table = name, path = %tombstone.display(), elapsed_ms = step_start.elapsed().as_millis(), "drop table: removed staged table directory");
            if let Some(root) = tombstone.parent() {
                let _ = std::fs::remove_dir(root);
            }
        }
    }

    tracing::info!(
        db,
        table = name,
        total_elapsed_ms = total_start.elapsed().as_millis(),
        "drop table: saved catalog and removed metadata managers"
    );

    if let Some(error) = cleanup_error {
        return Err(error);
    }

    tracing::info!(
        db,
        table = name,
        total_elapsed_ms = total_start.elapsed().as_millis(),
        "drop table: finished"
    );

    Ok(ExecResult::Ok { rows_affected: 0 })
}

fn exec_rename_table(engine: &Engine, db: &str, name: &str, new_name: &str) -> Result<ExecResult> {
    let operation_dir = engine
        .data_dir()
        .join(db)
        .join(RENAME_TOMBSTONE_DIR)
        .join(name);
    if operation_dir.exists() {
        return Err(TaibaiError::Storage(format!(
            "table '{}.{}' has a pending rename recovery at '{}'",
            db,
            name,
            operation_dir.display()
        )));
    }

    let mut catalogs = engine.catalogs().write().unwrap();
    let mut lock_attempts = 0u32;
    let catalog_before = loop {
        let catalog = get_existing_catalog(&mut catalogs, db)?;
        if catalog.get_table(name).is_none() {
            return Err(TaibaiError::Sql(format!(
                "table '{}.{}' does not exist",
                db, name
            )));
        }
        if catalog.get_table(new_name).is_some() {
            return Err(TaibaiError::Sql(format!(
                "table '{}.{}' already exists",
                db, new_name
            )));
        }

        let catalog_before = catalog.clone();
        let old_table_dir = engine.data_dir().join(db).join(name);
        let new_table_dir = engine.data_dir().join(db).join(new_name);
        let mut op = || {
            if new_table_dir.exists() {
                return Err(TaibaiError::Storage(format!(
                    "cannot rename table '{}.{}': destination directory '{}' already exists",
                    db,
                    name,
                    new_table_dir.display()
                )));
            }
            engine.batch_writer().drain_table_tail(db, name)?;
            engine.segment_mgr().flush_table_active(db, name)?;
                engine.segment_mgr().close_table_state_handles(db, name);
            engine
                .segment_mgr()
                .invalidate_readers_under(&old_table_dir);

            std::fs::create_dir_all(&operation_dir)?;
            std::fs::write(operation_dir.join(RENAME_TARGET_FILE), new_name.as_bytes())?;

            let renamed = if old_table_dir.exists() {
                std::fs::rename(&old_table_dir, &new_table_dir)?;
                true
            } else {
                false
            };
            catalog.rename_table(name, new_name)?;
            let meta_dir = engine.data_dir().join("meta");
            if let Err(error) = catalog.save(&meta_dir) {
                *catalog = catalog_before.clone();
                if renamed {
                    std::fs::rename(&new_table_dir, &old_table_dir).map_err(|restore_error| {
                        TaibaiError::Storage(format!(
                            "failed to persist rename of table '{}.{}': {}; additionally failed to restore directory '{}': {}",
                            db,
                            name,
                            error,
                            old_table_dir.display(),
                            restore_error
                        ))
                    })?;
                }
                let _ = remove_dir_all_with_retry(&operation_dir);
                return Err(error);
            }
            let renamed_rows = engine.batch_writer().rename_memtables(db, name, new_name);
            engine.segment_mgr().remove_table_state(db, name);
            tracing::info!(
                db,
                old_table = name,
                new_table = new_name,
                renamed_rows,
                "renamed resident memtables"
            );
            Ok(())
        };
        let attempt = if lock_attempts >= DDL_TABLE_LOCK_BLOCK_AFTER_ATTEMPTS {
            Some(
                engine
                    .segment_mgr()
                    .with_table_mutation_lock(db, name, &mut op)?,
            )
        } else {
            engine
                .segment_mgr()
                .try_with_table_mutation_lock(db, name, &mut op)?
        };
        match attempt {
            Some(()) => break catalog_before,
            None => {
                lock_attempts += 1;
                if lock_attempts == 1 || lock_attempts.is_multiple_of(200) {
                    tracing::warn!(
                        db,
                        table = name,
                        lock_attempts,
                        "rename table: table mutation lock busy; retrying without the catalog lock"
                    );
                }
                drop(catalogs);
                thread::sleep(DDL_TABLE_LOCK_RETRY_INTERVAL);
                catalogs = engine.catalogs().write().unwrap();
            }
        }
    };
    drop(catalogs);

    engine
        .auto_increment_manager()
        .rename_table(db, name, new_name);
    engine.index_manager().rename_index(db, name, new_name);
    let unique_index_names = catalog_before
        .get_table(name)
        .map(|meta| {
            meta.unique_indexes
                .iter()
                .map(|index| index.name.clone())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    engine
        .index_manager()
        .remove_table_unique_index_snapshots(db, name, &unique_index_names);
    engine
        .index_manager()
        .remove_table_unique_index_snapshots(db, new_name, &unique_index_names);
    engine.hot_store_manager().remove(db, name);
    if let Err(error) = remove_dir_all_with_retry(&operation_dir) {
        tracing::warn!(
            database = db,
            table = name,
            path = %operation_dir.display(),
            error = %error,
            "rename committed but recovery journal cleanup was deferred"
        );
    }
    if let Some(root) = operation_dir.parent() {
        let _ = std::fs::remove_dir(root);
    }
    Ok(ExecResult::Ok { rows_affected: 0 })
}

fn exec_partition_change(
    engine: &Engine,
    db: &str,
    name: &str,
    partition_key: &str,
    drop_partition: bool,
) -> Result<ExecResult> {
    let table_dir = engine.data_dir().join(db).join(name);
    let operation_dir = engine
        .data_dir()
        .join(db)
        .join(PARTITION_TOMBSTONE_DIR)
        .join(name);
    if operation_dir.exists() {
        return Err(TaibaiError::Storage(format!(
            "table '{}.{}' has a pending partition recovery at '{}'",
            db,
            name,
            operation_dir.display()
        )));
    }

    let mut catalogs = engine.catalogs().write().unwrap();
    let mut lock_attempts = 0u32;
    let operation_dir = loop {
        let catalog = get_existing_catalog(&mut catalogs, db)?;
        let before_meta = catalog
            .get_table(name)
            .ok_or_else(|| TaibaiError::Sql(format!("table '{}.{}' does not exist", db, name)))?
            .clone();
        let mut op = || {
            let resident_partition_keys = engine
                .batch_writer()
                .memtable_manager()
                .partition_keys(db, name);
            let partition_key = resolve_partition_key(
                &before_meta,
                &table_dir,
                partition_key,
                &resident_partition_keys,
            )?
            .ok_or_else(|| {
                TaibaiError::Sql(format!(
                    "partition '{}' does not exist in table '{}.{}'",
                    partition_key, db, name
                ))
            })?;

            // Only the partition being changed must become immutable before
            // its files are staged. Sibling active writers stay resident and
            // continue accumulating rows instead of being fragmented by an
            // unrelated partition operation.
            engine
                .segment_mgr()
                .seal_partition_active_under_table_lock(db, name, &partition_key)?;
            write_before_meta(&operation_dir, &before_meta)?;
            stage_partition_files(&operation_dir, &table_dir, &partition_key)?;

            if let Some(meta) = catalog.get_table_mut(name) {
                if drop_partition {
                    if let Some(partition) = meta
                        .partitions
                        .iter()
                        .find(|part| part.key == partition_key)
                    {
                        meta.row_count -= partition.row_count;
                        meta.segment_count -= partition.segment_count;
                    }
                    meta.partitions.retain(|part| part.key != partition_key);
                } else if let Some(partition) = meta
                    .partitions
                    .iter_mut()
                    .find(|part| part.key == partition_key)
                {
                    meta.row_count -= partition.row_count;
                    meta.segment_count -= partition.segment_count;
                    partition.row_count = 0;
                    partition.segment_count = 0;
                }
            }

            let meta_dir = engine.data_dir().join("meta");
            if let Err(error) = catalog.save(&meta_dir) {
                catalog.tables.insert(name.to_string(), before_meta.clone());
                restore_partition_files(&operation_dir, &table_dir)?;
                return Err(error);
            }
            mark_operation_committed(&operation_dir)?;
            // Clear resident rows only after all fallible staging/catalog work has committed.
            // A flush worker that already owns a target memtable observes the cancelled state
            // under this same mutation lock before it is allowed to write its detached snapshot.
            let cleared_rows =
                engine
                    .batch_writer()
                    .clear_partition_memtables(db, name, &partition_key);
            tracing::info!(
                db,
                table = name,
                partition_key,
                cleared_rows,
                "partition change cleared resident memtables and flushed active segments"
            );
            engine.hot_store_manager().remove(db, name);
            engine.index_manager().mark_index_stale(db, name);
            let unique_index_names = before_meta
                .unique_indexes
                .iter()
                .map(|index| index.name.clone())
                .collect::<Vec<_>>();
            engine.index_manager().remove_table_unique_index_snapshots(
                db,
                name,
                &unique_index_names,
            );
            Ok(operation_dir.clone())
        };
        let attempt = if lock_attempts >= DDL_TABLE_LOCK_BLOCK_AFTER_ATTEMPTS {
            Some(
                engine
                    .segment_mgr()
                    .with_table_mutation_lock(db, name, &mut op)?,
            )
        } else {
            engine
                .segment_mgr()
                .try_with_table_mutation_lock(db, name, &mut op)?
        };
        match attempt {
            Some(operation_dir) => break operation_dir,
            None => {
                lock_attempts += 1;
                if lock_attempts == 1 || lock_attempts.is_multiple_of(200) {
                    tracing::warn!(
                        db,
                        table = name,
                        lock_attempts,
                        "partition change: table mutation lock busy; retrying without the catalog lock"
                    );
                }
                drop(catalogs);
                thread::sleep(DDL_TABLE_LOCK_RETRY_INTERVAL);
                catalogs = engine.catalogs().write().unwrap();
            }
        }
    };
    drop(catalogs);

    if let Err(error) = remove_committed_operation_directory(&operation_dir) {
        let operation = if drop_partition {
            "DROP PARTITION"
        } else {
            "TRUNCATE PARTITION"
        };
        return Err(TaibaiError::Storage(format!(
            "{} for '{}.{}' committed, but cleanup of staged partition data at '{}' failed: {}; cleanup will be retried when the engine restarts",
            operation,
            db,
            name,
            operation_dir.display(),
            error
        )));
    }
    if let Some(root) = operation_dir.parent() {
        let _ = std::fs::remove_dir(root);
    }
    Ok(ExecResult::Ok { rows_affected: 0 })
}

fn exec_set_hot(engine: &Engine, db: &str, name: &str, hot: &HotDef) -> Result<ExecResult> {
    let mut catalogs = engine.catalogs().write().unwrap();
    let mut lock_attempts = 0u32;
    loop {
        let catalog = get_existing_catalog(&mut catalogs, db)?;
        let before_meta = catalog
            .get_table(name)
            .ok_or_else(|| TaibaiError::Sql(format!("table '{}.{}' does not exist", db, name)))?
            .clone();
        validate_hot_def(&before_meta.columns, name, hot)?;

        let mut op = || {
            // A normal table may already have resident rows that predate the
            // hot-store index. Persist that prefix before enabling HOT so a
            // later indexed memtable read cannot omit those rows.
            engine
                .batch_writer()
                .flush_table_under_table_lock(db, name)?;
            engine.segment_mgr().flush_table_active(db, name)?;

            let meta = catalog.get_table_mut(name).ok_or_else(|| {
                TaibaiError::Sql(format!("table '{}.{}' does not exist", db, name))
            })?;
            meta.hot_time_column = Some(hot.time_column.clone());
            meta.hot_index_column = hot.index_column.clone();
            meta.hot_close_time = hot.close_time.clone();

            let meta_dir = engine.data_dir().join("meta");
            if let Err(error) = catalog.save(&meta_dir) {
                catalog.tables.insert(name.to_string(), before_meta.clone());
                return Err(error);
            }

            // Existing durable segments remain authoritative. The resident
            // store starts empty and is populated only by subsequent inserts.
            engine.hot_store_manager().remove(db, name);
            Ok(())
        };
        let attempt = if lock_attempts >= DDL_TABLE_LOCK_BLOCK_AFTER_ATTEMPTS {
            Some(
                engine
                    .segment_mgr()
                    .with_table_mutation_lock(db, name, &mut op)?,
            )
        } else {
            engine
                .segment_mgr()
                .try_with_table_mutation_lock(db, name, &mut op)?
        };
        match attempt {
            Some(()) => break,
            None => {
                lock_attempts += 1;
                if lock_attempts == 1 || lock_attempts.is_multiple_of(200) {
                    tracing::warn!(
                        db,
                        table = name,
                        lock_attempts,
                        "set hot: table mutation lock busy; retrying without the catalog lock"
                    );
                }
                drop(catalogs);
                thread::sleep(DDL_TABLE_LOCK_RETRY_INTERVAL);
                catalogs = engine.catalogs().write().unwrap();
            }
        }
    }
    drop(catalogs);

    Ok(ExecResult::Ok { rows_affected: 0 })
}

pub fn exec_alter_table(
    engine: &Engine,
    db: &str,
    name: &str,
    operations: &[AlterOperation],
) -> Result<ExecResult> {
    if operations.len() != 1
        && operations.iter().any(|operation| {
            matches!(
                operation,
                AlterOperation::RenameTable { .. }
                    | AlterOperation::DropPartition { .. }
                    | AlterOperation::TruncatePartition { .. }
                    | AlterOperation::SetHot { .. }
            )
        })
    {
        return Err(TaibaiError::Sql(
            "RENAME TABLE, DROP PARTITION, TRUNCATE PARTITION, and SET HOT must be executed as a single ALTER operation"
                .into(),
        ));
    }
    if let [operation] = operations {
        match operation {
            AlterOperation::RenameTable { new_name } => {
                return exec_rename_table(engine, db, name, new_name)
            }
            AlterOperation::DropPartition { partition_key } => {
                return exec_partition_change(engine, db, name, partition_key, true)
            }
            AlterOperation::TruncatePartition { partition_key } => {
                return exec_partition_change(engine, db, name, partition_key, false)
            }
            AlterOperation::SetHot { hot } => return exec_set_hot(engine, db, name, hot),
            _ => {}
        }
    }

    let mut catalogs = engine.catalogs().write().unwrap();
    let catalog = catalogs
        .get_mut(db)
        .ok_or_else(|| TaibaiError::Sql(format!("database '{}' does not exist", db)))?;

    if catalog.get_table(name).is_none() {
        return Err(TaibaiError::Sql(format!(
            "table '{}.{}' does not exist",
            db, name
        )));
    }

    for op in operations {
        match op {
            AlterOperation::AddColumn { column } => {
                let default_value = column.default_value.as_ref().map(|dv| match dv {
                    DefaultValue::CurrentTimestamp => ColumnDefaultValue::CurrentTimestamp,
                    DefaultValue::Literal(s) => ColumnDefaultValue::Literal(s.clone()),
                });
                let mut col_def = ColumnDef::new(&column.name, column.data_type)
                    .with_nullable(column.nullable)
                    .with_scale(column.scale)
                    .with_precision(column.precision)
                    .with_auto_increment(column.auto_increment)
                    .with_primary_key(column.primary_key)
                    .with_default_value(default_value);
                validate_column_constraints(std::slice::from_mut(&mut col_def))?;
                if !col_def.nullable && col_def.default_value.is_none() {
                    let existing_rows = catalog
                        .get_table(name)
                        .map(|meta| meta.row_count)
                        .unwrap_or(0);
                    if existing_rows > 0 {
                        return Err(TaibaiError::Sql(format!(
                            "cannot add NOT NULL column '{}' without default to non-empty table",
                            col_def.name
                        )));
                    }
                }
                let added_auto_increment = col_def.auto_increment;
                let added_column_name = col_def.name.clone();
                catalog.add_column(name, col_def)?;
                if added_auto_increment {
                    if let Some(meta) = catalog.get_table_mut(name) {
                        meta.auto_increment_next_value = 1;
                    }
                    engine
                        .auto_increment_manager()
                        .register(db, name, &added_column_name, 1)?;
                }
            }
            AlterOperation::DropColumn { name: col_name } => {
                let dropped_auto_increment = catalog
                    .get_table(name)
                    .and_then(|meta| {
                        meta.columns
                            .iter()
                            .find(|col| col.name.eq_ignore_ascii_case(col_name))
                    })
                    .is_some_and(|col| col.auto_increment);
                catalog.drop_column(name, col_name)?;
                if dropped_auto_increment {
                    engine.auto_increment_manager().unregister(db, name);
                }
                if let Some(meta) = catalog.get_table_mut(name) {
                    meta.unique_indexes.retain(|idx| {
                        !idx.columns
                            .iter()
                            .any(|col| col.eq_ignore_ascii_case(col_name))
                    });
                    meta.secondary_indexes.retain(|idx| {
                        !idx.columns
                            .iter()
                            .any(|col| col.eq_ignore_ascii_case(col_name))
                    });
                    meta.update_partition_key_idx();
                    meta.update_ttl_column_idx();
                }
            }
            AlterOperation::AddUniqueKey { key } => {
                let table_meta = catalog
                    .get_table(name)
                    .ok_or_else(|| {
                        TaibaiError::Sql(format!("table '{}.{}' does not exist", db, name))
                    })?
                    .clone();
                let schema = Schema::new(table_meta.columns.clone());
                let index_name = key
                    .name
                    .clone()
                    .unwrap_or_else(|| format!("uk_{}", table_meta.unique_indexes.len() + 1));
                let indices = resolve_unique_index_columns(&schema, &key.columns)?;
                super::dml_insert::validate_unique_key_uniqueness(
                    engine,
                    db,
                    name,
                    &schema,
                    &indices,
                    &index_name,
                    None,
                )?;
                catalog
                    .get_table_mut(name)
                    .ok_or_else(|| {
                        TaibaiError::Sql(format!("table '{}.{}' does not exist", db, name))
                    })?
                    .add_unique_index(&index_name, key.columns.clone())?;
            }
            AlterOperation::DropIndex { name: index_name } => {
                let table_meta = catalog.get_table_mut(name).ok_or_else(|| {
                    TaibaiError::Sql(format!("table '{}.{}' does not exist", db, name))
                })?;
                if table_meta
                    .unique_indexes
                    .iter()
                    .any(|idx| idx.name.eq_ignore_ascii_case(index_name))
                {
                    table_meta.drop_unique_index(index_name)?;
                    engine
                        .index_manager()
                        .remove_unique_index_snapshot(db, name, index_name);
                    engine.index_manager().mark_index_stale(db, name);
                } else {
                    table_meta.drop_secondary_index(index_name)?;
                    engine.index_manager().mark_index_stale(db, name);
                }
            }
            AlterOperation::RenameColumn { old_name, new_name } => {
                catalog.rename_column(name, old_name, new_name)?;
                if let Some(meta) = catalog.get_table_mut(name) {
                    for index in &mut meta.unique_indexes {
                        for column in &mut index.columns {
                            if column.eq_ignore_ascii_case(old_name) {
                                *column = new_name.clone();
                            }
                        }
                    }
                    for index in &mut meta.secondary_indexes {
                        for column in &mut index.columns {
                            if column.eq_ignore_ascii_case(old_name) {
                                *column = new_name.clone();
                            }
                        }
                    }
                }
            }
            AlterOperation::ModifyColumn { column } => {
                let existing = catalog
                    .get_table(name)
                    .and_then(|meta| {
                        meta.columns
                            .iter()
                            .find(|col| col.name.eq_ignore_ascii_case(&column.name))
                    })
                    .ok_or_else(|| {
                        TaibaiError::Sql(format!(
                            "column '{}' does not exist in table '{}'",
                            column.name, name
                        ))
                    })?;
                if existing.data_type != column.data_type
                    || existing.scale != column.scale
                    || existing.precision != column.precision
                {
                    return Err(TaibaiError::Sql(format!(
                        "MODIFY COLUMN type changes require column rewrite and are not supported yet for '{}'",
                        column.name
                    )));
                }
                catalog.modify_column(
                    name,
                    &column.name,
                    column.data_type,
                    column.scale,
                    column.precision,
                )?;
                engine.persist_auto_increment_next_value(db, name, false)?;
            }
            AlterOperation::RenameTable { new_name: _ } => {
                unreachable!("filesystem ALTER operations are handled before generic ALTER")
            }
            AlterOperation::DropPartition { partition_key } => {
                let _ = partition_key;
                unreachable!("filesystem ALTER operations are handled before generic ALTER")
            }
            AlterOperation::TruncatePartition { partition_key } => {
                let _ = partition_key;
                unreachable!("filesystem ALTER operations are handled before generic ALTER")
            }
            AlterOperation::SetHot { hot: _ } => {
                unreachable!("SET HOT is handled before generic ALTER")
            }
            AlterOperation::SetTtl { ttl } => {
                let meta = catalog.get_table_mut(name).ok_or_else(|| {
                    TaibaiError::Sql(format!("table '{}.{}' does not exist", db, name))
                })?;
                validate_ttl_definition(meta, ttl)?;
                meta.set_ttl(&ttl.column, ttl.duration_secs);
            }
            AlterOperation::DropTtl => {
                let meta = catalog.get_table_mut(name).ok_or_else(|| {
                    TaibaiError::Sql(format!("table '{}.{}' does not exist", db, name))
                })?;
                meta.clear_ttl();
            }
        }
    }

    let meta_dir = engine.data_dir().join("meta");
    catalog.save(&meta_dir)?;

    Ok(ExecResult::Ok { rows_affected: 0 })
}

fn build_table_unique_indexes(
    ast_columns: &[ColumnDefAst],
    columns: &[ColumnDef],
    unique_keys: &[UniqueKeyDef],
) -> Result<Vec<(String, Vec<String>)>> {
    let mut indexes = Vec::new();
    for col in ast_columns
        .iter()
        .filter(|col| col.unique && !col.primary_key)
    {
        indexes.push((format!("uk_{}", col.name), vec![col.name.clone()]));
    }
    for (idx, key) in unique_keys.iter().enumerate() {
        let name = key
            .name
            .clone()
            .unwrap_or_else(|| format!("uk_{}", idx + 1));
        if key.columns.is_empty() {
            return Err(TaibaiError::Sql(
                "unique key must contain at least one column".into(),
            ));
        }
        for key_col in &key.columns {
            if !columns
                .iter()
                .any(|col| col.name.eq_ignore_ascii_case(key_col))
            {
                return Err(TaibaiError::Sql(format!(
                    "unique key column '{}' not found",
                    key_col
                )));
            }
        }
        indexes.push((name, key.columns.clone()));
    }
    let mut seen = std::collections::HashSet::new();
    for (name, _) in &indexes {
        if !seen.insert(name.to_ascii_lowercase()) {
            return Err(TaibaiError::Sql(format!("duplicate unique key '{}'", name)));
        }
    }
    Ok(indexes)
}

fn validate_ttl_definition(
    table_meta: &crate::storage::catalog::schema::TableMeta,
    ttl: &TtlDef,
) -> Result<()> {
    validate_ttl_columns(&table_meta.columns, ttl)
}

fn validate_ttl_columns(columns: &[ColumnDef], ttl: &TtlDef) -> Result<()> {
    if ttl.duration_secs <= 0 {
        return Err(TaibaiError::Sql(
            "TTL duration must be a positive number of seconds".into(),
        ));
    }

    let column = columns
        .iter()
        .find(|c| c.name.eq_ignore_ascii_case(&ttl.column))
        .ok_or_else(|| TaibaiError::Sql(format!("TTL column '{}' not found", ttl.column)))?;

    if column.data_type != DataType::Timestamp {
        return Err(TaibaiError::Sql(format!(
            "TTL column '{}' must be TIMESTAMP",
            ttl.column
        )));
    }

    Ok(())
}

fn apply_table_primary_key(columns: &mut [ColumnDef], primary_key: &[String]) -> Result<()> {
    for pk_col in primary_key {
        let column = columns
            .iter_mut()
            .find(|c| c.name.eq_ignore_ascii_case(pk_col))
            .ok_or_else(|| {
                TaibaiError::Sql(format!("primary key column '{}' not found", pk_col))
            })?;
        column.primary_key = true;
    }
    Ok(())
}

fn validate_column_constraints(columns: &mut [ColumnDef]) -> Result<()> {
    let mut seen_names = std::collections::HashSet::new();
    let mut seen_pk = std::collections::HashSet::new();

    for col in columns {
        let name_key = col.name.to_ascii_lowercase();
        if !seen_names.insert(name_key.clone()) {
            return Err(TaibaiError::Sql(format!("duplicate column '{}'", col.name)));
        }
        if col.primary_key {
            if !seen_pk.insert(name_key) {
                return Err(TaibaiError::Sql(format!(
                    "duplicate primary key column '{}'",
                    col.name
                )));
            }
            col.nullable = false;
        }
        if let Some(default_value) = col.default_value.take() {
            col.default_value = Some(normalize_default_value(
                default_value,
                col.data_type,
                col.scale,
                &col.name,
            )?);
        }
    }
    Ok(())
}

fn normalize_default_value(
    default_value: ColumnDefaultValue,
    data_type: DataType,
    scale: i32,
    column_name: &str,
) -> Result<ColumnDefaultValue> {
    match default_value {
        ColumnDefaultValue::CurrentTimestamp => {
            if data_type != DataType::Timestamp {
                return Err(TaibaiError::Sql(format!(
                    "CURRENT_TIMESTAMP default requires TIMESTAMP column '{}'",
                    column_name
                )));
            }
            Ok(ColumnDefaultValue::CurrentTimestamp)
        }
        ColumnDefaultValue::Literal(value) => {
            let expr = crate::sql::parser::ast::Expr::Literal(value);
            let scalar = super::helpers::eval_expr_to_scalar_with_scale_timezone(
                &expr, &data_type, scale, 0,
            )?;
            if !scalar.is_null() && scalar.data_type() != data_type {
                return Err(TaibaiError::Sql(format!(
                    "default value for column '{}' has type {}, expected {}",
                    column_name,
                    scalar.data_type().display_name(),
                    data_type.display_name()
                )));
            }
            Ok(ColumnDefaultValue::Literal(scalar))
        }
    }
}

pub fn exec_truncate_table(engine: &Engine, db: &str, table: &str) -> Result<ExecResult> {
    let table_dir = engine.data_dir().join(db).join(table);
    let operation_dir = engine
        .data_dir()
        .join(db)
        .join(TRUNCATE_TOMBSTONE_DIR)
        .join(table);
    if operation_dir.exists() {
        return Err(TaibaiError::Storage(format!(
            "table '{}.{}' has a pending TRUNCATE recovery at '{}'",
            db,
            table,
            operation_dir.display()
        )));
    }

    let mut catalogs = engine.catalogs().write().unwrap();
    let mut lock_attempts = 0u32;
    let operation_dir = loop {
        let catalog = catalogs
            .get_mut(db)
            .ok_or_else(|| TaibaiError::Sql(format!("database '{}' does not exist", db)))?;

        let before_meta = catalog
            .get_table(table)
            .ok_or_else(|| TaibaiError::Sql(format!("table '{}.{}' does not exist", db, table)))?
            .clone();

        let mut op = || {
            engine.batch_writer().drain_table_tail(db, table)?;
                engine.segment_mgr().close_table_state_handles(db, table);
            engine.segment_mgr().invalidate_readers_under(&table_dir);

            write_before_meta(&operation_dir, &before_meta)?;
            let fresh_data = operation_dir.join(FRESH_DATA_DIR);
            std::fs::create_dir_all(&fresh_data)?;
            if table_dir.exists() {
                std::fs::rename(&table_dir, operation_dir.join(STAGED_DATA_DIR))?;
            }

            if let Some(meta) = catalog.get_table_mut(table) {
                meta.reset_auto_increment_next_value();
                meta.row_count = 0;
                meta.segment_count = 0;
                for part in &mut meta.partitions {
                    part.row_count = 0;
                    part.segment_count = 0;
                }
            }
            let meta_dir = engine.data_dir().join("meta");
            if let Err(error) = catalog.save(&meta_dir) {
                catalog
                    .tables
                    .insert(table.to_string(), before_meta.clone());
                rollback_staged_table_data(&operation_dir, &table_dir)?;
                return Err(error);
            }
            mark_operation_committed(&operation_dir)?;

            if let Err(error) = std::fs::rename(&fresh_data, &table_dir) {
                return Err(TaibaiError::Storage(format!(
                    "TRUNCATE TABLE '{}.{}' committed, but failed to activate the empty table directory '{}': {}; restart will retry recovery",
                    db,
                    table,
                    table_dir.display(),
                    error
                )));
            }

            engine.batch_writer().clear_memtables(db, table);
            engine.segment_mgr().remove_table_state(db, table);
            engine.hot_store_manager().remove(db, table);
            engine.index_manager().clear_index(db, table);
            let unique_index_names = before_meta
                .unique_indexes
                .iter()
                .map(|index| index.name.clone())
                .collect::<Vec<_>>();
            engine.index_manager().remove_table_unique_index_snapshots(
                db,
                table,
                &unique_index_names,
            );
            Ok(operation_dir.clone())
        };
        let attempt = if lock_attempts >= DDL_TABLE_LOCK_BLOCK_AFTER_ATTEMPTS {
            Some(
                engine
                    .segment_mgr()
                    .with_table_mutation_lock(db, table, &mut op)?,
            )
        } else {
            engine
                .segment_mgr()
                .try_with_table_mutation_lock(db, table, &mut op)?
        };
        match attempt {
            Some(operation_dir) => break operation_dir,
            None => {
                lock_attempts += 1;
                if lock_attempts == 1 || lock_attempts.is_multiple_of(200) {
                    tracing::warn!(
                        db,
                        table,
                        lock_attempts,
                        "truncate table: table mutation lock busy; retrying without the catalog lock"
                    );
                }
                drop(catalogs);
                thread::sleep(DDL_TABLE_LOCK_RETRY_INTERVAL);
                catalogs = engine.catalogs().write().unwrap();
            }
        }
    };

    if engine.auto_increment_manager().get(db, table).is_some() {
        engine.auto_increment_manager().reset(db, table, 1)?;
    }
    drop(catalogs);

    if let Err(error) = remove_committed_operation_directory(&operation_dir) {
        return Err(TaibaiError::Storage(format!(
            "TRUNCATE TABLE '{}.{}' committed, but cleanup of previous data at '{}' failed: {}; cleanup will be retried when the engine restarts",
            db,
            table,
            operation_dir.display(),
            error
        )));
    }
    if let Some(root) = operation_dir.parent() {
        let _ = std::fs::remove_dir(root);
    }

    Ok(ExecResult::Ok { rows_affected: 0 })
}
