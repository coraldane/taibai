pub fn exec_replace(
    engine: &Engine,
    db: &str,
    table: &str,
    columns: &[String],
    values: &[Vec<Expr>],
    timezone_offset_hours: i32,
    txn_id: u64,
    _bulk_import_mode: bool,
) -> Result<ExecResult> {
    if txn_id > 0 {
        return Err(TaibaiError::Sql(
            "REPLACE is not supported inside an explicit transaction".to_string(),
        ));
    }

    let _unique_guard = PRIMARY_KEY_INSERT_LOCK.lock().unwrap();
    let materialized =
        materialize_insert_values(engine, db, table, columns, values, timezone_offset_hours)?;
    let constraints = replace_constraint_indices(&materialized.table_meta, &materialized.schema)?;
    let key_sets = replace_key_sets(&materialized.batch, &constraints);
    let (survivors, replaced_inside_batch) =
        collapse_replace_batch(&materialized.batch, &constraints)?;
    let inserted_rows = materialized.batch.num_rows() as u64;
    let original_row_count = materialized.table_meta.row_count;
    let internal_txn_id = engine.alloc_txn_id();

    // Flush before taking the statement-wide table lock. The delete and the
    // staged insert then run under one lock, so DELETE/UPDATE cannot interleave.
    engine.flush_table_buffers(db, table)?;
    engine
        .segment_mgr()
        .with_table_mutation_lock(db, table, || {
            let undo = super::dml_delete::delete_replace_conflicts_under_table_lock(
                engine, db, table, &key_sets,
            )?;
            let deleted = undo.deleted_rows();

            #[cfg(test)]
            if FORCE_REPLACE_INSERT_FAILURE.with(|force| force.replace(false)) {
                rollback_replace_under_table_lock(
                    engine,
                    db,
                    table,
                    &materialized.table_meta,
                    original_row_count,
                    internal_txn_id,
                    &undo,
                )?;
                return Err(TaibaiError::Internal(
                    "forced REPLACE insert failure".to_string(),
                ));
            }

            let insert_result = exec_materialized_insert_batch(
                engine,
                db,
                table,
                &materialized.table_meta,
                materialized.schema,
                survivors,
                internal_txn_id,
                false,
                false,
                false,
                true,
                materialized.missing_auto_inc,
            );
            if let Err(error) = insert_result {
                rollback_replace_under_table_lock(
                    engine,
                    db,
                    table,
                    &materialized.table_meta,
                    original_row_count,
                    internal_txn_id,
                    &undo,
                )?;
                return Err(error);
            }

            let finalize_result = super::dml_delete::finalize_replace_delete_under_table_lock(
                engine,
                db,
                table,
                &materialized.table_meta,
                &undo,
            )
            .and_then(|_| {
                engine
                    .batch_writer()
                    .commit_txn_under_table_lock(internal_txn_id)
            });
            if let Err(error) = finalize_result {
                rollback_replace_under_table_lock(
                    engine,
                    db,
                    table,
                    &materialized.table_meta,
                    original_row_count,
                    internal_txn_id,
                    &undo,
                )?;
                return Err(error);
            }
            // The resident hot store is append-only and cannot remove the rows
            // replaced above. Drop it so subsequent reads use the authoritative
            // segments plus the newly committed memtable.
            engine.hot_store_manager().remove(db, table);

            // Publish the committed replacement to an on-disk segment before
            // releasing the table lock. A following DELETE/UPDATE must not miss
            // rows that were still resident in the committed memtable.
            engine
                .batch_writer()
                .flush_table_under_table_lock(db, table)?;

            Ok(ExecResult::Ok {
                rows_affected: deleted + inserted_rows + replaced_inside_batch,
            })
        })
}

pub fn exec_insert(
    engine: &Engine,
    db: &str,
    table: &str,
    columns: &[String],
    values: &[Vec<Expr>],
    timezone_offset_hours: i32,
    txn_id: u64,
    bulk_import_mode: bool,
) -> Result<ExecResult> {
    exec_insert_inner(
        engine,
        db,
        table,
        columns,
        values,
        timezone_offset_hours,
        txn_id,
        bulk_import_mode,
        true,
    )
}

fn exec_insert_inner(
    engine: &Engine,
    db: &str,
    table: &str,
    columns: &[String],
    values: &[Vec<Expr>],
    timezone_offset_hours: i32,
    txn_id: u64,
    bulk_import_mode: bool,
    lock_unique_constraints: bool,
) -> Result<ExecResult> {
    let materialized =
        materialize_insert_values(engine, db, table, columns, values, timezone_offset_hours)?;

    exec_materialized_insert_batch(
        engine,
        db,
        table,
        &materialized.table_meta,
        materialized.schema,
        materialized.batch,
        txn_id,
        bulk_import_mode,
        lock_unique_constraints,
        true,
        false,
        materialized.missing_auto_inc,
    )
}

fn materialize_insert_values(
    engine: &Engine,
    db: &str,
    table: &str,
    columns: &[String],
    values: &[Vec<Expr>],
    timezone_offset_hours: i32,
) -> Result<MaterializedInsert> {
    let catalogs = engine.catalogs().read().unwrap();
    let catalog = catalogs
        .get(db)
        .ok_or_else(|| TaibaiError::Sql(format!("database '{}' does not exist", db)))?;

    let table_meta = catalog
        .get_table(table)
        .ok_or_else(|| TaibaiError::Sql(format!("table '{}.{}' does not exist", db, table)))?
        .clone();

    let schema = Schema::new(table_meta.columns.clone());

    let auto_inc_cols: Vec<usize> = schema
        .columns
        .iter()
        .enumerate()
        .filter(|(_, c)| c.auto_increment)
        .map(|(i, _)| i)
        .collect();

    let col_indices = resolve_insert_columns(&schema, columns, values)?;

    let mut provided_by_col = vec![None; schema.columns.len()];
    for (value_idx, &col_idx) in col_indices.iter().enumerate() {
        provided_by_col[col_idx] = Some(value_idx);
    }

    let missing_auto_inc: Vec<usize> = auto_inc_cols
        .iter()
        .filter(|&idx| provided_by_col[*idx].is_none())
        .cloned()
        .collect();
    let mut is_missing_auto_inc = vec![false; schema.columns.len()];
    for &idx in &missing_auto_inc {
        is_missing_auto_inc[idx] = true;
    }

    let auto_inc_counter = if !missing_auto_inc.is_empty() {
        engine.auto_increment_manager().get(db, table)
    } else {
        None
    };

    let mut batch = RecordBatch::with_capacity(schema.clone(), values.len());

    if can_direct_materialize(&schema, &provided_by_col, &is_missing_auto_inc, values) {
        for row in values {
            for (col_idx, provided) in provided_by_col.iter().enumerate() {
                let val_idx = provided.unwrap();
                push_literal_fast(
                    batch.column_mut(col_idx).unwrap(),
                    &row[val_idx],
                    schema.columns[col_idx].scale,
                    timezone_offset_hours,
                )?;
            }
        }
    } else {
        for row in values {
            materialize_insert_row(
                engine,
                db,
                table,
                &auto_inc_counter,
                &mut batch,
                &schema,
                &provided_by_col,
                timezone_offset_hours,
                |col_idx, value_idx| {
                    if is_default_value_expr(&row[value_idx]) {
                        materialize_missing_insert_scalar(
                            engine,
                            db,
                            table,
                            &auto_inc_counter,
                            &schema,
                            col_idx,
                            timezone_offset_hours,
                        )
                    } else {
                        eval_expr_to_scalar_with_scale_timezone(
                            &row[value_idx],
                            &schema.columns[col_idx].data_type,
                            schema.columns[col_idx].scale,
                            timezone_offset_hours,
                        )
                    }
                },
            )?;
        }
    }

    drop(catalogs);

    Ok(MaterializedInsert {
        table_meta,
        schema,
        batch,
        missing_auto_inc,
    })
}

fn exec_materialized_insert_batch(
    engine: &Engine,
    db: &str,
    table: &str,
    table_meta: &crate::storage::catalog::schema::TableMeta,
    schema: Schema,
    batch: RecordBatch,
    txn_id: u64,
    bulk_import_mode: bool,
    lock_unique_constraints: bool,
    validate_existing_constraints: bool,
    table_lock_held: bool,
    missing_auto_inc: Vec<usize>,
) -> Result<ExecResult> {
    let auto_inc_cols: Vec<usize> = schema
        .columns
        .iter()
        .enumerate()
        .filter(|(_, c)| c.auto_increment)
        .map(|(i, _)| i)
        .collect();
    let mut is_missing_auto_inc = vec![false; schema.columns.len()];
    for &idx in &missing_auto_inc {
        if idx < is_missing_auto_inc.len() {
            is_missing_auto_inc[idx] = true;
        }
    }

    let total_start = Instant::now();
    let row_count = batch.num_rows() as u64;
    log_insert_profile(
        "cluster_sort",
        row_count as usize,
        std::time::Duration::ZERO,
    );

    let constraint_start = Instant::now();
    let unique_constraints = unique_constraints(table_meta, &schema)?;
    let has_unique_checks = !unique_constraints.is_empty();
    let primary_key_indices = primary_key_indices(&schema);
    let _pk_guard =
        if !lock_unique_constraints || (primary_key_indices.is_empty() && !has_unique_checks) {
            None
        } else {
            Some(PRIMARY_KEY_INSERT_LOCK.lock().unwrap())
        };

    if validate_existing_constraints {
        validate_unique_constraints(engine, db, table, &batch, &schema, &unique_constraints)?;
    } else {
        validate_unique_constraints_in_batch(table, &batch, &schema, &unique_constraints)?;
    }

    if !primary_key_indices.is_empty() {
        if generated_auto_increment_key_guarantees_uniqueness(
            &primary_key_indices,
            &is_missing_auto_inc,
        ) {
            validate_primary_key_values_present(&batch, &schema, &primary_key_indices)?;
        } else if validate_existing_constraints {
            validate_primary_key_uniqueness(
                engine,
                db,
                table,
                &batch,
                &schema,
                &primary_key_indices,
            )?;
        } else {
            validate_primary_key_uniqueness_in_batch(table, &batch, &schema, &primary_key_indices)?;
        }
    }
    advance_explicit_auto_increment_counters(
        engine,
        db,
        table,
        &batch,
        &auto_inc_cols,
        &missing_auto_inc,
    )?;

    let auto_increment_touched = !auto_inc_cols.is_empty();
    let unique_index_entries =
        build_unique_index_entries_for_batch(table_meta, &schema, &batch, &unique_constraints)?;
    log_insert_profile(
        "constraints",
        row_count as usize,
        constraint_start.elapsed(),
    );

    if txn_id == 0 && !bulk_import_mode {
        match engine.feed_hot_store(db, table, table_meta, &batch) {
            Ok(()) => {
                if engine
                    .hot_store_manager()
                    .soft_limit_exceeded_for(db, table)
                {
                    let _ = engine.flush_table_buffers(db, table);
                }
            }
            Err(crate::common::error::TaibaiError::ResourceBusy(_)) => {
                engine.reclaim_hot_store_pressure(db, table);
                if let Err(e) = engine.feed_hot_store(db, table, table_meta, &batch) {
                    return Err(e);
                }
            }
            Err(e) => return Err(e),
        }
    }
    let write_start = Instant::now();
    if bulk_import_mode {
        engine
            .exec_bulk_insert_partitioned_with_wal_control(db, table, table_meta, batch, false)?;
    } else {
        write_batch_by_partition(
            engine,
            db,
            table,
            table_meta,
            batch,
            schema,
            txn_id,
            table_lock_held,
        )?;
    }
    log_insert_profile("write", row_count as usize, write_start.elapsed());

    let catalog_start = Instant::now();
    append_unique_index_entries_after_insert(engine, db, table, unique_index_entries)?;

    engine.record_inserted_rows(
        db,
        table,
        row_count as i64,
        !bulk_import_mode && txn_id > 0,
        !bulk_import_mode && txn_id == 0,
    )?;
    if auto_increment_touched {
        engine.persist_auto_increment_next_value(db, table, !bulk_import_mode && txn_id > 0)?;
    }
    log_insert_profile("catalog", row_count as usize, catalog_start.elapsed());
    log_insert_profile("total_exec", row_count as usize, total_start.elapsed());

    Ok(ExecResult::Ok {
        rows_affected: row_count,
    })
}

fn build_unique_index_entries_for_batch(
    table_meta: &crate::storage::catalog::schema::TableMeta,
    schema: &Schema,
    batch: &RecordBatch,
    constraints: &[(String, Vec<usize>)],
) -> Result<Vec<(String, Vec<UniqueIndexEntry>)>> {
    let mut result = Vec::new();
    for (index_name, indices) in constraints {
        let columns = table_meta
            .unique_indexes
            .iter()
            .find(|index| index.name == *index_name)
            .map(|index| index.columns.clone())
            .unwrap_or_default();
        let mut entries = Vec::new();
        for row_idx in 0..batch.num_rows() {
            let Some(key) = unique_key_for_row(batch, row_idx, indices, schema) else {
                continue;
            };
            entries.push(UniqueIndexEntry {
                key,
                location: UniqueRowLocation {
                    segment_path: String::new(),
                    row_id: row_idx as u64,
                },
            });
        }
        if !columns.is_empty() && !entries.is_empty() {
            result.push((index_name.clone(), entries));
        }
    }
    Ok(result)
}

fn append_unique_index_entries_after_insert(
    engine: &Engine,
    db: &str,
    table: &str,
    entries_by_index: Vec<(String, Vec<UniqueIndexEntry>)>,
) -> Result<()> {
    for (index_name, entries) in entries_by_index {
        let _ =
            engine
                .index_manager()
                .append_unique_index_entries(db, table, &index_name, entries)?;
    }
    Ok(())
}

fn replace_constraint_indices(
    table_meta: &crate::storage::catalog::schema::TableMeta,
    schema: &Schema,
) -> Result<Vec<Vec<usize>>> {
    let mut constraints = Vec::new();
    let primary_key = primary_key_indices(schema);
    if !primary_key.is_empty() {
        constraints.push(primary_key);
    }
    constraints.extend(
        unique_constraints(table_meta, schema)?
            .into_iter()
            .map(|(_, indices)| indices),
    );
    Ok(constraints)
}

fn replace_key_sets(
    batch: &RecordBatch,
    constraints: &[Vec<usize>],
) -> Vec<(Vec<usize>, HashSet<Vec<ScalarValue>>)> {
    constraints
        .iter()
        .map(|indices| {
            let keys = (0..batch.num_rows())
                .filter_map(|row_idx| replace_key_for_row(batch, row_idx, indices))
                .collect::<HashSet<_>>();
            (indices.clone(), keys)
        })
        .filter(|(_, keys)| !keys.is_empty())
        .collect()
}

fn rollback_replace_under_table_lock(
    engine: &Engine,
    db: &str,
    table: &str,
    table_meta: &crate::storage::catalog::schema::TableMeta,
    original_row_count: i64,
    txn_id: u64,
    undo: &super::dml_delete::ReplaceDeleteUndo,
) -> Result<()> {
    let mut errors = Vec::new();
    if let Err(error) = engine.batch_writer().rollback_txn(txn_id) {
        errors.push(format!("rollback staged insert: {}", error));
    }
    if let Err(error) = super::dml_delete::restore_replace_delete_under_table_lock(engine, undo) {
        errors.push(format!("restore deleted rows: {}", error));
    }

    for index in &table_meta.unique_indexes {
        engine
            .index_manager()
            .remove_unique_index_snapshot(db, table, &index.name);
    }

    let catalog_restore: Result<()> = (|| {
        let mut catalogs = engine.catalogs().write().unwrap();
        if let Some(catalog) = catalogs.get_mut(db) {
            if let Some(meta) = catalog.get_table_mut(table) {
                meta.row_count = original_row_count;
            }
            catalog.save(&engine.data_dir().join("meta"))?;
        }
        Ok(())
    })();
    if let Err(error) = catalog_restore {
        errors.push(format!("restore catalog: {}", error));
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(TaibaiError::Internal(format!(
            "REPLACE rollback failed: {}",
            errors.join("; ")
        )))
    }
}

fn replace_key_for_row(
    batch: &RecordBatch,
    row_idx: usize,
    indices: &[usize],
) -> Option<Vec<ScalarValue>> {
    let mut key = Vec::with_capacity(indices.len());
    for &col_idx in indices {
        let value = batch.scalar(row_idx, col_idx)?;
        // As with MySQL unique indexes, a key containing NULL does not
        // conflict. Primary-key columns have already been checked non-NULL.
        if value.is_null() {
            return None;
        }
        key.push(value);
    }
    Some(key)
}

fn collapse_replace_batch(
    batch: &RecordBatch,
    constraints: &[Vec<usize>],
) -> Result<(RecordBatch, u64)> {
    let row_count = batch.num_rows();
    if row_count <= 1 || constraints.is_empty() {
        return Ok((batch.clone(), 0));
    }

    let mut active = vec![true; row_count];
    let mut owners = constraints
        .iter()
        .map(|_| HashMap::<Vec<ScalarValue>, usize>::new())
        .collect::<Vec<_>>();
    let mut replaced = 0u64;
    for current in 0..row_count {
        let mut conflicts = HashSet::new();
        for (constraint_idx, indices) in constraints.iter().enumerate() {
            let Some(key) = replace_key_for_row(batch, current, indices) else {
                continue;
            };
            if let Some(prior) = owners[constraint_idx].insert(key, current) {
                if active[prior] {
                    conflicts.insert(prior);
                }
            }
        }
        for prior in conflicts {
            active[prior] = false;
            replaced += 1;
            for (constraint_idx, indices) in constraints.iter().enumerate() {
                let Some(key) = replace_key_for_row(batch, prior, indices) else {
                    continue;
                };
                if owners[constraint_idx].get(&key) == Some(&prior) {
                    owners[constraint_idx].remove(&key);
                }
            }
        }
    }

    let survivors = active.iter().filter(|keep| **keep).count();
    Ok((filter_batch_by_mask(batch, &active, survivors)?, replaced))
}

fn write_batch_by_partition(
    engine: &Engine,
    db: &str,
    table: &str,
    table_meta: &crate::storage::catalog::schema::TableMeta,
    batch: RecordBatch,
    schema: Schema,
    txn_id: u64,
    table_lock_held: bool,
) -> Result<()> {
    if table_meta.partition_key.is_empty() || batch.num_rows() == 0 {
        write_partition_batch(
            engine,
            db,
            table,
            table_meta,
            "",
            batch,
            schema,
            txn_id,
            table_lock_held,
        )?;
        return Ok(());
    }

    let partition_start = Instant::now();
    let single_partition = single_partition_key(table_meta, &batch);
    log_insert_profile(
        "partition_route",
        batch.num_rows(),
        partition_start.elapsed(),
    );
    if let Some(partition_key) = single_partition {
        write_partition_batch(
            engine,
            db,
            table,
            table_meta,
            &partition_key,
            batch,
            schema,
            txn_id,
            table_lock_held,
        )?;
        return Ok(());
    }

    if let Some(ranges) = partition_ranges_if_contiguous(table_meta, &batch) {
        for (partition_key, start, end) in ranges {
            let part_batch = slice_batch_fast(&batch, start, end)?;
            write_partition_batch(
                engine,
                db,
                table,
                table_meta,
                &partition_key,
                part_batch,
                schema.clone(),
                txn_id,
                table_lock_held,
            )?;
        }
        return Ok(());
    }

    let groups = partition_masks(table_meta, &batch);
    if groups.len() <= 1 {
        let partition_key = groups
            .first()
            .map(|(key, _)| key.clone())
            .unwrap_or_default();
        write_partition_batch(
            engine,
            db,
            table,
            table_meta,
            &partition_key,
            batch,
            schema,
            txn_id,
            table_lock_held,
        )?;
        return Ok(());
    }

    for (partition_key, mask) in groups {
        let match_count = mask.iter().filter(|keep| **keep).count();
        let part_batch = filter_batch_by_mask(&batch, &mask, match_count)?;
        write_partition_batch(
            engine,
            db,
            table,
            table_meta,
            &partition_key,
            part_batch,
            schema.clone(),
            txn_id,
            table_lock_held,
        )?;
    }
    Ok(())
}

fn write_partition_batch(
    engine: &Engine,
    db: &str,
    table: &str,
    table_meta: &crate::storage::catalog::schema::TableMeta,
    partition_key: &str,
    batch: RecordBatch,
    schema: Schema,
    txn_id: u64,
    table_lock_held: bool,
) -> Result<()> {
    let batch = sort_batch_for_cluster_key(table_meta, batch)?;
    if table_lock_held {
        engine
            .batch_writer()
            .write_columns_with_txn_under_table_lock(
                db,
                table,
                partition_key,
                batch,
                schema,
                txn_id,
            )?;
    } else {
        engine.batch_writer().write_columns_with_txn(
            db,
            table,
            partition_key,
            batch,
            schema,
            txn_id,
        )?;
    }
    Ok(())
}

fn sort_batch_for_cluster_key(
    table_meta: &crate::storage::catalog::schema::TableMeta,
    batch: RecordBatch,
) -> Result<RecordBatch> {
    if table_meta.cluster_key.is_empty() || batch.num_rows() <= 1 {
        return Ok(batch);
    }

    let mut key_indices = Vec::with_capacity(table_meta.cluster_key.len());
    for key_col in &table_meta.cluster_key {
        let idx = batch.schema.index_of(key_col).ok_or_else(|| {
            TaibaiError::Sql(format!("cluster key column '{}' not found", key_col))
        })?;
        key_indices.push(idx);
    }
    let desc = vec![false; key_indices.len()];
    let inserter = SortKeyInserter::new(key_indices, desc);
    if inserter.is_sorted(&batch) {
        return Ok(batch);
    }
    sort_batch_by_key(&batch, &inserter)
}

fn single_partition_key(
    table_meta: &crate::storage::catalog::schema::TableMeta,
    batch: &RecordBatch,
) -> Option<String> {
    let col_idx = table_meta.get_partition_column_index_cached()?;
    let first = batch
        .scalar(0, col_idx)
        .unwrap_or(crate::types::scalar::ScalarValue::Int64(None));
    let first_key = compute_partition_key_for_value(table_meta, &first);

    for row_idx in 1..batch.num_rows() {
        let val = batch
            .scalar(row_idx, col_idx)
            .unwrap_or(crate::types::scalar::ScalarValue::Int64(None));
        if compute_partition_key_for_value(table_meta, &val) != first_key {
            return None;
        }
    }

    Some(first_key)
}

fn partition_ranges_if_contiguous(
    table_meta: &crate::storage::catalog::schema::TableMeta,
    batch: &RecordBatch,
) -> Option<Vec<(String, usize, usize)>> {
    if table_meta.partition_key.is_empty() || batch.num_rows() == 0 {
        return Some(vec![(String::new(), 0, batch.num_rows())]);
    }
    let col_idx = table_meta.get_partition_column_index_cached()?;
    let first = batch
        .scalar(0, col_idx)
        .unwrap_or(crate::types::scalar::ScalarValue::Int64(None));
    let mut current_key = compute_partition_key_for_value(table_meta, &first);
    let mut current_start = 0usize;
    let mut closed = HashSet::new();
    let mut ranges = Vec::new();

    for row_idx in 1..batch.num_rows() {
        let val = batch
            .scalar(row_idx, col_idx)
            .unwrap_or(crate::types::scalar::ScalarValue::Int64(None));
        let key = compute_partition_key_for_value(table_meta, &val);
        if key != current_key {
            if !closed.insert(current_key.clone()) {
                return None;
            }
            ranges.push((current_key, current_start, row_idx));
            if closed.contains(&key) {
                return None;
            }
            current_key = key;
            current_start = row_idx;
        }
    }
    ranges.push((current_key, current_start, batch.num_rows()));
    Some(ranges)
}

fn slice_batch_fast(batch: &RecordBatch, start: usize, end: usize) -> Result<RecordBatch> {
    if start == 0 && end == batch.num_rows() {
        return Ok(batch.clone());
    }
    let columns = batch
        .columns
        .iter()
        .map(|col| col.slice_fast(start, end))
        .collect();
    RecordBatch::try_new(batch.schema.clone(), columns)
}

fn partition_masks(
    table_meta: &crate::storage::catalog::schema::TableMeta,
    batch: &RecordBatch,
) -> Vec<(String, Vec<bool>)> {
    if table_meta.partition_key.is_empty() || batch.num_rows() == 0 {
        return vec![(String::new(), vec![true; batch.num_rows()])];
    }
    let col_idx = match table_meta.get_partition_column_index_cached() {
        Some(idx) => idx,
        None => return vec![(String::new(), vec![true; batch.num_rows()])],
    };
    let mut key_order = Vec::new();
    let mut groups: HashMap<String, Vec<bool>> = HashMap::new();
    for row_idx in 0..batch.num_rows() {
        let val = batch
            .scalar(row_idx, col_idx)
            .unwrap_or(crate::types::scalar::ScalarValue::Int64(None));
        let key = compute_partition_key_for_value(table_meta, &val);
        if !groups.contains_key(&key) {
            key_order.push(key.clone());
            groups.insert(key.clone(), vec![false; batch.num_rows()]);
        }
        if let Some(mask) = groups.get_mut(&key) {
            mask[row_idx] = true;
        }
    }
    key_order
        .into_iter()
        .filter_map(|key| groups.remove(&key).map(|mask| (key, mask)))
        .collect()
}

fn filter_batch_by_mask(
    batch: &RecordBatch,
    mask: &[bool],
    match_count: usize,
) -> Result<RecordBatch> {
    let columns = batch
        .columns
        .iter()
        .map(|col| col.filter_by_mask_fast(mask, match_count))
        .collect();
    RecordBatch::try_new(batch.schema.clone(), columns)
}

fn can_direct_materialize(
    schema: &Schema,
    provided_by_col: &[Option<usize>],
    is_missing_auto_inc: &[bool],
    values: &[Vec<Expr>],
) -> bool {
    if values.iter().flatten().any(is_default_value_expr) {
        return false;
    }
    schema.columns.iter().enumerate().all(|(idx, col)| {
        provided_by_col[idx].is_some()
            && !is_missing_auto_inc[idx]
            && col.default_value.is_none()
            && col.nullable
            && !col.primary_key
    })
}

fn is_default_value_expr(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Function { name, args, .. }
            if name == "__TAIBAI_DEFAULT__" && args.is_empty()
    )
}

fn push_literal_fast(
    col: &mut ColumnArray,
    expr: &Expr,
    scale: i32,
    timezone_offset_hours: i32,
) -> Result<()> {
    let Expr::Literal(value) = expr else {
        return col.push(&eval_expr_to_scalar_with_scale_timezone(
            expr,
            &col.data_type(),
            scale,
            timezone_offset_hours,
        )?);
    };

    if value.is_null() {
        return col.push(&ScalarValue::null_for_type(col.data_type()));
    }

    match (col, value) {
        (ColumnArray::Int32 { data, bitmap }, ScalarValue::Int64(Some(v))) => {
            data.push(
                i32::try_from(*v)
                    .map_err(|_| TaibaiError::Sql(format!("value '{}' overflows INT32", v)))?,
            );
            bitmap.push_valid();
            Ok(())
        }
        (ColumnArray::Int64 { data, bitmap }, ScalarValue::Int64(Some(v))) => {
            data.push(*v);
            bitmap.push_valid();
            Ok(())
        }
        (ColumnArray::Timestamp { data, bitmap }, ScalarValue::Int64(Some(v))) => {
            data.push(*v);
            bitmap.push_valid();
            Ok(())
        }
        (ColumnArray::Decimal { data, bitmap }, ScalarValue::Float64(Some(v))) => {
            let multiplier = decimal_multiplier(scale)?;
            data.push((*v * multiplier as f64).round() as i64);
            bitmap.push_valid();
            Ok(())
        }
        (ColumnArray::Decimal { data, bitmap }, ScalarValue::Int64(Some(v))) => {
            let multiplier = decimal_multiplier(scale)?;
            data.push(v.checked_mul(multiplier).ok_or_else(|| {
                TaibaiError::Sql(format!("decimal value '{}' overflows scale {}", v, scale))
            })?);
            bitmap.push_valid();
            Ok(())
        }
        (ColumnArray::VarChar { data, bitmap }, ScalarValue::VarChar(Some(v))) => {
            data.push(v.clone());
            bitmap.push_valid();
            Ok(())
        }
        (col, value) if col.data_type() == value.data_type() => col.push(value),
        (col, value) => col.push(&eval_expr_to_scalar_with_scale_timezone(
            &Expr::Literal(value.clone()),
            &col.data_type(),
            scale,
            timezone_offset_hours,
        )?),
    }
}

fn decimal_multiplier(scale: i32) -> Result<i64> {
    if !(0..=18).contains(&scale) {
        return Err(TaibaiError::Sql(format!("invalid decimal scale {}", scale)));
    }
    10_i64
        .checked_pow(scale as u32)
        .ok_or_else(|| TaibaiError::Sql(format!("invalid decimal scale {}", scale)))
}

fn resolve_insert_columns(
    schema: &Schema,
    columns: &[String],
    values: &[Vec<Expr>],
) -> Result<Vec<usize>> {
    if columns.is_empty() {
        if values.iter().all(|row| row.is_empty()) {
            return Ok(Vec::new());
        }
        for row in values {
            if row.len() != schema.columns.len() {
                return Err(TaibaiError::Sql(format!(
                    "insert has {} values but table has {} columns",
                    row.len(),
                    schema.columns.len()
                )));
            }
        }
        return Ok((0..schema.columns.len()).collect());
    }

    let mut seen = HashSet::new();
    let mut indices = Vec::with_capacity(columns.len());
    for col_name in columns {
        let idx = schema
            .index_of(col_name)
            .ok_or_else(|| TaibaiError::Sql(format!("column '{}' not found", col_name)))?;
        if !seen.insert(idx) {
            return Err(TaibaiError::Sql(format!(
                "column '{}' specified more than once",
                col_name
            )));
        }
        indices.push(idx);
    }

    for row in values {
        if row.len() != columns.len() {
            return Err(TaibaiError::Sql(format!(
                "insert has {} values but {} columns were specified",
                row.len(),
                columns.len()
            )));
        }
    }

    Ok(indices)
}

fn materialize_default_value(
    default: &ColumnDefaultValue,
    schema: &Schema,
    col_idx: usize,
    timezone_offset_hours: i32,
) -> Result<ScalarValue> {
    match default {
        ColumnDefaultValue::CurrentTimestamp => {
            if schema.columns[col_idx].data_type != DataType::Timestamp {
                return Err(TaibaiError::Sql(format!(
                    "CURRENT_TIMESTAMP default requires TIMESTAMP column '{}'",
                    schema.columns[col_idx].name
                )));
            }
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_micros() as i64;
            Ok(ScalarValue::Timestamp(Some(now)))
        }
        ColumnDefaultValue::Literal(value) => {
            let expr = Expr::Literal(value.clone());
            eval_expr_to_scalar_with_scale_timezone(
                &expr,
                &schema.columns[col_idx].data_type,
                schema.columns[col_idx].scale,
                timezone_offset_hours,
            )
        }
    }
}

fn validate_nullable(schema: &Schema, col_idx: usize, scalar: &ScalarValue) -> Result<()> {
    let col = &schema.columns[col_idx];
    if (col.primary_key || !col.nullable) && scalar.is_null() {
        return Err(TaibaiError::Sql(format!(
            "column '{}' cannot be NULL",
            col.name
        )));
    }
    Ok(())
}
