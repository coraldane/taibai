pub(crate) fn unique_constraints(
    table_meta: &crate::storage::catalog::schema::TableMeta,
    schema: &Schema,
) -> Result<Vec<(String, Vec<usize>)>> {
    let mut constraints = Vec::new();
    for index in &table_meta.unique_indexes {
        let mut indices = Vec::with_capacity(index.columns.len());
        for column in &index.columns {
            indices.push(schema.index_of(column).ok_or_else(|| {
                TaibaiError::Sql(format!("unique index column '{}' not found", column))
            })?);
        }
        constraints.push((index.name.clone(), indices));
    }
    Ok(constraints)
}

fn validate_unique_constraints(
    engine: &Engine,
    db: &str,
    table: &str,
    batch: &RecordBatch,
    schema: &Schema,
    constraints: &[(String, Vec<usize>)],
) -> Result<()> {
    for (name, indices) in constraints {
        validate_unique_key_uniqueness(engine, db, table, schema, indices, name, Some(batch))?;
    }
    Ok(())
}

fn validate_unique_constraints_in_batch(
    table: &str,
    batch: &RecordBatch,
    schema: &Schema,
    constraints: &[(String, Vec<usize>)],
) -> Result<()> {
    for (name, indices) in constraints {
        let mut keys = HashSet::new();
        for row_idx in 0..batch.num_rows() {
            let Some(key) = unique_key_for_row(batch, row_idx, indices, schema) else {
                continue;
            };
            // Probe first and move on success: hashing twice is cheaper than
            // cloning every key cell (a String copy for VarChar) per row.
            if keys.contains(&key) {
                return Err(duplicate_unique_key_error(table, name, &key));
            }
            keys.insert(key);
        }
    }
    Ok(())
}

pub fn validate_unique_key_uniqueness(
    engine: &Engine,
    db: &str,
    table: &str,
    schema: &Schema,
    indices: &[usize],
    index_name: &str,
    new_batch: Option<&RecordBatch>,
) -> Result<()> {
    let mut keys = HashSet::new();
    if let Some(batch) = new_batch {
        for row_idx in 0..batch.num_rows() {
            let Some(key) = unique_key_for_row(batch, row_idx, indices, schema) else {
                continue;
            };
            if keys.contains(&key) {
                return Err(duplicate_unique_key_error(table, index_name, &key));
            }
            keys.insert(key);
        }
    }
    if new_batch.is_some() {
        if let Some(columns) = engine
            .index_manager()
            .unique_index_columns(db, table, index_name)?
        {
            let index_columns = indices
                .iter()
                .map(|idx| schema.columns[*idx].name.clone())
                .collect::<Vec<_>>();
            if columns == index_columns {
                // Pass the set directly: the lookup is generic over the key
                // iteration, so the whole key set need not be re-cloned per
                // INSERT.
                match engine.index_manager().unique_keys_exist(db, table, index_name, &keys)? {
                    Some(false) => return Ok(()),
                    Some(true) => {
                        for key in &keys {
                            if engine
                                .index_manager()
                                .unique_key_exists(db, table, index_name, key)?
                                .unwrap_or(false)
                            {
                                return Err(duplicate_unique_key_error(table, index_name, key));
                            }
                        }
                        return Ok(());
                    }
                    None => {}
                }
            }
        }
    }
    load_existing_unique_keys(engine, db, table, schema, indices, index_name, &keys)
}

/// Collect one entry per live row for the given key columns. Rows sharing a
/// duplicate key from an unvalidated write is preserved: REPLACE treats every location as a
/// replaceable conflict, so callers must see each one instead of failing on
/// pre-existing dirty data.
pub fn collect_unique_index_entries(
    engine: &Engine,
    db: &str,
    table: &str,
    indices: &[usize],
    reason: &str,
) -> Result<Vec<UniqueIndexEntry>> {
    collect_unique_index_entries_from(
        engine.segment_mgr(),
        &engine.memtable_manager().get_by_table(db, table),
        &engine.data_dir().join(db).join(table),
        table,
        indices,
        reason,
    )
}

pub(crate) fn collect_unique_index_entries_from(
    segment_mgr: &SegmentManager,
    memtables: &[std::sync::Arc<crate::storage::core::memtable::MemTable>],
    table_dir: &std::path::Path,
    table: &str,
    indices: &[usize],
    reason: &str,
) -> Result<Vec<UniqueIndexEntry>> {
    let started = std::time::Instant::now();
    let mut entries = Vec::new();
    if table_dir.exists() {
        for seg_path in SegmentManager::discover_segments(table_dir)? {
            let reader = segment_mgr.open_reader_cached(&seg_path)?;
            for rg_idx in 0..reader.num_row_groups() {
                let batch = reader.read_row_group_with_projection(rg_idx, indices)?;
                let rg_start = reader.row_group_start_row(rg_idx);
                let delete_bitmap = reader.delete_bitmap();
                let projected_indices: Vec<usize> = (0..batch.num_columns()).collect();
                for row_idx in 0..batch.num_rows() {
                    let row_id = rg_start + row_idx as u64;
                    if crate::storage::segment::delete_bitmap::is_deleted(delete_bitmap, row_id) {
                        continue;
                    }
                    let Some(key) =
                        unique_key_for_row(&batch, row_idx, &projected_indices, &batch.schema)
                    else {
                        continue;
                    };
                    entries.push(UniqueIndexEntry {
                        key,
                        location: UniqueRowLocation {
                            segment_path: seg_path.to_string_lossy().to_string(),
                            row_id,
                        },
                    });
                }
            }
        }
    }
    for memtable in memtables {
        if let Some(batch) = memtable.read() {
            for row_idx in 0..batch.num_rows() {
                let Some(key) = unique_key_for_row(&batch, row_idx, indices, &batch.schema) else {
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
        }
    }
    tracing::warn!(
        table,
        key_columns = indices.len(),
        entries = entries.len(),
        elapsed_ms = started.elapsed().as_millis() as u64,
        reason,
        "unique index snapshot rebuild from full-table scan"
    );
    Ok(entries)
}

/// Historical duplicate keys among themselves do not violate the statement
/// being validated: only a collision with a key the current batch is writing
/// is an error. This mirrors the snapshot-based path (`unique_key_exists`),
/// which never fails on unrelated pre-existing duplicates.
///
/// The fallback collects the whole key set once and then INSTALLS it as the
/// index snapshot, so a single lost/invalid snapshot costs one full-table
/// scan instead of one per subsequent INSERT (the primary-key fallback in
/// `validate_primary_key_uniqueness` already did this via
/// `install_primary_key_index`).
fn load_existing_unique_keys(
    engine: &Engine,
    db: &str,
    table: &str,
    schema: &Schema,
    indices: &[usize],
    index_name: &str,
    new_keys: &HashSet<Vec<ScalarValue>>,
) -> Result<()> {
    let entries = collect_unique_index_entries(
        engine,
        db,
        table,
        indices,
        "unique key validation fallback",
    )?;
    if let Some(entry) = entries.iter().find(|entry| new_keys.contains(&entry.key)) {
        return Err(duplicate_unique_key_error(table, index_name, &entry.key));
    }
    // Install the scanned snapshot so future statements probe it instead of
    // rescanning the table. Failure only leaves the slow path in place — the
    // statement itself is unaffected.
    let columns = indices
        .iter()
        .map(|&idx| schema.columns[idx].name.clone())
        .collect::<Vec<_>>();
    if let Err(error) = engine.index_manager().persist_unique_index_entries(
        db,
        table,
        index_name,
        &columns,
        entries,
        Some(engine.segment_mgr().memtable_flush_epoch(db, table)),
        None,
    ) {
        tracing::warn!(
            db,
            table,
            index = %index_name,
            error = %error,
            "failed to install unique-index snapshot after fallback scan"
        );
    }
    Ok(())
}

fn unique_key_for_row(
    batch: &RecordBatch,
    row_idx: usize,
    indices: &[usize],
    schema: &Schema,
) -> Option<Vec<ScalarValue>> {
    let mut key = Vec::with_capacity(indices.len());
    for &col_idx in indices {
        let value = batch
            .scalar(row_idx, col_idx)
            .unwrap_or(ScalarValue::null_for_type(
                schema.columns[col_idx].data_type,
            ));
        if value.is_null() {
            return None;
        }
        key.push(value);
    }
    Some(key)
}

pub fn validate_unique_indexes_in_batches(
    table: &str,
    schema: &Schema,
    unique_indexes: &[crate::storage::catalog::schema::UniqueIndexMeta],
    batches: &[std::sync::Arc<RecordBatch>],
) -> Result<()> {
    for index in unique_indexes {
        let mut indices = Vec::with_capacity(index.columns.len());
        for column in &index.columns {
            indices.push(schema.index_of(column).ok_or_else(|| {
                TaibaiError::Sql(format!("unique index column '{}' not found", column))
            })?);
        }
        let mut keys = HashSet::new();
        for batch in batches {
            for row_idx in 0..batch.num_rows() {
                let Some(key) = unique_key_for_row(batch, row_idx, &indices, schema) else {
                    continue;
                };
                if keys.contains(&key) {
                    return Err(duplicate_unique_key_error(table, &index.name, &key));
                }
                keys.insert(key);
            }
        }
    }
    Ok(())
}

fn duplicate_unique_key_error(table: &str, index_name: &str, key: &[ScalarValue]) -> TaibaiError {
    let key_text = key
        .iter()
        .map(|v| v.to_text().unwrap_or_else(|| "NULL".to_string()))
        .collect::<Vec<_>>()
        .join(", ");
    TaibaiError::Sql(format!(
        "duplicate unique key '{}' for table '{}': ({})",
        index_name, table, key_text
    ))
}

pub(crate) fn primary_key_indices(schema: &Schema) -> Vec<usize> {
    schema
        .columns
        .iter()
        .enumerate()
        .filter(|(_, col)| col.primary_key)
        .map(|(idx, _)| idx)
        .collect()
}

fn generated_auto_increment_key_guarantees_uniqueness(
    primary_key_indices: &[usize],
    is_missing_auto_inc: &[bool],
) -> bool {
    primary_key_indices
        .iter()
        .any(|&idx| is_missing_auto_inc.get(idx).copied().unwrap_or(false))
}

fn validate_primary_key_values_present(
    batch: &RecordBatch,
    schema: &Schema,
    primary_key_indices: &[usize],
) -> Result<()> {
    for row_idx in 0..batch.num_rows() {
        primary_key_for_row(batch, row_idx, primary_key_indices, schema)?;
    }
    Ok(())
}

pub(crate) fn primary_keys_for_batch(
    batch: &RecordBatch,
    schema: &Schema,
    primary_key_indices: &[usize],
) -> Result<Vec<Vec<ScalarValue>>> {
    (0..batch.num_rows())
        .map(|row_idx| primary_key_for_row(batch, row_idx, primary_key_indices, schema))
        .collect()
}

pub fn validate_primary_key_uniqueness(
    engine: &Engine,
    db: &str,
    table: &str,
    batch: &RecordBatch,
    schema: &Schema,
    primary_key_indices: &[usize],
) -> Result<()> {
    let keys = primary_keys_for_batch(batch, schema, primary_key_indices)?;
    let mut unique = HashSet::with_capacity(keys.len());
    for key in &keys {
        if !unique.insert(key.clone()) {
            return Err(duplicate_primary_key_error(table, key));
        }
    }

    if let Some(exists) = engine.index_manager().unique_keys_exist(
        db,
        table,
        crate::storage::index::index_manager::PRIMARY_LOCATION_INDEX_NAME,
        &keys,
    )? {
        if exists {
            return Err(TaibaiError::Sql(format!(
                "duplicate primary key for table '{}'",
                table
            )));
        }
        return Ok(());
    }

    if let Some(exists) = engine
        .index_manager()
        .primary_keys_exist(db, table, &keys)?
    {
        if exists {
            return Err(TaibaiError::Sql(format!(
                "duplicate primary key for table '{}'",
                table
            )));
        }
        return Ok(());
    }

    let entries = collect_unique_index_entries(
        engine,
        db,
        table,
        primary_key_indices,
        "primary key uniqueness fallback",
    )?;
    let historical_keys = entries
        .into_iter()
        .map(|entry| entry.key)
        .collect::<Vec<_>>();
    if historical_keys.iter().any(|key| unique.contains(key)) {
        return Err(TaibaiError::Sql(format!(
            "duplicate primary key for table '{}'",
            table
        )));
    }
    engine
        .index_manager()
        .install_primary_key_index(db, table, historical_keys)?;
    Ok(())
}

fn validate_primary_key_uniqueness_in_batch(
    table: &str,
    batch: &RecordBatch,
    schema: &Schema,
    primary_key_indices: &[usize],
) -> Result<()> {
    let mut keys = HashSet::new();
    for row_idx in 0..batch.num_rows() {
        let key = primary_key_for_row(batch, row_idx, primary_key_indices, schema)?;
        if keys.contains(&key) {
            return Err(duplicate_primary_key_error(table, &key));
        }
        keys.insert(key);
    }
    Ok(())
}

fn advance_explicit_auto_increment_counters(
    engine: &Engine,
    db: &str,
    table: &str,
    batch: &RecordBatch,
    auto_inc_cols: &[usize],
    missing_auto_inc: &[usize],
) -> Result<()> {
    for &col_idx in auto_inc_cols {
        if missing_auto_inc.contains(&col_idx) {
            continue;
        }

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
            max_value = Some(max_value.map_or(unsigned, |current: u64| current.max(unsigned)));
        }

        if let Some(value) = max_value {
            engine
                .auto_increment_manager()
                .advance_past(db, table, value)?;
        }
    }
    Ok(())
}

fn primary_key_for_row(
    batch: &RecordBatch,
    row_idx: usize,
    primary_key_indices: &[usize],
    schema: &Schema,
) -> Result<Vec<ScalarValue>> {
    let mut key = Vec::with_capacity(primary_key_indices.len());
    for &col_idx in primary_key_indices {
        let value = batch
            .scalar(row_idx, col_idx)
            .unwrap_or(ScalarValue::null_for_type(
                schema.columns[col_idx].data_type,
            ));
        if value.is_null() {
            return Err(TaibaiError::Sql(format!(
                "primary key column '{}' cannot be NULL",
                schema.columns[col_idx].name
            )));
        }
        key.push(value);
    }
    Ok(key)
}

fn duplicate_primary_key_error(table: &str, key: &[ScalarValue]) -> TaibaiError {
    let key_text = key
        .iter()
        .map(|v| v.to_text().unwrap_or_else(|| "NULL".to_string()))
        .collect::<Vec<_>>()
        .join(", ");
    TaibaiError::Sql(format!(
        "duplicate primary key for table '{}': ({})",
        table, key_text
    ))
}
