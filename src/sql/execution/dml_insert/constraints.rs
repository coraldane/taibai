fn unique_constraints(
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
            if !keys.insert(key.clone()) {
                return Err(duplicate_unique_key_error(table, name, &key));
            }
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
            if !keys.insert(key.clone()) {
                return Err(duplicate_unique_key_error(table, index_name, &key));
            }
        }
    }
    if new_batch.is_some() {
        if let Some(data) = engine
            .index_manager()
            .load_unique_index_data(db, table, index_name)?
        {
            let index_columns = indices
                .iter()
                .map(|idx| schema.columns[*idx].name.clone())
                .collect::<Vec<_>>();
            if data.columns == index_columns {
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
        }
    }
    load_existing_unique_keys(engine, db, table, indices, index_name, &mut keys)
}

pub fn collect_unique_index_entries(
    engine: &Engine,
    db: &str,
    table: &str,
    indices: &[usize],
    index_name: &str,
) -> Result<Vec<UniqueIndexEntry>> {
    let mut keys = HashSet::new();
    let mut entries = Vec::new();
    let table_dir = engine.data_dir().join(db).join(table);
    if table_dir.exists() {
        for seg_path in SegmentManager::discover_segments(&table_dir)? {
            let reader = engine.segment_mgr().open_reader_cached(&seg_path)?;
            for rg_idx in 0..reader.num_row_groups() {
                let mut batch = reader.read_row_group_with_projection(rg_idx, indices)?;
                let rg_start = reader.row_group_start_row(rg_idx);
                if !reader.delete_bitmap().is_empty() {
                    batch = crate::storage::segment::delete_bitmap::filter_batch(
                        &batch,
                        rg_start,
                        reader.delete_bitmap(),
                    )?;
                }
                let projected_indices: Vec<usize> = (0..batch.num_columns()).collect();
                for row_idx in 0..batch.num_rows() {
                    let Some(key) =
                        unique_key_for_row(&batch, row_idx, &projected_indices, &batch.schema)
                    else {
                        continue;
                    };
                    if !keys.insert(key.clone()) {
                        return Err(duplicate_unique_key_error(table, index_name, &key));
                    }
                    entries.push(UniqueIndexEntry {
                        key,
                        location: UniqueRowLocation {
                            segment_path: seg_path.to_string_lossy().to_string(),
                            row_id: rg_start + row_idx as u64,
                        },
                    });
                }
            }
        }
    }
    for memtable in engine.memtable_manager().get_by_table(db, table) {
        if let Some(batch) = memtable.read() {
            for row_idx in 0..batch.num_rows() {
                let Some(key) = unique_key_for_row(&batch, row_idx, indices, &batch.schema) else {
                    continue;
                };
                if !keys.insert(key.clone()) {
                    return Err(duplicate_unique_key_error(table, index_name, &key));
                }
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
    Ok(entries)
}

fn load_existing_unique_keys(
    engine: &Engine,
    db: &str,
    table: &str,
    indices: &[usize],
    index_name: &str,
    inserted_keys: &mut HashSet<Vec<ScalarValue>>,
) -> Result<()> {
    let table_dir = engine.data_dir().join(db).join(table);
    if table_dir.exists() {
        for seg_path in SegmentManager::discover_segments(&table_dir)? {
            let reader = engine.segment_mgr().open_reader_cached(&seg_path)?;
            for rg_idx in 0..reader.num_row_groups() {
                let mut batch = reader.read_row_group_with_projection(rg_idx, indices)?;
                if !reader.delete_bitmap().is_empty() {
                    batch = crate::storage::segment::delete_bitmap::filter_batch(
                        &batch,
                        reader.row_group_start_row(rg_idx),
                        reader.delete_bitmap(),
                    )?;
                }
                check_existing_unique_batch(table, index_name, &batch, inserted_keys)?;
            }
        }
    }
    for memtable in engine.memtable_manager().get_by_table(db, table) {
        if let Some(batch) = memtable.read() {
            check_existing_unique_batch_with_indices(
                table,
                index_name,
                &batch,
                indices,
                inserted_keys,
            )?;
        }
    }
    Ok(())
}

fn check_existing_unique_batch(
    table: &str,
    index_name: &str,
    batch: &RecordBatch,
    inserted_keys: &mut HashSet<Vec<ScalarValue>>,
) -> Result<()> {
    let projected_indices: Vec<usize> = (0..batch.num_columns()).collect();
    check_existing_unique_batch_with_indices(
        table,
        index_name,
        batch,
        &projected_indices,
        inserted_keys,
    )
}

fn check_existing_unique_batch_with_indices(
    table: &str,
    index_name: &str,
    batch: &RecordBatch,
    indices: &[usize],
    inserted_keys: &mut HashSet<Vec<ScalarValue>>,
) -> Result<()> {
    for row_idx in 0..batch.num_rows() {
        let Some(key) = unique_key_for_row(batch, row_idx, indices, &batch.schema) else {
            continue;
        };
        if !inserted_keys.insert(key.clone()) {
            return Err(duplicate_unique_key_error(table, index_name, &key));
        }
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
    batches: &[RecordBatch],
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
                if !keys.insert(key.clone()) {
                    return Err(duplicate_unique_key_error(table, &index.name, &key));
                }
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

fn primary_key_indices(schema: &Schema) -> Vec<usize> {
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

fn validate_primary_key_uniqueness(
    engine: &Engine,
    db: &str,
    table: &str,
    batch: &RecordBatch,
    schema: &Schema,
    primary_key_indices: &[usize],
) -> Result<()> {
    let mut keys = HashSet::new();
    for row_idx in 0..batch.num_rows() {
        let key = primary_key_for_row(batch, row_idx, primary_key_indices, schema)?;
        if !keys.insert(key.clone()) {
            return Err(duplicate_primary_key_error(table, &key));
        }
    }

    load_existing_primary_keys(engine, db, table, primary_key_indices, &mut keys)?;
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
        if !keys.insert(key.clone()) {
            return Err(duplicate_primary_key_error(table, &key));
        }
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

fn load_existing_primary_keys(
    engine: &Engine,
    db: &str,
    table: &str,
    primary_key_indices: &[usize],
    inserted_keys: &mut HashSet<Vec<ScalarValue>>,
) -> Result<()> {
    let table_dir = engine.data_dir().join(db).join(table);
    if table_dir.exists() {
        for seg_path in SegmentManager::discover_segments(&table_dir)? {
            let reader = engine.segment_mgr().open_reader_cached(&seg_path)?;
            for rg_idx in 0..reader.num_row_groups() {
                let mut batch =
                    reader.read_row_group_with_projection(rg_idx, primary_key_indices)?;
                if !reader.delete_bitmap().is_empty() {
                    batch = crate::storage::segment::delete_bitmap::filter_batch(
                        &batch,
                        reader.row_group_start_row(rg_idx),
                        reader.delete_bitmap(),
                    )?;
                }
                check_existing_batch(table, &batch, inserted_keys)?;
            }
        }
    }

    for memtable in engine.memtable_manager().get_by_table(db, table) {
        if let Some(batch) = memtable.read() {
            check_existing_batch_with_indices(table, &batch, primary_key_indices, inserted_keys)?;
        }
    }

    Ok(())
}

fn check_existing_batch(
    table: &str,
    batch: &RecordBatch,
    inserted_keys: &HashSet<Vec<ScalarValue>>,
) -> Result<()> {
    let projected_indices: Vec<usize> = (0..batch.num_columns()).collect();
    check_existing_batch_with_indices(table, batch, &projected_indices, inserted_keys)
}

fn check_existing_batch_with_indices(
    table: &str,
    batch: &RecordBatch,
    primary_key_indices: &[usize],
    inserted_keys: &HashSet<Vec<ScalarValue>>,
) -> Result<()> {
    for row_idx in 0..batch.num_rows() {
        let key = primary_key_for_row(batch, row_idx, primary_key_indices, &batch.schema)?;
        if inserted_keys.contains(&key) {
            return Err(duplicate_primary_key_error(table, &key));
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

fn compute_partition_key_for_value(
    table_meta: &crate::storage::catalog::schema::TableMeta,
    val: &ScalarValue,
) -> String {
    if table_meta.partition_key.is_empty() {
        return String::new();
    }
    use crate::storage::catalog::schema::PartitionType;
    match table_meta.partition_type {
        PartitionType::Day
        | PartitionType::Year
        | PartitionType::Month
        | PartitionType::Hash
        | PartitionType::Mod => {
            crate::storage::partition::partition_function::compute_partition_key_from_scalar(
                &table_meta.partition_strategy(),
                val,
            )
        }
        PartitionType::Value => val.to_text().unwrap_or_default(),
        PartitionType::None => String::new(),
    }
}
