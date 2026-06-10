use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use crate::common::error::{TaibaiError, Result};
use crate::protocol::types::DataType;
use crate::storage::index::sort_key::SortKeyInserter;
use crate::storage::segment_manager::SegmentManager;
use crate::types::array::ColumnArray;
use crate::types::batch::{ColumnDefaultValue, RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

use super::engine::{change_row_from_images, Engine, ExecResult, MAX_CHANGE_ROWS_PER_EVENT};
use super::helpers::{eval_expr_to_scalar_with_scale_timezone, sort_batch_by_key};
use crate::sql::parser::ast::Expr;

lazy_static::lazy_static! {
    static ref PRIMARY_KEY_INSERT_LOCK: Mutex<()> = Mutex::new(());
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
    change_rows: &mut Vec<crate::storage::change_stream::ChangeRow>,
    row_images_truncated: &mut bool,
) -> Result<ExecResult> {
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

    if can_direct_materialize(&schema, &provided_by_col, &is_missing_auto_inc) {
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
            for (col_idx, is_auto_inc) in is_missing_auto_inc.iter().enumerate() {
                let provided_idx = provided_by_col[col_idx];

                if *is_auto_inc && provided_idx.is_none() {
                    let next_val = match &auto_inc_counter {
                        Some(counter) => counter.next_value(),
                        None => engine.auto_increment_manager().next_value(db, table)?,
                    };
                    let scalar = match schema.columns[col_idx].data_type {
                        crate::protocol::types::DataType::Int32 => {
                            ScalarValue::Int32(Some(next_val as i32))
                        }
                        crate::protocol::types::DataType::Int64 => {
                            ScalarValue::Int64(Some(next_val as i64))
                        }
                        crate::protocol::types::DataType::UInt32 => {
                            ScalarValue::UInt32(Some(next_val as u32))
                        }
                        crate::protocol::types::DataType::UInt64 => {
                            ScalarValue::UInt64(Some(next_val))
                        }
                        _ => ScalarValue::Int64(Some(next_val as i64)),
                    };
                    batch.column_mut(col_idx).unwrap().push(&scalar)?;
                } else if let Some(val_idx) = provided_idx {
                    let scalar = eval_expr_to_scalar_with_scale_timezone(
                        &row[val_idx],
                        &schema.columns[col_idx].data_type,
                        schema.columns[col_idx].scale,
                        timezone_offset_hours,
                    )?;
                    validate_nullable(&schema, col_idx, &scalar)?;
                    batch.column_mut(col_idx).unwrap().push(&scalar)?;
                } else if let Some(default) = &schema.columns[col_idx].default_value {
                    let scalar = materialize_default_value(
                        default,
                        &schema,
                        col_idx,
                        timezone_offset_hours,
                    )?;
                    validate_nullable(&schema, col_idx, &scalar)?;
                    batch.column_mut(col_idx).unwrap().push(&scalar)?;
                } else {
                    let scalar = ScalarValue::null_for_type(schema.columns[col_idx].data_type);
                    validate_nullable(&schema, col_idx, &scalar)?;
                    batch.column_mut(col_idx).unwrap().push(&scalar)?;
                }
            }
        }
    }

    drop(catalogs);

    let has_sort_key = !table_meta.cluster_key.is_empty();
    if has_sort_key {
        let key_indices: Vec<usize> = table_meta
            .cluster_key
            .iter()
            .filter_map(|k| schema.index_of(k))
            .collect();
        if !key_indices.is_empty() {
            let inserter = SortKeyInserter::new(key_indices, vec![false]);
            if !inserter.is_sorted(&batch) {
                batch = sort_batch_by_key(&batch, &inserter)?;
            }
        }
    }

    let row_count = batch.num_rows() as u64;

    let primary_key_indices = primary_key_indices(&schema);
    let _pk_guard = if primary_key_indices.is_empty() {
        None
    } else {
        Some(PRIMARY_KEY_INSERT_LOCK.lock().unwrap())
    };

    if !primary_key_indices.is_empty() {
        if generated_auto_increment_key_guarantees_uniqueness(
            &primary_key_indices,
            &is_missing_auto_inc,
        ) {
            validate_primary_key_values_present(&batch, &schema, &primary_key_indices)?;
        } else {
            validate_primary_key_uniqueness(
                engine,
                db,
                table,
                &batch,
                &schema,
                &primary_key_indices,
            )?;
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

    for row_idx in 0..batch.num_rows() {
        if change_rows.len() >= MAX_CHANGE_ROWS_PER_EVENT {
            *row_images_truncated = true;
            break;
        }
        change_rows.push(change_row_from_images(
            &schema,
            None,
            0,
            Some(&batch),
            row_idx,
        ));
    }

    if bulk_import_mode {
        engine.exec_bulk_insert_partitioned_with_wal_control(
            db,
            table,
            &table_meta,
            batch,
            false,
        )?;
    } else {
        write_batch_by_partition(engine, db, table, &table_meta, batch, schema, txn_id)?;
    }

    engine.record_inserted_rows(
        db,
        table,
        values.len() as i64,
        !bulk_import_mode && txn_id > 0,
        !bulk_import_mode && txn_id == 0,
    )?;

    Ok(ExecResult::Ok {
        rows_affected: row_count,
    })
}

fn write_batch_by_partition(
    engine: &Engine,
    db: &str,
    table: &str,
    table_meta: &crate::storage::catalog::schema::TableMeta,
    batch: RecordBatch,
    schema: Schema,
    txn_id: u64,
) -> Result<()> {
    if table_meta.partition_key.is_empty() || batch.num_rows() == 0 {
        engine
            .batch_writer()
            .write_columns_with_txn(db, table, "", batch, schema, txn_id)?;
        return Ok(());
    }

    if let Some(partition_key) = single_partition_key(table_meta, &batch) {
        engine.batch_writer().write_columns_with_txn(
            db,
            table,
            &partition_key,
            batch,
            schema,
            txn_id,
        )?;
        return Ok(());
    }

    let groups = partition_masks(table_meta, &batch);
    if groups.len() <= 1 {
        let partition_key = groups
            .first()
            .map(|(key, _)| key.clone())
            .unwrap_or_default();
        engine.batch_writer().write_columns_with_txn(
            db,
            table,
            &partition_key,
            batch,
            schema,
            txn_id,
        )?;
        return Ok(());
    }

    for (partition_key, mask) in groups {
        let match_count = mask.iter().filter(|keep| **keep).count();
        let part_batch = filter_batch_by_mask(&batch, &mask, match_count)?;
        engine.batch_writer().write_columns_with_txn(
            db,
            table,
            &partition_key,
            part_batch,
            schema.clone(),
            txn_id,
        )?;
    }
    Ok(())
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
) -> bool {
    schema.columns.iter().enumerate().all(|(idx, col)| {
        provided_by_col[idx].is_some()
            && !is_missing_auto_inc[idx]
            && col.default_value.is_none()
            && col.nullable
            && !col.primary_key
    })
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
                let batch = reader.read_row_group_with_projection(rg_idx, primary_key_indices)?;
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
