use crate::common::error::{MonaxError, Result};
use crate::storage::index::sort_key::SortKeyInserter;
use crate::types::batch::{RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

use super::engine::{Engine, ExecResult};
use super::helpers::{sort_batch_by_key, eval_expr_to_scalar_with_scale_timezone};
use crate::sql::parser::ast::Expr;

pub fn exec_insert(
    engine: &Engine,
    db: &str,
    table: &str,
    columns: &[String],
    values: &[Vec<Expr>],
    timezone_offset_hours: i32,
    txn_id: u64,
) -> Result<ExecResult> {
    let catalogs = engine.catalogs().read().unwrap();
    let catalog = catalogs.get(db)
        .ok_or_else(|| MonaxError::Sql(format!("database '{}' does not exist", db)))?;

    let table_meta = catalog.get_table(table)
        .ok_or_else(|| MonaxError::Sql(format!("table '{}.{}' does not exist", db, table)))?
        .clone();

    let schema = Schema::new(table_meta.columns.clone());

    let auto_inc_cols: Vec<usize> = schema.columns.iter()
        .enumerate()
        .filter(|(_, c)| c.auto_increment)
        .map(|(i, _)| i)
        .collect();

    let col_indices: Vec<usize> = if columns.is_empty() {
        (0..schema.columns.len()).collect()
    } else {
        columns.iter().map(|c| {
            schema.index_of(c)
                .ok_or_else(|| MonaxError::Sql(format!("column '{}' not found", c)))
        }).collect::<Result<Vec<_>>>()?
    };

    let missing_auto_inc: Vec<usize> = auto_inc_cols.iter()
        .filter(|&idx| !col_indices.contains(idx))
        .cloned()
        .collect();

    let auto_inc_counter = if !missing_auto_inc.is_empty() {
        engine.auto_increment_manager().get(db, table)
    } else {
        None
    };

    let mut batch = RecordBatch::new(schema.clone());
    
    for row in values {
        for col_idx in 0..schema.columns.len() {
            let is_auto_inc = missing_auto_inc.contains(&col_idx);
            let provided_idx = col_indices.iter().position(|&i| i == col_idx);
            
            if is_auto_inc && provided_idx.is_none() {
                let next_val = match &auto_inc_counter {
                    Some(counter) => counter.next_value(),
                    None => engine.auto_increment_manager().next_value(db, table)?,
                };
                let scalar = match schema.columns[col_idx].data_type {
                    crate::protocol::types::DataType::Int32 => ScalarValue::Int32(Some(next_val as i32)),
                    crate::protocol::types::DataType::Int64 => ScalarValue::Int64(Some(next_val as i64)),
                    crate::protocol::types::DataType::UInt32 => ScalarValue::UInt32(Some(next_val as u32)),
                    crate::protocol::types::DataType::UInt64 => ScalarValue::UInt64(Some(next_val)),
                    _ => ScalarValue::Int64(Some(next_val as i64)),
                };
                batch.column_mut(col_idx).unwrap().push(&scalar)?;
            } else if let Some(val_idx) = provided_idx {
                let scalar = eval_expr_to_scalar_with_scale_timezone(
                    &row[val_idx], 
                    &schema.columns[col_idx].data_type, 
                    schema.columns[col_idx].scale, 
                    timezone_offset_hours
                )?;
                batch.column_mut(col_idx).unwrap().push(&scalar)?;
            } else {
                let scalar = ScalarValue::null_for_type(schema.columns[col_idx].data_type);
                batch.column_mut(col_idx).unwrap().push(&scalar)?;
            }
        }
    }

    drop(catalogs);

    let has_sort_key = !table_meta.cluster_key.is_empty();
    if has_sort_key {
        let key_indices: Vec<usize> = table_meta.cluster_key.iter()
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

    if engine.is_bulk_mode() {
        engine.exec_bulk_insert_with_wal_control(db, table, batch, false)?;
    } else {
        let partition_key = determine_partition_key(&table_meta, &batch);
        engine.batch_writer().write_columns_with_txn(db, table, &partition_key, batch, schema, txn_id)?;
    }

    {
        let mut catalogs = engine.catalogs().write().unwrap();
        if let Some(catalog) = catalogs.get_mut(db) {
            if let Some(meta) = catalog.get_table_mut(table) {
                meta.row_count += values.len() as i64;
            }
            if !engine.is_bulk_mode() {
                let meta_dir = engine.data_dir().join("meta");
                catalog.save(&meta_dir)?;
            }
        }
    }

    Ok(ExecResult::Ok { rows_affected: row_count })
}

fn determine_partition_key(
    table_meta: &crate::storage::catalog::schema::TableMeta,
    batch: &RecordBatch,
) -> String {
    if table_meta.partition_key.is_empty() {
        return String::new();
    }
    let col_idx = match table_meta.get_partition_column_index_cached() {
        Some(idx) => idx,
        None => return String::new(),
    };
    if batch.num_rows() == 0 {
        return String::new();
    }
    let val = batch.scalar(0, col_idx)
        .unwrap_or(crate::types::scalar::ScalarValue::Int64(None));

    use crate::storage::catalog::schema::PartitionType;
    match table_meta.partition_type {
        PartitionType::Day | PartitionType::Year | PartitionType::Month | PartitionType::Hash => {
            crate::storage::partition::partition_function::compute_partition_key_from_scalar(
                &table_meta.partition_strategy(),
                &val,
            )
        }
        PartitionType::Value => val.to_text().unwrap_or_default(),
        PartitionType::None => String::new(),
    }
}
