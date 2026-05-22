use crate::common::error::{MonaxError, Result};
use crate::storage::segment_manager::SegmentManager;
use crate::types::batch::RecordBatch;
use crate::types::batch::Schema;
use crate::types::scalar::ScalarValue;

use super::engine::{Engine, ExecResult};
use super::helpers::{eval_where_clause, eval_expr_to_scalar};
use crate::sql::parser::ast::{Assignment, Expr};

pub fn exec_update(
    engine: &Engine,
    db: &str,
    table: &str,
    assignments: &[Assignment],
    where_clause: &Option<Expr>,
) -> Result<ExecResult> {
    let (table_schema, _table_meta) = {
        let catalogs = engine.catalogs().read().unwrap();
        let catalog = catalogs.get(db)
            .ok_or_else(|| MonaxError::Sql(format!("database '{}' does not exist", db)))?;
        let meta = catalog.get_table(table)
            .ok_or_else(|| MonaxError::Sql(format!("table '{}.{}' does not exist", db, table)))?
            .clone();
        (Schema::new(meta.columns.clone()), meta)
    };

    let table_dir = engine.data_dir().join(db).join(table);
    let mgr = engine.segment_mgr();
    let mut all_batches = Vec::new();
    if table_dir.exists() {
        let segment_files = SegmentManager::discover_segments(&table_dir)?;
        for seg_path in segment_files {
            let reader = mgr.open_reader(&seg_path)?;
            let batches = reader.scan(4096)?;
            all_batches.extend(batches);
        }
    }

    if all_batches.is_empty() {
        return Ok(ExecResult::Ok { rows_affected: 0 });
    }

    let mut updated_count = 0u64;
    let mut new_batches = Vec::new();
    for batch in all_batches {
        let mut new_batch = RecordBatch::new(batch.schema.clone());
        for row_idx in 0..batch.num_rows() {
            let matches = if let Some(ref where_expr) = where_clause {
                eval_where_clause(where_expr, &batch, row_idx, &table_schema)?
            } else {
                true
            };

            if matches {
                for col_idx in 0..batch.num_columns() {
                    let col_name = &table_schema.columns[col_idx].name;
                    let is_assigned = assignments.iter().any(|a| &a.column == col_name);
                    let val = if is_assigned {
                        let assign = assignments.iter().find(|a| &a.column == col_name).unwrap();
                        eval_expr_to_scalar(&assign.value, &table_schema.columns[col_idx].data_type)?
                    } else {
                        batch.scalar(row_idx, col_idx)
                            .unwrap_or(ScalarValue::null_for_type(table_schema.columns[col_idx].data_type))
                    };
                    new_batch.column_mut(col_idx).unwrap().push(&val)?;
                }
                updated_count += 1;
            } else {
                for col_idx in 0..batch.num_columns() {
                    let val = batch.scalar(row_idx, col_idx)
                        .unwrap_or(ScalarValue::null_for_type(table_schema.columns[col_idx].data_type));
                    new_batch.column_mut(col_idx).unwrap().push(&val)?;
                }
            }
        }
        if new_batch.num_rows() > 0 {
            new_batches.push(new_batch);
        }
    }

    if table_dir.exists() {
        for seg_file in SegmentManager::discover_segments(&table_dir)? {
            std::fs::remove_file(&seg_file)?;
        }
    }

    for batch in new_batches {
        let seg_path = mgr.next_segment_path(&table_dir, "")?;
        mgr.write_new_segment(&seg_path, batch.schema.clone(), 8192, &batch)?;
    }

    {
        let _ = mgr.clear_wal(&table_dir);
    }

    let mut catalogs = engine.catalogs().write().unwrap();
    if let Some(catalog) = catalogs.get_mut(db) {
        if let Some(_meta) = catalog.get_table_mut(table) {
            let meta_dir = engine.data_dir().join("meta");
            catalog.save(&meta_dir)?;
        }
    }

    Ok(ExecResult::Ok { rows_affected: updated_count })
}
