use crate::common::error::{MonaxError, Result};
use crate::storage::segment_manager::SegmentManager;
use crate::types::batch::Schema;

use super::engine::{Engine, ExecResult};
use super::helpers::{
    build_row_group_predicate, collect_expr_column_indices, eval_where_clause_batch,
};
use crate::sql::parser::ast::Expr;

pub fn exec_delete(
    engine: &Engine,
    db: &str,
    table: &str,
    where_clause: &Option<Expr>,
) -> Result<ExecResult> {
    let table_schema = {
        let catalogs = engine.catalogs().read().unwrap();
        let catalog = catalogs
            .get(db)
            .ok_or_else(|| MonaxError::Sql(format!("database '{}' does not exist", db)))?;
        let meta = catalog
            .get_table(table)
            .ok_or_else(|| MonaxError::Sql(format!("table '{}.{}' does not exist", db, table)))?
            .clone();
        Schema::new(meta.columns.clone())
    };

    let table_dir = engine.data_dir().join(db).join(table);
    if !table_dir.exists() {
        return Ok(ExecResult::Ok { rows_affected: 0 });
    }

    let mgr = engine.segment_mgr();
    let mut total_deleted: u64 = 0;
    let predicate = build_row_group_predicate(where_clause, &table_schema);
    let predicate_projection = where_clause
        .as_ref()
        .map(|expr| collect_expr_column_indices(expr, &table_schema))
        .unwrap_or_default();

    mgr.with_table_mutation_lock(db, table, || {
        let segment_files = SegmentManager::discover_segments(&table_dir)?;
        for seg_path in segment_files {
            let (new_bitmap, new_deleted_count, prev_deleted) = {
                let reader = mgr.open_reader(&seg_path)?;
                let total_rows = reader.row_count();
                if total_rows == 0 {
                    continue;
                }
                let prev_bitmap = reader.delete_bitmap().to_vec();
                let prev_deleted = reader.deleted_row_count();
                let bitmap_bytes = ((total_rows + 7) / 8) as usize;
                let mut new_bitmap = vec![0u8; bitmap_bytes];
                if !prev_bitmap.is_empty() {
                    let copy_len = prev_bitmap.len().min(bitmap_bytes);
                    new_bitmap[..copy_len].copy_from_slice(&prev_bitmap[..copy_len]);
                }

                let mut newly_deleted: u64 = 0;
                let mut row_id: u64 = 0;
                for rg_idx in 0..reader.num_row_groups() {
                    let rg_rows = reader.row_group_rows(rg_idx);
                    if row_range_all_deleted(&new_bitmap, row_id, rg_rows) {
                        row_id += rg_rows;
                        continue;
                    }

                    if let Some(ref pred) = predicate {
                        if !pred.matches_row_group(reader.zone_maps(rg_idx)) {
                            row_id += rg_rows;
                            continue;
                        }
                    }

                    if let Some(ref where_expr) = where_clause {
                        let batch = if predicate_projection.is_empty() {
                            reader.read_row_group(rg_idx)?
                        } else {
                            reader.read_row_group_with_projection(rg_idx, &predicate_projection)?
                        };
                        let mask = eval_where_clause_batch(where_expr, &batch, &batch.schema)?;
                        for (i, matches) in mask.into_iter().enumerate() {
                            if matches && mark_deleted(&mut new_bitmap, row_id + i as u64) {
                                newly_deleted += 1;
                            }
                        }
                        row_id += batch.num_rows() as u64;
                    } else {
                        for i in 0..rg_rows {
                            if mark_deleted(&mut new_bitmap, row_id + i) {
                                newly_deleted += 1;
                            }
                        }
                        row_id += rg_rows;
                    }
                }

                let new_deleted_count = prev_deleted + newly_deleted;
                total_deleted += newly_deleted;
                (new_bitmap, new_deleted_count, prev_deleted)
            };

            if new_deleted_count == prev_deleted {
                continue;
            }

            mgr.rewrite_delete_footer(&seg_path, &new_bitmap, new_deleted_count)?;
        }
        Ok(())
    })?;

    if total_deleted > 0 {
        let mut catalogs = engine.catalogs().write().unwrap();
        if let Some(catalog) = catalogs.get_mut(db) {
            if let Some(meta) = catalog.get_table_mut(table) {
                meta.row_count = (meta.row_count - total_deleted as i64).max(0);
                let meta_dir = engine.data_dir().join("meta");
                catalog.save(&meta_dir)?;
            }
        }
    }

    Ok(ExecResult::Ok {
        rows_affected: total_deleted,
    })
}

fn row_range_all_deleted(bitmap: &[u8], start_row: u64, row_count: u64) -> bool {
    (0..row_count).all(|i| is_deleted(bitmap, start_row + i))
}

fn is_deleted(bitmap: &[u8], row_id: u64) -> bool {
    let byte = (row_id / 8) as usize;
    let bit = (row_id % 8) as u8;
    bitmap.get(byte).map_or(false, |b| (b & (1u8 << bit)) != 0)
}

fn mark_deleted(bitmap: &mut [u8], row_id: u64) -> bool {
    let byte = (row_id / 8) as usize;
    let bit = (row_id % 8) as u8;
    let Some(slot) = bitmap.get_mut(byte) else {
        return false;
    };
    let mask = 1u8 << bit;
    if (*slot & mask) != 0 {
        return false;
    }
    *slot |= mask;
    true
}
