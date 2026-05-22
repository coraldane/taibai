use crate::common::error::{MonaxError, Result};
use crate::storage::segment_manager::SegmentManager;
use crate::types::batch::Schema;

use super::engine::{Engine, ExecResult};
use super::helpers::eval_where_clause;
use crate::sql::parser::ast::Expr;

pub fn exec_delete(
    engine: &Engine,
    db: &str,
    table: &str,
    where_clause: &Option<Expr>,
) -> Result<ExecResult> {
    let table_schema = {
        let catalogs = engine.catalogs().read().unwrap();
        let catalog = catalogs.get(db)
            .ok_or_else(|| MonaxError::Sql(format!("database '{}' does not exist", db)))?;
        let meta = catalog.get_table(table)
            .ok_or_else(|| MonaxError::Sql(format!("table '{}.{}' does not exist", db, table)))?
            .clone();
        Schema::new(meta.columns.clone())
    };

    let table_dir = engine.data_dir().join(db).join(table);
    if !table_dir.exists() {
        return Ok(ExecResult::Ok { rows_affected: 0 });
    }

    let mgr = engine.segment_mgr();
    let segment_files = SegmentManager::discover_segments(&table_dir)?;
    let mut total_deleted: u64 = 0;

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
                let batch = reader.read_row_group(rg_idx)?;
                for i in 0..batch.num_rows() {
                    let rid = row_id + i as u64;
                    let byte = (rid / 8) as usize;
                    let bit = (rid % 8) as u8;
                    let already = (new_bitmap[byte] & (1u8 << bit)) != 0;
                    if already { continue; }
                    let matches = if let Some(ref where_expr) = where_clause {
                        eval_where_clause(where_expr, &batch, i, &table_schema)?
                    } else {
                        true
                    };
                    if matches {
                        new_bitmap[byte] |= 1u8 << bit;
                        newly_deleted += 1;
                    }
                }
                row_id += batch.num_rows() as u64;
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

    Ok(ExecResult::Ok { rows_affected: total_deleted })
}
