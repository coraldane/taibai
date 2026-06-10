use crate::common::error::{TaibaiError, Result};
use crate::storage::segment_manager::SegmentManager;
use crate::types::batch::RecordBatch;
use crate::types::batch::Schema;
use crate::types::scalar::ScalarValue;

use super::engine::{change_row_from_images, Engine, ExecResult, MAX_CHANGE_ROWS_PER_EVENT};
use super::helpers::{
    build_row_group_predicate, collect_expr_column_indices, eval_expr_to_scalar,
    eval_where_clause_batch,
};
use crate::sql::parser::ast::{Assignment, Expr};

pub fn exec_update(
    engine: &Engine,
    db: &str,
    table: &str,
    assignments: &[Assignment],
    where_clause: &Option<Expr>,
    change_rows: &mut Vec<crate::storage::change_stream::ChangeRow>,
    row_images_truncated: &mut bool,
) -> Result<ExecResult> {
    let (table_schema, _table_meta) = {
        let catalogs = engine.catalogs().read().unwrap();
        let catalog = catalogs
            .get(db)
            .ok_or_else(|| TaibaiError::Sql(format!("database '{}' does not exist", db)))?;
        let meta = catalog
            .get_table(table)
            .ok_or_else(|| TaibaiError::Sql(format!("table '{}.{}' does not exist", db, table)))?
            .clone();
        (Schema::new(meta.columns.clone()), meta)
    };

    let table_dir = engine.data_dir().join(db).join(table);
    let mgr = engine.segment_mgr();
    let predicate = build_row_group_predicate(where_clause, &table_schema);
    let predicate_projection = where_clause
        .as_ref()
        .map(|expr| collect_expr_column_indices(expr, &table_schema))
        .unwrap_or_default();

    engine.flush_all_buffers()?;

    let updated_count = mgr.with_table_mutation_lock(db, table, || {
        if !table_dir.exists() {
            return Ok(0);
        }

        let segment_files = SegmentManager::discover_segments(&table_dir)?;
        if segment_files.is_empty() {
            return Ok(0);
        }

        let mut updated_count = 0u64;
        let mut rewritten_segments = Vec::new();

        for seg_path in segment_files {
            let reader = mgr.open_reader(&seg_path)?;
            if !segment_may_match(&reader, predicate.as_ref()) {
                continue;
            }

            let mut segment_updated = false;
            let mut row_group_updates = Vec::new();

            for rg_idx in 0..reader.num_row_groups() {
                if let Some(ref pred) = predicate {
                    if !pred.matches_row_group(reader.zone_maps(rg_idx)) {
                        row_group_updates.push((rg_idx, None));
                        continue;
                    }
                }

                let mask = build_update_mask(
                    &reader,
                    rg_idx,
                    where_clause,
                    &predicate_projection,
                    &table_schema,
                )?;
                if !mask.iter().any(|m| *m) {
                    row_group_updates.push((rg_idx, None));
                    continue;
                }

                segment_updated = true;
                let batch = reader.read_row_group(rg_idx)?;
                let mut new_batch = RecordBatch::new(batch.schema.clone());
                for (row_idx, matches) in mask.into_iter().enumerate() {
                    for col_idx in 0..batch.num_columns() {
                        let val = if matches {
                            updated_value_for_column(
                                assignments,
                                &batch,
                                row_idx,
                                col_idx,
                                &table_schema,
                            )?
                        } else {
                            batch
                                .scalar(row_idx, col_idx)
                                .unwrap_or(ScalarValue::null_for_type(
                                    table_schema.columns[col_idx].data_type,
                                ))
                        };
                        new_batch.column_mut(col_idx).unwrap().push(&val)?;
                    }
                    if matches {
                        updated_count += 1;
                        if change_rows.len() < MAX_CHANGE_ROWS_PER_EVENT {
                            change_rows.push(change_row_from_images(
                                &table_schema,
                                Some(&batch),
                                row_idx,
                                Some(&new_batch),
                                row_idx,
                            ));
                        } else {
                            *row_images_truncated = true;
                        }
                    }
                }
                row_group_updates.push((rg_idx, Some(new_batch)));
            }

            if segment_updated {
                let mut new_batches = Vec::new();
                for (rg_idx, batch) in row_group_updates {
                    match batch {
                        Some(batch) => new_batches.push(batch),
                        None => new_batches.push(reader.read_row_group(rg_idx)?),
                    }
                }
                rewritten_segments.push((seg_path, new_batches));
            }
        }

        if rewritten_segments.is_empty() {
            return Ok(0);
        }

        mgr.clear_active_segments(db, table);
        for (old_seg, batches) in rewritten_segments {
            mgr.remove_segment_file(&old_seg)?;
            for batch in batches {
                let seg_path = mgr.next_segment_path(&table_dir, "")?;
                mgr.write_new_segment(&seg_path, batch.schema.clone(), 8192, &batch)?;
            }
        }

        {
            let _ = mgr.clear_wal(&table_dir);
        }

        Ok(updated_count)
    })?;

    if updated_count == 0 {
        return Ok(ExecResult::Ok { rows_affected: 0 });
    }

    let mut catalogs = engine.catalogs().write().unwrap();
    if let Some(catalog) = catalogs.get_mut(db) {
        if let Some(_meta) = catalog.get_table_mut(table) {
            let meta_dir = engine.data_dir().join("meta");
            catalog.save(&meta_dir)?;
        }
    }

    Ok(ExecResult::Ok {
        rows_affected: updated_count,
    })
}

fn build_update_mask(
    reader: &crate::storage::segment::SegmentReader,
    row_group_idx: usize,
    where_clause: &Option<Expr>,
    predicate_projection: &[usize],
    table_schema: &Schema,
) -> Result<Vec<bool>> {
    let Some(where_expr) = where_clause.as_ref() else {
        return Ok(vec![true; reader.row_group_rows(row_group_idx) as usize]);
    };
    let predicate_batch = if predicate_projection.is_empty() {
        reader.read_row_group(row_group_idx)?
    } else {
        reader.read_row_group_with_projection(row_group_idx, predicate_projection)?
    };
    eval_where_clause_batch(where_expr, &predicate_batch, &predicate_batch.schema)
        .or_else(|_| eval_where_clause_batch(where_expr, &predicate_batch, table_schema))
}

fn segment_may_match(
    reader: &crate::storage::segment::SegmentReader,
    predicate: Option<&crate::storage::segment::RowGroupPredicate>,
) -> bool {
    let Some(predicate) = predicate else {
        return true;
    };
    let segment_zone_maps = reader.segment_zone_maps();
    if !segment_zone_maps.is_empty() {
        return predicate.matches_row_group(segment_zone_maps);
    }
    (0..reader.num_row_groups()).any(|rg_idx| predicate.matches_row_group(reader.zone_maps(rg_idx)))
}

fn updated_value_for_column(
    assignments: &[Assignment],
    batch: &RecordBatch,
    row_idx: usize,
    col_idx: usize,
    table_schema: &Schema,
) -> Result<ScalarValue> {
    let col_name = &table_schema.columns[col_idx].name;
    if let Some(assign) = assignments.iter().find(|a| &a.column == col_name) {
        return eval_expr_to_scalar(&assign.value, &table_schema.columns[col_idx].data_type);
    }
    Ok(batch
        .scalar(row_idx, col_idx)
        .unwrap_or(ScalarValue::null_for_type(
            table_schema.columns[col_idx].data_type,
        )))
}
