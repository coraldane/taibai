use crate::common::error::{Result, TaibaiError};
use crate::storage::segment_manager::SegmentManager;
use crate::types::batch::RecordBatch;
use crate::types::batch::Schema;
use crate::types::scalar::ScalarValue;
use std::sync::Arc;

use super::engine::{Engine, ExecResult};
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
    reader_txn_id: u64,
) -> Result<ExecResult> {
    let (table_schema, table_meta) = {
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
    if table_meta.is_partitioned()
        && assignments.iter().any(|assignment| {
            assignment
                .column
                .eq_ignore_ascii_case(&table_meta.partition_key)
        })
    {
        return Err(TaibaiError::Sql(format!(
            "updating partition key '{}' is not supported; use DELETE followed by INSERT",
            table_meta.partition_key
        )));
    }
    let predicate = build_row_group_predicate(where_clause, &table_schema);
    let predicate_projection = where_clause
        .as_ref()
        .map(|expr| collect_expr_column_indices(expr, &table_schema))
        .unwrap_or_default();
    let target_partition_key =
        super::dml_delete::delete_partition_eq_key(where_clause, &table_schema, &table_meta);

    let updated_count = mgr.with_table_mutation_lock(db, table, || {
        let mut updated_count = 0u64;
        let mut memtable_updates = Vec::new();
        for memtable in engine
            .memtable_manager()
            .get_visible_to(db, table, reader_txn_id)
        {
            let Some(batch) = memtable.read_snapshot() else {
                continue;
            };
            let (new_batch, affected) =
                update_memtable_batch(assignments, where_clause, &table_schema, &batch)?;
            if affected > 0 {
                updated_count += affected;
                memtable_updates.push((memtable, new_batch));
            }
        }

        // A background memtable flush may have produced an active segment.
        // Seal only this table's active writers before replacing segment files;
        // pending memtables remain resident and are not flushed here.
        if let Some(partition_key) = target_partition_key.as_deref() {
            mgr.seal_partition_active_under_table_lock(db, table, partition_key)?;
        } else {
            mgr.seal_table_active_under_table_lock(db, table)?;
        }

        let segment_files = if table_dir.exists() {
            super::dml_delete::prune_segments_by_partition(
                SegmentManager::discover_segments(&table_dir)?,
                where_clause,
                &table_schema,
                &table_meta,
            )
        } else {
            Vec::new()
        };
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
                    let absolute_row = reader.row_group_start_row(rg_idx) + row_idx as u64;
                    if crate::storage::segment::delete_bitmap::is_deleted(
                        reader.delete_bitmap(),
                        absolute_row,
                    ) {
                        continue;
                    }
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
                    }
                }
                row_group_updates.push((rg_idx, Some(new_batch)));
            }

            if segment_updated {
                let mut new_batches = Vec::new();
                for (rg_idx, batch) in row_group_updates {
                    match batch {
                        Some(batch) => new_batches.push(batch),
                        None => {
                            let mut batch = reader.read_row_group(rg_idx)?;
                            if !reader.delete_bitmap().is_empty() {
                                batch = crate::storage::segment::delete_bitmap::filter_batch(
                                    &batch,
                                    reader.row_group_start_row(rg_idx),
                                    reader.delete_bitmap(),
                                )?;
                            }
                            if batch.num_rows() > 0 {
                                new_batches.push(batch);
                            }
                        }
                    }
                }
                rewritten_segments.push((seg_path, new_batches));
            }
        }

        if rewritten_segments.is_empty() && memtable_updates.is_empty() {
            return Ok(0);
        }

        if !table_meta.unique_indexes.is_empty() {
            let mut validation_batches = Vec::new();
            for seg_path in SegmentManager::discover_segments(&table_dir)? {
                if rewritten_segments
                    .iter()
                    .any(|(old_seg, _)| old_seg == &seg_path)
                {
                    continue;
                }
                let reader = mgr.open_reader(&seg_path)?;
                for rg_idx in 0..reader.num_row_groups() {
                    let mut batch = reader.read_row_group(rg_idx)?;
                    if !reader.delete_bitmap().is_empty() {
                        batch = crate::storage::segment::delete_bitmap::filter_batch(
                            &batch,
                            reader.row_group_start_row(rg_idx),
                            reader.delete_bitmap(),
                        )?;
                    }
                    if batch.num_rows() > 0 {
                        validation_batches.push(batch);
                    }
                }
            }
            for (_, batches) in &rewritten_segments {
                validation_batches.extend(batches.iter().cloned());
            }
            for memtable in engine.memtable_manager().get_by_table(db, table) {
                let batch = memtable_updates
                    .iter()
                    .find(|(updated, _)| Arc::ptr_eq(updated, &memtable))
                    .map(|(_, batch)| batch.clone())
                    .or_else(|| memtable.read_snapshot());
                if let Some(batch) = batch {
                    if batch.num_rows() > 0 {
                        validation_batches.push(batch);
                    }
                }
            }
            super::dml_insert::validate_unique_indexes_in_batches(
                table,
                &table_schema,
                &table_meta.unique_indexes,
                &validation_batches,
            )?;
        }

        for (old_seg, batches) in rewritten_segments {
            let Some(schema) = batches.first().map(|batch| batch.schema.clone()) else {
                mgr.remove_segment_file(&old_seg)?;
                continue;
            };
            let partition_key = old_seg.file_name().and_then(|name| {
                crate::storage::partition::parse_partition_key(&name.to_string_lossy())
            });
            let final_path =
                mgr.next_replacement_segment_path(&table_dir, partition_key.as_deref())?;
            let tmp_path =
                crate::storage::segment::store::replacement_tmp_path(&final_path, "update_tmp");
            let write_result = mgr.write_new_segment_batches(
                &tmp_path,
                schema,
                mgr.row_group_target_rows(),
                &batches,
            );
            if let Err(error) = write_result {
                let _ = std::fs::remove_file(&tmp_path);
                let _ = crate::storage::segment::meta::remove_sidecar_meta(&tmp_path);
                return Err(error);
            }
            mgr.replace_segments_with_tmp(&tmp_path, &final_path, &[old_seg])?;
        }

        let mut tail_persist_failed = false;
        for (memtable, batch) in memtable_updates {
            if !memtable.replace_pending_batch(batch) {
                return Err(TaibaiError::Internal(format!(
                    "memtable for '{}.{}' left pending state during UPDATE",
                    db, table
                )));
            }
            if let Err(error) = engine.batch_writer().persist_memtable_tail(&memtable) {
                tail_persist_failed = true;
                tracing::warn!(
                    db,
                    table,
                    error = %error,
                    "UPDATE durable tail write failed; flushing target table"
                );
            }
        }
        if tail_persist_failed {
            engine
                .batch_writer()
                .flush_table_under_table_lock(db, table)?;
        }

        // HotStore is append-only. Once rows are updated in place it is no
        // longer authoritative, and must not be lazily recreated from only
        // later INSERTs while older memtable rows are still live.
        engine.hot_store_manager().invalidate(db, table);

        Ok(updated_count)
    })?;

    if updated_count == 0 {
        return Ok(ExecResult::Ok { rows_affected: 0 });
    }

    let unique_index_names = table_meta
        .unique_indexes
        .iter()
        .map(|index| index.name.clone())
        .collect::<Vec<_>>();
    engine
        .index_manager()
        .remove_table_unique_index_snapshots(db, table, &unique_index_names);

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

fn update_memtable_batch(
    assignments: &[Assignment],
    where_clause: &Option<Expr>,
    table_schema: &Schema,
    batch: &RecordBatch,
) -> Result<(RecordBatch, u64)> {
    let mask = if let Some(where_expr) = where_clause.as_ref() {
        eval_where_clause_batch(where_expr, batch, table_schema)?
    } else {
        vec![true; batch.num_rows()]
    };
    if !mask.iter().any(|matches| *matches) {
        return Ok((batch.clone(), 0));
    }

    let mut affected = 0u64;
    let mut new_batch = RecordBatch::new(batch.schema.clone());
    for (row_idx, matches) in mask.into_iter().enumerate() {
        for col_idx in 0..batch.num_columns() {
            let value = if matches {
                updated_value_for_column(assignments, batch, row_idx, col_idx, table_schema)?
            } else {
                batch
                    .scalar(row_idx, col_idx)
                    .unwrap_or(ScalarValue::null_for_type(
                        table_schema.columns[col_idx].data_type,
                    ))
            };
            new_batch.column_mut(col_idx).unwrap().push(&value)?;
        }
        if matches {
            affected += 1;
        }
    }
    Ok((new_batch, affected))
}

fn build_update_mask(
    reader: &crate::storage::segment::SegmentReader,
    row_group_idx: usize,
    where_clause: &Option<Expr>,
    predicate_projection: &[usize],
    table_schema: &Schema,
) -> Result<Vec<bool>> {
    let mut mask = if let Some(where_expr) = where_clause.as_ref() {
        let predicate_batch = if predicate_projection.is_empty() {
            reader.read_row_group(row_group_idx)?
        } else {
            reader.read_row_group_with_projection(row_group_idx, predicate_projection)?
        };
        eval_where_clause_batch(where_expr, &predicate_batch, &predicate_batch.schema)
            .or_else(|_| eval_where_clause_batch(where_expr, &predicate_batch, table_schema))?
    } else {
        vec![true; reader.row_group_rows(row_group_idx) as usize]
    };
    if !reader.delete_bitmap().is_empty() {
        let first_row = reader.row_group_start_row(row_group_idx);
        for (row_idx, matches) in mask.iter_mut().enumerate() {
            if crate::storage::segment::delete_bitmap::is_deleted(
                reader.delete_bitmap(),
                first_row + row_idx as u64,
            ) {
                *matches = false;
            }
        }
    }
    Ok(mask)
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
