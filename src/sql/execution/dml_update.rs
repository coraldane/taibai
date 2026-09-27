use crate::common::error::{Result, TaibaiError};
use crate::storage::segment_manager::SegmentManager;
use crate::types::array::ColumnArray;
use crate::types::batch::RecordBatch;
use crate::types::batch::Schema;
use crate::types::scalar::ScalarValue;
use std::collections::HashMap;
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
    mgr.set_table_compression_level(db, table, table_meta.compression_level);
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

    // Resolve subqueries in the WHERE clause and in assignment values up
    // front, exactly like DELETE does. Without this, `UPDATE ... WHERE id IN
    // (SELECT ...)` used to fall into the predicate evaluator's legacy
    // "match everything" fallback and rewrite the whole table.
    let resolved_where_clause =
        super::dml_delete::resolve_where_in_subqueries(engine, db, where_clause)?;
    let resolved_assignments: Vec<Assignment> = assignments
        .iter()
        .map(|assignment| {
            Ok(Assignment {
                column: assignment.column.clone(),
                value: super::dml_delete::resolve_expr_in_subqueries_pub(
                    engine,
                    db,
                    &assignment.value,
                )?,
            })
        })
        .collect::<Result<_>>()?;
    let assignments = resolved_assignments.as_slice();
    let where_clause = &resolved_where_clause;

    let predicate = build_row_group_predicate(where_clause, &table_schema);
    let predicate_projection = where_clause
        .as_ref()
        .map(|expr| collect_expr_column_indices(expr, &table_schema))
        .unwrap_or_default();
    let target_partition_key =
        super::dml_delete::delete_partition_eq_key(where_clause, &table_schema, &table_meta);

    engine
        .batch_writer()
        .checkpoint_before_rewrite(db, table, 1)?;

    // From here the statement rewrites memtables and segment tails; the work
    // is no longer safely abortable, so cancellation checks stop firing.
    crate::common::query_cancel::enter_committing_phase();

    let update_body = || -> Result<u64> {
        // Updated rows are published as change events (post-update values
        // for memtable rows, which are entirely in memory).
        let publish_updates = engine.pubsub().has_subscribers();
        let mut updated_row_batches: Vec<RecordBatch> = Vec::new();
        let mut updated_count = 0u64;
        let mut memtable_updates: Vec<(
            Arc<crate::storage::core::memtable::MemTable>,
            Arc<RecordBatch>,
        )> = Vec::new();
        let narrowed_partition = target_partition_key
            .as_deref()
            .filter(|key| !key.is_empty());
        for memtable in engine
            .memtable_manager()
            .get_visible_to(db, table, reader_txn_id)
        {
            if let Some(key) = narrowed_partition {
                if memtable.partition_key() != key {
                    continue;
                }
            }
            let Some(batch) = memtable.read_snapshot_arc() else {
                continue;
            };
            let (new_batch, affected, matched_rows) =
                update_memtable_batch(assignments, where_clause, &table_schema, &batch)?;
            if let Some(new_batch) = new_batch {
                updated_count += affected;
                if publish_updates {
                    let columns = batch
                        .columns
                        .iter()
                        .map(|col| col.take(&matched_rows))
                        .collect();
                    if let Ok(updated) = RecordBatch::try_new(batch.schema.clone(), columns) {
                        updated_row_batches.push(updated);
                    }
                }
                memtable_updates.push((memtable, Arc::new(new_batch)));
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
        let mut rewritten_segments: Vec<(std::path::PathBuf, Vec<Arc<RecordBatch>>)> = Vec::new();
        // Assignments are constant per statement; resolve them once for every
        // segment/row group below.
        let assigned = precompute_assigned_values(assignments, &table_schema)?;

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
                let rg_publish = publish_updates;
                // Surviving rows keep their order; matched survivors receive
                // the assigned value in their assigned columns only.
                let start_row = reader.row_group_start_row(rg_idx);
                let delete_bitmap = reader.delete_bitmap();
                let mut kept: Vec<u32> = Vec::new();
                let mut kept_matched: Vec<bool> = Vec::with_capacity(mask.len());
                for (row_idx, matches) in mask.iter().enumerate() {
                    let absolute_row = start_row + row_idx as u64;
                    if crate::storage::segment::delete_bitmap::is_deleted(
                        delete_bitmap,
                        absolute_row,
                    ) {
                        continue;
                    }
                    kept.push(row_idx as u32);
                    kept_matched.push(*matches);
                }
                let mut new_columns = Vec::with_capacity(batch.num_columns());
                for (col_idx, column) in batch.columns.iter().enumerate() {
                    new_columns.push(rebuild_update_column(
                        column,
                        assigned.get(&col_idx),
                        &kept,
                        &kept_matched,
                    )?);
                }
                let new_batch = RecordBatch::try_new(batch.schema.clone(), new_columns)?;
                updated_count += kept_matched.iter().filter(|m| **m).count() as u64;
                if rg_publish {
                    let matched: Vec<u32> = kept_matched
                        .iter()
                        .enumerate()
                        .filter_map(|(idx, m)| m.then_some(idx as u32))
                        .collect();
                    if !matched.is_empty() {
                        let columns = new_batch
                            .columns
                            .iter()
                            .map(|col| col.take(&matched))
                            .collect();
                        if let Ok(updated) = RecordBatch::try_new(new_batch.schema.clone(), columns)
                        {
                            updated_row_batches.push(updated);
                        }
                    }
                }
                row_group_updates.push((rg_idx, Some(new_batch)));
            }

            if segment_updated {
                let mut new_batches = Vec::new();
                for (rg_idx, batch) in row_group_updates {
                    match batch {
                        Some(batch) => new_batches.push(Arc::new(batch)),
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
                                new_batches.push(Arc::new(batch));
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

        // Unique/PK validation. Only indexes whose columns are actually
        // touched by this UPDATE can gain new collisions (assignments are
        // constant per statement), and the primary key must be validated
        // even though it is not listed in `unique_indexes` — UPDATE used to
        // skip PK checks entirely, letting `SET id = <existing>` through.
        let indexes_to_validate = indexes_touched_by_assignments(
            &table_meta.unique_indexes,
            &table_schema,
            assigned.keys().copied().collect(),
        );
        if !indexes_to_validate.is_empty() {
            // Materialize only the key columns (not whole rows) to keep the
            // validation working set proportional to the index width.
            let mut needed: Vec<usize> = indexes_to_validate
                .iter()
                .flat_map(|index| index.columns.iter())
                .filter_map(|column| table_schema.index_of(column))
                .collect();
            needed.sort_unstable();
            needed.dedup();
            let validation_schema = Schema::new(
                needed
                    .iter()
                    .map(|&idx| table_schema.columns[idx].clone())
                    .collect(),
            );
            let project = |batch: &RecordBatch| -> RecordBatch {
                RecordBatch::try_new(
                    validation_schema.clone(),
                    needed
                        .iter()
                        .map(|&idx| batch.columns[idx].clone())
                        .collect(),
                )
                .expect("validation projection preserves column types")
            };
            let mut validation_batches: Vec<Arc<RecordBatch>> = Vec::new();
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
                        validation_batches.push(Arc::new(project(&batch)));
                    }
                }
            }
            for (_, batches) in &rewritten_segments {
                validation_batches.extend(batches.iter().map(|batch| Arc::new(project(batch))));
            }
            // Visible-to-this-transaction memtables only: another session's
            // uncommitted keys used to cause spurious duplicate errors here.
            for memtable in engine
                .memtable_manager()
                .get_visible_to(db, table, reader_txn_id)
            {
                let batch = memtable_updates
                    .iter()
                    .find(|(updated, _)| Arc::ptr_eq(updated, &memtable))
                    .map(|(_, batch)| Arc::clone(batch))
                    .or_else(|| memtable.read_snapshot_arc());
                if let Some(batch) = batch {
                    if batch.num_rows() > 0 {
                        validation_batches.push(Arc::new(project(&batch)));
                    }
                }
            }
            super::dml_insert::validate_unique_indexes_in_batches(
                table,
                &validation_schema,
                &indexes_to_validate,
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
                mgr.table_compression_level(db, table),
            );
            if let Err(error) = write_result {
                let _ = std::fs::remove_file(&tmp_path);
                let _ = crate::storage::segment::meta::remove_sidecar_meta(&tmp_path);
                return Err(error);
            }
            if let Err(error) =
                mgr.replace_segments_with_tmp(&tmp_path, &final_path, &[old_seg])
            {
                // The replacement failed: drop the orphaned temp segment
                // instead of leaking it (startup cleanup only knows the
                // compaction temp patterns).
                let _ = std::fs::remove_file(&tmp_path);
                let _ = crate::storage::segment::meta::remove_sidecar_meta(&tmp_path);
                return Err(error);
            }
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
            // Partition-pruned UPDATEs run under the target partition's lock
            // only; flushing the whole table with the "table lock held"
            // freeze would race concurrent appends in OTHER partitions.
            // Flush just the pruned partition, whose appenders this
            // statement's partition lock already excludes.
            if let Some(partition) = target_partition_key
                .as_deref()
                .filter(|key| !key.is_empty())
            {
                engine
                    .batch_writer()
                    .flush_partition_under_partition_lock(db, table, partition)?;
            } else {
                engine
                    .batch_writer()
                    .flush_table_under_table_lock(db, table)?;
            }
        }

        // HotStore is append-only. Once rows are updated in place it is no
        // longer authoritative, and must not be lazily recreated from only
        // later INSERTs while older memtable rows are still live.
        engine.invalidate_hot_store(db, table);

        // Publish post-update rows as change events once every segment
        // replacement has committed.
        if publish_updates && updated_count > 0 {
            if let Some((first, rest)) = updated_row_batches.split_first() {
                let schema = first.schema.clone();
                let mut columns: Vec<crate::types::array::ColumnArray> =
                    (0..schema.columns.len())
                        .map(|c| first.column(c).unwrap().clone())
                        .collect();
                for batch in rest {
                    for (c, col) in columns.iter_mut().enumerate() {
                        col.append_owned(batch.column(c).unwrap().clone())?;
                    }
                }
                let combined = RecordBatch::try_new(schema, columns)?;
                engine.pubsub().publish_change(
                    crate::pubsub::ChangeKind::Update,
                    db,
                    table,
                    combined,
                );
            }
        }

        if updated_count > 0 {
            // Rebuild location indexes while STILL holding the mutation lock.
            // Doing this after releasing the lock (as before) left a window
            // where a concurrent INSERT probed a stale index snapshot and
            // committed a key this UPDATE had just introduced, permanently
            // baking a duplicate into the rebuilt index.
            if let Err(error) = super::dml_insert::rebuild_table_location_indexes(
                engine,
                db,
                table,
                &table_meta,
                &table_schema,
            ) {
                tracing::warn!(
                    db,
                    table,
                    error = %error,
                    "UPDATE index auto-rebuild failed; falling back to snapshot invalidation"
                );
                engine
                    .index_manager()
                    .invalidate_primary_key_index(db, table);
                let unique_index_names = table_meta
                    .unique_indexes
                    .iter()
                    .map(|index| index.name.clone())
                    .collect::<Vec<_>>();
                engine
                    .index_manager()
                    .invalidate_table_unique_index_snapshots(db, table, &unique_index_names);
            }
        }

        Ok(updated_count)
    };

    let updated_count = match target_partition_key
        .as_deref()
        .filter(|key| !key.is_empty())
    {
        Some(partition) => {
            mgr.with_partition_set_mutation_lock(db, table, &[partition], update_body)?
        }
        None => mgr.with_table_mutation_lock(db, table, update_body)?,
    };

    if updated_count == 0 {
        return Ok(ExecResult::Ok { rows_affected: 0 });
    }

    Ok(ExecResult::Ok {
        rows_affected: updated_count,
    })
}

/// Unique indexes whose key can change under this UPDATE: an index needs
/// revalidation only when at least one of its columns is assigned a new
/// value. The primary key is included as a pseudo index — it is deliberately
/// absent from `unique_indexes`, and UPDATE previously skipped it entirely.
fn indexes_touched_by_assignments(
    unique_indexes: &[crate::storage::catalog::schema::UniqueIndexMeta],
    table_schema: &Schema,
    assigned_columns: std::collections::HashSet<usize>,
) -> Vec<crate::storage::catalog::schema::UniqueIndexMeta> {
    let mut indexes: Vec<crate::storage::catalog::schema::UniqueIndexMeta> = unique_indexes
        .iter()
        .filter(|index| {
            index.columns.iter().any(|column| {
                table_schema
                    .index_of(column)
                    .is_some_and(|idx| assigned_columns.contains(&idx))
            })
        })
        .cloned()
        .collect();
    let pk_columns: Vec<String> = table_schema
        .columns
        .iter()
        .enumerate()
        .filter(|(idx, column)| column.primary_key && assigned_columns.contains(idx))
        .map(|(_, column)| column.name.clone())
        .collect();
    if !pk_columns.is_empty() {
        indexes.push(crate::storage::catalog::schema::UniqueIndexMeta {
            name: "PRIMARY".to_string(),
            columns: pk_columns,
        });
    }
    indexes
}

fn update_memtable_batch(
    assignments: &[Assignment],
    where_clause: &Option<Expr>,
    table_schema: &Schema,
    batch: &RecordBatch,
) -> Result<(Option<RecordBatch>, u64, Vec<u32>)> {
    let mask = if let Some(where_expr) = where_clause.as_ref() {
        eval_where_clause_batch(where_expr, batch, table_schema)?
    } else {
        vec![true; batch.num_rows()]
    };
    // `None` means no row matched, so the caller skips the memtable without
    // paying for any copy.
    let matched_rows: Vec<u32> = mask
        .iter()
        .enumerate()
        .filter_map(|(row_idx, matches)| matches.then_some(row_idx as u32))
        .collect();
    if matched_rows.is_empty() {
        return Ok((None, 0, Vec::new()));
    }

    let assigned = precompute_assigned_values(assignments, table_schema)?;
    let all_rows: Vec<u32> = (0..batch.num_rows() as u32).collect();
    let mut new_columns = Vec::with_capacity(batch.num_columns());
    for (col_idx, column) in batch.columns.iter().enumerate() {
        new_columns.push(rebuild_update_column(
            column,
            assigned.get(&col_idx),
            &all_rows,
            &mask,
        )?);
    }
    let affected = matched_rows.len() as u64;
    Ok((
        Some(RecordBatch::try_new(batch.schema.clone(), new_columns)?),
        affected,
        matched_rows,
    ))
}

/// Rebuild one column for the surviving rows of an UPDATE: untouched rows
/// keep their value, matched survivors receive the (constant) assigned value.
fn rebuild_update_column(
    column: &ColumnArray,
    assigned: Option<&ScalarValue>,
    kept: &[u32],
    kept_matched: &[bool],
) -> Result<ColumnArray> {
    match assigned {
        // Untouched columns: one bulk gather instead of a per-cell
        // ScalarValue round-trip.
        None => Ok(column.take(kept)),
        Some(value) => {
            let mut out = ColumnArray::empty(column.data_type());
            for (kept_idx, &src_row) in kept.iter().enumerate() {
                if kept_matched[kept_idx] {
                    out.push(value)?;
                } else {
                    out.push(&column.scalar(src_row as usize))?;
                }
            }
            Ok(out)
        }
    }
}

/// Assignment expressions are constant per statement (they are evaluated
/// without row context), so resolve every assigned column index and value
/// once instead of per row. Evaluation errors abort the statement (matching
/// the pre-optimization behavior) instead of silently writing NULL.
fn precompute_assigned_values(
    assignments: &[Assignment],
    table_schema: &Schema,
) -> Result<HashMap<usize, ScalarValue>> {
    let mut assigned = HashMap::new();
    for assign in assignments {
        if let Some(col_idx) = table_schema
            .columns
            .iter()
            .position(|column| column.name == assign.column)
        {
            if !assigned.contains_key(&col_idx) {
                let value =
                    eval_expr_to_scalar(&assign.value, &table_schema.columns[col_idx].data_type)?;
                assigned.insert(col_idx, value);
            }
        }
    }
    Ok(assigned)
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
