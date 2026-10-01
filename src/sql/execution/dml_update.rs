use crate::common::error::{Result, TaibaiError};
use crate::storage::segment_manager::SegmentManager;
use crate::types::array::ColumnArray;
use crate::types::batch::RecordBatch;
use crate::types::batch::Schema;
use crate::types::scalar::ScalarValue;
use std::collections::HashMap;
use std::sync::Arc;

use super::engine::{Engine, ExecResult};
use super::helpers::{eval_expr_to_scalar, eval_where_clause_batch};
use crate::sql::parser::ast::{Assignment, Expr};

pub fn exec_update(
    engine: &Engine,
    db: &str,
    table: &str,
    assignments: &[Assignment],
    where_clause: &Option<Expr>,
    reader_txn_id: u64,
) -> Result<ExecResult> {
    if reader_txn_id > 0 {
        return super::txn_dml::exec_update_in_txn(
            engine,
            db,
            table,
            assignments,
            where_clause,
            reader_txn_id,
        );
    }
    super::txn_dml::refuse_auto_commit_dml_while_pinned(engine, db, table)?;
    // Join the INSERT/REPLACE writer protocol. UPDATE is the mutator the
    // existing guards missed: INSERT probes unique keys under this striped
    // lock, but an UPDATE that introduces a key under only the table mutation
    // lock can interleave between the probe and the publish, and the INSERT
    // then commits a duplicate the post-hoc index rebuild bakes in
    // permanently. Held across the whole statement and taken before the
    // mutation lock so the global order constraint_insert_lock → mutation
    // lock matches the INSERT path.
    let _constraint_guard = super::dml_insert::constraint_insert_lock(db, table)
        .lock()
        .unwrap();

    let ctx = super::txn_dml::DmlStatementContext::plan(engine, db, table, where_clause)?;
    // Shadow the raw parameter: every consumer below — the memtable batch
    // updates, the segment masks — must see the subquery-resolved clause,
    // never the statement's original AST.
    let where_clause = &ctx.resolved_where;
    let table_schema = &ctx.table_schema;
    let table_meta = &ctx.table_meta;
    let predicate = ctx.predicate.as_ref();
    let predicate_projection = &ctx.predicate_projection;
    let target_partition_key = ctx.narrowed_partition();

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

    // Resolve subqueries in the assignment values up front, exactly like the
    // WHERE clause inside `DmlStatementContext::plan` — without it, a SET
    // subquery would reach the evaluator as an opaque expression.
    let resolved_assignments: Vec<Assignment> = assignments
        .iter()
        .map(|assignment| {
            Ok(Assignment {
                column: assignment.column.clone(),
                value: super::query::resolve_expr_in_subqueries_clone(
                    engine,
                    db,
                    &assignment.value,
                )?,
            })
        })
        .collect::<Result<_>>()?;
    let assignments = resolved_assignments.as_slice();

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
        let mut updated_events = super::helpers::ChangeEventBuffer::new("update", db, table);
        let mut updated_count = 0u64;
        let mut memtable_updates: Vec<(
            Arc<crate::storage::core::memtable::MemTable>,
            Arc<RecordBatch>,
            Vec<u32>,
            Vec<(usize, ScalarValue)>,
        )> = Vec::new();
        let narrowed_partition = target_partition_key;
        super::dml_delete::for_each_visible_memtable(
            engine,
            db,
            table,
            &table_schema,
            reader_txn_id,
            narrowed_partition,
            |memtable, _original, batch, _live_positions| {
                // C9: `batch` (the Arc) is consumed — update_memtable_batch
                // moves untouched columns out of it when it is the sole owner.
                let (new_batch, affected, matched_rows, assigned_pairs) =
                    update_memtable_batch(assignments, where_clause, &table_schema, batch)?;
                if let Some(new_batch) = new_batch {
                    updated_count += affected;
                    if publish_updates {
                        // Change events carry the post-update row images: take
                        // from `new_batch`, not the pre-update snapshot.
                        updated_events.push(&new_batch, &matched_rows);
                    }
                    memtable_updates.push((
                        memtable,
                        Arc::new(new_batch),
                        matched_rows,
                        assigned_pairs,
                    ));
                }
                Ok(())
            },
        )?;

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
        let mut staged_segments: Vec<(std::path::PathBuf, std::path::PathBuf, std::path::PathBuf)> =
            Vec::new();
        let mut segments_removed = 0usize;
        // Assignments are constant per statement; resolve them once for every
        // segment/row group below.
        let assigned = precompute_assigned_values(assignments, &table_schema)?;
        let compression_level = mgr.table_compression_level(db, table);
        let row_group_target = mgr.row_group_target_rows();

        // Two passes per segment, so the statement streams instead of
        // materializing whole rewritten segments in memory:
        //   Pass A evaluates the WHERE masks from the projected predicate
        //   columns only and skips segments with no match at all;
        //   Pass B rewrites the segment row group by row group straight into
        //   the replacement temp file — peak memory is one row group, not the
        //   whole segment.
        for seg_path in segment_files {
            let reader = mgr.open_reader(&seg_path)?;
            if !super::helpers::segment_may_match(&reader, &table_schema, predicate) {
                continue;
            }

        // Pass A: masks (projected reads only).
        //
        // Footer zone maps follow the segment's own column order, so the
        // catalog-space predicate must be re-anchored per segment before it
        // may touch them — a positional probe after a DROP COLUMN reads the
        // wrong column's zone map and silently skips row groups that hold
        // matching rows. When alignment fails (a predicate column predates
        // the footer), pruning is disabled for the whole segment: the
        // default-filled mask read below decides row by row, matching the
        // DELETE/SELECT/COUNT absent-column semantics.
        let aligned_predicate = predicate
            .as_ref()
            .and_then(|pred| reader.align_row_group_predicate(&table_schema, pred).ok());
        let mut rg_masks: Vec<Option<Vec<bool>>> = Vec::with_capacity(reader.num_row_groups());
        let mut any_match = false;
        for rg_idx in 0..reader.num_row_groups() {
            if let Some(ref pred) = aligned_predicate {
                if !pred.matches_row_group(reader.zone_maps(rg_idx)) {
                    rg_masks.push(None);
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
                any_match |= mask.iter().any(|m| *m);
                rg_masks.push(Some(mask));
            }
            if !any_match {
                continue;
            }

            // Pass B: stream the replacement segment. Row groups are
            // aggregated into one pending batch and written with a single
            // write_batch call — write_batch encodes each call's column slice
            // as one block, so feeding it row-group-sized batches directly
            // would splice separately-encoded blocks inside one row group and
            // corrupt the segment. Aggregating keeps the peak memory at one
            // row group while staying byte-identical to the old
            // materialize-everything-then-combine write.
            let partition_key = seg_path.file_name().and_then(|name| {
                crate::storage::partition::parse_partition_key(&name.to_string_lossy())
            });
            let final_path =
                mgr.next_replacement_segment_path(&table_dir, partition_key.as_deref())?;
            let tmp_path =
                crate::storage::segment::store::replacement_tmp_path(&final_path, "update_tmp");
            let mut writer: Option<crate::storage::segment::SegmentWriter> = None;
            let mut pending: Option<RecordBatch> = None;
            let mut written_rows = 0usize;
            let rewrite_result = (|| -> Result<()> {
                for (rg_idx, mask_opt) in rg_masks.into_iter().enumerate() {
                    // Catalog-ordered whole-row-group read: `assigned` maps
                    // catalog column indices, and the pending batches must
                    // extend column-wise across segments of different ages —
                    // both only hold when every batch is in catalog order
                    // (positional reads hit the wrong physical column after
                    // a DROP/RENAME COLUMN).
                    let mut batch =
                        reader.read_row_group_columns_aligned(rg_idx, &table_schema)?;
                    match mask_opt {
                        Some(mask) if mask.iter().any(|m| *m) => {
                            // Surviving rows keep their order; matched survivors
                            // receive the assigned value in their assigned
                            // columns only.
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
                            let new_batch =
                                rebuild_update_batch(&batch, &assigned, &kept, &kept_matched)?;
                            updated_count += kept_matched.iter().filter(|m| **m).count() as u64;
                            if publish_updates {
                                let matched: Vec<u32> = kept_matched
                                    .iter()
                                    .enumerate()
                                    .filter_map(|(idx, m)| m.then_some(idx as u32))
                                    .collect();
                                updated_events.push(&new_batch, &matched);
                            }
                            batch = new_batch;
                        }
                        _ => {
                            // Unmatched or zone-pruned row group: passthrough,
                            // minus rows a previous DELETE already tombstoned.
                            if !reader.delete_bitmap().is_empty() {
                                batch = crate::storage::segment::delete_bitmap::filter_batch(
                                    batch,
                                    reader.row_group_start_row(rg_idx),
                                    reader.delete_bitmap(),
                                )?;
                            }
                        }
                    }
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    pending = match pending.take() {
                        Some(mut accumulated) => {
                            for col_idx in 0..batch.num_columns() {
                                let source = batch.column(col_idx).expect("validated column");
                                accumulated
                                    .column_mut(col_idx)
                                    .expect("validated column")
                                    .extend(source)?;
                            }
                            Some(accumulated)
                        }
                        None => Some(batch),
                    };
                    let pending_rows = pending.as_ref().expect("just set").num_rows();
                    if pending_rows >= row_group_target {
                        if writer.is_none() {
                            // Disk-full or permission errors must fail the
                            // statement (the caller cleans the temp file) —
                            // not panic while holding the table lock.
                            writer = Some(crate::storage::segment::SegmentWriter::create(
                                &tmp_path,
                                pending.as_ref().expect("just set").schema.clone(),
                                row_group_target,
                                compression_level,
                            )?);
                        }
                        writer
                            .as_mut()
                            .expect("created above")
                            .write_batch(pending.as_ref().expect("just set"))?;
                        written_rows += pending_rows;
                        pending = None;
                    }
                }
                if let Some(pending) = pending.take() {
                    if writer.is_none() {
                        writer = Some(crate::storage::segment::SegmentWriter::create(
                            &tmp_path,
                            pending.schema.clone(),
                            row_group_target,
                            compression_level,
                        )?);
                    }
                    writer
                        .as_mut()
                        .expect("created above")
                        .write_batch(&pending)?;
                    written_rows += pending.num_rows();
                }
                Ok(())
            })();
            match rewrite_result {
                Ok(()) => {}
                Err(error) => {
                    // Drop this temp file and every earlier one; the old
                    // segments are untouched, so the statement aborts cleanly.
                    let _ = std::fs::remove_file(&tmp_path);
                    let _ = crate::storage::segment::meta::remove_sidecar_meta(&tmp_path);
                    for (_, tmp, final_path) in &staged_segments {
                        let _ = std::fs::remove_file(tmp);
                        let _ = crate::storage::segment::meta::remove_sidecar_meta(tmp);
                        let _ = std::fs::remove_file(final_path);
                    }
                    return Err(error);
                }
            }
            drop(reader);
            match writer {
                Some(writer) => {
                    writer.seal()?;
                    staged_segments.push((seg_path, tmp_path, final_path));
                }
                None => {
                    // Nothing survived the delete footer in this segment:
                    // remove it outright instead of writing an empty file.
                    mgr.remove_segment_file(&seg_path)?;
                    segments_removed += 1;
                }
            }
        }

        if staged_segments.is_empty() && segments_removed == 0 && memtable_updates.is_empty() {
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
            // Full-schema batches (memtables) are projected here; segment and
            // staged reads below use read_row_group_with_projection directly.
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
                if staged_segments
                    .iter()
                    .any(|(old_seg, _, _)| old_seg == &seg_path)
                {
                    continue;
                }
                let reader = mgr.open_reader(&seg_path)?;
                for rg_idx in 0..reader.num_row_groups() {
                    // Name-based read: `needed` is catalog-space and old
                    // segments keep pre-DDL footers.
                    let mut batch =
                        reader.read_row_group_columns(rg_idx, &table_schema, &needed)?;
                    if !reader.delete_bitmap().is_empty() {
                        batch = crate::storage::segment::delete_bitmap::filter_batch(
                            batch,
                            reader.row_group_start_row(rg_idx),
                            reader.delete_bitmap(),
                        )?;
                    }
                    if batch.num_rows() > 0 {
                        validation_batches.push(Arc::new(batch));
                    }
                }
            }
            // Staged replacement segments (already sealed, not yet swapped
            // in): read their key columns straight from the temp files.
            // Staged batches were written catalog-ordered by Pass B, so the
            // projection matches directly.
            for (_, tmp_path, _) in &staged_segments {
                let tmp_reader = crate::storage::segment::SegmentReader::open(tmp_path)?;
                for rg_idx in 0..tmp_reader.num_row_groups() {
                    let batch = tmp_reader.read_row_group_with_projection(rg_idx, &needed)?;
                    if batch.num_rows() > 0 {
                        validation_batches.push(Arc::new(batch));
                    }
                }
            }
            // Visible-to-this-transaction memtables only: another session's
            // uncommitted keys used to cause spurious duplicate errors here.
            for memtable in engine
                .memtable_manager()
                .get_visible_to(db, table, reader_txn_id)
            {
                let batch = memtable_updates
                    .iter()
                    .find(|(updated, _, _, _)| Arc::ptr_eq(updated, &memtable))
                    .map(|(_, batch, _, _)| Arc::clone(batch))
                    .or_else(|| memtable.read_snapshot_arc());
                if let Some(batch) = batch {
                    if batch.num_rows() > 0 {
                        validation_batches.push(Arc::new(project(&batch)));
                    }
                }
            }
            if let Err(error) = super::dml_insert::validate_unique_indexes_in_batches(
                table,
                &validation_schema,
                &indexes_to_validate,
                &validation_batches,
            ) {
                // Validation failed: the temp files are the statement's only
                // side effect so far — drop them; the old segments are intact.
                for (_, tmp, final_path) in &staged_segments {
                    let _ = std::fs::remove_file(tmp);
                    let _ = crate::storage::segment::meta::remove_sidecar_meta(tmp);
                    let _ = std::fs::remove_file(final_path);
                }
                return Err(error);
            }
        }

        // Two-phase rewrite, phase two: every replacement temp file was
        // written (and validated) above; only the metadata-only swaps remain.
        // A failure in the swaps drops the orphaned temp segment instead of
        // leaking it (startup cleanup only knows the compaction temp
        // patterns).
        let segments_rewritten = !staged_segments.is_empty();
        for (old_seg, tmp_path, final_path) in staged_segments {
            if let Err(error) = mgr.replace_segments_with_tmp(&tmp_path, &final_path, &[old_seg]) {
                let _ = std::fs::remove_file(&tmp_path);
                let _ = crate::storage::segment::meta::remove_sidecar_meta(&tmp_path);
                return Err(error);
            }
        }

        let mut tail_persist_failed = false;
        for (memtable, batch, matched_rows, assigned_pairs) in memtable_updates {
            // Durable side BEFORE the in-memory swap: the segment rewrites
            // above already committed the new values, so a crash between the
            // two steps must not let recovery restore the pre-update tail
            // next to them. This loop runs under the table/partition
            // mutation lock, so no flush can invalidate `batch` in between.
            // The incremental `.upd` sidecar costs O(affected rows) instead
            // of a whole-tail rewrite (quadratic for repeated small UPDATEs
            // on a large memtable). A tail write failure still swaps in
            // memory and falls back to the flush below (same degradation as
            // before).
            if let Err(error) = engine.batch_writer().update_memtable_tail_rows(
                &memtable,
                &matched_rows,
                &assigned_pairs,
                &batch,
            ) {
                tail_persist_failed = true;
                tracing::warn!(
                    db,
                    table,
                    error = %error,
                    "UPDATE durable tail write failed; flushing target table"
                );
            }
            if !memtable.replace_pending_batch(batch) {
                return Err(TaibaiError::Internal(format!(
                    "memtable for '{}.{}' left pending state during UPDATE",
                    db, table
                )));
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
        // replacement has committed. One event per batch: rewritten segments
        // and memtables may carry different schemas after ALTER TABLE ADD
        // COLUMN (metadata-only), so merging by column index would panic.
        if publish_updates && updated_count > 0 {
            for batch in updated_events.batches {
                engine
                    .pubsub()
                    .publish_change(crate::pubsub::ChangeKind::Update, db, table, batch);
            }
        }
        if updated_count > 0 {
            // Rebuild location indexes while STILL holding the mutation lock.
            // Doing this after releasing the lock (as before) left a window
            // where a concurrent INSERT probed a stale index snapshot and
            // committed a key this UPDATE had just introduced, permanently
            // baking a duplicate into the rebuilt index.
            //
            // Only two things can stale a snapshot, so the full-table rebuild
            // runs only when one of them happened: a segment swap moved rows
            // to new segment paths, or an assignment rewrote a key column
            // (including memtable rows, whose positions are position-stable
            // so their row ids in the snapshots stay valid for non-key
            // updates). A non-key UPDATE over memtable rows only skips it.
            let keys_touched = !indexes_to_validate.is_empty();
            if keys_touched || segments_rewritten || segments_removed > 0 {
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
        }

        Ok(updated_count)
    };

    let update_outcome = match target_partition_key
        .as_deref()
        .filter(|key| !key.is_empty())
    {
        Some(partition) => {
            mgr.with_partition_set_mutation_lock(db, table, &[partition], update_body)
        }
        None => mgr.with_table_mutation_lock(db, table, update_body),
    };
    // Rows may already be rewritten when a later step of the statement fails
    // (partial segment swap, mid-loop memtable swap). The resident hot store
    // is append-only and must never serve pre-update rows after any rewrite
    // happened, so retire it on the error path too — the old early-return
    // skipped the invalidation and left stale mirror rows resident.
    if update_outcome.is_err() {
        engine.invalidate_hot_store(db, table);
    }
    let updated_count = update_outcome?;

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
    let pk_columns: Vec<String> = {
        // The primary key is one composite constraint: once any of its columns
        // is rewritten, the NEW (c1, c2, ...) combinations must be checked for
        // uniqueness against the full key. Filtering to the assigned subset
        // used to reject legal updates — PK(id, ts) with `SET id = 1` flagged
        // (1, 1) vs (1, 2) as duplicates.
        let any_pk_assigned = table_schema
            .columns
            .iter()
            .enumerate()
            .any(|(idx, column)| column.primary_key && assigned_columns.contains(&idx));
        if !any_pk_assigned {
            Vec::new()
        } else {
            table_schema
                .columns
                .iter()
                .filter(|column| column.primary_key)
                .map(|column| column.name.clone())
                .collect()
        }
    };
    if !pk_columns.is_empty() {
        indexes.push(crate::storage::catalog::schema::UniqueIndexMeta {
            name: "PRIMARY".to_string(),
            columns: pk_columns,
        });
    }
    indexes
}

pub(crate) fn update_memtable_batch(
    assignments: &[Assignment],
    where_clause: &Option<Expr>,
    table_schema: &Schema,
    batch: Arc<RecordBatch>,
) -> Result<(
    Option<RecordBatch>,
    u64,
    Vec<u32>,
    Vec<(usize, ScalarValue)>,
)> {
    let mask = if let Some(where_expr) = where_clause.as_ref() {
        // Resolve against the batch's OWN schema first: a resident memtable
        // written before a DROP/RENAME COLUMN keeps its insert-time column
        // order, and name-based resolution (index_of follows previous_names)
        // keeps the predicate on the right physical column. The table schema
        // stays as the fallback for hand-built batches.
        eval_where_clause_batch(where_expr, &batch, &batch.schema)
            .or_else(|_| eval_where_clause_batch(where_expr, &batch, table_schema))?
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
        return Ok((None, 0, Vec::new(), Vec::new()));
    }

    // Assignments resolve against THIS batch's column layout: the catalog
    // index of a column and its index inside an older memtable's batch can
    // disagree after a DROP/RENAME COLUMN, and writing by catalog position
    // would corrupt the neighboring physical column.
    let mut assigned = HashMap::new();
    for assign in assignments {
        if let Some(local_idx) = batch.schema.index_of(&assign.column) {
            let col_type = batch.schema.columns[local_idx].data_type;
            if !assigned.contains_key(&local_idx) {
                let value = eval_expr_to_scalar(&assign.value, &col_type)?;
                assigned.insert(local_idx, value);
            }
        }
    }
    // Canonical ascending column order, ready for the durable `.upd` sidecar
    // (which replays these constants by physical column index).
    let assigned_pairs: Vec<(usize, ScalarValue)> = assigned
        .iter()
        .map(|(col_idx, value)| (*col_idx, value.clone()))
        .collect();
    let affected = matched_rows.len() as u64;
    // C9: only the ASSIGNED columns pay a rebuild. `rebuild_update_column`
    // takes `&[u32]`, so keep the single identity-index allocation but pass
    // it to assigned columns only; untouched columns never walk it.
    let all_rows: Vec<u32> = if assigned.is_empty() {
        Vec::new()
    } else {
        (0..batch.num_rows() as u32).collect()
    };
    let mut new_columns = Vec::with_capacity(batch.num_columns());
    // C9: untouched columns move straight into the replacement batch when
    // this statement is the snapshot Arc's sole owner (the transaction view
    // and schema-aligned copies); when the memtable still shares the Arc a
    // flat clone is the fallback — still cheaper than the old per-row
    // identity gather through `rebuild_update_column`, and never taken for
    // assigned columns (those are rebuilt either way).
    let new_batch = match Arc::try_unwrap(batch) {
        Ok(owned) => {
            let schema = owned.schema;
            for (col_idx, column) in owned.columns.into_iter().enumerate() {
                new_columns.push(match assigned.get(&col_idx) {
                    Some(value) => rebuild_update_column(&column, Some(value), &all_rows, &mask)?,
                    None => column,
                });
            }
            RecordBatch::try_new(schema, new_columns)?
        }
        Err(shared) => {
            for (col_idx, column) in shared.columns.iter().enumerate() {
                new_columns.push(match assigned.get(&col_idx) {
                    Some(value) => rebuild_update_column(column, Some(value), &all_rows, &mask)?,
                    None => column.clone(),
                });
            }
            RecordBatch::try_new(shared.schema.clone(), new_columns)?
        }
    };
    Ok((Some(new_batch), affected, matched_rows, assigned_pairs))
}

/// Rebuild a whole batch for the surviving rows of an UPDATE: untouched rows
/// keep their value, matched survivors receive the (constant) assigned value
/// in their assigned columns. The single image constructor shared by the
/// rewritten-row-group (auto-commit) and deferred-image (transactional)
/// segment paths.
pub(crate) fn rebuild_update_batch(
    batch: &RecordBatch,
    assigned: &std::collections::HashMap<usize, ScalarValue>,
    kept: &[u32],
    kept_matched: &[bool],
) -> Result<RecordBatch> {
    let mut new_columns = Vec::with_capacity(batch.num_columns());
    for (col_idx, column) in batch.columns.iter().enumerate() {
        new_columns.push(rebuild_update_column(
            column,
            assigned.get(&col_idx),
            kept,
            kept_matched,
        )?);
    }
    RecordBatch::try_new(batch.schema.clone(), new_columns)
}

/// Rebuild one column for the surviving rows of an UPDATE: untouched rows
/// keep their value, matched survivors receive the (constant) assigned value.
pub(crate) fn rebuild_update_column(
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
            // Split the survivors into maximal constant runs: long unmatched
            // runs go through `gather_into` (memcpy for ascending runs, bulk
            // bitmaps), and only the matched rows pay a per-row push. The old
            // loop routed EVERY row through a scalar() + push() round-trip,
            // cloning a String per VarChar cell of a million-row group to
            // change one row.
            let mut out = ColumnArray::empty(column.data_type());
            let mut idx = 0usize;
            while idx < kept.len() {
                let matched = kept_matched[idx];
                let start = idx;
                while idx < kept.len() && kept_matched[idx] == matched {
                    idx += 1;
                }
                if matched {
                    for _ in start..idx {
                        out.push(value)?;
                    }
                } else {
                    column.gather_into(&kept[start..idx], &mut out)?;
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
pub(crate) fn precompute_assigned_values(
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
        } else {
            // A silent skip used to report rows_affected while changing
            // nothing (`UPDATE t SET nosuchcol = 1 WHERE ...`).
            return Err(TaibaiError::Sql(format!(
                "unknown column '{}' in SET clause",
                assign.column
            )));
        }
    }
    Ok(assigned)
}

pub(crate) fn build_update_mask(
    reader: &crate::storage::segment::SegmentReader,
    row_group_idx: usize,
    where_clause: &Option<Expr>,
    predicate_projection: &[usize],
    table_schema: &Schema,
) -> Result<Vec<bool>> {
    let mut mask = if let Some(where_expr) = where_clause.as_ref() {
        // Name-based read: the batch carries the CATALOG defs of the
        // requested columns in request order, so evaluation by catalog
        // position stays correct even when this segment's footer predates a
        // DROP/RENAME COLUMN (positional projection reads silently hit the
        // wrong physical column after the DDL). A footer that predates a
        // PREDICATE column cannot serve the narrow read at all — degrade to
        // the full aligned read (missing columns filled with their DEFAULT,
        // the same absent-column semantics as DELETE/SELECT/COUNT) instead
        // of failing the statement.
        let segment_drifts = !predicate_projection.is_empty()
            && crate::storage::segment::reader::any_column_absent_in_schema(
                table_schema,
                reader.schema(),
                predicate_projection,
            );
        let predicate_batch = if predicate_projection.is_empty() || segment_drifts {
            reader.read_row_group_columns_aligned(row_group_idx, table_schema)?
        } else {
            reader.read_row_group_columns(row_group_idx, table_schema, predicate_projection)?
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
