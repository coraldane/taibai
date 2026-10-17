use crate::common::error::{Result, TaibaiError};
use crate::storage::segment_manager::SegmentManager;
use crate::types::array::ColumnArray;
use crate::types::batch::RecordBatch;
use crate::types::batch::Schema;
use crate::types::scalar::ScalarValue;
use std::collections::HashMap;
use std::sync::Arc;

use super::engine::{Engine, ExecResult};
use super::helpers::{eval_expr_to_scalar_with_scale_timezone, eval_where_clause_batch};
use crate::sql::parser::ast::{Assignment, Expr};

/// One staged memtable UPDATE: (memtable, rewritten image batch, tombstoned
/// positions, staged assignment values).
type StagedMemtableUpdate = (
    Arc<crate::storage::core::memtable::MemTable>,
    Arc<RecordBatch>,
    Vec<u32>,
    Vec<(usize, ScalarValue)>,
);

/// [`update_memtable_batch`] outcome: (rewritten image batch, deleted row
/// count, tombstoned positions, staged assignment values).
type MemtableUpdateOutcome = (
    Option<RecordBatch>,
    u64,
    Vec<u32>,
    Vec<(usize, ScalarValue)>,
);

/// Per segment: (path, full-length new bitmap, new deleted count, undo =
/// previous bitmap bytes + previous deleted count).
type FooterReinsertEntry = (std::path::PathBuf, Vec<u8>, u64, (Vec<u8>, u64));

/// Compatibility entry point for callers using the default UTC conversion.
pub fn exec_update(
    engine: &Engine,
    db: &str,
    table: &str,
    assignments: &[Assignment],
    where_clause: &Option<Expr>,
    reader_txn_id: u64,
) -> Result<ExecResult> {
    exec_update_with_timezone(
        engine,
        db,
        table,
        assignments,
        where_clause,
        reader_txn_id,
        0,
    )
}

pub(crate) fn exec_update_with_timezone(
    engine: &Engine,
    db: &str,
    table: &str,
    assignments: &[Assignment],
    where_clause: &Option<Expr>,
    reader_txn_id: u64,
    timezone_offset_hours: i32,
) -> Result<ExecResult> {
    if reader_txn_id > 0 {
        return super::txn_dml::exec_update_in_txn(
            engine,
            db,
            table,
            assignments,
            where_clause,
            reader_txn_id,
            timezone_offset_hours,
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
    let _constraint_guard = super::dml_insert::acquire_constraint_insert_lock(db, table)?;

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

    let resolved_assignments =
        super::txn_dml::plan_update_assignments(engine, db, assignments, table_meta)?;
    let assigned =
        precompute_assigned_values(&resolved_assignments, table_schema, timezone_offset_hours)?;

    engine.batch_writer().checkpoint_before_rewrite(
        db,
        table,
        crate::storage::core::batch_writer::RewriteEstimate::SidecarMutation,
    )?;

    // From here the statement rewrites memtables and segment tails; the work
    // is no longer safely abortable, so cancellation checks stop firing.
    crate::common::query_cancel::enter_committing_phase();

    let mutation_started = std::cell::Cell::new(false);
    let update_body = || -> Result<u64> {
        // In-lock re-check: the fn-top gate races a transaction registering
        // its memtable-position pin under this lock, and this statement
        // shifts those positions.
        super::txn_dml::refuse_auto_commit_dml_while_pinned(engine, db, table)?;
        // Updated rows are published as change events (post-update values
        // for memtable rows, which are entirely in memory).
        let publish_updates = engine.pubsub().has_subscribers();
        let mut updated_events = super::helpers::ChangeEventBuffer::new("update", db, table);
        let mut updated_count = 0u64;
        let mut budget = super::dml_staging::MutationBudget::new();
        let mut memtable_updates: Vec<StagedMemtableUpdate> = Vec::new();
        let mut index_maintenance =
            super::dml_delete::DeleteIndexMaintenance::plan(table_meta, table_schema);
        let narrowed_partition = target_partition_key;
        super::dml_delete::for_each_visible_memtable(
            engine,
            db,
            table,
            table_schema,
            reader_txn_id,
            narrowed_partition,
            |memtable, _original, batch, _live_positions| {
                // C9: `batch` (the Arc) is consumed — update_memtable_batch
                // moves untouched columns out of it when it is the sole owner.
                let (new_batch, affected, matched_rows, assigned_pairs) =
                    update_memtable_batch(&assigned, where_clause, table_schema, batch.clone())?;
                if let Some(new_batch) = new_batch {
                    budget.charge(
                        new_batch
                            .mutation_allocation_bytes(&batch)
                            .saturating_add(matched_rows.len() * 4),
                    )?;
                    index_maintenance.record_memtable_update(&batch, &matched_rows, &assigned);
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
        if let Some(partition_key) = target_partition_key {
            mgr.seal_partition_active_under_table_lock(db, table, partition_key)?;
        } else {
            mgr.seal_table_active_under_table_lock(db, table)?;
        }

        let segment_files = if table_dir.exists() {
            super::dml_delete::prune_segments_by_partition(
                SegmentManager::discover_segments(&table_dir)?,
                where_clause,
                table_schema,
                table_meta,
            )
        } else {
            Vec::new()
        };
        // Assignments are constant per statement; resolve them once for every
        // segment/row group below.

        // Validation set up front: the segment dry-run below gathers the
        // surviving rows' key columns while it scans, so no second
        // validation sweep over the table is needed.
        let indexes_to_validate = indexes_touched_by_assignments(
            &table_meta.unique_indexes,
            table_schema,
            assigned.keys().copied().collect(),
        );
        let (validation_schema, validation_indices): (Schema, Vec<usize>) =
            if indexes_to_validate.is_empty() {
                (Schema::empty(), Vec::new())
            } else {
                let mut needed: Vec<usize> = indexes_to_validate
                    .iter()
                    .flat_map(|index| index.columns.iter())
                    .filter_map(|column| table_schema.index_of(column))
                    .collect();
                needed.sort_unstable();
                needed.dedup();
                let schema = Schema::new(
                    needed
                        .iter()
                        .map(|&idx| table_schema.columns[idx].clone())
                        .collect(),
                );
                (schema, needed)
            };

        // Segment leg: tombstone + reinsert (REPLACE-equivalent semantics).
        // The old two-pass whole-segment rewrite amplified a single-row
        // UPDATE into a full segment rewrite plus a per-index full-table
        // rebuild; under the no-amplification policy the matched rows are
        // tombstoned in the segment footer (an O(rows/8) bitmap rewrite,
        // the same primitive DELETE and REPLACE use) and their updated
        // images go through the standard insert path with incremental index
        // maintenance on both halves.
        //
        // Ordering is correctness-load-bearing: the dry run below touches
        // NOTHING durable; uniqueness validation runs on its output before
        // any footer is rewritten, so a key collision aborts the statement
        // with zero side effects. Only after validation do the footers
        // tombstone (with undo on IO failure) and the images insert.
        let mut segment_plan = plan_segment_rows_for_reinsert(
            mgr,
            index_maintenance,
            table_schema,
            segment_files,
            predicate,
            where_clause,
            predicate_projection,
            &assigned,
            publish_updates,
            &mut updated_events,
            &mut budget,
        )?;
        updated_count += segment_plan.updated_count;

        // Unique/PK validation. The comparison set is EVERY key that will
        // exist after the statement: ALL segment survivors (the dedicated
        // sweep below — never the WHERE-pruned dry-run scan, which cannot
        // see keys in pruned row groups/segments/partitions), every visible
        // memtable row (post-update images for rows this statement already
        // rewrote in memory), AND the new images themselves — two matched
        // rows assigned the same constant key must collide here, and a new
        // key must not equal any surviving key.
        if !indexes_to_validate.is_empty() {
            // Bounded-memory validation (P4): the keys this statement
            // INTRODUCES — reinserted images and updated memtable rows, both
            // already bounded by the statement's mutation budget — are
            // materialized once; every other live row is STREAMED past that
            // set without accumulation. Collisions purely among survivors
            // are pre-existing state this statement did not create (the
            // INSERT-side validator documents the same philosophy). The
            // previous shape collected every survivor's key columns into a
            // Vec before comparing, so a wide table's validation cost scaled
            // with the whole table, not the statement.
            let mut validator = super::dml_insert::IntroducedKeyValidator::new_with_budget(
                table,
                &validation_schema,
                &indexes_to_validate,
                &mut budget,
            );
            let projected_indices: Vec<usize> = validation_indices.to_vec();
            let identity_indices: Vec<usize> = (0..validation_indices.len()).collect();
            // Introduced: segment images (full catalog layout).
            for image in &segment_plan.image_batches {
                validator.introduce_full_batch(image, &projected_indices)?;
            }
            // Introduced: updated memtable rows — only the MATCHED rows
            // carry new keys; the rest keep theirs and stream below.
            for (_, batch, matched_rows, _) in &memtable_updates {
                validator.introduce_matched(batch, &projected_indices, matched_rows)?;
            }
            // Survivors: every visible memtable row this statement did not
            // rewrite.
            for memtable in engine
                .memtable_manager()
                .get_visible_to(db, table, reader_txn_id)
            {
                if let Some(updated) = memtable_updates
                    .iter()
                    .find(|(updated, _, _, _)| Arc::ptr_eq(updated, &memtable))
                {
                    let (_, batch, matched_rows, _) = updated;
                    validator.check_rows_excluding(batch, &projected_indices, matched_rows)?;
                } else if let Some(batch) = memtable.read_snapshot_arc() {
                    validator.check_rows_excluding(&batch, &projected_indices, &[])?;
                }
            }
            // Survivors in segments: probe the maintained unique-index
            // snapshots first — O(introduced keys) location lookups instead
            // of streaming every row group of the table's key columns
            // (which made even a 1-row point UPDATE pay a full-table scan).
            // Same trust model as the INSERT validator: a fresh snapshot's
            // answer decides; an ABSENT snapshot (invalidated index) falls
            // back to the streaming sweep below. Rows this statement
            // tombstones are excluded: their old keys retire, so a
            // self-assignment (SET uk = <current value>) must not collide
            // with its own image. Memtable-resident markers (empty segment
            // path) are covered by the memtable checks above.
            let mut planned_tombstones: std::collections::HashMap<std::path::PathBuf, Vec<u8>> =
                std::collections::HashMap::new();
            for (seg_path, new_bitmap, _, _) in &segment_plan.footers {
                planned_tombstones.insert(seg_path.clone(), new_bitmap.clone());
            }
            let mut sweep_needed = false;
            'probe: for index in &indexes_to_validate {
                let Some(keys) = validator.introduced_keys(&index.name) else {
                    continue;
                };
                for key in keys {
                    match engine.index_manager().unique_key_locations(
                        db,
                        table,
                        &index.name,
                        std::iter::once(key),
                    )? {
                        Some(locations) => {
                            for location in locations {
                                if location.segment_path.is_empty() {
                                    continue;
                                }
                                let path = std::path::PathBuf::from(&location.segment_path);
                                let tombstoned =
                                    planned_tombstones.get(&path).is_some_and(|bitmap| {
                                        crate::storage::segment::delete_bitmap::is_deleted(
                                            bitmap,
                                            location.row_id,
                                        )
                                    });
                                if !tombstoned {
                                    // Nothing durable has happened yet — clean abort.
                                    return Err(validator.probe_collision_error(&index.name, key));
                                }
                            }
                        }
                        None => {
                            sweep_needed = true;
                            break 'probe;
                        }
                    }
                }
            }
            if sweep_needed {
                // Fallback: ALL segment rows, streamed row group by row group
                // (never the WHERE-pruned dry-run scan — uniqueness is global
                // across row groups, segments and partitions).
                let mut check_error: Option<crate::common::error::TaibaiError> = None;
                for_each_surviving_key_row_group(
                    mgr,
                    &table_dir,
                    table_schema,
                    &validation_indices,
                    &planned_tombstones,
                    &mut |projected| {
                        if check_error.is_none() {
                            check_error = validator
                                .check_rows_excluding(projected, &identity_indices, &[])
                                .err();
                        }
                    },
                )?;
                if let Some(error) = check_error {
                    // Nothing durable has happened yet — clean abort.
                    return Err(error);
                }
            }
            validator.finish()?;
        }

        // New-key index entries are built BEFORE the batches move into the
        // write path (each entry's row_id is its batch row index).
        let unique_constraints: Vec<(String, Vec<usize>)> = table_meta
            .unique_indexes
            .iter()
            .filter_map(|index| {
                let indices: Option<Vec<usize>> = index
                    .columns
                    .iter()
                    .map(|column| table_schema.index_of(column))
                    .collect();
                indices.map(|indices| (index.name.clone(), indices))
            })
            .collect();
        let primary_key_indices: Vec<usize> = table_schema
            .columns
            .iter()
            .enumerate()
            .filter(|(_, column)| column.primary_key)
            .map(|(idx, _)| idx)
            .collect();
        let mut unique_index_entries: Vec<(
            String,
            Vec<crate::storage::index::index_manager::UniqueIndexEntry>,
        )> = Vec::new();
        let mut primary_keys: Vec<Vec<ScalarValue>> = Vec::new();
        for batch in &segment_plan.image_batches {
            if !unique_constraints.is_empty() {
                unique_index_entries.extend(
                    super::dml_insert::build_unique_index_entries_for_batch(
                        table_meta,
                        table_schema,
                        batch,
                        &unique_constraints,
                    )?,
                );
            }
            if !primary_key_indices.is_empty() {
                unique_index_entries.push((
                    crate::storage::index::index_manager::PRIMARY_LOCATION_INDEX_NAME.to_string(),
                    super::dml_insert::primary_location_entries_for_batch(
                        table_schema,
                        batch,
                        &primary_key_indices,
                    ),
                ));
                primary_keys.extend(super::dml_insert::primary_keys_for_batch(
                    batch,
                    table_schema,
                    &primary_key_indices,
                )?);
            }
        }

        // Only matched resident rows with changed keys produce new entries.
        // They use the same publish path as segment images and INSERT.
        for (_, batch, rows, _) in &memtable_updates {
            let touched: Vec<_> = unique_constraints
                .iter()
                .filter(|(_, columns)| columns.iter().any(|c| assigned.contains_key(c)))
                .cloned()
                .collect();
            if touched.is_empty() && !primary_key_indices.iter().any(|c| assigned.contains_key(c)) {
                continue;
            }
            let positions: std::sync::Arc<[u32]> = rows.clone().into();
            let images = RecordBatch::try_new(
                batch.schema.clone(),
                batch
                    .columns
                    .iter()
                    .map(|c| c.shared_take(positions.clone()))
                    .collect(),
            )?;
            unique_index_entries.extend(super::dml_insert::build_unique_index_entries_for_batch(
                table_meta,
                table_schema,
                &images,
                &touched,
            )?);
            if primary_key_indices.iter().any(|c| assigned.contains_key(c)) {
                unique_index_entries.push((
                    crate::storage::index::index_manager::PRIMARY_LOCATION_INDEX_NAME.into(),
                    super::dml_insert::primary_location_entries_for_batch(
                        table_schema,
                        &images,
                        &primary_key_indices,
                    ),
                ));
                primary_keys.extend(super::dml_insert::primary_keys_for_batch(
                    &images,
                    table_schema,
                    &primary_key_indices,
                )?);
            }
        }

        for (_, entries) in &unique_index_entries {
            budget.charge(
                entries
                    .iter()
                    .map(|entry| {
                        std::mem::size_of_val(entry)
                            + entry.key.capacity() * std::mem::size_of::<ScalarValue>()
                            + entry
                                .key
                                .iter()
                                .map(super::spill::scalar_heap_bytes)
                                .sum::<usize>()
                            + entry.location.segment_path.capacity()
                    })
                    .sum(),
            )?;
        }
        budget.charge(
            primary_keys
                .iter()
                .map(|key| {
                    key.capacity() * std::mem::size_of::<ScalarValue>()
                        + key
                            .iter()
                            .map(super::spill::scalar_heap_bytes)
                            .sum::<usize>()
                })
                .sum(),
        )?;

        // Phase 2a, ordered for statement atomicity:
        //   1. Stage the images into a PRIVATE transaction (invisible until
        //      its commit; nothing durable is rewritten).
        //   2. Tombstone the footers (apply_footers undoes its own partial
        //      rewrites on IO failure).
        //   3. Publish the staged rows with one commit.
        // Any failure before step 3 rolls the private transaction back and
        // the table is exactly as before. The previous order (footers first,
        // images second) had no undo once the footers were fsynced: a failed
        // image insert left the matched rows tombstoned with no replacement
        // — the client saw an error but the rows were gone.
        // Row count stays net-zero for an UPDATE: tombstones are not counted
        // and the staged inserts skip the catalog.
        let image_txn = engine.alloc_txn_id();
        let image_batches = std::mem::take(&mut segment_plan.image_batches);
        // Stage the images and collect their durability watermark in ONE
        // step through the shared staging entry. The watermark is only
        // meaningful after the staging writes created the transaction's
        // memtables; a collection placed before the staging once shipped an
        // always-empty watermark, and recovery — unable to prove the images
        // durable — rolled every leftover journal back, resurrecting updated
        // rows next to their committed images.
        let staged_images = match super::dml_staging::stage_image_transaction(
            engine,
            db,
            table,
            image_txn,
            |txn_id| {
                // The index feeds the test-only fault injection below.
                #[allow(clippy::unused_enumerate_index)]
                for (_batch_idx, batch) in image_batches.into_iter().enumerate() {
                    #[cfg(test)]
                    if _batch_idx == 1
                        && crate::common::fault_injection::should_fail(
                            crate::common::fault_injection::FaultPoint::UpdateImageAppend,
                        )
                    {
                        return Err(TaibaiError::Internal(
                            "forced UPDATE image append failure".into(),
                        ));
                    }
                    super::dml_insert::write_batch_by_partition(
                        engine,
                        db,
                        table,
                        table_meta,
                        batch,
                        table_schema.clone(),
                        txn_id,
                        true,
                        false,
                    )?;
                }
                Ok(())
            },
        ) {
            Ok(staged) => staged,
            Err((txn_id, error)) => {
                // Discard the staged (still-invisible) images. The clean-up is
                // best-effort beyond this point — the original error wins.
                if let Err(rollback_error) = engine.batch_writer().rollback_txn(txn_id) {
                    tracing::error!(
                        db,
                        table,
                        txn_id,
                        error = %rollback_error,
                        "UPDATE staging rollback failed; staged images may linger invisibly"
                    );
                }
                let _ = engine.txn_mutations().remove(txn_id);
                return Err(error);
            }
        };
        let image_watermark = staged_images.watermark;
        // Same id the staging ran under; read from the outcome so the pair
        // cannot drift apart at a call site.
        let image_txn = staged_images.txn_id;
        // Durability watermark for the deferred footer journals: the staged
        // images live in the private transaction's memtables; once their
        // tails are durable, recovery may keep the tombstones (roll forward),
        // otherwise it restores the pre-statement footers (roll back).
        if !segment_plan.footers.is_empty() {
            mutation_started.set(true);
        }
        if let Err(error) = segment_plan.apply_footers_deferred(mgr, image_watermark.as_slice()) {
            // apply_footers undoes its own partial rewrites; discard the
            // staged images (still invisible) as above.
            if let Err(rollback_error) = engine.batch_writer().rollback_txn(image_txn) {
                tracing::error!(
                    db,
                    table,
                    txn_id = image_txn,
                    error = %rollback_error,
                    "UPDATE footer-apply rollback failed; staged images may linger invisibly"
                );
            }
            let _ = engine.txn_mutations().remove(image_txn);
            return Err(error);
        }
        if let Err(error) = engine.batch_writer().commit_txn_under_table_lock(image_txn) {
            // The visibility flip inside commit may already have happened
            // (ambiguous commit — the engine's established semantics for a
            // failed durable-tail wait). The statement's effects are
            // consistent either way: footers tombstoned + images visible, or
            // neither. Nothing to roll back here.
            tracing::error!(
                db,
                table,
                txn_id = image_txn,
                error = %error,
                "UPDATE image commit failed after footers applied; transaction fate ambiguous"
            );
            let _ = engine.txn_mutations().remove(image_txn);
            return Err(error);
        }
        let _ = engine.txn_mutations().remove(image_txn);
        // The commit is durable: the tombstone leg is now unconditionally
        // correct. Clear the deferred journals (best effort — a leftover is
        // re-judged by startup recovery, which will find the watermark met).
        for (seg_path, ..) in &segment_plan.footers {
            crate::storage::segment::reader::clear_deferred_delete_journal(seg_path);
        }

        let mut tail_persist_failed = false;
        for (memtable, batch, matched_rows, assigned_pairs) in memtable_updates {
            mutation_started.set(true);
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
            if let Some(partition) = target_partition_key.filter(|key| !key.is_empty()) {
                engine
                    .batch_writer()
                    .flush_partition_under_partition_lock(db, table, partition)?;
            } else {
                engine
                    .batch_writer()
                    .flush_table_under_table_lock(db, table)?;
            }
        }

        // Incremental index maintenance, both halves, still under the
        // mutation lock: retire the old (key -> segment position) entries
        // recorded during the dry run, then append the images' entries
        // exactly like an INSERT would. `apply` returning false means the
        // precise-maintenance row cap tripped — fall back to snapshot
        // invalidation (DELETE's contract), never a rebuild.
        if !segment_plan.index_maintenance.apply(engine, db, table) {
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
        if !unique_index_entries.is_empty() {
            if let Err(error) = super::dml_insert::append_unique_index_entries_after_insert(
                engine,
                db,
                table,
                unique_index_entries,
            ) {
                tracing::warn!(
                    db,
                    table,
                    error = %error,
                    "UPDATE new-key index append failed; invalidating snapshots"
                );
                for index in &table_meta.unique_indexes {
                    engine
                        .index_manager()
                        .invalidate_unique_index_snapshot(db, table, &index.name);
                }
                engine
                    .index_manager()
                    .invalidate_primary_key_index(db, table);
            }
        }
        if !primary_keys.is_empty() {
            if let Err(error) = engine
                .index_manager()
                .append_primary_keys(db, table, &primary_keys)
            {
                tracing::warn!(
                    db,
                    table,
                    error = %error,
                    "UPDATE primary-key append failed; invalidating location index"
                );
                engine
                    .index_manager()
                    .invalidate_primary_key_index(db, table);
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

        Ok(updated_count)
    };

    let update_outcome = match target_partition_key.filter(|key| !key.is_empty()) {
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
    if update_outcome.is_err() && mutation_started.get() {
        engine.invalidate_hot_store(db, table);
        engine
            .index_manager()
            .invalidate_primary_key_index(db, table);
        let names = table_meta
            .unique_indexes
            .iter()
            .map(|index| index.name.clone())
            .collect::<Vec<_>>();
        engine
            .index_manager()
            .invalidate_table_unique_index_snapshots(db, table, &names);
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
    assigned: &HashMap<usize, ScalarValue>,
    where_clause: &Option<Expr>,
    table_schema: &Schema,
    batch: Arc<RecordBatch>,
) -> Result<MemtableUpdateOutcome> {
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

    // Canonical ascending column order, ready for the durable `.upd` sidecar
    // (which replays these constants by physical column index).
    let assigned_pairs: Vec<(usize, ScalarValue)> = assigned
        .iter()
        .map(|(col_idx, value)| (*col_idx, value.clone()))
        .collect();
    let affected = matched_rows.len() as u64;
    let new_columns = batch
        .columns
        .iter()
        .enumerate()
        .map(|(idx, column)| match assigned.get(&idx) {
            Some(value) => column.shared_update(&mask, value),
            None => Ok(column.shared_clone()),
        })
        .collect::<Result<Vec<_>>>()?;
    let new_batch = RecordBatch::try_new(batch.schema.clone(), new_columns)?;
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
    timezone_offset_hours: i32,
) -> Result<HashMap<usize, ScalarValue>> {
    let mut assigned = HashMap::new();
    for assign in assignments {
        if let Some(col_idx) = table_schema.index_of(&assign.column) {
            if let std::collections::hash_map::Entry::Vacant(e) = assigned.entry(col_idx) {
                let value = eval_expr_to_scalar_with_scale_timezone(
                    &assign.value,
                    &table_schema.columns[col_idx].data_type,
                    table_schema.columns[col_idx].scale,
                    timezone_offset_hours,
                )?;
                let column = &table_schema.columns[col_idx];
                if (column.primary_key || !column.nullable) && value.is_null() {
                    return Err(TaibaiError::Sql(format!(
                        "column '{}' cannot be NULL",
                        column.name
                    )));
                }
                e.insert(value);
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

/// Dry-run plan of the segment leg of an auto-commit UPDATE (tombstone +
/// reinsert). Phase 1 (`collect`) scans and gathers everything the
/// validation needs WITHOUT touching a single footer; phase 2 (`apply`)
/// tombstones footers (with undo) and hands the image batches to the
/// caller for insertion. Keeping validation ahead of every footer write is
/// what makes a uniqueness failure a clean abort — the previous single-pass
/// shape tombstoned rows first and lost them on the error path.
struct SegmentReinsertPlan {
    /// Per segment: (path, full-length new bitmap, new deleted count,
    /// undo = previous bitmap bytes + previous deleted count).
    footers: Vec<FooterReinsertEntry>,
    image_batches: Vec<RecordBatch>,
    index_maintenance: crate::sql::execution::dml_delete::DeleteIndexMaintenance,
    updated_count: u64,
}

/// Stream every live segment row's key columns past `visit` — deliberately
/// NOT pruned by the statement's WHERE or partition scope: uniqueness is
/// global, and collecting validation keys inside the pruned dry-run scan
/// (5a30a42) let `UPDATE ... SET uk = <existing key in a pruned row
/// group/segment/partition>` commit a physical duplicate. The sweep reads
/// one row group at a time (narrow key projection, name-aligned for drift
/// layouts) and hands the gathered survivors to the callback, so memory is
/// bounded by one row group instead of the whole table. Rows marked in
/// `planned_tombstones` (this statement's not-yet-applied new footers) are
/// excluded alongside footer-dead rows: their old keys retire.
fn for_each_surviving_key_row_group(
    mgr: &crate::storage::segment_manager::SegmentManager,
    table_dir: &std::path::Path,
    table_schema: &Schema,
    validation_indices: &[usize],
    planned_tombstones: &std::collections::HashMap<std::path::PathBuf, Vec<u8>>,
    visit: &mut dyn FnMut(&RecordBatch),
) -> Result<()> {
    if !table_dir.exists() {
        return Ok(());
    }
    let mut validation_schema_columns = Vec::with_capacity(validation_indices.len());
    for &idx in validation_indices {
        validation_schema_columns.push(table_schema.columns[idx].clone());
    }
    let validation_schema = Schema::new(validation_schema_columns);
    for seg_path in crate::storage::segment_manager::SegmentManager::discover_segments(table_dir)? {
        let reader = mgr.open_reader_cached(&seg_path)?;
        let planned = planned_tombstones.get(&seg_path);
        for rg_idx in 0..reader.num_row_groups() {
            // Name-based narrow read: catalog indices hit the wrong physical
            // column on segments whose footer predates a DROP/RENAME COLUMN.
            let batch = reader.read_row_group_columns(rg_idx, table_schema, validation_indices)?;
            let rg_start = reader.row_group_start_row(rg_idx);
            let delete_bitmap = reader.delete_bitmap();
            let survivors: Vec<u32> = (0..batch.num_rows() as u32)
                .filter(|&row_idx| {
                    let absolute = rg_start + row_idx as u64;
                    !crate::storage::segment::delete_bitmap::is_deleted(delete_bitmap, absolute)
                        && !planned.is_some_and(|bitmap| {
                            crate::storage::segment::delete_bitmap::is_deleted(bitmap, absolute)
                        })
                })
                .collect();
            if survivors.is_empty() {
                continue;
            }
            let gathered = RecordBatch::try_new(
                validation_schema.clone(),
                (0..batch.num_columns())
                    .map(|col_idx| batch.columns[col_idx].clone().take(&survivors))
                    .collect(),
            )
            .expect("validation projection preserves column types");
            visit(&gathered);
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn plan_segment_rows_for_reinsert(
    mgr: &crate::storage::segment_manager::SegmentManager,
    mut index_maintenance: super::dml_delete::DeleteIndexMaintenance,
    table_schema: &Schema,
    segment_files: Vec<std::path::PathBuf>,
    predicate: Option<&crate::storage::segment::RowGroupPredicate>,
    where_clause: &Option<Expr>,
    predicate_projection: &[usize],
    assigned: &std::collections::HashMap<usize, ScalarValue>,
    publish_updates: bool,
    updated_events: &mut super::helpers::ChangeEventBuffer,
    budget: &mut super::dml_staging::MutationBudget,
) -> Result<SegmentReinsertPlan> {
    let maintaining = index_maintenance.still_maintaining();
    let key_positions: Vec<Vec<usize>> = index_maintenance.schema_key_positions().to_vec();

    let mut footers = Vec::new();
    let mut image_pending: Option<RecordBatch> = None;
    let mut image_batches: Vec<RecordBatch> = Vec::new();
    let mut updated_count = 0u64;
    let row_group_target = mgr.row_group_target_rows();

    for seg_path in segment_files {
        let reader = mgr.open_reader(&seg_path)?;
        if !super::helpers::segment_may_match(&reader, table_schema, predicate) {
            continue;
        }
        let aligned_predicate =
            predicate.and_then(|pred| reader.align_row_group_predicate(table_schema, pred).ok());
        let footer_bitmap = reader.delete_bitmap().to_vec();
        let base_deleted = reader.deleted_row_count();
        // Compose a FULL-LENGTH bitmap (footer bitmaps only cover rows that
        // had deletions when they were written): mark_deleted on a short
        // slice is a silent no-op.
        let bitmap_bytes = reader.row_count().div_ceil(8) as usize;
        let mut new_bitmap = vec![0u8; bitmap_bytes];
        let copy_len = footer_bitmap.len().min(bitmap_bytes);
        new_bitmap[..copy_len].copy_from_slice(&footer_bitmap[..copy_len]);
        let mut newly_deleted = 0u64;
        let segment_id =
            maintaining.then(|| index_maintenance.intern_segment(&seg_path.to_string_lossy()));
        let mut segment_matched = false;

        for rg_idx in 0..reader.num_row_groups() {
            if let Some(pred) = &aligned_predicate {
                if !pred.matches_row_group(reader.zone_maps(rg_idx)) {
                    continue;
                }
            }
            let mask = build_update_mask(
                &reader,
                rg_idx,
                where_clause,
                predicate_projection,
                table_schema,
            )?;
            let any_matched = mask.iter().any(|m| *m);
            // Read the row group only when something consumes it: matched
            // rows feed images + index retirement. Index maintenance alone
            // never needs the batch (it records matched rows only), so the
            // old `!maintaining` guard forced a full-table full-column
            // decode on every PK-bearing table.
            if !any_matched {
                continue;
            }
            // Catalog-ordered whole-row-group read: assignments map catalog
            // indices and the images must carry the catalog layout.
            let batch = reader.read_row_group_columns_aligned(rg_idx, table_schema)?;
            let start_row = reader.row_group_start_row(rg_idx);
            let mut matched_rows: Vec<u32> = Vec::new();
            for (row_idx, matches) in mask.iter().enumerate() {
                let absolute_row = start_row + row_idx as u64;
                if crate::storage::segment::delete_bitmap::is_deleted(&footer_bitmap, absolute_row)
                {
                    continue;
                }
                if *matches {
                    matched_rows.push(row_idx as u32);
                    crate::storage::segment::delete_bitmap::mark_deleted(
                        &mut new_bitmap,
                        absolute_row,
                    );
                    newly_deleted += 1;
                }
            }
            if let Some(segment_id) = segment_id {
                if !matched_rows.is_empty() {
                    // The old (key -> position) entries retire at apply
                    // time; record from the PRE-image batch.
                    for row_idx in &matched_rows {
                        index_maintenance.record_segment_row(
                            segment_id,
                            start_row + *row_idx as u64,
                            &batch,
                            *row_idx as usize,
                            &key_positions,
                        );
                    }
                }
            }
            if !matched_rows.is_empty() {
                segment_matched = true;
                let matched_matched = vec![true; matched_rows.len()];
                let images =
                    rebuild_update_batch(&batch, assigned, &matched_rows, &matched_matched)?;
                if publish_updates {
                    let idxs: Vec<u32> = (0..images.num_rows() as u32).collect();
                    updated_events.push(&images, &idxs);
                }
                updated_count += matched_rows.len() as u64;
                budget.charge(images.estimated_size_bytes().saturating_mul(2))?;
                image_pending = match image_pending.take() {
                    Some(mut accumulated) => {
                        for col_idx in 0..images.num_columns() {
                            let source = images.column(col_idx).expect("validated column");
                            accumulated
                                .column_mut(col_idx)
                                .expect("validated column")
                                .extend(source)?;
                        }
                        Some(accumulated)
                    }
                    None => Some(images),
                };
                if image_pending.as_ref().expect("just set").num_rows() >= row_group_target {
                    image_batches.push(image_pending.take().expect("just set"));
                }
            }
        }
        drop(reader);
        if segment_matched {
            budget.charge(new_bitmap.len().saturating_add(footer_bitmap.len()))?;
            footers.push((
                seg_path,
                new_bitmap,
                base_deleted + newly_deleted,
                (footer_bitmap, base_deleted),
            ));
        }
    }
    if let Some(pending) = image_pending.take() {
        image_batches.push(pending);
    }
    Ok(SegmentReinsertPlan {
        footers,
        image_batches,
        index_maintenance,
        updated_count,
    })
}

impl SegmentReinsertPlan {
    /// Phase 2a: tombstone every collected footer under the DEFERRED journal
    /// lifecycle — each rewrite's journal also records the pre-statement
    /// footer (undo) and the statement's image watermark, and survives until
    /// the caller clears it after the images are durable. A crash anywhere
    /// in between rolls the footers back at startup
    /// (`recover_statement_footer_journals`).
    ///
    /// On failure the already applied footers are restored from their undo
    /// snapshots, so an IO error is a clean abort rather than a half-deleted
    /// table. Each restore rewrite runs under the IMMEDIATE journal
    /// lifecycle, which overwrites and consumes the deferred journal of the
    /// same segment — the directory holds no pending statement state after a
    /// successful undo.
    fn apply_footers_deferred(
        &self,
        mgr: &crate::storage::segment_manager::SegmentManager,
        image_checks: &[crate::storage::segment::reader::ImageDurabilityCheck],
    ) -> Result<()> {
        // Matched segment rows imply staged image rows imply a non-empty
        // watermark. An empty watermark here means the collection ran before
        // the staging (the regression this asserts against): every journal
        // would carry a watermark that can never be satisfied, and any crash
        // in the commit→clear window would resurrect the old rows next to
        // the committed images.
        debug_assert!(
            self.footers.is_empty() || !image_checks.is_empty(),
            "deferred footer journals of a matched UPDATE must carry a non-empty image watermark"
        );
        let mut applied = std::collections::VecDeque::<(std::path::PathBuf, Vec<u8>, u64)>::new();
        for (seg_path, new_bitmap, new_deleted, (old_bitmap, old_deleted)) in &self.footers {
            if let Err(error) = mgr.rewrite_delete_footer_with_mode(
                seg_path,
                new_bitmap,
                *new_deleted,
                crate::storage::segment::reader::FooterRewriteMode::Deferred(image_checks.to_vec()),
            ) {
                // Roll the applied footers back: the statement aborts with
                // the table exactly as it was.
                while let Some((path, bitmap, deleted)) = applied.pop_front() {
                    let _ = mgr.rewrite_delete_footer(&path, &bitmap, deleted);
                }
                return Err(error);
            }
            applied.push_back((seg_path.clone(), old_bitmap.clone(), *old_deleted));
        }
        Ok(())
    }
}
