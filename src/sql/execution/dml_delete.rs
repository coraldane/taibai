use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use crate::common::error::{Result, TaibaiError};
use crate::storage::catalog::schema::{PartitionType, TableMeta};
use crate::storage::core::memtable::{MemTable, TxnVisibility};
use crate::storage::index::index_manager::UniqueRowLocation;
use crate::storage::index::sort_key::compare_scalars_for_sort;
use crate::storage::partition::pruning::ZoneMap;
use crate::storage::partition::{
    parse_partition_key, partition_function::compute_partition_key_from_scalar,
};
use crate::storage::segment::delete_bitmap;
use crate::storage::segment::RangeOp;
use crate::storage::segment_manager::SegmentManager;
use crate::types::batch::{RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

use super::engine::{Engine, ExecResult};
use super::helpers::{eval_expr_to_scalar, eval_where_clause_batch};
use super::scan::cluster_prune;
use super::scan::row_filter::ClusterKeyBounds;
use crate::sql::parser::ast::{BinOp, Expr};

/// One segment footer rewrite performed by a REPLACE statement:
/// `(path, pre-rewrite bitmap, pre-rewrite deleted count, post-rewrite
/// bitmap, post-rewrite deleted count)`. The pre-state restores the footer
/// on rollback; the post-state identifies OUR deferred journal once the
/// statement is durable and clears it.
pub(crate) type ReplaceSegmentFooterWrite = (PathBuf, Vec<u8>, u64, Vec<u8>, u64);

#[derive(Default)]
pub(crate) struct ReplaceDeleteUndo {
    pub(crate) segment_footers: Vec<ReplaceSegmentFooterWrite>,
    deleted_rows: u64,
    deleted_locations: Vec<UniqueRowLocation>,
    /// Original batches of memtables whose conflicting rows were filtered out,
    /// restored verbatim on rollback.
    memtable_backups: Vec<(Arc<MemTable>, Arc<RecordBatch>)>,
}

impl ReplaceDeleteUndo {
    pub(crate) fn deleted_rows(&self) -> u64 {
        self.deleted_rows
    }

    pub(crate) fn merge(&mut self, other: ReplaceDeleteUndo) {
        self.segment_footers.extend(other.segment_footers);
        self.deleted_rows += other.deleted_rows;
        self.deleted_locations.extend(other.deleted_locations);
        self.memtable_backups.extend(other.memtable_backups);
    }
}

/// A planned memtable conflict delete: the memtable, its original batch, the
/// batch with conflicting rows removed, and the ascending positions removed.
/// Computed before any mutation so the delete stays two-phase (plan, then
/// apply). Positions let the apply step compact the REPLACE key index instead
/// of invalidating it.
pub(crate) struct ReplaceMemtableConflictPlan {
    pub(crate) memtable: Arc<MemTable>,
    pub(crate) original: Arc<RecordBatch>,
    pub(crate) filtered: Arc<RecordBatch>,
    pub(crate) removed_positions: Vec<u32>,
}

fn filter_batch_keep(batch: &RecordBatch, keep: &[bool], keep_count: usize) -> Result<RecordBatch> {
    let columns = batch
        .columns
        .iter()
        .map(|column| column.filter_by_mask_fast(keep, keep_count))
        .collect();
    RecordBatch::try_new(batch.schema.clone(), columns)
}

/// Find rows in the table's committed, non-transactional pending memtables
/// conflicting with `key_sets`. The caller holds the table mutation lock, so
/// no INSERT or flush can mutate the memtables between this plan and
/// `apply_replace_memtable_conflicts`. Conflict matching probes a lazily
/// built per-memtable key index (incrementally maintained across appends)
/// instead of rescanning every memtable row per statement.
pub(crate) fn plan_replace_memtable_conflicts(
    engine: &Engine,
    db: &str,
    table: &str,
    key_sets: &[(Vec<usize>, HashSet<Vec<ScalarValue>>)],
) -> Result<Vec<ReplaceMemtableConflictPlan>> {
    let mut plans = Vec::new();
    if key_sets.is_empty() {
        return Ok(plans);
    }
    for memtable in engine.memtable_manager().get_by_table(db, table) {
        if memtable.is_transactional() || memtable.visibility() != TxnVisibility::Committed {
            continue;
        }
        // Share the memtable's own Arc as the undo backup instead of
        // deep-copying the batch; the table mutation lock is held, so the
        // resident batch cannot change between the read and the swap.
        let planned = memtable.read_snapshot_arc().and_then(|batch| {
            if batch.num_rows() == 0 {
                return None;
            }
            let positions = engine
                .memtable_manager()
                .replace_key_conflicts(&memtable, key_sets, &batch);
            if positions.is_empty() {
                return None;
            }
            let removed = positions.len();
            let mut keep = vec![true; batch.num_rows()];
            for &row_idx in &positions {
                keep[row_idx as usize] = false;
            }
            let filtered = filter_batch_keep(&batch, &keep, batch.num_rows() - removed).ok()?;
            Some((batch, Arc::new(filtered), positions))
        });
        if let Some((original, filtered, removed_positions)) = planned {
            plans.push(ReplaceMemtableConflictPlan {
                memtable,
                original,
                filtered,
                removed_positions,
            });
        }
    }
    Ok(plans)
}

/// Scan the table's committed, non-transactional pending memtables for rows
/// matching the DELETE predicate. Reuses the REPLACE two-phase plan/apply
/// machinery so the physical removal and durable-tail rewrite are identical.
/// The single walk over a table's visible memtables shared by the DELETE
/// planner, auto-commit UPDATE, and transactional UPDATE: memtables visible
/// to the statement's transaction, optionally narrowed to one partition,
/// each yielded with
/// - its original snapshot (positions in plans, tombstone registrations and
///   events are absolute against this batch), and
/// - the registry's transaction-tombstone view of it (`live_positions`
///   maps view coordinates back to original ones; an identity view when
///   nothing is tombstoned — including every auto-commit statement).
///
/// The walk-and-view they used to re-derive per caller is
/// `TxnMutationRegistry::memtable_batch_visible_to_txn`; new memtable-side
/// consumers must iterate through here so the visibility rules stay in one
/// place.

/// One segment's DELETE match result (auto-commit and deferred transactional
/// paths share the walk).
pub(crate) struct SegmentDeleteMatch {
    /// The segment's tombstone bitmap as it was before this statement —
    /// deferred-DML registration needs the pre-statement state.
    pub footer_bitmap: Vec<u8>,
    /// The working bitmap: footer tombstones + `seed_tombstones` + every
    /// newly matched row marked.
    pub new_bitmap: Vec<u8>,
    /// Absolute row ids newly matched by this statement, in row order.
    pub matched_ids: Vec<u64>,
}

/// The shared per-segment DELETE match walk: seed the working bitmap from
/// the segment's footer tombstones plus `seed_tombstones` (tombstones
/// recorded by earlier statements of the same transaction), then walk row
/// groups — skipping fully-deleted ranges and zone-map-pruned groups —
/// marking every row the WHERE clause selects.
///
/// `need_batches` reads each candidate row group even without a WHERE
/// clause so batch-granularity consumers can run; the auto-commit path sets
/// it for change events and index maintenance, the transactional path leaves
/// it off for WHERE-less deletes (which today publish no events — nothing
/// was read to publish). `read_row_group` picks full vs projected reads;
/// `on_row_group` observes each candidate group's matches as
/// (in-group index, absolute row id) pairs against the read batch.
pub(crate) fn match_segment_for_delete(
    reader: &crate::storage::segment::SegmentReader,
    table_schema: &Schema,
    predicate: Option<&crate::storage::segment::RowGroupPredicate>,
    where_clause: Option<&Expr>,
    seed_tombstones: &[u64],
    need_batches: bool,
    read_row_group: impl Fn(&crate::storage::segment::SegmentReader, usize) -> Result<RecordBatch>,
    mut on_row_group: impl FnMut(&RecordBatch, &[(u32, u64)]) -> Result<()>,
) -> Result<SegmentDeleteMatch> {
    let total_rows = reader.row_count();
    let bitmap_bytes = total_rows.div_ceil(8) as usize;
    let footer_bitmap = reader.delete_bitmap().to_vec();
    let mut new_bitmap = vec![0u8; bitmap_bytes];
    let copy_len = footer_bitmap.len().min(bitmap_bytes);
    new_bitmap[..copy_len].copy_from_slice(&footer_bitmap[..copy_len]);
    for row_id in seed_tombstones {
        delete_bitmap::mark_deleted(&mut new_bitmap, *row_id);
    }
    // Re-anchor the catalog-space predicate onto this segment's footer zone
    // maps — positional use after a DROP/RENAME COLUMN prunes the WRONG row
    // groups. A column absent from this older segment carries its DEFAULT on
    // every row (default-fill semantics, shared with UPDATE/SELECT/COUNT):
    // zone maps cannot decide such clauses, so keep every row group a
    // candidate and let the row-wise mask on default-filled batches decide —
    // the silent "no match" used here dropped rows a DELETE had to remove.
    let aligned_predicate = match predicate {
        Some(pred) => {
            let clause_columns: Vec<usize> =
                pred.clauses.iter().map(|clause| clause.column_index).collect();
            if crate::storage::segment::reader::any_column_absent_in_schema(
                table_schema,
                reader.schema(),
                &clause_columns,
            ) {
                None
            } else {
                match reader.align_row_group_predicate(table_schema, pred) {
                    Ok(aligned) => Some(aligned),
                    // Out-of-range catalog index is an internal error, not a
                    // provable no-match: fail loudly instead of skipping the
                    // segment silently.
                    Err(error) => return Err(error),
                }
            }
        }
        None => None,
    };

    let mut matched_ids = Vec::new();
    let mut row_id: u64 = 0;
    for rg_idx in 0..reader.num_row_groups() {
        let rg_rows = reader.row_group_rows(rg_idx);
        if delete_bitmap::row_range_all_deleted(&new_bitmap, row_id, rg_rows) {
            row_id += rg_rows;
            continue;
        }
        if let Some(pred) = &aligned_predicate {
            if !pred.matches_row_group(reader.zone_maps(rg_idx)) {
                row_id += rg_rows;
                continue;
            }
        }
        if !where_clause.is_some() && !need_batches {
            // Delete-everything-in-the-group fast path: no batch is read, so
            // there is nothing for batch-granularity consumers to observe.
            for i in 0..rg_rows {
                if delete_bitmap::mark_deleted(&mut new_bitmap, row_id + i) {
                    matched_ids.push(row_id + i);
                }
            }
            row_id += rg_rows;
            continue;
        }
        let batch = read_row_group(reader, rg_idx)?;
        let mask = match where_clause {
            Some(where_expr) => eval_where_clause_batch(where_expr, &batch, &batch.schema)?,
            None => vec![true; batch.num_rows()],
        };
        let mut matched: Vec<(u32, u64)> = Vec::new();
        for (i, matches) in mask.into_iter().enumerate() {
            if matches && delete_bitmap::mark_deleted(&mut new_bitmap, row_id + i as u64) {
                matched.push((i as u32, row_id + i as u64));
            }
        }
        if !matched.is_empty() {
            on_row_group(&batch, &matched)?;
            matched_ids.extend(matched.iter().map(|(_, absolute)| *absolute));
        }
        row_id += batch.num_rows() as u64;
    }
    Ok(SegmentDeleteMatch {
        footer_bitmap,
        new_bitmap,
        matched_ids,
    })
}

pub(crate) fn for_each_visible_memtable(
    engine: &Engine,
    db: &str,
    table: &str,
    table_schema: &Schema,
    reader_txn_id: u64,
    narrowed_partition: Option<&str>,
    mut consume: impl FnMut(
        Arc<crate::storage::core::memtable::MemTable>,
        Arc<RecordBatch>,
        Arc<RecordBatch>,
        Option<Vec<u32>>,
    ) -> Result<()>,
) -> Result<()> {
    // Visible-to-this-transaction memtables: committed rows plus this
    // transaction's own uncommitted INSERTs (so a DELETE/UPDATE inside a
    // transaction reaches rows the same transaction just wrote). Rows of
    // other still-open transactions stay untouched.
    for memtable in engine
        .memtable_manager()
        .get_visible_to(db, table, reader_txn_id)
    {
        if let Some(key) = narrowed_partition {
            if memtable.partition_key() != key {
                continue;
            }
        }
        let Some(original) = memtable.read_snapshot_arc() else {
            continue;
        };
        if original.num_rows() == 0 {
            continue;
        }
        let (view, live_positions) = engine.txn_mutations().memtable_batch_visible_to_txn(
            reader_txn_id,
            &memtable,
            &original,
        )?;
        // A memtable recovered from a pre-ALTER durable tail keeps its
        // insert-time layout while the plan speaks catalog positions and
        // names. Align the view to the catalog before any consumer touches
        // it — the DELETE planner resolves predicate columns against
        // table_schema and would otherwise read the wrong physical column
        // after a DROP COLUMN shifted positions. Alignment is
        // row-preserving, so `live_positions` and the consumers'
        // absolute-position lifts stay valid. `original` stays untouched:
        // the memtable's own batch (and its tail) keep their layout until
        // an UPDATE swaps in an aligned replacement.
        let view = if crate::storage::core::memtable::batch_matches_schema(&view, table_schema) {
            view
        } else {
            Arc::new(crate::storage::core::memtable::align_batch_to_schema(
                &view, table_schema,
            )?)
        };
        consume(memtable, original, view, live_positions)?;
    }
    Ok(())
}

/// The caller holds the table mutation lock.
pub(crate) fn plan_delete_memtable_rows(
    engine: &Engine,
    db: &str,
    table: &str,
    where_clause: &Option<Expr>,
    table_schema: &Schema,
    target_partition_key: Option<&str>,
    reader_txn_id: u64,
) -> Result<Vec<ReplaceMemtableConflictPlan>> {
    let mut plans = Vec::new();
    for_each_visible_memtable(
        engine,
        db,
        table,
        table_schema,
        reader_txn_id,
        target_partition_key,
        |memtable, original, view, live_positions| {
            if view.num_rows() == 0 {
                return Ok(());
            }
            // Propagate evaluation errors: swallowing them here used to make the
            // memtable look "no match", deleting segment rows while leaving the
            // memtable rows alive.
            let mask = match where_clause.as_ref() {
                Some(expr) => eval_where_clause_batch(expr, &view, table_schema)?,
                None => vec![true; view.num_rows()],
            };
            // Lift the matches back to absolute positions in the original batch
            // so plans, tombstone registrations and delete events all speak the
            // same coordinate system.
            let mut absolute_mask = vec![false; original.num_rows()];
            for (filtered_idx, &matches) in mask.iter().enumerate() {
                if matches {
                    let absolute = live_positions
                        .as_ref()
                        .map_or(filtered_idx as u32, |map| map[filtered_idx]);
                    absolute_mask[absolute as usize] = true;
                }
            }
            let removed = absolute_mask.iter().filter(|matches| **matches).count();
            if removed > 0 {
                let keep = absolute_mask
                    .iter()
                    .map(|matches| !matches)
                    .collect::<Vec<_>>();
                let filtered = filter_batch_keep(&original, &keep, original.num_rows() - removed)?;
                let removed_positions = absolute_mask
                    .iter()
                    .enumerate()
                    .filter_map(|(row_idx, matches)| matches.then_some(row_idx as u32))
                    .collect::<Vec<_>>();
                plans.push(ReplaceMemtableConflictPlan {
                    memtable,
                    original,
                    filtered: Arc::new(filtered),
                    removed_positions,
                });
            }
            Ok(())
        },
    )?;
    Ok(plans)
}

/// Apply previously planned memtable conflict deletes: swap each memtable's
/// batch for the filtered one and rewrite its durable tail so crash recovery
/// cannot resurrect removed rows. On any failure everything applied so far is
/// restored before the error is returned.
pub(crate) fn apply_replace_memtable_conflicts(
    engine: &Engine,
    db: &str,
    table: &str,
    plans: Vec<ReplaceMemtableConflictPlan>,
) -> Result<ReplaceDeleteUndo> {
    let mut undo = ReplaceDeleteUndo::default();
    if plans.is_empty() {
        return Ok(undo);
    }
    // Drain group-committed tail appends so no in-flight INSERT delta can land
    // after a rewritten tail snapshot.
    engine.batch_writer().drain_table_tail(db, table)?;
    let table_dir = engine.data_dir().join(db).join(table);
    for plan in plans {
        let removed = (plan.original.num_rows() - plan.filtered.num_rows()) as u64;
        if !plan.memtable.replace_pending_batch(plan.filtered.clone()) {
            let _ = restore_replace_memtable_undo(engine, &undo);
            return Err(TaibaiError::Internal(
                "REPLACE conflict delete raced with a memtable flush".to_string(),
            ));
        }
        engine.memtable_manager().compact_replace_key_index(
            &plan.memtable,
            &plan.removed_positions,
            plan.original.num_rows(),
        );
        // A transactional memtable has no durable tail yet — its rows only
        // reach the tail at COMMIT — so only committed memtables rewrite
        // the tail here. The in-memory batch swap above is exactly what
        // ROLLBACK needs to undo.
        let tail_result = if plan.memtable.is_transactional() {
            Ok(())
        } else if plan.filtered.num_rows() == 0 {
            // Nothing survived the delete: a clear tombstone (fail-closed)
            // replaces the memtable's tail instead of persisting an empty
            // snapshot generation.
            crate::storage::core::durable_tail::clear(&table_dir, plan.memtable.id())
        } else {
            // Incremental first: a delete sidecar costs O(affected rows)
            // instead of rewriting the whole tail snapshot (quadratic for
            // repeated small deletes on a large memtable). Falls back to a
            // full persist when no live tail exists to hang the sidecar on.
            match crate::storage::core::durable_tail::delete_rows(
                &table_dir,
                plan.memtable.id(),
                &plan.removed_positions,
            ) {
                Ok(true) => Ok(()),
                Ok(false) => crate::storage::core::durable_tail::persist(
                    &table_dir,
                    plan.memtable.id(),
                    plan.memtable.partition_key(),
                    &plan.filtered,
                ),
                Err(error) => Err(error),
            }
        };
        if let Err(error) = tail_result {
            let _ = plan.memtable.replace_pending_batch(plan.original.clone());
            if !plan.memtable.is_transactional() {
                let _ = crate::storage::core::durable_tail::persist(
                    &table_dir,
                    plan.memtable.id(),
                    plan.memtable.partition_key(),
                    &plan.original,
                );
            }
            let _ = restore_replace_memtable_undo(engine, &undo);
            return Err(error);
        }
        undo.memtable_backups.push((plan.memtable, plan.original));
        undo.deleted_rows += removed;
    }
    Ok(undo)
}

fn restore_replace_memtable_undo(engine: &Engine, undo: &ReplaceDeleteUndo) -> Result<()> {
    let mut first_error = None;
    for (memtable, original) in undo.memtable_backups.iter().rev() {
        if !memtable.replace_pending_batch(original.clone()) {
            let error = TaibaiError::Internal(
                "REPLACE rollback could not restore a flushed memtable".to_string(),
            );
            tracing::error!(
                db = memtable.db(),
                table = memtable.table(),
                "REPLACE rollback: memtable left pending state before restore"
            );
            if first_error.is_none() {
                first_error = Some(error);
            }
            continue;
        }
        let table_dir = engine.data_dir().join(memtable.db()).join(memtable.table());
        if let Err(error) = crate::storage::core::durable_tail::persist(
            &table_dir,
            memtable.id(),
            memtable.partition_key(),
            original,
        ) {
            tracing::warn!(
                db = memtable.db(),
                table = memtable.table(),
                error = %error,
                "REPLACE rollback: failed to restore durable tail; flushing memtable"
            );
            if let Err(flush_error) = engine
                .batch_writer()
                .flush_memtable_restore_fallback_under_table_lock(memtable)
            {
                tracing::error!(
                    db = memtable.db(),
                    table = memtable.table(),
                    error = %flush_error,
                    "REPLACE rollback: fallback flush failed"
                );
                if first_error.is_none() {
                    first_error = Some(flush_error);
                }
                continue;
            }
            if first_error.is_none() {
                first_error = Some(error);
            }
        }
    }
    match first_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

/// Delete rows conflicting with the REPLACE key sets. Segment legs rewrite
/// footers under the DEFERRED journal lifecycle: the caller passes the
/// durability watermark of the REPLACE's staged replacement rows, so a crash
/// between a footer's fsync and the staged insert's commit restores the old
/// rows at startup instead of losing them (the same protocol as UPDATE's
/// tombstone leg). The caller must have staged those rows into the private
/// transaction BEFORE calling, so the watermark references real memtables.
///
/// The caller holds the table mutation lock.
pub(crate) fn delete_replace_conflicts_under_table_lock(
    engine: &Engine,
    db: &str,
    table: &str,
    table_meta: &TableMeta,
    key_sets: &[(Vec<usize>, HashSet<Vec<ScalarValue>>)],
    image_checks: &[crate::storage::segment::reader::ImageDurabilityCheck],
) -> Result<ReplaceDeleteUndo> {
    let table_dir = engine.data_dir().join(db).join(table);
    let mut undo = ReplaceDeleteUndo::default();
    if !table_dir.exists() || key_sets.is_empty() {
        return Ok(undo);
    }

    let primary_key_indices = table_meta
        .columns
        .iter()
        .enumerate()
        .filter_map(|(index, column)| column.primary_key.then_some(index))
        .collect::<Vec<_>>();
    let mut fallback = vec![false; key_sets.len()];
    let schema = Schema::new(table_meta.columns.clone());
    let mut direct_locations = Vec::new();
    if !primary_key_indices.is_empty() {
        if let Some((key_set_offset, (_, keys))) = key_sets
            .iter()
            .enumerate()
            .find(|(_, (indices, _))| *indices == primary_key_indices)
        {
            match engine.index_manager().unique_key_locations(
                db,
                table,
                crate::storage::index::index_manager::PRIMARY_LOCATION_INDEX_NAME,
                keys,
            )? {
                Some(locations) => direct_locations.extend(locations),
                None => fallback[key_set_offset] = true,
            }
        }
    }
    for index in &table_meta.unique_indexes {
        let indices = index
            .columns
            .iter()
            .map(|column| {
                schema.index_of(column).ok_or_else(|| {
                    TaibaiError::Internal(format!("unique index column '{}' is missing", column))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let Some((key_set_offset, (_, keys))) = key_sets
            .iter()
            .enumerate()
            .find(|(_, (key_indices, _))| *key_indices == indices)
        else {
            continue;
        };
        match engine
            .index_manager()
            .unique_key_locations(db, table, &index.name, keys)?
        {
            Some(locations) => direct_locations.extend(locations),
            None => fallback[key_set_offset] = true,
        }
    }
    let fallback_key_sets = key_sets
        .iter()
        .zip(&fallback)
        .filter_map(|(key_set, fallback)| fallback.then_some(key_set))
        .collect::<Vec<_>>();
    let projected_indices = fallback_key_sets
        .iter()
        .flat_map(|(indices, _)| indices.iter().copied())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let key_envelopes = build_replace_key_envelopes_from_refs(&fallback_key_sets);
    let projected_positions = fallback_key_sets
        .iter()
        .map(|(indices, keys)| {
            let positions = indices
                .iter()
                .map(|index| {
                    projected_indices
                        .iter()
                        .position(|projected| projected == index)
                        .ok_or_else(|| {
                            TaibaiError::Internal("replace projection is inconsistent".to_string())
                        })
                })
                .collect::<Result<Vec<_>>>()?;
            Ok((positions, keys))
        })
        .collect::<Result<Vec<_>>>()?;

    let mut direct_rows = HashMap::<PathBuf, HashSet<u64>>::new();
    for location in direct_locations {
        direct_rows
            .entry(PathBuf::from(location.segment_path))
            .or_default()
            .insert(location.row_id);
    }
    let mgr = engine.segment_mgr();
    let segment_paths = if fallback_key_sets.is_empty() {
        direct_rows.keys().map(PathBuf::from).collect::<Vec<_>>()
    } else {
        SegmentManager::discover_segments(&table_dir)?
    };
    // Direct index locations are trusted only after verifying the physical row
    // still carries a conflicting key: a stale or misaligned location must
    // never delete an unrelated live row.
    let verify_indices = key_sets
        .iter()
        .flat_map(|(indices, _)| indices.iter().copied())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let verify_positions = key_sets
        .iter()
        .map(|(indices, keys)| {
            let positions = indices
                .iter()
                .map(|index| {
                    verify_indices
                        .iter()
                        .position(|projected| projected == index)
                        .ok_or_else(|| {
                            TaibaiError::Internal(
                                "replace verification projection is inconsistent".to_string(),
                            )
                        })
                })
                .collect::<Result<Vec<_>>>()?;
            Ok((positions, keys))
        })
        .collect::<Result<Vec<(Vec<usize>, &HashSet<Vec<ScalarValue>>)>>>()?;
    for seg_path in segment_paths {
        let reader = mgr.open_reader_cached(&seg_path)?;
        let total_rows = reader.row_count();
        if total_rows == 0 || reader.deleted_row_count() >= total_rows {
            continue;
        }
        let old_bitmap = reader.delete_bitmap().to_vec();
        let old_deleted = reader.deleted_row_count();
        let mut new_bitmap = old_bitmap.clone();
        new_bitmap.resize(total_rows.div_ceil(8) as usize, 0);
        let mut newly_deleted = 0u64;
        let segment_name = seg_path.to_string_lossy().to_string();
        let direct_rows_in_segment = direct_rows.get(&seg_path);
        // HashSet: the fallback scan below probes this set for EVERY row of
        // EVERY row group, and the old Vec::contains made that O(rows ×
        // direct candidates) — 10^10 comparisons at 1M rows × 10k candidates,
        // under the table mutation lock.
        let direct_rows_in_segment = direct_rows_in_segment.map(|rows| {
            rows.iter()
                .copied()
                .filter(|row_id| *row_id < total_rows)
                .collect::<std::collections::HashSet<u64>>()
        });
        let direct_row_sets = direct_rows_in_segment
            .as_ref()
            .map(|rows| (rows, &verify_positions));
        for rg_idx in 0..reader.num_row_groups() {
            let rg_start = reader.row_group_start_row(rg_idx);
            let rg_rows = reader.row_group_rows(rg_idx);
            if fallback_key_sets.is_empty() {
                let Some((candidate_rows, positions)) = direct_row_sets.as_ref() else {
                    break;
                };
                let candidates = candidate_rows
                    .iter()
                    .copied()
                    .filter(|row_id| *row_id >= rg_start && *row_id < rg_start + rg_rows)
                    .collect::<Vec<_>>();
                if candidates.is_empty() {
                    continue;
                }
                let batch =
                    reader.read_row_group_columns(rg_idx, &schema, &verify_indices)?;
                for row_id in candidates {
                    let row_idx = (row_id - rg_start) as usize;
                    if !replace_row_conflicts(&batch, row_idx, positions) {
                        tracing::warn!(
                            db,
                            table,
                            segment = %segment_name,
                            row_id,
                            "skipping stale unique-index location during REPLACE conflict delete"
                        );
                        continue;
                    }
                    if delete_bitmap::mark_deleted(&mut new_bitmap, row_id) {
                        newly_deleted += 1;
                        undo.deleted_locations.push(UniqueRowLocation {
                            segment_path: segment_name.clone(),
                            row_id,
                        });
                    }
                }
                continue;
            }
            if !replace_keys_may_match_row_group_from_refs(
                reader.zone_maps(rg_idx),
                &fallback_key_sets,
                &key_envelopes,
            ) && direct_row_sets.is_none()
            {
                continue;
            }
            let batch =
                reader.read_row_group_columns(rg_idx, &schema, &projected_indices)?;
            for row_idx in 0..batch.num_rows() {
                let absolute_row = rg_start + row_idx as u64;
                if delete_bitmap::is_deleted(&new_bitmap, absolute_row) {
                    continue;
                }
                let conflicts = replace_row_conflicts(&batch, row_idx, &projected_positions);
                if !conflicts {
                    if let Some((candidate_rows, positions)) = direct_row_sets.as_ref() {
                        if candidate_rows.contains(&absolute_row)
                            && replace_row_conflicts(&batch, row_idx, positions)
                            && delete_bitmap::mark_deleted(&mut new_bitmap, absolute_row)
                        {
                            newly_deleted += 1;
                            undo.deleted_locations.push(UniqueRowLocation {
                                segment_path: segment_name.clone(),
                                row_id: absolute_row,
                            });
                        }
                    }
                    continue;
                }
                if delete_bitmap::mark_deleted(&mut new_bitmap, absolute_row) {
                    newly_deleted += 1;
                    undo.deleted_locations.push(UniqueRowLocation {
                        segment_path: segment_name.clone(),
                        row_id: absolute_row,
                    });
                }
            }
        }

        if newly_deleted == 0 {
            continue;
        }
        undo.segment_footers.push((
            seg_path.clone(),
            old_bitmap,
            old_deleted,
            new_bitmap.clone(),
            old_deleted + newly_deleted,
        ));
        // Deferred lifecycle: the journal also records the pre-rewrite footer
        // (undo) and the REPLACE staging's durability watermark. The restore
        // paths below rewrite with the IMMEDIATE lifecycle, which overwrites
        // this segment's journal and removes it on landing, so an in-process
        // rollback leaves no journal behind either.
        if let Err(error) = mgr.rewrite_delete_footer_with_mode(
            &seg_path,
            &new_bitmap,
            old_deleted + newly_deleted,
            crate::storage::segment::reader::FooterRewriteMode::Deferred(image_checks.to_vec()),
        ) {
            let _ = restore_replace_delete_under_table_lock(engine, &undo);
            return Err(error);
        }
        undo.deleted_rows += newly_deleted;
    }
    Ok(undo)
}

type ReplaceKeyEnvelope = (Vec<usize>, Vec<(ScalarValue, ScalarValue)>);

/// Whether the row at `row_idx` carries a key conflicting with any REPLACE
/// key set. Each item pairs per-set key positions inside the projected batch
/// with that set's keys. Shared by the full-scan fallback and the
/// direct-location verification so both apply identical conflict rules.
fn replace_row_conflicts(
    batch: &RecordBatch,
    row_idx: usize,
    paired_positions: &[(Vec<usize>, &HashSet<Vec<ScalarValue>>)],
) -> bool {
    paired_positions.iter().any(|(positions, keys)| {
        if positions.len() == 1 {
            let Some(value) = batch.scalar(row_idx, positions[0]) else {
                return false;
            };
            return !value.is_null() && keys.contains(std::slice::from_ref(&value));
        }
        let mut key = Vec::with_capacity(positions.len());
        for &col_idx in positions {
            let Some(value) = batch.scalar(row_idx, col_idx) else {
                return false;
            };
            if value.is_null() {
                return false;
            }
            key.push(value);
        }
        keys.contains(&key)
    })
}

fn build_replace_key_envelopes_from_refs(
    key_sets: &[&(Vec<usize>, HashSet<Vec<ScalarValue>>)],
) -> Vec<ReplaceKeyEnvelope> {
    key_sets
        .iter()
        .filter_map(|(indices, keys)| {
            let first = keys.iter().next()?;
            let mut ranges = first
                .iter()
                .map(|value| (value.clone(), value.clone()))
                .collect::<Vec<_>>();
            for key in keys.iter().skip(1) {
                for (value, (min, max)) in key.iter().zip(&mut ranges) {
                    if compare_scalars_for_sort(value, min).is_lt() {
                        *min = value.clone();
                    }
                    if compare_scalars_for_sort(value, max).is_gt() {
                        *max = value.clone();
                    }
                }
            }
            Some((indices.clone(), ranges))
        })
        .collect()
}

fn replace_keys_may_match_row_group_from_refs(
    zone_maps: &[ZoneMap],
    key_sets: &[&(Vec<usize>, HashSet<Vec<ScalarValue>>)],
    envelopes: &[ReplaceKeyEnvelope],
) -> bool {
    key_sets
        .iter()
        .zip(envelopes)
        .any(|((indices, keys), (_, ranges))| {
            if keys.len() <= 64 {
                return keys.iter().any(|key| {
                    indices.iter().zip(key).all(|(column_index, value)| {
                        zone_maps
                            .get(*column_index)
                            .map(|zone_map| zone_map.contains(value))
                            .unwrap_or(true)
                    })
                });
            }
            indices
                .iter()
                .zip(ranges)
                .all(|(column_index, (min, max))| {
                    zone_maps
                        .get(*column_index)
                        .map(|zone_map| zone_map.overlaps(min, max))
                        .unwrap_or(true)
                })
        })
}

pub(crate) fn restore_replace_delete_under_table_lock(
    engine: &Engine,
    undo: &ReplaceDeleteUndo,
) -> Result<()> {
    let mut first_error = restore_replace_memtable_undo(engine, undo).err();
    for (path, bitmap, deleted_count, ..) in undo.segment_footers.iter().rev() {
        // The restore rewrites with the IMMEDIATE lifecycle, which overwrites
        // any deferred journal at the same path and removes it on landing;
        // the explicit clear below makes that guarantee independent of the
        // rewrite's own journal handling.
        if let Err(error) = engine
            .segment_mgr()
            .rewrite_delete_footer(path, bitmap, *deleted_count)
        {
            if first_error.is_none() {
                first_error = Some(error);
            }
            continue;
        }
        crate::storage::segment::reader::clear_deferred_delete_journal(path);
    }
    match first_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

pub(crate) fn finalize_replace_delete(
    engine: &Engine,
    db: &str,
    table: &str,
    table_meta: &TableMeta,
    undo: &ReplaceDeleteUndo,
) -> Result<()> {
    if undo.deleted_rows == 0 {
        return Ok(());
    }
    engine
        .index_manager()
        .invalidate_primary_key_cache(db, table);
    if table_meta.columns.iter().any(|column| column.primary_key)
        && !engine.index_manager().append_unique_index_tombstones(
            db,
            table,
            crate::storage::index::index_manager::PRIMARY_LOCATION_INDEX_NAME,
            undo.deleted_locations.clone(),
        )?
    {
        engine.index_manager().invalidate_unique_index_snapshot(
            db,
            table,
            crate::storage::index::index_manager::PRIMARY_LOCATION_INDEX_NAME,
        );
    }
    for index in &table_meta.unique_indexes {
        if !engine.index_manager().append_unique_index_tombstones(
            db,
            table,
            &index.name,
            undo.deleted_locations.clone(),
        )? {
            engine
                .index_manager()
                .invalidate_unique_index_snapshot(db, table, &index.name);
        }
    }

    let mut catalogs = engine.catalogs().write().unwrap();
    if let Some(catalog) = catalogs.get_mut(db) {
        if let Some(meta) = catalog.get_table_mut(table) {
            meta.row_count = (meta.row_count - undo.deleted_rows as i64).max(0);
            let meta_dir = engine.data_dir().join("meta");
            catalog.save(&meta_dir)?;
        }
    }
    Ok(())
}

/// Upper bound on per-statement precise index maintenance. Deletes larger
/// than this abandon incremental bookkeeping and fall back to one wholesale
/// snapshot invalidation, where a single amortized rebuild is cheaper than
/// recording every deleted row.
const MAX_PRECISELY_MAINTAINED_DELETE_ROWS: usize = 65_536;

/// Incremental unique-index bookkeeping for DELETE: per-index key sets and
/// segment row locations of every deleted row, applied as location
/// tombstones plus key removals after the physical delete, instead of
/// invalidating the snapshots and forcing a full-table rebuild.
pub(crate) struct DeleteIndexMaintenance {
    indexes: Vec<(String, Vec<usize>)>,
    /// Key-column positions inside a full-schema batch, per index.
    schema_key_positions: Vec<Vec<usize>>,
    keys: Vec<HashSet<Vec<ScalarValue>>>,
    /// Interned segment paths: one String per segment touched by the
    /// statement, referenced by id from `locations`. Recording one String
    /// clone per deleted row allocated the same path up to 65k times per
    /// statement.
    segment_paths: Vec<String>,
    /// (segment id into `segment_paths`, absolute row id) of every recorded
    /// row.
    locations: Vec<(u32, u64)>,
    maintained: bool,
    rows_seen: usize,
}

impl DeleteIndexMaintenance {
    pub(crate) fn plan(table_meta: &TableMeta, schema: &Schema) -> Self {
        let mut indexes = Vec::new();
        for index in &table_meta.unique_indexes {
            let indices = index
                .columns
                .iter()
                .map(|column| schema.index_of(column))
                .collect::<Option<Vec<_>>>();
            if let Some(indices) = indices {
                indexes.push((index.name.clone(), indices));
            }
        }
        let primary_indices = schema
            .columns
            .iter()
            .enumerate()
            .filter(|(_, column)| column.primary_key)
            .map(|(idx, _)| idx)
            .collect::<Vec<_>>();
        if !primary_indices.is_empty() {
            indexes.push((
                crate::storage::index::index_manager::PRIMARY_LOCATION_INDEX_NAME.to_string(),
                primary_indices,
            ));
        }
        let schema_key_positions = indexes.iter().map(|(_, indices)| indices.clone()).collect();
        let keys = vec![HashSet::new(); indexes.len()];
        Self {
            indexes,
            schema_key_positions,
            keys,
            segment_paths: Vec::new(),
            locations: Vec::new(),
            maintained: true,
            rows_seen: 0,
        }
    }

    /// Return the id of `path` in the statement's segment table, interning it
    /// on first sight.
    pub(crate) fn intern_segment(&mut self, path: &str) -> u32 {
        if let Some(pos) = self.segment_paths.iter().position(|existing| existing == path) {
            return pos as u32;
        }
        self.segment_paths.push(path.to_string());
        (self.segment_paths.len() - 1) as u32
    }

    fn has_indexes(&self) -> bool {
        !self.indexes.is_empty()
    }

    pub(crate) fn still_maintaining(&self) -> bool {
        self.maintained && self.has_indexes()
    }

    /// Schema-space key column positions per maintained index, for callers
    /// (the UPDATE reinsert path) recording rows from catalog-ordered
    /// batches outside this module.
    pub(crate) fn schema_key_positions(&self) -> &[Vec<usize>] {
        &self.schema_key_positions
    }

    fn admit_row(&mut self) -> bool {
        if !self.has_indexes() || !self.maintained {
            return false;
        }
        if self.rows_seen >= MAX_PRECISELY_MAINTAINED_DELETE_ROWS {
            self.maintained = false;
            for key_set in &mut self.keys {
                key_set.clear();
            }
            self.locations.clear();
            return false;
        }
        self.rows_seen += 1;
        true
    }

    /// Row-group projection covering the WHERE columns plus every index key
    /// column. Identical to the predicate-only projection when the table has
    /// no unique indexes.
    fn projection(&self, predicate_projection: &[usize]) -> Vec<usize> {
        if !self.has_indexes() {
            return predicate_projection.to_vec();
        }
        let mut projection = predicate_projection.to_vec();
        for (_, indices) in &self.indexes {
            projection.extend(indices.iter().copied());
        }
        projection.sort_unstable();
        projection.dedup();
        projection
    }

    /// For each index, the positions of its key columns inside the merged
    /// row-group projection.
    fn projected_key_positions(&self, projection: &[usize]) -> Vec<Vec<usize>> {
        self.indexes
            .iter()
            .map(|(_, indices)| {
                indices
                    .iter()
                    .map(|index| {
                        projection
                            .iter()
                            .position(|projected| projected == index)
                            .expect("projection contains every index key column")
                    })
                    .collect()
            })
            .collect()
    }

    fn record_memtable_rows(&mut self, plans: &[ReplaceMemtableConflictPlan]) {
        for plan in plans {
            for &row_idx in &plan.removed_positions {
                if !self.admit_row() {
                    return;
                }
                self.record_keys_from_positions(&plan.original, row_idx as usize);
            }
        }
    }

    fn record_keys_from_positions(&mut self, batch: &RecordBatch, row_idx: usize) {
        for (index_offset, key_positions) in self.schema_key_positions.iter().enumerate() {
            let mut key = Vec::with_capacity(key_positions.len());
            let mut valid = true;
            for &position in key_positions {
                let value = batch.scalar(row_idx, position).unwrap_or_else(|| {
                    ScalarValue::null_for_type(batch.schema.columns[position].data_type)
                });
                if value.is_null() {
                    valid = false;
                    break;
                }
                key.push(value);
            }
            if valid {
                self.keys[index_offset].insert(key);
            }
        }
    }

    pub(crate) fn record_segment_row(
        &mut self,
        segment_id: u32,
        row_id: u64,
        batch: &RecordBatch,
        row_idx: usize,
        projected_positions: &[Vec<usize>],
    ) {
        if !self.admit_row() {
            return;
        }
        self.locations.push((segment_id, row_id));
        for (index_offset, key_positions) in projected_positions.iter().enumerate() {
            let mut key = Vec::with_capacity(key_positions.len());
            let mut valid = true;
            for &position in key_positions {
                let value = batch.scalar(row_idx, position).unwrap_or_else(|| {
                    ScalarValue::null_for_type(batch.schema.columns[position].data_type)
                });
                if value.is_null() {
                    valid = false;
                    break;
                }
                key.push(value);
            }
            if valid {
                self.keys[index_offset].insert(key);
            }
        }
    }

    /// Publishes the recorded maintenance. Any failure means the caller must
    /// fall back to wholesale snapshot invalidation.
    pub(crate) fn apply(&self, engine: &Engine, db: &str, table: &str) -> bool {
        if !self.has_indexes() {
            return true;
        }
        if !self.maintained {
            return false;
        }
        let locations: Vec<UniqueRowLocation> = self
            .locations
            .iter()
            .map(|(segment_id, row_id)| UniqueRowLocation {
                segment_path: self.segment_paths[*segment_id as usize].clone(),
                row_id: *row_id,
            })
            .collect();
        for (index_offset, (index_name, _)) in self.indexes.iter().enumerate() {
            let snapshot_missing = || {
                matches!(
                    engine
                        .index_manager()
                        .unique_index_columns(db, table, index_name),
                    Ok(None)
                )
            };
            match engine.index_manager().append_unique_index_tombstones(
                db,
                table,
                index_name,
                locations.clone(),
            ) {
                Ok(true) => {}
                Ok(false) if snapshot_missing() => {}
                Ok(false) => return false,
                Err(error) => {
                    tracing::warn!(
                        db,
                        table,
                        index = %index_name,
                        error = %error,
                        "DELETE failed to append index tombstones; falling back to invalidation"
                    );
                    return false;
                }
            }
            let keys = self.keys[index_offset].iter().cloned().collect::<Vec<_>>();
            match engine
                .index_manager()
                .remove_unique_index_keys(db, table, index_name, &keys)
            {
                Ok(true) => {}
                Ok(false) if snapshot_missing() => {}
                Ok(false) => return false,
                Err(error) => {
                    tracing::warn!(
                        db,
                        table,
                        index = %index_name,
                        error = %error,
                        "DELETE failed to remove index keys; falling back to invalidation"
                    );
                    return false;
                }
            }
        }
        true
    }
}

pub fn exec_delete(
    engine: &Engine,
    db: &str,
    table: &str,
    where_clause: &Option<Expr>,
    reader_txn_id: u64,
) -> Result<ExecResult> {
    if reader_txn_id > 0 {
        return super::txn_dml::exec_delete_in_txn(engine, db, table, where_clause, reader_txn_id);
    }

    let ctx = super::txn_dml::DmlStatementContext::plan(engine, db, table, where_clause)?;
    let resolved_where_clause = &ctx.resolved_where;
    let table_schema = &ctx.table_schema;
    let table_meta = &ctx.table_meta;
    let target_partition_key = ctx.narrowed_partition();
    let predicate = ctx.predicate.as_ref();
    let predicate_projection = &ctx.predicate_projection;

    super::txn_dml::refuse_auto_commit_dml_while_pinned(engine, db, table)?;

    // Join the INSERT/REPLACE writer protocol: the probe→publish window on
    // the INSERT side is guarded by this striped lock, so row-rewriting
    // statements must hold it too (index maintenance, footer rewrites, and
    // snapshot invalidation below are invisible to a concurrent INSERT probe
    // otherwise). Acquired before any table/partition mutation lock to keep
    // the global order constraint_insert_lock → mutation lock identical to
    // the INSERT path; REPLACE holds the same lock across its own body and
    // never re-enters exec_delete, so the mutex stays acquirable.
    let _constraint_guard = super::dml_insert::constraint_insert_lock(db, table)
        .lock()
        .unwrap();

    let table_dir = engine.data_dir().join(db).join(table);
    let mgr = engine.segment_mgr();
    let mut total_deleted: u64 = 0;
    // Segments whose delete footer was successfully rewritten by this
    // statement (0 for memtable-only deletes).
    let mut footers_rewritten: u64 = 0;

    engine
        .batch_writer()
        .checkpoint_before_rewrite(db, table, 1)?;

    // From here the statement rewrites memtables and segment tails; the work
    // is no longer safely abortable, so cancellation checks stop firing.
    crate::common::query_cancel::enter_committing_phase();

    let mut index_maintenance = DeleteIndexMaintenance::plan(&table_meta, &table_schema);

    let mut delete_body = || -> Result<bool> {
        // In-lock re-check: the fn-top gate races a transaction registering
        // its pin under this lock; deleting rows under a fresh pin would
        // corrupt that transaction's COMMIT.
        super::txn_dml::refuse_auto_commit_dml_while_pinned(engine, db, table)?;
        // Deleted-row batches for the change feed (segments + memtables).
        let mut deleted_events = super::helpers::ChangeEventBuffer::new("delete", db, table);
        // Memtable-resident rows are removed in place (no forced flush),
        // mirroring the REPLACE conflict path; durable tails are rewritten
        // so crash recovery cannot resurrect deleted rows.
        let memtable_plans = plan_delete_memtable_rows(
            engine,
            db,
            table,
            &resolved_where_clause,
            &table_schema,
            target_partition_key
                .as_deref()
                .filter(|key| !key.is_empty()),
            reader_txn_id,
        )?;
        if !memtable_plans.is_empty() {
            index_maintenance.record_memtable_rows(&memtable_plans);
            // Collect memtable-resident deleted rows for the change feed
            // before the plans are consumed.
            if engine.pubsub().has_subscribers() {
                for plan in &memtable_plans {
                    deleted_events.push(&plan.original, &plan.removed_positions);
                }
            }
            let undo = apply_replace_memtable_conflicts(engine, db, table, memtable_plans)?;
            total_deleted += undo.deleted_rows();
        }

        let mut segment_files = if table_dir.exists() {
            prune_segments_by_partition(
                SegmentManager::discover_segments(&table_dir)?,
                &resolved_where_clause,
                &table_schema,
                &table_meta,
            )
        } else {
            Vec::new()
        };
        if let Some(ref pred) = predicate {
            segment_files = prune_segments_by_cluster_key(
                segment_files,
                mgr,
                &table_schema,
                &table_meta,
                pred,
            )?;
        }
        // When change subscribers exist the deleted rows themselves must
        // be published, so row groups are read with every column instead of
        // the key-only projection; index key positions then map to the full
        // schema directly.
        let publish_deletes = engine.pubsub().has_subscribers();
        let key_projection = if publish_deletes {
            Vec::new()
        } else {
            index_maintenance.projection(&predicate_projection)
        };
        let projected_key_positions =
            index_maintenance.projected_key_positions(&if key_projection.is_empty() {
                (0..table_schema.columns.len()).collect::<Vec<_>>()
            } else {
                key_projection.clone()
            });
        // Key positions of the FULL catalog-order batch: the fallback read
        // for a segment whose footer predates a projected (WHERE/key) column.
        let full_schema_key_positions = index_maintenance
            .projected_key_positions(&(0..table_schema.columns.len()).collect::<Vec<_>>());
        let mut first_rewrite_error: Option<crate::common::error::TaibaiError> = None;
        for seg_path in segment_files {
            let (new_bitmap, new_deleted_count, prev_deleted) = {
                let reader = mgr.open_reader_cached(&seg_path)?;
                let total_rows = reader.row_count();
                if total_rows == 0 {
                    continue;
                }
                if let Some(ref pred) = predicate {
                    if !super::helpers::segment_may_match(&reader, table_schema, Some(pred)) {
                        continue;
                    }
                }
                // A footer predating a projected column cannot serve the
                // narrow read: degrade this segment to the full aligned read
                // (defaults filled) and map index keys to their catalog
                // positions.
                let segment_drifts = !key_projection.is_empty()
                    && crate::storage::segment::reader::any_column_absent_in_schema(
                        table_schema,
                        reader.schema(),
                        &key_projection,
                    );
                let key_positions = if segment_drifts {
                    &full_schema_key_positions
                } else {
                    &projected_key_positions
                };
                let segment_id = index_maintenance
                    .still_maintaining()
                    .then(|| index_maintenance.intern_segment(&seg_path.to_string_lossy()));
                let prev_deleted = reader.deleted_row_count();
                let maintaining = index_maintenance.still_maintaining();
                let outcome = match_segment_for_delete(
                    &reader,
                    table_schema,
                    predicate,
                    resolved_where_clause.as_ref(),
                    &[],
                    publish_deletes || maintaining,
                    |reader, rg_idx| {
                        // Name-based read: the batch carries catalog defs in
                        // request order, so key positions and WHERE evaluation
                        // stay correct on pre-DDL footers.
                        if key_projection.is_empty() || segment_drifts {
                            reader.read_row_group_columns_aligned(rg_idx, table_schema)
                        } else {
                            reader.read_row_group_columns(rg_idx, table_schema, &key_projection)
                        }
                    },
                    |batch, matched| {
                        if publish_deletes {
                            let deleted_rows: Vec<u32> =
                                matched.iter().map(|(idx, _)| *idx).collect();
                            deleted_events.push(batch, &deleted_rows);
                        }
                        if let Some(segment_id) = segment_id {
                            for (idx, absolute) in matched {
                                index_maintenance.record_segment_row(
                                    segment_id,
                                    *absolute,
                                    batch,
                                    *idx as usize,
                                    key_positions,
                                );
                            }
                        }
                        Ok(())
                    },
                )?;
                let new_deleted_count = prev_deleted + outcome.matched_ids.len() as u64;
                total_deleted += outcome.matched_ids.len() as u64;
                (outcome.new_bitmap, new_deleted_count, prev_deleted)
            };

            if new_deleted_count == prev_deleted {
                continue;
            }

            // A failed footer rewrite must not abort the remaining segments:
            // rows already tombstoned elsewhere stay deleted either way, and
            // continuing maximizes how much of the statement is applied. The
            // error is reported after the loop.
            if let Err(error) = mgr.rewrite_delete_footer(&seg_path, &new_bitmap, new_deleted_count)
            {
                tracing::warn!(
                    file = %seg_path.display(),
                    error = %error,
                    "DELETE footer rewrite failed; continuing with remaining segments"
                );
                // This segment's rows were not tombstoned after all.
                total_deleted -= new_deleted_count - prev_deleted;
                first_rewrite_error = first_rewrite_error.or(Some(error));
            } else {
                footers_rewritten += 1;
            }
        }

        if let Some(error) = first_rewrite_error {
            // Partial application: stale index snapshots could still point at
            // rows deleted in the successfully rewritten segments.
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
            if total_deleted > 0 {
                // The successfully rewritten segments and the memtable removal
                // are durable even though the statement reports an error; the
                // catalog must reflect them or row_count drifts upward forever
                // (the success-path decrement below is skipped on Err).
                engine.invalidate_hot_store(db, table);
                let mut catalogs = engine.catalogs().write().unwrap();
                if let Some(catalog) = catalogs.get_mut(db) {
                    if let Some(meta) = catalog.get_table_mut(table) {
                        meta.row_count = (meta.row_count - total_deleted as i64).max(0);
                        let meta_dir = engine.data_dir().join("meta");
                        let _ = catalog.save(&meta_dir);
                    }
                }
            }
            return Err(error);
        }

        if total_deleted > 0 {
            engine.invalidate_hot_store(db, table);
        }

        // Publish the deleted rows as change events only after every footer
        // rewrite succeeded (errors return above), so subscribers never see
        // deletions that did not physically happen. Each batch is a separate
        // event: batches from different sources may carry different schemas
        // (ALTER TABLE ADD COLUMN is metadata-only), and merging them by
        // column index would panic or mix columns.
        if publish_deletes && total_deleted > 0 {
            for batch in deleted_events.batches {
                engine
                    .pubsub()
                    .publish_change(crate::pubsub::ChangeKind::Delete, db, table, batch);
            }
        }

        // C22: the catalog row_count decrement + save runs INSIDE the table
        // mutation lock (same order as the partial-error path above: table
        // lock → catalog write lock), after every segment/memtable operation
        // of the statement. Doing it after releasing the lock raced DDL's
        // catalog save — a lost update let row_count drift upward forever.
        if total_deleted > 0 {
            let mut catalogs = engine.catalogs().write().unwrap();
            if let Some(catalog) = catalogs.get_mut(db) {
                if let Some(meta) = catalog.get_table_mut(table) {
                    meta.row_count = (meta.row_count - total_deleted as i64).max(0);
                    let meta_dir = engine.data_dir().join("meta");
                    // Best-effort persistence (same contract as the partial
                    // error path above): failing the statement here would
                    // skip index maintenance and the invalidation fallback
                    // entirely, leaving unique-index snapshots pointing at
                    // deleted rows. A missed save only falls behind on disk
                    // until the next successful one.
                    if let Err(error) = catalog.save(&meta_dir) {
                        tracing::warn!(
                            db,
                            table,
                            error = %error,
                            "catalog row_count save after DELETE deferred (best-effort)"
                        );
                    }
                }
            }
        }

        Ok(index_maintenance.apply(engine, db, table))
    };

    let indexes_maintained = match target_partition_key
        .as_deref()
        .filter(|key| !key.is_empty())
    {
        Some(partition) => {
            mgr.with_partition_set_mutation_lock(db, table, &[partition], &mut delete_body)?
        }
        None => mgr.with_table_mutation_lock(db, table, &mut delete_body)?,
    };

    if total_deleted > 0 {
        // Footer rewrites bump the content epoch inside the
        // `rewrite_delete_footer` primitive itself, so the transactional
        // COMMIT path (which rewrites the same footers) gets identical
        // freshness without remembering to call anything.
        if !indexes_maintained {
            tracing::warn!(
                db,
                table,
                deleted_rows = total_deleted,
                "DELETE fell back to unique-index snapshot invalidation"
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
        } else {
            engine
                .index_manager()
                .invalidate_primary_key_cache(db, table);
        }
        // The row_count decrement + catalog save itself moved INTO the
        // mutation-lock window (C22, above in delete_body); only the index
        // cache work remains here.
    }

    Ok(ExecResult::Ok {
        rows_affected: total_deleted,
    })
}

pub(crate) fn prune_segments_by_partition(
    segment_files: Vec<PathBuf>,
    where_clause: &Option<Expr>,
    schema: &Schema,
    table_meta: &TableMeta,
) -> Vec<PathBuf> {
    if !table_meta.is_partitioned() {
        return segment_files;
    }
    let Some(target_key) = delete_partition_eq_key(where_clause, schema, table_meta) else {
        return segment_files;
    };
    if target_key.is_empty() {
        return segment_files;
    }

    segment_files
        .into_iter()
        .filter(|path| {
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                return true;
            };
            match parse_partition_key(name) {
                Some(key) => key == target_key,
                None => true,
            }
        })
        .collect()
}

pub(crate) fn prune_segments_by_cluster_key(
    segment_files: Vec<PathBuf>,
    mgr: &SegmentManager,
    schema: &Schema,
    table_meta: &TableMeta,
    predicate: &crate::storage::segment::RowGroupPredicate,
) -> Result<Vec<PathBuf>> {
    if segment_files.len() <= 1 {
        return Ok(segment_files);
    }
    let Some(bounds) = cluster_key_bounds(schema, table_meta, predicate) else {
        return Ok(segment_files);
    };
    let Some((start, end)) =
        cluster_prune::cluster_segment_range(&segment_files, mgr, schema, &bounds)
    else {
        return Ok(segment_files);
    };
    if start > end {
        return Ok(Vec::new());
    }
    Ok(segment_files[start..=end].to_vec())
}

fn cluster_key_bounds(
    schema: &Schema,
    table_meta: &TableMeta,
    predicate: &crate::storage::segment::RowGroupPredicate,
) -> Option<ClusterKeyBounds> {
    let key_col = table_meta.cluster_key.first()?;
    let key_col_idx = schema.index_of(key_col)?;

    let mut lower: Option<(ScalarValue, bool)> = None;
    let mut upper: Option<(ScalarValue, bool)> = None;
    for clause in &predicate.clauses {
        if clause.column_index != key_col_idx {
            continue;
        }
        match clause.op {
            RangeOp::Eq => {
                return Some(ClusterKeyBounds::single(
                    key_col_idx,
                    Some((clause.value.clone(), true)),
                    Some((clause.value.clone(), true)),
                ));
            }
            RangeOp::Gt => set_tighter_lower(&mut lower, clause.value.clone(), false),
            RangeOp::Ge => set_tighter_lower(&mut lower, clause.value.clone(), true),
            RangeOp::Lt => set_tighter_upper(&mut upper, clause.value.clone(), false),
            RangeOp::Le => set_tighter_upper(&mut upper, clause.value.clone(), true),
        }
    }

    if lower.is_some() || upper.is_some() {
        Some(ClusterKeyBounds::single(key_col_idx, lower, upper))
    } else {
        None
    }
}

fn set_tighter_lower(
    current: &mut Option<(ScalarValue, bool)>,
    candidate: ScalarValue,
    inclusive: bool,
) {
    let replace = match current {
        Some((value, current_inclusive)) => match compare_scalars_for_sort(&candidate, value) {
            std::cmp::Ordering::Greater => true,
            std::cmp::Ordering::Equal => !inclusive && *current_inclusive,
            std::cmp::Ordering::Less => false,
        },
        None => true,
    };
    if replace {
        *current = Some((candidate, inclusive));
    }
}

fn set_tighter_upper(
    current: &mut Option<(ScalarValue, bool)>,
    candidate: ScalarValue,
    inclusive: bool,
) {
    let replace = match current {
        Some((value, current_inclusive)) => match compare_scalars_for_sort(&candidate, value) {
            std::cmp::Ordering::Less => true,
            std::cmp::Ordering::Equal => !inclusive && *current_inclusive,
            std::cmp::Ordering::Greater => false,
        },
        None => true,
    };
    if replace {
        *current = Some((candidate, inclusive));
    }
}

pub(crate) fn delete_partition_eq_key(
    where_clause: &Option<Expr>,
    schema: &Schema,
    table_meta: &TableMeta,
) -> Option<String> {
    let expr = where_clause.as_ref()?;
    let pk_idx = table_meta.get_partition_column_index()?;
    let pk_col = &schema.columns.get(pk_idx)?.name;
    let literal_expr = find_eq_literal_for_column(expr, pk_col)?;
    let scalar = eval_expr_to_scalar(literal_expr, &schema.columns[pk_idx].data_type).ok()?;

    match table_meta.partition_type {
        PartitionType::Day
        | PartitionType::Month
        | PartitionType::Year
        | PartitionType::Hash
        | PartitionType::Mod => Some(compute_partition_key_from_scalar(
            &table_meta.partition_strategy(),
            &scalar,
        )),
        PartitionType::Value => scalar.to_text(),
        PartitionType::None => None,
    }
}

fn find_eq_literal_for_column<'a>(expr: &'a Expr, column: &str) -> Option<&'a Expr> {
    match expr {
        Expr::BinaryOp { left, op, right } if *op == BinOp::And => {
            find_eq_literal_for_column(left, column)
                .or_else(|| find_eq_literal_for_column(right, column))
        }
        Expr::BinaryOp { left, op, right } if *op == BinOp::Eq => {
            if expr_matches_column(left, column) && matches!(right.as_ref(), Expr::Literal(_)) {
                return Some(right);
            }
            if expr_matches_column(right, column) && matches!(left.as_ref(), Expr::Literal(_)) {
                return Some(left);
            }
            None
        }
        _ => None,
    }
}

fn expr_matches_column(expr: &Expr, column: &str) -> bool {
    match expr {
        Expr::Column(name) => name.eq_ignore_ascii_case(column),
        Expr::QualifiedColumn { column: name, .. } => name.eq_ignore_ascii_case(column),
        _ => false,
    }
}

#[cfg(test)]
#[path = "dml_delete_test.rs"]
mod tests;

#[cfg(test)]
#[path = "replace_pruning_test.rs"]
mod replace_pruning_tests;
