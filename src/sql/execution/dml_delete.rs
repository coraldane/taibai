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
use crate::storage::segment::{RangeOp, SegmentReader};
use crate::storage::segment_manager::SegmentManager;
use crate::types::batch::{RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

use super::engine::{Engine, ExecResult};
use super::helpers::{
    build_row_group_predicate, collect_expr_column_indices, eval_expr_to_scalar,
    eval_where_clause_batch,
};
use crate::sql::parser::ast::{BinOp, Expr};

#[derive(Default)]
pub(crate) struct ReplaceDeleteUndo {
    segment_footers: Vec<(PathBuf, Vec<u8>, u64)>,
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
    memtable: Arc<MemTable>,
    original: Arc<RecordBatch>,
    filtered: Arc<RecordBatch>,
    removed_positions: Vec<u32>,
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
/// The caller holds the table mutation lock.
fn plan_delete_memtable_rows(
    engine: &Engine,
    db: &str,
    table: &str,
    where_clause: &Option<Expr>,
    table_schema: &Schema,
    target_partition_key: Option<&str>,
) -> Result<Vec<ReplaceMemtableConflictPlan>> {
    let mut plans = Vec::new();
    for memtable in engine.memtable_manager().get_by_table(db, table) {
        if memtable.is_transactional() || memtable.visibility() != TxnVisibility::Committed {
            continue;
        }
        if let Some(key) = target_partition_key {
            if memtable.partition_key() != key {
                continue;
            }
        }
        let planned = memtable.read_snapshot_arc().and_then(|batch| {
            if batch.num_rows() == 0 {
                return None;
            }
            let mask = match where_clause.as_ref() {
                Some(expr) => eval_where_clause_batch(expr, &batch, table_schema).ok()?,
                None => vec![true; batch.num_rows()],
            };
            let removed = mask.iter().filter(|matches| **matches).count();
            if removed == 0 {
                return None;
            }
            let keep = mask.iter().map(|matches| !matches).collect::<Vec<_>>();
            let filtered = filter_batch_keep(&batch, &keep, batch.num_rows() - removed).ok()?;
            let removed_positions = mask
                .iter()
                .enumerate()
                .filter_map(|(row_idx, matches)| matches.then_some(row_idx as u32))
                .collect::<Vec<_>>();
            Some((batch, Arc::new(filtered), removed_positions))
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
        if let Err(error) = crate::storage::core::durable_tail::persist(
            &table_dir,
            plan.memtable.id(),
            plan.memtable.partition_key(),
            &plan.filtered,
        ) {
            let _ = plan.memtable.replace_pending_batch(plan.original.clone());
            let _ = crate::storage::core::durable_tail::persist(
                &table_dir,
                plan.memtable.id(),
                plan.memtable.partition_key(),
                &plan.original,
            );
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

pub(crate) fn delete_replace_conflicts_under_table_lock(
    engine: &Engine,
    db: &str,
    table: &str,
    table_meta: &TableMeta,
    key_sets: &[(Vec<usize>, HashSet<Vec<ScalarValue>>)],
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
        let direct_rows_in_segment = direct_rows_in_segment.as_ref().map(|rows| {
            rows.iter()
                .copied()
                .filter(|row_id| *row_id < total_rows)
                .collect::<Vec<_>>()
        });
        let direct_row_sets = direct_rows_in_segment
            .as_deref()
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
                let batch = reader.read_row_group_with_projection(rg_idx, &verify_indices)?;
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
            let batch = reader.read_row_group_with_projection(rg_idx, &projected_indices)?;
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
        undo.segment_footers
            .push((seg_path.clone(), old_bitmap, old_deleted));
        if let Err(error) =
            mgr.rewrite_delete_footer(&seg_path, &new_bitmap, old_deleted + newly_deleted)
        {
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
    for (path, bitmap, deleted_count) in undo.segment_footers.iter().rev() {
        if let Err(error) = engine
            .segment_mgr()
            .rewrite_delete_footer(path, bitmap, *deleted_count)
        {
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
    locations: Vec<UniqueRowLocation>,
    maintained: bool,
    rows_seen: usize,
}

impl DeleteIndexMaintenance {
    fn plan(table_meta: &TableMeta, schema: &Schema) -> Self {
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
            locations: Vec::new(),
            maintained: true,
            rows_seen: 0,
        }
    }

    fn has_indexes(&self) -> bool {
        !self.indexes.is_empty()
    }

    fn still_maintaining(&self) -> bool {
        self.maintained && self.has_indexes()
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

    fn record_segment_row(
        &mut self,
        segment_path: &str,
        row_id: u64,
        batch: &RecordBatch,
        row_idx: usize,
        projected_positions: &[Vec<usize>],
    ) {
        if !self.admit_row() {
            return;
        }
        self.locations.push(UniqueRowLocation {
            segment_path: segment_path.to_string(),
            row_id,
        });
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
    fn apply(&self, engine: &Engine, db: &str, table: &str) -> bool {
        if !self.has_indexes() {
            return true;
        }
        if !self.maintained {
            return false;
        }
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
                self.locations.clone(),
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

    let resolved_where_clause = resolve_in_subqueries(engine, db, where_clause)?;

    let target_partition_key =
        delete_partition_eq_key(&resolved_where_clause, &table_schema, &table_meta);
    let table_dir = engine.data_dir().join(db).join(table);
    let mgr = engine.segment_mgr();
    let mut total_deleted: u64 = 0;
    let predicate = build_row_group_predicate(&resolved_where_clause, &table_schema);
    let predicate_projection = resolved_where_clause
        .as_ref()
        .map(|expr| collect_expr_column_indices(expr, &table_schema))
        .unwrap_or_default();

    engine
        .batch_writer()
        .checkpoint_before_rewrite(db, table, 1)?;

    let mut index_maintenance = DeleteIndexMaintenance::plan(&table_meta, &table_schema);

    let mut delete_body = || -> Result<bool> {
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
        )?;
        if !memtable_plans.is_empty() {
            index_maintenance.record_memtable_rows(&memtable_plans);
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
        let key_projection = index_maintenance.projection(&predicate_projection);
        let projected_key_positions = index_maintenance.projected_key_positions(&key_projection);
        for seg_path in segment_files {
            let (new_bitmap, new_deleted_count, prev_deleted) = {
                let reader = mgr.open_reader_cached(&seg_path)?;
                let total_rows = reader.row_count();
                if total_rows == 0 {
                    continue;
                }
                if let Some(ref pred) = predicate {
                    if !segment_may_match(&reader, pred) {
                        continue;
                    }
                }
                let segment_path_string = index_maintenance
                    .still_maintaining()
                    .then(|| seg_path.to_string_lossy().to_string());
                let prev_bitmap = reader.delete_bitmap().to_vec();
                let prev_deleted = reader.deleted_row_count();
                let bitmap_bytes = total_rows.div_ceil(8) as usize;
                let mut new_bitmap = vec![0u8; bitmap_bytes];
                if !prev_bitmap.is_empty() {
                    let copy_len = prev_bitmap.len().min(bitmap_bytes);
                    new_bitmap[..copy_len].copy_from_slice(&prev_bitmap[..copy_len]);
                }

                let mut newly_deleted: u64 = 0;
                let mut row_id: u64 = 0;
                for rg_idx in 0..reader.num_row_groups() {
                    let rg_rows = reader.row_group_rows(rg_idx);
                    if delete_bitmap::row_range_all_deleted(&new_bitmap, row_id, rg_rows) {
                        row_id += rg_rows;
                        continue;
                    }

                    if let Some(ref pred) = predicate {
                        if !pred.matches_row_group(reader.zone_maps(rg_idx)) {
                            row_id += rg_rows;
                            continue;
                        }
                    }

                    if let Some(ref where_expr) = resolved_where_clause {
                        let batch = if key_projection.is_empty() {
                            reader.read_row_group(rg_idx)?
                        } else {
                            reader.read_row_group_with_projection(rg_idx, &key_projection)?
                        };
                        let mask = eval_where_clause_batch(where_expr, &batch, &batch.schema)?;
                        for (i, matches) in mask.into_iter().enumerate() {
                            if matches
                                && delete_bitmap::mark_deleted(&mut new_bitmap, row_id + i as u64)
                            {
                                newly_deleted += 1;
                                if let Some(segment_path) = segment_path_string.as_deref() {
                                    index_maintenance.record_segment_row(
                                        segment_path,
                                        row_id + i as u64,
                                        &batch,
                                        i,
                                        &projected_key_positions,
                                    );
                                }
                            }
                        }
                        row_id += batch.num_rows() as u64;
                    } else if let Some(segment_path) = segment_path_string.as_deref() {
                        let batch = if key_projection.is_empty() {
                            reader.read_row_group(rg_idx)?
                        } else {
                            reader.read_row_group_with_projection(rg_idx, &key_projection)?
                        };
                        for i in 0..batch.num_rows() {
                            if delete_bitmap::mark_deleted(&mut new_bitmap, row_id + i as u64) {
                                newly_deleted += 1;
                                index_maintenance.record_segment_row(
                                    segment_path,
                                    row_id + i as u64,
                                    &batch,
                                    i,
                                    &projected_key_positions,
                                );
                            }
                        }
                        row_id += rg_rows;
                    } else {
                        for i in 0..rg_rows {
                            if delete_bitmap::mark_deleted(&mut new_bitmap, row_id + i) {
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

        if total_deleted > 0 {
            engine.invalidate_hot_store(db, table);
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

fn resolve_in_subqueries(
    engine: &Engine,
    db: &str,
    where_clause: &Option<Expr>,
) -> Result<Option<Expr>> {
    where_clause
        .as_ref()
        .map(|expr| resolve_expr_in_subqueries(engine, db, expr))
        .transpose()
}

fn resolve_expr_in_subqueries(engine: &Engine, db: &str, expr: &Expr) -> Result<Expr> {
    Ok(match expr {
        Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
            left: Box::new(resolve_expr_in_subqueries(engine, db, left)?),
            op: *op,
            right: Box::new(resolve_expr_in_subqueries(engine, db, right)?),
        },
        Expr::UnaryNot(inner) => {
            Expr::UnaryNot(Box::new(resolve_expr_in_subqueries(engine, db, inner)?))
        }
        Expr::UnaryMinus(inner) => {
            Expr::UnaryMinus(Box::new(resolve_expr_in_subqueries(engine, db, inner)?))
        }
        Expr::IsNull(inner) => {
            Expr::IsNull(Box::new(resolve_expr_in_subqueries(engine, db, inner)?))
        }
        Expr::IsNotNull(inner) => {
            Expr::IsNotNull(Box::new(resolve_expr_in_subqueries(engine, db, inner)?))
        }
        Expr::Exists(inner) => {
            if let Expr::Subquery(subquery) = inner.as_ref() {
                let exists = super::query::execute_exists_subquery(engine, db, subquery)?;
                Expr::Literal(ScalarValue::Bool(Some(exists)))
            } else {
                Expr::Exists(Box::new(resolve_expr_in_subqueries(engine, db, inner)?))
            }
        }
        Expr::In {
            expr: inner,
            values,
            negated,
        } => {
            let resolved_inner = resolve_expr_in_subqueries(engine, db, inner)?;
            let resolved_values = if let [Expr::Subquery(subquery)] = values.as_slice() {
                execute_in_subquery(engine, db, subquery)?
                    .into_iter()
                    .map(Expr::Literal)
                    .collect()
            } else {
                values
                    .iter()
                    .map(|value| resolve_expr_in_subqueries(engine, db, value))
                    .collect::<Result<Vec<_>>>()?
            };
            Expr::In {
                expr: Box::new(resolved_inner),
                values: resolved_values,
                negated: *negated,
            }
        }
        Expr::Between {
            expr: inner,
            low,
            high,
            negated,
        } => Expr::Between {
            expr: Box::new(resolve_expr_in_subqueries(engine, db, inner)?),
            low: Box::new(resolve_expr_in_subqueries(engine, db, low)?),
            high: Box::new(resolve_expr_in_subqueries(engine, db, high)?),
            negated: *negated,
        },
        Expr::Case {
            operand,
            when_clauses,
            else_clause,
        } => Expr::Case {
            operand: operand
                .as_ref()
                .map(|expr| resolve_expr_in_subqueries(engine, db, expr).map(Box::new))
                .transpose()?,
            when_clauses: when_clauses
                .iter()
                .map(|(when_expr, then_expr)| {
                    Ok((
                        resolve_expr_in_subqueries(engine, db, when_expr)?,
                        resolve_expr_in_subqueries(engine, db, then_expr)?,
                    ))
                })
                .collect::<Result<Vec<_>>>()?,
            else_clause: else_clause
                .as_ref()
                .map(|expr| resolve_expr_in_subqueries(engine, db, expr).map(Box::new))
                .transpose()?,
        },
        Expr::Like {
            expr: inner,
            pattern,
            negated,
        } => Expr::Like {
            expr: Box::new(resolve_expr_in_subqueries(engine, db, inner)?),
            pattern: Box::new(resolve_expr_in_subqueries(engine, db, pattern)?),
            negated: *negated,
        },
        Expr::Function {
            name,
            args,
            distinct,
        } => Expr::Function {
            name: name.clone(),
            args: args
                .iter()
                .map(|arg| resolve_expr_in_subqueries(engine, db, arg))
                .collect::<Result<Vec<_>>>()?,
            distinct: *distinct,
        },
        Expr::Subquery(subquery) => {
            let value = super::query::execute_scalar_subquery(engine, db, subquery)?;
            Expr::Literal(value)
        }
        Expr::WindowFunction {
            func,
            partition_by,
            order_by,
            frame,
        } => Expr::WindowFunction {
            func: func.clone(),
            partition_by: partition_by
                .iter()
                .map(|arg| resolve_expr_in_subqueries(engine, db, arg))
                .collect::<Result<Vec<_>>>()?,
            order_by: order_by
                .iter()
                .map(|item| {
                    Ok(crate::sql::parser::ast::OrderByItem {
                        expr: resolve_expr_in_subqueries(engine, db, &item.expr)?,
                        desc: item.desc,
                    })
                })
                .collect::<Result<Vec<_>>>()?,
            frame: frame.clone(),
        },
        Expr::Correlated(_) => expr.clone(),
        Expr::Literal(_)
        | Expr::Placeholder(_)
        | Expr::Column(_)
        | Expr::QualifiedColumn { .. } => expr.clone(),
    })
}

fn resolve_stmt_in_subqueries(
    engine: &Engine,
    db: &str,
    stmt: &crate::sql::parser::ast::Statement,
) -> Result<crate::sql::parser::ast::Statement> {
    let mut stmt = stmt.clone();
    if let crate::sql::parser::ast::Statement::Select {
        select_list,
        where_clause,
        group_by,
        having,
        order_by,
        limit,
        offset,
        ..
    } = &mut stmt
    {
        for item in select_list {
            if let crate::sql::parser::ast::SelectItem::Expr(expr, _) = item {
                *expr = resolve_expr_in_subqueries(engine, db, expr)?;
            }
        }
        if let Some(expr) = where_clause {
            *expr = resolve_expr_in_subqueries(engine, db, expr)?;
        }
        for expr in group_by {
            *expr = resolve_expr_in_subqueries(engine, db, expr)?;
        }
        if let Some(expr) = having {
            *expr = resolve_expr_in_subqueries(engine, db, expr)?;
        }
        for item in order_by {
            item.expr = resolve_expr_in_subqueries(engine, db, &item.expr)?;
        }
        if let Some(expr) = limit {
            *expr = resolve_expr_in_subqueries(engine, db, expr)?;
        }
        if let Some(expr) = offset {
            *expr = resolve_expr_in_subqueries(engine, db, expr)?;
        }
    }
    Ok(stmt)
}

fn execute_in_subquery(
    engine: &Engine,
    db: &str,
    subquery: &crate::sql::parser::ast::Statement,
) -> Result<Vec<ScalarValue>> {
    let subquery = resolve_stmt_in_subqueries(engine, db, subquery)?;
    if let Some(values) = execute_simple_derived_subquery(engine, db, &subquery)? {
        return Ok(values);
    }
    let ExecResult::Query { batches, .. } = super::query::exec_select(engine, db, &subquery)?
    else {
        return Err(TaibaiError::Sql("IN subquery must be a SELECT".into()));
    };
    Ok(first_column_values(&batches, 0))
}

fn execute_simple_derived_subquery(
    engine: &Engine,
    db: &str,
    stmt: &crate::sql::parser::ast::Statement,
) -> Result<Option<Vec<ScalarValue>>> {
    use crate::sql::parser::ast::{FromClause, SelectItem, Statement, TableRef};

    let Statement::Select {
        select_list,
        from: Some(FromClause::Table(TableRef::Subquery { query, alias })),
        where_clause: None,
        group_by,
        having: None,
        order_by: _,
        limit: None,
        offset: None,
        ..
    } = stmt
    else {
        return Ok(None);
    };

    if !group_by.is_empty() || select_list.len() != 1 {
        return Ok(None);
    }

    let inner = resolve_stmt_in_subqueries(engine, db, query)?;
    let ExecResult::Query { schema, batches } = super::query::exec_select(engine, db, &inner)?
    else {
        return Err(TaibaiError::Sql(
            "derived IN subquery must be a SELECT".into(),
        ));
    };

    let col_idx = match &select_list[0] {
        SelectItem::Star => 0,
        SelectItem::Expr(Expr::Column(name), _) => schema.index_of(name).unwrap_or(0),
        SelectItem::Expr(Expr::QualifiedColumn { table, column }, _)
            if table.eq_ignore_ascii_case(alias) =>
        {
            schema.index_of(column).unwrap_or(0)
        }
        SelectItem::Expr(_, _) => 0,
    };
    Ok(Some(first_column_values(&batches, col_idx)))
}

fn first_column_values(
    batches: &[crate::types::batch::RecordBatch],
    col_idx: usize,
) -> Vec<ScalarValue> {
    let mut values = Vec::new();
    for batch in batches {
        if batch.num_columns() <= col_idx {
            continue;
        }
        for row_idx in 0..batch.num_rows() {
            values.push(
                batch
                    .scalar(row_idx, col_idx)
                    .unwrap_or(ScalarValue::Int64(None)),
            );
        }
    }
    values
}

fn segment_may_match(
    reader: &SegmentReader,
    predicate: &crate::storage::segment::RowGroupPredicate,
) -> bool {
    let segment_zone_maps = reader.segment_zone_maps();
    if !segment_zone_maps.is_empty() {
        return predicate.matches_row_group(segment_zone_maps);
    }
    (0..reader.num_row_groups()).any(|rg_idx| predicate.matches_row_group(reader.zone_maps(rg_idx)))
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

#[derive(Debug, Clone)]
struct ClusterKeyBounds {
    column_index: usize,
    lower: Option<(ScalarValue, bool)>,
    upper: Option<(ScalarValue, bool)>,
}

fn prune_segments_by_cluster_key(
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
    let Some((start, end)) = cluster_segment_range(&segment_files, mgr, &bounds)? else {
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
                return Some(ClusterKeyBounds {
                    column_index: key_col_idx,
                    lower: Some((clause.value.clone(), true)),
                    upper: Some((clause.value.clone(), true)),
                });
            }
            RangeOp::Gt => set_tighter_lower(&mut lower, clause.value.clone(), false),
            RangeOp::Ge => set_tighter_lower(&mut lower, clause.value.clone(), true),
            RangeOp::Lt => set_tighter_upper(&mut upper, clause.value.clone(), false),
            RangeOp::Le => set_tighter_upper(&mut upper, clause.value.clone(), true),
        }
    }

    if lower.is_some() || upper.is_some() {
        Some(ClusterKeyBounds {
            column_index: key_col_idx,
            lower,
            upper,
        })
    } else {
        None
    }
}

fn cluster_segment_range(
    segments: &[PathBuf],
    mgr: &SegmentManager,
    bounds: &ClusterKeyBounds,
) -> Result<Option<(usize, usize)>> {
    if !cluster_segment_zone_maps_are_ordered(segments, mgr, bounds.column_index)? {
        return Ok(None);
    }
    let start = match &bounds.lower {
        Some((value, inclusive)) => {
            let Some(start) = first_segment_with_max_at_least(
                segments,
                mgr,
                bounds.column_index,
                value,
                *inclusive,
            )?
            else {
                return Ok(None);
            };
            start
        }
        None => 0,
    };
    let end = match &bounds.upper {
        Some((value, inclusive)) => {
            let Some(end) = last_segment_with_min_at_most(
                segments,
                mgr,
                bounds.column_index,
                value,
                *inclusive,
            )?
            else {
                return Ok(None);
            };
            if end < 0 {
                return Ok(Some((1, 0)));
            }
            end as usize
        }
        None => segments.len().saturating_sub(1),
    };
    Ok(Some((start, end)))
}

fn cluster_segment_zone_maps_are_ordered(
    segments: &[PathBuf],
    mgr: &SegmentManager,
    col_idx: usize,
) -> Result<bool> {
    let mut previous_max: Option<ScalarValue> = None;
    for segment in segments {
        let Some(zm) = segment_cluster_zone_map(mgr, segment, col_idx)? else {
            return Ok(false);
        };
        if compare_scalars_for_sort(&zm.min_value, &zm.max_value) == std::cmp::Ordering::Greater {
            return Ok(false);
        }
        if let Some(prev) = &previous_max {
            if compare_scalars_for_sort(prev, &zm.min_value) == std::cmp::Ordering::Greater {
                return Ok(false);
            }
        }
        previous_max = Some(zm.max_value);
    }
    Ok(true)
}

fn first_segment_with_max_at_least(
    segments: &[PathBuf],
    mgr: &SegmentManager,
    col_idx: usize,
    value: &ScalarValue,
    inclusive: bool,
) -> Result<Option<usize>> {
    let mut lo = 0usize;
    let mut hi = segments.len();
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let Some(zm) = segment_cluster_zone_map(mgr, &segments[mid], col_idx)? else {
            return Ok(None);
        };
        let cmp = compare_scalars_for_sort(&zm.max_value, value);
        let before = if inclusive {
            cmp == std::cmp::Ordering::Less
        } else {
            cmp != std::cmp::Ordering::Greater
        };
        if before {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    Ok(Some(lo))
}

fn last_segment_with_min_at_most(
    segments: &[PathBuf],
    mgr: &SegmentManager,
    col_idx: usize,
    value: &ScalarValue,
    inclusive: bool,
) -> Result<Option<isize>> {
    let mut lo = 0usize;
    let mut hi = segments.len();
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let Some(zm) = segment_cluster_zone_map(mgr, &segments[mid], col_idx)? else {
            return Ok(None);
        };
        let cmp = compare_scalars_for_sort(&zm.min_value, value);
        let at_or_before = if inclusive {
            cmp != std::cmp::Ordering::Greater
        } else {
            cmp == std::cmp::Ordering::Less
        };
        if at_or_before {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    Ok(Some(lo as isize - 1))
}

fn segment_cluster_zone_map(
    mgr: &SegmentManager,
    segment: &std::path::Path,
    col_idx: usize,
) -> Result<Option<ZoneMap>> {
    let reader = mgr.open_reader_cached(segment)?;
    let Some(zm) = reader.segment_zone_maps().get(col_idx) else {
        return Ok(None);
    };
    if zm.row_count == 0 || zm.min_value.is_null() || zm.max_value.is_null() {
        return Ok(None);
    }
    Ok(Some(zm.clone()))
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
