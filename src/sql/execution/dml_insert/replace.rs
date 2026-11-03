//! REPLACE conflict-removal machinery, previously misplaced in
//! `dml_delete.rs`: the undo journal, the under-lock conflict deleter, and
//! the finalize/restore pair that write.rs's `exec_replace` drives. Pure
//! DELETE concerns stayed in dml_delete; the shared memtable-row removal
//! plan (`ReplaceMemtableConflictPlan` family) remains there too because
//! exec_delete applies the same plans.

use super::*;
use std::path::PathBuf;

use crate::sql::execution::dml_delete::{restore_replace_memtable_undo, ReplaceDeleteUndo};
use crate::storage::catalog::schema::TableMeta;
use crate::storage::index::sort_key::compare_scalars_for_sort;
use crate::storage::partition::pruning::ZoneMap;
use crate::storage::segment::delete_bitmap;

/// Retain the previous minimum size; small batches skip checkpoint preflight.
pub(super) const REPLACE_CHECKPOINT_MIN_ROWS: u64 = 4096;

/// Preflight only: never filter batches or modify rows. The caller holds the
/// table mutation lock; after checkpointing it must resolve conflicts again,
/// because concurrent mutations and flushes may change the physical layout.
pub(super) fn select_replace_checkpoint_memtables(
    engine: &Engine,
    candidates: &[Arc<crate::storage::core::memtable::MemTable>],
    key_sets: &[(Vec<usize>, HashSet<Vec<ScalarValue>>)],
) -> Result<HashSet<uuid::Uuid>> {
    use crate::storage::core::memtable::{MemTableState, TxnVisibility};
    let mut selected = HashSet::new();
    for mt in candidates {
        crate::common::query_cancel::check()?;
        if mt.state() != MemTableState::Pending
            || mt.is_transactional()
            || mt.visibility() != TxnVisibility::Committed
            || mt.row_count() < REPLACE_CHECKPOINT_MIN_ROWS
        {
            continue;
        }
        // Surface a spill load error instead of treating it as no conflict.
        let batch = mt.read_snapshot_arc().ok_or_else(|| {
            TaibaiError::Storage(format!(
                "cannot read memtable {} for REPLACE checkpoint",
                mt.id()
            ))
        })?;
        let conflicts = engine
            .memtable_manager()
            .replace_key_conflicts(mt, key_sets, &batch);
        if !conflicts.is_empty()
            && (conflicts.len() as u64).saturating_mul(4) < batch.num_rows() as u64
        {
            selected.insert(mt.id());
        }
    }
    Ok(selected)
}

/// Resolved while holding the same table lock as conflict removal. Staging
/// private replacement rows does not change committed segment locations.
pub(super) struct ReplaceConflictLocations {
    pub direct: Vec<UniqueRowLocation>,
    pub fallback: Vec<bool>,
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
    locations: ReplaceConflictLocations,
    image_checks: &[crate::storage::segment::reader::ImageDurabilityCheck],
) -> Result<ReplaceDeleteUndo> {
    let table_dir = engine.data_dir().join(db).join(table);
    let mut undo = ReplaceDeleteUndo::default();
    if !table_dir.exists() || key_sets.is_empty() {
        return Ok(undo);
    }

    let ReplaceConflictLocations {
        direct: direct_locations,
        fallback,
    } = locations;
    let schema = Schema::new(table_meta.columns.clone());
    let fallback_key_sets = key_sets
        .iter()
        .zip(&fallback)
        .filter_map(|(key_set, fallback)| fallback.then_some(key_set))
        .collect::<Vec<_>>();
    // Mixed fallback and direct verification must share the same projection.
    let projected_indices = key_sets
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
    let verify_indices = &projected_indices;
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
        // Sort once, then slice each row group's candidates by binary search.
        // This replaces the O(row_groups * candidates) repeated HashSet walk.
        let mut candidates = direct_rows
            .remove(&seg_path)
            .unwrap_or_default()
            .into_iter()
            .filter(|row_id| *row_id < total_rows)
            .collect::<Vec<_>>();
        candidates.sort_unstable();
        for rg_idx in 0..reader.num_row_groups() {
            let rg_start = reader.row_group_start_row(rg_idx);
            let rg_rows = reader.row_group_rows(rg_idx);
            let group_candidates = replace_candidates_in_range(&candidates, rg_start, rg_rows);
            if fallback_key_sets.is_empty() {
                if group_candidates.is_empty() {
                    continue;
                }
                let batch = reader.read_row_group_columns(rg_idx, &schema, verify_indices)?;
                for &row_id in group_candidates {
                    let row_idx = (row_id - rg_start) as usize;
                    if !replace_row_conflicts(&batch, row_idx, &verify_positions) {
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
                &reader.zone_maps(rg_idx),
                &fallback_key_sets,
                &key_envelopes,
            ) && group_candidates.is_empty()
            {
                continue;
            }
            let batch = reader.read_row_group_columns(rg_idx, &schema, &projected_indices)?;
            let mut direct_candidates = group_candidates.iter().copied().peekable();
            for row_idx in 0..batch.num_rows() {
                let absolute_row = rg_start + row_idx as u64;
                let is_direct = direct_candidates.next_if_eq(&absolute_row).is_some();
                if delete_bitmap::is_deleted(&new_bitmap, absolute_row) {
                    continue;
                }
                let conflicts = replace_row_conflicts(&batch, row_idx, &projected_positions)
                    || (is_direct && replace_row_conflicts(&batch, row_idx, &verify_positions));
                if !conflicts {
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

fn replace_candidates_in_range(candidates: &[u64], start: u64, rows: u64) -> &[u64] {
    let first = candidates.partition_point(|row| *row < start);
    let last = candidates.partition_point(|row| *row < start.saturating_add(rows));
    &candidates[first..last]
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

#[cfg(test)]
#[path = "../replace_pruning_test.rs"]
mod replace_pruning_tests;
