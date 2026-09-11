use std::collections::{HashMap, HashSet};
use std::path::Path;

use super::codec::{
    decode_unique_index_delta, encode_unique_key, UniqueIndexDelta, UniqueIndexDeltaRecord,
    UNIQUE_INDEX_BLOCK_SIZE,
};
use super::types::{
    UniqueIndexBlock, UniqueIndexData, UniqueIndexEntry, UniqueIndexState, UniqueRowLocation,
};
use crate::common::error::Result;
use crate::storage::segment_manager::SegmentManager;
use crate::types::batch::RecordBatch;
use crate::types::scalar::ScalarValue;
/// Merges a base snapshot with serialized delta records and a set of
/// in-memory tombstones into one materialized `UniqueIndexData`. Progress
/// fields are copied from the base; callers override them as needed.
///
/// Records replay strictly in file order: a removed key only erases entries
/// recorded before the removal, so a later re-insert of the same key in the
/// same delta file survives (mirrors the in-memory mutation order).
pub(super) fn materialize_unique_index_state_parts(
    state_base: &UniqueIndexData,
    delta_bytes: &[u8],
    tombstones: &HashSet<UniqueRowLocation>,
    removed_keys: &HashSet<Vec<u8>>,
) -> Result<UniqueIndexData> {
    let decoded = if delta_bytes.is_empty() {
        UniqueIndexDelta::default()
    } else {
        decode_unique_index_delta(delta_bytes)?
    };
    let mut data = UniqueIndexData {
        db: state_base.db.clone(),
        table: state_base.table.clone(),
        index_name: state_base.index_name.clone(),
        columns: state_base.columns.clone(),
        entries: state_base
            .entries
            .iter()
            .map(|entry| UniqueIndexEntry {
                key: if entry.location.segment_path.is_empty() {
                    entry.key.clone()
                } else {
                    Vec::new()
                },
                location: entry.location.clone(),
            })
            .collect(),
        flush_epoch: state_base.flush_epoch,
        segment_watermarks: state_base.segment_watermarks.clone(),
        tombstones: state_base.tombstones.clone(),
        encoded_keys: state_base.encoded_keys.clone(),
        blocks: Vec::new(),
    };
    data.tombstones.extend(tombstones.iter().cloned());
    let mut rows: Vec<(Vec<u8>, UniqueIndexEntry)> = data
        .encoded_keys
        .iter()
        .cloned()
        .zip(data.entries.iter().cloned())
        .collect();
    let mut dead: Vec<bool> = vec![false; rows.len()];
    let mut positions_by_key: HashMap<Vec<u8>, Vec<usize>> = HashMap::new();
    for (idx, (encoded, _)) in rows.iter().enumerate() {
        positions_by_key
            .entry(encoded.clone())
            .or_default()
            .push(idx);
    }
    let kill_key_rows =
        |key: &[u8],
         rows: &mut Vec<(Vec<u8>, UniqueIndexEntry)>,
         dead: &mut Vec<bool>,
         positions_by_key: &mut HashMap<Vec<u8>, Vec<usize>>| {
            if let Some(positions) = positions_by_key.remove(key) {
                for position in positions {
                    rows[position].0 = Vec::new();
                    dead[position] = true;
                }
            }
        };
    for removed in removed_keys {
        kill_key_rows(removed, &mut rows, &mut dead, &mut positions_by_key);
    }
    for record in decoded.records {
        match record {
            UniqueIndexDeltaRecord::Entry(addition) => {
                positions_by_key
                    .entry(addition.encoded_key.clone())
                    .or_default()
                    .push(rows.len());
                rows.push((addition.encoded_key, addition.entry));
                dead.push(false);
            }
            UniqueIndexDeltaRecord::Tombstone(location) => {
                data.tombstones.insert(location);
            }
            UniqueIndexDeltaRecord::RemovedKey(encoded) => {
                kill_key_rows(&encoded, &mut rows, &mut dead, &mut positions_by_key);
            }
        }
    }
    let mut kept: Vec<(Vec<u8>, UniqueIndexEntry)> = Vec::with_capacity(rows.len());
    for (idx, row) in rows.into_iter().enumerate() {
        if !dead[idx] {
            kept.push(row);
        }
    }
    kept.sort_by(|left, right| left.0.cmp(&right.0));
    data.encoded_keys = kept.iter().map(|(encoded, _)| encoded.clone()).collect();
    data.entries = kept.into_iter().map(|(_, entry)| entry).collect();
    data.blocks = build_unique_index_blocks(&data.encoded_keys);
    Ok(data)
}

pub(super) fn unique_index_key(db: &str, table: &str, index_name: &str) -> String {
    format!("{}\0{}\0{}", db, table, index_name.to_ascii_lowercase())
}

pub(super) fn projected_unique_key_for_row(
    batch: &RecordBatch,
    row_idx: usize,
) -> Option<Vec<ScalarValue>> {
    let mut key = Vec::with_capacity(batch.num_columns());
    for col_idx in 0..batch.num_columns() {
        let value = batch
            .scalar(row_idx, col_idx)
            .unwrap_or_else(|| ScalarValue::null_for_type(batch.schema.columns[col_idx].data_type));
        if value.is_null() {
            return None;
        }
        key.push(value);
    }
    Some(key)
}

pub(super) fn unique_index_state_contains_any_key(
    segment_mgr: &SegmentManager,
    state: &UniqueIndexState,
    sorted_encoded_keys: &[Vec<u8>],
) -> Result<bool> {
    if sorted_encoded_keys.is_empty() {
        return Ok(false);
    }
    let delta = state.delta.read().unwrap();
    for encoded in sorted_encoded_keys {
        if let Some(locations) = delta.key_locations.get(encoded.as_slice()) {
            for location in locations {
                if !unique_index_tombstones_contains(&delta.tombstones, location)
                    && unique_index_entry_location_is_live(segment_mgr, location)?
                {
                    return Ok(true);
                }
            }
        }
    }
    let tombstones = &delta.tombstones;
    let base = &state.base;
    for encoded in sorted_encoded_keys {
        if delta.removed_keys.contains(encoded.as_slice()) {
            continue;
        }
        let Ok(mut idx) = base
            .encoded_keys
            .binary_search_by(|key| key.as_slice().cmp(encoded.as_slice()))
        else {
            continue;
        };
        while idx > 0 && base.encoded_keys[idx - 1] == *encoded {
            idx -= 1;
        }
        while idx < base.encoded_keys.len() && base.encoded_keys[idx] == *encoded {
            let location = &base.entries[idx].location;
            if !unique_index_tombstones_contains(tombstones, location)
                && unique_index_entry_location_is_live(segment_mgr, location)?
            {
                return Ok(true);
            }
            idx += 1;
        }
    }
    Ok(false)
}

pub(super) fn unique_index_state_contains_key(
    segment_mgr: &SegmentManager,
    state: &UniqueIndexState,
    encoded_key: &[u8],
) -> Result<bool> {
    let delta = state.delta.read().unwrap();
    if let Some(locations) = delta.key_locations.get(encoded_key) {
        for location in locations {
            if !unique_index_tombstones_contains(&delta.tombstones, location)
                && unique_index_entry_location_is_live(segment_mgr, location)?
            {
                return Ok(true);
            }
        }
    }
    if delta.removed_keys.contains(encoded_key) {
        return Ok(false);
    }
    let tombstones = &delta.tombstones;
    let base = &state.base;
    let Ok(mut idx) = base
        .encoded_keys
        .binary_search_by(|key| key.as_slice().cmp(encoded_key))
    else {
        return Ok(false);
    };
    while idx > 0 && base.encoded_keys[idx - 1].as_slice() == encoded_key {
        idx -= 1;
    }
    while idx < base.encoded_keys.len() && base.encoded_keys[idx].as_slice() == encoded_key {
        let location = &base.entries[idx].location;
        if !unique_index_tombstones_contains(tombstones, location)
            && unique_index_entry_location_is_live(segment_mgr, location)?
        {
            return Ok(true);
        }
        idx += 1;
    }
    Ok(false)
}

pub(super) fn prepare_unique_index_data(data: &mut UniqueIndexData) -> Result<()> {
    let mut encoded = data
        .entries
        .iter()
        .map(|entry| Ok((encode_unique_key(&entry.key)?, entry.clone())))
        .collect::<Result<Vec<_>>>()?;
    encoded.sort_by(|left, right| left.0.cmp(&right.0));
    data.entries.clear();
    data.encoded_keys.clear();
    data.entries.reserve(encoded.len());
    data.encoded_keys.reserve(encoded.len());
    for (key, entry) in encoded {
        data.encoded_keys.push(key);
        data.entries.push(entry);
    }
    data.blocks = build_unique_index_blocks(&data.encoded_keys);
    Ok(())
}

pub(super) fn build_unique_index_blocks(encoded_keys: &[Vec<u8>]) -> Vec<UniqueIndexBlock> {
    let mut blocks = Vec::new();
    let mut start = 0usize;
    while start < encoded_keys.len() {
        let len = UNIQUE_INDEX_BLOCK_SIZE.min(encoded_keys.len() - start);
        blocks.push(UniqueIndexBlock {
            first_key: encoded_keys[start].clone(),
            start,
            len,
        });
        start += len;
    }
    blocks
}

/// In-place snapshot compaction for prepared data (entries aligned with
/// encoded_keys): drops tombstoned locations and collapses duplicate
/// memtable-resident placeholders (empty segment path) to one entry per key.
///
/// Placeholder dedup is semantics-preserving: placeholders are never
/// tombstoned (tombstones only reference segment locations), so a key probes
/// as existing before the compaction if and only if it still probes as
/// existing afterwards. Without it, high-frequency upserts of the same key
/// accumulate one placeholder per write and the snapshot grows with total
/// upsert count instead of table cardinality.
pub(super) fn compact_prepared_unique_index_entries(data: &mut UniqueIndexData) {
    let mut keep = Vec::with_capacity(data.entries.len());
    let mut seen_memtable_keys: HashSet<Vec<u8>> = HashSet::new();
    for idx in 0..data.entries.len() {
        let location = &data.entries[idx].location;
        let entry_keep = if data.tombstones.contains(location) {
            false
        } else if location.segment_path.is_empty() {
            seen_memtable_keys.insert(data.encoded_keys[idx].clone())
        } else {
            true
        };
        keep.push(entry_keep);
    }
    let mut write = 0usize;
    for (read, entry_keep) in keep.iter().enumerate() {
        if *entry_keep {
            data.entries.swap(read, write);
            data.encoded_keys.swap(read, write);
            write += 1;
        }
    }
    data.entries.truncate(write);
    data.encoded_keys.truncate(write);
    data.blocks = build_unique_index_blocks(&data.encoded_keys);
}

pub(super) fn unique_index_tombstones_contains(
    tombstones: &HashSet<UniqueRowLocation>,
    location: &UniqueRowLocation,
) -> bool {
    tombstones.contains(location)
        || tombstones.iter().any(|tombstone| {
            tombstone.row_id == location.row_id
                && (normalize_unique_segment_path(&tombstone.segment_path)
                    == normalize_unique_segment_path(&location.segment_path)
                    || unique_segment_file_name(&tombstone.segment_path)
                        == unique_segment_file_name(&location.segment_path))
        })
}

pub(super) fn normalize_unique_segment_path(path: &str) -> String {
    path.replace('\\', "/")
}

pub(super) fn unique_segment_file_name(path: &str) -> Option<String> {
    Path::new(path)
        .file_name()
        .map(|name| name.to_string_lossy().to_string())
}

pub(super) fn unique_index_entry_location_is_live(
    segment_mgr: &SegmentManager,
    location: &UniqueRowLocation,
) -> Result<bool> {
    if location.segment_path.is_empty() {
        return Ok(true);
    }
    let path = Path::new(&location.segment_path);
    if !path.exists() {
        return Ok(false);
    }
    let reader = segment_mgr.open_reader(path)?;
    if location.row_id >= reader.row_count() {
        return Ok(false);
    }
    Ok(!crate::storage::segment::delete_bitmap::is_deleted(
        reader.delete_bitmap(),
        location.row_id,
    ))
}

pub(super) fn sanitize_file_component(name: &str) -> String {
    name.chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' {
                ch
            } else {
                '_'
            }
        })
        .collect()
}
