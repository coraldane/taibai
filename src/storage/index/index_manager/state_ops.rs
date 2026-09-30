use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::path::Path;

use super::codec::{
    decode_unique_index_delta, encode_unique_key, UniqueIndexBaseWriter, UniqueIndexDeltaRecord,
    UNIQUE_INDEX_BLOCK_SIZE,
};
use super::types::{
    UniqueIndexBase, UniqueIndexBlock, UniqueIndexData, UniqueIndexState, UniqueRowLocation,
};
use crate::common::error::Result;
use crate::storage::segment_manager::SegmentManager;
use crate::types::batch::RecordBatch;
use crate::types::scalar::ScalarValue;

/// Delta records replayed to their final per-key state before a streaming
/// merge. Replay preserves record order semantics: a removed key erases
/// additions recorded before the removal, so a later re-insert of the same
/// key in the same delta file survives (mirrors the in-memory mutation
/// order).
pub(super) struct ReplayedDelta {
    /// Surviving additions keyed by encoded key, in replay order.
    pub(super) additions: HashMap<Vec<u8>, Vec<UniqueRowLocation>>,
    /// Keys whose base rows die. Includes the snapshot's in-memory removals
    /// plus every removed-key record seen during replay.
    pub(super) removed_keys: HashSet<Vec<u8>>,
    pub(super) tombstones: HashSet<UniqueRowLocation>,
}

pub(super) fn replay_unique_index_delta(
    delta_bytes: &[u8],
    removed_keys: HashSet<Vec<u8>>,
    tombstones: HashSet<UniqueRowLocation>,
) -> Result<ReplayedDelta> {
    let mut additions: HashMap<Vec<u8>, Vec<UniqueRowLocation>> = HashMap::new();
    let mut removed_keys = removed_keys;
    let mut tombstones = tombstones;
    if !delta_bytes.is_empty() {
        for record in decode_unique_index_delta(delta_bytes)?.records {
            match record {
                UniqueIndexDeltaRecord::Entry(addition) => {
                    additions
                        .entry(addition.encoded_key)
                        .or_default()
                        .push(addition.entry.location);
                }
                UniqueIndexDeltaRecord::Tombstone(location) => {
                    tombstones.insert(location);
                }
                UniqueIndexDeltaRecord::RemovedKey(encoded) => {
                    additions.remove(&encoded);
                    removed_keys.insert(encoded);
                }
            }
        }
    }
    Ok(ReplayedDelta {
        additions,
        removed_keys,
        tombstones,
    })
}

/// Merge the (sorted, file-backed) base with the replayed delta into `writer`
/// in one streaming pass. Base blocks are decoded one at a time; the only
/// unbounded-memory structures are the delta additions themselves, which the
/// compaction threshold already caps. Output order matches the old in-memory
/// materialization: ascending encoded key, base locations before delta
/// additions within a key, one memtable placeholder per key, tombstoned
/// locations dropped.
pub(super) fn stream_merge_unique_index_base<W: Write>(
    base: &UniqueIndexBase,
    replayed: ReplayedDelta,
    writer: &mut UniqueIndexBaseWriter<W>,
) -> Result<()> {
    let ReplayedDelta {
        additions,
        removed_keys,
        tombstones,
    } = replayed;
    let mut delta_keys: Vec<(Vec<u8>, Vec<UniqueRowLocation>)> = additions.into_iter().collect();
    delta_keys.sort_by(|left, right| left.0.cmp(&right.0));
    let mut delta_pos = 0usize;
    let mut placeholder_seen: HashSet<Vec<u8>> = HashSet::new();

    // Emit one location unless a tombstone kills it or a placeholder for the
    // key was already written (placeholders collapse to one per key).
    fn emit<W: Write>(
        writer: &mut UniqueIndexBaseWriter<W>,
        key: &[u8],
        location: &UniqueRowLocation,
        tombstones: &HashSet<UniqueRowLocation>,
        placeholder_seen: &mut HashSet<Vec<u8>>,
    ) -> Result<()> {
        if unique_index_tombstones_contains(tombstones, location) {
            return Ok(());
        }
        if location.segment_path.is_empty() && !placeholder_seen.insert(key.to_vec()) {
            return Ok(());
        }
        writer.push_entry(key, location)
    }

    for block_idx in 0..base.blocks.len() {
        let block = base.block(block_idx)?;
        for entry_idx in 0..block.keys.len() {
            let key = &block.keys[entry_idx];
            // Delta-only keys that sort before this base key are emitted
            // first so the output stays globally sorted.
            while delta_pos < delta_keys.len()
                && delta_keys[delta_pos].0.as_slice() < key.as_slice()
            {
                let (delta_key, delta_locations) = &delta_keys[delta_pos];
                for location in delta_locations {
                    emit(
                        writer,
                        delta_key,
                        location,
                        &tombstones,
                        &mut placeholder_seen,
                    )?;
                }
                delta_pos += 1;
            }
            let delta_key_is_current = delta_pos < delta_keys.len()
                && delta_keys[delta_pos].0.as_slice() == key.as_slice();
            if removed_keys.contains(key.as_slice()) {
                // Base rows of a removed key die; surviving delta additions
                // for the same key still merge in.
                if delta_key_is_current {
                    for location in &delta_keys[delta_pos].1 {
                        emit(writer, key, location, &tombstones, &mut placeholder_seen)?;
                    }
                    delta_pos += 1;
                }
                continue;
            }
            emit(
                writer,
                key,
                &block.locations[entry_idx],
                &tombstones,
                &mut placeholder_seen,
            )?;
            if delta_key_is_current {
                for location in &delta_keys[delta_pos].1 {
                    emit(writer, key, location, &tombstones, &mut placeholder_seen)?;
                }
                delta_pos += 1;
            }
        }
    }
    // Remaining delta-only keys sort after every base key.
    while delta_pos < delta_keys.len() {
        let (delta_key, delta_locations) = &delta_keys[delta_pos];
        for location in delta_locations {
            emit(
                writer,
                delta_key,
                location,
                &tombstones,
                &mut placeholder_seen,
            )?;
        }
        delta_pos += 1;
    }
    Ok(())
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
        for location in base.find_locations(encoded)? {
            if !unique_index_tombstones_contains(tombstones, &location)
                && unique_index_entry_location_is_live(segment_mgr, &location)?
            {
                return Ok(true);
            }
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
    for location in state.base.find_locations(encoded_key)? {
        if !unique_index_tombstones_contains(tombstones, &location)
            && unique_index_entry_location_is_live(segment_mgr, &location)?
        {
            return Ok(true);
        }
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
