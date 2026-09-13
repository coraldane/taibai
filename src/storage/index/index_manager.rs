use std::borrow::Borrow;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

use crate::common::error::{Result, TaibaiError};
use crate::storage::segment_manager::SegmentManager;
use crate::types::scalar::ScalarValue;

mod codec;
mod compaction;
mod state_ops;
mod types;

use self::codec::*;
#[cfg(test)]
use self::compaction::UniqueIndexCompactionJob;
use self::state_ops::*;
use self::types::*;
pub use self::types::{
    PreparedUniqueIndexEntry, UniqueIndexBlock, UniqueIndexData, UniqueIndexEntry,
    UniqueIndexSnapshot, UniqueRowLocation,
};
pub const PRIMARY_LOCATION_INDEX_NAME: &str = "__taibai_primary_location";

pub struct IndexManager {
    unique_indexes: Arc<RwLock<HashMap<String, Arc<UniqueIndexState>>>>,
    primary_key_indexes: RwLock<HashMap<String, HashSet<Vec<u8>>>>,
    segment_mgr: Arc<SegmentManager>,
    data_dir: PathBuf,
    unique_index_compaction: Mutex<UniqueIndexCompactionSchedule>,
}

#[derive(Default)]
struct UniqueIndexCompactionSchedule {
    pause_count: usize,
    pending: HashSet<(String, String, String)>,
}

impl IndexManager {
    pub fn new(segment_mgr: Arc<SegmentManager>) -> Self {
        Self::new_with_data_dir(segment_mgr, PathBuf::from("/tmp/taibai_default"))
    }

    pub fn new_with_data_dir(segment_mgr: Arc<SegmentManager>, data_dir: PathBuf) -> Self {
        Self {
            unique_indexes: Arc::new(RwLock::new(HashMap::new())),
            primary_key_indexes: RwLock::new(HashMap::new()),
            segment_mgr,
            data_dir,
            unique_index_compaction: Mutex::new(UniqueIndexCompactionSchedule::default()),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn persist_unique_index_entries(
        &self,
        db: &str,
        table: &str,
        index_name: &str,
        columns: &[String],
        entries: Vec<UniqueIndexEntry>,
        flush_epoch: Option<u64>,
        segment_watermarks: Option<HashMap<String, u64>>,
    ) -> Result<()> {
        let segment_watermarks = match segment_watermarks {
            Some(watermarks) => watermarks,
            None => self.current_segment_watermarks(db, table)?,
        };
        let dir = self.unique_index_dir(db, table);
        std::fs::create_dir_all(&dir)?;
        let mut data = UniqueIndexData {
            db: db.to_string(),
            table: table.to_string(),
            index_name: index_name.to_string(),
            columns: columns.to_vec(),
            entries,
            flush_epoch,
            segment_watermarks,
            tombstones: HashSet::new(),
            encoded_keys: Vec::new(),
            blocks: Vec::new(),
        };
        prepare_unique_index_data(&mut data)?;
        self.persist_prepared_unique_index_data(db, table, index_name, data)
    }

    /// Persist an already-prepared snapshot (entries and encoded keys aligned,
    /// blocks built) without re-encoding or re-cloning it. Refresh and delta
    /// compaction rewrite large snapshots through this path to keep peak
    /// memory at one materialized copy plus the encoded bytes.
    fn persist_prepared_unique_index_data(
        &self,
        db: &str,
        table: &str,
        index_name: &str,
        data: UniqueIndexData,
    ) -> Result<()> {
        let dir = self.unique_index_dir(db, table);
        std::fs::create_dir_all(&dir)?;
        let base = sanitize_file_component(index_name);
        let data_path = dir.join(format!("{}.keys.tbi", base));
        let data_tmp = data_path.with_extension("keys.tbi.tmp");
        let invalid_path = dir.join(format!("{}.invalid", base));
        let data_bytes = encode_unique_index_data(&data)?;
        std::fs::write(&data_tmp, data_bytes)?;
        std::fs::rename(data_tmp, data_path)?;
        let delta_path = dir.join(format!("{}.delta.tbi", base));
        let _ = std::fs::remove_file(delta_path);
        let _ = std::fs::remove_file(dir.join(format!("{}.progress.tbi", base)));
        let _ = std::fs::remove_file(invalid_path);
        self.unique_indexes.write().unwrap().insert(
            unique_index_key(db, table, index_name),
            Arc::new(UniqueIndexState::new(
                data,
                UniqueIndexDeltaState::default(),
            )),
        );
        Ok(())
    }

    pub fn load_unique_index_snapshot(
        &self,
        db: &str,
        table: &str,
        index_name: &str,
    ) -> Result<Option<UniqueIndexSnapshot>> {
        let Some(state) = self.load_unique_index_state(db, table, index_name)? else {
            return Ok(None);
        };
        let delta = state.delta.read().unwrap();
        Ok(Some(UniqueIndexSnapshot {
            db: state.base.db.clone(),
            table: state.base.table.clone(),
            index_name: state.base.index_name.clone(),
            columns: state.base.columns.clone(),
            key_count: state.base.entries.len() + delta.entry_count as usize,
        }))
    }

    fn load_unique_index_state(
        &self,
        db: &str,
        table: &str,
        index_name: &str,
    ) -> Result<Option<Arc<UniqueIndexState>>> {
        let cache_key = unique_index_key(db, table, index_name);
        if let Some(state) = self.unique_indexes.read().unwrap().get(&cache_key).cloned() {
            return Ok(Some(state));
        }
        let base = sanitize_file_component(index_name);
        let dir = self.unique_index_dir(db, table);
        if dir.join(format!("{}.invalid", base)).exists() {
            return Ok(None);
        }
        let path = dir.join(format!("{}.keys.tbi", base));
        if !path.exists() {
            return Ok(None);
        }
        let bytes = std::fs::read(path)?;
        let Some(mut data) = decode_unique_index_data(&bytes)? else {
            return Ok(None);
        };
        if data.encoded_keys.len() != data.entries.len() {
            prepare_unique_index_data(&mut data)?;
        }
        let delta_path = self
            .unique_index_dir(db, table)
            .join(format!("{}.delta.tbi", base));
        let delta = if delta_path.exists() {
            decode_unique_index_delta(&std::fs::read(delta_path)?)?.into_state()?
        } else {
            UniqueIndexDeltaState::default()
        };
        let progress_path = dir.join(format!("{}.progress.tbi", base));
        let state = if progress_path.exists() {
            let progress = decode_unique_index_progress(&std::fs::read(progress_path)?);
            Arc::new(UniqueIndexState::with_progress(data, delta, progress))
        } else {
            Arc::new(UniqueIndexState::new(data, delta))
        };
        let current = self
            .unique_indexes
            .write()
            .unwrap()
            .entry(cache_key)
            .or_insert_with(|| Arc::clone(&state))
            .clone();
        Ok(Some(current))
    }

    fn materialize_unique_index_state(
        &self,
        db: &str,
        table: &str,
        index_name: &str,
        state: &UniqueIndexState,
    ) -> Result<UniqueIndexData> {
        let (tombstones, removed_keys) = {
            let delta = state.delta.read().unwrap();
            (delta.tombstones.clone(), delta.removed_keys.clone())
        };
        let dir = self.unique_index_dir(db, table);
        let base = sanitize_file_component(index_name);
        let delta_path = dir.join(format!("{}.delta.tbi", base));
        let delta_bytes = if delta_path.exists() {
            std::fs::read(&delta_path)?
        } else {
            Vec::new()
        };
        materialize_unique_index_state_parts(&state.base, &delta_bytes, &tombstones, &removed_keys)
    }

    pub fn load_unique_index_data(
        &self,
        db: &str,
        table: &str,
        index_name: &str,
    ) -> Result<Option<Arc<UniqueIndexData>>> {
        let Some(state) = self.load_unique_index_state(db, table, index_name)? else {
            return Ok(None);
        };
        Ok(Some(Arc::new(self.materialize_unique_index_state(
            db, table, index_name, &state,
        )?)))
    }

    pub fn unique_index_columns(
        &self,
        db: &str,
        table: &str,
        index_name: &str,
    ) -> Result<Option<Vec<String>>> {
        Ok(self
            .load_unique_index_state(db, table, index_name)?
            .map(|state| state.base.columns.clone()))
    }

    pub fn primary_keys_exist(
        &self,
        db: &str,
        table: &str,
        keys: &[Vec<ScalarValue>],
    ) -> Result<Option<bool>> {
        let cache_key = format!("{}\0{}", db, table);
        let indexes = self.primary_key_indexes.read().unwrap();
        let Some(index) = indexes.get(&cache_key) else {
            return Ok(None);
        };
        for key in keys {
            if index.contains(&encode_unique_key(key)?) {
                return Ok(Some(true));
            }
        }
        Ok(Some(false))
    }

    pub fn install_primary_key_index(
        &self,
        db: &str,
        table: &str,
        keys: impl IntoIterator<Item = Vec<ScalarValue>>,
    ) -> Result<()> {
        let encoded = keys
            .into_iter()
            .map(|key| encode_unique_key(&key))
            .collect::<Result<HashSet<_>>>()?;
        self.primary_key_indexes
            .write()
            .unwrap()
            .insert(format!("{}\0{}", db, table), encoded);
        Ok(())
    }

    pub fn append_primary_keys(
        &self,
        db: &str,
        table: &str,
        keys: &[Vec<ScalarValue>],
    ) -> Result<bool> {
        let cache_key = format!("{}\0{}", db, table);
        let mut indexes = self.primary_key_indexes.write().unwrap();
        let Some(current) = indexes.get_mut(&cache_key) else {
            return Ok(false);
        };
        for key in keys {
            current.insert(encode_unique_key(key)?);
        }
        Ok(true)
    }

    pub fn invalidate_primary_key_cache(&self, db: &str, table: &str) {
        self.primary_key_indexes
            .write()
            .unwrap()
            .remove(&format!("{}\0{}", db, table));
    }

    pub fn invalidate_primary_key_index(&self, db: &str, table: &str) {
        self.invalidate_primary_key_cache(db, table);
        self.invalidate_unique_index_snapshot_if_present(db, table, PRIMARY_LOCATION_INDEX_NAME);
    }

    pub fn unique_key_exists(
        &self,
        db: &str,
        table: &str,
        index_name: &str,
        key: &[ScalarValue],
    ) -> Result<Option<bool>> {
        let Some(mut state) = self.load_unique_index_state(db, table, index_name)? else {
            return Ok(None);
        };
        // Same staleness guard as unique_key_locations: a snapshot built
        // before rows reached segments (memtable flush or direct append)
        // misses live keys, so refresh it before probing.
        if state.progress_epoch() != Some(self.segment_mgr.memtable_flush_epoch(db, table)) {
            let Some(refreshed) =
                self.refresh_stale_unique_index_state(db, table, index_name, &state)?
            else {
                return Ok(None);
            };
            state = refreshed;
        }
        let encoded = encode_unique_key(key)?;
        Ok(Some(unique_index_state_contains_key(
            &self.segment_mgr,
            &state,
            &encoded,
        )?))
    }

    fn current_segment_watermarks(&self, db: &str, table: &str) -> Result<HashMap<String, u64>> {
        let table_dir = self.data_dir.join(db).join(table);
        let mut watermarks = HashMap::new();
        if table_dir.exists() {
            for seg_path in SegmentManager::discover_segments(&table_dir)? {
                let reader = self.segment_mgr.open_reader_cached(&seg_path)?;
                watermarks.insert(seg_path.to_string_lossy().to_string(), reader.row_count());
            }
        }
        Ok(watermarks)
    }

    /// Incrementally catch a stale unique-index snapshot up to the current
    /// memtable-flush epoch: only rows appended to segments beyond the
    /// snapshot's per-segment watermarks are scanned, instead of rebuilding
    /// the whole snapshot from every segment. Returns the refreshed state, or
    /// `None` when a safe incremental refresh is not possible and callers
    /// should fall back to a full rebuild.
    fn refresh_stale_unique_index_state(
        &self,
        db: &str,
        table: &str,
        index_name: &str,
        state: &Arc<UniqueIndexState>,
    ) -> Result<Option<Arc<UniqueIndexState>>> {
        if state.base.columns.is_empty() {
            return Ok(None);
        }
        let epoch_before = self.segment_mgr.memtable_flush_epoch(db, table);
        let _mutation_guard = state.mutation_lock.lock().unwrap();
        let cache_key = unique_index_key(db, table, index_name);
        let is_current = self
            .unique_indexes
            .read()
            .unwrap()
            .get(&cache_key)
            .is_some_and(|current| Arc::ptr_eq(current, state));
        if !is_current {
            // Another thread refreshed (or replaced) the state concurrently;
            // reuse its result instead of falling back to a full rebuild.
            return Ok(self
                .unique_indexes
                .read()
                .unwrap()
                .get(&cache_key)
                .filter(|current| {
                    current.progress_epoch()
                        == Some(self.segment_mgr.memtable_flush_epoch(db, table))
                })
                .cloned());
        }
        let progress_watermarks = state.progress_watermarks();
        let mut additions = Vec::new();
        let mut watermarks = HashMap::new();
        let table_dir = self.data_dir.join(db).join(table);
        if table_dir.exists() {
            for seg_path in SegmentManager::discover_segments(&table_dir)? {
                let reader = self.segment_mgr.open_reader_cached(&seg_path)?;
                let current_rows = reader.row_count();
                let segment_path = seg_path.to_string_lossy().to_string();
                let watermark = progress_watermarks.get(&segment_path).copied().unwrap_or(0);
                watermarks.insert(segment_path, current_rows);
                if current_rows <= watermark {
                    continue;
                }
                let schema = reader.schema();
                let indices = state
                    .base
                    .columns
                    .iter()
                    .map(|column| {
                        schema.index_of(column).ok_or_else(|| {
                            TaibaiError::Storage(format!(
                                "unique index column '{}' missing from segment {}",
                                column,
                                seg_path.display()
                            ))
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                for rg_idx in 0..reader.num_row_groups() {
                    let rg_start = reader.row_group_start_row(rg_idx);
                    let rg_rows = reader.row_group_rows(rg_idx);
                    if rg_start + rg_rows <= watermark {
                        continue;
                    }
                    let batch = reader.read_row_group_with_projection(rg_idx, &indices)?;
                    let delete_bitmap = reader.delete_bitmap();
                    for row_idx in 0..batch.num_rows() {
                        let row_id = rg_start + row_idx as u64;
                        if crate::storage::segment::delete_bitmap::is_deleted(delete_bitmap, row_id)
                        {
                            continue;
                        }
                        let key = projected_unique_key_for_row(&batch, row_idx);
                        let Some(key) = key else { continue };
                        additions.push(UniqueIndexEntry {
                            key,
                            location: UniqueRowLocation {
                                segment_path: seg_path.to_string_lossy().to_string(),
                                row_id,
                            },
                        });
                    }
                }
            }
        }
        let prepared_additions = self.prepare_unique_index_entries(additions)?;
        let delta_path = self
            .unique_index_dir(db, table)
            .join(format!("{}.delta.tbi", sanitize_file_component(index_name)));
        append_prepared_unique_index_delta(&delta_path, &prepared_additions)?;
        let delta_count = {
            let mut delta = state.delta.write().unwrap();
            for addition in prepared_additions {
                delta
                    .key_locations
                    .entry(addition.encoded_key.clone())
                    .or_default()
                    .push(addition.entry.location.clone());
                delta.entry_count += 1;
            }
            delta
                .entry_count
                .saturating_add(delta.tombstones.len() as u64)
        };
        let needs_compaction =
            unique_index_delta_needs_compaction(delta_count, state.base.entries.len());
        state.set_progress(Some(epoch_before), watermarks);
        self.persist_unique_index_progress(db, table, index_name)?;
        drop(_mutation_guard);
        if needs_compaction {
            self.try_dispatch_unique_index_compaction(db, table, index_name, state);
        }
        Ok(Some(Arc::clone(state)))
    }

    /// Persist the in-memory refresh progress (epoch plus per-segment
    /// watermarks) as a small sidecar file so restarts resume from it
    /// instead of rescanning already-indexed segment tails.
    fn persist_unique_index_progress(&self, db: &str, table: &str, index_name: &str) -> Result<()> {
        let cache_key = unique_index_key(db, table, index_name);
        let Some(state) = self.unique_indexes.read().unwrap().get(&cache_key).cloned() else {
            return Ok(());
        };
        let progress = state.progress.read().unwrap().clone();
        let dir = self.unique_index_dir(db, table);
        std::fs::create_dir_all(&dir)?;
        let base = sanitize_file_component(index_name);
        let path = dir.join(format!("{}.progress.tbi", base));
        let tmp = dir.join(format!("{}.progress.tbi.tmp", base));
        std::fs::write(&tmp, encode_unique_index_progress(&progress))?;
        std::fs::rename(tmp, path)?;
        Ok(())
    }

    /// Bring a persisted unique-index snapshot up to the current
    /// segment-content epoch without probing keys: the incremental refresh
    /// appends only the newly written segment tail to the delta. Bulk write
    /// paths call this after a successful write so the next batch starts
    /// against an up-to-date index instead of paying a full snapshot rewrite.
    pub fn refresh_unique_index_state(
        &self,
        db: &str,
        table: &str,
        index_name: &str,
    ) -> Result<()> {
        let Some(state) = self.load_unique_index_state(db, table, index_name)? else {
            return Ok(());
        };
        if state.progress_epoch() == Some(self.segment_mgr.memtable_flush_epoch(db, table)) {
            return Ok(());
        }
        self.refresh_stale_unique_index_state(db, table, index_name, &state)?;
        Ok(())
    }

    /// `keys` is generic so callers can pass a slice, a `HashSet` of keys, or
    /// an empty iterator without cloning the key set into a `Vec` first.
    pub fn unique_key_locations<I>(
        &self,
        db: &str,
        table: &str,
        index_name: &str,
        keys: I,
    ) -> Result<Option<Vec<UniqueRowLocation>>>
    where
        I: IntoIterator,
        I::Item: std::borrow::Borrow<Vec<ScalarValue>>,
    {
        let Some(mut state) = self.load_unique_index_state(db, table, index_name)? else {
            return Ok(None);
        };
        // A memtable flush after this snapshot was built may have moved rows
        // into segments without index entries. Incrementally refresh the
        // snapshot (scanning only rows appended past its per-segment
        // watermarks) instead of forcing a full rebuild; fall back to the
        // rebuild path only when the incremental refresh is not possible.
        if state.progress_epoch() != Some(self.segment_mgr.memtable_flush_epoch(db, table)) {
            let Some(refreshed) =
                self.refresh_stale_unique_index_state(db, table, index_name, &state)?
            else {
                return Ok(None);
            };
            state = refreshed;
        }
        let encoded_keys = keys
            .into_iter()
            .map(|key| encode_unique_key(key.borrow()))
            .collect::<Result<Vec<_>>>()?;
        let mut locations = {
            let delta = state.delta.read().unwrap();
            let mut locations = Vec::new();
            for encoded in &encoded_keys {
                if let Some(matches) = delta.key_locations.get(encoded) {
                    locations.extend(
                        matches
                            .iter()
                            .filter(|location| {
                                !location.segment_path.is_empty()
                                    && !unique_index_tombstones_contains(
                                        &delta.tombstones,
                                        location,
                                    )
                            })
                            .cloned(),
                    );
                }
                let Ok(mut idx) = state
                    .base
                    .encoded_keys
                    .binary_search_by(|value| value.as_slice().cmp(encoded))
                else {
                    continue;
                };
                while idx > 0 && state.base.encoded_keys[idx - 1] == *encoded {
                    idx -= 1;
                }
                while idx < state.base.encoded_keys.len()
                    && state.base.encoded_keys[idx] == *encoded
                {
                    let location = &state.base.entries[idx].location;
                    if !location.segment_path.is_empty()
                        && !unique_index_tombstones_contains(&delta.tombstones, location)
                    {
                        locations.push(location.clone());
                    }
                    idx += 1;
                }
            }
            locations
        };
        locations.sort_by(|left, right| {
            left.segment_path
                .cmp(&right.segment_path)
                .then(left.row_id.cmp(&right.row_id))
        });
        locations.dedup();
        let mut start = 0;
        while start < locations.len() {
            let path = Path::new(&locations[start].segment_path);
            if !path.exists() {
                return Ok(None);
            }
            let reader = self.segment_mgr.open_reader_cached(path)?;
            let mut end = start + 1;
            while end < locations.len()
                && locations[end].segment_path == locations[start].segment_path
            {
                end += 1;
            }
            if locations[start..end]
                .iter()
                .any(|location| location.row_id >= reader.row_count())
            {
                return Ok(None);
            }
            start = end;
        }
        Ok(Some(locations))
    }

    /// Generic for the same reason as [`unique_key_locations`].
    pub fn unique_keys_exist<I>(
        &self,
        db: &str,
        table: &str,
        index_name: &str,
        keys: I,
    ) -> Result<Option<bool>>
    where
        I: IntoIterator,
        I::Item: std::borrow::Borrow<Vec<ScalarValue>>,
    {
        let Some(mut state) = self.load_unique_index_state(db, table, index_name)? else {
            return Ok(None);
        };
        // Same staleness guard as unique_key_locations, resolved by the same
        // incremental refresh; a full rebuild is only needed when the refresh
        // cannot run.
        if state.progress_epoch() != Some(self.segment_mgr.memtable_flush_epoch(db, table)) {
            let Some(refreshed) =
                self.refresh_stale_unique_index_state(db, table, index_name, &state)?
            else {
                return Ok(None);
            };
            state = refreshed;
        }
        let mut encoded_keys = keys
            .into_iter()
            .map(|key| encode_unique_key(key.borrow()))
            .collect::<Result<Vec<_>>>()?;
        encoded_keys.sort();
        encoded_keys.dedup();
        Ok(Some(unique_index_state_contains_any_key(
            &self.segment_mgr,
            &state,
            &encoded_keys,
        )?))
    }

    pub fn prepare_unique_index_entries(
        &self,
        entries: Vec<UniqueIndexEntry>,
    ) -> Result<Vec<PreparedUniqueIndexEntry>> {
        entries
            .into_iter()
            .map(|entry| {
                Ok(PreparedUniqueIndexEntry {
                    encoded_key: encode_unique_key(&entry.key)?,
                    entry,
                })
            })
            .collect()
    }

    pub fn append_prepared_unique_index_entries(
        &self,
        db: &str,
        table: &str,
        index_name: &str,
        entries: Vec<PreparedUniqueIndexEntry>,
    ) -> Result<bool> {
        if entries.is_empty() {
            return Ok(true);
        }
        let Some(state) = self.load_unique_index_state(db, table, index_name)? else {
            return Ok(false);
        };
        let needs_compaction = {
            let _mutation_guard = state.mutation_lock.lock().unwrap();
            let cache_key = unique_index_key(db, table, index_name);
            let is_current = self
                .unique_indexes
                .read()
                .unwrap()
                .get(&cache_key)
                .is_some_and(|current| Arc::ptr_eq(current, &state));
            if !is_current {
                drop(_mutation_guard);
                return self.append_prepared_unique_index_entries(db, table, index_name, entries);
            }
            let dir = self.unique_index_dir(db, table);
            std::fs::create_dir_all(&dir)?;
            let base = sanitize_file_component(index_name);
            let delta_path = dir.join(format!("{}.delta.tbi", base));
            append_prepared_unique_index_delta(&delta_path, &entries)?;
            {
                let mut delta = state.delta.write().unwrap();
                for addition in entries {
                    delta
                        .key_locations
                        .entry(addition.encoded_key.clone())
                        .or_default()
                        .push(addition.entry.location.clone());
                    delta.entry_count += 1;
                }
            }
            let delta_count = {
                let delta = state.delta.read().unwrap();
                delta
                    .entry_count
                    .saturating_add(delta.tombstones.len() as u64)
            };
            unique_index_delta_needs_compaction(delta_count, state.base.entries.len())
        };
        if needs_compaction {
            self.try_dispatch_unique_index_compaction(db, table, index_name, &state);
        }
        Ok(true)
    }

    pub fn append_unique_index_entries(
        &self,
        db: &str,
        table: &str,
        index_name: &str,
        entries: Vec<UniqueIndexEntry>,
    ) -> Result<bool> {
        let prepared = self.prepare_unique_index_entries(entries)?;
        self.append_prepared_unique_index_entries(db, table, index_name, prepared)
    }

    pub fn append_unique_index_tombstones(
        &self,
        db: &str,
        table: &str,
        index_name: &str,
        tombstones: Vec<UniqueRowLocation>,
    ) -> Result<bool> {
        if tombstones.is_empty() {
            return Ok(true);
        }
        let Some(state) = self.load_unique_index_state(db, table, index_name)? else {
            return Ok(false);
        };
        let needs_compaction = {
            let _mutation_guard = state.mutation_lock.lock().unwrap();
            let cache_key = unique_index_key(db, table, index_name);
            let is_current = self
                .unique_indexes
                .read()
                .unwrap()
                .get(&cache_key)
                .is_some_and(|current| Arc::ptr_eq(current, &state));
            if !is_current {
                drop(_mutation_guard);
                return self.append_unique_index_tombstones(db, table, index_name, tombstones);
            }
            let dir = self.unique_index_dir(db, table);
            let base = sanitize_file_component(index_name);
            let data_path = dir.join(format!("{}.keys.tbi", base));
            if !data_path.exists() {
                return Ok(false);
            }
            std::fs::create_dir_all(&dir)?;
            let delta_path = dir.join(format!("{}.delta.tbi", base));
            append_unique_index_tombstone_delta(&delta_path, &tombstones)?;
            let delta_count = {
                let mut delta = state.delta.write().unwrap();
                delta.tombstones.extend(tombstones);
                (delta.entry_count, delta.tombstones.len() as u64)
            };
            unique_index_delta_needs_compaction(
                delta_count.0.saturating_add(delta_count.1),
                state.base.entries.len(),
            )
        };
        if needs_compaction {
            self.try_dispatch_unique_index_compaction(db, table, index_name, &state);
        }
        Ok(true)
    }

    /// Removes every delta entry for the given keys (memtable placeholders
    /// and segment-backed locations recorded after a flush) and persists the
    /// removal as an ordered delta record, so reloads and compaction replay
    /// it before any later re-insert of the same key. Base entries for these
    /// keys are expected to be handled by location tombstones. Returns
    /// Ok(false) when the snapshot cannot be maintained (base file missing
    /// under a live state); callers fall back to snapshot invalidation.
    pub fn remove_unique_index_keys(
        &self,
        db: &str,
        table: &str,
        index_name: &str,
        keys: &[Vec<ScalarValue>],
    ) -> Result<bool> {
        let mut encoded_keys = keys
            .iter()
            .map(|key| encode_unique_key(key))
            .collect::<Result<Vec<_>>>()?;
        encoded_keys.sort();
        encoded_keys.dedup();
        if encoded_keys.is_empty() {
            return Ok(true);
        }
        let Some(state) = self.load_unique_index_state(db, table, index_name)? else {
            return Ok(true);
        };
        let needs_compaction = {
            let _mutation_guard = state.mutation_lock.lock().unwrap();
            let cache_key = unique_index_key(db, table, index_name);
            let is_current = self
                .unique_indexes
                .read()
                .unwrap()
                .get(&cache_key)
                .is_some_and(|current| Arc::ptr_eq(current, &state));
            if !is_current {
                drop(_mutation_guard);
                return self.remove_unique_index_keys(db, table, index_name, keys);
            }
            let dir = self.unique_index_dir(db, table);
            let base = sanitize_file_component(index_name);
            if !dir.join(format!("{}.keys.tbi", base)).exists() {
                return Ok(false);
            }
            std::fs::create_dir_all(&dir)?;
            let delta_path = dir.join(format!("{}.delta.tbi", base));
            append_unique_index_removed_key_delta(&delta_path, &encoded_keys)?;
            let delta_count = {
                let mut delta = state.delta.write().unwrap();
                for encoded in &encoded_keys {
                    if let Some(locations) = delta.key_locations.remove(encoded) {
                        delta.entry_count =
                            delta.entry_count.saturating_sub(locations.len() as u64);
                    }
                    delta.removed_keys.insert(encoded.clone());
                }
                delta
                    .entry_count
                    .saturating_add(delta.tombstones.len() as u64)
            };
            unique_index_delta_needs_compaction(delta_count, state.base.entries.len())
        };
        if needs_compaction {
            self.try_dispatch_unique_index_compaction(db, table, index_name, &state);
        }
        Ok(true)
    }

    pub fn invalidate_unique_index_snapshot_if_present(
        &self,
        db: &str,
        table: &str,
        index_name: &str,
    ) {
        let base = sanitize_file_component(index_name);
        let dir = self.unique_index_dir(db, table);
        if dir.join(format!("{}.keys.tbi", base)).exists()
            || self
                .unique_indexes
                .read()
                .unwrap()
                .contains_key(&unique_index_key(db, table, index_name))
        {
            self.invalidate_unique_index_snapshot(db, table, index_name);
        }
    }

    pub fn invalidate_unique_index_snapshot(&self, db: &str, table: &str, index_name: &str) {
        let dir = self.unique_index_dir(db, table);
        let base = sanitize_file_component(index_name);
        let _ = std::fs::create_dir_all(&dir);
        let _ = std::fs::write(dir.join(format!("{}.invalid", base)), []);
        self.unique_indexes
            .write()
            .unwrap()
            .remove(&unique_index_key(db, table, index_name));
    }

    pub fn invalidate_table_unique_index_snapshots(
        &self,
        db: &str,
        table: &str,
        index_names: &[String],
    ) {
        self.invalidate_primary_key_index(db, table);
        for index_name in index_names {
            self.invalidate_unique_index_snapshot(db, table, index_name);
        }
    }

    pub fn remove_unique_index_snapshot(&self, db: &str, table: &str, index_name: &str) {
        let dir = self.unique_index_dir(db, table);
        let base = sanitize_file_component(index_name);
        let _ = std::fs::remove_file(dir.join(format!("{}.keys.tbi", base)));
        let _ = std::fs::remove_file(dir.join(format!("{}.delta.tbi", base)));
        let _ = std::fs::remove_file(dir.join(format!("{}.invalid", base)));
        self.unique_indexes
            .write()
            .unwrap()
            .remove(&unique_index_key(db, table, index_name));
    }

    pub fn remove_table_unique_index_snapshots(
        &self,
        db: &str,
        table: &str,
        index_names: &[String],
    ) {
        self.invalidate_primary_key_cache(db, table);
        self.remove_unique_index_snapshot(db, table, PRIMARY_LOCATION_INDEX_NAME);
        for index_name in index_names {
            self.remove_unique_index_snapshot(db, table, index_name);
        }
    }

    fn unique_index_dir(&self, db: &str, table: &str) -> PathBuf {
        self.data_dir
            .join(db)
            .join(table)
            .join("indexes")
            .join("unique")
    }
}

#[cfg(test)]
#[path = "index_manager_test.rs"]
mod tests;

impl Default for IndexManager {
    fn default() -> Self {
        let segment_mgr = SegmentManager::new(std::path::Path::new("/tmp/taibai_default"))
            .expect("failed to create default SegmentManager");
        Self::new(Arc::new(segment_mgr))
    }
}
