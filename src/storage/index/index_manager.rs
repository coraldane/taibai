#![allow(dead_code)]
use std::collections::{HashMap, HashSet};
use std::io::{Cursor, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

use serde::{Deserialize, Serialize};

use crate::common::error::{Result, TaibaiError};
use crate::storage::index::sparse_index::SparseIndexImpl;
use crate::storage::segment_manager::SegmentManager;
use crate::types::scalar::ScalarValue;

pub const PRIMARY_LOCATION_INDEX_NAME: &str = "__taibai_primary_location";

pub struct IndexManager {
    indexes: RwLock<HashMap<String, HashMap<String, Arc<SparseIndexImpl>>>>,
    unique_indexes: RwLock<HashMap<String, Arc<UniqueIndexState>>>,
    primary_key_indexes: RwLock<HashMap<String, HashSet<Vec<u8>>>>,
    segment_mgr: Arc<SegmentManager>,
    data_dir: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UniqueIndexSnapshot {
    pub db: String,
    pub table: String,
    pub index_name: String,
    pub columns: Vec<String>,
    pub key_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct UniqueRowLocation {
    pub segment_path: String,
    pub row_id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UniqueIndexEntry {
    pub key: Vec<ScalarValue>,
    pub location: UniqueRowLocation,
}

#[derive(Clone)]
pub struct PreparedUniqueIndexEntry {
    pub encoded_key: Vec<u8>,
    pub entry: UniqueIndexEntry,
}

struct UniqueIndexState {
    flush_epoch: Option<u64>,
    base: Arc<UniqueIndexData>,
    delta: RwLock<UniqueIndexDeltaState>,
    mutation_lock: Mutex<()>,
}

#[derive(Default)]
struct UniqueIndexDeltaState {
    entries: Vec<PreparedUniqueIndexEntry>,
    key_locations: HashMap<Vec<u8>, Vec<UniqueRowLocation>>,
    tombstones: HashSet<UniqueRowLocation>,
}

impl UniqueIndexState {
    fn new(base: UniqueIndexData, delta: UniqueIndexDeltaState) -> Self {
        Self {
            flush_epoch: base.flush_epoch,
            base: Arc::new(base),
            delta: RwLock::new(delta),
            mutation_lock: Mutex::new(()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UniqueIndexData {
    pub db: String,
    pub table: String,
    pub index_name: String,
    pub columns: Vec<String>,
    pub entries: Vec<UniqueIndexEntry>,
    /// Memtable-flush epoch stamped when this snapshot was built. `None` (legacy
    /// files) is treated as stale: the first probe rebuilds the snapshot so its
    /// segment coverage is guaranteed before it is trusted for row locations.
    #[serde(default)]
    pub flush_epoch: Option<u64>,
    #[serde(skip)]
    pub tombstones: HashSet<UniqueRowLocation>,
    #[serde(skip)]
    pub encoded_keys: Vec<Vec<u8>>,
    #[serde(skip)]
    pub blocks: Vec<UniqueIndexBlock>,
}

#[derive(Debug, Clone)]
pub struct UniqueIndexBlock {
    pub first_key: Vec<u8>,
    pub start: usize,
    pub len: usize,
}

impl IndexManager {
    pub fn new(segment_mgr: Arc<SegmentManager>) -> Self {
        Self::new_with_data_dir(segment_mgr, PathBuf::from("/tmp/taibai_default"))
    }

    pub fn new_with_data_dir(segment_mgr: Arc<SegmentManager>, data_dir: PathBuf) -> Self {
        Self {
            indexes: RwLock::new(HashMap::new()),
            unique_indexes: RwLock::new(HashMap::new()),
            primary_key_indexes: RwLock::new(HashMap::new()),
            segment_mgr,
            data_dir,
        }
    }

    pub fn get_or_create_index(&self, db: &str, table: &str) -> Arc<SparseIndexImpl> {
        let indexes = self.indexes.read().unwrap();

        if let Some(db_indexes) = indexes.get(db) {
            if let Some(idx) = db_indexes.get(table) {
                return idx.clone();
            }
        }

        drop(indexes);

        let new_index = Arc::new(SparseIndexImpl::new());
        let mut indexes = self.indexes.write().unwrap();

        indexes
            .entry(db.to_string())
            .or_default()
            .insert(table.to_string(), new_index.clone());

        new_index
    }

    pub fn rebuild_index_from_segments(
        &self,
        db: &str,
        table: &str,
        segments: &[String],
    ) -> Result<()> {
        let index = self.get_or_create_index(db, table);
        index.clear();

        let mut current_row_id: i64 = 0;
        let mut block_data = Vec::new();

        for seg_path in segments.iter() {
            let reader = self.segment_mgr.open_reader(Path::new(seg_path))?;

            let num_row_groups = reader.num_row_groups();
            for rg_idx in 0..num_row_groups {
                let rg_row_count = reader.row_group_rows(rg_idx) as i64;

                if rg_row_count == 0 {
                    continue;
                }

                block_data.push(crate::storage::index::sparse_index::SparseBlockData {
                    block_id: block_data.len(),
                    zone_map: None,
                    bloom_filter: None,
                    start_row_id: current_row_id,
                    row_count: rg_row_count,
                });

                current_row_id += rg_row_count;
            }
        }

        index.rebuild_from_blocks(block_data);
        Ok(())
    }

    pub fn search(&self, db: &str, table: &str, value: &ScalarValue) -> Option<Vec<i64>> {
        let indexes = self.indexes.read().unwrap();

        let idx = indexes.get(db).and_then(|db_idx| db_idx.get(table))?;

        Some(idx.search(value))
    }

    pub fn range_search(
        &self,
        db: &str,
        table: &str,
        min: &ScalarValue,
        max: &ScalarValue,
    ) -> Option<Vec<i64>> {
        let indexes = self.indexes.read().unwrap();

        let idx = indexes.get(db).and_then(|db_idx| db_idx.get(table))?;

        Some(idx.range_search(min, max))
    }

    pub fn get_index(&self, db: &str, table: &str) -> Option<Arc<SparseIndexImpl>> {
        let indexes = self.indexes.read().unwrap();
        indexes
            .get(db)
            .and_then(|db_idx| db_idx.get(table))
            .cloned()
    }

    pub fn update_index(&self, db: &str, table: &str, key: &ScalarValue, row_id: i64) {
        if let Some(idx) = self.get_index(db, table) {
            idx.insert(key, row_id);
        }
    }

    pub fn mark_index_stale(&self, db: &str, table: &str) {
        if let Some(idx) = self.get_index(db, table) {
            idx.mark_stale();
        }
    }

    pub fn is_index_stale(&self, db: &str, table: &str) -> bool {
        if let Some(idx) = self.get_index(db, table) {
            idx.is_stale()
        } else {
            false
        }
    }

    pub fn clear_index(&self, db: &str, table: &str) {
        if let Some(idx) = self.get_index(db, table) {
            idx.clear();
        }
    }

    pub fn remove_index(&self, db: &str, table: &str) {
        let mut indexes = self.indexes.write().unwrap();
        if let Some(db_indexes) = indexes.get_mut(db) {
            db_indexes.remove(table);
        }
    }

    pub fn rename_index(&self, db: &str, old_name: &str, new_name: &str) {
        let mut indexes = self.indexes.write().unwrap();
        if let Some(db_indexes) = indexes.get_mut(db) {
            if let Some(idx) = db_indexes.remove(old_name) {
                db_indexes.insert(new_name.to_string(), idx);
            }
        }
    }

    pub fn remove_database_indexes(&self, db: &str) {
        let mut indexes = self.indexes.write().unwrap();
        indexes.remove(db);
    }

    pub fn index_size(&self, db: &str, table: &str) -> i64 {
        if let Some(idx) = self.get_index(db, table) {
            idx.size()
        } else {
            0
        }
    }

    pub fn index_block_count(&self, db: &str, table: &str) -> usize {
        if let Some(idx) = self.get_index(db, table) {
            idx.block_count()
        } else {
            0
        }
    }

    pub fn persist_unique_index_entries(
        &self,
        db: &str,
        table: &str,
        index_name: &str,
        columns: &[String],
        entries: Vec<UniqueIndexEntry>,
        flush_epoch: Option<u64>,
    ) -> Result<()> {
        let dir = self.unique_index_dir(db, table);
        std::fs::create_dir_all(&dir)?;
        let mut data = UniqueIndexData {
            db: db.to_string(),
            table: table.to_string(),
            index_name: index_name.to_string(),
            columns: columns.to_vec(),
            entries,
            flush_epoch,
            tombstones: HashSet::new(),
            encoded_keys: Vec::new(),
            blocks: Vec::new(),
        };
        prepare_unique_index_data(&mut data)?;
        let base = sanitize_file_component(index_name);
        let data_path = dir.join(format!("{}.keys.tbi", base));
        let data_tmp = data_path.with_extension("keys.tbi.tmp");
        let invalid_path = dir.join(format!("{}.invalid", base));
        let data_bytes = encode_unique_index_data(&data)?;
        std::fs::write(&data_tmp, data_bytes)?;
        std::fs::rename(data_tmp, data_path)?;
        let delta_path = dir.join(format!("{}.delta.tbi", base));
        let _ = std::fs::remove_file(delta_path);
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
            key_count: state.base.entries.len() + delta.entries.len(),
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
        let state = Arc::new(UniqueIndexState::new(data, delta));
        let current = self
            .unique_indexes
            .write()
            .unwrap()
            .entry(cache_key)
            .or_insert_with(|| Arc::clone(&state))
            .clone();
        Ok(Some(current))
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
        Ok(Some(Arc::new(materialize_unique_index_state(&state)?)))
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
        let Some(state) = self.load_unique_index_state(db, table, index_name)? else {
            return Ok(None);
        };
        let encoded = encode_unique_key(key)?;
        Ok(Some(unique_index_state_contains_key(
            &self.segment_mgr,
            &state,
            &encoded,
        )?))
    }

    pub fn unique_key_locations(
        &self,
        db: &str,
        table: &str,
        index_name: &str,
        keys: &[Vec<ScalarValue>],
    ) -> Result<Option<Vec<UniqueRowLocation>>> {
        let Some(state) = self.load_unique_index_state(db, table, index_name)? else {
            return Ok(None);
        };
        // A memtable flush after this snapshot was built may have moved rows into
        // segments without index entries; force a rebuild rather than miss them.
        if state.flush_epoch != Some(self.segment_mgr.memtable_flush_epoch(db, table)) {
            return Ok(None);
        }
        let encoded_keys = keys
            .iter()
            .map(|key| encode_unique_key(key))
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

    pub fn unique_keys_exist(
        &self,
        db: &str,
        table: &str,
        index_name: &str,
        keys: &[Vec<ScalarValue>],
    ) -> Result<Option<bool>> {
        let Some(state) = self.load_unique_index_state(db, table, index_name)? else {
            return Ok(None);
        };
        // Same staleness guard as unique_key_locations: rows flushed to segments
        // after this snapshot was built may be missing from it entirely.
        if state.flush_epoch != Some(self.segment_mgr.memtable_flush_epoch(db, table)) {
            return Ok(None);
        }
        for key in keys {
            let encoded = encode_unique_key(key)?;
            if unique_index_state_contains_key(&self.segment_mgr, &state, &encoded)? {
                return Ok(Some(true));
            }
        }
        Ok(Some(false))
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
                delta.entries.push(addition);
            }
        }
        if std::fs::metadata(&delta_path)?.len() >= UNIQUE_INDEX_DELTA_COMPACT_THRESHOLD_BYTES {
            let data = materialize_unique_index_state(&state)?;
            let columns = data.columns.clone();
            let live_entries = live_unique_index_entries(&data);
            // Delta compaction preserves the snapshot's epoch: flushing since the
            // snapshot was built keeps it stale so the next probe rebuilds it.
            self.persist_unique_index_entries(
                db,
                table,
                index_name,
                &columns,
                live_entries,
                state.flush_epoch,
            )?;
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
        state.delta.write().unwrap().tombstones.extend(tombstones);
        if std::fs::metadata(&delta_path)?.len() >= UNIQUE_INDEX_DELTA_COMPACT_THRESHOLD_BYTES {
            let data = materialize_unique_index_state(&state)?;
            let columns = data.columns.clone();
            let live_entries = live_unique_index_entries(&data);
            self.persist_unique_index_entries(
                db,
                table,
                index_name,
                &columns,
                live_entries,
                state.flush_epoch,
            )?;
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

const UNIQUE_INDEX_MAGIC_V1: &[u8; 8] = b"TBIUKV1\0";
const UNIQUE_INDEX_MAGIC_V2: &[u8; 8] = b"TBIUKV2\0";
const UNIQUE_INDEX_MAGIC_V3: &[u8; 8] = b"TBIUKV3\0";
const UNIQUE_INDEX_MAGIC_V4: &[u8; 8] = b"TBIUKV4\0";
const UNIQUE_INDEX_DELTA_MAGIC_V1: &[u8; 8] = b"TBIUKD1\0";
const UNIQUE_INDEX_DELTA_MAGIC_V2: &[u8; 8] = b"TBIUKD2\0";
const UNIQUE_INDEX_BLOCK_SIZE: usize = 256;
#[cfg(test)]
const UNIQUE_INDEX_DELTA_COMPACT_THRESHOLD_BYTES: u64 = 4096;
#[cfg(not(test))]
const UNIQUE_INDEX_DELTA_COMPACT_THRESHOLD_BYTES: u64 = 64 * 1024 * 1024;

fn unique_index_key(db: &str, table: &str, index_name: &str) -> String {
    format!("{}\0{}\0{}", db, table, index_name.to_ascii_lowercase())
}

fn materialize_unique_index_state(state: &UniqueIndexState) -> Result<UniqueIndexData> {
    let delta = state.delta.read().unwrap();
    let mut data = (*state.base).clone();
    let additions = delta.entries.clone();
    data.tombstones.extend(delta.tombstones.iter().cloned());
    drop(delta);
    merge_prepared_unique_index_entries(&mut data, additions)?;
    Ok(data)
}

fn unique_index_state_contains_key(
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

fn merge_prepared_unique_index_entries(
    data: &mut UniqueIndexData,
    mut additions: Vec<PreparedUniqueIndexEntry>,
) -> Result<()> {
    additions.sort_by(|left, right| left.encoded_key.cmp(&right.encoded_key));
    let existing_entries = std::mem::take(&mut data.entries);
    let existing_keys = std::mem::take(&mut data.encoded_keys);
    let mut existing = existing_keys.into_iter().zip(existing_entries).peekable();
    let mut additions = additions.into_iter().peekable();
    let capacity = existing.size_hint().0 + additions.size_hint().0;
    let mut merged_entries = Vec::with_capacity(capacity);
    let mut merged_keys = Vec::with_capacity(capacity);
    while existing.peek().is_some() && additions.peek().is_some() {
        let take_existing = existing.peek().unwrap().0 <= additions.peek().unwrap().encoded_key;
        if take_existing {
            let (key, entry) = existing.next().unwrap();
            merged_keys.push(key);
            merged_entries.push(entry);
        } else {
            let addition = additions.next().unwrap();
            merged_keys.push(addition.encoded_key);
            merged_entries.push(addition.entry);
        }
    }
    for (key, entry) in existing {
        merged_keys.push(key);
        merged_entries.push(entry);
    }
    for addition in additions {
        merged_keys.push(addition.encoded_key);
        merged_entries.push(addition.entry);
    }
    data.entries = merged_entries;
    data.encoded_keys = merged_keys;
    data.blocks = build_unique_index_blocks(&data.encoded_keys);
    Ok(())
}

fn prepare_unique_index_data(data: &mut UniqueIndexData) -> Result<()> {
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

fn merge_unique_index_entries(
    data: &mut UniqueIndexData,
    entries: Vec<UniqueIndexEntry>,
) -> Result<()> {
    let additions = entries
        .into_iter()
        .map(|entry| {
            Ok(PreparedUniqueIndexEntry {
                encoded_key: encode_unique_key(&entry.key)?,
                entry,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    merge_prepared_unique_index_entries(data, additions)
}

fn build_unique_index_blocks(encoded_keys: &[Vec<u8>]) -> Vec<UniqueIndexBlock> {
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

fn unique_index_data_contains_key(
    segment_mgr: &SegmentManager,
    data: &UniqueIndexData,
    key: &[ScalarValue],
) -> Result<bool> {
    let needle = encode_unique_key(key)?;
    if data.encoded_keys.is_empty() {
        return Ok(false);
    }
    let Ok(mut idx) = data.encoded_keys.binary_search(&needle) else {
        return Ok(false);
    };
    while idx > 0 && data.encoded_keys[idx - 1] == needle {
        idx -= 1;
    }
    while idx < data.encoded_keys.len() && data.encoded_keys[idx] == needle {
        if !unique_index_tombstones_contains(&data.tombstones, &data.entries[idx].location)
            && unique_index_entry_location_is_live(segment_mgr, &data.entries[idx].location)?
        {
            return Ok(true);
        }
        idx += 1;
    }
    Ok(false)
}

fn live_unique_index_entries(data: &UniqueIndexData) -> Vec<UniqueIndexEntry> {
    data.entries
        .iter()
        .filter(|entry| !data.tombstones.contains(&entry.location))
        .cloned()
        .collect()
}

fn unique_index_tombstones_contains(
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

fn normalize_unique_segment_path(path: &str) -> String {
    path.replace('\\', "/")
}

fn unique_segment_file_name(path: &str) -> Option<String> {
    Path::new(path)
        .file_name()
        .map(|name| name.to_string_lossy().to_string())
}

fn unique_index_entry_location_is_live(
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

fn encode_unique_key(key: &[ScalarValue]) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    put_u32(&mut out, key.len() as u32);
    for value in key {
        encode_scalar_value(&mut out, value)?;
    }
    Ok(out)
}

fn encode_unique_index_data(data: &UniqueIndexData) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(UNIQUE_INDEX_MAGIC_V4);
    put_string(&mut out, &data.db);
    put_string(&mut out, &data.table);
    put_string(&mut out, &data.index_name);
    put_u32(&mut out, data.columns.len() as u32);
    for column in &data.columns {
        put_string(&mut out, column);
    }
    put_u64(&mut out, data.entries.len() as u64);
    put_u32(&mut out, UNIQUE_INDEX_BLOCK_SIZE as u32);
    put_u32(&mut out, data.blocks.len() as u32);
    for block in &data.blocks {
        put_u64(&mut out, block.start as u64);
        put_u32(&mut out, block.len as u32);
        put_bytes(&mut out, &block.first_key);
    }
    let mut path_ids: HashMap<&str, u32> = HashMap::new();
    let mut paths: Vec<&str> = Vec::new();
    for entry in &data.entries {
        let path = entry.location.segment_path.as_str();
        if !path_ids.contains_key(path) {
            path_ids.insert(path, paths.len() as u32);
            paths.push(path);
        }
    }
    put_u32(&mut out, paths.len() as u32);
    for path in &paths {
        put_string(&mut out, path);
    }
    for (idx, entry) in data.entries.iter().enumerate() {
        let encoded_key = &data.encoded_keys[idx];
        let prefix_len = data
            .blocks
            .get(idx / UNIQUE_INDEX_BLOCK_SIZE)
            .map(|block| common_prefix_len(&block.first_key, encoded_key))
            .unwrap_or(0);
        put_varint(&mut out, prefix_len as u64);
        put_varint(&mut out, (encoded_key.len() - prefix_len) as u64);
        out.extend_from_slice(&encoded_key[prefix_len..]);
        put_varint(
            &mut out,
            path_ids[entry.location.segment_path.as_str()] as u64,
        );
        put_varint(&mut out, entry.location.row_id);
    }
    out.push(if data.flush_epoch.is_some() { 1 } else { 0 });
    put_u64(&mut out, data.flush_epoch.unwrap_or(0));
    Ok(out)
}

fn append_prepared_unique_index_delta(
    path: &Path,
    entries: &[PreparedUniqueIndexEntry],
) -> Result<()> {
    let is_new = !path.exists() || std::fs::metadata(path)?.len() == 0;
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    if is_new {
        file.write_all(UNIQUE_INDEX_DELTA_MAGIC_V2)?;
    }
    for addition in entries {
        let entry = &addition.entry;
        let mut record = Vec::new();
        record.push(1);
        put_bytes(&mut record, &addition.encoded_key);
        put_u32(&mut record, entry.key.len() as u32);
        for value in &entry.key {
            encode_scalar_value(&mut record, value)?;
        }
        put_string(&mut record, &entry.location.segment_path);
        put_u64(&mut record, entry.location.row_id);
        write_u32(&mut file, record.len() as u32)?;
        file.write_all(&record)?;
    }
    file.sync_all()?;
    Ok(())
}

fn append_unique_index_delta(path: &Path, entries: &[UniqueIndexEntry]) -> Result<()> {
    append_unique_index_delta_records(path, entries, &[])
}

fn append_unique_index_tombstone_delta(
    path: &Path,
    tombstones: &[UniqueRowLocation],
) -> Result<()> {
    append_unique_index_delta_records(path, &[], tombstones)
}

fn append_unique_index_delta_records(
    path: &Path,
    entries: &[UniqueIndexEntry],
    tombstones: &[UniqueRowLocation],
) -> Result<()> {
    let is_new = !path.exists() || std::fs::metadata(path)?.len() == 0;
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    if is_new {
        file.write_all(UNIQUE_INDEX_DELTA_MAGIC_V2)?;
    }
    for entry in entries {
        let mut record = Vec::new();
        record.push(1);
        put_bytes(&mut record, &encode_unique_key(&entry.key)?);
        put_u32(&mut record, entry.key.len() as u32);
        for value in &entry.key {
            encode_scalar_value(&mut record, value)?;
        }
        put_string(&mut record, &entry.location.segment_path);
        put_u64(&mut record, entry.location.row_id);
        write_u32(&mut file, record.len() as u32)?;
        file.write_all(&record)?;
    }
    for location in tombstones {
        let mut record = Vec::new();
        record.push(2);
        put_string(&mut record, &location.segment_path);
        put_u64(&mut record, location.row_id);
        write_u32(&mut file, record.len() as u32)?;
        file.write_all(&record)?;
    }
    file.sync_all()?;
    Ok(())
}

#[derive(Default)]
struct UniqueIndexDelta {
    entries: Vec<PreparedUniqueIndexEntry>,
    tombstones: Vec<UniqueRowLocation>,
}

impl UniqueIndexDelta {
    fn into_state(self) -> Result<UniqueIndexDeltaState> {
        let mut state = UniqueIndexDeltaState {
            entries: self.entries,
            tombstones: self.tombstones.into_iter().collect(),
            key_locations: HashMap::new(),
        };
        for addition in &state.entries {
            state
                .key_locations
                .entry(addition.encoded_key.clone())
                .or_default()
                .push(addition.entry.location.clone());
        }
        Ok(state)
    }
}

fn decode_unique_index_delta(bytes: &[u8]) -> Result<UniqueIndexDelta> {
    if bytes.is_empty() {
        return Ok(UniqueIndexDelta::default());
    }
    let mut cursor = Cursor::new(bytes);
    let mut magic = [0u8; 8];
    cursor.read_exact(&mut magic)?;
    if &magic == UNIQUE_INDEX_DELTA_MAGIC_V1 {
        return decode_unique_index_delta_v1(bytes, cursor);
    }
    if &magic != UNIQUE_INDEX_DELTA_MAGIC_V2 {
        return Err(TaibaiError::Storage(
            "invalid unique index delta file".to_string(),
        ));
    }
    let mut delta = UniqueIndexDelta::default();
    while (cursor.position() as usize) < bytes.len() {
        let len = read_u32(&mut cursor)? as usize;
        let start = cursor.position() as usize;
        let end = start + len;
        if end > bytes.len() {
            return Err(TaibaiError::Storage(
                "truncated unique index delta record".to_string(),
            ));
        }
        let record = &bytes[start..end];
        let mut record_cursor = Cursor::new(record);
        let record_type = read_u8(&mut record_cursor)?;
        match record_type {
            1 => {
                let encoded_key = read_bytes(&mut record_cursor)?;
                let entry = read_unique_index_entry(&mut record_cursor)?;
                delta
                    .entries
                    .push(PreparedUniqueIndexEntry { encoded_key, entry });
            }
            2 => {
                let segment_path = read_string(&mut record_cursor)?;
                let row_id = read_u64(&mut record_cursor)?;
                delta.tombstones.push(UniqueRowLocation {
                    segment_path,
                    row_id,
                });
            }
            other => {
                return Err(TaibaiError::Storage(format!(
                    "invalid unique index delta record type {}",
                    other
                )));
            }
        }
        cursor.set_position(end as u64);
    }
    Ok(delta)
}

fn decode_unique_index_delta_v1(
    bytes: &[u8],
    mut cursor: Cursor<&[u8]>,
) -> Result<UniqueIndexDelta> {
    let mut delta = UniqueIndexDelta::default();
    while (cursor.position() as usize) < bytes.len() {
        let len = read_u32(&mut cursor)? as usize;
        let start = cursor.position() as usize;
        let end = start + len;
        if end > bytes.len() {
            return Err(TaibaiError::Storage(
                "truncated unique index delta record".to_string(),
            ));
        }
        let record = &bytes[start..end];
        let mut record_cursor = Cursor::new(record);
        let encoded_key = read_bytes(&mut record_cursor)?;
        let entry = read_unique_index_entry(&mut record_cursor)?;
        delta
            .entries
            .push(PreparedUniqueIndexEntry { encoded_key, entry });
        cursor.set_position(end as u64);
    }
    Ok(delta)
}

fn write_u32<W: Write>(writer: &mut W, value: u32) -> Result<()> {
    writer.write_all(&value.to_le_bytes())?;
    Ok(())
}
fn decode_unique_index_data(bytes: &[u8]) -> Result<Option<UniqueIndexData>> {
    let mut cursor = Cursor::new(bytes);
    let mut magic = [0u8; 8];
    cursor.read_exact(&mut magic)?;
    if &magic == UNIQUE_INDEX_MAGIC_V1 {
        return decode_unique_index_data_v1(&mut cursor);
    }
    if &magic == UNIQUE_INDEX_MAGIC_V2 {
        return decode_unique_index_data_v2(&mut cursor);
    }
    if &magic == UNIQUE_INDEX_MAGIC_V3 {
        return decode_unique_index_data_v3(&mut cursor);
    }
    if &magic == UNIQUE_INDEX_MAGIC_V4 {
        return decode_unique_index_data_v4(&mut cursor);
    }
    Err(TaibaiError::Storage(
        "invalid unique index binary file".to_string(),
    ))
}

fn decode_unique_index_data_v1(cursor: &mut Cursor<&[u8]>) -> Result<Option<UniqueIndexData>> {
    let db = read_string(cursor)?;
    let table = read_string(cursor)?;
    let index_name = read_string(cursor)?;
    let column_count = read_u32(cursor)? as usize;
    let mut columns = Vec::with_capacity(column_count);
    for _ in 0..column_count {
        columns.push(read_string(cursor)?);
    }
    let entry_count = read_u64(cursor)? as usize;
    let mut entries = Vec::with_capacity(entry_count);
    for _ in 0..entry_count {
        entries.push(read_unique_index_entry(cursor)?);
    }
    Ok(Some(UniqueIndexData {
        db,
        table,
        index_name,
        columns,
        entries,
        flush_epoch: None,
        tombstones: HashSet::new(),
        encoded_keys: Vec::new(),
        blocks: Vec::new(),
    }))
}

fn decode_unique_index_data_v2(cursor: &mut Cursor<&[u8]>) -> Result<Option<UniqueIndexData>> {
    let db = read_string(cursor)?;
    let table = read_string(cursor)?;
    let index_name = read_string(cursor)?;
    let column_count = read_u32(cursor)? as usize;
    let mut columns = Vec::with_capacity(column_count);
    for _ in 0..column_count {
        columns.push(read_string(cursor)?);
    }
    let entry_count = read_u64(cursor)? as usize;
    let _block_size = read_u32(cursor)? as usize;
    let block_count = read_u32(cursor)? as usize;
    let mut blocks = Vec::with_capacity(block_count);
    for _ in 0..block_count {
        blocks.push(UniqueIndexBlock {
            start: read_u64(cursor)? as usize,
            len: read_u32(cursor)? as usize,
            first_key: read_bytes(cursor)?,
        });
    }
    let mut entries = Vec::with_capacity(entry_count);
    let mut encoded_keys = Vec::with_capacity(entry_count);
    for _ in 0..entry_count {
        encoded_keys.push(read_bytes(cursor)?);
        entries.push(read_unique_index_entry(cursor)?);
    }
    Ok(Some(UniqueIndexData {
        db,
        table,
        index_name,
        columns,
        entries,
        flush_epoch: None,
        tombstones: HashSet::new(),
        encoded_keys,
        blocks,
    }))
}

fn read_unique_index_entry(cursor: &mut Cursor<&[u8]>) -> Result<UniqueIndexEntry> {
    let key_len = read_u32(cursor)? as usize;
    let mut key = Vec::with_capacity(key_len);
    for _ in 0..key_len {
        key.push(decode_scalar_value(cursor)?);
    }
    let segment_path = read_string(cursor)?;
    let row_id = read_u64(cursor)?;
    Ok(UniqueIndexEntry {
        key,
        location: UniqueRowLocation {
            segment_path,
            row_id,
        },
    })
}

fn decode_unique_index_data_v3(cursor: &mut Cursor<&[u8]>) -> Result<Option<UniqueIndexData>> {
    decode_unique_index_data_blocks(cursor)
}

fn decode_unique_index_data_v4(cursor: &mut Cursor<&[u8]>) -> Result<Option<UniqueIndexData>> {
    let Some(mut data) = decode_unique_index_data_blocks(cursor)? else {
        return Ok(None);
    };
    let mut flag = [0u8; 1];
    cursor.read_exact(&mut flag)?;
    let epoch = read_u64(cursor)?;
    data.flush_epoch = (flag[0] == 1).then_some(epoch);
    Ok(Some(data))
}

fn decode_unique_index_data_blocks(cursor: &mut Cursor<&[u8]>) -> Result<Option<UniqueIndexData>> {
    let db = read_string(cursor)?;
    let table = read_string(cursor)?;
    let index_name = read_string(cursor)?;
    let column_count = read_u32(cursor)? as usize;
    let mut columns = Vec::with_capacity(column_count);
    for _ in 0..column_count {
        columns.push(read_string(cursor)?);
    }
    let entry_count = read_u64(cursor)? as usize;
    let _block_size = read_u32(cursor)? as usize;
    let block_count = read_u32(cursor)? as usize;
    let mut blocks = Vec::with_capacity(block_count);
    for _ in 0..block_count {
        blocks.push(UniqueIndexBlock {
            start: read_u64(cursor)? as usize,
            len: read_u32(cursor)? as usize,
            first_key: read_bytes(cursor)?,
        });
    }
    let path_count = read_u32(cursor)? as usize;
    let mut paths = Vec::with_capacity(path_count);
    for _ in 0..path_count {
        paths.push(read_string(cursor)?);
    }
    let mut entries = Vec::with_capacity(entry_count);
    let mut encoded_keys = Vec::with_capacity(entry_count);
    for block in &blocks {
        for _ in 0..block.len {
            let prefix_len = read_varint(cursor)? as usize;
            let suffix_len = read_varint(cursor)? as usize;
            if prefix_len > block.first_key.len() {
                return Err(TaibaiError::Storage(
                    "corrupt unique index key prefix".to_string(),
                ));
            }
            let mut encoded_key = Vec::with_capacity(prefix_len + suffix_len);
            encoded_key.extend_from_slice(&block.first_key[..prefix_len]);
            let mut suffix = vec![0u8; suffix_len];
            cursor.read_exact(&mut suffix)?;
            encoded_key.extend_from_slice(&suffix);
            let path_idx = read_varint(cursor)? as usize;
            let row_id = read_varint(cursor)?;
            let Some(segment_path) = paths.get(path_idx) else {
                return Err(TaibaiError::Storage(
                    "corrupt unique index path reference".to_string(),
                ));
            };
            let key = decode_encoded_unique_key(&encoded_key)?;
            encoded_keys.push(encoded_key);
            entries.push(UniqueIndexEntry {
                key,
                location: UniqueRowLocation {
                    segment_path: segment_path.clone(),
                    row_id,
                },
            });
        }
    }
    if entries.len() != entry_count {
        return Err(TaibaiError::Storage(
            "corrupt unique index entry count".to_string(),
        ));
    }
    Ok(Some(UniqueIndexData {
        db,
        table,
        index_name,
        columns,
        entries,
        flush_epoch: None,
        tombstones: HashSet::new(),
        encoded_keys,
        blocks,
    }))
}

fn decode_encoded_unique_key(encoded: &[u8]) -> Result<Vec<ScalarValue>> {
    let mut cursor = Cursor::new(encoded);
    let count = read_u32(&mut cursor)? as usize;
    let mut key = Vec::with_capacity(count);
    for _ in 0..count {
        key.push(decode_scalar_value(&mut cursor)?);
    }
    Ok(key)
}

fn common_prefix_len(left: &[u8], right: &[u8]) -> usize {
    left.iter()
        .zip(right.iter())
        .take_while(|(left, right)| left == right)
        .count()
}

fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_varint(out: &mut Vec<u8>, mut value: u64) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

fn put_i64(out: &mut Vec<u8>, value: i64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_string(out: &mut Vec<u8>, value: &str) {
    let bytes = value.as_bytes();
    put_u32(out, bytes.len() as u32);
    out.extend_from_slice(bytes);
}

fn put_bytes(out: &mut Vec<u8>, value: &[u8]) {
    put_u32(out, value.len() as u32);
    out.extend_from_slice(value);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(value: i64, row_id: u64) -> UniqueIndexEntry {
        UniqueIndexEntry {
            key: vec![ScalarValue::Int64(Some(value))],
            location: UniqueRowLocation {
                segment_path: String::new(),
                row_id,
            },
        }
    }

    #[test]
    fn incremental_unique_index_merge_keeps_entries_sorted_and_aligned() -> Result<()> {
        let mut data = UniqueIndexData {
            db: "db".to_string(),
            table: "ticks".to_string(),
            index_name: "uk_value".to_string(),
            columns: vec!["value".to_string()],
            entries: vec![entry(2, 2), entry(4, 4)],
            flush_epoch: None,
            tombstones: HashSet::new(),
            encoded_keys: Vec::new(),
            blocks: Vec::new(),
        };
        prepare_unique_index_data(&mut data)?;

        merge_unique_index_entries(
            &mut data,
            vec![entry(5, 5), entry(1, 1), entry(4, 40), entry(3, 3)],
        )?;

        assert_eq!(data.entries.len(), 6);
        assert!(data.encoded_keys.windows(2).all(|keys| keys[0] <= keys[1]));
        for (encoded, entry) in data.encoded_keys.iter().zip(&data.entries) {
            assert_eq!(*encoded, encode_unique_key(&entry.key)?);
        }
        assert_eq!(
            data.entries
                .iter()
                .filter(|entry| entry.key == vec![ScalarValue::Int64(Some(4))])
                .count(),
            2
        );
        assert_eq!(data.blocks.len(), 1);
        Ok(())
    }

    #[test]
    fn direct_location_lookup_skips_transient_locations() -> Result<()> {
        let dir = tempfile::tempdir().unwrap();
        let manager = IndexManager::new_with_data_dir(
            Arc::new(SegmentManager::new(dir.path())?),
            dir.path().to_path_buf(),
        );
        manager.persist_unique_index_entries(
            "db",
            "ticks",
            "uk_value",
            &["value".to_string()],
            vec![entry(1, 0)],
            Some(0),
        )?;
        // Memtable-resident pseudo locations (empty segment_path) are skipped:
        // REPLACE deletes conflicting memtable rows in place, so a location
        // probe only needs segment-side results.
        assert_eq!(
            manager.unique_key_locations(
                "db",
                "ticks",
                "uk_value",
                &[vec![ScalarValue::Int64(Some(1))]],
            )?,
            Some(Vec::new())
        );
        Ok(())
    }

    #[test]
    fn flush_epoch_mismatch_forces_location_probe_rebuild() -> Result<()> {
        let dir = tempfile::tempdir().unwrap();
        let segment_mgr = Arc::new(SegmentManager::new(dir.path())?);
        let manager =
            IndexManager::new_with_data_dir(Arc::clone(&segment_mgr), dir.path().to_path_buf());
        manager.persist_unique_index_entries(
            "db",
            "ticks",
            "uk_value",
            &["value".to_string()],
            vec![entry(1, 0)],
            Some(segment_mgr.memtable_flush_epoch("db", "ticks")),
        )?;
        // A memtable flush after the snapshot was built may have moved rows
        // into segments without index entries; probing must report stale.
        segment_mgr.note_memtable_flush("db", "ticks");
        assert_eq!(
            manager.unique_key_locations(
                "db",
                "ticks",
                "uk_value",
                &[vec![ScalarValue::Int64(Some(1))]],
            )?,
            None
        );
        // The epoch survives a restart through the on-disk marker.
        let segment_mgr = Arc::new(SegmentManager::new(dir.path())?);
        let manager =
            IndexManager::new_with_data_dir(Arc::clone(&segment_mgr), dir.path().to_path_buf());
        assert_eq!(segment_mgr.memtable_flush_epoch("db", "ticks"), 1);
        assert_eq!(
            manager.unique_key_locations(
                "db",
                "ticks",
                "uk_value",
                &[vec![ScalarValue::Int64(Some(1))]],
            )?,
            None
        );
        Ok(())
    }

    #[test]
    fn incremental_unique_index_merge_handles_many_batches() -> Result<()> {
        let mut data = UniqueIndexData {
            db: "db".to_string(),
            table: "ticks".to_string(),
            index_name: "uk_value".to_string(),
            columns: vec!["value".to_string()],
            entries: Vec::new(),
            flush_epoch: None,
            tombstones: HashSet::new(),
            encoded_keys: Vec::new(),
            blocks: Vec::new(),
        };

        for batch in 0..100 {
            let start = batch * 100;
            let entries = (0..100)
                .rev()
                .map(|offset| entry((start + offset) as i64, (start + offset) as u64))
                .collect();
            merge_unique_index_entries(&mut data, entries)?;
        }

        assert_eq!(data.entries.len(), 10_000);
        assert!(data.encoded_keys.windows(2).all(|keys| keys[0] <= keys[1]));
        assert_eq!(
            data.blocks.len(),
            data.entries.len().div_ceil(UNIQUE_INDEX_BLOCK_SIZE)
        );
        Ok(())
    }

    fn stock_entry(id: &str, date: i32, time: i32, row_id: u64, path: &str) -> UniqueIndexEntry {
        UniqueIndexEntry {
            key: vec![
                ScalarValue::VarChar(Some(id.to_string())),
                ScalarValue::Int32(Some(date)),
                ScalarValue::Int32(Some(time)),
            ],
            location: UniqueRowLocation {
                segment_path: path.to_string(),
                row_id,
            },
        }
    }

    fn stock_minute_index_data(entry_count: u32, path: &str) -> UniqueIndexData {
        let entries = (0..entry_count)
            .map(|idx| {
                stock_entry(
                    &format!("{:06}.SZ", idx / 240),
                    20260701,
                    93000 + (idx % 240) as i32,
                    idx as u64,
                    path,
                )
            })
            .collect();
        UniqueIndexData {
            db: "quant_base".to_string(),
            table: "t_stock_minute".to_string(),
            index_name: PRIMARY_LOCATION_INDEX_NAME.to_string(),
            columns: vec![
                "security_id".to_string(),
                "deal_date".to_string(),
                "deal_time".to_string(),
            ],
            entries,
            flush_epoch: None,
            tombstones: HashSet::new(),
            encoded_keys: Vec::new(),
            blocks: Vec::new(),
        }
    }

    fn encode_legacy_v2_unique_index_data(data: &UniqueIndexData) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        out.extend_from_slice(UNIQUE_INDEX_MAGIC_V2);
        put_string(&mut out, &data.db);
        put_string(&mut out, &data.table);
        put_string(&mut out, &data.index_name);
        put_u32(&mut out, data.columns.len() as u32);
        for column in &data.columns {
            put_string(&mut out, column);
        }
        put_u64(&mut out, data.entries.len() as u64);
        put_u32(&mut out, UNIQUE_INDEX_BLOCK_SIZE as u32);
        put_u32(&mut out, data.blocks.len() as u32);
        for block in &data.blocks {
            put_u64(&mut out, block.start as u64);
            put_u32(&mut out, block.len as u32);
            put_bytes(&mut out, &block.first_key);
        }
        for (idx, entry) in data.entries.iter().enumerate() {
            put_bytes(&mut out, &data.encoded_keys[idx]);
            put_u32(&mut out, entry.key.len() as u32);
            for value in &entry.key {
                encode_scalar_value(&mut out, value)?;
            }
            put_string(&mut out, &entry.location.segment_path);
            put_u64(&mut out, entry.location.row_id);
        }
        Ok(out)
    }

    #[test]
    fn unique_index_data_v4_roundtrip_preserves_entries() -> Result<()> {
        let path = "/data/monitor-data/taibai_data/quant_base/t_stock_minute/p2026_0007.dat";
        let mut data = stock_minute_index_data(3000, path);
        data.flush_epoch = Some(7);
        prepare_unique_index_data(&mut data)?;
        let bytes = encode_unique_index_data(&data)?;
        assert_eq!(&bytes[..8], UNIQUE_INDEX_MAGIC_V4);
        let decoded = decode_unique_index_data(&bytes)?.unwrap();
        assert_eq!(decoded.db, data.db);
        assert_eq!(decoded.table, data.table);
        assert_eq!(decoded.index_name, data.index_name);
        assert_eq!(decoded.columns, data.columns);
        assert_eq!(decoded.flush_epoch, Some(7));
        assert_eq!(decoded.entries.len(), data.entries.len());
        assert_eq!(decoded.encoded_keys, data.encoded_keys);
        for (decoded_entry, expected_entry) in decoded.entries.iter().zip(&data.entries) {
            assert_eq!(decoded_entry.key, expected_entry.key);
            assert_eq!(decoded_entry.location, expected_entry.location);
        }
        assert_eq!(decoded.blocks.len(), data.blocks.len());
        for (decoded_block, expected_block) in decoded.blocks.iter().zip(&data.blocks) {
            assert_eq!(decoded_block.start, expected_block.start);
            assert_eq!(decoded_block.len, expected_block.len);
            assert_eq!(decoded_block.first_key, expected_block.first_key);
        }
        Ok(())
    }

    #[test]
    fn unique_index_data_v3_shrinks_shared_path_keyset() -> Result<()> {
        let path = "/data/monitor-data/taibai_data/quant_base/t_stock_minute/p2026_0007.dat";
        let mut data = stock_minute_index_data(2000, path);
        prepare_unique_index_data(&mut data)?;
        let v2_bytes = encode_legacy_v2_unique_index_data(&data)?;
        let v3_bytes = encode_unique_index_data(&data)?;
        assert!(v3_bytes.len() * 3 < v2_bytes.len());
        assert!(v3_bytes.len() < data.entries.len() * 40);
        Ok(())
    }

    #[test]
    fn unique_index_data_v2_snapshot_still_decodes() -> Result<()> {
        let path = "/data/monitor-data/taibai_data/quant_base/t_stock_minute/p2026_0007.dat";
        let mut data = stock_minute_index_data(600, path);
        prepare_unique_index_data(&mut data)?;
        let v2_bytes = encode_legacy_v2_unique_index_data(&data)?;
        let decoded = decode_unique_index_data(&v2_bytes)?.unwrap();
        assert_eq!(decoded.entries.len(), data.entries.len());
        assert_eq!(decoded.encoded_keys, data.encoded_keys);
        for (decoded_entry, expected_entry) in decoded.entries.iter().zip(&data.entries) {
            assert_eq!(decoded_entry.key, expected_entry.key);
            assert_eq!(decoded_entry.location, expected_entry.location);
        }
        Ok(())
    }
}

fn read_u8(cursor: &mut Cursor<&[u8]>) -> Result<u8> {
    let mut buf = [0u8; 1];
    cursor.read_exact(&mut buf)?;
    Ok(buf[0])
}

fn read_varint(cursor: &mut Cursor<&[u8]>) -> Result<u64> {
    let mut value = 0u64;
    let mut shift = 0u32;
    loop {
        let byte = read_u8(cursor)?;
        value |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
        shift += 7;
        if shift >= 64 {
            return Err(TaibaiError::Storage(
                "invalid varint in unique index".to_string(),
            ));
        }
    }
}

fn read_u32(cursor: &mut Cursor<&[u8]>) -> Result<u32> {
    let mut bytes = [0u8; 4];
    cursor.read_exact(&mut bytes)?;
    Ok(u32::from_le_bytes(bytes))
}

fn read_u64(cursor: &mut Cursor<&[u8]>) -> Result<u64> {
    let mut bytes = [0u8; 8];
    cursor.read_exact(&mut bytes)?;
    Ok(u64::from_le_bytes(bytes))
}

fn read_i64(cursor: &mut Cursor<&[u8]>) -> Result<i64> {
    let mut bytes = [0u8; 8];
    cursor.read_exact(&mut bytes)?;
    Ok(i64::from_le_bytes(bytes))
}

fn read_string(cursor: &mut Cursor<&[u8]>) -> Result<String> {
    let bytes = read_bytes(cursor)?;
    String::from_utf8(bytes)
        .map_err(|err| TaibaiError::Storage(format!("decode unique index string: {}", err)))
}

fn read_bytes(cursor: &mut Cursor<&[u8]>) -> Result<Vec<u8>> {
    let len = read_u32(cursor)? as usize;
    let mut bytes = vec![0u8; len];
    cursor.read_exact(&mut bytes)?;
    Ok(bytes)
}

fn encode_scalar_value(out: &mut Vec<u8>, value: &ScalarValue) -> Result<()> {
    match value {
        ScalarValue::Bool(v) => encode_option_bool(out, 1, *v),
        ScalarValue::Int8(v) => encode_option_i64(out, 2, v.map(|v| v as i64)),
        ScalarValue::Int16(v) => encode_option_i64(out, 3, v.map(|v| v as i64)),
        ScalarValue::Int32(v) => encode_option_i64(out, 4, v.map(|v| v as i64)),
        ScalarValue::Int64(v) => encode_option_i64(out, 5, *v),
        ScalarValue::UInt8(v) => encode_option_u64(out, 6, v.map(|v| v as u64)),
        ScalarValue::UInt16(v) => encode_option_u64(out, 7, v.map(|v| v as u64)),
        ScalarValue::UInt32(v) => encode_option_u64(out, 8, v.map(|v| v as u64)),
        ScalarValue::UInt64(v) => encode_option_u64(out, 9, *v),
        ScalarValue::Float32(v) => encode_option_u64(out, 10, v.map(|v| v.to_bits() as u64)),
        ScalarValue::Float64(v) => encode_option_u64(out, 11, v.map(|v| v.to_bits())),
        ScalarValue::Decimal(v) => encode_option_i64(out, 12, *v),
        ScalarValue::Date(v) => encode_option_i64(out, 13, v.map(|v| v as i64)),
        ScalarValue::Time(v) => encode_option_i64(out, 14, *v),
        ScalarValue::Timestamp(v) => encode_option_i64(out, 15, *v),
        ScalarValue::Interval(v) => encode_option_i64(out, 16, *v),
        ScalarValue::VarChar(v) => encode_option_string(out, 17, v.as_deref()),
        ScalarValue::Blob(v) => encode_option_bytes(out, 18, v.as_deref()),
        ScalarValue::Vector(v) => {
            out.push(19);
            match v {
                Some(values) => {
                    out.push(1);
                    put_u32(out, values.len() as u32);
                    for value in values {
                        out.extend_from_slice(&value.to_bits().to_le_bytes());
                    }
                }
                None => out.push(0),
            }
        }
    }
    Ok(())
}

fn encode_option_bool(out: &mut Vec<u8>, tag: u8, value: Option<bool>) {
    out.push(tag);
    match value {
        Some(value) => {
            out.push(1);
            out.push(u8::from(value));
        }
        None => out.push(0),
    }
}

fn encode_option_i64(out: &mut Vec<u8>, tag: u8, value: Option<i64>) {
    out.push(tag);
    match value {
        Some(value) => {
            out.push(1);
            put_i64(out, value);
        }
        None => out.push(0),
    }
}

fn encode_option_u64(out: &mut Vec<u8>, tag: u8, value: Option<u64>) {
    out.push(tag);
    match value {
        Some(value) => {
            out.push(1);
            put_u64(out, value);
        }
        None => out.push(0),
    }
}

fn encode_option_string(out: &mut Vec<u8>, tag: u8, value: Option<&str>) {
    out.push(tag);
    match value {
        Some(value) => {
            out.push(1);
            put_string(out, value);
        }
        None => out.push(0),
    }
}

fn encode_option_bytes(out: &mut Vec<u8>, tag: u8, value: Option<&[u8]>) {
    out.push(tag);
    match value {
        Some(value) => {
            out.push(1);
            put_bytes(out, value);
        }
        None => out.push(0),
    }
}

fn decode_scalar_value(cursor: &mut Cursor<&[u8]>) -> Result<ScalarValue> {
    let mut tag = [0u8; 1];
    cursor.read_exact(&mut tag)?;
    let mut present = [0u8; 1];
    cursor.read_exact(&mut present)?;
    let present = present[0] != 0;
    Ok(match tag[0] {
        1 => ScalarValue::Bool(if present {
            Some(read_bool(cursor)?)
        } else {
            None
        }),
        2 => ScalarValue::Int8(if present {
            Some(read_i64(cursor)? as i8)
        } else {
            None
        }),
        3 => ScalarValue::Int16(if present {
            Some(read_i64(cursor)? as i16)
        } else {
            None
        }),
        4 => ScalarValue::Int32(if present {
            Some(read_i64(cursor)? as i32)
        } else {
            None
        }),
        5 => ScalarValue::Int64(if present {
            Some(read_i64(cursor)?)
        } else {
            None
        }),
        6 => ScalarValue::UInt8(if present {
            Some(read_u64(cursor)? as u8)
        } else {
            None
        }),
        7 => ScalarValue::UInt16(if present {
            Some(read_u64(cursor)? as u16)
        } else {
            None
        }),
        8 => ScalarValue::UInt32(if present {
            Some(read_u64(cursor)? as u32)
        } else {
            None
        }),
        9 => ScalarValue::UInt64(if present {
            Some(read_u64(cursor)?)
        } else {
            None
        }),
        10 => ScalarValue::Float32(if present {
            Some(f32::from_bits(read_u64(cursor)? as u32))
        } else {
            None
        }),
        11 => ScalarValue::Float64(if present {
            Some(f64::from_bits(read_u64(cursor)?))
        } else {
            None
        }),
        12 => ScalarValue::Decimal(if present {
            Some(read_i64(cursor)?)
        } else {
            None
        }),
        13 => ScalarValue::Date(if present {
            Some(read_i64(cursor)? as i32)
        } else {
            None
        }),
        14 => ScalarValue::Time(if present {
            Some(read_i64(cursor)?)
        } else {
            None
        }),
        15 => ScalarValue::Timestamp(if present {
            Some(read_i64(cursor)?)
        } else {
            None
        }),
        16 => ScalarValue::Interval(if present {
            Some(read_i64(cursor)?)
        } else {
            None
        }),
        17 => ScalarValue::VarChar(if present {
            Some(read_string(cursor)?)
        } else {
            None
        }),
        18 => ScalarValue::Blob(if present {
            Some(read_bytes(cursor)?)
        } else {
            None
        }),
        19 => {
            if present {
                let len = read_u32(cursor)? as usize;
                let mut values = Vec::with_capacity(len);
                for _ in 0..len {
                    let mut bytes = [0u8; 4];
                    cursor.read_exact(&mut bytes)?;
                    values.push(f32::from_bits(u32::from_le_bytes(bytes)));
                }
                ScalarValue::Vector(Some(values))
            } else {
                ScalarValue::Vector(None)
            }
        }
        tag => return Err(TaibaiError::Storage(format!("unknown scalar tag {}", tag))),
    })
}

fn read_bool(cursor: &mut Cursor<&[u8]>) -> Result<bool> {
    let mut value = [0u8; 1];
    cursor.read_exact(&mut value)?;
    Ok(value[0] != 0)
}

fn sanitize_file_component(name: &str) -> String {
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

impl Default for IndexManager {
    fn default() -> Self {
        let segment_mgr = SegmentManager::new(std::path::Path::new("/tmp/taibai_default"))
            .expect("failed to create default SegmentManager");
        Self::new(Arc::new(segment_mgr))
    }
}
