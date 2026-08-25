#![allow(dead_code)]
use std::collections::{HashMap, HashSet};
use std::io::{Cursor, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use serde::{Deserialize, Serialize};

use crate::common::error::{Result, TaibaiError};
use crate::storage::index::sparse_index::SparseIndexImpl;
use crate::storage::segment_manager::SegmentManager;
use crate::types::batch::RecordBatch;
use crate::types::scalar::ScalarValue;

pub const PRIMARY_LOCATION_INDEX_NAME: &str = "__taibai_primary_location";

pub struct IndexManager {
    indexes: RwLock<HashMap<String, HashMap<String, Arc<SparseIndexImpl>>>>,
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
    base: Arc<UniqueIndexData>,
    delta: RwLock<UniqueIndexDeltaState>,
    progress: RwLock<UniqueIndexProgress>,
    mutation_lock: Mutex<()>,
    compaction_in_flight: AtomicBool,
}

#[derive(Default, Clone)]
struct UniqueIndexProgress {
    flush_epoch: Option<u64>,
    segment_watermarks: HashMap<String, u64>,
}

#[derive(Default)]
struct UniqueIndexDeltaState {
    entry_count: u64,
    key_locations: HashMap<Vec<u8>, Vec<UniqueRowLocation>>,
    tombstones: HashSet<UniqueRowLocation>,
    /// Encoded keys whose entries were removed by deletes. Replays in record
    /// order during materialization so a later re-insert of the same key
    /// survives; also blocks placeholder resurrection on reload.
    removed_keys: HashSet<Vec<u8>>,
}

impl UniqueIndexState {
    fn new(base: UniqueIndexData, delta: UniqueIndexDeltaState) -> Self {
        let progress = UniqueIndexProgress {
            flush_epoch: base.flush_epoch,
            segment_watermarks: base.segment_watermarks.clone(),
        };
        Self::with_progress(base, delta, progress)
    }

    fn with_progress(
        base: UniqueIndexData,
        delta: UniqueIndexDeltaState,
        progress: UniqueIndexProgress,
    ) -> Self {
        Self {
            base: Arc::new(base),
            delta: RwLock::new(delta),
            progress: RwLock::new(progress),
            mutation_lock: Mutex::new(()),
            compaction_in_flight: AtomicBool::new(false),
        }
    }

    fn progress_epoch(&self) -> Option<u64> {
        self.progress.read().unwrap().flush_epoch
    }

    fn progress_watermarks(&self) -> HashMap<String, u64> {
        self.progress.read().unwrap().segment_watermarks.clone()
    }

    fn set_progress(&self, flush_epoch: Option<u64>, segment_watermarks: HashMap<String, u64>) {
        let mut progress = self.progress.write().unwrap();
        progress.flush_epoch = flush_epoch;
        progress.segment_watermarks = segment_watermarks;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UniqueIndexData {
    pub db: String,
    pub table: String,
    pub index_name: String,
    pub columns: Vec<String>,
    pub entries: Vec<UniqueIndexEntry>,
    /// Memtable-flush epoch stamped when this snapshot was built. A missing
    /// epoch is treated as stale and causes the first probe to rebuild it.
    #[serde(default)]
    pub flush_epoch: Option<u64>,
    /// Per-segment row-count watermark captured when this snapshot's entries
    /// were collected. Enables bounded incremental refresh after memtable
    /// flushes instead of a full segment rescan.
    #[serde(default)]
    pub segment_watermarks: HashMap<String, u64>,
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
            unique_indexes: Arc::new(RwLock::new(HashMap::new())),
            primary_key_indexes: RwLock::new(HashMap::new()),
            segment_mgr,
            data_dir,
            unique_index_compaction: Mutex::new(UniqueIndexCompactionSchedule::default()),
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
                    let mut batch = reader.read_row_group_with_projection(rg_idx, &indices)?;
                    if !reader.delete_bitmap().is_empty() {
                        batch = crate::storage::segment::delete_bitmap::filter_batch(
                            &batch,
                            rg_start,
                            reader.delete_bitmap(),
                        )?;
                    }
                    for row_idx in 0..batch.num_rows() {
                        let key = projected_unique_key_for_row(&batch, row_idx);
                        let Some(key) = key else { continue };
                        additions.push(UniqueIndexEntry {
                            key,
                            location: UniqueRowLocation {
                                segment_path: seg_path.to_string_lossy().to_string(),
                                row_id: rg_start + row_idx as u64,
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

    pub fn unique_key_locations(
        &self,
        db: &str,
        table: &str,
        index_name: &str,
        keys: &[Vec<ScalarValue>],
    ) -> Result<Option<Vec<UniqueRowLocation>>> {
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
            .iter()
            .map(|key| encode_unique_key(key))
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

    pub fn pause_unique_index_compaction(&self) {
        let mut schedule = self.unique_index_compaction.lock().unwrap();
        schedule.pause_count = schedule.pause_count.saturating_add(1);
    }

    pub fn resume_unique_index_compaction(&self) {
        let pending = {
            let mut schedule = self.unique_index_compaction.lock().unwrap();
            schedule.pause_count = schedule.pause_count.saturating_sub(1);
            if schedule.pause_count > 0 {
                return;
            }
            std::mem::take(&mut schedule.pending)
        };
        for (db, table, index_name) in pending {
            let cache_key = unique_index_key(&db, &table, &index_name);
            let state = self.unique_indexes.read().unwrap().get(&cache_key).cloned();
            let Some(state) = state else {
                continue;
            };
            let delta_count = {
                let delta = state.delta.read().unwrap();
                delta
                    .entry_count
                    .saturating_add(delta.tombstones.len() as u64)
            };
            if unique_index_delta_needs_compaction(delta_count, state.base.entries.len()) {
                self.dispatch_unique_index_compaction(&db, &table, &index_name, &state);
            }
        }
    }

    fn try_dispatch_unique_index_compaction(
        &self,
        db: &str,
        table: &str,
        index_name: &str,
        state: &Arc<UniqueIndexState>,
    ) {
        let mut schedule = self.unique_index_compaction.lock().unwrap();
        if schedule.pause_count > 0 {
            schedule
                .pending
                .insert((db.to_string(), table.to_string(), index_name.to_string()));
            return;
        }
        drop(schedule);
        self.dispatch_unique_index_compaction(db, table, index_name, state);
    }

    fn dispatch_unique_index_compaction(
        &self,
        db: &str,
        table: &str,
        index_name: &str,
        state: &Arc<UniqueIndexState>,
    ) {
        if state.compaction_in_flight.swap(true, Ordering::AcqRel) {
            return;
        }
        let job = UniqueIndexCompactionJob {
            db: db.to_string(),
            table: table.to_string(),
            index_name: index_name.to_string(),
            state: Arc::clone(state),
            unique_indexes: Arc::clone(&self.unique_indexes),
            data_dir: self.data_dir.clone(),
        };
        #[cfg(test)]
        {
            job.run();
        }
        #[cfg(not(test))]
        {
            if std::thread::Builder::new()
                .name("taibai-unique-index-compaction".to_string())
                .spawn(move || job.run())
                .is_err()
            {
                state.compaction_in_flight.store(false, Ordering::Release);
            }
        }
    }
}

/// Merges a base snapshot with serialized delta records and a set of
/// in-memory tombstones into one materialized `UniqueIndexData`. Progress
/// fields are copied from the base; callers override them as needed.
///
/// Records replay strictly in file order: a removed key only erases entries
/// recorded before the removal, so a later re-insert of the same key in the
/// same delta file survives (mirrors the in-memory mutation order).
fn materialize_unique_index_state_parts(
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

struct UniqueIndexCompactionSnapshot {
    delta_bytes: Vec<u8>,
    tombstones: HashSet<UniqueRowLocation>,
    removed_keys: HashSet<Vec<u8>>,
}

/// Single-flight background compaction of the delta back into the base
/// snapshot. Split into snapshot -> unlocked merge -> publish so appends
/// that race the merge (records appended past the snapshot length) are
/// preserved: publish rewrites the delta file to exactly that suffix.
struct UniqueIndexCompactionJob {
    db: String,
    table: String,
    index_name: String,
    state: Arc<UniqueIndexState>,
    unique_indexes: Arc<RwLock<HashMap<String, Arc<UniqueIndexState>>>>,
    data_dir: PathBuf,
}

impl UniqueIndexCompactionJob {
    /// Best effort: any failure leaves the pre-compaction base and delta
    /// untouched (still a valid, merely larger, index) and the next
    /// threshold crossing retries.
    fn run(self) {
        if let Err(_error) = self.execute() {}
        self.state
            .compaction_in_flight
            .store(false, Ordering::Release);
    }

    fn execute(&self) -> Result<()> {
        let Some(snapshot) = self.snapshot()? else {
            return Ok(());
        };
        let (tmp_path, data) = self.build_compacted_base(&snapshot)?;
        if !self.publish(snapshot.delta_bytes.len(), &tmp_path, data)? {
            let _ = std::fs::remove_file(&tmp_path);
        }
        Ok(())
    }

    fn snapshot(&self) -> Result<Option<UniqueIndexCompactionSnapshot>> {
        let _guard = self.state.mutation_lock.lock().unwrap();
        if !self.state_is_current() {
            return Ok(None);
        }
        let delta_path = self.delta_path();
        let delta_bytes = if delta_path.exists() {
            std::fs::read(&delta_path)?
        } else {
            Vec::new()
        };
        let (tombstones, removed_keys) = {
            let delta = self.state.delta.read().unwrap();
            (delta.tombstones.clone(), delta.removed_keys.clone())
        };
        Ok(Some(UniqueIndexCompactionSnapshot {
            delta_bytes,
            tombstones,
            removed_keys,
        }))
    }

    fn state_is_current(&self) -> bool {
        self.unique_indexes
            .read()
            .unwrap()
            .get(&unique_index_key(&self.db, &self.table, &self.index_name))
            .is_some_and(|current| Arc::ptr_eq(current, &self.state))
    }

    fn index_dir(&self) -> PathBuf {
        self.data_dir
            .join(&self.db)
            .join(&self.table)
            .join("indexes")
            .join("unique")
    }

    fn delta_path(&self) -> PathBuf {
        self.index_dir().join(format!(
            "{}.delta.tbi",
            sanitize_file_component(&self.index_name)
        ))
    }

    fn base_path(&self) -> PathBuf {
        self.index_dir().join(format!(
            "{}.keys.tbi",
            sanitize_file_component(&self.index_name)
        ))
    }

    fn build_compacted_base(
        &self,
        snapshot: &UniqueIndexCompactionSnapshot,
    ) -> Result<(PathBuf, UniqueIndexData)> {
        let mut data = materialize_unique_index_state_parts(
            &self.state.base,
            &snapshot.delta_bytes,
            &snapshot.tombstones,
            &snapshot.removed_keys,
        )?;
        // Embed the snapshot progress: the progress sidecar file may
        // advance during the merge and outranks this on restart.
        data.flush_epoch = self.state.progress_epoch();
        data.segment_watermarks = self.state.progress_watermarks();
        compact_prepared_unique_index_entries(&mut data);
        let tmp = self.base_path().with_extension("keys.tbi.tmp");
        std::fs::create_dir_all(self.index_dir())?;
        std::fs::write(&tmp, encode_unique_index_data(&data)?)?;
        Ok((tmp, data))
    }

    /// Publishes the compacted base. Appends that raced the merge (records
    /// past `snapshot_len`) survive as the rewritten delta file's only
    /// content. The base rename happens before the delta rewrite: a crash
    /// in between leaves duplicate-but-benign entries, never lost ones.
    fn publish(&self, snapshot_len: usize, tmp_path: &Path, data: UniqueIndexData) -> Result<bool> {
        let _guard = self.state.mutation_lock.lock().unwrap();
        if !self.state_is_current() {
            return Ok(false);
        }
        let delta_path = self.delta_path();
        let current = if delta_path.exists() {
            std::fs::read(&delta_path)?
        } else {
            Vec::new()
        };
        let suffix = current[snapshot_len.min(current.len())..].to_vec();
        let suffix_state = decode_unique_index_delta_records(&suffix, 0)?.into_state()?;
        std::fs::rename(tmp_path, self.base_path())?;
        if suffix.is_empty() {
            let _ = std::fs::remove_file(&delta_path);
        } else {
            let mut suffix_file = UNIQUE_INDEX_DELTA_MAGIC_V2.to_vec();
            suffix_file.extend_from_slice(&suffix);
            let delta_tmp = delta_path.with_extension("delta.tbi.tmp");
            std::fs::write(&delta_tmp, suffix_file)?;
            std::fs::rename(delta_tmp, &delta_path)?;
        }
        let progress = self.state.progress.read().unwrap().clone();
        self.unique_indexes.write().unwrap().insert(
            unique_index_key(&self.db, &self.table, &self.index_name),
            Arc::new(UniqueIndexState::with_progress(
                data,
                suffix_state,
                progress,
            )),
        );
        Ok(true)
    }
}

const UNIQUE_INDEX_MAGIC_V5: &[u8; 8] = b"TBIUKV5\0";
const UNIQUE_INDEX_DELTA_MAGIC_V2: &[u8; 8] = b"TBIUKD2\0";
const UNIQUE_INDEX_PROGRESS_MAGIC: &[u8; 8] = b"TBIUKP1\0";
const UNIQUE_INDEX_BLOCK_SIZE: usize = 256;
#[cfg(test)]
const UNIQUE_INDEX_DELTA_COMPACT_MIN_ENTRIES: u64 = 32;
#[cfg(not(test))]
const UNIQUE_INDEX_DELTA_COMPACT_MIN_ENTRIES: u64 = 262_144;
#[cfg(test)]
const UNIQUE_INDEX_DELTA_COMPACT_MAX_ENTRIES: u64 = 4_096;
#[cfg(not(test))]
/// Absolute ceiling that keeps the in-memory delta map bounded even when the
/// base snapshot is far larger than the ratio threshold alone would imply.
const UNIQUE_INDEX_DELTA_COMPACT_MAX_ENTRIES: u64 = 4_194_304;
const UNIQUE_INDEX_DELTA_COMPACT_BASE_RATIO: u64 = 2;

fn unique_index_delta_needs_compaction(delta_count: u64, base_count: usize) -> bool {
    let threshold = UNIQUE_INDEX_DELTA_COMPACT_MIN_ENTRIES
        .max(base_count as u64 / UNIQUE_INDEX_DELTA_COMPACT_BASE_RATIO)
        .min(UNIQUE_INDEX_DELTA_COMPACT_MAX_ENTRIES);
    delta_count >= threshold
}

fn unique_index_key(db: &str, table: &str, index_name: &str) -> String {
    format!("{}\0{}\0{}", db, table, index_name.to_ascii_lowercase())
}

fn projected_unique_key_for_row(batch: &RecordBatch, row_idx: usize) -> Option<Vec<ScalarValue>> {
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

fn unique_index_state_contains_any_key(
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
fn compact_prepared_unique_index_entries(data: &mut UniqueIndexData) {
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
    out.extend_from_slice(UNIQUE_INDEX_MAGIC_V5);
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
    for path in data.segment_watermarks.keys() {
        if !path_ids.contains_key(path.as_str()) {
            path_ids.insert(path.as_str(), paths.len() as u32);
            paths.push(path.as_str());
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
    put_u32(&mut out, data.segment_watermarks.len() as u32);
    for (path, row_count) in &data.segment_watermarks {
        put_varint(&mut out, path_ids[path.as_str()] as u64);
        put_u64(&mut out, *row_count);
    }
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
    append_unique_index_delta_records(path, entries, &[], &[])
}

fn append_unique_index_tombstone_delta(
    path: &Path,
    tombstones: &[UniqueRowLocation],
) -> Result<()> {
    append_unique_index_delta_records(path, &[], tombstones, &[])
}

fn append_unique_index_removed_key_delta(path: &Path, removed_keys: &[Vec<u8>]) -> Result<()> {
    append_unique_index_delta_records(path, &[], &[], removed_keys)
}

fn append_unique_index_delta_records(
    path: &Path,
    entries: &[UniqueIndexEntry],
    tombstones: &[UniqueRowLocation],
    removed_keys: &[Vec<u8>],
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
    for encoded in removed_keys {
        let mut record = Vec::new();
        record.push(3);
        put_bytes(&mut record, encoded);
        write_u32(&mut file, record.len() as u32)?;
        file.write_all(&record)?;
    }
    file.sync_all()?;
    Ok(())
}

enum UniqueIndexDeltaRecord {
    Entry(PreparedUniqueIndexEntry),
    Tombstone(UniqueRowLocation),
    RemovedKey(Vec<u8>),
}

#[derive(Default)]
struct UniqueIndexDelta {
    records: Vec<UniqueIndexDeltaRecord>,
}

impl UniqueIndexDelta {
    fn into_state(self) -> Result<UniqueIndexDeltaState> {
        let mut state = UniqueIndexDeltaState::default();
        for record in self.records {
            match record {
                UniqueIndexDeltaRecord::Entry(addition) => {
                    state
                        .key_locations
                        .entry(addition.encoded_key.clone())
                        .or_default()
                        .push(addition.entry.location);
                    state.entry_count += 1;
                }
                UniqueIndexDeltaRecord::Tombstone(location) => {
                    state.tombstones.insert(location);
                }
                UniqueIndexDeltaRecord::RemovedKey(encoded) => {
                    if let Some(locations) = state.key_locations.remove(&encoded) {
                        state.entry_count =
                            state.entry_count.saturating_sub(locations.len() as u64);
                    }
                    state.removed_keys.insert(encoded);
                }
            }
        }
        Ok(state)
    }
}

fn encode_unique_index_progress(progress: &UniqueIndexProgress) -> Vec<u8> {
    let mut out = UNIQUE_INDEX_PROGRESS_MAGIC.to_vec();
    out.push(if progress.flush_epoch.is_some() { 1 } else { 0 });
    put_u64(&mut out, progress.flush_epoch.unwrap_or(0));
    put_u32(&mut out, progress.segment_watermarks.len() as u32);
    for (path, row_count) in &progress.segment_watermarks {
        put_string(&mut out, path);
        put_u64(&mut out, *row_count);
    }
    out
}

fn decode_unique_index_progress(bytes: &[u8]) -> UniqueIndexProgress {
    if bytes.len() < 8 || &bytes[..8] != UNIQUE_INDEX_PROGRESS_MAGIC {
        return UniqueIndexProgress::default();
    }
    let mut cursor = Cursor::new(&bytes[8..]);
    let mut progress = UniqueIndexProgress::default();
    let (Ok(flag), Ok(epoch), Ok(count)) = (
        read_u8(&mut cursor),
        read_u64(&mut cursor),
        read_u32(&mut cursor),
    ) else {
        return UniqueIndexProgress::default();
    };
    progress.flush_epoch = (flag == 1).then_some(epoch);
    for _ in 0..count {
        let (Ok(path), Ok(row_count)) = (read_string(&mut cursor), read_u64(&mut cursor)) else {
            return UniqueIndexProgress::default();
        };
        progress.segment_watermarks.insert(path, row_count);
    }
    progress
}

fn decode_unique_index_delta(bytes: &[u8]) -> Result<UniqueIndexDelta> {
    if bytes.is_empty() {
        return Ok(UniqueIndexDelta::default());
    }
    let mut cursor = Cursor::new(bytes);
    let mut magic = [0u8; 8];
    cursor.read_exact(&mut magic)?;
    if &magic != UNIQUE_INDEX_DELTA_MAGIC_V2 {
        return Err(TaibaiError::Storage(
            "invalid unique index delta file".to_string(),
        ));
    }
    decode_unique_index_delta_records(bytes, cursor.position() as usize)
}

/// Decodes length-framed delta records starting at `offset`. Offset 8 skips
/// the file magic; offset 0 decodes a suffix appended after a compaction
/// snapshot, which is how appends that raced an in-flight merge survive.
fn decode_unique_index_delta_records(bytes: &[u8], offset: usize) -> Result<UniqueIndexDelta> {
    let tail = &bytes[offset.min(bytes.len())..];
    let mut delta = UniqueIndexDelta::default();
    let mut cursor = Cursor::new(tail);
    while (cursor.position() as usize) < tail.len() {
        let len = read_u32(&mut cursor)? as usize;
        let start = cursor.position() as usize;
        let end = start + len;
        if end > tail.len() {
            return Err(TaibaiError::Storage(
                "truncated unique index delta record".to_string(),
            ));
        }
        let record = &tail[start..end];
        let mut record_cursor = Cursor::new(record);
        let record_type = read_u8(&mut record_cursor)?;
        match record_type {
            1 => {
                let encoded_key = read_bytes(&mut record_cursor)?;
                let entry = read_unique_index_entry(&mut record_cursor)?;
                delta
                    .records
                    .push(UniqueIndexDeltaRecord::Entry(PreparedUniqueIndexEntry {
                        encoded_key,
                        entry,
                    }));
            }
            2 => {
                let segment_path = read_string(&mut record_cursor)?;
                let row_id = read_u64(&mut record_cursor)?;
                delta
                    .records
                    .push(UniqueIndexDeltaRecord::Tombstone(UniqueRowLocation {
                        segment_path,
                        row_id,
                    }));
            }
            3 => {
                let encoded_key = read_bytes(&mut record_cursor)?;
                delta
                    .records
                    .push(UniqueIndexDeltaRecord::RemovedKey(encoded_key));
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

fn write_u32<W: Write>(writer: &mut W, value: u32) -> Result<()> {
    writer.write_all(&value.to_le_bytes())?;
    Ok(())
}
fn decode_unique_index_data(bytes: &[u8]) -> Result<Option<UniqueIndexData>> {
    let mut cursor = Cursor::new(bytes);
    let mut magic = [0u8; 8];
    cursor.read_exact(&mut magic)?;
    if &magic != UNIQUE_INDEX_MAGIC_V5 {
        return Err(TaibaiError::Storage(
            "unsupported unique index version".to_string(),
        ));
    }
    decode_unique_index_data_v5(&mut cursor)
}

fn decode_unique_index_data_v5(cursor: &mut Cursor<&[u8]>) -> Result<Option<UniqueIndexData>> {
    let Some((mut data, paths)) = decode_unique_index_data_blocks_with_paths(cursor)? else {
        return Ok(None);
    };
    let mut flag = [0u8; 1];
    cursor.read_exact(&mut flag)?;
    let epoch = read_u64(cursor)?;
    data.flush_epoch = (flag[0] == 1).then_some(epoch);
    let watermark_count = read_u32(cursor)? as usize;
    for _ in 0..watermark_count {
        let path_idx = read_varint(cursor)? as usize;
        let row_count = read_u64(cursor)?;
        let Some(segment_path) = paths.get(path_idx) else {
            return Err(TaibaiError::Storage(
                "corrupt unique index watermark path reference".to_string(),
            ));
        };
        data.segment_watermarks
            .insert(segment_path.clone(), row_count);
    }
    Ok(Some(data))
}

fn decode_unique_index_data_blocks_with_paths(
    cursor: &mut Cursor<&[u8]>,
) -> Result<Option<(UniqueIndexData, Vec<String>)>> {
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
    Ok(Some((
        UniqueIndexData {
            db,
            table,
            index_name,
            columns,
            entries,
            flush_epoch: None,
            segment_watermarks: HashMap::new(),
            tombstones: HashSet::new(),
            encoded_keys,
            blocks,
        },
        paths,
    )))
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
#[path = "index_manager_test.rs"]
mod tests;

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
