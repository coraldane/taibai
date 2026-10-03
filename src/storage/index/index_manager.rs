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

/// Unique/PK keys published per explicit transaction, per (db, table), per
/// index name — the statement-time visibility record ROLLBACK retracts.
type TxnPublishedUniqueKeys =
    std::sync::Mutex<HashMap<u64, HashMap<(String, String), Vec<(String, Vec<Vec<ScalarValue>>)>>>>;

pub struct IndexManager {
    unique_indexes: Arc<RwLock<HashMap<String, Arc<UniqueIndexState>>>>,
    primary_key_indexes: RwLock<HashMap<String, HashSet<Vec<u8>>>>,
    segment_mgr: Arc<SegmentManager>,
    data_dir: PathBuf,
    unique_index_compaction: Mutex<UniqueIndexCompactionSchedule>,
    /// Unique/PK keys that explicit-transaction INSERT statements published
    /// at statement time. Entries become visible to constraint probes before
    /// COMMIT, so ROLLBACK must retract exactly these keys — otherwise the
    /// next INSERT of the same key sees a phantom duplicate until the next
    /// snapshot refresh. COMMIT only drops the record.
    txn_published: TxnPublishedUniqueKeys,
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
            txn_published: Mutex::new(HashMap::new()),
        }
    }

    /// Records the unique-index/PK keys a transactional INSERT just published,
    /// so ROLLBACK can retract them (see `discard_txn_published_entries_at_rollback`).
    pub fn record_txn_published_keys(
        &self,
        txn_id: u64,
        db: &str,
        table: &str,
        index_name: &str,
        keys: Vec<Vec<ScalarValue>>,
    ) {
        if txn_id == 0 || keys.is_empty() {
            return;
        }
        let mut guard = self.txn_published.lock().unwrap();
        let per_table = guard.entry(txn_id).or_default();
        let per_index = per_table
            .entry((db.to_string(), table.to_string()))
            .or_default();
        if let Some(existing) = per_index.iter_mut().find(|(name, _)| name == index_name) {
            existing.1.extend(keys);
        } else {
            per_index.push((index_name.to_string(), keys));
        }
    }

    /// COMMIT path: the published entries are now authoritative, drop the
    /// rollback record.
    pub fn forget_txn_published_entries(&self, txn_id: u64) {
        if txn_id == 0 {
            return;
        }
        self.txn_published.lock().unwrap().remove(&txn_id);
    }

    /// Removes keys from the in-memory primary-key existence cache. Returns
    /// false when the cache is absent or a key cannot be encoded, so callers
    /// can fall back to dropping the whole cache.
    pub fn remove_primary_keys(&self, db: &str, table: &str, keys: &[Vec<ScalarValue>]) -> bool {
        let cache_key = format!("{}\0{}", db, table);
        let mut indexes = self.primary_key_indexes.write().unwrap();
        let Some(current) = indexes.get_mut(&cache_key) else {
            return false;
        };
        for key in keys {
            let Ok(encoded) = encode_unique_key(key) else {
                return false;
            };
            current.remove(&encoded);
        }
        true
    }

    /// ROLLBACK path: retract the index entries this transaction published at
    /// statement time. Key removal is precise and persisted (delta removal
    /// records); when a snapshot cannot be maintained it is invalidated so
    /// probes fall back to a full scan until the next rebuild — slow, never
    /// wrong.
    pub fn discard_txn_published_entries_at_rollback(&self, txn_id: u64) {
        if txn_id == 0 {
            return;
        }
        let Some(per_table) = self.txn_published.lock().unwrap().remove(&txn_id) else {
            return;
        };
        for ((db, table), per_index) in per_table {
            for (index_name, keys) in per_index {
                let is_primary = index_name == PRIMARY_LOCATION_INDEX_NAME;
                if is_primary && !self.remove_primary_keys(&db, &table, &keys) {
                    self.invalidate_primary_key_cache(&db, &table);
                }
                match self.remove_unique_index_keys(&db, &table, &index_name, &keys) {
                    Ok(true) => {}
                    _ => {
                        if is_primary {
                            self.invalidate_primary_key_index(&db, &table);
                        } else {
                            self.invalidate_unique_index_snapshot(&db, &table, &index_name);
                        }
                    }
                }
            }
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

    /// Persist an already-prepared snapshot (entries and encoded keys aligned)
    /// by streaming it into the on-demand base format, then install a state
    /// that reads that file lazily. Peak memory stays at the caller's prepared
    /// snapshot; nothing re-encodes the whole index into one buffer.
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
        let file = std::fs::File::create(&data_tmp)?;
        let mut writer = UniqueIndexBaseWriter::new(
            std::io::BufWriter::new(file),
            db,
            table,
            index_name,
            data.columns.clone(),
        )?;
        for (encoded, entry) in data.encoded_keys.iter().zip(&data.entries) {
            writer.push_entry(encoded, &entry.location)?;
        }
        let buffered = writer.finish(data.flush_epoch, &data.segment_watermarks)?;
        let file = buffered
            .into_inner()
            .map_err(|error| TaibaiError::Storage(error.to_string()))?;
        file.sync_all()?;
        std::fs::rename(&data_tmp, &data_path)?;
        let delta_path = dir.join(format!("{}.delta.tbi", base));
        let _ = std::fs::remove_file(delta_path);
        let _ = std::fs::remove_file(dir.join(format!("{}.progress.tbi", base)));
        let _ = std::fs::remove_file(invalid_path);
        let base_reader = UniqueIndexBase::open(&data_path)?;
        self.unique_indexes.write().unwrap().insert(
            unique_index_key(db, table, index_name),
            Arc::new(UniqueIndexState::new(
                base_reader,
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
            key_count: state.base.entry_count + delta.entry_count as usize,
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
        let t_load = std::time::Instant::now();
        log_index_profile("load_state_cache_miss", 0, std::time::Duration::ZERO);
        let base = sanitize_file_component(index_name);
        let dir = self.unique_index_dir(db, table);
        if dir.join(format!("{}.invalid", base)).exists() {
            return Ok(None);
        }
        let path = dir.join(format!("{}.keys.tbi", base));
        if !path.exists() {
            return Ok(None);
        }
        let base_reader = self.open_unique_index_base(&path)?;
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
            Arc::new(UniqueIndexState::with_progress(
                base_reader,
                delta,
                progress,
            ))
        } else {
            Arc::new(UniqueIndexState::new(base_reader, delta))
        };
        let current = self
            .unique_indexes
            .write()
            .unwrap()
            .entry(cache_key)
            .or_insert_with(|| Arc::clone(&state))
            .clone();
        log_index_profile("load_state_full_decode", 0, t_load.elapsed());
        Ok(Some(current))
    }

    /// Open a persisted base snapshot. On-demand (V6) files parse their
    /// header and footer only, so a 500 MB index loads in microseconds and
    /// stays resident at one small directory; legacy (V5) files fall back to
    /// a full in-memory decode until the next compaction rewrites them.
    fn open_unique_index_base(&self, path: &Path) -> Result<UniqueIndexBase> {
        use std::io::Read;
        let mut file = std::fs::File::open(path)?;
        let mut magic = [0u8; 8];
        file.read_exact(&mut magic)?;
        drop(file);
        if magic == *UNIQUE_INDEX_MAGIC_V6 {
            return UniqueIndexBase::open(path);
        }
        let bytes = std::fs::read(path)?;
        let Some(mut data) = decode_unique_index_data(&bytes)? else {
            return Err(TaibaiError::Storage(
                "unsupported unique index version".to_string(),
            ));
        };
        if data.encoded_keys.len() != data.entries.len() {
            prepare_unique_index_data(&mut data)?;
        }
        Ok(UniqueIndexBase::from_decoded(data))
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
        let t0 = std::time::Instant::now();
        let cache_key = format!("{}\0{}", db, table);
        let mut indexes = self.primary_key_indexes.write().unwrap();
        let Some(current) = indexes.get_mut(&cache_key) else {
            return Ok(false);
        };
        for key in keys {
            current.insert(encode_unique_key(key)?);
        }
        log_index_profile("append_pk_insert", keys.len(), t0.elapsed());
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

    /// Load the index state for a probe, incrementally refreshing it first
    /// when it went stale against the table's memtable-flush epoch (a flush
    /// after the snapshot was built may have moved rows into segments
    /// without index entries). `Ok(None)` — no state, or the incremental
    /// refresh could not run — makes the caller fall back to its
    /// no-snapshot behavior. See `unique_keys_exist_if_current` for the
    /// deliberately refresh-free variant used by the bulk write path.
    fn fresh_unique_index_state(
        &self,
        db: &str,
        table: &str,
        index_name: &str,
    ) -> Result<Option<Arc<UniqueIndexState>>> {
        let Some(mut state) = self.load_unique_index_state(db, table, index_name)? else {
            return Ok(None);
        };
        if state.progress_epoch() != Some(self.segment_mgr.memtable_flush_epoch(db, table)) {
            let Some(refreshed) =
                self.refresh_stale_unique_index_state(db, table, index_name, &state)?
            else {
                return Ok(None);
            };
            state = refreshed;
        }
        Ok(Some(state))
    }

    pub fn unique_key_exists(
        &self,
        db: &str,
        table: &str,
        index_name: &str,
        key: &[ScalarValue],
    ) -> Result<Option<bool>> {
        let Some(state) = self.fresh_unique_index_state(db, table, index_name)? else {
            return Ok(None);
        };
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
        let prof_start = std::time::Instant::now();
        let progress_watermarks = state.progress_watermarks();
        let mut additions = Vec::new();
        let mut watermarks = HashMap::new();
        let mut prof_scanned_rows: u64 = 0;
        let table_dir = self.data_dir.join(db).join(table);
        if table_dir.exists() {
            for seg_path in SegmentManager::discover_segments(&table_dir)? {
                let reader_t = std::time::Instant::now();
                let reader = self.segment_mgr.open_reader_cached(&seg_path)?;
                let current_rows = reader.row_count();
                let reader_open_us = reader_t.elapsed().as_micros() as u64;
                if reader_open_us > 1000 {
                    log_index_profile(
                        "stale_reader_open",
                        current_rows as usize,
                        std::time::Duration::from_micros(reader_open_us),
                    );
                }
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
                    let rg_t = std::time::Instant::now();
                    let batch = reader.read_row_group_with_projection(rg_idx, &indices)?;
                    prof_scanned_rows += batch.num_rows() as u64;
                    let rg_us = rg_t.elapsed().as_micros() as u64;
                    if rg_us > 5000 {
                        log_index_profile(
                            "stale_rg_read",
                            batch.num_rows(),
                            std::time::Duration::from_micros(rg_us),
                        );
                    }
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
        log_index_profile(
            "stale_scan_total",
            prof_scanned_rows as usize,
            prof_start.elapsed(),
        );
        let prep_t = std::time::Instant::now();
        let prepared_additions = self.prepare_unique_index_entries(additions)?;
        log_index_profile("stale_prepare", prepared_additions.len(), prep_t.elapsed());
        let delta_path = self
            .unique_index_dir(db, table)
            .join(format!("{}.delta.tbi", sanitize_file_component(index_name)));
        let delta_t = std::time::Instant::now();
        append_prepared_unique_index_delta(&delta_path, &prepared_additions)?;
        log_index_profile(
            "stale_delta_append",
            prepared_additions.len(),
            delta_t.elapsed(),
        );
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
            unique_index_delta_needs_compaction(delta_count, state.base.entry_count);
        state.set_progress(Some(epoch_before), watermarks);
        let prog_t = std::time::Instant::now();
        self.persist_unique_index_progress(db, table, index_name)?;
        log_index_profile(
            "stale_progress_persist",
            delta_count as usize,
            prog_t.elapsed(),
        );
        log_index_profile(
            "stale_needs_compaction",
            if needs_compaction { 1 } else { 0 },
            std::time::Duration::ZERO,
        );
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
        let t0 = std::time::Instant::now();
        let Some(state) = self.load_unique_index_state(db, table, index_name)? else {
            log_index_profile("refresh_no_state", 0, t0.elapsed());
            return Ok(());
        };
        if state.progress_epoch() == Some(self.segment_mgr.memtable_flush_epoch(db, table)) {
            log_index_profile("refresh_current", 0, t0.elapsed());
            return Ok(());
        }
        let t1 = std::time::Instant::now();
        self.refresh_stale_unique_index_state(db, table, index_name, &state)?;
        log_index_profile("refresh_stale", 0, t1.elapsed());
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
        let Some(state) = self.fresh_unique_index_state(db, table, index_name)? else {
            return Ok(None);
        };
        let encoded_keys = keys
            .into_iter()
            .map(|key| encode_unique_key(key.borrow()))
            .collect::<Result<Vec<_>>>()?;
        let mut locations = {
            let delta = state.delta.read().unwrap();
            // Build the tombstone probe index once for the whole key loop:
            // per-location linear scans made many-key probes quadratic in the
            // tombstone count.
            let tombstone_index = UniqueTombstoneIndex::build(&delta.tombstones);
            let mut locations = Vec::new();
            for encoded in &encoded_keys {
                if let Some(matches) = delta.key_locations.get(encoded) {
                    locations.extend(
                        matches
                            .iter()
                            .filter(|location| {
                                !location.segment_path.is_empty()
                                    && !tombstone_index.contains(location)
                            })
                            .cloned(),
                    );
                }
                for location in state.base.find_locations(encoded)? {
                    if !location.segment_path.is_empty() && !tombstone_index.contains(&location) {
                        locations.push(location);
                    }
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

    /// Generic for the same reason as `unique_key_locations`.
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
        let Some(state) = self.fresh_unique_index_state(db, table, index_name)? else {
            return Ok(None);
        };
        Self::state_contains_any_encoded_key(&self.segment_mgr, state, keys)
    }

    pub fn unique_keys_exist_if_current<I>(
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
        let Some(state) = self.load_unique_index_state(db, table, index_name)? else {
            return Ok(None);
        };
        if state.progress_epoch() != Some(self.segment_mgr.memtable_flush_epoch(db, table)) {
            // Stale state: a synchronous refresh can cost a full-table
            // scan, which a bulk import must not pay on its write path.
            // Post-write index refresh keeps uniqueness tracked either way.
            return Ok(None);
        }
        Self::state_contains_any_encoded_key(&self.segment_mgr, state, keys)
    }

    fn state_contains_any_encoded_key<I>(
        segment_mgr: &SegmentManager,
        state: Arc<UniqueIndexState>,
        keys: I,
    ) -> Result<Option<bool>>
    where
        I: IntoIterator,
        I::Item: std::borrow::Borrow<Vec<ScalarValue>>,
    {
        let mut encoded_keys = keys
            .into_iter()
            .map(|key| encode_unique_key(key.borrow()))
            .collect::<Result<Vec<_>>>()?;
        encoded_keys.sort();
        encoded_keys.dedup();
        Ok(Some(unique_index_state_contains_any_key(
            segment_mgr,
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
            unique_index_delta_needs_compaction(delta_count, state.base.entry_count)
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

    /// Batched variant of `append_prepared_unique_index_entries` for one
    /// statement's entries across all of a table's location indexes (named
    /// unique indexes plus the primary-key location index). Instead of a full
    /// load-state / lock / write / fsync cycle per index, every state is
    /// loaded once up front, every index's mutation lock is held across the
    /// whole batch, the delta files are appended without per-append fsyncs,
    /// and each written file is then fsynced exactly once before the locks
    /// are released — one shared lock pass and one fsync per delta file per
    /// statement. Per-index lock semantics and the on-disk delta format are
    /// unchanged; an index whose entries are empty or whose snapshot is
    /// absent is skipped exactly as the per-index path's early returns did,
    /// and an index whose state was concurrently replaced falls back to the
    /// single-index append (which reloads and rechecks). `Err` carries the
    /// index name whose append failed so the caller can invalidate exactly
    /// that one snapshot, as the previous per-index loop did.
    pub fn append_prepared_unique_index_entries_batch(
        &self,
        db: &str,
        table: &str,
        batches: Vec<(String, Vec<PreparedUniqueIndexEntry>)>,
    ) -> std::result::Result<(), (String, TaibaiError)> {
        // Phase 1 (no locks): resolve every state up front so cache-miss
        // loads and their full-file decodes never run inside a mutation
        // lock. Empty entry lists and absent snapshots are no-ops, matching
        // the per-index early `Ok(true)`/`Ok(false)` returns.
        let mut pending: Vec<(String, Vec<PreparedUniqueIndexEntry>, Arc<UniqueIndexState>)> =
            Vec::new();
        for (index_name, entries) in batches {
            if entries.is_empty() {
                continue;
            }
            match self.load_unique_index_state(db, table, &index_name) {
                Ok(Some(state)) => pending.push((index_name, entries, state)),
                Ok(None) => {}
                Err(error) => return Err((index_name, error)),
            }
        }
        if pending.is_empty() {
            return Ok(());
        }

        // Phase 2: take every index's mutation lock in list order and hold
        // all of them for the whole statement. No other code path acquires
        // two of these per-index locks, so the ordering cannot deadlock.
        let guards: Vec<_> = pending
            .iter()
            .map(|(_, _, state)| state.mutation_lock.lock().unwrap())
            .collect();
        let mut failure: Option<(String, TaibaiError)> = None;
        let mut deferred: Vec<usize> = Vec::new();
        let mut to_sync: Vec<(String, std::fs::File)> = Vec::new();
        let mut to_compact: Vec<(String, Arc<UniqueIndexState>)> = Vec::new();
        for (pos, (index_name, entries, state)) in pending.iter().enumerate() {
            let _guard = &guards[pos];
            let cache_key = unique_index_key(db, table, index_name);
            let is_current = self
                .unique_indexes
                .read()
                .unwrap()
                .get(&cache_key)
                .is_some_and(|current| Arc::ptr_eq(current, state));
            if !is_current {
                // The loaded state was replaced (refresh/rebuild) after the
                // phase-1 load; retry through the single-index path once the
                // locks are released, exactly as its self-retry did.
                deferred.push(pos);
                continue;
            }
            let append_result = (|| -> Result<std::fs::File> {
                let dir = self.unique_index_dir(db, table);
                std::fs::create_dir_all(&dir)?;
                let base = sanitize_file_component(index_name);
                let delta_path = dir.join(format!("{}.delta.tbi", base));
                append_prepared_unique_index_delta_deferred_sync(&delta_path, entries)
            })();
            let file = match append_result {
                Ok(file) => file,
                Err(error) => {
                    failure = Some((index_name.clone(), error));
                    break;
                }
            };
            to_sync.push((index_name.clone(), file));
            let needs_compaction = {
                let mut delta = state.delta.write().unwrap();
                for addition in entries {
                    delta
                        .key_locations
                        .entry(addition.encoded_key.clone())
                        .or_default()
                        .push(addition.entry.location.clone());
                    delta.entry_count += 1;
                }
                let delta_count = delta
                    .entry_count
                    .saturating_add(delta.tombstones.len() as u64);
                unique_index_delta_needs_compaction(delta_count, state.base.entry_count)
            };
            if needs_compaction {
                to_compact.push((index_name.clone(), Arc::clone(state)));
            }
        }
        // Phase 3: one fsync per written file, still under the shared lock
        // pass — the delta bytes must be durable before a concurrent
        // compaction could rename the file out from under this append.
        if failure.is_none() {
            for (index_name, file) in &mut to_sync {
                if let Err(error) = file.sync_all() {
                    failure = Some((index_name.clone(), error.into()));
                    break;
                }
            }
        }
        drop(guards);

        if let Some((index_name, error)) = failure {
            return Err((index_name, error));
        }
        for (index_name, state) in &to_compact {
            self.try_dispatch_unique_index_compaction(db, table, index_name, state);
        }
        for pos in deferred {
            let (index_name, entries, _) = &mut pending[pos];
            let index_name = index_name.clone();
            let entries = std::mem::take(entries);
            if let Err(error) =
                self.append_prepared_unique_index_entries(db, table, &index_name, entries)
            {
                return Err((index_name, error));
            }
        }
        Ok(())
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
                state.base.entry_count,
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
            unique_index_delta_needs_compaction(delta_count, state.base.entry_count)
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

    /// Evict every cached index state of the table WITHOUT touching the
    /// on-disk files. Base states pin open file handles (the rename-race
    /// fix in `UniqueIndexBase`), so directory-level operations — RENAME
    /// TABLE, TRUNCATE's staged move, DROP TABLE — must call this first:
    /// on Windows, renaming or removing a directory that still has an open
    /// handle inside fails with PermissionDenied.
    pub fn drop_table_index_states(&self, db: &str, table: &str) {
        self.primary_key_indexes
            .write()
            .unwrap()
            .remove(&format!("{}\0{}", db, table));
        self.unique_indexes
            .write()
            .unwrap()
            .retain(|key, _| !key.starts_with(&format!("{}\0{}\0", db, table)));
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

fn log_index_profile(stage: &str, rows: usize, elapsed: std::time::Duration) {
    if crate::common::config::insert_profile_enabled() {
        println!(
            "[insert-profile] stage=index_{} rows={} elapsed_us={}",
            stage,
            rows,
            elapsed.as_micros()
        );
    }
}
