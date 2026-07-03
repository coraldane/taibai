#![allow(dead_code)]
mod active_writer;
mod cluster_meta_cache;
mod config;
mod helpers;
mod reader_cache;
mod state;

use std::collections::{HashMap, HashSet};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

use crate::common::error::{Result, TaibaiError};
use crate::storage::bulk_payload::{
    parse_direct_bulk_payload, DirectBulkColumn, DirectBulkRequest,
};
use crate::storage::core::wal::WalWriter;
use crate::storage::partition::partition_function::compute_partition_key_from_scalar;
use crate::storage::segment::meta::remove_sidecar_meta;
use crate::storage::segment::{
    encode_segment_header, store, SegmentReader, SegmentWriter, SEGMENT_HEADER_SIZE,
};
use crate::types::batch::{RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

pub use config::SegmentConfig;
pub(crate) use helpers::direct_bulk_request_to_batch;
pub use helpers::table_key;

use active_writer::ActiveSegmentWriter;
use cluster_meta_cache::ClusterMetaCache;
use config::{
    bulk_compression_level, reader_cache_capacity, FLUSH_CHECK_INTERVAL_SECS,
    FLUSH_IDLE_THRESHOLD_SECS, SEGMENT_BUFWRITER_CAP,
};
use helpers::{
    discover_max_partition_seq, discover_max_seq, ensure_state_not_retired, filter_batch_by_mask,
    is_transient_footer_error,
};
use reader_cache::ReaderCache;
use state::TableSegmentState;

fn same_segment_path(a: &Path, b: &Path) -> bool {
    a == b || a.to_string_lossy().replace('\\', "/") == b.to_string_lossy().replace('\\', "/")
}

enum PreparedDirectColumns<'a> {
    Borrowed(Vec<DirectBulkColumn<'a>>),
    Owned(Vec<OwnedDirectBulkColumn<'a>>),
}

struct PreparedDirectBulkGroup<'a> {
    partition_key: String,
    row_count: usize,
    columns: PreparedDirectColumns<'a>,
    compressed_columns: Vec<(Vec<u8>, u64)>,
}

struct OwnedDirectBulkColumn<'a> {
    name: &'a str,
    data_type: crate::protocol::types::DataType,
    scale: i32,
    precision: i32,
    data: Vec<u8>,
    nulls: Vec<u8>,
    variable_ranges: Option<Vec<(usize, usize)>>,
}

pub struct SegmentManager {
    data_dir: PathBuf,
    tables: RwLock<HashMap<String, Arc<TableSegmentState>>>,
    reader_cache: Mutex<ReaderCache>,
    cluster_meta_cache: Mutex<ClusterMetaCache>,
    flush_stop: Arc<AtomicBool>,
    flush_paused: Arc<AtomicBool>,
    next_txn_id: AtomicU64,
    config: SegmentConfig,
}

impl SegmentManager {
    pub fn new(data_dir: &Path) -> Result<Self> {
        Self::with_config(data_dir, SegmentConfig::default())
    }

    pub fn with_config(data_dir: &Path, config: SegmentConfig) -> Result<Self> {
        let mut tables = HashMap::new();

        if data_dir.exists() {
            if let Ok(dbs) = std::fs::read_dir(data_dir) {
                for db_entry in dbs.flatten() {
                    if !db_entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                        continue;
                    }
                    let db_name = db_entry.file_name().to_string_lossy().to_string();
                    if db_name == "meta" || db_name == "wal" {
                        continue;
                    }
                    let db_dir = data_dir.join(&db_name);
                    if let Ok(tables_dir) = std::fs::read_dir(&db_dir) {
                        for tbl_entry in tables_dir.flatten() {
                            if !tbl_entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                                continue;
                            }
                            let tbl_dir = tbl_entry.path();
                            cleanup_incomplete_compaction_files(&tbl_dir);
                            let max_seq = discover_max_seq(&tbl_dir);
                            let partition_seq = discover_max_partition_seq(&tbl_dir);
                            let key = table_key(&db_name, &tbl_entry.file_name().to_string_lossy());
                            tables.insert(
                                key,
                                Arc::new(TableSegmentState::with_partition_seq(
                                    max_seq,
                                    partition_seq,
                                )),
                            );
                        }
                    }
                }
            }
        }

        let flush_stop = Arc::new(AtomicBool::new(false));
        let flush_paused = Arc::new(AtomicBool::new(false));

        Ok(Self {
            data_dir: data_dir.to_path_buf(),
            tables: RwLock::new(tables),
            reader_cache: Mutex::new(ReaderCache::new(reader_cache_capacity())),
            cluster_meta_cache: Mutex::new(ClusterMetaCache::new()),
            flush_stop,
            flush_paused,
            next_txn_id: AtomicU64::new(1),
            config,
        })
    }

    pub fn set_config(&mut self, config: SegmentConfig) {
        self.config = config;
    }

    fn min_segment_seal_size(&self) -> u64 {
        self.config.min_segment_size
    }

    fn max_segment_size(&self) -> u64 {
        self.config.max_segment_size
    }

    pub fn start_flush_thread(mgr: &Arc<Self>) {
        let mgr_clone = Arc::clone(mgr);
        let stop = mgr.flush_stop.clone();
        let paused = mgr.flush_paused.clone();
        std::thread::Builder::new()
            .name("taibai-flush".into())
            .spawn(move || {
                while !stop.load(Ordering::Acquire) {
                    std::thread::sleep(std::time::Duration::from_secs(FLUSH_CHECK_INTERVAL_SECS));
                    if stop.load(Ordering::Acquire) {
                        break;
                    }
                    if paused.load(Ordering::Acquire) {
                        tracing::debug!("flush thread paused, skipping periodic check");
                        continue;
                    }
                    if let Err(e) = mgr_clone.periodic_flush_check() {
                        tracing::warn!(error = %e, "periodic flush check failed");
                    }
                }
            })
            .expect("failed to spawn flush thread");
    }

    pub fn shutdown(&self) {
        self.flush_stop.store(true, Ordering::Release);
        if let Err(e) = self.flush_all_active() {
            tracing::warn!(error = %e, "failed to flush all active segments during shutdown");
        }
    }

    pub fn pause_flush(&self) {
        self.flush_paused.store(true, Ordering::Release);
        tracing::info!("flush thread paused");
    }

    pub fn resume_flush(&self) {
        self.flush_paused.store(false, Ordering::Release);
        tracing::info!("flush thread resumed");
    }

    pub fn is_flush_paused(&self) -> bool {
        self.flush_paused.load(Ordering::Acquire)
    }

    pub fn alloc_txn_id(&self) -> u64 {
        self.next_txn_id.fetch_add(1, Ordering::Relaxed)
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    pub fn table_dir(&self, db: &str, table: &str) -> PathBuf {
        self.data_dir.join(db).join(table)
    }

    fn get_or_create_state(&self, key: &str) -> Arc<TableSegmentState> {
        {
            let tables = self.tables.read().unwrap();
            if let Some(state) = tables.get(key) {
                return Arc::clone(state);
            }
        }
        let mut tables = self.tables.write().unwrap();
        tables
            .entry(key.to_string())
            .or_insert_with(|| Arc::new(TableSegmentState::new(0)))
            .clone()
    }

    fn table_state_key_from_dir(&self, table_dir: &Path) -> Result<String> {
        let relative = table_dir.strip_prefix(&self.data_dir).map_err(|_| {
            TaibaiError::Storage(format!(
                "table dir '{}' is outside data dir '{}'",
                table_dir.display(),
                self.data_dir.display()
            ))
        })?;

        let mut components = relative.components();
        let db = components
            .next()
            .ok_or_else(|| TaibaiError::Storage("table dir is missing database name".into()))?;
        let table = components
            .next()
            .ok_or_else(|| TaibaiError::Storage("table dir is missing table name".into()))?;

        Ok(table_key(
            &db.as_os_str().to_string_lossy(),
            &table.as_os_str().to_string_lossy(),
        ))
    }

    fn get_or_create_state_for_table_dir(
        &self,
        table_dir: &Path,
    ) -> Result<Arc<TableSegmentState>> {
        let key = self.table_state_key_from_dir(table_dir)?;
        Ok(self.get_or_create_state(&key))
    }

    pub fn discover_segments(table_dir: &Path) -> Result<Vec<PathBuf>> {
        store::discover_segments(table_dir)
    }

    pub fn discover_inactive_segments(
        &self,
        db: &str,
        table: &str,
        table_dir: &Path,
    ) -> Result<Vec<PathBuf>> {
        let segments = Self::discover_segments(table_dir)?;
        let key = table_key(db, table);
        let active_paths = {
            let tables = self.tables.read().unwrap();
            tables
                .get(&key)
                .map(|state| {
                    let active_guard = state.active_segments.lock().unwrap();
                    active_guard
                        .values()
                        .map(|segment| segment.path.clone())
                        .collect::<HashSet<_>>()
                })
                .unwrap_or_default()
        };

        if active_paths.is_empty() {
            return Ok(segments);
        }

        Ok(segments
            .into_iter()
            .filter(|path| !active_paths.contains(path))
            .collect())
    }

    pub fn find_unsealed_segment(
        table_dir: &Path,
        min_seal_size: u64,
    ) -> Result<Option<(PathBuf, Schema, u64)>> {
        let segments = Self::discover_segments(table_dir)?;

        for seg_path in segments.iter().rev() {
            let file_len = match std::fs::metadata(seg_path) {
                Ok(m) => m.len(),
                Err(_) => continue,
            };

            if file_len < SEGMENT_HEADER_SIZE as u64 {
                continue;
            }

            if let Ok(reader) = SegmentReader::open(seg_path) {
                let seg_size = file_len;
                if seg_size < min_seal_size && reader.row_count() > 0 {
                    return Ok(Some((
                        seg_path.clone(),
                        reader.schema().clone(),
                        reader.row_count(),
                    )));
                }
            }
        }

        Ok(None)
    }

    pub fn next_segment_path(&self, table_dir: &Path, _partition_key: &str) -> Result<PathBuf> {
        let state = self.get_or_create_state_for_table_dir(table_dir)?;
        let seq = state.seq.fetch_add(1, Ordering::Relaxed) + 1;
        Ok(table_dir.join(store::regular_segment_filename(seq)))
    }

    pub fn next_partition_segment_path(
        &self,
        table_dir: &Path,
        partition_key: &str,
    ) -> Result<PathBuf> {
        let state = self.get_or_create_state_for_table_dir(table_dir)?;
        let seq = {
            let mut partition_seq = state.partition_seq.lock().unwrap();
            let entry = partition_seq.entry(partition_key.to_string()).or_insert(0);
            *entry += 1;
            *entry
        };
        Ok(table_dir.join(store::partition_segment_filename(partition_key, seq)))
    }

    pub fn next_replacement_segment_path(
        &self,
        table_dir: &Path,
        partition_key: Option<&str>,
    ) -> Result<PathBuf> {
        store::next_segment_path_by_scan(table_dir, partition_key)
    }

    pub fn open_reader(&self, path: &Path) -> Result<SegmentReader> {
        if self.requires_owned_reader(path) {
            return self.open_mutable_reader_owned(path);
        }

        match SegmentReader::open(path) {
            Ok(reader) => Ok(reader),
            Err(err) if is_transient_footer_error(&err) => self.open_mutable_reader_owned(path),
            Err(err) => Err(err),
        }
    }

    pub fn open_reader_cached(&self, path: &Path) -> Result<Arc<SegmentReader>> {
        // Active writers can change their data and sidecar footer at any time, so
        // readers for them must remain uncached snapshots. A small *inactive*
        // segment may be resumed later, but is stable until that happens; cache an
        // owned snapshot for it and let recovery invalidate the cache before append.
        if self.is_mutable_segment_path(path) {
            self.invalidate_reader(path);
            return Ok(Arc::new(self.open_mutable_reader_owned(path)?));
        }

        if let Some(reader) = self.reader_cache.lock().unwrap().get(path) {
            return Ok(reader);
        }

        let reader = match if self.requires_owned_reader(path) {
            SegmentReader::open_owned(path)
        } else {
            SegmentReader::open(path)
        } {
            Ok(reader) => reader,
            Err(err) if is_transient_footer_error(&err) => self.open_mutable_reader_owned(path)?,
            Err(err) => return Err(err),
        };
        Ok(self
            .reader_cache
            .lock()
            .unwrap()
            .put(path.to_path_buf(), reader))
    }

    pub fn invalidate_reader(&self, path: &Path) {
        self.reader_cache.lock().unwrap().invalidate(path);
        self.cluster_meta_cache.lock().unwrap().invalidate(path);
    }

    pub fn invalidate_readers_under(&self, dir: &Path) {
        self.reader_cache.lock().unwrap().invalidate_under(dir);
        self.cluster_meta_cache
            .lock()
            .unwrap()
            .invalidate_under(dir);
    }

    /// Read-side metadata cache for a sealed segment's cluster-key column zone map.
    ///
    /// Returns the cached zone map for `col_idx` in `path`, deriving and caching it
    /// on first access. Returns `None` when the segment has no usable bounds for the
    /// column (empty/null). Mutable (unsealed) segments are never cached because their
    /// footer can still change. The cache shares the reader-cache invalidation
    /// lifecycle, so compaction/seal/delete automatically clears stale entries.
    pub fn cluster_zone_map_cached(
        &self,
        path: &Path,
        col_idx: usize,
    ) -> Option<crate::storage::partition::pruning::ZoneMap> {
        if self.is_mutable_segment_path(path) {
            return self.derive_cluster_zone_map(path, col_idx);
        }

        if let Some(cached) = self.cluster_meta_cache.lock().unwrap().get(path, col_idx) {
            return cached;
        }

        let derived = self.derive_cluster_zone_map(path, col_idx);
        self.cluster_meta_cache
            .lock()
            .unwrap()
            .put(path.to_path_buf(), col_idx, derived.clone());
        derived
    }

    fn derive_cluster_zone_map(
        &self,
        path: &Path,
        col_idx: usize,
    ) -> Option<crate::storage::partition::pruning::ZoneMap> {
        let reader = self.open_reader_cached(path).ok()?;
        let zm = reader.segment_zone_maps().get(col_idx)?;
        if zm.row_count == 0 || zm.min_value.is_null() || zm.max_value.is_null() {
            return None;
        }
        Some(zm.clone())
    }

    pub fn remove_segment_file(&self, path: &Path) -> Result<()> {
        self.invalidate_reader(path);
        std::fs::remove_file(path)?;
        remove_sidecar_meta(path)?;
        Ok(())
    }

    pub fn replace_segments_with_tmp(
        &self,
        tmp_path: &Path,
        final_path: &Path,
        old_segments: &[PathBuf],
    ) -> Result<()> {
        if final_path.exists() {
            return Err(TaibaiError::Storage(format!(
                "segment target already exists: {}",
                final_path.display()
            )));
        }

        std::fs::rename(tmp_path, final_path)?;
        for old_seg in old_segments {
            if old_seg != final_path {
                self.remove_segment_file(old_seg)?;
            }
        }
        let _ = self.open_reader_cached(final_path);
        Ok(())
    }

    pub fn rewrite_delete_footer(
        &self,
        path: &Path,
        bitmap: &[u8],
        deleted_count: u64,
    ) -> Result<()> {
        self.invalidate_reader(path);
        SegmentReader::rewrite_footer_with_deletes(path, bitmap, deleted_count)?;
        if let Some(state) = self.state_for_segment_path(path) {
            let mut active_guard = state.active_segments.lock().unwrap();
            for segment in active_guard.values_mut() {
                if same_segment_path(&segment.path, path) {
                    segment.delete_bitmap = bitmap.to_vec();
                    segment.deleted_row_count = deleted_count;
                    segment.dirty_meta = true;
                    break;
                }
            }
        }
        Ok(())
    }

    pub fn with_table_mutation_lock<T>(
        &self,
        db: &str,
        table: &str,
        f: impl FnOnce() -> Result<T>,
    ) -> Result<T> {
        let key = table_key(db, table);
        let state = self.get_or_create_state(&key);
        let _guard = state.mutation_lock.lock().unwrap();
        f()
    }

    fn open_mutable_reader_owned(&self, path: &Path) -> Result<SegmentReader> {
        let Some(state) = self.state_for_segment_path(path) else {
            return SegmentReader::open_owned(path);
        };

        let mut active_guard = state.active_segments.lock().unwrap();
        for segment in active_guard.values_mut() {
            if same_segment_path(&segment.path, path) {
                if segment.dirty_meta {
                    segment.flush_meta(&|path| self.invalidate_reader(path))?;
                }
                return SegmentReader::open_owned(path);
            }
        }
        SegmentReader::open_owned(path)
    }

    fn is_mutable_segment_path(&self, path: &Path) -> bool {
        self.is_active_segment_path(path)
    }

    fn requires_owned_reader(&self, path: &Path) -> bool {
        if self.is_mutable_segment_path(path) {
            return true;
        }
        std::fs::metadata(path)
            .map(|meta| meta.len() < self.min_segment_seal_size())
            .unwrap_or(false)
    }

    fn is_active_segment_path(&self, path: &Path) -> bool {
        let Some(state) = self.state_for_segment_path(path) else {
            return false;
        };

        let active_guard = state.active_segments.lock().unwrap();
        active_guard
            .values()
            .any(|segment| same_segment_path(&segment.path, path))
    }

    fn state_for_segment_path(&self, path: &Path) -> Option<Arc<TableSegmentState>> {
        let tables = self.tables.read().unwrap();
        let table_dir = path.parent()?;
        let key = self.table_state_key_from_dir(table_dir).ok()?;
        tables.get(&key).cloned()
    }

    pub fn write_new_segment(
        &self,
        path: &Path,
        schema: Schema,
        row_group_size: usize,
        batch: &RecordBatch,
    ) -> Result<()> {
        let mut writer = SegmentWriter::create(path, schema, row_group_size)?;
        writer.write_batch(batch)?;
        writer.seal()?;
        Ok(())
    }

    pub fn write_wal(&self, table_dir: &Path, batch: &RecordBatch) -> Result<()> {
        let state = self.get_or_create_state_for_table_dir(table_dir)?;

        let wal_data = crate::sql::execution::wal_helper::serialize_batch_for_wal(batch);
        let txn_id = self.alloc_txn_id();

        let mut wal_guard = state.wal_writer.lock().unwrap();
        if wal_guard.is_none() {
            let wal_dir = table_dir.join("wal");
            let writer = WalWriter::open(&wal_dir, 64 * 1024 * 1024)?;
            *wal_guard = Some(writer);
        }
        let wal_writer = wal_guard.as_mut().unwrap();
        wal_writer.write_compact(txn_id, &wal_data, false)?;
        Ok(())
    }

    pub fn sync_all_wals(&self) {
        let tables = self.tables.read().unwrap();
        for state in tables.values() {
            let mut wal_guard = state.wal_writer.lock().unwrap();
            if let Some(ref mut w) = *wal_guard {
                let _ = w.sync();
            }
        }
    }

    pub fn clear_wal(&self, table_dir: &Path) -> Result<()> {
        let state = self.get_or_create_state_for_table_dir(table_dir)?;
        let mut wal_guard = state.wal_writer.lock().unwrap();
        if let Some(ref mut w) = *wal_guard {
            w.clear()?;
        }
        Ok(())
    }

    pub fn clear_active_segments(&self, db: &str, table: &str) {
        let key = table_key(db, table);
        let tables = self.tables.read().unwrap();
        if let Some(state) = tables.get(&key) {
            state.active_segments.lock().unwrap().clear();
        }
    }

    pub fn remove_wal(&self, table_dir: &Path) {
        let Ok(key) = self.table_state_key_from_dir(table_dir) else {
            return;
        };
        let tables = self.tables.write().unwrap();
        if let Some(state) = tables.get(&key) {
            let mut wal_guard = state.wal_writer.lock().unwrap();
            *wal_guard = None;
        }
    }

    pub fn remove_table_state(&self, db: &str, table: &str) {
        let key = table_key(db, table);
        let mut tables = self.tables.write().unwrap();
        if let Some(state) = tables.remove(&key) {
            state.retired.store(true, Ordering::Release);
            let mut wal_guard = state.wal_writer.lock().unwrap();
            *wal_guard = None;
            state.active_segments.lock().unwrap().clear();
        }
    }

    pub fn rename_table_state(&self, db: &str, old_name: &str, new_name: &str) {
        let old_key = table_key(db, old_name);
        let new_key = table_key(db, new_name);
        let mut tables = self.tables.write().unwrap();
        if let Some(state) = tables.remove(&old_key) {
            tables.insert(new_key, state);
        }
    }

    pub fn remove_database_state(&self, db: &str) {
        let prefix = format!("{}:", db);
        let mut tables = self.tables.write().unwrap();
        let keys: Vec<String> = tables
            .keys()
            .filter(|key| key.starts_with(&prefix))
            .cloned()
            .collect();
        for key in keys {
            if let Some(state) = tables.remove(&key) {
                state.retired.store(true, Ordering::Release);
                let mut wal_guard = state.wal_writer.lock().unwrap();
                *wal_guard = None;
                state.active_segments.lock().unwrap().clear();
            }
        }
    }

    pub fn clear_all_wals(&self) {
        let tables = self.tables.read().unwrap();
        for state in tables.values() {
            let mut wal_guard = state.wal_writer.lock().unwrap();
            if let Some(ref mut w) = *wal_guard {
                let _ = w.clear();
            }
        }
    }

    pub fn bulk_insert_batch(
        &self,
        db: &str,
        table: &str,
        batch: RecordBatch,
        _disable_wal: bool,
    ) -> Result<u64> {
        self.bulk_insert_batch_with_partition_inner(db, table, "", batch, true)
    }

    pub fn bulk_insert_batch_deferred(
        &self,
        db: &str,
        table: &str,
        batch: RecordBatch,
        _disable_wal: bool,
    ) -> Result<u64> {
        self.bulk_insert_batch_with_partition_inner(db, table, "", batch, false)
    }

    pub fn bulk_insert_batch_with_partition(
        &self,
        db: &str,
        table: &str,
        partition_key: &str,
        batch: RecordBatch,
    ) -> Result<u64> {
        self.bulk_insert_batch_with_partition_inner(db, table, partition_key, batch, true)
    }

    pub fn bulk_insert_batch_with_partition_deferred(
        &self,
        db: &str,
        table: &str,
        partition_key: &str,
        batch: RecordBatch,
    ) -> Result<u64> {
        self.bulk_insert_batch_with_partition_inner(db, table, partition_key, batch, false)
    }

    fn bulk_insert_batch_with_partition_inner(
        &self,
        db: &str,
        table: &str,
        partition_key: &str,
        batch: RecordBatch,
        flush_immediately: bool,
    ) -> Result<u64> {
        let row_count = batch.num_rows() as u64;
        if row_count == 0 {
            return Ok(0);
        }
        let table_dir = self.data_dir.join(db).join(table);
        std::fs::create_dir_all(&table_dir)?;
        let key = table_key(db, table);
        let state = self.get_or_create_state(&key);
        let _mutation_guard = state.mutation_lock.lock().unwrap();
        ensure_state_not_retired(&state, db, table)?;

        {
            let mut active_guard = state.active_segments.lock().unwrap();
            self.get_or_create_active_segment_for_partition_locked(
                &table_dir,
                &batch.schema,
                partition_key,
                &mut active_guard,
            )?;

            let seg = active_guard.get_mut(partition_key).unwrap();
            seg.append_batch(&batch, &|path| self.invalidate_reader(path))?;

            if seg.current_size >= self.max_segment_size() {
                seg.seal(&|path| self.invalidate_reader(path))?;
                active_guard.remove(partition_key);
            } else if flush_immediately && (seg.buffered_rows > 0 || seg.total_rows > 0) {
                seg.flush_buffer(&|path| self.invalidate_reader(path))?;
                seg.flush_meta(&|path| self.invalidate_reader(path))?;
            }
        }

        Ok(row_count)
    }

    fn get_or_create_active_segment_for_partition_locked(
        &self,
        table_dir: &Path,
        schema: &Schema,
        partition_key: &str,
        active_guard: &mut std::sync::MutexGuard<HashMap<String, ActiveSegmentWriter>>,
    ) -> Result<()> {
        if active_guard.contains_key(partition_key) {
            return Ok(());
        }

        let unsealed = if partition_key.is_empty() {
            Self::find_unsealed_segment(table_dir, self.min_segment_seal_size())?
        } else {
            Self::find_unsealed_partition_segment(
                table_dir,
                partition_key,
                self.min_segment_seal_size(),
            )?
        };

        let active = if let Some((seg_path, existing_schema, existing_rows)) = unsealed {
            if existing_schema.columns.len() == schema.columns.len() {
                tracing::info!(
                    path = %seg_path.display(),
                    partition_key,
                    existing_rows,
                    "resuming unsealed segment for append"
                );
                if let Some(recovered) = self.recover_active_segment(&seg_path, &existing_schema)? {
                    recovered
                } else {
                    self.create_new_active_segment(table_dir, schema, partition_key)?
                }
            } else {
                self.create_new_active_segment(table_dir, schema, partition_key)?
            }
        } else {
            self.create_new_active_segment(table_dir, schema, partition_key)?
        };

        active_guard.insert(partition_key.to_string(), active);
        Ok(())
    }

    fn create_new_active_segment(
        &self,
        table_dir: &Path,
        schema: &Schema,
        partition_key: &str,
    ) -> Result<ActiveSegmentWriter> {
        let seg_path = if partition_key.is_empty() {
            self.next_segment_path(table_dir, "")?
        } else {
            self.next_partition_segment_path(table_dir, partition_key)?
        };

        remove_sidecar_meta(&seg_path)?;
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&seg_path)?;
        file.write_all(&encode_segment_header())?;

        tracing::info!(
            path = %seg_path.display(),
            partition_key,
            "created new active segment"
        );

        Ok(ActiveSegmentWriter {
            file: std::io::BufWriter::with_capacity(SEGMENT_BUFWRITER_CAP, file),
            path: seg_path,
            schema: schema.clone(),
            row_groups: Vec::new(),
            current_size: SEGMENT_HEADER_SIZE as u64,
            total_rows: 0,
            delete_bitmap: Vec::new(),
            deleted_row_count: 0,
            buffered_batch: None,
            buffered_rows: 0,
            dirty_meta: false,
            data_end: SEGMENT_HEADER_SIZE as u64,
            sidecar_row_groups_flushed: 0,
            last_write_at: Instant::now(),
        })
    }

    pub fn find_unsealed_partition_segment(
        table_dir: &Path,
        partition_key: &str,
        min_seal_size: u64,
    ) -> Result<Option<(PathBuf, Schema, u64)>> {
        let _pattern = format!("p{}_*", partition_key);
        let segments: Vec<PathBuf> = if let Ok(entries) = std::fs::read_dir(table_dir) {
            entries
                .filter_map(|e| e.ok())
                .filter(|e| {
                    let file_name = e.file_name();
                    let name = file_name.to_string_lossy();
                    name.starts_with(&format!("p{}", partition_key))
                        && name.ends_with(".dat")
                        && name.contains('_')
                })
                .map(|e| e.path())
                .collect()
        } else {
            Vec::new()
        };

        for seg_path in segments.iter().rev() {
            let file_len = match std::fs::metadata(seg_path) {
                Ok(m) => m.len(),
                Err(_) => continue,
            };

            if file_len < SEGMENT_HEADER_SIZE as u64 {
                continue;
            }

            if let Ok(reader) = SegmentReader::open(seg_path) {
                if file_len < min_seal_size && reader.row_count() > 0 {
                    return Ok(Some((
                        seg_path.clone(),
                        reader.schema().clone(),
                        reader.row_count(),
                    )));
                }
            }
        }

        Ok(None)
    }

    fn recover_active_segment(
        &self,
        seg_path: &Path,
        schema: &Schema,
    ) -> Result<Option<ActiveSegmentWriter>> {
        let file_len = std::fs::metadata(seg_path)?.len();

        if file_len < SEGMENT_HEADER_SIZE as u64 {
            return Ok(None);
        }

        self.invalidate_reader(seg_path);

        let reader = SegmentReader::open_owned(seg_path)?;
        let data_end = reader.data_end();

        let row_groups = reader.footer_row_groups_info();

        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .read(true)
            .open(seg_path)?;

        file.set_len(data_end)?;
        file.seek(SeekFrom::Start(data_end))?;

        tracing::info!(
            path = %seg_path.display(),
            data_end,
            existing_rows = reader.row_count(),
            row_groups = row_groups.len(),
            "recovered active segment from unsealed file"
        );

        Ok(Some(ActiveSegmentWriter {
            file: std::io::BufWriter::with_capacity(SEGMENT_BUFWRITER_CAP, file),
            path: seg_path.to_path_buf(),
            schema: schema.clone(),
            row_groups,
            current_size: data_end,
            total_rows: reader.row_count(),
            delete_bitmap: reader.delete_bitmap().to_vec(),
            deleted_row_count: reader.deleted_row_count(),
            buffered_batch: None,
            buffered_rows: 0,
            dirty_meta: true,
            data_end,
            sidecar_row_groups_flushed: 0,
            last_write_at: Instant::now(),
        }))
    }

    pub fn bulk_insert_payload(&self, db: &str, payload: &[u8], _disable_wal: bool) -> Result<u64> {
        use rayon::prelude::*;

        let req = parse_direct_bulk_payload(payload)?;
        if req.row_count == 0 {
            return Ok(0);
        }

        let table_dir = self.data_dir.join(db).join(req.table_name);
        std::fs::create_dir_all(&table_dir)?;

        let schema = {
            let col_defs: Vec<crate::types::batch::ColumnDef> = req
                .columns
                .iter()
                .map(|c| {
                    crate::types::batch::ColumnDef::new(c.name, c.data_type)
                        .with_scale(c.scale)
                        .with_precision(c.precision)
                })
                .collect();
            Schema::new(col_defs)
        };

        let key = table_key(db, req.table_name);
        let state = self.get_or_create_state(&key);

        let compression_level = bulk_compression_level();
        let compressed_columns: Vec<(Vec<u8>, u64)> = req
            .columns
            .par_iter()
            .map(|col| {
                let mut encoded = Vec::with_capacity(4 + col.data.len());
                encoded.extend_from_slice(&0u32.to_le_bytes());
                encoded.extend_from_slice(col.data);
                let orig_len = encoded.len() as u64;
                let compressed = zstd::bulk::compress(&encoded, compression_level)
                    .map_err(|e| TaibaiError::Storage(format!("zstd error: {}", e)))?;
                Ok((compressed, orig_len))
            })
            .collect::<Result<Vec<_>>>()?;

        {
            let _mutation_guard = state.mutation_lock.lock().unwrap();
            ensure_state_not_retired(&state, db, req.table_name)?;

            let mut active_guard = state.active_segments.lock().unwrap();
            self.get_or_create_active_segment_for_partition_locked(
                &table_dir,
                &schema,
                "",
                &mut active_guard,
            )?;

            let seg = active_guard.get_mut("").unwrap();

            seg.append_direct_row_group(
                &req.columns,
                &compressed_columns,
                req.row_count as u64,
                true,
                None,
            )?;

            if seg.current_size >= self.max_segment_size() {
                seg.seal(&|path| self.invalidate_reader(path))?;
                active_guard.remove("");
            } else {
                seg.flush_meta(&|path| self.invalidate_reader(path))?;
            }
        }

        Ok(req.row_count as u64)
    }

    pub fn bulk_insert_payload_with_catalog(
        &self,
        db: &str,
        payload: &[u8],
        _disable_wal: bool,
        catalogs: &crate::storage::catalog::schema::DatabaseCatalog,
        flush_immediately: bool,
    ) -> Result<u64> {
        let req = parse_direct_bulk_payload(payload)?;
        if req.row_count == 0 {
            return Ok(0);
        }

        let (schema, table_meta) = {
            let table_meta = catalogs.get_table(req.table_name).ok_or_else(|| {
                TaibaiError::Sql(format!("table '{}.{}' does not exist", db, req.table_name))
            })?;
            (Schema::new(table_meta.columns.clone()), table_meta.clone())
        };

        self.validate_direct_bulk_request(&req, &schema)?;
        self.append_direct_bulk_request(db, &req, &schema, &table_meta, flush_immediately)
    }

    fn validate_direct_bulk_request(
        &self,
        req: &DirectBulkRequest<'_>,
        schema: &Schema,
    ) -> Result<()> {
        if schema.num_columns() != req.columns.len() {
            return Err(TaibaiError::Protocol(format!(
                "bulk column count mismatch: schema has {} columns but payload has {}",
                schema.num_columns(),
                req.columns.len()
            )));
        }
        for (idx, (col_def, payload_col)) in
            schema.columns.iter().zip(req.columns.iter()).enumerate()
        {
            if col_def.data_type != payload_col.data_type {
                return Err(TaibaiError::Protocol(format!(
                    "bulk column {} type mismatch for '{}': schema={} payload={}",
                    idx,
                    col_def.name,
                    col_def.data_type.display_name(),
                    payload_col.data_type.display_name()
                )));
            }
            if !col_def.name.eq_ignore_ascii_case(payload_col.name) {
                return Err(TaibaiError::Protocol(format!(
                    "bulk column {} name mismatch: schema='{}' payload='{}'",
                    idx, col_def.name, payload_col.name
                )));
            }
            if !payload_col.nulls.is_empty() {
                return Err(TaibaiError::Protocol(
                    "direct bulk path currently supports all-valid columns only".into(),
                ));
            }
        }
        Ok(())
    }

    fn append_direct_bulk_request(
        &self,
        db: &str,
        req: &DirectBulkRequest<'_>,
        schema: &Schema,
        table_meta: &crate::storage::catalog::schema::TableMeta,
        flush_immediately: bool,
    ) -> Result<u64> {
        let table_dir = self.data_dir.join(db).join(req.table_name);
        std::fs::create_dir_all(&table_dir)?;
        let partition_groups = self.direct_bulk_partition_groups(req, table_meta)?;
        let prepared_groups = self.prepare_direct_bulk_groups(req, partition_groups)?;
        let key = table_key(db, req.table_name);
        let state = self.get_or_create_state(&key);
        let build_value_sets = flush_immediately;
        let zone_map_columns = if flush_immediately {
            None
        } else {
            Some(self.direct_bulk_key_column_indices(schema, table_meta))
        };

        let _mutation_guard = state.mutation_lock.lock().unwrap();
        ensure_state_not_retired(&state, db, req.table_name)?;

        let mut written = 0u64;
        let mut active_guard = state.active_segments.lock().unwrap();
        for group in prepared_groups {
            self.get_or_create_active_segment_for_partition_locked(
                &table_dir,
                schema,
                &group.partition_key,
                &mut active_guard,
            )?;
            let seg = active_guard.get_mut(&group.partition_key).unwrap();
            match &group.columns {
                PreparedDirectColumns::Borrowed(columns) => {
                    seg.append_direct_row_group(
                        columns,
                        &group.compressed_columns,
                        group.row_count as u64,
                        build_value_sets,
                        zone_map_columns.as_deref(),
                    )?;
                }
                PreparedDirectColumns::Owned(storage) => {
                    let columns: Vec<DirectBulkColumn<'_>> = storage
                        .iter()
                        .map(|col| DirectBulkColumn {
                            name: col.name,
                            data_type: col.data_type,
                            scale: col.scale,
                            precision: col.precision,
                            data: &col.data,
                            nulls: &col.nulls,
                            variable_ranges: col.variable_ranges.clone(),
                        })
                        .collect();
                    seg.append_direct_row_group(
                        &columns,
                        &group.compressed_columns,
                        group.row_count as u64,
                        build_value_sets,
                        zone_map_columns.as_deref(),
                    )?;
                }
            }
            if seg.current_size >= self.max_segment_size() {
                seg.seal(&|path| self.invalidate_reader(path))?;
                active_guard.remove(&group.partition_key);
            } else if flush_immediately {
                seg.flush_meta(&|path| self.invalidate_reader(path))?;
            }
            written += group.row_count as u64;
        }
        Ok(written)
    }

    fn direct_bulk_key_column_indices(
        &self,
        schema: &Schema,
        table_meta: &crate::storage::catalog::schema::TableMeta,
    ) -> Vec<usize> {
        let mut indices = Vec::new();
        if let Some(idx) = table_meta.get_partition_column_index_cached() {
            indices.push(idx);
        }
        for key in &table_meta.cluster_key {
            if let Some(idx) = schema.index_of(key) {
                if !indices.contains(&idx) {
                    indices.push(idx);
                }
            }
        }
        indices
    }

    fn direct_bulk_partition_groups(
        &self,
        req: &DirectBulkRequest<'_>,
        table_meta: &crate::storage::catalog::schema::TableMeta,
    ) -> Result<Vec<(String, Vec<usize>)>> {
        if !table_meta.is_partitioned() {
            return Ok(vec![(String::new(), (0..req.row_count).collect())]);
        }
        let pk_idx = table_meta
            .get_partition_column_index_cached()
            .ok_or_else(|| {
                TaibaiError::Sql(format!(
                    "partitioned table '{}' has no cached partition column index",
                    table_meta.name
                ))
            })?;
        let pk_col = req.columns.get(pk_idx).ok_or_else(|| {
            TaibaiError::Protocol(format!(
                "partition column index {} out of bulk payload",
                pk_idx
            ))
        })?;
        let strategy = table_meta.partition_strategy();
        let mut groups: HashMap<String, Vec<usize>> = HashMap::new();
        let mut order = Vec::new();
        for row_idx in 0..req.row_count {
            let value = helpers::direct_bulk_column_scalar_at(pk_col, row_idx)?;
            let partition_key = compute_partition_key_from_scalar(&strategy, &value);
            if !groups.contains_key(&partition_key) {
                order.push(partition_key.clone());
                groups.insert(partition_key.clone(), Vec::new());
            }
            groups.get_mut(&partition_key).unwrap().push(row_idx);
        }
        Ok(order
            .into_iter()
            .filter_map(|key| groups.remove(&key).map(|rows| (key, rows)))
            .collect())
    }

    fn prepare_direct_bulk_groups<'a>(
        &self,
        req: &'a DirectBulkRequest<'a>,
        partition_groups: Vec<(String, Vec<usize>)>,
    ) -> Result<Vec<PreparedDirectBulkGroup<'a>>> {
        let mut prepared = Vec::with_capacity(partition_groups.len());
        for (partition_key, rows) in partition_groups {
            if rows.is_empty() {
                continue;
            }
            if self.direct_bulk_rows_are_full_range(&rows, req.row_count) {
                let columns: Vec<DirectBulkColumn<'a>> = req
                    .columns
                    .iter()
                    .map(|col| DirectBulkColumn {
                        name: col.name,
                        data_type: col.data_type,
                        scale: col.scale,
                        precision: col.precision,
                        data: col.data,
                        nulls: col.nulls,
                        variable_ranges: col.variable_ranges.clone(),
                    })
                    .collect();
                let compressed_columns = self.compress_direct_bulk_columns(&columns)?;
                prepared.push(PreparedDirectBulkGroup {
                    partition_key,
                    row_count: req.row_count,
                    columns: PreparedDirectColumns::Borrowed(columns),
                    compressed_columns,
                });
            } else {
                let storage = self.direct_bulk_select_columns(req, &rows)?;
                let columns: Vec<DirectBulkColumn<'_>> = storage
                    .iter()
                    .map(|col| DirectBulkColumn {
                        name: col.name,
                        data_type: col.data_type,
                        scale: col.scale,
                        precision: col.precision,
                        data: &col.data,
                        nulls: &col.nulls,
                        variable_ranges: col.variable_ranges.clone(),
                    })
                    .collect();
                let compressed_columns = self.compress_direct_bulk_columns(&columns)?;
                prepared.push(PreparedDirectBulkGroup {
                    partition_key,
                    row_count: rows.len(),
                    columns: PreparedDirectColumns::Owned(storage),
                    compressed_columns,
                });
            }
        }
        Ok(prepared)
    }

    fn direct_bulk_rows_are_full_range(&self, rows: &[usize], row_count: usize) -> bool {
        rows.len() == row_count
            && rows
                .iter()
                .enumerate()
                .all(|(idx, &row_idx)| idx == row_idx)
    }

    fn direct_bulk_select_columns<'a>(
        &self,
        req: &'a DirectBulkRequest<'a>,
        rows: &[usize],
    ) -> Result<Vec<OwnedDirectBulkColumn<'a>>> {
        req.columns
            .iter()
            .map(|col| self.direct_bulk_select_column(col, rows))
            .collect()
    }

    fn direct_bulk_select_column<'a>(
        &self,
        col: &DirectBulkColumn<'a>,
        rows: &[usize],
    ) -> Result<OwnedDirectBulkColumn<'a>> {
        let mut data = Vec::new();
        let mut variable_ranges = col
            .variable_ranges
            .as_ref()
            .map(|_| Vec::with_capacity(rows.len()));
        if let Some(width) = col.data_type.fixed_size() {
            data.reserve(rows.len() * width);
            for &row_idx in rows {
                let offset = row_idx
                    .checked_mul(width)
                    .ok_or_else(|| TaibaiError::Protocol("bulk column offset overflow".into()))?;
                let end = offset + width;
                if end > col.data.len() {
                    return Err(TaibaiError::Protocol(format!(
                        "bulk column '{}' row {} is truncated",
                        col.name, row_idx
                    )));
                }
                data.extend_from_slice(&col.data[offset..end]);
            }
        } else {
            for &row_idx in rows {
                let (offset, len) = col
                    .variable_ranges
                    .as_ref()
                    .and_then(|ranges| ranges.get(row_idx).copied())
                    .ok_or_else(|| {
                        TaibaiError::Protocol(format!(
                            "bulk column '{}' row {} not found",
                            col.name, row_idx
                        ))
                    })?;
                if offset + len > col.data.len() {
                    return Err(TaibaiError::Protocol(format!(
                        "bulk column '{}' variable data truncated",
                        col.name
                    )));
                }
                data.extend_from_slice(&(len as u32).to_le_bytes());
                let value_offset = data.len();
                data.extend_from_slice(&col.data[offset..offset + len]);
                if let Some(ranges) = variable_ranges.as_mut() {
                    ranges.push((value_offset, len));
                }
            }
        }
        Ok(OwnedDirectBulkColumn {
            name: col.name,
            data_type: col.data_type,
            scale: col.scale,
            precision: col.precision,
            data,
            nulls: Vec::new(),
            variable_ranges,
        })
    }

    fn compress_direct_bulk_columns(
        &self,
        columns: &[DirectBulkColumn<'_>],
    ) -> Result<Vec<(Vec<u8>, u64)>> {
        use rayon::prelude::*;
        let compression_level = bulk_compression_level();
        columns
            .par_iter()
            .map(|col| {
                let mut encoded = Vec::with_capacity(4 + col.data.len());
                encoded.extend_from_slice(&0u32.to_le_bytes());
                encoded.extend_from_slice(col.data);
                let orig_len = encoded.len() as u64;
                let compressed = zstd::bulk::compress(&encoded, compression_level)
                    .map_err(|e| TaibaiError::Storage(format!("zstd error: {}", e)))?;
                Ok((compressed, orig_len))
            })
            .collect::<Result<Vec<_>>>()
    }

    pub fn bulk_insert_batch_partitioned(
        &self,
        db: &str,
        table: &str,
        table_meta: &crate::storage::catalog::schema::TableMeta,
        batch: RecordBatch,
    ) -> Result<u64> {
        self.bulk_insert_batch_partitioned_inner(db, table, table_meta, batch, true)
    }

    pub fn bulk_insert_batch_partitioned_deferred(
        &self,
        db: &str,
        table: &str,
        table_meta: &crate::storage::catalog::schema::TableMeta,
        batch: RecordBatch,
    ) -> Result<u64> {
        self.bulk_insert_batch_partitioned_inner(db, table, table_meta, batch, false)
    }

    fn bulk_insert_batch_partitioned_inner(
        &self,
        db: &str,
        table: &str,
        table_meta: &crate::storage::catalog::schema::TableMeta,
        batch: RecordBatch,
        flush_immediately: bool,
    ) -> Result<u64> {
        if !table_meta.is_partitioned() {
            return self.bulk_insert_batch_with_partition_inner(
                db,
                table,
                "",
                batch,
                flush_immediately,
            );
        }

        let pk_idx = match table_meta.get_partition_column_index_cached() {
            Some(idx) => idx,
            None => {
                return self.bulk_insert_batch_with_partition_inner(
                    db,
                    table,
                    "",
                    batch,
                    flush_immediately,
                )
            }
        };
        let mut key_order = Vec::new();
        let mut groups: HashMap<String, Vec<bool>> = HashMap::new();
        for row_idx in 0..batch.num_rows() {
            let val = batch
                .scalar(row_idx, pk_idx)
                .unwrap_or(ScalarValue::Int64(None));
            let key = compute_partition_key_from_scalar(&table_meta.partition_strategy(), &val);
            if !groups.contains_key(&key) {
                key_order.push(key.clone());
                groups.insert(key.clone(), vec![false; batch.num_rows()]);
            }
            if let Some(mask) = groups.get_mut(&key) {
                mask[row_idx] = true;
            }
        }

        let mut written = 0;
        for partition_key in key_order {
            let Some(mask) = groups.remove(&partition_key) else {
                continue;
            };
            let match_count = mask.iter().filter(|keep| **keep).count();
            let part_batch = filter_batch_by_mask(&batch, &mask, match_count)?;
            written += self.bulk_insert_batch_with_partition_inner(
                db,
                table,
                &partition_key,
                part_batch,
                flush_immediately,
            )?;
        }
        Ok(written)
    }

    pub fn flush_all_active(&self) -> Result<u64> {
        self.sync_all_wals();

        let mut total = 0u64;
        let tables = self.tables.read().unwrap();
        for state in tables.values() {
            let mut active_guard = state.active_segments.lock().unwrap();
            let keys_to_seal: Vec<String> = active_guard
                .iter()
                .filter(|(_, seg)| {
                    seg.total_rows > 0 && seg.current_size >= self.min_segment_seal_size()
                })
                .map(|(k, _)| k.clone())
                .collect();

            for key in keys_to_seal {
                if let Some(seg) = active_guard.get_mut(&key) {
                    seg.flush_buffer(&|path| self.invalidate_reader(path))?;
                    seg.write_final_footer()?;
                    remove_sidecar_meta(&seg.path)?;
                    self.invalidate_reader(&seg.path);
                    tracing::info!(
                        path = %seg.path.display(),
                        size_mb = seg.current_size / (1024 * 1024),
                        rows = seg.total_rows,
                        partition_key = key,
                        "sealing segment on flush (meets minimum size threshold)"
                    );
                    seg.finalize()?;
                    total += seg.total_rows;
                }
                active_guard.remove(&key);
            }

            for (_, seg) in active_guard.iter_mut() {
                if seg.total_rows > 0 || seg.buffered_rows > 0 {
                    seg.flush_buffer(&|path| self.invalidate_reader(path))?;
                    seg.flush_meta(&|path| self.invalidate_reader(path))?;
                    total += seg.total_rows;
                    tracing::info!(
                        path = %seg.path.display(),
                        size_mb = seg.current_size / (1024 * 1024),
                        rows = seg.total_rows,
                        buffered = seg.buffered_rows,
                        "active segment flushed but kept open (below minimum seal size)"
                    );
                }
            }
        }
        drop(tables);

        self.clear_all_wals();

        Ok(total)
    }

    fn periodic_flush_check(&self) -> Result<u64> {
        let now = Instant::now();
        let mut total = 0u64;
        let tables = self.tables.read().unwrap();
        for state in tables.values() {
            let mut active_guard = state.active_segments.lock().unwrap();
            for (_, seg) in active_guard.iter_mut() {
                if !seg.dirty_meta && seg.buffered_rows == 0 {
                    continue;
                }
                let idle = now.duration_since(seg.last_write_at).as_secs();
                if idle < FLUSH_IDLE_THRESHOLD_SECS {
                    continue;
                }
                if seg.buffered_rows > 0 {
                    seg.flush_buffer(&|path| self.invalidate_reader(path))?;
                }
                if seg.dirty_meta {
                    seg.flush_meta(&|path| self.invalidate_reader(path))?;
                }
                total += seg.total_rows;
            }
        }
        Ok(total)
    }

    pub fn begin_txn(&self) -> u64 {
        self.alloc_txn_id()
    }

    pub fn commit_txn(&self, txn_id: u64) -> Result<()> {
        self.sync_all_wals();
        tracing::debug!(txn_id, "transaction committed in storage layer");
        Ok(())
    }

    pub fn rollback_txn(&self, txn_id: u64) -> Result<()> {
        tracing::debug!(txn_id, "transaction rolled back in storage layer");
        Ok(())
    }

    pub fn write_txn_wal(&self, table_dir: &Path, txn_id: u64, batch: &RecordBatch) -> Result<()> {
        let state = self.get_or_create_state_for_table_dir(table_dir)?;

        let wal_data = crate::sql::execution::wal_helper::serialize_batch_for_wal(batch);

        let mut wal_guard = state.wal_writer.lock().unwrap();
        if wal_guard.is_none() {
            let wal_dir = table_dir.join("wal");
            let writer = WalWriter::open(&wal_dir, 64 * 1024 * 1024)?;
            *wal_guard = Some(writer);
        }
        let wal_writer = wal_guard.as_mut().unwrap();
        wal_writer.write_compact(txn_id, &wal_data, false)?;
        Ok(())
    }

    pub fn clear_txn_wal(&self, table_dir: &Path, txn_id: u64) -> Result<()> {
        let state = self.get_or_create_state_for_table_dir(table_dir)?;
        let mut wal_guard = state.wal_writer.lock().unwrap();
        if let Some(ref mut w) = *wal_guard {
            w.clear_entries_for_txn(txn_id)?;
        }
        Ok(())
    }

    pub fn sync_table_wal(&self, table_dir: &Path) -> Result<()> {
        let state = self.get_or_create_state_for_table_dir(table_dir)?;
        let mut wal_guard = state.wal_writer.lock().unwrap();
        if let Some(ref mut w) = *wal_guard {
            w.sync()?;
        }
        Ok(())
    }
}

fn cleanup_incomplete_compaction_files(table_dir: &Path) {
    let Ok(entries) = std::fs::read_dir(table_dir) else {
        return;
    };

    for entry in entries.flatten() {
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if !is_incomplete_compaction_file(name) {
            continue;
        }
        match std::fs::remove_file(&path) {
            Ok(()) => tracing::warn!(
                path = %path.display(),
                "removed incomplete compact temporary file during startup"
            ),
            Err(err) => tracing::warn!(
                path = %path.display(),
                error = %err,
                "failed to remove incomplete compact temporary file during startup"
            ),
        }
    }
}

fn is_incomplete_compaction_file(name: &str) -> bool {
    name.ends_with(".compact_tmp") || name.ends_with(".meta.compact.tmp")
}
