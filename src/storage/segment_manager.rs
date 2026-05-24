use std::collections::HashMap;
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::Instant;

use crate::common::error::{MonaxError, Result};
use crate::protocol::types::DataType;
use crate::storage::bloomfilter::BloomFilter;
use crate::storage::segment::{
    encode_segment_footer, encode_segment_header, RowGroupMeta, SegmentReader, SegmentWriter,
    SEGMENT_HEADER_SIZE,
};
use crate::storage::pruning::ZoneMap;
use crate::storage::wal::WalWriter;
use crate::types::batch::{RecordBatch, Schema};
const SEGMENT_BUFWRITER_CAP: usize = 512 * 1024;
const SQL_INSERT_ROW_GROUP_TARGET_ROWS: usize = 500_000;
const FLUSH_CHECK_INTERVAL_SECS: u64 = 10;
const FLUSH_IDLE_THRESHOLD_SECS: u64 = 5;
const FLUSH_ROW_THRESHOLD: u64 = 100_000;
const FLUSH_META_AFTER_ROW_GROUP: bool = true;

const DEFAULT_MIN_SEGMENT_SIZE: u64 = 256 * 1024 * 1024;
const DEFAULT_MAX_SEGMENT_SIZE: u64 = 1024 * 1024 * 1024;

pub struct SegmentConfig {
    pub min_segment_size: u64,
    pub max_segment_size: u64,
}

impl Default for SegmentConfig {
    fn default() -> Self {
        Self {
            min_segment_size: DEFAULT_MIN_SEGMENT_SIZE,
            max_segment_size: DEFAULT_MAX_SEGMENT_SIZE,
        }
    }
}

struct ActiveSegment {
    file: std::io::BufWriter<std::fs::File>,
    path: PathBuf,
    schema: Schema,
    row_groups: Vec<RowGroupMeta>,
    current_size: u64,
    total_rows: u64,
    buffered_batch: Option<RecordBatch>,
    buffered_rows: usize,
    dirty_meta: bool,
    data_end: u64,
    last_write_at: Instant,
}

impl ActiveSegment {
    fn is_sealed(&self) -> bool {
        !self.dirty_meta && self.buffered_rows == 0 && self.row_groups.is_empty()
    }
}

struct TableSegmentState {
    seq: AtomicU32,
    active_segments: Mutex<HashMap<String, ActiveSegment>>,
    wal_writer: Mutex<Option<WalWriter>>,
}

impl TableSegmentState {
    fn new(initial_seq: u32) -> Self {
        Self {
            seq: AtomicU32::new(initial_seq),
            active_segments: Mutex::new(HashMap::new()),
            wal_writer: Mutex::new(None),
        }
    }
}

pub struct SegmentManager {
    data_dir: PathBuf,
    tables: RwLock<HashMap<String, Arc<TableSegmentState>>>,
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
                            let max_seq = discover_max_seq(&tbl_dir);
                            let key = table_key(&db_name, &tbl_entry.file_name().to_string_lossy());
                            tables.insert(key, Arc::new(TableSegmentState::new(max_seq)));
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
            flush_stop,
            flush_paused,
            next_txn_id: AtomicU64::new(0),
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
            .name("monax-flush".into())
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

    pub fn discover_segments(table_dir: &Path) -> Result<Vec<PathBuf>> {
        if !table_dir.exists() {
            return Ok(Vec::new());
        }
        let mut files = Vec::new();
        for entry in std::fs::read_dir(table_dir)? {
            let entry = entry?;
            let path = entry.path();
            if let Some(name) = path.file_name() {
                let name = name.to_string_lossy();
                if (name.starts_with("seg_") || name.starts_with('p')) && name.ends_with(".dat") {
                    files.push(path);
                }
            }
        }
        files.sort();
        Ok(files)
    }

    pub fn find_unsealed_segment(table_dir: &Path, min_seal_size: u64) -> Result<Option<(PathBuf, Schema, u64)>> {
        let segments = Self::discover_segments(table_dir)?;
        
        for seg_path in segments.iter().rev() {
            let file_len = match std::fs::metadata(seg_path) {
                Ok(m) => m.len(),
                Err(_) => continue,
            };
            
            if file_len < (SEGMENT_HEADER_SIZE as u64) + 8 {
                continue;
            }
            
            if let Ok(reader) = SegmentReader::open(seg_path) {
                let seg_size = file_len;
                if seg_size < min_seal_size && reader.row_count() > 0 {
                    return Ok(Some((seg_path.clone(), reader.schema().clone(), reader.row_count())));
                }
            }
        }
        
        Ok(None)
    }

    pub fn next_segment_path(&self, table_dir: &Path, _partition_key: &str) -> Result<PathBuf> {
        let key = table_dir.to_string_lossy().to_string();
        let state = self.get_or_create_state(&key);
        let seq = state.seq.fetch_add(1, Ordering::Relaxed) + 1;
        Ok(table_dir.join(format!("seg_{:05}.dat", seq)))
    }

    pub fn next_partition_segment_path(
        &self,
        table_dir: &Path,
        partition_key: &str,
    ) -> Result<PathBuf> {
        let key = table_dir.to_string_lossy().to_string();
        let state = self.get_or_create_state(&key);
        let seq = state.seq.fetch_add(1, Ordering::Relaxed) + 1;
        Ok(table_dir.join(format!("p{}_{:04}.dat", partition_key, seq)))
    }

    pub fn open_reader(&self, path: &Path) -> Result<SegmentReader> {
        SegmentReader::open(path)
    }

    pub fn rewrite_delete_footer(
        &self,
        path: &Path,
        bitmap: &[u8],
        deleted_count: u64,
    ) -> Result<()> {
        SegmentReader::rewrite_footer_with_deletes(path, bitmap, deleted_count)
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
        let key = table_dir.to_string_lossy().to_string();
        let state = self.get_or_create_state(&key);

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
        let key = table_dir.to_string_lossy().to_string();
        let state = self.get_or_create_state(&key);
        let mut wal_guard = state.wal_writer.lock().unwrap();
        if let Some(ref mut w) = *wal_guard {
            w.clear()?;
        }
        Ok(())
    }

    pub fn remove_wal(&self, table_dir: &Path) {
        let key = table_dir.to_string_lossy().to_string();
        let tables = self.tables.write().unwrap();
        if let Some(state) = tables.get(&key) {
            let mut wal_guard = state.wal_writer.lock().unwrap();
            *wal_guard = None;
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
        self.bulk_insert_batch_with_partition(db, table, "", batch)
    }
    
    pub fn bulk_insert_batch_with_partition(
        &self,
        db: &str,
        table: &str,
        partition_key: &str,
        batch: RecordBatch,
    ) -> Result<u64> {
        let row_count = batch.num_rows() as u64;
        if row_count == 0 {
            return Ok(0);
        }
        let table_dir = self.data_dir.join(db).join(table);
        std::fs::create_dir_all(&table_dir)?;
        let key = table_key(db, table);
        let state = self.get_or_create_state(&key);

        {
            let mut active_guard = state.active_segments.lock().unwrap();
            self.get_or_create_active_segment_for_partition_locked(
                &table_dir,
                &batch.schema,
                partition_key,
                &mut active_guard,
            )?;
            
            let seg = active_guard.get_mut(partition_key).unwrap();
            let mut wal_keys_to_clear = self.append_to_segment_buffer(seg, &batch)?;

            let mut sealed = false;
            let mut need_flush_buffer = false;
            let mut need_flush_meta = false;
            
            if seg.current_size >= self.max_segment_size() {
                self.seal_segment(seg)?;
                active_guard.remove(partition_key);
                sealed = true;
            } else {
                if seg.dirty_meta {
                    let idle = Instant::now().duration_since(seg.last_write_at).as_secs();
                    if idle >= FLUSH_IDLE_THRESHOLD_SECS || seg.total_rows >= FLUSH_ROW_THRESHOLD {
                        need_flush_meta = true;
                    }
                }
                if seg.buffered_rows > 0 && !need_flush_meta {
                    let idle = Instant::now().duration_since(seg.last_write_at).as_secs();
                    if idle >= FLUSH_IDLE_THRESHOLD_SECS {
                        need_flush_buffer = true;
                        need_flush_meta = true;
                    }
                }
            }

            if need_flush_buffer && !sealed {
                let seg = active_guard.get_mut(partition_key).unwrap();
                if let Some(wk) = self.flush_segment_buffer(seg)? {
                    wal_keys_to_clear.push(wk);
                }
            }
            if need_flush_meta && !sealed {
                let seg = active_guard.get_mut(partition_key).unwrap();
                self.flush_meta(seg)?;
            }
        }

        Ok(row_count)
    }

    fn get_or_create_active_segment_for_partition_locked(
        &self,
        table_dir: &Path,
        schema: &Schema,
        partition_key: &str,
        active_guard: &mut std::sync::MutexGuard<HashMap<String, ActiveSegment>>,
    ) -> Result<()> {
        if active_guard.contains_key(partition_key) {
            return Ok(());
        }
        
        let unsealed = if partition_key.is_empty() {
            Self::find_unsealed_segment(table_dir, self.min_segment_seal_size())?
        } else {
            Self::find_unsealed_partition_segment(table_dir, partition_key, self.min_segment_seal_size())?
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
    ) -> Result<ActiveSegment> {
        let seg_path = if partition_key.is_empty() {
            self.next_segment_path(table_dir, "")?
        } else {
            self.next_partition_segment_path(table_dir, partition_key)?
        };
        
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
        
        Ok(ActiveSegment {
            file: std::io::BufWriter::with_capacity(SEGMENT_BUFWRITER_CAP, file),
            path: seg_path,
            schema: schema.clone(),
            row_groups: Vec::new(),
            current_size: SEGMENT_HEADER_SIZE as u64,
            total_rows: 0,
            buffered_batch: None,
            buffered_rows: 0,
            dirty_meta: false,
            data_end: SEGMENT_HEADER_SIZE as u64,
            last_write_at: Instant::now(),
        })
    }
    
    pub fn find_unsealed_partition_segment(
        table_dir: &Path,
        partition_key: &str,
        min_seal_size: u64,
    ) -> Result<Option<(PathBuf, Schema, u64)>> {
        let pattern = format!("p{}_*", partition_key);
        let segments: Vec<PathBuf> = if let Ok(entries) = std::fs::read_dir(table_dir) {
            entries
                .filter_map(|e| e.ok())
                .filter(|e| {
                    let file_name = e.file_name();
                    let name = file_name.to_string_lossy();
                    name.starts_with(&format!("p{}", partition_key)) && 
                        name.ends_with(".dat") &&
                        name.contains('_')
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
            
            if file_len < (SEGMENT_HEADER_SIZE as u64) + 8 {
                continue;
            }
            
            if let Ok(reader) = SegmentReader::open(seg_path) {
                if file_len < min_seal_size && reader.row_count() > 0 {
                    return Ok(Some((seg_path.clone(), reader.schema().clone(), reader.row_count())));
                }
            }
        }
        
        Ok(None)
    }

    fn recover_active_segment(&self, seg_path: &Path, schema: &Schema) -> Result<Option<ActiveSegment>> {
        let file_len = std::fs::metadata(seg_path)?.len();
        
        if file_len < (SEGMENT_HEADER_SIZE as u64) + 8 {
            return Ok(None);
        }
        
        let reader = SegmentReader::open(seg_path)?;
        let footer_offset = {
            let mut file = std::fs::File::open(seg_path)?;
            use std::io::Read;
            file.seek(SeekFrom::End(-8))?;
            let mut buf = [0u8; 8];
            file.read_exact(&mut buf)?;
            u64::from_le_bytes(buf)
        };
        
        let row_groups = reader.footer_row_groups_info();
        
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .read(true)
            .open(seg_path)?;
        
        file.set_len(footer_offset)?;
        file.seek(SeekFrom::Start(footer_offset))?;
        
        tracing::info!(
            path = %seg_path.display(),
            data_end = footer_offset,
            existing_rows = reader.row_count(),
            row_groups = row_groups.len(),
            "recovered active segment from unsealed file"
        );
        
        Ok(Some(ActiveSegment {
            file: std::io::BufWriter::with_capacity(SEGMENT_BUFWRITER_CAP, file),
            path: seg_path.to_path_buf(),
            schema: schema.clone(),
            row_groups: row_groups,
            current_size: footer_offset,
            total_rows: reader.row_count(),
            buffered_batch: None,
            buffered_rows: 0,
            dirty_meta: true,
            data_end: footer_offset,
            last_write_at: Instant::now(),
        }))
    }

    pub fn bulk_insert_payload(
        &self,
        db: &str,
        payload: &[u8],
        _disable_wal: bool,
    ) -> Result<u64> {
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
                .map(|c| crate::types::batch::ColumnDef::new(c.name, c.data_type))
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
                    .map_err(|e| MonaxError::Storage(format!("zstd error: {}", e)))?;
                Ok((compressed, orig_len))
            })
            .collect::<Result<Vec<_>>>()?;

        {
            let mut active_guard = state.active_segments.lock().unwrap();
            self.get_or_create_active_segment_for_partition_locked(
                &table_dir,
                &schema,
                "",
                &mut active_guard,
            )?;

            let seg = active_guard.get_mut("").unwrap();
            
            if seg.current_size != seg.data_end {
                seg.file.get_ref().set_len(seg.data_end)?;
                seg.file.seek(SeekFrom::Start(seg.data_end))?;
                seg.current_size = seg.data_end;
            }
            
            let offset = seg.current_size;
            let mut column_compressed_sizes = Vec::with_capacity(compressed_columns.len());
            let mut compressed_sizes = Vec::with_capacity(compressed_columns.len());
            let mut uncompressed_sizes = Vec::with_capacity(compressed_columns.len());
            let mut total_length = 0u64;
            let mut write_buf = Vec::new();

            for (compressed, uncompressed_size) in &compressed_columns {
                let n = compressed.len() as u64;
                column_compressed_sizes.push(n);
                compressed_sizes.push(n);
                uncompressed_sizes.push(*uncompressed_size);
                total_length += n;
                write_buf.extend_from_slice(compressed);
            }
            seg.file.write_all(&write_buf)?;

            seg.row_groups.push(RowGroupMeta {
                offset,
                length: total_length,
                row_count: req.row_count as u64,
                zone_maps: placeholder_zone_maps(&seg.schema, req.row_count as u64),
                column_compressed_sizes,
                compressed_sizes,
                uncompressed_sizes,
                bloom_filters: vec![None; seg.schema.num_columns()],
            });
            seg.current_size += total_length;
            seg.total_rows += req.row_count as u64;
            seg.dirty_meta = true;
            seg.data_end = seg.current_size;
            seg.last_write_at = Instant::now();

            let mut sealed = false;
            let mut need_flush_meta = false;
            
            if seg.current_size >= self.max_segment_size() {
                self.seal_segment(seg)?;
                active_guard.remove("");
                sealed = true;
            } else {
                if seg.dirty_meta {
                    let idle = Instant::now().duration_since(seg.last_write_at).as_secs();
                    if idle >= FLUSH_IDLE_THRESHOLD_SECS || seg.total_rows >= FLUSH_ROW_THRESHOLD {
                        need_flush_meta = true;
                    }
                }
            }

            if need_flush_meta && !sealed {
                let seg = active_guard.get_mut("").unwrap();
                self.flush_meta(seg)?;
            }
        }

        Ok(req.row_count as u64)
    }

    pub fn bulk_insert_payload_with_catalog(
        &self,
        db: &str,
        payload: &[u8],
        disable_wal: bool,
        catalogs: &crate::storage::schema::DatabaseCatalog,
    ) -> Result<u64> {
        use rayon::prelude::*;

        let req = parse_direct_bulk_payload(payload)?;
        if req.row_count == 0 {
            return Ok(0);
        }

        let table_dir = self.data_dir.join(db).join(req.table_name);
        std::fs::create_dir_all(&table_dir)?;

        let schema = {
            let table_meta = catalogs
                .get_table(req.table_name)
                .ok_or_else(|| {
                    MonaxError::Sql(format!("table '{}.{}' does not exist", db, req.table_name))
                })?;
            Schema::new(table_meta.columns.clone())
        };

        if schema.num_columns() != req.columns.len() {
            return Err(MonaxError::Protocol(format!(
                "bulk column count mismatch: schema has {} columns but payload has {}",
                schema.num_columns(),
                req.columns.len()
            )));
        }

        for (idx, (col_def, payload_col)) in schema
            .columns
            .iter()
            .zip(req.columns.iter())
            .enumerate()
        {
            if col_def.data_type != payload_col.data_type {
                return Err(MonaxError::Protocol(format!(
                    "bulk column {} type mismatch for '{}': schema={} payload={}",
                    idx,
                    col_def.name,
                    col_def.data_type.display_name(),
                    payload_col.data_type.display_name()
                )));
            }
            if !col_def.name.eq_ignore_ascii_case(payload_col.name) {
                return Err(MonaxError::Protocol(format!(
                    "bulk column {} name mismatch: schema='{}' payload='{}'",
                    idx, col_def.name, payload_col.name
                )));
            }
            if !payload_col.nulls.is_empty() {
                return Err(MonaxError::Protocol(
                    "direct bulk path currently supports all-valid columns only".into(),
                ));
            }
        }

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
                    .map_err(|e| MonaxError::Storage(format!("zstd error: {}", e)))?;
                Ok((compressed, orig_len))
            })
            .collect::<Result<Vec<_>>>()?;

        {
            let mut active_guard = state.active_segments.lock().unwrap();
            self.get_or_create_active_segment_for_partition_locked(
                &table_dir,
                &schema,
                "",
                &mut active_guard,
            )?;

            let seg = active_guard.get_mut("").unwrap();
            
            if seg.current_size != seg.data_end {
                seg.file.get_ref().set_len(seg.data_end)?;
                seg.file.seek(SeekFrom::Start(seg.data_end))?;
                seg.current_size = seg.data_end;
            }
            
            let offset = seg.current_size;
            let mut column_compressed_sizes = Vec::with_capacity(compressed_columns.len());
            let mut compressed_sizes = Vec::with_capacity(compressed_columns.len());
            let mut uncompressed_sizes = Vec::with_capacity(compressed_columns.len());
            let mut total_length = 0u64;
            let mut write_buf = Vec::new();

            for (compressed, uncompressed_size) in &compressed_columns {
                let n = compressed.len() as u64;
                column_compressed_sizes.push(n);
                compressed_sizes.push(n);
                uncompressed_sizes.push(*uncompressed_size);
                total_length += n;
                write_buf.extend_from_slice(compressed);
            }
            seg.file.write_all(&write_buf)?;

            seg.row_groups.push(RowGroupMeta {
                offset,
                length: total_length,
                row_count: req.row_count as u64,
                zone_maps: placeholder_zone_maps(&seg.schema, req.row_count as u64),
                column_compressed_sizes,
                compressed_sizes,
                uncompressed_sizes,
                bloom_filters: vec![None; seg.schema.num_columns()],
            });
            seg.current_size += total_length;
            seg.total_rows += req.row_count as u64;
            seg.dirty_meta = true;
            seg.data_end = seg.current_size;
            seg.last_write_at = Instant::now();

            let mut sealed = false;
            let mut need_flush_meta = false;
            
            if seg.current_size >= self.max_segment_size() {
                self.seal_segment(seg)?;
                active_guard.remove("");
                sealed = true;
            } else {
                if seg.dirty_meta {
                    let idle = Instant::now().duration_since(seg.last_write_at).as_secs();
                    if idle >= FLUSH_IDLE_THRESHOLD_SECS || seg.total_rows >= FLUSH_ROW_THRESHOLD {
                        need_flush_meta = true;
                    }
                }
            }

            if need_flush_meta && !sealed {
                let seg = active_guard.get_mut("").unwrap();
                self.flush_meta(seg)?;
            }
        }

        Ok(req.row_count as u64)
    }

    pub fn flush_all_active(&self) -> Result<u64> {
        self.sync_all_wals();

        let mut total = 0u64;
        let tables = self.tables.read().unwrap();
        for state in tables.values() {
            let mut active_guard = state.active_segments.lock().unwrap();
            let keys_to_seal: Vec<String> = active_guard
                .iter()
                .filter(|(_, seg)| seg.total_rows > 0 && seg.current_size >= self.min_segment_seal_size())
                .map(|(k, _)| k.clone())
                .collect();
            
            for key in keys_to_seal {
                if let Some(seg) = active_guard.get_mut(&key) {
                    if let Some(_) = self.flush_segment_buffer(seg)? {
                    }
                    self.flush_meta(seg)?;
                    tracing::info!(
                        path = %seg.path.display(),
                        size_mb = seg.current_size / (1024 * 1024),
                        rows = seg.total_rows,
                        partition_key = key,
                        "sealing segment on flush (meets minimum size threshold)"
                    );
                    self.finalize_segment(seg)?;
                    total += seg.total_rows;
                }
                active_guard.remove(&key);
            }
            
            for (_, seg) in active_guard.iter_mut() {
                if seg.total_rows > 0 {
                    if let Some(_) = self.flush_segment_buffer(seg)? {
                    }
                    self.flush_meta(seg)?;
                    total += seg.total_rows;
                    tracing::info!(
                        path = %seg.path.display(),
                        size_mb = seg.current_size / (1024 * 1024),
                        rows = seg.total_rows,
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
                    if let Some(_) = self.flush_segment_buffer(seg)? {
                    }
                }
                if seg.dirty_meta {
                    self.flush_meta(seg)?;
                }
                total += seg.total_rows;
            }
        }
        Ok(total)
    }

    fn seal_segment(&self, segment: &mut ActiveSegment) -> Result<()> {
        if let Some(_) = self.flush_segment_buffer(segment)? {
        }
        self.flush_meta(segment)?;
        self.finalize_segment(segment)?;

        tracing::info!(
            path = %segment.path.display(),
            size_mb = segment.current_size / (1024 * 1024),
            rows = segment.total_rows,
            row_groups = segment.row_groups.len(),
            "segment sealed"
        );

        Ok(())
    }

    fn finalize_segment(&self, segment: &mut ActiveSegment) -> Result<()> {
        segment.file.flush()?;
        segment.file.get_ref().sync_all()?;

        tracing::info!(
            path = %segment.path.display(),
            size_mb = segment.current_size / (1024 * 1024),
            rows = segment.total_rows,
            row_groups = segment.row_groups.len(),
            "segment finalized and closed"
        );

        Ok(())
    }

    fn flush_meta(&self, segment: &mut ActiveSegment) -> Result<()> {
        if !segment.dirty_meta {
            return Ok(());
        }

        segment.file.flush()?;
        segment.file.get_ref().set_len(segment.data_end)?;
        segment.file.seek(SeekFrom::Start(segment.data_end))?;

        let footer = self.encode_footer(segment)?;
        segment.file.write_all(&footer)?;
        segment.file.write_all(&(segment.data_end as u64).to_le_bytes())?;
        segment.file.flush()?;
        segment.file.get_ref().sync_all()?;

        segment.current_size = segment.data_end + footer.len() as u64 + 8;
        segment.dirty_meta = false;

        tracing::debug!(
            path = %segment.path.display(),
            rows = segment.total_rows,
            row_groups = segment.row_groups.len(),
            "segment meta flushed"
        );

        Ok(())
    }

    fn encode_footer(&self, segment: &ActiveSegment) -> Result<Vec<u8>> {
        let table_name = segment
            .path
            .parent()
            .and_then(|p| p.file_name())
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_default();
        Ok(encode_segment_footer(
            &segment.schema,
            segment.total_rows,
            &segment.row_groups,
            &[],
            0,
            &table_name,
        ))
    }

    fn append_to_segment_buffer(
        &self,
        segment: &mut ActiveSegment,
        batch: &RecordBatch,
    ) -> Result<Vec<String>> {
        let mut wal_keys_to_clear = Vec::new();
        if let Some(ref mut buffered) = segment.buffered_batch {
            for col_idx in 0..batch.num_columns() {
                let src = batch.column(col_idx).unwrap();
                let dst = buffered.column_mut(col_idx).unwrap();
                dst.extend(src)?;
            }
            segment.buffered_rows += batch.num_rows();
        } else {
            segment.buffered_batch = Some(batch.clone());
            segment.buffered_rows = batch.num_rows();
        }

        while segment.buffered_rows >= SQL_INSERT_ROW_GROUP_TARGET_ROWS {
            if let Some(wk) = self.flush_segment_buffer(segment)? {
                wal_keys_to_clear.push(wk);
            }
        }
        Ok(wal_keys_to_clear)
    }

    fn flush_segment_buffer(&self, segment: &mut ActiveSegment) -> Result<Option<String>> {
        use rayon::prelude::*;

        let buffered = match segment.buffered_batch.take() {
            Some(b) => b,
            None => return Ok(None),
        };
        let total_buffered = segment.buffered_rows;
        segment.buffered_rows = 0;

        let compression_level = bulk_compression_level();
        let num_cols = buffered.num_columns();

        let rows_to_flush = SQL_INSERT_ROW_GROUP_TARGET_ROWS.min(total_buffered);

        let compressed_columns: Vec<(Vec<u8>, u64)> = (0..num_cols)
            .into_par_iter()
            .map(|col_idx| {
                let col = buffered.column(col_idx).unwrap();
                let encoded = encode_column_direct_range(col, 0, rows_to_flush);
                let orig_len = encoded.len() as u64;
                let compressed = zstd::bulk::compress(&encoded, compression_level)
                    .map_err(|e| MonaxError::Storage(format!("zstd error: {}", e)))?;
                Ok((compressed, orig_len))
            })
            .collect::<Result<Vec<_>>>()?;

        if segment.current_size != segment.data_end {
            segment.file.get_ref().set_len(segment.data_end)?;
            segment.file.seek(SeekFrom::Start(segment.data_end))?;
            segment.current_size = segment.data_end;
        }

        let offset = segment.current_size;
        let mut column_compressed_sizes = Vec::with_capacity(num_cols);
        let mut compressed_sizes = Vec::with_capacity(num_cols);
        let mut uncompressed_sizes = Vec::with_capacity(num_cols);
        let mut total_length = 0u64;
        let mut write_buf = Vec::new();
        for (compressed, uncompressed_size) in &compressed_columns {
            let n = compressed.len() as u64;
            column_compressed_sizes.push(n);
            compressed_sizes.push(n);
            uncompressed_sizes.push(*uncompressed_size);
            total_length += n;
            write_buf.extend_from_slice(compressed);
        }
        segment.file.write_all(&write_buf)?;
        segment.row_groups.push(RowGroupMeta {
            offset,
            length: total_length,
            row_count: rows_to_flush as u64,
            zone_maps: placeholder_zone_maps(&segment.schema, rows_to_flush as u64),
            column_compressed_sizes,
            compressed_sizes,
            uncompressed_sizes,
            bloom_filters: vec![None; segment.schema.num_columns()],
        });
        segment.current_size += total_length;
        segment.total_rows += rows_to_flush as u64;
        segment.dirty_meta = true;
        segment.data_end = segment.current_size;
        segment.last_write_at = Instant::now();

        if FLUSH_META_AFTER_ROW_GROUP {
            self.flush_meta(segment)?;
        }

        let remaining = total_buffered - rows_to_flush;
        if remaining > 0 {
            let mut remainder = RecordBatch::new(buffered.schema.clone());
            for col_idx in 0..num_cols {
                let col = buffered.column(col_idx).unwrap();
                let dst = remainder.column_mut(col_idx).unwrap();
                for row_idx in rows_to_flush..total_buffered {
                    let val = col.scalar(row_idx);
                    dst.push(&val)?;
                }
            }
            segment.buffered_batch = Some(remainder);
            segment.buffered_rows = remaining;
        }

        Ok(Some(segment.path.to_string_lossy().to_string()))
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
        let key = table_dir.to_string_lossy().to_string();
        let state = self.get_or_create_state(&key);

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
        let key = table_dir.to_string_lossy().to_string();
        let state = self.get_or_create_state(&key);
        let mut wal_guard = state.wal_writer.lock().unwrap();
        if let Some(ref mut w) = *wal_guard {
            w.clear_entries_for_txn(txn_id)?;
        }
        Ok(())
    }

    pub fn sync_table_wal(&self, table_dir: &Path) -> Result<()> {
        let key = table_dir.to_string_lossy().to_string();
        let state = self.get_or_create_state(&key);
        let mut wal_guard = state.wal_writer.lock().unwrap();
        if let Some(ref mut w) = *wal_guard {
            w.sync()?;
        }
        Ok(())
    }
}

pub fn table_key(db: &str, table: &str) -> String {
    format!("{}:{}", db, table)
}

fn discover_max_seq(table_dir: &Path) -> u32 {
    let mut max_seq = 0u32;
    if let Ok(entries) = std::fs::read_dir(table_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(name) = path.file_stem() {
                let stem = name.to_string_lossy().to_string();
                if let Some(seq_str) = stem.strip_prefix("seg_") {
                    if let Ok(seq) = seq_str.parse::<u32>() {
                        max_seq = max_seq.max(seq);
                    }
                }
            }
        }
    }
    max_seq
}

fn placeholder_zone_maps(schema: &Schema, row_count: u64) -> Vec<ZoneMap> {
    schema
        .columns
        .iter()
        .map(|col| {
            let null = crate::storage::encoding::null_for_dtype(col.data_type);
            ZoneMap {
                min_value: null.clone(),
                max_value: null,
                null_count: 0,
                row_count,
            }
        })
        .collect()
}

fn bulk_compression_level() -> i32 {
    static LEVEL: OnceLock<i32> = OnceLock::new();
    *LEVEL.get_or_init(|| {
        std::env::var("MONAX_BULK_ZSTD_LEVEL")
            .ok()
            .and_then(|v| v.parse::<i32>().ok())
            .unwrap_or(1)
    })
}

struct DirectBulkRequest<'a> {
    table_name: &'a str,
    row_count: usize,
    columns: Vec<DirectBulkColumn<'a>>,
}

struct DirectBulkColumn<'a> {
    name: &'a str,
    data_type: DataType,
    data: &'a [u8],
    nulls: &'a [u8],
}

fn parse_direct_bulk_payload(payload: &[u8]) -> Result<DirectBulkRequest<'_>> {
    if payload.len() < 10 {
        return Err(MonaxError::Protocol("bulk payload too short".into()));
    }

    let mut pos = 0usize;
    let table_len = read_u16_at(payload, &mut pos)? as usize;
    let table_name = read_utf8_at(payload, &mut pos, table_len)?;
    let row_count = read_u32_at(payload, &mut pos)? as usize;
    let column_count = read_u32_at(payload, &mut pos)? as usize;

    let mut columns = Vec::with_capacity(column_count);
    for col_idx in 0..column_count {
        let name_len = read_u16_at(payload, &mut pos)? as usize;
        let name = read_utf8_at(payload, &mut pos, name_len)?;
        let dtype = DataType::from_u8(read_u8_at(payload, &mut pos)?)?;
        let _scale = read_u32_at(payload, &mut pos)? as i32;
        let _precision = read_u32_at(payload, &mut pos)? as i32;
        let elem_count = read_u32_at(payload, &mut pos)? as usize;
        let nulls_size = read_u32_at(payload, &mut pos)? as usize;

        if elem_count != row_count {
            return Err(MonaxError::Protocol(format!(
                "column {} row count mismatch: header={} column={}",
                col_idx, row_count, elem_count
            )));
        }

        let data_len = direct_column_data_len(dtype, elem_count, &payload[pos..])?;
        if pos + data_len > payload.len() {
            return Err(MonaxError::Protocol(format!(
                "column {} data exceeds payload",
                col_idx
            )));
        }
        let data = &payload[pos..pos + data_len];
        pos += data_len;

        if pos + nulls_size > payload.len() {
            return Err(MonaxError::Protocol(format!(
                "column {} null bitmap exceeds payload",
                col_idx
            )));
        }
        let nulls = &payload[pos..pos + nulls_size];
        pos += nulls_size;

        columns.push(DirectBulkColumn {
            name,
            data_type: dtype,
            data,
            nulls,
        });
    }

    Ok(DirectBulkRequest {
        table_name,
        row_count,
        columns,
    })
}

fn direct_column_data_len(data_type: DataType, elem_count: usize, data: &[u8]) -> Result<usize> {
    if let Some(width) = data_type.fixed_size() {
        return elem_count
            .checked_mul(width)
            .ok_or_else(|| MonaxError::Protocol("fixed-width column length overflow".into()));
    }

    let mut offset = 0usize;
    for _ in 0..elem_count {
        if offset + 4 > data.len() {
            return Err(MonaxError::Protocol(
                "variable column length prefix truncated".into(),
            ));
        }
        let len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
        offset = offset
            .checked_add(4)
            .and_then(|v| v.checked_add(len))
            .ok_or_else(|| MonaxError::Protocol("variable column length overflow".into()))?;
        if offset > data.len() {
            return Err(MonaxError::Protocol("variable column data truncated".into()));
        }
    }
    Ok(offset)
}

fn read_u8_at(payload: &[u8], pos: &mut usize) -> Result<u8> {
    if *pos >= payload.len() {
        return Err(MonaxError::Protocol("payload truncated reading u8".into()));
    }
    let v = payload[*pos];
    *pos += 1;
    Ok(v)
}

fn read_u16_at(payload: &[u8], pos: &mut usize) -> Result<u16> {
    if *pos + 2 > payload.len() {
        return Err(MonaxError::Protocol("payload truncated reading u16".into()));
    }
    let v = u16::from_le_bytes(payload[*pos..*pos + 2].try_into().unwrap());
    *pos += 2;
    Ok(v)
}

fn read_u32_at(payload: &[u8], pos: &mut usize) -> Result<u32> {
    if *pos + 4 > payload.len() {
        return Err(MonaxError::Protocol("payload truncated reading u32".into()));
    }
    let v = u32::from_le_bytes(payload[*pos..*pos + 4].try_into().unwrap());
    *pos += 4;
    Ok(v)
}

fn read_utf8_at<'a>(payload: &'a [u8], pos: &mut usize, len: usize) -> Result<&'a str> {
    if *pos + len > payload.len() {
        return Err(MonaxError::Protocol("payload truncated reading string".into()));
    }
    let s = std::str::from_utf8(&payload[*pos..*pos + len])
        .map_err(|e| MonaxError::Protocol(format!("invalid utf8 in bulk payload: {}", e)))?;
    *pos += len;
    Ok(s)
}

fn encode_column_direct_range(
    col: &crate::types::array::ColumnArray,
    start: usize,
    len: usize,
) -> Vec<u8> {
    let mut data = Vec::with_capacity(4 + len * col.data_type().fixed_size().unwrap_or(8));
    data.extend_from_slice(&0u32.to_le_bytes());
    let end = start + len;

    match col {
        crate::types::array::ColumnArray::Int32 { data: vals, .. } => {
            let slice = &vals[start..end];
            let bytes =
                unsafe { std::slice::from_raw_parts(slice.as_ptr() as *const u8, slice.len() * 4) };
            data.extend_from_slice(bytes);
        }
        crate::types::array::ColumnArray::Int64 { data: vals, .. } => {
            let slice = &vals[start..end];
            let bytes =
                unsafe { std::slice::from_raw_parts(slice.as_ptr() as *const u8, slice.len() * 8) };
            data.extend_from_slice(bytes);
        }
        crate::types::array::ColumnArray::Float64 { data: vals, .. } => {
            let slice = &vals[start..end];
            let bytes =
                unsafe { std::slice::from_raw_parts(slice.as_ptr() as *const u8, slice.len() * 8) };
            data.extend_from_slice(bytes);
        }
        crate::types::array::ColumnArray::Decimal { data: vals, .. } => {
            let slice = &vals[start..end];
            let bytes =
                unsafe { std::slice::from_raw_parts(slice.as_ptr() as *const u8, slice.len() * 8) };
            data.extend_from_slice(bytes);
        }
        crate::types::array::ColumnArray::VarChar { data: vals, .. } => {
            for v in &vals[start..end] {
                data.extend_from_slice(&(v.len() as u32).to_le_bytes());
                data.extend_from_slice(v.as_bytes());
            }
        }
        crate::types::array::ColumnArray::Timestamp { data: vals, .. } => {
            let slice = &vals[start..end];
            let bytes =
                unsafe { std::slice::from_raw_parts(slice.as_ptr() as *const u8, slice.len() * 8) };
            data.extend_from_slice(bytes);
        }
        crate::types::array::ColumnArray::Bool { data: vals, .. } => {
            data.extend_from_slice(&vals[start..end]);
        }
        crate::types::array::ColumnArray::Int8 { data: vals, .. } => {
            let slice = &vals[start..end];
            let bytes =
                unsafe { std::slice::from_raw_parts(slice.as_ptr() as *const u8, slice.len()) };
            data.extend_from_slice(bytes);
        }
        crate::types::array::ColumnArray::Int16 { data: vals, .. } => {
            let slice = &vals[start..end];
            let bytes = unsafe {
                std::slice::from_raw_parts(slice.as_ptr() as *const u8, slice.len() * 2)
            };
            data.extend_from_slice(bytes);
        }
        crate::types::array::ColumnArray::UInt8 { data: vals, .. } => {
            data.extend_from_slice(&vals[start..end]);
        }
        crate::types::array::ColumnArray::UInt16 { data: vals, .. } => {
            let slice = &vals[start..end];
            let bytes = unsafe {
                std::slice::from_raw_parts(slice.as_ptr() as *const u8, slice.len() * 2)
            };
            data.extend_from_slice(bytes);
        }
        crate::types::array::ColumnArray::UInt32 { data: vals, .. } => {
            let slice = &vals[start..end];
            let bytes = unsafe {
                std::slice::from_raw_parts(slice.as_ptr() as *const u8, slice.len() * 4)
            };
            data.extend_from_slice(bytes);
        }
        crate::types::array::ColumnArray::UInt64 { data: vals, .. } => {
            let slice = &vals[start..end];
            let bytes = unsafe {
                std::slice::from_raw_parts(slice.as_ptr() as *const u8, slice.len() * 8)
            };
            data.extend_from_slice(bytes);
        }
        crate::types::array::ColumnArray::Float32 { data: vals, .. } => {
            let slice = &vals[start..end];
            let bytes = unsafe {
                std::slice::from_raw_parts(slice.as_ptr() as *const u8, slice.len() * 4)
            };
            data.extend_from_slice(bytes);
        }
        crate::types::array::ColumnArray::Date { data: vals, .. } => {
            let slice = &vals[start..end];
            let bytes = unsafe {
                std::slice::from_raw_parts(slice.as_ptr() as *const u8, slice.len() * 4)
            };
            data.extend_from_slice(bytes);
        }
        crate::types::array::ColumnArray::Time { data: vals, .. } => {
            let slice = &vals[start..end];
            let bytes = unsafe {
                std::slice::from_raw_parts(slice.as_ptr() as *const u8, slice.len() * 8)
            };
            data.extend_from_slice(bytes);
        }
        crate::types::array::ColumnArray::Interval { data: vals, .. } => {
            let slice = &vals[start..end];
            let bytes = unsafe {
                std::slice::from_raw_parts(slice.as_ptr() as *const u8, slice.len() * 8)
            };
            data.extend_from_slice(bytes);
        }
        crate::types::array::ColumnArray::Blob { data: vals, .. } => {
            for v in &vals[start..end] {
                data.extend_from_slice(&(v.len() as u32).to_le_bytes());
                data.extend_from_slice(v);
            }
        }
        crate::types::array::ColumnArray::Vector { data: vals, .. } => {
            for v in &vals[start..end] {
                data.extend_from_slice(&(v.len() as u32).to_le_bytes());
                for f in v {
                    data.extend_from_slice(&f.to_le_bytes());
                }
            }
        }
    }

    data
}
