use std::collections::HashMap;
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock, RwLock};

use crate::common::error::{MonaxError, Result};
use crate::net::session::Session;
use crate::protocol::types::DataType;
use crate::storage::schema::DatabaseCatalog;
use crate::storage::segment::{FOOTER_SIZE, SEGMENT_MAGIC, SEGMENT_VERSION};
use crate::storage::wal::WalWriter;
use crate::types::batch::{RecordBatch, Schema};

use crate::sql::parser::ast::Statement;
use crate::sql::parser::parser::Parser;

const MAX_SEGMENT_SIZE: u64 = 2 * 1024 * 1024 * 1024;
const MIN_SEGMENT_SIZE: u64 = 1 * 1024 * 1024 * 1024;
const SEGMENT_BUFWRITER_CAP: usize = 512 * 1024;

pub enum ExecResult {
    Ok { rows_affected: u64 },
    Query { schema: Schema, batches: Vec<RecordBatch> },
}

struct ActiveSegment {
    file: std::io::BufWriter<std::fs::File>,
    path: PathBuf,
    schema: Schema,
    row_groups: Vec<RowGroupMeta>,
    current_size: u64,
    total_rows: u64,
}

#[derive(Debug, Clone)]
struct RowGroupMeta {
    offset: u64,
    length: u64,
    row_count: u64,
    column_sizes: Vec<u64>,
    compressed_sizes: Vec<u64>,
    uncompressed_sizes: Vec<u64>,
}

pub struct Engine {
    data_dir: PathBuf,
    catalogs: RwLock<HashMap<String, DatabaseCatalog>>,
    segment_seqs: Mutex<HashMap<String, u32>>,
    wal_writers: Mutex<HashMap<String, WalWriter>>,
    next_txn_id: AtomicU64,
    bulk_mode: AtomicBool,
    active_segments: Mutex<HashMap<String, ActiveSegment>>,
}

impl Engine {
    pub fn new(data_dir: &Path) -> Result<Self> {
        let mut catalogs = HashMap::new();
        let meta_dir = data_dir.join("meta");
        if meta_dir.exists() {
            if let Ok(catalog) = DatabaseCatalog::load_or_create(&meta_dir, "default") {
                catalogs.insert("default".to_string(), catalog);
            }
        }

        let mut segment_seqs_init = HashMap::new();
        if data_dir.exists() {
            if let Ok(dbs) = std::fs::read_dir(data_dir) {
                for db_entry in dbs.flatten() {
                    if db_entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                        let db_name = db_entry.file_name().to_string_lossy().to_string();
                        if db_name == "meta" || db_name == "wal" {
                            continue;
                        }
                        if !catalogs.contains_key(&db_name) {
                            if let Ok(catalog) = DatabaseCatalog::load_or_create(&meta_dir, &db_name) {
                                catalogs.insert(db_name.clone(), catalog);
                            }
                        }
                        let db_dir = data_dir.join(&db_name);
                        if let Ok(tables) = std::fs::read_dir(&db_dir) {
                            for tbl_entry in tables.flatten() {
                                if tbl_entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                                    let tbl_dir = tbl_entry.path();
                                    if let Ok(segments) = super::helpers::discover_segments(&tbl_dir) {
                                        for seg in segments {
                                            if let Some(name) = seg.file_stem() {
                                                let stem = name.to_string_lossy().to_string();
                                                if let Some(seq_str) = stem.strip_prefix("seg-") {
                                                    if let Ok(seq) = seq_str.parse::<u32>() {
                                                        let key = format!("{}/{}", db_name, tbl_entry.file_name().to_string_lossy());
                                                        segment_seqs_init.entry(key).and_modify(|v: &mut u32| *v = (*v).max(seq)).or_insert(seq);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(Self {
            data_dir: data_dir.to_path_buf(),
            catalogs: RwLock::new(catalogs),
            segment_seqs: Mutex::new(segment_seqs_init),
            wal_writers: Mutex::new(HashMap::new()),
            next_txn_id: AtomicU64::new(0),
            bulk_mode: AtomicBool::new(false),
            active_segments: Mutex::new(HashMap::new()),
        })
    }

    pub fn execute(&self, session: &mut Session, sql: &str) -> Result<ExecResult> {
        let stmts = Parser::parse(sql)?;
        if stmts.is_empty() {
            return Err(MonaxError::Sql("empty query".into()));
        }
        self.execute_stmt(session, &stmts[0])
    }

    pub fn execute_prepared(&self, session: &mut Session, stmt: &Statement) -> Result<ExecResult> {
        self.execute_stmt(session, stmt)
    }

    fn execute_stmt(&self, session: &mut Session, stmt: &Statement) -> Result<ExecResult> {
        match stmt {
            Statement::CreateTable { name, columns, if_not_exists, sort_key, partition_by } => {
                super::ddl::exec_create_table(self, &session.database, name, columns, *if_not_exists, sort_key, partition_by)
            }
            Statement::DropTable { name, if_exists } => {
                super::ddl::exec_drop_table(self, &session.database, name, *if_exists)
            }
            Statement::Insert { table, columns, values } => {
                super::dml_insert::exec_insert(self, &session.database, table, columns, values, session.timezone_offset_hours)
            }
            Statement::Update { table, assignments, where_clause } => {
                super::dml_update::exec_update(self, &session.database, table, assignments, where_clause)
            }
            Statement::Delete { table, where_clause } => {
                super::dml_delete::exec_delete(self, &session.database, table, where_clause)
            }
            Statement::Select { .. } => {
                super::query::exec_select(self, &session.database, stmt)
            }
            Statement::UseDatabase(database) => {
                session.database = database.clone();
                Ok(ExecResult::Ok { rows_affected: 0 })
            }
            Statement::ShowTables => {
                super::ddl::exec_show_tables(self, &session.database)
            }
            Statement::ShowDatabases => {
                super::ddl::exec_show_databases(self)
            }
        }
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    pub fn next_segment_path(&self, table_dir: &Path, _partition_key: &str) -> Result<PathBuf> {
        let key = table_dir.to_string_lossy().to_string();
        let mut seqs = self.segment_seqs.lock().unwrap();
        let seq = seqs.entry(key).or_insert(0);
        *seq += 1;
        Ok(table_dir.join(format!("seg_{:05}.dat", seq)))
    }

    pub fn next_partition_segment_path(&self, table_dir: &Path, partition_key: &str) -> Result<PathBuf> {
        let key = table_dir.to_string_lossy().to_string();
        let mut seqs = self.segment_seqs.lock().unwrap();
        let seq = seqs.entry(key).or_insert(0);
        *seq += 1;
        Ok(table_dir.join(format!("p{}_{:04}.dat", partition_key, seq)))
    }

    pub fn get_table_meta(&self, db: &str, table: &str) -> Option<crate::storage::schema::TableMeta> {
        let catalogs = self.catalogs.read().unwrap();
        catalogs.get(db).and_then(|c| c.get_table(table).cloned())
    }

    pub fn set_bulk_mode(&self, enabled: bool) {
        self.bulk_mode.store(enabled, Ordering::Release);
    }

    pub fn is_bulk_mode(&self) -> bool {
        self.bulk_mode.load(Ordering::Acquire)
    }

    pub fn alloc_txn_id(&self) -> u64 {
        self.next_txn_id.fetch_add(1, Ordering::Relaxed)
    }

    pub fn flush_all_buffers(&self) -> Result<u64> {
        {
            let mut wal_writers = self.wal_writers().lock().unwrap();
            for (_, writer) in wal_writers.iter_mut() {
                let _ = writer.sync();
            }
        }

        let mut total = 0u64;
        {
            let mut active = self.active_segments.lock().unwrap();
            for (_, mut segment) in active.drain() {
                if segment.total_rows > 0 {
                    self.seal_segment(&mut segment)?;
                    total += segment.total_rows;
                }
            }
        }

        {
            let mut wal_writers = self.wal_writers().lock().unwrap();
            for (_, writer) in wal_writers.iter_mut() {
                let _ = writer.clear();
            }
        }

        {
            let mut catalogs = self.catalogs.write().unwrap();
            let meta_dir = self.data_dir.join("meta");
            for (_, catalog) in catalogs.iter_mut() {
                let _ = catalog.save(&meta_dir);
            }
        }

        Ok(total)
    }

    pub(crate) fn catalogs(&self) -> &RwLock<HashMap<String, DatabaseCatalog>> {
        &self.catalogs
    }

    pub(crate) fn wal_writers(&self) -> &Mutex<HashMap<String, WalWriter>> {
        &self.wal_writers
    }

    pub fn exec_bulk_insert_with_wal_control(
        &self,
        db: &str,
        table: &str,
        batch: RecordBatch,
        _disable_wal: bool,
    ) -> Result<u64> {
        use rayon::prelude::*;

        let row_count = batch.num_rows() as u64;
        if row_count == 0 {
            return Ok(0);
        }
        let table_dir = self.data_dir.join(db).join(table);
        std::fs::create_dir_all(&table_dir)?;
        let key = format!("{}:{}", db, table);

        let compression_level = bulk_compression_level();

        let num_cols = batch.num_columns();
        let compressed_columns: Vec<(Vec<u8>, u64)> = (0..num_cols)
            .into_par_iter()
            .map(|col_idx| {
                let col = batch.column(col_idx).unwrap();
                let encoded = encode_column_direct(col);
                let orig_len = encoded.len() as u64;
                let compressed = zstd::bulk::compress(&encoded, compression_level)
                    .map_err(|e| MonaxError::Storage(format!("zstd error: {}", e)))?;
                Ok((compressed, orig_len))
            })
            .collect::<Result<Vec<_>>>()?;

        let mut active_segments = self.active_segments.lock().unwrap();
        if !active_segments.contains_key(&key) {
            let schema = batch.schema.clone();
            let seg_path = self.next_segment_path(&table_dir, "")?;
            let file = std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&seg_path)?;
            active_segments.insert(
                key.clone(),
                ActiveSegment {
                    file: std::io::BufWriter::with_capacity(SEGMENT_BUFWRITER_CAP, file),
                    path: seg_path,
                    schema,
                    row_groups: Vec::new(),
                    current_size: 0,
                    total_rows: 0,
                },
            );
        }
        let segment = active_segments.get_mut(&key).unwrap();
        let offset = segment.current_size;
        let mut column_sizes = Vec::with_capacity(num_cols);
        let mut compressed_sizes = Vec::with_capacity(num_cols);
        let mut uncompressed_sizes = Vec::with_capacity(num_cols);
        let mut total_length = 0u64;
        let mut write_buf = Vec::new();
        for (compressed, uncompressed_size) in &compressed_columns {
            let n = compressed.len() as u64;
            column_sizes.push(n);
            compressed_sizes.push(n);
            uncompressed_sizes.push(*uncompressed_size);
            total_length += n;
            write_buf.extend_from_slice(compressed);
        }
        segment.file.write_all(&write_buf)?;
        segment.row_groups.push(RowGroupMeta {
            offset,
            length: total_length,
            row_count,
            column_sizes,
            compressed_sizes,
            uncompressed_sizes,
        });
        segment.current_size += total_length;
        segment.total_rows += row_count;
        let mut sealed = false;
        if segment.current_size >= MAX_SEGMENT_SIZE {
            self.seal_segment(segment)?;
            active_segments.remove(&key);
            sealed = true;
        }
        drop(active_segments);

        if sealed {
            let wal_key = table_dir.to_string_lossy().to_string();
            let mut wal_writers = self.wal_writers.lock().unwrap();
            if let Some(wal_writer) = wal_writers.get_mut(&wal_key) {
                let _ = wal_writer.clear();
            }
        }

        Ok(row_count)
    }

    pub fn exec_bulk_insert_payload_with_wal_control(
        &self,
        db: &str,
        payload: &[u8],
        disable_wal: bool,
    ) -> Result<u64> {
        use rayon::prelude::*;

        let req = parse_direct_bulk_payload(payload)?;
        if req.row_count == 0 {
            return Ok(0);
        }

        let table_dir = self.data_dir.join(db).join(req.table_name);
        std::fs::create_dir_all(&table_dir)?;

        let schema = {
            let catalogs = self.catalogs.read().unwrap();
            let catalog = catalogs
                .get(db)
                .ok_or_else(|| MonaxError::Sql(format!("database '{}' does not exist", db)))?;
            let table_meta = catalog
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

        let key = format!("{}:{}", db, req.table_name);
        let mut active_segments = self.active_segments.lock().unwrap();
        if !active_segments.contains_key(&key) {
            let seg_path = self.next_segment_path(&table_dir, "")?;
            let file = std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&seg_path)?;
            active_segments.insert(
                key.clone(),
                ActiveSegment {
                    file: std::io::BufWriter::with_capacity(SEGMENT_BUFWRITER_CAP, file),
                    path: seg_path,
                    schema,
                    row_groups: Vec::new(),
                    current_size: 0,
                    total_rows: 0,
                },
            );
        }

        let segment = active_segments.get_mut(&key).unwrap();
        let offset = segment.current_size;
        let mut column_sizes = Vec::with_capacity(compressed_columns.len());
        let mut compressed_sizes = Vec::with_capacity(compressed_columns.len());
        let mut uncompressed_sizes = Vec::with_capacity(compressed_columns.len());
        let mut total_length = 0u64;
        let mut write_buf = Vec::new();

        for (compressed, uncompressed_size) in &compressed_columns {
            let n = compressed.len() as u64;
            column_sizes.push(n);
            compressed_sizes.push(n);
            uncompressed_sizes.push(*uncompressed_size);
            total_length += n;
            write_buf.extend_from_slice(compressed);
        }
        segment.file.write_all(&write_buf)?;

        segment.row_groups.push(RowGroupMeta {
            offset,
            length: total_length,
            row_count: req.row_count as u64,
            column_sizes,
            compressed_sizes,
            uncompressed_sizes,
        });
        segment.current_size += total_length;
        segment.total_rows += req.row_count as u64;

        let mut sealed = false;
        if segment.current_size >= MAX_SEGMENT_SIZE {
            self.seal_segment(segment)?;
            active_segments.remove(&key);
            sealed = true;
        }
        drop(active_segments);

        if sealed {
            let wal_key = table_dir.to_string_lossy().to_string();
            let mut wal_writers = self.wal_writers.lock().unwrap();
            if let Some(wal_writer) = wal_writers.get_mut(&wal_key) {
                let _ = wal_writer.clear();
            }
        }

        if !disable_wal {
            let bulk_mode = self.bulk_mode.load(Ordering::Acquire);
            if !bulk_mode {
                let mut catalogs = self.catalogs.write().unwrap();
                if let Some(catalog) = catalogs.get_mut(db) {
                    if let Some(meta) = catalog.get_table_mut(req.table_name) {
                        meta.row_count += req.row_count as i64;
                    }
                }
            }
        }

        Ok(req.row_count as u64)
    }

    fn seal_segment(&self, segment: &mut ActiveSegment) -> Result<()> {
        segment.file.flush()?;
        let footer_offset = segment.file.seek(SeekFrom::End(0))?;

        let footer = self.encode_footer(segment)?;
        segment.file.write_all(&footer)?;
        segment
            .file
            .write_all(&(footer_offset as u64).to_le_bytes())?;
        segment.file.flush()?;
        segment.file.get_ref().sync_all()?;

        tracing::debug!(
            path = %segment.path.display(),
            rows = segment.total_rows,
            row_groups = segment.row_groups.len(),
            "segment sealed"
        );

        Ok(())
    }

    fn encode_footer(&self, segment: &ActiveSegment) -> Result<Vec<u8>> {
        let mut buf = Vec::new();

        buf.extend_from_slice(&SEGMENT_MAGIC.to_le_bytes());
        buf.extend_from_slice(&SEGMENT_VERSION.to_le_bytes());
        buf.extend_from_slice(&0u64.to_le_bytes());
        buf.extend_from_slice(&segment.total_rows.to_le_bytes());
        buf.extend_from_slice(&(segment.row_groups.len() as u32).to_le_bytes());
        buf.extend_from_slice(&0u64.to_le_bytes());
        buf.extend_from_slice(&0u64.to_le_bytes());

        let table_name = segment
            .path
            .parent()
            .and_then(|p| p.file_name())
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_default();
        buf.extend_from_slice(&(table_name.len() as u32).to_le_bytes());
        buf.extend_from_slice(table_name.as_bytes());
        buf.extend_from_slice(&(segment.schema.columns.len() as u32).to_le_bytes());

        for col in &segment.schema.columns {
            buf.extend_from_slice(&(col.name.len() as u32).to_le_bytes());
            buf.extend_from_slice(col.name.as_bytes());
            buf.push(col.data_type as u8);
            buf.push(col.nullable as u8);
            buf.extend_from_slice(&(col.scale as u32).to_le_bytes());
            buf.extend_from_slice(&(col.precision as u32).to_le_bytes());
        }

        for rg in &segment.row_groups {
            buf.extend_from_slice(&rg.offset.to_le_bytes());
            buf.extend_from_slice(&rg.length.to_le_bytes());
            buf.extend_from_slice(&rg.row_count.to_le_bytes());

            for &s in &rg.column_sizes {
                buf.extend_from_slice(&s.to_le_bytes());
            }
            for &s in &rg.compressed_sizes {
                buf.extend_from_slice(&s.to_le_bytes());
            }
            for &s in &rg.uncompressed_sizes {
                buf.extend_from_slice(&s.to_le_bytes());
            }
        }

        if buf.len() < FOOTER_SIZE {
            buf.resize(FOOTER_SIZE, 0);
        }

        Ok(buf)
    }
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

fn bulk_compression_level() -> i32 {
    static LEVEL: OnceLock<i32> = OnceLock::new();
    *LEVEL.get_or_init(|| {
        std::env::var("MONAX_BULK_ZSTD_LEVEL")
            .ok()
            .and_then(|v| v.parse::<i32>().ok())
            .unwrap_or(1)
    })
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

fn encode_column_direct(col: &crate::types::array::ColumnArray) -> Vec<u8> {
    let mut data = Vec::with_capacity(4 + col.len() * col.data_type().fixed_size().unwrap_or(8));
    data.extend_from_slice(&0u32.to_le_bytes());

    match col {
        crate::types::array::ColumnArray::Int32 { data: vals, .. } => {
            let bytes =
                unsafe { std::slice::from_raw_parts(vals.as_ptr() as *const u8, vals.len() * 4) };
            data.extend_from_slice(bytes);
        }
        crate::types::array::ColumnArray::Int64 { data: vals, .. } => {
            let bytes =
                unsafe { std::slice::from_raw_parts(vals.as_ptr() as *const u8, vals.len() * 8) };
            data.extend_from_slice(bytes);
        }
        crate::types::array::ColumnArray::Float64 { data: vals, .. } => {
            let bytes =
                unsafe { std::slice::from_raw_parts(vals.as_ptr() as *const u8, vals.len() * 8) };
            data.extend_from_slice(bytes);
        }
        crate::types::array::ColumnArray::Decimal { data: vals, .. } => {
            let bytes =
                unsafe { std::slice::from_raw_parts(vals.as_ptr() as *const u8, vals.len() * 8) };
            data.extend_from_slice(bytes);
        }
        crate::types::array::ColumnArray::VarChar { data: vals, .. } => {
            for v in vals {
                data.extend_from_slice(&(v.len() as u32).to_le_bytes());
                data.extend_from_slice(v.as_bytes());
            }
        }
        crate::types::array::ColumnArray::Timestamp { data: vals, .. } => {
            let bytes =
                unsafe { std::slice::from_raw_parts(vals.as_ptr() as *const u8, vals.len() * 8) };
            data.extend_from_slice(bytes);
        }
        crate::types::array::ColumnArray::Bool { data: vals, .. } => {
            data.extend_from_slice(vals);
        }
        crate::types::array::ColumnArray::Int8 { data: vals, .. } => {
            let bytes =
                unsafe { std::slice::from_raw_parts(vals.as_ptr() as *const u8, vals.len()) };
            data.extend_from_slice(bytes);
        }
        crate::types::array::ColumnArray::Int16 { data: vals, .. } => {
            let bytes = unsafe {
                std::slice::from_raw_parts(vals.as_ptr() as *const u8, vals.len() * 2)
            };
            data.extend_from_slice(bytes);
        }
        crate::types::array::ColumnArray::UInt8 { data: vals, .. } => {
            data.extend_from_slice(vals);
        }
        crate::types::array::ColumnArray::UInt16 { data: vals, .. } => {
            let bytes = unsafe {
                std::slice::from_raw_parts(vals.as_ptr() as *const u8, vals.len() * 2)
            };
            data.extend_from_slice(bytes);
        }
        crate::types::array::ColumnArray::UInt32 { data: vals, .. } => {
            let bytes = unsafe {
                std::slice::from_raw_parts(vals.as_ptr() as *const u8, vals.len() * 4)
            };
            data.extend_from_slice(bytes);
        }
        crate::types::array::ColumnArray::UInt64 { data: vals, .. } => {
            let bytes = unsafe {
                std::slice::from_raw_parts(vals.as_ptr() as *const u8, vals.len() * 8)
            };
            data.extend_from_slice(bytes);
        }
        crate::types::array::ColumnArray::Float32 { data: vals, .. } => {
            let bytes = unsafe {
                std::slice::from_raw_parts(vals.as_ptr() as *const u8, vals.len() * 4)
            };
            data.extend_from_slice(bytes);
        }
        crate::types::array::ColumnArray::Date { data: vals, .. } => {
            let bytes = unsafe {
                std::slice::from_raw_parts(vals.as_ptr() as *const u8, vals.len() * 4)
            };
            data.extend_from_slice(bytes);
        }
        crate::types::array::ColumnArray::Time { data: vals, .. } => {
            let bytes = unsafe {
                std::slice::from_raw_parts(vals.as_ptr() as *const u8, vals.len() * 8)
            };
            data.extend_from_slice(bytes);
        }
        crate::types::array::ColumnArray::Interval { data: vals, .. } => {
            let bytes = unsafe {
                std::slice::from_raw_parts(vals.as_ptr() as *const u8, vals.len() * 8)
            };
            data.extend_from_slice(bytes);
        }
        crate::types::array::ColumnArray::Blob { data: vals, .. } => {
            for v in vals {
                data.extend_from_slice(&(v.len() as u32).to_le_bytes());
                data.extend_from_slice(v);
            }
        }
        crate::types::array::ColumnArray::Vector { data: vals, .. } => {
            for v in vals {
                data.extend_from_slice(&(v.len() as u32).to_le_bytes());
                for f in v {
                    data.extend_from_slice(&f.to_le_bytes());
                }
            }
        }
    }

    data
}
