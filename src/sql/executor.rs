use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock, RwLock};

use crate::common::error::{MonaxError, Result};
use crate::net::session::Session;
use crate::protocol::types::DataType;
use crate::storage::schema::{DatabaseCatalog, PartitionType, TableMeta};
use crate::storage::segment::{SegmentReader, SegmentWriter, FOOTER_SIZE, SEGMENT_MAGIC, SEGMENT_VERSION};
use crate::storage::wal::{WalWriter, WalRecovery, RecordType};
use crate::types::batch::{ColumnDef, RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

use super::parser::ast::*;
use super::parser::parser::Parser;
use super::planner;
use super::optimizer;
use super::operators;

const MAX_SEGMENT_SIZE: u64 = 2 * 1024 * 1024 * 1024;

pub enum ExecResult {
    Ok { rows_affected: u64 },
    Query { schema: Schema, batches: Vec<RecordBatch> },
}

struct ActiveSegment {
    file: BufWriter<File>,
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
    next_txn_id: Mutex<u64>,
    bulk_mode: Mutex<bool>,
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

                        if let Ok(tables) = std::fs::read_dir(db_entry.path()) {
                            for tbl_entry in tables.flatten() {
                                if tbl_entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                                    let table_name = tbl_entry.file_name().to_string_lossy().to_string();
                                    let table_dir = tbl_entry.path();
                                    let key = table_dir.to_string_lossy().to_string();

                                    let max_seg_seq = discover_segments(&table_dir)
                                        .unwrap_or_default()
                                        .iter()
                                        .filter_map(|p| p.file_name())
                                        .filter_map(|n| n.to_str())
                                        .filter(|n| n.starts_with("seg_") && n.ends_with(".dat"))
                                        .filter_map(|n| {
                                            let num_part = &n[4..n.len()-4];
                                            num_part.parse::<u32>().ok()
                                        })
                                        .max()
                                        .unwrap_or(0);
                                    segment_seqs_init.insert(key.clone(), max_seg_seq);

                                    let wal_dir = table_dir.join("wal");
                                    if wal_dir.exists() {
                                        let schema_opt = catalogs.get(&db_name).and_then(|c| c.get_table(&table_name)).map(|m| Schema::new(m.columns.clone()));

                                        if let Some(schema) = schema_opt {
                                            match WalRecovery::replay(&wal_dir) {
                                                Ok(records) if !records.is_empty() => {
                                                    tracing::info!(
                                                        db = %db_name,
                                                        table = %table_name,
                                                        count = records.len(),
                                                        "WAL recovery: replaying records"
                                                    );

                                                    let seq = segment_seqs_init.get_mut(&key).unwrap();
                                                    for record in records {
                                                        if let Ok(batch) = deserialize_batch_from_wal(&record.data, &schema) {
                                                            if batch.num_rows() > 0 {
                                                                *seq += 1;
                                                                let filename = format!("seg_{:05}.dat", *seq);
                                                                let seg_path = table_dir.join(filename);

                                                                match SegmentWriter::create(&seg_path, schema.clone(), 8192) {
                                                                    Ok(mut writer) => {
                                                                        if let Err(e) = writer.write_batch(&batch) {
                                                                            tracing::warn!(path = %seg_path.display(), error = %e, "Failed to write recovered batch");
                                                                        } else if let Err(e) = writer.seal() {
                                                                            tracing::warn!(path = %seg_path.display(), error = %e, "Failed to seal recovered segment");
                                                                        } else {
                                                                            tracing::debug!(path = %seg_path.display(), rows = batch.num_rows(), "WAL recovery: wrote segment");
                                                                        }
                                                                    }
                                                                    Err(e) => {
                                                                        tracing::warn!(path = %seg_path.display(), error = %e, "Failed to create segment for WAL recovery");
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }

                                                    if let Ok(mut wal_writers) = WalWriter::open(&wal_dir, 64 * 1024 * 1024) {
                                                        wal_writers.clear()?;
                                                    }
                                                }
                                                Ok(_) => {}
                                                Err(e) => {
                                                    tracing::warn!(
                                                        path = %wal_dir.display(),
                                                        error = %e,
                                                        "WAL recovery failed"
                                                    );
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
            next_txn_id: Mutex::new(1),
            bulk_mode: Mutex::new(false),
            active_segments: Mutex::new(HashMap::new()),
        })
    }

    /// Execute a SQL string in the context of a session.
    pub fn execute(&self, session: &mut Session, sql: &str) -> Result<ExecResult> {
        let stmts = Parser::parse(sql)?;
        if stmts.is_empty() {
            return Err(MonaxError::Sql("empty query".into()));
        }
        // Execute only the first statement for now
        self.execute_stmt(session, &stmts[0])
    }

    /// Execute a prepared statement.
    pub fn execute_prepared(&self, session: &mut Session, stmt: &Statement) -> Result<ExecResult> {
        self.execute_stmt(session, stmt)
    }

    fn execute_stmt(&self, session: &mut Session, stmt: &Statement) -> Result<ExecResult> {
        match stmt {
            Statement::CreateTable { name, columns, if_not_exists, sort_key, partition_by } => {
                self.exec_create_table(&session.database, name, columns, *if_not_exists, sort_key, partition_by)
            }
            Statement::DropTable { name, if_exists } => {
                self.exec_drop_table(&session.database, name, *if_exists)
            }
            Statement::Insert { table, columns, values } => {
                self.exec_insert(&session.database, table, columns, values)
            }
            Statement::Update { table, assignments, where_clause } => {
                self.exec_update(&session.database, table, assignments, where_clause)
            }
            Statement::Delete { table, where_clause } => {
                self.exec_delete(&session.database, table, where_clause)
            }
            Statement::Select { .. } => {
                self.exec_select(&session.database, stmt)
            }
            Statement::ShowTables => {
                self.exec_show_tables(&session.database)
            }
            Statement::ShowDatabases => {
                self.exec_show_databases()
            }
            Statement::UseDatabase(db) => {
                session.database = db.clone();
                Ok(ExecResult::Ok { rows_affected: 0 })
            }
        }
    }

    fn exec_create_table(
        &self,
        db: &str,
        name: &str,
        columns: &[ColumnDefAst],
        if_not_exists: bool,
        sort_key: &Option<Vec<String>>,
        partition_by: &Option<PartitionDef>,
    ) -> Result<ExecResult> {
        let mut catalogs = self.catalogs.write().unwrap();
        let catalog = self.get_or_create_catalog(&mut catalogs, db)?;

        if catalog.get_table(name).is_some() {
            if if_not_exists {
                return Ok(ExecResult::Ok { rows_affected: 0 });
            }
            return Err(MonaxError::Sql(format!("table '{}.{}' already exists", db, name)));
        }

        let col_defs: Vec<ColumnDef> = columns.iter().map(|c| {
            ColumnDef::new(&c.name, c.data_type)
                .with_nullable(c.nullable)
                .with_scale(c.scale)
                .with_precision(c.precision)
        }).collect();

        catalog.create_table(name, col_defs)?;

        // Wire sort_key and partition_by into the table metadata
        if let Some(meta) = catalog.get_table_mut(name) {
            if let Some(ref keys) = sort_key {
                meta.cluster_key = keys.clone();
            }
            if let Some(ref pdef) = partition_by {
                meta.partition_key = pdef.column.clone();
                meta.partition_type = match pdef.strategy {
                    PartitionStrategy::Day => PartitionType::Function,
                    PartitionStrategy::Year => PartitionType::Function,
                    PartitionStrategy::Value => PartitionType::Value,
                };
            }
        }

        // Create the table data directory
        let table_dir = self.data_dir.join(db).join(name);
        std::fs::create_dir_all(&table_dir)?;

        // Save catalog
        let meta_dir = self.data_dir.join("meta");
        catalog.save(&meta_dir)?;

        Ok(ExecResult::Ok { rows_affected: 0 })
    }

    fn exec_drop_table(&self, db: &str, name: &str, if_exists: bool) -> Result<ExecResult> {
        let mut catalogs = self.catalogs.write().unwrap();
        let catalog = self.get_or_create_catalog(&mut catalogs, db)?;

        if catalog.get_table(name).is_none() {
            if if_exists {
                return Ok(ExecResult::Ok { rows_affected: 0 });
            }
            return Err(MonaxError::Sql(format!("table '{}.{}' does not exist", db, name)));
        }

        catalog.drop_table(name)?;

        // Remove WAL writer for this table
        let table_dir = self.data_dir.join(db).join(name);
        {
            let key = table_dir.to_string_lossy().to_string();
            let mut wal_writers = self.wal_writers.lock().unwrap();
            wal_writers.remove(&key);
        }

        // Delete table data directory
        if table_dir.exists() {
            let _ = std::fs::remove_dir_all(&table_dir);
        }

        let meta_dir = self.data_dir.join("meta");
        catalog.save(&meta_dir)?;

        Ok(ExecResult::Ok { rows_affected: 0 })
    }

    fn exec_insert(
        &self,
        db: &str,
        table: &str,
        columns: &[String],
        values: &[Vec<Expr>],
    ) -> Result<ExecResult> {
        let catalogs = self.catalogs.read().unwrap();
        let catalog = catalogs.get(db)
            .ok_or_else(|| MonaxError::Sql(format!("database '{}' does not exist", db)))?;

        let table_meta = catalog.get_table(table)
            .ok_or_else(|| MonaxError::Sql(format!("table '{}.{}' does not exist", db, table)))?
            .clone();

        let schema = Schema::new(table_meta.columns.clone());

        // Determine column indices
        let col_indices: Vec<usize> = if columns.is_empty() {
            (0..schema.columns.len()).collect()
        } else {
            columns.iter().map(|c| {
                schema.index_of(c)
                    .ok_or_else(|| MonaxError::Sql(format!("column '{}' not found", c)))
            }).collect::<Result<Vec<_>>>()?
        };

        if col_indices.len() != values[0].len() {
            return Err(MonaxError::Sql("column count doesn't match value count".into()));
        }

        // Build record batch
        let mut batch = RecordBatch::new(schema.clone());
        for row in values {
            for (val_idx, &col_idx) in col_indices.iter().enumerate() {
                let scalar = eval_expr_to_scalar(&row[val_idx], &schema.columns[col_idx].data_type)?;
                batch.column_mut(col_idx).unwrap().push(&scalar)?;
            }
        }

        drop(catalogs); // release read lock

        // Determine partition key and write directory
        let table_dir = self.data_dir.join(db).join(table);
        let (write_dir, partition_key) = self.determine_write_dir(&table_meta, &batch, &table_dir)?;
        std::fs::create_dir_all(&write_dir)?;

        // Write to WAL (write-ahead log)
        self.write_wal(&table_dir, &batch)?;

        // Apply sort key insertion if the table has cluster keys
        let has_sort_key = !table_meta.cluster_key.is_empty();
        if has_sort_key {
            // For sort-keyed tables, sort the batch before writing
            let key_indices: Vec<usize> = table_meta.cluster_key.iter()
                .filter_map(|k| schema.index_of(k))
                .collect();
            if !key_indices.is_empty() {
                let inserter = crate::storage::sort_key::SortKeyInserter::new(key_indices, vec![false]);
                if !inserter.is_sorted(&batch) {
                    // Sort the batch by sort key columns
                    batch = sort_batch_by_key(&batch, &inserter)?;
                }
            }
        }

        // Write to segment (use partition filename if partitioned)
        let seg_path = if partition_key.is_empty() {
            self.next_segment_path(&write_dir, "")?
        } else {
            self.next_partition_segment_path(&write_dir, &partition_key)?
        };
        let mut writer = SegmentWriter::create(&seg_path, schema, 8192)?;
        writer.write_batch(&batch)?;
        writer.seal()?;

        // Clear WAL after successful segment write to prevent duplicate data on recovery
        {
            let mut wal_writers = self.wal_writers.lock().unwrap();
            let key = table_dir.to_string_lossy().to_string();
            if let Some(wal_writer) = wal_writers.get_mut(&key) {
                wal_writer.clear()?;
            }
        }

        // Update row count in catalog
        let mut catalogs = self.catalogs.write().unwrap();
        if let Some(catalog) = catalogs.get_mut(db) {
            if let Some(meta) = catalog.get_table_mut(table) {
                meta.row_count += values.len() as i64;
                meta.segment_count += 1;
            }
            let meta_dir = self.data_dir.join("meta");
            catalog.save(&meta_dir)?;
        }

        Ok(ExecResult::Ok { rows_affected: values.len() as u64 })
    }

    /// Execute an UPDATE statement.
    /// For columnar storage, this reads all segments, modifies matching rows, and rewrites.
    fn exec_update(
        &self,
        db: &str,
        table: &str,
        assignments: &[Assignment],
        where_clause: &Option<Expr>,
    ) -> Result<ExecResult> {
        // Get table schema and metadata
        let (table_schema, _table_meta) = {
            let catalogs = self.catalogs.read().unwrap();
            let catalog = catalogs.get(db)
                .ok_or_else(|| MonaxError::Sql(format!("database '{}' does not exist", db)))?;
            let meta = catalog.get_table(table)
                .ok_or_else(|| MonaxError::Sql(format!("table '{}.{}' does not exist", db, table)))?
                .clone();
            (Schema::new(meta.columns.clone()), meta)
        };

        // Read all existing data
        let table_dir = self.data_dir.join(db).join(table);
        let mut all_batches = Vec::new();
        if table_dir.exists() {
            let segment_files = discover_segments(&table_dir)?;
            for seg_path in segment_files {
                let reader = SegmentReader::open(&seg_path)?;
                let batches = reader.scan(4096)?;
                all_batches.extend(batches);
            }
        }

        if all_batches.is_empty() {
            return Ok(ExecResult::Ok { rows_affected: 0 });
        }

        // Build column index map for assignments
        let _assignment_map: HashMap<String, usize> = table_schema.columns.iter()
            .enumerate()
            .map(|(i, c)| (c.name.clone(), i))
            .collect();

        // Process each batch, applying updates
        let mut updated_count = 0u64;
        let mut new_batches = Vec::new();
        for batch in all_batches {
            let mut new_batch = RecordBatch::new(batch.schema.clone());
            for row_idx in 0..batch.num_rows() {
                // Check WHERE clause
                let matches = if let Some(ref where_expr) = where_clause {
                    eval_where_clause(where_expr, &batch, row_idx, &table_schema)?
                } else {
                    true // No WHERE means update all rows
                };

                if matches {
                    // Copy row with modifications
                    for col_idx in 0..batch.num_columns() {
                        let col_name = &table_schema.columns[col_idx].name;
                        // Check if this column is being updated
                        let is_assigned = assignments.iter().any(|a| &a.column == col_name);
                        let val = if is_assigned {
                            let assign = assignments.iter().find(|a| &a.column == col_name).unwrap();
                            eval_expr_to_scalar(&assign.value, &table_schema.columns[col_idx].data_type)?
                        } else {
                            batch.scalar(row_idx, col_idx)
                                .unwrap_or(ScalarValue::null_for_type(table_schema.columns[col_idx].data_type))
                        };
                        new_batch.column_mut(col_idx).unwrap().push(&val)?;
                    }
                    updated_count += 1;
                } else {
                    // Copy row unchanged
                    for col_idx in 0..batch.num_columns() {
                        let val = batch.scalar(row_idx, col_idx)
                            .unwrap_or(ScalarValue::null_for_type(table_schema.columns[col_idx].data_type));
                        new_batch.column_mut(col_idx).unwrap().push(&val)?;
                    }
                }
            }
            if new_batch.num_rows() > 0 {
                new_batches.push(new_batch);
            }
        }

        // Rewrite segments with updated data
        // First, delete old segments
        if table_dir.exists() {
            for seg_file in discover_segments(&table_dir)? {
                std::fs::remove_file(&seg_file)?;
            }
        }

        // Write new segments
        for batch in new_batches {
            let seg_path = self.next_segment_path(&table_dir, "")?;
            let mut writer = SegmentWriter::create(&seg_path, batch.schema.clone(), 8192)?;
            writer.write_batch(&batch)?;
            writer.seal()?;
        }

        // Clear WAL after successful segment write
        {
            let mut wal_writers = self.wal_writers.lock().unwrap();
            let key = table_dir.to_string_lossy().to_string();
            if let Some(wal_writer) = wal_writers.get_mut(&key) {
                wal_writer.clear()?;
            }
        }

        // Update catalog row count (unchanged for UPDATE)
        let mut catalogs = self.catalogs.write().unwrap();
        if let Some(catalog) = catalogs.get_mut(db) {
            if let Some(_meta) = catalog.get_table_mut(table) {
                // Row count unchanged for UPDATE
                let meta_dir = self.data_dir.join("meta");
                catalog.save(&meta_dir)?;
            }
        }

        Ok(ExecResult::Ok { rows_affected: updated_count })
    }

    /// Execute a DELETE statement.
    /// For columnar storage, this reads all segments, filters out matching rows, and rewrites.
    fn exec_delete(
        &self,
        db: &str,
        table: &str,
        where_clause: &Option<Expr>,
    ) -> Result<ExecResult> {
        // Get table schema and metadata
        let (table_schema, _table_meta) = {
            let catalogs = self.catalogs.read().unwrap();
            let catalog = catalogs.get(db)
                .ok_or_else(|| MonaxError::Sql(format!("database '{}' does not exist", db)))?;
            let meta = catalog.get_table(table)
                .ok_or_else(|| MonaxError::Sql(format!("table '{}.{}' does not exist", db, table)))?
                .clone();
            (Schema::new(meta.columns.clone()), meta)
        };

        // Read all existing data
        let table_dir = self.data_dir.join(db).join(table);
        let mut all_batches = Vec::new();
        if table_dir.exists() {
            let segment_files = discover_segments(&table_dir)?;
            for seg_path in segment_files {
                let reader = SegmentReader::open(&seg_path)?;
                let batches = reader.scan(4096)?;
                all_batches.extend(batches);
            }
        }

        if all_batches.is_empty() {
            return Ok(ExecResult::Ok { rows_affected: 0 });
        }

        let original_rows = all_batches.iter().map(|b| b.num_rows()).sum::<usize>();

        // Process each batch, filtering out deleted rows
        let mut new_batches = Vec::new();
        for batch in all_batches {
            let mut new_batch = RecordBatch::new(batch.schema.clone());
            for row_idx in 0..batch.num_rows() {
                // Check WHERE clause
                let should_delete = if let Some(ref where_expr) = where_clause {
                    eval_where_clause(where_expr, &batch, row_idx, &table_schema)?
                } else {
                    true // No WHERE means delete all rows
                };

                if !should_delete {
                    // Keep this row
                    for col_idx in 0..batch.num_columns() {
                        let val = batch.scalar(row_idx, col_idx)
                            .unwrap_or(ScalarValue::null_for_type(table_schema.columns[col_idx].data_type));
                        new_batch.column_mut(col_idx).unwrap().push(&val)?;
                    }
                }
            }
            if new_batch.num_rows() > 0 {
                new_batches.push(new_batch);
            }
        }

        let new_rows = new_batches.iter().map(|b| b.num_rows()).sum::<usize>();
        let deleted_count = (original_rows - new_rows) as u64;

        // Delete old segments
        if table_dir.exists() {
            for seg_file in discover_segments(&table_dir)? {
                std::fs::remove_file(&seg_file)?;
            }
        }

        // Write new segments with remaining data
        for batch in new_batches {
            let seg_path = self.next_segment_path(&table_dir, "")?;
            let mut writer = SegmentWriter::create(&seg_path, batch.schema.clone(), 8192)?;
            writer.write_batch(&batch)?;
            writer.seal()?;
        }

        // Clear WAL after successful segment write
        {
            let mut wal_writers = self.wal_writers.lock().unwrap();
            let key = table_dir.to_string_lossy().to_string();
            if let Some(wal_writer) = wal_writers.get_mut(&key) {
                wal_writer.clear()?;
            }
        }

        // Update catalog row count
        let mut catalogs = self.catalogs.write().unwrap();
        if let Some(catalog) = catalogs.get_mut(db) {
            if let Some(meta) = catalog.get_table_mut(table) {
                meta.row_count = new_rows as i64;
                let meta_dir = self.data_dir.join("meta");
                catalog.save(&meta_dir)?;
            }
        }

        Ok(ExecResult::Ok { rows_affected: deleted_count })
    }

    /// Write a batch to the WAL for write-ahead logging.
    fn write_wal(&self, table_dir: &Path, batch: &RecordBatch) -> Result<()> {
        let wal_dir = table_dir.join("wal");
        let key = table_dir.to_string_lossy().to_string();

        let wal_data = serialize_batch_for_wal(batch);

        let txn_id = {
            let mut txn = self.next_txn_id.lock().unwrap();
            *txn += 1;
            *txn - 1
        };

        let mut wal_writers = self.wal_writers.lock().unwrap();
        if !wal_writers.contains_key(&key) {
            let writer = WalWriter::open(&wal_dir, 64 * 1024 * 1024)?;
            wal_writers.insert(key.clone(), writer);
        }
        let wal_writer = wal_writers.get_mut(&key).unwrap();
        wal_writer.write_batch(txn_id, &[(RecordType::Insert, wal_data.as_slice())])?;

        Ok(())
    }

    /// Determine the write directory and partition filename for an insert, considering partitioning.
    /// Returns (write_dir, partition_key) where partition_key is empty for non-partitioned tables.
    fn determine_write_dir(
        &self,
        table_meta: &TableMeta,
        batch: &RecordBatch,
        default_dir: &Path,
    ) -> Result<(PathBuf, String)> {
        if table_meta.partition_key.is_empty() {
            return Ok((default_dir.to_path_buf(), String::new()));
        }

        // Find the partition column index
        let part_col_idx = batch.schema.columns.iter()
            .position(|c| c.name == table_meta.partition_key);

        let Some(col_idx) = part_col_idx else {
            return Ok((default_dir.to_path_buf(), String::new()));
        };

        // Get the partition strategy
        let strategy = match table_meta.partition_type {
            PartitionType::Function => {
                // Try to determine if day/month/year from partitions config
                crate::storage::partition::PartitionStrategy::Day
            }
            PartitionType::Value => crate::storage::partition::PartitionStrategy::Day,
            PartitionType::None => return Ok((default_dir.to_path_buf(), String::new())),
        };

        // Compute partition key from the first row's partition column value
        let first_val = batch.scalar(0, col_idx)
            .unwrap_or(ScalarValue::Int64(None));

        let partition_key = crate::storage::partition::compute_partition_key(&first_val, strategy)
            .unwrap_or_else(|_| "default".to_string());

        // Partitioned files go in the same table directory with p{key}_ prefix
        Ok((default_dir.to_path_buf(), partition_key))
    }

    fn exec_select(&self, db: &str, stmt: &Statement) -> Result<ExecResult> {
        // Extract the FROM clause to determine if this is a table scan or literal select
        let from = match stmt {
            Statement::Select { from, .. } => from,
            _ => return Err(MonaxError::Internal("exec_select called with non-SELECT".into())),
        };

        match from {
            None => {
                // SELECT without FROM (e.g., SELECT 1)
                let select_list = match stmt {
                    Statement::Select { select_list, .. } => select_list,
                    _ => unreachable!(),
                };
                let schema = Schema::new(vec![ColumnDef::new("1", DataType::Int64)]);
                let mut batch = RecordBatch::new(schema.clone());
                match &select_list[0] {
                    SelectItem::Expr(expr, _) => {
                        let val = eval_expr_to_scalar(expr, &DataType::Int64)?;
                        batch.column_mut(0).unwrap().push(&val)?;
                    }
                    SelectItem::Star => {
                        batch.column_mut(0).unwrap().push(&ScalarValue::Int64(Some(1)))?;
                    }
                }
                Ok(ExecResult::Query { schema, batches: vec![batch] })
            }
            Some(from_clause) => {
                // Extract all table names from FROM clause
                let table_names = Self::extract_table_names(from_clause);

                // Get schemas for all tables
                let mut table_schemas = HashMap::new();
                let catalogs = self.catalogs.read().unwrap();
                let catalog = catalogs.get(db)
                    .ok_or_else(|| MonaxError::Sql(format!("database '{}' does not exist", db)))?;

                for table_name in &table_names {
                    let meta = catalog.get_table(table_name)
                        .ok_or_else(|| MonaxError::Sql(format!("table '{}.{}' does not exist", db, table_name)))?
                        .clone();
                    table_schemas.insert(table_name.clone(), Schema::new(meta.columns.clone()));
                }

                // Plan the query
                let plan = planner::plan_select(stmt, &table_schemas)?;

                // Optimize
                let plan = optimizer::optimize(plan);

                // Load data for all tables
                let mut table_batches: HashMap<String, Vec<RecordBatch>> = HashMap::new();
                for table_name in &table_names {
                    let table_dir = self.data_dir.join(db).join(table_name);
                    let mut batches = Vec::new();
                    if table_dir.exists() {
                        let segment_files = discover_segments(&table_dir)?;
                        for seg_path in segment_files {
                            let reader = SegmentReader::open(&seg_path)?;
                            batches.extend(reader.scan(4096)?);
                        }
                    }
                    table_batches.insert(table_name.clone(), batches);
                }

                // Build and run the pipeline
                let mut pipeline = operators::build_pipeline(plan, &table_batches)?;
                let mut result_batches = Vec::new();
                let mut result_schema = None;
                while let Some(batch) = pipeline.next_batch()? {
                    if result_schema.is_none() {
                        result_schema = Some(batch.schema.clone());
                    }
                    result_batches.push(batch);
                }

                let schema = result_schema.unwrap_or_else(|| Schema::empty());
                Ok(ExecResult::Query { schema, batches: result_batches })
            }
        }
    }

    /// Extract all table names from a FROM clause.
    fn extract_table_names(from: &FromClause) -> Vec<String> {
        match from {
            FromClause::Table(t) => vec![t.name.clone()],
            FromClause::Join(join_expr) => {
                let mut names = Vec::new();
                Self::collect_table_names_from_join(join_expr, &mut names);
                names
            }
        }
    }

    fn collect_table_names_from_join(join_expr: &JoinExpr, names: &mut Vec<String>) {
        match &*join_expr.left {
            FromClause::Table(t) => names.push(t.name.clone()),
            FromClause::Join(inner) => Self::collect_table_names_from_join(inner, names),
        }
        for jc in &join_expr.joins {
            names.push(jc.right.name.clone());
        }
    }

                /// Prune segment files using sort key min/max index.
    /// Reads segment footers to extract min/max values for sort key columns,
    /// then uses range predicates from WHERE clause to prune.
    #[allow(dead_code)]
    fn prune_segments_by_sort_key(
        &self,
        segment_files: &[PathBuf],
        cluster_key: &[String],
        stmt: &Statement,
    ) -> Vec<PathBuf> {
        // Extract WHERE clause range predicates for sort key column
        let (lower_bound, upper_bound) = extract_range_bounds(stmt, cluster_key);

        // If no bounds, return all files
        if lower_bound.is_none() && upper_bound.is_none() {
            return segment_files.to_vec();
        }

        // Build key indices (assuming first sort key column for pruning)
        let key_indices: Vec<usize> = vec![0]; // Simplified: use first key column
        let inserter = crate::storage::sort_key::SortKeyInserter::new(key_indices, vec![false]);

        // Build segment index by reading min/max from segment footers
        let mut index = crate::storage::sort_key::SegmentIndex::new();
        for seg_path in segment_files {
            // Try to read segment footer to get min/max
            if let Ok(reader) = SegmentReader::open(seg_path) {
                // Read first batch to extract min/max (simplified approach)
                if let Ok(batches) = reader.scan(8192) {
                    if let Some(first_batch) = batches.first() {
                        // Find sort key column index in the batch schema
                        if let Some(key_col_idx) = first_batch.schema.columns.iter()
                            .position(|c| cluster_key.contains(&c.name))
                        {
                            let inserter_for_col = crate::storage::sort_key::SortKeyInserter::new(vec![key_col_idx], vec![false]);
                            let min_max = inserter_for_col.extract_min_max(first_batch);
                            index.add_entry(seg_path.clone(), first_batch.num_rows(), min_max);
                        } else {
                            // No sort key column found, include the segment
                            index.add_entry(seg_path.clone(), first_batch.num_rows(), vec![]);
                        }
                    } else {
                        // Empty segment, include it
                        index.add_entry(seg_path.clone(), 0, vec![]);
                    }
                }
            } else {
                // Can't read segment, include it anyway
                index.add_entry(seg_path.clone(), 0, vec![]);
            }
        }

        // Prune using range bounds
        index.prune_ranges(&inserter, lower_bound.as_ref(), upper_bound.as_ref())
    }

    /// Prune partition segments based on WHERE clause bounds on partition column.
    #[allow(dead_code)]
    fn prune_partition_segments(
        &self,
        segment_files: &[PathBuf],
        partition_key_col: &str,
        stmt: &Statement,
        strategy: crate::storage::partition::PartitionStrategy,
    ) -> Vec<PathBuf> {
        // Extract WHERE clause bounds for partition column
        let (lower_bound, upper_bound) = extract_range_bounds(stmt, &[partition_key_col.to_string()]);

        // If no bounds, return all files
        if lower_bound.is_none() && upper_bound.is_none() {
            return segment_files.to_vec();
        }

        let pruner = crate::storage::partition::PartitionPruner::new(strategy);

        // Discover available partition keys from filenames
        let available_partitions: Vec<String> = segment_files.iter()
            .filter_map(|p| p.file_name().and_then(|n| n.to_str()))
            .filter_map(|name| crate::storage::partition::parse_partition_key(name))
            .collect();

        // Prune partitions
        let matching_partitions = pruner.prune(&available_partitions, lower_bound.as_ref(), upper_bound.as_ref());

        // Filter segment files by matching partition keys
        segment_files.iter()
            .filter(|p| {
                if let Some(name) = p.file_name().and_then(|n| n.to_str()) {
                    if let Some(key) = crate::storage::partition::parse_partition_key(name) {
                        return matching_partitions.contains(&key);
                    }
                }
                true // Include non-partition segments
            })
            .cloned()
            .collect()
    }

    fn exec_show_tables(&self, db: &str) -> Result<ExecResult> {
        let catalogs = self.catalogs.read().unwrap();
        let schema = Schema::new(vec![
            ColumnDef::new("table_name", DataType::VarChar),
        ]);

        let mut batch = RecordBatch::new(schema.clone());

        if let Some(catalog) = catalogs.get(db) {
            for name in catalog.table_names() {
                batch.column_mut(0).unwrap().push(&ScalarValue::VarChar(Some(name.to_string())))?;
            }
        }

        Ok(ExecResult::Query { schema, batches: vec![batch] })
    }

    fn exec_show_databases(&self) -> Result<ExecResult> {
        let catalogs = self.catalogs.read().unwrap();
        let schema = Schema::new(vec![
            ColumnDef::new("database", DataType::VarChar),
        ]);

        let mut batch = RecordBatch::new(schema.clone());
        for name in catalogs.keys() {
            batch.column_mut(0).unwrap().push(&ScalarValue::VarChar(Some(name.clone())))?;
        }

        Ok(ExecResult::Query { schema, batches: vec![batch] })
    }

    fn get_or_create_catalog<'a>(
        &self,
        catalogs: &'a mut HashMap<String, DatabaseCatalog>,
        db: &str,
    ) -> Result<&'a mut DatabaseCatalog> {
        if !catalogs.contains_key(db) {
            let meta_dir = self.data_dir.join("meta");
            let catalog = DatabaseCatalog::load_or_create(&meta_dir, db)?;
            catalogs.insert(db.to_string(), catalog);
        }
        Ok(catalogs.get_mut(db).unwrap())
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    pub fn next_segment_path(&self, table_dir: &Path, _partition_key: &str) -> Result<PathBuf> {
        let key = table_dir.to_string_lossy().to_string();
        let mut seqs = self.segment_seqs.lock().unwrap();
        let seq = seqs.entry(key).or_insert(0);
        *seq += 1;
        let filename = format!("seg_{:05}.dat", *seq);
        Ok(table_dir.join(filename))
    }

    /// Get next segment path for a partitioned table.
    pub fn next_partition_segment_path(&self, table_dir: &Path, partition_key: &str) -> Result<PathBuf> {
        let key = format!("{}:{}", table_dir.to_string_lossy(), partition_key);
        let mut seqs = self.segment_seqs.lock().unwrap();
        let seq = seqs.entry(key).or_insert(0);
        *seq += 1;
        let filename = crate::storage::partition::partition_filename(partition_key, *seq);
        Ok(table_dir.join(filename))
    }

    /// Get table metadata for a given database and table name.
    pub fn get_table_meta(&self, db: &str, table: &str) -> Option<TableMeta> {
        let catalogs = self.catalogs.read().unwrap();
        catalogs.get(db).and_then(|c| c.get_table(table)).cloned()
    }

    pub fn exec_bulk_insert(&self, db: &str, table: &str, batch: RecordBatch) -> Result<u64> {
        let row_count = batch.num_rows() as u64;
        if row_count == 0 {
            return Ok(0);
        }

        let table_dir = self.data_dir.join(db).join(table);
        std::fs::create_dir_all(&table_dir)?;

        let key = format!("{}:{}", db, table);

        let compressed_columns: Vec<(Vec<u8>, u64)> = (0..batch.num_columns())
            .map(|col_idx| {
                let col = batch.column(col_idx).unwrap();
                let encoded = encode_column_direct(col);
                let compressed = zstd::bulk::compress(&encoded, 0)
                    .map_err(|e| MonaxError::Storage(format!("zstd error: {}", e)))?;
                Ok((compressed, encoded.len() as u64))
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

            active_segments.insert(key.clone(), ActiveSegment {
                file: BufWriter::new(file),
                path: seg_path,
                schema,
                row_groups: Vec::new(),
                current_size: 0,
                total_rows: 0,
            });
        }

        let segment = active_segments.get_mut(&key).unwrap();

        let offset = segment.current_size;

        let mut column_sizes = Vec::with_capacity(compressed_columns.len());
        let mut compressed_sizes = Vec::with_capacity(compressed_columns.len());
        let mut uncompressed_sizes = Vec::with_capacity(compressed_columns.len());
        let mut total_length = 0u64;

        for (compressed, uncompressed_size) in &compressed_columns {
            segment.file.write_all(compressed)?;
            let n = compressed.len() as u64;
            column_sizes.push(n);
            compressed_sizes.push(n);
            uncompressed_sizes.push(*uncompressed_size);
            total_length += n;
        }

        segment.row_groups.push(RowGroupMeta {
            offset,
            length: total_length,
            row_count: row_count,
            column_sizes,
            compressed_sizes,
            uncompressed_sizes,
        });

        segment.current_size += total_length;
        segment.total_rows += row_count;

        self.seal_segment(segment)?;
        active_segments.remove(&key);

        {
            let mut catalogs = self.catalogs.write().unwrap();
            if let Some(catalog) = catalogs.get_mut(db) {
                if let Some(meta) = catalog.get_table_mut(table) {
                    meta.row_count += row_count as i64;
                }
            }
        }

        Ok(row_count)
    }

    fn seal_segment(&self, segment: &mut ActiveSegment) -> Result<()> {
        segment.file.flush()?;
        let footer_offset = segment.file.seek(SeekFrom::End(0))?;

        let footer = self.encode_footer(segment)?;
        segment.file.write_all(&footer)?;
        segment.file.write_all(&(footer_offset as u64).to_le_bytes())?;
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

        let table_name = segment.path
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

    pub fn exec_bulk_insert_with_wal_control(&self, db: &str, table: &str, batch: RecordBatch, disable_wal: bool) -> Result<u64> {
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
            let file = std::fs::OpenOptions::new().create(true).write(true).truncate(true).open(&seg_path)?;
            active_segments.insert(key.clone(), ActiveSegment { file: BufWriter::new(file), path: seg_path, schema, row_groups: Vec::new(), current_size: 0, total_rows: 0 });
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
        segment.row_groups.push(RowGroupMeta { offset, length: total_length, row_count, column_sizes, compressed_sizes, uncompressed_sizes });
        segment.current_size += total_length;
        segment.total_rows += row_count;
        if segment.current_size >= MAX_SEGMENT_SIZE {
            self.seal_segment(segment)?;
            active_segments.remove(&key);
        }
        if !disable_wal {
            let bulk_mode = *self.bulk_mode.lock().unwrap();
            if !bulk_mode {
                let mut catalogs = self.catalogs.write().unwrap();
                if let Some(catalog) = catalogs.get_mut(db) {
                    if let Some(meta) = catalog.get_table_mut(table) {
                        meta.row_count += row_count as i64;
                    }
                }
            }
        }
        Ok(row_count)
    }

    pub fn exec_bulk_insert_payload_with_wal_control(&self, db: &str, payload: &[u8], disable_wal: bool) -> Result<u64> {
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
                .ok_or_else(|| MonaxError::Sql(format!("table '{}.{}' does not exist", db, req.table_name)))?;
            Schema::new(table_meta.columns.clone())
        };

        if schema.num_columns() != req.columns.len() {
            return Err(MonaxError::Protocol(format!(
                "bulk column count mismatch: schema has {} columns but payload has {}",
                schema.num_columns(),
                req.columns.len()
            )));
        }

        for (idx, (col_def, payload_col)) in schema.columns.iter().zip(req.columns.iter()).enumerate() {
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
            active_segments.insert(key.clone(), ActiveSegment {
                file: BufWriter::new(file),
                path: seg_path,
                schema,
                row_groups: Vec::new(),
                current_size: 0,
                total_rows: 0,
            });
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

        if segment.current_size >= MAX_SEGMENT_SIZE {
            self.seal_segment(segment)?;
            active_segments.remove(&key);
        }

        if !disable_wal {
            let bulk_mode = *self.bulk_mode.lock().unwrap();
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

    pub fn set_bulk_mode(&self, enabled: bool) {
        let mut mode = self.bulk_mode.lock().unwrap();
        *mode = enabled;
    }
    pub fn flush_all_buffers(&self) -> Result<u64> {
        let mut total_rows = 0u64;
        let mut active_segments = self.active_segments.lock().unwrap();

        let keys: Vec<String> = active_segments.keys().cloned().collect();
        for key in keys {
            if let Some(mut segment) = active_segments.remove(&key) {
                self.seal_segment(&mut segment)?;
                total_rows += segment.total_rows;
            }
        }

        let meta_dir = self.data_dir.join("meta");
        let catalogs = self.catalogs.read().unwrap();
        for (_, catalog) in catalogs.iter() {
            catalog.save(&meta_dir)?;
        }

        Ok(total_rows)
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
            .unwrap_or(3)
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
            return Err(MonaxError::Protocol(format!("column {} data exceeds payload", col_idx)));
        }
        let data = &payload[pos..pos + data_len];
        pos += data_len;

        if pos + nulls_size > payload.len() {
            return Err(MonaxError::Protocol(format!("column {} null bitmap exceeds payload", col_idx)));
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
            return Err(MonaxError::Protocol("variable column length prefix truncated".into()));
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
            let bytes = unsafe { std::slice::from_raw_parts(vals.as_ptr() as *const u8, vals.len() * 4) };
            data.extend_from_slice(bytes);
        }
        crate::types::array::ColumnArray::Int64 { data: vals, .. } => {
            let bytes = unsafe { std::slice::from_raw_parts(vals.as_ptr() as *const u8, vals.len() * 8) };
            data.extend_from_slice(bytes);
        }
        crate::types::array::ColumnArray::Float64 { data: vals, .. } => {
            let bytes = unsafe { std::slice::from_raw_parts(vals.as_ptr() as *const u8, vals.len() * 8) };
            data.extend_from_slice(bytes);
        }
        crate::types::array::ColumnArray::Decimal { data: vals, .. } => {
            let bytes = unsafe { std::slice::from_raw_parts(vals.as_ptr() as *const u8, vals.len() * 8) };
            data.extend_from_slice(bytes);
        }
        crate::types::array::ColumnArray::VarChar { data: vals, .. } => {
            for v in vals {
                data.extend_from_slice(&(v.len() as u32).to_le_bytes());
                data.extend_from_slice(v.as_bytes());
            }
        }
        crate::types::array::ColumnArray::Timestamp { data: vals, .. } => {
            let bytes = unsafe { std::slice::from_raw_parts(vals.as_ptr() as *const u8, vals.len() * 8) };
            data.extend_from_slice(bytes);
        }
        crate::types::array::ColumnArray::Bool { data: vals, .. } => {
            data.extend_from_slice(vals);
        }
        crate::types::array::ColumnArray::Int8 { data: vals, .. } => {
            let bytes = unsafe { std::slice::from_raw_parts(vals.as_ptr() as *const u8, vals.len()) };
            data.extend_from_slice(bytes);
        }
        crate::types::array::ColumnArray::Int16 { data: vals, .. } => {
            let bytes = unsafe { std::slice::from_raw_parts(vals.as_ptr() as *const u8, vals.len() * 2) };
            data.extend_from_slice(bytes);
        }
        crate::types::array::ColumnArray::UInt8 { data: vals, .. } => {
            data.extend_from_slice(vals);
        }
        crate::types::array::ColumnArray::UInt16 { data: vals, .. } => {
            let bytes = unsafe { std::slice::from_raw_parts(vals.as_ptr() as *const u8, vals.len() * 2) };
            data.extend_from_slice(bytes);
        }
        crate::types::array::ColumnArray::UInt32 { data: vals, .. } => {
            let bytes = unsafe { std::slice::from_raw_parts(vals.as_ptr() as *const u8, vals.len() * 4) };
            data.extend_from_slice(bytes);
        }
        crate::types::array::ColumnArray::UInt64 { data: vals, .. } => {
            let bytes = unsafe { std::slice::from_raw_parts(vals.as_ptr() as *const u8, vals.len() * 8) };
            data.extend_from_slice(bytes);
        }
        crate::types::array::ColumnArray::Float32 { data: vals, .. } => {
            let bytes = unsafe { std::slice::from_raw_parts(vals.as_ptr() as *const u8, vals.len() * 4) };
            data.extend_from_slice(bytes);
        }
        crate::types::array::ColumnArray::Date { data: vals, .. } => {
            let bytes = unsafe { std::slice::from_raw_parts(vals.as_ptr() as *const u8, vals.len() * 4) };
            data.extend_from_slice(bytes);
        }
        crate::types::array::ColumnArray::Time { data: vals, .. } => {
            let bytes = unsafe { std::slice::from_raw_parts(vals.as_ptr() as *const u8, vals.len() * 8) };
            data.extend_from_slice(bytes);
        }
        crate::types::array::ColumnArray::Interval { data: vals, .. } => {
            let bytes = unsafe { std::slice::from_raw_parts(vals.as_ptr() as *const u8, vals.len() * 8) };
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

/// Serialize a RecordBatch to bytes for WAL storage.
/// Format: [u32 num_cols][u32 num_rows]
/// Per column: [u8 dtype][u32 data_len][data bytes]
fn serialize_batch_for_wal(batch: &RecordBatch) -> Vec<u8> {
    let mut buf = Vec::new();
    let num_cols = batch.num_columns() as u32;
    let num_rows = batch.num_rows() as u32;

    buf.extend_from_slice(&num_cols.to_le_bytes());
    buf.extend_from_slice(&num_rows.to_le_bytes());

    for col_idx in 0..batch.num_columns() {
        let col = batch.column(col_idx).unwrap();
        buf.push(col.data_type() as u8);

        // Serialize values
        let mut data = Vec::new();
        for i in 0..batch.num_rows() {
            let val = col.scalar(i);
            if val.is_null() {
                data.push(0u8); // null flag
            } else {
                data.push(1u8); // non-null flag
                serialize_scalar(&mut data, &val);
            }
        }
        buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
        buf.extend_from_slice(&data);
    }

    buf
}

/// Serialize a single scalar value to bytes.
fn serialize_scalar(buf: &mut Vec<u8>, val: &ScalarValue) {
    match val {
        ScalarValue::Bool(Some(v)) => buf.push(if *v { 1 } else { 0 }),
        ScalarValue::Int8(Some(v)) => buf.push(*v as u8),
        ScalarValue::Int16(Some(v)) => buf.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::Int32(Some(v)) => buf.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::Int64(Some(v)) => buf.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::UInt8(Some(v)) => buf.push(*v),
        ScalarValue::UInt16(Some(v)) => buf.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::UInt32(Some(v)) => buf.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::UInt64(Some(v)) => buf.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::Float32(Some(v)) => buf.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::Float64(Some(v)) => buf.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::VarChar(Some(v)) => {
            buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
            buf.extend_from_slice(v.as_bytes());
        }
        ScalarValue::Timestamp(Some(v)) => buf.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::Date(Some(v)) => buf.extend_from_slice(&v.to_le_bytes()),
        _ => {}
    }
}

/// Deserialize a RecordBatch from WAL bytes.
/// Format: [u32 num_cols][u32 num_rows]
/// Per column: [u8 dtype][u32 data_len][data bytes]
fn deserialize_batch_from_wal(data: &[u8], schema: &Schema) -> Result<RecordBatch> {
    if data.len() < 8 {
        return Err(MonaxError::Internal("WAL data too short".into()));
    }

    let num_cols = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
    let num_rows = u32::from_le_bytes([data[4], data[5], data[6], data[7]]) as usize;

    if num_cols != schema.columns.len() {
        return Err(MonaxError::Internal(
            format!("WAL column count mismatch: {} vs {}", num_cols, schema.columns.len())
        ));
    }

    let mut batch = RecordBatch::new(schema.clone());
    let mut offset = 8;

    for col_idx in 0..num_cols {
        if offset >= data.len() {
            return Err(MonaxError::Internal("WAL data truncated".into()));
        }

        let _dtype_byte = data[offset];
        offset += 1;

        if offset + 4 > data.len() {
            return Err(MonaxError::Internal("WAL data truncated".into()));
        }
        let data_len = u32::from_le_bytes([
            data[offset], data[offset + 1], data[offset + 2], data[offset + 3]
        ]) as usize;
        offset += 4;

        let col_data = &data[offset..offset + data_len];
        offset += data_len;

        // Deserialize values from col_data
        let mut val_offset = 0;
        for _row_idx in 0..num_rows {
            if val_offset >= col_data.len() {
                break;
            }
            let null_flag = col_data[val_offset];
            val_offset += 1;

            if null_flag == 0 {
                // Null value
                batch.column_mut(col_idx).unwrap().push(&ScalarValue::null_for_type(schema.columns[col_idx].data_type))?;
            } else {
                // Non-null value
                let val = deserialize_scalar(col_data, &mut val_offset, schema.columns[col_idx].data_type)?;
                batch.column_mut(col_idx).unwrap().push(&val)?;
            }
        }
    }

    Ok(batch)
}

/// Deserialize a single scalar value from bytes.
fn deserialize_scalar(data: &[u8], offset: &mut usize, dtype: DataType) -> Result<ScalarValue> {
    match dtype {
        DataType::Bool => {
            let v = data[*offset];
            *offset += 1;
            Ok(ScalarValue::Bool(Some(v != 0)))
        }
        DataType::Int8 => {
            let v = data[*offset] as i8;
            *offset += 1;
            Ok(ScalarValue::Int8(Some(v)))
        }
        DataType::Int16 => {
            if *offset + 2 > data.len() {
                return Err(MonaxError::Internal("WAL data truncated for Int16".into()));
            }
            let v = i16::from_le_bytes([data[*offset], data[*offset + 1]]);
            *offset += 2;
            Ok(ScalarValue::Int16(Some(v)))
        }
        DataType::Int32 => {
            if *offset + 4 > data.len() {
                return Err(MonaxError::Internal("WAL data truncated for Int32".into()));
            }
            let v = i32::from_le_bytes([
                data[*offset], data[*offset + 1], data[*offset + 2], data[*offset + 3]
            ]);
            *offset += 4;
            Ok(ScalarValue::Int32(Some(v)))
        }
        DataType::Int64 => {
            if *offset + 8 > data.len() {
                return Err(MonaxError::Internal("WAL data truncated for Int64".into()));
            }
            let v = i64::from_le_bytes([
                data[*offset], data[*offset + 1], data[*offset + 2], data[*offset + 3],
                data[*offset + 4], data[*offset + 5], data[*offset + 6], data[*offset + 7]
            ]);
            *offset += 8;
            Ok(ScalarValue::Int64(Some(v)))
        }
        DataType::UInt8 => {
            let v = data[*offset];
            *offset += 1;
            Ok(ScalarValue::UInt8(Some(v)))
        }
        DataType::UInt16 => {
            if *offset + 2 > data.len() {
                return Err(MonaxError::Internal("WAL data truncated for UInt16".into()));
            }
            let v = u16::from_le_bytes([data[*offset], data[*offset + 1]]);
            *offset += 2;
            Ok(ScalarValue::UInt16(Some(v)))
        }
        DataType::UInt32 => {
            if *offset + 4 > data.len() {
                return Err(MonaxError::Internal("WAL data truncated for UInt32".into()));
            }
            let v = u32::from_le_bytes([
                data[*offset], data[*offset + 1], data[*offset + 2], data[*offset + 3]
            ]);
            *offset += 4;
            Ok(ScalarValue::UInt32(Some(v)))
        }
        DataType::UInt64 => {
            if *offset + 8 > data.len() {
                return Err(MonaxError::Internal("WAL data truncated for UInt64".into()));
            }
            let v = u64::from_le_bytes([
                data[*offset], data[*offset + 1], data[*offset + 2], data[*offset + 3],
                data[*offset + 4], data[*offset + 5], data[*offset + 6], data[*offset + 7]
            ]);
            *offset += 8;
            Ok(ScalarValue::UInt64(Some(v)))
        }
        DataType::Float32 => {
            if *offset + 4 > data.len() {
                return Err(MonaxError::Internal("WAL data truncated for Float32".into()));
            }
            let v = f32::from_le_bytes([
                data[*offset], data[*offset + 1], data[*offset + 2], data[*offset + 3]
            ]);
            *offset += 4;
            Ok(ScalarValue::Float32(Some(v)))
        }
        DataType::Float64 => {
            if *offset + 8 > data.len() {
                return Err(MonaxError::Internal("WAL data truncated for Float64".into()));
            }
            let v = f64::from_le_bytes([
                data[*offset], data[*offset + 1], data[*offset + 2], data[*offset + 3],
                data[*offset + 4], data[*offset + 5], data[*offset + 6], data[*offset + 7]
            ]);
            *offset += 8;
            Ok(ScalarValue::Float64(Some(v)))
        }
        DataType::VarChar => {
            if *offset + 4 > data.len() {
                return Err(MonaxError::Internal("WAL data truncated for VarChar length".into()));
            }
            let len = u32::from_le_bytes([
                data[*offset], data[*offset + 1], data[*offset + 2], data[*offset + 3]
            ]) as usize;
            *offset += 4;
            if *offset + len > data.len() {
                return Err(MonaxError::Internal("WAL data truncated for VarChar value".into()));
            }
            let s = String::from_utf8_lossy(&data[*offset..*offset + len]).to_string();
            *offset += len;
            Ok(ScalarValue::VarChar(Some(s)))
        }
        DataType::Timestamp => {
            if *offset + 8 > data.len() {
                return Err(MonaxError::Internal("WAL data truncated for Timestamp".into()));
            }
            let v = i64::from_le_bytes([
                data[*offset], data[*offset + 1], data[*offset + 2], data[*offset + 3],
                data[*offset + 4], data[*offset + 5], data[*offset + 6], data[*offset + 7]
            ]);
            *offset += 8;
            Ok(ScalarValue::Timestamp(Some(v)))
        }
        DataType::Date => {
            if *offset + 4 > data.len() {
                return Err(MonaxError::Internal("WAL data truncated for Date".into()));
            }
            let v = i32::from_le_bytes([
                data[*offset], data[*offset + 1], data[*offset + 2], data[*offset + 3]
            ]);
            *offset += 4;
            Ok(ScalarValue::Date(Some(v)))
        }
        _ => Ok(ScalarValue::null_for_type(dtype)),
    }
}

/// Evaluate a WHERE clause expression for a specific row.
/// Returns true if the row matches the condition.
fn eval_where_clause(
    expr: &Expr,
    batch: &RecordBatch,
    row_idx: usize,
    schema: &Schema,
) -> Result<bool> {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            let left_val = eval_expr_for_row(left, batch, row_idx, schema)?;
            let right_val = eval_expr_for_row(right, batch, row_idx, schema)?;
            match op {
                BinOp::Eq => Ok(left_val == right_val),
                BinOp::Neq => Ok(left_val != right_val),
                BinOp::Lt => Ok(compare_scalars(&left_val, &right_val) == Some(std::cmp::Ordering::Less)),
                BinOp::Gt => Ok(compare_scalars(&left_val, &right_val) == Some(std::cmp::Ordering::Greater)),
                BinOp::Lte => Ok(compare_scalars(&left_val, &right_val) != Some(std::cmp::Ordering::Greater)),
                BinOp::Gte => Ok(compare_scalars(&left_val, &right_val) != Some(std::cmp::Ordering::Less)),
                BinOp::And => {
                    let left_bool = eval_where_clause(left, batch, row_idx, schema)?;
                    let right_bool = eval_where_clause(right, batch, row_idx, schema)?;
                    Ok(left_bool && right_bool)
                }
                BinOp::Or => {
                    let left_bool = eval_where_clause(left, batch, row_idx, schema)?;
                    let right_bool = eval_where_clause(right, batch, row_idx, schema)?;
                    Ok(left_bool || right_bool)
                }
                _ => Ok(true),
            }
        }
        Expr::IsNull(inner) => {
            let val = eval_expr_for_row(inner, batch, row_idx, schema)?;
            Ok(val.is_null())
        }
        Expr::IsNotNull(inner) => {
            let val = eval_expr_for_row(inner, batch, row_idx, schema)?;
            Ok(!val.is_null())
        }
        Expr::UnaryNot(inner) => {
            let inner_bool = eval_where_clause(inner, batch, row_idx, schema)?;
            Ok(!inner_bool)
        }
        Expr::Literal(v) => {
            // A literal as a condition is truthy if non-null and non-zero
            Ok(!v.is_null())
        }
        Expr::Column(_) => Ok(true), // Column alone as condition (unusual)
        Expr::QualifiedColumn { .. } => Ok(true), // Qualified column in WHERE (table.column)
        Expr::Function { .. } => Ok(true), // Functions in WHERE not fully supported
    }
}

/// Evaluate an expression to get a scalar value for a specific row.
fn eval_expr_for_row(
    expr: &Expr,
    batch: &RecordBatch,
    row_idx: usize,
    schema: &Schema,
) -> Result<ScalarValue> {
    match expr {
        Expr::Column(col_name) => {
            let col_idx = schema.index_of(col_name)
                .ok_or_else(|| MonaxError::Sql(format!("column '{}' not found", col_name)))?;
            Ok(batch.scalar(row_idx, col_idx)
                .unwrap_or(ScalarValue::null_for_type(schema.columns[col_idx].data_type)))
        }
        Expr::Literal(v) => Ok(v.clone()),
        Expr::BinaryOp { left, op, right } => {
            // For arithmetic ops in WHERE, just return left value (simplified)
            let left_val = eval_expr_for_row(left, batch, row_idx, schema)?;
            let right_val = eval_expr_for_row(right, batch, row_idx, schema)?;
            // Only handle comparison ops here - arithmetic not supported in WHERE
            match op {
                BinOp::Add => {
                    match (&left_val, &right_val) {
                        (ScalarValue::Int64(Some(a)), ScalarValue::Int64(Some(b))) => Ok(ScalarValue::Int64(Some(a + b))),
                        _ => Ok(left_val),
                    }
                }
                BinOp::Sub => {
                    match (&left_val, &right_val) {
                        (ScalarValue::Int64(Some(a)), ScalarValue::Int64(Some(b))) => Ok(ScalarValue::Int64(Some(a - b))),
                        _ => Ok(left_val),
                    }
                }
                _ => Ok(left_val),
            }
        }
        Expr::IsNull(inner) => eval_expr_for_row(inner, batch, row_idx, schema),
        Expr::IsNotNull(inner) => eval_expr_for_row(inner, batch, row_idx, schema),
        Expr::UnaryNot(inner) => eval_expr_for_row(inner, batch, row_idx, schema),
        Expr::QualifiedColumn { column, .. } => {
            // For qualified column, find by column name in the merged schema
            let col_idx = schema.index_of(column)
                .ok_or_else(|| MonaxError::Sql(format!("column '{}' not found", column)))?;
            Ok(batch.scalar(row_idx, col_idx)
                .unwrap_or(ScalarValue::null_for_type(schema.columns[col_idx].data_type)))
        }
        Expr::Function { .. } => Ok(ScalarValue::Int64(None)),
    }
}

/// Extract range bounds (lower, upper) from WHERE clause for the given key columns.
/// Returns (lower_bound, upper_bound) as ScalarValues if range predicates are found.
#[allow(dead_code)]
fn extract_range_bounds(stmt: &Statement, key_columns: &[String]) -> (Option<ScalarValue>, Option<ScalarValue>) {
    let where_clause = match stmt {
        Statement::Select { where_clause, .. } => where_clause,
        Statement::Update { where_clause, .. } => where_clause,
        Statement::Delete { where_clause, .. } => where_clause,
        _ => &None,
    };

    let Some(where_expr) = where_clause else {
        return (None, None);
    };

    // Extract bounds from comparison expressions
    extract_bounds_from_expr(where_expr, key_columns)
}

/// Recursively extract bounds from an expression.
#[allow(dead_code)]
fn extract_bounds_from_expr(expr: &Expr, key_columns: &[String]) -> (Option<ScalarValue>, Option<ScalarValue>) {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            // Check if this is a comparison on a key column
            match op {
                BinOp::Eq => {
                    // Equality is both lower and upper bound
                    if let Expr::Column(col) = left.as_ref() {
                        if key_columns.contains(col) {
                            if let Expr::Literal(val) = right.as_ref() {
                                return (Some(val.clone()), Some(val.clone()));
                            }
                        }
                    }
                    if let Expr::Column(col) = right.as_ref() {
                        if key_columns.contains(col) {
                            if let Expr::Literal(val) = left.as_ref() {
                                return (Some(val.clone()), Some(val.clone()));
                            }
                        }
                    }
                    (None, None)
                }
                BinOp::Lt => {
                    // col < val means upper_bound = val (exclusive, but we use inclusive for simplicity)
                    if let Expr::Column(col) = left.as_ref() {
                        if key_columns.contains(col) {
                            if let Expr::Literal(val) = right.as_ref() {
                                return (None, Some(val.clone()));
                            }
                        }
                    }
                    (None, None)
                }
                BinOp::Gt => {
                    // col > val means lower_bound = val (exclusive)
                    if let Expr::Column(col) = left.as_ref() {
                        if key_columns.contains(col) {
                            if let Expr::Literal(val) = right.as_ref() {
                                return (Some(val.clone()), None);
                            }
                        }
                    }
                    (None, None)
                }
                BinOp::Lte => {
                    // col <= val means upper_bound = val
                    if let Expr::Column(col) = left.as_ref() {
                        if key_columns.contains(col) {
                            if let Expr::Literal(val) = right.as_ref() {
                                return (None, Some(val.clone()));
                            }
                        }
                    }
                    (None, None)
                }
                BinOp::Gte => {
                    // col >= val means lower_bound = val
                    if let Expr::Column(col) = left.as_ref() {
                        if key_columns.contains(col) {
                            if let Expr::Literal(val) = right.as_ref() {
                                return (Some(val.clone()), None);
                            }
                        }
                    }
                    (None, None)
                }
                BinOp::And => {
                    // Combine bounds from both sides
                    let (left_lower, left_upper) = extract_bounds_from_expr(left, key_columns);
                    let (right_lower, right_upper) = extract_bounds_from_expr(right, key_columns);
                    // Take the tighter bounds
                    let lower = match (left_lower, right_lower) {
                        (Some(l), Some(r)) => Some(if compare_scalars(&l, &r) == Some(std::cmp::Ordering::Greater) { l } else { r }),
                        (Some(l), None) => Some(l),
                        (None, Some(r)) => Some(r),
                        (None, None) => None,
                    };
                    let upper = match (left_upper, right_upper) {
                        (Some(l), Some(r)) => Some(if compare_scalars(&l, &r) == Some(std::cmp::Ordering::Less) { l } else { r }),
                        (Some(l), None) => Some(l),
                        (None, Some(r)) => Some(r),
                        (None, None) => None,
                    };
                    (lower, upper)
                }
                _ => (None, None),
            }
        }
        _ => (None, None),
    }
}

/// Compare two scalar values, returning their ordering.
fn compare_scalars(a: &ScalarValue, b: &ScalarValue) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (ScalarValue::Int64(Some(a)), ScalarValue::Int64(Some(b))) => Some(a.cmp(b)),
        (ScalarValue::Int32(Some(a)), ScalarValue::Int32(Some(b))) => Some(a.cmp(b)),
        (ScalarValue::Float64(Some(a)), ScalarValue::Float64(Some(b))) => a.partial_cmp(b),
        (ScalarValue::VarChar(Some(a)), ScalarValue::VarChar(Some(b))) => Some(a.cmp(b)),
        _ => None,
    }
}

/// Evaluate an expression to a scalar value.
fn eval_expr_to_scalar(expr: &Expr, dt: &DataType) -> Result<ScalarValue> {
    match expr {
        Expr::Literal(v) => {
            // Coerce literal to target type
            match (dt, v) {
                (DataType::Int32, ScalarValue::Int64(Some(v))) => Ok(ScalarValue::Int32(Some(*v as i32))),
                (DataType::Int16, ScalarValue::Int64(Some(v))) => Ok(ScalarValue::Int16(Some(*v as i16))),
                (DataType::Int8, ScalarValue::Int64(Some(v))) => Ok(ScalarValue::Int8(Some(*v as i8))),
                (DataType::Float32, ScalarValue::Float64(Some(v))) => Ok(ScalarValue::Float32(Some(*v as f32))),
                _ => Ok(v.clone()),
            }
        }
        Expr::Column(_) => {
            Ok(ScalarValue::null_for_type(*dt))
        }
        _ => {
            Ok(ScalarValue::null_for_type(*dt))
        }
    }
}

/// Sort a batch by its sort key columns.
fn sort_batch_by_key(batch: &RecordBatch, inserter: &crate::storage::sort_key::SortKeyInserter) -> Result<RecordBatch> {
    let num_rows = batch.num_rows();
    if num_rows <= 1 {
        return Ok(batch.clone());
    }

    // Build a list of (sort_key_values, row_index) pairs
    let mut rows_with_keys: Vec<(Vec<ScalarValue>, usize)> = Vec::with_capacity(num_rows);
    for row_idx in 0..num_rows {
        let key_values: Vec<ScalarValue> = inserter.key_indices.iter()
            .map(|&col_idx| batch.scalar(row_idx, col_idx)
                .unwrap_or(ScalarValue::null_for_type(batch.schema.columns[col_idx].data_type)))
            .collect();
        rows_with_keys.push((key_values, row_idx));
    }

    // Sort by key values
    rows_with_keys.sort_by(|a, b| {
        for (ki, _) in inserter.key_indices.iter().enumerate() {
            let cmp = crate::storage::sort_key::compare_scalars_for_sort(&a.0[ki], &b.0[ki]);
            let cmp = if inserter.desc.get(ki).copied().unwrap_or(false) {
                cmp.reverse()
            } else {
                cmp
            };
            if cmp != std::cmp::Ordering::Equal {
                return cmp;
            }
        }
        std::cmp::Ordering::Equal
    });

    // Build a new batch with rows in sorted order
    let mut sorted_batch = RecordBatch::new(batch.schema.clone());
    for (_, orig_row_idx) in rows_with_keys {
        for col_idx in 0..batch.num_columns() {
            let val = batch.scalar(orig_row_idx, col_idx)
                .unwrap_or(ScalarValue::null_for_type(batch.schema.columns[col_idx].data_type));
            sorted_batch.column_mut(col_idx).unwrap().push(&val)?;
        }
    }

    Ok(sorted_batch)
}

/// Discover segment files in a table directory (includes partition segments).
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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn make_engine() -> (TempDir, Engine) {
        let dir = TempDir::new().unwrap();
        let engine = Engine::new(dir.path()).unwrap();
        (dir, engine)
    }

    fn make_session() -> Session {
        Session::new("test".into(), "default".into())
    }

    #[test]
    fn test_create_and_show_tables() {
        let (_dir, engine) = make_engine();
        let mut session = make_session();

        engine.execute(&mut session, "CREATE TABLE users (id INT64, name VARCHAR)").unwrap();
        let result = engine.execute(&mut session, "SHOW TABLES").unwrap();

        match result {
            ExecResult::Query { batches, .. } => {
                assert_eq!(batches[0].num_rows(), 1);
                assert_eq!(
                    batches[0].scalar(0, 0).unwrap(),
                    ScalarValue::VarChar(Some("users".into()))
                );
            }
            _ => panic!("expected query result"),
        }
    }

    #[test]
    fn test_insert_and_select() {
        let (_dir, engine) = make_engine();
        let mut session = make_session();

        engine.execute(&mut session, "CREATE TABLE t (id INT64, val VARCHAR)").unwrap();
        engine.execute(&mut session, "INSERT INTO t (id, val) VALUES (1, 'hello'), (2, 'world')").unwrap();

        let result = engine.execute(&mut session, "SELECT * FROM t").unwrap();
        match result {
            ExecResult::Query { schema, batches } => {
                assert_eq!(schema.columns.len(), 2);
                let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                assert_eq!(total_rows, 2);
            }
            _ => panic!("expected query result"),
        }
    }

    #[test]
    fn test_drop_table() {
        let (_dir, engine) = make_engine();
        let mut session = make_session();

        engine.execute(&mut session, "CREATE TABLE t (id INT64)").unwrap();
        engine.execute(&mut session, "DROP TABLE t").unwrap();

        let result = engine.execute(&mut session, "SHOW TABLES").unwrap();
        match result {
            ExecResult::Query { batches, .. } => {
                assert_eq!(batches[0].num_rows(), 0);
            }
            _ => panic!("expected query result"),
        }
    }

    #[test]
    fn test_use_database() {
        let (_dir, engine) = make_engine();
        let mut session = make_session();

        engine.execute(&mut session, "USE testdb").unwrap();
        assert_eq!(session.database, "testdb");
    }

    #[test]
    fn test_select_literal() {
        let (_dir, engine) = make_engine();
        let mut session = make_session();

        let result = engine.execute(&mut session, "SELECT 42").unwrap();
        match result {
            ExecResult::Query { batches, .. } => {
                assert_eq!(batches[0].num_rows(), 1);
            }
            _ => panic!("expected query result"),
        }
    }

    #[test]
    fn test_duplicate_table_error() {
        let (_dir, engine) = make_engine();
        let mut session = make_session();

        engine.execute(&mut session, "CREATE TABLE t (id INT64)").unwrap();
        let result = engine.execute(&mut session, "CREATE TABLE t (id INT64)");
        assert!(result.is_err());
    }

    #[test]
    fn test_create_if_not_exists() {
        let (_dir, engine) = make_engine();
        let mut session = make_session();

        engine.execute(&mut session, "CREATE TABLE t (id INT64)").unwrap();
        engine.execute(&mut session, "CREATE TABLE IF NOT EXISTS t (id INT64)").unwrap();
    }

    #[test]
    fn test_wal_written_on_insert() {
        let (_dir, engine) = make_engine();
        let mut session = make_session();

        engine.execute(&mut session, "CREATE TABLE t (id INT64, name VARCHAR)").unwrap();
        engine.execute(&mut session, "INSERT INTO t (id, name) VALUES (1, 'test')").unwrap();

        // Verify WAL file was created
        let wal_dir = _dir.path().join("default").join("t").join("wal");
        assert!(wal_dir.exists(), "WAL directory should exist after insert");
    }

    #[test]
    fn test_bulk_insert() {
        let (_dir, engine) = make_engine();
        let mut session = make_session();

        engine.execute(&mut session, "CREATE TABLE t (id INT64, val VARCHAR)").unwrap();

        let schema = Schema::new(vec![
            ColumnDef::new("id", DataType::Int64),
            ColumnDef::new("val", DataType::VarChar),
        ]);
        let mut batch = RecordBatch::new(schema);
        batch.column_mut(0).unwrap().push(&ScalarValue::Int64(Some(1))).unwrap();
        batch.column_mut(1).unwrap().push(&ScalarValue::VarChar(Some("hello".into()))).unwrap();

        let rows = engine.exec_bulk_insert("default", "t", batch).unwrap();
        assert_eq!(rows, 1);

        let result = engine.execute(&mut session, "SELECT * FROM t").unwrap();
        match result {
            ExecResult::Query { batches, .. } => {
                let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                assert_eq!(total_rows, 1);
            }
            _ => panic!("expected query result"),
        }
    }
}
