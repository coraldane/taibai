use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use crate::common::error::{MonaxError, Result};
use crate::net::session::Session;
use crate::storage::schema::DatabaseCatalog;
use crate::storage::segment_manager::SegmentManager;

use crate::sql::parser::ast::Statement;
use crate::sql::parser::parser::Parser;

use crate::types::batch::{RecordBatch, Schema};

pub enum ExecResult {
    Ok { rows_affected: u64 },
    Query { schema: Schema, batches: Vec<RecordBatch> },
}

pub struct Engine {
    data_dir: PathBuf,
    catalogs: RwLock<HashMap<String, DatabaseCatalog>>,
    segment_mgr: Arc<SegmentManager>,
    bulk_mode: AtomicBool,
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
                                catalogs.insert(db_name, catalog);
                            }
                        }
                    }
                }
            }
        }

        let segment_mgr = Arc::new(SegmentManager::new(data_dir)?);
        SegmentManager::start_flush_thread(&segment_mgr);

        Ok(Self {
            data_dir: data_dir.to_path_buf(),
            catalogs: RwLock::new(catalogs),
            segment_mgr,
            bulk_mode: AtomicBool::new(false),
        })
    }

    pub fn execute(&self, session: &mut Session, sql: &str) -> Result<ExecResult> {
        if sql.as_bytes().get(0).map(|b| b.to_ascii_uppercase()) == Some(b'I') {
            if let Some(result) = crate::sql::parser::fast_insert::try_fast_parse_insert(sql) {
                let stmt = result?;
                return self.execute_stmt(session, &stmt);
            }
        }
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
            Statement::Explain { analyze, stmt: inner } => {
                super::explain::exec_explain(self, &session.database, inner, *analyze)
            }
        }
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    pub fn segment_mgr(&self) -> &Arc<SegmentManager> {
        &self.segment_mgr
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
        self.segment_mgr.alloc_txn_id()
    }

    pub fn flush_all_buffers(&self) -> Result<u64> {
        self.segment_mgr.flush_all_active()?;

        {
            let mut catalogs = self.catalogs.write().unwrap();
            let meta_dir = self.data_dir.join("meta");
            for (_, catalog) in catalogs.iter_mut() {
                let _ = catalog.save(&meta_dir);
            }
        }

        Ok(0)
    }

    pub(crate) fn catalogs(&self) -> &RwLock<HashMap<String, DatabaseCatalog>> {
        &self.catalogs
    }

    pub fn exec_bulk_insert_with_wal_control(
        &self,
        db: &str,
        table: &str,
        batch: RecordBatch,
        disable_wal: bool,
    ) -> Result<u64> {
        self.segment_mgr.bulk_insert_batch(db, table, batch, disable_wal)
    }

    pub fn exec_bulk_insert_payload_with_wal_control(
        &self,
        db: &str,
        payload: &[u8],
        disable_wal: bool,
    ) -> Result<u64> {
        let catalogs = self.catalogs.read().unwrap();
        let catalog = catalogs
            .get(db)
            .ok_or_else(|| MonaxError::Sql(format!("database '{}' does not exist", db)))?;

        let result = self.segment_mgr.bulk_insert_payload_with_catalog(db, payload, disable_wal, catalog)?;
        drop(catalogs);

        if !disable_wal {
            let bulk_mode = self.bulk_mode.load(Ordering::Acquire);
            if !bulk_mode {
                let req = {
                    let catalogs = self.catalogs.read().unwrap();
                    let catalog = catalogs.get(db)
                        .ok_or_else(|| MonaxError::Sql(format!("database '{}' does not exist", db)))?;
                    let table_name = {
                        let mut pos = 0usize;
                        let payload_bytes = payload;
                        if payload_bytes.len() < 2 {
                            return Err(MonaxError::Protocol("bulk payload too short".into()));
                        }
                        let table_len = u16::from_le_bytes(payload_bytes[0..2].try_into().unwrap()) as usize;
                        pos += 2;
                        if pos + table_len > payload_bytes.len() {
                            return Err(MonaxError::Protocol("bulk payload truncated".into()));
                        }
                        std::str::from_utf8(&payload_bytes[pos..pos + table_len])
                            .map_err(|e| MonaxError::Protocol(format!("invalid utf8: {}", e)))?
                            .to_string()
                    };
                    if catalog.get_table(&table_name).is_some() {
                        let row_count = result as i64;
                        drop(catalogs);
                        let mut catalogs_mut = self.catalogs.write().unwrap();
                        if let Some(catalog) = catalogs_mut.get_mut(db) {
                            if let Some(meta) = catalog.get_table_mut(&table_name) {
                                meta.row_count += row_count;
                            }
                            let meta_dir = self.data_dir.join("meta");
                            catalog.save(&meta_dir)?;
                        }
                    }
                    result
                };
                return Ok(req);
            }
        }

        Ok(result)
    }
}
