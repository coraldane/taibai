#![allow(dead_code)]
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use crate::common::error::{MonaxError, Result};
use crate::net::session::Session;
use crate::storage::catalog::auth::AuthManager;
use crate::storage::catalog::auto_increment::AutoIncrementManager;
use crate::storage::catalog::schema::DatabaseCatalog;
use crate::storage::core::batch_writer::{BatchWriter, BatchWriterConfig};
use crate::storage::core::memtable::MemTableManager;
use crate::storage::index::index_manager::IndexManager;
use crate::storage::segment_manager::SegmentManager;

use crate::sql::parser::ast::Statement;
use crate::sql::parser::parser::Parser;

use crate::types::batch::{RecordBatch, Schema};

#[derive(Debug)]
pub enum ExecResult {
    Ok {
        rows_affected: u64,
    },
    Query {
        schema: Schema,
        batches: Vec<RecordBatch>,
    },
}

pub struct Engine {
    data_dir: PathBuf,
    catalogs: RwLock<HashMap<String, DatabaseCatalog>>,
    segment_mgr: Arc<SegmentManager>,
    batch_writer: BatchWriter,
    bulk_mode: AtomicBool,
    auth_manager: Arc<AuthManager>,
    auto_increment_mgr: Arc<AutoIncrementManager>,
    index_manager: Arc<IndexManager>,
}

impl Engine {
    pub fn new(data_dir: &Path, default_database: &str) -> Result<Self> {
        let mut catalogs = HashMap::new();
        let meta_dir = data_dir.join("meta");
        if meta_dir.exists() {
            if let Ok(catalog) = DatabaseCatalog::load_or_create(&meta_dir, default_database) {
                catalogs.insert(default_database.to_string(), catalog);
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
                            if let Ok(catalog) =
                                DatabaseCatalog::load_or_create(&meta_dir, &db_name)
                            {
                                catalogs.insert(db_name, catalog);
                            }
                        }
                    }
                }
            }
        }

        let segment_mgr = Arc::new(SegmentManager::new(data_dir)?);
        SegmentManager::start_flush_thread(&segment_mgr);

        let batch_writer = BatchWriter::new(BatchWriterConfig::default(), Arc::clone(&segment_mgr));

        let auth_manager = Arc::new(AuthManager::new(data_dir)?);
        let auto_increment_mgr = Arc::new(AutoIncrementManager::new());
        let index_manager = Arc::new(IndexManager::new(Arc::clone(&segment_mgr)));

        Ok(Self {
            data_dir: data_dir.to_path_buf(),
            catalogs: RwLock::new(catalogs),
            segment_mgr,
            batch_writer,
            bulk_mode: AtomicBool::new(false),
            auth_manager,
            auto_increment_mgr,
            index_manager,
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

    pub fn execute_prepared(
        &self,
        session: &mut Session,
        prepared: &mut crate::sql::execution::prepared::PreparedStatement,
    ) -> Result<ExecResult> {
        let has_placeholders = crate::sql::execution::prepared::has_placeholders(&prepared.stmt);
        let mut stmt = prepared.stmt.clone();
        if !prepared.params.is_empty() {
            crate::sql::execution::prepared::bind_params(&mut stmt, &prepared.params)?;
        }
        match &stmt {
            Statement::Select { .. } => {
                if let Some(ref plan) = prepared.cached_plan {
                    super::query::exec_select_with_plan(
                        self,
                        &session.database,
                        &stmt,
                        plan.clone(),
                    )
                } else {
                    let result = super::query::exec_select(self, &session.database, &stmt);
                    if let Ok(ref _r) = result {
                        // Only cache plan if statement has no placeholders (params won't change structure)
                        if !has_placeholders {
                            let table_schemas =
                                self.gather_table_schemas(&session.database, &stmt)?;
                            if let Ok(plan) =
                                crate::sql::planner::plan_select(&stmt, &table_schemas)
                            {
                                prepared.cached_plan = Some(crate::sql::optimizer::optimize(plan));
                            }
                        }
                    }
                    result
                }
            }
            _ => self.execute_stmt(session, &stmt),
        }
    }

    fn gather_table_schemas(
        &self,
        db: &str,
        stmt: &Statement,
    ) -> Result<std::collections::HashMap<String, crate::types::batch::Schema>> {
        use crate::types::batch::Schema;
        let mut table_names = Vec::new();
        if let Statement::Select { from, .. } = stmt {
            if let Some(ref from_clause) = from {
                table_names.extend(super::query::extract_table_names(from_clause));
            }
        }
        let mut table_schemas = std::collections::HashMap::new();
        let catalogs = self.catalogs().read().unwrap();
        if let Some(catalog) = catalogs.get(db) {
            for table_name in &table_names {
                if let Some(meta) = catalog.get_table(table_name) {
                    table_schemas.insert(table_name.clone(), Schema::new(meta.columns.clone()));
                }
            }
        }
        Ok(table_schemas)
    }

    fn execute_stmt(&self, session: &mut Session, stmt: &Statement) -> Result<ExecResult> {
        match stmt {
            Statement::CreateTable {
                name,
                columns,
                if_not_exists,
                cluster_by,
                partition_by,
            } => super::ddl::exec_create_table(
                self,
                &session.database,
                name,
                columns,
                *if_not_exists,
                cluster_by,
                partition_by,
            ),
            Statement::DropTable { name, if_exists } => {
                super::ddl::exec_drop_table(self, &session.database, name, *if_exists)
            }
            Statement::AlterTable { name, operations } => {
                super::ddl::exec_alter_table(self, &session.database, name, operations)
            }
            Statement::Truncate { table } => {
                super::ddl::exec_truncate_table(self, &session.database, table)
            }
            Statement::CreateDatabase {
                name,
                if_not_exists,
            } => super::ddl::exec_create_database(self, name, *if_not_exists),
            Statement::DropDatabase { name, if_exists } => {
                super::ddl::exec_drop_database(self, name, *if_exists)
            }
            Statement::CreateUser { name, password } => {
                super::ddl::exec_create_user(self, name, password)
            }
            Statement::CreateRole { name } => super::ddl::exec_create_role(self, name),
            Statement::DropUser { name, if_exists } => {
                super::ddl::exec_drop_user(self, name, *if_exists)
            }
            Statement::DropRole { name, if_exists } => {
                super::ddl::exec_drop_role(self, name, *if_exists)
            }
            Statement::Grant { privileges, on, to } => {
                super::ddl::exec_grant(self, privileges, on, to)
            }
            Statement::Revoke {
                privileges,
                on,
                from,
            } => super::ddl::exec_revoke(self, privileges, on, from),
            Statement::Insert {
                table,
                columns,
                values,
            } => {
                let txn_id = session.active_txn_id();
                let result = super::dml_insert::exec_insert(
                    self,
                    &session.database,
                    table,
                    columns,
                    values,
                    session.timezone_offset_hours,
                    txn_id,
                );
                if result.is_ok() && txn_id > 0 {
                    session.mark_txn_write();
                }
                result
            }
            Statement::Update {
                table,
                assignments,
                where_clause,
            } => super::dml_update::exec_update(
                self,
                &session.database,
                table,
                assignments,
                where_clause,
            ),
            Statement::Delete {
                table,
                where_clause,
            } => super::dml_delete::exec_delete(self, &session.database, table, where_clause),
            Statement::Select { .. } => super::query::exec_select(self, &session.database, stmt),
            Statement::UseDatabase(database) => {
                let catalogs = self.catalogs().read().unwrap();
                if !catalogs.contains_key(database.as_str()) {
                    return Err(MonaxError::Sql(format!(
                        "database '{}' does not exist",
                        database
                    )));
                }
                drop(catalogs);
                session.database = database.clone();
                Ok(ExecResult::Ok { rows_affected: 0 })
            }
            Statement::ShowTables => super::ddl::exec_show_tables(self, &session.database),
            Statement::ShowDatabases => super::ddl::exec_show_databases(self),
            Statement::ShowColumns { table } => {
                super::ddl::exec_show_columns(self, &session.database, table)
            }
            Statement::ShowIndexes { table } => {
                super::ddl::exec_show_indexes(self, &session.database, table)
            }
            Statement::ShowCreateTable { table } => {
                super::ddl::exec_show_create_table(self, &session.database, table)
            }
            Statement::Explain {
                analyze,
                stmt: inner,
            } => super::explain::exec_explain(self, &session.database, inner, *analyze),
            Statement::Begin => {
                if session.in_txn() {
                    return Err(MonaxError::Sql("transaction already in progress".into()));
                }
                let txn_id = self.alloc_txn_id();
                session.begin_txn(txn_id);
                Ok(ExecResult::Ok {
                    rows_affected: txn_id,
                })
            }
            Statement::Commit => {
                if !session.in_txn() {
                    return Err(MonaxError::Sql("no active transaction".into()));
                }
                let txn_id = session.active_txn_id();
                let had_writes = session
                    .active_txn
                    .as_ref()
                    .map(|t| t.had_writes)
                    .unwrap_or(false);
                if had_writes {
                    self.batch_writer().commit_txn(txn_id)?;
                }
                session.commit_txn();
                Ok(ExecResult::Ok { rows_affected: 0 })
            }
            Statement::Rollback => {
                if !session.in_txn() {
                    return Err(MonaxError::Sql("no active transaction".into()));
                }
                let txn_id = session.active_txn_id();
                let rolled_back = self.batch_writer().rollback_txn(txn_id)?;
                tracing::debug!(
                    txn_id,
                    rows_rolled_back = rolled_back,
                    "transaction rolled back"
                );
                session.rollback_txn();
                Ok(ExecResult::Ok {
                    rows_affected: rolled_back,
                })
            }
            Statement::AlterUser { name, operations } => {
                super::ddl::exec_alter_user(self, name, operations)
            }
            Statement::Union {
                left,
                right,
                all,
                order_by,
                limit,
                offset,
            } => super::query::exec_union(
                self,
                &session.database,
                left,
                right,
                *all,
                order_by,
                limit,
                offset,
            ),
            Statement::ShowUsers => super::ddl::exec_show_users(self),
            Statement::ShowRoles => super::ddl::exec_show_roles(self),
            Statement::ShowGrants { user } => super::ddl::exec_show_grants(self, user),
            Statement::ShowPartitions { table } => {
                super::ddl::exec_show_partitions(self, &session.database, table)
            }
            Statement::ShowStats => super::ddl::exec_show_stats(self, &session.database),
            Statement::ShowEngines => super::ddl::exec_show_engines(self),
            Statement::ShowProcesslist { full } => super::ddl::exec_show_processlist(self, *full),
            Statement::FlushTable { table } => {
                super::ddl::exec_flush_table(self, &session.database, table)
            }
            Statement::OptimizeTable { table } => {
                super::ddl::exec_optimize_table(self, &session.database, table)
            }
            Statement::PauseFlush => {
                self.segment_mgr().pause_flush();
                Ok(ExecResult::Ok { rows_affected: 0 })
            }
            Statement::ResumeFlush => {
                self.segment_mgr().resume_flush();
                Ok(ExecResult::Ok { rows_affected: 0 })
            }
        }
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    pub fn segment_mgr(&self) -> &Arc<SegmentManager> {
        &self.segment_mgr
    }

    pub fn batch_writer(&self) -> &BatchWriter {
        &self.batch_writer
    }

    pub fn memtable_manager(&self) -> &Arc<MemTableManager> {
        self.batch_writer.memtable_manager()
    }

    pub fn auth_manager(&self) -> &Arc<AuthManager> {
        &self.auth_manager
    }

    pub fn auto_increment_manager(&self) -> &Arc<AutoIncrementManager> {
        &self.auto_increment_mgr
    }

    pub fn index_manager(&self) -> &Arc<IndexManager> {
        &self.index_manager
    }

    pub fn get_table_meta(
        &self,
        db: &str,
        table: &str,
    ) -> Option<crate::storage::catalog::schema::TableMeta> {
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
        if let Err(e) = self.batch_writer.flush_all() {
            tracing::warn!(error = %e, "batch writer flush failed");
        }

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
        self.segment_mgr
            .bulk_insert_batch(db, table, batch, disable_wal)
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

        let result =
            self.segment_mgr
                .bulk_insert_payload_with_catalog(db, payload, disable_wal, catalog)?;
        drop(catalogs);

        if !disable_wal {
            let bulk_mode = self.bulk_mode.load(Ordering::Acquire);
            if !bulk_mode {
                let req = {
                    let catalogs = self.catalogs.read().unwrap();
                    let catalog = catalogs.get(db).ok_or_else(|| {
                        MonaxError::Sql(format!("database '{}' does not exist", db))
                    })?;
                    let table_name = {
                        let mut pos = 0usize;
                        let payload_bytes = payload;
                        if payload_bytes.len() < 2 {
                            return Err(MonaxError::Protocol("bulk payload too short".into()));
                        }
                        let table_len =
                            u16::from_le_bytes(payload_bytes[0..2].try_into().unwrap()) as usize;
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
