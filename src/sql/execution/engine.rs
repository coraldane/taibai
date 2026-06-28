#![allow(dead_code)]
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use crate::common::error::{Result, TaibaiError};
use crate::net::session::Session;
use crate::storage::catalog::auth::{AuthManager, PermissionObjectType, PermissionType};
use crate::storage::catalog::auto_increment::AutoIncrementManager;
use crate::storage::catalog::schema::DatabaseCatalog;
use crate::storage::core::batch_writer::{BatchWriter, BatchWriterConfig};
use crate::storage::core::memtable::MemTableManager;
use crate::storage::index::index_manager::IndexManager;
use crate::storage::segment_manager::{SegmentConfig, SegmentManager};

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
    catalog_save_state: Mutex<CatalogSaveState>,
    segment_mgr: Arc<SegmentManager>,
    batch_writer: BatchWriter,
    bulk_mode_sessions: AtomicUsize,
    auto_compact_enabled: AtomicBool,
    in_flight_operations: AtomicUsize,
    last_activity_epoch_ms: AtomicU64,
    auth_manager: Arc<AuthManager>,
    auto_increment_mgr: Arc<AutoIncrementManager>,
    index_manager: Arc<IndexManager>,
}

struct CatalogSaveState {
    pending_insert_rows: u64,
    last_save_at: Instant,
}

const CATALOG_SAVE_INSERT_ROWS: u64 = 500_000;
const CATALOG_SAVE_INTERVAL: Duration = Duration::from_secs(1);

impl Engine {
    pub fn new(data_dir: &Path, default_database: &str) -> Result<Self> {
        Self::new_with_storage_config(data_dir, default_database, SegmentConfig::default())
    }
    pub fn new_with_storage_config(
        data_dir: &Path,
        default_database: &str,
        segment_config: SegmentConfig,
    ) -> Result<Self> {
        let mut catalogs = HashMap::new();
        let meta_dir = data_dir.join("meta");

        if meta_dir.exists() {
            let catalog_dir = DatabaseCatalog::catalog_dir(&meta_dir);
            if catalog_dir.exists() {
                if let Ok(entries) = std::fs::read_dir(&catalog_dir) {
                    for entry in entries.flatten() {
                        let path = entry.path();
                        if path.extension().and_then(|s| s.to_str()) != Some("json") {
                            continue;
                        }
                        if let Some(db_name) = path.file_stem().and_then(|s| s.to_str()) {
                            if let Ok(catalog) = DatabaseCatalog::load_or_create(&meta_dir, db_name)
                            {
                                catalogs.insert(db_name.to_string(), catalog);
                            }
                        }
                    }
                }
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
                        if let std::collections::hash_map::Entry::Vacant(e) =
                            catalogs.entry(db_name.clone())
                        {
                            if let Ok(catalog) =
                                DatabaseCatalog::load_or_create(&meta_dir, &db_name)
                            {
                                e.insert(catalog);
                            }
                        }
                    }
                }
            }
        }

        if !catalogs.contains_key(default_database) {
            let catalog = DatabaseCatalog::load_or_create(&meta_dir, default_database)?;
            catalogs.insert(default_database.to_string(), catalog);
        }

        let segment_mgr = Arc::new(SegmentManager::with_config(data_dir, segment_config)?);
        SegmentManager::start_flush_thread(&segment_mgr);

        let batch_writer = BatchWriter::new(BatchWriterConfig::default(), Arc::clone(&segment_mgr));

        let auth_manager = Arc::new(AuthManager::new(data_dir)?);
        let auto_increment_mgr = Arc::new(AutoIncrementManager::new());
        for (db_name, catalog) in &catalogs {
            for table_meta in catalog.tables.values() {
                for (col_idx, col) in table_meta.columns.iter().enumerate() {
                    if col.auto_increment {
                        let catalog_start = (table_meta.row_count.max(0) as u64) + 1;
                        let start_value = match restored_auto_increment_start(
                            &segment_mgr,
                            data_dir,
                            db_name,
                            &table_meta.name,
                            col_idx,
                            catalog_start,
                        ) {
                            Ok(value) => value,
                            Err(e) => {
                                tracing::warn!(
                                    error = %e,
                                    table = %table_meta.name,
                                    column = %col.name,
                                    "failed to scan auto_increment max value, using catalog row count"
                                );
                                catalog_start
                            }
                        };
                        auto_increment_mgr.register(
                            db_name,
                            &table_meta.name,
                            &col.name,
                            start_value,
                        )?;
                    }
                }
            }
        }
        let index_manager = Arc::new(IndexManager::new(Arc::clone(&segment_mgr)));

        Ok(Self {
            data_dir: data_dir.to_path_buf(),
            catalogs: RwLock::new(catalogs),
            catalog_save_state: Mutex::new(CatalogSaveState {
                pending_insert_rows: 0,
                last_save_at: Instant::now(),
            }),
            segment_mgr,
            batch_writer,
            bulk_mode_sessions: AtomicUsize::new(0),
            auto_compact_enabled: AtomicBool::new(true),
            in_flight_operations: AtomicUsize::new(0),
            last_activity_epoch_ms: AtomicU64::new(current_epoch_millis()),
            auth_manager,
            auto_increment_mgr,
            index_manager,
        })
    }

    pub fn execute(&self, session: &mut Session, sql: &str) -> Result<ExecResult> {
        let _op_guard = self.begin_operation();
        if sql.as_bytes().first().map(|b| b.to_ascii_uppercase()) == Some(b'I') {
            if let Some(result) = crate::sql::parser::fast_insert::try_fast_parse_insert(sql) {
                let stmt = result?;
                return self.execute_stmt(session, &stmt);
            }
        }
        let stmts = Parser::parse(sql)?;
        if stmts.is_empty() {
            return Err(TaibaiError::Sql("empty query".into()));
        }
        self.execute_stmt(session, &stmts[0])
    }

    pub fn execute_prepared(
        &self,
        session: &mut Session,
        prepared: &mut crate::sql::execution::prepared::PreparedStatement,
    ) -> Result<ExecResult> {
        let _op_guard = self.begin_operation();
        let has_placeholders = crate::sql::execution::prepared::has_placeholders(&prepared.stmt);
        let mut stmt = prepared.stmt.clone();
        if !prepared.params.is_empty() {
            crate::sql::execution::prepared::bind_params(&mut stmt, &prepared.params)?;
        }
        self.ensure_permissions(session, &stmt)?;
        match &stmt {
            Statement::Select { .. } => {
                if let Some(plan) = prepared.cached_plan.as_ref() {
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
                                prepared.planned_database = Some(session.database.clone());
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
        let mut table_refs = Vec::new();
        if let Statement::Select {
            from: Some(ref from_clause),
            ..
        } = stmt
        {
            table_refs.extend(super::query::extract_table_refs(from_clause, db));
        }
        let mut table_schemas = std::collections::HashMap::new();
        let catalogs = self.catalogs().read().unwrap();
        for table_ref in &table_refs {
            if let Some(catalog) = catalogs.get(&table_ref.database) {
                if let Some(meta) = catalog.get_table(&table_ref.table) {
                    table_schemas.insert(table_ref.key.clone(), Schema::new(meta.columns.clone()));
                }
            }
        }
        Ok(table_schemas)
    }

    fn execute_stmt(&self, session: &mut Session, stmt: &Statement) -> Result<ExecResult> {
        self.ensure_permissions(session, stmt)?;
        match stmt {
            Statement::CreateTable {
                name,
                columns,
                if_not_exists,
                primary_key,
                unique_keys,
                cluster_by,
                partition_by,
                ttl,
            } => {
                let db = name.effective_database(&session.database).to_string();
                let table = name.name.clone();
                let result = super::ddl::exec_create_table(
                    self,
                    &db,
                    &table,
                    columns,
                    *if_not_exists,
                    primary_key,
                    unique_keys,
                    cluster_by,
                    partition_by,
                    ttl,
                );
                result
            }
            Statement::CreateIndex {
                name,
                table,
                columns,
                unique,
                if_not_exists,
                index_type,
            } => super::ddl::exec_create_index(
                self,
                table.effective_database(&session.database),
                &table.name,
                name,
                columns,
                *unique,
                *if_not_exists,
                index_type.as_deref(),
            ),
            Statement::DropIndex {
                name,
                table,
                if_exists,
            } => super::ddl::exec_drop_index(
                self,
                table.effective_database(&session.database),
                &table.name,
                name,
                *if_exists,
            ),
            Statement::DropTable { name, if_exists } => {
                let db = name.effective_database(&session.database).to_string();
                let table = name.name.clone();
                let result = super::ddl::exec_drop_table(self, &db, &table, *if_exists);
                result
            }
            Statement::AlterTable { name, operations } => {
                let db = name.effective_database(&session.database).to_string();
                let table = name.name.clone();
                let result = super::ddl::exec_alter_table(self, &db, &table, operations);
                result
            }
            Statement::Truncate { table } => {
                let db = table.effective_database(&session.database).to_string();
                let table_name = table.name.clone();
                let result = super::ddl::exec_truncate_table(self, &db, &table_name);
                result
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
                    table.effective_database(&session.database),
                    &table.name,
                    columns,
                    values,
                    session.timezone_offset_hours,
                    txn_id,
                    session.bulk_import_mode,
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
            } => {
                let db = table.effective_database(&session.database).to_string();
                let result = super::dml_update::exec_update(
                    self,
                    &db,
                    &table.name,
                    assignments,
                    where_clause,
                );
                if result.is_ok() && session.in_txn() {
                    session.mark_txn_write();
                }
                result
            }
            Statement::Delete {
                table,
                where_clause,
            } => {
                let db = table.effective_database(&session.database).to_string();
                let result = super::dml_delete::exec_delete(
                    self,
                    &db,
                    &table.name,
                    where_clause,
                );
                if result.is_ok() && session.in_txn() {
                    session.mark_txn_write();
                }
                result
            }
            Statement::Select { .. } => super::query::exec_select(self, &session.database, stmt),
            Statement::UseDatabase(database) => {
                let catalogs = self.catalogs().read().unwrap();
                if !catalogs.contains_key(database.as_str()) {
                    return Err(TaibaiError::Sql(format!(
                        "database '{}' does not exist",
                        database
                    )));
                }
                drop(catalogs);
                session.database = database.clone();
                Ok(ExecResult::Ok { rows_affected: 0 })
            }
            Statement::ShowTables { database } => {
                let db = database.as_deref().unwrap_or(&session.database);
                super::ddl::exec_show_tables(self, db)
            }
            Statement::ShowDatabases => super::ddl::exec_show_databases(self),
            Statement::ShowColumns { table } => super::ddl::exec_show_columns(
                self,
                table.effective_database(&session.database),
                &table.name,
            ),
            Statement::ShowIndexes { table } => super::ddl::exec_show_indexes(
                self,
                table.effective_database(&session.database),
                &table.name,
            ),
            Statement::ShowCreateTable { table } => super::ddl::exec_show_create_table(
                self,
                table.effective_database(&session.database),
                &table.name,
            ),
            Statement::Explain {
                analyze,
                stmt: inner,
            } => super::explain::exec_explain(self, &session.database, inner, *analyze),
            Statement::Begin => {
                if session.in_txn() {
                    return Err(TaibaiError::Sql("transaction already in progress".into()));
                }
                let txn_id = self.alloc_txn_id();
                session.begin_txn(txn_id);
                Ok(ExecResult::Ok {
                    rows_affected: txn_id,
                })
            }
            Statement::Commit => {
                if !session.in_txn() {
                    return Err(TaibaiError::Sql("no active transaction".into()));
                }
                self.commit_session_txn(session)?;
                Ok(ExecResult::Ok { rows_affected: 0 })
            }
            Statement::Rollback => {
                if !session.in_txn() {
                    return Err(TaibaiError::Sql("no active transaction".into()));
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
            Statement::ShowStats => super::ddl::exec_show_stats(self, &session.database),
            Statement::ShowEngines => super::ddl::exec_show_engines(self),
            Statement::ShowProcesslist { full } => super::ddl::exec_show_processlist(self, *full),
            Statement::AlterUser { name, operations } => super::ddl::exec_alter_user(self, name, operations),
            Statement::ShowUsers => super::ddl::exec_show_users(self),
            Statement::ShowRoles => super::ddl::exec_show_roles(self),
            Statement::ShowGrants { user } => super::ddl::exec_show_grants(self, user),
            Statement::ShowPartitions { table } => {
                let db = table.as_ref().map(|t| t.effective_database(&session.database)).unwrap_or(&session.database);
                let table_name = table.as_ref().map(|t| t.name.clone());
                super::ddl::exec_show_partitions(self, db, &table_name)
            }
            Statement::FlushTable { table } => {
                let db = table.as_ref().map(|t| t.effective_database(&session.database)).unwrap_or(&session.database);
                let table_name = table.as_ref().map(|t| t.name.clone());
                super::ddl::exec_flush_table(self, db, &table_name)
            },
            Statement::Union { .. } => super::query::exec_select(self, &session.database, stmt),
            Statement::OptimizeTable { table, partition } => super::ddl::exec_optimize_table(
                self,
                table.effective_database(&session.database),
                &table.name,
                partition.as_deref(),
            ),
            Statement::StartAutoCompact => {
                self.set_auto_compact_enabled(true);
                Ok(ExecResult::Ok { rows_affected: 0 })
            }
            Statement::StopAutoCompact => {
                self.set_auto_compact_enabled(false);
                Ok(ExecResult::Ok { rows_affected: 0 })
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

    fn ensure_permissions(&self, session: &Session, stmt: &Statement) -> Result<()> {
        match stmt {
            Statement::CreateTable { name, .. } => self.require_table_permission(
                session,
                PermissionType::Create,
                name.effective_database(&session.database),
                &name.name,
            ),
            Statement::CreateIndex { table, .. } => self.require_table_permission(
                session,
                PermissionType::Create,
                table.effective_database(&session.database),
                &table.name,
            ),
            Statement::DropIndex { table, .. } => self.require_table_permission(
                session,
                PermissionType::Drop,
                table.effective_database(&session.database),
                &table.name,
            ),
            Statement::DropTable { name, .. } => self.require_table_permission(
                session,
                PermissionType::Drop,
                name.effective_database(&session.database),
                &name.name,
            ),
            Statement::AlterTable { name, .. } => self.require_table_permission(
                session,
                PermissionType::Alter,
                name.effective_database(&session.database),
                &name.name,
            ),
            Statement::Truncate { table } => self.require_table_permission(
                session,
                PermissionType::Truncate,
                table.effective_database(&session.database),
                &table.name,
            ),
            Statement::CreateDatabase { name, .. } => {
                self.require_database_permission(session, PermissionType::Create, name)
            }
            Statement::DropDatabase { name, .. } => {
                self.require_database_permission(session, PermissionType::Drop, name)
            }
            Statement::Insert { table, .. } => self.require_table_permission(
                session,
                PermissionType::Insert,
                table.effective_database(&session.database),
                &table.name,
            ),
            Statement::Update { table, .. } => self.require_table_permission(
                session,
                PermissionType::Update,
                table.effective_database(&session.database),
                &table.name,
            ),
            Statement::Delete { table, .. } => self.require_table_permission(
                session,
                PermissionType::Delete,
                table.effective_database(&session.database),
                &table.name,
            ),
            Statement::Select { from, .. } => {
                if let Some(from_clause) = from {
                    for table_ref in
                        super::query::extract_table_refs(from_clause, &session.database)
                    {
                        self.require_table_permission(
                            session,
                            PermissionType::Select,
                            &table_ref.database,
                            &table_ref.table,
                        )?;
                    }
                }
                Ok(())
            }
            Statement::Union { left, right, .. } => {
                self.ensure_permissions(session, left)?;
                self.ensure_permissions(session, right)
            }
            Statement::ShowTables { database } => self.require_database_permission(
                session,
                PermissionType::Select,
                database.as_deref().unwrap_or(&session.database),
            ),
            Statement::ShowColumns { table }
            | Statement::ShowIndexes { table }
            | Statement::ShowCreateTable { table } => self.require_table_permission(
                session,
                PermissionType::Select,
                table.effective_database(&session.database),
                &table.name,
            ),
            Statement::ShowPartitions { table } => {
                if let Some(table) = table {
                    self.require_table_permission(
                        session,
                        PermissionType::Select,
                        table.effective_database(&session.database),
                        &table.name,
                    )
                } else {
                    self.require_database_permission(
                        session,
                        PermissionType::Select,
                        &session.database,
                    )
                }
            }
            Statement::ShowStats => {
                self.require_database_permission(session, PermissionType::Select, &session.database)
            }
            Statement::FlushTable { table } => {
                if let Some(table) = table {
                    self.require_table_permission(
                        session,
                        PermissionType::Alter,
                        table.effective_database(&session.database),
                        &table.name,
                    )
                } else {
                    self.require_database_permission(
                        session,
                        PermissionType::Alter,
                        &session.database,
                    )
                }
            }
            Statement::OptimizeTable { table, .. } => self.require_table_permission(
                session,
                PermissionType::Alter,
                table.effective_database(&session.database),
                &table.name,
            ),
            Statement::Explain { stmt: inner, .. } => self.ensure_permissions(session, inner),
            Statement::UseDatabase(_)
            | Statement::ShowDatabases
            | Statement::ShowEngines
            | Statement::Begin
            | Statement::Commit
            | Statement::Rollback => Ok(()),
            Statement::CreateUser { .. }
            | Statement::AlterUser { .. }
            | Statement::CreateRole { .. }
            | Statement::DropUser { .. }
            | Statement::DropRole { .. }
            | Statement::Grant { .. }
            | Statement::Revoke { .. }
            | Statement::ShowUsers
            | Statement::ShowRoles
            | Statement::ShowGrants { .. }
            | Statement::ShowProcesslist { .. }
            | Statement::StartAutoCompact
            | Statement::StopAutoCompact
            | Statement::PauseFlush
            | Statement::ResumeFlush => self.require_global_admin(session),
        }
    }

    fn require_table_permission(
        &self,
        session: &Session,
        permission: PermissionType,
        db: &str,
        table: &str,
    ) -> Result<()> {
        self.require_permission(
            session,
            permission,
            PermissionObjectType::Table,
            &format!("{}.{}", db, table),
        )
    }

    fn require_database_permission(
        &self,
        session: &Session,
        permission: PermissionType,
        db: &str,
    ) -> Result<()> {
        self.require_permission(session, permission, PermissionObjectType::Database, db)
    }

    fn require_global_admin(&self, session: &Session) -> Result<()> {
        self.require_permission(
            session,
            PermissionType::All,
            PermissionObjectType::Global,
            "*",
        )
    }

    fn require_permission(
        &self,
        session: &Session,
        permission: PermissionType,
        object_type: PermissionObjectType,
        object_name: &str,
    ) -> Result<()> {
        if self.auth_manager.check_permission(
            &session.username,
            permission.clone(),
            object_type.clone(),
            object_name,
        ) {
            return Ok(());
        }
        Err(TaibaiError::Sql(format!(
            "access denied for user '{}' to {:?} on {:?} '{}'",
            session.username, permission, object_type, object_name
        )))
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

    pub fn commit_session_txn(&self, session: &mut Session) -> Result<()> {
        if !session.in_txn() {
            return Err(TaibaiError::Sql("no active transaction".into()));
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
        Ok(())
    }

    pub fn get_table_meta(
        &self,
        db: &str,
        table: &str,
    ) -> Option<crate::storage::catalog::schema::TableMeta> {
        let catalogs = self.catalogs.read().unwrap();
        catalogs.get(db).and_then(|c| c.get_table(table).cloned())
    }

    pub fn database_exists(&self, db: &str) -> bool {
        self.catalogs.read().unwrap().contains_key(db)
    }

    pub fn begin_bulk_import(&self) -> usize {
        self.bulk_mode_sessions.fetch_add(1, Ordering::AcqRel) + 1
    }

    pub fn end_bulk_import(&self) -> usize {
        self.bulk_mode_sessions
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                Some(count.saturating_sub(1))
            })
            .unwrap_or(0)
            .saturating_sub(1)
    }

    pub fn is_bulk_mode(&self) -> bool {
        self.bulk_mode_sessions.load(Ordering::Acquire) > 0
    }

    pub fn bulk_mode_session_count(&self) -> usize {
        self.bulk_mode_sessions.load(Ordering::Acquire)
    }

    pub fn set_auto_compact_enabled(&self, enabled: bool) {
        self.auto_compact_enabled.store(enabled, Ordering::Release);
        tracing::info!(enabled, "auto compact runtime flag updated");
    }

    pub fn auto_compact_enabled(&self) -> bool {
        self.auto_compact_enabled.load(Ordering::Acquire)
    }

    fn begin_operation(&self) -> EngineOperationGuard<'_> {
        self.mark_activity();
        self.in_flight_operations.fetch_add(1, Ordering::AcqRel);
        EngineOperationGuard { engine: self }
    }

    fn mark_activity(&self) {
        self.last_activity_epoch_ms
            .store(current_epoch_millis(), Ordering::Release);
    }

    pub fn is_idle_for(&self, duration: Duration) -> bool {
        if self.in_flight_operations.load(Ordering::Acquire) > 0 {
            return false;
        }
        let last = self.last_activity_epoch_ms.load(Ordering::Acquire);
        current_epoch_millis().saturating_sub(last) >= duration.as_millis() as u64
    }

    pub fn has_active_segments(&self) -> bool {
        self.segment_mgr.has_active_segments()
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
        {
            let mut state = self.catalog_save_state.lock().unwrap();
            state.pending_insert_rows = 0;
            state.last_save_at = Instant::now();
        }

        Ok(0)
    }

    pub(crate) fn catalogs(&self) -> &RwLock<HashMap<String, DatabaseCatalog>> {
        &self.catalogs
    }

    pub(crate) fn record_inserted_rows(
        &self,
        db: &str,
        table: &str,
        rows: i64,
        save_immediately: bool,
        save_deferred: bool,
    ) -> Result<()> {
        let mut should_save = save_immediately;
        if save_deferred && !save_immediately {
            let mut state = self.catalog_save_state.lock().unwrap();
            state.pending_insert_rows =
                state.pending_insert_rows.saturating_add(rows.max(0) as u64);
            if state.pending_insert_rows >= CATALOG_SAVE_INSERT_ROWS
                || state.last_save_at.elapsed() >= CATALOG_SAVE_INTERVAL
            {
                state.pending_insert_rows = 0;
                state.last_save_at = Instant::now();
                should_save = true;
            }
        }

        let mut catalogs = self.catalogs.write().unwrap();
        if let Some(catalog) = catalogs.get_mut(db) {
            if let Some(meta) = catalog.get_table_mut(table) {
                meta.row_count += rows;
            }
            if should_save {
                let meta_dir = self.data_dir.join("meta");
                catalog.save(&meta_dir)?;
            }
        }
        Ok(())
    }

    fn validate_batch_unique_indexes(
        &self,
        db: &str,
        table: &str,
        batch: &RecordBatch,
    ) -> Result<()> {
        let catalogs = self.catalogs.read().unwrap();
        let Some(catalog) = catalogs.get(db) else {
            return Ok(());
        };
        let Some(table_meta) = catalog.get_table(table) else {
            return Ok(());
        };
        let schema = Schema::new(table_meta.columns.clone());
        for index in &table_meta.unique_indexes {
            let mut indices = Vec::with_capacity(index.columns.len());
            for column in &index.columns {
                indices.push(schema.index_of(column).ok_or_else(|| {
                    TaibaiError::Sql(format!("unique index column '{}' not found", column))
                })?);
            }
            super::dml_insert::validate_unique_key_uniqueness(
                self,
                db,
                table,
                &schema,
                &indices,
                &index.name,
                Some(batch),
            )?;
        }
        Ok(())
    }

    pub fn exec_bulk_insert_with_wal_control(
        &self,
        db: &str,
        table: &str,
        batch: RecordBatch,
        disable_wal: bool,
    ) -> Result<u64> {
        self.validate_batch_unique_indexes(db, table, &batch)?;
        if self.is_bulk_mode() {
            self.segment_mgr
                .bulk_insert_batch_deferred(db, table, batch, disable_wal)
        } else {
            self.segment_mgr
                .bulk_insert_batch(db, table, batch, disable_wal)
        }
    }

    pub fn exec_bulk_insert_partitioned_with_wal_control(
        &self,
        db: &str,
        table: &str,
        table_meta: &crate::storage::catalog::schema::TableMeta,
        batch: RecordBatch,
        disable_wal: bool,
    ) -> Result<u64> {
        self.validate_batch_unique_indexes(db, table, &batch)?;
        if self.is_bulk_mode() {
            self.segment_mgr
                .bulk_insert_batch_partitioned_deferred(db, table, table_meta, batch)
        } else if table_meta.is_partitioned() {
            self.segment_mgr
                .bulk_insert_batch_partitioned(db, table, table_meta, batch)
        } else {
            self.segment_mgr
                .bulk_insert_batch(db, table, batch, disable_wal)
        }
    }

    pub fn exec_bulk_insert_payload_with_wal_control(
        &self,
        db: &str,
        payload: &[u8],
        disable_wal: bool,
    ) -> Result<u64> {
        self.exec_bulk_insert_payload_with_options(db, payload, disable_wal, false)
    }

    pub fn exec_bulk_insert_payload_with_options(
        &self,
        db: &str,
        payload: &[u8],
        disable_wal: bool,
        bulk_import_mode: bool,
    ) -> Result<u64> {
        let _op_guard = self.begin_operation();
        let table_name = bulk_payload_table_name(payload)?.to_string();
        let catalogs = self.catalogs.read().unwrap();
        let catalog = catalogs
            .get(db)
            .ok_or_else(|| TaibaiError::Sql(format!("database '{}' does not exist", db)))?;

        if let Some(table_meta) = catalog.get_table(&table_name) {
            if !table_meta.unique_indexes.is_empty() {
                let req = crate::storage::bulk_payload::parse_direct_bulk_payload(payload)?;
                let schema = Schema::new(table_meta.columns.clone());
                let batch =
                    crate::storage::segment_manager::direct_bulk_request_to_batch(&req, &schema)?;
                self.validate_batch_unique_indexes(db, &table_name, &batch)?;
            }
        }

        let result = self.segment_mgr.bulk_insert_payload_with_catalog(
            db,
            payload,
            disable_wal,
            catalog,
            !bulk_import_mode,
        )?;
        drop(catalogs);

        if !disable_wal && !bulk_import_mode {
            let req = {
                let catalogs = self.catalogs.read().unwrap();
                let catalog = catalogs
                    .get(db)
                    .ok_or_else(|| TaibaiError::Sql(format!("database '{}' does not exist", db)))?;
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
                if result > 0 {
                }
                result
            };
            return Ok(req);
        }

        if result > 0 {
        }
        Ok(result)
    }
}

struct EngineOperationGuard<'a> {
    engine: &'a Engine,
}

impl Drop for EngineOperationGuard<'_> {
    fn drop(&mut self) {
        self.engine
            .in_flight_operations
            .fetch_sub(1, Ordering::AcqRel);
        self.engine.mark_activity();
    }
}

fn current_epoch_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn bulk_payload_table_name(payload: &[u8]) -> Result<&str> {
    if payload.len() < 2 {
        return Err(TaibaiError::Protocol("bulk payload too short".into()));
    }
    let table_len = u16::from_le_bytes(payload[0..2].try_into().unwrap()) as usize;
    let start = 2;
    let end = start + table_len;
    if end > payload.len() {
        return Err(TaibaiError::Protocol("bulk payload truncated".into()));
    }
    std::str::from_utf8(&payload[start..end])
        .map_err(|e| TaibaiError::Protocol(format!("invalid utf8: {}", e)))
}

fn restored_auto_increment_start(
    segment_mgr: &Arc<SegmentManager>,
    data_dir: &Path,
    db: &str,
    table: &str,
    col_idx: usize,
    catalog_start: u64,
) -> Result<u64> {
    let table_dir = data_dir.join(db).join(table);
    if !table_dir.exists() {
        return Ok(catalog_start);
    }

    let mut max_value = catalog_start.saturating_sub(1);
    for seg_path in SegmentManager::discover_segments(&table_dir)? {
        let reader = segment_mgr.open_reader_cached(&seg_path)?;
        for rg_idx in 0..reader.num_row_groups() {
            let batch = reader.read_row_group_with_projection(rg_idx, &[col_idx])?;
            for row_idx in 0..batch.num_rows() {
                let Some(value) = batch.scalar(row_idx, 0) else {
                    continue;
                };
                let Some(int_value) = value.integer_i128() else {
                    continue;
                };
                let Ok(unsigned) = u64::try_from(int_value) else {
                    continue;
                };
                max_value = max_value.max(unsigned);
            }
        }
    }

    Ok(max_value.saturating_add(1).max(catalog_start))
}
