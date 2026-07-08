#![allow(dead_code)]
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use crate::common::error::{Result, TaibaiError};
use crate::net::session::Session;
use crate::storage::catalog::auth::{AuthManager, PermissionObjectType, PermissionType};
use crate::storage::catalog::auto_increment::AutoIncrementManager;
use crate::storage::catalog::schema::DatabaseCatalog;
use crate::storage::core::batch_writer::{BatchWriter, BatchWriterConfig};
use crate::storage::core::compactor::Compactor;
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
    catalogs: Arc<RwLock<HashMap<String, DatabaseCatalog>>>,
    catalog_save_state: Mutex<CatalogSaveState>,
    segment_mgr: Arc<SegmentManager>,
    batch_writer: BatchWriter,
    bulk_mode_sessions: AtomicUsize,
    in_flight_operations: AtomicUsize,
    auth_manager: Arc<AuthManager>,
    auto_increment_mgr: Arc<AutoIncrementManager>,
    index_manager: Arc<IndexManager>,
    hot_store_mgr: Arc<crate::storage::core::hot_store::HotStoreManager>,
}

struct CatalogSaveState {
    pending_insert_rows: u64,
    last_save_at: Instant,
}

const CATALOG_SAVE_INSERT_ROWS: u64 = 500_000;
const CATALOG_SAVE_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug, Clone)]
pub struct AutoCompactConfig {
    pub enabled: bool,
    pub min_deleted_ratio: f64,
    pub min_deleted_rows: u64,
    pub interval: Duration,
    pub max_tables_per_run: usize,
}

impl Default for AutoCompactConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            min_deleted_ratio: 0.30,
            min_deleted_rows: 8192,
            interval: Duration::from_secs(60),
            max_tables_per_run: 8,
        }
    }
}

fn start_delete_bitmap_cleanup_thread(
    segment_mgr: Arc<SegmentManager>,
    data_dir: PathBuf,
    catalogs: Arc<RwLock<HashMap<String, DatabaseCatalog>>>,
    config: AutoCompactConfig,
) {
    if !config.enabled {
        return;
    }
    std::thread::spawn(move || loop {
        std::thread::sleep(config.interval);
        let compactor = Compactor::new(Arc::clone(&segment_mgr), data_dir.clone());
        let table_metas = {
            let catalogs = catalogs.read().unwrap();
            catalogs
                .iter()
                .flat_map(|(db, catalog)| {
                    catalog
                        .tables
                        .iter()
                        .map(|(table, meta)| (db.clone(), table.clone(), meta.clone()))
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>()
        };
        let mut compacted = 0usize;
        for (db, table, table_meta) in table_metas {
            if compacted >= config.max_tables_per_run {
                break;
            }
            let table_dir = data_dir.join(&db).join(&table);
            if !table_dir.exists() {
                continue;
            }
            match table_needs_delete_cleanup(
                &segment_mgr,
                &db,
                &table,
                &table_dir,
                config.min_deleted_ratio,
                config.min_deleted_rows,
            ) {
                Ok(true) => match compactor.optimize_table(&db, &table, &table_meta) {
                    Ok(stats) => {
                        if stats.rows_dropped > 0 || stats.segments_written > 0 {
                            compacted += 1;
                            tracing::info!(
                                db,
                                table,
                                rows_dropped = stats.rows_dropped,
                                rows_written = stats.rows_written,
                                segments_read = stats.segments_read,
                                segments_written = stats.segments_written,
                                "auto compact cleaned delete bitmap rows"
                            );
                        }
                    }
                    Err(err) => {
                        tracing::warn!(db, table, error = %err, "auto compact failed");
                    }
                },
                Ok(false) => {
                    match segment_mgr.cleanup_fully_deleted_segments(&db, &table, &table_dir) {
                        Ok(removed) if removed > 0 => {
                            compacted += 1;
                            tracing::info!(
                                db,
                                table,
                                segments_removed = removed,
                                "delete bitmap cleanup removed fully deleted segments"
                            );
                        }
                        Ok(_) => {}
                        Err(err) => {
                            tracing::warn!(db, table, error = %err, "delete bitmap cleanup failed");
                        }
                    }
                }
                Err(err) => {
                    tracing::warn!(db, table, error = %err, "auto compact scan failed");
                }
            }
        }
    });
}

fn table_needs_delete_cleanup(
    segment_mgr: &SegmentManager,
    db: &str,
    table: &str,
    table_dir: &Path,
    min_deleted_ratio: f64,
    min_deleted_rows: u64,
) -> Result<bool> {
    let mut rows = 0u64;
    let mut deleted = 0u64;
    for path in segment_mgr.discover_inactive_segments(db, table, table_dir)? {
        let reader = segment_mgr.open_reader_cached(&path)?;
        rows = rows.saturating_add(reader.row_count());
        deleted = deleted.saturating_add(reader.deleted_row_count());
    }
    if deleted == 0 || deleted < min_deleted_rows {
        return Ok(false);
    }
    if rows == 0 {
        return Ok(false);
    }
    Ok((deleted as f64 / rows as f64) >= min_deleted_ratio)
}

impl Engine {
    pub fn new(data_dir: &Path, default_database: &str) -> Result<Self> {
        Self::new_with_storage_config_and_auto_compact(
            data_dir,
            default_database,
            SegmentConfig::default(),
            AutoCompactConfig::default(),
        )
    }
    pub fn new_with_storage_config(
        data_dir: &Path,
        default_database: &str,
        segment_config: SegmentConfig,
    ) -> Result<Self> {
        Self::new_with_storage_config_and_auto_compact(
            data_dir,
            default_database,
            segment_config,
            AutoCompactConfig::default(),
        )
    }

    pub fn new_with_storage_config_and_auto_compact(
        data_dir: &Path,
        default_database: &str,
        segment_config: SegmentConfig,
        auto_compact_config: AutoCompactConfig,
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
        for (db_name, catalog) in &mut catalogs {
            for table_meta in catalog.tables.values_mut() {
                table_meta.ensure_auto_increment_next_value();
                if let Some(col) = table_meta.auto_increment_column() {
                    auto_increment_mgr.register(
                        db_name,
                        &table_meta.name,
                        &col.name,
                        table_meta.auto_increment_next_value,
                    )?;
                }
            }
        }
        let index_manager = Arc::new(IndexManager::new_with_data_dir(
            Arc::clone(&segment_mgr),
            data_dir.to_path_buf(),
        ));
        let hot_store_mgr = Arc::new(crate::storage::core::hot_store::HotStoreManager::new());
        let catalogs = Arc::new(RwLock::new(catalogs));
        start_delete_bitmap_cleanup_thread(
            Arc::clone(&segment_mgr),
            data_dir.to_path_buf(),
            Arc::clone(&catalogs),
            auto_compact_config,
        );

        let engine = Self {
            data_dir: data_dir.to_path_buf(),
            catalogs,
            catalog_save_state: Mutex::new(CatalogSaveState {
                pending_insert_rows: 0,
                last_save_at: Instant::now(),
            }),
            segment_mgr,
            batch_writer,
            bulk_mode_sessions: AtomicUsize::new(0),
            in_flight_operations: AtomicUsize::new(0),
            auth_manager,
            auto_increment_mgr,
            index_manager,
            hot_store_mgr,
        };
        engine.warmup_hot_stores();
        Ok(engine)
    }

    pub fn execute(&self, session: &mut Session, sql: &str) -> Result<ExecResult> {
        let _op_guard = self.begin_operation();
        if sql.as_bytes().first().map(|b| b.to_ascii_uppercase()) == Some(b'I') {
            if let Some(result) = super::dml_insert::try_exec_fast_insert_sql(
                self,
                &session.database,
                sql,
                session.timezone_offset_hours,
                session.active_txn_id(),
                session.bulk_import_mode,
                &session.username,
            ) {
                let result = result;
                if result.is_ok() && session.active_txn_id() > 0 {
                    session.mark_txn_write();
                }
                return result;
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
                    super::query::exec_select_with_plan_and_txn(
                        self,
                        &session.database,
                        &stmt,
                        plan.clone(),
                        session.active_txn_id(),
                    )
                } else {
                    let result = super::query::exec_select_with_txn(
                        self,
                        &session.database,
                        &stmt,
                        session.active_txn_id(),
                    );
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
                hot,
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
                    hot,
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
            Statement::Replace {
                table,
                columns,
                values,
            } => {
                let txn_id = session.active_txn_id();
                let result = super::dml_insert::exec_replace(
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
                let result = super::dml_delete::exec_delete(self, &db, &table.name, where_clause);
                if result.is_ok() && session.in_txn() {
                    session.mark_txn_write();
                }
                result
            }
            Statement::Select { .. } => super::query::exec_select_with_txn(
                self,
                &session.database,
                stmt,
                session.active_txn_id(),
            ),
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
            Statement::ShowUniqueIndexStatus { table } => {
                super::ddl::exec_show_unique_index_status(
                    self,
                    table.effective_database(&session.database),
                    &table.name,
                )
            }
            Statement::RebuildUniqueIndex { table, index_name } => {
                super::ddl::exec_rebuild_unique_index(
                    self,
                    table.effective_database(&session.database),
                    &table.name,
                    index_name.as_deref(),
                )
            }
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
            Statement::AlterUser { name, operations } => {
                super::ddl::exec_alter_user(self, name, operations)
            }
            Statement::ShowUsers => super::ddl::exec_show_users(self),
            Statement::ShowRoles => super::ddl::exec_show_roles(self),
            Statement::ShowGrants { user } => super::ddl::exec_show_grants(self, user),
            Statement::ShowPartitions { table } => {
                let db = table
                    .as_ref()
                    .map(|t| t.effective_database(&session.database))
                    .unwrap_or(&session.database);
                let table_name = table.as_ref().map(|t| t.name.clone());
                super::ddl::exec_show_partitions(self, db, &table_name)
            }
            Statement::FlushTable { table } => {
                let db = table
                    .as_ref()
                    .map(|t| t.effective_database(&session.database))
                    .unwrap_or(&session.database);
                let table_name = table.as_ref().map(|t| t.name.clone());
                super::ddl::exec_flush_table(self, db, &table_name)
            }
            Statement::Union { .. } => super::query::exec_select_with_txn(
                self,
                &session.database,
                stmt,
                session.active_txn_id(),
            ),
            Statement::OptimizeTable { table, partition } => super::ddl::exec_optimize_table(
                self,
                table.effective_database(&session.database),
                &table.name,
                partition.as_deref(),
            ),
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
            Statement::Insert { table, .. } | Statement::Replace { table, .. } => self
                .require_table_permission(
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
            | Statement::ShowUniqueIndexStatus { table }
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
            Statement::OptimizeTable { table, .. }
            | Statement::RebuildUniqueIndex { table, .. } => self.require_table_permission(
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

    pub fn hot_store_manager(
        &self,
    ) -> &Arc<crate::storage::core::hot_store::HotStoreManager> {
        &self.hot_store_mgr
    }

    pub fn feed_hot_store(
        &self,
        db: &str,
        table: &str,
        table_meta: &crate::storage::catalog::schema::TableMeta,
        batch: &crate::types::batch::RecordBatch,
    ) -> crate::common::error::Result<()> {
        self.hot_store_mgr.try_append(db, table, table_meta, batch)
    }

    /// Reclaim resident memory under hot-store backpressure (design §3.1 hard
    /// valve): drop the already-persisted append-prefix from the store, then
    /// checkpoint-flush (seal active segments + clear WAL). Persisted rows stay
    /// queryable via the segment scan; only the in-memory mirror is released.
    pub fn reclaim_hot_store_pressure(&self, db: &str, table: &str) -> bool {
        let store_rows = self
            .hot_store_mgr
            .get(db, table)
            .map(|s| s.row_count())
            .unwrap_or(0);
        let live = self
            .batch_writer
            .memtable_manager()
            .get_by_table(db, table)
            .iter()
            .map(|mt| mt.row_count())
            .sum::<u64>();
        let prefix = store_rows.saturating_sub(live);
        if prefix > 0 {
            let dropped = self.hot_store_mgr.compact_prefix(db, table, prefix);
            tracing::warn!(
                db,
                table,
                dropped,
                "hot store backpressure: reclaimed persisted prefix"
            );
        }
        if let Err(e) = self.flush_table_buffers(db, table) {
            tracing::warn!(db, table, error = %e, "hot store backpressure: checkpoint flush failed");
        }
        prefix > 0
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

    fn begin_operation(&self) -> EngineOperationGuard<'_> {
        self.in_flight_operations.fetch_add(1, Ordering::AcqRel);
        EngineOperationGuard { engine: self }
    }

    pub fn alloc_txn_id(&self) -> u64 {
        self.segment_mgr.alloc_txn_id()
    }

    pub fn flush_table_buffers(&self, db: &str, table: &str) -> Result<u64> {
        if let Err(e) = self.batch_writer.flush_table(db, table) {
            tracing::warn!(db, table, error = %e, "batch writer table flush failed");
        }

        let rows = self.segment_mgr.flush_table_active(db, table)?;

        {
            let mut catalogs = self.catalogs.write().unwrap();
            if let Some(catalog) = catalogs.get_mut(db) {
                let meta_dir = self.data_dir.join("meta");
                let _ = catalog.save(&meta_dir);
            }
        }
        {
            let mut state = self.catalog_save_state.lock().unwrap();
            state.pending_insert_rows = 0;
            state.last_save_at = Instant::now();
        }

        Ok(rows)
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

    /// Close-of-day archival (design §4.2): flush every live memtable, seal all
    /// active segments (and clear WALs), then evict every resident hot store and
    /// drop Archived memtables. After this the day's data is fully handed off to
    /// the history layer; the next trading day starts with empty resident state.
    /// Intended to be triggered by an external scheduler at market close.
    pub fn close_of_day_flush(&self) -> Result<()> {
        tracing::info!("close-of-day: flushing all resident data to history");
        self.flush_all_buffers()?;
        self.hot_store_mgr.evict_all();
        self.batch_writer.memtable_manager().remove_all_archived();
        tracing::info!("close-of-day: complete, resident state evicted");
        Ok(())
    }

    pub fn close_of_day_flush_table(&self, db: &str, table: &str) -> Result<()> {
        tracing::info!(db, table, "close-of-day: flushing table resident data to history");
        let table_meta = {
            let catalogs = self.catalogs.read().unwrap();
            let catalog = catalogs
                .get(db)
                .ok_or_else(|| TaibaiError::Sql(format!("database '{}' does not exist", db)))?;
            catalog
                .get_table(table)
                .ok_or_else(|| TaibaiError::Sql(format!("table '{}.{}' does not exist", db, table)))?
                .clone()
        };
        self.flush_table_buffers(db, table)?;
        let compactor = Compactor::new(Arc::clone(&self.segment_mgr), self.data_dir.clone());
        let stats = compactor.optimize_table(db, table, &table_meta)?;
        self.index_manager.mark_index_stale(db, table);
        self.hot_store_mgr.remove(db, table);
        self.batch_writer.memtable_manager().remove_archived(db, table);
        let today = chrono::Local::now().date_naive().to_string();
        {
            let mut catalogs = self.catalogs.write().unwrap();
            if let Some(catalog) = catalogs.get_mut(db) {
                if let Some(meta) = catalog.get_table_mut(table) {
                    meta.hot_last_close_date = Some(today);
                }
                let meta_dir = self.data_dir.join("meta");
                catalog.save(&meta_dir)?;
            }
        }
        tracing::info!(
            db,
            table,
            rows_written = stats.rows_written,
            rows_dropped = stats.rows_dropped,
            "close-of-day: table resident state evicted and optimized"
        );
        Ok(())
    }

    /// Rebuild resident hot stores from on-disk segments after a restart (design
    /// gap fix). Without this, already-flushed data would not be resident and
    /// point lookups would fall back to the segment scan until new writes refill
    /// the store. For each hot-eligible table we load the most recently written
    /// segments (up to a row budget) so the inverted index is warm again.
    /// Best-effort: unreadable segments are skipped.
    pub fn warmup_hot_stores(&self) {
        use crate::storage::core::hot_store::resolve_key_column;
        use crate::storage::segment::reader::SegmentReader;
        use crate::storage::segment::delete_bitmap;

        const WARMUP_ROW_BUDGET: u64 = 20_000_000;

        let catalogs = self.catalogs.read().unwrap();
        for (db, catalog) in catalogs.iter() {
            for (table, table_meta) in catalog.tables.iter() {
                if resolve_key_column(table_meta).is_none() {
                    continue;
                }
                let table_dir = self.segment_mgr.table_dir(db, table);
                if !table_dir.exists() {
                    continue;
                }
                let segments = match SegmentManager::discover_segments(&table_dir) {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::warn!(db, table, error = %e, "warmup: discover segments failed");
                        continue;
                    }
                };
                if segments.is_empty() {
                    continue;
                }

                let mut mtimed: Vec<(std::time::SystemTime, std::path::PathBuf)> = Vec::new();
                for seg in &segments {
                    if let Ok(meta) = std::fs::metadata(seg) {
                        if let Ok(mtime) = meta.modified() {
                            mtimed.push((mtime, seg.clone()));
                        }
                    }
                }
                mtimed.sort_by(|a, b| b.cmp(a));
                if mtimed.is_empty() {
                    continue;
                }

                let mut loaded_rows: u64 = 0;
                for (_, seg_path) in &mtimed {
                    if loaded_rows >= WARMUP_ROW_BUDGET {
                        break;
                    }
                    let reader = match SegmentReader::open(seg_path) {
                        Ok(r) => r,
                        Err(e) => {
                            tracing::warn!(path = %seg_path.display(), error = %e, "warmup: open segment failed");
                            continue;
                        }
                    };
                    let num_cols = reader.schema().num_columns();
                    let projection: Vec<usize> = (0..num_cols).collect();
                    let del_bitmap = reader.delete_bitmap();
                    let has_deletes = !del_bitmap.is_empty();
                    for rg in 0..reader.num_row_groups() {
                        let batch = match reader.read_row_group_with_projection(rg, &projection) {
                            Ok(b) => b,
                            Err(e) => {
                                tracing::warn!(error = %e, "warmup: read row group failed");
                                continue;
                            }
                        };
                        let first_row_id = reader.row_group_start_row(rg);
                        let batch = if has_deletes {
                            match delete_bitmap::filter_batch(&batch, first_row_id, del_bitmap) {
                                Ok(b) => b,
                                Err(_) => batch,
                            }
                        } else {
                            batch
                        };
                        if batch.num_rows() == 0 {
                            continue;
                        }
                        loaded_rows = loaded_rows.saturating_add(batch.num_rows() as u64);
                        if let Err(e) =
                            self.hot_store_mgr.try_append(db, table, table_meta, &batch)
                        {
                            tracing::warn!(db, table, error = %e, "warmup: feed hot store failed");
                        }
                    }
                }
                if loaded_rows > 0 {
                    tracing::info!(
                        db,
                        table,
                        loaded_rows,
                        "warmup: rebuilt resident hot store from segments"
                    );
                }
            }
        }
    }

    /// Graceful shutdown (design §4.3): flush every live memtable, seal active
    /// segments and clear WALs so a restart loses nothing. Safe to call once.
    pub fn shutdown(&mut self) -> Result<()> {
        tracing::info!("engine shutdown: flushing resident data");
        if let Err(e) = self.flush_all_buffers() {
            tracing::warn!(error = %e, "engine shutdown: flush_all_buffers failed");
        }
        self.batch_writer.stop();
        Ok(())
    }

    pub(crate) fn catalogs(&self) -> &Arc<RwLock<HashMap<String, DatabaseCatalog>>> {
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
                if let Some(counter) = self.auto_increment_mgr.get(db, table) {
                    meta.auto_increment_next_value = counter.current_value();
                }
            }
            if should_save {
                let meta_dir = self.data_dir.join("meta");
                catalog.save(&meta_dir)?;
            }
        }
        Ok(())
    }

    pub(crate) fn persist_auto_increment_next_value(
        &self,
        db: &str,
        table: &str,
        save_immediately: bool,
    ) -> Result<()> {
        let Some(counter) = self.auto_increment_mgr.get(db, table) else {
            return Ok(());
        };
        let mut catalogs = self.catalogs.write().unwrap();
        let Some(catalog) = catalogs.get_mut(db) else {
            return Ok(());
        };
        let Some(meta) = catalog.get_table_mut(table) else {
            return Ok(());
        };
        meta.auto_increment_next_value = counter.current_value();
        if save_immediately {
            let meta_dir = self.data_dir.join("meta");
            catalog.save(&meta_dir)?;
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

        let mut explicit_auto_increment_max = None;
        let mut unique_index_names = Vec::new();
        if let Some(table_meta) = catalog.get_table(&table_name) {
            unique_index_names = table_meta
                .unique_indexes
                .iter()
                .map(|index| index.name.clone())
                .collect();
            if !table_meta.unique_indexes.is_empty() || table_meta.has_auto_increment() {
                let req = crate::storage::bulk_payload::parse_direct_bulk_payload(payload)?;
                let schema = Schema::new(table_meta.columns.clone());
                let batch =
                    crate::storage::segment_manager::direct_bulk_request_to_batch(&req, &schema)?;
                if !table_meta.unique_indexes.is_empty() {
                    self.validate_batch_unique_indexes(db, &table_name, &batch)?;
                }
                if let Some((col_idx, _)) = table_meta
                    .columns
                    .iter()
                    .enumerate()
                    .find(|(_, col)| col.auto_increment)
                {
                    let mut max_value = None;
                    for row_idx in 0..batch.num_rows() {
                        let Some(value) = batch.scalar(row_idx, col_idx) else {
                            continue;
                        };
                        let Some(int_value) = value.integer_i128() else {
                            continue;
                        };
                        let Ok(unsigned) = u64::try_from(int_value) else {
                            continue;
                        };
                        max_value =
                            Some(max_value.map_or(unsigned, |current: u64| current.max(unsigned)));
                    }
                    explicit_auto_increment_max = max_value;
                }
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

        if result > 0 && !unique_index_names.is_empty() {
            self.index_manager.remove_table_unique_index_snapshots(
                db,
                &table_name,
                &unique_index_names,
            );
        }

        if let Some(max_value) = explicit_auto_increment_max {
            self.auto_increment_mgr
                .advance_past(db, &table_name, max_value)?;
        }

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
                            if let Some(counter) = self.auto_increment_mgr.get(db, &table_name) {
                                meta.auto_increment_next_value = counter.current_value();
                            }
                        }
                        let meta_dir = self.data_dir.join("meta");
                        catalog.save(&meta_dir)?;
                    }
                }
                if result > 0 {}
                result
            };
            return Ok(req);
        }

        if explicit_auto_increment_max.is_some() {
            self.persist_auto_increment_next_value(db, &table_name, false)?;
        }
        if result > 0 {}
        Ok(result)
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        // Best-effort graceful shutdown so a process exit does not lose the
        // resident write buffer (design §4.3). Idempotent with explicit shutdown().
        let _ = self.shutdown();
    }
}

fn parse_hh_mm(s: &str) -> Option<(u32, u32)> {
    use chrono::{NaiveTime, Timelike};
    let t = NaiveTime::parse_from_str(s.trim(), "%H:%M").ok()?;
    Some((t.hour(), t.minute()))
}

/// Start the background scheduler that flushes HOT tables once per day when
/// each table's `HOT ... CLOSE AT` time is reached. Tables without an explicit
/// close time default to 15:30 local time.
pub fn start_close_of_day_scheduler(engine: Arc<Engine>) -> std::thread::JoinHandle<()> {
    use chrono::{Local, Timelike};
    use std::collections::HashSet;
    std::thread::Builder::new()
        .name("taibai-close-of-day".into())
        .spawn(move || {
            let mut last_fired: HashSet<(String, String, chrono::NaiveDate)> = HashSet::new();
            loop {
                std::thread::sleep(Duration::from_secs(30));
                let now = Local::now();
                let today = now.date_naive();
                let today_str = today.to_string();
                last_fired.retain(|(_, _, d)| *d == today);
                let hot_tables = {
                    let catalogs = engine.catalogs.read().unwrap();
                    let mut tables = Vec::new();
                    for (db, catalog) in catalogs.iter() {
                        for (table, meta) in catalog.tables.iter() {
                            if meta.hot_time_column.is_some() {
                                tables.push((
                                    db.clone(),
                                    table.clone(),
                                    meta.hot_close_time.as_deref().unwrap_or("15:30").to_string(),
                                    meta.hot_last_close_date.clone(),
                                ));
                            }
                        }
                    }
                    tables
                };
                for (db, table, close_time, last_close_date) in hot_tables {
                    let Some((target_h, target_m)) = parse_hh_mm(&close_time).or(Some((15, 30))) else {
                        continue;
                    };
                    let key = (db.clone(), table.clone(), today);
                    if last_fired.contains(&key) || last_close_date.as_deref() == Some(today_str.as_str()) {
                        continue;
                    }
                    let reached = now.hour() > target_h
                        || (now.hour() == target_h && now.minute() >= target_m);
                    if reached {
                        tracing::info!(db, table, close_time, "close-of-day scheduler: firing table archival flush");
                        if let Err(e) = engine.close_of_day_flush_table(&db, &table) {
                            tracing::warn!(db, table, error = %e, "close-of-day scheduler: table flush failed");
                        }
                        last_fired.insert(key);
                    }
                }
            }
        })
        .expect("failed to spawn close-of-day scheduler")
}

struct EngineOperationGuard<'a> {
    engine: &'a Engine,
}

impl Drop for EngineOperationGuard<'_> {
    fn drop(&mut self) {
        self.engine
            .in_flight_operations
            .fetch_sub(1, Ordering::AcqRel);
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::net::session::Session;
    use crate::protocol::types::DataType;
    use crate::storage::segment_manager::SegmentManager;
    use crate::types::scalar::ScalarValue;

    fn session() -> Session {
        Session::new("root".to_string(), "main".to_string())
    }

    fn bulk_payload(
        table: &str,
        row_count: u32,
        columns: Vec<(&str, DataType, Vec<u8>)>,
    ) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.extend_from_slice(&(table.len() as u16).to_le_bytes());
        payload.extend_from_slice(table.as_bytes());
        payload.extend_from_slice(&row_count.to_le_bytes());
        payload.extend_from_slice(&(columns.len() as u32).to_le_bytes());
        for (name, dtype, data) in columns {
            payload.extend_from_slice(&(name.len() as u16).to_le_bytes());
            payload.extend_from_slice(name.as_bytes());
            payload.push(dtype as u8);
            payload.push(0);
            payload.push(0);
            payload.extend_from_slice(&row_count.to_le_bytes());
            payload.extend_from_slice(&0u32.to_le_bytes());
            payload.extend_from_slice(&data);
        }
        payload
    }

    fn int64_col(values: &[i64]) -> Vec<u8> {
        let mut data = Vec::new();
        for value in values {
            data.extend_from_slice(&value.to_le_bytes());
        }
        data
    }

    fn first_text(result: ExecResult, row: usize, col: usize) -> String {
        let ExecResult::Query { batches, .. } = result else {
            panic!("expected query result");
        };
        batches[0]
            .scalar(row, col)
            .and_then(|v| v.to_text())
            .unwrap()
    }

    fn first_i64(result: ExecResult, row: usize, col: usize) -> i64 {
        let ExecResult::Query { batches, .. } = result else {
            panic!("expected query result");
        };
        match batches[0].scalar(row, col).unwrap() {
            ScalarValue::Int64(Some(v)) => v,
            ScalarValue::Int32(Some(v)) => v as i64,
            value => panic!("unexpected value {:?}", value),
        }
    }

    #[test]
    fn fast_insert_sql_materializes_generic_multi_values() -> Result<()> {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::new(dir.path(), "main")?;
        let mut session = session();

        engine.execute(
            &mut session,
            "CREATE TABLE t (id INT64, name VARCHAR, amount DECIMAL(9,2), active BOOL)",
        )?;
        engine.execute(
            &mut session,
            "INSERT INTO t (id, name, amount, active) VALUES (1, 'alpha', 12.34, true), (2, 'it''s', -5.60, 0)",
        )?;

        assert_eq!(
            first_i64(
                engine.execute(&mut session, "SELECT id FROM t ORDER BY id")?,
                0,
                0
            ),
            1
        );
        assert_eq!(
            first_text(
                engine.execute(&mut session, "SELECT name FROM t WHERE id = 2")?,
                0,
                0,
            ),
            "it's"
        );
        assert_eq!(
            first_i64(
                engine.execute(&mut session, "SELECT COUNT(*) FROM t")?,
                0,
                0
            ),
            2
        );
        Ok(())
    }

    #[test]
    fn fast_insert_sql_materializes_missing_nullable_and_literal_default_columns() -> Result<()> {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::new(dir.path(), "main")?;
        let mut session = session();

        engine.execute(
            &mut session,
            "CREATE TABLE t (id INT64, name VARCHAR, tag VARCHAR DEFAULT 'new', note VARCHAR)",
        )?;
        engine.execute(
            &mut session,
            "INSERT INTO t (id, name) VALUES (1, 'alpha'), (2, 'beta')",
        )?;

        assert_eq!(
            first_text(
                engine.execute(&mut session, "SELECT tag FROM t WHERE id = 1")?,
                0,
                0,
            ),
            "new"
        );
        assert_eq!(
            first_i64(
                engine.execute(&mut session, "SELECT COUNT(*) FROM t WHERE note IS NULL")?,
                0,
                0
            ),
            2
        );
        Ok(())
    }

    #[test]
    fn fast_insert_sql_materializes_missing_auto_increment_and_current_timestamp() -> Result<()> {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::new(dir.path(), "main")?;
        let mut session = session();

        engine.execute(
            &mut session,
            "CREATE TABLE t (id INT64 AUTO_INCREMENT PRIMARY KEY, name VARCHAR, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
        )?;
        engine.execute(
            &mut session,
            "INSERT INTO t (name) VALUES ('alpha'), ('beta')",
        )?;

        assert_eq!(
            first_i64(
                engine.execute(&mut session, "SELECT id FROM t ORDER BY id")?,
                0,
                0
            ),
            1
        );
        assert_eq!(
            first_i64(
                engine.execute(&mut session, "SELECT id FROM t ORDER BY id")?,
                1,
                0
            ),
            2
        );
        assert_eq!(
            first_i64(
                engine.execute(
                    &mut session,
                    "SELECT COUNT(*) FROM t WHERE created_at IS NOT NULL"
                )?,
                0,
                0
            ),
            2
        );
        Ok(())
    }

    #[test]
    fn rebuild_unique_index_persists_snapshot_and_show_reports_it() -> Result<()> {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::new(dir.path(), "main")?;
        let mut session = session();

        engine.execute(
            &mut session,
            "CREATE TABLE t (id INT64, name VARCHAR, UNIQUE KEY uk_id (id))",
        )?;
        engine.execute(
            &mut session,
            "INSERT INTO t (id, name) VALUES (1, 'a'), (2, 'b')",
        )?;
        engine.execute(&mut session, "REBUILD UNIQUE INDEX uk_id ON t")?;

        let index_file = dir
            .path()
            .join("main")
            .join("t")
            .join("indexes")
            .join("unique")
            .join("uk_id.keys.tbi");
        assert!(index_file.exists());

        let message = first_text(
            engine.execute(&mut session, "SHOW UNIQUE INDEX STATUS FROM t")?,
            0,
            4,
        );
        assert_eq!(message, "persisted keys=2");
        Ok(())
    }

    #[test]
    fn drop_unique_index_removes_persisted_snapshot() -> Result<()> {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::new(dir.path(), "main")?;
        let mut session = session();

        engine.execute(
            &mut session,
            "CREATE TABLE t (id INT64, name VARCHAR, UNIQUE KEY uk_id (id))",
        )?;
        engine.execute(&mut session, "INSERT INTO t (id, name) VALUES (1, 'a')")?;
        engine.execute(&mut session, "REBUILD UNIQUE INDEX uk_id ON t")?;
        engine.execute(&mut session, "INSERT INTO t (id, name) VALUES (2, 'b')")?;
        let index_dir = dir
            .path()
            .join("main")
            .join("t")
            .join("indexes")
            .join("unique");
        let index_file = index_dir.join("uk_id.keys.tbi");
        let delta_file = index_dir.join("uk_id.delta.tbi");
        assert!(index_file.exists());
        assert!(delta_file.exists());

        engine.execute(&mut session, "DROP INDEX uk_id ON t")?;
        assert!(!index_file.exists());
        assert!(!delta_file.exists());
        Ok(())
    }

    #[test]
    fn rebuild_unique_index_supports_multi_column_point_checks() -> Result<()> {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::new(dir.path(), "main")?;
        let mut session = session();

        engine.execute(
            &mut session,
            "CREATE TABLE t (a INT64, b INT64, name VARCHAR, UNIQUE KEY uk_ab (a, b))",
        )?;
        engine.execute(
            &mut session,
            "INSERT INTO t (a, b, name) VALUES (1, 2, 'x'), (1, 3, 'y')",
        )?;
        engine.execute(&mut session, "REBUILD UNIQUE INDEX uk_ab ON t")?;

        let index_file = dir
            .path()
            .join("main")
            .join("t")
            .join("indexes")
            .join("unique")
            .join("uk_ab.keys.tbi");
        assert!(index_file.exists());

        let duplicate = engine.execute(
            &mut session,
            "INSERT INTO t (a, b, name) VALUES (1, 2, 'dup')",
        );
        assert!(duplicate.is_err());

        engine.execute(
            &mut session,
            "INSERT INTO t (a, b, name) VALUES (2, 2, 'ok')",
        )?;
        Ok(())
    }

    #[test]
    fn rebuild_unique_index_writes_v2_blocks_for_large_keyset() -> Result<()> {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::new(dir.path(), "main")?;
        let mut session = session();

        engine.execute(
            &mut session,
            "CREATE TABLE t (id INT64, name VARCHAR, UNIQUE KEY uk_id (id))",
        )?;
        let values = (0..300)
            .map(|id| format!("({}, 'n{}')", id, id))
            .collect::<Vec<_>>()
            .join(", ");
        engine.execute(
            &mut session,
            &format!("INSERT INTO t (id, name) VALUES {}", values),
        )?;
        engine.execute(&mut session, "REBUILD UNIQUE INDEX uk_id ON t")?;

        let index_file = dir
            .path()
            .join("main")
            .join("t")
            .join("indexes")
            .join("unique")
            .join("uk_id.keys.tbi");
        let bytes = std::fs::read(index_file)?;
        assert_eq!(&bytes[..8], b"TBIUKV2\0");

        let duplicate =
            engine.execute(&mut session, "INSERT INTO t (id, name) VALUES (299, 'dup')");
        assert!(duplicate.is_err());
        engine.execute(&mut session, "INSERT INTO t (id, name) VALUES (300, 'ok')")?;
        Ok(())
    }

    #[test]
    fn unique_index_incremental_insert_uses_delta_and_survives_reload() -> Result<()> {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::new(dir.path(), "main")?;
        let mut initial_session = session();

        engine.execute(
            &mut initial_session,
            "CREATE TABLE t (id INT64, name VARCHAR, UNIQUE KEY uk_id (id))",
        )?;
        engine.execute(
            &mut initial_session,
            "INSERT INTO t (id, name) VALUES (1, 'a')",
        )?;
        engine.execute(&mut initial_session, "REBUILD UNIQUE INDEX uk_id ON t")?;

        let index_dir = dir
            .path()
            .join("main")
            .join("t")
            .join("indexes")
            .join("unique");
        let base_file = index_dir.join("uk_id.keys.tbi");
        let delta_file = index_dir.join("uk_id.delta.tbi");
        let base_before = std::fs::metadata(&base_file)?.len();
        assert!(!delta_file.exists());

        engine.execute(
            &mut initial_session,
            "INSERT INTO t (id, name) VALUES (2, 'b')",
        )?;
        let base_after = std::fs::metadata(&base_file)?.len();
        assert_eq!(base_after, base_before);
        assert!(delta_file.exists());
        assert!(std::fs::metadata(&delta_file)?.len() > 8);

        let duplicate_from_cache = engine.execute(
            &mut initial_session,
            "INSERT INTO t (id, name) VALUES (2, 'dup')",
        );
        assert!(duplicate_from_cache.is_err());

        drop(engine);
        let engine = Engine::new(dir.path(), "main")?;
        let mut reloaded_session = session();
        let duplicate_from_delta = engine.execute(
            &mut reloaded_session,
            "INSERT INTO t (id, name) VALUES (2, 'dup')",
        );
        assert!(duplicate_from_delta.is_err());
        engine.execute(
            &mut reloaded_session,
            "INSERT INTO t (id, name) VALUES (3, 'c')",
        )?;
        Ok(())
    }

    #[test]
    fn unique_index_delta_compacts_when_threshold_is_reached() -> Result<()> {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::new(dir.path(), "main")?;
        let mut initial_session = session();

        engine.execute(
            &mut initial_session,
            "CREATE TABLE t (id INT64, name VARCHAR, UNIQUE KEY uk_id (id))",
        )?;
        engine.execute(
            &mut initial_session,
            "INSERT INTO t (id, name) VALUES (1, 'a')",
        )?;
        engine.execute(&mut initial_session, "REBUILD UNIQUE INDEX uk_id ON t")?;

        let index_dir = dir
            .path()
            .join("main")
            .join("t")
            .join("indexes")
            .join("unique");
        let base_file = index_dir.join("uk_id.keys.tbi");
        let delta_file = index_dir.join("uk_id.delta.tbi");
        let base_before = std::fs::metadata(&base_file)?.len();
        let values = (2..180)
            .map(|id| format!("({}, 'n{}')", id, id))
            .collect::<Vec<_>>()
            .join(", ");

        engine.execute(
            &mut initial_session,
            &format!("INSERT INTO t (id, name) VALUES {}", values),
        )?;

        let base_after = std::fs::metadata(&base_file)?.len();
        assert!(base_after > base_before);
        assert!(!delta_file.exists());
        let duplicate_from_cache = engine.execute(
            &mut initial_session,
            "INSERT INTO t (id, name) VALUES (179, 'dup')",
        );
        assert!(duplicate_from_cache.is_err());

        drop(engine);
        let engine = Engine::new(dir.path(), "main")?;
        let mut reloaded_session = session();
        let duplicate_after_reload = engine.execute(
            &mut reloaded_session,
            "INSERT INTO t (id, name) VALUES (179, 'dup')",
        );
        assert!(duplicate_after_reload.is_err());
        Ok(())
    }

    #[test]
    fn delete_then_insert_same_unique_key_after_rebuild() -> Result<()> {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::new(dir.path(), "main")?;
        let mut session = session();

        engine.execute(
            &mut session,
            "CREATE TABLE t (id INT64, name VARCHAR, UNIQUE KEY uk_id (id))",
        )?;
        engine.execute(&mut session, "INSERT INTO t (id, name) VALUES (1, 'a')")?;
        engine.execute(&mut session, "REBUILD UNIQUE INDEX uk_id ON t")?;
        let index_dir = dir
            .path()
            .join("main")
            .join("t")
            .join("indexes")
            .join("unique");
        let base_file = index_dir.join("uk_id.keys.tbi");
        let delta_file = index_dir.join("uk_id.delta.tbi");
        engine.execute(&mut session, "DELETE FROM t WHERE id = 1")?;
        assert!(base_file.exists());
        assert!(delta_file.exists());
        assert_eq!(
            engine.index_manager().unique_key_exists(
                "main",
                "t",
                "uk_id",
                &[ScalarValue::Int64(Some(1))]
            )?,
            Some(false)
        );
        engine.execute(&mut session, "INSERT INTO t (id, name) VALUES (1, 'b')")?;

        let name = first_text(
            engine.execute(&mut session, "SELECT name FROM t WHERE id = 1")?,
            0,
            0,
        );
        assert_eq!(name, "b");
        Ok(())
    }

    #[test]
    fn replace_into_works_after_rebuild_unique_index() -> Result<()> {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::new(dir.path(), "main")?;
        let mut session = session();

        engine.execute(
            &mut session,
            "CREATE TABLE t (id INT64, name VARCHAR, UNIQUE KEY uk_id (id))",
        )?;
        engine.execute(&mut session, "INSERT INTO t (id, name) VALUES (1, 'old')")?;
        engine.execute(&mut session, "REBUILD UNIQUE INDEX uk_id ON t")?;
        let index_dir = dir
            .path()
            .join("main")
            .join("t")
            .join("indexes")
            .join("unique");
        let base_file = index_dir.join("uk_id.keys.tbi");
        let delta_file = index_dir.join("uk_id.delta.tbi");
        engine.execute(&mut session, "REPLACE INTO t (id, name) VALUES (1, 'new')")?;
        assert!(base_file.exists());
        assert!(delta_file.exists());

        let name = first_text(
            engine.execute(&mut session, "SELECT name FROM t WHERE id = 1")?,
            0,
            0,
        );
        let count = first_i64(
            engine.execute(&mut session, "SELECT COUNT(*) FROM t")?,
            0,
            0,
        );
        assert_eq!(name, "new");
        assert_eq!(count, 1);
        Ok(())
    }

    #[test]
    fn update_unique_key_after_rebuild_allows_old_key_reuse() -> Result<()> {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::new(dir.path(), "main")?;
        let mut session = session();

        engine.execute(
            &mut session,
            "CREATE TABLE t (id INT64, code INT64, UNIQUE KEY uk_code (code))",
        )?;
        engine.execute(&mut session, "INSERT INTO t (id, code) VALUES (1, 10)")?;
        engine.execute(&mut session, "REBUILD UNIQUE INDEX uk_code ON t")?;
        engine.execute(&mut session, "UPDATE t SET code = 20 WHERE id = 1")?;
        engine.execute(&mut session, "INSERT INTO t (id, code) VALUES (2, 10)")?;

        let count = first_i64(
            engine.execute(&mut session, "SELECT COUNT(*) FROM t")?,
            0,
            0,
        );
        assert_eq!(count, 2);
        Ok(())
    }

    #[test]
    fn bulk_payload_invalidates_rebuilt_unique_index() -> Result<()> {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::new(dir.path(), "main")?;
        let mut session = session();

        engine.execute(
            &mut session,
            "CREATE TABLE t (id INT64, name INT64, UNIQUE KEY uk_id (id))",
        )?;
        engine.execute(&mut session, "INSERT INTO t (id, name) VALUES (1, 1)")?;
        engine.execute(&mut session, "REBUILD UNIQUE INDEX uk_id ON t")?;
        let payload = bulk_payload(
            "t",
            1,
            vec![
                ("id", DataType::Int64, int64_col(&[2])),
                ("name", DataType::Int64, int64_col(&[2])),
            ],
        );
        engine.exec_bulk_insert_payload_with_wal_control("main", &payload, false)?;

        let duplicate = engine.execute(&mut session, "INSERT INTO t (id, name) VALUES (2, 3)");
        assert!(duplicate.is_err());
        Ok(())
    }

    #[test]
    fn flush_table_only_flushes_named_table() -> Result<()> {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::new(dir.path(), "main")?;
        let mut session = session();

        engine.execute(&mut session, "CREATE TABLE t1 (id INT64)")?;
        engine.execute(&mut session, "CREATE TABLE t2 (id INT64)")?;
        engine.begin_bulk_import();
        session.bulk_import_mode = true;
        engine.execute(&mut session, "INSERT INTO t1 (id) VALUES (1)")?;
        engine.execute(&mut session, "INSERT INTO t2 (id) VALUES (2)")?;

        let t2_dir = dir.path().join("main").join("t2");
        let t2_before = SegmentManager::discover_segments(&t2_dir)?
            .into_iter()
            .map(|path| std::fs::metadata(path).map(|meta| meta.len()))
            .collect::<std::io::Result<Vec<_>>>()?;

        engine.execute(&mut session, "FLUSH TABLE t1")?;

        let t1_dir = dir.path().join("main").join("t1");
        let t1_segments = SegmentManager::discover_segments(&t1_dir)?;
        let t2_after = SegmentManager::discover_segments(&t2_dir)?
            .into_iter()
            .map(|path| std::fs::metadata(path).map(|meta| meta.len()))
            .collect::<std::io::Result<Vec<_>>>()?;
        assert!(!t1_segments.is_empty());
        assert_eq!(t2_after, t2_before);

        session.bulk_import_mode = false;
        engine.end_bulk_import();
        Ok(())
    }

    #[test]
    fn replace_into_deletes_conflicting_unique_row_before_insert() -> Result<()> {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::new(dir.path(), "main")?;
        let mut session = session();

        engine.execute(
            &mut session,
            "CREATE TABLE t (id INT64, name VARCHAR, UNIQUE KEY uk_id (id))",
        )?;
        engine.execute(&mut session, "INSERT INTO t (id, name) VALUES (1, 'old')")?;
        engine.execute(&mut session, "REPLACE INTO t (id, name) VALUES (1, 'new')")?;

        let name = first_text(
            engine.execute(&mut session, "SELECT name FROM t WHERE id = 1")?,
            0,
            0,
        );
        let count = first_i64(
            engine.execute(&mut session, "SELECT COUNT(*) FROM t")?,
            0,
            0,
        );
        assert_eq!(name, "new");
        assert_eq!(count, 1);
        Ok(())
    }
    #[test]
    fn committed_transaction_remains_visible_without_immediate_segment_flush() -> Result<()> {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::new(dir.path(), "main")?;
        let mut writer = session();
        let mut reader = session();

        engine.execute(&mut writer, "CREATE TABLE t (id INT64, name VARCHAR)")?;
        engine.execute(&mut writer, "BEGIN")?;
        engine.execute(&mut writer, "INSERT INTO t (id, name) VALUES (1, 'a')")?;

        let reader_count_before =
            first_i64(engine.execute(&mut reader, "SELECT COUNT(*) FROM t")?, 0, 0);
        let writer_count_before =
            first_i64(engine.execute(&mut writer, "SELECT COUNT(*) FROM t")?, 0, 0);
        assert_eq!(reader_count_before, 0);
        assert_eq!(writer_count_before, 1);

        engine.execute(&mut writer, "COMMIT")?;

        let reader_count_after =
            first_i64(engine.execute(&mut reader, "SELECT COUNT(*) FROM t")?, 0, 0);
        assert_eq!(reader_count_after, 1);

        let table_dir = dir.path().join("main").join("t");
        let segments = SegmentManager::discover_segments(&table_dir)?;
        assert!(segments.is_empty());
        Ok(())
    }
}
