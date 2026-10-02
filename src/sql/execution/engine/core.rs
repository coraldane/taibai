impl Engine {
    pub fn new(data_dir: &Path, default_database: &str) -> Result<Self> {
        Self::new_with_storage_config_and_auto_compact(
            data_dir,
            default_database,
            SegmentConfig::default(),
            AutoCompactConfig::default(),
            8 * 1024 * 1024 * 1024,
            false,
            crate::common::config::PubSubSettings::default(),
            true,
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
            8 * 1024 * 1024 * 1024,
            false,
            crate::common::config::PubSubSettings::default(),
            true,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_with_storage_config_and_auto_compact(
        data_dir: &Path,
        default_database: &str,
        segment_config: SegmentConfig,
        auto_compact_config: AutoCompactConfig,
        hot_store_memory_limit_bytes: u64,
        tail_async_fsync: bool,
        pubsub_settings: crate::common::config::PubSubSettings,
        hot_warmup_enabled: bool,
    ) -> Result<Self> {
        super::ddl::recover_database_drop_tombstones(data_dir)?;
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
                            if db_name.starts_with('.') {
                                continue;
                            }
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
                        if db_name.starts_with('.')
                            || db_name == "meta"
                            || db_name == "wal"
                            || db_name == ".taibai-drop-databases"
                        {
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

        super::ddl::recover_drop_tombstones(data_dir, &catalogs)?;
        super::ddl::recover_table_data_tombstones(data_dir, &catalogs)?;
        super::ddl::recover_rename_tombstones(data_dir, &catalogs)?;

        let row_group_target_rows = segment_config.row_group_target_rows.max(1) as u64;
        let segment_mgr = Arc::new(SegmentManager::with_config(data_dir, segment_config)?);
        SegmentManager::start_flush_thread(&segment_mgr);

        // Seed per-table zstd level overrides before any recovery pass can
        // create writers, so flushed tails and recovered active segments use
        // the table's COMPRESSION_LEVEL instead of the global default.
        for (db_name, catalog) in &catalogs {
            for table_meta in catalog.tables.values() {
                if table_meta.compression_level.is_some() {
                    segment_mgr.set_table_compression_level(
                        db_name,
                        &table_meta.name,
                        table_meta.compression_level,
                    );
                }
            }
        }

        let pubsub = crate::pubsub::PubSubHub::new(pubsub_settings.clone());
        segment_mgr.set_pubsub(Arc::clone(&pubsub));

        // Reliable-subscription layer. Disabled engines get an inert manager
        // so callers never need to branch; the hub mirrors committed inserts
        // into its per-table retention logs when subscriptions exist.
        let pubsub_manager =
            if pubsub_settings.enabled {
                crate::storage::core::pubsub::PubSubManager::start(
                    pubsub_settings,
                    data_dir.join("pubsub"),
                )
            } else {
                crate::storage::core::pubsub::PubSubManager::disabled()
            };
        pubsub.set_mirror(Arc::clone(&pubsub_manager));

        let batch_writer = BatchWriter::new(
            BatchWriterConfig {
                // A partial row group remains a durable memtable tail. Once a
                // full row group has accumulated it can be flushed without
                // manufacturing a tiny row group from scattered INSERTs.
                max_rows_per_memtable: row_group_target_rows,
                low_rate_max_rows: auto_compact_config
                    .low_rate_max_rows
                    .max(row_group_target_rows.saturating_sub(1)),
                low_rate_max_age_secs: auto_compact_config.low_rate_max_age.as_secs(),
                // Acknowledged INSERTs are already recoverable from durable
                // tails. Use the configured memory budget for asynchronous
                // checkpoint pressure instead of a fixed 512 MiB estimate.
                max_total_memory: hot_store_memory_limit_bytes,
                tail_async_fsync,
                ..BatchWriterConfig::default()
            },
            Arc::clone(&segment_mgr),
            Some(Arc::clone(&pubsub)),
        );
        let recovered_tail_tables =
            recover_durable_memtable_tails(data_dir, &catalogs, &batch_writer)?;

        let txn_mutations =
            Arc::new(crate::storage::core::txn_mutations::TxnMutationRegistry::new());
        segment_mgr.set_txn_mutations(Arc::clone(&txn_mutations));
        batch_writer.memtable_manager().set_txn_mutations(Arc::clone(&txn_mutations));

        // Replication primary: create the event log and wire it into the
        // commit thread (tail appends) and the segment manager (structural
        // events). Standby/none engines run without one.
        let replication_settings = crate::common::config::replication_settings();
        let rep_log = if replication_settings.role_is_primary() {
            let log = Arc::new(crate::storage::replication::RepLog::new(
                replication_settings.ring_capacity,
                data_dir,
            ));
            batch_writer.set_rep_log(Arc::clone(&log));
            segment_mgr.set_rep_log(Arc::clone(&log));
            // Memtable-level sidecars (`.del` / `.upd`) ship verbatim from
            // the durable-tail module; the standby's identical frame
            // history makes the same bytes apply to the same rows.
            crate::storage::core::durable_tail::set_rep_log(Arc::clone(&log));
            Some(log)
        } else {
            None
        };
        // Footer rewrites rolled back by startup recovery above may already
        // have been shipped to standbys; re-emit the recovered footer states
        // as compensation events (no-op without the rollback log).
        crate::storage::segment::reader::drain_footer_rollback_compensations(
            data_dir,
            |path, bitmap, deleted_count| {
                segment_mgr.emit_delete_footer_rewrite(path, bitmap, deleted_count)
            },
        );

        let auth_manager = Arc::new(AuthManager::new(data_dir)?);
        let auto_increment_mgr = Arc::new(AutoIncrementManager::new());
        for (db_name, catalog) in &mut catalogs {
            for table_meta in catalog.tables.values_mut() {
                let table_meta = std::sync::Arc::make_mut(table_meta);
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
        let hot_store_mgr = Arc::new(
            crate::storage::core::hot_store::HotStoreManager::with_memory_limit_bytes(
                hot_store_memory_limit_bytes,
            ),
        );
        let performance_baselines =
            Arc::new(crate::common::performance_baseline::PerformanceBaselineStore::load(data_dir));
        let catalogs = Arc::new(RwLock::new(catalogs));
        let auto_compact_supervisor = start_auto_compaction_thread(
            Arc::clone(&segment_mgr),
            data_dir.to_path_buf(),
            Arc::clone(&catalogs),
            Arc::clone(&index_manager),
            Arc::clone(batch_writer.memtable_manager()),
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
            pubsub,
            pubsub_manager,
            bulk_mode_sessions: AtomicUsize::new(0),
            bulk_dirty_tables: Mutex::new(HashSet::new()),
            in_flight_operations: AtomicUsize::new(0),
            auth_manager,
            auto_increment_mgr,
            index_manager,
            hot_store_mgr,
            performance_baselines,
            hot_partition_segments: Mutex::new(HashMap::new()),
            query_registry: Arc::new(crate::sql::execution::query_registry::QueryRegistry::new()),
            txn_mutations,
            rep_log,
            read_only: AtomicBool::new(false),
            auto_compact_supervisor: Mutex::new(auto_compact_supervisor),
            // Registered post-construction by `start_close_of_day_scheduler`
            // (the engine is Arc-wrapped by then).
            close_of_day_supervisor: Mutex::new(None),
        };
        if hot_warmup_enabled {
            engine.warmup_hot_stores();
        }
        for (db, table) in recovered_tail_tables {
            engine.invalidate_hot_store(&db, &table);
        }
        Ok(engine)
    }

    pub fn execute(&self, session: &mut Session, sql: &str) -> Result<ExecResult> {
        let _op_guard = self.begin_operation();
        if self.is_read_only() && !super::read_only_gate::starts_read_keyword(sql) {
            return Err(TaibaiError::Sql(
                "server is running as a replication standby and is read-only".into(),
            ));
        }
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
        // `execute` is the shared entry for the network and embedded APIs,
        // which carry one statement per call; silently dropping the trailing
        // statements of a multi-statement string would lose writes.
        if stmts.len() > 1 {
            return Err(TaibaiError::Sql(format!(
                "multi-statement strings are not supported here ({} statements); \
                 send them one at a time",
                stmts.len()
            )));
        }
        let result = self.execute_stmt(session, &stmts[0]);
        // DDL/auth statements mutate the catalog or auth store in place; a
        // replication primary ships a full snapshot so standbys reload.
        if result.is_ok() {
            if let Some((kind, db)) = Self::replication_snapshot_target(&stmts[0], &session.database) {
                let (path, event) = match kind {
                    "catalog" => {
                        let path = self
                            .data_dir()
                            .join("meta")
                            .join("catalogs")
                            .join(format!("{db}.json"));
                        // A missing file means the database was dropped; the
                        // standby apply treats an empty payload as removal.
                        let json = std::fs::read(&path).unwrap_or_default();
                        let event = crate::storage::replication::ReplicationEvent::CatalogSnapshot {
                            db,
                            json,
                        };
                        (path, event)
                    }
                    _ => {
                        let path = self.data_dir().join("meta").join("auth.json");
                        let json = std::fs::read(&path).unwrap_or_default();
                        let event =
                            crate::storage::replication::ReplicationEvent::AuthSnapshot { json };
                        (path, event)
                    }
                };
                let _ = &path;
                if let Some(log) = self.rep_log() {
                    log.enqueue(event);
                }
            }
        }
        result
    }

    pub fn execute_streaming_query<F>(
        &self,
        session: &Session,
        sql: &str,
        emit: F,
    ) -> Result<Option<Schema>>
    where
        F: FnMut(RecordBatch) -> Result<()>,
    {
        let _op_guard = self.begin_operation();
        if session.active_txn_id() != 0 {
            return Ok(None);
        }
        let stmts = Parser::parse(sql)?;
        let Some(stmt @ Statement::Select { .. }) = stmts.first() else {
            return Ok(None);
        };
        self.ensure_permissions(session, stmt)?;
        super::query::exec_select_streaming(self, &session.database, stmt, emit)
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
                let plan_matches_database = prepared
                    .planned_database
                    .as_deref()
                    .is_some_and(|db| db == session.database);
                // Schema fingerprint guard: a rebuilt table with the same
                // name invalidates cached column indices.
                let current_fingerprint = self
                    .gather_table_schemas(&session.database, &stmt)
                    .ok()
                    .map(|schemas| schema_fingerprint(&schemas));
                let schema_unchanged = match (
                    prepared.planned_schema_fingerprint,
                    current_fingerprint,
                ) {
                    (Some(planned), Some(current)) => planned == current,
                    // No fingerprint recorded (or none computable): only
                    // trust the plan when no fingerprint was needed before.
                    (None, _) => prepared.planned_schema_fingerprint.is_none(),
                    (Some(_), None) => false,
                };
                if let (Some(plan), true, true) = (
                    prepared.cached_plan.as_ref(),
                    plan_matches_database,
                    schema_unchanged,
                )
                {
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
                        // Cache the plan only for placeholder-free,
                        // subquery-free SELECTs. A cached plan cannot carry
                        // subqueries: resolved subquery literals would freeze
                        // one execution's result into every later one, and
                        // correlated plans embed snapshot ids from a
                        // query-private registry that no later execution can
                        // satisfy (second EXECUTE used to fail permanently).
                        // Skipping the resolve pass here also removes the
                        // cache-fill double execution of non-correlated
                        // subqueries.
                        if !has_placeholders
                            && !super::query::statement_contains_subquery(&stmt)
                        {
                            let table_schemas =
                                self.gather_table_schemas(&session.database, &stmt)?;
                            if let Ok(plan) = crate::sql::planner::plan_select(&stmt, &table_schemas) {
                                prepared.cached_plan =
                                    Some(crate::sql::optimizer::optimize(plan));
                                prepared.planned_database =
                                    Some(session.database.clone());
                                prepared.planned_schema_fingerprint = self
                                    .gather_table_schemas(&session.database, &stmt)
                                    .ok()
                                    .map(|schemas| schema_fingerprint(&schemas));
                            }
                        }
                    }
                    result
                }
            }
            _ => self.execute_stmt(session, &stmt),
        }
    }

    /// Execute a batch of parameter sets for a prepared INSERT as one logical
    /// multi-row INSERT.  The wire protocol sends all parameter sets together;
    /// keeping them together here is important because constraint validation,
    /// partition routing, durable-tail commits, and catalog accounting are all
    /// batch-oriented below the SQL layer.
    pub fn execute_prepared_write_batch(
        &self,
        session: &mut Session,
        prepared: &crate::sql::execution::prepared::PreparedStatement,
        batch: &[Vec<crate::types::scalar::ScalarValue>],
    ) -> Result<ExecResult> {
        let _op_guard = self.begin_operation();
        if batch.is_empty() {
            return Ok(ExecResult::Ok { rows_affected: 0 });
        }

        let (table, columns, values_template, is_replace) = match &prepared.stmt {
            Statement::Insert {
                table,
                columns,
                values,
            } => (table.clone(), columns.clone(), values.clone(), false),
            Statement::Replace {
                table,
                columns,
                values,
            } => (table.clone(), columns.clone(), values.clone(), true),
            _ => {
                return Err(TaibaiError::Sql(
                    "prepared batch fast path only supports INSERT and REPLACE".into(),
                ))
            }
        };

        let mut values = Vec::with_capacity(batch.len().saturating_mul(values_template.len()));
        for (row_idx, params) in batch.iter().enumerate() {
            if params.len() != prepared.param_count {
                return Err(TaibaiError::Sql(format!(
                    "row {} parameter count mismatch: expected {}, got {}",
                    row_idx,
                    prepared.param_count,
                    params.len()
                )));
            }
            // Only `values` need rebinding per parameter row, so clone the
            // pre-extracted values template (C14) instead of the whole
            // Statement (table + columns + values) on every row.
            // `bind_params`' Insert/Replace arm rewrites only `values`, so
            // the binding shell below carries empty table/columns; the
            // bound rows are spliced into the real statement built after
            // the loop, yielding exactly the values the previous
            // clone-whole-tree-per-row loop produced (same placeholder
            // substitution, same literals, coercion happens downstream).
            let mut bound = if is_replace {
                Statement::Replace {
                    table: crate::sql::parser::ast::ObjectName::bare(String::new()),
                    columns: Vec::new(),
                    values: values_template.clone(),
                }
            } else {
                Statement::Insert {
                    table: crate::sql::parser::ast::ObjectName::bare(String::new()),
                    columns: Vec::new(),
                    values: values_template.clone(),
                }
            };
            crate::sql::execution::prepared::bind_params(&mut bound, params).map_err(|error| {
                TaibaiError::Sql(format!("row {} parameter bind failed: {}", row_idx, error))
            })?;
            match bound {
                Statement::Insert {
                    values: bound_values,
                    ..
                }
                | Statement::Replace {
                    values: bound_values,
                    ..
                } => values.extend(bound_values),
                _ => {
                    return Err(TaibaiError::Sql(
                        "prepared statement changed kind during batch binding".into(),
                    ));
                }
            }
        }

        let stmt = if is_replace {
            Statement::Replace {
                table,
                columns,
                values,
            }
        } else {
            Statement::Insert {
                table,
                columns,
                values,
            }
        };
        self.ensure_permissions(session, &stmt)?;
        self.execute_stmt(session, &stmt)
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


    /// DDL and segment-rewriting maintenance statements have no transactional
/// undo path: the catalog is rewritten in place and segment rewrites are
/// immediately durable. They must not run inside an explicit transaction,
/// otherwise ROLLBACK would silently keep their effects. Returns the SQL
/// verb to name in the error message.
fn transaction_forbidden_verb(stmt: &Statement) -> Option<&'static str> {
    match stmt {
        Statement::CreateTable { .. }
        | Statement::CreateIndex { .. }
        | Statement::DropIndex { .. }
        | Statement::DropTable { .. }
        | Statement::AlterTable { .. }
        | Statement::Truncate { .. }
        | Statement::CreateDatabase { .. }
        | Statement::DropDatabase { .. }
        | Statement::CreateUser { .. }
        | Statement::AlterUser { .. }
        | Statement::DropUser { .. }
        | Statement::CreateRole { .. }
        | Statement::DropRole { .. }
        | Statement::Grant { .. }
        | Statement::Revoke { .. }
        | Statement::RebuildUniqueIndex { .. }
        | Statement::OptimizeTable { .. }
        | Statement::FlushTable { .. }
        | Statement::AnalyzeTable { .. } => Some(Self::stmt_name(stmt)),
        _ => None,
    }
}

    /// Which replication snapshot a successful DDL/auth statement implies:
    /// `("catalog", db)` ships the database's catalog JSON, `("auth", _)`
    /// ships the auth store JSON.
    fn replication_snapshot_target(
        stmt: &Statement,
        default_db: &str,
    ) -> Option<(&'static str, String)> {
        let table_db = |name: &crate::sql::parser::ast::ObjectName| {
            Some(("catalog", name.effective_database(default_db).to_string()))
        };
        match stmt {
            Statement::CreateTable { name, .. }
            | Statement::DropTable { name, .. }
            | Statement::AlterTable { name, .. }
            | Statement::Truncate { table: name }
            | Statement::CreateIndex { table: name, .. }
            | Statement::DropIndex { table: name, .. }
            | Statement::RebuildUniqueIndex { table: name, .. }
            | Statement::AnalyzeTable { table: name } => table_db(name),
            Statement::CreateDatabase { name, .. }
            | Statement::DropDatabase { name, .. } => {
                Some(("catalog", name.clone()))
            }
            Statement::CreateUser { .. }
            | Statement::AlterUser { .. }
            | Statement::DropUser { .. }
            | Statement::CreateRole { .. }
            | Statement::DropRole { .. }
            | Statement::Grant { .. }
            | Statement::Revoke { .. } => Some(("auth", String::new())),
            _ => None,
        }
    }

fn stmt_name(stmt: &Statement) -> &'static str {
    match stmt {
        Statement::CreateTable { .. } => "CREATE TABLE",
        Statement::CreateIndex { .. } => "CREATE INDEX",
        Statement::DropIndex { .. } => "DROP INDEX",
        Statement::DropTable { .. } => "DROP TABLE",
        Statement::AlterTable { .. } => "ALTER TABLE",
        Statement::Truncate { .. } => "TRUNCATE",
        Statement::CreateDatabase { .. } => "CREATE DATABASE",
        Statement::DropDatabase { .. } => "DROP DATABASE",
        Statement::CreateUser { .. } => "CREATE USER",
        Statement::AlterUser { .. } => "ALTER USER",
        Statement::DropUser { .. } => "DROP USER",
        Statement::CreateRole { .. } => "CREATE ROLE",
        Statement::DropRole { .. } => "DROP ROLE",
        Statement::Grant { .. } => "GRANT",
        Statement::Revoke { .. } => "REVOKE",
        Statement::RebuildUniqueIndex { .. } => "REBUILD UNIQUE INDEX",
        Statement::OptimizeTable { .. } => "OPTIMIZE",
        Statement::FlushTable { .. } => "FLUSH",
        Statement::AnalyzeTable { .. } => "ANALYZE",
        _ => "this statement",
    }
}

fn execute_stmt(&self, session: &mut Session, stmt: &Statement) -> Result<ExecResult> {
        self.ensure_permissions(session, stmt)?;
        if session.in_txn() {
            if let Some(verb) = Self::transaction_forbidden_verb(stmt) {
                return Err(TaibaiError::Sql(format!(
                    "{verb} is not supported inside an explicit transaction; COMMIT or ROLLBACK first"
                )));
            }
        }
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
                durability,
                compression_level,
            } => {
                let db = name.effective_database(&session.database).to_string();
                let table = name.name.clone();

                super::ddl::exec_create_table(
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
                    durability,
                    compression_level,
                )
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
                
                super::ddl::exec_drop_table(self, &db, &table, *if_exists)
            }
            Statement::AlterTable { name, operations } => {
                let db = name.effective_database(&session.database).to_string();
                let table = name.name.clone();
                
                super::ddl::exec_alter_table(self, &db, &table, operations)
            }
            Statement::Truncate { table } => {
                let db = table.effective_database(&session.database).to_string();
                let table_name = table.name.clone();
                
                super::ddl::exec_truncate_table(self, &db, &table_name)
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
            Statement::ReplaceSelect {
                table,
                columns,
                query,
            } => {
                let txn_id = session.active_txn_id();
                if txn_id > 0 {
                    return Err(TaibaiError::Sql(
                        "REPLACE is not supported inside an explicit transaction".to_string(),
                    ));
                }
                let query_result =
                    super::query::exec_select_with_txn(self, &session.database, query, txn_id)?;
                let ExecResult::Query { batches, .. } = query_result else {
                    return Err(TaibaiError::Internal(
                        "REPLACE SELECT source did not return rows".to_string(),
                    ));
                };
                let mut values = Vec::new();
                for batch in batches {
                    for row_idx in 0..batch.num_rows() {
                        let mut row = Vec::with_capacity(batch.num_columns());
                        for col_idx in 0..batch.num_columns() {
                            let value = batch.scalar(row_idx, col_idx).ok_or_else(|| {
                                TaibaiError::Internal(
                                    "REPLACE SELECT result column is missing".to_string(),
                                )
                            })?;
                            row.push(crate::sql::parser::ast::Expr::Literal(value));
                        }
                        values.push(row);
                    }
                }
                super::dml_insert::exec_replace(
                    self,
                    table.effective_database(&session.database),
                    &table.name,
                    columns,
                    &values,
                    session.timezone_offset_hours,
                    txn_id,
                    session.bulk_import_mode,
                )
            }
            Statement::InsertSelect {
                table,
                columns,
                query,
            } => {
                // Rows are read from the current snapshot and then routed
                // through the regular INSERT path (constraint validation,
                // partition routing, catalog accounting), mirroring
                // REPLACE SELECT. Like REPLACE SELECT this materializes the
                // whole result before writing; the SELECT's LIMIT/OFFSET
                // clauses are the knob for oversized sources.
                let txn_id = session.active_txn_id();
                let query_result =
                    super::query::exec_select_with_txn(self, &session.database, query, txn_id)?;
                let ExecResult::Query { batches, .. } = query_result else {
                    return Err(TaibaiError::Internal(
                        "INSERT SELECT source did not return rows".to_string(),
                    ));
                };
                // Stream the source in row chunks instead of materializing
                // the whole result as boxed Expr literals first (peak memory
                // used to be 2-3x the source). Every chunk goes through the
                // regular INSERT path, so constraint validation, partition
                // routing and catalog accounting are unchanged; a failing
                // chunk leaves earlier chunks inserted, matching the
                // multi-partition INSERT path's existing partial-failure
                // semantics. The SELECT's LIMIT/OFFSET remain the knob for
                // one-shot bulk loads.
                const INSERT_SELECT_CHUNK_ROWS: usize = 4096;
                let db = table.effective_database(&session.database);
                let mut chunk: Vec<Vec<crate::sql::parser::ast::Expr>> = Vec::new();
                let mut rows_affected_total = 0u64;
                let flush_chunk = |chunk: &mut Vec<Vec<crate::sql::parser::ast::Expr>>| {
                    if chunk.is_empty() {
                        return Ok(ExecResult::Ok { rows_affected: 0 });
                    }
                    let result = super::dml_insert::exec_insert(
                        self,
                        &db,
                        &table.name,
                        columns,
                        chunk.as_slice(),
                        session.timezone_offset_hours,
                        txn_id,
                        session.bulk_import_mode,
                    );
                    if result.is_ok() {
                        chunk.clear();
                    }
                    result
                };
                for batch in &batches {
                    for row_idx in 0..batch.num_rows() {
                        let mut row = Vec::with_capacity(batch.num_columns());
                        for col_idx in 0..batch.num_columns() {
                            let value = batch.scalar(row_idx, col_idx).ok_or_else(|| {
                                TaibaiError::Internal(
                                    "INSERT SELECT result column is missing".to_string(),
                                )
                            })?;
                            row.push(crate::sql::parser::ast::Expr::Literal(value));
                        }
                        chunk.push(row);
                        if chunk.len() >= INSERT_SELECT_CHUNK_ROWS {
                            let inserted = flush_chunk(&mut chunk)?;
                            if let ExecResult::Ok { rows_affected } = inserted {
                                rows_affected_total += rows_affected;
                            }
                        }
                    }
                }
                let last_result = flush_chunk(&mut chunk);
                if last_result.is_ok() && txn_id > 0 {
                    session.mark_txn_write();
                }
                last_result.map(|_| ExecResult::Ok {
                    rows_affected: rows_affected_total,
                })
            }
            Statement::Update {
                table,
                assignments,
                where_clause,
                ..
            } => {
                // The table alias only qualifies SET targets and WHERE
                // columns; both resolve by bare column name, so the alias
                // needs no execution-time handling.
                let db = table.effective_database(&session.database).to_string();
                let result = super::dml_update::exec_update(
                    self,
                    &db,
                    &table.name,
                    assignments,
                    where_clause,
                    session.active_txn_id(),
                );
                if result.is_ok() && session.in_txn() {
                    session.mark_txn_write();
                }
                result
            }
            Statement::Delete {
                table,
                where_clause,
                ..
            } => {
                // The table alias only matters for WHERE-clause qualification;
                // qualified columns resolve by column name regardless of the
                // qualifier, so the alias needs no execution-time handling.
                let db = table.effective_database(&session.database).to_string();
                let result =
                    super::dml_delete::exec_delete(
                    self,
                    &db,
                    &table.name,
                    where_clause,
                    session.active_txn_id(),
                );
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
                let rows = self.rollback_session_txn(session)?;
                Ok(ExecResult::Ok { rows_affected: rows })
            }
            Statement::ShowStats => super::ddl::exec_show_stats(self, &session.database),
            Statement::ShowPerformanceBaselines => {
                super::performance::exec_show_performance_baselines(self, &session.database)
            }
            Statement::ShowEngines => super::ddl::exec_show_engines(self),
            Statement::ShowProcesslist { full } => super::ddl::exec_show_processlist(self, *full),
            Statement::KillQuery { query_id } => {
                super::ddl::exec_kill_query(self, &session.username, *query_id)
            }
            Statement::ResetPerformanceBaselines => {
                super::performance::exec_reset_performance_baselines(self, &session.database)
            }
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
            Statement::Union { .. } => super::query::exec_union_with_txn(
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
            Statement::AnalyzeTable { table } => super::ddl::exec_analyze_table(
                self,
                table.effective_database(&session.database),
                &table.name,
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
            Statement::Insert { table, .. } => {
                self.require_table_permission(
                    session,
                    PermissionType::Insert,
                    table.effective_database(&session.database),
                    &table.name,
                )?;
                self.require_read_table_permissions(session, stmt)
            }
            Statement::Replace { table, .. } => {
                self.require_table_permission(
                    session,
                    PermissionType::Insert,
                    table.effective_database(&session.database),
                    &table.name,
                )?;
                self.require_table_permission(
                    session,
                    PermissionType::Delete,
                    table.effective_database(&session.database),
                    &table.name,
                )?;
                self.require_read_table_permissions(session, stmt)
            }
            Statement::ReplaceSelect { table, query, .. } => {
                self.require_table_permission(
                    session,
                    PermissionType::Insert,
                    table.effective_database(&session.database),
                    &table.name,
                )?;
                self.require_table_permission(
                    session,
                    PermissionType::Delete,
                    table.effective_database(&session.database),
                    &table.name,
                )?;
                self.ensure_permissions(session, query)
            }
            Statement::InsertSelect { table, query, .. } => {
                self.require_table_permission(
                    session,
                    PermissionType::Insert,
                    table.effective_database(&session.database),
                    &table.name,
                )?;
                self.ensure_permissions(session, query)
            }
            Statement::Update { table, .. } => {
                self.require_table_permission(
                    session,
                    PermissionType::Update,
                    table.effective_database(&session.database),
                    &table.name,
                )?;
                self.require_read_table_permissions(session, stmt)
            }
            Statement::Delete { table, .. } => {
                self.require_table_permission(
                    session,
                    PermissionType::Delete,
                    table.effective_database(&session.database),
                    &table.name,
                )?;
                self.require_read_table_permissions(session, stmt)
            }
            Statement::Select { .. } => self.require_read_table_permissions(session, stmt),
            Statement::Union { left, right, .. } => {
                self.ensure_permissions(session, left)?;
                self.ensure_permissions(session, right)?;
                self.require_read_table_permissions(session, stmt)
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
            Statement::ShowStats | Statement::ShowPerformanceBaselines => {
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
            | Statement::AnalyzeTable { table }
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
            | Statement::KillQuery { .. }
            | Statement::PauseFlush
            | Statement::ResumeFlush
            | Statement::ResetPerformanceBaselines => self.require_global_admin(session),
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

    /// Require SELECT on every table a statement reads beyond its primary
    /// write/FROM grant surface: subqueries in WHERE / SET / select items,
    /// derived tables, and join ON conditions. Runs on the full statement so
    /// no read path — SELECT, DML, prepared, streaming — can bypass it.
    fn require_read_table_permissions(&self, session: &Session, stmt: &Statement) -> Result<()> {
        let mut tables = Vec::new();
        super::query::collect_read_tables(stmt, &session.database, &mut tables);
        for reference in tables {
            self.require_table_permission(
                session,
                PermissionType::Select,
                &reference.database,
                &reference.table,
            )?;
        }
        Ok(())
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
}

/// Stable fingerprint of the table schemas a prepared plan was compiled
/// against: column names, types, scales and orders, plus table names.
fn schema_fingerprint(
    schemas: &std::collections::HashMap<String, crate::types::batch::Schema>,
) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    let mut names: Vec<&String> = schemas.keys().collect();
    names.sort();
    for name in names {
        name.hash(&mut hasher);
        let schema = &schemas[name];
        for col in &schema.columns {
            col.name.hash(&mut hasher);
            (col.data_type as u8).hash(&mut hasher);
            col.scale.hash(&mut hasher);
        }
    }
    hasher.finish()
}
