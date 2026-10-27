use super::constraints::*;
use super::fast::*;
use super::*;

/// The single condition under which an INSERT's rows are mirrored into the
/// hot store at statement time: auto-commit regular-path writes only.
/// Transactional rows are mirrored at COMMIT (or accounted as unfed), and
/// bulk/relaxed writes feed through the bulk tails. The feed guard, the
/// pubsub payload capture, and the `hot_fed` failure accounting all derive
/// from this one predicate so the three can no longer drift apart by
/// comment.
fn hot_mirror_eligible(txn_id: u64, bulk_import_mode: bool, relaxed_direct_insert: bool) -> bool {
    txn_id == 0 && !bulk_import_mode && !relaxed_direct_insert
}

#[allow(clippy::too_many_arguments)]
pub fn exec_replace(
    engine: &Engine,
    db: &str,
    table: &str,
    columns: &[String],
    values: &[Vec<Expr>],
    timezone_offset_hours: i32,
    txn_id: u64,
    _bulk_import_mode: bool,
) -> Result<ExecResult> {
    let mut profile = super::replace_profile::ReplaceProfile::new(db, table, values.len());
    if txn_id > 0 {
        return Err(TaibaiError::Sql(
            "REPLACE is not supported inside an explicit transaction".to_string(),
        ));
    }

    // Same pin-exclusion as auto-commit DELETE/UPDATE: REPLACE physically
    // removes conflicting memtable rows (shifting positions), which would
    // make an open transaction's COMMIT delete the wrong rows.
    if engine.txn_mutations().table_has_pins(db, table) {
        return Err(TaibaiError::ResourceBusy(format!(
            "table '{}.{}' is pinned by an open transaction; retry after it commits or rolls back",
            db, table
        )));
    }

    profile.phase("constraint_wait");
    let _unique_guard = acquire_constraint_insert_lock(db, table)?;
    profile.phase("materialize");
    let materialized =
        materialize_insert_values(engine, db, table, columns, values, timezone_offset_hours)?;
    let constraints = replace_constraint_indices(&materialized.table_meta, &materialized.schema)?;
    let key_sets = replace_key_sets(&materialized.batch, &constraints);
    let (survivors, replaced_inside_batch) =
        collapse_replace_batch(&materialized.batch, &constraints)?;
    let affected_insert_rows = materialized.batch.num_rows() as u64;
    let physical_insert_rows = survivors.num_rows() as u64;
    let location_schema = materialized.schema.clone();
    // Pseudo unique-index entries (empty segment_path) marking the replacement
    // rows as existing. Published only after the staged insert commits, so a
    // later plain INSERT still sees the keys without a segment location.
    let replace_unique_constraints =
        unique_constraints(&materialized.table_meta, &location_schema)?;
    let mut replace_unique_entries = build_unique_index_entries_for_batch(
        &materialized.table_meta,
        &location_schema,
        &survivors,
        &replace_unique_constraints,
    )?;
    let replace_primary_indices = primary_key_indices(&location_schema);
    if !replace_primary_indices.is_empty() {
        replace_unique_entries.push((
            crate::storage::index::index_manager::PRIMARY_LOCATION_INDEX_NAME.to_string(),
            primary_location_entries_for_batch(
                &location_schema,
                &survivors,
                &replace_primary_indices,
            ),
        ));
    }
    let internal_txn_id = engine.alloc_txn_id();
    profile.txn_id = internal_txn_id;

    // Select only large, sparsely conflicting memtables. A new key must not
    // flush unrelated pending data. Selection is read-only under the table
    // lock; flush I/O happens after release, and the main lock below resolves
    // conflicts afresh rather than trusting preflight row positions.
    profile.phase("checkpoint_candidates");
    let checkpoint_candidates = if key_sets.is_empty() {
        Vec::new()
    } else {
        engine
            .memtable_manager()
            .get_flushable_by_table(db, table)
            .into_iter()
            .filter(|mt| mt.row_count() >= super::replace::REPLACE_CHECKPOINT_MIN_ROWS)
            .collect::<Vec<_>>()
    };
    if !checkpoint_candidates.is_empty() {
        profile.phase("checkpoint_wait");
        let selected = engine
            .segment_mgr()
            .with_table_mutation_lock(db, table, || {
                profile.phase("checkpoint_probe");
                crate::sql::execution::txn_dml::refuse_auto_commit_dml_while_pinned(
                    engine, db, table,
                )?;
                super::replace::select_replace_checkpoint_memtables(
                    engine,
                    &checkpoint_candidates,
                    &key_sets,
                )
            })?;
        profile.phase("checkpoint_flush");
        engine
            .batch_writer()
            .checkpoint_selected_memtables(db, table, &selected)?;
        tracing::debug!(
            db,
            table,
            candidate_memtables = checkpoint_candidates.len(),
            selected_memtables = selected.len(),
            "REPLACE checkpoint selection"
        );
    }

    // Warm any missing location snapshots before entering the mutation lock.
    // The in-lock probe below repeats against current state; if maintenance
    // invalidated a snapshot in between it returns a retryable stale error
    // rather than doing a full scan under the lock.
    profile.phase("location_index_prepare");
    prepare_replace_location_indexes(
        engine,
        db,
        table,
        &materialized.table_meta,
        &location_schema,
        &key_sets,
    )?;

    profile.phase("table_wait");
    let (deleted, undo, pending_commits) =
        engine
            .segment_mgr()
            .with_table_mutation_lock(db, table, || {
                profile.table_acquired();
                // In-lock re-check (the check at the fn top races a
                // transaction registering its pin under this same lock):
                // REPLACE shifts memtable positions, which would make that
                // transaction's COMMIT delete the wrong rows.
                crate::sql::execution::txn_dml::refuse_auto_commit_dml_while_pinned(engine, db, table)?;
                let conflict_locations = ensure_replace_location_indexes(
                    engine,
                    db,
                    table,
                    &materialized.table_meta,
                    &location_schema,
                    &key_sets,
                )?;
                profile.phase("memtable_plan");
                let memtable_plans = crate::sql::execution::dml_delete::plan_replace_memtable_conflicts(
                    engine, db, table, &key_sets,
                )?;

                // Stage the replacement rows FIRST, into the private
                // transaction (invisible until commit; nothing durable is
                // rewritten yet), and collect the durability watermark in the
                // same step through the shared staging entry — the watermark
                // is only meaningful after the staging writes created the
                // transaction's memtables. The delete legs below then rewrite
                // segment footers under the DEFERRED journal lifecycle,
                // journaling this staging's watermark, so a crash between a
                // footer's tombstone fsync and the commit rolls the old rows
                // back at startup instead of deleting them with no
                // replacement — the same window UPDATE's stage-first order
                // closes. (The reverse order — tombstones first, durable
                // insert second — leaves a crash window of "new row visible
                // while the old row still exists", which recovery cannot
                // arbitrate; the staged-then-commit protocol keeps every
                // crash outcome atomic.)
                profile.phase("stage_images");
                let staged_images = match crate::sql::execution::dml_staging::stage_image_transaction(
                    engine,
                    db,
                    table,
                    internal_txn_id,
                    |txn_id| {
                        exec_materialized_insert_batch(
                            engine,
                            db,
                            table,
                            &materialized.table_meta,
                            materialized.schema,
                            survivors,
                            txn_id,
                            false,
                            false,
                            false,
                            true,
                            materialized.missing_auto_inc,
                            false,
                            false,
                        )
                        .map(|_| ())
                    },
                ) {
                    Ok(staged) => staged,
                    Err((txn_id, error)) => {
                        // Nothing else was mutated yet: discard the invisible
                        // staged rows and fail. Best-effort cleanup — the
                        // original error wins.
                        if let Err(rollback_error) = engine.batch_writer().rollback_txn(txn_id) {
                            tracing::warn!(
                                db,
                                table,
                                txn_id,
                                error = %rollback_error,
                                "REPLACE staged-insert rollback failed; staged rows may linger invisibly"
                            );
                        }
                        let _ = engine.txn_mutations().remove(txn_id);
                        return Err(error);
                    }
                };
                // Durability watermark for the deferred footer journals: the
                // staged rows live in the private transaction's memtables;
                // once their tails are durable, recovery keeps the
                // tombstones (roll forward), otherwise it restores the
                // pre-statement footers (roll back).
                let image_checks = staged_images.watermark;

                profile.phase("segment_delete");
                let mut undo = match super::replace::delete_replace_conflicts_under_table_lock(
                    engine,
                    db,
                    table,
                    &materialized.table_meta,
                    &key_sets,
                    conflict_locations,
                    image_checks.as_slice(),
                ) {
                    Ok(undo) => undo,
                    Err(error) => {
                        // The delete leg restored its own partial work
                        // (immediate footer rewrites, which also clear any
                        // deferred journal at the same path); discard the
                        // staged rows. Best-effort — the original error wins.
                        if let Err(rollback_error) =
                            engine.batch_writer().rollback_txn(internal_txn_id)
                        {
                            tracing::warn!(
                                db,
                                table,
                                txn_id = internal_txn_id,
                                error = %rollback_error,
                                "REPLACE staged-insert rollback after failed conflict delete failed"
                            );
                        }
                        let _ = engine.txn_mutations().remove(internal_txn_id);
                        return Err(error);
                    }
                };
                // The resident hot store is append-only and cannot remove the rows
                // replaced below. Drop it before filtering so subsequent reads
                // rebuild from the authoritative segments plus memtables.
                profile.phase("memtable_delete");
                engine.hot_store_manager().remove(db, table);
                match crate::sql::execution::dml_delete::apply_replace_memtable_conflicts(
                    engine,
                    db,
                    table,
                    memtable_plans,
                    image_checks.as_slice(),
                ) {
                    Ok(memtable_undo) => undo.merge(memtable_undo),
                    Err(error) => {
                        if let Err(rollback_error) = rollback_replace_under_table_lock(
                            engine,
                            db,
                            table,
                            &materialized.table_meta,
                            internal_txn_id,
                            &undo,
                        ) {
                            tracing::warn!(
                                db,
                                table,
                                error = %rollback_error,
                                "REPLACE rollback after failed memtable conflict delete failed"
                            );
                        }
                        return Err(error);
                    }
                }
                let deleted = undo.deleted_rows();

                #[cfg(test)]
                if FORCE_REPLACE_INSERT_FAILURE.with(|force| force.replace(false)) {
                    rollback_replace_under_table_lock(
                        engine,
                        db,
                        table,
                        &materialized.table_meta,
                        internal_txn_id,
                        &undo,
                    )?;
                    return Err(TaibaiError::Internal(
                        "forced REPLACE insert failure".to_string(),
                    ));
                }

                profile.phase("commit_submit");
                let finalize_result = engine
                    .batch_writer()
                    .commit_txn_submit_under_table_lock(internal_txn_id);
                let pending_commits = match finalize_result {
                    Ok(pending_commits) => pending_commits,
                    Err(error) => {
                        rollback_replace_under_table_lock(
                            engine,
                            db,
                            table,
                            &materialized.table_meta,
                            internal_txn_id,
                            &undo,
                        )?;
                        return Err(error);
                    }
                };

                profile.phase("index_publish");
                // Publish existence markers for the committed replacement rows.
                // Failure only downgrades later validation to a full scan; the
                // statement is already committed and must not fail here.
                if let Err(error) = append_unique_index_entries_after_insert(
                    engine,
                    db,
                    table,
                    replace_unique_entries,
                ) {
                    tracing::warn!(
                        db,
                        table,
                        error = %error,
                        "failed to publish REPLACE unique-index entries; invalidating indexes"
                    );
                    let index_names = materialized
                        .table_meta
                        .unique_indexes
                        .iter()
                        .map(|index| index.name.clone())
                        .collect::<Vec<_>>();
                    engine
                        .index_manager()
                        .invalidate_table_unique_index_snapshots(db, table, &index_names);
                }

                Ok((deleted, undo, pending_commits))
            })?;

    profile.table_released();

    profile.phase("commit_wait");
    if let Err(error) = engine
        .batch_writer()
        .wait_pending_tail_commits(pending_commits)
    {
        let rollback_result = engine
            .segment_mgr()
            .with_table_mutation_lock(db, table, || {
                rollback_replace_under_table_lock(
                    engine,
                    db,
                    table,
                    &materialized.table_meta,
                    internal_txn_id,
                    &undo,
                )
            });
        if let Err(rollback_error) = rollback_result {
            tracing::warn!(
                db,
                table,
                error = %rollback_error,
                "failed to restore replaced rows after durable commit failure"
            );
        }
        return Err(error);
    }

    profile.phase("cleanup");
    // The staged replacement rows are durable: the tombstone legs are now
    // unconditionally correct. Clear the deferred journals under a brief
    // second table-lock acquisition — journals are only ever written under
    // this lock, so the ownership-matched clear cannot race a concurrent
    // statement replacing the same journal path. Best effort: a leftover is
    // re-judged by startup recovery, which finds the watermark met and rolls
    // forward.
    if !undo.segment_footers.is_empty() {
        let table_dir = engine.data_dir().join(db).join(table);
        let journal_segments: Vec<(std::path::PathBuf, Vec<u8>, u64)> = undo
            .segment_footers
            .iter()
            .map(|(path, _, _, bitmap, deleted)| (path.clone(), bitmap.clone(), *deleted))
            .collect();
        let _ = engine
            .segment_mgr()
            .with_table_mutation_lock(db, table, || {
                for (path, bitmap, deleted) in &journal_segments {
                    crate::storage::segment::reader::clear_deferred_delete_journal_for_statement(
                        &table_dir, path, bitmap, *deleted,
                    );
                }
                Ok(())
            });
    }
    // The replacement rows are durable, so the memtable legs' guarded
    // sidecars are unconditionally correct: drop their guards (a leftover
    // would needlessly gate the memtable's flush until the next recovery).
    {
        let table_dir = engine.data_dir().join(db).join(table);
        for memtable in undo.guarded_memtables() {
            crate::storage::core::durable_tail::clear_statement_guards(&table_dir, memtable.id());
        }
    }
    // The internal transaction is terminal — drop its registry entry (same
    // cleanup as UPDATE's image transaction).
    let _ = engine.txn_mutations().remove(internal_txn_id);

    profile.phase("finalize_delete");
    super::replace::finalize_replace_delete(engine, db, table, &materialized.table_meta, &undo)?;
    profile.phase("catalog_insert");
    engine.record_inserted_rows(db, table, physical_insert_rows as i64, false, true)?;
    if materialized
        .table_meta
        .columns
        .iter()
        .any(|column| column.auto_increment)
    {
        engine.persist_auto_increment_next_value(db, table, false)?;
    }
    // REPLACE deletes and re-inserts through its own internal transaction and
    // never mirrors into the hot store; retire the resident fast path rather
    // than risk stale rows surviving in the mirror.
    engine.invalidate_hot_store(db, table);

    profile.success = true;
    Ok(ExecResult::Ok {
        rows_affected: deleted + affected_insert_rows + replaced_inside_batch,
    })
}

#[allow(clippy::too_many_arguments)]
pub fn exec_insert(
    engine: &Engine,
    db: &str,
    table: &str,
    columns: &[String],
    values: &[Vec<Expr>],
    timezone_offset_hours: i32,
    txn_id: u64,
    bulk_import_mode: bool,
) -> Result<ExecResult> {
    exec_insert_inner(
        engine,
        db,
        table,
        columns,
        values,
        timezone_offset_hours,
        txn_id,
        bulk_import_mode,
        true,
    )
}

#[allow(clippy::too_many_arguments)]
fn exec_insert_inner(
    engine: &Engine,
    db: &str,
    table: &str,
    columns: &[String],
    values: &[Vec<Expr>],
    timezone_offset_hours: i32,
    txn_id: u64,
    bulk_import_mode: bool,
    lock_unique_constraints: bool,
) -> Result<ExecResult> {
    let materialized =
        materialize_insert_values(engine, db, table, columns, values, timezone_offset_hours)?;

    exec_materialized_insert_batch(
        engine,
        db,
        table,
        &materialized.table_meta,
        materialized.schema,
        materialized.batch,
        txn_id,
        bulk_import_mode,
        lock_unique_constraints,
        true,
        false,
        materialized.missing_auto_inc,
        true,
        true,
    )
}

pub(crate) struct InsertSelectPlan {
    pub table_meta: std::sync::Arc<crate::storage::catalog::schema::TableMeta>,
    pub schema: Schema,
    pub provided_by_col: Vec<Option<usize>>,
    pub missing_auto_inc: Vec<usize>,
}

pub(crate) fn resolve_insert_select_plan(
    engine: &Engine,
    db: &str,
    table: &str,
    columns: &[String],
    source_num_cols: usize,
) -> Result<InsertSelectPlan> {
    let catalogs = engine.catalogs().read().unwrap();
    let catalog = catalogs
        .get(db)
        .ok_or_else(|| TaibaiError::Sql(format!("database '{}' does not exist", db)))?;
    let table_meta = catalog
        .get_table_shared(table)
        .ok_or_else(|| TaibaiError::Sql(format!("table '{}.{}' does not exist", db, table)))?
        .clone();
    engine
        .segment_mgr()
        .set_table_compression_level(db, table, table_meta.compression_level);
    let schema = Schema::new(table_meta.columns.clone());
    let col_indices = if columns.is_empty() {
        if source_num_cols != schema.columns.len() {
            return Err(TaibaiError::Sql(format!(
                "INSERT SELECT has {} source columns but table has {} columns",
                source_num_cols,
                schema.columns.len()
            )));
        }
        (0..schema.columns.len()).collect()
    } else {
        if source_num_cols != columns.len() {
            return Err(TaibaiError::Sql(format!(
                "INSERT SELECT has {} source columns but {} columns were specified",
                source_num_cols,
                columns.len()
            )));
        }
        let mut seen = HashSet::new();
        let mut indices = Vec::with_capacity(columns.len());
        for col_name in columns {
            let idx = schema
                .index_of(col_name)
                .ok_or_else(|| TaibaiError::Sql(format!("column '{}' not found", col_name)))?;
            if !seen.insert(idx) {
                return Err(TaibaiError::Sql(format!(
                    "column '{}' specified more than once",
                    col_name
                )));
            }
            indices.push(idx);
        }
        indices
    };
    let mut provided_by_col: Vec<Option<usize>> = vec![None; schema.columns.len()];
    for (value_idx, &col_idx) in col_indices.iter().enumerate() {
        provided_by_col[col_idx] = Some(value_idx);
    }
    let auto_inc_cols: Vec<usize> = schema
        .columns
        .iter()
        .enumerate()
        .filter(|(_, c)| c.auto_increment)
        .map(|(i, _)| i)
        .collect();
    let missing_auto_inc: Vec<usize> = auto_inc_cols
        .iter()
        .filter(|&idx| provided_by_col[*idx].is_none())
        .cloned()
        .collect();
    drop(catalogs);
    Ok(InsertSelectPlan {
        table_meta,
        schema,
        provided_by_col,
        missing_auto_inc,
    })
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn exec_insert_select_batch(
    engine: &Engine,
    db: &str,
    table: &str,
    table_meta: &std::sync::Arc<crate::storage::catalog::schema::TableMeta>,
    target_schema: &Schema,
    provided_by_col: &[Option<usize>],
    missing_auto_inc: &[usize],
    source_batch: RecordBatch,
    txn_id: u64,
    bulk_import_mode: bool,
    timezone_offset_hours: i32,
) -> Result<ExecResult> {
    let num_rows = source_batch.num_rows();
    if num_rows == 0 {
        return Ok(ExecResult::Ok { rows_affected: 0 });
    }
    let auto_inc_counter = if !missing_auto_inc.is_empty() {
        engine.auto_increment_manager().get(db, table)
    } else {
        None
    };
    let mut columns: Vec<ColumnArray> = Vec::with_capacity(target_schema.columns.len());
    for (target_col_idx, target_col) in target_schema.columns.iter().enumerate() {
        if let Some(src_value_idx) = provided_by_col[target_col_idx] {
            let src_col = &source_batch.columns[src_value_idx];
            if src_col.data_type() == target_col.data_type {
                columns.push(src_col.shared_clone());
            } else {
                let mut col = ColumnArray::with_capacity(target_col.data_type, num_rows);
                for row in 0..num_rows {
                    let scalar = source_batch.scalar(row, src_value_idx).ok_or_else(|| {
                        TaibaiError::Internal("INSERT SELECT source column missing".into())
                    })?;
                    let casted = eval_expr_to_scalar_with_scale_timezone(
                        &Expr::Literal(scalar),
                        &target_col.data_type,
                        target_col.scale,
                        timezone_offset_hours,
                    )?;
                    col.push(&casted)?;
                }
                columns.push(col);
            }
        } else {
            let mut col = ColumnArray::with_capacity(target_col.data_type, num_rows);
            for _ in 0..num_rows {
                let scalar = materialize_missing_insert_scalar(
                    engine,
                    db,
                    table,
                    &auto_inc_counter,
                    target_schema,
                    target_col_idx,
                    timezone_offset_hours,
                )?;
                col.push(&scalar)?;
            }
            columns.push(col);
        }
    }
    let batch = RecordBatch::try_new(target_schema.clone(), columns)?;
    exec_materialized_insert_batch(
        engine,
        db,
        table,
        table_meta,
        target_schema.clone(),
        batch,
        txn_id,
        bulk_import_mode,
        true,
        true,
        false,
        missing_auto_inc.to_vec(),
        true,
        true,
    )
}

fn materialize_insert_values(
    engine: &Engine,
    db: &str,
    table: &str,
    columns: &[String],
    values: &[Vec<Expr>],
    timezone_offset_hours: i32,
) -> Result<MaterializedInsert> {
    let catalogs = engine.catalogs().read().unwrap();
    let catalog = catalogs
        .get(db)
        .ok_or_else(|| TaibaiError::Sql(format!("database '{}' does not exist", db)))?;

    // Shared snapshot: escapes the catalog lock as an Arc clone instead of a
    // full TableMeta copy (all column defs, indexes and keys re-allocated).
    let table_meta = catalog
        .get_table_shared(table)
        .ok_or_else(|| TaibaiError::Sql(format!("table '{}.{}' does not exist", db, table)))?
        .clone();

    // Keep the storage-side per-table zstd level override in sync with the
    // catalog before any append/flush can create a writer.
    engine
        .segment_mgr()
        .set_table_compression_level(db, table, table_meta.compression_level);

    let schema = Schema::new(table_meta.columns.clone());

    let auto_inc_cols: Vec<usize> = schema
        .columns
        .iter()
        .enumerate()
        .filter(|(_, c)| c.auto_increment)
        .map(|(i, _)| i)
        .collect();

    let col_indices = resolve_insert_columns(&schema, columns, values)?;

    let mut provided_by_col = vec![None; schema.columns.len()];
    for (value_idx, &col_idx) in col_indices.iter().enumerate() {
        provided_by_col[col_idx] = Some(value_idx);
    }

    let missing_auto_inc: Vec<usize> = auto_inc_cols
        .iter()
        .filter(|&idx| provided_by_col[*idx].is_none())
        .cloned()
        .collect();
    let mut is_missing_auto_inc = vec![false; schema.columns.len()];
    for &idx in &missing_auto_inc {
        is_missing_auto_inc[idx] = true;
    }

    let auto_inc_counter = if !missing_auto_inc.is_empty() {
        engine.auto_increment_manager().get(db, table)
    } else {
        None
    };

    // Everything below works on the Arc-shared table_meta snapshot and the
    // engine handles; release the catalogs read guard before the per-row
    // materialization. Holding it here serialized every INSERT against DDL,
    // catalog saves and other databases' inserts for the whole (potentially
    // huge) row loop — the fast path already released before this point.
    drop(catalogs);

    let mut batch = RecordBatch::with_capacity(schema.clone(), values.len());

    if can_direct_materialize(&schema, &provided_by_col, &is_missing_auto_inc, values) {
        for row in values {
            for (col_idx, provided) in provided_by_col.iter().enumerate() {
                let val_idx = provided.unwrap();
                let column = &schema.columns[col_idx];
                push_literal_fast(
                    batch.column_mut(col_idx).unwrap(),
                    &row[val_idx],
                    column.scale,
                    timezone_offset_hours,
                    &column.name,
                    column.primary_key,
                    column.nullable,
                )?;
            }
        }
    } else {
        for row in values {
            materialize_insert_row(
                engine,
                db,
                table,
                &auto_inc_counter,
                &mut batch,
                &schema,
                &provided_by_col,
                timezone_offset_hours,
                |col_idx, value_idx| {
                    if is_default_value_expr(&row[value_idx]) {
                        materialize_missing_insert_scalar(
                            engine,
                            db,
                            table,
                            &auto_inc_counter,
                            &schema,
                            col_idx,
                            timezone_offset_hours,
                        )
                    } else {
                        eval_expr_to_scalar_with_scale_timezone(
                            &row[value_idx],
                            &schema.columns[col_idx].data_type,
                            schema.columns[col_idx].scale,
                            timezone_offset_hours,
                        )
                    }
                },
            )?;
        }
    }

    Ok(MaterializedInsert {
        table_meta,
        schema,
        batch,
        missing_auto_inc,
    })
}

#[allow(clippy::too_many_arguments)]
pub(super) fn exec_materialized_insert_batch(
    engine: &Engine,
    db: &str,
    table: &str,
    table_meta: &crate::storage::catalog::schema::TableMeta,
    schema: Schema,
    batch: RecordBatch,
    txn_id: u64,
    bulk_import_mode: bool,
    lock_unique_constraints: bool,
    validate_existing_constraints: bool,
    table_lock_held: bool,
    missing_auto_inc: Vec<usize>,
    publish_unique_index_entries: bool,
    publish_catalog: bool,
) -> Result<ExecResult> {
    let auto_inc_cols: Vec<usize> = schema
        .columns
        .iter()
        .enumerate()
        .filter(|(_, c)| c.auto_increment)
        .map(|(i, _)| i)
        .collect();
    let mut is_missing_auto_inc = vec![false; schema.columns.len()];
    for &idx in &missing_auto_inc {
        if idx < is_missing_auto_inc.len() {
            is_missing_auto_inc[idx] = true;
        }
    }

    let total_start = Instant::now();
    if !engine.segment_mgr().table_dir(db, table).is_dir() {
        return Err(TaibaiError::Sql(format!(
            "table '{}.{}' does not exist",
            db, table
        )));
    }
    let row_count = batch.num_rows() as u64;
    // RELAXED tables opt qualifying large auto-commit multi-row INSERTs into
    // the direct segment write path: no memtable, no durable tail, no
    // double write. Durability/visibility of the tail of the import is
    // bounded by the active-segment row-group flush (idle flush within
    // seconds, or full row groups), which the DURABILITY = RELAXED contract
    // documents.
    let relaxed_direct_insert = txn_id == 0
        && !bulk_import_mode
        && table_meta.durability == TableDurability::Relaxed
        && batch.num_rows() >= RELAXED_DIRECT_INSERT_MIN_ROWS;
    log_insert_profile(
        "cluster_sort",
        row_count as usize,
        std::time::Duration::ZERO,
    );

    let constraint_start = Instant::now();
    let unique_constraints = unique_constraints(table_meta, &schema)?;
    let has_unique_checks = !unique_constraints.is_empty();
    let primary_key_indices = primary_key_indices(&schema);
    let pk_guard =
        if !lock_unique_constraints || (primary_key_indices.is_empty() && !has_unique_checks) {
            None
        } else {
            Some(acquire_constraint_insert_lock(db, table)?)
        };
    // The stripe mutual-excludes the probe→publish window against other
    // unique-key writers. It does NOT need to cover the durable-tail fsync
    // wait: the writes below collect their pending tail commits, and the
    // stripe is dropped before they settle — concurrent same-table INSERTs
    // then join one fsync group instead of serializing on the stripe across
    // the full fsync round-trip (which defeated the group committer's
    // design for every constrained table).

    // The bulk/relaxed branch re-runs the full existing-data validation
    // inside `exec_bulk_insert_partitioned`; probing the index here too
    // doubled the constraint cost of every bulk SQL insert.
    let bulk_branch = bulk_import_mode || relaxed_direct_insert;
    if bulk_branch && crate::common::config::replication_settings().role_is_primary() {
        return Err(TaibaiError::Sql(
            "bulk/RELAXED inserts are not allowed on a replication primary;              use regular DURABLE INSERTs"
                .into(),
        ));
    }
    if bulk_branch || !validate_existing_constraints {
        validate_unique_constraints_in_batch(table, &batch, &schema, &unique_constraints)?;
    } else {
        validate_unique_constraints(engine, db, table, &batch, &schema, &unique_constraints)?;
    }

    if !primary_key_indices.is_empty() {
        if generated_auto_increment_key_guarantees_uniqueness(
            &primary_key_indices,
            &is_missing_auto_inc,
        ) {
            validate_primary_key_values_present(&batch, &schema, &primary_key_indices)?;
        } else if bulk_branch || !validate_existing_constraints {
            validate_primary_key_uniqueness_in_batch(table, &batch, &schema, &primary_key_indices)?;
        } else {
            validate_primary_key_uniqueness(
                engine,
                db,
                table,
                &batch,
                &schema,
                &primary_key_indices,
            )?;
        }
    }
    // Lock waits and constraint scans above are still cancellable. Only now
    // enter the irreversible phase, before publishing counters/rows/indexes.
    crate::common::query_cancel::check()?;
    crate::common::query_cancel::enter_committing_phase();
    advance_explicit_auto_increment_counters(
        engine,
        db,
        table,
        &batch,
        &auto_inc_cols,
        &missing_auto_inc,
    )?;

    let primary_keys = if primary_key_indices.is_empty() {
        Vec::new()
    } else {
        primary_keys_for_batch(&batch, &schema, &primary_key_indices)?
    };
    let mut unique_index_entries = if publish_unique_index_entries {
        build_unique_index_entries_for_batch(table_meta, &schema, &batch, &unique_constraints)?
    } else {
        Vec::new()
    };
    if publish_unique_index_entries && !primary_keys.is_empty() {
        // Reuse the once-materialized primary keys instead of re-extracting
        // them from the batch (C15): PK NULLs were already rejected by the
        // validation above, so every row yields exactly one entry in batch
        // order, byte-identical to what `primary_location_entries_for_batch`
        // produced from a fresh scan.
        unique_index_entries.push((
            crate::storage::index::index_manager::PRIMARY_LOCATION_INDEX_NAME.to_string(),
            primary_location_entries_from_keys(&primary_keys),
        ));
    }
    log_insert_profile(
        "constraints",
        row_count as usize,
        constraint_start.elapsed(),
    );

    // Hold the feed guard at function scope so the in-flight accounting
    // spans the hot append and the memtable staging below: a concurrent
    // backpressure reclaim must not mistake these rows for persisted prefix.
    let hot_feed_guard = if hot_mirror_eligible(txn_id, bulk_import_mode, relaxed_direct_insert) {
        Some(engine.begin_hot_feed(db, table))
    } else {
        None
    };
    if let Some(feed) = hot_feed_guard.as_ref() {
        match engine.feed_hot_store(db, table, table_meta, &batch, feed) {
            Ok(()) => {
                if engine
                    .hot_store_manager()
                    .soft_limit_exceeded_for(db, table)
                {
                    let _ = engine.flush_table_buffers(db, table);
                }
            }
            Err(crate::common::error::TaibaiError::ResourceBusy(_)) => {
                engine.reclaim_hot_store_pressure(db, table);
                engine.feed_hot_store(db, table, table_meta, &batch, feed)?
            }
            Err(e) => return Err(e),
        }
    }
    // Capture the change-notification payload before the batch moves into
    // the writer. Only immediately visible writes are published here:
    // transactional deltas are published at COMMIT by the batch writer, and
    // deferred bulk/relaxed writes become visible at flush time. The clone is
    // skipped entirely while no client is subscribed.
    let pubsub_batch = if hot_mirror_eligible(txn_id, bulk_import_mode, relaxed_direct_insert)
        && engine.pubsub().has_table_subscribers(db, table)
    {
        Some(batch.clone())
    } else {
        None
    };
    let write_start = Instant::now();
    // Pending tail commits of this statement's writes: settled only after
    // the constraint stripe is dropped (see pk_guard).
    let mut deferred_commits = Vec::new();
    if bulk_import_mode || relaxed_direct_insert {
        engine.exec_bulk_insert_partitioned(db, table, table_meta, batch, relaxed_direct_insert)?;
    } else {
        // Inside this else-branch bulk/relaxed are false, so the predicate
        // reduces to auto-commit: when it holds, every row of the batch is
        // mirrored into the hot store.
        let hot_fed = hot_mirror_eligible(txn_id, bulk_import_mode, relaxed_direct_insert);
        if let Err(error) = write_batch_by_partition_deferred(
            engine,
            db,
            table,
            table_meta,
            batch,
            schema,
            txn_id,
            table_lock_held,
            hot_fed,
            &mut deferred_commits,
        ) {
            if hot_fed {
                // The hot mirror already holds this batch but the
                // memtable/durable-tail write failed: the watermark
                // arithmetic would treat the unfed rows as committed
                // prefix-adjacent rows and the resident fast path would
                // serve ghosts the client was told were never inserted.
                // Drop the mirror entirely — it is a rebuildable
                // optimization, and UPDATE/DELETE already disable its
                // rebuild on invalidation.
                engine.invalidate_hot_store(db, table);
            }
            // Settle what earlier partitions of this statement submitted:
            // their fsync failures surface here exactly as the inline
            // settle would have reported them (the original error wins).
            let _ = engine
                .batch_writer()
                .settle_deferred_tail_commits(db, table, deferred_commits);
            return Err(error);
        }
    }
    if let Some(batch) = pubsub_batch {
        engine.pubsub().publish(db, table, batch);
    }
    log_insert_profile("write", row_count as usize, write_start.elapsed());

    let catalog_start = Instant::now();
    // Snapshot the published keys before the append consumes the entries, so
    // an explicit-transaction ROLLBACK can retract exactly these keys. The
    // record is taken before the append attempt: a half-failed publish (some
    // indexes appended, one errored) must still be retractable.
    if txn_id > 0 {
        for (index_name, entries) in &unique_index_entries {
            let keys = entries.iter().map(|entry| entry.key.clone()).collect();
            engine
                .index_manager()
                .record_txn_published_keys(txn_id, db, table, index_name, keys);
        }
        // The primary location index was already recorded by the loop above
        // when its entries were published; the standalone record from
        // primary_keys (one clone instead of two) is only needed by
        // publish-disabled callers such as REPLACE's internal staging call.
        // Retraction dedups keys, so recording once is exactly equivalent.
        if !publish_unique_index_entries && !primary_keys.is_empty() {
            engine.index_manager().record_txn_published_keys(
                txn_id,
                db,
                table,
                crate::storage::index::index_manager::PRIMARY_LOCATION_INDEX_NAME,
                primary_keys.clone(),
            );
        }
    }
    if publish_unique_index_entries {
        if let Err(error) =
            append_unique_index_entries_after_insert(engine, db, table, unique_index_entries)
        {
            // The rows are written and visible; settle their fsync before
            // propagating so the failure picture matches the inline-settle
            // contract (the original error still wins).
            let _ = engine
                .batch_writer()
                .settle_deferred_tail_commits(db, table, deferred_commits);
            return Err(error);
        }
    }
    if !primary_keys.is_empty() {
        let _ = engine
            .index_manager()
            .append_primary_keys(db, table, &primary_keys)?;
    }

    // The probe→publish window is closed: release the constraint stripe
    // BEFORE the durable-tail fsync wait, so concurrent same-table INSERTs
    // join this statement's fsync group instead of serializing on the
    // stripe across the round-trip.
    drop(pk_guard);
    engine
        .batch_writer()
        .settle_deferred_tail_commits(db, table, deferred_commits)?;

    if publish_catalog {
        // Account BEFORE record_inserted_rows: the catalog row_count
        // increment happens before its save step, so a save failure must
        // not strand an unaccounted increment that ROLLBACK would leave
        // behind (note first, retract even when record errors out).
        if txn_id > 0 {
            engine
                .txn_mutations()
                .note_inserted_rows(txn_id, db, table, row_count);
        }
        engine.record_inserted_rows(
            db,
            table,
            row_count as i64,
            // In-transaction INSERTs take the same throttled deferred catalog
            // save as auto-commit (C4) instead of serializing the whole
            // catalog JSON per statement: a crash may lose the last ~1s of
            // auto-increment-next-value persistence inside an open
            // transaction, the trade-off auto-commit already makes. Bulk
            // mode stays excluded from both (bulk flushes its dirty
            // catalogs at bulk end).
            false,
            !bulk_import_mode,
        )?;
        // No follow-up persist_auto_increment_next_value call here:
        // record_inserted_rows already writes the counter into the table
        // meta under the same catalog lock (and saves under the same
        // condition), so a second lock acquisition was pure serialization
        // overhead on every INSERT.
    }
    log_insert_profile("catalog", row_count as usize, catalog_start.elapsed());
    log_insert_profile("total_exec", row_count as usize, total_start.elapsed());

    Ok(ExecResult::Ok {
        rows_affected: row_count,
    })
}

pub(crate) fn build_unique_index_entries_for_batch(
    table_meta: &crate::storage::catalog::schema::TableMeta,
    schema: &Schema,
    batch: &RecordBatch,
    constraints: &[(String, Vec<usize>)],
) -> Result<Vec<(String, Vec<UniqueIndexEntry>)>> {
    let mut result = Vec::new();
    for (index_name, indices) in constraints {
        let columns = table_meta
            .unique_indexes
            .iter()
            .find(|index| index.name == *index_name)
            .map(|index| index.columns.clone())
            .unwrap_or_default();
        let mut entries = Vec::new();
        for row_idx in 0..batch.num_rows() {
            let Some(key) = unique_key_for_row(batch, row_idx, indices, schema) else {
                continue;
            };
            entries.push(UniqueIndexEntry {
                key,
                location: UniqueRowLocation {
                    segment_path: String::new(),
                    row_id: row_idx as u64,
                },
            });
        }
        if !columns.is_empty() && !entries.is_empty() {
            result.push((index_name.clone(), entries));
        }
    }
    Ok(result)
}

/// Primary-key location entries derived from the keys already materialized by
/// `primary_keys_for_batch` (C15): one entry per row, `row_id` = batch row
/// index. Valid by construction — primary-key validation upstream rejects
/// NULLs, so unlike the batch-scanning twin above no row can be skipped.
fn primary_location_entries_from_keys(primary_keys: &[Vec<ScalarValue>]) -> Vec<UniqueIndexEntry> {
    primary_keys
        .iter()
        .enumerate()
        .map(|(row_idx, key)| UniqueIndexEntry {
            key: key.clone(),
            location: UniqueRowLocation {
                segment_path: String::new(),
                row_id: row_idx as u64,
            },
        })
        .collect()
}

/// Pseudo existence entries (empty segment_path) for the primary-key
/// location index. Keeping this index incrementally maintained on INSERT
/// and REPLACE avoids a full-table rebuild on the next REPLACE conflict
/// lookup that `invalidate_unique_index_snapshot_if_present` used to cause.
pub(crate) fn primary_location_entries_for_batch(
    schema: &Schema,
    batch: &RecordBatch,
    indices: &[usize],
) -> Vec<UniqueIndexEntry> {
    let mut entries = Vec::new();
    for row_idx in 0..batch.num_rows() {
        let Some(key) = unique_key_for_row(batch, row_idx, indices, schema) else {
            continue;
        };
        entries.push(UniqueIndexEntry {
            key,
            location: UniqueRowLocation {
                segment_path: String::new(),
                row_id: row_idx as u64,
            },
        });
    }
    entries
}

pub(crate) fn append_unique_index_entries_after_insert(
    engine: &Engine,
    db: &str,
    table: &str,
    entries_by_index: Vec<(String, Vec<UniqueIndexEntry>)>,
) -> Result<()> {
    // Encode all indexes' entries first, then hand the whole statement to one
    // batched append: a single shared lock pass and one fsync per delta file
    // instead of a full lock/write/sync cycle per index (C3).
    let mut prepared = Vec::with_capacity(entries_by_index.len());
    for (index_name, entries) in entries_by_index {
        prepared.push((
            index_name,
            engine
                .index_manager()
                .prepare_unique_index_entries(entries)?,
        ));
    }
    match engine
        .index_manager()
        .append_prepared_unique_index_entries_batch(db, table, prepared)
    {
        Ok(()) => Ok(()),
        Err((index_name, error)) => {
            engine
                .index_manager()
                .invalidate_unique_index_snapshot(db, table, &index_name);
            Err(error)
        }
    }
}

/// Every location index of a table: named unique indexes plus the implicit
/// primary-key location index. Each entry is (index name, key columns,
/// key-column indices in the table schema); the list is empty when the table
/// has neither unique indexes nor a primary key.
#[allow(clippy::type_complexity)]
pub(crate) fn location_indexes_for_table(
    table_meta: &crate::storage::catalog::schema::TableMeta,
    schema: &Schema,
) -> Result<Vec<(String, Vec<String>, Vec<usize>)>> {
    let mut indexes = table_meta
        .unique_indexes
        .iter()
        .map(|index| {
            let indices = index
                .columns
                .iter()
                .map(|column| {
                    schema.index_of(column).ok_or_else(|| {
                        TaibaiError::Internal(format!(
                            "unique index column '{}' is missing",
                            column
                        ))
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            Ok((
                index.name.as_str().to_string(),
                index.columns.clone(),
                indices,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let primary_indices = primary_key_indices(schema);
    if !primary_indices.is_empty() {
        indexes.push((
            crate::storage::index::index_manager::PRIMARY_LOCATION_INDEX_NAME.to_string(),
            primary_indices
                .iter()
                .map(|index| table_meta.columns[*index].name.clone())
                .collect(),
            primary_indices,
        ));
    }
    Ok(indexes)
}

/// Logical memtable state covered by an unlocked scan. The segment epoch does
/// not advance for memtable-only inserts/deletes, so epoch validation alone
/// could publish a base that misses those rows. Identity detects new/retired
/// memtables; row count detects appends; content version detects wholesale
/// replacement even when the row count is unchanged.
#[derive(Clone, Debug, PartialEq, Eq)]
struct MemtableFingerprint {
    id: uuid::Uuid,
    rows: u64,
    content_version: u64,
    state: crate::storage::core::memtable::MemTableState,
    visibility: crate::storage::core::memtable::TxnVisibility,
}

fn memtable_fingerprints(
    memtables: &[std::sync::Arc<crate::storage::core::memtable::MemTable>],
) -> Vec<MemtableFingerprint> {
    let mut prints = memtables
        .iter()
        .map(|memtable| MemtableFingerprint {
            id: memtable.id(),
            rows: memtable.row_count(),
            content_version: memtable.content_version(),
            state: memtable.state(),
            visibility: memtable.visibility(),
        })
        .collect::<Vec<_>>();
    prints.sort_by_key(|print| print.id);
    prints
}

fn stale_location_rebuild(db: &str, table: &str) -> TaibaiError {
    TaibaiError::ResourceBusy(format!(
        "stale location-index rebuild for {db}.{table}; retry outside the table mutation lock"
    ))
}

fn location_rebuild_snapshot_is_current(
    segment_mgr: &SegmentManager,
    memtable_manager: &crate::storage::core::memtable::MemTableManager,
    db: &str,
    table: &str,
    flush_epoch: u64,
    memtable_prints: &[MemtableFingerprint],
) -> bool {
    segment_mgr.memtable_flush_epoch(db, table) == flush_epoch
        && memtable_fingerprints(&memtable_manager.get_by_table(db, table)) == memtable_prints
}

/// Rebuild one explicitly selected location index with the same safe
/// prepare/publish protocol as table maintenance.
#[allow(clippy::too_many_arguments)]
pub(crate) fn rebuild_single_location_index_two_phase(
    engine: &Engine,
    db: &str,
    table: &str,
    schema: &Schema,
    index_name: &str,
    columns: &[String],
    indices: &[usize],
) -> Result<()> {
    rebuild_location_indexes_two_phase(
        engine.index_manager(),
        engine.segment_mgr(),
        engine.memtable_manager(),
        engine.data_dir(),
        db,
        table,
        schema,
        &[(index_name.to_string(), columns.to_vec(), indices.to_vec())],
        None,
    )
}

/// Unlocked uniqueness-fallback rebuild: stream existing rows past the
/// statement's incoming keys, then publish only if the whole table snapshot
/// stayed stable. This preserves the old duplicate error while avoiding any
/// table scan or external merge under the mutation lock.
pub(crate) fn rebuild_unique_index_for_validation(
    engine: &Engine,
    db: &str,
    table: &str,
    schema: &Schema,
    indices: &[usize],
    index_name: &str,
    new_keys: &HashSet<Vec<ScalarValue>>,
) -> Result<()> {
    let columns = indices
        .iter()
        .map(|&idx| schema.columns[idx].name.clone())
        .collect::<Vec<_>>();
    rebuild_location_indexes_two_phase(
        engine.index_manager(),
        engine.segment_mgr(),
        engine.memtable_manager(),
        engine.data_dir(),
        db,
        table,
        schema,
        &[(index_name.to_string(), columns, indices.to_vec())],
        Some(new_keys),
    )
}

/// Full-table scan and stream preparation are entirely unlocked. Every build
/// must still describe the same epoch and memtable set when all preparation
/// finishes. Any drift discards all prepared files with a clear retry error;
/// there is deliberately no locked full-scan fallback.
#[allow(clippy::too_many_arguments)]
pub(crate) fn rebuild_table_location_indexes_two_phase(
    index_manager: &crate::storage::index::index_manager::IndexManager,
    segment_mgr: &SegmentManager,
    memtable_manager: &crate::storage::core::memtable::MemTableManager,
    data_dir: &std::path::Path,
    db: &str,
    table: &str,
    table_meta: &crate::storage::catalog::schema::TableMeta,
    schema: &Schema,
) -> Result<()> {
    let indexes = location_indexes_for_table(table_meta, schema)?;
    rebuild_location_indexes_two_phase(
        index_manager,
        segment_mgr,
        memtable_manager,
        data_dir,
        db,
        table,
        schema,
        &indexes,
        None,
    )?;
    if !indexes.is_empty() {
        index_manager.invalidate_primary_key_cache(db, table);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn rebuild_location_indexes_two_phase(
    index_manager: &crate::storage::index::index_manager::IndexManager,
    segment_mgr: &SegmentManager,
    memtable_manager: &crate::storage::core::memtable::MemTableManager,
    data_dir: &std::path::Path,
    db: &str,
    table: &str,
    schema: &Schema,
    indexes: &[(String, Vec<String>, Vec<usize>)],
    new_keys: Option<&HashSet<Vec<ScalarValue>>>,
) -> Result<()> {
    if indexes.is_empty() {
        return Ok(());
    }
    let epoch_before = segment_mgr.memtable_flush_epoch(db, table);
    let memtables = memtable_manager.get_by_table(db, table);
    let memtable_prints = memtable_fingerprints(&memtables);
    let table_dir = data_dir.join(db).join(table);
    let segments = SegmentManager::discover_segments(&table_dir)?;
    let mut builds = Vec::with_capacity(indexes.len());
    for (index_name, columns, indices) in indexes {
        let mut build = index_manager.unique_index_stream_begin(db, table, index_name)?;
        let mut visit = |entry: UniqueIndexEntry| {
            if new_keys.is_some_and(|keys| keys.contains(&entry.key)) {
                return Err(duplicate_unique_key_error(table, index_name, &entry.key));
            }
            index_manager.unique_index_stream_push(&mut build, entry)
        };
        crate::sql::execution::dml_insert::constraints::visit_unique_index_entries_in_segments(
            segment_mgr,
            &segments,
            schema,
            indices,
            &mut visit,
        )?;
        crate::sql::execution::dml_insert::constraints::visit_unique_index_entries_in_memtables(
            &memtables, schema, indices, &mut visit,
        )?;
        builds.push((index_name.clone(), columns.clone(), build));
    }

    #[cfg(test)]
    if FORCE_TWO_PHASE_REBUILD_STALE.with(|force| force.replace(false)) {
        segment_mgr.note_segments_changed(db, table);
    }
    #[cfg(test)]
    if FORCE_TWO_PHASE_MEMTABLE_STALE.with(|force| force.replace(false)) {
        if let Some(batch) = memtables.first().and_then(|mt| mt.read_snapshot_arc()) {
            let _ = memtables[0].replace_pending_batch(batch);
        }
    }
    if !location_rebuild_snapshot_is_current(
        segment_mgr,
        memtable_manager,
        db,
        table,
        epoch_before,
        &memtable_prints,
    ) {
        return Err(stale_location_rebuild(db, table));
    }

    // No index is published until every index has a complete, fsynced base.
    // Thus a scan/merge/prepare failure leaves all previous snapshots intact.
    let mut prepared = Vec::with_capacity(builds.len());
    for (index_name, columns, build) in builds {
        prepared.push((
            index_name.clone(),
            index_manager.unique_index_stream_prepare(db, table, &index_name, &columns, build)?,
        ));
    }
    segment_mgr.with_table_mutation_lock(db, table, || {
        if !location_rebuild_snapshot_is_current(
            segment_mgr,
            memtable_manager,
            db,
            table,
            epoch_before,
            &memtable_prints,
        ) {
            return Err(stale_location_rebuild(db, table));
        }
        for (index_name, base) in prepared {
            index_manager.unique_index_stream_publish(db, table, &index_name, base)?;
        }
        Ok(())
    })
}

fn prepare_replace_location_indexes(
    engine: &Engine,
    db: &str,
    table: &str,
    table_meta: &crate::storage::catalog::schema::TableMeta,
    schema: &Schema,
    key_sets: &[(Vec<usize>, HashSet<Vec<ScalarValue>>)],
) -> Result<()> {
    let indexes = location_indexes_for_table(table_meta, schema)?;
    for (index_name, _, indices) in &indexes {
        let keys = key_sets
            .iter()
            .find(|(key_indices, _)| key_indices == indices)
            .map(|(_, keys)| keys);
        let locations = match keys {
            Some(keys) => engine
                .index_manager()
                .unique_key_locations(db, table, index_name, keys)?,
            None => engine.index_manager().unique_key_locations(
                db,
                table,
                index_name,
                std::iter::empty::<&Vec<ScalarValue>>(),
            )?,
        };
        if locations.is_none() {
            // One table-wide rebuild warms every location index. It performs
            // all reads and sorting here, outside the mutation lock.
            return rebuild_table_location_indexes_two_phase(
                engine.index_manager(),
                engine.segment_mgr(),
                engine.memtable_manager(),
                engine.data_dir(),
                db,
                table,
                table_meta,
                schema,
            );
        }
    }
    Ok(())
}

fn ensure_replace_location_indexes(
    engine: &Engine,
    db: &str,
    table: &str,
    table_meta: &crate::storage::catalog::schema::TableMeta,
    schema: &Schema,
    key_sets: &[(Vec<usize>, HashSet<Vec<ScalarValue>>)],
) -> Result<super::replace::ReplaceConflictLocations> {
    let indexes = location_indexes_for_table(table_meta, schema)?;
    let mut resolved = super::replace::ReplaceConflictLocations {
        direct: Vec::new(),
        fallback: vec![false; key_sets.len()],
    };
    for (index_name, _columns, indices) in indexes {
        let key_set = key_sets
            .iter()
            .find(|(key_indices, _)| *key_indices == indices)
            .map(|(_, keys)| keys);
        let locations = match key_set {
            Some(keys) => {
                engine
                    .index_manager()
                    .unique_key_locations(db, table, &index_name, keys)?
            }
            None => engine.index_manager().unique_key_locations(
                db,
                table,
                &index_name,
                std::iter::empty::<&Vec<ScalarValue>>(),
            )?,
        };
        if locations.is_none() {
            // `prepare_replace_location_indexes` normally prevents this. A
            // concurrent invalidation after that unlocked pass must not turn
            // into a locked full scan: the caller can retry the statement or
            // let the next probe rebuild outside the lock.
            return Err(stale_location_rebuild(db, table));
        }
        if let Some(offset) = key_sets
            .iter()
            .position(|(key_indices, _)| *key_indices == indices)
        {
            match locations {
                Some(found) => resolved.direct.extend(found),
                None => resolved.fallback[offset] = true,
            }
        }
    }
    Ok(resolved)
}

fn replace_constraint_indices(
    table_meta: &crate::storage::catalog::schema::TableMeta,
    schema: &Schema,
) -> Result<Vec<Vec<usize>>> {
    let mut constraints = Vec::new();
    let primary_key = primary_key_indices(schema);
    if !primary_key.is_empty() {
        constraints.push(primary_key);
    }
    constraints.extend(
        unique_constraints(table_meta, schema)?
            .into_iter()
            .map(|(_, indices)| indices),
    );
    Ok(constraints)
}

fn replace_key_sets(
    batch: &RecordBatch,
    constraints: &[Vec<usize>],
) -> Vec<(Vec<usize>, HashSet<Vec<ScalarValue>>)> {
    constraints
        .iter()
        .map(|indices| {
            let keys = (0..batch.num_rows())
                .filter_map(|row_idx| replace_key_for_row(batch, row_idx, indices))
                .collect::<HashSet<_>>();
            (indices.clone(), keys)
        })
        .filter(|(_, keys)| !keys.is_empty())
        .collect()
}

fn rollback_replace_under_table_lock(
    engine: &Engine,
    db: &str,
    table: &str,
    table_meta: &crate::storage::catalog::schema::TableMeta,
    txn_id: u64,
    undo: &crate::sql::execution::dml_delete::ReplaceDeleteUndo,
) -> Result<()> {
    let mut errors = Vec::new();
    if let Err(error) = engine.batch_writer().rollback_txn(txn_id) {
        errors.push(format!("rollback staged insert: {}", error));
    }
    if let Err(error) = super::replace::restore_replace_delete_under_table_lock(engine, undo) {
        errors.push(format!("restore deleted rows: {}", error));
    }

    for index in &table_meta.unique_indexes {
        engine
            .index_manager()
            .remove_unique_index_snapshot(db, table, &index.name);
    }
    engine.index_manager().remove_unique_index_snapshot(
        db,
        table,
        crate::storage::index::index_manager::PRIMARY_LOCATION_INDEX_NAME,
    );
    // The internal transaction is discarded — drop its registry entry (the
    // txn_id is never reused by the allocator).
    let _ = engine.txn_mutations().remove(txn_id);

    if errors.is_empty() {
        Ok(())
    } else {
        Err(TaibaiError::Internal(format!(
            "REPLACE rollback failed: {}",
            errors.join("; ")
        )))
    }
}

fn replace_key_for_row(
    batch: &RecordBatch,
    row_idx: usize,
    indices: &[usize],
) -> Option<Vec<ScalarValue>> {
    let mut key = Vec::with_capacity(indices.len());
    for &col_idx in indices {
        let value = batch.scalar(row_idx, col_idx)?;
        // As with MySQL unique indexes, a key containing NULL does not
        // conflict. Primary-key columns have already been checked non-NULL.
        if value.is_null() {
            return None;
        }
        key.push(value);
    }
    Some(key)
}

fn collapse_replace_batch(
    batch: &RecordBatch,
    constraints: &[Vec<usize>],
) -> Result<(RecordBatch, u64)> {
    let row_count = batch.num_rows();
    if row_count <= 1 || constraints.is_empty() {
        return Ok((batch.clone(), 0));
    }

    let mut active = vec![true; row_count];
    let mut owners = constraints
        .iter()
        .map(|_| HashMap::<Vec<ScalarValue>, usize>::new())
        .collect::<Vec<_>>();
    let mut replaced = 0u64;
    for current in 0..row_count {
        let mut conflicts = HashSet::new();
        for (constraint_idx, indices) in constraints.iter().enumerate() {
            let Some(key) = replace_key_for_row(batch, current, indices) else {
                continue;
            };
            if let Some(prior) = owners[constraint_idx].insert(key, current) {
                if active[prior] {
                    conflicts.insert(prior);
                }
            }
        }
        for prior in conflicts {
            active[prior] = false;
            replaced += 1;
            for (constraint_idx, indices) in constraints.iter().enumerate() {
                let Some(key) = replace_key_for_row(batch, prior, indices) else {
                    continue;
                };
                if owners[constraint_idx].get(&key) == Some(&prior) {
                    owners[constraint_idx].remove(&key);
                }
            }
        }
    }

    let survivors = active.iter().filter(|keep| **keep).count();
    Ok((filter_batch_by_mask(batch, &active, survivors)?, replaced))
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn write_batch_by_partition(
    engine: &Engine,
    db: &str,
    table: &str,
    table_meta: &crate::storage::catalog::schema::TableMeta,
    batch: RecordBatch,
    schema: Schema,
    txn_id: u64,
    table_lock_held: bool,
    hot_fed: bool,
) -> Result<()> {
    // Transactional staging callers never produce pending tail commits (the
    // durable tail is written at COMMIT), so the scratch collector stays
    // empty and settling it is a no-op.
    let mut scratch = Vec::new();
    write_batch_by_partition_deferred(
        engine,
        db,
        table,
        table_meta,
        batch,
        schema,
        txn_id,
        table_lock_held,
        hot_fed,
        &mut scratch,
    )
}

/// Deferred-settle core: auto-commit callers that hold the constraint stripe
/// collect their pending tail commits here and settle them after releasing
/// the stripe, so the fsync wait never serializes same-table INSERTs.
#[allow(clippy::too_many_arguments)]
pub(crate) fn write_batch_by_partition_deferred(
    engine: &Engine,
    db: &str,
    table: &str,
    table_meta: &crate::storage::catalog::schema::TableMeta,
    batch: RecordBatch,
    schema: Schema,
    txn_id: u64,
    table_lock_held: bool,
    hot_fed: bool,
    deferred: &mut Vec<crate::storage::core::batch_writer::PendingTailCommit>,
) -> Result<()> {
    if table_meta.partition_key.is_empty() || batch.num_rows() == 0 {
        write_partition_batch(
            engine,
            db,
            table,
            table_meta,
            "",
            batch,
            schema,
            txn_id,
            table_lock_held,
            hot_fed,
            Some(&mut *deferred),
        )?;
        return Ok(());
    }

    let partition_start = Instant::now();
    let single_partition = single_partition_key(table_meta, &batch);
    log_insert_profile(
        "partition_route",
        batch.num_rows(),
        partition_start.elapsed(),
    );
    if let Some(partition_key) = single_partition {
        write_partition_batch(
            engine,
            db,
            table,
            table_meta,
            &partition_key,
            batch,
            schema,
            txn_id,
            table_lock_held,
            hot_fed,
            Some(&mut *deferred),
        )?;
        return Ok(());
    }

    if let Some(ranges) = partition_ranges_if_contiguous(table_meta, &batch) {
        if ranges.len() == 1 && ranges[0].1 == 0 && ranges[0].2 == batch.num_rows() {
            let (partition_key, _, _) = ranges.into_iter().next().unwrap();
            write_partition_batch(
                engine,
                db,
                table,
                table_meta,
                &partition_key,
                batch,
                schema,
                txn_id,
                table_lock_held,
                hot_fed,
                Some(&mut *deferred),
            )?;
            return Ok(());
        }
        for (partition_key, start, end) in &ranges {
            let part_batch = slice_batch_fast(&batch, *start, *end)?;
            write_partition_batch(
                engine,
                db,
                table,
                table_meta,
                partition_key,
                part_batch,
                schema.clone(),
                txn_id,
                table_lock_held,
                hot_fed,
                Some(&mut *deferred),
            )?;
        }
        return Ok(());
    }

    let groups = partition_row_indices(table_meta, &batch);
    if groups.len() <= 1 {
        let partition_key = groups
            .first()
            .map(|(key, _)| key.clone())
            .unwrap_or_default();
        write_partition_batch(
            engine,
            db,
            table,
            table_meta,
            &partition_key,
            batch,
            schema,
            txn_id,
            table_lock_held,
            hot_fed,
            Some(&mut *deferred),
        )?;
        return Ok(());
    }

    for (partition_key, indices) in groups {
        let part_batch = take_batch_rows(&batch, &indices)?;
        write_partition_batch(
            engine,
            db,
            table,
            table_meta,
            &partition_key,
            part_batch,
            schema.clone(),
            txn_id,
            table_lock_held,
            hot_fed,
            Some(&mut *deferred),
        )?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn write_partition_batch(
    engine: &Engine,
    db: &str,
    table: &str,
    table_meta: &crate::storage::catalog::schema::TableMeta,
    partition_key: &str,
    batch: RecordBatch,
    schema: Schema,
    txn_id: u64,
    table_lock_held: bool,
    hot_fed: bool,
    deferred: Option<&mut Vec<crate::storage::core::batch_writer::PendingTailCommit>>,
) -> Result<()> {
    let batch = sort_batch_for_cluster_key(table_meta, batch)?;
    if table_lock_held {
        engine
            .batch_writer()
            .write_columns_with_txn_under_table_lock(
                db,
                table,
                partition_key,
                batch,
                schema,
                txn_id,
                hot_fed,
            )?;
    } else if let Some(deferred) = deferred {
        let (_rows, pending) = engine
            .batch_writer()
            .write_columns_with_txn_deferred_settle(
                db,
                table,
                partition_key,
                batch,
                schema,
                txn_id,
                hot_fed,
            )?;
        if let Some(pending) = pending {
            deferred.push(pending);
        }
    } else {
        engine.batch_writer().write_columns_with_txn(
            db,
            table,
            partition_key,
            batch,
            schema,
            txn_id,
            hot_fed,
        )?;
    }
    Ok(())
}

fn sort_batch_for_cluster_key(
    table_meta: &crate::storage::catalog::schema::TableMeta,
    batch: RecordBatch,
) -> Result<RecordBatch> {
    if table_meta.cluster_key.is_empty() || batch.num_rows() <= 1 {
        return Ok(batch);
    }

    let mut key_indices = Vec::with_capacity(table_meta.cluster_key.len());
    for key_col in &table_meta.cluster_key {
        let idx = batch.schema.index_of(key_col).ok_or_else(|| {
            TaibaiError::Sql(format!("cluster key column '{}' not found", key_col))
        })?;
        key_indices.push(idx);
    }
    let desc = vec![false; key_indices.len()];
    let inserter = SortKeyInserter::new(key_indices, desc);
    if inserter.is_sorted(&batch) {
        return Ok(batch);
    }
    sort_batch_by_key(&batch, &inserter)
}

/// Build the typed, memoized partition-key computer for `table_meta` over
/// `batch`. `None` when the table has no partition column, mirroring
/// `get_partition_column_index_cached`.
fn partition_keys_computer<'a>(
    table_meta: &crate::storage::catalog::schema::TableMeta,
    batch: &'a RecordBatch,
) -> Option<crate::storage::partition::partition_function::PartitionKeysAt<'a>> {
    use crate::storage::partition::partition_function::{PartitionKeyKind, PartitionKeysAt};
    if table_meta.partition_key.is_empty() {
        return None;
    }
    let column = batch.column(table_meta.get_partition_column_index_cached()?)?;
    let kind = match table_meta.partition_type {
        crate::storage::catalog::schema::PartitionType::Value => PartitionKeyKind::ValueText,
        crate::storage::catalog::schema::PartitionType::None => return None,
        _ => PartitionKeyKind::Strategy(table_meta.partition_strategy()),
    };
    Some(PartitionKeysAt::new(kind, column))
}

fn single_partition_key(
    table_meta: &crate::storage::catalog::schema::TableMeta,
    batch: &RecordBatch,
) -> Option<String> {
    let mut keys = partition_keys_computer(table_meta, batch)?;
    let first_key = keys.key_at(0).to_string();

    for row_idx in 1..batch.num_rows() {
        if keys.key_at(row_idx) != first_key {
            return None;
        }
    }

    Some(first_key)
}

fn partition_ranges_if_contiguous(
    table_meta: &crate::storage::catalog::schema::TableMeta,
    batch: &RecordBatch,
) -> Option<Vec<(String, usize, usize)>> {
    if table_meta.partition_key.is_empty() || batch.num_rows() == 0 {
        return Some(vec![(String::new(), 0, batch.num_rows())]);
    }
    let mut keys = partition_keys_computer(table_meta, batch)?;
    let mut current_key = keys.key_at(0).to_string();
    let mut current_start = 0usize;
    let mut closed = HashSet::new();
    let mut ranges = Vec::new();

    for row_idx in 1..batch.num_rows() {
        let key = keys.key_at(row_idx);
        if key != current_key {
            if !closed.insert(current_key.clone()) {
                return None;
            }
            ranges.push((current_key, current_start, row_idx));
            if closed.contains(key) {
                return None;
            }
            current_key = key.to_string();
            current_start = row_idx;
        }
    }
    ranges.push((current_key, current_start, batch.num_rows()));
    Some(ranges)
}

fn slice_batch_fast(batch: &RecordBatch, start: usize, end: usize) -> Result<RecordBatch> {
    if start == 0 && end == batch.num_rows() {
        return Ok(batch.clone());
    }
    let columns = batch
        .columns
        .iter()
        .map(|col| col.slice_fast(start, end))
        .collect();
    RecordBatch::try_new(batch.schema.clone(), columns)
}

fn partition_row_indices(
    table_meta: &crate::storage::catalog::schema::TableMeta,
    batch: &RecordBatch,
) -> Vec<(String, Vec<u32>)> {
    if table_meta.partition_key.is_empty() || batch.num_rows() == 0 {
        return vec![(String::new(), (0..batch.num_rows() as u32).collect())];
    }
    let mut keys = match partition_keys_computer(table_meta, batch) {
        Some(computer) => computer,
        None => return vec![(String::new(), (0..batch.num_rows() as u32).collect())],
    };
    let mut key_order = Vec::new();
    let mut groups: HashMap<String, Vec<u32>> = HashMap::new();
    for row_idx in 0..batch.num_rows() {
        let key = keys.key_at(row_idx);
        if !groups.contains_key(key) {
            key_order.push(key.to_string());
            groups.insert(key.to_string(), Vec::new());
        }
        groups.get_mut(key).unwrap().push(row_idx as u32);
    }
    key_order
        .into_iter()
        .filter_map(|key| groups.remove(&key).map(|indices| (key, indices)))
        .collect()
}

fn take_batch_rows(batch: &RecordBatch, indices: &[u32]) -> Result<RecordBatch> {
    let columns = batch
        .columns
        .iter()
        .map(|column| column.take(indices))
        .collect();
    RecordBatch::try_new(batch.schema.clone(), columns)
}

fn filter_batch_by_mask(
    batch: &RecordBatch,
    mask: &[bool],
    match_count: usize,
) -> Result<RecordBatch> {
    let columns = batch
        .columns
        .iter()
        .map(|col| col.filter_by_mask_fast(mask, match_count))
        .collect();
    RecordBatch::try_new(batch.schema.clone(), columns)
}

fn can_direct_materialize(
    schema: &Schema,
    provided_by_col: &[Option<usize>],
    is_missing_auto_inc: &[bool],
    values: &[Vec<Expr>],
) -> bool {
    if values.iter().flatten().any(is_default_value_expr) {
        return false;
    }
    // PK / NOT NULL columns take this path too: `push_literal_fast` rejects
    // NULL literals for them, which is the only extra validation the slow
    // materialize path performed here.
    schema.columns.iter().enumerate().all(|(idx, col)| {
        provided_by_col[idx].is_some() && !is_missing_auto_inc[idx] && col.default_value.is_none()
    })
}

fn is_default_value_expr(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Function { name, args, .. }
            if name == "__TAIBAI_DEFAULT__" && args.is_empty()
    )
}

/// Same NULL rule the general materialize path enforces via `validate_nullable`:
/// a PK or NOT NULL column rejects an explicit NULL.
fn validate_literal_nullness(
    column_name: &str,
    primary_key: bool,
    nullable: bool,
    scalar: &ScalarValue,
) -> Result<()> {
    if (primary_key || !nullable) && scalar.is_null() {
        return Err(TaibaiError::Sql(format!(
            "column '{}' cannot be NULL",
            column_name
        )));
    }
    Ok(())
}

fn push_literal_fast(
    col: &mut ColumnArray,
    expr: &Expr,
    scale: i32,
    timezone_offset_hours: i32,
    column_name: &str,
    primary_key: bool,
    nullable: bool,
) -> Result<()> {
    let Expr::Literal(value) = expr else {
        let scalar = eval_expr_to_scalar_with_scale_timezone(
            expr,
            &col.data_type(),
            scale,
            timezone_offset_hours,
        )?;
        validate_literal_nullness(column_name, primary_key, nullable, &scalar)?;
        return col.push(&scalar);
    };

    if value.is_null() {
        if primary_key || !nullable {
            return Err(TaibaiError::Sql(format!(
                "column '{}' cannot be NULL",
                column_name
            )));
        }
        return col.push(&ScalarValue::null_for_type(col.data_type()));
    }

    match (col, value) {
        (ColumnArray::Int32 { data, bitmap }, ScalarValue::Int64(Some(v))) => {
            data.push(
                i32::try_from(*v)
                    .map_err(|_| TaibaiError::Sql(format!("value '{}' overflows INT32", v)))?,
            );
            bitmap.push_valid();
            Ok(())
        }
        (ColumnArray::Int64 { data, bitmap }, ScalarValue::Int64(Some(v))) => {
            data.push(*v);
            bitmap.push_valid();
            Ok(())
        }
        (ColumnArray::Timestamp { data, bitmap }, ScalarValue::Int64(Some(v))) => {
            data.push(*v);
            bitmap.push_valid();
            Ok(())
        }
        (ColumnArray::Decimal { data, bitmap }, ScalarValue::Float64(Some(v))) => {
            // Exact-decimal conversion via the shortest round-trip text —
            // see float_literal_to_decimal_i64; the f64 multiply below is
            // only the scientific-notation fallback.
            let scaled = crate::sql::execution::helpers::float_literal_to_decimal_i64(*v, scale)
                .unwrap_or_else(|| {
                    let multiplier =
                        crate::sql::execution::helpers::decimal_multiplier(scale).unwrap_or(1);
                    (*v * multiplier as f64).round() as i64
                });
            data.push(scaled);
            bitmap.push_valid();
            Ok(())
        }
        (ColumnArray::Decimal { data, bitmap }, ScalarValue::Int64(Some(v))) => {
            let multiplier = crate::sql::execution::helpers::decimal_multiplier(scale)?;
            data.push(v.checked_mul(multiplier).ok_or_else(|| {
                TaibaiError::Sql(format!("decimal value '{}' overflows scale {}", v, scale))
            })?);
            bitmap.push_valid();
            Ok(())
        }
        (ColumnArray::VarChar { data, bitmap }, ScalarValue::VarChar(Some(v))) => {
            // Copy the bytes straight into the buffer; cloning into a fresh
            // String would add a heap allocation per row.
            data.push_str(v);
            bitmap.push_valid();
            Ok(())
        }
        (col, value) if col.data_type() == value.data_type() => col.push(value),
        (col, value) => col.push(&eval_expr_to_scalar_with_scale_timezone(
            &Expr::Literal(value.clone()),
            &col.data_type(),
            scale,
            timezone_offset_hours,
        )?),
    }
}

fn resolve_insert_columns(
    schema: &Schema,
    columns: &[String],
    values: &[Vec<Expr>],
) -> Result<Vec<usize>> {
    if columns.is_empty() {
        if values.iter().all(|row| row.is_empty()) {
            return Ok(Vec::new());
        }
        for row in values {
            if row.len() != schema.columns.len() {
                return Err(TaibaiError::Sql(format!(
                    "insert has {} values but table has {} columns",
                    row.len(),
                    schema.columns.len()
                )));
            }
        }
        return Ok((0..schema.columns.len()).collect());
    }

    let mut seen = HashSet::new();
    let mut indices = Vec::with_capacity(columns.len());
    for col_name in columns {
        let idx = schema
            .index_of(col_name)
            .ok_or_else(|| TaibaiError::Sql(format!("column '{}' not found", col_name)))?;
        if !seen.insert(idx) {
            return Err(TaibaiError::Sql(format!(
                "column '{}' specified more than once",
                col_name
            )));
        }
        indices.push(idx);
    }

    for row in values {
        if row.len() != columns.len() {
            return Err(TaibaiError::Sql(format!(
                "insert has {} values but {} columns were specified",
                row.len(),
                columns.len()
            )));
        }
    }

    Ok(indices)
}

pub(super) fn materialize_default_value(
    default: &ColumnDefaultValue,
    schema: &Schema,
    col_idx: usize,
    timezone_offset_hours: i32,
) -> Result<ScalarValue> {
    match default {
        ColumnDefaultValue::CurrentTimestamp => {
            if schema.columns[col_idx].data_type != DataType::Timestamp {
                return Err(TaibaiError::Sql(format!(
                    "CURRENT_TIMESTAMP default requires TIMESTAMP column '{}'",
                    schema.columns[col_idx].name
                )));
            }
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_micros() as i64;
            Ok(ScalarValue::Timestamp(Some(now)))
        }
        ColumnDefaultValue::Literal(value) => {
            let expr = Expr::Literal(value.clone());
            eval_expr_to_scalar_with_scale_timezone(
                &expr,
                &schema.columns[col_idx].data_type,
                schema.columns[col_idx].scale,
                timezone_offset_hours,
            )
        }
    }
}

pub(super) fn validate_nullable(
    schema: &Schema,
    col_idx: usize,
    scalar: &ScalarValue,
) -> Result<()> {
    let col = &schema.columns[col_idx];
    if (col.primary_key || !col.nullable) && scalar.is_null() {
        return Err(TaibaiError::Sql(format!(
            "column '{}' cannot be NULL",
            col.name
        )));
    }
    Ok(())
}
