//! DELETE and UPDATE inside an explicit transaction.
//!
//! Statement time only *plans*: matched segment rows are recorded as
//! tombstone row ids and matched rows of committed memtables as batch
//! positions in the [`TxnMutationRegistry`]; UPDATE additionally appends the
//! new row images to the transaction's own memtable (visible to this
//! transaction only). ROLLBACK drops the registry entry — no durable state
//! was touched, which also makes a crash equivalent to ROLLBACK. COMMIT
//! materializes everything under the table mutation locks right before the
//! memtable visibility flip, reusing the same journaled footer rewrites as
//! auto-commit DML.
//!
//! Scope: tables with primary keys, unique or secondary indexes, HOT
//! acceleration or TTL are rejected — index tombstones and hot-mirror
//! accounting have no deferred path yet.

use std::collections::BTreeSet;
use std::sync::Arc;

use crate::common::error::{Result, TaibaiError};
use crate::storage::catalog::schema::TableMeta;
use crate::storage::core::memtable::MemTable;
use crate::storage::core::txn_mutations::{filter_batch_excluding_positions, TxnMutations};
use crate::storage::segment::delete_bitmap;
use crate::types::batch::{RecordBatch, Schema};

use super::dml_delete::{
    apply_replace_memtable_conflicts, delete_partition_eq_key, plan_delete_memtable_rows,
    prune_segments_by_cluster_key, prune_segments_by_partition,
};
use super::engine::{Engine, ExecResult};
use super::helpers::{build_row_group_predicate, collect_expr_column_indices};
use crate::sql::parser::ast::{Assignment, Expr};

/// Reject tables whose indexes or acceleration paths have no deferred-DML
/// story yet (see module docs).
pub(crate) fn ensure_txn_dml_supported(table_meta: &TableMeta) -> Result<()> {
    let reject = |what: &str| -> Result<()> {
        Err(TaibaiError::Sql(format!(
            "DML on table '{}.{}' inside an explicit transaction is not supported \
             ({}); commit or roll back first",
            table_meta.database, table_meta.name, what
        )))
    };
    if table_meta.columns.iter().any(|column| column.primary_key) {
        return reject("PRIMARY KEY");
    }
    if !table_meta.unique_indexes.is_empty() {
        return reject("UNIQUE index");
    }
    if !table_meta.secondary_indexes.is_empty() {
        return reject("secondary index");
    }
    if table_meta.hot_time_column.is_some() {
        return reject("HOT table");
    }
    if table_meta.ttl.is_some() {
        return reject("TTL table");
    }
    Ok(())
}

/// Write-write conflict gate for auto-commit DML: an open transaction's
/// deferred mutations pin memtable row positions and segment row ids.
/// Auto-commit DELETE/UPDATE physically remove memtable rows and rewrite
/// segment footers, so a transaction's COMMIT would delete/update the wrong
/// rows underneath the pin — refuse instead of racing it.
pub(crate) fn refuse_auto_commit_dml_while_pinned(
    engine: &Engine,
    db: &str,
    table: &str,
) -> Result<()> {
    if engine.txn_mutations().table_has_pins(db, table) {
        return Err(TaibaiError::ResourceBusy(format!(
            "table '{}.{}' is pinned by an open transaction; retry after it commits or rolls back",
            db, table
        )));
    }
    Ok(())
}

/// Write-write conflict gate for deferred DML: the first open transaction to
/// mutate a table pins it, and another transaction's DML on the same table
/// must wait until the pin is released. Called inside the mutation-lock
/// critical section so the check and the tombstone registration are atomic
/// against a competing transaction's own registration.
pub(crate) fn refuse_txn_dml_on_pinned_table(
    engine: &Engine,
    reader_txn_id: u64,
    db: &str,
    table: &str,
) -> Result<()> {
    if engine
        .txn_mutations()
        .table_pinned_by_other(reader_txn_id, db, table)
    {
        return Err(TaibaiError::ResourceBusy(format!(
            "table '{}.{}' is being modified by another open transaction; \
             retry after it commits or rolls back",
            db, table
        )));
    }
    Ok(())
}

/// Everything the four DML statement paths (auto-commit/transaction ×
/// DELETE/UPDATE) derive identically from the statement before touching any
/// data: the table metadata snapshot, the subquery-resolved WHERE clause,
/// the compiled row-group predicate, and the narrowed partition. Planning
/// phases consume this context; the materialization strategy (immediate for
/// auto-commit, deferred to COMMIT for transactions) is the caller's choice.
/// Constructed once per statement via [`DmlStatementContext::plan`], so the
/// next preamble step is added here — not re-copied into four paths.
pub(crate) struct DmlStatementContext {
    pub table_schema: Schema,
    pub table_meta: TableMeta,
    pub resolved_where: Option<Expr>,
    ///  when the WHERE clause compiles to no row-group predicate
    /// (statement matches everything).
    pub predicate: Option<crate::storage::segment::RowGroupPredicate>,
    /// Column indices the WHERE clause references (for projected reads).
    pub predicate_projection: Vec<usize>,
    /// The `partition = const` key the WHERE clause narrows to, if any.
    pub target_partition_key: Option<String>,
}

impl DmlStatementContext {
    pub(crate) fn plan(
        engine: &Engine,
        db: &str,
        table: &str,
        where_clause: &Option<Expr>,
    ) -> Result<Self> {
        let (table_schema, table_meta) = load_table(engine, db, table)?;
        // Subquery table permissions are enforced statement-wide by the
        // engine's permission gate before dispatch; resolution here turns
        // the subqueries into literals so the predicate evaluator (whose
        // legacy fallback treated them as "match every row") never sees one.
        let resolved_where = super::query::resolve_where_in_subqueries(engine, db, where_clause)?;
        let predicate = build_row_group_predicate(&resolved_where, &table_schema);
        let predicate_projection = resolved_where
            .as_ref()
            .map(|expr| collect_expr_column_indices(expr, &table_schema))
            .unwrap_or_default();
        let target_partition_key =
            delete_partition_eq_key(&resolved_where, &table_schema, &table_meta);
        Ok(Self {
            table_schema,
            table_meta,
            resolved_where,
            predicate,
            predicate_projection,
            target_partition_key,
        })
    }

    /// Mutation-lock scope derived from the narrowed partition: the single
    /// partition, or `None` for a table-wide lock.
    pub(crate) fn partition_scope(&self) -> Option<Vec<String>> {
        self.target_partition_key
            .as_deref()
            .filter(|key| !key.is_empty())
            .map(|key| vec![key.to_string()])
    }

    /// The narrowed partition as a `&str` filter argument (`None` when the
    /// statement spans the whole table).
    pub(crate) fn narrowed_partition(&self) -> Option<&str> {
        self.target_partition_key
            .as_deref()
            .filter(|key| !key.is_empty())
    }
}

fn run_under_mutation_locks<T>(
    engine: &Engine,
    db: &str,
    table: &str,
    partitions: &Option<Vec<String>>,
    body: impl FnOnce() -> Result<T>,
) -> Result<T> {
    let mgr = engine.segment_mgr();
    match partitions.as_deref() {
        Some([partition]) => {
            mgr.with_partition_set_mutation_lock(db, table, &[partition.as_str()], body)
        }
        Some(many) => {
            let owned: Vec<&str> = many.iter().map(String::as_str).collect();
            mgr.with_partition_set_mutation_lock(db, table, &owned, body)
        }
        None => mgr.with_table_mutation_lock(db, table, body),
    }
}

// ---------------------------------------------------------------------------
// DELETE
// ---------------------------------------------------------------------------

pub(crate) fn exec_delete_in_txn(
    engine: &Engine,
    db: &str,
    table: &str,
    where_clause: &Option<Expr>,
    reader_txn_id: u64,
) -> Result<ExecResult> {
    let ctx = DmlStatementContext::plan(engine, db, table, where_clause)?;
    ensure_txn_dml_supported(&ctx.table_meta)?;
    let partitions = ctx.partition_scope();
    let resolved_where_clause = &ctx.resolved_where;
    let table_schema = &ctx.table_schema;
    let table_meta = &ctx.table_meta;
    let target_partition_key = ctx
        .target_partition_key
        .as_deref()
        .filter(|key| !key.is_empty());
    let predicate = ctx.predicate.as_ref();

    let table_dir = engine.data_dir().join(db).join(table);
    let mgr = engine.segment_mgr();

    let delete_body = || -> Result<u64> {
        // Write-write conflict control: the check runs inside the mutation
        // lock — the same critical section the tombstone registration below
        // uses — so two transactions cannot interleave check and register.
        refuse_txn_dml_on_pinned_table(engine, reader_txn_id, db, table)?;
        let mut statement_deleted: u64 = 0;

        // Rows the transaction itself inserted live in transactional
        // memtables: swap them out of the batch right away (no durable
        // state, ROLLBACK-safe by construction).
        let memtable_plans = plan_delete_memtable_rows(
            engine,
            db,
            table,
            resolved_where_clause,
            table_schema,
            target_partition_key,
            reader_txn_id,
        )?;
        let (own_plans, committed_plans): (Vec<_>, Vec<_>) = memtable_plans
            .into_iter()
            .partition(|plan| plan.memtable.is_transactional());
        if !own_plans.is_empty() {
            let undo = apply_replace_memtable_conflicts(engine, db, table, own_plans)?;
            statement_deleted += undo.deleted_rows();
        }
        // Committed memtable rows are deferred: hide the positions from this
        // transaction's readers and remove them physically at COMMIT.
        for plan in committed_plans {
            statement_deleted += plan.removed_positions.len() as u64;
            engine.txn_mutations().register_memtable_tombstones(
                reader_txn_id,
                db,
                table,
                partitions.as_deref(),
                Arc::clone(&plan.memtable),
                plan.removed_positions.iter().copied(),
            )?;
            if engine.pubsub().has_subscribers() {
                match plan.original.take_rows(&plan.removed_positions) {
                    Ok(deleted) => {
                        engine.txn_mutations().defer_delete_event(
                            reader_txn_id,
                            db,
                            table,
                            deleted,
                        )?;
                    }
                    Err(error) => tracing::warn!(
                        db,
                        table,
                        error = %error,
                        "failed to build deferred delete-event batch"
                    ),
                }
            }
        }

        // Segment rows: plan tombstones only — the footer is untouched until
        // COMMIT. Seed from ids already recorded by earlier statements of
        // this transaction so `mark_deleted` counts only new hits.
        let mut segment_files = if table_dir.exists() {
            prune_segments_by_partition(
                crate::storage::segment_manager::SegmentManager::discover_segments(&table_dir)?,
                &resolved_where_clause,
                &table_schema,
                &table_meta,
            )
        } else {
            Vec::new()
        };
        if let Some(ref pred) = predicate {
            segment_files = prune_segments_by_cluster_key(
                segment_files,
                mgr,
                &table_schema,
                &table_meta,
                pred,
            )?;
        }

        let publish_deletes = engine.pubsub().has_subscribers();
        for seg_path in segment_files {
            let reader = mgr.open_reader(&seg_path)?;
            let total_rows = reader.row_count();
            if total_rows == 0 {
                continue;
            }
            if let Some(ref pred) = predicate {
                if !super::helpers::segment_may_match(&reader, &table_schema, Some(pred)) {
                    continue;
                }
            }
            let prior_ids = engine
                .txn_mutations()
                .segment_row_ids(reader_txn_id, &seg_path);
            // WHERE-less deletes publish no events here (nothing was read to
            // publish), so batches are only needed when a WHERE clause is
            // evaluated.
            let outcome = super::dml_delete::match_segment_for_delete(
                &reader,
                table_schema,
                predicate,
                resolved_where_clause.as_ref(),
                &prior_ids,
                resolved_where_clause.is_some(),
                |reader, rg_idx| reader.read_row_group_columns_aligned(rg_idx, table_schema),
                |batch, matched| {
                    if !publish_deletes {
                        return Ok(());
                    }
                    let deleted_in_rg: Vec<u32> = matched.iter().map(|(idx, _)| *idx).collect();
                    match batch.take_rows(&deleted_in_rg) {
                        Ok(deleted) => engine.txn_mutations().defer_delete_event(
                            reader_txn_id,
                            db,
                            table,
                            deleted,
                        )?,
                        Err(error) => tracing::warn!(
                            db,
                            table,
                            error = %error,
                            "failed to build deferred delete-event batch"
                        ),
                    }
                    Ok(())
                },
            )?;

            if outcome.matched_ids.is_empty() {
                continue;
            }
            statement_deleted += outcome.matched_ids.len() as u64;
            engine.txn_mutations().register_segment_tombstones(
                reader_txn_id,
                db,
                table,
                partitions.as_deref(),
                &seg_path,
                &outcome.footer_bitmap,
                outcome.matched_ids,
            )?;
        }

        Ok(statement_deleted)
    };

    let deleted = run_under_mutation_locks(engine, db, table, &partitions, delete_body)?;

    if deleted > 0 {
        engine
            .txn_mutations()
            .add_deleted_rows(reader_txn_id, db, table, deleted);
    }
    Ok(ExecResult::Ok {
        rows_affected: deleted,
    })
}

// ---------------------------------------------------------------------------
// UPDATE: tombstone the old row, append the new row image to the txn memtable
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
pub(crate) fn exec_update_in_txn(
    engine: &Engine,
    db: &str,
    table: &str,
    assignments: &[Assignment],
    where_clause: &Option<Expr>,
    reader_txn_id: u64,
) -> Result<ExecResult> {
    let ctx = DmlStatementContext::plan(engine, db, table, where_clause)?;
    ensure_txn_dml_supported(&ctx.table_meta)?;
    if ctx.table_meta.is_partitioned()
        && assignments.iter().any(|assignment| {
            assignment
                .column
                .eq_ignore_ascii_case(&ctx.table_meta.partition_key)
        })
    {
        return Err(TaibaiError::Sql(format!(
            "updating partition key '{}' is not supported; use DELETE followed by INSERT",
            ctx.table_meta.partition_key
        )));
    }

    let resolved_assignments: Vec<Assignment> = assignments
        .iter()
        .map(|assignment| {
            Ok(Assignment {
                column: assignment.column.clone(),
                value: super::query::resolve_expr_in_subqueries_clone(
                    engine,
                    db,
                    &assignment.value,
                )?,
            })
        })
        .collect::<Result<_>>()?;
    let assignments = resolved_assignments.as_slice();

    let resolved_where_clause = &ctx.resolved_where;
    let table_schema = &ctx.table_schema;
    let table_meta = &ctx.table_meta;
    let predicate = ctx.predicate.as_ref();
    let predicate_projection = &ctx.predicate_projection;
    let partitions = ctx.partition_scope();
    let narrowed_partition = ctx.narrowed_partition();

    let table_dir = engine.data_dir().join(db).join(table);
    let mgr = engine.segment_mgr();

    let update_body = || -> Result<u64> {
        // Write-write conflict control, same gate as delete_body.
        refuse_txn_dml_on_pinned_table(engine, reader_txn_id, db, table)?;
        let publish_updates = engine.pubsub().has_subscribers();
        let mut statement_updated: u64 = 0;
        // New row images per target partition, appended to the transaction's
        // own memtable after the planning pass.
        let mut new_rows: Vec<(String, RecordBatch)> = Vec::new();

        // Transactional memtables (this transaction's own INSERTs) update in
        // place — no durable state, ROLLBACK-safe. Committed memtables defer
        // via tombstone positions + new rows, exactly like segments. The
        // shared walk applies the transaction-tombstone view (same contract
        // as the DELETE planner), so a second UPDATE of the same row cannot
        // append a second copy of its new image at COMMIT.
        let txn_mutations = engine.txn_mutations();
        super::dml_delete::for_each_visible_memtable(
            engine,
            db,
            table,
            &table_schema,
            reader_txn_id,
            narrowed_partition,
            |memtable, _original, batch, live_positions| {
                let (new_batch, affected, matched_rows, _assigned_pairs) =
                    super::dml_update::update_memtable_batch(
                    assignments,
                    &resolved_where_clause,
                    &table_schema,
                    &batch,
                )?;
                let Some(new_batch) = new_batch else {
                    return Ok(());
                };
                statement_updated += affected;
                if memtable.is_transactional() {
                    if !memtable.replace_pending_batch(Arc::new(new_batch)) {
                        return Err(TaibaiError::Internal(
                            "transactional memtable left pending state during in-transaction UPDATE"
                                .to_string(),
                        ));
                    }
                    return Ok(());
                }
                // Map the filtered-batch positions back to absolute positions in
                // the committed memtable batch for the deferred removal.
                let absolute_rows: Vec<u32> = match &live_positions {
                    Some(map) => matched_rows.iter().map(|&pos| map[pos as usize]).collect(),
                    None => matched_rows.clone(),
                };
                txn_mutations.register_memtable_tombstones(
                    reader_txn_id,
                    db,
                    table,
                    partitions.as_deref(),
                    Arc::clone(&memtable),
                    absolute_rows.iter().copied(),
                )?;
                let updated_rows = new_batch.take_rows(&matched_rows)?;
                if publish_updates {
                    engine.txn_mutations().defer_update_event(
                        reader_txn_id,
                        db,
                        table,
                        updated_rows.clone(),
                    )?;
                }
                new_rows.push((memtable.partition_key().to_string(), updated_rows));
                Ok(())
            },
        )?;

        // Segment rows: tombstone ids + new row images; no rewrite, no seal,
        // so everything here is reversible until COMMIT.
        let assigned = super::dml_update::precompute_assigned_values(assignments, &table_schema)?;
        let mut segment_files = if table_dir.exists() {
            super::dml_delete::prune_segments_by_partition(
                crate::storage::segment_manager::SegmentManager::discover_segments(&table_dir)?,
                &resolved_where_clause,
                &table_schema,
                &table_meta,
            )
        } else {
            Vec::new()
        };
        if let Some(ref pred) = predicate {
            segment_files = super::dml_delete::prune_segments_by_cluster_key(
                segment_files,
                mgr,
                &table_schema,
                &table_meta,
                pred,
            )?;
        }

        for seg_path in segment_files {
            let reader = mgr.open_reader(&seg_path)?;
            if !super::helpers::segment_may_match(&reader, &table_schema, predicate) {
                continue;
            }
            let prior_ids = engine
                .txn_mutations()
                .segment_row_ids(reader_txn_id, &seg_path);
            let mut newly_deleted: Vec<u64> = Vec::new();
            let mut updated_images: Vec<RecordBatch> = Vec::new();

        // Footer zone maps follow the segment's own column order: re-anchor
        // the catalog-space predicate per segment. When a predicate column
        // is absent from this footer, alignment fails and row-group pruning
        // is disabled for the whole segment — the default-filled aligned
        // read below then decides row by row (the absent-column semantics
        // DELETE/SELECT/COUNT already use). The old code `break`ed out of
        // the loop here, silently skipping every remaining row group of the
        // segment and losing updates.
        let aligned_predicate = predicate
            .as_ref()
            .and_then(|pred| reader.align_row_group_predicate(&table_schema, pred).ok());

        for rg_idx in 0..reader.num_row_groups() {
            if let Some(ref pred) = aligned_predicate {
                if !pred.matches_row_group(reader.zone_maps(rg_idx)) {
                    continue;
                }
            }
                let mask = super::dml_update::build_update_mask(
                    &reader,
                    rg_idx,
                    &resolved_where_clause,
                    &predicate_projection,
                    &table_schema,
                )?;
                if !mask.iter().any(|m| *m) {
                    continue;
                }
                // Catalog-ordered read: `assigned` maps catalog indices.
                let batch = reader.read_row_group_columns_aligned(rg_idx, &table_schema)?;
                let start_row = reader.row_group_start_row(rg_idx);
                let footer_bitmap = reader.delete_bitmap();
                let mut matched: Vec<u32> = Vec::new();
                for (row_idx, matches) in mask.iter().enumerate() {
                    let absolute_row = start_row + row_idx as u64;
                    if !matches
                        || delete_bitmap::is_deleted(footer_bitmap, absolute_row)
                        || prior_ids.binary_search(&absolute_row).is_ok()
                    {
                        continue;
                    }
                    matched.push(row_idx as u32);
                    newly_deleted.push(absolute_row);
                }
                if matched.is_empty() {
                    continue;
                }
                // The updated image of exactly the matched rows.
                let kept_matched = vec![true; matched.len()];
                let image = super::dml_update::rebuild_update_batch(
                    &batch,
                    &assigned,
                    &matched,
                    &kept_matched,
                )?;
                updated_images.push(image);
            }

            if newly_deleted.is_empty() {
                continue;
            }
            let footer_bitmap = reader.delete_bitmap().to_vec();
            engine.txn_mutations().register_segment_tombstones(
                reader_txn_id,
                db,
                table,
                partitions.as_deref(),
                &seg_path,
                &footer_bitmap,
                newly_deleted,
            )?;
            let partition_key = seg_path
                .file_name()
                .and_then(|name| name.to_str())
                .and_then(|name| crate::storage::partition::parse_partition_key(name))
                .unwrap_or_default();
            for image in updated_images {
                statement_updated += image.num_rows() as u64;
                if publish_updates {
                    engine.txn_mutations().defer_update_event(
                        reader_txn_id,
                        db,
                        table,
                        image.clone(),
                    )?;
                }
                new_rows.push((partition_key.clone(), image));
            }
        }

        // Append the new row images to the transaction's own memtable.
        for (partition, batch) in new_rows {
            if batch.num_rows() == 0 {
                continue;
            }
            engine
                .batch_writer()
                .write_columns_with_txn_under_table_lock(
                    db,
                    table,
                    &partition,
                    batch,
                    table_schema.clone(),
                    reader_txn_id,
                    false,
                )?;
        }

        Ok(statement_updated)
    };

    let updated = run_under_mutation_locks(engine, db, table, &partitions, update_body)?;
    Ok(ExecResult::Ok {
        rows_affected: updated,
    })
}

// ---------------------------------------------------------------------------
// COMMIT / ROLLBACK
// ---------------------------------------------------------------------------

/// Materialize the transaction's deferred mutations: physically remove the
/// memtable rows, rewrite the segment footers with the union tombstones,
/// publish the deferred change events, fix the catalog counters. Runs under
/// the same table/partition mutation locks as auto-commit DML. Called by the
/// engine right before the memtable visibility flip, so concurrent readers
/// never observe old and new rows at the same time.
pub(crate) fn materialize_txn_mutations_at_commit(engine: &Engine, txn_id: u64) -> Result<()> {
    let Some(mutations) = engine.txn_mutations().take(txn_id) else {
        return Ok(());
    };
    if let Err(error) = materialize_tables(engine, &mutations) {
        // Keep the plans so the transaction can retry COMMIT or ROLLBACK;
        // the applied subset is idempotent (bitmap union, batch swap).
        engine.txn_mutations().put_back(txn_id, mutations);
        return Err(error);
    }
    Ok(())
}

fn materialize_tables(engine: &Engine, mutations: &TxnMutations) -> Result<()> {
    let mgr = engine.segment_mgr();
    for table_mutations in mutations.tables() {
        let db = table_mutations.db.as_str();
        let table = table_mutations.table.as_str();
        let materialize = || -> Result<()> {
            // 1. Committed memtable rows: swap the current batch for a
            //    filtered one and rewrite the durable tail, so crash
            //    recovery cannot resurrect removed rows.
            for entry in &table_mutations.memtables {
                materialize_memtable(engine, &entry.memtable, &entry.positions)?;
            }
            // 2. Segment tombstones: union the recorded ids into the fresh
            //    footer bitmap (the segment may have grown since planning)
            //    and rewrite via the journaled, idempotent path.
            for entry in &table_mutations.segments {
                let reader = mgr.open_reader(&entry.path)?;
                let total_rows = reader.row_count();
                let bitmap_bytes = total_rows.div_ceil(8) as usize;
                let mut bitmap = vec![0u8; bitmap_bytes];
                let footer_bitmap = reader.delete_bitmap();
                let copy_len = footer_bitmap.len().min(bitmap_bytes);
                bitmap[..copy_len].copy_from_slice(&footer_bitmap[..copy_len]);
                for &row_id in &entry.row_ids {
                    if (row_id as usize) < total_rows as usize {
                        delete_bitmap::mark_deleted(&mut bitmap, row_id);
                    }
                }
                let deleted_count: u64 = bitmap.iter().map(|byte| byte.count_ones() as u64).sum();
                drop(reader);
                mgr.rewrite_delete_footer(&entry.path, &bitmap, deleted_count)?;
            }
            // 3. Deferred change events, matching the auto-commit order:
            //    deletions first, then the post-update images.
            for batch in &table_mutations.deferred_delete_batches {
                engine.pubsub().publish_change(
                    crate::pubsub::ChangeKind::Delete,
                    db,
                    table,
                    batch.clone(),
                );
            }
            for batch in &table_mutations.deferred_update_batches {
                engine.pubsub().publish_change(
                    crate::pubsub::ChangeKind::Update,
                    db,
                    table,
                    batch.clone(),
                );
            }
            // 4. Counters and caches, as auto-commit DML would. Invalidate
            // the hot mirror for every mutated table, exactly like
            // auto-commit DELETE/UPDATE which invalidate unconditionally:
            // the mirror is append-only, so tombstoned rows (delete or
            // update) leave it stale. Deferred update images are not a
            // reliable trigger — they are only buffered when pubsub has
            // subscribers. The two paths share this invariant by code, not
            // by table eligibility — `ensure_txn_dml_supported` currently
            // rejects HOT tables, but relying on that gate here would
            // silently reintroduce stale point queries the day deferred DML
            // supports them.
            engine.invalidate_hot_store(db, table);
            if table_mutations.deleted_rows > 0 {
                let mut catalogs = engine.catalogs().write().unwrap();
                if let Some(catalog) = catalogs.get_mut(db) {
                    if let Some(meta) = catalog.get_table_mut(table) {
                        meta.row_count =
                            (meta.row_count - table_mutations.deleted_rows as i64).max(0);
                        let meta_dir = engine.data_dir().join("meta");
                        catalog.save(&meta_dir)?;
                    }
                }
            }
            Ok(())
        };

        match table_mutations.partitions.as_deref() {
            None => {
                mgr.with_table_mutation_lock(db, table, materialize)?;
            }
            Some(parts) => {
                let owned: Vec<&str> = parts.iter().map(String::as_str).collect();
                mgr.with_partition_set_mutation_lock(db, table, &owned, materialize)?;
            }
        }
    }
    Ok(())
}

fn materialize_memtable(
    engine: &Engine,
    memtable: &Arc<MemTable>,
    positions: &BTreeSet<u32>,
) -> Result<()> {
    let Some(snapshot) = memtable.read_snapshot_arc() else {
        return Err(TaibaiError::Internal(
            "deferred-DELETE memtable lost its batch before COMMIT".to_string(),
        ));
    };
    let filtered = filter_batch_excluding_positions(&snapshot, positions)?;
    // Durable tail BEFORE the in-memory swap: this runs under the table's
    // mutation lock, so a crash between the two steps restores the filtered
    // tail (deletes preserved) instead of resurrecting the deleted rows next
    // to a possibly already-rewritten tail.
    let table_dir = engine.data_dir().join(memtable.db()).join(memtable.table());
    crate::storage::core::durable_tail::persist(
        &table_dir,
        memtable.id(),
        memtable.partition_key(),
        &filtered,
    )?;
    if !memtable.replace_pending_batch(Arc::new(filtered.clone())) {
        return Err(TaibaiError::Internal(
            "deferred-DELETE memtable left pending state before COMMIT (a flush raced the transaction)"
                .to_string(),
        ));
    }
    Ok(())
}

/// Drop the transaction's deferred mutations without touching durable state
/// (ROLLBACK / crash semantics). Returns the deferred row count so the
/// rollback report can include rows whose deletion never happened.
pub(crate) fn discard_txn_mutations_at_rollback(engine: &Engine, txn_id: u64) -> u64 {
    engine.txn_mutations().remove(txn_id)
}

// ---------------------------------------------------------------------------
// shared helpers
// ---------------------------------------------------------------------------

fn load_table(engine: &Engine, db: &str, table: &str) -> Result<(Schema, TableMeta)> {
    let catalogs = engine.catalogs().read().unwrap();
    let catalog = catalogs
        .get(db)
        .ok_or_else(|| TaibaiError::Sql(format!("database '{}' does not exist", db)))?;
    let meta = catalog
        .get_table(table)
        .ok_or_else(|| TaibaiError::Sql(format!("table '{}.{}' does not exist", db, table)))?
        .clone();
    Ok((Schema::new(meta.columns.clone()), meta))
}
