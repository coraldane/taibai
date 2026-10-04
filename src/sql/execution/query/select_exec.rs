//! The SELECT/UNION execution pipeline: streaming entry points, the
//! materialized table-scan path, and plan-level set-operation folding.

use std::collections::HashMap;

use crate::common::error::{Result, TaibaiError};
use crate::protocol::types::DataType;
use crate::storage::segment::ScanStats;
use crate::types::batch::{ColumnDef, RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

use crate::sql::execution::engine::{Engine, ExecResult};
use crate::sql::execution::operators::profile::{QueryProfile, ScanProfileSeed};
use crate::sql::execution::physical::{
    build_physical_pipeline, build_profiled_materialized_pipeline, can_use_physical_pipeline,
    drain_pipeline, PhysicalContext,
};
use crate::sql::parser::ast::{
    Expr, FromClause, SelectItem, Statement,
};
use crate::sql::planner::{LogicalExpr, LogicalPlan};

use crate::sql::execution::scan::spec::compile_scan_spec;
use crate::sql::execution::scan::ScanExecutor;

use super::correlated::*;
use super::subqueries::*;
use super::*;

pub fn exec_select_streaming<F>(
    engine: &Engine,
    db: &str,
    stmt: &Statement,
    mut emit: F,
) -> Result<Option<Schema>>
where
    F: FnMut(RecordBatch) -> Result<()>,
{
    let Statement::Select {
        from: Some(from_clause),
        ..
    } = stmt
    else {
        return Ok(None);
    };
    // Queries with subqueries or derived tables go through the materialized
    // execution path, which owns subquery resolution and the correlated
    // runtime.
    if stmt_has_subquery_or_derived(stmt) {
        return Ok(None);
    }
    let resolved_stmt = resolve_in_subqueries(engine, db, stmt)?;
    let resolved_from = match &resolved_stmt {
        Statement::Select {
            from: Some(from), ..
        } => from,
        _ => from_clause,
    };
    let table_refs = extract_table_refs(resolved_from, db);
    let mut table_schemas = HashMap::new();
    let mut table_metas = HashMap::new();
    {
        let catalogs = engine.catalogs().read().unwrap();
        for table_ref in &table_refs {
            let catalog = catalogs.get(&table_ref.database).ok_or_else(|| {
                TaibaiError::Sql(format!("database '{}' does not exist", table_ref.database))
            })?;
            let meta = catalog
                .get_table(&table_ref.table)
                .ok_or_else(|| {
                    TaibaiError::Sql(format!(
                        "table '{}.{}' does not exist",
                        table_ref.database, table_ref.table
                    ))
                })?
                .clone();
            table_schemas.insert(table_ref.key.clone(), Schema::new(meta.columns.clone()));
            table_metas.insert(table_ref.key.clone(), meta);
        }
    }
    let plan = crate::sql::optimizer::optimize(crate::sql::planner::plan_select(
        &resolved_stmt,
        &table_schemas,
    )?);
    if !can_use_physical_pipeline(&plan) {
        return Ok(None);
    }
    let mut physical_ctx = PhysicalContext::new(engine, &table_refs, &table_schemas, &table_metas);
    let mut pipeline = build_physical_pipeline(plan, &mut physical_ctx)?;
    let mut schema = None;
    loop {
        crate::common::query_cancel::check()?;
        let Some(batch) = pipeline.next_batch()? else {
            break;
        };
        if schema.is_none() {
            schema = Some(batch.schema.clone());
        }
        emit(batch)?;
    }
    Ok(Some(schema.unwrap_or_else(Schema::empty)))
}

pub fn exec_select(engine: &Engine, db: &str, stmt: &Statement) -> Result<ExecResult> {
    exec_select_with_txn(engine, db, stmt, 0)
}

pub fn exec_select_with_txn(
    engine: &Engine,
    db: &str,
    stmt: &Statement,
    reader_txn_id: u64,
) -> Result<ExecResult> {
    exec_select_inner(engine, db, stmt, None, None, reader_txn_id, None, None)
}

pub fn exec_union_with_txn(
    engine: &Engine,
    db: &str,
    stmt: &Statement,
    reader_txn_id: u64,
) -> Result<ExecResult> {
    let Statement::Union {
        left,
        right,
        op,
        all,
        order_by,
        limit,
        offset,
    } = stmt
    else {
        return Err(TaibaiError::Internal(
            "exec_union called with non-UNION".into(),
        ));
    };

    let (left_result, right_result) = (
        exec_set_operand(engine, db, left, reader_txn_id)?,
        exec_set_operand(engine, db, right, reader_txn_id)?,
    );
    let (
        ExecResult::Query {
            schema: left_schema,
            ..
        },
        ExecResult::Query {
            batches: right_batches,
            ..
        },
    ) = (&left_result, &right_result)
    else {
        return Err(TaibaiError::Sql(
            "UNION operands must be SELECT queries".into(),
        ));
    };
    let left_schema = left_schema.clone();
    let left_batches = match left_result {
        ExecResult::Query { batches, .. } => batches,
        _ => unreachable!(),
    };

    if left_schema.columns.len() != right_schema_col_count(&right_result) {
        return Err(TaibaiError::Sql(format!(
            "UNION operands must have the same column count ({} vs {})",
            left_schema.columns.len(),
            right_schema_col_count(&right_result)
        )));
    }

    let mut rows: Vec<Vec<ScalarValue>> = Vec::new();
    let mut set_op_bytes = 0usize;
    let set_op_limit = crate::common::query_memory::result_memory_limit_bytes();
    for batch in left_batches.iter().chain(right_batches.iter()) {
        if batch.num_columns() == 0 {
            continue;
        }
        // Cancel + memory accounting inside the row loop: a large UNION used
        // to be neither interruptible nor bounded during materialization.
        crate::common::query_cancel::check()?;
        set_op_bytes = set_op_bytes.saturating_add(batch.estimated_size_bytes());
        if set_op_bytes > set_op_limit {
            return Err(TaibaiError::ResourceBusy(format!(
                "set operation exceeded memory limit of {} MB; add filters or LIMIT",
                set_op_limit / (1024 * 1024)
            )));
        }
        for row_idx in 0..batch.num_rows() {
            rows.push(
                (0..batch.num_columns())
                    .map(|c| batch.scalar(row_idx, c).unwrap_or(ScalarValue::Int64(None)))
                    .collect(),
            );
        }
    }

    // Dedup/set keys use the join hash table's compact typed key: hashing the
    // row itself cloned every cell a second time into the HashSet (a third
    // copy for INTERSECT/EXCEPT), while the key holds one boxed enum per row.
    pub(super) fn row_key(row: &[ScalarValue]) -> Vec<crate::sql::execution::join_hashtable::ScalarValueHashable> {
        row.iter()
            .map(crate::sql::execution::join_hashtable::ScalarValueHashable::from_scalar)
            .collect()
    }

    match op {
        crate::sql::parser::ast::SetOp::Union => {
            if !*all {
                // Typed keys: the old to_string-joined key could collide when a
                // value contained the separator, silently dropping distinct rows.
                let mut seen = std::collections::HashSet::new();
                rows.retain(|row| seen.insert(row_key(row)));
            }
        }
        // INTERSECT / EXCEPT apply set semantics: both operands are
        // deduplicated and the result keeps first-appearance order of the
        // (deduplicated) left rows.
        crate::sql::parser::ast::SetOp::Intersect | crate::sql::parser::ast::SetOp::Except => {
            let mut right_keys = std::collections::HashSet::new();
            let right_row_start = left_rows_count(&left_batches);
            for row in rows.iter().skip(right_row_start) {
                right_keys.insert(row_key(row));
            }
            rows.truncate(right_row_start);
            let mut seen = std::collections::HashSet::new();
            let keep = matches!(op, crate::sql::parser::ast::SetOp::Intersect);
            rows.retain(|row| {
                seen.insert(row_key(row))
                    && if keep {
                        right_keys.contains(&row_key(row))
                    } else {
                        !right_keys.contains(&row_key(row))
                    }
            });
        }
    }

    if !order_by.is_empty() {
        let mut keys: Vec<(usize, bool)> = Vec::with_capacity(order_by.len());
        for item in order_by {
            let idx = match &item.expr {
                Expr::Column(name) => left_schema.index_of(name).ok_or_else(|| {
                    TaibaiError::Sql(format!("unknown ORDER BY column '{name}' in UNION"))
                })?,
                Expr::Literal(ScalarValue::Int64(Some(value))) => {
                    let idx = (*value).max(1) as usize - 1;
                    if idx >= left_schema.columns.len() {
                        return Err(TaibaiError::Sql(format!(
                            "ORDER BY position {value} is out of range in UNION"
                        )));
                    }
                    idx
                }
                other => {
                    return Err(TaibaiError::Sql(format!(
                        "unsupported UNION ORDER BY expression: {other:?}"
                    )));
                }
            };
            keys.push((idx, item.desc));
        }
        rows.sort_by(|a, b| {
            for (idx, desc) in &keys {
                let mut ord = union_scalar_cmp(&a[*idx], &b[*idx]);
                if *desc {
                    ord = ord.reverse();
                }
                if ord != std::cmp::Ordering::Equal {
                    return ord;
                }
            }
            std::cmp::Ordering::Equal
        });
    }

    let offset_val = offset
        .as_ref()
        .map(crate::sql::planner::types::eval_const_usize)
        .transpose()?
        .unwrap_or(0);
    if offset_val > 0 {
        if offset_val >= rows.len() {
            rows.clear();
        } else {
            rows = rows.split_off(offset_val);
        }
    }
    if let Some(limit_expr) = limit {
        let limit_val = crate::sql::planner::types::eval_const_usize(limit_expr)?;
        rows.truncate(limit_val);
    }

    let mut columns: Vec<crate::types::array::ColumnArray> =
        Vec::with_capacity(left_schema.columns.len());
    for (col_idx, def) in left_schema.columns.iter().enumerate() {
        let mut array = crate::types::array::ColumnArray::empty(def.data_type);
        for row in &rows {
            array.push(&row[col_idx])?;
        }
        columns.push(array);
    }
    let batch = RecordBatch::try_new(left_schema.clone(), columns)?;
    Ok(ExecResult::Query {
        schema: left_schema,
        batches: vec![batch],
    })
}

/// Execute one operand of a set operation. Operands may themselves be set
/// operations (e.g. `(a UNION b) EXCEPT c` folds left-associatively), so a
/// nested Union recurses instead of hitting the SELECT-only path.
pub(super) fn exec_set_operand(
    engine: &Engine,
    db: &str,
    stmt: &Statement,
    reader_txn_id: u64,
) -> Result<ExecResult> {
    match stmt {
        Statement::Union { .. } => exec_union_with_txn(engine, db, stmt, reader_txn_id),
        _ => exec_select_with_txn(engine, db, stmt, reader_txn_id),
    }
}

pub(super) fn right_schema_col_count(result: &ExecResult) -> usize {
    match result {
        ExecResult::Query { schema, .. } => schema.columns.len(),
        ExecResult::Ok { .. } => 0,
    }
}

/// Number of rows the left operand contributed to `rows` (they were appended
/// first, so this is also the index where the right operand's rows start).
pub(super) fn left_rows_count(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

/// Set-operation row ordering: numeric promotion first, then natural
/// same-type order, NULLs last, Display text as the total-order fallback.
/// Delegates to `ScalarValue::compare_ordered`.
pub(super) fn union_scalar_cmp(a: &ScalarValue, b: &ScalarValue) -> std::cmp::Ordering {
    a.compare_ordered(b, std::cmp::Ordering::Greater)
}

pub fn exec_select_with_plan_and_txn(
    engine: &Engine,
    db: &str,
    stmt: &Statement,
    cached_plan: LogicalPlan,
    reader_txn_id: u64,
) -> Result<ExecResult> {
    exec_select_inner(
        engine,
        db,
        stmt,
        None,
        Some(cached_plan),
        reader_txn_id,
        None,
        None,
    )
}

pub(crate) fn exec_select_with_profile(
    engine: &Engine,
    db: &str,
    stmt: &Statement,
    stats: &mut HashMap<String, ScanStats>,
    prepared_plan: LogicalPlan,
    profile: &QueryProfile,
    pre_resolved: Option<PreResolvedExec<'_>>,
) -> Result<ExecResult> {
    exec_select_inner(
        engine,
        db,
        stmt,
        Some(stats),
        Some(prepared_plan),
        0,
        Some(profile),
        pre_resolved,
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn exec_select_inner(
    engine: &Engine,
    db: &str,
    stmt: &Statement,
    stats: Option<&mut HashMap<String, ScanStats>>,
    cached_plan: Option<LogicalPlan>,
    reader_txn_id: u64,
    profile: Option<&QueryProfile>,
    pre_resolved: Option<PreResolvedExec<'_>>,
) -> Result<ExecResult> {
    let from = match stmt {
        Statement::Select { from, .. } => from,
        _ => {
            return Err(TaibaiError::Internal(
                "exec_select called with non-SELECT".into(),
            ))
        }
    };

    match from {
        None => {
            let resolved = resolve_in_subqueries(engine, db, stmt)?;
            exec_literal_select(&resolved)
        }
        Some(from_clause) => exec_table_select(
            engine,
            db,
            stmt,
            from_clause,
            stats,
            cached_plan,
            reader_txn_id,
            profile,
            pre_resolved,
        ),
    }
}

pub(super) fn exec_literal_select(stmt: &Statement) -> Result<ExecResult> {
    let select_list = match stmt {
        Statement::Select { select_list, .. } => select_list,
        _ => unreachable!(),
    };
    let mut values = Vec::with_capacity(select_list.len());
    let mut columns = Vec::with_capacity(select_list.len());
    for item in select_list {
        match item {
            SelectItem::Expr(expr, alias) => {
                let val = crate::sql::execution::helpers::eval_constant_expr(expr)?;
                let name = alias
                    .clone()
                    .unwrap_or_else(|| crate::sql::planner::expr::expr_to_name(expr));
                columns.push(ColumnDef::new(&name, val.data_type()));
                values.push(val);
            }
            SelectItem::Star => {
                columns.push(ColumnDef::new("1", DataType::Int64));
                values.push(ScalarValue::Int64(Some(1)));
            }
        }
    }
    let schema = Schema::new(columns);
    let mut batch = RecordBatch::new(schema.clone());
    for (col_idx, val) in values.iter().enumerate() {
        batch.column_mut(col_idx).unwrap().push(val)?;
    }
    Ok(ExecResult::Query {
        schema,
        batches: vec![batch],
    })
}

#[allow(clippy::too_many_arguments)]
pub(super) fn exec_table_select(
    engine: &Engine,
    db: &str,
    stmt: &Statement,
    from_clause: &FromClause,
    stats: Option<&mut HashMap<String, ScanStats>>,
    cached_plan: Option<LogicalPlan>,
    reader_txn_id: u64,
    profile: Option<&QueryProfile>,
    pre_resolved: Option<PreResolvedExec<'_>>,
) -> Result<ExecResult> {
    let resolved;
    let resolved_stmt;
    let derived_tables;
    let mut decorrelated_batches: HashMap<String, Vec<std::sync::Arc<RecordBatch>>> =
        HashMap::new();
    let subquery_registry;
    let has_correlated;
    let (stmt, from_clause) = if let Some(pre) = pre_resolved {
        resolved_stmt = pre.resolved.stmt.clone();
        derived_tables = pre.resolved.derived.clone();
        subquery_registry = pre.resolved.registry.clone();
        has_correlated = pre.resolved.has_correlated;
        decorrelated_batches = pre.extra_batches.clone();
        let resolved_from = match &resolved_stmt {
            Statement::Select {
                from: Some(from), ..
            } => from,
            _ => from_clause,
        };
        (&resolved_stmt, resolved_from)
    } else if cached_plan.is_none() {
        resolved = resolve_query(engine, db, stmt)?;
        derived_tables = resolved.derived;
        subquery_registry = resolved.registry;
        has_correlated = resolved.has_correlated;
        resolved_stmt = resolved.stmt;
        let resolved_from = match &resolved_stmt {
            Statement::Select {
                from: Some(from), ..
            } => from,
            _ => from_clause,
        };
        (&resolved_stmt, resolved_from)
    } else {
        derived_tables = HashMap::new();
        subquery_registry = crate::sql::execution::subquery_runtime::new_registry();
        has_correlated = false;
        (stmt, from_clause)
    };
    let has_derived = !derived_tables.is_empty();

    let table_refs = extract_table_refs(from_clause, db);
    let table_names: Vec<String> = table_refs.iter().map(|t| t.key.clone()).collect();

    let mut table_schemas = HashMap::new();
    let mut table_cluster_keys: HashMap<String, Vec<String>> = HashMap::new();
    let mut table_metas: HashMap<String, crate::storage::catalog::schema::TableMeta> =
        HashMap::new();
    {
        let catalogs = engine.catalogs().read().unwrap();

        for table_ref in &table_refs {
            if table_ref.database == DERIVED_DB {
                let (schema, _) = derived_tables.get(&table_ref.key).ok_or_else(|| {
                    TaibaiError::Internal(format!(
                        "missing materialized derived table '{}'",
                        table_ref.key
                    ))
                })?;
                table_schemas.insert(table_ref.key.clone(), schema.clone());
                continue;
            }
            let catalog = catalogs.get(&table_ref.database).ok_or_else(|| {
                TaibaiError::Sql(format!("database '{}' does not exist", table_ref.database))
            })?;
            let meta = catalog
                .get_table(&table_ref.table)
                .ok_or_else(|| {
                    TaibaiError::Sql(format!(
                        "table '{}.{}' does not exist",
                        table_ref.database, table_ref.table
                    ))
                })?
                .clone();
            table_schemas.insert(table_ref.key.clone(), Schema::new(meta.columns.clone()));
            table_metas.insert(table_ref.key.clone(), meta.clone());
            if !meta.cluster_key.is_empty() {
                table_cluster_keys.insert(table_ref.key.clone(), meta.cluster_key.clone());
            }
        }
    }

    let plan = match cached_plan {
        Some(p) => p,
        None => {
            let plan = crate::sql::planner::plan_select(stmt, &table_schemas)?;
            let plan = crate::sql::optimizer::optimize(plan);
            // Rewrite correlated markers into semi/anti joins where the
            // shape allows; the rest keep evaluating per-row.
            let outcome = crate::sql::execution::decorrelate::decorrelate(plan, &subquery_registry);
            decorrelated_batches = outcome.extra_batches;
            outcome.plan
        }
    };

    // Aggregate fast paths (metadata folding for `Aggregate(single Scan)`
    // shapes, including plain COUNT(*)) are decided inside the two execution
    // backends below: the physical pipeline's Aggregate branch compiles a
    // FoldedAggregateExec, and the materialized path extracts the same fold
    // request from the plan into the scan spec. No AST-level detection here.
    if reader_txn_id == 0
        && stats.is_none()
        && !has_derived
        && !has_correlated
        && can_use_physical_pipeline(&plan)
    {
        let mut physical_ctx =
            PhysicalContext::new(engine, &table_refs, &table_schemas, &table_metas);
        let pipeline = build_physical_pipeline(plan, &mut physical_ctx)?;
        tracing::debug!("query: physical pipeline built, starting next_batch loop");
        let (schema, batches) = drain_pipeline(pipeline)?;
        tracing::debug!(
            "query: physical pipeline finished, {} batches",
            batches.len()
        );
        return Ok(ExecResult::Query { schema, batches });
    }

    exec_table_select_materialized(
        engine,
        &table_refs,
        table_names,
        table_schemas,
        table_cluster_keys,
        table_metas,
        plan,
        stats,
        reader_txn_id,
        profile,
        derived_tables,
        decorrelated_batches,
        has_correlated.then_some(&subquery_registry),
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn exec_table_select_materialized(
    engine: &Engine,
    table_refs: &[ResolvedTableRef],
    table_names: Vec<String>,
    table_schemas: HashMap<String, Schema>,
    table_cluster_keys: HashMap<String, Vec<String>>,
    table_metas: HashMap<String, crate::storage::catalog::schema::TableMeta>,
    plan: LogicalPlan,
    mut stats: Option<&mut HashMap<String, ScanStats>>,
    reader_txn_id: u64,
    profile: Option<&QueryProfile>,
    derived_batches: HashMap<String, (Schema, Vec<RecordBatch>)>,
    decorrelated_batches: HashMap<String, Vec<std::sync::Arc<RecordBatch>>>,
    subqueries: Option<&crate::sql::execution::subquery_runtime::SubqueryRegistry>,
) -> Result<ExecResult> {
    let mut table_batches: HashMap<String, Vec<std::sync::Arc<RecordBatch>>> = HashMap::new();
    let mut fully_applied_scan_filters: HashMap<String, bool> = HashMap::new();
    let mut scan_seeds: HashMap<String, ScanProfileSeed> = HashMap::new();
    // Cumulative scanned input held in `table_batches`, enforced against the
    // per-query memory budget alongside the result-side accounting.
    let mut input_memory_bytes = 0usize;

    for table_ref in table_refs {
        if table_ref.database == DERIVED_DB {
            continue;
        }
        let table_name = &table_ref.key;
        let schema = table_schemas.get(table_name).unwrap().clone();
        let meta = table_metas.get(table_name).unwrap().clone();

        let mut spec = compile_scan_spec(&plan, table_name, &schema, &meta);
        spec.table_name = table_ref.table.clone();
        spec.reader_txn_id = reader_txn_id;
        // Folded-aggregate fast path: the same plan-driven extraction the
        // physical pipeline's Aggregate branch performs. The engine carries
        // its own decline conditions (TTL, txn overlays, drift), so this
        // routes committed reads, explicit transactions and EXPLAIN ANALYZE
        // through one code path.
        spec.metadata_fold =
            crate::sql::execution::scan::spec::extract_metadata_fold(&plan, table_name);

        let mut executor = ScanExecutor::new(&spec, engine, schema, meta, &table_ref.database);
        if let Some(ref mut s) = stats {
            executor = executor.with_stats(s);
        }

        let scan_start = std::time::Instant::now();
        crate::common::query_cancel::check()?;
        let batches = executor.execute()?;
        let scanned_bytes: usize = batches.iter().map(RecordBatch::estimated_size_bytes).sum();
        let metadata_answered = executor.metadata_answered();
        scan_seeds.insert(
            table_name.clone(),
            ScanProfileSeed {
                elapsed: scan_start.elapsed(),
                peak_memory_bytes: scanned_bytes,
            },
        );
        // Input-side memory accounting: table_batches keeps every scanned row
        // resident for the rest of the query (joins, subqueries, set ops), so
        // the scanned input must respect the same per-query budget the result
        // side enforces — the old path capped only the output and let a large
        // JOIN materialize unbounded input memory.
        let input_limit = crate::common::query_memory::result_memory_limit_bytes();
        input_memory_bytes = input_memory_bytes.saturating_add(scanned_bytes);
        if input_memory_bytes > input_limit {
            return Err(TaibaiError::ResourceBusy(format!(
                "query input exceeded memory limit of {} MB while materializing table '{}'; add filters or LIMIT",
                input_limit / (1024 * 1024),
                table_ref.table
            )));
        }
        fully_applied_scan_filters.insert(
            table_name.clone(),
            spec.has_predicates() && spec.remaining_predicate.is_none(),
        );

        if spec.metadata_fold.is_some() && metadata_answered {
            // The scan executor produced the final aggregate rows via the
            // metadata folding engine; do not feed them through the pipeline
            // again. When the engine declined (metadata_answered == false),
            // the batches are raw scanned rows and the pipeline below must
            // apply the aggregate operator as usual. The extraction's
            // identity-projection guard makes the returned column order the
            // pipeline's own.
            if let Some(profile) = profile {
                // Surface the folded answer in EXPLAIN ANALYZE as its own
                // operator (no scan ran, so without this the operator
                // metrics section would be empty and the profile would
                // misrepresent what executed).
                let operator = profile.register(0, format!("AggregateFold {}", table_ref.table), 1);
                operator.add_elapsed(scan_start.elapsed());
                for batch in &batches {
                    operator.observe_batch(batch);
                }
            }
            let result_schema = if let Some(b) = batches.first() {
                b.schema.clone()
            } else {
                Schema::new(vec![ColumnDef::new("count(*)", DataType::Int64)])
            };
            return Ok(ExecResult::Query {
                schema: result_schema,
                batches,
            });
        }

        table_batches.insert(
            table_name.clone(),
            batches.into_iter().map(std::sync::Arc::new).collect(),
        );
    }

    let plan = if profile.is_none()
        && table_names.len() == 1
        && fully_applied_scan_filters
            .get(&table_names[0])
            .copied()
            .unwrap_or(false)
    {
        remove_redundant_pushed_filter(plan, &table_names[0])
    } else {
        plan
    };

    let plan = if profile.is_some() {
        plan
    } else {
        try_skip_sort_for_cluster_key(plan, &table_batches, &table_cluster_keys, &table_schemas)
    };

    for (key, (_, batches)) in derived_batches {
        table_batches.insert(key, batches.into_iter().map(std::sync::Arc::new).collect());
    }
    for (key, batches) in decorrelated_batches {
        table_batches.entry(key).or_insert(batches);
    }
    let mut pipeline = if let Some(profile) = profile {
        let mut physical_ctx =
            PhysicalContext::new(engine, table_refs, &table_schemas, &table_metas)
                .with_subqueries(subqueries.cloned());
        build_profiled_materialized_pipeline(
            plan,
            &mut physical_ctx,
            &table_batches,
            &scan_seeds,
            profile,
        )?
    } else {
        crate::sql::execution::operators::build_pipeline_with_subqueries(plan, &table_batches, subqueries)?
    };
    tracing::debug!("query: materialized pipeline built, starting next_batch loop");
    let mut result_batches = Vec::new();
    let mut retained_bytes = 0;
    let result_limit = crate::common::query_memory::result_memory_limit_bytes();
    let mut result_schema = None;
    loop {
        crate::common::query_cancel::check()?;
        let Some(batch) = pipeline.next_batch()? else {
            break;
        };
        tracing::debug!(
            "query: got batch with {} rows, schema columns={:?}",
            batch.num_rows(),
            batch
                .schema
                .columns
                .iter()
                .map(|c| (c.name.clone(), c.data_type))
                .collect::<Vec<_>>()
        );
        if result_schema.is_none() {
            result_schema = Some(batch.schema.clone());
        }
        crate::common::query_memory::push_result_batch(
            &mut result_batches,
            &mut retained_bytes,
            batch,
            result_limit,
        )?;
    }
    tracing::debug!(
        "query: finished next_batch loop, {} batches",
        result_batches.len()
    );

    let schema = result_schema.unwrap_or_else(Schema::empty);
    Ok(ExecResult::Query {
        schema,
        batches: result_batches,
    })
}

pub(super) fn remove_redundant_pushed_filter(plan: LogicalPlan, table_name: &str) -> LogicalPlan {
    match plan {
        LogicalPlan::Filter(filter) => {
            let input = remove_redundant_pushed_filter(*filter.input, table_name);
            match input {
                LogicalPlan::Scan(scan)
                    if scan.table_name.eq_ignore_ascii_case(table_name)
                        && scan.pushed_filter.is_some() =>
                {
                    LogicalPlan::Scan(scan)
                }
                other => LogicalPlan::Filter(crate::sql::planner::FilterPlan {
                    predicate: filter.predicate,
                    input: Box::new(other),
                }),
            }
        }
        LogicalPlan::Project(mut project) => {
            project.input = Box::new(remove_redundant_pushed_filter(*project.input, table_name));
            LogicalPlan::Project(project)
        }
        LogicalPlan::Sort(mut sort) => {
            sort.input = Box::new(remove_redundant_pushed_filter(*sort.input, table_name));
            LogicalPlan::Sort(sort)
        }
        LogicalPlan::Limit(mut limit) => {
            limit.input = Box::new(remove_redundant_pushed_filter(*limit.input, table_name));
            LogicalPlan::Limit(limit)
        }
        LogicalPlan::TopN(mut topn) => {
            topn.input = Box::new(remove_redundant_pushed_filter(*topn.input, table_name));
            LogicalPlan::TopN(topn)
        }
        LogicalPlan::Distinct(mut distinct) => {
            distinct.input = Box::new(remove_redundant_pushed_filter(*distinct.input, table_name));
            LogicalPlan::Distinct(distinct)
        }
        LogicalPlan::Window(mut window) => {
            window.input = Box::new(remove_redundant_pushed_filter(*window.input, table_name));
            LogicalPlan::Window(window)
        }
        other => other,
    }
}

pub(super) fn try_skip_sort_for_cluster_key(
    plan: LogicalPlan,
    table_batches: &HashMap<String, Vec<std::sync::Arc<RecordBatch>>>,
    table_cluster_keys: &HashMap<String, Vec<String>>,
    table_schemas: &HashMap<String, Schema>,
) -> LogicalPlan {
    let LogicalPlan::Sort(sort) = &plan else {
        return plan;
    };
    if table_batches.len() != 1 {
        return plan;
    }
    let (table_name, batches) = table_batches.iter().next().unwrap();
    if batches.len() != 1 {
        return plan;
    }
    let cluster_key = match table_cluster_keys.get(table_name) {
        Some(k) => k,
        None => return plan,
    };
    if cluster_key.is_empty() {
        return plan;
    }
    let schema = match table_schemas.get(table_name) {
        Some(s) => s,
        None => return plan,
    };
    let all_asc = sort.sort_keys.iter().all(|sk| !sk.desc);
    if !all_asc {
        return plan;
    }
    let matches = sort.sort_keys.len() == cluster_key.len()
        && sort
            .sort_keys
            .iter()
            .zip(cluster_key.iter())
            .all(|(sk, ck)| match &sk.expr {
                LogicalExpr::Column(idx) => schema
                    .columns
                    .get(*idx)
                    .map(|c| c.name == *ck)
                    .unwrap_or(false),
                _ => false,
            });
    if matches {
        *sort.input.clone()
    } else {
        plan
    }
}

pub(super) fn exec_query_materialized(
    engine: &Engine,
    db: &str,
    stmt: &Statement,
) -> Result<(Schema, Vec<RecordBatch>)> {
    let result = match stmt {
        Statement::Union { .. } => exec_union_with_txn(engine, db, stmt, 0)?,
        _ => exec_select(engine, db, stmt)?,
    };
    match result {
        ExecResult::Query { schema, batches } => Ok((schema, batches)),
        _ => Err(TaibaiError::Sql("derived table must be a query".into())),
    }
}
