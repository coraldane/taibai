use std::collections::HashMap;

use crate::common::error::{Result, TaibaiError};
use crate::protocol::types::DataType;
use crate::storage::segment::ScanStats;
use crate::types::batch::{ColumnDef, RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

use super::engine::{Engine, ExecResult};
use super::operators::profile::{QueryProfile, ScanProfileSeed};
use super::physical::{
    build_physical_pipeline, build_profiled_materialized_pipeline, can_use_physical_pipeline,
    drain_pipeline, PhysicalContext,
};
use crate::sql::parser::ast::{
    CorrelatedKind, CorrelatedSubquery, Expr, FromClause, ObjectName, SelectItem, Statement,
    TableRef,
};
use crate::sql::planner::{LogicalExpr, LogicalPlan};

use super::scan::spec::compile_scan_spec;
use super::scan::ScanExecutor;

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
    let table_names: Vec<String> = table_refs.iter().map(|table| table.key.clone()).collect();
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
    if detect_count_star(&resolved_stmt, &table_names)
        || detect_simple_metadata_aggregate(&resolved_stmt, &table_names)
        || !can_use_physical_pipeline(&plan)
    {
        return Ok(None);
    }
    let mut physical_ctx =
        PhysicalContext::new(engine, &table_refs, &table_schemas, &table_metas, false);
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
        exec_select_with_txn(engine, db, left, reader_txn_id)?,
        exec_select_with_txn(engine, db, right, reader_txn_id)?,
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
    for batch in left_batches.iter().chain(right_batches.iter()) {
        if batch.num_columns() == 0 {
            continue;
        }
        for row_idx in 0..batch.num_rows() {
            rows.push(
                (0..batch.num_columns())
                    .map(|c| batch.scalar(row_idx, c).unwrap_or(ScalarValue::Int64(None)))
                    .collect(),
            );
        }
    }

    if !*all {
        let mut seen = std::collections::HashSet::new();
        rows.retain(|row| {
            let key = row
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join("\u{1f}");
            seen.insert(key)
        });
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

fn right_schema_col_count(result: &ExecResult) -> usize {
    match result {
        ExecResult::Query { schema, .. } => schema.columns.len(),
        ExecResult::Ok { .. } => 0,
    }
}

fn union_scalar_cmp(a: &ScalarValue, b: &ScalarValue) -> std::cmp::Ordering {
    a.numeric_cmp(b)
        .or_else(|| match (a, b) {
            (ScalarValue::VarChar(Some(x)), ScalarValue::VarChar(Some(y))) => Some(x.cmp(y)),
            (ScalarValue::Date(Some(x)), ScalarValue::Date(Some(y))) => Some(x.cmp(y)),
            (ScalarValue::Timestamp(Some(x)), ScalarValue::Timestamp(Some(y))) => Some(x.cmp(y)),
            _ => None,
        })
        .unwrap_or_else(|| {
            let (null_a, null_b) = (a.is_null(), b.is_null());
            match (null_a, null_b) {
                (true, true) => std::cmp::Ordering::Equal,
                (true, false) => std::cmp::Ordering::Greater,
                (false, true) => std::cmp::Ordering::Less,
                (false, false) => a.to_string().cmp(&b.to_string()),
            }
        })
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
fn exec_select_inner(
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

fn exec_literal_select(stmt: &Statement) -> Result<ExecResult> {
    let select_list = match stmt {
        Statement::Select { select_list, .. } => select_list,
        _ => unreachable!(),
    };
    let mut values = Vec::with_capacity(select_list.len());
    let mut columns = Vec::with_capacity(select_list.len());
    for item in select_list {
        match item {
            SelectItem::Expr(expr, alias) => {
                let val = super::helpers::eval_constant_expr(expr)?;
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
fn exec_table_select(
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
    let mut decorrelated_batches: HashMap<String, Vec<RecordBatch>> = HashMap::new();
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
        subquery_registry = super::subquery_runtime::new_registry();
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
            let outcome = super::decorrelate::decorrelate(plan, &subquery_registry);
            decorrelated_batches = outcome.extra_batches;
            outcome.plan
        }
    };

    let is_count_star = !has_derived && !has_correlated && detect_count_star(stmt, &table_names);
    // Simple metadata-aggregate fast path: COUNT/MIN/MAX without GROUP BY.
    // Only detected on a single-table, committed-read query with no stats sink.
    let simple_metadata_aggregates = if reader_txn_id == 0
        && stats.is_none()
        && !is_count_star
        && !has_derived
        && !has_correlated
        && table_names.len() == 1
        && detect_simple_metadata_aggregate(stmt, &table_names)
    {
        extract_simple_metadata_aggregates(
            &plan,
            table_schemas.get(&table_names[0]).unwrap_or(&crate::types::batch::Schema::empty()),
        )
    } else {
        None
    };
    let is_simple_metadata = simple_metadata_aggregates.is_some();

    if reader_txn_id == 0
        && stats.is_none()
        && !is_count_star
        && !is_simple_metadata
        && !has_derived
        && !has_correlated
        && can_use_physical_pipeline(&plan)
    {
        let mut physical_ctx = PhysicalContext::new(
            engine,
            &table_refs,
            &table_schemas,
            &table_metas,
            is_count_star,
        );
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
        is_count_star,
        simple_metadata_aggregates,
        stats,
        reader_txn_id,
        profile,
        derived_tables,
        decorrelated_batches,
        has_correlated.then_some(&subquery_registry),
    )
}

#[allow(clippy::too_many_arguments)]
fn exec_table_select_materialized(
    engine: &Engine,
    table_refs: &[ResolvedTableRef],
    table_names: Vec<String>,
    table_schemas: HashMap<String, Schema>,
    table_cluster_keys: HashMap<String, Vec<String>>,
    table_metas: HashMap<String, crate::storage::catalog::schema::TableMeta>,
    plan: LogicalPlan,
    is_count_star: bool,
    simple_metadata_aggregates: Option<Vec<crate::sql::planner::AggregateExpr>>,
    mut stats: Option<&mut HashMap<String, ScanStats>>,
    reader_txn_id: u64,
    profile: Option<&QueryProfile>,
    derived_batches: HashMap<String, (Schema, Vec<RecordBatch>)>,
    decorrelated_batches: HashMap<String, Vec<RecordBatch>>,
    subqueries: Option<&super::subquery_runtime::SubqueryRegistry>,
) -> Result<ExecResult> {
    let mut table_batches: HashMap<String, Vec<RecordBatch>> = HashMap::new();
    let mut fully_applied_scan_filters: HashMap<String, bool> = HashMap::new();
    let mut scan_seeds: HashMap<String, ScanProfileSeed> = HashMap::new();

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

        if is_count_star && profile.is_none() {
            spec.is_count_star = true;
        }
        if simple_metadata_aggregates.is_some() {
            spec.simple_metadata_aggregates = simple_metadata_aggregates.clone();
        }

        let mut executor = ScanExecutor::new(&spec, engine, schema, meta, &table_ref.database);
        if let Some(ref mut s) = stats {
            executor = executor.with_stats(s);
        }

        let scan_start = std::time::Instant::now();
        let batches = executor.execute()?;
        scan_seeds.insert(
            table_name.clone(),
            ScanProfileSeed {
                elapsed: scan_start.elapsed(),
                peak_memory_bytes: batches.iter().map(RecordBatch::estimated_size_bytes).sum(),
            },
        );
        fully_applied_scan_filters.insert(
            table_name.clone(),
            spec.has_predicates() && spec.remaining_predicate.is_none(),
        );

        if spec.is_count_star {
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
        if spec.simple_metadata_aggregates.is_some() {
            // The scan executor already produced the single aggregate result row
            // via the metadata fast path; do not feed it through the pipeline.
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

        table_batches.insert(table_name.clone(), batches);
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
        table_batches.insert(key, batches);
    }
    for (key, batches) in decorrelated_batches {
        table_batches.entry(key).or_insert(batches);
    }
    let mut pipeline = if let Some(profile) = profile {
        let mut physical_ctx =
            PhysicalContext::new(engine, table_refs, &table_schemas, &table_metas, false)
                .with_subqueries(subqueries.cloned());
        build_profiled_materialized_pipeline(
            plan,
            &mut physical_ctx,
            &table_batches,
            &scan_seeds,
            profile,
        )?
    } else {
        super::operators::build_pipeline_with_subqueries(plan, &table_batches, subqueries)?
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

pub(crate) fn resolve_in_subqueries(
    engine: &Engine,
    db: &str,
    stmt: &Statement,
) -> Result<Statement> {
    let mut stmt = stmt.clone();
    resolve_stmt_in_subqueries(engine, db, &mut stmt)?;
    Ok(stmt)
}

fn resolve_stmt_in_subqueries(engine: &Engine, db: &str, stmt: &mut Statement) -> Result<()> {
    match stmt {
        Statement::Select {
            select_list,
            where_clause,
            group_by,
            having,
            order_by,
            limit,
            offset,
            ..
        } => {
            for item in select_list {
                if let SelectItem::Expr(expr, _) = item {
                    resolve_expr_in_subqueries(engine, db, expr)?;
                }
            }
            if let Some(expr) = where_clause {
                resolve_expr_in_subqueries(engine, db, expr)?;
            }
            for expr in group_by {
                resolve_expr_in_subqueries(engine, db, expr)?;
            }
            if let Some(expr) = having {
                resolve_expr_in_subqueries(engine, db, expr)?;
            }
            for item in order_by {
                resolve_expr_in_subqueries(engine, db, &mut item.expr)?;
            }
            if let Some(expr) = limit {
                resolve_expr_in_subqueries(engine, db, expr)?;
            }
            if let Some(expr) = offset {
                resolve_expr_in_subqueries(engine, db, expr)?;
            }
        }
        Statement::Union {
            left,
            right,
            order_by,
            limit,
            offset,
            ..
        } => {
            resolve_stmt_in_subqueries(engine, db, left)?;
            resolve_stmt_in_subqueries(engine, db, right)?;
            for item in order_by {
                resolve_expr_in_subqueries(engine, db, &mut item.expr)?;
            }
            if let Some(expr) = limit {
                resolve_expr_in_subqueries(engine, db, expr)?;
            }
            if let Some(expr) = offset {
                resolve_expr_in_subqueries(engine, db, expr)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn resolve_expr_in_subqueries(engine: &Engine, db: &str, expr: &mut Expr) -> Result<()> {
    match expr {
        Expr::BinaryOp { left, right, .. } => {
            resolve_expr_in_subqueries(engine, db, left)?;
            resolve_expr_in_subqueries(engine, db, right)?;
        }
        Expr::UnaryNot(inner)
        | Expr::UnaryMinus(inner)
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner) => resolve_expr_in_subqueries(engine, db, inner)?,
        Expr::Exists(inner) => {
            if let Expr::Subquery(subquery) = inner.as_ref() {
                let exists = execute_exists_subquery(engine, db, subquery)?;
                *expr = Expr::Literal(ScalarValue::Bool(Some(exists)));
            } else {
                resolve_expr_in_subqueries(engine, db, inner)?;
            }
        }
        Expr::In {
            expr: inner,
            values,
            ..
        } => {
            resolve_expr_in_subqueries(engine, db, inner)?;
            if let [Expr::Subquery(subquery)] = values.as_mut_slice() {
                let literals = execute_in_subquery(engine, db, subquery)?;
                *values = literals.into_iter().map(Expr::Literal).collect();
            } else {
                for value in values {
                    resolve_expr_in_subqueries(engine, db, value)?;
                }
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            resolve_expr_in_subqueries(engine, db, expr)?;
            resolve_expr_in_subqueries(engine, db, low)?;
            resolve_expr_in_subqueries(engine, db, high)?;
        }
        Expr::Case {
            operand,
            when_clauses,
            else_clause,
        } => {
            if let Some(operand) = operand {
                resolve_expr_in_subqueries(engine, db, operand)?;
            }
            for (when_expr, then_expr) in when_clauses {
                resolve_expr_in_subqueries(engine, db, when_expr)?;
                resolve_expr_in_subqueries(engine, db, then_expr)?;
            }
            if let Some(else_clause) = else_clause {
                resolve_expr_in_subqueries(engine, db, else_clause)?;
            }
        }
        Expr::Like { expr, pattern, .. } => {
            resolve_expr_in_subqueries(engine, db, expr)?;
            resolve_expr_in_subqueries(engine, db, pattern)?;
        }
        Expr::Subquery(subquery) => {
            let value = execute_scalar_subquery(engine, db, subquery)?;
            *expr = Expr::Literal(value);
        }
        Expr::Function { args, .. } => {
            for arg in args {
                resolve_expr_in_subqueries(engine, db, arg)?;
            }
        }
        Expr::WindowFunction {
            partition_by,
            order_by,
            ..
        } => {
            for expr in partition_by {
                resolve_expr_in_subqueries(engine, db, expr)?;
            }
            for item in order_by {
                resolve_expr_in_subqueries(engine, db, &mut item.expr)?;
            }
        }
        Expr::Correlated(_) => {}
        Expr::Literal(_)
        | Expr::Placeholder(_)
        | Expr::Column(_)
        | Expr::QualifiedColumn { .. } => {}
    }
    Ok(())
}

fn execute_in_subquery(
    engine: &Engine,
    db: &str,
    subquery: &Statement,
) -> Result<Vec<ScalarValue>> {
    let ExecResult::Query { batches, .. } = exec_select(engine, db, subquery)? else {
        return Err(TaibaiError::Sql("IN subquery must be a SELECT".into()));
    };

    let mut values = Vec::new();
    for batch in batches {
        if batch.num_columns() == 0 {
            continue;
        }
        for row_idx in 0..batch.num_rows() {
            values.push(batch.scalar(row_idx, 0).unwrap_or(ScalarValue::Int64(None)));
        }
    }
    Ok(values)
}

pub(crate) fn execute_scalar_subquery(
    engine: &Engine,
    db: &str,
    subquery: &Statement,
) -> Result<ScalarValue> {
    let ExecResult::Query { batches, .. } = exec_select(engine, db, subquery)? else {
        return Err(TaibaiError::Sql("scalar subquery must be a SELECT".into()));
    };
    let mut first: Option<ScalarValue> = None;
    let mut rows = 0usize;
    for batch in batches {
        if batch.num_columns() == 0 {
            continue;
        }
        for row_idx in 0..batch.num_rows() {
            rows += 1;
            if first.is_none() {
                first = Some(batch.scalar(row_idx, 0).unwrap_or(ScalarValue::Int64(None)));
            }
        }
    }
    if rows > 1 {
        return Err(TaibaiError::Sql(format!(
            "scalar subquery returned more than one row ({rows})"
        )));
    }
    Ok(first.unwrap_or(ScalarValue::Int64(None)))
}

pub(crate) fn execute_exists_subquery(
    engine: &Engine,
    db: &str,
    subquery: &Statement,
) -> Result<bool> {
    let ExecResult::Query { batches, .. } = exec_select(engine, db, subquery)? else {
        return Err(TaibaiError::Sql("EXISTS subquery must be a SELECT".into()));
    };
    Ok(batches.iter().any(|batch| batch.num_rows() > 0))
}

fn remove_redundant_pushed_filter(plan: LogicalPlan, table_name: &str) -> LogicalPlan {
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

fn detect_count_star(stmt: &Statement, table_names: &[String]) -> bool {
    let (select_list, group_by, having) = match stmt {
        Statement::Select {
            select_list,
            group_by,
            having,
            ..
        } => (select_list, group_by, having),
        _ => return false,
    };

    if having.is_some() || !group_by.is_empty() || table_names.len() != 1 || select_list.len() != 1
    {
        return false;
    }

    match &select_list[0] {
        SelectItem::Expr(
            Expr::Function {
                name,
                args,
                distinct: false,
            },
            _,
        ) => {
            name.eq_ignore_ascii_case("count")
                && args.len() == 1
                && matches!(&args[0], Expr::Column(col) if col == "*")
        }
        _ => false,
    }
}

/// Detect a "simple" aggregate query answerable from row-group metadata:
/// single table, no GROUP BY/HAVING, and every select item is a non-distinct
/// `COUNT(*)` / `COUNT(col)` / `MIN(col)` / `MAX(col)`.
fn detect_simple_metadata_aggregate(stmt: &Statement, table_names: &[String]) -> bool {
    let (select_list, group_by, having) = match stmt {
        Statement::Select {
            select_list,
            group_by,
            having,
            ..
        } => (select_list, group_by, having),
        _ => return false,
    };
    if !group_by.is_empty() || having.is_some() || table_names.len() != 1 {
        return false;
    }
    if select_list.is_empty() {
        return false;
    }
    select_list.iter().all(|item| match item {
        SelectItem::Expr(
            Expr::Function {
                name,
                args,
                distinct: false,
            },
            _,
        ) => {
            let ok_func = name.eq_ignore_ascii_case("count")
                || name.eq_ignore_ascii_case("min")
                || name.eq_ignore_ascii_case("max")
                || name.eq_ignore_ascii_case("sum");
            ok_func
                && args.len() == 1
                && match &args[0] {
                    Expr::Column(col) if col == "*" => name.eq_ignore_ascii_case("count"),
                    Expr::Column(_) => true,
                    _ => false,
                }
        }
        _ => false,
    })
}

/// Pull the aggregate expressions out of the plan root (must be an
/// `Aggregate` with no GROUP BY). Returns None on any shape mismatch so the
/// caller falls back to the normal path.
fn extract_simple_metadata_aggregates(
    plan: &LogicalPlan,
    schema: &crate::types::batch::Schema,
) -> Option<Vec<crate::sql::planner::AggregateExpr>> {
    // The optimizer may wrap the aggregate in a projection; unwrap one level.
    let plan = match plan {
        LogicalPlan::Project(plan) => plan.input.as_ref(),
        other => other,
    };
    match plan {
        LogicalPlan::Aggregate(agg) if agg.group_by.is_empty() && !agg.distinct => {
            // Restrict to COUNT/MIN/MAX/SUM to match
            // detect_simple_metadata_aggregate. SUM additionally requires a
            // numeric argument column: the metadata path answers it with the
            // argument's integer/float representation, and a routed query
            // never reaches the aggregate pipeline, so unanswerable shapes
            // must stay unrouted.
            let all_supported = agg.aggregates.iter().all(|a| {
                if a.distinct {
                    return false;
                }
                match a.func {
                    crate::sql::planner::AggFunc::Count
                    | crate::sql::planner::AggFunc::Min
                    | crate::sql::planner::AggFunc::Max => true,
                    crate::sql::planner::AggFunc::Sum => match &*a.arg {
                        crate::sql::planner::LogicalExpr::Column(idx) => schema
                            .columns
                            .get(*idx)
                            .map(|c| {
                                matches!(
                                    c.data_type,
                                    crate::protocol::types::DataType::Int64
                                        | crate::protocol::types::DataType::Int32
                                        | crate::protocol::types::DataType::Int16
                                        | crate::protocol::types::DataType::Int8
                                        | crate::protocol::types::DataType::Float64
                                        | crate::protocol::types::DataType::Float32
                                )
                            })
                            .unwrap_or(false),
                        _ => false,
                    },
                    _ => false,
                }
            });
            if all_supported {
                Some(agg.aggregates.clone())
            } else {
                None
            }
        }
        _ => None,
    }
}

fn try_skip_sort_for_cluster_key(
    plan: LogicalPlan,
    table_batches: &HashMap<String, Vec<RecordBatch>>,
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

#[derive(Debug, Clone)]
pub struct ResolvedTableRef {
    pub key: String,
    pub database: String,
    pub table: String,
}

impl ResolvedTableRef {
    fn new(object: &ObjectName, default_db: &str) -> Self {
        Self {
            key: object.plan_key(),
            database: object.effective_database(default_db).to_string(),
            table: object.name.clone(),
        }
    }
}

pub fn extract_table_refs(from: &FromClause, default_db: &str) -> Vec<ResolvedTableRef> {
    match from {
        FromClause::Table(t) => match t {
            TableRef::Table { name, .. } => vec![ResolvedTableRef::new(name, default_db)],
            TableRef::Subquery { .. } => Vec::new(),
        },
        FromClause::Join(join_expr) => {
            let mut refs = Vec::new();
            collect_table_refs_from_join(join_expr, default_db, &mut refs);
            refs
        }
    }
}

fn collect_table_refs_from_join(
    join_expr: &crate::sql::parser::ast::JoinExpr,
    default_db: &str,
    refs: &mut Vec<ResolvedTableRef>,
) {
    match &*join_expr.left {
        FromClause::Table(t) => match t {
            TableRef::Table { name, .. } => refs.push(ResolvedTableRef::new(name, default_db)),
            TableRef::Subquery { .. } => {}
        },
        FromClause::Join(inner) => collect_table_refs_from_join(inner, default_db, refs),
    }
    for jc in &join_expr.joins {
        match &jc.right {
            TableRef::Table { name, .. } => refs.push(ResolvedTableRef::new(name, default_db)),
            TableRef::Subquery { .. } => {}
        }
    }
}

// ============================================================================
// Derived tables and correlated subqueries
// ============================================================================

/// Synthetic database that backs materialized derived tables
/// (`FROM (SELECT ...) alias`). Queries never see it; it only keeps the
/// rewritten pseudo-table scans out of the real catalog namespace.
pub const DERIVED_DB: &str = "__derived__";

/// One query block's binding scope: the concatenated FROM schema and the
/// table alias map, matching how the planner assigns column offsets.
pub(crate) struct QueryBlockScope {
    pub schema: Schema,
    pub alias_map: crate::sql::planner::AliasMap,
}

/// The fully resolved form of a SELECT: derived tables materialized into
/// pseudo-table scans, uncorrelated subqueries collapsed into literals, and
/// correlated subqueries bound into markers registered in `registry`.
pub(crate) struct ResolvedQuery {
    pub stmt: Statement,
    pub derived: HashMap<String, (Schema, Vec<RecordBatch>)>,
    pub registry: super::subquery_runtime::SubqueryRegistry,
    pub has_correlated: bool,
}

/// Carries an already-resolved query (with its derived tables and subquery
/// registry) into an execution that would otherwise re-resolve. Used by
/// EXPLAIN ANALYZE, which plans/resolves up front and then executes the
/// cached plan for profiling.
pub(crate) struct PreResolvedExec<'a> {
    pub resolved: &'a ResolvedQuery,
    /// Decorrelated right-side pseudo-table batches produced alongside the
    /// cached plan.
    pub extra_batches: &'a HashMap<String, Vec<RecordBatch>>,
}

/// Resolve a SELECT statement for execution or planning (also used by
/// EXPLAIN).
pub(crate) fn resolve_query(engine: &Engine, db: &str, stmt: &Statement) -> Result<ResolvedQuery> {
    let mut working = stmt.clone();
    // 1. Materialize derived tables (FROM (SELECT ...) alias) into
    //    pseudo-table scans backed by the materialized batches.
    let derived = materialize_derived_tables(engine, db, &mut working)?;
    let working_from = match &working {
        Statement::Select {
            from: Some(from), ..
        } => from.clone(),
        _ => {
            return Err(TaibaiError::Internal(
                "resolve_query called with non-SELECT".into(),
            ))
        }
    };
    // 2. Build the query block scope (concatenated schema + alias map).
    let mut scope_schemas = HashMap::new();
    {
        let catalogs = engine.catalogs().read().unwrap();
        for table_ref in extract_table_refs(&working_from, db) {
            if table_ref.database == DERIVED_DB {
                let (schema, _) = derived.get(&table_ref.key).ok_or_else(|| {
                    TaibaiError::Internal(format!(
                        "missing materialized derived table '{}'",
                        table_ref.key
                    ))
                })?;
                scope_schemas.insert(table_ref.key.clone(), schema.clone());
            } else {
                let catalog = catalogs.get(&table_ref.database).ok_or_else(|| {
                    TaibaiError::Sql(format!("database '{}' does not exist", table_ref.database))
                })?;
                let meta = catalog.get_table(&table_ref.table).ok_or_else(|| {
                    TaibaiError::Sql(format!(
                        "table '{}.{}' does not exist",
                        table_ref.database, table_ref.table
                    ))
                })?;
                scope_schemas.insert(table_ref.key.clone(), Schema::new(meta.columns.clone()));
            }
        }
    }
    let (schema, alias_map) =
        crate::sql::planner::build_scope_bindings(&working_from, &scope_schemas);
    let scope = QueryBlockScope { schema, alias_map };
    // 3. Resolve expression-level subqueries: uncorrelated ones execute
    //    eagerly; correlated ones are bound into per-row markers.
    let registry = super::subquery_runtime::new_registry();
    resolve_query_subqueries(engine, db, &mut working, Some(&scope), &registry, true)?;
    let has_correlated = super::subquery_runtime::registry_has_snapshots(&registry);
    Ok(ResolvedQuery {
        stmt: working,
        derived,
        registry,
        has_correlated,
    })
}

/// Rewrite every `FROM (SELECT ...) alias` of the query block into a scan of
/// a synthetic table, executing each subquery once and keeping its batches.
fn materialize_derived_tables(
    engine: &Engine,
    db: &str,
    stmt: &mut Statement,
) -> Result<HashMap<String, (Schema, Vec<RecordBatch>)>> {
    let mut derived = HashMap::new();
    if let Statement::Select {
        from: Some(from), ..
    } = stmt
    {
        let mut counter = 0usize;
        rewrite_derived_in_from(from, engine, db, &mut counter, &mut derived)?;
    }
    Ok(derived)
}

fn rewrite_derived_in_from(
    from: &mut FromClause,
    engine: &Engine,
    db: &str,
    counter: &mut usize,
    derived: &mut HashMap<String, (Schema, Vec<RecordBatch>)>,
) -> Result<()> {
    match from {
        FromClause::Table(table_ref) => {
            rewrite_derived_table_ref(table_ref, engine, db, counter, derived)
        }
        FromClause::Join(join_expr) => {
            rewrite_derived_in_from(&mut join_expr.left, engine, db, counter, derived)?;
            for join_clause in &mut join_expr.joins {
                rewrite_derived_table_ref(&mut join_clause.right, engine, db, counter, derived)?;
            }
            Ok(())
        }
    }
}

fn rewrite_derived_table_ref(
    table_ref: &mut TableRef,
    engine: &Engine,
    db: &str,
    counter: &mut usize,
    derived: &mut HashMap<String, (Schema, Vec<RecordBatch>)>,
) -> Result<()> {
    if let TableRef::Subquery { query, alias } = table_ref {
        let subquery = (**query).clone();
        let (schema, batches) = exec_query_materialized(engine, db, &subquery)?;
        let pseudo = format!("__derived_{}", *counter);
        *counter += 1;
        let object = ObjectName {
            database: Some(DERIVED_DB.to_string()),
            name: pseudo,
        };
        derived.insert(object.plan_key(), (schema, batches));
        *table_ref = TableRef::Table {
            name: object,
            alias: Some(alias.clone()),
        };
    }
    Ok(())
}

fn exec_query_materialized(
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

/// Walk a statement, resolving expression-level subqueries. Uncorrelated
/// subqueries execute eagerly and collapse into literals (existing
/// behavior); correlated ones — when `allow_correlated` and an enclosing
/// scope are available — are rewritten into `Expr::Correlated` markers and
/// registered in `registry`.
fn resolve_query_subqueries(
    engine: &Engine,
    db: &str,
    stmt: &mut Statement,
    scope: Option<&QueryBlockScope>,
    registry: &super::subquery_runtime::SubqueryRegistry,
    allow_correlated: bool,
) -> Result<()> {
    match stmt {
        Statement::Select {
            select_list,
            where_clause,
            group_by,
            having,
            order_by,
            limit,
            offset,
            ..
        } => {
            for item in select_list {
                if let SelectItem::Expr(expr, _) = item {
                    resolve_query_expr(engine, db, expr, scope, registry, allow_correlated)?;
                }
            }
            if let Some(expr) = where_clause {
                resolve_query_expr(engine, db, expr, scope, registry, allow_correlated)?;
            }
            for expr in group_by {
                resolve_query_expr(engine, db, expr, scope, registry, allow_correlated)?;
            }
            if let Some(expr) = having {
                resolve_query_expr(engine, db, expr, scope, registry, allow_correlated)?;
            }
            for item in order_by {
                resolve_query_expr(
                    engine,
                    db,
                    &mut item.expr,
                    scope,
                    registry,
                    allow_correlated,
                )?;
            }
            if let Some(expr) = limit {
                resolve_query_expr(engine, db, expr, scope, registry, allow_correlated)?;
            }
            if let Some(expr) = offset {
                resolve_query_expr(engine, db, expr, scope, registry, allow_correlated)?;
            }
        }
        Statement::Union {
            left,
            right,
            order_by,
            limit,
            offset,
            ..
        } => {
            // Each UNION branch is its own query block; correlated references
            // crossing the UNION boundary are not supported.
            resolve_query_subqueries(engine, db, left, None, registry, false)?;
            resolve_query_subqueries(engine, db, right, None, registry, false)?;
            for item in order_by {
                resolve_query_expr(engine, db, &mut item.expr, None, registry, false)?;
            }
            if let Some(expr) = limit {
                resolve_query_expr(engine, db, expr, None, registry, false)?;
            }
            if let Some(expr) = offset {
                resolve_query_expr(engine, db, expr, None, registry, false)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn resolve_query_expr(
    engine: &Engine,
    db: &str,
    expr: &mut Expr,
    scope: Option<&QueryBlockScope>,
    registry: &super::subquery_runtime::SubqueryRegistry,
    allow_correlated: bool,
) -> Result<()> {
    match expr {
        Expr::BinaryOp { left, right, .. } => {
            resolve_query_expr(engine, db, left, scope, registry, allow_correlated)?;
            resolve_query_expr(engine, db, right, scope, registry, allow_correlated)?;
        }
        Expr::UnaryNot(inner)
        | Expr::UnaryMinus(inner)
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner) => {
            resolve_query_expr(engine, db, inner, scope, registry, allow_correlated)?;
        }
        Expr::Exists(inner) => {
            if let Expr::Subquery(subquery) = inner.as_ref() {
                let subquery = (**subquery).clone();
                *expr = resolve_subquery_node(
                    engine,
                    db,
                    CorrelatedKind::Exists { negated: false },
                    None,
                    &subquery,
                    scope,
                    registry,
                    allow_correlated,
                )?;
            } else {
                resolve_query_expr(engine, db, inner, scope, registry, allow_correlated)?;
            }
        }
        Expr::In {
            expr: inner,
            values,
            negated,
        } => {
            resolve_query_expr(engine, db, inner, scope, registry, allow_correlated)?;
            let subquery = match values.as_slice() {
                [Expr::Subquery(sub)] => Some((**sub).clone()),
                _ => None,
            };
            if let Some(subquery) = subquery {
                let lhs = (**inner).clone();
                *expr = resolve_subquery_node(
                    engine,
                    db,
                    CorrelatedKind::In { negated: *negated },
                    Some(lhs),
                    &subquery,
                    scope,
                    registry,
                    allow_correlated,
                )?;
            } else {
                for value in values {
                    resolve_query_expr(engine, db, value, scope, registry, allow_correlated)?;
                }
            }
        }
        Expr::Subquery(subquery) => {
            let subquery = (**subquery).clone();
            *expr = resolve_subquery_node(
                engine,
                db,
                CorrelatedKind::Scalar,
                None,
                &subquery,
                scope,
                registry,
                allow_correlated,
            )?;
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            resolve_query_expr(engine, db, expr, scope, registry, allow_correlated)?;
            resolve_query_expr(engine, db, low, scope, registry, allow_correlated)?;
            resolve_query_expr(engine, db, high, scope, registry, allow_correlated)?;
        }
        Expr::Case {
            operand,
            when_clauses,
            else_clause,
        } => {
            if let Some(operand) = operand {
                resolve_query_expr(engine, db, operand, scope, registry, allow_correlated)?;
            }
            for (when_expr, then_expr) in when_clauses {
                resolve_query_expr(engine, db, when_expr, scope, registry, allow_correlated)?;
                resolve_query_expr(engine, db, then_expr, scope, registry, allow_correlated)?;
            }
            if let Some(else_expr) = else_clause {
                resolve_query_expr(engine, db, else_expr, scope, registry, allow_correlated)?;
            }
        }
        Expr::Like { expr, pattern, .. } => {
            resolve_query_expr(engine, db, expr, scope, registry, allow_correlated)?;
            resolve_query_expr(engine, db, pattern, scope, registry, allow_correlated)?;
        }
        Expr::Function { args, .. } => {
            for arg in args {
                resolve_query_expr(engine, db, arg, scope, registry, allow_correlated)?;
            }
        }
        Expr::WindowFunction {
            partition_by,
            order_by,
            ..
        } => {
            for part in partition_by {
                resolve_query_expr(engine, db, part, scope, registry, allow_correlated)?;
            }
            for item in order_by {
                resolve_query_expr(
                    engine,
                    db,
                    &mut item.expr,
                    scope,
                    registry,
                    allow_correlated,
                )?;
            }
        }
        Expr::Correlated(_)
        | Expr::Literal(_)
        | Expr::Placeholder(_)
        | Expr::Column(_)
        | Expr::QualifiedColumn { .. } => {}
    }
    Ok(())
}

/// Resolve one subquery occurrence: try eager uncorrelated execution first;
/// on a correlation-shaped binding error, bind it as a correlated subquery.
#[allow(clippy::too_many_arguments)]
fn resolve_subquery_node(
    engine: &Engine,
    db: &str,
    kind: CorrelatedKind,
    lhs: Option<Expr>,
    subquery: &Statement,
    scope: Option<&QueryBlockScope>,
    registry: &super::subquery_runtime::SubqueryRegistry,
    allow_correlated: bool,
) -> Result<Expr> {
    let eager: Result<Expr> = (|| match kind {
        CorrelatedKind::Exists { negated } => {
            let exists = execute_exists_subquery(engine, db, subquery)?;
            Ok(Expr::Literal(ScalarValue::Bool(Some(if negated {
                !exists
            } else {
                exists
            }))))
        }
        CorrelatedKind::Scalar => {
            let value = execute_scalar_subquery(engine, db, subquery)?;
            Ok(Expr::Literal(value))
        }
        CorrelatedKind::In { negated } => {
            let lhs = lhs.clone().expect("IN subquery carries a lhs expression");
            let values = execute_in_subquery(engine, db, subquery)?
                .into_iter()
                .map(Expr::Literal)
                .collect();
            Ok(Expr::In {
                expr: Box::new(lhs),
                values,
                negated,
            })
        }
    })();
    match eager {
        Ok(resolved) => Ok(resolved),
        Err(e)
            if allow_correlated
                && scope.is_some()
                && crate::sql::planner::expr::is_correlation_binding_error(&e) =>
        {
            bind_correlated_subquery(
                engine,
                db,
                kind,
                lhs,
                subquery,
                scope.expect("scope checked above"),
                registry,
            )
        }
        Err(e) => Err(e),
    }
}

/// Bind a correlated subquery against its enclosing block:
/// 1. reject user parameter markers inside,
/// 2. materialize the inner block's own derived tables,
/// 3. eagerly resolve the inner block's own (uncorrelated) subqueries,
/// 4. snapshot the inner tables (schemas + batches) into the registry,
/// 5. split the inner WHERE into local and correlated conjuncts, rewriting
///    outer references to `Placeholder` markers,
/// 6. capture the scalar result type.
#[allow(clippy::too_many_arguments)]
fn bind_correlated_subquery(
    engine: &Engine,
    db: &str,
    kind: CorrelatedKind,
    lhs: Option<Expr>,
    inner_stmt: &Statement,
    scope: &QueryBlockScope,
    registry: &super::subquery_runtime::SubqueryRegistry,
) -> Result<Expr> {
    if !matches!(inner_stmt, Statement::Select { .. }) {
        return Err(TaibaiError::Sql(
            "correlated subqueries must be a single SELECT".into(),
        ));
    }
    let mut inner = inner_stmt.clone();
    if stmt_contains_placeholder(&inner) {
        return Err(TaibaiError::Sql(
            "parameter markers (?) are not supported inside correlated subqueries".into(),
        ));
    }

    let inner_derived = materialize_derived_tables(engine, db, &mut inner)?;
    if let Err(e) = resolve_query_subqueries(engine, db, &mut inner, None, registry, false) {
        if crate::sql::planner::expr::is_correlation_binding_error(&e) {
            return Err(TaibaiError::Sql(format!(
                "nested correlated subqueries are not supported (only one level of \
                 correlation is supported): {e}"
            )));
        }
        return Err(e);
    }

    // Snapshot the inner FROM tables so per-row evaluation never needs the
    // engine.
    let inner_from = match &inner {
        Statement::Select {
            from: Some(from), ..
        } => Some(from.clone()),
        _ => None,
    };
    let mut snapshot_schemas: HashMap<String, Schema> = HashMap::new();
    let mut snapshot_batches: HashMap<String, Vec<RecordBatch>> = HashMap::new();
    if let Some(from) = &inner_from {
        for table_ref in extract_table_refs(from, db) {
            if table_ref.database == DERIVED_DB {
                let (schema, batches) = inner_derived.get(&table_ref.key).ok_or_else(|| {
                    TaibaiError::Internal(format!(
                        "missing materialized derived table '{}'",
                        table_ref.key
                    ))
                })?;
                snapshot_schemas.insert(table_ref.key.clone(), schema.clone());
                snapshot_batches.insert(table_ref.key.clone(), batches.clone());
            } else {
                let (schema, batches) = snapshot_table(engine, &table_ref)?;
                snapshot_schemas.insert(table_ref.key.clone(), schema);
                snapshot_batches.insert(table_ref.key.clone(), batches);
            }
        }
    }

    let (inner_schema, inner_alias_map) = match &inner_from {
        Some(from) => crate::sql::planner::build_scope_bindings(from, &snapshot_schemas),
        None => (Schema::empty(), HashMap::new()),
    };

    // Pre-resolve select-list aliases in GROUP BY / HAVING / ORDER BY the
    // same way the planner would, so alias references don't look like
    // unresolvable (correlated) columns during marking.
    if let Statement::Select {
        select_list,
        group_by,
        having,
        order_by,
        ..
    } = &mut inner
    {
        let alias_entries: Vec<(String, Expr)> = select_list
            .iter()
            .filter_map(|item| match item {
                SelectItem::Expr(e, Some(alias)) => Some((alias.to_lowercase(), e.clone())),
                _ => None,
            })
            .collect();
        if !alias_entries.is_empty() {
            let substitute = |expr: &mut Expr| {
                *expr = crate::sql::planner::expr::substitute_select_aliases(
                    expr,
                    &alias_entries,
                    &inner_schema,
                );
            };
            for expr in group_by {
                substitute(expr);
            }
            if let Some(expr) = having {
                substitute(expr);
            }
            for item in order_by {
                substitute(&mut item.expr);
            }
        }
    }

    let mut binder = MarkerBinder {
        inner_schema: &inner_schema,
        inner_alias_map: &inner_alias_map,
        outer_schema: &scope.schema,
        outer_alias_map: &scope.alias_map,
        next_id: 0,
        params: Vec::new(),
    };

    // Split the inner WHERE into local and correlated conjuncts.
    let where_expr = match &mut inner {
        Statement::Select { where_clause, .. } => where_clause.take(),
        _ => None,
    };
    let mut correlated_conjuncts: Vec<Expr> = Vec::new();
    let mut local_conjuncts: Vec<Expr> = Vec::new();
    if let Some(where_expr) = where_expr {
        for conjunct in split_and_conjuncts(where_expr) {
            let marked = binder.mark_expr(conjunct)?;
            if expr_contains_placeholder(&marked) {
                correlated_conjuncts.push(marked);
            } else {
                local_conjuncts.push(marked);
            }
        }
    }
    if let Statement::Select { where_clause, .. } = &mut inner {
        *where_clause = combine_conjuncts(local_conjuncts);
    }

    // Mark remaining expression positions (select list, GROUP BY, HAVING,
    // ORDER BY). Those markers stay in place and are substituted per row.
    if let Statement::Select {
        select_list,
        group_by,
        having,
        order_by,
        ..
    } = &mut inner
    {
        for item in select_list {
            if let SelectItem::Expr(expr, _) = item {
                *expr = binder.mark_expr(expr.clone())?;
            }
        }
        for expr in group_by {
            *expr = binder.mark_expr(expr.clone())?;
        }
        if let Some(expr) = having {
            *expr = binder.mark_expr(expr.clone())?;
        }
        for item in order_by {
            item.expr = binder.mark_expr(item.expr.clone())?;
        }
    }

    let result_type = match kind {
        CorrelatedKind::Exists { .. } | CorrelatedKind::In { .. } => {
            crate::protocol::types::DataType::Bool
        }
        CorrelatedKind::Scalar => {
            // Probe the output type with a single-row execution of the local
            // part. Fall back to Int64 when the probe cannot run cleanly
            // (e.g. markers remain in the select list); projection coerces.
            let mut probe = inner.clone();
            if let Statement::Select { limit, offset, .. } = &mut probe {
                *limit = Some(Expr::Literal(ScalarValue::Int64(Some(1))));
                *offset = None;
            }
            match exec_select(engine, db, &probe) {
                Ok(ExecResult::Query { schema, .. }) if !schema.columns.is_empty() => {
                    schema.columns[0].data_type
                }
                _ => crate::protocol::types::DataType::Int64,
            }
        }
    };

    let id = registry
        .lock()
        .unwrap()
        .allocate(super::subquery_runtime::CorrelatedSnapshot {
            schemas: snapshot_schemas,
            batches: snapshot_batches,
            cache: HashMap::new(),
        });

    Ok(Expr::Correlated(Box::new(CorrelatedSubquery {
        id,
        kind,
        lhs: lhs.map(Box::new),
        stmt: Box::new(inner),
        conjuncts: correlated_conjuncts,
        params: binder.params,
        result_type,
    })))
}

/// Rewrites references that resolve outside the inner block into
/// `Placeholder` markers and records their enclosing-block column indexes.
struct MarkerBinder<'a> {
    inner_schema: &'a Schema,
    inner_alias_map: &'a crate::sql::planner::AliasMap,
    outer_schema: &'a Schema,
    outer_alias_map: &'a crate::sql::planner::AliasMap,
    next_id: usize,
    params: Vec<(usize, usize)>,
}

impl<'a> MarkerBinder<'a> {
    fn new_marker(&mut self, outer_idx: usize) -> Expr {
        let id = self.next_id;
        self.next_id += 1;
        self.params.push((id, outer_idx));
        Expr::Placeholder(id)
    }

    fn mark_expr(&mut self, expr: Expr) -> Result<Expr> {
        match expr {
            // `COUNT(*)` is spelled as a Function over Column("*"); the star
            // is not a real column reference.
            Expr::Column(ref name) if name == "*" => Ok(expr),
            Expr::Column(name) => {
                if self.inner_schema.index_of(&name).is_some() {
                    return Ok(Expr::Column(name));
                }
                if let Some(idx) = self.outer_schema.index_of(&name) {
                    return Ok(self.new_marker(idx));
                }
                Err(TaibaiError::Sql(format!("column '{name}' not found")))
            }
            Expr::QualifiedColumn { table, column } => {
                let table_lower = table.to_lowercase();
                if let Some(binding) = self.inner_alias_map.get(&table_lower) {
                    if col_in_range(self.inner_schema, binding, &column).is_some() {
                        return Ok(Expr::QualifiedColumn { table, column });
                    }
                    return Err(TaibaiError::Sql(format!(
                        "column '{column}' not found in table '{table}'"
                    )));
                }
                if let Some(binding) = self.outer_alias_map.get(&table_lower) {
                    if let Some(idx) = col_in_range(self.outer_schema, binding, &column) {
                        return Ok(self.new_marker(idx));
                    }
                    return Err(TaibaiError::Sql(format!(
                        "column '{column}' not found in table '{table}'"
                    )));
                }
                Err(TaibaiError::Sql(format!(
                    "unknown table or alias '{table}' (correlated references must resolve to \
                     the immediately enclosing query block)"
                )))
            }
            Expr::BinaryOp { left, op, right } => Ok(Expr::BinaryOp {
                left: Box::new(self.mark_expr(*left)?),
                op,
                right: Box::new(self.mark_expr(*right)?),
            }),
            Expr::UnaryNot(inner) => Ok(Expr::UnaryNot(Box::new(self.mark_expr(*inner)?))),
            Expr::UnaryMinus(inner) => Ok(Expr::UnaryMinus(Box::new(self.mark_expr(*inner)?))),
            Expr::IsNull(inner) => Ok(Expr::IsNull(Box::new(self.mark_expr(*inner)?))),
            Expr::IsNotNull(inner) => Ok(Expr::IsNotNull(Box::new(self.mark_expr(*inner)?))),
            Expr::In {
                expr,
                values,
                negated,
            } => Ok(Expr::In {
                expr: Box::new(self.mark_expr(*expr)?),
                values: values
                    .into_iter()
                    .map(|v| self.mark_expr(v))
                    .collect::<Result<Vec<_>>>()?,
                negated,
            }),
            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => Ok(Expr::Between {
                expr: Box::new(self.mark_expr(*expr)?),
                low: Box::new(self.mark_expr(*low)?),
                high: Box::new(self.mark_expr(*high)?),
                negated,
            }),
            Expr::Case {
                operand,
                when_clauses,
                else_clause,
            } => Ok(Expr::Case {
                operand: operand
                    .map(|e| self.mark_expr(*e))
                    .transpose()?
                    .map(Box::new),
                when_clauses: when_clauses
                    .into_iter()
                    .map(|(w, t)| Ok((self.mark_expr(w)?, self.mark_expr(t)?)))
                    .collect::<Result<Vec<_>>>()?,
                else_clause: else_clause
                    .map(|e| self.mark_expr(*e))
                    .transpose()?
                    .map(Box::new),
            }),
            Expr::Like {
                expr,
                pattern,
                negated,
            } => Ok(Expr::Like {
                expr: Box::new(self.mark_expr(*expr)?),
                pattern: Box::new(self.mark_expr(*pattern)?),
                negated,
            }),
            Expr::Function {
                name,
                args,
                distinct,
            } => Ok(Expr::Function {
                name,
                args: args
                    .into_iter()
                    .map(|a| self.mark_expr(a))
                    .collect::<Result<Vec<_>>>()?,
                distinct,
            }),
            other => Ok(other),
        }
    }
}

fn col_in_range(
    schema: &Schema,
    binding: &crate::sql::planner::TableBinding,
    column: &str,
) -> Option<usize> {
    (binding.start..binding.end).find(|&idx| {
        schema
            .columns
            .get(idx)
            .map(|c| c.name.eq_ignore_ascii_case(column))
            .unwrap_or(false)
    })
}

fn split_and_conjuncts(expr: Expr) -> Vec<Expr> {
    match expr {
        Expr::BinaryOp {
            left,
            op: crate::sql::parser::ast::BinOp::And,
            right,
        } => {
            let mut conjuncts = split_and_conjuncts(*left);
            conjuncts.extend(split_and_conjuncts(*right));
            conjuncts
        }
        other => vec![other],
    }
}

fn combine_conjuncts(conjuncts: Vec<Expr>) -> Option<Expr> {
    let mut combined: Option<Expr> = None;
    for conjunct in conjuncts {
        combined = Some(match combined {
            Some(prev) => Expr::BinaryOp {
                left: Box::new(prev),
                op: crate::sql::parser::ast::BinOp::And,
                right: Box::new(conjunct),
            },
            None => conjunct,
        });
    }
    combined
}

fn expr_contains_placeholder(expr: &Expr) -> bool {
    match expr {
        Expr::Placeholder(_) => true,
        Expr::BinaryOp { left, right, .. } => {
            expr_contains_placeholder(left) || expr_contains_placeholder(right)
        }
        Expr::UnaryNot(inner)
        | Expr::UnaryMinus(inner)
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner) => expr_contains_placeholder(inner),
        Expr::In { expr, values, .. } => {
            expr_contains_placeholder(expr) || values.iter().any(expr_contains_placeholder)
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_contains_placeholder(expr)
                || expr_contains_placeholder(low)
                || expr_contains_placeholder(high)
        }
        Expr::Case {
            operand,
            when_clauses,
            else_clause,
        } => {
            operand
                .as_ref()
                .map(|e| expr_contains_placeholder(e))
                .unwrap_or(false)
                || when_clauses
                    .iter()
                    .any(|(w, t)| expr_contains_placeholder(w) || expr_contains_placeholder(t))
                || else_clause
                    .as_ref()
                    .map(|e| expr_contains_placeholder(e))
                    .unwrap_or(false)
        }
        Expr::Like { expr, pattern, .. } => {
            expr_contains_placeholder(expr) || expr_contains_placeholder(pattern)
        }
        Expr::Function { args, .. } => args.iter().any(expr_contains_placeholder),
        Expr::WindowFunction {
            partition_by,
            order_by,
            ..
        } => {
            partition_by.iter().any(expr_contains_placeholder)
                || order_by
                    .iter()
                    .any(|item| expr_contains_placeholder(&item.expr))
        }
        Expr::Exists(inner) => expr_contains_placeholder(inner),
        _ => false,
    }
}

fn stmt_contains_placeholder(stmt: &Statement) -> bool {
    match stmt {
        Statement::Select {
            select_list,
            where_clause,
            group_by,
            having,
            order_by,
            limit,
            offset,
            ..
        } => {
            select_list
                .iter()
                .any(|item| matches!(item, SelectItem::Expr(e, _) if expr_contains_placeholder(e)))
                || where_clause
                    .as_ref()
                    .map(expr_contains_placeholder)
                    .unwrap_or(false)
                || group_by.iter().any(expr_contains_placeholder)
                || having
                    .as_ref()
                    .map(expr_contains_placeholder)
                    .unwrap_or(false)
                || order_by
                    .iter()
                    .any(|item| expr_contains_placeholder(&item.expr))
                || limit
                    .as_ref()
                    .map(expr_contains_placeholder)
                    .unwrap_or(false)
                || offset
                    .as_ref()
                    .map(expr_contains_placeholder)
                    .unwrap_or(false)
        }
        Statement::Union { left, right, .. } => {
            stmt_contains_placeholder(left) || stmt_contains_placeholder(right)
        }
        _ => false,
    }
}

fn expr_has_subquery(expr: &Expr) -> bool {
    match expr {
        Expr::Subquery(_) | Expr::Correlated(_) => true,
        Expr::Exists(inner) => match inner.as_ref() {
            Expr::Subquery(_) => true,
            other => expr_has_subquery(other),
        },
        Expr::In { expr, values, .. } => {
            expr_has_subquery(expr)
                || values.iter().any(|v| matches!(v, Expr::Subquery(_)))
                || values.iter().any(expr_has_subquery)
        }
        Expr::BinaryOp { left, right, .. } => expr_has_subquery(left) || expr_has_subquery(right),
        Expr::UnaryNot(inner)
        | Expr::UnaryMinus(inner)
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner) => expr_has_subquery(inner),
        Expr::Between {
            expr, low, high, ..
        } => expr_has_subquery(expr) || expr_has_subquery(low) || expr_has_subquery(high),
        Expr::Case {
            operand,
            when_clauses,
            else_clause,
        } => {
            operand
                .as_ref()
                .map(|e| expr_has_subquery(e))
                .unwrap_or(false)
                || when_clauses
                    .iter()
                    .any(|(w, t)| expr_has_subquery(w) || expr_has_subquery(t))
                || else_clause
                    .as_ref()
                    .map(|e| expr_has_subquery(e))
                    .unwrap_or(false)
        }
        Expr::Like { expr, pattern, .. } => expr_has_subquery(expr) || expr_has_subquery(pattern),
        Expr::Function { args, .. } => args.iter().any(expr_has_subquery),
        Expr::WindowFunction {
            partition_by,
            order_by,
            ..
        } => {
            partition_by.iter().any(expr_has_subquery)
                || order_by.iter().any(|item| expr_has_subquery(&item.expr))
        }
        _ => false,
    }
}

fn from_has_derived(from: &FromClause) -> bool {
    match from {
        FromClause::Table(t) => matches!(t, TableRef::Subquery { .. }),
        FromClause::Join(join_expr) => {
            from_has_derived(&join_expr.left)
                || join_expr.joins.iter().any(|jc| {
                    matches!(&jc.right, TableRef::Subquery { .. })
                        || jc
                            .condition
                            .as_ref()
                            .map(expr_has_subquery)
                            .unwrap_or(false)
                })
        }
    }
}

/// Fast pre-check used by the streaming path to defer subquery/derived-table
/// queries to the materialized execution path.
pub(crate) fn stmt_has_subquery_or_derived(stmt: &Statement) -> bool {
    match stmt {
        Statement::Select {
            select_list,
            from,
            where_clause,
            group_by,
            having,
            order_by,
            limit,
            offset,
            ..
        } => {
            from.as_ref().map(from_has_derived).unwrap_or(false)
                || select_list
                    .iter()
                    .any(|item| matches!(item, SelectItem::Expr(e, _) if expr_has_subquery(e)))
                || where_clause
                    .as_ref()
                    .map(expr_has_subquery)
                    .unwrap_or(false)
                || group_by.iter().any(expr_has_subquery)
                || having.as_ref().map(expr_has_subquery).unwrap_or(false)
                || order_by.iter().any(|item| expr_has_subquery(&item.expr))
                || limit.as_ref().map(expr_has_subquery).unwrap_or(false)
                || offset.as_ref().map(expr_has_subquery).unwrap_or(false)
        }
        Statement::Union {
            left,
            right,
            order_by,
            limit,
            offset,
            ..
        } => {
            stmt_has_subquery_or_derived(left)
                || stmt_has_subquery_or_derived(right)
                || order_by.iter().any(|item| expr_has_subquery(&item.expr))
                || limit.as_ref().map(expr_has_subquery).unwrap_or(false)
                || offset.as_ref().map(expr_has_subquery).unwrap_or(false)
        }
        _ => false,
    }
}

/// Materialize a full snapshot of one base table for the correlated-subquery
/// runtime. The snapshot is plain data, so per-row evaluation never needs the
/// engine.
fn snapshot_table(
    engine: &Engine,
    table_ref: &ResolvedTableRef,
) -> Result<(Schema, Vec<RecordBatch>)> {
    let stmt = Statement::Select {
        select_list: vec![SelectItem::Star],
        from: Some(FromClause::Table(TableRef::Table {
            name: ObjectName::qualified(table_ref.database.clone(), table_ref.table.clone()),
            alias: None,
        })),
        where_clause: None,
        group_by: Vec::new(),
        having: None,
        order_by: Vec::new(),
        limit: None,
        offset: None,
        distinct: false,
    };
    match exec_select(engine, &table_ref.database, &stmt)? {
        ExecResult::Query { schema, batches } => Ok((schema, batches)),
        _ => Err(TaibaiError::Sql(format!(
            "failed to snapshot table '{}.{}'",
            table_ref.database, table_ref.table
        ))),
    }
}
