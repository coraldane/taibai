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
use crate::sql::parser::ast::{Expr, FromClause, ObjectName, SelectItem, Statement, TableRef};
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
    exec_select_inner(engine, db, stmt, None, None, reader_txn_id, None)
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
    )
}

pub(crate) fn exec_select_with_profile(
    engine: &Engine,
    db: &str,
    stmt: &Statement,
    stats: &mut HashMap<String, ScanStats>,
    prepared_plan: LogicalPlan,
    profile: &QueryProfile,
) -> Result<ExecResult> {
    exec_select_inner(
        engine,
        db,
        stmt,
        Some(stats),
        Some(prepared_plan),
        0,
        Some(profile),
    )
}

fn exec_select_inner(
    engine: &Engine,
    db: &str,
    stmt: &Statement,
    stats: Option<&mut HashMap<String, ScanStats>>,
    cached_plan: Option<LogicalPlan>,
    reader_txn_id: u64,
    profile: Option<&QueryProfile>,
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
        None => exec_literal_select(stmt),
        Some(from_clause) => exec_table_select(
            engine,
            db,
            stmt,
            from_clause,
            stats,
            cached_plan,
            reader_txn_id,
            profile,
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
) -> Result<ExecResult> {
    let resolved_stmt;
    let (stmt, from_clause) = if cached_plan.is_none() {
        resolved_stmt = resolve_in_subqueries(engine, db, stmt)?;
        let resolved_from = match &resolved_stmt {
            Statement::Select {
                from: Some(from), ..
            } => from,
            _ => from_clause,
        };
        (&resolved_stmt, resolved_from)
    } else {
        (stmt, from_clause)
    };

    let table_refs = extract_table_refs(from_clause, db);
    let table_names: Vec<String> = table_refs.iter().map(|t| t.key.clone()).collect();

    let mut table_schemas = HashMap::new();
    let mut table_cluster_keys: HashMap<String, Vec<String>> = HashMap::new();
    let mut table_metas: HashMap<String, crate::storage::catalog::schema::TableMeta> =
        HashMap::new();
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
            crate::sql::optimizer::optimize(plan)
        }
    };

    let is_count_star = detect_count_star(stmt, &table_names);
    // Simple metadata-aggregate fast path: COUNT/MIN/MAX without GROUP BY.
    // Only detected on a single-table, committed-read query with no stats sink.
    let simple_metadata_aggregates = if reader_txn_id == 0
        && stats.is_none()
        && !is_count_star
        && table_names.len() == 1
        && detect_simple_metadata_aggregate(stmt, &table_names)
    {
        extract_simple_metadata_aggregates(&plan)
    } else {
        None
    };
    let is_simple_metadata = simple_metadata_aggregates.is_some();

    if reader_txn_id == 0
        && stats.is_none()
        && !is_count_star
        && !is_simple_metadata
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
) -> Result<ExecResult> {
    let mut table_batches: HashMap<String, Vec<RecordBatch>> = HashMap::new();
    let mut fully_applied_scan_filters: HashMap<String, bool> = HashMap::new();
    let mut scan_seeds: HashMap<String, ScanProfileSeed> = HashMap::new();

    for table_ref in table_refs {
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

    let mut pipeline = if let Some(profile) = profile {
        let mut physical_ctx =
            PhysicalContext::new(engine, table_refs, &table_schemas, &table_metas, false);
        build_profiled_materialized_pipeline(
            plan,
            &mut physical_ctx,
            &table_batches,
            &scan_seeds,
            profile,
        )?
    } else {
        super::operators::build_pipeline(plan, &table_batches)?
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

fn resolve_in_subqueries(engine: &Engine, db: &str, stmt: &Statement) -> Result<Statement> {
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
        | Expr::IsNotNull(inner)
        | Expr::Exists(inner) => resolve_expr_in_subqueries(engine, db, inner)?,
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
        Expr::Subquery(subquery) => resolve_stmt_in_subqueries(engine, db, subquery)?,
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

    if having.is_some() || !group_by.is_empty() || table_names.len() != 1 || select_list.len() != 1 {
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
                || name.eq_ignore_ascii_case("max");
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
) -> Option<Vec<crate::sql::planner::AggregateExpr>> {
    match plan {
        LogicalPlan::Aggregate(agg) if agg.group_by.is_empty() && !agg.distinct => {
            // Restrict to COUNT/MIN/MAX to match detect_simple_metadata_aggregate.
            let all_supported = agg.aggregates.iter().all(|a| {
                !a.distinct
                    && matches!(
                        a.func,
                        crate::sql::planner::AggFunc::Count
                            | crate::sql::planner::AggFunc::Min
                            | crate::sql::planner::AggFunc::Max
                    )
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
            TableRef::Subquery { alias, .. } => vec![ResolvedTableRef {
                key: alias.clone(),
                database: default_db.to_string(),
                table: alias.clone(),
            }],
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
            TableRef::Subquery { alias, .. } => refs.push(ResolvedTableRef {
                key: alias.clone(),
                database: default_db.to_string(),
                table: alias.clone(),
            }),
        },
        FromClause::Join(inner) => collect_table_refs_from_join(inner, default_db, refs),
    }
    for jc in &join_expr.joins {
        match &jc.right {
            TableRef::Table { name, .. } => refs.push(ResolvedTableRef::new(name, default_db)),
            TableRef::Subquery { alias, .. } => refs.push(ResolvedTableRef {
                key: alias.clone(),
                database: default_db.to_string(),
                table: alias.clone(),
            }),
        }
    }
}
