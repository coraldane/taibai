use std::collections::HashMap;

use crate::common::error::{MonaxError, Result};
use crate::protocol::types::DataType;
use crate::storage::segment::ScanStats;
use crate::types::batch::{ColumnDef, RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

use super::engine::{Engine, ExecResult};
use crate::sql::parser::ast::{Expr, FromClause, SelectItem, Statement};
use crate::sql::planner::{LogicalExpr, LogicalPlan};

use super::scan::spec::compile_scan_spec;
use super::scan::ScanExecutor;

pub fn exec_select(engine: &Engine, db: &str, stmt: &Statement) -> Result<ExecResult> {
    exec_select_inner(engine, db, stmt, None, None)
}

pub fn exec_select_with_plan(
    engine: &Engine,
    db: &str,
    stmt: &Statement,
    cached_plan: LogicalPlan,
) -> Result<ExecResult> {
    exec_select_inner(engine, db, stmt, None, Some(cached_plan))
}

pub fn exec_select_with_stats(
    engine: &Engine,
    db: &str,
    stmt: &Statement,
    stats: &mut HashMap<String, ScanStats>,
) -> Result<ExecResult> {
    exec_select_inner(engine, db, stmt, Some(stats), None)
}

fn exec_select_inner(
    engine: &Engine,
    db: &str,
    stmt: &Statement,
    mut stats: Option<&mut HashMap<String, ScanStats>>,
    cached_plan: Option<LogicalPlan>,
) -> Result<ExecResult> {
    let from = match stmt {
        Statement::Select { from, .. } => from,
        _ => {
            return Err(MonaxError::Internal(
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
            stats.as_deref_mut(),
            cached_plan,
        ),
    }
}

fn exec_literal_select(stmt: &Statement) -> Result<ExecResult> {
    let select_list = match stmt {
        Statement::Select { select_list, .. } => select_list,
        _ => unreachable!(),
    };
    let schema = Schema::new(vec![ColumnDef::new("1", DataType::Int64)]);
    let mut batch = RecordBatch::new(schema.clone());
    match &select_list[0] {
        SelectItem::Expr(expr, _) => {
            let val = super::helpers::eval_expr_to_scalar(expr, &DataType::Int64)?;
            batch.column_mut(0).unwrap().push(&val)?;
        }
        SelectItem::Star => {
            batch
                .column_mut(0)
                .unwrap()
                .push(&ScalarValue::Int64(Some(1)))?;
        }
    }
    Ok(ExecResult::Query {
        schema,
        batches: vec![batch],
    })
}

fn exec_table_select(
    engine: &Engine,
    db: &str,
    stmt: &Statement,
    from_clause: &FromClause,
    mut stats: Option<&mut HashMap<String, ScanStats>>,
    cached_plan: Option<LogicalPlan>,
) -> Result<ExecResult> {
    let table_names = extract_table_names(from_clause);

    let mut table_schemas = HashMap::new();
    let mut table_cluster_keys: HashMap<String, Vec<String>> = HashMap::new();
    let mut table_metas: HashMap<String, crate::storage::catalog::schema::TableMeta> =
        HashMap::new();
    {
        let catalogs = engine.catalogs().read().unwrap();
        let catalog = catalogs
            .get(db)
            .ok_or_else(|| MonaxError::Sql(format!("database '{}' does not exist", db)))?;

        for table_name in &table_names {
            let meta = catalog
                .get_table(table_name)
                .ok_or_else(|| {
                    MonaxError::Sql(format!("table '{}.{}' does not exist", db, table_name))
                })?
                .clone();
            table_schemas.insert(table_name.clone(), Schema::new(meta.columns.clone()));
            table_metas.insert(table_name.clone(), meta.clone());
            if !meta.cluster_key.is_empty() {
                table_cluster_keys.insert(table_name.clone(), meta.cluster_key.clone());
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

    let mut table_batches: HashMap<String, Vec<RecordBatch>> = HashMap::new();
    let mut fully_applied_scan_filters: HashMap<String, bool> = HashMap::new();

    for table_name in &table_names {
        let schema = table_schemas.get(table_name).unwrap().clone();
        let meta = table_metas.get(table_name).unwrap().clone();

        let mut spec = compile_scan_spec(&plan, table_name, &schema, &meta);

        if is_count_star {
            spec.is_count_star = true;
        }

        let mut executor = ScanExecutor::new(&spec, engine, schema, meta, db);
        if let Some(ref mut s) = stats {
            executor = executor.with_stats(s);
        }

        let batches = executor.execute()?;
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

        table_batches.insert(table_name.clone(), batches);
    }

    let plan = if table_names.len() == 1
        && fully_applied_scan_filters
            .get(&table_names[0])
            .copied()
            .unwrap_or(false)
    {
        remove_redundant_pushed_filter(plan, &table_names[0])
    } else {
        plan
    };

    let plan =
        try_skip_sort_for_cluster_key(plan, &table_batches, &table_cluster_keys, &table_schemas);

    let mut pipeline = super::operators::build_pipeline(plan, &table_batches)?;
    tracing::debug!("query: pipeline built, starting next_batch loop");
    let mut result_batches = Vec::new();
    let mut result_schema = None;
    while let Some(batch) = pipeline.next_batch()? {
        tracing::debug!("query: got batch with {} rows, schema columns={:?}", batch.num_rows(), batch.schema.columns.iter().map(|c| (c.name.clone(), c.data_type)).collect::<Vec<_>>());
        if result_schema.is_none() {
            result_schema = Some(batch.schema.clone());
        }
        result_batches.push(batch);
    }
    tracing::debug!("query: finished next_batch loop, {} batches", result_batches.len());

    let schema = result_schema.unwrap_or_else(|| Schema::empty());
    Ok(ExecResult::Query {
        schema,
        batches: result_batches,
    })
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
    let (select_list, group_by) = match stmt {
        Statement::Select {
            select_list,
            group_by,
            ..
        } => (select_list, group_by),
        _ => return false,
    };

    if !group_by.is_empty() || table_names.len() != 1 || select_list.len() != 1 {
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

pub fn extract_table_names(from: &FromClause) -> Vec<String> {
    match from {
        FromClause::Table(t) => vec![t.name.clone()],
        FromClause::Join(join_expr) => {
            let mut names = Vec::new();
            collect_table_names_from_join(join_expr, &mut names);
            names
        }
    }
}

fn collect_table_names_from_join(
    join_expr: &crate::sql::parser::ast::JoinExpr,
    names: &mut Vec<String>,
) {
    match &*join_expr.left {
        FromClause::Table(t) => names.push(t.name.clone()),
        FromClause::Join(inner) => collect_table_names_from_join(inner, names),
    }
    for jc in &join_expr.joins {
        names.push(jc.right.name.clone());
    }
}

use crate::sql::parser::ast::OrderByItem;

pub fn exec_union(
    engine: &Engine,
    db: &str,
    left: &Statement,
    right: &Statement,
    all: bool,
    order_by: &[OrderByItem],
    limit: &Option<Expr>,
    offset: &Option<Expr>,
) -> Result<ExecResult> {
    let left_result = exec_union_recursive(engine, db, left)?;
    let right_result = exec_union_recursive(engine, db, right)?;

    let left_batches = match left_result {
        ExecResult::Query { batches, .. } => batches,
        ExecResult::Ok { .. } => {
            return Err(MonaxError::Sql("UNION left side returned no data".into()))
        }
    };
    let right_batches = match right_result {
        ExecResult::Query { batches, .. } => batches,
        ExecResult::Ok { .. } => {
            return Err(MonaxError::Sql("UNION right side returned no data".into()))
        }
    };

    let schema = if !left_batches.is_empty() {
        left_batches[0].schema.clone()
    } else if !right_batches.is_empty() {
        right_batches[0].schema.clone()
    } else {
        return Err(MonaxError::Sql("UNION with empty result sets".into()));
    };

    let mut combined_batches = left_batches;
    combined_batches.extend(right_batches);

    if !all {
        combined_batches = deduplicate_batches(&combined_batches);
    }

    if !order_by.is_empty() {
        combined_batches = sort_batches(&combined_batches, order_by);
    }

    if let Some(offset_expr) = offset {
        let offset_val = super::helpers::eval_expr_to_int(offset_expr)?;
        if offset_val > 0 && (offset_val as usize) < combined_batches.len() {
            combined_batches = combined_batches
                .into_iter()
                .skip(offset_val as usize)
                .collect();
        } else if (offset_val as usize) >= combined_batches.len() {
            combined_batches = Vec::new();
        }
    }

    if let Some(limit_expr) = limit {
        let limit_val = super::helpers::eval_expr_to_int(limit_expr)?;
        if limit_val > 0 && limit_val < combined_batches.len() as i64 {
            combined_batches = combined_batches
                .into_iter()
                .take(limit_val as usize)
                .collect();
        }
    }

    Ok(ExecResult::Query {
        schema,
        batches: combined_batches,
    })
}

fn exec_union_recursive(engine: &Engine, db: &str, stmt: &Statement) -> Result<ExecResult> {
    match stmt {
        Statement::Union {
            left,
            right,
            all,
            order_by,
            limit,
            offset,
        } => exec_union(engine, db, left, right, *all, order_by, limit, offset),
        Statement::Select { .. } => exec_select(engine, db, stmt),
        _ => Err(MonaxError::Sql(format!(
            "UNION component must be SELECT, got {:?}",
            stmt
        ))),
    }
}

fn deduplicate_batches(batches: &[RecordBatch]) -> Vec<RecordBatch> {
    if batches.is_empty() {
        return Vec::new();
    }

    let mut seen_rows: std::collections::HashSet<Vec<String>> = std::collections::HashSet::new();
    let mut result_batches = Vec::new();
    let schema = batches[0].schema.clone();
    let mut current_batch = RecordBatch::new(schema.clone());

    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            let row_key = build_row_key(batch, row_idx);
            if seen_rows.insert(row_key) {
                for col_idx in 0..batch.num_columns() {
                    if let Some(col) = batch.column(col_idx) {
                        if let Some(dest_col) = current_batch.column_mut(col_idx) {
                            let val = col.scalar(row_idx);
                            let _ = dest_col.push(&val);
                        }
                    }
                }
                if current_batch.num_rows() >= 4096 {
                    result_batches.push(current_batch);
                    current_batch = RecordBatch::new(schema.clone());
                }
            }
        }
    }

    if current_batch.num_rows() > 0 {
        result_batches.push(current_batch);
    }

    result_batches
}

fn build_row_key(batch: &RecordBatch, row_idx: usize) -> Vec<String> {
    let mut key = Vec::with_capacity(batch.num_columns());
    for col_idx in 0..batch.num_columns() {
        if let Some(col) = batch.column(col_idx) {
            let val = col.scalar(row_idx);
            key.push(format!("{:?}", val));
        } else {
            key.push("NULL".to_string());
        }
    }
    key
}

fn sort_batches(batches: &[RecordBatch], order_by: &[OrderByItem]) -> Vec<RecordBatch> {
    if batches.is_empty() || order_by.is_empty() {
        return batches.to_vec();
    }

    let schema = batches[0].schema.clone();
    let mut all_rows: Vec<(usize, usize)> = Vec::new();

    for (batch_idx, batch) in batches.iter().enumerate() {
        for row_idx in 0..batch.num_rows() {
            all_rows.push((batch_idx, row_idx));
        }
    }

    all_rows.sort_by(|a, b| {
        for ob in order_by {
            let col_idx = match &ob.expr {
                Expr::Column(col_name) => schema.index_of(col_name).unwrap_or(0),
                Expr::QualifiedColumn { column, .. } => schema.index_of(column).unwrap_or(0),
                Expr::Literal(ScalarValue::Int64(Some(idx))) => *idx as usize,
                _ => 0,
            };

            let val_a = batches[a.0]
                .column(col_idx)
                .map(|c| c.scalar(a.1))
                .unwrap_or_else(|| ScalarValue::Int64(None));
            let val_b = batches[b.0]
                .column(col_idx)
                .map(|c| c.scalar(b.1))
                .unwrap_or_else(|| ScalarValue::Int64(None));

            let cmp = compare_scalar_values(&val_a, &val_b);
            if cmp != std::cmp::Ordering::Equal {
                return if ob.desc { cmp.reverse() } else { cmp };
            }
        }
        std::cmp::Ordering::Equal
    });

    let mut result_batch = RecordBatch::new(schema.clone());
    for (batch_idx, row_idx) in all_rows {
        for col_idx in 0..schema.num_columns() {
            if let Some(col) = batches[batch_idx].column(col_idx) {
                if let Some(dest_col) = result_batch.column_mut(col_idx) {
                    let val = col.scalar(row_idx);
                    let _ = dest_col.push(&val);
                }
            }
        }
        if result_batch.num_rows() >= 4096 {
            break;
        }
    }

    vec![result_batch]
}

fn compare_scalar_values(a: &ScalarValue, b: &ScalarValue) -> std::cmp::Ordering {
    match (a, b) {
        (ScalarValue::Int64(Some(va)), ScalarValue::Int64(Some(vb))) => va.cmp(&vb),
        (ScalarValue::Float64(Some(va)), ScalarValue::Float64(Some(vb))) => {
            va.partial_cmp(&vb).unwrap_or(std::cmp::Ordering::Equal)
        }
        (ScalarValue::VarChar(Some(va)), ScalarValue::VarChar(Some(vb))) => va.cmp(&vb),
        (ScalarValue::Timestamp(Some(va)), ScalarValue::Timestamp(Some(vb))) => va.cmp(&vb),
        (a_val, b_val) if a_val.is_null() && !b_val.is_null() => std::cmp::Ordering::Less,
        (a_val, b_val) if !a_val.is_null() && b_val.is_null() => std::cmp::Ordering::Greater,
        _ => std::cmp::Ordering::Equal,
    }
}
