use std::collections::HashMap;

use crate::common::error::{MonaxError, Result};
use crate::protocol::types::DataType;
use crate::storage::segment::{RangeClause, RangeOp, RowGroupPredicate, ScanStats};
use crate::storage::segment_manager::SegmentManager;
use crate::types::batch::{ColumnDef, RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

use super::engine::{Engine, ExecResult};
use crate::sql::parser::ast::{BinOp, FromClause, SelectItem, Statement};
use crate::sql::planner::{LogicalExpr, LogicalPlan, ScanPlan};

pub fn exec_select(engine: &Engine, db: &str, stmt: &Statement) -> Result<ExecResult> {
    exec_select_inner(engine, db, stmt, None)
}

/// Same as [exec_select], but populates per-table [ScanStats] in `stats`.
/// Used by EXPLAIN ANALYZE.
pub fn exec_select_with_stats(
    engine: &Engine,
    db: &str,
    stmt: &Statement,
    stats: &mut HashMap<String, ScanStats>,
) -> Result<ExecResult> {
    exec_select_inner(engine, db, stmt, Some(stats))
}

fn exec_select_inner(
    engine: &Engine,
    db: &str,
    stmt: &Statement,
    mut stats: Option<&mut HashMap<String, ScanStats>>,
) -> Result<ExecResult> {
    let from = match stmt {
        Statement::Select { from, .. } => from,
        _ => return Err(MonaxError::Internal("exec_select called with non-SELECT".into())),
    };

    match from {
        None => exec_literal_select(stmt),
        Some(from_clause) => exec_table_select(engine, db, stmt, from_clause, stats.as_deref_mut()),
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
            batch.column_mut(0).unwrap().push(&ScalarValue::Int64(Some(1)))?;
        }
    }
    Ok(ExecResult::Query { schema, batches: vec![batch] })
}

fn exec_table_select(
    engine: &Engine,
    db: &str,
    stmt: &Statement,
    from_clause: &FromClause,
    mut stats: Option<&mut HashMap<String, ScanStats>>,
) -> Result<ExecResult> {
    let table_names = extract_table_names(from_clause);

    let mut table_schemas = HashMap::new();
    {
        let catalogs = engine.catalogs().read().unwrap();
        let catalog = catalogs.get(db)
            .ok_or_else(|| MonaxError::Sql(format!("database '{}' does not exist", db)))?;

        for table_name in &table_names {
            let meta = catalog.get_table(table_name)
                .ok_or_else(|| MonaxError::Sql(format!("table '{}.{}' does not exist", db, table_name)))?
                .clone();
            table_schemas.insert(table_name.clone(), Schema::new(meta.columns.clone()));
        }
    }

    let plan = crate::sql::planner::plan_select(stmt, &table_schemas)?;
    let plan = crate::sql::optimizer::optimize(plan);

    let mut scan_predicates: HashMap<String, RowGroupPredicate> = HashMap::new();
    collect_scan_predicates(&plan, &mut scan_predicates);

    let mut table_batches: HashMap<String, Vec<RecordBatch>> = HashMap::new();
    let mgr = engine.segment_mgr();
    for table_name in &table_names {
        let table_dir = engine.data_dir().join(db).join(table_name);
        let mut batches = Vec::new();
        if table_dir.exists() {
            let segment_files = SegmentManager::discover_segments(&table_dir)?;
            let predicate = scan_predicates.get(table_name);
            let pred_ref = predicate.filter(|p| !p.is_empty());
            if let Some(p) = pred_ref {
                tracing::debug!(
                    table = %table_name,
                    clauses = p.clauses.len(),
                    "applying zone-map pruning to table scan"
                );
            }
            for seg_path in segment_files {
                let reader = mgr.open_reader(&seg_path)?;
                let table_stats = stats.as_deref_mut()
                    .map(|s| s.entry(table_name.clone()).or_default());
                batches.extend(reader.scan_with_predicate_and_stats(4096, pred_ref, table_stats)?);
            }
        }
        table_batches.insert(table_name.clone(), batches);
    }

    let mut pipeline = super::operators::build_pipeline(plan, &table_batches)?;
    let mut result_batches = Vec::new();
    let mut result_schema = None;
    while let Some(batch) = pipeline.next_batch()? {
        if result_schema.is_none() {
            result_schema = Some(batch.schema.clone());
        }
        result_batches.push(batch);
    }

    let schema = result_schema.unwrap_or_else(|| Schema::empty());
    Ok(ExecResult::Query { schema, batches: result_batches })
}

fn extract_table_names(from: &FromClause) -> Vec<String> {
    match from {
        FromClause::Table(t) => vec![t.name.clone()],
        FromClause::Join(join_expr) => {
            let mut names = Vec::new();
            collect_table_names_from_join(join_expr, &mut names);
            names
        }
    }
}

fn collect_table_names_from_join(join_expr: &crate::sql::parser::ast::JoinExpr, names: &mut Vec<String>) {
    match &*join_expr.left {
        FromClause::Table(t) => names.push(t.name.clone()),
        FromClause::Join(inner) => collect_table_names_from_join(inner, names),
    }
    for jc in &join_expr.joins {
        names.push(jc.right.name.clone());
    }
}

fn collect_scan_predicates(
    plan: &LogicalPlan,
    out: &mut HashMap<String, RowGroupPredicate>,
) {
    match plan {
        LogicalPlan::Scan(scan) => {
            if let Some(filter) = &scan.pushed_filter {
                let mut clauses = Vec::new();
                extract_range_clauses(filter, scan, &mut clauses);
                if !clauses.is_empty() {
                    out.entry(scan.table_name.clone())
                        .and_modify(|p| p.clauses.extend(clauses.clone()))
                        .or_insert_with(|| RowGroupPredicate::new(clauses));
                }
            }
        }
        LogicalPlan::Filter(f) => collect_scan_predicates(&f.input, out),
        LogicalPlan::Project(p) => collect_scan_predicates(&p.input, out),
        LogicalPlan::Aggregate(a) => collect_scan_predicates(&a.input, out),
        LogicalPlan::Sort(s) => collect_scan_predicates(&s.input, out),
        LogicalPlan::Limit(l) => collect_scan_predicates(&l.input, out),
        LogicalPlan::TopN(t) => collect_scan_predicates(&t.input, out),
        LogicalPlan::Distinct(d) => collect_scan_predicates(&d.input, out),
        LogicalPlan::Window(w) => collect_scan_predicates(&w.input, out),
        LogicalPlan::Join(j) => {
            collect_scan_predicates(&j.left, out);
            collect_scan_predicates(&j.right, out);
        }
        LogicalPlan::Empty => {}
    }
}

fn extract_range_clauses(
    expr: &LogicalExpr,
    scan: &ScanPlan,
    out: &mut Vec<RangeClause>,
) {
    match expr {
        LogicalExpr::BinaryOp { left, op, right } => match op {
            BinOp::And => {
                extract_range_clauses(left, scan, out);
                extract_range_clauses(right, scan, out);
            }
            BinOp::Eq | BinOp::Gt | BinOp::Gte | BinOp::Lt | BinOp::Lte => {
                if let Some(c) = try_build_clause(left, *op, right) {
                    out.push(c);
                } else if let Some(c) = try_build_clause_mirrored(left, *op, right) {
                    out.push(c);
                }
            }
            _ => {}
        },
        LogicalExpr::Between { expr, low, high, negated: false } => {
            if let (LogicalExpr::Column(idx), LogicalExpr::Literal(lo), LogicalExpr::Literal(hi)) =
                (&**expr, &**low, &**high)
            {
                out.push(RangeClause { column_index: *idx, op: RangeOp::Ge, value: lo.clone() });
                out.push(RangeClause { column_index: *idx, op: RangeOp::Le, value: hi.clone() });
            }
        }
        _ => {}
    }
}

fn try_build_clause(
    left: &LogicalExpr,
    op: BinOp,
    right: &LogicalExpr,
) -> Option<RangeClause> {
    if let (LogicalExpr::Column(idx), LogicalExpr::Literal(val)) = (left, right) {
        let rop = match op {
            BinOp::Eq => RangeOp::Eq,
            BinOp::Lt => RangeOp::Lt,
            BinOp::Lte => RangeOp::Le,
            BinOp::Gt => RangeOp::Gt,
            BinOp::Gte => RangeOp::Ge,
            _ => return None,
        };
        return Some(RangeClause { column_index: *idx, op: rop, value: val.clone() });
    }
    None
}

fn try_build_clause_mirrored(
    left: &LogicalExpr,
    op: BinOp,
    right: &LogicalExpr,
) -> Option<RangeClause> {
    if let (LogicalExpr::Literal(val), LogicalExpr::Column(idx)) = (left, right) {
        // Literal <op> Column  ===  Column <mirrored_op> Literal
        let rop = match op {
            BinOp::Eq => RangeOp::Eq,
            BinOp::Lt => RangeOp::Gt,
            BinOp::Lte => RangeOp::Ge,
            BinOp::Gt => RangeOp::Lt,
            BinOp::Gte => RangeOp::Le,
            _ => return None,
        };
        return Some(RangeClause { column_index: *idx, op: rop, value: val.clone() });
    }
    None
}