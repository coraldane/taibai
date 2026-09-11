use super::*;
use crate::protocol::types::DataType;
use crate::sql::parser::parser::Parser;
use crate::sql::planner::plan_select;
use crate::types::batch::{ColumnDef, Schema};
use std::collections::HashMap;

#[test]
fn predicate_pushdown_keeps_row_filter() {
    let schema = Schema::new(vec![
        ColumnDef::new("name", DataType::VarChar),
        ColumnDef::new("region_id", DataType::Int32),
    ]);
    let mut schemas = HashMap::new();
    schemas.insert("test_table".to_string(), schema);

    let stmt = Parser::parse(
        "SELECT name FROM test_table \
             WHERE name = 'alice' AND region_id = 20251111",
    )
    .unwrap()
    .remove(0);
    let plan = plan_select(&stmt, &schemas).unwrap();
    let (optimized, changed) = PredicatePushdownRule.apply(plan);

    assert!(changed);
    let LogicalPlan::Project(project) = optimized else {
        panic!("expected project root");
    };
    let LogicalPlan::Filter(filter) = *project.input else {
        panic!("predicate pushdown must preserve the row-level filter");
    };
    let LogicalPlan::Scan(scan) = *filter.input else {
        panic!("expected filter input to be scan");
    };
    assert!(scan.pushed_filter.is_some());
}

#[test]
fn pushes_left_where_and_right_on_filters_for_left_join() {
    let left = Schema::new(vec![
        ColumnDef::new("k", DataType::VarChar),
        ColumnDef::new("p", DataType::Int32),
        ColumnDef::new("x", DataType::Float64),
        ColumnDef::new("y", DataType::Float64),
    ]);
    let right = Schema::new(vec![
        ColumnDef::new("k", DataType::VarChar),
        ColumnDef::new("p", DataType::Int32),
        ColumnDef::new("v", DataType::Float64),
    ]);
    let mut schemas = HashMap::new();
    schemas.insert("left_t".to_string(), left);
    schemas.insert("right_t".to_string(), right);

    let stmt = Parser::parse(
        "SELECT l.k, r.v FROM left_t l \
             LEFT JOIN right_t r ON r.p = 42 AND l.k = r.k \
             WHERE l.p = 42 \
             ORDER BY (l.x - l.y) / l.y DESC LIMIT 20",
    )
    .unwrap()
    .remove(0);
    let plan = plan_select(&stmt, &schemas).unwrap();
    let (optimized, changed) = PredicatePushdownRule.apply(plan);
    assert!(changed);
    let mut scans = Vec::new();
    collect_scans(&optimized, &mut scans);
    assert_eq!(scans.len(), 2);
    let left_scan = scans.iter().find(|s| s.table_name == "left_t").unwrap();
    let right_scan = scans.iter().find(|s| s.table_name == "right_t").unwrap();
    assert!(left_scan.pushed_filter.is_some());
    assert!(right_scan.pushed_filter.is_some());
}

#[test]
fn merges_stacked_filters_before_pushing_into_join_scan() {
    let left = Schema::new(vec![
        ColumnDef::new("k", DataType::VarChar),
        ColumnDef::new("p", DataType::Int32),
    ]);
    let right = Schema::new(vec![
        ColumnDef::new("k", DataType::VarChar),
        ColumnDef::new("p", DataType::Int32),
        ColumnDef::new("v", DataType::VarChar),
    ]);
    let mut schemas = HashMap::new();
    schemas.insert("left_t".to_string(), left);
    schemas.insert("right_t".to_string(), right);

    let stmt = Parser::parse(
        "SELECT l.k, r.v FROM left_t l \
             LEFT JOIN right_t r ON r.p = 42 AND l.k = r.k \
             WHERE l.p = 42 AND l.k = 'k1'",
    )
    .unwrap()
    .remove(0);
    let plan = plan_select(&stmt, &schemas).unwrap();
    let (optimized, changed) = PredicatePushdownRule.apply(plan);
    assert!(changed);

    let mut scans = Vec::new();
    collect_scans(&optimized, &mut scans);
    let right_scan = scans.iter().find(|s| s.table_name == "right_t").unwrap();
    let pushed = right_scan
        .pushed_filter
        .as_ref()
        .expect("right scan should have pushed filters");
    assert!(has_eq(
        pushed,
        0,
        &ScalarValue::VarChar(Some("k1".to_string()))
    ));
    assert!(has_eq(pushed, 1, &ScalarValue::Int64(Some(42))));
}

#[test]
fn propagates_left_filter_across_left_join_equality_to_right_scan() {
    let left = Schema::new(vec![
        ColumnDef::new("k", DataType::Int64),
        ColumnDef::new("p", DataType::Int64),
        ColumnDef::new("score", DataType::Float64),
    ]);
    let right = Schema::new(vec![
        ColumnDef::new("p", DataType::Int64),
        ColumnDef::new("k", DataType::Int64),
        ColumnDef::new("label", DataType::VarChar),
    ]);
    let mut schemas = HashMap::new();
    schemas.insert("left_t".to_string(), left);
    schemas.insert("right_t".to_string(), right);

    let stmt = Parser::parse(
        "SELECT l.k, r.label FROM left_t l \
             LEFT JOIN right_t r ON r.p = l.p AND r.k = l.k \
             WHERE l.p = 42 \
             ORDER BY l.score DESC LIMIT 20",
    )
    .unwrap()
    .remove(0);
    let plan = plan_select(&stmt, &schemas).unwrap();
    let (optimized, changed) = PredicatePushdownRule.apply(plan);
    assert!(changed);
    let mut scans = Vec::new();
    collect_scans(&optimized, &mut scans);
    assert_eq!(scans.len(), 2);
    let right_scan = scans.iter().find(|s| s.table_name == "right_t").unwrap();
    assert!(right_scan.pushed_filter.is_some());
}

fn collect_scans<'a>(plan: &'a LogicalPlan, scans: &mut Vec<&'a ScanPlan>) {
    match plan {
        LogicalPlan::Scan(scan) => scans.push(scan),
        LogicalPlan::Filter(f) => collect_scans(&f.input, scans),
        LogicalPlan::Project(p) => collect_scans(&p.input, scans),
        LogicalPlan::Sort(s) => collect_scans(&s.input, scans),
        LogicalPlan::Limit(l) => collect_scans(&l.input, scans),
        LogicalPlan::TopN(t) => collect_scans(&t.input, scans),
        LogicalPlan::Join(j) => {
            collect_scans(&j.left, scans);
            collect_scans(&j.right, scans);
        }
        _ => {}
    }
}

fn has_eq(expr: &LogicalExpr, column: usize, value: &ScalarValue) -> bool {
    match expr {
        LogicalExpr::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => has_eq(left, column, value) || has_eq(right, column, value),
        LogicalExpr::BinaryOp {
            left,
            op: BinOp::Eq,
            right,
        } => {
            matches!(
                (&**left, &**right),
                (LogicalExpr::Column(idx), LogicalExpr::Literal(lit))
                    if *idx == column && lit == value
            ) || matches!(
                (&**left, &**right),
                (LogicalExpr::Literal(lit), LogicalExpr::Column(idx))
                    if *idx == column && lit == value
            )
        }
        _ => false,
    }
}
