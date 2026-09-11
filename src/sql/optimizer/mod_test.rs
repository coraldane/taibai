use super::*;
use crate::protocol::types::DataType;
use crate::sql::parser::parser::Parser;
use crate::sql::planner::{plan_select, LogicalPlan};
use crate::types::batch::{ColumnDef, Schema};
use std::collections::HashMap;

fn schemas() -> HashMap<String, Schema> {
    let mut schemas = HashMap::new();
    schemas.insert(
        "left_t".to_string(),
        Schema::new(vec![
            ColumnDef::new("k", DataType::Int64),
            ColumnDef::new("p", DataType::Int64),
            ColumnDef::new("x", DataType::Float64),
            ColumnDef::new("y", DataType::Float64),
        ]),
    );
    schemas.insert(
        "dim_t".to_string(),
        Schema::new(vec![
            ColumnDef::new("k", DataType::Int64),
            ColumnDef::new("name", DataType::VarChar),
        ]),
    );
    schemas.insert(
        "right_t".to_string(),
        Schema::new(vec![
            ColumnDef::new("p", DataType::Int64),
            ColumnDef::new("k", DataType::Int64),
            ColumnDef::new("v", DataType::Float64),
        ]),
    );
    schemas
}

#[test]
fn optimizer_stabilizes_when_rule_reports_no_effective_change() {
    let plan = LogicalPlan::Empty;
    let optimized = Optimizer::new().optimize(plan.clone());
    assert_eq!(optimized, plan);
}

#[test]
fn optimizes_join_order_by_expression_limit_offset_query() {
    let sql = "SELECT l.k, d.name, r.v
            FROM left_t l
            LEFT JOIN dim_t d ON l.k = d.k
            LEFT JOIN right_t r ON r.p = 42 AND l.k = r.k
            WHERE l.p = 42
            ORDER BY (l.x - l.y) / l.y DESC
            LIMIT 20 OFFSET 0";
    let stmts = Parser::parse(sql).unwrap();
    let plan = plan_select(&stmts[0], &schemas()).unwrap();
    let optimized = Optimizer::new().optimize(plan);
    assert!(matches!(optimized, LogicalPlan::Project(_)));
}
