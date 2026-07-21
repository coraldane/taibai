use super::*;
use crate::protocol::types::DataType;
use crate::sql::optimizer::Optimizer;
use crate::sql::parser::parser::Parser;
use crate::sql::planner::plan_select;
use crate::types::batch::{ColumnDef, Schema};
use std::collections::HashMap;

#[test]
fn pushes_topn_below_left_filter_before_dimension_joins() {
    let mut schemas = HashMap::new();
    schemas.insert(
        "fact_prices".to_string(),
        Schema::new(vec![
            ColumnDef::new("entity_id", DataType::VarChar),
            ColumnDef::new("bucket_id", DataType::Int64),
            ColumnDef::new("measure_a", DataType::Float64),
            ColumnDef::new("measure_b", DataType::Float64),
            ColumnDef::new("rank_score", DataType::Float64),
            ColumnDef::new("amount", DataType::Float64),
        ]),
    );
    schemas.insert(
        "dim_names".to_string(),
        Schema::new(vec![
            ColumnDef::new("bucket_id", DataType::Int64),
            ColumnDef::new("entity_id", DataType::VarChar),
            ColumnDef::new("name", DataType::VarChar),
        ]),
    );
    schemas.insert(
        "dim_profiles".to_string(),
        Schema::new(vec![
            ColumnDef::new("entity_id", DataType::VarChar),
            ColumnDef::new("group_name", DataType::VarChar),
            ColumnDef::new("scope_name", DataType::VarChar),
        ]),
    );
    schemas.insert(
        "fact_metrics".to_string(),
        Schema::new(vec![
            ColumnDef::new("bucket_id", DataType::Int64),
            ColumnDef::new("entity_id", DataType::VarChar),
            ColumnDef::new("metric_a", DataType::Float64),
            ColumnDef::new("metric_b", DataType::Float64),
        ]),
    );

    let stmt = Parser::parse(
        "SELECT f.entity_id, n.name,
                    f.measure_a, f.measure_b, f.rank_score, f.amount,
                    COALESCE(p.group_name, ''), COALESCE(p.scope_name, ''),
                    m.metric_a, m.metric_b
             FROM fact_prices f
             LEFT JOIN dim_names n ON n.bucket_id = f.bucket_id AND n.entity_id = f.entity_id
             LEFT JOIN dim_profiles p ON p.entity_id = f.entity_id
             LEFT JOIN fact_metrics m ON m.bucket_id = 42 AND f.entity_id = m.entity_id
             WHERE f.bucket_id = 42
             ORDER BY f.rank_score DESC
             LIMIT 20 OFFSET 80",
    )
    .unwrap()
    .remove(0);
    let plan = plan_select(&stmt, &schemas).unwrap();
    let optimized = Optimizer::new().optimize(plan);

    assert!(
        has_join_with_left_topn(&optimized, 100),
        "expected TopN(limit+offset) to be pushed below the residual left-side filter"
    );
}

fn has_join_with_left_topn(plan: &LogicalPlan, expected_limit: usize) -> bool {
    match plan {
        LogicalPlan::Join(join) => {
            leading_topn_limit(&join.left) == Some(expected_limit)
                || has_join_with_left_topn(&join.left, expected_limit)
                || has_join_with_left_topn(&join.right, expected_limit)
        }
        LogicalPlan::Filter(filter) => has_join_with_left_topn(&filter.input, expected_limit),
        LogicalPlan::Project(project) => has_join_with_left_topn(&project.input, expected_limit),
        LogicalPlan::Sort(sort) => has_join_with_left_topn(&sort.input, expected_limit),
        LogicalPlan::Limit(limit) => has_join_with_left_topn(&limit.input, expected_limit),
        LogicalPlan::TopN(topn) => has_join_with_left_topn(&topn.input, expected_limit),
        LogicalPlan::Aggregate(agg) => has_join_with_left_topn(&agg.input, expected_limit),
        LogicalPlan::Distinct(distinct) => has_join_with_left_topn(&distinct.input, expected_limit),
        LogicalPlan::Window(window) => has_join_with_left_topn(&window.input, expected_limit),
        LogicalPlan::SemiJoin(semi) => {
            has_join_with_left_topn(&semi.left, expected_limit)
                || has_join_with_left_topn(&semi.right, expected_limit)
        }
        LogicalPlan::Scan(_) | LogicalPlan::Empty => false,
    }
}

fn leading_topn_limit(plan: &LogicalPlan) -> Option<usize> {
    match plan {
        LogicalPlan::TopN(topn) if topn.offset == 0 => Some(topn.limit),
        LogicalPlan::Filter(filter) => leading_topn_limit(&filter.input),
        _ => None,
    }
}
