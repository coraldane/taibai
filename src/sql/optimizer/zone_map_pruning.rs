use crate::sql::parser::ast::BinOp;
use crate::sql::planner::*;

use super::Rule;

pub struct ZoneMapPruningRule;

impl Rule for ZoneMapPruningRule {
    fn apply(&self, plan: LogicalPlan) -> (LogicalPlan, bool) {
        apply_zone_map_pruning(plan)
    }
}

fn apply_zone_map_pruning(plan: LogicalPlan) -> (LogicalPlan, bool) {
    match plan {
        LogicalPlan::Filter(mut filter) => {
            let (input, changed) = apply_zone_map_pruning(*filter.input);
            filter.input = Box::new(input);

            let zone_maps = if let LogicalPlan::Scan(ref scan) = *filter.input {
                extract_zone_maps(&filter.predicate, &scan.schema)
            } else {
                Vec::new()
            };

            if !zone_maps.is_empty() {
                if let LogicalPlan::Scan(ref mut scan) = *filter.input {
                    let old_len = scan.zone_map_filters.len();
                    scan.zone_map_filters = zone_maps;
                    let new_len = scan.zone_map_filters.len();
                    return (LogicalPlan::Filter(filter), changed || new_len > old_len);
                }
            }

            (LogicalPlan::Filter(filter), changed)
        }
        LogicalPlan::Project(mut proj) => {
            let (input, changed) = apply_zone_map_pruning(*proj.input);
            proj.input = Box::new(input);
            (LogicalPlan::Project(proj), changed)
        }
        LogicalPlan::Sort(mut sort) => {
            let (input, changed) = apply_zone_map_pruning(*sort.input);
            sort.input = Box::new(input);
            (LogicalPlan::Sort(sort), changed)
        }
        LogicalPlan::Limit(mut limit) => {
            let (input, changed) = apply_zone_map_pruning(*limit.input);
            limit.input = Box::new(input);
            (LogicalPlan::Limit(limit), changed)
        }
        LogicalPlan::Aggregate(mut agg) => {
            let (input, changed) = apply_zone_map_pruning(*agg.input);
            agg.input = Box::new(input);
            (LogicalPlan::Aggregate(agg), changed)
        }
        other => (other, false),
    }
}

fn extract_zone_maps(
    expr: &LogicalExpr,
    schema: &crate::types::batch::Schema,
) -> Vec<ZoneMapFilter> {
    let mut filters = Vec::new();
    extract_zone_maps_recursive(expr, schema, &mut filters);
    filters
}

fn extract_zone_maps_recursive(
    expr: &LogicalExpr,
    schema: &crate::types::batch::Schema,
    filters: &mut Vec<ZoneMapFilter>,
) {
    if let LogicalExpr::BinaryOp { left, op, right } = expr {
        match op {
            BinOp::Eq | BinOp::Gt | BinOp::Gte | BinOp::Lt | BinOp::Lte => {
                if let LogicalExpr::Column(idx) = **left {
                    if let LogicalExpr::Literal(ref val) = **right {
                        if let Some(col_def) = schema.columns.get(idx) {
                            // Point filters for equality only: ZoneMapFilter
                            // has no way to express an unbounded side, and
                            // folding `a > 5` to [5, 5] turns a range into a
                            // point probe that would falsely exclude rows
                            // once anything consumes these filters (today it
                            // merely disqualifies the DISTINCT fast path).
                            if *op == BinOp::Eq {
                                filters.push(ZoneMapFilter {
                                    column: col_def.name.clone(),
                                    min_value: val.clone(),
                                    max_value: val.clone(),
                                });
                            }
                        }
                    }
                }
            }
            BinOp::And => {
                extract_zone_maps_recursive(left, schema, filters);
                extract_zone_maps_recursive(right, schema, filters);
            }
            _ => {}
        }
    }
}
