use crate::sql::parser::ast::BinOp;
use crate::sql::planner::*;
use crate::types::batch::Schema;
use crate::types::scalar::ScalarValue;

use super::Rule;

pub struct PartitionPruningRule;

impl Rule for PartitionPruningRule {
    fn name(&self) -> &str {
        "partition_pruning"
    }

    fn apply(&self, plan: LogicalPlan) -> (LogicalPlan, bool) {
        apply_partition_pruning(plan)
    }
}

fn apply_partition_pruning(plan: LogicalPlan) -> (LogicalPlan, bool) {
    match plan {
        LogicalPlan::Filter(mut filter) => {
            let (input, changed) = apply_partition_pruning(*filter.input);
            filter.input = Box::new(input);

            if let LogicalPlan::Scan(ref mut scan) = *filter.input {
                let time_range = extract_time_range(&filter.predicate, &scan.schema);
                if let Some(tr) = time_range {
                    let is_new = scan.time_range.is_none();
                    scan.time_range = Some(tr);
                    return (LogicalPlan::Filter(filter), changed || is_new);
                }
            }

            (LogicalPlan::Filter(filter), changed)
        }
        LogicalPlan::Project(mut proj) => {
            let (input, changed) = apply_partition_pruning(*proj.input);
            proj.input = Box::new(input);
            (LogicalPlan::Project(proj), changed)
        }
        LogicalPlan::Sort(mut sort) => {
            let (input, changed) = apply_partition_pruning(*sort.input);
            sort.input = Box::new(input);
            (LogicalPlan::Sort(sort), changed)
        }
        LogicalPlan::Limit(mut limit) => {
            let (input, changed) = apply_partition_pruning(*limit.input);
            limit.input = Box::new(input);
            (LogicalPlan::Limit(limit), changed)
        }
        LogicalPlan::Aggregate(mut agg) => {
            let (input, changed) = apply_partition_pruning(*agg.input);
            agg.input = Box::new(input);
            (LogicalPlan::Aggregate(agg), changed)
        }
        other => (other, false),
    }
}

fn extract_time_range(expr: &LogicalExpr, schema: &Schema) -> Option<TimeRange> {
    match expr {
        LogicalExpr::BinaryOp { left, op, right } => {
            match op {
                BinOp::Gte | BinOp::Gt => {
                    if let LogicalExpr::Column(idx) = **left {
                        if let LogicalExpr::Literal(ref val) = **right {
                            let col_name = schema.columns.get(idx)?.name.clone();
                            return Some(TimeRange {
                                column: col_name,
                                start: Some(val.clone()),
                                end: None,
                            });
                        }
                    }
                }
                BinOp::Lte | BinOp::Lt => {
                    if let LogicalExpr::Column(idx) = **left {
                        if let LogicalExpr::Literal(ref val) = **right {
                            let col_name = schema.columns.get(idx)?.name.clone();
                            return Some(TimeRange {
                                column: col_name,
                                start: None,
                                end: Some(val.clone()),
                            });
                        }
                    }
                }
                BinOp::And => {
                    let left_range = extract_time_range(left, schema);
                    let right_range = extract_time_range(right, schema);
                    if let (Some(l), Some(r)) = (&left_range, &right_range) {
                        if l.column == r.column {
                            return Some(TimeRange {
                                column: l.column.clone(),
                                start: l.start.as_ref().or(r.start.as_ref()).cloned(),
                                end: r.end.as_ref().or(l.end.as_ref()).cloned(),
                            });
                        }
                    }
                    return left_range.or(right_range);
                }
                _ => {}
            }
        }
        _ => {}
    }
    None
}
