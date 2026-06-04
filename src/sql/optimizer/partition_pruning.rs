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
                let partition_filter = extract_partition_filter(&filter.predicate, &scan.schema);

                let mut any_changed = changed;

                if let Some(tr) = time_range {
                    let is_new = scan.time_range.is_none();
                    scan.time_range = Some(tr);
                    any_changed = any_changed || is_new;
                }

                if let Some(pf) = partition_filter {
                    let is_new = scan.partition_filter.is_none();
                    scan.partition_filter = Some(pf);
                    any_changed = any_changed || is_new;
                }

                if any_changed != changed {
                    return (LogicalPlan::Filter(filter), any_changed);
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
        LogicalPlan::Join(mut join) => {
            let (left, left_changed) = apply_partition_pruning(*join.left);
            let (right, right_changed) = apply_partition_pruning(*join.right);
            join.left = Box::new(left);
            join.right = Box::new(right);
            (LogicalPlan::Join(join), left_changed || right_changed)
        }
        LogicalPlan::SemiJoin(mut semi) => {
            let (left, left_changed) = apply_partition_pruning(*semi.left);
            let (right, right_changed) = apply_partition_pruning(*semi.right);
            semi.left = Box::new(left);
            semi.right = Box::new(right);
            (LogicalPlan::SemiJoin(semi), left_changed || right_changed)
        }
        other => (other, false),
    }
}

fn extract_time_range(expr: &LogicalExpr, schema: &Schema) -> Option<TimeRange> {
    if let LogicalExpr::BinaryOp { left, op, right } = expr {
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
    None
}

fn extract_partition_filter(expr: &LogicalExpr, schema: &Schema) -> Option<PartitionFilter> {
    match expr {
        LogicalExpr::BinaryOp { left, op, right } => match op {
            BinOp::Eq => {
                if let LogicalExpr::Column(idx) = **left {
                    if let LogicalExpr::Literal(ref val) = **right {
                        let col_name = schema.columns.get(idx)?.name.clone();
                        return Some(PartitionFilter {
                            column: col_name,
                            values: vec![val.clone()],
                        });
                    }
                }
                if let LogicalExpr::Column(idx) = **right {
                    if let LogicalExpr::Literal(ref val) = **left {
                        let col_name = schema.columns.get(idx)?.name.clone();
                        return Some(PartitionFilter {
                            column: col_name,
                            values: vec![val.clone()],
                        });
                    }
                }
            }
            BinOp::And => {
                let left_filter = extract_partition_filter(left, schema);
                let right_filter = extract_partition_filter(right, schema);
                if let (Some(l), Some(r)) = (&left_filter, &right_filter) {
                    if l.column == r.column {
                        let merged_values: Vec<ScalarValue> = l
                            .values
                            .iter()
                            .filter(|v| r.values.contains(v))
                            .cloned()
                            .collect();
                        if !merged_values.is_empty() {
                            return Some(PartitionFilter {
                                column: l.column.clone(),
                                values: merged_values,
                            });
                        }
                    }
                }
                return left_filter.or(right_filter);
            }
            _ => {}
        },
        LogicalExpr::In {
            expr,
            values,
            negated: false,
        } => {
            if let LogicalExpr::Column(idx) = **expr {
                let col_name = schema.columns.get(idx)?.name.clone();
                return Some(PartitionFilter {
                    column: col_name,
                    values: values.clone(),
                });
            }
        }
        _ => {}
    }
    None
}

#[derive(Debug, Clone, PartialEq)]
pub struct PartitionFilter {
    pub column: String,
    pub values: Vec<ScalarValue>,
}
