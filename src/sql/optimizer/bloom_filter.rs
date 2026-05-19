use crate::sql::parser::ast::BinOp;
use crate::sql::planner::*;
use crate::types::batch::Schema;

use super::Rule;

pub struct BloomFilterRule;

impl Rule for BloomFilterRule {
    fn name(&self) -> &str {
        "bloom_filter"
    }

    fn apply(&self, plan: LogicalPlan) -> (LogicalPlan, bool) {
        apply_bloom_filter(plan)
    }
}

fn apply_bloom_filter(plan: LogicalPlan) -> (LogicalPlan, bool) {
    match plan {
        LogicalPlan::Filter(mut filter) => {
            let (input, changed) = apply_bloom_filter(*filter.input);
            filter.input = Box::new(input);

            if let LogicalPlan::Scan(ref mut scan) = *filter.input {
                let bloom_cols = extract_bloom_columns(&filter.predicate, &scan.schema);
                if !bloom_cols.is_empty() && bloom_cols != scan.bloom_filter_columns {
                    scan.bloom_filter_columns = bloom_cols;
                    return (LogicalPlan::Filter(filter), true);
                }
            }

            (LogicalPlan::Filter(filter), changed)
        }
        LogicalPlan::Project(mut proj) => {
            let (input, changed) = apply_bloom_filter(*proj.input);
            proj.input = Box::new(input);
            (LogicalPlan::Project(proj), changed)
        }
        LogicalPlan::Sort(mut sort) => {
            let (input, changed) = apply_bloom_filter(*sort.input);
            sort.input = Box::new(input);
            (LogicalPlan::Sort(sort), changed)
        }
        LogicalPlan::Limit(mut limit) => {
            let (input, changed) = apply_bloom_filter(*limit.input);
            limit.input = Box::new(input);
            (LogicalPlan::Limit(limit), changed)
        }
        LogicalPlan::Aggregate(mut agg) => {
            let (input, changed) = apply_bloom_filter(*agg.input);
            agg.input = Box::new(input);
            (LogicalPlan::Aggregate(agg), changed)
        }
        other => (other, false),
    }
}

fn extract_bloom_columns(expr: &LogicalExpr, schema: &Schema) -> Vec<String> {
    let mut cols = Vec::new();
    extract_bloom_recursive(expr, schema, &mut cols);
    cols.sort();
    cols.dedup();
    cols
}

fn extract_bloom_recursive(expr: &LogicalExpr, schema: &Schema, cols: &mut Vec<String>) {
    match expr {
        LogicalExpr::BinaryOp { left, op, .. } => {
            if *op == BinOp::Eq {
                if let LogicalExpr::Column(idx) = **left {
                    if let Some(col_def) = schema.columns.get(idx) {
                        cols.push(col_def.name.clone());
                    }
                }
            }
            if *op == BinOp::And {
                extract_bloom_recursive(left, schema, cols);
            }
        }
        _ => {}
    }
}
