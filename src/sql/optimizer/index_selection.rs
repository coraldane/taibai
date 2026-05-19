use crate::sql::parser::ast::BinOp;
use crate::sql::planner::*;
use crate::types::batch::Schema;

use super::Rule;

pub struct IndexSelectionRule;

impl Rule for IndexSelectionRule {
    fn name(&self) -> &str {
        "index_selection"
    }

    fn apply(&self, plan: LogicalPlan) -> (LogicalPlan, bool) {
        apply_index_selection(plan)
    }
}

fn apply_index_selection(plan: LogicalPlan) -> (LogicalPlan, bool) {
    match plan {
        LogicalPlan::Filter(mut filter) => {
            let (input, changed) = apply_index_selection(*filter.input);
            filter.input = Box::new(input);

            if let LogicalPlan::Scan(ref mut scan) = *filter.input {
                let schema = &scan.schema;
                let index_cols = extract_indexable_columns(&filter.predicate, schema);
                if !index_cols.is_empty() && index_cols != scan.index_columns {
                    scan.index_columns = index_cols;
                    return (LogicalPlan::Filter(filter), true);
                }
            }

            (LogicalPlan::Filter(filter), changed)
        }
        LogicalPlan::Project(mut proj) => {
            let (input, changed) = apply_index_selection(*proj.input);
            proj.input = Box::new(input);
            (LogicalPlan::Project(proj), changed)
        }
        LogicalPlan::Sort(mut sort) => {
            let (input, changed) = apply_index_selection(*sort.input);
            sort.input = Box::new(input);
            (LogicalPlan::Sort(sort), changed)
        }
        LogicalPlan::Limit(mut limit) => {
            let (input, changed) = apply_index_selection(*limit.input);
            limit.input = Box::new(input);
            (LogicalPlan::Limit(limit), changed)
        }
        LogicalPlan::Aggregate(mut agg) => {
            let (input, changed) = apply_index_selection(*agg.input);
            agg.input = Box::new(input);
            (LogicalPlan::Aggregate(agg), changed)
        }
        other => (other, false),
    }
}

fn extract_indexable_columns(expr: &LogicalExpr, schema: &Schema) -> Vec<String> {
    let mut cols = Vec::new();
    extract_indexable_recursive(expr, schema, &mut cols);
    cols.sort();
    cols.dedup();
    cols
}

fn extract_indexable_recursive(expr: &LogicalExpr, schema: &Schema, cols: &mut Vec<String>) {
    match expr {
        LogicalExpr::BinaryOp { left, op, right } => {
            match op {
                BinOp::Eq | BinOp::Gt | BinOp::Gte | BinOp::Lt | BinOp::Lte => {
                    if let LogicalExpr::Column(idx) = **left {
                        if let Some(col_def) = schema.columns.get(idx) {
                            cols.push(col_def.name.clone());
                        }
                    }
                }
                BinOp::And => {
                    extract_indexable_recursive(left, schema, cols);
                    extract_indexable_recursive(right, schema, cols);
                }
                _ => {}
            }
        }
        _ => {}
    }
}
