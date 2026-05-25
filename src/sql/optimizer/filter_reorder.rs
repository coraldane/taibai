/// Filter Reorder Rule - reorders AND-connected predicates by estimated selectivity.
/// Inspired by DuckDB's ExpressionHeuristics.
/// Order: equality conditions (sel ~0.1) > range conditions (sel ~0.33) > LIKE (sel ~0.5) > other
use crate::sql::parser::ast::BinOp;
use crate::sql::planner::LogicalExpr;
use crate::sql::planner::LogicalPlan;

use super::Rule;

pub struct FilterReorderRule;

impl Rule for FilterReorderRule {
    fn name(&self) -> &str {
        "filter_reorder"
    }

    fn apply(&self, plan: LogicalPlan) -> (LogicalPlan, bool) {
        reorder_filters(plan)
    }
}

fn reorder_filters(plan: LogicalPlan) -> (LogicalPlan, bool) {
    match plan {
        LogicalPlan::Filter(mut f) => {
            let (new_input, input_changed) = reorder_filters(*f.input);
            f.input = Box::new(new_input);
            let (new_pred, pred_changed) = reorder_predicate(f.predicate);
            f.predicate = new_pred;
            (LogicalPlan::Filter(f), input_changed || pred_changed)
        }
        LogicalPlan::Project(mut p) => {
            let (new_input, changed) = reorder_filters(*p.input);
            p.input = Box::new(new_input);
            (LogicalPlan::Project(p), changed)
        }
        LogicalPlan::Sort(mut s) => {
            let (new_input, changed) = reorder_filters(*s.input);
            s.input = Box::new(new_input);
            (LogicalPlan::Sort(s), changed)
        }
        LogicalPlan::Limit(mut l) => {
            let (new_input, changed) = reorder_filters(*l.input);
            l.input = Box::new(new_input);
            (LogicalPlan::Limit(l), changed)
        }
        LogicalPlan::Aggregate(mut a) => {
            let (new_input, changed) = reorder_filters(*a.input);
            a.input = Box::new(new_input);
            (LogicalPlan::Aggregate(a), changed)
        }
        LogicalPlan::Join(mut j) => {
            let (new_left, lc) = reorder_filters(*j.left);
            let (new_right, rc) = reorder_filters(*j.right);
            j.left = Box::new(new_left);
            j.right = Box::new(new_right);
            (LogicalPlan::Join(j), lc || rc)
        }
        _ => (plan, false),
    }
}

/// Reorder a predicate expression to put most selective conditions first.
fn reorder_predicate(expr: LogicalExpr) -> (LogicalExpr, bool) {
    // Flatten AND conditions
    let mut conditions = flatten_and(&expr);

    if conditions.len() <= 1 {
        return (expr, false);
    }

    // Sort by selectivity (lower = more selective = first)
    let original: Vec<_> = conditions.clone();
    conditions.sort_by_key(|c| estimate_selectivity(c));

    let changed = conditions != original;
    (rebuild_and(conditions), changed)
}

/// Flatten nested AND expressions into a list.
fn flatten_and(expr: &LogicalExpr) -> Vec<LogicalExpr> {
    match expr {
        LogicalExpr::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => {
            let mut result = flatten_and(left);
            result.extend(flatten_and(right));
            result
        }
        _ => vec![expr.clone()],
    }
}

/// Rebuild an AND expression from a list of conditions.
fn rebuild_and(mut conditions: Vec<LogicalExpr>) -> LogicalExpr {
    if conditions.is_empty() {
        return LogicalExpr::Literal(crate::types::scalar::ScalarValue::Bool(Some(true)));
    }
    conditions.reverse();
    let mut result = conditions.pop().unwrap();
    while let Some(cond) = conditions.pop() {
        result = LogicalExpr::BinaryOp {
            left: Box::new(result),
            op: BinOp::And,
            right: Box::new(cond),
        };
    }
    result
}

/// Estimate selectivity of a predicate (lower = more selective = runs first).
fn estimate_selectivity(expr: &LogicalExpr) -> u32 {
    match expr {
        LogicalExpr::BinaryOp { op, .. } => {
            match op {
                BinOp::Eq => 10, // Most selective
                BinOp::Neq => 50,
                BinOp::Lt | BinOp::Gt | BinOp::Lte | BinOp::Gte => 33,
                _ => 50,
            }
        }
        LogicalExpr::Like { .. } => 50, // LIKE is moderately selective
        LogicalExpr::In { .. } => 10,   // IN is highly selective
        LogicalExpr::Between { .. } => 33,
        LogicalExpr::IsNull(_) | LogicalExpr::IsNotNull(_) => 50,
        LogicalExpr::Not(_) => 60,
        _ => 100, // Unknown, put last
    }
}
