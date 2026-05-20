/// Subquery Optimizer - optimizes subqueries by evaluating them early and
/// replacing with literal values where possible.
/// Inspired by taibai's SubqueryOptimizer and DuckDB's InClauseRewriter.
use crate::sql::planner::{LogicalPlan, LogicalExpr};

use super::Rule;

pub struct SubqueryRule;

impl Rule for SubqueryRule {
    fn name(&self) -> &str {
        "subquery"
    }

    fn apply(&self, plan: LogicalPlan) -> (LogicalPlan, bool) {
        // In a real implementation, this would:
        // 1. Find scalar subqueries in expressions
        // 2. Execute them to get their values
        // 3. Replace the subquery with a Literal expression
        // 4. Transform IN (subquery) → IN (literal_values)
        // 5. Transform EXISTS (subquery) → true/false based on result
        //
        // For now, this is a placeholder that passes through the plan unchanged.
        (plan, false)
    }
}

/// Placeholder for subquery replacement logic.
/// In a full implementation, this would recursively walk expressions
/// and replace Subquery/Exists expressions with evaluated results.
#[allow(dead_code)]
fn replace_subqueries(expr: LogicalExpr) -> (LogicalExpr, bool) {
    match expr {
        LogicalExpr::BinaryOp { left, op, right } => {
            let (new_left, lc) = replace_subqueries(*left);
            let (new_right, rc) = replace_subqueries(*right);
            (LogicalExpr::BinaryOp {
                left: Box::new(new_left),
                op,
                right: Box::new(new_right),
            }, lc || rc)
        }
        LogicalExpr::Not(inner) => {
            let (new_inner, changed) = replace_subqueries(*inner);
            (LogicalExpr::Not(Box::new(new_inner)), changed)
        }
        _ => (expr, false),
    }
}
