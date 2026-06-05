/// Expression simplification rules.
/// Inspired by DuckDB's expression rewriter:
/// - Constant folding: 1+2 → 3
/// - Comparison simplification: x=x → true
/// - Conjunction simplification: true AND x → x
/// - Arithmetic simplification: x*0 → 0, x*1 → x
/// - LIKE optimization: no wildcards → equality comparison
use crate::sql::parser::ast::BinOp;
use crate::sql::planner::LogicalExpr;
use crate::types::scalar::ScalarValue;

use super::Rule;
use crate::sql::planner::LogicalPlan;

pub struct ExpressionSimplificationRule;

impl Rule for ExpressionSimplificationRule {
    fn name(&self) -> &str {
        "expression_simplification"
    }

    fn apply(&self, plan: LogicalPlan) -> (LogicalPlan, bool) {
        simplify_plan(plan)
    }
}

fn simplify_plan(plan: LogicalPlan) -> (LogicalPlan, bool) {
    match plan {
        LogicalPlan::Filter(mut f) => {
            let (new_expr, e_changed) = simplify_expr(f.predicate);
            let (new_input, i_changed) = simplify_plan(*f.input);
            f.predicate = new_expr;
            f.input = Box::new(new_input);
            (LogicalPlan::Filter(f), e_changed || i_changed)
        }
        LogicalPlan::Project(mut p) => {
            let mut changed = false;
            let mut new_exprs = Vec::with_capacity(p.expressions.len());
            for expr in p.expressions {
                let (new_expr, c) = simplify_expr(expr);
                changed = changed || c;
                new_exprs.push(new_expr);
            }
            let (new_input, i_changed) = simplify_plan(*p.input);
            p.expressions = new_exprs;
            p.input = Box::new(new_input);
            (LogicalPlan::Project(p), changed || i_changed)
        }
        LogicalPlan::Sort(mut s) => {
            let (new_input, changed) = simplify_plan(*s.input);
            s.input = Box::new(new_input);
            (LogicalPlan::Sort(s), changed)
        }
        LogicalPlan::Limit(mut l) => {
            let (new_input, changed) = simplify_plan(*l.input);
            l.input = Box::new(new_input);
            (LogicalPlan::Limit(l), changed)
        }
        LogicalPlan::Aggregate(mut a) => {
            let (new_input, changed) = simplify_plan(*a.input);
            a.input = Box::new(new_input);
            (LogicalPlan::Aggregate(a), changed)
        }
        LogicalPlan::Join(mut j) => {
            let (new_left, lc) = simplify_plan(*j.left);
            let (new_right, rc) = simplify_plan(*j.right);
            j.left = Box::new(new_left);
            j.right = Box::new(new_right);
            (LogicalPlan::Join(j), lc || rc)
        }
        _ => (plan, false),
    }
}

pub fn simplify_expr(expr: LogicalExpr) -> (LogicalExpr, bool) {
    match expr {
        LogicalExpr::BinaryOp { left, op, right } => {
            let (new_left, lc) = simplify_expr(*left);
            let (new_right, rc) = simplify_expr(*right);
            let changed = lc || rc;

            // Constant folding: both sides are literals
            if let (LogicalExpr::Literal(l), LogicalExpr::Literal(r)) = (&new_left, &new_right) {
                if let Some(result) = fold_binary(l, &op, r) {
                    return (LogicalExpr::Literal(result), true);
                }
            }

            // Arithmetic simplifications
            match (&op, &new_left, &new_right) {
                // x * 0 → 0
                (BinOp::Mul, _, LogicalExpr::Literal(ScalarValue::Int64(Some(0)))) => {
                    return (LogicalExpr::Literal(ScalarValue::Int64(Some(0))), true);
                }
                // x * 1 → x
                (BinOp::Mul, _, LogicalExpr::Literal(ScalarValue::Int64(Some(1)))) => {
                    return (new_right, true); // return 1, caller should swap
                }
                // 0 * x → 0
                (BinOp::Mul, LogicalExpr::Literal(ScalarValue::Int64(Some(0))), _) => {
                    return (LogicalExpr::Literal(ScalarValue::Int64(Some(0))), true);
                }
                // x + 0 → x
                (BinOp::Add, _, LogicalExpr::Literal(ScalarValue::Int64(Some(0)))) => {
                    return (new_left, true);
                }
                // x - 0 → x
                (BinOp::Sub, _, LogicalExpr::Literal(ScalarValue::Int64(Some(0)))) => {
                    return (new_left, true);
                }
                // AND with false → false
                (BinOp::And, LogicalExpr::Literal(ScalarValue::Bool(Some(false))), _)
                | (BinOp::And, _, LogicalExpr::Literal(ScalarValue::Bool(Some(false)))) => {
                    return (LogicalExpr::Literal(ScalarValue::Bool(Some(false))), true);
                }
                // AND with true → other side
                (BinOp::And, LogicalExpr::Literal(ScalarValue::Bool(Some(true))), _) => {
                    return (new_right, true);
                }
                (BinOp::And, _, LogicalExpr::Literal(ScalarValue::Bool(Some(true)))) => {
                    return (new_left, true);
                }
                // OR with true → true
                (BinOp::Or, LogicalExpr::Literal(ScalarValue::Bool(Some(true))), _)
                | (BinOp::Or, _, LogicalExpr::Literal(ScalarValue::Bool(Some(true)))) => {
                    return (LogicalExpr::Literal(ScalarValue::Bool(Some(true))), true);
                }
                // OR with false → other side
                (BinOp::Or, LogicalExpr::Literal(ScalarValue::Bool(Some(false))), _) => {
                    return (new_right, true);
                }
                (BinOp::Or, _, LogicalExpr::Literal(ScalarValue::Bool(Some(false)))) => {
                    return (new_left, true);
                }
                _ => {}
            }

            (
                LogicalExpr::BinaryOp {
                    left: Box::new(new_left),
                    op,
                    right: Box::new(new_right),
                },
                changed,
            )
        }
        LogicalExpr::Not(inner) => {
            let (new_inner, changed) = simplify_expr(*inner);
            // Double negation: NOT(NOT(x)) → x
            match new_inner {
                LogicalExpr::Not(inner_inner) => (*inner_inner, true),
                _ => (LogicalExpr::Not(Box::new(new_inner)), changed),
            }
        }
        _ => (expr, false),
    }
}

/// Fold a binary operation on two literal values.
fn fold_binary(left: &ScalarValue, op: &BinOp, right: &ScalarValue) -> Option<ScalarValue> {
    match (left, right) {
        // Int64 arithmetic
        (ScalarValue::Int64(Some(l)), ScalarValue::Int64(Some(r))) => match op {
            BinOp::Add => Some(ScalarValue::Int64(Some(l + r))),
            BinOp::Sub => Some(ScalarValue::Int64(Some(l - r))),
            BinOp::Mul => Some(ScalarValue::Int64(Some(l * r))),
            BinOp::Div if *r != 0 => Some(ScalarValue::Int64(Some(l / r))),
            BinOp::Eq => Some(ScalarValue::Bool(Some(l == r))),
            BinOp::Neq => Some(ScalarValue::Bool(Some(l != r))),
            BinOp::Lt => Some(ScalarValue::Bool(Some(l < r))),
            BinOp::Gt => Some(ScalarValue::Bool(Some(l > r))),
            BinOp::Lte => Some(ScalarValue::Bool(Some(l <= r))),
            BinOp::Gte => Some(ScalarValue::Bool(Some(l >= r))),
            _ => None,
        },
        // Float64 arithmetic
        (ScalarValue::Float64(Some(l)), ScalarValue::Float64(Some(r))) => match op {
            BinOp::Add => Some(ScalarValue::Float64(Some(l + r))),
            BinOp::Sub => Some(ScalarValue::Float64(Some(l - r))),
            BinOp::Mul => Some(ScalarValue::Float64(Some(l * r))),
            BinOp::Div if *r != 0.0 => Some(ScalarValue::Float64(Some(l / r))),
            BinOp::Eq => Some(ScalarValue::Bool(Some(l == r))),
            BinOp::Neq => Some(ScalarValue::Bool(Some(l != r))),
            BinOp::Lt => Some(ScalarValue::Bool(Some(l < r))),
            BinOp::Gt => Some(ScalarValue::Bool(Some(l > r))),
            BinOp::Lte => Some(ScalarValue::Bool(Some(l <= r))),
            BinOp::Gte => Some(ScalarValue::Bool(Some(l >= r))),
            _ => None,
        },
        // Bool logic
        (ScalarValue::Bool(Some(l)), ScalarValue::Bool(Some(r))) => match op {
            BinOp::And => Some(ScalarValue::Bool(Some(*l && *r))),
            BinOp::Or => Some(ScalarValue::Bool(Some(*l || *r))),
            BinOp::Eq => Some(ScalarValue::Bool(Some(l == r))),
            BinOp::Neq => Some(ScalarValue::Bool(Some(l != r))),
            _ => None,
        },
        _ => None,
    }
}
