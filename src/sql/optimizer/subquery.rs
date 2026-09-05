use crate::sql::planner::{AggregateExpr, LogicalExpr, LogicalPlan};

use super::Rule;

/// Subquery expressions (`ScalarSubquery` / `ExistsSubquery` / `InSubquery`)
/// are resolved against real data at AST level by the execution layer (see
/// `query::resolve_in_subqueries`) before planning. This rule only walks the
/// plan; it must NOT try to "evaluate" subqueries on fabricated empty data,
/// which silently produced wrong constants (EXISTS -> false, scalar -> NULL).
pub struct SubqueryRule;

impl Rule for SubqueryRule {
    fn apply(&self, plan: LogicalPlan) -> (LogicalPlan, bool) {
        optimize_subqueries(plan)
    }
}

fn optimize_subqueries(plan: LogicalPlan) -> (LogicalPlan, bool) {
    match plan {
        LogicalPlan::Project(mut proj) => {
            let input_plan = *proj.input.clone();
            let mut changed = false;
            let new_exprs: Vec<LogicalExpr> = proj
                .expressions
                .iter()
                .map(|e| {
                    let (new_e, c) = replace_subquery_expr(e.clone(), &input_plan);
                    changed = changed || c;
                    new_e
                })
                .collect();
            proj.expressions = new_exprs;
            let (new_input, input_changed) = optimize_subqueries(*proj.input);
            proj.input = Box::new(new_input);
            (LogicalPlan::Project(proj), changed || input_changed)
        }
        LogicalPlan::Filter(mut filter) => {
            let input_plan = *filter.input.clone();
            let (new_pred, pred_changed) =
                replace_subquery_expr(filter.predicate.clone(), &input_plan);
            filter.predicate = new_pred;
            let (new_input, input_changed) = optimize_subqueries(*filter.input);
            filter.input = Box::new(new_input);
            (LogicalPlan::Filter(filter), pred_changed || input_changed)
        }
        LogicalPlan::Join(join) => {
            let (new_cond, cond_changed) =
                replace_subquery_expr(join.condition.clone(), &LogicalPlan::Empty);
            let (new_left, left_changed) = optimize_subqueries(*join.left);
            let (new_right, right_changed) = optimize_subqueries(*join.right);
            (
                LogicalPlan::Join(crate::sql::planner::JoinPlan {
                    condition: new_cond,
                    left: Box::new(new_left),
                    right: Box::new(new_right),
                    left_schema: join.left_schema,
                    right_schema: join.right_schema,
                    join_type: join.join_type,
                    output_schema: join.output_schema,
                }),
                cond_changed || left_changed || right_changed,
            )
        }
        LogicalPlan::SemiJoin(semi) => {
            let (new_cond, cond_changed) =
                replace_subquery_expr(semi.condition.clone(), &LogicalPlan::Empty);
            let (new_left, left_changed) = optimize_subqueries(*semi.left);
            let (new_right, right_changed) = optimize_subqueries(*semi.right);
            (
                LogicalPlan::SemiJoin(crate::sql::planner::SemiJoinPlan {
                    condition: new_cond,
                    left: Box::new(new_left),
                    right: Box::new(new_right),
                    left_schema: semi.left_schema,
                    right_schema: semi.right_schema,
                    is_anti: semi.is_anti,
                }),
                cond_changed || left_changed || right_changed,
            )
        }
        LogicalPlan::Aggregate(mut agg) => {
            let (new_input, input_changed) = optimize_subqueries(*agg.input);
            agg.input = Box::new(new_input);
            (LogicalPlan::Aggregate(agg), input_changed)
        }
        LogicalPlan::Sort(mut sort) => {
            let (new_input, input_changed) = optimize_subqueries(*sort.input);
            sort.input = Box::new(new_input);
            (LogicalPlan::Sort(sort), input_changed)
        }
        LogicalPlan::Limit(mut limit) => {
            let (new_input, input_changed) = optimize_subqueries(*limit.input);
            limit.input = Box::new(new_input);
            (LogicalPlan::Limit(limit), input_changed)
        }
        LogicalPlan::TopN(mut topn) => {
            let (new_input, input_changed) = optimize_subqueries(*topn.input);
            topn.input = Box::new(new_input);
            (LogicalPlan::TopN(topn), input_changed)
        }
        LogicalPlan::Distinct(mut distinct) => {
            let (new_input, input_changed) = optimize_subqueries(*distinct.input);
            distinct.input = Box::new(new_input);
            (LogicalPlan::Distinct(distinct), input_changed)
        }
        LogicalPlan::Window(mut window) => {
            let (new_input, input_changed) = optimize_subqueries(*window.input);
            window.input = Box::new(new_input);
            (LogicalPlan::Window(window), input_changed)
        }
        LogicalPlan::Scan(scan) => (LogicalPlan::Scan(scan), false),
        LogicalPlan::Empty => (LogicalPlan::Empty, false),
    }
}

fn replace_subquery_expr(expr: LogicalExpr, _context_plan: &LogicalPlan) -> (LogicalExpr, bool) {
    match expr {
        LogicalExpr::ScalarSubquery(_) | LogicalExpr::ExistsSubquery(_) => (expr, false),
        LogicalExpr::InSubquery { .. } => (expr, false),
        LogicalExpr::BinaryOp { left, op, right } => {
            let (new_left, lc) = replace_subquery_expr(*left, _context_plan);
            let (new_right, rc) = replace_subquery_expr(*right, _context_plan);
            (
                LogicalExpr::BinaryOp {
                    left: Box::new(new_left),
                    op,
                    right: Box::new(new_right),
                },
                lc || rc,
            )
        }
        LogicalExpr::Not(inner) => {
            let (new_inner, changed) = replace_subquery_expr(*inner, _context_plan);
            (LogicalExpr::Not(Box::new(new_inner)), changed)
        }
        LogicalExpr::IsNull(inner) => {
            let (new_inner, changed) = replace_subquery_expr(*inner, _context_plan);
            (LogicalExpr::IsNull(Box::new(new_inner)), changed)
        }
        LogicalExpr::IsNotNull(inner) => {
            let (new_inner, changed) = replace_subquery_expr(*inner, _context_plan);
            (LogicalExpr::IsNotNull(Box::new(new_inner)), changed)
        }
        LogicalExpr::In {
            expr: inner_expr,
            values,
            negated,
        } => {
            let (new_expr, changed) = replace_subquery_expr(*inner_expr, _context_plan);
            (
                LogicalExpr::In {
                    expr: Box::new(new_expr),
                    values,
                    negated,
                },
                changed,
            )
        }
        LogicalExpr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let (new_expr, ec) = replace_subquery_expr(*expr, _context_plan);
            let (new_low, lc) = replace_subquery_expr(*low, _context_plan);
            let (new_high, hc) = replace_subquery_expr(*high, _context_plan);
            (
                LogicalExpr::Between {
                    expr: Box::new(new_expr),
                    low: Box::new(new_low),
                    high: Box::new(new_high),
                    negated,
                },
                ec || lc || hc,
            )
        }
        LogicalExpr::Case {
            operand,
            when_clauses,
            else_clause,
        } => {
            let mut changed = false;
            let new_operand = operand.map(|o| {
                let (new_o, c) = replace_subquery_expr(*o, _context_plan);
                changed = changed || c;
                Box::new(new_o)
            });
            let new_when: Vec<(LogicalExpr, LogicalExpr)> = when_clauses
                .iter()
                .map(|(cond, result)| {
                    let (new_cond, cc) = replace_subquery_expr(cond.clone(), _context_plan);
                    let (new_result, rc) = replace_subquery_expr(result.clone(), _context_plan);
                    changed = changed || cc || rc;
                    (new_cond, new_result)
                })
                .collect();
            let new_else = else_clause.map(|e| {
                let (new_e, c) = replace_subquery_expr(*e, _context_plan);
                changed = changed || c;
                Box::new(new_e)
            });
            (
                LogicalExpr::Case {
                    operand: new_operand,
                    when_clauses: new_when,
                    else_clause: new_else,
                },
                changed,
            )
        }
        LogicalExpr::Like {
            expr,
            pattern,
            negated,
        } => {
            let (new_expr, changed) = replace_subquery_expr(*expr, _context_plan);
            (
                LogicalExpr::Like {
                    expr: Box::new(new_expr),
                    pattern,
                    negated,
                },
                changed,
            )
        }
        LogicalExpr::Function { name, args } => {
            let mut changed = false;
            let new_args: Vec<LogicalExpr> = args
                .iter()
                .map(|a| {
                    let (new_a, c) = replace_subquery_expr(a.clone(), _context_plan);
                    changed = changed || c;
                    new_a
                })
                .collect();
            (
                LogicalExpr::Function {
                    name,
                    args: new_args,
                },
                changed,
            )
        }
        LogicalExpr::Aggregate(agg) => {
            let (new_arg, changed) = replace_subquery_expr(*agg.arg, _context_plan);
            (
                LogicalExpr::Aggregate(AggregateExpr {
                    func: agg.func,
                    arg: Box::new(new_arg),
                    distinct: agg.distinct,
                }),
                changed,
            )
        }
        other => (other, false),
    }
}
