use crate::sql::parser::ast::{BinOp, JoinType};
use crate::sql::planner::*;
use crate::types::scalar::ScalarValue;

use super::Rule;

pub struct PredicatePushdownRule;

impl Rule for PredicatePushdownRule {
    fn apply(&self, plan: LogicalPlan) -> (LogicalPlan, bool) {
        push_predicates(plan)
    }
}

fn push_predicates(plan: LogicalPlan) -> (LogicalPlan, bool) {
    match plan {
        LogicalPlan::Filter(filter) => push_filter(*filter.input, filter.predicate),
        LogicalPlan::Project(mut proj) => {
            let (input, changed) = push_predicates(*proj.input);
            proj.input = Box::new(input);
            (LogicalPlan::Project(proj), changed)
        }
        LogicalPlan::Aggregate(mut agg) => {
            let (input, changed) = push_predicates(*agg.input);
            agg.input = Box::new(input);
            (LogicalPlan::Aggregate(agg), changed)
        }
        LogicalPlan::Sort(mut sort) => {
            let (input, changed) = push_predicates(*sort.input);
            sort.input = Box::new(input);
            (LogicalPlan::Sort(sort), changed)
        }
        LogicalPlan::Limit(mut limit) => {
            let (input, changed) = push_predicates(*limit.input);
            limit.input = Box::new(input);
            (LogicalPlan::Limit(limit), changed)
        }
        LogicalPlan::TopN(mut topn) => {
            let (input, changed) = push_predicates(*topn.input);
            topn.input = Box::new(input);
            (LogicalPlan::TopN(topn), changed)
        }
        LogicalPlan::Join(mut join) => {
            let mut changed = false;
            let left_ncols = join.left_schema.columns.len();
            let parts = split_conjuncts(join.condition.clone());
            let mut join_parts = Vec::new();
            let mut left_filters = Vec::new();
            let mut right_filters = Vec::new();

            for part in parts {
                match expr_side(&part, left_ncols, join.output_schema.columns.len()) {
                    ExprSide::Left if can_push_join_side(join.join_type, true) => {
                        left_filters.push(part);
                        changed = true;
                    }
                    ExprSide::Right if can_push_join_side(join.join_type, false) => {
                        if let Some(remapped) = remap_right_expr(&part, left_ncols) {
                            right_filters.push(remapped);
                            changed = true;
                        } else {
                            join_parts.push(part);
                        }
                    }
                    _ => join_parts.push(part),
                }
            }

            if join_parts.is_empty() {
                join.condition =
                    LogicalExpr::Literal(crate::types::scalar::ScalarValue::Bool(Some(true)));
            } else {
                join.condition = combine_conjuncts(join_parts).unwrap();
            }

            let left_input = add_filter(*join.left, left_filters);
            let right_input = add_filter(*join.right, right_filters);
            let (left_input, left_changed) = push_predicates(left_input);
            let (right_input, right_changed) = push_predicates(right_input);
            join.left = Box::new(left_input);
            join.right = Box::new(right_input);
            (
                LogicalPlan::Join(join),
                changed || left_changed || right_changed,
            )
        }
        LogicalPlan::SemiJoin(mut semi) => {
            let (left, lc) = push_predicates(*semi.left);
            let (right, rc) = push_predicates(*semi.right);
            semi.left = Box::new(left);
            semi.right = Box::new(right);
            (LogicalPlan::SemiJoin(semi), lc || rc)
        }
        LogicalPlan::Distinct(mut distinct) => {
            let (input, changed) = push_predicates(*distinct.input);
            distinct.input = Box::new(input);
            (LogicalPlan::Distinct(distinct), changed)
        }
        LogicalPlan::Window(mut window) => {
            let (input, changed) = push_predicates(*window.input);
            window.input = Box::new(input);
            (LogicalPlan::Window(window), changed)
        }
        other => (other, false),
    }
}

fn push_filter(input: LogicalPlan, predicate: LogicalExpr) -> (LogicalPlan, bool) {
    match input {
        LogicalPlan::Scan(mut scan) => {
            let missing = missing_conjuncts(scan.pushed_filter.as_ref(), predicate.clone());
            if missing.is_empty() {
                return (
                    LogicalPlan::Filter(FilterPlan {
                        predicate,
                        input: Box::new(LogicalPlan::Scan(scan)),
                    }),
                    false,
                );
            }
            scan.pushed_filter = combine_scan_filter(scan.pushed_filter.take(), missing);
            (
                LogicalPlan::Filter(FilterPlan {
                    predicate,
                    input: Box::new(LogicalPlan::Scan(scan)),
                }),
                true,
            )
        }
        LogicalPlan::Filter(inner) => {
            let combined = combine_conjuncts(vec![predicate, inner.predicate])
                .unwrap_or(LogicalExpr::Literal(ScalarValue::Bool(Some(true))));
            let (plan, _) = push_filter(*inner.input, combined);
            (plan, true)
        }
        LogicalPlan::Sort(mut sort) => {
            let (sort_input, _) = push_predicates(*sort.input);
            sort.input = Box::new(LogicalPlan::Filter(FilterPlan {
                predicate,
                input: Box::new(sort_input),
            }));
            let (result, changed) = push_predicates(LogicalPlan::Sort(sort));
            (result, changed)
        }
        LogicalPlan::Join(mut join) => {
            let left_ncols = join.left_schema.columns.len();
            let total_cols = join.output_schema.columns.len();
            let mut residuals = Vec::new();
            let mut left_filters = Vec::new();
            let mut right_filters = Vec::new();
            let mut changed = false;

            for part in split_conjuncts(predicate) {
                match expr_side(&part, left_ncols, total_cols) {
                    ExprSide::Left if can_push_filter_through_join(join.join_type, true) => {
                        left_filters.push(part.clone());
                        if let Some(derived) = derive_join_side_filter(
                            &part,
                            &join.condition,
                            left_ncols,
                            total_cols,
                            true,
                        ) {
                            right_filters.push(derived);
                        }
                        residuals.push(part);
                        changed = true;
                    }
                    ExprSide::Right if can_push_filter_through_join(join.join_type, false) => {
                        if let Some(remapped) = remap_right_expr(&part, left_ncols) {
                            right_filters.push(remapped);
                            if let Some(derived) = derive_join_side_filter(
                                &part,
                                &join.condition,
                                left_ncols,
                                total_cols,
                                false,
                            ) {
                                left_filters.push(derived);
                            }
                            residuals.push(part);
                            changed = true;
                        } else {
                            residuals.push(part);
                        }
                    }
                    _ => residuals.push(part),
                }
            }

            let left = add_filter(*join.left, left_filters);
            let right = add_filter(*join.right, right_filters);
            let (left, lc) = push_predicates(left);
            let (right, rc) = push_predicates(right);
            join.left = Box::new(left);
            join.right = Box::new(right);
            let (plan, jc) = push_predicates(LogicalPlan::Join(join));
            if let Some(residual) = combine_conjuncts(residuals) {
                (
                    LogicalPlan::Filter(FilterPlan {
                        predicate: residual,
                        input: Box::new(plan),
                    }),
                    changed || lc || rc || jc,
                )
            } else {
                (plan, changed || lc || rc || jc)
            }
        }
        other => {
            let (input, input_changed) = push_predicates(other);
            (
                LogicalPlan::Filter(FilterPlan {
                    predicate,
                    input: Box::new(input),
                }),
                input_changed,
            )
        }
    }
}

fn add_filter(plan: LogicalPlan, filters: Vec<LogicalExpr>) -> LogicalPlan {
    match combine_conjuncts(filters) {
        Some(predicate) => match plan {
            LogicalPlan::Filter(filter) if filter.predicate == predicate => {
                LogicalPlan::Filter(filter)
            }
            other => LogicalPlan::Filter(FilterPlan {
                predicate,
                input: Box::new(other),
            }),
        },
        None => plan,
    }
}

fn split_conjuncts(expr: LogicalExpr) -> Vec<LogicalExpr> {
    match expr {
        LogicalExpr::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => {
            let mut parts = split_conjuncts(*left);
            parts.extend(split_conjuncts(*right));
            parts
        }
        other => vec![other],
    }
}

fn combine_conjuncts(mut exprs: Vec<LogicalExpr>) -> Option<LogicalExpr> {
    if exprs.is_empty() {
        return None;
    }
    let first = exprs.remove(0);
    Some(exprs.into_iter().fold(first, and_expr))
}

fn and_expr(left: LogicalExpr, right: LogicalExpr) -> LogicalExpr {
    LogicalExpr::BinaryOp {
        left: Box::new(left),
        op: BinOp::And,
        right: Box::new(right),
    }
}

fn contains_conjunct(existing: &LogicalExpr, predicate: &LogicalExpr) -> bool {
    split_conjunct_refs(existing).contains(&predicate)
}

fn missing_conjuncts(existing: Option<&LogicalExpr>, predicate: LogicalExpr) -> Vec<LogicalExpr> {
    let parts = split_conjuncts(predicate);
    let Some(existing) = existing else {
        return parts;
    };
    parts
        .into_iter()
        .filter(|part| !contains_conjunct(existing, part))
        .collect()
}

fn combine_scan_filter(
    existing: Option<LogicalExpr>,
    missing: Vec<LogicalExpr>,
) -> Option<LogicalExpr> {
    let missing = combine_conjuncts(missing)?;
    Some(match existing {
        Some(existing) => and_expr(existing, missing),
        None => missing,
    })
}

fn split_conjunct_refs(expr: &LogicalExpr) -> Vec<&LogicalExpr> {
    match expr {
        LogicalExpr::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => {
            let mut parts = split_conjunct_refs(left);
            parts.extend(split_conjunct_refs(right));
            parts
        }
        other => vec![other],
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExprSide {
    Left,
    Right,
    Both,
    None,
}

fn expr_side(expr: &LogicalExpr, left_ncols: usize, total_cols: usize) -> ExprSide {
    let mut cols = Vec::new();
    collect_cols_from_expr(expr, &mut cols);
    cols.sort_unstable();
    cols.dedup();
    if cols.is_empty() {
        return ExprSide::None;
    }
    let has_left = cols.iter().any(|idx| *idx < left_ncols);
    let has_right = cols
        .iter()
        .any(|idx| *idx >= left_ncols && *idx < total_cols);
    match (has_left, has_right) {
        (true, false) => ExprSide::Left,
        (false, true) => ExprSide::Right,
        (true, true) => ExprSide::Both,
        (false, false) => ExprSide::Both,
    }
}

fn can_push_join_side(join_type: JoinType, is_left: bool) -> bool {
    match join_type {
        JoinType::Inner | JoinType::Cross | JoinType::Semi | JoinType::Anti => true,
        JoinType::Left => !is_left,
        JoinType::Right => is_left,
        JoinType::Full => false,
        JoinType::Asof => !is_left,
    }
}

fn can_push_filter_through_join(join_type: JoinType, is_left: bool) -> bool {
    match join_type {
        JoinType::Inner | JoinType::Cross | JoinType::Semi | JoinType::Anti => true,
        JoinType::Left => is_left,
        JoinType::Right => !is_left,
        JoinType::Full => false,
        JoinType::Asof => is_left,
    }
}

fn derive_join_side_filter(
    filter: &LogicalExpr,
    join_condition: &LogicalExpr,
    left_ncols: usize,
    total_cols: usize,
    filter_is_left: bool,
) -> Option<LogicalExpr> {
    let (filter_col, value) = equality_column_literal(filter)?;
    for part in split_conjunct_refs(join_condition) {
        let Some((left_col, right_col)) = equality_column_column(part, left_ncols, total_cols)
        else {
            continue;
        };
        if filter_is_left && filter_col == left_col {
            return Some(LogicalExpr::BinaryOp {
                left: Box::new(LogicalExpr::Column(right_col - left_ncols)),
                op: BinOp::Eq,
                right: Box::new(LogicalExpr::Literal(value.clone())),
            });
        }
        if !filter_is_left && filter_col == right_col {
            return Some(LogicalExpr::BinaryOp {
                left: Box::new(LogicalExpr::Column(left_col)),
                op: BinOp::Eq,
                right: Box::new(LogicalExpr::Literal(value.clone())),
            });
        }
    }
    None
}

fn equality_column_literal(expr: &LogicalExpr) -> Option<(usize, ScalarValue)> {
    match expr {
        LogicalExpr::BinaryOp {
            left,
            op: BinOp::Eq,
            right,
        } => match (&**left, &**right) {
            (LogicalExpr::Column(idx), LogicalExpr::Literal(value)) => Some((*idx, value.clone())),
            (LogicalExpr::Literal(value), LogicalExpr::Column(idx)) => Some((*idx, value.clone())),
            _ => None,
        },
        _ => None,
    }
}

fn equality_column_column(
    expr: &LogicalExpr,
    left_ncols: usize,
    total_cols: usize,
) -> Option<(usize, usize)> {
    match expr {
        LogicalExpr::BinaryOp {
            left,
            op: BinOp::Eq,
            right,
        } => match (&**left, &**right) {
            (LogicalExpr::Column(l), LogicalExpr::Column(r)) => {
                normalize_join_columns(*l, *r, left_ncols, total_cols)
            }
            _ => None,
        },
        _ => None,
    }
}

fn normalize_join_columns(
    a: usize,
    b: usize,
    left_ncols: usize,
    total_cols: usize,
) -> Option<(usize, usize)> {
    let a_left = a < left_ncols;
    let b_left = b < left_ncols;
    let a_right = a >= left_ncols && a < total_cols;
    let b_right = b >= left_ncols && b < total_cols;
    match (a_left, a_right, b_left, b_right) {
        (true, false, false, true) => Some((a, b)),
        (false, true, true, false) => Some((b, a)),
        _ => None,
    }
}

fn remap_right_expr(expr: &LogicalExpr, left_ncols: usize) -> Option<LogicalExpr> {
    match expr {
        LogicalExpr::Column(idx) => idx.checked_sub(left_ncols).map(LogicalExpr::Column),
        LogicalExpr::Literal(_) | LogicalExpr::Wildcard => Some(expr.clone()),
        LogicalExpr::BinaryOp { left, op, right } => Some(LogicalExpr::BinaryOp {
            left: Box::new(remap_right_expr(left, left_ncols)?),
            op: *op,
            right: Box::new(remap_right_expr(right, left_ncols)?),
        }),
        LogicalExpr::Not(inner) => Some(LogicalExpr::Not(Box::new(remap_right_expr(
            inner, left_ncols,
        )?))),
        LogicalExpr::IsNull(inner) => Some(LogicalExpr::IsNull(Box::new(remap_right_expr(
            inner, left_ncols,
        )?))),
        LogicalExpr::IsNotNull(inner) => Some(LogicalExpr::IsNotNull(Box::new(remap_right_expr(
            inner, left_ncols,
        )?))),
        LogicalExpr::In {
            expr,
            values,
            negated,
        } => Some(LogicalExpr::In {
            expr: Box::new(remap_right_expr(expr, left_ncols)?),
            values: values.clone(),
            negated: *negated,
        }),
        LogicalExpr::Between {
            expr,
            low,
            high,
            negated,
        } => Some(LogicalExpr::Between {
            expr: Box::new(remap_right_expr(expr, left_ncols)?),
            low: Box::new(remap_right_expr(low, left_ncols)?),
            high: Box::new(remap_right_expr(high, left_ncols)?),
            negated: *negated,
        }),
        LogicalExpr::Case {
            operand,
            when_clauses,
            else_clause,
        } => {
            let operand = match operand {
                Some(e) => Some(Box::new(remap_right_expr(e, left_ncols)?)),
                None => None,
            };
            let else_clause = match else_clause {
                Some(e) => Some(Box::new(remap_right_expr(e, left_ncols)?)),
                None => None,
            };
            Some(LogicalExpr::Case {
                operand,
                when_clauses: when_clauses
                    .iter()
                    .map(|(when, then)| {
                        Some((
                            remap_right_expr(when, left_ncols)?,
                            remap_right_expr(then, left_ncols)?,
                        ))
                    })
                    .collect::<Option<Vec<_>>>()?,
                else_clause,
            })
        }
        LogicalExpr::Like {
            expr,
            pattern,
            negated,
        } => Some(LogicalExpr::Like {
            expr: Box::new(remap_right_expr(expr, left_ncols)?),
            pattern: pattern.clone(),
            negated: *negated,
        }),
        LogicalExpr::Function { name, args } => Some(LogicalExpr::Function {
            name: name.clone(),
            args: args
                .iter()
                .map(|arg| remap_right_expr(arg, left_ncols))
                .collect::<Option<Vec<_>>>()?,
        }),
        LogicalExpr::Aggregate(_)
        | LogicalExpr::ScalarSubquery(_)
        | LogicalExpr::ExistsSubquery(_)
        | LogicalExpr::InSubquery { .. } => None,
    }
}

#[cfg(test)]
#[path = "predicate_pushdown_test.rs"]
mod tests;
