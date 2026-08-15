use crate::sql::parser::ast::{BinOp, JoinType};
use crate::sql::planner::{LogicalExpr, LogicalPlan, ScanPlan, SemiJoinPlan};
use crate::types::scalar::ScalarValue;

use super::Rule;

pub struct SemiJoinRule;

impl Rule for SemiJoinRule {
    fn name(&self) -> &str {
        "semi_join"
    }

    fn apply(&self, plan: LogicalPlan) -> (LogicalPlan, bool) {
        transform_to_semi_join(plan)
    }
}

fn transform_to_semi_join(plan: LogicalPlan) -> (LogicalPlan, bool) {
    match plan {
        LogicalPlan::Filter(mut filter) => {
            let (new_pred, pred_changed) =
                transform_expr_to_semi_join(&filter.predicate, &filter.input);
            filter.predicate = new_pred;
            let (new_input, input_changed) = transform_to_semi_join(*filter.input);
            filter.input = Box::new(new_input);

            if let LogicalExpr::In {
                expr,
                values,
                negated,
            } = &filter.predicate
            {
                if !values.is_empty() && expr_is_column(expr) {
                    let left_plan = *filter.input.clone();
                    if let LogicalPlan::Scan(scan) = &left_plan {
                        let semi = build_semi_join_from_in(scan, values, *negated);
                        if let Some(semi_join) = semi {
                            return (semi_join, true);
                        }
                    }
                }
            }

            (LogicalPlan::Filter(filter), pred_changed || input_changed)
        }
        LogicalPlan::Project(mut proj) => {
            let (new_input, changed) = transform_to_semi_join(*proj.input);
            proj.input = Box::new(new_input);
            (LogicalPlan::Project(proj), changed)
        }
        LogicalPlan::Aggregate(mut agg) => {
            let (new_input, changed) = transform_to_semi_join(*agg.input);
            agg.input = Box::new(new_input);
            (LogicalPlan::Aggregate(agg), changed)
        }
        LogicalPlan::Sort(mut sort) => {
            let (new_input, changed) = transform_to_semi_join(*sort.input);
            sort.input = Box::new(new_input);
            (LogicalPlan::Sort(sort), changed)
        }
        LogicalPlan::Limit(mut limit) => {
            let (new_input, changed) = transform_to_semi_join(*limit.input);
            limit.input = Box::new(new_input);
            (LogicalPlan::Limit(limit), changed)
        }
        LogicalPlan::TopN(mut topn) => {
            let (new_input, changed) = transform_to_semi_join(*topn.input);
            topn.input = Box::new(new_input);
            (LogicalPlan::TopN(topn), changed)
        }
        LogicalPlan::Join(mut join) => {
            if join.join_type == JoinType::Semi || join.join_type == JoinType::Anti {
                let semi = LogicalPlan::SemiJoin(SemiJoinPlan {
                    left: join.left,
                    right: join.right,
                    condition: join.condition,
                    left_schema: join.left_schema,
                    right_schema: join.right_schema,
                    is_anti: join.join_type == JoinType::Anti,
                });
                return (semi, true);
            }
            let (new_left, left_changed) = transform_to_semi_join(*join.left);
            let (new_right, right_changed) = transform_to_semi_join(*join.right);
            join.left = Box::new(new_left);
            join.right = Box::new(new_right);
            (LogicalPlan::Join(join), left_changed || right_changed)
        }
        LogicalPlan::SemiJoin(mut semi) => {
            let (new_left, left_changed) = transform_to_semi_join(*semi.left);
            let (new_right, right_changed) = transform_to_semi_join(*semi.right);
            semi.left = Box::new(new_left);
            semi.right = Box::new(new_right);
            (LogicalPlan::SemiJoin(semi), left_changed || right_changed)
        }
        LogicalPlan::Distinct(mut distinct) => {
            let (new_input, changed) = transform_to_semi_join(*distinct.input);
            distinct.input = Box::new(new_input);
            (LogicalPlan::Distinct(distinct), changed)
        }
        LogicalPlan::Window(mut window) => {
            let (new_input, changed) = transform_to_semi_join(*window.input);
            window.input = Box::new(new_input);
            (LogicalPlan::Window(window), changed)
        }
        LogicalPlan::Scan(scan) => (LogicalPlan::Scan(scan), false),
        LogicalPlan::Empty => (LogicalPlan::Empty, false),
    }
}

fn transform_expr_to_semi_join(expr: &LogicalExpr, input: &LogicalPlan) -> (LogicalExpr, bool) {
    match expr {
        LogicalExpr::ExistsSubquery(subquery) => {
            if let LogicalPlan::Scan(scan) = input {
                let _semi = LogicalPlan::SemiJoin(SemiJoinPlan {
                    left: Box::new(LogicalPlan::Scan(scan.clone())),
                    right: subquery.clone(),
                    condition: LogicalExpr::Literal(ScalarValue::Bool(Some(true))),
                    left_schema: scan.schema.clone(),
                    right_schema: extract_schema(subquery),
                    is_anti: false,
                });
                return (LogicalExpr::Literal(ScalarValue::Bool(Some(true))), true);
            }
            (expr.clone(), false)
        }
        LogicalExpr::BinaryOp { left, op, right } => match op {
            BinOp::And => {
                let (new_left, lc) = transform_expr_to_semi_join(left, input);
                let (new_right, rc) = transform_expr_to_semi_join(right, input);
                (
                    LogicalExpr::BinaryOp {
                        left: Box::new(new_left),
                        op: BinOp::And,
                        right: Box::new(new_right),
                    },
                    lc || rc,
                )
            }
            BinOp::Or => {
                let (new_left, lc) = transform_expr_to_semi_join(left, input);
                let (new_right, rc) = transform_expr_to_semi_join(right, input);
                (
                    LogicalExpr::BinaryOp {
                        left: Box::new(new_left),
                        op: BinOp::Or,
                        right: Box::new(new_right),
                    },
                    lc || rc,
                )
            }
            _ => (expr.clone(), false),
        },
        _ => (expr.clone(), false),
    }
}

fn expr_is_column(expr: &LogicalExpr) -> bool {
    matches!(expr, LogicalExpr::Column(_))
}

fn build_semi_join_from_in(
    scan: &ScanPlan,
    values: &[ScalarValue],
    negated: bool,
) -> Option<LogicalPlan> {
    // Literal IN is already handled by scan/filter predicates. The previous
    // rewrite used an empty values scan and only compared the first value.
    let _ = (scan, values, negated);
    None
}

fn extract_schema(plan: &LogicalPlan) -> crate::types::batch::Schema {
    match plan {
        LogicalPlan::Scan(scan) => scan.schema.clone(),
        LogicalPlan::Project(proj) => extract_schema(&proj.input),
        LogicalPlan::Filter(filter) => extract_schema(&filter.input),
        LogicalPlan::Aggregate(agg) => extract_schema(&agg.input),
        LogicalPlan::Join(join) => join.output_schema.clone(),
        LogicalPlan::SemiJoin(semi) => semi.left_schema.clone(),
        LogicalPlan::Sort(sort) => extract_schema(&sort.input),
        LogicalPlan::Limit(limit) => extract_schema(&limit.input),
        LogicalPlan::TopN(topn) => extract_schema(&topn.input),
        LogicalPlan::Distinct(distinct) => extract_schema(&distinct.input),
        LogicalPlan::Window(window) => extract_schema(&window.input),
        LogicalPlan::Empty => crate::types::batch::Schema::empty(),
    }
}

pub struct SemiJoinColumnPruningRule;

impl Rule for SemiJoinColumnPruningRule {
    fn name(&self) -> &str {
        "semi_join_column_pruning"
    }

    fn apply(&self, plan: LogicalPlan) -> (LogicalPlan, bool) {
        prune_semi_join_columns(plan)
    }
}

fn prune_semi_join_columns(plan: LogicalPlan) -> (LogicalPlan, bool) {
    match plan {
        LogicalPlan::SemiJoin(mut semi) => {
            let condition_cols = extract_condition_columns(&semi.condition);
            let left_needed: Vec<usize> = condition_cols
                .iter()
                .filter(|&c| *c < semi.left_schema.columns.len())
                .cloned()
                .collect();

            let (new_left, left_changed) =
                apply_projection_to_input(*semi.left, &left_needed, &semi.left_schema);
            semi.left = Box::new(new_left);

            let right_needed: Vec<usize> = condition_cols
                .iter()
                .filter(|&c| *c >= semi.left_schema.columns.len())
                .map(|c| c - semi.left_schema.columns.len())
                .collect();

            let (new_right, right_changed) =
                apply_projection_to_input(*semi.right, &right_needed, &semi.right_schema);
            semi.right = Box::new(new_right);

            let (new_left, new_left_changed) = prune_semi_join_columns(*semi.left);
            let (new_right, new_right_changed) = prune_semi_join_columns(*semi.right);
            semi.left = Box::new(new_left);
            semi.right = Box::new(new_right);

            (
                LogicalPlan::SemiJoin(semi),
                left_changed || right_changed || new_left_changed || new_right_changed,
            )
        }
        LogicalPlan::Project(mut proj) => {
            let (new_input, changed) = prune_semi_join_columns(*proj.input);
            proj.input = Box::new(new_input);
            (LogicalPlan::Project(proj), changed)
        }
        LogicalPlan::Filter(mut filter) => {
            let (new_input, changed) = prune_semi_join_columns(*filter.input);
            filter.input = Box::new(new_input);
            (LogicalPlan::Filter(filter), changed)
        }
        LogicalPlan::Aggregate(mut agg) => {
            let (new_input, changed) = prune_semi_join_columns(*agg.input);
            agg.input = Box::new(new_input);
            (LogicalPlan::Aggregate(agg), changed)
        }
        LogicalPlan::Sort(mut sort) => {
            let (new_input, changed) = prune_semi_join_columns(*sort.input);
            sort.input = Box::new(new_input);
            (LogicalPlan::Sort(sort), changed)
        }
        LogicalPlan::Limit(mut limit) => {
            let (new_input, changed) = prune_semi_join_columns(*limit.input);
            limit.input = Box::new(new_input);
            (LogicalPlan::Limit(limit), changed)
        }
        LogicalPlan::TopN(mut topn) => {
            let (new_input, changed) = prune_semi_join_columns(*topn.input);
            topn.input = Box::new(new_input);
            (LogicalPlan::TopN(topn), changed)
        }
        LogicalPlan::Join(mut join) => {
            let (new_left, left_changed) = prune_semi_join_columns(*join.left);
            let (new_right, right_changed) = prune_semi_join_columns(*join.right);
            join.left = Box::new(new_left);
            join.right = Box::new(new_right);
            (LogicalPlan::Join(join), left_changed || right_changed)
        }
        LogicalPlan::Distinct(mut distinct) => {
            let (new_input, changed) = prune_semi_join_columns(*distinct.input);
            distinct.input = Box::new(new_input);
            (LogicalPlan::Distinct(distinct), changed)
        }
        LogicalPlan::Window(mut window) => {
            let (new_input, changed) = prune_semi_join_columns(*window.input);
            window.input = Box::new(new_input);
            (LogicalPlan::Window(window), changed)
        }
        LogicalPlan::Scan(scan) => (LogicalPlan::Scan(scan), false),
        LogicalPlan::Empty => (LogicalPlan::Empty, false),
    }
}

fn extract_condition_columns(expr: &LogicalExpr) -> Vec<usize> {
    let mut cols = Vec::new();
    extract_cols_from_expr(expr, &mut cols);
    cols.sort();
    cols.dedup();
    cols
}

fn extract_cols_from_expr(expr: &LogicalExpr, cols: &mut Vec<usize>) {
    match expr {
        LogicalExpr::Column(idx) => cols.push(*idx),
        LogicalExpr::BinaryOp { left, right, .. } => {
            extract_cols_from_expr(left, cols);
            extract_cols_from_expr(right, cols);
        }
        LogicalExpr::Not(inner) => extract_cols_from_expr(inner, cols),
        LogicalExpr::IsNull(inner) => extract_cols_from_expr(inner, cols),
        LogicalExpr::IsNotNull(inner) => extract_cols_from_expr(inner, cols),
        LogicalExpr::In { expr, .. } => extract_cols_from_expr(expr, cols),
        LogicalExpr::Between {
            expr, low, high, ..
        } => {
            extract_cols_from_expr(expr, cols);
            extract_cols_from_expr(low, cols);
            extract_cols_from_expr(high, cols);
        }
        LogicalExpr::Case {
            operand,
            when_clauses,
            else_clause,
        } => {
            if let Some(o) = operand {
                extract_cols_from_expr(o, cols);
            }
            for (cond, result) in when_clauses {
                extract_cols_from_expr(cond, cols);
                extract_cols_from_expr(result, cols);
            }
            if let Some(e) = else_clause {
                extract_cols_from_expr(e, cols);
            }
        }
        LogicalExpr::Like { expr, .. } => extract_cols_from_expr(expr, cols),
        LogicalExpr::Function { args, .. } => {
            for a in args {
                extract_cols_from_expr(a, cols);
            }
        }
        LogicalExpr::Aggregate(agg) => extract_cols_from_expr(&agg.arg, cols),
        _ => {}
    }
}

fn apply_projection_to_input(
    plan: LogicalPlan,
    needed_cols: &[usize],
    schema: &crate::types::batch::Schema,
) -> (LogicalPlan, bool) {
    if needed_cols.is_empty() {
        return (plan, false);
    }

    match plan {
        LogicalPlan::Scan(mut scan) => {
            let current_proj = scan
                .projection
                .as_ref()
                .map(|p| p.to_vec())
                .unwrap_or_else(|| (0..schema.columns.len()).collect());

            let has_all = needed_cols.iter().all(|c| current_proj.contains(c));
            if has_all && scan.projection.is_some() {
                return (LogicalPlan::Scan(scan), false);
            }

            scan.projection = Some(needed_cols.to_vec());
            (LogicalPlan::Scan(scan), true)
        }
        other => (other, false),
    }
}
