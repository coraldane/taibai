use crate::sql::parser::ast::{BinOp, JoinType};
use crate::sql::planner::*;

use super::Rule;

pub struct PredicatePushdownRule;

impl Rule for PredicatePushdownRule {
    fn name(&self) -> &str {
        "predicate_pushdown"
    }

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
            if scan
                .pushed_filter
                .as_ref()
                .is_some_and(|existing| contains_conjunct(existing, &predicate))
            {
                return (
                    LogicalPlan::Filter(FilterPlan {
                        predicate,
                        input: Box::new(LogicalPlan::Scan(scan)),
                    }),
                    false,
                );
            }
            scan.pushed_filter = Some(match scan.pushed_filter.take() {
                Some(existing) => and_expr(existing, predicate.clone()),
                None => predicate.clone(),
            });
            (
                LogicalPlan::Filter(FilterPlan {
                    predicate,
                    input: Box::new(LogicalPlan::Scan(scan)),
                }),
                true,
            )
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
                        residuals.push(part);
                        changed = true;
                    }
                    ExprSide::Right if can_push_filter_through_join(join.join_type, false) => {
                        if let Some(remapped) = remap_right_expr(&part, left_ncols) {
                            right_filters.push(remapped);
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
    split_conjunct_refs(existing)
        .iter()
        .any(|part| *part == predicate)
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
mod tests {
    use super::*;
    use crate::protocol::types::DataType;
    use crate::sql::parser::parser::Parser;
    use crate::sql::planner::plan_select;
    use crate::types::batch::{ColumnDef, Schema};
    use std::collections::HashMap;

    #[test]
    fn predicate_pushdown_keeps_row_filter() {
        let schema = Schema::new(vec![
            ColumnDef::new("name", DataType::VarChar),
            ColumnDef::new("region_id", DataType::Int32),
        ]);
        let mut schemas = HashMap::new();
        schemas.insert("test_table".to_string(), schema);

        let stmt = Parser::parse(
            "SELECT name FROM test_table \
             WHERE name = 'alice' AND region_id = 20251111",
        )
        .unwrap()
        .remove(0);
        let plan = plan_select(&stmt, &schemas).unwrap();
        let (optimized, changed) = PredicatePushdownRule.apply(plan);

        assert!(changed);
        let LogicalPlan::Project(project) = optimized else {
            panic!("expected project root");
        };
        let LogicalPlan::Filter(filter) = *project.input else {
            panic!("predicate pushdown must preserve the row-level filter");
        };
        let LogicalPlan::Scan(scan) = *filter.input else {
            panic!("expected filter input to be scan");
        };
        assert!(scan.pushed_filter.is_some());
    }

    #[test]
    fn pushes_left_where_and_right_on_filters_for_left_join() {
        let daily = Schema::new(vec![
            ColumnDef::new("security_id", DataType::Int64),
            ColumnDef::new("deal_date", DataType::Int32),
            ColumnDef::new("close_px", DataType::Float64),
            ColumnDef::new("pre_close", DataType::Float64),
        ]);
        let valuation = Schema::new(vec![
            ColumnDef::new("security_id", DataType::Int64),
            ColumnDef::new("deal_date", DataType::Int32),
            ColumnDef::new("pe_ratio", DataType::Float64),
        ]);
        let mut schemas = HashMap::new();
        schemas.insert("t_stock_daily".to_string(), daily);
        schemas.insert("t_valuation".to_string(), valuation);

        let stmt = Parser::parse(
            "SELECT d.security_id, v.pe_ratio FROM t_stock_daily d \
             LEFT JOIN t_valuation v ON v.deal_date = 20260604 AND d.security_id = v.security_id \
             WHERE d.deal_date = 20260604 \
             ORDER BY (d.close_px - d.pre_close) / d.pre_close DESC LIMIT 20",
        )
        .unwrap()
        .remove(0);
        let plan = plan_select(&stmt, &schemas).unwrap();
        let (optimized, changed) = PredicatePushdownRule.apply(plan);
        assert!(changed);
        let mut scans = Vec::new();
        collect_scans(&optimized, &mut scans);
        assert_eq!(scans.len(), 2);
        let daily_scan = scans
            .iter()
            .find(|s| s.table_name == "t_stock_daily")
            .unwrap();
        let valuation_scan = scans
            .iter()
            .find(|s| s.table_name == "t_valuation")
            .unwrap();
        assert!(daily_scan.pushed_filter.is_some());
        assert!(valuation_scan.pushed_filter.is_some());
    }

    fn collect_scans<'a>(plan: &'a LogicalPlan, scans: &mut Vec<&'a ScanPlan>) {
        match plan {
            LogicalPlan::Scan(scan) => scans.push(scan),
            LogicalPlan::Filter(f) => collect_scans(&f.input, scans),
            LogicalPlan::Project(p) => collect_scans(&p.input, scans),
            LogicalPlan::Sort(s) => collect_scans(&s.input, scans),
            LogicalPlan::Limit(l) => collect_scans(&l.input, scans),
            LogicalPlan::TopN(t) => collect_scans(&t.input, scans),
            LogicalPlan::Join(j) => {
                collect_scans(&j.left, scans);
                collect_scans(&j.right, scans);
            }
            _ => {}
        }
    }
}
