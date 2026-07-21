use crate::sql::parser::ast::JoinType;
use crate::sql::planner::{JoinPlan, LogicalExpr, LogicalPlan, ScanPlan, SortKey, TopNPlan};

use super::Rule;

pub struct OrderLimitPushdownRule;

impl Rule for OrderLimitPushdownRule {
    fn name(&self) -> &str {
        "order_limit_pushdown"
    }

    fn apply(&self, plan: LogicalPlan) -> (LogicalPlan, bool) {
        push_order_limit_to_scan(plan)
    }
}

fn push_order_limit_to_scan(plan: LogicalPlan) -> (LogicalPlan, bool) {
    match plan {
        LogicalPlan::TopN(mut topn) => {
            let (new_input, input_changed) = push_order_limit_to_scan(*topn.input);

            if let LogicalPlan::Scan(scan) = &new_input {
                if can_push_order_to_scan(&topn.sort_keys, scan)
                    && scan.pushed_order.is_none()
                    && scan.pushed_limit.is_none()
                {
                    let mut new_scan = scan.clone();
                    new_scan.pushed_order = Some(topn.sort_keys.clone());
                    new_scan.pushed_limit = Some(topn.limit.saturating_add(topn.offset));
                    return (
                        LogicalPlan::TopN(TopNPlan {
                            sort_keys: topn.sort_keys,
                            limit: topn.limit,
                            offset: topn.offset,
                            input: Box::new(LogicalPlan::Scan(new_scan)),
                        }),
                        true,
                    );
                }
            }

            topn.input = Box::new(new_input);
            (LogicalPlan::TopN(topn), input_changed)
        }
        LogicalPlan::Limit(mut limit) => {
            let (new_input, input_changed) = push_order_limit_to_scan(*limit.input);

            if let LogicalPlan::Scan(scan) = &new_input {
                if scan.pushed_limit.is_none() {
                    let mut new_scan = scan.clone();
                    new_scan.pushed_limit = Some(limit.limit.saturating_add(limit.offset));
                    limit.input = Box::new(LogicalPlan::Scan(new_scan));
                    return (LogicalPlan::Limit(limit), true);
                }
            }

            limit.input = Box::new(new_input);
            (LogicalPlan::Limit(limit), input_changed)
        }
        LogicalPlan::Sort(mut sort) => {
            let (new_input, input_changed) = push_order_limit_to_scan(*sort.input);

            if let LogicalPlan::Scan(scan) = &new_input {
                if can_push_order_to_scan(&sort.sort_keys, scan) && scan.pushed_order.is_none() {
                    let mut new_scan = scan.clone();
                    new_scan.pushed_order = Some(sort.sort_keys.clone());
                    sort.input = Box::new(LogicalPlan::Scan(new_scan));
                    return (LogicalPlan::Sort(sort), true);
                }
            }

            sort.input = Box::new(new_input);
            (LogicalPlan::Sort(sort), input_changed)
        }
        LogicalPlan::Filter(mut filter) => {
            let (new_input, changed) = push_order_limit_to_scan(*filter.input);
            filter.input = Box::new(new_input);
            (LogicalPlan::Filter(filter), changed)
        }
        LogicalPlan::Project(mut proj) => {
            let (new_input, changed) = push_order_limit_to_scan(*proj.input);
            proj.input = Box::new(new_input);
            (LogicalPlan::Project(proj), changed)
        }
        LogicalPlan::Aggregate(mut agg) => {
            let (new_input, changed) = push_order_limit_to_scan(*agg.input);
            agg.input = Box::new(new_input);
            (LogicalPlan::Aggregate(agg), changed)
        }
        LogicalPlan::Join(mut join) => {
            let (new_left, left_changed) = push_order_limit_to_scan(*join.left);
            let (new_right, right_changed) = push_order_limit_to_scan(*join.right);
            join.left = Box::new(new_left);
            join.right = Box::new(new_right);
            (LogicalPlan::Join(join), left_changed || right_changed)
        }
        LogicalPlan::SemiJoin(mut semi) => {
            let (new_left, left_changed) = push_order_limit_to_scan(*semi.left);
            let (new_right, right_changed) = push_order_limit_to_scan(*semi.right);
            semi.left = Box::new(new_left);
            semi.right = Box::new(new_right);
            (LogicalPlan::SemiJoin(semi), left_changed || right_changed)
        }
        LogicalPlan::Distinct(mut distinct) => {
            let (new_input, changed) = push_order_limit_to_scan(*distinct.input);
            distinct.input = Box::new(new_input);
            (LogicalPlan::Distinct(distinct), changed)
        }
        LogicalPlan::Window(mut window) => {
            let (new_input, changed) = push_order_limit_to_scan(*window.input);
            window.input = Box::new(new_input);
            (LogicalPlan::Window(window), changed)
        }
        LogicalPlan::Scan(scan) => (LogicalPlan::Scan(scan), false),
        LogicalPlan::Empty => (LogicalPlan::Empty, false),
    }
}

fn can_push_order_to_scan(sort_keys: &[SortKey], scan: &ScanPlan) -> bool {
    if scan.pushed_filter.is_some() {
        return false;
    }

    for sk in sort_keys {
        match &sk.expr {
            LogicalExpr::Column(idx) => {
                if *idx >= scan.schema.columns.len() {
                    return false;
                }
                let col = &scan.schema.columns[*idx];
                if !is_orderable_type(col.data_type) {
                    return false;
                }
            }
            _ => return false,
        }
    }

    true
}

fn is_orderable_type(dt: crate::protocol::types::DataType) -> bool {
    matches!(
        dt,
        crate::protocol::types::DataType::Int8
            | crate::protocol::types::DataType::Int16
            | crate::protocol::types::DataType::Int32
            | crate::protocol::types::DataType::Int64
            | crate::protocol::types::DataType::UInt8
            | crate::protocol::types::DataType::UInt16
            | crate::protocol::types::DataType::UInt32
            | crate::protocol::types::DataType::UInt64
            | crate::protocol::types::DataType::Float32
            | crate::protocol::types::DataType::Float64
            | crate::protocol::types::DataType::Date
            | crate::protocol::types::DataType::Time
            | crate::protocol::types::DataType::Timestamp
            | crate::protocol::types::DataType::VarChar
    )
}

pub struct TopNPushdownRule;

impl Rule for TopNPushdownRule {
    fn name(&self) -> &str {
        "topn_pushdown"
    }

    fn apply(&self, plan: LogicalPlan) -> (LogicalPlan, bool) {
        push_topn_to_scan(plan)
    }
}

fn push_topn_to_scan(plan: LogicalPlan) -> (LogicalPlan, bool) {
    match plan {
        LogicalPlan::TopN(mut topn) => match *topn.input {
            LogicalPlan::Scan(ref scan) => {
                if can_push_topn_to_scan(&topn, scan) {
                    let mut new_scan = scan.clone();
                    new_scan.pushed_order = Some(topn.sort_keys.clone());
                    new_scan.pushed_limit = Some(topn.limit + topn.offset);
                    let new_topn = LogicalPlan::TopN(TopNPlan {
                        sort_keys: topn.sort_keys,
                        limit: topn.limit,
                        offset: topn.offset,
                        input: Box::new(LogicalPlan::Scan(new_scan)),
                    });
                    return (new_topn, true);
                }
                topn.input = Box::new(LogicalPlan::Scan(scan.clone()));
                (LogicalPlan::TopN(topn), false)
            }
            LogicalPlan::Filter(ref filter) => {
                let topn_limit = topn.limit;
                let topn_offset = topn.offset;
                match *filter.input {
                    LogicalPlan::Scan(ref scan) => {
                        if can_push_topn_past_filter(&topn, &filter.predicate, scan) {
                            let mut new_scan = scan.clone();
                            new_scan.pushed_filter = Some(filter.predicate.clone());
                            new_scan.pushed_limit = Some(topn_limit * 3 + topn_offset);
                            let new_filter = crate::sql::planner::FilterPlan {
                                predicate: filter.predicate.clone(),
                                input: Box::new(LogicalPlan::Scan(new_scan)),
                            };
                            topn.input = Box::new(LogicalPlan::Filter(new_filter));
                            return (LogicalPlan::TopN(topn), true);
                        }
                        let new_filter = crate::sql::planner::FilterPlan {
                            predicate: filter.predicate.clone(),
                            input: Box::new(LogicalPlan::Scan(scan.clone())),
                        };
                        topn.input = Box::new(LogicalPlan::Filter(new_filter));
                        (LogicalPlan::TopN(topn), false)
                    }
                    LogicalPlan::Join(ref join)
                        if can_push_topn_to_filtered_left_join(
                            &topn.sort_keys,
                            &filter.predicate,
                            join,
                            topn_limit.saturating_add(topn_offset),
                        ) =>
                    {
                        let filter_pred = filter.predicate.clone();
                        let mut new_join = join.clone();
                        let original_left = new_join.left;
                        new_join.left = Box::new(LogicalPlan::TopN(TopNPlan {
                            sort_keys: topn.sort_keys.clone(),
                            limit: topn_limit.saturating_add(topn_offset),
                            offset: 0,
                            input: original_left,
                        }));
                        let (new_right, _) = push_topn_to_scan(*new_join.right);
                        new_join.right = Box::new(new_right);
                        topn.input =
                            Box::new(LogicalPlan::Filter(crate::sql::planner::FilterPlan {
                                predicate: filter_pred,
                                input: Box::new(LogicalPlan::Join(new_join)),
                            }));
                        (LogicalPlan::TopN(topn), true)
                    }
                    ref other => {
                        let filter_pred = filter.predicate.clone();
                        let other_plan = other.clone();
                        let (new_input, changed) = push_topn_to_scan(other_plan);
                        let new_filter = crate::sql::planner::FilterPlan {
                            predicate: filter_pred,
                            input: Box::new(new_input),
                        };
                        topn.input = Box::new(LogicalPlan::Filter(new_filter));
                        (LogicalPlan::TopN(topn), changed)
                    }
                }
            }
            other => {
                let sort_keys = topn.sort_keys.clone();
                let topn_limit = topn.limit;
                let topn_offset = topn.offset;
                if let LogicalPlan::Join(join) = &other {
                    if can_push_topn_to_left_join(
                        &sort_keys,
                        join,
                        topn_limit.saturating_add(topn_offset),
                    ) {
                        let mut new_join = join.clone();
                        let original_left = new_join.left;
                        new_join.left = Box::new(LogicalPlan::TopN(TopNPlan {
                            sort_keys: sort_keys.clone(),
                            limit: topn_limit.saturating_add(topn_offset),
                            offset: 0,
                            input: original_left,
                        }));
                        let (new_right, _) = push_topn_to_scan(*new_join.right);
                        new_join.right = Box::new(new_right);
                        topn.input = Box::new(LogicalPlan::Join(new_join));
                        return (LogicalPlan::TopN(topn), true);
                    }
                }
                let (new_input, changed) = push_topn_to_scan(other);
                topn.input = Box::new(new_input);
                (LogicalPlan::TopN(topn), changed)
            }
        },
        LogicalPlan::Limit(mut limit) => {
            let (new_input, changed) = push_topn_to_scan(*limit.input);
            limit.input = Box::new(new_input);
            (LogicalPlan::Limit(limit), changed)
        }
        LogicalPlan::Sort(mut sort) => {
            let (new_input, changed) = push_topn_to_scan(*sort.input);
            sort.input = Box::new(new_input);
            (LogicalPlan::Sort(sort), changed)
        }
        LogicalPlan::Filter(mut filter) => {
            let (new_input, changed) = push_topn_to_scan(*filter.input);
            filter.input = Box::new(new_input);
            (LogicalPlan::Filter(filter), changed)
        }
        LogicalPlan::Project(mut proj) => {
            let (new_input, changed) = push_topn_to_scan(*proj.input);
            proj.input = Box::new(new_input);
            (LogicalPlan::Project(proj), changed)
        }
        LogicalPlan::Aggregate(mut agg) => {
            let (new_input, changed) = push_topn_to_scan(*agg.input);
            agg.input = Box::new(new_input);
            (LogicalPlan::Aggregate(agg), changed)
        }
        LogicalPlan::Join(mut join) => {
            let (new_left, left_changed) = push_topn_to_scan(*join.left);
            let (new_right, right_changed) = push_topn_to_scan(*join.right);
            join.left = Box::new(new_left);
            join.right = Box::new(new_right);
            (LogicalPlan::Join(join), left_changed || right_changed)
        }
        LogicalPlan::SemiJoin(mut semi) => {
            let (new_left, left_changed) = push_topn_to_scan(*semi.left);
            let (new_right, right_changed) = push_topn_to_scan(*semi.right);
            semi.left = Box::new(new_left);
            semi.right = Box::new(new_right);
            (LogicalPlan::SemiJoin(semi), left_changed || right_changed)
        }
        LogicalPlan::Distinct(mut distinct) => {
            let (new_input, changed) = push_topn_to_scan(*distinct.input);
            distinct.input = Box::new(new_input);
            (LogicalPlan::Distinct(distinct), changed)
        }
        LogicalPlan::Window(mut window) => {
            let (new_input, changed) = push_topn_to_scan(*window.input);
            window.input = Box::new(new_input);
            (LogicalPlan::Window(window), changed)
        }
        LogicalPlan::Scan(scan) => (LogicalPlan::Scan(scan), false),
        LogicalPlan::Empty => (LogicalPlan::Empty, false),
    }
}

fn can_push_topn_to_scan(topn: &TopNPlan, scan: &ScanPlan) -> bool {
    if scan.pushed_filter.is_some() {
        return false;
    }
    if scan.pushed_order.is_some() || scan.pushed_limit.is_some() {
        return false;
    }
    can_push_order_to_scan(&topn.sort_keys, scan)
}

fn can_push_topn_to_left_join(sort_keys: &[SortKey], join: &JoinPlan, pushed_limit: usize) -> bool {
    if !matches!(join.join_type, JoinType::Left) {
        return false;
    }
    if contains_equivalent_topn(&join.left, sort_keys, pushed_limit) {
        return false;
    }

    sort_keys
        .iter()
        .all(|sk| expr_columns_within(&sk.expr, join.left_schema.columns.len()))
}

fn can_push_topn_to_filtered_left_join(
    sort_keys: &[SortKey],
    predicate: &LogicalExpr,
    join: &JoinPlan,
    pushed_limit: usize,
) -> bool {
    can_push_topn_to_left_join(sort_keys, join, pushed_limit)
        && expr_columns_within(predicate, join.left_schema.columns.len())
        && contains_filter_predicate(&join.left, predicate)
}

fn contains_equivalent_topn(
    plan: &LogicalPlan,
    sort_keys: &[SortKey],
    pushed_limit: usize,
) -> bool {
    match plan {
        LogicalPlan::TopN(topn) => {
            topn.offset == 0 && topn.limit <= pushed_limit && topn.sort_keys == sort_keys
                || contains_equivalent_topn(&topn.input, sort_keys, pushed_limit)
        }
        LogicalPlan::Filter(filter) => {
            contains_equivalent_topn(&filter.input, sort_keys, pushed_limit)
        }
        LogicalPlan::Project(project) => {
            contains_equivalent_topn(&project.input, sort_keys, pushed_limit)
        }
        LogicalPlan::Sort(sort) => contains_equivalent_topn(&sort.input, sort_keys, pushed_limit),
        LogicalPlan::Limit(limit) => {
            contains_equivalent_topn(&limit.input, sort_keys, pushed_limit)
        }
        LogicalPlan::Aggregate(agg) => {
            contains_equivalent_topn(&agg.input, sort_keys, pushed_limit)
        }
        LogicalPlan::Distinct(distinct) => {
            contains_equivalent_topn(&distinct.input, sort_keys, pushed_limit)
        }
        LogicalPlan::Window(window) => {
            contains_equivalent_topn(&window.input, sort_keys, pushed_limit)
        }
        LogicalPlan::Join(join) => {
            contains_equivalent_topn(&join.left, sort_keys, pushed_limit)
                || contains_equivalent_topn(&join.right, sort_keys, pushed_limit)
        }
        LogicalPlan::SemiJoin(semi) => {
            contains_equivalent_topn(&semi.left, sort_keys, pushed_limit)
                || contains_equivalent_topn(&semi.right, sort_keys, pushed_limit)
        }
        _ => false,
    }
}

fn contains_filter_predicate(plan: &LogicalPlan, predicate: &LogicalExpr) -> bool {
    match plan {
        LogicalPlan::Scan(scan) => scan
            .pushed_filter
            .as_ref()
            .is_some_and(|existing| expr_contains_conjunct(existing, predicate)),
        LogicalPlan::Filter(filter) => {
            expr_contains_conjunct(&filter.predicate, predicate)
                || contains_filter_predicate(&filter.input, predicate)
        }
        LogicalPlan::Project(project) => contains_filter_predicate(&project.input, predicate),
        LogicalPlan::Sort(sort) => contains_filter_predicate(&sort.input, predicate),
        LogicalPlan::Limit(limit) => contains_filter_predicate(&limit.input, predicate),
        LogicalPlan::TopN(topn) => contains_filter_predicate(&topn.input, predicate),
        LogicalPlan::Aggregate(agg) => contains_filter_predicate(&agg.input, predicate),
        LogicalPlan::Distinct(distinct) => contains_filter_predicate(&distinct.input, predicate),
        LogicalPlan::Window(window) => contains_filter_predicate(&window.input, predicate),
        LogicalPlan::Join(join) => {
            contains_filter_predicate(&join.left, predicate)
                || contains_filter_predicate(&join.right, predicate)
        }
        LogicalPlan::SemiJoin(semi) => {
            contains_filter_predicate(&semi.left, predicate)
                || contains_filter_predicate(&semi.right, predicate)
        }
        LogicalPlan::Empty => false,
    }
}

fn expr_contains_conjunct(existing: &LogicalExpr, predicate: &LogicalExpr) -> bool {
    if existing == predicate {
        return true;
    }
    match existing {
        LogicalExpr::BinaryOp {
            op: crate::sql::parser::ast::BinOp::And,
            left,
            right,
        } => expr_contains_conjunct(left, predicate) || expr_contains_conjunct(right, predicate),
        _ => false,
    }
}

fn expr_columns_within(expr: &LogicalExpr, upper_bound: usize) -> bool {
    match expr {
        LogicalExpr::Column(idx) => *idx < upper_bound,
        LogicalExpr::Literal(_) | LogicalExpr::Wildcard => true,
        LogicalExpr::BinaryOp { left, right, .. } => {
            expr_columns_within(left, upper_bound) && expr_columns_within(right, upper_bound)
        }
        LogicalExpr::Not(inner) | LogicalExpr::IsNull(inner) | LogicalExpr::IsNotNull(inner) => {
            expr_columns_within(inner, upper_bound)
        }
        LogicalExpr::Aggregate(agg) => expr_columns_within(&agg.arg, upper_bound),
        LogicalExpr::In { expr, .. } => expr_columns_within(expr, upper_bound),
        LogicalExpr::Between {
            expr, low, high, ..
        } => {
            expr_columns_within(expr, upper_bound)
                && expr_columns_within(low, upper_bound)
                && expr_columns_within(high, upper_bound)
        }
        LogicalExpr::Case {
            operand,
            when_clauses,
            else_clause,
        } => {
            operand
                .as_deref()
                .map(|e| expr_columns_within(e, upper_bound))
                .unwrap_or(true)
                && when_clauses.iter().all(|(when, then)| {
                    expr_columns_within(when, upper_bound) && expr_columns_within(then, upper_bound)
                })
                && else_clause
                    .as_deref()
                    .map(|e| expr_columns_within(e, upper_bound))
                    .unwrap_or(true)
        }
        LogicalExpr::Like { expr, .. } => expr_columns_within(expr, upper_bound),
        LogicalExpr::Function { args, .. } => {
            args.iter().all(|arg| expr_columns_within(arg, upper_bound))
        }
        LogicalExpr::InSubquery { expr, .. } => expr_columns_within(expr, upper_bound),
        LogicalExpr::ScalarSubquery(_) | LogicalExpr::ExistsSubquery(_) => false,
    }
}

fn can_push_topn_past_filter(topn: &TopNPlan, predicate: &LogicalExpr, scan: &ScanPlan) -> bool {
    if scan.pushed_filter.is_some() || scan.pushed_order.is_some() || scan.pushed_limit.is_some() {
        return false;
    }

    if !is_selective_predicate(predicate) {
        return false;
    }

    can_push_order_to_scan(&topn.sort_keys, scan)
}

fn is_selective_predicate(expr: &LogicalExpr) -> bool {
    match expr {
        LogicalExpr::BinaryOp {
            op: crate::sql::parser::ast::BinOp::And,
            left,
            right,
        } => is_selective_predicate(left) || is_selective_predicate(right),
        LogicalExpr::BinaryOp { op, .. } => {
            matches!(
                op,
                crate::sql::parser::ast::BinOp::Eq
                    | crate::sql::parser::ast::BinOp::Lt
                    | crate::sql::parser::ast::BinOp::Gt
            )
        }
        LogicalExpr::In { values, .. } => values.len() <= 100,
        LogicalExpr::Between { .. } => true,
        LogicalExpr::Like { .. } => false,
        _ => false,
    }
}

#[cfg(test)]
#[path = "order_limit_pushdown_tests.rs"]
mod tests;
