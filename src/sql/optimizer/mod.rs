#![allow(dead_code)]
pub mod bloom_filter;
pub mod column_pruning;
pub mod cost;
pub mod expr_rules;
pub mod filter_reorder;
pub mod index_selection;
pub mod join_reorder;
pub mod limit_pushdown;
pub mod order_limit_pushdown;
pub mod partition_pruning;
pub mod predicate_pushdown;
pub mod semi_join;
pub mod subquery;
pub mod topn;
pub mod zone_map_pruning;

use crate::sql::planner::{LogicalExpr, LogicalPlan};

pub trait Rule: Send + Sync {
    fn name(&self) -> &str;
    fn apply(&self, plan: LogicalPlan) -> (LogicalPlan, bool);
}

pub struct Optimizer {
    rules: Vec<Box<dyn Rule>>,
}

impl Optimizer {
    pub fn new() -> Self {
        Self {
            rules: vec![
                Box::new(expr_rules::ExpressionSimplificationRule),
                Box::new(predicate_pushdown::PredicatePushdownRule),
                Box::new(column_pruning::ColumnPruningRule),
                Box::new(zone_map_pruning::ZoneMapPruningRule),
                Box::new(bloom_filter::BloomFilterRule),
                Box::new(limit_pushdown::LimitPushdownRule),
                Box::new(order_limit_pushdown::OrderLimitPushdownRule),
                Box::new(topn::TopNRule),
                Box::new(order_limit_pushdown::TopNPushdownRule),
                Box::new(filter_reorder::FilterReorderRule),
                Box::new(index_selection::IndexSelectionRule),
                Box::new(partition_pruning::PartitionPruningRule),
                Box::new(join_reorder::JoinReorderRule),
                Box::new(semi_join::SemiJoinRule),
                Box::new(semi_join::SemiJoinColumnPruningRule),
                Box::new(subquery::SubqueryRule),
            ],
        }
    }

    pub fn add_rule(&mut self, rule: Box<dyn Rule>) {
        self.rules.push(rule);
    }

    pub fn optimize(&self, plan: LogicalPlan) -> LogicalPlan {
        let mut plan = plan;
        let max_iterations = 100;

        for _ in 0..max_iterations {
            let mut changed = false;
            for rule in &self.rules {
                let old_complexity = plan_complexity(&plan);
                let (new_plan, applied) = rule.apply(plan);
                let new_complexity = plan_complexity(&new_plan);
                if applied && new_complexity != old_complexity {
                    changed = true;
                }
                plan = new_plan;
            }
            if !changed {
                break;
            }
        }

        plan
    }
}

impl Default for Optimizer {
    fn default() -> Self {
        Self::new()
    }
}

pub fn optimize(plan: LogicalPlan) -> LogicalPlan {
    Optimizer::new().optimize(plan)
}

fn plan_complexity(plan: &LogicalPlan) -> usize {
    match plan {
        LogicalPlan::Scan(s) => {
            1 + s.projection.as_ref().map(|p| p.len()).unwrap_or(0)
                + s.pushed_filter.as_ref().map(expr_complexity).unwrap_or(0)
                + s.zone_map_filters.len()
                + s.pushed_limit.unwrap_or(0).min(1)
                + s.pushed_order
                    .as_ref()
                    .map(|keys| {
                        keys.iter()
                            .map(|sk| expr_complexity(&sk.expr))
                            .sum::<usize>()
                    })
                    .unwrap_or(0)
                + s.index_columns.len()
                + s.time_range.as_ref().map(|_| 1).unwrap_or(0)
                + s.bloom_filter_columns.len()
                + s.minmax_index_columns.len()
                + s.set_index_columns.len()
                + s.partition_filter.as_ref().map(|_| 1).unwrap_or(0)
        }
        LogicalPlan::Empty => 1,
        LogicalPlan::Filter(f) => 1 + expr_complexity(&f.predicate) + plan_complexity(&f.input),
        LogicalPlan::Project(p) => {
            1 + p.expressions.iter().map(expr_complexity).sum::<usize>() + plan_complexity(&p.input)
        }
        LogicalPlan::Aggregate(a) => {
            1 + a.group_by.iter().map(expr_complexity).sum::<usize>()
                + a.aggregates
                    .iter()
                    .map(|agg| expr_complexity(&agg.arg))
                    .sum::<usize>()
                + a.having.as_ref().map(expr_complexity).unwrap_or(0)
                + plan_complexity(&a.input)
        }
        LogicalPlan::Sort(s) => {
            1 + s
                .sort_keys
                .iter()
                .map(|sk| expr_complexity(&sk.expr))
                .sum::<usize>()
                + plan_complexity(&s.input)
        }
        LogicalPlan::Limit(l) => 1 + plan_complexity(&l.input),
        LogicalPlan::Join(j) => {
            1 + expr_complexity(&j.condition) + plan_complexity(&j.left) + plan_complexity(&j.right)
        }
        LogicalPlan::SemiJoin(s) => {
            1 + expr_complexity(&s.condition) + plan_complexity(&s.left) + plan_complexity(&s.right)
        }
        LogicalPlan::TopN(t) => {
            1 + t
                .sort_keys
                .iter()
                .map(|sk| expr_complexity(&sk.expr))
                .sum::<usize>()
                + plan_complexity(&t.input)
        }
        LogicalPlan::Distinct(d) => 1 + plan_complexity(&d.input),
        LogicalPlan::Window(w) => {
            1 + w.partition_by.iter().map(expr_complexity).sum::<usize>()
                + w.order_by
                    .iter()
                    .map(|sk| expr_complexity(&sk.expr))
                    .sum::<usize>()
                + plan_complexity(&w.input)
        }
    }
}

fn expr_complexity(expr: &LogicalExpr) -> usize {
    match expr {
        LogicalExpr::Column(_) | LogicalExpr::Literal(_) | LogicalExpr::Wildcard => 1,
        LogicalExpr::BinaryOp { left, right, .. } => {
            1 + expr_complexity(left) + expr_complexity(right)
        }
        LogicalExpr::Not(inner) | LogicalExpr::IsNull(inner) | LogicalExpr::IsNotNull(inner) => {
            1 + expr_complexity(inner)
        }
        LogicalExpr::Aggregate(agg) => 1 + expr_complexity(&agg.arg),
        LogicalExpr::In { expr, .. } => 1 + expr_complexity(expr),
        LogicalExpr::Between {
            expr, low, high, ..
        } => 1 + expr_complexity(expr) + expr_complexity(low) + expr_complexity(high),
        LogicalExpr::Case {
            operand,
            when_clauses,
            else_clause,
        } => {
            1 + operand.as_ref().map(|e| expr_complexity(e)).unwrap_or(0)
                + when_clauses
                    .iter()
                    .map(|(when, then)| expr_complexity(when) + expr_complexity(then))
                    .sum::<usize>()
                + else_clause
                    .as_ref()
                    .map(|e| expr_complexity(e))
                    .unwrap_or(0)
        }
        LogicalExpr::Like { expr, .. } => 1 + expr_complexity(expr),
        LogicalExpr::Function { args, .. } => 1 + args.iter().map(expr_complexity).sum::<usize>(),
        LogicalExpr::ScalarSubquery(plan) | LogicalExpr::ExistsSubquery(plan) => {
            1 + plan_complexity(plan)
        }
        LogicalExpr::InSubquery { expr, subquery, .. } => {
            1 + expr_complexity(expr) + plan_complexity(subquery)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::types::DataType;
    use crate::sql::parser::parser::Parser;
    use crate::sql::planner::{plan_select, LogicalPlan};
    use crate::types::batch::{ColumnDef, Schema};
    use std::collections::HashMap;

    fn schemas() -> HashMap<String, Schema> {
        let mut schemas = HashMap::new();
        schemas.insert(
            "t_stock_daily".to_string(),
            Schema::new(vec![
                ColumnDef::new("security_id", DataType::Int64),
                ColumnDef::new("deal_date", DataType::Int64),
                ColumnDef::new("open_px", DataType::Float64),
                ColumnDef::new("high_px", DataType::Float64),
                ColumnDef::new("low_px", DataType::Float64),
                ColumnDef::new("close_px", DataType::Float64),
                ColumnDef::new("pre_close", DataType::Float64),
                ColumnDef::new("trade_value", DataType::Float64),
            ]),
        );
        schemas.insert(
            "t_security_info".to_string(),
            Schema::new(vec![
                ColumnDef::new("security_id", DataType::Int64),
                ColumnDef::new("display_name", DataType::VarChar),
            ]),
        );
        schemas.insert(
            "t_valuation".to_string(),
            Schema::new(vec![
                ColumnDef::new("deal_date", DataType::Int64),
                ColumnDef::new("security_id", DataType::Int64),
                ColumnDef::new("turnover_ratio", DataType::Float64),
                ColumnDef::new("pe_ratio", DataType::Float64),
                ColumnDef::new("pb_ratio", DataType::Float64),
                ColumnDef::new("market_cap", DataType::Float64),
                ColumnDef::new("circulating_market_cap", DataType::Float64),
            ]),
        );
        schemas
    }

    #[test]
    fn optimizer_stabilizes_when_rule_reports_no_effective_change() {
        let plan = LogicalPlan::Empty;
        let optimized = Optimizer::new().optimize(plan.clone());
        assert_eq!(optimized, plan);
    }

    #[test]
    fn optimizes_join_order_by_expression_limit_offset_query() {
        let sql = "SELECT d.security_id, s.display_name,
            d.open_px, d.high_px, d.low_px, d.close_px, d.pre_close,
            d.trade_value,
            v.turnover_ratio, v.pe_ratio, v.pb_ratio, v.market_cap, v.circulating_market_cap
            FROM t_stock_daily d
            LEFT JOIN t_security_info s ON d.security_id = s.security_id
            LEFT JOIN t_valuation v ON v.deal_date = 20260605 AND d.security_id = v.security_id
            WHERE d.deal_date = 20260605
            ORDER BY (d.close_px - d.pre_close) / d.pre_close DESC
            LIMIT 20 OFFSET 0";
        let stmts = Parser::parse(sql).unwrap();
        let plan = plan_select(&stmts[0], &schemas()).unwrap();
        let optimized = Optimizer::new().optimize(plan);
        assert!(matches!(optimized, LogicalPlan::Project(_)));
    }
}
