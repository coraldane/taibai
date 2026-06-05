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

use crate::sql::planner::LogicalPlan;

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
                let (new_plan, applied) = rule.apply(plan);
                if applied {
                    plan = new_plan;
                    changed = true;
                } else {
                    plan = new_plan;
                }
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
