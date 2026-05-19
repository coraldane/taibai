pub mod predicate_pushdown;
pub mod column_pruning;
pub mod zone_map_pruning;
pub mod limit_pushdown;
pub mod index_selection;
pub mod partition_pruning;
pub mod join_reorder;
pub mod bloom_filter;
pub mod cost;
pub mod topn;
pub mod expr_rules;
pub mod filter_reorder;
pub mod subquery;

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
                // Expression simplification first (cheapest)
                Box::new(expr_rules::ExpressionSimplificationRule),
                // Predicate pushdown before scan optimization
                Box::new(predicate_pushdown::PredicatePushdownRule),
                // Column pruning
                Box::new(column_pruning::ColumnPruningRule),
                // Zone map pruning
                Box::new(zone_map_pruning::ZoneMapPruningRule),
                // Bloom filter optimization
                Box::new(bloom_filter::BloomFilterRule),
                // Limit pushdown
                Box::new(limit_pushdown::LimitPushdownRule),
                // TopN: combine ORDER BY + LIMIT
                Box::new(topn::TopNRule),
                // Filter reordering: put most selective first
                Box::new(filter_reorder::FilterReorderRule),
                // Index selection
                Box::new(index_selection::IndexSelectionRule),
                // Partition pruning
                Box::new(partition_pruning::PartitionPruningRule),
                // Join reorder
                Box::new(join_reorder::JoinReorderRule),
                // Subquery optimization (last, as it may need to execute subqueries)
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
