/// Aggregation Strategy - handles queries with GROUP BY or aggregate functions.
/// Uses optimized hash-based aggregation for in-memory processing.
use crate::common::error::Result;
use crate::types::batch::RecordBatch;

use super::strategy::{ExecutionStrategy, StrategyContext, costs};

pub struct AggregationStrategy;

impl ExecutionStrategy for AggregationStrategy {
    fn name(&self) -> &str {
        "aggregation"
    }

    fn can_execute(&self, ctx: &StrategyContext) -> Option<f64> {
        if ctx.properties.has_aggregates || ctx.properties.has_group_by {
            let base_cost = ctx.properties.table_row_count as f64 * costs::COST_FULL_SCAN;
            let agg_cost = base_cost * costs::COST_AGGREGATE;
            // If we can push down filters, reduce the cost
            let selectivity = if ctx.properties.has_where { 0.3 } else { 1.0 };
            Some(agg_cost * selectivity)
        } else {
            None
        }
    }

    fn execute(&self, _ctx: &StrategyContext) -> Result<Option<Vec<RecordBatch>>> {
        // The aggregation is handled by the standard pipeline's HashAggregateExec.
        // Future optimization: one-pass aggregation for simple cases (no GROUP BY, single aggregate).
        Ok(None)
    }
}
