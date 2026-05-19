/// CountStar Strategy - handles simple COUNT(*) queries with O(1) metadata lookup.
use crate::common::error::Result;
use crate::types::batch::RecordBatch;

use super::strategy::{ExecutionStrategy, StrategyContext, costs};

pub struct CountStarStrategy;

impl ExecutionStrategy for CountStarStrategy {
    fn name(&self) -> &str {
        "count_star"
    }

    fn can_execute(&self, ctx: &StrategyContext) -> Option<f64> {
        if ctx.properties.is_count_star && !ctx.properties.has_group_by {
            Some(costs::COST_COUNT_STAR)
        } else {
            None
        }
    }

    fn execute(&self, _ctx: &StrategyContext) -> Result<Option<Vec<RecordBatch>>> {
        // In a real implementation, this would look up the row count from table metadata.
        // For now, this returns None to fall back to the standard pipeline.
        // The actual O(1) path would be:
        // 1. Get table metadata row count
        // 2. Create a single-row RecordBatch with COUNT(*) = row_count
        // 3. Return it directly without scanning any data
        Ok(None)
    }
}
