/// NoFrom Strategy - handles SELECT queries without a FROM clause.
/// Example: SELECT 1+1, 'hello'
use crate::common::error::Result;
use crate::types::batch::RecordBatch;

use super::strategy::{ExecutionStrategy, StrategyContext, costs};

pub struct NoFromStrategy;

impl ExecutionStrategy for NoFromStrategy {
    fn name(&self) -> &str {
        "no_from"
    }

    fn can_execute(&self, ctx: &StrategyContext) -> Option<f64> {
        if ctx.properties.no_from {
            Some(costs::COST_NO_FROM)
        } else {
            None
        }
    }

    fn execute(&self, _ctx: &StrategyContext) -> Result<Option<Vec<RecordBatch>>> {
        // Handled directly by the query executor's literal select path.
        Ok(None)
    }
}
