/// TableScan Strategy - fallback strategy that performs a full table scan.
use crate::common::error::Result;
use crate::types::batch::RecordBatch;

use super::strategy::{ExecutionStrategy, StrategyContext, costs};

pub struct TableScanStrategy;

impl ExecutionStrategy for TableScanStrategy {
    fn name(&self) -> &str {
        "table_scan"
    }

    fn can_execute(&self, ctx: &StrategyContext) -> Option<f64> {
        // Always can execute - this is the fallback
        let cost = ctx.properties.table_row_count as f64 * costs::COST_FULL_SCAN;
        Some(cost)
    }

    fn execute(&self, _ctx: &StrategyContext) -> Result<Option<Vec<RecordBatch>>> {
        // Falls back to the standard pipeline
        Ok(None)
    }
}
