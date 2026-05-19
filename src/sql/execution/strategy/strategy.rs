/// Strategy trait and related types for strategy-based query execution.
/// Inspired by taibai's Strategy pattern and DuckDB's execution path selection.
use crate::common::error::Result;
use crate::types::batch::RecordBatch;

/// Properties extracted from a query for strategy selection.
#[derive(Debug, Clone)]
pub struct QueryProperties {
    /// Does the query have aggregate functions?
    pub has_aggregates: bool,
    /// Does the query have GROUP BY?
    pub has_group_by: bool,
    /// Does the query have a WHERE clause?
    pub has_where: bool,
    /// Number of predicate conditions.
    pub predicate_count: usize,
    /// Does the query have JOINs?
    pub has_joins: bool,
    /// Does the query have LIMIT?
    pub has_limit: bool,
    /// Estimated row count of the table.
    pub table_row_count: usize,
    /// Can predicates be pushed down to scan?
    pub can_pushdown: bool,
    /// Is this a simple COUNT(*) without GROUP BY?
    pub is_count_star: bool,
    /// Is there no FROM clause?
    pub no_from: bool,
    /// Is there an ORDER BY?
    pub has_order_by: bool,
    /// Is there a DISTINCT?
    pub has_distinct: bool,
    /// Estimated selectivity from predicates (0.0 to 1.0).
    pub estimated_selectivity: f64,
    /// Column names referenced in the query.
    pub referenced_columns: Vec<String>,
}

impl Default for QueryProperties {
    fn default() -> Self {
        Self {
            has_aggregates: false,
            has_group_by: false,
            has_where: false,
            predicate_count: 0,
            has_joins: false,
            has_limit: false,
            table_row_count: 0,
            can_pushdown: false,
            is_count_star: false,
            no_from: false,
            has_order_by: false,
            has_distinct: false,
            estimated_selectivity: 1.0,
            referenced_columns: Vec::new(),
        }
    }
}

/// A fingerprint of query characteristics for fast path lookup.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QuerySignature {
    pub has_aggregates: bool,
    pub has_group_by: bool,
    pub has_where: bool,
    pub predicate_count: usize,
    pub has_joins: bool,
    pub has_limit: bool,
}

impl QuerySignature {
    pub fn from_properties(props: &QueryProperties) -> Self {
        Self {
            has_aggregates: props.has_aggregates,
            has_group_by: props.has_group_by,
            has_where: props.has_where,
            predicate_count: props.predicate_count,
            has_joins: props.has_joins,
            has_limit: props.has_limit,
        }
    }
}

/// Context provided to strategies for execution.
#[derive(Debug, Clone)]
pub struct StrategyContext {
    pub properties: QueryProperties,
    pub table_name: Option<String>,
}

/// Cost constants for strategy cost estimation.
pub mod costs {
    /// Cost per row for a full table scan.
    pub const COST_FULL_SCAN: f64 = 1.0;
    /// Cost per row for an optimized scan with predicate pushdown.
    pub const COST_OPTIMIZED_SCAN: f64 = 0.5;
    /// Cost per row for a fast path (index/equality lookup).
    pub const COST_FAST_SCAN: f64 = 0.1;
    /// Cost per row for aggregation.
    pub const COST_AGGREGATE: f64 = 2.0;
    /// Cost for COUNT(*) metadata lookup (O(1)).
    pub const COST_COUNT_STAR: f64 = 0.01;
    /// Cost for no-FROM query evaluation.
    pub const COST_NO_FROM: f64 = 0.001;
}

/// Trait for query execution strategies.
pub trait ExecutionStrategy: Send + Sync {
    /// Returns the name of this strategy.
    fn name(&self) -> &str;

    /// Check if this strategy can handle the given context and return an estimated cost.
    /// Returns None if the strategy cannot handle this query.
    fn can_execute(&self, ctx: &StrategyContext) -> Option<f64>;

    /// Execute the strategy and return result batches.
    /// This is the fast-path execution; if it returns None, the system falls back
    /// to the standard pipeline.
    fn execute(&self, ctx: &StrategyContext) -> Result<Option<Vec<RecordBatch>>>;
}
