/// Query Analyzer - analyzes query properties for strategy selection.
/// Inspired by taibai's QueryAnalyzer with selectivity estimation.
use super::strategy::{QueryProperties, QuerySignature};

pub struct QueryAnalyzer;

impl QueryAnalyzer {
    pub fn new() -> Self {
        Self
    }

    /// Compute estimated selectivity based on predicate types.
    /// Uses heuristics inspired by DuckDB's ExpressionHeuristics.
    pub fn estimate_selectivity(&self, props: &QueryProperties) -> f64 {
        if !props.has_where {
            return 1.0;
        }

        // Base selectivity per predicate (heuristic)
        let base_selectivity = 0.33; // range predicate default
        let eq_selectivity = 0.1;    // equality predicate

        // Simplified: assume mix of predicates
        let estimated = if props.predicate_count == 0 {
            1.0
        } else {
            let s = eq_selectivity.powi(props.predicate_count as i32 / 2)
                * base_selectivity.powi((props.predicate_count - props.predicate_count / 2) as i32);
            s.max(0.001).min(1.0)
        };

        estimated
    }

    /// Compute a cost estimate for a given strategy.
    pub fn estimate_cost(
        &self,
        strategy_name: &str,
        props: &QueryProperties,
    ) -> f64 {
        let row_cost = match strategy_name {
            "count_star" => super::strategy::costs::COST_COUNT_STAR,
            "no_from" => super::strategy::costs::COST_NO_FROM,
            "fast_scan" => super::strategy::costs::COST_FAST_SCAN,
            "optimized_scan" => super::strategy::costs::COST_OPTIMIZED_SCAN,
            "aggregation" => super::strategy::costs::COST_AGGREGATE,
            _ => super::strategy::costs::COST_FULL_SCAN,
        };

        let rows = props.table_row_count as f64;
        let selectivity = self.estimate_selectivity(props);
        rows * row_cost * selectivity
    }
}

impl Default for QueryAnalyzer {
    fn default() -> Self {
        Self::new()
    }
}
