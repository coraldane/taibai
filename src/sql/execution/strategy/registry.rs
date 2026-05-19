/// Strategy Registry - manages available execution strategies and selects the best one.
/// Uses O(1) signature-based fast path lookup inspired by taibai.
use std::collections::HashMap;

use super::strategy::{ExecutionStrategy, QuerySignature, StrategyContext};

pub struct StrategyRegistry {
    strategies: Vec<Box<dyn ExecutionStrategy>>,
    /// Fast path cache: signature -> best strategy index
    cache: HashMap<QuerySignature, usize>,
}

impl StrategyRegistry {
    pub fn new() -> Self {
        Self {
            strategies: Vec::new(),
            cache: HashMap::new(),
        }
    }

    /// Register a new strategy.
    pub fn register(&mut self, strategy: Box<dyn ExecutionStrategy>) {
        self.strategies.push(strategy);
        self.cache.clear(); // Invalidate cache when strategies change
    }

    /// Select the best strategy for the given context.
    /// Returns the strategy name and estimated cost.
    pub fn select(&mut self, ctx: &StrategyContext) -> Option<(&str, f64)> {
        let sig = QuerySignature::from_properties(&ctx.properties);

        // Check cache first (O(1) fast path)
        if let Some(&idx) = self.cache.get(&sig) {
            let strategy = &self.strategies[idx];
            if let Some(cost) = strategy.can_execute(ctx) {
                return Some((strategy.name(), cost));
            }
        }

        // Find best strategy by cost
        let mut best_idx: Option<usize> = None;
        let mut best_cost = f64::MAX;

        for (idx, strategy) in self.strategies.iter().enumerate() {
            if let Some(cost) = strategy.can_execute(ctx) {
                if cost < best_cost {
                    best_cost = cost;
                    best_idx = Some(idx);
                }
            }
        }

        if let Some(idx) = best_idx {
            // Cache the result for future O(1) lookups
            self.cache.insert(sig, idx);
            Some((self.strategies[idx].name(), best_cost))
        } else {
            None
        }
    }

    /// Get a strategy by index (for execution after selection).
    pub fn get_strategy(&self, idx: usize) -> Option<&dyn ExecutionStrategy> {
        self.strategies.get(idx).map(|s| s.as_ref())
    }

    /// Create a registry with all default strategies registered.
    pub fn with_defaults() -> Self {
        let mut registry = Self::new();
        registry.register(Box::new(super::count_star::CountStarStrategy));
        registry.register(Box::new(super::no_from::NoFromStrategy));
        registry.register(Box::new(super::fast_scan::FastScanStrategy));
        registry.register(Box::new(super::optimized_scan::OptimizedScanStrategy));
        registry.register(Box::new(super::aggregation::AggregationStrategy));
        registry.register(Box::new(super::table_scan::TableScanStrategy));
        registry
    }
}

impl Default for StrategyRegistry {
    fn default() -> Self {
        Self::with_defaults()
    }
}
