pub mod strategy;
pub mod registry;
pub mod analyzer;
pub mod count_star;
pub mod no_from;
pub mod table_scan;
pub mod fast_scan;
pub mod aggregation;
pub mod optimized_scan;

pub use strategy::{ExecutionStrategy, StrategyContext, QueryProperties, QuerySignature};
pub use registry::StrategyRegistry;
pub use analyzer::QueryAnalyzer;
