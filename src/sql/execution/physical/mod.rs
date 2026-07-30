pub mod context;
pub mod planner;

pub use context::PhysicalContext;
pub(crate) use planner::build_profiled_materialized_pipeline;
pub use planner::{build_physical_pipeline, can_use_physical_pipeline, drain_pipeline};
