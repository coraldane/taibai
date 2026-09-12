#[allow(clippy::module_inception)]
pub mod partition;
pub mod partition_function;
pub mod pruning;
pub mod ttl;

pub use partition::{
    compute_partition_key, is_partition_segment, parse_partition_key, PartitionPruner,
    PartitionStrategy,
};
