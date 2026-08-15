#![allow(dead_code)]
use std::path::{Path, PathBuf};

use crate::common::error::{Result, TaibaiError};
use crate::types::scalar::ScalarValue;

/// Partition strategy for a table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionStrategy {
    /// No partitioning.
    None,
    /// Partition by day (YYYYMMDD).
    Day,
    /// Partition by year (YYYY).
    Year,
    /// Partition by month (YYYYMM).
    Month,
}

impl std::str::FromStr for PartitionStrategy {
    type Err = TaibaiError;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_uppercase().as_str() {
            "DAY" => Ok(PartitionStrategy::Day),
            "YEAR" => Ok(PartitionStrategy::Year),
            "MONTH" => Ok(PartitionStrategy::Month),
            _ => Err(TaibaiError::Sql(format!(
                "unknown partition strategy: '{}'",
                s
            ))),
        }
    }
}

/// Compute the partition key string from a scalar value and the partition strategy.
/// For DAY: returns "YYYYMMDD" (8 digits)
/// For MONTH: returns "YYYYMM" (6 digits)
/// For YEAR: returns "YYYY" (4 digits)
pub fn compute_partition_key(value: &ScalarValue, strategy: PartitionStrategy) -> Result<String> {
    let func = match strategy {
        PartitionStrategy::Day => {
            crate::storage::partition::partition_function::PartitionFunction::Day
        }
        PartitionStrategy::Month => {
            crate::storage::partition::partition_function::PartitionFunction::Month
        }
        PartitionStrategy::Year => {
            crate::storage::partition::partition_function::PartitionFunction::Year
        }
        PartitionStrategy::None => {
            return Err(TaibaiError::Internal(
                "cannot compute partition key for None strategy".into(),
            ))
        }
    };
    crate::storage::partition::partition_function::temporal_partition_key(&func, value).ok_or_else(
        || {
            TaibaiError::Sql(format!(
                "cannot extract partition key from value: {:?}",
                value
            ))
        },
    )
}

/// Build a partitioned segment filename.
/// Format: `p{partitionKey}_{seq:04}.dat` where seq is 1-based 4-digit zero-padded.
pub fn partition_filename(partition_key: &str, seq: u32) -> String {
    format!("p{}_{:04}.dat", partition_key, seq)
}

pub fn parse_partition_key(filename: &str) -> Option<String> {
    if !filename.starts_with('p') || !filename.contains('_') {
        return None;
    }
    let rest = &filename[1..];
    let underscore_pos = rest.find('_')?;
    let key = &rest[..underscore_pos];
    let suffix = &rest[underscore_pos + 1..];
    if !suffix.ends_with(".dat") {
        return None;
    }
    let seq_str = &suffix[..suffix.len() - 4];
    if seq_str.len() == 4 && seq_str.chars().all(|c| c.is_ascii_digit()) {
        Some(key.to_string())
    } else {
        None
    }
}

pub fn is_partition_segment(filename: &str) -> bool {
    parse_partition_key(filename).is_some()
}

pub fn is_regular_segment(filename: &str) -> bool {
    filename.starts_with("seg_") && filename.ends_with(".dat")
}

pub struct PartitionPruner {
    pub strategy: PartitionStrategy,
}

impl PartitionPruner {
    pub fn new(strategy: PartitionStrategy) -> Self {
        Self { strategy }
    }

    pub fn prune(
        &self,
        available_partitions: &[String],
        lower_bound: Option<&ScalarValue>,
        upper_bound: Option<&ScalarValue>,
    ) -> Vec<String> {
        if lower_bound.is_none() && upper_bound.is_none() {
            return available_partitions.to_vec();
        }

        let lower_key = lower_bound.and_then(|v| compute_partition_key(v, self.strategy).ok());
        let upper_key = upper_bound.and_then(|v| compute_partition_key(v, self.strategy).ok());

        available_partitions
            .iter()
            .filter(|key| {
                if let Some(ref lk) = lower_key {
                    if *key < lk {
                        return false;
                    }
                }
                if let Some(ref uk) = upper_key {
                    if *key > uk {
                        return false;
                    }
                }
                true
            })
            .cloned()
            .collect()
    }

    pub fn discover_partitions(&self, table_dir: &Path) -> Result<Vec<String>> {
        let mut partitions = Vec::new();
        if !table_dir.exists() {
            return Ok(partitions);
        }
        for entry in std::fs::read_dir(table_dir)? {
            let entry = entry?;
            if let Some(name) = entry.file_name().to_str() {
                if let Some(key) = parse_partition_key(name) {
                    if !partitions.contains(&key) {
                        partitions.push(key);
                    }
                }
            }
        }
        partitions.sort();
        Ok(partitions)
    }

    pub fn discover_partition_segments(
        &self,
        table_dir: &Path,
        partition_key: &str,
    ) -> Result<Vec<PathBuf>> {
        let mut files = Vec::new();
        if !table_dir.exists() {
            return Ok(files);
        }
        for entry in std::fs::read_dir(table_dir)? {
            let entry = entry?;
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if let Some(key) = parse_partition_key(name) {
                    if key == partition_key {
                        files.push(path);
                    }
                }
            }
        }
        files.sort();
        Ok(files)
    }
}

#[cfg(test)]
#[path = "partition_test.rs"]
mod tests;
