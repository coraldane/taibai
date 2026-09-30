//! Partition-level TTL expiry: for temporal partitions whose TTL column is
//! the partition column, an entire partition becomes invisible once its
//! period end plus the TTL duration has passed, which makes an O(1) physical
//! drop possible (instead of row-level read filtering forever).

use crate::storage::catalog::schema::{PartitionType, TableMeta, TtlInfo};

/// Last micros that a row in this partition period could carry (the period's
/// final microsecond), computed in UTC to match the write-side
/// `temporal_partition_key` arithmetic.
pub fn partition_period_end_micros(partition_type: PartitionType, key: &str) -> Option<i64> {
    let digits = key.trim();
    match partition_type {
        PartitionType::Day => {
            if digits.len() != 8 {
                return None;
            }
            let year: i32 = digits[0..4].parse().ok()?;
            let month: u32 = digits[4..6].parse().ok()?;
            let day: u32 = digits[6..8].parse().ok()?;
            let date = chrono::NaiveDate::from_ymd_opt(year, month, day)?;
            Some(day_start_micros(date.succ_opt()?) - 1)
        }
        PartitionType::Month => {
            if digits.len() != 6 {
                return None;
            }
            let year: i32 = digits[0..4].parse().ok()?;
            let month: u32 = digits[4..6].parse().ok()?;
            let next_month = if month == 12 {
                chrono::NaiveDate::from_ymd_opt(year + 1, 1, 1)?
            } else {
                chrono::NaiveDate::from_ymd_opt(year, month + 1, 1)?
            };
            Some(day_start_micros(next_month) - 1)
        }
        PartitionType::Year => {
            if digits.len() != 4 {
                return None;
            }
            let year: i32 = digits.parse().ok()?;
            let next_year = chrono::NaiveDate::from_ymd_opt(year + 1, 1, 1)?;
            Some(day_start_micros(next_year) - 1)
        }
        _ => None,
    }
}

fn day_start_micros(date: chrono::NaiveDate) -> i64 {
    date.and_hms_opt(0, 0, 0)
        .expect("midnight is always a valid time")
        .and_utc()
        .timestamp_micros()
}

/// True when every possible row in the partition is already expired under the
/// read-time TTL filter (`ts + duration <= now` means invisible). Only
/// temporal partitions whose TTL column IS the partition column qualify.
pub fn partition_fully_expired(
    ttl: &TtlInfo,
    table_meta: &TableMeta,
    key: &str,
    now_micros: i64,
) -> bool {
    let Some(partition_col) = table_meta.get_partition_column_index() else {
        return false;
    };
    if partition_col != ttl.column_idx as usize {
        // Rows in the partition can carry TTL-column values far outside the
        // partition period, so whole-partition expiry cannot be concluded.
        return false;
    }
    let Some(period_end) = partition_period_end_micros(table_meta.partition_type, key) else {
        return false;
    };
    let duration = ttl.duration_secs.saturating_mul(1_000_000);
    period_end.saturating_add(duration) <= now_micros
}

#[cfg(test)]
#[path = "ttl_test.rs"]
mod tests;
