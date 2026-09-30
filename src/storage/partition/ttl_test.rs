use super::*;

use crate::storage::catalog::schema::TableMeta;
use crate::types::batch::{ColumnDef, Schema};
use crate::types::scalar::ScalarValue;

fn ttl_meta() -> TableMeta {
    let schema = Schema::new(vec![
        ColumnDef::new("ts", crate::protocol::types::DataType::Timestamp),
        ColumnDef::new("price", crate::protocol::types::DataType::Float64),
    ]);
    let mut meta = TableMeta::new("main", "ticks", schema.columns.clone());
    meta.partition_type = crate::storage::catalog::schema::PartitionType::Day;
    meta.partition_key = "ts".to_string();
    meta
}

fn ttl_on(column_idx: usize, secs: i64) -> crate::storage::catalog::schema::TtlInfo {
    crate::storage::catalog::schema::TtlInfo {
        column: "ts".to_string(),
        duration_secs: secs,
        column_idx: column_idx as i32,
    }
}

#[test]
fn day_partition_period_end_is_last_micro_of_day() {
    let end = partition_period_end_micros(PartitionType::Day, "20240115").expect("day end");
    // 2024-01-16 00:00:00 UTC minus 1 micro.
    let next_day = chrono::NaiveDate::from_ymd_opt(2024, 1, 16)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc()
        .timestamp_micros();
    assert_eq!(end, next_day - 1);
}

#[test]
fn month_and_year_periods_end_before_next_period() {
    let month_end = partition_period_end_micros(PartitionType::Month, "202402").expect("month end");
    let march = chrono::NaiveDate::from_ymd_opt(2024, 3, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc()
        .timestamp_micros();
    assert_eq!(month_end, march - 1);

    let year_end = partition_period_end_micros(PartitionType::Year, "2023").expect("year end");
    let next_new_year = chrono::NaiveDate::from_ymd_opt(2024, 1, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc()
        .timestamp_micros();
    assert_eq!(year_end, next_new_year - 1);
}

#[test]
fn partition_fully_expired_respects_boundaries_and_guards() {
    let meta = ttl_meta();
    // TTL of one hour: the 20240115 partition is fully expired once now
    // reaches 2024-01-16 01:00:00 UTC.
    let ttl = ttl_on(0, 3_600);
    let boundary = chrono::NaiveDate::from_ymd_opt(2024, 1, 16)
        .unwrap()
        .and_hms_opt(1, 0, 0)
        .unwrap()
        .and_utc()
        .timestamp_micros();
    assert!(partition_fully_expired(&ttl, &meta, "20240115", boundary));
    // One second before the boundary the last row of the day is still live.
    assert!(!partition_fully_expired(
        &ttl,
        &meta,
        "20240115",
        boundary - 1_000_000
    ));

    // TTL column differing from the partition column can never be concluded.
    let ttl_other = ttl_on(1, 3_600);
    assert!(!partition_fully_expired(
        &ttl_other, &meta, "20240115", boundary
    ));

    // Non-temporal partitioning cannot be concluded either.
    let mut hash_meta = ttl_meta();
    hash_meta.partition_type = crate::storage::catalog::schema::PartitionType::Hash;
    assert!(!partition_fully_expired(&ttl, &hash_meta, "3", boundary));

    // Malformed keys are never reported expired.
    assert!(!partition_fully_expired(&ttl, &meta, "bad", boundary));
}

#[test]
fn partition_fully_expired_matches_read_filter_for_edge_rows() {
    // A row at the very last microsecond of the partition period must be
    // exactly expired when now == period_end + duration.
    let meta = ttl_meta();
    let ttl = ttl_on(0, 60);
    let period_end = partition_period_end_micros(PartitionType::Day, "20240115").unwrap();
    let now = period_end + 60_000_000;
    assert!(partition_fully_expired(&ttl, &meta, "20240115", now));
    // The read filter would drop that edge row: ts + duration > now is false.
    let edge_row_visible = period_end.saturating_add(60_000_000) > now;
    assert!(!edge_row_visible);
    let _ = ScalarValue::Int64(Some(0));
}
