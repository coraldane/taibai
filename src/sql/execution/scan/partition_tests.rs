use std::path::PathBuf;

use crate::protocol::types::DataType;
use crate::storage::catalog::schema::{PartitionType, TableMeta};
use crate::types::batch::ColumnDef;
use crate::types::scalar::ScalarValue;

use super::*;

#[test]
fn year_partition_eq_uses_year_key_from_date_int() {
    let files = vec![
        PathBuf::from("p2025_0001.dat"),
        PathBuf::from("p2026_0001.dat"),
    ];
    let mut meta = TableMeta::new(
        "main",
        "test_table",
        vec![ColumnDef::new("partition_key", DataType::Int64)],
    );
    meta.partition_type = PartitionType::Year;
    meta.partition_key = "partition_key".to_string();
    meta.update_partition_key_idx();

    let mut spec = ScanSpec::new("test_table");
    spec.eq_predicates
        .push((0, ScalarValue::Int64(Some(20260605))));
    spec.partition_info = Some(PartitionScanInfo {
        time_range: None,
        partition_filter: None,
        partition_key_col_idx: meta.get_partition_column_index(),
        is_partitioned: true,
    });

    let pruned = prune_segments_by_partition(files, &spec, Some(&meta));

    assert_eq!(pruned, vec![PathBuf::from("p2026_0001.dat")]);
}

#[test]
fn year_partition_upper_bound_excludes_later_years() {
    let files = vec![
        PathBuf::from("p2025_0001.dat"),
        PathBuf::from("p2026_0001.dat"),
    ];
    let mut meta = TableMeta::new(
        "main",
        "test_table",
        vec![ColumnDef::new("deal_date", DataType::Int32)],
    );
    meta.partition_type = PartitionType::Year;
    meta.partition_key = "deal_date".to_string();
    meta.update_partition_key_idx();

    let mut spec = ScanSpec::new("test_table");
    spec.partition_info = Some(PartitionScanInfo {
        time_range: Some(crate::sql::planner::TimeRange {
            column: "deal_date".to_string(),
            start: None,
            end: Some(ScalarValue::Int64(Some(20251231))),
        }),
        partition_filter: None,
        partition_key_col_idx: meta.get_partition_column_index(),
        is_partitioned: true,
    });

    let pruned = prune_segments_by_partition(files, &spec, Some(&meta));

    assert_eq!(pruned, vec![PathBuf::from("p2025_0001.dat")]);
}
