use super::*;
use std::str::FromStr;

#[test]
fn test_partition_filename() {
    assert_eq!(partition_filename("20260501", 1), "p20260501_0001.dat");
    assert_eq!(partition_filename("20260501", 12), "p20260501_0012.dat");
    assert_eq!(partition_filename("2024", 3), "p2024_0003.dat");
}

#[test]
fn test_parse_partition_key() {
    assert_eq!(
        parse_partition_key("p20260501_0001.dat"),
        Some("20260501".to_string())
    );
    assert_eq!(
        parse_partition_key("p2024_0003.dat"),
        Some("2024".to_string())
    );
    assert_eq!(parse_partition_key("seg_00001.dat"), None);
    assert_eq!(parse_partition_key("random.dat"), None);
    assert_eq!(parse_partition_key("p_noseq.dat"), None);
}

#[test]
fn test_is_partition_segment() {
    assert!(is_partition_segment("p20260501_0001.dat"));
    assert!(!is_partition_segment("seg_00001.dat"));
    assert!(!is_partition_segment("p_invalid.dat"));
}

#[test]
fn test_is_regular_segment() {
    assert!(is_regular_segment("seg_00001.dat"));
    assert!(!is_regular_segment("p20260501_0001.dat"));
}

#[test]
fn test_compute_partition_key_day() {
    let val = ScalarValue::Int64(Some(1775040000000));
    let key = compute_partition_key(&val, PartitionStrategy::Day).unwrap();
    assert_eq!(key.len(), 8);
    assert!(key.starts_with("202"));
}

#[test]
fn test_compute_partition_key_year() {
    let val = ScalarValue::Int64(Some(1775040000000));
    let key = compute_partition_key(&val, PartitionStrategy::Year).unwrap();
    assert_eq!(key.len(), 4);
}

#[test]
fn test_partition_pruner_no_bounds() {
    let pruner = PartitionPruner::new(PartitionStrategy::Day);
    let partitions = vec![
        "20260501".to_string(),
        "20260502".to_string(),
        "20260503".to_string(),
    ];
    let result = pruner.prune(&partitions, None, None);
    assert_eq!(result.len(), 3);
}

#[test]
fn test_partition_strategy_from_str() {
    assert_eq!(
        PartitionStrategy::from_str("day").unwrap(),
        PartitionStrategy::Day
    );
    assert_eq!(
        PartitionStrategy::from_str("YEAR").unwrap(),
        PartitionStrategy::Year
    );
    assert_eq!(
        PartitionStrategy::from_str("month").unwrap(),
        PartitionStrategy::Month
    );
    assert!(PartitionStrategy::from_str("invalid").is_err());
}

/// Regression: bounds that are not valid YYYYMMDD-style dates (a wall-clock
/// `deal_time` value like 2000, or an invalid date like 20261000) must not
/// prune partitions. The old epoch-micros fallback reinterpreted them as
/// "197001" and eliminated every partition.
#[test]
fn pruner_ignores_bounds_that_are_not_valid_date_numbers() {
    let pruner = PartitionPruner::new(PartitionStrategy::Month);
    let partitions = vec!["202609".to_string()];

    let upper_wall_clock = ScalarValue::Int64(Some(2000));
    assert_eq!(
        pruner.prune(&partitions, None, Some(&upper_wall_clock)),
        partitions,
        "wall-clock upper bound must not prune"
    );

    let upper_invalid_date = ScalarValue::Int32(Some(20261000));
    assert_eq!(
        pruner.prune(&partitions, None, Some(&upper_invalid_date)),
        partitions,
        "invalid YYYYMMDD upper bound must not prune"
    );

    let lower_invalid_date = ScalarValue::Int64(Some(930));
    assert_eq!(
        pruner.prune(&partitions, Some(&lower_invalid_date), None),
        partitions,
        "invalid lower bound must not prune"
    );

    // Convertible bounds keep pruning.
    let upper_valid = ScalarValue::Int64(Some(20261101));
    assert_eq!(
        pruner.prune(&partitions, None, Some(&upper_valid)),
        partitions,
        "later-month upper bound keeps the partition"
    );
    let upper_before = ScalarValue::Int64(Some(20260831));
    assert!(
        pruner.prune(&partitions, None, Some(&upper_before)).is_empty(),
        "earlier-month upper bound prunes the partition"
    );
}
