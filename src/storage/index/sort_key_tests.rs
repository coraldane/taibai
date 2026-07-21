use super::*;
use crate::protocol::types::DataType;
use crate::types::batch::{ColumnDef, Schema};

fn make_schema() -> Schema {
    Schema::new(vec![
        ColumnDef::new("id", DataType::Int64),
        ColumnDef::new("name", DataType::VarChar),
        ColumnDef::new("score", DataType::Float64),
    ])
}

fn make_sorted_batch() -> RecordBatch {
    let schema = make_schema();
    let mut batch = RecordBatch::new(schema);
    let data: Vec<(i64, &str, f64)> = vec![
        (1, "alice", 85.0),
        (3, "charlie", 72.5),
        (5, "eve", 91.0),
        (7, "grace", 88.0),
        (9, "irene", 79.5),
    ];
    for (id, name, score) in data {
        batch
            .column_mut(0)
            .unwrap()
            .push(&ScalarValue::Int64(Some(id)))
            .unwrap();
        batch
            .column_mut(1)
            .unwrap()
            .push(&ScalarValue::VarChar(Some(name.to_string())))
            .unwrap();
        batch
            .column_mut(2)
            .unwrap()
            .push(&ScalarValue::Float64(Some(score)))
            .unwrap();
    }
    batch
}

#[test]
fn test_find_insert_position_beginning() {
    let batch = make_sorted_batch();
    let inserter = SortKeyInserter::new(vec![0], vec![false]);
    let key = vec![ScalarValue::Int64(Some(0))];
    let pos = inserter.find_insert_position(&batch, &key).unwrap();
    assert_eq!(pos, 0);
}

#[test]
fn test_find_insert_position_end() {
    let batch = make_sorted_batch();
    let inserter = SortKeyInserter::new(vec![0], vec![false]);
    let key = vec![ScalarValue::Int64(Some(100))];
    let pos = inserter.find_insert_position(&batch, &key).unwrap();
    assert_eq!(pos, 5);
}

#[test]
fn test_find_insert_position_middle() {
    let batch = make_sorted_batch();
    let inserter = SortKeyInserter::new(vec![0], vec![false]);
    let key = vec![ScalarValue::Int64(Some(6))];
    let pos = inserter.find_insert_position(&batch, &key).unwrap();
    assert_eq!(pos, 3); // Between 5 and 7
}

#[test]
fn test_insert_sorted() {
    let batch = make_sorted_batch();
    let inserter = SortKeyInserter::new(vec![0], vec![false]);
    let values = vec![
        ScalarValue::Int64(Some(6)),
        ScalarValue::VarChar(Some("frank".to_string())),
        ScalarValue::Float64(Some(80.0)),
    ];
    let new_batch = inserter.insert_sorted(&batch, &values).unwrap();
    assert_eq!(new_batch.num_rows(), 6);
    // Check that id=6 is at position 3
    assert_eq!(new_batch.scalar(3, 0).unwrap(), ScalarValue::Int64(Some(6)));
    // Verify sorted
    assert!(inserter.is_sorted(&new_batch));
}

#[test]
fn test_is_sorted_true() {
    let batch = make_sorted_batch();
    let inserter = SortKeyInserter::new(vec![0], vec![false]);
    assert!(inserter.is_sorted(&batch));
}

#[test]
fn test_is_sorted_false() {
    let schema = make_schema();
    let mut batch = RecordBatch::new(schema);
    batch
        .column_mut(0)
        .unwrap()
        .push(&ScalarValue::Int64(Some(5)))
        .unwrap();
    batch
        .column_mut(1)
        .unwrap()
        .push(&ScalarValue::VarChar(Some("a".into())))
        .unwrap();
    batch
        .column_mut(2)
        .unwrap()
        .push(&ScalarValue::Float64(Some(1.0)))
        .unwrap();
    batch
        .column_mut(0)
        .unwrap()
        .push(&ScalarValue::Int64(Some(3)))
        .unwrap();
    batch
        .column_mut(1)
        .unwrap()
        .push(&ScalarValue::VarChar(Some("b".into())))
        .unwrap();
    batch
        .column_mut(2)
        .unwrap()
        .push(&ScalarValue::Float64(Some(2.0)))
        .unwrap();
    let inserter = SortKeyInserter::new(vec![0], vec![false]);
    assert!(!inserter.is_sorted(&batch));
}

#[test]
fn test_extract_min_max() {
    let batch = make_sorted_batch();
    let inserter = SortKeyInserter::new(vec![0], vec![false]);
    let min_max = inserter.extract_min_max(&batch);
    assert_eq!(min_max.len(), 1);
    assert_eq!(min_max[0].0, Some(ScalarValue::Int64(Some(1))));
    assert_eq!(min_max[0].1, Some(ScalarValue::Int64(Some(9))));
}

#[test]
fn test_range_overlaps() {
    let batch = make_sorted_batch();
    let inserter = SortKeyInserter::new(vec![0], vec![false]);
    let min_max = inserter.extract_min_max(&batch);

    // Range fully within segment
    assert!(inserter.range_overlaps(
        &min_max,
        Some(&ScalarValue::Int64(Some(2))),
        Some(&ScalarValue::Int64(Some(8)))
    ));

    // Range before segment
    assert!(!inserter.range_overlaps(
        &min_max,
        Some(&ScalarValue::Int64(Some(-5))),
        Some(&ScalarValue::Int64(Some(0)))
    ));

    // Range after segment
    assert!(!inserter.range_overlaps(
        &min_max,
        Some(&ScalarValue::Int64(Some(10))),
        Some(&ScalarValue::Int64(Some(20)))
    ));

    // Range overlapping start
    assert!(inserter.range_overlaps(
        &min_max,
        Some(&ScalarValue::Int64(Some(0))),
        Some(&ScalarValue::Int64(Some(2)))
    ));

    // No bounds = always matches
    assert!(inserter.range_overlaps(&min_max, None, None));
}

#[test]
fn test_segment_index_prune() {
    let mut index = SegmentIndex::new();
    let inserter = SortKeyInserter::new(vec![0], vec![false]);

    // Add two segments: [1,5] and [10,20]
    index.add_entry(
        std::path::PathBuf::from("seg_00001.dat"),
        5,
        vec![(
            Some(ScalarValue::Int64(Some(1))),
            Some(ScalarValue::Int64(Some(5))),
        )],
    );
    index.add_entry(
        std::path::PathBuf::from("seg_00002.dat"),
        10,
        vec![(
            Some(ScalarValue::Int64(Some(10))),
            Some(ScalarValue::Int64(Some(20))),
        )],
    );

    // Query range [3,7] should only match first segment
    let pruned = index.prune_ranges(
        &inserter,
        Some(&ScalarValue::Int64(Some(3))),
        Some(&ScalarValue::Int64(Some(7))),
    );
    assert_eq!(pruned.len(), 1);
    assert!(pruned[0].to_string_lossy().contains("seg_00001"));

    // Query range [15,25] should only match second segment
    let pruned = index.prune_ranges(
        &inserter,
        Some(&ScalarValue::Int64(Some(15))),
        Some(&ScalarValue::Int64(Some(25))),
    );
    assert_eq!(pruned.len(), 1);
    assert!(pruned[0].to_string_lossy().contains("seg_00002"));

    // Query range [0,30] should match both
    let pruned = index.prune_ranges(
        &inserter,
        Some(&ScalarValue::Int64(Some(0))),
        Some(&ScalarValue::Int64(Some(30))),
    );
    assert_eq!(pruned.len(), 2);
}

#[test]
fn test_descending_sort() {
    let schema = make_schema();
    let mut batch = RecordBatch::new(schema);
    for id in [9i64, 7, 5, 3, 1] {
        batch
            .column_mut(0)
            .unwrap()
            .push(&ScalarValue::Int64(Some(id)))
            .unwrap();
        batch
            .column_mut(1)
            .unwrap()
            .push(&ScalarValue::VarChar(Some(format!("u{}", id))))
            .unwrap();
        batch
            .column_mut(2)
            .unwrap()
            .push(&ScalarValue::Float64(Some(id as f64)))
            .unwrap();
    }
    let inserter = SortKeyInserter::new(vec![0], vec![true]);
    assert!(inserter.is_sorted(&batch));

    let key = vec![ScalarValue::Int64(Some(6))];
    let pos = inserter.find_insert_position(&batch, &key).unwrap();
    assert_eq!(pos, 2); // Between 7 and 5
}
