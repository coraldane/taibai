use super::*;
use crate::protocol::types::DataType;
use crate::storage::segment::{SegmentReader, SegmentWriter};
use crate::types::batch::{ColumnDef, RecordBatch, Schema};
use tempfile::TempDir;

#[test]
fn cluster_key_range_read_returns_only_matching_sorted_range() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("seg_00001.dat");
    let schema = Schema::new(vec![
        ColumnDef::new("name", DataType::VarChar),
        ColumnDef::new("value", DataType::Int64),
    ]);

    let mut batch = RecordBatch::new(schema.clone());
    for (name, value) in [
        ("bob", 1),
        ("alice", 2),
        ("alice", 3),
        ("alice", 4),
        ("charlie", 5),
    ] {
        batch
            .column_mut(0)
            .unwrap()
            .push(&ScalarValue::VarChar(Some(name.to_string())))
            .unwrap();
        batch
            .column_mut(1)
            .unwrap()
            .push(&ScalarValue::Int64(Some(value)))
            .unwrap();
    }

    {
        let mut writer = SegmentWriter::create(&path, schema.clone(), 8192, 3).unwrap();
        writer.write_batch(&batch).unwrap();
        writer.seal().unwrap();
    }

    let reader = SegmentReader::open(&path).unwrap();
    let result = cluster_key_range_read(
        &reader,
        &schema,
        0,
        &ClusterKeyBounds::single(
            0,
            Some((ScalarValue::VarChar(Some("alice".to_string())), true)),
            Some((ScalarValue::VarChar(Some("alice".to_string())), true)),
        ),
        &[0, 1],
        &[0],
        &[(0, ScalarValue::VarChar(Some("alice".to_string())))],
        &[],
        &[],
        None,
    )
    .unwrap();

    assert_eq!(result.num_rows(), 3);
    assert_eq!(
        result.scalar(0, 0).unwrap(),
        ScalarValue::VarChar(Some("alice".to_string()))
    );
    assert_eq!(result.scalar(0, 1).unwrap(), ScalarValue::Int64(Some(2)));
    assert_eq!(result.scalar(2, 1).unwrap(), ScalarValue::Int64(Some(4)));

    let filtered = cluster_key_range_read(
        &reader,
        &schema,
        0,
        &ClusterKeyBounds::single(
            0,
            Some((ScalarValue::VarChar(Some("alice".to_string())), true)),
            Some((ScalarValue::VarChar(Some("alice".to_string())), true)),
        ),
        &[0, 1],
        &[0, 1],
        &[(0, ScalarValue::VarChar(Some("alice".to_string())))],
        &[crate::storage::partition::pruning::RangeClause {
            column_index: 1,
            op: RangeOp::Gt,
            value: ScalarValue::Int64(Some(2)),
        }],
        &[],
        None,
    )
    .unwrap();

    assert_eq!(filtered.num_rows(), 2);
    assert_eq!(filtered.scalar(0, 1).unwrap(), ScalarValue::Int64(Some(3)));
    assert_eq!(filtered.scalar(1, 1).unwrap(), ScalarValue::Int64(Some(4)));
}

#[test]
fn cluster_key_range_read_falls_back_when_key_column_is_unsorted() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("seg_00001.dat");
    let schema = Schema::new(vec![
        ColumnDef::new("security_id", DataType::VarChar),
        ColumnDef::new("deal_date", DataType::Int64),
        ColumnDef::new("price", DataType::Float64),
    ]);

    let mut batch = RecordBatch::new(schema.clone());
    for (security_id, deal_date, price) in
        [("688256.SH", 20260709, 11.1), ("000001.SZ", 20260709, 12.2)]
    {
        batch
            .column_mut(0)
            .unwrap()
            .push(&ScalarValue::VarChar(Some(security_id.to_string())))
            .unwrap();
        batch
            .column_mut(1)
            .unwrap()
            .push(&ScalarValue::Int64(Some(deal_date)))
            .unwrap();
        batch
            .column_mut(2)
            .unwrap()
            .push(&ScalarValue::Float64(Some(price)))
            .unwrap();
    }

    {
        let mut writer = SegmentWriter::create(&path, schema.clone(), 8192, 3).unwrap();
        writer.write_batch(&batch).unwrap();
        writer.seal().unwrap();
    }

    let reader = SegmentReader::open(&path).unwrap();
    let result = cluster_key_range_read(
        &reader,
        &schema,
        0,
        &ClusterKeyBounds::single(
            0,
            Some((ScalarValue::VarChar(Some("688256.SH".to_string())), true)),
            Some((ScalarValue::VarChar(Some("688256.SH".to_string())), true)),
        ),
        &[0],
        &[0, 1],
        &[
            (0, ScalarValue::VarChar(Some("688256.SH".to_string()))),
            (1, ScalarValue::Int64(Some(20260709))),
        ],
        &[],
        &[],
        None,
    )
    .unwrap();

    assert_eq!(result.num_rows(), 1);
    assert_eq!(
        result.scalar(0, 0).unwrap(),
        ScalarValue::VarChar(Some("688256.SH".to_string()))
    );
}

#[test]
fn cluster_key_range_read_binary_searches_composite_key() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("seg_00001.dat");
    let schema = Schema::new(vec![
        ColumnDef::new("symbol", DataType::VarChar),
        ColumnDef::new("ts", DataType::Int64),
        ColumnDef::new("price", DataType::Float64),
    ]);

    // Sorted by (symbol, ts): alice ts 1..2, bob ts 3..5.
    let mut batch = RecordBatch::new(schema.clone());
    for (symbol, ts) in [
        ("alice", 1),
        ("alice", 2),
        ("bob", 3),
        ("bob", 4),
        ("bob", 5),
    ] {
        batch
            .column_mut(0)
            .unwrap()
            .push(&ScalarValue::VarChar(Some(symbol.to_string())))
            .unwrap();
        batch
            .column_mut(1)
            .unwrap()
            .push(&ScalarValue::Int64(Some(ts)))
            .unwrap();
    }
    for i in 0..5 {
        batch
            .column_mut(2)
            .unwrap()
            .push(&ScalarValue::Float64(Some(i as f64)))
            .unwrap();
    }

    {
        let mut writer = SegmentWriter::create(&path, schema.clone(), 8192, 3).unwrap();
        writer.write_batch(&batch).unwrap();
        writer.seal().unwrap();
    }

    let reader = SegmentReader::open(&path).unwrap();
    let bounds = ClusterKeyBounds {
        prefix_eq: vec![(0, ScalarValue::VarChar(Some("bob".to_string())))],
        range_column_index: 1,
        lower: Some((ScalarValue::Int64(Some(4)), true)),
        upper: Some((ScalarValue::Int64(Some(4)), true)),
    };
    let result = cluster_key_range_read(
        &reader,
        &schema,
        0,
        &bounds,
        &[0, 1, 2],
        &[0, 1],
        &[],
        &[],
        &[],
        None,
    )
    .unwrap();

    assert_eq!(result.num_rows(), 1, "rows: {:?}", {
        (0..result.num_rows())
            .map(|r| (result.scalar(r, 0).unwrap(), result.scalar(r, 1).unwrap()))
            .collect::<Vec<_>>()
    });
    assert_eq!(
        result.scalar(0, 0).unwrap(),
        ScalarValue::VarChar(Some("bob".to_string()))
    );
    assert_eq!(result.scalar(0, 1).unwrap(), ScalarValue::Int64(Some(4)));

    // Exclusive bounds: bob AND ts > 3 AND ts < 5 is the same single row.
    let bounds = ClusterKeyBounds {
        prefix_eq: vec![(0, ScalarValue::VarChar(Some("bob".to_string())))],
        range_column_index: 1,
        lower: Some((ScalarValue::Int64(Some(3)), false)),
        upper: Some((ScalarValue::Int64(Some(5)), false)),
    };
    let result = cluster_key_range_read(
        &reader,
        &schema,
        0,
        &bounds,
        &[0, 1, 2],
        &[0, 1],
        &[],
        &[],
        &[],
        None,
    )
    .unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_eq!(result.scalar(0, 1).unwrap(), ScalarValue::Int64(Some(4)));

    // A different prefix pins the alice run; the ts range selects its rows.
    let bounds = ClusterKeyBounds {
        prefix_eq: vec![(0, ScalarValue::VarChar(Some("alice".to_string())))],
        range_column_index: 1,
        lower: Some((ScalarValue::Int64(Some(2)), true)),
        upper: Some((ScalarValue::Int64(Some(9)), true)),
    };
    let result = cluster_key_range_read(
        &reader,
        &schema,
        0,
        &bounds,
        &[0, 1, 2],
        &[0, 1],
        &[],
        &[],
        &[],
        None,
    )
    .unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_eq!(
        result.scalar(0, 0).unwrap(),
        ScalarValue::VarChar(Some("alice".to_string()))
    );
    assert_eq!(result.scalar(0, 1).unwrap(), ScalarValue::Int64(Some(2)));
}
