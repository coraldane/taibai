use super::*;
use crate::types::batch::ColumnDef;

/// A table clustered by a varchar leading key, plus a secondary key.
fn varchar_key_schema() -> Schema {
    Schema::new(vec![
        ColumnDef::new("category", DataType::VarChar),
        ColumnDef::new("ts", DataType::Timestamp),
        ColumnDef::new("price", DataType::Float64),
        ColumnDef::new("volume", DataType::Int64),
    ])
}

fn varchar_key_meta(schema: &Schema) -> TableMeta {
    let mut meta = TableMeta::new("main", "events", schema.columns.clone());
    meta.cluster_key = vec!["category".to_string(), "ts".to_string()];
    meta.hot_time_column = Some("ts".to_string());
    meta
}

fn build_varchar_batch(category: &str, ts: &[i64], prices: &[f64], vols: &[i64]) -> RecordBatch {
    let mut batch = RecordBatch::new(varchar_key_schema());
    let c0 = batch.column_mut(0).unwrap();
    for _ in 0..ts.len() {
        c0.push(&ScalarValue::VarChar(Some(category.to_string())))
            .unwrap();
    }
    let c1 = batch.column_mut(1).unwrap();
    for v in ts {
        c1.push(&ScalarValue::Timestamp(Some(*v))).unwrap();
    }
    let c2 = batch.column_mut(2).unwrap();
    for v in prices {
        c2.push(&ScalarValue::Float64(Some(*v))).unwrap();
    }
    let c3 = batch.column_mut(3).unwrap();
    for v in vols {
        c3.push(&ScalarValue::Int64(Some(*v))).unwrap();
    }
    batch
}

#[test]
fn append_then_lookup_varchar_key() {
    let schema = varchar_key_schema();
    let meta = varchar_key_meta(&schema);
    let mgr = HotStoreManager::new();
    let batch = build_varchar_batch("alpha", &[1, 2, 3], &[10.0, 10.5, 11.0], &[100, 200, 300]);
    mgr.try_append("main", "events", &meta, &batch).unwrap();

    let store = mgr.get("main", "events").unwrap();
    assert_eq!(store.row_count(), 3);

    let result = store
        .lookup(
            &ScalarValue::VarChar(Some("alpha".into())),
            None,
            None,
            0,
            false,
        )
        .unwrap();
    assert_eq!(result.num_rows(), 3);
    assert_eq!(result.num_columns(), 4);
    match result.column(0).unwrap() {
        ColumnArray::VarChar { data, .. } => {
            assert_eq!(data, &vec!["alpha".to_string(); 3]);
        }
        _ => panic!("expected varchar key column"),
    }
    match result.column(3).unwrap() {
        ColumnArray::Int64 { data, .. } => assert_eq!(data, &vec![100, 200, 300]),
        _ => panic!("expected int64 volume"),
    }
}

#[test]
fn lookup_unknown_value_returns_none() {
    let schema = varchar_key_schema();
    let meta = varchar_key_meta(&schema);
    let mgr = HotStoreManager::new();
    let batch = build_varchar_batch("alpha", &[1], &[10.0], &[100]);
    mgr.try_append("main", "events", &meta, &batch).unwrap();

    let store = mgr.get("main", "events").unwrap();
    assert!(store
        .lookup(
            &ScalarValue::VarChar(Some("zzz".into())),
            None,
            None,
            0,
            false
        )
        .is_none());
    assert_eq!(
        store.count_by_key(&ScalarValue::VarChar(Some("zzz".into()))),
        None
    );
}

#[test]
fn incremental_index_across_appends() {
    let schema = varchar_key_schema();
    let meta = varchar_key_meta(&schema);
    let mgr = HotStoreManager::new();
    mgr.try_append(
        "main",
        "events",
        &meta,
        &build_varchar_batch("A", &[1, 2], &[1.0, 2.0], &[1, 2]),
    )
    .unwrap();
    mgr.try_append(
        "main",
        "events",
        &meta,
        &build_varchar_batch("B", &[3], &[3.0], &[3]),
    )
    .unwrap();
    mgr.try_append(
        "main",
        "events",
        &meta,
        &build_varchar_batch("A", &[4], &[4.0], &[4]),
    )
    .unwrap();

    let store = mgr.get("main", "events").unwrap();
    assert_eq!(store.row_count(), 4);
    assert_eq!(store.distinct_key_count(), 2);
    assert_eq!(
        store.count_by_key(&ScalarValue::VarChar(Some("A".into()))),
        Some(3)
    );
    let a = store
        .lookup(
            &ScalarValue::VarChar(Some("A".into())),
            None,
            Some(&[1, 3]),
            0,
            false,
        )
        .unwrap();
    assert_eq!(a.num_rows(), 3);
    assert_eq!(a.num_columns(), 2);
}

#[test]
fn lookup_uses_key_partition_index_and_reverse_offsets() {
    let schema = Schema::new(vec![
        ColumnDef::new("symbol", DataType::VarChar),
        ColumnDef::new("deal_date", DataType::Int64),
        ColumnDef::new("deal_time", DataType::Int32),
        ColumnDef::new("price", DataType::Int32),
    ]);
    let mut meta = TableMeta::new("main", "ticks", schema.columns.clone());
    meta.cluster_key = vec![
        "symbol".to_string(),
        "deal_date".to_string(),
        "deal_time".to_string(),
    ];
    meta.partition_key = "deal_date".to_string();
    meta.partition_type = crate::storage::catalog::schema::PartitionType::Value;
    meta.hot_time_column = Some("deal_time".to_string());
    meta.hot_index_column = Some("symbol".to_string());
    meta.update_partition_key_idx();

    let mgr = HotStoreManager::new();
    let mut batch = RecordBatch::new(schema);
    for (symbol, deal_date, deal_time, price) in [
        ("A", 20260708, 93000, 1),
        ("A", 20260709, 93001, 10),
        ("A", 20260709, 93002, 20),
        ("B", 20260709, 93003, 99),
    ] {
        batch
            .column_mut(0)
            .unwrap()
            .push(&ScalarValue::VarChar(Some(symbol.to_string())))
            .unwrap();
        batch
            .column_mut(1)
            .unwrap()
            .push(&ScalarValue::Int64(Some(deal_date)))
            .unwrap();
        batch
            .column_mut(2)
            .unwrap()
            .push(&ScalarValue::Int32(Some(deal_time)))
            .unwrap();
        batch
            .column_mut(3)
            .unwrap()
            .push(&ScalarValue::Int32(Some(price)))
            .unwrap();
    }
    mgr.try_append("main", "ticks", &meta, &batch).unwrap();

    let out = mgr
        .get("main", "ticks")
        .unwrap()
        .lookup(
            &ScalarValue::VarChar(Some("A".to_string())),
            Some(&ScalarValue::Int64(Some(20260709))),
            Some(&[3]),
            0,
            true,
        )
        .unwrap();
    assert_eq!(out.num_rows(), 2);
    match out.column(0).unwrap() {
        ColumnArray::Int32 { data, .. } => assert_eq!(data, &vec![20, 10]),
        _ => panic!("expected projected price column"),
    }
}

#[test]
fn integer_key_point_lookup() {
    // Demonstrates generality: an integer cluster-key column is indexed too.
    let schema = Schema::new(vec![
        ColumnDef::new("id", DataType::Int64),
        ColumnDef::new("payload", DataType::VarChar),
    ]);
    let mut meta = TableMeta::new("main", "kv", schema.columns.clone());
    meta.cluster_key = vec!["id".to_string()];
    meta.hot_time_column = Some("id".to_string());

    let mgr = HotStoreManager::new();
    let mut batch = RecordBatch::new(schema.clone());
    let c0 = batch.column_mut(0).unwrap();
    for v in [10i64, 20, 10, 30] {
        c0.push(&ScalarValue::Int64(Some(v))).unwrap();
    }
    let c1 = batch.column_mut(1).unwrap();
    for s in ["a", "b", "c", "d"] {
        c1.push(&ScalarValue::VarChar(Some(s.into()))).unwrap();
    }
    mgr.try_append("main", "kv", &meta, &batch).unwrap();

    let store = mgr.get("main", "kv").unwrap();
    assert_eq!(store.count_by_key(&ScalarValue::Int64(Some(10))), Some(2));
    let r = store
        .lookup(&ScalarValue::Int64(Some(10)), None, None, 0, false)
        .unwrap();
    assert_eq!(r.num_rows(), 2);
    match r.column(0).unwrap() {
        ColumnArray::Int64 { data, .. } => assert_eq!(data, &vec![10, 10]),
        _ => panic!("expected int64 key column"),
    }
}

#[test]
fn lookup_watermark_excludes_flushed_prefix() {
    let schema = varchar_key_schema();
    let meta = varchar_key_meta(&schema);
    let mgr = HotStoreManager::new();
    // Append order -> offsets: A@0, A@1, B@2, A@3  (A at [0,1,3]).
    mgr.try_append(
        "main",
        "events",
        &meta,
        &build_varchar_batch("A", &[1, 2], &[1.0, 2.0], &[1, 2]),
    )
    .unwrap();
    mgr.try_append(
        "main",
        "events",
        &meta,
        &build_varchar_batch("B", &[3], &[3.0], &[3]),
    )
    .unwrap();
    mgr.try_append(
        "main",
        "events",
        &meta,
        &build_varchar_batch("A", &[4], &[4.0], &[4]),
    )
    .unwrap();

    let store = mgr.get("main", "events").unwrap();
    assert_eq!(store.row_count(), 4);

    // No watermark: all three A rows.
    let all = store
        .lookup(
            &ScalarValue::VarChar(Some("A".into())),
            None,
            Some(&[3]),
            0,
            false,
        )
        .unwrap();
    assert_eq!(all.num_rows(), 3);

    // Watermark 3 simulates "offsets [0,3) already persisted to segments";
    // only the live A row at offset 3 is gathered (volume = 4).
    let live = store
        .lookup(
            &ScalarValue::VarChar(Some("A".into())),
            None,
            Some(&[3]),
            3,
            false,
        )
        .unwrap();
    assert_eq!(live.num_rows(), 1);
    match live.column(0).unwrap() {
        ColumnArray::Int64 { data, .. } => assert_eq!(data, &vec![4]),
        _ => panic!("expected volume column"),
    }

    // Watermark past every A offset -> empty result (segment scan covers them).
    let none = store
        .lookup(
            &ScalarValue::VarChar(Some("A".into())),
            None,
            Some(&[3]),
            4,
            false,
        )
        .unwrap();
    assert_eq!(none.num_rows(), 0);
}

#[test]
fn ineligible_table_is_noop() {
    // No cluster key -> not eligible.
    let schema = Schema::new(vec![
        ColumnDef::new("id", DataType::Int64),
        ColumnDef::new("v", DataType::Float64),
    ]);
    let meta = TableMeta::new("main", "t", schema.columns.clone());
    let mgr = HotStoreManager::new();
    assert!(resolve_key_column(&meta).is_none());
    assert!(mgr.get("main", "t").is_none());

    // Blob leading cluster key -> not eligible.
    let schema2 = Schema::new(vec![
        ColumnDef::new("raw", DataType::Blob),
        ColumnDef::new("v", DataType::Float64),
    ]);
    let mut meta2 = TableMeta::new("main", "t2", schema2.columns.clone());
    meta2.cluster_key = vec!["raw".to_string()];
    assert!(resolve_key_column(&meta2).is_none());
}

#[test]
fn resolve_key_column_prefers_hot_index_column() {
    let schema = Schema::new(vec![
        ColumnDef::new("trade_time", DataType::Timestamp),
        ColumnDef::new("security_id", DataType::VarChar),
    ]);
    let mut meta = TableMeta::new("main", "tick", schema.columns.clone());
    meta.cluster_key = vec!["trade_time".to_string()];
    meta.hot_time_column = Some("trade_time".to_string());
    meta.hot_index_column = Some("security_id".to_string());
    let idx = resolve_key_column(&meta).expect("resolvable");
    assert_eq!(
        idx, 1,
        "must prefer hot_index_column over cluster_key leading column"
    );

    meta.hot_index_column = None;
    let idx = resolve_key_column(&meta).expect("resolvable");
    assert_eq!(idx, 0, "must fall back to cluster_key leading column");
}
