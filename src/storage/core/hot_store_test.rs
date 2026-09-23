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

    let result = owned(
        store
            .lookup(
                &ScalarValue::VarChar(Some("alpha".into())),
                None,
                None,
                0,
                false,
            )
            .unwrap(),
    );
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
        store.count_by_key_partition_from(&ScalarValue::VarChar(Some("zzz".into())), None, 0),
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
        store.count_by_key_partition_from(&ScalarValue::VarChar(Some("A".into())), None, 0),
        Some(3)
    );
    let a = owned(
        store
            .lookup(
                &ScalarValue::VarChar(Some("A".into())),
                None,
                Some(&[1, 3]),
                0,
                false,
            )
            .unwrap(),
    );
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

    let out = owned(
        mgr.get("main", "ticks")
            .unwrap()
            .lookup(
                &ScalarValue::VarChar(Some("A".to_string())),
                Some(&ScalarValue::Int64(Some(20260709))),
                Some(&[3]),
                0,
                true,
            )
            .unwrap(),
    );
    assert_eq!(out.num_rows(), 2);
    match out.column(0).unwrap() {
        ColumnArray::Int32 { data, .. } => assert_eq!(data, &vec![20, 10]),
        _ => panic!("expected projected price column"),
    }

    let store = mgr.get("main", "ticks").unwrap();
    let filtered = owned(
        store
            .lookup_filtered(
                &ScalarValue::VarChar(Some("A".to_string())),
                Some(&ScalarValue::Int64(Some(20260709))),
                Some(&[3]),
                0,
                true,
                Some(1),
                &[
                    (0, ScalarValue::VarChar(Some("A".to_string()))),
                    (1, ScalarValue::Int64(Some(20260709))),
                ],
                &[RangeClause {
                    column_index: 2,
                    op: RangeOp::Ge,
                    value: ScalarValue::Int32(Some(93001)),
                }],
            )
            .unwrap(),
    );
    assert_eq!(filtered.num_rows(), 1);
    match filtered.column(0).unwrap() {
        ColumnArray::Int32 { data, .. } => assert_eq!(data, &vec![20]),
        _ => panic!("expected projected price column"),
    }

    // The persisted-prefix watermark is resolved by binary search over sorted
    // append offsets rather than scanning the entire key posting list.
    assert_eq!(
        store.count_by_key_partition_from(&ScalarValue::VarChar(Some("A".to_string())), None, 2),
        Some(1)
    );
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
    assert_eq!(
        store.count_by_key_partition_from(&ScalarValue::Int64(Some(10)), None, 0),
        Some(2)
    );
    let r = owned(
        store
            .lookup(&ScalarValue::Int64(Some(10)), None, None, 0, false)
            .unwrap(),
    );
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
    let all = owned(
        store
            .lookup(
                &ScalarValue::VarChar(Some("A".into())),
                None,
                Some(&[3]),
                0,
                false,
            )
            .unwrap(),
    );
    assert_eq!(all.num_rows(), 3);

    // Watermark 3 simulates "offsets [0,3) already persisted to segments";
    // only the live A row at offset 3 is gathered (volume = 4).
    let live = owned(
        store
            .lookup(
                &ScalarValue::VarChar(Some("A".into())),
                None,
                Some(&[3]),
                3,
                false,
            )
            .unwrap(),
    );
    assert_eq!(live.num_rows(), 1);
    match live.column(0).unwrap() {
        ColumnArray::Int64 { data, .. } => assert_eq!(data, &vec![4]),
        _ => panic!("expected volume column"),
    }

    // Watermark past every A offset -> empty result (segment scan covers them).
    let none = owned(
        store
            .lookup(
                &ScalarValue::VarChar(Some("A".into())),
                None,
                Some(&[3]),
                4,
                false,
            )
            .unwrap(),
    );
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

// ---------------------------------------------------------------------------
// Chunked-store tests: sealed chunk boundaries, prefix reclamation and the
// backpressure valve.
// ---------------------------------------------------------------------------

fn kv_schema() -> Schema {
    Schema::new(vec![
        ColumnDef::new("id", DataType::Int64),
        ColumnDef::new("v", DataType::Int64),
    ])
}

fn kv_store(chunk_rows: usize, memory_limit: u64) -> HotStore {
    HotStore::with_chunk_rows(kv_schema(), 0, None, memory_limit, chunk_rows)
}

fn kv_batch(ids: std::ops::Range<i64>) -> RecordBatch {
    let mut batch = RecordBatch::new(kv_schema());
    for id in ids {
        batch
            .column_mut(0)
            .unwrap()
            .push(&ScalarValue::Int64(Some(id)))
            .unwrap();
        batch
            .column_mut(1)
            .unwrap()
            .push(&ScalarValue::Int64(Some(id * 10)))
            .unwrap();
    }
    batch
}

/// Rows whose key repeats across several sealed chunks: key `i % 3` for
/// `i in 0..rows`.
/// Materialize `Selected` view columns so tests can pattern-match owned
/// variants.
fn owned(batch: RecordBatch) -> RecordBatch {
    let cols = batch
        .columns
        .iter()
        .map(|c| c.flattened().clone())
        .collect();
    RecordBatch::try_new(batch.schema.clone(), cols).unwrap()
}

fn keyed_batch(rows: i64) -> RecordBatch {
    let mut batch = RecordBatch::new(kv_schema());
    for i in 0..rows {
        batch
            .column_mut(0)
            .unwrap()
            .push(&ScalarValue::Int64(Some(i % 3)))
            .unwrap();
        batch
            .column_mut(1)
            .unwrap()
            .push(&ScalarValue::Int64(Some(i * 10)))
            .unwrap();
    }
    batch
}

#[test]
fn chunk_seal_keeps_global_order_across_boundaries() {
    // chunk_rows = 8: appending 20 rows yields sealed chunks [0,8) and [8,16)
    // plus an open chunk [16,20).
    let store = kv_store(8, u64::MAX);
    store.append(&keyed_batch(20)).unwrap();
    assert_eq!(store.row_count(), 20);

    // Forward lookup gathers ascending append order across chunk boundaries.
    let fwd = owned(
        store
            .lookup(&ScalarValue::Int64(Some(0)), None, Some(&[1]), 0, false)
            .unwrap(),
    );
    match fwd.column(0).unwrap() {
        ColumnArray::Int64 { data, .. } => {
            assert_eq!(data, &vec![0, 30, 60, 90, 120, 150, 180]);
        }
        _ => panic!("expected int64 payload column"),
    }

    // Reverse lookup starts at the newest row, even when it lives in the open
    // chunk, and the LIMIT short-circuits without walking older chunks.
    let rev = owned(
        store
            .lookup_filtered(
                &ScalarValue::Int64(Some(0)),
                None,
                Some(&[1]),
                0,
                true,
                Some(2),
                &[],
                &[],
            )
            .unwrap(),
    );
    match rev.column(0).unwrap() {
        ColumnArray::Int64 { data, .. } => assert_eq!(data, &vec![180, 150]),
        _ => panic!("expected int64 payload column"),
    }

    // Watermark above both sealed chunks leaves only the open-chunk row.
    let live = owned(
        store
            .lookup(&ScalarValue::Int64(Some(0)), None, Some(&[1]), 16, false)
            .unwrap(),
    );
    match live.column(0).unwrap() {
        ColumnArray::Int64 { data, .. } => assert_eq!(data, &vec![180]),
        _ => panic!("expected int64 payload column"),
    }
    assert_eq!(
        store.count_by_key_partition_from(&ScalarValue::Int64(Some(0)), None, 16),
        Some(1)
    );
    assert_eq!(
        store.count_by_key_partition_from(&ScalarValue::Int64(Some(0)), None, 0),
        Some(7)
    );
}

#[test]
fn compact_prefix_drops_whole_chunks_and_trims_boundary() {
    let store = kv_store(8, u64::MAX);
    // 12 + 8 rows with unique ids 0..20 -> sealed [0,8), [8,16), open [16,20).
    store.append(&kv_batch(0..12)).unwrap();
    store.append(&kv_batch(12..20)).unwrap();
    assert_eq!(store.row_count(), 20);

    // Cut at 10: chunk [0,8) released whole, chunk [8,16) trimmed to [10,16).
    let dropped = store.compact_prefix(10);
    assert_eq!(dropped, 10);
    assert_eq!(store.row_count(), 10);

    assert!(store
        .lookup(&ScalarValue::Int64(Some(9)), None, None, 0, false)
        .is_none());
    let kept = owned(
        store
            .lookup(&ScalarValue::Int64(Some(10)), None, Some(&[1]), 0, false)
            .unwrap(),
    );
    match kept.column(0).unwrap() {
        ColumnArray::Int64 { data, .. } => assert_eq!(data, &vec![100]),
        _ => panic!("expected int64 payload column"),
    }
    let from_open = owned(
        store
            .lookup(&ScalarValue::Int64(Some(19)), None, Some(&[1]), 0, false)
            .unwrap(),
    );
    match from_open.column(0).unwrap() {
        ColumnArray::Int64 { data, .. } => assert_eq!(data, &vec![190]),
        _ => panic!("expected int64 payload column"),
    }

    // Watermarks are relative to the reclaimed front: the surviving `id = 11`
    // row now sits at relative offset 0.
    assert_eq!(
        store.count_filtered(
            &ScalarValue::Int64(Some(11)),
            None,
            0,
            &[],
            &[RangeClause {
                column_index: 1,
                op: RangeOp::Ge,
                value: ScalarValue::Int64(Some(0)),
            }],
        ),
        Some(1)
    );
    // Watermarks are relative to the reclaimed front: the surviving `id = 11`
    // row now sits at relative offset 1.
    assert_eq!(
        store.count_by_key_partition_from(&ScalarValue::Int64(Some(11)), None, 0),
        Some(1)
    );
    assert_eq!(
        store.count_by_key_partition_from(&ScalarValue::Int64(Some(11)), None, 1),
        Some(1)
    );
    assert_eq!(
        store.count_by_key_partition_from(&ScalarValue::Int64(Some(11)), None, 2),
        Some(0)
    );

    // Second reclaim of 8 of the 10 surviving rows cuts into the open chunk.
    let dropped = store.compact_prefix(8);
    assert_eq!(dropped, 8);
    assert_eq!(store.row_count(), 2);
    assert!(store
        .lookup(&ScalarValue::Int64(Some(17)), None, None, 0, false)
        .is_none());
    let tail = owned(
        store
            .lookup(&ScalarValue::Int64(Some(18)), None, Some(&[1]), 0, false)
            .unwrap(),
    );
    match tail.column(0).unwrap() {
        ColumnArray::Int64 { data, .. } => assert_eq!(data, &vec![180]),
        _ => panic!("expected int64 payload column"),
    }
}

#[test]
fn hard_memory_limit_rejects_before_mutation() {
    // A 2-column Int64 batch of 4 rows estimates well above a 10-byte limit.
    let store = kv_store(8, 10);
    let err = store
        .append(&kv_batch(0..4))
        .expect_err("append must be rejected under backpressure");
    assert!(matches!(
        err,
        crate::common::error::TaibaiError::ResourceBusy(_)
    ));
    assert_eq!(store.row_count(), 0);
    assert_eq!(
        store.count_by_key_partition_from(&ScalarValue::Int64(Some(1)), None, 0),
        None
    );
}

#[test]
fn small_sealed_results_are_zero_copy_views() {
    // Rows spread across sealed chunks: a small lookup must come back as
    // `Selected` view columns (no gathered copy), still reading correctly.
    let store = kv_store(8, u64::MAX);
    store.append(&keyed_batch(40)).unwrap();
    // Watermark pushes everything into sealed chunks.
    let result = store
        .lookup_filtered(
            &ScalarValue::Int64(Some(1)),
            None,
            Some(&[1]),
            0,
            true,
            Some(3),
            &[],
            &[],
        )
        .unwrap();
    assert_eq!(result.num_rows(), 3);
    assert!(result.column(0).unwrap().is_selected());
    let owned_batch = owned(result);
    match owned_batch.column(0).unwrap() {
        ColumnArray::Int64 { data, .. } => {
            // key 1 rows: i = 1,4,...,37 -> newest three are 37, 34, 31 (x10)
            assert_eq!(data, &vec![370, 340, 310]);
        }
        _ => panic!("expected int64 payload column"),
    }
}

#[test]
fn parallel_gather_matches_serial_semantics() {
    // Default 64Ki-row chunks: 70K rows give a sealed chunk plus an open one
    // and push one key well over the parallel-gather candidate threshold.
    let store = kv_store(65_536, u64::MAX);
    let rows = 70_000i64;
    let mut batch = RecordBatch::new(kv_schema());
    for i in 0..rows {
        batch
            .column_mut(0)
            .unwrap()
            .push(&ScalarValue::Int64(Some(i % 7)))
            .unwrap();
        batch
            .column_mut(1)
            .unwrap()
            .push(&ScalarValue::Int64(Some(i)))
            .unwrap();
    }
    store.append(&batch).unwrap();

    let expected: Vec<i64> = (0..rows).filter(|i| i % 7 == 3).collect();

    // Full-key gather across both chunks (parallel path), ascending order.
    let fwd = owned(
        store
            .lookup_filtered(
                &ScalarValue::Int64(Some(3)),
                None,
                Some(&[1]),
                0,
                false,
                None,
                &[],
                &[],
            )
            .unwrap(),
    );
    assert_eq!(fwd.num_rows() as i64, rows / 7);
    match fwd.column(0).unwrap() {
        ColumnArray::Int64 { data, .. } => assert_eq!(&data[..], &expected[..]),
        _ => panic!("expected int64 payload column"),
    }

    // Reverse full gather (parallel path), descending append order.
    let rev = owned(
        store
            .lookup_filtered(
                &ScalarValue::Int64(Some(3)),
                None,
                Some(&[1]),
                0,
                true,
                None,
                &[],
                &[],
            )
            .unwrap(),
    );
    match rev.column(0).unwrap() {
        ColumnArray::Int64 { data, .. } => {
            let mut reversed = expected.clone();
            reversed.reverse();
            assert_eq!(&data[..], &reversed[..]);
        }
        _ => panic!("expected int64 payload column"),
    }

    // Watermark cuts both chunks; only rows at or after offset 60_000 remain.
    let expected_from: Vec<i64> = (60_000i64..rows).filter(|i| i % 7 == 3).collect();
    let live = owned(
        store
            .lookup_filtered(
                &ScalarValue::Int64(Some(3)),
                None,
                Some(&[1]),
                60_000,
                false,
                None,
                &[],
                &[],
            )
            .unwrap(),
    );
    match live.column(0).unwrap() {
        ColumnArray::Int64 { data, .. } => assert_eq!(&data[..], &expected_from[..]),
        _ => panic!("expected int64 payload column"),
    }
    assert_eq!(
        store.count_by_key_partition_from(&ScalarValue::Int64(Some(3)), None, 60_000),
        Some(expected_from.len() as u64)
    );

    // Parallel path with a pushed-down predicate, and an unseen key.
    let filtered = owned(
        store
            .lookup_filtered(
                &ScalarValue::Int64(Some(3)),
                None,
                Some(&[1]),
                0,
                false,
                None,
                &[],
                &[RangeClause {
                    column_index: 1,
                    op: RangeOp::Lt,
                    value: ScalarValue::Int64(Some(35_000)),
                }],
            )
            .unwrap(),
    );
    let expected_filtered: Vec<i64> = expected.iter().copied().filter(|v| *v < 35_000).collect();
    match filtered.column(0).unwrap() {
        ColumnArray::Int64 { data, .. } => assert_eq!(&data[..], &expected_filtered[..]),
        _ => panic!("expected int64 payload column"),
    }
    assert!(store
        .lookup_filtered(
            &ScalarValue::Int64(Some(9)),
            None,
            None,
            0,
            false,
            None,
            &[],
            &[]
        )
        .is_none());
}

/// Regression: partitioned hot table (PARTITION BY MONTH(deal_date)) with a
/// VARCHAR cluster key and an Int32 partition column. Bulk-style multi-batch
/// appends scatter each security's rows across chunk boundaries; an equality
/// query pinning the partition column must return every row of that day.
#[test]
fn lookup_partition_eq_month_partitioned_table() {
    let schema = Schema::new(vec![
        ColumnDef::new("security_id", DataType::VarChar),
        ColumnDef::new("deal_date", DataType::Int32),
        ColumnDef::new("deal_time", DataType::Int32),
        ColumnDef::new("val", DataType::Int64),
    ]);
    let mut meta = TableMeta::new("main", "minute_part", schema.columns.clone());
    meta.cluster_key = vec!["security_id".to_string()];
    meta.hot_time_column = Some("gmt_create".to_string());
    meta.partition_key = "deal_date".to_string();
    meta.partition_type = crate::storage::catalog::schema::PartitionType::Month;
    meta.update_partition_key_idx();

    let mgr = HotStoreManager::new();

    // Two securities x 240 minute bars, appended in 20k-row batches with the
    // rows shuffled per batch, mimicking an unordered CSV bulk load.
    let total_rows = 200_000;
    let mut rows: Vec<(u32, i32, i32)> = Vec::with_capacity(total_rows);
    for r in 0..total_rows {
        let sec = (r % 2) as u32; // 0,1 alternating = worst-case scatter
        let deal_time = 930 + (r % 240) as i32;
        rows.push((sec, 20260916, deal_time));
    }

    let batch_size = 20_000;
    let mut start = 0usize;
    while start < total_rows {
        let end = (start + batch_size).min(total_rows);
        let mut batch = RecordBatch::new(schema.clone());
        let c0 = batch.column_mut(0).unwrap();
        for r in &rows[start..end] {
            c0.push(&ScalarValue::VarChar(Some(format!("sec{:05}", r.0))))
                .unwrap();
        }
        let c1 = batch.column_mut(1).unwrap();
        for r in &rows[start..end] {
            c1.push(&ScalarValue::Int32(Some(r.1))).unwrap();
        }
        let c2 = batch.column_mut(2).unwrap();
        for r in &rows[start..end] {
            c2.push(&ScalarValue::Int32(Some(r.2))).unwrap();
        }
        let c3 = batch.column_mut(3).unwrap();
        for r in &rows[start..end] {
            c3.push(&ScalarValue::Int64(Some(r.0 as i64 * 1_000_000)))
                .unwrap();
        }
        mgr.try_append("main", "minute_part", &meta, &batch)
            .unwrap();
        start = end;
    }

    let store = mgr.get("main", "minute_part").unwrap();
    assert_eq!(store.row_count(), total_rows as u64);

    let key = ScalarValue::VarChar(Some("sec00000".into()));
    let partition = ScalarValue::Int64(Some(20260916));

    // Plain key lookup: every row of the security.
    let all = owned(store.lookup(&key, None, None, 0, false).unwrap());
    assert_eq!(all.num_rows(), total_rows / 2);

    // Composite key+partition lookup: still every row of the day.
    let day = owned(
        store
            .lookup_filtered(&key, Some(&partition), None, 0, false, None, &[], &[])
            .unwrap(),
    );
    assert_eq!(
        day.num_rows(),
        total_rows / 2,
        "composite key+partition lookup lost rows"
    );

    let count = store
        .count_by_key_partition_from(&key, Some(&partition), 0)
        .unwrap();
    assert_eq!(count, (total_rows as u64) / 2, "composite count lost rows");
}
