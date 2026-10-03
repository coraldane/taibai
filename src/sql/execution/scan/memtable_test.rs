use super::*;
use crate::protocol::types::DataType;
use crate::storage::catalog::schema::TtlInfo;
use crate::types::batch::{ColumnDef, Schema};
use std::sync::Arc;

fn col(name: &str) -> ColumnDef {
    ColumnDef::new(name, DataType::Int64)
}

/// P0: a transactional reader's tombstone filter used to run BEFORE the
/// catalog alignment, so predicates evaluated against the memtable's
/// insert-time layout while using catalog-space column indices. Layout:
/// memtable keeps (a, id, v); the catalog dropped `a` and is (id, v).
/// The `id >= 2` clause (catalog index 0) used to hit the `a` column
/// and matched nothing.
#[test]
fn txn_memtable_scan_aligns_before_tombstone_predicate_eval() {
    let old_schema = Schema::new(vec![col("a"), col("id"), col("v")]);
    let catalog_schema = Schema::new(vec![col("id"), col("v")]);

    let manager = MemTableManager::new(10_000, 60, 2, 3600);
    let registry = Arc::new(crate::storage::core::txn_mutations::TxnMutationRegistry::new());
    manager.set_txn_mutations(Arc::clone(&registry));

    let memtable = manager.get_or_create("main", "t", "", &old_schema);
    let mut batch = RecordBatch::new(old_schema.clone());
    for (a, id, v) in [(0, 1, 10), (0, 2, 20), (0, 3, 30)] {
        batch
            .column_mut(0)
            .unwrap()
            .push(&ScalarValue::Int64(Some(a)))
            .unwrap();
        batch
            .column_mut(1)
            .unwrap()
            .push(&ScalarValue::Int64(Some(id)))
            .unwrap();
        batch
            .column_mut(2)
            .unwrap()
            .push(&ScalarValue::Int64(Some(v)))
            .unwrap();
    }
    memtable.append(&batch).unwrap();
    // Deferred delete inside txn 9 (position 0 = the id = 1 row): forces
    // the tombstone-filtered branch under test.
    registry
        .register_memtable_tombstones(9, "main", "t", None, Arc::clone(&memtable), [0u32])
        .unwrap();

    let mut spec = ScanSpec::new("t");
    spec.table_schema = catalog_schema.clone();
    spec.reader_txn_id = 9;
    spec.range_predicates
        .push(crate::storage::partition::pruning::RangeClause {
            column_index: 0, // catalog `id`
            op: crate::storage::partition::pruning::RangeOp::Ge,
            value: ScalarValue::Int64(Some(2)),
        });

    let hot_mgr = crate::storage::core::hot_store::HotStoreManager::new();
    let batches = scan_memtables(&manager, &hot_mgr, "main", "t", &spec).unwrap();
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 2, "ids 2 and 3 match id >= 2");
    assert_eq!(batch.schema.columns.len(), 2);
    assert_eq!(batch.schema.columns[0].name, "id");
    match batch.column(0).unwrap() {
        crate::types::array::ColumnArray::Int64 { data, .. } => {
            assert_eq!(data, &vec![2i64, 3]);
        }
        other => panic!("expected int64 id column, got {other:?}"),
    }
}

/// P0: the resident hot-store shortcut truncated its gather at the
/// pushed LIMIT before TTL filtering runs. With expired rows present the
/// query returned fewer live rows than exist, so the shortcut must be
/// disabled for TTL tables — the same gate count_resident_hot_store has
/// always had.
#[test]
fn resident_hot_scan_declines_ttl_tables() {
    use crate::storage::catalog::schema::TableMeta;

    let schema = Schema::new(vec![
        ColumnDef::new("category", DataType::VarChar),
        ColumnDef::new("ts", DataType::Timestamp),
        ColumnDef::new("price", DataType::Float64),
        ColumnDef::new("volume", DataType::Int64),
    ]);
    let mut meta = TableMeta::new("main", "events", schema.columns.clone());
    meta.cluster_key = vec!["category".to_string(), "ts".to_string()];
    meta.hot_time_column = Some("ts".to_string());

    let hot_mgr = crate::storage::core::hot_store::HotStoreManager::new();
    let memtable_mgr = MemTableManager::new(10_000, 60, 2, 3600);
    let mut batch = RecordBatch::new(schema.clone());
    for i in 0..3i64 {
        batch
            .column_mut(0)
            .unwrap()
            .push(&ScalarValue::VarChar(Some("alpha".into())))
            .unwrap();
        batch
            .column_mut(1)
            .unwrap()
            .push(&ScalarValue::Timestamp(Some(1_000 + i)))
            .unwrap();
        batch
            .column_mut(2)
            .unwrap()
            .push(&ScalarValue::Float64(Some(10.0)))
            .unwrap();
        batch
            .column_mut(3)
            .unwrap()
            .push(&ScalarValue::Int64(Some(i)))
            .unwrap();
    }
    hot_mgr.try_append("main", "events", &meta, &batch).unwrap();
    // The resident shortcut only answers while the store claims full
    // residency (close-of-day / restart warmup marks this in production).
    hot_mgr.get("main", "events").unwrap().mark_full_resident();

    let mut spec = ScanSpec::new("events");
    spec.table_schema = schema.clone();
    spec.eq_predicates
        .push((0, ScalarValue::VarChar(Some("alpha".into()))));
    spec.limit = Some(2);

    // Control: without a TTL the shortcut serves the lookup.
    assert!(
        scan_resident_hot_store(&hot_mgr, &memtable_mgr, "main", &meta, &spec).is_some(),
        "control: no TTL, resident shortcut must serve the lookup"
    );

    // With a TTL the shortcut must decline regardless of the spec flag.
    meta.ttl = Some(TtlInfo {
        column: "ts".to_string(),
        duration_secs: 60,
        column_idx: 1,
    });
    spec.ttl_active = true;
    assert!(scan_resident_hot_store(&hot_mgr, &memtable_mgr, "main", &meta, &spec).is_none());
    // Hand-built specs that forgot to raise the flag are still stopped
    // by the authoritative table-meta check.
    spec.ttl_active = false;
    assert!(scan_resident_hot_store(&hot_mgr, &memtable_mgr, "main", &meta, &spec).is_none());
}

/// P0: the resident hot-store lookup consumed `spec.limit` as a row cap
/// even when a DISTINCT / aggregate / window wrapper above the scan needs
/// every row — the dedup or aggregate then ran over truncated input and
/// returned wrong results. The shared pushdown gate must refuse a cap for
/// those shapes.
#[test]
fn resident_hot_scan_does_not_truncate_distinct_or_aggregate_shapes() {
    use crate::storage::catalog::schema::TableMeta;

    let schema = Schema::new(vec![
        ColumnDef::new("category", DataType::VarChar),
        ColumnDef::new("ts", DataType::Timestamp),
        ColumnDef::new("volume", DataType::Int64),
    ]);
    let mut meta = TableMeta::new("main", "events", schema.columns.clone());
    meta.cluster_key = vec!["category".to_string(), "ts".to_string()];
    meta.hot_time_column = Some("ts".to_string());

    let hot_mgr = crate::storage::core::hot_store::HotStoreManager::new();
    let memtable_mgr = MemTableManager::new(10_000, 60, 2, 3600);
    let mut batch = RecordBatch::new(schema.clone());
    for i in 0..6i64 {
        batch
            .column_mut(0)
            .unwrap()
            .push(&ScalarValue::VarChar(Some("alpha".into())))
            .unwrap();
        batch
            .column_mut(1)
            .unwrap()
            .push(&ScalarValue::Timestamp(Some(1_000 + i)))
            .unwrap();
        batch
            .column_mut(2)
            .unwrap()
            .push(&ScalarValue::Int64(Some(i / 2)))
            .unwrap();
    }
    hot_mgr.try_append("main", "events", &meta, &batch).unwrap();
    hot_mgr.get("main", "events").unwrap().mark_full_resident();

    let base_spec = |has_distinct: bool| {
        let mut spec = ScanSpec::new("events");
        spec.table_schema = schema.clone();
        spec.eq_predicates
            .push((0, ScalarValue::VarChar(Some("alpha".into()))));
        spec.limit = Some(2);
        spec.has_distinct_or_window = has_distinct;
        spec
    };

    // Control: a plain shape keeps the pushed cap.
    let plain = base_spec(false);
    let batch = scan_resident_hot_store(&hot_mgr, &memtable_mgr, "main", &meta, &plain)
        .expect("plain shape keeps the resident shortcut");
    assert_eq!(batch.num_rows(), 2, "plain LIMIT shape is capped at 2");

    // DISTINCT wrapper: the scan must return every matching row so the
    // dedup above sees all of them (3 distinct volumes among 6 rows).
    let distinct = base_spec(true);
    let batch = scan_resident_hot_store(&hot_mgr, &memtable_mgr, "main", &meta, &distinct)
        .expect("distinct shape still uses the resident shortcut");
    assert_eq!(
        batch.num_rows(),
        6,
        "DISTINCT must not be truncated by the pushed LIMIT"
    );

    // Aggregate wrapper: same contract.
    let mut aggregate = base_spec(false);
    aggregate.has_distinct_or_window = false;
    aggregate.has_aggregate = true;
    let batch = scan_resident_hot_store(&hot_mgr, &memtable_mgr, "main", &meta, &aggregate)
        .expect("aggregate shape still uses the resident shortcut");
    assert_eq!(
        batch.num_rows(),
        6,
        "aggregates must not be truncated by the pushed LIMIT"
    );
}
