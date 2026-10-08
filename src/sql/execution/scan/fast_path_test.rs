use super::*;
use crate::protocol::types::DataType;
use crate::storage::catalog::schema::TtlInfo;
use crate::types::batch::ColumnDef;
use crate::types::scalar::ScalarValue;

fn test_meta(ttl: Option<TtlInfo>) -> TableMeta {
    let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
    let mut meta = TableMeta::new("main", "events", schema.columns.clone());
    meta.ttl = ttl;
    meta
}

fn test_ctx<'a>(
    spec: &'a ScanSpec,
    meta: &'a TableMeta,
    schema: &'a Schema,
    segment_mgr: &'a SegmentManager,
    memtable_mgr: &'a MemTableManager,
    hot_mgr: &'a HotStoreManager,
) -> FastPathContext<'a> {
    FastPathContext {
        table_dir: PathBuf::from("nonexistent-test-table-dir"),
        spec,
        table_meta: meta,
        table_schema: schema,
        db: "main",
        segment_mgr,
        memtable_mgr,
        hot_mgr,
    }
}

/// The universal gate: either the spec flag (set at spec-compile time) or
/// the table metadata blocks — a hand-built spec that forgot the flag is
/// still stopped by the meta check.
#[test]
fn ttl_blocks_on_either_signal() {
    let ttl = TtlInfo {
        column: "ts".to_string(),
        duration_secs: 3600,
        column_idx: 1,
    };
    let meta_with_ttl = test_meta(Some(ttl));
    let meta_without = test_meta(None);

    let mut spec = ScanSpec::new("events");
    spec.ttl_active = true;
    assert!(ttl_blocks(&spec, &meta_without));

    let clean_spec = ScanSpec::new("events");
    assert!(ttl_blocks(&clean_spec, &meta_with_ttl));

    assert!(ttl_blocks(&spec, &meta_with_ttl));
    assert!(!ttl_blocks(&clean_spec, &meta_without));
}

/// Without a `distinct_column` in the spec the node does not apply at
/// all — it declines before touching any manager.
#[test]
fn distinct_node_declines_not_applicable_without_target_column() {
    let meta = test_meta(None);
    let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
    let spec = ScanSpec::new("events");

    let dir = std::env::temp_dir().join(format!("taibai_fp_test_{}", std::process::id()));
    let segment_mgr = SegmentManager::new(&dir).unwrap();
    let memtable_mgr = MemTableManager::new(10_000, 60, 2, 3600);
    let hot_mgr = HotStoreManager::new();
    let ctx = test_ctx(&spec, &meta, &schema, &segment_mgr, &memtable_mgr, &hot_mgr);

    let outcome = DistinctMetadataFastPath.try_answer(&ctx).unwrap();
    match outcome {
        FastPathOutcome::Declined(decline) => {
            assert_eq!(decline.node, "distinct_metadata");
            assert_eq!(decline.reason, DeclineReason::NotApplicable);
        }
        other => panic!("expected decline, got {other:?}"),
    }
}

/// With no hot store for the table the resident node declines through the
/// free function's own gate (no store registered in the manager).
#[test]
fn resident_node_declines_without_hot_store() {
    let meta = test_meta(None);
    let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
    let mut spec = ScanSpec::new("events");
    spec.eq_predicates.push((0, ScalarValue::Int64(Some(1))));

    let dir = std::env::temp_dir().join(format!("taibai_fp_test_res_{}", std::process::id()));
    let segment_mgr = SegmentManager::new(&dir).unwrap();
    let memtable_mgr = MemTableManager::new(10_000, 60, 2, 3600);
    let hot_mgr = HotStoreManager::new();
    let ctx = test_ctx(&spec, &meta, &schema, &segment_mgr, &memtable_mgr, &hot_mgr);

    let node = ResidentHotFastPath {
        projection_cols: &[0],
        strip_ttl_column: false,
    };
    let outcome = node.try_answer(&ctx).unwrap();
    match outcome {
        FastPathOutcome::Declined(decline) => {
            assert_eq!(decline.node, "resident_hot");
            assert_eq!(decline.reason, DeclineReason::NodeInternal);
        }
        other => panic!("expected decline, got {other:?}"),
    }
}

/// The folded node delegates the TTL decline: with table TTL set the fold
/// engine returns `Ok(None)` and the node reports a decline, never an
/// error and never a fabricated answer.
#[test]
fn folded_node_declines_ttl_via_delegation() {
    let ttl = TtlInfo {
        column: "ts".to_string(),
        duration_secs: 3600,
        column_idx: 0,
    };
    let meta = test_meta(Some(ttl));
    let schema = Schema::new(vec![ColumnDef::new("ts", DataType::Timestamp)]);
    let mut spec = ScanSpec::new("events");
    spec.table_schema = schema.clone();

    let dir = std::env::temp_dir().join(format!("taibai_fp_test_fold_{}", std::process::id()));
    let segment_mgr = SegmentManager::new(&dir).unwrap();
    let memtable_mgr = MemTableManager::new(10_000, 60, 2, 3600);
    let hot_mgr = HotStoreManager::new();
    let ctx = test_ctx(&spec, &meta, &schema, &segment_mgr, &memtable_mgr, &hot_mgr);

    let node = FoldedAggregateFastPath {
        group_by: &[],
        aggregates: &[],
    };
    let outcome = node.try_answer(&ctx).unwrap();
    match outcome {
        FastPathOutcome::Declined(decline) => {
            assert_eq!(decline.node, "folded_aggregate");
            assert_eq!(decline.reason, DeclineReason::NodeInternal);
        }
        other => panic!("expected decline, got {other:?}"),
    }
}
