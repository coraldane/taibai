use super::*;
use crate::types::batch::ColumnDef;

fn sum_schema() -> Schema {
    Schema::new(vec![ColumnDef::new("v", DataType::Int64)])
}

fn batch_of(values: &[i64]) -> RecordBatch {
    let schema = sum_schema();
    let mut batch = RecordBatch::new(schema);
    for value in values {
        batch
            .column_mut(0)
            .unwrap()
            .push(&ScalarValue::Int64(Some(*value)))
            .unwrap();
    }
    batch
}

/// The data-fold path must report overflow exactly like the footer
/// col_sums path and the generic aggregate operator: cross-group
/// accumulation that exceeds i64 is an error, not a wrapped value.
#[test]
fn data_fold_sum_overflow_errors_instead_of_wrapping() {
    let schema = sum_schema();
    let mut accumulator = AggAccum::new(AggFunc::Sum, Some(0), &schema);
    accumulator
        .fold_batch(&batch_of(&[i64::MAX - 1]), Some(0))
        .expect("first partial fits");
    let error = accumulator
        .fold_batch(&batch_of(&[i64::MAX - 1]), Some(0))
        .expect_err("cross-group overflow must error");
    assert!(error.to_string().contains("SUM overflow"), "got: {error}");
}

/// Cached per-segment partials flow through the same accumulation point.
#[test]
fn cached_partial_sum_overflow_errors_instead_of_wrapping() {
    let schema = sum_schema();
    let mut accumulator = AggAccum::new(AggFunc::Sum, Some(0), &schema);
    accumulator
        .fold_batch(&batch_of(&[i64::MAX - 1]), Some(0))
        .expect("first partial fits");
    let error = accumulator
        .fold_cached_sum(&SumCacheValue {
            i: Some(i64::MAX - 1),
            f: None,
        })
        .expect_err("cached-partial overflow must error");
    assert!(error.to_string().contains("SUM overflow"), "got: {error}");
}

/// The drift classifier: RENAME (previous_names) and DROP COLUMN (extra
/// footer columns) resolve as Present; a missing column folds its
/// constant DEFAULT (absent default = constant NULL); CURRENT_TIMESTAMP
/// is not constant and must decline.
#[test]
fn footer_column_drift_classifies_by_name() {
    let mut renamed = ColumnDef::new("total", DataType::Int64);
    renamed.previous_names.push("v".to_string());
    let table = Schema::new(vec![
        ColumnDef::new("id", DataType::Int64),
        renamed,
        ColumnDef::new("score", DataType::Int64).with_default_value(Some(
            crate::types::batch::ColumnDefaultValue::Literal(ScalarValue::Int64(Some(7))),
        )),
        ColumnDef::new("note", DataType::Int64),
        ColumnDef::new("created", DataType::Timestamp).with_default_value(Some(
            crate::types::batch::ColumnDefaultValue::CurrentTimestamp,
        )),
    ]);
    // Segment written before the RENAME and before every ADD: footer
    // keeps the old names and none of the new columns. Extra footer
    // columns (a later DROP COLUMN) would be ignored the same way.
    let footer = Schema::new(vec![
        ColumnDef::new("dropped_extra", DataType::Int64),
        ColumnDef::new("id", DataType::Int64),
        ColumnDef::new("v", DataType::Int64),
    ]);

    assert_eq!(
        footer_column_drift(&table, &footer, 0),
        FooterColumnDrift::Present
    );
    assert_eq!(
        footer_column_drift(&table, &footer, 1),
        FooterColumnDrift::Present,
        "RENAME drift resolves through previous_names"
    );
    assert_eq!(
        footer_column_drift(&table, &footer, 2),
        FooterColumnDrift::ConstantDefault(ScalarValue::Int64(Some(7)))
    );
    assert_eq!(
        footer_column_drift(&table, &footer, 3),
        FooterColumnDrift::ConstantDefault(ScalarValue::Int64(None)),
        "absent default folds as a constant NULL"
    );
    assert_eq!(
        footer_column_drift(&table, &footer, 4),
        FooterColumnDrift::NonConstantDefault
    );
}

/// The constant-DEFAULT fold must mirror the zone-map fold semantics:
/// a NULL default (or zero rows) contributes nothing, a non-null
/// default contributes to every aggregate, and SUM reports overflow
/// instead of wrapping.
#[test]
fn fold_constant_default_matches_zone_map_semantics() {
    let schema = sum_schema();
    let seven = ScalarValue::Int64(Some(7));

    let mut sum = AggAccum::new(AggFunc::Sum, Some(0), &schema);
    sum.fold_constant_default(&seven, 50).unwrap();
    assert_eq!(sum.finalize(), ScalarValue::Int64(Some(350)));

    // NULL default: all-NULL column — SUM stays NULL, not 0.
    let mut sum_null = AggAccum::new(AggFunc::Sum, Some(0), &schema);
    sum_null
        .fold_constant_default(&ScalarValue::Int64(None), 50)
        .unwrap();
    assert_eq!(sum_null.finalize(), ScalarValue::Int64(None));

    // Zero rows contribute nothing (MIN stays NULL).
    let mut min = AggAccum::new(AggFunc::Min, Some(0), &schema);
    min.fold_constant_default(&seven, 0).unwrap();
    assert_eq!(min.finalize(), ScalarValue::Int64(None));
    let mut min = AggAccum::new(AggFunc::Min, Some(0), &schema);
    min.fold_constant_default(&seven, 10).unwrap();
    assert_eq!(min.finalize(), ScalarValue::Int64(Some(7)));

    let mut max = AggAccum::new(AggFunc::Max, Some(0), &schema);
    max.fold_constant_default(&seven, 10).unwrap();
    assert_eq!(max.finalize(), ScalarValue::Int64(Some(7)));

    let mut count = AggAccum::new(AggFunc::Count, Some(0), &schema);
    count.fold_constant_default(&seven, 10).unwrap();
    count
        .fold_constant_default(&ScalarValue::Int64(None), 5)
        .unwrap();
    assert_eq!(count.finalize(), ScalarValue::Int64(Some(10)));

    let mut avg = AggAccum::new(AggFunc::Avg, Some(0), &schema);
    avg.fold_constant_default(&seven, 21).unwrap();
    match avg.finalize() {
        ScalarValue::Float64(Some(value)) => assert_eq!(value, 7.0),
        other => panic!("expected 7.0, got {other:?}"),
    }

    let mut sum_overflow = AggAccum::new(AggFunc::Sum, Some(0), &schema);
    sum_overflow
        .fold_batch(&batch_of(&[i64::MAX - 1]), Some(0))
        .unwrap();
    let error = sum_overflow
        .fold_constant_default(&seven, u64::MAX)
        .expect_err("default x rows overflow must error");
    assert!(error.to_string().contains("SUM overflow"), "got: {error}");
}

/// Engine-level: after ALTER ADD COLUMN with a constant default, the
/// pre-ALTER segment's footer drifts from the catalog. The metadata
/// fast path must NOT decline (the old whole-schema gate returned
/// None): SUM(new_col) folds default x rows, COUNT folds the row count,
/// and a RENAME-drifted SUM resolves by previous name and folds the
/// footer's per-row-group col_sums.
#[test]
fn compute_simple_aggregate_keeps_fast_path_across_add_and_rename_drift() {
    let dir = tempfile::tempdir().expect("temp dir");
    let engine = crate::sql::execution::engine::Engine::new(dir.path(), "main").expect("engine");
    let mut session = crate::net::session::Session::new("root".to_string(), "main".to_string());
    engine
        .execute(&mut session, "CREATE TABLE t (id INT64, v INT64)")
        .expect("create");
    let mut sql = String::from("INSERT INTO t (id, v) VALUES ");
    for i in 0..50i64 {
        if i > 0 {
            sql.push_str(", ");
        }
        sql.push_str(&format!("({}, {})", i + 1, i + 1));
    }
    engine.execute(&mut session, &sql).expect("insert");
    engine.flush_table_buffers("main", "t").expect("flush");
    engine
        .execute(
            &mut session,
            "ALTER TABLE t ADD COLUMN score INT64 DEFAULT 7",
        )
        .expect("add column");
    engine
        .execute(&mut session, "ALTER TABLE t RENAME COLUMN v TO value")
        .expect("rename");

    let table_meta = engine.get_table_meta("main", "t").expect("table meta");
    let table_schema = Schema::new(table_meta.columns.clone());
    let mut spec = ScanSpec::new("t");
    spec.table_schema = table_schema.clone();
    let score_idx = table_schema.index_of("score").expect("score column");
    let value_idx = table_schema.index_of("value").expect("value column");
    let aggregates = vec![
        AggregateExpr {
            func: AggFunc::Sum,
            arg: Box::new(LogicalExpr::Column(score_idx)),
            distinct: false,
        },
        AggregateExpr {
            func: AggFunc::Count,
            arg: Box::new(LogicalExpr::Column(score_idx)),
            distinct: false,
        },
        AggregateExpr {
            func: AggFunc::Sum,
            arg: Box::new(LogicalExpr::Column(value_idx)),
            distinct: false,
        },
    ];
    let batch = compute_folded_aggregate(
        &spec,
        &[],
        &aggregates,
        &table_meta,
        &table_schema,
        &dir.path().join("main").join("t"),
        engine.segment_mgr(),
        engine.memtable_manager(),
        engine.hot_store_manager(),
        "main",
    )
    .expect("compute")
    .expect("drifted footer (ADD COLUMN default + RENAME) must keep the fast path");
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(
        batch.scalar(0, 0),
        Some(ScalarValue::Int64(Some(350))),
        "SUM(score) = default 7 x 50 drifted rows"
    );
    assert_eq!(
        batch.scalar(0, 1),
        Some(ScalarValue::Int64(Some(50))),
        "COUNT(score) counts the default rows"
    );
    assert_eq!(
        batch.scalar(0, 2),
        Some(ScalarValue::Int64(Some(1275))),
        "SUM(value) folds the renamed column's footer sums"
    );
    engine.segment_mgr().shutdown();
}

/// Engine-level boundary: DEFAULT CURRENT_TIMESTAMP is not a constant,
/// so a segment predating the column still declines the fast path.
#[test]
fn compute_simple_aggregate_declines_current_timestamp_default() {
    let dir = tempfile::tempdir().expect("temp dir");
    let engine = crate::sql::execution::engine::Engine::new(dir.path(), "main").expect("engine");
    let mut session = crate::net::session::Session::new("root".to_string(), "main".to_string());
    engine
        .execute(&mut session, "CREATE TABLE t (id INT64, v INT64)")
        .expect("create");
    engine
        .execute(
            &mut session,
            "INSERT INTO t (id, v) VALUES (1, 10), (2, 20)",
        )
        .expect("insert");
    engine.flush_table_buffers("main", "t").expect("flush");
    engine
        .execute(
            &mut session,
            "ALTER TABLE t ADD COLUMN created TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
        )
        .expect("add column");

    let table_meta = engine.get_table_meta("main", "t").expect("table meta");
    let table_schema = Schema::new(table_meta.columns.clone());
    let mut spec = ScanSpec::new("t");
    spec.table_schema = table_schema.clone();
    let created_idx = table_schema.index_of("created").expect("created column");
    let aggregates = vec![AggregateExpr {
        func: AggFunc::Min,
        arg: Box::new(LogicalExpr::Column(created_idx)),
        distinct: false,
    }];
    let batch = compute_folded_aggregate(
        &spec,
        &[],
        &aggregates,
        &table_meta,
        &table_schema,
        &dir.path().join("main").join("t"),
        engine.segment_mgr(),
        engine.memtable_manager(),
        engine.hot_store_manager(),
        "main",
    )
    .expect("compute");
    assert!(
        batch.is_none(),
        "CURRENT_TIMESTAMP default must decline the metadata fast path"
    );
    engine.segment_mgr().shutdown();
}

/// Engine-level: a day-partitioned table's GROUP BY partition key folds
/// entirely from segment metadata (returns `Some`), one row per day in
/// first-seen order — no row data is decoded.
#[test]
fn compute_folded_aggregate_answers_group_by_partition_key_from_metadata() {
    let dir = tempfile::tempdir().expect("temp dir");
    let engine = crate::sql::execution::engine::Engine::new(dir.path(), "main").expect("engine");
    let mut session = crate::net::session::Session::new("root".to_string(), "main".to_string());
    engine
        .execute(
            &mut session,
            "CREATE TABLE t (id INT64, deal_date INT64) PARTITION BY DAY(deal_date)",
        )
        .expect("create");
    let mut sql = String::from("INSERT INTO t (id, deal_date) VALUES ");
    let mut id = 0i64;
    let mut first = true;
    for day in [20240101i64, 20240102, 20240103] {
        for _ in 0..10 {
            if !first {
                sql.push_str(", ");
            }
            first = false;
            id += 1;
            sql.push_str(&format!("({}, {})", id, day));
        }
    }
    engine.execute(&mut session, &sql).expect("insert");
    engine.flush_table_buffers("main", "t").expect("flush");

    let table_meta = engine.get_table_meta("main", "t").expect("table meta");
    let table_schema = Schema::new(table_meta.columns.clone());
    let mut spec = ScanSpec::new("t");
    spec.table_schema = table_schema.clone();
    let date_idx = table_schema.index_of("deal_date").expect("date column");
    let group_by = vec![LogicalExpr::Column(date_idx)];
    let aggregates = vec![AggregateExpr {
        func: AggFunc::Count,
        arg: Box::new(LogicalExpr::Wildcard),
        distinct: false,
    }];
    let batch = compute_folded_aggregate(
        &spec,
        &group_by,
        &aggregates,
        &table_meta,
        &table_schema,
        &dir.path().join("main").join("t"),
        engine.segment_mgr(),
        engine.memtable_manager(),
        engine.hot_store_manager(),
        "main",
    )
    .expect("compute")
    .expect("day-partitioned GROUP BY must fold from metadata");
    assert_eq!(batch.num_rows(), 3, "one row per day");
    assert_eq!(batch.num_columns(), 2, "group key + count columns");
    for (row, day) in [(0usize, 20240101i64), (1, 20240102), (2, 20240103)] {
        assert_eq!(batch.scalar(row, 0), Some(ScalarValue::Int64(Some(day))));
        assert_eq!(batch.scalar(row, 1), Some(ScalarValue::Int64(Some(10))));
    }
    engine.segment_mgr().shutdown();
}

/// Engine-level: an active TTL makes every row's visibility row-
/// dependent, which metadata folds cannot see — the engine must decline
/// so the regular scan pipeline filters the TTL column first.
#[test]
fn compute_folded_aggregate_declines_active_ttl() {
    let dir = tempfile::tempdir().expect("temp dir");
    let engine = crate::sql::execution::engine::Engine::new(dir.path(), "main").expect("engine");
    let mut session = crate::net::session::Session::new("root".to_string(), "main".to_string());
    engine
        .execute(
            &mut session,
            "CREATE TABLE t (id INT64, ts TIMESTAMP) PARTITION BY DAY(ts) TTL (ts, 3600)",
        )
        .expect("create");
    engine
        .execute(
            &mut session,
            "INSERT INTO t (id, ts) VALUES (1, '2024-01-01 00:00:00')",
        )
        .expect("insert");
    engine.flush_table_buffers("main", "t").expect("flush");

    let table_meta = engine.get_table_meta("main", "t").expect("table meta");
    let table_schema = Schema::new(table_meta.columns.clone());
    let mut spec = ScanSpec::new("t");
    spec.table_schema = table_schema.clone();
    let ts_idx = table_schema.index_of("ts").expect("ts column");
    let group_by = vec![LogicalExpr::Column(ts_idx)];
    let aggregates = vec![AggregateExpr {
        func: AggFunc::Count,
        arg: Box::new(LogicalExpr::Wildcard),
        distinct: false,
    }];
    let batch = compute_folded_aggregate(
        &spec,
        &group_by,
        &aggregates,
        &table_meta,
        &table_schema,
        &dir.path().join("main").join("t"),
        engine.segment_mgr(),
        engine.memtable_manager(),
        engine.hot_store_manager(),
        "main",
    )
    .expect("compute");
    assert!(
        batch.is_none(),
        "active TTL must decline the folding engine"
    );
    engine.segment_mgr().shutdown();
}

/// Engine-level: expression GROUP BY keys decline (`None`), leaving the
/// regular pipeline in charge.
#[test]
fn compute_folded_aggregate_declines_expression_group_key() {
    let dir = tempfile::tempdir().expect("temp dir");
    let engine = crate::sql::execution::engine::Engine::new(dir.path(), "main").expect("engine");
    let mut session = crate::net::session::Session::new("root".to_string(), "main".to_string());
    engine
        .execute(&mut session, "CREATE TABLE t (id INT64, v INT64)")
        .expect("create");
    engine
        .execute(
            &mut session,
            "INSERT INTO t (id, v) VALUES (1, 10), (2, 20)",
        )
        .expect("insert");
    engine.flush_table_buffers("main", "t").expect("flush");

    let table_meta = engine.get_table_meta("main", "t").expect("table meta");
    let table_schema = Schema::new(table_meta.columns.clone());
    let mut spec = ScanSpec::new("t");
    spec.table_schema = table_schema.clone();
    let v_idx = table_schema.index_of("v").expect("v column");
    let group_by = vec![LogicalExpr::BinaryOp {
        left: Box::new(LogicalExpr::Column(v_idx)),
        op: crate::sql::parser::ast::BinOp::Add,
        right: Box::new(LogicalExpr::Literal(ScalarValue::Int64(Some(1)))),
    }];
    let aggregates = vec![AggregateExpr {
        func: AggFunc::Count,
        arg: Box::new(LogicalExpr::Wildcard),
        distinct: false,
    }];
    let batch = compute_folded_aggregate(
        &spec,
        &group_by,
        &aggregates,
        &table_meta,
        &table_schema,
        &dir.path().join("main").join("t"),
        engine.segment_mgr(),
        engine.memtable_manager(),
        engine.hot_store_manager(),
        "main",
    )
    .expect("compute");
    assert!(batch.is_none(), "expression group keys must decline");
    engine.segment_mgr().shutdown();
}
