//! Tests for the streaming aggregate operator and the metadata-driven
//! COUNT/MIN/MAX fast path. These guard against the OOM regression where the
//! old `HashAggregateExec` materialized every input row.

use tempfile::TempDir;

use taibai::net::session::Session;
use taibai::sql::execution::engine::{Engine, ExecResult};
use taibai::types::scalar::ScalarValue;

fn create_engine() -> (TempDir, Engine, Session) {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("failed to create engine");
    let session = Session::new("root".to_string(), "main".to_string());
    (temp_dir, engine, session)
}

fn collect_rows(result: ExecResult) -> Vec<Vec<ScalarValue>> {
    let ExecResult::Query { batches, .. } = result else {
        panic!("expected query result");
    };
    let mut rows = Vec::new();
    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            rows.push(
                (0..batch.num_columns())
                    .map(|col_idx| batch.scalar(row_idx, col_idx).expect("scalar"))
                    .collect(),
            );
        }
    }
    rows
}

fn first_scalar(result: ExecResult, row: usize, col: usize) -> ScalarValue {
    let rows = collect_rows(result);
    rows.get(row)
        .and_then(|r| r.get(col))
        .cloned()
        .unwrap_or_else(|| panic!("missing scalar at ({},{})", row, col))
}

fn seed_metrics(engine: &Engine, session: &mut Session) {
    engine
        .execute(
            session,
            "CREATE TABLE metrics (
                id INT,
                bucket INT,
                amount FLOAT,
                note VARCHAR
            )",
        )
        .expect("create metrics");
    // Spread rows across multiple batches/row groups via repeated inserts.
    for batch in 0..4 {
        let mut sql = String::from("INSERT INTO metrics VALUES ");
        let mut parts = Vec::new();
        for i in 0..200 {
            let id = batch * 200 + i;
            let bucket = id % 5; // 5 distinct groups
            let amount = (id as f64) * 1.5;
            let note = if id % 7 == 0 { "NULL" } else { "'x'" };
            parts.push(format!("({}, {}, {}, {})", id, bucket, amount, note));
        }
        sql.push_str(&parts.join(", "));
        engine
            .execute(session, &sql)
            .unwrap_or_else(|e| panic!("insert batch {}: {}", batch, e));
    }
    engine
        .execute(session, "FLUSH TABLE metrics")
        .expect("flush metrics");
}

#[test]
fn count_min_max_combo_matches_expected() {
    let (_temp_dir, engine, mut session) = create_engine();
    seed_metrics(&engine, &mut session);

    // The exact query shape from the OOM report: COUNT + MIN/MAX over a
    // filtered single table.
    let rows = collect_rows(
        engine
            .execute(
                &mut session,
                "SELECT COUNT(*), MIN(id), MAX(id), MIN(bucket), MAX(bucket) FROM metrics WHERE bucket >= 0",
            )
            .expect("combo query"),
    );

    assert_eq!(rows.len(), 1, "single aggregate row");
    let r = &rows[0];
    // 800 total rows; id ranges [0,799]; bucket ranges [0,4].
    assert_eq!(r[0], ScalarValue::Int64(Some(800)));
    assert_eq!(r[1], ScalarValue::Int32(Some(0)));
    assert_eq!(r[2], ScalarValue::Int32(Some(799)));
    assert_eq!(r[3], ScalarValue::Int32(Some(0)));
    assert_eq!(r[4], ScalarValue::Int32(Some(4)));
    engine.segment_mgr().shutdown();
}

#[test]
fn simple_metadata_aggregate_with_where_clause() {
    let (_temp_dir, engine, mut session) = create_engine();
    seed_metrics(&engine, &mut session);

    let rows = collect_rows(
        engine
            .execute(
                &mut session,
                "SELECT COUNT(*), MIN(id), MAX(id) FROM metrics WHERE bucket = 2",
            )
            .expect("filtered combo"),
    );
    assert_eq!(rows.len(), 1);
    let r = &rows[0];
    // bucket == 2 => ids {2,7,12,...,799? } -> 160 rows, min 2, max 797
    assert_eq!(r[0], ScalarValue::Int64(Some(160)));
    assert_eq!(r[1], ScalarValue::Int32(Some(2)));
    assert_eq!(r[2], ScalarValue::Int32(Some(797)));
    engine.segment_mgr().shutdown();
}

#[test]
fn group_by_streaming_aggregate_matches_expected() {
    let (_temp_dir, engine, mut session) = create_engine();
    seed_metrics(&engine, &mut session);

    let mut rows = collect_rows(
        engine
            .execute(
                &mut session,
                "SELECT bucket, COUNT(*), MIN(id), MAX(id) FROM metrics GROUP BY bucket ORDER BY bucket",
            )
            .expect("group by"),
    );
    // ORDER BY may be applied outside; sort defensively by bucket.
    rows.sort_by_key(|r| match r[0] {
        ScalarValue::Int32(Some(v)) => v,
        _ => i32::MIN,
    });
    assert_eq!(rows.len(), 5, "5 buckets");
    for (i, r) in rows.iter().enumerate() {
        assert_eq!(r[0], ScalarValue::Int32(Some(i as i32)));
        assert_eq!(r[1], ScalarValue::Int64(Some(160)), "bucket {} count", i);
    }
    engine.segment_mgr().shutdown();
}

#[test]
fn sum_and_avg_via_streaming_accumulator() {
    let (_temp_dir, engine, mut session) = create_engine();
    seed_metrics(&engine, &mut session);

    let rows = collect_rows(
        engine
            .execute(
                &mut session,
                "SELECT SUM(id), AVG(id) FROM metrics WHERE bucket = 0",
            )
            .expect("sum/avg"),
    );
    assert_eq!(rows.len(), 1);
    let r = &rows[0];
    // bucket == 0 => ids {0,5,10,...,795} = 160 values; sum = 5*(0+1+..+159)
    let expected_sum: i64 = 5 * (0..160i64).sum::<i64>();
    match &r[0] {
        ScalarValue::Int64(Some(v)) => assert_eq!(*v, expected_sum),
        other => panic!("unexpected sum scalar {:?}", other),
    }
    match &r[1] {
        ScalarValue::Float64(Some(v)) => {
            let expected_avg = expected_sum as f64 / 160.0;
            assert!(
                (v - expected_avg).abs() < 1e-6,
                "avg {} vs expected {}",
                v,
                expected_avg
            );
        }
        other => panic!("unexpected avg scalar {:?}", other),
    }
    engine.segment_mgr().shutdown();
}

#[test]
fn range_predicate_full_row_group_matches_metadata_result() {
    let (_temp_dir, engine, mut session) = create_engine();
    engine
        .execute(
            &mut session,
            "CREATE TABLE covered_metrics (bucket INT, amount INT)",
        )
        .expect("create covered_metrics");
    engine
        .execute(
            &mut session,
            "INSERT INTO covered_metrics VALUES (10, 100), (20, 200), (30, 300)",
        )
        .expect("insert covered_metrics");
    engine
        .execute(&mut session, "FLUSH TABLE covered_metrics")
        .expect("flush covered_metrics");

    let rows = collect_rows(
        engine
            .execute(
                &mut session,
                "SELECT COUNT(*), MIN(amount), MAX(amount) FROM covered_metrics WHERE bucket >= 0",
            )
            .expect("covered aggregate"),
    );
    assert_eq!(rows[0][0], ScalarValue::Int64(Some(3)));
    assert_eq!(rows[0][1], ScalarValue::Int32(Some(100)));
    assert_eq!(rows[0][2], ScalarValue::Int32(Some(300)));
    engine.segment_mgr().shutdown();
}

#[test]
fn filtered_max_does_not_use_non_matching_row_group_extreme() {
    let (_temp_dir, engine, mut session) = create_engine();
    engine
        .execute(
            &mut session,
            "CREATE TABLE filtered_metrics (bucket INT, amount INT)",
        )
        .expect("create filtered_metrics");
    engine
        .execute(
            &mut session,
            "INSERT INTO filtered_metrics VALUES (1, 10), (2, 999), (1, 20)",
        )
        .expect("insert filtered_metrics");
    engine
        .execute(&mut session, "FLUSH TABLE filtered_metrics")
        .expect("flush filtered_metrics");

    let rows = collect_rows(
        engine
            .execute(
                &mut session,
                "SELECT MAX(amount) FROM filtered_metrics WHERE bucket = 1",
            )
            .expect("filtered max"),
    );
    assert_eq!(rows[0][0], ScalarValue::Int32(Some(20)));
    engine.segment_mgr().shutdown();
}

#[test]
fn filtered_max_reads_predicate_and_aggregate_columns() {
    let (_temp_dir, engine, mut session) = create_engine();
    engine
        .execute(
            &mut session,
            "CREATE TABLE projected_metrics (id INT, bucket INT, amount INT)",
        )
        .expect("create projected_metrics");
    engine
        .execute(
            &mut session,
            "INSERT INTO projected_metrics VALUES (1, 1, 10), (2, 2, 999), (3, 1, 20)",
        )
        .expect("insert projected_metrics");
    engine
        .execute(&mut session, "FLUSH TABLE projected_metrics")
        .expect("flush projected_metrics");

    let rows = collect_rows(
        engine
            .execute(
                &mut session,
                "SELECT MAX(amount) FROM projected_metrics WHERE bucket = 1",
            )
            .expect("projected filtered max"),
    );
    assert_eq!(rows[0][0], ScalarValue::Int32(Some(20)));
    engine.segment_mgr().shutdown();
}

#[test]
fn aggregate_respects_delete_bitmap_for_min_max() {
    let (_temp_dir, engine, mut session) = create_engine();
    seed_metrics(&engine, &mut session);

    // Delete the smallest id; MIN must shift.
    engine
        .execute(&mut session, "DELETE FROM metrics WHERE id = 0")
        .expect("delete id=0");

    let rows = collect_rows(
        engine
            .execute(
                &mut session,
                "SELECT COUNT(*), MIN(id), MAX(id) FROM metrics",
            )
            .expect("min/max after delete"),
    );
    let r = &rows[0];
    assert_eq!(r[0], ScalarValue::Int64(Some(799)));
    assert_eq!(r[1], ScalarValue::Int32(Some(1)), "min shifted up");
    assert_eq!(r[2], ScalarValue::Int32(Some(799)));
    engine.segment_mgr().shutdown();
}

#[test]
fn large_input_does_not_oom() {
    // Regression for the original OOM: streaming aggregation must keep memory
    // bounded even with many rows. We don't assert memory directly; the test
    // passing (rather than aborting) inside the test process is the signal.
    let (_temp_dir, engine, mut session) = create_engine();
    engine
        .execute(&mut session, "CREATE TABLE big (id INT, grp INT, val INT)")
        .expect("create big");

    // Insert 50k rows in chunks. Each insert is a separate batch so the engine
    // builds several row groups.
    for chunk in 0..50 {
        let mut sql = String::from("INSERT INTO big VALUES ");
        let parts: Vec<String> = (0..1000)
            .map(|i| {
                let id = chunk * 1000 + i;
                format!("({}, {}, {})", id, id % 100, id)
            })
            .collect();
        sql.push_str(&parts.join(", "));
        engine.execute(&mut session, &sql).expect("insert chunk");
    }
    engine
        .execute(&mut session, "FLUSH TABLE big")
        .expect("flush big");

    let rows = collect_rows(
        engine
            .execute(
                &mut session,
                "SELECT COUNT(*), MIN(val), MAX(val), SUM(val) FROM big",
            )
            .expect("big aggregate"),
    );
    assert_eq!(rows.len(), 1);
    let r = &rows[0];
    assert_eq!(r[0], ScalarValue::Int64(Some(50_000)));
    assert_eq!(r[1], ScalarValue::Int32(Some(0)));
    assert_eq!(r[2], ScalarValue::Int32(Some(49_999)));
    // sum 0..50000
    let expected: i64 = (0..50_000i64).sum();
    match r[3] {
        ScalarValue::Int64(Some(v)) => assert_eq!(v, expected),
        ref other => panic!("unexpected sum {:?}", other),
    }
    engine.segment_mgr().shutdown();
}

// `first_scalar` is exercised by ad-hoc asserts above; keep the helper linked.
#[test]
fn _link_first_scalar_helper() {
    let (_temp_dir, engine, mut session) = create_engine();
    engine
        .execute(&mut session, "CREATE TABLE t (a INT)")
        .expect("create");
    engine
        .execute(&mut session, "INSERT INTO t VALUES (42)")
        .expect("insert");
    let v = first_scalar(
        engine
            .execute(&mut session, "SELECT MIN(a) FROM t")
            .expect("min"),
        0,
        0,
    );
    assert_eq!(v, ScalarValue::Int32(Some(42)));
    engine.segment_mgr().shutdown();
}

#[test]
fn case_expressions_execute_in_projection_and_aggregates() {
    let (_temp_dir, engine, mut session) = create_engine();
    engine
        .execute(
            &mut session,
            "CREATE TABLE case_metrics (id INT, volume BIGINT, high_px DECIMAL(18, 4), low_px DECIMAL(18, 4))",
        )
        .expect("create case metrics");
    engine
        .execute(
            &mut session,
            "INSERT INTO case_metrics VALUES (1, 10, 10.5000, 10.2000), (2, 0, 99.0000, 1.0000), (3, 20, 10.8000, 10.3000)",
        )
        .expect("insert case metrics");

    let rows = collect_rows(
        engine
            .execute(
                &mut session,
                "SELECT MAX(CASE volume WHEN 0 THEN 0 ELSE high_px END), MIN(CASE WHEN volume = 0 THEN 5000 ELSE low_px END) FROM case_metrics",
            )
            .expect("aggregate case query"),
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0][0],
        ScalarValue::Decimal(Some(108000)),
        "unexpected CASE aggregate rows: {:?}",
        rows
    );
    assert_eq!(rows[0][1], ScalarValue::Decimal(Some(102000)));

    // Production-shaped statMinute query: SUM(trade_value) must preserve
    // DECIMAL rather than emitting an INT64 into a DECIMAL result column.
    let rows = collect_rows(
        engine
            .execute(
                &mut session,
                "SELECT volume, MAX(CASE volume WHEN 0 THEN 0 ELSE high_px END), MIN(CASE volume WHEN 0 THEN 5000 ELSE low_px END), SUM(volume), SUM(high_px), COUNT(*) FROM case_metrics GROUP BY volume ORDER BY volume",
            )
            .expect("production-shaped decimal aggregate query"),
    );
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][4], ScalarValue::Decimal(Some(990000)));
    assert_eq!(rows[1][4], ScalarValue::Decimal(Some(105000)));
    assert_eq!(rows[2][4], ScalarValue::Decimal(Some(108000)));

    let rows = collect_rows(
        engine
            .execute(
                &mut session,
                "SELECT volume, MAX(CASE volume WHEN 0 THEN 0 ELSE high_px END) FROM case_metrics GROUP BY volume ORDER BY volume",
            )
            .expect("grouped aggregate case query"),
    );
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][1], ScalarValue::Decimal(Some(0)));
    assert_eq!(rows[1][1], ScalarValue::Decimal(Some(105000)));
    assert_eq!(rows[2][1], ScalarValue::Decimal(Some(108000)));

    let rows = collect_rows(
        engine
            .execute(
                &mut session,
                "SELECT CASE WHEN volume = 0 THEN 'zero' WHEN volume > 10 THEN 'large' ELSE 'small' END FROM case_metrics ORDER BY id",
            )
            .expect("searched case query"),
    );
    assert_eq!(rows[0][0], ScalarValue::VarChar(Some("small".into())));
    assert_eq!(rows[1][0], ScalarValue::VarChar(Some("zero".into())));
    assert_eq!(rows[2][0], ScalarValue::VarChar(Some("large".into())));

    let rows = collect_rows(
        engine
            .execute(
                &mut session,
                "SELECT id, CASE WHEN volume = 0 THEN CASE WHEN id = 2 THEN 'nested' ELSE 'other' END END FROM case_metrics WHERE CASE WHEN volume > 0 THEN true ELSE false END ORDER BY id",
            )
            .expect("nested and filter case query"),
    );
    assert_eq!(rows.len(), 2);
    assert!(rows[0][1].is_null());
    assert!(rows[1][1].is_null());
    engine.segment_mgr().shutdown();
}

// ---------------------------------------------------------------------------
// Run-level predicate folding: AVG/COUNT/SUM/MIN/MAX over an RLE-encoded
// column with eq/range predicates fold straight from the encoded runs
// instead of decode + per-row mask + gather. These tests pin the semantics
// against hand-computed results, including NULL rows, zero-match AVG,
// vacuous (zone-map proven) predicates, and the delete-bitmap fallback.
// ---------------------------------------------------------------------------

fn seed_run_ticks(engine: &Engine, session: &mut Session) {
    engine
        .execute(
            session,
            "CREATE TABLE run_ticks (day INT, sym VARCHAR, px INT)",
        )
        .expect("create run_ticks");
    // Long runs so the px column lands RLE-encoded after FLUSH: four value
    // bands of 500 rows each, plus 50 NULL-px rows.
    let mut parts = Vec::new();
    for &(px, len) in &[(10i64, 500usize), (60, 500), (200, 500), (5, 500)] {
        for i in 0..len {
            parts.push(format!("(1, 's{}', {})", i % 7, px));
        }
    }
    for i in 0..50 {
        parts.push(format!("(1, 's{}', NULL)", i % 7));
    }
    let sql = format!("INSERT INTO run_ticks VALUES {}", parts.join(", "));
    engine.execute(session, &sql).expect("insert run_ticks");
    engine
        .execute(session, "FLUSH TABLE run_ticks")
        .expect("flush run_ticks");
}

#[test]
fn run_fold_avg_count_sum_min_max_with_predicate() {
    let (_temp_dir, engine, mut session) = create_engine();
    seed_run_ticks(&engine, &mut session);

    let rows = collect_rows(
        engine
            .execute(
                &mut session,
                "SELECT COUNT(*), AVG(px), SUM(px), MIN(px), MAX(px) FROM run_ticks WHERE px > 20",
            )
            .expect("run-fold aggregate"),
    );
    assert_eq!(rows.len(), 1);
    let r = &rows[0];
    // Matching rows: px=60 (500 rows) and px=200 (500 rows); the 50 NULL
    // rows never satisfy the predicate.
    assert_eq!(r[0], ScalarValue::Int64(Some(1000)));
    match &r[1] {
        ScalarValue::Float64(Some(v)) => assert!((v - 130.0).abs() < 1e-9, "avg {} vs 130", v),
        other => panic!("unexpected avg scalar {:?}", other),
    }
    assert_eq!(r[2], ScalarValue::Int64(Some(130_000)));
    assert_eq!(r[3], ScalarValue::Int32(Some(60)));
    assert_eq!(r[4], ScalarValue::Int32(Some(200)));
    engine.segment_mgr().shutdown();
}

#[test]
fn run_fold_with_vacuous_partition_predicate() {
    let (_temp_dir, engine, mut session) = create_engine();
    seed_run_ticks(&engine, &mut session);

    // day = 1 holds for every row group (zone map min == max): the clause is
    // vacuous, px >= 60 stays the only discriminating predicate.
    let rows = collect_rows(
        engine
            .execute(
                &mut session,
                "SELECT COUNT(*), AVG(px) FROM run_ticks WHERE day = 1 AND px >= 60",
            )
            .expect("vacuous + range aggregate"),
    );
    assert_eq!(rows[0][0], ScalarValue::Int64(Some(1000)));
    match &rows[0][1] {
        ScalarValue::Float64(Some(v)) => assert!((v - 130.0).abs() < 1e-9),
        other => panic!("unexpected avg scalar {:?}", other),
    }
    engine.segment_mgr().shutdown();
}

#[test]
fn run_fold_zero_match_avg_is_null_and_count_zero() {
    let (_temp_dir, engine, mut session) = create_engine();
    seed_run_ticks(&engine, &mut session);

    let rows = collect_rows(
        engine
            .execute(
                &mut session,
                "SELECT COUNT(*), AVG(px), SUM(px), MIN(px) FROM run_ticks WHERE px > 1000",
            )
            .expect("zero-match aggregate"),
    );
    assert_eq!(rows[0][0], ScalarValue::Int64(Some(0)));
    assert!(rows[0][1].is_null(), "avg over no rows is NULL");
    assert!(rows[0][2].is_null(), "sum over no rows is NULL");
    assert!(rows[0][3].is_null(), "min over no rows is NULL");
    engine.segment_mgr().shutdown();
}

#[test]
fn run_fold_decimal_avg_uses_raw_domain_threshold() {
    let (_temp_dir, engine, mut session) = create_engine();
    engine
        .execute(
            &mut session,
            "CREATE TABLE dec_ticks (day INT, px DECIMAL(9,2))",
        )
        .expect("create dec_ticks");
    let mut parts = Vec::new();
    for &(px, len) in &[
        ("10.00", 300usize),
        ("60.50", 300),
        ("200.25", 300),
        ("5.75", 300),
    ] {
        for _ in 0..len {
            parts.push(format!("(1, {})", px));
        }
    }
    let sql = format!("INSERT INTO dec_ticks VALUES {}", parts.join(", "));
    engine
        .execute(&mut session, &sql)
        .expect("insert dec_ticks");
    engine
        .execute(&mut session, "FLUSH TABLE dec_ticks")
        .expect("flush dec_ticks");

    // The comparison happens in the raw scaled domain everywhere in the
    // pipeline (`px > 10.00` thresholds raw 1000): the run fold must agree
    // with the row kernels. Matching bands: 60.50 and 200.25 across 600
    // rows -> the raw-domain sum is 7822500, and AVG must divide out the
    // DECIMAL(9,2) scale at finalize: 7822500 / 600 / 100 = 130.375.
    let rows = collect_rows(
        engine
            .execute(
                &mut session,
                "SELECT COUNT(*), AVG(px) FROM dec_ticks WHERE px > 10.00",
            )
            .expect("decimal run-fold aggregate"),
    );
    assert_eq!(rows[0][0], ScalarValue::Int64(Some(600)));
    match &rows[0][1] {
        ScalarValue::Float64(Some(v)) => {
            assert!((v - 130.375).abs() < 1e-9, "avg {} vs 130.375", v)
        }
        other => panic!("unexpected avg scalar {:?}", other),
    }

    // Cross-check the same predicate through the GROUP BY pipeline: the
    // routed (run-fold) answer must equal the generic engine answer.
    let grouped = collect_rows(
        engine
            .execute(
                &mut session,
                "SELECT COUNT(*), AVG(px) FROM dec_ticks WHERE px > 10.00 GROUP BY day",
            )
            .expect("decimal grouped cross-check"),
    );
    assert_eq!(rows[0], grouped[0], "routed must equal grouped");
    engine.segment_mgr().shutdown();
}

#[test]
fn run_fold_falls_back_correctly_with_delete_bitmap() {
    let (_temp_dir, engine, mut session) = create_engine();
    seed_run_ticks(&engine, &mut session);

    // A delete bitmap disables the run fold for that segment; the batch fold
    // must produce the same answer over the surviving rows.
    engine
        .execute(&mut session, "DELETE FROM run_ticks WHERE px = 60")
        .expect("delete px=60 band");

    let rows = collect_rows(
        engine
            .execute(
                &mut session,
                "SELECT COUNT(*), AVG(px), MIN(px), MAX(px) FROM run_ticks WHERE px > 20",
            )
            .expect("post-delete aggregate"),
    );
    assert_eq!(rows[0][0], ScalarValue::Int64(Some(500)));
    match &rows[0][1] {
        ScalarValue::Float64(Some(v)) => assert!((v - 200.0).abs() < 1e-9),
        other => panic!("unexpected avg scalar {:?}", other),
    }
    assert_eq!(rows[0][2], ScalarValue::Int32(Some(200)));
    assert_eq!(rows[0][3], ScalarValue::Int32(Some(200)));
    engine.segment_mgr().shutdown();
}

#[test]
fn avg_without_predicate_routes_and_matches_generic_pipeline() {
    let (_temp_dir, engine, mut session) = create_engine();
    seed_run_ticks(&engine, &mut session);

    // No-predicate AVG now routes through the metadata path's data folds;
    // cross-check it against the GROUP BY pipeline over the same data.
    let routed = collect_rows(
        engine
            .execute(
                &mut session,
                "SELECT COUNT(*), COUNT(px), SUM(px), AVG(px) FROM run_ticks",
            )
            .expect("no-predicate avg"),
    );
    let grouped = collect_rows(
        engine
            .execute(
                &mut session,
                "SELECT COUNT(*), COUNT(px), SUM(px), AVG(px) FROM run_ticks GROUP BY day",
            )
            .expect("grouped avg cross-check"),
    );
    for (routed, grouped) in routed.iter().zip(grouped.iter()) {
        assert_eq!(routed, grouped, "routed row must equal grouped row");
    }
    assert_eq!(routed[0][0], ScalarValue::Int64(Some(2050)));
    // NULL literals now survive the flush (the buffered-insert encoder
    // previously dropped the null bitmap), so both pipelines average the
    // 2000 non-null px values: 137500 / 2000 = 68.75.
    assert_eq!(routed[0][1], ScalarValue::Int64(Some(2000)), "count(px)");
    match &routed[0][3] {
        ScalarValue::Float64(Some(a)) => {
            assert!((a - 68.75).abs() < 1e-9, "avg {} vs 68.75", a);
        }
        other => panic!("unexpected avg scalar {:?}", other),
    }
    engine.segment_mgr().shutdown();
}
