//! Reproduction tests for aggregate regressions:
//! 1. DECIMAL aggregate results must keep the argument column's scale.
//! 2. COUNT must not double-count rows across flush / hot-store boundaries.

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

fn collect_rows(result: ExecResult) -> (taibai::types::batch::Schema, Vec<Vec<ScalarValue>>) {
    let ExecResult::Query { schema, batches } = result else {
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
    (schema, rows)
}

fn first_i64(result: ExecResult, row: usize, col: usize) -> i64 {
    let (_schema, rows) = collect_rows(result);
    match rows.get(row).and_then(|r| r.get(col)) {
        Some(ScalarValue::Int64(Some(v))) => *v,
        other => panic!("expected Int64 at ({},{}) got {:?}", row, col, other),
    }
}

#[test]
fn decimal_aggregate_result_keeps_scale() {
    let (_temp_dir, engine, mut session) = create_engine();
    engine
        .execute(
            &mut session,
            "CREATE TABLE dm (id INT, sym VARCHAR, px DECIMAL(18,4))",
        )
        .expect("create dm");
    engine
        .execute(
            &mut session,
            "INSERT INTO dm VALUES (1, 'A', 10.5000), (2, 'B', 20.2500), (3, 'A', 30.1250)",
        )
        .expect("insert dm");

    for stage in ["memtable", "flushed"] {
        let (schema, rows) = collect_rows(
            engine
                .execute(&mut session, "SELECT SUM(px), MIN(px), MAX(px) FROM dm")
                .expect("sum/min/max"),
        );
        assert_eq!(rows.len(), 1, "stage {}", stage);
        assert_eq!(
            rows[0][0],
            ScalarValue::Decimal(Some(608750)),
            "stage {}",
            stage
        );
        assert_eq!(
            rows[0][1],
            ScalarValue::Decimal(Some(105000)),
            "stage {}",
            stage
        );
        assert_eq!(
            rows[0][2],
            ScalarValue::Decimal(Some(301250)),
            "stage {}",
            stage
        );
        assert_eq!(schema.columns[0].scale, 4, "stage {} sum scale", stage);
        assert_eq!(schema.columns[1].scale, 4, "stage {} min scale", stage);
        assert_eq!(schema.columns[2].scale, 4, "stage {} max scale", stage);

        let (schema, rows) = collect_rows(
            engine
                .execute(
                    &mut session,
                    "SELECT sym, SUM(px) FROM dm GROUP BY sym ORDER BY sym",
                )
                .expect("grouped sum"),
        );
        assert_eq!(rows.len(), 2, "stage {}", stage);
        assert_eq!(
            rows[0][1],
            ScalarValue::Decimal(Some(406250)),
            "stage {}",
            stage
        );
        assert_eq!(
            rows[1][1],
            ScalarValue::Decimal(Some(202500)),
            "stage {}",
            stage
        );
        assert_eq!(
            schema.columns[1].scale, 4,
            "stage {} grouped sum scale",
            stage
        );

        if stage == "memtable" {
            engine
                .execute(&mut session, "FLUSH TABLE dm")
                .expect("flush dm");
        }
    }
    engine.segment_mgr().shutdown();
}

#[test]
fn count_variants_and_grouping() {
    let (_temp_dir, engine, mut session) = create_engine();
    engine
        .execute(
            &mut session,
            "CREATE TABLE tick (
                id INT,
                sym VARCHAR,
                px DECIMAL(18,4),
                ts TIMESTAMP
            ) CLUSTER BY (sym) HOT BY (ts) INDEX (sym)",
        )
        .expect("create tick");
    engine
        .execute(
            &mut session,
            "INSERT INTO tick VALUES
                (1, 'A', 10.5000, 1786000000000000),
                (2, 'B', 20.2500, 1786000000000001),
                (3, 'A', 30.1250, 1786000000000002)",
        )
        .expect("insert tick");

    assert_eq!(
        first_i64(
            engine
                .execute(&mut session, "SELECT COUNT(px) FROM tick")
                .unwrap(),
            0,
            0
        ),
        3,
        "count col unflushed"
    );
    assert_eq!(
        first_i64(
            engine
                .execute(&mut session, "SELECT COUNT(ts) FROM tick")
                .unwrap(),
            0,
            0
        ),
        3,
        "count ts unflushed"
    );
    assert_eq!(
        first_i64(
            engine
                .execute(&mut session, "SELECT COUNT(sym) FROM tick")
                .unwrap(),
            0,
            0
        ),
        3,
        "count sym unflushed"
    );
    assert_eq!(
        first_i64(
            engine
                .execute(&mut session, "SELECT COUNT(px) FROM tick WHERE sym = 'A'")
                .unwrap(),
            0,
            0
        ),
        2,
        "count col filtered"
    );
    assert_eq!(
        first_i64(
            engine
                .execute(&mut session, "SELECT COUNT(px + 1) FROM tick")
                .unwrap(),
            0,
            0
        ),
        3,
        "count expr"
    );

    let (_schema, rows) = collect_rows(
        engine
            .execute(
                &mut session,
                "SELECT sym, COUNT(*) FROM tick GROUP BY sym ORDER BY sym",
            )
            .expect("grouped count star"),
    );
    assert_eq!(rows.len(), 2, "grouped count star groups");
    assert_eq!(
        rows[0][0],
        ScalarValue::VarChar(Some("A".into())),
        "group key A"
    );
    assert_eq!(rows[0][1], ScalarValue::Int64(Some(2)), "group count A");
    assert_eq!(
        rows[1][0],
        ScalarValue::VarChar(Some("B".into())),
        "group key B"
    );
    assert_eq!(rows[1][1], ScalarValue::Int64(Some(1)), "group count B");

    let (_schema, rows) = collect_rows(
        engine
            .execute(
                &mut session,
                "SELECT sym, COUNT(px) FROM tick GROUP BY sym ORDER BY sym",
            )
            .expect("grouped count col"),
    );
    assert_eq!(rows.len(), 2, "grouped count col groups");
    assert_eq!(
        rows[0][0],
        ScalarValue::VarChar(Some("A".into())),
        "group key A col"
    );
    assert_eq!(rows[0][1], ScalarValue::Int64(Some(2)), "group count A col");
    assert_eq!(rows[1][1], ScalarValue::Int64(Some(1)), "group count B col");

    let (_schema, rows) = collect_rows(
        engine
            .execute(&mut session, "SELECT COUNT(*), COUNT(px) FROM tick")
            .expect("combined counts"),
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], ScalarValue::Int64(Some(3)), "combined star");
    assert_eq!(rows[0][1], ScalarValue::Int64(Some(3)), "combined col");

    engine
        .execute(&mut session, "FLUSH TABLE tick")
        .expect("flush tick");
    assert_eq!(
        first_i64(
            engine
                .execute(&mut session, "SELECT COUNT(px) FROM tick")
                .unwrap(),
            0,
            0
        ),
        3,
        "count col flushed"
    );
    let (_schema, rows) = collect_rows(
        engine
            .execute(
                &mut session,
                "SELECT sym, COUNT(px) FROM tick GROUP BY sym ORDER BY sym",
            )
            .expect("grouped count col flushed"),
    );
    assert_eq!(
        rows[0][1],
        ScalarValue::Int64(Some(2)),
        "group count A col flushed"
    );
    engine.segment_mgr().shutdown();
}

#[test]
fn count_star_across_flush_boundaries() {
    let (_temp_dir, engine, mut session) = create_engine();
    engine
        .execute(
            &mut session,
            "CREATE TABLE tick (
                id INT,
                sym VARCHAR,
                px DECIMAL(18,4),
                ts TIMESTAMP
            ) CLUSTER BY (sym) HOT BY (ts) INDEX (sym)",
        )
        .expect("create tick");

    engine
        .execute(
            &mut session,
            "INSERT INTO tick VALUES
                (1, 'A', 10.5000, 1786000000000000),
                (2, 'B', 20.2500, 1786000000000001),
                (3, 'A', 30.1250, 1786000000000002)",
        )
        .expect("insert tick");

    assert_eq!(
        first_i64(
            engine
                .execute(&mut session, "SELECT COUNT(*) FROM tick")
                .unwrap(),
            0,
            0
        ),
        3,
        "count unflushed"
    );

    assert_eq!(
        first_i64(
            engine
                .execute(&mut session, "SELECT COUNT(px) FROM tick")
                .unwrap(),
            0,
            0
        ),
        3,
        "count col unflushed"
    );

    engine
        .execute(&mut session, "FLUSH TABLE tick")
        .expect("flush tick");
    assert_eq!(
        first_i64(
            engine
                .execute(&mut session, "SELECT COUNT(*) FROM tick")
                .unwrap(),
            0,
            0
        ),
        3,
        "count after flush"
    );

    assert_eq!(
        first_i64(
            engine
                .execute(&mut session, "SELECT COUNT(px) FROM tick")
                .unwrap(),
            0,
            0
        ),
        3,
        "count col after flush"
    );

    engine
        .execute(
            &mut session,
            "INSERT INTO tick VALUES
                (4, 'A', 11.0000, 1786000000000003),
                (5, 'B', 21.0000, 1786000000000004)",
        )
        .expect("insert tick 2");
    assert_eq!(
        first_i64(
            engine
                .execute(&mut session, "SELECT COUNT(*) FROM tick")
                .unwrap(),
            0,
            0
        ),
        5,
        "count mixed"
    );

    assert_eq!(
        first_i64(
            engine
                .execute(&mut session, "SELECT COUNT(*) FROM tick WHERE sym = 'A'")
                .unwrap(),
            0,
            0
        ),
        3,
        "count filtered A"
    );
    assert_eq!(
        first_i64(
            engine
                .execute(&mut session, "SELECT COUNT(*) FROM tick WHERE sym = 'B'")
                .unwrap(),
            0,
            0
        ),
        2,
        "count filtered B"
    );
    assert_eq!(
        first_i64(
            engine
                .execute(&mut session, "SELECT COUNT(px) FROM tick")
                .unwrap(),
            0,
            0
        ),
        5,
        "count col"
    );
    engine.segment_mgr().shutdown();
}

#[test]
fn count_never_doubles_across_flush_transition() {
    use std::sync::Arc;

    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Arc::new(Engine::new(temp_dir.path(), "main").expect("failed to create engine"));
    let mut writer = Session::new("root".to_string(), "main".to_string());
    engine
        .execute(
            &mut writer,
            "CREATE TABLE tick (id INT, sym VARCHAR, px DECIMAL(18,4), ts TIMESTAMP) \
             CLUSTER BY (sym) HOT BY (ts) INDEX (sym)",
        )
        .expect("create tick");

    struct FlushDelayReset(Arc<Engine>);
    impl Drop for FlushDelayReset {
        fn drop(&mut self) {
            self.0.segment_mgr().set_flush_transition_delay_for_tests(0);
        }
    }
    engine
        .segment_mgr()
        .set_flush_transition_delay_for_tests(120);
    let _delay_reset = FlushDelayReset(Arc::clone(&engine));

    const ROUNDS: usize = 3;
    const BATCHES: usize = 8;
    const ROWS_PER_BATCH: usize = 3000;

    let mut inserted = 0usize;
    for _round in 0..ROUNDS {
        for _batch in 0..BATCHES {
            let values = (0..ROWS_PER_BATCH)
                .map(|i| {
                    let id = inserted + i + 1;
                    let sym = if id.is_multiple_of(2) { "AAA" } else { "BBB" };
                    format!(
                        "({}, '{}', {}.{}000, {})",
                        id,
                        sym,
                        10 + (id % 90),
                        id % 10,
                        1_786_000_000_000_000u64 + id as u64
                    )
                })
                .collect::<Vec<_>>()
                .join(",\n");
            engine
                .execute(&mut writer, &format!("INSERT INTO tick VALUES\n{}", values))
                .expect("insert tick");
            inserted += ROWS_PER_BATCH;
        }

        let flusher_engine = Arc::clone(&engine);
        let flusher = std::thread::spawn(move || {
            let mut s = Session::new("root".to_string(), "main".to_string());
            flusher_engine
                .execute(&mut s, "FLUSH TABLE tick")
                .expect("flush tick");
        });

        let mut worst_total = 0i64;
        let mut worst_filtered = 0i64;
        let mut iterations = 0usize;
        let mut reader = Session::new("root".to_string(), "main".to_string());
        while !flusher.is_finished() {
            iterations += 1;
            if let Ok(value) = engine
                .execute(&mut reader, "SELECT COUNT(*) FROM tick")
                .map(|r| first_i64(r, 0, 0))
            {
                worst_total = worst_total.max(value);
            }
            if let Ok(value) = engine
                .execute(&mut reader, "SELECT COUNT(*) FROM tick WHERE sym = 'AAA'")
                .map(|r| first_i64(r, 0, 0))
            {
                worst_filtered = worst_filtered.max(value);
            }
        }
        flusher.join().expect("flusher thread panicked");
        println!(
            "round: iterations={} worst_total={} worst_filtered={} expected={}",
            iterations, worst_total, worst_filtered, inserted
        );
        assert_eq!(
            worst_total, inserted as i64,
            "COUNT(*) saw {} while inserting {} rows",
            worst_total, inserted
        );
        assert_eq!(
            worst_filtered,
            (inserted / 2) as i64,
            "filtered COUNT(*) saw {} while inserting {} rows",
            worst_filtered,
            inserted
        );
    }
    engine.segment_mgr().shutdown();
}

#[test]
fn count_star_survives_engine_reopen() {
    let temp_dir = TempDir::new().expect("temp dir");
    {
        let engine = Engine::new(temp_dir.path(), "main").expect("engine");
        let mut session = Session::new("root".to_string(), "main".to_string());
        engine
            .execute(
                &mut session,
                "CREATE TABLE tick (
                    id INT,
                    sym VARCHAR,
                    px DECIMAL(18,4),
                    ts TIMESTAMP
                ) CLUSTER BY (sym) HOT BY (ts) INDEX (sym)",
            )
            .expect("create tick");
        engine
            .execute(
                &mut session,
                "INSERT INTO tick VALUES
                    (1, 'A', 10.5000, 1786000000000000),
                    (2, 'B', 20.2500, 1786000000000001)",
            )
            .expect("insert tick");
        engine
            .execute(&mut session, "FLUSH TABLE tick")
            .expect("flush tick");
        engine
            .execute(
                &mut session,
                "INSERT INTO tick VALUES (3, 'A', 30.1250, 1786000000000002)",
            )
            .expect("insert tick 2");
        engine.segment_mgr().shutdown();
    }
    {
        let engine = Engine::new(temp_dir.path(), "main").expect("reopen engine");
        let mut session = Session::new("root".to_string(), "main".to_string());
        assert_eq!(
            first_i64(
                engine
                    .execute(&mut session, "SELECT COUNT(*) FROM tick")
                    .unwrap(),
                0,
                0
            ),
            3,
            "count after reopen"
        );
        assert_eq!(
            first_i64(
                engine
                    .execute(&mut session, "SELECT COUNT(*) FROM tick WHERE sym = 'A'")
                    .unwrap(),
                0,
                0
            ),
            2,
            "count filtered after reopen"
        );
        engine.segment_mgr().shutdown();
    }
}

#[test]
fn min_max_bool_column_folds_correctly() {
    // Sanity for MIN/MAX over a Bool column across the flush boundary. The
    // compare primitive itself is locked by compare_ordered unit tests and
    // the spilled_min_max_merge_orders_bool_states unit test (the spill path
    // is where the former missing Bool arm actually bit).
    let (_temp_dir, engine, mut session) = create_engine();
    engine
        .execute(&mut session, "CREATE TABLE flags (id INT, on_off BOOLEAN)")
        .expect("create flags");
    engine
        .execute(
            &mut session,
            "INSERT INTO flags VALUES (2, true), (1, false), (3, true)",
        )
        .expect("insert flags");

    for stage in ["memtable", "flushed"] {
        if stage == "flushed" {
            engine
                .execute(&mut session, "FLUSH TABLE flags")
                .expect("flush");
        }
        let (_schema, rows) = collect_rows(
            engine
                .execute(&mut session, "SELECT MIN(on_off), MAX(on_off) FROM flags")
                .expect("select min/max"),
        );
        assert_eq!(rows.len(), 1, "{stage}");
        assert_eq!(rows[0][0], ScalarValue::Bool(Some(false)), "{stage}: MIN");
        assert_eq!(rows[0][1], ScalarValue::Bool(Some(true)), "{stage}: MAX");
    }
}

#[test]
fn decimal_multiplication_rescales_when_result_scale_is_capped() {
    let (_temp_dir, engine, mut session) = create_engine();
    engine
        .execute(
            &mut session,
            "CREATE TABLE decimal_products (id INT, a DECIMAL(18,10), b DECIMAL(18,10))",
        )
        .unwrap();
    engine.execute(&mut session,
        "INSERT INTO decimal_products VALUES (1, 0.1, 0.1), (2, 1.0, 1.0), (3, -0.1, 0.1), (4, NULL, 1.0)"
    ).unwrap();
    for flushed in [false, true] {
        if flushed {
            engine
                .execute(&mut session, "FLUSH TABLE decimal_products")
                .unwrap();
        }
        let (schema, rows) = collect_rows(
            engine
                .execute(
                    &mut session,
                    "SELECT a * b FROM decimal_products ORDER BY id",
                )
                .unwrap(),
        );
        assert_eq!(schema.columns[0].scale, 18);
        assert_eq!(
            rows,
            vec![
                vec![ScalarValue::Decimal(Some(10_000_000_000_000_000))],
                vec![ScalarValue::Decimal(Some(1_000_000_000_000_000_000))],
                vec![ScalarValue::Decimal(Some(-10_000_000_000_000_000))],
                vec![ScalarValue::Decimal(None)],
            ]
        );
    }
    engine.segment_mgr().shutdown();
}
