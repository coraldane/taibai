use tempfile::TempDir;

use taibai::net::session::Session;
use taibai::sql::execution::engine::{Engine, ExecResult};
use taibai::types::scalar::ScalarValue;

/// Collect the `ts` (INT32) values returned by a query as a multiset (Vec),
/// preserving duplicates so that double-counting regressions are caught.
fn collect_ts_values(result: ExecResult) -> Vec<i32> {
    let ExecResult::Query { batches, .. } = result else {
        panic!("expected query result, got {:?}", result);
    };
    let mut out = Vec::new();
    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            match batch.scalar(row_idx, 0) {
                Some(ScalarValue::Int32(Some(v))) => out.push(v),
                other => panic!("expected int32 ts, got {:?}", other),
            }
        }
    }
    out.sort();
    out
}

/// End-to-end check that the hot-store fast path stays correct across a flush.
///
/// The hot store mirrors auto-commit inserts but is never cleared on flush, so
/// once rows are persisted to a segment they would be served twice (once by the
/// segment scan, once by the hot gather) unless the gather applies a watermark
/// that excludes already-flushed rows. This test pins that behaviour: after a
/// flush and a fresh insert, a point lookup on the leading cluster key returns
/// exactly the union of segment + live rows, with no duplication.
#[test]
fn hot_fast_path_does_not_double_count_after_flush() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("failed to create engine");
    let mut session = Session::new("root".to_string(), "main".to_string());

    // Single active partition (all rows share deal_date) so flushed rows form a
    // contiguous append-prefix of the hot store -- the watermark invariant.
    engine
        .execute(
            &mut session,
            "CREATE TABLE ticks (
                sym VARCHAR NOT NULL,
                deal_date INT32 NOT NULL,
                trade_time TIMESTAMP NOT NULL,
                ts INT32 NOT NULL
            ) PARTITION BY deal_date CLUSTER BY (sym, deal_date) HOT BY (trade_time) INDEX (sym)",
        )
        .expect("create ticks");

    // Auto-commit inserts feed the hot store.
    engine
        .execute(
            &mut session,
            "INSERT INTO ticks VALUES ('A', 20260108, 1782718238000000, 1), ('A', 20260108, 1782718238000001, 2), ('B', 20260108, 1782718238000002, 1)",
        )
        .expect("insert initial rows");

    // Fast path: live memtable data only, watermark == 0.
    let rows = collect_ts_values(
        engine
            .execute(&mut session, "SELECT ts FROM ticks WHERE sym = 'A'")
            .expect("select before flush"),
    );
    assert_eq!(rows, vec![1, 2]);

    // Persist the initial rows to a segment; the hot store is NOT cleared.
    engine
        .execute(&mut session, "FLUSH TABLE ticks")
        .expect("flush initial rows");

    // After flush there is no live memtable data, so the fast path is skipped
    // and the segment scan serves the query alone.
    let rows = collect_ts_values(
        engine
            .execute(&mut session, "SELECT ts FROM ticks WHERE sym = 'A'")
            .expect("select after flush"),
    );
    assert_eq!(rows, vec![1, 2]);

    // A fresh auto-commit insert creates a new live memtable while the hot store
    // still holds the flushed prefix. Without the watermark the gather would
    // return A@1, A@2 (stale) plus A@3 (live), and the segment scan would also
    // return A@1, A@2 -- duplicating them into [1, 1, 2, 2, 3].
    engine
        .execute(
            &mut session,
            "INSERT INTO ticks VALUES ('A', 20260108, 1782718238000003, 3)",
        )
        .expect("insert live row after flush");

    let rows = collect_ts_values(
        engine
            .execute(&mut session, "SELECT ts FROM ticks WHERE sym = 'A'")
            .expect("select after post-flush insert"),
    );
    assert_eq!(
        rows,
        vec![1, 2, 3],
        "post-flush point lookup must union segment + live rows without duplication"
    );

    engine.segment_mgr().shutdown();
}

#[test]
fn hot_clause_indexes_named_column_and_echoes() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("failed to create engine");
    let mut session = Session::new("root".to_string(), "main".to_string());

    engine
        .execute(
            &mut session,
            "CREATE TABLE tick_hot (
                security_id VARCHAR NOT NULL,
                trade_time TIMESTAMP,
                price INT32
            ) CLUSTER BY (trade_time) HOT BY (trade_time) INDEX (security_id) CLOSE AT '15:30'",
        )
        .expect("create with HOT clause");

    let result = engine
        .execute(&mut session, "SHOW CREATE TABLE tick_hot")
        .expect("show create");
    let ExecResult::Query { batches, .. } = result else {
        panic!("expected query result");
    };
    let mut create_sql = String::new();
    for batch in &batches {
        for r in 0..batch.num_rows() {
            if let Some(ScalarValue::VarChar(Some(s))) = batch.scalar(r, 1) {
                create_sql.push_str(&s);
            }
        }
    }
    assert!(
        create_sql.contains("HOT BY (trade_time) INDEX (security_id) CLOSE AT '15:30'"),
        "SHOW CREATE TABLE must echo HOT clause, got: {}",
        create_sql
    );

    engine
        .execute(
            &mut session,
            "INSERT INTO tick_hot (security_id, trade_time, price) VALUES \
             ('A', 1782718238000000, 10), \
             ('B', 1782718238000001, 20), \
             ('A', 1782718238000002, 30)",
        )
        .expect("insert rows");

    let rows = collect_ts_values(
        engine
            .execute(&mut session, "SELECT price FROM tick_hot WHERE security_id = 'A'")
            .expect("select by hot index column"),
    );
    assert_eq!(rows, vec![10, 30]);

    engine.segment_mgr().shutdown();
}

#[test]
fn hot_clause_rejects_invalid_close_time() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("failed to create engine");
    let mut session = Session::new("root".to_string(), "main".to_string());

    let err = engine
        .execute(
            &mut session,
            "CREATE TABLE bad_close (security_id VARCHAR, trade_time TIMESTAMP) \
             HOT BY (trade_time) INDEX (security_id) CLOSE AT '25:99'",
        )
        .expect_err("invalid HOT CLOSE AT time should be rejected");
    let msg = format!("{err}");
    assert!(
        msg.contains("HH:MM"),
        "expected close time validation error, got: {msg}"
    );

    engine.segment_mgr().shutdown();
}

#[test]
fn hot_clause_requires_timestamp_time_column() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("failed to create engine");
    let mut session = Session::new("root".to_string(), "main".to_string());

    let err = engine
        .execute(
            &mut session,
            "CREATE TABLE bad_tick (security_id VARCHAR, trade_time VARCHAR) \
             HOT BY (trade_time) INDEX (security_id)",
        )
        .expect_err("non-timestamp HOT column should be rejected");
    let msg = format!("{err}");
    assert!(
        msg.contains("must be TIMESTAMP"),
        "expected timestamp validation error, got: {msg}"
    );

    engine.segment_mgr().shutdown();
}
