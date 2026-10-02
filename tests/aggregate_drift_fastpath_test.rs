//! Aggregate metadata fast path across schema drift (ALTER ADD / RENAME
//! COLUMN).
//!
//! A segment written before an ALTER keeps the pre-DDL footer layout. The
//! metadata fast path used to decline wholesale on any drift, permanently
//! degrading SUM/COUNT/MIN/MAX to full scans until compaction rewrote the
//! segment; it now resolves the query's aggregate columns by name and folds
//! constant DEFAULTs arithmetically. These tests pin the observable
//! contract: results stay exact on every path — the fast path itself (plus
//! the in-crate unit tests asserting `compute_simple_aggregate` no longer
//! returns `None` for drifted footers) and the fallback shapes that still
//! decline (predicates on the added column, CURRENT_TIMESTAMP defaults).

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

fn exec(engine: &Engine, session: &mut Session, sql: &str) -> ExecResult {
    match engine.execute(session, sql) {
        Ok(result) => result,
        Err(error) => panic!("'{sql}' failed: {error}"),
    }
}

fn scalars(engine: &Engine, session: &mut Session, sql: &str) -> Vec<ScalarValue> {
    let result = exec(engine, session, sql);
    let ExecResult::Query { batches, .. } = result else {
        panic!("'{sql}' expected query result");
    };
    let mut values = Vec::new();
    for batch in &batches {
        for row_idx in 0..batch.num_rows() {
            for col_idx in 0..batch.num_columns() {
                values.push(
                    batch
                        .scalar(row_idx, col_idx)
                        .unwrap_or(ScalarValue::Int64(None)),
                );
            }
        }
    }
    assert_eq!(values.len(), 1, "'{sql}' must return a single scalar");
    values
}

fn i64_value(engine: &Engine, session: &mut Session, sql: &str) -> i64 {
    match scalars(engine, session, sql).remove(0) {
        ScalarValue::Int8(Some(value)) => value as i64,
        ScalarValue::Int16(Some(value)) => value as i64,
        ScalarValue::Int32(Some(value)) => value as i64,
        ScalarValue::Int64(Some(value)) => value,
        ScalarValue::UInt8(Some(value)) => value as i64,
        ScalarValue::UInt16(Some(value)) => value as i64,
        ScalarValue::UInt32(Some(value)) => value as i64,
        other => panic!("'{sql}' expected an integer, got {other:?}"),
    }
}

/// Whether the scalar is the NULL of an integer column (INT folds come
/// back as Int32, INT64 as Int64).
fn is_null_integer(value: &ScalarValue) -> bool {
    matches!(
        value,
        ScalarValue::Int8(None)
            | ScalarValue::Int16(None)
            | ScalarValue::Int32(None)
            | ScalarValue::Int64(None)
    )
}

/// ADD COLUMN with a constant DEFAULT: the drifted (pre-ALTER) segment
/// keeps the metadata fast path — SUM(new_col) folds DEFAULT x rows,
/// COUNT/MIN/MAX fold the default, and post-ALTER resident rows (a
/// non-default value) still merge in through the memtable fold.
#[test]
fn aggregates_over_added_constant_default_column_stay_exact() {
    let (_temp_dir, engine, mut session) = create_engine();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE t (id INT PRIMARY KEY, v INT)",
    );
    let mut sql = String::from("INSERT INTO t (id, v) VALUES ");
    for i in 0..50i64 {
        if i > 0 {
            sql.push_str(", ");
        }
        sql.push_str(&format!("({}, {})", i + 1, i + 1));
    }
    exec(&engine, &mut session, &sql);
    exec(&engine, &mut session, "FLUSH TABLE t");
    exec(
        &engine,
        &mut session,
        "ALTER TABLE t ADD COLUMN score INT DEFAULT 7",
    );
    // A resident post-ALTER row with a non-default value.
    exec(
        &engine,
        &mut session,
        "INSERT INTO t (id, v, score) VALUES (51, 51, 30)",
    );

    assert_eq!(i64_value(&engine, &mut session, "SELECT SUM(score) FROM t"), 380);
    assert_eq!(
        i64_value(&engine, &mut session, "SELECT COUNT(score) FROM t"),
        51
    );
    assert_eq!(i64_value(&engine, &mut session, "SELECT MIN(score) FROM t"), 7);
    assert_eq!(i64_value(&engine, &mut session, "SELECT MAX(score) FROM t"), 30);
    match scalars(&engine, &mut session, "SELECT AVG(score) FROM t").remove(0) {
        ScalarValue::Float64(Some(value)) => {
            assert!((value - 380.0 / 51.0).abs() < 1e-9, "got {value}");
        }
        other => panic!("expected Float64 AVG, got {other:?}"),
    }
    // The pre-existing column keeps folding from the drifted footer.
    assert_eq!(i64_value(&engine, &mut session, "SELECT SUM(v) FROM t"), 1326);
    engine.segment_mgr().shutdown();
}

/// Predicate on a PRE-EXISTING column + aggregate on the added column: the
/// fast path stays (the predicate column resolves by name) and the default
/// column folds per surviving row of the masked batch read.
#[test]
fn sum_of_added_column_with_predicate_on_old_column() {
    let (_temp_dir, engine, mut session) = create_engine();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE t (id INT PRIMARY KEY, v INT)",
    );
    let mut sql = String::from("INSERT INTO t (id, v) VALUES ");
    for i in 0..50i64 {
        if i > 0 {
            sql.push_str(", ");
        }
        sql.push_str(&format!("({}, {})", i + 1, i + 1));
    }
    exec(&engine, &mut session, &sql);
    exec(&engine, &mut session, "FLUSH TABLE t");
    exec(
        &engine,
        &mut session,
        "ALTER TABLE t ADD COLUMN score INT DEFAULT 7",
    );

    // Rows 26..=50 survive (v > 25): 25 default rows contribute 7 x 25.
    assert_eq!(
        i64_value(&engine, &mut session, "SELECT SUM(score) FROM t WHERE v > 25"),
        175
    );
    assert_eq!(
        i64_value(&engine, &mut session, "SELECT COUNT(score) FROM t WHERE v > 25"),
        25
    );
    assert_eq!(
        i64_value(
            &engine,
            &mut session,
            "SELECT SUM(score) FROM t WHERE v > 25 AND v < 30"
        ),
        28
    );
    engine.segment_mgr().shutdown();
}

/// Predicates on the ADDED column still take the fallback (default-filled
/// evaluation in the regular pipeline) — the results must match the
/// default-fill semantics exactly.
#[test]
fn predicate_on_added_column_falls_back_but_stays_correct() {
    let (_temp_dir, engine, mut session) = create_engine();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE t (id INT PRIMARY KEY, v INT)",
    );
    let mut sql = String::from("INSERT INTO t (id, v) VALUES ");
    for i in 0..50i64 {
        if i > 0 {
            sql.push_str(", ");
        }
        sql.push_str(&format!("({}, {})", i + 1, i + 1));
    }
    exec(&engine, &mut session, &sql);
    exec(&engine, &mut session, "FLUSH TABLE t");
    exec(
        &engine,
        &mut session,
        "ALTER TABLE t ADD COLUMN score INT DEFAULT 7",
    );

    assert_eq!(
        i64_value(&engine, &mut session, "SELECT SUM(score) FROM t WHERE score = 7"),
        350
    );
    let empty_sum =
        scalars(&engine, &mut session, "SELECT SUM(score) FROM t WHERE score = 99").remove(0);
    assert!(
        is_null_integer(&empty_sum),
        "SUM over zero matching rows is NULL, got {empty_sum:?}"
    );
    assert_eq!(
        i64_value(
            &engine,
            &mut session,
            "SELECT COUNT(*) FROM t WHERE score = 7"
        ),
        50
    );
    engine.segment_mgr().shutdown();
}

/// ADD COLUMN without a DEFAULT: drifted rows carry NULL — SUM/MIN/MAX see
/// an all-NULL column (NULL result), COUNT(col) counts only the rows that
/// materialized after the ALTER.
#[test]
fn added_column_without_default_folds_null_semantics() {
    let (_temp_dir, engine, mut session) = create_engine();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE t (id INT PRIMARY KEY, v INT)",
    );
    let mut sql = String::from("INSERT INTO t (id, v) VALUES ");
    for i in 0..10i64 {
        if i > 0 {
            sql.push_str(", ");
        }
        sql.push_str(&format!("({}, {})", i + 1, i + 1));
    }
    exec(&engine, &mut session, &sql);
    exec(&engine, &mut session, "FLUSH TABLE t");
    exec(&engine, &mut session, "ALTER TABLE t ADD COLUMN score INT");

    match scalars(&engine, &mut session, "SELECT SUM(score) FROM t").remove(0) {
        value if is_null_integer(&value) => {}
        other => panic!("SUM over an all-NULL drifted column must be NULL, got {other:?}"),
    }
    match scalars(&engine, &mut session, "SELECT MIN(score) FROM t").remove(0) {
        value if is_null_integer(&value) => {}
        other => panic!("MIN over an all-NULL drifted column must be NULL, got {other:?}"),
    }
    assert_eq!(
        i64_value(&engine, &mut session, "SELECT COUNT(score) FROM t"),
        0,
        "drifted rows carry NULL and do not count"
    );

    // Post-ALTER rows materialize real values and fold in.
    exec(
        &engine,
        &mut session,
        "INSERT INTO t (id, v, score) VALUES (11, 11, 5), (12, 12, 9)",
    );
    assert_eq!(i64_value(&engine, &mut session, "SELECT SUM(score) FROM t"), 14);
    assert_eq!(i64_value(&engine, &mut session, "SELECT COUNT(score) FROM t"), 2);
    assert_eq!(i64_value(&engine, &mut session, "SELECT MIN(score) FROM t"), 5);
    assert_eq!(i64_value(&engine, &mut session, "SELECT MAX(score) FROM t"), 9);
    engine.segment_mgr().shutdown();
}

/// DEFAULT CURRENT_TIMESTAMP is not constant across writes: those segments
/// keep declining the fast path, and the fallback reads the ALTER-pinned
/// backfill — identical values on repeated queries.
#[test]
fn current_timestamp_default_stays_stable_through_the_fallback() {
    let (_temp_dir, engine, mut session) = create_engine();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE t (id INT PRIMARY KEY, v INT)",
    );
    exec(
        &engine,
        &mut session,
        "INSERT INTO t (id, v) VALUES (1, 10), (2, 20)",
    );
    exec(&engine, &mut session, "FLUSH TABLE t");
    exec(
        &engine,
        &mut session,
        "ALTER TABLE t ADD COLUMN created TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
    );

    let first = scalars(&engine, &mut session, "SELECT MIN(created) FROM t").remove(0);
    let second = scalars(&engine, &mut session, "SELECT MIN(created) FROM t").remove(0);
    assert_eq!(
        first, second,
        "backfilled CURRENT_TIMESTAMP must be pinned at ALTER time"
    );
    engine.segment_mgr().shutdown();
}

/// RENAME COLUMN drift: the fast path resolves the aggregate column
/// through previous_names and keeps folding the footer statistics.
#[test]
fn aggregates_follow_renamed_column_on_drifted_segments() {
    let (_temp_dir, engine, mut session) = create_engine();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE t (id INT PRIMARY KEY, v INT)",
    );
    let mut sql = String::from("INSERT INTO t (id, v) VALUES ");
    for i in 0..50i64 {
        if i > 0 {
            sql.push_str(", ");
        }
        sql.push_str(&format!("({}, {})", i + 1, i + 1));
    }
    exec(&engine, &mut session, &sql);
    exec(&engine, &mut session, "FLUSH TABLE t");
    exec(
        &engine,
        &mut session,
        "ALTER TABLE t RENAME COLUMN v TO value",
    );

    assert_eq!(i64_value(&engine, &mut session, "SELECT SUM(value) FROM t"), 1275);
    assert_eq!(i64_value(&engine, &mut session, "SELECT MIN(value) FROM t"), 1);
    assert_eq!(i64_value(&engine, &mut session, "SELECT MAX(value) FROM t"), 50);
    assert_eq!(
        i64_value(&engine, &mut session, "SELECT COUNT(value) FROM t"),
        50
    );
    engine.segment_mgr().shutdown();
}

/// DROP COLUMN drift: the footer keeps extra columns the query never
/// mentions; name resolution ignores them and the fold stays exact.
#[test]
fn aggregates_after_drop_column_ignore_extra_footer_columns() {
    let (_temp_dir, engine, mut session) = create_engine();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)",
    );
    let mut sql = String::from("INSERT INTO t (id, a, b) VALUES ");
    for i in 0..40i64 {
        if i > 0 {
            sql.push_str(", ");
        }
        sql.push_str(&format!("({}, 5, {})", i + 1, i + 1));
    }
    exec(&engine, &mut session, &sql);
    exec(&engine, &mut session, "FLUSH TABLE t");
    exec(&engine, &mut session, "ALTER TABLE t DROP COLUMN a");

    assert_eq!(i64_value(&engine, &mut session, "SELECT SUM(b) FROM t"), 820);
    assert_eq!(i64_value(&engine, &mut session, "SELECT COUNT(b) FROM t"), 40);
    assert_eq!(i64_value(&engine, &mut session, "SELECT MIN(b) FROM t"), 1);
    assert_eq!(i64_value(&engine, &mut session, "SELECT MAX(b) FROM t"), 40);
    engine.segment_mgr().shutdown();
}
