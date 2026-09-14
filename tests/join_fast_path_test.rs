//! Regression tests for the join fast probe path (`emit_fast_chunk`): an
//! output chunk must stay inside one build batch even when a single probe
//! row's matches span several build batches.

use tempfile::TempDir;

use taibai::net::session::Session;
use taibai::sql::execution::engine::{Engine, ExecResult};
use taibai::types::scalar::ScalarValue;

fn query_rows(engine: &Engine, session: &mut Session, sql: &str) -> Vec<Vec<ScalarValue>> {
    let ExecResult::Query { batches, .. } = engine
        .execute(session, sql)
        .unwrap_or_else(|error| panic!("query failed: {sql}\n{error}"))
    else {
        panic!("expected query result for: {sql}");
    };
    let mut rows = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            rows.push(
                (0..batch.num_columns())
                    .map(|c| batch.scalar(row, c).expect("column value"))
                    .collect(),
            );
        }
    }
    rows
}

#[test]
fn fast_probe_matches_spanning_build_batches() {
    let temp_dir = TempDir::new().expect("temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("engine");
    let mut session = Session::new("root".to_string(), "main".to_string());

    engine
        .execute(&mut session, "CREATE TABLE probe (k INT64)")
        .expect("create probe");
    engine
        .execute(&mut session, "INSERT INTO probe VALUES (1)")
        .expect("insert probe");
    engine
        .execute(&mut session, "CREATE TABLE build_t (k INT64, v VARCHAR)")
        .expect("create build_t");
    // Two separate statements: the build side receives two batches, and the
    // probe row's matches span both. Gathering every match from the first
    // batch would duplicate 'first' instead of returning both values.
    engine
        .execute(&mut session, "INSERT INTO build_t VALUES (1, 'first')")
        .expect("insert build first");
    engine
        .execute(&mut session, "INSERT INTO build_t VALUES (1, 'second')")
        .expect("insert build second");

    let rows = query_rows(
        &engine,
        &mut session,
        "SELECT b.v FROM probe p JOIN build_t b ON p.k = b.k ORDER BY b.v",
    );
    assert_eq!(rows.len(), 2, "both build rows must match: {rows:?}");
    assert_eq!(rows[0][0], ScalarValue::VarChar(Some("first".into())));
    assert_eq!(rows[1][0], ScalarValue::VarChar(Some("second".into())));
}
