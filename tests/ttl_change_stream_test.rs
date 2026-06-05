use tempfile::TempDir;

use monax::net::session::Session;
use monax::sql::execution::engine::{Engine, ExecResult};
use monax::storage::change_stream::ChangeOperation;
use monax::types::scalar::ScalarValue;

fn create_engine() -> (TempDir, Engine, Session) {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("failed to create engine");
    let session = Session::new("root".to_string(), "main".to_string());
    (temp_dir, engine, session)
}

fn now_micros() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as i64
}

fn collect_i32(result: ExecResult) -> Vec<i32> {
    let ExecResult::Query { batches, .. } = result else {
        panic!("expected query result");
    };
    let mut values = Vec::new();
    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            match batch.scalar(row_idx, 0) {
                Some(ScalarValue::Int32(Some(v))) => values.push(v),
                other => panic!("expected int32, got {:?}", other),
            }
        }
    }
    values
}

fn count_value(result: ExecResult) -> i64 {
    let ExecResult::Query { batches, .. } = result else {
        panic!("expected query result");
    };
    match batches[0].scalar(0, 0) {
        Some(ScalarValue::Int64(Some(v))) => v,
        other => panic!("expected count, got {:?}", other),
    }
}

#[test]
fn ttl_filters_expired_rows_from_segments_and_count_star() {
    let (_temp_dir, engine, mut session) = create_engine();
    engine
        .execute(
            &mut session,
            "CREATE TABLE t (id INT32, created_at TIMESTAMP) TTL (created_at, 60)",
        )
        .expect("create table");

    let expired = now_micros() - 120_000_000;
    let fresh = now_micros();
    engine
        .execute(
            &mut session,
            &format!("INSERT INTO t VALUES (1, {}), (2, {})", expired, fresh),
        )
        .expect("insert rows");
    engine
        .execute(&mut session, "FLUSH TABLE t")
        .expect("flush rows");

    let ids = collect_i32(
        engine
            .execute(&mut session, "SELECT id FROM t")
            .expect("select ids"),
    );
    assert_eq!(ids, vec![2]);

    let count = count_value(
        engine
            .execute(&mut session, "SELECT COUNT(*) FROM t")
            .expect("count rows"),
    );
    assert_eq!(count, 1);
    engine.segment_mgr().shutdown();
}

#[test]
fn alter_table_can_set_and_drop_ttl() {
    let (_temp_dir, engine, mut session) = create_engine();
    engine
        .execute(&mut session, "CREATE TABLE t (id INT32, ts TIMESTAMP)")
        .expect("create table");
    engine
        .execute(&mut session, "ALTER TABLE t SET TTL (ts, 60)")
        .expect("set ttl");
    assert!(engine
        .get_table_meta("main", "t")
        .expect("table meta")
        .ttl
        .is_some());

    engine
        .execute(&mut session, "ALTER TABLE t DROP TTL")
        .expect("drop ttl");
    assert!(engine
        .get_table_meta("main", "t")
        .expect("table meta")
        .ttl
        .is_none());
    engine.segment_mgr().shutdown();
}

#[test]
fn change_stream_records_successful_dml_events() {
    let (_temp_dir, engine, mut session) = create_engine();
    engine
        .execute(&mut session, "CREATE TABLE t (id INT32, value INT32)")
        .expect("create table");
    let mut rx = engine.subscribe_changes();

    engine
        .execute(&mut session, "INSERT INTO t VALUES (1, 10)")
        .expect("insert");
    let event = rx.try_recv().expect("insert event");
    assert_eq!(event.operation, ChangeOperation::Insert);
    assert_eq!(event.database, "main");
    assert_eq!(event.table, "t");
    assert_eq!(event.rows_affected, 1);

    engine
        .execute(&mut session, "UPDATE t SET value = 20 WHERE id = 1")
        .expect("update");
    engine
        .execute(&mut session, "DELETE FROM t WHERE id = 1")
        .expect("delete");

    let recent = engine.recent_changes(Some("main"), Some("t"));
    assert_eq!(recent.len(), 3);
    assert_eq!(recent[0].operation, ChangeOperation::Insert);
    assert_eq!(recent[1].operation, ChangeOperation::Update);
    assert_eq!(recent[2].operation, ChangeOperation::Delete);
    engine.segment_mgr().shutdown();
}

#[test]
fn change_stream_publishes_transaction_insert_on_commit_only() {
    let (_temp_dir, engine, mut session) = create_engine();
    engine
        .execute(&mut session, "CREATE TABLE t (id INT32)")
        .expect("create table");
    let mut rx = engine.subscribe_changes();

    engine.execute(&mut session, "BEGIN").expect("begin");
    engine
        .execute(&mut session, "INSERT INTO t VALUES (1)")
        .expect("insert");
    assert!(rx.try_recv().is_err());

    engine.execute(&mut session, "COMMIT").expect("commit");
    let event = rx.try_recv().expect("commit event");
    assert_eq!(event.operation, ChangeOperation::Insert);
    assert_eq!(event.rows_affected, 1);
    engine.segment_mgr().shutdown();
}
