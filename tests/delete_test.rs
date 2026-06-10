use tempfile::TempDir;

use taibai::net::session::Session;
use taibai::sql::execution::engine::{Engine, ExecResult};

fn create_engine() -> (TempDir, Engine, Session) {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("failed to create engine");
    let session = Session::new("root".to_string(), "main".to_string());
    (temp_dir, engine, session)
}

fn row_count(result: ExecResult) -> usize {
    let ExecResult::Query { batches, .. } = result else {
        panic!("expected query result");
    };
    batches.iter().map(|batch| batch.num_rows()).sum()
}

#[test]
fn delete_where_marks_matching_rows_only() {
    let (_temp_dir, engine, mut session) = create_engine();
    engine
        .execute(
            &mut session,
            "CREATE TABLE t (id INT32, value INT32, payload VARCHAR)",
        )
        .expect("create table");
    engine
        .execute(
            &mut session,
            "INSERT INTO t VALUES (1, 10, 'a'), (2, 20, 'b'), (3, 20, 'c')",
        )
        .expect("insert rows");
    engine
        .execute(&mut session, "FLUSH TABLE t")
        .expect("flush rows");

    let deleted = engine
        .execute(&mut session, "DELETE FROM t WHERE value = 20")
        .expect("delete rows");
    match deleted {
        ExecResult::Ok { rows_affected } => assert_eq!(rows_affected, 2),
        other => panic!("expected ok, got {:?}", other),
    }

    let remaining = engine
        .execute(&mut session, "SELECT id FROM t")
        .expect("select remaining rows");
    assert_eq!(row_count(remaining), 1);
    engine.segment_mgr().shutdown();
}

#[test]
fn delete_without_where_marks_rows_without_reading_predicate_columns() {
    let (_temp_dir, engine, mut session) = create_engine();
    engine
        .execute(&mut session, "CREATE TABLE t (id INT32, value INT32)")
        .expect("create table");
    engine
        .execute(&mut session, "INSERT INTO t VALUES (1, 10), (2, 20)")
        .expect("insert rows");
    engine
        .execute(&mut session, "FLUSH TABLE t")
        .expect("flush rows");

    let deleted = engine
        .execute(&mut session, "DELETE FROM t")
        .expect("delete all rows");
    match deleted {
        ExecResult::Ok { rows_affected } => assert_eq!(rows_affected, 2),
        other => panic!("expected ok, got {:?}", other),
    }

    let remaining = engine
        .execute(&mut session, "SELECT id FROM t")
        .expect("select remaining rows");
    assert_eq!(row_count(remaining), 0);
    engine.segment_mgr().shutdown();
}
