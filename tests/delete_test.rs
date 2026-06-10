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

fn count_star(result: ExecResult) -> i64 {
    let ExecResult::Query { batches, .. } = result else {
        panic!("expected query result");
    };
    batches
        .first()
        .and_then(|batch| batch.scalar(0, 0))
        .and_then(|value| match value {
            taibai::types::scalar::ScalarValue::Int64(Some(v)) => Some(v),
            _ => None,
        })
        .expect("count(*) value")
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

#[test]
fn delete_with_not_in_literal_list_keeps_non_matching_rows() {
    let (_temp_dir, engine, mut session) = create_engine();
    engine
        .execute(
            &mut session,
            "CREATE TABLE t_sync_task_monitor (
                id INT64,
                deal_date INT32,
                table_name VARCHAR
            ) PARTITION BY YEAR(deal_date) CLUSTER BY (deal_date, table_name)",
        )
        .expect("create table");
    engine
        .execute(
            &mut session,
            "INSERT INTO t_sync_task_monitor VALUES
                (1, 20260609, 'a'),
                (2, 20260609, 'a'),
                (3, 20260609, 'b'),
                (4, 20260609, 'b'),
                (5, 20260610, 'a')",
        )
        .expect("insert rows");
    engine
        .execute(&mut session, "FLUSH TABLE t_sync_task_monitor")
        .expect("flush rows");

    let deleted = engine
        .execute(
            &mut session,
            "DELETE FROM t_sync_task_monitor
             WHERE deal_date = 20260609
               AND id NOT IN (2, 4)",
        )
        .expect("delete rows");
    match deleted {
        ExecResult::Ok { rows_affected } => assert_eq!(rows_affected, 2),
        other => panic!("expected ok, got {:?}", other),
    }

    let count = engine
        .execute(
            &mut session,
            "SELECT COUNT(*) FROM t_sync_task_monitor WHERE deal_date = 20260609",
        )
        .expect("count remaining rows");
    assert_eq!(count_star(count), 2);

    let remaining = engine
        .execute(
            &mut session,
            "SELECT * FROM t_sync_task_monitor WHERE deal_date = 20260609",
        )
        .expect("select remaining rows");
    assert_eq!(row_count(remaining), 2);
    engine.segment_mgr().shutdown();
}

#[test]
fn delete_with_not_in_subquery_keeps_group_max_rows() {
    let (_temp_dir, engine, mut session) = create_engine();
    engine
        .execute(
            &mut session,
            "CREATE TABLE t_sync_task_monitor (
                id INT64,
                deal_date INT32,
                table_name VARCHAR
            ) PARTITION BY YEAR(deal_date) CLUSTER BY (deal_date, table_name)",
        )
        .expect("create table");
    engine
        .execute(
            &mut session,
            "INSERT INTO t_sync_task_monitor VALUES
                (1, 20260609, 'a'),
                (2, 20260609, 'a'),
                (3, 20260609, 'b'),
                (4, 20260609, 'b'),
                (5, 20260610, 'a')",
        )
        .expect("insert rows");
    engine
        .execute(&mut session, "FLUSH TABLE t_sync_task_monitor")
        .expect("flush rows");

    let deleted = engine
        .execute(
            &mut session,
            "DELETE FROM t_sync_task_monitor
             WHERE deal_date = 20260609
               AND id NOT IN (
                   SELECT MAX(id)
                   FROM t_sync_task_monitor
                   WHERE deal_date = 20260609
                   GROUP BY deal_date, table_name
               )",
        )
        .expect("delete rows");
    match deleted {
        ExecResult::Ok { rows_affected } => assert_eq!(rows_affected, 2),
        other => panic!("expected ok, got {:?}", other),
    }

    let count = engine
        .execute(
            &mut session,
            "SELECT COUNT(*) FROM t_sync_task_monitor WHERE deal_date = 20260609",
        )
        .expect("count remaining rows");
    assert_eq!(count_star(count), 2);

    let remaining = engine
        .execute(
            &mut session,
            "SELECT * FROM t_sync_task_monitor WHERE deal_date = 20260609",
        )
        .expect("select remaining rows");
    assert_eq!(row_count(remaining), 2);
    engine.segment_mgr().shutdown();
}

#[test]
fn delete_with_not_in_derived_subquery_keeps_group_max_rows() {
    let (_temp_dir, engine, mut session) = create_engine();
    engine
        .execute(
            &mut session,
            "CREATE TABLE t_sync_task_monitor (
                id INT64,
                deal_date INT32,
                table_name VARCHAR
            ) PARTITION BY YEAR(deal_date) CLUSTER BY (deal_date, table_name)",
        )
        .expect("create table");
    engine
        .execute(
            &mut session,
            "INSERT INTO t_sync_task_monitor VALUES
                (1, 20260609, 'a'),
                (2, 20260609, 'a'),
                (3, 20260609, 'b'),
                (4, 20260609, 'b'),
                (5, 20260610, 'a')",
        )
        .expect("insert rows");
    engine
        .execute(&mut session, "FLUSH TABLE t_sync_task_monitor")
        .expect("flush rows");

    let deleted = engine
        .execute(
            &mut session,
            "DELETE FROM t_sync_task_monitor
             WHERE deal_date = 20260609
               AND id NOT IN (
                   SELECT max_id
                   FROM (
                       SELECT MAX(id) AS max_id
                       FROM t_sync_task_monitor
                       WHERE deal_date = 20260609
                       GROUP BY deal_date, table_name
                   ) t
               )",
        )
        .expect("delete rows");
    match deleted {
        ExecResult::Ok { rows_affected } => assert_eq!(rows_affected, 2),
        other => panic!("expected ok, got {:?}", other),
    }

    let count = engine
        .execute(
            &mut session,
            "SELECT COUNT(*) FROM t_sync_task_monitor WHERE deal_date = 20260609",
        )
        .expect("count remaining rows");
    assert_eq!(count_star(count), 2);

    let remaining = engine
        .execute(
            &mut session,
            "SELECT * FROM t_sync_task_monitor WHERE deal_date = 20260609",
        )
        .expect("select remaining rows");
    assert_eq!(row_count(remaining), 2);
    engine.segment_mgr().shutdown();
}
