//! Deferred DELETE/UPDATE inside explicit transactions: rollback discards
//! the plans, commit materializes them durably, and other sessions never see
//! uncommitted tombstones or row images.

use tempfile::TempDir;

use taibai::net::session::Session;
use taibai::sql::execution::engine::{Engine, ExecResult};
use taibai::storage::core::txn_mutations::filter_batch_excluding_positions;
use taibai::types::batch::RecordBatch;
use taibai::types::scalar::ScalarValue;
use std::collections::BTreeSet;

fn create_test_engine() -> (TempDir, Engine) {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("failed to create engine");
    (temp_dir, engine)
}

fn session() -> Session {
    Session::new("root".to_string(), "main".to_string())
}

fn ok_rows_affected(result: ExecResult) -> u64 {
    match result {
        ExecResult::Ok { rows_affected } => rows_affected,
        other => panic!("expected Ok, got {:?}", other),
    }
}

fn query_i64(engine: &Engine, session: &mut Session, sql: &str) -> Vec<i64> {
    match engine.execute(session, sql).expect("query failed") {
        ExecResult::Query { batches, .. } => {
            let mut out = Vec::new();
            for batch in batches {
                for row in 0..batch.num_rows() {
                    match batch.scalar(row, 0) {
                        Some(ScalarValue::Int64(Some(v))) => out.push(v),
                        Some(ScalarValue::Int32(Some(v))) => out.push(v as i64),
                        other => panic!("unexpected scalar {:?}", other),
                    }
                }
            }
            out
        }
        other => panic!("expected Query, got {:?}", other),
    }
}

fn setup(engine: &Engine, session: &mut Session, ddl: &str, rows: &[i64]) {
    engine.execute(session, ddl).expect("create failed");
    for chunk in rows.chunks(2) {
        let values = chunk
            .iter()
            .map(|id| format!("({id}, 'n{id}', {id})"))
            .collect::<Vec<_>>()
            .join(", ");
        engine
            .execute(session, &format!("INSERT INTO t VALUES {values}"))
            .expect("insert failed");
    }
    engine
        .execute(session, "FLUSH")
        .expect("flush failed");
}

const DDL: &str = "CREATE TABLE t (id INT32, name VARCHAR, value INT64)";

#[test]
fn rollback_after_flushed_delete_restores_rows() {
    let (_dir, engine) = create_test_engine();
    let mut a = session();
    setup(&engine, &mut a, DDL, &[1, 2, 3, 4]);

    engine.execute(&mut a, "BEGIN").unwrap();
    let deleted = ok_rows_affected(
        engine
            .execute(&mut a, "DELETE FROM t WHERE id <= 2")
            .unwrap(),
    );
    assert_eq!(deleted, 2);
    // The transaction sees its own tombstones.
    assert_eq!(query_i64(&engine, &mut a, "SELECT id FROM t"), vec![3, 4]);

    engine.execute(&mut a, "ROLLBACK").unwrap();
    assert_eq!(
        query_i64(&engine, &mut a, "SELECT id FROM t"),
        vec![1, 2, 3, 4]
    );

    // Nothing durable happened: a fresh session sees the full row set too.
    let mut b = session();
    assert_eq!(
        query_i64(&engine, &mut b, "SELECT id FROM t"),
        vec![1, 2, 3, 4]
    );
}

#[test]
fn commit_after_flushed_delete_is_durable() {
    let dir = TempDir::new().unwrap();
    {
        let engine = Engine::new(dir.path(), "main").unwrap();
        let mut a = session();
        setup(&engine, &mut a, DDL, &[1, 2, 3, 4]);

        engine.execute(&mut a, "BEGIN").unwrap();
        assert_eq!(
            ok_rows_affected(engine.execute(&mut a, "DELETE FROM t WHERE id <= 2").unwrap()),
            2
        );
        engine.execute(&mut a, "COMMIT").unwrap();
        assert_eq!(query_i64(&engine, &mut a, "SELECT id FROM t"), vec![3, 4]);
    }
    // Durable across restart.
    let engine = Engine::new(dir.path(), "main").unwrap();
    let mut b = session();
    assert_eq!(query_i64(&engine, &mut b, "SELECT id FROM t"), vec![3, 4]);
}

#[test]
fn uncommitted_delete_is_invisible_to_other_sessions() {
    let (_dir, engine) = create_test_engine();
    let mut a = session();
    let mut b = session();
    setup(&engine, &mut a, DDL, &[1, 2, 3, 4]);

    engine.execute(&mut a, "BEGIN").unwrap();
    engine.execute(&mut a, "DELETE FROM t WHERE id = 1").unwrap();

    // Another session still sees the full committed state.
    assert_eq!(
        query_i64(&engine, &mut b, "SELECT id FROM t"),
        vec![1, 2, 3, 4]
    );
    // COUNT(*) through the metadata path also stays honest.
    assert_eq!(
        query_i64(&engine, &mut b, "SELECT COUNT(id) FROM t"),
        vec![4]
    );

    engine.execute(&mut a, "ROLLBACK").unwrap();
    assert_eq!(
        query_i64(&engine, &mut b, "SELECT id FROM t"),
        vec![1, 2, 3, 4]
    );
}

#[test]
fn rollback_after_flushed_update_restores_values() {
    let (_dir, engine) = create_test_engine();
    let mut a = session();
    setup(&engine, &mut a, DDL, &[1, 2, 3]);

    engine.execute(&mut a, "BEGIN").unwrap();
    let updated = ok_rows_affected(
        engine
            .execute(&mut a, "UPDATE t SET value = 100 WHERE id = 1")
            .unwrap(),
    );
    assert_eq!(updated, 1);
    // The transaction reads its own writes.
    assert_eq!(
        query_i64(&engine, &mut a, "SELECT value FROM t WHERE id = 1"),
        vec![100]
    );

    engine.execute(&mut a, "ROLLBACK").unwrap();
    assert_eq!(
        query_i64(&engine, &mut a, "SELECT value FROM t WHERE id = 1"),
        vec![1]
    );
    let mut b = session();
    assert_eq!(
        query_i64(&engine, &mut b, "SELECT value FROM t WHERE id = 1"),
        vec![1]
    );
}

#[test]
fn commit_after_flushed_update_is_durable_and_accurate() {
    let dir = TempDir::new().unwrap();
    {
        let engine = Engine::new(dir.path(), "main").unwrap();
        let mut a = session();
        setup(&engine, &mut a, DDL, &[1, 2, 3]);

        engine.execute(&mut a, "BEGIN").unwrap();
        assert_eq!(
            ok_rows_affected(
                engine
                    .execute(&mut a, "UPDATE t SET value = 100 WHERE id = 1")
                    .unwrap()
            ),
            1
        );
        engine.execute(&mut a, "COMMIT").unwrap();
        // Exactly one row per id: no phantom duplicates from the old row.
        // (Order may change: the updated image is appended after the
        // surviving segment rows.)
        let mut ids = query_i64(&engine, &mut a, "SELECT id FROM t");
        ids.sort();
        assert_eq!(ids, vec![1, 2, 3]);
        assert_eq!(
            query_i64(&engine, &mut a, "SELECT value FROM t WHERE id = 1"),
            vec![100]
        );
    }
    let engine = Engine::new(dir.path(), "main").unwrap();
    let mut b = session();
    let mut ids = query_i64(&engine, &mut b, "SELECT id FROM t");
    ids.sort();
    assert_eq!(ids, vec![1, 2, 3]);
    assert_eq!(
        query_i64(&engine, &mut b, "SELECT value FROM t WHERE id = 1"),
        vec![100]
    );
}

#[test]
fn uncommitted_update_is_invisible_to_other_sessions() {
    let (_dir, engine) = create_test_engine();
    let mut a = session();
    let mut b = session();
    setup(&engine, &mut a, DDL, &[1, 2]);

    engine.execute(&mut a, "BEGIN").unwrap();
    engine
        .execute(&mut a, "UPDATE t SET value = 100 WHERE id = 1")
        .unwrap();

    assert_eq!(
        query_i64(&engine, &mut b, "SELECT value FROM t WHERE id = 1"),
        vec![1]
    );
    assert_eq!(
        query_i64(&engine, &mut b, "SELECT COUNT(value) FROM t"),
        vec![2]
    );
    engine.execute(&mut a, "ROLLBACK").unwrap();
}

#[test]
fn txn_delete_then_insert_then_rollback() {
    let (_dir, engine) = create_test_engine();
    let mut a = session();
    setup(&engine, &mut a, DDL, &[1, 2]);

    engine.execute(&mut a, "BEGIN").unwrap();
    engine.execute(&mut a, "DELETE FROM t WHERE id = 1").unwrap();
    engine
        .execute(&mut a, "INSERT INTO t VALUES (9, 'n9', 9)")
        .unwrap();
    let mut ids = query_i64(&engine, &mut a, "SELECT id FROM t");
    ids.sort();
    assert_eq!(ids, vec![2, 9]);

    engine.execute(&mut a, "ROLLBACK").unwrap();
    let mut ids = query_i64(&engine, &mut a, "SELECT id FROM t");
    ids.sort();
    assert_eq!(ids, vec![1, 2]);
}

#[test]
fn multiple_deletes_in_one_txn_accumulate() {
    let dir = TempDir::new().unwrap();
    {
        let engine = Engine::new(dir.path(), "main").unwrap();
        let mut a = session();
        setup(&engine, &mut a, DDL, &[1, 2, 3, 4, 5, 6]);

        engine.execute(&mut a, "BEGIN").unwrap();
        assert_eq!(
            ok_rows_affected(engine.execute(&mut a, "DELETE FROM t WHERE id <= 2").unwrap()),
            2
        );
        assert_eq!(
            ok_rows_affected(engine.execute(&mut a, "DELETE FROM t WHERE id = 4").unwrap()),
            1
        );
        let mut ids = query_i64(&engine, &mut a, "SELECT id FROM t");
        ids.sort();
        assert_eq!(ids, vec![3, 5, 6]);
        engine.execute(&mut a, "COMMIT").unwrap();
    }
    let engine = Engine::new(dir.path(), "main").unwrap();
    let mut b = session();
    let mut ids = query_i64(&engine, &mut b, "SELECT id FROM t");
    ids.sort();
    assert_eq!(ids, vec![3, 5, 6]);
}

#[test]
fn update_of_row_inserted_in_same_txn() {
    let (_dir, engine) = create_test_engine();
    let mut a = session();
    setup(&engine, &mut a, DDL, &[1]);

    engine.execute(&mut a, "BEGIN").unwrap();
    engine
        .execute(&mut a, "INSERT INTO t VALUES (7, 'n7', 7)")
        .unwrap();
    // The row lives in the transactional memtable, not a segment.
    let updated = ok_rows_affected(
        engine
            .execute(&mut a, "UPDATE t SET value = 700 WHERE id = 7")
            .unwrap(),
    );
    assert_eq!(updated, 1);
    assert_eq!(
        query_i64(&engine, &mut a, "SELECT value FROM t WHERE id = 7"),
        vec![700]
    );
    engine.execute(&mut a, "COMMIT").unwrap();
    assert_eq!(
        query_i64(&engine, &mut a, "SELECT value FROM t WHERE id = 7"),
        vec![700]
    );
    // The untouched pre-existing row is intact exactly once.
    assert_eq!(query_i64(&engine, &mut a, "SELECT id FROM t WHERE id = 1"), vec![1]);
}

#[test]
fn ddl_is_rejected_inside_transaction() {
    let (_dir, engine) = create_test_engine();
    let mut a = session();
    setup(&engine, &mut a, DDL, &[1]);

    engine.execute(&mut a, "BEGIN").unwrap();
    for sql in [
        "CREATE TABLE other (id INT32)",
        "DROP TABLE t",
        "ALTER TABLE t ADD COLUMN extra INT32",
        "TRUNCATE TABLE t",
        "OPTIMIZE TABLE t",
        "FLUSH",
    ] {
        let result = engine.execute(&mut a, sql);
        assert!(result.is_err(), "{sql} must be rejected inside a transaction");
    }
    engine.execute(&mut a, "ROLLBACK").unwrap();
    // The table is untouched.
    assert_eq!(query_i64(&engine, &mut a, "SELECT id FROM t"), vec![1]);
}

#[test]
fn pk_table_rejects_transactional_dml() {
    let (_dir, engine) = create_test_engine();
    let mut a = session();
    engine
        .execute(&mut a, "CREATE TABLE p (id INT32 PRIMARY KEY, v INT64)")
        .unwrap();
    engine
        .execute(&mut a, "INSERT INTO p VALUES (1, 1)")
        .unwrap();
    engine.execute(&mut a, "FLUSH").unwrap();

    engine.execute(&mut a, "BEGIN").unwrap();
    for sql in [
        "DELETE FROM p WHERE id = 1",
        "UPDATE p SET v = 2 WHERE id = 1",
    ] {
        let result = engine.execute(&mut a, sql);
        let error = result.expect_err("indexed table must reject transactional DML");
        assert!(
            error.to_string().contains("PRIMARY KEY"),
            "unexpected error for {sql}: {error}"
        );
    }
    engine.execute(&mut a, "ROLLBACK").unwrap();
}

#[test]
fn unique_index_table_rejects_transactional_dml() {
    let (_dir, engine) = create_test_engine();
    let mut a = session();
    engine
        .execute(
            &mut a,
            "CREATE TABLE u (id INT32, v INT64, UNIQUE KEY uk_v (v))",
        )
        .unwrap();
    engine.execute(&mut a, "INSERT INTO u VALUES (1, 1)").unwrap();
    engine.execute(&mut a, "FLUSH").unwrap();

    engine.execute(&mut a, "BEGIN").unwrap();
    let error = engine
        .execute(&mut a, "DELETE FROM u WHERE id = 1")
        .expect_err("unique-index table must reject transactional DML");
    assert!(error.to_string().contains("UNIQUE"), "unexpected: {error}");
    engine.execute(&mut a, "ROLLBACK").unwrap();
}

#[test]
fn filter_batch_excluding_positions_removes_rows() {
    let (_dir, engine) = create_test_engine();
    let mut a = session();
    setup(&engine, &mut a, DDL, &[1, 2, 3, 4]);
    let ExecResult::Query { mut batches, .. } = engine
        .execute(&mut a, "SELECT id, value FROM t")
        .unwrap()
    else {
        panic!("expected query");
    };
    let batch: RecordBatch = batches.remove(0);
    let positions: BTreeSet<u32> = [1u32, 3].into_iter().collect();
    let filtered = filter_batch_excluding_positions(&batch, &positions).unwrap();
    assert_eq!(filtered.num_rows(), 2);
    let mut ids = Vec::new();
    for row in 0..filtered.num_rows() {
        match filtered.scalar(row, 0) {
            Some(ScalarValue::Int32(Some(v))) => ids.push(v),
            other => panic!("unexpected {:?}", other),
        }
    }
    assert_eq!(ids, vec![1, 3]);
    // Empty positions is a no-op clone.
    let untouched = filter_batch_excluding_positions(&batch, &BTreeSet::new()).unwrap();
    assert_eq!(untouched.num_rows(), batch.num_rows());
}

/// Regression: two open transactions could plan mutations against the same
/// rows and both commit, leaving two new images of one row (or an image past
/// a tombstone). The first transaction's DML pins the table; another open
/// transaction's DML on the same table is refused until it commits or rolls
/// back, while the pinning transaction itself keeps access.
#[test]
fn concurrent_txn_dml_on_same_table_is_refused() {
    let (_dir, engine) = create_test_engine();
    let mut a = session();
    let mut b = session();
    setup(&engine, &mut a, DDL, &[1, 2, 3, 4]);

    engine.execute(&mut a, "BEGIN").unwrap();
    let deleted = ok_rows_affected(
        engine
            .execute(&mut a, "DELETE FROM t WHERE id <= 2")
            .unwrap(),
    );
    assert_eq!(deleted, 2);

    // Another open transaction must not plan mutations on the pinned table.
    engine.execute(&mut b, "BEGIN").unwrap();
    let err = engine
        .execute(&mut b, "UPDATE t SET value = 99 WHERE id = 3")
        .expect_err("concurrent transaction DML must be refused");
    assert!(
        err.to_string().contains("another open transaction"),
        "unexpected error: {err}"
    );

    // The pinning transaction itself can keep mutating the table.
    let deleted = ok_rows_affected(
        engine.execute(&mut a, "DELETE FROM t WHERE id = 3").unwrap(),
    );
    assert_eq!(deleted, 1);

    engine.execute(&mut b, "ROLLBACK").unwrap();
    engine.execute(&mut a, "COMMIT").unwrap();
    assert_eq!(query_i64(&engine, &mut a, "SELECT id FROM t"), vec![4]);
}
