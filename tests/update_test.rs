use tempfile::TempDir;

use taibai::net::session::Session;
use taibai::protocol::types::DataType;
use taibai::sql::execution::engine::{Engine, ExecResult};
use taibai::types::array::ColumnArray;
use taibai::types::batch::{ColumnDef, RecordBatch, Schema};
use taibai::types::scalar::ScalarValue;

fn create_engine() -> (TempDir, Engine, Session) {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("failed to create engine");
    let session = Session::new("root".to_string(), "main".to_string());
    (temp_dir, engine, session)
}

fn collect_i32(result: ExecResult, col_idx: usize) -> Vec<i32> {
    let ExecResult::Query { batches, .. } = result else {
        panic!("expected query result");
    };
    let mut values = Vec::new();
    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            match batch.scalar(row_idx, col_idx) {
                Some(ScalarValue::Int32(Some(v))) => values.push(v),
                other => panic!("expected int32, got {:?}", other),
            }
        }
    }
    values
}

#[test]
fn update_where_preserves_non_matching_rows() {
    let (_temp_dir, engine, mut session) = create_engine();
    engine
        .execute(&mut session, "CREATE TABLE t (id INT32, value INT32)")
        .expect("create table");
    engine
        .execute(
            &mut session,
            "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)",
        )
        .expect("insert rows");
    engine
        .execute(&mut session, "FLUSH TABLE t")
        .expect("flush rows");

    let updated = engine
        .execute(&mut session, "UPDATE t SET value = 99 WHERE id = 2")
        .expect("update row");
    match updated {
        ExecResult::Ok { rows_affected } => assert_eq!(rows_affected, 1),
        other => panic!("expected ok, got {:?}", other),
    }

    let mut ids = collect_i32(
        engine
            .execute(&mut session, "SELECT id FROM t")
            .expect("select ids"),
        0,
    );
    ids.sort_unstable();
    assert_eq!(ids, vec![1, 2, 3]);

    let mut values = collect_i32(
        engine
            .execute(&mut session, "SELECT value FROM t")
            .expect("select values"),
        0,
    );
    values.sort_unstable();
    assert_eq!(values, vec![10, 30, 99]);
    engine.segment_mgr().shutdown();
}

#[test]
fn update_where_no_match_keeps_table_unchanged() {
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

    let updated = engine
        .execute(&mut session, "UPDATE t SET value = 99 WHERE id = 9")
        .expect("update no rows");
    match updated {
        ExecResult::Ok { rows_affected } => assert_eq!(rows_affected, 0),
        other => panic!("expected ok, got {:?}", other),
    }

    let mut values = collect_i32(
        engine
            .execute(&mut session, "SELECT value FROM t")
            .expect("select values"),
        0,
    );
    values.sort_unstable();
    assert_eq!(values, vec![10, 20]);
    engine.segment_mgr().shutdown();
}

#[test]
fn update_preserves_unmatched_row_groups_in_rewritten_segment() {
    let (temp_dir, engine, mut session) = create_engine();
    engine
        .execute(&mut session, "CREATE TABLE t (id INT32, value INT32)")
        .expect("create table");

    let schema = Schema::new(vec![
        ColumnDef::new("id", DataType::Int32),
        ColumnDef::new("value", DataType::Int32),
    ]);
    let mut ids = ColumnArray::empty(DataType::Int32);
    let mut values = ColumnArray::empty(DataType::Int32);
    for i in 1..=5 {
        ids.push(&ScalarValue::Int32(Some(i))).unwrap();
        values.push(&ScalarValue::Int32(Some(i * 10))).unwrap();
    }
    let batch = RecordBatch::try_new(schema.clone(), vec![ids, values]).unwrap();
    let table_dir = temp_dir.path().join("main").join("t");
    std::fs::create_dir_all(&table_dir).unwrap();
    let seg_path = table_dir.join("seg_00001.dat");
    engine
        .segment_mgr()
        .write_new_segment(&seg_path, schema, 2, &batch)
        .expect("write multi-row-group segment");

    let updated = engine
        .execute(&mut session, "UPDATE t SET value = 99 WHERE id = 3")
        .expect("update one row");
    match updated {
        ExecResult::Ok { rows_affected } => assert_eq!(rows_affected, 1),
        other => panic!("expected ok, got {:?}", other),
    }

    let mut ids = collect_i32(
        engine
            .execute(&mut session, "SELECT id FROM t")
            .expect("select ids"),
        0,
    );
    ids.sort_unstable();
    assert_eq!(ids, vec![1, 2, 3, 4, 5]);

    let mut values = collect_i32(
        engine
            .execute(&mut session, "SELECT value FROM t")
            .expect("select values"),
        0,
    );
    values.sort_unstable();
    assert_eq!(values, vec![10, 20, 40, 50, 99]);
    engine.segment_mgr().shutdown();
}
