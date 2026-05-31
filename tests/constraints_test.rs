use tempfile::TempDir;

use monax::net::session::Session;
use monax::sql::execution::engine::{Engine, ExecResult};
use monax::types::scalar::ScalarValue;

fn create_engine() -> (TempDir, Engine, Session) {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("failed to create engine");
    let session = Session::new("root".to_string(), "main".to_string());
    (temp_dir, engine, session)
}

#[test]
fn insert_fills_defaults_and_enforces_inline_primary_key() {
    let (_temp_dir, engine, mut session) = create_engine();

    engine
        .execute(
            &mut session,
            "CREATE TABLE users (id INT32 PRIMARY KEY, name VARCHAR DEFAULT 'anonymous', score INT32 DEFAULT 7)",
        )
        .expect("create table should succeed");

    engine
        .execute(&mut session, "INSERT INTO users (id) VALUES (1)")
        .expect("insert should fill defaults");

    let duplicate = engine.execute(&mut session, "INSERT INTO users (id) VALUES (1)");
    assert!(duplicate.is_err(), "duplicate primary key should fail");

    let result = engine
        .execute(&mut session, "SELECT id, name, score FROM users")
        .expect("select should succeed");
    match result {
        ExecResult::Query { batches, .. } => {
            assert_eq!(batches.len(), 1);
            assert_eq!(batches[0].num_rows(), 1);
            assert_eq!(batches[0].scalar(0, 0), Some(ScalarValue::Int32(Some(1))));
            assert_eq!(
                batches[0].scalar(0, 1),
                Some(ScalarValue::VarChar(Some("anonymous".to_string())))
            );
            assert_eq!(batches[0].scalar(0, 2), Some(ScalarValue::Int32(Some(7))));
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn insert_rejects_duplicates_inside_multi_row_batch() {
    let (_temp_dir, engine, mut session) = create_engine();

    engine
        .execute(
            &mut session,
            "CREATE TABLE users (id INT64 PRIMARY KEY, name VARCHAR)",
        )
        .expect("create table should succeed");

    let result = engine.execute(
        &mut session,
        "INSERT INTO users (id, name) VALUES (1, 'alice'), (1, 'bob')",
    );

    assert!(result.is_err(), "duplicate in the same insert should fail");
}

#[test]
fn table_level_composite_primary_key_is_unique() {
    let (_temp_dir, engine, mut session) = create_engine();

    engine
        .execute(
            &mut session,
            "CREATE TABLE orders (tenant INT32, id INT32, note VARCHAR DEFAULT 'new', PRIMARY KEY (tenant, id))",
        )
        .expect("create table with table-level primary key should succeed");

    engine
        .execute(
            &mut session,
            "INSERT INTO orders (tenant, id) VALUES (1, 1)",
        )
        .expect("first insert should succeed");
    engine
        .execute(
            &mut session,
            "INSERT INTO orders (tenant, id) VALUES (1, 2)",
        )
        .expect("different composite key should succeed");

    let duplicate = engine.execute(
        &mut session,
        "INSERT INTO orders (tenant, id) VALUES (1, 1)",
    );
    assert!(duplicate.is_err(), "duplicate composite key should fail");
}

#[test]
fn primary_key_cannot_be_null() {
    let (_temp_dir, engine, mut session) = create_engine();

    engine
        .execute(
            &mut session,
            "CREATE TABLE users (id INT64 PRIMARY KEY, name VARCHAR)",
        )
        .expect("create table should succeed");

    let result = engine.execute(&mut session, "INSERT INTO users (name) VALUES ('alice')");
    assert!(
        result.is_err(),
        "omitted primary key without default should fail"
    );
}
