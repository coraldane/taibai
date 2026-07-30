use std::sync::Arc;
use tempfile::TempDir;

use taibai::net::session::Session;
use taibai::sql::execution::engine::{Engine, ExecResult};
use taibai::sql::execution::prepared::PreparedStatementCache;
use taibai::types::scalar::ScalarValue;

fn create_engine() -> (TempDir, Engine, Session) {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("failed to create engine");
    let session = Session::new("root".to_string(), "main".to_string());
    (temp_dir, engine, session)
}

#[test]
fn primary_key_cache_rejects_duplicates_and_allows_deleted_key_reuse() {
    let (_temp_dir, engine, mut session) = create_engine();
    engine
        .execute(
            &mut session,
            "CREATE TABLE cached_pk (id INT64 PRIMARY KEY, value VARCHAR)",
        )
        .expect("create table");
    engine
        .execute(&mut session, "INSERT INTO cached_pk VALUES (1, 'a')")
        .expect("first insert");
    engine
        .execute(&mut session, "INSERT INTO cached_pk VALUES (2, 'b')")
        .expect("second insert");
    assert!(engine
        .execute(&mut session, "INSERT INTO cached_pk VALUES (1, 'dup')")
        .is_err());

    engine
        .execute(&mut session, "DELETE FROM cached_pk WHERE id = 1")
        .expect("delete");
    engine
        .execute(&mut session, "INSERT INTO cached_pk VALUES (1, 'new')")
        .expect("reuse deleted key");
}

#[test]
fn unique_index_allows_reusing_incremental_key_after_delete() {
    let (temp_dir, engine, mut session) = create_engine();
    engine
        .execute(
            &mut session,
            "CREATE TABLE users (id INT64 PRIMARY KEY, code INT64, UNIQUE KEY uk_code (code))",
        )
        .expect("create table");
    engine
        .execute(&mut session, "INSERT INTO users VALUES (1, 10)")
        .expect("insert base row");
    engine
        .execute(&mut session, "REBUILD UNIQUE INDEX uk_code ON users")
        .expect("rebuild unique index");
    engine
        .execute(&mut session, "INSERT INTO users VALUES (2, 20)")
        .expect("insert delta row");
    engine
        .execute(&mut session, "DELETE FROM users WHERE id = 2")
        .expect("delete delta row");
    engine.flush_all_buffers().expect("flush before restart");
    engine.segment_mgr().shutdown();
    drop(engine);

    let engine = Engine::new(temp_dir.path(), "main").expect("restart engine");
    let mut session = Session::new("root".to_string(), "main".to_string());
    engine
        .execute(&mut session, "INSERT INTO users VALUES (3, 20)")
        .expect("reuse deleted delta key");
    assert!(engine
        .execute(&mut session, "INSERT INTO users VALUES (4, 20)")
        .is_err());
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

#[test]
fn omitted_auto_increment_primary_key_generates_unique_values() {
    let (_temp_dir, engine, mut session) = create_engine();

    engine
        .execute(
            &mut session,
            "CREATE TABLE users (id INT64 AUTO_INCREMENT PRIMARY KEY, score INT64)",
        )
        .expect("create table should succeed");

    engine
        .execute(
            &mut session,
            "INSERT INTO users (score) VALUES (10), (20), (30)",
        )
        .expect("auto increment insert should succeed");

    let duplicate = engine.execute(&mut session, "INSERT INTO users (id, score) VALUES (2, 99)");
    assert!(
        duplicate.is_err(),
        "explicit duplicate primary key should still fail"
    );

    let result = engine
        .execute(&mut session, "SELECT id, score FROM users ORDER BY id")
        .expect("select should succeed");
    match result {
        ExecResult::Query { batches, .. } => {
            assert_eq!(batches[0].num_rows(), 3);
            assert_eq!(batches[0].scalar(0, 0), Some(ScalarValue::Int64(Some(1))));
            assert_eq!(batches[0].scalar(1, 0), Some(ScalarValue::Int64(Some(2))));
            assert_eq!(batches[0].scalar(2, 0), Some(ScalarValue::Int64(Some(3))));
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn explicit_auto_increment_value_advances_counter() {
    let (_temp_dir, engine, mut session) = create_engine();

    engine
        .execute(
            &mut session,
            "CREATE TABLE users (id INT64 AUTO_INCREMENT PRIMARY KEY, score INT64)",
        )
        .expect("create table should succeed");

    engine
        .execute(
            &mut session,
            "INSERT INTO users (id, score) VALUES (10, 100)",
        )
        .expect("explicit auto increment value should insert");
    engine
        .execute(&mut session, "INSERT INTO users (score) VALUES (200)")
        .expect("generated value should follow explicit max");

    let result = engine
        .execute(&mut session, "SELECT id FROM users ORDER BY id")
        .expect("select should succeed");
    match result {
        ExecResult::Query { batches, .. } => {
            assert_eq!(batches[0].num_rows(), 2);
            assert_eq!(batches[0].scalar(0, 0), Some(ScalarValue::Int64(Some(10))));
            assert_eq!(batches[0].scalar(1, 0), Some(ScalarValue::Int64(Some(11))));
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn explicit_auto_increment_value_restores_counter_after_restart() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");

    {
        let engine = Engine::new(temp_dir.path(), "main").expect("failed to create engine");
        let mut session = Session::new("root".to_string(), "main".to_string());

        engine
            .execute(
                &mut session,
                "CREATE TABLE users (id INT64 AUTO_INCREMENT PRIMARY KEY, score INT64)",
            )
            .expect("create table should succeed");
        engine
            .execute(
                &mut session,
                "INSERT INTO users (id, score) VALUES (10, 100)",
            )
            .expect("explicit auto increment value should insert");
        engine.flush_all_buffers().expect("flush should succeed");
        engine.segment_mgr().shutdown();
    }

    let engine = Engine::new(temp_dir.path(), "main").expect("failed to restart engine");
    let mut session = Session::new("root".to_string(), "main".to_string());

    engine
        .execute(&mut session, "INSERT INTO users (score) VALUES (200)")
        .expect("generated value should follow restored explicit max");

    let result = engine
        .execute(&mut session, "SELECT id FROM users ORDER BY id")
        .expect("select should succeed");
    match result {
        ExecResult::Query { batches, .. } => {
            assert_eq!(batches[0].num_rows(), 2);
            assert_eq!(batches[0].scalar(0, 0), Some(ScalarValue::Int64(Some(10))));
            assert_eq!(batches[0].scalar(1, 0), Some(ScalarValue::Int64(Some(11))));
        }
        other => panic!("expected query result, got {:?}", other),
    }

    engine.segment_mgr().shutdown();
}

#[test]
fn replace_values_replaces_primary_key_and_allows_omitted_into() {
    let (_temp_dir, engine, mut session) = create_engine();
    engine
        .execute(
            &mut session,
            "CREATE TABLE users (id INT64 PRIMARY KEY, name VARCHAR)",
        )
        .expect("create table should succeed");
    engine
        .execute(&mut session, "INSERT INTO users VALUES (1, 'old')")
        .expect("insert should succeed");

    let result = engine
        .execute(&mut session, "REPLACE users VALUES (1, 'new')")
        .expect("replace by primary key should succeed");
    assert!(matches!(result, ExecResult::Ok { rows_affected: 2 }));

    let result = engine
        .execute(&mut session, "SELECT id, name FROM users")
        .expect("select should succeed");
    match result {
        ExecResult::Query { batches, .. } => {
            assert_eq!(batches[0].num_rows(), 1);
            assert_eq!(
                batches[0].scalar(0, 1),
                Some(ScalarValue::VarChar(Some("new".to_string())))
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn replace_values_uses_materialized_defaults_and_expressions_for_conflicts() {
    let (_temp_dir, engine, mut session) = create_engine();
    engine
        .execute(
            &mut session,
            "CREATE TABLE users (id INT64 PRIMARY KEY, code INT64 DEFAULT 7, name VARCHAR, UNIQUE KEY uk_code (code))",
        )
        .expect("create table should succeed");
    engine
        .execute(&mut session, "INSERT INTO users VALUES (1, 7, 'old')")
        .expect("insert should succeed");

    engine
        .execute(
            &mut session,
            "REPLACE INTO users (id, name) VALUES (2, 'default')",
        )
        .expect("defaulted unique key should replace");
    engine
        .execute(
            &mut session,
            "REPLACE INTO users (id, code, name) VALUES (3, 3 + 4, 'expression')",
        )
        .expect("expression unique key should replace");

    let result = engine
        .execute(&mut session, "SELECT id, code, name FROM users")
        .expect("select should succeed");
    match result {
        ExecResult::Query { batches, .. } => {
            assert_eq!(
                batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
                1
            );
            let batch = batches
                .iter()
                .find(|batch| batch.num_rows() > 0)
                .expect("one result row");
            assert_eq!(batch.scalar(0, 0), Some(ScalarValue::Int64(Some(3))));
            assert_eq!(batch.scalar(0, 1), Some(ScalarValue::Int64(Some(7))));
            assert_eq!(
                batch.scalar(0, 2),
                Some(ScalarValue::VarChar(Some("expression".to_string())))
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn replace_values_supports_default_keyword_and_empty_value_list() {
    let (_temp_dir, engine, mut session) = create_engine();
    engine
        .execute(
            &mut session,
            "CREATE TABLE users (
                id INT64 AUTO_INCREMENT PRIMARY KEY,
                code INT64 DEFAULT 7,
                name VARCHAR DEFAULT 'anonymous',
                UNIQUE KEY uk_code (code)
            )",
        )
        .expect("create table");
    engine
        .execute(&mut session, "INSERT INTO users VALUES ()")
        .expect("empty values should use all defaults");

    let result = engine
        .execute(
            &mut session,
            "REPLACE INTO users VALUES (DEFAULT, DEFAULT, 'explicit')",
        )
        .expect("replace with DEFAULT should succeed");
    assert!(matches!(result, ExecResult::Ok { rows_affected: 2 }));

    let result = engine
        .execute(&mut session, "REPLACE users VALUES ()")
        .expect("empty replace values should use all defaults");
    assert!(matches!(result, ExecResult::Ok { rows_affected: 2 }));

    let result = engine
        .execute(&mut session, "SELECT id, code, name FROM users")
        .expect("select defaults row");
    match result {
        ExecResult::Query { batches, .. } => {
            let rows = batches.iter().map(|batch| batch.num_rows()).sum::<usize>();
            assert_eq!(rows, 1);
            let batch = batches
                .iter()
                .find(|batch| batch.num_rows() == 1)
                .expect("one row batch");
            assert_eq!(batch.scalar(0, 0), Some(ScalarValue::Int64(Some(3))));
            assert_eq!(batch.scalar(0, 1), Some(ScalarValue::Int64(Some(7))));
            assert_eq!(
                batch.scalar(0, 2),
                Some(ScalarValue::VarChar(Some("anonymous".to_string())))
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn replace_values_processes_batch_conflicts_in_input_order() {
    let (_temp_dir, engine, mut session) = create_engine();
    engine
        .execute(
            &mut session,
            "CREATE TABLE users (id INT64 PRIMARY KEY, code INT64, name VARCHAR, UNIQUE KEY uk_code (code))",
        )
        .expect("create table should succeed");
    engine
        .execute(
            &mut session,
            "INSERT INTO users VALUES (1, 10, 'old-1'), (2, 20, 'old-2')",
        )
        .expect("insert should succeed");

    let result = engine
        .execute(
            &mut session,
            "REPLACE INTO users VALUES (1, 11, 'first'), (1, 12, 'second'), (3, 12, 'last')",
        )
        .expect("batch replace should succeed");
    assert!(matches!(result, ExecResult::Ok { rows_affected: 6 }));

    let result = engine
        .execute(&mut session, "SELECT id, code, name FROM users ORDER BY id")
        .expect("select should succeed");
    match result {
        ExecResult::Query { batches, .. } => {
            assert_eq!(batches[0].num_rows(), 2);
            assert_eq!(batches[0].scalar(0, 0), Some(ScalarValue::Int64(Some(2))));
            assert_eq!(batches[0].scalar(1, 0), Some(ScalarValue::Int64(Some(3))));
            assert_eq!(batches[0].scalar(1, 1), Some(ScalarValue::Int64(Some(12))));
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn replace_values_can_delete_rows_conflicting_on_different_unique_keys() {
    let (_temp_dir, engine, mut session) = create_engine();
    engine
        .execute(
            &mut session,
            "CREATE TABLE users (id INT64 PRIMARY KEY, a INT64, b INT64, UNIQUE KEY uk_a (a), UNIQUE KEY uk_b (b))",
        )
        .expect("create table should succeed");
    engine
        .execute(
            &mut session,
            "INSERT INTO users VALUES (1, 10, 100), (2, 20, 200)",
        )
        .expect("insert should succeed");

    let result = engine
        .execute(&mut session, "REPLACE INTO users VALUES (3, 10, 200)")
        .expect("replace should delete both conflicting rows");
    assert!(matches!(result, ExecResult::Ok { rows_affected: 3 }));

    let result = engine
        .execute(&mut session, "SELECT id, a, b FROM users")
        .expect("select should succeed");
    match result {
        ExecResult::Query { batches, .. } => {
            assert_eq!(batches[0].num_rows(), 1);
            assert_eq!(batches[0].scalar(0, 0), Some(ScalarValue::Int64(Some(3))));
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn replace_values_supports_composite_primary_keys_and_nullable_unique_keys() {
    let (_temp_dir, engine, mut session) = create_engine();
    engine
        .execute(
            &mut session,
            "CREATE TABLE items (tenant INT64, id INT64, code INT64, name VARCHAR, PRIMARY KEY (tenant, id), UNIQUE KEY uk_code (code))",
        )
        .expect("create table should succeed");
    engine
        .execute(
            &mut session,
            "INSERT INTO items VALUES (1, 1, NULL, 'old'), (1, 2, NULL, 'other')",
        )
        .expect("nullable unique keys should not conflict");

    engine
        .execute(
            &mut session,
            "REPLACE INTO items VALUES (1, 1, NULL, 'new')",
        )
        .expect("composite primary key should replace");

    let result = engine
        .execute(
            &mut session,
            "SELECT tenant, id, name FROM items ORDER BY id",
        )
        .expect("select should succeed");
    match result {
        ExecResult::Query { batches, .. } => {
            let rows = batches.iter().map(|batch| batch.num_rows()).sum::<usize>();
            assert_eq!(rows, 2);
            let names = batches
                .iter()
                .flat_map(|batch| (0..batch.num_rows()).filter_map(|row| batch.scalar(row, 2)))
                .collect::<Vec<_>>();
            assert!(names.contains(&ScalarValue::VarChar(Some("new".to_string()))));
            assert!(names.contains(&ScalarValue::VarChar(Some("other".to_string()))));
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn replace_select_replaces_query_rows() {
    let (_temp_dir, engine, mut session) = create_engine();
    engine
        .execute(&mut session, "CREATE TABLE source (id INT64, name VARCHAR)")
        .expect("create source should succeed");
    engine
        .execute(
            &mut session,
            "CREATE TABLE target (id INT64 PRIMARY KEY, name VARCHAR)",
        )
        .expect("create target should succeed");
    engine
        .execute(
            &mut session,
            "INSERT INTO source VALUES (1, 'new'), (2, 'second')",
        )
        .expect("insert source should succeed");
    engine
        .execute(&mut session, "INSERT INTO target VALUES (1, 'old')")
        .expect("insert target should succeed");

    let result = engine
        .execute(
            &mut session,
            "REPLACE target (id, name) SELECT id, name FROM source ORDER BY id",
        )
        .expect("replace select should succeed");
    assert!(matches!(result, ExecResult::Ok { rows_affected: 3 }));

    let result = engine
        .execute(&mut session, "SELECT id, name FROM target ORDER BY id")
        .expect("select should succeed");
    match result {
        ExecResult::Query { batches, .. } => {
            let rows = batches.iter().map(|batch| batch.num_rows()).sum::<usize>();
            assert_eq!(rows, 2);
            let values = batches
                .iter()
                .flat_map(|batch| {
                    (0..batch.num_rows()).map(|row| {
                        (
                            batch.scalar(row, 0).expect("id"),
                            batch.scalar(row, 1).expect("name"),
                        )
                    })
                })
                .collect::<Vec<_>>();
            assert!(values.contains(&(
                ScalarValue::Int64(Some(1)),
                ScalarValue::VarChar(Some("new".to_string()))
            )));
            assert!(values.contains(&(
                ScalarValue::Int64(Some(2)),
                ScalarValue::VarChar(Some("second".to_string()))
            )));
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn prepared_replace_select_binds_source_parameters() {
    let (_temp_dir, engine, mut session) = create_engine();
    engine
        .execute(&mut session, "CREATE TABLE source (id INT64, name VARCHAR)")
        .expect("create source");
    engine
        .execute(
            &mut session,
            "CREATE TABLE target (id INT64 PRIMARY KEY, name VARCHAR)",
        )
        .expect("create target");
    engine
        .execute(
            &mut session,
            "INSERT INTO source VALUES (1, 'new'), (2, 'ignored')",
        )
        .expect("insert source");
    engine
        .execute(&mut session, "INSERT INTO target VALUES (1, 'old')")
        .expect("insert target");

    let mut cache = PreparedStatementCache::new();
    let mut prepared = cache
        .prepare("REPLACE INTO target SELECT id, name FROM source WHERE id = ?")
        .expect("prepare replace select");
    prepared.params = vec![ScalarValue::Int64(Some(1))];
    let result = engine
        .execute_prepared(&mut session, &mut prepared)
        .expect("execute prepared replace select");
    assert!(matches!(result, ExecResult::Ok { rows_affected: 2 }));
    let result = engine
        .execute(&mut session, "SELECT name FROM target WHERE id = 1")
        .expect("select target");
    match result {
        ExecResult::Query { batches, .. } => assert_eq!(
            batches[0].scalar(0, 0),
            Some(ScalarValue::VarChar(Some("new".to_string())))
        ),
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn replace_moves_primary_key_row_between_partitions() {
    let (_temp_dir, engine, mut session) = create_engine();
    engine
        .execute(
            &mut session,
            "CREATE TABLE events (id INT64 PRIMARY KEY, event_day INT32, name VARCHAR) PARTITION BY YEAR(event_day)",
        )
        .expect("create partitioned table");
    engine
        .execute(
            &mut session,
            "INSERT INTO events VALUES (1, 20250101, 'old')",
        )
        .expect("insert old partition row");
    engine
        .execute(
            &mut session,
            "REPLACE INTO events VALUES (1, 20260101, 'new')",
        )
        .expect("replace into new partition");

    let result = engine
        .execute(
            &mut session,
            "SELECT event_day, name FROM events WHERE id = 1",
        )
        .expect("select replaced row");
    match result {
        ExecResult::Query { batches, .. } => {
            let rows = batches.iter().map(|batch| batch.num_rows()).sum::<usize>();
            assert_eq!(rows, 1);
            let batch = batches
                .iter()
                .find(|batch| batch.num_rows() == 1)
                .expect("one row batch");
            assert_eq!(batch.scalar(0, 0), Some(ScalarValue::Int32(Some(20260101))));
            assert_eq!(
                batch.scalar(0, 1),
                Some(ScalarValue::VarChar(Some("new".to_string())))
            );
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn replace_invalidates_append_only_hot_store() {
    let (_temp_dir, engine, mut session) = create_engine();
    engine
        .execute(
            &mut session,
            "CREATE TABLE ticks (
                security_id VARCHAR PRIMARY KEY,
                trade_time TIMESTAMP NOT NULL,
                value INT64
            ) CLUSTER BY (security_id) HOT BY (trade_time) INDEX (security_id)",
        )
        .expect("create hot table");
    engine
        .execute(
            &mut session,
            "INSERT INTO ticks VALUES ('A', 1783492992000000, 1)",
        )
        .expect("insert hot row");
    assert!(engine.hot_store_manager().get("main", "ticks").is_some());

    engine
        .execute(
            &mut session,
            "REPLACE INTO ticks VALUES ('A', 1783492993000000, 2)",
        )
        .expect("replace hot row");
    assert!(engine.hot_store_manager().get("main", "ticks").is_none());

    let result = engine
        .execute(
            &mut session,
            "SELECT trade_time, value FROM ticks WHERE security_id = 'A'",
        )
        .expect("select replaced hot row");
    match result {
        ExecResult::Query { batches, .. } => {
            let values = batches
                .iter()
                .flat_map(|batch| (0..batch.num_rows()).filter_map(|row| batch.scalar(row, 1)))
                .collect::<Vec<_>>();
            assert_eq!(values, vec![ScalarValue::Int64(Some(2))]);
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn replace_remains_consistent_after_restart() {
    let temp_dir = TempDir::new().expect("temp dir");
    {
        let engine = Engine::new(temp_dir.path(), "main").expect("create engine");
        let mut session = Session::new("root".to_string(), "main".to_string());
        engine
            .execute(
                &mut session,
                "CREATE TABLE users (id INT64 PRIMARY KEY, code INT64, name VARCHAR, UNIQUE KEY uk_code (code))",
            )
            .expect("create table");
        engine
            .execute(&mut session, "INSERT INTO users VALUES (1, 10, 'old')")
            .expect("insert old row");
        engine
            .execute(&mut session, "REBUILD UNIQUE INDEX uk_code ON users")
            .expect("rebuild unique index");
        engine
            .execute(&mut session, "REPLACE INTO users VALUES (1, 10, 'new')")
            .expect("replace row");
        engine.flush_all_buffers().expect("flush replacement");
        engine.segment_mgr().shutdown();
    }

    let engine = Engine::new(temp_dir.path(), "main").expect("restart engine");
    let mut session = Session::new("root".to_string(), "main".to_string());
    let result = engine
        .execute(&mut session, "SELECT name FROM users WHERE id = 1")
        .expect("select replacement after restart");
    match result {
        ExecResult::Query { batches, .. } => assert_eq!(
            batches[0].scalar(0, 0),
            Some(ScalarValue::VarChar(Some("new".to_string())))
        ),
        other => panic!("expected query result, got {:?}", other),
    }
    assert!(engine
        .execute(
            &mut session,
            "INSERT INTO users VALUES (2, 10, 'duplicate')",
        )
        .is_err());
    engine.segment_mgr().shutdown();
}

#[test]
fn replace_and_update_are_serialized_at_table_level() {
    let temp_dir = TempDir::new().expect("temp dir");
    let engine = Arc::new(Engine::new(temp_dir.path(), "main").expect("create engine"));
    let mut root = Session::new("root".to_string(), "main".to_string());
    engine
        .execute(
            &mut root,
            "CREATE TABLE users (id INT64 PRIMARY KEY, code INT64, name VARCHAR, UNIQUE KEY uk_code (code))",
        )
        .expect("create table");
    engine
        .execute(&mut root, "INSERT INTO users VALUES (1, 10, 'initial')")
        .expect("insert initial row");

    let replace_engine = Arc::clone(&engine);
    let replace_thread = std::thread::spawn(move || {
        let mut session = Session::new("root".to_string(), "main".to_string());
        for idx in 0..10 {
            replace_engine
                .execute(
                    &mut session,
                    &format!("REPLACE INTO users VALUES (1, 10, 'replace-{}')", idx),
                )
                .expect("concurrent replace");
        }
    });
    let update_engine = Arc::clone(&engine);
    let update_thread = std::thread::spawn(move || {
        let mut session = Session::new("root".to_string(), "main".to_string());
        for idx in 0..10 {
            update_engine
                .execute(
                    &mut session,
                    &format!("UPDATE users SET name = 'update-{}' WHERE id = 1", idx),
                )
                .expect("concurrent update");
        }
    });
    replace_thread.join().expect("replace thread");
    update_thread.join().expect("update thread");

    engine
        .execute(&mut root, "REPLACE INTO users VALUES (1, 10, 'final')")
        .expect("final replace");
    let result = engine
        .execute(&mut root, "SELECT COUNT(*) FROM users")
        .expect("count final rows");
    match result {
        ExecResult::Query { batches, .. } => {
            assert_eq!(batches[0].scalar(0, 0), Some(ScalarValue::Int64(Some(1))))
        }
        other => panic!("expected query result, got {:?}", other),
    }
    engine.segment_mgr().shutdown();
}

#[test]
fn replace_is_rejected_inside_transaction_without_deleting_old_row() {
    let (_temp_dir, engine, mut session) = create_engine();
    engine
        .execute(
            &mut session,
            "CREATE TABLE users (id INT64 PRIMARY KEY, name VARCHAR)",
        )
        .expect("create table should succeed");
    engine
        .execute(&mut session, "INSERT INTO users VALUES (1, 'old')")
        .expect("insert should succeed");
    engine
        .execute(&mut session, "BEGIN")
        .expect("begin should succeed");

    let replace = engine.execute(&mut session, "REPLACE INTO users VALUES (1, 'new')");
    assert!(replace.is_err(), "transactional replace must fail safely");
    engine
        .execute(&mut session, "ROLLBACK")
        .expect("rollback should succeed");

    let result = engine
        .execute(&mut session, "SELECT name FROM users WHERE id = 1")
        .expect("select should succeed");
    match result {
        ExecResult::Query { batches, .. } => assert_eq!(
            batches[0].scalar(0, 0),
            Some(ScalarValue::VarChar(Some("old".to_string())))
        ),
        other => panic!("expected query result, got {:?}", other),
    }
}
