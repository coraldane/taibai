use tempfile::TempDir;

use taibai::net::session::Session;
use taibai::sql::execution::engine::{Engine, ExecResult};
use taibai::types::scalar::ScalarValue;

fn table_names(result: ExecResult) -> Vec<String> {
    match result {
        ExecResult::Query { batches, .. } => {
            let mut names = Vec::new();
            for batch in batches {
                for row in 0..batch.num_rows() {
                    if let Some(ScalarValue::VarChar(Some(name))) = batch.scalar(row, 0) {
                        names.push(name);
                    }
                }
            }
            names.sort();
            names
        }
        other => panic!("expected query result, got {:?}", other),
    }
}

#[test]
fn show_tables_is_isolated_by_current_database() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("failed to create engine");
    let mut session = Session::new("root".to_string(), "main".to_string());

    engine
        .execute(&mut session, "CREATE DATABASE db1")
        .expect("create db1");
    engine
        .execute(&mut session, "CREATE DATABASE db2")
        .expect("create db2");

    engine.execute(&mut session, "USE db1").expect("use db1");
    engine
        .execute(&mut session, "CREATE TABLE t1 (id INT64)")
        .expect("create t1");

    engine.execute(&mut session, "USE db2").expect("use db2");
    engine
        .execute(&mut session, "CREATE TABLE t2 (id INT64)")
        .expect("create t2");

    assert_eq!(
        table_names(engine.execute(&mut session, "SHOW TABLES").unwrap()),
        vec!["t2"]
    );
    assert_eq!(
        table_names(
            engine
                .execute(&mut session, "SHOW TABLES FROM db1")
                .unwrap()
        ),
        vec!["t1"]
    );

    engine
        .execute(&mut session, "USE db1")
        .expect("use db1 again");
    assert_eq!(
        table_names(engine.execute(&mut session, "SHOW TABLES").unwrap()),
        vec!["t1"]
    );

    engine.segment_mgr().shutdown();
}

#[test]
fn database_catalogs_remain_isolated_after_restart() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    {
        let engine = Engine::new(temp_dir.path(), "main").expect("failed to create engine");
        let mut session = Session::new("root".to_string(), "main".to_string());

        engine
            .execute(&mut session, "CREATE DATABASE db1")
            .expect("create db1");
        engine
            .execute(&mut session, "CREATE DATABASE db2")
            .expect("create db2");
        engine.execute(&mut session, "USE db1").expect("use db1");
        engine
            .execute(&mut session, "CREATE TABLE t1 (id INT64)")
            .expect("create t1");
        engine.execute(&mut session, "USE db2").expect("use db2");
        engine
            .execute(&mut session, "CREATE TABLE t2 (id INT64)")
            .expect("create t2");
        engine.segment_mgr().shutdown();
    }

    let engine = Engine::new(temp_dir.path(), "main").expect("failed to restart engine");
    let mut session = Session::new("root".to_string(), "db1".to_string());

    assert_eq!(
        table_names(engine.execute(&mut session, "SHOW TABLES").unwrap()),
        vec!["t1"]
    );
    assert_eq!(
        table_names(
            engine
                .execute(&mut session, "SHOW TABLES FROM db2")
                .unwrap()
        ),
        vec!["t2"]
    );

    engine.segment_mgr().shutdown();
}

#[test]
fn current_database_must_exist_for_table_operations() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("failed to create engine");
    let mut session = Session::new("root".to_string(), "missing".to_string());

    assert!(engine.execute(&mut session, "SHOW TABLES").is_err());
    assert!(engine
        .execute(&mut session, "CREATE TABLE t (id INT64)")
        .is_err());

    engine.segment_mgr().shutdown();
}

#[test]
fn explicit_database_table_names_use_target_database() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("failed to create engine");
    let mut session = Session::new("root".to_string(), "main".to_string());

    engine
        .execute(&mut session, "CREATE DATABASE db1")
        .expect("create db1");
    engine
        .execute(&mut session, "CREATE DATABASE db2")
        .expect("create db2");

    engine
        .execute(&mut session, "CREATE TABLE db1.t (id INT64)")
        .expect("create db1.t");
    engine
        .execute(&mut session, "CREATE TABLE db2.t (id INT64)")
        .expect("create db2.t");

    engine.execute(&mut session, "USE db1").expect("use db1");
    engine
        .execute(&mut session, "INSERT INTO db2.t VALUES (42)")
        .expect("insert into explicit db2 table");

    let result = engine
        .execute(&mut session, "SELECT id FROM db2.t")
        .expect("select explicit db2 table");
    match result {
        ExecResult::Query { batches, .. } => {
            assert_eq!(batches.len(), 1);
            assert_eq!(batches[0].num_rows(), 1);
            assert_eq!(batches[0].scalar(0, 0), Some(ScalarValue::Int64(Some(42))));
        }
        other => panic!("expected query result, got {:?}", other),
    }

    assert_eq!(
        table_names(engine.execute(&mut session, "SHOW TABLES").unwrap()),
        vec!["t"]
    );
    assert_eq!(
        table_names(
            engine
                .execute(&mut session, "SHOW TABLES FROM db2")
                .unwrap()
        ),
        vec!["t"]
    );
    engine
        .execute(&mut session, "DROP TABLE db2.t")
        .expect("drop explicit db2 table");
    assert!(engine
        .execute(&mut session, "SELECT id FROM db2.t")
        .is_err());

    engine.segment_mgr().shutdown();
}

#[test]
fn cross_database_join_resolves_qualified_columns_by_alias() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("failed to create engine");
    let mut session = Session::new("root".to_string(), "main".to_string());

    engine
        .execute(&mut session, "CREATE DATABASE db1")
        .expect("create db1");
    engine
        .execute(&mut session, "CREATE DATABASE db2")
        .expect("create db2");
    engine
        .execute(&mut session, "CREATE TABLE db1.t (k INT64, id INT64)")
        .expect("create db1.t");
    engine
        .execute(&mut session, "CREATE TABLE db2.t (k INT64, id INT64)")
        .expect("create db2.t");
    engine
        .execute(&mut session, "INSERT INTO db1.t VALUES (1, 10)")
        .expect("insert db1");
    engine
        .execute(&mut session, "INSERT INTO db2.t VALUES (1, 20)")
        .expect("insert db2");

    let result = engine
        .execute(
            &mut session,
            "SELECT a.id, b.id FROM db1.t a JOIN db2.t b ON a.k = b.k",
        )
        .expect("cross database join");
    match result {
        ExecResult::Query { batches, .. } => {
            assert_eq!(batches.len(), 1);
            assert_eq!(batches[0].num_rows(), 1);
            assert_eq!(batches[0].scalar(0, 0), Some(ScalarValue::Int64(Some(10))));
            assert_eq!(batches[0].scalar(0, 1), Some(ScalarValue::Int64(Some(20))));
        }
        other => panic!("expected query result, got {:?}", other),
    }

    let result = engine
        .execute(&mut session, "SELECT db1.t.id FROM db1.t")
        .expect("three part qualified column");
    match result {
        ExecResult::Query { batches, .. } => {
            assert_eq!(batches[0].scalar(0, 0), Some(ScalarValue::Int64(Some(10))));
        }
        other => panic!("expected query result, got {:?}", other),
    }

    engine.segment_mgr().shutdown();
}

#[test]
fn auto_increment_counters_are_restored_per_database() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    {
        let engine = Engine::new(temp_dir.path(), "main").expect("failed to create engine");
        let mut session = Session::new("root".to_string(), "main".to_string());

        engine
            .execute(&mut session, "CREATE DATABASE db1")
            .expect("create db1");
        engine
            .execute(
                &mut session,
                "CREATE TABLE db1.t (id INT64 AUTO_INCREMENT, value INT64)",
            )
            .expect("create db1.t");
        engine
            .execute(&mut session, "INSERT INTO db1.t (value) VALUES (10)")
            .expect("insert auto increment row");
        engine.flush_all_buffers().expect("flush inserted row");
        engine.segment_mgr().shutdown();
    }

    let engine = Engine::new(temp_dir.path(), "main").expect("restart engine");
    let counter = engine
        .auto_increment_manager()
        .get("db1", "t")
        .expect("auto increment counter should be restored");
    assert_eq!(counter.current_value(), 2);

    engine.segment_mgr().shutdown();
}

#[test]
fn drop_database_clears_database_scoped_runtime_state() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("failed to create engine");
    let mut session = Session::new("root".to_string(), "main".to_string());

    engine
        .execute(&mut session, "CREATE DATABASE db1")
        .expect("create db1");
    engine
        .execute(
            &mut session,
            "CREATE TABLE db1.t (id INT64 AUTO_INCREMENT, value INT64)",
        )
        .expect("create db1.t");
    engine
        .execute(&mut session, "INSERT INTO db1.t (value) VALUES (10)")
        .expect("insert auto increment row");

    assert!(engine.auto_increment_manager().get("db1", "t").is_some());
    assert!(!engine
        .memtable_manager()
        .get_by_table("db1", "t")
        .is_empty());

    engine
        .execute(&mut session, "DROP DATABASE db1")
        .expect("drop db1");

    assert!(!engine.database_exists("db1"));
    assert!(engine.auto_increment_manager().get("db1", "t").is_none());
    assert!(engine
        .memtable_manager()
        .get_by_table("db1", "t")
        .is_empty());
    assert!(!temp_dir.path().join("db1").exists());

    engine.segment_mgr().shutdown();
}

#[test]
fn non_admin_users_follow_database_table_permissions() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("failed to create engine");
    let mut root = Session::new("root".to_string(), "main".to_string());

    engine
        .execute(&mut root, "CREATE DATABASE db1")
        .expect("create db1");
    engine
        .execute(&mut root, "CREATE TABLE db1.t (id INT64)")
        .expect("create db1.t");
    engine
        .execute(&mut root, "INSERT INTO db1.t VALUES (7)")
        .expect("insert root row");
    engine
        .execute(&mut root, "CREATE USER alice IDENTIFIED BY 'pw'")
        .expect("create alice");

    let mut alice = Session::new("alice".to_string(), "main".to_string());
    assert!(engine.execute(&mut alice, "SELECT id FROM db1.t").is_err());
    assert!(engine
        .execute(&mut alice, "INSERT INTO db1.t VALUES (8)")
        .is_err());
    assert!(engine
        .execute(&mut alice, "CREATE TABLE db1.owned (id INT64)")
        .is_err());

    engine
        .execute(&mut root, "GRANT SELECT ON TABLE db1.t TO alice")
        .expect("grant select");
    let result = engine
        .execute(&mut alice, "SELECT id FROM db1.t")
        .expect("alice can select db1.t");
    match result {
        ExecResult::Query { batches, .. } => {
            assert_eq!(batches[0].scalar(0, 0), Some(ScalarValue::Int64(Some(7))));
        }
        other => panic!("expected query result, got {:?}", other),
    }

    engine
        .execute(&mut root, "GRANT INSERT ON TABLE db1.* TO alice")
        .expect("grant insert on db1 tables");
    engine
        .execute(&mut alice, "INSERT INTO db1.t VALUES (8)")
        .expect("alice can insert with db1 wildcard table permission");

    engine
        .execute(&mut root, "GRANT CREATE ON DATABASE db1 TO alice")
        .expect("grant create on db1");
    engine
        .execute(&mut alice, "CREATE TABLE db1.owned (id INT64)")
        .expect("alice can create table in db1");

    engine.segment_mgr().shutdown();
}
