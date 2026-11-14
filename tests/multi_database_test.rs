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
    assert!(
        engine
            .execute(&mut alice, "REPLACE INTO db1.t VALUES (8)")
            .is_err(),
        "REPLACE also requires DELETE permission"
    );
    engine
        .execute(&mut root, "GRANT DELETE ON TABLE db1.t TO alice")
        .expect("grant delete");
    engine
        .execute(&mut alice, "REPLACE INTO db1.t VALUES (9)")
        .expect("alice can replace after insert and delete grants");

    engine
        .execute(&mut root, "GRANT CREATE ON DATABASE db1 TO alice")
        .expect("grant create on db1");
    engine
        .execute(&mut alice, "CREATE TABLE db1.owned (id INT64)")
        .expect("alice can create table in db1");

    engine.segment_mgr().shutdown();
}

#[test]
fn subquery_tables_require_select_permission_on_every_read_path() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("failed to create engine");
    let mut root = Session::new("root".to_string(), "main".to_string());

    engine
        .execute(&mut root, "CREATE TABLE t (id INT64, v INT64)")
        .expect("create t");
    engine
        .execute(&mut root, "INSERT INTO t VALUES (1, 10), (2, 20)")
        .expect("seed t");
    engine
        .execute(&mut root, "CREATE TABLE secret (id INT64)")
        .expect("create secret");
    engine
        .execute(&mut root, "INSERT INTO secret VALUES (1)")
        .expect("seed secret");
    engine
        .execute(&mut root, "CREATE USER alice IDENTIFIED BY 'pw'")
        .expect("create alice");
    engine
        .execute(&mut root, "GRANT SELECT ON TABLE main.t TO alice")
        .expect("grant select on t only");
    engine
        .execute(&mut root, "GRANT UPDATE ON TABLE main.t TO alice")
        .expect("grant update on t only");
    engine
        .execute(&mut root, "GRANT DELETE ON TABLE main.t TO alice")
        .expect("grant delete on t only");

    let mut alice = Session::new("alice".to_string(), "main".to_string());

    // WHERE IN (SELECT ...): the subquery table must be denied even though
    // every FROM table is granted.
    assert!(
        engine
            .execute(
                &mut alice,
                "SELECT id FROM t WHERE id IN (SELECT id FROM secret)"
            )
            .is_err(),
        "IN-subquery on a non-granted table must be denied"
    );
    assert!(
        engine
            .execute(
                &mut alice,
                "SELECT id FROM t WHERE EXISTS (SELECT 1 FROM secret)"
            )
            .is_err(),
        "EXISTS-subquery on a non-granted table must be denied"
    );
    // Scalar subquery in the select list.
    assert!(
        engine
            .execute(&mut alice, "SELECT (SELECT max(id) FROM secret) FROM t")
            .is_err(),
        "scalar subquery in the select list must be denied"
    );
    // Derived table in FROM.
    assert!(
        engine
            .execute(&mut alice, "SELECT * FROM (SELECT id FROM secret) d")
            .is_err(),
        "derived table over a non-granted table must be denied"
    );
    // UPDATE reading a non-granted table through WHERE and SET.
    assert!(
        engine
            .execute(
                &mut alice,
                "UPDATE t SET v = 1 WHERE id IN (SELECT id FROM secret)"
            )
            .is_err(),
        "UPDATE WHERE-subquery on a non-granted table must be denied"
    );
    assert!(
        engine
            .execute(&mut alice, "UPDATE t SET v = (SELECT max(id) FROM secret)")
            .is_err(),
        "UPDATE SET-subquery on a non-granted table must be denied"
    );
    // DELETE reading a non-granted table.
    assert!(
        engine
            .execute(
                &mut alice,
                "DELETE FROM t WHERE id IN (SELECT id FROM secret)"
            )
            .is_err(),
        "DELETE WHERE-subquery on a non-granted table must be denied"
    );

    // Granting SELECT on the subquery table unblocks every path; the data
    // must still be correct afterwards.
    engine
        .execute(&mut root, "GRANT SELECT ON TABLE main.secret TO alice")
        .expect("grant select on secret");
    engine
        .execute(
            &mut alice,
            "SELECT id FROM t WHERE id IN (SELECT id FROM secret)",
        )
        .expect("IN-subquery allowed once the subquery table is granted");
    engine
        .execute(&mut alice, "SELECT * FROM (SELECT id FROM secret) d")
        .expect("derived table allowed once granted");
    engine
        .execute(&mut alice, "UPDATE t SET v = (SELECT max(id) FROM secret)")
        .expect("UPDATE SET-subquery allowed once granted");
    engine
        .execute(
            &mut alice,
            "UPDATE t SET v = 99 WHERE id IN (SELECT id FROM secret)",
        )
        .expect("UPDATE WHERE-subquery allowed once granted");
    engine
        .execute(
            &mut alice,
            "DELETE FROM t WHERE id IN (SELECT id FROM secret)",
        )
        .expect("DELETE WHERE-subquery allowed once granted");

    let result = engine
        .execute(&mut root, "SELECT v FROM t")
        .expect("root reads t");
    match result {
        ExecResult::Query { batches, .. } => {
            // The SET-subquery update wrote max(secret.id)=1 to both rows;
            // the WHERE-scoped update then set v=99 on the one row matching
            // secret (id=1) and the DELETE removed it, leaving (2, 1).
            assert_eq!(batches[0].num_rows(), 1);
            assert_eq!(batches[0].scalar(0, 0), Some(ScalarValue::Int64(Some(1))));
        }
        other => panic!("expected query result, got {:?}", other),
    }

    engine.segment_mgr().shutdown();
}

#[test]
fn drop_database_releases_hot_store_mirrors() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("failed to create engine");
    let mut session = Session::new("root".to_string(), "main".to_string());

    engine
        .execute(&mut session, "CREATE DATABASE db1")
        .expect("create db1");
    engine
        .execute(
            &mut session,
            "CREATE TABLE db1.ticks (
                security_id VARCHAR PRIMARY KEY,
                trade_time TIMESTAMP NOT NULL,
                value INT64
            ) CLUSTER BY (security_id) HOT BY (trade_time) INDEX (security_id)",
        )
        .expect("create hot table");
    engine
        .execute(
            &mut session,
            "INSERT INTO db1.ticks VALUES ('A', 1783492992000000, 1)",
        )
        .expect("insert hot row");
    assert!(engine.hot_store_manager().get("db1", "ticks").is_some());
    assert!(
        !engine.hot_store_manager().memory_snapshot().is_empty(),
        "hot mirror must be resident before the drop"
    );

    engine
        .execute(&mut session, "DROP DATABASE db1")
        .expect("drop db1");

    // Regression: DROP DATABASE used to leave every dropped table's hot
    // mirror (each allowed to grow to the full per-store memory limit)
    // resident until process restart.
    assert!(engine.hot_store_manager().get("db1", "ticks").is_none());
    assert!(engine.hot_store_manager().memory_snapshot().is_empty());

    engine.segment_mgr().shutdown();
}

#[test]
fn drop_database_with_primary_key_table_releases_index_state() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("failed to create engine");
    let mut session = Session::new("root".to_string(), "main".to_string());
    let create = "CREATE TABLE db1.ticks (
            security_id VARCHAR PRIMARY KEY,
            trade_time TIMESTAMP NOT NULL,
            value INT64
        ) CLUSTER BY (security_id) HOT BY (trade_time) INDEX (security_id)";

    engine
        .execute(&mut session, "CREATE DATABASE db1")
        .expect("create db1");
    engine.execute(&mut session, create).expect("create table");
    engine
        .execute(
            &mut session,
            "INSERT INTO db1.ticks VALUES ('A', 1783492992000000, 1)",
        )
        .expect("insert row");

    // Regression: the cached unique-index state pins an open handle on its
    // index base file. On Windows that handle made the staging rename of the
    // database directory fail with "access denied", so DROP DATABASE always
    // errored for any table with a PRIMARY KEY. DROP TABLE already released
    // the states before staging; DROP DATABASE did not.
    engine
        .execute(&mut session, "DROP DATABASE db1")
        .expect("drop db1");
    assert!(!temp_dir.path().join("db1").exists());

    // Recreating the same database and table must not inherit the dropped
    // index state: 'A' is gone with the old database, so it is insertable
    // again. A stale cached state would report a duplicate primary key.
    engine
        .execute(&mut session, "CREATE DATABASE db1")
        .expect("recreate db1");
    engine
        .execute(&mut session, create)
        .expect("recreate table");
    engine
        .execute(
            &mut session,
            "INSERT INTO db1.ticks VALUES ('A', 1783492992000000, 1)",
        )
        .expect("insert row into the recreated database");
    match engine
        .execute(&mut session, "SELECT COUNT(*) FROM db1.ticks")
        .expect("count rows")
    {
        ExecResult::Query { batches, .. } => {
            assert_eq!(batches[0].scalar(0, 0), Some(ScalarValue::Int64(Some(1))));
        }
        other => panic!("expected query result, got {:?}", other),
    }

    engine.segment_mgr().shutdown();
}

fn show_memory_rows(result: ExecResult) -> Vec<(String, String, i64)> {
    let ExecResult::Query { batches, .. } = result else {
        panic!("expected query result");
    };
    let mut rows = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            let component = match batch.scalar(row, 0) {
                Some(ScalarValue::VarChar(Some(v))) => v,
                other => panic!("expected component name, got {:?}", other),
            };
            let name = match batch.scalar(row, 1) {
                Some(ScalarValue::VarChar(Some(v))) => v,
                other => panic!("expected metric name, got {:?}", other),
            };
            let value = match batch.scalar(row, 2) {
                Some(ScalarValue::Int64(Some(v))) => v,
                other => panic!("expected int64 value, got {:?}", other),
            };
            rows.push((component, name, value));
        }
    }
    rows
}

#[test]
fn show_memory_reports_component_rows_and_requires_admin() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("failed to create engine");
    let mut session = Session::new("root".to_string(), "main".to_string());

    engine
        .execute(&mut session, "CREATE DATABASE db1")
        .expect("create db1");
    engine
        .execute(
            &mut session,
            "CREATE TABLE db1.ticks (
                security_id VARCHAR PRIMARY KEY,
                trade_time TIMESTAMP NOT NULL,
                value INT64
            ) CLUSTER BY (security_id) HOT BY (trade_time) INDEX (security_id)",
        )
        .expect("create hot table");
    engine
        .execute(
            &mut session,
            "INSERT INTO db1.ticks VALUES ('A', 1783492992000000, 1)",
        )
        .expect("insert hot row");

    let rows = show_memory_rows(
        engine
            .execute(&mut session, "SHOW MEMORY")
            .expect("show memory"),
    );
    let has =
        |component: &str, name: &str| rows.iter().any(|(c, n, _)| c == component && n == name);
    assert!(has("hot_store", "stores"), "rows: {rows:?}");
    assert!(has("hot_store", "db1.ticks"), "rows: {rows:?}");
    assert!(has("replication", "enabled"), "rows: {rows:?}");
    assert!(has("replication", "ring_max_bytes"), "rows: {rows:?}");
    assert!(has("segment_readers", "mutable_entries"), "rows: {rows:?}");
    assert!(
        has("segment_readers", "cache_budget_bytes"),
        "rows: {rows:?}"
    );
    assert!(
        has("segment_readers", "mutable_budget_bytes"),
        "rows: {rows:?}"
    );
    assert!(has("memtable", "estimated_bytes"), "rows: {rows:?}");

    engine
        .execute(&mut session, "CREATE USER bob IDENTIFIED BY 'pw'")
        .expect("create bob");
    let mut bob = Session::new("bob".to_string(), "main".to_string());
    assert!(
        engine.execute(&mut bob, "SHOW MEMORY").is_err(),
        "SHOW MEMORY must require global admin"
    );

    engine.segment_mgr().shutdown();
}

#[test]
fn show_workload_reports_admission_canary_and_fast_path_counters() {
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let engine = std::sync::Arc::new(Engine::new(temp_dir.path(), "main").expect("engine"));
    let mut session = Session::new("root".to_string(), "main".to_string());
    engine
        .execute(&mut session, "CREATE TABLE t_wl (id INT64)")
        .expect("create table");

    let rows = show_memory_rows(
        engine
            .execute(&mut session, "SHOW WORKLOAD")
            .expect("show workload"),
    );
    let value = |component: &str, name: &str| -> Option<i64> {
        rows.iter()
            .find(|(c, n, _)| c == component && n == name)
            .map(|(_, _, v)| *v)
    };
    // Admission pools must all be present with a capacity.
    for pool in ["query", "shared", "control"] {
        assert!(
            value("admission", &format!("{pool}.capacity")).is_some(),
            "missing {pool} capacity, rows: {rows:?}"
        );
    }
    // The canary has not run inside a bare engine test: status stays at
    // "never ran" with zero failures.
    assert_eq!(value("canary", "last_status"), Some(3));
    assert_eq!(value("canary", "consecutive_failures"), Some(0));
    assert!(value("pools", "rayon_global_threads").is_some());
    // The folded-answer counter is a process-global atomic reported by SHOW
    // WORKLOAD, so tests running in parallel inside this binary may already
    // have folded a metadata answer before this one starts. Assert the
    // increment this test causes instead of an absolute zero.
    let folded_baseline = value("fast_path", "metadata_answers_total").expect("fast path counter");

    // A metadata-foldable aggregate bumps the fast-path counter.
    engine
        .execute(&mut session, "INSERT INTO t_wl VALUES (1), (2), (3)")
        .expect("insert rows");
    engine
        .execute(&mut session, "SELECT MAX(id) FROM t_wl")
        .expect("foldable aggregate");
    let rows = show_memory_rows(
        engine
            .execute(&mut session, "SHOW WORKLOAD")
            .expect("show workload again"),
    );
    let folded = rows
        .iter()
        .find(|(c, n, _)| c == "fast_path" && n == "metadata_answers_total")
        .map(|(_, _, v)| *v)
        .expect("fast path counter");
    assert!(
        folded >= folded_baseline + 1,
        "MAX(id) should have folded to metadata, got {folded} (baseline {folded_baseline})"
    );

    engine.segment_mgr().shutdown();
}
