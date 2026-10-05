//! GRANT/REVOKE REPLICATION and the ADD PARTITION rejection.
//!
//! REPLICATION guards REPL_HELLO, which checks the Global scope; these tests
//! pin the end-to-end semantics: unqualified and wildcard grants normalize to
//! Global, scoped objects are rejected, and revoke removes the grant.

use tempfile::TempDir;

use taibai::net::session::Session;
use taibai::sql::execution::engine::Engine;
use taibai::storage::catalog::auth::{PermissionObjectType, PermissionType};

fn create_engine() -> (TempDir, Engine, Session) {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("failed to create engine");
    let session = Session::new("root".to_string(), "main".to_string());
    (temp_dir, engine, session)
}

fn holds_replication(engine: &Engine, user: &str) -> bool {
    engine.auth_manager().check_permission(
        user,
        PermissionType::Replication,
        PermissionObjectType::Global,
        "*",
    )
}

#[test]
fn grant_replication_without_on_authorizes_global_stream() {
    let (_temp_dir, engine, mut session) = create_engine();
    engine
        .execute(&mut session, "CREATE USER repl IDENTIFIED BY 'pw'")
        .unwrap();
    engine
        .execute(&mut session, "GRANT REPLICATION TO repl")
        .unwrap();
    assert!(holds_replication(&engine, "repl"));
}

#[test]
fn grant_replication_wildcard_object_is_normalized_to_global() {
    let (_temp_dir, engine, mut session) = create_engine();
    engine
        .execute(&mut session, "CREATE USER repl IDENTIFIED BY 'pw'")
        .unwrap();
    engine
        .execute(&mut session, "GRANT REPLICATION ON *.* TO repl")
        .unwrap();
    assert!(holds_replication(&engine, "repl"));
}

#[test]
fn grant_replication_on_scoped_object_is_rejected() {
    let (_temp_dir, engine, mut session) = create_engine();
    engine
        .execute(&mut session, "CREATE USER repl IDENTIFIED BY 'pw'")
        .unwrap();
    for sql in [
        "GRANT REPLICATION ON TABLE main.ticks TO repl",
        "GRANT REPLICATION ON DATABASE main TO repl",
    ] {
        let err = engine
            .execute(&mut session, sql)
            .expect_err("scoped REPLICATION grant must fail");
        assert!(
            err.to_string().contains("global privilege"),
            "{sql} -> {err}"
        );
    }
    assert!(!holds_replication(&engine, "repl"));
}

#[test]
fn revoke_replication_removes_the_global_grant() {
    let (_temp_dir, engine, mut session) = create_engine();
    engine
        .execute(&mut session, "CREATE USER repl IDENTIFIED BY 'pw'")
        .unwrap();
    engine
        .execute(&mut session, "GRANT REPLICATION TO repl")
        .unwrap();
    engine
        .execute(&mut session, "REVOKE REPLICATION FROM repl")
        .unwrap();
    assert!(!holds_replication(&engine, "repl"));
}

#[test]
fn alter_table_add_partition_is_rejected_with_clear_error() {
    let (_temp_dir, engine, mut session) = create_engine();
    let err = engine
        .execute(&mut session, "ALTER TABLE ticks ADD PARTITION p20240101")
        .expect_err("ADD PARTITION must be rejected");
    let message = err.to_string();
    assert!(
        message.contains("ADD PARTITION is not supported"),
        "{message}"
    );
    assert!(
        message.contains("DROP PARTITION") && message.contains("TRUNCATE PARTITION"),
        "error should point at the supported alternatives: {message}"
    );
}
