use std::sync::Arc;
use std::thread;

use tempfile::TempDir;

use taibai::net::session::Session;
use taibai::sql::execution::engine::Engine;

fn session() -> Session {
    Session::new("root".to_string(), "main".to_string())
}

#[test]
fn concurrent_insert_and_select_on_active_segment_is_stable() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Arc::new(Engine::new(temp_dir.path(), "main").expect("create engine"));
    let mut setup = session();
    engine
        .execute(&mut setup, "CREATE TABLE t (id INT32, value INT32)")
        .expect("create table");

    let writer_engine = Arc::clone(&engine);
    let writer = thread::spawn(move || {
        let mut s = session();
        for i in 0..80 {
            writer_engine
                .execute(&mut s, &format!("INSERT INTO t VALUES ({}, {})", i, i * 10))
                .expect("insert row");
        }
    });

    let reader_engine = Arc::clone(&engine);
    let reader = thread::spawn(move || {
        let mut s = session();
        for _ in 0..120 {
            reader_engine
                .execute(&mut s, "SELECT id, value FROM t")
                .expect("select rows while inserting");
        }
    });

    writer.join().expect("writer thread panicked");
    reader.join().expect("reader thread panicked");
    engine.segment_mgr().shutdown();
}

#[test]
fn concurrent_insert_and_delete_footer_rewrite_is_stable() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Arc::new(Engine::new(temp_dir.path(), "main").expect("create engine"));
    let mut setup = session();
    engine
        .execute(&mut setup, "CREATE TABLE t (id INT32, value INT32)")
        .expect("create table");

    let writer_engine = Arc::clone(&engine);
    let writer = thread::spawn(move || {
        let mut s = session();
        for i in 0..60 {
            writer_engine
                .execute(&mut s, &format!("INSERT INTO t VALUES ({}, {})", i, i * 10))
                .expect("insert row");
        }
    });

    let deleter_engine = Arc::clone(&engine);
    let deleter = thread::spawn(move || {
        let mut s = session();
        for i in 0..30 {
            deleter_engine
                .execute(&mut s, &format!("DELETE FROM t WHERE id = {}", i))
                .expect("delete row while inserting");
        }
    });

    writer.join().expect("writer thread panicked");
    deleter.join().expect("deleter thread panicked");
    engine.segment_mgr().shutdown();
}

#[test]
fn concurrent_insert_and_update_table_rewrite_is_stable() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Arc::new(Engine::new(temp_dir.path(), "main").expect("create engine"));
    let mut setup = session();
    engine
        .execute(&mut setup, "CREATE TABLE t (id INT32, value INT32)")
        .expect("create table");
    engine
        .execute(&mut setup, "INSERT INTO t VALUES (0, 0)")
        .expect("seed table");

    let writer_engine = Arc::clone(&engine);
    let writer = thread::spawn(move || {
        let mut s = session();
        for i in 1..50 {
            writer_engine
                .execute(&mut s, &format!("INSERT INTO t VALUES ({}, {})", i, i * 10))
                .expect("insert row");
        }
    });

    let updater_engine = Arc::clone(&engine);
    let updater = thread::spawn(move || {
        let mut s = session();
        for _ in 0..10 {
            updater_engine
                .execute(&mut s, "UPDATE t SET value = 1")
                .expect("update table while inserting");
        }
    });

    writer.join().expect("writer thread panicked");
    updater.join().expect("updater thread panicked");
    engine.segment_mgr().shutdown();
}
