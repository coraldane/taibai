use super::*;
use crate::net::session::Session;
use std::cell::Cell;
use std::sync::{Arc, Barrier};
use tempfile::TempDir;

fn scalar_count(engine: &Engine, session: &mut Session, sql: &str) -> i64 {
    match engine.execute(session, sql).expect("execute count query") {
        ExecResult::Query { batches, .. } => match batches[0].scalar(0, 0) {
            Some(ScalarValue::Int64(Some(value))) => value,
            other => panic!("unexpected count value: {other:?}"),
        },
        other => panic!("unexpected count result: {other:?}"),
    }
}

fn partition_segment_exists(table_dir: &Path, partition_key: &str) -> bool {
    std::fs::read_dir(table_dir)
        .expect("read table directory")
        .filter_map(|entry| entry.ok())
        .any(|entry| {
            entry
                .file_name()
                .to_str()
                .and_then(crate::storage::partition::parse_partition_key)
                .as_deref()
                == Some(partition_key)
        })
}

#[test]
fn delete_retries_transient_errors_until_success() {
    let attempts = Cell::new(0);
    retry_delete("test delete", Path::new("test-path"), |_| {
        let attempt = attempts.get() + 1;
        attempts.set(attempt);
        if attempt < 3 {
            Err(io::Error::new(io::ErrorKind::Interrupted, "busy"))
        } else {
            Ok(())
        }
    })
    .unwrap();
    assert_eq!(attempts.get(), 3);
}

#[test]
fn delete_returns_error_after_retries_are_exhausted() {
    let attempts = Cell::new(0);
    let error = retry_delete("test delete", Path::new("test-path"), |_| {
        attempts.set(attempts.get() + 1);
        Err(io::Error::new(io::ErrorKind::PermissionDenied, "busy"))
    })
    .unwrap_err();
    assert_eq!(attempts.get(), FILE_DELETE_MAX_ATTEMPTS);
    assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    assert!(error.to_string().contains("failed after 5 attempt(s)"));
}

#[test]
fn delete_treats_not_found_as_success() {
    let attempts = Cell::new(0);
    retry_delete("test delete", Path::new("test-path"), |_| {
        attempts.set(attempts.get() + 1);
        Err(io::Error::new(io::ErrorKind::NotFound, "missing"))
    })
    .unwrap();
    assert_eq!(attempts.get(), 1);
}

#[test]
fn delete_does_not_retry_permanent_errors() {
    let attempts = Cell::new(0);
    let error = retry_delete("test delete", Path::new("test-path"), |_| {
        attempts.set(attempts.get() + 1);
        Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid"))
    })
    .unwrap_err();
    assert_eq!(attempts.get(), 1);
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
}

#[test]
fn drop_table_staging_failure_keeps_catalog_entry() {
    let temp_dir = TempDir::new().expect("create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("create engine");
    let mut session = Session::new("root".to_string(), "main".to_string());

    engine
        .execute(&mut session, "CREATE TABLE guarded_drop (id INT64)")
        .expect("create table");
    let table_dir = temp_dir.path().join("main").join("guarded_drop");
    std::fs::remove_dir(&table_dir).expect("remove empty table directory");
    std::fs::write(&table_dir, b"not a directory").expect("replace table path with a file");

    let error = engine
        .execute(&mut session, "DROP TABLE guarded_drop")
        .expect_err("drop must fail before changing the catalog");
    assert!(
        error.to_string().contains("is not a directory"),
        "unexpected drop error: {error}"
    );
    assert!(engine.get_table_meta("main", "guarded_drop").is_some());

    std::fs::remove_file(&table_dir).expect("remove invalid table path");
    engine
        .execute(&mut session, "DROP TABLE guarded_drop")
        .expect("retry drop");
    assert!(engine.get_table_meta("main", "guarded_drop").is_none());
    engine.segment_mgr().shutdown();
}

#[test]
fn drop_table_removes_two_hundred_partition_files() {
    let temp_dir = TempDir::new().expect("create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("create engine");
    let mut session = Session::new("root".to_string(), "main".to_string());

    engine
        .execute(
            &mut session,
            "CREATE TABLE partitioned_drop (id INT64) PARTITION BY id",
        )
        .expect("create partitioned table");
    let database_dir = temp_dir.path().join("main");
    let table_dir = database_dir.join("partitioned_drop");
    for partition in 0..200 {
        let path = table_dir.join(format!("p{partition}_0001.dat"));
        std::fs::write(path, partition.to_string()).expect("write partition fixture");
    }

    engine
        .execute(&mut session, "DROP TABLE partitioned_drop")
        .expect("drop partitioned table");
    assert!(!table_dir.exists());
    assert!(engine.get_table_meta("main", "partitioned_drop").is_none());
    assert!(
        !database_dir
            .join(DROP_TOMBSTONE_DIR)
            .join("partitioned_drop")
            .exists(),
        "drop tombstone entry should be removed after a successful cleanup"
    );
    // The tombstone ROOT persists: removing it races with concurrent DROPs
    // that are between create_dir_all and their staging rename.
    engine.segment_mgr().shutdown();
}

#[test]
fn engine_startup_removes_interrupted_drop_tombstones() {
    let temp_dir = TempDir::new().expect("create temp dir");
    let tombstone = temp_dir
        .path()
        .join("main")
        .join(DROP_TOMBSTONE_DIR)
        .join("interrupted");
    std::fs::create_dir_all(&tombstone).expect("create tombstone");
    std::fs::write(tombstone.join("p1_0001.dat"), b"stale").expect("write stale data");

    let engine = Engine::new(temp_dir.path(), "main").expect("create engine");
    assert!(!tombstone.exists());
    engine.segment_mgr().shutdown();
}

#[test]
fn engine_startup_restores_uncommitted_drop_tombstone() {
    let temp_dir = TempDir::new().expect("create temp dir");
    let table_dir = temp_dir.path().join("main").join("recover_me");
    {
        let engine = Engine::new(temp_dir.path(), "main").expect("create engine");
        let mut session = Session::new("root".to_string(), "main".to_string());
        engine
            .execute(&mut session, "CREATE TABLE recover_me (id INT64)")
            .expect("create table");
        engine.segment_mgr().shutdown();
    }

    let tombstone = temp_dir
        .path()
        .join("main")
        .join(DROP_TOMBSTONE_DIR)
        .join("recover_me");
    std::fs::create_dir_all(tombstone.parent().unwrap()).expect("create tombstone root");
    std::fs::rename(&table_dir, &tombstone).expect("simulate crash after staging");

    let engine = Engine::new(temp_dir.path(), "main").expect("restart engine");
    assert!(table_dir.exists());
    assert!(!tombstone.exists());
    assert!(engine.get_table_meta("main", "recover_me").is_some());
    engine.segment_mgr().shutdown();
}

#[test]
fn drop_table_reports_committed_physical_cleanup_failure_to_client() {
    let temp_dir = TempDir::new().expect("create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("create engine");
    let mut session = Session::new("root".to_string(), "main".to_string());
    engine
        .execute(&mut session, "CREATE TABLE cleanup_error (id INT64)")
        .expect("create table");

    FORCE_DROP_CLEANUP_FAILURE.with(|force| force.set(true));
    let error = engine
        .execute(&mut session, "DROP TABLE cleanup_error")
        .expect_err("physical cleanup failure must reach the client");
    let message = error.to_string();
    assert!(
        message.contains("was removed from the catalog"),
        "{message}"
    );
    assert!(
        message.contains("physical data cleanup failed"),
        "{message}"
    );
    assert!(
        message.contains("retried when the engine restarts"),
        "{message}"
    );
    assert!(engine.get_table_meta("main", "cleanup_error").is_none());
    assert!(temp_dir
        .path()
        .join("main")
        .join(DROP_TOMBSTONE_DIR)
        .join("cleanup_error")
        .exists());
    engine.segment_mgr().shutdown();
}

#[test]
fn drop_database_reports_cleanup_failure_and_retry_finishes_cleanup() {
    let temp_dir = TempDir::new().expect("create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("create engine");
    let mut session = Session::new("root".to_string(), "main".to_string());
    engine
        .execute(&mut session, "CREATE DATABASE doomed")
        .expect("create database");
    engine
        .execute(&mut session, "CREATE TABLE doomed.t (id INT64)")
        .expect("create table");

    FORCE_DDL_CLEANUP_FAILURE.with(|force| force.set(true));
    let error = engine
        .execute(&mut session, "DROP DATABASE doomed")
        .expect_err("cleanup failure must reach client");
    let message = error.to_string();
    assert!(message.contains("removed from the catalog"), "{message}");
    assert!(
        message.contains("physical data cleanup failed"),
        "{message}"
    );
    assert!(!engine.database_exists("doomed"));

    engine
        .execute(&mut session, "DROP DATABASE doomed")
        .expect("retry pending cleanup");
    assert!(
        !temp_dir
            .path()
            .join(DATABASE_DROP_TOMBSTONE_DIR)
            .join("doomed")
            .exists(),
        "database tombstone entry should be removed after the retry"
    );
    engine.segment_mgr().shutdown();
}

#[test]
fn drop_database_cancels_detached_memtable_flushes() {
    let temp_dir = TempDir::new().expect("create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("create engine");
    let mut session = Session::new("root".to_string(), "main".to_string());
    engine
        .execute(&mut session, "CREATE DATABASE drop_in_flight")
        .expect("create database");
    engine
        .execute(&mut session, "CREATE TABLE drop_in_flight.t (id INT64)")
        .expect("create table");
    engine
        .execute(&mut session, "INSERT INTO drop_in_flight.t VALUES (1)")
        .expect("insert pending row");

    let memtable = engine
        .batch_writer()
        .memtable_manager()
        .get_by_table("drop_in_flight", "t")
        .into_iter()
        .next()
        .expect("pending memtable");
    memtable.set_state(crate::storage::core::memtable::MemTableState::Flushing);
    let detached_batch = memtable.read_snapshot().expect("detached flush snapshot");

    engine
        .execute(&mut session, "DROP DATABASE drop_in_flight")
        .expect("drop database during detached flush");

    let wrote = engine
        .segment_mgr()
        .flush_memtable_batch_if(
            "drop_in_flight",
            "t",
            "",
            &detached_batch,
            0,
            || memtable.state() == crate::storage::core::memtable::MemTableState::Flushing,
            || {},
        )
        .expect("cancel detached database flush");
    assert!(!wrote);
    assert!(!temp_dir.path().join("drop_in_flight").exists());
    engine.segment_mgr().shutdown();
}

#[test]
fn truncate_cleanup_failure_keeps_empty_table_and_is_recovered_on_restart() {
    let temp_dir = TempDir::new().expect("create temp dir");
    {
        let engine = Engine::new(temp_dir.path(), "main").expect("create engine");
        let mut session = Session::new("root".to_string(), "main".to_string());
        engine
            .execute(&mut session, "CREATE TABLE truncate_me (id INT64)")
            .expect("create table");
        engine
            .execute(&mut session, "INSERT INTO truncate_me VALUES (1), (2)")
            .expect("insert rows");

        FORCE_DDL_CLEANUP_FAILURE.with(|force| force.set(true));
        let error = engine
            .execute(&mut session, "TRUNCATE TABLE truncate_me")
            .expect_err("cleanup failure must reach client");
        assert!(error.to_string().contains("TRUNCATE TABLE"));
        assert_eq!(
            scalar_count(&engine, &mut session, "SELECT COUNT(*) FROM truncate_me"),
            0
        );
        assert!(temp_dir
            .path()
            .join("main")
            .join(TRUNCATE_TOMBSTONE_DIR)
            .join("truncate_me")
            .exists());
        engine.segment_mgr().shutdown();
    }

    let engine = Engine::new(temp_dir.path(), "main").expect("restart engine");
    let mut session = Session::new("root".to_string(), "main".to_string());
    assert_eq!(
        scalar_count(&engine, &mut session, "SELECT COUNT(*) FROM truncate_me"),
        0
    );
    assert!(!temp_dir
        .path()
        .join("main")
        .join(TRUNCATE_TOMBSTONE_DIR)
        .exists());
    engine.segment_mgr().shutdown();
}

#[test]
fn drop_partition_matches_exact_partition_key() {
    let temp_dir = TempDir::new().expect("create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("create engine");
    let mut session = Session::new("root".to_string(), "main".to_string());
    engine
        .execute(
            &mut session,
            "CREATE TABLE partition_keys (id INT64) PARTITION BY id",
        )
        .expect("create table");
    engine
        .execute(&mut session, "INSERT INTO partition_keys VALUES (1), (10)")
        .expect("insert partitions");
    engine
        .execute(&mut session, "FLUSH TABLE partition_keys")
        .expect("flush table");

    exec_partition_change(&engine, "main", "partition_keys", "1", true)
        .expect("drop exact partition");
    assert_eq!(
        scalar_count(
            &engine,
            &mut session,
            "SELECT COUNT(*) FROM partition_keys WHERE id = 1"
        ),
        0
    );
    assert_eq!(
        scalar_count(
            &engine,
            &mut session,
            "SELECT COUNT(*) FROM partition_keys WHERE id = 10"
        ),
        1
    );
    engine.segment_mgr().shutdown();
}

#[test]
fn alter_table_drop_partition_accepts_storage_name_and_removes_files() {
    let temp_dir = TempDir::new().expect("create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("create engine");
    let mut session = Session::new("root".to_string(), "main".to_string());
    engine
        .execute(
            &mut session,
            "CREATE TABLE drop_partition_sql (id INT64) PARTITION BY id",
        )
        .expect("create table");
    engine
        .execute(
            &mut session,
            "INSERT INTO drop_partition_sql VALUES (1), (10)",
        )
        .expect("insert partitions");
    engine
        .execute(&mut session, "FLUSH TABLE drop_partition_sql")
        .expect("flush table");

    let table_dir = temp_dir.path().join("main").join("drop_partition_sql");
    assert!(partition_segment_exists(&table_dir, "1"));
    assert!(partition_segment_exists(&table_dir, "10"));

    engine
        .execute(
            &mut session,
            "ALTER TABLE drop_partition_sql DROP PARTITION p1",
        )
        .expect("drop p-prefixed partition");

    assert!(!partition_segment_exists(&table_dir, "1"));
    assert!(partition_segment_exists(&table_dir, "10"));
    assert_eq!(
        scalar_count(
            &engine,
            &mut session,
            "SELECT COUNT(*) FROM drop_partition_sql WHERE id = 1"
        ),
        0
    );
    assert_eq!(
        scalar_count(
            &engine,
            &mut session,
            "SELECT COUNT(*) FROM drop_partition_sql WHERE id = 10"
        ),
        1
    );
    engine.segment_mgr().shutdown();
}

#[test]
fn alter_table_truncate_partition_accepts_storage_name_and_removes_files() {
    let temp_dir = TempDir::new().expect("create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("create engine");
    let mut session = Session::new("root".to_string(), "main".to_string());
    engine
        .execute(
            &mut session,
            "CREATE TABLE truncate_partition_sql (id INT64) PARTITION BY id",
        )
        .expect("create table");
    engine
        .execute(
            &mut session,
            "INSERT INTO truncate_partition_sql VALUES (1), (10)",
        )
        .expect("insert partitions");
    engine
        .execute(&mut session, "FLUSH TABLE truncate_partition_sql")
        .expect("flush table");

    let table_dir = temp_dir.path().join("main").join("truncate_partition_sql");
    assert!(partition_segment_exists(&table_dir, "1"));
    assert!(partition_segment_exists(&table_dir, "10"));

    engine
        .execute(
            &mut session,
            "ALTER TABLE truncate_partition_sql TRUNCATE PARTITION p1",
        )
        .expect("truncate p-prefixed partition");

    assert!(!partition_segment_exists(&table_dir, "1"));
    assert!(partition_segment_exists(&table_dir, "10"));
    assert_eq!(
        scalar_count(
            &engine,
            &mut session,
            "SELECT COUNT(*) FROM truncate_partition_sql WHERE id = 1"
        ),
        0
    );
    assert_eq!(
        scalar_count(
            &engine,
            &mut session,
            "SELECT COUNT(*) FROM truncate_partition_sql WHERE id = 10"
        ),
        1
    );
    engine.segment_mgr().shutdown();
}

#[test]
fn truncate_partition_clears_only_target_partition_memtables() {
    let temp_dir = TempDir::new().expect("create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("create engine");
    let mut session = Session::new("root".to_string(), "main".to_string());
    engine
        .execute(
            &mut session,
            "CREATE TABLE truncate_partition_memtables (id INT64) PARTITION BY id",
        )
        .expect("create table");
    engine
        .execute(
            &mut session,
            "INSERT INTO truncate_partition_memtables VALUES (1), (10)",
        )
        .expect("insert pending partitions");

    engine
        .execute(
            &mut session,
            "ALTER TABLE truncate_partition_memtables TRUNCATE PARTITION p1",
        )
        .expect("truncate pending partition");

    assert_eq!(
        scalar_count(
            &engine,
            &mut session,
            "SELECT COUNT(*) FROM truncate_partition_memtables WHERE id = 1"
        ),
        0
    );
    assert_eq!(
        scalar_count(
            &engine,
            &mut session,
            "SELECT COUNT(*) FROM truncate_partition_memtables WHERE id = 10"
        ),
        1
    );

    engine
        .execute(&mut session, "FLUSH TABLE truncate_partition_memtables")
        .expect("flush surviving partition");
    assert_eq!(
        scalar_count(
            &engine,
            &mut session,
            "SELECT COUNT(*) FROM truncate_partition_memtables WHERE id = 1"
        ),
        0
    );
    assert_eq!(
        scalar_count(
            &engine,
            &mut session,
            "SELECT COUNT(*) FROM truncate_partition_memtables WHERE id = 10"
        ),
        1
    );
    engine.segment_mgr().shutdown();
}

#[test]
fn truncate_partition_cancels_detached_in_flight_memtable_flush() {
    let temp_dir = TempDir::new().expect("create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("create engine");
    let mut session = Session::new("root".to_string(), "main".to_string());
    engine
        .execute(
            &mut session,
            "CREATE TABLE truncate_partition_in_flight (id INT64) PARTITION BY id",
        )
        .expect("create table");
    engine
        .execute(
            &mut session,
            "INSERT INTO truncate_partition_in_flight VALUES (1)",
        )
        .expect("insert pending partition");

    let memtable = engine
        .batch_writer()
        .memtable_manager()
        .get_by_table("main", "truncate_partition_in_flight")
        .into_iter()
        .next()
        .expect("pending memtable");
    memtable.set_state(crate::storage::core::memtable::MemTableState::Flushing);
    let detached_batch = memtable.read_snapshot().expect("detached flush snapshot");

    engine
        .execute(
            &mut session,
            "ALTER TABLE truncate_partition_in_flight TRUNCATE PARTITION p1",
        )
        .expect("truncate while flush is detached");

    let wrote = engine
        .segment_mgr()
        .flush_memtable_batch_if(
            "main",
            "truncate_partition_in_flight",
            "1",
            &detached_batch,
            0,
            || memtable.state() == crate::storage::core::memtable::MemTableState::Flushing,
            || {},
        )
        .expect("detached flush cancellation");
    assert!(!wrote);
    assert_eq!(
        scalar_count(
            &engine,
            &mut session,
            "SELECT COUNT(*) FROM truncate_partition_in_flight WHERE id = 1"
        ),
        0
    );
    engine.segment_mgr().shutdown();
}

#[test]
fn truncate_partition_flushes_bulk_import_state_for_surviving_partitions() {
    let temp_dir = TempDir::new().expect("create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("create engine");
    let mut session = Session::new("root".to_string(), "main".to_string());
    engine
        .execute(
            &mut session,
            "CREATE TABLE truncate_partition_bulk (id INT64) PARTITION BY id",
        )
        .expect("create table");

    engine.begin_bulk_import();
    session.bulk_import_mode = true;
    engine
        .execute(
            &mut session,
            "INSERT INTO truncate_partition_bulk VALUES (1)",
        )
        .expect("bulk insert target partition");
    engine
        .execute(
            &mut session,
            "INSERT INTO truncate_partition_bulk VALUES (10)",
        )
        .expect("bulk insert surviving partition");

    let table_dir = temp_dir.path().join("main").join("truncate_partition_bulk");
    assert!(partition_segment_exists(&table_dir, "1"));
    assert!(partition_segment_exists(&table_dir, "10"));

    engine
        .execute(
            &mut session,
            "ALTER TABLE truncate_partition_bulk TRUNCATE PARTITION p1",
        )
        .expect("truncate partition during bulk import");

    assert!(!partition_segment_exists(&table_dir, "1"));
    assert!(partition_segment_exists(&table_dir, "10"));
    assert_eq!(
        engine
            .segment_mgr()
            .active_partition_keys("main", "truncate_partition_bulk"),
        vec!["10".to_string()],
        "truncating p1 must not seal the surviving partition writer"
    );
    let surviving_segments =
        crate::storage::segment_manager::SegmentManager::discover_segments(&table_dir)
            .expect("discover surviving segments");
    let surviving_rows = surviving_segments
        .iter()
        .map(|path| {
            engine
                .segment_mgr()
                .open_reader_cached(path)
                .expect("open surviving segment")
                .row_count()
        })
        .sum::<u64>();
    assert_eq!(surviving_rows, 1);

    session.bulk_import_mode = false;
    engine.end_bulk_import();
    engine
        .flush_all_buffers()
        .expect("finish bulk import flush");
    assert_eq!(
        scalar_count(
            &engine,
            &mut session,
            "SELECT COUNT(*) FROM truncate_partition_bulk WHERE id = 1"
        ),
        0
    );
    assert_eq!(
        scalar_count(
            &engine,
            &mut session,
            "SELECT COUNT(*) FROM truncate_partition_bulk WHERE id = 10"
        ),
        1
    );
    engine.segment_mgr().shutdown();
}

#[test]
fn alter_table_partition_change_rejects_unknown_partition() {
    let temp_dir = TempDir::new().expect("create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("create engine");
    let mut session = Session::new("root".to_string(), "main".to_string());
    engine
        .execute(
            &mut session,
            "CREATE TABLE unknown_partition (id INT64) PARTITION BY id",
        )
        .expect("create table");
    engine
        .execute(&mut session, "INSERT INTO unknown_partition VALUES (1)")
        .expect("insert partition");

    let error = engine
        .execute(
            &mut session,
            "ALTER TABLE unknown_partition DROP PARTITION p2",
        )
        .expect_err("unknown partition must fail");
    assert!(error.to_string().contains("partition 'p2' does not exist"));
    assert_eq!(
        scalar_count(
            &engine,
            &mut session,
            "SELECT COUNT(*) FROM unknown_partition"
        ),
        1
    );
    assert!(!temp_dir
        .path()
        .join("main")
        .join(PARTITION_TOMBSTONE_DIR)
        .exists());
    engine.segment_mgr().shutdown();
}

#[test]
fn rename_destination_failure_keeps_old_catalog_and_directory() {
    let temp_dir = TempDir::new().expect("create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("create engine");
    let mut session = Session::new("root".to_string(), "main".to_string());
    engine
        .execute(&mut session, "CREATE TABLE old_name (id INT64)")
        .expect("create table");
    let database_dir = temp_dir.path().join("main");
    let destination = database_dir.join("new_name");
    std::fs::create_dir_all(&destination).expect("create conflicting destination");

    engine
        .execute(&mut session, "ALTER TABLE old_name RENAME TO new_name")
        .expect_err("rename must fail before catalog mutation");
    assert!(engine.get_table_meta("main", "old_name").is_some());
    assert!(engine.get_table_meta("main", "new_name").is_none());
    assert!(database_dir.join("old_name").exists());

    std::fs::remove_dir(&destination).expect("remove destination conflict");
    engine
        .execute(&mut session, "ALTER TABLE old_name RENAME TO new_name")
        .expect("rename table");
    assert!(engine.get_table_meta("main", "old_name").is_none());
    assert!(engine.get_table_meta("main", "new_name").is_some());
    assert!(destination.exists());
    engine.segment_mgr().shutdown();
}

#[test]
fn engine_startup_rolls_back_uncommitted_rename() {
    let temp_dir = TempDir::new().expect("create temp dir");
    {
        let engine = Engine::new(temp_dir.path(), "main").expect("create engine");
        let mut session = Session::new("root".to_string(), "main".to_string());
        engine
            .execute(&mut session, "CREATE TABLE rename_old (id INT64)")
            .expect("create table");
        engine.segment_mgr().shutdown();
    }
    let database_dir = temp_dir.path().join("main");
    let old_dir = database_dir.join("rename_old");
    let new_dir = database_dir.join("rename_new");
    let operation_dir = database_dir.join(RENAME_TOMBSTONE_DIR).join("rename_old");
    std::fs::create_dir_all(&operation_dir).expect("create rename journal");
    std::fs::write(operation_dir.join(RENAME_TARGET_FILE), b"rename_new")
        .expect("write rename target");
    std::fs::rename(&old_dir, &new_dir).expect("simulate directory rename");

    let engine = Engine::new(temp_dir.path(), "main").expect("restart engine");
    assert!(old_dir.exists());
    assert!(!new_dir.exists());
    assert!(engine.get_table_meta("main", "rename_old").is_some());
    engine.segment_mgr().shutdown();
}

#[test]
fn engine_startup_restores_uncommitted_database_drop() {
    let temp_dir = TempDir::new().expect("create temp dir");
    {
        let engine = Engine::new(temp_dir.path(), "main").expect("create engine");
        let mut session = Session::new("root".to_string(), "main".to_string());
        engine
            .execute(&mut session, "CREATE DATABASE recover_db")
            .expect("create database");
        engine.segment_mgr().shutdown();
    }
    let database_dir = temp_dir.path().join("recover_db");
    let tombstone = temp_dir
        .path()
        .join(DATABASE_DROP_TOMBSTONE_DIR)
        .join("recover_db");
    std::fs::create_dir_all(tombstone.parent().unwrap()).expect("create database tombstones");
    std::fs::rename(&database_dir, &tombstone).expect("simulate staged database drop");

    let engine = Engine::new(temp_dir.path(), "main").expect("restart engine");
    assert!(database_dir.exists());
    assert!(engine.database_exists("recover_db"));
    engine.segment_mgr().shutdown();
}

#[test]
fn engine_startup_rolls_back_uncommitted_truncate() {
    let temp_dir = TempDir::new().expect("create temp dir");
    let table_dir = temp_dir.path().join("main").join("truncate_recovery");
    let operation_dir = temp_dir
        .path()
        .join("main")
        .join(TRUNCATE_TOMBSTONE_DIR)
        .join("truncate_recovery");
    {
        let engine = Engine::new(temp_dir.path(), "main").expect("create engine");
        let mut session = Session::new("root".to_string(), "main".to_string());
        engine
            .execute(&mut session, "CREATE TABLE truncate_recovery (id INT64)")
            .expect("create table");
        engine
            .execute(&mut session, "INSERT INTO truncate_recovery VALUES (7)")
            .expect("insert row");
        engine
            .execute(&mut session, "FLUSH TABLE truncate_recovery")
            .expect("flush table");
        let before_meta = engine
            .get_table_meta("main", "truncate_recovery")
            .expect("table metadata");
        engine
            .segment_mgr()
            .remove_table_state("main", "truncate_recovery");
        engine.segment_mgr().invalidate_readers_under(&table_dir);
        write_before_meta(&operation_dir, &before_meta).expect("write truncate journal");
        std::fs::create_dir_all(operation_dir.join(FRESH_DATA_DIR)).expect("create fresh data");
        std::fs::rename(&table_dir, operation_dir.join(STAGED_DATA_DIR))
            .expect("stage old table data");
        engine.segment_mgr().shutdown();
    }

    let engine = Engine::new(temp_dir.path(), "main").expect("restart engine");
    let mut session = Session::new("root".to_string(), "main".to_string());
    assert_eq!(
        scalar_count(
            &engine,
            &mut session,
            "SELECT COUNT(*) FROM truncate_recovery"
        ),
        1
    );
    assert!(!operation_dir.exists());
    engine.segment_mgr().shutdown();
}

#[test]
fn engine_startup_rolls_back_uncommitted_partition_drop() {
    let temp_dir = TempDir::new().expect("create temp dir");
    let before_meta;
    {
        let engine = Engine::new(temp_dir.path(), "main").expect("create engine");
        let mut session = Session::new("root".to_string(), "main".to_string());
        engine
            .execute(
                &mut session,
                "CREATE TABLE partition_recovery (id INT64) PARTITION BY id",
            )
            .expect("create table");
        engine
            .execute(
                &mut session,
                "INSERT INTO partition_recovery VALUES (1), (2)",
            )
            .expect("insert partitions");
        engine
            .execute(&mut session, "FLUSH TABLE partition_recovery")
            .expect("flush table");
        before_meta = engine
            .get_table_meta("main", "partition_recovery")
            .expect("table metadata");
        engine.segment_mgr().shutdown();
    }

    let table_dir = temp_dir.path().join("main").join("partition_recovery");
    let operation_dir = temp_dir
        .path()
        .join("main")
        .join(PARTITION_TOMBSTONE_DIR)
        .join("partition_recovery");
    write_before_meta(&operation_dir, &before_meta).expect("write partition journal");
    stage_partition_files(&operation_dir, &table_dir, "1").expect("stage partition files");

    let engine = Engine::new(temp_dir.path(), "main").expect("restart engine");
    let mut session = Session::new("root".to_string(), "main".to_string());
    assert_eq!(
        scalar_count(
            &engine,
            &mut session,
            "SELECT COUNT(*) FROM partition_recovery WHERE id = 1"
        ),
        1
    );
    assert_eq!(
        scalar_count(
            &engine,
            &mut session,
            "SELECT COUNT(*) FROM partition_recovery WHERE id = 2"
        ),
        1
    );
    assert!(!operation_dir.exists());
    engine.segment_mgr().shutdown();
}

#[test]
fn engine_startup_fails_when_cataloged_table_cannot_be_restored() {
    let temp_dir = TempDir::new().expect("create temp dir");
    {
        let engine = Engine::new(temp_dir.path(), "main").expect("create engine");
        let mut session = Session::new("root".to_string(), "main".to_string());
        engine
            .execute(&mut session, "CREATE TABLE blocked_restore (id INT64)")
            .expect("create table");
        engine.segment_mgr().shutdown();
    }
    let table_dir = temp_dir.path().join("main").join("blocked_restore");
    let tombstone = temp_dir
        .path()
        .join("main")
        .join(DROP_TOMBSTONE_DIR)
        .join("blocked_restore");
    std::fs::create_dir_all(tombstone.parent().unwrap()).expect("create tombstone root");
    std::fs::rename(&table_dir, &tombstone).expect("stage table directory");
    std::fs::write(&table_dir, b"blocks directory restore").expect("create conflicting file");

    let error = match Engine::new(temp_dir.path(), "main") {
        Ok(_) => panic!("engine startup should fail"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("is not a directory"));
    assert!(tombstone.exists());
}

#[test]
fn rename_table_preserves_pending_memtables_under_new_name() {
    let temp_dir = TempDir::new().expect("create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("create engine");
    let mut session = Session::new("root".to_string(), "main".to_string());
    engine
        .execute(&mut session, "CREATE TABLE rename_pending (id INT64)")
        .expect("create table");
    engine
        .execute(&mut session, "INSERT INTO rename_pending VALUES (1)")
        .expect("insert pending row");

    engine
        .execute(
            &mut session,
            "ALTER TABLE rename_pending RENAME TO rename_pending_new",
        )
        .expect("rename table with pending row");

    assert_eq!(
        scalar_count(
            &engine,
            &mut session,
            "SELECT COUNT(*) FROM rename_pending_new"
        ),
        1
    );
    engine
        .execute(&mut session, "FLUSH TABLE rename_pending_new")
        .expect("flush renamed memtable");
    assert_eq!(
        scalar_count(
            &engine,
            &mut session,
            "SELECT COUNT(*) FROM rename_pending_new"
        ),
        1
    );
    assert!(!temp_dir.path().join("main").join("rename_pending").exists());
    engine.segment_mgr().shutdown();
}

#[test]
fn rename_table_cancels_detached_flush_for_old_name() {
    let temp_dir = TempDir::new().expect("create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("create engine");
    let mut session = Session::new("root".to_string(), "main".to_string());
    engine
        .execute(&mut session, "CREATE TABLE rename_in_flight (id INT64)")
        .expect("create table");
    engine
        .execute(&mut session, "INSERT INTO rename_in_flight VALUES (1)")
        .expect("insert pending row");

    let memtable = engine
        .batch_writer()
        .memtable_manager()
        .get_by_table("main", "rename_in_flight")
        .into_iter()
        .next()
        .expect("pending memtable");
    memtable.set_state(crate::storage::core::memtable::MemTableState::Flushing);
    let detached_batch = memtable.read_snapshot().expect("detached flush snapshot");

    engine
        .execute(
            &mut session,
            "ALTER TABLE rename_in_flight RENAME TO rename_in_flight_new",
        )
        .expect("rename during detached flush");

    let wrote = engine
        .segment_mgr()
        .flush_memtable_batch_if(
            "main",
            "rename_in_flight",
            "",
            &detached_batch,
            0,
            || memtable.state() == crate::storage::core::memtable::MemTableState::Flushing,
            || {},
        )
        .expect("cancel detached old-name flush");
    assert!(!wrote);
    assert!(!temp_dir
        .path()
        .join("main")
        .join("rename_in_flight")
        .exists());
    assert_eq!(
        scalar_count(
            &engine,
            &mut session,
            "SELECT COUNT(*) FROM rename_in_flight_new"
        ),
        1
    );
    engine.segment_mgr().shutdown();
}

#[test]
fn rename_table_flushes_bulk_import_active_state() {
    let temp_dir = TempDir::new().expect("create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("create engine");
    let mut session = Session::new("root".to_string(), "main".to_string());
    engine
        .execute(&mut session, "CREATE TABLE rename_bulk (id INT64)")
        .expect("create table");

    engine.begin_bulk_import();
    session.bulk_import_mode = true;
    engine
        .execute(&mut session, "INSERT INTO rename_bulk VALUES (1)")
        .expect("bulk insert row");
    engine
        .execute(
            &mut session,
            "ALTER TABLE rename_bulk RENAME TO rename_bulk_new",
        )
        .expect("rename table during bulk import");

    session.bulk_import_mode = false;
    engine.end_bulk_import();
    engine
        .flush_all_buffers()
        .expect("finish bulk import flush");
    assert_eq!(
        scalar_count(
            &engine,
            &mut session,
            "SELECT COUNT(*) FROM rename_bulk_new"
        ),
        1
    );
    assert!(!temp_dir.path().join("main").join("rename_bulk").exists());
    engine.segment_mgr().shutdown();
}

#[test]
fn rename_and_truncate_invalidate_unique_index_snapshots() {
    let temp_dir = TempDir::new().expect("create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("create engine");
    let mut session = Session::new("root".to_string(), "main".to_string());
    engine
        .execute(&mut session, "CREATE TABLE unique_ddl (id INT64 UNIQUE)")
        .expect("create unique table");
    engine
        .execute(&mut session, "INSERT INTO unique_ddl VALUES (1)")
        .expect("insert unique row");
    engine
        .execute(&mut session, "FLUSH TABLE unique_ddl")
        .expect("flush table");
    engine
        .execute(&mut session, "REBUILD UNIQUE INDEX uk_id ON unique_ddl")
        .expect("rebuild unique index");

    engine
        .execute(
            &mut session,
            "ALTER TABLE unique_ddl RENAME TO unique_ddl_new",
        )
        .expect("rename unique table");
    engine
        .execute(&mut session, "INSERT INTO unique_ddl_new VALUES (1)")
        .expect_err("duplicate must still be rejected after rename");

    engine
        .execute(&mut session, "TRUNCATE TABLE unique_ddl_new")
        .expect("truncate unique table");
    engine
        .execute(&mut session, "INSERT INTO unique_ddl_new VALUES (1)")
        .expect("truncated unique key can be reused");
    engine.segment_mgr().shutdown();
}

#[test]
fn concurrent_insert_cannot_recreate_dropped_table_directory() {
    let temp_dir = TempDir::new().expect("create temp dir");
    let engine = Arc::new(Engine::new(temp_dir.path(), "main").expect("create engine"));
    let mut setup = Session::new("root".to_string(), "main".to_string());
    engine
        .execute(&mut setup, "CREATE TABLE concurrent_drop (id INT64)")
        .expect("create table");

    let barrier = Arc::new(Barrier::new(2));
    let writer_engine = Arc::clone(&engine);
    let writer_barrier = Arc::clone(&barrier);
    let writer = thread::spawn(move || {
        let mut session = Session::new("root".to_string(), "main".to_string());
        writer_barrier.wait();
        for value in 0..500 {
            if writer_engine
                .execute(
                    &mut session,
                    &format!("INSERT INTO concurrent_drop VALUES ({value})"),
                )
                .is_err()
            {
                break;
            }
        }
    });

    barrier.wait();
    engine
        .execute(&mut setup, "DROP TABLE concurrent_drop")
        .expect("drop table during inserts");
    writer.join().expect("join writer");

    assert!(engine.get_table_meta("main", "concurrent_drop").is_none());
    assert!(!temp_dir
        .path()
        .join("main")
        .join("concurrent_drop")
        .exists());
    engine.segment_mgr().shutdown();
}

#[test]
fn create_table_durability_default_is_strict() {
    let temp_dir = TempDir::new().expect("create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("create engine");
    let mut session = Session::new("root".to_string(), "main".to_string());

    engine
        .execute(&mut session, "CREATE TABLE strict_t (id INT64)")
        .expect("create table");
    let meta = engine.get_table_meta("main", "strict_t").expect("meta");
    assert_eq!(meta.durability, TableDurability::Strict);

    engine
        .execute(&mut session, "DROP TABLE strict_t")
        .expect("drop table");
    engine.segment_mgr().shutdown();
}

#[test]
fn create_table_durability_relaxed_round_trip() {
    let temp_dir = TempDir::new().expect("create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("create engine");
    let mut session = Session::new("root".to_string(), "main".to_string());

    engine
        .execute(
            &mut session,
            "CREATE TABLE relaxed_t (id INT64, day INT32) DURABILITY = RELAXED",
        )
        .expect("create relaxed table");
    let meta = engine.get_table_meta("main", "relaxed_t").expect("meta");
    assert_eq!(meta.durability, TableDurability::Relaxed);

    match engine
        .execute(&mut session, "SHOW CREATE TABLE relaxed_t")
        .expect("show create table")
    {
        ExecResult::Query { batches, .. } => {
            let sql = match batches[0].scalar(0, 1) {
                Some(ScalarValue::VarChar(Some(value))) => value,
                other => panic!("unexpected create sql: {other:?}"),
            };
            assert!(
                sql.contains("DURABILITY = RELAXED"),
                "SHOW CREATE TABLE must render durability: {sql}"
            );
        }
        other => panic!("unexpected show result: {other:?}"),
    }

    engine
        .execute(&mut session, "DROP TABLE relaxed_t")
        .expect("drop table");
    engine.segment_mgr().shutdown();
}

#[test]
fn create_table_durability_rejects_unknown_level() {
    let temp_dir = TempDir::new().expect("create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("create engine");
    let mut session = Session::new("root".to_string(), "main".to_string());

    let error = engine
        .execute(
            &mut session,
            "CREATE TABLE bad_t (id INT64) DURABILITY = FAST",
        )
        .expect_err("unknown durability level must fail");
    assert!(
        error.to_string().contains("DURABILITY"),
        "unexpected error: {error}"
    );
    assert!(engine.get_table_meta("main", "bad_t").is_none());
    engine.segment_mgr().shutdown();
}

#[test]
fn create_table_compression_level_round_trip() {
    let temp_dir = TempDir::new().expect("create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("create engine");
    let mut session = Session::new("root".to_string(), "main".to_string());

    engine
        .execute(
            &mut session,
            "CREATE TABLE archive_t (id INT64) COMPRESSION_LEVEL = 19",
        )
        .expect("create archive table");
    let meta = engine.get_table_meta("main", "archive_t").expect("meta");
    assert_eq!(meta.compression_level, Some(19));
    assert_eq!(
        engine
            .segment_mgr()
            .table_compression_level("main", "archive_t"),
        19
    );

    match engine
        .execute(&mut session, "SHOW CREATE TABLE archive_t")
        .expect("show create table")
    {
        ExecResult::Query { batches, .. } => {
            let sql = match batches[0].scalar(0, 1) {
                Some(ScalarValue::VarChar(Some(value))) => value,
                other => panic!("unexpected create sql: {other:?}"),
            };
            assert!(
                sql.contains("COMPRESSION_LEVEL = 19"),
                "SHOW CREATE TABLE must render compression level: {sql}"
            );
        }
        other => panic!("unexpected show result: {other:?}"),
    }

    // A table without the option follows the global configured level.
    engine
        .execute(&mut session, "CREATE TABLE default_t (id INT64)")
        .expect("create default table");
    let meta = engine.get_table_meta("main", "default_t").expect("meta");
    assert_eq!(meta.compression_level, None);
    assert_eq!(
        engine
            .segment_mgr()
            .table_compression_level("main", "default_t"),
        crate::common::config::bulk_zstd_level()
    );

    engine
        .execute(&mut session, "DROP TABLE archive_t")
        .expect("drop table");
    engine
        .execute(&mut session, "DROP TABLE default_t")
        .expect("drop table");
    engine.segment_mgr().shutdown();
}

#[test]
fn create_table_compression_level_rejects_out_of_range() {
    let temp_dir = TempDir::new().expect("create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("create engine");
    let mut session = Session::new("root".to_string(), "main".to_string());

    for level in [0, 23] {
        let error = engine
            .execute(
                &mut session,
                &format!("CREATE TABLE bad_t (id INT64) COMPRESSION_LEVEL = {level}"),
            )
            .expect_err("out-of-range compression level must fail");
        assert!(
            error.to_string().contains("COMPRESSION_LEVEL"),
            "unexpected error: {error}"
        );
    }
    assert!(engine.get_table_meta("main", "bad_t").is_none());
    engine.segment_mgr().shutdown();
}

#[test]
fn alter_table_compression_level_set_and_reset() {
    let temp_dir = TempDir::new().expect("create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("create engine");
    let mut session = Session::new("root".to_string(), "main".to_string());

    engine
        .execute(&mut session, "CREATE TABLE logs_t (id INT64)")
        .expect("create table");
    engine
        .execute(&mut session, "INSERT INTO logs_t VALUES (1)")
        .expect("insert row");

    engine
        .execute(
            &mut session,
            "ALTER TABLE logs_t SET COMPRESSION_LEVEL = 22",
        )
        .expect("alter set compression level");
    let meta = engine.get_table_meta("main", "logs_t").expect("meta");
    assert_eq!(meta.compression_level, Some(22));
    assert_eq!(
        engine
            .segment_mgr()
            .table_compression_level("main", "logs_t"),
        22
    );

    engine
        .execute(
            &mut session,
            "ALTER TABLE logs_t SET COMPRESSION_LEVEL = DEFAULT",
        )
        .expect("alter reset compression level");
    let meta = engine.get_table_meta("main", "logs_t").expect("meta");
    assert_eq!(meta.compression_level, None);
    assert_eq!(
        engine
            .segment_mgr()
            .table_compression_level("main", "logs_t"),
        crate::common::config::bulk_zstd_level()
    );

    engine
        .execute(&mut session, "DROP TABLE logs_t")
        .expect("drop table");
    engine.segment_mgr().shutdown();
}

#[test]
fn show_version_returns_build_identity() {
    let temp_dir = TempDir::new().expect("create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("create engine");
    let mut session = Session::new("root".to_string(), "main".to_string());
    match engine
        .execute(&mut session, "SHOW VERSION")
        .expect("show version")
    {
        ExecResult::Query { batches, .. } => match batches[0].scalar(0, 0) {
            Some(ScalarValue::VarChar(Some(version))) => {
                assert!(
                    !version.is_empty() && version.contains("commit") && version.contains("built"),
                    "version must carry the build identity, got: {version}"
                );
            }
            other => panic!("unexpected version value: {other:?}"),
        },
        other => panic!("unexpected result: {other:?}"),
    }
    engine.segment_mgr().shutdown();
}
