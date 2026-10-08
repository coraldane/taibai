use tempfile::TempDir;

use taibai::net::session::Session;
use taibai::protocol::types::DataType;
use taibai::sql::execution::engine::{Engine, ExecResult};
use taibai::storage::core::durable_tail;
use taibai::storage::segment_manager::{SegmentConfig, SegmentManager};
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
        .write_new_segment(&seg_path, schema, 2, &batch, 3)
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

#[test]
fn update_mutates_target_memtable_without_flushing_other_tables() {
    let (temp_dir, engine, mut session) = create_engine();
    engine
        .execute(&mut session, "CREATE TABLE target (id INT32, value INT32)")
        .expect("create target table");
    engine
        .execute(
            &mut session,
            "CREATE TABLE unrelated (id INT32, value INT32)",
        )
        .expect("create unrelated table");
    engine
        .execute(&mut session, "INSERT INTO target VALUES (1, 10)")
        .expect("insert target row");
    engine
        .execute(&mut session, "INSERT INTO unrelated VALUES (1, 20)")
        .expect("insert unrelated row");

    let updated = engine
        .execute(&mut session, "UPDATE target SET value = 99 WHERE id = 1")
        .expect("update resident row");
    match updated {
        ExecResult::Ok { rows_affected } => assert_eq!(rows_affected, 1),
        other => panic!("expected ok, got {:?}", other),
    }
    for value in 100..125 {
        let updated = engine
            .execute(
                &mut session,
                &format!("UPDATE target SET value = {} WHERE id = 1", value),
            )
            .expect("repeat resident update");
        match updated {
            ExecResult::Ok { rows_affected } => assert_eq!(rows_affected, 1),
            other => panic!("expected ok, got {:?}", other),
        }
    }
    assert_eq!(
        collect_i32(
            engine
                .execute(&mut session, "SELECT value FROM target")
                .expect("select updated row"),
            0,
        ),
        vec![124]
    );

    let target_dir = temp_dir.path().join("main").join("target");
    let unrelated_dir = temp_dir.path().join("main").join("unrelated");
    let target_schema = Schema::new(vec![
        ColumnDef::new("id", DataType::Int32),
        ColumnDef::new("value", DataType::Int32),
    ]);
    let tails =
        durable_tail::recover(&target_dir, &target_schema).expect("recover updated durable tail");
    assert_eq!(tails.len(), 1);
    assert_eq!(
        tails[0].batch.scalar(0, 1),
        Some(ScalarValue::Int32(Some(124)))
    );
    assert!(SegmentManager::discover_segments(&target_dir)
        .expect("discover target segments")
        .is_empty());
    assert!(SegmentManager::discover_segments(&unrelated_dir)
        .expect("discover unrelated segments")
        .is_empty());

    engine
        .execute(&mut session, "FLUSH TABLE target")
        .expect("flush target table");
    let target_segments =
        SegmentManager::discover_segments(&target_dir).expect("discover flushed target segments");
    assert_eq!(target_segments.len(), 1);
    let reader = engine
        .segment_mgr()
        .open_reader(&target_segments[0])
        .expect("open flushed target segment");
    assert_eq!(reader.num_row_groups(), 1);
    assert_eq!(reader.row_count(), 1);
    assert!(durable_tail::recover(&target_dir, &target_schema)
        .expect("verify durable tail cleared")
        .is_empty());
    assert!(SegmentManager::discover_segments(&unrelated_dir)
        .expect("discover unrelated segments after target flush")
        .is_empty());
    engine.segment_mgr().shutdown();
}

#[test]
fn updated_memtable_is_persisted_by_graceful_restart() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    {
        let mut engine = Engine::new(temp_dir.path(), "main").expect("create engine");
        let mut session = Session::new("root".to_string(), "main".to_string());
        engine
            .execute(&mut session, "CREATE TABLE t (id INT32, value INT32)")
            .expect("create table");
        engine
            .execute(&mut session, "INSERT INTO t VALUES (1, 10)")
            .expect("insert row");
        engine
            .execute(&mut session, "UPDATE t SET value = 99 WHERE id = 1")
            .expect("update resident row");
        engine.shutdown().expect("graceful shutdown");
    }

    let table_dir = temp_dir.path().join("main").join("t");
    let schema = Schema::new(vec![
        ColumnDef::new("id", DataType::Int32),
        ColumnDef::new("value", DataType::Int32),
    ]);
    // The updated memtable is tail-backed and restart-safe, so the graceful
    // shutdown leaves it resident (no tiny row group) and keeps its tail.
    if table_dir.exists() {
        assert!(SegmentManager::discover_segments(&table_dir)
            .expect("discover segments after shutdown")
            .is_empty());
        assert!(!durable_tail::recover(&table_dir, &schema)
            .expect("verify shutdown tail retained")
            .is_empty());
    }

    let engine = Engine::new(temp_dir.path(), "main").expect("reopen engine");
    let mut session = Session::new("root".to_string(), "main".to_string());
    assert_eq!(
        collect_i32(
            engine
                .execute(&mut session, "SELECT value FROM t")
                .expect("select after restart"),
            0,
        ),
        vec![99]
    );
    engine.segment_mgr().shutdown();
}

#[test]
fn engine_startup_recovers_durable_low_rate_tail() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let schema = Schema::new(vec![
        ColumnDef::new("id", DataType::Int32),
        ColumnDef::new("value", DataType::Int32),
    ]);
    {
        let mut engine = Engine::new(temp_dir.path(), "main").expect("create engine");
        let mut session = Session::new("root".to_string(), "main".to_string());
        engine
            .execute(&mut session, "CREATE TABLE t (id INT32, value INT32)")
            .expect("create table");
        engine.shutdown().expect("shutdown empty engine");
    }

    let mut tail_batch = RecordBatch::new(schema.clone());
    tail_batch
        .column_mut(0)
        .unwrap()
        .push(&ScalarValue::Int32(Some(1)))
        .unwrap();
    tail_batch
        .column_mut(1)
        .unwrap()
        .push(&ScalarValue::Int32(Some(99)))
        .unwrap();
    let table_dir = temp_dir.path().join("main").join("t");
    durable_tail::persist(&table_dir, uuid::Uuid::new_v4(), "", &tail_batch)
        .expect("persist simulated crash tail");

    let engine = Engine::new(temp_dir.path(), "main").expect("reopen engine");
    let mut session = Session::new("root".to_string(), "main".to_string());
    assert_eq!(
        collect_i32(
            engine
                .execute(&mut session, "SELECT value FROM t")
                .expect("select recovered tail"),
            0,
        ),
        vec![99]
    );
    assert!(SegmentManager::discover_segments(&table_dir)
        .expect("discover segments before recovered tail flush")
        .is_empty());
    engine.segment_mgr().shutdown();
}

#[test]
fn update_invalidates_hot_store_for_following_inserts() {
    let (_temp_dir, engine, mut session) = create_engine();
    engine
        .execute(
            &mut session,
            "CREATE TABLE ticks (symbol VARCHAR, value INT32) CLUSTER BY (symbol)",
        )
        .expect("create clustered table");
    engine
        .execute(
            &mut session,
            "INSERT INTO ticks VALUES ('A', 10), ('B', 20)",
        )
        .expect("insert hot rows");
    engine
        .execute(
            &mut session,
            "UPDATE ticks SET value = 99 WHERE symbol = 'A'",
        )
        .expect("update hot row");
    engine
        .execute(&mut session, "INSERT INTO ticks VALUES ('C', 30)")
        .expect("insert after hot store invalidation");

    let mut values = collect_i32(
        engine
            .execute(&mut session, "SELECT value FROM ticks")
            .expect("select all resident rows"),
        0,
    );
    values.sort_unstable();
    assert_eq!(values, vec![20, 30, 99]);
    engine.segment_mgr().shutdown();
}

#[test]
fn partition_update_seals_and_rewrites_only_the_target_partition() {
    let (temp_dir, engine, mut session) = create_engine();
    engine
        .execute(
            &mut session,
            "CREATE TABLE partitioned (id INT32, value INT32) PARTITION BY id",
        )
        .expect("create partitioned table");

    engine.begin_bulk_import();
    session.bulk_import_mode = true;
    engine
        .execute(&mut session, "INSERT INTO partitioned VALUES (1, 10)")
        .expect("insert target partition row");
    engine
        .execute(&mut session, "INSERT INTO partitioned VALUES (2, 20)")
        .expect("insert sibling partition row");
    let mut active_before = engine
        .segment_mgr()
        .active_partition_keys("main", "partitioned");
    active_before.sort();
    assert_eq!(active_before, vec!["1".to_string(), "2".to_string()]);

    let updated = engine
        .execute(
            &mut session,
            "UPDATE partitioned SET value = 99 WHERE id = 1",
        )
        .expect("update one partition");
    match updated {
        ExecResult::Ok { rows_affected } => assert_eq!(rows_affected, 1),
        other => panic!("expected ok, got {:?}", other),
    }

    assert_eq!(
        engine
            .segment_mgr()
            .active_partition_keys("main", "partitioned"),
        vec!["2".to_string()]
    );
    let table_dir = temp_dir.path().join("main").join("partitioned");
    let names = SegmentManager::discover_segments(&table_dir)
        .expect("discover partitioned segments")
        .into_iter()
        .map(|path| path.file_name().unwrap().to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    assert!(names.iter().any(|name| name.starts_with("p1_")));
    assert!(names.iter().any(|name| name.starts_with("p2_")));
    assert!(names.iter().all(|name| !name.starts_with("seg_")));

    let mut values = collect_i32(
        engine
            .execute(&mut session, "SELECT value FROM partitioned")
            .expect("select partition rows"),
        0,
    );
    values.sort_unstable();
    assert_eq!(values, vec![20, 99]);

    session.bulk_import_mode = false;
    engine.end_bulk_import();
    engine.segment_mgr().shutdown();
}

#[test]
fn update_rejects_partition_key_changes_instead_of_misplacing_rows() {
    let (_temp_dir, engine, mut session) = create_engine();
    engine
        .execute(
            &mut session,
            "CREATE TABLE partitioned (id INT32, value INT32) PARTITION BY id",
        )
        .expect("create partitioned table");
    engine
        .execute(&mut session, "INSERT INTO partitioned VALUES (1, 10)")
        .expect("insert partition row");

    let error = engine
        .execute(&mut session, "UPDATE partitioned SET id = 2 WHERE id = 1")
        .expect_err("partition-key update must be rejected");
    assert!(error.to_string().contains("updating partition key 'id'"));
    assert_eq!(
        collect_i32(
            engine
                .execute(&mut session, "SELECT id FROM partitioned")
                .expect("select unchanged partition row"),
            0,
        ),
        vec![1]
    );
    engine.segment_mgr().shutdown();
}

#[test]
fn delete_and_replace_do_not_flush_unrelated_low_rate_table() {
    let (temp_dir, engine, mut session) = create_engine();
    engine
        .execute(&mut session, "CREATE TABLE low_rate (id INT32)")
        .expect("create low-rate table");
    engine
        .execute(&mut session, "CREATE TABLE deleted (id INT32)")
        .expect("create delete target");
    engine
        .execute(
            &mut session,
            "CREATE TABLE replaced (id INT32 PRIMARY KEY, value INT32)",
        )
        .expect("create replace target");
    engine
        .execute(&mut session, "INSERT INTO low_rate VALUES (1)")
        .expect("insert low-rate row");
    engine
        .execute(&mut session, "INSERT INTO deleted VALUES (1)")
        .expect("insert delete target");
    engine
        .execute(&mut session, "INSERT INTO replaced VALUES (1, 10)")
        .expect("insert replace target");

    engine
        .execute(&mut session, "DELETE FROM deleted WHERE id = 1")
        .expect("delete target row");
    engine
        .execute(&mut session, "REPLACE INTO replaced VALUES (1, 20)")
        .expect("replace target row");

    let low_rate_dir = temp_dir.path().join("main").join("low_rate");
    assert!(SegmentManager::discover_segments(&low_rate_dir)
        .expect("discover unrelated low-rate segments")
        .is_empty());
    assert_eq!(
        collect_i32(
            engine
                .execute(&mut session, "SELECT id FROM low_rate")
                .expect("select unrelated low-rate row"),
            0,
        ),
        vec![1]
    );
    engine.segment_mgr().shutdown();
}

#[test]
fn scattered_inserts_materialize_only_at_row_group_target() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Engine::new_with_storage_config(
        temp_dir.path(),
        "main",
        SegmentConfig {
            row_group_target_rows: 1024,
            ..SegmentConfig::default()
        },
    )
    .expect("failed to create engine");
    let mut session = Session::new("root".to_string(), "main".to_string());
    engine
        .execute(&mut session, "CREATE TABLE low_rate (id INT32)")
        .expect("create low-rate table");
    let values = (0..1024)
        .map(|value| format!("({value})"))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(
            &mut session,
            &format!("INSERT INTO low_rate VALUES {values}"),
        )
        .expect("fill durable low-rate tail");

    let table_dir = temp_dir.path().join("main").join("low_rate");
    let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int32)]);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    let segments = loop {
        let segments = SegmentManager::discover_segments(&table_dir).expect("discover segments");
        if !segments.is_empty() || std::time::Instant::now() >= deadline {
            break segments;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    };
    assert_eq!(segments.len(), 1);
    let reader = engine
        .segment_mgr()
        .open_reader(&segments[0])
        .expect("open full row-group segment");
    assert_eq!(reader.row_count(), 1024);
    assert_eq!(reader.num_row_groups(), 1);

    engine
        .execute(&mut session, "INSERT INTO low_rate VALUES (1024)")
        .expect("insert scattered tail row");
    let segments =
        SegmentManager::discover_segments(&table_dir).expect("discover materialized segments");
    assert_eq!(segments.len(), 1);
    // The scattered row's durable-tail frame is written by the background
    // flush, not synchronously by the INSERT — poll for it instead of
    // racing the flush (this read used to flake under load). A fresh
    // deadline: reusing the segment poll's deadline left this loop with the
    // leftover time only, failing under parallel-suite load.
    let tail_deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    let tails = loop {
        let tails =
            durable_tail::recover(&table_dir, &schema).expect("recover partial durable tail");
        if !tails.is_empty() || std::time::Instant::now() >= tail_deadline {
            break tails;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    };
    assert_eq!(tails.len(), 1);
    assert_eq!(tails[0].batch.num_rows(), 1);
    assert_eq!(
        collect_i32(
            engine
                .execute(&mut session, "SELECT id FROM low_rate")
                .expect("read materialized rows"),
            0,
        )
        .len(),
        1025
    );
    engine.segment_mgr().shutdown();
}

#[test]
fn update_with_as_alias_updates_through_qualified_set_and_where() {
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
        .execute(
            &mut session,
            "UPDATE t AS a SET a.value = 99 WHERE a.id = 2",
        )
        .expect("update row through alias");
    match updated {
        ExecResult::Ok { rows_affected } => assert_eq!(rows_affected, 1),
        other => panic!("expected ok, got {:?}", other),
    }

    let values = collect_i32(
        engine
            .execute(&mut session, "SELECT value FROM t ORDER BY id")
            .expect("select updated rows"),
        0,
    );
    assert_eq!(values, vec![10, 99, 30]);
    engine.segment_mgr().shutdown();
}

#[test]
fn update_with_implicit_alias_updates_matching_rows() {
    let (_temp_dir, engine, mut session) = create_engine();
    engine
        .execute(&mut session, "CREATE TABLE t (id INT32, value INT32)")
        .expect("create table");
    engine
        .execute(&mut session, "INSERT INTO t VALUES (1, 10), (2, 20)")
        .expect("insert rows");

    let updated = engine
        .execute(&mut session, "UPDATE t tgt SET value = 50 WHERE tgt.id = 2")
        .expect("update row through implicit alias");
    match updated {
        ExecResult::Ok { rows_affected } => assert_eq!(rows_affected, 1),
        other => panic!("expected ok, got {:?}", other),
    }

    let values = collect_i32(
        engine
            .execute(&mut session, "SELECT value FROM t ORDER BY id")
            .expect("select updated rows"),
        0,
    );
    assert_eq!(values, vec![10, 50]);
    engine.segment_mgr().shutdown();
}

#[test]
fn update_set_column_qualified_by_table_name_still_works() {
    let (_temp_dir, engine, mut session) = create_engine();
    engine
        .execute(&mut session, "CREATE TABLE t (id INT32, value INT32)")
        .expect("create table");
    engine
        .execute(&mut session, "INSERT INTO t VALUES (1, 10), (2, 20)")
        .expect("insert rows");

    let updated = engine
        .execute(&mut session, "UPDATE t SET t.value = 70 WHERE t.id = 1")
        .expect("update row with table-name-qualified column");
    match updated {
        ExecResult::Ok { rows_affected } => assert_eq!(rows_affected, 1),
        other => panic!("expected ok, got {:?}", other),
    }

    let values = collect_i32(
        engine
            .execute(&mut session, "SELECT value FROM t ORDER BY id")
            .expect("select updated rows"),
        0,
    );
    assert_eq!(values, vec![70, 20]);
    engine.segment_mgr().shutdown();
}

#[test]
fn update_with_alias_still_rejects_partition_key_assignment() {
    let (_temp_dir, engine, mut session) = create_engine();
    engine
        .execute(
            &mut session,
            "CREATE TABLE partitioned (id INT32, value INT32) PARTITION BY id",
        )
        .expect("create partitioned table");
    engine
        .execute(&mut session, "INSERT INTO partitioned VALUES (1, 10)")
        .expect("insert row");

    let error = engine
        .execute(
            &mut session,
            "UPDATE partitioned AS p SET p.id = 2 WHERE p.value = 10",
        )
        .expect_err("partition key assignment must be rejected");
    assert!(
        error.to_string().contains("updating partition key"),
        "unexpected error: {error}"
    );
    engine.segment_mgr().shutdown();
}

/// The memtable UPDATE fast path is incremental now: the statement writes an
/// `.upd` sidecar instead of rewriting the whole tail. An ABRUPT stop (no
/// graceful checkpoint) with a delete sidecar stacked UNDER an update sidecar
/// must replay both in generation order on restart: the update speaks in the
/// post-delete coordinate space, so replaying deletes after updates (or
/// dropping either) would corrupt or lose the effect.
#[test]
fn abrupt_restart_replays_pending_update_and_delete_sidecars() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    {
        let engine = Engine::new(temp_dir.path(), "main").expect("create engine");
        let mut session = Session::new("root".to_string(), "main".to_string());
        engine
            .execute(&mut session, "CREATE TABLE t (id INT32, value INT32)")
            .expect("create table");
        for id in 1..=6 {
            engine
                .execute(&mut session, &format!("INSERT INTO t VALUES ({id}, {id}0)"))
                .expect("insert row");
        }
        engine
            .execute(&mut session, "UPDATE t SET value = 99 WHERE id <= 2")
            .expect("update two rows");
        engine
            .execute(&mut session, "DELETE FROM t WHERE id = 3")
            .expect("delete one row");
        engine
            .execute(&mut session, "UPDATE t SET value = 77 WHERE id = 4")
            .expect("update another row");
        // Simulate a crash: skip Engine's Drop so no shutdown checkpoint
        // folds the pending sidecars.
        std::mem::forget(engine);
    }

    let engine = Engine::new(temp_dir.path(), "main").expect("reopen engine");
    let mut session = Session::new("root".to_string(), "main".to_string());
    let mut values = collect_i32(
        engine
            .execute(&mut session, "SELECT value FROM t")
            .expect("select after crash restart"),
        0,
    );
    values.sort_unstable();
    // id 1,2 → 99; id 3 deleted; id 4 → 77; id 5,6 untouched.
    assert_eq!(values, vec![50, 60, 77, 99, 99]);
    engine.segment_mgr().shutdown();
}

/// Repeated small UPDATEs on a resident memtable must keep the durable cost
/// proportional to the affected rows (the sidecar chain replays once per
/// recovery), and every intermediate value must remain query- and
/// restart-correct.
#[test]
fn repeated_small_updates_stay_correct_across_restart() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    {
        let engine = Engine::new(temp_dir.path(), "main").expect("create engine");
        let mut session = Session::new("root".to_string(), "main".to_string());
        engine
            .execute(&mut session, "CREATE TABLE t (id INT32, value INT32)")
            .expect("create table");
        for id in 1..=3 {
            engine
                .execute(&mut session, &format!("INSERT INTO t VALUES ({id}, 0)"))
                .expect("insert row");
        }
        for round in 1..=30 {
            engine
                .execute(
                    &mut session,
                    &format!("UPDATE t SET value = {round} WHERE id = 1"),
                )
                .expect("repeat update");
        }
        // The restart lands on the sidecar path only when the tail still
        // holds pending generations; a graceful shutdown folds them, so
        // assert through an abrupt stop.
        std::mem::forget(engine);
    }

    let engine = Engine::new(temp_dir.path(), "main").expect("reopen engine");
    let mut session = Session::new("root".to_string(), "main".to_string());
    assert_eq!(
        collect_i32(
            engine
                .execute(&mut session, "SELECT value FROM t WHERE id = 1")
                .expect("select updated row after restart"),
            0,
        ),
        vec![30]
    );
    assert_eq!(
        collect_i32(
            engine
                .execute(&mut session, "SELECT value FROM t WHERE id = 2")
                .expect("select untouched row after restart"),
            0,
        ),
        vec![0]
    );
    engine.segment_mgr().shutdown();
}
