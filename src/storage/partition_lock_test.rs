use super::*;
use crate::common::error::Result;
use crate::protocol::types::DataType;
use crate::storage::core::batch_writer::{BatchWriter, BatchWriterConfig};
use crate::types::batch::{ColumnDef, RecordBatch, Schema};
use crate::types::scalar::ScalarValue;
use std::sync::mpsc;
use std::time::Duration;

const TEST_DB: &str = "main";
const TEST_TABLE: &str = "t_partition_locks";

fn test_schema() -> Schema {
    Schema::new(vec![ColumnDef::new("id", DataType::Int64)])
}

fn make_batch(schema: &Schema, start_id: i64, rows: usize) -> Result<RecordBatch> {
    let mut batch = RecordBatch::new(schema.clone());
    for offset in 0..rows {
        batch
            .column_mut(0)
            .unwrap()
            .push(&ScalarValue::Int64(Some(start_id + offset as i64)))?;
    }
    Ok(batch)
}

fn test_writer_config() -> BatchWriterConfig {
    BatchWriterConfig {
        max_rows_per_memtable: 64,
        flush_interval_secs: 3600,
        max_total_memory: 1 << 30,
        worker_count: 1,
        low_rate_max_rows: 1024,
        low_rate_max_age_secs: 3600,
        spill_enabled: true,
        spill_min_age_secs: 0,
        tail_async_fsync: false,
        memory_hard_limit: 1 << 31,
    }
}

#[test]
fn wide_gate_resets_after_panic_inside_critical_section() {
    let dir = tempfile::tempdir().unwrap();
    let segment_mgr = Arc::new(SegmentManager::new(dir.path()).unwrap());

    let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = segment_mgr.with_table_mutation_lock(TEST_DB, TEST_TABLE, || -> Result<()> {
            panic!("boom inside wide lock");
        });
    }));
    assert!(caught.is_err());

    let outcome =
        segment_mgr.try_with_partition_mutation_lock(TEST_DB, TEST_TABLE, "20260923", || Ok(()));
    assert!(
        outcome.unwrap().is_some(),
        "partition gate must be cleared after a panic in the wide-lock critical section"
    );
}

#[test]
fn wide_gate_collection_is_bounded_while_partition_lock_held() {
    let dir = tempfile::tempdir().unwrap();
    let manager = SegmentManager::new(dir.path()).unwrap();
    let held = manager.partition_lock_arc_for_tests(TEST_DB, TEST_TABLE, "p1");
    let _held_guard = held.lock_for_test();

    // A wide-gate holder must not block indefinitely behind one long-held
    // partition lock: the collection wait is bounded and surfaces as an
    // explicit ResourceBusy error, releasing the gate and every partition
    // lock it already collected.
    let started = std::time::Instant::now();
    let outcome = acquire_partition_lock_for_wide_gate(
        &held,
        "main:t_partition_locks",
        "p1",
        Duration::from_millis(200),
    );
    assert!(
        outcome.is_err(),
        "bounded wide-gate collection must time out behind a held partition lock"
    );
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "collection timeout must be honored promptly"
    );

    // Other partitions stay untouched by the failed collection.
    let other = manager.try_with_partition_mutation_lock(TEST_DB, TEST_TABLE, "p2", || Ok(5));
    assert_eq!(other.unwrap(), Some(5));

    drop(_held_guard);
    let guard = acquire_partition_lock_for_wide_gate(
        &held,
        "main:t_partition_locks",
        "p1",
        Duration::from_secs(2),
    )
    .expect("collection must succeed once the partition lock is free");
    drop(guard);
}

#[test]
fn partition_locks_are_independent() {
    let dir = tempfile::tempdir().unwrap();
    let manager = SegmentManager::new(dir.path()).unwrap();
    let held = manager.partition_lock_arc_for_tests(TEST_DB, TEST_TABLE, "20250101");
    let _held_guard = held.lock_for_test();

    let blocked =
        manager.try_with_partition_mutation_lock(TEST_DB, TEST_TABLE, "20250101", || Ok(()));
    assert!(blocked.unwrap().is_none(), "held partition lock must block");

    let other = manager.try_with_partition_mutation_lock(TEST_DB, TEST_TABLE, "20250102", || Ok(7));
    assert_eq!(other.unwrap(), Some(7), "other partitions stay available");
}

#[test]
fn wide_gate_waits_for_partition_writers() {
    let dir = tempfile::tempdir().unwrap();
    let manager = Arc::new(SegmentManager::new(dir.path()).unwrap());
    let held = manager.partition_lock_arc_for_tests(TEST_DB, TEST_TABLE, "p1");
    let _held_guard = held.lock_for_test();

    let gate_manager = Arc::clone(&manager);
    let (tx, rx) = mpsc::channel::<()>();
    let gate = std::thread::spawn(move || {
        gate_manager
            .with_table_mutation_lock(TEST_DB, TEST_TABLE, || {
                tx.send(()).unwrap();
                Ok(())
            })
            .unwrap();
    });

    assert!(
        rx.recv_timeout(Duration::from_millis(500)).is_err(),
        "wide gate must wait for in-flight partition writers"
    );

    drop(_held_guard);
    rx.recv_timeout(Duration::from_secs(5))
        .expect("wide gate must proceed after partition writer releases");
    gate.join().unwrap();
}

#[test]
fn partition_writers_blocked_while_wide_gate_held() {
    let dir = tempfile::tempdir().unwrap();
    let manager = Arc::new(SegmentManager::new(dir.path()).unwrap());
    let (entered_tx, entered_rx) = mpsc::channel::<()>();
    let (release_tx, release_rx) = mpsc::channel::<()>();

    let gate_manager = Arc::clone(&manager);
    let gate = std::thread::spawn(move || {
        gate_manager
            .with_table_mutation_lock(TEST_DB, TEST_TABLE, || {
                entered_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                Ok(())
            })
            .unwrap();
    });

    entered_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("wide gate must be acquirable on an idle table");

    let blocked = manager.try_with_partition_mutation_lock(TEST_DB, TEST_TABLE, "p1", || Ok(()));
    assert!(
        blocked.unwrap().is_none(),
        "partition writers must be frozen while the wide gate is held"
    );

    release_tx.send(()).unwrap();
    gate.join().unwrap();

    let allowed = manager.try_with_partition_mutation_lock(TEST_DB, TEST_TABLE, "p1", || Ok(()));
    assert!(allowed.unwrap().is_some());
}

#[test]
fn writes_to_other_partitions_proceed_while_partition_locked() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let table_dir = dir.path().join(TEST_DB).join(TEST_TABLE);
    std::fs::create_dir_all(&table_dir)?;
    let schema = test_schema();
    let segment_mgr = Arc::new(SegmentManager::new(dir.path())?);
    let mut writer = BatchWriter::new(test_writer_config(), Arc::clone(&segment_mgr), None);

    let held = segment_mgr.partition_lock_arc_for_tests(TEST_DB, TEST_TABLE, "p1");
    let held_guard = held.lock_for_test();

    let free_schema = schema.clone();
    let free_manager = Arc::clone(&segment_mgr);
    let (free_tx, free_rx) = mpsc::channel();
    let free = std::thread::spawn(move || {
        let result = free_manager.bulk_insert_batch_with_partition(
            TEST_DB,
            TEST_TABLE,
            "p2",
            make_batch(&free_schema, 0, 8).unwrap(),
        );
        free_tx.send(result).unwrap();
    });

    let blocked_schema = schema.clone();
    let blocked_manager = Arc::clone(&segment_mgr);
    let (blocked_tx, blocked_rx) = mpsc::channel();
    let blocked = std::thread::spawn(move || {
        let result = blocked_manager.bulk_insert_batch_with_partition(
            TEST_DB,
            TEST_TABLE,
            "p1",
            make_batch(&blocked_schema, 100, 8).unwrap(),
        );
        blocked_tx.send(result).unwrap();
    });

    free_rx
        .recv_timeout(Duration::from_secs(10))
        .unwrap()
        .expect("bulk insert into an unlocked partition must succeed");
    assert_eq!(free.join().unwrap(), ());

    assert!(
        blocked_rx.recv_timeout(Duration::from_millis(400)).is_err(),
        "bulk insert into the locked partition must wait"
    );
    drop(held_guard);
    blocked_rx
        .recv_timeout(Duration::from_secs(10))
        .unwrap()
        .expect("bulk insert must proceed after the partition lock releases");
    assert_eq!(blocked.join().unwrap(), ());

    writer.stop();
    Ok(())
}

#[test]
fn flush_table_avoids_other_partitions_locks() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let table_dir = dir.path().join(TEST_DB).join(TEST_TABLE);
    std::fs::create_dir_all(&table_dir)?;
    let schema = test_schema();
    let segment_mgr = Arc::new(SegmentManager::new(dir.path())?);
    let writer = BatchWriter::new(test_writer_config(), Arc::clone(&segment_mgr), None);

    writer.write_columns(
        TEST_DB,
        TEST_TABLE,
        "p2",
        make_batch(&schema, 0, 4)?,
        schema.clone(),
        true,
    )?;

    let held = segment_mgr.partition_lock_arc_for_tests(TEST_DB, TEST_TABLE, "p1");
    let held_guard = held.lock_for_test();

    let (flush_tx, flush_rx) = mpsc::channel::<Result<()>>();
    let flush = std::thread::spawn(move || {
        let result = writer.flush_partition(TEST_DB, TEST_TABLE, "p2");
        let _ = flush_tx.send(result);
        writer
    });

    flush_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("flushing partition p2 must not need partition p1's lock")
        .expect("partition flush must succeed");
    let mut writer = flush.join().unwrap();
    writer.stop();
    drop(held_guard);
    Ok(())
}

#[test]
fn partition_set_lock_leaves_other_partitions_free() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let manager = SegmentManager::new(dir.path())?;
    let held = manager.partition_lock_arc_for_tests(TEST_DB, TEST_TABLE, "20250101");
    let _held_guard = held.lock_for_test();

    let outcome =
        manager.try_with_partition_mutation_lock(TEST_DB, TEST_TABLE, "20250102", || Ok(()));
    assert!(
        outcome.unwrap().is_some(),
        "a held partition must not freeze unrelated partitions"
    );
    Ok(())
}

#[test]
fn partition_set_lock_waits_for_held_member_partition() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let manager = Arc::new(SegmentManager::new(dir.path())?);
    let held = manager.partition_lock_arc_for_tests(TEST_DB, TEST_TABLE, "20250101");
    let held_guard = held.lock_for_test();

    let (done_tx, done_rx) = mpsc::channel::<()>();
    let set_manager = Arc::clone(&manager);
    let subset = std::thread::spawn(move || {
        set_manager
            .with_partition_set_mutation_lock(
                TEST_DB,
                TEST_TABLE,
                &["20250102", "20250101"],
                || Ok(()),
            )
            .unwrap();
        done_tx.send(()).unwrap();
    });

    assert!(
        done_rx.try_recv().is_err(),
        "subset lock must wait for its member partitions"
    );

    drop(held_guard);
    done_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("subset lock must proceed after release");
    subset.join().unwrap();
    Ok(())
}

#[test]
fn wide_lock_excludes_partition_set_writers() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let manager = Arc::new(SegmentManager::new(dir.path())?);
    let (entered_tx, entered_rx) = mpsc::channel::<()>();
    let (release_tx, release_rx) = mpsc::channel::<()>();

    let gate_manager = Arc::clone(&manager);
    let gate = std::thread::spawn(move || {
        gate_manager
            .with_table_mutation_lock(TEST_DB, TEST_TABLE, || {
                entered_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                Ok(())
            })
            .unwrap();
    });
    entered_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("wide lock must be acquirable");

    let (done_tx, done_rx) = mpsc::channel::<()>();
    let set_manager = Arc::clone(&manager);
    let subset = std::thread::spawn(move || {
        set_manager
            .with_partition_set_mutation_lock(TEST_DB, TEST_TABLE, &["p1"], || Ok(()))
            .unwrap();
        done_tx.send(()).unwrap();
    });

    assert!(
        done_rx.try_recv().is_err(),
        "partition set writers must respect the wide gate"
    );

    release_tx.send(()).unwrap();
    gate.join().unwrap();
    done_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("subset lock must proceed after the wide lock releases");
    subset.join().unwrap();
    Ok(())
}

#[test]
fn overlapping_partition_sets_serialize_on_shared_members() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let manager = Arc::new(SegmentManager::new(dir.path())?);
    let (entered_tx, entered_rx) = mpsc::channel::<()>();
    let (release_tx, release_rx) = mpsc::channel::<()>();

    let set_manager = Arc::clone(&manager);
    let first = std::thread::spawn(move || {
        set_manager
            .with_partition_set_mutation_lock(TEST_DB, TEST_TABLE, &["p1", "p2"], || {
                entered_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                Ok(())
            })
            .unwrap();
    });
    entered_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("first subset must be acquirable");

    let blocked = manager.try_with_partition_mutation_lock(TEST_DB, TEST_TABLE, "p2", || Ok(()));
    assert!(
        blocked.unwrap().is_none(),
        "shared member partition must be locked by the first subset"
    );
    let free = manager.try_with_partition_mutation_lock(TEST_DB, TEST_TABLE, "p3", || Ok(9));
    assert_eq!(
        free.unwrap(),
        Some(9),
        "non-overlapping partitions stay available"
    );

    release_tx.send(()).unwrap();
    first.join().unwrap();
    Ok(())
}
