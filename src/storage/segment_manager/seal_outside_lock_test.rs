use super::*;
use crate::protocol::types::DataType;
use crate::types::batch::ColumnDef;
use crate::types::scalar::ScalarValue;

fn text_batch(rows: usize) -> RecordBatch {
    let schema = Schema::new(vec![ColumnDef::new("value", DataType::VarChar)]);
    let mut batch = RecordBatch::with_capacity(schema, rows);
    for i in 0..rows {
        batch
            .column_mut(0)
            .unwrap()
            .push(&ScalarValue::VarChar(Some(format!(
                "payload-{:012x}-{}",
                (i as u64) * 2654435761u64,
                "x".repeat(48)
            ))))
            .unwrap();
    }
    batch
}

fn table_state(manager: &SegmentManager) -> Arc<TableSegmentState> {
    manager.get_or_create_state(&table_key("main", "t"))
}

#[test]
fn wide_gate_waits_for_in_flight_seal() {
    let dir = tempfile::tempdir().unwrap();
    let manager = Arc::new(SegmentManager::new(dir.path()).unwrap());
    let state = table_state(&manager);
    let sealing_path = dir.path().join("main").join("t").join("p1_0009.dat");
    state.register_sealing_segment("p1", sealing_path.clone());

    let entered = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let gate_manager = Arc::clone(&manager);
    let entered_flag = Arc::clone(&entered);
    let gate = std::thread::spawn(move || {
        gate_manager
            .with_table_mutation_lock("main", "t", || {
                entered_flag.store(true, Ordering::SeqCst);
                Ok::<(), TaibaiError>(())
            })
            .unwrap();
    });

    std::thread::sleep(Duration::from_millis(200));
    assert!(
        !entered.load(Ordering::SeqCst),
        "wide gate must not enter while a seal is in flight"
    );

    state.finish_sealing_segment(&sealing_path);
    gate.join().unwrap();
    assert!(entered.load(Ordering::SeqCst));
}

#[test]
fn try_gate_declines_during_in_flight_seal() {
    let dir = tempfile::tempdir().unwrap();
    let manager = Arc::new(SegmentManager::new(dir.path()).unwrap());
    let state = table_state(&manager);
    let sealing_path = dir.path().join("main").join("t").join("p1_0001.dat");
    state.register_sealing_segment("p1", sealing_path.clone());

    let outcome = manager
        .try_with_table_mutation_lock("main", "t", || Ok::<(), TaibaiError>(()))
        .unwrap();
    assert!(outcome.is_none(), "try gate must decline while sealing");

    state.finish_sealing_segment(&sealing_path);
    let outcome = manager
        .try_with_table_mutation_lock("main", "t", || Ok::<(), TaibaiError>(()))
        .unwrap();
    assert!(outcome.is_some(), "try gate must proceed once drained");
}

#[test]
fn find_unsealed_skips_paths_mid_seal() {
    let dir = tempfile::tempdir().unwrap();
    let manager = SegmentManager::new(dir.path()).unwrap();
    let state = table_state(&manager);
    let table_dir = dir.path().join("main").join("t");
    // `find_unsealed_segment` serves the unpartitioned bucket, so the
    // resumable target must be a regular segment (empty partition key);
    // partition files are that search's explicit non-goals now.
    manager
        .bulk_insert_batch_with_partition("main", "t", "", text_batch(8))
        .unwrap();

    let active_path = {
        let guard = state.active_segments.lock().unwrap();
        guard.get("").expect("active segment").path.clone()
    };

    let empty = HashSet::new();
    let found = SegmentManager::find_unsealed_segment(&table_dir, u64::MAX, &empty).unwrap();
    assert!(found.is_some(), "sanity: the small segment is resumable");

    state.register_sealing_segment("", active_path.clone());
    let found = SegmentManager::find_unsealed_segment(&table_dir, u64::MAX, &state.sealing_paths())
        .unwrap();
    assert!(
        found.is_none(),
        "a path mid-seal must never be resumed for append"
    );

    state.finish_sealing_segment(&active_path);
    let found = SegmentManager::find_unsealed_segment(&table_dir, u64::MAX, &empty).unwrap();
    assert!(found.is_some(), "resumable again once the seal finished");
}

#[test]
fn bulk_write_to_other_partition_proceeds_during_seal() {
    let dir = tempfile::tempdir().unwrap();
    let manager = Arc::new(SegmentManager::new(dir.path()).unwrap());
    let state = table_state(&manager);
    let table_dir = dir.path().join("main").join("t");
    std::fs::create_dir_all(&table_dir).unwrap();
    let sealing_path = table_dir.join("p1_0007.dat");
    state.register_sealing_segment("p1", sealing_path.clone());

    let (tx, rx) = std::sync::mpsc::channel();
    let writer_manager = Arc::clone(&manager);
    std::thread::spawn(move || {
        writer_manager
            .bulk_insert_batch_with_partition("main", "t", "p2", text_batch(64))
            .unwrap();
        tx.send(()).unwrap();
    });

    rx.recv_timeout(Duration::from_secs(2))
        .expect("write to another partition must not wait for an in-flight seal");
    state.finish_sealing_segment(&sealing_path);
}

#[test]
fn interleaved_partition_batches_with_forced_seals_stay_readable() {
    let dir = tempfile::tempdir().unwrap();
    let manager = Arc::new(
        SegmentManager::with_config(
            dir.path(),
            SegmentConfig {
                min_segment_size: 1,
                max_segment_size: 24 * 1024,
                row_group_target_rows: 1024,
            },
        )
        .unwrap(),
    );
    let table_dir = dir.path().join("main").join("t");

    const BATCHES: usize = 6;
    const ROWS: usize = 4096;

    let left = Arc::clone(&manager);
    let left_thread = std::thread::spawn(move || {
        for _ in 0..BATCHES {
            left.bulk_insert_batch_with_partition("main", "t", "pA", text_batch(ROWS))
                .unwrap();
        }
    });
    let right = Arc::clone(&manager);
    let right_thread = std::thread::spawn(move || {
        for _ in 0..BATCHES {
            right
                .bulk_insert_batch_with_partition("main", "t", "pB", text_batch(ROWS))
                .unwrap();
        }
    });
    left_thread.join().unwrap();
    right_thread.join().unwrap();

    let paths = SegmentManager::discover_segments(&table_dir).unwrap();
    let mut total_rows = 0u64;
    for path in &paths {
        total_rows += manager.open_reader(path).unwrap().row_count();
    }
    assert_eq!(
        total_rows,
        (2 * BATCHES * ROWS) as u64,
        "every row must survive the forced seals"
    );
    assert!(
        paths.len() > 2,
        "tiny max_segment_size must have produced multiple sealed segments"
    );
}
