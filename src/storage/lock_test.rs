use super::*;

#[test]
fn blocked_table_state_does_not_block_global_registry() {
    let dir = tempfile::tempdir().unwrap();
    let manager = Arc::new(SegmentManager::new(dir.path()).unwrap());
    let state = manager.get_or_create_state(&table_key("main", "blocked"));
    let active_guard = state.active_segments.lock().unwrap();

    let blocked_manager = Arc::clone(&manager);
    let blocked =
        std::thread::spawn(move || blocked_manager.active_partition_keys("main", "blocked"));
    std::thread::sleep(std::time::Duration::from_millis(50));

    let (tx, rx) = std::sync::mpsc::channel();
    let registry_manager = Arc::clone(&manager);
    let registry = std::thread::spawn(move || {
        registry_manager.get_or_create_state(&table_key("main", "other"));
        tx.send(()).unwrap();
    });

    rx.recv_timeout(std::time::Duration::from_secs(1))
        .expect("global table registry was blocked by another table's mutex");
    drop(active_guard);
    blocked.join().unwrap();
    registry.join().unwrap();
}

// ---------------------------------------------------------------------------
// Segment replacement journal recovery
// ---------------------------------------------------------------------------

#[test]
fn replace_journal_recovery_retires_old_segments() {
    let tmp = tempfile::tempdir().unwrap();
    let table_dir = tmp.path().join("main").join("t");
    std::fs::create_dir_all(&table_dir).unwrap();
    let final_path = table_dir.join("seg_00003.dat");
    let old_a = table_dir.join("seg_00001.dat");
    let old_b = table_dir.join("seg_00002.dat");
    std::fs::write(&final_path, b"new").unwrap();
    std::fs::write(&old_a, b"old-a").unwrap();
    std::fs::write(&old_b, b"old-b").unwrap();

    // Simulate a crash after the rename but before the old removals.
    crate::storage::segment_manager::manager_core_test_support::write_replace_journal_for_test(
        &crate::storage::segment_manager::manager_core_test_support::replace_journal_path_for_test(
            &final_path,
        ),
        &[old_a.clone(), old_b.clone()],
    )
    .unwrap();
    assert!(
        crate::storage::segment_manager::manager_core_test_support::replace_journal_path_for_test(
            &final_path
        )
        .exists()
    );

    crate::storage::segment_manager::manager_core_test_support::recover_replace_journals_for_test(
        &table_dir,
    );

    assert!(final_path.exists(), "replacement survives");
    assert!(!old_a.exists(), "retired old segment A");
    assert!(!old_b.exists(), "retired old segment B");
    assert!(
        !crate::storage::segment_manager::manager_core_test_support::replace_journal_path_for_test(
            &final_path
        )
        .exists(),
        "journal removed after recovery"
    );
}

#[test]
fn replace_journal_recovery_discards_when_rename_never_happened() {
    let tmp = tempfile::tempdir().unwrap();
    let table_dir = tmp.path().join("main").join("t");
    std::fs::create_dir_all(&table_dir).unwrap();
    let final_path = table_dir.join("seg_00003.dat");
    let old_a = table_dir.join("seg_00001.dat");
    std::fs::write(&old_a, b"old-a").unwrap();

    let journal =
        crate::storage::segment_manager::manager_core_test_support::replace_journal_path_for_test(
            &final_path,
        );
    crate::storage::segment_manager::manager_core_test_support::write_replace_journal_for_test(
        &journal,
        &[old_a.clone()],
    )
    .unwrap();

    crate::storage::segment_manager::manager_core_test_support::recover_replace_journals_for_test(
        &table_dir,
    );

    assert!(!final_path.exists(), "rename never happened");
    assert!(old_a.exists(), "old segment untouched");
    assert!(!journal.exists(), "journal discarded");
}
