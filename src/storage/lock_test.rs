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
