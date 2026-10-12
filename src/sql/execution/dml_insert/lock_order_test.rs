#[test]
fn replace_does_not_publish_catalog_while_holding_table_lock() {
    let source = include_str!("write.rs");
    let replace = source
        .split_once("pub fn exec_replace(")
        .unwrap()
        .1
        .split_once("pub fn exec_insert(")
        .unwrap()
        .0;
    let locked = replace
        .split_once("with_table_mutation_lock")
        .unwrap()
        .1
        .split_once("})?;")
        .unwrap()
        .0;
    assert!(!locked.contains("record_inserted_rows"));
    assert!(!locked.contains("persist_auto_increment_next_value"));
    assert!(!locked.contains("finalize_replace_delete("));
}

#[test]
fn cancelled_insert_stops_waiting_for_constraint_lock() {
    use crate::common::query_cancel::{QueryCancellationGuard, QueryContext};
    use std::sync::{atomic::AtomicBool, Arc};
    use std::time::Duration;
    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(super::Engine::new(dir.path(), "main").unwrap());
    let mut session = crate::net::session::Session::new("root".into(), "main".into());
    engine
        .execute(
            &mut session,
            "CREATE TABLE cancel_constraint (id INT64 PRIMARY KEY)",
        )
        .unwrap();
    let held = super::constraint_insert_lock("main", "cancel_constraint")
        .lock()
        .unwrap();
    let context = Arc::new(QueryContext::new(Arc::new(AtomicBool::new(false))));
    let worker_context = Arc::clone(&context);
    let worker_engine = Arc::clone(&engine);
    let (tx, rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        let _guard = QueryCancellationGuard::enter(worker_context);
        tx.send(worker_engine.execute(&mut session, "INSERT INTO cancel_constraint VALUES (1)"))
            .unwrap();
    });
    std::thread::sleep(Duration::from_millis(100));
    context.cancel();
    let early = rx.recv_timeout(Duration::from_millis(500));
    drop(held);
    worker.join().unwrap();
    let result =
        early.expect("cancelled INSERT must return before the constraint lock is released");
    assert!(result.unwrap_err().to_string().contains("cancelled"));
    assert_eq!(
        engine
            .get_table_meta("main", "cancel_constraint")
            .unwrap()
            .row_count,
        0
    );
    engine.segment_mgr().shutdown();
}
