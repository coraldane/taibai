use super::Engine;
use crate::net::session::Session;
use crate::sql::execution::engine::ExecResult;
use crate::types::scalar::ScalarValue;

fn create_engine() -> (tempfile::TempDir, Engine, Session) {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main").unwrap();
    let session = Session::new("root".into(), "main".into());
    (dir, engine, session)
}

fn exec(engine: &Engine, session: &mut Session, sql: &str) -> ExecResult {
    engine
        .execute(session, sql)
        .unwrap_or_else(|e| panic!("sql failed: {sql}: {e}"))
}

fn count_rows(engine: &Engine, session: &mut Session, table: &str) -> i64 {
    match exec(engine, session, &format!("SELECT COUNT(*) FROM {table}")) {
        ExecResult::Query { batches, .. } => {
            let mut total = 0i64;
            for batch in batches {
                for row in 0..batch.num_rows() {
                    match batch.scalar(row, 0) {
                        Some(ScalarValue::Int64(Some(n))) => total += n,
                        Some(ScalarValue::Int32(Some(n))) => total += n as i64,
                        other => panic!("unexpected count scalar: {other:?}"),
                    }
                }
            }
            total
        }
        _ => panic!("expected query result"),
    }
}

fn table_meta(
    engine: &Engine,
    db: &str,
    table: &str,
) -> crate::storage::catalog::schema::TableMeta {
    engine
        .catalogs()
        .read()
        .unwrap()
        .get(db)
        .and_then(|catalog| catalog.get_table(table))
        .cloned()
        .unwrap_or_else(|| panic!("table {db}.{table} missing"))
}

fn insert_rows(engine: &Engine, session: &mut Session, table: &str, start: i64, count: i64) {
    for id in start..start + count {
        exec(
            engine,
            session,
            &format!("INSERT INTO {table} VALUES ({id}, {})", id * 7),
        );
    }
}

fn insert_rows_single_col(
    engine: &Engine,
    session: &mut Session,
    table: &str,
    start: i64,
    count: i64,
) {
    for id in start..start + count {
        exec(
            engine,
            session,
            &format!("INSERT INTO {table} VALUES ({id})"),
        );
    }
}

#[test]
fn two_phase_rebuild_covers_immutable_segments_and_live_memtables() {
    let (_dir, engine, mut session) = create_engine();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE two_phase_t (id INT64 PRIMARY KEY, code INT64, UNIQUE KEY uk_code (code))",
    );
    insert_rows(&engine, &mut session, "two_phase_t", 1, 300);
    engine.flush_table_buffers("main", "two_phase_t").unwrap();
    // These stay in the live memtable and must be picked up by the locked
    // phase; the dup probe below fails if the snapshot misses them.
    insert_rows(&engine, &mut session, "two_phase_t", 301, 50);

    let meta = table_meta(&engine, "main", "two_phase_t");
    let schema = crate::types::batch::Schema::new(meta.columns.clone());
    super::rebuild_table_location_indexes_two_phase(
        engine.index_manager(),
        engine.segment_mgr(),
        engine.memtable_manager(),
        engine.data_dir(),
        "main",
        "two_phase_t",
        &meta,
        &schema,
    )
    .unwrap();

    assert_eq!(count_rows(&engine, &mut session, "two_phase_t"), 350);
    // Unique enforcement must see flushed AND memtable rows through the
    // freshly published base (publish clears the delta files).
    assert!(
        engine
            .execute(&mut session, "INSERT INTO two_phase_t VALUES (1000, 42)")
            .is_err(),
        "duplicate of memtable-row code=42 must be rejected after rebuild"
    );
    engine
        .execute(&mut session, "INSERT INTO two_phase_t VALUES (1001, 9999)")
        .unwrap();
}

#[test]
fn optimize_table_uses_two_phase_rebuild_and_keeps_unique_enforcement() {
    let (_dir, engine, mut session) = create_engine();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE optimize_t (id INT64 PRIMARY KEY, code INT64, UNIQUE KEY uk_code (code))",
    );
    insert_rows(&engine, &mut session, "optimize_t", 1, 400);
    engine.flush_table_buffers("main", "optimize_t").unwrap();
    exec(&engine, &mut session, "OPTIMIZE TABLE optimize_t");
    assert_eq!(count_rows(&engine, &mut session, "optimize_t"), 400);
    assert!(engine
        .execute(&mut session, "INSERT INTO optimize_t VALUES (5000, 7)")
        .is_err());
    engine
        .execute(&mut session, "INSERT INTO optimize_t VALUES (5001, 7001)")
        .unwrap();
}

#[test]
fn memtable_drift_is_detected_without_a_segment_epoch_change() {
    let (_dir, engine, mut session) = create_engine();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE memtable_drift_t (id INT64 PRIMARY KEY)",
    );
    insert_rows_single_col(&engine, &mut session, "memtable_drift_t", 1, 10);
    let before = engine
        .segment_mgr()
        .memtable_flush_epoch("main", "memtable_drift_t");
    super::FORCE_TWO_PHASE_MEMTABLE_STALE.with(|force| force.set(true));
    let meta = table_meta(&engine, "main", "memtable_drift_t");
    let schema = crate::types::batch::Schema::new(meta.columns.clone());
    let error = super::rebuild_table_location_indexes_two_phase(
        engine.index_manager(),
        engine.segment_mgr(),
        engine.memtable_manager(),
        engine.data_dir(),
        "main",
        "memtable_drift_t",
        &meta,
        &schema,
    )
    .unwrap_err();
    assert!(matches!(
        error,
        crate::common::error::TaibaiError::ResourceBusy(_)
    ));
    assert_eq!(
        engine
            .segment_mgr()
            .memtable_flush_epoch("main", "memtable_drift_t"),
        before,
        "the test hook changes only memtable content version"
    );
    super::rebuild_table_location_indexes_two_phase(
        engine.index_manager(),
        engine.segment_mgr(),
        engine.memtable_manager(),
        engine.data_dir(),
        "main",
        "memtable_drift_t",
        &meta,
        &schema,
    )
    .unwrap();
    assert!(engine
        .execute(&mut session, "INSERT INTO memtable_drift_t VALUES (1)")
        .is_err());
}

#[test]
fn drift_aborts_without_locked_full_scan_fallback() {
    let (_dir, engine, mut session) = create_engine();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE fingerprint_t (id INT64 PRIMARY KEY)",
    );
    insert_rows_single_col(&engine, &mut session, "fingerprint_t", 1, 400);
    engine.flush_table_buffers("main", "fingerprint_t").unwrap();
    exec(&engine, &mut session, "OPTIMIZE TABLE fingerprint_t");

    // A delete rewrites an immutable segment footer. Force one additional
    // epoch change after the unlocked scan to deterministically exercise the
    // old fallback point. The new protocol must return Stale/Retry instead of
    // succeeding via a full scan inside the mutation lock.
    exec(
        &engine,
        &mut session,
        "DELETE FROM fingerprint_t WHERE id = 1",
    );
    super::FORCE_TWO_PHASE_REBUILD_STALE.with(|force| force.set(true));
    let meta = table_meta(&engine, "main", "fingerprint_t");
    let schema = crate::types::batch::Schema::new(meta.columns.clone());
    let error = super::rebuild_table_location_indexes_two_phase(
        engine.index_manager(),
        engine.segment_mgr(),
        engine.memtable_manager(),
        engine.data_dir(),
        "main",
        "fingerprint_t",
        &meta,
        &schema,
    )
    .unwrap_err();
    match error {
        crate::common::error::TaibaiError::ResourceBusy(message) => {
            assert!(message.contains("stale"), "{message}");
            assert!(message.contains("retry"), "{message}");
        }
        other => panic!("expected retryable stale build, got {other}"),
    }

    // A later unlocked attempt succeeds and publishes an exact snapshot.
    super::rebuild_table_location_indexes_two_phase(
        engine.index_manager(),
        engine.segment_mgr(),
        engine.memtable_manager(),
        engine.data_dir(),
        "main",
        "fingerprint_t",
        &meta,
        &schema,
    )
    .unwrap();
    assert_eq!(count_rows(&engine, &mut session, "fingerprint_t"), 399);
    engine
        .execute(&mut session, "INSERT INTO fingerprint_t VALUES (1)")
        .unwrap();
    assert!(engine
        .execute(&mut session, "INSERT INTO fingerprint_t VALUES (2)")
        .is_err());
}
