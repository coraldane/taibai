use super::write::{fingerprints_stable, segment_fingerprints};
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
fn delete_during_unlocked_scan_is_detected_by_fingerprint_check() {
    let (_dir, engine, mut session) = create_engine();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE fingerprint_t (id INT64 PRIMARY KEY)",
    );
    insert_rows_single_col(&engine, &mut session, "fingerprint_t", 1, 400);
    engine.flush_table_buffers("main", "fingerprint_t").unwrap();
    exec(&engine, &mut session, "OPTIMIZE TABLE fingerprint_t");

    let segment_mgr = engine.segment_mgr();
    let table_dir = engine.data_dir().join("main").join("fingerprint_t");
    let before = segment_mgr
        .discover_inactive_segments("main", "fingerprint_t", &table_dir)
        .unwrap();
    assert!(
        !before.is_empty(),
        "OPTIMIZE must leave inactive segments for this test"
    );
    let prints = segment_fingerprints(segment_mgr, &before).unwrap();
    assert!(fingerprints_stable(&prints, segment_mgr, &before).unwrap());

    // A delete rewrites the immutable segment footer: row count unchanged,
    // deleted count bumped — exactly the race the locked phase must catch.
    exec(
        &engine,
        &mut session,
        "DELETE FROM fingerprint_t WHERE id = 1",
    );
    let after = segment_mgr
        .discover_inactive_segments("main", "fingerprint_t", &table_dir)
        .unwrap();
    assert!(!fingerprints_stable(&prints, segment_mgr, &after).unwrap());

    // The full two-phase call still succeeds on the changed table (it
    // re-fingerprints after the delete, then falls back or fast-paths
    // consistently) and the snapshot stays exact.
    let meta = table_meta(&engine, "main", "fingerprint_t");
    let schema = crate::types::batch::Schema::new(meta.columns.clone());
    super::rebuild_table_location_indexes_two_phase(
        engine.index_manager(),
        segment_mgr,
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
