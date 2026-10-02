//! Crash-window invariant regressions from the structural audit (2026-10,
//! "same class, new site" round): each test runs a REAL statement path and
//! asserts the invariant that the class of bug violated — no duplicates, no
//! lost rows, no leftover arbitration state after the statement acks.

use super::*;

fn result_rows(result: ExecResult) -> usize {
    let ExecResult::Query { batches, .. } = result else {
        panic!("expected query result");
    };
    batches.iter().map(|batch| batch.num_rows()).sum()
}

fn table_dir_of(engine: &Engine, table: &str) -> std::path::PathBuf {
    engine.data_dir().join("main").join(table)
}

fn leftover_statement_journals(engine: &Engine, table: &str) -> Vec<String> {
    let dir = table_dir_of(engine, table);
    let mut leftover = Vec::new();
    if let Ok(entries) = std::fs::read_dir(&dir) {
        for entry in entries.flatten() {
            if let Some(name) = entry.file_name().to_str() {
                if name.ends_with(".del") {
                    leftover.push(name.to_string());
                }
            }
        }
    }
    leftover
}

fn leftover_guards(engine: &Engine, table: &str) -> Vec<String> {
    let dir = table_dir_of(engine, table).join("memtable_tail");
    let mut leftover = Vec::new();
    if let Ok(entries) = std::fs::read_dir(&dir) {
        for entry in entries.flatten() {
            if let Some(name) = entry.file_name().to_str() {
                if name.ends_with(".guard") {
                    leftover.push(name.to_string());
                }
            }
        }
    }
    leftover
}

/// P0 (staging watermark): the watermark handed to deferred journals must be
/// collected AFTER the staging writes created the transaction's memtables.
/// The pre-structural-fix UPDATE collected it before staging — always empty
/// — so recovery could only ever roll back, resurrecting updated rows next
/// to their committed images. This pins the single staging entry's contract
/// directly: a staging that wrote rows must produce a non-empty watermark
/// referencing exactly those rows.
#[test]
fn staged_image_transaction_collects_watermark_after_staging() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut writer = session();

    engine.execute(&mut writer, "CREATE TABLE t (id INT64, v INT64)")?;
    let table_meta = engine.get_table_meta("main", "t").unwrap();
    let schema = crate::types::batch::Schema::new(table_meta.columns.clone());
    let mut batch = crate::types::batch::RecordBatch::with_capacity(schema.clone(), 2);
    for id in 1..=2i64 {
        batch
            .column_mut(0)
            .unwrap()
            .push(&ScalarValue::Int64(Some(id)))?;
        batch
            .column_mut(1)
            .unwrap()
            .push(&ScalarValue::Int64(Some(id * 10)))?;
    }

    let txn_id = engine.alloc_txn_id();
    let staged = crate::sql::execution::dml_staging::stage_image_transaction(
        &engine,
        "main",
        "t",
        txn_id,
        |txn| {
            crate::sql::execution::dml_insert::write_batch_by_partition(
                &engine,
                "main",
                "t",
                &table_meta,
                batch,
                schema,
                txn,
                true,
                false,
            )
        },
    )
    .map_err(|(_, error)| error)?;
    assert!(
        !staged.watermark.as_slice().is_empty(),
        "the watermark must reference the memtables the staging just created"
    );
    assert!(
        staged
            .watermark
            .as_slice()
            .iter()
            .all(|check| check.expected_rows == 2),
        "watermark row counts must match the staged rows, got {:?}",
        staged.watermark.as_slice()
    );
    let _ = engine.batch_writer().rollback_txn(staged.txn_id);
    Ok(())
}

/// P0 (REPLACE memtable leg): the guarded `.del` sidecar is written before
/// the replacement rows' tail fsync; a crash in that window must restore the
/// old rows, and the acked path must leave neither journal nor guard. Runs
/// the real REPLACE flow and restarts.
#[test]
fn replace_memtable_leg_acks_clean_and_survives_restart() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut writer = session();

    engine.execute(
        &mut writer,
        "CREATE TABLE t (id INT64, name VARCHAR, UNIQUE KEY uk_id (id))",
    )?;
    // Small insert: the row stays memtable-resident with a durable tail —
    // exactly the leg the guard protects.
    engine.execute(&mut writer, "INSERT INTO t (id, name) VALUES (1, 'first')")?;
    engine.execute(&mut writer, "REPLACE INTO t (id, name) VALUES (1, 'second')")?;

    assert!(
        leftover_statement_journals(&engine, "t").is_empty(),
        "no statement journal may outlive the REPLACE"
    );
    assert!(
        leftover_guards(&engine, "t").is_empty(),
        "no statement guard may outlive the REPLACE (it would gate flushes forever)"
    );

    drop(engine);
    let engine = Engine::new(dir.path(), "main")?;
    let mut reader = session();
    let count = first_i64(engine.execute(&mut reader, "SELECT COUNT(*) FROM t")?, 0, 0);
    assert_eq!(count, 1, "exactly one row after REPLACE and restart");
    let name = first_text(engine.execute(&mut reader, "SELECT name FROM t WHERE id = 1")?, 0, 0);
    assert_eq!(name, "second");
    Ok(())
}

/// P0 (explicit-transaction COMMIT): materialization rewrites segment
/// footers (now under DEFERRED journals with the transaction's own
/// memtables as watermark) before the commit's tail fsync. The acked COMMIT
/// must leave no journal or guard behind, and a restart must see the
/// transaction's delete+insert exactly.
#[test]
fn txn_commit_materialization_acks_clean_and_survives_restart() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut writer = session();

    engine.execute(&mut writer, "CREATE TABLE t (id INT64, v INT64)")?;
    engine.execute(&mut writer, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40)")?;
    engine.execute(&mut writer, "FLUSH TABLE t")?;

    engine.execute(&mut writer, "BEGIN")?;
    engine.execute(&mut writer, "DELETE FROM t WHERE id <= 2")?;
    engine.execute(&mut writer, "INSERT INTO t VALUES (5, 50)")?;
    engine.execute(&mut writer, "COMMIT")?;

    assert!(
        leftover_statement_journals(&engine, "t").is_empty(),
        "no statement journal may outlive the COMMIT"
    );
    assert!(
        leftover_guards(&engine, "t").is_empty(),
        "no statement guard may outlive the COMMIT"
    );

    drop(engine);
    let engine = Engine::new(dir.path(), "main")?;
    let mut reader = session();
    let count = first_i64(engine.execute(&mut reader, "SELECT COUNT(*) FROM t")?, 0, 0);
    assert_eq!(count, 3, "delete + insert of a committed txn recover exactly");
    for id in [3i64, 4, 5] {
        let found = first_i64(
            engine.execute(&mut reader, &format!("SELECT COUNT(*) FROM t WHERE id = {id}"))?,
            0,
            0,
        );
        assert_eq!(found, 1, "row {id} must survive the restart");
    }
    Ok(())
}

/// A transaction whose COMMIT never happened must leave NO durable trace of
/// its deferred deletes: the guarded sidecar and the deferred journal both
/// roll back at restart (the watermark — the txn's own never-durable
/// memtables — is unsatisfiable).
#[test]
fn uncommitted_txn_deferred_delete_rolls_back_completely() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut writer = session();

    engine.execute(&mut writer, "CREATE TABLE t (id INT64, v INT64)")?;
    engine.execute(&mut writer, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")?;
    engine.execute(&mut writer, "FLUSH TABLE t")?;

    // BEGIN + DELETE + INSERT, then simulate a crash before COMMIT's fsync:
    // dropping the engine without COMMIT leaves the memtable deletes'
    // sidecars (if any) guarded by the txn's watermark.
    engine.execute(&mut writer, "BEGIN")?;
    engine.execute(&mut writer, "DELETE FROM t WHERE id = 1")?;
    engine.execute(&mut writer, "INSERT INTO t VALUES (9, 90)")?;
    drop(writer);
    drop(engine);

    let engine = Engine::new(dir.path(), "main")?;
    let mut reader = session();
    let count = first_i64(engine.execute(&mut reader, "SELECT COUNT(*) FROM t")?, 0, 0);
    assert_eq!(count, 3, "the aborted txn's delete must roll back completely");
    let found = first_i64(
        engine.execute(&mut reader, "SELECT COUNT(*) FROM t WHERE id = 1")?,
        0,
        0,
    );
    assert_eq!(found, 1, "the deleted row must be restored");
    let gone = first_i64(
        engine.execute(&mut reader, "SELECT COUNT(*) FROM t WHERE id = 9")?,
        0,
        0,
    );
    assert_eq!(gone, 0, "the uncommitted insert must not be durable");
    Ok(())
}

/// P0 (SELECT scan paths): the executor (inline + parallel) and cursor once
/// fed catalog-space predicate indices straight into footer-space zone
/// maps. After DROP COLUMN shifted every catalog index left, the wrong
/// column's zone map decided "no match" and matching rows silently
/// vanished. Same shape as the UPDATE variant in audit_regression, pinning
/// the read side.
#[test]
fn select_scan_paths_align_predicates_after_drop_column() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut writer = session();

    // `id` values far from the `a` predicate value: with the pre-fix
    // positional probe, `a = 1` consulted id's zone map [100,102] and
    // pruned the row groups.
    engine.execute(&mut writer, "CREATE TABLE t (id INT64, a INT64, v INT64)")?;
    engine.execute(
        &mut writer,
        "INSERT INTO t VALUES (100, 1, 10), (101, 1, 11), (102, 2, 12)",
    )?;
    engine.flush_table_buffers("main", "t")?;
    engine.execute(&mut writer, "ALTER TABLE t DROP COLUMN id")?;

    // Executor inline path (single segment).
    let rows = result_rows(engine.execute(&mut writer, "SELECT v FROM t WHERE a = 1")?);
    assert_eq!(rows, 2, "both a = 1 rows must be returned by the scan");

    // Count fast path.
    let count = first_i64(
        engine.execute(&mut writer, "SELECT COUNT(*) FROM t WHERE a = 1")?,
        0,
        0,
    );
    assert_eq!(count, 2, "COUNT must agree with the scan");

    // Executor parallel path (second segment makes segments.len() > 1).
    engine.execute(&mut writer, "INSERT INTO t (a, v) VALUES (3, 13)")?;
    engine.execute(&mut writer, "FLUSH TABLE t")?;
    let rows = result_rows(engine.execute(&mut writer, "SELECT v FROM t WHERE a >= 1")?);
    assert_eq!(rows, 4, "every row must be visible from every segment");
    Ok(())
}
