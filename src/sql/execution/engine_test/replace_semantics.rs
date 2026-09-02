use super::*;

#[test]
fn delete_then_insert_same_unique_key_after_rebuild() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();

    engine.execute(
        &mut session,
        "CREATE TABLE t (id INT64, name VARCHAR, UNIQUE KEY uk_id (id))",
    )?;
    engine.execute(&mut session, "INSERT INTO t (id, name) VALUES (1, 'a')")?;
    engine.execute(&mut session, "REBUILD UNIQUE INDEX uk_id ON t")?;
    let index_dir = dir
        .path()
        .join("main")
        .join("t")
        .join("indexes")
        .join("unique");
    let base_file = index_dir.join("uk_id.keys.tbi");
    let invalid_file = index_dir.join("uk_id.invalid");
    engine.execute(&mut session, "DELETE FROM t WHERE id = 1")?;
    assert!(base_file.exists());
    assert!(!invalid_file.exists());
    assert!(index_dir.join("uk_id.delta.tbi").exists());
    assert_eq!(
        engine.index_manager().unique_key_exists(
            "main",
            "t",
            "uk_id",
            &[ScalarValue::Int64(Some(1))]
        )?,
        Some(false)
    );
    engine.execute(&mut session, "INSERT INTO t (id, name) VALUES (1, 'b')")?;

    let name = first_text(
        engine.execute(&mut session, "SELECT name FROM t WHERE id = 1")?,
        0,
        0,
    );
    assert_eq!(name, "b");

    // Flush moves rows into segments; DELETE must maintain the index
    // incrementally for physical rows too (no invalidation marker) and the
    // removal must survive an engine restart via the persisted delta.
    engine.execute(&mut session, "INSERT INTO t (id, name) VALUES (2, 'c')")?;
    engine.execute(&mut session, "FLUSH TABLE t")?;
    engine.execute(&mut session, "DELETE FROM t WHERE id = 1")?;
    assert!(!invalid_file.exists());
    assert_eq!(
        engine.index_manager().unique_key_exists(
            "main",
            "t",
            "uk_id",
            &[ScalarValue::Int64(Some(1))]
        )?,
        Some(false)
    );
    assert_eq!(
        engine.index_manager().unique_key_exists(
            "main",
            "t",
            "uk_id",
            &[ScalarValue::Int64(Some(2))]
        )?,
        Some(true)
    );

    drop(engine);
    let engine = Engine::new(dir.path(), "main")?;
    assert_eq!(
        engine.index_manager().unique_key_exists(
            "main",
            "t",
            "uk_id",
            &[ScalarValue::Int64(Some(1))]
        )?,
        Some(false)
    );
    let duplicate = engine.execute(&mut session, "INSERT INTO t (id, name) VALUES (2, 'dup')");
    assert!(duplicate.is_err());
    engine.execute(
        &mut session,
        "REPLACE INTO t (id, name) VALUES (1, 'again')",
    )?;
    let name = first_text(
        engine.execute(&mut session, "SELECT name FROM t WHERE id = 1")?,
        0,
        0,
    );
    assert_eq!(name, "again");
    let count = first_i64(
        engine.execute(&mut session, "SELECT COUNT(*) FROM t")?,
        0,
        0,
    );
    assert_eq!(count, 2);
    Ok(())
}

#[test]
fn replace_into_works_after_rebuild_unique_index() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();

    engine.execute(
        &mut session,
        "CREATE TABLE t (id INT64, name VARCHAR, UNIQUE KEY uk_id (id))",
    )?;
    engine.execute(&mut session, "INSERT INTO t (id, name) VALUES (1, 'old')")?;
    engine.execute(&mut session, "REBUILD UNIQUE INDEX uk_id ON t")?;
    let index_dir = dir
        .path()
        .join("main")
        .join("t")
        .join("indexes")
        .join("unique");
    let base_file = index_dir.join("uk_id.keys.tbi");
    let invalid_file = index_dir.join("uk_id.invalid");
    engine.execute(&mut session, "REPLACE INTO t (id, name) VALUES (1, 'new')")?;
    assert!(base_file.exists());
    assert!(!invalid_file.exists());
    // Replacement rows stay memtable-resident; their pseudo index entries mark
    // existence but carry no segment location, so the probe comes back empty
    // until a flush plus rebuild materializes physical locations.
    let locations = engine
        .index_manager()
        .unique_key_locations("main", "t", "uk_id", &[vec![ScalarValue::Int64(Some(1))]])?
        .unwrap();
    assert!(locations.is_empty());

    engine.execute(
        &mut session,
        "REPLACE INTO t (id, name) VALUES (1, 'newer')",
    )?;
    let locations = engine
        .index_manager()
        .unique_key_locations("main", "t", "uk_id", &[vec![ScalarValue::Int64(Some(1))]])?
        .unwrap();
    assert!(locations.is_empty());

    let name = first_text(
        engine.execute(&mut session, "SELECT name FROM t WHERE id = 1")?,
        0,
        0,
    );
    let count = first_i64(
        engine.execute(&mut session, "SELECT COUNT(*) FROM t")?,
        0,
        0,
    );
    assert_eq!(name, "newer");
    assert_eq!(count, 1);
    Ok(())
}

#[test]
fn replace_deletes_conflicting_memtable_rows_in_place() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();

    engine.execute(
        &mut session,
        "CREATE TABLE t (id INT64, name VARCHAR, UNIQUE KEY uk_id (id))",
    )?;
    // The conflicting row never leaves the memtable before the REPLACE.
    engine.execute(&mut session, "INSERT INTO t VALUES (1, 'old')")?;
    assert_eq!(
        engine.index_manager().unique_key_locations(
            "main",
            "t",
            "uk_id",
            &[vec![ScalarValue::Int64(Some(1))]],
        )?,
        Some(Vec::new())
    );

    // REPLACE must delete the memtable-resident row in place: no flush
    // happens, so the probe still has no segment location for the key.
    engine.execute(&mut session, "REPLACE INTO t VALUES (1, 'new')")?;
    assert_eq!(
        engine.index_manager().unique_key_locations(
            "main",
            "t",
            "uk_id",
            &[vec![ScalarValue::Int64(Some(1))]],
        )?,
        Some(Vec::new())
    );
    let name = first_text(
        engine.execute(&mut session, "SELECT name FROM t WHERE id = 1")?,
        0,
        0,
    );
    assert_eq!(name, "new");
    let count = first_i64(
        engine.execute(&mut session, "SELECT COUNT(*) FROM t")?,
        0,
        0,
    );
    assert_eq!(count, 1);

    // A second REPLACE conflicts with the row the first REPLACE left resident
    // in the memtable.
    engine.execute(&mut session, "REPLACE INTO t VALUES (1, 'newer')")?;
    let name = first_text(
        engine.execute(&mut session, "SELECT name FROM t WHERE id = 1")?,
        0,
        0,
    );
    let count = first_i64(
        engine.execute(&mut session, "SELECT COUNT(*) FROM t")?,
        0,
        0,
    );
    assert_eq!(name, "newer");
    assert_eq!(count, 1);

    // After a flush the row lives in a segment; the next REPLACE rebuilds the
    // index (flush-epoch mismatch) and deletes the segment row directly.
    engine.execute(&mut session, "FLUSH TABLE t")?;
    engine.execute(&mut session, "REPLACE INTO t VALUES (1, 'final')")?;
    let name = first_text(
        engine.execute(&mut session, "SELECT name FROM t WHERE id = 1")?,
        0,
        0,
    );
    let count = first_i64(
        engine.execute(&mut session, "SELECT COUNT(*) FROM t")?,
        0,
        0,
    );
    assert_eq!(name, "final");
    assert_eq!(count, 1);
    Ok(())
}

#[test]
fn primary_key_replace_rebuilds_and_maintains_physical_locations() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();
    let index_name = crate::storage::index::index_manager::PRIMARY_LOCATION_INDEX_NAME;

    engine.execute(
        &mut session,
        "CREATE TABLE t (id INT64 PRIMARY KEY, name VARCHAR)",
    )?;
    assert!(engine
        .index_manager()
        .load_unique_index_snapshot("main", "t", index_name)?
        .is_some());
    engine.execute(&mut session, "INSERT INTO t VALUES (1, 'old')")?;
    // INSERT appends a placeholder delta entry instead of invalidating the
    // primary-key location snapshot.
    assert!(engine
        .index_manager()
        .load_unique_index_snapshot("main", "t", index_name)?
        .is_some());

    engine.execute(&mut session, "REPLACE INTO t VALUES (1, 'new')")?;
    // Replacement rows stay memtable-resident, so location probes skip their
    // pseudo entries until a flush plus rebuild derives physical locations.
    let locations = engine
        .index_manager()
        .unique_key_locations(
            "main",
            "t",
            index_name,
            &[vec![ScalarValue::Int64(Some(1))]],
        )?
        .unwrap();
    assert!(locations.is_empty());

    engine.execute(&mut session, "REPLACE INTO t VALUES (1, 'newer')")?;
    let locations = engine
        .index_manager()
        .unique_key_locations(
            "main",
            "t",
            index_name,
            &[vec![ScalarValue::Int64(Some(1))]],
        )?
        .unwrap();
    assert!(locations.is_empty());

    engine.execute(&mut session, "OPTIMIZE TABLE t")?;
    // OPTIMIZE flushes memtables and rewrites segments, so the location index
    // is rebuilt eagerly and the key now has a physical location again.
    let locations = engine
        .index_manager()
        .unique_key_locations(
            "main",
            "t",
            index_name,
            &[vec![ScalarValue::Int64(Some(1))]],
        )?
        .unwrap();
    assert_eq!(locations.len(), 1);
    assert!(!locations[0].segment_path.is_empty());
    engine.execute(&mut session, "REPLACE INTO t VALUES (1, 'optimized')")?;
    let locations = engine
        .index_manager()
        .unique_key_locations(
            "main",
            "t",
            index_name,
            &[vec![ScalarValue::Int64(Some(1))]],
        )?
        .unwrap();
    assert!(locations.is_empty());
    drop(engine);

    // Restart: shutdown no longer flushes tailed memtables, so the rows are
    // recovered from durable tails, the flush epoch is unchanged, and the
    // probe still reports no physical locations.
    let engine = Engine::new(dir.path(), "main")?;
    let locations = engine
        .index_manager()
        .unique_key_locations(
            "main",
            "t",
            index_name,
            &[vec![ScalarValue::Int64(Some(1))]],
        )?
        .unwrap();
    assert!(locations.is_empty());

    // An explicit flush moves the rows into a segment and advances the flush
    // epoch; the next REPLACE observes the mismatch, rebuilds the index, and
    // keys that were not replaced away then have physical locations again.
    engine.execute(&mut session, "FLUSH TABLE t")?;
    engine.execute(&mut session, "REPLACE INTO t VALUES (2, 'other')")?;
    let locations = engine
        .index_manager()
        .unique_key_locations(
            "main",
            "t",
            index_name,
            &[vec![ScalarValue::Int64(Some(1))]],
        )?
        .unwrap();
    assert_eq!(locations.len(), 1);
    assert!(!locations[0].segment_path.is_empty());
    engine.execute(&mut session, "DELETE FROM t WHERE id = 2")?;
    let name = first_text(
        engine.execute(&mut session, "SELECT name FROM t WHERE id = 1")?,
        0,
        0,
    );
    assert_eq!(name, "optimized");
    Ok(())
}

#[test]
fn replace_memtable_conflict_removes_old_row_without_flushing() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();

    engine.execute(
        &mut session,
        "CREATE TABLE t (id INT64, name VARCHAR, PRIMARY KEY (id))",
    )?;
    engine.execute(
        &mut session,
        "INSERT INTO t VALUES (1, 'one'), (2, 'two'), (3, 'three')",
    )?;
    let epoch_before = engine.segment_mgr().memtable_flush_epoch("main", "t");

    engine.execute(&mut session, "REPLACE INTO t VALUES (1, 'uno')")?;

    // The old memtable-resident version is gone and the replacement is visible.
    let name = first_text(
        engine.execute(&mut session, "SELECT name FROM t WHERE id = 1")?,
        0,
        0,
    );
    assert_eq!(name, "uno");
    let count = first_i64(
        engine.execute(&mut session, "SELECT COUNT(*) FROM t")?,
        0,
        0,
    );
    assert_eq!(count, 3);

    // REPLACE handled the conflict in place: no memtable flush happened...
    assert_eq!(
        engine.segment_mgr().memtable_flush_epoch("main", "t"),
        epoch_before
    );
    // ...so the unique index only tracks the resident row via a pseudo
    // location that the probe filters out.
    let locations = engine
        .index_manager()
        .unique_key_locations(
            "main",
            "t",
            crate::storage::index::index_manager::PRIMARY_LOCATION_INDEX_NAME,
            &[vec![ScalarValue::Int64(Some(1))]],
        )?
        .unwrap();
    assert!(locations.is_empty());
    Ok(())
}

#[test]
fn replace_into_deletes_conflicting_unique_row_before_insert() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();

    engine.execute(
        &mut session,
        "CREATE TABLE t (id INT64, name VARCHAR, UNIQUE KEY uk_id (id))",
    )?;
    engine.execute(&mut session, "INSERT INTO t (id, name) VALUES (1, 'old')")?;
    engine.execute(&mut session, "REPLACE INTO t (id, name) VALUES (1, 'new')")?;

    let name = first_text(
        engine.execute(&mut session, "SELECT name FROM t WHERE id = 1")?,
        0,
        0,
    );
    let count = first_i64(
        engine.execute(&mut session, "SELECT COUNT(*) FROM t")?,
        0,
        0,
    );
    assert_eq!(name, "new");
    assert_eq!(count, 1);
    Ok(())
}

#[test]
fn replace_insert_failure_restores_deleted_rows_and_unique_state() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();

    engine.execute(
            &mut session,
            "CREATE TABLE t (id INT64 PRIMARY KEY, code INT64, name VARCHAR, UNIQUE KEY uk_code (code))",
        )?;
    engine.execute(
        &mut session,
        "INSERT INTO t (id, code, name) VALUES (1, 10, 'old')",
    )?;
    engine.execute(&mut session, "REBUILD UNIQUE INDEX uk_code ON t")?;

    crate::sql::execution::dml_insert::force_next_replace_insert_failure();
    let failed = engine.execute(
        &mut session,
        "REPLACE INTO t (id, code, name) VALUES (1, 10, 'new')",
    );
    assert!(failed.is_err());

    let name = first_text(
        engine.execute(&mut session, "SELECT name FROM t WHERE id = 1")?,
        0,
        0,
    );
    let count = first_i64(
        engine.execute(&mut session, "SELECT COUNT(*) FROM t")?,
        0,
        0,
    );
    assert_eq!(name, "old");
    assert_eq!(count, 1);
    assert!(engine
        .execute(
            &mut session,
            "INSERT INTO t (id, code, name) VALUES (2, 10, 'duplicate')",
        )
        .is_err());

    engine.execute(
        &mut session,
        "REPLACE INTO t (id, code, name) VALUES (1, 10, 'new')",
    )?;
    let name = first_text(
        engine.execute(&mut session, "SELECT name FROM t WHERE id = 1")?,
        0,
        0,
    );
    assert_eq!(name, "new");
    Ok(())
}

#[test]
fn replace_replaces_all_preexisting_duplicate_primary_key_rows() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();

    engine.execute(
        &mut session,
        "CREATE TABLE t (id INT64 PRIMARY KEY, val INT64)",
    )?;
    // Simulate an unvalidated writer that bypassed primary-key validation and left
    // two live rows sharing one key.
    inject_unvalidated_bulk_row(
        &engine,
        "main",
        "t",
        vec![
            ("id", DataType::Int64, int64_col(&[1])),
            ("val", DataType::Int64, int64_col(&[10])),
        ],
    )?;
    inject_unvalidated_bulk_row(
        &engine,
        "main",
        "t",
        vec![
            ("id", DataType::Int64, int64_col(&[1])),
            ("val", DataType::Int64, int64_col(&[20])),
        ],
    )?;

    // REPLACE must never fail on pre-existing duplicates; it replaces every
    // conflicting row.
    engine.execute(&mut session, "REPLACE INTO t VALUES (1, 30)")?;
    let count = first_i64(
        engine.execute(&mut session, "SELECT COUNT(*) FROM t")?,
        0,
        0,
    );
    assert_eq!(count, 1);
    let val = first_i64(
        engine.execute(&mut session, "SELECT val FROM t WHERE id = 1")?,
        0,
        0,
    );
    assert_eq!(val, 30);

    // Same-batch duplicates keep only the last row, across a flush boundary.
    engine.execute(&mut session, "FLUSH TABLE t")?;
    engine.execute(&mut session, "REPLACE INTO t VALUES (1, 40), (1, 50)")?;
    let count = first_i64(
        engine.execute(&mut session, "SELECT COUNT(*) FROM t")?,
        0,
        0,
    );
    assert_eq!(count, 1);
    let val = first_i64(
        engine.execute(&mut session, "SELECT val FROM t WHERE id = 1")?,
        0,
        0,
    );
    assert_eq!(val, 50);
    Ok(())
}

#[test]
fn insert_rejects_duplicate_primary_key_after_unvalidated_bulk_write() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();

    engine.execute(
        &mut session,
        "CREATE TABLE t (id INT64 PRIMARY KEY, val INT64)",
    )?;
    inject_unvalidated_bulk_row(
        &engine,
        "main",
        "t",
        vec![
            ("id", DataType::Int64, int64_col(&[1])),
            ("val", DataType::Int64, int64_col(&[10])),
        ],
    )?;

    let error = engine
        .execute(&mut session, "INSERT INTO t VALUES (1, 99)")
        .unwrap_err();
    assert!(
        error.to_string().contains("duplicate"),
        "INSERT must reject a primary key written by an unvalidated bulk write: {error}"
    );
    Ok(())
}
#[test]
fn replace_replaces_all_preexisting_duplicate_unique_index_rows() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();

    engine.execute(
        &mut session,
        "CREATE TABLE t (id INT64, code INT64, UNIQUE KEY uk_code (code))",
    )?;
    inject_unvalidated_bulk_row(
        &engine,
        "main",
        "t",
        vec![
            ("id", DataType::Int64, int64_col(&[1])),
            ("code", DataType::Int64, int64_col(&[100])),
        ],
    )?;
    inject_unvalidated_bulk_row(
        &engine,
        "main",
        "t",
        vec![
            ("id", DataType::Int64, int64_col(&[2])),
            ("code", DataType::Int64, int64_col(&[100])),
        ],
    )?;

    engine.execute(&mut session, "REPLACE INTO t (id, code) VALUES (3, 100)")?;
    let count = first_i64(
        engine.execute(&mut session, "SELECT COUNT(*) FROM t")?,
        0,
        0,
    );
    assert_eq!(count, 1);
    let id = first_i64(
        engine.execute(&mut session, "SELECT id FROM t WHERE code = 100")?,
        0,
        0,
    );
    assert_eq!(id, 3);
    Ok(())
}

#[test]

fn bulk_import_refreshes_primary_key_snapshot_for_replace() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();

    engine.execute(
        &mut session,
        "CREATE TABLE t (id INT64 PRIMARY KEY, val INT64)",
    )?;
    engine.execute(&mut session, "REPLACE INTO t VALUES (1, 10)")?;

    let payload = bulk_payload(
        "t",
        1,
        vec![
            ("id", DataType::Int64, int64_col(&[2])),
            ("val", DataType::Int64, int64_col(&[20])),
        ],
    );
    engine.exec_bulk_insert_payload_with_options("main", &payload, false)?;

    engine.execute(&mut session, "REPLACE INTO t VALUES (2, 99)")?;
    let total = first_i64(
        engine.execute(&mut session, "SELECT COUNT(*) FROM t")?,
        0,
        0,
    );
    assert_eq!(total, 2);
    let with_key = first_i64(
        engine.execute(&mut session, "SELECT COUNT(*) FROM t WHERE id = 2")?,
        0,
        0,
    );
    assert_eq!(with_key, 1);
    let val = first_i64(
        engine.execute(&mut session, "SELECT val FROM t WHERE id = 2")?,
        0,
        0,
    );
    assert_eq!(val, 99);
    Ok(())
}
