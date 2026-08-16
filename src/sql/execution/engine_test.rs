use super::*;
use crate::net::session::Session;
use crate::protocol::types::DataType;
use crate::storage::segment_manager::SegmentManager;
use crate::types::scalar::ScalarValue;

fn session() -> Session {
    Session::new("root".to_string(), "main".to_string())
}

#[test]
fn auto_compaction_detects_low_rate_row_group_fragmentation() {
    let config = AutoCompactConfig::default();
    let fragmented = TablePhysicalHealth {
        live_rows: 40,
        deleted_rows: 0,
        row_groups: 40,
        small_row_groups: 40,
    };
    assert!(fragmented.needs_fragmentation_cleanup(8192, &config));

    let compact = TablePhysicalHealth {
        live_rows: 40,
        deleted_rows: 0,
        row_groups: 1,
        small_row_groups: 1,
    };
    assert!(!compact.needs_fragmentation_cleanup(8192, &config));

    let legitimately_large = TablePhysicalHealth {
        live_rows: 8192 * 40,
        deleted_rows: 0,
        row_groups: 40,
        small_row_groups: 0,
    };
    assert!(!legitimately_large.needs_fragmentation_cleanup(8192, &config));
}

fn bulk_payload(table: &str, row_count: u32, columns: Vec<(&str, DataType, Vec<u8>)>) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&(table.len() as u16).to_le_bytes());
    payload.extend_from_slice(table.as_bytes());
    payload.extend_from_slice(&row_count.to_le_bytes());
    payload.extend_from_slice(&(columns.len() as u32).to_le_bytes());
    for (name, dtype, data) in columns {
        payload.extend_from_slice(&(name.len() as u16).to_le_bytes());
        payload.extend_from_slice(name.as_bytes());
        payload.push(dtype as u8);
        payload.push(0);
        payload.push(0);
        payload.extend_from_slice(&row_count.to_le_bytes());
        payload.extend_from_slice(&0u32.to_le_bytes());
        payload.extend_from_slice(&data);
    }
    payload
}

fn int64_col(values: &[i64]) -> Vec<u8> {
    let mut data = Vec::new();
    for value in values {
        data.extend_from_slice(&value.to_le_bytes());
    }
    data
}

fn first_text(result: ExecResult, row: usize, col: usize) -> String {
    let ExecResult::Query { batches, .. } = result else {
        panic!("expected query result");
    };
    batches[0]
        .scalar(row, col)
        .and_then(|v| v.to_text())
        .unwrap()
}

fn first_i64(result: ExecResult, row: usize, col: usize) -> i64 {
    let ExecResult::Query { batches, .. } = result else {
        panic!("expected query result");
    };
    match batches[0].scalar(row, col).unwrap() {
        ScalarValue::Int64(Some(v)) => v,
        ScalarValue::Int32(Some(v)) => v as i64,
        value => panic!("unexpected value {:?}", value),
    }
}

#[test]
fn graceful_shutdown_keeps_tailed_memtable_resident() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = Engine::new(dir.path(), "main")?;
        let mut session = session();
        engine.execute(&mut session, "CREATE TABLE low_rate (id INT64)")?;
        engine.execute(&mut session, "INSERT INTO low_rate VALUES (9)")?;
        engine.flush_shutdown_buffers()?;
    }

    // The tailed memtable is restart-safe, so shutdown leaves it resident
    // instead of cutting a tiny row group; restart recovers it from the tail.
    let table_dir = dir.path().join("main").join("low_rate");
    if table_dir.exists() {
        assert!(SegmentManager::discover_segments(&table_dir)?.is_empty());
    }
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();
    assert_eq!(
        first_i64(
            engine.execute(&mut session, "SELECT COUNT(*) FROM low_rate")?,
            0,
            0
        ),
        1
    );
    Ok(())
}

#[test]
fn internal_data_directories_are_not_registered_as_databases() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join(".memtable_flush_tmp").join("orphan"))?;
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();

    let ExecResult::Query { batches, .. } = engine.execute(&mut session, "SHOW DATABASES")? else {
        panic!("expected query result");
    };
    let databases = (0..batches[0].num_rows())
        .filter_map(|row| batches[0].scalar(row, 0).and_then(|value| value.to_text()))
        .collect::<Vec<_>>();

    assert!(databases.iter().any(|database| database == "main"));
    assert!(!databases.iter().any(|database| database.starts_with('.')));
    assert!(!engine
        .catalogs()
        .read()
        .unwrap()
        .contains_key(".memtable_flush_tmp"));
    Ok(())
}

#[test]
fn fast_insert_sql_materializes_generic_multi_values() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();

    engine.execute(
        &mut session,
        "CREATE TABLE t (id INT64, name VARCHAR, amount DECIMAL(9,2), active BOOL)",
    )?;
    engine.execute(
            &mut session,
            "INSERT INTO t (id, name, amount, active) VALUES (1, 'alpha', 12.34, true), (2, 'it''s', -5.60, 0)",
        )?;

    assert_eq!(
        first_i64(
            engine.execute(&mut session, "SELECT id FROM t ORDER BY id")?,
            0,
            0
        ),
        1
    );
    assert_eq!(
        first_text(
            engine.execute(&mut session, "SELECT name FROM t WHERE id = 2")?,
            0,
            0,
        ),
        "it's"
    );
    assert_eq!(
        first_i64(
            engine.execute(&mut session, "SELECT COUNT(*) FROM t")?,
            0,
            0
        ),
        2
    );
    Ok(())
}

#[test]
fn fast_insert_sql_materializes_missing_nullable_and_literal_default_columns() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();

    engine.execute(
        &mut session,
        "CREATE TABLE t (id INT64, name VARCHAR, tag VARCHAR DEFAULT 'new', note VARCHAR)",
    )?;
    engine.execute(
        &mut session,
        "INSERT INTO t (id, name) VALUES (1, 'alpha'), (2, 'beta')",
    )?;

    assert_eq!(
        first_text(
            engine.execute(&mut session, "SELECT tag FROM t WHERE id = 1")?,
            0,
            0,
        ),
        "new"
    );
    assert_eq!(
        first_i64(
            engine.execute(&mut session, "SELECT COUNT(*) FROM t WHERE note IS NULL")?,
            0,
            0
        ),
        2
    );
    Ok(())
}

#[test]
fn fast_insert_sql_materializes_missing_auto_increment_and_current_timestamp() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();

    engine.execute(
            &mut session,
            "CREATE TABLE t (id INT64 AUTO_INCREMENT PRIMARY KEY, name VARCHAR, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
        )?;
    engine.execute(
        &mut session,
        "INSERT INTO t (name) VALUES ('alpha'), ('beta')",
    )?;

    assert_eq!(
        first_i64(
            engine.execute(&mut session, "SELECT id FROM t ORDER BY id")?,
            0,
            0
        ),
        1
    );
    assert_eq!(
        first_i64(
            engine.execute(&mut session, "SELECT id FROM t ORDER BY id")?,
            1,
            0
        ),
        2
    );
    assert_eq!(
        first_i64(
            engine.execute(
                &mut session,
                "SELECT COUNT(*) FROM t WHERE created_at IS NOT NULL"
            )?,
            0,
            0
        ),
        2
    );
    Ok(())
}

#[test]
fn rebuild_unique_index_persists_snapshot_and_show_reports_it() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();

    engine.execute(
        &mut session,
        "CREATE TABLE t (id INT64, name VARCHAR, UNIQUE KEY uk_id (id))",
    )?;
    engine.execute(
        &mut session,
        "INSERT INTO t (id, name) VALUES (1, 'a'), (2, 'b')",
    )?;
    engine.execute(&mut session, "REBUILD UNIQUE INDEX uk_id ON t")?;

    let index_file = dir
        .path()
        .join("main")
        .join("t")
        .join("indexes")
        .join("unique")
        .join("uk_id.keys.tbi");
    assert!(index_file.exists());

    let message = first_text(
        engine.execute(&mut session, "SHOW UNIQUE INDEX STATUS FROM t")?,
        0,
        4,
    );
    assert_eq!(message, "persisted keys=2");
    Ok(())
}

#[test]
fn drop_unique_index_removes_persisted_snapshot() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();

    engine.execute(
        &mut session,
        "CREATE TABLE t (id INT64, name VARCHAR, UNIQUE KEY uk_id (id))",
    )?;
    engine.execute(&mut session, "INSERT INTO t (id, name) VALUES (1, 'a')")?;
    engine.execute(&mut session, "REBUILD UNIQUE INDEX uk_id ON t")?;
    engine.execute(&mut session, "INSERT INTO t (id, name) VALUES (2, 'b')")?;
    let index_dir = dir
        .path()
        .join("main")
        .join("t")
        .join("indexes")
        .join("unique");
    let index_file = index_dir.join("uk_id.keys.tbi");
    let delta_file = index_dir.join("uk_id.delta.tbi");
    assert!(index_file.exists());
    assert!(delta_file.exists());

    engine.execute(&mut session, "DROP INDEX uk_id ON t")?;
    assert!(!index_file.exists());
    assert!(!delta_file.exists());
    Ok(())
}

#[test]
fn rebuild_unique_index_supports_multi_column_point_checks() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();

    engine.execute(
        &mut session,
        "CREATE TABLE t (a INT64, b INT64, name VARCHAR, UNIQUE KEY uk_ab (a, b))",
    )?;
    engine.execute(
        &mut session,
        "INSERT INTO t (a, b, name) VALUES (1, 2, 'x'), (1, 3, 'y')",
    )?;
    engine.execute(&mut session, "REBUILD UNIQUE INDEX uk_ab ON t")?;

    let index_file = dir
        .path()
        .join("main")
        .join("t")
        .join("indexes")
        .join("unique")
        .join("uk_ab.keys.tbi");
    assert!(index_file.exists());

    let duplicate = engine.execute(
        &mut session,
        "INSERT INTO t (a, b, name) VALUES (1, 2, 'dup')",
    );
    assert!(duplicate.is_err());

    engine.execute(
        &mut session,
        "INSERT INTO t (a, b, name) VALUES (2, 2, 'ok')",
    )?;
    Ok(())
}

#[test]
fn rebuild_unique_index_writes_v3_blocks_for_large_keyset() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();

    engine.execute(
        &mut session,
        "CREATE TABLE t (id INT64, name VARCHAR, UNIQUE KEY uk_id (id))",
    )?;
    let values = (0..300)
        .map(|id| format!("({}, 'n{}')", id, id))
        .collect::<Vec<_>>()
        .join(", ");
    engine.execute(
        &mut session,
        &format!("INSERT INTO t (id, name) VALUES {}", values),
    )?;
    engine.execute(&mut session, "REBUILD UNIQUE INDEX uk_id ON t")?;

    let index_file = dir
        .path()
        .join("main")
        .join("t")
        .join("indexes")
        .join("unique")
        .join("uk_id.keys.tbi");
    let bytes = std::fs::read(index_file)?;
    assert_eq!(&bytes[..8], b"TBIUKV5\0");

    let duplicate = engine.execute(&mut session, "INSERT INTO t (id, name) VALUES (299, 'dup')");
    assert!(duplicate.is_err());
    engine.execute(&mut session, "INSERT INTO t (id, name) VALUES (300, 'ok')")?;
    Ok(())
}

#[test]
fn unique_index_incremental_insert_uses_delta_and_survives_reload() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut initial_session = session();

    engine.execute(
        &mut initial_session,
        "CREATE TABLE t (id INT64, name VARCHAR, UNIQUE KEY uk_id (id))",
    )?;
    engine.execute(
        &mut initial_session,
        "INSERT INTO t (id, name) VALUES (1, 'a')",
    )?;
    engine.execute(&mut initial_session, "REBUILD UNIQUE INDEX uk_id ON t")?;

    let index_dir = dir
        .path()
        .join("main")
        .join("t")
        .join("indexes")
        .join("unique");
    let base_file = index_dir.join("uk_id.keys.tbi");
    let delta_file = index_dir.join("uk_id.delta.tbi");
    let base_before = std::fs::metadata(&base_file)?.len();
    assert!(!delta_file.exists());

    engine.execute(
        &mut initial_session,
        "INSERT INTO t (id, name) VALUES (2, 'b')",
    )?;
    let base_after = std::fs::metadata(&base_file)?.len();
    assert_eq!(base_after, base_before);
    assert!(delta_file.exists());
    assert!(std::fs::metadata(&delta_file)?.len() > 8);

    let duplicate_from_cache = engine.execute(
        &mut initial_session,
        "INSERT INTO t (id, name) VALUES (2, 'dup')",
    );
    assert!(duplicate_from_cache.is_err());

    drop(engine);
    let engine = Engine::new(dir.path(), "main")?;
    let mut reloaded_session = session();
    let duplicate_from_delta = engine.execute(
        &mut reloaded_session,
        "INSERT INTO t (id, name) VALUES (2, 'dup')",
    );
    assert!(duplicate_from_delta.is_err());
    engine.execute(
        &mut reloaded_session,
        "INSERT INTO t (id, name) VALUES (3, 'c')",
    )?;
    Ok(())
}

#[test]
fn unique_index_delta_compacts_when_threshold_is_reached() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut initial_session = session();

    engine.execute(
        &mut initial_session,
        "CREATE TABLE t (id INT64, name VARCHAR, UNIQUE KEY uk_id (id))",
    )?;
    engine.execute(
        &mut initial_session,
        "INSERT INTO t (id, name) VALUES (1, 'a')",
    )?;
    engine.execute(&mut initial_session, "REBUILD UNIQUE INDEX uk_id ON t")?;

    let index_dir = dir
        .path()
        .join("main")
        .join("t")
        .join("indexes")
        .join("unique");
    let base_file = index_dir.join("uk_id.keys.tbi");
    let delta_file = index_dir.join("uk_id.delta.tbi");
    let base_before = std::fs::metadata(&base_file)?.len();
    let values = (2..180)
        .map(|id| format!("({}, 'n{}')", id, id))
        .collect::<Vec<_>>()
        .join(", ");

    engine.execute(
        &mut initial_session,
        &format!("INSERT INTO t (id, name) VALUES {}", values),
    )?;

    let base_after = std::fs::metadata(&base_file)?.len();
    assert!(base_after > base_before);
    assert!(!delta_file.exists());
    let duplicate_from_cache = engine.execute(
        &mut initial_session,
        "INSERT INTO t (id, name) VALUES (179, 'dup')",
    );
    assert!(duplicate_from_cache.is_err());

    drop(engine);
    let engine = Engine::new(dir.path(), "main")?;
    let mut reloaded_session = session();
    let duplicate_after_reload = engine.execute(
        &mut reloaded_session,
        "INSERT INTO t (id, name) VALUES (179, 'dup')",
    );
    assert!(duplicate_after_reload.is_err());
    Ok(())
}

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
    assert!(invalid_file.exists());
    assert_eq!(
        engine.index_manager().unique_key_exists(
            "main",
            "t",
            "uk_id",
            &[ScalarValue::Int64(Some(1))]
        )?,
        None
    );
    engine.execute(&mut session, "INSERT INTO t (id, name) VALUES (1, 'b')")?;

    let name = first_text(
        engine.execute(&mut session, "SELECT name FROM t WHERE id = 1")?,
        0,
        0,
    );
    assert_eq!(name, "b");
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
    assert!(engine
        .index_manager()
        .load_unique_index_snapshot("main", "t", index_name)?
        .is_none());

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
    assert!(engine
        .index_manager()
        .unique_key_locations(
            "main",
            "t",
            index_name,
            &[vec![ScalarValue::Int64(Some(1))]],
        )?
        .is_none());
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
fn update_unique_key_after_rebuild_allows_old_key_reuse() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();

    engine.execute(
        &mut session,
        "CREATE TABLE t (id INT64, code INT64, UNIQUE KEY uk_code (code))",
    )?;
    engine.execute(&mut session, "INSERT INTO t (id, code) VALUES (1, 10)")?;
    engine.execute(&mut session, "REBUILD UNIQUE INDEX uk_code ON t")?;
    engine.execute(&mut session, "UPDATE t SET code = 20 WHERE id = 1")?;
    engine.execute(&mut session, "INSERT INTO t (id, code) VALUES (2, 10)")?;

    let count = first_i64(
        engine.execute(&mut session, "SELECT COUNT(*) FROM t")?,
        0,
        0,
    );
    assert_eq!(count, 2);
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
fn delete_removes_memtable_rows_without_flushing() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    {
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

        engine.execute(&mut session, "DELETE FROM t WHERE id = 1")?;
        let count = first_i64(
            engine.execute(&mut session, "SELECT COUNT(*) FROM t")?,
            0,
            0,
        );
        assert_eq!(count, 2);
        // No forced flush: the rows were removed from the memtable in place.
        assert_eq!(
            engine.segment_mgr().memtable_flush_epoch("main", "t"),
            epoch_before
        );
    }

    // The filtered durable tail survives a restart; the deleted row must not
    // resurrect, and its key must be insertable again.
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();
    let count = first_i64(
        engine.execute(&mut session, "SELECT COUNT(*) FROM t")?,
        0,
        0,
    );
    assert_eq!(count, 2);
    engine.execute(&mut session, "INSERT INTO t VALUES (1, 'uno')")?;
    let name = first_text(
        engine.execute(&mut session, "SELECT name FROM t WHERE id = 1")?,
        0,
        0,
    );
    assert_eq!(name, "uno");
    Ok(())
}

#[test]
fn bulk_payload_incrementally_maintains_rebuilt_unique_index() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();

    engine.execute(
        &mut session,
        "CREATE TABLE t (id INT64, name INT64, UNIQUE KEY uk_id (id))",
    )?;
    engine.execute(&mut session, "INSERT INTO t (id, name) VALUES (1, 1)")?;
    engine.execute(&mut session, "REBUILD UNIQUE INDEX uk_id ON t")?;
    let payload = bulk_payload(
        "t",
        1,
        vec![
            ("id", DataType::Int64, int64_col(&[2])),
            ("name", DataType::Int64, int64_col(&[2])),
        ],
    );
    engine.exec_bulk_insert_payload_with_options("main", &payload, false)?;

    let duplicate = engine.execute(&mut session, "INSERT INTO t (id, name) VALUES (2, 3)");
    assert!(duplicate.is_err());
    Ok(())
}

#[test]
fn bulk_payload_refresh_appends_delta_without_rewriting_base_snapshot() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();
    engine.execute(
        &mut session,
        "CREATE TABLE t (id INT64 PRIMARY KEY, code INT64, UNIQUE KEY uk_code (code))",
    )?;
    let index_dir = dir
        .path()
        .join("main")
        .join("t")
        .join("indexes")
        .join("unique");
    let mut next_id = 0i64;
    for _ in 0..2 {
        let ids = vec![next_id, next_id + 1];
        let codes = vec![next_id * 10, next_id * 10 + 1];
        let payload = bulk_payload(
            "t",
            2,
            vec![
                ("id", DataType::Int64, int64_col(&ids)),
                ("code", DataType::Int64, int64_col(&codes)),
            ],
        );
        engine.exec_bulk_insert_payload_with_options("main", &payload, false)?;
        next_id += 2;
    }
    engine.execute(&mut session, "REBUILD UNIQUE INDEX uk_code ON t")?;
    let base_path = index_dir.join("uk_code.keys.tbi");
    let base_before = std::fs::read(&base_path)?;
    engine.begin_bulk_import();
    session.bulk_import_mode = true;
    for _ in 0..4 {
        let ids = vec![next_id, next_id + 1];
        let codes = vec![next_id * 10, next_id * 10 + 1];
        let payload = bulk_payload(
            "t",
            2,
            vec![
                ("id", DataType::Int64, int64_col(&ids)),
                ("code", DataType::Int64, int64_col(&codes)),
            ],
        );
        engine.exec_bulk_insert_payload_with_options("main", &payload, true)?;
        next_id += 2;
    }
    session.bulk_import_mode = false;
    engine.end_bulk_import();
    assert_eq!(std::fs::read(&base_path)?, base_before);
    assert!(index_dir.join("uk_code.delta.tbi").exists());
    assert!(index_dir.join("uk_code.progress.tbi").exists());
    let duplicate = engine.execute(
        &mut session,
        &format!(
            "INSERT INTO t (id, code) VALUES ({}, {})",
            next_id,
            (next_id - 2) * 10
        ),
    );
    assert!(duplicate.is_err());
    Ok(())
}

#[test]
fn bulk_payload_delta_compaction_is_amortized_against_base_size() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();
    engine.execute(
        &mut session,
        "CREATE TABLE t (id INT64, code INT64, UNIQUE KEY uk_id (id))",
    )?;
    let index_dir = dir
        .path()
        .join("main")
        .join("t")
        .join("indexes")
        .join("unique");
    let base_file = index_dir.join("uk_id.keys.tbi");
    let mut next_id = 0i64;
    for _ in 0..8 {
        let ids: Vec<i64> = (next_id..next_id + 250).collect();
        let codes: Vec<i64> = ids.iter().map(|v| v * 3).collect();
        let payload = bulk_payload(
            "t",
            250,
            vec![
                ("id", DataType::Int64, int64_col(&ids)),
                ("code", DataType::Int64, int64_col(&codes)),
            ],
        );
        engine.exec_bulk_insert_payload_with_options("main", &payload, false)?;
        next_id += 250;
    }
    engine.execute(&mut session, "REBUILD UNIQUE INDEX uk_id ON t")?;
    let base_seed = std::fs::metadata(&base_file)?.len();
    assert!(
        base_seed > 2 * 4096,
        "seed base must dominate the absolute floor"
    );

    engine.begin_bulk_import();
    session.bulk_import_mode = true;
    let mut base_lengths = Vec::new();
    for _ in 0..40 {
        let ids: Vec<i64> = (next_id..next_id + 50).collect();
        let codes: Vec<i64> = ids.iter().map(|v| v * 3).collect();
        let payload = bulk_payload(
            "t",
            50,
            vec![
                ("id", DataType::Int64, int64_col(&ids)),
                ("code", DataType::Int64, int64_col(&codes)),
            ],
        );
        engine.exec_bulk_insert_payload_with_options("main", &payload, true)?;
        next_id += 50;
        base_lengths.push(std::fs::metadata(&base_file)?.len());
    }
    session.bulk_import_mode = false;
    assert_eq!(engine.end_bulk_import(), 0);

    assert!(base_lengths.iter().all(|len| *len == base_seed));
    assert_eq!(std::fs::metadata(&base_file)?.len(), base_seed);
    engine.flush_bulk_dirty_tables()?;
    assert!(std::fs::metadata(&base_file)?.len() > base_seed);
    let duplicate = engine.execute(
        &mut session,
        &format!("INSERT INTO t (id, code) VALUES ({}, 0)", next_id - 1),
    );
    assert!(duplicate.is_err());
    Ok(())
}

#[test]
#[ignore = "diagnostic: run manually with --ignored --nocapture"]
fn bulk_stage_timing_drift_diagnostic() -> Result<()> {
    crate::common::config::set_insert_profile_enabled(true);
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();
    engine.execute(
        &mut session,
        "CREATE TABLE t (id INT64 PRIMARY KEY, v INT64)",
    )?;
    let batches = 12;
    let rows_per_batch = 400_000i64;
    engine.begin_bulk_import();
    session.bulk_import_mode = true;
    let mut next_id = 0i64;
    for batch in 0..batches {
        let ids: Vec<i64> = (next_id..next_id + rows_per_batch).collect();
        let vs: Vec<i64> = ids.iter().map(|v| v * 3).collect();
        let payload = bulk_payload(
            "t",
            rows_per_batch as u32,
            vec![
                ("id", DataType::Int64, int64_col(&ids)),
                ("v", DataType::Int64, int64_col(&vs)),
            ],
        );
        let start = std::time::Instant::now();
        engine.exec_bulk_insert_payload_with_options("main", &payload, true)?;
        println!(
            "[drift] batch={} total_ms={}",
            batch,
            start.elapsed().as_millis()
        );
        next_id += rows_per_batch;
    }
    session.bulk_import_mode = false;
    engine.end_bulk_import();
    Ok(())
}

#[test]
fn bulk_payload_unique_indexes_survive_restart_and_multiple_batches() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = Engine::new(dir.path(), "main")?;
        let mut session = session();
        engine.execute(
            &mut session,
            "CREATE TABLE t (id INT64 PRIMARY KEY, code INT64, UNIQUE KEY uk_code (code))",
        )?;
        for value in 1..=4i64 {
            let payload = bulk_payload(
                "t",
                1,
                vec![
                    ("id", DataType::Int64, int64_col(&[value])),
                    ("code", DataType::Int64, int64_col(&[value * 10])),
                ],
            );
            assert_eq!(
                engine.exec_bulk_insert_payload_with_options("main", &payload, false)?,
                1
            );
        }
    }

    let engine = Engine::new(dir.path(), "main")?;
    let primary_duplicate = bulk_payload(
        "t",
        1,
        vec![
            ("id", DataType::Int64, int64_col(&[2])),
            ("code", DataType::Int64, int64_col(&[90])),
        ],
    );
    assert!(engine
        .exec_bulk_insert_payload_with_options("main", &primary_duplicate, false)
        .is_err());
    let unique_duplicate = bulk_payload(
        "t",
        1,
        vec![
            ("id", DataType::Int64, int64_col(&[9])),
            ("code", DataType::Int64, int64_col(&[30])),
        ],
    );
    assert!(engine
        .exec_bulk_insert_payload_with_options("main", &unique_duplicate, false)
        .is_err());
    let new_row = bulk_payload(
        "t",
        1,
        vec![
            ("id", DataType::Int64, int64_col(&[9])),
            ("code", DataType::Int64, int64_col(&[90])),
        ],
    );
    assert_eq!(
        engine.exec_bulk_insert_payload_with_options("main", &new_row, false)?,
        1
    );
    Ok(())
}

#[test]
fn concurrent_bulk_payload_workers_complete_without_catalog_lock_upgrade() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(Engine::new(dir.path(), "main")?);
    let mut session = session();
    engine.execute(&mut session, "CREATE TABLE t (id INT64)")?;

    let (tx, rx) = std::sync::mpsc::channel();
    let barrier = Arc::new(std::sync::Barrier::new(8));
    let mut workers = Vec::new();
    for worker_id in 0..8 {
        let engine = Arc::clone(&engine);
        let barrier = Arc::clone(&barrier);
        let tx = tx.clone();
        workers.push(std::thread::spawn(move || {
            let payload = bulk_payload(
                "t",
                1,
                vec![("id", DataType::Int64, int64_col(&[worker_id]))],
            );
            barrier.wait();
            let result = engine.exec_bulk_insert_payload_with_options("main", &payload, false);
            tx.send(result).unwrap();
        }));
    }
    drop(tx);

    for _ in 0..8 {
        assert_eq!(
            rx.recv_timeout(std::time::Duration::from_secs(10))
                .expect("bulk worker timed out")?,
            1
        );
    }
    for worker in workers {
        worker.join().unwrap();
    }

    assert_eq!(engine.get_table_meta("main", "t").unwrap().row_count, 8);
    Ok(())
}

#[test]
fn mixed_read_write_flush_workers_keep_making_progress() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(Engine::new(dir.path(), "main")?);
    let mut setup = session();
    engine.execute(&mut setup, "CREATE TABLE t (id INT64)")?;

    let barrier = Arc::new(std::sync::Barrier::new(8));
    let (tx, rx) = std::sync::mpsc::channel();
    for worker_id in 0..8i64 {
        let engine = Arc::clone(&engine);
        let barrier = Arc::clone(&barrier);
        let tx = tx.clone();
        std::thread::spawn(move || {
            barrier.wait();
            let result = (|| -> Result<()> {
                if worker_id < 4 {
                    for iteration in 0..20i64 {
                        let value = worker_id * 1000 + iteration;
                        let payload = bulk_payload(
                            "t",
                            1,
                            vec![("id", DataType::Int64, int64_col(&[value]))],
                        );
                        engine.exec_bulk_insert_payload_with_options("main", &payload, false)?;
                    }
                } else if worker_id < 7 {
                    let mut worker_session = session();
                    for _ in 0..20 {
                        engine.execute(&mut worker_session, "SELECT COUNT(*) FROM t")?;
                        engine.get_table_meta("main", "t").unwrap();
                    }
                } else {
                    for _ in 0..10 {
                        engine.flush_table_buffers("main", "t")?;
                    }
                }
                Ok(())
            })();
            tx.send(result).unwrap();
        });
    }
    drop(tx);

    for _ in 0..8 {
        rx.recv_timeout(std::time::Duration::from_secs(30))
            .expect("mixed workload worker timed out")?;
    }
    assert_eq!(engine.get_table_meta("main", "t").unwrap().row_count, 80);
    Ok(())
}

#[test]
fn flush_table_only_flushes_named_table() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();

    engine.execute(&mut session, "CREATE TABLE t1 (id INT64)")?;
    engine.execute(&mut session, "CREATE TABLE t2 (id INT64)")?;
    engine.begin_bulk_import();
    session.bulk_import_mode = true;
    engine.execute(&mut session, "INSERT INTO t1 (id) VALUES (1)")?;
    engine.execute(&mut session, "INSERT INTO t2 (id) VALUES (2)")?;

    let t2_dir = dir.path().join("main").join("t2");
    let t2_before = SegmentManager::discover_segments(&t2_dir)?
        .into_iter()
        .map(|path| std::fs::metadata(path).map(|meta| meta.len()))
        .collect::<std::io::Result<Vec<_>>>()?;

    engine.execute(&mut session, "FLUSH TABLE t1")?;

    let t1_dir = dir.path().join("main").join("t1");
    let t1_segments = SegmentManager::discover_segments(&t1_dir)?;
    let t2_after = SegmentManager::discover_segments(&t2_dir)?
        .into_iter()
        .map(|path| std::fs::metadata(path).map(|meta| meta.len()))
        .collect::<std::io::Result<Vec<_>>>()?;
    assert!(!t1_segments.is_empty());
    assert_eq!(t2_after, t2_before);

    session.bulk_import_mode = false;
    engine.end_bulk_import();
    Ok(())
}

#[test]
fn optimize_partition_leaves_sibling_partition_and_other_tables_resident() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();

    engine.execute(
        &mut session,
        "CREATE TABLE partitioned (id INT64, value INT64) PARTITION BY id",
    )?;
    engine.execute(&mut session, "CREATE TABLE low_rate (id INT64)")?;
    engine.execute(&mut session, "INSERT INTO low_rate VALUES (9)")?;

    engine.begin_bulk_import();
    session.bulk_import_mode = true;
    engine.execute(&mut session, "INSERT INTO partitioned VALUES (1, 10)")?;
    engine.execute(&mut session, "INSERT INTO partitioned VALUES (2, 20)")?;

    let table_dir = dir.path().join("main").join("partitioned");
    let sibling_path = SegmentManager::discover_segments(&table_dir)?
        .into_iter()
        .find(|path| {
            path.file_name()
                .is_some_and(|name| name.to_string_lossy().starts_with("p2_"))
        })
        .expect("sibling partition segment");
    let sibling_len_before = std::fs::metadata(&sibling_path)?.len();

    engine.execute(&mut session, "OPTIMIZE TABLE partitioned ON PARTITION p1")?;

    assert_eq!(
        engine
            .segment_mgr()
            .active_partition_keys("main", "partitioned"),
        vec!["2".to_string()]
    );
    assert_eq!(std::fs::metadata(&sibling_path)?.len(), sibling_len_before);
    assert!(
        SegmentManager::discover_segments(&dir.path().join("main").join("low_rate"))?.is_empty()
    );

    session.bulk_import_mode = false;
    engine.end_bulk_import();
    Ok(())
}

#[test]
fn bulk_multi_value_insert_routes_every_value_partition() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();
    engine.execute(
        &mut session,
        "CREATE TABLE partitioned (id INT64, value INT64) PARTITION BY id",
    )?;
    engine.begin_bulk_import();
    session.bulk_import_mode = true;
    engine.execute(
        &mut session,
        "INSERT INTO partitioned VALUES (1, 10), (2, 20)",
    )?;

    let mut active = engine
        .segment_mgr()
        .active_partition_keys("main", "partitioned");
    active.sort();
    assert_eq!(active, vec!["1".to_string(), "2".to_string()]);

    session.bulk_import_mode = false;
    engine.end_bulk_import();
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
fn committed_transaction_remains_visible_without_immediate_segment_flush() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut writer = session();
    let mut reader = session();

    engine.execute(&mut writer, "CREATE TABLE t (id INT64, name VARCHAR)")?;
    engine.execute(&mut writer, "BEGIN")?;
    engine.execute(&mut writer, "INSERT INTO t (id, name) VALUES (1, 'a')")?;

    let reader_count_before =
        first_i64(engine.execute(&mut reader, "SELECT COUNT(*) FROM t")?, 0, 0);
    let writer_count_before =
        first_i64(engine.execute(&mut writer, "SELECT COUNT(*) FROM t")?, 0, 0);
    assert_eq!(reader_count_before, 0);
    assert_eq!(writer_count_before, 1);

    engine.execute(&mut writer, "COMMIT")?;

    let reader_count_after =
        first_i64(engine.execute(&mut reader, "SELECT COUNT(*) FROM t")?, 0, 0);
    assert_eq!(reader_count_after, 1);

    let table_dir = dir.path().join("main").join("t");
    let segments = SegmentManager::discover_segments(&table_dir)?;
    assert!(segments.is_empty());
    Ok(())
}

#[test]
fn close_of_day_only_optimizes_partitions_written_since_last_close() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();

    engine.execute(
        &mut session,
        "CREATE TABLE daily_partitioned (trade_date INT64, seq INT64) PARTITION BY trade_date",
    )?;
    engine.execute(
        &mut session,
        "INSERT INTO daily_partitioned (trade_date, seq) VALUES (1, 2), (1, 1)",
    )?;
    engine.close_of_day_flush_table("main", "daily_partitioned")?;

    let table_dir = dir.path().join("main").join("daily_partitioned");
    let first_partition_path = SegmentManager::discover_segments(&table_dir)?
        .into_iter()
        .find(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("p1_"))
        })
        .expect("first partition segment after initial close");
    let first_partition_bytes = std::fs::read(&first_partition_path)?;

    engine.execute(
        &mut session,
        "INSERT INTO daily_partitioned (trade_date, seq) VALUES (2, 2), (2, 1)",
    )?;
    engine.close_of_day_flush_table("main", "daily_partitioned")?;

    assert!(first_partition_path.exists());
    assert_eq!(std::fs::read(&first_partition_path)?, first_partition_bytes);
    let segments = SegmentManager::discover_segments(&table_dir)?;
    assert!(segments.iter().any(|path| {
        path.file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with("p2_"))
    }));
    Ok(())
}
#[test]
fn bulk_flush_merges_same_table_memtables_into_one_row_group() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();
    engine.execute(
        &mut session,
        "CREATE TABLE t (id INT64, v INT64, UNIQUE KEY uk_id (id))",
    )?;
    // Committed shared memtable #1.
    engine.execute(&mut session, "INSERT INTO t VALUES (1, 10), (2, 20)")?;
    // REPLACE stages its insert in an internal-txn memtable that commits as a
    // second committed memtable (same table, same partition, txn 0).
    engine.execute(&mut session, "REPLACE INTO t VALUES (3, 30)")?;

    engine.flush_all_buffers()?;

    let table_dir = dir.path().join("main").join("t");
    let segments = SegmentManager::discover_segments(&table_dir)?;
    assert_eq!(segments.len(), 1);
    let reader = crate::storage::segment::reader::SegmentReader::open(&segments[0])?;
    // Before group merging, each memtable flush cut its own tiny row group.
    assert_eq!(reader.num_row_groups(), 1);
    let count = first_i64(
        engine.execute(&mut session, "SELECT COUNT(*) FROM t")?,
        0,
        0,
    );
    assert_eq!(count, 3);

    // Restart: the merged segment plus cleared durable-tail intents must
    // recover cleanly (no double-replay, no loss).
    drop(engine);
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = Session::new("root".to_string(), "main".to_string());
    let count = first_i64(
        engine.execute(&mut session, "SELECT COUNT(*) FROM t")?,
        0,
        0,
    );
    assert_eq!(count, 3);
    Ok(())
}

#[test]
fn relaxed_durability_large_insert_bypasses_durable_tail() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = Session::new("root".to_string(), "main".to_string());

    engine.execute(
        &mut session,
        "CREATE TABLE relaxed_bulk (id INT64) DURABILITY = RELAXED",
    )?;

    let mut sql = String::from("INSERT INTO relaxed_bulk VALUES (0)");
    for value in 1..1024 {
        sql.push_str(&format!(", ({value})"));
    }
    engine.execute(&mut session, &sql)?;

    let tail_dir = dir
        .path()
        .join("main")
        .join("relaxed_bulk")
        .join("memtable_tail");
    let tail_files = std::fs::read_dir(&tail_dir)
        .map(|entries| entries.filter_map(|entry| entry.ok()).count())
        .unwrap_or(0);
    assert_eq!(
        tail_files, 0,
        "large relaxed INSERT must not create durable tail files"
    );

    engine.flush_table_buffers("main", "relaxed_bulk")?;
    let count = first_i64(
        engine.execute(&mut session, "SELECT COUNT(*) FROM relaxed_bulk")?,
        0,
        0,
    );
    assert_eq!(count, 1024);
    engine.segment_mgr().shutdown();
    Ok(())
}

#[test]
fn relaxed_durability_small_insert_keeps_strict_pipeline() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = Session::new("root".to_string(), "main".to_string());

    engine.execute(
        &mut session,
        "CREATE TABLE relaxed_small (id INT64) DURABILITY = RELAXED",
    )?;
    engine.execute(&mut session, "INSERT INTO relaxed_small VALUES (1), (2)")?;

    let tail_dir = dir
        .path()
        .join("main")
        .join("relaxed_small")
        .join("memtable_tail");
    let tail_files = std::fs::read_dir(&tail_dir)
        .map(|entries| entries.filter_map(|entry| entry.ok()).count())
        .unwrap_or(0);
    assert!(
        tail_files > 0,
        "small INSERT on a relaxed table must keep the durable tail pipeline"
    );

    engine.flush_table_buffers("main", "relaxed_small")?;
    let count = first_i64(
        engine.execute(&mut session, "SELECT COUNT(*) FROM relaxed_small")?,
        0,
        0,
    );
    assert_eq!(count, 2);
    engine.segment_mgr().shutdown();
    Ok(())
}

fn inject_unvalidated_bulk_row(
    engine: &Engine,
    db: &str,
    table: &str,
    columns: Vec<(&str, DataType, Vec<u8>)>,
) -> Result<()> {
    let payload = bulk_payload(table, 1, columns);
    let catalog = {
        let catalogs = engine.catalogs().read().unwrap();
        catalogs.get(db).cloned().expect("database exists")
    };
    engine
        .segment_mgr()
        .bulk_insert_payload_with_catalog(db, &payload, &catalog, true)?;
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
fn bulk_payload_rejects_duplicate_primary_keys() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();

    engine.execute(
        &mut session,
        "CREATE TABLE t (id INT64 PRIMARY KEY, val INT64)",
    )?;

    let dup_in_batch = bulk_payload(
        "t",
        2,
        vec![
            ("id", DataType::Int64, int64_col(&[1, 1])),
            ("val", DataType::Int64, int64_col(&[10, 20])),
        ],
    );
    assert!(engine
        .exec_bulk_insert_payload_with_options("main", &dup_in_batch, false)
        .is_err());

    let ok = bulk_payload(
        "t",
        1,
        vec![
            ("id", DataType::Int64, int64_col(&[1])),
            ("val", DataType::Int64, int64_col(&[10])),
        ],
    );
    engine.exec_bulk_insert_payload_with_options("main", &ok, false)?;

    let dup_existing = bulk_payload(
        "t",
        1,
        vec![
            ("id", DataType::Int64, int64_col(&[1])),
            ("val", DataType::Int64, int64_col(&[30])),
        ],
    );
    assert!(engine
        .exec_bulk_insert_payload_with_options("main", &dup_existing, false)
        .is_err());

    let count = first_i64(
        engine.execute(&mut session, "SELECT COUNT(*) FROM t")?,
        0,
        0,
    );
    assert_eq!(count, 1);
    Ok(())
}

#[test]
fn concurrent_bulk_payload_rejects_duplicate_primary_key() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(Engine::new(dir.path(), "main")?);
    let mut session = session();
    engine.execute(
        &mut session,
        "CREATE TABLE t (id INT64 PRIMARY KEY, val INT64)",
    )?;

    let barrier = Arc::new(std::sync::Barrier::new(2));
    let mut workers = Vec::new();
    for value in [10, 20] {
        let engine = Arc::clone(&engine);
        let barrier = Arc::clone(&barrier);
        workers.push(std::thread::spawn(move || {
            let payload = bulk_payload(
                "t",
                1,
                vec![
                    ("id", DataType::Int64, int64_col(&[1])),
                    ("val", DataType::Int64, int64_col(&[value])),
                ],
            );
            barrier.wait();
            engine.exec_bulk_insert_payload_with_options("main", &payload, false)
        }));
    }

    let successes = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .filter(Result::is_ok)
        .count();
    assert_eq!(successes, 1);
    let count = first_i64(
        engine.execute(&mut session, "SELECT COUNT(*) FROM t WHERE id = 1")?,
        0,
        0,
    );
    assert_eq!(count, 1);
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
