use super::*;

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
fn delete_then_reinsert_reuses_compacted_memtable_placeholder_keys() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();

    engine.execute(
        &mut session,
        "CREATE TABLE t (id INT64, name VARCHAR, UNIQUE KEY uk_id (id))",
    )?;
    let mut values = String::new();
    for id in 1..=40i64 {
        if !values.is_empty() {
            values.push_str(", ");
        }
        values.push_str(&format!("({}, 'a')", id));
    }
    engine.execute(
        &mut session,
        &format!("INSERT INTO t (id, name) VALUES {}", values),
    )?;
    engine.execute(&mut session, "DELETE FROM t")?;
    engine.execute(
        &mut session,
        &format!("INSERT INTO t (id, name) VALUES {}", values),
    )?;
    let count = first_i64(
        engine.execute(&mut session, "SELECT COUNT(*) FROM t")?,
        0,
        0,
    );
    assert_eq!(count, 40);
    let duplicate = engine.execute(&mut session, "INSERT INTO t (id, name) VALUES (1, 'dup')");
    assert!(duplicate.is_err());
    Ok(())
}

#[test]
fn delete_then_reinsert_reuses_keys_after_flush_and_refresh() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();

    engine.execute(
        &mut session,
        "CREATE TABLE t (id INT64, name VARCHAR, UNIQUE KEY uk_id (id))",
    )?;
    let mut values = String::new();
    for id in 1..=40i64 {
        if !values.is_empty() {
            values.push_str(", ");
        }
        values.push_str(&format!("({}, 'a')", id));
    }
    engine.execute(
        &mut session,
        &format!("INSERT INTO t (id, name) VALUES {}", values),
    )?;
    engine.execute(&mut session, "FLUSH TABLE t")?;
    assert_eq!(
        engine.index_manager().unique_key_exists(
            "main",
            "t",
            "uk_id",
            &[ScalarValue::Int64(Some(1))]
        )?,
        Some(true)
    );
    engine.execute(&mut session, "DELETE FROM t")?;
    assert_eq!(
        engine.index_manager().unique_key_exists(
            "main",
            "t",
            "uk_id",
            &[ScalarValue::Int64(Some(1))]
        )?,
        Some(false)
    );
    engine.execute(
        &mut session,
        &format!("INSERT INTO t (id, name) VALUES {}", values),
    )?;
    let count = first_i64(
        engine.execute(&mut session, "SELECT COUNT(*) FROM t")?,
        0,
        0,
    );
    assert_eq!(count, 40);
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
fn rebuild_unique_index_writes_on_demand_blocks_for_large_keyset() -> Result<()> {
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
    assert_eq!(&bytes[..8], b"TBIUKV6\0");

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
fn update_rewrites_segments_and_auto_rebuilds_index() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();

    engine.execute(
        &mut session,
        "CREATE TABLE t (id INT64 PRIMARY KEY, code INT64, UNIQUE KEY uk_code (code))",
    )?;
    engine.execute(&mut session, "INSERT INTO t VALUES (1, 10), (2, 20)")?;
    engine.execute(&mut session, "FLUSH TABLE t")?;
    engine.execute(&mut session, "REBUILD UNIQUE INDEX uk_code ON t")?;

    let index_dir = dir
        .path()
        .join("main")
        .join("t")
        .join("indexes")
        .join("unique");
    let invalid_file = index_dir.join("uk_code.invalid");
    assert!(!invalid_file.exists());

    // UPDATE routes matched rows through tombstone + reinsert: the old key
    // is retired incrementally and the new key appended (no snapshot
    // invalidation), while the updated row itself is memtable-resident
    // until the next flush.
    engine.execute(&mut session, "UPDATE t SET code = 21 WHERE id = 1")?;
    assert!(!invalid_file.exists(), "maintenance stays eager, no invalidation");
    assert_eq!(
        engine
            .index_manager()
            .unique_key_exists("main", "t", "uk_code", &[ScalarValue::Int64(Some(21))])?,
        Some(true),
        "the new key is visible immediately"
    );
    assert_eq!(
        engine
            .index_manager()
            .unique_key_exists(
                "main",
                "t",
                "uk_code",
                &[ScalarValue::Int64(Some(10))],
            )?,
        Some(false),
        "the old key is retired incrementally"
    );

    engine.execute(&mut session, "REBUILD UNIQUE INDEX uk_code ON t")?;
    let locations = engine
        .index_manager()
        .unique_key_locations(
            "main",
            "t",
            "uk_code",
            &[vec![ScalarValue::Int64(Some(21))]],
        )?
        .unwrap();
    assert_eq!(locations.len(), 1);
    assert!(!locations[0].segment_path.is_empty());

    // The freed key is reusable and the new key stays unique.
    engine.execute(&mut session, "INSERT INTO t VALUES (3, 10)")?;
    let duplicate = engine.execute(&mut session, "INSERT INTO t VALUES (4, 21)");
    assert!(duplicate.is_err());
    let count = first_i64(
        engine.execute(&mut session, "SELECT COUNT(*) FROM t")?,
        0,
        0,
    );
    assert_eq!(count, 3);
    Ok(())
}

#[test]
fn optimize_table_auto_rebuilds_index() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();

    engine.execute(
        &mut session,
        "CREATE TABLE t (id INT64, name VARCHAR, UNIQUE KEY uk_id (id))",
    )?;
    engine.execute(
        &mut session,
        "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')",
    )?;
    engine.execute(&mut session, "FLUSH TABLE t")?;
    engine.execute(&mut session, "REBUILD UNIQUE INDEX uk_id ON t")?;

    let index_dir = dir
        .path()
        .join("main")
        .join("t")
        .join("indexes")
        .join("unique");
    let invalid_file = index_dir.join("uk_id.invalid");
    assert!(!invalid_file.exists());

    // The deleted row makes OPTIMIZE rewrite the segment instead of skipping
    // it; the index must be rebuilt to the new locations, not invalidated.
    engine.execute(&mut session, "DELETE FROM t WHERE id = 3")?;
    engine.execute(&mut session, "OPTIMIZE TABLE t")?;
    assert!(!invalid_file.exists());
    let locations = engine
        .index_manager()
        .unique_key_locations("main", "t", "uk_id", &[vec![ScalarValue::Int64(Some(1))]])?
        .unwrap();
    assert_eq!(locations.len(), 1);
    assert!(!locations[0].segment_path.is_empty());
    assert_eq!(
        engine.index_manager().unique_key_exists(
            "main",
            "t",
            "uk_id",
            &[ScalarValue::Int64(Some(3))]
        )?,
        Some(false)
    );

    // The rebuilt snapshot still enforces uniqueness.
    let duplicate = engine.execute(&mut session, "INSERT INTO t VALUES (1, 'dup')");
    assert!(duplicate.is_err());
    let count = first_i64(
        engine.execute(&mut session, "SELECT COUNT(*) FROM t")?,
        0,
        0,
    );
    assert_eq!(count, 2);
    Ok(())
}

#[test]

fn rebuild_location_index_after_delete_keeps_physical_rows() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();
    engine.execute(
        &mut session,
        "CREATE TABLE t (id INT64, name VARCHAR, UNIQUE KEY uk_id (id))",
    )?;
    let mut values = String::new();
    for id in 1..=20i64 {
        if !values.is_empty() {
            values.push_str(", ");
        }
        values.push_str(&format!("({}, 'a')", id));
    }
    engine.execute(
        &mut session,
        &format!("INSERT INTO t (id, name) VALUES {}", values),
    )?;
    engine.execute(&mut session, "FLUSH TABLE t")?;

    // In-segment DELETE leaves a delete bitmap hole; rebuilding the location
    // index (the same full scan UPDATE/OPTIMIZE/compaction rebuilds run) must
    // keep row locations aligned with physical rows.
    engine.execute(&mut session, "DELETE FROM t WHERE id = 5")?;
    engine.execute(&mut session, "REBUILD UNIQUE INDEX uk_id ON t")?;

    for (key, expected_row) in [(6i64, 5u64), (9, 8), (10, 9), (11, 10), (20, 19)] {
        let locations = engine
            .index_manager()
            .unique_key_locations("main", "t", "uk_id", &[vec![ScalarValue::Int64(Some(key))]])?
            .expect("location snapshot present");
        assert_eq!(locations.len(), 1, "key {} must have one location", key);
        assert_eq!(
            locations[0].row_id, expected_row,
            "key {} must keep its physical row location",
            key
        );
    }

    // REPLACE must delete exactly the conflicting row: with shifted locations
    // it previously deleted the row before the target and duplicated the key.
    engine.execute(&mut session, "REPLACE INTO t (id, name) VALUES (10, 'new')")?;
    let total = first_i64(
        engine.execute(&mut session, "SELECT COUNT(*) FROM t")?,
        0,
        0,
    );
    assert_eq!(total, 19);
    let id9 = first_i64(
        engine.execute(&mut session, "SELECT COUNT(*) FROM t WHERE id = 9")?,
        0,
        0,
    );
    assert_eq!(id9, 1, "row before the replaced key must survive");
    let id10 = first_i64(
        engine.execute(&mut session, "SELECT COUNT(*) FROM t WHERE id = 10")?,
        0,
        0,
    );
    assert_eq!(id10, 1, "replaced key must stay unique");
    let name = first_text(
        engine.execute(&mut session, "SELECT name FROM t WHERE id = 10")?,
        0,
        0,
    );
    assert_eq!(name, "new");
    Ok(())
}

#[test]
fn replace_after_delete_keeps_unrelated_rows_after_incremental_refresh() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();

    engine.execute(
        &mut session,
        "CREATE TABLE t (id INT64, name VARCHAR, PRIMARY KEY (id))",
    )?;
    let n: i64 = 24;
    let mut first_batch = String::new();
    for id in 1..=n {
        if !first_batch.is_empty() {
            first_batch.push_str(", ");
        }
        first_batch.push_str(&format!("({}, 'first')", id));
    }
    engine.execute(
        &mut session,
        &format!("INSERT INTO t (id, name) VALUES {}", first_batch),
    )?;
    engine.execute(&mut session, "FLUSH TABLE t")?;

    let mut second_batch = String::new();
    for id in (n + 1)..=(2 * n) {
        if !second_batch.is_empty() {
            second_batch.push_str(", ");
        }
        second_batch.push_str(&format!("({}, 'second')", id));
    }
    engine.execute(
        &mut session,
        &format!("INSERT INTO t (id, name) VALUES {}", second_batch),
    )?;
    engine.execute(&mut session, "FLUSH TABLE t")?;

    // Delete a row inside the latest flushed range, then flush again so the
    // location index refresh re-scans row groups that now contain deleted
    // rows. Locations must keep pointing at physical rows.
    engine.execute(&mut session, "DELETE FROM t WHERE id = 15")?;
    engine.execute(
        &mut session,
        &format!("INSERT INTO t (id, name) VALUES ({}, 'third')", 3 * n),
    )?;
    engine.execute(&mut session, "FLUSH TABLE t")?;

    engine.execute(
        &mut session,
        "REPLACE INTO t (id, name) VALUES (16, 'replaced')",
    )?;
    let name = first_text(
        engine.execute(&mut session, "SELECT name FROM t WHERE id = 16")?,
        0,
        0,
    );
    assert_eq!(name, "replaced");
    let total = first_i64(
        engine.execute(&mut session, "SELECT COUNT(*) FROM t")?,
        0,
        0,
    );
    assert_eq!(total, 2 * n);
    let neighbors = first_i64(
        engine.execute(
            &mut session,
            "SELECT COUNT(*) FROM t WHERE id > 15 AND id <= 24",
        )?,
        0,
        0,
    );
    assert_eq!(neighbors, 9);
    Ok(())
}

#[test]
fn replace_after_update_rebuild_keeps_rows_following_deleted_rows() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();

    engine.execute(
        &mut session,
        "CREATE TABLE t (id INT64, name VARCHAR, PRIMARY KEY (id))",
    )?;
    let n: i64 = 20;
    let mut values = String::new();
    for id in 1..=n {
        if !values.is_empty() {
            values.push_str(", ");
        }
        values.push_str(&format!("({}, 'first')", id));
    }
    engine.execute(
        &mut session,
        &format!("INSERT INTO t (id, name) VALUES {}", values),
    )?;
    engine.execute(&mut session, "FLUSH TABLE t")?;

    engine.execute(&mut session, "DELETE FROM t WHERE id = 1")?;
    // UPDATE rewrites segments and eagerly rebuilds the location index; the
    // rebuild scan must not shift row locations past the deleted row.
    engine.execute(&mut session, "UPDATE t SET name = 'updated' WHERE id = 2")?;

    engine.execute(
        &mut session,
        "REPLACE INTO t (id, name) VALUES (3, 'replaced')",
    )?;
    let total = first_i64(
        engine.execute(&mut session, "SELECT COUNT(*) FROM t")?,
        0,
        0,
    );
    assert_eq!(total, n - 1);
    let name = first_text(
        engine.execute(&mut session, "SELECT name FROM t WHERE id = 2")?,
        0,
        0,
    );
    assert_eq!(name, "updated");
    let name = first_text(
        engine.execute(&mut session, "SELECT name FROM t WHERE id = 3")?,
        0,
        0,
    );
    assert_eq!(name, "replaced");
    let survivors = first_i64(
        engine.execute(&mut session, "SELECT COUNT(*) FROM t WHERE id >= 4")?,
        0,
        0,
    );
    assert_eq!(survivors, n - 3);
    Ok(())
}
