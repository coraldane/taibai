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
fn rebuild_unique_index_writes_v2_blocks_for_large_keyset() -> Result<()> {
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
    assert_eq!(&bytes[..8], b"TBIUKV2\0");

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
    assert!(invalid_file.exists());

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
fn bulk_payload_invalidates_rebuilt_unique_index() -> Result<()> {
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
    engine.exec_bulk_insert_payload_with_wal_control("main", &payload, false)?;

    let duplicate = engine.execute(&mut session, "INSERT INTO t (id, name) VALUES (2, 3)");
    assert!(duplicate.is_err());
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
