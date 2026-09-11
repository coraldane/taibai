use super::*;

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
