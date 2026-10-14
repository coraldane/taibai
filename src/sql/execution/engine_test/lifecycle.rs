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

#[test]
fn optimize_partition_does_not_freeze_other_partitions() -> Result<()> {
    use std::sync::mpsc;
    use std::time::Duration;

    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(Engine::new(dir.path(), "main")?);
    let mut session = session();

    engine.execute(
        &mut session,
        "CREATE TABLE opt_freeze (id INT64, value INT64) PARTITION BY id",
    )?;
    engine.begin_bulk_import();
    session.bulk_import_mode = true;
    engine.execute(&mut session, "INSERT INTO opt_freeze VALUES (1, 10)")?;
    engine.execute(&mut session, "INSERT INTO opt_freeze VALUES (2, 20)")?;
    session.bulk_import_mode = false;
    engine.end_bulk_import();

    let held = engine
        .segment_mgr()
        .partition_lock_arc_for_tests("main", "opt_freeze", "1");
    let held_guard = held.lock_for_test();

    let (done_tx, done_rx) = mpsc::channel::<bool>();
    let optimize_engine = Arc::clone(&engine);
    let optimizer_session =
        crate::net::session::Session::new("root".to_string(), "main".to_string());
    let optimizer = std::thread::spawn(move || {
        let mut session = optimizer_session;
        let ok = optimize_engine
            .execute(&mut session, "OPTIMIZE TABLE opt_freeze ON PARTITION p1")
            .is_ok();
        done_tx.send(ok).unwrap();
    });

    engine.execute(&mut session, "INSERT INTO opt_freeze VALUES (2, 21)")?;

    assert!(
        done_rx.try_recv().is_err(),
        "optimize must stay blocked while the target partition lock is held"
    );

    drop(held_guard);
    assert!(
        done_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("optimize thread must not hang"),
        "optimize must succeed after the partition lock is released"
    );
    optimizer.join().unwrap();

    assert_eq!(
        first_i64(
            engine.execute(&mut session, "SELECT COUNT(*) FROM opt_freeze")?,
            0,
            0
        ),
        3
    );
    assert_eq!(
        first_i64(
            engine.execute(&mut session, "SELECT COUNT(*) FROM opt_freeze WHERE id = 1")?,
            0,
            0
        ),
        1
    );
    Ok(())
}

#[test]
fn optimize_heavy_phase_keeps_target_partition_writable() -> Result<()> {
    use std::sync::mpsc;
    use std::time::{Duration, Instant};

    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(Engine::new(dir.path(), "main")?);
    let mut session = session();

    engine.execute(
        &mut session,
        "CREATE TABLE opt_writable (id INT64, value INT64) PARTITION BY id",
    )?;
    engine.begin_bulk_import();
    session.bulk_import_mode = true;
    engine.execute(
        &mut session,
        "INSERT INTO opt_writable VALUES (1, 10), (1, 11), (1, 12)",
    )?;
    session.bulk_import_mode = false;
    engine.end_bulk_import();

    // Mark a row deleted so the OPTIMIZE rewrite path runs; a clean single
    // segment would be skipped without ever reaching the heavy phase.
    engine.execute(&mut session, "DELETE FROM opt_writable WHERE value = 10")?;

    // Regression for the partition-lock pileup: the decode/sort/rewrite of
    // a partition compaction must run OUTSIDE the partition mutation lock,
    // so INSERTs into the very partition being compacted proceed. Before
    // the split they queued behind the whole rewrite and timed out after
    // mutation_lock_timeout_secs.
    let result = (|| -> Result<()> {
        crate::storage::core::compactor::Compactor::set_heavy_phase_delay_for_tests(2_000);

        let (done_tx, done_rx) = mpsc::channel::<bool>();
        let optimize_engine = Arc::clone(&engine);
        let optimizer_session =
            crate::net::session::Session::new("root".to_string(), "main".to_string());
        let optimizer = std::thread::spawn(move || {
            let mut session = optimizer_session;
            let ok = optimize_engine
                .execute(&mut session, "OPTIMIZE TABLE opt_writable ON PARTITION p1")
                .is_ok();
            done_tx.send(ok).unwrap();
        });

        std::thread::sleep(Duration::from_millis(200));
        let insert_started = Instant::now();
        engine.execute(&mut session, "INSERT INTO opt_writable VALUES (1, 99)")?;
        // The heavy phase stalls for 2s; the INSERT must finish well before
        // that. 1.9s (not 1.5s) leaves headroom for a loaded CI machine
        // while still proving the INSERT never queues behind the 2s stall.
        assert!(
            insert_started.elapsed() < Duration::from_millis(1_900),
            "INSERT into the partition being compacted must not wait for the compaction heavy phase"
        );

        assert!(
            done_rx.try_recv().is_err(),
            "optimize must still be in its heavy phase while the INSERT completed"
        );
        assert!(
            done_rx
                .recv_timeout(Duration::from_secs(10))
                .expect("optimize thread must not hang"),
            "optimize must succeed after its heavy phase"
        );
        optimizer.join().unwrap();
        Ok(())
    })();
    crate::storage::core::compactor::Compactor::set_heavy_phase_delay_for_tests(0);
    result?;

    assert_eq!(
        first_i64(
            engine.execute(&mut session, "SELECT COUNT(*) FROM opt_writable")?,
            0,
            0
        ),
        3
    );
    Ok(())
}

#[test]
fn delete_partition_eq_does_not_freeze_other_partitions() -> Result<()> {
    use std::sync::mpsc;
    use std::time::Duration;

    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(Engine::new(dir.path(), "main")?);
    let mut session = session();

    engine.execute(
        &mut session,
        "CREATE TABLE del_freeze (id INT64, value INT64) PARTITION BY id",
    )?;
    engine.begin_bulk_import();
    session.bulk_import_mode = true;
    engine.execute(&mut session, "INSERT INTO del_freeze VALUES (1, 10)")?;
    engine.execute(&mut session, "INSERT INTO del_freeze VALUES (2, 20)")?;
    session.bulk_import_mode = false;
    engine.end_bulk_import();

    let held = engine
        .segment_mgr()
        .partition_lock_arc_for_tests("main", "del_freeze", "1");
    let held_guard = held.lock_for_test();

    let (done_tx, done_rx) = mpsc::channel::<bool>();
    let delete_engine = Arc::clone(&engine);
    let deleter_session = crate::net::session::Session::new("root".to_string(), "main".to_string());
    let deleter = std::thread::spawn(move || {
        let mut session = deleter_session;
        let ok = delete_engine
            .execute(&mut session, "DELETE FROM del_freeze WHERE id = 1")
            .is_ok();
        done_tx.send(ok).unwrap();
    });

    engine.execute(&mut session, "INSERT INTO del_freeze VALUES (2, 21)")?;

    assert!(
        done_rx.try_recv().is_err(),
        "delete must stay blocked while the target partition lock is held"
    );

    drop(held_guard);
    assert!(
        done_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("delete thread must not hang"),
        "delete must succeed after the partition lock is released"
    );
    deleter.join().unwrap();

    assert_eq!(
        first_i64(
            engine.execute(&mut session, "SELECT COUNT(*) FROM del_freeze")?,
            0,
            0
        ),
        2
    );
    Ok(())
}

#[test]
fn optimize_partition_accepts_quoted_key_and_reports_granularity() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();

    // YEAR bucketing: the physical partition key is the 4-digit year, not
    // the YYYYMMDD source value. A quoted DAY-style name must be rejected
    // with the available partitions listed, and the quoted year must work.
    engine.execute(
        &mut session,
        "CREATE TABLE t_stock_minute (security_id STRING, deal_date INT64) PARTITION BY YEAR(deal_date)",
    )?;
    engine.execute(
        &mut session,
        "INSERT INTO t_stock_minute VALUES ('000529.SZ', 20261008)",
    )?;
    engine.execute(&mut session, "FLUSH TABLE t_stock_minute")?;

    let day_style = engine
        .execute(
            &mut session,
            "OPTIMIZE TABLE t_stock_minute ON PARTITION '20261008'",
        )
        .unwrap_err()
        .to_string();
    assert!(
        day_style.contains("partition '20261008' not found"),
        "day-style key must be rejected, got: {}",
        day_style
    );
    assert!(
        day_style.contains("available partitions: 2026"),
        "error must list the year-granularity partitions, got: {}",
        day_style
    );

    engine.execute(
        &mut session,
        "OPTIMIZE TABLE t_stock_minute ON PARTITION '2026'",
    )?;
    assert_eq!(
        first_i64(
            engine.execute(&mut session, "SELECT COUNT(*) FROM t_stock_minute")?,
            0,
            0
        ),
        1
    );
    Ok(())
}

#[test]
fn show_partitions_lists_sealed_and_active_partitions() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();

    engine.execute(
        &mut session,
        "CREATE TABLE minute_year (security_id STRING, deal_date INT64) PARTITION BY YEAR(deal_date)",
    )?;
    engine.execute(&mut session, "CREATE TABLE unpartitioned (id INT64)")?;

    // Rows still in the active writer must already show up, so the user can
    // discover the partition name before any flush.
    engine.execute(
        &mut session,
        "INSERT INTO minute_year VALUES ('A', 20261008)",
    )?;
    let result = engine.execute(&mut session, "SHOW PARTITIONS FROM minute_year")?;
    let ExecResult::Query { batches, .. } = &result else {
        panic!("expected query result");
    };
    assert_eq!(batches[0].num_rows(), 1, "active partition must be listed");
    assert_eq!(
        batches[0]
            .scalar(0, 1)
            .and_then(|value| value.to_text())
            .unwrap(),
        "2026"
    );

    engine.execute(&mut session, "FLUSH TABLE minute_year")?;
    engine.execute(
        &mut session,
        "INSERT INTO minute_year VALUES ('B', 20250102)",
    )?;

    let result = engine.execute(&mut session, "SHOW PARTITIONS FROM minute_year")?;
    let ExecResult::Query { batches, .. } = &result else {
        panic!("expected query result");
    };
    assert_eq!(batches[0].num_rows(), 2);
    // BTreeMap ordering: 2025 (active, 1 row) before 2026 (sealed, 1 row).
    let text = |row: usize, col: usize| {
        batches[0]
            .scalar(row, col)
            .and_then(|value| value.to_text())
            .unwrap()
    };
    assert_eq!(text(0, 1), "2025");
    assert_eq!(text(0, 2), "1");
    assert_eq!(text(1, 1), "2026");
    assert_eq!(
        first_i64(
            engine.execute(&mut session, "SELECT COUNT(*) FROM minute_year")?,
            0,
            0
        ),
        2
    );

    // Non-partitioned tables never appear, per-table or database-wide.
    let result = engine.execute(&mut session, "SHOW PARTITIONS FROM unpartitioned")?;
    let ExecResult::Query { batches, .. } = result else {
        panic!("expected query result");
    };
    assert_eq!(batches[0].num_rows(), 0);
    let result = engine.execute(&mut session, "SHOW PARTITIONS")?;
    let ExecResult::Query { batches, .. } = result else {
        panic!("expected query result");
    };
    assert_eq!(batches[0].num_rows(), 2);
    Ok(())
}
