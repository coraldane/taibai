use super::*;

#[test]
fn optimize_audit_external_merge_cache() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let dir = tmp.path().join("default/t");
    std::fs::create_dir_all(&dir)?;
    let schema = test_schema();
    let mut runs = Vec::new();
    for r in 0..40 {
        let path = dir.join(format!("run{r}.tmp"));
        let rows = (0..64).map(|i| (r + i * 40, "payload")).collect::<Vec<_>>();
        write_segment(&path, &schema, &rows)?;
        runs.push(path);
    }
    let mgr = Arc::new(SegmentManager::new(tmp.path())?);
    let c = Compactor::new(mgr, tmp.path().to_path_buf());
    let out = dir.join("out.tmp");
    let mut writer = SegmentWriter::create(&out, schema.clone(), 8192, 3)?;
    assert_eq!(
        c.merge_runs_into_writer(&runs, &schema, &[0], 8192, &mut writer)?,
        2560
    );
    writer.seal()?;
    assert_eq!(read_ids(&out)?, (0..2560).collect::<Vec<_>>());
    assert_eq!(
        super::super::stream_merge::MERGE_DECODED_GROUPS.with(|n| n.get()),
        40
    );
    Ok(())
}

#[test]
fn optimize_audit_streaming_query_keeps_all_rows() -> Result<()> {
    use crate::net::session::Session;
    use crate::sql::execution::engine::Engine;
    let tmp = tempfile::tempdir()?;
    let engine = Engine::new(tmp.path(), "default")?;
    let mut session = Session::new("root".into(), "default".into());
    engine.execute(&mut session, "CREATE TABLE t (id INT64, name VARCHAR)")?;
    let schema = test_schema();
    let dir = tmp.path().join("default/t");
    std::fs::create_dir_all(&dir)?;
    write_segment(&dir.join("seg_00001.dat"), &schema, &[(3, "c"), (1, "a")])?;
    write_segment(&dir.join("seg_00002.dat"), &schema, &[(2, "b")])?;
    let mut ids = Vec::new();
    let mut optimized = false;
    let emitted = engine.execute_streaming_query(&session, "SELECT id, name FROM t", |batch| {
        for r in 0..batch.num_rows() {
            ids.push(batch.scalar(r, 0).unwrap());
        }
        if !optimized {
            optimized = true;
            let mut other = Session::new("root".into(), "default".into());
            engine.execute(&mut other, "OPTIMIZE TABLE t")?;
        }
        Ok(())
    })?;
    assert!(emitted.is_some());
    assert_eq!(
        ids.len(),
        3,
        "pure physical optimization caused a streaming query to lose rows: {ids:?}"
    );
    Ok(())
}

#[test]
fn optimize_audit_prepared_outputs_are_isolated() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let dir = tmp.path().join("default/t");
    std::fs::create_dir_all(&dir)?;
    let schema = test_schema();
    write_segment(&dir.join("seg_00001.dat"), &schema, &[(3, "c"), (1, "a")])?;
    write_segment(&dir.join("seg_00002.dat"), &schema, &[(2, "b")])?;
    let mgr = Arc::new(SegmentManager::new(tmp.path())?);
    let c = Compactor::new(mgr.clone(), tmp.path().to_path_buf());
    let _maintenance = mgr.capture_maintenance_snapshot("default", "t", &dir, None)?;
    let files = _maintenance.planned_segments();
    let a = c.prepare_compact_segment_group(
        "default",
        "t",
        &dir,
        None,
        files.clone(),
        &schema,
        &[0],
        &[0],
        8192,
        None,
        3,
    )?;
    // Background cleanup can prepare the same input without cluster sorting
    // before the foreground optimize commits its already-prepared output.
    let _b = c.prepare_compact_segment_group(
        "default",
        "t",
        &dir,
        None,
        files,
        &schema,
        &[],
        &[0],
        8192,
        None,
        3,
    )?;
    mgr.with_partition_mutation_lock("default", "t", "", || {
        c.commit_prepared_compaction("default", "t", &dir, None, a)
    })?;
    let outputs = SegmentManager::discover_segments(&dir)?;
    assert_eq!(
        read_ids(&outputs[0])?,
        vec![1, 2, 3],
        "another prepared rewrite overwrote optimize output"
    );
    Ok(())
}

#[test]
fn optimize_audit_expired_small_segment_is_removed() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let dir = tmp.path().join("default/t");
    std::fs::create_dir_all(&dir)?;
    let schema = ttl_schema();
    let input = dir.join("seg_00001.dat");
    write_ttl_segment(&input, &schema, &[now_micros() - 3_600_000_000])?;
    let mgr = Arc::new(SegmentManager::new(tmp.path())?);
    let c = Compactor::new(mgr, tmp.path().to_path_buf());
    let stats = c.optimize_table("default", "t", &ttl_meta(&schema, 60))?;
    assert_eq!(
        stats.rows_dropped_ttl, 1,
        "fully expired small segment must be purged"
    );
    assert!(!input.exists());
    Ok(())
}

#[cfg(windows)]
#[test]
fn optimize_audit_retirement_failure_must_not_publish_duplicates() -> Result<()> {
    use std::os::windows::fs::OpenOptionsExt;
    let tmp = tempfile::tempdir()?;
    let dir = tmp.path().join("default/t");
    std::fs::create_dir_all(&dir)?;
    let schema = test_schema();
    let input = dir.join("seg_00001.dat");
    write_segment(&input, &schema, &[(3, "c"), (1, "a")])?;
    let mgr = Arc::new(SegmentManager::new(tmp.path())?);
    let c = Compactor::new(mgr.clone(), tmp.path().to_path_buf());
    let maintenance = mgr.capture_maintenance_snapshot("default", "t", &dir, None)?;
    let a = c.prepare_compact_segment_group(
        "default",
        "t",
        &dir,
        None,
        maintenance.planned_segments(),
        &schema,
        &[0],
        &[0],
        8192,
        None,
        3,
    )?;
    // A live handle denying delete sharing makes retirement fail after rename.
    let held = std::fs::OpenOptions::new()
        .read(true)
        .share_mode(3)
        .open(&input)?;
    let result = mgr.with_partition_mutation_lock("default", "t", "", || {
        c.commit_prepared_compaction("default", "t", &dir, None, a)
    });
    // Release the maintenance plan lease before verifying recovery: exactly
    // like production, where `optimize_table` returns and drops its snapshot.
    // Retirement is only drained once the last directory reader leaves, so
    // recovery called under an active lease can only queue, not delete.
    drop(maintenance);
    let outputs = SegmentManager::discover_segments(&dir)?;
    drop(held);
    assert!(
        result.is_err() || outputs.len() == 1,
        "OPTIMIZE returned success but {} live segments contain both old and replacement rows",
        outputs.len()
    );
    crate::storage::segment_manager::manager_core_test_support::recover_replace_journals_for_test(
        &dir,
    );
    assert!(
        !input.exists(),
        "recovery must retry the deferred retirement"
    );
    assert_eq!(SegmentManager::discover_segments(&dir)?.len(), 1);
    Ok(())
}

#[test]
fn readers_keep_old_files_across_chained_replacements() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let dir = tmp.path().join("default/t");
    std::fs::create_dir_all(&dir)?;
    let schema = test_schema();
    let old = dir.join("seg_00001.dat");
    write_segment(&old, &schema, &[(2, "b"), (1, "a")])?;
    let snapshot = crate::storage::segment::snapshot::snapshot_segments(&dir)?;
    let mgr = Arc::new(SegmentManager::new(tmp.path())?);
    let c = Compactor::new(mgr, tmp.path().to_path_buf());
    c.optimize_table("default", "t", &test_meta(&schema))?;
    let middle = SegmentManager::discover_segments(&dir)?[0].clone();
    c.optimize_table("default", "t", &test_meta(&schema))?;
    assert_eq!(read_ids(&old)?, vec![2, 1]);
    assert!(middle.exists());
    let live = SegmentManager::discover_segments(&dir)?;
    assert_eq!(live.len(), 1);
    assert_eq!(read_ids(&live[0])?, vec![1, 2]);
    drop(snapshot);
    assert!(!old.exists());
    assert!(!middle.exists());
    assert!(live[0].exists());
    Ok(())
}

#[test]
fn concurrent_snapshot_discovery_and_retirement_remain_consistent() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let dir = tmp.path().join("default/t");
    std::fs::create_dir_all(&dir)?;
    let schema = test_schema();
    write_segment(
        &dir.join("seg_00001.dat"),
        &schema,
        &[(3, "c"), (1, "a"), (2, "b")],
    )?;
    let mgr = Arc::new(SegmentManager::new(tmp.path())?);
    let c = Compactor::new(mgr, tmp.path().to_path_buf());
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let reader_barrier = barrier.clone();
    let reader_dir = dir.clone();
    let reader = std::thread::spawn(move || -> Result<()> {
        reader_barrier.wait();
        for _ in 0..100 {
            let snapshot = crate::storage::segment::snapshot::snapshot_segments(&reader_dir)?;
            let mut ids = Vec::new();
            for file in &snapshot.paths {
                ids.extend(read_ids(file)?);
            }
            ids.sort_unstable();
            assert_eq!(ids, vec![1, 2, 3]);
        }
        Ok(())
    });
    barrier.wait();
    for _ in 0..20 {
        c.optimize_table("default", "t", &test_meta(&schema))?;
    }
    reader.join().unwrap()?;
    Ok(())
}

#[test]
fn partial_optimize_failure_accounts_for_committed_ttl_rows() -> Result<()> {
    use crate::net::session::Session;
    use crate::sql::execution::engine::Engine;
    let tmp = tempfile::tempdir()?;
    let engine = Engine::new(tmp.path(), "default")?;
    let mut session = Session::new("root".into(), "default".into());
    engine.execute(
        &mut session,
        "CREATE TABLE t (id INT64, ts TIMESTAMP) PARTITION BY id TTL (ts, 60)",
    )?;
    let dir = tmp.path().join("default/t");
    std::fs::create_dir_all(&dir)?;
    write_ttl_segment(
        &dir.join("p1_0001.dat"),
        &ttl_schema(),
        &[now_micros() - 3_600_000_000],
    )?;
    std::fs::write(dir.join("p2_0001.dat"), b"corrupt segment")?;
    engine
        .catalogs()
        .write()
        .unwrap()
        .get_mut("default")
        .unwrap()
        .get_table_mut("t")
        .unwrap()
        .row_count = 2;
    assert!(engine.execute(&mut session, "OPTIMIZE TABLE t").is_err());
    assert!(!dir.join("p1_0001.dat").exists());
    assert_eq!(
        engine
            .catalogs()
            .read()
            .unwrap()
            .get("default")
            .unwrap()
            .get_table("t")
            .unwrap()
            .row_count,
        1
    );
    Ok(())
}

struct BudgetReset;
impl Drop for BudgetReset {
    fn drop(&mut self) {
        CLUSTERED_MERGE_BUDGET_OVERRIDE.store(0, AtomicOrdering::Relaxed);
    }
}

#[cfg(windows)]
#[test]
fn restart_does_not_reuse_names_reserved_by_failed_retirement() -> Result<()> {
    use std::os::windows::fs::OpenOptionsExt;
    let tmp = tempfile::tempdir()?;
    let dir = tmp.path().join("default/t");
    std::fs::create_dir_all(&dir)?;
    let old = dir.join("seg_00001.dat");
    write_segment(&old, &test_schema(), &[(1, "a")])?;
    let held = std::fs::OpenOptions::new()
        .read(true)
        .share_mode(3)
        .open(&old)?;
    crate::storage::segment_manager::manager_core_test_support::write_replace_journal_for_test(
        &dir.join("partial.retire"),
        &[old.clone(), dir.join("seg_00009.dat")],
    )?;
    let mgr = SegmentManager::new(tmp.path())?;
    assert_eq!(mgr.next_segment_path(&dir, "")?, dir.join("seg_00010.dat"));
    drop(held);
    crate::storage::segment_manager::manager_core_test_support::recover_replace_journals_for_test(
        &dir,
    );
    assert!(!old.exists());
    Ok(())
}

#[test]
fn partition_optimize_with_unique_index_does_not_wait_for_sibling_lock() -> Result<()> {
    use crate::net::session::Session;
    use crate::sql::execution::engine::Engine;
    let tmp = tempfile::tempdir()?;
    let engine = Arc::new(Engine::new(tmp.path(), "default")?);
    let mut session = Session::new("root".into(), "default".into());
    engine.execute(
        &mut session,
        "CREATE TABLE t (id INT64, value INT64, UNIQUE KEY uk_value (value)) PARTITION BY id",
    )?;
    engine.execute(
        &mut session,
        "INSERT INTO t VALUES (1, 10), (1, 11), (2, 20)",
    )?;
    engine.execute(&mut session, "FLUSH TABLE t")?;
    engine.execute(&mut session, "DELETE FROM t WHERE id = 1 AND value = 10")?;
    let sibling = engine
        .segment_mgr()
        .partition_lock_arc_for_tests("default", "t", "2");
    let held = sibling.lock_for_test();
    let (tx, rx) = std::sync::mpsc::channel();
    let other = engine.clone();
    let worker = std::thread::spawn(move || {
        let mut session = Session::new("root".into(), "default".into());
        tx.send(
            other
                .execute(&mut session, "OPTIMIZE TABLE t ON PARTITION '1'")
                .map(|_| ()),
        )
        .unwrap();
    });
    let result = rx.recv_timeout(std::time::Duration::from_secs(5));
    drop(held);
    worker.join().unwrap();
    result.expect("partition optimization must not acquire the sibling partition lock")?;
    assert!(engine
        .execute(&mut session, "INSERT INTO t VALUES (1, 11)")
        .is_err());
    engine.execute(&mut session, "INSERT INTO t VALUES (1, 12)")?;
    Ok(())
}

#[test]
fn variable_width_external_merge_uses_multiple_bounded_waves() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let dir = tmp.path().join("default/t");
    std::fs::create_dir_all(&dir)?;
    let schema = test_schema();
    let text = "x".repeat(128);
    let rows = (0..2000)
        .rev()
        .map(|id| (id, text.as_str()))
        .collect::<Vec<_>>();
    write_segment_with_row_group_size(&dir.join("seg_00001.dat"), &schema, &rows, 32)?;
    let mgr = Arc::new(SegmentManager::with_config(
        tmp.path(),
        SegmentConfig {
            row_group_target_rows: 32,
            ..SegmentConfig::default()
        },
    )?);
    let c = Compactor::new(mgr, tmp.path().to_path_buf());
    CLUSTERED_MERGE_BUDGET_OVERRIDE.store(128 * 1024, AtomicOrdering::Relaxed);
    let _reset = BudgetReset;
    let stats = c.optimize_table("default", "t", &test_meta(&schema))?;
    assert_eq!(stats.rows_written, 2000);
    assert!(super::super::clustered::EXTERNAL_MERGE_WAVES.with(|n| n.get()) > 0);
    let files = SegmentManager::discover_segments(&dir)?;
    assert_eq!(files.len(), 1);
    assert_eq!(read_ids(&files[0])?, (0..2000).collect::<Vec<_>>());
    assert!(!std::fs::read_dir(&dir)?.any(|entry| entry
        .unwrap()
        .file_name()
        .to_string_lossy()
        .contains(".tmp.")));
    Ok(())
}

#[test]
fn oversized_row_group_fails_without_changing_inputs_or_leaking_runs() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let dir = tmp.path().join("default/t");
    std::fs::create_dir_all(&dir)?;
    let schema = test_schema();
    let input = dir.join("seg_00001.dat");
    write_segment(&input, &schema, &[(1, &"x".repeat(100_000))])?;
    let mgr = Arc::new(SegmentManager::new(tmp.path())?);
    let c = Compactor::new(mgr, tmp.path().to_path_buf());
    CLUSTERED_MERGE_BUDGET_OVERRIDE.store(128 * 1024, AtomicOrdering::Relaxed);
    let _reset = BudgetReset;
    assert!(matches!(
        c.optimize_table("default", "t", &test_meta(&schema)),
        Err(TaibaiError::ResourceBusy(_))
    ));
    assert_eq!(SegmentManager::discover_segments(&dir)?, vec![input]);
    // segment_catalog is a persistent capture artifact, not a leaked merge run.
    assert_eq!(
        std::fs::read_dir(&dir)?
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .filter(|name| !name.starts_with("segment_catalog"))
            .count(),
        1
    );
    Ok(())
}
