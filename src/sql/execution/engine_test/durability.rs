use super::*;

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
