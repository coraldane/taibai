use super::*;

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
