//! End-to-end tests for partition-level TTL expiry (roadmap step 3, P3.4):
//! fully-expired DAY partitions are physically reclaimed by the purge pass,
//! and compaction physically removes rows the read-time TTL filter would
//! otherwise hide forever.

use std::path::Path;

use tempfile::TempDir;

use taibai::net::session::Session;
use taibai::sql::execution::engine::{Engine, ExecResult};
use taibai::storage::segment::SegmentReader;

fn scalar_i64(result: ExecResult) -> i64 {
    let ExecResult::Query { batches, .. } = result else {
        panic!("expected query result");
    };
    let mut out = None;
    for batch in batches {
        for row in 0..batch.num_rows() {
            match batch.scalar(row, 0).expect("value") {
                ScalarValue::Int64(Some(v)) => out = Some(v),
                other => panic!("unexpected value {other:?}"),
            }
        }
    }
    out.expect("at least one row")
}

use taibai::types::scalar::ScalarValue;

fn total_physical_rows(dir: &Path, engine: &Engine, db: &str, table: &str) -> u64 {
    let table_dir = dir.join(db).join(table);
    let mut total = 0u64;
    if !table_dir.exists() {
        return 0;
    }
    for seg in taibai::storage::segment_manager::SegmentManager::discover_segments(&table_dir)
        .expect("discover")
    {
        let reader = SegmentReader::open(&seg).expect("open reader");
        for rg in 0..reader.num_row_groups() {
            total += reader.row_group_rows(rg);
        }
    }
    let _ = engine;
    total
}

fn micros(s: &str) -> i64 {
    chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
        .expect("datetime")
        .and_utc()
        .timestamp_micros()
}

#[test]
fn ttl_purge_drops_expired_day_partitions_and_updates_catalog() {
    let temp_dir = TempDir::new().expect("temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("engine");
    let mut session = Session::new("root".to_string(), "main".to_string());

    engine
        .execute(
            &mut session,
            "CREATE TABLE events (
                ts TIMESTAMP,
                value DOUBLE
            ) PARTITION BY DAY(ts) TTL (ts, 3600)",
        )
        .expect("create events");

    // Two rows on an old day (long expired with a one-hour TTL) and two rows
    // far in the future (still live).
    engine
        .execute(
            &mut session,
            &format!(
                "INSERT INTO events VALUES ({}, 1.0), ({}, 2.0)",
                micros("2024-01-15 10:00:00"),
                micros("2024-01-15 11:00:00")
            ),
        )
        .expect("insert old");
    engine
        .execute(
            &mut session,
            &format!(
                "INSERT INTO events VALUES ({}, 3.0), ({}, 4.0)",
                micros("2030-01-01 00:00:00"),
                micros("2030-01-01 00:00:01")
            ),
        )
        .expect("insert fresh");
    engine
        .execute(&mut session, "FLUSH TABLE events")
        .expect("flush");

    let table_dir = temp_dir.path().join("main").join("events");
    assert!(
        table_dir.join("p20240115_0001.dat").exists()
            || table_dir.join("p20240115_0000.dat").exists(),
        "old partition file should exist before purge: {:?}",
        std::fs::read_dir(&table_dir)
            .unwrap()
            .flatten()
            .map(|e| e.file_name())
            .collect::<Vec<_>>()
    );

    // Before the purge the read-time filter already hides the old rows.
    let count = engine
        .execute(&mut session, "SELECT COUNT(*) FROM events")
        .expect("count");
    assert_eq!(scalar_i64(count), 2);

    // The purge physically reclaims the expired partition.
    let reclaimed = engine.run_ttl_partition_purge().expect("purge");
    assert_eq!(reclaimed, 2);

    assert!(!table_dir.join("p20240115_0001.dat").exists());
    assert!(!table_dir.join("p20240115_0000.dat").exists());

    // Queries still return exactly the live rows.
    let count = engine
        .execute(&mut session, "SELECT COUNT(*) FROM events")
        .expect("count after purge");
    assert_eq!(scalar_i64(count), 2);

    // Catalog accounting is updated. (The write path does not always
    // populate the partitions list, so only assert the expired key is gone.)
    let meta = engine.get_table_meta("main", "events").expect("meta");
    assert_eq!(meta.row_count, 2);
    assert!(meta.partitions.iter().all(|info| info.key != "20240115"));

    // Idempotent: a second run has nothing to do.
    let reclaimed = engine.run_ttl_partition_purge().expect("purge 2");
    assert_eq!(reclaimed, 0);

    engine.segment_mgr().shutdown();
}

#[test]
fn ttl_purge_drains_expired_partition_memtables_before_deleting_files() {
    let temp_dir = TempDir::new().expect("temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("engine");
    let mut session = Session::new("root".to_string(), "main".to_string());

    engine
        .execute(
            &mut session,
            "CREATE TABLE events_mem (
                ts TIMESTAMP,
                value DOUBLE
            ) PARTITION BY DAY(ts) TTL (ts, 3600)",
        )
        .expect("create events_mem");

    // Rows on disk make the partition a purge candidate...
    engine.begin_bulk_import();
    session.bulk_import_mode = true;
    engine
        .execute(
            &mut session,
            &format!(
                "INSERT INTO events_mem VALUES ({}, 1.0), ({}, 2.0)",
                micros("2024-01-15 10:00:00"),
                micros("2024-01-15 11:00:00")
            ),
        )
        .expect("insert old to segments");
    session.bulk_import_mode = false;
    engine.end_bulk_import();

    // ...and rows that are still memtable-resident for the same expired
    // partition. The purge must drain them inside the partition lock and
    // delete the flushed files too; otherwise a later flush resurrects the
    // partition.
    engine
        .execute(
            &mut session,
            &format!(
                "INSERT INTO events_mem VALUES ({}, 3.0), ({}, 4.0)",
                micros("2024-01-15 12:00:00"),
                micros("2024-01-15 13:00:00")
            ),
        )
        .expect("insert old to memtable");

    let reclaimed = engine.run_ttl_partition_purge().expect("purge");
    assert_eq!(reclaimed, 4);

    engine
        .execute(&mut session, "FLUSH TABLE events_mem")
        .expect("flush after purge");

    let table_dir = temp_dir.path().join("main").join("events_mem");
    let leftovers = taibai::storage::segment_manager::SegmentManager::discover_segments(&table_dir)
        .expect("discover");
    assert!(
        leftovers.iter().all(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_none_or(|name| !name.starts_with("p20240115"))
        }),
        "expired partition must not resurrect after the purge: {:?}",
        leftovers
    );

    let count = engine
        .execute(&mut session, "SELECT COUNT(*) FROM events_mem")
        .expect("count after purge");
    assert_eq!(scalar_i64(count), 0);

    engine.segment_mgr().shutdown();
}

#[test]
fn compaction_physically_purges_expired_rows() {
    let temp_dir = TempDir::new().expect("temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("engine");
    let mut session = Session::new("root".to_string(), "main".to_string());

    engine
        .execute(
            &mut session,
            "CREATE TABLE plain (
                ts TIMESTAMP,
                value DOUBLE
            ) TTL (ts, 3600)",
        )
        .expect("create plain");

    engine
        .execute(
            &mut session,
            &format!(
                "INSERT INTO plain VALUES ({}, 1.0), ({}, 2.0), ({}, 3.0)",
                micros("2024-01-15 10:00:00"),
                micros("2024-01-15 11:00:00"),
                micros("2024-01-15 12:00:00")
            ),
        )
        .expect("insert old");
    engine
        .execute(&mut session, "FLUSH TABLE plain")
        .expect("flush old");
    engine
        .segment_mgr()
        .seal_table_active("main", "plain")
        .expect("seal old segment");

    engine
        .execute(
            &mut session,
            &format!(
                "INSERT INTO plain VALUES ({}, 4.0), ({}, 5.0)",
                micros("2030-01-01 00:00:00"),
                micros("2030-01-01 00:00:01")
            ),
        )
        .expect("insert fresh");
    engine
        .execute(&mut session, "FLUSH TABLE plain")
        .expect("flush fresh");
    engine
        .segment_mgr()
        .seal_table_active("main", "plain")
        .expect("seal fresh segment");

    assert_eq!(
        total_physical_rows(temp_dir.path(), &engine, "main", "plain"),
        5
    );

    engine
        .execute(&mut session, "OPTIMIZE TABLE plain")
        .expect("optimize");

    // Compaction physically dropped the expired rows.
    assert_eq!(
        total_physical_rows(temp_dir.path(), &engine, "main", "plain"),
        2
    );

    let count = engine
        .execute(&mut session, "SELECT COUNT(*) FROM plain")
        .expect("count after optimize");
    assert_eq!(scalar_i64(count), 2);

    engine.segment_mgr().shutdown();
}
