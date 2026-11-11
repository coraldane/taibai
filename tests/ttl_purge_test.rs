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

/// Unpartitioned TTL tables used to leave expired rows behind the read-time
/// filter forever: the purge pass only understood whole temporal partitions.
/// Fully-expired segments must now be physically dropped file-by-file, with
/// catalog accounting and idempotent reruns. (The selective multi-segment
/// case — expired files dropped while neighbours survive — is covered by
/// the maintenance unit tests; here the whole table is expired.)
#[test]
fn ttl_purge_drops_fully_expired_segments_of_unpartitioned_table() {
    let temp_dir = TempDir::new().expect("temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("engine");
    let mut session = Session::new("root".to_string(), "main".to_string());

    engine
        .execute(
            &mut session,
            "CREATE TABLE plain_ticks (
                ts TIMESTAMP,
                value DOUBLE
            ) TTL (ts, 3600)",
        )
        .expect("create plain_ticks");

    engine.begin_bulk_import();
    session.bulk_import_mode = true;
    engine
        .execute(
            &mut session,
            &format!(
                "INSERT INTO plain_ticks VALUES ({}, 1.0), ({}, 2.0)",
                micros("2024-01-15 10:00:00"),
                micros("2024-01-15 11:00:00")
            ),
        )
        .expect("insert old (bulk)");
    session.bulk_import_mode = false;
    engine.end_bulk_import();
    engine.flush_all_buffers().expect("flush bulk buffers");
    engine
        .segment_mgr()
        .seal_table_active("main", "plain_ticks")
        .expect("seal bulk segment");

    let table_dir = temp_dir.path().join("main").join("plain_ticks");
    let segments_before =
        taibai::storage::segment_manager::SegmentManager::discover_segments(&table_dir)
            .expect("discover before");
    assert!(!segments_before.is_empty());
    assert_eq!(
        total_physical_rows(temp_dir.path(), &engine, "main", "plain_ticks"),
        2
    );

    // The read-time filter already hides the old rows; the purge must also
    // reclaim them physically.
    let count = engine
        .execute(&mut session, "SELECT COUNT(*) FROM plain_ticks")
        .expect("count before purge");
    assert_eq!(scalar_i64(count), 0);

    let reclaimed = engine.run_ttl_partition_purge().expect("purge");
    assert_eq!(reclaimed, 2);

    let segments_after =
        taibai::storage::segment_manager::SegmentManager::discover_segments(&table_dir)
            .expect("discover after");
    assert!(
        segments_after.is_empty(),
        "fully-expired segments must be dropped: {segments_after:?}"
    );
    assert_eq!(
        total_physical_rows(temp_dir.path(), &engine, "main", "plain_ticks"),
        0
    );

    let count = engine
        .execute(&mut session, "SELECT COUNT(*) FROM plain_ticks")
        .expect("count after purge");
    assert_eq!(scalar_i64(count), 0);

    let meta = engine.get_table_meta("main", "plain_ticks").expect("meta");
    assert_eq!(meta.row_count, 0, "catalog row_count must drop with the files");

    // Idempotent: nothing left to reclaim.
    let reclaimed = engine.run_ttl_partition_purge().expect("purge 2");
    assert_eq!(reclaimed, 0);

    engine.segment_mgr().shutdown();
}

/// A segment straddling the TTL cutoff (some rows expired, some live) must
/// never be dropped by the segment-level purge — only compaction may
/// rewrite it. The purge reports zero and the files stay.
#[test]
fn ttl_purge_keeps_partially_expired_segments() {
    let temp_dir = TempDir::new().expect("temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("engine");
    let mut session = Session::new("root".to_string(), "main".to_string());

    engine
        .execute(
            &mut session,
            "CREATE TABLE mixed_ticks (
                ts TIMESTAMP,
                value DOUBLE
            ) TTL (ts, 3600)",
        )
        .expect("create mixed_ticks");

    let now = chrono::Utc::now().timestamp_micros();
    engine
        .execute(
            &mut session,
            &format!(
                "INSERT INTO mixed_ticks VALUES ({}, 1.0), ({}, 2.0)",
                now - 3_600 * 1_000_000 - 60_000_000, // long expired
                now + 3_600 * 1_000_000,              // live
            ),
        )
        .expect("insert mixed");
    engine
        .execute(&mut session, "FLUSH TABLE mixed_ticks")
        .expect("flush");
    engine
        .segment_mgr()
        .seal_table_active("main", "mixed_ticks")
        .expect("seal");

    let reclaimed = engine.run_ttl_partition_purge().expect("purge");
    assert_eq!(reclaimed, 0, "partially-expired segment must be kept");

    let table_dir = temp_dir.path().join("main").join("mixed_ticks");
    let segments = taibai::storage::segment_manager::SegmentManager::discover_segments(&table_dir).expect("discover");
    assert_eq!(segments.len(), 1, "segment file must survive: {segments:?}");
    assert_eq!(
        total_physical_rows(temp_dir.path(), &engine, "main", "mixed_ticks"),
        2,
        "rows stay physical until compaction rewrites them"
    );

    let count = engine
        .execute(&mut session, "SELECT COUNT(*) FROM mixed_ticks")
        .expect("count");
    assert_eq!(scalar_i64(count), 1, "only the live row is visible");

    engine.segment_mgr().shutdown();
}
