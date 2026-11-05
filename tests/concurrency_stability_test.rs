use std::sync::Arc;
use std::thread;

use tempfile::TempDir;

use taibai::net::session::Session;
use taibai::sql::execution::engine::{Engine, ExecResult};
use taibai::storage::core::memtable::MemTableState;
use taibai::storage::segment::SegmentReader;
use taibai::storage::segment_manager::SegmentManager;

fn session() -> Session {
    Session::new("root".to_string(), "main".to_string())
}

fn count_star(result: ExecResult) -> i64 {
    let ExecResult::Query { batches, .. } = result else {
        panic!("expected query result");
    };
    batches
        .first()
        .and_then(|batch| batch.scalar(0, 0))
        .and_then(|value| match value {
            taibai::types::scalar::ScalarValue::Int64(Some(v)) => Some(v),
            _ => None,
        })
        .expect("count(*) value")
}

fn first_column_values(result: ExecResult) -> Vec<String> {
    let ExecResult::Query { batches, .. } = result else {
        panic!("expected query result");
    };
    let mut values = Vec::new();
    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            if let Some(taibai::types::scalar::ScalarValue::VarChar(Some(value))) =
                batch.scalar(row_idx, 0)
            {
                values.push(value);
            }
        }
    }
    values
}

fn rows_affected(result: ExecResult) -> u64 {
    let ExecResult::Ok { rows_affected } = result else {
        panic!("expected ok result");
    };
    rows_affected
}

#[test]
fn concurrent_insert_and_select_on_active_segment_is_stable() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Arc::new(Engine::new(temp_dir.path(), "main").expect("create engine"));
    let mut setup = session();
    engine
        .execute(&mut setup, "CREATE TABLE t (id INT32, value INT32)")
        .expect("create table");

    let writer_engine = Arc::clone(&engine);
    let writer = thread::spawn(move || {
        let mut s = session();
        for i in 0..80 {
            writer_engine
                .execute(&mut s, &format!("INSERT INTO t VALUES ({}, {})", i, i * 10))
                .expect("insert row");
        }
    });

    let reader_engine = Arc::clone(&engine);
    let reader = thread::spawn(move || {
        let mut s = session();
        for _ in 0..120 {
            reader_engine
                .execute(&mut s, "SELECT id, value FROM t")
                .expect("select rows while inserting");
        }
    });

    writer.join().expect("writer thread panicked");
    reader.join().expect("reader thread panicked");
    engine.segment_mgr().shutdown();
}

#[test]
fn concurrent_insert_and_delete_footer_rewrite_is_stable() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Arc::new(Engine::new(temp_dir.path(), "main").expect("create engine"));
    let mut setup = session();
    engine
        .execute(&mut setup, "CREATE TABLE t (id INT32, value INT32)")
        .expect("create table");

    let writer_engine = Arc::clone(&engine);
    let writer = thread::spawn(move || {
        let mut s = session();
        for i in 0..60 {
            writer_engine
                .execute(&mut s, &format!("INSERT INTO t VALUES ({}, {})", i, i * 10))
                .expect("insert row");
        }
    });

    let deleter_engine = Arc::clone(&engine);
    let deleter = thread::spawn(move || {
        let mut s = session();
        for i in 0..30 {
            deleter_engine
                .execute(&mut s, &format!("DELETE FROM t WHERE id = {}", i))
                .expect("delete row while inserting");
        }
    });

    writer.join().expect("writer thread panicked");
    deleter.join().expect("deleter thread panicked");
    engine.segment_mgr().shutdown();
}

#[test]
fn concurrent_insert_and_update_table_rewrite_is_stable() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Arc::new(Engine::new(temp_dir.path(), "main").expect("create engine"));
    let mut setup = session();
    engine
        .execute(&mut setup, "CREATE TABLE t (id INT32, value INT32)")
        .expect("create table");
    engine
        .execute(&mut setup, "INSERT INTO t VALUES (0, 0)")
        .expect("seed table");

    let writer_engine = Arc::clone(&engine);
    let writer = thread::spawn(move || {
        let mut s = session();
        for i in 1..50 {
            writer_engine
                .execute(&mut s, &format!("INSERT INTO t VALUES ({}, {})", i, i * 10))
                .expect("insert row");
        }
    });

    let updater_engine = Arc::clone(&engine);
    let updater = thread::spawn(move || {
        let mut s = session();
        for _ in 0..10 {
            updater_engine
                .execute(&mut s, "UPDATE t SET value = 1")
                .expect("update table while inserting");
        }
    });

    writer.join().expect("writer thread panicked");
    updater.join().expect("updater thread panicked");
    engine.segment_mgr().shutdown();
}

#[test]
fn flushing_memtable_rows_remain_countable() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("create engine");
    let mut setup = session();
    engine
        .execute(
            &mut setup,
            "CREATE TABLE t_partitioned_metrics (
                id INT64 AUTO_INCREMENT PRIMARY KEY,
                entity_key VARCHAR NOT NULL,
                event_day INT32 NOT NULL,
                metric_value DECIMAL(12, 4)
            ) PARTITION BY YEAR(event_day) CLUSTER BY (entity_key, event_day)",
        )
        .expect("create table");
    std::fs::create_dir_all(temp_dir.path().join("main").join("t_partitioned_metrics"))
        .expect("create table data dir");
    engine
        .execute(
            &mut setup,
            "INSERT INTO t_partitioned_metrics (entity_key, event_day, metric_value) VALUES
                ('new_1', 20260629, 1.0000),
                ('new_2', 20260629, 2.0000),
                ('other', 20260630, 3.0000)",
        )
        .expect("insert rows");

    for memtable in engine
        .memtable_manager()
        .get_by_table("main", "t_partitioned_metrics")
    {
        memtable.set_state(MemTableState::Flushing);
    }

    let mut verifier = session();
    let count = engine
        .execute(
            &mut verifier,
            "SELECT COUNT(*) FROM t_partitioned_metrics WHERE event_day = 20260629",
        )
        .expect("count flushing rows");
    assert_eq!(count_star(count), 2);
    engine.segment_mgr().shutdown();
}

#[test]
fn flushing_memtable_rows_remain_selectable_by_point_lookup() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("create engine");
    let mut setup = session();
    engine
        .execute(
            &mut setup,
            "CREATE TABLE ticks (
                security_id VARCHAR NOT NULL,
                deal_date INT64 NOT NULL,
                price DOUBLE
            ) PARTITION BY DAY(deal_date) CLUSTER BY (security_id, deal_date)",
        )
        .expect("create table");
    engine
        .execute(
            &mut setup,
            "INSERT INTO ticks VALUES
                ('688256.SH', 20260709, 11.1),
                ('000001.SZ', 20260709, 12.2)",
        )
        .expect("insert rows");

    for memtable in engine.memtable_manager().get_by_table("main", "ticks") {
        memtable.set_state(MemTableState::Flushing);
    }

    let mut verifier = session();
    let rows = first_column_values(
        engine
            .execute(
                &mut verifier,
                "SELECT security_id FROM ticks
                 WHERE deal_date = 20260709 AND security_id = '688256.SH'",
            )
            .expect("select flushing row"),
    );
    assert_eq!(rows, vec!["688256.SH".to_string()]);
    engine.segment_mgr().shutdown();
}

#[test]
fn flushed_memtable_rows_remain_selectable_by_point_lookup() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("create engine");
    let mut setup = session();
    engine
        .execute(
            &mut setup,
            "CREATE TABLE ticks (
                security_id VARCHAR NOT NULL,
                deal_date INT64 NOT NULL,
                price DOUBLE
            ) PARTITION BY DAY(deal_date) CLUSTER BY (security_id, deal_date)",
        )
        .expect("create table");
    engine
        .execute(
            &mut setup,
            "INSERT INTO ticks VALUES
                ('688256.SH', 20260709, 11.1),
                ('000001.SZ', 20260709, 12.2)",
        )
        .expect("insert rows");
    engine
        .execute(&mut setup, "FLUSH TABLE ticks")
        .expect("flush table");
    let segments = SegmentManager::discover_segments(&temp_dir.path().join("main").join("ticks"))
        .expect("discover flushed segments");
    assert!(
        segments.iter().any(|path| path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with("p20260709_"))),
        "expected a 20260709 partition segment after flush, got {:?}",
        segments
    );
    let segment_rows = segments
        .iter()
        .map(|path| {
            SegmentReader::open(path)
                .expect("open flushed segment")
                .row_count()
        })
        .sum::<u64>();
    assert_eq!(segment_rows, 2);

    let mut verifier = session();
    let rows = first_column_values(
        engine
            .execute(
                &mut verifier,
                "SELECT security_id FROM ticks
                 WHERE deal_date = 20260709 AND security_id = '688256.SH'",
            )
            .expect("select flushed row"),
    );
    assert_eq!(rows, vec!["688256.SH".to_string()]);
    engine.segment_mgr().shutdown();
}

#[test]
fn concurrent_sql_insert_after_delete_same_partition_key_is_immediately_countable() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Arc::new(Engine::new(temp_dir.path(), "main").expect("create engine"));
    let mut setup = session();
    engine
        .execute(
            &mut setup,
            "CREATE TABLE t_partitioned_metrics (
                id INT64 AUTO_INCREMENT PRIMARY KEY,
                gmt_create TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                entity_key VARCHAR NOT NULL,
                event_day INT32 NOT NULL,
                metric_value DECIMAL(12, 4)
            ) PARTITION BY YEAR(event_day) CLUSTER BY (entity_key, event_day)",
        )
        .expect("create table");
    engine
        .execute(
            &mut setup,
            "INSERT INTO t_partitioned_metrics (entity_key, event_day, metric_value) VALUES
                ('old_1', 20260629, 1.0000),
                ('old_2', 20260629, 2.0000),
                ('other', 20260630, 3.0000)",
        )
        .expect("insert old rows");
    engine
        .execute(&mut setup, "FLUSH TABLE t_partitioned_metrics")
        .expect("flush old rows");
    engine
        .execute(
            &mut setup,
            "DELETE FROM t_partitioned_metrics WHERE event_day = 20260629",
        )
        .expect("delete old partition-key rows");

    let mut writers = Vec::new();
    for worker in 0..4 {
        let writer_engine = Arc::clone(&engine);
        writers.push(thread::spawn(move || {
            let mut s = session();
            for row in 0..50 {
                let entity_key = format!("W{}_{:03}", worker, row);
                writer_engine
                    .execute(
                        &mut s,
                        &format!(
                            "INSERT INTO t_partitioned_metrics (entity_key, event_day, metric_value) \
                             VALUES ('{}', 20260629, {}.0000)",
                            entity_key,
                            worker * 100 + row
                        ),
                    )
                    .expect("insert replacement row");
            }
        }));
    }
    for writer in writers {
        writer.join().expect("writer thread panicked");
    }

    let mut verifier = session();
    let count = engine
        .execute(
            &mut verifier,
            "SELECT COUNT(*) FROM t_partitioned_metrics WHERE event_day = 20260629",
        )
        .expect("count replacement rows");
    assert_eq!(count_star(count), 200);
    engine.segment_mgr().shutdown();
}

#[test]
fn multi_value_sql_insert_after_delete_same_partition_key_is_immediately_countable() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("create engine");
    let mut setup = session();
    engine
        .execute(
            &mut setup,
            "CREATE TABLE t_partitioned_metrics (
                id INT64 AUTO_INCREMENT PRIMARY KEY,
                gmt_create TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                entity_key VARCHAR NOT NULL,
                source_day INT32,
                event_day INT32 NOT NULL,
                metric_value DECIMAL(12, 4)
            ) PARTITION BY YEAR(event_day) CLUSTER BY (entity_key, event_day)",
        )
        .expect("create table");
    engine
        .execute(
            &mut setup,
            "INSERT INTO t_partitioned_metrics (entity_key, source_day, event_day, metric_value) VALUES
                ('old_1', 20260629, 20260629, 1.0000),
                ('old_2', 20260629, 20260629, 2.0000),
                ('other', 20260630, 20260630, 3.0000)",
        )
        .expect("insert old rows");
    engine
        .execute(&mut setup, "FLUSH TABLE t_partitioned_metrics")
        .expect("flush old rows");
    assert_eq!(
        rows_affected(
            engine
                .execute(
                    &mut setup,
                    "DELETE FROM t_partitioned_metrics WHERE event_day = 20260629"
                )
                .expect("delete old partition-key rows")
        ),
        2
    );

    let mut sql = String::from(
        "INSERT INTO t_partitioned_metrics (entity_key, source_day, event_day, metric_value) VALUES ",
    );
    for row in 0..5189 {
        if row > 0 {
            sql.push(',');
        }
        sql.push_str(&format!("('K{:06}',20260629,20260629,{}.0000)", row, row));
    }

    assert_eq!(
        rows_affected(
            engine
                .execute(&mut setup, &sql)
                .expect("insert replacement rows")
        ),
        5189
    );

    let mut verifier = session();
    let count = engine
        .execute(
            &mut verifier,
            "SELECT COUNT(*) FROM t_partitioned_metrics WHERE event_day = 20260629",
        )
        .expect("count replacement rows");
    assert_eq!(count_star(count), 5189);
    engine.segment_mgr().shutdown();
}

#[test]
fn recovered_segment_insert_after_delete_same_partition_key_is_immediately_countable() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    {
        let engine = Engine::new(temp_dir.path(), "main").expect("create engine");
        let mut setup = session();
        engine
            .execute(
                &mut setup,
                "CREATE TABLE t_partitioned_metrics (
                    id INT64 AUTO_INCREMENT PRIMARY KEY,
                    gmt_create TIMESTAMP,
                    gmt_modified TIMESTAMP,
                    entity_key VARCHAR NOT NULL,
                    event_day INT32 NOT NULL,
                    metric_a DECIMAL(12, 4),
                    metric_b DECIMAL(12, 4),
                    metric_c DECIMAL(12, 4),
                    metric_d DECIMAL(12, 4),
                    metric_e DECIMAL(12, 4),
                    metric_f DECIMAL(12, 4),
                    metric_g INT64,
                    metric_h DECIMAL(20, 4),
                    metric_i DECIMAL(18, 6),
                    metric_text VARCHAR,
                    metric_j DECIMAL(12, 4),
                    metric_k DECIMAL(12, 2)
                ) PARTITION BY YEAR(event_day) CLUSTER BY (entity_key, event_day)",
            )
            .expect("create table");

        let mut seed = String::from(
            "INSERT INTO t_partitioned_metrics (
                gmt_create, gmt_modified, entity_key, event_day, metric_a, metric_b, metric_c,
                metric_d, metric_e, metric_f, metric_g, metric_h, metric_i, metric_text,
                metric_j, metric_k
            ) VALUES ",
        );
        for row in 0..5189 {
            if row > 0 {
                seed.push(',');
            }
            seed.push_str(&format!(
                "(1782718238000000,1782718759000000,'K{:06}',20260629,10.0000,11.0000,9.0000,12.0000,8.0000,10.5000,{},1000.0000,1.000000,'10.50',10.0000,0.50)",
                row, row
            ));
        }
        engine.execute(&mut setup, &seed).expect("insert seed rows");
        engine
            .execute(&mut setup, "FLUSH TABLE t_partitioned_metrics")
            .expect("flush seed rows");
        engine.segment_mgr().shutdown();
    }

    let engine = Engine::new(temp_dir.path(), "main").expect("reopen engine");
    let mut delete_session = session();
    let before = engine
        .execute(
            &mut delete_session,
            "SELECT COUNT(*) FROM t_partitioned_metrics WHERE event_day = 20260629",
        )
        .expect("count seed rows");
    assert_eq!(count_star(before), 5189);
    assert_eq!(
        rows_affected(
            engine
                .execute(
                    &mut delete_session,
                    "DELETE FROM t_partitioned_metrics WHERE event_day = 20260629"
                )
                .expect("delete seed rows")
        ),
        5189
    );
    let after_delete = engine
        .execute(
            &mut delete_session,
            "SELECT COUNT(*) FROM t_partitioned_metrics WHERE event_day = 20260629",
        )
        .expect("count after delete");
    assert_eq!(count_star(after_delete), 0);

    let mut insert = String::from(
        "INSERT INTO t_partitioned_metrics (
            gmt_create, gmt_modified, entity_key, event_day, metric_a, metric_b, metric_c,
            metric_d, metric_e, metric_f, metric_g, metric_h, metric_i, metric_text,
            metric_j, metric_k
        ) VALUES ",
    );
    for row in 0..5189 {
        if row > 0 {
            insert.push(',');
        }
        insert.push_str(&format!(
            "(1782718238000000,1782718759000000,'N{:06}',20260629,10.0000,11.0000,9.0000,12.0000,8.0000,10.5000,{},1000.0000,1.000000,'10.50',10.0000,0.50)",
            row, row
        ));
    }

    let mut insert_session = session();
    assert_eq!(
        rows_affected(
            engine
                .execute(&mut insert_session, &insert)
                .expect("insert replacement rows")
        ),
        5189
    );

    engine
        .flush_all_buffers()
        .expect("simulate writer disconnect flush");

    let mut verifier = session();
    let count = engine
        .execute(
            &mut verifier,
            "SELECT COUNT(*) FROM t_partitioned_metrics WHERE event_day = 20260629",
        )
        .expect("count replacement rows");
    assert_eq!(count_star(count), 5189);
    engine.segment_mgr().shutdown();
}

fn int64_column_values(result: ExecResult) -> Vec<i64> {
    let ExecResult::Query { batches, .. } = result else {
        panic!("expected query result");
    };
    let mut values = Vec::new();
    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            if let Some(taibai::types::scalar::ScalarValue::Int64(Some(value))) =
                batch.scalar(row_idx, 0)
            {
                values.push(value);
            }
        }
    }
    values
}

/// Regression for the snapshot scan conversion: the materialized-scan
/// executor holds the table flush read lock only while building its atomic
/// snapshot (segment list + memtable batches) and decodes afterwards,
/// protected by the segment lease. While a single writer moves rows into
/// new segments via repeated flushes, every concurrent scan must observe
/// the appended ids exactly once — a row lost between a memtable
/// retirement and the segment snapshot (or duplicated across the two legs)
/// breaks the contiguity assertion below.
#[test]
fn executor_scan_sees_each_row_exactly_once_across_concurrent_flushes() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Arc::new(Engine::new(temp_dir.path(), "main").expect("create engine"));
    let mut setup = session();
    engine
        .execute(
            &mut setup,
            "CREATE TABLE t_snapshot_scan (id INT64 AUTO_INCREMENT PRIMARY KEY, pad INT32)",
        )
        .expect("create table");
    engine
        .execute(&mut setup, "INSERT INTO t_snapshot_scan (pad) VALUES (1), (2), (3)")
        .expect("seed rows");
    engine
        .flush_table_buffers("main", "t_snapshot_scan")
        .expect("seed flush");

    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let writer_engine = Arc::clone(&engine);
    let writer_stop = Arc::clone(&stop);
    let writer = thread::spawn(move || {
        let mut s = session();
        let mut inserted = 0u64;
        while !writer_stop.load(std::sync::atomic::Ordering::Relaxed) {
            writer_engine
                .execute(&mut s, "INSERT INTO t_snapshot_scan (pad) VALUES (7)")
                .expect("insert row");
            inserted += 1;
            if inserted % 3 == 0 {
                writer_engine
                    .flush_table_buffers("main", "t_snapshot_scan")
                    .expect("flush memtable");
            }
        }
        inserted
    });

    let reader_engine = Arc::clone(&engine);
    let reader = thread::spawn(move || {
        let mut s = session();
        for _ in 0..300 {
            // The predicate defeats metadata folding, forcing the general
            // segment + memtable scan path this regression guards.
            let scan = reader_engine
                .execute(&mut s, "SELECT id FROM t_snapshot_scan WHERE id >= 0")
                .expect("scan while flushing");
            let ids = int64_column_values(scan);
            let mut sorted = ids.clone();
            sorted.sort_unstable();
            sorted.dedup();
            assert_eq!(
                sorted.len(),
                ids.len(),
                "a row appeared twice in one scan (segment/memtable leg overlap)"
            );
            if let Some(max) = ids.iter().max() {
                assert_eq!(
                    ids.len(),
                    *max as usize,
                    "visible ids must stay contiguous 1..=max across flush transitions"
                );
            }
        }
    });

    reader.join().expect("reader thread");
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let inserted = writer.join().expect("writer thread");

    let mut verifier = session();
    let final_scan = engine
        .execute(&mut verifier, "SELECT id FROM t_snapshot_scan WHERE id >= 0")
        .expect("final scan");
    let ids = int64_column_values(final_scan);
    assert_eq!(ids.len() as u64, inserted + 3, "final row count");
    engine.segment_mgr().shutdown();
}
