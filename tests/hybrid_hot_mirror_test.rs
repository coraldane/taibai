use tempfile::TempDir;

use taibai::net::session::Session;
use taibai::sql::execution::engine::{Engine, ExecResult};
use taibai::types::scalar::ScalarValue;

/// Collect the `ts` (INT32) payload column as a sorted multiset so duplicate
/// rows (mirror leg overlapping the segment/memtable legs) are caught.
fn collect_ts_values(result: ExecResult) -> Vec<i32> {
    let ExecResult::Query { batches, .. } = result else {
        panic!("expected query result, got {:?}", result);
    };
    let mut out = Vec::new();
    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            match batch.scalar(row_idx, 0) {
                Some(ScalarValue::Int32(Some(v))) => out.push(v),
                other => panic!("expected int32 ts, got {:?}", other),
            }
        }
    }
    out.sort();
    out
}

fn collect_count(result: ExecResult) -> i64 {
    let ExecResult::Query { batches, .. } = result else {
        panic!("expected query result, got {:?}", result);
    };
    batches
        .first()
        .and_then(|batch| batch.scalar(0, 0))
        .and_then(|value| match value {
            ScalarValue::Int64(Some(v)) => Some(v),
            _ => None,
        })
        .expect("count(*) result")
}

fn explain_text(result: ExecResult) -> String {
    let ExecResult::Query { batches, .. } = result else {
        panic!("expected query result, got {:?}", result);
    };
    let mut out = String::new();
    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            if let Some(ScalarValue::VarChar(Some(line))) = batch.scalar(row_idx, 0) {
                out.push_str(&line);
                out.push('\n');
            }
        }
    }
    out
}

/// Month-partitioned hot table mirroring the production `stock_minute`
/// shape: the partition (a whole month) can never be fully resident after a
/// close archives its earlier days, so intraday point lookups need the
/// hybrid partial-residency path.
fn create_table(engine: &Engine, session: &mut Session, table: &str) {
    engine
        .execute(
            session,
            &format!(
                "CREATE TABLE {table} (
                    security_id VARCHAR,
                    deal_date INT32,
                    deal_time INT32,
                    gmt_create TIMESTAMP,
                    ts INT32
                ) CLUSTER BY (security_id, deal_time)
                  HOT BY (gmt_create) INDEX (security_id) CLOSE AT '23:59'
                  PARTITION BY MONTH(deal_date)"
            ),
        )
        .expect("create month-partitioned hot table");
}

/// Deterministic engine for these tests: the background auto-compaction
/// thread is disabled because merging archived segments with post-close ones
/// mid-test would nondeterministically change which segments predate the
/// close cutoff.
fn new_engine(temp_dir: &TempDir, hot_warmup_enabled: bool) -> Engine {
    Engine::new_with_storage_config_and_auto_compact(
        temp_dir.path(),
        "main",
        taibai::storage::segment_manager::SegmentConfig::default(),
        taibai::sql::execution::AutoCompactConfig {
            enabled: false,
            ..taibai::sql::execution::AutoCompactConfig::default()
        },
        64 * 1024 * 1024,
        false,
        taibai::common::config::PubSubSettings::default(),
        hot_warmup_enabled,
    )
    .expect("failed to create engine")
}

/// Build the shared fixture: pre-close day 20260916 flushed and archived by a
/// close-of-day, then post-close day 20260917 written through auto-commit
/// inserts (mirrored).
fn run_until_post_close(table: &str) -> (TempDir, Engine, Session) {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = new_engine(&temp_dir, true);
    let mut session = Session::new("root".to_string(), "main".to_string());

    create_table(&engine, &mut session, table);

    // Pre-close day: two securities on 20260916, persisted to a segment.
    engine
        .execute(
            &mut session,
            &format!(
                "INSERT INTO {table} VALUES
                    ('A', 20260916, 930, 1782711000000000, 1),
                    ('B', 20260916, 930, 1782711000000001, 2)"
            ),
        )
        .expect("insert pre-close rows");
    engine
        .execute(&mut session, &format!("FLUSH TABLE {table}"))
        .expect("flush pre-close rows");

    // Archive the pre-close day: seals segments, empties the resident mirror
    // and records hot_mirror_covers_from = 20260916.
    engine
        .close_of_day_flush_table("main", table)
        .expect("close-of-day flush");

    // Post-close day: an intraday row for A (mirrored, unflushed).
    engine
        .execute(
            &mut session,
            &format!("INSERT INTO {table} VALUES ('A', 20260917, 931, 1782797460000000, 10)"),
        )
        .expect("insert post-close row");

    (temp_dir, engine, session)
}

#[test]
fn hybrid_scan_serves_post_close_day_on_month_partition() {
    let (_temp_dir, engine, mut session) = run_until_post_close("hyb_minute");

    // The month partition holds archived rows, so it is not fully resident.
    let store = engine
        .hot_store_manager()
        .get("main", "hyb_minute")
        .expect("post-close writes should rebuild a hot store");
    assert!(
        !store.is_resident_for_partition(Some("202609")),
        "a month partition with archived days must not be marked resident"
    );

    // The post-close day lookup must be answered by the hybrid mirror leg.
    let plan = explain_text(
        engine
            .execute(
                &mut session,
                "EXPLAIN ANALYZE SELECT ts FROM hyb_minute
                 WHERE deal_date = 20260917 AND security_id = 'A'",
            )
            .expect("explain hybrid lookup"),
    );
    assert!(
        plan.contains("hot_mirror"),
        "post-close day lookup should engage the hybrid mirror leg, got:\n{}",
        plan
    );

    let rows = collect_ts_values(
        engine
            .execute(
                &mut session,
                "SELECT ts FROM hyb_minute WHERE deal_date = 20260917 AND security_id = 'A'",
            )
            .expect("post-close day lookup"),
    );
    assert_eq!(rows, vec![10]);

    // The archived day stays on the plain scan path (the lookup value does
    // not exceed the coverage bound).
    let rows = collect_ts_values(
        engine
            .execute(
                &mut session,
                "SELECT ts FROM hyb_minute WHERE deal_date = 20260916 AND security_id = 'A'",
            )
            .expect("archived day lookup"),
    );
    assert_eq!(rows, vec![1]);

    engine.segment_mgr().shutdown();
}

#[test]
fn hybrid_backfilled_old_rows_stay_out_of_the_mirror_leg() {
    let (_temp_dir, engine, mut session) = run_until_post_close("hyb_minute");

    // Backfill an old-dated row after the close: it is mirrored (post-close
    // write) but its deal_date is below the coverage bound.
    engine
        .execute(
            &mut session,
            "INSERT INTO hyb_minute VALUES ('C', 20260916, 931, 1782797460000001, 20)",
        )
        .expect("backfill old-dated row");

    // The post-close day lookup must not pick up the backfilled row.
    let rows = collect_ts_values(
        engine
            .execute(
                &mut session,
                "SELECT ts FROM hyb_minute WHERE deal_date = 20260917 AND security_id = 'A'",
            )
            .expect("post-close day lookup with backfill present"),
    );
    assert_eq!(rows, vec![10]);

    // The backfilled row remains queryable on the plain scan path.
    let rows = collect_ts_values(
        engine
            .execute(
                &mut session,
                "SELECT ts FROM hyb_minute WHERE deal_date = 20260916 AND security_id = 'C'",
            )
            .expect("backfilled row lookup"),
    );
    assert_eq!(rows, vec![20]);

    // Key-only lookup spans both legs: the mirror answers the post-close row,
    // the segment/memtable legs answer the archived row — exactly once each.
    let rows = collect_ts_values(
        engine
            .execute(
                &mut session,
                "SELECT ts FROM hyb_minute WHERE security_id = 'A'",
            )
            .expect("key-only lookup across the coverage boundary"),
    );
    assert_eq!(rows, vec![1, 10]);

    engine.segment_mgr().shutdown();
}

#[test]
fn hybrid_gate_closes_after_mirror_bypass() {
    let (_temp_dir, engine, mut session) = run_until_post_close("hyb_minute");

    // A committed transaction delta never passes through the mirror, which
    // must close the coverage gate for the partition.
    engine.execute(&mut session, "BEGIN").expect("begin txn");
    engine
        .execute(
            &mut session,
            "INSERT INTO hyb_minute VALUES ('A', 20260917, 932, 1782797520000000, 11)",
        )
        .expect("txn row on the post-close day");
    engine.execute(&mut session, "COMMIT").expect("commit txn");

    assert!(
        engine
            .batch_writer()
            .memtable_manager()
            .hot_unfed_rows("main", "hyb_minute", "202609")
            > 0,
        "committed txn rows must be accounted as unmirrored"
    );

    // Correctness first: both post-close rows are visible...
    let rows = collect_ts_values(
        engine
            .execute(
                &mut session,
                "SELECT ts FROM hyb_minute WHERE deal_date = 20260917 AND security_id = 'A'",
            )
            .expect("post-close lookup after bypass"),
    );
    assert_eq!(rows, vec![10, 11]);

    // ...through the plain scan only — the hybrid leg must be disengaged.
    let plan = explain_text(
        engine
            .execute(
                &mut session,
                "EXPLAIN ANALYZE SELECT ts FROM hyb_minute
                 WHERE deal_date = 20260917 AND security_id = 'A'",
            )
            .expect("explain after bypass"),
    );
    assert!(
        !plan.contains("hot_mirror"),
        "a bypassed mirror must not serve the hybrid leg, got:\n{}",
        plan
    );

    engine.segment_mgr().shutdown();
}

#[test]
fn hybrid_count_serves_post_close_rows() {
    let (_temp_dir, engine, mut session) = run_until_post_close("hyb_minute");

    let count = collect_count(
        engine
            .execute(
                &mut session,
                "SELECT count(*) FROM hyb_minute WHERE deal_date = 20260917 AND security_id = 'A'",
            )
            .expect("hybrid count on the post-close day"),
    );
    assert_eq!(count, 1);

    // One more unflushed row, then a flushed one, to span both legs.
    engine
        .execute(
            &mut session,
            "INSERT INTO hyb_minute VALUES ('A', 20260917, 932, 1782797520000000, 11)",
        )
        .expect("second post-close row");
    engine
        .execute(&mut session, "FLUSH TABLE hyb_minute")
        .expect("flush post-close rows");

    let count = collect_count(
        engine
            .execute(
                &mut session,
                "SELECT count(*) FROM hyb_minute WHERE deal_date = 20260917 AND security_id = 'A'",
            )
            .expect("hybrid count after flush"),
    );
    assert_eq!(count, 2);

    let count = collect_count(
        engine
            .execute(
                &mut session,
                "SELECT count(*) FROM hyb_minute WHERE deal_date = 20260916 AND security_id = 'A'",
            )
            .expect("plain count on the archived day"),
    );
    assert_eq!(count, 1);

    engine.segment_mgr().shutdown();
}

#[test]
fn restart_reestablishes_hybrid_coverage_for_month_partition() {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    {
        let mut engine = new_engine(&temp_dir, true);
        let mut session = Session::new("root".to_string(), "main".to_string());

        create_table(&engine, &mut session, "hyb_minute");
        engine
            .execute(
                &mut session,
                "INSERT INTO hyb_minute VALUES
                    ('A', 20260916, 930, 1782711000000000, 1),
                    ('B', 20260916, 930, 1782711000000001, 2)",
            )
            .expect("insert pre-close rows");
        engine
            .execute(&mut session, "FLUSH TABLE hyb_minute")
            .expect("flush pre-close rows");
        engine
            .close_of_day_flush_table("main", "hyb_minute")
            .expect("close-of-day flush");

        // Sleep so the post-close segment mtime clearly postdates the close
        // instant (mtime granularity is milliseconds).
        std::thread::sleep(std::time::Duration::from_millis(20));
        engine
            .execute(
                &mut session,
                "INSERT INTO hyb_minute VALUES
                    ('A', 20260917, 931, 1782797460000000, 10),
                    ('B', 20260917, 931, 1782797460000001, 20)",
            )
            .expect("insert post-close rows");
        engine
            .execute(&mut session, "FLUSH TABLE hyb_minute")
            .expect("flush post-close rows");
        engine.shutdown().expect("shutdown engine");
    }

    let engine = new_engine(&temp_dir, true);
    let mut session = Session::new("root".to_string(), "main".to_string());

    // After the restart the mirror was rebuilt from every segment written
    // after the close. Post-close writes append to the partition's most
    // recent segment file, so depending on file layout the table ends up
    // either fully resident (all segments loaded) or hybrid-covered; both
    // serve the post-close day lookup from memory without a full segment
    // scan.
    let store = engine
        .hot_store_manager()
        .get("main", "hyb_minute")
        .expect("post-close segments should rebuild a hot store");
    assert!(
        store.row_count() >= 2,
        "post-close rows must be resident after restart"
    );

    let plan = explain_text(
        engine
            .execute(
                &mut session,
                "EXPLAIN ANALYZE SELECT ts FROM hyb_minute
                 WHERE deal_date = 20260917 AND security_id = 'A'",
            )
            .expect("explain lookup after restart"),
    );
    assert!(
        !plan.contains("segments      :") || plan.contains("hot_mirror"),
        "post-close day lookup after restart must take a memory fast path, got:\n{}",
        plan
    );

    let rows = collect_ts_values(
        engine
            .execute(
                &mut session,
                "SELECT ts FROM hyb_minute WHERE deal_date = 20260917 AND security_id = 'A'",
            )
            .expect("post-close lookup after restart"),
    );
    assert_eq!(rows, vec![10]);

    // Archived rows remain queryable through the segment scan.
    let count = collect_count(
        engine
            .execute(
                &mut session,
                "SELECT count(*) FROM hyb_minute WHERE deal_date = 20260916",
            )
            .expect("archived rows after restart"),
    );
    assert_eq!(count, 2);

    engine.segment_mgr().shutdown();
}
