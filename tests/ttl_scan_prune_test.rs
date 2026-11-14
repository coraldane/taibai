//! TTL visibility feeds zone-map pruning: a fully-expired segment is
//! skipped without opening (sidecar meta) or without reading any row group
//! (reader fallback), so `LIMIT 1` on a table whose history has expired no
//! longer degenerates into a full-table walk. The read-time TTL filter
//! stays the authority on which rows are visible.

use tempfile::TempDir;

use taibai::net::session::Session;
use taibai::sql::execution::engine::{Engine, ExecResult};
use taibai::types::scalar::ScalarValue;

fn micros(s: &str) -> i64 {
    chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
        .expect("datetime")
        .and_utc()
        .timestamp_micros()
}

fn collect_text(result: ExecResult) -> String {
    let ExecResult::Query { batches, .. } = result else {
        panic!("expected query result");
    };
    let mut out = String::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            if let ScalarValue::VarChar(Some(line)) = batch.scalar(row, 0).expect("value") {
                out.push_str(&line);
                out.push('\n');
            }
        }
    }
    out
}

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

/// Parse `  <label> : <value>`-style counters out of an EXPLAIN ANALYZE
/// report (segments / row_groups read / row_groups pruned).
fn analyze_counter(report: &str, label: &str) -> Option<u64> {
    for line in report.lines() {
        let Some((key, value)) = line.split_once(':') else {
            continue;
        };
        if !key.trim().starts_with(label) {
            continue;
        }
        // "row_groups" carries three numbers; callers wanting a specific one
        // pass the full prefix including the trailing word.
        return value
            .trim()
            .split(|c: char| !c.is_ascii_digit())
            .find(|part| !part.is_empty())
            .and_then(|part| part.parse().ok());
    }
    None
}

fn row_groups_field(report: &str, which: &str) -> u64 {
    let line = report
        .lines()
        .find(|l| l.trim_start().starts_with("row_groups"))
        .expect("row_groups line");
    // "row_groups    : A total, B read, C pruned (P%)"
    let value = line.split_once(':').expect("counter").1;
    let numbers: Vec<u64> = value
        .split(|c: char| !c.is_ascii_digit())
        .filter(|p| !p.is_empty())
        .filter_map(|p| p.parse().ok())
        .collect();
    let idx = match which {
        "total" => 0,
        "read" => 1,
        "pruned" => 2,
        other => panic!("unknown row_groups field {other}"),
    };
    numbers[idx]
}

#[test]
fn limit_scan_skips_fully_expired_segments() {
    let temp_dir = TempDir::new().expect("temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("engine");
    let mut session = Session::new("root".to_string(), "main".to_string());

    engine
        .execute(
            &mut session,
            "CREATE TABLE ticks (
                ts TIMESTAMP,
                value DOUBLE
            ) TTL (ts, 3600)",
        )
        .expect("create ticks");

    // One expired segment (2024, far past the one-hour TTL) sealed away,
    // then one live segment (2030).
    engine
        .execute(
            &mut session,
            &format!(
                "INSERT INTO ticks VALUES ({}, 1.0), ({}, 2.0)",
                micros("2024-01-15 10:00:00"),
                micros("2024-01-15 11:00:00")
            ),
        )
        .expect("insert old");
    engine
        .execute(&mut session, "FLUSH TABLE ticks")
        .expect("flush old");
    engine
        .segment_mgr()
        .seal_table_active("main", "ticks")
        .expect("seal old");

    engine
        .execute(
            &mut session,
            &format!(
                "INSERT INTO ticks VALUES ({}, 3.0), ({}, 4.0)",
                micros("2030-01-01 00:00:00"),
                micros("2030-01-01 00:00:01")
            ),
        )
        .expect("insert fresh");
    engine
        .execute(&mut session, "FLUSH TABLE ticks")
        .expect("flush fresh");
    engine
        .segment_mgr()
        .seal_table_active("main", "ticks")
        .expect("seal fresh");

    // Correctness first: the live rows are the only visible ones, and the
    // LIMIT walk must terminate on them without decoding the expired
    // segment's row groups.
    let explain = collect_text(
        engine
            .execute(&mut session, "EXPLAIN ANALYZE SELECT * FROM ticks LIMIT 1")
            .expect("explain"),
    );
    let read = row_groups_field(&explain, "read");
    let total = row_groups_field(&explain, "total");
    assert!(
        read < total,
        "expired row groups must not be read: {read} read of {total} total\n{explain}"
    );

    let count = engine
        .execute(&mut session, "SELECT COUNT(*) FROM ticks")
        .expect("count");
    assert_eq!(scalar_i64(count), 2);

    let limit_one = engine
        .execute(&mut session, "SELECT COUNT(*) FROM (SELECT * FROM ticks LIMIT 1) AS t")
        .expect("limit one");
    assert_eq!(scalar_i64(limit_one), 1);

    engine.segment_mgr().shutdown();
}

/// A row group straddling the TTL cutoff is Partial: pruning must keep it
/// and the read-time filter decides row by row. NULL TTL timestamps are
/// never visible, so an all-NULL row group may be pruned.
#[test]
fn boundary_row_group_is_kept_and_row_filtered() {
    let temp_dir = TempDir::new().expect("temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("engine");
    let mut session = Session::new("root".to_string(), "main".to_string());

    engine
        .execute(
            &mut session,
            "CREATE TABLE boundary (
                ts TIMESTAMP,
                value DOUBLE
            ) TTL (ts, 3600)",
        )
        .expect("create boundary");

    // All rows inside one segment/row group, with mixed visibility: the
    // zone map spans from long-expired to far-future, so the prune must
    // classify it Partial and read it.
    let now = chrono::Utc::now().timestamp_micros();
    engine
        .execute(
            &mut session,
            &format!(
                "INSERT INTO boundary VALUES \
                 ({}, 1.0), \
                 ({}, 2.0), \
                 ({}, 3.0), \
                 ({}, 4.0)",
                now - 3_600 * 1_000_000 - 60_000_000, // expired
                now - 1_800 * 1_000_000,              // live (half TTL)
                now,                                  // live
                now + 3_600 * 1_000_000,              // live
            ),
        )
        .expect("insert mixed");
    engine
        .execute(&mut session, "FLUSH TABLE boundary")
        .expect("flush");

    let count = engine
        .execute(&mut session, "SELECT COUNT(*) FROM boundary")
        .expect("count");
    assert_eq!(scalar_i64(count), 3, "expired row filtered, three live kept");

    let report = collect_text(
        engine
            .execute(&mut session, "EXPLAIN ANALYZE SELECT COUNT(*) FROM boundary")
            .expect("explain"),
    );
    let read = row_groups_field(&report, "read");
    assert!(read > 0, "straddling row group must be read\n{report}");
    let _ = analyze_counter(&report, "segments");

    engine.segment_mgr().shutdown();
}
