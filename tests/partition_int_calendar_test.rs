//! Integer temporal partitioning: one value, one interpretation.
//!
//! Calendar numbers (YYYYMMDD / YYYYMM) partition by calendar date, bare
//! integers inside the epoch magnitude windows (seconds >= 10^9, millis >=
//! 10^10, micros >= 10^13) partition by epoch time, and every other
//! non-null integer — an invalid date number like 20240132, a wall-clock
//! 930 — lands in the unpartitioned bucket exactly like NULL. The old
//! unconditional seconds fallback used to collapse all of those into
//! `p19700823`-style partitions (which the TTL purger would then treat as
//! half-century-old data and drop); these tests pin the disjoint-domain
//! rule end to end: directory layout, write/read consistency for every
//! value class, and no 1970 partitions ever.

use std::path::Path;

use tempfile::TempDir;

use taibai::net::session::Session;
use taibai::sql::execution::engine::{Engine, ExecResult};
use taibai::types::scalar::ScalarValue;

fn exec(engine: &Engine, session: &mut Session, sql: &str) -> ExecResult {
    match engine.execute(session, sql) {
        Ok(result) => result,
        Err(error) => panic!("'{sql}' failed: {error}"),
    }
}

fn count(engine: &Engine, session: &mut Session, sql: &str) -> i64 {
    let result = exec(engine, session, sql);
    let ExecResult::Query { batches, .. } = result else {
        panic!("'{sql}' expected query result");
    };
    let mut total = 0i64;
    for batch in &batches {
        for row in 0..batch.num_rows() {
            if let Some(ScalarValue::Int64(Some(v))) = batch.scalar(row, 0) {
                total += v;
            }
        }
    }
    total
}

fn segment_names(table_dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(table_dir)
        .expect("table dir")
        .filter_map(|entry| {
            let name = entry.expect("entry").file_name().into_string().ok()?;
            name.ends_with(".dat").then_some(name)
        })
        .collect();
    names.sort();
    names
}

#[test]
fn integer_partition_values_partition_by_disjoint_domains() {
    let temp_dir = TempDir::new().expect("temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("engine");
    let mut session = Session::new("root".to_string(), "main".to_string());

    exec(
        &engine,
        &mut session,
        "CREATE TABLE t (id INT64, deal_date INT64) PARTITION BY DAY(deal_date)",
    );
    exec(
        &engine,
        &mut session,
        "INSERT INTO t (id, deal_date) VALUES \
         (1, 20240101), (2, 20240101), (3, 20240131), \
         (4, 20240132), (5, 20240132), (6, 20240230), (7, 930), \
         (8, 1775040000000), (9, 1775040000000), (10, 1672531200), \
         (11, NULL)",
    );
    engine.flush_table_buffers("main", "t").expect("flush");

    let table_dir = temp_dir.path().join("main").join("t");
    let names = segment_names(&table_dir);

    // Calendar numbers own their partitions.
    assert!(
        names.iter().any(|name| name.starts_with("p20240101_")),
        "calendar day partition missing: {names:?}"
    );
    assert!(
        names.iter().any(|name| name.starts_with("p20240131_")),
        "month-end calendar partition missing: {names:?}"
    );
    // Epoch-window integers own their derived-date partitions.
    assert!(
        names.iter().any(|name| name.starts_with("p20260401_")),
        "epoch-millis partition (1775040000000 -> 2026-04-01) missing: {names:?}"
    );
    assert!(
        names.iter().any(|name| name.starts_with("p20230101_")),
        "epoch-seconds partition (1672531200 -> 2023-01-01) missing: {names:?}"
    );
    // Invalid date numbers and wall clocks share the unpartitioned bucket
    // with NULL (empty partition key, regular `seg_*.dat` files), never a
    // 1970 date and never another partition's segment.
    assert!(
        names.iter().any(|name| name.starts_with("seg_")),
        "unpartitioned bucket (regular segment) missing: {names:?}"
    );
    assert!(
        names.iter().all(|name| !name.starts_with("p1970")),
        "non-calendar integers must never collapse into 1970 partitions: {names:?}"
    );

    // Write/read consistency for every value class: eq predicates find
    // their rows regardless of which bucket they landed in.
    assert_eq!(
        count(
            &engine,
            &mut session,
            "SELECT COUNT(*) FROM t WHERE deal_date = 20240101"
        ),
        2
    );
    assert_eq!(
        count(
            &engine,
            &mut session,
            "SELECT COUNT(*) FROM t WHERE deal_date = 20240131"
        ),
        1
    );
    assert_eq!(
        count(
            &engine,
            &mut session,
            "SELECT COUNT(*) FROM t WHERE deal_date = 20240132"
        ),
        2
    );
    assert_eq!(
        count(
            &engine,
            &mut session,
            "SELECT COUNT(*) FROM t WHERE deal_date = 20240230"
        ),
        1
    );
    assert_eq!(
        count(
            &engine,
            &mut session,
            "SELECT COUNT(*) FROM t WHERE deal_date = 930"
        ),
        1
    );
    assert_eq!(
        count(
            &engine,
            &mut session,
            "SELECT COUNT(*) FROM t WHERE deal_date = 1775040000000"
        ),
        2
    );
    assert_eq!(
        count(
            &engine,
            &mut session,
            "SELECT COUNT(*) FROM t WHERE deal_date = 1672531200"
        ),
        1
    );
    assert_eq!(
        count(
            &engine,
            &mut session,
            "SELECT COUNT(*) FROM t WHERE deal_date IS NULL"
        ),
        1
    );
    assert_eq!(count(&engine, &mut session, "SELECT COUNT(*) FROM t"), 11);

    engine.segment_mgr().shutdown();
}

/// Range pruning over a calendar INT column: a valid bound prunes to its
/// period, an invalid bound (20240132) prunes nothing but still returns
/// exact results.
#[test]
fn range_pruning_over_integer_partition_column_stays_exact() {
    let temp_dir = TempDir::new().expect("temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("engine");
    let mut session = Session::new("root".to_string(), "main".to_string());

    exec(
        &engine,
        &mut session,
        "CREATE TABLE t (id INT64, deal_date INT64) PARTITION BY DAY(deal_date)",
    );
    exec(
        &engine,
        &mut session,
        "INSERT INTO t (id, deal_date) VALUES \
         (1, 20240101), (2, 20240102), (3, 20240103), (4, 20240132)",
    );
    engine.flush_table_buffers("main", "t").expect("flush");

    assert_eq!(
        count(
            &engine,
            &mut session,
            "SELECT COUNT(*) FROM t WHERE deal_date >= 20240102 AND deal_date <= 20240103"
        ),
        2,
        "valid bounds prune and count exactly"
    );
    assert_eq!(
        count(
            &engine,
            &mut session,
            "SELECT COUNT(*) FROM t WHERE deal_date > 20240131"
        ),
        1,
        "the invalid-date row is still visible above the bound"
    );

    engine.segment_mgr().shutdown();
}
