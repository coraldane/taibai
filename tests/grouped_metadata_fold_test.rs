//! GROUP BY answered by the metadata folding engine.
//!
//! A day-partitioned table guarantees one distinct partition-key value per
//! segment, so `GROUP BY <partition key>` collapses to a footer walk with
//! zero decoded rows; split units (mixed NULLs, drifted keys, unsorted
//! month-partitioned data) decode only the key + argument columns. These
//! tests pin the observable contract: the folded result equals both the
//! per-day `COUNT(*)` answers and the regular HashAggregate pipeline — the
//! latter forced through the `HAVING COUNT(*) >= 0` trick, which declines
//! the folded operator (HAVING is unsupported there) while keeping every
//! group. The fast path being hit at all is pinned by the in-crate unit
//! tests asserting `compute_folded_aggregate` returns `Some`.

use std::collections::BTreeMap;

use tempfile::TempDir;

use taibai::net::session::Session;
use taibai::sql::execution::engine::{Engine, ExecResult};
use taibai::types::scalar::ScalarValue;

fn create_engine() -> (TempDir, Engine, Session) {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("failed to create engine");
    let session = Session::new("root".to_string(), "main".to_string());
    (temp_dir, engine, session)
}

fn exec(engine: &Engine, session: &mut Session, sql: &str) -> ExecResult {
    match engine.execute(session, sql) {
        Ok(result) => result,
        Err(error) => panic!("'{sql}' failed: {error}"),
    }
}

/// Rows of a two-column grouped result as (key, value) pairs keyed by the
/// group key's display form, so folded / fallback order differences do not
/// affect the comparison.
fn grouped_i64_rows(engine: &Engine, session: &mut Session, sql: &str) -> BTreeMap<i64, i64> {
    let result = exec(engine, session, sql);
    let ExecResult::Query { batches, .. } = result else {
        panic!("'{sql}' expected query result");
    };
    let mut rows = BTreeMap::new();
    for batch in &batches {
        for row in 0..batch.num_rows() {
            let key = match batch.scalar(row, 0) {
                Some(ScalarValue::Int64(Some(v))) => v,
                Some(ScalarValue::Int32(Some(v))) => v as i64,
                other => panic!("'{sql}' expected integer group key, got {other:?}"),
            };
            let value = match batch.scalar(row, 1) {
                Some(ScalarValue::Int64(Some(v))) => v,
                Some(ScalarValue::Int32(Some(v))) => v as i64,
                other => panic!("'{sql}' expected integer aggregate, got {other:?}"),
            };
            rows.insert(key, value);
        }
    }
    rows
}

fn count_where(engine: &Engine, session: &mut Session, table: &str, day: i64) -> i64 {
    let result = exec(
        engine,
        session,
        &format!("SELECT COUNT(*) FROM {table} WHERE deal_date = {day}"),
    );
    let ExecResult::Query { batches, .. } = result else {
        panic!("expected query result");
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

fn create_day_table(engine: &Engine, session: &mut Session) {
    exec(
        engine,
        session,
        "CREATE TABLE ticks (
            security_id VARCHAR,
            deal_date INT64,
            price INT64,
            PRIMARY KEY (security_id, deal_date)
        ) PARTITION BY DAY(deal_date)",
    );
}

fn insert_days(
    engine: &Engine,
    session: &mut Session,
    per_day: &[(i64, i64)], // (day, rows)
    next_id: &mut i64,
) {
    let mut sql = String::from("INSERT INTO ticks (security_id, deal_date, price) VALUES ");
    let mut first = true;
    for &(day, rows) in per_day {
        for row in 0..rows {
            if !first {
                sql.push_str(", ");
            }
            first = false;
            *next_id += 1;
            sql.push_str(&format!("('s{}', {}, {})", next_id, day, row + 1));
        }
    }
    exec(engine, session, &sql);
}

/// GROUP BY the day partition key matches the per-day COUNT(*) answers and
/// the regular aggregate pipeline (HAVING-forced fallback) on a fully
/// flushed table.
#[test]
fn group_by_partition_key_matches_per_day_counts() {
    let (_temp_dir, engine, mut session) = create_engine();
    create_day_table(&engine, &mut session);
    let mut id = 0i64;
    insert_days(
        &engine,
        &mut session,
        &[(20240101, 12), (20240102, 7), (20240103, 20)],
        &mut id,
    );
    exec(&engine, &mut session, "FLUSH TABLE ticks");

    let folded = grouped_i64_rows(
        &engine,
        &mut session,
        "SELECT deal_date, COUNT(*) FROM ticks GROUP BY deal_date",
    );
    let fallback = grouped_i64_rows(
        &engine,
        &mut session,
        "SELECT deal_date, COUNT(*) FROM ticks GROUP BY deal_date HAVING COUNT(*) >= 0",
    );
    assert_eq!(folded, fallback, "folded and pipeline paths must agree");
    let mut expected = BTreeMap::new();
    for &(day, rows) in &[(20240101i64, 12i64), (20240102, 7), (20240103, 20)] {
        let per_day = count_where(&engine, &mut session, "ticks", day);
        assert_eq!(per_day, rows, "per-day count for {day}");
        expected.insert(day, rows);
    }
    assert_eq!(folded, expected);
    engine.segment_mgr().shutdown();
}

/// Deletes in flushed segments plus unflushed memtable rows must both fold
/// correctly into their day's group.
#[test]
fn group_by_partition_key_folds_deletes_and_memtable_rows() {
    let (_temp_dir, engine, mut session) = create_engine();
    create_day_table(&engine, &mut session);
    let mut id = 0i64;
    insert_days(
        &engine,
        &mut session,
        &[(20240101, 10), (20240102, 10), (20240103, 10)],
        &mut id,
    );
    exec(&engine, &mut session, "FLUSH TABLE ticks");
    exec(
        &engine,
        &mut session,
        "DELETE FROM ticks WHERE deal_date = 20240102 AND security_id IN ('s11', 's12', 's13', 's14', 's15')",
    );
    // Unflushed rows for a brand-new day and for an existing day.
    insert_days(
        &engine,
        &mut session,
        &[(20240104, 6), (20240101, 3)],
        &mut id,
    );

    let folded = grouped_i64_rows(
        &engine,
        &mut session,
        "SELECT deal_date, COUNT(*) FROM ticks GROUP BY deal_date",
    );
    let fallback = grouped_i64_rows(
        &engine,
        &mut session,
        "SELECT deal_date, COUNT(*) FROM ticks GROUP BY deal_date HAVING COUNT(*) >= 0",
    );
    assert_eq!(folded, fallback);
    let expected: BTreeMap<i64, i64> =
        BTreeMap::from([(20240101, 13), (20240102, 5), (20240103, 10), (20240104, 6)]);
    assert_eq!(folded, expected);
    engine.segment_mgr().shutdown();
}

/// SUM / MIN / MAX per group fold from footer col sums and zone maps.
#[test]
fn grouped_sum_min_max_per_day() {
    let (_temp_dir, engine, mut session) = create_engine();
    create_day_table(&engine, &mut session);
    let mut id = 0i64;
    insert_days(
        &engine,
        &mut session,
        &[(20240101, 10), (20240102, 5)],
        &mut id,
    );
    exec(&engine, &mut session, "FLUSH TABLE ticks");

    let folded_sum = grouped_i64_rows(
        &engine,
        &mut session,
        "SELECT deal_date, SUM(price) FROM ticks GROUP BY deal_date",
    );
    let fallback_sum = grouped_i64_rows(
        &engine,
        &mut session,
        "SELECT deal_date, SUM(price) FROM ticks GROUP BY deal_date HAVING SUM(price) >= 0",
    );
    assert_eq!(folded_sum, fallback_sum);
    // prices are 1..=rows per day.
    assert_eq!(
        folded_sum,
        BTreeMap::from([(20240101i64, 55i64), (20240102, 15)])
    );

    let folded_min = grouped_i64_rows(
        &engine,
        &mut session,
        "SELECT deal_date, MIN(price) FROM ticks GROUP BY deal_date",
    );
    assert_eq!(
        folded_min,
        BTreeMap::from([(20240101i64, 1i64), (20240102, 1)])
    );
    let folded_max = grouped_i64_rows(
        &engine,
        &mut session,
        "SELECT deal_date, MAX(price) FROM ticks GROUP BY deal_date",
    );
    assert_eq!(
        folded_max,
        BTreeMap::from([(20240101i64, 10i64), (20240102, 5)])
    );
    engine.segment_mgr().shutdown();
}

/// NULL partition keys route to the unpartitioned bucket and must form one
/// NULL group. The NULL key column is VARCHAR-typed NULL here, so the count
/// comparison uses row counts instead of the i64 helper.
#[test]
fn null_partition_keys_form_null_group() {
    let (_temp_dir, engine, mut session) = create_engine();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE t (id INT64, deal_date INT64) PARTITION BY DAY(deal_date)",
    );
    exec(
        &engine,
        &mut session,
        "INSERT INTO t (id, deal_date) VALUES (1, 20240101), (2, 20240101), (3, NULL), (4, NULL), (5, NULL)",
    );
    exec(&engine, &mut session, "FLUSH TABLE t");

    let result = exec(
        &engine,
        &mut session,
        "SELECT deal_date, COUNT(*) FROM t GROUP BY deal_date",
    );
    let ExecResult::Query { batches, .. } = result else {
        panic!("expected query result");
    };
    let mut groups: Vec<(Option<i64>, i64)> = Vec::new();
    for batch in &batches {
        for row in 0..batch.num_rows() {
            let key = match batch.scalar(row, 0) {
                Some(ScalarValue::Int64(Some(v))) => Some(v),
                Some(ScalarValue::Int64(None)) => None,
                other => panic!("expected INT64 group key, got {other:?}"),
            };
            let count = match batch.scalar(row, 1) {
                Some(ScalarValue::Int64(Some(v))) => v,
                other => panic!("expected INT64 count, got {other:?}"),
            };
            groups.push((key, count));
        }
    }
    groups.sort();
    assert_eq!(
        groups,
        vec![(None, 3), (Some(20240101), 2)],
        "NULL keys collapse into one NULL group"
    );
    engine.segment_mgr().shutdown();
}

/// A WHERE range on the group key prunes to the surviving days.
#[test]
fn group_by_with_range_predicate() {
    let (_temp_dir, engine, mut session) = create_engine();
    create_day_table(&engine, &mut session);
    let mut id = 0i64;
    insert_days(
        &engine,
        &mut session,
        &[(20240101, 8), (20240102, 9), (20240103, 4)],
        &mut id,
    );
    exec(&engine, &mut session, "FLUSH TABLE ticks");

    let folded = grouped_i64_rows(
        &engine,
        &mut session,
        "SELECT deal_date, COUNT(*) FROM ticks WHERE deal_date >= 20240102 GROUP BY deal_date",
    );
    let fallback = grouped_i64_rows(
        &engine,
        &mut session,
        "SELECT deal_date, COUNT(*) FROM ticks WHERE deal_date >= 20240102 GROUP BY deal_date HAVING COUNT(*) >= 0",
    );
    assert_eq!(folded, fallback);
    assert_eq!(folded, BTreeMap::from([(20240102i64, 9i64), (20240103, 4)]));
    engine.segment_mgr().shutdown();
}

/// RENAME on the group key column: the drifted footer resolves by previous
/// name and the folded path stays exact.
#[test]
fn group_by_survives_key_column_rename() {
    let (_temp_dir, engine, mut session) = create_engine();
    create_day_table(&engine, &mut session);
    let mut id = 0i64;
    insert_days(
        &engine,
        &mut session,
        &[(20240101, 6), (20240102, 9)],
        &mut id,
    );
    exec(&engine, &mut session, "FLUSH TABLE ticks");
    exec(
        &engine,
        &mut session,
        "ALTER TABLE ticks RENAME COLUMN deal_date TO trade_date",
    );

    let folded = grouped_i64_rows(
        &engine,
        &mut session,
        "SELECT trade_date, COUNT(*) FROM ticks GROUP BY trade_date",
    );
    let fallback = grouped_i64_rows(
        &engine,
        &mut session,
        "SELECT trade_date, COUNT(*) FROM ticks GROUP BY trade_date HAVING COUNT(*) >= 0",
    );
    assert_eq!(folded, fallback);
    assert_eq!(folded, BTreeMap::from([(20240101i64, 6i64), (20240102, 9)]));
    engine.segment_mgr().shutdown();
}

/// GROUP BY a column added after the segments were written (constant
/// DEFAULT): the drifted segments collapse into the default's group, and
/// post-ALTER rows fold into their real groups.
#[test]
fn group_by_added_constant_default_column() {
    let (_temp_dir, engine, mut session) = create_engine();
    create_day_table(&engine, &mut session);
    let mut id = 0i64;
    insert_days(&engine, &mut session, &[(20240101, 6)], &mut id);
    exec(&engine, &mut session, "FLUSH TABLE ticks");
    exec(
        &engine,
        &mut session,
        "ALTER TABLE ticks ADD COLUMN tag INT64 DEFAULT 5",
    );
    exec(
        &engine,
        &mut session,
        "INSERT INTO ticks (security_id, deal_date, price, tag) VALUES ('x', 20240102, 1, 7)",
    );

    let folded = grouped_i64_rows(
        &engine,
        &mut session,
        "SELECT tag, COUNT(*) FROM ticks GROUP BY tag",
    );
    let fallback = grouped_i64_rows(
        &engine,
        &mut session,
        "SELECT tag, COUNT(*) FROM ticks GROUP BY tag HAVING COUNT(*) >= 0",
    );
    assert_eq!(folded, fallback);
    assert_eq!(folded, BTreeMap::from([(5i64, 6i64), (7, 1)]));
    engine.segment_mgr().shutdown();
}

/// Month partitioning keeps many days per segment: the engine falls to
/// row-group / data folding and must still match the pipeline exactly.
#[test]
fn month_partition_group_by_day_stays_exact() {
    let (_temp_dir, engine, mut session) = create_engine();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE m (
            id INT64,
            deal_date INT64,
            PRIMARY KEY (id, deal_date)
        ) PARTITION BY MONTH(deal_date)",
    );
    // Deliberately interleaved days inside one month partition: row-group
    // zone maps stay multi-valued, forcing per-row data folds.
    let mut sql = String::from("INSERT INTO m (id, deal_date) VALUES ");
    let mut id = 0i64;
    let mut first = true;
    for round in 0..6i64 {
        for day in [20240101i64, 20240115, 20240131] {
            for _ in 0..3 {
                if !first {
                    sql.push_str(", ");
                }
                first = false;
                id += 1;
                sql.push_str(&format!("({}, {})", id, day + round));
            }
        }
    }
    exec(&engine, &mut session, &sql);
    exec(&engine, &mut session, "FLUSH TABLE m");

    let folded = grouped_i64_rows(
        &engine,
        &mut session,
        "SELECT deal_date, COUNT(*) FROM m GROUP BY deal_date",
    );
    let fallback = grouped_i64_rows(
        &engine,
        &mut session,
        "SELECT deal_date, COUNT(*) FROM m GROUP BY deal_date HAVING COUNT(*) >= 0",
    );
    assert_eq!(folded, fallback);
    // day+round for round in 0..6, 3 rows each per base day.
    let mut expected = BTreeMap::new();
    for round in 0..6i64 {
        for day in [20240101i64, 20240115, 20240131] {
            *expected.entry(day + round).or_insert(0i64) += 3;
        }
    }
    assert_eq!(folded, expected);
    engine.segment_mgr().shutdown();
}

/// A multi-column GROUP BY key folds per row (unit zone maps are rarely
/// single-valued on both columns); the result must match the pipeline
/// row-for-row.
#[test]
fn multi_column_group_by_stays_exact() {
    let (_temp_dir, engine, mut session) = create_engine();
    create_day_table(&engine, &mut session);
    let mut id = 0i64;
    insert_days(
        &engine,
        &mut session,
        &[(20240101, 6), (20240102, 6)],
        &mut id,
    );
    exec(&engine, &mut session, "FLUSH TABLE ticks");

    let collect = |session: &mut Session, sql: &str| -> Vec<String> {
        let result = exec(&engine, session, sql);
        let ExecResult::Query { batches, .. } = result else {
            panic!("expected query result");
        };
        let mut rows = Vec::new();
        for batch in &batches {
            for row in 0..batch.num_rows() {
                let day = batch.scalar(row, 0).unwrap_or(ScalarValue::Int64(None));
                let sec = batch.scalar(row, 1).unwrap_or(ScalarValue::Int64(None));
                let count = batch.scalar(row, 2).unwrap_or(ScalarValue::Int64(None));
                rows.push(format!("{day:?}|{sec:?}|{count:?}"));
            }
        }
        rows.sort();
        rows
    };
    let folded = collect(
        &mut session,
        "SELECT deal_date, security_id, COUNT(*) FROM ticks GROUP BY deal_date, security_id",
    );
    let fallback = collect(
        &mut session,
        "SELECT deal_date, security_id, COUNT(*) FROM ticks GROUP BY deal_date, security_id HAVING COUNT(*) >= 0",
    );
    assert_eq!(folded, fallback);
    assert_eq!(
        folded.len(),
        12,
        "unique security ids: one group per row, 12 rows"
    );
    assert!(
        folded.iter().all(|row| row.ends_with("|Int64(Some(1))")),
        "every group holds exactly one row: {folded:?}"
    );
    engine.segment_mgr().shutdown();
}
