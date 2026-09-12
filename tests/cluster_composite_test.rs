//! End-to-end tests for composite cluster-key prefix pushdown (roadmap step
//! 3, P3.2): on a `CLUSTER BY (symbol, ts)` table, `symbol = ? AND ts`
//! range queries must return exact results via the composite binary search,
//! and multi-column `ORDER BY symbol, ts LIMIT n` must ride the cluster
//! ordering for early stop.

use tempfile::TempDir;

use taibai::net::session::Session;
use taibai::sql::execution::engine::{Engine, ExecResult};
use taibai::types::scalar::ScalarValue;

const SYMBOLS: usize = 3;
const TS_PER_SYMBOL: i64 = 6_000;
const INSERT_BATCH: i64 = 1_000;

fn setup_clustered_engine() -> (TempDir, Engine, Session) {
    let temp_dir = TempDir::new().expect("temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("engine");
    let mut session = Session::new("root".to_string(), "main".to_string());

    engine
        .execute(
            &mut session,
            "CREATE TABLE ticks (
                symbol VARCHAR,
                ts INT64,
                price DOUBLE
            ) CLUSTER BY (symbol, ts)",
        )
        .expect("create ticks");

    // Insert symbol-major so pre-OPTIMIZE data is NOT globally sorted; the
    // OPTIMIZE TABLE below establishes the (symbol, ts) cluster order that
    // the composite binary search relies on.
    let mut statement = String::with_capacity(64 * 1024);
    for symbol_idx in 0..SYMBOLS {
        let symbol = format!("S{symbol_idx:02}");
        let mut ts = 1i64;
        while ts <= TS_PER_SYMBOL {
            statement.clear();
            statement.push_str("INSERT INTO ticks VALUES ");
            let mut first = true;
            let end = (ts + INSERT_BATCH).min(TS_PER_SYMBOL + 1);
            while ts < end {
                if !first {
                    statement.push_str(", ");
                }
                first = false;
                statement.push_str(&format!("('{}', {}, {:.1})", symbol, ts, ts as f64 / 10.0));
                ts += 1;
            }
            engine.execute(&mut session, &statement).expect("insert");
        }
    }
    engine
        .execute(&mut session, "OPTIMIZE TABLE ticks")
        .expect("optimize");

    (temp_dir, engine, session)
}

fn query_rows(engine: &Engine, session: &mut Session, sql: &str) -> Vec<Vec<ScalarValue>> {
    let ExecResult::Query { batches, .. } = engine.execute(session, sql).expect("query ok") else {
        panic!("expected query result for: {sql}");
    };
    let mut rows = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            rows.push(
                (0..batch.num_columns())
                    .map(|c| batch.scalar(row, c).expect("column value"))
                    .collect(),
            );
        }
    }
    rows
}

#[test]
fn composite_range_pruning_returns_exact_rows_across_row_groups() {
    let (_dir, engine, mut session) = setup_clustered_engine();

    // Range straddles row-group boundaries (18,000 rows → 3 row groups of
    // 8,192): S02's ts 3000..3500 lives inside the second row group only.
    let rows = query_rows(
        &engine,
        &mut session,
        "SELECT COUNT(*), MIN(ts), MAX(ts) FROM ticks
         WHERE symbol = 'S02' AND ts >= 3000 AND ts <= 3500",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], ScalarValue::Int64(Some(501)));
    assert_eq!(rows[0][1], ScalarValue::Int64(Some(3000)));
    assert_eq!(rows[0][2], ScalarValue::Int64(Some(3500)));

    // Inclusive / exclusive boundary semantics survive the tuple search.
    let rows = query_rows(
        &engine,
        &mut session,
        "SELECT COUNT(*) FROM ticks
         WHERE symbol = 'S02' AND ts > 5999 AND ts <= 6000",
    );
    assert_eq!(rows[0][0], ScalarValue::Int64(Some(1)));

    // Whole-symbol equality run (prefix-only bounds).
    let rows = query_rows(
        &engine,
        &mut session,
        "SELECT COUNT(*), MIN(ts), MAX(ts) FROM ticks WHERE symbol = 'S01'",
    );
    assert_eq!(rows[0][0], ScalarValue::Int64(Some(TS_PER_SYMBOL)));
    assert_eq!(rows[0][1], ScalarValue::Int64(Some(1)));
    assert_eq!(rows[0][2], ScalarValue::Int64(Some(TS_PER_SYMBOL)));

    // A range that matches nothing.
    let rows = query_rows(
        &engine,
        &mut session,
        "SELECT COUNT(*) FROM ticks
         WHERE symbol = 'S02' AND ts >= 7000 AND ts <= 8000",
    );
    assert_eq!(rows[0][0], ScalarValue::Int64(Some(0)));
}

#[test]
fn multi_column_order_limit_rides_cluster_order() {
    let (_dir, engine, mut session) = setup_clustered_engine();

    // ORDER BY matches the leading cluster-key prefix: early stop reads only
    // the head of the sorted data, yet returns exactly the first rows.
    let rows = query_rows(
        &engine,
        &mut session,
        "SELECT symbol, ts FROM ticks ORDER BY symbol, ts LIMIT 4",
    );
    assert_eq!(rows.len(), 4);
    for (offset, row) in rows.iter().enumerate() {
        assert_eq!(row[0], ScalarValue::VarChar(Some("S00".into())));
        assert_eq!(row[1], ScalarValue::Int64(Some(offset as i64 + 1)));
    }

    // Reversed direction over the full key prefix.
    let rows = query_rows(
        &engine,
        &mut session,
        "SELECT symbol, ts FROM ticks ORDER BY symbol DESC, ts DESC LIMIT 3",
    );
    assert_eq!(rows.len(), 3);
    let last_symbol = format!("S{:02}", SYMBOLS - 1);
    for (offset, row) in rows.iter().enumerate() {
        assert_eq!(row[0], ScalarValue::VarChar(Some(last_symbol.clone())));
        assert_eq!(
            row[1],
            ScalarValue::Int64(Some(TS_PER_SYMBOL - offset as i64))
        );
    }

    // Equality-pinned prefix + descending order on the next key column.
    let rows = query_rows(
        &engine,
        &mut session,
        "SELECT symbol, ts FROM ticks
         WHERE symbol = 'S02' ORDER BY ts DESC LIMIT 2",
    );
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][1], ScalarValue::Int64(Some(TS_PER_SYMBOL)));
    assert_eq!(rows[1][1], ScalarValue::Int64(Some(TS_PER_SYMBOL - 1)));
}

#[test]
fn composite_range_results_match_full_scan_for_every_symbol() {
    let (_dir, engine, mut session) = setup_clustered_engine();

    for symbol_idx in 0..SYMBOLS {
        let symbol = format!("S{symbol_idx:02}");
        let rows = query_rows(
            &engine,
            &mut session,
            &format!(
                "SELECT COUNT(*), MIN(ts), MAX(ts) FROM ticks
                 WHERE symbol = '{symbol}' AND ts >= 1000 AND ts < 2000"
            ),
        );
        assert_eq!(rows[0][0], ScalarValue::Int64(Some(1000)));
        assert_eq!(rows[0][1], ScalarValue::Int64(Some(1000)));
        assert_eq!(rows[0][2], ScalarValue::Int64(Some(1999)));
    }
}
