//! End-to-end tests for ASOF JOIN (roadmap step 3, P3.1): parser wiring,
//! partitioned time-key matching in the executor, both keyword orders, both
//! match directions, LEFT-preserving semantics, multi-key partitioning, and
//! loud failures for unsupported conditions.

use tempfile::TempDir;

use taibai::net::session::Session;
use taibai::sql::execution::engine::{Engine, ExecResult};
use taibai::types::scalar::ScalarValue;

fn setup_engine() -> (TempDir, Engine, Session) {
    let temp_dir = TempDir::new().expect("temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("engine");
    let session = Session::new("root".to_string(), "main".to_string());
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

fn setup_trades_quotes(engine: &Engine, session: &mut Session) {
    engine
        .execute(
            session,
            "CREATE TABLE quotes (symbol VARCHAR, ts INT64, price DOUBLE)",
        )
        .expect("create quotes");
    engine
        .execute(
            session,
            "INSERT INTO quotes VALUES
                ('AAPL', 100, 10.0),
                ('AAPL', 200, 11.0),
                ('AAPL', 300, 12.0),
                ('MSFT', 150, 20.0)",
        )
        .expect("insert quotes");
    engine
        .execute(
            session,
            "CREATE TABLE trades (symbol VARCHAR, ts INT64, qty INT64)",
        )
        .expect("create trades");
    engine
        .execute(
            session,
            "INSERT INTO trades VALUES
                ('AAPL', 50, 1),
                ('AAPL', 100, 2),
                ('AAPL', 250, 3),
                ('MSFT', 500, 4),
                ('GOOG', 100, 5)",
        )
        .expect("insert trades");
}

#[test]
fn asof_join_matches_most_recent_quote_at_or_before() {
    let (_dir, engine, mut session) = setup_engine();
    setup_trades_quotes(&engine, &mut session);

    let rows = query_rows(
        &engine,
        &mut session,
        "SELECT trades.ts, quotes.price
         FROM trades ASOF JOIN quotes
         ON trades.symbol = quotes.symbol AND trades.ts >= quotes.ts
         ORDER BY trades.qty",
    );

    // AAPL@50 has no quote at or before 50 and GOOG has no quotes at all:
    // both are dropped by the inner-flavored ASOF JOIN.
    assert_eq!(rows.len(), 3, "rows: {rows:?}");
    assert_eq!(rows[0][0], ScalarValue::Int64(Some(100)));
    assert_eq!(rows[0][1], ScalarValue::Float64(Some(10.0)));
    assert_eq!(rows[1][0], ScalarValue::Int64(Some(250)));
    assert_eq!(rows[1][1], ScalarValue::Float64(Some(11.0)));
    assert_eq!(rows[2][0], ScalarValue::Int64(Some(500)));
    assert_eq!(rows[2][1], ScalarValue::Float64(Some(20.0)));
}

#[test]
fn left_asof_join_preserves_unmatched_left_rows() {
    let (_dir, engine, mut session) = setup_engine();
    setup_trades_quotes(&engine, &mut session);

    let rows = query_rows(
        &engine,
        &mut session,
        "SELECT trades.ts, quotes.price
         FROM trades LEFT ASOF JOIN quotes
         ON trades.symbol = quotes.symbol AND trades.ts >= quotes.ts
         ORDER BY trades.qty",
    );

    assert_eq!(rows.len(), 5, "rows: {rows:?}");
    // AAPL@50: no quote at or before → NULL price.
    assert_eq!(rows[0][0], ScalarValue::Int64(Some(50)));
    assert_eq!(rows[0][1], ScalarValue::Float64(None));
    // AAPL@100 → quote@100.
    assert_eq!(rows[1][1], ScalarValue::Float64(Some(10.0)));
    // AAPL@250 → quote@200.
    assert_eq!(rows[2][1], ScalarValue::Float64(Some(11.0)));
    // MSFT@500 → quote@150.
    assert_eq!(rows[3][1], ScalarValue::Float64(Some(20.0)));
    // GOOG@100: symbol absent on the right → NULL price.
    assert_eq!(rows[4][0], ScalarValue::Int64(Some(100)));
    assert_eq!(rows[4][1], ScalarValue::Float64(None));
}

#[test]
fn asof_left_join_keyword_order_is_accepted() {
    let (_dir, engine, mut session) = setup_engine();
    setup_trades_quotes(&engine, &mut session);

    let rows = query_rows(
        &engine,
        &mut session,
        "SELECT trades.ts, quotes.price
         FROM trades ASOF LEFT JOIN quotes
         ON trades.symbol = quotes.symbol AND trades.ts >= quotes.ts",
    );
    assert_eq!(
        rows.len(),
        5,
        "ASOF LEFT JOIN parses and preserves left rows"
    );
}

#[test]
fn asof_join_partitions_by_equality_key() {
    let (_dir, engine, mut session) = setup_engine();
    // Same timestamps across symbols; the equality key must isolate them.
    engine
        .execute(
            &mut session,
            "CREATE TABLE q (symbol VARCHAR, ts INT64, price DOUBLE)",
        )
        .expect("create q");
    engine
        .execute(
            &mut session,
            "INSERT INTO q VALUES
                ('AAA', 100, 1.0),
                ('BBB', 200, 2.0)",
        )
        .expect("insert q");
    engine
        .execute(&mut session, "CREATE TABLE t (symbol VARCHAR, ts INT64)")
        .expect("create t");
    engine
        .execute(
            &mut session,
            "INSERT INTO t VALUES ('BBB', 150), ('AAA', 150)",
        )
        .expect("insert t");

    let rows = query_rows(
        &engine,
        &mut session,
        "SELECT t.symbol, q.price
         FROM t ASOF JOIN q
         ON t.symbol = q.symbol AND t.ts >= q.ts
         ORDER BY t.symbol",
    );

    // BBB@150 has no BBB quote at or before 150 (BBB quote is at 200) → dropped;
    // AAA@150 matches the AAA quote at 100.
    assert_eq!(rows.len(), 1, "rows: {rows:?}");
    assert_eq!(rows[0][0], ScalarValue::VarChar(Some("AAA".into())));
    assert_eq!(rows[0][1], ScalarValue::Float64(Some(1.0)));
}

#[test]
fn asof_join_supports_at_or_after_direction() {
    let (_dir, engine, mut session) = setup_engine();
    setup_trades_quotes(&engine, &mut session);

    // `l.ts <= r.ts` picks the earliest quote at or after the trade:
    // AAPL@50 and AAPL@100 both land on the 100 quote, AAPL@250 on the 300
    // quote; MSFT@500 has nothing after → dropped; GOOG has no quotes.
    let rows = query_rows(
        &engine,
        &mut session,
        "SELECT trades.ts, quotes.price
         FROM trades ASOF JOIN quotes
         ON trades.symbol = quotes.symbol AND trades.ts <= quotes.ts
         ORDER BY trades.qty",
    );

    assert_eq!(rows.len(), 3, "rows: {rows:?}");
    assert_eq!(rows[0][0], ScalarValue::Int64(Some(50)));
    assert_eq!(rows[0][1], ScalarValue::Float64(Some(10.0)));
    assert_eq!(rows[1][0], ScalarValue::Int64(Some(100)));
    assert_eq!(rows[1][1], ScalarValue::Float64(Some(10.0)));
    assert_eq!(rows[2][0], ScalarValue::Int64(Some(250)));
    assert_eq!(rows[2][1], ScalarValue::Float64(Some(12.0)));
}

#[test]
fn asof_join_multi_column_equality_keys() {
    let (_dir, engine, mut session) = setup_engine();
    engine
        .execute(
            &mut session,
            "CREATE TABLE q (exchange VARCHAR, symbol VARCHAR, ts INT64, price DOUBLE)",
        )
        .expect("create q");
    engine
        .execute(
            &mut session,
            "INSERT INTO q VALUES
                ('NASDAQ', 'AAA', 100, 1.0),
                ('NYSE', 'AAA', 100, 9.0),
                ('NASDAQ', 'AAA', 300, 3.0)",
        )
        .expect("insert q");
    engine
        .execute(
            &mut session,
            "CREATE TABLE t (exchange VARCHAR, symbol VARCHAR, ts INT64)",
        )
        .expect("create t");
    engine
        .execute(
            &mut session,
            "INSERT INTO t VALUES ('NYSE', 'AAA', 200), ('NASDAQ', 'AAA', 200)",
        )
        .expect("insert t");

    let rows = query_rows(
        &engine,
        &mut session,
        "SELECT t.exchange, q.price
         FROM t ASOF JOIN q
         ON t.exchange = q.exchange
         AND t.symbol = q.symbol
         AND t.ts >= q.ts
         ORDER BY t.exchange",
    );

    // (NYSE, AAA)@200 → NYSE quote at 100 (price 9.0);
    // (NASDAQ, AAA)@200 → NASDAQ quote at 100 (price 1.0) — the 300 quote
    // must not be picked.
    assert_eq!(rows.len(), 2, "rows: {rows:?}");
    assert_eq!(rows[0][0], ScalarValue::VarChar(Some("NASDAQ".into())));
    assert_eq!(rows[0][1], ScalarValue::Float64(Some(1.0)));
    assert_eq!(rows[1][0], ScalarValue::VarChar(Some("NYSE".into())));
    assert_eq!(rows[1][1], ScalarValue::Float64(Some(9.0)));
}

#[test]
fn asof_join_fails_loudly_on_bad_conditions() {
    let (_dir, engine, mut session) = setup_engine();
    engine
        .execute(&mut session, "CREATE TABLE a (ts INT64, v INT64)")
        .expect("create a");
    engine
        .execute(&mut session, "CREATE TABLE b (ts INT64, v INT64)")
        .expect("create b");

    // Missing the time-key inequality.
    let result = engine.execute(&mut session, "SELECT * FROM a ASOF JOIN b ON a.v = b.v");
    assert!(result.is_err(), "equality-only ON must be rejected");

    // Only an inequality, no equality keys.
    let result = engine.execute(&mut session, "SELECT * FROM a ASOF JOIN b ON a.ts >= b.ts");
    assert!(result.is_err(), "time-key-only ON must be rejected");

    // Non-numeric time key.
    engine
        .execute(&mut session, "CREATE TABLE c (symbol VARCHAR, v INT64)")
        .expect("create c");
    engine
        .execute(&mut session, "CREATE TABLE d (symbol VARCHAR, v INT64)")
        .expect("create d");
    let result = engine.execute(
        &mut session,
        "SELECT * FROM a ASOF JOIN c ON a.v = c.v AND a.ts >= c.symbol",
    );
    assert!(result.is_err(), "non-numeric time key must be rejected");
}

#[test]
fn asof_join_null_keys_and_times_never_match() {
    let (_dir, engine, mut session) = setup_engine();
    engine
        .execute(
            &mut session,
            "CREATE TABLE q (symbol VARCHAR, ts INT64, price DOUBLE)",
        )
        .expect("create q");
    engine
        .execute(&mut session, "INSERT INTO q VALUES ('AAA', 100, 1.0)")
        .expect("insert q");
    engine
        .execute(&mut session, "CREATE TABLE t (symbol VARCHAR, ts INT64)")
        .expect("create t");
    engine
        .execute(
            &mut session,
            "INSERT INTO t VALUES (NULL, 100), ('AAA', NULL), ('AAA', 150)",
        )
        .expect("insert t");

    let rows = query_rows(
        &engine,
        &mut session,
        "SELECT t.ts, q.price FROM t ASOF JOIN q ON t.symbol = q.symbol AND t.ts >= q.ts",
    );

    // Only (AAA, 150) matches; NULL symbol and NULL ts rows are dropped.
    assert_eq!(rows.len(), 1, "rows: {rows:?}");
    assert_eq!(rows[0][0], ScalarValue::Int64(Some(150)));
    assert_eq!(rows[0][1], ScalarValue::Float64(Some(1.0)));
}
