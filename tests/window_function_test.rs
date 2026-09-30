//! End-to-end tests for window functions (roadmap step 3, P3.5): ranking,
//! offsets, running/whole-partition/frame aggregates, and ORDER BY over a
//! window alias.

use tempfile::TempDir;

use taibai::net::session::Session;
use taibai::sql::execution::engine::{Engine, ExecResult};
use taibai::types::scalar::ScalarValue;

fn setup_engine() -> (TempDir, Engine, Session) {
    let temp_dir = TempDir::new().expect("temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("engine");
    let mut session = Session::new("root".to_string(), "main".to_string());

    engine
        .execute(
            &mut session,
            "CREATE TABLE ticks (symbol VARCHAR, ts INT64, price DOUBLE)",
        )
        .expect("create ticks");
    // AAPL has a price tie at ts 1 and 2 (RANK/DENSE_RANK peers).
    engine
        .execute(
            &mut session,
            "INSERT INTO ticks VALUES
                ('AAPL', 1, 10.0),
                ('AAPL', 2, 10.0),
                ('AAPL', 3, 12.0),
                ('MSFT', 1, 20.0),
                ('MSFT', 2, 21.0)",
        )
        .expect("insert");

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

fn i64(v: i64) -> ScalarValue {
    ScalarValue::Int64(Some(v))
}

fn f64(v: f64) -> ScalarValue {
    ScalarValue::Float64(Some(v))
}

#[test]
fn row_number_partitions_and_orders() {
    let (_dir, engine, mut session) = setup_engine();
    let rows = query_rows(
        &engine,
        &mut session,
        "SELECT symbol, ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY ts) rn
         FROM ticks ORDER BY symbol, ts",
    );
    assert_eq!(
        rows,
        vec![
            vec![ScalarValue::VarChar(Some("AAPL".into())), i64(1)],
            vec![ScalarValue::VarChar(Some("AAPL".into())), i64(2)],
            vec![ScalarValue::VarChar(Some("AAPL".into())), i64(3)],
            vec![ScalarValue::VarChar(Some("MSFT".into())), i64(1)],
            vec![ScalarValue::VarChar(Some("MSFT".into())), i64(2)],
        ]
    );
}

#[test]
fn rank_and_dense_rank_handle_ties() {
    let (_dir, engine, mut session) = setup_engine();
    let rows = query_rows(
        &engine,
        &mut session,
        "SELECT ts, RANK() OVER (PARTITION BY symbol ORDER BY price) r,
                DENSE_RANK() OVER (PARTITION BY symbol ORDER BY price) dr
         FROM ticks WHERE symbol = 'AAPL' ORDER BY ts",
    );
    // Prices 10.0, 10.0, 12.0: RANK has gaps, DENSE_RANK does not.
    assert_eq!(rows[0], vec![i64(1), i64(1), i64(1)]);
    assert_eq!(rows[1], vec![i64(2), i64(1), i64(1)]);
    assert_eq!(rows[2], vec![i64(3), i64(3), i64(2)]);
}

#[test]
fn lag_and_lead_offset_within_partition() {
    let (_dir, engine, mut session) = setup_engine();
    let rows = query_rows(
        &engine,
        &mut session,
        "SELECT ts,
                LAG(price) OVER (PARTITION BY symbol ORDER BY ts) prev,
                LEAD(price) OVER (PARTITION BY symbol ORDER BY ts) next,
                LEAD(price, 1, -1.0) OVER (PARTITION BY symbol ORDER BY ts) next_default
         FROM ticks WHERE symbol = 'MSFT' ORDER BY ts",
    );
    // MSFT: 20.0 then 21.0. First LAG is NULL; last LEAD is NULL unless a
    // default is given (here -1.0).
    assert_eq!(
        rows[0],
        vec![i64(1), ScalarValue::Float64(None), f64(21.0), f64(21.0)]
    );
    assert_eq!(
        rows[1],
        vec![i64(2), f64(20.0), ScalarValue::Float64(None), f64(-1.0)]
    );
}

#[test]
fn running_aggregate_uses_default_range_frame() {
    let (_dir, engine, mut session) = setup_engine();
    let rows = query_rows(
        &engine,
        &mut session,
        "SELECT ts, SUM(price) OVER (PARTITION BY symbol ORDER BY ts) running
         FROM ticks WHERE symbol = 'AAPL' ORDER BY ts",
    );
    // Default frame (unbounded preceding -> current row, peers included):
    // 10.0, 20.0 (both 10.0 rows are peers), 32.0.
    assert_eq!(rows[0][1], f64(10.0));
    assert_eq!(rows[1][1], f64(20.0));
    assert_eq!(rows[2][1], f64(32.0));
}

#[test]
fn whole_partition_aggregate_without_order_by() {
    let (_dir, engine, mut session) = setup_engine();
    let rows = query_rows(
        &engine,
        &mut session,
        "SELECT ts, SUM(price) OVER (PARTITION BY symbol) total
         FROM ticks WHERE symbol = 'MSFT' ORDER BY ts",
    );
    assert_eq!(rows[0][1], f64(41.0));
    assert_eq!(rows[1][1], f64(41.0));
}

#[test]
fn rows_frame_moves_window() {
    let (_dir, engine, mut session) = setup_engine();
    let rows = query_rows(
        &engine,
        &mut session,
        "SELECT ts, AVG(price) OVER (
             PARTITION BY symbol ORDER BY ts
             ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
         ) ma
         FROM ticks WHERE symbol = 'AAPL' ORDER BY ts",
    );
    // Moving average over the previous row and the current row.
    assert_eq!(rows[0][1], f64(10.0));
    assert_eq!(rows[1][1], f64(10.0));
    assert_eq!(rows[2][1], f64(11.0));
}

#[test]
fn order_by_window_alias_sorts_on_computed_column() {
    let (_dir, engine, mut session) = setup_engine();
    let rows = query_rows(
        &engine,
        &mut session,
        "SELECT symbol, ts, ROW_NUMBER() OVER (ORDER BY ts DESC) rn
         FROM ticks ORDER BY rn DESC LIMIT 2",
    );
    // The two earliest rows (ts=1 of each symbol) carry the largest
    // descending row numbers; ordering by rn itself is what is asserted.
    let rn_values: Vec<i64> = rows
        .iter()
        .map(|row| match &row[2] {
            ScalarValue::Int64(Some(v)) => *v,
            other => panic!("unexpected rn {other:?}"),
        })
        .collect();
    // ORDER BY rn DESC: output must be in descending rn order.
    assert_eq!(rn_values, vec![5, 4], "rows: {rows:?}");
    let mut sorted = rn_values.clone();
    sorted.sort();
    assert_eq!(sorted, vec![4, 5]);
}

#[test]
fn count_star_over_partition_counts_rows() {
    let (_dir, engine, mut session) = setup_engine();
    let rows = query_rows(
        &engine,
        &mut session,
        "SELECT DISTINCT COUNT(*) OVER (PARTITION BY symbol) c
         FROM ticks ORDER BY c",
    );
    assert_eq!(rows, vec![vec![i64(2)], vec![i64(3)]]);
}

#[test]
fn window_functions_on_interleaved_unsorted_input() {
    let (_dir, engine, mut session) = setup_engine();
    engine
        .execute(
            &mut session,
            "CREATE TABLE mixed (symbol VARCHAR, ts INT64, price DOUBLE)",
        )
        .expect("create mixed");
    // Physical insertion order interleaves the partitions and scrambles the
    // order key inside each partition: the executor must sort row indices by
    // (partition, order) keys itself, not trust input adjacency.
    engine
        .execute(
            &mut session,
            "INSERT INTO mixed VALUES
                ('MSFT', 3, 23.0),
                ('AAPL', 2, 10.0),
                ('MSFT', 1, 21.0),
                ('AAPL', 3, 12.0),
                ('AAPL', 1, 10.0),
                ('MSFT', 2, 22.0)",
        )
        .expect("insert");
    let rows = query_rows(
        &engine,
        &mut session,
        "SELECT symbol, ts,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY ts) rn,
                RANK() OVER (PARTITION BY symbol ORDER BY price) rk,
                DENSE_RANK() OVER (PARTITION BY symbol ORDER BY price) drk,
                LEAD(price) OVER (PARTITION BY symbol ORDER BY ts) nxt
         FROM mixed ORDER BY symbol, ts",
    );
    assert_eq!(
        rows,
        vec![
            vec![vs("AAPL"), i64(1), i64(1), i64(1), i64(1), f64(10.0)],
            vec![vs("AAPL"), i64(2), i64(2), i64(1), i64(1), f64(12.0)],
            vec![
                vs("AAPL"),
                i64(3),
                i64(3),
                i64(3),
                i64(2),
                ScalarValue::Float64(None)
            ],
            vec![vs("MSFT"), i64(1), i64(1), i64(1), i64(1), f64(22.0)],
            vec![vs("MSFT"), i64(2), i64(2), i64(2), i64(2), f64(23.0)],
            vec![
                vs("MSFT"),
                i64(3),
                i64(3),
                i64(3),
                i64(3),
                ScalarValue::Float64(None)
            ],
        ]
    );
}

fn vs(s: &str) -> ScalarValue {
    ScalarValue::VarChar(Some(s.to_string()))
}

#[test]
fn range_offset_frames_use_value_semantics() {
    let (_dir, engine, mut session) = setup_engine();
    engine
        .execute(&mut session, "CREATE TABLE r (ts INT64, v DOUBLE)")
        .expect("create r");
    // Gaps in the order key distinguish value-based RANGE bounds from row
    // counts: a ROWS frame at ts=100 would include ts=3, the RANGE frame
    // must not.
    engine
        .execute(
            &mut session,
            "INSERT INTO r VALUES (1, 1.0),(2, 2.0),(3, 3.0),(100, 100.0),(101, 101.0)",
        )
        .expect("insert r");
    let rows = query_rows(
        &engine,
        &mut session,
        "SELECT ts, SUM(v) OVER (ORDER BY ts RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) s
         FROM r ORDER BY ts",
    );
    assert_eq!(
        rows,
        vec![
            vec![i64(1), f64(1.0)],
            vec![i64(2), f64(3.0)],
            vec![i64(3), f64(5.0)],
            vec![i64(100), f64(100.0)],
            vec![i64(101), f64(201.0)],
        ]
    );

    // DESC order: "preceding" means larger values.
    let rows = query_rows(
        &engine,
        &mut session,
        "SELECT ts, SUM(v) OVER (ORDER BY ts DESC RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) s
         FROM r ORDER BY ts",
    );
    assert_eq!(
        rows,
        vec![
            vec![i64(1), f64(3.0)],
            vec![i64(2), f64(5.0)],
            vec![i64(3), f64(3.0)],
            vec![i64(100), f64(201.0)],
            vec![i64(101), f64(101.0)],
        ]
    );
}

fn wc_create_engine() -> (tempfile::TempDir, Engine, Session) {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let engine = Engine::new(temp_dir.path(), "main").unwrap();
    let session = Session::new("root".to_string(), "main".to_string());
    (temp_dir, engine, session)
}

#[test]
fn window_sum_decimal_and_empty_min_frame_do_not_error() {
    let (_temp_dir, engine, mut session) = wc_create_engine();
    engine
        .execute(
            &mut session,
            "CREATE TABLE tw (id INT64, px DECIMAL(9,2), ts INT64)",
        )
        .expect("create");
    engine
        .execute(
            &mut session,
            "INSERT INTO tw (id, px, ts) VALUES (1, 10.50, 1), (2, 20.25, 2), (3, 5.75, 3)",
        )
        .expect("insert");

    // SUM(DECIMAL) OVER used to coerce to Int64 and fail the strict column
    // push with an Internal "type mismatch" error.
    let result = engine
        .execute(&mut session, "SELECT SUM(px) OVER () FROM tw")
        .expect("decimal window sum");
    let rows = collect_first_column(result);
    assert_eq!(rows.len(), 3);

    // MIN over a frame that is empty for the first rows (end bound 2
    // PRECEDING while only 1 row exists) used to emit Int64(None) into a
    // non-Int64 column.
    let result = engine
        .execute(
            &mut session,
            "SELECT MIN(ts) OVER (ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND 2 PRECEDING) FROM tw",
        )
        .expect("empty-frame min");
    assert_eq!(collect_first_column(result).len(), 3);
    engine.segment_mgr().shutdown();
}

#[test]
fn range_frame_preceding_end_bound_is_inclusive() {
    let (_temp_dir, engine, mut session) = wc_create_engine();
    engine
        .execute(&mut session, "CREATE TABLE tr (v INT64)")
        .expect("create");
    engine
        .execute(&mut session, "INSERT INTO tr (v) VALUES (1), (2), (3)")
        .expect("insert");

    // SQL standard / PostgreSQL: RANGE BETWEEN UNBOUNDED PRECEDING AND 1
    // PRECEDING for v=3 covers {1, 2} (v <= 3-1), not just {1}.
    let result = engine
        .execute(
            &mut session,
            "SELECT SUM(v) OVER (ORDER BY v RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) FROM tr ORDER BY v",
        )
        .expect("range frame");
    let sums = collect_first_column(result);
    // v=1: empty frame -> NULL; v=2: {1}; v=3: {1,2}
    assert_eq!(sums.len(), 3);
    assert_eq!(sums[2], "3", "inclusive PRECEDING end bound sums 1+2");
    engine.segment_mgr().shutdown();
}

fn collect_first_column(result: taibai::sql::execution::engine::ExecResult) -> Vec<String> {
    use taibai::sql::execution::engine::ExecResult;
    let ExecResult::Query { batches, .. } = result else {
        panic!("expected query result");
    };
    let mut values = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            values.push(
                batch
                    .scalar(row, 0)
                    .map(|v| v.to_text().unwrap_or_else(|| "NULL".to_string()))
                    .unwrap_or_else(|| "NULL".to_string()),
            );
        }
    }
    values
}
