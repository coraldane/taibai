use tempfile::TempDir;

use taibai::net::session::Session;
use taibai::sql::execution::engine::{Engine, ExecResult};
use taibai::types::scalar::ScalarValue;

fn collect_first_column(result: ExecResult) -> Vec<ScalarValue> {
    let ExecResult::Query { batches, .. } = result else {
        panic!("expected query result");
    };
    let mut values = Vec::new();
    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            values.push(batch.scalar(row_idx, 0).expect("first column"));
        }
    }
    values
}

#[test]
fn topn_limit_offset_applies_offset_after_sorting_join_results() {
    let temp_dir = TempDir::new().expect("temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("engine");
    let mut session = Session::new("root".to_string(), "main".to_string());

    engine
        .execute(
            &mut session,
            "CREATE TABLE t_stock_daily (
                security_id VARCHAR,
                deal_date INT64,
                open_px DOUBLE,
                high_px DOUBLE,
                low_px DOUBLE,
                close_px DOUBLE,
                pre_close DOUBLE,
                trade_value DOUBLE
            )",
        )
        .expect("create daily");
    engine
        .execute(
            &mut session,
            "CREATE TABLE t_security_info (
                security_id VARCHAR,
                display_name VARCHAR
            )",
        )
        .expect("create security");
    engine
        .execute(
            &mut session,
            "CREATE TABLE t_valuation (
                deal_date INT64,
                security_id VARCHAR,
                turnover_ratio DOUBLE,
                pe_ratio DOUBLE,
                pb_ratio DOUBLE,
                market_cap DOUBLE,
                circulating_market_cap DOUBLE
            )",
        )
        .expect("create valuation");

    engine
        .execute(
            &mut session,
            "INSERT INTO t_stock_daily VALUES
                ('A', 20260608, 10.0, 15.0, 10.0, 15.0, 10.0, 100.0),
                ('B', 20260608, 10.0, 14.0, 10.0, 14.0, 10.0, 100.0),
                ('C', 20260608, 10.0, 13.0, 10.0, 13.0, 10.0, 100.0),
                ('D', 20260608, 10.0, 12.0, 10.0, 12.0, 10.0, 100.0),
                ('E', 20260608, 10.0, 11.0, 10.0, 11.0, 10.0, 100.0)",
        )
        .expect("insert daily");
    engine
        .execute(
            &mut session,
            "INSERT INTO t_security_info VALUES
                ('A', 'Alpha'),
                ('B', 'Beta'),
                ('C', 'Charlie'),
                ('D', 'Delta'),
                ('E', 'Echo')",
        )
        .expect("insert security");
    engine
        .execute(
            &mut session,
            "INSERT INTO t_valuation VALUES
                (20260608, 'A', 1.0, 10.0, 1.0, 100.0, 90.0),
                (20260608, 'B', 1.0, 10.0, 1.0, 100.0, 90.0),
                (20260608, 'C', 1.0, 10.0, 1.0, 100.0, 90.0),
                (20260608, 'D', 1.0, 10.0, 1.0, 100.0, 90.0),
                (20260608, 'E', 1.0, 10.0, 1.0, 100.0, 90.0)",
        )
        .expect("insert valuation");

    let rows = collect_first_column(
        engine
            .execute(
                &mut session,
                "SELECT d.security_id, s.display_name,
                        d.open_px, d.high_px, d.low_px, d.close_px, d.pre_close,
                        d.trade_value,
                        v.turnover_ratio, v.pe_ratio, v.pb_ratio, v.market_cap, v.circulating_market_cap
                FROM t_stock_daily d
                LEFT JOIN t_security_info s ON d.security_id = s.security_id
                LEFT JOIN t_valuation v ON v.deal_date = 20260608 AND d.security_id = v.security_id
                WHERE d.deal_date = 20260608
                ORDER BY (d.close_px - d.pre_close) / d.pre_close DESC
                LIMIT 2 OFFSET 2",
            )
            .expect("select second page"),
    );

    assert_eq!(
        rows,
        vec![
            ScalarValue::VarChar(Some("C".to_string())),
            ScalarValue::VarChar(Some("D".to_string())),
        ]
    );
}

#[test]
fn topn_pushdown_keeps_offset_semantics_when_left_join_multiplies_rows() {
    let temp_dir = TempDir::new().expect("temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("engine");
    let mut session = Session::new("root".to_string(), "main".to_string());

    engine
        .execute(
            &mut session,
            "CREATE TABLE fact_rows (
                id VARCHAR,
                bucket_id INT64,
                score DOUBLE
            )",
        )
        .expect("create fact");
    engine
        .execute(
            &mut session,
            "CREATE TABLE dim_rows (
                id VARCHAR,
                label VARCHAR
            )",
        )
        .expect("create dim");
    engine
        .execute(
            &mut session,
            "CREATE TABLE dim_more (
                id VARCHAR,
                extra VARCHAR
            )",
        )
        .expect("create dim_more");

    engine
        .execute(
            &mut session,
            "INSERT INTO fact_rows VALUES
                ('A', 1, 5.0),
                ('B', 1, 4.0),
                ('C', 1, 3.0),
                ('D', 1, 2.0),
                ('E', 1, 1.0)",
        )
        .expect("insert fact");
    engine
        .execute(
            &mut session,
            "INSERT INTO dim_rows VALUES
                ('A', 'a1'),
                ('A', 'a2'),
                ('B', 'b1'),
                ('C', 'c1'),
                ('D', 'd1'),
                ('E', 'e1')",
        )
        .expect("insert dim");
    engine
        .execute(
            &mut session,
            "INSERT INTO dim_more VALUES
                ('A', 'ma'),
                ('B', 'mb'),
                ('C', 'mc'),
                ('D', 'md'),
                ('E', 'me')",
        )
        .expect("insert dim_more");

    let rows = collect_first_column(
        engine
            .execute(
                &mut session,
                "SELECT f.id, d.label, m.extra
                 FROM fact_rows f
                 LEFT JOIN dim_rows d ON f.id = d.id
                 LEFT JOIN dim_more m ON f.id = m.id
                 WHERE f.bucket_id = 1
                 ORDER BY f.score DESC
                 LIMIT 2 OFFSET 2",
            )
            .expect("select second page"),
    );

    assert_eq!(
        rows,
        vec![
            ScalarValue::VarChar(Some("B".to_string())),
            ScalarValue::VarChar(Some("C".to_string())),
        ]
    );
}
