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

fn collect_rows(result: ExecResult) -> Vec<Vec<ScalarValue>> {
    let ExecResult::Query { batches, .. } = result else {
        panic!("expected query result");
    };
    let mut rows = Vec::new();
    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            rows.push(
                (0..batch.num_columns())
                    .map(|col_idx| batch.scalar(row_idx, col_idx).expect("scalar"))
                    .collect(),
            );
        }
    }
    rows
}

fn query(engine: &Engine, session: &mut Session, sql: &str) -> Vec<Vec<ScalarValue>> {
    collect_rows(
        engine
            .execute(session, sql)
            .unwrap_or_else(|error| panic!("query failed: {sql}\n{error}")),
    )
}

fn query_err(engine: &Engine, session: &mut Session, sql: &str) -> String {
    match engine.execute(session, sql) {
        Err(error) => error.to_string(),
        Ok(result) => panic!("expected error for: {sql}\ngot: {:?}", collect_rows(result)),
    }
}

fn fmt_cell(value: &ScalarValue) -> String {
    match value {
        ScalarValue::Float64(Some(v)) => format!("f:{v:.6}"),
        ScalarValue::Float32(Some(v)) => format!("f:{v:.6}"),
        other => format!("{other:?}"),
    }
}

fn sorted_rows(rows: Vec<Vec<ScalarValue>>) -> Vec<String> {
    let mut rendered = rows
        .iter()
        .map(|row| row.iter().map(fmt_cell).collect::<Vec<_>>().join(","))
        .collect::<Vec<_>>();
    rendered.sort();
    rendered
}

fn assert_rows_unordered(
    sql: &str,
    actual: Vec<Vec<ScalarValue>>,
    expected: Vec<Vec<ScalarValue>>,
) {
    let actual_sorted = sorted_rows(actual);
    let expected_sorted = sorted_rows(expected);
    assert_eq!(
        actual_sorted, expected_sorted,
        "row mismatch for query: {sql}\nactual:   {actual_sorted:?}\nexpected: {expected_sorted:?}"
    );
}

fn i64(v: i64) -> ScalarValue {
    ScalarValue::Int64(Some(v))
}

fn i32(v: i32) -> ScalarValue {
    ScalarValue::Int32(Some(v))
}

fn f64v(v: f64) -> ScalarValue {
    ScalarValue::Float64(Some(v))
}

fn s(v: &str) -> ScalarValue {
    ScalarValue::VarChar(Some(v.to_string()))
}

fn seed_quote_tables(engine: &Engine, session: &mut Session) {
    engine
        .execute(
            session,
            "CREATE TABLE t_quote (
                security_id VARCHAR(20),
                deal_date INT,
                deal_time INT,
                price DOUBLE,
                volume INT
            )",
        )
        .expect("create t_quote");
    engine
        .execute(
            session,
            "INSERT INTO t_quote VALUES
                ('000001.SZ', 20260904, 930, 10.0, 100),
                ('000001.SZ', 20260904, 1000, 10.5, 200),
                ('000001.SZ', 20260904, 1459, 11.0, 300),
                ('000002.SZ', 20260904, 930, 20.0, 400),
                ('000002.SZ', 20260905, 930, 21.5, NULL),
                ('000002.SZ', 20260905, 1500, NULL, 600),
                ('600000.SH', 20260904, 930, 5.25, 700),
                ('600000.SH', 20260904, 1000, 5.5, 800),
                ('600000.SH', 20260905, 930, 5.75, 900),
                ('600000.SH', 20260905, 1000, 6.0, 1000),
                ('600000.SH', 20260905, 1459, 6.25, 1100),
                ('688001.SH', 20260905, 930, 50.0, 1200)",
        )
        .expect("insert t_quote");

    engine
        .execute(
            session,
            "CREATE TABLE t_security (
                security_id VARCHAR(20),
                sector VARCHAR(20)
            )",
        )
        .expect("create t_security");
    engine
        .execute(
            session,
            "INSERT INTO t_security VALUES
                ('000001.SZ', 'bank'),
                ('000002.SZ', 'realestate'),
                ('600000.SH', 'bank'),
                ('999999.SZ', 'orphan')",
        )
        .expect("insert t_security");
}

#[test]
fn projection_column_and_star() {
    let (_temp_dir, engine, mut session) = create_engine();
    seed_quote_tables(&engine, &mut session);

    let rows = query(
        &engine,
        &mut session,
        "SELECT security_id FROM t_quote WHERE deal_date = 20260904 ORDER BY security_id",
    );
    assert_eq!(
        rows,
        vec![
            vec![s("000001.SZ")],
            vec![s("000001.SZ")],
            vec![s("000001.SZ")],
            vec![s("000002.SZ")],
            vec![s("600000.SH")],
            vec![s("600000.SH")],
        ],
        "single column projection with WHERE"
    );

    let rows = query(
        &engine,
        &mut session,
        "SELECT * FROM t_quote WHERE volume = 1200",
    );
    assert_eq!(rows.len(), 1, "star projection row count");
    assert_eq!(rows[0][0], s("688001.SH"));
    assert_eq!(rows[0][1], i32(20260905));
    assert_eq!(rows[0][4], i32(1200));
}

#[test]
fn projection_alias_and_arithmetic() {
    let (_temp_dir, engine, mut session) = create_engine();
    seed_quote_tables(&engine, &mut session);

    let rows = query(
        &engine,
        &mut session,
        "SELECT security_id AS sid, price * volume AS amount
         FROM t_quote
         WHERE deal_date = 20260904 AND deal_time = 930",
    );
    assert_eq!(rows.len(), 3, "alias projection rows");
    assert_eq!(rows[0][0], s("000001.SZ"));
    assert_eq!(rows[0][1], f64v(1000.0));
    assert_eq!(rows[1][0], s("000002.SZ"));
    assert_eq!(rows[1][1], f64v(8000.0));
    assert_eq!(rows[2][0], s("600000.SH"));
    assert_eq!(rows[2][1], f64v(3675.0));

    let rows = query(
        &engine,
        &mut session,
        "SELECT price * volume FROM t_quote WHERE security_id = '000002.SZ' ORDER BY deal_time",
    );
    assert_eq!(rows.len(), 3, "null propagation rows");
    assert_eq!(rows[0][1 - 1], f64v(8000.0));
    assert_eq!(
        rows[1][0],
        ScalarValue::Float64(None),
        "NULL volume -> NULL"
    );
    assert_eq!(rows[2][0], ScalarValue::Float64(None), "NULL price -> NULL");
}

#[test]
fn where_operators_and_or_not() {
    let (_temp_dir, engine, mut session) = create_engine();
    seed_quote_tables(&engine, &mut session);

    let rows = query(
        &engine,
        &mut session,
        "SELECT COUNT(*) FROM t_quote WHERE deal_date = 20260904 OR volume >= 1100",
    );
    assert_eq!(rows, vec![vec![i64(8)]], "OR predicate");

    let rows = query(
        &engine,
        &mut session,
        "SELECT COUNT(*) FROM t_quote WHERE deal_date = 20260905 AND price >= 6.0",
    );
    assert_eq!(
        rows,
        vec![vec![i64(4)]],
        "AND predicate (21.5, 6.0, 6.25, 50.0)"
    );

    let rows = query(
        &engine,
        &mut session,
        "SELECT COUNT(*) FROM t_quote WHERE NOT (deal_date = 20260904)",
    );
    assert_eq!(rows, vec![vec![i64(6)]], "NOT predicate");

    let rows = query(
        &engine,
        &mut session,
        "SELECT COUNT(*) FROM t_quote WHERE deal_date <> 20260904",
    );
    assert_eq!(rows, vec![vec![i64(6)]], "<> predicate");
}

#[test]
fn where_in_between_like_null_checks() {
    let (_temp_dir, engine, mut session) = create_engine();
    seed_quote_tables(&engine, &mut session);

    let rows = query(
        &engine,
        &mut session,
        "SELECT COUNT(*) FROM t_quote WHERE security_id IN ('000001.SZ', '688001.SH')",
    );
    assert_eq!(rows, vec![vec![i64(4)]], "IN list");

    let rows = query(
        &engine,
        &mut session,
        "SELECT COUNT(*) FROM t_quote WHERE security_id NOT IN ('000001.SZ', '688001.SH')",
    );
    assert_eq!(rows, vec![vec![i64(8)]], "NOT IN list");

    let rows = query(
        &engine,
        &mut session,
        "SELECT COUNT(*) FROM t_quote WHERE price BETWEEN 10.0 AND 21.5",
    );
    assert_eq!(rows, vec![vec![i64(5)]], "BETWEEN inclusive bounds");

    let rows = query(
        &engine,
        &mut session,
        "SELECT COUNT(*) FROM t_quote WHERE security_id LIKE '0000%'",
    );
    assert_eq!(rows, vec![vec![i64(6)]], "LIKE prefix");

    let rows = query(
        &engine,
        &mut session,
        "SELECT COUNT(*) FROM t_quote WHERE security_id LIKE '00000_.S_'",
    );
    assert_eq!(rows, vec![vec![i64(6)]], "LIKE underscore");

    let rows = query(
        &engine,
        &mut session,
        "SELECT COUNT(*) FROM t_quote WHERE price IS NULL",
    );
    assert_eq!(rows, vec![vec![i64(1)]], "IS NULL");

    let rows = query(
        &engine,
        &mut session,
        "SELECT COUNT(*) FROM t_quote WHERE price IS NOT NULL",
    );
    assert_eq!(rows, vec![vec![i64(11)]], "IS NOT NULL");

    let rows = query(
        &engine,
        &mut session,
        "SELECT COUNT(*) FROM t_quote WHERE price = NULL",
    );
    assert_eq!(
        rows,
        vec![vec![i64(0)]],
        "comparison with NULL yields nothing"
    );
}

#[test]
fn order_by_and_limit_offset() {
    let (_temp_dir, engine, mut session) = create_engine();
    seed_quote_tables(&engine, &mut session);

    let rows = query(
        &engine,
        &mut session,
        "SELECT deal_date, deal_time FROM t_quote
         WHERE security_id = '600000.SH'
         ORDER BY deal_date DESC, deal_time ASC",
    );
    assert_eq!(
        rows,
        vec![
            vec![i32(20260905), i32(930)],
            vec![i32(20260905), i32(1000)],
            vec![i32(20260905), i32(1459)],
            vec![i32(20260904), i32(930)],
            vec![i32(20260904), i32(1000)],
        ],
        "mixed direction multi-key ORDER BY"
    );

    let rows = query(
        &engine,
        &mut session,
        "SELECT security_id FROM t_quote ORDER BY security_id LIMIT 3 OFFSET 1",
    );
    assert_eq!(
        rows,
        vec![
            vec![s("000001.SZ")],
            vec![s("000001.SZ")],
            vec![s("000002.SZ")],
        ],
        "LIMIT with OFFSET"
    );

    // Derived table with its own LIMIT, aggregated outside.
    let rows = query(
        &engine,
        &mut session,
        "SELECT COUNT(*) FROM (SELECT 1 AS x FROM t_quote LIMIT 5) sub",
    );
    assert_eq!(rows, vec![vec![i64(5)]], "derived table with LIMIT");

    let rows = query(
        &engine,
        &mut session,
        "SELECT security_id FROM t_quote ORDER BY deal_time LIMIT 100",
    );
    assert_eq!(rows.len(), 12, "LIMIT beyond row count");
}

#[test]
fn aggregates_without_group_by() {
    let (_temp_dir, engine, mut session) = create_engine();
    seed_quote_tables(&engine, &mut session);

    let rows = query(
        &engine,
        &mut session,
        "SELECT COUNT(*), COUNT(price), COUNT(volume), COUNT(DISTINCT security_id)
         FROM t_quote",
    );
    assert_eq!(
        rows,
        vec![vec![i64(12), i64(11), i64(11), i64(4)]],
        "global aggregates with NULL semantics (4 distinct security ids)"
    );

    let rows = query(
        &engine,
        &mut session,
        "SELECT SUM(price), MIN(price), MAX(price), AVG(price) FROM t_quote",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], f64v(151.75), "SUM skips NULL");
    assert_eq!(rows[0][1], f64v(5.25), "MIN");
    assert_eq!(rows[0][2], f64v(50.0), "MAX");
    assert_eq!(
        rows[0][3],
        f64v(151.75 / 11.0),
        "AVG = SUM/COUNT over non-null values"
    );
}

#[test]
fn aggregates_on_empty_input() {
    let (_temp_dir, engine, mut session) = create_engine();
    seed_quote_tables(&engine, &mut session);

    let rows = query(
        &engine,
        &mut session,
        "SELECT COUNT(*), COUNT(price), SUM(price), AVG(price), MIN(price), MAX(price)
         FROM t_quote WHERE deal_date = 20990101",
    );
    assert_eq!(
        rows.len(),
        1,
        "aggregates on empty input still return one row"
    );
    assert_eq!(rows[0][0], i64(0), "COUNT(*) = 0 on empty");
    assert_eq!(rows[0][1], i64(0), "COUNT(col) = 0 on empty");
    assert_eq!(
        rows[0][2],
        ScalarValue::Float64(None),
        "SUM = NULL on empty"
    );
    assert_eq!(
        rows[0][3],
        ScalarValue::Float64(None),
        "AVG = NULL on empty"
    );
    assert_eq!(
        rows[0][4],
        ScalarValue::Float64(None),
        "MIN = NULL on empty"
    );
    assert_eq!(
        rows[0][5],
        ScalarValue::Float64(None),
        "MAX = NULL on empty"
    );
}

#[test]
fn aggregates_over_arithmetic_expressions() {
    let (_temp_dir, engine, mut session) = create_engine();
    seed_quote_tables(&engine, &mut session);

    let rows = query(
        &engine,
        &mut session,
        "SELECT SUM(price * volume) FROM t_quote",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0][0],
        f64v(100525.0),
        "SUM over expression, NULL rows skipped"
    );

    let rows = query(
        &engine,
        &mut session,
        "SELECT MAX(price) - MIN(price) FROM t_quote",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], f64v(44.75), "arithmetic between two aggregates");

    let rows = query(&engine, &mut session, "SELECT COUNT(*) + 1 FROM t_quote");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], i64(13), "COUNT(*) + 1");
}

#[test]
fn group_by_basic() {
    let (_temp_dir, engine, mut session) = create_engine();
    seed_quote_tables(&engine, &mut session);

    let rows = query(
        &engine,
        &mut session,
        "SELECT security_id, COUNT(*), COUNT(price), SUM(volume)
         FROM t_quote GROUP BY security_id",
    );
    assert_rows_unordered(
        "group by single key",
        rows,
        vec![
            vec![s("000001.SZ"), i64(3), i64(3), i64(600)],
            vec![s("000002.SZ"), i64(3), i64(2), i64(1000)],
            vec![s("600000.SH"), i64(5), i64(5), i64(4500)],
            vec![s("688001.SH"), i64(1), i64(1), i64(1200)],
        ],
    );

    let rows = query(
        &engine,
        &mut session,
        "SELECT security_id, deal_date, COUNT(*)
         FROM t_quote GROUP BY security_id, deal_date",
    );
    assert_rows_unordered(
        "group by two keys",
        rows,
        vec![
            vec![s("000001.SZ"), i32(20260904), i64(3)],
            vec![s("000002.SZ"), i32(20260904), i64(1)],
            vec![s("000002.SZ"), i32(20260905), i64(2)],
            vec![s("600000.SH"), i32(20260904), i64(2)],
            vec![s("600000.SH"), i32(20260905), i64(3)],
            vec![s("688001.SH"), i32(20260905), i64(1)],
        ],
    );
}

#[test]
fn group_by_expression_key() {
    let (_temp_dir, engine, mut session) = create_engine();
    seed_quote_tables(&engine, &mut session);

    let rows = query(
        &engine,
        &mut session,
        "SELECT deal_time / 100 AS hour_bucket, COUNT(*)
         FROM t_quote GROUP BY deal_time / 100",
    );
    assert_rows_unordered(
        "group by expression (float division semantics)",
        rows,
        vec![
            vec![f64v(9.3), i64(6)],
            vec![f64v(10.0), i64(3)],
            vec![f64v(14.59), i64(2)],
            vec![f64v(15.0), i64(1)],
        ],
    );
}

#[test]
fn having_filters_groups() {
    let (_temp_dir, engine, mut session) = create_engine();
    seed_quote_tables(&engine, &mut session);

    let rows = query(
        &engine,
        &mut session,
        "SELECT security_id, COUNT(*) cnt FROM t_quote GROUP BY security_id HAVING cnt >= 3",
    );
    assert_rows_unordered(
        "HAVING on aggregate alias",
        rows,
        vec![
            vec![s("000001.SZ"), i64(3)],
            vec![s("000002.SZ"), i64(3)],
            vec![s("600000.SH"), i64(5)],
        ],
    );

    let rows = query(
        &engine,
        &mut session,
        "SELECT security_id FROM t_quote GROUP BY security_id HAVING COUNT(*) >= 3",
    );
    assert_rows_unordered(
        "HAVING COUNT(*) not present in SELECT",
        rows,
        vec![
            vec![s("000001.SZ")],
            vec![s("000002.SZ")],
            vec![s("600000.SH")],
        ],
    );

    let rows = query(
        &engine,
        &mut session,
        "SELECT security_id, MIN(price) lo FROM t_quote GROUP BY security_id HAVING SUM(volume) > 4000",
    );
    assert_rows_unordered(
        "HAVING SUM not present in SELECT",
        rows,
        vec![vec![s("600000.SH"), f64v(5.25)]],
    );

    let rows = query(
        &engine,
        &mut session,
        "SELECT security_id FROM t_quote GROUP BY security_id HAVING security_id LIKE '0000%'",
    );
    assert_rows_unordered(
        "HAVING on group column",
        rows,
        vec![vec![s("000001.SZ")], vec![s("000002.SZ")]],
    );
}

#[test]
fn group_by_order_variants() {
    let (_temp_dir, engine, mut session) = create_engine();
    seed_quote_tables(&engine, &mut session);

    let rows = query(
        &engine,
        &mut session,
        "SELECT security_id, COUNT(*) cnt FROM t_quote GROUP BY security_id
         ORDER BY cnt DESC, security_id ASC",
    );
    assert_eq!(
        rows,
        vec![
            vec![s("600000.SH"), i64(5)],
            vec![s("000001.SZ"), i64(3)],
            vec![s("000002.SZ"), i64(3)],
            vec![s("688001.SH"), i64(1)],
        ],
        "ORDER BY aggregate alias"
    );

    let rows = query(
        &engine,
        &mut session,
        "SELECT security_id, COUNT(*) FROM t_quote GROUP BY security_id
         ORDER BY COUNT(*) DESC, security_id ASC",
    );
    assert_eq!(
        rows,
        vec![
            vec![s("600000.SH"), i64(5)],
            vec![s("000001.SZ"), i64(3)],
            vec![s("000002.SZ"), i64(3)],
            vec![s("688001.SH"), i64(1)],
        ],
        "ORDER BY aggregate expression"
    );

    let rows = query(
        &engine,
        &mut session,
        "SELECT COUNT(*) cnt, security_id FROM t_quote GROUP BY security_id
         ORDER BY security_id",
    );
    assert_eq!(
        rows,
        vec![
            vec![i64(3), s("000001.SZ")],
            vec![i64(3), s("000002.SZ")],
            vec![i64(5), s("600000.SH")],
            vec![i64(1), s("688001.SH")],
        ],
        "ORDER BY group column when aggregate is first in SELECT"
    );
}

#[test]
fn group_by_null_key_forms_single_group() {
    let (_temp_dir, engine, mut session) = create_engine();
    engine
        .execute(
            &mut session,
            "CREATE TABLE t_null_group (grp VARCHAR(10), v INT)",
        )
        .expect("create");
    engine
        .execute(
            &mut session,
            "INSERT INTO t_null_group VALUES ('a', 1), ('a', 2), (NULL, 3), (NULL, 4), ('b', 5)",
        )
        .expect("insert");

    let rows = query(
        &engine,
        &mut session,
        "SELECT grp, COUNT(*) FROM t_null_group GROUP BY grp",
    );
    let mut found_null_group = false;
    for row in &rows {
        if row[0].is_null() {
            assert_eq!(row[1], i64(2), "NULL keys group together");
            found_null_group = true;
        }
    }
    assert!(found_null_group, "NULL group key must appear: {rows:?}");
    assert_eq!(rows.len(), 3, "three groups total: {rows:?}");
}

#[test]
fn distinct_deduplicates_rows() {
    let (_temp_dir, engine, mut session) = create_engine();
    seed_quote_tables(&engine, &mut session);

    let rows = query(
        &engine,
        &mut session,
        "SELECT DISTINCT security_id FROM t_quote ORDER BY security_id",
    );
    assert_eq!(
        rows,
        vec![
            vec![s("000001.SZ")],
            vec![s("000002.SZ")],
            vec![s("600000.SH")],
            vec![s("688001.SH")],
        ],
        "DISTINCT single column"
    );

    let rows = query(
        &engine,
        &mut session,
        "SELECT DISTINCT security_id, deal_date FROM t_quote ORDER BY security_id, deal_date",
    );
    assert_eq!(
        rows,
        vec![
            vec![s("000001.SZ"), i32(20260904)],
            vec![s("000002.SZ"), i32(20260904)],
            vec![s("000002.SZ"), i32(20260905)],
            vec![s("600000.SH"), i32(20260904)],
            vec![s("600000.SH"), i32(20260905)],
            vec![s("688001.SH"), i32(20260905)],
        ],
        "DISTINCT two columns"
    );

    let rows = query(
        &engine,
        &mut session,
        "SELECT DISTINCT security_id FROM t_quote ORDER BY security_id LIMIT 2",
    );
    assert_eq!(
        rows,
        vec![vec![s("000001.SZ")], vec![s("000002.SZ")]],
        "DISTINCT with ORDER BY and LIMIT"
    );
}

#[test]
fn joins_inner_left_right_cross() {
    let (_temp_dir, engine, mut session) = create_engine();
    seed_quote_tables(&engine, &mut session);

    let rows = query(
        &engine,
        &mut session,
        "SELECT COUNT(*) FROM t_quote q INNER JOIN t_security sec ON q.security_id = sec.security_id
         WHERE q.deal_date = 20260905",
    );
    assert_eq!(rows, vec![vec![i64(5)]], "INNER JOIN drops unmatched rows");

    let rows = query(
        &engine,
        &mut session,
        "SELECT COUNT(*) FROM t_quote q LEFT JOIN t_security sec ON q.security_id = sec.security_id",
    );
    assert_eq!(rows, vec![vec![i64(12)]], "LEFT JOIN keeps all left rows");

    let rows = query(
        &engine,
        &mut session,
        "SELECT q.security_id, sec.sector
         FROM t_quote q LEFT JOIN t_security sec ON q.security_id = sec.security_id
         WHERE sec.sector IS NULL",
    );
    assert_eq!(
        rows,
        vec![vec![s("688001.SH"), ScalarValue::VarChar(None)]],
        "LEFT JOIN null-pads unmatched right side"
    );

    let rows = query(
        &engine,
        &mut session,
        "SELECT COUNT(*) FROM t_quote q RIGHT JOIN t_security sec ON q.security_id = sec.security_id",
    );
    assert_eq!(
        rows,
        vec![vec![i64(12)]],
        "RIGHT JOIN keeps all right rows (11 matched + 1 orphan)"
    );

    let rows = query(
        &engine,
        &mut session,
        "SELECT COUNT(*) FROM t_quote q FULL JOIN t_security sec ON q.security_id = sec.security_id",
    );
    assert_eq!(rows, vec![vec![i64(13)]], "FULL JOIN keeps unmatched rows from both sides (11 + left-only 688001 + right-only 999999)");

    let rows = query(
        &engine,
        &mut session,
        "SELECT COUNT(*) FROM t_quote q CROSS JOIN t_security sec",
    );
    assert_eq!(rows, vec![vec![i64(48)]], "CROSS JOIN cartesian product");

    let rows = query(
        &engine,
        &mut session,
        "SELECT COUNT(*) FROM t_quote q, t_security sec WHERE q.security_id = sec.security_id",
    );
    assert_eq!(
        rows,
        vec![vec![i64(11)]],
        "comma join with WHERE behaves as INNER JOIN"
    );
}

#[test]
fn join_with_group_by() {
    let (_temp_dir, engine, mut session) = create_engine();
    seed_quote_tables(&engine, &mut session);

    let rows = query(
        &engine,
        &mut session,
        "SELECT sec.sector, COUNT(*), SUM(q.volume)
         FROM t_quote q JOIN t_security sec ON q.security_id = sec.security_id
         GROUP BY sec.sector",
    );
    assert_rows_unordered(
        "join + group by",
        rows,
        vec![
            vec![s("bank"), i64(8), i64(5100)],
            vec![s("realestate"), i64(3), i64(1000)],
        ],
    );
}

#[test]
fn subqueries_in_where() {
    let (_temp_dir, engine, mut session) = create_engine();
    seed_quote_tables(&engine, &mut session);

    let rows = query(
        &engine,
        &mut session,
        "SELECT COUNT(*) FROM t_quote WHERE security_id IN
            (SELECT security_id FROM t_security WHERE sector = 'bank')",
    );
    assert_eq!(rows, vec![vec![i64(8)]], "IN subquery");

    let rows = query(
        &engine,
        &mut session,
        "SELECT COUNT(*) FROM t_quote WHERE security_id NOT IN
            (SELECT security_id FROM t_security WHERE sector = 'bank')",
    );
    assert_eq!(rows, vec![vec![i64(4)]], "NOT IN subquery");

    // Correlated subqueries are supported: 11 of 12 quote rows have a
    // matching security (only 688001.SH is missing from t_security).
    let rows = query(
        &engine,
        &mut session,
        "SELECT COUNT(*) FROM t_quote q WHERE EXISTS
            (SELECT 1 FROM t_security sec WHERE sec.security_id = q.security_id)",
    );
    assert_eq!(rows, vec![vec![i64(11)]], "correlated EXISTS");

    let rows = query(
        &engine,
        &mut session,
        "SELECT COUNT(*) FROM t_quote q WHERE NOT EXISTS
            (SELECT 1 FROM t_security sec WHERE sec.security_id = q.security_id)",
    );
    assert_eq!(rows, vec![vec![i64(1)]], "correlated NOT EXISTS");

    let rows = query(
        &engine,
        &mut session,
        "SELECT COUNT(*) FROM t_quote WHERE price >
            (SELECT AVG(price) FROM t_quote)",
    );
    assert_eq!(rows, vec![vec![i64(3)]], "scalar subquery comparison");
}

#[test]
fn derived_table_basic_count() {
    let (_temp_dir, engine, mut session) = create_engine();
    seed_quote_tables(&engine, &mut session);

    // FROM derived tables are fully supported (previously a planner error).
    let rows = query(
        &engine,
        &mut session,
        "SELECT COUNT(*) FROM (SELECT security_id FROM t_quote) sub",
    );
    assert_eq!(rows, vec![vec![i64(12)]], "derived table count");
}

#[test]
fn union_and_union_all() {
    let (_temp_dir, engine, mut session) = create_engine();
    seed_quote_tables(&engine, &mut session);

    let rows = query(
        &engine,
        &mut session,
        "SELECT security_id FROM t_quote WHERE deal_date = 20260904
         UNION
         SELECT security_id FROM t_quote WHERE deal_date = 20260905
         ORDER BY security_id",
    );
    assert_eq!(
        rows,
        vec![
            vec![s("000001.SZ")],
            vec![s("000002.SZ")],
            vec![s("600000.SH")],
            vec![s("688001.SH")],
        ],
        "UNION deduplicates"
    );

    let rows = query(
        &engine,
        &mut session,
        "SELECT security_id FROM t_quote WHERE deal_date = 20260904
         UNION ALL
         SELECT security_id FROM t_quote WHERE deal_date = 20260905",
    );
    assert_eq!(rows.len(), 12, "UNION ALL keeps duplicates");
}

#[test]
fn case_expressions() {
    let (_temp_dir, engine, mut session) = create_engine();
    seed_quote_tables(&engine, &mut session);

    let rows = query(
        &engine,
        &mut session,
        "SELECT deal_time,
            CASE WHEN price >= 20.0 THEN 'high' WHEN price >= 6.0 THEN 'mid' ELSE 'low' END band
         FROM t_quote WHERE security_id = '000002.SZ' ORDER BY deal_time",
    );
    assert_eq!(
        rows,
        vec![
            vec![i32(930), s("high")],
            vec![i32(930), s("high")],
            vec![i32(1500), s("low")],
        ],
        "searched CASE: NULL price fails every WHEN comparison -> ELSE 'low'"
    );

    let rows = query(
        &engine,
        &mut session,
        "SELECT CASE security_id WHEN '000001.SZ' THEN 1 WHEN '000002.SZ' THEN 2 END tag
         FROM t_quote GROUP BY security_id ORDER BY security_id",
    );
    assert_eq!(
        rows,
        vec![
            vec![i64(1)],
            vec![i64(2)],
            vec![ScalarValue::Int64(None)],
            vec![ScalarValue::Int64(None)],
        ],
        "simple CASE without ELSE yields NULL"
    );
}

#[test]
fn scalar_functions_in_projection_and_where() {
    let (_temp_dir, engine, mut session) = create_engine();
    seed_quote_tables(&engine, &mut session);

    let rows = query(
        &engine,
        &mut session,
        "SELECT UPPER(security_id), LENGTH(sector) FROM t_security ORDER BY security_id",
    );
    assert_eq!(rows[0][0], s("000001.SZ"), "UPPER leaves ids unchanged");
    assert_eq!(rows[1][1], i64(10), "LENGTH('realestate')");

    let rows = query(
        &engine,
        &mut session,
        "SELECT COUNT(*) FROM t_quote WHERE COALESCE(price, 0.0) < 6.0",
    );
    assert_eq!(
        rows,
        vec![vec![i64(4)]],
        "COALESCE in WHERE (5.25, 5.5, 5.75, and NULL->0)"
    );

    let rows = query(
        &engine,
        &mut session,
        "SELECT ABS(-5), ROUND(2.567, 2) FROM t_security WHERE security_id = '000001.SZ'",
    );
    assert_eq!(rows, vec![vec![f64v(5.0), f64v(2.57)]], "ABS and ROUND");
}

#[test]
fn window_functions_fail_loudly_with_group_by() {
    let (_temp_dir, engine, mut session) = create_engine();
    seed_quote_tables(&engine, &mut session);

    // Window functions over aggregated inputs are not supported yet; they
    // must fail loudly instead of silently returning wrong data.
    let message = query_err(
        &engine,
        &mut session,
        "SELECT security_id, COUNT(*), ROW_NUMBER() OVER (PARTITION BY security_id) rn
         FROM t_quote GROUP BY security_id",
    );
    assert!(
        !message.is_empty(),
        "window functions combined with GROUP BY must fail loudly"
    );

    let message = query_err(
        &engine,
        &mut session,
        "SELECT security_id + ROW_NUMBER() OVER (ORDER BY deal_time) FROM t_quote",
    );
    assert!(
        !message.is_empty(),
        "nested window expressions must fail loudly"
    );
}

#[test]
fn literal_select_without_from() {
    let (_temp_dir, engine, mut session) = create_engine();
    seed_quote_tables(&engine, &mut session);

    let rows = query(&engine, &mut session, "SELECT 1 + 2 AS three, 'txt' AS tag");
    assert_eq!(rows, vec![vec![i64(3), s("txt")]], "no-FROM literal select");
}

#[test]
fn two_sums_on_different_columns() {
    let (_temp_dir, engine, mut session) = create_engine();
    seed_quote_tables(&engine, &mut session);

    let rows = query(
        &engine,
        &mut session,
        "SELECT SUM(price), SUM(volume) FROM t_quote",
    );
    assert_eq!(
        rows,
        vec![vec![f64v(151.75), i64(7300)]],
        "two SUM aggregates"
    );
}

#[test]
fn sum_returns_null_for_empty_input_via_metadata_path() {
    let (_dir, engine, mut session) = create_engine();
    engine
        .execute(&mut session, "CREATE TABLE t (v INT64)")
        .expect("create t");
    engine
        .execute(&mut session, "INSERT INTO t VALUES (1),(2),(3)")
        .expect("insert t");

    // SQL semantics: SUM over an empty set is NULL, not 0. The metadata
    // aggregate fast path must match the generic accumulator here.
    assert_eq!(
        query(&engine, &mut session, "SELECT SUM(v) FROM t WHERE v > 1000"),
        vec![vec![ScalarValue::Int64(None)]]
    );

    engine
        .execute(&mut session, "CREATE TABLE n (v INT64)")
        .expect("create n");
    engine
        .execute(&mut session, "INSERT INTO n VALUES (NULL),(NULL)")
        .expect("insert n");
    assert_eq!(
        query(&engine, &mut session, "SELECT SUM(v) FROM n"),
        vec![vec![ScalarValue::Int64(None)]]
    );

    engine
        .execute(&mut session, "CREATE TABLE e (v INT64)")
        .expect("create e");
    assert_eq!(
        query(&engine, &mut session, "SELECT SUM(v) FROM e"),
        vec![vec![ScalarValue::Int64(None)]]
    );
}
