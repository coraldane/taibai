//! End-to-end coverage for CTEs (WITH) and set operations
//! (UNION / INTERSECT / EXCEPT), including operator precedence and the
//! trailing ORDER BY / LIMIT / OFFSET placement on set-operation chains.

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

fn i64(v: i64) -> ScalarValue {
    ScalarValue::Int64(Some(v))
}

fn s(v: &str) -> ScalarValue {
    ScalarValue::VarChar(Some(v.to_string()))
}

fn seed(engine: &Engine, session: &mut Session) {
    engine
        .execute(
            session,
            "CREATE TABLE nums (id INT64, region VARCHAR, amount INT64)",
        )
        .unwrap();
    engine
        .execute(
            session,
            "INSERT INTO nums (id, region, amount) VALUES
             (1, 'east', 100), (2, 'east', 200), (3, 'west', 50),
             (4, 'west', 75), (5, 'north', 300)",
        )
        .unwrap();
}

#[test]
fn cte_basic_and_cte_chains() {
    let (_dir, engine, mut session) = create_engine();
    seed(&engine, &mut session);

    // Basic: single CTE with filter + projection.
    let rows = query(
        &engine,
        &mut session,
        "WITH big AS (SELECT id, amount FROM nums WHERE amount >= 100)
         SELECT id FROM big ORDER BY id",
    );
    assert_eq!(rows, vec![vec![i64(1)], vec![i64(2)], vec![i64(5)]]);

    // A later CTE referencing an earlier one.
    let rows = query(
        &engine,
        &mut session,
        "WITH east AS (SELECT id, amount FROM nums WHERE region = 'east'),
             east_total AS (SELECT SUM(amount) AS total FROM east)
         SELECT total FROM east_total",
    );
    assert_eq!(rows, vec![vec![i64(300)]]);

    // CTE joined with a base table (name shadowing: the CTE wins only for
    // its own name; other tables still resolve normally).
    let rows = query(
        &engine,
        &mut session,
        "WITH big AS (SELECT id FROM nums WHERE amount >= 100)
         SELECT n.region, COUNT(*) AS c
         FROM nums n JOIN big ON n.id = big.id
         GROUP BY n.region ORDER BY n.region",
    );
    assert_eq!(
        rows,
        vec![vec![s("east"), i64(2)], vec![s("north"), i64(1)]]
    );
}

#[test]
fn cte_inside_subqueries_and_placeholders() {
    let (_dir, engine, mut session) = create_engine();
    seed(&engine, &mut session);

    // CTE referenced from an IN subquery.
    let rows = query(
        &engine,
        &mut session,
        "WITH big AS (SELECT id FROM nums WHERE amount >= 100)
         SELECT id FROM nums WHERE id IN (SELECT id FROM big) ORDER BY id",
    );
    assert_eq!(rows, vec![vec![i64(1)], vec![i64(2)], vec![i64(5)]]);

    // CTE + UNION in the main body + prepared-style ? placeholder binding.
    let sql = "WITH east AS (SELECT id FROM nums WHERE region = ?)
               SELECT id FROM east UNION ALL SELECT id FROM east ORDER BY id";
    let stmts = taibai::sql::parser::parser::Parser::parse(sql).expect("parse");
    let mut stmt = stmts.into_iter().next().expect("statement");
    taibai::sql::execution::prepared::bind_params(
        &mut stmt,
        &[ScalarValue::VarChar(Some("west".into()))],
    )
    .expect("bind");
    assert!(
        !taibai::sql::execution::prepared::has_placeholders(&stmt),
        "CTE placeholder not bound"
    );
}

#[test]
fn cte_rejects_recursive_and_unknown_table_still_errors() {
    let (_dir, engine, mut session) = create_engine();
    seed(&engine, &mut session);

    let err = query_err(
        &engine,
        &mut session,
        "WITH RECURSIVE t AS (SELECT 1) SELECT * FROM t",
    );
    assert!(err.contains("RECURSIVE"), "unexpected error: {err}");

    // A CTE does not exist under a database qualifier.
    let err = query_err(
        &engine,
        &mut session,
        "WITH big AS (SELECT 1 AS x) SELECT * FROM main.big",
    );
    assert!(
        err.contains("does not exist") || err.contains("not found"),
        "unexpected error: {err}"
    );
}

#[test]
fn intersect_and_except_set_semantics() {
    let (_dir, engine, mut session) = create_engine();
    seed(&engine, &mut session);

    // INTERSECT: rows in both operands (deduplicated).
    let rows = query(
        &engine,
        &mut session,
        "SELECT region FROM nums WHERE amount >= 100
         INTERSECT
         SELECT region FROM nums WHERE region = 'east' ORDER BY region",
    );
    assert_eq!(rows, vec![vec![s("east")]]);

    // INTERSECT deduplicates even when both sides repeat a row.
    let rows = query(
        &engine,
        &mut session,
        "SELECT region FROM nums WHERE region = 'east'
         INTERSECT
         SELECT region FROM nums WHERE amount > 0",
    );
    assert_eq!(rows, vec![vec![s("east")]]);

    // EXCEPT: left rows missing from the right operand.
    let rows = query(
        &engine,
        &mut session,
        "SELECT region FROM nums
         EXCEPT
         SELECT region FROM nums WHERE region = 'west' ORDER BY region",
    );
    assert_eq!(rows, vec![vec![s("east")], vec![s("north")]]);

    // EXCEPT deduplicates the left side.
    let rows = query(
        &engine,
        &mut session,
        "SELECT region FROM nums WHERE region = 'east'
         EXCEPT
         SELECT region FROM nums WHERE region = 'west'",
    );
    assert_eq!(rows, vec![vec![s("east")]]);

    // INTERSECT / EXCEPT ALL is rejected loudly.
    let err = query_err(
        &engine,
        &mut session,
        "SELECT region FROM nums INTERSECT ALL SELECT region FROM nums",
    );
    assert!(err.contains("INTERSECT ALL"), "unexpected error: {err}");
}

#[test]
fn set_op_precedence_matches_postgresql() {
    let (_dir, engine, mut session) = create_engine();
    seed(&engine, &mut session);

    // INTERSECT binds tighter: `a UNION b INTERSECT c` == `a UNION (b INTERSECT c)`.
    // a = amount>=100 = {1,2,5}; b = east = {1,2}; c = amount<=200 = {1,2,3,4};
    // b ∩ c = {1,2}; a ∪ (b ∩ c) = {1,2,5}.
    let rows = query(
        &engine,
        &mut session,
        "SELECT id FROM nums WHERE amount >= 100
         UNION
         SELECT id FROM nums WHERE region = 'east'
         INTERSECT
         SELECT id FROM nums WHERE amount <= 200
         ORDER BY id",
    );
    assert_eq!(rows, vec![vec![i64(1)], vec![i64(2)], vec![i64(5)]]);

    // Contrast: with parentheses the intersect binds the LEFT side instead,
    // (a ∩ b) ∪ c = {1,2} ∪ {1,2,3,4} = {1,2,3,4}.
    let rows2 = query(
        &engine,
        &mut session,
        "(SELECT id FROM nums WHERE amount >= 100
          INTERSECT
          SELECT id FROM nums WHERE region = 'east')
         UNION
         SELECT id FROM nums WHERE amount <= 200
         ORDER BY id",
    );
    assert_eq!(
        rows2,
        vec![
            vec![i64(1)],
            vec![i64(2)],
            vec![i64(3)],
            vec![i64(4)]
        ]
    );

    // UNION and EXCEPT are left-associative: `a UNION b EXCEPT c`
    // == `(a UNION b) EXCEPT c`.
    let rows = query(
        &engine,
        &mut session,
        "SELECT id FROM nums WHERE region = 'east'
         UNION
         SELECT id FROM nums WHERE region = 'west'
         EXCEPT
         SELECT id FROM nums WHERE amount >= 100
         ORDER BY id",
    );
    // (east ∪ west) = {1,2,3,4}; minus big {1,2,5} → {3,4}.
    assert_eq!(rows, vec![vec![i64(3)], vec![i64(4)]]);
}

#[test]
fn union_trailing_order_by_and_limit_apply_to_whole_chain() {
    let (_dir, engine, mut session) = create_engine();
    seed(&engine, &mut session);

    // Regression: trailing clauses used to attach to the last operand's own
    // SELECT (no global sort/limit). They must apply to the combined rows.
    // east ∪ west = {1,2,3,4}; ORDER BY id DESC LIMIT 2 → [4,3].
    let rows = query(
        &engine,
        &mut session,
        "SELECT id FROM nums WHERE region = 'east'
         UNION
         SELECT id FROM nums WHERE region = 'west'
         ORDER BY id DESC LIMIT 2",
    );
    assert_eq!(rows, vec![vec![i64(4)], vec![i64(3)]]);

    // ORDER BY id LIMIT 2 OFFSET 1 → [1,2,3,4] → offset [2,3,4] → limit [2,3].
    let rows = query(
        &engine,
        &mut session,
        "SELECT id FROM nums WHERE region = 'east'
         UNION
         SELECT id FROM nums WHERE region = 'west'
         ORDER BY id LIMIT 2 OFFSET 1",
    );
    assert_eq!(rows, vec![vec![i64(2)], vec![i64(3)]]);
}
