//! Integration tests for FROM derived tables and correlated subqueries.
//!
//! Covers the two subquery shapes documented as supported in
//! `docs/user-guide/limitations.md` (功能边界).

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

fn fmt_cell(value: &ScalarValue) -> String {
    match value {
        ScalarValue::Float64(Some(v)) => format!("f:{v:.6}"),
        ScalarValue::Int64(Some(v)) => format!("i:{v}"),
        ScalarValue::Int32(Some(v)) => format!("i32:{v}"),
        ScalarValue::VarChar(Some(v)) => format!("s:{v}"),
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

fn assert_sorted_eq(engine: &Engine, session: &mut Session, sql: &str, expected: &[&str]) {
    let rows = query(engine, session, sql);
    let rendered = sorted_rows(rows);
    let expected: Vec<String> = expected.iter().map(|s| s.to_string()).collect();
    assert_eq!(rendered, expected, "unexpected result for: {sql}");
}

fn setup(engine: &Engine, session: &mut Session) {
    engine
        .execute(
            session,
            "CREATE TABLE products (id INT PRIMARY KEY, name VARCHAR(20), category VARCHAR(10), price DOUBLE)",
        )
        .expect("create products");
    engine
        .execute(
            session,
            "CREATE TABLE orders (id INT PRIMARY KEY, product_id INT, qty INT)",
        )
        .expect("create orders");
    for (id, name, category, price) in [
        (1, "apple", "fruit", 3.0),
        (2, "banana", "fruit", 2.0),
        (3, "carrot", "veg", 1.5),
        (4, "date", "fruit", 8.0),
        (5, "eggplant", "veg", 4.0),
    ] {
        engine
            .execute(
                session,
                &format!("INSERT INTO products VALUES ({id}, '{name}', '{category}', {price})"),
            )
            .expect("insert product");
    }
    for (id, product_id, qty) in [(1, 1, 10), (2, 2, 20), (3, 1, 5), (4, 3, 7), (5, 5, 2)] {
        engine
            .execute(
                session,
                &format!("INSERT INTO orders VALUES ({id}, {product_id}, {qty})"),
            )
            .expect("insert order");
    }
}

// ---------------------------------------------------------------------------
// FROM derived tables
// ---------------------------------------------------------------------------

#[test]
fn derived_table_basic_select() {
    let (_dir, engine, mut session) = create_engine();
    setup(&engine, &mut session);

    assert_sorted_eq(
        &engine,
        &mut session,
        "SELECT d.name FROM (SELECT name, price FROM products WHERE price > 2.0) d ORDER BY d.name",
        &["s:apple", "s:date", "s:eggplant"],
    );
}

#[test]
fn derived_table_with_where_and_aggregation() {
    let (_dir, engine, mut session) = create_engine();
    setup(&engine, &mut session);

    // Filter the derived output further.
    assert_sorted_eq(
        &engine,
        &mut session,
        "SELECT t.category FROM (SELECT category, price FROM products) t WHERE t.price < 2.0",
        &["s:veg"],
    );

    // Aggregate over a derived table.
    assert_sorted_eq(
        &engine,
        &mut session,
        "SELECT COUNT(*) FROM (SELECT DISTINCT category FROM products) c",
        &["i:2"],
    );

    // Derived table producing an aggregate, then filtered/order outside.
    assert_sorted_eq(
        &engine,
        &mut session,
        "SELECT g.category, g.total FROM \
         (SELECT category, SUM(price) AS total FROM products GROUP BY category) g \
         WHERE g.total > 10 ORDER BY g.category",
        &["s:fruit,f:13.000000"],
    );
}

#[test]
fn derived_table_join() {
    let (_dir, engine, mut session) = create_engine();
    setup(&engine, &mut session);

    assert_sorted_eq(
        &engine,
        &mut session,
        "SELECT p.name, o.total \
         FROM products p \
         JOIN (SELECT product_id, SUM(qty) AS total FROM orders GROUP BY product_id) o \
         ON p.id = o.product_id \
         ORDER BY p.name",
        &[
            "s:apple,i:15",
            "s:banana,i:20",
            "s:carrot,i:7",
            "s:eggplant,i:2",
        ],
    );
}

#[test]
fn derived_table_nested() {
    let (_dir, engine, mut session) = create_engine();
    setup(&engine, &mut session);

    assert_sorted_eq(
        &engine,
        &mut session,
        "SELECT inner_cat FROM \
         (SELECT category AS inner_cat, price FROM \
          (SELECT category, price FROM products WHERE price > 1.0) mid \
          WHERE price < 4.0) outer_d \
         ORDER BY inner_cat",
        &["s:fruit", "s:fruit", "s:veg"],
    );
}

#[test]
fn derived_table_in_union_branch() {
    let (_dir, engine, mut session) = create_engine();
    setup(&engine, &mut session);

    assert_sorted_eq(
        &engine,
        &mut session,
        "SELECT name FROM (SELECT name FROM products WHERE category = 'veg') v \
         UNION ALL \
         SELECT name FROM (SELECT name FROM products WHERE price > 7) e \
         ORDER BY name",
        &["s:carrot", "s:date", "s:eggplant"],
    );
}

// ---------------------------------------------------------------------------
// Correlated subqueries
// ---------------------------------------------------------------------------

#[test]
fn correlated_exists() {
    let (_dir, engine, mut session) = create_engine();
    setup(&engine, &mut session);

    assert_sorted_eq(
        &engine,
        &mut session,
        "SELECT p.name FROM products p \
         WHERE EXISTS (SELECT 1 FROM orders o WHERE o.product_id = p.id) \
         ORDER BY p.name",
        &["s:apple", "s:banana", "s:carrot", "s:eggplant"],
    );
}

#[test]
fn correlated_not_exists() {
    let (_dir, engine, mut session) = create_engine();
    setup(&engine, &mut session);

    assert_sorted_eq(
        &engine,
        &mut session,
        "SELECT p.name FROM products p \
         WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.product_id = p.id) \
         ORDER BY p.name",
        &["s:date"],
    );
}

#[test]
fn correlated_exists_with_local_predicate() {
    let (_dir, engine, mut session) = create_engine();
    setup(&engine, &mut session);

    assert_sorted_eq(
        &engine,
        &mut session,
        "SELECT p.name FROM products p \
         WHERE EXISTS (SELECT 1 FROM orders o WHERE o.product_id = p.id AND o.qty > 6) \
         ORDER BY p.name",
        &["s:apple", "s:banana", "s:carrot"],
    );
}

#[test]
fn correlated_in() {
    let (_dir, engine, mut session) = create_engine();
    setup(&engine, &mut session);

    assert_sorted_eq(
        &engine,
        &mut session,
        "SELECT p.name FROM products p \
         WHERE p.id IN (SELECT o.product_id FROM orders o WHERE o.qty > p.price * 2) \
         ORDER BY p.name",
        &["s:apple", "s:banana", "s:carrot"],
    );
}

#[test]
fn correlated_scalar_comparison() {
    let (_dir, engine, mut session) = create_engine();
    setup(&engine, &mut session);

    // Products priced above the average price of their own category:
    // fruit avg = 13/3 ≈ 4.33 → only date(8); veg avg = 2.75 → only eggplant(4).
    assert_sorted_eq(
        &engine,
        &mut session,
        "SELECT p.name FROM products p \
         WHERE p.price > (SELECT AVG(p2.price) FROM products p2 WHERE p2.category = p.category) \
         ORDER BY p.name",
        &["s:date", "s:eggplant"],
    );
}

#[test]
fn correlated_scalar_in_select_list() {
    let (_dir, engine, mut session) = create_engine();
    setup(&engine, &mut session);

    // For each product, the total ordered quantity; SUM over no rows is NULL.
    assert_sorted_eq(
        &engine,
        &mut session,
        "SELECT p.name, (SELECT SUM(o.qty) FROM orders o WHERE o.product_id = p.id) AS total \
         FROM products p ORDER BY p.name",
        &[
            "s:apple,i:15",
            "s:banana,i:20",
            "s:carrot,i:7",
            "s:date,Int64(None)",
            "s:eggplant,i:2",
        ],
    );
}

#[test]
fn correlated_exists_with_or_and_not() {
    let (_dir, engine, mut session) = create_engine();
    setup(&engine, &mut session);

    assert_sorted_eq(
        &engine,
        &mut session,
        "SELECT p.name FROM products p \
         WHERE p.price > 7 OR EXISTS (SELECT 1 FROM orders o WHERE o.product_id = p.id AND o.qty > 15) \
         ORDER BY p.name",
        &["s:banana", "s:date"],
    );

    assert_sorted_eq(
        &engine,
        &mut session,
        "SELECT p.name FROM products p \
         WHERE NOT (EXISTS (SELECT 1 FROM orders o WHERE o.product_id = p.id) AND p.category = 'veg') \
         ORDER BY p.name",
        &["s:apple", "s:banana", "s:date"],
    );
}

// ---------------------------------------------------------------------------
// Errors stay loud
// ---------------------------------------------------------------------------

#[test]
fn derived_table_requires_alias_at_parse() {
    let (_dir, engine, mut session) = create_engine();
    setup(&engine, &mut session);
    let err = query_err_string(
        &engine,
        &mut session,
        "SELECT name FROM (SELECT name FROM products)",
    );
    assert!(err.contains("alias"), "expected alias error, got: {err}");
}

#[test]
fn unknown_outer_reference_is_a_clear_error() {
    let (_dir, engine, mut session) = create_engine();
    setup(&engine, &mut session);
    let err = query_err_string(
        &engine,
        &mut session,
        "SELECT p.name FROM products p WHERE EXISTS (SELECT 1 FROM orders o WHERE o.qty = nope.nope)",
    );
    assert!(
        err.contains("nope") || err.contains("unknown"),
        "expected clear resolution error, got: {err}"
    );
}

fn query_err_string(engine: &Engine, session: &mut Session, sql: &str) -> String {
    match engine.execute(session, sql) {
        Err(error) => error.to_string(),
        Ok(result) => panic!("expected error for: {sql}\ngot: {:?}", collect_rows(result)),
    }
}

// ---------------------------------------------------------------------------
// Combined / edge cases
// ---------------------------------------------------------------------------

#[test]
fn derived_table_referenced_by_correlated_subquery() {
    let (_dir, engine, mut session) = create_engine();
    setup(&engine, &mut session);

    // The derived table's column feeds the correlated comparison.
    assert_sorted_eq(
        &engine,
        &mut session,
        "SELECT d.name FROM \
         (SELECT id, name, price FROM products WHERE price >= 2.0) d \
         WHERE EXISTS (SELECT 1 FROM orders o WHERE o.product_id = d.id) \
         ORDER BY d.name",
        &["s:apple", "s:banana", "s:eggplant"],
    );
}

#[test]
fn correlated_subquery_with_inner_order_and_limit() {
    let (_dir, engine, mut session) = create_engine();
    setup(&engine, &mut session);

    // Newest order per product: per-row execution keeps LIMIT semantics.
    assert_sorted_eq(
        &engine,
        &mut session,
        "SELECT p.name, \
         (SELECT o.qty FROM orders o WHERE o.product_id = p.id ORDER BY o.id DESC LIMIT 1) AS latest \
         FROM products p WHERE p.id <= 3 ORDER BY p.name",
        &["s:apple,i:5", "s:banana,i:20", "s:carrot,i:7"],
    );
}

#[test]
fn two_correlated_subqueries_in_one_predicate() {
    let (_dir, engine, mut session) = create_engine();
    setup(&engine, &mut session);

    // apple: max qty 10 / min 5 = 2.0; banana: 20/20 = 1.0; carrot: 7/7 = 1.0.
    assert_sorted_eq(
        &engine,
        &mut session,
        "SELECT p.name FROM products p \
         WHERE (SELECT MAX(o.qty) FROM orders o WHERE o.product_id = p.id) > \
               (SELECT MIN(o.qty) FROM orders o WHERE o.product_id = p.id) * 1.5 \
         ORDER BY p.name",
        &["s:apple"],
    );
}

#[test]
fn correlated_scalar_type_survives_group_by_outer() {
    let (_dir, engine, mut session) = create_engine();
    setup(&engine, &mut session);

    // Correlated scalar inside an outer aggregate expression.
    assert_sorted_eq(
        &engine,
        &mut session,
        "SELECT p.category, SUM((SELECT COUNT(*) FROM orders o WHERE o.product_id = p.id)) AS n \
         FROM products p GROUP BY p.category ORDER BY p.category",
        &["s:fruit,i:3", "s:veg,i:2"],
    );
}

#[test]
fn uncorrelated_subquery_still_eager() {
    let (_dir, engine, mut session) = create_engine();
    setup(&engine, &mut session);

    // Baseline: uncorrelated subqueries keep working everywhere.
    assert_sorted_eq(
        &engine,
        &mut session,
        "SELECT name FROM products WHERE price = (SELECT MAX(price) FROM products)",
        &["s:date"],
    );

    assert_sorted_eq(
        &engine,
        &mut session,
        "SELECT name FROM products WHERE id NOT IN (SELECT product_id FROM orders) ORDER BY name",
        &["s:date"],
    );

    // No-FROM select with a subquery.
    assert_sorted_eq(
        &engine,
        &mut session,
        "SELECT (SELECT COUNT(*) FROM products) AS n",
        &["i:5"],
    );
}

// ---------------------------------------------------------------------------
// Semi-join decorrelation (plan shape + self-table cases)
// ---------------------------------------------------------------------------

#[test]
fn self_table_correlated_exists() {
    let (_dir, engine, mut session) = create_engine();
    setup(&engine, &mut session);

    // Orders sharing a product with a different order (excludes self-match).
    // orders: (1,p1) (2,p2) (3,p1) (4,p3) (5,p5) → ids 1 and 3 qualify.
    assert_sorted_eq(
        &engine,
        &mut session,
        "SELECT o1.id FROM orders o1 \
         WHERE EXISTS (SELECT 1 FROM orders o2 \
                       WHERE o2.product_id = o1.product_id AND o2.id <> o1.id) \
         ORDER BY o1.id",
        &["i32:1", "i32:3"],
    );

    // Anti variant: products ordered exactly once.
    assert_sorted_eq(
        &engine,
        &mut session,
        "SELECT o1.id FROM orders o1 \
         WHERE NOT EXISTS (SELECT 1 FROM orders o2 \
                           WHERE o2.product_id = o1.product_id AND o2.id <> o1.id) \
         ORDER BY o1.id",
        &["i32:2", "i32:4", "i32:5"],
    );
}

#[test]
fn explain_shows_semi_join_for_decorrelatable_exists() {
    let (_dir, engine, mut session) = create_engine();
    setup(&engine, &mut session);

    let plan = explain(
        &engine,
        &mut session,
        "SELECT p.name FROM products p \
         WHERE EXISTS (SELECT 1 FROM orders o WHERE o.product_id = p.id)",
    );
    assert!(
        plan.contains("SemiJoin"),
        "expected a decorrelated plan, got:\n{plan}"
    );

    // OR-wrapped EXISTS cannot decorrelate: stays as a per-row marker.
    let plan = explain(
        &engine,
        &mut session,
        "SELECT p.name FROM products p \
         WHERE p.price > 7 OR EXISTS (SELECT 1 FROM orders o WHERE o.product_id = p.id)",
    );
    assert!(
        plan.contains("correlated subquery"),
        "expected a per-row correlated plan, got:\n{plan}"
    );
}

fn explain(engine: &Engine, session: &mut Session, sql: &str) -> String {
    let result = engine
        .execute(session, &format!("EXPLAIN {sql}"))
        .unwrap_or_else(|error| panic!("EXPLAIN failed: {sql}\n{error}"));
    let ExecResult::Query { batches, .. } = result else {
        panic!("expected query result");
    };
    let mut out = String::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            if let Some(ScalarValue::VarChar(Some(line))) = batch.scalar(row, 0) {
                out.push_str(&line);
                out.push('\n');
            }
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Scalar decorrelation (LEFT JOIN + group by correlation keys)
// ---------------------------------------------------------------------------

#[test]
fn scalar_count_decorrelated_zero_vs_null() {
    let (_dir, engine, mut session) = create_engine();
    setup(&engine, &mut session);

    // COUNT over an empty correlation group must be 0 (not NULL): every
    // product satisfies >= 0, including 'date' which has no orders.
    assert_sorted_eq(
        &engine,
        &mut session,
        "SELECT p.name FROM products p \
         WHERE (SELECT COUNT(*) FROM orders o WHERE o.product_id = p.id) >= 0 \
         ORDER BY p.name",
        &["s:apple", "s:banana", "s:carrot", "s:date", "s:eggplant"],
    );

    // ...and > 1 selects exactly the product with two orders.
    assert_sorted_eq(
        &engine,
        &mut session,
        "SELECT p.name FROM products p \
         WHERE (SELECT COUNT(*) FROM orders o WHERE o.product_id = p.id) > 1",
        &["s:apple"],
    );
}

#[test]
fn scalar_decorrelation_under_not() {
    let (_dir, engine, mut session) = create_engine();
    setup(&engine, &mut session);

    // NOT of the decorrelatable comparison: exact complement of
    // correlated_scalar_comparison's result.
    assert_sorted_eq(
        &engine,
        &mut session,
        "SELECT p.name FROM products p \
         WHERE NOT (p.price > \
                    (SELECT AVG(p2.price) FROM products p2 WHERE p2.category = p.category)) \
         ORDER BY p.name",
        &["s:apple", "s:banana", "s:carrot"],
    );
}

#[test]
fn explain_scalar_decorrelation_removes_marker() {
    let (_dir, engine, mut session) = create_engine();
    setup(&engine, &mut session);

    let plan = explain(
        &engine,
        &mut session,
        "SELECT p.name FROM products p \
         WHERE p.price > (SELECT AVG(p2.price) FROM products p2 WHERE p2.category = p.category)",
    );
    assert!(
        plan.contains("Join"),
        "expected a join-based plan, got:\n{plan}"
    );
    assert!(
        !plan.contains("correlated subquery"),
        "scalar marker should be gone, got:\n{plan}"
    );
}

// ---------------------------------------------------------------------------
// Aggregate EXISTS / IN decorrelation (group by correlation keys)
// ---------------------------------------------------------------------------

#[test]
fn correlated_exists_with_having_decorrelates() {
    let (_dir, engine, mut session) = create_engine();
    setup(&engine, &mut session);

    // Per-product order counts: p1 -> 2, p2/p3/p5 -> 1, p4 -> 0.
    assert_sorted_eq(
        &engine,
        &mut session,
        "SELECT p.name FROM products p \
         WHERE EXISTS (SELECT 1 FROM orders o WHERE o.product_id = p.id \
                       HAVING COUNT(*) > 1) \
         ORDER BY p.name",
        &["s:apple"],
    );

    assert_sorted_eq(
        &engine,
        &mut session,
        "SELECT p.name FROM products p \
         WHERE EXISTS (SELECT 1 FROM orders o WHERE o.product_id = p.id \
                       HAVING COUNT(*) > 5)",
        &[],
    );
}

#[test]
fn correlated_in_with_aggregate_decorrelates() {
    let (_dir, engine, mut session) = create_engine();
    setup(&engine, &mut session);

    // Per-product MAX(qty): p1 -> 10, p2 -> 20, p3 -> 7, p5 -> 2.
    // p.id * 10 matches for apple (1*10=10) and banana (2*10=20).
    assert_sorted_eq(
        &engine,
        &mut session,
        "SELECT p.name FROM products p \
         WHERE p.id * 10 IN (SELECT MAX(o.qty) FROM orders o WHERE o.product_id = p.id) \
         ORDER BY p.name",
        &["s:apple", "s:banana"],
    );

    // Anti variant: carrot (30 vs 7), date (no group), eggplant (50 vs 2).
    assert_sorted_eq(
        &engine,
        &mut session,
        "SELECT p.name FROM products p \
         WHERE p.id * 10 NOT IN (SELECT MAX(o.qty) FROM orders o WHERE o.product_id = p.id) \
         ORDER BY p.name",
        &["s:carrot", "s:date", "s:eggplant"],
    );
}

#[test]
fn explain_aggregate_exists_shows_semi_join() {
    let (_dir, engine, mut session) = create_engine();
    setup(&engine, &mut session);

    let plan = explain(
        &engine,
        &mut session,
        "SELECT p.name FROM products p \
         WHERE EXISTS (SELECT 1 FROM orders o WHERE o.product_id = p.id \
                       HAVING COUNT(*) > 1)",
    );
    assert!(
        plan.contains("SemiJoin"),
        "expected semi join, got:\n{plan}"
    );
    assert!(
        !plan.contains("correlated subquery"),
        "marker should be gone, got:\n{plan}"
    );
}

// ---------------------------------------------------------------------------
// EXPLAIN ANALYZE with subqueries / derived tables
// ---------------------------------------------------------------------------

#[test]
fn explain_analyze_handles_subquery_features() {
    let (_dir, engine, mut session) = create_engine();
    setup(&engine, &mut session);

    let cases = [
        // Derived table.
        "SELECT COUNT(*) FROM (SELECT name, price FROM products WHERE price > 2.0) d",
        // Decorrelated EXISTS.
        "SELECT p.name FROM products p \
         WHERE EXISTS (SELECT 1 FROM orders o WHERE o.product_id = p.id)",
        // Decorrelated scalar.
        "SELECT p.name FROM products p \
         WHERE p.price > (SELECT AVG(p2.price) FROM products p2 WHERE p2.category = p.category)",
        // Aggregate EXISTS.
        "SELECT p.name FROM products p \
         WHERE EXISTS (SELECT 1 FROM orders o WHERE o.product_id = p.id HAVING COUNT(*) > 1)",
        // NOT decorrelatable (OR) — per-row path under the profiled pipeline.
        "SELECT p.name FROM products p \
         WHERE p.price > 7 OR EXISTS (SELECT 1 FROM orders o WHERE o.product_id = p.id)",
    ];
    for sql in cases {
        let result = engine
            .execute(&mut session, &format!("EXPLAIN ANALYZE {sql}"))
            .unwrap_or_else(|error| panic!("EXPLAIN ANALYZE failed: {sql}\n{error}"));
        let ExecResult::Query { batches, .. } = result else {
            panic!("expected query result");
        };
        let mut text = String::new();
        for batch in &batches {
            for row in 0..batch.num_rows() {
                if let Some(ScalarValue::VarChar(Some(line))) = batch.scalar(row, 0) {
                    text.push_str(&line);
                    text.push('\n');
                }
            }
        }
        assert!(
            text.contains("=== ANALYZE ==="),
            "missing analyze section for: {sql}\n{text}"
        );
        assert!(
            text.contains("rows_returned"),
            "missing row count for: {sql}\n{text}"
        );
    }
}
