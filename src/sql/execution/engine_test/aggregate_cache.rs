use super::*;

fn first_f64(result: &ExecResult, row: usize, col: usize) -> f64 {
    let ExecResult::Query { batches, .. } = result else {
        panic!("expected query result");
    };
    match batches[0].scalar(row, col).unwrap() {
        ScalarValue::Float64(Some(v)) => v,
        other => panic!("expected Float64, got {other:?}"),
    }
}

fn first_i64_ref(result: &ExecResult, row: usize, col: usize) -> i64 {
    let ExecResult::Query { batches, .. } = result else {
        panic!("expected query result");
    };
    match batches[0].scalar(row, col).unwrap() {
        ScalarValue::Int64(Some(v)) => v,
        other => panic!("expected Int64, got {other:?}"),
    }
}

/// Regression: a pure-SUM query populates the per-segment partial-sum cache
/// (Float64 columns cannot be answered from footer sums). A follow-up
/// SUM+AVG query used to hit that cache and fold the Avg accumulator with
/// empty col_sums, finalizing AVG to NULL. The mixed query must stream the
/// segment and return both aggregates exactly.
#[test]
fn sum_avg_mixed_query_survives_partial_sum_cache_hit() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();

    engine.execute(&mut session, "CREATE TABLE t (id INT64, price FLOAT64)")?;
    let mut sql = String::from("INSERT INTO t (id, price) VALUES ");
    for i in 0..100i64 {
        if i > 0 {
            sql.push_str(", ");
        }
        sql.push_str(&format!("({}, {})", i + 1, i + 1));
    }
    engine.execute(&mut session, &sql)?;
    engine.flush_table_buffers("main", "t")?;

    // Warm the partial-sum cache with a pure-SUM query.
    let sum_only = engine.execute(&mut session, "SELECT SUM(price) FROM t")?;
    assert_eq!(first_f64(&sum_only, 0, 0), 5050.0);

    // The mixed query must not answer AVG from a SUM-only cache.
    let mixed = engine.execute(&mut session, "SELECT SUM(price), AVG(price) FROM t")?;
    let sum_mixed = first_f64(&mixed, 0, 0);
    let avg_mixed = first_f64(&mixed, 0, 1);
    assert_eq!(sum_mixed, 5050.0);
    assert_eq!(avg_mixed, 50.5);

    // AVG-only queries (cold and warm) stay correct as well.
    let avg_only = engine.execute(&mut session, "SELECT AVG(price) FROM t")?;
    assert_eq!(first_f64(&avg_only, 0, 0), 50.5);
    Ok(())
}

/// Regression (P0): the metadata aggregate fast path folded zone maps
/// positionally with table-schema column indices. After ALTER ADD COLUMN,
/// pre-ALTER segments lack the new column and the fold silently skipped
/// them (COUNT/SUM under-counted, MIN/MAX missed the pinned default); after
/// DROP COLUMN every index shifted onto the wrong column. The fast path must
/// decline when a stored segment schema differs from the table schema, so
/// the regular pipeline remaps columns by name.
#[test]
fn metadata_aggregates_match_after_add_and_drop_column() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();

    // ADD COLUMN: a constant default over an old-layout segment.
    engine.execute(&mut session, "CREATE TABLE t (id INT64, v INT64)")?;
    let mut sql = String::from("INSERT INTO t (id, v) VALUES ");
    for i in 0..50i64 {
        if i > 0 {
            sql.push_str(", ");
        }
        sql.push_str(&format!("({}, {})", i + 1, i + 1));
    }
    engine.execute(&mut session, &sql)?;
    engine.flush_table_buffers("main", "t")?;
    engine.execute(
        &mut session,
        "ALTER TABLE t ADD COLUMN score INT64 DEFAULT 7",
    )?;

    let r = engine.execute(
        &mut session,
        "SELECT COUNT(score), MIN(score), MAX(score), SUM(score) FROM t",
    )?;
    assert_eq!(
        first_i64_ref(&r, 0, 0),
        50,
        "COUNT(score) must cover old rows"
    );
    assert_eq!(
        first_i64_ref(&r, 0, 1),
        7,
        "MIN(score) must see the default"
    );
    assert_eq!(
        first_i64_ref(&r, 0, 2),
        7,
        "MAX(score) must see the default"
    );
    assert_eq!(
        first_i64_ref(&r, 0, 3),
        350,
        "SUM(score) must cover old rows"
    );

    // DROP COLUMN: the dropped column sat first, so every index shifts.
    engine.execute(&mut session, "CREATE TABLE u (a INT64, b INT64, c INT64)")?;
    let mut sql = String::from("INSERT INTO u (a, b, c) VALUES ");
    for i in 0..40i64 {
        if i > 0 {
            sql.push_str(", ");
        }
        sql.push_str(&format!("({}, 5, 9)", i + 1));
    }
    engine.execute(&mut session, &sql)?;
    engine.flush_table_buffers("main", "u")?;
    engine.execute(&mut session, "ALTER TABLE u DROP COLUMN a")?;

    let r = engine.execute(
        &mut session,
        "SELECT COUNT(b), MIN(b), MAX(b), SUM(b) FROM u",
    )?;
    assert_eq!(
        first_i64_ref(&r, 0, 0),
        40,
        "COUNT(b) must not read column a"
    );
    assert_eq!(first_i64_ref(&r, 0, 1), 5, "MIN(b) must not read column a");
    assert_eq!(first_i64_ref(&r, 0, 2), 5, "MAX(b) must not read column a");
    assert_eq!(
        first_i64_ref(&r, 0, 3),
        200,
        "SUM(b) must not read column a"
    );
    Ok(())
}
