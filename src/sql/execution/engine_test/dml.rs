use super::*;

#[test]
fn fast_insert_sql_materializes_generic_multi_values() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();

    engine.execute(
        &mut session,
        "CREATE TABLE t (id INT64, name VARCHAR, amount DECIMAL(9,2), active BOOL)",
    )?;
    engine.execute(
            &mut session,
            "INSERT INTO t (id, name, amount, active) VALUES (1, 'alpha', 12.34, true), (2, 'it''s', -5.60, 0)",
        )?;

    assert_eq!(
        first_i64(
            engine.execute(&mut session, "SELECT id FROM t ORDER BY id")?,
            0,
            0
        ),
        1
    );
    assert_eq!(
        first_text(
            engine.execute(&mut session, "SELECT name FROM t WHERE id = 2")?,
            0,
            0,
        ),
        "it's"
    );
    assert_eq!(
        first_i64(
            engine.execute(&mut session, "SELECT COUNT(*) FROM t")?,
            0,
            0
        ),
        2
    );
    Ok(())
}

#[test]
fn fast_insert_sql_materializes_missing_nullable_and_literal_default_columns() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();

    engine.execute(
        &mut session,
        "CREATE TABLE t (id INT64, name VARCHAR, tag VARCHAR DEFAULT 'new', note VARCHAR)",
    )?;
    engine.execute(
        &mut session,
        "INSERT INTO t (id, name) VALUES (1, 'alpha'), (2, 'beta')",
    )?;

    assert_eq!(
        first_text(
            engine.execute(&mut session, "SELECT tag FROM t WHERE id = 1")?,
            0,
            0,
        ),
        "new"
    );
    assert_eq!(
        first_i64(
            engine.execute(&mut session, "SELECT COUNT(*) FROM t WHERE note IS NULL")?,
            0,
            0
        ),
        2
    );
    Ok(())
}

#[test]
fn fast_insert_sql_materializes_missing_auto_increment_and_current_timestamp() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();

    engine.execute(
            &mut session,
            "CREATE TABLE t (id INT64 AUTO_INCREMENT PRIMARY KEY, name VARCHAR, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
        )?;
    engine.execute(
        &mut session,
        "INSERT INTO t (name) VALUES ('alpha'), ('beta')",
    )?;

    assert_eq!(
        first_i64(
            engine.execute(&mut session, "SELECT id FROM t ORDER BY id")?,
            0,
            0
        ),
        1
    );
    assert_eq!(
        first_i64(
            engine.execute(&mut session, "SELECT id FROM t ORDER BY id")?,
            1,
            0
        ),
        2
    );
    assert_eq!(
        first_i64(
            engine.execute(
                &mut session,
                "SELECT COUNT(*) FROM t WHERE created_at IS NOT NULL"
            )?,
            0,
            0
        ),
        2
    );
    Ok(())
}

#[test]
fn delete_removes_memtable_rows_without_flushing() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = Engine::new(dir.path(), "main")?;
        let mut session = session();

        engine.execute(
            &mut session,
            "CREATE TABLE t (id INT64, name VARCHAR, PRIMARY KEY (id))",
        )?;
        engine.execute(
            &mut session,
            "INSERT INTO t VALUES (1, 'one'), (2, 'two'), (3, 'three')",
        )?;
        let epoch_before = engine.segment_mgr().memtable_flush_epoch("main", "t");

        engine.execute(&mut session, "DELETE FROM t WHERE id = 1")?;
        let count = first_i64(
            engine.execute(&mut session, "SELECT COUNT(*) FROM t")?,
            0,
            0,
        );
        assert_eq!(count, 2);
        // No forced flush: the rows were removed from the memtable in place.
        assert_eq!(
            engine.segment_mgr().memtable_flush_epoch("main", "t"),
            epoch_before
        );
    }

    // The filtered durable tail survives a restart; the deleted row must not
    // resurrect, and its key must be insertable again.
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();
    let count = first_i64(
        engine.execute(&mut session, "SELECT COUNT(*) FROM t")?,
        0,
        0,
    );
    assert_eq!(count, 2);
    engine.execute(&mut session, "INSERT INTO t VALUES (1, 'uno')")?;
    let name = first_text(
        engine.execute(&mut session, "SELECT name FROM t WHERE id = 1")?,
        0,
        0,
    );
    assert_eq!(name, "uno");
    Ok(())
}

#[test]
fn bulk_multi_value_insert_routes_every_value_partition() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();
    engine.execute(
        &mut session,
        "CREATE TABLE partitioned (id INT64, value INT64) PARTITION BY id",
    )?;
    engine.begin_bulk_import();
    session.bulk_import_mode = true;
    engine.execute(
        &mut session,
        "INSERT INTO partitioned VALUES (1, 10), (2, 20)",
    )?;

    let mut active = engine
        .segment_mgr()
        .active_partition_keys("main", "partitioned");
    active.sort();
    assert_eq!(active, vec!["1".to_string(), "2".to_string()]);

    session.bulk_import_mode = false;
    engine.end_bulk_import();
    Ok(())
}

#[test]
fn like_predicate_filters_rows_in_scan() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();

    engine.execute(&mut session, "CREATE TABLE t (id INT64, name VARCHAR)")?;
    engine.execute(
        &mut session,
        "INSERT INTO t (id, name) VALUES (1, 'apple'), (2, 'apply'), (3, 'banana'), (4, 'Apple'), (5, NULL)",
    )?;

    let contains = first_i64(
        engine.execute(
            &mut session,
            "SELECT COUNT(*) FROM t WHERE name LIKE '%appl%'",
        )?,
        0,
        0,
    );
    assert_eq!(contains, 2);
    let prefix = first_i64(
        engine.execute(
            &mut session,
            "SELECT COUNT(*) FROM t WHERE name LIKE 'app%'",
        )?,
        0,
        0,
    );
    assert_eq!(prefix, 2);
    let underscore = first_i64(
        engine.execute(
            &mut session,
            "SELECT COUNT(*) FROM t WHERE name LIKE 'banan_'",
        )?,
        0,
        0,
    );
    assert_eq!(underscore, 1);
    let underscore_two = first_i64(
        engine.execute(
            &mut session,
            "SELECT COUNT(*) FROM t WHERE name LIKE 'appl_'",
        )?,
        0,
        0,
    );
    assert_eq!(underscore_two, 2);
    let exact = first_i64(
        engine.execute(
            &mut session,
            "SELECT COUNT(*) FROM t WHERE name LIKE 'banana'",
        )?,
        0,
        0,
    );
    assert_eq!(exact, 1);
    let negated = first_i64(
        engine.execute(
            &mut session,
            "SELECT COUNT(*) FROM t WHERE name NOT LIKE '%an%'",
        )?,
        0,
        0,
    );
    assert_eq!(negated, 3);
    let no_match = first_i64(
        engine.execute(
            &mut session,
            "SELECT COUNT(*) FROM t WHERE name LIKE '%zz%'",
        )?,
        0,
        0,
    );
    assert_eq!(no_match, 0);

    // Same predicate after a flush must filter on segment data as well.
    engine.execute(&mut session, "FLUSH TABLE t")?;
    let contains = first_i64(
        engine.execute(
            &mut session,
            "SELECT COUNT(*) FROM t WHERE name LIKE '%appl%'",
        )?,
        0,
        0,
    );
    assert_eq!(contains, 2);
    Ok(())
}

#[test]
fn delete_with_like_predicate_only_removes_matching_rows() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();

    engine.execute(&mut session, "CREATE TABLE t (id INT64, name VARCHAR)")?;
    engine.execute(
        &mut session,
        "INSERT INTO t (id, name) VALUES (1, 'apple'), (2, 'apply'), (3, 'banana'), (4, 'Apple')",
    )?;
    engine.execute(&mut session, "DELETE FROM t WHERE name LIKE '%appl%'")?;
    let total = first_i64(
        engine.execute(&mut session, "SELECT COUNT(*) FROM t")?,
        0,
        0,
    );
    assert_eq!(total, 2);
    let remaining = first_text(
        engine.execute(&mut session, "SELECT name FROM t WHERE id = 3")?,
        0,
        0,
    );
    assert_eq!(remaining, "banana");
    Ok(())
}

// ---------------------------------------------------------------------------
// Regression tests for the DML evaluator audit fixes.
// ---------------------------------------------------------------------------

/// `WHERE a != v` used to fall into the batch evaluator's "match everything"
/// fallback and deleted every row of the table.
#[test]
fn delete_with_neq_predicate_only_removes_matching_rows() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();
    engine.execute(&mut session, "CREATE TABLE t (id INT64, v INT64)")?;
    engine.execute(
        &mut session,
        "INSERT INTO t (id, v) VALUES (1, 10), (2, 20), (3, 30), (4, 20)",
    )?;
    let result = engine.execute(&mut session, "DELETE FROM t WHERE v != 20")?;
    match result {
        crate::sql::execution::engine::ExecResult::Ok { rows_affected } => {
            assert_eq!(rows_affected, 2);
        }
        other => panic!("unexpected result {:?}", other),
    }
    assert_eq!(
        first_i64(
            engine.execute(&mut session, "SELECT COUNT(*) FROM t")?,
            0,
            0
        ),
        2
    );
    engine.segment_mgr().shutdown();
    Ok(())
}

/// `WHERE v BETWEEN a AND b` used to match every row in UPDATE.
#[test]
fn update_with_between_predicate_only_touches_matching_rows() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();
    engine.execute(&mut session, "CREATE TABLE t (id INT64, v INT64)")?;
    engine.execute(
        &mut session,
        "INSERT INTO t (id, v) VALUES (1, 10), (2, 20), (3, 30)",
    )?;
    engine.execute(
        &mut session,
        "UPDATE t SET v = 99 WHERE v BETWEEN 15 AND 25",
    )?;
    assert_eq!(
        first_i64(
            engine.execute(&mut session, "SELECT COUNT(*) FROM t WHERE v = 99")?,
            0,
            0
        ),
        1
    );
    assert_eq!(
        first_i64(
            engine.execute(&mut session, "SELECT COUNT(*) FROM t WHERE v = 10")?,
            0,
            0
        ),
        1
    );
    engine.segment_mgr().shutdown();
    Ok(())
}

/// `SET a = b` (column reference) used to silently write NULL into every
/// assigned row; it must be rejected instead.
#[test]
fn update_assignment_with_column_reference_errors() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();
    engine.execute(&mut session, "CREATE TABLE t (id INT64, a INT64, b INT64)")?;
    engine.execute(&mut session, "INSERT INTO t (id, a, b) VALUES (1, 5, 7)")?;
    let result = engine.execute(&mut session, "UPDATE t SET a = b WHERE id = 1");
    assert!(result.is_err(), "SET a = b must error, got {:?}", result);
    // Data untouched.
    assert_eq!(
        first_i64(
            engine.execute(&mut session, "SELECT a FROM t WHERE id = 1")?,
            0,
            0
        ),
        5
    );
    engine.segment_mgr().shutdown();
    Ok(())
}

/// UPDATE used to skip primary-key uniqueness validation entirely (PK is not
/// part of `unique_indexes`), letting `SET id = <existing>` through.
#[test]
fn update_rejects_duplicate_primary_key() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();
    engine.execute(
        &mut session,
        "CREATE TABLE t (id INT64 PRIMARY KEY, v INT64)",
    )?;
    engine.execute(
        &mut session,
        "INSERT INTO t (id, v) VALUES (1, 10), (2, 20)",
    )?;
    let result = engine.execute(&mut session, "UPDATE t SET id = 2 WHERE id = 1");
    assert!(
        result.is_err(),
        "SET id = existing pk must error, got {:?}",
        result
    );
    assert_eq!(
        first_i64(
            engine.execute(&mut session, "SELECT COUNT(*) FROM t")?,
            0,
            0
        ),
        2
    );
    engine.segment_mgr().shutdown();
    Ok(())
}

/// `NOT (v = x)` must not delete rows where v IS NULL (SQL UNKNOWN).
#[test]
fn delete_not_predicate_keeps_null_rows() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();
    engine.execute(&mut session, "CREATE TABLE t (id INT64, v INT64)")?;
    engine.execute(
        &mut session,
        "INSERT INTO t (id, v) VALUES (1, 5), (2, NULL)",
    )?;
    engine.execute(&mut session, "DELETE FROM t WHERE NOT (v = 10)")?;
    assert_eq!(
        first_i64(
            engine.execute(&mut session, "SELECT COUNT(*) FROM t")?,
            0,
            0
        ),
        1
    );
    assert_eq!(
        first_i64(
            engine.execute(&mut session, "SELECT COUNT(*) FROM t WHERE v IS NULL")?,
            0,
            0
        ),
        1
    );
    engine.segment_mgr().shutdown();
    Ok(())
}

/// `v NOT IN (1, 2)` must not delete NULL rows either.
#[test]
fn delete_not_in_predicate_keeps_null_rows() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();
    engine.execute(&mut session, "CREATE TABLE t (id INT64, v INT64)")?;
    engine.execute(
        &mut session,
        "INSERT INTO t (id, v) VALUES (1, 5), (2, NULL)",
    )?;
    engine.execute(&mut session, "DELETE FROM t WHERE v NOT IN (1, 2)")?;
    assert_eq!(
        first_i64(
            engine.execute(&mut session, "SELECT COUNT(*) FROM t")?,
            0,
            0
        ),
        1
    );
    engine.segment_mgr().shutdown();
    Ok(())
}

/// The fast insert path used to hard-fail on TIMESTAMP/DATE string literals
/// instead of parsing them like the standard path does.
#[test]
fn fast_insert_accepts_timestamp_and_date_string_literals() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();
    engine.execute(
        &mut session,
        "CREATE TABLE t (id INT64, ts TIMESTAMP, d DATE)",
    )?;
    engine.execute(
        &mut session,
        "INSERT INTO t (id, ts, d) VALUES (1, '2024-06-15 10:30:00', '2024-06-15')",
    )?;
    assert_eq!(
        first_i64(
            engine.execute(&mut session, "SELECT COUNT(*) FROM t")?,
            0,
            0
        ),
        1
    );
    assert_eq!(
        first_i64(
            engine.execute(
                &mut session,
                "SELECT COUNT(*) FROM t WHERE ts = 1718447400000000"
            )?,
            0,
            0
        ),
        1
    );
    engine.segment_mgr().shutdown();
    Ok(())
}

/// `UPDATE ... WHERE id IN (SELECT ...)` used to hit the "match every row"
/// fallback; it must now be resolved like DELETE does.
#[test]
fn update_where_in_subquery_only_touches_matching_rows() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();
    engine.execute(&mut session, "CREATE TABLE t (id INT64, v INT64)")?;
    engine.execute(&mut session, "CREATE TABLE sel (id INT64)")?;
    engine.execute(
        &mut session,
        "INSERT INTO t (id, v) VALUES (1, 1), (2, 2), (3, 3)",
    )?;
    engine.execute(&mut session, "INSERT INTO sel (id) VALUES (2)")?;
    engine.execute(
        &mut session,
        "UPDATE t SET v = 99 WHERE id IN (SELECT id FROM sel)",
    )?;
    assert_eq!(
        first_i64(
            engine.execute(&mut session, "SELECT COUNT(*) FROM t WHERE v = 99")?,
            0,
            0
        ),
        1
    );
    engine.segment_mgr().shutdown();
    Ok(())
}

/// DELETE inside a transaction must remove rows the same transaction just
/// inserted (they used to survive and reappear after COMMIT).
#[test]
fn delete_inside_transaction_removes_own_uncommitted_rows() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();
    engine.execute(&mut session, "CREATE TABLE t (id INT64, v INT64)")?;
    engine.execute(&mut session, "BEGIN")?;
    engine.execute(
        &mut session,
        "INSERT INTO t (id, v) VALUES (1, 10), (2, 20)",
    )?;
    // Visible inside the transaction...
    assert_eq!(
        first_i64(
            engine.execute(&mut session, "SELECT COUNT(*) FROM t")?,
            0,
            0
        ),
        2
    );
    engine.execute(&mut session, "DELETE FROM t WHERE id = 1")?;
    assert_eq!(
        first_i64(
            engine.execute(&mut session, "SELECT COUNT(*) FROM t")?,
            0,
            0
        ),
        1,
        "own uncommitted rows must be deletable inside the transaction"
    );
    engine.execute(&mut session, "COMMIT")?;
    assert_eq!(
        first_i64(
            engine.execute(&mut session, "SELECT COUNT(*) FROM t")?,
            0,
            0
        ),
        1,
        "deleted rows must stay deleted after COMMIT"
    );
    engine.segment_mgr().shutdown();
    Ok(())
}

/// A committed DELETE stays effective after a restart (durable-tail rewrite
/// for memtable-resident rows).
#[test]
fn committed_delete_of_own_insert_survives_restart() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = Engine::new(dir.path(), "main")?;
        let mut session = session();
        engine.execute(&mut session, "CREATE TABLE t (id INT64)")?;
        engine.execute(&mut session, "BEGIN")?;
        engine.execute(&mut session, "INSERT INTO t (id) VALUES (1), (2), (3)")?;
        engine.execute(&mut session, "DELETE FROM t WHERE id = 2")?;
        engine.execute(&mut session, "COMMIT")?;
        engine.segment_mgr().shutdown();
    }
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();
    assert_eq!(
        first_i64(
            engine.execute(&mut session, "SELECT COUNT(*) FROM t")?,
            0,
            0
        ),
        2
    );
    engine.segment_mgr().shutdown();
    Ok(())
}

/// Regression: the rewritten batch evaluator mapped the literal-first
/// `lit <= col` / `lit >= col` forms to the wrong column kernel, inverting
/// the predicate.
#[test]
fn swapped_literal_comparison_predicates_delete_correct_rows() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();
    engine.execute(&mut session, "CREATE TABLE t (id INT64, v INT64)")?;
    engine.execute(
        &mut session,
        "INSERT INTO t (id, v) VALUES (1, 10), (2, 20), (3, 30)",
    )?;

    engine.execute(&mut session, "DELETE FROM t WHERE 15 <= v")?;
    assert_eq!(
        first_i64(
            engine.execute(&mut session, "SELECT COUNT(*) FROM t")?,
            0,
            0
        ),
        1,
        "15 <= v must keep only v=10"
    );

    engine.execute(&mut session, "UPDATE t SET v = 99 WHERE 50 >= v")?;
    assert_eq!(
        first_i64(
            engine.execute(&mut session, "SELECT COUNT(*) FROM t WHERE v = 99")?,
            0,
            0
        ),
        1,
        "50 >= v must match v=10"
    );
    engine.segment_mgr().shutdown();
    Ok(())
}

/// Regression: numeric literal predicates (`WHERE 1`) must keep MySQL
/// truthiness after the evaluator rewrite.
#[test]
fn numeric_literal_predicate_follows_mysql_truthiness() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();
    engine.execute(&mut session, "CREATE TABLE t (id INT64, v INT64)")?;
    engine.execute(
        &mut session,
        "INSERT INTO t (id, v) VALUES (1, 10), (2, 20)",
    )?;
    engine.execute(&mut session, "DELETE FROM t WHERE 1")?;
    assert_eq!(
        first_i64(
            engine.execute(&mut session, "SELECT COUNT(*) FROM t")?,
            0,
            0
        ),
        0,
        "WHERE 1 deletes every row"
    );
    engine.execute(&mut session, "INSERT INTO t (id, v) VALUES (3, 30)")?;
    engine.execute(&mut session, "DELETE FROM t WHERE 0")?;
    assert_eq!(
        first_i64(
            engine.execute(&mut session, "SELECT COUNT(*) FROM t")?,
            0,
            0
        ),
        1,
        "WHERE 0 matches nothing"
    );
    engine.segment_mgr().shutdown();
    Ok(())
}

/// Regression: LIMIT pushdown used to pass straight through DISTINCT (and
/// window) nodes, truncating the scan before dedup —
/// `SELECT DISTINCT a, b FROM t LIMIT 4` returned fewer distinct rows.
#[test]
fn distinct_with_limit_returns_expected_row_count() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();
    engine.execute(&mut session, "CREATE TABLE t (a INT64, b INT64)")?;
    // 12 rows collapsing to 6 distinct pairs.
    let values: Vec<String> = (0..12).map(|i| format!("({}, {})", i % 3, i % 2)).collect();
    engine.execute(
        &mut session,
        &format!("INSERT INTO t VALUES {}", values.join(", ")),
    )?;
    let result = engine.execute(
        &mut session,
        "SELECT COUNT(*) FROM (SELECT DISTINCT a, b FROM t LIMIT 4) sub",
    )?;
    assert_eq!(first_i64(result, 0, 0), 4, "LIMIT applies after DISTINCT");
    engine.segment_mgr().shutdown();
    Ok(())
}

/// Regression: `WHERE col_a <= col_b` between INCOMPARABLE column types
/// matched every row in DML (and still matches none in SELECT); both sides
/// must agree: incomparable means no match.
#[test]
fn incomparable_column_comparison_matches_nothing() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();
    engine.execute(
        &mut session,
        "CREATE TABLE t (id INT64, s VARCHAR, n INT64)",
    )?;
    engine.execute(
        &mut session,
        "INSERT INTO t (id, s, n) VALUES (1, 'x', 5), (2, 'y', 10)",
    )?;
    let selected = first_i64(
        engine.execute(&mut session, "SELECT COUNT(*) FROM t WHERE s <= n")?,
        0,
        0,
    );
    assert_eq!(selected, 0, "SELECT: incomparable types never match");
    let result = engine.execute(&mut session, "DELETE FROM t WHERE s <= n");
    assert!(
        result.is_err()
            || matches!(
                result,
                Ok(crate::sql::execution::engine::ExecResult::Ok { rows_affected: 0 })
            ),
        "DML must not delete rows on incomparable comparison"
    );
    assert_eq!(
        first_i64(
            engine.execute(&mut session, "SELECT COUNT(*) FROM t")?,
            0,
            0
        ),
        2,
        "no rows lost"
    );
    engine.segment_mgr().shutdown();
    Ok(())
}

/// Regression: a cached prepared plan survived DROP + re-CREATE of the same
/// table with a different column order, so EXECUTE read wrong columns. The
/// plan is now discarded when the schema fingerprint changes.
#[test]
fn prepared_plan_invalidated_by_table_rebuild() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();
    engine.execute(&mut session, "CREATE TABLE t (a INT64, b INT64)")?;
    engine.execute(&mut session, "INSERT INTO t (a, b) VALUES (1, 2)")?;

    // Prepare, execute once (plan gets cached), then rebuild the table with
    // swapped columns. The cached plan's column index for `a` now points at
    // `b`; without fingerprint invalidation EXECUTE returned 2 instead of 1.
    let prepared = session.prepared_statements.prepare("SELECT a FROM t")?;
    let id = prepared.id;
    let mut stmt = session.prepared_statements.get_mut(id).unwrap().clone();
    let first = engine.execute_prepared(&mut session, &mut stmt)?;
    session.prepared_statements.insert(stmt);
    let _ = first;

    engine.execute(&mut session, "DROP TABLE t")?;
    engine.execute(&mut session, "CREATE TABLE t (b INT64, a INT64)")?;
    engine.execute(&mut session, "INSERT INTO t (b, a) VALUES (20, 10)")?;

    let mut stmt = session.prepared_statements.get_mut(id).unwrap().clone();
    let result = engine.execute_prepared(&mut session, &mut stmt)?;
    session.prepared_statements.insert(stmt);
    let ExecResult::Query { batches, .. } = result else {
        panic!("expected query result");
    };
    let value = batches
        .iter()
        .flat_map(|b| (0..b.num_rows()).filter_map(move |r| b.scalar(r, 0)))
        .filter_map(|v| v.integer_i128())
        .max();
    // Row from the rebuilt table only (old table dropped); `a` is 10.
    assert_eq!(value, Some(10), "SELECT a must read column a after rebuild");
    engine.segment_mgr().shutdown();
    Ok(())
}

/// Regression: ALTER TABLE ADD COLUMN DEFAULT CURRENT_TIMESTAMP backfilled
/// pre-existing rows with the query instant, so repeated reads returned
/// different values. The value is pinned at ALTER time now.
#[test]
fn alter_add_column_current_timestamp_is_stable() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();
    engine.execute(&mut session, "CREATE TABLE t (id INT64)")?;
    engine.execute(&mut session, "INSERT INTO t (id) VALUES (1), (2)")?;
    engine.execute(
        &mut session,
        "ALTER TABLE t ADD COLUMN created TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
    )?;
    std::thread::sleep(std::time::Duration::from_millis(5));
    let first = engine.execute(&mut session, "SELECT created FROM t WHERE id = 1")?;
    std::thread::sleep(std::time::Duration::from_millis(5));
    let second = engine.execute(&mut session, "SELECT created FROM t WHERE id = 1")?;
    let v1 = collect_one_ts(first);
    let v2 = collect_one_ts(second);
    assert_eq!(
        v1, v2,
        "backfilled CURRENT_TIMESTAMP must be stable across reads"
    );
    engine.segment_mgr().shutdown();
    Ok(())
}

fn collect_one_ts(result: crate::sql::execution::engine::ExecResult) -> Option<i64> {
    let crate::sql::execution::engine::ExecResult::Query { batches, .. } = result else {
        panic!("expected query result");
    };
    batches
        .iter()
        .flat_map(|b| (0..b.num_rows()).filter_map(move |r| b.scalar(r, 0)))
        .find_map(|v| match v {
            crate::types::scalar::ScalarValue::Timestamp(ts) => ts,
            _ => None,
        })
}

#[test]
fn insert_select_copies_rows_with_filter() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();

    engine.execute(&mut session, "CREATE TABLE src (id INT64, name VARCHAR)")?;
    engine.execute(&mut session, "CREATE TABLE dst (id INT64, name VARCHAR)")?;
    engine.execute(
        &mut session,
        "INSERT INTO src (id, name) VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')",
    )?;
    engine.execute(
        &mut session,
        "INSERT INTO dst (id, name) SELECT id, name FROM src WHERE id >= 2",
    )?;

    assert_eq!(
        first_i64(
            engine.execute(&mut session, "SELECT COUNT(*) FROM dst")?,
            0,
            0
        ),
        2
    );
    assert_eq!(
        first_text(
            engine.execute(&mut session, "SELECT name FROM dst WHERE id = 3")?,
            0,
            0
        ),
        "gamma"
    );
    Ok(())
}

#[test]
fn insert_select_supports_expressions_aggregates_and_defaults() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();

    engine.execute(&mut session, "CREATE TABLE orders (id INT64, amount INT64)")?;
    engine.execute(
        &mut session,
        "INSERT INTO orders (id, amount) VALUES (1, 100), (2, 250), (3, 50)",
    )?;
    engine.execute(
        &mut session,
        "CREATE TABLE summary (bucket VARCHAR, total INT64)",
    )?;
    // Aggregating source + expression columns + column-list projection.
    engine.execute(
        &mut session,
        "INSERT INTO summary (bucket, total) SELECT 'all', SUM(amount) FROM orders",
    )?;

    assert_eq!(
        first_i64(
            engine.execute(
                &mut session,
                "SELECT total FROM summary WHERE bucket = 'all'"
            )?,
            0,
            0
        ),
        400
    );
    Ok(())
}

#[test]
fn insert_select_converts_source_types_via_cast() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();

    engine.execute(&mut session, "CREATE TABLE raw (id INT64, price VARCHAR)")?;
    engine.execute(
        &mut session,
        "INSERT INTO raw (id, price) VALUES (1, '12.5'), (2, '7.25')",
    )?;
    engine.execute(
        &mut session,
        "CREATE TABLE priced (id INT64, price FLOAT64)",
    )?;
    // Text sources convert explicitly with CAST, like a VALUES insert of the
    // same text would require.
    engine.execute(
        &mut session,
        "INSERT INTO priced (id, price) SELECT id, CAST(price AS DOUBLE) FROM raw",
    )?;

    let result = engine.execute(&mut session, "SELECT price FROM priced WHERE id = 1")?;
    let crate::sql::execution::engine::ExecResult::Query { batches, .. } = result else {
        panic!("expected query result");
    };
    match batches[0].scalar(0, 0).unwrap() {
        crate::types::scalar::ScalarValue::Float64(Some(v)) => assert!((v - 12.5).abs() < 1e-9),
        other => panic!("expected FLOAT64 12.5, got {:?}", other),
    }
    Ok(())
}

#[test]
fn cast_works_in_select_list_and_where() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();

    engine.execute(&mut session, "CREATE TABLE t (id INT64, price VARCHAR)")?;
    engine.execute(
        &mut session,
        "INSERT INTO t (id, price) VALUES (1, '12.9'), (2, '3.2')",
    )?;

    // CAST in the select list truncates like to_int.
    let result = engine.execute(
        &mut session,
        "SELECT CAST(price AS BIGINT) FROM t WHERE id = 1",
    )?;
    let crate::sql::execution::engine::ExecResult::Query { batches, .. } = result else {
        panic!("expected query result");
    };
    match batches[0].scalar(0, 0).unwrap() {
        crate::types::scalar::ScalarValue::Int64(Some(v)) => assert_eq!(v, 12),
        other => panic!("expected INT64 12, got {:?}", other),
    }

    // CAST in WHERE + alias-agnostic type names + arithmetic.
    assert_eq!(
        first_i64(
            engine.execute(
                &mut session,
                "SELECT COUNT(*) FROM t WHERE CAST(price AS DOUBLE) > 5.0"
            )?,
            0,
            0
        ),
        1
    );
    // CAST of a NULL stays NULL for the target type.
    assert_eq!(
        first_i64(
            engine.execute(
                &mut session,
                "SELECT COUNT(*) FROM t WHERE CAST(NULL AS INT64) IS NULL"
            )?,
            0,
            0
        ),
        2
    );
    Ok(())
}

#[test]
fn comments_and_quoted_identifiers_parse_end_to_end() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();

    // Copy-pasted-from-a-tutorial shape: comments, quoted idents, trailing semicolon.
    engine
        .execute(
            &mut session,
            "-- create the table\nCREATE TABLE t (\"select\" INT64, `order` INT64); /* block\ntoo */",
        )
        .expect("CREATE failed");
    engine
        .execute(
            &mut session,
            "INSERT INTO t (\"select\", `order`) VALUES (1, 2) -- values\n, (3, 4);",
        )
        .expect("INSERT failed");
    let r = engine
        .execute(
            &mut session,
            "SELECT COUNT(*) FROM t WHERE \"select\" = 1 AND `order` = 2",
        )
        .expect("SELECT failed");
    assert_eq!(first_i64(r, 0, 0), 1);
    Ok(())
}

#[test]
fn rollback_releases_published_primary_keys_for_reinsert() -> Result<()> {
    // The INSERT publishes its PK/unique keys into the index at statement
    // time, even inside an explicit transaction. ROLLBACK must retract them,
    // or the re-insert of the same key hits a phantom duplicate until the
    // next snapshot refresh.
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();

    engine.execute(
        &mut session,
        "CREATE TABLE t (id INT64 PRIMARY KEY, v VARCHAR)",
    )?;
    engine.execute(&mut session, "BEGIN")?;
    engine.execute(&mut session, "INSERT INTO t (id, v) VALUES (1, 'a')")?;
    engine.execute(&mut session, "ROLLBACK")?;
    engine.execute(&mut session, "INSERT INTO t (id, v) VALUES (1, 'b')")?;

    assert_eq!(
        first_i64(
            engine.execute(&mut session, "SELECT COUNT(*) FROM t")?,
            0,
            0
        ),
        1
    );
    assert_eq!(
        first_text(
            engine.execute(&mut session, "SELECT v FROM t WHERE id = 1")?,
            0,
            0
        ),
        "b"
    );
    Ok(())
}

#[test]
fn transaction_updates_same_memtable_row_twice_without_duplicating() -> Result<()> {
    // Deferred-DELETE memtable positions recorded by the first UPDATE must
    // not match again in the second: the committed memtable batch never
    // changes between statements, so a re-match appended a second copy of
    // the new row image at COMMIT (one row became two).
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();

    engine.execute(&mut session, "CREATE TABLE t (id INT64, v INT64)")?;
    engine.execute(&mut session, "INSERT INTO t (id, v) VALUES (5, 0)")?;
    engine.execute(&mut session, "BEGIN")?;
    engine.execute(&mut session, "UPDATE t SET v = 1 WHERE id = 5")?;
    engine.execute(&mut session, "UPDATE t SET v = 2 WHERE id = 5")?;
    engine.execute(&mut session, "COMMIT")?;

    assert_eq!(
        first_i64(
            engine.execute(&mut session, "SELECT COUNT(*) FROM t WHERE id = 5")?,
            0,
            0
        ),
        1,
        "the row must exist exactly once after two in-transaction UPDATEs"
    );
    assert_eq!(
        first_i64(
            engine.execute(&mut session, "SELECT v FROM t WHERE id = 5")?,
            0,
            0
        ),
        2
    );
    Ok(())
}

#[test]
fn update_set_unknown_column_reports_error_instead_of_silent_noop() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();

    engine.execute(&mut session, "CREATE TABLE t (id INT64, v INT64)")?;
    engine.execute(&mut session, "INSERT INTO t (id, v) VALUES (1, 10)")?;

    let result = engine.execute(&mut session, "UPDATE t SET nosuchcol = 1 WHERE id = 1");
    assert!(result.is_err(), "unknown SET column must be an error");

    Ok(())
}

#[test]
fn composite_pk_update_validates_against_the_full_key() -> Result<()> {
    // PK(id, ts) with only `id` assigned: the uniqueness probe used to be
    // built from the assigned subset alone, so (1,1) and (1,2) — two legal
    // rows sharing id — were rejected as duplicates.
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();

    engine.execute(
        &mut session,
        "CREATE TABLE t (id INT64, ts INT64, PRIMARY KEY (id, ts))",
    )?;
    engine.execute(&mut session, "INSERT INTO t (id, ts) VALUES (1, 1), (1, 2)")?;
    engine.execute(&mut session, "UPDATE t SET id = 9 WHERE ts = 1")?;

    assert_eq!(
        first_i64(
            engine.execute(&mut session, "SELECT COUNT(*) FROM t WHERE id = 9")?,
            0,
            0
        ),
        1
    );
    Ok(())
}

#[test]
fn chained_intersect_parses_and_folds_left() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();

    engine.execute(&mut session, "CREATE TABLE t (v INT64)")?;
    engine.execute(&mut session, "INSERT INTO t (v) VALUES (1), (2), (3), (4)")?;

    // `a INTERSECT b INTERSECT c` used to fold only the first pair and
    // then fail on the leftover INTERSECT token.
    let result = engine.execute(
        &mut session,
        "SELECT v FROM t WHERE v > 1 INTERSECT SELECT v FROM t WHERE v < 4 INTERSECT SELECT v FROM t WHERE v <> 3",
    )?;
    let mut rows = 0usize;
    let mut values: Vec<i64> = Vec::new();
    if let ExecResult::Query { batches, .. } = result {
        for batch in &batches {
            rows += batch.num_rows();
            for row in 0..batch.num_rows() {
                if let ScalarValue::Int64(Some(v)) = batch.scalar(row, 0).unwrap() {
                    values.push(v);
                }
            }
        }
    } else {
        panic!("expected query result");
    }
    assert_eq!(rows, 1, "intersection is {{2}}, got {values:?}");
    assert_eq!(values, vec![2]);
    Ok(())
}

#[test]
fn fast_insert_path_matches_standard_path() -> Result<()> {
    // Differential rail: the fast INSERT path hand-parses the SQL text while
    // the standard path goes through the real parser and expression
    // evaluator. Both must produce byte-identical rows for the same
    // statements, including NULLs, escaped quotes, boundary integers and
    // negative decimals.
    struct Lcg(u64);
    impl Lcg {
        fn next(&mut self) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            self.0 >> 33
        }
    }
    let mut rng = Lcg(0x9E37_79B9_7F4A_7C15);

    let mut statements = Vec::new();
    let mut next_id: i64 = 0;
    for _ in 0..40 {
        let rows = 1 + (rng.next() % 4) as usize;
        let mut values = Vec::new();
        for _ in 0..rows {
            next_id += 1;
            let name = match rng.next() % 6 {
                0 => "NULL".to_string(),
                1 => "'plain'".to_string(),
                2 => "'it''s'".to_string(),
                3 => "''".to_string(),
                4 => format!("'name-{}'", rng.next() % 1000),
                _ => "'dq\"inside'".to_string(),
            };
            let amount = match rng.next() % 4 {
                0 => "NULL".to_string(),
                1 => format!("{:.2}", (rng.next() % 1_000_000) as i64 as f64 / 100.0),
                2 => format!("-{:.2}", (rng.next() % 50_000) as i64 as f64 / 100.0),
                _ => format!("{}", rng.next() % 100_000),
            };
            let active = if rng.next().is_multiple_of(2) {
                "true"
            } else {
                "false"
            };
            let small = match rng.next() % 4 {
                0 => "NULL".to_string(),
                1 => i32::MIN.to_string(),
                2 => i32::MAX.to_string(),
                _ => format!("{}", (rng.next() % 1_000_000) as i32),
            };
            let score = if rng.next().is_multiple_of(5) {
                "NULL".to_string()
            } else {
                format!("{:.3}", (rng.next() % 100_000) as i64 as f64 / 8.0)
            };
            let ts = if rng.next().is_multiple_of(4) {
                "NULL".to_string()
            } else {
                format!(
                    "'2026-{:02}-{:02} {:02}:{:02}:00'",
                    1 + rng.next() % 12,
                    1 + rng.next() % 28,
                    rng.next() % 24,
                    rng.next() % 60
                )
            };
            values.push(format!(
                "({}, {}, {}, {}, {}, {}, {})",
                next_id, name, amount, active, small, score, ts
            ));
        }
        statements.push(format!(
            "INSERT INTO t (id, name, amount, active, small, score, ts) VALUES {}",
            values.join(", ")
        ));
    }

    let run = |fast_disabled: bool| -> Result<Vec<String>> {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::new(dir.path(), "main")?;
        let mut session = session();
        engine.execute(
            &mut session,
            "CREATE TABLE t (id INT64, name VARCHAR, amount DECIMAL(9,2), active BOOL, small INT32, score FLOAT64, ts TIMESTAMP)",
        )?;
        crate::sql::execution::dml_insert::set_fast_insert_disabled_for_tests(fast_disabled);
        let mut outcome = Ok(());
        for stmt in &statements {
            if let Err(error) = engine.execute(&mut session, stmt) {
                outcome = Err(error);
                break;
            }
        }
        crate::sql::execution::dml_insert::set_fast_insert_disabled_for_tests(false);
        outcome?;
        let result = engine.execute(&mut session, "SELECT * FROM t ORDER BY id")?;
        let mut rows = Vec::new();
        if let ExecResult::Query { batches, .. } = result {
            for batch in &batches {
                for row in 0..batch.num_rows() {
                    let cells: Vec<String> = (0..batch.num_columns())
                        .map(|col| {
                            batch
                                .scalar(row, col)
                                .and_then(|v| v.to_text())
                                .unwrap_or_else(|| "NULL".to_string())
                        })
                        .collect();
                    rows.push(cells.join("|"));
                }
            }
        }
        Ok(rows)
    };

    let via_fast = run(false)?;
    let via_standard = run(true)?;
    assert!(!via_fast.is_empty());
    assert_eq!(
        via_fast.len(),
        via_standard.len(),
        "row counts differ between insert paths"
    );
    assert_eq!(
        via_fast, via_standard,
        "fast INSERT path diverged from the standard path"
    );
    Ok(())
}

/// A transactional UPDATE must invalidate the hot mirror at COMMIT, exactly
/// like auto-commit UPDATE does: the mirror is append-only, so rows it still
/// holds become stale once the deferred update images replace them. HOT
/// tables are currently rejected from transactional DML, but the invariant
/// must not depend on that gate staying closed.
#[test]
fn txn_update_commit_invalidates_hot_mirror() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();
    engine.execute(&mut session, "CREATE TABLE t (id INT64, v INT64)")?;
    engine.execute(&mut session, "INSERT INTO t (id, v) VALUES (1, 10)")?;
    let epoch_before = engine.hot_store_manager().feed_epoch("main", "t");
    engine.execute(&mut session, "BEGIN")?;
    engine.execute(&mut session, "UPDATE t SET v = 99 WHERE id = 1")?;
    engine.execute(&mut session, "COMMIT")?;
    assert_ne!(
        engine.hot_store_manager().feed_epoch("main", "t"),
        epoch_before,
        "COMMIT of a transactional UPDATE must invalidate the hot mirror"
    );
    engine.segment_mgr().shutdown();
    Ok(())
}
