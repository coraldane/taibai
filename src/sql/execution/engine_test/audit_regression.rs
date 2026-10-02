//! Regression tests from the storage-path audit (2026-10): one test per
//! fixed defect class, each failing on the pre-fix code.

use super::*;

fn row_count_of(engine: &Engine, table: &str) -> i64 {
    engine
        .catalogs()
        .read()
        .unwrap()
        .get("main")
        .unwrap()
        .get_table(table)
        .unwrap()
        .row_count
}

/// P0 (auto-commit UPDATE): Pass A probed footer zone maps with the
/// CATALOG-space predicate. After DROP COLUMN shifted every catalog index
/// left, the wrong column's zone map decided "no match" and the row group
/// was silently skipped — the UPDATE reported zero affected rows. The
/// predicate must be re-anchored per segment by name.
#[test]
fn update_row_group_pruning_aligns_predicates_after_drop_column() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();

    // `id` values are chosen far away from the `a` predicate value: with the
    // pre-fix positional probe, the `a = 1` clause consulted id's zone map
    // [100,102] and pruned every row group.
    engine.execute(&mut session, "CREATE TABLE t (id INT64, a INT64, v INT64)")?;
    engine.execute(
        &mut session,
        "INSERT INTO t VALUES (100, 1, 10), (101, 1, 11), (102, 2, 12)",
    )?;
    engine.flush_table_buffers("main", "t")?;
    engine.execute(&mut session, "ALTER TABLE t DROP COLUMN id")?;

    engine.execute(&mut session, "UPDATE t SET v = 99 WHERE a = 1")?;
    let updated = first_i64(
        engine.execute(&mut session, "SELECT COUNT(*) FROM t WHERE v = 99")?,
        0,
        0,
    );
    assert_eq!(updated, 2, "both a = 1 rows must be updated");
    Ok(())
}

/// P0 (transactional UPDATE): when a predicate column predates the segment
/// footer, the row-group loop used to `break` out and silently skip the
/// whole segment. With the default-fill semantics (the ones DELETE, SELECT
/// and COUNT already use), `WHERE new_col = default` must match the old
/// rows and update them.
#[test]
fn txn_update_matches_default_filled_predicate_after_add_column() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();

    engine.execute(&mut session, "CREATE TABLE t (id INT64, v INT64)")?;
    engine.execute(
        &mut session,
        "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)",
    )?;
    engine.flush_table_buffers("main", "t")?;
    engine.execute(&mut session, "ALTER TABLE t ADD COLUMN c INT64 DEFAULT 7")?;

    engine.execute(&mut session, "BEGIN")?;
    engine.execute(&mut session, "UPDATE t SET v = 99 WHERE c = 7")?;
    engine.execute(&mut session, "COMMIT")?;

    let updated = first_i64(
        engine.execute(&mut session, "SELECT COUNT(*) FROM t WHERE v = 99")?,
        0,
        0,
    );
    assert_eq!(updated, 3, "every old row carries c = DEFAULT 7");
    Ok(())
}

/// P1: transactional INSERTs incremented the catalog row_count at statement
/// time, but ROLLBACK never retracted the delta — the table's stats (and the
/// optimizer's cardinality) stayed inflated forever.
#[test]
fn rollback_retracts_inserted_rows_from_catalog_row_count() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();

    engine.execute(&mut session, "CREATE TABLE t (id INT64)")?;
    engine.execute(&mut session, "BEGIN")?;
    engine.execute(&mut session, "INSERT INTO t VALUES (1), (2), (3)")?;
    assert_eq!(row_count_of(&engine, "t"), 3);

    engine.execute(&mut session, "ROLLBACK")?;
    assert_eq!(
        row_count_of(&engine, "t"),
        0,
        "ROLLBACK must retract the statement-time row_count increments"
    );

    engine.execute(&mut session, "INSERT INTO t VALUES (7)")?;
    assert_eq!(row_count_of(&engine, "t"), 1);
    Ok(())
}

/// P0: `x * 1` used to simplify to the literal `1` itself, `x * 0` to `0`
/// (breaking NULL semantics: SQL NULL * 0 = NULL), and integer constant
/// folding overflowed unchecked. All three silently changed query results.
#[test]
fn arithmetic_simplifications_preserve_results_and_nulls() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();
    engine.execute(&mut session, "CREATE TABLE t (id INT64, a INT64)")?;
    engine.execute(
        &mut session,
        "INSERT INTO t VALUES (1, 10), (2, 20), (3, NULL)",
    )?;

    // x * 1 must stay x, not collapse to the literal 1.
    let kept = first_i64(
        engine.execute(&mut session, "SELECT COUNT(*) FROM t WHERE a * 1 = 10")?,
        0,
        0,
    );
    assert_eq!(kept, 1, "a * 1 = 10 must match only the a = 10 row");

    // NULL * 0 is NULL: COUNT(expr) counts non-NULL results, so exactly the
    // two rows with a present are counted — pre-fix the fold produced 0 for
    // the NULL row too and the count was 3.
    let non_null = first_i64(
        engine.execute(&mut session, "SELECT COUNT(a * 0) FROM t")?,
        0,
        0,
    );
    assert_eq!(non_null, 2, "NULL * 0 must stay NULL, not 0");

    // Overflow folds like the runtime: NULL, not a wrapped value.
    let overflow = first_i64(
        engine.execute(
            &mut session,
            "SELECT COUNT(*) FROM t WHERE id = 9223372036854775807 + 1",
        )?,
        0,
        0,
    );
    assert_eq!(overflow, 0, "overflowed literal folds to NULL and matches nothing");
    Ok(())
}

/// P0: `WHERE NOT (...)` flipped the two-valued mask, so rows where the
/// inner predicate was UNKNOWN (NULL) survived. Three-valued NOT keeps only
/// rows the inner predicate definitely excludes-to-false.
#[test]
fn not_filter_excludes_null_rows() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();
    engine.execute(&mut session, "CREATE TABLE t (a INT64)")?;
    engine.execute(&mut session, "INSERT INTO t VALUES (1), (5), (NULL)")?;
    let kept = first_i64(
        engine.execute(&mut session, "SELECT COUNT(*) FROM t WHERE NOT (a = 5)")?,
        0,
        0,
    );
    assert_eq!(kept, 1, "NOT (a = 5) keeps only a = 1; NULL is UNKNOWN");
    Ok(())
}

/// P0: join keys hashed per enum variant never matched across integer
/// widths — an INT32 foreign key joined to an INT64 primary key returned
/// zero rows without an error.
#[test]
fn join_matches_across_integer_widths() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();
    engine.execute(&mut session, "CREATE TABLE dim (id INT64, name VARCHAR)")?;
    engine.execute(&mut session, "CREATE TABLE fact (id INT32, v INT64)")?;
    engine.execute(&mut session, "INSERT INTO dim VALUES (1, 'one'), (2, 'two')")?;
    engine.execute(&mut session, "INSERT INTO fact VALUES (1, 100), (2, 200)")?;
    let joined = first_i64(
        engine.execute(
            &mut session,
            "SELECT COUNT(*) FROM fact JOIN dim ON fact.id = dim.id",
        )?,
        0,
        0,
    );
    assert_eq!(joined, 2, "INT32 = INT64 keys must hash and compare equal");
    Ok(())
}

/// P0: a correlated NOT IN was rewritten into an anti join, which outputs
/// outer rows whose equality found no match — including the NULL cases SQL
/// excludes. With a NULL in the subquery result the answer is always empty.
#[test]
fn correlated_not_in_with_null_subquery_value_excludes_all_rows() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();
    engine.execute(&mut session, "CREATE TABLE t (k INT64, x INT64)")?;
    engine.execute(&mut session, "CREATE TABLE u (k INT64, c INT64)")?;
    engine.execute(&mut session, "INSERT INTO t VALUES (1, 10), (2, 20)")?;
    // The subquery for k = 1 yields {5, NULL}; for k = 2 yields {7}.
    engine.execute(&mut session, "INSERT INTO u VALUES (1, 5), (1, NULL), (2, 7)")?;
    let kept = first_i64(
        engine.execute(
            &mut session,
            "SELECT COUNT(*) FROM t WHERE x NOT IN (SELECT c FROM u WHERE u.k = t.k)",
        )?,
        0,
        0,
    );
    // Correct three-valued result: the k = 1 row is UNKNOWN (set holds a
    // NULL) and excluded; the k = 2 row (20 NOT IN {7}) is TRUE and kept.
    // The pre-fix anti join output BOTH rows — no match found for either.
    assert_eq!(kept, 1, "only the k = 2 row survives; the NULL set excludes k = 1");
    Ok(())
}

/// P0: an explicit ROWS frame with UNBOUNDED PRECEDING .. CURRENT ROW took
/// the RANGE running-aggregate shortcut, flushing whole peer groups — every
/// row of a peer group reported the group total instead of its running
/// value.
#[test]
fn window_rows_frame_accumulates_row_by_row() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();
    engine.execute(&mut session, "CREATE TABLE t (k INT64, v INT64)")?;
    engine.execute(&mut session, "INSERT INTO t VALUES (1, 10), (1, 20), (2, 30)")?;
    let r = engine.execute(
        &mut session,
        "SELECT SUM(v) OVER (ORDER BY k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t",
    )?;
    // Storage order follows insertion; peers k = 1 must report 10 and 30,
    // not both 30 (the peer-group total).
    assert_eq!(first_i64_at(&r, 0, 0), 10, "first row: running sum is 10");
    assert_eq!(first_i64_at(&r, 1, 0), 30, "second row: running sum is 30");
    Ok(())
}

/// P0: DML plans walking visible memtables resolved predicate columns
/// against the CATALOG while a memtable recovered from a pre-ALTER tail
/// keeps its insert-time layout — positions met the wrong physical column.
/// The shared walk now aligns the view to the catalog first.
#[test]
fn dml_memtable_walk_aligns_drifted_layouts() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();
    engine.execute(&mut session, "CREATE TABLE t (id INT64, v INT64)")?;
    let table_schema = {
        let catalogs = engine.catalogs().read().unwrap();
        let meta = catalogs.get("main").unwrap().get_table("t").unwrap();
        crate::types::batch::Schema::new(meta.columns.clone())
    };

    // Bypass the SQL path: plant a memtable whose batch predates a
    // (simulated) DROP COLUMN layout — (a, id, v) while the catalog is
    // (id, v). This is what tail recovery produces after a pre-ALTER crash.
    use crate::protocol::types::DataType;
    use crate::types::batch::ColumnDef;
    let old_schema = crate::types::batch::Schema::new(vec![
        ColumnDef::new("a", DataType::Int64),
        ColumnDef::new("id", DataType::Int64),
        ColumnDef::new("v", DataType::Int64),
    ]);
    let manager = engine.memtable_manager();
    let memtable = manager.get_or_create("main", "t", "", &old_schema);
    let mut batch = crate::types::batch::RecordBatch::new(old_schema);
    for (a, id, v) in [(0, 1, 10), (0, 2, 20)] {
        use crate::types::scalar::ScalarValue;
        batch.column_mut(0).unwrap().push(&ScalarValue::Int64(Some(a))).unwrap();
        batch.column_mut(1).unwrap().push(&ScalarValue::Int64(Some(id))).unwrap();
        batch.column_mut(2).unwrap().push(&ScalarValue::Int64(Some(v))).unwrap();
    }
    memtable.append(&batch)?;

    let mut seen_layouts = Vec::new();
    crate::sql::execution::dml_delete::for_each_visible_memtable(
        &engine,
        "main",
        "t",
        &table_schema,
        0,
        None,
        |_memtable, _original, view, _live| {
            seen_layouts.push((
                view.schema.columns.iter().map(|c| c.name.clone()).collect::<Vec<_>>(),
                view.column(0).map(|c| c.len()),
            ));
            Ok(())
        },
    )?;
    assert_eq!(seen_layouts.len(), 1);
    assert_eq!(
        seen_layouts[0].0,
        vec!["id".to_string(), "v".to_string()],
        "the DML view must be catalog-aligned"
    );
    assert_eq!(seen_layouts[0].1, Some(2), "row-preserving");
    Ok(())
}

/// Same as `first_i64` for a borrowed result (lets one query answer two
/// assertions without being consumed).
fn first_i64_at(result: &ExecResult, row: usize, col: usize) -> i64 {
    let ExecResult::Query { batches, .. } = result else {
        panic!("expected query result");
    };
    match batches[0].scalar(row, col).unwrap() {
        ScalarValue::Int64(Some(v)) => v,
        ScalarValue::Int32(Some(v)) => v as i64,
        value => panic!("unexpected value {:?}", value),
    }
}

/// P1: LEFT JOIN's unmatched build rows used to be boxed into the output
/// buffer all at once (two Vec<ScalarValue> per row). The chunked stash
/// must still emit every row, across many drain batches.
#[test]
fn left_join_emits_all_unmatched_rows_across_chunks() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();
    engine.execute(&mut session, "CREATE TABLE big (id INT64)")?;
    let mut sql = String::from("INSERT INTO big VALUES ");
    for i in 0..10_000i64 {
        if i > 0 {
            sql.push_str(", ");
        }
        sql.push_str(&format!("({})", i + 1));
    }
    engine.execute(&mut session, &sql)?;
    engine.execute(&mut session, "CREATE TABLE small (id INT64, v INT64)")?;
    engine.execute(&mut session, "INSERT INTO small VALUES (5000, 1)")?;

    // 9 999 unmatched rows drain in BATCH_SIZE chunks; every one must
    // surface exactly once.
    let unmatched = first_i64(
        engine.execute(
            &mut session,
            "SELECT COUNT(*) FROM big LEFT JOIN small ON big.id = small.id WHERE small.id IS NULL",
        )?,
        0,
        0,
    );
    assert_eq!(unmatched, 9_999);
    let matched = first_i64(
        engine.execute(
            &mut session,
            "SELECT COUNT(*) FROM big LEFT JOIN small ON big.id = small.id WHERE small.id IS NOT NULL",
        )?,
        0,
        0,
    );
    assert_eq!(matched, 1);
    Ok(())
}

/// A4: chunked unmatched-row emission on the SLOW paths too — a residual
/// (non-equi) condition defeats the fast probe, forcing the scalar-row
/// probe_left/probe_right drivers. 10k unmatched rows must still drain
/// across stash chunks exactly once.
#[test]
fn slow_path_joins_emit_all_unmatched_rows_across_chunks() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();

    engine.execute(&mut session, "CREATE TABLE big (id INT64)")?;
    let mut sql = String::from("INSERT INTO big VALUES ");
    for i in 0..10_000i64 {
        if i > 0 {
            sql.push_str(", ");
        }
        sql.push_str(&format!("({})", i + 1));
    }
    engine.execute(&mut session, &sql)?;
    engine.execute(&mut session, "CREATE TABLE small (id INT64, v INT64)")?;
    engine.execute(&mut session, "INSERT INTO small VALUES (5000, 1)")?;

    // Residual condition keeps the match off every row -> 10 000 unmatched.
    let left = first_i64(
        engine.execute(
            &mut session,
            "SELECT COUNT(*) FROM big LEFT JOIN small ON big.id = small.id AND small.v > 100 \
             WHERE small.id IS NULL",
        )?,
        0,
        0,
    );
    assert_eq!(left, 10_000, "slow-path LEFT: every row unmatched");

    let right = first_i64(
        engine.execute(
            &mut session,
            "SELECT COUNT(*) FROM small RIGHT JOIN big ON small.id = big.id AND small.v > 100 \
             WHERE small.id IS NULL",
        )?,
        0,
        0,
    );
    assert_eq!(right, 10_000, "slow-path RIGHT: unmatched right side in chunks");
    Ok(())
}

/// Adversarial-review repro: the constant-default-column fast path took the
/// run-length representation for a NULL default regardless of type, and a
/// NULL default on a VarChar column hit expand_runs' unreachable!() — a
/// plain `SELECT` after `ADD COLUMN` (no DEFAULT) crashed the engine.
#[test]
fn null_default_added_column_selects_without_panic() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();
    engine.execute(&mut session, "CREATE TABLE t (id INT64)")?;
    engine.execute(&mut session, "INSERT INTO t VALUES (1), (2)")?;
    engine.flush_table_buffers("main", "t")?;
    engine.execute(&mut session, "ALTER TABLE t ADD COLUMN extra VARCHAR")?;

    // Segments predate the column: every `extra` is the NULL default.
    let counted = first_i64(
        engine.execute(&mut session, "SELECT COUNT(extra) FROM t WHERE id >= 1")?,
        0,
        0,
    );
    assert_eq!(counted, 0, "COUNT over an all-NULL column is 0");
    let rows = first_i64(
        engine.execute(&mut session, "SELECT COUNT(*) FROM t WHERE extra IS NULL")?,
        0,
        0,
    );
    assert_eq!(rows, 2);
    Ok(())
}

/// Adversarial-review regression suite for the UPDATE tombstone+reinsert
/// routing: uniqueness must be enforced against EVERY post-statement key
/// (survivors, memtable rows, and the new images themselves), and a
/// violation must abort with zero durable side effects — not delete the
/// matched rows.
#[test]
fn update_unique_violations_abort_cleanly() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();
    engine.execute(
        &mut session,
        "CREATE TABLE t (id INT64 PRIMARY KEY, code INT64, UNIQUE KEY uk_code (code))",
    )?;
    engine.execute(&mut session, "INSERT INTO t VALUES (1, 10), (2, 20)")?;
    engine.flush_table_buffers("main", "t")?;

    // Collision with a surviving SEGMENT row: must error and lose nothing.
    let result = engine.execute(&mut session, "UPDATE t SET code = 20 WHERE id = 1");
    assert!(result.is_err(), "new key collides with a surviving row");
    let count = first_i64(
        engine.execute(&mut session, "SELECT COUNT(*) FROM t")?,
        0,
        0,
    );
    assert_eq!(count, 2, "a rejected UPDATE must not delete matched rows");

    // Intra-statement collision: two rows assigned the same constant key.
    let result = engine.execute(&mut session, "UPDATE t SET code = 99 WHERE id IN (1, 2)");
    assert!(result.is_err(), "two rows cannot take one key");
    let count = first_i64(
        engine.execute(&mut session, "SELECT COUNT(*) FROM t")?,
        0,
        0,
    );
    assert_eq!(count, 2);

    // Collision with a MEMTABLE row this statement does not touch.
    engine.execute(&mut session, "INSERT INTO t VALUES (3, 30)")?; // stays in memtable
    let result = engine.execute(&mut session, "UPDATE t SET code = 30 WHERE id = 1");
    assert!(result.is_err(), "new key collides with a memtable row");
    let count = first_i64(
        engine.execute(&mut session, "SELECT COUNT(*) FROM t")?,
        0,
        0,
    );
    assert_eq!(count, 3);

    // A key freed by an earlier DELETE (tombstoned row still physically
    // present) must be REUSABLE — dead-row keys must not falsely collide.
    engine.execute(&mut session, "DELETE FROM t WHERE id = 2")?;
    engine.execute(&mut session, "INSERT INTO t VALUES (4, 20)")?; // reuses 20
    // Free key 10 as well: its only physical occurrence must be a tombstoned
    // row before the reuse below is legal. (The old assertion updated to 10
    // while row 1 still held it — that only passed because the pre-fix
    // validation could not see keys in WHERE-pruned segments.)
    engine.execute(&mut session, "DELETE FROM t WHERE id = 1")?;
    let result = engine.execute(&mut session, "UPDATE t SET code = 10 WHERE id = 4");
    assert!(result.is_ok(), "a key freed by DELETE must be reusable: {result:?}");
    let code = first_i64(
        engine.execute(&mut session, "SELECT code FROM t WHERE id = 4")?,
        0,
        0,
    );
    assert_eq!(code, 10);
    Ok(())
}
