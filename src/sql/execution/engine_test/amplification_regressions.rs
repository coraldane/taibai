//! Regressions for the second structural batch (2026-10-03): txn-DELETE
//! statement staging, point-DELETE location narrowing, and UPDATE
//! uniqueness probing — each pins the invariant the pre-fix defect class
//! violated.

use super::*;

/// P2→P0-class (txn DELETE): a deferred DELETE that exhausts the mutation
/// budget mid-statement must leave NO registered tombstones behind — the
/// transaction's later COMMIT must not delete rows of a statement the client
/// saw fail. The staged-apply protocol (pre-check + compensation) makes the
/// half-statement unrepresentable.
#[test]
fn txn_delete_over_budget_failure_leaves_no_tombstones() -> Result<()> {
    // 100 bytes = ~2 tombstone positions at the 48-byte charge: the first
    // 1-row DELETE fits, the second 3-row DELETE must not.
    crate::storage::core::txn_mutations::set_txn_mutation_budget_for_tests(Some(100));
    let result = (|| {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::new(dir.path(), "main")?;
        let mut writer = session();

        engine.execute(&mut writer, "CREATE TABLE t (id INT64, v INT64)")?;
        engine.execute(
            &mut writer,
            "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40)",
        )?;
        engine.execute(&mut writer, "FLUSH TABLE t")?;

        engine.execute(&mut writer, "BEGIN")?;
        // Charge most of the 1 KiB budget with a first, succeeding DELETE.
        engine.execute(&mut writer, "DELETE FROM t WHERE id = 1")?;
        // The second DELETE's staged charges no longer fit: it must fail
        // BEFORE any of its tombstones are registered.
        let over_budget = engine.execute(&mut writer, "DELETE FROM t WHERE id >= 2");
        assert!(
            over_budget.is_err(),
            "second DELETE must fail against the 1 KiB budget"
        );
        engine.execute(&mut writer, "COMMIT")?;

        // Only the FIRST statement's delete may be materialized.
        let count = first_i64(engine.execute(&mut writer, "SELECT COUNT(*) FROM t")?, 0, 0);
        assert_eq!(count, 3, "the failed DELETE's rows must survive COMMIT");
        let gone = first_i64(
            engine.execute(&mut writer, "SELECT COUNT(*) FROM t WHERE id = 1")?,
            0,
            0,
        );
        assert_eq!(gone, 0, "the succeeded DELETE's row must be gone");
        Ok(())
    })();
    crate::storage::core::txn_mutations::set_txn_mutation_budget_for_tests(None);
    result
}

/// Point-DELETE narrowing: `DELETE ... WHERE pk = x` resolves the affected
/// segment through the location index instead of scanning every candidate
/// segment. Correctness contract: exactly the matched row disappears, rows
/// in other segments stay, and an absent key deletes nothing.
#[test]
fn point_delete_narrows_via_location_index() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut writer = session();

    engine.execute(&mut writer, "CREATE TABLE t (id INT64, v INT64)")?;
    // Two segments with interleaved id ranges: zone maps overlap, so a scan
    // cannot prune by range — exactly the case the location index decides.
    engine.execute(&mut writer, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")?;
    engine.execute(&mut writer, "FLUSH TABLE t")?;
    engine.execute(&mut writer, "INSERT INTO t VALUES (4, 40), (5, 50), (6, 60)")?;
    engine.execute(&mut writer, "FLUSH TABLE t")?;

    // Delete a row living in the SECOND segment.
    engine.execute(&mut writer, "DELETE FROM t WHERE id = 5")?;
    let count = first_i64(engine.execute(&mut writer, "SELECT COUNT(*) FROM t")?, 0, 0);
    assert_eq!(count, 5);
    let hit = first_i64(
        engine.execute(&mut writer, "SELECT COUNT(*) FROM t WHERE id = 5")?,
        0,
        0,
    );
    assert_eq!(hit, 0, "the point-deleted row must be gone");

    // An absent key deletes nothing and errors nothing.
    let affected = engine.execute(&mut writer, "DELETE FROM t WHERE id = 999")?;
    let ExecResult::Ok { rows_affected } = affected else {
        panic!("expected ok result");
    };
    assert_eq!(rows_affected, 0, "absent key deletes nothing");

    // A memtable-resident point delete still works after the narrowing.
    engine.execute(&mut writer, "INSERT INTO t VALUES (7, 70)")?;
    engine.execute(&mut writer, "DELETE FROM t WHERE id = 7")?;
    let count = first_i64(engine.execute(&mut writer, "SELECT COUNT(*) FROM t")?, 0, 0);
    assert_eq!(count, 5, "memtable-resident point delete works");

    // Non-point predicates keep the scan path.
    engine.execute(&mut writer, "DELETE FROM t WHERE id <= 2")?;
    let count = first_i64(engine.execute(&mut writer, "SELECT COUNT(*) FROM t")?, 0, 0);
    assert_eq!(count, 3, "range delete still works");
    Ok(())
}

/// Point-DELETE inside an explicit transaction uses the same narrowing; the
/// deferred tombstone still only materializes at COMMIT and rolls back with
/// ROLLBACK.
#[test]
fn txn_point_delete_narrows_and_rolls_back() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut writer = session();

    engine.execute(&mut writer, "CREATE TABLE t (id INT64, v INT64)")?;
    engine.execute(&mut writer, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")?;
    engine.execute(&mut writer, "FLUSH TABLE t")?;
    engine.execute(&mut writer, "INSERT INTO t VALUES (4, 40)")?;
    engine.execute(&mut writer, "FLUSH TABLE t")?;

    engine.execute(&mut writer, "BEGIN")?;
    engine.execute(&mut writer, "DELETE FROM t WHERE id = 4")?;
    let in_txn = first_i64(engine.execute(&mut writer, "SELECT COUNT(*) FROM t")?, 0, 0);
    assert_eq!(in_txn, 3, "deleted within the transaction's view");
    engine.execute(&mut writer, "ROLLBACK")?;
    let after = first_i64(engine.execute(&mut writer, "SELECT COUNT(*) FROM t")?, 0, 0);
    assert_eq!(after, 4, "ROLLBACK restores the point-deleted row");
    Ok(())
}

/// UPDATE uniqueness via the location-index probe: a new key colliding with
/// a row in ANOTHER segment (invisible to any WHERE-pruned scan) is
/// rejected with the statement fully aborted; a fresh key and a
/// self-assignment both pass.
#[test]
fn update_unique_validation_probes_across_segments() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut writer = session();

    engine.execute(
        &mut writer,
        "CREATE TABLE t (id INT64, uk INT64, v INT64, UNIQUE KEY uk_val (uk))",
    )?;
    engine.execute(&mut writer, "INSERT INTO t VALUES (1, 100, 10), (2, 200, 20)")?;
    engine.execute(&mut writer, "FLUSH TABLE t")?;
    engine.execute(&mut writer, "INSERT INTO t VALUES (3, 300, 30), (4, 400, 40)")?;
    engine.execute(&mut writer, "FLUSH TABLE t")?;

    // Collision with a row in the FIRST segment while updating a row of the
    // second: only a global check (probe or sweep) can see it.
    let collision = engine.execute(&mut writer, "UPDATE t SET uk = 100 WHERE id = 4");
    assert!(collision.is_err(), "cross-segment key collision must be rejected");
    let intact = first_i64(
        engine.execute(&mut writer, "SELECT v FROM t WHERE id = 4")?,
        0,
        0,
    );
    assert_eq!(intact, 40, "the aborted UPDATE must leave the row untouched");

    // A fresh key passes.
    engine.execute(&mut writer, "UPDATE t SET uk = 900 WHERE id = 4")?;
    let updated = first_i64(
        engine.execute(&mut writer, "SELECT uk FROM t WHERE id = 4")?,
        0,
        0,
    );
    assert_eq!(updated, 900);

    // Self-assignment must not collide with the row's own retiring key.
    engine.execute(&mut writer, "UPDATE t SET uk = 900 WHERE id = 4")?;
    let count = first_i64(engine.execute(&mut writer, "SELECT COUNT(*) FROM t")?, 0, 0);
    assert_eq!(count, 4, "self-assignment keeps every row");
    Ok(())
}

/// Hot-mirror invalidation is no longer terminal: an in-place UPDATE drops
/// the append-only mirror and marks the table stale, and the NEXT INSERT
/// feed rebuilds it from the FULL live state (every visible memtable batch
/// — the watermark derivation requires exactly that set). The pre-fix
/// behavior kept the table's point-lookup fast path disabled until process
/// restart.
#[test]
fn hot_store_rebuilds_from_live_state_after_invalidation() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut writer = session();

    engine.execute(
        &mut writer,
        "CREATE TABLE h (id INT64, v INT64, ts TIMESTAMP) CLUSTER BY (id) HOT BY (ts) INDEX (id)",
    )?;
    engine.execute(
        &mut writer,
        "INSERT INTO h VALUES (1, 10, 1782797460000000), (2, 20, 1782797460000001)",
    )?;
    assert!(!engine.hot_store_manager().is_stale("main", "h"));

    // The in-place UPDATE invalidates the append-only mirror...
    engine.execute(&mut writer, "UPDATE h SET v = 99 WHERE id = 1")?;
    assert!(
        engine.hot_store_manager().is_stale("main", "h"),
        "UPDATE drops the append-only mirror and marks the table stale"
    );

    // ...and the next feed rebuilds it from the full live state.
    engine.execute(
        &mut writer,
        "INSERT INTO h VALUES (3, 30, 1782797460000002)",
    )?;
    assert!(
        !engine.hot_store_manager().is_stale("main", "h"),
        "the next INSERT feed rebuilds the mirror (was: disabled until restart)"
    );
    let store_rows = engine
        .hot_store_manager()
        .get("main", "h")
        .map(|store| store.row_count())
        .unwrap_or(0);
    assert!(
        store_rows >= 2,
        "the rebuilt mirror holds the live memtable rows, got {store_rows}"
    );

    // Point lookups answer for pre- and post-rebuild rows alike.
    for id in [1i64, 2, 3] {
        let found = first_i64(
            engine.execute(&mut writer, &format!("SELECT COUNT(*) FROM h WHERE id = {id}"))?,
            0,
            0,
        );
        assert_eq!(found, 1, "point lookup for id {id}");
    }
    let updated = first_i64(
        engine.execute(&mut writer, "SELECT v FROM h WHERE id = 1")?,
        0,
        0,
    );
    assert_eq!(updated, 99);
    Ok(())
}

/// Join-side projection pruning (B6): the optimizer narrows each join
/// side's scan to the columns the join condition and its consumers
/// reference; the physical join re-pads narrowed children to the full
/// planned side schema. Results must be IDENTICAL to the unpruned read
/// across plain joins, nested joins and outer joins.
#[test]
fn join_projection_pruning_keeps_results_identical() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut writer = session();

    engine.execute(
        &mut writer,
        "CREATE TABLE a (k INT64, a1 INT64, a2 INT64, a3 INT64, a4 INT64, a5 INT64)",
    )?;
    engine.execute(
        &mut writer,
        "CREATE TABLE b (k INT64, b1 INT64, b2 INT64, b3 INT64, b4 INT64, b5 INT64)",
    )?;
    for k in 1..=8i64 {
        engine.execute(
            &mut writer,
            &format!(
                "INSERT INTO a VALUES ({k}, {k}00, {k}01, {k}02, {k}03, {k}04), 
                           ({} , 0, 0, 0, 0, 0)",
                k + 100
            ),
        )?;
    }
    for k in 1..=8i64 {
        engine.execute(
            &mut writer,
            &format!(
                "INSERT INTO b VALUES ({k}, {k}10, {k}11, {k}12, {k}13, {k}14)"
            ),
        )?;
    }
    engine.execute(&mut writer, "FLUSH TABLE a")?;
    engine.execute(&mut writer, "FLUSH TABLE b")?;

    // Plain join: project one column per side, filter on a THIRD side
    // column, order by a side key — exercises requirement propagation of
    // projection + filter + join-key columns.
    let count = first_i64(
        engine.execute(
            &mut writer,
            "SELECT COUNT(*) FROM a JOIN b ON a.k = b.k WHERE a.a2 > 300",
        )?,
        0,
        0,
    );
    assert_eq!(count, 6, "six a2 values (301..801) clear the filter");
    let sample = first_i64(
        engine.execute(
            &mut writer,
            "SELECT b.b5 FROM a JOIN b ON a.k = b.k WHERE a.k = 4",
        )?,
        0,
        0,
    );
    assert_eq!(sample, 414, "projected right-side column resolves correctly");

    // Outer join: the layout guard may decline pruning; results must be
    // correct either way.
    let left_rows = first_i64(
        engine.execute(
            &mut writer,
            "SELECT COUNT(*) FROM a LEFT JOIN b ON a.k = b.k WHERE b.b1 IS NULL",
        )?,
        0,
        0,
    );
    assert_eq!(left_rows, 8, "only the k+100 rows lack a match");

    // Nested join across three tables.
    engine.execute(&mut writer, "CREATE TABLE c (k INT64, c1 INT64, c2 INT64)")?;
    for k in 1..=4i64 {
        engine.execute(&mut writer, &format!("INSERT INTO c VALUES ({k}, {k}00, {k}99)"))?;
    }
    engine.execute(&mut writer, "FLUSH TABLE c")?;
    let nested = first_i64(
        engine.execute(
            &mut writer,
            "SELECT COUNT(*) FROM a JOIN b ON a.k = b.k JOIN c ON b.k = c.k",
        )?,
        0,
        0,
    );
    assert_eq!(nested, 4, "three-way join narrows each side independently");
    let nested_value = first_i64(
        engine.execute(
            &mut writer,
            "SELECT c.c2 FROM a JOIN b ON a.k = b.k JOIN c ON b.k = c.k WHERE a.k = 3",
        )?,
        0,
        0,
    );
    assert_eq!(nested_value, 399);
    Ok(())
}
