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
