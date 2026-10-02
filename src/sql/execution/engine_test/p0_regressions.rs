//! Regression tests for the P0 findings of the full-path audit. Each test
//! names the failure mode it pins down; the fault-injection seams
//! (`crate::common::fault_injection`) let the atomicity tests fail durable
//! steps deterministically instead of relying on IO errors.

use super::*;

/// P0 (INSERT): a transactional INSERT that exceeds the mutation budget must
/// leave NO rows behind — a later COMMIT must not publish ghost rows whose
/// unique-index keys were never recorded.
#[test]
fn txn_insert_over_budget_failure_leaves_no_ghost_rows() -> Result<()> {
    crate::storage::core::txn_mutations::set_txn_mutation_budget_for_tests(Some(1024));
    let result = (|| {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::new(dir.path(), "main")?;
        let mut session = session();

        engine.execute(
            &mut session,
            "CREATE TABLE t (id INT64, name VARCHAR, UNIQUE KEY uk_id (id))",
        )?;
        engine.execute(&mut session, "BEGIN")?;

        // First statement fits the budget and succeeds.
        engine.execute(&mut session, "INSERT INTO t (id, name) VALUES (1, 'a')")?;

        // Second statement blows past it: its rows must not survive the
        // failed statement into COMMIT.
        let big_values: String = (2..=40i64)
            .map(|id| format!("({}, 'batch-two')", id))
            .collect::<Vec<_>>()
            .join(", ");
        let over_budget =
            engine.execute(&mut session, &format!("INSERT INTO t (id, name) VALUES {}", big_values));
        assert!(
            over_budget.is_err(),
            "second statement must fail against the 1 KiB budget"
        );

        engine.execute(&mut session, "COMMIT")?;

        let count = first_i64(engine.execute(&mut session, "SELECT COUNT(*) FROM t")?, 0, 0);
        assert_eq!(count, 1, "failed statement's rows must not be committed");

        // The surviving row's unique key must be enforced.
        let duplicate = engine.execute(&mut session, "INSERT INTO t (id, name) VALUES (1, 'dup')");
        assert!(duplicate.is_err(), "unique key of committed row must hold");
        Ok(())
    })();
    crate::storage::core::txn_mutations::set_txn_mutation_budget_for_tests(None);
    result
}

/// P0 (query): RLE passthrough columns must aggregate correctly in the
/// GENERIC accumulator paths (multi-key GROUP BY, aggregates with HAVING),
/// not just the run-aware fast paths. Rows live in a sealed segment so the
/// px column lands run-encoded (long constant bands, no WHERE predicate).
#[test]
fn rle_columns_aggregate_correctly_on_generic_paths() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();

    engine.execute(
        &mut session,
        "CREATE TABLE ticks (symbol VARCHAR, day VARCHAR, px INT64)",
    )?;

    // Seed one segment directly through the segment writer (the adaptive
    // encoding path): 4 (symbol, day) groups of 500 rows each, px constant
    // per group, so the px column wins the RLE size contest and the no-
    // predicate scan hands it downstream in run form.
    let groups = [("A", "d1", 10i64), ("A", "d2", 20), ("B", "d1", 30), ("B", "d2", 40)];
    let table_meta = engine.get_table_meta("main", "ticks").unwrap();
    let schema = crate::types::batch::Schema::new(table_meta.columns.clone());
    let mut batch = crate::types::batch::RecordBatch::with_capacity(schema.clone(), 2000);
    for (symbol, day, px) in groups {
        for _ in 0..500 {
            batch
                .column_mut(0)
                .unwrap()
                .push(&ScalarValue::VarChar(Some(symbol.to_string())))?;
            batch
                .column_mut(1)
                .unwrap()
                .push(&ScalarValue::VarChar(Some(day.to_string())))?;
            batch
                .column_mut(2)
                .unwrap()
                .push(&ScalarValue::Int64(Some(px)))?;
        }
    }
    let segment_path = engine.data_dir().join("main").join("ticks").join("seg_00001.dat");
    engine
        .segment_mgr()
        .write_new_segment(&segment_path, schema, 8192, &batch, 3)?;

    // Multi-key GROUP BY: the fast path only supports a single key, so this
    // exercises the generic per-row accumulator.
    let result = engine.execute(
        &mut session,
        "SELECT symbol, day, SUM(px), AVG(px) FROM ticks GROUP BY symbol, day ORDER BY symbol, day",
    )?;
    let ExecResult::Query { ref batches, .. } = result else {
        panic!("expected query result");
    };
    let mut rows: Vec<(String, String, i64, f64)> = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            rows.push((
                batch.scalar(row, 0).and_then(|v| v.to_text()).unwrap(),
                batch.scalar(row, 1).and_then(|v| v.to_text()).unwrap(),
                match batch.scalar(row, 2).unwrap() {
                    ScalarValue::Int64(Some(v)) => v,
                    other => panic!("unexpected sum {:?}", other),
                },
                match batch.scalar(row, 3).unwrap() {
                    ScalarValue::Float64(Some(v)) => v,
                    other => panic!("unexpected avg {:?}", other),
                },
            ));
        }
    }
    assert_eq!(rows.len(), 4, "all four groups must survive aggregation");
    for (row, (symbol, day, px)) in rows.iter().zip(groups.iter()) {
        assert_eq!(row.0, *symbol);
        assert_eq!(row.1, *day);
        assert_eq!(
            row.2,
            px * 500,
            "SUM over a run-encoded column must count every row"
        );
        assert_eq!(row.3, *px as f64, "AVG over a run-encoded column");
    }

    // HAVING forces the no-GROUP-BY streaming pipeline off the metadata fast
    // path; its column-wide sum kernel must see the run column too.
    let total = first_i64(
        engine.execute(
            &mut session,
            "SELECT SUM(px) FROM ticks HAVING SUM(px) > 0",
        )?,
        0,
        0,
    );
    assert_eq!(total, (10 + 20 + 30 + 40) * 500);
    Ok(())
}

/// P0 (UPDATE): unique-key validation must see keys in regions the WHERE
/// clause prunes away. Collecting validation keys inside the pruned dry-run
/// scan let `UPDATE ... SET uk = K` succeed when K already lived in another
/// row group / partition, committing a physical duplicate. Rows are seeded
/// directly through the segment writer so the row-group split (and zone-map
/// pruning) is deterministic.
#[test]
fn update_unique_check_sees_keys_in_pruned_row_groups() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();

    engine.execute(
        &mut session,
        "CREATE TABLE t (id INT64, uk VARCHAR, UNIQUE KEY uk_key (uk))",
    )?;

    // One segment, row_group_size 2: rg1 = ids {1,2}, rg2 = ids {3,4} with
    // uk='x' on id 3 — the duplicate target lives in the pruned row group.
    let table_meta = engine.get_table_meta("main", "t").unwrap();
    let schema = crate::types::batch::Schema::new(table_meta.columns.clone());
    let mut batch = crate::types::batch::RecordBatch::with_capacity(schema.clone(), 4);
    for (id, uk) in [(1i64, "a"), (2, "b"), (3, "x"), (4, "c")] {
        batch
            .column_mut(0)
            .unwrap()
            .push(&ScalarValue::Int64(Some(id)))?;
        batch
            .column_mut(1)
            .unwrap()
            .push(&ScalarValue::VarChar(Some(uk.to_string())))?;
    }
    let segment_path = engine.data_dir().join("main").join("t").join("seg_00001.dat");
    engine
        .segment_mgr()
        .write_new_segment(&segment_path, schema, 2, &batch, 3)?;

    // WHERE id=1 prunes rg2 (zone map ids 3..4): before the fix the
    // validation set never saw 'x' and this UPDATE committed a duplicate.
    let collided = engine.execute(&mut session, "UPDATE t SET uk = 'x' WHERE id = 1");
    assert!(
        collided.is_err(),
        "assigning an existing key from a pruned row group must be rejected"
    );

    // Clean abort: the matched row keeps its original key.
    let uk_of_one = first_text(
        engine.execute(&mut session, "SELECT uk FROM t WHERE id = 1")?,
        0,
        0,
    );
    assert_eq!(uk_of_one, "a");
    let count_x = first_i64(
        engine.execute(&mut session, "SELECT COUNT(*) FROM t WHERE uk = 'x'")?,
        0,
        0,
    );
    assert_eq!(count_x, 1);
    Ok(())
}

/// P0 (UPDATE), partition flavor: uniqueness is global across partitions, so
/// a partition-pruned UPDATE must still validate against OTHER partitions'
/// segments.
#[test]
fn update_unique_check_sees_keys_in_other_partitions() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();

    engine.execute(
        &mut session,
        "CREATE TABLE tp (id INT64, uk VARCHAR, region VARCHAR, UNIQUE KEY uk_key (uk)) PARTITION BY region",
    )?;

    let table_meta = engine.get_table_meta("main", "tp").unwrap();
    let schema = crate::types::batch::Schema::new(table_meta.columns.clone());
    let table_dir = engine.segment_mgr().table_dir("main", "tp");
    std::fs::create_dir_all(&table_dir)?;

    let seed_partition = |part: &str, rows: &[(i64, &str)]| -> Result<()> {
        let mut batch = crate::types::batch::RecordBatch::with_capacity(schema.clone(), rows.len());
        for (id, uk) in rows {
            batch
                .column_mut(0)
                .unwrap()
                .push(&ScalarValue::Int64(Some(*id)))?;
            batch
                .column_mut(1)
                .unwrap()
                .push(&ScalarValue::VarChar(Some(uk.to_string())))?;
            batch
                .column_mut(2)
                .unwrap()
                .push(&ScalarValue::VarChar(Some(part.to_string())))?;
        }
        let path = table_dir.join(format!("p{}_{:04}.dat", part, 1));
        engine.segment_mgr().write_new_segment(&path, schema.clone(), 8192, &batch, 3)?;
        Ok(())
    };
    seed_partition("A", &[(1, "a"), (2, "b")])?;
    seed_partition("B", &[(3, "x")])?;

    let collided =
        engine.execute(&mut session, "UPDATE tp SET uk = 'x' WHERE region = 'A' AND id = 1");
    assert!(
        collided.is_err(),
        "assigning a key that exists in another partition must be rejected"
    );
    let count_x = first_i64(
        engine.execute(&mut session, "SELECT COUNT(*) FROM tp WHERE uk = 'x'")?,
        0,
        0,
    );
    assert_eq!(count_x, 1);
    Ok(())
}

/// P0 (UPDATE): a failure between the segment-footer tombstones and the
/// image inserts used to leave the matched rows deleted with no replacement
/// (the undo snapshots were dropped as soon as apply_footers succeeded).
/// The statement now stages images into a private transaction and only
/// tombstones footers once staging succeeded, so any pre-commit failure is a
/// clean rollback.
#[test]
fn update_image_append_failure_rolls_back_completely() -> Result<()> {
    use crate::common::fault_injection::{arm, FaultPoint};

    let dir = tempfile::tempdir().unwrap();
    // Small row-group target: 4 matched rows produce 2 image batches, so the
    // injected failure at the second append leaves one batch staged.
    let engine = Engine::new_with_storage_config(
        dir.path(),
        "main",
        crate::storage::segment_manager::SegmentConfig {
            row_group_target_rows: 2,
            ..Default::default()
        },
    )?;
    let mut session = session();

    engine.execute(&mut session, "CREATE TABLE t (id INT64, v INT64)")?;
    let table_meta = engine.get_table_meta("main", "t").unwrap();
    let schema = crate::types::batch::Schema::new(table_meta.columns.clone());
    let mut batch = crate::types::batch::RecordBatch::with_capacity(schema.clone(), 4);
    for id in 1..=4i64 {
        batch
            .column_mut(0)
            .unwrap()
            .push(&ScalarValue::Int64(Some(id)))?;
        batch
            .column_mut(1)
            .unwrap()
            .push(&ScalarValue::Int64(Some(id * 10)))?;
    }
    let segment_path = engine.data_dir().join("main").join("t").join("seg_00001.dat");
    // Row-group size 2: two row groups -> two image batches, so the injected
    // failure at the second append leaves one batch staged.
    engine
        .segment_mgr()
        .write_new_segment(&segment_path, schema, 2, &batch, 3)?;

    arm(FaultPoint::UpdateImageAppend);
    let failed = engine.execute(&mut session, "UPDATE t SET v = 99 WHERE id >= 1");
    assert!(failed.is_err(), "injected append failure must surface as an error");

    // Exact rollback: every row keeps its original value, none lost.
    for id in 1..=4i64 {
        let value = first_i64(
            engine.execute(&mut session, &format!("SELECT v FROM t WHERE id = {}", id))?,
            0,
            0,
        );
        assert_eq!(value, id * 10, "row {} must keep its pre-update value", id);
    }
    let count = first_i64(engine.execute(&mut session, "SELECT COUNT(*) FROM t")?, 0, 0);
    assert_eq!(count, 4);

    // The table is fully usable afterwards: the same UPDATE succeeds.
    engine.execute(&mut session, "UPDATE t SET v = 99 WHERE id >= 1")?;
    for id in 1..=4i64 {
        let value = first_i64(
            engine.execute(&mut session, &format!("SELECT v FROM t WHERE id = {}", id))?,
            0,
            0,
        );
        assert_eq!(value, 99);
    }
    Ok(())
}

/// P0 (txn UPDATE): a failed in-transaction UPDATE must leave the registry
/// untouched — the old code registered tombstones during the scan, so a
/// statement that failed on budget (no IO fault needed) left them behind and
/// the transaction's COMMIT physically deleted the rows without inserting
/// their replacements.
#[test]
fn txn_update_over_budget_commit_keeps_rows() -> Result<()> {
    // Measured estimates: 8-row image batch ≈ 144 bytes (C8 doubles it),
    // 4-row ≈ 72, positions 12x48 = 576 — the statement's total projected
    // charge (~1014) exceeds 700, so the pre-check must reject it BEFORE any
    // registration or append. (The injected variant below pins the same
    // contract at the append step.)
    crate::storage::core::txn_mutations::set_txn_mutation_budget_for_tests(Some(700));
    let result = (|| {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::new(dir.path(), "main")?;
        let mut session = session();

        engine.execute(&mut session, "CREATE TABLE t (id INT64, v INT64)")?;
        let table_meta = engine.get_table_meta("main", "t").unwrap();
        let schema = crate::types::batch::Schema::new(table_meta.columns.clone());
        let mut batch = crate::types::batch::RecordBatch::with_capacity(schema.clone(), 8);
        for id in 1..=8i64 {
            batch
                .column_mut(0)
                .unwrap()
                .push(&ScalarValue::Int64(Some(id)))?;
            batch
                .column_mut(1)
                .unwrap()
                .push(&ScalarValue::Int64(Some(id * 10)))?;
        }
        // Two segments: the old code registered the FIRST segment's
        // tombstones before the budget charge failed on the second — the
        // later COMMIT then deleted those rows.
        engine
            .segment_mgr()
            .write_new_segment(
                &engine.data_dir().join("main").join("t").join("seg_00001.dat"),
                schema.clone(),
                4,
                &batch,
                3,
            )?;
        let mut second = crate::types::batch::RecordBatch::with_capacity(schema.clone(), 4);
        for id in 9..=12i64 {
            second
                .column_mut(0)
                .unwrap()
                .push(&ScalarValue::Int64(Some(id)))?;
            second
                .column_mut(1)
                .unwrap()
                .push(&ScalarValue::Int64(Some(id * 10)))?;
        }
        engine
            .segment_mgr()
            .write_new_segment(
                &engine.data_dir().join("main").join("t").join("seg_00002.dat"),
                schema,
                4,
                &second,
                3,
            )?;

        engine.execute(&mut session, "BEGIN")?;
        let over_budget = engine.execute(&mut session, "UPDATE t SET v = 99 WHERE id >= 1");
        assert!(over_budget.is_err(), "statement must fail against the 1 KiB budget");
        engine.execute(&mut session, "COMMIT")?;

        // Every row must still exist with its original value.
        let count = first_i64(engine.execute(&mut session, "SELECT COUNT(*) FROM t")?, 0, 0);
        assert_eq!(count, 12, "no row may disappear through the failed statement");
        for id in 1..=12i64 {
            let value = first_i64(
                engine.execute(&mut session, &format!("SELECT v FROM t WHERE id = {}", id))?,
                0,
                0,
            );
            assert_eq!(value, id * 10, "row {} must keep its pre-statement value", id);
        }
        Ok(())
    })();
    crate::storage::core::txn_mutations::set_txn_mutation_budget_for_tests(None);
    result
}

/// P0 (txn UPDATE), injected variant: a failure at the image-append step
/// discards all staged tombstones, so a later COMMIT is a no-op.
#[test]
fn txn_update_image_append_failure_discards_staging() -> Result<()> {
    use crate::common::fault_injection::{arm, FaultPoint};

    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();

    engine.execute(&mut session, "CREATE TABLE t (id INT64, v INT64)")?;
    let table_meta = engine.get_table_meta("main", "t").unwrap();
    let schema = crate::types::batch::Schema::new(table_meta.columns.clone());
    let mut batch = crate::types::batch::RecordBatch::with_capacity(schema.clone(), 4);
    for id in 1..=4i64 {
        batch
            .column_mut(0)
            .unwrap()
            .push(&ScalarValue::Int64(Some(id)))?;
        batch
            .column_mut(1)
            .unwrap()
            .push(&ScalarValue::Int64(Some(id * 10)))?;
    }
    let segment_path = engine.data_dir().join("main").join("t").join("seg_00001.dat");
    engine
        .segment_mgr()
        .write_new_segment(&segment_path, schema, 2, &batch, 3)?;

    engine.execute(&mut session, "BEGIN")?;
    arm(FaultPoint::TxnImageAppend);
    let failed = engine.execute(&mut session, "UPDATE t SET v = 99 WHERE id >= 1");
    assert!(failed.is_err(), "injected append failure must surface as an error");
    engine.execute(&mut session, "COMMIT")?;

    let count = first_i64(engine.execute(&mut session, "SELECT COUNT(*) FROM t")?, 0, 0);
    assert_eq!(count, 4, "no row may disappear through the failed statement");
    for id in 1..=4i64 {
        let value = first_i64(
            engine.execute(&mut session, &format!("SELECT v FROM t WHERE id = {}", id))?,
            0,
            0,
        );
        assert_eq!(value, id * 10, "row {} must keep its pre-statement value", id);
    }
    Ok(())
}

/// P1 (fast INSERT): the fast path's byte scanner kept quotes glued onto the
/// table token, so `INSERT INTO "order" ...` was rejected with a bogus
/// "table does not exist" before the standard parser ever ran. Quoted
/// identifiers and lookup misses must fall back to the standard parser.
#[test]
fn fast_insert_falls_back_for_quoted_table_names() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = session();

    engine.execute(
        &mut session,
        "CREATE TABLE \"order\" (id INT64, name VARCHAR)",
    )?;
    engine.execute(&mut session, "INSERT INTO \"order\" (id, name) VALUES (1, 'a')")?;
    let count = first_i64(
        engine.execute(&mut session, "SELECT COUNT(*) FROM \"order\"")?,
        0,
        0,
    );
    assert_eq!(count, 1, "quoted table names must work end to end");

    // A miss falls back instead of short-circuiting with the fast path's own
    // error text: the standard parser still reports the missing table.
    let missing = engine.execute(&mut session, "INSERT INTO no_such_table (id) VALUES (1)");
    let error = missing.err().expect("missing table must error");
    assert!(
        error.to_string().contains("no_such_table"),
        "standard parser must own the missing-table verdict: {error}"
    );
    Ok(())
}
