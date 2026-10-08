use super::*;
use crate::types::{
    array::ColumnArray,
    batch::{ColumnDef, RecordBatch, Schema},
};

#[test]
fn resident_key_changes_retire_old_and_publish_new_keys() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut s = session();
    for flush in [false, true] {
        engine.execute(
            &mut s,
            "CREATE TABLE u (id INT64 PRIMARY KEY, uk INT64, UNIQUE KEY uq (uk))",
        )?;
        engine.execute(&mut s, "INSERT INTO u VALUES (1,100)")?;
        if flush {
            engine.execute(&mut s, "FLUSH TABLE u")?;
        }
        engine.execute(&mut s, "UPDATE u SET id=2, uk=200 WHERE id=1")?;
        assert!(engine
            .execute(&mut s, "INSERT INTO u VALUES (2,300)")
            .is_err());
        assert!(engine
            .execute(&mut s, "INSERT INTO u VALUES (3,200)")
            .is_err());
        engine.execute(&mut s, "INSERT INTO u VALUES (1,100)")?;
        assert_eq!(
            first_i64(engine.execute(&mut s, "SELECT COUNT(*) FROM u")?, 0, 0),
            2
        );
        engine.execute(&mut s, "DELETE FROM u WHERE id=2")?;
        engine.execute(&mut s, "INSERT INTO u VALUES (2,200)")?;
        engine.execute(&mut s, "DROP TABLE u")?;
    }
    Ok(())
}

#[test]
fn assignments_validate_before_any_transaction_or_storage_change() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut s = session();
    engine.execute(
        &mut s,
        "CREATE TABLE t (id INT64 NOT NULL, v INT64 NOT NULL, d DECIMAL(10,2))",
    )?;
    engine.execute(&mut s, "BEGIN")?;
    engine.execute(&mut s, "INSERT INTO t VALUES (1,10,1.23)")?;
    assert!(engine
        .execute(&mut s, "UPDATE t SET v=99, nosuch=5")
        .is_err());
    assert!(engine.execute(&mut s, "UPDATE t SET v=NULL").is_err());
    assert!(engine.execute(&mut s, "UPDATE t SET id=NULL").is_err());
    assert_eq!(
        first_i64(engine.execute(&mut s, "SELECT v FROM t")?, 0, 0),
        10
    );
    engine.execute(&mut s, "UPDATE t SET d=4.56")?;
    engine.execute(&mut s, "COMMIT")?;
    let ExecResult::Query { batches, .. } = engine.execute(&mut s, "SELECT d FROM t")? else {
        panic!()
    };
    assert_eq!(
        batches[0].scalar(0, 0),
        Some(ScalarValue::Decimal(Some(456)))
    );
    engine.execute(&mut s, "FLUSH TABLE t")?;
    engine.execute(&mut s, "UPDATE t SET d=7.89")?;
    let ExecResult::Query { batches, .. } = engine.execute(&mut s, "SELECT d FROM t")? else {
        panic!()
    };
    assert_eq!(
        batches[0].scalar(0, 0),
        Some(ScalarValue::Decimal(Some(789)))
    );
    Ok(())
}

#[test]
fn delete_read_failure_preserves_resident_rows_and_indexes() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut s = session();
    engine.execute(&mut s, "CREATE TABLE t (id INT64, UNIQUE KEY uq (id))")?;
    engine.execute(&mut s, "INSERT INTO t VALUES (1)")?;
    let broken = dir.path().join("main/t/seg_99999.dat");
    std::fs::write(&broken, b"broken")?;
    assert!(engine.execute(&mut s, "DELETE FROM t").is_err());
    std::fs::remove_file(broken)?;
    assert_eq!(
        first_i64(engine.execute(&mut s, "SELECT COUNT(*) FROM t")?, 0, 0),
        1
    );
    assert!(engine.execute(&mut s, "INSERT INTO t VALUES (1)").is_err());
    Ok(())
}

#[test]
fn mutation_budget_failure_keeps_resident_snapshot() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut s = session();
    engine.execute(&mut s, "CREATE TABLE t (id INT64, v INT64)")?;
    engine.execute(&mut s, "INSERT INTO t VALUES (1,10),(2,20),(3,30)")?;
    crate::storage::core::txn_mutations::set_txn_mutation_budget_for_tests(Some(1));
    let update = engine.execute(&mut s, "UPDATE t SET v=99");
    let delete = engine.execute(&mut s, "DELETE FROM t WHERE id=1");
    crate::storage::core::txn_mutations::set_txn_mutation_budget_for_tests(None);
    assert!(update.is_err());
    assert!(delete.is_err());
    assert_eq!(
        first_i64(engine.execute(&mut s, "SELECT SUM(v) FROM t")?, 0, 0),
        60
    );
    Ok(())
}

#[test]
fn window_moving_frames_match_reference_with_peers_and_nulls() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut s = session();
    engine.execute(&mut s, "CREATE TABLE w (id INT64, k INT64, v INT64)")?;
    engine.execute(
        &mut s,
        "INSERT INTO w VALUES (1,1,10),(2,1,NULL),(3,2,30),(4,3,50),(5,4,10)",
    )?;
    let ExecResult::Query {batches,..}=engine.execute(&mut s,"SELECT id, AVG(v) OVER (ORDER BY id) AS av, SUM(v) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS s, MIN(v) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS m, COUNT(v) OVER (ORDER BY k RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS c FROM w ORDER BY id")? else {panic!()};
    let rows: Vec<_> = batches
        .iter()
        .flat_map(|b| {
            (0..b.num_rows()).map(move |r| {
                (0..b.num_columns())
                    .map(|c| b.scalar(r, c).unwrap())
                    .collect::<Vec<_>>()
            })
        })
        .collect();
    for (i, row) in rows.iter().enumerate() {
        assert_eq!(
            row[1],
            ScalarValue::Float64(Some([10., 10., 20., 30., 25.][i]))
        );
        assert_eq!(row[2], ScalarValue::Int64(Some([10, 40, 80, 90, 60][i])));
        assert_eq!(row[3], ScalarValue::Int64(Some([10, 10, 30, 30, 10][i])));
        assert_eq!(row[4], ScalarValue::Int64(Some([1, 1, 2, 2, 2][i])));
    }
    Ok(())
}

#[test]
fn blocking_query_operators_reject_over_budget_inputs() -> Result<()> {
    use crate::sql::{
        execution::operators::{scan::ScanExec, topn::TopNExec, window::WindowExec, ExecutionOp},
        planner::{LogicalExpr, SortKey, WindowFuncKind, WindowFuncPlan},
    };
    let schema = Schema::new(vec![ColumnDef::new("v", DataType::Int64)]);
    let mut column = ColumnArray::empty(DataType::Int64);
    for i in 0..1000 {
        column.push(&ScalarValue::Int64(Some(i)))?;
    }
    let batch = RecordBatch::try_new(schema.clone(), vec![column])?;
    let input = || Box::new(ScanExec::new(vec![batch.clone()], None)) as Box<dyn ExecutionOp>;
    let mut window = WindowExec::new(
        input(),
        vec![WindowFuncPlan {
            kind: WindowFuncKind::RowNumber,
            partition_by: vec![],
            order_by: vec![],
            frame: None,
            output_name: "rn".into(),
        }],
        schema.clone(),
    )
    .with_memory_limit_bytes(1024);
    assert!(window.next_batch().is_err());
    let mut top = TopNExec::new(
        vec![SortKey {
            expr: LogicalExpr::Column(0),
            desc: false,
        }],
        input(),
        schema,
    )
    .with_capacity(900)
    .with_memory_limit_bytes(1024);
    assert!(top.next_batch().is_err());
    Ok(())
}

#[test]
fn failed_transaction_image_append_preserves_prior_inserts_and_updates() -> Result<()> {
    use crate::common::fault_injection::{arm, FaultPoint};
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut s = session();
    engine.execute(&mut s, "CREATE TABLE t (id INT64, v INT64)")?;
    engine.execute(&mut s, "INSERT INTO t VALUES (1,10)")?;
    engine.execute(&mut s, "FLUSH TABLE t")?;
    engine.execute(&mut s, "BEGIN")?;
    engine.execute(&mut s, "INSERT INTO t VALUES (2,20)")?;
    engine.execute(&mut s, "UPDATE t SET v=25 WHERE id=2")?;
    arm(FaultPoint::TxnImageAppend);
    assert!(engine.execute(&mut s, "UPDATE t SET v=99").is_err());
    assert_eq!(
        first_i64(engine.execute(&mut s, "SELECT SUM(v) FROM t")?, 0, 0),
        35
    );
    engine.execute(&mut s, "UPDATE t SET v=30 WHERE id=1")?;
    assert_eq!(
        first_i64(engine.execute(&mut s, "SELECT SUM(v) FROM t")?, 0, 0),
        55
    );
    engine.execute(&mut s, "COMMIT")?;
    assert_eq!(
        first_i64(engine.execute(&mut s, "SELECT SUM(v) FROM t")?, 0, 0),
        55
    );
    Ok(())
}

#[test]
fn shared_mutations_bound_copying_fragmentation_and_dead_payload() -> Result<()> {
    use std::sync::Arc;
    let mut data = ColumnArray::empty(DataType::VarChar);
    for i in 0..1024 {
        data.push(&ScalarValue::VarChar(Some(format!(
            "{i:04}{}",
            "x".repeat(1024)
        ))))?;
    }
    let leaf = Arc::new(data);
    let positions: Arc<[u32]> = (0..1024).collect::<Vec<_>>().into();
    let mut view = ColumnArray::selected(
        vec![(leaf.clone(), positions)].into(),
        DataType::VarChar,
        1024,
    );
    let original_bytes = leaf.estimated_size_bytes();
    assert_eq!(view.bitmap().null_count(), 0);
    let ColumnArray::Selected { flat, .. } = &view else {
        panic!()
    };
    assert!(
        flat.get().is_none(),
        "null checks must not copy wide payloads"
    );
    for row in 0..100 {
        let mut mask = vec![false; 1024];
        mask[row] = true;
        view = view.shared_update(&mask, &ScalarValue::VarChar(Some("updated".into())))?;
        let ColumnArray::Selected { parts, .. } = &view else {
            panic!("mutation must retain an immutable view")
        };
        assert!(parts.len() <= 64);
        assert!(
            parts.iter().all(|(source, _)| !source.is_selected()),
            "no parent snapshot chain"
        );
    }
    let sparse = view
        .shared_take(vec![100, 101, 102].into())
        .compact_sparse_selection();
    assert!(
        sparse.estimated_size_bytes() < original_bytes / 8,
        "mostly dead leaves must be reclaimed"
    );
    assert_eq!(sparse.scalar(0), leaf.scalar(100));
    let mut uneven = ColumnArray::empty(DataType::VarChar);
    for value in ["x".repeat(128 * 1024), "a".into(), "b".into()] {
        uneven.push(&ScalarValue::VarChar(Some(value)))?;
    }
    let uneven = ColumnArray::selected(
        vec![(Arc::new(uneven), vec![0, 1, 2].into())].into(),
        DataType::VarChar,
        3,
    );
    let surviving = uneven
        .shared_take(vec![1, 2].into())
        .compact_sparse_selection();
    assert!(
        surviving.estimated_size_bytes() < 1024,
        "one deleted wide row must not stay pinned by many tiny rows"
    );

    Ok(())
}

#[test]
fn update_timestamp_conversion_matches_insert_session_timezone() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut s = session();
    s.timezone_offset_hours = 8;
    engine.execute(&mut s, "CREATE TABLE t (id INT64, ts TIMESTAMP)")?;
    engine.execute(
        &mut s,
        "INSERT INTO t VALUES (1,'2026-10-05 12:34:56'),(2,'2026-10-01 00:00:00')",
    )?;
    for flush in [false, true] {
        if flush {
            engine.execute(&mut s, "FLUSH TABLE t")?;
        }
        engine.execute(&mut s, "UPDATE t SET ts='2026-10-05 12:34:56' WHERE id=2")?;
        let ExecResult::Query { batches, .. } =
            engine.execute(&mut s, "SELECT ts FROM t ORDER BY id")?
        else {
            panic!()
        };
        assert_eq!(batches[0].scalar(0, 0), batches[0].scalar(1, 0));
    }
    Ok(())
}

#[test]
fn transaction_delete_read_failure_preserves_own_inserts() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut s = session();
    engine.execute(&mut s, "CREATE TABLE t (id INT64, v INT64)")?;
    engine.execute(&mut s, "BEGIN")?;
    engine.execute(&mut s, "INSERT INTO t VALUES (1,10),(2,20)")?;
    let broken = dir.path().join("main/t/seg_99999.dat");
    std::fs::write(&broken, b"broken")?;
    assert!(engine.execute(&mut s, "DELETE FROM t").is_err());
    std::fs::remove_file(broken)?;
    assert_eq!(
        first_i64(engine.execute(&mut s, "SELECT COUNT(*) FROM t")?, 0, 0),
        2
    );
    engine.execute(&mut s, "COMMIT")?;
    assert_eq!(
        first_i64(engine.execute(&mut s, "SELECT SUM(v) FROM t")?, 0, 0),
        30
    );
    Ok(())
}
