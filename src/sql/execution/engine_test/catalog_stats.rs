//! Regression tests for the merged catalog-stats save path: DELETE retracts,
//! rollback retractions, and restart recovery must all converge on the same
//! row_count with the debounced gate replacing per-statement catalog saves.

use super::*;
use crate::sql::execution::engine::ExecResult;
use std::sync::Arc;

fn table_row_count(engine: &Engine, db: &str, table: &str) -> i64 {
    engine
        .get_table_meta(db, table)
        .unwrap_or_else(|| panic!("table {db}.{table} missing from catalog"))
        .row_count
}

fn insert_rows(engine: &Engine, session: &mut Session, table: &str, count: u64) -> Result<()> {
    let values: Vec<String> = (0..count).map(|id| format!("({id})")).collect();
    engine.execute(
        session,
        &format!("INSERT INTO {table} VALUES {}", values.join(", ")),
    )?;
    Ok(())
}

#[test]
fn delete_retract_survives_flush_and_restart() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut sess = session();
    engine.execute(&mut sess, "CREATE TABLE t (id INT64)")?;
    insert_rows(&engine, &mut sess, "t", 3)?;
    let ExecResult::Ok { rows_affected } =
        engine.execute(&mut sess, "DELETE FROM t WHERE id = 2")?
    else {
        panic!("expected ok result");
    };
    assert_eq!(rows_affected, 1);
    assert_eq!(table_row_count(&engine, "main", "t"), 2);

    // The flush checkpoint force-saves the catalog; restart must load the
    // retracted stats (restart recovery reads the persisted snapshot).
    engine.flush_table_buffers("main", "t")?;
    drop(engine);
    let engine = Engine::new(dir.path(), "main")?;
    let mut sess = session();
    assert_eq!(table_row_count(&engine, "main", "t"), 2);
    assert_eq!(
        first_i64(engine.execute(&mut sess, "SELECT COUNT(*) FROM t")?, 0, 0),
        2
    );
    Ok(())
}

#[test]
fn rollback_retraction_is_persisted_without_flush() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut sess = session();
    engine.execute(&mut sess, "CREATE TABLE t (id INT64)")?;
    insert_rows(&engine, &mut sess, "t", 2)?;
    engine.flush_table_buffers("main", "t")?;

    engine.execute(&mut sess, "BEGIN")?;
    insert_rows(&engine, &mut sess, "t", 2)?;
    engine.execute(&mut sess, "ROLLBACK")?;

    // The rollback retracts the statement-time counts in memory and forces
    // one save per touched database (merged from the previous per-table
    // saves); no flush is needed for the persisted snapshot to catch up.
    assert_eq!(table_row_count(&engine, "main", "t"), 2);
    drop(engine);
    let engine = Engine::new(dir.path(), "main")?;
    assert_eq!(table_row_count(&engine, "main", "t"), 2);
    Ok(())
}

#[test]
fn concurrent_deletes_and_ddl_keep_row_count_consistent() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(Engine::new(dir.path(), "main")?);
    let total_rows: u64 = 400;
    {
        let mut sess = session();
        engine.execute(&mut sess, "CREATE TABLE t (id INT64)")?;
        insert_rows(&engine, &mut sess, "t", total_rows)?;
    }

    let workers: u64 = 4;
    let per_worker = total_rows / workers;
    let mut handles = Vec::new();
    for worker in 0..workers {
        let engine = Arc::clone(&engine);
        handles.push(std::thread::spawn(move || -> Result<u64> {
            let mut sess = session();
            let lower = worker * per_worker;
            let upper = lower + per_worker;
            let ExecResult::Ok { rows_affected } = engine.execute(
                &mut sess,
                &format!("DELETE FROM t WHERE id >= {lower} AND id < {upper}"),
            )?
            else {
                panic!("expected ok result");
            };
            Ok(rows_affected)
        }));
    }
    // Concurrent DDL churn: every CREATE TABLE force-saves the whole catalog
    // while the delete retractions mutate row_count under the same write
    // lock — the C22 lost-update race this path must never reintroduce.
    let ddl_engine = Arc::clone(&engine);
    let ddl_handle = std::thread::spawn(move || -> Result<()> {
        let mut sess = session();
        for round in 0..8 {
            ddl_engine.execute(
                &mut sess,
                &format!("CREATE TABLE ddl_churn_{round} (id INT64)"),
            )?;
        }
        Ok(())
    });

    let mut deleted = 0u64;
    for handle in handles {
        deleted += handle.join().unwrap()?;
    }
    ddl_handle.join().unwrap()?;

    assert_eq!(deleted, total_rows);
    assert_eq!(table_row_count(&engine, "main", "t"), 0);

    // Flush checkpoint + restart: persisted stats match memory and the data.
    engine.flush_table_buffers("main", "t")?;
    drop(engine);
    let engine = Engine::new(dir.path(), "main")?;
    let mut sess = session();
    assert_eq!(table_row_count(&engine, "main", "t"), 0);
    assert_eq!(
        first_i64(engine.execute(&mut sess, "SELECT COUNT(*) FROM t")?, 0, 0),
        0
    );
    Ok(())
}
