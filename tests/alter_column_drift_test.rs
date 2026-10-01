//! Regression: catalog↔physical column alignment across DROP/RENAME COLUMN.
//!
//! Segments and memtables written before a `DROP COLUMN` keep the pre-DDL
//! physical layout, while catalog indices shift immediately. DML and SELECT
//! used to apply catalog indices positionally to those stale layouts,
//! silently writing, deleting, and reading the WRONG columns.

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

fn exec(engine: &Engine, session: &mut Session, sql: &str) -> ExecResult {
    match engine.execute(session, sql) {
        Ok(result) => result,
        Err(error) => panic!("'{sql}' failed: {error}"),
    }
}

fn query_all(engine: &Engine, session: &mut Session, sql: &str) -> Vec<Vec<String>> {
    let result = exec(engine, session, sql);
    let ExecResult::Query { batches, .. } = result else {
        panic!("'{sql}' expected query result");
    };
    let mut rows = Vec::new();
    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            let row: Vec<String> = (0..batch.num_columns())
                .map(|col| {
                    batch
                        .scalar(row_idx, col)
                        .unwrap_or(ScalarValue::Int64(None))
                        .to_text()
                        .unwrap_or_else(|| "NULL".to_string())
                })
                .collect();
            rows.push(row);
        }
    }
    rows
}

#[test]
fn drop_column_update_writes_the_right_column() {
    let (_temp_dir, engine, mut session) = create_engine();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT, c INT)",
    );
    exec(
        &engine,
        &mut session,
        "INSERT INTO t VALUES (1, 10, 20, 30), (2, 11, 21, 31)",
    );
    exec(&engine, &mut session, "FLUSH TABLE t");
    exec(&engine, &mut session, "ALTER TABLE t DROP COLUMN b");

    // Pre-DDL segments still carry 4 physical columns while the catalog has
    // 3; `SET c = 99` used to land in the physical slot that used to be b.
    exec(&engine, &mut session, "UPDATE t SET c = 99 WHERE id = 1");

    let rows = query_all(&engine, &mut session, "SELECT id, a, c FROM t ORDER BY id");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], vec!["1", "10", "99"], "c updated, a untouched");
    assert_eq!(rows[1], vec!["2", "11", "31"], "other row untouched");
}

#[test]
fn drop_column_delete_removes_the_right_rows() {
    let (_temp_dir, engine, mut session) = create_engine();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)",
    );
    exec(
        &engine,
        &mut session,
        "INSERT INTO t VALUES (1, 100, 200), (2, 101, 201), (3, 102, 202)",
    );
    exec(&engine, &mut session, "FLUSH TABLE t");
    exec(&engine, &mut session, "ALTER TABLE t DROP COLUMN b");

    exec(&engine, &mut session, "DELETE FROM t WHERE a = 101");

    let rows = query_all(&engine, &mut session, "SELECT id, a FROM t ORDER BY id");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], vec!["1", "100"]);
    assert_eq!(rows[1], vec!["3", "102"]);
}

#[test]
fn drop_column_select_reads_aligned_columns() {
    let (_temp_dir, engine, mut session) = create_engine();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)",
    );
    exec(&engine, &mut session, "INSERT INTO t VALUES (1, 10, 20)");
    exec(&engine, &mut session, "FLUSH TABLE t");
    exec(&engine, &mut session, "ALTER TABLE t DROP COLUMN b");

    let rows = query_all(&engine, &mut session, "SELECT id, a FROM t");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], vec!["1", "10"]);
}

#[test]
fn rename_column_dml_and_select_follow_the_new_name() {
    let (_temp_dir, engine, mut session) = create_engine();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)",
    );
    exec(&engine, &mut session, "INSERT INTO t VALUES (1, 10, 20)");
    exec(&engine, &mut session, "FLUSH TABLE t");
    exec(&engine, &mut session, "ALTER TABLE t RENAME COLUMN b TO c");

    exec(&engine, &mut session, "UPDATE t SET c = 21 WHERE id = 1");
    let rows = query_all(&engine, &mut session, "SELECT id, a, c FROM t");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], vec!["1", "10", "21"]);
}

/// ADD COLUMN is catalog-only metadata: a resident memtable predating the
/// ALTER keeps its insert-time layout, and the read path used to apply
/// catalog indices positionally to it — predicates and projections on the
/// added column silently missed every resident row, and SELECT * came back
/// at the old width. Scans now align the batch to the catalog and fill the
/// added column with its DEFAULT, the same semantics as the segment-side
/// default-fill read.
#[test]
fn resident_memtable_predating_add_column_reads_default_fill() {
    let (_temp_dir, engine, mut session) = create_engine();
    exec(&engine, &mut session, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&engine, &mut session, "INSERT INTO t VALUES (1, 10), (2, 20)");
    // Rows stay resident: no flush before the ALTER.
    exec(&engine, &mut session, "ALTER TABLE t ADD COLUMN score INT DEFAULT 7");

    // SELECT *: the added column materializes with its DEFAULT.
    let mut rows = query_all(&engine, &mut session, "SELECT * FROM t");
    rows.sort();
    assert_eq!(rows, vec![vec!["1", "10", "7"], vec!["2", "20", "7"]]);

    // Predicates on the added column match the default.
    assert_eq!(
        query_all(&engine, &mut session, "SELECT id FROM t WHERE score = 7"),
        vec![vec!["1"], vec!["2"]]
    );
    assert_eq!(
        query_all(&engine, &mut session, "SELECT id FROM t WHERE score = 8"),
        Vec::<Vec<String>>::new()
    );
    // COUNT(*) fast path over the resident rows.
    assert_eq!(
        query_all(&engine, &mut session, "SELECT COUNT(*) FROM t WHERE score = 7"),
        vec![vec!["2"]]
    );

    // NOTE: INSERTs written after the ALTER are a separate, write-side
    // limitation: both the hot-mirror feed and the memtable append reject a
    // new-width batch while the stale-width memtable/store is still active
    // ("column count mismatch"), so new rows only land once the stale
    // memtable rotates out. This test pins the read side.
}

/// Same drift for DROP COLUMN: the resident batch keeps the dropped column
/// while catalog indices shift, so reading a surviving column by catalog
/// position used to return the dropped neighbor's values.
#[test]
fn resident_memtable_predating_drop_column_reads_surviving_columns() {
    let (_temp_dir, engine, mut session) = create_engine();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)",
    );
    exec(&engine, &mut session, "INSERT INTO t VALUES (1, 10, 20), (2, 11, 21)");
    // Rows stay resident: no flush before the ALTER.
    exec(&engine, &mut session, "ALTER TABLE t DROP COLUMN a");

    let mut rows = query_all(&engine, &mut session, "SELECT id, b FROM t");
    rows.sort();
    assert_eq!(rows, vec![vec!["1", "20"], vec!["2", "21"]]);
    assert_eq!(
        query_all(&engine, &mut session, "SELECT COUNT(*) FROM t WHERE b = 21"),
        vec![vec!["1"]]
    );
}
