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
    exec(
        &engine,
        &mut session,
        "CREATE TABLE t (id INT PRIMARY KEY, v INT)",
    );
    exec(
        &engine,
        &mut session,
        "INSERT INTO t VALUES (1, 10), (2, 20)",
    );
    // Rows stay resident: no flush before the ALTER.
    exec(
        &engine,
        &mut session,
        "ALTER TABLE t ADD COLUMN score INT DEFAULT 7",
    );

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
        query_all(
            &engine,
            &mut session,
            "SELECT COUNT(*) FROM t WHERE score = 7"
        ),
        vec![vec!["2"]]
    );

    // Width-changing DDL converges hot state first: the ALTER flushed the
    // resident rows into a segment, so post-ALTER inserts land in a fresh
    // catalog-width memtable instead of failing the append width check.
    exec(&engine, &mut session, "INSERT INTO t VALUES (3, 30, 8)");
    assert_eq!(
        query_all(&engine, &mut session, "SELECT id FROM t WHERE score = 8"),
        vec![vec!["3"]]
    );
    assert_eq!(
        query_all(&engine, &mut session, "SELECT COUNT(*) FROM t"),
        vec![vec!["3"]]
    );
}

/// The invariant behind the read-side fixes: a width-changing ALTER converges
/// hot state (memtables, durable tails) into segments inside the table
/// mutation lock BEFORE the catalog flips, so no stale-layout hot state can
/// outlive the ALTER and post-ALTER writes just work.
#[test]
fn add_column_converges_hot_state_before_catalog_flip() {
    use taibai::storage::core::durable_tail;

    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("create engine");
    let mut session = Session::new("root".to_string(), "main".to_string());
    exec(
        &engine,
        &mut session,
        "CREATE TABLE t (id INT PRIMARY KEY, v INT)",
    );
    exec(
        &engine,
        &mut session,
        "INSERT INTO t VALUES (1, 10), (2, 20)",
    );
    let table_dir = temp_dir.path().join("main").join("t");

    exec(
        &engine,
        &mut session,
        "ALTER TABLE t ADD COLUMN score INT DEFAULT 7",
    );

    // Hot state converged: the memtable's rows are durable in a segment and
    // no stale tail generation remains.
    let segments = taibai::storage::segment_manager::SegmentManager::discover_segments(&table_dir)
        .expect("discover segments");
    assert_eq!(segments.len(), 1, "resident rows flushed into a segment");
    assert_eq!(
        durable_tail::generation_file_count(&table_dir).expect("tail dir"),
        0,
        "no stale tail generations may outlive the ALTER"
    );

    // Reads span the flushed rows and the new default.
    assert_eq!(
        query_all(
            &engine,
            &mut session,
            "SELECT COUNT(*) FROM t WHERE score = 7"
        ),
        vec![vec!["2"]]
    );
    // Post-ALTER inserts land in a fresh catalog-width memtable.
    exec(&engine, &mut session, "INSERT INTO t VALUES (3, 30, 8)");
    assert_eq!(
        query_all(&engine, &mut session, "SELECT COUNT(*) FROM t"),
        vec![vec!["3"]]
    );
    engine.segment_mgr().shutdown();
}

/// Width-changing DDL must not run while a transaction has written to the
/// table: the flush skips transactional memtables, so the flip would leave a
/// stale-layout transactional memtable behind.
#[test]
fn add_column_refuses_open_transaction_with_writes() {
    let (_temp_dir, engine, mut session) = create_engine();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE t (id INT PRIMARY KEY, v INT)",
    );

    // Another session's open transaction with writes: the flush skips its
    // transactional memtable, so the layout flip would strand it stale-width.
    let mut txn_session = Session::new("root".to_string(), "main".to_string());
    exec(&engine, &mut txn_session, "BEGIN");
    exec(&engine, &mut txn_session, "INSERT INTO t VALUES (1, 10)");

    let error = engine
        .execute(&mut session, "ALTER TABLE t ADD COLUMN score INT DEFAULT 7")
        .expect_err("ALTER must refuse while another transaction wrote the table");
    assert!(
        error.to_string().contains("commit or rollback"),
        "unexpected error: {error}"
    );

    exec(&engine, &mut txn_session, "ROLLBACK");
    exec(
        &engine,
        &mut session,
        "ALTER TABLE t ADD COLUMN score INT DEFAULT 7",
    );
    exec(&engine, &mut session, "INSERT INTO t VALUES (2, 20, 7)");
    assert_eq!(
        query_all(&engine, &mut session, "SELECT COUNT(*) FROM t"),
        vec![vec!["1"]]
    );
    engine.segment_mgr().shutdown();
}

/// DROP COLUMN converges the same way: the resident batch (old width) is
/// flushed before the catalog drops the column, and post-ALTER inserts use
/// the narrowed layout.
#[test]
fn drop_column_converges_hot_state() {
    let (_temp_dir, engine, mut session) = create_engine();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)",
    );
    exec(&engine, &mut session, "INSERT INTO t VALUES (1, 10, 20)");
    // Resident (no flush): the stale-width memtable must not survive the DDL.
    exec(&engine, &mut session, "ALTER TABLE t DROP COLUMN a");

    let mut rows = query_all(&engine, &mut session, "SELECT id, b FROM t");
    rows.sort();
    assert_eq!(rows, vec![vec!["1", "20"]]);
    exec(&engine, &mut session, "INSERT INTO t VALUES (2, 21)");
    assert_eq!(
        query_all(&engine, &mut session, "SELECT COUNT(*) FROM t WHERE b = 21"),
        vec![vec!["1"]]
    );
    engine.segment_mgr().shutdown();
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
    exec(
        &engine,
        &mut session,
        "INSERT INTO t VALUES (1, 10, 20), (2, 11, 21)",
    );
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

/// DELETE's segment matching follows the default-fill semantics: a WHERE on
/// a column a segment predates must remove the rows carrying the DEFAULT.
/// UPDATE already matched them through aligned reads — the two used to
/// diverge, leaving rows UPDATE could change but DELETE could not see.
#[test]
fn delete_where_added_column_matches_default_rows_on_old_segments() {
    let (_temp_dir, engine, mut session) = create_engine();
    exec(
        &engine,
        &mut session,
        "CREATE TABLE t (id INT PRIMARY KEY, v INT)",
    );
    exec(
        &engine,
        &mut session,
        "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)",
    );
    exec(&engine, &mut session, "FLUSH TABLE t");
    exec(
        &engine,
        &mut session,
        "ALTER TABLE t ADD COLUMN score INT DEFAULT 7",
    );
    // A post-ALTER row with a non-default value keeps the statement honest.
    exec(&engine, &mut session, "INSERT INTO t VALUES (4, 40, 8)");

    exec(&engine, &mut session, "DELETE FROM t WHERE score = 7");
    let mut rows = query_all(&engine, &mut session, "SELECT id, v, score FROM t");
    rows.sort();
    assert_eq!(
        rows,
        vec![vec!["4", "40", "8"]],
        "old default rows deleted; the non-default row survives"
    );

    // The mirror shape: a non-matching comparison removes nothing.
    exec(&engine, &mut session, "INSERT INTO t VALUES (5, 50, 7)");
    exec(&engine, &mut session, "DELETE FROM t WHERE score = 99");
    assert_eq!(
        query_all(&engine, &mut session, "SELECT COUNT(*) FROM t"),
        vec![vec!["2"]]
    );
    engine.segment_mgr().shutdown();
}
