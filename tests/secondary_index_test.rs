use tempfile::TempDir;

use taibai::net::session::Session;
use taibai::sql::execution::engine::{Engine, ExecResult};
use taibai::types::batch::{RecordBatch, Schema};
use taibai::types::scalar::ScalarValue;

fn create_engine() -> (TempDir, Engine, Session) {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("failed to create engine");
    let session = Session::new("root".to_string(), "main".to_string());
    (temp_dir, engine, session)
}

fn collect_rows(result: ExecResult) -> Vec<Vec<ScalarValue>> {
    let ExecResult::Query { batches, .. } = result else {
        panic!("expected query result");
    };
    let mut rows = Vec::new();
    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            rows.push(
                (0..batch.num_columns())
                    .map(|col_idx| batch.scalar(row_idx, col_idx).expect("scalar"))
                    .collect(),
            );
        }
    }
    rows
}

fn explain_text(result: ExecResult) -> String {
    collect_rows(result)
        .into_iter()
        .filter_map(|row| match row.into_iter().next() {
            Some(ScalarValue::VarChar(Some(line))) => Some(line),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn show_create_sql(result: ExecResult) -> String {
    let rows = collect_rows(result);
    match rows.first().and_then(|row| row.get(1)) {
        Some(ScalarValue::VarChar(Some(sql))) => sql.clone(),
        other => panic!("expected SHOW CREATE TABLE sql, got {:?}", other),
    }
}

fn seed_two_row_group_segment(engine: &Engine, db: &str, table: &str) {
    let table_meta = engine
        .get_table_meta(db, table)
        .expect("table metadata should exist");
    let schema = Schema::new(table_meta.columns.clone());
    let mut batch = RecordBatch::with_capacity(schema.clone(), 8300);
    for id in 0..8192 {
        let symbol = if id % 2 == 0 { "a" } else { "z" };
        batch
            .column_mut(0)
            .unwrap()
            .push(&ScalarValue::Int32(Some(id)))
            .expect("push id");
        batch
            .column_mut(1)
            .unwrap()
            .push(&ScalarValue::VarChar(Some(symbol.to_string())))
            .expect("push symbol");
    }
    for id in 8192..8300 {
        batch
            .column_mut(0)
            .unwrap()
            .push(&ScalarValue::Int32(Some(id)))
            .expect("push id");
        batch
            .column_mut(1)
            .unwrap()
            .push(&ScalarValue::VarChar(Some("m".to_string())))
            .expect("push symbol");
    }

    let segment_path = engine.data_dir().join(db).join(table).join("seg_00001.dat");
    engine
        .segment_mgr()
        .write_new_segment(&segment_path, schema, 8192, &batch, 3)
        .expect("write segment");
}

#[test]
fn create_show_and_drop_secondary_bloom_index() {
    let (_temp_dir, engine, mut session) = create_engine();

    engine
        .execute(
            &mut session,
            "CREATE TABLE ticks (id INT32, symbol VARCHAR)",
        )
        .expect("create table");
    engine
        .execute(
            &mut session,
            "CREATE INDEX idx_symbol ON ticks(symbol) TYPE bloom",
        )
        .expect("create secondary index");
    engine
        .execute(
            &mut session,
            "CREATE INDEX IF NOT EXISTS idx_symbol ON ticks(symbol) TYPE bloom",
        )
        .expect("idempotent create secondary index");

    let rows = collect_rows(
        engine
            .execute(&mut session, "SHOW INDEXES FROM ticks")
            .expect("show indexes"),
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0],
        vec![
            ScalarValue::VarChar(Some("ticks".to_string())),
            ScalarValue::Int64(Some(1)),
            ScalarValue::VarChar(Some("idx_symbol".to_string())),
            ScalarValue::Int64(Some(1)),
            ScalarValue::VarChar(Some("symbol".to_string())),
            ScalarValue::VarChar(Some("BLOOM".to_string())),
        ]
    );

    let create_text = show_create_sql(
        engine
            .execute(&mut session, "SHOW CREATE TABLE ticks")
            .expect("show create table"),
    );
    assert!(create_text.contains("CREATE INDEX idx_symbol ON ticks (symbol) TYPE bloom"));

    engine
        .execute(&mut session, "DROP INDEX idx_symbol ON ticks")
        .expect("drop secondary index");
    let rows = collect_rows(
        engine
            .execute(&mut session, "SHOW INDEXES FROM ticks")
            .expect("show indexes after drop"),
    );
    assert!(rows.is_empty());

    engine.segment_mgr().shutdown();
}

#[test]
fn minmax_index_is_recorded_and_explain_uses_catalog_indexes_only() {
    let (_temp_dir, engine, mut session) = create_engine();

    engine
        .execute(&mut session, "CREATE TABLE ticks (id INT32, price INT32)")
        .expect("create table");

    let no_index_plan = explain_text(
        engine
            .execute(
                &mut session,
                "EXPLAIN SELECT id FROM ticks WHERE price > 10",
            )
            .expect("explain without index"),
    );
    assert!(
        !no_index_plan.contains("index_columns"),
        "unexpected index columns without catalog index:\n{}",
        no_index_plan
    );

    engine
        .execute(
            &mut session,
            "CREATE INDEX idx_price ON ticks(price) TYPE minmax",
        )
        .expect("create minmax index");

    let rows = collect_rows(
        engine
            .execute(&mut session, "SHOW INDEXES FROM ticks")
            .expect("show indexes"),
    );
    assert_eq!(
        rows[0],
        vec![
            ScalarValue::VarChar(Some("ticks".to_string())),
            ScalarValue::Int64(Some(1)),
            ScalarValue::VarChar(Some("idx_price".to_string())),
            ScalarValue::Int64(Some(1)),
            ScalarValue::VarChar(Some("price".to_string())),
            ScalarValue::VarChar(Some("MINMAX".to_string())),
        ]
    );

    let plan = explain_text(
        engine
            .execute(
                &mut session,
                "EXPLAIN SELECT id FROM ticks WHERE price > 10",
            )
            .expect("explain with minmax index"),
    );
    assert!(
        plan.contains("index_columns: [\"price\"]"),
        "missing real index columns:\n{}",
        plan
    );
    assert!(
        plan.contains("minmax_index_columns: [\"price\"]"),
        "missing minmax index columns:\n{}",
        plan
    );

    let duplicate = engine.execute(&mut session, "CREATE INDEX idx_price ON ticks(price)");
    assert!(
        duplicate.is_err(),
        "duplicate index should fail without IF NOT EXISTS"
    );

    engine.segment_mgr().shutdown();
}

#[test]
fn set_index_prunes_row_groups_for_equality_predicate() {
    let (_temp_dir, engine, mut session) = create_engine();

    engine
        .execute(
            &mut session,
            "CREATE TABLE ticks (id INT32, symbol VARCHAR)",
        )
        .expect("create table");
    seed_two_row_group_segment(&engine, "main", "ticks");

    engine
        .execute(
            &mut session,
            "CREATE INDEX idx_symbol_set ON ticks(symbol) TYPE set",
        )
        .expect("create set index");

    let rows = collect_rows(
        engine
            .execute(&mut session, "SHOW INDEXES FROM ticks")
            .expect("show indexes"),
    );
    assert_eq!(
        rows[0],
        vec![
            ScalarValue::VarChar(Some("ticks".to_string())),
            ScalarValue::Int64(Some(1)),
            ScalarValue::VarChar(Some("idx_symbol_set".to_string())),
            ScalarValue::Int64(Some(1)),
            ScalarValue::VarChar(Some("symbol".to_string())),
            ScalarValue::VarChar(Some("SET".to_string())),
        ]
    );

    let plan = explain_text(
        engine
            .execute(
                &mut session,
                "EXPLAIN ANALYZE SELECT id FROM ticks WHERE symbol = 'm'",
            )
            .expect("explain analyze"),
    );

    assert!(
        plan.contains("set_index_columns: [\"symbol\"]"),
        "missing set index in explain output:\n{}",
        plan
    );
    assert!(
        plan.contains("row_groups    : 2 total, 1 read, 1 pruned"),
        "set index did not prune expected row group:\n{}",
        plan
    );

    let rows = collect_rows(
        engine
            .execute(&mut session, "SELECT id FROM ticks WHERE symbol = 'm'")
            .expect("select rows"),
    );
    assert_eq!(rows.len(), 108);

    engine.segment_mgr().shutdown();
}

#[test]
fn secondary_bloom_index_prunes_row_groups_for_equality_predicate() {
    let (_temp_dir, engine, mut session) = create_engine();

    engine
        .execute(
            &mut session,
            "CREATE TABLE ticks (id INT32, symbol VARCHAR)",
        )
        .expect("create table");

    seed_two_row_group_segment(&engine, "main", "ticks");

    engine
        .execute(&mut session, "CREATE INDEX idx_symbol ON ticks(symbol)")
        .expect("create secondary index");

    let plan = explain_text(
        engine
            .execute(
                &mut session,
                "EXPLAIN ANALYZE SELECT id FROM ticks WHERE symbol = 'm'",
            )
            .expect("explain analyze"),
    );

    assert!(
        plan.contains("row_groups    : 2 total, 1 read, 1 pruned"),
        "unexpected explain output:\n{}",
        plan
    );

    let rows = collect_rows(
        engine
            .execute(&mut session, "SELECT id FROM ticks WHERE symbol = 'm'")
            .expect("count rows"),
    );
    assert_eq!(rows.len(), 108);

    engine.segment_mgr().shutdown();
}
