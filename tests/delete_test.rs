use tempfile::TempDir;

use taibai::net::session::Session;
use taibai::sql::execution::engine::{Engine, ExecResult};
use taibai::storage::segment_manager::SegmentManager;

fn create_engine() -> (TempDir, Engine, Session) {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("failed to create engine");
    let session = Session::new("root".to_string(), "main".to_string());
    (temp_dir, engine, session)
}

fn row_count(result: ExecResult) -> usize {
    let ExecResult::Query { batches, .. } = result else {
        panic!("expected query result");
    };
    batches.iter().map(|batch| batch.num_rows()).sum()
}

fn count_star(result: ExecResult) -> i64 {
    let ExecResult::Query { batches, .. } = result else {
        panic!("expected query result");
    };
    batches
        .first()
        .and_then(|batch| batch.scalar(0, 0))
        .and_then(|value| match value {
            taibai::types::scalar::ScalarValue::Int64(Some(v)) => Some(v),
            _ => None,
        })
        .expect("count(*) value")
}

#[test]
fn delete_where_marks_matching_rows_only() {
    let (_temp_dir, engine, mut session) = create_engine();
    engine
        .execute(
            &mut session,
            "CREATE TABLE t (id INT32, value INT32, payload VARCHAR)",
        )
        .expect("create table");
    engine
        .execute(
            &mut session,
            "INSERT INTO t VALUES (1, 10, 'a'), (2, 20, 'b'), (3, 20, 'c')",
        )
        .expect("insert rows");
    engine
        .execute(&mut session, "FLUSH TABLE t")
        .expect("flush rows");

    let deleted = engine
        .execute(&mut session, "DELETE FROM t WHERE value = 20")
        .expect("delete rows");
    match deleted {
        ExecResult::Ok { rows_affected } => assert_eq!(rows_affected, 2),
        other => panic!("expected ok, got {:?}", other),
    }

    let remaining = engine
        .execute(&mut session, "SELECT id FROM t")
        .expect("select remaining rows");
    assert_eq!(row_count(remaining), 1);
    engine.segment_mgr().shutdown();
}

#[test]
fn partition_delete_flushes_only_the_target_partition() {
    let (temp_dir, engine, mut session) = create_engine();
    engine
        .execute(
            &mut session,
            "CREATE TABLE partitioned (id INT32, value INT32) PARTITION BY id",
        )
        .expect("create partitioned table");

    engine.begin_bulk_import();
    session.bulk_import_mode = true;
    engine
        .execute(&mut session, "INSERT INTO partitioned VALUES (1, 10)")
        .expect("insert target partition row");
    engine
        .execute(&mut session, "INSERT INTO partitioned VALUES (2, 20)")
        .expect("insert sibling partition row");

    let table_dir = temp_dir.path().join("main").join("partitioned");
    let active_segments =
        SegmentManager::discover_segments(&table_dir).expect("discover active segments");
    let active_names = active_segments
        .iter()
        .map(|path| path.file_name().unwrap().to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    let sibling_path = active_segments
        .into_iter()
        .find(|path| {
            path.file_name()
                .is_some_and(|name| name.to_string_lossy().starts_with("p2_"))
        })
        .unwrap_or_else(|| panic!("sibling partition segment, found {active_names:?}"));
    let sibling_len_before = std::fs::metadata(&sibling_path).unwrap().len();

    let deleted = engine
        .execute(&mut session, "DELETE FROM partitioned WHERE id = 1")
        .expect("delete one partition");
    match deleted {
        ExecResult::Ok { rows_affected } => assert_eq!(rows_affected, 1),
        other => panic!("expected ok, got {:?}", other),
    }

    let mut active_after = engine
        .segment_mgr()
        .active_partition_keys("main", "partitioned");
    active_after.sort();
    assert_eq!(active_after, vec!["1".to_string(), "2".to_string()]);
    assert_eq!(
        std::fs::metadata(&sibling_path).unwrap().len(),
        sibling_len_before
    );
    assert_eq!(
        count_star(
            engine
                .execute(&mut session, "SELECT COUNT(*) FROM partitioned")
                .expect("count remaining partition rows")
        ),
        1
    );

    session.bulk_import_mode = false;
    engine.end_bulk_import();
    engine.segment_mgr().shutdown();
}

#[test]
fn delete_without_where_marks_rows_without_reading_predicate_columns() {
    let (_temp_dir, engine, mut session) = create_engine();
    engine
        .execute(&mut session, "CREATE TABLE t (id INT32, value INT32)")
        .expect("create table");
    engine
        .execute(&mut session, "INSERT INTO t VALUES (1, 10), (2, 20)")
        .expect("insert rows");
    engine
        .execute(&mut session, "FLUSH TABLE t")
        .expect("flush rows");

    let deleted = engine
        .execute(&mut session, "DELETE FROM t")
        .expect("delete all rows");
    match deleted {
        ExecResult::Ok { rows_affected } => assert_eq!(rows_affected, 2),
        other => panic!("expected ok, got {:?}", other),
    }

    let remaining = engine
        .execute(&mut session, "SELECT id FROM t")
        .expect("select remaining rows");
    assert_eq!(row_count(remaining), 0);
    engine.segment_mgr().shutdown();
}

#[test]
fn delete_with_not_in_literal_list_keeps_non_matching_rows() {
    let (_temp_dir, engine, mut session) = create_engine();
    engine
        .execute(
            &mut session,
            "CREATE TABLE t_sync_task_monitor (
                id INT64,
                deal_date INT32,
                table_name VARCHAR
            ) PARTITION BY YEAR(deal_date) CLUSTER BY (deal_date, table_name)",
        )
        .expect("create table");
    engine
        .execute(
            &mut session,
            "INSERT INTO t_sync_task_monitor VALUES
                (1, 20260609, 'a'),
                (2, 20260609, 'a'),
                (3, 20260609, 'b'),
                (4, 20260609, 'b'),
                (5, 20260610, 'a')",
        )
        .expect("insert rows");
    engine
        .execute(&mut session, "FLUSH TABLE t_sync_task_monitor")
        .expect("flush rows");

    let deleted = engine
        .execute(
            &mut session,
            "DELETE FROM t_sync_task_monitor
             WHERE deal_date = 20260609
               AND id NOT IN (2, 4)",
        )
        .expect("delete rows");
    match deleted {
        ExecResult::Ok { rows_affected } => assert_eq!(rows_affected, 2),
        other => panic!("expected ok, got {:?}", other),
    }

    let count = engine
        .execute(
            &mut session,
            "SELECT COUNT(*) FROM t_sync_task_monitor WHERE deal_date = 20260609",
        )
        .expect("count remaining rows");
    assert_eq!(count_star(count), 2);

    let remaining = engine
        .execute(
            &mut session,
            "SELECT * FROM t_sync_task_monitor WHERE deal_date = 20260609",
        )
        .expect("select remaining rows");
    assert_eq!(row_count(remaining), 2);
    engine.segment_mgr().shutdown();
}

#[test]
fn delete_with_not_in_subquery_keeps_group_max_rows() {
    let (_temp_dir, engine, mut session) = create_engine();
    engine
        .execute(
            &mut session,
            "CREATE TABLE t_sync_task_monitor (
                id INT64,
                deal_date INT32,
                table_name VARCHAR
            ) PARTITION BY YEAR(deal_date) CLUSTER BY (deal_date, table_name)",
        )
        .expect("create table");
    engine
        .execute(
            &mut session,
            "INSERT INTO t_sync_task_monitor VALUES
                (1, 20260609, 'a'),
                (2, 20260609, 'a'),
                (3, 20260609, 'b'),
                (4, 20260609, 'b'),
                (5, 20260610, 'a')",
        )
        .expect("insert rows");
    engine
        .execute(&mut session, "FLUSH TABLE t_sync_task_monitor")
        .expect("flush rows");

    let deleted = engine
        .execute(
            &mut session,
            "DELETE FROM t_sync_task_monitor
             WHERE deal_date = 20260609
               AND id NOT IN (
                   SELECT MAX(id)
                   FROM t_sync_task_monitor
                   WHERE deal_date = 20260609
                   GROUP BY deal_date, table_name
               )",
        )
        .expect("delete rows");
    match deleted {
        ExecResult::Ok { rows_affected } => assert_eq!(rows_affected, 2),
        other => panic!("expected ok, got {:?}", other),
    }

    let count = engine
        .execute(
            &mut session,
            "SELECT COUNT(*) FROM t_sync_task_monitor WHERE deal_date = 20260609",
        )
        .expect("count remaining rows");
    assert_eq!(count_star(count), 2);

    let remaining = engine
        .execute(
            &mut session,
            "SELECT * FROM t_sync_task_monitor WHERE deal_date = 20260609",
        )
        .expect("select remaining rows");
    assert_eq!(row_count(remaining), 2);
    engine.segment_mgr().shutdown();
}

#[test]
fn delete_with_not_in_derived_subquery_keeps_group_max_rows() {
    let (_temp_dir, engine, mut session) = create_engine();
    engine
        .execute(
            &mut session,
            "CREATE TABLE t_sync_task_monitor (
                id INT64,
                deal_date INT32,
                table_name VARCHAR
            ) PARTITION BY YEAR(deal_date) CLUSTER BY (deal_date, table_name)",
        )
        .expect("create table");
    engine
        .execute(
            &mut session,
            "INSERT INTO t_sync_task_monitor VALUES
                (1, 20260609, 'a'),
                (2, 20260609, 'a'),
                (3, 20260609, 'b'),
                (4, 20260609, 'b'),
                (5, 20260610, 'a')",
        )
        .expect("insert rows");
    engine
        .execute(&mut session, "FLUSH TABLE t_sync_task_monitor")
        .expect("flush rows");

    let deleted = engine
        .execute(
            &mut session,
            "DELETE FROM t_sync_task_monitor
             WHERE deal_date = 20260609
               AND id NOT IN (
                   SELECT max_id
                   FROM (
                       SELECT MAX(id) AS max_id
                       FROM t_sync_task_monitor
                       WHERE deal_date = 20260609
                       GROUP BY deal_date, table_name
                   ) t
               )",
        )
        .expect("delete rows");
    match deleted {
        ExecResult::Ok { rows_affected } => assert_eq!(rows_affected, 2),
        other => panic!("expected ok, got {:?}", other),
    }

    let count = engine
        .execute(
            &mut session,
            "SELECT COUNT(*) FROM t_sync_task_monitor WHERE deal_date = 20260609",
        )
        .expect("count remaining rows");
    assert_eq!(count_star(count), 2);

    let remaining = engine
        .execute(
            &mut session,
            "SELECT * FROM t_sync_task_monitor WHERE deal_date = 20260609",
        )
        .expect("select remaining rows");
    assert_eq!(row_count(remaining), 2);
    engine.segment_mgr().shutdown();
}

#[test]
fn delete_then_insert_same_deal_date_keeps_new_rows_visible() {
    let (_temp_dir, engine, mut session) = create_engine();
    engine
        .execute(
            &mut session,
            "CREATE TABLE t_sync_task_monitor (
                id INT64,
                deal_date INT32,
                table_name VARCHAR
            ) PARTITION BY YEAR(deal_date) CLUSTER BY (deal_date, table_name)",
        )
        .expect("create table");
    engine
        .execute(
            &mut session,
            "INSERT INTO t_sync_task_monitor VALUES
                (1, 20260609, 'old_a'),
                (2, 20260609, 'old_b'),
                (3, 20260610, 'other')",
        )
        .expect("insert original rows");
    engine
        .execute(&mut session, "FLUSH TABLE t_sync_task_monitor")
        .expect("flush original rows");

    let deleted = engine
        .execute(
            &mut session,
            "DELETE FROM t_sync_task_monitor WHERE deal_date = 20260609",
        )
        .expect("delete old deal_date rows");
    match deleted {
        ExecResult::Ok { rows_affected } => assert_eq!(rows_affected, 2),
        other => panic!("expected ok, got {:?}", other),
    }

    engine
        .execute(
            &mut session,
            "INSERT INTO t_sync_task_monitor VALUES
                (4, 20260609, 'new_a'),
                (5, 20260609, 'new_b')",
        )
        .expect("insert replacement rows");
    engine
        .execute(&mut session, "FLUSH TABLE t_sync_task_monitor")
        .expect("flush replacement rows");

    let count = engine
        .execute(
            &mut session,
            "SELECT COUNT(*) FROM t_sync_task_monitor WHERE deal_date = 20260609",
        )
        .expect("count replacement rows");
    assert_eq!(count_star(count), 2);

    let remaining = engine
        .execute(
            &mut session,
            "SELECT * FROM t_sync_task_monitor WHERE deal_date = 20260609",
        )
        .expect("select replacement rows");
    assert_eq!(row_count(remaining), 2);
    engine.segment_mgr().shutdown();
}

#[test]
fn bulk_import_insert_is_visible_before_stop_bulk_import() {
    use taibai::net::bulk_importer::encode_columnar_bulk_insert;
    use taibai::protocol::types::DataType;
    use taibai::types::batch::{ColumnDef, RecordBatch, Schema};
    use taibai::types::scalar::ScalarValue;

    let (_temp_dir, engine, mut session) = create_engine();
    engine
        .execute(
            &mut session,
            "CREATE TABLE t_stock_daily (
                security_id VARCHAR,
                deal_date INT32,
                close_px DECIMAL(18, 4)
            ) PARTITION BY YEAR(deal_date) CLUSTER BY (security_id, deal_date)",
        )
        .expect("create table");

    engine.begin_bulk_import();
    let schema = Schema::new(vec![
        ColumnDef::new("security_id", DataType::VarChar),
        ColumnDef::new("deal_date", DataType::Int32),
        ColumnDef::new("close_px", DataType::Decimal)
            .with_precision(18)
            .with_scale(4),
    ]);
    let mut batch = RecordBatch::new(schema);
    batch
        .column_mut(0)
        .unwrap()
        .push(&ScalarValue::VarChar(Some("000001.SZ".to_string())))
        .unwrap();
    batch
        .column_mut(1)
        .unwrap()
        .push(&ScalarValue::Int32(Some(20260629)))
        .unwrap();
    batch
        .column_mut(2)
        .unwrap()
        .push(&ScalarValue::Decimal(Some(1234000)))
        .unwrap();
    batch
        .column_mut(0)
        .unwrap()
        .push(&ScalarValue::VarChar(Some("000002.SZ".to_string())))
        .unwrap();
    batch
        .column_mut(1)
        .unwrap()
        .push(&ScalarValue::Int32(Some(20260629)))
        .unwrap();
    batch
        .column_mut(2)
        .unwrap()
        .push(&ScalarValue::Decimal(Some(2345000)))
        .unwrap();

    let payload = encode_columnar_bulk_insert("t_stock_daily", &batch).expect("encode payload");
    let inserted = engine
        .exec_bulk_insert_payload_with_options("main", &payload, true)
        .expect("bulk insert payload");
    assert_eq!(inserted, 2);

    let count = engine
        .execute(
            &mut session,
            "SELECT COUNT(*) FROM t_stock_daily WHERE deal_date = 20260629",
        )
        .expect("count inserted rows before stop bulk import");
    assert_eq!(count_star(count), 2);

    engine.end_bulk_import();
    engine.segment_mgr().shutdown();
}

#[test]
fn delete_with_as_alias_filters_by_qualified_column() {
    let (_temp_dir, engine, mut session) = create_engine();
    engine
        .execute(
            &mut session,
            "CREATE TABLE t (id INT32, value INT32) PARTITION BY id",
        )
        .expect("create table");
    engine
        .execute(
            &mut session,
            "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)",
        )
        .expect("insert rows");
    engine
        .execute(&mut session, "FLUSH TABLE t")
        .expect("flush rows");

    let deleted = engine
        .execute(&mut session, "DELETE FROM t AS a WHERE a.value >= 20")
        .expect("delete rows with AS alias");
    match deleted {
        ExecResult::Ok { rows_affected } => assert_eq!(rows_affected, 2),
        other => panic!("expected ok, got {:?}", other),
    }

    let remaining = engine
        .execute(&mut session, "SELECT id FROM t ORDER BY id")
        .expect("select remaining rows");
    assert_eq!(row_count(remaining), 1);
    engine.segment_mgr().shutdown();
}

#[test]
fn delete_with_implicit_alias_filters_matching_rows() {
    let (_temp_dir, engine, mut session) = create_engine();
    engine
        .execute(&mut session, "CREATE TABLE t (id INT32, value INT32)")
        .expect("create table");
    engine
        .execute(&mut session, "INSERT INTO t VALUES (1, 10), (2, 20)")
        .expect("insert rows");
    engine
        .execute(&mut session, "FLUSH TABLE t")
        .expect("flush rows");

    let deleted = engine
        .execute(&mut session, "DELETE FROM t tgt WHERE tgt.id = 2")
        .expect("delete rows with implicit alias");
    match deleted {
        ExecResult::Ok { rows_affected } => assert_eq!(rows_affected, 1),
        other => panic!("expected ok, got {:?}", other),
    }

    let remaining = engine
        .execute(&mut session, "SELECT id FROM t")
        .expect("select remaining rows");
    assert_eq!(row_count(remaining), 1);
    engine.segment_mgr().shutdown();
}

#[test]
fn delete_with_alias_without_where_removes_all_rows() {
    let (_temp_dir, engine, mut session) = create_engine();
    engine
        .execute(&mut session, "CREATE TABLE t (id INT32, value INT32)")
        .expect("create table");
    engine
        .execute(&mut session, "INSERT INTO t VALUES (1, 10), (2, 20)")
        .expect("insert rows");
    engine
        .execute(&mut session, "FLUSH TABLE t")
        .expect("flush rows");

    let deleted = engine
        .execute(&mut session, "DELETE FROM t AS a")
        .expect("delete all rows through alias");
    match deleted {
        ExecResult::Ok { rows_affected } => assert_eq!(rows_affected, 2),
        other => panic!("expected ok, got {:?}", other),
    }

    let remaining = engine
        .execute(&mut session, "SELECT COUNT(*) FROM t")
        .expect("count remaining rows");
    assert_eq!(count_star(remaining), 0);
    engine.segment_mgr().shutdown();
}

#[test]
fn delete_with_alias_still_prunes_the_target_partition() {
    let (_temp_dir, engine, mut session) = create_engine();
    engine
        .execute(
            &mut session,
            "CREATE TABLE partitioned (id INT32, value INT32) PARTITION BY id",
        )
        .expect("create partitioned table");
    engine
        .execute(
            &mut session,
            "INSERT INTO partitioned VALUES (1, 10), (2, 20)",
        )
        .expect("insert rows");

    let deleted = engine
        .execute(&mut session, "DELETE FROM partitioned AS p WHERE p.id = 1")
        .expect("delete one partition through alias");
    match deleted {
        ExecResult::Ok { rows_affected } => assert_eq!(rows_affected, 1),
        other => panic!("expected ok, got {:?}", other),
    }

    let remaining = engine
        .execute(&mut session, "SELECT COUNT(*) FROM partitioned")
        .expect("count remaining rows");
    assert_eq!(count_star(remaining), 1);
    engine.segment_mgr().shutdown();
}

/// The hashed IN fast path (column operand over >4 literal candidates, the
/// shape `WHERE id IN (SELECT ...)` produces after subquery resolution) must
/// keep the generic loop's exact semantics: strict matching, NULL column
/// values excluded, NOT IN over a NULL candidate matches nothing, and
/// Int32-vs-Int64 numeric equality still matches.
#[test]
fn delete_in_large_literal_list_semantics() {
    let (_temp_dir, engine, mut session) = create_engine();
    engine
        .execute(&mut session, "CREATE TABLE in_t (id INT32, tag VARCHAR)")
        .expect("create table");
    for i in 0..40 {
        let tag = if i % 2 == 0 { "even" } else { "odd" };
        engine
            .execute(
                &mut session,
                &format!("INSERT INTO in_t VALUES ({i}, '{tag}')"),
            )
            .expect("seed");
    }

    // >4 literal candidates via a subquery (resolver expands to literals).
    engine
        .execute(&mut session, "DELETE FROM in_t WHERE id IN (SELECT id FROM in_t WHERE id < 5)")
        .expect("delete via large IN");
    let count = |engine: &Engine, session: &mut Session, sql: &str| -> i64 {
        match engine.execute(session, sql).expect("count") {
            ExecResult::Query { batches, .. } => batches
                .iter()
                .map(|b| b.scalar(0, 0).unwrap())
                .map(|v| match v {
                    taibai::types::scalar::ScalarValue::Int64(Some(n)) => n,
                    other => panic!("unexpected count {other:?}"),
                })
                .sum(),
            other => panic!("expected query, got {other:?}"),
        }
    };
    assert_eq!(count(&engine, &mut session, "SELECT COUNT(*) FROM in_t"), 35);

    // NOT IN over a list containing NULL: matches nothing.
    engine
        .execute(&mut session, "DELETE FROM in_t WHERE id NOT IN (1, 2, 3, 4, 5, NULL)")
        .expect("not-in null delete");
    assert_eq!(count(&engine, &mut session, "SELECT COUNT(*) FROM in_t"), 35);

    // IN over a list containing NULL still matches the non-null hits.
    engine
        .execute(&mut session, "DELETE FROM in_t WHERE id IN (5, 6, 7, 8, 9, NULL)")
        .expect("in null delete");
    assert_eq!(count(&engine, &mut session, "SELECT COUNT(*) FROM in_t"), 30);

    // Cross-width numeric equality: Int32 column vs Int64 literals.
    engine
        .execute(
            &mut session,
            "DELETE FROM in_t WHERE id IN (10, 11, 12, 13, 14, 15, 16)",
        )
        .expect("cross-width delete");
    assert_eq!(count(&engine, &mut session, "SELECT COUNT(*) FROM in_t"), 23);
}
