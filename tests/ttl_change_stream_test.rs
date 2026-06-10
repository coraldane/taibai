use tempfile::TempDir;

use std::io::Write;

use monax::net::bulk_importer::encode_columnar_bulk_insert;
use monax::net::session::Session;
use monax::protocol::types::DataType;
use monax::sql::execution::engine::{Engine, ExecResult};
use monax::storage::change_stream::{ChangeOperation, ChangePublishRequest};
use monax::types::batch::{ColumnDef, RecordBatch, Schema};
use monax::types::scalar::ScalarValue;

fn create_engine() -> (TempDir, Engine, Session) {
    let temp_dir = TempDir::new().expect("failed to create temp dir");
    let engine = Engine::new(temp_dir.path(), "main").expect("failed to create engine");
    let session = Session::new("root".to_string(), "main".to_string());
    (temp_dir, engine, session)
}

fn now_micros() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as i64
}

fn collect_i32(result: ExecResult) -> Vec<i32> {
    let ExecResult::Query { batches, .. } = result else {
        panic!("expected query result");
    };
    let mut values = Vec::new();
    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            match batch.scalar(row_idx, 0) {
                Some(ScalarValue::Int32(Some(v))) => values.push(v),
                other => panic!("expected int32, got {:?}", other),
            }
        }
    }
    values
}

fn count_value(result: ExecResult) -> i64 {
    let ExecResult::Query { batches, .. } = result else {
        panic!("expected query result");
    };
    match batches[0].scalar(0, 0) {
        Some(ScalarValue::Int64(Some(v))) => v,
        other => panic!("expected count, got {:?}", other),
    }
}

#[test]
fn ttl_filters_expired_rows_from_segments_and_count_star() {
    let (_temp_dir, engine, mut session) = create_engine();
    engine
        .execute(
            &mut session,
            "CREATE TABLE t (id INT32, created_at TIMESTAMP) TTL (created_at, 60)",
        )
        .expect("create table");

    let expired = now_micros() - 120_000_000;
    let fresh = now_micros();
    engine
        .execute(
            &mut session,
            &format!("INSERT INTO t VALUES (1, {}), (2, {})", expired, fresh),
        )
        .expect("insert rows");
    engine
        .execute(&mut session, "FLUSH TABLE t")
        .expect("flush rows");

    let ids = collect_i32(
        engine
            .execute(&mut session, "SELECT id FROM t")
            .expect("select ids"),
    );
    assert_eq!(ids, vec![2]);

    let count = count_value(
        engine
            .execute(&mut session, "SELECT COUNT(*) FROM t")
            .expect("count rows"),
    );
    assert_eq!(count, 1);
    engine.segment_mgr().shutdown();
}

#[test]
fn alter_table_can_set_and_drop_ttl() {
    let (_temp_dir, engine, mut session) = create_engine();
    engine
        .execute(&mut session, "CREATE TABLE t (id INT32, ts TIMESTAMP)")
        .expect("create table");
    engine
        .execute(&mut session, "ALTER TABLE t SET TTL (ts, 60)")
        .expect("set ttl");
    assert!(engine
        .get_table_meta("main", "t")
        .expect("table meta")
        .ttl
        .is_some());

    engine
        .execute(&mut session, "ALTER TABLE t DROP TTL")
        .expect("drop ttl");
    assert!(engine
        .get_table_meta("main", "t")
        .expect("table meta")
        .ttl
        .is_none());
    engine.segment_mgr().shutdown();
}

#[test]
fn change_stream_records_successful_dml_events() {
    let (_temp_dir, engine, mut session) = create_engine();
    engine
        .execute(&mut session, "CREATE TABLE t (id INT32, value INT32)")
        .expect("create table");
    let mut rx = engine.subscribe_changes();

    engine
        .execute(&mut session, "INSERT INTO t VALUES (1, 10)")
        .expect("insert");
    let event = rx.try_recv().expect("insert event");
    assert_eq!(event.operation, ChangeOperation::Insert);
    assert_eq!(event.database, "main");
    assert_eq!(event.table, "t");
    assert_eq!(event.rows_affected, 1);
    assert_eq!(event.rows.len(), 1);
    assert_eq!(
        event.rows[0].after.as_ref().unwrap().get("id"),
        Some(&ScalarValue::Int32(Some(1)))
    );

    engine
        .execute(&mut session, "UPDATE t SET value = 20 WHERE id = 1")
        .expect("update");
    engine
        .execute(&mut session, "DELETE FROM t WHERE id = 1")
        .expect("delete");

    let recent = engine
        .recent_changes(Some("main"), Some("t"))
        .into_iter()
        .filter(|event| {
            matches!(
                event.operation,
                ChangeOperation::Insert | ChangeOperation::Update | ChangeOperation::Delete
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(recent.len(), 3);
    assert_eq!(recent[0].operation, ChangeOperation::Insert);
    assert_eq!(recent[1].operation, ChangeOperation::Update);
    assert_eq!(recent[2].operation, ChangeOperation::Delete);
    assert_eq!(
        recent[1].rows[0].before.as_ref().unwrap().get("value"),
        Some(&ScalarValue::Int32(Some(10)))
    );
    assert_eq!(
        recent[1].rows[0].after.as_ref().unwrap().get("value"),
        Some(&ScalarValue::Int32(Some(20)))
    );
    assert_eq!(
        recent[2].rows[0].before.as_ref().unwrap().get("id"),
        Some(&ScalarValue::Int32(Some(1)))
    );
    engine.segment_mgr().shutdown();
}

#[test]
fn change_stream_publishes_transaction_insert_on_commit_only() {
    let (_temp_dir, engine, mut session) = create_engine();
    engine
        .execute(&mut session, "CREATE TABLE t (id INT32)")
        .expect("create table");
    let mut rx = engine.subscribe_changes();

    engine.execute(&mut session, "BEGIN").expect("begin");
    engine
        .execute(&mut session, "INSERT INTO t VALUES (1)")
        .expect("insert");
    assert!(rx.try_recv().is_err());

    engine.execute(&mut session, "COMMIT").expect("commit");
    let event = rx.try_recv().expect("commit event");
    assert_eq!(event.operation, ChangeOperation::Insert);
    assert_eq!(event.rows_affected, 1);
    engine.segment_mgr().shutdown();
}

#[test]
fn show_change_consumers_lists_acknowledged_offsets() {
    let (_temp_dir, engine, mut session) = create_engine();
    engine.ack_change_consumer("etl-b", 42).expect("ack etl-b");
    engine.ack_change_consumer("etl-a", 7).expect("ack etl-a");

    let ExecResult::Query { schema, batches } = engine
        .execute(&mut session, "SHOW CHANGE CONSUMERS")
        .expect("show change consumers")
    else {
        panic!("expected query result");
    };

    assert_eq!(
        schema.column_names().collect::<Vec<_>>(),
        vec!["consumer_group", "sequence"]
    );
    assert_eq!(batches[0].num_rows(), 2);
    assert_eq!(
        batches[0].scalar(0, 0),
        Some(ScalarValue::VarChar(Some("etl-a".to_string())))
    );
    assert_eq!(batches[0].scalar(0, 1), Some(ScalarValue::UInt64(Some(7))));
    assert_eq!(
        batches[0].scalar(1, 0),
        Some(ScalarValue::VarChar(Some("etl-b".to_string())))
    );
    assert_eq!(batches[0].scalar(1, 1), Some(ScalarValue::UInt64(Some(42))));
    engine.segment_mgr().shutdown();
}

#[test]
fn change_stream_management_sql_can_show_reset_and_drop_consumer_state() {
    let (_temp_dir, engine, mut session) = create_engine();
    engine
        .execute(&mut session, "CREATE TABLE t (id INT32)")
        .expect("create table");
    engine
        .execute(&mut session, "INSERT INTO t VALUES (1)")
        .expect("insert");
    engine
        .execute(&mut session, "RESET CHANGE CONSUMER 'etl-a' TO 1")
        .expect("reset consumer");

    let ExecResult::Query { batches, .. } = engine
        .execute(&mut session, "SHOW CHANGE CONSUMERS")
        .expect("show consumers")
    else {
        panic!("expected query result");
    };
    assert_eq!(
        batches[0].scalar(0, 0),
        Some(ScalarValue::VarChar(Some("etl-a".to_string())))
    );
    assert_eq!(batches[0].scalar(0, 1), Some(ScalarValue::UInt64(Some(1))));

    let ExecResult::Query { batches, .. } = engine
        .execute(&mut session, "SHOW CHANGE STATUS")
        .expect("show status")
    else {
        panic!("expected query result");
    };
    assert_eq!(batches[0].num_rows(), 1);
    assert_eq!(batches[0].scalar(0, 6), Some(ScalarValue::UInt64(Some(1))));

    let ExecResult::Query { batches, .. } = engine
        .execute(&mut session, "SHOW CHANGE EVENTS FROM 1 LIMIT 2")
        .expect("show events")
    else {
        panic!("expected query result");
    };
    assert_eq!(batches[0].num_rows(), 2);
    assert_eq!(
        batches[0].scalar(0, 4),
        Some(ScalarValue::VarChar(Some("CreateTable".to_string())))
    );

    engine
        .execute(&mut session, "CREATE TABLE other (id INT32)")
        .expect("create other");
    let ExecResult::Query { batches, .. } = engine
        .execute(&mut session, "SHOW CHANGE EVENTS FOR t FROM 1 LIMIT 100")
        .expect("show filtered events")
    else {
        panic!("expected query result");
    };
    for row_idx in 0..batches[0].num_rows() {
        assert_eq!(
            batches[0].scalar(row_idx, 3),
            Some(ScalarValue::VarChar(Some("t".to_string())))
        );
    }

    let ExecResult::Ok { rows_affected } = engine
        .execute(&mut session, "DROP CHANGE CONSUMER 'etl-a'")
        .expect("drop consumer")
    else {
        panic!("expected ok result");
    };
    assert_eq!(rows_affected, 1);
    assert_eq!(engine.change_consumer_sequence("etl-a"), 0);
    engine.segment_mgr().shutdown();
}

#[test]
fn change_consumer_ack_sql_participates_in_transactions() {
    let (_temp_dir, engine, mut session) = create_engine();

    engine.execute(&mut session, "BEGIN").expect("begin");
    let ExecResult::Ok { rows_affected } = engine
        .execute(&mut session, "ACK CHANGE CONSUMER 'etl-a' TO 10")
        .expect("ack in txn")
    else {
        panic!("expected ok result");
    };
    assert_eq!(rows_affected, 10);
    assert_eq!(engine.change_consumer_sequence("etl-a"), 0);
    engine.execute(&mut session, "COMMIT").expect("commit");
    assert_eq!(engine.change_consumer_sequence("etl-a"), 10);

    engine.execute(&mut session, "BEGIN").expect("begin again");
    engine
        .execute(&mut session, "ACK CHANGE CONSUMER 'etl-a' TO 20")
        .expect("ack before rollback");
    engine
        .execute(&mut session, "ROLLBACK")
        .expect("rollback ack");
    assert_eq!(engine.change_consumer_sequence("etl-a"), 10);
    engine.segment_mgr().shutdown();
}

#[test]
fn change_stream_drains_committed_outbox_on_restart() {
    let (temp_dir, engine, mut session) = create_engine();
    engine
        .execute(&mut session, "CREATE TABLE t (id INT32)")
        .expect("create table");
    let outbox_id = engine
        .change_stream()
        .prepare_outbox(vec![ChangePublishRequest {
            database: "main".to_string(),
            table: "t".to_string(),
            operation: ChangeOperation::Insert,
            rows_affected: 1,
            rows: Vec::new(),
            row_images_truncated: false,
        }])
        .expect("prepare outbox")
        .expect("outbox id");
    engine
        .change_stream()
        .commit_outbox(outbox_id)
        .expect("commit outbox");
    engine.segment_mgr().shutdown();

    let restarted = Engine::new(temp_dir.path(), "main").expect("restart engine");
    let events = restarted
        .change_events(None, None, Some("main"), Some("t"))
        .expect("load events");
    assert!(events
        .iter()
        .any(|event| { event.operation == ChangeOperation::Insert && event.rows_affected == 1 }));
    let status = restarted.change_stream_status().expect("status");
    assert_eq!(status.outbox_prepared, 0);
    assert_eq!(status.outbox_committed, 0);
    restarted.segment_mgr().shutdown();
}

#[test]
fn change_stream_records_table_management_events() {
    let (_temp_dir, engine, mut session) = create_engine();
    engine
        .execute(&mut session, "CREATE TABLE t (id INT32)")
        .expect("create table");
    engine
        .execute(&mut session, "TRUNCATE TABLE t")
        .expect("truncate table");
    engine
        .execute(&mut session, "ALTER TABLE t ADD COLUMN value INT32")
        .expect("alter table");
    engine
        .execute(&mut session, "DROP TABLE t")
        .expect("drop table");

    let operations = engine
        .recent_changes(Some("main"), Some("t"))
        .into_iter()
        .map(|event| event.operation)
        .collect::<Vec<_>>();
    assert_eq!(
        operations,
        vec![
            ChangeOperation::CreateTable,
            ChangeOperation::Truncate,
            ChangeOperation::AlterTable,
            ChangeOperation::DropTable
        ]
    );
    engine.segment_mgr().shutdown();
}

#[test]
fn change_stream_records_direct_bulk_insert_events() {
    let (_temp_dir, engine, mut session) = create_engine();
    engine
        .execute(&mut session, "CREATE TABLE t (id INT32)")
        .expect("create table");
    let mut batch = RecordBatch::new(Schema::new(vec![ColumnDef::new("id", DataType::Int32)]));
    batch
        .column_mut(0)
        .unwrap()
        .push(&ScalarValue::Int32(Some(1)))
        .expect("push id");
    let payload = encode_columnar_bulk_insert("t", &batch).expect("encode bulk payload");

    let rows = engine
        .exec_bulk_insert_payload_with_options("main", &payload, false, false)
        .expect("bulk insert");
    assert_eq!(rows, 1);

    let insert_events = engine
        .recent_changes(Some("main"), Some("t"))
        .into_iter()
        .filter(|event| event.operation == ChangeOperation::Insert)
        .collect::<Vec<_>>();
    assert_eq!(insert_events.len(), 1);
    assert_eq!(insert_events[0].rows_affected, 1);
    engine.segment_mgr().shutdown();
}

#[test]
fn change_stream_repairs_corrupt_log_tail_on_restart() {
    let (temp_dir, engine, mut session) = create_engine();
    engine
        .execute(&mut session, "CREATE TABLE t (id INT32 PRIMARY KEY)")
        .expect("create table");
    engine
        .execute(&mut session, "INSERT INTO t VALUES (1)")
        .expect("insert row");
    engine.segment_mgr().shutdown();

    let log_path = temp_dir.path().join("meta").join("change_stream.log");
    {
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&log_path)
            .expect("open change log");
        writeln!(file, "{{\"event\":{{\"sequence\":999}},\"checksum\":1}}")
            .expect("append corrupt record");
    }

    let repaired = Engine::new(temp_dir.path(), "main").expect("restart engine");
    let events = repaired
        .change_events(None, None, Some("main"), Some("t"))
        .expect("load repaired events");
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].operation, ChangeOperation::CreateTable);
    assert_eq!(events[1].operation, ChangeOperation::Insert);
    assert!(events.iter().all(|event| event.sequence != 999));
    repaired.segment_mgr().shutdown();
}
