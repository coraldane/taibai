use super::*;
use crate::storage::segment_manager::SegmentManager;
use std::sync::atomic::AtomicBool;

#[test]
fn heartbeat_query_classifier_only_matches_select_one() {
    for sql in [
        "SELECT 1",
        "select 1",
        "SELECT\t1",
        "SELECT 1;",
        "select  1 ;",
    ] {
        assert!(is_heartbeat_query(sql), "{sql}");
    }

    for sql in [
        "SELECT 2",
        "SELECT 1 FROM metrics",
        "SELECT 1, 2",
        "SELECT 1; SELECT 2",
        "EXPLAIN SELECT 1",
    ] {
        assert!(!is_heartbeat_query(sql), "{sql}");
    }
}

#[test]
fn query_log_classifier_only_tracks_result_set_queries() {
    // The classifier short-circuits when the query log is disabled; enable a
    // throwaway log so the classification logic is exercised.
    let log_dir = tempfile::TempDir::new().expect("temp dir");
    crate::common::query_log::init(&log_dir.path().join("queries.log"), 16)
        .expect("init query log");

    for sql in [
        "SELECT * FROM metrics",
        "select * from metrics",
        "SHOW TABLES",
        "DESCRIBE TABLE metrics",
        "EXPLAIN SELECT * FROM metrics",
        "SELECT * FROM",
    ] {
        assert!(should_log_query_sql(sql), "{sql}");
    }

    for sql in [
        "CREATE TABLE metrics (id INT64)",
        "INSERT INTO metrics VALUES (1)",
        "insert into metrics values (1)",
        "DELETE FROM metrics WHERE id = 1",
        "UPDATE metrics SET id = 2",
        "BEGIN",
        "COMMIT",
        "ROLLBACK",
        "START BULK IMPORT",
        "STOP BULK IMPORT",
    ] {
        assert!(!should_log_query_sql(sql), "{sql}");
    }
}

#[test]
fn test_header_round_trip() {
    let h = MessageHeader {
        msg_type: MSG_TYPE_PING,
        payload_len: 0,
    };
    let encoded = h.encode();
    let decoded = MessageHeader::decode(&encoded).unwrap();
    assert_eq!(decoded.msg_type, MSG_TYPE_PING);
    assert_eq!(decoded.payload_len, 0);
}

#[test]
fn test_header_bad_magic() {
    let bad = [0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00];
    assert!(MessageHeader::decode(&bad).is_err());
}

#[test]
fn test_build_pong() {
    let msg = build_pong();
    assert_eq!(msg.msg_type, MSG_TYPE_PONG);
    assert!(msg.payload.is_empty());
}

#[test]
fn test_build_ok() {
    let msg = build_ok(42);
    assert_eq!(msg.msg_type, MSG_TYPE_OK);
    assert_eq!(msg.payload.len(), 8);
    let n = u64::from_le_bytes(msg.payload[..].try_into().unwrap());
    assert_eq!(n, 42);
}

#[test]
fn test_build_error() {
    let msg = build_error("test error");
    assert_eq!(msg.msg_type, MSG_TYPE_ERROR);
    assert_eq!(&msg.payload[..], b"test error");
}

#[test]
fn streaming_query_stops_when_chunk_receiver_is_dropped() {
    let dir = tempfile::TempDir::new().unwrap();
    let engine = Arc::new(Engine::new(dir.path(), "main").unwrap());
    let config = Arc::new(ServerConfig::default());
    let mut handler = logged_in_handler(config, Arc::clone(&engine), "root");
    handler.session.as_mut().unwrap().streaming_results = true;
    engine
        .execute(
            handler.session.as_mut().unwrap(),
            "CREATE TABLE dropped_stream (id INT64)",
        )
        .unwrap();
    engine
        .execute(
            handler.session.as_mut().unwrap(),
            "INSERT INTO dropped_stream VALUES (1), (2)",
        )
        .unwrap();

    let (sender, receiver) = tokio::sync::mpsc::channel(1);
    drop(receiver);
    let mut payload = Vec::new();
    write_len_prefixed_string(&mut payload, "");
    write_len_prefixed_string(&mut payload, "SELECT id FROM dropped_stream");
    let responses = handler
        .handle_message_cancellable(
            Message::new(MSG_TYPE_QUERY, Bytes::from(payload)),
            Arc::new(crate::common::query_cancel::QueryContext::new(Arc::new(
                AtomicBool::new(false),
            ))),
            Some(sender),
        )
        .unwrap();

    assert_eq!(responses.len(), 1);
    assert_eq!(responses[0].msg_type, MSG_TYPE_ERROR);
    assert!(String::from_utf8_lossy(&responses[0].payload).contains("disconnected"));
}

#[test]
fn streaming_query_emits_chunks_and_end_marker() {
    let dir = tempfile::TempDir::new().unwrap();
    let engine = Arc::new(Engine::new(dir.path(), "main").unwrap());
    let config = Arc::new(ServerConfig::default());
    let mut handler = logged_in_handler(config, Arc::clone(&engine), "root");
    handler.session.as_mut().unwrap().streaming_results = true;
    engine
        .execute(
            handler.session.as_mut().unwrap(),
            "CREATE TABLE stream_rows (id INT64)",
        )
        .unwrap();
    engine
        .execute(
            handler.session.as_mut().unwrap(),
            "INSERT INTO stream_rows VALUES (1), (2), (3)",
        )
        .unwrap();

    let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
    let consumer = std::thread::spawn(move || {
        let mut chunks = Vec::new();
        while let Some(message) = receiver.blocking_recv() {
            chunks.push(message);
        }
        chunks
    });
    let mut payload = Vec::new();
    write_len_prefixed_string(&mut payload, "");
    write_len_prefixed_string(&mut payload, "SELECT id FROM stream_rows");
    let responses = handler
        .handle_message_cancellable(
            Message::new(MSG_TYPE_QUERY, Bytes::from(payload)),
            Arc::new(crate::common::query_cancel::QueryContext::new(Arc::new(
                AtomicBool::new(false),
            ))),
            Some(sender),
        )
        .unwrap();
    drop(handler);
    let chunks = consumer.join().unwrap();

    assert!(!chunks.is_empty());
    assert!(chunks
        .iter()
        .all(|message| message.msg_type == MSG_TYPE_RESULT_CHUNK));
    assert_eq!(responses.len(), 1);
    assert_eq!(responses[0].msg_type, MSG_TYPE_RESULT_END);
}

#[test]
fn streaming_query_sends_schema_chunk_for_empty_result() {
    let dir = tempfile::TempDir::new().unwrap();
    let engine = Arc::new(Engine::new(dir.path(), "main").unwrap());
    let config = Arc::new(ServerConfig::default());
    let mut handler = logged_in_handler(config, Arc::clone(&engine), "root");
    handler.session.as_mut().unwrap().streaming_results = true;
    engine
        .execute(
            handler.session.as_mut().unwrap(),
            "CREATE TABLE empty_stream (id INT64, label VARCHAR)",
        )
        .unwrap();

    let (sender, mut receiver) = tokio::sync::mpsc::channel(4);
    let consumer = std::thread::spawn(move || {
        let mut chunks = Vec::new();
        while let Some(message) = receiver.blocking_recv() {
            chunks.push(message);
        }
        chunks
    });
    let mut payload = Vec::new();
    write_len_prefixed_string(&mut payload, "");
    write_len_prefixed_string(&mut payload, "SELECT id, label FROM empty_stream");
    let responses = handler
        .handle_message_cancellable(
            Message::new(MSG_TYPE_QUERY, Bytes::from(payload)),
            Arc::new(crate::common::query_cancel::QueryContext::new(Arc::new(
                AtomicBool::new(false),
            ))),
            Some(sender),
        )
        .unwrap();
    drop(handler);
    let chunks = consumer.join().unwrap();

    assert_eq!(
        chunks.len(),
        1,
        "an empty result must still deliver its schema, got {} chunk(s)",
        chunks.len()
    );
    assert_eq!(chunks[0].msg_type, MSG_TYPE_RESULT_CHUNK);
    let crate::net::client::QueryResult::Rows {
        schema, batches, ..
    } = crate::net::client::parse_binary_result(&chunks[0].payload).unwrap()
    else {
        panic!("schema-only chunk did not decode to rows")
    };
    let names: Vec<&str> = schema
        .columns
        .iter()
        .map(|column| column.name.as_str())
        .collect();
    assert_eq!(names, vec!["id", "label"]);
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        0
    );
    assert_eq!(responses.len(), 1);
    assert_eq!(responses[0].msg_type, MSG_TYPE_RESULT_END);
}

#[test]
fn query_response_size_reserves_timing_trailer() {
    let dir = tempfile::TempDir::new().unwrap();
    let engine = Arc::new(Engine::new(dir.path(), "main").unwrap());
    let mut config = ServerConfig::default();
    config.server.max_message_size = 64;
    let handler = Handler::new(Arc::new(config), engine);

    assert!(handler
        .validate_query_response(Message::new(
            MSG_TYPE_BINARY_QUERY_RESULT,
            Bytes::from(vec![0; 36])
        ))
        .is_ok());
    assert!(handler
        .validate_query_response(Message::new(
            MSG_TYPE_BINARY_QUERY_RESULT,
            Bytes::from(vec![0; 37])
        ))
        .is_err());
}

#[test]
fn test_login_dispatch() {
    let dir = tempfile::TempDir::new().unwrap();
    let engine = Arc::new(Engine::new(dir.path(), "main").unwrap());
    let config = Arc::new(ServerConfig::default());
    let mut handler = Handler::new(config, engine);

    let mut payload = Vec::new();
    write_len_prefixed_string(&mut payload, "root");
    write_len_prefixed_string(&mut payload, "taibai");
    write_len_prefixed_string(&mut payload, "main");
    payload.extend_from_slice(&0i32.to_le_bytes());
    payload.push(0);

    let msg = Message::new(MSG_TYPE_LOGIN, Bytes::from(payload));
    let responses = handler.handle_message(msg).unwrap();
    assert_eq!(responses.len(), 1);
    assert_eq!(responses[0].msg_type, MSG_TYPE_AUTH_OK);
    assert!(handler.session.is_some());
    assert_eq!(handler.session.as_ref().unwrap().username, "root");
}

fn logged_in_handler(config: Arc<ServerConfig>, engine: Arc<Engine>, username: &str) -> Handler {
    let mut handler = Handler::new(config, engine);
    handler.session = Some(Session::new(username.to_string(), "main".to_string()));
    handler
}

#[test]
fn duplicate_login_is_rejected_and_preserves_existing_session_state() {
    let dir = tempfile::TempDir::new().unwrap();
    let engine = Arc::new(Engine::new(dir.path(), "main").unwrap());
    let config = Arc::new(ServerConfig::default());
    let mut handler = Handler::new(config, engine);
    {
        let session = Session::new("root".to_string(), "main".to_string());
        handler.session = Some(session);
    }
    let prepared = handler
        .session
        .as_mut()
        .unwrap()
        .prepared_statements
        .prepare("INSERT INTO t (a) VALUES (?)")
        .unwrap();
    let live_id = prepared.id;

    let mut payload = Vec::new();
    write_len_prefixed_string(&mut payload, "root");
    write_len_prefixed_string(&mut payload, "taibai");
    write_len_prefixed_string(&mut payload, "main");
    payload.extend_from_slice(&0i32.to_le_bytes());
    payload.push(0);

    let responses = handler
        .handle_message(Message::new(MSG_TYPE_LOGIN, Bytes::from(payload)))
        .unwrap();
    assert_eq!(responses.len(), 1);
    assert_eq!(responses[0].msg_type, MSG_TYPE_AUTH_FAIL);
    let message = String::from_utf8_lossy(&responses[0].payload).to_string();
    assert!(message.contains("already logged in"), "message: {message}");

    let session = handler.session.as_ref().unwrap();
    assert_eq!(session.username, "root");
    assert!(
        session.prepared_statements.get(live_id).is_some(),
        "existing prepared statements must survive a rejected duplicate login"
    );
}

#[test]
fn unknown_statement_id_error_includes_next_unissued_id_hint() {
    let dir = tempfile::TempDir::new().unwrap();
    let engine = Arc::new(Engine::new(dir.path(), "main").unwrap());
    let config = Arc::new(ServerConfig::default());
    let mut handler = logged_in_handler(config, engine, "root");
    handler
        .session
        .as_mut()
        .unwrap()
        .prepared_statements
        .prepare("INSERT INTO t (a) VALUES (?)")
        .unwrap();

    let bogus_id: u32 = 3_182_271_608;

    let mut exec_payload = Vec::new();
    exec_payload.extend_from_slice(&bogus_id.to_le_bytes());
    exec_payload.extend_from_slice(&0u32.to_le_bytes());
    let response = handler
        .handle_message(Message::new(MSG_TYPE_EXECUTE, Bytes::from(exec_payload)))
        .unwrap();
    assert_eq!(response[0].msg_type, MSG_TYPE_ERROR);
    let message = String::from_utf8_lossy(&response[0].payload).to_string();
    assert!(message.contains("not found"), "message: {message}");
    assert!(
        message.contains("next unissued id is 2"),
        "message: {message}"
    );

    let mut batch_payload = Vec::new();
    batch_payload.extend_from_slice(&bogus_id.to_le_bytes());
    batch_payload.extend_from_slice(&0u32.to_le_bytes());
    let response = handler
        .handle_message(Message::new(
            MSG_TYPE_BATCH_EXECUTE,
            Bytes::from(batch_payload),
        ))
        .unwrap();
    assert_eq!(response[0].msg_type, MSG_TYPE_ERROR);
    let message = String::from_utf8_lossy(&response[0].payload).to_string();
    assert!(message.contains("not found"), "message: {message}");
    assert!(
        message.contains("next unissued id is 2"),
        "message: {message}"
    );
}

#[test]
fn prepared_insert_rejected_after_schema_change() {
    let dir = tempfile::TempDir::new().unwrap();
    let engine = Arc::new(Engine::new(dir.path(), "main").unwrap());
    let config = Arc::new(ServerConfig::default());
    let mut handler = Handler::new(config, engine);

    let mut payload = Vec::new();
    write_len_prefixed_string(&mut payload, "root");
    write_len_prefixed_string(&mut payload, "taibai");
    write_len_prefixed_string(&mut payload, "main");
    payload.extend_from_slice(&0i32.to_le_bytes());
    payload.push(0);
    handler
        .handle_message(Message::new(MSG_TYPE_LOGIN, Bytes::from(payload)))
        .unwrap();

    let mut sql_payload = Vec::new();
    write_len_prefixed_string(&mut sql_payload, "main");
    write_len_prefixed_string(
        &mut sql_payload,
        "CREATE TABLE drift (id INT64, value INT64)",
    );
    handler
        .handle_message(Message::new(MSG_TYPE_QUERY, Bytes::from(sql_payload)))
        .unwrap();

    let mut prep_payload = Vec::new();
    write_len_prefixed_string(&mut prep_payload, "INSERT INTO drift VALUES (?, ?)");
    let prepared = handler
        .handle_message(Message::new(MSG_TYPE_PREPARE, Bytes::from(prep_payload)))
        .unwrap();
    let stmt_id = u32::from_le_bytes(prepared[0].payload[..4].try_into().unwrap());

    let mut exec_payload = Vec::new();
    exec_payload.extend_from_slice(&stmt_id.to_le_bytes());
    exec_payload.extend_from_slice(&2u32.to_le_bytes());
    append_int64_param(&mut exec_payload, 1);
    append_int64_param(&mut exec_payload, 100);
    let response = handler
        .handle_message(Message::new(MSG_TYPE_EXECUTE, Bytes::from(exec_payload)))
        .unwrap();
    assert_eq!(ok_rows(&response), 1);

    let mut alter_payload = Vec::new();
    write_len_prefixed_string(&mut alter_payload, "main");
    write_len_prefixed_string(&mut alter_payload, "ALTER TABLE drift DROP COLUMN value");
    let response = handler
        .handle_message(Message::new(MSG_TYPE_QUERY, Bytes::from(alter_payload)))
        .unwrap();
    assert_eq!(
        response[0].msg_type,
        MSG_TYPE_OK,
        "drop column failed: {}",
        String::from_utf8_lossy(&response[0].payload)
    );

    let mut alter_payload = Vec::new();
    write_len_prefixed_string(&mut alter_payload, "main");
    write_len_prefixed_string(
        &mut alter_payload,
        "ALTER TABLE drift ADD COLUMN value VARCHAR",
    );
    let response = handler
        .handle_message(Message::new(MSG_TYPE_QUERY, Bytes::from(alter_payload)))
        .unwrap();
    assert_eq!(
        response[0].msg_type,
        MSG_TYPE_OK,
        "add column failed: {}",
        String::from_utf8_lossy(&response[0].payload)
    );

    let mut exec_payload = Vec::new();
    exec_payload.extend_from_slice(&stmt_id.to_le_bytes());
    exec_payload.extend_from_slice(&2u32.to_le_bytes());
    append_int64_param(&mut exec_payload, 2);
    append_int64_param(&mut exec_payload, 200);
    let response = handler
        .handle_message(Message::new(MSG_TYPE_EXECUTE, Bytes::from(exec_payload)))
        .unwrap();
    assert_eq!(response[0].msg_type, MSG_TYPE_ERROR);
    let message = String::from_utf8_lossy(&response[0].payload).to_string();
    assert!(message.contains("re-prepare"), "message: {message}");
    assert!(
        handler
            .session
            .as_ref()
            .unwrap()
            .prepared_statements
            .get(stmt_id)
            .is_some(),
        "rejected statement must remain cached so subsequent errors stay actionable"
    );

    let mut batch_payload = Vec::new();
    batch_payload.extend_from_slice(&stmt_id.to_le_bytes());
    batch_payload.extend_from_slice(&1u32.to_le_bytes());
    append_int64_param(&mut batch_payload, 3);
    append_int64_param(&mut batch_payload, 300);
    let response = handler
        .handle_message(Message::new(
            MSG_TYPE_BATCH_EXECUTE,
            Bytes::from(batch_payload),
        ))
        .unwrap();
    assert_eq!(response[0].msg_type, MSG_TYPE_ERROR);
    let message = String::from_utf8_lossy(&response[0].payload).to_string();
    assert!(message.contains("re-prepare"), "message: {message}");
}

fn ok_rows(resp: &[Message]) -> u64 {
    assert_eq!(resp.len(), 1);
    assert_eq!(resp[0].msg_type, MSG_TYPE_OK);
    u64::from_le_bytes(resp[0].payload[..8].try_into().unwrap())
}

fn append_int64_param(payload: &mut Vec<u8>, value: i64) {
    payload.push(DataType::Int64 as u8);
    payload.extend_from_slice(&value.to_le_bytes());
}

#[test]
fn bulk_import_mode_is_reference_counted_across_connections() {
    let dir = tempfile::TempDir::new().unwrap();
    let engine = Arc::new(Engine::new(dir.path(), "main").unwrap());
    let config = Arc::new(ServerConfig::default());
    let mut first = logged_in_handler(Arc::clone(&config), Arc::clone(&engine), "root");
    let mut second = logged_in_handler(config, Arc::clone(&engine), "admin");

    ok_rows(&first.handle_start_bulk_import("START BULK IMPORT").unwrap());
    assert!(engine.is_bulk_mode());
    assert_eq!(engine.bulk_mode_session_count(), 1);

    ok_rows(
        &second
            .handle_start_bulk_import("START BULK IMPORT")
            .unwrap(),
    );
    assert!(engine.is_bulk_mode());
    assert_eq!(engine.bulk_mode_session_count(), 2);

    assert_eq!(ok_rows(&first.handle_stop_bulk_import().unwrap()), 0);
    assert!(engine.is_bulk_mode());
    assert_eq!(engine.bulk_mode_session_count(), 1);

    first.session.as_mut().unwrap().had_writes = true;
    first.flush_on_disconnect();
    assert!(engine.is_bulk_mode());
    assert_eq!(engine.bulk_mode_session_count(), 1);

    ok_rows(&second.handle_stop_bulk_import().unwrap());
    assert!(!engine.is_bulk_mode());
    assert_eq!(engine.bulk_mode_session_count(), 0);
}

#[test]
fn bulk_import_disconnect_releases_only_that_connection() {
    let dir = tempfile::TempDir::new().unwrap();
    let engine = Arc::new(Engine::new(dir.path(), "main").unwrap());
    let config = Arc::new(ServerConfig::default());
    let mut first = logged_in_handler(Arc::clone(&config), Arc::clone(&engine), "root");
    let mut second = logged_in_handler(config, Arc::clone(&engine), "admin");

    ok_rows(&first.handle_start_bulk_import("START BULK IMPORT").unwrap());
    ok_rows(
        &second
            .handle_start_bulk_import("START BULK IMPORT")
            .unwrap(),
    );

    first.flush_on_disconnect();
    assert!(engine.is_bulk_mode());
    assert_eq!(engine.bulk_mode_session_count(), 1);
    assert!(!first.session.as_ref().unwrap().bulk_import_mode);

    first.flush_on_disconnect();
    assert_eq!(engine.bulk_mode_session_count(), 1);

    ok_rows(&second.handle_stop_bulk_import().unwrap());
    assert!(!engine.is_bulk_mode());
    assert_eq!(engine.bulk_mode_session_count(), 0);
}

#[test]
fn ordinary_disconnect_does_not_materialize_low_rate_inserts() {
    let dir = tempfile::TempDir::new().unwrap();
    let engine = Arc::new(Engine::new(dir.path(), "main").unwrap());
    let config = Arc::new(ServerConfig::default());
    let mut handler = logged_in_handler(config, Arc::clone(&engine), "root");

    engine
        .execute(
            handler.session.as_mut().unwrap(),
            "CREATE TABLE low_rate (id INT64)",
        )
        .unwrap();
    engine
        .execute(
            handler.session.as_mut().unwrap(),
            "INSERT INTO low_rate VALUES (1)",
        )
        .unwrap();
    handler.session.as_mut().unwrap().had_writes = true;

    handler.flush_on_disconnect();

    let table_dir = dir.path().join("main").join("low_rate");
    assert!(SegmentManager::discover_segments(&table_dir)
        .unwrap()
        .is_empty());
}

#[test]
fn stopping_bulk_import_flushes_only_tables_written_in_bulk_mode() {
    let dir = tempfile::TempDir::new().unwrap();
    let engine = Arc::new(Engine::new(dir.path(), "main").unwrap());
    let config = Arc::new(ServerConfig::default());
    let mut handler = logged_in_handler(config, Arc::clone(&engine), "root");

    engine
        .execute(
            handler.session.as_mut().unwrap(),
            "CREATE TABLE bulk_target (id INT64)",
        )
        .unwrap();
    engine
        .execute(
            handler.session.as_mut().unwrap(),
            "CREATE TABLE low_rate (id INT64)",
        )
        .unwrap();
    engine
        .execute(
            handler.session.as_mut().unwrap(),
            "INSERT INTO low_rate VALUES (1)",
        )
        .unwrap();

    ok_rows(
        &handler
            .handle_start_bulk_import("START BULK IMPORT")
            .unwrap(),
    );
    engine
        .execute(
            handler.session.as_mut().unwrap(),
            "INSERT INTO bulk_target VALUES (2)",
        )
        .unwrap();
    ok_rows(&handler.handle_stop_bulk_import().unwrap());

    let target_dir = dir.path().join("main").join("bulk_target");
    let low_rate_dir = dir.path().join("main").join("low_rate");
    assert!(!SegmentManager::discover_segments(&target_dir)
        .unwrap()
        .is_empty());
    assert!(SegmentManager::discover_segments(&low_rate_dir)
        .unwrap()
        .is_empty());
}

#[test]
fn test_ping_dispatch() {
    let dir = tempfile::TempDir::new().unwrap();
    let engine = Arc::new(Engine::new(dir.path(), "main").unwrap());
    let config = Arc::new(ServerConfig::default());
    let mut handler = Handler::new(config, engine);

    let msg = Message::empty(MSG_TYPE_PING);
    let responses = handler.handle_message(msg).unwrap();
    assert_eq!(responses.len(), 1);
    assert_eq!(responses[0].msg_type, MSG_TYPE_PONG);
}

#[test]
fn test_logout_dispatch() {
    let dir = tempfile::TempDir::new().unwrap();
    let engine = Arc::new(Engine::new(dir.path(), "main").unwrap());
    let config = Arc::new(ServerConfig::default());
    let mut handler = Handler::new(config, engine);

    let msg = Message::empty(MSG_TYPE_LOGOUT);
    let responses = handler.handle_message(msg).unwrap();
    assert!(responses.is_empty());
}

#[test]
fn test_prepare_and_execute() {
    let dir = tempfile::TempDir::new().unwrap();
    let engine = Arc::new(Engine::new(dir.path(), "main").unwrap());
    let config = Arc::new(ServerConfig::default());
    let mut handler = Handler::new(config, engine);

    let mut payload = Vec::new();
    write_len_prefixed_string(&mut payload, "root");
    write_len_prefixed_string(&mut payload, "taibai");
    write_len_prefixed_string(&mut payload, "main");
    payload.extend_from_slice(&0i32.to_le_bytes());
    payload.push(0);
    handler
        .handle_message(Message::new(MSG_TYPE_LOGIN, Bytes::from(payload)))
        .unwrap();

    let mut sql_payload = Vec::new();
    write_len_prefixed_string(&mut sql_payload, "main");
    write_len_prefixed_string(&mut sql_payload, "CREATE TABLE t (id INT64)");
    handler
        .handle_message(Message::new(MSG_TYPE_QUERY, Bytes::from(sql_payload)))
        .unwrap();

    let mut prep_payload = Vec::new();
    write_len_prefixed_string(&mut prep_payload, "SELECT 42");
    let resp = handler
        .handle_message(Message::new(MSG_TYPE_PREPARE, Bytes::from(prep_payload)))
        .unwrap();
    assert_eq!(resp[0].msg_type, MSG_TYPE_PREPARE);

    let exec_payload = Bytes::copy_from_slice(&1u32.to_le_bytes());
    let resp = handler
        .handle_message(Message::new(MSG_TYPE_EXECUTE, exec_payload))
        .unwrap();
    assert_eq!(resp[0].msg_type, MSG_TYPE_BINARY_QUERY_RESULT);

    let close_payload = Bytes::copy_from_slice(&1u32.to_le_bytes());
    let resp = handler
        .handle_message(Message::new(MSG_TYPE_CLOSE_STMT, close_payload))
        .unwrap();
    assert_eq!(resp[0].msg_type, MSG_TYPE_OK);
}

#[test]
fn test_prepare_execute_with_params() {
    let dir = tempfile::TempDir::new().unwrap();
    let engine = Arc::new(Engine::new(dir.path(), "main").unwrap());
    let config = Arc::new(ServerConfig::default());
    let mut handler = Handler::new(config, engine);

    let mut payload = Vec::new();
    write_len_prefixed_string(&mut payload, "root");
    write_len_prefixed_string(&mut payload, "taibai");
    write_len_prefixed_string(&mut payload, "main");
    payload.extend_from_slice(&0i32.to_le_bytes());
    payload.push(0);
    handler
        .handle_message(Message::new(MSG_TYPE_LOGIN, Bytes::from(payload)))
        .unwrap();

    let mut sql_payload = Vec::new();
    write_len_prefixed_string(&mut sql_payload, "main");
    write_len_prefixed_string(
        &mut sql_payload,
        "CREATE TABLE t (id INT64, name VARCHAR, score FLOAT64)",
    );
    handler
        .handle_message(Message::new(MSG_TYPE_QUERY, Bytes::from(sql_payload)))
        .unwrap();

    // Insert a row
    let mut sql_payload = Vec::new();
    write_len_prefixed_string(&mut sql_payload, "main");
    write_len_prefixed_string(&mut sql_payload, "INSERT INTO t VALUES (1, 'alice', 95.5)");
    handler
        .handle_message(Message::new(MSG_TYPE_QUERY, Bytes::from(sql_payload)))
        .unwrap();

    // Prepare SELECT with placeholders
    let mut prep_payload = Vec::new();
    write_len_prefixed_string(&mut prep_payload, "SELECT * FROM t WHERE id = ?");
    let resp = handler
        .handle_message(Message::new(MSG_TYPE_PREPARE, Bytes::from(prep_payload)))
        .unwrap();
    assert_eq!(resp[0].msg_type, MSG_TYPE_PREPARE);
    let stmt_id = u32::from_le_bytes(resp[0].payload[..4].try_into().unwrap());
    let param_count = u32::from_le_bytes(resp[0].payload[4..8].try_into().unwrap());
    assert_eq!(param_count, 1);

    // Execute with Int64 param = 1
    let mut exec_payload = Vec::new();
    exec_payload.extend_from_slice(&stmt_id.to_le_bytes());
    exec_payload.extend_from_slice(&1u32.to_le_bytes()); // param_count
    exec_payload.push(DataType::Int64 as u8); // type_tag
    exec_payload.extend_from_slice(&1i64.to_le_bytes()); // value
    let resp = handler
        .handle_message(Message::new(MSG_TYPE_EXECUTE, Bytes::from(exec_payload)))
        .unwrap();
    assert_eq!(resp[0].msg_type, MSG_TYPE_BINARY_QUERY_RESULT);

    // Execute with Int64 param = 999 (should return 0 rows)
    let mut exec_payload = Vec::new();
    exec_payload.extend_from_slice(&stmt_id.to_le_bytes());
    exec_payload.extend_from_slice(&1u32.to_le_bytes());
    exec_payload.push(DataType::Int64 as u8);
    exec_payload.extend_from_slice(&999i64.to_le_bytes());
    let resp = handler
        .handle_message(Message::new(MSG_TYPE_EXECUTE, Bytes::from(exec_payload)))
        .unwrap();
    assert_eq!(resp[0].msg_type, MSG_TYPE_BINARY_QUERY_RESULT);

    // Close
    let close_payload = Bytes::copy_from_slice(&stmt_id.to_le_bytes());
    let resp = handler
        .handle_message(Message::new(MSG_TYPE_CLOSE_STMT, close_payload))
        .unwrap();
    assert_eq!(resp[0].msg_type, MSG_TYPE_OK);
}

#[test]
fn prepared_insert_batch_writes_all_rows_as_one_batch() {
    let dir = tempfile::TempDir::new().unwrap();
    let engine = Arc::new(Engine::new(dir.path(), "main").unwrap());
    let config = Arc::new(ServerConfig::default());
    let mut handler = logged_in_handler(config, Arc::clone(&engine), "root");

    let mut sql_payload = Vec::new();
    write_len_prefixed_string(&mut sql_payload, "main");
    write_len_prefixed_string(
        &mut sql_payload,
        "CREATE TABLE batch_insert (id INT64, value INT64, UNIQUE KEY uk_id (id))",
    );
    handler
        .handle_message(Message::new(MSG_TYPE_QUERY, Bytes::from(sql_payload)))
        .unwrap();

    let index_path = dir
        .path()
        .join("main")
        .join("batch_insert")
        .join("indexes")
        .join("unique")
        .join("uk_id.keys.tbi");
    assert!(index_path.is_file());

    let mut prep_payload = Vec::new();
    write_len_prefixed_string(&mut prep_payload, "INSERT INTO batch_insert VALUES (?, ?)");
    let prepared = handler
        .handle_message(Message::new(MSG_TYPE_PREPARE, Bytes::from(prep_payload)))
        .unwrap();
    let stmt_id = u32::from_le_bytes(prepared[0].payload[..4].try_into().unwrap());

    let mut batch_payload = Vec::new();
    batch_payload.extend_from_slice(&stmt_id.to_le_bytes());
    const ROWS: u32 = 5_000;
    batch_payload.extend_from_slice(&ROWS.to_le_bytes());
    for id in 1..=i64::from(ROWS) {
        append_int64_param(&mut batch_payload, id);
        append_int64_param(&mut batch_payload, id * 10);
    }
    let response = handler
        .handle_message(Message::new(
            MSG_TYPE_BATCH_EXECUTE,
            Bytes::from(batch_payload),
        ))
        .unwrap();
    assert_eq!(ok_rows(&response), u64::from(ROWS));

    let catalogs = engine.catalogs().read().unwrap();
    assert_eq!(
        catalogs
            .get("main")
            .unwrap()
            .get_table("batch_insert")
            .unwrap()
            .row_count,
        i64::from(ROWS)
    );
}

#[test]
fn prepared_replace_batch_executes_as_one_logical_replace() {
    let dir = tempfile::TempDir::new().unwrap();
    let engine = Arc::new(Engine::new(dir.path(), "main").unwrap());
    let config = Arc::new(ServerConfig::default());
    let mut handler = logged_in_handler(config, Arc::clone(&engine), "root");

    let mut sql_payload = Vec::new();
    write_len_prefixed_string(&mut sql_payload, "main");
    write_len_prefixed_string(
        &mut sql_payload,
        "CREATE TABLE batch_replace (id INT64 PRIMARY KEY, value INT64)",
    );
    handler
        .handle_message(Message::new(MSG_TYPE_QUERY, Bytes::from(sql_payload)))
        .unwrap();

    let mut insert_payload = Vec::new();
    write_len_prefixed_string(&mut insert_payload, "main");
    write_len_prefixed_string(
        &mut insert_payload,
        "INSERT INTO batch_replace VALUES (1, 10), (2, 20)",
    );
    handler
        .handle_message(Message::new(MSG_TYPE_QUERY, Bytes::from(insert_payload)))
        .unwrap();

    let mut prep_payload = Vec::new();
    write_len_prefixed_string(
        &mut prep_payload,
        "REPLACE INTO batch_replace VALUES (?, ?)",
    );
    let prepared = handler
        .handle_message(Message::new(MSG_TYPE_PREPARE, Bytes::from(prep_payload)))
        .unwrap();
    let stmt_id = u32::from_le_bytes(prepared[0].payload[..4].try_into().unwrap());

    let mut batch_payload = Vec::new();
    batch_payload.extend_from_slice(&stmt_id.to_le_bytes());
    batch_payload.extend_from_slice(&4u32.to_le_bytes());
    for (id, value) in [(1, 11), (3, 30), (3, 31), (4, 40)] {
        append_int64_param(&mut batch_payload, id);
        append_int64_param(&mut batch_payload, value);
    }
    let response = handler
        .handle_message(Message::new(
            MSG_TYPE_BATCH_EXECUTE,
            Bytes::from(batch_payload),
        ))
        .unwrap();
    assert_eq!(ok_rows(&response), 6);

    let mut session = Session::new("root".to_string(), "main".to_string());
    let result = engine
        .execute(
            &mut session,
            "SELECT id, value FROM batch_replace ORDER BY id",
        )
        .unwrap();
    let ExecResult::Query { batches, .. } = result else {
        panic!("expected query result")
    };
    let rows = batches
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows())
                .map(|row| (batch.scalar(row, 0).unwrap(), batch.scalar(row, 1).unwrap()))
        })
        .collect::<Vec<_>>();
    assert_eq!(
        rows,
        vec![
            (ScalarValue::Int64(Some(1)), ScalarValue::Int64(Some(11))),
            (ScalarValue::Int64(Some(2)), ScalarValue::Int64(Some(20))),
            (ScalarValue::Int64(Some(3)), ScalarValue::Int64(Some(31))),
            (ScalarValue::Int64(Some(4)), ScalarValue::Int64(Some(40))),
        ]
    );
    assert_eq!(
        engine
            .catalogs()
            .read()
            .unwrap()
            .get("main")
            .unwrap()
            .get_table("batch_replace")
            .unwrap()
            .row_count,
        4
    );
}

#[test]
fn prepared_insert_batch_rejects_duplicates_before_writing_any_row() {
    let dir = tempfile::TempDir::new().unwrap();
    let engine = Arc::new(Engine::new(dir.path(), "main").unwrap());
    let config = Arc::new(ServerConfig::default());
    let mut handler = logged_in_handler(config, Arc::clone(&engine), "root");

    let mut sql_payload = Vec::new();
    write_len_prefixed_string(&mut sql_payload, "main");
    write_len_prefixed_string(
        &mut sql_payload,
        "CREATE TABLE batch_duplicate (id INT64, UNIQUE KEY uk_id (id))",
    );
    handler
        .handle_message(Message::new(MSG_TYPE_QUERY, Bytes::from(sql_payload)))
        .unwrap();

    let mut prep_payload = Vec::new();
    write_len_prefixed_string(&mut prep_payload, "INSERT INTO batch_duplicate VALUES (?)");
    let prepared = handler
        .handle_message(Message::new(MSG_TYPE_PREPARE, Bytes::from(prep_payload)))
        .unwrap();
    let stmt_id = u32::from_le_bytes(prepared[0].payload[..4].try_into().unwrap());

    let mut batch_payload = Vec::new();
    batch_payload.extend_from_slice(&stmt_id.to_le_bytes());
    batch_payload.extend_from_slice(&2u32.to_le_bytes());
    append_int64_param(&mut batch_payload, 7);
    append_int64_param(&mut batch_payload, 7);
    let response = handler
        .handle_message(Message::new(
            MSG_TYPE_BATCH_EXECUTE,
            Bytes::from(batch_payload),
        ))
        .unwrap();
    assert_eq!(response.len(), 1);
    assert_eq!(response[0].msg_type, MSG_TYPE_ERROR);

    let catalogs = engine.catalogs().read().unwrap();
    assert_eq!(
        catalogs
            .get("main")
            .unwrap()
            .get_table("batch_duplicate")
            .unwrap()
            .row_count,
        0
    );
}

#[test]
fn test_prepare_execute_with_null_param() {
    let dir = tempfile::TempDir::new().unwrap();
    let engine = Arc::new(Engine::new(dir.path(), "main").unwrap());
    let config = Arc::new(ServerConfig::default());
    let mut handler = Handler::new(config, engine);

    let mut payload = Vec::new();
    write_len_prefixed_string(&mut payload, "root");
    write_len_prefixed_string(&mut payload, "taibai");
    write_len_prefixed_string(&mut payload, "main");
    payload.extend_from_slice(&0i32.to_le_bytes());
    payload.push(0);
    handler
        .handle_message(Message::new(MSG_TYPE_LOGIN, Bytes::from(payload)))
        .unwrap();

    let mut sql_payload = Vec::new();
    write_len_prefixed_string(&mut sql_payload, "main");
    write_len_prefixed_string(&mut sql_payload, "CREATE TABLE t2 (id INT64, val VARCHAR)");
    handler
        .handle_message(Message::new(MSG_TYPE_QUERY, Bytes::from(sql_payload)))
        .unwrap();

    let mut prep_payload = Vec::new();
    write_len_prefixed_string(&mut prep_payload, "INSERT INTO t2 VALUES (?, ?)");
    let resp = handler
        .handle_message(Message::new(MSG_TYPE_PREPARE, Bytes::from(prep_payload)))
        .unwrap();
    assert_eq!(resp[0].msg_type, MSG_TYPE_PREPARE);
    let stmt_id = u32::from_le_bytes(resp[0].payload[..4].try_into().unwrap());
    let param_count = u32::from_le_bytes(resp[0].payload[4..8].try_into().unwrap());
    assert_eq!(param_count, 2);

    // Execute with Int64 + NULL VarChar
    let mut exec_payload = Vec::new();
    exec_payload.extend_from_slice(&stmt_id.to_le_bytes());
    exec_payload.extend_from_slice(&2u32.to_le_bytes()); // param_count
    exec_payload.push(DataType::Int64 as u8);
    exec_payload.extend_from_slice(&42i64.to_le_bytes());
    exec_payload.push(0u8); // NULL
    let resp = handler
        .handle_message(Message::new(MSG_TYPE_EXECUTE, Bytes::from(exec_payload)))
        .unwrap();
    assert_eq!(resp[0].msg_type, MSG_TYPE_OK);
}

#[test]
fn test_transaction_messages() {
    let dir = tempfile::TempDir::new().unwrap();
    let engine = Arc::new(Engine::new(dir.path(), "main").unwrap());
    let config = Arc::new(ServerConfig::default());
    let mut handler = Handler::new(config, engine);

    let mut payload = Vec::new();
    write_len_prefixed_string(&mut payload, "root");
    write_len_prefixed_string(&mut payload, "taibai");
    write_len_prefixed_string(&mut payload, "main");
    payload.extend_from_slice(&0i32.to_le_bytes());
    payload.push(0);
    handler
        .handle_message(Message::new(MSG_TYPE_LOGIN, Bytes::from(payload)))
        .unwrap();

    let resp = handler
        .handle_message(Message::empty(MSG_TYPE_BEGIN_TXN))
        .unwrap();
    assert_eq!(resp[0].msg_type, MSG_TYPE_OK);

    let resp = handler
        .handle_message(Message::empty(MSG_TYPE_COMMIT_TXN))
        .unwrap();
    assert_eq!(resp[0].msg_type, MSG_TYPE_OK);

    handler
        .handle_message(Message::empty(MSG_TYPE_BEGIN_TXN))
        .unwrap();
    let resp = handler
        .handle_message(Message::empty(MSG_TYPE_ROLLBACK_TXN))
        .unwrap();
    assert_eq!(resp[0].msg_type, MSG_TYPE_OK);
}

#[test]
fn test_txn_error_nested_begin() {
    let dir = tempfile::TempDir::new().unwrap();
    let engine = Arc::new(Engine::new(dir.path(), "main").unwrap());
    let config = Arc::new(ServerConfig::default());
    let mut handler = Handler::new(config, engine);

    let mut payload = Vec::new();
    write_len_prefixed_string(&mut payload, "root");
    write_len_prefixed_string(&mut payload, "taibai");
    write_len_prefixed_string(&mut payload, "main");
    payload.extend_from_slice(&0i32.to_le_bytes());
    payload.push(0);
    handler
        .handle_message(Message::new(MSG_TYPE_LOGIN, Bytes::from(payload)))
        .unwrap();

    handler
        .handle_message(Message::empty(MSG_TYPE_BEGIN_TXN))
        .unwrap();
    let resp = handler
        .handle_message(Message::empty(MSG_TYPE_BEGIN_TXN))
        .unwrap();
    assert_eq!(resp[0].msg_type, MSG_TYPE_ERROR);
    let msg = String::from_utf8_lossy(&resp[0].payload);
    assert!(msg.contains("already in progress"), "got: {msg}");
}

#[test]
fn test_txn_error_commit_without_active() {
    let dir = tempfile::TempDir::new().unwrap();
    let engine = Arc::new(Engine::new(dir.path(), "main").unwrap());
    let config = Arc::new(ServerConfig::default());
    let mut handler = Handler::new(config, engine);

    let mut payload = Vec::new();
    write_len_prefixed_string(&mut payload, "root");
    write_len_prefixed_string(&mut payload, "taibai");
    write_len_prefixed_string(&mut payload, "main");
    payload.extend_from_slice(&0i32.to_le_bytes());
    payload.push(0);
    handler
        .handle_message(Message::new(MSG_TYPE_LOGIN, Bytes::from(payload)))
        .unwrap();

    let resp = handler
        .handle_message(Message::empty(MSG_TYPE_COMMIT_TXN))
        .unwrap();
    assert_eq!(resp[0].msg_type, MSG_TYPE_ERROR);
    let msg = String::from_utf8_lossy(&resp[0].payload);
    assert!(msg.contains("no active transaction"), "got: {msg}");
}

#[test]
fn test_txn_error_rollback_without_active() {
    let dir = tempfile::TempDir::new().unwrap();
    let engine = Arc::new(Engine::new(dir.path(), "main").unwrap());
    let config = Arc::new(ServerConfig::default());
    let mut handler = Handler::new(config, engine);

    let mut payload = Vec::new();
    write_len_prefixed_string(&mut payload, "root");
    write_len_prefixed_string(&mut payload, "taibai");
    write_len_prefixed_string(&mut payload, "main");
    payload.extend_from_slice(&0i32.to_le_bytes());
    payload.push(0);
    handler
        .handle_message(Message::new(MSG_TYPE_LOGIN, Bytes::from(payload)))
        .unwrap();

    let resp = handler
        .handle_message(Message::empty(MSG_TYPE_ROLLBACK_TXN))
        .unwrap();
    assert_eq!(resp[0].msg_type, MSG_TYPE_ERROR);
    let msg = String::from_utf8_lossy(&resp[0].payload);
    assert!(msg.contains("no active transaction"), "got: {msg}");
}

#[test]
fn prepare_insert_returns_decimal_target_scale() {
    let dir = tempfile::TempDir::new().unwrap();
    let engine = Arc::new(Engine::new(dir.path(), "main").unwrap());
    let config = Arc::new(ServerConfig::default());
    let mut handler = logged_in_handler(config, engine, "root");

    let mut query = Vec::new();
    write_len_prefixed_string(&mut query, "main");
    write_len_prefixed_string(
        &mut query,
        "CREATE TABLE prices (id INT64, amount DECIMAL(12,4))",
    );
    handler
        .handle_message(Message::new(MSG_TYPE_QUERY, Bytes::from(query)))
        .unwrap();

    for sql in [
        "INSERT INTO prices (id, amount) VALUES (?, ?)",
        "INSERT INTO prices VALUES (?, ?)",
    ] {
        let mut prepare = Vec::new();
        write_len_prefixed_string(&mut prepare, sql);
        let response = handler
            .handle_message(Message::new(MSG_TYPE_PREPARE, Bytes::from(prepare)))
            .unwrap();
        assert_eq!(response[0].msg_type, MSG_TYPE_PREPARE);
        assert_eq!(
            u32::from_le_bytes(response[0].payload[4..8].try_into().unwrap()),
            2
        );
        assert_eq!(response[0].payload[10], DataType::Decimal as u8);
        assert_eq!(response[0].payload[11], 4);
    }
}

impl Handler {
    pub fn handle_message(&mut self, msg: Message) -> Result<Vec<Message>> {
        self.handle_message_cancellable(
            msg,
            Arc::new(crate::common::query_cancel::QueryContext::new(Arc::new(
                AtomicBool::new(false),
            ))),
            None,
        )
    }
}

#[test]
fn dropped_bulk_handler_releases_session_without_disconnect_flush() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(Engine::new(dir.path(), "main").unwrap());
    let mut handler = logged_in_handler(
        Arc::new(ServerConfig::default()),
        Arc::clone(&engine),
        "root",
    );
    ok_rows(
        &handler
            .handle_start_bulk_import("START BULK IMPORT")
            .unwrap(),
    );
    assert_eq!(engine.bulk_mode_session_count(), 1);
    drop(handler);
    assert_eq!(engine.bulk_mode_session_count(), 0);
    engine.segment_mgr().shutdown();
}

#[test]
fn set_streaming_results_toggles_session_flag() {
    let dir = tempfile::TempDir::new().unwrap();
    let engine = Arc::new(Engine::new(dir.path(), "main").unwrap());
    let mut handler = logged_in_handler(
        Arc::new(ServerConfig::default()),
        Arc::clone(&engine),
        "root",
    );

    let run = |handler: &mut Handler, sql: &str| -> Message {
        let mut payload = Vec::new();
        write_len_prefixed_string(&mut payload, "");
        write_len_prefixed_string(&mut payload, sql);
        handler
            .handle_message_cancellable(
                Message::new(MSG_TYPE_QUERY, Bytes::from(payload)),
                Arc::new(crate::common::query_cancel::QueryContext::new(Arc::new(
                    AtomicBool::new(false),
                ))),
                None,
            )
            .unwrap()
            .remove(0)
    };

    assert_eq!(
        run(&mut handler, "SET streaming_results = true").msg_type,
        MSG_TYPE_OK
    );
    assert!(handler.session.as_ref().unwrap().streaming_results);

    assert_eq!(
        run(&mut handler, "SET STREAMING_RESULTS = off").msg_type,
        MSG_TYPE_OK
    );
    assert!(!handler.session.as_ref().unwrap().streaming_results);

    assert_eq!(
        run(&mut handler, "SET SESSION streaming_results=1").msg_type,
        MSG_TYPE_OK
    );
    assert!(handler.session.as_ref().unwrap().streaming_results);

    let rejected = run(&mut handler, "SET streaming_results = banana");
    assert_eq!(rejected.msg_type, MSG_TYPE_ERROR);
    assert!(
        handler.session.as_ref().unwrap().streaming_results,
        "an invalid SET value must leave the previous setting unchanged"
    );

    let unknown = run(&mut handler, "SET autocommit = 1");
    assert_eq!(unknown.msg_type, MSG_TYPE_ERROR);
}

#[test]
fn streaming_splits_oversized_batches_into_frame_sized_chunks() {
    let dir = tempfile::TempDir::new().unwrap();
    let engine = Arc::new(Engine::new(dir.path(), "main").unwrap());
    let mut config = ServerConfig::default();
    config.server.max_message_size = 256;
    let mut handler = logged_in_handler(Arc::new(config), Arc::clone(&engine), "root");
    handler.session.as_mut().unwrap().streaming_results = true;
    engine
        .execute(
            handler.session.as_mut().unwrap(),
            "CREATE TABLE split_rows (id INT64)",
        )
        .unwrap();
    let values = (0..200)
        .map(|i| format!("({i})"))
        .collect::<Vec<_>>()
        .join(", ");
    engine
        .execute(
            handler.session.as_mut().unwrap(),
            &format!("INSERT INTO split_rows VALUES {values}"),
        )
        .unwrap();

    let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
    let consumer = std::thread::spawn(move || {
        let mut chunks = Vec::new();
        while let Some(message) = receiver.blocking_recv() {
            chunks.push(message);
        }
        chunks
    });
    let mut payload = Vec::new();
    write_len_prefixed_string(&mut payload, "");
    write_len_prefixed_string(&mut payload, "SELECT id FROM split_rows");
    let responses = handler
        .handle_message_cancellable(
            Message::new(MSG_TYPE_QUERY, Bytes::from(payload)),
            Arc::new(crate::common::query_cancel::QueryContext::new(Arc::new(
                AtomicBool::new(false),
            ))),
            Some(sender),
        )
        .unwrap();
    drop(handler);
    let chunks = consumer.join().unwrap();

    assert!(
        chunks.len() >= 2,
        "oversized batch must split, got {}",
        chunks.len()
    );
    assert!(chunks
        .iter()
        .all(|message| message.msg_type == MSG_TYPE_RESULT_CHUNK && message.payload.len() <= 256));
    assert_eq!(responses.len(), 1);
    assert_eq!(responses[0].msg_type, MSG_TYPE_RESULT_END);

    let mut rows = Vec::new();
    for chunk in &chunks {
        let crate::net::client::QueryResult::Rows { batches, .. } =
            crate::net::client::parse_binary_result(&chunk.payload).unwrap()
        else {
            panic!("chunk did not decode to rows")
        };
        for batch in batches {
            for row in 0..batch.num_rows() {
                rows.push(batch.scalar(row, 0).unwrap());
            }
        }
    }
    let expected: Vec<ScalarValue> = (0..200).map(|i| ScalarValue::Int64(Some(i))).collect();
    assert_eq!(rows, expected);
}
#[test]
fn batch_execute_rejects_oversized_batch_size() {
    let dir = tempfile::TempDir::new().unwrap();
    let engine = Arc::new(Engine::new(dir.path(), "main").unwrap());
    let config = Arc::new(ServerConfig::default());
    let mut handler = logged_in_handler(config, Arc::clone(&engine), "root");
    let mut sql_payload = Vec::new();
    write_len_prefixed_string(&mut sql_payload, "main");
    write_len_prefixed_string(
        &mut sql_payload,
        "CREATE TABLE batch_oversized (id INT64, value INT64)",
    );
    handler
        .handle_message(Message::new(MSG_TYPE_QUERY, Bytes::from(sql_payload)))
        .unwrap();

    let mut prep_payload = Vec::new();
    write_len_prefixed_string(
        &mut prep_payload,
        "INSERT INTO batch_oversized VALUES (?, ?)",
    );
    let prepared = handler
        .handle_message(Message::new(MSG_TYPE_PREPARE, Bytes::from(prep_payload)))
        .unwrap();
    let stmt_id = u32::from_le_bytes(prepared[0].payload[..4].try_into().unwrap());

    // Claims ~4 billion rows but carries no parameter bytes: must be rejected
    // up front instead of preallocating a giant Vec.
    let mut batch_payload = Vec::new();
    batch_payload.extend_from_slice(&stmt_id.to_le_bytes());
    batch_payload.extend_from_slice(&u32::MAX.to_le_bytes());
    let response = handler
        .handle_message(Message::new(
            MSG_TYPE_BATCH_EXECUTE,
            Bytes::from(batch_payload),
        ))
        .unwrap();
    assert_eq!(response.len(), 1);
    assert_ne!(response[0].msg_type, MSG_TYPE_PREPARE);
    let text = String::from_utf8_lossy(&response[0].payload);
    assert!(
        text.contains("exceeds limit"),
        "unexpected response: {}",
        text
    );

    // The prepared statement must still be usable after the rejection.
    let mut ok_payload = Vec::new();
    ok_payload.extend_from_slice(&stmt_id.to_le_bytes());
    ok_payload.extend_from_slice(&1u32.to_le_bytes());
    append_int64_param(&mut ok_payload, 1);
    append_int64_param(&mut ok_payload, 10);
    let response = handler
        .handle_message(Message::new(
            MSG_TYPE_BATCH_EXECUTE,
            Bytes::from(ok_payload),
        ))
        .unwrap();
    assert_eq!(ok_rows(&response), 1);
}

mod replication_push_bridge_tests {
    use super::*;
    use crate::storage::replication::{RepLog, ReplicationEvent};
    use std::time::Duration;

    fn bridge_tail_event() -> ReplicationEvent {
        ReplicationEvent::TailAppend {
            db: "main".into(),
            table: "t".into(),
            partition: "2026".into(),
            memtable_id: uuid::Uuid::new_v4(),
            first_lsn: 1,
            last_lsn: 2,
            frames: vec![1, 2, 3, 4],
        }
    }

    /// Join a bridge thread through a bounded wait so a regression hangs the
    /// assert instead of the whole test binary.
    fn join_with_timeout(bridge: std::thread::JoinHandle<()>) {
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _ = bridge.join();
            let _ = done_tx.send(());
        });
        done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("bridge thread did not exit");
    }

    #[test]
    fn bridge_exits_when_client_disconnects_on_idle_primary() {
        let temp = tempfile::TempDir::new().unwrap();
        let rep_log = std::sync::Arc::new(RepLog::new(64, u64::MAX, temp.path()));
        let (push_tx, push_rx) = tokio::sync::mpsc::channel::<Message>(1);
        // Standby vanishes; the primary publishes nothing afterwards, so the
        // only exit signal is the park-timeout disconnect probe.
        drop(push_rx);

        let bridge = spawn_replication_push_bridge(
            rep_log.clone(),
            push_tx,
            0,
            Vec::new(),
            Duration::from_millis(100),
        );
        join_with_timeout(bridge);
        assert_eq!(rep_log.session_count(), 0);
    }

    #[test]
    fn bridge_caps_pending_buffer_on_permanent_seq_gap() {
        let temp = tempfile::TempDir::new().unwrap();
        let rep_log = std::sync::Arc::new(RepLog::new(64, u64::MAX, temp.path()));
        let (push_tx, _push_rx) = tokio::sync::mpsc::channel::<Message>(16);

        // History missing seq 1: `next` never advances, so every replayed
        // event piles up in `pending` until the cap drops the session.
        let history: Vec<(u64, std::sync::Arc<ReplicationEvent>)> =
            (2..=(REPL_PUSH_MAX_PENDING as u64 + 3))
                .map(|seq| (seq, std::sync::Arc::new(bridge_tail_event())))
                .collect();

        let bridge = spawn_replication_push_bridge(
            rep_log.clone(),
            push_tx,
            0,
            history,
            Duration::from_millis(100),
        );
        join_with_timeout(bridge);
        assert_eq!(rep_log.session_count(), 0);
    }

    #[tokio::test]
    async fn bridge_streams_history_in_seq_order_and_releases_session() {
        let temp = tempfile::TempDir::new().unwrap();
        let rep_log = std::sync::Arc::new(RepLog::new(64, u64::MAX, temp.path()));
        let (push_tx, mut push_rx) = tokio::sync::mpsc::channel::<Message>(64);

        let history: Vec<(u64, std::sync::Arc<ReplicationEvent>)> = (1..=4)
            .map(|seq| (seq, std::sync::Arc::new(bridge_tail_event())))
            .collect();
        let bridge = spawn_replication_push_bridge(
            rep_log.clone(),
            push_tx,
            0,
            history,
            Duration::from_millis(100),
        );

        for expected_seq in 1..=4u64 {
            let message = tokio::time::timeout(Duration::from_secs(5), push_rx.recv())
                .await
                .expect("push timed out")
                .expect("push channel closed");
            let seq = u64::from_le_bytes(message.payload[..8].try_into().unwrap());
            assert_eq!(seq, expected_seq);
        }

        // Client disappears; the probe must reap the bridge and session.
        drop(push_rx);
        join_with_timeout(bridge);
        assert_eq!(rep_log.session_count(), 0);
    }
}

#[test]
fn empty_result_keeps_columns_without_streaming() {
    let dir = tempfile::TempDir::new().unwrap();
    let engine = Arc::new(Engine::new(dir.path(), "main").unwrap());
    let config = Arc::new(ServerConfig::default());
    let mut handler = logged_in_handler(config, Arc::clone(&engine), "root");
    engine
        .execute(
            handler.session.as_mut().unwrap(),
            "CREATE TABLE empty_single (id INT64, label VARCHAR)",
        )
        .unwrap();

    let mut payload = Vec::new();
    write_len_prefixed_string(&mut payload, "");
    write_len_prefixed_string(&mut payload, "SELECT id, label FROM empty_single");
    let responses = handler
        .handle_message_cancellable(
            Message::new(MSG_TYPE_QUERY, Bytes::from(payload)),
            Arc::new(crate::common::query_cancel::QueryContext::new(Arc::new(
                AtomicBool::new(false),
            ))),
            None,
        )
        .unwrap();

    assert_eq!(responses.len(), 1);
    assert_eq!(responses[0].msg_type, MSG_TYPE_BINARY_QUERY_RESULT);
    let crate::net::client::QueryResult::Rows {
        schema, batches, ..
    } = crate::net::client::parse_binary_result(&responses[0].payload).unwrap()
    else {
        panic!("empty result did not decode to rows")
    };
    let names: Vec<&str> = schema
        .columns
        .iter()
        .map(|column| column.name.as_str())
        .collect();
    assert_eq!(names, vec!["id", "label"]);
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        0
    );
}
