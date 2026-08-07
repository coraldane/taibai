use super::*;
use crate::storage::segment_manager::SegmentManager;

#[test]
fn query_log_classifier_only_tracks_result_set_queries() {
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
            Arc::new(AtomicBool::new(false)),
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
    handler.session.as_mut().unwrap().binary_results = true;
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
            Arc::new(AtomicBool::new(false)),
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
fn query_response_size_reserves_timing_trailer() {
    let dir = tempfile::TempDir::new().unwrap();
    let engine = Arc::new(Engine::new(dir.path(), "main").unwrap());
    let mut config = ServerConfig::default();
    config.server.max_message_size = 64;
    let handler = Handler::new(Arc::new(config), engine);

    assert!(handler
        .validate_query_response(Message::new(MSG_TYPE_RESULT, Bytes::from(vec![0; 36])))
        .is_ok());
    assert!(handler
        .validate_query_response(Message::new(MSG_TYPE_RESULT, Bytes::from(vec![0; 37])))
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
    let mut first = logged_in_handler(Arc::clone(&config), Arc::clone(&engine), "first");
    let mut second = logged_in_handler(config, Arc::clone(&engine), "second");

    ok_rows(
        &first
            .handle_start_bulk_import("START BULK IMPORT {\"disable_wal\": true}")
            .unwrap(),
    );
    assert!(engine.is_bulk_mode());
    assert_eq!(engine.bulk_mode_session_count(), 1);

    ok_rows(
        &second
            .handle_start_bulk_import("START BULK IMPORT {\"disable_wal\": true}")
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
    let mut first = logged_in_handler(Arc::clone(&config), Arc::clone(&engine), "first");
    let mut second = logged_in_handler(config, Arc::clone(&engine), "second");

    ok_rows(
        &first
            .handle_start_bulk_import("START BULK IMPORT {\"disable_wal\": true}")
            .unwrap(),
    );
    ok_rows(
        &second
            .handle_start_bulk_import("START BULK IMPORT {\"disable_wal\": true}")
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
    assert_eq!(resp[0].msg_type, MSG_TYPE_RESULT);

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
    assert_eq!(resp[0].msg_type, MSG_TYPE_RESULT);

    // Execute with Int64 param = 999 (should return 0 rows)
    let mut exec_payload = Vec::new();
    exec_payload.extend_from_slice(&stmt_id.to_le_bytes());
    exec_payload.extend_from_slice(&1u32.to_le_bytes());
    exec_payload.push(DataType::Int64 as u8);
    exec_payload.extend_from_slice(&999i64.to_le_bytes());
    let resp = handler
        .handle_message(Message::new(MSG_TYPE_EXECUTE, Bytes::from(exec_payload)))
        .unwrap();
    assert_eq!(resp[0].msg_type, MSG_TYPE_RESULT);

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
