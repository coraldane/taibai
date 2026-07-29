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
