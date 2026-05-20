use std::sync::Arc;
use bytes::Bytes;

use crate::common::config::ServerConfig;
use crate::common::error::Result;
use crate::net::session::Session;
use crate::protocol::message::*;
use crate::sql::execution::{Engine, ExecResult};
use crate::types::scalar::ScalarValue;

pub struct Handler {
    pub session: Option<Session>,
    config: Arc<ServerConfig>,
    engine: Arc<Engine>,
}

impl Handler {
    pub fn new(config: Arc<ServerConfig>, engine: Arc<Engine>) -> Self {
        Self {
            session: None,
            config,
            engine,
        }
    }

    pub fn handle_message(&mut self, msg: Message) -> Result<Vec<Message>> {
        match msg.msg_type {
            MSG_TYPE_LOGIN => self.handle_login(&msg.payload),
            MSG_TYPE_PING => Ok(vec![build_pong()]),
            MSG_TYPE_LOGOUT => {
                tracing::info!(session = ?self.session.as_ref().map(|s| s.session_id_string()), "logout");
                Ok(vec![])
            }
            MSG_TYPE_QUERY => self.handle_query(&msg.payload),
            MSG_TYPE_BULK_INSERT_COLUMNAR => self.handle_bulk_insert(&msg.payload),
            MSG_TYPE_FLUSH => self.handle_flush(),

            MSG_TYPE_PREPARE => self.handle_prepare(&msg.payload),
            MSG_TYPE_EXECUTE => self.handle_execute(&msg.payload),
            MSG_TYPE_CLOSE_STMT => self.handle_close_stmt(&msg.payload),

            MSG_TYPE_BEGIN_TXN => self.handle_begin_txn(),
            MSG_TYPE_COMMIT_TXN => self.handle_commit_txn(),
            MSG_TYPE_ROLLBACK_TXN => self.handle_rollback_txn(),

            MSG_TYPE_AUTH_FAIL => {
                Ok(vec![build_error("authentication failed")])
            }

            MSG_TYPE_CURSOR => self.handle_cursor_open(&msg.payload),
            MSG_TYPE_CURSOR_FETCH => self.handle_cursor_fetch(&msg.payload),
            MSG_TYPE_CURSOR_CLOSE => self.handle_cursor_close(&msg.payload),

            MSG_TYPE_BINARY_QUERY_RESULT => {
                Ok(vec![build_error("unexpected binary result message")])
            }

            other => {
                tracing::warn!(msg_type = other, "unsupported message type");
                Ok(vec![build_error(&format!("unsupported message type: {other:#04X}"))])
            }
        }
    }

    fn handle_login(&mut self, payload: &[u8]) -> Result<Vec<Message>> {
        let (username, rest) = read_len_prefixed_string(payload)?;
        let (_password, rest) = read_len_prefixed_string(rest)?;
        let (database, rest) = read_len_prefixed_string(rest)?;

        let database = if database.is_empty() {
            self.config.default_database.clone()
        } else {
            database.to_string()
        };

        let binary_results = rest.first().map(|&b| b != 0).unwrap_or(false);

        let mut session = Session::new(username.to_string(), database);
        session.binary_results = binary_results;
        let session_id = session.session_id_string();
        tracing::info!(username, session_id = %session_id, "login success");
        self.session = Some(session);

        Ok(vec![build_auth_ok(&session_id)])
    }

    fn handle_query(&mut self, payload: &[u8]) -> Result<Vec<Message>> {
        if let Some(session) = &mut self.session {
            session.touch();
        }

        let (_database, rest) = match read_len_prefixed_string(payload) {
            Ok(v) => v,
            Err(_) => ("", payload)
        };
        let sql = match read_len_prefixed_string(rest) {
            Ok((s, _)) => s.trim(),
            Err(_) => std::str::from_utf8(payload).unwrap_or("").trim()
        };

        tracing::info!(sql, "query received");

        let session = match &mut self.session {
            Some(s) => s,
            None => return Ok(vec![build_error("not logged in")]),
        };

        if sql.starts_with("START BULK IMPORT") {
            return self.handle_start_bulk_import(sql);
        }
        if sql == "STOP BULK IMPORT" {
            return self.handle_stop_bulk_import();
        }

        let use_binary = session.binary_results;

        match self.engine.execute(session, sql) {
            Ok(ExecResult::Ok { rows_affected }) => {
                Ok(vec![build_ok(rows_affected)])
            }
            Ok(ExecResult::Query { schema, batches }) => {
                if use_binary {
                    let msg = build_binary_query_result(&schema, &batches);
                    Ok(vec![msg])
                } else {
                    let msg = build_query_result(&schema, &batches);
                    Ok(vec![msg])
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, sql, "query error");
                Ok(vec![build_error(&e.to_string())])
            }
        }
    }

    fn handle_start_bulk_import(&mut self, sql: &str) -> Result<Vec<Message>> {
        let disable_wal = sql.contains("\"disable_wal\":true") || sql.contains("\"disable_wal\": true");

        self.engine.set_bulk_mode(disable_wal);

        if let Some(session) = &mut self.session {
            session.bulk_import_mode = true;
            session.disable_wal = disable_wal;
        }

        tracing::info!(disable_wal, "bulk import mode started");
        Ok(vec![build_ok(0)])
    }

    fn handle_stop_bulk_import(&mut self) -> Result<Vec<Message>> {
        self.engine.set_bulk_mode(false);
        let flushed = self.engine.flush_all_buffers()?;

        if let Some(session) = &mut self.session {
            session.bulk_import_mode = false;
            session.disable_wal = false;
        }

        tracing::info!(rows_flushed = flushed, "bulk import mode stopped");
        Ok(vec![build_ok(flushed)])
    }

    fn handle_bulk_insert(&mut self, payload: &[u8]) -> Result<Vec<Message>> {
        let session = match &self.session {
            Some(s) => s,
            None => return Ok(vec![build_error("not logged in")]),
        };

        let db = session.database.clone();
        let disable_wal = session.disable_wal;

        match self.engine.exec_bulk_insert_payload_with_wal_control(&db, payload, disable_wal) {
            Ok(row_count) => Ok(vec![build_ok(row_count)]),
            Err(e) => {
                tracing::warn!(error = %e, "bulk insert error");
                Ok(vec![build_error(&e.to_string())])
            }
        }
    }

    fn handle_prepare(&mut self, payload: &[u8]) -> Result<Vec<Message>> {
        let session = match &mut self.session {
            Some(s) => s,
            None => return Ok(vec![build_error("not logged in")]),
        };
        session.touch();

        let sql = match read_len_prefixed_string(payload) {
            Ok((s, _)) => s.trim(),
            Err(_) => std::str::from_utf8(payload).unwrap_or("").trim(),
        };

        match session.prepared_statements.prepare(sql) {
            Ok(stmt) => {
                let mut buf = Vec::new();
                buf.extend_from_slice(&stmt.id.to_le_bytes());
                buf.extend_from_slice(&(stmt.param_types.len() as u32).to_le_bytes());
                Ok(vec![Message::new(MSG_TYPE_PREPARE, Bytes::from(buf))])
            }
            Err(e) => Ok(vec![build_error(&e.to_string())]),
        }
    }

    fn handle_execute(&mut self, payload: &[u8]) -> Result<Vec<Message>> {
        let session = match &mut self.session {
            Some(s) => s,
            None => return Ok(vec![build_error("not logged in")]),
        };
        session.touch();

        if payload.len() < 4 {
            return Ok(vec![build_error("invalid execute message: missing statement ID")]);
        }
        let stmt_id = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);

        let stmt = match session.prepared_statements.get(stmt_id) {
            Some(s) => s.clone(),
            None => return Ok(vec![build_error(&format!("prepared statement {} not found", stmt_id))]),
        };

        let use_binary = session.binary_results;

        match self.engine.execute_prepared(session, &stmt.stmt) {
            Ok(ExecResult::Ok { rows_affected }) => {
                Ok(vec![build_ok(rows_affected)])
            }
            Ok(ExecResult::Query { schema, batches }) => {
                if use_binary {
                    Ok(vec![build_binary_query_result(&schema, &batches)])
                } else {
                    Ok(vec![build_query_result(&schema, &batches)])
                }
            }
            Err(e) => Ok(vec![build_error(&e.to_string())]),
        }
    }

    fn handle_close_stmt(&mut self, payload: &[u8]) -> Result<Vec<Message>> {
        let session = match &mut self.session {
            Some(s) => s,
            None => return Ok(vec![build_error("not logged in")]),
        };

        if payload.len() < 4 {
            return Ok(vec![build_error("invalid close statement message")]);
        }
        let stmt_id = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
        session.prepared_statements.close(stmt_id);
        Ok(vec![build_ok(0)])
    }

    fn handle_begin_txn(&mut self) -> Result<Vec<Message>> {
        let session = match &mut self.session {
            Some(s) => s,
            None => return Ok(vec![build_error("not logged in")]),
        };
        session.active_txn_id += 1;
        Ok(vec![build_ok(session.active_txn_id)])
    }

    fn handle_commit_txn(&mut self) -> Result<Vec<Message>> {
        let session = match &mut self.session {
            Some(s) => s,
            None => return Ok(vec![build_error("not logged in")]),
        };
        session.active_txn_id = 0;
        Ok(vec![build_ok(0)])
    }

    fn handle_rollback_txn(&mut self) -> Result<Vec<Message>> {
        let session = match &mut self.session {
            Some(s) => s,
            None => return Ok(vec![build_error("not logged in")]),
        };
        session.active_txn_id = 0;
        Ok(vec![build_ok(0)])
    }

    fn handle_cursor_open(&mut self, payload: &[u8]) -> Result<Vec<Message>> {
        self.handle_query(payload)
    }

    fn handle_cursor_fetch(&mut self, payload: &[u8]) -> Result<Vec<Message>> {
        let _ = payload;
        Ok(vec![build_error("cursors not fully implemented")])
    }

    fn handle_cursor_close(&mut self, payload: &[u8]) -> Result<Vec<Message>> {
        let _ = payload;
        Ok(vec![build_ok(0)])
    }

    fn handle_flush(&mut self) -> Result<Vec<Message>> {
        let _session = match &self.session {
            Some(s) => s,
            None => return Ok(vec![build_error("not logged in")]),
        };

        match self.engine.flush_all_buffers() {
            Ok(rows_flushed) => {
                tracing::info!(rows_flushed, "flushed all table buffers");
                Ok(vec![build_ok(rows_flushed)])
            }
            Err(e) => Ok(vec![build_error(&e.to_string())]),
        }
    }
}

fn build_auth_ok(session_id: &str) -> Message {
    Message::new(MSG_TYPE_AUTH_OK, Bytes::from(session_id.to_string()))
}

fn build_pong() -> Message {
    Message::empty(MSG_TYPE_PONG)
}

pub fn build_ok(rows_affected: u64) -> Message {
    Message::new(MSG_TYPE_OK, Bytes::copy_from_slice(&rows_affected.to_le_bytes()))
}

pub fn build_error(msg: &str) -> Message {
    Message::new(MSG_TYPE_ERROR, Bytes::from(msg.to_string()))
}

fn build_query_result(schema: &crate::types::batch::Schema, batches: &[crate::types::batch::RecordBatch]) -> Message {
    let mut buf = Vec::new();

    buf.extend_from_slice(&(schema.columns.len() as u32).to_le_bytes());

    for col in &schema.columns {
        write_len_prefixed_string(&mut buf, &col.name);
        buf.push(col.data_type as u8);
        buf.extend_from_slice(&(col.scale as u32).to_le_bytes());
    }

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    buf.extend_from_slice(&(total_rows as u32).to_le_bytes());

    for batch in batches {
        for row in 0..batch.num_rows() {
            for col_idx in 0..schema.columns.len() {
                let scalar = batch.scalar(row, col_idx)
                    .unwrap_or(ScalarValue::null_for_type(schema.columns[col_idx].data_type));

                if scalar.is_null() {
                    buf.push(1);
                } else {
                    buf.push(0);
                    write_scalar_value(&mut buf, &scalar);
                }
            }
        }
    }

    Message::new(MSG_TYPE_RESULT, Bytes::from(buf))
}

fn build_binary_query_result(schema: &crate::types::batch::Schema, batches: &[crate::types::batch::RecordBatch]) -> Message {
    let merged = if batches.len() == 1 {
        batches[0].clone()
    } else if batches.is_empty() {
        crate::types::batch::RecordBatch::new(schema.clone())
    } else {
        merge_batches(batches)
    };

    let encoded = crate::protocol::result::encode_binary_result(&merged);
    Message::new(MSG_TYPE_BINARY_QUERY_RESULT, Bytes::from(encoded))
}

fn merge_batches(batches: &[crate::types::batch::RecordBatch]) -> crate::types::batch::RecordBatch {
    if batches.is_empty() {
        return crate::types::batch::RecordBatch::new(crate::types::batch::Schema::empty());
    }
    let schema = batches[0].schema.clone();
    let num_cols = schema.columns.len();
    let mut columns = Vec::with_capacity(num_cols);

    for col_idx in 0..num_cols {
        let dt = schema.columns[col_idx].data_type;
        let mut col = crate::types::array::ColumnArray::empty(dt);
        for batch in batches {
            if let Some(src) = batch.column(col_idx) {
                for i in 0..batch.num_rows() {
                    col.push(&src.scalar(i)).unwrap();
                }
            }
        }
        columns.push(col);
    }

    crate::types::batch::RecordBatch::try_new(schema, columns).unwrap()
}

fn write_scalar_value(buf: &mut Vec<u8>, val: &ScalarValue) {
    match val {
        ScalarValue::Bool(Some(v)) => buf.push(if *v { 1 } else { 0 }),
        ScalarValue::Int8(Some(v)) => buf.push(*v as u8),
        ScalarValue::Int16(Some(v)) => buf.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::Int32(Some(v)) => buf.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::Int64(Some(v)) => buf.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::UInt8(Some(v)) => buf.push(*v),
        ScalarValue::UInt16(Some(v)) => buf.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::UInt32(Some(v)) => buf.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::UInt64(Some(v)) => buf.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::Float32(Some(v)) => buf.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::Float64(Some(v)) => buf.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::Decimal(Some(v)) => buf.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::Date(Some(v)) => buf.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::Time(Some(v)) => buf.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::Timestamp(Some(v)) => buf.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::Interval(Some(v)) => buf.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::VarChar(Some(v)) => {
            buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
            buf.extend_from_slice(v.as_bytes());
        }
        ScalarValue::Blob(Some(v)) => {
            buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
            buf.extend_from_slice(v);
        }
        ScalarValue::Vector(Some(v)) => {
            buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
            for f in v {
                buf.extend_from_slice(&f.to_le_bytes());
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_round_trip() {
        let h = MessageHeader { msg_type: MSG_TYPE_PING, payload_len: 0 };
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
        let engine = Arc::new(Engine::new(dir.path()).unwrap());
        let config = Arc::new(ServerConfig::default());
        let mut handler = Handler::new(config, engine);

        let mut payload = Vec::new();
        write_len_prefixed_string(&mut payload, "root");
        write_len_prefixed_string(&mut payload, "pass");
        write_len_prefixed_string(&mut payload, "test");

        let msg = Message::new(MSG_TYPE_LOGIN, Bytes::from(payload));
        let responses = handler.handle_message(msg).unwrap();
        assert_eq!(responses.len(), 1);
        assert_eq!(responses[0].msg_type, MSG_TYPE_AUTH_OK);
        assert!(handler.session.is_some());
        assert_eq!(handler.session.as_ref().unwrap().username, "root");
    }

    #[test]
    fn test_ping_dispatch() {
        let dir = tempfile::TempDir::new().unwrap();
        let engine = Arc::new(Engine::new(dir.path()).unwrap());
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
        let engine = Arc::new(Engine::new(dir.path()).unwrap());
        let config = Arc::new(ServerConfig::default());
        let mut handler = Handler::new(config, engine);

        let msg = Message::empty(MSG_TYPE_LOGOUT);
        let responses = handler.handle_message(msg).unwrap();
        assert!(responses.is_empty());
    }

    #[test]
    fn test_prepare_and_execute() {
        let dir = tempfile::TempDir::new().unwrap();
        let engine = Arc::new(Engine::new(dir.path()).unwrap());
        let config = Arc::new(ServerConfig::default());
        let mut handler = Handler::new(config, engine);

        let mut payload = Vec::new();
        write_len_prefixed_string(&mut payload, "root");
        write_len_prefixed_string(&mut payload, "pass");
        write_len_prefixed_string(&mut payload, "default");
        handler.handle_message(Message::new(MSG_TYPE_LOGIN, Bytes::from(payload))).unwrap();

        let mut sql_payload = Vec::new();
        write_len_prefixed_string(&mut sql_payload, "default");
        write_len_prefixed_string(&mut sql_payload, "CREATE TABLE t (id INT64)");
        handler.handle_message(Message::new(MSG_TYPE_QUERY, Bytes::from(sql_payload))).unwrap();

        let mut prep_payload = Vec::new();
        write_len_prefixed_string(&mut prep_payload, "SELECT 42");
        let resp = handler.handle_message(Message::new(MSG_TYPE_PREPARE, Bytes::from(prep_payload))).unwrap();
        assert_eq!(resp[0].msg_type, MSG_TYPE_PREPARE);

        let exec_payload = Bytes::copy_from_slice(&1u32.to_le_bytes());
        let resp = handler.handle_message(Message::new(MSG_TYPE_EXECUTE, exec_payload)).unwrap();
        assert_eq!(resp[0].msg_type, MSG_TYPE_RESULT);

        let close_payload = Bytes::copy_from_slice(&1u32.to_le_bytes());
        let resp = handler.handle_message(Message::new(MSG_TYPE_CLOSE_STMT, close_payload)).unwrap();
        assert_eq!(resp[0].msg_type, MSG_TYPE_OK);
    }

    #[test]
    fn test_transaction_messages() {
        let dir = tempfile::TempDir::new().unwrap();
        let engine = Arc::new(Engine::new(dir.path()).unwrap());
        let config = Arc::new(ServerConfig::default());
        let mut handler = Handler::new(config, engine);

        let mut payload = Vec::new();
        write_len_prefixed_string(&mut payload, "root");
        write_len_prefixed_string(&mut payload, "pass");
        write_len_prefixed_string(&mut payload, "default");
        handler.handle_message(Message::new(MSG_TYPE_LOGIN, Bytes::from(payload))).unwrap();

        let resp = handler.handle_message(Message::empty(MSG_TYPE_BEGIN_TXN)).unwrap();
        assert_eq!(resp[0].msg_type, MSG_TYPE_OK);

        let resp = handler.handle_message(Message::empty(MSG_TYPE_COMMIT_TXN)).unwrap();
        assert_eq!(resp[0].msg_type, MSG_TYPE_OK);

        handler.handle_message(Message::empty(MSG_TYPE_BEGIN_TXN)).unwrap();
        let resp = handler.handle_message(Message::empty(MSG_TYPE_ROLLBACK_TXN)).unwrap();
        assert_eq!(resp[0].msg_type, MSG_TYPE_OK);
    }
}
