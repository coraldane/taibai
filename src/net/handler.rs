use bytes::Bytes;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::common::config::ServerConfig;
use crate::common::error::Result;
use crate::net::session::Session;
use crate::protocol::message::*;
use crate::sql::execution::{Engine, ExecResult};

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

    pub fn flush_on_disconnect(&self) {
        if let Some(ref session) = self.session {
            if session.had_writes {
                match self.engine.flush_all_buffers() {
                    Ok(n) => {
                        tracing::info!(rows_flushed = n, session_id = %session.session_id_string(), "flushed buffers on disconnect")
                    }
                    Err(e) => tracing::warn!(error = %e, "failed to flush buffers on disconnect"),
                }
            }
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

            MSG_TYPE_AUTH_FAIL => Ok(vec![build_error("authentication failed")]),

            MSG_TYPE_CURSOR => self.handle_cursor_open(&msg.payload),
            MSG_TYPE_CURSOR_FETCH => self.handle_cursor_fetch(&msg.payload),
            MSG_TYPE_CURSOR_CLOSE => self.handle_cursor_close(&msg.payload),

            MSG_TYPE_BINARY_QUERY_RESULT => {
                Ok(vec![build_error("unexpected binary result message")])
            }

            other => {
                tracing::warn!(msg_type = other, "unsupported message type");
                Ok(vec![build_error(&format!(
                    "unsupported message type: {other:#04X}"
                ))])
            }
        }
    }

    fn handle_login(&mut self, payload: &[u8]) -> Result<Vec<Message>> {
        let (username, rest) = read_len_prefixed_string(payload)?;
        let (_password, rest) = read_len_prefixed_string(rest)?;
        let (database, rest) = read_len_prefixed_string(rest)?;

        let database = if database.is_empty() {
            self.config.storage.default_database.clone()
        } else {
            database.to_string()
        };

        let binary_results = rest.first().map(|&b| b != 0).unwrap_or(false);

        let timezone_offset_hours = if rest.len() >= 5 {
            i32::from_le_bytes([rest[1], rest[2], rest[3], rest[4]])
        } else {
            0
        };

        let mut session = Session::new(username.to_string(), database);
        session.binary_results = binary_results;
        session.timezone_offset_hours = timezone_offset_hours;
        let session_id = session.session_id_string();
        tracing::info!(username, session_id = %session_id, timezone_offset_hours, "login success");
        self.session = Some(session);

        Ok(vec![build_auth_ok(&session_id)])
    }

    fn handle_query(&mut self, payload: &[u8]) -> Result<Vec<Message>> {
        let server_start = Instant::now();

        if let Some(session) = &mut self.session {
            session.touch();
        }

        let (_database, rest) = match read_len_prefixed_string(payload) {
            Ok(v) => v,
            Err(_) => ("", payload),
        };
        let sql = match read_len_prefixed_string(rest) {
            Ok((s, _)) => s.trim(),
            Err(_) => std::str::from_utf8(payload).unwrap_or("").trim(),
        };

        let is_insert = sql.to_uppercase().starts_with("INSERT");
        if !is_insert {
            tracing::info!(sql, "query received");
        }

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
                if is_insert && rows_affected > 0 {
                    if let Some(s) = self.session.as_mut() {
                        s.had_writes = true;
                        s.mark_txn_write();
                    }
                }
                Ok(vec![build_ok_with_server_elapsed(
                    rows_affected,
                    server_start.elapsed(),
                )])
            }
            Ok(ExecResult::Query { schema, batches }) => {
                let msg = if use_binary {
                    build_binary_query_result(&schema, &batches)
                } else {
                    build_query_result(&schema, &batches)
                };
                Ok(vec![append_server_elapsed(msg, server_start.elapsed())])
            }
            Err(e) => {
                if !is_insert {
                    tracing::warn!(error = %e, sql, "query error");
                }
                Ok(vec![build_error(&e.to_string())])
            }
        }
    }

    fn handle_start_bulk_import(&mut self, sql: &str) -> Result<Vec<Message>> {
        let disable_wal =
            sql.contains("\"disable_wal\":true") || sql.contains("\"disable_wal\": true");

        self.engine.set_bulk_mode(true);

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
        let (db, disable_wal) = match &self.session {
            Some(s) => (s.database.clone(), s.disable_wal),
            None => return Ok(vec![build_error("not logged in")]),
        };

        if let Some(session) = &mut self.session {
            session.had_writes = true;
        }

        match self
            .engine
            .exec_bulk_insert_payload_with_wal_control(&db, payload, disable_wal)
        {
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
        let server_start = Instant::now();

        let session = match &mut self.session {
            Some(s) => s,
            None => return Ok(vec![build_error("not logged in")]),
        };
        session.touch();

        if payload.len() < 4 {
            return Ok(vec![build_error(
                "invalid execute message: missing statement ID",
            )]);
        }
        let stmt_id = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);

        let use_binary = session.binary_results;

        // Take prepared statement out of cache temporarily to allow mutation
        let mut stmt = match session.prepared_statements.remove(stmt_id) {
            Some(s) => s,
            None => {
                return Ok(vec![build_error(&format!(
                    "prepared statement {} not found",
                    stmt_id
                ))])
            }
        };

        let result = self.engine.execute_prepared(session, &mut stmt);

        // Put it back into cache
        session.prepared_statements.insert(stmt);

        match result {
            Ok(ExecResult::Ok { rows_affected }) => Ok(vec![build_ok_with_server_elapsed(
                rows_affected,
                server_start.elapsed(),
            )]),
            Ok(ExecResult::Query { schema, batches }) => {
                let msg = if use_binary {
                    build_binary_query_result(&schema, &batches)
                } else {
                    build_query_result(&schema, &batches)
                };
                Ok(vec![append_server_elapsed(msg, server_start.elapsed())])
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
        if session.in_txn() {
            return Ok(vec![build_error("transaction already in progress")]);
        }
        let txn_id = self.engine.alloc_txn_id();
        session.begin_txn(txn_id);
        Ok(vec![build_ok(txn_id)])
    }

    fn handle_commit_txn(&mut self) -> Result<Vec<Message>> {
        let session = match &mut self.session {
            Some(s) => s,
            None => return Ok(vec![build_error("not logged in")]),
        };
        if !session.in_txn() {
            return Ok(vec![build_error("no active transaction")]);
        }
        let txn_id = session.active_txn_id();
        let had_writes = session
            .active_txn
            .as_ref()
            .map(|t| t.had_writes)
            .unwrap_or(false);
        if had_writes {
            self.engine.batch_writer().commit_txn(txn_id)?;
        }
        session.commit_txn();
        Ok(vec![build_ok(0)])
    }

    fn handle_rollback_txn(&mut self) -> Result<Vec<Message>> {
        let session = match &mut self.session {
            Some(s) => s,
            None => return Ok(vec![build_error("not logged in")]),
        };
        if !session.in_txn() {
            return Ok(vec![build_error("no active transaction")]);
        }
        let txn_id = session.active_txn_id();
        let rolled_back = self.engine.batch_writer().rollback_txn(txn_id)?;
        session.rollback_txn();
        Ok(vec![build_ok(rolled_back)])
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
    Message::new(
        MSG_TYPE_OK,
        Bytes::copy_from_slice(&rows_affected.to_le_bytes()),
    )
}

fn build_ok_with_server_elapsed(rows_affected: u64, elapsed: Duration) -> Message {
    let mut buf = Vec::with_capacity(20);
    buf.extend_from_slice(&rows_affected.to_le_bytes());
    append_server_elapsed_trailer(&mut buf, elapsed);
    Message::new(MSG_TYPE_OK, Bytes::from(buf))
}

pub fn build_error(msg: &str) -> Message {
    Message::new(MSG_TYPE_ERROR, Bytes::from(msg.to_string()))
}

fn append_server_elapsed(msg: Message, elapsed: Duration) -> Message {
    let mut payload = Vec::with_capacity(msg.payload.len() + 12);
    payload.extend_from_slice(&msg.payload);
    append_server_elapsed_trailer(&mut payload, elapsed);
    Message::new(msg.msg_type, Bytes::from(payload))
}

fn append_server_elapsed_trailer(buf: &mut Vec<u8>, elapsed: Duration) {
    buf.extend_from_slice(b"MXTM");
    buf.extend_from_slice(&(elapsed.as_micros().min(u128::from(u64::MAX)) as u64).to_le_bytes());
}

fn build_query_result(
    schema: &crate::types::batch::Schema,
    batches: &[crate::types::batch::RecordBatch],
) -> Message {
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
                let col_def = &schema.columns[col_idx];
                match batch.column(col_idx) {
                    Some(col) => write_column_value_with_scale(&mut buf, col, row, col_def.scale),
                    None => buf.push(1),
                }
            }
        }
    }

    Message::new(MSG_TYPE_RESULT, Bytes::from(buf))
}

fn build_binary_query_result(
    schema: &crate::types::batch::Schema,
    batches: &[crate::types::batch::RecordBatch],
) -> Message {
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

fn write_column_value_with_scale(
    buf: &mut Vec<u8>,
    col: &crate::types::array::ColumnArray,
    row: usize,
    scale: i32,
) {
    if col.bitmap().is_null(row) {
        buf.push(1);
        return;
    }

    buf.push(0);
    match col {
        crate::types::array::ColumnArray::Bool { data, .. } => {
            buf.push(if data[row] != 0 { 1 } else { 0 })
        }
        crate::types::array::ColumnArray::Int8 { data, .. } => buf.push(data[row] as u8),
        crate::types::array::ColumnArray::Int16 { data, .. } => {
            buf.extend_from_slice(&data[row].to_le_bytes())
        }
        crate::types::array::ColumnArray::Int32 { data, .. } => {
            buf.extend_from_slice(&data[row].to_le_bytes())
        }
        crate::types::array::ColumnArray::Int64 { data, .. } => {
            buf.extend_from_slice(&data[row].to_le_bytes())
        }
        crate::types::array::ColumnArray::UInt8 { data, .. } => buf.push(data[row]),
        crate::types::array::ColumnArray::UInt16 { data, .. } => {
            buf.extend_from_slice(&data[row].to_le_bytes())
        }
        crate::types::array::ColumnArray::UInt32 { data, .. } => {
            buf.extend_from_slice(&data[row].to_le_bytes())
        }
        crate::types::array::ColumnArray::UInt64 { data, .. } => {
            buf.extend_from_slice(&data[row].to_le_bytes())
        }
        crate::types::array::ColumnArray::Float32 { data, .. } => {
            buf.extend_from_slice(&data[row].to_le_bytes())
        }
        crate::types::array::ColumnArray::Float64 { data, .. } => {
            buf.extend_from_slice(&data[row].to_le_bytes())
        }
        crate::types::array::ColumnArray::Decimal { data, .. } => {
            write_decimal_value_with_scale(buf, data[row], scale)
        }
        crate::types::array::ColumnArray::Date { data, .. } => {
            buf.extend_from_slice(&data[row].to_le_bytes())
        }
        crate::types::array::ColumnArray::Time { data, .. }
        | crate::types::array::ColumnArray::Timestamp { data, .. }
        | crate::types::array::ColumnArray::Interval { data, .. } => {
            buf.extend_from_slice(&data[row].to_le_bytes())
        }
        crate::types::array::ColumnArray::VarChar { data, .. } => {
            let bytes = data[row].as_bytes();
            buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(bytes);
        }
        crate::types::array::ColumnArray::Blob { data, .. } => {
            buf.extend_from_slice(&(data[row].len() as u32).to_le_bytes());
            buf.extend_from_slice(&data[row]);
        }
        crate::types::array::ColumnArray::Vector { data, .. } => {
            buf.extend_from_slice(&(data[row].len() as u32).to_le_bytes());
            for f in &data[row] {
                buf.extend_from_slice(&f.to_le_bytes());
            }
        }
    }
}

fn write_decimal_value_with_scale(buf: &mut Vec<u8>, value: i64, scale: i32) {
    if scale > 0 {
        let divisor = 10_i64.pow(scale as u32);
        let int_part = value / divisor;
        let frac_part = (value.abs() % divisor) as u32;
        let s = if frac_part == 0 {
            format!("{}", int_part)
        } else {
            let width = scale as usize;
            format!("{}.{:0width$}", int_part, frac_part)
        };
        buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
        buf.extend_from_slice(s.as_bytes());
    } else {
        buf.extend_from_slice(&value.to_le_bytes());
    }
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

#[cfg(test)]
mod tests {
    use super::*;

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
        write_len_prefixed_string(&mut payload, "pass");
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
    fn test_transaction_messages() {
        let dir = tempfile::TempDir::new().unwrap();
        let engine = Arc::new(Engine::new(dir.path(), "main").unwrap());
        let config = Arc::new(ServerConfig::default());
        let mut handler = Handler::new(config, engine);

        let mut payload = Vec::new();
        write_len_prefixed_string(&mut payload, "root");
        write_len_prefixed_string(&mut payload, "pass");
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
        write_len_prefixed_string(&mut payload, "pass");
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
        write_len_prefixed_string(&mut payload, "pass");
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
        write_len_prefixed_string(&mut payload, "pass");
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
}
