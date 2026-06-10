use bytes::Bytes;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::common::config::ServerConfig;
use crate::common::error::{TaibaiError, Result};
use crate::net::session::Session;
use crate::protocol::message::*;
use crate::protocol::types::DataType;
use crate::sql::execution::{Engine, ExecResult};
use crate::types::scalar::ScalarValue;

pub struct Handler {
    pub session: Option<Session>,
    config: Arc<ServerConfig>,
    engine: Arc<Engine>,
}

#[derive(Debug, Clone, Copy)]
struct ServerTimings {
    total: Duration,
    execute: Duration,
    encode: Duration,
}

impl Handler {
    pub fn new(config: Arc<ServerConfig>, engine: Arc<Engine>) -> Self {
        Self {
            session: None,
            config,
            engine,
        }
    }

    pub fn flush_on_disconnect(&mut self) {
        if let Some(ref mut session) = self.session {
            if session.bulk_import_mode {
                let remaining = self.engine.end_bulk_import();
                session.bulk_import_mode = false;
                session.disable_wal = false;
                tracing::info!(
                    remaining_bulk_sessions = remaining,
                    session_id = %session.session_id_string(),
                    "bulk import session ended on disconnect"
                );
            }
            if session.had_writes {
                if self.engine.is_bulk_mode() {
                    tracing::info!(
                        session_id = %session.session_id_string(),
                        active_bulk_sessions = self.engine.bulk_mode_session_count(),
                        "deferred disconnect flush while bulk import sessions remain active"
                    );
                } else {
                    match self.engine.flush_all_buffers() {
                        Ok(n) => {
                            tracing::info!(rows_flushed = n, session_id = %session.session_id_string(), "flushed buffers on disconnect")
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "failed to flush buffers on disconnect")
                        }
                    }
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
            MSG_TYPE_BATCH_EXECUTE => self.handle_batch_execute(&msg.payload),
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
        let (password, rest) = read_len_prefixed_string(rest)?;
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

        let auth_result = self
            .engine
            .auth_manager()
            .authenticate(username, password)?;
        if !auth_result {
            tracing::warn!(username, "login failed: invalid credentials");
            return Ok(vec![build_auth_fail("invalid username or password")]);
        }

        let is_locked = self.engine.auth_manager().is_locked(username);
        if is_locked {
            tracing::warn!(username, "login failed: account locked");
            return Ok(vec![build_auth_fail("account is locked")]);
        }

        if !self.engine.database_exists(&database) {
            tracing::warn!(username, database, "login failed: database does not exist");
            return Ok(vec![build_auth_fail(&format!(
                "database '{}' does not exist",
                database
            ))]);
        }

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

        let (database, rest) = match read_len_prefixed_string(payload) {
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
        let original_database = session.database.clone();
        let has_query_database = !database.is_empty();
        if has_query_database {
            if !self.engine.database_exists(database) {
                return Ok(vec![build_error(&format!(
                    "database '{}' does not exist",
                    database
                ))]);
            }
            session.database = database.to_string();
        }

        let execute_start = Instant::now();
        let result = self.engine.execute(session, sql);
        let execute_elapsed = execute_start.elapsed();
        if has_query_database && !sql.to_ascii_uppercase().starts_with("USE ") {
            session.database = original_database;
        }

        match result {
            Ok(ExecResult::Ok { rows_affected }) => {
                if is_insert && rows_affected > 0 {
                    if let Some(s) = self.session.as_mut() {
                        s.had_writes = true;
                        s.mark_txn_write();
                    }
                }
                let timings = ServerTimings {
                    total: server_start.elapsed(),
                    execute: execute_elapsed,
                    encode: Duration::ZERO,
                };
                Ok(vec![build_ok_with_server_timings(rows_affected, timings)])
            }
            Ok(ExecResult::Query { schema, batches }) => {
                let encode_start = Instant::now();
                let msg = if use_binary {
                    build_binary_query_result(&schema, &batches)
                } else {
                    build_query_result(&schema, &batches)
                };
                let timings = ServerTimings {
                    total: server_start.elapsed(),
                    execute: execute_elapsed,
                    encode: encode_start.elapsed(),
                };
                Ok(vec![append_server_timings(msg, timings)])
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
        let Some(session) = self.session.as_mut() else {
            return Ok(vec![build_error("not logged in")]);
        };

        let disable_wal =
            sql.contains("\"disable_wal\":true") || sql.contains("\"disable_wal\": true");

        let active_sessions = if session.bulk_import_mode {
            self.engine.bulk_mode_session_count()
        } else {
            self.engine.begin_bulk_import()
        };

        session.bulk_import_mode = true;
        session.disable_wal = disable_wal;

        tracing::info!(
            disable_wal,
            active_bulk_sessions = active_sessions,
            "bulk import mode started"
        );
        Ok(vec![build_ok(0)])
    }

    fn handle_stop_bulk_import(&mut self) -> Result<Vec<Message>> {
        let Some(session) = self.session.as_mut() else {
            return Ok(vec![build_error("not logged in")]);
        };

        let remaining_sessions = if session.bulk_import_mode {
            self.engine.end_bulk_import()
        } else {
            self.engine.bulk_mode_session_count()
        };

        let flushed = if remaining_sessions == 0 {
            self.engine.flush_all_buffers()?
        } else {
            0
        };

        session.bulk_import_mode = false;
        session.disable_wal = false;

        tracing::info!(
            rows_flushed = flushed,
            remaining_bulk_sessions = remaining_sessions,
            "bulk import mode stopped"
        );
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

        match self.engine.exec_bulk_insert_payload_with_options(
            &db,
            payload,
            disable_wal,
            self.session
                .as_ref()
                .map(|s| s.bulk_import_mode)
                .unwrap_or(false),
        ) {
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
                buf.extend_from_slice(&(stmt.param_count as u32).to_le_bytes());
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

        let mut offset = 4usize;
        let mut params: Vec<ScalarValue> = Vec::new();

        if payload.len() >= offset + 4 {
            let param_count = u32::from_le_bytes([
                payload[offset],
                payload[offset + 1],
                payload[offset + 2],
                payload[offset + 3],
            ]) as usize;
            offset += 4;

            for i in 0..param_count {
                if offset >= payload.len() {
                    return Ok(vec![build_error(&format!(
                        "truncated parameters: expected {} params, got {}",
                        param_count, i
                    ))]);
                }
                let type_tag = payload[offset];
                offset += 1;

                if type_tag == 0 {
                    params.push(ScalarValue::VarChar(None));
                    continue;
                }

                let dt = match DataType::from_u8(type_tag) {
                    Ok(dt) => dt,
                    Err(e) => {
                        return Ok(vec![build_error(&format!(
                            "invalid param type tag {}: {}",
                            type_tag, e
                        ))])
                    }
                };

                let (val, consumed) = match Self::decode_param_value(&payload[offset..], dt) {
                    Ok(v) => v,
                    Err(e) => {
                        return Ok(vec![build_error(&format!(
                            "param {} decode error: {}",
                            i, e
                        ))])
                    }
                };
                params.push(val);
                offset += consumed;
            }
        }

        let use_binary = session.binary_results;

        let mut stmt = match session.prepared_statements.remove(stmt_id) {
            Some(s) => s,
            None => {
                return Ok(vec![build_error(&format!(
                    "prepared statement {} not found",
                    stmt_id
                ))])
            }
        };

        stmt.params = params;

        let execute_start = Instant::now();
        let result = self.engine.execute_prepared(session, &mut stmt);
        let execute_elapsed = execute_start.elapsed();

        session.prepared_statements.insert(stmt);

        match result {
            Ok(ExecResult::Ok { rows_affected }) => {
                let timings = ServerTimings {
                    total: server_start.elapsed(),
                    execute: execute_elapsed,
                    encode: Duration::ZERO,
                };
                Ok(vec![build_ok_with_server_timings(rows_affected, timings)])
            }
            Ok(ExecResult::Query { schema, batches }) => {
                let encode_start = Instant::now();
                let msg = if use_binary {
                    build_binary_query_result(&schema, &batches)
                } else {
                    build_query_result(&schema, &batches)
                };
                let timings = ServerTimings {
                    total: server_start.elapsed(),
                    execute: execute_elapsed,
                    encode: encode_start.elapsed(),
                };
                Ok(vec![append_server_timings(msg, timings)])
            }
            Err(e) => Ok(vec![build_error(&e.to_string())]),
        }
    }

    fn decode_param_value(data: &[u8], dt: DataType) -> Result<(ScalarValue, usize)> {
        match dt {
            DataType::Bool => {
                if data.is_empty() {
                    return Err(TaibaiError::Protocol("no data for Bool param".into()));
                }
                Ok((ScalarValue::Bool(Some(data[0] != 0)), 1))
            }
            DataType::Int8 => {
                if data.is_empty() {
                    return Err(TaibaiError::Protocol("no data for Int8 param".into()));
                }
                Ok((ScalarValue::Int8(Some(data[0] as i8)), 1))
            }
            DataType::Int16 => {
                if data.len() < 2 {
                    return Err(TaibaiError::Protocol("no data for Int16 param".into()));
                }
                Ok((
                    ScalarValue::Int16(Some(i16::from_le_bytes([data[0], data[1]]))),
                    2,
                ))
            }
            DataType::Int32 => {
                if data.len() < 4 {
                    return Err(TaibaiError::Protocol("no data for Int32 param".into()));
                }
                let v = i32::from_le_bytes([data[0], data[1], data[2], data[3]]);
                Ok((ScalarValue::Int32(Some(v)), 4))
            }
            DataType::Int64 => {
                if data.len() < 8 {
                    return Err(TaibaiError::Protocol("no data for Int64 param".into()));
                }
                let v = i64::from_le_bytes([
                    data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                ]);
                Ok((ScalarValue::Int64(Some(v)), 8))
            }
            DataType::UInt8 => {
                if data.is_empty() {
                    return Err(TaibaiError::Protocol("no data for UInt8 param".into()));
                }
                Ok((ScalarValue::UInt8(Some(data[0])), 1))
            }
            DataType::UInt16 => {
                if data.len() < 2 {
                    return Err(TaibaiError::Protocol("no data for UInt16 param".into()));
                }
                Ok((
                    ScalarValue::UInt16(Some(u16::from_le_bytes([data[0], data[1]]))),
                    2,
                ))
            }
            DataType::UInt32 => {
                if data.len() < 4 {
                    return Err(TaibaiError::Protocol("no data for UInt32 param".into()));
                }
                let v = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
                Ok((ScalarValue::UInt32(Some(v)), 4))
            }
            DataType::UInt64 => {
                if data.len() < 8 {
                    return Err(TaibaiError::Protocol("no data for UInt64 param".into()));
                }
                let v = u64::from_le_bytes([
                    data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                ]);
                Ok((ScalarValue::UInt64(Some(v)), 8))
            }
            DataType::Float32 => {
                if data.len() < 4 {
                    return Err(TaibaiError::Protocol("no data for Float32 param".into()));
                }
                let v = f32::from_le_bytes([data[0], data[1], data[2], data[3]]);
                Ok((ScalarValue::Float32(Some(v)), 4))
            }
            DataType::Float64 => {
                if data.len() < 8 {
                    return Err(TaibaiError::Protocol("no data for Float64 param".into()));
                }
                let v = f64::from_le_bytes([
                    data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                ]);
                Ok((ScalarValue::Float64(Some(v)), 8))
            }
            DataType::Decimal => {
                if data.len() < 8 {
                    return Err(TaibaiError::Protocol("no data for Decimal param".into()));
                }
                let v = i64::from_le_bytes([
                    data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                ]);
                Ok((ScalarValue::Decimal(Some(v)), 8))
            }
            DataType::Date => {
                if data.len() < 4 {
                    return Err(TaibaiError::Protocol("no data for Date param".into()));
                }
                let v = i32::from_le_bytes([data[0], data[1], data[2], data[3]]);
                Ok((ScalarValue::Date(Some(v)), 4))
            }
            DataType::Time => {
                if data.len() < 8 {
                    return Err(TaibaiError::Protocol("no data for Time param".into()));
                }
                let v = i64::from_le_bytes([
                    data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                ]);
                Ok((ScalarValue::Time(Some(v)), 8))
            }
            DataType::Timestamp => {
                if data.len() < 8 {
                    return Err(TaibaiError::Protocol("no data for Timestamp param".into()));
                }
                let v = i64::from_le_bytes([
                    data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                ]);
                Ok((ScalarValue::Timestamp(Some(v)), 8))
            }
            DataType::Interval => {
                if data.len() < 8 {
                    return Err(TaibaiError::Protocol("no data for Interval param".into()));
                }
                let v = i64::from_le_bytes([
                    data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                ]);
                Ok((ScalarValue::Interval(Some(v)), 8))
            }
            DataType::VarChar => {
                if data.len() < 4 {
                    return Err(TaibaiError::Protocol("no data for VarChar param".into()));
                }
                let len = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
                if data.len() < 4 + len {
                    return Err(TaibaiError::Protocol("VarChar param data truncated".into()));
                }
                let s = std::str::from_utf8(&data[4..4 + len]).map_err(|e| {
                    TaibaiError::Protocol(format!("invalid UTF-8 in VarChar param: {}", e))
                })?;
                Ok((ScalarValue::VarChar(Some(s.to_string())), 4 + len))
            }
            DataType::Blob => {
                if data.len() < 4 {
                    return Err(TaibaiError::Protocol("no data for Blob param".into()));
                }
                let len = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
                if data.len() < 4 + len {
                    return Err(TaibaiError::Protocol("Blob param data truncated".into()));
                }
                Ok((ScalarValue::Blob(Some(data[4..4 + len].to_vec())), 4 + len))
            }
            DataType::Vector => {
                if data.len() < 4 {
                    return Err(TaibaiError::Protocol("no data for Vector param".into()));
                }
                let dim = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
                let total = 4 + dim * 4;
                if data.len() < total {
                    return Err(TaibaiError::Protocol("Vector param data truncated".into()));
                }
                let mut floats = Vec::with_capacity(dim);
                for i in 0..dim {
                    let off = 4 + i * 4;
                    floats.push(f32::from_le_bytes([
                        data[off],
                        data[off + 1],
                        data[off + 2],
                        data[off + 3],
                    ]));
                }
                Ok((ScalarValue::Vector(Some(floats)), total))
            }
        }
    }

    fn handle_batch_execute(&mut self, payload: &[u8]) -> Result<Vec<Message>> {
        let server_start = Instant::now();

        let session = match &mut self.session {
            Some(s) => s,
            None => return Ok(vec![build_error("not logged in")]),
        };
        session.touch();

        if payload.len() < 8 {
            return Ok(vec![build_error(
                "invalid batch execute message: missing statement ID or batch size",
            )]);
        }
        let stmt_id = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
        let batch_size =
            u32::from_le_bytes([payload[4], payload[5], payload[6], payload[7]]) as usize;

        let mut offset = 8usize;

        let mut stmt = match session.prepared_statements.remove(stmt_id) {
            Some(s) => s,
            None => {
                return Ok(vec![build_error(&format!(
                    "prepared statement {} not found",
                    stmt_id
                ))])
            }
        };

        let param_count = stmt.param_count;
        let mut total_rows_affected = 0u64;
        let mut had_writes = false;

        for row_idx in 0..batch_size {
            let mut params: Vec<ScalarValue> = Vec::with_capacity(param_count);

            for param_idx in 0..param_count {
                if offset >= payload.len() {
                    session.prepared_statements.insert(stmt);
                    return Ok(vec![build_error(&format!(
                        "truncated parameters: row {}, param {}",
                        row_idx, param_idx
                    ))]);
                }
                let type_tag = payload[offset];
                offset += 1;

                if type_tag == 0 {
                    params.push(ScalarValue::VarChar(None));
                    continue;
                }

                let dt = match DataType::from_u8(type_tag) {
                    Ok(dt) => dt,
                    Err(e) => {
                        session.prepared_statements.insert(stmt);
                        return Ok(vec![build_error(&format!(
                            "invalid param type tag {}: {}",
                            type_tag, e
                        ))]);
                    }
                };

                let (val, consumed) = match Self::decode_param_value(&payload[offset..], dt) {
                    Ok(v) => v,
                    Err(e) => {
                        session.prepared_statements.insert(stmt);
                        return Ok(vec![build_error(&format!(
                            "row {} param {} decode error: {}",
                            row_idx, param_idx, e
                        ))]);
                    }
                };
                params.push(val);
                offset += consumed;
            }

            stmt.params = params;

            let result = self.engine.execute_prepared(session, &mut stmt);
            match result {
                Ok(ExecResult::Ok { rows_affected }) => {
                    total_rows_affected += rows_affected;
                    if rows_affected > 0 {
                        had_writes = true;
                    }
                }
                Ok(ExecResult::Query { .. }) => {
                    session.prepared_statements.insert(stmt);
                    return Ok(vec![build_error(
                        "batch execute returned query result, expected Ok",
                    )]);
                }
                Err(e) => {
                    session.prepared_statements.insert(stmt);
                    return Ok(vec![build_error(&format!(
                        "row {} execute error: {}",
                        row_idx, e
                    ))]);
                }
            }
        }

        if had_writes {
            session.had_writes = true;
            session.mark_txn_write();
        }

        session.prepared_statements.insert(stmt);

        let timings = ServerTimings {
            total: server_start.elapsed(),
            execute: server_start.elapsed(),
            encode: Duration::ZERO,
        };
        Ok(vec![build_ok_with_server_timings(
            total_rows_affected,
            timings,
        )])
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
        self.engine.commit_session_txn(session)?;
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

fn build_auth_fail(reason: &str) -> Message {
    Message::new(MSG_TYPE_AUTH_FAIL, Bytes::from(reason.to_string()))
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

fn build_ok_with_server_timings(rows_affected: u64, timings: ServerTimings) -> Message {
    let mut buf = Vec::with_capacity(36);
    buf.extend_from_slice(&rows_affected.to_le_bytes());
    append_server_timing_trailer(&mut buf, timings);
    Message::new(MSG_TYPE_OK, Bytes::from(buf))
}

pub fn build_error(msg: &str) -> Message {
    Message::new(MSG_TYPE_ERROR, Bytes::from(msg.to_string()))
}

fn append_server_timings(msg: Message, timings: ServerTimings) -> Message {
    let mut payload = Vec::with_capacity(msg.payload.len() + 28);
    payload.extend_from_slice(&msg.payload);
    append_server_timing_trailer(&mut payload, timings);
    Message::new(msg.msg_type, Bytes::from(payload))
}

fn append_server_timing_trailer(buf: &mut Vec<u8>, timings: ServerTimings) {
    buf.extend_from_slice(b"MXTP");
    append_duration_micros(buf, timings.total);
    append_duration_micros(buf, timings.execute);
    append_duration_micros(buf, timings.encode);
}

fn append_duration_micros(buf: &mut Vec<u8>, elapsed: Duration) {
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
        buf.push(col.scale as u8);
        buf.push(col.precision as u8);
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
    let encoded = crate::protocol::result::encode_binary_result_batches(schema, batches);
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
        let divisor = 10_i128.pow(scale as u32);
        let sign = if value < 0 { "-" } else { "" };
        let abs = (value as i128).abs();
        let int_part = abs / divisor;
        let frac_part = abs % divisor;
        let s = if frac_part == 0 {
            format!("{}{}", sign, int_part)
        } else {
            let width = scale as usize;
            format!("{}{}.{:0width$}", sign, int_part, frac_part)
        };
        buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
        buf.extend_from_slice(s.as_bytes());
    } else {
        buf.extend_from_slice(&value.to_le_bytes());
    }
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
        write_len_prefixed_string(&mut payload, "taibai");
        write_len_prefixed_string(&mut payload, "main");

        let msg = Message::new(MSG_TYPE_LOGIN, Bytes::from(payload));
        let responses = handler.handle_message(msg).unwrap();
        assert_eq!(responses.len(), 1);
        assert_eq!(responses[0].msg_type, MSG_TYPE_AUTH_OK);
        assert!(handler.session.is_some());
        assert_eq!(handler.session.as_ref().unwrap().username, "root");
    }

    fn logged_in_handler(
        config: Arc<ServerConfig>,
        engine: Arc<Engine>,
        username: &str,
    ) -> Handler {
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
}
