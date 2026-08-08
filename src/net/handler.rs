use bytes::Bytes;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::common::config::ServerConfig;
use crate::common::error::{Result, TaibaiError};
use crate::net::session::Session;
use crate::protocol::message::*;
use crate::protocol::types::DataType;
use crate::sql::execution::{Engine, ExecResult};
use crate::sql::parser::ast::Statement;
use crate::sql::parser::parser::Parser;
use crate::types::scalar::ScalarValue;

pub struct Handler {
    pub session: Option<Session>,
    config: Arc<ServerConfig>,
    engine: Arc<Engine>,
    stream_sender: Option<tokio::sync::mpsc::Sender<Message>>,
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
            stream_sender: None,
        }
    }

    pub fn flush_on_disconnect(&mut self) {
        if let Some(ref mut session) = self.session {
            let mut remaining_bulk_sessions = self.engine.bulk_mode_session_count();
            if session.bulk_import_mode {
                remaining_bulk_sessions = self.engine.end_bulk_import();
                session.bulk_import_mode = false;
                session.disable_wal = false;
                tracing::info!(
                    remaining_bulk_sessions,
                    session_id = %session.session_id_string(),
                    "bulk import session ended on disconnect"
                );
            }
            session.had_writes = false;
            if remaining_bulk_sessions == 0 {
                match self.engine.flush_bulk_dirty_tables() {
                    Ok(n) if n > 0 => {
                        tracing::info!(rows_flushed = n, session_id = %session.session_id_string(), "flushed bulk-import dirty tables on disconnect")
                    }
                    Ok(_) => {}
                    Err(e) => {
                        tracing::warn!(error = %e, "failed to flush bulk-import dirty tables on disconnect")
                    }
                }
            }
        }
    }

    #[cfg(test)]
    pub fn handle_message(&mut self, msg: Message) -> Result<Vec<Message>> {
        self.handle_message_cancellable(msg, Arc::new(AtomicBool::new(false)), None)
    }

    pub fn handle_message_cancellable(
        &mut self,
        msg: Message,
        cancelled: Arc<AtomicBool>,
        stream_sender: Option<tokio::sync::mpsc::Sender<Message>>,
    ) -> Result<Vec<Message>> {
        self.stream_sender = stream_sender;
        let _cancellation = crate::common::query_cancel::QueryCancellationGuard::enter(cancelled);
        let result = match msg.msg_type {
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
        };
        self.stream_sender = None;
        result
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
        let streaming_results = rest.get(5).is_some_and(|&b| b != 0);

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
        session.streaming_results = streaming_results;
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

        let is_insert = sql
            .as_bytes()
            .get(..b"INSERT".len())
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(b"INSERT"));
        let should_log_query = should_log_query_sql(sql);
        if !is_insert {
            tracing::info!(sql, "query received");
        }

        let session = match &mut self.session {
            Some(s) => s,
            None => {
                log_query_if_needed(
                    should_log_query,
                    "",
                    database,
                    sql,
                    "error:not_logged_in",
                    server_start.elapsed(),
                );
                return Ok(vec![build_error("not logged in")]);
            }
        };
        let query_user = session.username.clone();
        let query_database = if database.is_empty() {
            session.database.clone()
        } else {
            database.to_string()
        };

        if sql.starts_with("START BULK IMPORT") {
            let result = self.handle_start_bulk_import(sql);
            return result;
        }
        if sql == "STOP BULK IMPORT" {
            let result = self.handle_stop_bulk_import();
            return result;
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
        if session.streaming_results {
            if let Some(sender) = self.stream_sender.clone() {
                let max_message_size = self.config.server.max_message_size;
                match self.engine.execute_streaming_query(session, sql, |batch| {
                    let payload = crate::protocol::result::encode_binary_result(&batch);
                    if payload.len() > max_message_size {
                        return Err(TaibaiError::ResourceBusy(format!(
                            "query result chunk exceeded maximum message size of {} MB",
                            max_message_size / (1024 * 1024)
                        )));
                    }
                    sender
                        .blocking_send(Message::new(MSG_TYPE_RESULT_CHUNK, Bytes::from(payload)))
                        .map_err(|_| {
                            TaibaiError::Network("client disconnected during query stream".into())
                        })
                }) {
                    Ok(Some(_)) => {
                        if has_query_database {
                            session.database = original_database;
                        }
                        let timings = ServerTimings {
                            total: server_start.elapsed(),
                            execute: execute_start.elapsed(),
                            encode: Duration::ZERO,
                        };
                        return Ok(vec![append_server_timings(
                            Message::empty(MSG_TYPE_RESULT_END),
                            timings,
                        )]);
                    }
                    Ok(None) => {}
                    Err(error) => {
                        if has_query_database {
                            session.database = original_database;
                        }
                        return Ok(vec![build_error(&error.to_string())]);
                    }
                }
            }
        }
        let result = self.engine.execute(session, sql);
        let execute_elapsed = execute_start.elapsed();
        let is_use = sql
            .as_bytes()
            .get(..b"USE ".len())
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(b"USE "));
        if has_query_database && !is_use {
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
                log_query_if_needed(
                    should_log_query,
                    &query_user,
                    &query_database,
                    sql,
                    "ok",
                    server_start.elapsed(),
                );
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
                let msg = match self.validate_query_response(msg) {
                    Ok(msg) => msg,
                    Err(error) => return Ok(vec![build_error(&error.to_string())]),
                };
                log_query_if_needed(
                    should_log_query,
                    &query_user,
                    &query_database,
                    sql,
                    "ok",
                    server_start.elapsed(),
                );
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
                let status = format!("error:{}", e);
                log_query_if_needed(
                    should_log_query,
                    &query_user,
                    &query_database,
                    sql,
                    &status,
                    server_start.elapsed(),
                );
                Ok(vec![build_error(&e.to_string())])
            }
        }
    }

    fn validate_query_response(&self, msg: Message) -> Result<Message> {
        let max_bytes = self.config.server.max_message_size.min(u32::MAX as usize);
        let payload_limit = max_bytes.saturating_sub(28);
        if msg.payload.len() > payload_limit {
            return Err(TaibaiError::ResourceBusy(format!(
                "query response exceeded maximum message size of {} MB; add filters or LIMIT",
                max_bytes / (1024 * 1024)
            )));
        }
        Ok(msg)
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

        // The reference count has already been released. Keep the connection
        // state consistent even if flushing a dirty table reports an error;
        // the dirty-table entry remains queued for a later retry.
        session.bulk_import_mode = false;
        session.disable_wal = false;

        let flushed = if remaining_sessions == 0 {
            self.engine.flush_bulk_dirty_tables()?
        } else {
            0
        };

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
                if let Statement::Insert { table, columns, .. } | Statement::Replace { table, columns, .. } = &stmt.stmt {
                    let database = table.effective_database(&session.database);
                    if let Some(meta) = self.engine.get_table_meta(database, &table.name) {
                        for param_index in 0..stmt.param_count {
                            let column = columns.get(param_index).and_then(|name| {
                                meta.columns.iter().find(|column| column.name.eq_ignore_ascii_case(name))
                            });
                            buf.push(column.map(|column| column.data_type as u8).unwrap_or(0));
                            buf.push(column.map(|column| column.scale as u8).unwrap_or(0));
                        }
                    }
                }
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
                let msg = match self.validate_query_response(msg) {
                    Ok(msg) => msg,
                    Err(error) => return Ok(vec![build_error(&error.to_string())]),
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
        let mut parameter_batch = Vec::with_capacity(batch_size);

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

            parameter_batch.push(params);
        }

        let result = if matches!(
            stmt.stmt,
            Statement::Insert { .. } | Statement::Replace { .. }
        ) {
            self.engine
                .execute_prepared_write_batch(session, &stmt, &parameter_batch)
        } else {
            let mut total_rows_affected = 0u64;
            let mut error = None;
            for (row_idx, params) in parameter_batch.into_iter().enumerate() {
                stmt.params = params;
                match self.engine.execute_prepared(session, &mut stmt) {
                    Ok(ExecResult::Ok { rows_affected }) => {
                        total_rows_affected += rows_affected;
                    }
                    Ok(ExecResult::Query { .. }) => {
                        error = Some(TaibaiError::Sql(
                            "batch execute returned query result, expected Ok".into(),
                        ));
                        break;
                    }
                    Err(err) => {
                        error = Some(TaibaiError::Sql(format!(
                            "row {} execute error: {}",
                            row_idx, err
                        )));
                        break;
                    }
                }
            }
            match error {
                Some(error) => Err(error),
                None => Ok(ExecResult::Ok {
                    rows_affected: total_rows_affected,
                }),
            }
        };

        let total_rows_affected = match result {
            Ok(ExecResult::Ok { rows_affected }) => rows_affected,
            Ok(ExecResult::Query { .. }) => {
                session.prepared_statements.insert(stmt);
                return Ok(vec![build_error(
                    "batch execute returned query result, expected Ok",
                )]);
            }
            Err(error) => {
                session.prepared_statements.insert(stmt);
                return Ok(vec![build_error(&error.to_string())]);
            }
        };

        if total_rows_affected > 0 {
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

fn log_query_if_needed(
    should_log: bool,
    user: &str,
    database: &str,
    sql: &str,
    status: &str,
    elapsed: Duration,
) {
    if should_log {
        crate::common::query_log::log_query(user, database, sql, status, elapsed);
    }
}

fn should_log_query_sql(sql: &str) -> bool {
    // INSERT statements can be megabytes long. Do not parse every statement a
    // second time merely to decide whether it produces a result set.
    if !starts_like_query(sql) {
        return false;
    }
    match Parser::parse(sql) {
        Ok(stmts) => stmts.first().is_some_and(statement_returns_rows),
        Err(_) => starts_like_query(sql),
    }
}

fn statement_returns_rows(stmt: &Statement) -> bool {
    matches!(
        stmt,
        Statement::Select { .. }
            | Statement::Union { .. }
            | Statement::ShowTables { .. }
            | Statement::ShowDatabases
            | Statement::ShowColumns { .. }
            | Statement::ShowIndexes { .. }
            | Statement::ShowCreateTable { .. }
            | Statement::ShowUsers
            | Statement::ShowRoles
            | Statement::ShowGrants { .. }
            | Statement::ShowPartitions { .. }
            | Statement::ShowStats
            | Statement::ShowEngines
            | Statement::ShowProcesslist { .. }
            | Statement::Explain { .. }
    )
}

fn starts_like_query(sql: &str) -> bool {
    let trimmed = sql.trim_start();
    let first = trimmed
        .split(|ch: char| ch.is_ascii_whitespace() || ch == ';')
        .next()
        .unwrap_or_default();
    ["SELECT", "SHOW", "EXPLAIN", "DESCRIBE", "DESC"]
        .iter()
        .any(|keyword| first.eq_ignore_ascii_case(keyword))
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
#[path = "handler_tests.rs"]
mod tests;
