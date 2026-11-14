use bytes::Bytes;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::common::config::ServerConfig;
use crate::common::error::{Result, TaibaiError};
use crate::net::pubsub_session::{
    confirmed_message, decode_subscribe, decode_table_and_sub, decode_table_sub_and_seq,
    frame_to_message, spawn_delivery,
};
use crate::net::session::Session;
use crate::protocol::message::*;
use crate::protocol::types::DataType;
use crate::pubsub::ConnHandle;
use crate::sql::execution::{Engine, ExecResult};
use crate::sql::parser::ast::Statement;
use crate::sql::parser::parser::Parser;
use crate::storage::catalog::auth::{PermissionObjectType, PermissionType};
use crate::storage::core::pubsub::{PublishedFrame, SubscribeMode, SubscribeRequest};
use crate::types::batch::RecordBatch;
use crate::types::scalar::ScalarValue;

/// Park interval between live-fan-out waits in the replication push bridge.
/// Each wake doubles as a disconnect probe: an idle primary never produces an
/// event whose send would fail, so without the probe a vanished standby left
/// its bridge thread (stack, 4096-slot channel and registered session) parked
/// in `recv_timeout` forever — one leaked set per standby reconnect.
const REPL_PUSH_PARK_TIMEOUT: Duration = Duration::from_secs(30);

/// Cap on out-of-order events buffered while the bridge waits for the missing
/// seq. A permanent gap (a session the ring already skipped) otherwise grows
/// the buffer by one entry per subsequent event; past the cap the bridge
/// drops the session and the standby reconnects + re-pulls history from its
/// durably applied seq.
const REPL_PUSH_MAX_PENDING: usize = 8192;

/// Bridge thread of `REPL_HELLO`: replays ring history first, then streams
/// live fan-out, both funneled into the connection's push queue in strict seq
/// order. `sink_tx` must already be registered on the `RepLog`. The bridge
/// exits — unregistering the session — when the push queue rejects (client
/// gone), the push channel is observed closed at a park timeout, the session
/// is dropped by the ring, or the pending buffer exceeds
/// [`REPL_PUSH_MAX_PENDING`]. Returns the detached thread handle so tests can
/// join it.
fn spawn_replication_push_bridge(
    rep_log: Arc<crate::storage::replication::RepLog>,
    push_tx: tokio::sync::mpsc::Sender<Message>,
    last_seq: u64,
    history: Vec<(u64, Arc<crate::storage::replication::ReplicationEvent>)>,
    park_timeout: Duration,
) -> std::thread::JoinHandle<()> {
    let (sink_tx, sink_rx) = crossbeam_channel::bounded::<(
        u64,
        Arc<crate::storage::replication::ReplicationEvent>,
    )>(4096);
    // Register the bridge's own sink: `publish`'s fan-out drops sessions that
    // stall past its send timeout, which disconnects `sink_rx` below.
    rep_log.register_session(&sink_tx);
    std::thread::Builder::new()
        .name("taibai-repl-push".into())
        .spawn(move || {
            let push = |seq: u64, event: &crate::storage::replication::ReplicationEvent| {
                let mut payload_bytes = Vec::with_capacity(8 + 4 + 128);
                payload_bytes.extend_from_slice(&seq.to_le_bytes());
                let encoded = event.encode();
                payload_bytes.extend_from_slice(&(encoded.len() as u32).to_le_bytes());
                payload_bytes.extend_from_slice(&encoded);
                push_tx.blocking_send(Message {
                    msg_type: MSG_TYPE_REPL_EVENT,
                    payload: bytes::Bytes::from(payload_bytes),
                })
            };
            let abandon = || rep_log.remove_session(&sink_tx);
            let mut next = last_seq + 1;
            let mut pending: std::collections::BTreeMap<
                u64,
                Arc<crate::storage::replication::ReplicationEvent>,
            > = std::collections::BTreeMap::new();
            for (seq, event) in history {
                if seq < next {
                    continue;
                }
                pending.insert(seq, event);
                if pending.len() > REPL_PUSH_MAX_PENDING {
                    abandon();
                    return;
                }
                while let Some(event) = pending.remove(&next) {
                    if push(next, &event).is_err() {
                        abandon();
                        return;
                    }
                    next += 1;
                }
            }
            loop {
                while let Some(event) = pending.remove(&next) {
                    if push(next, &event).is_err() {
                        abandon();
                        return;
                    }
                    next += 1;
                }
                match sink_rx.recv_timeout(park_timeout) {
                    Ok((seq, event)) => {
                        if seq < next {
                            continue;
                        }
                        pending.insert(seq, event);
                        if pending.len() > REPL_PUSH_MAX_PENDING {
                            abandon();
                            return;
                        }
                    }
                    Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                        // The client can vanish while the primary is idle: no
                        // event ever triggers a failing send, so probe the
                        // push channel here instead of parking forever.
                        if push_tx.is_closed() {
                            abandon();
                            return;
                        }
                        // The fan-out may also have dropped this session for
                        // stalling while the connection is still open. The
                        // bridge holds its own `sink_tx`, so that drop does
                        // not surface as `Disconnected` — probe it directly.
                        if !rep_log.has_session(&sink_tx) {
                            abandon();
                            return;
                        }
                    }
                    Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
                }
            }
            rep_log.remove_session(&sink_tx);
        })
        .expect("failed to spawn replication push thread")
}

pub struct Handler {
    pub session: Option<Session>,
    config: Arc<ServerConfig>,
    engine: Arc<Engine>,
    stream_sender: Option<tokio::sync::mpsc::Sender<Message>>,
    /// Connection push queue used by the pubsub fan-out thread. Set once by
    /// the connection task after the handler is created.
    push_channel: Option<tokio::sync::mpsc::Sender<Message>>,
    /// Delivery handle grouping every subscription made on this connection.
    pubsub_conn: Option<Arc<ConnHandle>>,
}

#[derive(Debug, Clone, Copy)]
struct ServerTimings {
    total: Duration,
    execute: Duration,
    encode: Duration,
}

impl Drop for Handler {
    fn drop(&mut self) {
        // Safety net for every path that abandons a handler without the
        // normal `flush_on_disconnect` cleanup (decode errors, push send
        // failures, aborted request workers): release the connection's
        // pubsub subscriptions so they cannot outlive the socket. The normal
        // path already `take()`s the handle first, making this a no-op there.
        if let Some(conn) = self.pubsub_conn.take() {
            self.engine.pubsub().drop_connection(&conn);
            tracing::debug!(
                conn_id = conn.conn_id(),
                "pubsub subscriptions released on handler drop"
            );
        }
        if let Some(session) = &self.session {
            let removed = self
                .engine
                .pubsub_manager()
                .unsubscribe_session(&session.id);
            if removed > 0 {
                tracing::debug!(
                    session_id = %session.id,
                    removed,
                    "reliable pubsub subscriptions released on handler drop"
                );
            }
        }
        // A client that vanishes mid-transaction must not leave its
        // transactional memtables and mutation pins behind forever: pins
        // block compaction/flush for the whole table, and transactional
        // memtables are only removable via commit/rollback.
        self.rollback_open_txn_on_disconnect();
        // Cleanup can time out waiting for a request permit or unwind. Do not
        // leave the global bulk-mode count elevated after this session dies.
        if let Some(session) = self.session.as_mut() {
            if session.bulk_import_mode {
                session.bulk_import_mode = false;
                self.engine.end_bulk_import();
            }
        }
    }
}

impl Handler {
    pub fn new(config: Arc<ServerConfig>, engine: Arc<Engine>) -> Self {
        Self {
            session: None,
            config,
            engine,
            stream_sender: None,
            push_channel: None,
            pubsub_conn: None,
        }
    }

    /// Install the per-connection push queue. The server creates it before
    /// the first request is processed; subscriptions made later deliver
    /// through it.
    pub fn set_push_channel(&mut self, push_tx: tokio::sync::mpsc::Sender<Message>) {
        self.push_channel = Some(push_tx);
    }

    /// Whether this connection currently holds an open bulk-import session.
    /// The connection loop widens the idle window while this is true: bulk
    /// import clients go silent for long stretches and must not be idle-
    /// disconnected mid-import.
    pub(crate) fn is_bulk_import_mode(&self) -> bool {
        self.session.as_ref().is_some_and(|s| s.bulk_import_mode)
    }

    pub fn flush_on_disconnect(&mut self) {
        // Same rationale as the Drop safety net below; the normal path gets
        // the tidy, ordered rollback before any bulk-import flush work.
        self.rollback_open_txn_on_disconnect();
        if let Some(conn) = self.pubsub_conn.take() {
            self.engine.pubsub().drop_connection(&conn);
        }
        if let Some(ref session) = self.session {
            let removed = self
                .engine
                .pubsub_manager()
                .unsubscribe_session(&session.id);
            if removed > 0 {
                tracing::debug!(
                    session_id = %session.id,
                    removed,
                    "reliable pubsub subscriptions released on disconnect"
                );
            }
        }
        if let Some(ref mut session) = self.session {
            let mut remaining_bulk_sessions = self.engine.bulk_mode_session_count();
            if session.bulk_import_mode {
                remaining_bulk_sessions = self.engine.end_bulk_import();
                session.bulk_import_mode = false;
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

    /// Rolls back any open transaction, tolerating (and logging) failures so
    /// it is safe from both the normal disconnect path and `Drop` — including
    /// `Drop` during unwind, where a second panic would abort the process.
    fn rollback_open_txn_on_disconnect(&mut self) {
        let Some(session) = self.session.as_mut() else {
            return;
        };
        if !session.in_txn() {
            return;
        }
        let txn_id = session.active_txn_id();
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.engine.rollback_session_txn(session)
        }));
        match outcome {
            Ok(Ok(rows)) => tracing::info!(
                txn_id,
                rows_rolled_back = rows,
                "open transaction rolled back on disconnect"
            ),
            Ok(Err(error)) => tracing::warn!(
                txn_id,
                error = %error,
                "failed to roll back open transaction on disconnect"
            ),
            Err(_) => {
                tracing::warn!(
                    txn_id,
                    "panicked while rolling back open transaction on disconnect; \
                     forcing txn mutation cleanup"
                );
                if let Err(cleanup_err) = std::panic::catch_unwind(
                    std::panic::AssertUnwindSafe(|| {
                        let _ = self.engine.batch_writer().rollback_txn(txn_id);
                        self.engine
                            .index_manager()
                            .discard_txn_published_entries_at_rollback(txn_id);
                    }),
                ) {
                    tracing::error!(
                        txn_id,
                        "cleanup after rollback panic also panicked: {:?}",
                        cleanup_err
                    );
                }
            },
        }
    }

    pub fn handle_message_cancellable(
        &mut self,
        msg: Message,
        context: Arc<crate::common::query_cancel::QueryContext>,
        stream_sender: Option<tokio::sync::mpsc::Sender<Message>>,
    ) -> Result<Vec<Message>> {
        self.stream_sender = stream_sender;
        // Commit/rollback/flush and bulk-import chunks mutate durable state in
        // steps that cannot be rolled back cheaply, so they run to completion
        // even when the client disconnects mid-request.
        if matches!(
            msg.msg_type,
            MSG_TYPE_COMMIT_TXN
                | MSG_TYPE_ROLLBACK_TXN
                | MSG_TYPE_FLUSH
                | MSG_TYPE_BULK_INSERT_COLUMNAR
        ) {
            context.enter_phase(crate::common::query_cancel::QueryPhase::Committing);
        } else if context.phase() == crate::common::query_cancel::QueryPhase::Planning {
            context.enter_phase(crate::common::query_cancel::QueryPhase::Executing);
        }
        let _cancellation = crate::common::query_cancel::QueryCancellationGuard::enter(context);
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
            MSG_TYPE_REPL_HELLO => self.handle_repl_hello(&msg.payload),
            MSG_TYPE_REPL_SEGMENT_FETCH => self.handle_repl_segment_fetch(&msg.payload),
            MSG_TYPE_REPL_ACK => self.handle_repl_ack(&msg.payload),

            MSG_TYPE_PREPARE => self.handle_prepare(&msg.payload),
            MSG_TYPE_EXECUTE => self.handle_execute(&msg.payload),
            MSG_TYPE_BATCH_EXECUTE => self.handle_batch_execute(&msg.payload),
            MSG_TYPE_CLOSE_STMT => self.handle_close_stmt(&msg.payload),

            MSG_TYPE_BEGIN_TXN => self.handle_begin_txn(),
            MSG_TYPE_COMMIT_TXN => self.handle_commit_txn(),
            MSG_TYPE_ROLLBACK_TXN => self.handle_rollback_txn(),

            MSG_TYPE_AUTH_FAIL => Ok(vec![build_error("authentication failed")]),

            // 0x40 is a query-execution alias (see message.rs); there are no
            // cursor semantics to open, so the payload runs as a plain query.
            MSG_TYPE_CURSOR => self.handle_cursor_open(&msg.payload),

            MSG_TYPE_SUBSCRIBE => self.handle_subscribe(&msg.payload),
            MSG_TYPE_UNSUBSCRIBE => self.handle_unsubscribe(&msg.payload),

            MSG_TYPE_SUBSCRIBE_V2 => self.handle_subscribe_v2(&msg.payload),
            MSG_TYPE_UNSUBSCRIBE_V2 => self.handle_unsubscribe_v2(&msg.payload),
            MSG_TYPE_SUB_ACK => self.handle_sub_ack(&msg.payload),

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

    /// List the request in the engine's running-query registry so it is
    /// visible to `SHOW PROCESSLIST` and stoppable via `KILL QUERY`. Called
    /// by the connection task before the request queues, so queued work is
    /// killable too. Requests before login (or lightweight ones) are not
    /// registered; dropping the returned guard unregisters.
    pub(crate) fn register_running_query(
        &self,
        context: &Arc<crate::common::query_cancel::QueryContext>,
        msg: &Message,
    ) -> Option<crate::sql::execution::query_registry::QueryRegistration> {
        if !is_heavy_request(msg.msg_type) {
            return None;
        }
        let session = self.session.as_ref()?;
        let sql = if msg.msg_type == MSG_TYPE_QUERY {
            message_sql_snippet(&msg.payload)
        } else {
            String::new()
        };
        Some(self.engine.query_registry().register(
            Arc::clone(context),
            session.session_id_string(),
            session.username.clone(),
            session.database.clone(),
            &sql,
        ))
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

        if rest.len() != 5 {
            return Err(TaibaiError::Protocol("invalid login capabilities".into()));
        }
        let timezone_offset_hours = i32::from_le_bytes(rest[0..4].try_into().unwrap());
        let streaming_results = rest[4] != 0;

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

        if self.session.is_some() {
            tracing::warn!(
                username,
                "duplicate login on an authenticated connection rejected to preserve existing session state (prepared statements, active transaction)"
            );
            return Ok(vec![build_auth_fail(
                "connection already logged in; reuse the established session or open a new connection",
            )]);
        }

        let mut session = Session::new(username.to_string(), database);
        session.streaming_results = streaming_results;
        session.timezone_offset_hours = timezone_offset_hours;
        let session_id = session.session_id_string();
        tracing::info!(username, database = %session.database, session_id = %session_id, timezone_offset_hours, "login success");
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
        if !is_insert && !is_heartbeat_query(sql) {
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
        match parse_streaming_results_set(sql) {
            StreamingResultsSet::NotASetStatement => {}
            StreamingResultsSet::Enable => {
                session.streaming_results = true;
                return Ok(vec![build_ok(0)]);
            }
            StreamingResultsSet::Disable => {
                session.streaming_results = false;
                return Ok(vec![build_ok(0)]);
            }
            StreamingResultsSet::Invalid => {
                return Ok(vec![build_error(
                    "unsupported SET statement; usage: SET streaming_results = {true|false|on|off|1|0}",
                )]);
            }
        }

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
                // Half the frame cap per chunk: encoded bytes can exceed the
                // pre-encode estimate, and each chunk carries its own copy of
                // the schema header.
                let chunk_budget = max_message_size / 2;
                // Streaming contract: the schema travels in the chunks, and
                // RESULT_END carries only timings. A result set with zero rows
                // therefore still emits one zero-row chunk, otherwise the
                // client would have no way to learn the column list (a
                // zero-row SELECT would come back as zero columns).
                let mut chunks_sent = false;
                match self.engine.execute_streaming_query(session, sql, |batch| {
                    for part in split_batch_for_chunks(batch, chunk_budget)? {
                        let payload = crate::protocol::result::encode_binary_result(&part);
                        if payload.len() > max_message_size {
                            return Err(TaibaiError::ResourceBusy(format!(
                                "query result chunk exceeded maximum message size of {} MB",
                                max_message_size / (1024 * 1024)
                            )));
                        }
                        sender
                            .blocking_send(Message::new(
                                MSG_TYPE_RESULT_CHUNK,
                                Bytes::from(payload),
                            ))
                            .map_err(|_| {
                                TaibaiError::Network(
                                    "client disconnected during query stream".into(),
                                )
                            })?;
                        chunks_sent = true;
                    }
                    Ok(())
                }) {
                    Ok(Some(schema)) => {
                        if has_query_database {
                            session.database = original_database;
                        }
                        if !chunks_sent && schema.num_columns() > 0 {
                            let payload =
                                crate::protocol::result::encode_binary_result_batches(&schema, &[]);
                            sender
                                .blocking_send(Message::new(
                                    MSG_TYPE_RESULT_CHUNK,
                                    Bytes::from(payload),
                                ))
                                .map_err(|_| {
                                    TaibaiError::Network(
                                        "client disconnected during query stream".into(),
                                    )
                                })?;
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
                        return Ok(vec![build_query_error(&error)]);
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
                let encoded = crate::protocol::result::encode_binary_result_batches(&schema, &batches);
                drop(batches);
                let timings = ServerTimings {
                    total: server_start.elapsed(),
                    execute: execute_elapsed,
                    encode: encode_start.elapsed(),
                };
                let mut payload = encoded;
                append_server_timing_trailer(&mut payload, timings);
                let msg = Message::new(MSG_TYPE_BINARY_QUERY_RESULT, Bytes::from(payload));
                let msg = match self.validate_query_response(msg) {
                    Ok(msg) => msg,
                    Err(error) => return Ok(vec![build_query_error(&error)]),
                };
                log_query_if_needed(
                    should_log_query,
                    &query_user,
                    &query_database,
                    sql,
                    "ok",
                    server_start.elapsed(),
                );
                Ok(vec![msg])
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
                Ok(vec![build_query_error(&e)])
            }
        }
    }

    fn validate_query_response(&self, msg: Message) -> Result<Message> {
        let max_bytes = self.config.server.max_message_size.min(u32::MAX as usize);
        let payload_limit = max_bytes.saturating_sub(28);
        if msg.payload.len() > payload_limit {
            return Err(TaibaiError::ResourceBusy(format!(
                "query response exceeded maximum message size of {} MB; add filters or LIMIT, or SET streaming_results = true to stream the result in chunks",
                max_bytes / (1024 * 1024)
            )));
        }
        Ok(msg)
    }

    fn handle_start_bulk_import(&mut self, _sql: &str) -> Result<Vec<Message>> {
        let Some(session) = self.session.as_mut() else {
            return Ok(vec![build_error("not logged in")]);
        };

        // Entering bulk mode globally defers unique-index compaction and
        // catalog flushes: require an INSERT grant on the current database so
        // read-only accounts cannot degrade the server for everyone else.
        // Per-table writes are permission-checked again in the bulk handler.
        if !self.engine.auth_manager().check_permission(
            &session.username,
            PermissionType::Insert,
            PermissionObjectType::Database,
            &session.database,
        ) {
            return Ok(vec![build_error(&format!(
                "access denied for user '{}' to {:?} on {:?} '{}'",
                session.username,
                PermissionType::Insert,
                PermissionObjectType::Database,
                session.database
            ))]);
        }

        let active_sessions = if session.bulk_import_mode {
            self.engine.bulk_mode_session_count()
        } else {
            self.engine.begin_bulk_import()
        };

        session.bulk_import_mode = true;

        tracing::info!(
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
        let db = match &self.session {
            Some(s) => s.database.clone(),
            None => return Ok(vec![build_error("not logged in")]),
        };

        if let Some(session) = &mut self.session {
            session.had_writes = true;
        }

        // Covers the whole engine call chain (payload decode → constraint
        // checks → column compression → locks → seal). The segment-manager
        // WARN only sees its own stage; anything slow that hides above it
        // (e.g. zstd at a high table COMPRESSION_LEVEL) surfaces here.
        const SLOW_BULK_INSERT_REQUEST_WARN: Duration = Duration::from_secs(10);
        let started = Instant::now();
        let outcome = self.engine.exec_bulk_insert_payload_as(
            &db,
            payload,
            self.session
                .as_ref()
                .map(|s| s.bulk_import_mode)
                .unwrap_or(false),
            self.session.as_ref().map(|s| s.username.as_str()),
        );
        let elapsed = started.elapsed();
        if elapsed >= SLOW_BULK_INSERT_REQUEST_WARN {
            tracing::warn!(
                db = %db,
                payload_len = payload.len(),
                elapsed_ms = elapsed.as_millis() as u64,
                "slow bulk insert request: check the table COMPRESSION_LEVEL (zstd 19 is single-digit MB/s per core) and the segment-manager seal WARN"
            );
        }
        match outcome {
            Ok(row_count) => Ok(vec![build_ok(row_count)]),
            Err(e) => {
                tracing::warn!(db = %db, session_id = ?self.session.as_ref().map(|s| &s.id), error = %e, "bulk insert error");
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
                if let Statement::Insert { table, columns, .. }
                | Statement::Replace { table, columns, .. } = &stmt.stmt
                {
                    let database = table.effective_database(&session.database);
                    if let Some(meta) = self.engine.get_table_meta(database, &table.name) {
                        for param_index in 0..stmt.param_count {
                            let column = if columns.is_empty() {
                                meta.columns.get(param_index)
                            } else {
                                columns.get(param_index).and_then(|name| {
                                    meta.columns
                                        .iter()
                                        .find(|column| column.name.eq_ignore_ascii_case(name))
                                })
                            };
                            buf.push(column.map(|column| column.data_type as u8).unwrap_or(0));
                            buf.push(column.map(|column| column.scale as u8).unwrap_or(0));
                        }
                    }
                }
                if let Some((_, param_types)) =
                    Self::current_insert_param_types(&self.engine, session, &stmt)
                {
                    if let Some(cached) = session.prepared_statements.get_mut(stmt.id) {
                        cached.param_types = param_types;
                    }
                }
                Ok(vec![Message::new(MSG_TYPE_PREPARE, Bytes::from(buf))])
            }
            Err(e) => Ok(vec![build_error(&e.to_string())]),
        }
    }

    /// Take the prepared statement `stmt_id` out of the session cache, or
    /// produce the protocol error response when the id is unknown (with a
    /// diagnostic warn — usually a protocol/version mismatch or a stale
    /// connection). The statement is removed up front so a failed execute
    /// cannot double-run it; successful paths re-insert it.
    fn take_prepared_statement(
        session: &mut Session,
        stmt_id: u32,
        context: &str,
    ) -> std::result::Result<crate::sql::execution::prepared::PreparedStatement, Vec<Message>> {
        match session.prepared_statements.remove(stmt_id) {
            Some(stmt) => Ok(stmt),
            None => {
                tracing::warn!(
                    stmt_id,
                    live_statements = session.prepared_statements.len(),
                    next_unissued_id = session.prepared_statements.next_id(),
                    "prepared statement lookup failed on {context}; client may reference an unknown id (protocol/version mismatch or stale connection)"
                );
                Err(vec![build_error(&format!(
                    "prepared statement {} not found (session has {} prepared statement(s); ids start at 1, next unissued id is {})",
                    stmt_id,
                    session.prepared_statements.len(),
                    session.prepared_statements.next_id()
                ))])
            }
        }
    }

    /// Reject a prepared statement whose target table's parameter types no
    /// longer match the PREPARE-time signature. `Ok(stmt)` hands the
    /// statement back unchanged; `Err(messages)` carries the rejection
    /// response (the statement is re-inserted into the session cache).
    fn reject_if_schema_changed(
        engine: &Engine,
        session: &mut Session,
        stmt: crate::sql::execution::prepared::PreparedStatement,
        stmt_id: u32,
    ) -> std::result::Result<crate::sql::execution::prepared::PreparedStatement, Vec<Message>> {
        if let Some((_, current_types)) = Self::current_insert_param_types(engine, session, &stmt) {
            if !stmt.param_types.is_empty() && stmt.param_types != current_types {
                return Err(Self::reject_stale_prepared_statement(
                    engine, session, stmt, stmt_id,
                ));
            }
        }
        Ok(stmt)
    }

    fn current_insert_param_types(
        engine: &Engine,
        session: &Session,
        stmt: &crate::sql::execution::prepared::PreparedStatement,
    ) -> Option<(String, Vec<crate::protocol::types::DataType>)> {
        let (table, columns) = match &stmt.stmt {
            Statement::Insert { table, columns, .. }
            | Statement::Replace { table, columns, .. } => (table, columns),
            _ => return None,
        };
        let database = table.effective_database(&session.database);
        let meta = engine.get_table_meta(database, &table.name)?;
        let mut types = Vec::with_capacity(stmt.param_count);
        for param_index in 0..stmt.param_count {
            let column = if columns.is_empty() {
                meta.columns.get(param_index)
            } else {
                columns.get(param_index).and_then(|name| {
                    meta.columns
                        .iter()
                        .find(|column| column.name.eq_ignore_ascii_case(name))
                })
            };
            types.push(column?.data_type);
        }
        Some((format!("{}.{}", database, table.name), types))
    }

    fn reject_stale_prepared_statement(
        engine: &Engine,
        session: &mut Session,
        stmt: crate::sql::execution::prepared::PreparedStatement,
        stmt_id: u32,
    ) -> Vec<Message> {
        let qualified_name = Self::current_insert_param_types(engine, session, &stmt)
            .map(|(name, _)| name)
            .unwrap_or_else(|| "unknown table".to_string());
        session.prepared_statements.insert(stmt);
        tracing::warn!(
            stmt_id,
            table = %qualified_name,
            "prepared statement rejected: table schema changed after PREPARE"
        );
        vec![build_error(&format!(
            "prepared statement {} no longer matches current schema of {} \
             (table altered after PREPARE); re-prepare the statement",
            stmt_id, qualified_name
        ))]
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

        let mut stmt = match Self::take_prepared_statement(session, stmt_id, "execute") {
            Ok(stmt) => stmt,
            Err(messages) => return Ok(messages),
        };

        stmt.params = params;

        let mut stmt = match Self::reject_if_schema_changed(&self.engine, session, stmt, stmt_id) {
            Ok(stmt) => stmt,
            Err(messages) => return Ok(messages),
        };

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
                let encoded = crate::protocol::result::encode_binary_result_batches(&schema, &batches);
                drop(batches);
                let timings = ServerTimings {
                    total: server_start.elapsed(),
                    execute: execute_elapsed,
                    encode: encode_start.elapsed(),
                };
                let mut payload = encoded;
                append_server_timing_trailer(&mut payload, timings);
                let msg = Message::new(MSG_TYPE_BINARY_QUERY_RESULT, Bytes::from(payload));
                let msg = match self.validate_query_response(msg) {
                    Ok(msg) => msg,
                    Err(error) => return Ok(vec![build_query_error(&error)]),
                };
                Ok(vec![msg])
            }
            Err(e) => Ok(vec![build_query_error(&e)]),
        }
    }

    fn decode_param_value(data: &[u8], dt: DataType) -> Result<(ScalarValue, usize)> {
        crate::protocol::scalar_codec::decode_param_value(data, dt)
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

        let stmt = match Self::take_prepared_statement(session, stmt_id, "batch execute") {
            Ok(stmt) => stmt,
            Err(messages) => return Ok(messages),
        };

        let param_count = stmt.param_count;

        let mut stmt = match Self::reject_if_schema_changed(&self.engine, session, stmt, stmt_id) {
            Ok(stmt) => stmt,
            Err(messages) => return Ok(messages),
        };
        // batch_size comes straight from the client: never trust it for
        // preallocation. Each parameter costs at least one type-tag byte, so
        // the remaining payload bounds how many rows can really follow.
        const MAX_BATCH_EXECUTE_ROWS_NO_PARAMS: usize = 1_000_000;
        let remaining = payload.len() - offset;
        let max_rows = if param_count == 0 {
            MAX_BATCH_EXECUTE_ROWS_NO_PARAMS
        } else {
            remaining / param_count
        };
        if batch_size > max_rows {
            session.prepared_statements.insert(stmt);
            return Ok(vec![build_error(&format!(
                "invalid batch execute message: batch size {} exceeds limit {} for this payload",
                batch_size, max_rows
            ))]);
        }
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
        let rows = self.engine.rollback_session_txn(session)?;
        Ok(vec![build_ok(rows)])
    }

    fn handle_cursor_open(&mut self, payload: &[u8]) -> Result<Vec<Message>> {
        self.handle_query(payload)
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

    /// MSG_TYPE_SUBSCRIBE: payload is one length-prefixed topic string, either
    /// `table` (session database), `database.table`, `database.*` or `>`.
    /// Replies MSG_TYPE_OK carrying the subscription id, or an error.
    /// SUBSCRIBE permission gate shared by the hub (v1, wildcard topics)
    /// and reliable (v2, exact table) subscription paths. `Err(messages)` is
    /// the ready-to-send rejection reply.
    fn require_subscribe_permission(
        engine: &Engine,
        username: &str,
        topic_display: &str,
        object_type: PermissionObjectType,
        object: &str,
    ) -> std::result::Result<(), Vec<Message>> {
        if engine.auth_manager().check_permission(
            username,
            PermissionType::Subscribe,
            object_type,
            object,
        ) {
            Ok(())
        } else {
            tracing::warn!(
                username,
                topic = topic_display,
                "subscribe rejected: missing SUBSCRIBE permission"
            );
            Err(vec![build_error(&format!(
                "access denied for user '{username}' to SUBSCRIBE on '{topic_display}'"
            ))])
        }
    }

    fn handle_subscribe(&mut self, payload: &[u8]) -> Result<Vec<Message>> {
        let (topic, _) = match read_len_prefixed_string(payload) {
            Ok(v) => v,
            Err(_) => return Ok(vec![build_error("invalid subscribe message")]),
        };
        let session = match &mut self.session {
            Some(s) => s,
            None => return Ok(vec![build_error("not logged in")]),
        };
        session.touch();

        let Some(push_tx) = self.push_channel.clone() else {
            return Ok(vec![build_error(
                "subscriptions are not available on this connection",
            )]);
        };

        // Validate the pattern before the permission check so malformed
        // topics (e.g. `db.t*cks`) are rejected as such instead of being
        // permission-checked as a database-level wildcard.
        if let Err(reason) = crate::pubsub::TopicPattern::parse(topic) {
            return Ok(vec![build_error(&format!(
                "invalid subscription topic '{topic}': {reason}"
            ))]);
        }

        // Resolve the topic against the session database, validate that the
        // referenced objects exist, and require SUBSCRIBE (or admin) rights.
        // `>` subscribes to every database; `db`-scoped wildcards require a
        // database-level grant, exact tables a table-level grant.
        let permission_object = if topic == ">" {
            (PermissionObjectType::Global, "*".to_string())
        } else if topic == "*" || topic.is_empty() {
            return Ok(vec![build_error(&format!(
                "invalid subscription topic '{topic}': expected 'table', 'database.table', 'database.*' or '>'"
            ))]);
        } else {
            match topic.split_once('.') {
                Some((db, rest)) if rest.contains('*') || rest == ">" => {
                    (PermissionObjectType::Database, db.to_string())
                }
                Some((db, table)) => (PermissionObjectType::Table, format!("{db}.{table}")),
                None => (
                    PermissionObjectType::Table,
                    format!("{}.{}", session.database, topic),
                ),
            }
        };
        let (topic_db, topic_table) = if topic == ">" {
            (None, None)
        } else {
            match topic.split_once('.') {
                Some((db, table)) => (Some(db), Some(table)),
                None => (Some(session.database.as_str()), Some(topic)),
            }
        };
        if let Some(db) = topic_db {
            if !self.engine.database_exists(db) {
                return Ok(vec![build_error(&format!(
                    "database '{db}' does not exist"
                ))]);
            }
        }
        if let (Some(db), Some(table)) = (topic_db, topic_table) {
            if !table.contains('*')
                && table != ">"
                && self.engine.get_table_meta(db, table).is_none()
            {
                return Ok(vec![build_error(&format!(
                    "table '{db}.{table}' does not exist"
                ))]);
            }
        }
        let subscriber = session.username.clone();
        if let Err(messages) = Self::require_subscribe_permission(
            &self.engine,
            &subscriber,
            topic,
            permission_object.0.clone(),
            &permission_object.1,
        ) {
            return Ok(messages);
        }

        let conn = match &self.pubsub_conn {
            Some(conn) => Arc::clone(conn),
            None => {
                let conn = self.engine.pubsub().create_conn(push_tx);
                self.pubsub_conn = Some(Arc::clone(&conn));
                conn
            }
        };
        // Register the fully qualified pattern: hub subjects are always
        // `database.table`, so a bare table name must be expanded before
        // matching.
        let registered_topic = if topic.contains('.') || topic == ">" {
            topic.to_string()
        } else {
            format!("{}.{}", session.database, topic)
        };
        match self.engine.pubsub().subscribe(&conn, &registered_topic) {
            Ok(sub_id) => {
                tracing::info!(
                    username = %session.username,
                    topic = %registered_topic,
                    sub_id,
                    conn_id = conn.conn_id(),
                    "subscription registered"
                );
                Ok(vec![build_ok(sub_id)])
            }
            Err(reason) => Ok(vec![build_error(&reason)]),
        }
    }

    /// MSG_TYPE_UNSUBSCRIBE: payload is a u64 subscription id.
    fn handle_unsubscribe(&mut self, payload: &[u8]) -> Result<Vec<Message>> {
        if payload.len() < 8 {
            return Ok(vec![build_error("invalid unsubscribe message")]);
        }
        let session = match &mut self.session {
            Some(s) => s,
            None => return Ok(vec![build_error("not logged in")]),
        };
        session.touch();
        let sub_id = u64::from_le_bytes(payload[..8].try_into().unwrap());
        let Some(conn) = &self.pubsub_conn else {
            return Ok(vec![build_error(&format!(
                "subscription {sub_id} not found"
            ))]);
        };
        if self.engine.pubsub().unsubscribe(conn, sub_id) {
            tracing::info!(username = %session.username, sub_id, "subscription removed");
            Ok(vec![build_ok(0)])
        } else {
            Ok(vec![build_error(&format!(
                "subscription {sub_id} not found"
            ))])
        }
    }

    /// MSG_TYPE_SUBSCRIBE_V2: resumable single-table subscription with an
    /// optional start sequence, named bookmark and column filter. Replies
    /// SUB_CONFIRMED (or SUB_ERROR); data arrives later as SUB_DATA pushes.
    fn handle_subscribe_v2(&mut self, payload: &[u8]) -> Result<Vec<Message>> {
        let (username, session_id) = match &mut self.session {
            Some(session) => {
                session.touch();
                (session.username.clone(), session.id)
            }
            None => return Ok(vec![build_error("not logged in")]),
        };

        let request = match decode_subscribe(payload) {
            Ok(request) => request,
            Err(err) => {
                return Ok(vec![build_error(&err.to_string())]);
            }
        };

        // The reliable stream is per exact table: retention, acks and
        // bookmarks are all keyed on (db, table), so wildcards make no sense.
        if request.db.is_empty()
            || request.table.is_empty()
            || request.table.contains('*')
            || request.table.contains('>')
        {
            return Ok(vec![build_error(
                "reliable subscriptions require an exact 'database.table' topic",
            )]);
        }
        let manager = Arc::clone(self.engine.pubsub_manager());
        if !manager.is_enabled() {
            return Ok(vec![build_error("pubsub is disabled on this server")]);
        }
        let Some(table_meta) = self.engine.get_table_meta(&request.db, &request.table) else {
            return Ok(vec![build_error(&format!(
                "table '{}.{}' does not exist",
                request.db, request.table
            ))]);
        };
        if let Some(column) = &request.filter_column {
            if !table_meta.columns.iter().any(|c| &c.name == column) {
                return Ok(vec![build_error(&format!(
                    "filter column '{column}' does not exist on '{}.{}'",
                    request.db, request.table
                ))]);
            }
        }
        let topic_display = format!("{}.{}", request.db, request.table);
        if let Err(messages) = Self::require_subscribe_permission(
            &self.engine,
            &username,
            &topic_display,
            PermissionObjectType::Table,
            &topic_display,
        ) {
            return Ok(messages);
        }
        let Some(push_tx) = self.push_channel.clone() else {
            return Ok(vec![build_error(
                "subscriptions are not available on this connection",
            )]);
        };

        // Frames from the delivery task are re-encoded as protocol messages
        // and funneled into the shared per-connection push queue, so the
        // connection loop forwards them exactly like hub notifications.
        let frame_capacity = self.config.pubsub.notification_channel_capacity.max(1);
        let max_frame_batches = self.config.pubsub.max_batches_per_frame.max(1);
        let (frame_tx, mut frame_rx) = tokio::sync::mpsc::channel::<PublishedFrame>(frame_capacity);
        tokio::spawn(async move {
            while let Some(frame) = frame_rx.recv().await {
                match frame_to_message(frame) {
                    Ok(msg) => {
                        if push_tx.send(msg).await.is_err() {
                            break;
                        }
                    }
                    Err(err) => tracing::warn!(
                        error = %err,
                        "reliable pubsub frame encode failed; frame dropped"
                    ),
                }
            }
        });

        let subscribe_request = SubscribeRequest {
            db: request.db.clone(),
            table: request.table.clone(),
            mode: match request.from_seq {
                Some(seq) => SubscribeMode::FromSeq(seq),
                None => SubscribeMode::FromNow,
            },
            filter_column: request.filter_column,
            filter_values: request.filter_values,
            named: request.named,
            session_id,
            frame_tx: frame_tx.clone(),
        };
        match manager.subscribe(subscribe_request) {
            Ok((grant, handle)) => {
                spawn_delivery(handle, max_frame_batches, frame_tx);
                tracing::info!(
                    username,
                    db = %request.db,
                    table = %request.table,
                    sub_id = grant.sub_id,
                    granted_from = grant.granted_from,
                    "reliable subscription registered"
                );
                Ok(vec![confirmed_message(&grant)])
            }
            Err(reason) => Ok(vec![build_error(&reason.to_string())]),
        }
    }

    /// MSG_TYPE_UNSUBSCRIBE_V2: payload is `[string db][string table][u64 sub_id]`.
    fn handle_unsubscribe_v2(&mut self, payload: &[u8]) -> Result<Vec<Message>> {
        let Some(session) = &mut self.session else {
            return Ok(vec![build_error("not logged in")]);
        };
        session.touch();
        match decode_table_and_sub(payload) {
            Ok((db, table, sub_id)) => {
                if self
                    .engine
                    .pubsub_manager()
                    .unsubscribe(&db, &table, sub_id)
                {
                    tracing::info!(username = %session.username, db, table, sub_id, "reliable subscription removed");
                    Ok(vec![build_ok(0)])
                } else {
                    Ok(vec![build_error(&format!(
                        "subscription {sub_id} not found"
                    ))])
                }
            }
            Err(err) => Ok(vec![build_error(&err.to_string())]),
        }
    }

    /// MSG_TYPE_SUB_ACK: payload is `[string db][string table][u64 sub_id][u64 seq]`.
    fn handle_sub_ack(&mut self, payload: &[u8]) -> Result<Vec<Message>> {
        let Some(session) = &mut self.session else {
            return Ok(vec![build_error("not logged in")]);
        };
        session.touch();
        match decode_table_sub_and_seq(payload) {
            Ok((db, table, sub_id, seq)) => {
                match self.engine.pubsub_manager().ack(&db, &table, sub_id, seq) {
                    Ok(()) => Ok(vec![build_ok(0)]),
                    Err(err) => Ok(vec![build_error(&err.to_string())]),
                }
            }
            Err(err) => Ok(vec![build_error(&err.to_string())]),
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

fn is_heartbeat_query(sql: &str) -> bool {
    let sql = sql.trim_end();
    let sql = sql.strip_suffix(';').unwrap_or(sql).trim_end();
    let mut tokens = sql.split_ascii_whitespace();
    matches!(
        (tokens.next(), tokens.next(), tokens.next()),
        (Some(select), Some("1"), None) if select.eq_ignore_ascii_case("SELECT")
    )
}

/// Requests that run outside the async networking path and can outlive the
/// client's patience; these are the ones worth listing and killing.
pub(crate) fn is_heavy_request(msg_type: u8) -> bool {
    matches!(
        msg_type,
        MSG_TYPE_QUERY
            | MSG_TYPE_BULK_INSERT_COLUMNAR
            | MSG_TYPE_FLUSH
            | MSG_TYPE_EXECUTE
            | MSG_TYPE_BATCH_EXECUTE
            | MSG_TYPE_COMMIT_TXN
            | MSG_TYPE_ROLLBACK_TXN
            | MSG_TYPE_CURSOR
            | MSG_TYPE_REPL_SEGMENT_FETCH
    )
}

/// Only a single administrative statement gets the independent control slot.
/// The normal handler still performs authentication and authorization checks.
pub(crate) fn is_control_request(msg: &Message) -> bool {
    if msg.msg_type != MSG_TYPE_QUERY || msg.payload.len() > 4096 {
        return false;
    }
    let sql = message_sql_snippet(&msg.payload);
    matches!(
        Parser::parse(&sql).as_deref(),
        Ok([Statement::KillQuery { .. } | Statement::ShowProcesslist { .. }])
    )
}

/// Best-effort SQL text from a QUERY payload (`[database][sql]`), bounded by
/// the registry's snippet cap. Malformed payloads fall back to the raw bytes.
fn message_sql_snippet(payload: &[u8]) -> String {
    if let Ok((_, rest)) = read_len_prefixed_string(payload) {
        if let Ok((sql, _)) = read_len_prefixed_string(rest) {
            return sql.trim().to_string();
        }
    }
    std::str::from_utf8(payload)
        .unwrap_or("")
        .trim()
        .to_string()
}

fn should_log_query_sql(sql: &str) -> bool {
    // INSERT statements can be megabytes long. Do not parse every statement a
    // second time merely to decide whether it produces a result set.
    //
    // The parse here is only needed to classify the statement for the query
    // log, so skip it entirely when the query log is disabled: otherwise
    // every SELECT/SHOW/EXPLAIN would be parsed twice per request even with
    // logging off.
    if !crate::common::query_log::enabled() {
        return false;
    }
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

impl Handler {
    // ===================== Replication (R1) =====================

    /// REPL_HELLO (standby -> primary, after LOGIN): authenticate, then turn
    /// this connection into a replication event stream. Live events flow
    /// through the per-connection push queue; a resumed standby first
    /// receives the ring history.
    fn handle_repl_hello(&mut self, payload: &[u8]) -> Result<Vec<Message>> {
        let Some(session) = &self.session else {
            return Ok(vec![build_error("REPL_HELLO requires LOGIN first")]);
        };
        let username = session.username.clone();
        let Some(rep_log) = self.engine.rep_log().map(Arc::clone) else {
            return Ok(vec![build_error("server is not a replication primary")]);
        };

        let (user, rest) = read_len_prefixed_string(payload)?;
        let (password, rest) = read_len_prefixed_string(rest)?;
        if rest.len() < 8 {
            return Ok(vec![build_error("REPL_HELLO payload truncated")]);
        }
        let last_seq = u64::from_le_bytes(rest[..8].try_into().unwrap());

        // The logged-in session must belong to the same user, and that user
        // must hold the REPLICATION permission.
        if user != username {
            return Ok(vec![build_error(&format!(
                "REPL_HELLO user '{user}' does not match the logged-in user '{username}'"
            ))]);
        }
        match self.engine.auth_manager().authenticate(&username, password) {
            Ok(true) => {}
            _ => {
                return Ok(vec![build_error(&format!(
                    "REPL_HELLO credentials for user '{username}' are invalid"
                ))]);
            }
        }
        if !self.engine.auth_manager().check_permission(
            &username,
            crate::storage::catalog::auth::PermissionType::Replication,
            crate::storage::catalog::auth::PermissionObjectType::Global,
            "*",
        ) {
            return Ok(vec![build_error(&format!(
                "access denied for user '{username}' to REPLICATION"
            ))]);
        }

        // A (re)connecting standby proved it durably applied `last_seq`.
        // Fast-forward BEFORE reading history: a primary that restarted on a
        // stale seq file may have stamped fresh events with seqs this
        // standby already consumed. note_standby_applied re-stamps the ring
        // above the watermark so the history below (and the live fan-out)
        // delivers them; calling it after events_after would hand the bridge
        // a snapshot that still skips them.
        rep_log.note_standby_applied(last_seq);

        let history = match rep_log.events_after(last_seq) {
            Ok(history) => history,
            Err(error) => return Ok(vec![build_error(&error.to_string())]),
        };

        let Some(push_tx) = self.push_channel.clone() else {
            return Ok(vec![build_error(
                "replication pushes are not available on this connection",
            )]);
        };

        // Bridge thread: ring history first, then live fan-out, both funneled
        // into the connection push queue in strict seq order. The bridge owns
        // its ring session (register + unregister); the returned handle is
        // dropped on purpose (detached), the bridge reaps itself via the
        // disconnect probe / pending cap / channel teardown.
        let _bridge = spawn_replication_push_bridge(
            rep_log,
            push_tx,
            last_seq,
            history,
            REPL_PUSH_PARK_TIMEOUT,
        );

        Ok(vec![build_ok(0)])
    }

    /// REPL_SEGMENT_FETCH (standby -> primary): stream the requested segment
    /// file back as one or more REPL_SEGMENT_CHUNK response frames.
    fn handle_repl_segment_fetch(&self, payload: &[u8]) -> Result<Vec<Message>> {
        let Some(_session) = &self.session else {
            return Ok(vec![build_error("REPL_SEGMENT_FETCH requires LOGIN first")]);
        };
        if self.engine.rep_log().is_none() {
            return Ok(vec![build_error("server is not a replication primary")]);
        }
        let (db, rest) = read_len_prefixed_string(payload)?;
        let (table, rest) = read_len_prefixed_string(rest)?;
        let (segment, rest) = read_len_prefixed_string(rest)?;
        if rest.len() < 12 {
            return Ok(vec![build_error("REPL_SEGMENT_FETCH payload truncated")]);
        }
        let offset = u64::from_le_bytes(rest[..8].try_into().unwrap());
        let max_chunk = u32::from_le_bytes(rest[8..12].try_into().unwrap()).max(1);
        // Path safety: bare file name only.
        if segment.contains('/')
            || segment.contains('\\')
            || segment.contains("..")
            || db.contains("..")
            || table.contains("..")
        {
            return Ok(vec![build_error("REPL_SEGMENT_FETCH: unsafe path")]);
        }
        let path = self.engine.data_dir().join(db).join(table).join(segment);
        let file = match std::fs::File::open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                // A compaction/TTL may have retired the segment between the
                // SegmentUpdate's commit and this fetch. When the ring proves
                // the retirement, tell the standby to skip the stale update;
                // the replacement event later in the stream carries the rows.
                if self
                    .engine
                    .rep_log()
                    .is_some_and(|log| log.segment_retired(db, table, segment))
                {
                    return Ok(vec![Message::empty(MSG_TYPE_REPL_SEGMENT_GONE)]);
                }
                return Ok(vec![build_error(&format!(
                    "segment fetch failed for {db}.{table}/{segment}: {error}"
                ))]);
            }
            Err(error) => {
                return Ok(vec![build_error(&format!(
                    "segment fetch failed for {db}.{table}/{segment}: {error}"
                ))]);
            }
        };
        let total_len = file.metadata()?.len();
        let mut file = file;
        use std::io::{Read, Seek, SeekFrom};
        file.seek(SeekFrom::Start(offset))?;
        let chunk_len = (max_chunk as usize).min(4 * 1024 * 1024);
        let mut chunk = vec![0u8; chunk_len];
        let read = file.read(&mut chunk)?;
        chunk.truncate(read);

        let mut message_bytes = Vec::with_capacity(16 + read);
        message_bytes.extend_from_slice(&total_len.to_le_bytes());
        message_bytes.extend_from_slice(&offset.to_le_bytes());
        message_bytes.extend_from_slice(&chunk);
        Ok(vec![Message {
            msg_type: MSG_TYPE_REPL_SEGMENT_CHUNK,
            payload: bytes::Bytes::from(message_bytes),
        }])
    }

    /// REPL_ACK (standby -> primary): progress report. The ack doubles as the
    /// durability driver for the primary's seq file — without it the file
    /// lags `persist_every` events behind and a crash reuses consumed seqs.
    fn handle_repl_ack(&self, payload: &[u8]) -> Result<Vec<Message>> {
        if payload.len() >= 8 {
            let applied = u64::from_le_bytes(payload[..8].try_into().unwrap());
            if let Some(rep_log) = self.engine.rep_log() {
                rep_log.note_standby_applied(applied);
            }
            tracing::debug!(applied_seq = applied, "standby replication ack");
        }
        Ok(Vec::new())
    }
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

/// Error response for the SQL execution paths (`MSG_TYPE_QUERY` /
/// `MSG_TYPE_EXECUTE`). The wire protocol carries error payloads as plain
/// text with no error-code or classification field, so transient
/// resource-busy conditions (concurrent DML inside the ALTER convergence
/// window, hot-store backpressure) get the stable
/// [`RETRYABLE_ERROR_PREFIX`] marker instead — the minimal extension that
/// lets clients distinguish "back off and re-issue" from "fail the
/// statement". Deterministic overruns (query/transaction memory budgets,
/// result-set and input-materialization limits) keep the plain one-shot
/// error text: retrying reproduces the same failure. Everything else keeps
/// the plain one-shot error text.
/// Connection-layer and authentication errors intentionally do NOT pass
/// through here: they are not transient.
fn build_query_error(error: &TaibaiError) -> Message {
    match error {
        TaibaiError::ResourceBusy(message) => {
            // Budget overruns are DETERMINISTIC: retrying just redoes the
            // same doomed work four times over. Only contention-shaped
            // busy (lock windows, backpressure) is transient. Result-set
            // and input-materialization overruns ("exceeded memory limit")
            // are deterministic in the same sense — the same query against
            // the same data fails identically — so they must not advertise
            // retry.
            let budget_error = message.contains("memory budget")
                || message.contains("transaction budget")
                || message.contains("exceeded the budget")
                || message.contains("exceeded memory limit")
                || message.contains("exceeded maximum message size");
            if budget_error {
                build_error(&format!("resource busy: {message}"))
            } else {
                build_error(&format!(
                    "{} resource busy: {} (transient server-side contention; the                      built-in client auto-retries this only for read-only statements                      — SELECT/SHOW/EXPLAIN; writes must be retried by the application)",
                    crate::net::client::RETRYABLE_ERROR_PREFIX,
                    message
                ))
            }
        }
        other => build_error(&other.to_string()),
    }
}

/// Outcome of parsing a `SET STREAMING_RESULTS` statement.
enum StreamingResultsSet {
    NotASetStatement,
    Enable,
    Disable,
    Invalid,
}

/// `SET STREAMING_RESULTS = {TRUE|FALSE|ON|OFF|1|0}` (optionally
/// `SET SESSION ...`, case-insensitive, `=` optional): switch the session
/// between single-message results (default, bounded by the wire frame cap)
/// and chunked streaming results (bounded by the query memory budgets
/// instead). Only `SET` statements naming this variable are recognized; any
/// other `SET ...` reports Invalid with usage guidance instead of reaching
/// the SQL engine.
fn parse_streaming_results_set(sql: &str) -> StreamingResultsSet {
    let mut tokens = sql.split_whitespace();
    let Some(first) = tokens.next() else {
        return StreamingResultsSet::NotASetStatement;
    };
    if !first.eq_ignore_ascii_case("SET") {
        return StreamingResultsSet::NotASetStatement;
    }
    let mut name = tokens.next().unwrap_or("");
    let mut inline_value: Option<&str> = None;
    if let Some((prefix, value)) = name.split_once('=') {
        name = prefix;
        inline_value = Some(value);
    }
    if name.eq_ignore_ascii_case("SESSION") && inline_value.is_none() {
        name = tokens.next().unwrap_or("");
        if let Some((prefix, value)) = name.split_once('=') {
            name = prefix;
            inline_value = Some(value);
        }
    }
    if !name.eq_ignore_ascii_case("STREAMING_RESULTS") {
        return StreamingResultsSet::Invalid;
    }
    let value = match (inline_value, tokens.collect::<Vec<_>>().as_slice()) {
        (Some(value), []) => value,
        (None, ["=", value]) | (None, [value]) => value,
        _ => return StreamingResultsSet::Invalid,
    };
    match value.to_ascii_uppercase().as_str() {
        "TRUE" | "ON" | "1" => StreamingResultsSet::Enable,
        "FALSE" | "OFF" | "0" => StreamingResultsSet::Disable,
        _ => StreamingResultsSet::Invalid,
    }
}

/// Split an oversized result batch into row slices that fit the per-chunk
/// budget. A single scan batch can exceed the wire frame cap (wide rows at
/// row-group sizes) and would otherwise abort the whole stream mid-flight.
/// Slice rows via `take_rows` using the estimated-size ratio; every emitted
/// slice is re-checked against the hard frame cap after encoding.
fn split_batch_for_chunks(batch: RecordBatch, chunk_budget: usize) -> Result<Vec<RecordBatch>> {
    let estimate = batch.estimated_size_bytes().max(1);
    let total_rows = batch.num_rows();
    if estimate <= chunk_budget || total_rows <= 1 {
        return Ok(vec![batch]);
    }
    let rows_per_chunk = ((chunk_budget as u128 * total_rows as u128) / estimate as u128) as usize;
    let rows_per_chunk = rows_per_chunk.clamp(1, total_rows);
    let mut parts = Vec::new();
    for start in (0..total_rows).step_by(rows_per_chunk) {
        let end = (start + rows_per_chunk).min(total_rows);
        let indices: Vec<u32> = (start as u32..end as u32).collect();
        parts.push(batch.take_rows(&indices)?);
    }
    Ok(parts)
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

#[cfg(test)]
#[path = "handler_query_error_test.rs"]
mod query_error_tests;

#[cfg(test)]
#[path = "handler_test.rs"]
mod tests;
