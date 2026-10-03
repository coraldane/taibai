use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::Semaphore;
use tokio::time::timeout;
use tokio_util::codec::Framed;

use crate::common::error::{Result, TaibaiError};
use crate::net::pubsub_session::decode_sub_confirmed;
use crate::protocol::codec::MessageCodec;
use crate::protocol::message::*;
use crate::protocol::types::DataType;
use crate::types::array::ColumnArray;
use crate::types::batch::{ColumnDef, RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

pub struct BulkImportConfig;

/// Upper bound on server notifications buffered while request replies are
/// being consumed (mirrors the server's bounded per-connection push queue).
const MAX_BUFFERED_NOTIFICATIONS: usize = 8_192;
/// Decode guards for server results: a frame that fails these is corrupt or
/// hostile, and must surface as a protocol error instead of a giant
/// allocation that aborts the client process.
const MAX_BINARY_RESULT_COLUMNS: usize = 4_096;
/// A column of N rows needs at least one byte per row somewhere in the
/// payload; anything denser is malformed.
const MAX_BINARY_RESULT_ROWS_PER_BYTE: u64 = 1;

/// Stable marker the server prefixes onto transient (resource-busy) SQL
/// errors. The wire protocol has no error-code or classification field —
/// `MSG_TYPE_ERROR` payloads are plain message text — so this prefix is the
/// minimal extension letting a client tell "back off and re-issue" apart
/// from "fail the statement". See `Handler::build_query_error`.
pub const RETRYABLE_ERROR_PREFIX: &str = "[RETRYABLE]";

/// Automatic retries a read-only statement gets when the server answers
/// with a retryable (resource-busy) error before the client gives up and
/// surfaces the error to the caller.
const RETRYABLE_MAX_RETRIES: usize = 3;

/// Backoff before retry attempt `n` (0-based): 10ms / 40ms / 160ms plus
/// jitter of up to +50%, so a cohort of clients hitting the same busy
/// resource de-synchronizes instead of retrying in lockstep.
fn retry_backoff_delay(attempt: usize) -> Duration {
    let base_ms = 10u64.saturating_mul(1u64 << (2 * attempt.min(6) as u32));
    let base_ms = base_ms.min(160);
    // No rand dependency: hash the wall clock. The goal is de-synchronization,
    // not cryptographic quality.
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.subsec_nanos() as u64)
        .unwrap_or(0);
    let jitter = nanos % (base_ms / 2 + 1);
    Duration::from_millis(base_ms + jitter)
}

/// Whether a server error message carries the retryable marker.
pub fn is_retryable_error_message(message: &str) -> bool {
    message.trim_start().starts_with(RETRYABLE_ERROR_PREFIX)
}

/// Whether `sql` is a statement the client may safely re-issue after a
/// retryable server error. ONLY leading-keyword SELECT / SHOW / EXPLAIN
/// qualify: they are idempotent reads, so a retry can never commit anything
/// twice.
///
/// Everything else — INSERT/UPDATE/DELETE/REPLACE, DDL, transaction control,
/// and read/write CTEs (`WITH ... INSERT`) — is deliberately excluded: the
/// first attempt may have committed before the error was raised (or the
/// retry may commit twice), so auto-retrying a write is an
/// at-least-once/at-most-once gamble the CLIENT cannot resolve. For those
/// statements the retryable marker is passed straight through to the caller
/// (see [`Client::query`]); the application decides whether and how to
/// retry.
pub fn statement_is_read_only(sql: &str) -> bool {
    let first_word = sql.split_whitespace().next().unwrap_or("");
    matches!(
        first_word.to_ascii_uppercase().as_str(),
        "SELECT" | "SHOW" | "EXPLAIN"
    )
}

pub struct Client {
    framed: Framed<tokio::net::TcpStream, MessageCodec>,
    session_id: Option<String>,
    default_timeout: Duration,
    pipeline_mode: Arc<AtomicBool>,
    pipeline_depth: usize,
    pipeline_sem: Option<Arc<Semaphore>>,
    pending_count: Arc<AtomicI64>,
    response_handled: Arc<AtomicI64>,
    bulk_import_mode: bool,
    /// Server-pushed change notifications diverted from the response stream
    /// while awaiting request replies. Drained via `recv_notification`.
    /// Bounded: a subscriber that never drains them must not grow memory
    /// without limit (mirrors the server's bounded push queue).
    pending_notifications: std::collections::VecDeque<Message>,
    /// Reliable-subscription events (SUB_DATA / SUB_ERROR) diverted the same
    /// way; drained via `recv_sub_event`. Bounded for the same reason.
    pending_sub_events: std::collections::VecDeque<Message>,
    /// A response frame that was read while waiting for a notification and
    /// could not be consumed yet; replayed by `next_frame`.
    pending_response: Option<Message>,
}

#[derive(Debug, Clone)]
pub struct PreparedStatement {
    pub id: u32,
    pub param_count: usize,
    pub sql: String,
}

#[derive(Debug, Clone)]
pub struct ServerTimings {
    pub total: Duration,
    pub execute: Option<Duration>,
    pub encode: Option<Duration>,
}

#[derive(Debug, Clone)]
pub struct ClientTimings {
    pub send: Duration,
    pub recv: Duration,
    pub decode: Duration,
}

#[derive(Debug, Clone)]
pub enum QueryResult {
    Ok {
        rows_affected: u64,
        server_elapsed: Option<Duration>,
        server_timings: Option<ServerTimings>,
        client_timings: Option<ClientTimings>,
    },
    Rows {
        schema: Schema,
        batches: Vec<RecordBatch>,
        server_elapsed: Option<Duration>,
        server_timings: Option<ServerTimings>,
        client_timings: Option<ClientTimings>,
    },
    Error(String),
}

impl Client {
    pub async fn connect<A: tokio::net::ToSocketAddrs>(addr: A) -> Result<Self> {
        // Bulk imports push 40-90MB frames over loopback; the OS default
        // 64KB send buffer paces them at ~1GB/s. Larger buffers let big
        // messages drain without ACK-per-chunk stalls (best-effort: the
        // fallback path keeps platforms without TcpSocket working).
        let resolved = tokio::net::lookup_host(&addr)
            .await
            .ok()
            .and_then(|mut addrs| addrs.next());
        let stream = match resolved {
            Some(addr) => match tokio::net::TcpSocket::new_v4() {
                Ok(socket) => {
                    let _ = socket.set_send_buffer_size(8 * 1024 * 1024);
                    let _ = socket.set_recv_buffer_size(8 * 1024 * 1024);
                    match socket.connect(addr).await {
                        Ok(s) => s,
                        Err(_) => TcpStream::connect(addr).await?,
                    }
                }
                Err(_) => TcpStream::connect(addr).await?,
            },
            None => TcpStream::connect(addr).await?,
        };
        // The protocol is request/response heavy: disable Nagle so small
        // queries are not stalled by delayed-ACK interaction.
        let _ = stream.set_nodelay(true);
        let framed = Framed::new(
            stream,
            MessageCodec::new(MAX_MESSAGE_SIZE).with_compression(4096),
        );
        Ok(Self {
            framed,
            session_id: None,
            default_timeout: Duration::from_secs(300),
            pipeline_mode: Arc::new(AtomicBool::new(false)),
            pipeline_depth: 0,
            pipeline_sem: None,
            pending_count: Arc::new(AtomicI64::new(0)),
            response_handled: Arc::new(AtomicI64::new(0)),
            bulk_import_mode: false,
            pending_notifications: std::collections::VecDeque::new(),
            pending_sub_events: std::collections::VecDeque::new(),
            pending_response: None,
        })
    }

    pub fn set_timeout(&mut self, dur: Duration) {
        self.default_timeout = dur;
    }

    pub async fn login(&mut self, username: &str, password: &str, database: &str) -> Result<()> {
        self.login_with_timezone(username, password, database, 0)
            .await
    }

    pub async fn login_with_timezone(
        &mut self,
        username: &str,
        password: &str,
        database: &str,
        timezone_offset_hours: i32,
    ) -> Result<()> {
        let mut payload = Vec::new();
        write_len_prefixed_string(&mut payload, username);
        write_len_prefixed_string(&mut payload, password);
        write_len_prefixed_string(&mut payload, database);
        payload.extend_from_slice(&timezone_offset_hours.to_le_bytes());
        payload.push(1);

        self.send(Message::new(MSG_TYPE_LOGIN, Bytes::from(payload)))
            .await?;

        let resp = self.recv().await?;
        match resp.msg_type {
            MSG_TYPE_AUTH_OK => {
                let sid = String::from_utf8_lossy(&resp.payload).to_string();
                self.session_id = Some(sid);
                Ok(())
            }
            MSG_TYPE_AUTH_FAIL => {
                let msg = String::from_utf8_lossy(&resp.payload).to_string();
                Err(TaibaiError::Auth(msg))
            }
            other => Err(TaibaiError::Protocol(format!(
                "unexpected login response type: {other:#04X}"
            ))),
        }
    }

    pub async fn ping(&mut self) -> Result<Duration> {
        let start = tokio::time::Instant::now();
        self.send(Message::empty(MSG_TYPE_PING)).await?;
        let resp = self.recv().await?;
        if resp.msg_type != MSG_TYPE_PONG {
            return Err(TaibaiError::Protocol(format!(
                "expected PONG, got {:#04X}",
                resp.msg_type
            )));
        }
        Ok(start.elapsed())
    }

    /// Execute `sql` and return the full result.
    ///
    /// Retry boundary: when the server answers a READ-ONLY statement
    /// (leading SELECT/SHOW/EXPLAIN, see [`statement_is_read_only`]) with a
    /// retryable error ([`RETRYABLE_ERROR_PREFIX`] — transient resource
    /// busy), the client automatically re-issues it up to
    /// `RETRYABLE_MAX_RETRIES` times with 10ms/40ms/160ms (+jitter)
    /// backoff. Write statements are NEVER auto-retried here: a retried
    /// INSERT may commit twice, so their retryable errors are returned
    /// as-is (marker intact) and the application decides.
    pub async fn query(&mut self, sql: &str) -> Result<QueryResult> {
        let read_only = statement_is_read_only(sql);
        for attempt in 0..=RETRYABLE_MAX_RETRIES {
            let result = self.query_once(sql).await?;
            let QueryResult::Error(message) = &result else {
                return Ok(result);
            };
            if attempt == RETRYABLE_MAX_RETRIES
                || !read_only
                || !is_retryable_error_message(message)
            {
                return Ok(result);
            }
            let delay = retry_backoff_delay(attempt);
            tracing::debug!(
                attempt,
                delay_ms = delay.as_millis() as u64,
                sql,
                "retryable server error on read-only statement; backing off before retry"
            );
            tokio::time::sleep(delay).await;
        }
        unreachable!("the loop returns on the final attempt")
    }

    async fn query_once(&mut self, sql: &str) -> Result<QueryResult> {
        let mut payload = Vec::new();
        write_len_prefixed_string(&mut payload, "");
        write_len_prefixed_string(&mut payload, sql);

        let send_start = tokio::time::Instant::now();
        self.send(Message::new(MSG_TYPE_QUERY, Bytes::from(payload)))
            .await?;
        let send_elapsed = send_start.elapsed();

        let recv_start = tokio::time::Instant::now();
        let resp = self.recv().await?;
        let mut streamed_batches = Vec::new();
        let mut streamed_schema = None;
        let resp = if resp.msg_type == MSG_TYPE_RESULT_CHUNK {
            let mut current = resp;
            loop {
                match current.msg_type {
                    MSG_TYPE_RESULT_CHUNK => {
                        let QueryResult::Rows {
                            schema, batches, ..
                        } = parse_binary_result(&current.payload)?
                        else {
                            unreachable!()
                        };
                        if let Some(expected) = &streamed_schema {
                            if expected != &schema {
                                return Err(TaibaiError::Protocol(
                                    "query stream schema changed between chunks".into(),
                                ));
                            }
                        } else {
                            streamed_schema = Some(schema);
                        }
                        streamed_batches.extend(batches);
                    }
                    MSG_TYPE_RESULT_END | MSG_TYPE_ERROR => break current,
                    other => {
                        return Err(TaibaiError::Protocol(format!(
                            "unexpected query stream response type: {other:#04X}"
                        )))
                    }
                }
                current = self.recv().await?;
            }
        } else {
            resp
        };
        let recv_elapsed = recv_start.elapsed();

        let decode_start = tokio::time::Instant::now();
        let mut result = match resp.msg_type {
            MSG_TYPE_RESULT_END => {
                let server_timings = parse_server_timing_trailer(&resp.payload, 0);
                Ok(QueryResult::Rows {
                    schema: streamed_schema.unwrap_or_else(Schema::empty),
                    batches: streamed_batches,
                    server_elapsed: server_timings.as_ref().map(|timings| timings.total),
                    server_timings,
                    client_timings: None,
                })
            }
            MSG_TYPE_OK => {
                if resp.payload.len() < 8 {
                    return Err(TaibaiError::Protocol("OK payload too short".into()));
                }
                let rows_affected = u64::from_le_bytes(resp.payload[..8].try_into().unwrap());
                let server_timings = parse_server_timing_trailer(&resp.payload, 8);
                Ok(QueryResult::Ok {
                    rows_affected,
                    server_elapsed: server_timings.as_ref().map(|t| t.total),
                    server_timings,
                    client_timings: None,
                })
            }
            MSG_TYPE_BINARY_QUERY_RESULT => Ok(parse_binary_result(&resp.payload)?),
            MSG_TYPE_ERROR => {
                let msg = String::from_utf8_lossy(&resp.payload).to_string();
                Ok(QueryResult::Error(msg))
            }
            other => Err(TaibaiError::Protocol(format!(
                "unexpected query response type: {other:#04X}"
            ))),
        }?;
        let decode_elapsed = decode_start.elapsed();
        attach_client_timings(
            &mut result,
            ClientTimings {
                send: send_elapsed,
                recv: recv_elapsed,
                decode: decode_elapsed,
            },
        );
        Ok(result)
    }

    /// Stream `sql` result batch-by-batch.
    ///
    /// No automatic retry here, by design: batches are handed to `on_batch`
    /// as they arrive, so a mid-stream retry would re-deliver rows the
    /// callback has already seen — not idempotent from the consumer's
    /// perspective. Retryable errors surface as `Err(TaibaiError::Sql(msg))`
    /// with the [`RETRYABLE_ERROR_PREFIX`] marker intact; re-issue from the
    /// caller if the statement is read-only.
    pub async fn query_each_batch<F>(
        &mut self,
        sql: &str,
        mut on_batch: F,
    ) -> Result<Option<ServerTimings>>
    where
        F: FnMut(&Schema, RecordBatch) -> Result<()>,
    {
        let mut payload = Vec::new();
        write_len_prefixed_string(&mut payload, "");
        write_len_prefixed_string(&mut payload, sql);
        self.send(Message::new(MSG_TYPE_QUERY, Bytes::from(payload)))
            .await?;

        loop {
            let response = self.recv().await?;
            match response.msg_type {
                MSG_TYPE_RESULT_CHUNK | MSG_TYPE_BINARY_QUERY_RESULT => {
                    let QueryResult::Rows {
                        schema, batches, ..
                    } = parse_binary_result(&response.payload)?
                    else {
                        unreachable!()
                    };
                    for batch in batches {
                        on_batch(&schema, batch)?;
                    }
                    if response.msg_type == MSG_TYPE_BINARY_QUERY_RESULT {
                        return Ok(None);
                    }
                }
                MSG_TYPE_RESULT_END => {
                    return Ok(parse_server_timing_trailer(&response.payload, 0));
                }
                MSG_TYPE_ERROR => {
                    return Err(TaibaiError::Sql(
                        String::from_utf8_lossy(&response.payload).to_string(),
                    ));
                }
                other => {
                    return Err(TaibaiError::Protocol(format!(
                        "unexpected streaming query response type: {other:#04X}"
                    )));
                }
            }
        }
    }

    /// Execute a (typically write) statement and return rows_affected.
    ///
    /// Delegates to [`Client::query`](Self::query), so read-only statements
    /// benefit from its automatic retryable-error retry; write statements
    /// are not retried there, and their retryable marker travels unchanged
    /// inside the returned error for the application to act on.
    pub async fn execute(&mut self, sql: &str) -> Result<u64> {
        match self.query(sql).await? {
            QueryResult::Ok { rows_affected, .. } => Ok(rows_affected),
            QueryResult::Error(msg) => Err(TaibaiError::Sql(msg)),
            QueryResult::Rows { .. } => Ok(0),
        }
    }

    pub async fn send_execute(&mut self, sql: &str) -> Result<()> {
        let mut payload = Vec::new();
        write_len_prefixed_string(&mut payload, "");
        write_len_prefixed_string(&mut payload, sql);
        self.send(Message::new(MSG_TYPE_QUERY, Bytes::from(payload)))
            .await
    }

    pub async fn recv_execute_result(&mut self) -> Result<u64> {
        let resp = self.recv().await?;
        match resp.msg_type {
            MSG_TYPE_OK => {
                if resp.payload.len() < 8 {
                    return Err(TaibaiError::Protocol("OK payload too short".into()));
                }
                Ok(u64::from_le_bytes(resp.payload[..8].try_into().unwrap()))
            }
            MSG_TYPE_ERROR => Err(TaibaiError::Sql(
                String::from_utf8_lossy(&resp.payload).to_string(),
            )),
            other => Err(TaibaiError::Protocol(format!(
                "unexpected execute response type: {other:#04X}"
            ))),
        }
    }

    pub async fn logout(&mut self) -> Result<()> {
        self.send(Message::empty(MSG_TYPE_LOGOUT)).await?;
        Ok(())
    }

    pub async fn close(self) -> Result<()> {
        use tokio::io::AsyncWriteExt;
        let _ = self.framed.into_inner().shutdown().await;
        Ok(())
    }

    pub async fn start_bulk_import(&mut self, _config: &BulkImportConfig) -> Result<()> {
        let config_json = "{}";
        let sql = format!("START BULK IMPORT {}", config_json);
        self.execute(&sql).await?;
        self.bulk_import_mode = true;
        Ok(())
    }

    pub async fn stop_bulk_import(&mut self) -> Result<u64> {
        let rows_flushed = self.execute("STOP BULK IMPORT").await?;
        self.bulk_import_mode = false;
        Ok(rows_flushed)
    }

    pub async fn bulk_insert_columnar(&mut self, payload: Bytes) -> Result<u64> {
        self.send(Message::new(MSG_TYPE_BULK_INSERT_COLUMNAR, payload))
            .await?;
        let resp = self.recv().await?;
        match resp.msg_type {
            MSG_TYPE_OK => {
                if resp.payload.len() < 8 {
                    return Err(TaibaiError::Protocol("OK payload too short".into()));
                }
                Ok(u64::from_le_bytes(resp.payload[..8].try_into().unwrap()))
            }
            MSG_TYPE_ERROR => Err(TaibaiError::Storage(
                String::from_utf8_lossy(&resp.payload).to_string(),
            )),
            other => Err(TaibaiError::Protocol(format!(
                "unexpected bulk insert response type: {other:#04X}"
            ))),
        }
    }

    pub async fn flush(&mut self) -> Result<u64> {
        self.send(Message::empty(MSG_TYPE_FLUSH)).await?;
        let resp = self.recv().await?;
        match resp.msg_type {
            MSG_TYPE_OK => {
                if resp.payload.len() < 8 {
                    return Err(TaibaiError::Protocol("OK payload too short".into()));
                }
                Ok(u64::from_le_bytes(resp.payload[..8].try_into().unwrap()))
            }
            MSG_TYPE_ERROR => Err(TaibaiError::Storage(
                String::from_utf8_lossy(&resp.payload).to_string(),
            )),
            other => Err(TaibaiError::Protocol(format!(
                "unexpected flush response type: {other:#04X}"
            ))),
        }
    }

    pub async fn start_pipeline(&mut self, depth: usize) -> Result<()> {
        if self.pipeline_mode.load(Ordering::Acquire) {
            return Err(TaibaiError::Internal("pipeline already started".into()));
        }

        let depth = if depth < 1 {
            4
        } else if depth > 16 {
            16
        } else {
            depth
        };
        self.pipeline_depth = depth;
        self.pipeline_sem = Some(Arc::new(Semaphore::new(depth)));
        self.pending_count.store(0, Ordering::Relaxed);
        self.response_handled.store(0, Ordering::Relaxed);
        self.pipeline_mode.store(true, Ordering::Release);

        Ok(())
    }

    pub async fn pipeline_bulk_insert_columnar(&mut self, payload: Bytes) -> Result<()> {
        if !self.pipeline_mode.load(Ordering::Acquire) {
            return Err(TaibaiError::Internal("pipeline not started".into()));
        }

        self.pending_count.fetch_add(1, Ordering::Relaxed);
        self.send(Message::new(MSG_TYPE_BULK_INSERT_COLUMNAR, payload))
            .await?;
        Ok(())
    }

    pub async fn stop_pipeline(&mut self) -> Result<()> {
        if !self.pipeline_mode.load(Ordering::Acquire) {
            return Ok(());
        }

        for _ in 0..100 {
            let pending = self.pending_count.load(Ordering::Relaxed);
            let handled = self.response_handled.load(Ordering::Relaxed);
            if pending == handled {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        self.pipeline_mode.store(false, Ordering::Release);
        self.send(Message::empty(MSG_TYPE_PING)).await?;
        // The PING/PONG fences the pipeline: every buffered bulk response
        // ordered before it must be drained first. Reading exactly one
        // frame and discarding it (the old behavior) silently swallowed a
        // late bulk response when the drain timed out, permanently shifting
        // the request/response pairing.
        for _ in 0..1000 {
            let msg = self.recv().await?;
            if msg.msg_type == MSG_TYPE_PONG {
                return Ok(());
            }
            tracing::debug!(
                msg_type = msg.msg_type,
                "stop_pipeline: discarding response ordered before the PONG fence"
            );
        }
        Err(TaibaiError::Protocol(
            "pipeline stop: no PONG within the fence window".into(),
        ))
    }

    pub async fn drain_pipeline(&mut self) -> Result<()> {
        while self.pending_count.load(Ordering::Relaxed)
            > self.response_handled.load(Ordering::Relaxed)
        {
            self.drain_one_pipeline_response().await?;
        }
        Ok(())
    }

    pub async fn drain_one_pipeline_response(&mut self) -> Result<()> {
        if self.pending_count.load(Ordering::Relaxed)
            <= self.response_handled.load(Ordering::Relaxed)
        {
            return Ok(());
        }

        let resp = self.recv().await?;
        self.response_handled.fetch_add(1, Ordering::Relaxed);
        match resp.msg_type {
            MSG_TYPE_OK => Ok(()),
            MSG_TYPE_ERROR => Err(TaibaiError::Storage(
                String::from_utf8_lossy(&resp.payload).to_string(),
            )),
            other => Err(TaibaiError::Protocol(format!(
                "unexpected pipeline response type: {other:#04X}"
            ))),
        }
    }

    pub fn pipeline_in_flight(&self) -> i64 {
        self.pending_count.load(Ordering::Relaxed) - self.response_handled.load(Ordering::Relaxed)
    }

    pub fn is_bulk_import_mode(&self) -> bool {
        self.bulk_import_mode
    }

    pub fn is_pipeline_mode(&self) -> bool {
        self.pipeline_mode.load(Ordering::Acquire)
    }

    pub fn pipeline_depth(&self) -> usize {
        self.pipeline_depth
    }

    pub async fn prepare(&mut self, sql: &str) -> Result<PreparedStatement> {
        let sql_bytes = sql.as_bytes();
        let mut payload = Vec::with_capacity(4 + sql_bytes.len());
        payload.extend_from_slice(&(sql_bytes.len() as u32).to_le_bytes());
        payload.extend_from_slice(sql_bytes);

        self.send(Message::new(MSG_TYPE_PREPARE, Bytes::from(payload)))
            .await?;

        let resp = self.recv().await?;
        match resp.msg_type {
            MSG_TYPE_PREPARE => {
                if resp.payload.len() < 8 {
                    return Err(TaibaiError::Protocol("prepare response too short".into()));
                }
                let stmt_id = u32::from_le_bytes(resp.payload[..4].try_into().unwrap());
                let param_count =
                    u32::from_le_bytes(resp.payload[4..8].try_into().unwrap()) as usize;
                Ok(PreparedStatement {
                    id: stmt_id,
                    param_count,
                    sql: sql.to_string(),
                })
            }
            MSG_TYPE_ERROR => {
                let msg = String::from_utf8_lossy(&resp.payload).to_string();
                Err(TaibaiError::Sql(msg))
            }
            other => Err(TaibaiError::Protocol(format!(
                "unexpected prepare response type: {other:#04X}"
            ))),
        }
    }

    pub async fn execute_prepared(
        &mut self,
        stmt: &PreparedStatement,
        params: &[ScalarValue],
    ) -> Result<QueryResult> {
        let mut payload = Vec::with_capacity(8 + params.len() * 12);
        payload.extend_from_slice(&stmt.id.to_le_bytes());
        payload.extend_from_slice(&(params.len() as u32).to_le_bytes());

        for param in params {
            encode_scalar_param(&mut payload, param);
        }

        self.send(Message::new(MSG_TYPE_EXECUTE, Bytes::from(payload)))
            .await?;

        let resp = self.recv().await?;
        match resp.msg_type {
            MSG_TYPE_OK => {
                let rows_affected = if resp.payload.len() >= 8 {
                    u64::from_le_bytes(resp.payload[..8].try_into().unwrap())
                } else {
                    0
                };
                Ok(QueryResult::Ok {
                    rows_affected,
                    server_elapsed: None,
                    server_timings: None,
                    client_timings: None,
                })
            }
            MSG_TYPE_BINARY_QUERY_RESULT => Ok(parse_binary_result(&resp.payload)?),
            MSG_TYPE_ERROR => {
                let msg = String::from_utf8_lossy(&resp.payload).to_string();
                Ok(QueryResult::Error(msg))
            }
            other => Err(TaibaiError::Protocol(format!(
                "unexpected execute response type: {other:#04X}"
            ))),
        }
    }

    pub async fn close_stmt(&mut self, stmt: &PreparedStatement) -> Result<()> {
        let payload = Bytes::copy_from_slice(&stmt.id.to_le_bytes());
        self.send(Message::new(MSG_TYPE_CLOSE_STMT, payload))
            .await?;

        let resp = self.recv().await?;
        match resp.msg_type {
            MSG_TYPE_OK => Ok(()),
            MSG_TYPE_ERROR => {
                let msg = String::from_utf8_lossy(&resp.payload).to_string();
                Err(TaibaiError::Sql(msg))
            }
            other => Err(TaibaiError::Protocol(format!(
                "unexpected close_stmt response type: {other:#04X}"
            ))),
        }
    }

    pub async fn execute_prepared_batch(
        &mut self,
        stmt: &PreparedStatement,
        params_batch: &[Vec<ScalarValue>],
    ) -> Result<u64> {
        if params_batch.is_empty() {
            return Ok(0);
        }

        let param_count = stmt.param_count;
        let batch_size = params_batch.len();

        let mut payload = Vec::with_capacity(12 + batch_size * param_count * 12);
        payload.extend_from_slice(&stmt.id.to_le_bytes());
        payload.extend_from_slice(&(batch_size as u32).to_le_bytes());

        for params in params_batch {
            if params.len() != param_count {
                return Err(TaibaiError::Internal(format!(
                    "param count mismatch: expected {}, got {}",
                    param_count,
                    params.len()
                )));
            }
            for param in params {
                encode_scalar_param(&mut payload, param);
            }
        }

        self.send(Message::new(MSG_TYPE_BATCH_EXECUTE, Bytes::from(payload)))
            .await?;

        let resp = self.recv().await?;
        match resp.msg_type {
            MSG_TYPE_OK => {
                if resp.payload.len() >= 8 {
                    Ok(u64::from_le_bytes(resp.payload[..8].try_into().unwrap()))
                } else {
                    Ok(0)
                }
            }
            MSG_TYPE_ERROR => {
                let msg = String::from_utf8_lossy(&resp.payload).to_string();
                Err(TaibaiError::Sql(msg))
            }
            other => Err(TaibaiError::Protocol(format!(
                "unexpected batch_execute response type: {other:#04X}"
            ))),
        }
    }

    async fn send(&mut self, msg: Message) -> Result<()> {
        timeout(self.default_timeout, self.framed.send(msg))
            .await
            .map_err(|_| TaibaiError::Network("send timeout".into()))?
    }

    async fn recv(&mut self) -> Result<Message> {
        loop {
            let msg = self.next_frame().await?;
            // Interleaved server pushes are never valid request replies:
            // buffer them so every request/response flow stays intact. At the
            // cap the OLDEST buffered notification is dropped: like the
            // server's bounded push queue this is at-most-once, and the seq
            // gap still tells the subscriber what happened.
            if matches!(
                msg.msg_type,
                MSG_TYPE_NOTIFICATION | MSG_TYPE_NOTIFICATION_DELETE | MSG_TYPE_NOTIFICATION_UPDATE
            ) {
                if self.pending_notifications.len() >= MAX_BUFFERED_NOTIFICATIONS {
                    self.pending_notifications.pop_front();
                }
                self.pending_notifications.push_back(msg);
                continue;
            }
            if matches!(msg.msg_type, MSG_TYPE_SUB_DATA | MSG_TYPE_SUB_ERROR) {
                if self.pending_sub_events.len() >= MAX_BUFFERED_NOTIFICATIONS {
                    self.pending_sub_events.pop_front();
                }
                self.pending_sub_events.push_back(msg);
                continue;
            }
            return Ok(msg);
        }
    }

    async fn next_frame(&mut self) -> Result<Message> {
        if let Some(msg) = self.pending_response.take() {
            return Ok(msg);
        }
        timeout(self.default_timeout, self.framed.next())
            .await
            .map_err(|_| TaibaiError::Network("recv timeout".into()))?
            .ok_or_else(|| TaibaiError::Network("connection closed".into()))?
    }

    /// Register a change subscription. `topic` is a table in the current
    /// database (`"ticks"`), a qualified subject (`"main.ticks"`), a database
    /// wildcard (`"main.*"`), or `>` for every table. Returns the
    /// subscription id used to correlate pushed notifications and to
    /// unsubscribe.
    pub async fn subscribe(&mut self, topic: &str) -> Result<u64> {
        let mut payload = Vec::with_capacity(4 + topic.len());
        write_len_prefixed_string(&mut payload, topic);
        self.send(Message::new(MSG_TYPE_SUBSCRIBE, Bytes::from(payload)))
            .await?;
        let resp = self.recv().await?;
        match resp.msg_type {
            MSG_TYPE_OK => {
                if resp.payload.len() < 8 {
                    return Err(TaibaiError::Protocol("subscribe reply too short".into()));
                }
                Ok(u64::from_le_bytes(resp.payload[..8].try_into().unwrap()))
            }
            MSG_TYPE_ERROR => Err(TaibaiError::Sql(
                String::from_utf8_lossy(&resp.payload).to_string(),
            )),
            other => Err(TaibaiError::Protocol(format!(
                "unexpected subscribe reply type: {other:#04X}"
            ))),
        }
    }

    /// Remove a previously registered subscription.
    pub async fn unsubscribe(&mut self, sub_id: u64) -> Result<()> {
        self.send(Message::new(
            MSG_TYPE_UNSUBSCRIBE,
            Bytes::copy_from_slice(&sub_id.to_le_bytes()),
        ))
        .await?;
        let resp = self.recv().await?;
        match resp.msg_type {
            MSG_TYPE_OK => Ok(()),
            MSG_TYPE_ERROR => Err(TaibaiError::Sql(
                String::from_utf8_lossy(&resp.payload).to_string(),
            )),
            other => Err(TaibaiError::Protocol(format!(
                "unexpected unsubscribe reply type: {other:#04X}"
            ))),
        }
    }

    /// Wait for the next change notification pushed by the server. buffered
    /// notifications that arrived while awaiting request replies are returned
    /// first.
    pub async fn recv_notification(&mut self) -> Result<Notification> {
        if let Some(notification) = self.pop_buffered_notification() {
            return notification;
        }
        let msg = self.next_frame().await?;
        if matches!(
            msg.msg_type,
            MSG_TYPE_NOTIFICATION | MSG_TYPE_NOTIFICATION_DELETE | MSG_TYPE_NOTIFICATION_UPDATE
        ) {
            let msg_type = msg.msg_type;
            return parse_notification(msg_type, &msg.payload);
        }
        // Not a notification: park it so the following `recv` gets the
        // response intact instead of losing the frame.
        self.pending_response = Some(msg);
        Err(TaibaiError::Protocol(
            "no change notification pending; a response frame arrived first".into(),
        ))
    }

    /// [`recv_notification`](Self::recv_notification) bounded by a timeout;
    /// `None` means no notification arrived in time.
    pub async fn recv_notification_timeout(
        &mut self,
        dur: Duration,
    ) -> Result<Option<Notification>> {
        match timeout(dur, self.recv_notification()).await {
            Ok(result) => result.map(Some),
            Err(_) => Ok(None),
        }
    }

    fn pop_buffered_notification(&mut self) -> Option<Result<Notification>> {
        let msg = self.pending_notifications.pop_front()?;
        {
            let msg_type = msg.msg_type;
            Some(parse_notification(msg_type, &msg.payload))
        }
    }

    /// Register a reliable, resumable subscription on the exact table
    /// `db.table`. With `from_seq` the server replays every committed batch
    /// after that sequence (subject to its retention window); with `named`
    /// the server persists an ack bookmark so a later subscribe with the
    /// same name resumes where this one left off. `filter` restricts
    /// delivery to rows whose text-rendered `column` value is one of the
    /// given values.
    ///
    /// Data arrives as SUB_DATA pushes; consume them with
    /// [`recv_sub_event`](Self::recv_sub_event) and acknowledge processed
    /// sequences with [`ack_subscription`](Self::ack_subscription).
    pub async fn subscribe_v2(
        &mut self,
        db: &str,
        table: &str,
        from_seq: Option<u64>,
        named: Option<&str>,
        filter: Option<(&str, &[&str])>,
    ) -> Result<SubscriptionGrant> {
        let filter_values: Option<std::collections::HashSet<String>> =
            filter.map(|(_, values)| values.iter().map(|v| (*v).to_string()).collect());
        let payload = crate::net::pubsub_session::encode_subscribe(
            db,
            table,
            from_seq,
            named,
            filter.map(|(column, _)| column),
            filter_values.as_ref(),
        );
        self.send(Message::new(MSG_TYPE_SUBSCRIBE_V2, payload))
            .await?;
        let resp = self.recv().await?;
        match resp.msg_type {
            MSG_TYPE_SUB_CONFIRMED => {
                let grant = decode_sub_confirmed(&resp.payload)?;
                Ok(SubscriptionGrant {
                    sub_id: grant.sub_id,
                    epoch: grant.epoch,
                    granted_from: grant.granted_from,
                    floor: grant.floor,
                })
            }
            MSG_TYPE_SUB_ERROR => {
                let err = crate::net::pubsub_session::decode_sub_error(&resp.payload)?;
                Err(TaibaiError::Sql(format!(
                    "subscribe failed: {}",
                    err.message
                )))
            }
            MSG_TYPE_ERROR => Err(TaibaiError::Sql(
                String::from_utf8_lossy(&resp.payload).to_string(),
            )),
            other => Err(TaibaiError::Protocol(format!(
                "unexpected subscribe_v2 reply type: {other:#04X}"
            ))),
        }
    }

    /// Remove a reliable subscription.
    pub async fn unsubscribe_v2(&mut self, db: &str, table: &str, sub_id: u64) -> Result<()> {
        let mut payload = Vec::new();
        write_len_prefixed_string(&mut payload, db);
        write_len_prefixed_string(&mut payload, table);
        payload.extend_from_slice(&sub_id.to_le_bytes());
        self.send(Message::new(MSG_TYPE_UNSUBSCRIBE_V2, Bytes::from(payload)))
            .await?;
        self.expect_ok("unsubscribe_v2").await
    }

    /// Acknowledge every sequence up to `seq` as processed, advancing the
    /// subscription's resumable bookmark (flushed periodically for named
    /// subscriptions).
    pub async fn ack_subscription(
        &mut self,
        db: &str,
        table: &str,
        sub_id: u64,
        seq: u64,
    ) -> Result<()> {
        let mut payload = Vec::new();
        write_len_prefixed_string(&mut payload, db);
        write_len_prefixed_string(&mut payload, table);
        payload.extend_from_slice(&sub_id.to_le_bytes());
        payload.extend_from_slice(&seq.to_le_bytes());
        self.send(Message::new(MSG_TYPE_SUB_ACK, Bytes::from(payload)))
            .await?;
        self.expect_ok("ack_subscription").await
    }

    /// Wait for the next reliable-subscription event: a data push or a
    /// resync notice (the retained window no longer covers this
    /// subscription; resubscribe from the reported floor). Buffered events
    /// that arrived while awaiting request replies are returned first.
    pub async fn recv_sub_event(&mut self) -> Result<SubEvent> {
        if let Some(event) = self.pop_buffered_sub_event() {
            return event;
        }
        let msg = self.next_frame().await?;
        if matches!(msg.msg_type, MSG_TYPE_SUB_DATA | MSG_TYPE_SUB_ERROR) {
            let msg_type = msg.msg_type;
            return sub_event_from_message(msg_type, &msg.payload);
        }
        // Not a subscription event: park it so the following `recv` gets
        // the response intact instead of losing the frame.
        self.pending_response = Some(msg);
        Err(TaibaiError::Protocol(
            "no subscription event pending; a response frame arrived first".into(),
        ))
    }

    /// [`recv_sub_event`](Self::recv_sub_event) bounded by a timeout;
    /// `None` means no event arrived in time.
    pub async fn recv_sub_event_timeout(&mut self, dur: Duration) -> Result<Option<SubEvent>> {
        match timeout(dur, self.recv_sub_event()).await {
            Ok(result) => result.map(Some),
            Err(_) => Ok(None),
        }
    }

    fn pop_buffered_sub_event(&mut self) -> Option<Result<SubEvent>> {
        let msg = self.pending_sub_events.pop_front()?;
        let msg_type = msg.msg_type;
        Some(sub_event_from_message(msg_type, &msg.payload))
    }

    async fn expect_ok(&mut self, what: &str) -> Result<()> {
        let resp = self.recv().await?;
        match resp.msg_type {
            MSG_TYPE_OK => Ok(()),
            MSG_TYPE_ERROR => Err(TaibaiError::Sql(
                String::from_utf8_lossy(&resp.payload).to_string(),
            )),
            other => Err(TaibaiError::Protocol(format!(
                "unexpected {what} response type: {other:#04X}"
            ))),
        }
    }
}

fn parse_binary_result(payload: &[u8]) -> Result<QueryResult> {
    if payload.len() < 4 {
        return Err(TaibaiError::Protocol(
            "binary result payload too short".into(),
        ));
    }
    let mut offset = 0usize;
    let col_count = u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap()) as usize;
    offset += 4;
    // A corrupt or hostile frame must surface as a protocol error, not as a
    // multi-gigabyte allocation that aborts the client process.
    if col_count > MAX_BINARY_RESULT_COLUMNS {
        return Err(TaibaiError::Protocol(format!(
            "binary result column count {col_count} exceeds the maximum of {MAX_BINARY_RESULT_COLUMNS}"
        )));
    }

    let mut columns = Vec::with_capacity(col_count);
    for _ in 0..col_count {
        let (name, rest) = read_len_prefixed_string(&payload[offset..])?;
        if rest.len() < 3 {
            return Err(TaibaiError::Protocol(
                "truncated binary column metadata".into(),
            ));
        }
        let data_type = DataType::from_u8(rest[0])?;
        let scale = rest[1] as i32;
        let precision = rest[2] as i32;
        columns.push(ColumnDef {
            name: name.to_string(),
            data_type,
            nullable: true,
            scale,
            precision,
            auto_increment: false,
            primary_key: false,
            default_value: None,
            column_id: 0,
            previous_names: Vec::new(),
            added_at_micros: None,
        });
        offset += 4 + name.len() + 3;
    }

    if payload.len() < offset + 4 {
        return Err(TaibaiError::Protocol(
            "binary result missing row count".into(),
        ));
    }
    let row_count = u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap()) as usize;
    offset += 4;
    if row_count as u64 > (payload.len() as u64).saturating_mul(MAX_BINARY_RESULT_ROWS_PER_BYTE) {
        return Err(TaibaiError::Protocol(format!(
            "binary result row count {row_count} is inconsistent with payload size {}",
            payload.len()
        )));
    }

    let schema = Schema::new(columns);
    let mut arrays = Vec::with_capacity(schema.num_columns());
    for col_def in &schema.columns {
        if payload.len() < offset + 4 {
            return Err(TaibaiError::Protocol(
                "binary result missing null bitmap length".into(),
            ));
        }
        let bitmap_len =
            u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;
        if payload.len() < offset + bitmap_len + 4 {
            return Err(TaibaiError::Protocol(
                "binary result null bitmap truncated".into(),
            ));
        }
        let nulls = &payload[offset..offset + bitmap_len];
        offset += bitmap_len;

        let data_len = u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;
        if payload.len() < offset + data_len {
            return Err(TaibaiError::Protocol(
                "binary result column data truncated".into(),
            ));
        }
        let data = &payload[offset..offset + data_len];
        offset += data_len;
        arrays.push(decode_binary_column(
            col_def.data_type,
            data,
            nulls,
            row_count,
        )?);
    }

    let server_timings = parse_server_timing_trailer(payload, offset);
    Ok(QueryResult::Rows {
        schema: schema.clone(),
        batches: vec![RecordBatch::try_new(schema, arrays)?],
        server_elapsed: server_timings.as_ref().map(|t| t.total),
        server_timings,
        client_timings: None,
    })
}

fn attach_client_timings(result: &mut QueryResult, timings: ClientTimings) {
    match result {
        QueryResult::Ok { client_timings, .. } | QueryResult::Rows { client_timings, .. } => {
            *client_timings = Some(timings);
        }
        QueryResult::Error(_) => {}
    }
}

/// Decode a fixed-width column in one pass: build the typed vector directly
/// from the wire bytes instead of materializing a `ScalarValue` per row and
/// pushing it (which copied every value twice).
fn decode_fixed_width_column(
    data_type: DataType,
    data: &[u8],
    nulls: &[u8],
    row_count: usize,
    width: usize,
) -> Result<ColumnArray> {
    if data.len() < row_count * width {
        return Err(TaibaiError::Protocol(
            "binary result column data truncated".into(),
        ));
    }
    let mut bitmap = crate::types::bitmap::NullBitmap::new_valid(row_count);
    for row in 0..row_count {
        if nulls.get(row).copied().unwrap_or(0) != 0 {
            bitmap.set_null(row);
        }
    }
    macro_rules! decode_le {
        ($variant:ident, $t:ty) => {{
            let mut new_data = Vec::with_capacity(row_count);
            for row in 0..row_count {
                let off = row * width;
                let mut bytes = [0u8; std::mem::size_of::<$t>()];
                bytes.copy_from_slice(&data[off..off + width]);
                new_data.push(<$t>::from_le_bytes(bytes));
            }
            return Ok(ColumnArray::$variant {
                data: new_data,
                bitmap,
            });
        }};
    }
    match data_type {
        DataType::Bool => {
            let mut new_data = Vec::with_capacity(row_count);
            for row in 0..row_count {
                new_data.push(u8::from(data[row * width] != 0));
            }
            Ok(ColumnArray::Bool {
                data: new_data,
                bitmap,
            })
        }
        DataType::Int8 => {
            let mut new_data = Vec::with_capacity(row_count);
            for row in 0..row_count {
                new_data.push(data[row * width] as i8);
            }
            Ok(ColumnArray::Int8 {
                data: new_data,
                bitmap,
            })
        }
        DataType::UInt8 => {
            let mut new_data = Vec::with_capacity(row_count);
            for row in 0..row_count {
                new_data.push(data[row * width]);
            }
            Ok(ColumnArray::UInt8 {
                data: new_data,
                bitmap,
            })
        }
        DataType::Int16 => decode_le!(Int16, i16),
        DataType::Int32 => decode_le!(Int32, i32),
        DataType::Int64 => decode_le!(Int64, i64),
        DataType::UInt16 => decode_le!(UInt16, u16),
        DataType::UInt32 => decode_le!(UInt32, u32),
        DataType::UInt64 => decode_le!(UInt64, u64),
        DataType::Float32 => decode_le!(Float32, f32),
        DataType::Float64 => decode_le!(Float64, f64),
        DataType::Decimal => decode_le!(Decimal, i64),
        DataType::Date => decode_le!(Date, i32),
        DataType::Time => decode_le!(Time, i64),
        DataType::Timestamp => decode_le!(Timestamp, i64),
        DataType::Interval => decode_le!(Interval, i64),
        _ => unreachable!("variable-width types are handled by the caller"),
    }
}

fn decode_binary_column(
    data_type: DataType,
    data: &[u8],
    nulls: &[u8],
    row_count: usize,
) -> Result<ColumnArray> {
    // Fixed-width columns decode in one bulk pass; only variable-width
    // columns keep the per-row ScalarValue round-trip.
    if let Some(width) = data_type.fixed_size() {
        return decode_fixed_width_column(data_type, data, nulls, row_count, width);
    }
    let mut col = ColumnArray::empty(data_type);
    let mut offset = 0usize;
    for row in 0..row_count {
        let is_null = nulls.get(row).copied().unwrap_or(0) != 0;
        let value = if is_null {
            ScalarValue::null_for_type(data_type)
        } else {
            let (value, consumed) =
                read_scalar_value(data.get(offset..).unwrap_or(&[]), data_type, 0)?;
            offset += consumed;
            value
        };

        if is_null {
            offset += data_type.fixed_size().unwrap_or(4);
        }
        col.push(&value)?;
    }
    Ok(col)
}

fn parse_server_timing_trailer(payload: &[u8], offset: usize) -> Option<ServerTimings> {
    if let Some(trailer) = payload.get(offset..offset + 28) {
        if &trailer[..4] == b"MXTP" {
            let total = u64::from_le_bytes(trailer[4..12].try_into().ok()?);
            let execute = u64::from_le_bytes(trailer[12..20].try_into().ok()?);
            let encode = u64::from_le_bytes(trailer[20..28].try_into().ok()?);
            return Some(ServerTimings {
                total: Duration::from_micros(total),
                execute: Some(Duration::from_micros(execute)),
                encode: Some(Duration::from_micros(encode)),
            });
        }
    }

    parse_server_elapsed_trailer(payload, offset).map(|total| ServerTimings {
        total,
        execute: None,
        encode: None,
    })
}

fn parse_server_elapsed_trailer(payload: &[u8], offset: usize) -> Option<Duration> {
    let trailer = payload.get(offset..offset + 12)?;
    if &trailer[..4] != b"MXTM" {
        return None;
    }
    let micros = u64::from_le_bytes(trailer[4..12].try_into().ok()?);
    Some(Duration::from_micros(micros))
}

fn read_scalar_value(data: &[u8], dt: DataType, scale: i32) -> Result<(ScalarValue, usize)> {
    match dt {
        DataType::Bool => {
            if data.is_empty() {
                return Err(TaibaiError::Protocol("no data for Bool".into()));
            }
            Ok((ScalarValue::Bool(Some(data[0] != 0)), 1))
        }
        DataType::Int8 => {
            if data.is_empty() {
                return Err(TaibaiError::Protocol("no data for Int8".into()));
            }
            Ok((ScalarValue::Int8(Some(data[0] as i8)), 1))
        }
        DataType::Int16 => {
            if data.len() < 2 {
                return Err(TaibaiError::Protocol("no data for Int16".into()));
            }
            Ok((
                ScalarValue::Int16(Some(i16::from_le_bytes(data[..2].try_into().unwrap()))),
                2,
            ))
        }
        DataType::Int32 => {
            if data.len() < 4 {
                return Err(TaibaiError::Protocol("no data for Int32".into()));
            }
            Ok((
                ScalarValue::Int32(Some(i32::from_le_bytes(data[..4].try_into().unwrap()))),
                4,
            ))
        }
        DataType::Int64 => {
            if data.len() < 8 {
                return Err(TaibaiError::Protocol("no data for Int64".into()));
            }
            Ok((
                ScalarValue::Int64(Some(i64::from_le_bytes(data[..8].try_into().unwrap()))),
                8,
            ))
        }
        DataType::UInt8 => {
            if data.is_empty() {
                return Err(TaibaiError::Protocol("no data for UInt8".into()));
            }
            Ok((ScalarValue::UInt8(Some(data[0])), 1))
        }
        DataType::UInt16 => {
            if data.len() < 2 {
                return Err(TaibaiError::Protocol("no data for UInt16".into()));
            }
            Ok((
                ScalarValue::UInt16(Some(u16::from_le_bytes(data[..2].try_into().unwrap()))),
                2,
            ))
        }
        DataType::UInt32 => {
            if data.len() < 4 {
                return Err(TaibaiError::Protocol("no data for UInt32".into()));
            }
            Ok((
                ScalarValue::UInt32(Some(u32::from_le_bytes(data[..4].try_into().unwrap()))),
                4,
            ))
        }
        DataType::UInt64 => {
            if data.len() < 8 {
                return Err(TaibaiError::Protocol("no data for UInt64".into()));
            }
            Ok((
                ScalarValue::UInt64(Some(u64::from_le_bytes(data[..8].try_into().unwrap()))),
                8,
            ))
        }
        DataType::Float32 => {
            if data.len() < 4 {
                return Err(TaibaiError::Protocol("no data for Float32".into()));
            }
            Ok((
                ScalarValue::Float32(Some(f32::from_le_bytes(data[..4].try_into().unwrap()))),
                4,
            ))
        }
        DataType::Float64 => {
            if data.len() < 8 {
                return Err(TaibaiError::Protocol("no data for Float64".into()));
            }
            Ok((
                ScalarValue::Float64(Some(f64::from_le_bytes(data[..8].try_into().unwrap()))),
                8,
            ))
        }
        DataType::Decimal => {
            if scale > 0 {
                if data.len() < 4 {
                    return Err(TaibaiError::Protocol("no data for Decimal".into()));
                }
                let len = u32::from_le_bytes(data[..4].try_into().unwrap()) as usize;
                if data.len() < 4 + len {
                    return Err(TaibaiError::Protocol("Decimal data truncated".into()));
                }
                let s = std::str::from_utf8(&data[4..4 + len]).map_err(|e| {
                    TaibaiError::Protocol(format!("invalid UTF-8 in Decimal: {}", e))
                })?;
                let value = parse_scaled_decimal_text(s, scale)?;
                return Ok((ScalarValue::Decimal(Some(value)), 4 + len));
            }
            if data.len() < 8 {
                return Err(TaibaiError::Protocol("no data for Decimal".into()));
            }
            Ok((
                ScalarValue::Decimal(Some(i64::from_le_bytes(data[..8].try_into().unwrap()))),
                8,
            ))
        }
        DataType::Date => {
            if data.len() < 4 {
                return Err(TaibaiError::Protocol("no data for Date".into()));
            }
            Ok((
                ScalarValue::Date(Some(i32::from_le_bytes(data[..4].try_into().unwrap()))),
                4,
            ))
        }
        DataType::Time => {
            if data.len() < 8 {
                return Err(TaibaiError::Protocol("no data for Time".into()));
            }
            Ok((
                ScalarValue::Time(Some(i64::from_le_bytes(data[..8].try_into().unwrap()))),
                8,
            ))
        }
        DataType::Timestamp => {
            if data.len() < 8 {
                return Err(TaibaiError::Protocol("no data for Timestamp".into()));
            }
            Ok((
                ScalarValue::Timestamp(Some(i64::from_le_bytes(data[..8].try_into().unwrap()))),
                8,
            ))
        }
        DataType::Interval => {
            if data.len() < 8 {
                return Err(TaibaiError::Protocol("no data for Interval".into()));
            }
            Ok((
                ScalarValue::Interval(Some(i64::from_le_bytes(data[..8].try_into().unwrap()))),
                8,
            ))
        }
        DataType::VarChar => {
            if data.len() < 4 {
                return Err(TaibaiError::Protocol("no data for VarChar".into()));
            }
            let len = u32::from_le_bytes(data[..4].try_into().unwrap()) as usize;
            if data.len() < 4 + len {
                return Err(TaibaiError::Protocol("VarChar data truncated".into()));
            }
            let s = String::from_utf8_lossy(&data[4..4 + len]).to_string();
            Ok((ScalarValue::VarChar(Some(s)), 4 + len))
        }
        DataType::Blob => {
            if data.len() < 4 {
                return Err(TaibaiError::Protocol("no data for Blob".into()));
            }
            let len = u32::from_le_bytes(data[..4].try_into().unwrap()) as usize;
            if data.len() < 4 + len {
                return Err(TaibaiError::Protocol("Blob data truncated".into()));
            }
            let b = data[4..4 + len].to_vec();
            Ok((ScalarValue::Blob(Some(b)), 4 + len))
        }
        DataType::Vector => {
            if data.len() < 4 {
                return Err(TaibaiError::Protocol("no data for Vector".into()));
            }
            let len = u32::from_le_bytes(data[..4].try_into().unwrap()) as usize;
            let total_bytes = 4 + len * 4;
            if data.len() < total_bytes {
                return Err(TaibaiError::Protocol("Vector data truncated".into()));
            }
            let mut floats = Vec::with_capacity(len);
            for i in 0..len {
                let start = 4 + i * 4;
                floats.push(f32::from_le_bytes(
                    data[start..start + 4].try_into().unwrap(),
                ));
            }
            Ok((ScalarValue::Vector(Some(floats)), total_bytes))
        }
    }
}

fn parse_scaled_decimal_text(s: &str, scale: i32) -> Result<i64> {
    let s = s.trim();
    if s.is_empty() {
        return Err(TaibaiError::Protocol("empty Decimal value".into()));
    }

    let negative = s.starts_with('-');
    let unsigned = if negative || s.starts_with('+') {
        &s[1..]
    } else {
        s
    };
    let mut parts = unsigned.splitn(2, '.');
    let int_part = parts.next().unwrap_or("");
    let frac_part = parts.next().unwrap_or("");

    if int_part.is_empty() && frac_part.is_empty() {
        return Err(TaibaiError::Protocol(format!("invalid Decimal '{}'", s)));
    }
    if !int_part.chars().all(|c| c.is_ascii_digit())
        || !frac_part.chars().all(|c| c.is_ascii_digit())
    {
        return Err(TaibaiError::Protocol(format!("invalid Decimal '{}'", s)));
    }

    let multiplier = 10_i64
        .checked_pow(scale as u32)
        .ok_or_else(|| TaibaiError::Protocol(format!("Decimal scale {} overflows", scale)))?;
    let int_value = if int_part.is_empty() {
        0
    } else {
        int_part
            .parse::<i64>()
            .map_err(|e| TaibaiError::Protocol(format!("invalid Decimal '{}': {}", s, e)))?
    };

    let mut frac = frac_part.to_string();
    let scale_usize = scale as usize;
    if frac.len() > scale_usize {
        return Err(TaibaiError::Protocol(format!(
            "Decimal '{}' exceeds scale {}",
            s, scale
        )));
    }
    while frac.len() < scale_usize {
        frac.push('0');
    }
    let frac_value = if frac.is_empty() {
        0
    } else {
        frac.parse::<i64>()
            .map_err(|e| TaibaiError::Protocol(format!("invalid Decimal '{}': {}", s, e)))?
    };

    let magnitude = int_value
        .checked_mul(multiplier)
        .and_then(|v| v.checked_add(frac_value))
        .ok_or_else(|| TaibaiError::Protocol(format!("Decimal '{}' overflows", s)))?;
    Ok(if negative { -magnitude } else { magnitude })
}

#[cfg(test)]
#[path = "client_test.rs"]
mod tests;

#[cfg(test)]
#[path = "client_retry_test.rs"]
mod retry_tests;

fn encode_scalar_param(buf: &mut Vec<u8>, val: &ScalarValue) {
    match val {
        ScalarValue::Bool(Some(v)) => {
            buf.push(DataType::Bool as u8);
            buf.push(if *v { 1 } else { 0 });
        }
        ScalarValue::Bool(None) => buf.push(0u8),

        ScalarValue::Int8(Some(v)) => {
            buf.push(DataType::Int8 as u8);
            buf.push(*v as u8);
        }
        ScalarValue::Int8(None) => buf.push(0u8),

        ScalarValue::Int16(Some(v)) => {
            buf.push(DataType::Int16 as u8);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        ScalarValue::Int16(None) => buf.push(0u8),

        ScalarValue::Int32(Some(v)) => {
            buf.push(DataType::Int32 as u8);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        ScalarValue::Int32(None) => buf.push(0u8),

        ScalarValue::Int64(Some(v)) => {
            buf.push(DataType::Int64 as u8);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        ScalarValue::Int64(None) => buf.push(0u8),

        ScalarValue::UInt8(Some(v)) => {
            buf.push(DataType::UInt8 as u8);
            buf.push(*v);
        }
        ScalarValue::UInt8(None) => buf.push(0u8),

        ScalarValue::UInt16(Some(v)) => {
            buf.push(DataType::UInt16 as u8);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        ScalarValue::UInt16(None) => buf.push(0u8),

        ScalarValue::UInt32(Some(v)) => {
            buf.push(DataType::UInt32 as u8);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        ScalarValue::UInt32(None) => buf.push(0u8),

        ScalarValue::UInt64(Some(v)) => {
            buf.push(DataType::UInt64 as u8);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        ScalarValue::UInt64(None) => buf.push(0u8),

        ScalarValue::Float32(Some(v)) => {
            buf.push(DataType::Float32 as u8);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        ScalarValue::Float32(None) => buf.push(0u8),

        ScalarValue::Float64(Some(v)) => {
            buf.push(DataType::Float64 as u8);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        ScalarValue::Float64(None) => buf.push(0u8),

        ScalarValue::Decimal(Some(v)) => {
            buf.push(DataType::Decimal as u8);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        ScalarValue::Decimal(None) => buf.push(0u8),

        ScalarValue::Date(Some(v)) => {
            buf.push(DataType::Date as u8);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        ScalarValue::Date(None) => buf.push(0u8),

        ScalarValue::Time(Some(v)) => {
            buf.push(DataType::Time as u8);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        ScalarValue::Time(None) => buf.push(0u8),

        ScalarValue::Timestamp(Some(v)) => {
            buf.push(DataType::Timestamp as u8);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        ScalarValue::Timestamp(None) => buf.push(0u8),

        ScalarValue::Interval(Some(v)) => {
            buf.push(DataType::Interval as u8);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        ScalarValue::Interval(None) => buf.push(0u8),

        ScalarValue::VarChar(Some(v)) => {
            buf.push(DataType::VarChar as u8);
            let bytes = v.as_bytes();
            buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(bytes);
        }
        ScalarValue::VarChar(None) => buf.push(0u8),

        ScalarValue::Blob(Some(v)) => {
            buf.push(DataType::Blob as u8);
            buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
            buf.extend_from_slice(v);
        }
        ScalarValue::Blob(None) => buf.push(0u8),

        ScalarValue::Vector(Some(v)) => {
            buf.push(DataType::Vector as u8);
            buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
            for f in v {
                buf.extend_from_slice(&f.to_le_bytes());
            }
        }
        ScalarValue::Vector(None) => buf.push(0u8),
    }
}

/// A server-pushed change notification for one committed insert batch.
///
/// `seq` is a per-topic monotonic counter: a gap against the previous
/// notification means the server dropped events for this subscription
/// (slow consumer), and the client should re-read the table to catch up.
#[derive(Debug, Clone)]
pub struct Notification {
    pub sub_id: u64,
    pub seq: u64,
    pub topic: String,
    /// What the event carries: inserted rows, deleted rows, or post-update
    /// rows. Older servers only push inserts.
    pub kind: NotificationKind,
    pub schema: Schema,
    pub batches: Vec<RecordBatch>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotificationKind {
    Insert,
    Delete,
    Update,
}

impl Notification {
    pub fn num_rows(&self) -> usize {
        self.batches.iter().map(|batch| batch.num_rows()).sum()
    }
}

/// Payload layout (MSG_TYPE_NOTIFICATION):
/// `[u64 sub_id][u64 seq][string topic][binary result batches]`
fn parse_notification(msg_type: u8, payload: &[u8]) -> Result<Notification> {
    if payload.len() < 16 {
        return Err(TaibaiError::Protocol(
            "notification payload too short".into(),
        ));
    }
    let sub_id = u64::from_le_bytes(payload[..8].try_into().unwrap());
    let seq = u64::from_le_bytes(payload[8..16].try_into().unwrap());
    let (topic, rest) = read_len_prefixed_string(&payload[16..])?;
    // The fan-out thread compresses the encoded result section (only the
    // section, never the outer payload) once it exceeds the compression
    // threshold. The outer payload starts with sub_id bytes, so the codec's
    // zlib sniff never fires; decompress the section here. A section that
    // merely starts with 0x78 by coincidence fails decompression and is
    // parsed as-is.
    let rest_section: Option<Bytes> = match rest.first() {
        Some(&0x78) => crate::protocol::compress::zlib_decompress_complete(rest).map(Bytes::from),
        _ => None,
    };
    let rest: &[u8] = match &rest_section {
        Some(bytes) => bytes.as_ref(),
        None => rest,
    };
    let QueryResult::Rows {
        schema, batches, ..
    } = parse_binary_result(rest)?
    else {
        return Err(TaibaiError::Protocol(
            "notification body did not decode to rows".into(),
        ));
    };
    let kind = match msg_type {
        crate::protocol::message::MSG_TYPE_NOTIFICATION_DELETE => NotificationKind::Delete,
        crate::protocol::message::MSG_TYPE_NOTIFICATION_UPDATE => NotificationKind::Update,
        _ => NotificationKind::Insert,
    };
    Ok(Notification {
        sub_id,
        seq,
        topic: topic.to_string(),
        kind,
        schema,
        batches,
    })
}

/// Server confirmation for a reliable subscription
/// ([`Client::subscribe_v2`](Client::subscribe_v2)).
#[derive(Debug, Clone)]
pub struct SubscriptionGrant {
    pub sub_id: u64,
    /// Server pub/sub epoch; a changed epoch means the server restarted and
    /// sequence numbers reset.
    pub epoch: u64,
    /// Last sequence already covered by this subscription (the requested
    /// `from_seq`, the current head for a from-now subscribe, or the named
    /// bookmark). Delivery starts at `granted_from + 1`.
    pub granted_from: u64,
    /// Oldest sequence still retained; resume requests below it are refused.
    pub floor: u64,
}

/// One reliable-subscription event.
#[derive(Debug)]
pub enum SubEvent {
    /// Committed batches, carrying contiguous sequences
    /// `seq_start..=seq_end`. Acknowledge processed sequences with
    /// [`Client::ack_subscription`](Client::ack_subscription).
    Data(crate::net::pubsub_session::SubDataFrame),
    /// The retained window no longer covers this subscription; resubscribe
    /// with `from_seq = floor - 1` (or later) and re-read the table for the
    /// missing range.
    ResyncRequired { sub_id: u64, floor: u64 },
}

fn sub_event_from_message(msg_type: u8, payload: &[u8]) -> Result<SubEvent> {
    match msg_type {
        MSG_TYPE_SUB_DATA => Ok(SubEvent::Data(crate::net::pubsub_session::decode_sub_data(
            payload,
        )?)),
        MSG_TYPE_SUB_ERROR => {
            let err = crate::net::pubsub_session::decode_sub_error(payload)?;
            if err.code == crate::net::pubsub_session::SUB_ERROR_RESYNC_REQUIRED {
                Ok(SubEvent::ResyncRequired {
                    sub_id: err.sub_id,
                    floor: err.aux,
                })
            } else {
                Err(TaibaiError::Protocol(format!(
                    "subscription {} error {}: {}",
                    err.sub_id, err.code, err.message
                )))
            }
        }
        other => Err(TaibaiError::Protocol(format!(
            "not a subscription event frame: {other:#04X}"
        ))),
    }
}
