#![allow(dead_code)]
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
use crate::protocol::codec::MessageCodec;
use crate::protocol::message::*;
use crate::protocol::types::DataType;
use crate::types::array::ColumnArray;
use crate::types::batch::{ColumnDef, RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

#[allow(dead_code)]
pub struct BulkImportConfig {
    pub disable_wal: bool,
}

#[allow(dead_code)]
pub struct PipelineStats {
    pub sent: i64,
    pub completed: i64,
    pub errors: i64,
}

#[allow(dead_code)]
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
        let stream = TcpStream::connect(addr).await?;
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
        payload.push(1);
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

    pub async fn query(&mut self, sql: &str) -> Result<QueryResult> {
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
            MSG_TYPE_RESULT => Ok(parse_text_result(&resp.payload)?),
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
                MSG_TYPE_RESULT => {
                    let QueryResult::Rows {
                        schema,
                        batches,
                        server_timings,
                        ..
                    } = parse_text_result(&response.payload)?
                    else {
                        unreachable!()
                    };
                    for batch in batches {
                        on_batch(&schema, batch)?;
                    }
                    return Ok(server_timings);
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

    pub async fn start_bulk_import(&mut self, config: &BulkImportConfig) -> Result<()> {
        let config_json = if config.disable_wal {
            "{\"disable_wal\":true}"
        } else {
            "{\"disable_wal\":false}"
        };
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
        self.recv().await?;
        Ok(())
    }

    pub fn get_pipeline_stats(&self) -> PipelineStats {
        PipelineStats {
            sent: self.pending_count.load(Ordering::Relaxed),
            completed: self.response_handled.load(Ordering::Relaxed),
            errors: 0,
        }
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

    pub async fn recv_response(&mut self) -> Result<Message> {
        self.recv().await
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
            MSG_TYPE_RESULT => Ok(parse_text_result(&resp.payload)?),
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
        timeout(self.default_timeout, self.framed.next())
            .await
            .map_err(|_| TaibaiError::Network("recv timeout".into()))?
            .ok_or_else(|| TaibaiError::Network("connection closed".into()))?
    }
}

fn parse_text_result(payload: &[u8]) -> Result<QueryResult> {
    if payload.len() < 4 {
        return Err(TaibaiError::Protocol("result payload too short".into()));
    }
    let mut offset = 0usize;

    let col_count = u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap()) as usize;
    offset += 4;

    let mut columns = Vec::with_capacity(col_count);
    for _ in 0..col_count {
        let (name, rest) = read_len_prefixed_string(&payload[offset..])?;
        if rest.len() < 3 {
            return Err(TaibaiError::Protocol("truncated column metadata".into()));
        }
        let dtype_byte = rest[0];
        let scale = rest[1] as i32;
        let precision = rest[2] as i32;
        let data_type = DataType::from_u8(dtype_byte)?;
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
        });
        offset += 4 + name.len() + 3;
    }

    if payload.len() < offset + 4 {
        return Err(TaibaiError::Protocol("result missing row count".into()));
    }
    let row_count = u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap()) as usize;
    offset += 4;

    let schema = Schema::new(columns);

    if row_count == 0 {
        let server_timings = parse_server_timing_trailer(payload, offset);
        return Ok(QueryResult::Rows {
            schema: schema.clone(),
            batches: vec![RecordBatch::new(schema)],
            server_elapsed: server_timings.as_ref().map(|t| t.total),
            server_timings,
            client_timings: None,
        });
    }

    let mut arrays: Vec<ColumnArray> = schema
        .columns
        .iter()
        .map(|col| ColumnArray::empty(col.data_type))
        .collect();

    for _ in 0..row_count {
        for (col_idx, col) in schema.columns.iter().enumerate() {
            if offset >= payload.len() {
                return Err(TaibaiError::Protocol("truncated row data".into()));
            }
            let is_null = payload[offset] != 0;
            offset += 1;

            let dt = col.data_type;
            let value = if is_null {
                ScalarValue::null_for_type(dt)
            } else {
                let scale = col.scale;
                let (val, consumed) = read_scalar_value(&payload[offset..], dt, scale)?;
                offset += consumed;
                val
            };
            arrays[col_idx].push(&value)?;
        }
    }

    let batch = RecordBatch::try_new(schema.clone(), arrays)?;
    let server_timings = parse_server_timing_trailer(payload, offset);
    Ok(QueryResult::Rows {
        schema,
        batches: vec![batch],
        server_elapsed: server_timings.as_ref().map(|t| t.total),
        server_timings,
        client_timings: None,
    })
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

fn decode_binary_column(
    data_type: DataType,
    data: &[u8],
    nulls: &[u8],
    row_count: usize,
) -> Result<ColumnArray> {
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
