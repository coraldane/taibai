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

use crate::common::error::{MonaxError, Result};
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
pub enum QueryResult {
    Ok {
        rows_affected: u64,
    },
    Rows {
        schema: Schema,
        batches: Vec<RecordBatch>,
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
        payload.push(0);
        payload.extend_from_slice(&timezone_offset_hours.to_le_bytes());

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
                Err(MonaxError::Auth(msg))
            }
            other => Err(MonaxError::Protocol(format!(
                "unexpected login response type: {other:#04X}"
            ))),
        }
    }

    pub async fn ping(&mut self) -> Result<Duration> {
        let start = tokio::time::Instant::now();
        self.send(Message::empty(MSG_TYPE_PING)).await?;
        let resp = self.recv().await?;
        if resp.msg_type != MSG_TYPE_PONG {
            return Err(MonaxError::Protocol(format!(
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

        self.send(Message::new(MSG_TYPE_QUERY, Bytes::from(payload)))
            .await?;

        let resp = self.recv().await?;
        match resp.msg_type {
            MSG_TYPE_OK => {
                if resp.payload.len() < 8 {
                    return Err(MonaxError::Protocol("OK payload too short".into()));
                }
                let rows_affected = u64::from_le_bytes(resp.payload[..8].try_into().unwrap());
                Ok(QueryResult::Ok { rows_affected })
            }
            MSG_TYPE_RESULT => Ok(parse_text_result(&resp.payload)?),
            MSG_TYPE_ERROR => {
                let msg = String::from_utf8_lossy(&resp.payload).to_string();
                Ok(QueryResult::Error(msg))
            }
            other => Err(MonaxError::Protocol(format!(
                "unexpected query response type: {other:#04X}"
            ))),
        }
    }

    pub async fn execute(&mut self, sql: &str) -> Result<u64> {
        match self.query(sql).await? {
            QueryResult::Ok { rows_affected } => Ok(rows_affected),
            QueryResult::Error(msg) => Err(MonaxError::Sql(msg)),
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
                    return Err(MonaxError::Protocol("OK payload too short".into()));
                }
                Ok(u64::from_le_bytes(resp.payload[..8].try_into().unwrap()))
            }
            MSG_TYPE_ERROR => Err(MonaxError::Sql(
                String::from_utf8_lossy(&resp.payload).to_string(),
            )),
            other => Err(MonaxError::Protocol(format!(
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
                    return Err(MonaxError::Protocol("OK payload too short".into()));
                }
                Ok(u64::from_le_bytes(resp.payload[..8].try_into().unwrap()))
            }
            MSG_TYPE_ERROR => Err(MonaxError::Storage(
                String::from_utf8_lossy(&resp.payload).to_string(),
            )),
            other => Err(MonaxError::Protocol(format!(
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
                    return Err(MonaxError::Protocol("OK payload too short".into()));
                }
                Ok(u64::from_le_bytes(resp.payload[..8].try_into().unwrap()))
            }
            MSG_TYPE_ERROR => Err(MonaxError::Storage(
                String::from_utf8_lossy(&resp.payload).to_string(),
            )),
            other => Err(MonaxError::Protocol(format!(
                "unexpected flush response type: {other:#04X}"
            ))),
        }
    }

    pub async fn start_pipeline(&mut self, depth: usize) -> Result<()> {
        if self.pipeline_mode.load(Ordering::Acquire) {
            return Err(MonaxError::Internal("pipeline already started".into()));
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
            return Err(MonaxError::Internal("pipeline not started".into()));
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
            MSG_TYPE_ERROR => Err(MonaxError::Storage(
                String::from_utf8_lossy(&resp.payload).to_string(),
            )),
            other => Err(MonaxError::Protocol(format!(
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

    async fn send(&mut self, msg: Message) -> Result<()> {
        timeout(self.default_timeout, self.framed.send(msg))
            .await
            .map_err(|_| MonaxError::Network("send timeout".into()))?
    }

    async fn recv(&mut self) -> Result<Message> {
        timeout(self.default_timeout, self.framed.next())
            .await
            .map_err(|_| MonaxError::Network("recv timeout".into()))?
            .ok_or_else(|| MonaxError::Network("connection closed".into()))?
    }
}

fn parse_text_result(payload: &[u8]) -> Result<QueryResult> {
    if payload.len() < 4 {
        return Err(MonaxError::Protocol("result payload too short".into()));
    }
    let mut offset = 0usize;

    let col_count = u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap()) as usize;
    offset += 4;

    let mut columns = Vec::with_capacity(col_count);
    for _ in 0..col_count {
        let (name, rest) = read_len_prefixed_string(&payload[offset..])?;
        if rest.len() < 5 {
            return Err(MonaxError::Protocol("truncated column metadata".into()));
        }
        let dtype_byte = rest[0];
        let scale = u32::from_le_bytes(rest[1..5].try_into().unwrap());
        let data_type = DataType::from_u8(dtype_byte)?;
        columns.push(ColumnDef {
            name: name.to_string(),
            data_type,
            nullable: true,
            scale: scale as i32,
            precision: 0,
            auto_increment: false,
        });
        offset += 4 + name.len() + 5;
    }

    if payload.len() < offset + 4 {
        return Err(MonaxError::Protocol("result missing row count".into()));
    }
    let row_count = u32::from_le_bytes(payload[offset..offset + 4].try_into().unwrap()) as usize;
    offset += 4;

    let schema = Schema::new(columns);

    if row_count == 0 {
        return Ok(QueryResult::Rows {
            schema: schema.clone(),
            batches: vec![RecordBatch::new(schema)],
        });
    }

    let mut arrays: Vec<ColumnArray> = schema
        .columns
        .iter()
        .map(|col| ColumnArray::empty(col.data_type))
        .collect();

    for _ in 0..row_count {
        for col_idx in 0..schema.columns.len() {
            if offset >= payload.len() {
                return Err(MonaxError::Protocol("truncated row data".into()));
            }
            let is_null = payload[offset] != 0;
            offset += 1;

            let dt = schema.columns[col_idx].data_type;
            let value = if is_null {
                ScalarValue::null_for_type(dt)
            } else {
                let scale = schema.columns[col_idx].scale;
                let (val, consumed) = read_scalar_value(&payload[offset..], dt, scale)?;
                offset += consumed;
                val
            };
            arrays[col_idx].push(&value)?;
        }
    }

    let batch = RecordBatch::try_new(schema.clone(), arrays)?;
    Ok(QueryResult::Rows {
        schema,
        batches: vec![batch],
    })
}

fn read_scalar_value(data: &[u8], dt: DataType, scale: i32) -> Result<(ScalarValue, usize)> {
    match dt {
        DataType::Bool => {
            if data.is_empty() {
                return Err(MonaxError::Protocol("no data for Bool".into()));
            }
            Ok((ScalarValue::Bool(Some(data[0] != 0)), 1))
        }
        DataType::Int8 => {
            if data.is_empty() {
                return Err(MonaxError::Protocol("no data for Int8".into()));
            }
            Ok((ScalarValue::Int8(Some(data[0] as i8)), 1))
        }
        DataType::Int16 => {
            if data.len() < 2 {
                return Err(MonaxError::Protocol("no data for Int16".into()));
            }
            Ok((
                ScalarValue::Int16(Some(i16::from_le_bytes(data[..2].try_into().unwrap()))),
                2,
            ))
        }
        DataType::Int32 => {
            if data.len() < 4 {
                return Err(MonaxError::Protocol("no data for Int32".into()));
            }
            Ok((
                ScalarValue::Int32(Some(i32::from_le_bytes(data[..4].try_into().unwrap()))),
                4,
            ))
        }
        DataType::Int64 => {
            if data.len() < 8 {
                return Err(MonaxError::Protocol("no data for Int64".into()));
            }
            Ok((
                ScalarValue::Int64(Some(i64::from_le_bytes(data[..8].try_into().unwrap()))),
                8,
            ))
        }
        DataType::UInt8 => {
            if data.is_empty() {
                return Err(MonaxError::Protocol("no data for UInt8".into()));
            }
            Ok((ScalarValue::UInt8(Some(data[0])), 1))
        }
        DataType::UInt16 => {
            if data.len() < 2 {
                return Err(MonaxError::Protocol("no data for UInt16".into()));
            }
            Ok((
                ScalarValue::UInt16(Some(u16::from_le_bytes(data[..2].try_into().unwrap()))),
                2,
            ))
        }
        DataType::UInt32 => {
            if data.len() < 4 {
                return Err(MonaxError::Protocol("no data for UInt32".into()));
            }
            Ok((
                ScalarValue::UInt32(Some(u32::from_le_bytes(data[..4].try_into().unwrap()))),
                4,
            ))
        }
        DataType::UInt64 => {
            if data.len() < 8 {
                return Err(MonaxError::Protocol("no data for UInt64".into()));
            }
            Ok((
                ScalarValue::UInt64(Some(u64::from_le_bytes(data[..8].try_into().unwrap()))),
                8,
            ))
        }
        DataType::Float32 => {
            if data.len() < 4 {
                return Err(MonaxError::Protocol("no data for Float32".into()));
            }
            Ok((
                ScalarValue::Float32(Some(f32::from_le_bytes(data[..4].try_into().unwrap()))),
                4,
            ))
        }
        DataType::Float64 => {
            if data.len() < 8 {
                return Err(MonaxError::Protocol("no data for Float64".into()));
            }
            Ok((
                ScalarValue::Float64(Some(f64::from_le_bytes(data[..8].try_into().unwrap()))),
                8,
            ))
        }
        DataType::Decimal => {
            if scale > 0 {
                if data.len() < 4 {
                    return Err(MonaxError::Protocol("no data for Decimal".into()));
                }
                let len = u32::from_le_bytes(data[..4].try_into().unwrap()) as usize;
                if data.len() < 4 + len {
                    return Err(MonaxError::Protocol("Decimal data truncated".into()));
                }
                let s = std::str::from_utf8(&data[4..4 + len]).map_err(|e| {
                    MonaxError::Protocol(format!("invalid UTF-8 in Decimal: {}", e))
                })?;
                let value = parse_scaled_decimal_text(s, scale)?;
                return Ok((ScalarValue::Decimal(Some(value)), 4 + len));
            }
            if data.len() < 8 {
                return Err(MonaxError::Protocol("no data for Decimal".into()));
            }
            Ok((
                ScalarValue::Decimal(Some(i64::from_le_bytes(data[..8].try_into().unwrap()))),
                8,
            ))
        }
        DataType::Date => {
            if data.len() < 4 {
                return Err(MonaxError::Protocol("no data for Date".into()));
            }
            Ok((
                ScalarValue::Date(Some(i32::from_le_bytes(data[..4].try_into().unwrap()))),
                4,
            ))
        }
        DataType::Time => {
            if data.len() < 8 {
                return Err(MonaxError::Protocol("no data for Time".into()));
            }
            Ok((
                ScalarValue::Time(Some(i64::from_le_bytes(data[..8].try_into().unwrap()))),
                8,
            ))
        }
        DataType::Timestamp => {
            if data.len() < 8 {
                return Err(MonaxError::Protocol("no data for Timestamp".into()));
            }
            Ok((
                ScalarValue::Timestamp(Some(i64::from_le_bytes(data[..8].try_into().unwrap()))),
                8,
            ))
        }
        DataType::Interval => {
            if data.len() < 8 {
                return Err(MonaxError::Protocol("no data for Interval".into()));
            }
            Ok((
                ScalarValue::Interval(Some(i64::from_le_bytes(data[..8].try_into().unwrap()))),
                8,
            ))
        }
        DataType::VarChar => {
            if data.len() < 4 {
                return Err(MonaxError::Protocol("no data for VarChar".into()));
            }
            let len = u32::from_le_bytes(data[..4].try_into().unwrap()) as usize;
            if data.len() < 4 + len {
                return Err(MonaxError::Protocol("VarChar data truncated".into()));
            }
            let s = String::from_utf8_lossy(&data[4..4 + len]).to_string();
            Ok((ScalarValue::VarChar(Some(s)), 4 + len))
        }
        DataType::Blob => {
            if data.len() < 4 {
                return Err(MonaxError::Protocol("no data for Blob".into()));
            }
            let len = u32::from_le_bytes(data[..4].try_into().unwrap()) as usize;
            if data.len() < 4 + len {
                return Err(MonaxError::Protocol("Blob data truncated".into()));
            }
            let b = data[4..4 + len].to_vec();
            Ok((ScalarValue::Blob(Some(b)), 4 + len))
        }
        DataType::Vector => {
            if data.len() < 4 {
                return Err(MonaxError::Protocol("no data for Vector".into()));
            }
            let len = u32::from_le_bytes(data[..4].try_into().unwrap()) as usize;
            let total_bytes = 4 + len * 4;
            if data.len() < total_bytes {
                return Err(MonaxError::Protocol("Vector data truncated".into()));
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
        return Err(MonaxError::Protocol("empty Decimal value".into()));
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
        return Err(MonaxError::Protocol(format!("invalid Decimal '{}'", s)));
    }
    if !int_part.chars().all(|c| c.is_ascii_digit())
        || !frac_part.chars().all(|c| c.is_ascii_digit())
    {
        return Err(MonaxError::Protocol(format!("invalid Decimal '{}'", s)));
    }

    let multiplier = 10_i64
        .checked_pow(scale as u32)
        .ok_or_else(|| MonaxError::Protocol(format!("Decimal scale {} overflows", scale)))?;
    let int_value = if int_part.is_empty() {
        0
    } else {
        int_part
            .parse::<i64>()
            .map_err(|e| MonaxError::Protocol(format!("invalid Decimal '{}': {}", s, e)))?
    };

    let mut frac = frac_part.to_string();
    let scale_usize = scale as usize;
    if frac.len() > scale_usize {
        return Err(MonaxError::Protocol(format!(
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
            .map_err(|e| MonaxError::Protocol(format!("invalid Decimal '{}': {}", s, e)))?
    };

    let magnitude = int_value
        .checked_mul(multiplier)
        .and_then(|v| v.checked_add(frac_value))
        .ok_or_else(|| MonaxError::Protocol(format!("Decimal '{}' overflows", s)))?;
    Ok(if negative { -magnitude } else { magnitude })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::message::write_len_prefixed_string;

    #[test]
    fn parses_scaled_decimal_without_misaligning_following_varchar() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&2u32.to_le_bytes());
        write_column_meta(&mut payload, "name", DataType::VarChar, 0);
        write_column_meta(&mut payload, "score", DataType::Decimal, 2);
        payload.extend_from_slice(&2u32.to_le_bytes());

        write_varchar_cell(&mut payload, "alice");
        write_decimal_cell(&mut payload, "12.34");
        write_varchar_cell(&mut payload, "dave");
        write_decimal_cell(&mut payload, "0");

        let result = parse_text_result(&payload).unwrap();
        let QueryResult::Rows { schema, batches } = result else {
            panic!("expected row result");
        };

        assert_eq!(schema.columns[1].scale, 2);
        assert_eq!(
            batches[0].scalar(0, 0).unwrap(),
            ScalarValue::VarChar(Some("alice".into()))
        );
        assert_eq!(
            batches[0].scalar(0, 1).unwrap(),
            ScalarValue::Decimal(Some(1234))
        );
        assert_eq!(
            batches[0].scalar(1, 0).unwrap(),
            ScalarValue::VarChar(Some("dave".into()))
        );
        assert_eq!(
            batches[0].scalar(1, 1).unwrap(),
            ScalarValue::Decimal(Some(0))
        );
    }

    #[test]
    fn parses_scaled_decimal_text_values() {
        assert_eq!(parse_scaled_decimal_text("123", 2).unwrap(), 12300);
        assert_eq!(parse_scaled_decimal_text("123.4", 2).unwrap(), 12340);
        assert_eq!(parse_scaled_decimal_text("-0.23", 2).unwrap(), -23);
        assert!(parse_scaled_decimal_text("1.234", 2).is_err());
    }

    fn write_column_meta(buf: &mut Vec<u8>, name: &str, data_type: DataType, scale: i32) {
        write_len_prefixed_string(buf, name);
        buf.push(data_type as u8);
        buf.extend_from_slice(&(scale as u32).to_le_bytes());
    }

    fn write_varchar_cell(buf: &mut Vec<u8>, value: &str) {
        buf.push(0);
        write_len_prefixed_string(buf, value);
    }

    fn write_decimal_cell(buf: &mut Vec<u8>, value: &str) {
        buf.push(0);
        write_len_prefixed_string(buf, value);
    }
}
