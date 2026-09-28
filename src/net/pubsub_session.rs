use std::collections::HashSet;
use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::mpsc;

use crate::common::error::{Result, TaibaiError};
use crate::net::bulk_importer::{decode_columnar_bulk_insert, encode_columnar_bulk_insert};
use crate::protocol::message::{
    read_len_prefixed_string, write_len_prefixed_string, Message, MSG_TYPE_SUB_CONFIRMED,
    MSG_TYPE_SUB_DATA, MSG_TYPE_SUB_ERROR,
};
use crate::storage::core::pubsub::{
    DrainResult, FrameKind, PublishedFrame, SubscribeGrant, SubscriptionHandle,
};
use crate::types::array::ColumnArray;
use crate::types::batch::RecordBatch;

pub const SUB_ERROR_RESYNC_REQUIRED: u8 = 1;
pub const SUB_ERROR_DENIED: u8 = 2;
pub const SUB_ERROR_NOT_FOUND: u8 = 3;

const SUBSCRIBE_MODE_FROM_NOW: u8 = 0;
const SUBSCRIBE_MODE_FROM_SEQ: u8 = 1;

pub struct SubscribePayload {
    pub db: String,
    pub table: String,
    pub from_seq: Option<u64>,
    pub named: Option<String>,
    pub filter_column: Option<String>,
    pub filter_values: Option<HashSet<String>>,
}

fn read_u8(data: &[u8]) -> Result<(u8, &[u8])> {
    if data.is_empty() {
        return Err(TaibaiError::Protocol("unexpected end of payload".into()));
    }
    Ok((data[0], &data[1..]))
}

fn read_u64_le(data: &[u8]) -> Result<(u64, &[u8])> {
    if data.len() < 8 {
        return Err(TaibaiError::Protocol(
            "insufficient data for u64 field".into(),
        ));
    }
    Ok((
        u64::from_le_bytes(data[0..8].try_into().unwrap()),
        &data[8..],
    ))
}

fn read_u32_le(data: &[u8]) -> Result<(u32, &[u8])> {
    if data.len() < 4 {
        return Err(TaibaiError::Protocol(
            "insufficient data for u32 field".into(),
        ));
    }
    Ok((
        u32::from_le_bytes(data[0..4].try_into().unwrap()),
        &data[4..],
    ))
}

fn write_u64_le(buf: &mut Vec<u8>, value: u64) {
    buf.extend_from_slice(&value.to_le_bytes());
}

fn write_u32_le(buf: &mut Vec<u8>, value: u32) {
    buf.extend_from_slice(&value.to_le_bytes());
}

fn read_str(data: &[u8]) -> Result<(String, &[u8])> {
    let (s, rest) = read_len_prefixed_string(data)?;
    Ok((s.to_string(), rest))
}

pub fn decode_subscribe(payload: &[u8]) -> Result<SubscribePayload> {
    let (db, rest) = read_str(payload)?;
    let (table, rest) = read_str(rest)?;
    let (mode, rest) = read_u8(rest)?;
    let (from_seq, rest) = read_u64_le(rest)?;
    let (named, rest) = read_str(rest)?;
    let (has_filter, rest) = read_u8(rest)?;
    // The filter section advances `rest` itself: the inner reads must hand
    // the remaining bytes back, or the trailing check below sees a stale
    // offset and every filtered subscribe is rejected.
    let (filter_column, filter_values, rest) = if has_filter == 1 {
        let (column, rest) = read_str(rest)?;
        let (count, mut rest) = read_u32_le(rest)?;
        let mut values = HashSet::with_capacity(count as usize);
        for _ in 0..count {
            let (v, r) = read_str(rest)?;
            values.insert(v);
            rest = r;
        }
        (Some(column), Some(values), rest)
    } else {
        (None, None, rest)
    };
    if !rest.is_empty() {
        return Err(TaibaiError::Protocol(
            "trailing bytes in SUBSCRIBE payload".into(),
        ));
    }
    Ok(SubscribePayload {
        db,
        table,
        from_seq: if mode == SUBSCRIBE_MODE_FROM_SEQ {
            Some(from_seq)
        } else {
            None
        },
        named: if named.is_empty() { None } else { Some(named) },
        filter_column,
        filter_values,
    })
}

pub fn encode_subscribe(
    db: &str,
    table: &str,
    from_seq: Option<u64>,
    named: Option<&str>,
    filter_column: Option<&str>,
    filter_values: Option<&HashSet<String>>,
) -> Bytes {
    let mut buf = Vec::new();
    write_str(&mut buf, db);
    write_str(&mut buf, table);
    buf.push(if from_seq.is_some() {
        SUBSCRIBE_MODE_FROM_SEQ
    } else {
        SUBSCRIBE_MODE_FROM_NOW
    });
    write_u64_le(&mut buf, from_seq.unwrap_or(0));
    write_str(&mut buf, named.unwrap_or(""));
    match (filter_column, filter_values) {
        (Some(column), Some(values)) => {
            buf.push(1);
            write_str(&mut buf, column);
            write_u32_le(&mut buf, values.len() as u32);
            for value in values {
                write_str(&mut buf, value);
            }
        }
        _ => buf.push(0),
    }
    Bytes::from(buf)
}

fn write_str(buf: &mut Vec<u8>, s: &str) {
    write_len_prefixed_string(buf, s);
}

pub fn encode_sub_confirmed(grant: &SubscribeGrant) -> Bytes {
    let mut buf = Vec::new();
    write_u64_le(&mut buf, grant.sub_id);
    write_u64_le(&mut buf, grant.epoch);
    write_u64_le(&mut buf, grant.granted_from);
    write_u64_le(&mut buf, grant.floor);
    Bytes::from(buf)
}

/// Decode a MSG_TYPE_SUB_CONFIRMED payload (client side).
pub fn decode_sub_confirmed(payload: &[u8]) -> Result<SubscribeGrant> {
    let (sub_id, rest) = read_u64_le(payload)?;
    let (epoch, rest) = read_u64_le(rest)?;
    let (granted_from, rest) = read_u64_le(rest)?;
    let (floor, _rest) = read_u64_le(rest)?;
    Ok(SubscribeGrant {
        sub_id,
        epoch,
        granted_from,
        floor,
    })
}

pub fn encode_sub_error(sub_id: u64, code: u8, aux: u64, message: &str) -> Bytes {
    let mut buf = Vec::new();
    write_u64_le(&mut buf, sub_id);
    buf.push(code);
    write_u64_le(&mut buf, aux);
    write_str(&mut buf, message);
    Bytes::from(buf)
}

pub fn encode_sub_id_message(db: &str, table: &str, sub_id: u64) -> Bytes {
    let mut buf = Vec::new();
    write_str(&mut buf, db);
    write_str(&mut buf, table);
    write_u64_le(&mut buf, sub_id);
    Bytes::from(buf)
}

pub fn encode_sub_ack(db: &str, table: &str, sub_id: u64, seq: u64) -> Bytes {
    let mut buf = encode_sub_id_message(db, table, sub_id).to_vec();
    write_u64_le(&mut buf, seq);
    Bytes::from(buf)
}

pub fn decode_table_and_sub(payload: &[u8]) -> Result<(String, String, u64)> {
    let (db, rest) = read_str(payload)?;
    let (table, rest) = read_str(rest)?;
    let (sub_id, _rest) = read_u64_le(rest)?;
    Ok((db, table, sub_id))
}

pub fn decode_table_sub_and_seq(payload: &[u8]) -> Result<(String, String, u64, u64)> {
    let (db, rest) = read_str(payload)?;
    let (table, rest) = read_str(rest)?;
    let (sub_id, rest) = read_u64_le(rest)?;
    let (seq, _rest) = read_u64_le(rest)?;
    Ok((db, table, sub_id, seq))
}

fn encode_sub_data(
    sub_id: u64,
    seq_start: u64,
    seq_end: u64,
    table: &str,
    batches: &[Arc<RecordBatch>],
) -> Result<Bytes> {
    let mut buf = Vec::new();
    write_u64_le(&mut buf, sub_id);
    write_u64_le(&mut buf, seq_start);
    write_u64_le(&mut buf, seq_end);
    write_u32_le(&mut buf, batches.len() as u32);
    for batch in batches {
        let body = encode_columnar_bulk_insert(table, batch)?;
        write_u32_le(&mut buf, body.len() as u32);
        buf.extend_from_slice(&body);
    }
    Ok(Bytes::from(buf))
}

pub fn frame_to_message(frame: PublishedFrame) -> Result<Message> {
    match frame.kind {
        FrameKind::Data {
            seq_start,
            seq_end,
            batches,
        } => {
            let payload =
                encode_sub_data(frame.sub_id, seq_start, seq_end, &frame.table, &batches)?;
            Ok(Message::new(MSG_TYPE_SUB_DATA, payload))
        }
        FrameKind::ResyncRequired { floor } => Ok(Message::new(
            MSG_TYPE_SUB_ERROR,
            encode_sub_error(
                frame.sub_id,
                SUB_ERROR_RESYNC_REQUIRED,
                floor,
                "committed sequence gap detected; resubscribe from floor",
            ),
        )),
    }
}

pub fn confirmed_message(grant: &SubscribeGrant) -> Message {
    Message::new(MSG_TYPE_SUB_CONFIRMED, encode_sub_confirmed(grant))
}

pub fn error_message(sub_id: u64, code: u8, aux: u64, message: &str) -> Message {
    Message::new(
        MSG_TYPE_SUB_ERROR,
        encode_sub_error(sub_id, code, aux, message),
    )
}

/// One materialized data push, decoded by the client library.
#[derive(Debug)]
pub struct SubDataFrame {
    pub sub_id: u64,
    pub seq_start: u64,
    pub seq_end: u64,
    pub table: String,
    pub batches: Vec<RecordBatch>,
}

/// Server-pushed error for a reliable subscription (e.g. resync request).
#[derive(Debug)]
pub struct SubErrorFrame {
    pub sub_id: u64,
    pub code: u8,
    pub aux: u64,
    pub message: String,
}

/// Decode a MSG_TYPE_SUB_DATA payload back into batches. The schema comes
/// from the wire column metadata, so no catalog access is needed.
pub fn decode_sub_data(payload: &[u8]) -> Result<SubDataFrame> {
    let (sub_id, rest) = read_u64_le(payload)?;
    let (seq_start, rest) = read_u64_le(rest)?;
    let (seq_end, rest) = read_u64_le(rest)?;
    let (count, rest) = read_u32_le(rest)?;
    let mut rest = rest;
    let mut table = String::new();
    let mut batches = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let (len, r) = read_u32_le(rest)?;
        let body = r
            .get(..len as usize)
            .ok_or_else(|| TaibaiError::Protocol("sub data batch body exceeds payload".into()))?;
        if table.is_empty() {
            table = bulk_body_table_name(body)?;
        }
        batches.push(decode_columnar_bulk_insert(body)?);
        rest = &r[len as usize..];
    }
    if !rest.is_empty() {
        return Err(TaibaiError::Protocol(
            "trailing bytes in SUB_DATA payload".into(),
        ));
    }
    Ok(SubDataFrame {
        sub_id,
        seq_start,
        seq_end,
        table,
        batches,
    })
}

/// The bulk body starts with `[u16 table_len][table bytes]`.
fn bulk_body_table_name(body: &[u8]) -> Result<String> {
    if body.len() < 2 {
        return Err(TaibaiError::Protocol("bulk body too short".into()));
    }
    let len = u16::from_le_bytes([body[0], body[1]]) as usize;
    let bytes = body
        .get(2..2 + len)
        .ok_or_else(|| TaibaiError::Protocol("bulk table name exceeds body".into()))?;
    String::from_utf8(bytes.to_vec())
        .map_err(|e| TaibaiError::Protocol(format!("invalid UTF-8 table name: {e}")))
}

/// Decode a MSG_TYPE_SUB_ERROR payload.
pub fn decode_sub_error(payload: &[u8]) -> Result<SubErrorFrame> {
    let (sub_id, rest) = read_u64_le(payload)?;
    let (code, rest) = read_u8(rest)?;
    let (aux, rest) = read_u64_le(rest)?;
    let (message, _rest) = read_str(rest)?;
    Ok(SubErrorFrame {
        sub_id,
        code,
        aux,
        message: message.to_string(),
    })
}

/// Keep only the rows whose `column` value (rendered as text) is in `values`.
/// NULLs never match. Returns `None` when no row survives.
pub fn apply_filter(
    batch: &RecordBatch,
    column: &str,
    values: &HashSet<String>,
) -> Result<Option<RecordBatch>> {
    let idx = batch
        .schema
        .columns
        .iter()
        .position(|c| c.name == column)
        .ok_or_else(|| {
            TaibaiError::Protocol(format!("filter column {column} not found in table schema"))
        })?;
    let mut indices: Vec<u32> = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        if let Some(text) = batch.scalar(row, idx).and_then(|scalar| scalar.to_text()) {
            if values.contains(&text) {
                indices.push(row as u32);
            }
        }
    }
    if indices.len() == batch.num_rows() {
        return Ok(Some(batch.clone()));
    }
    if indices.is_empty() {
        return Ok(None);
    }
    let columns: Vec<ColumnArray> = batch.columns.iter().map(|col| col.take(&indices)).collect();
    Ok(Some(RecordBatch::try_new(batch.schema.clone(), columns)?))
}

pub fn spawn_delivery(
    handle: SubscriptionHandle,
    max_frame_batches: usize,
    frame_tx: mpsc::Sender<PublishedFrame>,
) {
    let db = handle.table().0.to_string();
    let table = handle.table().1.to_string();
    tokio::spawn(async move {
        loop {
            match handle.drain(max_frame_batches) {
                DrainResult::Data {
                    seq_start,
                    seq_end,
                    batches,
                } => {
                    let mut out: Vec<Arc<RecordBatch>> = Vec::with_capacity(batches.len());
                    for batch in &batches {
                        let kept: Arc<RecordBatch> = match handle.filter() {
                            Some((col, values)) => match apply_filter(batch, col, values) {
                                Ok(Some(kept)) => Arc::new(kept),
                                Ok(None) => continue,
                                Err(err) => {
                                    tracing::warn!(
                                        db = %db,
                                        table = %table,
                                        sub_id = handle.sub_id(),
                                        error = %err,
                                        "pubsub filter failed; passing batch through"
                                    );
                                    Arc::clone(batch)
                                }
                            },
                            None => Arc::clone(batch),
                        };
                        out.push(kept);
                    }
                    handle.advance_delivered(seq_end);
                    if out.is_empty() {
                        continue;
                    }
                    let frame = PublishedFrame {
                        db: db.clone(),
                        table: table.clone(),
                        sub_id: handle.sub_id(),
                        kind: FrameKind::Data {
                            seq_start,
                            seq_end,
                            batches: out,
                        },
                    };
                    if frame_tx.send(frame).await.is_err() {
                        return;
                    }
                }
                DrainResult::Resync { floor } => {
                    let _ = handle.send_frame(FrameKind::ResyncRequired { floor }).await;
                    return;
                }
                DrainResult::Empty => {
                    if handle.is_closed() {
                        return;
                    }
                    tokio::select! {
                        _ = handle.wait_changed() => {}
                        _ = tokio::time::sleep(handle.poll_interval()) => {}
                    }
                }
            }
        }
    });
}

#[cfg(test)]
#[path = "pubsub_session_test.rs"]
mod tests;
