//! Tail-log wire format: frame/header encoding, CRC, scalar cell
//! codec, and version detection (V2/V3).

use std::fs;
use std::io::{Read, Write};
use std::path::Path;
use std::sync::atomic::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};

use crc32fast::Hasher;

use crate::common::error::{Result, TaibaiError};
use crate::protocol::types::DataType;
use crate::sql::execution::wal_helper::{
    deserialize_batch_from_wal,
    serialize_batch_for_wal,
};
use crate::types::batch::{RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

use super::committer::*;

pub(super) const TAIL_MAGIC: u32 = 0x5441_494C;
/// V2 tail log frames carry the row count in the frame header so recovery can
/// index a tail without decompressing any payload.
pub(super) const TAIL_LOG_MAGIC_V2: u32 = 0x544C_4F48;
/// V3 tail log frames extend the V2 header with a globally allocated LSN and
/// a commit timestamp (micros since the Unix epoch), which replication site
/// tracking and point-in-time recovery replay both need. The file header
/// magic distinguishes the frame layouts; V2 files stay decodable.
pub(super) const TAIL_LOG_MAGIC_V3: u32 = 0x544C_4F56;

pub(super) const FLUSH_MAGIC: u32 = 0x464C_5553;

pub(super) fn read_le_u32(body: &[u8], offset: &mut usize) -> Result<u32> {
    if body.len() < *offset + 4 {
        return Err(TaibaiError::Storage(
            "durable tail update sidecar is truncated".into(),
        ));
    }
    let value = u32::from_le_bytes(body[*offset..*offset + 4].try_into().unwrap());
    *offset += 4;
    Ok(value)
}

pub(super) fn read_len_prefixed<'a>(body: &'a [u8], offset: &mut usize) -> Result<&'a [u8]> {
    let len = read_le_u32(body, offset)? as usize;
    if body.len() < *offset + len {
        return Err(TaibaiError::Storage(
            "durable tail update sidecar is truncated".into(),
        ));
    }
    let slice = &body[*offset..*offset + len];
    *offset += len;
    Ok(slice)
}

/// One assigned cell on disk: `[u8 dtype][u8 null][payload]`, payload absent
/// when the null flag is set and length-prefixed for the variable-width
/// types. Fixed-width payload layout follows [`ScalarValue`]'s inner repr.
pub(super) fn encode_scalar_cell(bytes: &mut Vec<u8>, value: &ScalarValue) {
    bytes.push(value.data_type() as u8);
    if value.is_null() {
        bytes.push(1u8);
        return;
    }
    bytes.push(0u8);
    match value {
        ScalarValue::Bool(Some(v)) => bytes.push(*v as u8),
        ScalarValue::Int8(Some(v)) => bytes.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::Int16(Some(v)) => bytes.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::Int32(Some(v)) => bytes.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::Int64(Some(v)) => bytes.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::UInt8(Some(v)) => bytes.push(*v),
        ScalarValue::UInt16(Some(v)) => bytes.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::UInt32(Some(v)) => bytes.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::UInt64(Some(v)) => bytes.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::Float32(Some(v)) => bytes.extend_from_slice(&v.to_bits().to_le_bytes()),
        ScalarValue::Float64(Some(v)) => bytes.extend_from_slice(&v.to_bits().to_le_bytes()),
        ScalarValue::Decimal(Some(v)) => bytes.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::Date(Some(v)) => bytes.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::Time(Some(v)) => bytes.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::Timestamp(Some(v)) => bytes.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::Interval(Some(v)) => bytes.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::VarChar(Some(v)) => {
            bytes.extend_from_slice(&(v.len() as u32).to_le_bytes());
            bytes.extend_from_slice(v.as_bytes());
        }
        ScalarValue::Blob(Some(v)) => {
            bytes.extend_from_slice(&(v.len() as u32).to_le_bytes());
            bytes.extend_from_slice(v);
        }
        ScalarValue::Vector(Some(v)) => {
            bytes.extend_from_slice(&(v.len() as u32).to_le_bytes());
            for element in v {
                bytes.extend_from_slice(&element.to_bits().to_le_bytes());
            }
        }
        // Nulls returned early above; the None arms are unreachable.
        _ => unreachable!("non-null scalar cell matched a None payload"),
    }
}

pub(super) fn decode_scalar_cell(body: &[u8], offset: &mut usize) -> Result<ScalarValue> {
    if body.len() < *offset + 2 {
        return Err(TaibaiError::Storage(
            "durable tail update sidecar is truncated".into(),
        ));
    }
    let data_type = DataType::from_u8(body[*offset])?;
    *offset += 1;
    let is_null = body[*offset] == 1;
    *offset += 1;
    if is_null {
        return Ok(ScalarValue::null_for_type(data_type));
    }
    // Fixed-width payloads are raw little-endian bytes; variable-width ones
    // are u32-length-prefixed (mirroring encode_scalar_cell exactly).
    let value = match data_type {
        DataType::Bool => ScalarValue::Bool(Some(read_raw(body, offset, 1)?[0] != 0)),
        DataType::Int8 => ScalarValue::Int8(Some(read_raw(body, offset, 1)?[0] as i8)),
        DataType::Int16 => ScalarValue::Int16(Some(i16::from_le_bytes(
            read_raw(body, offset, 2)?.try_into().unwrap(),
        ))),
        DataType::Int32 => ScalarValue::Int32(Some(i32::from_le_bytes(
            read_raw(body, offset, 4)?.try_into().unwrap(),
        ))),
        DataType::Int64 => ScalarValue::Int64(Some(i64::from_le_bytes(
            read_raw(body, offset, 8)?.try_into().unwrap(),
        ))),
        DataType::UInt8 => ScalarValue::UInt8(Some(read_raw(body, offset, 1)?[0])),
        DataType::UInt16 => ScalarValue::UInt16(Some(u16::from_le_bytes(
            read_raw(body, offset, 2)?.try_into().unwrap(),
        ))),
        DataType::UInt32 => ScalarValue::UInt32(Some(u32::from_le_bytes(
            read_raw(body, offset, 4)?.try_into().unwrap(),
        ))),
        DataType::UInt64 => ScalarValue::UInt64(Some(u64::from_le_bytes(
            read_raw(body, offset, 8)?.try_into().unwrap(),
        ))),
        DataType::Float32 => ScalarValue::Float32(Some(f32::from_bits(u32::from_le_bytes(
            read_raw(body, offset, 4)?.try_into().unwrap(),
        )))),
        DataType::Float64 => ScalarValue::Float64(Some(f64::from_bits(u64::from_le_bytes(
            read_raw(body, offset, 8)?.try_into().unwrap(),
        )))),
        DataType::Decimal => ScalarValue::Decimal(Some(i64::from_le_bytes(
            read_raw(body, offset, 8)?.try_into().unwrap(),
        ))),
        DataType::Date => ScalarValue::Date(Some(i32::from_le_bytes(
            read_raw(body, offset, 4)?.try_into().unwrap(),
        ))),
        DataType::Time => ScalarValue::Time(Some(i64::from_le_bytes(
            read_raw(body, offset, 8)?.try_into().unwrap(),
        ))),
        DataType::Timestamp => ScalarValue::Timestamp(Some(i64::from_le_bytes(
            read_raw(body, offset, 8)?.try_into().unwrap(),
        ))),
        DataType::Interval => ScalarValue::Interval(Some(i64::from_le_bytes(
            read_raw(body, offset, 8)?.try_into().unwrap(),
        ))),
        DataType::VarChar => ScalarValue::VarChar(Some(
            String::from_utf8(read_len_prefixed(body, offset)?.to_vec())
                .map_err(|_| TaibaiError::Storage("update sidecar VarChar is not UTF-8".into()))?,
        )),
        DataType::Blob => ScalarValue::Blob(Some(read_len_prefixed(body, offset)?.to_vec())),
        DataType::Vector => {
            let count = read_le_u32(body, offset)? as usize;
            if body.len() < *offset + count * 4 {
                return Err(TaibaiError::Storage(
                    "durable tail update sidecar is truncated".into(),
                ));
            }
            let elements = body[*offset..*offset + count * 4]
                .chunks_exact(4)
                .map(|chunk| f32::from_bits(u32::from_le_bytes(chunk.try_into().unwrap())))
                .collect();
            *offset += count * 4;
            ScalarValue::Vector(Some(elements))
        }
    };
    Ok(value)
}

/// A raw fixed-width payload read that verifies the exact width, so a
/// corrupt-but-CRC-bearing file fails loudly instead of misreading bytes.
pub(super) fn read_raw<'a>(body: &'a [u8], offset: &mut usize, width: usize) -> Result<&'a [u8]> {
    if body.len() < *offset + width {
        return Err(TaibaiError::Storage(
            "durable tail update sidecar is truncated".into(),
        ));
    }
    let slice = &body[*offset..*offset + width];
    *offset += width;
    Ok(slice)
}

pub(crate) fn decode_tail_file(path: &Path, schema: &Schema) -> Result<(String, RecordBatch)> {
    decode_tail(&fs::read(path)?, schema)
}

pub(super) fn write_new_synced(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

pub(super) fn encode_tail_log(partition_key: &str, batch: &RecordBatch) -> Result<Vec<u8>> {
    let mut bytes = encode_tail_log_header(partition_key);
    bytes.extend_from_slice(&encode_tail_frame(batch)?);
    Ok(bytes)
}

pub(super) fn encode_tail_log_header(partition_key: &str) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(8 + partition_key.len());
    bytes.extend_from_slice(&TAIL_LOG_MAGIC_V3.to_le_bytes());
    bytes.extend_from_slice(&(partition_key.len() as u32).to_le_bytes());
    bytes.extend_from_slice(partition_key.as_bytes());
    bytes
}

pub(super) fn encode_tail_frame(batch: &RecordBatch) -> Result<Vec<u8>> {
    let payload = serialize_batch_for_wal(batch)?;
    // Direct-write helpers (persist/append) allocate from the same
    // monotonic counter as the commit thread, so every durable frame
    // carries a unique, increasing LSN.
    let lsn = TAIL_LSN_HIGH_WATER.fetch_add(1, Ordering::AcqRel) + 1;
    let commit_ts_micros = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0);
    Ok(encode_tail_frame_payload(
        &payload,
        batch.num_rows() as u64,
        lsn,
        commit_ts_micros,
    ))
}

/// V3 frame layout: `[u64 payload_len][u64 rows][u64 lsn][i64 commit_ts_micros]
/// [payload][crc32]`, the CRC covering the four header fields and the payload.
/// V2 frames (`[u64 payload_len][u64 rows][payload][crc32]`) and V1 frames
/// (no `rows` field) remain decodable via the file-header magic.
pub(super) fn encode_tail_frame_payload(
    payload: &[u8],
    rows: u64,
    lsn: u64,
    commit_ts_micros: i64,
) -> Vec<u8> {
    let mut frame = Vec::with_capacity(32 + payload.len());
    frame.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    frame.extend_from_slice(&rows.to_le_bytes());
    frame.extend_from_slice(&lsn.to_le_bytes());
    frame.extend_from_slice(&commit_ts_micros.to_le_bytes());
    frame.extend_from_slice(payload);
    let mut hasher = Hasher::new();
    hasher.update(&frame);
    frame.extend_from_slice(&hasher.finalize().to_le_bytes());
    frame
}

/// Per-frame metadata surfaced to recovery, replication and PITR consumers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TailFrameMeta {
    pub lsn: u64,
    pub commit_ts_micros: i64,
    pub rows: u64,
}

/// A tail log's frame layout, resolved once from the file-header magic.
/// V3 frames carry LSN/commit-timestamp fields; V2 files decode with zeros
/// there; V1 frames (no `rows` field) stay decodable through the same
/// header-length arithmetic. All version dispatch lives here.
#[derive(Debug, Clone, Copy)]
pub(super) struct TailFormat {
    pub(super) frame_header_len: usize,
}

impl TailFormat {
    pub(super) fn from_magic(magic: u32) -> Option<Self> {
        match magic {
            TAIL_LOG_MAGIC_V3 => Some(Self {
                frame_header_len: 32,
            }),
            TAIL_LOG_MAGIC_V2 => Some(Self {
                frame_header_len: 16,
            }),
            _ => None,
        }
    }

    /// V3+ frames carry the LSN and commit timestamp in the header.
    pub(super) fn has_commit_metadata(self) -> bool {
        self.frame_header_len >= 32
    }
}

pub(super) fn append_crc(mut bytes: Vec<u8>) -> Vec<u8> {
    let mut hasher = Hasher::new();
    hasher.update(&bytes);
    bytes.extend_from_slice(&hasher.finalize().to_le_bytes());
    bytes
}

pub(super) fn verify_crc(bytes: &[u8]) -> Result<&[u8]> {
    if bytes.len() < 4 {
        return Err(TaibaiError::Storage(
            "durable tail record is truncated".into(),
        ));
    }
    let body_end = bytes.len() - 4;
    let stored_crc = u32::from_le_bytes(bytes[body_end..].try_into().unwrap());
    let mut hasher = Hasher::new();
    hasher.update(&bytes[..body_end]);
    if stored_crc != hasher.finalize() {
        return Err(TaibaiError::Storage(
            "durable tail checksum mismatch".into(),
        ));
    }
    Ok(&bytes[..body_end])
}

pub(super) fn decode_tail(bytes: &[u8], schema: &Schema) -> Result<(String, RecordBatch)> {
    decode_tail_with_meta(bytes, schema).map(|(partition, batch, _)| (partition, batch))
}

/// Decode a tail log and surface per-frame metadata (LSN, commit timestamp).
/// V2 files decode with zero LSNs/timestamps.
pub(super) fn decode_tail_with_meta(
    bytes: &[u8],
    schema: &Schema,
) -> Result<(String, RecordBatch, Vec<TailFrameMeta>)> {
    if bytes.len() < 4 {
        return Err(TaibaiError::Storage(
            "unsupported durable tail version".into(),
        ));
    }
    let magic = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
    let format = TailFormat::from_magic(magic)
        .ok_or_else(|| TaibaiError::Storage("unsupported durable tail version".into()))?;
    decode_tail_log_inner(bytes, schema, format)
}

pub(super) fn decode_tail_log_inner(
    bytes: &[u8],
    schema: &Schema,
    format: TailFormat,
) -> Result<(String, RecordBatch, Vec<TailFrameMeta>)> {
    if bytes.len() < 8 {
        return Err(TaibaiError::Storage("durable tail log is truncated".into()));
    }
    let partition_len = u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize;
    let mut offset = 8usize.saturating_add(partition_len);
    if offset > bytes.len() {
        return Err(TaibaiError::Storage(
            "invalid durable tail log header".into(),
        ));
    }
    let partition_key = std::str::from_utf8(&bytes[8..offset])
        .map_err(|_| TaibaiError::Storage("invalid durable tail partition key".into()))?
        .to_string();
    let mut recovered = RecordBatch::new(schema.clone());
    let mut frames = Vec::new();
    let mut complete_frames = 0usize;
    while offset < bytes.len() {
        // A crash may leave the final append incomplete. All preceding frames
        // were individually synced and remain a valid recovery prefix.
        if bytes.len().saturating_sub(offset) < format.frame_header_len + 4 {
            break;
        }
        let payload_len =
            u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap()) as usize;
        let payload_start = offset + format.frame_header_len;
        let payload_end = payload_start.saturating_add(payload_len);
        let frame_end = payload_end.saturating_add(4);
        if frame_end > bytes.len() {
            break;
        }
        let mut hasher = Hasher::new();
        hasher.update(&bytes[offset..payload_end]);
        let stored_crc = u32::from_le_bytes(bytes[payload_end..frame_end].try_into().unwrap());
        if stored_crc != hasher.finalize() {
            return Err(TaibaiError::Storage(
                "durable tail log frame checksum mismatch".into(),
            ));
        }
        let batch = deserialize_batch_from_wal(&bytes[payload_start..payload_end], schema)?;
        append_batch(&mut recovered, &batch)?;
        // V2 frames have no LSN/timestamp fields; report zeros there.
        let (lsn, commit_ts_micros) = if format.has_commit_metadata() {
            (
                u64::from_le_bytes(bytes[offset + 16..offset + 24].try_into().unwrap()),
                i64::from_le_bytes(bytes[offset + 24..offset + 32].try_into().unwrap()),
            )
        } else {
            (0, 0)
        };
        frames.push(TailFrameMeta {
            lsn,
            commit_ts_micros,
            rows: batch_rows_hint(&bytes[offset..payload_start]),
        });
        complete_frames += 1;
        offset = frame_end;
    }
    if complete_frames == 0 {
        return Err(TaibaiError::Storage(
            "durable tail log contains no complete frame".into(),
        ));
    }
    Ok((partition_key, recovered, frames))
}

/// The `rows` field of a frame header (V2 and V3 share its position).
pub(super) fn batch_rows_hint(frame_head: &[u8]) -> u64 {
    if frame_head.len() >= 16 {
        u64::from_le_bytes(frame_head[8..16].try_into().unwrap())
    } else {
        0
    }
}

/// Remove the given positions from a batch (columnar take, no per-cell
/// scalar round-trip). Local copy of the deferred-tombstone filter so the
/// tail codec does not depend on the transaction registry.
pub(super) fn filter_out_positions(batch: &RecordBatch, positions: &[u32]) -> Result<RecordBatch> {
    let mut doomed = positions.to_vec();
    doomed.sort_unstable();
    doomed.dedup();
    if doomed.is_empty() {
        return Ok(batch.clone());
    }
    let mut keep = Vec::with_capacity(batch.num_rows().saturating_sub(doomed.len()));
    let mut doomed_iter = doomed.iter();
    let mut next = doomed_iter.next();
    for row in 0..batch.num_rows() as u32 {
        match next {
            Some(&position) if position == row => {
                next = doomed_iter.next();
            }
            _ => keep.push(row),
        }
    }
    if keep.len() == batch.num_rows() {
        return Ok(batch.clone());
    }
    let columns = batch
        .columns
        .iter()
        .map(|column| column.take(&keep))
        .collect();
    RecordBatch::try_new(batch.schema.clone(), columns)
}

pub(super) fn append_batch(target: &mut RecordBatch, batch: &RecordBatch) -> Result<()> {
    for col_idx in 0..batch.num_columns() {
        let source = batch
            .column(col_idx)
            .ok_or_else(|| TaibaiError::Storage("durable tail delta is missing a column".into()))?;
        let destination = target
            .column_mut(col_idx)
            .ok_or_else(|| TaibaiError::Storage("durable tail schema column mismatch".into()))?;
        destination.extend(source)?;
    }
    Ok(())
}

pub(super) fn read_tail_log_version(path: &Path) -> Result<Option<u32>> {
    let mut file = fs::File::open(path)?;
    let mut header = [0u8; 4];
    if let Err(error) = file.read_exact(&mut header) {
        if error.kind() == std::io::ErrorKind::UnexpectedEof {
            return Ok(None);
        }
        return Err(error.into());
    }
    let magic = u32::from_le_bytes(header);
    Ok(TailFormat::from_magic(magic).map(|_| magic))
}

/// Row count of a current-format tail log obtained by walking frame headers
/// only. Returns `None` when the header, frame layout, or checksum is invalid.
pub(super) fn scan_tail_log_rows(bytes: &[u8]) -> Option<u64> {
    if bytes.len() < 8 {
        return None;
    }
    let magic = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
    let format = TailFormat::from_magic(magic)?;
    let frame_min_len = format.frame_header_len + 4;
    let partition_len = u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize;
    let mut offset = 8usize.saturating_add(partition_len);
    if offset > bytes.len() {
        return None;
    }
    let mut rows = 0u64;
    while offset < bytes.len() {
        // Mirror decode_tail_log: a crash-truncated final frame ends the
        // valid prefix without failing the scan.
        if bytes.len().saturating_sub(offset) < frame_min_len {
            break;
        }
        let payload_len =
            u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap()) as usize;
        let frame_rows = u64::from_le_bytes(bytes[offset + 8..offset + 16].try_into().unwrap());
        let payload_start = offset + format.frame_header_len;
        let payload_end = payload_start.saturating_add(payload_len);
        let frame_end = payload_end.saturating_add(4);
        if frame_end > bytes.len() {
            break;
        }
        let mut hasher = Hasher::new();
        hasher.update(&bytes[offset..payload_end]);
        let stored_crc = u32::from_le_bytes(bytes[payload_end..frame_end].try_into().unwrap());
        if stored_crc != hasher.finalize() {
            return None;
        }
        rows = rows.saturating_add(frame_rows);
        offset = frame_end;
    }
    Some(rows)
}

pub(super) fn read_tail_log_partition_key(path: &Path) -> Result<Option<String>> {
    let mut file = fs::File::open(path)?;
    let mut header = [0u8; 8];
    if let Err(error) = file.read_exact(&mut header) {
        if error.kind() == std::io::ErrorKind::UnexpectedEof {
            return Ok(None);
        }
        return Err(error.into());
    }
    let magic = u32::from_le_bytes(header[0..4].try_into().unwrap());
    if TailFormat::from_magic(magic).is_none() {
        return Ok(None);
    }
    let partition_len = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;
    let mut partition = vec![0u8; partition_len];
    file.read_exact(&mut partition)?;
    String::from_utf8(partition)
        .map(Some)
        .map_err(|_| TaibaiError::Storage("invalid durable tail partition key".into()))
}

/// A tail file shorter than the V2 log header cannot hold even one committed
/// frame: nothing was fsynced into it, so recovery must treat it as absent
/// instead of failing the whole table's recovery. The header is
/// variable-length (magic + partition-key bytes), so both the magic and the
/// full header extent are validated against the file size.
pub(super) const TAIL_LOG_HEADER_LEN: u64 = 8;

// ---------------------------------------------------------------------------
// Damage observability
// ---------------------------------------------------------------------------
//
// Recovery discards damaged durability artifacts by design: a tail torn
// before its first fsync holds no committed rows, a torn flush-intent marker
// proves nothing. The policies are correct — but they used to be silent, so
// data loss after a crash was undiscoverable. Every discard now bumps a
// counter and emits a structured event; `tail_damage_snapshot()` aggregates
// for tests and health reporting.
