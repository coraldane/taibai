use crate::common::error::{Result, TaibaiError};
use bytes::Bytes;

pub const MAGIC: [u8; 2] = [0x01, 0xDB];
pub const HEADER_SIZE: usize = 7;
pub const MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024;

pub const MSG_TYPE_QUERY: u8 = 0x01;
pub const MSG_TYPE_ERROR: u8 = 0x03;
pub const MSG_TYPE_PING: u8 = 0x04;
pub const MSG_TYPE_PONG: u8 = 0x05;
pub const MSG_TYPE_OK: u8 = 0x07;
pub const MSG_TYPE_LOGIN: u8 = 0x08;
pub const MSG_TYPE_LOGOUT: u8 = 0x09;
pub const MSG_TYPE_AUTH_OK: u8 = 0x0A;
pub const MSG_TYPE_AUTH_FAIL: u8 = 0x0B;

pub const MSG_TYPE_BULK_INSERT_COLUMNAR: u8 = 0x23;
pub const MSG_TYPE_BINARY_QUERY_RESULT: u8 = 0x24;
pub const MSG_TYPE_FLUSH: u8 = 0x25;
pub const MSG_TYPE_RESULT_CHUNK: u8 = 0x26;
pub const MSG_TYPE_RESULT_END: u8 = 0x27;

pub const MSG_TYPE_BEGIN_TXN: u8 = 0x30;
pub const MSG_TYPE_COMMIT_TXN: u8 = 0x31;
pub const MSG_TYPE_ROLLBACK_TXN: u8 = 0x32;

/// Query-execution alias kept for wire compatibility with older clients:
/// the payload is a normal query and the server returns the full result.
/// Cursor semantics (partial FETCH / cursor CLOSE) were never implemented
/// and the former 0x41/0x42 types are no longer reserved; senders receive
/// the standard "unsupported message type" error.
pub const MSG_TYPE_CURSOR: u8 = 0x40;

pub const MSG_TYPE_PREPARE: u8 = 0x50;
pub const MSG_TYPE_EXECUTE: u8 = 0x51;
pub const MSG_TYPE_CLOSE_STMT: u8 = 0x52;
pub const MSG_TYPE_BATCH_EXECUTE: u8 = 0x53;

pub const MSG_TYPE_SUBSCRIBE: u8 = 0x60;
pub const MSG_TYPE_UNSUBSCRIBE: u8 = 0x61;
/// Server push. Payloads are already compressed once by the pubsub fan-out
/// thread (when above the compression threshold), so the codec must not
/// compress them again.
pub const MSG_TYPE_NOTIFICATION: u8 = 0x62;
/// Row-insert change event (same payload layout as MSG_TYPE_NOTIFICATION).
pub const MSG_TYPE_NOTIFICATION_INSERT: u8 = 0x63;
/// Row-delete change event: the payload carries the deleted rows.
pub const MSG_TYPE_NOTIFICATION_DELETE: u8 = 0x64;
/// Row-update change event: the payload carries the post-update rows.
pub const MSG_TYPE_NOTIFICATION_UPDATE: u8 = 0x65;

/// Reliable-subscription request (client -> server). Payload:
/// `[string db][string table][u8 mode][u64 from_seq][string named]
///  [u8 has_filter][...]`
pub const MSG_TYPE_SUBSCRIBE_V2: u8 = 0x66;
/// Reliable-subscription confirmation (server -> client). Payload:
/// `[u64 sub_id][u64 epoch][u64 granted_from][u64 floor]`
pub const MSG_TYPE_SUB_CONFIRMED: u8 = 0x67;
/// Reliable-subscription data push (server -> client). Payload:
/// `[u64 sub_id][u64 seq_start][u64 seq_end][u32 batch_count]
///  [u32 len][bulk batch]...`
pub const MSG_TYPE_SUB_DATA: u8 = 0x68;
/// Reliable-subscription error / resync notice. Payload:
/// `[u64 sub_id][u8 code][u64 aux][string message]`
pub const MSG_TYPE_SUB_ERROR: u8 = 0x69;
/// Reliable-subscription acknowledgement (client -> server). Payload:
/// `[string db][string table][u64 sub_id][u64 seq]`
pub const MSG_TYPE_SUB_ACK: u8 = 0x6A;
/// Reliable-subscription removal (client -> server). Payload:
/// `[string db][string table][u64 sub_id]`
pub const MSG_TYPE_UNSUBSCRIBE_V2: u8 = 0x6B;

/// Replication handshake (standby -> primary, after LOGIN). Payload:
/// `[string user][string password][u64 last_applied_seq]`. The primary
/// replies with REPL events pushed on the connection (or an ERROR frame
/// when auth fails or the standby is behind the event ring).
pub const MSG_TYPE_REPL_HELLO: u8 = 0x70;
/// Replication event push (primary -> standby). Payload:
/// `[u64 rep_seq][u32 len][event bytes]`. Events are already binary; the
/// codec must not re-compress them.
pub const MSG_TYPE_REPL_EVENT: u8 = 0x71;
/// Segment content fetch (standby -> primary). Payload:
/// `[string db][string table][string segment][u64 offset][u32 max_chunk]`.
pub const MSG_TYPE_REPL_SEGMENT_FETCH: u8 = 0x72;
/// Segment content chunk (primary -> standby, via the push queue). Payload:
/// `[u64 total_len][u64 offset][chunk bytes]`. The final chunk satisfies
/// `offset + chunk_len == total_len`.
pub const MSG_TYPE_REPL_SEGMENT_CHUNK: u8 = 0x73;
/// Replication progress acknowledgement (standby -> primary). Payload:
/// `[u64 applied_seq]`.
pub const MSG_TYPE_REPL_ACK: u8 = 0x74;
/// Segment fetch verdict (primary -> standby): the requested segment no
/// longer exists on the primary because a compaction/TTL event in the ring
/// retired it. The replacement (or removal) arrives later in the same
/// stream, so the standby skips the triggering `SegmentUpdate` instead of
/// failing the applier. Emitted only when the retirement is provable from
/// the primary's event ring; unknown files still fail loudly.
pub const MSG_TYPE_REPL_SEGMENT_GONE: u8 = 0x75;

#[derive(Debug, Clone)]
pub struct MessageHeader {
    pub msg_type: u8,
    pub payload_len: u32,
}

impl MessageHeader {
    pub fn encode(&self) -> [u8; HEADER_SIZE] {
        let mut buf = [0u8; HEADER_SIZE];
        buf[0] = MAGIC[0];
        buf[1] = MAGIC[1];
        buf[2] = self.msg_type;
        buf[3..7].copy_from_slice(&self.payload_len.to_le_bytes());
        buf
    }

    pub fn decode(buf: &[u8; HEADER_SIZE]) -> Result<Self> {
        if buf[0] != MAGIC[0] || buf[1] != MAGIC[1] {
            return Err(TaibaiError::Protocol(format!(
                "invalid magic: expected [{:#04X}, {:#04X}], got [{:#04X}, {:#04X}]",
                MAGIC[0], MAGIC[1], buf[0], buf[1]
            )));
        }
        let msg_type = buf[2];
        let payload_len = u32::from_le_bytes([buf[3], buf[4], buf[5], buf[6]]);
        Ok(MessageHeader {
            msg_type,
            payload_len,
        })
    }
}

#[derive(Debug, Clone)]
pub struct Message {
    pub msg_type: u8,
    pub payload: Bytes,
}

impl Message {
    pub fn new(msg_type: u8, payload: Bytes) -> Self {
        Self { msg_type, payload }
    }

    pub fn empty(msg_type: u8) -> Self {
        Self {
            msg_type,
            payload: Bytes::new(),
        }
    }
}

pub fn read_len_prefixed_string(data: &[u8]) -> Result<(&str, &[u8])> {
    if data.len() < 4 {
        return Err(TaibaiError::Protocol(
            "insufficient data for length prefix".into(),
        ));
    }
    let len = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
    if data.len() < 4 + len {
        return Err(TaibaiError::Protocol(format!(
            "string length {len} exceeds remaining data {}",
            data.len() - 4
        )));
    }
    let s = std::str::from_utf8(&data[4..4 + len])
        .map_err(|e| TaibaiError::Protocol(format!("invalid UTF-8 in string: {e}")))?;
    Ok((s, &data[4 + len..]))
}

pub fn write_len_prefixed_string(buf: &mut Vec<u8>, s: &str) {
    let bytes = s.as_bytes();
    buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    buf.extend_from_slice(bytes);
}
