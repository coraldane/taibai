use crate::common::error::{TaibaiError, Result};
use bytes::Bytes;

pub const MAGIC: [u8; 2] = [0x01, 0xDB];
pub const HEADER_SIZE: usize = 7;
pub const MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024;

pub const MSG_TYPE_QUERY: u8 = 0x01;
pub const MSG_TYPE_RESULT: u8 = 0x02;
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

pub const MSG_TYPE_BEGIN_TXN: u8 = 0x30;
pub const MSG_TYPE_COMMIT_TXN: u8 = 0x31;
pub const MSG_TYPE_ROLLBACK_TXN: u8 = 0x32;

pub const MSG_TYPE_CURSOR: u8 = 0x40;
pub const MSG_TYPE_CURSOR_FETCH: u8 = 0x41;
pub const MSG_TYPE_CURSOR_CLOSE: u8 = 0x42;

pub const MSG_TYPE_PREPARE: u8 = 0x50;
pub const MSG_TYPE_EXECUTE: u8 = 0x51;
pub const MSG_TYPE_CLOSE_STMT: u8 = 0x52;
pub const MSG_TYPE_BATCH_EXECUTE: u8 = 0x53;

pub const MSG_TYPE_CHANGE_SUBSCRIBE: u8 = 0x60;
pub const MSG_TYPE_CHANGE_EVENT: u8 = 0x61;
pub const MSG_TYPE_CHANGE_ACK: u8 = 0x62;
pub const MSG_TYPE_CHANGE_ACK_STATE: u8 = 0x63;

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

pub fn write_len_prefixed_bytes(buf: &mut Vec<u8>, data: &[u8]) {
    buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
    buf.extend_from_slice(data);
}
