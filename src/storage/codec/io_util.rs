use std::io::{self, Cursor, Read};

use crate::common::error::{TaibaiError, Result};

pub fn read_u8(cursor: &mut Cursor<&[u8]>) -> std::result::Result<u8, io::Error> {
    let mut buf = [0u8; 1];
    cursor.read_exact(&mut buf)?;
    Ok(buf[0])
}

pub fn read_u32_le(cursor: &mut Cursor<&[u8]>) -> std::result::Result<u32, io::Error> {
    let mut buf = [0u8; 4];
    cursor.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

pub fn read_u64_le(cursor: &mut Cursor<&[u8]>) -> std::result::Result<u64, io::Error> {
    let mut buf = [0u8; 8];
    cursor.read_exact(&mut buf)?;
    Ok(u64::from_le_bytes(buf))
}

pub fn write_varint(buf: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        buf.push((value as u8) | 0x80);
        value >>= 7;
    }
    buf.push(value as u8);
}

pub fn read_varint(cursor: &mut Cursor<&[u8]>) -> Result<u64> {
    let mut value = 0u64;
    let mut shift = 0u32;
    loop {
        let byte = read_u8(cursor)
            .map_err(|e| TaibaiError::Storage(format!("varint read error: {}", e)))?;
        value |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
        shift += 7;
        if shift >= 64 {
            return Err(TaibaiError::Storage("varint too long".into()));
        }
    }
}

pub fn zigzag_encode(value: i64) -> u64 {
    ((value << 1) ^ (value >> 63)) as u64
}

pub fn zigzag_decode(value: u64) -> i64 {
    ((value >> 1) as i64) ^ (-((value & 1) as i64))
}

pub fn read_len_prefixed_str(cursor: &mut Cursor<&[u8]>) -> std::result::Result<String, io::Error> {
    let len = read_u32_le(cursor)? as usize;
    let mut buf = vec![0u8; len];
    cursor.read_exact(&mut buf)?;
    Ok(String::from_utf8_lossy(&buf).into_owned())
}

pub fn encode_len_prefixed_str(buf: &mut Vec<u8>, s: &str) {
    buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
    buf.extend_from_slice(s.as_bytes());
}
