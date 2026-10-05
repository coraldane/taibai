//! Single source of truth for `ScalarValue` parameter wire encoding.
//!
//! One encoder, one decoder, shared by the client (`net/client.rs`) and the
//! server (`net/handler.rs`). These used to be three drifting copies: the
//! client decoder carried a text-form Decimal branch no producer emits and a
//! lossy UTF-8 VarChar decode, while the server was strict. The wire format
//! itself is unchanged: `[u8 DataType tag]` is written by the caller, then
//! the payload produced here.

use crate::common::error::{Result, TaibaiError};
use crate::protocol::types::DataType;
use crate::types::scalar::ScalarValue;

/// Append the payload (without the leading DataType tag) for `val`.
/// Nulls are encoded by the caller as a bare `0u8` tag.
pub fn encode_param_payload(buf: &mut Vec<u8>, val: &ScalarValue) {
    match val {
        ScalarValue::Bool(Some(v)) => buf.push(if *v { 1 } else { 0 }),
        ScalarValue::Int8(Some(v)) => buf.push(*v as u8),
        ScalarValue::Int16(Some(v)) => buf.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::Int32(Some(v)) => buf.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::Int64(Some(v)) => buf.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::UInt8(Some(v)) => buf.push(*v),
        ScalarValue::UInt16(Some(v)) => buf.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::UInt32(Some(v)) => buf.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::UInt64(Some(v)) => buf.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::Float32(Some(v)) => buf.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::Float64(Some(v)) => buf.extend_from_slice(&v.to_le_bytes()),
        // Decimal is an unscaled i64 on the wire; scale lives in the schema.
        ScalarValue::Decimal(Some(v)) => buf.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::Date(Some(v)) => buf.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::Time(Some(v)) => buf.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::Timestamp(Some(v)) => buf.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::Interval(Some(v)) => buf.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::VarChar(Some(v)) => {
            let bytes = v.as_bytes();
            buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(bytes);
        }
        ScalarValue::Blob(Some(v)) => {
            buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
            buf.extend_from_slice(v);
        }
        ScalarValue::Vector(Some(v)) => {
            buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
            for f in v {
                buf.extend_from_slice(&f.to_le_bytes());
            }
        }
        _ => {}
    }
}

/// Full param encoding: DataType tag byte + payload; nulls are a bare `0u8`.
pub fn encode_scalar_param(buf: &mut Vec<u8>, val: &ScalarValue) {
    let tag = match val {
        ScalarValue::Bool(Some(_)) => DataType::Bool,
        ScalarValue::Int8(Some(_)) => DataType::Int8,
        ScalarValue::Int16(Some(_)) => DataType::Int16,
        ScalarValue::Int32(Some(_)) => DataType::Int32,
        ScalarValue::Int64(Some(_)) => DataType::Int64,
        ScalarValue::UInt8(Some(_)) => DataType::UInt8,
        ScalarValue::UInt16(Some(_)) => DataType::UInt16,
        ScalarValue::UInt32(Some(_)) => DataType::UInt32,
        ScalarValue::UInt64(Some(_)) => DataType::UInt64,
        ScalarValue::Float32(Some(_)) => DataType::Float32,
        ScalarValue::Float64(Some(_)) => DataType::Float64,
        ScalarValue::Decimal(Some(_)) => DataType::Decimal,
        ScalarValue::Date(Some(_)) => DataType::Date,
        ScalarValue::Time(Some(_)) => DataType::Time,
        ScalarValue::Timestamp(Some(_)) => DataType::Timestamp,
        ScalarValue::Interval(Some(_)) => DataType::Interval,
        ScalarValue::VarChar(Some(_)) => DataType::VarChar,
        ScalarValue::Blob(Some(_)) => DataType::Blob,
        ScalarValue::Vector(Some(_)) => DataType::Vector,
        _ => {
            buf.push(0u8);
            return;
        }
    };
    buf.push(tag as u8);
    encode_param_payload(buf, val);
}

/// Decode one non-null payload of the given type; returns the value and the
/// bytes consumed. Variable-width strings are strict UTF-8: the encoder only
/// ever writes valid UTF-8, so anything else is wire corruption and must
/// surface as an error instead of being silently replaced.
pub fn decode_param_value(data: &[u8], dt: DataType) -> Result<(ScalarValue, usize)> {
    let too_short = |what: &str| TaibaiError::Protocol(format!("no data for {what} param"));
    let fixed = |v: ScalarValue, width: usize| Ok((v, width));
    match dt {
        DataType::Bool => {
            if data.is_empty() {
                return Err(too_short("Bool"));
            }
            fixed(ScalarValue::Bool(Some(data[0] != 0)), 1)
        }
        DataType::Int8 => {
            if data.is_empty() {
                return Err(too_short("Int8"));
            }
            fixed(ScalarValue::Int8(Some(data[0] as i8)), 1)
        }
        DataType::Int16 => data
            .get(..2)
            .map(|b| ScalarValue::Int16(Some(i16::from_le_bytes(b.try_into().unwrap()))))
            .ok_or_else(|| too_short("Int16"))
            .and_then(|v| fixed(v, 2)),
        DataType::Int32 => data
            .get(..4)
            .map(|b| ScalarValue::Int32(Some(i32::from_le_bytes(b.try_into().unwrap()))))
            .ok_or_else(|| too_short("Int32"))
            .and_then(|v| fixed(v, 4)),
        DataType::Int64 => data
            .get(..8)
            .map(|b| ScalarValue::Int64(Some(i64::from_le_bytes(b.try_into().unwrap()))))
            .ok_or_else(|| too_short("Int64"))
            .and_then(|v| fixed(v, 8)),
        DataType::UInt8 => {
            if data.is_empty() {
                return Err(too_short("UInt8"));
            }
            fixed(ScalarValue::UInt8(Some(data[0])), 1)
        }
        DataType::UInt16 => data
            .get(..2)
            .map(|b| ScalarValue::UInt16(Some(u16::from_le_bytes(b.try_into().unwrap()))))
            .ok_or_else(|| too_short("UInt16"))
            .and_then(|v| fixed(v, 2)),
        DataType::UInt32 => data
            .get(..4)
            .map(|b| ScalarValue::UInt32(Some(u32::from_le_bytes(b.try_into().unwrap()))))
            .ok_or_else(|| too_short("UInt32"))
            .and_then(|v| fixed(v, 4)),
        DataType::UInt64 => data
            .get(..8)
            .map(|b| ScalarValue::UInt64(Some(u64::from_le_bytes(b.try_into().unwrap()))))
            .ok_or_else(|| too_short("UInt64"))
            .and_then(|v| fixed(v, 8)),
        DataType::Float32 => data
            .get(..4)
            .map(|b| ScalarValue::Float32(Some(f32::from_le_bytes(b.try_into().unwrap()))))
            .ok_or_else(|| too_short("Float32"))
            .and_then(|v| fixed(v, 4)),
        DataType::Float64 => data
            .get(..8)
            .map(|b| ScalarValue::Float64(Some(f64::from_le_bytes(b.try_into().unwrap()))))
            .ok_or_else(|| too_short("Float64"))
            .and_then(|v| fixed(v, 8)),
        DataType::Decimal => data
            .get(..8)
            .map(|b| ScalarValue::Decimal(Some(i64::from_le_bytes(b.try_into().unwrap()))))
            .ok_or_else(|| too_short("Decimal"))
            .and_then(|v| fixed(v, 8)),
        DataType::Date => data
            .get(..4)
            .map(|b| ScalarValue::Date(Some(i32::from_le_bytes(b.try_into().unwrap()))))
            .ok_or_else(|| too_short("Date"))
            .and_then(|v| fixed(v, 4)),
        DataType::Time => data
            .get(..8)
            .map(|b| ScalarValue::Time(Some(i64::from_le_bytes(b.try_into().unwrap()))))
            .ok_or_else(|| too_short("Time"))
            .and_then(|v| fixed(v, 8)),
        DataType::Timestamp => data
            .get(..8)
            .map(|b| ScalarValue::Timestamp(Some(i64::from_le_bytes(b.try_into().unwrap()))))
            .ok_or_else(|| too_short("Timestamp"))
            .and_then(|v| fixed(v, 8)),
        DataType::Interval => data
            .get(..8)
            .map(|b| ScalarValue::Interval(Some(i64::from_le_bytes(b.try_into().unwrap()))))
            .ok_or_else(|| too_short("Interval"))
            .and_then(|v| fixed(v, 8)),
        DataType::VarChar => decode_len_prefixed(data, "VarChar", |bytes| {
            std::str::from_utf8(bytes)
                .map(|s| ScalarValue::VarChar(Some(s.to_string())))
                .map_err(|e| TaibaiError::Protocol(format!("invalid UTF-8 in VarChar param: {e}")))
        }),
        DataType::Blob => decode_len_prefixed(data, "Blob", |bytes| {
            Ok(ScalarValue::Blob(Some(bytes.to_vec())))
        }),
        DataType::Vector => {
            let Some(dim_bytes) = data.get(..4) else {
                return Err(too_short("Vector"));
            };
            let dim = u32::from_le_bytes(dim_bytes.try_into().unwrap()) as usize;
            let total = 4 + dim * 4;
            let Some(payload) = data.get(..total) else {
                return Err(TaibaiError::Protocol("Vector param data truncated".into()));
            };
            let mut floats = Vec::with_capacity(dim);
            for i in 0..dim {
                let off = 4 + i * 4;
                floats.push(f32::from_le_bytes(
                    payload[off..off + 4].try_into().unwrap(),
                ));
            }
            Ok((ScalarValue::Vector(Some(floats)), total))
        }
    }
}

fn decode_len_prefixed(
    data: &[u8],
    what: &str,
    make: impl Fn(&[u8]) -> Result<ScalarValue>,
) -> Result<(ScalarValue, usize)> {
    let Some(len_bytes) = data.get(..4) else {
        return Err(TaibaiError::Protocol(format!("no data for {what} param")));
    };
    let len = u32::from_le_bytes(len_bytes.try_into().unwrap()) as usize;
    let Some(payload) = data.get(4..4 + len) else {
        return Err(TaibaiError::Protocol(format!(
            "{what} param data truncated"
        )));
    };
    let value = make(payload)?;
    Ok((value, 4 + len))
}
