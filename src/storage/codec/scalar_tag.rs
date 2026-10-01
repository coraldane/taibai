use std::io::Read;

use crate::common::error::{Result, TaibaiError};
use crate::protocol::types::DataType;
use crate::storage::codec::io_util::{read_u32_le, read_u8};
use crate::types::scalar::ScalarValue;

pub fn null_for_dtype(dtype: DataType) -> ScalarValue {
    match dtype {
        DataType::Bool => ScalarValue::Bool(None),
        DataType::Int8 => ScalarValue::Int8(None),
        DataType::Int16 => ScalarValue::Int16(None),
        DataType::Int32 => ScalarValue::Int32(None),
        DataType::Int64 => ScalarValue::Int64(None),
        DataType::UInt8 => ScalarValue::UInt8(None),
        DataType::UInt16 => ScalarValue::UInt16(None),
        DataType::UInt32 => ScalarValue::UInt32(None),
        DataType::UInt64 => ScalarValue::UInt64(None),
        DataType::Float32 => ScalarValue::Float32(None),
        DataType::Float64 => ScalarValue::Float64(None),
        DataType::Decimal => ScalarValue::Decimal(None),
        DataType::Date => ScalarValue::Date(None),
        DataType::Time => ScalarValue::Time(None),
        DataType::Timestamp => ScalarValue::Timestamp(None),
        DataType::Interval => ScalarValue::Interval(None),
        DataType::VarChar => ScalarValue::VarChar(None),
        DataType::Blob => ScalarValue::Blob(None),
        DataType::Vector => ScalarValue::Vector(None),
    }
}

pub fn encode_scalar_with_tag(buf: &mut Vec<u8>, v: &ScalarValue) {
    let tag = scalar_tag(v);
    buf.push(tag);
    if v.is_null() {
        return;
    }
    match v {
        ScalarValue::Bool(Some(x)) => buf.push(if *x { 1 } else { 0 }),
        ScalarValue::Int8(Some(x)) => buf.push(*x as u8),
        ScalarValue::Int16(Some(x)) => buf.extend_from_slice(&x.to_le_bytes()),
        ScalarValue::Int32(Some(x)) => buf.extend_from_slice(&x.to_le_bytes()),
        ScalarValue::Int64(Some(x)) => buf.extend_from_slice(&x.to_le_bytes()),
        ScalarValue::UInt8(Some(x)) => buf.push(*x),
        ScalarValue::UInt16(Some(x)) => buf.extend_from_slice(&x.to_le_bytes()),
        ScalarValue::UInt32(Some(x)) => buf.extend_from_slice(&x.to_le_bytes()),
        ScalarValue::UInt64(Some(x)) => buf.extend_from_slice(&x.to_le_bytes()),
        ScalarValue::Float32(Some(x)) => buf.extend_from_slice(&x.to_le_bytes()),
        ScalarValue::Float64(Some(x)) => buf.extend_from_slice(&x.to_le_bytes()),
        ScalarValue::Decimal(Some(x)) => buf.extend_from_slice(&x.to_le_bytes()),
        ScalarValue::Date(Some(x)) => buf.extend_from_slice(&x.to_le_bytes()),
        ScalarValue::Time(Some(x)) => buf.extend_from_slice(&x.to_le_bytes()),
        ScalarValue::Timestamp(Some(x)) => buf.extend_from_slice(&x.to_le_bytes()),
        ScalarValue::Interval(Some(x)) => buf.extend_from_slice(&x.to_le_bytes()),
        ScalarValue::VarChar(Some(x)) => {
            buf.extend_from_slice(&(x.len() as u32).to_le_bytes());
            buf.extend_from_slice(x.as_bytes());
        }
        ScalarValue::Blob(Some(x)) => {
            buf.extend_from_slice(&(x.len() as u32).to_le_bytes());
            buf.extend_from_slice(x);
        }
        ScalarValue::Vector(Some(x)) => {
            buf.extend_from_slice(&(x.len() as u32).to_le_bytes());
            for f in x {
                buf.extend_from_slice(&f.to_le_bytes());
            }
        }
        _ => {}
    }
}

fn scalar_tag(v: &ScalarValue) -> u8 {
    let dtype_tag = match v.data_type() {
        DataType::Bool => 1u8,
        DataType::Int8 => 2,
        DataType::Int16 => 3,
        DataType::Int32 => 4,
        DataType::Int64 => 5,
        DataType::UInt8 => 6,
        DataType::UInt16 => 7,
        DataType::UInt32 => 8,
        DataType::UInt64 => 9,
        DataType::Float32 => 10,
        DataType::Float64 => 11,
        DataType::Decimal => 12,
        DataType::Date => 13,
        DataType::Time => 14,
        DataType::Timestamp => 15,
        DataType::Interval => 16,
        DataType::VarChar => 17,
        DataType::Blob => 18,
        DataType::Vector => 19,
    };
    if v.is_null() {
        dtype_tag | 0x80
    } else {
        dtype_tag
    }
}

pub fn decode_scalar_with_tag(cursor: &mut std::io::Cursor<&[u8]>) -> Result<ScalarValue> {
    let tag = read_u8(cursor).map_err(|e| TaibaiError::Storage(format!("read tag: {}", e)))?;
    let is_null = (tag & 0x80) != 0;
    let dtype_tag = tag & 0x7F;
    let dtype = match dtype_tag {
        1 => DataType::Bool,
        2 => DataType::Int8,
        3 => DataType::Int16,
        4 => DataType::Int32,
        5 => DataType::Int64,
        6 => DataType::UInt8,
        7 => DataType::UInt16,
        8 => DataType::UInt32,
        9 => DataType::UInt64,
        10 => DataType::Float32,
        11 => DataType::Float64,
        12 => DataType::Decimal,
        13 => DataType::Date,
        14 => DataType::Time,
        15 => DataType::Timestamp,
        16 => DataType::Interval,
        17 => DataType::VarChar,
        18 => DataType::Blob,
        19 => DataType::Vector,
        other => return Err(TaibaiError::Storage(format!("bad scalar tag: {}", other))),
    };
    if is_null {
        return Ok(null_for_dtype(dtype));
    }
    let v = match dtype {
        DataType::Bool => ScalarValue::Bool(Some(read_u8(cursor)? != 0)),
        DataType::Int8 => ScalarValue::Int8(Some(read_u8(cursor)? as i8)),
        DataType::Int16 => {
            let mut b = [0u8; 2];
            cursor.read_exact(&mut b)?;
            ScalarValue::Int16(Some(i16::from_le_bytes(b)))
        }
        DataType::Int32 => {
            let mut b = [0u8; 4];
            cursor.read_exact(&mut b)?;
            ScalarValue::Int32(Some(i32::from_le_bytes(b)))
        }
        DataType::Int64 => {
            let mut b = [0u8; 8];
            cursor.read_exact(&mut b)?;
            ScalarValue::Int64(Some(i64::from_le_bytes(b)))
        }
        DataType::UInt8 => ScalarValue::UInt8(Some(read_u8(cursor)?)),
        DataType::UInt16 => {
            let mut b = [0u8; 2];
            cursor.read_exact(&mut b)?;
            ScalarValue::UInt16(Some(u16::from_le_bytes(b)))
        }
        DataType::UInt32 => {
            let mut b = [0u8; 4];
            cursor.read_exact(&mut b)?;
            ScalarValue::UInt32(Some(u32::from_le_bytes(b)))
        }
        DataType::UInt64 => {
            let mut b = [0u8; 8];
            cursor.read_exact(&mut b)?;
            ScalarValue::UInt64(Some(u64::from_le_bytes(b)))
        }
        DataType::Float32 => {
            let mut b = [0u8; 4];
            cursor.read_exact(&mut b)?;
            ScalarValue::Float32(Some(f32::from_le_bytes(b)))
        }
        DataType::Float64 => {
            let mut b = [0u8; 8];
            cursor.read_exact(&mut b)?;
            ScalarValue::Float64(Some(f64::from_le_bytes(b)))
        }
        DataType::Decimal => {
            let mut b = [0u8; 8];
            cursor.read_exact(&mut b)?;
            ScalarValue::Decimal(Some(i64::from_le_bytes(b)))
        }
        DataType::Date => {
            let mut b = [0u8; 4];
            cursor.read_exact(&mut b)?;
            ScalarValue::Date(Some(i32::from_le_bytes(b)))
        }
        DataType::Time => {
            let mut b = [0u8; 8];
            cursor.read_exact(&mut b)?;
            ScalarValue::Time(Some(i64::from_le_bytes(b)))
        }
        DataType::Timestamp => {
            let mut b = [0u8; 8];
            cursor.read_exact(&mut b)?;
            ScalarValue::Timestamp(Some(i64::from_le_bytes(b)))
        }
        DataType::Interval => {
            let mut b = [0u8; 8];
            cursor.read_exact(&mut b)?;
            ScalarValue::Interval(Some(i64::from_le_bytes(b)))
        }
        DataType::VarChar => {
            let len = read_u32_le(cursor)? as usize;
            // Validate before allocating: a corrupt length prefix must not
            // zero-fill up to 4GB from a truncated stream.
            if len > cursor.get_ref().len().saturating_sub(cursor.position() as usize) {
                return Err(TaibaiError::Storage("scalar varchar: truncated value".into()));
            }
            let mut b = vec![0u8; len];
            cursor.read_exact(&mut b)?;
            ScalarValue::VarChar(Some(String::from_utf8_lossy(&b).into_owned()))
        }
        DataType::Blob => {
            let len = read_u32_le(cursor)? as usize;
            if len > cursor.get_ref().len().saturating_sub(cursor.position() as usize) {
                return Err(TaibaiError::Storage("scalar blob: truncated value".into()));
            }
            let mut b = vec![0u8; len];
            cursor.read_exact(&mut b)?;
            ScalarValue::Blob(Some(b))
        }
        DataType::Vector => {
            let len = read_u32_le(cursor)? as usize;
            // Validate before allocating: a corrupt length prefix must not
            // preallocate up to 16GB of floats from a truncated stream.
            let len_bytes = len.checked_mul(4).ok_or_else(|| {
                TaibaiError::Storage("scalar vector: element count overflow".into())
            })?;
            if len_bytes > cursor.get_ref().len().saturating_sub(cursor.position() as usize) {
                return Err(TaibaiError::Storage("scalar vector: truncated value".into()));
            }
            let mut v = Vec::with_capacity(len);
            for _ in 0..len {
                let mut b = [0u8; 4];
                cursor.read_exact(&mut b)?;
                v.push(f32::from_le_bytes(b));
            }
            ScalarValue::Vector(Some(v))
        }
    };
    Ok(v)
}
