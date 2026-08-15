use std::io::{Cursor, Read};

use crate::common::error::{Result, TaibaiError};
use crate::protocol::types::DataType;
use crate::storage::codec::encoding::EncodingType;
use crate::storage::codec::io_util::{
    read_u32_le, read_varint, write_varint, zigzag_decode, zigzag_encode,
};
use crate::types::array::{ColumnArray, Utf8Buffer};
use crate::types::batch::ColumnDef;
use crate::types::bitmap::NullBitmap;

pub(crate) const COLUMN_ENCODING_MAGIC: &[u8; 4] = b"MXE1";

pub fn encode_column(col: &ColumnArray, offset: usize, count: usize) -> Result<Vec<u8>> {
    let mut buf = Vec::new();

    let null_flags: Vec<bool> = (offset..offset + count)
        .map(|i| col.bitmap().is_null(i))
        .collect();
    let bitmap = NullBitmap::from_bools(&null_flags);
    let bitmap_bytes = bitmap.to_bytes();

    buf.extend_from_slice(&(bitmap_bytes.len() as u32).to_le_bytes());
    buf.extend_from_slice(&bitmap_bytes);

    if let Some(encoded) = encode_column_adaptive(col, offset, count) {
        buf.extend_from_slice(COLUMN_ENCODING_MAGIC);
        buf.push(encoded.0.to_u8());
        buf.extend_from_slice(&encoded.1);
        return Ok(buf);
    }

    for i in offset..offset + count {
        if col.bitmap().is_null(i) {
            continue;
        }
        encode_value(&mut buf, col, i);
    }

    Ok(buf)
}

fn encode_column_adaptive(
    col: &ColumnArray,
    offset: usize,
    count: usize,
) -> Option<(EncodingType, Vec<u8>)> {
    if count < 128 {
        return None;
    }

    match col {
        ColumnArray::Int8 { data, bitmap } => {
            encode_signed_numeric_adaptive(data, bitmap, offset, count, |v| *v as i64, 1)
        }
        ColumnArray::Int16 { data, bitmap } => {
            encode_signed_numeric_adaptive(data, bitmap, offset, count, |v| *v as i64, 2)
        }
        ColumnArray::Int32 { data, bitmap } => {
            encode_signed_numeric_adaptive(data, bitmap, offset, count, |v| *v as i64, 4)
        }
        ColumnArray::Int64 { data, bitmap } => {
            encode_signed_numeric_adaptive(data, bitmap, offset, count, |v| *v, 8)
        }
        ColumnArray::Decimal { data, bitmap } => {
            encode_signed_numeric_adaptive(data, bitmap, offset, count, |v| *v, 8)
        }
        ColumnArray::Date { data, bitmap } => {
            encode_signed_numeric_adaptive(data, bitmap, offset, count, |v| *v as i64, 4)
        }
        ColumnArray::Time { data, bitmap } => {
            encode_signed_numeric_adaptive(data, bitmap, offset, count, |v| *v, 8)
        }
        ColumnArray::Timestamp { data, bitmap } => {
            encode_signed_numeric_adaptive(data, bitmap, offset, count, |v| *v, 8)
        }
        ColumnArray::Interval { data, bitmap } => {
            encode_signed_numeric_adaptive(data, bitmap, offset, count, |v| *v, 8)
        }
        ColumnArray::UInt8 { data, bitmap } => {
            encode_unsigned_numeric_adaptive(data, bitmap, offset, count, |v| *v as u64, 1)
        }
        ColumnArray::UInt16 { data, bitmap } => {
            encode_unsigned_numeric_adaptive(data, bitmap, offset, count, |v| *v as u64, 2)
        }
        ColumnArray::UInt32 { data, bitmap } => {
            encode_unsigned_numeric_adaptive(data, bitmap, offset, count, |v| *v as u64, 4)
        }
        ColumnArray::UInt64 { data, bitmap } => {
            encode_unsigned_numeric_adaptive(data, bitmap, offset, count, |v| *v, 8)
        }
        ColumnArray::VarChar { data, bitmap } => {
            encode_varchar_dictionary_adaptive(data, bitmap, offset, count)
        }
        _ => None,
    }
}

fn encode_signed_numeric_adaptive<T, F>(
    data: &[T],
    bitmap: &NullBitmap,
    offset: usize,
    count: usize,
    to_i64: F,
    plain_width: usize,
) -> Option<(EncodingType, Vec<u8>)>
where
    F: Fn(&T) -> i64,
{
    let values: Vec<i64> = (offset..offset + count)
        .filter(|&i| !bitmap.is_null(i))
        .map(|i| to_i64(&data[i]))
        .collect();
    choose_numeric_encoding(&values, plain_width)
}

fn encode_unsigned_numeric_adaptive<T, F>(
    data: &[T],
    bitmap: &NullBitmap,
    offset: usize,
    count: usize,
    to_u64: F,
    plain_width: usize,
) -> Option<(EncodingType, Vec<u8>)>
where
    F: Fn(&T) -> u64,
{
    let mut values = Vec::new();
    for (i, item) in data.iter().enumerate().skip(offset).take(count) {
        if bitmap.is_null(i) {
            continue;
        }
        let v = to_u64(item);
        if v > i64::MAX as u64 {
            return None;
        }
        values.push(v as i64);
    }
    choose_numeric_encoding(&values, plain_width)
}

fn choose_numeric_encoding(values: &[i64], plain_width: usize) -> Option<(EncodingType, Vec<u8>)> {
    if values.len() < 128 {
        return None;
    }
    let plain_size = values.len().saturating_mul(plain_width);

    let mut best: Option<(EncodingType, Vec<u8>)> = None;
    let rle = encode_i64_rle_payload(values);
    if rle.len() + 5 < plain_size {
        best = Some((EncodingType::Rle, rle));
    }

    if values.windows(2).all(|w| w[0] <= w[1]) {
        let delta = encode_i64_delta_payload(values);
        if (delta.len() + 5 < plain_size || plain_width == 8 && delta.len() < plain_size)
            && best
                .as_ref()
                .map(|(_, payload)| delta.len() < payload.len())
                .unwrap_or(true)
        {
            best = Some((EncodingType::Delta, delta));
        }
    }

    best
}

fn encode_i64_rle_payload(values: &[i64]) -> Vec<u8> {
    let mut runs = Vec::new();
    let mut current = values[0];
    let mut len = 1u32;
    for &value in &values[1..] {
        if value == current && len < u32::MAX {
            len += 1;
        } else {
            runs.push((current, len));
            current = value;
            len = 1;
        }
    }
    runs.push((current, len));

    let mut out = Vec::with_capacity(4 + runs.len() * 12);
    out.extend_from_slice(&(runs.len() as u32).to_le_bytes());
    for (value, len) in runs {
        out.extend_from_slice(&value.to_le_bytes());
        out.extend_from_slice(&len.to_le_bytes());
    }
    out
}

fn encode_i64_delta_payload(values: &[i64]) -> Vec<u8> {
    let mut out = Vec::with_capacity(12 + values.len() * 2);
    out.extend_from_slice(&(values.len() as u32).to_le_bytes());
    out.extend_from_slice(&values[0].to_le_bytes());
    let mut prev = values[0];
    for &value in &values[1..] {
        write_varint(&mut out, zigzag_encode(value - prev));
        prev = value;
    }
    out
}

fn encode_varchar_dictionary_adaptive(
    data: &Utf8Buffer,
    bitmap: &NullBitmap,
    offset: usize,
    count: usize,
) -> Option<(EncodingType, Vec<u8>)> {
    let mut dict = Vec::<&str>::new();
    let mut index_map = std::collections::HashMap::<&str, u32>::new();
    let mut indices = Vec::new();
    let mut plain_size = 0usize;

    for (i, item) in data.iter().enumerate().skip(offset).take(count) {
        if bitmap.is_null(i) {
            continue;
        }
        let value = item;
        plain_size += 4 + value.len();
        let idx = if let Some(&idx) = index_map.get(value) {
            idx
        } else {
            if dict.len() >= u32::MAX as usize {
                return None;
            }
            let idx = dict.len() as u32;
            dict.push(value);
            index_map.insert(value, idx);
            idx
        };
        indices.push(idx);
    }

    if indices.len() < 128 || dict.len() >= indices.len() / 2 {
        return None;
    }

    let mut out = Vec::new();
    out.extend_from_slice(&(dict.len() as u32).to_le_bytes());
    for value in dict {
        out.extend_from_slice(&(value.len() as u32).to_le_bytes());
        out.extend_from_slice(value.as_bytes());
    }
    out.extend_from_slice(&(indices.len() as u32).to_le_bytes());
    for idx in indices {
        out.extend_from_slice(&idx.to_le_bytes());
    }

    if out.len() + 5 < plain_size {
        Some((EncodingType::Dictionary, out))
    } else {
        None
    }
}

pub fn encode_value(buf: &mut Vec<u8>, col: &ColumnArray, idx: usize) {
    match col {
        ColumnArray::Bool { data, .. } => buf.push(data[idx]),
        ColumnArray::Int8 { data, .. } => buf.push(data[idx] as u8),
        ColumnArray::Int16 { data, .. } => buf.extend_from_slice(&data[idx].to_le_bytes()),
        ColumnArray::Int32 { data, .. } => buf.extend_from_slice(&data[idx].to_le_bytes()),
        ColumnArray::Int64 { data, .. } => buf.extend_from_slice(&data[idx].to_le_bytes()),
        ColumnArray::UInt8 { data, .. } => buf.push(data[idx]),
        ColumnArray::UInt16 { data, .. } => buf.extend_from_slice(&data[idx].to_le_bytes()),
        ColumnArray::UInt32 { data, .. } => buf.extend_from_slice(&data[idx].to_le_bytes()),
        ColumnArray::UInt64 { data, .. } => buf.extend_from_slice(&data[idx].to_le_bytes()),
        ColumnArray::Float32 { data, .. } => buf.extend_from_slice(&data[idx].to_le_bytes()),
        ColumnArray::Float64 { data, .. } => buf.extend_from_slice(&data[idx].to_le_bytes()),
        ColumnArray::Decimal { data, .. } => buf.extend_from_slice(&data[idx].to_le_bytes()),
        ColumnArray::Date { data, .. } => buf.extend_from_slice(&data[idx].to_le_bytes()),
        ColumnArray::Time { data, .. } => buf.extend_from_slice(&data[idx].to_le_bytes()),
        ColumnArray::Timestamp { data, .. } => buf.extend_from_slice(&data[idx].to_le_bytes()),
        ColumnArray::Interval { data, .. } => buf.extend_from_slice(&data[idx].to_le_bytes()),
        ColumnArray::VarChar { data, .. } => {
            buf.extend_from_slice(&(data[idx].len() as u32).to_le_bytes());
            buf.extend_from_slice(data[idx].as_bytes());
        }
        ColumnArray::Blob { data, .. } => {
            buf.extend_from_slice(&(data[idx].len() as u32).to_le_bytes());
            buf.extend_from_slice(&data[idx]);
        }
        ColumnArray::Vector { data, .. } => {
            let v = &data[idx];
            buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
            for f in v {
                buf.extend_from_slice(&f.to_le_bytes());
            }
        }
    }
}

pub fn decode_column(data: &[u8], row_count: usize, col_def: &ColumnDef) -> Result<ColumnArray> {
    let mut cursor = Cursor::new(data);
    let bitmap_len = read_u32_le(&mut cursor)? as usize;
    let mut bitmap_data = vec![0u8; bitmap_len];
    cursor.read_exact(&mut bitmap_data)?;
    let bitmap = NullBitmap::from_bytes(&bitmap_data, row_count);

    let remaining_data = &data[4 + bitmap_len..];
    if remaining_data.starts_with(COLUMN_ENCODING_MAGIC) {
        return decode_adaptive_column(remaining_data, row_count, col_def, bitmap);
    }
    let mut dc = Cursor::new(remaining_data);

    macro_rules! read_fixed {
        ($ty:ty, $size:expr, $variant:ident) => {{
            let mut vals = Vec::with_capacity(row_count);
            for i in 0..row_count {
                if bitmap.is_null(i) {
                    vals.push(<$ty>::default());
                } else {
                    let mut b = [0u8; $size];
                    dc.read_exact(&mut b)
                        .map_err(|e| TaibaiError::Storage(format!("read error: {}", e)))?;
                    vals.push(<$ty>::from_le_bytes(b));
                }
            }
            ColumnArray::$variant {
                data: vals.into(),
                bitmap: bitmap.clone(),
            }
        }};
    }

    let col = match col_def.data_type {
        DataType::Bool => {
            let mut vals = Vec::with_capacity(row_count);
            for i in 0..row_count {
                vals.push(if bitmap.is_null(i) {
                    0u8
                } else {
                    let mut b = [0u8; 1];
                    dc.read_exact(&mut b)
                        .map_err(|e| TaibaiError::Storage(format!("read error: {}", e)))?;
                    b[0]
                });
            }
            ColumnArray::Bool {
                data: vals,
                bitmap: bitmap.clone(),
            }
        }
        DataType::Int8 => {
            let mut vals = Vec::with_capacity(row_count);
            for i in 0..row_count {
                vals.push(if bitmap.is_null(i) {
                    0i8
                } else {
                    let mut b = [0u8; 1];
                    dc.read_exact(&mut b)
                        .map_err(|e| TaibaiError::Storage(format!("read error: {}", e)))?;
                    b[0] as i8
                });
            }
            ColumnArray::Int8 {
                data: vals,
                bitmap: bitmap.clone(),
            }
        }
        DataType::Int16 => read_fixed!(i16, 2, Int16),
        DataType::Int32 => read_fixed!(i32, 4, Int32),
        DataType::Int64 => read_fixed!(i64, 8, Int64),
        DataType::UInt8 => {
            let mut vals = Vec::with_capacity(row_count);
            for i in 0..row_count {
                vals.push(if bitmap.is_null(i) {
                    0u8
                } else {
                    let mut b = [0u8; 1];
                    dc.read_exact(&mut b)
                        .map_err(|e| TaibaiError::Storage(format!("read error: {}", e)))?;
                    b[0]
                });
            }
            ColumnArray::UInt8 {
                data: vals,
                bitmap: bitmap.clone(),
            }
        }
        DataType::UInt16 => read_fixed!(u16, 2, UInt16),
        DataType::UInt32 => read_fixed!(u32, 4, UInt32),
        DataType::UInt64 => read_fixed!(u64, 8, UInt64),
        DataType::Float32 => read_fixed!(f32, 4, Float32),
        DataType::Float64 => read_fixed!(f64, 8, Float64),
        DataType::Decimal => read_fixed!(i64, 8, Decimal),
        DataType::Date => read_fixed!(i32, 4, Date),
        DataType::Time => read_fixed!(i64, 8, Time),
        DataType::Timestamp => read_fixed!(i64, 8, Timestamp),
        DataType::Interval => read_fixed!(i64, 8, Interval),
        DataType::VarChar => {
            let mut vals = Vec::with_capacity(row_count);
            for i in 0..row_count {
                if bitmap.is_null(i) {
                    vals.push(String::new());
                } else {
                    let len = read_u32_le(&mut dc).unwrap_or(0) as usize;
                    let mut s = vec![0u8; len];
                    dc.read_exact(&mut s).unwrap_or(());
                    vals.push(String::from_utf8_lossy(&s).into_owned());
                }
            }
            ColumnArray::VarChar {
                data: vals.into(),
                bitmap: bitmap.clone(),
            }
        }
        DataType::Blob => {
            let mut vals = Vec::with_capacity(row_count);
            for i in 0..row_count {
                if bitmap.is_null(i) {
                    vals.push(Vec::new());
                } else {
                    let len = read_u32_le(&mut dc).unwrap_or(0) as usize;
                    let mut v = vec![0u8; len];
                    dc.read_exact(&mut v).unwrap_or(());
                    vals.push(v);
                }
            }
            ColumnArray::Blob {
                data: vals.into(),
                bitmap: bitmap.clone(),
            }
        }
        DataType::Vector => {
            let mut vals = Vec::with_capacity(row_count);
            for i in 0..row_count {
                if bitmap.is_null(i) {
                    vals.push(Vec::new());
                } else {
                    let count = read_u32_le(&mut dc).unwrap_or(0) as usize;
                    let mut v = Vec::with_capacity(count);
                    for _ in 0..count {
                        let mut b = [0u8; 4];
                        dc.read_exact(&mut b).unwrap_or(());
                        v.push(f32::from_le_bytes(b));
                    }
                    vals.push(v);
                }
            }
            ColumnArray::Vector {
                data: vals,
                bitmap: bitmap.clone(),
            }
        }
    };

    Ok(col)
}

pub(crate) fn decode_adaptive_column(
    data: &[u8],
    row_count: usize,
    col_def: &ColumnDef,
    bitmap: NullBitmap,
) -> Result<ColumnArray> {
    if data.len() < COLUMN_ENCODING_MAGIC.len() + 1 {
        return Err(TaibaiError::Storage(
            "adaptive column header truncated".into(),
        ));
    }
    let encoding = EncodingType::from_u8(data[COLUMN_ENCODING_MAGIC.len()])?;
    let payload = &data[COLUMN_ENCODING_MAGIC.len() + 1..];

    match encoding {
        EncodingType::Rle => {
            let values = decode_i64_rle_payload(payload)?;
            build_numeric_column_from_i64(values, row_count, col_def, bitmap)
        }
        EncodingType::Delta => {
            let values = decode_i64_delta_payload(payload)?;
            build_numeric_column_from_i64(values, row_count, col_def, bitmap)
        }
        EncodingType::Dictionary if col_def.data_type == DataType::VarChar => {
            let values = decode_varchar_dictionary_payload(payload)?;
            build_varchar_column(values, row_count, bitmap)
        }
        other => Err(TaibaiError::Storage(format!(
            "unsupported adaptive column encoding {:?} for {}",
            other,
            col_def.data_type.display_name()
        ))),
    }
}

pub(crate) fn decode_i64_rle_payload(payload: &[u8]) -> Result<Vec<i64>> {
    let mut cursor = Cursor::new(payload);
    let run_count = read_u32_le(&mut cursor)? as usize;
    let mut values = Vec::new();
    for _ in 0..run_count {
        let mut value_buf = [0u8; 8];
        cursor.read_exact(&mut value_buf)?;
        let value = i64::from_le_bytes(value_buf);
        let len = read_u32_le(&mut cursor)? as usize;
        values.extend(std::iter::repeat_n(value, len));
    }
    Ok(values)
}

pub(crate) fn decode_i64_delta_payload(payload: &[u8]) -> Result<Vec<i64>> {
    let mut cursor = Cursor::new(payload);
    let count = read_u32_le(&mut cursor)? as usize;
    if count == 0 {
        return Ok(Vec::new());
    }
    let mut base_buf = [0u8; 8];
    cursor.read_exact(&mut base_buf)?;
    let mut values = Vec::with_capacity(count);
    let mut prev = i64::from_le_bytes(base_buf);
    values.push(prev);
    for _ in 1..count {
        let encoded = read_varint(&mut cursor)?;
        let delta = zigzag_decode(encoded);
        let next = prev
            .checked_add(delta)
            .ok_or_else(|| TaibaiError::Storage("delta decoded value overflowed i64".into()))?;
        values.push(next);
        prev = next;
    }
    Ok(values)
}

fn decode_varchar_dictionary_payload(payload: &[u8]) -> Result<Vec<String>> {
    let mut cursor = Cursor::new(payload);
    let dict_count = read_u32_le(&mut cursor)? as usize;
    let mut dict = Vec::with_capacity(dict_count);
    for _ in 0..dict_count {
        let len = read_u32_le(&mut cursor)? as usize;
        let mut bytes = vec![0u8; len];
        cursor.read_exact(&mut bytes)?;
        dict.push(String::from_utf8_lossy(&bytes).into_owned());
    }
    let index_count = read_u32_le(&mut cursor)? as usize;
    let mut values = Vec::with_capacity(index_count);
    for _ in 0..index_count {
        let idx = read_u32_le(&mut cursor)? as usize;
        let value = dict.get(idx).ok_or_else(|| {
            TaibaiError::Storage(format!("dictionary index {} out of bounds", idx))
        })?;
        values.push(value.clone());
    }
    Ok(values)
}

fn build_numeric_column_from_i64(
    values: Vec<i64>,
    row_count: usize,
    col_def: &ColumnDef,
    bitmap: NullBitmap,
) -> Result<ColumnArray> {
    let expected = row_count.saturating_sub(bitmap.null_count());
    if values.len() != expected {
        return Err(TaibaiError::Storage(format!(
            "adaptive numeric value count mismatch: got {}, expected {}",
            values.len(),
            expected
        )));
    }

    let mut value_pos = 0usize;
    macro_rules! build_signed {
        ($variant:ident, $ty:ty) => {{
            let mut out = Vec::with_capacity(row_count);
            for row in 0..row_count {
                if bitmap.is_null(row) {
                    out.push(<$ty>::default());
                } else {
                    out.push(values[value_pos] as $ty);
                    value_pos += 1;
                }
            }
            ColumnArray::$variant { data: out, bitmap }
        }};
    }
    macro_rules! build_unsigned {
        ($variant:ident, $ty:ty) => {{
            let mut out = Vec::with_capacity(row_count);
            for row in 0..row_count {
                if bitmap.is_null(row) {
                    out.push(<$ty>::default());
                } else {
                    out.push(values[value_pos] as $ty);
                    value_pos += 1;
                }
            }
            ColumnArray::$variant { data: out, bitmap }
        }};
    }

    Ok(match col_def.data_type {
        DataType::Int8 => build_signed!(Int8, i8),
        DataType::Int16 => build_signed!(Int16, i16),
        DataType::Int32 => build_signed!(Int32, i32),
        DataType::Int64 => build_signed!(Int64, i64),
        DataType::Decimal => build_signed!(Decimal, i64),
        DataType::Date => build_signed!(Date, i32),
        DataType::Time => build_signed!(Time, i64),
        DataType::Timestamp => build_signed!(Timestamp, i64),
        DataType::Interval => build_signed!(Interval, i64),
        DataType::UInt8 => build_unsigned!(UInt8, u8),
        DataType::UInt16 => build_unsigned!(UInt16, u16),
        DataType::UInt32 => build_unsigned!(UInt32, u32),
        DataType::UInt64 => build_unsigned!(UInt64, u64),
        other => {
            return Err(TaibaiError::Storage(format!(
                "adaptive numeric encoding cannot decode {}",
                other.display_name()
            )))
        }
    })
}

fn build_varchar_column(
    values: Vec<String>,
    row_count: usize,
    bitmap: NullBitmap,
) -> Result<ColumnArray> {
    let expected = row_count.saturating_sub(bitmap.null_count());
    if values.len() != expected {
        return Err(TaibaiError::Storage(format!(
            "adaptive varchar value count mismatch: got {}, expected {}",
            values.len(),
            expected
        )));
    }

    let mut data = Vec::with_capacity(row_count);
    let mut value_pos = 0usize;
    for row in 0..row_count {
        if bitmap.is_null(row) {
            data.push(String::new());
        } else {
            data.push(values[value_pos].clone());
            value_pos += 1;
        }
    }
    Ok(ColumnArray::VarChar {
        data: data.into(),
        bitmap,
    })
}

fn project_column(col: &ColumnArray, selected_rows: &[usize]) -> Result<ColumnArray> {
    let mut out = ColumnArray::empty(col.data_type());
    for &row in selected_rows {
        if row >= col.len() {
            return Err(TaibaiError::Storage(format!(
                "selected row {} out of bounds {}",
                row,
                col.len()
            )));
        }
        out.push(&col.scalar(row))?;
    }
    Ok(out)
}

pub fn decode_column_selected(
    data: &[u8],
    row_count: usize,
    col_def: &ColumnDef,
    selected_rows: &[usize],
) -> Result<ColumnArray> {
    let mut cursor = Cursor::new(data);
    let bitmap_len = read_u32_le(&mut cursor)? as usize;
    let mut bitmap_data = vec![0u8; bitmap_len];
    cursor.read_exact(&mut bitmap_data)?;
    let bitmap = NullBitmap::from_bytes(&bitmap_data, row_count);
    let values = &data[4 + bitmap_len..];

    if values.starts_with(COLUMN_ENCODING_MAGIC) {
        let decoded = decode_adaptive_column(values, row_count, col_def, bitmap)?;
        return project_column(&decoded, selected_rows);
    }

    macro_rules! decode_fixed_variant {
        ($ty:ty, $width:expr, $variant:ident) => {
            decode_selected_fixed::<$ty, $width, _>(
                values,
                &bitmap,
                row_count,
                selected_rows,
                |data, bitmap| ColumnArray::$variant { data, bitmap },
            )
        };
    }

    match col_def.data_type {
        DataType::Bool => decode_fixed_variant!(u8, 1, Bool),
        DataType::Int8 => decode_fixed_variant!(i8, 1, Int8),
        DataType::Int16 => decode_fixed_variant!(i16, 2, Int16),
        DataType::Int32 => decode_fixed_variant!(i32, 4, Int32),
        DataType::Int64 => decode_fixed_variant!(i64, 8, Int64),
        DataType::UInt8 => decode_fixed_variant!(u8, 1, UInt8),
        DataType::UInt16 => decode_fixed_variant!(u16, 2, UInt16),
        DataType::UInt32 => decode_fixed_variant!(u32, 4, UInt32),
        DataType::UInt64 => decode_fixed_variant!(u64, 8, UInt64),
        DataType::Float32 => decode_fixed_variant!(f32, 4, Float32),
        DataType::Float64 => decode_fixed_variant!(f64, 8, Float64),
        DataType::Decimal => decode_fixed_variant!(i64, 8, Decimal),
        DataType::Date => decode_fixed_variant!(i32, 4, Date),
        DataType::Time => decode_fixed_variant!(i64, 8, Time),
        DataType::Timestamp => decode_fixed_variant!(i64, 8, Timestamp),
        DataType::Interval => decode_fixed_variant!(i64, 8, Interval),
        DataType::VarChar => decode_selected_varchar(values, &bitmap, row_count, selected_rows),
        DataType::Blob => decode_selected_blob(values, &bitmap, row_count, selected_rows),
        DataType::Vector => decode_selected_vector(values, &bitmap, row_count, selected_rows),
    }
}

trait FixedLe: Copy + Default {
    fn from_le_bytes(bytes: &[u8]) -> Self;
}

macro_rules! impl_fixed_le {
    ($ty:ty, $size:expr) => {
        impl FixedLe for $ty {
            fn from_le_bytes(bytes: &[u8]) -> Self {
                <$ty>::from_le_bytes(bytes[..$size].try_into().unwrap())
            }
        }
    };
}

impl_fixed_le!(u8, 1);
impl_fixed_le!(i8, 1);
impl_fixed_le!(i16, 2);
impl_fixed_le!(i32, 4);
impl_fixed_le!(i64, 8);
impl_fixed_le!(u16, 2);
impl_fixed_le!(u32, 4);
impl_fixed_le!(u64, 8);
impl_fixed_le!(f32, 4);
impl_fixed_le!(f64, 8);

fn selected_bitmap(bitmap: &NullBitmap, selected_rows: &[usize]) -> NullBitmap {
    let null_flags: Vec<bool> = selected_rows
        .iter()
        .map(|&row| bitmap.is_null(row))
        .collect();
    NullBitmap::from_bools(&null_flags)
}

fn decode_selected_fixed<T, const WIDTH: usize, F>(
    values: &[u8],
    bitmap: &NullBitmap,
    row_count: usize,
    selected_rows: &[usize],
    build: F,
) -> Result<ColumnArray>
where
    T: FixedLe,
    F: FnOnce(Vec<T>, NullBitmap) -> ColumnArray,
{
    let mut out = Vec::with_capacity(selected_rows.len());
    let out_bitmap = selected_bitmap(bitmap, selected_rows);

    if bitmap.null_count() == 0 {
        for &row in selected_rows {
            if row >= row_count {
                return Err(TaibaiError::Storage(format!(
                    "selected row {} out of bounds {}",
                    row, row_count
                )));
            }
            let offset = row
                .checked_mul(WIDTH)
                .ok_or_else(|| TaibaiError::Storage("fixed column offset overflow".into()))?;
            let end = offset + WIDTH;
            if end > values.len() {
                return Err(TaibaiError::Storage(
                    "fixed column selected offset exceeds data".into(),
                ));
            }
            out.push(T::from_le_bytes(&values[offset..end]));
        }
        return Ok(build(out, out_bitmap));
    }

    let mut selected_pos = 0usize;
    let mut value_offset = 0usize;
    for row in 0..row_count {
        let is_selected = selected_pos < selected_rows.len() && selected_rows[selected_pos] == row;
        if bitmap.is_null(row) {
            if is_selected {
                out.push(T::default());
                selected_pos += 1;
            }
            continue;
        }

        let end = value_offset + WIDTH;
        if end > values.len() {
            return Err(TaibaiError::Storage(
                "fixed column selected scan exceeds data".into(),
            ));
        }
        if is_selected {
            out.push(T::from_le_bytes(&values[value_offset..end]));
            selected_pos += 1;
            if selected_pos == selected_rows.len() {
                break;
            }
        }
        value_offset = end;
    }

    Ok(build(out, out_bitmap))
}

fn decode_selected_varchar(
    values: &[u8],
    bitmap: &NullBitmap,
    row_count: usize,
    selected_rows: &[usize],
) -> Result<ColumnArray> {
    let mut out = Vec::with_capacity(selected_rows.len());
    let out_bitmap = selected_bitmap(bitmap, selected_rows);
    let mut selected_pos = 0usize;
    let mut value_offset = 0usize;

    for row in 0..row_count {
        let is_selected = selected_pos < selected_rows.len() && selected_rows[selected_pos] == row;
        if bitmap.is_null(row) {
            if is_selected {
                out.push(String::new());
                selected_pos += 1;
            }
            continue;
        }
        if value_offset + 4 > values.len() {
            return Err(TaibaiError::Storage(
                "variable column length prefix truncated".into(),
            ));
        }
        let len =
            u32::from_le_bytes(values[value_offset..value_offset + 4].try_into().unwrap()) as usize;
        let start = value_offset + 4;
        let end = start + len;
        if end > values.len() {
            return Err(TaibaiError::Storage(
                "variable column selected value truncated".into(),
            ));
        }
        if is_selected {
            out.push(String::from_utf8_lossy(&values[start..end]).into_owned());
            selected_pos += 1;
            if selected_pos == selected_rows.len() {
                break;
            }
        }
        value_offset = end;
    }

    Ok(ColumnArray::VarChar {
        data: out.into(),
        bitmap: out_bitmap,
    })
}

fn decode_selected_blob(
    values: &[u8],
    bitmap: &NullBitmap,
    row_count: usize,
    selected_rows: &[usize],
) -> Result<ColumnArray> {
    let mut out = Vec::with_capacity(selected_rows.len());
    let out_bitmap = selected_bitmap(bitmap, selected_rows);
    let mut selected_pos = 0usize;
    let mut value_offset = 0usize;

    for row in 0..row_count {
        let is_selected = selected_pos < selected_rows.len() && selected_rows[selected_pos] == row;
        if bitmap.is_null(row) {
            if is_selected {
                out.push(Vec::new());
                selected_pos += 1;
            }
            continue;
        }
        if value_offset + 4 > values.len() {
            return Err(TaibaiError::Storage(
                "blob column length prefix truncated".into(),
            ));
        }
        let len =
            u32::from_le_bytes(values[value_offset..value_offset + 4].try_into().unwrap()) as usize;
        let start = value_offset + 4;
        let end = start + len;
        if end > values.len() {
            return Err(TaibaiError::Storage(
                "blob column selected value truncated".into(),
            ));
        }
        if is_selected {
            out.push(values[start..end].to_vec());
            selected_pos += 1;
            if selected_pos == selected_rows.len() {
                break;
            }
        }
        value_offset = end;
    }

    Ok(ColumnArray::Blob {
        data: out.into(),
        bitmap: out_bitmap,
    })
}

fn decode_selected_vector(
    values: &[u8],
    bitmap: &NullBitmap,
    row_count: usize,
    selected_rows: &[usize],
) -> Result<ColumnArray> {
    let mut out = Vec::with_capacity(selected_rows.len());
    let out_bitmap = selected_bitmap(bitmap, selected_rows);
    let mut selected_pos = 0usize;
    let mut value_offset = 0usize;

    for row in 0..row_count {
        let is_selected = selected_pos < selected_rows.len() && selected_rows[selected_pos] == row;
        if bitmap.is_null(row) {
            if is_selected {
                out.push(Vec::new());
                selected_pos += 1;
            }
            continue;
        }
        if value_offset + 4 > values.len() {
            return Err(TaibaiError::Storage(
                "vector column length prefix truncated".into(),
            ));
        }
        let count =
            u32::from_le_bytes(values[value_offset..value_offset + 4].try_into().unwrap()) as usize;
        let start = value_offset + 4;
        let end = start + count * 4;
        if end > values.len() {
            return Err(TaibaiError::Storage(
                "vector column selected value truncated".into(),
            ));
        }
        if is_selected {
            let mut v = Vec::with_capacity(count);
            for chunk in values[start..end].chunks_exact(4) {
                v.push(f32::from_le_bytes(chunk.try_into().unwrap()));
            }
            out.push(v);
            selected_pos += 1;
            if selected_pos == selected_rows.len() {
                break;
            }
        }
        value_offset = end;
    }

    Ok(ColumnArray::Vector {
        data: out,
        bitmap: out_bitmap,
    })
}
