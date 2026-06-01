use std::io::{Cursor, Read};

use crate::common::error::{MonaxError, Result};
use crate::protocol::types::DataType;
use crate::storage::codec::column_codec::{decode_adaptive_column, COLUMN_ENCODING_MAGIC};
use crate::storage::codec::io_util::read_u32_le;
use crate::types::batch::ColumnDef;
use crate::types::bitmap::NullBitmap;
use crate::types::scalar::ScalarValue;

pub fn selected_eq_rows_from_raw(
    data: &[u8],
    row_count: usize,
    col_def: &ColumnDef,
    value: &ScalarValue,
) -> Result<Vec<usize>> {
    let mut cursor = Cursor::new(data);
    let bitmap_len = read_u32_le(&mut cursor)? as usize;
    let mut bitmap_data = vec![0u8; bitmap_len];
    cursor.read_exact(&mut bitmap_data)?;
    let bitmap = NullBitmap::from_bytes(&bitmap_data, row_count);

    let remaining = &data[4 + bitmap_len..];
    if remaining.starts_with(COLUMN_ENCODING_MAGIC) {
        return selected_eq_rows_adaptive(remaining, row_count, col_def, &bitmap, value);
    }

    match (&col_def.data_type, value) {
        (DataType::VarChar, ScalarValue::VarChar(Some(v))) => {
            selected_eq_rows_varchar(remaining, &bitmap, row_count, v.as_bytes())
        }
        _ => selected_eq_rows_fixed(remaining, &bitmap, row_count, col_def.data_type, value),
    }
}

pub fn count_eq_from_raw(
    data: &[u8],
    row_count: usize,
    col_def: &ColumnDef,
    value: &ScalarValue,
) -> Result<u64> {
    let mut cursor = Cursor::new(data);
    let bitmap_len = read_u32_le(&mut cursor)? as usize;
    let mut bitmap_data = vec![0u8; bitmap_len];
    cursor.read_exact(&mut bitmap_data)?;
    let bitmap = NullBitmap::from_bytes(&bitmap_data, row_count);

    let remaining = &data[4 + bitmap_len..];

    if remaining.starts_with(COLUMN_ENCODING_MAGIC) {
        return count_eq_adaptive(remaining, row_count, col_def, &bitmap, value);
    }

    let count = match (&col_def.data_type, value) {
        (DataType::Int64, ScalarValue::Int64(Some(v))) => {
            count_eq_fixed_le8(remaining, &bitmap, row_count, *v)
        }
        (DataType::Int32, ScalarValue::Int32(Some(v))) => {
            count_eq_fixed_le4(remaining, &bitmap, row_count, *v)
        }
        (DataType::Int16, ScalarValue::Int16(Some(v))) => {
            count_eq_fixed_le2(remaining, &bitmap, row_count, *v)
        }
        (DataType::Int8, ScalarValue::Int8(Some(v))) => {
            count_eq_fixed_le1(remaining, &bitmap, row_count, *v)
        }
        (DataType::UInt64, ScalarValue::UInt64(Some(v))) => {
            count_eq_fixed_le8(remaining, &bitmap, row_count, *v as i64)
        }
        (DataType::UInt32, ScalarValue::UInt32(Some(v))) => {
            count_eq_fixed_le4(remaining, &bitmap, row_count, *v as i32)
        }
        (DataType::UInt16, ScalarValue::UInt16(Some(v))) => {
            count_eq_fixed_le2(remaining, &bitmap, row_count, *v as i16)
        }
        (DataType::UInt8, ScalarValue::UInt8(Some(v))) => {
            count_eq_fixed_le1(remaining, &bitmap, row_count, *v as i8)
        }
        (DataType::Float64, ScalarValue::Float64(Some(v))) => {
            count_eq_fixed_le8f(remaining, &bitmap, row_count, *v)
        }
        (DataType::Float32, ScalarValue::Float32(Some(v))) => {
            count_eq_fixed_le4f(remaining, &bitmap, row_count, *v)
        }
        (DataType::Decimal, ScalarValue::Decimal(Some(v))) => {
            count_eq_fixed_le8(remaining, &bitmap, row_count, *v)
        }
        (DataType::Date, ScalarValue::Date(Some(v))) => {
            count_eq_fixed_le4(remaining, &bitmap, row_count, *v)
        }
        (DataType::Timestamp, ScalarValue::Timestamp(Some(v))) => {
            count_eq_fixed_le8(remaining, &bitmap, row_count, *v)
        }
        (DataType::VarChar, ScalarValue::VarChar(Some(v))) => {
            count_eq_varchar(remaining, &bitmap, row_count, v)
        }
        _ => return Ok(0),
    };

    Ok(count)
}

fn selected_eq_rows_adaptive(
    data: &[u8],
    row_count: usize,
    col_def: &ColumnDef,
    bitmap: &NullBitmap,
    value: &ScalarValue,
) -> Result<Vec<usize>> {
    if data.len() < COLUMN_ENCODING_MAGIC.len() + 1 {
        return Err(MonaxError::Storage(
            "adaptive column header truncated".into(),
        ));
    }
    let encoding =
        crate::storage::codec::encoding::EncodingType::from_u8(data[COLUMN_ENCODING_MAGIC.len()])?;
    let payload = &data[COLUMN_ENCODING_MAGIC.len() + 1..];

    match encoding {
        crate::storage::codec::encoding::EncodingType::Dictionary
            if col_def.data_type == DataType::VarChar =>
        {
            let target = match value {
                ScalarValue::VarChar(Some(s)) => s.as_bytes(),
                _ => return Ok(Vec::new()),
            };
            selected_eq_rows_varchar_dictionary(payload, row_count, bitmap, target)
        }
        _ => {
            let decoded = decode_adaptive_column(data, row_count, col_def, bitmap.clone())?;
            Ok((0..decoded.len())
                .filter(|&row| scalar_values_equal(&decoded.scalar(row), value))
                .collect())
        }
    }
}

fn selected_eq_rows_varchar_dictionary(
    payload: &[u8],
    row_count: usize,
    bitmap: &NullBitmap,
    target: &[u8],
) -> Result<Vec<usize>> {
    let mut cursor = Cursor::new(payload);
    let dict_count = read_u32_le(&mut cursor)? as usize;
    let mut target_idx = None;
    for idx in 0..dict_count {
        let len = read_u32_le(&mut cursor)? as usize;
        let start = cursor.position() as usize;
        let end = start + len;
        if end > payload.len() {
            return Err(MonaxError::Storage("dictionary value truncated".into()));
        }
        if &payload[start..end] == target {
            target_idx = Some(idx as u32);
        }
        cursor.set_position(end as u64);
    }

    let index_count = read_u32_le(&mut cursor)? as usize;
    let Some(target_idx) = target_idx else {
        return Ok(Vec::new());
    };

    let mut selected = Vec::new();
    let mut value_pos = 0usize;
    for row in 0..row_count {
        if bitmap.is_null(row) {
            continue;
        }
        if value_pos >= index_count {
            break;
        }
        let idx = read_u32_le(&mut cursor)?;
        if idx == target_idx {
            selected.push(row);
        }
        value_pos += 1;
    }
    Ok(selected)
}

fn selected_eq_rows_varchar(
    data: &[u8],
    bitmap: &NullBitmap,
    row_count: usize,
    target: &[u8],
) -> Result<Vec<usize>> {
    let mut selected = Vec::new();
    let mut pos = 0usize;
    for row in 0..row_count {
        if bitmap.is_null(row) {
            continue;
        }
        if pos + 4 > data.len() {
            return Err(MonaxError::Storage(
                "variable column length prefix truncated".into(),
            ));
        }
        let len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        let start = pos + 4;
        let end = start + len;
        if end > data.len() {
            return Err(MonaxError::Storage(
                "variable column selected value truncated".into(),
            ));
        }
        if &data[start..end] == target {
            selected.push(row);
        }
        pos = end;
    }
    Ok(selected)
}

fn selected_eq_rows_fixed(
    data: &[u8],
    bitmap: &NullBitmap,
    row_count: usize,
    data_type: DataType,
    value: &ScalarValue,
) -> Result<Vec<usize>> {
    let Some(width) = data_type.fixed_size() else {
        return Ok(Vec::new());
    };
    let mut selected = Vec::new();
    let mut offset = 0usize;
    for row in 0..row_count {
        if bitmap.is_null(row) {
            continue;
        }
        let end = offset + width;
        if end > data.len() {
            return Err(MonaxError::Storage(
                "fixed column selected scan exceeds data".into(),
            ));
        }
        if fixed_value_matches(&data[offset..end], data_type, value) {
            selected.push(row);
        }
        offset = end;
    }
    Ok(selected)
}

fn fixed_value_matches(bytes: &[u8], data_type: DataType, value: &ScalarValue) -> bool {
    match (data_type, value) {
        (DataType::Bool, ScalarValue::Bool(Some(v))) => (bytes[0] != 0) == *v,
        (DataType::Int8, ScalarValue::Int8(Some(v))) => bytes[0] as i8 == *v,
        (DataType::UInt8, ScalarValue::UInt8(Some(v))) => bytes[0] == *v,
        (DataType::Int16, ScalarValue::Int16(Some(v))) => {
            i16::from_le_bytes(bytes[..2].try_into().unwrap()) == *v
        }
        (DataType::UInt16, ScalarValue::UInt16(Some(v))) => {
            u16::from_le_bytes(bytes[..2].try_into().unwrap()) == *v
        }
        (DataType::Int32, ScalarValue::Int32(Some(v))) => {
            i32::from_le_bytes(bytes[..4].try_into().unwrap()) == *v
        }
        (DataType::UInt32, ScalarValue::UInt32(Some(v))) => {
            u32::from_le_bytes(bytes[..4].try_into().unwrap()) == *v
        }
        (DataType::Int64, ScalarValue::Int64(Some(v)))
        | (DataType::Time, ScalarValue::Time(Some(v)))
        | (DataType::Timestamp, ScalarValue::Timestamp(Some(v)))
        | (DataType::Interval, ScalarValue::Interval(Some(v)))
        | (DataType::Decimal, ScalarValue::Decimal(Some(v))) => {
            i64::from_le_bytes(bytes[..8].try_into().unwrap()) == *v
        }
        (DataType::UInt64, ScalarValue::UInt64(Some(v))) => {
            u64::from_le_bytes(bytes[..8].try_into().unwrap()) == *v
        }
        (DataType::Date, ScalarValue::Date(Some(v))) => {
            i32::from_le_bytes(bytes[..4].try_into().unwrap()) == *v
        }
        (DataType::Float32, ScalarValue::Float32(Some(v))) => {
            f32::from_le_bytes(bytes[..4].try_into().unwrap()) == *v
        }
        (DataType::Float64, ScalarValue::Float64(Some(v))) => {
            f64::from_le_bytes(bytes[..8].try_into().unwrap()) == *v
        }
        _ => false,
    }
}

fn scalar_values_equal(left: &ScalarValue, right: &ScalarValue) -> bool {
    match (left, right) {
        (ScalarValue::Bool(Some(l)), ScalarValue::Bool(Some(r))) => l == r,
        (ScalarValue::Int8(Some(l)), ScalarValue::Int8(Some(r))) => l == r,
        (ScalarValue::Int16(Some(l)), ScalarValue::Int16(Some(r))) => l == r,
        (ScalarValue::Int32(Some(l)), ScalarValue::Int32(Some(r))) => l == r,
        (ScalarValue::Int64(Some(l)), ScalarValue::Int64(Some(r))) => l == r,
        (ScalarValue::UInt8(Some(l)), ScalarValue::UInt8(Some(r))) => l == r,
        (ScalarValue::UInt16(Some(l)), ScalarValue::UInt16(Some(r))) => l == r,
        (ScalarValue::UInt32(Some(l)), ScalarValue::UInt32(Some(r))) => l == r,
        (ScalarValue::UInt64(Some(l)), ScalarValue::UInt64(Some(r))) => l == r,
        (ScalarValue::Float32(Some(l)), ScalarValue::Float32(Some(r))) => l == r,
        (ScalarValue::Float64(Some(l)), ScalarValue::Float64(Some(r))) => l == r,
        (ScalarValue::Decimal(Some(l)), ScalarValue::Decimal(Some(r))) => l == r,
        (ScalarValue::Date(Some(l)), ScalarValue::Date(Some(r))) => l == r,
        (ScalarValue::Time(Some(l)), ScalarValue::Time(Some(r))) => l == r,
        (ScalarValue::Timestamp(Some(l)), ScalarValue::Timestamp(Some(r))) => l == r,
        (ScalarValue::Interval(Some(l)), ScalarValue::Interval(Some(r))) => l == r,
        (ScalarValue::VarChar(Some(l)), ScalarValue::VarChar(Some(r))) => l == r,
        _ => false,
    }
}

fn count_eq_fixed_le8(data: &[u8], bitmap: &NullBitmap, row_count: usize, target: i64) -> u64 {
    let mut count: u64 = 0;
    let mut offset = 0usize;
    for i in 0..row_count {
        if bitmap.is_null(i) {
            continue;
        }
        if offset + 8 > data.len() {
            break;
        }
        let val = i64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
        if val == target {
            count += 1;
        }
        offset += 8;
    }
    count
}

fn count_eq_fixed_le4(data: &[u8], bitmap: &NullBitmap, row_count: usize, target: i32) -> u64 {
    let mut count: u64 = 0;
    let mut offset = 0usize;
    for i in 0..row_count {
        if bitmap.is_null(i) {
            continue;
        }
        if offset + 4 > data.len() {
            break;
        }
        let val = i32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
        if val == target {
            count += 1;
        }
        offset += 4;
    }
    count
}

fn count_eq_fixed_le2(data: &[u8], bitmap: &NullBitmap, row_count: usize, target: i16) -> u64 {
    let mut count: u64 = 0;
    let mut offset = 0usize;
    for i in 0..row_count {
        if bitmap.is_null(i) {
            continue;
        }
        if offset + 2 > data.len() {
            break;
        }
        let val = i16::from_le_bytes(data[offset..offset + 2].try_into().unwrap());
        if val == target {
            count += 1;
        }
        offset += 2;
    }
    count
}

fn count_eq_fixed_le1(data: &[u8], bitmap: &NullBitmap, row_count: usize, target: i8) -> u64 {
    let mut count: u64 = 0;
    let mut offset = 0usize;
    for i in 0..row_count {
        if bitmap.is_null(i) {
            continue;
        }
        if offset + 1 > data.len() {
            break;
        }
        if data[offset] as i8 == target {
            count += 1;
        }
        offset += 1;
    }
    count
}

fn count_eq_fixed_le8f(data: &[u8], bitmap: &NullBitmap, row_count: usize, target: f64) -> u64 {
    let mut count: u64 = 0;
    let mut offset = 0usize;
    for i in 0..row_count {
        if bitmap.is_null(i) {
            continue;
        }
        if offset + 8 > data.len() {
            break;
        }
        let val = f64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
        if val == target {
            count += 1;
        }
        offset += 8;
    }
    count
}

fn count_eq_fixed_le4f(data: &[u8], bitmap: &NullBitmap, row_count: usize, target: f32) -> u64 {
    let mut count: u64 = 0;
    let mut offset = 0usize;
    for i in 0..row_count {
        if bitmap.is_null(i) {
            continue;
        }
        if offset + 4 > data.len() {
            break;
        }
        let val = f32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
        if val == target {
            count += 1;
        }
        offset += 4;
    }
    count
}

fn count_eq_varchar(data: &[u8], bitmap: &NullBitmap, row_count: usize, target: &str) -> u64 {
    let target_bytes = target.as_bytes();
    let mut count: u64 = 0;
    let mut pos = 0usize;
    for i in 0..row_count {
        if bitmap.is_null(i) {
            continue;
        }
        if pos + 4 > data.len() {
            break;
        }
        let len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        if pos + len > data.len() {
            break;
        }
        if &data[pos..pos + len] == target_bytes {
            count += 1;
        }
        pos += len;
    }
    count
}

fn count_eq_adaptive(
    data: &[u8],
    row_count: usize,
    col_def: &ColumnDef,
    bitmap: &NullBitmap,
    value: &ScalarValue,
) -> Result<u64> {
    if data.len() < COLUMN_ENCODING_MAGIC.len() + 1 {
        return Err(MonaxError::Storage(
            "adaptive column header truncated".into(),
        ));
    }
    let encoding =
        crate::storage::codec::encoding::EncodingType::from_u8(data[COLUMN_ENCODING_MAGIC.len()])?;
    let payload = &data[COLUMN_ENCODING_MAGIC.len() + 1..];

    match encoding {
        crate::storage::codec::encoding::EncodingType::Rle => {
            count_eq_rle_raw(payload, row_count, col_def, bitmap, value)
        }
        crate::storage::codec::encoding::EncodingType::Delta => {
            let values = crate::storage::codec::column_codec::decode_i64_delta_payload(payload)?;
            count_eq_numeric_from_i64(&values, row_count, col_def, bitmap, value)
        }
        crate::storage::codec::encoding::EncodingType::Dictionary
            if col_def.data_type == DataType::VarChar =>
        {
            count_eq_varchar_dictionary(payload, row_count, bitmap, value)
        }
        other => Err(MonaxError::Storage(format!(
            "unsupported adaptive column encoding {:?} for count_eq",
            other
        ))),
    }
}

fn count_eq_rle_raw(
    payload: &[u8],
    row_count: usize,
    col_def: &ColumnDef,
    bitmap: &NullBitmap,
    value: &ScalarValue,
) -> Result<u64> {
    let target = match extract_i64_from_scalar(value) {
        Some(t) => t,
        None => return Ok(0),
    };

    if !is_numeric_type(&col_def.data_type) {
        return Ok(0);
    }

    let mut cursor = Cursor::new(payload);
    let run_count = read_u32_le(&mut cursor)? as usize;
    let mut runs = Vec::with_capacity(run_count);
    for _ in 0..run_count {
        let mut value_buf = [0u8; 8];
        cursor.read_exact(&mut value_buf)?;
        let val = i64::from_le_bytes(value_buf);
        let len = read_u32_le(&mut cursor)? as usize;
        runs.push((val, len));
    }

    let mut count: u64 = 0;
    let mut row = 0usize;
    for (val, len) in &runs {
        for _ in 0..*len {
            if row >= row_count {
                break;
            }
            if !bitmap.is_null(row) && *val == target {
                count += 1;
            }
            row += 1;
        }
    }

    Ok(count)
}

fn count_eq_numeric_from_i64(
    values: &[i64],
    row_count: usize,
    col_def: &ColumnDef,
    bitmap: &NullBitmap,
    value: &ScalarValue,
) -> Result<u64> {
    let target = match extract_i64_from_scalar(value) {
        Some(t) => t,
        None => return Ok(0),
    };

    if !is_numeric_type(&col_def.data_type) {
        return Ok(0);
    }

    let mut count: u64 = 0;
    let mut val_pos = 0usize;

    for i in 0..row_count {
        if bitmap.is_null(i) {
            continue;
        }
        if val_pos < values.len() && values[val_pos] == target {
            count += 1;
        }
        val_pos += 1;
    }

    Ok(count)
}

fn count_eq_varchar_dictionary(
    payload: &[u8],
    row_count: usize,
    bitmap: &NullBitmap,
    value: &ScalarValue,
) -> Result<u64> {
    let target_str = match value {
        ScalarValue::VarChar(Some(s)) => s.as_str(),
        _ => return Ok(0),
    };

    let mut cursor = Cursor::new(payload);
    let dict_count = read_u32_le(&mut cursor)? as usize;
    let mut dict = Vec::with_capacity(dict_count);
    for _ in 0..dict_count {
        let len = read_u32_le(&mut cursor)? as usize;
        let mut bytes = vec![0u8; len];
        cursor.read_exact(&mut bytes)?;
        dict.push(String::from_utf8_lossy(&bytes).into_owned());
    }

    let mut match_dict_idx: Option<u32> = None;
    for (idx, entry) in dict.iter().enumerate() {
        if entry == target_str {
            match_dict_idx = Some(idx as u32);
            break;
        }
    }

    let index_count = read_u32_le(&mut cursor)? as usize;
    let mut count: u64 = 0;
    let mut val_pos = 0usize;

    for i in 0..row_count {
        if bitmap.is_null(i) {
            continue;
        }
        if val_pos < index_count {
            let idx = read_u32_le(&mut cursor).unwrap_or(u32::MAX);
            if let Some(target_idx) = match_dict_idx {
                if idx == target_idx {
                    count += 1;
                }
            }
        }
        val_pos += 1;
    }

    Ok(count)
}

fn extract_i64_from_scalar(value: &ScalarValue) -> Option<i64> {
    match value {
        ScalarValue::Int64(Some(v)) => Some(*v),
        ScalarValue::Int32(Some(v)) => Some(*v as i64),
        ScalarValue::Int16(Some(v)) => Some(*v as i64),
        ScalarValue::Int8(Some(v)) => Some(*v as i64),
        ScalarValue::UInt64(Some(v)) => Some(*v as i64),
        ScalarValue::UInt32(Some(v)) => Some(*v as i64),
        ScalarValue::UInt16(Some(v)) => Some(*v as i64),
        ScalarValue::UInt8(Some(v)) => Some(*v as i64),
        ScalarValue::Decimal(Some(v)) => Some(*v),
        ScalarValue::Date(Some(v)) => Some(*v as i64),
        ScalarValue::Timestamp(Some(v)) => Some(*v),
        _ => None,
    }
}

fn is_numeric_type(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Decimal
            | DataType::Date
            | DataType::Timestamp
            | DataType::Time
            | DataType::Interval
    )
}
