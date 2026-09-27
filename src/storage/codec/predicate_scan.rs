use std::io::{Cursor, Read};

use crate::common::error::{Result, TaibaiError};
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
        return Err(TaibaiError::Storage(
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
            let decoded = decode_adaptive_column(data, row_count, col_def, bitmap.clone(), false)?;
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
            return Err(TaibaiError::Storage("dictionary value truncated".into()));
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
            return Err(TaibaiError::Storage(
                "variable column length prefix truncated".into(),
            ));
        }
        let len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        let start = pos + 4;
        let end = start + len;
        if end > data.len() {
            return Err(TaibaiError::Storage(
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
            return Err(TaibaiError::Storage(
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
        return Err(TaibaiError::Storage(
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
        crate::storage::codec::encoding::EncodingType::DeltaOfDelta => {
            let values = crate::storage::codec::column_codec::decode_i64_dod_payload(payload)?;
            count_eq_numeric_from_i64(&values, row_count, col_def, bitmap, value)
        }
        crate::storage::codec::encoding::EncodingType::GorillaXor => {
            count_eq_gorilla(payload, row_count, col_def, bitmap, value)
        }
        other => Err(TaibaiError::Storage(format!(
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

fn count_eq_gorilla(
    payload: &[u8],
    row_count: usize,
    col_def: &ColumnDef,
    bitmap: &NullBitmap,
    value: &ScalarValue,
) -> Result<u64> {
    let width = match col_def.data_type {
        DataType::Float32 => 32,
        DataType::Float64 | DataType::Decimal => 64,
        _ => return Ok(0),
    };
    let target = match (&col_def.data_type, value) {
        (DataType::Float32, ScalarValue::Float32(Some(v))) => Some(v.to_bits() as u64),
        (DataType::Float64, ScalarValue::Float64(Some(v))) => Some(v.to_bits()),
        (DataType::Decimal, ScalarValue::Decimal(Some(v))) => Some(*v as u64),
        _ => None,
    };
    let Some(target) = target else {
        return Ok(0);
    };
    let bits = crate::storage::codec::column_codec::decode_gorilla_xor_payload(payload, width)?;
    let mut count: u64 = 0;
    let mut val_pos = 0usize;
    for row in 0..row_count {
        if bitmap.is_null(row) {
            continue;
        }
        if val_pos < bits.len() && bits[val_pos] == target {
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

// ---------------------------------------------------------------------------
// Run-level predicate folding for Rle/DeltaRle columns.
//
// Every run of an Rle/DeltaRle payload carries a single value, so a run
// either fully satisfies an eq/range predicate or fully fails it, and a
// no-GROUP-BY aggregate over the surviving rows only needs how many rows
// each matching run covers and its value — never where those rows sit.
// NULL rows never satisfy a predicate on the column, and runs only encode
// non-null values, so the null bitmap is not consulted at all.
// ---------------------------------------------------------------------------

use crate::storage::codec::io_util::{read_varint, zigzag_decode};
use crate::storage::partition::pruning::RangeOp;

/// One predicate evaluated against a run value, expressed in the column's
/// raw numeric domain — the same domain `numeric_cmp`/`compare_scalar_at`
/// compare in (no decimal rescaling anywhere in the pipeline).
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RunPredicate {
    Int { op: RangeOp, value: i128 },
    Float { op: RangeOp, value: f64 },
    /// The predicate value cannot order against a raw i64 run value (NULL or
    /// a non-numeric scalar): it matches no row, mirroring the row kernels'
    /// `vec![false; n]` fallback.
    Never,
}

impl RunPredicate {
    pub fn new(op: RangeOp, value: &ScalarValue) -> Self {
        if let Some(i) = value.integer_i128() {
            return Self::Int { op, value: i };
        }
        if let Some(f) = value.numeric_f64() {
            return Self::Float { op, value: f };
        }
        Self::Never
    }

    fn matches(&self, run_value: i64) -> bool {
        match self {
            Self::Int { op, value } => range_op_holds(*op, (run_value as i128).cmp(value)),
            // A NaN threshold orders against nothing, matching the per-row
            // kernels (`partial_cmp` -> None -> predicate false).
            Self::Float { op, value } => (run_value as f64)
                .partial_cmp(value)
                .is_some_and(|ord| range_op_holds(*op, ord)),
            Self::Never => false,
        }
    }
}

fn range_op_holds(op: RangeOp, ord: std::cmp::Ordering) -> bool {
    match op {
        RangeOp::Eq => ord == std::cmp::Ordering::Equal,
        RangeOp::Lt => ord == std::cmp::Ordering::Less,
        RangeOp::Le => ord != std::cmp::Ordering::Greater,
        RangeOp::Gt => ord == std::cmp::Ordering::Greater,
        RangeOp::Ge => ord != std::cmp::Ordering::Less,
    }
}

/// Aggregate state folded from matching runs.
#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub struct RunFold {
    /// Rows covered by matching runs. All of them are non-null (runs only
    /// encode non-null values) and satisfy every predicate.
    pub matched_rows: u64,
    /// Σ value×len over matching runs. Wrapping arithmetic, identical to
    /// `col_sum_i64`'s per-element wrapping fold over the same rows.
    pub sum_i: i64,
    pub any: bool,
    pub min: i64,
    pub max: i64,
}

impl RunFold {
    fn add_run(&mut self, value: i64, len: u64) {
        if !self.any {
            self.min = value;
            self.max = value;
            self.any = true;
        } else {
            if value < self.min {
                self.min = value;
            }
            if value > self.max {
                self.max = value;
            }
        }
        self.matched_rows += len;
        self.sum_i = self.sum_i.wrapping_add(value.wrapping_mul(len as i64));
    }
}

/// Walk the runs of an Rle/DeltaRle column straight from its raw (decompressed)
/// block and fold count/sum/min/max over the runs whose value satisfies every
/// predicate. `data` is the full column block: `[bitmap_len][bitmap][values]`
/// where `values` starts with the adaptive encoding magic.
///
/// Returns `None` when the block is not Rle/DeltaRle-adaptive (the caller
/// falls back to the regular decode+mask path); malformed payloads surface as
/// `Some(Err(..))` exactly like the decoder would report them.
pub fn fold_runs_from_raw(data: &[u8], predicates: &[RunPredicate]) -> Option<Result<RunFold>> {
    if data.len() < 4 {
        return None;
    }
    let bitmap_len = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
    if data.len() < 4 + bitmap_len {
        return None;
    }
    let values = &data[4 + bitmap_len..];
    if values.len() < COLUMN_ENCODING_MAGIC.len() + 1 || !values.starts_with(COLUMN_ENCODING_MAGIC)
    {
        return None;
    }
    let tag = values[COLUMN_ENCODING_MAGIC.len()];
    let encoding = match crate::storage::codec::encoding::EncodingType::from_u8(tag) {
        Ok(encoding) => encoding,
        Err(_) => return None,
    };
    let payload = &values[COLUMN_ENCODING_MAGIC.len() + 1..];
    match encoding {
        crate::storage::codec::encoding::EncodingType::Rle => {
            Some(fold_plain_rle_runs(payload, predicates))
        }
        crate::storage::codec::encoding::EncodingType::DeltaRle => {
            Some(fold_delta_rle_runs(payload, predicates))
        }
        _ => None,
    }
}

fn fold_plain_rle_runs(payload: &[u8], predicates: &[RunPredicate]) -> Result<RunFold> {
    let mut cursor = Cursor::new(payload);
    let run_count = read_u32_le(&mut cursor)? as usize;
    let mut fold = RunFold::default();
    for _ in 0..run_count {
        let mut value_buf = [0u8; 8];
        cursor.read_exact(&mut value_buf)?;
        let value = i64::from_le_bytes(value_buf);
        let len = read_u32_le(&mut cursor)? as u64;
        if predicates.iter().all(|p| p.matches(value)) {
            fold.add_run(value, len);
        }
    }
    Ok(fold)
}

fn fold_delta_rle_runs(payload: &[u8], predicates: &[RunPredicate]) -> Result<RunFold> {
    let mut cursor = Cursor::new(payload);
    let run_count = read_u32_le(&mut cursor)? as usize;
    let mut fold = RunFold::default();
    let mut prev = 0i64;
    for _ in 0..run_count {
        let delta = zigzag_decode(read_varint(&mut cursor)?);
        prev = prev.wrapping_add(delta);
        let len = read_varint(&mut cursor)? as u64;
        if predicates.iter().all(|p| p.matches(prev)) {
            fold.add_run(prev, len);
        }
    }
    Ok(fold)
}

#[cfg(test)]
mod run_fold_tests {
    use super::*;
    use crate::storage::codec::column_codec::{
        decode_i64_delta_rle_payload, decode_i64_rle_payload, encode_i64_delta_rle_payload,
    };

    /// Reference fold: expand the payload, filter per value, aggregate.
    fn reference_fold(values: &[i64], predicates: &[RunPredicate]) -> RunFold {
        let mut out = RunFold::default();
        for &value in values {
            if predicates.iter().all(|p| p.matches(value)) {
                if !out.any {
                    out.min = value;
                    out.max = value;
                    out.any = true;
                } else {
                    out.min = out.min.min(value);
                    out.max = out.max.max(value);
                }
                out.matched_rows += 1;
                out.sum_i = out.sum_i.wrapping_add(value);
            }
        }
        out
    }

    fn raw_column(payload: &[u8], tag: u8, null_rows: usize, non_null: usize) -> Vec<u8> {
        // bitmap_len = 0 + [magic][tag][payload] — a null-free column (the
        // fold never consults the bitmap; runs cover only non-null values).
        let bitmap = crate::types::bitmap::NullBitmap::new_valid(non_null + null_rows);
        let bitmap_bytes = bitmap.to_bytes();
        let mut raw = Vec::with_capacity(4 + bitmap_bytes.len() + 6 + payload.len());
        raw.extend_from_slice(&(bitmap_bytes.len() as u32).to_le_bytes());
        raw.extend_from_slice(&bitmap_bytes);
        raw.extend_from_slice(COLUMN_ENCODING_MAGIC);
        raw.push(tag);
        raw.extend_from_slice(payload);
        raw
    }

    fn sample_values() -> Vec<i64> {
        // Long runs, negatives, zeros, repeated extremes.
        let mut values = Vec::new();
        for &(value, len) in &[
            (10i64, 40usize),
            (-5, 30),
            (0, 25),
            (100, 20),
            (7, 35),
            (-100, 10),
            (10, 20),
        ] {
            values.extend(std::iter::repeat_n(value, len));
        }
        values
    }

    fn int_pred(op: RangeOp, value: i128) -> RunPredicate {
        RunPredicate::Int {
            op,
            value,
        }
    }

    #[test]
    fn delta_rle_run_fold_matches_reference_across_predicates() {
        let values = sample_values();
        let payload = encode_i64_delta_rle_payload(&values);
        assert_eq!(
            decode_i64_delta_rle_payload(&payload).unwrap(),
            values,
            "fixture must round-trip"
        );
        let raw = raw_column(
            &payload,
            crate::storage::codec::encoding::EncodingType::DeltaRle.to_u8(),
            0,
            values.len(),
        );

        let cases: Vec<Vec<RunPredicate>> = vec![
            vec![],
            vec![int_pred(RangeOp::Gt, 7)],
            vec![int_pred(RangeOp::Ge, 0)],
            vec![int_pred(RangeOp::Lt, 50)],
            vec![int_pred(RangeOp::Le, 10)],
            vec![int_pred(RangeOp::Eq, 10)],
            vec![int_pred(RangeOp::Gt, -100), int_pred(RangeOp::Lt, 100)],
            vec![int_pred(RangeOp::Gt, 1000)],
        ];
        for predicates in cases {
            let folded = fold_runs_from_raw(&raw, &predicates)
                .expect("adaptive column")
                .expect("fold ok");
            let reference = reference_fold(&values, &predicates);
            assert_eq!(
                folded, reference,
                "mismatch for predicates {:?}",
                predicates
            );
        }
    }

    #[test]
    fn plain_rle_run_fold_matches_reference() {
        let values = sample_values();
        // Re-encode as plain Rle (value + u32 len per run).
        let mut runs: Vec<(i64, u32)> = Vec::new();
        for &value in &values {
            match runs.last_mut() {
                Some((last, len)) if *last == value => *len += 1,
                _ => runs.push((value, 1)),
            }
        }
        let mut payload = Vec::new();
        payload.extend_from_slice(&(runs.len() as u32).to_le_bytes());
        for (value, len) in runs {
            payload.extend_from_slice(&value.to_le_bytes());
            payload.extend_from_slice(&len.to_le_bytes());
        }
        assert_eq!(decode_i64_rle_payload(&payload).unwrap(), values);
        let raw = raw_column(
            &payload,
            crate::storage::codec::encoding::EncodingType::Rle.to_u8(),
            0,
            values.len(),
        );

        let predicates = vec![int_pred(RangeOp::Ge, 7)];
        let folded = fold_runs_from_raw(&raw, &predicates)
            .expect("adaptive column")
            .expect("fold ok");
        assert_eq!(folded, reference_fold(&values, &predicates));
    }

    #[test]
    fn non_run_encodings_decline_and_never_predicates_fold_nothing() {
        // BitPacked tag: fold must decline (None).
        let raw = raw_column(
            &[],
            crate::storage::codec::encoding::EncodingType::BitPacked.to_u8(),
            0,
            0,
        );
        assert!(fold_runs_from_raw(&raw, &[]).is_none());
        // No magic at all (plain column): decline.
        assert!(fold_runs_from_raw(&[0, 0, 0, 0, 1, 2, 3], &[]).is_none());

        // A NULL / varchar threshold never matches, mirroring the row
        // kernels' all-false fallback.
        let values = vec![1i64, 2, 3];
        let payload = encode_i64_delta_rle_payload(&values);
        let raw = raw_column(
            &payload,
            crate::storage::codec::encoding::EncodingType::DeltaRle.to_u8(),
            0,
            values.len(),
        );
        let never = RunPredicate::new(RangeOp::Gt, &ScalarValue::VarChar(Some("x".into())));
        assert_eq!(never, RunPredicate::Never);
        let folded = fold_runs_from_raw(&raw, &[never])
            .expect("adaptive column")
            .expect("fold ok");
        assert_eq!(folded.matched_rows, 0);
        assert!(!folded.any);
    }

    #[test]
    fn run_predicate_float_threshold_uses_f64_domain() {
        // Float thresholds compare in the raw f64 domain (numeric_cmp
        // semantics), so 10.5 excludes the run of 10 and includes 100.
        let values = sample_values();
        let payload = encode_i64_delta_rle_payload(&values);
        let raw = raw_column(
            &payload,
            crate::storage::codec::encoding::EncodingType::DeltaRle.to_u8(),
            0,
            values.len(),
        );
        let pred = RunPredicate::Float {
            op: RangeOp::Gt,
            value: 10.5,
        };
        let folded = fold_runs_from_raw(&raw, &[pred])
            .expect("adaptive column")
            .expect("fold ok");
        let reference = reference_fold(&values, &[pred]);
        assert_eq!(folded, reference);
    }
}

/// Diagnostic: `(runs, rows_covered)` of an Rle/DeltaRle column block, for
/// run-length viability probes. `None` for any other encoding.
pub fn debug_run_stats_from_raw(data: &[u8]) -> Option<(u64, u64)> {
    if data.len() < 4 {
        return None;
    }
    let bitmap_len = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
    if data.len() < 4 + bitmap_len {
        return None;
    }
    let values = &data[4 + bitmap_len..];
    if values.len() < COLUMN_ENCODING_MAGIC.len() + 1 || !values.starts_with(COLUMN_ENCODING_MAGIC)
    {
        return None;
    }
    let tag = values[COLUMN_ENCODING_MAGIC.len()];
    let encoding = match crate::storage::codec::encoding::EncodingType::from_u8(tag) {
        Ok(encoding) => encoding,
        Err(_) => return None,
    };
    let payload = &values[COLUMN_ENCODING_MAGIC.len() + 1..];
    let mut cursor = Cursor::new(payload);
    let run_count = read_u32_le(&mut cursor).ok()? as u64;
    let mut rows = 0u64;
    match encoding {
        crate::storage::codec::encoding::EncodingType::Rle => {
            for _ in 0..run_count {
                let mut value_buf = [0u8; 8];
                cursor.read_exact(&mut value_buf).ok()?;
                let len = read_u32_le(&mut cursor).ok()? as u64;
                rows += len;
            }
        }
        crate::storage::codec::encoding::EncodingType::DeltaRle => {
            for _ in 0..run_count {
                let _ = read_varint(&mut cursor).ok()?;
                let len = read_varint(&mut cursor).ok()? as u64;
                rows += len;
            }
        }
        _ => return None,
    }
    Some((run_count, rows))
}
