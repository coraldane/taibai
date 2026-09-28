use std::collections::HashSet;
use std::path::Path;
use std::sync::atomic::Ordering;

use crate::common::error::{Result, TaibaiError};
use crate::protocol::types::DataType;
use crate::storage::bulk_payload::{null_bit_is_set, DirectBulkColumn, DirectBulkRequest};
use crate::storage::partition::pruning::ZoneMap;
use crate::storage::segment::store;
use crate::storage::segment::writer::VALUE_SET_MAX_DISTINCT;
use crate::storage::segment_manager::state::TableSegmentState;
use crate::types::array::ColumnArray;
use crate::types::batch::{RecordBatch, Schema};
use crate::types::bitmap::NullBitmap;
use crate::types::scalar::ScalarValue;

pub fn table_key(db: &str, table: &str) -> String {
    format!("{}:{}", db, table)
}

pub(super) fn is_transient_footer_error(err: &TaibaiError) -> bool {
    matches!(err, TaibaiError::Storage(msg) if msg.contains("invalid footer offset"))
}

pub(super) fn ensure_state_not_retired(
    state: &TableSegmentState,
    db: &str,
    table: &str,
) -> Result<()> {
    if state.retired.load(Ordering::Acquire) {
        return Err(TaibaiError::Storage(format!(
            "table '{}.{}' was removed while a write was waiting",
            db, table
        )));
    }
    Ok(())
}

pub(super) fn discover_max_seq(table_dir: &Path) -> u32 {
    let mut max_seq = 0u32;
    if let Ok(entries) = std::fs::read_dir(table_dir) {
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            if let Some(seq) = store::parse_segment_seq(&name) {
                max_seq = max_seq.max(seq);
            }
        }
    }
    max_seq
}

pub(super) fn discover_max_partition_seq(
    table_dir: &Path,
) -> std::collections::HashMap<String, u32> {
    let mut max_seq: std::collections::HashMap<String, u32> = std::collections::HashMap::new();
    if let Ok(entries) = std::fs::read_dir(table_dir) {
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            let Some(partition_key) =
                crate::storage::partition::partition::parse_partition_key(&name)
            else {
                continue;
            };
            if let Some(seq) = store::parse_partition_seq(&name, &partition_key) {
                max_seq
                    .entry(partition_key)
                    .and_modify(|current| *current = (*current).max(seq))
                    .or_insert(seq);
            }
        }
    }
    max_seq
}

pub(super) fn compute_zone_map_from_column(
    column: &ColumnArray,
    start_row: usize,
    row_count: usize,
) -> ZoneMap {
    let end_row = start_row + row_count;
    let bitmap = column.bitmap();
    let null_count = if bitmap.is_all_valid() {
        0
    } else {
        (start_row..end_row)
            .filter(|&row| bitmap.is_null(row))
            .count() as u64
    };

    macro_rules! ordered_zone_map {
        ($values:expr, $variant:ident) => {{
            let mut min = None;
            let mut max = None;
            for (row, &value) in $values.iter().enumerate().take(end_row).skip(start_row) {
                if bitmap.is_null(row) {
                    continue;
                }
                if min.is_none_or(|current| value < current) {
                    min = Some(value);
                }
                if max.is_none_or(|current| value > current) {
                    max = Some(value);
                }
            }
            ZoneMap {
                min_value: ScalarValue::$variant(min),
                max_value: ScalarValue::$variant(max),
                null_count,
                row_count: row_count as u64,
            }
        }};
    }

    macro_rules! float_zone_map {
        ($values:expr, $variant:ident) => {{
            let mut min = None;
            let mut max = None;
            for (row, &value) in $values.iter().enumerate().take(end_row).skip(start_row) {
                if bitmap.is_null(row) {
                    continue;
                }
                if min.is_none_or(|current| value.partial_cmp(&current).is_some_and(|o| o.is_lt()))
                {
                    min = Some(value);
                }
                if max.is_none_or(|current| value.partial_cmp(&current).is_some_and(|o| o.is_gt()))
                {
                    max = Some(value);
                }
            }
            ZoneMap {
                min_value: ScalarValue::$variant(min),
                max_value: ScalarValue::$variant(max),
                null_count,
                row_count: row_count as u64,
            }
        }};
    }

    match column {
        ColumnArray::Bool { data, .. } => {
            let mut min = None;
            let mut max = None;
            for (row, &raw) in data.iter().enumerate().take(end_row).skip(start_row) {
                if bitmap.is_null(row) {
                    continue;
                }
                let value = raw != 0;
                if min.is_none_or(|current: bool| !value & current) {
                    min = Some(value);
                }
                if max.is_none_or(|current: bool| value & !current) {
                    max = Some(value);
                }
            }
            ZoneMap {
                min_value: ScalarValue::Bool(min),
                max_value: ScalarValue::Bool(max),
                null_count,
                row_count: row_count as u64,
            }
        }
        ColumnArray::Int8 { data, .. } => ordered_zone_map!(data, Int8),
        ColumnArray::Int16 { data, .. } => ordered_zone_map!(data, Int16),
        ColumnArray::Int32 { data, .. } => ordered_zone_map!(data, Int32),
        ColumnArray::Int64 { data, .. } => ordered_zone_map!(data, Int64),
        ColumnArray::UInt8 { data, .. } => ordered_zone_map!(data, UInt8),
        ColumnArray::UInt16 { data, .. } => ordered_zone_map!(data, UInt16),
        ColumnArray::UInt32 { data, .. } => ordered_zone_map!(data, UInt32),
        ColumnArray::UInt64 { data, .. } => ordered_zone_map!(data, UInt64),
        ColumnArray::Float32 { data, .. } => float_zone_map!(data, Float32),
        ColumnArray::Float64 { data, .. } => float_zone_map!(data, Float64),
        ColumnArray::Decimal { data, .. } => ordered_zone_map!(data, Decimal),
        ColumnArray::Date { data, .. } => ordered_zone_map!(data, Date),
        ColumnArray::Time { data, .. } => ordered_zone_map!(data, Time),
        ColumnArray::Timestamp { data, .. } => ordered_zone_map!(data, Timestamp),
        ColumnArray::Interval { data, .. } => ordered_zone_map!(data, Interval),
        ColumnArray::VarChar { data, .. } => {
            let mut min_row = None;
            let mut max_row = None;
            for row in start_row..end_row {
                if bitmap.is_null(row) {
                    continue;
                }
                if min_row.is_none_or(|current| data.value(row) < data.value(current)) {
                    min_row = Some(row);
                }
                if max_row.is_none_or(|current| data.value(row) > data.value(current)) {
                    max_row = Some(row);
                }
            }
            ZoneMap {
                min_value: ScalarValue::VarChar(min_row.map(|row| data.value(row).to_string())),
                max_value: ScalarValue::VarChar(max_row.map(|row| data.value(row).to_string())),
                null_count,
                row_count: row_count as u64,
            }
        }
        _ => {
            let mut zone_map = ZoneMap::default();
            for row in start_row..end_row {
                if !bitmap.is_null(row) {
                    zone_map.update(&column.scalar(row));
                }
            }
            zone_map.null_count = null_count;
            zone_map.row_count = row_count as u64;
            zone_map
        }
    }
}

pub(crate) fn direct_bulk_request_to_batch(
    req: &DirectBulkRequest<'_>,
    schema: &Schema,
) -> Result<RecordBatch> {
    let mut columns = Vec::with_capacity(req.columns.len());
    for col in &req.columns {
        columns.push(direct_bulk_column_to_array(col, req.row_count)?);
    }
    RecordBatch::try_new(schema.clone(), columns)
}

/// Decode one columnar payload straight into its typed column. The wire
/// format is already column-major, so fixed-width payloads are converted
/// element-wise without `ScalarValue` boxing, VarChar bytes are pushed
/// straight into the UTF-8 buffer (no per-row `String` allocation), and only
/// columns that actually carry NULLs pay for per-row bitmap work.
pub(crate) fn direct_bulk_column_to_array(
    col: &DirectBulkColumn<'_>,
    rows: usize,
) -> Result<ColumnArray> {
    let has_nulls = !col.nulls.is_empty() && (0..rows).any(|r| null_bit_is_set(col.nulls, r));
    if has_nulls {
        // Rare path: per-row decode honouring the NULL bitmap.
        let mut array = ColumnArray::with_capacity(col.data_type, rows);
        for row_idx in 0..rows {
            if null_bit_is_set(col.nulls, row_idx) {
                array.push_null_value();
            } else {
                array.push(&direct_bulk_column_scalar_at(col, row_idx)?)?;
            }
        }
        return Ok(array);
    }

    macro_rules! bulk_fixed {
        ($variant:ident, $ty:ty, $width:expr) => {{
            if col.data.len() == rows * $width {
                let mut vals = Vec::with_capacity(rows);
                for chunk in col.data.chunks_exact($width) {
                    vals.push(<$ty>::from_le_bytes(chunk.try_into().expect("width match")));
                }
                return Ok(ColumnArray::$variant {
                    data: vals,
                    bitmap: NullBitmap::new_valid(rows),
                });
            }
        }};
    }

    match col.data_type {
        DataType::Bool => {
            if col.data.len() == rows {
                return Ok(ColumnArray::Bool {
                    data: col.data.to_vec(),
                    bitmap: NullBitmap::new_valid(rows),
                });
            }
        }
        DataType::Int8 => {
            if col.data.len() == rows {
                return Ok(ColumnArray::Int8 {
                    data: col.data.iter().map(|&b| b as i8).collect(),
                    bitmap: NullBitmap::new_valid(rows),
                });
            }
        }
        DataType::UInt8 => {
            if col.data.len() == rows {
                return Ok(ColumnArray::UInt8 {
                    data: col.data.to_vec(),
                    bitmap: NullBitmap::new_valid(rows),
                });
            }
        }
        DataType::Int16 => bulk_fixed!(Int16, i16, 2),
        DataType::UInt16 => bulk_fixed!(UInt16, u16, 2),
        DataType::Int32 => bulk_fixed!(Int32, i32, 4),
        DataType::UInt32 => bulk_fixed!(UInt32, u32, 4),
        DataType::Float32 => bulk_fixed!(Float32, f32, 4),
        DataType::Date => bulk_fixed!(Date, i32, 4),
        DataType::Int64 => bulk_fixed!(Int64, i64, 8),
        DataType::UInt64 => bulk_fixed!(UInt64, u64, 8),
        DataType::Float64 => bulk_fixed!(Float64, f64, 8),
        DataType::Decimal => bulk_fixed!(Decimal, i64, 8),
        DataType::Time => bulk_fixed!(Time, i64, 8),
        DataType::Timestamp => bulk_fixed!(Timestamp, i64, 8),
        DataType::Interval => bulk_fixed!(Interval, i64, 8),
        DataType::VarChar => {
            if let Some(ranges) = &col.variable_ranges {
                if ranges.len() == rows {
                    // Protocol-supplied offsets: validate before slicing so a
                    // malformed payload reports an error instead of panicking
                    // the decode task (parity with the scalar fallback).
                    let max_end: usize = ranges
                        .iter()
                        .map(|&(offset, len)| offset.saturating_add(len))
                        .max()
                        .unwrap_or(0);
                    if max_end > col.data.len() {
                        return Err(TaibaiError::Protocol(
                            "bulk varchar ranges exceed column payload".into(),
                        ));
                    }
                    let total_bytes: usize = ranges.iter().map(|&(_, len)| len).sum();
                    let mut buf = crate::types::array::Utf8Buffer::with_capacity(rows);
                    buf.reserve_values(rows, total_bytes);
                    for &(offset, len) in ranges {
                        let value =
                            std::str::from_utf8(&col.data[offset..offset + len]).map_err(|_| {
                                TaibaiError::Protocol("invalid utf8 in bulk varchar".into())
                            })?;
                        buf.push_str(value);
                    }
                    return Ok(ColumnArray::VarChar {
                        data: buf,
                        bitmap: NullBitmap::new_valid(rows),
                    });
                }
            }
        }
        DataType::Blob | DataType::Vector => {}
    }

    // Fallback for unexpected payload sizes and variable-width types without
    // prebuilt ranges: per-row decode (still NULL-free).
    let mut array = ColumnArray::with_capacity(col.data_type, rows);
    for row_idx in 0..rows {
        array.push(&direct_bulk_column_scalar_at(col, row_idx)?)?;
    }
    Ok(array)
}

pub(crate) fn direct_bulk_column_scalar_at(
    col: &DirectBulkColumn<'_>,
    row_idx: usize,
) -> Result<ScalarValue> {
    if is_null_at(col.nulls, row_idx) {
        return Ok(ScalarValue::null_for_type(col.data_type));
    }

    if let Some(width) = col.data_type.fixed_size() {
        let offset = row_idx
            .checked_mul(width)
            .ok_or_else(|| TaibaiError::Protocol("bulk column offset overflow".into()))?;
        return read_fixed_scalar(col.data, offset, col.data_type).ok_or_else(|| {
            TaibaiError::Protocol(format!(
                "bulk column '{}' row {} is truncated",
                col.name, row_idx
            ))
        });
    }

    let (offset, len) = direct_bulk_variable_range_at(col, row_idx)?;
    match col.data_type {
        DataType::VarChar => {
            let s = std::str::from_utf8(&col.data[offset..offset + len])
                .map_err(|_| TaibaiError::Protocol("invalid utf8 in bulk varchar".into()))?;
            Ok(ScalarValue::VarChar(Some(s.to_string())))
        }
        DataType::Blob => Ok(ScalarValue::Blob(Some(
            col.data[offset..offset + len].to_vec(),
        ))),
        DataType::Vector => {
            if len % 4 != 0 {
                return Err(TaibaiError::Protocol(
                    "bulk vector payload length must be a multiple of 4".into(),
                ));
            }
            let mut values = Vec::with_capacity(len / 4);
            for chunk in col.data[offset..offset + len].chunks_exact(4) {
                values.push(f32::from_le_bytes(chunk.try_into().unwrap()));
            }
            Ok(ScalarValue::Vector(Some(values)))
        }
        _ => Err(TaibaiError::Protocol(format!(
            "unsupported variable bulk type {}",
            col.data_type.display_name()
        ))),
    }
}

fn direct_bulk_variable_range_at(
    col: &DirectBulkColumn<'_>,
    target_row: usize,
) -> Result<(usize, usize)> {
    if let Some(ranges) = &col.variable_ranges {
        return ranges.get(target_row).copied().ok_or_else(|| {
            TaibaiError::Protocol(format!(
                "bulk column '{}' row {} not found",
                col.name, target_row
            ))
        });
    }

    let mut offset = 0usize;
    for row_idx in 0..=target_row {
        if is_null_at(col.nulls, row_idx) {
            continue;
        }
        if offset + 4 > col.data.len() {
            return Err(TaibaiError::Protocol(format!(
                "bulk column '{}' variable length prefix truncated",
                col.name
            )));
        }
        let len = u32::from_le_bytes(col.data[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;
        if offset + len > col.data.len() {
            return Err(TaibaiError::Protocol(format!(
                "bulk column '{}' variable data truncated",
                col.name
            )));
        }
        if row_idx == target_row {
            return Ok((offset, len));
        }
        offset += len;
    }
    Err(TaibaiError::Protocol(format!(
        "bulk column '{}' row {} not found",
        col.name, target_row
    )))
}

pub(super) fn slice_batch_fast(
    batch: &RecordBatch,
    offset: usize,
    len: usize,
) -> Result<RecordBatch> {
    let end = offset.checked_add(len).ok_or_else(|| {
        crate::common::error::TaibaiError::Internal("batch slice range overflow".into())
    })?;
    if end > batch.num_rows() {
        return Err(crate::common::error::TaibaiError::Internal(format!(
            "batch slice {}..{} exceeds {} rows",
            offset,
            end,
            batch.num_rows()
        )));
    }
    let columns = batch
        .columns
        .iter()
        .map(|col| col.slice_fast(offset, end))
        .collect();
    RecordBatch::try_new(batch.schema.clone(), columns)
}

pub(super) fn compute_zone_maps_from_bulk_columns(
    columns: &[DirectBulkColumn<'_>],
    _schema: &Schema,
    row_count: u64,
) -> Vec<ZoneMap> {
    let mut zone_maps = Vec::with_capacity(columns.len());
    for col in columns.iter() {
        let mut zm = ZoneMap::default();
        let dtype = col.data_type;
        let data = col.data;
        let nulls = col.nulls;
        let elem_count = row_count as usize;

        let mut null_count = 0u64;
        if let Some(width) = dtype.fixed_size() {
            for i in 0..elem_count {
                if is_null_at(nulls, i) {
                    null_count += 1;
                    continue;
                }
                let offset = i * width;
                if let Some(val) = read_fixed_scalar(data, offset, dtype) {
                    zm.update(&val);
                }
            }
        } else {
            for i in 0..elem_count {
                if is_null_at(nulls, i) {
                    null_count += 1;
                    continue;
                }
                let Some((offset, len)) = col
                    .variable_ranges
                    .as_ref()
                    .and_then(|ranges| ranges.get(i).copied())
                else {
                    break;
                };
                if offset + len > data.len() {
                    break;
                }
                if dtype == DataType::VarChar {
                    let s = std::str::from_utf8(&data[offset..offset + len])
                        .unwrap_or("")
                        .to_string();
                    zm.update(&ScalarValue::VarChar(Some(s)));
                }
            }
        }

        zm.null_count = null_count;
        zm.row_count = row_count;
        zone_maps.push(zm);
    }
    zone_maps
}

/// Footer-persisted running sums for one bulk-imported row group: wrapping
/// i64 over the non-null values of each sum-supported column, read straight
/// from the fixed-width LE payload. Unsupported columns carry 0.
pub(super) fn col_sums_from_bulk_columns(
    columns: &[DirectBulkColumn<'_>],
    row_count: u64,
) -> Vec<i64> {
    use DataType::{
        Date, Decimal, Int16, Int32, Int64, Int8, Interval, Time, Timestamp, UInt16, UInt32, UInt8,
    };
    let elem_count = row_count as usize;
    let mut col_sums = Vec::with_capacity(columns.len());
    for col in columns {
        let mut sum = 0i64;
        let dtype = col.data_type;
        if crate::storage::segment::column_sum_supported(dtype) {
            let width = dtype.fixed_size().unwrap_or(0);
            let data = col.data;
            // (type, width, signed) read of one LE scalar as i64.
            let read = |offset: usize| -> Option<i64> {
                if width == 0 || offset + width > data.len() {
                    return None;
                }
                let bytes = &data[offset..offset + width];
                Some(match dtype {
                    Int8 => bytes[0] as i8 as i64,
                    UInt8 => bytes[0] as i64,
                    Int16 => i16::from_le_bytes(bytes.try_into().unwrap()) as i64,
                    UInt16 => u16::from_le_bytes(bytes.try_into().unwrap()) as i64,
                    Int32 | Date => i32::from_le_bytes(bytes.try_into().unwrap()) as i64,
                    UInt32 => u32::from_le_bytes(bytes.try_into().unwrap()) as i64,
                    Int64 | Decimal | Time | Timestamp | Interval => {
                        i64::from_le_bytes(bytes.try_into().unwrap())
                    }
                    _ => 0,
                })
            };
            for row in 0..elem_count {
                if col.is_null(row) {
                    continue;
                }
                if let Some(raw) = read(row * width) {
                    sum = sum.wrapping_add(raw);
                }
            }
        }
        col_sums.push(sum);
    }
    col_sums
}

pub(super) fn compute_value_sets_from_bulk_columns(
    columns: &[DirectBulkColumn<'_>],
    row_count: u64,
) -> Result<Vec<Option<Vec<ScalarValue>>>> {
    let mut result = Vec::with_capacity(columns.len());
    for col in columns {
        let mut values = HashSet::new();
        let mut complete = true;
        for row_idx in 0..row_count as usize {
            let value = direct_bulk_column_scalar_at(col, row_idx)?;
            if value.is_null() {
                continue;
            }
            values.insert(value);
            if values.len() > VALUE_SET_MAX_DISTINCT {
                complete = false;
                break;
            }
        }
        if complete {
            let mut values: Vec<ScalarValue> = values.into_iter().collect();
            values.sort_by(crate::storage::index::sort_key::compare_scalars_for_sort);
            result.push(Some(values));
        } else {
            result.push(None);
        }
    }
    Ok(result)
}

fn is_null_at(nulls: &[u8], idx: usize) -> bool {
    null_bit_is_set(nulls, idx)
}

fn read_fixed_scalar(data: &[u8], offset: usize, dtype: DataType) -> Option<ScalarValue> {
    match dtype {
        DataType::Bool => {
            if offset >= data.len() {
                return None;
            }
            Some(ScalarValue::Bool(Some(data[offset] != 0)))
        }
        DataType::Int8 => {
            if offset >= data.len() {
                return None;
            }
            Some(ScalarValue::Int8(Some(data[offset] as i8)))
        }
        DataType::Int16 => {
            if offset + 2 > data.len() {
                return None;
            }
            Some(ScalarValue::Int16(Some(i16::from_le_bytes(
                data[offset..offset + 2].try_into().ok()?,
            ))))
        }
        DataType::Int32 => {
            if offset + 4 > data.len() {
                return None;
            }
            Some(ScalarValue::Int32(Some(i32::from_le_bytes(
                data[offset..offset + 4].try_into().ok()?,
            ))))
        }
        DataType::Int64 => {
            if offset + 8 > data.len() {
                return None;
            }
            Some(ScalarValue::Int64(Some(i64::from_le_bytes(
                data[offset..offset + 8].try_into().ok()?,
            ))))
        }
        DataType::UInt8 => {
            if offset >= data.len() {
                return None;
            }
            Some(ScalarValue::UInt8(Some(data[offset])))
        }
        DataType::UInt16 => {
            if offset + 2 > data.len() {
                return None;
            }
            Some(ScalarValue::UInt16(Some(u16::from_le_bytes(
                data[offset..offset + 2].try_into().ok()?,
            ))))
        }
        DataType::UInt32 => {
            if offset + 4 > data.len() {
                return None;
            }
            Some(ScalarValue::UInt32(Some(u32::from_le_bytes(
                data[offset..offset + 4].try_into().ok()?,
            ))))
        }
        DataType::UInt64 => {
            if offset + 8 > data.len() {
                return None;
            }
            Some(ScalarValue::UInt64(Some(u64::from_le_bytes(
                data[offset..offset + 8].try_into().ok()?,
            ))))
        }
        DataType::Float32 => {
            if offset + 4 > data.len() {
                return None;
            }
            Some(ScalarValue::Float32(Some(f32::from_le_bytes(
                data[offset..offset + 4].try_into().ok()?,
            ))))
        }
        DataType::Float64 => {
            if offset + 8 > data.len() {
                return None;
            }
            Some(ScalarValue::Float64(Some(f64::from_le_bytes(
                data[offset..offset + 8].try_into().ok()?,
            ))))
        }
        DataType::Decimal => {
            if offset + 8 > data.len() {
                return None;
            }
            Some(ScalarValue::Decimal(Some(i64::from_le_bytes(
                data[offset..offset + 8].try_into().ok()?,
            ))))
        }
        DataType::Date => {
            if offset + 4 > data.len() {
                return None;
            }
            Some(ScalarValue::Date(Some(i32::from_le_bytes(
                data[offset..offset + 4].try_into().ok()?,
            ))))
        }
        DataType::Time => {
            if offset + 8 > data.len() {
                return None;
            }
            Some(ScalarValue::Time(Some(i64::from_le_bytes(
                data[offset..offset + 8].try_into().ok()?,
            ))))
        }
        DataType::Timestamp => {
            if offset + 8 > data.len() {
                return None;
            }
            Some(ScalarValue::Timestamp(Some(i64::from_le_bytes(
                data[offset..offset + 8].try_into().ok()?,
            ))))
        }
        DataType::Interval => {
            if offset + 8 > data.len() {
                return None;
            }
            Some(ScalarValue::Interval(Some(i64::from_le_bytes(
                data[offset..offset + 8].try_into().ok()?,
            ))))
        }
        _ => None,
    }
}

#[cfg(test)]
#[path = "zone_map_test.rs"]
mod zone_map_tests;

#[cfg(test)]
mod bulk_decode_bench {
    //! Diagnostic comparison of the per-row ScalarValue decode (legacy
    //! algorithm) against the direct typed decode, on a synthetic tick
    //! payload. Run with:
    //! `cargo test --lib direct_bulk -- --ignored --nocapture`
    use super::*;
    use crate::protocol::types::DataType;
    use crate::storage::bulk_payload::DirectBulkColumn;
    use std::time::Instant;

    fn tick_request(rows: usize) -> (DirectBulkRequest<'static>, Vec<Vec<u8>>) {
        // Leak payloads to keep 'static references for DirectBulkRequest.
        let mut symbol_bytes: Vec<u8> = Vec::new();
        let mut symbol_ranges: Vec<(usize, usize)> = Vec::with_capacity(rows);
        let mut fixed: [Vec<u8>; 4] = [const { Vec::new() }; 4];
        for seq in 0..rows {
            let symbol = format!("SH{:06}", seq % 5000);
            symbol_ranges.push((symbol_bytes.len(), symbol.len()));
            symbol_bytes.extend_from_slice(symbol.as_bytes());
            fixed[0].extend_from_slice(&20260911i32.to_le_bytes());
            fixed[1].extend_from_slice(&(1_000 + (seq % 90_000) as i64).to_le_bytes());
            fixed[2].extend_from_slice(&100i64.to_le_bytes());
            fixed[3].extend_from_slice(&(1_783_700_000_000_000u64 as i64).to_le_bytes());
        }
        let symbol_bytes: &'static [u8] = Box::leak(symbol_bytes.into_boxed_slice());
        let symbol_ranges: &'static [(usize, usize)] = Box::leak(symbol_ranges.into_boxed_slice());
        let fixed_static: Vec<&'static [u8]> = fixed
            .into_iter()
            .map(|v| Box::leak(v.into_boxed_slice()) as &'static [u8])
            .collect();
        let mk = |name: &'static str,
                  dtype: DataType,
                  data: &'static [u8],
                  ranges: Option<&'static [(usize, usize)]>| DirectBulkColumn {
            name,
            data_type: dtype,
            scale: 0,
            precision: 0,
            data,
            nulls: &[],
            variable_ranges: ranges.map(|r| r.to_vec()),
        };
        let columns = vec![
            mk(
                "security_id",
                DataType::VarChar,
                symbol_bytes,
                Some(symbol_ranges),
            ),
            mk("deal_date", DataType::Int32, fixed_static[0], None),
            mk("price", DataType::Decimal, fixed_static[1], None),
            mk("trans_volume", DataType::Int64, fixed_static[2], None),
            mk("local_time", DataType::Timestamp, fixed_static[3], None),
        ];
        (
            DirectBulkRequest {
                table_name: "t",
                row_count: rows,
                columns,
            },
            Vec::new(),
        )
    }

    #[test]
    #[ignore = "diagnostic benchmark: run with --ignored --nocapture"]
    fn direct_decode_vs_per_row_scalar() {
        let rows = 100_000;
        let (req, _) = tick_request(rows);

        let start = Instant::now();
        let mut legacy_rows = 0usize;
        for col in &req.columns {
            let mut array = ColumnArray::empty(col.data_type);
            for row_idx in 0..req.row_count {
                let scalar = direct_bulk_column_scalar_at(col, row_idx).unwrap();
                array.push(&scalar).unwrap();
                legacy_rows += 1;
            }
        }
        let legacy = start.elapsed();

        let start = Instant::now();
        let mut direct_rows = 0usize;
        for col in &req.columns {
            let array = direct_bulk_column_to_array(col, req.row_count).unwrap();
            direct_rows += array.len();
        }
        let direct = start.elapsed();

        assert_eq!(legacy_rows, direct_rows);
        println!(
            "decode {rows} rows x {} cols: legacy per-row Scalar = {legacy:?}, direct typed = {direct:?} ({:.1}x faster)",
            req.columns.len(),
            legacy.as_secs_f64() / direct.as_secs_f64()
        );
    }
}
