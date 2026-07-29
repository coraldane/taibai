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

#[allow(dead_code)]
pub(super) fn placeholder_zone_maps(schema: &Schema, row_count: u64) -> Vec<ZoneMap> {
    schema
        .columns
        .iter()
        .map(|col| {
            let null = crate::storage::codec::scalar_tag::null_for_dtype(col.data_type);
            ZoneMap {
                min_value: null.clone(),
                max_value: null,
                null_count: 0,
                row_count,
            }
        })
        .collect()
}

pub(super) fn compute_zone_maps_from_batch(
    batch: &RecordBatch,
    start_row: usize,
    row_count: usize,
) -> Vec<ZoneMap> {
    batch
        .columns
        .iter()
        .map(|column| compute_zone_map_from_column(column, start_row, row_count))
        .collect()
}

fn compute_zone_map_from_column(
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
            for row in start_row..end_row {
                if bitmap.is_null(row) {
                    continue;
                }
                let value = $values[row];
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
            for row in start_row..end_row {
                if bitmap.is_null(row) {
                    continue;
                }
                let value = $values[row];
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
            for row in start_row..end_row {
                if bitmap.is_null(row) {
                    continue;
                }
                let value = data[row] != 0;
                if min.is_none_or(|current| value < current) {
                    min = Some(value);
                }
                if max.is_none_or(|current| value > current) {
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
        let mut array = ColumnArray::empty(col.data_type);
        for row_idx in 0..req.row_count {
            let scalar = direct_bulk_column_scalar_at(col, row_idx)?;
            array.push(&scalar)?;
        }
        columns.push(array);
    }
    RecordBatch::try_new(schema.clone(), columns)
}

pub(super) fn direct_bulk_column_scalar_at(
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

pub(super) fn sort_batch_by_key_indices(
    batch: &RecordBatch,
    key_indices: &[usize],
) -> Result<RecordBatch> {
    if batch.num_rows() <= 1 {
        return Ok(batch.clone());
    }

    let mut rows: Vec<usize> = (0..batch.num_rows()).collect();
    rows.sort_by(|left, right| {
        for &col_idx in key_indices {
            let left_value = batch.scalar(*left, col_idx).unwrap_or_else(|| {
                ScalarValue::null_for_type(batch.schema.columns[col_idx].data_type)
            });
            let right_value = batch.scalar(*right, col_idx).unwrap_or_else(|| {
                ScalarValue::null_for_type(batch.schema.columns[col_idx].data_type)
            });
            let cmp = crate::storage::index::sort_key::compare_scalars_for_sort(
                &left_value,
                &right_value,
            );
            if cmp != std::cmp::Ordering::Equal {
                return cmp;
            }
        }
        std::cmp::Ordering::Equal
    });

    let mut sorted = RecordBatch::new(batch.schema.clone());
    for row_idx in rows {
        for col_idx in 0..batch.num_columns() {
            let value = batch.scalar(row_idx, col_idx).unwrap_or_else(|| {
                ScalarValue::null_for_type(batch.schema.columns[col_idx].data_type)
            });
            sorted.column_mut(col_idx).unwrap().push(&value)?;
        }
    }
    Ok(sorted)
}

pub(super) fn filter_batch_by_mask(
    batch: &RecordBatch,
    mask: &[bool],
    match_count: usize,
) -> Result<RecordBatch> {
    let columns = batch
        .columns
        .iter()
        .map(|col| col.filter_by_mask_fast(mask, match_count))
        .collect();
    RecordBatch::try_new(batch.schema.clone(), columns)
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
mod zone_map_tests {
    use super::*;
    use crate::types::batch::ColumnDef;

    #[test]
    fn typed_zone_maps_preserve_ranges_and_null_counts() {
        let schema = Schema::new(vec![
            ColumnDef::new("number", DataType::Int32),
            ColumnDef::new("text", DataType::VarChar),
        ]);
        let mut batch = RecordBatch::new(schema);
        for (number, text) in [
            (
                ScalarValue::Int32(Some(9)),
                ScalarValue::VarChar(Some("z".into())),
            ),
            (
                ScalarValue::Int32(None),
                ScalarValue::VarChar(Some("b".into())),
            ),
            (ScalarValue::Int32(Some(-4)), ScalarValue::VarChar(None)),
            (
                ScalarValue::Int32(Some(3)),
                ScalarValue::VarChar(Some("m".into())),
            ),
        ] {
            batch.column_mut(0).unwrap().push(&number).unwrap();
            batch.column_mut(1).unwrap().push(&text).unwrap();
        }

        let maps = compute_zone_maps_from_batch(&batch, 0, 4);
        assert_eq!(maps[0].min_value, ScalarValue::Int32(Some(-4)));
        assert_eq!(maps[0].max_value, ScalarValue::Int32(Some(9)));
        assert_eq!(maps[0].null_count, 1);
        assert_eq!(maps[1].min_value, ScalarValue::VarChar(Some("b".into())));
        assert_eq!(maps[1].max_value, ScalarValue::VarChar(Some("z".into())));
        assert_eq!(maps[1].null_count, 1);
        assert!(maps.iter().all(|map| map.row_count == 4));
    }
}
