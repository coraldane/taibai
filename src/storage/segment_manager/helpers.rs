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

/// Fixed-width little-endian payload layout of one bulk type. This table is
/// the single authority for "which raw slot type backs this DataType":
/// scalar decode (`read_fixed_scalar`), whole-column bulk decode
/// (`SlotKind::bulk_column`), typed zone maps (`fixed_zone_map_typed`) and
/// column sums (`col_sums_from_bulk_columns`) all dispatch through it, so a
/// new fixed-width type is registered in one place.
#[derive(Clone, Copy, PartialEq, Eq)]
enum SlotKind {
    Bool,
    I8,
    I16,
    I32,
    I64,
    U8,
    U16,
    U32,
    U64,
    F32,
    F64,
    /// Shares the i64 slot; the scale lives in the schema, not the payload.
    Decimal,
    /// Shares the i32 slot (days since the epoch).
    Date,
    /// Time, Timestamp and Interval share the i64 slot.
    Time,
    Timestamp,
    Interval,
}

fn slot_kind(dtype: DataType) -> Option<SlotKind> {
    use DataType as D;
    Some(match dtype {
        D::Bool => SlotKind::Bool,
        D::Int8 => SlotKind::I8,
        D::Int16 => SlotKind::I16,
        D::Int32 => SlotKind::I32,
        D::Int64 => SlotKind::I64,
        D::UInt8 => SlotKind::U8,
        D::UInt16 => SlotKind::U16,
        D::UInt32 => SlotKind::U32,
        D::UInt64 => SlotKind::U64,
        D::Float32 => SlotKind::F32,
        D::Float64 => SlotKind::F64,
        D::Decimal => SlotKind::Decimal,
        D::Date => SlotKind::Date,
        D::Time => SlotKind::Time,
        D::Timestamp => SlotKind::Timestamp,
        D::Interval => SlotKind::Interval,
        D::VarChar | D::Blob | D::Vector => return None,
    })
}

impl SlotKind {
    fn width(self) -> usize {
        match self {
            Self::Bool | Self::I8 | Self::U8 => 1,
            Self::I16 | Self::U16 | Self::F32 => 2,
            Self::I32 | Self::U32 | Self::Date => 4,
            Self::I64 | Self::U64 | Self::F64 | Self::Decimal | Self::Time | Self::Timestamp
            | Self::Interval => 8,
        }
    }

    /// Bounds-checked decode of the slot at `offset` into a ScalarValue.
    fn read_scalar(self, data: &[u8], offset: usize) -> Option<ScalarValue> {
        macro_rules! le {
            ($t:ty, $variant:ident) => {{
                let end = offset + std::mem::size_of::<$t>();
                if end > data.len() {
                    return None;
                }
                let raw = <$t>::from_le_bytes(data[offset..end].try_into().ok()?);
                Some(ScalarValue::$variant(Some(raw)))
            }};
        }
        match self {
            Self::Bool => {
                if offset >= data.len() {
                    return None;
                }
                Some(ScalarValue::Bool(Some(data[offset] != 0)))
            }
            Self::I8 => le!(i8, Int8),
            Self::I16 => le!(i16, Int16),
            Self::I32 => le!(i32, Int32),
            Self::I64 => le!(i64, Int64),
            Self::U8 => le!(u8, UInt8),
            Self::U16 => le!(u16, UInt16),
            Self::U32 => le!(u32, UInt32),
            Self::U64 => le!(u64, UInt64),
            Self::F32 => le!(f32, Float32),
            Self::F64 => le!(f64, Float64),
            Self::Decimal => le!(i64, Decimal),
            Self::Date => le!(i32, Date),
            Self::Time => le!(i64, Time),
            Self::Timestamp => le!(i64, Timestamp),
            Self::Interval => le!(i64, Interval),
        }
    }

    /// i64-domain projection of the slot, used by column sums and typed
    /// zone maps. `None` for Bool/UInt64/Float32/Float64, whose order or
    /// range an i64 projection does not preserve — those keep the per-row
    /// `ZoneMap::update` / `ScalarValue` paths. The type set matches
    /// `crate::storage::segment::column_sum_supported`.
    fn read_i64(self, data: &[u8], offset: usize) -> Option<i64> {
        macro_rules! le {
            ($t:ty) => {{
                let end = offset + std::mem::size_of::<$t>();
                if end > data.len() {
                    return None;
                }
                Some(<$t>::from_le_bytes(data[offset..end].try_into().ok()?) as i64)
            }};
        }
        match self {
            Self::I8 => le!(i8),
            Self::U8 => le!(u8),
            Self::I16 => le!(i16),
            Self::U16 => le!(u16),
            Self::I32 | Self::Date => le!(i32),
            Self::U32 => le!(u32),
            Self::I64 | Self::Decimal | Self::Time | Self::Timestamp | Self::Interval => le!(i64),
            _ => None,
        }
    }

    /// ScalarValue for an i64-domain value (`read_i64`'s inverse). Other
    /// kinds have no i64 projection; the placeholder they return is never
    /// reached because callers only feed kinds accepted by `read_i64`.
    fn scalar_from_i64(self, v: i64) -> ScalarValue {
        match self {
            Self::I8 => ScalarValue::Int8(Some(v as i8)),
            Self::U8 => ScalarValue::UInt8(Some(v as u8)),
            Self::I16 => ScalarValue::Int16(Some(v as i16)),
            Self::U16 => ScalarValue::UInt16(Some(v as u16)),
            Self::I32 => ScalarValue::Int32(Some(v as i32)),
            Self::U32 => ScalarValue::UInt32(Some(v as u32)),
            Self::Date => ScalarValue::Date(Some(v as i32)),
            Self::I64 => ScalarValue::Int64(Some(v)),
            Self::Decimal => ScalarValue::Decimal(Some(v)),
            Self::Time => ScalarValue::Time(Some(v)),
            Self::Timestamp => ScalarValue::Timestamp(Some(v)),
            Self::Interval => ScalarValue::Interval(Some(v)),
            _ => ScalarValue::Int64(None),
        }
    }

    /// Decode a NULL-free payload of exactly `rows` slots into the typed
    /// column (the bulk import fast path).
    fn bulk_column(self, data: &[u8], rows: usize) -> ColumnArray {
        macro_rules! slots {
            ($variant:ident, $t:ty) => {{
                let mut vals = Vec::with_capacity(rows);
                for chunk in data.chunks_exact(std::mem::size_of::<$t>()) {
                    vals.push(<$t>::from_le_bytes(chunk.try_into().expect("width match")));
                }
                ColumnArray::$variant {
                    data: vals,
                    bitmap: NullBitmap::new_valid(rows),
                }
            }};
        }
        match self {
            Self::Bool => ColumnArray::Bool {
                data: data.to_vec(),
                bitmap: NullBitmap::new_valid(rows),
            },
            Self::I8 => ColumnArray::Int8 {
                data: data.iter().map(|&b| b as i8).collect(),
                bitmap: NullBitmap::new_valid(rows),
            },
            Self::U8 => ColumnArray::UInt8 {
                data: data.to_vec(),
                bitmap: NullBitmap::new_valid(rows),
            },
            Self::I16 => slots!(Int16, i16),
            Self::U16 => slots!(UInt16, u16),
            Self::I32 => slots!(Int32, i32),
            Self::U32 => slots!(UInt32, u32),
            Self::F32 => slots!(Float32, f32),
            Self::Date => slots!(Date, i32),
            Self::I64 => slots!(Int64, i64),
            Self::U64 => slots!(UInt64, u64),
            Self::F64 => slots!(Float64, f64),
            Self::Decimal => slots!(Decimal, i64),
            Self::Time => slots!(Time, i64),
            Self::Timestamp => slots!(Timestamp, i64),
            Self::Interval => slots!(Interval, i64),
        }
    }
}

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
    let loop_ranges = loop_variable_ranges(col, rows)?;
    let has_nulls = !col.nulls.is_empty() && (0..rows).any(|r| null_bit_is_set(&col.nulls, r));
    if has_nulls {
        // Rare path: per-row decode honouring the NULL bitmap.
        let mut array = ColumnArray::with_capacity(col.data_type, rows);
        for row_idx in 0..rows {
            if null_bit_is_set(&col.nulls, row_idx) {
                array.push_null_value();
            } else {
                array.push(&bulk_column_scalar_at(
                    col,
                    row_idx,
                    loop_ranges.as_deref(),
                )?)?;
            }
        }
        return Ok(array);
    }

    // Fixed-width types: the payload must be exactly `rows` little-endian
    // slots (SlotKind is the shared decode table); a size mismatch falls
    // through to the per-row fallback below.
    if let Some(kind) = slot_kind(col.data_type) {
        if rows.checked_mul(kind.width()) == Some(col.data.len()) {
            return Ok(kind.bulk_column(col.data, rows));
        }
    }

    if col.data_type == DataType::VarChar {
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

    // Fallback for unexpected payload sizes and variable-width types without
    // prebuilt ranges: per-row decode (still NULL-free).
    let mut array = ColumnArray::with_capacity(col.data_type, rows);
    for row_idx in 0..rows {
        array.push(&bulk_column_scalar_at(
            col,
            row_idx,
            loop_ranges.as_deref(),
        )?)?;
    }
    Ok(array)
}

pub(crate) fn direct_bulk_column_scalar_at(
    col: &DirectBulkColumn<'_>,
    row_idx: usize,
) -> Result<ScalarValue> {
    bulk_column_scalar_at(col, row_idx, None)
}

/// Row decode with precomputed variable-width ranges. Loop callers hoist
/// the range walk with [`loop_variable_ranges`] — re-walking the
/// length-prefixed payload per row made such loops quadratic in the
/// payload size.
pub(crate) fn bulk_column_scalar_at(
    col: &DirectBulkColumn<'_>,
    row_idx: usize,
    ranges: Option<&[(usize, usize)]>,
) -> Result<ScalarValue> {
    if is_null_at(&col.nulls, row_idx) {
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

    let (offset, len) = match ranges {
        Some(ranges) => ranges.get(row_idx).copied().ok_or_else(|| {
            TaibaiError::Protocol(format!(
                "bulk column '{}' row {} not found",
                col.name, row_idx
            ))
        })?,
        None => walk_variable_range_at(col, row_idx)?,
    };
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

/// (offset, len) of one length-prefixed variable-width value, walked from
/// the payload start. Single-row callers only; loop callers precompute the
/// whole range vector with [`loop_variable_ranges`].
fn walk_variable_range_at(
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
        if is_null_at(&col.nulls, row_idx) {
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

/// Ranges for one per-row loop over `col`: the protocol-supplied ranges
/// when present, otherwise one linear walk of the length-prefixed payload.
/// `None` for fixed-width columns (per-row ranges are not consulted).
pub(crate) fn loop_variable_ranges(
    col: &DirectBulkColumn<'_>,
    rows: usize,
) -> Result<Option<Vec<(usize, usize)>>> {
    if col.data_type.fixed_size().is_some() {
        return Ok(None);
    }
    if let Some(ranges) = col.variable_ranges.as_ref() {
        return Ok(Some(ranges.clone()));
    }
    let mut out = Vec::with_capacity(rows);
    let mut offset = 0usize;
    for row in 0..rows {
        if is_null_at(&col.nulls, row) {
            // Placeholder keeps row indexing aligned; null rows never
            // consult their range.
            out.push((0, 0));
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
        out.push((offset, len));
        offset += len;
    }
    Ok(Some(out))
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

use rayon::prelude::*;

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
        let nulls = &col.nulls;
        let elem_count = row_count as usize;

        let mut null_count = 0u64;
        if let Some(width) = dtype.fixed_size() {
            if !fixed_zone_map_typed(dtype, data, nulls, elem_count, &mut null_count, &mut zm) {
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
            }
        } else if dtype == DataType::VarChar {
            // UTF-8 String ordering is byte-wise, so min/max are found over
            // raw value slices without allocating a String per row; only the
            // two bounding values materialize. Invalid UTF-8 falls back to
            // the legacy per-row pass (which read invalid values as "").
            if !varchar_zone_map_bytes(col, nulls, elem_count, &mut null_count, &mut zm) {
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
                    let value = std::str::from_utf8(&data[offset..offset + len])
                        .unwrap_or("")
                        .to_string();
                    zm.update(&ScalarValue::VarChar(Some(value)));
                }
            }
        }

        zm.null_count = null_count;
        zm.row_count = row_count;
        zone_maps.push(zm);
    }
    zone_maps
}

/// Byte-level min/max over the non-null VarChar values of one bulk row-group
/// piece. Returns false when a scanned value is not valid UTF-8, in which case
/// the caller reruns the legacy per-row semantics.
fn varchar_zone_map_bytes(
    col: &DirectBulkColumn<'_>,
    nulls: &[u8],
    elem_count: usize,
    null_count: &mut u64,
    zm: &mut ZoneMap,
) -> bool {
    let Some(ranges) = col.variable_ranges.as_ref() else {
        return true;
    };
    let data = col.data;
    let mut min_bytes: Option<&[u8]> = None;
    let mut max_bytes: Option<&[u8]> = None;
    for i in 0..elem_count {
        if is_null_at(nulls, i) {
            *null_count += 1;
            continue;
        }
        let Some((offset, len)) = ranges.get(i).copied() else {
            break;
        };
        if offset + len > data.len() {
            break;
        }
        let value = &data[offset..offset + len];
        if std::str::from_utf8(value).is_err() {
            return false;
        }
        match min_bytes {
            None => {
                min_bytes = Some(value);
                max_bytes = Some(value);
            }
            Some(cur_min) => {
                if value < cur_min {
                    min_bytes = Some(value);
                }
                if Some(value) > max_bytes {
                    max_bytes = Some(value);
                }
            }
        }
    }
    if let (Some(min), Some(max)) = (min_bytes, max_bytes) {
        if let (Ok(min), Ok(max)) = (std::str::from_utf8(min), std::str::from_utf8(max)) {
            zm.min_value = ScalarValue::VarChar(Some(min.to_string()));
            zm.max_value = ScalarValue::VarChar(Some(max.to_string()));
        }
    }
    true
}

/// Typed min/max over the non-null fixed-width values of one bulk row-group
/// piece. Same ScalarValue variants and ordering as the `read_fixed_scalar`
/// loop, without a per-row enum match; returns false for types (e.g. Bool)
/// and layouts the generic path must handle.
fn fixed_zone_map_typed(
    dtype: DataType,
    data: &[u8],
    nulls: &[u8],
    elem_count: usize,
    null_count: &mut u64,
    zm: &mut ZoneMap,
) -> bool {
    let Some(width) = dtype.fixed_size() else {
        return false;
    };
    if width == 0 || elem_count == 0 {
        return false;
    }
    let byte_len = match elem_count.checked_mul(width) {
        Some(len) => len,
        None => return false,
    };
    if data.len() < byte_len {
        return false;
    }
    let null_free = nulls.is_empty();

    // Signed and zero-extended unsigned orders both match their i64
    // projections, so one i64 min/max pass serves every sum-supported type.
    macro_rules! minmax {
        ($t:ty, $to_i64:expr) => {{
            let base = data.as_ptr() as *const $t;
            if null_free {
                let mut min = unsafe { base.read_unaligned() };
                let mut max = min;
                for i in 1..elem_count {
                    let v = unsafe { base.add(i).read_unaligned() };
                    if v < min {
                        min = v;
                    }
                    if v > max {
                        max = v;
                    }
                }
                ($to_i64(min), $to_i64(max))
            } else {
                // Nulls present: seed from the first non-null row so NULL
                // slots never contribute a value.
                let mut seed = None;
                for i in 0..elem_count {
                    if is_null_at(nulls, i) {
                        *null_count += 1;
                        continue;
                    }
                    seed = Some(i);
                    break;
                }
                match seed {
                    None => {
                        *null_count = elem_count as u64;
                        (0i64, 0i64)
                    }
                    Some(first) => {
                        let mut min = unsafe { base.add(first).read_unaligned() };
                        let mut max = min;
                        for i in (first + 1)..elem_count {
                            if is_null_at(nulls, i) {
                                *null_count += 1;
                                continue;
                            }
                            let v = unsafe { base.add(i).read_unaligned() };
                            if v < min {
                                min = v;
                            }
                            if v > max {
                                max = v;
                            }
                        }
                        ($to_i64(min), $to_i64(max))
                    }
                }
            }
        }};
    }

    let Some(kind) = slot_kind(dtype) else {
        return false;
    };
    let (min, max) = match kind {
        SlotKind::I8 => minmax!(i8, |v: i8| v as i64),
        SlotKind::U8 => minmax!(u8, |v: u8| v as i64),
        SlotKind::I16 => minmax!(i16, |v: i16| v as i64),
        SlotKind::U16 => minmax!(u16, |v: u16| v as i64),
        SlotKind::I32 | SlotKind::Date => minmax!(i32, |v: i32| v as i64),
        SlotKind::U32 => minmax!(u32, |v: u32| v as i64),
        SlotKind::I64 | SlotKind::Decimal | SlotKind::Time | SlotKind::Timestamp
        | SlotKind::Interval => minmax!(i64, |v: i64| v),
        // Bool/UInt64/Float orders are not preserved by an i64 projection;
        // the caller's per-row ZoneMap::update path handles them.
        _ => return false,
    };
    if null_free || *null_count < elem_count as u64 {
        zm.min_value = kind.scalar_from_i64(min);
        zm.max_value = kind.scalar_from_i64(max);
    }
    true
}

/// Footer-persisted running sums for one bulk-imported row group: wrapping
/// i64 over the non-null values of each sum-supported column, read straight
/// from the fixed-width LE payload. Unsupported columns carry 0.
pub(super) fn col_sums_from_bulk_columns(
    columns: &[DirectBulkColumn<'_>],
    row_count: u64,
) -> Vec<i64> {
    let elem_count = row_count as usize;
    columns
        .par_iter()
        .map(|col| {
            let dtype = col.data_type;
            if !crate::storage::segment::column_sum_supported(dtype) {
                return 0i64;
            }
            let Some(width) = dtype.fixed_size() else {
                return 0i64;
            };
            if width == 0 {
                return 0i64;
            }
            let data = col.data;
            let Some(byte_len) = elem_count.checked_mul(width) else {
                return 0i64;
            };
            if col.nulls.is_empty() && data.len() >= byte_len {
                // Null-free fast path (the common bulk case): the payload is
                // exactly elem_count fixed-width LE slots, so fold raw words
                // with unaligned loads instead of a per-row enum match.
                // Row order + wrapping i64 keep this bit-exact with the
                // bitmap path below.
                return unsafe {
                    match slot_kind(dtype) {
                        Some(SlotKind::I8) => sum_fixed_le::<i8>(data, elem_count),
                        Some(SlotKind::U8) => sum_fixed_le::<u8>(data, elem_count),
                        Some(SlotKind::I16) => sum_fixed_le::<i16>(data, elem_count),
                        Some(SlotKind::U16) => sum_fixed_le::<u16>(data, elem_count),
                        Some(SlotKind::I32 | SlotKind::Date) => {
                            sum_fixed_le::<i32>(data, elem_count)
                        }
                        Some(SlotKind::U32) => sum_fixed_le::<u32>(data, elem_count),
                        Some(
                            SlotKind::I64
                            | SlotKind::Decimal
                            | SlotKind::Time
                            | SlotKind::Timestamp
                            | SlotKind::Interval,
                        ) => sum_fixed_le::<i64>(data, elem_count),
                        _ => 0,
                    }
                };
            }
            let Some(kind) = slot_kind(dtype) else {
                return 0i64;
            };
            let mut sum = 0i64;
            for row in 0..elem_count {
                if col.is_null(row) {
                    continue;
                }
                if let Some(raw) = kind.read_i64(data, row * width) {
                    sum = sum.wrapping_add(raw);
                }
            }
            sum
        })
        .collect()
}

/// Wrapping i64 sum over `rows` little-endian `T` slots of `data`, in row
/// order. The bulk piece layout guarantees `data.len() >= rows * size_of::<T>()`.
///
/// # Safety
/// `data` must contain at least `rows * size_of::<T>()` bytes.
unsafe fn sum_fixed_le<T: Copy + Into<i64>>(data: &[u8], rows: usize) -> i64 {
    let mut acc = 0i64;
    let base = data.as_ptr() as *const T;
    for i in 0..rows {
        acc = acc.wrapping_add(base.add(i).read_unaligned().into());
    }
    acc
}

pub(super) fn compute_value_sets_from_bulk_columns(
    columns: &[DirectBulkColumn<'_>],
    row_count: u64,
) -> Result<Vec<Option<Vec<ScalarValue>>>> {
    let mut result = Vec::with_capacity(columns.len());
    for col in columns {
        let loop_ranges = loop_variable_ranges(col, row_count as usize)?;
        let mut values = HashSet::new();
        let mut complete = true;
        for row_idx in 0..row_count as usize {
            let value = bulk_column_scalar_at(col, row_idx, loop_ranges.as_deref())?;
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
    slot_kind(dtype)?.read_scalar(data, offset)
}

#[cfg(test)]
#[path = "zone_map_test.rs"]
mod zone_map_tests;

#[cfg(test)]
#[path = "bulk_decode_bench.rs"]
mod bulk_decode_bench;
