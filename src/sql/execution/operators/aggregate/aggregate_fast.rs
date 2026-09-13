//! Single-column GROUP BY fast path.
//!
//! Generic grouped aggregation pays three per-row costs: an `encode_key_at`
//! key-buffer build, a SipHash lookup over `Vec<u8>`, and a per-accumulator
//! dispatch through `numeric_*_at`. The fast path interns the single group
//! column into dense u32 ids with an FxHash-family map, and accumulates
//! through typed column views resolved once per batch. Any shape that does
//! not fit (multi-key, DISTINCT aggregates, Min/Max, unsupported types)
//! stays on the generic loop in the parent module.

use std::collections::HashMap;
use std::hash::Hasher;

use crate::common::error::{Result, TaibaiError};
use crate::sql::execution::spill::{MemoryReservation, SpillFile};
use crate::sql::planner::{AggFunc, AggregateExpr, LogicalExpr};
use crate::types::array::{ColumnArray, Utf8Buffer};
use crate::types::batch::RecordBatch;
use crate::types::bitmap::NullBitmap;
use crate::types::scalar::ScalarValue;

use super::{spill_group_run, Accumulator, CountKind};

/// FxHash-family hasher (same construction as the hot store's string pool)
/// so group interning does not pay SipHash per row.
#[derive(Default)]
struct FastGroupHasher {
    hash: u64,
}

const FAST_GROUP_SEED: u64 = 0x517c_c1b7_2722_0a95;

impl FastGroupHasher {
    #[inline]
    fn add(&mut self, word: u64) {
        self.hash = (self.hash.rotate_left(5) ^ word).wrapping_mul(FAST_GROUP_SEED);
    }
}

impl Hasher for FastGroupHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.hash
    }

    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        let mut chunks = bytes.chunks_exact(8);
        for chunk in &mut chunks {
            self.add(u64::from_le_bytes(chunk.try_into().unwrap()));
        }
        let rem = chunks.remainder();
        if !rem.is_empty() {
            let mut buf = [0u8; 8];
            buf[..rem.len()].copy_from_slice(rem);
            self.add(u64::from_le_bytes(buf) ^ ((rem.len() as u64) << 56));
        }
    }
}

type FastGroupMap<K> = HashMap<K, u32, std::hash::BuildHasherDefault<FastGroupHasher>>;

/// Typed view of one aggregate argument column, resolved once per batch so
/// the row loop indexes plain slices instead of dispatching per row.
#[derive(Clone, Copy)]
enum FastArgCol<'a> {
    /// COUNT(*) — no argument column.
    None,
    /// COUNT(col) over a column the fast path does not type: only the null
    /// bitmap is needed.
    Bitmap(&'a NullBitmap),
    I64(&'a [i64], &'a NullBitmap),
    I32(&'a [i32], &'a NullBitmap),
    F64(&'a [f64], &'a NullBitmap),
    F32(&'a [f32], &'a NullBitmap),
}

impl<'a> FastArgCol<'a> {
    #[inline]
    fn is_valid(&self, row: usize) -> bool {
        match self {
            FastArgCol::None => true,
            FastArgCol::Bitmap(b)
            | FastArgCol::I64(_, b)
            | FastArgCol::I32(_, b)
            | FastArgCol::F64(_, b)
            | FastArgCol::F32(_, b) => !b.is_null(row),
        }
    }
}

enum FastGroupCol<'a> {
    Str(&'a Utf8Buffer, &'a NullBitmap),
    I64(&'a [i64], &'a NullBitmap),
    I32(&'a [i32], &'a NullBitmap),
}

/// Dictionary-keyed group table: dense u32 group ids with ordered `keys` /
/// `scalars` arrays that convert 1:1 into the generic `groups` / `group_keys`
/// structures for spilling and output.
pub(super) struct FastGroups {
    str_map: FastGroupMap<String>,
    i64_map: FastGroupMap<i64>,
    null_id: Option<u32>,
    keys: Vec<Vec<u8>>,
    scalars: Vec<ScalarValue>,
    accs: Vec<Vec<Accumulator>>,
}

impl FastGroups {
    pub(super) fn new() -> Self {
        Self {
            str_map: FastGroupMap::default(),
            i64_map: FastGroupMap::default(),
            null_id: None,
            keys: Vec::new(),
            scalars: Vec::new(),
            accs: Vec::new(),
        }
    }

    /// Convert into the generic grouping structures (insertion order kept).
    pub(super) fn into_maps(
        self,
    ) -> (
        HashMap<Vec<u8>, Vec<Accumulator>>,
        Vec<(Vec<u8>, Vec<ScalarValue>)>,
    ) {
        let mut groups = HashMap::with_capacity(self.accs.len());
        for (i, accs) in self.accs.into_iter().enumerate() {
            groups.insert(self.keys[i].clone(), accs);
        }
        let group_keys: Vec<(Vec<u8>, Vec<ScalarValue>)> = self
            .keys
            .into_iter()
            .zip(self.scalars.into_iter())
            .map(|(key, scalar)| (key, vec![scalar]))
            .collect();
        (groups, group_keys)
    }
}

pub(super) fn fast_group_supported(
    group_by: &[LogicalExpr],
    gb_indices: &[usize],
    aggregates: &[AggregateExpr],
    agg_col_indices: &[Option<usize>],
    batch: &RecordBatch,
) -> bool {
    if group_by.len() != 1 || !matches!(&group_by[0], LogicalExpr::Column(_)) {
        return false;
    }
    let Some(group_array) = batch.column(gb_indices[0]) else {
        return false;
    };
    let group_ok = matches!(
        group_array,
        ColumnArray::VarChar { .. }
            | ColumnArray::Int64 { .. }
            | ColumnArray::Int32 { .. }
            | ColumnArray::Decimal { .. }
            | ColumnArray::Timestamp { .. }
            | ColumnArray::Time { .. }
            | ColumnArray::Date { .. }
    );
    if !group_ok {
        return false;
    }
    for (agg, col_idx) in aggregates.iter().zip(agg_col_indices) {
        if agg.distinct {
            return false;
        }
        match agg.func {
            AggFunc::Count => {
                // COUNT(*) or COUNT(col) over any type: the fast path only
                // needs the argument's null bitmap, which every variant has.
                if let Some(idx) = col_idx {
                    if batch.column(*idx).is_none() {
                        return false;
                    }
                }
            }
            AggFunc::Sum | AggFunc::Avg => {
                let Some(idx) = col_idx else {
                    return false;
                };
                let Some(array) = batch.column(*idx) else {
                    return false;
                };
                let supported = matches!(
                    array,
                    ColumnArray::Int64 { .. }
                        | ColumnArray::Int32 { .. }
                        | ColumnArray::Decimal { .. }
                        | ColumnArray::Timestamp { .. }
                        | ColumnArray::Time { .. }
                        | ColumnArray::Date { .. }
                        | ColumnArray::Float64 { .. }
                        | ColumnArray::Float32 { .. }
                );
                if !supported {
                    return false;
                }
            }
            _ => return false,
        }
    }
    true
}

#[allow(clippy::too_many_arguments)]
pub(super) fn fast_group_update(
    fast: &mut FastGroups,
    batch: &RecordBatch,
    gb_col: usize,
    group_by: &[LogicalExpr],
    aggregates: &[AggregateExpr],
    agg_col_indices: &[Option<usize>],
    physical_schema: &crate::types::batch::Schema,
    max_groups: usize,
    memory_limit_bytes: usize,
    estimated_group_bytes: &mut usize,
    memory: &mut MemoryReservation,
    spills: &mut Vec<SpillFile>,
    spill_rows: usize,
) -> Result<()> {
    let group_array = batch
        .column(gb_col)
        .ok_or_else(|| TaibaiError::Internal("fast group: group column missing".into()))?;
    let group_col = match group_array {
        ColumnArray::VarChar { data, bitmap } => FastGroupCol::Str(data, bitmap),
        ColumnArray::Int64 { data, bitmap }
        | ColumnArray::Decimal { data, bitmap }
        | ColumnArray::Timestamp { data, bitmap }
        | ColumnArray::Time { data, bitmap } => FastGroupCol::I64(data, bitmap),
        ColumnArray::Int32 { data, bitmap } | ColumnArray::Date { data, bitmap } => {
            FastGroupCol::I32(data, bitmap)
        }
        _ => {
            return Err(TaibaiError::Internal(
                "fast group: unsupported group column".into(),
            ))
        }
    };

    let mut arg_views: Vec<FastArgCol> = Vec::with_capacity(aggregates.len());
    for (_agg, col_idx) in aggregates.iter().zip(agg_col_indices) {
        let Some(idx) = col_idx else {
            arg_views.push(FastArgCol::None);
            continue;
        };
        let Some(array) = batch.column(*idx) else {
            arg_views.push(FastArgCol::None);
            continue;
        };
        arg_views.push(match array {
            ColumnArray::Int64 { data, bitmap }
            | ColumnArray::Decimal { data, bitmap }
            | ColumnArray::Timestamp { data, bitmap }
            | ColumnArray::Time { data, bitmap } => FastArgCol::I64(data, bitmap),
            ColumnArray::Int32 { data, bitmap } | ColumnArray::Date { data, bitmap } => {
                FastArgCol::I32(data, bitmap)
            }
            ColumnArray::Float64 { data, bitmap } => FastArgCol::F64(data, bitmap),
            ColumnArray::Float32 { data, bitmap } => FastArgCol::F32(data, bitmap),
            ColumnArray::VarChar { bitmap, .. }
            | ColumnArray::Blob { bitmap, .. }
            | ColumnArray::Vector { bitmap, .. }
            | ColumnArray::Bool { bitmap, .. }
            | ColumnArray::Int8 { bitmap, .. }
            | ColumnArray::Int16 { bitmap, .. }
            | ColumnArray::UInt8 { bitmap, .. }
            | ColumnArray::UInt16 { bitmap, .. }
            | ColumnArray::UInt32 { bitmap, .. }
            | ColumnArray::UInt64 { bitmap, .. } => FastArgCol::Bitmap(bitmap),
            _ => FastArgCol::None,
        });
    }

    let rows = batch.num_rows();
    for row in 0..rows {
        let group_id: u32 = match &group_col {
            FastGroupCol::Str(data, bitmap) => {
                if bitmap.is_null(row) {
                    get_or_create_null_group(fast, batch, gb_col, row, group_by, aggregates, physical_schema, max_groups)?
                } else if let Some(id) = fast.str_map.get(&data[row]) {
                    *id
                } else {
                    let id = new_fast_group(
                        fast, batch, gb_col, row, group_by, aggregates, physical_schema,
                        max_groups,
                    )?;
                    fast.str_map.insert(data[row].to_owned(), id);
                    id
                }
            }
            FastGroupCol::I64(data, bitmap) => {
                if bitmap.is_null(row) {
                    get_or_create_null_group(fast, batch, gb_col, row, group_by, aggregates, physical_schema, max_groups)?
                } else if let Some(id) = fast.i64_map.get(&data[row]) {
                    *id
                } else {
                    let id = new_fast_group(
                        fast, batch, gb_col, row, group_by, aggregates, physical_schema,
                        max_groups,
                    )?;
                    fast.i64_map.insert(data[row], id);
                    id
                }
            }
            FastGroupCol::I32(data, bitmap) => {
                if bitmap.is_null(row) {
                    get_or_create_null_group(fast, batch, gb_col, row, group_by, aggregates, physical_schema, max_groups)?
                } else if let Some(id) = fast.i64_map.get(&(data[row] as i64)) {
                    *id
                } else {
                    let id = new_fast_group(
                        fast, batch, gb_col, row, group_by, aggregates, physical_schema,
                        max_groups,
                    )?;
                    fast.i64_map.insert(data[row] as i64, id);
                    id
                }
            }
        };

        let accums = &mut fast.accs[group_id as usize];
        for (k, acc) in accums.iter_mut().enumerate() {
            match acc {
                Accumulator::Count { count, col } => match col {
                    CountKind::Star => *count += 1,
                    CountKind::Column(_) => {
                        if arg_views[k].is_valid(row) {
                            *count += 1;
                        }
                    }
                },
                Accumulator::SumI(value) => match &arg_views[k] {
                    FastArgCol::I64(data, bitmap) => {
                        if !bitmap.is_null(row) {
                            *value = Some(value.unwrap_or(0).wrapping_add(data[row]));
                        }
                    }
                    FastArgCol::I32(data, bitmap) => {
                        if !bitmap.is_null(row) {
                            *value = Some(value.unwrap_or(0).wrapping_add(data[row] as i64));
                        }
                    }
                    _ => {}
                },
                Accumulator::SumF(value) => match &arg_views[k] {
                    FastArgCol::F64(data, bitmap) => {
                        if !bitmap.is_null(row) {
                            *value = Some(value.unwrap_or(0.0) + data[row]);
                        }
                    }
                    FastArgCol::F32(data, bitmap) => {
                        if !bitmap.is_null(row) {
                            *value = Some(value.unwrap_or(0.0) + data[row] as f64);
                        }
                    }
                    FastArgCol::I64(data, bitmap) => {
                        if !bitmap.is_null(row) {
                            *value = Some(value.unwrap_or(0.0) + data[row] as f64);
                        }
                    }
                    FastArgCol::I32(data, bitmap) => {
                        if !bitmap.is_null(row) {
                            *value = Some(value.unwrap_or(0.0) + data[row] as f64);
                        }
                    }
                    _ => {}
                },
                Accumulator::Avg { sum, count, any } => match &arg_views[k] {
                    FastArgCol::I64(data, bitmap) => {
                        if !bitmap.is_null(row) {
                            *sum += data[row] as f64;
                            *count += 1;
                            *any = true;
                        }
                    }
                    FastArgCol::I32(data, bitmap) => {
                        if !bitmap.is_null(row) {
                            *sum += data[row] as f64;
                            *count += 1;
                            *any = true;
                        }
                    }
                    FastArgCol::F64(data, bitmap) => {
                        if !bitmap.is_null(row) {
                            *sum += data[row];
                            *count += 1;
                            *any = true;
                        }
                    }
                    FastArgCol::F32(data, bitmap) => {
                        if !bitmap.is_null(row) {
                            *sum += data[row] as f64;
                            *count += 1;
                            *any = true;
                        }
                    }
                    _ => {}
                },
                // Min/Max/Distinct are excluded by eligibility.
                _ => {}
            }
        }
    }

    // Mirror the generic path's once-per-batch deep memory refresh and spill
    // boundary; the fast table converts 1:1 into the generic structures.
    *estimated_group_bytes = fast
        .keys
        .iter()
        .map(|k| k.len())
        .sum::<usize>()
        + fast.scalars.len() * std::mem::size_of::<ScalarValue>()
        + fast
            .accs
            .iter()
            .map(|accums| {
                accums
                    .iter()
                    .map(Accumulator::estimated_size_bytes)
                    .sum::<usize>()
            })
            .sum::<usize>();
    super::super::profile::record_current_peak_memory(*estimated_group_bytes);
    if *estimated_group_bytes >= memory_limit_bytes || !memory.try_resize(*estimated_group_bytes) {
        let (groups, group_keys) = std::mem::replace(fast, FastGroups::new()).into_maps();
        spills.push(spill_group_run(&groups, &group_keys, spill_rows)?);
        memory.release();
        *estimated_group_bytes = 0;
    }
    Ok(())
}

fn get_or_create_null_group(
    fast: &mut FastGroups,
    batch: &RecordBatch,
    gb_col: usize,
    row: usize,
    group_by: &[LogicalExpr],
    aggregates: &[AggregateExpr],
    physical_schema: &crate::types::batch::Schema,
    max_groups: usize,
) -> Result<u32> {
    if let Some(id) = fast.null_id {
        return Ok(id);
    }
    let id = new_fast_group(
        fast, batch, gb_col, row, group_by, aggregates, physical_schema, max_groups,
    )?;
    fast.null_id = Some(id);
    Ok(id)
}

#[allow(clippy::too_many_arguments)]
fn new_fast_group(
    fast: &mut FastGroups,
    batch: &RecordBatch,
    gb_col: usize,
    row: usize,
    group_by: &[LogicalExpr],
    aggregates: &[AggregateExpr],
    physical_schema: &crate::types::batch::Schema,
    max_groups: usize,
) -> Result<u32> {
    let _ = group_by;
    if fast.accs.len() >= max_groups {
        return Err(TaibaiError::Sql(format!(
            "aggregate group count exceeds limit {}; \
             consider refining GROUP BY to reduce cardinality",
            max_groups
        )));
    }
    let array = batch
        .column(gb_col)
        .ok_or_else(|| TaibaiError::Internal("fast group: group column missing".into()))?;
    let mut key = Vec::new();
    array.encode_key_at(row, &mut key);
    let scalar = array.scalar(row);
    let accs: Result<Vec<Accumulator>> = aggregates
        .iter()
        .map(|agg| Accumulator::new_for(agg, arg_column_type(agg, physical_schema)))
        .collect();
    let accs = accs?;
    let id = fast.accs.len() as u32;
    fast.keys.push(key);
    fast.scalars.push(scalar);
    fast.accs.push(accs);
    Ok(id)
}

fn arg_column_type(agg: &AggregateExpr, schema: &crate::types::batch::Schema) -> crate::protocol::types::DataType {
    match &*agg.arg {
        LogicalExpr::Column(idx) => schema
            .columns
            .get(*idx)
            .map(|c| c.data_type)
            .unwrap_or(crate::protocol::types::DataType::Int64),
        _ => crate::protocol::types::DataType::Int64,
    }
}
