//! Single-column GROUP BY fast path.
//!
//! Generic grouped aggregation pays three per-row costs: an `encode_key_at`
//! key-buffer build, a SipHash lookup over `Vec<u8>`, and a per-accumulator
//! dispatch through `numeric_*_at`. The fast path interns the single group
//! column into dense u32 ids with an FxHash-family map, and accumulates
//! through typed column views resolved once per batch.
//!
//! Batches are additionally split into per-slot row ranges, each slot owning
//! an independent group table; the tables merge at the end (group
//! accumulation is commutative), so the row loop scales across rayon lanes
//! instead of running on one thread. Any shape that does not fit (multi-key,
//! DISTINCT aggregates, Min/Max, unsupported types) stays on the generic
//! loop in the parent module.

use std::collections::HashMap;

use rayon::prelude::*;

use crate::common::error::{Result, TaibaiError};
use crate::sql::execution::spill::{MemoryReservation, SpillFile};
use crate::sql::planner::{AggFunc, AggregateExpr, LogicalExpr};
use crate::types::array::{ColumnArray, Utf8Buffer};
use crate::types::batch::{RecordBatch, Schema};
use crate::types::bitmap::NullBitmap;
use crate::types::scalar::ScalarValue;

use super::{spill_group_run, Accumulator, CountKind};

/// Minimum rows in a batch before splitting it across group-table slots.
const PARALLEL_MIN_ROWS: usize = 8_192;

/// Typed-loop shapes run inline on one lane below this batch size: at
/// ~2ns/row the split's scheduling overhead exceeds the parallelism gain.
const TYPED_INLINE_MAX_ROWS: usize = 65_536;

/// Diagnostic counter: how many row ranges ran the tight dictionary-key
/// loop (read by tests/external probes).
pub(crate) static TYPED_RANGE_CALLS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Temporary diagnostics: why the tight loop did not run.
pub(crate) static TYPED_MISS_GROUP: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
pub(crate) static TYPED_MISS_NULLS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
pub(crate) static TYPED_MISS_KINDS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Per-batch fixed-cost breakdown (nanoseconds, summed): lane par-join vs
/// post-join memory accounting.
pub(crate) static AGG_JOIN_NS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub(crate) static AGG_ACCOUNT_NS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
pub(crate) static AGG_SETUP_NS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

type FastGroupMap<K> =
    HashMap<K, u32, std::hash::BuildHasherDefault<crate::common::fast_hash::FastHasher>>;

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
    /// Dictionary passthrough: rows map to dict entries by u32 index; the
    /// per-slot `dict_gid` table caches batch-local entry -> group id, and
    /// the global `dict_map` keeps group identity stable across batches.
    Dict(&'a [u32], &'a Utf8Buffer, &'a NullBitmap),
}

/// Accumulator shape resolved once per range for the tight dictionary-key
/// loop: null-free Count/Sum/Avg over an i64-view argument. Every other
/// shape keeps the generic per-row dispatch.
enum TypedAcc<'a> {
    CountStar,
    SumI64(&'a [i64]),
    AvgI64(&'a [i64]),
}

impl<'a> TypedAcc<'a> {
    fn classify(agg: &AggregateExpr, view: &FastArgCol<'a>) -> Option<Self> {
        if agg.distinct {
            return None;
        }
        match (&agg.func, view) {
            // A null-free COUNT(col) counts rows — identical to COUNT(*).
            (AggFunc::Count, FastArgCol::None) => Some(TypedAcc::CountStar),
            (AggFunc::Count, FastArgCol::I64(_, bm)) if bm.null_count() == 0 => {
                Some(TypedAcc::CountStar)
            }
            (AggFunc::Sum, FastArgCol::I64(data, bm)) if bm.null_count() == 0 => {
                Some(TypedAcc::SumI64(data))
            }
            (AggFunc::Avg, FastArgCol::I64(data, bm)) if bm.null_count() == 0 => {
                Some(TypedAcc::AvgI64(data))
            }
            _ => None,
        }
    }
}

/// Where a merged entry lands: an existing group (merge accumulators) or a
/// fresh append into this table's arrays.
enum FastMergeTarget {
    Existing(u32),
    AppendStr(String, Vec<u8>),
    AppendI64(i64, Vec<u8>),
    AppendNull,
}

/// One slot's dictionary-keyed group table: dense u32 group ids with ordered
/// `keys` / `scalars` arrays that convert 1:1 into the generic `groups` /
/// `group_keys` structures for spilling and output.
pub(super) struct FastGroups {
    str_map: FastGroupMap<String>,
    i64_map: FastGroupMap<i64>,
    /// Dictionary fast path: batch-local entry -> group id cache. Reset per
    /// batch because each batch carries its own dict; group identity across
    /// batches comes from `dict_map`.
    dict_gid: Vec<Option<u32>>,
    /// Global dictionary-value -> group id map: keeps one group per distinct
    /// value across batches instead of one per batch-local entry.
    dict_map: FastGroupMap<String>,
    null_id: Option<u32>,
    keys: Vec<Vec<u8>>,
    scalars: Vec<ScalarValue>,
    accs: Vec<Vec<Accumulator>>,
    /// Running `estimated_bytes` — maintained on group creation / merge so
    /// the per-batch memory reservation is O(1) instead of O(groups).
    cached_bytes: usize,
}

impl FastGroups {
    fn new() -> Self {
        Self {
            str_map: FastGroupMap::default(),
            i64_map: FastGroupMap::default(),
            dict_map: FastGroupMap::default(),
            dict_gid: Vec::new(),
            null_id: None,
            keys: Vec::new(),
            scalars: Vec::new(),
            accs: Vec::new(),
            cached_bytes: 0,
        }
    }

    /// Size estimate of one freshly created group (key bytes, scalar, and
    /// its accumulator row).
    fn group_bytes(key: &[u8], accs: &[Accumulator]) -> usize {
        key.len()
            + std::mem::size_of::<ScalarValue>()
            + accs
                .iter()
                .map(Accumulator::estimated_size_bytes)
                .sum::<usize>()
    }

    /// O(1): maintained incrementally on group creation and merge. Merging
    /// accumulators of the fast-path shapes (Count/Sum/Avg) does not change
    /// their size, so only appended groups adjust the cache.
    fn estimated_bytes(&self) -> usize {
        self.cached_bytes
    }

    /// Merge another slot's table into this one. Group discovery order of
    /// the merged table is deterministic: this table's groups first, then
    /// the other table's in its insertion order.
    fn merge_from(&mut self, other: FastGroups) {
        let FastGroups {
            str_map: _,
            i64_map: _,
            dict_map: _,
            dict_gid: _,
            null_id: _,
            keys,
            scalars,
            accs,
            cached_bytes: other_bytes,
        } = other;
        for (key, scalar, mut accumulators) in keys
            .into_iter()
            .zip(scalars.into_iter())
            .zip(accs.into_iter())
            .map(|((k, s), a)| (k, s, a))
        {
            // Resolve the target group in this table, creating it (with the
            // other table's accumulators) on first sight. A NULL key scalar
            // routes to the table's single null group.
            let target = match &scalar {
                ScalarValue::VarChar(Some(value)) => match self.str_map.get(value.as_str()) {
                    Some(id) => {
                        let id = *id;
                        FastMergeTarget::Existing(id)
                    }
                    None => FastMergeTarget::AppendStr(value.clone(), key.clone()),
                },
                ScalarValue::Int64(Some(v))
                | ScalarValue::Decimal(Some(v))
                | ScalarValue::Timestamp(Some(v))
                | ScalarValue::Time(Some(v)) => match self.i64_map.get(v) {
                    Some(id) => FastMergeTarget::Existing(*id),
                    None => FastMergeTarget::AppendI64(*v, key.clone()),
                },
                ScalarValue::Int32(Some(v)) | ScalarValue::Date(Some(v)) => {
                    match self.i64_map.get(&(*v as i64)) {
                        Some(id) => FastMergeTarget::Existing(*id),
                        None => FastMergeTarget::AppendI64(*v as i64, key.clone()),
                    }
                }
                _ => match self.null_id {
                    Some(id) => FastMergeTarget::Existing(id),
                    None => FastMergeTarget::AppendNull,
                },
            };
            match target {
                FastMergeTarget::Existing(id) => {
                    let dst = &mut self.accs[id as usize];
                    for (dst_acc, src_acc) in dst.iter_mut().zip(accumulators.drain(..)) {
                        merge_accumulator(dst_acc, src_acc);
                    }
                }
                FastMergeTarget::AppendStr(value, key) => {
                    let id = self.accs.len() as u32;
                    self.str_map.insert(value, id);
                    self.keys.push(key);
                    self.scalars.push(scalar);
                    self.accs.push(accumulators);
                }
                FastMergeTarget::AppendI64(v, key) => {
                    let id = self.accs.len() as u32;
                    self.i64_map.insert(v, id);
                    self.keys.push(key);
                    self.scalars.push(scalar);
                    self.accs.push(accumulators);
                }
                FastMergeTarget::AppendNull => {
                    let id = self.accs.len() as u32;
                    self.null_id = Some(id);
                    self.keys.push(key);
                    self.scalars.push(scalar);
                    self.accs.push(accumulators);
                }
            }
        }
        self.cached_bytes += other_bytes;
    }

    /// Convert into the generic grouping structures (merge order kept).
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

    #[allow(clippy::too_many_arguments)]
    fn update_row_range(
        &mut self,
        group_col: &FastGroupCol,
        arg_views: &[FastArgCol],
        batch: &RecordBatch,
        gb_col: usize,
        aggregates: &[AggregateExpr],
        physical_schema: &Schema,
        max_groups: usize,
        start: usize,
        end: usize,
    ) -> Result<()> {
        // Tight loop for the dominant scan-aggregate shape: dictionary group
        // keys with null-free typed Count/Sum/Avg accumulators. Everything a
        // row needs is resolved once per range (data slices, accumulator
        // kinds), so the per-row work is an id lookup, an accumulator-slot
        // deref and typed adds — no bitmap probes, no per-row enum dispatch.
        // Any other shape (nulls, other types, DISTINCT) uses the generic
        // interpreter below.
        if let FastGroupCol::Dict(ids, dict, gbm) = group_col {
            if gbm.null_count() == 0 {
                let kinds = aggregates
                    .iter()
                    .zip(arg_views.iter())
                    .map(|(agg, view)| TypedAcc::classify(agg, view))
                    .collect::<Option<Vec<_>>>();
                if let Some(kinds) = kinds {
                    return self.update_row_range_dict_typed(
                        ids,
                        dict,
                        start,
                        end,
                        &kinds,
                        batch,
                        gb_col,
                        aggregates,
                        physical_schema,
                        max_groups,
                    );
                }
                TYPED_MISS_KINDS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            } else {
                TYPED_MISS_NULLS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        } else {
            TYPED_MISS_GROUP.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }

        for row in start..end {
            let group_id: u32 = match group_col {
                FastGroupCol::Dict(ids, dict, bitmap) => {
                    if bitmap.is_null(row) {
                        self.get_or_create_null_group(
                            batch,
                            gb_col,
                            row,
                            aggregates,
                            physical_schema,
                            max_groups,
                        )?
                    } else {
                        let entry = ids[row] as usize;
                        if entry >= self.dict_gid.len() {
                            self.dict_gid.resize(dict.len().max(entry + 1), None);
                        }
                        match self.dict_gid[entry] {
                            Some(id) => id,
                            None => {
                                let value = dict.value(entry);
                                let id = match self.dict_map.get(value).copied() {
                                    Some(id) => id,
                                    None => {
                                        let id = self.new_group(
                                            batch,
                                            gb_col,
                                            row,
                                            aggregates,
                                            physical_schema,
                                            max_groups,
                                        )?;
                                        self.cached_bytes += value.len() + 24;
                                        self.dict_map.insert(value.to_string(), id);
                                        id
                                    }
                                };
                                self.dict_gid[entry] = Some(id);
                                id
                            }
                        }
                    }
                }
                FastGroupCol::Str(data, bitmap) => {
                    if bitmap.is_null(row) {
                        self.get_or_create_null_group(
                            batch,
                            gb_col,
                            row,
                            aggregates,
                            physical_schema,
                            max_groups,
                        )?
                    } else if let Some(id) = self.str_map.get(&data[row]) {
                        *id
                    } else {
                        let id = self.new_group(
                            batch,
                            gb_col,
                            row,
                            aggregates,
                            physical_schema,
                            max_groups,
                        )?;
                        self.str_map.insert(data[row].to_owned(), id);
                        id
                    }
                }
                FastGroupCol::I64(data, bitmap) => {
                    if bitmap.is_null(row) {
                        self.get_or_create_null_group(
                            batch,
                            gb_col,
                            row,
                            aggregates,
                            physical_schema,
                            max_groups,
                        )?
                    } else if let Some(id) = self.i64_map.get(&data[row]) {
                        *id
                    } else {
                        let id = self.new_group(
                            batch,
                            gb_col,
                            row,
                            aggregates,
                            physical_schema,
                            max_groups,
                        )?;
                        self.i64_map.insert(data[row], id);
                        id
                    }
                }
                FastGroupCol::I32(data, bitmap) => {
                    if bitmap.is_null(row) {
                        self.get_or_create_null_group(
                            batch,
                            gb_col,
                            row,
                            aggregates,
                            physical_schema,
                            max_groups,
                        )?
                    } else if let Some(id) = self.i64_map.get(&(data[row] as i64)) {
                        *id
                    } else {
                        let id = self.new_group(
                            batch,
                            gb_col,
                            row,
                            aggregates,
                            physical_schema,
                            max_groups,
                        )?;
                        self.i64_map.insert(data[row] as i64, id);
                        id
                    }
                }
            };

            let accums = &mut self.accs[group_id as usize];
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
        Ok(())
    }

    /// Monomorphic row loop for dictionary group keys over null-free
    /// Count/Sum/Avg(i64-view) accumulators. `kinds` is classified once per
    /// range; unseen dict entries fall through to `new_group` at most
    /// `dict_len` times per batch.
    #[allow(clippy::too_many_arguments)]
    fn update_row_range_dict_typed(
        &mut self,
        ids: &[u32],
        dict: &Utf8Buffer,
        start: usize,
        end: usize,
        kinds: &[TypedAcc],
        batch: &RecordBatch,
        gb_col: usize,
        aggregates: &[AggregateExpr],
        physical_schema: &Schema,
        max_groups: usize,
    ) -> Result<()> {
        TYPED_RANGE_CALLS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if self.dict_gid.len() < dict.len() {
            self.dict_gid.resize(dict.len(), None);
        }
        for row in start..end {
            let entry = ids[row] as usize;
            let group_id = match self.dict_gid[entry] {
                Some(id) => id,
                None => {
                    let value = dict.value(entry);
                    let id = match self.dict_map.get(value).copied() {
                        Some(id) => id,
                        None => {
                            let id = self.new_group(
                                batch,
                                gb_col,
                                row,
                                aggregates,
                                physical_schema,
                                max_groups,
                            )?;
                            self.cached_bytes += value.len() + 24;
                            self.dict_map.insert(value.to_string(), id);
                            id
                        }
                    };
                    self.dict_gid[entry] = Some(id);
                    id
                }
            };
            let accs = &mut self.accs[group_id as usize];
            for (k, kind) in kinds.iter().enumerate() {
                match *kind {
                    TypedAcc::CountStar => match &mut accs[k] {
                        Accumulator::Count { count, .. } => {
                            *count = count.wrapping_add(1);
                        }
                        _ => {
                            return Err(TaibaiError::Internal(
                                "fast group: accumulator shape mismatch".into(),
                            ))
                        }
                    },
                    TypedAcc::SumI64(data) => match &mut accs[k] {
                        Accumulator::SumI(value) => {
                            *value = Some(value.unwrap_or(0).wrapping_add(data[row]));
                        }
                        _ => {
                            return Err(TaibaiError::Internal(
                                "fast group: accumulator shape mismatch".into(),
                            ))
                        }
                    },
                    TypedAcc::AvgI64(data) => match &mut accs[k] {
                        Accumulator::Avg { sum, count, any } => {
                            *sum += data[row] as f64;
                            *count += 1;
                            *any = true;
                        }
                        _ => {
                            return Err(TaibaiError::Internal(
                                "fast group: accumulator shape mismatch".into(),
                            ))
                        }
                    },
                }
            }
        }
        Ok(())
    }

    fn get_or_create_null_group(
        &mut self,
        batch: &RecordBatch,
        gb_col: usize,
        row: usize,
        aggregates: &[AggregateExpr],
        physical_schema: &Schema,
        max_groups: usize,
    ) -> Result<u32> {
        if let Some(id) = self.null_id {
            return Ok(id);
        }
        let id = self.new_group(batch, gb_col, row, aggregates, physical_schema, max_groups)?;
        self.null_id = Some(id);
        Ok(id)
    }

    fn new_group(
        &mut self,
        batch: &RecordBatch,
        gb_col: usize,
        row: usize,
        aggregates: &[AggregateExpr],
        physical_schema: &Schema,
        max_groups: usize,
    ) -> Result<u32> {
        if self.accs.len() >= max_groups {
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
        let id = self.accs.len() as u32;
        self.cached_bytes += Self::group_bytes(&key, &accs);
        self.keys.push(key);
        self.scalars.push(scalar);
        self.accs.push(accs);
        Ok(id)
    }
}

fn merge_accumulator(dst: &mut Accumulator, src: Accumulator) {
    match (dst, src) {
        (Accumulator::Count { count: d, .. }, Accumulator::Count { count: s, .. }) => {
            // Saturate like the generic `Accumulator::merge`: a count is a
            // row cardinality and must never wrap to a small value.
            *d = d.saturating_add(s);
        }
        (Accumulator::SumI(d), Accumulator::SumI(s)) => {
            *d = match (d.take(), s) {
                (None, None) => None,
                (Some(a), None) => Some(a),
                (None, b) => b,
                (Some(a), Some(b)) => Some(a.wrapping_add(b)),
            };
        }
        (Accumulator::SumF(d), Accumulator::SumF(s)) => {
            *d = match (d.take(), s) {
                (None, None) => None,
                (Some(a), None) => Some(a),
                (None, b) => b,
                (Some(a), Some(b)) => Some(a + b),
            };
        }
        (
            Accumulator::Avg {
                sum: d_sum,
                count: d_count,
                any: d_any,
            },
            Accumulator::Avg {
                sum: s_sum,
                count: s_count,
                any: s_any,
            },
        ) => {
            *d_sum += s_sum;
            *d_count += s_count;
            *d_any |= s_any;
        }
        _ => {}
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
            | ColumnArray::Dictionary { .. }
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
                // Temporal types stay on the generic path: the generic
                // accumulator does not sum them (SUM yields NULL), and the
                // fast path's raw-integer fold would diverge from that.
                let supported = matches!(
                    array,
                    ColumnArray::Int64 { .. }
                        | ColumnArray::Int32 { .. }
                        | ColumnArray::Decimal { .. }
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

/// Per-slot parallel update for one input batch. `tables` holds one lazily
/// created group table per lane; the batch's rows are split into contiguous
/// ranges so each table sees a disjoint, deterministic subset.
#[allow(clippy::too_many_arguments)]
pub(super) fn fast_group_update(
    tables: &mut Vec<Option<FastGroups>>,
    batch: &RecordBatch,
    gb_col: usize,
    aggregates: &[AggregateExpr],
    agg_col_indices: &[Option<usize>],
    physical_schema: &Schema,
    max_groups: usize,
    memory_limit_bytes: usize,
    estimated_group_bytes: &mut usize,
    memory: &mut MemoryReservation,
    spills: &mut Vec<SpillFile>,
    spill_rows: usize,
) -> Result<()> {
    let setup_t0 = std::time::Instant::now();
    let group_array = batch
        .column(gb_col)
        .ok_or_else(|| TaibaiError::Internal("fast group: group column missing".into()))?;
    let group_col = match group_array {
        ColumnArray::VarChar { data, bitmap } => FastGroupCol::Str(data, bitmap),
        ColumnArray::Dictionary {
            ids, dict, bitmap, ..
        } => FastGroupCol::Dict(ids, dict, bitmap),
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

    // Each batch carries its own dictionary; entry ids are batch-local.
    for table in tables.iter_mut() {
        if let Some(f) = table {
            f.dict_gid.clear();
        }
    }
    AGG_SETUP_NS.fetch_add(
        setup_t0.elapsed().as_nanos() as u64,
        std::sync::atomic::Ordering::Relaxed,
    );
    let join_t0 = std::time::Instant::now();

    let rows = batch.num_rows();
    let max_lanes = crate::common::config::query_limits()
        .max_parallel_row_groups
        .clamp(1, 8);
    // The typed dictionary-key loop runs ~2ns/row: at typical batch sizes
    // (~16K rows → ~35µs of work) splitting into 8 rayon lane tasks costs
    // more in scheduling than the parallelism saves, so those run inline on
    // a single lane. Branchier generic shapes and large batches keep lanes.
    let typed_shape = matches!(&group_col, FastGroupCol::Dict(_, _, bm) if bm.null_count() == 0)
        && aggregates
            .iter()
            .zip(arg_views.iter())
            .all(|(agg, view)| TypedAcc::classify(agg, view).is_some());
    let lanes = if typed_shape && rows < TYPED_INLINE_MAX_ROWS {
        1
    } else if rows >= PARALLEL_MIN_ROWS {
        max_lanes
    } else {
        1
    };
    // Grow only: shrinking the slot vec would drop the group state the
    // dropped slots accumulated for earlier batches.
    if tables.len() < lanes {
        tables.resize_with(lanes, || None);
    }
    // Slots beyond this batch's lane count stay untouched (their group state
    // remains valid); only the first `lanes` slots take rows this time.
    let lanes = lanes.min(tables.len());

    if lanes == 1 {
        let table = tables[0].get_or_insert_with(FastGroups::new);
        table.update_row_range(
            &group_col,
            &arg_views,
            batch,
            gb_col,
            aggregates,
            physical_schema,
            max_groups,
            0,
            rows,
        )?;
    } else {
        let chunk_size = rows.div_ceil(lanes);
        let results: Vec<Result<()>> = tables
            .par_iter_mut()
            .take(lanes)
            .enumerate()
            .map(|(slot, table)| {
                let start = slot * chunk_size;
                let end = (start + chunk_size).min(rows);
                if start >= end {
                    return Ok(());
                }
                let table = table.get_or_insert_with(FastGroups::new);
                table.update_row_range(
                    &group_col,
                    &arg_views,
                    batch,
                    gb_col,
                    aggregates,
                    physical_schema,
                    max_groups,
                    start,
                    end,
                )
            })
            .collect();
        for result in results {
            result?;
        }
    }
    AGG_JOIN_NS.fetch_add(
        join_t0.elapsed().as_nanos() as u64,
        std::sync::atomic::Ordering::Relaxed,
    );
    let account_t0 = std::time::Instant::now();

    // Mirror the generic path's once-per-batch deep memory refresh and spill
    // boundary; the slot tables merge 1:1 into the generic structures.
    let total: usize = tables
        .iter()
        .map(|table| table.as_ref().map_or(0, FastGroups::estimated_bytes))
        .sum();
    *estimated_group_bytes = total;
    super::super::profile::record_current_peak_memory(total);
    if total >= memory_limit_bytes || !memory.try_resize(total) {
        let merged = super::aggregate_fast::merge_slot_tables(tables);
        let (groups, group_keys) = merged.into_maps();
        spills.push(spill_group_run(&groups, &group_keys, spill_rows)?);
        memory.release();
        *estimated_group_bytes = 0;
    }
    AGG_ACCOUNT_NS.fetch_add(
        account_t0.elapsed().as_nanos() as u64,
        std::sync::atomic::Ordering::Relaxed,
    );
    Ok(())
}

/// Merge every slot table into one (slot order, then each slot's insertion
/// order), ready for `into_maps`.
pub(super) fn merge_slot_tables(tables: &mut Vec<Option<FastGroups>>) -> FastGroups {
    let mut merged = FastGroups::new();
    for table in tables.iter_mut() {
        if let Some(f) = table.take() {
            merged.merge_from(f);
        }
    }
    merged
}

fn arg_column_type(agg: &AggregateExpr, schema: &Schema) -> crate::protocol::types::DataType {
    match &*agg.arg {
        LogicalExpr::Column(idx) => schema
            .columns
            .get(*idx)
            .map(|c| c.data_type)
            .unwrap_or(crate::protocol::types::DataType::Int64),
        _ => crate::protocol::types::DataType::Int64,
    }
}
