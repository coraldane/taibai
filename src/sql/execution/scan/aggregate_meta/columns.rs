//! Schema drift, projected reads and run-fold planning.

use super::*;

/// How one catalog-space aggregate argument column relates to a segment
/// footer schema, resolved by name (current name, then previous names).
#[derive(Debug, PartialEq)]
pub(super) enum FooterColumnDrift {
    /// The footer carries the column (possibly under a previous name, or
    /// with extra columns the query never mentions after DROP COLUMN).
    Present,
    /// The footer predates the column: every row of this segment logically
    /// carries this constant DEFAULT (an absent default is a constant NULL).
    ConstantDefault(ScalarValue),
    /// The footer predates the column and its DEFAULT is not a compile-time
    /// constant (CURRENT_TIMESTAMP) — the metadata fold cannot answer it.
    NonConstantDefault,
}

/// Classify one column of `table_schema` against `footer_schema`. Reuses the
/// catalog's `previous_names` exactly like the count fast paths and the
/// regular pipeline's name-based remapping (`align_indices_against_schema`).
pub(super) fn footer_column_drift(
    table_schema: &Schema,
    footer_schema: &Schema,
    column: usize,
) -> FooterColumnDrift {
    let Some(col_def) = table_schema.columns.get(column) else {
        // Out-of-range catalog index: let the regular alignment surface its
        // own error instead of guessing here.
        return FooterColumnDrift::Present;
    };
    let mut names = Vec::with_capacity(col_def.previous_names.len() + 1);
    names.push(col_def.name.clone());
    names.extend(col_def.previous_names.iter().cloned());
    if names
        .iter()
        .any(|name| footer_schema.index_of(name).is_some())
    {
        return FooterColumnDrift::Present;
    }
    match &col_def.default_value {
        Some(crate::types::batch::ColumnDefaultValue::Literal(value)) => {
            FooterColumnDrift::ConstantDefault(value.clone())
        }
        Some(crate::types::batch::ColumnDefaultValue::CurrentTimestamp) => {
            FooterColumnDrift::NonConstantDefault
        }
        None => FooterColumnDrift::ConstantDefault(ScalarValue::null_for_type(col_def.data_type)),
    }
}

/// The data-fold batch for one row group, in `read_columns` (catalog) order:
/// footer-resolvable columns decode from the row group, columns the footer
/// predates splice in as their constant DEFAULT — the same value
/// `read_row_group_columns_aligned` materializes on the regular path, so the
/// two paths can never disagree. Synthesis happens before the delete/mask
/// filters so the default column follows the surviving row count.
pub(super) fn read_data_fold_batch(
    reader: &SegmentReader,
    table_schema: &Schema,
    row_group_index: usize,
    read_columns: &[usize],
    defaults: &std::collections::HashMap<usize, ScalarValue>,
) -> Result<RecordBatch> {
    let mut present: Vec<usize> = Vec::with_capacity(read_columns.len());
    for &column in read_columns {
        if !defaults.contains_key(&column) {
            present.push(column);
        }
    }
    let projected = if present.is_empty() {
        None
    } else {
        let aligned_read_columns =
            row_filter::align_indices_to_footer(table_schema, reader.schema(), &present)?;
        Some(reader.read_row_group_with_projection(row_group_index, &aligned_read_columns)?)
    };
    let rows = projected
        .as_ref()
        .map(|batch| batch.num_rows())
        .unwrap_or_else(|| reader.row_group_rows(row_group_index) as usize);
    let mut present_columns = projected
        .map(|batch| batch.columns)
        .unwrap_or_else(Vec::new);
    let mut present_iter = present_columns.drain(..);
    let mut columns = Vec::with_capacity(read_columns.len());
    let mut col_defs = Vec::with_capacity(read_columns.len());
    for &column in read_columns {
        let col_def = table_schema
            .columns
            .get(column)
            .ok_or_else(|| {
                TaibaiError::Internal(format!("aggregate column index {column} out of range"))
            })?
            .clone();
        if let Some(default) = defaults.get(&column) {
            columns.push(constant_scalar_column(col_def.data_type, default, rows)?);
        } else {
            columns.push(present_iter.next().ok_or_else(|| {
                TaibaiError::Internal(format!(
                    "data-fold column {column} missing from projected read"
                ))
            })?);
        }
        col_defs.push(col_def);
    }
    RecordBatch::try_new(Schema::new(col_defs), columns)
        .map_err(|e| TaibaiError::Internal(e.to_string()))
}

/// A full-length column whose every row holds the same scalar (the constant
/// DEFAULT of a column this segment predates).
pub(super) fn constant_scalar_column(
    data_type: DataType,
    value: &ScalarValue,
    row_count: usize,
) -> Result<crate::types::array::ColumnArray> {
    let mut column = crate::types::array::ColumnArray::empty(data_type);
    for _ in 0..row_count {
        column.push(value)?;
    }
    Ok(column)
}

/// Whether every aggregate can fold from these zone maps: each argument
/// column's map must carry computed statistics. Bulk direct writes compute
/// zone maps for the partition/cluster key columns only and fill the rest
/// with defaults whose NULL min / zero null_count are indistinguishable
/// from a genuinely all-null row group — folding those from metadata would
/// miscount NULLs for COUNT(col) and return NULL for MIN/MAX (the same
/// defect once fixed for SUM/AVG). Such segments must fold from data.
/// Columns carrying a constant DEFAULT (the footer predates them) fold
/// arithmetically and are always answerable. COUNT(*) has no argument
/// column and is always foldable.
pub(super) fn aggregates_foldable_from_metadata(
    accumulators: &[AggAccum],
    aggregate_columns: &[Option<usize>],
    column_map: &std::collections::HashMap<usize, usize>,
    defaults: &std::collections::HashMap<usize, ScalarValue>,
    zone_maps: &[crate::storage::partition::pruning::ZoneMap],
) -> bool {
    accumulators
        .iter()
        .zip(aggregate_columns)
        .all(|(_, column)| match column {
            Some(col) => {
                defaults.contains_key(col)
                    || zone_maps
                        .get(row_filter::mapped_column(column_map, *col))
                        .is_some_and(ZoneMap::is_computed)
            }
            None => true,
        })
}

pub(super) fn fold_batch(
    accumulators: &mut [AggAccum],
    aggregate_positions: &[Option<usize>],
    batch: &RecordBatch,
) -> Result<()> {
    for (accumulator, column) in accumulators.iter_mut().zip(aggregate_positions) {
        accumulator.fold_batch(batch, *column)?;
    }
    Ok(())
}

/// A row group's run-level fold plan: which column's encoded runs answer
/// every aggregate, and which predicates the runs must satisfy.
pub(super) struct RunFoldPlan {
    /// Table-schema index of the folded column.
    pub(super) fold_column: usize,
    /// Segment-schema index of the same column (name-mapped).
    pub(super) segment_column: usize,
    pub(super) predicates: Vec<RunPredicate>,
}

/// Finalize a discriminating run fold on `fold_column`: the column type
/// must be i64-representable, every aggregate must read it (COUNT(*) aside),
/// and the segment must still carry it (name-mapped).
pub(super) fn plan_run_fold_on_column(
    fold_column: Option<usize>,
    predicates: Vec<RunPredicate>,
    accumulators: &[AggAccum],
    table_schema: &Schema,
    reader: &SegmentReader,
) -> Option<RunFoldPlan> {
    let fold_column = fold_column?;
    if !run_fold_type_ok(table_schema.columns.get(fold_column)?.data_type) {
        return None;
    }
    if accumulators
        .iter()
        .any(|a| a.column.is_some_and(|column| column != fold_column))
    {
        return None;
    }
    let segment_column = reader
        .schema()
        .index_of(&table_schema.columns.get(fold_column)?.name)?;
    Some(RunFoldPlan {
        fold_column,
        segment_column,
        predicates,
    })
}

/// Plan a run fold for a row group where every predicate is vacuous: the
/// aggregates must share one i64-domain argument column, and COUNT(*)
/// additionally requires that column null-free (run lengths count non-null
/// values only). No predicates — every run folds.
pub(super) fn plan_vacuous_run_fold(
    aggregate_columns: &[Option<usize>],
    accumulators: &[AggAccum],
    table_schema: &Schema,
    column_map: &std::collections::HashMap<usize, usize>,
    zone_maps: &[ZoneMap],
    reader: &SegmentReader,
) -> Option<RunFoldPlan> {
    let mut distinct: Vec<usize> = Vec::new();
    for &column in aggregate_columns.iter().flatten() {
        if !distinct.contains(&column) {
            distinct.push(column);
        }
    }
    if distinct.len() != 1 {
        return None;
    }
    let column = distinct[0];
    if !run_fold_type_ok(table_schema.columns.get(column)?.data_type) {
        return None;
    }
    let has_count_star = accumulators.iter().any(|a| a.column.is_none());
    // A column the footer predates (folded via its constant DEFAULT) has no
    // runs and no zone map. Resolve by name before the zone-map lookup so
    // the identity fallback of `mapped_column` can never index an unrelated
    // footer slot.
    reader
        .schema()
        .index_of(&table_schema.columns.get(column)?.name)?;
    let footer_column = row_filter::mapped_column(column_map, column);
    if has_count_star && zone_maps.get(footer_column)?.null_count != 0 {
        return None;
    }
    let segment_column = reader
        .schema()
        .index_of(&table_schema.columns.get(column)?.name)?;
    Some(RunFoldPlan {
        fold_column: column,
        segment_column,
        predicates: Vec::new(),
    })
}

/// Run folding compares and sums the raw i64 run values, so the column must
/// be i64-backed with an ordering the i64 view preserves (UInt64 excluded:
/// its u64 domain can wrap the i64 view). This is the reader-side view of
/// exactly the type set the writer stores footer sums for — delegates to
/// `column_sum_supported` so the two sides cannot drift apart again.
pub(super) fn run_fold_type_ok(dt: DataType) -> bool {
    crate::storage::segment::column_sum_supported(dt)
}

/// Fold one row group's run-level result into the accumulators. The shape
/// checks in [`plan_run_fold`] guarantee every accumulator reads the fold
/// column (or is COUNT(*)).
pub(super) fn fold_run_fold(
    accumulators: &mut [AggAccum],
    col_type: DataType,
    out: &RunFold,
) -> Result<()> {
    for accumulator in accumulators.iter_mut() {
        match accumulator.func {
            AggFunc::Count => accumulator.count += out.matched_rows,
            AggFunc::Min => {
                if out.any {
                    accumulator.merge_min(raw_i64_scalar(col_type, out.min));
                }
            }
            AggFunc::Max => {
                if out.any {
                    accumulator.merge_max(raw_i64_scalar(col_type, out.max));
                }
            }
            AggFunc::Sum => {
                if out.any {
                    accumulator.add_i64(out.sum_i)?;
                }
            }
            AggFunc::Avg => {
                if out.any {
                    accumulator.add_avg(out.sum_i as f64, out.matched_rows);
                }
            }
        }
    }
    Ok(())
}

/// Reinterpret a raw run value as the column's scalar type. The i64 view is
/// exact for every [`run_fold_type_ok`] type (narrow ints round-trip through
/// the wider stored value by construction).
pub(super) fn raw_i64_scalar(dt: DataType, value: i64) -> ScalarValue {
    match dt {
        DataType::Int8 => ScalarValue::Int8(Some(value as i8)),
        DataType::Int16 => ScalarValue::Int16(Some(value as i16)),
        DataType::Int32 => ScalarValue::Int32(Some(value as i32)),
        DataType::Int64 => ScalarValue::Int64(Some(value)),
        DataType::UInt8 => ScalarValue::UInt8(Some(value as u8)),
        DataType::UInt16 => ScalarValue::UInt16(Some(value as u16)),
        DataType::UInt32 => ScalarValue::UInt32(Some(value as u32)),
        DataType::Decimal => ScalarValue::Decimal(Some(value)),
        DataType::Date => ScalarValue::Date(Some(value as i32)),
        DataType::Time => ScalarValue::Time(Some(value)),
        DataType::Timestamp => ScalarValue::Timestamp(Some(value)),
        DataType::Interval => ScalarValue::Interval(Some(value)),
        _ => ScalarValue::Int64(Some(value)),
    }
}

/// Aggregate argument columns the metadata fast path can answer: SUM needs a
/// plain numeric argument (the answer is the argument's integer/float
/// representation); AVG additionally allows Decimal because its answer is
/// always sum/count as Float64, summed in the raw i64 domain exactly like
/// the regular pipeline's `col_sum_i64`. Anything else keeps the generic
/// scan.
pub(super) fn aggregate_args_supported(
    aggregates: &[AggregateExpr],
    table_schema: &Schema,
) -> bool {
    for agg in aggregates {
        if agg.distinct {
            continue;
        }
        let supported = match agg.func {
            crate::sql::planner::AggFunc::Sum => match &*agg.arg {
                crate::sql::planner::LogicalExpr::Column(idx) => table_schema
                    .columns
                    .get(*idx)
                    .map(|c| {
                        matches!(
                            c.data_type,
                            crate::protocol::types::DataType::Int64
                                | crate::protocol::types::DataType::Int32
                                | crate::protocol::types::DataType::Int16
                                | crate::protocol::types::DataType::Int8
                                | crate::protocol::types::DataType::Float64
                                | crate::protocol::types::DataType::Float32
                        )
                    })
                    .unwrap_or(false),
                _ => false,
            },
            crate::sql::planner::AggFunc::Avg => match &*agg.arg {
                crate::sql::planner::LogicalExpr::Column(idx) => table_schema
                    .columns
                    .get(*idx)
                    .map(|c| {
                        matches!(
                            c.data_type,
                            crate::protocol::types::DataType::Int64
                                | crate::protocol::types::DataType::Int32
                                | crate::protocol::types::DataType::Int16
                                | crate::protocol::types::DataType::Int8
                                | crate::protocol::types::DataType::Float64
                                | crate::protocol::types::DataType::Float32
                                | crate::protocol::types::DataType::Decimal
                        )
                    })
                    .unwrap_or(false),
                _ => false,
            },
            _ => continue,
        };
        if !supported {
            return false;
        }
    }
    true
}
