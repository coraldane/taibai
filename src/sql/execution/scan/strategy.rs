use crate::common::error::Result;
use crate::storage::catalog::schema::TableMeta;
use crate::storage::segment::SegmentReader;
use crate::types::array::ColumnArray;
use crate::types::batch::{RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

use super::row_filter::{self, ClusterKeyBounds};
use super::row_group::{self, RowGroupPruneResult};
use super::spec::ScanSpec;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(clippy::enum_variant_names)]
pub enum RowGroupReadStrategy {
    PlainRead,
    PredicateFirstRead,
    ClusterKeyRangeRead,
}

#[derive(Clone)]
pub struct ScanPlan {
    pub predicate_cols: Vec<usize>,
    pub projection_cols: Vec<usize>,
    pub strip_ttl_column: bool,
    pub cluster_key_bounds: Option<ClusterKeyBounds>,
    pub strategy: RowGroupReadStrategy,
    pub scan_limit: Option<usize>,
    pub ordered_limit: Option<OrderedClusterLimit>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderedClusterLimit {
    pub order_column_index: usize,
    pub desc: bool,
}

pub struct ScanPlanner<'a> {
    spec: &'a ScanSpec,
    table_schema: &'a Schema,
    table_meta: &'a TableMeta,
}

impl<'a> ScanPlanner<'a> {
    pub fn new(spec: &'a ScanSpec, table_schema: &'a Schema, table_meta: &'a TableMeta) -> Self {
        Self {
            spec,
            table_schema,
            table_meta,
        }
    }

    pub fn build(&self) -> ScanPlan {
        let predicate_cols = self.spec.all_predicate_columns();
        let mut projection_cols = match &self.spec.projection {
            Some(p) => p.clone(),
            None => (0..self.table_schema.num_columns()).collect(),
        };
        let strip_ttl_column = self.extend_projection_for_ttl(&mut projection_cols);

        let cluster_key_bounds = self.cluster_key_bounds();
        let ordered_limit = self.ordered_cluster_order();
        let strategy = self.select_strategy(&predicate_cols, &projection_cols, &cluster_key_bounds);

        ScanPlan {
            predicate_cols,
            projection_cols,
            strip_ttl_column,
            cluster_key_bounds,
            strategy,
            scan_limit: self.effective_scan_limit(),
            ordered_limit,
        }
    }

    fn select_strategy(
        &self,
        predicate_cols: &[usize],
        projection_cols: &[usize],
        cluster_key_bounds: &Option<ClusterKeyBounds>,
    ) -> RowGroupReadStrategy {
        if cluster_key_bounds.is_some() {
            return RowGroupReadStrategy::ClusterKeyRangeRead;
        }
        if self.should_use_predicate_first()
            && !predicate_cols.is_empty()
            && !projection_cols.is_empty()
        {
            return RowGroupReadStrategy::PredicateFirstRead;
        }
        RowGroupReadStrategy::PlainRead
    }

    fn effective_scan_limit(&self) -> Option<usize> {
        super::spec::pushdown_safe_limit(self.spec, self.ordered_cluster_order().is_some())
            .map(|limit| limit.get())
    }

    fn ordered_cluster_order(&self) -> Option<OrderedClusterLimit> {
        if !self.spec.has_order_by
            || self.spec.has_aggregate
            || self.spec.has_group_by
            || self.spec.has_join
            || self.spec.has_distinct_or_window
            || self.spec.remaining_predicate.is_some()
            || self.spec.order_by.is_empty()
        {
            return None;
        }

        let cluster_key = self.spec.cluster_key.as_ref()?;
        let first = self.spec.order_by.first()?;
        let first_name = self
            .table_schema
            .columns
            .get(first.column_index)?
            .name
            .as_str();
        let first_pos = cluster_key
            .iter()
            .position(|key| key.eq_ignore_ascii_case(first_name))?;

        // Order keys before the matched cluster position must be pinned by
        // equality predicates, otherwise the surviving rows are not globally
        // ordered by the ORDER BY suffix.
        for key in cluster_key.iter().take(first_pos) {
            let key_idx = self.table_schema.index_of(key)?;
            self.spec.eq_value_for_column(key_idx)?;
        }

        // The ORDER BY list must occupy consecutive cluster-key positions,
        // all in the same direction; segment/RG reversal then yields a
        // globally ordered stream whichever direction it is.
        for (offset, order) in self.spec.order_by.iter().enumerate() {
            let name = self
                .table_schema
                .columns
                .get(order.column_index)?
                .name
                .as_str();
            let pos = cluster_key
                .iter()
                .position(|key| key.eq_ignore_ascii_case(name))?;
            if pos != first_pos + offset {
                return None;
            }
            if order.desc != first.desc {
                return None;
            }
        }
        if first_pos + self.spec.order_by.len() > cluster_key.len() {
            return None;
        }

        Some(OrderedClusterLimit {
            order_column_index: first.column_index,
            desc: first.desc,
        })
    }

    fn should_use_predicate_first(&self) -> bool {
        if !self.spec.has_predicates() {
            return false;
        }
        if self.spec.projection.is_none() {
            return false;
        }
        if self.spec.all_predicate_columns().is_empty() {
            return false;
        }
        true
    }

    fn extend_projection_for_ttl(&self, projection_cols: &mut Vec<usize>) -> bool {
        let Some(ttl_idx) = self.table_meta.get_ttl_column_index_cached() else {
            return false;
        };
        if projection_cols.contains(&ttl_idx) {
            return false;
        }
        projection_cols.push(ttl_idx);
        self.spec.projection.is_some()
    }

    /// Walk the cluster key in order: equality-pinned columns extend the
    /// prefix; the first column with range predicates becomes the range
    /// column. A prefix without any range still yields bounds (equality run
    /// on the prefix), so `symbol = ?` on `CLUSTER BY (symbol, ts)` keeps
    /// using the sorted-key binary search.
    fn cluster_key_bounds(&self) -> Option<ClusterKeyBounds> {
        let cluster_key = self.spec.cluster_key.as_ref()?;
        let mut prefix: Vec<(usize, ScalarValue)> = Vec::new();

        for key_col in cluster_key {
            let col_idx = self.table_schema.index_of(key_col)?;

            let eq_value = self.spec.eq_value_for_column(col_idx).cloned().or_else(|| {
                self.spec
                    .range_predicates
                    .iter()
                    .find(|clause| {
                        clause.column_index == col_idx
                            && clause.op == crate::storage::partition::pruning::RangeOp::Eq
                    })
                    .map(|clause| clause.value.clone())
            });
            if let Some(value) = eq_value {
                prefix.push((col_idx, value));
                continue;
            }

            let mut lower: Option<(ScalarValue, bool)> = None;
            let mut upper: Option<(ScalarValue, bool)> = None;
            for clause in &self.spec.range_predicates {
                if clause.column_index != col_idx {
                    continue;
                }
                match clause.op {
                    crate::storage::partition::pruning::RangeOp::Eq => {}
                    crate::storage::partition::pruning::RangeOp::Gt => {
                        set_tighter_lower(&mut lower, clause.value.clone(), false);
                    }
                    crate::storage::partition::pruning::RangeOp::Ge => {
                        set_tighter_lower(&mut lower, clause.value.clone(), true);
                    }
                    crate::storage::partition::pruning::RangeOp::Lt => {
                        set_tighter_upper(&mut upper, clause.value.clone(), false);
                    }
                    crate::storage::partition::pruning::RangeOp::Le => {
                        set_tighter_upper(&mut upper, clause.value.clone(), true);
                    }
                }
            }

            if lower.is_some() || upper.is_some() {
                return Some(ClusterKeyBounds {
                    prefix_eq: prefix,
                    range_column_index: col_idx,
                    lower,
                    upper,
                });
            }
            break;
        }

        let (last_col, last_value) = prefix.last().cloned()?;
        Some(ClusterKeyBounds {
            prefix_eq: prefix[..prefix.len() - 1].to_vec(),
            range_column_index: last_col,
            lower: Some((last_value.clone(), true)),
            upper: Some((last_value, true)),
        })
    }
}

pub fn prune_segment(spec: &ScanSpec, reader: &SegmentReader) -> Option<RowGroupPruneResult> {
    let pred = spec.to_row_group_predicate();
    let segment_zone_maps = reader.segment_zone_maps();
    // Re-index the catalog-space predicate and check lists onto this
    // segment's footer column order — after a DROP/RENAME COLUMN, positional
    // use prunes the WRONG row groups.
    let Ok(aligned) = super::row_filter::align_prune_checks_to_footer(
        &spec.table_schema,
        reader.schema(),
        &pred,
        &row_group::build_bloom_check_list(spec),
        &row_group::build_in_check_list(spec),
        &row_group::build_set_check_list(spec),
        &row_group::build_set_in_check_list(spec),
    ) else {
        // A referenced column is absent from this older segment: no row in it
        // can match the clause.
        return None;
    };
    if !row_group::prune_segment_by_zone_map(&aligned.predicate, segment_zone_maps) {
        return None;
    }

    let in_check_refs: Vec<&super::spec::InPredicate> = aligned.in_checks.iter().collect();
    let set_in_check_refs: Vec<&super::spec::InPredicate> = aligned.set_in_checks.iter().collect();

    Some(row_group::prune_row_groups(
        reader,
        &aligned.predicate,
        &aligned.bloom_checks,
        &in_check_refs,
        &aligned.set_checks,
        &set_in_check_refs,
        segment_zone_maps,
    ))
}

/// For unordered LIMIT queries, visit row groups with the highest estimated
/// match density first. This is a small cost-based decision backed entirely by
/// persisted row-group statistics, so it adds no write-path work.
pub fn rank_row_groups_for_limit(
    spec: &ScanSpec,
    reader: &SegmentReader,
    candidates: &mut [usize],
) {
    candidates.sort_by(|left, right| {
        let left_score = estimated_match_density(spec, reader, *left);
        let right_score = estimated_match_density(spec, reader, *right);
        right_score
            .partial_cmp(&left_score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.cmp(right))
    });
}

fn estimated_match_density(spec: &ScanSpec, reader: &SegmentReader, row_group: usize) -> f64 {
    let mut selectivity = 1.0f64;
    let value_sets = reader.value_sets(row_group);
    let zone_maps = reader.zone_maps(row_group);
    // Statistics lookups follow the segment's footer order; the spec's
    // predicate columns are catalog-space. Misalignment only skews this
    // cost-based ordering (never correctness), so an unresolvable column
    // just falls back to the generic 0.1 estimate.
    let footer =
        |column: usize| -> Option<usize> {
            super::row_filter::align_indices_to_footer(
                &spec.table_schema,
                reader.schema(),
                &[column],
            )
            .ok()
            .and_then(|aligned| aligned.first().copied())
        };
    for (column, value) in &spec.eq_predicates {
        let Some(footer_column) = footer(*column) else {
            selectivity *= 0.1;
            continue;
        };
        if let Some(values) = value_sets
            .get(footer_column)
            .and_then(|values| values.as_ref())
        {
            selectivity *= 1.0 / values.len().max(1) as f64;
            continue;
        }
        if let Some(zone) = zone_maps.get(footer_column) {
            if zone.min_value == *value && zone.max_value == *value {
                continue;
            }
        }
        selectivity *= 0.1;
    }
    for predicate in &spec.in_predicates {
        let Some(footer_column) = footer(predicate.column_index) else {
            selectivity *= (predicate.values.len() as f64 * 0.1).min(1.0);
            continue;
        };
        if let Some(values) = value_sets
            .get(footer_column)
            .and_then(|values| values.as_ref())
        {
            selectivity *=
                (predicate.values.len() as f64 / values.len().max(1) as f64).clamp(0.0, 1.0);
        } else {
            selectivity *= (predicate.values.len() as f64 * 0.1).min(1.0);
        }
    }
    for predicate in &spec.range_predicates {
        let footer_column = match footer(predicate.column_index) {
            Some(column) => column,
            None => {
                selectivity *= 0.33;
                continue;
            }
        };
        let Some(zone) = zone_maps.get(footer_column) else {
            selectivity *= 0.33;
            continue;
        };
        let (Some(min), Some(max), Some(value)) = (
            zone.min_value.numeric_f64(),
            zone.max_value.numeric_f64(),
            predicate.value.numeric_f64(),
        ) else {
            selectivity *= 0.33;
            continue;
        };
        let fraction = if max <= min {
            1.0
        } else {
            ((value - min) / (max - min)).clamp(0.0, 1.0)
        };
        selectivity *= match predicate.op {
            crate::storage::partition::pruning::RangeOp::Eq => 0.1,
            crate::storage::partition::pruning::RangeOp::Lt
            | crate::storage::partition::pruning::RangeOp::Le => fraction,
            crate::storage::partition::pruning::RangeOp::Gt
            | crate::storage::partition::pruning::RangeOp::Ge => 1.0 - fraction,
        };
    }
    selectivity.clamp(0.0, 1.0)
}

pub fn read_row_group_with_strategy(
    spec: &ScanSpec,
    plan: &ScanPlan,
    table_schema: &Schema,
    reader: &SegmentReader,
    rg_idx: usize,
) -> Result<RecordBatch> {
    if reader_schema_differs(reader.schema(), table_schema) {
        return read_plain_row_group(spec, &plan.projection_cols, table_schema, reader, rg_idx);
    }
    match plan.strategy {
        RowGroupReadStrategy::ClusterKeyRangeRead => {
            let bounds = plan
                .cluster_key_bounds
                .as_ref()
                .expect("cluster key bounds present for cluster key strategy");
            row_filter::cluster_key_range_read(
                reader,
                table_schema,
                rg_idx,
                bounds,
                &plan.projection_cols,
                &plan.predicate_cols,
                &spec.eq_predicates,
                &spec.range_predicates,
                &spec.in_predicates,
                spec.remaining_predicate.as_ref(),
            )
        }
        RowGroupReadStrategy::PredicateFirstRead => row_filter::predicate_first_read(
            reader,
            table_schema,
            rg_idx,
            &plan.predicate_cols,
            &plan.projection_cols,
            &spec.eq_predicates,
            &spec.range_predicates,
            &spec.in_predicates,
            spec.remaining_predicate.as_ref(),
        ),
        RowGroupReadStrategy::PlainRead => {
            read_plain_row_group(spec, &plan.projection_cols, table_schema, reader, rg_idx)
        }
    }
}

pub fn read_plain_row_group(
    spec: &ScanSpec,
    projection_cols: &[usize],
    table_schema: &Schema,
    reader: &SegmentReader,
    rg_idx: usize,
) -> Result<RecordBatch> {
    if spec.has_predicates() && !projection_cols.is_empty() {
        return read_predicated_plain_row_group(
            spec,
            projection_cols,
            table_schema,
            reader,
            rg_idx,
        );
    }

    // Dictionary passthrough: when nothing downstream needs per-row strings,
    // dictionary-encoded columns stay as (ids, dict) so the grouped
    // aggregate's fast path consumes u32 ids directly. Predicates materialize
    // dictionary columns before typed evaluation, and delete-bitmap gathers
    // need plain columns, so both cases keep the expanded form. Run
    // passthrough rides the same gate for Rle/DeltaRle columns: the
    // run-aware aggregate consumes runs directly, everything else flattens
    // lazily.
    let passthrough_safe = !spec.has_predicates() && reader.delete_bitmap().is_empty();
    let allow_dictionary = passthrough_safe;
    let allow_runs = passthrough_safe;
    let mut batch = read_projected_row_group_passthrough(
        projection_cols,
        table_schema,
        reader,
        rg_idx,
        allow_dictionary,
        allow_runs,
    )?;

    if !reader.delete_bitmap().is_empty() {
        // O(1) prefix sum; the old clone-the-whole-footer sum was quadratic
        // in the row-group count for bitmap segments.
        let rg_start_row: u64 = reader.row_group_start_row(rg_idx);
        batch = crate::storage::segment::delete_bitmap::filter_batch(
            batch,
            rg_start_row,
            reader.delete_bitmap(),
        )?;
    }

    if batch.num_rows() == 0 || !spec.has_predicates() {
        return Ok(batch);
    }

    // Projection covers every column (empty projection reads the full row
    // group above), so the full-table predicate indices line up as-is.
    row_filter::apply_predicates(
        &batch,
        &spec.eq_predicates,
        &spec.range_predicates,
        &spec.in_predicates,
        spec.remaining_predicate.as_ref(),
    )
}

/// Predicated plain read. Predicates carry full-table column indices, but the
/// caller's projection may not include the predicate columns at all (e.g. a
/// GROUP BY scan narrowed to its group keys). On such a batch an equality on
/// a filtered-out column would zero every row and a range on one would
/// silently pass — so read the union of projection and predicate columns,
/// evaluate with remapped indices, and project back down.
fn read_predicated_plain_row_group(
    spec: &ScanSpec,
    projection_cols: &[usize],
    table_schema: &Schema,
    reader: &SegmentReader,
    rg_idx: usize,
) -> Result<RecordBatch> {
    let mut union_cols = projection_cols.to_vec();
    for idx in spec.all_predicate_columns() {
        if !union_cols.contains(&idx) {
            union_cols.push(idx);
        }
    }
    union_cols.sort_unstable();

    if union_cols.len() == projection_cols.len() {
        let mut batch =
            read_row_group_with_schema_mapping(projection_cols, table_schema, reader, rg_idx)?;
        batch = apply_delete_bitmap(reader, rg_idx, batch)?;
        if batch.num_rows() == 0 {
            return Ok(batch);
        }
        return row_filter::apply_predicates_with_column_map(
            &batch,
            projection_cols,
            &spec.eq_predicates,
            &spec.range_predicates,
            &spec.in_predicates,
            spec.remaining_predicate.as_ref(),
        );
    }

    let mut batch = read_row_group_with_schema_mapping(&union_cols, table_schema, reader, rg_idx)?;
    batch = apply_delete_bitmap(reader, rg_idx, batch)?;
    if batch.num_rows() == 0 {
        return Ok(batch);
    }
    let filtered = row_filter::apply_predicates_with_column_map(
        &batch,
        &union_cols,
        &spec.eq_predicates,
        &spec.range_predicates,
        &spec.in_predicates,
        spec.remaining_predicate.as_ref(),
    )?;
    project_union_columns(filtered, &union_cols, projection_cols)
}

fn apply_delete_bitmap(
    reader: &SegmentReader,
    rg_idx: usize,
    batch: RecordBatch,
) -> Result<RecordBatch> {
    if reader.delete_bitmap().is_empty() {
        return Ok(batch);
    }
    // O(1) prefix sum instead of cloning the whole footer per row group.
    let rg_start_row: u64 = reader.row_group_start_row(rg_idx);
    crate::storage::segment::delete_bitmap::filter_batch(
        batch,
        rg_start_row,
        reader.delete_bitmap(),
    )
}

/// Keep only `projection_cols` from a batch read over `union_cols`, in
/// projection order.
fn project_union_columns(
    batch: RecordBatch,
    union_cols: &[usize],
    projection_cols: &[usize],
) -> Result<RecordBatch> {
    let mut columns = Vec::with_capacity(projection_cols.len());
    let mut col_defs = Vec::with_capacity(projection_cols.len());
    for &idx in projection_cols {
        let pos = union_cols.iter().position(|&u| u == idx).ok_or_else(|| {
            crate::common::error::TaibaiError::Internal(format!(
                "projected column {idx} missing from predicate batch"
            ))
        })?;
        columns.push(batch.columns[pos].clone());
        col_defs.push(batch.schema.columns[pos].clone());
    }
    RecordBatch::try_new(Schema::new(col_defs), columns)
        .map_err(|e| crate::common::error::TaibaiError::Internal(e.to_string()))
}

pub fn read_projected_row_group(
    projection_cols: &[usize],
    table_schema: &Schema,
    reader: &SegmentReader,
    rg_idx: usize,
) -> Result<RecordBatch> {
    read_projected_row_group_allow_dictionary(projection_cols, table_schema, reader, rg_idx, false)
}

pub fn read_projected_row_group_allow_dictionary(
    projection_cols: &[usize],
    table_schema: &Schema,
    reader: &SegmentReader,
    rg_idx: usize,
    allow_dictionary: bool,
) -> Result<RecordBatch> {
    read_projected_row_group_passthrough(
        projection_cols,
        table_schema,
        reader,
        rg_idx,
        allow_dictionary,
        false,
    )
}

/// `allow_runs` keeps Rle/DeltaRle columns in run form: safe when no
/// predicate must evaluate against them and no delete gather needs plain
/// values — the same contract as dictionary passthrough.
pub fn read_projected_row_group_passthrough(
    projection_cols: &[usize],
    table_schema: &Schema,
    reader: &SegmentReader,
    rg_idx: usize,
    allow_dictionary: bool,
    allow_runs: bool,
) -> Result<RecordBatch> {
    if projection_cols.is_empty() {
        if reader_schema_differs(reader.schema(), table_schema) {
            let all_cols: Vec<usize> = (0..table_schema.num_columns()).collect();
            return read_row_group_with_schema_mapping(&all_cols, table_schema, reader, rg_idx);
        }
        return reader.read_row_group(rg_idx);
    }

    if !reader_schema_differs(reader.schema(), table_schema) {
        return reader.read_row_group_with_projection_passthrough(
            rg_idx,
            projection_cols,
            allow_dictionary,
            allow_runs,
        );
    }

    read_row_group_with_schema_mapping(projection_cols, table_schema, reader, rg_idx)
}

fn read_row_group_with_schema_mapping(
    projection_cols: &[usize],
    table_schema: &Schema,
    reader: &SegmentReader,
    rg_idx: usize,
) -> Result<RecordBatch> {
    let row_count = reader.row_group_rows(rg_idx) as usize;
    let mut segment_projection = Vec::new();
    let mut segment_positions = Vec::with_capacity(projection_cols.len());
    for &table_col_idx in projection_cols {
        let table_col = &table_schema.columns[table_col_idx];
        let pos = reader
            .schema()
            .index_of_column_id(table_col.column_id)
            .or_else(|| reader.schema().index_of(&table_col.name));
        if let Some(segment_col_idx) = pos {
            segment_positions.push(Some(segment_projection.len()));
            segment_projection.push(segment_col_idx);
        } else {
            segment_positions.push(None);
        }
    }

    let segment_batch = if segment_projection.is_empty() {
        None
    } else {
        Some(reader.read_row_group_with_projection(rg_idx, &segment_projection)?)
    };

    let mut columns = Vec::with_capacity(projection_cols.len());
    let mut col_defs = Vec::with_capacity(projection_cols.len());
    for (out_idx, &table_col_idx) in projection_cols.iter().enumerate() {
        let col_def = &table_schema.columns[table_col_idx];
        if let Some(segment_pos) = segment_positions[out_idx] {
            columns.push(segment_batch.as_ref().unwrap().columns[segment_pos].clone());
        } else {
            columns.push(default_column_for_missing_schema_col(col_def, row_count)?);
        }
        col_defs.push(col_def.clone());
    }

    RecordBatch::try_new(Schema::new(col_defs), columns)
}

pub(super) fn reader_schema_differs(reader_schema: &Schema, table_schema: &Schema) -> bool {
    if reader_schema.num_columns() != table_schema.num_columns() {
        return true;
    }
    reader_schema
        .columns
        .iter()
        .zip(table_schema.columns.iter())
        .any(|(left, right)| {
            !left.name.eq_ignore_ascii_case(&right.name)
                || left.data_type != right.data_type
                || left.scale != right.scale
                || left.precision != right.precision
        })
}

fn default_column_for_missing_schema_col(
    col_def: &crate::types::batch::ColumnDef,
    row_count: usize,
) -> Result<ColumnArray> {
    let value = match &col_def.default_value {
        Some(crate::types::batch::ColumnDefaultValue::Literal(value)) => value.clone(),
        Some(crate::types::batch::ColumnDefaultValue::CurrentTimestamp) => {
            // Rows that predate an ALTER ADD COLUMN read the value pinned at
            // ALTER time (col.added_at_micros); reading "now" made repeated
            // queries return different values for the same row.
            ScalarValue::Timestamp(Some(col_def.added_at_micros.unwrap_or_else(now_micros)))
        }
        None => ScalarValue::null_for_type(col_def.data_type),
    };
    // Same constant-column construction the segment-side aligned read uses
    // (Rle single-run build, flattened before return): keeps the SELECT-side
    // fill byte-identical to `default_value_column`'s output while dropping
    // the per-row push loop.
    crate::storage::segment::reader::constant_column(col_def.data_type, &value, row_count)
}

pub fn apply_ttl_filter_and_projection(
    batch: RecordBatch,
    table_meta: &TableMeta,
    projection_cols: &[usize],
    strip_ttl_column: bool,
) -> Result<RecordBatch> {
    let Some(ttl) = table_meta.ttl.as_ref() else {
        return Ok(batch);
    };
    let Some(ttl_idx) = table_meta.get_ttl_column_index_cached() else {
        return Ok(batch);
    };
    let Some(ttl_pos) = projection_cols.iter().position(|idx| *idx == ttl_idx) else {
        return Ok(batch);
    };
    if batch.num_rows() == 0 || ttl_pos >= batch.num_columns() {
        return Ok(batch);
    }

    let now = now_micros();
    let ttl_micros = ttl.duration_secs.saturating_mul(1_000_000);
    let ttl_col = batch.column(ttl_pos).unwrap();
    // Typed walk of the raw timestamp column: the generic `scalar()` path
    // allocated one boxed ScalarValue per row on EVERY batch of EVERY scan
    // of a TTL table.
    let mask: Vec<bool> = match ttl_col {
        crate::types::array::ColumnArray::Timestamp { data, bitmap } => (0..data.len())
            .map(|row_idx| {
                !bitmap.is_null(row_idx) && data[row_idx].saturating_add(ttl_micros) > now
            })
            .collect(),
        other => (0..batch.num_rows())
            .map(|row_idx| match other.scalar(row_idx) {
                ScalarValue::Timestamp(Some(ts)) => ts.saturating_add(ttl_micros) > now,
                _ => false,
            })
            .collect(),
    };

    let match_count = mask.iter().filter(|keep| **keep).count();
    let filtered = if match_count == batch.num_rows() {
        batch
    } else if match_count == 0 {
        RecordBatch::new(batch.schema.clone())
    } else {
        let columns = batch
            .columns
            .iter()
            .map(|col| col.filter_by_mask_fast(&mask, match_count))
            .collect();
        RecordBatch::try_new(batch.schema.clone(), columns)?
    };

    if strip_ttl_column {
        strip_projection_column(filtered, ttl_pos)
    } else {
        Ok(filtered)
    }
}

fn strip_projection_column(batch: RecordBatch, column_pos: usize) -> Result<RecordBatch> {
    if column_pos >= batch.num_columns() {
        return Ok(batch);
    }
    let schema = Schema::new(
        batch
            .schema
            .columns
            .iter()
            .enumerate()
            .filter_map(|(idx, col)| (idx != column_pos).then_some(col.clone()))
            .collect(),
    );
    let columns = batch
        .columns
        .into_iter()
        .enumerate()
        .filter_map(|(idx, col)| (idx != column_pos).then_some(col))
        .collect();
    RecordBatch::try_new(schema, columns)
}

fn now_micros() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as i64
}

fn set_tighter_lower(
    current: &mut Option<(crate::types::scalar::ScalarValue, bool)>,
    candidate: crate::types::scalar::ScalarValue,
    inclusive: bool,
) {
    let replace = match current {
        Some((value, current_inclusive)) => {
            match crate::storage::index::sort_key::compare_scalars_for_sort(&candidate, value) {
                std::cmp::Ordering::Greater => true,
                std::cmp::Ordering::Equal => !inclusive && *current_inclusive,
                std::cmp::Ordering::Less => false,
            }
        }
        None => true,
    };
    if replace {
        *current = Some((candidate, inclusive));
    }
}

fn set_tighter_upper(
    current: &mut Option<(crate::types::scalar::ScalarValue, bool)>,
    candidate: crate::types::scalar::ScalarValue,
    inclusive: bool,
) {
    let replace = match current {
        Some((value, current_inclusive)) => {
            match crate::storage::index::sort_key::compare_scalars_for_sort(&candidate, value) {
                std::cmp::Ordering::Less => true,
                std::cmp::Ordering::Equal => !inclusive && *current_inclusive,
                std::cmp::Ordering::Greater => false,
            }
        }
        None => true,
    };
    if replace {
        *current = Some((candidate, inclusive));
    }
}

#[cfg(test)]
#[path = "strategy_test.rs"]
mod tests;
