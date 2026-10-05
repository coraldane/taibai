//! Group identities, batch folding and result assembly.

use super::*;

/// Fold a hot-store / memtable batch (catalog-schema-aligned, columns
/// resolved by name) into the group table. Returns `None` when a GROUP BY
/// key column is missing, so the caller declines the whole query.
pub(super) fn fold_named_batch(
    groups: &mut GroupFold,
    key_columns: &[usize],
    aggregate_columns: &[Option<usize>],
    table_schema: &Schema,
    batch: &RecordBatch,
) -> Result<Option<()>> {
    let aggregate_positions: Vec<Option<usize>> = aggregate_columns
        .iter()
        .map(|column| {
            column.and_then(|index| {
                table_schema
                    .columns
                    .get(index)
                    .and_then(|column| batch.schema.index_of(&column.name))
            })
        })
        .collect();
    let key_positions = key_columns
        .iter()
        .map(|key| {
            table_schema
                .columns
                .get(*key)
                .and_then(|column| batch.schema.index_of(&column.name))
        })
        .collect::<Option<Vec<_>>>();
    // Batches aligned to the catalog schema always carry the key columns;
    // declining keeps the engine honest if that invariant ever breaks.
    let Some(key_positions) = key_positions else {
        return Ok(None);
    };
    fold_grouped_data_batch(groups, &key_positions, &aggregate_positions, batch)?;
    Ok(Some(()))
}

/// Assemble the final result batch: key columns under their source names,
/// then one column per aggregate — the same layout `grouped_output_schema`
/// gives the HashAggregate pipeline, so the Project above maps both
/// identically.
pub(super) fn finalize_folded(
    groups: GroupFold,
    key_columns: &[usize],
    aggregates: &[AggregateExpr],
    table_schema: &Schema,
) -> Result<RecordBatch> {
    let mut columns: Vec<crate::types::batch::ColumnDef> =
        Vec::with_capacity(key_columns.len() + aggregates.len());
    for &key in key_columns {
        let def = &table_schema.columns[key];
        columns.push(crate::types::batch::ColumnDef::new(
            def.name.as_str(),
            def.data_type,
        ));
    }
    for aggregate in aggregates {
        columns.push(aggregate_result_column_def(aggregate, table_schema));
    }
    let schema = Schema::new(columns);
    let mut batch = RecordBatch::new(schema);
    for (key, accumulators) in groups.into_rows() {
        for (index, value) in key.into_iter().enumerate() {
            batch
                .column_mut(index)
                .ok_or_else(|| TaibaiError::Internal("group key col".into()))?
                .push(&value)?;
        }
        for (offset, accumulator) in accumulators.into_iter().enumerate() {
            let index = key_columns.len() + offset;
            batch
                .column_mut(index)
                .ok_or_else(|| TaibaiError::Internal("agg col".into()))?
                .push(&accumulator.finalize())?;
        }
    }
    Ok(batch)
}

/// Columns the partial-sum cache needs: (column name, is-float) per SUM
/// aggregate, in aggregate order.
pub(super) fn sum_needed_columns(
    aggregates: &[AggregateExpr],
    aggregate_columns: &[Option<usize>],
    table_schema: &Schema,
) -> Vec<(String, bool)> {
    aggregates
        .iter()
        .zip(aggregate_columns)
        .filter(|(agg, _)| agg.func == crate::sql::planner::AggFunc::Sum)
        .map(|(_agg, col)| {
            let def = col.and_then(|i| table_schema.columns.get(i));
            let name = def.map(|c| c.name.clone()).unwrap_or_default();
            let is_float = def
                .map(|c| matches!(c.data_type, DataType::Float32 | DataType::Float64))
                .unwrap_or(false);
            (name, is_float)
        })
        .collect()
}

pub(super) fn aggregate_source_columns(
    spec: &ScanSpec,
    aggregate_columns: &[Option<usize>],
) -> Vec<usize> {
    let mut columns = spec.all_predicate_columns();
    for column in aggregate_columns.iter().flatten() {
        if !columns.contains(column) {
            columns.push(*column);
        }
    }
    columns
}

pub(super) fn fold_metadata(
    accumulators: &mut [AggAccum],
    column_map: &std::collections::HashMap<usize, usize>,
    defaults: &std::collections::HashMap<usize, ScalarValue>,
    zone_maps: &[crate::storage::partition::pruning::ZoneMap],
    row_count: u64,
    col_sums: &[i64],
) -> Result<()> {
    for accumulator in accumulators {
        let default = accumulator.column.and_then(|column| defaults.get(&column));
        accumulator.fold_metadata(default, zone_maps, row_count, col_sums, column_map)?;
    }
    Ok(())
}

/// One accumulator set per distinct group key, in first-seen order over the
/// deterministic (segment → row group → row) unit traversal — the same order
/// the streaming aggregate pipeline would discover groups in. The no-GROUP-BY
/// shape is a single pre-created group with the empty key, so the folded
/// single-row behaviour falls out of the same machinery.
pub(super) struct GroupFold {
    pub(super) keys: Vec<Vec<ScalarValue>>,
    pub(super) index: std::collections::HashMap<Vec<ScalarValue>, usize>,
    pub(super) rows: Vec<Vec<AggAccum>>,
    pub(super) grouped: bool,
    /// Prototype accumulator set cloned for every new group (shape queries
    /// like `has_sum` checks also read it).
    pub(super) template: Vec<AggAccum>,
}

impl GroupFold {
    pub(super) fn new(template: Vec<AggAccum>, grouped: bool) -> Self {
        let mut this = Self {
            keys: Vec::new(),
            index: std::collections::HashMap::new(),
            rows: Vec::new(),
            grouped,
            template,
        };
        if !grouped {
            this.keys.push(Vec::new());
            this.rows.push(this.template.clone());
        }
        this
    }

    /// The accumulator set for `key`, creating the group on first sight.
    /// The no-GROUP-BY shape ignores the key (there is exactly one group).
    pub(super) fn group(&mut self, key: Vec<ScalarValue>) -> &mut Vec<AggAccum> {
        if !self.grouped {
            return &mut self.rows[0];
        }
        let index = match self.index.get(&key) {
            Some(&index) => index,
            None => {
                let index = self.keys.len();
                self.keys.push(key.clone());
                self.rows.push(self.template.clone());
                self.index.insert(key, index);
                index
            }
        };
        &mut self.rows[index]
    }

    /// The single accumulator set (no-GROUP-BY shape).
    pub(super) fn single_mut(&mut self) -> &mut Vec<AggAccum> {
        debug_assert!(!self.grouped);
        &mut self.rows[0]
    }

    pub(super) fn single(&self) -> &Vec<AggAccum> {
        debug_assert!(!self.grouped);
        &self.rows[0]
    }

    pub(super) fn into_rows(self) -> impl Iterator<Item = (Vec<ScalarValue>, Vec<AggAccum>)> {
        self.keys.into_iter().zip(self.rows)
    }
}

/// What a fold unit's zone maps say about one GROUP BY key column.
pub(super) enum KeyZone {
    /// Every row of the unit carries this exact value (all-NULL included).
    Single(ScalarValue),
    /// Rows split across values, or the statistics are missing: the unit
    /// must fold from data so each row lands in its own group.
    Split,
}

pub(super) fn key_column_zone_value(
    key: usize,
    null_value: &ScalarValue,
    defaults: &std::collections::HashMap<usize, ScalarValue>,
    column_map: &std::collections::HashMap<usize, usize>,
    zone_maps: &[ZoneMap],
) -> KeyZone {
    // A key column the footer predates carries one constant DEFAULT on
    // every row: the unit's key is that constant.
    if let Some(default) = defaults.get(&key) {
        return KeyZone::Single(default.clone());
    }
    let Some(zone_map) = zone_maps.get(row_filter::mapped_column(column_map, key)) else {
        return KeyZone::Split;
    };
    if !zone_map.is_computed() {
        return KeyZone::Split;
    }
    if zone_map.min_value.is_null() && zone_map.max_value.is_null() {
        // `min_value` is only ever set from a non-null value, so both ends
        // NULL means the unit holds no non-null rows: one NULL group.
        return KeyZone::Single(null_value.clone());
    }
    if !zone_map.min_value.is_null()
        && !zone_map.max_value.is_null()
        && zone_map.min_value == zone_map.max_value
        && zone_map.null_count == 0
    {
        return KeyZone::Single(zone_map.min_value.clone());
    }
    // Mixed values, or value rows plus NULL rows: only a pure COUNT(*) could
    // split those arithmetically — keep it conservative and fold from data.
    KeyZone::Split
}

/// The single group key a whole fold unit collapses into, or `None` when any
/// key column splits across values (the unit must then fold from data). The
/// no-GROUP-BY shape always returns the empty key.
pub(super) fn unit_group_key(
    key_columns: &[usize],
    key_null_values: &[ScalarValue],
    defaults: &std::collections::HashMap<usize, ScalarValue>,
    column_map: &std::collections::HashMap<usize, usize>,
    zone_maps: &[ZoneMap],
) -> Option<Vec<ScalarValue>> {
    let mut key = Vec::with_capacity(key_columns.len());
    for (position, &column) in key_columns.iter().enumerate() {
        match key_column_zone_value(
            column,
            &key_null_values[position],
            defaults,
            column_map,
            zone_maps,
        ) {
            KeyZone::Single(value) => key.push(value),
            KeyZone::Split => return None,
        }
    }
    Some(key)
}

/// The single group key a decoded batch collapses into when every key column
/// is batch-singular (one value, or all NULL) — the common clustered case,
/// letting the whole batch fold vectorized into one group.
pub(super) fn batch_group_key(
    batch: &RecordBatch,
    key_positions: &[usize],
) -> Option<Vec<ScalarValue>> {
    let rows = batch.num_rows();
    if rows == 0 {
        return None;
    }
    let mut key = Vec::with_capacity(key_positions.len());
    for &position in key_positions {
        let values = batch.column(position)?;
        let data_type = batch.schema.columns.get(position)?.data_type;
        let non_null = values.col_count_non_null();
        if non_null == 0 {
            key.push(ScalarValue::null_for_type(data_type));
            continue;
        }
        if non_null != rows as u64 {
            return None;
        }
        let (Some(min), Some(max)) = (values.col_min(), values.col_max()) else {
            return None;
        };
        if min != max {
            return None;
        }
        key.push(min);
    }
    Some(key)
}

/// Fold one decoded fold-unit batch into the group table. Batch-singular key
/// sets fold vectorized into a single group; split keys fall to a per-row
/// loop that extracts each row's key and folds its argument values.
pub(super) fn fold_grouped_data_batch(
    groups: &mut GroupFold,
    key_positions: &[usize],
    aggregate_positions: &[Option<usize>],
    batch: &RecordBatch,
) -> Result<()> {
    if batch.num_rows() == 0 {
        return Ok(());
    }
    if !groups.grouped {
        return fold_batch(groups.single_mut(), aggregate_positions, batch);
    }
    if let Some(key) = batch_group_key(batch, key_positions) {
        return fold_batch(groups.group(key), aggregate_positions, batch);
    }
    let rows = batch.num_rows();
    for row in 0..rows {
        let key: Vec<ScalarValue> = key_positions
            .iter()
            .map(|&position| {
                batch
                    .column(position)
                    .expect("key position checked")
                    .scalar(row)
            })
            .collect();
        let accumulators = groups.group(key);
        for (accumulator, position) in accumulators.iter_mut().zip(aggregate_positions) {
            match position {
                None => accumulator.fold_row(None)?,
                Some(column) => accumulator.fold_row(Some(
                    &batch
                        .column(*column)
                        .expect("aggregate position checked")
                        .scalar(row),
                ))?,
            }
        }
    }
    Ok(())
}
