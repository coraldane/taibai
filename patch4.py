# -*- coding: utf-8 -*-
import io

p = 'src/sql/execution/scan/row_filter.rs'
with io.open(p, encoding='utf-8') as f:
    s = f.read()

# Fallback helper (insert once, after imports)
anchor = "use crate::types::array::ColumnArray;"
helper = anchor + """

/// Expand dictionary columns into plain varchars before typed predicate
/// evaluation. Recursion terminates: the materialized batch has no
/// dictionary columns.
fn materialize_dictionary_columns(
    batch: &RecordBatch,
) -> crate::common::error::Result<RecordBatch> {
    if batch
        .columns
        .iter()
        .any(|c| matches!(c, ColumnArray::Dictionary { .. }))
    {
        let columns: crate::common::error::Result<Vec<ColumnArray>> = batch
            .columns
            .iter()
            .map(|c| {
                if matches!(c, ColumnArray::Dictionary { .. }) {
                    Ok(c.materialize_varchar())
                } else {
                    Ok(c.clone())
                }
            })
            .collect();
        return RecordBatch::try_new(batch.schema.clone(), columns?);
    }
    Ok(batch.clone())
}"""
assert s.count(anchor) == 1
s = s.replace(anchor, helper, 1)

# apply_predicates entry
old = """pub fn apply_predicates(
    batch: &RecordBatch,
    eq_predicates: &[(usize, ScalarValue)],
    range_predicates: &[crate::storage::partition::pruning::RangeClause],
    in_predicates: &[InPredicate],
    remaining_predicate: Option<&crate::sql::planner::LogicalExpr>,
) -> crate::common::error::Result<RecordBatch> {
    if batch.num_rows() == 0 {
        return Ok(batch.clone());
    }"""
new = """pub fn apply_predicates(
    batch: &RecordBatch,
    eq_predicates: &[(usize, ScalarValue)],
    range_predicates: &[crate::storage::partition::pruning::RangeClause],
    in_predicates: &[InPredicate],
    remaining_predicate: Option<&crate::sql::planner::LogicalExpr>,
) -> crate::common::error::Result<RecordBatch> {
    if batch.num_rows() == 0 {
        return Ok(batch.clone());
    }
    if batch
        .columns
        .iter()
        .any(|c| matches!(c, ColumnArray::Dictionary { .. }))
    {
        return apply_predicates(
            &materialize_dictionary_columns(batch)?,
            eq_predicates,
            range_predicates,
            in_predicates,
            remaining_predicate,
        );
    }"""
assert s.count(old) == 1
s = s.replace(old, new)

# apply_predicates_with_column_map entry
old = """) -> crate::common::error::Result<RecordBatch> {
    let col_pos_by_original: std::collections::HashMap<usize, usize> = source_columns"""
new = """) -> crate::common::error::Result<RecordBatch> {
    if batch
        .columns
        .iter()
        .any(|c| matches!(c, ColumnArray::Dictionary { .. }))
    {
        return apply_predicates_with_column_map(
            &materialize_dictionary_columns(batch)?,
            source_columns,
            eq_predicates,
            range_predicates,
            in_predicates,
            remaining_predicate,
        );
    }
    let col_pos_by_original: std::collections::HashMap<usize, usize> = source_columns"""
assert s.count(old) == 1
s = s.replace(old, new)

with io.open(p, 'w', encoding='utf-8', newline='') as f:
    f.write(s)
print('row_filter fallbacks added')
