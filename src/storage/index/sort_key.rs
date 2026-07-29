#![allow(dead_code)]
use crate::common::error::Result;
use crate::types::batch::RecordBatch;
use crate::types::scalar::ScalarValue;

/// Manages sorted insertion into a RecordBatch based on sort key columns.
///
/// When a table has a SORT KEY, rows must be inserted in sorted order.
/// This module provides:
/// - Binary search insertion position finding
/// - Sorted batch merging
/// - Min/max index for range pruning
pub struct SortKeyInserter {
    /// Column indices that form the sort key (in priority order).
    pub key_indices: Vec<usize>,
    /// Sort direction for each key column (true = descending).
    pub desc: Vec<bool>,
}

impl SortKeyInserter {
    /// Create a new inserter for the given key column indices.
    pub fn new(key_indices: Vec<usize>, desc: Vec<bool>) -> Self {
        Self { key_indices, desc }
    }

    /// Find the insertion position for a row with the given key values
    /// using binary search. Returns the index where the row should be inserted.
    pub fn find_insert_position(
        &self,
        batch: &RecordBatch,
        key_values: &[ScalarValue],
    ) -> Result<usize> {
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return Ok(0);
        }

        let mut lo = 0usize;
        let mut hi = num_rows;

        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let cmp = self.compare_row_to_key(batch, mid, key_values);
            match cmp {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => lo = mid + 1, // Insert after equal elements (stable)
            }
        }

        Ok(lo)
    }

    /// Insert a row into the batch at the correct sorted position.
    /// Returns a new batch with the row inserted.
    pub fn insert_sorted(
        &self,
        batch: &RecordBatch,
        values: &[ScalarValue],
    ) -> Result<RecordBatch> {
        let key_values: Vec<ScalarValue> = self
            .key_indices
            .iter()
            .map(|&idx| values[idx].clone())
            .collect();
        let pos = self.find_insert_position(batch, &key_values)?;

        let num_cols = batch.num_columns();
        let num_rows = batch.num_rows();
        let mut new_columns = Vec::with_capacity(num_cols);

        for (col_idx, col) in batch.columns.iter().enumerate() {
            let mut new_col = crate::types::array::ColumnArray::empty(col.data_type());
            for i in 0..pos {
                new_col.push(&col.scalar(i))?;
            }
            new_col.push(&values[col_idx])?;
            for i in pos..num_rows {
                new_col.push(&col.scalar(i))?;
            }
            new_columns.push(new_col);
        }

        RecordBatch::try_new(batch.schema.clone(), new_columns)
    }

    /// Compare a row in the batch to the given key values using the sort key columns.
    fn compare_row_to_key(
        &self,
        batch: &RecordBatch,
        row_idx: usize,
        key_values: &[ScalarValue],
    ) -> std::cmp::Ordering {
        for (ki, &col_idx) in self.key_indices.iter().enumerate() {
            let row_val = batch
                .scalar(row_idx, col_idx)
                .unwrap_or(ScalarValue::Int64(None));
            let key_val = &key_values[ki];
            let cmp = compare_scalars_for_sort(&row_val, key_val);
            let cmp = if self.desc.get(ki).copied().unwrap_or(false) {
                cmp.reverse()
            } else {
                cmp
            };
            if cmp != std::cmp::Ordering::Equal {
                return cmp;
            }
        }
        std::cmp::Ordering::Equal
    }

    /// Check if the batch is sorted according to the sort key.
    pub fn is_sorted(&self, batch: &RecordBatch) -> bool {
        if batch.num_rows() <= 1 {
            return true;
        }

        for i in 1..batch.num_rows() {
            for (ki, &col_idx) in self.key_indices.iter().enumerate() {
                let Some(column) = batch.column(col_idx) else {
                    return false;
                };
                let cmp = column.compare_at(i - 1, i);
                let cmp = if self.desc.get(ki).copied().unwrap_or(false) {
                    cmp.reverse()
                } else {
                    cmp
                };
                match cmp {
                    std::cmp::Ordering::Less => break, // This column is fine, check next row
                    std::cmp::Ordering::Greater => return false,
                    std::cmp::Ordering::Equal => continue, // Check next key column
                }
            }
        }
        true
    }

    /// Extract min and max values for the sort key columns from a batch.
    /// Returns Vec<(Option<ScalarValue>, Option<ScalarValue>)> per key column: (min, max).
    pub fn extract_min_max(
        &self,
        batch: &RecordBatch,
    ) -> Vec<(Option<ScalarValue>, Option<ScalarValue>)> {
        let num_rows = batch.num_rows();
        self.key_indices
            .iter()
            .map(|&col_idx| {
                if num_rows == 0 {
                    return (None, None);
                }
                let mut min_val = batch.scalar(0, col_idx);
                let mut max_val = min_val.clone();
                for i in 1..num_rows {
                    if let Some(val) = batch.scalar(i, col_idx) {
                        if val.is_null() {
                            continue;
                        }
                        let new_min = match &min_val {
                            Some(m) if !m.is_null() => {
                                compare_scalars_for_sort(&val, m) == std::cmp::Ordering::Less
                            }
                            _ => true,
                        };
                        if new_min {
                            min_val = Some(val.clone());
                        }
                        let new_max = match &max_val {
                            Some(m) if !m.is_null() => {
                                compare_scalars_for_sort(&val, m) == std::cmp::Ordering::Greater
                            }
                            _ => true,
                        };
                        if new_max {
                            max_val = Some(val);
                        }
                    }
                }
                (min_val, max_val)
            })
            .collect()
    }

    /// Check if a range predicate on the sort key can possibly match any rows in this batch.
    /// Given lower/upper bounds on the first sort key column, returns true if overlap is possible.
    pub fn range_overlaps(
        &self,
        batch_min_max: &[(Option<ScalarValue>, Option<ScalarValue>)],
        lower_bound: Option<&ScalarValue>,
        upper_bound: Option<&ScalarValue>,
    ) -> bool {
        if batch_min_max.is_empty() {
            return true;
        }
        let (seg_min, seg_max) = &batch_min_max[0];

        // No overlap if segment max < query lower bound
        if let (Some(ref smax), Some(lb)) = (seg_max, lower_bound) {
            if compare_scalars_for_sort(smax, lb) == std::cmp::Ordering::Less {
                return false;
            }
        }

        // No overlap if segment min > query upper bound
        if let (Some(ref smin), Some(ub)) = (seg_min, upper_bound) {
            if compare_scalars_for_sort(smin, ub) == std::cmp::Ordering::Greater {
                return false;
            }
        }

        true
    }
}

/// Compare two scalar values for sorting purposes.
/// Nulls are sorted last (greater than all non-null values).
pub fn compare_scalars_for_sort(a: &ScalarValue, b: &ScalarValue) -> std::cmp::Ordering {
    if a.is_null() && b.is_null() {
        return std::cmp::Ordering::Equal;
    }
    if a.is_null() {
        return std::cmp::Ordering::Greater; // Nulls last
    }
    if b.is_null() {
        return std::cmp::Ordering::Less;
    }

    match (a, b) {
        _ if a.numeric_cmp(b).is_some() => a.numeric_cmp(b).unwrap_or(std::cmp::Ordering::Equal),
        (ScalarValue::Int64(Some(l)), ScalarValue::Int64(Some(r))) => l.cmp(r),
        (ScalarValue::Int32(Some(l)), ScalarValue::Int32(Some(r))) => l.cmp(r),
        (ScalarValue::Int16(Some(l)), ScalarValue::Int16(Some(r))) => l.cmp(r),
        (ScalarValue::Int8(Some(l)), ScalarValue::Int8(Some(r))) => l.cmp(r),
        (ScalarValue::UInt64(Some(l)), ScalarValue::UInt64(Some(r))) => l.cmp(r),
        (ScalarValue::UInt32(Some(l)), ScalarValue::UInt32(Some(r))) => l.cmp(r),
        (ScalarValue::UInt16(Some(l)), ScalarValue::UInt16(Some(r))) => l.cmp(r),
        (ScalarValue::UInt8(Some(l)), ScalarValue::UInt8(Some(r))) => l.cmp(r),
        (ScalarValue::Float64(Some(l)), ScalarValue::Float64(Some(r))) => {
            l.partial_cmp(r).unwrap_or(std::cmp::Ordering::Equal)
        }
        (ScalarValue::Float32(Some(l)), ScalarValue::Float32(Some(r))) => {
            l.partial_cmp(r).unwrap_or(std::cmp::Ordering::Equal)
        }
        (ScalarValue::VarChar(Some(l)), ScalarValue::VarChar(Some(r))) => l.cmp(r),
        (ScalarValue::Bool(Some(l)), ScalarValue::Bool(Some(r))) => l.cmp(r),
        // Cross-type: promote to i64
        (ScalarValue::Int64(Some(l)), ScalarValue::Int32(Some(r))) => l.cmp(&(*r as i64)),
        (ScalarValue::Int32(Some(l)), ScalarValue::Int64(Some(r))) => (*l as i64).cmp(r),
        // Timestamp as i64 comparison
        (ScalarValue::Timestamp(Some(l)), ScalarValue::Timestamp(Some(r))) => l.cmp(r),
        (ScalarValue::Date(Some(l)), ScalarValue::Date(Some(r))) => l.cmp(r),
        _ => std::cmp::Ordering::Equal,
    }
}

pub fn scalar_lt(a: &ScalarValue, b: &ScalarValue) -> bool {
    if a.is_null() || b.is_null() {
        return false;
    }
    compare_scalars_for_sort(a, b) == std::cmp::Ordering::Less
}

pub fn scalar_le(a: &ScalarValue, b: &ScalarValue) -> bool {
    if a.is_null() || b.is_null() {
        return false;
    }
    matches!(
        compare_scalars_for_sort(a, b),
        std::cmp::Ordering::Less | std::cmp::Ordering::Equal
    )
}
/// A segment index entry that stores min/max values for the sort key columns.
/// Used for range pruning during query execution.
#[derive(Debug, Clone)]
pub struct SegmentIndexEntry {
    /// Segment file path.
    pub path: std::path::PathBuf,
    /// Row count in the segment.
    pub row_count: usize,
    /// Per sort-key column: (min, max).
    pub min_max: Vec<(Option<ScalarValue>, Option<ScalarValue>)>,
}

/// A segment index for a table, supporting range pruning.
#[derive(Debug, Clone, Default)]
pub struct SegmentIndex {
    pub entries: Vec<SegmentIndexEntry>,
}

impl SegmentIndex {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Add a segment to the index.
    pub fn add_entry(
        &mut self,
        path: std::path::PathBuf,
        row_count: usize,
        min_max: Vec<(Option<ScalarValue>, Option<ScalarValue>)>,
    ) {
        self.entries.push(SegmentIndexEntry {
            path,
            row_count,
            min_max,
        });
    }

    /// Prune segments that cannot match the given range predicate.
    /// Returns paths of segments that may contain matching data.
    pub fn prune_ranges(
        &self,
        inserter: &SortKeyInserter,
        lower_bound: Option<&ScalarValue>,
        upper_bound: Option<&ScalarValue>,
    ) -> Vec<std::path::PathBuf> {
        self.entries
            .iter()
            .filter(|entry| inserter.range_overlaps(&entry.min_max, lower_bound, upper_bound))
            .map(|entry| entry.path.clone())
            .collect()
    }
}

#[cfg(test)]
#[path = "sort_key_tests.rs"]
mod tests;
