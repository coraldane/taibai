use crate::types::batch::RecordBatch;
use crate::types::scalar::ScalarValue;

/// Manages sorted-order checks for sort key columns.
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
}

/// Compare two scalar values for sorting purposes.
/// Nulls are sorted last (greater than all non-null values).
/// Delegates to `ScalarValue::compare_ordered`; NULLs-last is the ascending
/// ORDER BY / zone-map convention.
pub fn compare_scalars_for_sort(a: &ScalarValue, b: &ScalarValue) -> std::cmp::Ordering {
    a.compare_ordered(b, std::cmp::Ordering::Greater)
}

pub fn scalar_lt(a: &ScalarValue, b: &ScalarValue) -> bool {
    if a.is_null() || b.is_null() {
        return false;
    }
    compare_scalars_for_sort(a, b) == std::cmp::Ordering::Less
}

#[cfg(test)]
#[path = "sort_key_test.rs"]
mod tests;
