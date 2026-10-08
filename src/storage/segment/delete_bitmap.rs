use crate::common::error::Result;
use crate::types::batch::RecordBatch;

pub fn is_deleted(bitmap: &[u8], row_id: u64) -> bool {
    let byte = (row_id / 8) as usize;
    let bit = (row_id % 8) as u8;
    bitmap.get(byte).is_some_and(|b| (b & (1u8 << bit)) != 0)
}

/// Number of deleted rows in `[start_row, start_row + row_count)`. Bits past
/// the end of the bitmap count as not deleted (matching [`is_deleted`]).
pub fn row_range_deleted_count(bitmap: &[u8], start_row: u64, row_count: u64) -> u64 {
    let end_row = start_row.saturating_add(row_count);
    let mut count = 0u64;
    for row in start_row..end_row {
        if is_deleted(bitmap, row) {
            count += 1;
        }
    }
    count
}

/// Word-at-a-time variant of [`is_deleted`] over a row range: the DELETE and
/// COUNT paths test every candidate row group against the bitmap, so the
/// per-row division/modulo/branch cost showed up as O(total rows) on every
/// statement. Bits past the end of the bitmap count as not deleted.
pub fn row_range_has_deleted(bitmap: &[u8], start_row: u64, row_count: u64) -> bool {
    if row_count == 0 {
        return false;
    }
    let end_row = start_row.saturating_add(row_count);
    // Head: unaligned rows to the next byte boundary.
    let mut row = start_row;
    let head_end = end_row.min((start_row / 8 + 1) * 8);
    while row < head_end {
        if is_deleted(bitmap, row) {
            return true;
        }
        row += 1;
    }
    // Middle: whole u64 words (byte-aligned after the head).
    while row + 64 <= end_row {
        let byte = (row / 8) as usize;
        let Some(word_bytes) = bitmap.get(byte..byte + 8) else {
            return false; // past the bitmap: not deleted
        };
        let mut word = [0u8; 8];
        word.copy_from_slice(word_bytes);
        if u64::from_le_bytes(word) != 0 {
            return true;
        }
        row += 64;
    }
    // Tail.
    while row < end_row {
        if is_deleted(bitmap, row) {
            return true;
        }
        row += 1;
    }
    false
}

/// Word-at-a-time variant of "every row in range is deleted". Bits past the
/// end of the bitmap count as not deleted (matching [`is_deleted`]), so a
/// short bitmap makes the range not-all-deleted.
pub fn row_range_all_deleted(bitmap: &[u8], start_row: u64, row_count: u64) -> bool {
    if row_count == 0 {
        return false;
    }
    let end_row = start_row.saturating_add(row_count);
    let mut row = start_row;
    let head_end = end_row.min((start_row / 8 + 1) * 8);
    while row < head_end {
        if !is_deleted(bitmap, row) {
            return false;
        }
        row += 1;
    }
    while row + 64 <= end_row {
        let byte = (row / 8) as usize;
        let Some(word_bytes) = bitmap.get(byte..byte + 8) else {
            return false;
        };
        let mut word = [0u8; 8];
        word.copy_from_slice(word_bytes);
        if u64::from_le_bytes(word) != u64::MAX {
            return false;
        }
        row += 64;
    }
    while row < end_row {
        if !is_deleted(bitmap, row) {
            return false;
        }
        row += 1;
    }
    true
}

pub fn mark_deleted(bitmap: &mut [u8], row_id: u64) -> bool {
    let byte = (row_id / 8) as usize;
    let bit = (row_id % 8) as u8;
    let Some(slot) = bitmap.get_mut(byte) else {
        return false;
    };
    let mask = 1u8 << bit;
    if (*slot & mask) != 0 {
        return false;
    }
    *slot |= mask;
    true
}

/// Filters out rows marked in the delete bitmap. Takes the batch by value so
/// the common "no deletions in this row group" case returns it unchanged —
/// callers rebind their owned batch, and the previous clone-on-no-deletions
/// path deep-copied every column of every candidate row group on segments
/// that carry any deletion at all.
pub fn filter_batch(
    batch: RecordBatch,
    first_row_id: u64,
    delete_bitmap: &[u8],
) -> Result<RecordBatch> {
    let num_rows = batch.num_rows();
    if !row_range_has_deleted(delete_bitmap, first_row_id, num_rows as u64) {
        return Ok(batch);
    }
    let mut keep: Vec<u32> = Vec::with_capacity(num_rows);
    for i in 0..num_rows {
        let row_id = first_row_id + i as u64;
        if !is_deleted(delete_bitmap, row_id) {
            keep.push(i as u32);
        }
    }

    if keep.len() == num_rows {
        return Ok(batch);
    }

    // One bulk gather per column (memcpy runs for fixed-width types) instead
    // of a scalar() + push() round-trip per surviving cell.
    let columns = (0..batch.num_columns())
        .map(|c| batch.column(c).expect("column exists").take(&keep))
        .collect();
    RecordBatch::try_new(batch.schema.clone(), columns)
}

#[cfg(test)]
#[path = "delete_bitmap_test.rs"]
mod tests;
