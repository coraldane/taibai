use crate::common::error::Result;
use crate::types::batch::RecordBatch;

pub fn is_deleted(bitmap: &[u8], row_id: u64) -> bool {
    let byte = (row_id / 8) as usize;
    let bit = (row_id % 8) as u8;
    bitmap.get(byte).is_some_and(|b| (b & (1u8 << bit)) != 0)
}

pub fn row_range_all_deleted(bitmap: &[u8], start_row: u64, row_count: u64) -> bool {
    (0..row_count).all(|i| is_deleted(bitmap, start_row + i))
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

pub fn filter_batch(
    batch: &RecordBatch,
    first_row_id: u64,
    delete_bitmap: &[u8],
) -> Result<RecordBatch> {
    let num_rows = batch.num_rows();
    let mut keep: Vec<u32> = Vec::with_capacity(num_rows);
    for i in 0..num_rows {
        let row_id = first_row_id + i as u64;
        if !is_deleted(delete_bitmap, row_id) {
            keep.push(i as u32);
        }
    }

    if keep.len() == num_rows {
        return Ok(batch.clone());
    }

    // One bulk gather per column (memcpy runs for fixed-width types) instead
    // of a scalar() + push() round-trip per surviving cell: this runs for
    // every candidate row group of every segment that carries a delete
    // bitmap, so the per-cell rebuild dominated post-DELETE scans.
    let columns = (0..batch.num_columns())
        .map(|c| batch.column(c).expect("column exists").take(&keep))
        .collect();
    RecordBatch::try_new(batch.schema.clone(), columns)
}
