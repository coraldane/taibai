use crate::common::error::Result;
use crate::types::batch::RecordBatch;

pub fn is_deleted(bitmap: &[u8], row_id: u64) -> bool {
    let byte = (row_id / 8) as usize;
    let bit = (row_id % 8) as u8;
    bitmap.get(byte).map_or(false, |b| (b & (1u8 << bit)) != 0)
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
    let mut keep = Vec::with_capacity(num_rows);
    for i in 0..num_rows {
        let row_id = first_row_id + i as u64;
        if !is_deleted(delete_bitmap, row_id) {
            keep.push(i);
        }
    }

    if keep.len() == num_rows {
        return Ok(batch.clone());
    }

    let mut columns = Vec::with_capacity(batch.num_columns());
    for c in 0..batch.num_columns() {
        let src = batch.column(c).unwrap();
        let mut new_col = crate::types::array::ColumnArray::empty(src.data_type());
        for &i in &keep {
            new_col.push(&src.scalar(i))?;
        }
        columns.push(new_col);
    }
    RecordBatch::try_new(batch.schema.clone(), columns)
}
