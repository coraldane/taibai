use crate::types::batch::RecordBatch;

pub fn apply_filter_mask_fast(batch: &RecordBatch, mask: &[bool]) -> RecordBatch {
    if mask.iter().all(|&m| m) {
        return batch.clone();
    }
    let match_count = mask.iter().filter(|&m| *m).count();
    let mut columns = Vec::with_capacity(batch.num_columns());
    for col_idx in 0..batch.num_columns() {
        if let Some(col) = batch.column(col_idx) {
            columns.push(col.filter_by_mask_fast(mask, match_count));
        }
    }
    RecordBatch::try_new(batch.schema.clone(), columns).unwrap()
}
