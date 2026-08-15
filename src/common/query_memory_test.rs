use super::*;
use crate::protocol::types::DataType;
use crate::types::array::ColumnArray;
use crate::types::batch::{ColumnDef, Schema};

#[test]
fn result_batches_are_rejected_before_exceeding_limit() {
    let schema = Schema::new(vec![ColumnDef::new("value", DataType::Int64)]);
    let batch = RecordBatch::try_new(
        schema,
        vec![ColumnArray::Int64 {
            data: vec![1, 2, 3],
            bitmap: crate::types::bitmap::NullBitmap::new_valid(3),
        }],
    )
    .unwrap();
    let batch_bytes = batch.estimated_size_bytes();
    let mut batches = Vec::new();
    let mut retained_bytes = 0;
    push_result_batch(
        &mut batches,
        &mut retained_bytes,
        batch.clone(),
        batch_bytes,
    )
    .unwrap();
    assert!(push_result_batch(&mut batches, &mut retained_bytes, batch, batch_bytes).is_err());
    assert_eq!(batches.len(), 1);
    assert_eq!(retained_bytes, batch_bytes);
}
