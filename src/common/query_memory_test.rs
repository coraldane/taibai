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

#[test]
fn result_limit_error_names_the_binding_configuration() {
    let err = result_limit_error(32 * 1024 * 1024);
    let TaibaiError::ResourceBusy(text) = &err else {
        panic!("expected ResourceBusy, got {err:?}")
    };
    assert!(text.contains("exceeded memory limit of 32 MB"), "{text}");
    // Default limits: the wire cap (32 MB) binds before the query budget
    // (2048 MB), so the message must name server.max_message_size — the
    // key that actually constrains results — not the query budget.
    assert!(text.contains("server.max_message_size = 64 MB"), "{text}");
}
