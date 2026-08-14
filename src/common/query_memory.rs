use crate::common::error::{Result, TaibaiError};
use crate::types::batch::RecordBatch;

pub fn result_memory_limit_bytes() -> usize {
    let limits = crate::common::config::query_limits();
    let configured = limits.query_memory_limit_mb.saturating_mul(1024 * 1024);
    let query_budget = usize::try_from(configured).unwrap_or(usize::MAX).max(1) / 2;
    let message_budget = limits.max_result_message_bytes.max(1) / 2;
    query_budget.min(message_budget).min(u32::MAX as usize / 2)
}

pub fn push_result_batch(
    batches: &mut Vec<RecordBatch>,
    retained_bytes: &mut usize,
    batch: RecordBatch,
    limit_bytes: usize,
) -> Result<()> {
    let next_bytes = retained_bytes.saturating_add(batch.estimated_size_bytes());
    if next_bytes > limit_bytes {
        return Err(TaibaiError::ResourceBusy(format!(
            "query result exceeded memory limit of {} MB; add filters or LIMIT",
            limit_bytes / (1024 * 1024)
        )));
    }
    *retained_bytes = next_bytes;
    batches.push(batch);
    Ok(())
}

#[cfg(test)]
mod tests {
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
}
