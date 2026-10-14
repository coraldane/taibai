use crate::common::error::{Result, TaibaiError};
use crate::types::batch::RecordBatch;

struct ResultBudget {
    limit_bytes: usize,
    query_budget_bytes: usize,
    message_budget_bytes: usize,
    max_result_message_bytes: usize,
    query_memory_limit_mb: u64,
}

fn result_budget() -> ResultBudget {
    let limits = crate::common::config::query_limits();
    let configured = limits.query_memory_limit_mb.saturating_mul(1024 * 1024);
    let query_budget_bytes = usize::try_from(configured).unwrap_or(usize::MAX).max(1) / 2;
    let message_budget_bytes = limits.max_result_message_bytes.max(1) / 2;
    ResultBudget {
        limit_bytes: query_budget_bytes
            .min(message_budget_bytes)
            .min(u32::MAX as usize / 2),
        query_budget_bytes,
        message_budget_bytes,
        max_result_message_bytes: limits.max_result_message_bytes,
        query_memory_limit_mb: limits.query_memory_limit_mb,
    }
}

pub fn result_memory_limit_bytes() -> usize {
    result_budget().limit_bytes
}

/// Deterministic overrun error for the result budget. The message must name
/// the configuration that actually binds, so operators adjust the right key
/// instead of discovering the `max_message_size` coupling the hard way. The
/// phrase "exceeded memory limit" is load-bearing: `Handler::build_query_error`
/// matches it to classify the error as deterministic (no `[RETRYABLE]`).
pub(crate) fn result_limit_error(limit_bytes: usize) -> TaibaiError {
    let budget = result_budget();
    let bounded_by = if budget.message_budget_bytes < budget.query_budget_bytes {
        format!(
            "bounded by server.max_message_size = {} MB (a result must fit in a single wire message, and every client enforces its own receive cap)",
            budget.max_result_message_bytes / (1024 * 1024)
        )
    } else {
        format!(
            "bounded by buffer.query_memory_limit_mb = {} MB",
            budget.query_memory_limit_mb
        )
    };
    TaibaiError::ResourceBusy(format!(
        "query result exceeded memory limit of {} MB ({}); add filters or LIMIT",
        limit_bytes / (1024 * 1024),
        bounded_by
    ))
}

pub fn push_result_batch(
    batches: &mut Vec<RecordBatch>,
    retained_bytes: &mut usize,
    batch: RecordBatch,
    limit_bytes: usize,
) -> Result<()> {
    let next_bytes = retained_bytes.saturating_add(batch.estimated_size_bytes());
    if next_bytes > limit_bytes {
        return Err(result_limit_error(limit_bytes));
    }
    *retained_bytes = next_bytes;
    batches.push(batch);
    Ok(())
}

#[cfg(test)]
#[path = "query_memory_test.rs"]
mod tests;
