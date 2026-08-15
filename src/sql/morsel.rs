use std::sync::Mutex;

use crate::common::error::Result;
use crate::types::batch::RecordBatch;

/// Default morsel size (number of rows per morsel/task).
pub const DEFAULT_MORSEL_SIZE: usize = 65536;

/// A Morsel is a small batch of rows that a single task processes.
/// Morsel-driven parallelism splits work into small units that can be
/// distributed across threads, enabling work stealing and load balancing.
#[derive(Debug)]
pub struct Morsel {
    /// The batch data for this morsel.
    pub batch: RecordBatch,
    /// Source partition/segment index (for affinity).
    pub source_idx: usize,
}

impl Morsel {
    pub fn new(batch: RecordBatch, source_idx: usize) -> Self {
        Self { batch, source_idx }
    }

    pub fn num_rows(&self) -> usize {
        self.batch.num_rows()
    }

    pub fn is_empty(&self) -> bool {
        self.batch.num_rows() == 0
    }
}

/// Split a vector of record batches into morsels of approximately `morsel_size` rows.
/// Each input batch is split into chunks of `morsel_size` rows.
pub fn split_into_morsels(batches: Vec<RecordBatch>, morsel_size: usize) -> Vec<Morsel> {
    let mut morsels = Vec::new();

    for (batch_idx, batch) in batches.into_iter().enumerate() {
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            continue;
        }

        if num_rows <= morsel_size {
            morsels.push(Morsel::new(batch, batch_idx));
            continue;
        }

        // Split into chunks
        let mut start = 0;
        while start < num_rows {
            let end = (start + morsel_size).min(num_rows);
            let chunk = trim_batch_range(&batch, start, end);
            morsels.push(Morsel::new(chunk, batch_idx));
            start = end;
        }
    }

    morsels
}

/// A shared work queue of morsels that supports work stealing.
pub struct MorselQueue {
    morsels: Mutex<Vec<Morsel>>,
}

impl MorselQueue {
    pub fn new(morsels: Vec<Morsel>) -> Self {
        Self {
            morsels: Mutex::new(morsels),
        }
    }

    /// Try to steal a morsel from the queue.
    /// Returns None if the queue is empty.
    pub fn steal(&self) -> Option<Morsel> {
        let mut morsels = self.morsels.lock().unwrap();
        morsels.pop()
    }

    /// Returns the number of remaining morsels.
    pub fn len(&self) -> usize {
        self.morsels.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.morsels.lock().unwrap().is_empty()
    }
}

/// Result accumulator for parallel morsel processing.
pub struct MorselResult {
    results: Mutex<Vec<RecordBatch>>,
}

impl MorselResult {
    pub fn new() -> Self {
        Self {
            results: Mutex::new(Vec::new()),
        }
    }

    /// Push a processed batch result.
    pub fn push(&self, batch: RecordBatch) {
        if batch.num_rows() > 0 {
            self.results.lock().unwrap().push(batch);
        }
    }

    /// Drain all results and merge into a single batch.
    pub fn drain(self) -> Option<RecordBatch> {
        let results = self.results.into_inner().unwrap();
        if results.is_empty() {
            return None;
        }
        if results.len() == 1 {
            return Some(results.into_iter().next().unwrap());
        }

        // Merge all batches
        let schema = results[0].schema.clone();
        let num_cols = schema.columns.len();
        let mut columns = Vec::with_capacity(num_cols);

        for col_idx in 0..num_cols {
            let dt = schema.columns[col_idx].data_type;
            let mut col = crate::types::array::ColumnArray::empty(dt);
            for batch in &results {
                if let Some(src) = batch.column(col_idx) {
                    for i in 0..batch.num_rows() {
                        col.push(&src.scalar(i)).unwrap();
                    }
                }
            }
            columns.push(col);
        }

        Some(RecordBatch::try_new(schema, columns).unwrap())
    }
}

/// Configuration for morsel-driven parallel execution.
#[derive(Debug, Clone)]
pub struct MorselConfig {
    /// Target number of rows per morsel.
    pub morsel_size: usize,
    /// Number of worker threads (0 = use all available cores).
    pub num_workers: usize,
}

impl Default for MorselConfig {
    fn default() -> Self {
        Self {
            morsel_size: DEFAULT_MORSEL_SIZE,
            num_workers: 0,
        }
    }
}

impl MorselConfig {
    pub fn with_morsel_size(mut self, size: usize) -> Self {
        self.morsel_size = size;
        self
    }

    pub fn with_workers(mut self, n: usize) -> Self {
        self.num_workers = n;
        self
    }

    pub fn effective_workers(&self) -> usize {
        if self.num_workers == 0 {
            num_cpus()
        } else {
            self.num_workers
        }
    }
}

fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
}

/// Sequential morsel execution (for correctness testing before parallel).
/// Processes morsels one at a time using the provided closure.
pub fn execute_morsels_sequential<F>(
    morsels: Vec<Morsel>,
    mut process_fn: F,
) -> Result<Vec<RecordBatch>>
where
    F: FnMut(Morsel) -> Result<Option<RecordBatch>>,
{
    let mut results = Vec::new();
    for morsel in morsels {
        if let Some(batch) = process_fn(morsel)? {
            if batch.num_rows() > 0 {
                results.push(batch);
            }
        }
    }
    Ok(results)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn trim_batch_range(batch: &RecordBatch, start: usize, end: usize) -> RecordBatch {
    let mut columns = Vec::with_capacity(batch.num_columns());
    for col_idx in 0..batch.num_columns() {
        let col = batch.column(col_idx).unwrap();
        let mut new_col = crate::types::array::ColumnArray::empty(col.data_type());
        for i in start..end {
            new_col.push(&col.scalar(i)).unwrap();
        }
        columns.push(new_col);
    }
    RecordBatch::try_new(batch.schema.clone(), columns).unwrap()
}

#[cfg(test)]
#[path = "morsel_test.rs"]
mod tests;
