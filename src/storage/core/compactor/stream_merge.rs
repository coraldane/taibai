//! Merge with one pinned decoded block per input stream. Advancing a stream
//! never competes with output gathering for an LRU entry.
use super::*;
use std::cmp::Reverse;
use std::collections::BinaryHeap;

struct Head {
    keys: Vec<crate::types::scalar::ScalarValue>,
    stream: usize,
    row: usize,
}
impl PartialEq for Head {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for Head {}
impl PartialOrd for Head {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Head {
    fn cmp(&self, other: &Self) -> Ordering {
        for (left, right) in self.keys.iter().zip(&other.keys) {
            let order = compare_scalars_for_sort(left, right);
            if order != Ordering::Equal {
                return order;
            }
        }
        self.stream
            .cmp(&other.stream)
            .then(self.row.cmp(&other.row))
    }
}

impl Compactor {
    pub(super) fn merge_runs_into_writer(
        &self,
        paths: &[PathBuf],
        schema: &Schema,
        keys: &[usize],
        output_rows: usize,
        writer: &mut SegmentWriter,
    ) -> Result<usize> {
        let readers = paths
            .iter()
            .map(|p| self.segment_mgr.open_reader(p))
            .collect::<Result<Vec<_>>>()?;
        let mut groups = vec![0usize; readers.len()];
        let mut batches: Vec<Option<RecordBatch>> = (0..readers.len()).map(|_| None).collect();
        let mut heap = BinaryHeap::new();
        let budget = clustered_merge_budget_bytes();
        let mut resident = 0usize;
        let mut decoded = 0usize;
        let load = |stream: usize,
                    groups: &mut [usize],
                    batches: &mut [Option<RecordBatch>],
                    resident: &mut usize,
                    decoded: &mut usize|
         -> Result<bool> {
            if let Some(previous) = batches[stream].take() {
                *resident -= previous.estimated_size_bytes();
            }
            let reader = &readers[stream];
            while groups[stream] < reader.num_row_groups() {
                let rg = groups[stream];
                groups[stream] += 1;
                if reader.row_group_rows(rg) == 0 {
                    continue;
                }
                let batch = reader.read_row_group_columns_aligned(rg, schema)?;
                *resident = resident.saturating_add(batch.estimated_size_bytes());
                ensure_compaction_memory(*resident, budget / 2, "merge input blocks")?;
                batches[stream] = Some(batch);
                *decoded += 1;
                return Ok(true);
            }
            Ok(false)
        };
        let head = |stream: usize, row: usize, batches: &[Option<RecordBatch>]| -> Head {
            let batch = batches[stream].as_ref().unwrap();
            Head {
                keys: keys
                    .iter()
                    .map(|&k| batch.column(k).unwrap().scalar(row))
                    .collect(),
                stream,
                row,
            }
        };
        for stream in 0..readers.len() {
            if load(
                stream,
                &mut groups,
                &mut batches,
                &mut resident,
                &mut decoded,
            )? {
                heap.push(Reverse(head(stream, 0, &batches)));
            }
        }
        let mut output = RecordBatch::new(schema.clone());
        let mut output_bytes = 0usize;
        let mut written = 0usize;
        while let Some(Reverse(current)) = heap.pop() {
            let batch = batches[current.stream].as_ref().unwrap();
            for col in 0..schema.num_columns() {
                output.column_mut(col).unwrap().extend_range(
                    batch.column(col).unwrap(),
                    current.row,
                    current.row + 1,
                )?;
            }
            written += 1;
            output_bytes =
                output_bytes.saturating_add(super::runs::row_allocation_bytes(batch, current.row));
            if output.num_rows() >= output_rows.max(1) || output_bytes >= budget / 8 {
                writer.write_batch_parallel(&output)?;
                output = RecordBatch::new(schema.clone());
                output_bytes = 0;
            }
            let next = current.row + 1;
            if next < batch.num_rows() {
                heap.push(Reverse(head(current.stream, next, &batches)));
            } else if load(
                current.stream,
                &mut groups,
                &mut batches,
                &mut resident,
                &mut decoded,
            )? {
                heap.push(Reverse(head(current.stream, 0, &batches)));
            }
        }
        if !output.is_empty() {
            writer.write_batch_parallel(&output)?;
        }
        tracing::debug!(
            runs = paths.len(),
            rows = written,
            decoded_row_groups = decoded,
            "streaming compaction merge completed"
        );
        #[cfg(test)]
        MERGE_DECODED_GROUPS.with(|count| count.set(decoded));
        Ok(written)
    }
}

#[cfg(test)]
thread_local! { pub(super) static MERGE_DECODED_GROUPS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }

pub(super) fn ensure_compaction_memory(bytes: usize, budget: usize, phase: &str) -> Result<()> {
    if bytes > budget.max(1) {
        return Err(TaibaiError::ResourceBusy(format!(
            "compaction {phase} needs {bytes} bytes, exceeding its {budget} byte budget"
        )));
    }
    Ok(())
}
