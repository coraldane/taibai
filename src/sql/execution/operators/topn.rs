use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::sync::Arc;

use crate::common::error::Result;
use crate::sql::execution::operators::filter::{eval_expr_column, remap_expr_for_batch};
use crate::sql::execution::operators::sort::compare_scalars_null_last;
use crate::sql::planner::{LogicalExpr, SortKey};
use crate::types::array::ColumnArray;
use crate::types::batch::{RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

use super::profile;
use super::ExecutionOp;

const CHUNK_ROWS: usize = 4096;

pub struct TopNExec {
    sort_keys: Vec<SortKey>,
    input: Box<dyn ExecutionOp>,
    schema: Schema,
    capacity: usize,
    state: TopNState,
}

enum TopNState {
    Ready,
    Final(Option<RecordBatch>),
    Done,
}

impl TopNExec {
    pub fn new(sort_keys: Vec<SortKey>, input: Box<dyn ExecutionOp>, schema: Schema) -> Self {
        Self {
            sort_keys,
            input,
            schema,
            capacity: usize::MAX,
            state: TopNState::Ready,
        }
    }

    pub fn with_capacity(mut self, capacity: usize) -> Self {
        self.capacity = capacity;
        self
    }

    fn consume(&mut self) -> Result<Option<RecordBatch>> {
        if self.capacity == 0 {
            return Ok(None);
        }
        let mut acc = Accumulator::new(self.capacity, &self.sort_keys, self.schema.clone());
        while let Some(batch) = self.input.next_batch()? {
            crate::common::query_cancel::check()?;
            acc.push_batch(&batch)?;
        }
        acc.finish()
    }
}

impl ExecutionOp for TopNExec {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if matches!(self.state, TopNState::Ready) {
            let output = self.consume()?;
            self.state = match output {
                Some(batch) => TopNState::Final(Some(batch)),
                None => TopNState::Done,
            };
        }
        match &mut self.state {
            TopNState::Final(batch) => {
                let result = batch.take();
                self.state = TopNState::Done;
                Ok(result)
            }
            TopNState::Done => Ok(None),
            TopNState::Ready => unreachable!("topn ready state must be consumed above"),
        }
    }
}

struct Accumulator {
    capacity: usize,
    sort_keys: Vec<SortKey>,
    initial_schema: Schema,
    schema: Option<Schema>,
    sort_exprs: Option<Vec<(LogicalExpr, bool)>>,
    descending: Arc<Vec<bool>>,
    chunks: Vec<RecordBatch>,
    slots: usize,
    live: Vec<bool>,
    dead: usize,
    heap: BinaryHeap<HeapEntry>,
}

struct HeapEntry {
    keys: Vec<ScalarValue>,
    slot: usize,
    descending: Arc<Vec<bool>>,
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        for (index, descending) in self.descending.iter().enumerate() {
            let ordering = compare_scalars_null_last(&self.keys[index], &other.keys[index]);
            let ordering = if *descending {
                ordering.reverse()
            } else {
                ordering
            };
            if !ordering.is_eq() {
                return ordering;
            }
        }
        self.slot.cmp(&other.slot)
    }
}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for HeapEntry {}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Accumulator {
    fn new(capacity: usize, sort_keys: &[SortKey], schema: Schema) -> Self {
        let descending = Arc::new(sort_keys.iter().map(|key| key.desc).collect::<Vec<_>>());
        Self {
            capacity,
            sort_keys: sort_keys.to_vec(),
            initial_schema: schema.clone(),
            schema: Some(schema),
            sort_exprs: None,
            descending,
            chunks: Vec::new(),
            slots: 0,
            live: Vec::new(),
            dead: 0,
            heap: BinaryHeap::with_capacity(capacity.min(1024)),
        }
    }

    fn push_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        if self.sort_exprs.is_none() {
            let physical = batch.schema.clone();
            let keys = &self.sort_keys;
            let logical = &self.initial_schema;
            self.sort_exprs = Some(
                keys.iter()
                    .map(|key| {
                        (
                            remap_expr_for_batch(&key.expr, logical, &physical),
                            key.desc,
                        )
                    })
                    .collect::<Vec<_>>(),
            );
        }
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let sort_exprs = self.sort_exprs.as_ref().expect("sort exprs initialized");
        let key_columns = sort_exprs
            .iter()
            .map(|(expr, _)| eval_expr_column(expr, batch))
            .collect::<Result<Vec<_>>>()?;
        for row in 0..batch.num_rows() {
            let keys = key_columns
                .iter()
                .map(|column| column[row].clone())
                .collect::<Vec<_>>();
            let entry = HeapEntry {
                keys,
                slot: self.slots,
                descending: Arc::clone(&self.descending),
            };
            let accept = if self.heap.len() < self.capacity {
                true
            } else {
                let worst = self.heap.peek().expect("full heap has a worst entry");
                entry.cmp(worst) == Ordering::Less
            };
            if accept {
                if self.heap.len() >= self.capacity {
                    let worst = self.heap.pop().expect("full heap pops the worst entry");
                    self.live[worst.slot] = false;
                    self.dead += 1;
                }
                self.append_row(batch, row)?;
                self.heap.push(entry);
                if self.dead >= self.capacity.max(64) {
                    self.compact()?;
                }
            }
        }
        Ok(())
    }

    fn append_row(&mut self, batch: &RecordBatch, row: usize) -> Result<()> {
        let need_new_chunk = match self.chunks.last() {
            Some(chunk) => chunk.num_rows() >= CHUNK_ROWS,
            None => true,
        };
        if need_new_chunk {
            let schema = self.schema.clone().expect("schema initialized");
            let hint = CHUNK_ROWS.min(self.capacity.max(8));
            let columns = schema
                .columns
                .iter()
                .map(|column| ColumnArray::with_capacity(column.data_type, hint))
                .collect::<Vec<_>>();
            self.chunks.push(RecordBatch::try_new(schema, columns)?);
        }
        let chunk = self.chunks.last_mut().expect("chunk appended above");
        for (index, column) in batch.columns.iter().enumerate() {
            let value = column.scalar(row);
            chunk
                .column_mut(index)
                .expect("column present")
                .push(&value)?;
        }
        self.live.push(true);
        self.slots += 1;
        Ok(())
    }

    fn compact(&mut self) -> Result<()> {
        let live_slots = self
            .live
            .iter()
            .enumerate()
            .filter(|(_, alive)| **alive)
            .map(|(slot, _)| slot)
            .collect::<Vec<_>>();
        let remap = live_slots
            .iter()
            .enumerate()
            .map(|(new, old)| (*old, new))
            .collect::<HashMap<usize, usize>>();
        let schema = self.schema.clone().expect("schema initialized");
        let mut columns = schema
            .columns
            .iter()
            .map(|column| ColumnArray::with_capacity(column.data_type, live_slots.len()))
            .collect::<Vec<_>>();
        let mut base = 0usize;
        for chunk in &self.chunks {
            let end = base + chunk.num_rows();
            let local = live_slots
                .iter()
                .filter(|slot| **slot >= base && **slot < end)
                .map(|slot| (*slot - base) as u32)
                .collect::<Vec<_>>();
            if !local.is_empty() {
                for (index, column) in chunk.columns.iter().enumerate() {
                    columns[index].extend(&column.take(&local))?;
                }
            }
            base = end;
        }
        let merged = RecordBatch::try_new(schema, columns)?;
        profile::record_current_peak_memory(merged.estimated_size_bytes());
        self.chunks = vec![merged];
        let old = std::mem::take(&mut self.heap);
        self.heap = old
            .into_iter()
            .map(|mut entry| {
                entry.slot = remap[&entry.slot];
                entry
            })
            .collect();
        self.live = vec![true; live_slots.len()];
        self.slots = live_slots.len();
        self.dead = 0;
        Ok(())
    }

    fn finish(mut self) -> Result<Option<RecordBatch>> {
        if self.heap.is_empty() {
            return Ok(None);
        }
        self.compact()?;
        let mut entries = self.heap.drain().collect::<Vec<_>>();
        entries.sort();
        let rows = entries
            .iter()
            .map(|entry| entry.slot as u32)
            .collect::<Vec<_>>();
        let single = &self.chunks[0];
        let output = single
            .columns
            .iter()
            .map(|column| column.take(&rows))
            .collect::<Vec<_>>();
        let schema = self.schema.expect("schema initialized");
        profile::record_current_peak_memory(output.iter().map(|c| c.estimated_size_bytes()).sum());
        Ok(Some(RecordBatch::try_new(schema, output)?))
    }
}

#[cfg(test)]
#[path = "topn_test.rs"]
mod tests;
