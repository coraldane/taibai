use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Arc;

use crate::common::error::Result;
use crate::sql::execution::operators::filter::{eval_expr_column, remap_expr_for_batch};
use crate::sql::execution::spill::{MemoryManager, SpillFile};
use crate::sql::planner::{LogicalExpr, SortKey};
use crate::storage::segment::SegmentReader;
use crate::types::array::ColumnArray;
use crate::types::batch::{RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

use super::ExecutionOp;

pub struct SortExec {
    pub sort_keys: Vec<SortKey>,
    pub input: Box<dyn ExecutionOp>,
    pub schema: Schema,
    pub limit: Option<usize>,
    state: SortState,
    memory_limit_bytes: usize,
}

enum SortState {
    Ready,
    InMemory(Option<RecordBatch>),
    External(ExternalMerge),
    Done,
}

impl SortExec {
    pub fn new(sort_keys: Vec<SortKey>, input: Box<dyn ExecutionOp>, schema: Schema) -> Self {
        let memory_limit_bytes = crate::common::config::query_limits()
            .query_memory_limit_mb
            .saturating_mul(1024 * 1024)
            .min(usize::MAX as u64) as usize;
        Self {
            sort_keys,
            input,
            schema,
            limit: None,
            state: SortState::Ready,
            memory_limit_bytes: memory_limit_bytes.max(1),
        }
    }

    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Primarily useful for deterministic tests and embedded deployments.
    pub fn with_memory_limit_bytes(mut self, bytes: usize) -> Self {
        self.memory_limit_bytes = bytes.max(1);
        self
    }

    fn initialize(&mut self) -> Result<()> {
        let mut buffered = Vec::new();
        let mut buffered_bytes = 0usize;
        let mut output_schema = None;
        let mut sort_exprs = None;
        let mut spills = Vec::new();
        let mut memory = MemoryManager::reservation();
        let spill_rows = crate::common::config::query_limits()
            .spill_batch_rows
            .max(1);

        while let Some(batch) = self.input.next_batch()? {
            if output_schema.is_none() {
                let physical_schema = batch.schema.clone();
                sort_exprs = Some(
                    self.sort_keys
                        .iter()
                        .map(|key| {
                            (
                                remap_expr_for_batch(&key.expr, &self.schema, &physical_schema),
                                key.desc,
                            )
                        })
                        .collect::<Vec<_>>(),
                );
                output_schema = Some(physical_schema);
            }
            buffered_bytes = buffered_bytes.saturating_add(batch.estimated_size_bytes());
            super::profile::record_current_peak_memory(buffered_bytes);
            buffered.push(batch);
            if buffered_bytes >= self.memory_limit_bytes || !memory.try_resize(buffered_bytes) {
                let sorted = sort_batches(
                    &buffered,
                    output_schema.as_ref().unwrap().clone(),
                    sort_exprs.as_ref().unwrap(),
                    self.limit,
                )?;
                spills.push(SpillFile::write_batches(
                    sorted.schema.clone(),
                    &[sorted],
                    spill_rows,
                )?);
                buffered.clear();
                buffered_bytes = 0;
                memory.release();
            }
        }

        let Some(output_schema) = output_schema else {
            self.state = SortState::Done;
            return Ok(());
        };
        let sort_exprs = sort_exprs.unwrap_or_default();

        if spills.is_empty() {
            let sorted = sort_batches(&buffered, output_schema, &sort_exprs, self.limit)?;
            self.state = SortState::InMemory(Some(sorted));
            return Ok(());
        }

        if !buffered.is_empty() {
            let sorted = sort_batches(&buffered, output_schema.clone(), &sort_exprs, self.limit)?;
            spills.push(SpillFile::write_batches(
                sorted.schema.clone(),
                &[sorted],
                spill_rows,
            )?);
        }
        self.state = SortState::External(ExternalMerge::new(
            spills,
            output_schema,
            sort_exprs,
            self.limit,
            spill_rows,
        )?);
        Ok(())
    }
}

impl ExecutionOp for SortExec {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if matches!(self.state, SortState::Ready) {
            self.initialize()?;
        }
        match &mut self.state {
            SortState::InMemory(batch) => {
                let result = batch.take();
                self.state = SortState::Done;
                Ok(result)
            }
            SortState::External(merge) => {
                let result = merge.next_batch()?;
                if result.is_none() {
                    self.state = SortState::Done;
                }
                Ok(result)
            }
            SortState::Ready => unreachable!("sort initialization must change state"),
            SortState::Done => Ok(None),
        }
    }
}

fn sort_batches(
    batches: &[RecordBatch],
    output_schema: Schema,
    sort_exprs: &[(LogicalExpr, bool)],
    limit: Option<usize>,
) -> Result<RecordBatch> {
    let total_rows = batches.iter().map(RecordBatch::num_rows).sum();
    if total_rows == 0 {
        return Ok(RecordBatch::new(output_schema));
    }
    let mut columns = output_schema
        .columns
        .iter()
        .map(|column| ColumnArray::with_capacity(column.data_type, total_rows))
        .collect::<Vec<_>>();
    for batch in batches {
        for (index, column) in batch.columns.iter().enumerate() {
            columns[index].extend(column)?;
        }
    }
    let merged = RecordBatch::try_new(output_schema.clone(), columns)?;
    let evaluated_keys = evaluate_batch_sort_key_columns(sort_exprs, &merged)?;
    let mut rows = (0..total_rows as u32).collect::<Vec<_>>();
    let compare = |left: &u32, right: &u32| {
        compare_key_rows(&evaluated_keys, *left as usize, *right as usize, sort_exprs)
    };
    if let Some(limit) = limit {
        if limit > 0 && rows.len() > limit {
            rows.select_nth_unstable_by(limit, compare);
            rows.truncate(limit);
        }
        rows.sort_by(compare);
        rows.truncate(limit);
    } else {
        rows.sort_by(compare);
    }
    let output = merged
        .columns
        .iter()
        .map(|column| column.take(&rows))
        .collect();
    RecordBatch::try_new(output_schema, output)
}

enum SortKeyColumn {
    Typed(ColumnArray),
    Scalar(Vec<ScalarValue>),
}

fn evaluate_batch_sort_key_columns(
    sort_exprs: &[(LogicalExpr, bool)],
    batch: &RecordBatch,
) -> Result<Vec<SortKeyColumn>> {
    sort_exprs
        .iter()
        .map(|(expr, _)| match expr {
            LogicalExpr::Column(index) if batch.column(*index).is_some() => {
                Ok(SortKeyColumn::Typed(batch.column(*index).unwrap().clone()))
            }
            _ => Ok(SortKeyColumn::Scalar(eval_expr_column(expr, batch)?)),
        })
        .collect()
}

fn compare_key_rows(
    keys: &[SortKeyColumn],
    left: usize,
    right: usize,
    sort_exprs: &[(LogicalExpr, bool)],
) -> Ordering {
    for (index, (_, desc)) in sort_exprs.iter().enumerate() {
        let ordering = match &keys[index] {
            SortKeyColumn::Typed(column) => column.compare_at(left, right),
            SortKeyColumn::Scalar(values) => compare_scalars(&values[left], &values[right]),
        };
        let ordering = if *desc { ordering.reverse() } else { ordering };
        if !ordering.is_eq() {
            return ordering;
        }
    }
    Ordering::Equal
}

fn evaluate_batch_sort_keys(
    sort_exprs: &[(LogicalExpr, bool)],
    batch: &RecordBatch,
) -> Result<Vec<Vec<ScalarValue>>> {
    sort_exprs
        .iter()
        .map(|(expr, _)| eval_expr_column(expr, batch))
        .collect()
}

struct SpillCursor {
    _spill: SpillFile,
    reader: SegmentReader,
    row_group: usize,
    batch: Option<RecordBatch>,
    keys: Vec<Vec<ScalarValue>>,
    row: usize,
}

impl SpillCursor {
    fn new(spill: SpillFile, sort_exprs: &[(LogicalExpr, bool)]) -> Result<Self> {
        let reader = spill.open_reader()?;
        let mut cursor = Self {
            _spill: spill,
            reader,
            row_group: 0,
            batch: None,
            keys: Vec::new(),
            row: 0,
        };
        cursor.load_next_batch(sort_exprs)?;
        Ok(cursor)
    }

    fn load_next_batch(&mut self, sort_exprs: &[(LogicalExpr, bool)]) -> Result<()> {
        while self.row_group < self.reader.num_row_groups() {
            let batch = self.reader.read_row_group(self.row_group)?;
            self.row_group += 1;
            if batch.num_rows() == 0 {
                continue;
            }
            self.keys = evaluate_batch_sort_keys(sort_exprs, &batch)?;
            self.batch = Some(batch);
            self.row = 0;
            return Ok(());
        }
        self.batch = None;
        self.keys.clear();
        Ok(())
    }

    fn has_row(&self) -> bool {
        self.batch
            .as_ref()
            .is_some_and(|batch| self.row < batch.num_rows())
    }

    fn current_keys(&self) -> Vec<ScalarValue> {
        self.keys
            .iter()
            .map(|values| values[self.row].clone())
            .collect()
    }

    fn advance(&mut self, sort_exprs: &[(LogicalExpr, bool)]) -> Result<()> {
        self.row += 1;
        if !self.has_row() {
            self.load_next_batch(sort_exprs)?;
        }
        Ok(())
    }
}

struct ExternalMerge {
    cursors: Vec<SpillCursor>,
    heap: BinaryHeap<MergeHeapItem>,
    schema: Schema,
    sort_exprs: Vec<(LogicalExpr, bool)>,
    remaining: Option<usize>,
    batch_rows: usize,
}

#[derive(Clone)]
struct MergeHeapItem {
    keys: Vec<ScalarValue>,
    cursor: usize,
    descending: Arc<Vec<bool>>,
}

impl PartialEq for MergeHeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.cursor == other.cursor && self.keys == other.keys
    }
}

impl Eq for MergeHeapItem {}

impl Ord for MergeHeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        for (index, descending) in self.descending.iter().enumerate() {
            let ordering = compare_scalars(&self.keys[index], &other.keys[index]);
            let ordering = if *descending {
                ordering.reverse()
            } else {
                ordering
            };
            if !ordering.is_eq() {
                return ordering.reverse();
            }
        }
        other.cursor.cmp(&self.cursor)
    }
}

impl PartialOrd for MergeHeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl ExternalMerge {
    fn new(
        spills: Vec<SpillFile>,
        schema: Schema,
        sort_exprs: Vec<(LogicalExpr, bool)>,
        limit: Option<usize>,
        batch_rows: usize,
    ) -> Result<Self> {
        let mut cursors = Vec::with_capacity(spills.len());
        for spill in spills {
            cursors.push(SpillCursor::new(spill, &sort_exprs)?);
        }
        let descending = Arc::new(sort_exprs.iter().map(|(_, desc)| *desc).collect());
        let mut heap = BinaryHeap::with_capacity(cursors.len());
        for (cursor, state) in cursors.iter().enumerate() {
            if state.has_row() {
                heap.push(MergeHeapItem {
                    keys: state.current_keys(),
                    cursor,
                    descending: Arc::clone(&descending),
                });
            }
        }
        Ok(Self {
            cursors,
            heap,
            schema,
            sort_exprs,
            remaining: limit,
            batch_rows: batch_rows.max(1),
        })
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.remaining == Some(0) {
            return Ok(None);
        }
        let target = self
            .remaining
            .unwrap_or(self.batch_rows)
            .min(self.batch_rows);
        let mut output = RecordBatch::with_capacity(self.schema.clone(), target);
        while output.num_rows() < target {
            let Some(item) = self.heap.pop() else {
                break;
            };
            let cursor = &mut self.cursors[item.cursor];
            let batch = cursor
                .batch
                .as_ref()
                .expect("active spill cursor has batch");
            for column in 0..batch.num_columns() {
                let value = batch
                    .scalar(cursor.row, column)
                    .unwrap_or(ScalarValue::Int64(None));
                output.column_mut(column).unwrap().push(&value)?;
            }
            cursor.advance(&self.sort_exprs)?;
            if cursor.has_row() {
                self.heap.push(MergeHeapItem {
                    keys: cursor.current_keys(),
                    cursor: item.cursor,
                    descending: item.descending,
                });
            }
            if let Some(remaining) = self.remaining.as_mut() {
                *remaining = remaining.saturating_sub(1);
                if *remaining == 0 {
                    break;
                }
            }
        }
        if output.num_rows() == 0 {
            Ok(None)
        } else {
            Ok(Some(output))
        }
    }
}

pub fn compare_scalars(left: &ScalarValue, right: &ScalarValue) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (left, right) {
        (ScalarValue::Int8(Some(left)), ScalarValue::Int8(Some(right))) => left.cmp(right),
        (ScalarValue::Int16(Some(left)), ScalarValue::Int16(Some(right))) => left.cmp(right),
        (ScalarValue::Int32(Some(left)), ScalarValue::Int32(Some(right))) => left.cmp(right),
        (ScalarValue::Int64(Some(left)), ScalarValue::Int64(Some(right))) => left.cmp(right),
        (ScalarValue::UInt8(Some(left)), ScalarValue::UInt8(Some(right))) => left.cmp(right),
        (ScalarValue::UInt16(Some(left)), ScalarValue::UInt16(Some(right))) => left.cmp(right),
        (ScalarValue::UInt32(Some(left)), ScalarValue::UInt32(Some(right))) => left.cmp(right),
        (ScalarValue::UInt64(Some(left)), ScalarValue::UInt64(Some(right))) => left.cmp(right),
        (ScalarValue::Float32(Some(left)), ScalarValue::Float32(Some(right))) => {
            left.partial_cmp(right).unwrap_or(Ordering::Equal)
        }
        (ScalarValue::Float64(Some(left)), ScalarValue::Float64(Some(right))) => {
            left.partial_cmp(right).unwrap_or(Ordering::Equal)
        }
        (ScalarValue::Decimal(Some(left)), ScalarValue::Decimal(Some(right))) => left.cmp(right),
        (ScalarValue::Date(Some(left)), ScalarValue::Date(Some(right))) => left.cmp(right),
        (ScalarValue::Time(Some(left)), ScalarValue::Time(Some(right))) => left.cmp(right),
        (ScalarValue::Timestamp(Some(left)), ScalarValue::Timestamp(Some(right))) => {
            left.cmp(right)
        }
        (ScalarValue::Interval(Some(left)), ScalarValue::Interval(Some(right))) => left.cmp(right),
        (ScalarValue::VarChar(Some(left)), ScalarValue::VarChar(Some(right))) => left.cmp(right),
        (left, right) if left.is_null() && right.is_null() => Ordering::Equal,
        (left, _) if left.is_null() => Ordering::Greater,
        (_, right) if right.is_null() => Ordering::Less,
        _ => left
            .numeric_cmp(right)
            .unwrap_or_else(|| left.to_string().cmp(&right.to_string())),
    }
}

#[cfg(test)]
#[path = "sort_test.rs"]
mod tests;
