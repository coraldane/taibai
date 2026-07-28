use std::collections::VecDeque;
use std::hash::{DefaultHasher, Hash, Hasher};

use crate::common::error::{Result, TaibaiError};
use crate::sql::execution::join_hashtable::{JoinHashTable, RowPointer};
use crate::sql::execution::spill::{SpillFile, SpillWriter};
use crate::sql::planner::LogicalExpr;
use crate::storage::segment::SegmentReader;
use crate::types::array::ColumnArray;
use crate::types::batch::{RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

use super::{join::join_rows_match_expr, ExecutionOp};

struct JoinPartition {
    build: SpillFile,
    probe: SpillFile,
}

struct PartitionProbeState {
    partition: JoinPartition,
    build_reader: SegmentReader,
    next_build_row_group: usize,
    hash_table: Option<JoinHashTable>,
    probe_reader: Option<SegmentReader>,
    next_probe_row_group: usize,
    probe_batch: Option<RecordBatch>,
    probe_row: usize,
    pending_probe_row: Option<Vec<ScalarValue>>,
    pending_matches: Vec<RowPointer>,
    pending_match: usize,
}

impl PartitionProbeState {
    fn new(partition: JoinPartition) -> Result<Self> {
        let build_reader = partition.build.open_reader()?;
        Ok(Self {
            partition,
            build_reader,
            next_build_row_group: 0,
            hash_table: None,
            probe_reader: None,
            next_probe_row_group: 0,
            probe_batch: None,
            probe_row: 0,
            pending_probe_row: None,
            pending_matches: Vec::new(),
            pending_match: 0,
        })
    }

    fn load_next_build_chunk(
        &mut self,
        key_columns: &[usize],
        memory_limit_bytes: usize,
    ) -> Result<bool> {
        if self.next_build_row_group >= self.build_reader.num_row_groups() {
            self.hash_table = None;
            return Ok(false);
        }
        let mut table = JoinHashTable::new(key_columns.to_vec());
        let mut bytes = 0usize;
        while self.next_build_row_group < self.build_reader.num_row_groups() {
            let batch = self
                .build_reader
                .read_row_group(self.next_build_row_group)?;
            let batch_bytes = batch.estimated_size_bytes().saturating_mul(2);
            if table.row_count() > 0 && bytes.saturating_add(batch_bytes) > memory_limit_bytes {
                break;
            }
            self.next_build_row_group += 1;
            bytes = bytes.saturating_add(batch_bytes);
            super::profile::record_current_peak_memory(bytes);
            table.insert_batch(batch);
        }
        table.finalize();
        self.hash_table = Some(table);
        self.probe_reader = Some(self.partition.probe.open_reader()?);
        self.next_probe_row_group = 0;
        self.probe_batch = None;
        self.probe_row = 0;
        self.pending_probe_row = None;
        self.pending_matches.clear();
        self.pending_match = 0;
        Ok(true)
    }

    fn next_probe_row(&mut self) -> Result<Option<Vec<ScalarValue>>> {
        loop {
            if let Some(batch) = self.probe_batch.as_ref() {
                if self.probe_row < batch.num_rows() {
                    let row = (0..batch.num_columns())
                        .map(|column| {
                            batch
                                .scalar(self.probe_row, column)
                                .unwrap_or(ScalarValue::Int64(None))
                        })
                        .collect();
                    self.probe_row += 1;
                    return Ok(Some(row));
                }
            }
            let reader = self
                .probe_reader
                .as_ref()
                .ok_or_else(|| TaibaiError::Internal("join probe reader missing".into()))?;
            if self.next_probe_row_group >= reader.num_row_groups() {
                return Ok(None);
            }
            self.probe_batch = Some(reader.read_row_group(self.next_probe_row_group)?);
            self.next_probe_row_group += 1;
            self.probe_row = 0;
        }
    }
}

/// Grace-style partitioned inner join. Oversized or skewed build partitions
/// are processed in bounded row-group chunks; each chunk rescans its matching
/// probe partition, preserving correctness without unbounded hash-table growth.
pub(super) struct PartitionedInnerJoin {
    partitions: VecDeque<JoinPartition>,
    current: Option<PartitionProbeState>,
    build_key_columns: Vec<usize>,
    probe_key_columns: Vec<usize>,
    build_left: bool,
    condition: LogicalExpr,
    left_schema: Schema,
    right_schema: Schema,
    output_schema: Schema,
    memory_limit_bytes: usize,
    batch_rows: usize,
}

impl PartitionedInnerJoin {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn create(
        initial_build_batches: Vec<RecordBatch>,
        build_input: &mut dyn ExecutionOp,
        probe_input: &mut dyn ExecutionOp,
        build_schema: Schema,
        probe_schema: Schema,
        build_key_columns: Vec<usize>,
        probe_key_columns: Vec<usize>,
        build_left: bool,
        condition: LogicalExpr,
        left_schema: Schema,
        right_schema: Schema,
        output_schema: Schema,
        memory_limit_bytes: usize,
        partition_count: usize,
        batch_rows: usize,
    ) -> Result<Self> {
        let partition_count = partition_count.max(2).next_power_of_two();
        let build_files = partition_input(
            initial_build_batches,
            build_input,
            &build_schema,
            &build_key_columns,
            partition_count,
            batch_rows,
        )?;
        let probe_files = partition_input(
            Vec::new(),
            probe_input,
            &probe_schema,
            &probe_key_columns,
            partition_count,
            batch_rows,
        )?;
        let partitions = build_files
            .into_iter()
            .zip(probe_files)
            .filter_map(|(build, probe)| {
                Some(JoinPartition {
                    build: build?,
                    probe: probe?,
                })
            })
            .collect();
        Ok(Self {
            partitions,
            current: None,
            build_key_columns,
            probe_key_columns,
            build_left,
            condition,
            left_schema,
            right_schema,
            output_schema,
            memory_limit_bytes: memory_limit_bytes.max(1),
            batch_rows: batch_rows.max(1),
        })
    }

    pub(super) fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        let mut output = RecordBatch::with_capacity(self.output_schema.clone(), self.batch_rows);
        while output.num_rows() < self.batch_rows {
            if self.current.is_none() {
                let Some(partition) = self.partitions.pop_front() else {
                    break;
                };
                self.current = Some(PartitionProbeState::new(partition)?);
            }
            let state = self.current.as_mut().expect("join partition initialized");
            if state.hash_table.is_none()
                && !state.load_next_build_chunk(&self.build_key_columns, self.memory_limit_bytes)?
            {
                self.current = None;
                continue;
            }

            if state.pending_match < state.pending_matches.len() {
                let pointer = state.pending_matches[state.pending_match];
                state.pending_match += 1;
                let build_row = collect_build_row(
                    state.hash_table.as_ref().expect("join hash table missing"),
                    &pointer,
                );
                let probe_row = state
                    .pending_probe_row
                    .as_ref()
                    .expect("join probe row missing");
                let (left_row, right_row) = if self.build_left {
                    (build_row, probe_row.clone())
                } else {
                    (probe_row.clone(), build_row)
                };
                if join_rows_match_expr(
                    &self.condition,
                    &left_row,
                    &right_row,
                    self.left_schema.columns.len(),
                )? {
                    append_output_row(&mut output, &left_row, &right_row)?;
                }
                continue;
            }

            let Some(probe_row) = state.next_probe_row()? else {
                state.hash_table = None;
                continue;
            };
            let key = self
                .probe_key_columns
                .iter()
                .map(|index| probe_row[*index].clone())
                .collect::<Vec<_>>();
            state.pending_matches = state
                .hash_table
                .as_ref()
                .expect("join hash table missing")
                .probe(&key);
            state.pending_match = 0;
            state.pending_probe_row = Some(probe_row);
        }
        if output.num_rows() == 0 {
            Ok(None)
        } else {
            Ok(Some(output))
        }
    }
}

fn partition_input(
    initial_batches: Vec<RecordBatch>,
    input: &mut dyn ExecutionOp,
    schema: &Schema,
    key_columns: &[usize],
    partition_count: usize,
    batch_rows: usize,
) -> Result<Vec<Option<SpillFile>>> {
    let mut writers: Vec<Option<SpillWriter>> = std::iter::repeat_with(|| None)
        .take(partition_count)
        .collect();
    for batch in initial_batches {
        write_partitioned_batch(
            &batch,
            schema,
            key_columns,
            &mut writers,
            partition_count,
            batch_rows,
        )?;
    }
    while let Some(batch) = input.next_batch()? {
        write_partitioned_batch(
            &batch,
            schema,
            key_columns,
            &mut writers,
            partition_count,
            batch_rows,
        )?;
    }
    writers
        .into_iter()
        .map(|writer| writer.map(SpillWriter::finish).transpose())
        .collect()
}

fn write_partitioned_batch(
    batch: &RecordBatch,
    schema: &Schema,
    key_columns: &[usize],
    writers: &mut [Option<SpillWriter>],
    partition_count: usize,
    batch_rows: usize,
) -> Result<()> {
    let mut columns: Vec<Vec<ColumnArray>> = (0..partition_count)
        .map(|_| {
            schema
                .columns
                .iter()
                .map(|column| ColumnArray::empty(column.data_type))
                .collect()
        })
        .collect();
    let mut row_counts = vec![0usize; partition_count];
    for row in 0..batch.num_rows() {
        let key = key_columns
            .iter()
            .map(|index| {
                batch
                    .scalar(row, *index)
                    .unwrap_or(ScalarValue::Int64(None))
            })
            .collect::<Vec<_>>();
        if key.iter().any(ScalarValue::is_null) {
            continue;
        }
        let partition = hash_partition(&key, partition_count);
        for column in 0..batch.num_columns() {
            columns[partition][column].push(
                &batch
                    .scalar(row, column)
                    .unwrap_or(ScalarValue::Int64(None)),
            )?;
        }
        row_counts[partition] += 1;
    }
    for (partition, partition_columns) in columns.into_iter().enumerate() {
        if row_counts[partition] == 0 {
            continue;
        }
        let partition_batch = RecordBatch::try_new(schema.clone(), partition_columns)?;
        if writers[partition].is_none() {
            writers[partition] = Some(SpillWriter::create(schema.clone(), batch_rows)?);
        }
        writers[partition]
            .as_mut()
            .expect("partition spill writer initialized")
            .write_batch(&partition_batch)?;
    }
    Ok(())
}

fn hash_partition(key: &[ScalarValue], partition_count: usize) -> usize {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish() as usize & (partition_count - 1)
}

fn collect_build_row(table: &JoinHashTable, pointer: &RowPointer) -> Vec<ScalarValue> {
    let columns = table.build_data[pointer.batch_idx].num_columns();
    (0..columns)
        .map(|column| table.get_scalar(pointer, column))
        .collect()
}

fn append_output_row(
    output: &mut RecordBatch,
    left: &[ScalarValue],
    right: &[ScalarValue],
) -> Result<()> {
    for (column, value) in left.iter().chain(right).enumerate() {
        output
            .column_mut(column)
            .ok_or_else(|| TaibaiError::Internal("join output column missing".into()))?
            .push(value)?;
    }
    Ok(())
}
