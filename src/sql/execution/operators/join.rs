/// Hash Join Execution Operator - DuckDB-inspired Sink/Probe two-phase design.
/// Supports: Inner, Left, Right, Full, Cross, Semi, Anti, and Asof joins.
/// Uses columnar data processing through RecordBatch for cache efficiency.
use crate::common::error::Result;
use crate::sql::execution::functions::registry::FunctionRegistry;
use crate::sql::execution::join_hashtable::{AsofJoinIndex, JoinHashTable, RowPointer};
use crate::sql::parser::ast::BinOp;
use crate::sql::parser::ast::JoinType;
use crate::sql::planner::LogicalExpr;
use crate::types::array::ColumnArray;
use crate::types::batch::{RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

use super::join_spill::PartitionedInnerJoin;
use super::{ExecutionOp, RuntimeInFilter};

/// Columnar Hash Join Executor
pub struct HashJoinExec {
    pub left: Box<dyn ExecutionOp>,
    pub right: Box<dyn ExecutionOp>,
    pub join_type: JoinType,
    pub condition: LogicalExpr,
    pub left_schema: Schema,
    pub right_schema: Schema,
    pub output_schema: Schema,
    /// Column indices for left side join keys (extracted from condition)
    left_key_cols: Vec<usize>,
    /// Column indices for right side join keys (extracted from condition)
    right_key_cols: Vec<usize>,

    // Build phase state
    build_left: bool,
    memory_limit_bytes: usize,
    spill_state: Option<PartitionedInnerJoin>,
    hash_table: Option<JoinHashTable>,
    asof_index: Option<AsofJoinIndex>,
    // Probe phase state
    probe_state: ProbeState,
}

#[derive(Default)]
struct ProbeState {
    right_batches: Vec<RecordBatch>,
    left_batches: Vec<RecordBatch>,
    right_batch_idx: usize,
    right_row_idx: usize,
    right_collected: bool,
    left_batch_idx: usize,
    left_row_idx: usize,
    output_buffer: Vec<OutputRow>,
    buffer_pos: usize,
    build_done: bool,
    finalize_done: bool,
}

/// Represents one output row as (left_row_values, right_row_values or None)
type OutputRow = (Vec<ScalarValue>, Option<Vec<ScalarValue>>);

impl HashJoinExec {
    pub fn new(
        left: Box<dyn ExecutionOp>,
        right: Box<dyn ExecutionOp>,
        join_type: JoinType,
        condition: LogicalExpr,
        left_schema: Schema,
        right_schema: Schema,
        output_schema: Schema,
    ) -> Self {
        let (left_key_cols, right_key_cols) =
            extract_join_keys(&condition, left_schema.columns.len());

        Self {
            left,
            right,
            join_type,
            condition,
            left_schema,
            right_schema,
            output_schema,
            left_key_cols,
            right_key_cols,
            build_left: true,
            memory_limit_bytes: crate::common::config::query_limits()
                .query_memory_limit_mb
                .saturating_mul(1024 * 1024)
                .min(usize::MAX as u64) as usize,
            spill_state: None,
            hash_table: None,
            asof_index: None,
            probe_state: ProbeState::default(),
        }
    }

    /// For commutative inner joins the physical planner can choose the
    /// smaller input as the hash-table build side without changing SQL output
    /// column order.
    pub fn with_inner_build_left(mut self, build_left: bool) -> Self {
        if self.join_type == JoinType::Inner {
            self.build_left = build_left;
        }
        self
    }

    pub fn with_memory_limit_bytes(mut self, memory_limit_bytes: usize) -> Self {
        self.memory_limit_bytes = memory_limit_bytes.max(1);
        self
    }

    /// Build phase: consume all left-side batches into the hash table.
    fn build(&mut self) -> Result<()> {
        if self.join_type == JoinType::Inner {
            return self.build_inner();
        }
        match self.join_type {
            JoinType::Right | JoinType::Full => {
                // For RIGHT/FULL join, build hash table from right side
                let right_key_cols = self.right_key_cols.to_vec();
                let mut ht = JoinHashTable::new(right_key_cols);
                while let Some(batch) = self.right.next_batch()? {
                    ht.insert_batch(batch);
                }
                ht.finalize();
                self.hash_table = Some(ht);
            }
            JoinType::Asof => {
                // Asof join: build sorted index from right side
                let ts_col = self.right_key_cols.first().copied().unwrap_or(0);
                let mut batches = Vec::new();
                while let Some(batch) = self.right.next_batch()? {
                    batches.push(batch);
                }
                let mut asof_idx = AsofJoinIndex::new();
                asof_idx.build(batches, ts_col);
                self.asof_index = Some(asof_idx);
            }
            _ => {
                if self.join_type == JoinType::Cross {
                    self.probe_state.build_done = true;
                    return Ok(());
                }
                let mut ht = JoinHashTable::new(self.left_key_cols.clone());
                while let Some(batch) = self.left.next_batch()? {
                    ht.insert_batch(batch);
                }
                ht.finalize();
                self.inject_right_runtime_filter(&ht);
                self.hash_table = Some(ht);
            }
        }
        self.probe_state.build_done = true;
        Ok(())
    }

    fn build_inner(&mut self) -> Result<()> {
        let (build_keys, probe_keys) = if self.build_left {
            (self.left_key_cols.clone(), self.right_key_cols.clone())
        } else {
            (self.right_key_cols.clone(), self.left_key_cols.clone())
        };
        let mut table = JoinHashTable::new(build_keys.clone());
        let mut estimated_bytes = 0usize;

        loop {
            let batch = if self.build_left {
                self.left.next_batch()?
            } else {
                self.right.next_batch()?
            };
            let Some(batch) = batch else {
                break;
            };
            estimated_bytes =
                estimated_bytes.saturating_add(batch.estimated_size_bytes().saturating_mul(2));
            super::profile::record_current_peak_memory(estimated_bytes);
            table.insert_batch(batch);
            if estimated_bytes >= self.memory_limit_bytes {
                let initial_build_batches = std::mem::take(&mut table.build_data);
                let limits = crate::common::config::query_limits();
                self.spill_state = Some(if self.build_left {
                    PartitionedInnerJoin::create(
                        initial_build_batches,
                        self.left.as_mut(),
                        self.right.as_mut(),
                        self.left_schema.clone(),
                        self.right_schema.clone(),
                        build_keys,
                        probe_keys,
                        true,
                        self.condition.clone(),
                        self.left_schema.clone(),
                        self.right_schema.clone(),
                        self.output_schema.clone(),
                        self.memory_limit_bytes,
                        limits.join_spill_partitions,
                        limits.spill_batch_rows,
                    )?
                } else {
                    PartitionedInnerJoin::create(
                        initial_build_batches,
                        self.right.as_mut(),
                        self.left.as_mut(),
                        self.right_schema.clone(),
                        self.left_schema.clone(),
                        build_keys,
                        probe_keys,
                        false,
                        self.condition.clone(),
                        self.left_schema.clone(),
                        self.right_schema.clone(),
                        self.output_schema.clone(),
                        self.memory_limit_bytes,
                        limits.join_spill_partitions,
                        limits.spill_batch_rows,
                    )?
                });
                self.probe_state.build_done = true;
                return Ok(());
            }
        }

        table.finalize();
        if self.build_left {
            self.inject_right_runtime_filter(&table);
        } else {
            self.inject_left_runtime_filter(&table);
        }
        self.hash_table = Some(table);
        self.probe_state.build_done = true;
        Ok(())
    }
    fn inject_right_runtime_filter(&mut self, ht: &JoinHashTable) {
        if !matches!(
            self.join_type,
            JoinType::Inner | JoinType::Left | JoinType::Semi
        ) {
            return;
        }
        if self.left_key_cols.len() != 1 || self.right_key_cols.len() != 1 {
            return;
        }
        let values: Vec<ScalarValue> = ht
            .unique_key_values()
            .into_iter()
            .filter_map(|key| key.into_iter().next())
            .filter(|value| !value.is_null())
            .collect();
        if values.is_empty() || values.len() > 1024 {
            return;
        }
        self.right.add_runtime_in_filter(RuntimeInFilter {
            column_index: self.right_key_cols[0],
            values,
        });
    }

    fn inject_left_runtime_filter(&mut self, ht: &JoinHashTable) {
        if self.left_key_cols.len() != 1 || self.right_key_cols.len() != 1 {
            return;
        }
        let values: Vec<ScalarValue> = ht
            .unique_key_values()
            .into_iter()
            .filter_map(|key| key.into_iter().next())
            .filter(|value| !value.is_null())
            .collect();
        if values.is_empty() || values.len() > 1024 {
            return;
        }
        self.left.add_runtime_in_filter(RuntimeInFilter {
            column_index: self.left_key_cols[0],
            values,
        });
    }
}

impl ExecutionOp for HashJoinExec {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if !self.probe_state.build_done {
            self.build()?;
        }

        if let Some(spill) = self.spill_state.as_mut() {
            return spill.next_batch();
        }

        match self.join_type {
            JoinType::Inner => self.probe_inner(),
            JoinType::Left => self.probe_left(),
            JoinType::Right => self.probe_right(),
            JoinType::Full => self.probe_full(),
            JoinType::Cross => self.probe_cross(),
            JoinType::Semi => self.probe_semi(),
            JoinType::Anti => self.probe_anti(),
            JoinType::Asof => self.probe_asof(),
        }
    }
}

impl HashJoinExec {
    const BATCH_SIZE: usize = 4096;

    /// Inner join: emit rows where both sides match.
    fn probe_inner(&mut self) -> Result<Option<RecordBatch>> {
        if !self.build_left {
            return self.probe_inner_build_right();
        }
        loop {
            if self.probe_state.buffer_pos < self.probe_state.output_buffer.len() {
                return self.drain_buffer();
            }
            let right_row = match self.get_next_right_row() {
                Some(row) => row,
                None => return Ok(None),
            };
            let key: Vec<ScalarValue> = self
                .right_key_cols
                .iter()
                .map(|&idx| right_row[idx].clone())
                .collect();
            let matches = {
                let ht = self.hash_table.as_ref().unwrap();
                ht.probe(&key)
            };
            for ptr in &matches {
                let left_row = self.collect_left_row(ptr);
                if self.join_rows_match(&left_row, &right_row)? {
                    self.probe_state
                        .output_buffer
                        .push((left_row, Some(right_row.clone())));
                }
            }
            if self.probe_state.output_buffer.is_empty() {
                continue;
            }
            return self.drain_buffer();
        }
    }

    fn probe_inner_build_right(&mut self) -> Result<Option<RecordBatch>> {
        loop {
            if self.probe_state.buffer_pos < self.probe_state.output_buffer.len() {
                return self.drain_buffer();
            }
            let left_row = match self.get_next_left_row() {
                Some(row) => row,
                None => return Ok(None),
            };
            let key: Vec<ScalarValue> = self
                .left_key_cols
                .iter()
                .map(|&index| left_row[index].clone())
                .collect();
            let matches = self.hash_table.as_ref().unwrap().probe(&key);
            for pointer in &matches {
                let right_row = {
                    let table = self.hash_table.as_ref().unwrap();
                    self.collect_build_row(table, pointer)
                };
                if self.join_rows_match(&left_row, &right_row)? {
                    self.probe_state
                        .output_buffer
                        .push((left_row.clone(), Some(right_row)));
                }
            }
            if !self.probe_state.output_buffer.is_empty() {
                return self.drain_buffer();
            }
        }
    }

    /// Left join: emit matching rows + unmatched left rows with NULL right.
    fn probe_left(&mut self) -> Result<Option<RecordBatch>> {
        loop {
            if self.probe_state.buffer_pos < self.probe_state.output_buffer.len() {
                return self.drain_buffer();
            }
            let right_row = match self.get_next_right_row() {
                Some(row) => row,
                None => {
                    if self.probe_state.finalize_done {
                        return Ok(None);
                    }
                    self.emit_unmatched_left()?;
                    self.probe_state.finalize_done = true;
                    if self.probe_state.output_buffer.is_empty() {
                        return Ok(None);
                    }
                    return self.drain_buffer();
                }
            };
            let key: Vec<ScalarValue> = self
                .right_key_cols
                .iter()
                .map(|&idx| right_row[idx].clone())
                .collect();
            let matches = {
                let ht = self.hash_table.as_ref().unwrap();
                ht.probe(&key)
            };
            if matches.is_empty() {
                // No left match for this right row - for LEFT JOIN, we don't emit this case
                // We only emit unmatched left rows at the end
            } else {
                for ptr in &matches {
                    let left_row = self.collect_left_row(ptr);
                    if self.join_rows_match(&left_row, &right_row)? {
                        {
                            let ht = self.hash_table.as_mut().unwrap();
                            ht.mark_matched(ptr);
                        }
                        self.probe_state
                            .output_buffer
                            .push((left_row, Some(right_row.clone())));
                    }
                }
            }
            if self.probe_state.output_buffer.is_empty() {
                continue;
            }
            return self.drain_buffer();
        }
    }

    /// Right join: build from right, probe with left, emit unmatched right with NULL left.
    fn probe_right(&mut self) -> Result<Option<RecordBatch>> {
        loop {
            if self.probe_state.buffer_pos < self.probe_state.output_buffer.len() {
                return self.drain_buffer();
            }
            let left_row = match self.get_next_left_row() {
                Some(row) => row,
                None => {
                    if self.probe_state.finalize_done {
                        return Ok(None);
                    }
                    self.emit_unmatched_right()?;
                    self.probe_state.finalize_done = true;
                    if self.probe_state.output_buffer.is_empty() {
                        return Ok(None);
                    }
                    return self.drain_buffer();
                }
            };
            let key: Vec<ScalarValue> = self
                .left_key_cols
                .iter()
                .map(|&idx| left_row[idx].clone())
                .collect();
            let matches = {
                let ht = self.hash_table.as_ref().unwrap();
                ht.probe(&key)
            };
            if !matches.is_empty() {
                for ptr in &matches {
                    let right_row = {
                        let ht = self.hash_table.as_ref().unwrap();
                        self.collect_build_row(ht, ptr)
                    };
                    if self.join_rows_match(&left_row, &right_row)? {
                        {
                            let ht = self.hash_table.as_mut().unwrap();
                            ht.mark_matched(ptr);
                        }
                        self.probe_state
                            .output_buffer
                            .push((left_row.clone(), Some(right_row)));
                    }
                }
            }
            if self.probe_state.output_buffer.is_empty() {
                continue;
            }
            return self.drain_buffer();
        }
    }

    /// Full outer join: combine left and right join logic.
    fn probe_full(&mut self) -> Result<Option<RecordBatch>> {
        loop {
            if self.probe_state.buffer_pos < self.probe_state.output_buffer.len() {
                return self.drain_buffer();
            }
            let left_row = match self.get_next_left_row() {
                Some(row) => row,
                None => {
                    if self.probe_state.finalize_done {
                        return Ok(None);
                    }
                    self.emit_unmatched_right()?;
                    self.probe_state.finalize_done = true;
                    if self.probe_state.output_buffer.is_empty() {
                        return Ok(None);
                    }
                    return self.drain_buffer();
                }
            };
            let key: Vec<ScalarValue> = self
                .left_key_cols
                .iter()
                .map(|&idx| left_row[idx].clone())
                .collect();
            let matches = {
                let ht = self.hash_table.as_ref().unwrap();
                ht.probe(&key)
            };
            if matches.is_empty() {
                let null_right = self.make_null_right_row();
                self.probe_state
                    .output_buffer
                    .push((left_row, Some(null_right)));
            } else {
                let mut matched_any = false;
                for ptr in &matches {
                    let right_row = {
                        let ht = self.hash_table.as_ref().unwrap();
                        self.collect_build_row(ht, ptr)
                    };
                    if self.join_rows_match(&left_row, &right_row)? {
                        matched_any = true;
                        {
                            let ht = self.hash_table.as_mut().unwrap();
                            ht.mark_matched(ptr);
                        }
                        self.probe_state
                            .output_buffer
                            .push((left_row.clone(), Some(right_row)));
                    }
                }
                if !matched_any {
                    let null_right = self.make_null_right_row();
                    self.probe_state
                        .output_buffer
                        .push((left_row, Some(null_right)));
                }
            }
            if self.probe_state.output_buffer.is_empty() {
                continue;
            }
            return self.drain_buffer();
        }
    }

    /// Cross join: Cartesian product of left and right.
    fn probe_cross(&mut self) -> Result<Option<RecordBatch>> {
        if !self.probe_state.right_collected {
            let mut left_rows = Vec::new();
            while let Some(batch) = self.left.next_batch()? {
                for row in 0..batch.num_rows() {
                    let vals: Vec<ScalarValue> = (0..batch.num_columns())
                        .map(|c| batch.scalar(row, c).unwrap_or(ScalarValue::Int64(None)))
                        .collect();
                    left_rows.push(vals);
                }
            }
            while let Ok(Some(batch)) = self.right.next_batch() {
                self.probe_state.right_batches.push(batch);
            }
            self.probe_state.right_collected = true;
            for left_row in &left_rows {
                for right_batch in &self.probe_state.right_batches {
                    for right_row_idx in 0..right_batch.num_rows() {
                        let right_row: Vec<ScalarValue> = (0..right_batch.num_columns())
                            .map(|c| {
                                right_batch
                                    .scalar(right_row_idx, c)
                                    .unwrap_or(ScalarValue::Int64(None))
                            })
                            .collect();
                        self.probe_state
                            .output_buffer
                            .push((left_row.clone(), Some(right_row)));
                    }
                }
            }
        }
        self.drain_buffer()
    }

    /// Semi join: emit left rows that have at least one match in right.
    fn probe_semi(&mut self) -> Result<Option<RecordBatch>> {
        loop {
            if self.probe_state.buffer_pos < self.probe_state.output_buffer.len() {
                return self.drain_buffer();
            }
            let right_row = match self.get_next_right_row() {
                Some(row) => row,
                None => return Ok(None),
            };
            let key: Vec<ScalarValue> = self
                .right_key_cols
                .iter()
                .map(|&idx| right_row[idx].clone())
                .collect();
            let matches = {
                let ht = self.hash_table.as_ref().unwrap();
                ht.probe(&key)
            };
            for ptr in &matches {
                let left_row = self.collect_left_row(ptr);
                if self.join_rows_match(&left_row, &right_row)? {
                    self.probe_state.output_buffer.push((left_row, None));
                }
            }
            if self.probe_state.output_buffer.is_empty() {
                continue;
            }
            return self.drain_buffer();
        }
    }

    /// Anti join: emit left rows that have NO match in right.
    fn probe_anti(&mut self) -> Result<Option<RecordBatch>> {
        if !self.probe_state.right_collected {
            while let Ok(Some(batch)) = self.right.next_batch() {
                for row in 0..batch.num_rows() {
                    let right_row: Vec<ScalarValue> = (0..batch.num_columns())
                        .map(|c| batch.scalar(row, c).unwrap_or(ScalarValue::Int64(None)))
                        .collect();
                    let key: Vec<ScalarValue> = self
                        .right_key_cols
                        .iter()
                        .map(|&idx| right_row[idx].clone())
                        .collect();
                    let _matches = {
                        let ht = self.hash_table.as_mut().unwrap();
                        let m = ht.probe(&key);
                        m
                    };
                    for ptr in &_matches {
                        let left_row = self.collect_left_row(ptr);
                        if self.join_rows_match(&left_row, &right_row)? {
                            let ht = self.hash_table.as_mut().unwrap();
                            ht.mark_matched(ptr);
                        }
                    }
                }
            }
            self.probe_state.right_collected = true;
            let unmatched = {
                let ht = self.hash_table.as_ref().unwrap();
                ht.unmatched_rows()
            };
            for ptr in &unmatched {
                let left_row = self.collect_left_row(ptr);
                self.probe_state.output_buffer.push((left_row, None));
            }
        }
        self.drain_buffer()
    }

    /// Asof join: for each left row, find closest right row by timestamp.
    fn probe_asof(&mut self) -> Result<Option<RecordBatch>> {
        if self.probe_state.buffer_pos < self.probe_state.output_buffer.len() {
            return self.drain_buffer();
        }
        let left_row = match self.get_next_left_row() {
            Some(row) => row,
            None => return Ok(None),
        };
        let ts_col = self.left_key_cols.first().copied().unwrap_or(0);
        let probe_ts = left_row[ts_col].clone();
        let found = {
            let asof = self.asof_index.as_ref().unwrap();
            asof.find_asof(&probe_ts)
        };
        if let Some(_ptr) = found {
            let right_row: Vec<ScalarValue> = (0..self.right_schema.columns.len())
                .map(|_| ScalarValue::Int64(None))
                .collect();
            self.probe_state
                .output_buffer
                .push((left_row, Some(right_row)));
        } else {
            let null_right = self.make_null_right_row();
            self.probe_state
                .output_buffer
                .push((left_row, Some(null_right)));
        }
        self.drain_buffer()
    }

    // --- Helper methods ---

    fn get_next_right_row(&mut self) -> Option<Vec<ScalarValue>> {
        if !self.probe_state.right_collected {
            while let Ok(Some(batch)) = self.right.next_batch() {
                if batch.num_rows() > 0 {
                    self.probe_state.right_batches.push(batch);
                }
            }
            self.probe_state.right_collected = true;
        }
        if self.probe_state.right_batch_idx < self.probe_state.right_batches.len() {
            let batch = &self.probe_state.right_batches[self.probe_state.right_batch_idx];
            if self.probe_state.right_row_idx < batch.num_rows() {
                let row: Vec<ScalarValue> = (0..batch.num_columns())
                    .map(|c| {
                        batch
                            .scalar(self.probe_state.right_row_idx, c)
                            .unwrap_or(ScalarValue::Int64(None))
                    })
                    .collect();
                self.probe_state.right_row_idx += 1;
                if self.probe_state.right_row_idx >= batch.num_rows() {
                    self.probe_state.right_batch_idx += 1;
                    self.probe_state.right_row_idx = 0;
                }
                return Some(row);
            }
        }
        None
    }

    fn get_next_left_row(&mut self) -> Option<Vec<ScalarValue>> {
        loop {
            if self.probe_state.left_batch_idx < self.probe_state.left_batches.len() {
                let batch = &self.probe_state.left_batches[self.probe_state.left_batch_idx];
                if self.probe_state.left_row_idx < batch.num_rows() {
                    let row: Vec<ScalarValue> = (0..batch.num_columns())
                        .map(|c| {
                            batch
                                .scalar(self.probe_state.left_row_idx, c)
                                .unwrap_or(ScalarValue::Int64(None))
                        })
                        .collect();
                    self.probe_state.left_row_idx += 1;
                    if self.probe_state.left_row_idx >= batch.num_rows() {
                        self.probe_state.left_batch_idx += 1;
                        self.probe_state.left_row_idx = 0;
                    }
                    return Some(row);
                }
            }
            match self.left.next_batch() {
                Ok(Some(batch)) => {
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    self.probe_state.left_batches.push(batch);
                    self.probe_state.left_batch_idx = self.probe_state.left_batches.len() - 1;
                    self.probe_state.left_row_idx = 0;
                }
                _ => return None,
            }
        }
    }

    fn collect_left_row(&self, ptr: &RowPointer) -> Vec<ScalarValue> {
        let ht = self.hash_table.as_ref().unwrap();
        (0..self.left_schema.columns.len())
            .map(|c| ht.get_scalar(ptr, c))
            .collect()
    }

    fn collect_build_row(&self, ht: &JoinHashTable, ptr: &RowPointer) -> Vec<ScalarValue> {
        (0..self.right_schema.columns.len())
            .map(|c| ht.get_scalar(ptr, c))
            .collect()
    }

    fn collect_build_row_inner(&self, ht: &JoinHashTable, ptr: &RowPointer) -> Vec<ScalarValue> {
        self.collect_build_row(ht, ptr)
    }

    fn make_null_right_row(&self) -> Vec<ScalarValue> {
        self.right_schema
            .columns
            .iter()
            .map(|c| ScalarValue::null_for_type(c.data_type))
            .collect()
    }

    fn make_null_left_row(&self) -> Vec<ScalarValue> {
        self.left_schema
            .columns
            .iter()
            .map(|c| ScalarValue::null_for_type(c.data_type))
            .collect()
    }

    fn emit_unmatched_left(&mut self) -> Result<()> {
        let unmatched = {
            let ht = self.hash_table.as_ref().unwrap();
            ht.unmatched_rows()
        };
        let null_right = self.make_null_right_row();
        for ptr in &unmatched {
            let left_row = self.collect_left_row(ptr);
            self.probe_state
                .output_buffer
                .push((left_row, Some(null_right.clone())));
        }
        Ok(())
    }

    fn emit_unmatched_right(&mut self) -> Result<()> {
        let unmatched = {
            let ht = self.hash_table.as_ref().unwrap();
            ht.unmatched_rows()
        };
        let null_left = self.make_null_left_row();
        for ptr in &unmatched {
            let right_row = {
                let ht = self.hash_table.as_ref().unwrap();
                self.collect_build_row_inner(ht, ptr)
            };
            self.probe_state
                .output_buffer
                .push((null_left.clone(), Some(right_row)));
        }
        Ok(())
    }

    fn join_rows_match(&self, left_row: &[ScalarValue], right_row: &[ScalarValue]) -> Result<bool> {
        join_rows_match_expr(
            &self.condition,
            left_row,
            right_row,
            self.left_schema.columns.len(),
        )
    }

    fn drain_buffer(&mut self) -> Result<Option<RecordBatch>> {
        let start = self.probe_state.buffer_pos;
        let end = (start + Self::BATCH_SIZE).min(self.probe_state.output_buffer.len());
        if start >= end {
            return Ok(None);
        }

        let left_ncols = self.left_schema.columns.len();
        let right_ncols = self.right_schema.columns.len();
        let output_ncols = left_ncols + right_ncols;

        // Determine output schema based on join type
        let mut columns: Vec<ColumnArray> = Vec::with_capacity(output_ncols);
        for i in 0..output_ncols {
            let dt = if i < left_ncols {
                self.left_schema.columns[i].data_type
            } else {
                self.right_schema.columns[i - left_ncols].data_type
            };
            columns.push(ColumnArray::empty(dt));
        }

        for idx in start..end {
            let (left_row, right_row_opt) = &self.probe_state.output_buffer[idx];
            for (i, val) in left_row.iter().enumerate() {
                columns[i].push(val)?;
            }
            match right_row_opt {
                Some(right_row) => {
                    for (i, val) in right_row.iter().enumerate() {
                        columns[left_ncols + i].push(val)?;
                    }
                }
                None => {
                    if output_ncols > left_ncols {
                        for i in 0..right_ncols {
                            let null_val =
                                ScalarValue::null_for_type(self.right_schema.columns[i].data_type);
                            columns[left_ncols + i].push(&null_val)?;
                        }
                    }
                }
            }
        }

        self.probe_state.buffer_pos = end;
        if self.probe_state.buffer_pos >= self.probe_state.output_buffer.len() {
            self.probe_state.output_buffer.clear();
            self.probe_state.buffer_pos = 0;
        }

        // Build appropriate output schema
        let schema = self.output_schema.clone();
        Ok(Some(RecordBatch::try_new(schema, columns)?))
    }
}

/// Extract join key column indices from the join condition.
/// Returns (left_key_indices, right_key_indices).
fn extract_join_keys(condition: &LogicalExpr, left_ncols: usize) -> (Vec<usize>, Vec<usize>) {
    let mut left_keys = Vec::new();
    let mut right_keys = Vec::new();
    collect_keys(condition, left_ncols, &mut left_keys, &mut right_keys);
    (left_keys, right_keys)
}

fn collect_keys(
    expr: &LogicalExpr,
    left_ncols: usize,
    left_keys: &mut Vec<usize>,
    right_keys: &mut Vec<usize>,
) {
    match expr {
        LogicalExpr::BinaryOp { left, op, right } => {
            if matches!(op, BinOp::And) {
                collect_keys(left, left_ncols, left_keys, right_keys);
                collect_keys(right, left_ncols, left_keys, right_keys);
                return;
            }
            if matches!(op, BinOp::Eq) {
                if let LogicalExpr::Column(l) = **left {
                    if let LogicalExpr::Column(r) = **right {
                        if l < left_ncols && r >= left_ncols {
                            left_keys.push(l);
                            right_keys.push(r - left_ncols);
                        } else if r < left_ncols && l >= left_ncols {
                            left_keys.push(r);
                            right_keys.push(l - left_ncols);
                        }
                    }
                }
            }
        }
        LogicalExpr::Literal(_) => {}
        _ => {}
    }
}

pub(super) fn join_rows_match_expr(
    condition: &LogicalExpr,
    left_row: &[ScalarValue],
    right_row: &[ScalarValue],
    left_ncols: usize,
) -> Result<bool> {
    match eval_join_expr(condition, left_row, right_row, left_ncols)? {
        ScalarValue::Bool(Some(value)) => Ok(value),
        value => Ok(!value.is_null()),
    }
}

fn eval_join_expr(
    expr: &LogicalExpr,
    left_row: &[ScalarValue],
    right_row: &[ScalarValue],
    left_ncols: usize,
) -> Result<ScalarValue> {
    match expr {
        LogicalExpr::Column(idx) => {
            if *idx < left_ncols {
                Ok(left_row
                    .get(*idx)
                    .cloned()
                    .unwrap_or(ScalarValue::Int64(None)))
            } else {
                Ok(right_row
                    .get(*idx - left_ncols)
                    .cloned()
                    .unwrap_or(ScalarValue::Int64(None)))
            }
        }
        LogicalExpr::Literal(value) => Ok(value.clone()),
        LogicalExpr::BinaryOp { left, op, right } => {
            if matches!(op, BinOp::And | BinOp::Or) {
                let left_bool =
                    scalar_truthy(&eval_join_expr(left, left_row, right_row, left_ncols)?);
                let right_bool =
                    scalar_truthy(&eval_join_expr(right, left_row, right_row, left_ncols)?);
                return Ok(ScalarValue::Bool(Some(if matches!(op, BinOp::And) {
                    left_bool && right_bool
                } else {
                    left_bool || right_bool
                })));
            }
            let left_value = eval_join_expr(left, left_row, right_row, left_ncols)?;
            let right_value = eval_join_expr(right, left_row, right_row, left_ncols)?;
            Ok(eval_join_binary_scalar(&left_value, op, &right_value))
        }
        LogicalExpr::Not(inner) => {
            let value = eval_join_expr(inner, left_row, right_row, left_ncols)?;
            Ok(ScalarValue::Bool(Some(!scalar_truthy(&value))))
        }
        LogicalExpr::IsNull(inner) => {
            let value = eval_join_expr(inner, left_row, right_row, left_ncols)?;
            Ok(ScalarValue::Bool(Some(value.is_null())))
        }
        LogicalExpr::IsNotNull(inner) => {
            let value = eval_join_expr(inner, left_row, right_row, left_ncols)?;
            Ok(ScalarValue::Bool(Some(!value.is_null())))
        }
        LogicalExpr::Function { name, args } => {
            let arg_values = args
                .iter()
                .map(|arg| eval_join_expr(arg, left_row, right_row, left_ncols))
                .collect::<Result<Vec<_>>>()?;
            FunctionRegistry::default().execute(name, &arg_values)
        }
        _ => Ok(ScalarValue::Int64(None)),
    }
}

fn scalar_truthy(value: &ScalarValue) -> bool {
    match value {
        ScalarValue::Bool(Some(v)) => *v,
        _ => !value.is_null(),
    }
}

fn eval_join_binary_scalar(left: &ScalarValue, op: &BinOp, right: &ScalarValue) -> ScalarValue {
    if left.is_null() || right.is_null() {
        return ScalarValue::Bool(None);
    }
    let cmp = left.numeric_cmp(right).or_else(|| match (left, right) {
        (ScalarValue::VarChar(Some(l)), ScalarValue::VarChar(Some(r))) => Some(l.cmp(r)),
        (ScalarValue::Bool(Some(l)), ScalarValue::Bool(Some(r))) => Some(l.cmp(r)),
        _ => None,
    });
    match op {
        BinOp::Eq => ScalarValue::Bool(Some(cmp == Some(std::cmp::Ordering::Equal))),
        BinOp::Neq => ScalarValue::Bool(Some(cmp != Some(std::cmp::Ordering::Equal))),
        BinOp::Lt => ScalarValue::Bool(Some(cmp == Some(std::cmp::Ordering::Less))),
        BinOp::Gt => ScalarValue::Bool(Some(cmp == Some(std::cmp::Ordering::Greater))),
        BinOp::Lte => ScalarValue::Bool(Some(cmp != Some(std::cmp::Ordering::Greater))),
        BinOp::Gte => ScalarValue::Bool(Some(cmp != Some(std::cmp::Ordering::Less))),
        _ => ScalarValue::Bool(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::types::DataType;
    use crate::sql::planner::merge_schemas;
    use crate::types::batch::ColumnDef;

    struct OneBatchExec(Option<RecordBatch>);

    impl ExecutionOp for OneBatchExec {
        fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
            Ok(self.0.take())
        }
    }

    fn int_batch(schema: &Schema, values: &[i64]) -> RecordBatch {
        let mut column = ColumnArray::empty(DataType::Int64);
        for value in values {
            column.push(&ScalarValue::Int64(Some(*value))).unwrap();
        }
        RecordBatch::try_new(schema.clone(), vec![column]).unwrap()
    }

    #[test]
    fn inner_join_can_build_smaller_right_side_without_reordering_output() {
        let left_schema = Schema::new(vec![ColumnDef::new("left_id", DataType::Int64)]);
        let right_schema = Schema::new(vec![ColumnDef::new("right_id", DataType::Int64)]);
        let condition = LogicalExpr::BinaryOp {
            left: Box::new(LogicalExpr::Column(0)),
            op: BinOp::Eq,
            right: Box::new(LogicalExpr::Column(1)),
        };
        let mut join = HashJoinExec::new(
            Box::new(OneBatchExec(Some(int_batch(&left_schema, &[1, 2, 3])))),
            Box::new(OneBatchExec(Some(int_batch(&right_schema, &[2])))),
            JoinType::Inner,
            condition,
            left_schema.clone(),
            right_schema.clone(),
            merge_schemas(&left_schema, &right_schema),
        )
        .with_inner_build_left(false);

        let output = join.next_batch().unwrap().unwrap();
        assert_eq!(output.schema.columns[0].name, "left_id");
        assert_eq!(output.schema.columns[1].name, "right_id");
        assert_eq!(output.scalar(0, 0), Some(ScalarValue::Int64(Some(2))));
        assert_eq!(output.scalar(0, 1), Some(ScalarValue::Int64(Some(2))));
        assert!(join.next_batch().unwrap().is_none());
    }

    #[test]
    fn inner_join_spills_partitions_and_preserves_duplicate_matches() {
        let left_schema = Schema::new(vec![ColumnDef::new("left_id", DataType::Int64)]);
        let right_schema = Schema::new(vec![ColumnDef::new("right_id", DataType::Int64)]);
        let condition = LogicalExpr::BinaryOp {
            left: Box::new(LogicalExpr::Column(0)),
            op: BinOp::Eq,
            right: Box::new(LogicalExpr::Column(1)),
        };
        for build_left in [true, false] {
            let mut join = HashJoinExec::new(
                Box::new(OneBatchExec(Some(int_batch(&left_schema, &[1, 2, 2, 3])))),
                Box::new(OneBatchExec(Some(int_batch(&right_schema, &[2, 2, 4])))),
                JoinType::Inner,
                condition.clone(),
                left_schema.clone(),
                right_schema.clone(),
                merge_schemas(&left_schema, &right_schema),
            )
            .with_inner_build_left(build_left)
            .with_memory_limit_bytes(1);

            let mut rows = Vec::new();
            while let Some(batch) = join.next_batch().unwrap() {
                for row in 0..batch.num_rows() {
                    rows.push((batch.scalar(row, 0), batch.scalar(row, 1)));
                }
            }
            assert_eq!(rows.len(), 4);
            assert!(rows.iter().all(|(left, right)| {
                *left == Some(ScalarValue::Int64(Some(2)))
                    && *right == Some(ScalarValue::Int64(Some(2)))
            }));
        }
    }
}
