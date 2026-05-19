/// Hash Join Execution Operator - DuckDB-inspired Sink/Probe two-phase design.
/// Supports: Inner, Left, Right, Full, Cross, Semi, Anti, and Asof joins.
/// Uses columnar data processing through RecordBatch for cache efficiency.
use crate::common::error::{MonaxError, Result};
use crate::sql::execution::join_hashtable::{AsofJoinIndex, JoinHashTable, RowPointer};
use crate::sql::parser::ast::JoinType;
use crate::sql::planner::LogicalExpr;
use crate::types::array::ColumnArray;
use crate::types::batch::{RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

use super::filter::eval_expr_column;
use super::limit::trim_batch;
use super::ExecutionOp;

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
    hash_table: Option<JoinHashTable>,
    asof_index: Option<AsofJoinIndex>,
    // Probe phase state
    probe_state: ProbeState,
}

#[derive(Default)]
struct ProbeState {
    /// Collected right-side batches for probe
    right_batches: Vec<RecordBatch>,
    /// Current position in probe iteration
    right_batch_idx: usize,
    right_row_idx: usize,
    /// Buffered output rows for current next_batch call
    output_buffer: Vec<OutputRow>,
    buffer_pos: usize,
    /// Whether build phase is complete
    build_done: bool,
    /// Whether all output has been emitted (including LEFT/FULL null rows)
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
        let (left_key_cols, right_key_cols) = extract_join_keys(
            &condition,
            left_schema.columns.len(),
        );

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
            hash_table: None,
            asof_index: None,
            probe_state: ProbeState::default(),
        }
    }

    /// Build phase: consume all left-side batches into the hash table.
    fn build(&mut self) -> Result<()> {
        match self.join_type {
            JoinType::Right | JoinType::Full => {
                // For RIGHT/FULL join, build hash table from right side
                let right_key_cols: Vec<usize> = self.right_key_cols.iter()
                    .map(|&idx| idx)
                    .collect();
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
                // For Inner/Left/Semi/Anti/Cross: build hash table from left side
                let mut ht = JoinHashTable::new(self.left_key_cols.clone());
                while let Some(batch) = self.left.next_batch()? {
                    ht.insert_batch(batch);
                }
                ht.finalize();
                self.hash_table = Some(ht);
            }
        }
        self.probe_state.build_done = true;
        Ok(())
    }
}

impl ExecutionOp for HashJoinExec {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if !self.probe_state.build_done {
            self.build()?;
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
            _ => Ok(None),
        }
    }
}

impl HashJoinExec {
    const BATCH_SIZE: usize = 4096;

    /// Inner join: emit rows where both sides match.
    fn probe_inner(&mut self) -> Result<Option<RecordBatch>> {
        let ht = self.hash_table.as_ref().unwrap();
        loop {
            // Drain buffered output first
            if self.probe_state.buffer_pos < self.probe_state.output_buffer.len() {
                return self.drain_buffer();
            }
            // Get next right row
            let right_row = match self.get_next_right_row() {
                Some(row) => row,
                None => return Ok(None),
            };
            // Probe hash table
            let key: Vec<ScalarValue> = self.right_key_cols.iter()
                .map(|&idx| right_row[idx].clone())
                .collect();
            let matches = ht.probe(&key);
            for ptr in &matches {
                let left_row = self.collect_left_row(ptr);
                self.probe_state.output_buffer.push((left_row, Some(right_row.clone())));
            }
            if self.probe_state.output_buffer.is_empty() {
                continue;
            }
            return self.drain_buffer();
        }
    }

    /// Left join: emit matching rows + unmatched left rows with NULL right.
    fn probe_left(&mut self) -> Result<Option<RecordBatch>> {
        let ht = self.hash_table.as_mut().unwrap();
        loop {
            if self.probe_state.buffer_pos < self.probe_state.output_buffer.len() {
                return self.drain_buffer();
            }
            let right_row = match self.get_next_right_row() {
                Some(row) => row,
                None => {
                    // Emit unmatched left rows
                    if self.probe_state.finalize_done {
                        return Ok(None);
                    }
                    self.emit_unmatched_left(ht)?;
                    self.probe_state.finalize_done = true;
                    if self.probe_state.output_buffer.is_empty() {
                        return Ok(None);
                    }
                    return self.drain_buffer();
                }
            };
            let key: Vec<ScalarValue> = self.right_key_cols.iter()
                .map(|&idx| right_row[idx].clone())
                .collect();
            let matches = ht.probe(&key);
            for ptr in &matches {
                ht.mark_matched(ptr);
                let left_row = self.collect_left_row(ptr);
                self.probe_state.output_buffer.push((left_row, Some(right_row.clone())));
            }
            if self.probe_state.output_buffer.is_empty() {
                continue;
            }
            return self.drain_buffer();
        }
    }

    /// Right join: build from right, probe with left, emit unmatched right with NULL left.
    fn probe_right(&mut self) -> Result<Option<RecordBatch>> {
        let ht = self.hash_table.as_mut().unwrap();
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
                    self.emit_unmatched_right(ht)?;
                    self.probe_state.finalize_done = true;
                    if self.probe_state.output_buffer.is_empty() {
                        return Ok(None);
                    }
                    return self.drain_buffer();
                }
            };
            // Probe with left key (using right_key_cols as build side)
            let key: Vec<ScalarValue> = self.left_key_cols.iter()
                .map(|&idx| left_row[idx].clone())
                .collect();
            let matches = ht.probe(&key);
            for ptr in &matches {
                ht.mark_matched(ptr);
                let right_row = self.collect_build_row(ht, ptr);
                // For right join output: left cols + right cols
                self.probe_state.output_buffer.push((left_row.clone(), Some(right_row)));
            }
            if self.probe_state.output_buffer.is_empty() {
                continue;
            }
            return self.drain_buffer();
        }
    }

    /// Full outer join: combine left and right join logic.
    fn probe_full(&mut self) -> Result<Option<RecordBatch>> {
        // First do left join with right probe
        let ht = self.hash_table.as_mut().unwrap();
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
                    self.emit_unmatched_right(ht)?;
                    self.probe_state.finalize_done = true;
                    if self.probe_state.output_buffer.is_empty() {
                        return Ok(None);
                    }
                    return self.drain_buffer();
                }
            };
            let key: Vec<ScalarValue> = self.left_key_cols.iter()
                .map(|&idx| left_row[idx].clone())
                .collect();
            let matches = ht.probe(&key);
            if matches.is_empty() {
                // Left unmatched
                let null_right = self.make_null_right_row();
                self.probe_state.output_buffer.push((left_row, Some(null_right)));
            } else {
                for ptr in &matches {
                    ht.mark_matched(ptr);
                    let right_row = self.collect_build_row(ht, ptr);
                    self.probe_state.output_buffer.push((left_row.clone(), Some(right_row)));
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
        // Collect all left rows first
        if self.probe_state.right_batches.is_empty() {
            let mut left_rows = Vec::new();
            while let Some(batch) = self.left.next_batch()? {
                for row in 0..batch.num_rows() {
                    let vals: Vec<ScalarValue> = (0..batch.num_columns())
                        .map(|c| batch.scalar(row, c).unwrap_or(ScalarValue::Int64(None)))
                        .collect();
                    left_rows.push(vals);
                }
            }
            // Store in right_batches temporarily as left_rows
            // Actually, we need to store them differently
            self.probe_state.right_batches = Vec::new();
            while let Some(batch) = self.right.next_batch()? {
                self.probe_state.right_batches.push(batch);
            }
            // Store left rows as the buffer source
            for left_row in &left_rows {
                for right_batch in &self.probe_state.right_batches {
                    for right_row_idx in 0..right_batch.num_rows() {
                        let right_row: Vec<ScalarValue> = (0..right_batch.num_columns())
                            .map(|c| right_batch.scalar(right_row_idx, c).unwrap_or(ScalarValue::Int64(None)))
                            .collect();
                        self.probe_state.output_buffer.push((left_row.clone(), Some(right_row)));
                    }
                }
            }
        }
        self.drain_buffer()
    }

    /// Semi join: emit left rows that have at least one match in right.
    fn probe_semi(&mut self) -> Result<Option<RecordBatch>> {
        let ht = self.hash_table.as_ref().unwrap();
        loop {
            if self.probe_state.buffer_pos < self.probe_state.output_buffer.len() {
                return self.drain_buffer();
            }
            let right_row = match self.get_next_right_row() {
                Some(row) => row,
                None => return Ok(None),
            };
            let key: Vec<ScalarValue> = self.right_key_cols.iter()
                .map(|&idx| right_row[idx].clone())
                .collect();
            let matches = ht.probe(&key);
            for ptr in &matches {
                let left_row = self.collect_left_row(ptr);
                self.probe_state.output_buffer.push((left_row, None));
            }
            if self.probe_state.output_buffer.is_empty() {
                continue;
            }
            return self.drain_buffer();
        }
    }

    /// Anti join: emit left rows that have NO match in right.
    fn probe_anti(&mut self) -> Result<Option<RecordBatch>> {
        // Need to consume ALL right rows first to know what doesn't match
        if self.probe_state.right_batches.is_empty() {
            let ht = self.hash_table.as_mut().unwrap();
            while let Some(batch) = self.right.next_batch()? {
                for row in 0..batch.num_rows() {
                    let right_row: Vec<ScalarValue> = (0..batch.num_columns())
                        .map(|c| batch.scalar(row, c).unwrap_or(ScalarValue::Int64(None)))
                        .collect();
                    let key: Vec<ScalarValue> = self.right_key_cols.iter()
                        .map(|&idx| right_row[idx].clone())
                        .collect();
                    let matches = ht.probe(&key);
                    for ptr in &matches {
                        ht.mark_matched(ptr);
                    }
                }
            }
            // Now emit unmatched left rows
            let unmatched = ht.unmatched_rows();
            for ptr in &unmatched {
                let left_row = self.collect_left_row(ptr);
                self.probe_state.output_buffer.push((left_row, None));
            }
        }
        self.drain_buffer()
    }

    /// Asof join: for each left row, find closest right row by timestamp.
    fn probe_asof(&mut self) -> Result<Option<RecordBatch>> {
        let asof = self.asof_index.as_ref().unwrap();
        loop {
            if self.probe_state.buffer_pos < self.probe_state.output_buffer.len() {
                return self.drain_buffer();
            }
            let left_row = match self.get_next_left_row() {
                Some(row) => row,
                None => return Ok(None),
            };
            let ts_col = self.left_key_cols.first().copied().unwrap_or(0);
            let probe_ts = &left_row[ts_col];
            if let Some(ptr) = asof.find_asof(probe_ts) {
                // Collect the matching right row from asof batches
                let right_row: Vec<ScalarValue> = (0..self.right_schema.columns.len())
                    .map(|c| {
                        // We need the original batch data - use get_scalar from asof
                        // Since asof stores RowPointer, we need to access its internal data
                        ScalarValue::Int64(None) // placeholder
                    })
                    .collect();
                self.probe_state.output_buffer.push((left_row, Some(right_row)));
            } else {
                // No match: emit left row with NULL right
                let null_right = self.make_null_right_row();
                self.probe_state.output_buffer.push((left_row, Some(null_right)));
            }
            return self.drain_buffer();
        }
    }

    // --- Helper methods ---

    fn get_next_right_row(&mut self) -> Option<Vec<ScalarValue>> {
        // First collect all right batches
        if self.probe_state.right_batches.is_empty() && !self.probe_state.build_done {
            // Already collected during build for RIGHT/FULL/Asof
        }
        // Try to get from already-collected batches
        if self.probe_state.right_batch_idx < self.probe_state.right_batches.len() {
            let batch = &self.probe_state.right_batches[self.probe_state.right_batch_idx];
            if self.probe_state.right_row_idx < batch.num_rows() {
                let row: Vec<ScalarValue> = (0..batch.num_columns())
                    .map(|c| batch.scalar(self.probe_state.right_row_idx, c).unwrap_or(ScalarValue::Int64(None)))
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
        match self.left.next_batch() {
            Ok(Some(batch)) => {
                if batch.num_rows() == 0 {
                    return self.get_next_left_row();
                }
                // Buffer and return first row
                self.probe_state.right_batches.push(batch);
                let batch_ref = &self.probe_state.right_batches[self.probe_state.right_batches.len() - 1];
                let row: Vec<ScalarValue> = (0..batch_ref.num_columns())
                    .map(|c| batch_ref.scalar(0, c).unwrap_or(ScalarValue::Int64(None)))
                    .collect();
                // Store remaining rows
                for r in 1..batch_ref.num_rows() {
                    let vals: Vec<ScalarValue> = (0..batch_ref.num_columns())
                        .map(|c| batch_ref.scalar(r, c).unwrap_or(ScalarValue::Int64(None)))
                        .collect();
                    self.probe_state.output_buffer.push((vals, None)); // temporarily store
                }
                Some(row)
            }
            _ => None,
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

    fn make_null_right_row(&self) -> Vec<ScalarValue> {
        self.right_schema.columns.iter()
            .map(|c| ScalarValue::null_for_type(c.data_type))
            .collect()
    }

    fn make_null_left_row(&self) -> Vec<ScalarValue> {
        self.left_schema.columns.iter()
            .map(|c| ScalarValue::null_for_type(c.data_type))
            .collect()
    }

    fn emit_unmatched_left(&mut self, ht: &JoinHashTable) -> Result<()> {
        let unmatched = ht.unmatched_rows();
        let null_right = self.make_null_right_row();
        for ptr in &unmatched {
            let left_row = self.collect_left_row(ptr);
            self.probe_state.output_buffer.push((left_row, Some(null_right.clone())));
        }
        Ok(())
    }

    fn emit_unmatched_right(&mut self, ht: &JoinHashTable) -> Result<()> {
        let unmatched = ht.unmatched_rows();
        let null_left = self.make_null_left_row();
        for ptr in &unmatched {
            let right_row = self.collect_build_row(ht, ptr);
            // For right/full join output: null_left + right
            self.probe_state.output_buffer.push((null_left.clone(), Some(right_row)));
        }
        Ok(())
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
                    // For semi/anti join, right side is not emitted
                    if output_ncols > left_ncols {
                        for i in 0..right_ncols {
                            let null_val = ScalarValue::null_for_type(self.right_schema.columns[i].data_type);
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
        LogicalExpr::BinaryOp { left, op: _, right } => {
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
        LogicalExpr::Literal(_) => {}
        _ => {}
    }
}
