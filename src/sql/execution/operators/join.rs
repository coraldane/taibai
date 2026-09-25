/// Hash Join Execution Operator - DuckDB-inspired Sink/Probe two-phase design.
/// Supports: Inner, Left, Right, Full, Cross, Semi, Anti, and Asof joins.
/// Uses columnar data processing through RecordBatch for cache efficiency.
use crate::common::error::{Result, TaibaiError};
use crate::protocol::types::DataType;
use crate::sql::execution::functions::registry::FunctionRegistry;
use crate::sql::execution::join_hashtable::{AsofJoinIndex, AsofOp, JoinHashTable, RowPointer};
use crate::sql::execution::spill::{MemoryManager, MemoryReservation};
use crate::sql::parser::ast::BinOp;
use crate::sql::parser::ast::JoinType;
use crate::sql::planner::LogicalExpr;
use crate::types::array::ColumnArray;
use crate::types::batch::{RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

use super::join_spill::PartitionedInnerJoin;
use super::{ExecutionOp, RuntimeInFilter};

/// Decomposed ASOF join condition: equality keys plus exactly one time-key
/// inequality (`l.ts <op> r.ts`).
#[derive(Debug, Clone)]
struct AsofPlan {
    eq_left_cols: Vec<usize>,
    eq_right_cols: Vec<usize>,
    left_time_col: usize,
    right_time_col: usize,
    op: AsofOp,
}

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
    memory_reservation: Option<MemoryReservation>,
    asof_index: Option<AsofJoinIndex>,
    asof_plan: Option<AsofPlan>,
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
    // --- fast probe path (pure-equality conditions) ---
    /// Enabled when the condition is a pure conjunction of column = column
    /// pairs and no spill path was taken; see `condition_is_pure_eq`.
    fast_enabled: bool,
    /// (probe row, build row pointer) matches for the active probe batch.
    fast_pairs: Vec<(u32, RowPointer)>,
    fast_pairs_pos: usize,
    /// Index of the active probe batch within `right_batches`/`left_batches`.
    fast_probe_batch_idx: usize,
    /// Scratch buffers reused across batches to keep the hot loop
    /// allocation-free apart from the match list growth.
    pair_scratch: Vec<RowPointer>,
    probe_rows_scratch: Vec<u32>,
    build_rows_scratch: Vec<u32>,
}

/// Which input feeds the hash-table build side in the fast probe path.
#[derive(Clone, Copy, PartialEq, Eq)]
enum FastJoinPlan {
    /// Build = left input, probe = right input.
    BuildLeftProbeRight,
    /// Build = right input, probe = left input.
    BuildRightProbeLeft,
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
            memory_reservation: None,
            asof_index: None,
            asof_plan: None,
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
                    crate::common::query_cancel::check()?;
                    ht.insert_batch(batch);
                }
                ht.finalize();
                self.probe_state.fast_enabled = self.condition_is_pure_eq();
                self.hash_table = Some(ht);
            }
            JoinType::Asof | JoinType::LeftAsof => {
                // Asof join: build the partitioned sorted index from the
                // right side; matched rows are fetched from the same batches.
                let plan = self.asof_join_plan()?;
                let mut batches = Vec::new();
                while let Some(batch) = self.right.next_batch()? {
                    crate::common::query_cancel::check()?;
                    batches.push(batch);
                }
                let mut asof_idx = AsofJoinIndex::new();
                asof_idx.build(batches, &plan.eq_right_cols, plan.right_time_col);
                self.asof_plan = Some(plan);
                self.asof_index = Some(asof_idx);
            }
            _ => {
                if self.join_type == JoinType::Cross {
                    self.probe_state.build_done = true;
                    return Ok(());
                }
                let mut ht = JoinHashTable::new(self.left_key_cols.clone());
                while let Some(batch) = self.left.next_batch()? {
                    crate::common::query_cancel::check()?;
                    ht.insert_batch(batch);
                }
                ht.finalize();
                self.inject_right_runtime_filter(&ht);
                self.probe_state.fast_enabled = self.condition_is_pure_eq();
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
        let mut memory = MemoryManager::reservation();

        loop {
            crate::common::query_cancel::check()?;
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
            if estimated_bytes >= self.memory_limit_bytes || !memory.try_resize(estimated_bytes) {
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
        self.memory_reservation = Some(memory);
        self.probe_state.fast_enabled = self.condition_is_pure_eq();
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
            JoinType::Inner if self.probe_state.fast_enabled => {
                let plan = if self.build_left {
                    FastJoinPlan::BuildLeftProbeRight
                } else {
                    FastJoinPlan::BuildRightProbeLeft
                };
                self.probe_fast(plan, false)
            }
            JoinType::Left if self.probe_state.fast_enabled => {
                self.probe_fast(FastJoinPlan::BuildLeftProbeRight, false)
            }
            JoinType::Right if self.probe_state.fast_enabled => {
                self.probe_fast(FastJoinPlan::BuildRightProbeLeft, false)
            }
            JoinType::Semi if self.probe_state.fast_enabled => {
                self.probe_fast(FastJoinPlan::BuildLeftProbeRight, true)
            }
            JoinType::Asof | JoinType::LeftAsof => self.probe_asof(),
            JoinType::Inner => self.probe_inner(),
            JoinType::Left => self.probe_left(),
            JoinType::Right => self.probe_right(),
            JoinType::Full => self.probe_full(),
            JoinType::Cross => self.probe_cross(),
            JoinType::Semi => self.probe_semi(),
            JoinType::Anti => self.probe_anti(),
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

                        ht.probe(&key)
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

    /// Asof join: for each left row, binary-search the equality-key
    /// partition for the right row selected by the time-key direction.
    /// `ASOF JOIN` drops unmatched left rows; `LEFT ASOF JOIN` emits them
    /// with a NULL right side. NULL equality/time keys never match.
    fn probe_asof(&mut self) -> Result<Option<RecordBatch>> {
        if self.probe_state.buffer_pos < self.probe_state.output_buffer.len() {
            return self.drain_buffer();
        }
        let left_preserving = self.join_type == JoinType::LeftAsof;
        loop {
            if self.probe_state.output_buffer.len() >= Self::BATCH_SIZE {
                return self.drain_buffer();
            }
            let left_row = match self.get_next_left_row() {
                Some(row) => row,
                None => {
                    if self.probe_state.output_buffer.is_empty() {
                        return Ok(None);
                    }
                    return self.drain_buffer();
                }
            };

            let matched = {
                let asof = self.asof_index.as_ref().unwrap();
                let plan = self.asof_plan.as_ref().expect("asof plan built");
                asof.partition_for_scalars(&left_row, &plan.eq_left_cols)
                    .and_then(|entries| {
                        let probe_ts = &left_row[plan.left_time_col];
                        if probe_ts.is_null() {
                            None
                        } else {
                            asof.find_asof(entries, probe_ts, plan.op)
                        }
                    })
            };

            match matched {
                Some(ptr) => {
                    let right_row: Vec<ScalarValue> = (0..self.right_schema.columns.len())
                        .map(|c| self.asof_index.as_ref().unwrap().get_scalar(&ptr, c))
                        .collect();
                    self.probe_state
                        .output_buffer
                        .push((left_row, Some(right_row)));
                }
                None => {
                    if left_preserving {
                        let null_right = self.make_null_right_row();
                        self.probe_state
                            .output_buffer
                            .push((left_row, Some(null_right)));
                    }
                }
            }
        }
    }

    /// Decompose the ON clause into equality keys plus exactly one time-key
    /// inequality. Fails loudly on shapes ASOF cannot execute.
    fn asof_join_plan(&self) -> Result<AsofPlan> {
        let left_ncols = self.left_schema.columns.len();
        let mut plan = AsofPlan {
            eq_left_cols: Vec::new(),
            eq_right_cols: Vec::new(),
            left_time_col: 0,
            right_time_col: 0,
            op: AsofOp::AtOrBefore,
        };
        let mut has_time = false;

        fn walk(
            expr: &LogicalExpr,
            left_ncols: usize,
            plan: &mut AsofPlan,
            has_time: &mut bool,
        ) -> Result<()> {
            match expr {
                LogicalExpr::BinaryOp {
                    left,
                    op: BinOp::And,
                    right,
                } => {
                    walk(left, left_ncols, plan, has_time)?;
                    walk(right, left_ncols, plan, has_time)
                }
                LogicalExpr::BinaryOp { left, op, right } => {
                    let (LogicalExpr::Column(l), LogicalExpr::Column(r)) =
                        (left.as_ref(), right.as_ref())
                    else {
                        return Err(TaibaiError::Sql(
                            "ASOF JOIN ON conditions must compare plain columns".into(),
                        ));
                    };
                    let (left_col, right_col) = if *l < left_ncols && *r >= left_ncols {
                        (*l, *r - left_ncols)
                    } else if (*r < left_ncols) && (*l >= left_ncols) {
                        (*r, *l - left_ncols)
                    } else {
                        return Err(TaibaiError::Sql(
                            "ASOF JOIN ON conditions must span both join inputs".into(),
                        ));
                    };
                    match op {
                        BinOp::Eq => {
                            plan.eq_left_cols.push(left_col);
                            plan.eq_right_cols.push(right_col);
                            Ok(())
                        }
                        BinOp::Gte | BinOp::Gt | BinOp::Lte | BinOp::Lt => {
                            if *has_time {
                                return Err(TaibaiError::Sql(
                                    "ASOF JOIN accepts exactly one time-key inequality".into(),
                                ));
                            }
                            *has_time = true;
                            plan.left_time_col = left_col;
                            plan.right_time_col = right_col;
                            plan.op = match op {
                                BinOp::Gte => AsofOp::AtOrBefore,
                                BinOp::Gt => AsofOp::Before,
                                BinOp::Lte => AsofOp::AtOrAfter,
                                _ => AsofOp::After,
                            };
                            Ok(())
                        }
                        _ => Err(TaibaiError::Sql(
                            "ASOF JOIN ON supports equality keys and one time-key inequality"
                                .into(),
                        )),
                    }
                }
                _ => Err(TaibaiError::Sql(
                    "ASOF JOIN ON supports equality keys and one time-key inequality".into(),
                )),
            }
        }

        walk(&self.condition, left_ncols, &mut plan, &mut has_time)?;
        if !has_time {
            return Err(TaibaiError::Sql(
                "ASOF JOIN requires one time-key inequality in ON, e.g. l.ts >= r.ts".into(),
            ));
        }
        if plan.eq_left_cols.is_empty() {
            return Err(TaibaiError::Sql(
                "ASOF JOIN requires at least one equality key besides the time key".into(),
            ));
        }
        let time_type_ok = |schema: &Schema, col: usize| {
            matches!(
                schema.columns.get(col).map(|c| c.data_type),
                Some(DataType::Timestamp) | Some(DataType::Int64) | Some(DataType::Float64)
            )
        };
        if !time_type_ok(&self.left_schema, plan.left_time_col)
            || !time_type_ok(&self.right_schema, plan.right_time_col)
        {
            return Err(TaibaiError::Sql(
                "ASOF JOIN time key must be TIMESTAMP, INT64 or DOUBLE".into(),
            ));
        }
        // Mixed key types cannot be compared (units differ, and the typed
        // comparator would degrade every pair to Equal), so require one type.
        let left_time_type = self
            .left_schema
            .columns
            .get(plan.left_time_col)
            .map(|c| c.data_type);
        let right_time_type = self
            .right_schema
            .columns
            .get(plan.right_time_col)
            .map(|c| c.data_type);
        if left_time_type != right_time_type {
            return Err(TaibaiError::Sql(format!(
                "ASOF JOIN time keys must have the same type (left {left_time_type:?}, right {right_time_type:?})"
            )));
        }
        Ok(plan)
    }

    // --- Fast probe path (pure-equality conditions) ---

    /// True when the join condition is exactly a conjunction of
    /// `column = column` equality pairs that the hash table fully covers. For
    /// such conditions a hash match implies the condition holds (keys of the
    /// same variant compare equal), so the per-pair residual evaluation can
    /// be skipped. Float/Blob/Vector keys keep the scalar path: their hash
    /// equality (bit patterns / NULL-ish mapping) is broader than the scalar
    /// equality the residual check applies.
    fn condition_is_pure_eq(&self) -> bool {
        let left_ncols = self.left_schema.columns.len();
        fn walk(expr: &LogicalExpr, left_ncols: usize, leaves: &mut usize) -> bool {
            match expr {
                LogicalExpr::BinaryOp {
                    left,
                    op: BinOp::And,
                    right,
                } => walk(left, left_ncols, leaves) && walk(right, left_ncols, leaves),
                LogicalExpr::BinaryOp {
                    left,
                    op: BinOp::Eq,
                    right,
                } => {
                    *leaves += 1;
                    matches!(
                        (left.as_ref(), right.as_ref()),
                        (LogicalExpr::Column(l), LogicalExpr::Column(r))
                            if (*l < left_ncols) != (*r < left_ncols)
                    )
                }
                _ => false,
            }
        }
        let mut leaves = 0usize;
        if !walk(&self.condition, left_ncols, &mut leaves) {
            return false;
        }
        if leaves == 0 || leaves != self.left_key_cols.len() || leaves != self.right_key_cols.len()
        {
            return false;
        }
        let risky_key = |schema: &Schema, cols: &[usize]| {
            cols.iter().any(|&i| {
                matches!(
                    schema.columns.get(i).map(|c| c.data_type),
                    Some(DataType::Float32)
                        | Some(DataType::Float64)
                        | Some(DataType::Blob)
                        | Some(DataType::Vector)
                )
            })
        };
        !risky_key(&self.left_schema, &self.left_key_cols)
            && !risky_key(&self.right_schema, &self.right_key_cols)
    }

    /// Advance to the next probe batch on the right input; returns its index
    /// in `right_batches`, or `None` when the probe side is exhausted.
    fn next_right_probe_batch(&mut self) -> Option<usize> {
        if !self.probe_state.right_collected {
            while let Ok(Some(batch)) = self.right.next_batch() {
                if batch.num_rows() > 0 {
                    self.probe_state.right_batches.push(batch);
                }
            }
            self.probe_state.right_collected = true;
        }
        let idx = self.probe_state.right_batch_idx;
        if idx < self.probe_state.right_batches.len() {
            self.probe_state.right_batch_idx += 1;
            return Some(idx);
        }
        None
    }

    /// Advance to the next probe batch on the left input (lazily collected).
    fn next_left_probe_batch(&mut self) -> Option<usize> {
        loop {
            let idx = self.probe_state.left_batch_idx;
            if idx < self.probe_state.left_batches.len() {
                self.probe_state.left_batch_idx += 1;
                return Some(idx);
            }
            match self.left.next_batch() {
                Ok(Some(batch)) => {
                    if batch.num_rows() > 0 {
                        self.probe_state.left_batches.push(batch);
                    }
                }
                _ => return None,
            }
        }
    }

    /// Fast probe driver: emit buffered match pairs, scan the next probe
    /// batch when the buffer drains, and run the unmatched-build-side
    /// finalize for LEFT/RIGHT joins once the probe side is exhausted.
    fn probe_fast(&mut self, plan: FastJoinPlan, null_right: bool) -> Result<Option<RecordBatch>> {
        let probe_left = plan == FastJoinPlan::BuildRightProbeLeft;
        loop {
            if self.probe_state.fast_pairs_pos < self.probe_state.fast_pairs.len() {
                return self.emit_fast_chunk(plan, null_right);
            }
            if !self.scan_next_fast_batch(probe_left)? {
                match self.join_type {
                    JoinType::Left => {
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
                    JoinType::Right => {
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
                    _ => return Ok(None),
                }
            }
        }
    }

    /// Fill `fast_pairs` from the next probe batch. Returns `false` when the
    /// probe side is exhausted.
    fn scan_next_fast_batch(&mut self, probe_left: bool) -> Result<bool> {
        let batch_idx = if probe_left {
            self.next_left_probe_batch()
        } else {
            self.next_right_probe_batch()
        };
        let Some(batch_idx) = batch_idx else {
            return Ok(false);
        };
        self.probe_state.fast_probe_batch_idx = batch_idx;
        let key_cols: &[usize] = if probe_left {
            &self.left_key_cols
        } else {
            &self.right_key_cols
        };
        let mark = matches!(self.join_type, JoinType::Left | JoinType::Right);
        let table = self.hash_table.as_mut().unwrap();
        let state = &mut self.probe_state;
        if probe_left {
            let probe_batch = &state.left_batches[batch_idx];
            scan_probe_batch(
                probe_batch,
                key_cols,
                table,
                mark,
                &mut state.fast_pairs,
                &mut state.pair_scratch,
            );
        } else {
            let probe_batch = &state.right_batches[batch_idx];
            scan_probe_batch(
                probe_batch,
                key_cols,
                table,
                mark,
                &mut state.fast_pairs,
                &mut state.pair_scratch,
            );
        }
        Ok(true)
    }

    /// Emit up to BATCH_SIZE match pairs as a columnar batch: probe and build
    /// columns are gathered with a single `take` per column instead of
    /// materializing and re-pushing scalar rows.
    fn emit_fast_chunk(
        &mut self,
        plan: FastJoinPlan,
        null_right: bool,
    ) -> Result<Option<RecordBatch>> {
        let build_is_left = plan == FastJoinPlan::BuildLeftProbeRight;
        let probe_left = !build_is_left;
        let state = &mut self.probe_state;
        let pos = state.fast_pairs_pos;
        if pos >= state.fast_pairs.len() {
            return Ok(None);
        }
        let mut end = (pos + Self::BATCH_SIZE).min(state.fast_pairs.len());
        // Keep the chunk inside one build batch so a single take per column
        // suffices: matches for one probe row can span several build batches,
        // so drop trailing pairs from other batches; they stay buffered for
        // the next call.
        let build_batch_idx = state.fast_pairs[pos].1.batch_idx;
        while end > pos && state.fast_pairs[end - 1].1.batch_idx != build_batch_idx {
            end -= 1;
        }

        let probe_rows = &mut state.probe_rows_scratch;
        probe_rows.clear();
        let build_rows = &mut state.build_rows_scratch;
        build_rows.clear();
        for (row, ptr) in &state.fast_pairs[pos..end] {
            probe_rows.push(*row);
            build_rows.push(ptr.row_idx as u32);
        }

        let table = self.hash_table.as_ref().unwrap();
        let build_batch = &table.build_data[build_batch_idx];
        let probe_batch_idx = state.fast_probe_batch_idx;
        let probe_batch = if probe_left {
            &state.left_batches[probe_batch_idx]
        } else {
            &state.right_batches[probe_batch_idx]
        };
        let rows_in_chunk = end - pos;

        let left_ncols = self.left_schema.columns.len();
        let right_ncols = self.right_schema.columns.len();
        let mut columns: Vec<ColumnArray> = Vec::with_capacity(left_ncols + right_ncols);
        for c in 0..left_ncols {
            let (source, indices) = if build_is_left {
                (build_batch.column(c), &build_rows[..])
            } else {
                (probe_batch.column(c), &probe_rows[..])
            };
            let source = source.expect("output column missing from join input");
            columns.push(source.take(indices));
        }
        for c in 0..right_ncols {
            if null_right {
                columns.push(Self::null_column(
                    self.right_schema.columns[c].data_type,
                    rows_in_chunk,
                )?);
                continue;
            }
            let (source, indices) = if build_is_left {
                (probe_batch.column(c), &probe_rows[..])
            } else {
                (build_batch.column(c), &build_rows[..])
            };
            let source = source.expect("output column missing from join input");
            columns.push(source.take(indices));
        }

        state.fast_pairs_pos = end;
        if state.fast_pairs_pos >= state.fast_pairs.len() {
            state.fast_pairs.clear();
            state.fast_pairs_pos = 0;
        }
        let schema = self.output_schema.clone();
        Ok(Some(RecordBatch::try_new(schema, columns)?))
    }

    fn null_column(data_type: DataType, rows: usize) -> Result<ColumnArray> {
        let mut column = ColumnArray::empty(data_type);
        let null = ScalarValue::null_for_type(data_type);
        for _ in 0..rows {
            column.push(&null)?;
        }
        Ok(column)
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
                self.collect_build_row(ht, ptr)
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

/// Collect (probe row, build pointer) matches for every row of one probe
/// batch, probing typed keys straight from column storage. Rows with NULL
/// join keys are skipped (they never match), mirroring the build side.
fn scan_probe_batch(
    probe_batch: &RecordBatch,
    key_columns: &[usize],
    table: &mut JoinHashTable,
    mark: bool,
    pairs: &mut Vec<(u32, RowPointer)>,
    scratch: &mut Vec<RowPointer>,
) {
    pairs.clear();
    for row in 0..probe_batch.num_rows() {
        let matches = table.probe_batch_row(probe_batch, row, key_columns);
        let Some(matches) = matches else {
            continue;
        };
        scratch.clear();
        scratch.extend_from_slice(matches);
        for ptr in &scratch[..] {
            if mark {
                table.mark_matched(ptr);
            }
            pairs.push((row as u32, *ptr));
        }
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
#[path = "join_test.rs"]
mod tests;
