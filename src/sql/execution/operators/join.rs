use std::collections::HashMap;

use crate::common::error::Result;
use crate::sql::parser::ast::JoinType;
use crate::sql::planner::LogicalExpr;
use crate::types::batch::{RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

use super::ExecutionOp;
use super::limit::trim_batch;

#[allow(dead_code)]
pub struct HashJoinExec {
    pub left: Box<dyn ExecutionOp>,
    pub right: Box<dyn ExecutionOp>,
    pub join_type: JoinType,
    pub condition: LogicalExpr,
    pub left_schema: Schema,
    pub right_schema: Schema,
    pub output_schema: Schema,
    hash_table: HashMap<String, Vec<usize>>,
    left_batches: Vec<RecordBatch>,
    right_batch_idx: usize,
    right_row_idx: usize,
    build_done: bool,
    left_matched: Vec<bool>,
}

impl HashJoinExec {
    #[allow(dead_code)]
    pub fn new(
        left: Box<dyn ExecutionOp>,
        right: Box<dyn ExecutionOp>,
        join_type: JoinType,
        condition: LogicalExpr,
        left_schema: Schema,
        right_schema: Schema,
        output_schema: Schema,
    ) -> Self {
        Self {
            left, right, join_type, condition,
            left_schema, right_schema, output_schema,
            hash_table: HashMap::new(),
            left_batches: Vec::new(),
            right_batch_idx: 0,
            right_row_idx: 0,
            build_done: false,
            left_matched: Vec::new(),
        }
    }
}

impl ExecutionOp for HashJoinExec {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if !self.build_done {
            while let Some(batch) = self.left.next_batch()? {
                self.left_batches.push(batch);
            }
            self.build_done = true;
            let total_left_rows: usize = self.left_batches.iter().map(|b| b.num_rows()).sum();
            self.left_matched = vec![false; total_left_rows];
        }

        Ok(None)
    }
}
