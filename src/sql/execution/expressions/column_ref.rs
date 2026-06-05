use crate::common::error::Result;
use crate::protocol::types::DataType;
use crate::types::array::ColumnArray;
use crate::types::batch::RecordBatch;

use super::ColumnExpression;

pub struct ColumnRefExpression {
    pub index: usize,
    pub data_type: DataType,
}

impl ColumnRefExpression {
    pub fn new(index: usize, data_type: DataType) -> Self {
        Self { index, data_type }
    }
}

impl ColumnExpression for ColumnRefExpression {
    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnArray> {
        Ok(batch.column(self.index).unwrap().clone())
    }

    fn data_type(&self) -> DataType {
        self.data_type
    }
}
