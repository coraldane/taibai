use crate::common::error::Result;
use crate::protocol::types::DataType;
use crate::types::array::ColumnArray;
use crate::types::batch::RecordBatch;
use crate::types::scalar::ScalarValue;

use super::ColumnExpression;

pub struct LiteralExpression {
    pub value: ScalarValue,
}

impl LiteralExpression {
    pub fn new(value: ScalarValue) -> Self {
        Self { value }
    }
}

impl ColumnExpression for LiteralExpression {
    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnArray> {
        let num_rows = batch.num_rows();
        let mut col = ColumnArray::empty(self.value.data_type());
        for _ in 0..num_rows {
            col.push(&self.value)?;
        }
        Ok(col)
    }

    fn data_type(&self) -> DataType {
        self.value.data_type()
    }
}
