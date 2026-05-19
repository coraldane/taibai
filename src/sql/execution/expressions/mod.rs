pub mod column_ref;
pub mod literal;
pub mod comparison;
pub mod arithmetic;
pub mod logical;
pub mod aggregate;
pub mod cast;

use crate::common::error::Result;
use crate::types::batch::RecordBatch;
use crate::types::array::ColumnArray;

pub trait ColumnExpression: Send + Sync {
    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnArray>;
    fn data_type(&self) -> crate::protocol::types::DataType;
}
