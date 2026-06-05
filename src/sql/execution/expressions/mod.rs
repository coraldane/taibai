#![allow(dead_code)]

pub mod aggregate;
pub mod arithmetic;
pub mod cast;
pub mod column_ref;
pub mod comparison;
pub mod literal;
pub mod logical;

use crate::common::error::Result;
use crate::types::array::ColumnArray;
use crate::types::batch::RecordBatch;

pub trait ColumnExpression: Send + Sync {
    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnArray>;
    fn data_type(&self) -> crate::protocol::types::DataType;
}
