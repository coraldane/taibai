use crate::common::error::Result;
use crate::protocol::types::DataType;
use crate::sql::planner::LogicalExpr;
use crate::sql::execution::operators::filter::{eval_expr_column, remap_expr_for_batch};
use crate::types::array::ColumnArray;
use crate::types::batch::{ColumnDef, RecordBatch, Schema};

use super::ExecutionOp;

pub struct ProjectExec {
    pub expressions: Vec<LogicalExpr>,
    pub aliases: Vec<Option<String>>,
    pub input: Box<dyn ExecutionOp>,
    pub input_schema: Schema,
}

impl ExecutionOp for ProjectExec {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        match self.input.next_batch()? {
            Some(batch) => {
                let mut columns = Vec::with_capacity(self.expressions.len());
                let mut col_defs = Vec::with_capacity(self.expressions.len());

                for (i, expr) in self.expressions.iter().enumerate() {
                    let expr = remap_expr_for_batch(expr, &self.input_schema, &batch.schema);
                    let vals = eval_expr_column(&expr, &batch)?;
                    let alias = self.aliases[i].as_deref().unwrap_or("expr");
                    let dt = infer_type(&expr, &batch.schema);

                    let mut col = ColumnArray::empty(dt);
                    for v in &vals {
                        col.push(v)?;
                    }
                    columns.push(col);
                    col_defs.push(ColumnDef::new(alias, dt));
                }

                let schema = Schema::new(col_defs);
                Ok(Some(RecordBatch::try_new(schema, columns)?))
            }
            None => Ok(None),
        }
    }
}

pub fn infer_type(expr: &LogicalExpr, schema: &Schema) -> DataType {
    match expr {
        LogicalExpr::Column(idx) => schema.columns.get(*idx).map(|c| c.data_type).unwrap_or(DataType::Int64),
        LogicalExpr::Literal(v) => v.data_type(),
        LogicalExpr::BinaryOp { .. } => DataType::Int64,
        LogicalExpr::Aggregate(agg) => {
            match agg.func {
                crate::sql::planner::AggFunc::Count => DataType::Int64,
                _ => DataType::Float64,
            }
        }
        _ => DataType::Int64,
    }
}
