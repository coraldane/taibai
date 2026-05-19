use crate::common::error::Result;
use crate::protocol::types::DataType;
use crate::sql::planner::LogicalExpr;
use crate::sql::execution::operators::filter::eval_expr_column;
use crate::sql::execution::operators::project::infer_type;
use crate::types::array::ColumnArray;
use crate::types::batch::{ColumnDef, RecordBatch, Schema};

use super::PushOp;

pub struct PushProject {
    expressions: Vec<LogicalExpr>,
    aliases: Vec<Option<String>>,
    buffer: Vec<RecordBatch>,
}

impl PushProject {
    pub fn new(expressions: Vec<LogicalExpr>, aliases: Vec<Option<String>>) -> Self {
        Self { expressions, aliases, buffer: Vec::new() }
    }
}

impl PushOp for PushProject {
    fn push(&mut self, batch: RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let mut columns = Vec::with_capacity(self.expressions.len());
        let mut col_defs = Vec::with_capacity(self.expressions.len());
        for (i, expr) in self.expressions.iter().enumerate() {
            let vals = eval_expr_column(expr, &batch)?;
            let alias = self.aliases[i].as_deref().unwrap_or("expr");
            let dt = infer_type(expr, &batch.schema);
            let mut col = ColumnArray::empty(dt);
            for v in &vals { col.push(v)?; }
            columns.push(col);
            col_defs.push(ColumnDef::new(alias, dt));
        }
        let schema = Schema::new(col_defs);
        let projected = RecordBatch::try_new(schema, columns)?;
        self.buffer.push(projected);
        Ok(())
    }

    fn finalize(self: Box<Self>) -> Result<Option<RecordBatch>> {
        if self.buffer.is_empty() {
            return Ok(None);
        }
        if self.buffer.len() == 1 {
            return Ok(Some(self.buffer.into_iter().next().unwrap()));
        }
        let schema = self.buffer[0].schema.clone();
        let num_cols = schema.columns.len();
        let mut columns = Vec::with_capacity(num_cols);
        for col_idx in 0..num_cols {
            let dt = schema.columns[col_idx].data_type;
            let mut col = ColumnArray::empty(dt);
            for batch in &self.buffer {
                if let Some(src) = batch.column(col_idx) {
                    for i in 0..batch.num_rows() {
                        col.push(&src.scalar(i)).unwrap();
                    }
                }
            }
            columns.push(col);
        }
        Ok(Some(RecordBatch::try_new(schema, columns).unwrap()))
    }
}
