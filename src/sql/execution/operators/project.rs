use crate::common::error::Result;
use crate::protocol::types::DataType;
use crate::sql::execution::operators::filter::{eval_expr_column, remap_expr_for_batch};
use crate::sql::planner::LogicalExpr;
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
                    let dt = infer_type(&expr, &batch.schema);

                    let mut col = ColumnArray::empty(dt);
                    for v in &vals {
                        col.push(v)?;
                    }
                    columns.push(col);
                    col_defs.push(project_column_def(
                        &expr,
                        &batch.schema,
                        self.aliases[i].as_deref(),
                    ));
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
        LogicalExpr::Column(idx) => schema
            .columns
            .get(*idx)
            .map(|c| c.data_type)
            .unwrap_or(DataType::Int64),
        LogicalExpr::Literal(v) => v.data_type(),
        LogicalExpr::BinaryOp { .. } => DataType::Int64,
        LogicalExpr::Aggregate(agg) => match agg.func {
            crate::sql::planner::AggFunc::Count => DataType::Int64,
            _ => DataType::Float64,
        },
        _ => DataType::Int64,
    }
}

pub fn project_column_def(expr: &LogicalExpr, schema: &Schema, alias: Option<&str>) -> ColumnDef {
    if let LogicalExpr::Column(idx) = expr {
        if let Some(source) = schema.columns.get(*idx) {
            let mut col_def = source.clone();
            if let Some(alias) = alias {
                col_def.name = alias.to_string();
            }
            return col_def;
        }
    }

    ColumnDef::new(alias.unwrap_or("expr"), infer_type(expr, schema))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::array::ColumnArray;
    use crate::types::scalar::ScalarValue;

    #[test]
    fn project_preserves_decimal_scale_for_plain_columns() {
        let schema = Schema::new(vec![
            ColumnDef::new("security_id", DataType::VarChar),
            ColumnDef::new("pre_close", DataType::Decimal)
                .with_scale(2)
                .with_precision(9),
        ]);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                ColumnArray::from_scalar_values(
                    DataType::VarChar,
                    &[ScalarValue::VarChar(Some("688256.SH".to_string()))],
                )
                .unwrap(),
                ColumnArray::from_scalar_values(
                    DataType::Decimal,
                    &[ScalarValue::Decimal(Some(1234))],
                )
                .unwrap(),
            ],
        )
        .unwrap();

        let mut exec = ProjectExec {
            expressions: vec![LogicalExpr::Column(1)],
            aliases: vec![None],
            input: Box::new(crate::sql::execution::operators::scan::ScanExec::new(
                vec![batch],
                None,
            )),
            input_schema: schema,
        };

        let result = exec.next_batch().unwrap().unwrap();

        assert_eq!(result.schema.columns[0].name, "pre_close");
        assert_eq!(result.schema.columns[0].data_type, DataType::Decimal);
        assert_eq!(result.schema.columns[0].scale, 2);
        assert_eq!(result.schema.columns[0].precision, 9);
        assert_eq!(
            result.scalar(0, 0).unwrap(),
            ScalarValue::Decimal(Some(1234))
        );
    }
}
