use std::collections::HashMap;

use crate::common::error::Result;
use crate::protocol::types::DataType;
use crate::sql::execution::operators::aggregate::compute_aggregate;
use crate::sql::planner::{AggFunc, AggregateExpr, LogicalExpr};
use crate::types::batch::{ColumnDef, RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

use super::PushOp;

pub struct PushHashAggregate {
    group_by: Vec<LogicalExpr>,
    aggregates: Vec<AggregateExpr>,
    input_schema: Schema,
    groups: HashMap<Vec<String>, Vec<Vec<ScalarValue>>>,
}

impl PushHashAggregate {
    pub fn new(
        group_by: Vec<LogicalExpr>,
        aggregates: Vec<AggregateExpr>,
        input_schema: Schema,
    ) -> Self {
        Self {
            group_by,
            aggregates,
            input_schema,
            groups: HashMap::new(),
        }
    }
}

impl PushOp for PushHashAggregate {
    fn push(&mut self, batch: RecordBatch) -> Result<()> {
        for row in 0..batch.num_rows() {
            let vals: Vec<ScalarValue> = (0..batch.num_columns())
                .map(|c| batch.scalar(row, c).unwrap_or(ScalarValue::Int64(None)))
                .collect();
            let key: Vec<String> = self
                .group_by
                .iter()
                .map(|expr| match expr {
                    LogicalExpr::Column(idx) => {
                        vals.get(*idx).map(|v| format!("{}", v)).unwrap_or_default()
                    }
                    _ => String::new(),
                })
                .collect();
            self.groups.entry(key).or_default().push(vals);
        }
        Ok(())
    }

    fn finalize(self: Box<Self>) -> Result<Option<RecordBatch>> {
        if self.groups.is_empty() {
            return Ok(None);
        }
        let mut col_defs = Vec::new();
        for expr in &self.group_by {
            if let LogicalExpr::Column(idx) = expr {
                if let Some(cd) = self.input_schema.columns.get(*idx) {
                    col_defs.push(
                        ColumnDef::new(&cd.name, cd.data_type)
                            .with_scale(cd.scale)
                            .with_precision(cd.precision),
                    );
                }
            }
        }
        for agg in &self.aggregates {
            let name = format!(
                "{:?}({})",
                agg.func,
                match &*agg.arg {
                    LogicalExpr::Wildcard => "*".into(),
                    LogicalExpr::Column(idx) => self
                        .input_schema
                        .columns
                        .get(*idx)
                        .map(|c| c.name.clone())
                        .unwrap_or_default(),
                    _ => "expr".into(),
                }
            );
            let dt = aggregate_result_type(agg, &self.input_schema);
            col_defs.push(ColumnDef::new(&name, dt));
        }
        let schema = Schema::new(col_defs);
        let mut batch = RecordBatch::new(schema);
        for rows in self.groups.values() {
            let mut row_vals = Vec::new();
            for expr in &self.group_by {
                if let LogicalExpr::Column(idx) = expr {
                    row_vals.push(
                        rows.first()
                            .and_then(|r| r.get(*idx))
                            .cloned()
                            .unwrap_or(ScalarValue::Int64(None)),
                    );
                }
            }
            for agg in &self.aggregates {
                row_vals.push(compute_aggregate(agg, rows, &self.input_schema));
            }
            for (col_idx, val) in row_vals.iter().enumerate() {
                batch.column_mut(col_idx).unwrap().push(val)?;
            }
        }
        Ok(Some(batch))
    }
}

fn aggregate_result_type(agg: &AggregateExpr, schema: &Schema) -> DataType {
    match agg.func {
        AggFunc::Count => DataType::Int64,
        AggFunc::Sum => match &*agg.arg {
            LogicalExpr::Column(idx) => schema
                .columns
                .get(*idx)
                .map(|c| match c.data_type {
                    DataType::Float32 | DataType::Float64 => DataType::Float64,
                    DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::UInt8
                    | DataType::UInt16
                    | DataType::UInt32
                    | DataType::UInt64 => DataType::Int64,
                    other => other,
                })
                .unwrap_or(DataType::Int64),
            _ => DataType::Int64,
        },
        AggFunc::Avg => DataType::Float64,
        AggFunc::Min | AggFunc::Max => match &*agg.arg {
            LogicalExpr::Column(idx) => schema
                .columns
                .get(*idx)
                .map(|c| c.data_type)
                .unwrap_or(DataType::Int64),
            _ => DataType::Int64,
        },
    }
}
