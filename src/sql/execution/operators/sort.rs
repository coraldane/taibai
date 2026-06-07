use crate::common::error::Result;
use crate::sql::execution::operators::filter::{eval_expr_column, remap_expr_for_batch};
use crate::sql::planner::{LogicalExpr, SortKey};
use crate::types::batch::{RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

use super::ExecutionOp;

pub struct SortExec {
    pub sort_keys: Vec<SortKey>,
    pub input: Box<dyn ExecutionOp>,
    pub schema: Schema,
    pub limit: Option<usize>,
    done: bool,
}

struct SortRow {
    values: Vec<ScalarValue>,
    keys: Vec<ScalarValue>,
}

impl SortExec {
    pub fn new(sort_keys: Vec<SortKey>, input: Box<dyn ExecutionOp>, schema: Schema) -> Self {
        Self {
            sort_keys,
            input,
            schema,
            limit: None,
            done: false,
        }
    }

    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }
}

impl ExecutionOp for SortExec {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.done {
            return Ok(None);
        }
        self.done = true;

        let mut all_batches = Vec::new();
        while let Some(batch) = self.input.next_batch()? {
            all_batches.push(batch);
        }
        if all_batches.is_empty() {
            return Ok(None);
        }
        let output_schema = all_batches[0].schema.clone();

        let total_rows: usize = all_batches.iter().map(|b| b.num_rows()).sum();
        if total_rows == 0 {
            return Ok(Some(RecordBatch::new(output_schema)));
        }

        let sort_exprs: Vec<(LogicalExpr, bool)> = self
            .sort_keys
            .iter()
            .map(|sk| {
                (
                    remap_expr_for_batch(&sk.expr, &self.schema, &output_schema),
                    sk.desc,
                )
            })
            .collect();

        let mut rows = Vec::with_capacity(total_rows);
        for batch in &all_batches {
            let evaluated_keys = evaluate_batch_sort_keys(&sort_exprs, batch)?;
            for row in 0..batch.num_rows() {
                let values: Vec<ScalarValue> = (0..batch.num_columns())
                    .map(|c| batch.scalar(row, c).unwrap_or(ScalarValue::Int64(None)))
                    .collect();
                let keys = evaluated_keys
                    .iter()
                    .map(|vals| vals[row].clone())
                    .collect();
                rows.push(SortRow { values, keys });
            }
        }

        if let Some(limit) = self.limit {
            topn_sort(&mut rows, &sort_exprs, limit);
            rows.truncate(limit);
        } else {
            rows.sort_by(|a, b| compare_rows(a, b, &sort_exprs));
        }

        let mut batch = RecordBatch::new(output_schema);
        for row in &rows {
            for (col_idx, val) in row.values.iter().enumerate() {
                batch.column_mut(col_idx).unwrap().push(val)?;
            }
        }
        Ok(Some(batch))
    }
}

fn evaluate_batch_sort_keys(
    sort_exprs: &[(LogicalExpr, bool)],
    batch: &RecordBatch,
) -> Result<Vec<Vec<ScalarValue>>> {
    sort_exprs
        .iter()
        .map(|(expr, _)| eval_expr_column(expr, batch))
        .collect()
}

fn topn_sort(rows: &mut [SortRow], sort_exprs: &[(LogicalExpr, bool)], limit: usize) {
    if rows.len() <= limit {
        rows.sort_by(|a, b| compare_rows(a, b, sort_exprs));
        return;
    }

    rows.select_nth_unstable_by(limit, |a, b| compare_rows(a, b, sort_exprs));
    rows[..limit].sort_by(|a, b| compare_rows(a, b, sort_exprs));
}

fn compare_rows(
    a: &SortRow,
    b: &SortRow,
    sort_exprs: &[(LogicalExpr, bool)],
) -> std::cmp::Ordering {
    for (idx, (_, desc)) in sort_exprs.iter().enumerate() {
        let cmp = compare_scalars(&a.keys[idx], &b.keys[idx]);
        let cmp = if *desc { cmp.reverse() } else { cmp };
        if cmp != std::cmp::Ordering::Equal {
            return cmp;
        }
    }
    std::cmp::Ordering::Equal
}

pub fn compare_scalars(a: &ScalarValue, b: &ScalarValue) -> std::cmp::Ordering {
    if a.is_null() && b.is_null() {
        return std::cmp::Ordering::Equal;
    }
    if a.is_null() {
        return std::cmp::Ordering::Less;
    }
    if b.is_null() {
        return std::cmp::Ordering::Greater;
    }
    match (a, b) {
        _ if a.numeric_cmp(b).is_some() => a.numeric_cmp(b).unwrap_or(std::cmp::Ordering::Equal),
        (ScalarValue::Int64(Some(l)), ScalarValue::Int64(Some(r))) => l.cmp(r),
        (ScalarValue::Int32(Some(l)), ScalarValue::Int32(Some(r))) => l.cmp(r),
        (ScalarValue::Int16(Some(l)), ScalarValue::Int16(Some(r))) => l.cmp(r),
        (ScalarValue::Int8(Some(l)), ScalarValue::Int8(Some(r))) => l.cmp(r),
        (ScalarValue::UInt64(Some(l)), ScalarValue::UInt64(Some(r))) => l.cmp(r),
        (ScalarValue::UInt32(Some(l)), ScalarValue::UInt32(Some(r))) => l.cmp(r),
        (ScalarValue::UInt16(Some(l)), ScalarValue::UInt16(Some(r))) => l.cmp(r),
        (ScalarValue::UInt8(Some(l)), ScalarValue::UInt8(Some(r))) => l.cmp(r),
        (ScalarValue::Float64(Some(l)), ScalarValue::Float64(Some(r))) => {
            l.partial_cmp(r).unwrap_or(std::cmp::Ordering::Equal)
        }
        (ScalarValue::Float32(Some(l)), ScalarValue::Float32(Some(r))) => {
            l.partial_cmp(r).unwrap_or(std::cmp::Ordering::Equal)
        }
        (ScalarValue::VarChar(Some(l)), ScalarValue::VarChar(Some(r))) => l.cmp(r),
        (ScalarValue::Date(Some(l)), ScalarValue::Date(Some(r))) => l.cmp(r),
        (ScalarValue::Timestamp(Some(l)), ScalarValue::Timestamp(Some(r))) => l.cmp(r),
        (ScalarValue::Int64(Some(l)), ScalarValue::Int32(Some(r))) => l.cmp(&(*r as i64)),
        (ScalarValue::Int32(Some(l)), ScalarValue::Int64(Some(r))) => (*l as i64).cmp(r),
        (ScalarValue::Float64(Some(l)), ScalarValue::Int64(Some(r))) => l
            .partial_cmp(&(*r as f64))
            .unwrap_or(std::cmp::Ordering::Equal),
        (ScalarValue::Int64(Some(l)), ScalarValue::Float64(Some(r))) => (*l as f64)
            .partial_cmp(r)
            .unwrap_or(std::cmp::Ordering::Equal),
        _ => std::cmp::Ordering::Equal,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::types::DataType;
    use crate::sql::parser::ast::BinOp;
    use crate::sql::planner::SortKey;
    use crate::types::array::ColumnArray;
    use crate::types::batch::ColumnDef;

    struct OneBatchExec {
        batch: Option<RecordBatch>,
    }

    impl ExecutionOp for OneBatchExec {
        fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
            Ok(self.batch.take())
        }
    }

    #[test]
    fn sorts_by_expression_not_first_column() {
        let schema = Schema::new(vec![
            ColumnDef::new("id", DataType::Int64),
            ColumnDef::new("close_px", DataType::Float64),
            ColumnDef::new("pre_close", DataType::Float64),
        ]);
        let mut id = ColumnArray::empty(DataType::Int64);
        let mut close = ColumnArray::empty(DataType::Float64);
        let mut pre = ColumnArray::empty(DataType::Float64);
        for (i, c, p) in [(3, 11.0, 10.0), (1, 20.0, 10.0), (2, 9.0, 10.0)] {
            id.push(&ScalarValue::Int64(Some(i))).unwrap();
            close.push(&ScalarValue::Float64(Some(c))).unwrap();
            pre.push(&ScalarValue::Float64(Some(p))).unwrap();
        }
        let batch = RecordBatch::try_new(schema.clone(), vec![id, close, pre]).unwrap();
        let expr = LogicalExpr::BinaryOp {
            left: Box::new(LogicalExpr::BinaryOp {
                left: Box::new(LogicalExpr::Column(1)),
                op: BinOp::Sub,
                right: Box::new(LogicalExpr::Column(2)),
            }),
            op: BinOp::Div,
            right: Box::new(LogicalExpr::Column(2)),
        };
        let mut exec = SortExec::new(
            vec![SortKey { expr, desc: true }],
            Box::new(OneBatchExec { batch: Some(batch) }),
            schema,
        )
        .with_limit(2);
        let result = exec.next_batch().unwrap().unwrap();
        assert_eq!(result.scalar(0, 0), Some(ScalarValue::Int64(Some(1))));
        assert_eq!(result.scalar(1, 0), Some(ScalarValue::Int64(Some(3))));
    }
}
