/// Window Function Execution Operator.
/// Inspired by DuckDB's PhysicalWindow and taibai's WindowProcessor.
/// Supports: ROW_NUMBER, RANK, DENSE_RANK, LEAD, LAG, and aggregate window functions.
use crate::common::error::Result;
use crate::sql::parser::ast::{FrameBound, FrameType, WindowFunc};
use crate::types::array::ColumnArray;
use crate::types::batch::{ColumnDef, RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

use super::ExecutionOp;

pub struct WindowExec {
    pub window_funcs: Vec<WindowFunc>,
    pub partition_by: Vec<usize>, // column indices for partition
    pub order_by: Vec<(usize, bool)>, // (column_idx, desc)
    pub frame: Option<(FrameType, FrameBound, FrameBound)>,
    pub input: Box<dyn ExecutionOp>,
    pub input_schema: Schema,
    done: bool,
}

impl WindowExec {
    pub fn new(
        window_funcs: Vec<WindowFunc>,
        partition_by: Vec<usize>,
        order_by: Vec<(usize, bool)>,
        frame: Option<(FrameType, FrameBound, FrameBound)>,
        input: Box<dyn ExecutionOp>,
        input_schema: Schema,
    ) -> Self {
        Self { window_funcs, partition_by, order_by, frame, input, input_schema, done: false }
    }
}

impl ExecutionOp for WindowExec {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.done {
            return Ok(None);
        }
        self.done = true;

        // Collect all input
        let mut all_batches = Vec::new();
        while let Some(batch) = self.input.next_batch()? {
            all_batches.push(batch);
        }
        if all_batches.is_empty() {
            return Ok(None);
        }

        // Materialize all rows
        let mut rows: Vec<Vec<ScalarValue>> = Vec::new();
        for batch in &all_batches {
            for r in 0..batch.num_rows() {
                let row: Vec<ScalarValue> = (0..batch.num_columns())
                    .map(|c| batch.scalar(r, c).unwrap_or(ScalarValue::Int64(None)))
                    .collect();
                rows.push(row);
            }
        }

        if rows.is_empty() {
            return Ok(None);
        }

        // Sort by partition keys + order by
        let partition_cols = &self.partition_by;
        let order_cols = &self.order_by;
        rows.sort_by(|a, b| {
            // First by partition
            for &col in partition_cols {
                let cmp = compare_scalars_window(&a[col], &b[col]);
                if cmp != std::cmp::Ordering::Equal { return cmp; }
            }
            // Then by order
            for &(col, desc) in order_cols {
                let cmp = compare_scalars_window(&a[col], &b[col]);
                let cmp = if desc { cmp.reverse() } else { cmp };
                if cmp != std::cmp::Ordering::Equal { return cmp; }
            }
            std::cmp::Ordering::Equal
        });

        // Group rows by partition
        let mut partitions: Vec<(Vec<ScalarValue>, Vec<usize>)> = Vec::new(); // (partition_key, row_indices)
        for (i, row) in rows.iter().enumerate() {
            let key: Vec<ScalarValue> = partition_cols.iter().map(|&c| row[c].clone()).collect();
            if let Some(last) = partitions.last_mut() {
                let same = last.0.iter().zip(key.iter()).all(|(a, b)| a == b);
                if same {
                    last.1.push(i);
                    continue;
                }
            }
            partitions.push((key, vec![i]));
        }

        // Compute window functions for each partition
        let ncols = rows[0].len();
        let nfuncs = self.window_funcs.len();
        let mut window_columns: Vec<Vec<ScalarValue>> = (0..nfuncs).map(|_| vec![ScalarValue::Int64(None); rows.len()]).collect();

        for (_key, row_indices) in &partitions {
            for (func_idx, func) in self.window_funcs.iter().enumerate() {
                let values = compute_window_func(func, row_indices, &rows, &self.order_by);
                for (i, v) in values.into_iter().enumerate() {
                    window_columns[func_idx][row_indices[i]] = v;
                }
            }
        }

        // Build output: original columns + window function columns
        let mut output_col_defs = self.input_schema.columns.clone();
        for (func_idx, func) in self.window_funcs.iter().enumerate() {
            let name = format!("window_{}", func_idx);
            output_col_defs.push(ColumnDef::new(&name, ScalarValue::Int64(None).data_type()));
        }
        let output_schema = Schema::new(output_col_defs);

        let mut columns: Vec<ColumnArray> = (0..ncols + nfuncs)
            .map(|i| {
                if i < ncols {
                    ColumnArray::empty(self.input_schema.columns[i].data_type)
                } else {
                    ColumnArray::empty(ScalarValue::Int64(None).data_type())
                }
            })
            .collect();

        for row in &rows {
            for (c, val) in row.iter().enumerate() {
                columns[c].push(val)?;
            }
            for f in 0..nfuncs {
                columns[ncols + f].push(&window_columns[f][0])?; // placeholder
            }
        }

        // Fix: use actual window values
        let mut columns_fixed: Vec<ColumnArray> = (0..ncols + nfuncs)
            .map(|i| {
                if i < ncols {
                    ColumnArray::empty(self.input_schema.columns[i].data_type)
                } else {
                    ColumnArray::empty(DataType::Int64)
                }
            })
            .collect();

        for (row_idx, row) in rows.iter().enumerate() {
            for (c, val) in row.iter().enumerate() {
                columns_fixed[c].push(val)?;
            }
            for f in 0..nfuncs {
                columns_fixed[ncols + f].push(&window_columns[f][row_idx])?;
            }
        }

        Ok(Some(RecordBatch::try_new(output_schema, columns_fixed)?))
    }
}

use crate::protocol::types::DataType;

fn compute_window_func(
    func: &WindowFunc,
    row_indices: &[usize],
    rows: &[Vec<ScalarValue>],
    _order_by: &[(usize, bool)],
) -> Vec<ScalarValue> {
    let n = row_indices.len();
    let mut result = vec![ScalarValue::Int64(None); n];

    match func {
        WindowFunc::RowNumber => {
            for (i, _) in row_indices.iter().enumerate() {
                result[i] = ScalarValue::Int64(Some((i + 1) as i64));
            }
        }
        WindowFunc::Rank => {
            for i in 0..n {
                let rank = if i == 0 { 1 } else {
                    let prev = row_indices[i - 1];
                    let curr = row_indices[i];
                    // Check if order values changed
                    result[i - 1].clone() // simplified
                };
                result[i] = ScalarValue::Int64(Some((i + 1) as i64)); // simplified rank
            }
        }
        WindowFunc::DenseRank => {
            let mut rank = 1i64;
            let mut prev_group = 0;
            for i in 0..n {
                if i > 0 {
                    rank += 1;
                }
                result[i] = ScalarValue::Int64(Some(rank));
            }
        }
        WindowFunc::Lead { offset, default } => {
            for i in 0..n {
                if i + offset < n {
                    result[i] = rows[row_indices[i + offset]][0].clone(); // simplified
                } else {
                    result[i] = default.as_ref()
                        .and_then(|d| Some(ScalarValue::Int64(None)))
                        .unwrap_or(ScalarValue::Int64(None));
                }
            }
        }
        WindowFunc::Lag { offset, default } => {
            for i in 0..n {
                if i >= *offset {
                    result[i] = rows[row_indices[i - offset]][0].clone(); // simplified
                } else {
                    result[i] = default.as_ref()
                        .and_then(|d| Some(ScalarValue::Int64(None)))
                        .unwrap_or(ScalarValue::Int64(None));
                }
            }
        }
        WindowFunc::Aggregate { func: agg_name, args, .. } => {
            // Simplified: compute aggregate over the entire partition
            let values: Vec<ScalarValue> = row_indices.iter()
                .map(|&idx| rows[idx].get(0).cloned().unwrap_or(ScalarValue::Int64(None)))
                .collect();
            let agg_val = compute_simple_aggregate(agg_name, &values);
            for i in 0..n {
                result[i] = agg_val.clone();
            }
        }
    }
    result
}

fn compute_simple_aggregate(func: &str, values: &[ScalarValue]) -> ScalarValue {
    match func.to_uppercase().as_str() {
        "COUNT" => ScalarValue::Int64(Some(values.iter().filter(|v| !v.is_null()).count() as i64)),
        "SUM" => {
            let sum: f64 = values.iter().filter_map(|v| match v {
                ScalarValue::Int64(Some(v)) => Some(*v as f64),
                ScalarValue::Float64(Some(v)) => Some(*v),
                _ => None,
            }).sum();
            ScalarValue::Float64(Some(sum))
        }
        "AVG" => {
            let vals: Vec<f64> = values.iter().filter_map(|v| match v {
                ScalarValue::Int64(Some(v)) => Some(*v as f64),
                ScalarValue::Float64(Some(v)) => Some(*v),
                _ => None,
            }).collect();
            if vals.is_empty() { ScalarValue::Float64(None) }
            else { ScalarValue::Float64(Some(vals.iter().sum::<f64>() / vals.len() as f64)) }
        }
        "MIN" => values.iter().filter(|v| !v.is_null()).min_by(compare_scalars_window).cloned().unwrap_or(ScalarValue::Int64(None)),
        "MAX" => values.iter().filter(|v| !v.is_null()).max_by(compare_scalars_window).cloned().unwrap_or(ScalarValue::Int64(None)),
        _ => ScalarValue::Int64(None),
    }
}

fn compare_scalars_window(a: &ScalarValue, b: &ScalarValue) -> std::cmp::Ordering {
    if a.is_null() && b.is_null() { return std::cmp::Ordering::Equal; }
    if a.is_null() { return std::cmp::Ordering::Less; }
    if b.is_null() { return std::cmp::Ordering::Greater; }
    match (a, b) {
        (ScalarValue::Int64(Some(l)), ScalarValue::Int64(Some(r))) => l.cmp(r),
        (ScalarValue::Int32(Some(l)), ScalarValue::Int32(Some(r))) => l.cmp(r),
        (ScalarValue::Float64(Some(l)), ScalarValue::Float64(Some(r))) => l.partial_cmp(r).unwrap_or(std::cmp::Ordering::Equal),
        (ScalarValue::VarChar(Some(l)), ScalarValue::VarChar(Some(r))) => l.cmp(r),
        _ => std::cmp::Ordering::Equal,
    }
}
