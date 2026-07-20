use std::collections::HashMap;

use crate::common::error::Result;
use crate::protocol::types::DataType;
use crate::sql::parser::ast::{BinOp, JoinType};
use crate::sql::planner::*;
use crate::types::array::ColumnArray;
use crate::types::batch::{ColumnDef, RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

/// A pull-model operator that yields RecordBatches.
pub trait ExecutionOp {
    /// Returns the next batch, or None if exhausted.
    fn next_batch(&mut self) -> Result<Option<RecordBatch>>;
}

// ---------------------------------------------------------------------------
// ScanExec - reads from pre-loaded segment batches
// ---------------------------------------------------------------------------

pub struct ScanExec {
    batches: Vec<RecordBatch>,
    pos: usize,
    projection: Option<Vec<usize>>,
}

impl ScanExec {
    pub fn new(batches: Vec<RecordBatch>, projection: Option<Vec<usize>>) -> Self {
        Self {
            batches,
            pos: 0,
            projection,
        }
    }
}

impl ExecutionOp for ScanExec {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.pos >= self.batches.len() {
            return Ok(None);
        }
        let batch = &self.batches[self.pos];
        self.pos += 1;

        match &self.projection {
            Some(indices) => {
                let schema_cols: Vec<&ColumnDef> =
                    indices.iter().map(|&i| &batch.schema.columns[i]).collect();
                let new_schema = Schema::new(schema_cols.iter().map(|c| (*c).clone()).collect());
                let mut cols = Vec::with_capacity(indices.len());
                for &idx in indices {
                    cols.push(batch.column(idx).unwrap().clone());
                }
                // Fix bitmap lengths to match
                Ok(Some(RecordBatch::try_new(new_schema, cols)?))
            }
            None => Ok(Some(batch.clone())),
        }
    }
}

// ---------------------------------------------------------------------------
// FilterExec - filters rows by a predicate
// ---------------------------------------------------------------------------

pub struct FilterExec {
    pub predicate: LogicalExpr,
    pub input: Box<dyn ExecutionOp>,
    pub schema: Schema,
}

impl ExecutionOp for FilterExec {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        while let Some(batch) = self.input.next_batch()? {
            let mask = eval_filter(&self.predicate, &batch)?;
            let filtered = apply_filter_mask(&batch, &mask);
            if filtered.num_rows() > 0 {
                return Ok(Some(filtered));
            }
            // Skip empty batches, try next
        }
        Ok(None)
    }
}

/// Evaluate a filter predicate against a batch, returning a boolean mask (true = keep).
pub fn eval_filter(expr: &LogicalExpr, batch: &RecordBatch) -> Result<Vec<bool>> {
    let num_rows = batch.num_rows();
    let mut mask = vec![true; num_rows];

    match expr {
        LogicalExpr::BinaryOp { left, op, right } => {
            match op {
                BinOp::And => {
                    let left_mask = eval_filter(left, batch)?;
                    let right_mask = eval_filter(right, batch)?;
                    for i in 0..num_rows {
                        mask[i] = left_mask[i] && right_mask[i];
                    }
                }
                BinOp::Or => {
                    let left_mask = eval_filter(left, batch)?;
                    let right_mask = eval_filter(right, batch)?;
                    for i in 0..num_rows {
                        mask[i] = left_mask[i] || right_mask[i];
                    }
                }
                _ => {
                    // Comparison operators
                    let left_vals = eval_expr_column(left, batch)?;
                    let right_vals = eval_expr_column(right, batch)?;
                    for i in 0..num_rows {
                        mask[i] = eval_comparison(&left_vals[i], op, &right_vals[i]);
                    }
                }
            }
        }
        LogicalExpr::Not(inner) => {
            let inner_mask = eval_filter(inner, batch)?;
            for i in 0..num_rows {
                mask[i] = !inner_mask[i];
            }
        }
        LogicalExpr::IsNull(inner) => {
            let vals = eval_expr_column(inner, batch)?;
            for i in 0..num_rows {
                mask[i] = vals[i].is_null();
            }
        }
        LogicalExpr::IsNotNull(inner) => {
            let vals = eval_expr_column(inner, batch)?;
            for i in 0..num_rows {
                mask[i] = !vals[i].is_null();
            }
        }
        LogicalExpr::Literal(v) => {
            // Treat non-null as true, null as false
            for i in 0..num_rows {
                mask[i] = !v.is_null();
            }
        }
        _ => {
            // Fallback: evaluate as scalar, treat truthy as true
            let vals = eval_expr_column(expr, batch)?;
            for i in 0..num_rows {
                mask[i] = !vals[i].is_null();
            }
        }
    }

    Ok(mask)
}

fn eval_comparison(left: &ScalarValue, op: &BinOp, right: &ScalarValue) -> bool {
    if left.is_null() || right.is_null() {
        return false;
    }
    match (left, right) {
        (ScalarValue::Int64(Some(l)), ScalarValue::Int64(Some(r))) => cmp_i64(*l, op, *r),
        (ScalarValue::Int32(Some(l)), ScalarValue::Int32(Some(r))) => {
            cmp_i64(*l as i64, op, *r as i64)
        }
        (ScalarValue::Int16(Some(l)), ScalarValue::Int16(Some(r))) => {
            cmp_i64(*l as i64, op, *r as i64)
        }
        (ScalarValue::Int8(Some(l)), ScalarValue::Int8(Some(r))) => {
            cmp_i64(*l as i64, op, *r as i64)
        }
        (ScalarValue::UInt64(Some(l)), ScalarValue::UInt64(Some(r))) => cmp_u64(*l, op, *r),
        (ScalarValue::UInt32(Some(l)), ScalarValue::UInt32(Some(r))) => {
            cmp_u64(*l as u64, op, *r as u64)
        }
        (ScalarValue::Float64(Some(l)), ScalarValue::Float64(Some(r))) => cmp_f64(*l, op, *r),
        (ScalarValue::Float32(Some(l)), ScalarValue::Float32(Some(r))) => {
            cmp_f64(*l as f64, op, *r as f64)
        }
        (ScalarValue::VarChar(Some(l)), ScalarValue::VarChar(Some(r))) => match op {
            BinOp::Eq => l == r,
            BinOp::Neq => l != r,
            BinOp::Lt => l < r,
            BinOp::Gt => l > r,
            BinOp::Lte => l <= r,
            BinOp::Gte => l >= r,
            _ => false,
        },
        // Cross-type: promote to i64
        (ScalarValue::Int64(Some(l)), ScalarValue::Int32(Some(r))) => cmp_i64(*l, op, *r as i64),
        (ScalarValue::Int32(Some(l)), ScalarValue::Int64(Some(r))) => cmp_i64(*l as i64, op, *r),
        _ => false,
    }
}

fn cmp_i64(l: i64, op: &BinOp, r: i64) -> bool {
    match op {
        BinOp::Eq => l == r,
        BinOp::Neq => l != r,
        BinOp::Lt => l < r,
        BinOp::Gt => l > r,
        BinOp::Lte => l <= r,
        BinOp::Gte => l >= r,
        _ => false,
    }
}
fn cmp_u64(l: u64, op: &BinOp, r: u64) -> bool {
    match op {
        BinOp::Eq => l == r,
        BinOp::Neq => l != r,
        BinOp::Lt => l < r,
        BinOp::Gt => l > r,
        BinOp::Lte => l <= r,
        BinOp::Gte => l >= r,
        _ => false,
    }
}
fn cmp_f64(l: f64, op: &BinOp, r: f64) -> bool {
    match op {
        BinOp::Eq => l == r,
        BinOp::Neq => l != r,
        BinOp::Lt => l < r,
        BinOp::Gt => l > r,
        BinOp::Lte => l <= r,
        BinOp::Gte => l >= r,
        _ => false,
    }
}

/// Evaluate an expression for every row in a batch, producing a Vec<ScalarValue>.
pub fn eval_expr_column(expr: &LogicalExpr, batch: &RecordBatch) -> Result<Vec<ScalarValue>> {
    let num_rows = batch.num_rows();
    match expr {
        LogicalExpr::Column(idx) => Ok((0..num_rows)
            .map(|i| batch.scalar(i, *idx).unwrap_or(ScalarValue::Int64(None)))
            .collect()),
        LogicalExpr::Literal(v) => Ok(vec![v.clone(); num_rows]),
        LogicalExpr::BinaryOp { left, op, right } => {
            let lv = eval_expr_column(left, batch)?;
            let rv = eval_expr_column(right, batch)?;
            let mut result = Vec::with_capacity(num_rows);
            for i in 0..num_rows {
                result.push(eval_binary_scalar(&lv[i], op, &rv[i]));
            }
            Ok(result)
        }
        LogicalExpr::IsNull(inner) => {
            let vals = eval_expr_column(inner, batch)?;
            Ok(vals
                .iter()
                .map(|v| ScalarValue::Bool(Some(v.is_null())))
                .collect())
        }
        LogicalExpr::IsNotNull(inner) => {
            let vals = eval_expr_column(inner, batch)?;
            Ok(vals
                .iter()
                .map(|v| ScalarValue::Bool(Some(!v.is_null())))
                .collect())
        }
        LogicalExpr::Not(inner) => {
            let vals = eval_expr_column(inner, batch)?;
            Ok(vals
                .iter()
                .map(|v| match v {
                    ScalarValue::Bool(Some(b)) => ScalarValue::Bool(Some(!b)),
                    _ => ScalarValue::Bool(None),
                })
                .collect())
        }
        _ => Ok(vec![ScalarValue::Int64(None); num_rows]),
    }
}

fn eval_binary_scalar(left: &ScalarValue, op: &BinOp, right: &ScalarValue) -> ScalarValue {
    if left.is_null() || right.is_null() {
        return ScalarValue::Int64(None);
    }
    match (left, right) {
        (ScalarValue::Int64(Some(l)), ScalarValue::Int64(Some(r))) => match op {
            BinOp::Add => ScalarValue::Int64(Some(l + r)),
            BinOp::Sub => ScalarValue::Int64(Some(l - r)),
            BinOp::Mul => ScalarValue::Int64(Some(l * r)),
            BinOp::Div => ScalarValue::Int64(Some(l / r)),
            BinOp::Eq => ScalarValue::Bool(Some(l == r)),
            BinOp::Neq => ScalarValue::Bool(Some(l != r)),
            BinOp::Lt => ScalarValue::Bool(Some(l < r)),
            BinOp::Gt => ScalarValue::Bool(Some(l > r)),
            BinOp::Lte => ScalarValue::Bool(Some(l <= r)),
            BinOp::Gte => ScalarValue::Bool(Some(l >= r)),
            BinOp::And | BinOp::Or => ScalarValue::Bool(None),
        },
        (ScalarValue::Float64(Some(l)), ScalarValue::Float64(Some(r))) => match op {
            BinOp::Add => ScalarValue::Float64(Some(l + r)),
            BinOp::Sub => ScalarValue::Float64(Some(l - r)),
            BinOp::Mul => ScalarValue::Float64(Some(l * r)),
            BinOp::Div => ScalarValue::Float64(Some(l / r)),
            _ => ScalarValue::Bool(None),
        },
        (ScalarValue::Int32(Some(l)), ScalarValue::Int32(Some(r))) => match op {
            BinOp::Add => ScalarValue::Int32(Some(l + r)),
            BinOp::Sub => ScalarValue::Int32(Some(l - r)),
            _ => ScalarValue::Bool(None),
        },
        _ => ScalarValue::Int64(None),
    }
}

/// Apply a boolean mask to a batch, keeping only rows where mask[i] is true.
pub fn apply_filter_mask(batch: &RecordBatch, mask: &[bool]) -> RecordBatch {
    let _keep_count = mask.iter().filter(|&&m| m).count();
    let mut columns = Vec::with_capacity(batch.num_columns());
    for col_idx in 0..batch.num_columns() {
        let col = batch.column(col_idx).unwrap();
        let mut new_col = ColumnArray::empty(col.data_type());
        for (i, &keep) in mask.iter().enumerate() {
            if keep {
                new_col.push(&col.scalar(i)).unwrap();
            }
        }
        columns.push(new_col);
    }
    RecordBatch::try_new(batch.schema.clone(), columns).unwrap()
}

// ---------------------------------------------------------------------------
// ProjectExec - select/transform columns
// ---------------------------------------------------------------------------

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
                    let vals = eval_expr_column(expr, &batch)?;
                    let alias = self.aliases[i]
                        .as_deref()
                        .unwrap_or_else(|| infer_expr_name(expr));
                    let dt = infer_type(expr, &batch.schema);

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
        LogicalExpr::Column(idx) => schema
            .columns
            .get(*idx)
            .map(|c| c.data_type)
            .unwrap_or(DataType::Int64),
        LogicalExpr::Literal(v) => v.data_type(),
        LogicalExpr::BinaryOp { .. } => DataType::Int64,
        LogicalExpr::Aggregate(agg) => {
            let agg_prefix = match agg.func {
                AggFunc::Count => "count",
                AggFunc::Sum => "sum",
                AggFunc::Avg => "avg",
                AggFunc::Min => "min",
                AggFunc::Max => "max",
            };
            schema
                .columns
                .iter()
                .find(|c| {
                    let name = c.name.to_ascii_lowercase();
                    name.starts_with(&format!("{}(", agg_prefix))
                        || name == format!("{}(*)", agg_prefix)
                })
                .map(|c| c.data_type)
                .unwrap_or_else(|| aggregate_result_type(agg, schema))
        }
        _ => DataType::Int64,
    }
}

pub fn infer_expr_name(expr: &LogicalExpr) -> &'static str {
    match expr {
        LogicalExpr::Column(_) => "col",
        LogicalExpr::Literal(_) => "literal",
        LogicalExpr::BinaryOp { .. } => "expr",
        LogicalExpr::Aggregate(agg) => match agg.func {
            AggFunc::Count => "count(*)",
            AggFunc::Sum => "sum",
            AggFunc::Avg => "avg",
            AggFunc::Min => "min",
            AggFunc::Max => "max",
        },
        _ => "expr",
    }
}

// ---------------------------------------------------------------------------
// SortExec - sort rows
// ---------------------------------------------------------------------------

pub struct SortExec {
    pub sort_keys: Vec<SortKey>,
    pub input: Box<dyn ExecutionOp>,
    pub schema: Schema,
    done: bool,
}

impl SortExec {
    pub fn new(sort_keys: Vec<SortKey>, input: Box<dyn ExecutionOp>, schema: Schema) -> Self {
        Self {
            sort_keys,
            input,
            schema,
            done: false,
        }
    }
}

impl ExecutionOp for SortExec {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.done {
            return Ok(None);
        }
        self.done = true;

        // Collect all batches
        let mut all_batches = Vec::new();
        while let Some(batch) = self.input.next_batch()? {
            all_batches.push(batch);
        }

        if all_batches.is_empty() {
            return Ok(None);
        }

        // Combine into rows and sort
        let mut rows: Vec<Vec<ScalarValue>> = Vec::new();
        for batch in &all_batches {
            for row in 0..batch.num_rows() {
                let row_vals: Vec<ScalarValue> = (0..batch.num_columns())
                    .map(|c| batch.scalar(row, c).unwrap_or(ScalarValue::Int64(None)))
                    .collect();
                rows.push(row_vals);
            }
        }

        if rows.is_empty() {
            return Ok(Some(RecordBatch::new(self.schema.clone())));
        }

        // Sort rows by sort keys
        let sort_key_indices: Vec<(usize, bool)> = self
            .sort_keys
            .iter()
            .map(|sk| match &sk.expr {
                LogicalExpr::Column(idx) => (*idx, sk.desc),
                _ => (0, sk.desc),
            })
            .collect();

        rows.sort_by(|a, b| {
            for &(idx, desc) in &sort_key_indices {
                let cmp = compare_scalars(&a[idx], &b[idx]);
                let cmp = if desc { cmp.reverse() } else { cmp };
                if cmp != std::cmp::Ordering::Equal {
                    return cmp;
                }
            }
            std::cmp::Ordering::Equal
        });

        // Build output batch
        let mut batch = RecordBatch::new(self.schema.clone());
        for row in &rows {
            for (col_idx, val) in row.iter().enumerate() {
                batch.column_mut(col_idx).unwrap().push(val)?;
            }
        }

        Ok(Some(batch))
    }
}

fn compare_scalars(a: &ScalarValue, b: &ScalarValue) -> std::cmp::Ordering {
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
        (ScalarValue::Float64(Some(l)), ScalarValue::Float64(Some(r))) => {
            l.partial_cmp(r).unwrap_or(std::cmp::Ordering::Equal)
        }
        (ScalarValue::VarChar(Some(l)), ScalarValue::VarChar(Some(r))) => l.cmp(r),
        _ => std::cmp::Ordering::Equal,
    }
}

// ---------------------------------------------------------------------------
// LimitExec - limit rows
// ---------------------------------------------------------------------------

pub struct LimitExec {
    pub limit: usize,
    pub offset: usize,
    pub input: Box<dyn ExecutionOp>,
    pub schema: Schema,
    rows_returned: usize,
    skip_remaining: usize,
    done: bool,
}

impl LimitExec {
    pub fn new(limit: usize, offset: usize, input: Box<dyn ExecutionOp>, schema: Schema) -> Self {
        Self {
            limit,
            offset,
            input,
            schema,
            rows_returned: 0,
            skip_remaining: offset,
            done: false,
        }
    }
}

impl ExecutionOp for LimitExec {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.done || self.rows_returned >= self.limit {
            return Ok(None);
        }

        match self.input.next_batch()? {
            Some(batch) => {
                // Handle offset skip
                if self.skip_remaining > 0 {
                    if batch.num_rows() <= self.skip_remaining {
                        self.skip_remaining -= batch.num_rows();
                        return self.next_batch();
                    }
                    // Partial skip: trim the batch
                    let start = self.skip_remaining;
                    self.skip_remaining = 0;
                    let remaining = self.limit - self.rows_returned;
                    let end = (start + remaining).min(batch.num_rows());
                    let trimmed = trim_batch(&batch, start, end);
                    self.rows_returned += trimmed.num_rows();
                    if self.rows_returned >= self.limit {
                        self.done = true;
                    }
                    return Ok(Some(trimmed));
                }

                let remaining = self.limit - self.rows_returned;
                if batch.num_rows() <= remaining {
                    self.rows_returned += batch.num_rows();
                    Ok(Some(batch))
                } else {
                    let trimmed = trim_batch(&batch, 0, remaining);
                    self.rows_returned = self.limit;
                    self.done = true;
                    Ok(Some(trimmed))
                }
            }
            None => {
                self.done = true;
                Ok(None)
            }
        }
    }
}

fn trim_batch(batch: &RecordBatch, start: usize, end: usize) -> RecordBatch {
    let mut columns = Vec::with_capacity(batch.num_columns());
    for col_idx in 0..batch.num_columns() {
        let col = batch.column(col_idx).unwrap();
        let mut new_col = ColumnArray::empty(col.data_type());
        for i in start..end {
            new_col.push(&col.scalar(i)).unwrap();
        }
        columns.push(new_col);
    }
    RecordBatch::try_new(batch.schema.clone(), columns).unwrap()
}

// ---------------------------------------------------------------------------
// HashAggregateExec - GROUP BY + aggregate functions
// ---------------------------------------------------------------------------

pub struct HashAggregateExec {
    pub group_by: Vec<LogicalExpr>,
    pub aggregates: Vec<AggregateExpr>,
    pub input: Box<dyn ExecutionOp>,
    pub input_schema: Schema,
    done: bool,
}

impl HashAggregateExec {
    pub fn new(
        group_by: Vec<LogicalExpr>,
        aggregates: Vec<AggregateExpr>,
        input: Box<dyn ExecutionOp>,
        input_schema: Schema,
    ) -> Self {
        Self {
            group_by,
            aggregates,
            input,
            input_schema,
            done: false,
        }
    }
}

impl ExecutionOp for HashAggregateExec {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.done {
            return Ok(None);
        }
        self.done = true;
        tracing::debug!("operators.rs HashAggregateExec: starting aggregation");

        // Collect all input
        let mut all_rows: Vec<Vec<ScalarValue>> = Vec::new();
        while let Some(batch) = self.input.next_batch()? {
            for row in 0..batch.num_rows() {
                let vals: Vec<ScalarValue> = (0..batch.num_columns())
                    .map(|c| batch.scalar(row, c).unwrap_or(ScalarValue::Int64(None)))
                    .collect();
                all_rows.push(vals);
            }
        }

        // Group by key
        let mut groups: HashMap<Vec<String>, Vec<Vec<ScalarValue>>> = HashMap::new();
        for row in &all_rows {
            let key: Vec<String> = self
                .group_by
                .iter()
                .map(|expr| match expr {
                    LogicalExpr::Column(idx) => {
                        row.get(*idx).map(|v| v.to_string()).unwrap_or_default()
                    }
                    _ => String::new(),
                })
                .collect();
            groups.entry(key).or_default().push(row.clone());
        }

        // Build output schema
        let mut col_defs = Vec::new();
        tracing::debug!(
            "HashAggregateExec: input_schema = {:?}",
            self.input_schema
                .columns
                .iter()
                .map(|c| (c.name.clone(), c.data_type))
                .collect::<Vec<_>>()
        );
        for expr in &self.group_by {
            match expr {
                LogicalExpr::Column(idx) => {
                    if let Some(cd) = self.input_schema.columns.get(*idx) {
                        col_defs.push(
                            ColumnDef::new(&cd.name, cd.data_type)
                                .with_scale(cd.scale)
                                .with_precision(cd.precision),
                        );
                    }
                }
                _ => {}
            }
        }
        for agg in &self.aggregates {
            let func_name = match agg.func {
                AggFunc::Count => "count",
                AggFunc::Sum => "sum",
                AggFunc::Avg => "avg",
                AggFunc::Min => "min",
                AggFunc::Max => "max",
            };
            let arg_name = match &*agg.arg {
                LogicalExpr::Wildcard => "*".to_string(),
                LogicalExpr::Column(idx) => self
                    .input_schema
                    .columns
                    .get(*idx)
                    .map(|c| c.name.clone())
                    .unwrap_or_default(),
                _ => "expr".to_string(),
            };
            let name = if agg.distinct {
                format!("{}(distinct {})", func_name, arg_name)
            } else {
                format!("{}({})", func_name, arg_name)
            };
            let dt = aggregate_result_type(agg, &self.input_schema);
            col_defs.push(ColumnDef::new(&name, dt));
        }
        let schema = Schema::new(col_defs);

        // Build output rows
        let mut batch = RecordBatch::new(schema.clone());
        for (_key, rows) in &groups {
            // Group-by columns
            for expr in &self.group_by {
                if let LogicalExpr::Column(idx) = expr {
                    let _val = rows
                        .first()
                        .and_then(|r| r.get(*idx))
                        .cloned()
                        .unwrap_or(ScalarValue::Int64(None));
                    let _col_idx = batch.schema.columns.len(); // Will be set after
                                                               // Actually push directly
                }
            }

            // Re-do: push group values then aggregate values
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
                let agg_val = compute_aggregate(agg, rows, &self.input_schema);
                row_vals.push(agg_val);
            }

            for (col_idx, val) in row_vals.iter().enumerate() {
                batch.column_mut(col_idx).unwrap().push(val)?;
            }
        }

        Ok(Some(batch))
    }
}

pub fn compute_aggregate(
    agg: &AggregateExpr,
    rows: &[Vec<ScalarValue>],
    schema: &Schema,
) -> ScalarValue {
    let col_idx = match &*agg.arg {
        LogicalExpr::Column(idx) => Some(*idx),
        LogicalExpr::Wildcard => None,
        _ => None,
    };

    match agg.func {
        AggFunc::Count => {
            if let Some(idx) = col_idx {
                let non_null = rows
                    .iter()
                    .filter(|r| !r.get(idx).map(|v| v.is_null()).unwrap_or(true))
                    .count();
                ScalarValue::Int64(Some(non_null as i64))
            } else {
                ScalarValue::Int64(Some(rows.len() as i64))
            }
        }
        AggFunc::Sum => {
            if let Some(idx) = col_idx {
                let input_type = schema
                    .columns
                    .get(idx)
                    .map(|c| c.data_type)
                    .unwrap_or(DataType::Int64);
                if matches!(input_type, DataType::Float32 | DataType::Float64) {
                    let sum: f64 = rows
                        .iter()
                        .filter_map(|r| r.get(idx).and_then(|v| scalar_to_f64(v)))
                        .sum();
                    ScalarValue::Float64(Some(sum))
                } else {
                    let sum: i64 = rows
                        .iter()
                        .filter_map(|r| r.get(idx).and_then(|v| v.integer_i128()))
                        .filter_map(|v| i64::try_from(v).ok())
                        .sum();
                    ScalarValue::Int64(Some(sum))
                }
            } else {
                ScalarValue::Int64(None)
            }
        }
        AggFunc::Avg => {
            if let Some(idx) = col_idx {
                let vals: Vec<f64> = rows
                    .iter()
                    .filter_map(|r| r.get(idx).and_then(|v| scalar_to_f64(v)))
                    .collect();
                if vals.is_empty() {
                    ScalarValue::Float64(None)
                } else {
                    ScalarValue::Float64(Some(vals.iter().sum::<f64>() / vals.len() as f64))
                }
            } else {
                ScalarValue::Float64(None)
            }
        }
        AggFunc::Min => {
            if let Some(idx) = col_idx {
                rows.iter()
                    .filter_map(|r| r.get(idx).cloned())
                    .filter(|v| !v.is_null())
                    .min_by(|a, b| compare_scalars(a, b))
                    .unwrap_or(ScalarValue::Int64(None))
            } else {
                ScalarValue::Int64(None)
            }
        }
        AggFunc::Max => {
            if let Some(idx) = col_idx {
                rows.iter()
                    .filter_map(|r| r.get(idx).cloned())
                    .filter(|v| !v.is_null())
                    .max_by(|a, b| compare_scalars(a, b))
                    .unwrap_or(ScalarValue::Int64(None))
            } else {
                ScalarValue::Int64(None)
            }
        }
    }
}

fn scalar_to_f64(v: &ScalarValue) -> Option<f64> {
    v.numeric_f64()
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

// ---------------------------------------------------------------------------
// HashJoinExec - hash join implementation
// ---------------------------------------------------------------------------

#[allow(dead_code)]
pub struct HashJoinExec {
    pub left: Box<dyn ExecutionOp>,
    pub right: Box<dyn ExecutionOp>,
    pub join_type: JoinType,
    pub condition: LogicalExpr,
    pub left_schema: Schema,
    pub right_schema: Schema,
    pub output_schema: Schema,
    /// Hash table built from left side: key -> list of left row indices
    hash_table: HashMap<String, Vec<usize>>,
    /// Left batches stored for output construction
    left_batches: Vec<RecordBatch>,
    /// Current right batch being probed
    right_batch_idx: usize,
    /// Current row in right batch being probed
    right_row_idx: usize,
    /// Whether build phase is complete
    build_done: bool,
    /// Rows emitted for current probe (for LEFT JOIN null emission tracking)
    left_matched: Vec<bool>,
}

impl HashJoinExec {
    pub fn new(
        left: Box<dyn ExecutionOp>,
        right: Box<dyn ExecutionOp>,
        join_type: JoinType,
        condition: LogicalExpr,
        left_schema: Schema,
        right_schema: Schema,
        output_schema: Schema,
    ) -> Self {
        Self {
            left,
            right,
            join_type,
            condition,
            left_schema,
            right_schema,
            output_schema,
            hash_table: HashMap::new(),
            left_batches: Vec::new(),
            right_batch_idx: 0,
            right_row_idx: 0,
            build_done: false,
            left_matched: Vec::new(),
        }
    }

    /// Build hash table from left side (consume all left batches)
    fn build_phase(&mut self) -> Result<()> {
        // Collect all left batches
        let mut left_row_idx = 0;
        while let Some(batch) = self.left.next_batch()? {
            // Build hash table for this batch
            for row in 0..batch.num_rows() {
                // Extract join key value for this row
                let key_val = self.extract_join_key_left(&batch, row);
                let key_str = key_val.to_string();
                self.hash_table
                    .entry(key_str)
                    .or_default()
                    .push(left_row_idx);
                left_row_idx += 1;
            }
            self.left_batches.push(batch);
        }
        // Initialize matched tracking for LEFT JOIN
        if self.join_type == JoinType::Left {
            self.left_matched = vec![false; left_row_idx];
        }
        self.build_done = true;
        Ok(())
    }

    /// Extract join key value from left batch row (assumes single column key for now)
    fn extract_join_key_left(&self, batch: &RecordBatch, row: usize) -> ScalarValue {
        // For now, assume condition is a simple equality comparison like a.id = b.id
        // The left column is the first column in the condition
        match &self.condition {
            LogicalExpr::BinaryOp { left, .. } => match &**left {
                LogicalExpr::Column(idx) => {
                    batch.scalar(row, *idx).unwrap_or(ScalarValue::Int64(None))
                }
                _ => ScalarValue::Int64(None),
            },
            _ => ScalarValue::Int64(None),
        }
    }

    /// Extract join key value from right batch row
    fn extract_join_key_right(&self, batch: &RecordBatch, row: usize) -> ScalarValue {
        match &self.condition {
            LogicalExpr::BinaryOp { right, .. } => match &**right {
                LogicalExpr::Column(idx) => {
                    batch.scalar(row, *idx).unwrap_or(ScalarValue::Int64(None))
                }
                _ => ScalarValue::Int64(None),
            },
            _ => ScalarValue::Int64(None),
        }
    }
}

impl ExecutionOp for HashJoinExec {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        // Build phase first
        if !self.build_done {
            self.build_phase()?;
        }

        // Probe phase: iterate through right batches
        let mut output_rows: Vec<Vec<ScalarValue>> = Vec::new();

        // Consume one right batch and probe
        if let Some(right_batch) = self.right.next_batch()? {
            for right_row in 0..right_batch.num_rows() {
                let key_val = self.extract_join_key_right(&right_batch, right_row);
                let key_str = key_val.to_string();

                // Probe hash table
                if let Some(left_indices) = self.hash_table.get(&key_str) {
                    // For each matching left row, emit a joined row
                    for &left_row_idx in left_indices {
                        // Find which left batch contains this row
                        let (batch_idx, row_in_batch) = self.find_left_row_position(left_row_idx);
                        let left_batch = &self.left_batches[batch_idx];

                        // Build output row: left columns + right columns
                        let mut row_vals = Vec::new();
                        for col in 0..self.left_schema.columns.len() {
                            row_vals.push(
                                left_batch
                                    .scalar(row_in_batch, col)
                                    .unwrap_or(ScalarValue::Int64(None)),
                            );
                        }
                        for col in 0..self.right_schema.columns.len() {
                            row_vals.push(
                                right_batch
                                    .scalar(right_row, col)
                                    .unwrap_or(ScalarValue::Int64(None)),
                            );
                        }
                        output_rows.push(row_vals);

                        // Mark as matched for LEFT JOIN
                        if self.join_type == JoinType::Left {
                            self.left_matched[left_row_idx] = true;
                        }
                    }
                } else if self.join_type == JoinType::Left {
                    // LEFT JOIN: emit left row with nulls for right columns when no match
                    // But we're iterating right rows here - we need to emit unmatched left rows at end
                }
            }

            // Build output batch from collected rows
            if output_rows.is_empty() {
                // Return empty batch with correct schema
                return Ok(Some(RecordBatch::new(self.output_schema.clone())));
            }

            let mut batch = RecordBatch::new(self.output_schema.clone());
            for row_vals in &output_rows {
                for (col_idx, val) in row_vals.iter().enumerate() {
                    batch.column_mut(col_idx).unwrap().push(val)?;
                }
            }
            return Ok(Some(batch));
        }

        // Right exhausted - for LEFT JOIN, emit unmatched left rows
        if self.join_type == JoinType::Left {
            let mut unmatched_rows: Vec<Vec<ScalarValue>> = Vec::new();
            for (left_row_idx, matched) in self.left_matched.iter().enumerate() {
                if !*matched {
                    let (batch_idx, row_in_batch) = self.find_left_row_position(left_row_idx);
                    let left_batch = &self.left_batches[batch_idx];

                    let mut row_vals = Vec::new();
                    for col in 0..self.left_schema.columns.len() {
                        row_vals.push(
                            left_batch
                                .scalar(row_in_batch, col)
                                .unwrap_or(ScalarValue::Int64(None)),
                        );
                    }
                    // Nulls for right columns
                    for _ in 0..self.right_schema.columns.len() {
                        row_vals.push(ScalarValue::Int64(None));
                    }
                    unmatched_rows.push(row_vals);
                }
            }

            if !unmatched_rows.is_empty() {
                // Mark all as matched so we don't emit again
                self.left_matched = vec![true; self.left_matched.len()];

                let mut batch = RecordBatch::new(self.output_schema.clone());
                for row_vals in &unmatched_rows {
                    for (col_idx, val) in row_vals.iter().enumerate() {
                        batch.column_mut(col_idx).unwrap().push(val)?;
                    }
                }
                return Ok(Some(batch));
            }
        }

        Ok(None)
    }
}

impl HashJoinExec {
    /// Find which batch and row position for a global left row index
    fn find_left_row_position(&self, global_idx: usize) -> (usize, usize) {
        let mut idx = global_idx;
        for (batch_idx, batch) in self.left_batches.iter().enumerate() {
            if idx < batch.num_rows() {
                return (batch_idx, idx);
            }
            idx -= batch.num_rows();
        }
        (0, 0) // Should not happen
    }
}

// ---------------------------------------------------------------------------
// Pipeline builder: LogicalPlan 鈫?ExecutionOp tree
// ---------------------------------------------------------------------------

/// Build an execution pipeline from a logical plan.
/// table_batches: HashMap of table name -> pre-loaded batches for scan nodes
pub fn build_pipeline(
    plan: LogicalPlan,
    table_batches: &HashMap<String, Vec<RecordBatch>>,
) -> Result<Box<dyn ExecutionOp>> {
    match plan {
        LogicalPlan::Scan(scan) => {
            let batches = table_batches
                .get(&scan.table_name)
                .cloned()
                .unwrap_or_default();
            Ok(Box::new(
                ScanExec::new(batches, scan.projection).with_table_schema(scan.schema.clone()),
            ))
        }
        LogicalPlan::Filter(filter) => {
            let schema = get_scan_schema(&filter.input);
            let input = build_pipeline(*filter.input, table_batches)?;
            Ok(Box::new(FilterExec {
                predicate: filter.predicate,
                input,
                schema: schema.clone(),
            }))
        }
        LogicalPlan::Project(proj) => {
            let input_schema = get_scan_schema(&proj.input);
            let input = build_pipeline(*proj.input, table_batches)?;
            Ok(Box::new(ProjectExec {
                expressions: proj.expressions,
                aliases: proj.aliases,
                input,
                input_schema,
            }))
        }
        LogicalPlan::Sort(sort) => {
            let schema = get_scan_schema(&sort.input);
            let input = build_pipeline(*sort.input, table_batches)?;
            Ok(Box::new(SortExec::new(sort.sort_keys, input, schema)))
        }
        LogicalPlan::Limit(limit) => {
            let schema = get_scan_schema(&limit.input);
            let input = build_pipeline(*limit.input, table_batches)?;
            Ok(Box::new(LimitExec::new(
                limit.limit,
                limit.offset,
                input,
                schema,
            )))
        }
        LogicalPlan::Aggregate(agg) => {
            let input_schema = get_scan_schema(&agg.input);
            let input = build_pipeline(*agg.input, table_batches)?;
            Ok(Box::new(HashAggregateExec::new(
                agg.group_by,
                agg.aggregates,
                input,
                input_schema,
            )))
        }
        LogicalPlan::Join(join) => {
            let left = build_pipeline(*join.left, table_batches)?;
            let right = build_pipeline(*join.right, table_batches)?;
            Ok(Box::new(HashJoinExec::new(
                left,
                right,
                join.join_type,
                join.condition,
                join.left_schema,
                join.right_schema,
                join.output_schema,
            )))
        }
        LogicalPlan::Empty => Ok(Box::new(ScanExec::new(vec![], None))),
    }
}

fn get_scan_schema(plan: &LogicalPlan) -> Schema {
    match plan {
        LogicalPlan::Scan(s) => s.schema.clone(),
        LogicalPlan::Filter(f) => get_scan_schema(&f.input),
        LogicalPlan::Project(p) => get_scan_schema(&p.input),
        LogicalPlan::Sort(s) => get_scan_schema(&s.input),
        LogicalPlan::Limit(l) => get_scan_schema(&l.input),
        LogicalPlan::Aggregate(a) => get_scan_schema(&a.input),
        LogicalPlan::Join(j) => j.output_schema.clone(),
        LogicalPlan::Empty => Schema::empty(),
    }
}

#[cfg(test)]
#[path = "operators_tests.rs"]
mod tests;
