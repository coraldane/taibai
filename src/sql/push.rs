use crate::common::error::Result;
use crate::sql::planner::*;
use crate::types::array::ColumnArray;
use crate::types::batch::{ColumnDef, RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

/// Push-model operator: receives batches and produces a final result.
/// Unlike the pull model (next_batch), data is pushed through the pipeline
/// for better CPU cache efficiency through monomorphization.
pub trait PushOp {
    /// Push a batch through the pipeline.
    fn push(&mut self, batch: RecordBatch) -> Result<()>;
    /// Finalize and return the accumulated result.
    fn finalize(self: Box<Self>) -> Result<Option<RecordBatch>>;
}

fn aggregate_result_type(agg: &AggregateExpr, schema: &Schema) -> crate::protocol::types::DataType {
    use crate::protocol::types::DataType;
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
// PushFilter - filters rows as batches are pushed through
// ---------------------------------------------------------------------------

pub struct PushFilter {
    predicate: LogicalExpr,
    output_schema: Schema,
    buffer: Vec<RecordBatch>,
}

impl PushFilter {
    pub fn new(predicate: LogicalExpr, schema: Schema) -> Self {
        Self {
            predicate,
            output_schema: schema,
            buffer: Vec::new(),
        }
    }
}

impl PushOp for PushFilter {
    fn push(&mut self, batch: RecordBatch) -> Result<()> {
        use crate::sql::operators::{apply_filter_mask, eval_filter};
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let mask = eval_filter(&self.predicate, &batch)?;
        let filtered = apply_filter_mask(&batch, &mask);
        if filtered.num_rows() > 0 {
            self.buffer.push(filtered);
        }
        Ok(())
    }

    fn finalize(self: Box<Self>) -> Result<Option<RecordBatch>> {
        if self.buffer.is_empty() {
            return Ok(Some(RecordBatch::new(self.output_schema)));
        }
        // Merge all buffered batches
        Ok(Some(merge_batches(&self.buffer)))
    }
}

// ---------------------------------------------------------------------------
// PushProject - project columns as batches are pushed through
// ---------------------------------------------------------------------------

pub struct PushProject {
    expressions: Vec<LogicalExpr>,
    aliases: Vec<Option<String>>,
    buffer: Vec<RecordBatch>,
}

impl PushProject {
    pub fn new(expressions: Vec<LogicalExpr>, aliases: Vec<Option<String>>) -> Self {
        Self {
            expressions,
            aliases,
            buffer: Vec::new(),
        }
    }
}

impl PushOp for PushProject {
    fn push(&mut self, batch: RecordBatch) -> Result<()> {
        use crate::sql::operators::{eval_expr_column, infer_type};
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
            for v in &vals {
                col.push(v)?;
            }
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
        Ok(Some(merge_batches(&self.buffer)))
    }
}

// ---------------------------------------------------------------------------
// PushLimit - limits the number of rows
// ---------------------------------------------------------------------------

#[allow(dead_code)]
pub struct PushLimit {
    limit: usize,
    offset: usize,
    rows_collected: usize,
    skip_remaining: usize,
    buffer: Vec<RecordBatch>,
    done: bool,
}

impl PushLimit {
    pub fn new(limit: usize, offset: usize) -> Self {
        Self {
            limit,
            offset,
            rows_collected: 0,
            skip_remaining: offset,
            buffer: Vec::new(),
            done: false,
        }
    }
}

impl PushOp for PushLimit {
    fn push(&mut self, batch: RecordBatch) -> Result<()> {
        if self.done {
            return Ok(());
        }

        let num_rows = batch.num_rows();

        // Handle offset skip
        if self.skip_remaining > 0 {
            if num_rows <= self.skip_remaining {
                self.skip_remaining -= num_rows;
                return Ok(());
            }
            // Partial skip
            let start = self.skip_remaining;
            self.skip_remaining = 0;
            let remaining = self.limit - self.rows_collected;
            let end = (start + remaining).min(num_rows);
            let trimmed = trim_batch(&batch, start, end);
            self.rows_collected += trimmed.num_rows();
            self.buffer.push(trimmed);
            if self.rows_collected >= self.limit {
                self.done = true;
            }
            return Ok(());
        }

        let remaining = self.limit - self.rows_collected;
        if num_rows <= remaining {
            self.rows_collected += num_rows;
            self.buffer.push(batch);
        } else {
            let trimmed = trim_batch(&batch, 0, remaining);
            self.rows_collected = self.limit;
            self.done = true;
            self.buffer.push(trimmed);
        }

        Ok(())
    }

    fn finalize(self: Box<Self>) -> Result<Option<RecordBatch>> {
        if self.buffer.is_empty() {
            return Ok(None);
        }
        Ok(Some(merge_batches(&self.buffer)))
    }
}

// ---------------------------------------------------------------------------
// PushHashAggregate - GROUP BY + aggregate functions
// ---------------------------------------------------------------------------

pub struct PushHashAggregate {
    group_by: Vec<LogicalExpr>,
    aggregates: Vec<AggregateExpr>,
    input_schema: Schema,
    groups: std::collections::HashMap<Vec<String>, Vec<Vec<ScalarValue>>>,
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
            groups: std::collections::HashMap::new(),
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
        use crate::sql::operators::compute_aggregate;

        // Build output schema
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

        let mut batch = RecordBatch::new(schema.clone());
        for (_key, rows) in &self.groups {
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

        if batch.num_rows() == 0 {
            Ok(None)
        } else {
            Ok(Some(batch))
        }
    }
}

// ---------------------------------------------------------------------------
// Enum-based monomorphic pipeline for cache efficiency
// ---------------------------------------------------------------------------

/// A monomorphic pipeline operator that avoids dynamic dispatch.
/// The entire pipeline is represented as a single enum, enabling
/// the compiler to inline and optimize across operator boundaries.
///
/// Data flow: push() processes the batch and forwards to child (downstream).
/// Pipeline is built scan-side-out: `Filter { child: Project { child: Sink } }`
/// so Filter receives data first, processes, and pushes to Project, then Sink.
pub enum MonoPipeline {
    Filter {
        predicate: LogicalExpr,
        child: Box<MonoPipeline>,
    },
    Project {
        expressions: Vec<LogicalExpr>,
        aliases: Vec<Option<String>>,
        child: Box<MonoPipeline>,
    },
    Limit {
        limit: usize,
        rows_collected: usize,
        skip_remaining: usize,
        done: bool,
        child: Box<MonoPipeline>,
    },
    Aggregate {
        group_by: Vec<LogicalExpr>,
        aggregates: Vec<AggregateExpr>,
        input_schema: Schema,
        child: Box<MonoPipeline>,
        groups: std::collections::HashMap<Vec<String>, Vec<Vec<ScalarValue>>>,
    },
    Sink {
        buffer: Vec<RecordBatch>,
    },
}

impl MonoPipeline {
    /// Push a batch through the pipeline. Each operator processes and forwards downstream.
    pub fn push(&mut self, batch: RecordBatch) -> Result<()> {
        match self {
            MonoPipeline::Filter { predicate, child } => {
                use crate::sql::operators::{apply_filter_mask, eval_filter};
                if batch.num_rows() > 0 {
                    let mask = eval_filter(predicate, &batch)?;
                    let filtered = apply_filter_mask(&batch, &mask);
                    if filtered.num_rows() > 0 {
                        child.push(filtered)?;
                    }
                }
                Ok(())
            }
            MonoPipeline::Project {
                expressions,
                aliases,
                child,
            } => {
                use crate::sql::operators::{eval_expr_column, infer_type};
                if batch.num_rows() > 0 {
                    let mut columns = Vec::with_capacity(expressions.len());
                    let mut col_defs = Vec::with_capacity(expressions.len());
                    for (i, expr) in expressions.iter().enumerate() {
                        let vals = eval_expr_column(expr, &batch)?;
                        let alias = aliases[i].as_deref().unwrap_or("expr");
                        let dt = infer_type(expr, &batch.schema);
                        let mut col = ColumnArray::empty(dt);
                        for v in &vals {
                            col.push(v)?;
                        }
                        columns.push(col);
                        col_defs.push(ColumnDef::new(alias, dt));
                    }
                    let schema = Schema::new(col_defs);
                    child.push(RecordBatch::try_new(schema, columns)?)?;
                }
                Ok(())
            }
            MonoPipeline::Limit {
                limit,
                rows_collected,
                skip_remaining,
                done,
                child,
            } => {
                if *done {
                    return Ok(());
                }
                let num_rows = batch.num_rows();
                if num_rows == 0 {
                    return Ok(());
                }
                if *skip_remaining > 0 {
                    if num_rows <= *skip_remaining {
                        *skip_remaining -= num_rows;
                        return Ok(());
                    }
                    let start = *skip_remaining;
                    *skip_remaining = 0;
                    let remaining = *limit - *rows_collected;
                    let end = (start + remaining).min(num_rows);
                    let trimmed = trim_batch(&batch, start, end);
                    *rows_collected += trimmed.num_rows();
                    child.push(trimmed)?;
                } else {
                    let remaining = *limit - *rows_collected;
                    if num_rows <= remaining {
                        *rows_collected += num_rows;
                        child.push(batch)?;
                    } else {
                        let trimmed = trim_batch(&batch, 0, remaining);
                        *rows_collected = *limit;
                        *done = true;
                        child.push(trimmed)?;
                    }
                }
                if *rows_collected >= *limit {
                    *done = true;
                }
                Ok(())
            }
            MonoPipeline::Aggregate {
                group_by, groups, ..
            } => {
                // Aggregate buffers all groups locally; pushes to child in finalize
                for row in 0..batch.num_rows() {
                    let vals: Vec<ScalarValue> = (0..batch.num_columns())
                        .map(|c| batch.scalar(row, c).unwrap_or(ScalarValue::Int64(None)))
                        .collect();
                    let key: Vec<String> = group_by
                        .iter()
                        .map(|expr| match expr {
                            LogicalExpr::Column(idx) => {
                                vals.get(*idx).map(|v| format!("{}", v)).unwrap_or_default()
                            }
                            _ => String::new(),
                        })
                        .collect();
                    groups.entry(key).or_default().push(vals);
                }
                Ok(())
            }
            MonoPipeline::Sink { buffer } => {
                if batch.num_rows() > 0 {
                    buffer.push(batch);
                }
                Ok(())
            }
        }
    }

    /// Finalize and return the accumulated result.
    pub fn finalize(self) -> Result<Option<RecordBatch>> {
        match self {
            MonoPipeline::Filter { child, .. } => child.finalize(),
            MonoPipeline::Project { child, .. } => child.finalize(),
            MonoPipeline::Limit { child, .. } => child.finalize(),
            MonoPipeline::Aggregate {
                group_by,
                aggregates,
                input_schema,
                groups,
                mut child,
            } => {
                use crate::sql::operators::compute_aggregate;
                if groups.is_empty() {
                    return child.finalize();
                }
                let mut col_defs = Vec::new();
                for expr in &group_by {
                    if let LogicalExpr::Column(idx) = expr {
                        if let Some(cd) = input_schema.columns.get(*idx) {
                            col_defs.push(
                                ColumnDef::new(&cd.name, cd.data_type)
                                    .with_scale(cd.scale)
                                    .with_precision(cd.precision),
                            );
                        }
                    }
                }
                for agg in &aggregates {
                    let name = format!(
                        "{:?}({})",
                        agg.func,
                        match &*agg.arg {
                            LogicalExpr::Wildcard => "*".into(),
                            LogicalExpr::Column(idx) => input_schema
                                .columns
                                .get(*idx)
                                .map(|c| c.name.clone())
                                .unwrap_or_default(),
                            _ => "expr".into(),
                        }
                    );
                    let dt = aggregate_result_type(agg, &input_schema);
                    col_defs.push(ColumnDef::new(&name, dt));
                }
                let schema = Schema::new(col_defs);
                let mut batch = RecordBatch::new(schema);
                for (_key, rows) in &groups {
                    let mut row_vals = Vec::new();
                    for expr in &group_by {
                        if let LogicalExpr::Column(idx) = expr {
                            row_vals.push(
                                rows.first()
                                    .and_then(|r| r.get(*idx))
                                    .cloned()
                                    .unwrap_or(ScalarValue::Int64(None)),
                            );
                        }
                    }
                    for agg in &aggregates {
                        row_vals.push(compute_aggregate(agg, rows, &input_schema));
                    }
                    for (col_idx, val) in row_vals.iter().enumerate() {
                        batch.column_mut(col_idx).unwrap().push(val)?;
                    }
                }
                child.push(batch)?;
                child.finalize()
            }
            MonoPipeline::Sink { buffer } => {
                if buffer.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(merge_batches(&buffer)))
                }
            }
        }
    }

    /// Build a mono pipeline from a logical plan.
    /// The pipeline is built scan-side-out so data flows forward:
    /// Filter { child: Project { child: Sink } }
    pub fn build(plan: LogicalPlan) -> Self {
        // Build the pipeline from innermost (scan-side) to outermost (result-side).
        // The plan tree is: Project(Filter(Scan)), so we build:
        // Scan 鈫?Filter 鈫?Project, stored as Filter { child: Project { child: Sink } }
        let sink = MonoPipeline::Sink { buffer: Vec::new() };
        let pipeline = Self::build_from(plan, sink);
        pipeline
    }

    fn build_from(plan: LogicalPlan, child: MonoPipeline) -> MonoPipeline {
        match plan {
            LogicalPlan::Filter(filter) => {
                let inner = Self::build_from(*filter.input, child);
                MonoPipeline::Filter {
                    predicate: filter.predicate,
                    child: Box::new(inner),
                }
            }
            LogicalPlan::Project(proj) => {
                let inner = Self::build_from(*proj.input, child);
                MonoPipeline::Project {
                    expressions: proj.expressions,
                    aliases: proj.aliases,
                    child: Box::new(inner),
                }
            }
            LogicalPlan::Limit(limit) => {
                let inner = Self::build_from(*limit.input, child);
                MonoPipeline::Limit {
                    limit: limit.limit,
                    rows_collected: 0,
                    skip_remaining: limit.offset,
                    done: false,
                    child: Box::new(inner),
                }
            }
            LogicalPlan::Aggregate(agg) => {
                let input_schema = get_schema_from_plan(&agg.input);
                let inner = Self::build_from(*agg.input, child);
                MonoPipeline::Aggregate {
                    group_by: agg.group_by,
                    aggregates: agg.aggregates,
                    input_schema,
                    child: Box::new(inner),
                    groups: std::collections::HashMap::new(),
                }
            }
            LogicalPlan::Sort(_)
            | LogicalPlan::Scan(_)
            | LogicalPlan::Empty
            | LogicalPlan::Join(_) => {
                // Leaf node: the child is already the downstream pipeline
                // Note: Join is not fully supported in push-based pipeline yet
                child
            }
        }
    }
}

fn get_schema_from_plan(plan: &LogicalPlan) -> Schema {
    match plan {
        LogicalPlan::Scan(s) => s.schema.clone(),
        LogicalPlan::Filter(f) => get_schema_from_plan(&f.input),
        LogicalPlan::Project(p) => get_schema_from_plan(&p.input),
        LogicalPlan::Sort(s) => get_schema_from_plan(&s.input),
        LogicalPlan::Limit(l) => get_schema_from_plan(&l.input),
        LogicalPlan::Aggregate(a) => get_schema_from_plan(&a.input),
        LogicalPlan::Join(j) => j.output_schema.clone(),
        LogicalPlan::Empty => Schema::empty(),
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn merge_batches(batches: &[RecordBatch]) -> RecordBatch {
    if batches.is_empty() {
        return RecordBatch::new(Schema::empty());
    }
    if batches.len() == 1 {
        return batches[0].clone();
    }

    let schema = batches[0].schema.clone();
    let num_cols = schema.columns.len();
    let mut columns = Vec::with_capacity(num_cols);

    for col_idx in 0..num_cols {
        let dt = schema.columns[col_idx].data_type;
        let mut col = ColumnArray::empty(dt);
        for batch in batches {
            if let Some(src) = batch.column(col_idx) {
                for i in 0..batch.num_rows() {
                    col.push(&src.scalar(i)).unwrap();
                }
            }
        }
        columns.push(col);
    }

    RecordBatch::try_new(schema, columns).unwrap()
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

#[cfg(test)]
#[path = "push_test.rs"]
mod tests;
