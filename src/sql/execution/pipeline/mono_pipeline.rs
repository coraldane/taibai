use crate::common::error::Result;
use crate::sql::planner::{LogicalPlan, LogicalExpr};
use crate::types::batch::RecordBatch;

use super::push_filter::PushFilter;
use super::push_project::PushProject;
use super::push_aggregate::PushHashAggregate;
use super::push_limit::PushLimit;
use super::PushOp;

pub enum MonoPipeline {
    Filter(PushFilter),
    Project(PushProject),
    Aggregate(PushHashAggregate),
    Limit(PushLimit),
    Chain { head: Box<MonoPipeline>, tail: Box<MonoPipeline> },
    Source { batches: Vec<RecordBatch>, pos: usize },
}

impl MonoPipeline {
    pub fn build(plan: &LogicalPlan) -> Result<Self> {
        match plan {
            LogicalPlan::Scan(_) => {
                Ok(MonoPipeline::Source { batches: Vec::new(), pos: 0 })
            }
            LogicalPlan::Filter(f) => {
                let child = Self::build(&f.input)?;
                let push = PushFilter::new(f.predicate.clone(), schema_of(&f.input));
                Ok(MonoPipeline::Chain {
                    head: Box::new(child),
                    tail: Box::new(MonoPipeline::Filter(push)),
                })
            }
            LogicalPlan::Project(p) => {
                let child = Self::build(&p.input)?;
                let push = PushProject::new(p.expressions.clone(), p.aliases.clone());
                Ok(MonoPipeline::Chain {
                    head: Box::new(child),
                    tail: Box::new(MonoPipeline::Project(push)),
                })
            }
            LogicalPlan::Aggregate(a) => {
                let child = Self::build(&a.input)?;
                let input_schema = schema_of(&a.input);
                let push = PushHashAggregate::new(
                    a.group_by.clone(),
                    a.aggregates.clone(),
                    input_schema,
                );
                Ok(MonoPipeline::Chain {
                    head: Box::new(child),
                    tail: Box::new(MonoPipeline::Aggregate(push)),
                })
            }
            LogicalPlan::Limit(l) => {
                let child = Self::build(&l.input)?;
                let push = PushLimit::new(l.limit, l.offset);
                Ok(MonoPipeline::Chain {
                    head: Box::new(child),
                    tail: Box::new(MonoPipeline::Limit(push)),
                })
            }
            _ => Ok(MonoPipeline::Source { batches: Vec::new(), pos: 0 }),
        }
    }

    pub fn execute(&mut self, source_batches: Vec<RecordBatch>) -> Result<Option<RecordBatch>> {
        match self {
            MonoPipeline::Source { batches, pos } => {
                *batches = source_batches;
                *pos = 0;
                if batches.is_empty() {
                    return Ok(None);
                }
                let schema = batches[0].schema.clone();
                let num_cols = schema.columns.len();
                let mut columns = Vec::with_capacity(num_cols);
                for col_idx in 0..num_cols {
                    let dt = schema.columns[col_idx].data_type;
                    let mut col = crate::types::array::ColumnArray::empty(dt);
                    for batch in batches.iter() {
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
            MonoPipeline::Filter(push) => {
                let mut push_op = std::mem::replace(push, PushFilter::new(
                    LogicalExpr::Literal(crate::types::scalar::ScalarValue::Bool(Some(true))),
                    crate::types::batch::Schema::empty(),
                ));
                for batch in source_batches {
                    push_op.push(batch)?;
                }
                Box::new(push_op).finalize()
            }
            MonoPipeline::Project(push) => {
                for batch in source_batches {
                    push.push(batch)?;
                }
                let taken = std::mem::replace(push, PushProject::new(Vec::new(), Vec::new()));
                Box::new(taken).finalize()
            }
            MonoPipeline::Aggregate(push) => {
                for batch in source_batches {
                    push.push(batch)?;
                }
                let schema = crate::types::batch::Schema::empty();
                let taken = std::mem::replace(push, PushHashAggregate::new(
                    Vec::new(), Vec::new(), schema,
                ));
                Box::new(taken).finalize()
            }
            MonoPipeline::Limit(push) => {
                for batch in source_batches {
                    push.push(batch)?;
                }
                let taken = std::mem::replace(push, PushLimit::new(0, 0));
                Box::new(taken).finalize()
            }
            MonoPipeline::Chain { head, tail } => {
                let intermediate = head.execute(source_batches)?;
                match intermediate {
                    Some(batch) => tail.execute(vec![batch]),
                    None => Ok(None),
                }
            }
        }
    }
}

fn schema_of(plan: &LogicalPlan) -> crate::types::batch::Schema {
    match plan {
        LogicalPlan::Scan(s) => s.schema.clone(),
        LogicalPlan::Filter(f) => schema_of(&f.input),
        LogicalPlan::Project(p) => {
            let _input_schema = schema_of(&p.input);
            let mut cols = Vec::new();
            for (i, _expr) in p.expressions.iter().enumerate() {
                let name = p.aliases[i].as_deref().unwrap_or("expr");
                let dt = crate::protocol::types::DataType::Int64;
                cols.push(crate::types::batch::ColumnDef::new(name, dt));
            }
            crate::types::batch::Schema::new(cols)
        }
        LogicalPlan::Aggregate(a) => {
            let input_schema = schema_of(&a.input);
            let mut cols = Vec::new();
            for expr in &a.group_by {
                if let LogicalExpr::Column(idx) = expr {
                    if let Some(cd) = input_schema.columns.get(*idx) {
                        cols.push(cd.clone());
                    }
                }
            }
            for agg in &a.aggregates {
                let name = format!("{:?}", agg.func);
                cols.push(crate::types::batch::ColumnDef::new(&name, crate::protocol::types::DataType::Float64));
            }
            crate::types::batch::Schema::new(cols)
        }
        LogicalPlan::Sort(s) => schema_of(&s.input),
        LogicalPlan::Limit(l) => schema_of(&l.input),
        LogicalPlan::Join(j) => j.output_schema.clone(),
        LogicalPlan::TopN(t) => schema_of(&t.input),
        LogicalPlan::Distinct(d) => schema_of(&d.input),
        LogicalPlan::Window(w) => schema_of(&w.input),
        LogicalPlan::Empty => crate::types::batch::Schema::empty(),
    }
}
