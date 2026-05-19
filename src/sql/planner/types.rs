use std::collections::HashMap;

use crate::sql::parser::ast::*;
use crate::types::batch::Schema;
use crate::types::scalar::ScalarValue;

#[derive(Debug, Clone)]
pub enum LogicalPlan {
    Scan(ScanPlan),
    Filter(FilterPlan),
    Project(ProjectPlan),
    Aggregate(AggregatePlan),
    Sort(SortPlan),
    Limit(LimitPlan),
    Join(JoinPlan),
    Empty,
}

#[derive(Debug, Clone)]
pub struct ScanPlan {
    pub table_name: String,
    pub schema: Schema,
    pub projection: Option<Vec<usize>>,
    pub pushed_filter: Option<LogicalExpr>,
    pub zone_map_filters: Vec<ZoneMapFilter>,
    pub pushed_limit: Option<usize>,
    pub index_columns: Vec<String>,
    pub time_range: Option<TimeRange>,
    pub bloom_filter_columns: Vec<String>,
}

impl ScanPlan {
    pub fn new(table_name: String, schema: Schema) -> Self {
        Self {
            table_name,
            schema,
            projection: None,
            pushed_filter: None,
            zone_map_filters: Vec::new(),
            pushed_limit: None,
            index_columns: Vec::new(),
            time_range: None,
            bloom_filter_columns: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct FilterPlan {
    pub predicate: LogicalExpr,
    pub input: Box<LogicalPlan>,
}

#[derive(Debug, Clone)]
pub struct ProjectPlan {
    pub expressions: Vec<LogicalExpr>,
    pub aliases: Vec<Option<String>>,
    pub input: Box<LogicalPlan>,
}

#[derive(Debug, Clone)]
pub struct AggregatePlan {
    pub group_by: Vec<LogicalExpr>,
    pub aggregates: Vec<AggregateExpr>,
    pub input: Box<LogicalPlan>,
}

#[derive(Debug, Clone)]
pub struct SortPlan {
    pub sort_keys: Vec<SortKey>,
    pub input: Box<LogicalPlan>,
}

#[derive(Debug, Clone)]
pub struct LimitPlan {
    pub limit: usize,
    pub offset: usize,
    pub input: Box<LogicalPlan>,
}

#[derive(Debug, Clone)]
pub struct JoinPlan {
    pub left: Box<LogicalPlan>,
    pub right: Box<LogicalPlan>,
    pub join_type: JoinType,
    pub condition: LogicalExpr,
    pub left_schema: Schema,
    pub right_schema: Schema,
    pub output_schema: Schema,
}

#[derive(Debug, Clone)]
pub struct SortKey {
    pub expr: LogicalExpr,
    pub desc: bool,
}

#[derive(Debug, Clone)]
pub enum LogicalExpr {
    Column(usize),
    Literal(ScalarValue),
    BinaryOp {
        left: Box<LogicalExpr>,
        op: BinOp,
        right: Box<LogicalExpr>,
    },
    Not(Box<LogicalExpr>),
    IsNull(Box<LogicalExpr>),
    IsNotNull(Box<LogicalExpr>),
    Aggregate(AggregateExpr),
    Wildcard,
}

#[derive(Debug, Clone)]
pub struct AggregateExpr {
    pub func: AggFunc,
    pub arg: Box<LogicalExpr>,
    pub distinct: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

#[derive(Debug, Clone)]
pub struct ZoneMapFilter {
    pub column: String,
    pub min_value: ScalarValue,
    pub max_value: ScalarValue,
}

#[derive(Debug, Clone)]
pub struct TimeRange {
    pub column: String,
    pub start: Option<ScalarValue>,
    pub end: Option<ScalarValue>,
}

pub fn merge_schemas(left: &Schema, right: &Schema) -> Schema {
    let mut columns = left.columns.clone();
    columns.extend(right.columns.clone());
    Schema::new(columns)
}

pub fn eval_const_usize(expr: &Expr) -> crate::common::error::Result<usize> {
    match expr {
        Expr::Literal(ScalarValue::Int64(Some(v))) => Ok(*v as usize),
        Expr::Literal(ScalarValue::Int32(Some(v))) => Ok(*v as usize),
        _ => Err(crate::common::error::MonaxError::Sql("expected integer literal for LIMIT/OFFSET".into())),
    }
}
