
use crate::sql::parser::ast::*;
use crate::types::batch::Schema;
use crate::types::scalar::ScalarValue;

#[derive(Debug, Clone, PartialEq)]
pub enum LogicalPlan {
    Scan(ScanPlan),
    Filter(FilterPlan),
    Project(ProjectPlan),
    Aggregate(AggregatePlan),
    Sort(SortPlan),
    Limit(LimitPlan),
    Join(JoinPlan),
    SemiJoin(SemiJoinPlan),
    TopN(TopNPlan),
    Distinct(DistinctPlan),
    Window(WindowPlan),
    Empty,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ScanPlan {
    pub table_name: String,
    pub schema: Schema,
    pub projection: Option<Vec<usize>>,
    pub pushed_filter: Option<LogicalExpr>,
    pub zone_map_filters: Vec<ZoneMapFilter>,
    pub pushed_limit: Option<usize>,
    pub pushed_order: Option<Vec<SortKey>>,
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
            pushed_order: None,
            index_columns: Vec::new(),
            time_range: None,
            bloom_filter_columns: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FilterPlan {
    pub predicate: LogicalExpr,
    pub input: Box<LogicalPlan>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProjectPlan {
    pub expressions: Vec<LogicalExpr>,
    pub aliases: Vec<Option<String>>,
    pub input: Box<LogicalPlan>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AggregatePlan {
    pub group_by: Vec<LogicalExpr>,
    pub aggregates: Vec<AggregateExpr>,
    pub input: Box<LogicalPlan>,
    /// Optional HAVING filter applied after aggregation
    pub having: Option<LogicalExpr>,
    /// Whether DISTINCT should be applied to output
    pub distinct: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SortPlan {
    pub sort_keys: Vec<SortKey>,
    pub input: Box<LogicalPlan>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LimitPlan {
    pub limit: usize,
    pub offset: usize,
    pub input: Box<LogicalPlan>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct JoinPlan {
    pub left: Box<LogicalPlan>,
    pub right: Box<LogicalPlan>,
    pub join_type: JoinType,
    pub condition: LogicalExpr,
    pub left_schema: Schema,
    pub right_schema: Schema,
    pub output_schema: Schema,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SemiJoinPlan {
    pub left: Box<LogicalPlan>,
    pub right: Box<LogicalPlan>,
    pub condition: LogicalExpr,
    pub left_schema: Schema,
    pub right_schema: Schema,
    pub is_anti: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TopNPlan {
    pub sort_keys: Vec<SortKey>,
    pub limit: usize,
    pub offset: usize,
    pub input: Box<LogicalPlan>,
}

/// Distinct plan.
#[derive(Debug, Clone, PartialEq)]
pub struct DistinctPlan {
    pub input: Box<LogicalPlan>,
}

/// Window function plan.
#[derive(Debug, Clone, PartialEq)]
pub struct WindowPlan {
    pub window_funcs: Vec<WindowFuncPlan>,
    pub partition_by: Vec<LogicalExpr>,
    pub order_by: Vec<SortKey>,
    pub input: Box<LogicalPlan>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WindowFuncPlan {
    pub func: WindowFunc,
    pub output_name: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SortKey {
    pub expr: LogicalExpr,
    pub desc: bool,
}

#[derive(Debug, Clone, PartialEq)]
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
    In {
        expr: Box<LogicalExpr>,
        values: Vec<ScalarValue>,
        negated: bool,
    },
    Between {
        expr: Box<LogicalExpr>,
        low: Box<LogicalExpr>,
        high: Box<LogicalExpr>,
        negated: bool,
    },
    Case {
        operand: Option<Box<LogicalExpr>>,
        when_clauses: Vec<(LogicalExpr, LogicalExpr)>,
        else_clause: Option<Box<LogicalExpr>>,
    },
    Like {
        expr: Box<LogicalExpr>,
        pattern: String,
        negated: bool,
    },
    Function {
        name: String,
        args: Vec<LogicalExpr>,
    },
    ScalarSubquery(Box<LogicalPlan>),
    ExistsSubquery(Box<LogicalPlan>),
    InSubquery {
        expr: Box<LogicalExpr>,
        subquery: Box<LogicalPlan>,
        negated: bool,
    },
    Wildcard,
}

#[derive(Debug, Clone, PartialEq)]
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

#[derive(Debug, Clone, PartialEq)]
pub struct ZoneMapFilter {
    pub column: String,
    pub min_value: ScalarValue,
    pub max_value: ScalarValue,
}

#[derive(Debug, Clone, PartialEq)]
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
