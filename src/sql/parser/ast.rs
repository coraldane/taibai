use crate::protocol::types::DataType;
use crate::types::scalar::ScalarValue;

/// SQL Abstract Syntax Tree.
#[derive(Debug, Clone)]
pub enum Statement {
    CreateTable {
        name: String,
        columns: Vec<ColumnDefAst>,
        if_not_exists: bool,
        sort_key: Option<Vec<String>>,
        partition_by: Option<PartitionDef>,
    },
    DropTable {
        name: String,
        if_exists: bool,
    },
    Insert {
        table: String,
        columns: Vec<String>,
        values: Vec<Vec<Expr>>,
    },
    Update {
        table: String,
        assignments: Vec<Assignment>,
        where_clause: Option<Expr>,
    },
    Delete {
        table: String,
        where_clause: Option<Expr>,
    },
    Select {
        select_list: Vec<SelectItem>,
        from: Option<FromClause>,
        where_clause: Option<Expr>,
        group_by: Vec<Expr>,
        having: Option<Expr>,
        order_by: Vec<OrderByItem>,
        limit: Option<Expr>,
        offset: Option<Expr>,
        distinct: bool,
    },
    ShowTables,
    ShowDatabases,
    UseDatabase(String),
    /// EXPLAIN [ANALYZE] <stmt>: returns the logical plan of an inner statement
    /// (and optionally the actual scan statistics from a real execution).
    Explain { analyze: bool, stmt: Box<Statement> },
}

/// Assignment for UPDATE statement: column = value.
#[derive(Debug, Clone)]
pub struct Assignment {
    pub column: String,
    pub value: Expr,
}

#[derive(Debug, Clone)]
pub struct ColumnDefAst {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub scale: i32,
    pub precision: i32,
}

#[derive(Debug, Clone)]
pub struct PartitionDef {
    pub column: String,
    pub strategy: PartitionStrategy,
}

#[derive(Debug, Clone)]
pub enum PartitionStrategy {
    Day,
    Year,
    Value,
}

/// JOIN type specifier.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
    /// Semi join: returns left rows that have a match in right (no right columns in output)
    Semi,
    /// Anti join: returns left rows that do NOT have a match in right
    Anti,
    /// Asof join: matches closest timestamp (used in time-series)
    Asof,
}

/// A single JOIN clause in a FROM clause.
#[derive(Debug, Clone)]
pub struct JoinClause {
    pub join_type: JoinType,
    pub right: TableRef,
    pub condition: Option<Expr>, // ON clause, None for CROSS JOIN
}

/// FROM clause: base table with optional chained JOINs.
#[derive(Debug, Clone)]
pub enum FromClause {
    Table(TableRef),
    Join(JoinExpr),
}

/// Join expression with left side and chained JOINs.
#[derive(Debug, Clone)]
pub struct JoinExpr {
    pub left: Box<FromClause>,
    pub joins: Vec<JoinClause>, // chained joins
}

#[derive(Debug, Clone)]
pub enum SelectItem {
    Star,
    Expr(Expr, Option<String>), // (expr, alias)
}

#[derive(Debug, Clone)]
pub struct TableRef {
    pub name: String,
    pub alias: Option<String>,
}

#[derive(Debug, Clone)]
pub struct OrderByItem {
    pub expr: Expr,
    pub desc: bool,
}

/// SQL Expression AST - enhanced with IN, BETWEEN, CASE, LIKE, Subquery support.
#[derive(Debug, Clone)]
pub enum Expr {
    Literal(ScalarValue),
    Column(String),
    QualifiedColumn {
        table: String,
        column: String,
    },
    BinaryOp {
        left: Box<Expr>,
        op: BinOp,
        right: Box<Expr>,
    },
    UnaryNot(Box<Expr>),
    UnaryMinus(Box<Expr>),
    IsNull(Box<Expr>),
    IsNotNull(Box<Expr>),
    /// IN expression: expr IN (values...) or expr IN (subquery)
    In {
        expr: Box<Expr>,
        values: Vec<Expr>,
        negated: bool,
    },
    /// BETWEEN expression: expr BETWEEN low AND high
    Between {
        expr: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
        negated: bool,
    },
    /// CASE WHEN ... THEN ... ELSE ... END
    Case {
        operand: Option<Box<Expr>>,
        when_clauses: Vec<(Expr, Expr)>, // (condition, result)
        else_clause: Option<Box<Expr>>,
    },
    /// LIKE pattern matching: expr LIKE pattern
    Like {
        expr: Box<Expr>,
        pattern: Box<Expr>,
        negated: bool,
    },
    /// EXISTS subquery
    Exists(Box<Expr>),
    /// Scalar subquery (returns a single value)
    Subquery(Box<Statement>),
    Function {
        name: String,
        args: Vec<Expr>,
        distinct: bool,
    },
    /// Window function: func OVER (PARTITION BY ... ORDER BY ...)
    WindowFunction {
        func: WindowFunc,
        partition_by: Vec<Expr>,
        order_by: Vec<OrderByItem>,
        frame: Option<WindowFrame>,
    },
}

/// Window function types
#[derive(Debug, Clone)]
pub enum WindowFunc {
    RowNumber,
    Rank,
    DenseRank,
    Lead { offset: usize, default: Option<Box<Expr>> },
    Lag { offset: usize, default: Option<Box<Expr>> },
    Aggregate { func: String, args: Vec<Expr>, distinct: bool },
}

/// Window frame specification
#[derive(Debug, Clone)]
pub struct WindowFrame {
    pub frame_type: FrameType,
    pub start: FrameBound,
    pub end: FrameBound,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FrameType {
    Rows,
    Range,
}

#[derive(Debug, Clone)]
pub enum FrameBound {
    UnboundedPreceding,
    Preceding(usize),
    CurrentRow,
    Following(usize),
    UnboundedFollowing,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BinOp {
    Eq,
    Neq,
    Lt,
    Gt,
    Lte,
    Gte,
    And,
    Or,
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Like,
    NotLike,
    In,
    NotIn,
}
