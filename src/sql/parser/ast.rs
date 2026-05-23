use crate::protocol::types::DataType;
use crate::types::scalar::ScalarValue;

#[derive(Debug, Clone, PartialEq)]
pub enum AlterOperation {
    AddColumn { column: ColumnDefAst },
    DropColumn { name: String },
    RenameColumn { old_name: String, new_name: String },
    RenameTable { new_name: String },
    DropPartition { partition_key: String },
    TruncatePartition { partition_key: String },
}

#[derive(Debug, Clone, PartialEq)]
pub enum AlterUserOperation {
    SetPassword { password: String },
    Lock,
    Unlock,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Privilege {
    All,
    Select,
    Insert,
    Update,
    Delete,
    Create,
    Drop,
    Alter,
    Truncate,
}

#[derive(Debug, Clone, PartialEq)]
pub enum GrantObject {
    Table(String),
    Database(String),
    AllTables,
    AllDatabases,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    CreateTable {
        name: String,
        columns: Vec<ColumnDefAst>,
        if_not_exists: bool,
        cluster_by: Option<Vec<String>>,
        partition_by: Option<PartitionDef>,
    },
    DropTable {
        name: String,
        if_exists: bool,
    },
    AlterTable {
        name: String,
        operations: Vec<AlterOperation>,
    },
    Truncate {
        table: String,
    },
    CreateDatabase {
        name: String,
        if_not_exists: bool,
    },
    DropDatabase {
        name: String,
        if_exists: bool,
    },
    CreateUser {
        name: String,
        password: String,
    },
    AlterUser {
        name: String,
        operations: Vec<AlterUserOperation>,
    },
    CreateRole {
        name: String,
    },
    DropUser {
        name: String,
        if_exists: bool,
    },
    DropRole {
        name: String,
        if_exists: bool,
    },
    Grant {
        privileges: Vec<Privilege>,
        on: Option<GrantObject>,
        to: String,
    },
    Revoke {
        privileges: Vec<Privilege>,
        on: Option<GrantObject>,
        from: String,
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
    Union {
        left: Box<Statement>,
        right: Box<Statement>,
        all: bool,
        order_by: Vec<OrderByItem>,
        limit: Option<Expr>,
        offset: Option<Expr>,
    },
    ShowTables,
    ShowDatabases,
    ShowColumns { table: String },
    ShowIndexes { table: String },
    ShowCreateTable { table: String },
    ShowUsers,
    ShowRoles,
    ShowGrants { user: Option<String> },
    ShowPartitions { table: Option<String> },
    ShowStats,
    ShowEngines,
    ShowProcesslist { full: bool },
    UseDatabase(String),
    Explain { analyze: bool, stmt: Box<Statement> },
    Begin,
    Commit,
    Rollback,
    FlushTable { table: Option<String> },
    OptimizeTable { table: String },
    PauseFlush,
    ResumeFlush,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Assignment {
    pub column: String,
    pub value: Expr,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDefAst {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub scale: i32,
    pub precision: i32,
    pub auto_increment: bool,
}

impl ColumnDefAst {
    pub fn new(name: String, data_type: DataType) -> Self {
        Self {
            name,
            data_type,
            nullable: true,
            scale: 0,
            precision: 0,
            auto_increment: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum PartitionStrategy {
    Day,
    Year,
    Month,
    Value,
    Hash(i32),
}

#[derive(Debug, Clone, PartialEq)]
pub struct PartitionDef {
    pub column: String,
    pub strategy: PartitionStrategy,
    pub param: Option<i32>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
    Semi,
    Anti,
    Asof,
}

#[derive(Debug, Clone, PartialEq)]
pub struct JoinClause {
    pub join_type: JoinType,
    pub right: TableRef,
    pub condition: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FromClause {
    Table(TableRef),
    Join(JoinExpr),
}

#[derive(Debug, Clone, PartialEq)]
pub struct JoinExpr {
    pub left: Box<FromClause>,
    pub joins: Vec<JoinClause>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SelectItem {
    Star,
    Expr(Expr, Option<String>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct TableRef {
    pub name: String,
    pub alias: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderByItem {
    pub expr: Expr,
    pub desc: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Literal(ScalarValue),
    Placeholder(usize),
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
    In {
        expr: Box<Expr>,
        values: Vec<Expr>,
        negated: bool,
    },
    Between {
        expr: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
        negated: bool,
    },
    Case {
        operand: Option<Box<Expr>>,
        when_clauses: Vec<(Expr, Expr)>,
        else_clause: Option<Box<Expr>>,
    },
    Like {
        expr: Box<Expr>,
        pattern: Box<Expr>,
        negated: bool,
    },
    Exists(Box<Expr>),
    Subquery(Box<Statement>),
    Function {
        name: String,
        args: Vec<Expr>,
        distinct: bool,
    },
    WindowFunction {
        func: WindowFunc,
        partition_by: Vec<Expr>,
        order_by: Vec<OrderByItem>,
        frame: Option<WindowFrame>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum WindowFunc {
    RowNumber,
    Rank,
    DenseRank,
    Lead { offset: usize, default: Option<Box<Expr>> },
    Lag { offset: usize, default: Option<Box<Expr>> },
    Aggregate { func: String, args: Vec<Expr>, distinct: bool },
}

#[derive(Debug, Clone, PartialEq)]
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

#[derive(Debug, Clone, PartialEq)]
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
