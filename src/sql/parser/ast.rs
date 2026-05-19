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
        order_by: Vec<OrderByItem>,
        limit: Option<Expr>,
        offset: Option<Expr>,
        distinct: bool,
    },
    ShowTables,
    ShowDatabases,
    UseDatabase(String),
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
    Cross,
}

/// A single JOIN clause in a FROM clause.
#[derive(Debug, Clone)]
pub struct JoinClause {
    pub join_type: JoinType,
    pub right: TableRef,
    pub condition: Option<Expr>,  // ON clause, None for CROSS JOIN
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
    pub joins: Vec<JoinClause>,  // chained joins
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
    IsNull(Box<Expr>),
    IsNotNull(Box<Expr>),
    Function {
        name: String,
        args: Vec<Expr>,
        distinct: bool,
    },
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
}