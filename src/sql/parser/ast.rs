#![allow(dead_code)]
use crate::protocol::types::DataType;
use crate::types::scalar::ScalarValue;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ObjectName {
    pub database: Option<String>,
    pub name: String,
}

impl ObjectName {
    pub fn bare(name: String) -> Self {
        Self {
            database: None,
            name,
        }
    }

    pub fn qualified(database: String, name: String) -> Self {
        Self {
            database: Some(database),
            name,
        }
    }

    pub fn effective_database<'a>(&'a self, default_db: &'a str) -> &'a str {
        self.database.as_deref().unwrap_or(default_db)
    }

    pub fn plan_key(&self) -> String {
        match &self.database {
            Some(db) => format!("{}.{}", db, self.name),
            None => self.name.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum AlterOperation {
    AddColumn { column: ColumnDefAst },
    DropColumn { name: String },
    AddUniqueKey { key: UniqueKeyDef },
    DropIndex { name: String },
    RenameColumn { old_name: String, new_name: String },
    ModifyColumn { column: ColumnDefAst },
    RenameTable { new_name: String },
    DropPartition { partition_key: String },
    TruncatePartition { partition_key: String },
    SetTtl { ttl: TtlDef },
    DropTtl,
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
    Subscribe,
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
    CreateIndex {
        name: String,
        table: ObjectName,
        columns: Vec<String>,
        unique: bool,
        if_not_exists: bool,
        index_type: Option<String>,
    },
    CreateTable {
        name: ObjectName,
        columns: Vec<ColumnDefAst>,
        if_not_exists: bool,
        primary_key: Vec<String>,
        unique_keys: Vec<UniqueKeyDef>,
        cluster_by: Option<Vec<String>>,
        partition_by: Option<PartitionDef>,
        ttl: Option<TtlDef>,
    },
    DropTable {
        name: ObjectName,
        if_exists: bool,
    },
    DropIndex {
        name: String,
        table: ObjectName,
        if_exists: bool,
    },
    AlterTable {
        name: ObjectName,
        operations: Vec<AlterOperation>,
    },
    Truncate {
        table: ObjectName,
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
        table: ObjectName,
        columns: Vec<String>,
        values: Vec<Vec<Expr>>,
    },
    Replace {
        table: ObjectName,
        columns: Vec<String>,
        values: Vec<Vec<Expr>>,
    },
    Update {
        table: ObjectName,
        assignments: Vec<Assignment>,
        where_clause: Option<Expr>,
    },
    Delete {
        table: ObjectName,
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
    ShowTables {
        database: Option<String>,
    },
    ShowDatabases,
    ShowColumns {
        table: ObjectName,
    },
    ShowIndexes {
        table: ObjectName,
    },
    ShowUniqueIndexStatus {
        table: ObjectName,
    },
    RebuildUniqueIndex {
        table: ObjectName,
        index_name: Option<String>,
    },
    ShowCreateTable {
        table: ObjectName,
    },
    ShowUsers,
    ShowRoles,
    ShowGrants {
        user: Option<String>,
    },
    ShowPartitions {
        table: Option<ObjectName>,
    },
    ShowStats,
    ShowEngines,
    ShowProcesslist {
        full: bool,
    },
    UseDatabase(String),
    Explain {
        analyze: bool,
        stmt: Box<Statement>,
    },
    Begin,
    Commit,
    Rollback,
    FlushTable {
        table: Option<ObjectName>,
    },
    OptimizeTable {
        table: ObjectName,
        partition: Option<String>,
    },
    PauseFlush,
    ResumeFlush,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Assignment {
    pub column: String,
    pub value: Expr,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UniqueKeyDef {
    pub name: Option<String>,
    pub columns: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDefAst {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub scale: i32,
    pub precision: i32,
    pub auto_increment: bool,
    pub primary_key: bool,
    pub unique: bool,
    pub default_value: Option<DefaultValue>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum DefaultValue {
    CurrentTimestamp,
    Literal(ScalarValue),
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
            primary_key: false,
            unique: false,
            default_value: None,
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
    Mod(i32),
}

#[derive(Debug, Clone, PartialEq)]
pub struct PartitionDef {
    pub column: String,
    pub strategy: PartitionStrategy,
    pub param: Option<i32>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TtlDef {
    pub column: String,
    pub duration_secs: i64,
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
pub enum TableRef {
    Table {
        name: ObjectName,
        alias: Option<String>,
    },
    Subquery {
        query: Box<Statement>,
        alias: String,
    },
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
    Lead {
        offset: usize,
        default: Option<Box<Expr>>,
    },
    Lag {
        offset: usize,
        default: Option<Box<Expr>>,
    },
    Aggregate {
        func: String,
        args: Vec<Expr>,
        distinct: bool,
    },
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
