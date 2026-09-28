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
    SetHot { hot: HotDef },
    SetTtl { ttl: TtlDef },
    DropTtl,
    /// `SET COMPRESSION_LEVEL = n`; `None` means `DEFAULT`, resetting the
    /// table to follow the global compression level again.
    SetCompressionLevel { level: Option<i32> },
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
#[allow(clippy::large_enum_variant)]
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
        hot: Option<HotDef>,
        durability: Option<DurabilityLevel>,
        /// Per-table zstd compression level (1..=22); `None` follows the
        /// global `compression.bulk_zstd_level` config.
        compression_level: Option<i32>,
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
    ReplaceSelect {
        table: ObjectName,
        columns: Vec<String>,
        query: Box<Statement>,
    },
    /// `INSERT INTO t [(cols)] SELECT ...`: rows come from a SELECT executed
    /// against the current snapshot, then go through the normal INSERT path.
    InsertSelect {
        table: ObjectName,
        columns: Vec<String>,
        query: Box<Statement>,
    },
    Update {
        table: ObjectName,
        /// Optional `UPDATE t [AS] alias` table alias; usable as the column
        /// qualifier in SET targets and the WHERE clause.
        alias: Option<String>,
        assignments: Vec<Assignment>,
        where_clause: Option<Expr>,
    },
    Delete {
        table: ObjectName,
        /// Optional `DELETE FROM t [AS] alias` table alias; usable as the
        /// column qualifier in the WHERE clause.
        alias: Option<String>,
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
        op: SetOp,
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
    ShowPerformanceBaselines,
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
    AnalyzeTable {
        table: ObjectName,
    },
    PauseFlush,
    ResumeFlush,
    ResetPerformanceBaselines,
    KillQuery {
        query_id: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetOp {
    /// `UNION` / `UNION ALL`.
    Union,
    /// `INTERSECT`: distinct rows present in both operands.
    Intersect,
    /// `EXCEPT`: distinct rows of the left operand missing from the right.
    Except,
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
pub struct HotDef {
    pub time_column: String,
    pub index_column: Option<String>,
    pub close_time: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TtlDef {
    pub column: String,
    pub duration_secs: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurabilityLevel {
    Strict,
    Relaxed,
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
    /// `LEFT ASOF JOIN`: asof matching that preserves unmatched left rows
    /// with NULL right-side columns (the kdb `aj` convention).
    LeftAsof,
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

/// The shape of a correlated subquery occurrence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CorrelatedKind {
    /// `EXISTS (SELECT ...)` / `NOT EXISTS (SELECT ...)`
    Exists { negated: bool },
    /// `(SELECT ...)` used as a scalar value; must yield at most one row.
    Scalar,
    /// `<lhs> IN (SELECT ...)` / `NOT IN`; `lhs` is evaluated in the outer scope.
    In { negated: bool },
}

/// A bound correlated subquery. Produced only by the execution layer's
/// binding pass; the parser never creates this node.
#[derive(Debug, Clone, PartialEq)]
pub struct CorrelatedSubquery {
    /// Registry key for the execution-time snapshot of the inner query.
    pub id: u64,
    pub kind: CorrelatedKind,
    /// For `CorrelatedKind::In`, the left-hand side expression (outer scope).
    pub lhs: Option<Box<Expr>>,
    /// The inner statement with correlated WHERE conjuncts removed and outer
    /// references replaced by `Expr::Placeholder(id)` markers.
    pub stmt: Box<Statement>,
    /// The WHERE conjuncts that reference the outer block, still carrying
    /// placeholder markers. Re-inserted (with markers substituted) before each
    /// per-row execution.
    pub conjuncts: Vec<Expr>,
    /// `placeholder id -> column index in the enclosing block's schema`.
    pub params: Vec<(usize, usize)>,
    /// Data type of the value this node evaluates to (captured at bind time).
    pub result_type: crate::protocol::types::DataType,
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
    /// A correlated subquery left in place by the execution layer's binding
    /// pass (`query::bind_correlated_subquery`). The inner statement's
    /// references to the enclosing query block were rewritten to
    /// `Expr::Placeholder(id)` markers; `params` maps each marker id to the
    /// enclosing block's column index. The execution layer evaluates the node
    /// once per outer row (see `execution::subquery_runtime`).
    Correlated(Box<CorrelatedSubquery>),
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
        arg: Box<Expr>,
        offset: usize,
        default: Option<Box<Expr>>,
    },
    Lag {
        arg: Box<Expr>,
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
