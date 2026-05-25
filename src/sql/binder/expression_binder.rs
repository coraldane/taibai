use crate::common::error::{MonaxError, Result};
use crate::sql::parser::ast::Expr;
use crate::types::scalar::ScalarValue;

use super::BindContext;

#[derive(Debug, Clone)]
pub struct BoundExpr {
    pub kind: BoundExprKind,
    pub data_type: DataType,
}

#[derive(Debug, Clone)]
pub enum BoundExprKind {
    ColumnRef {
        index: usize,
        name: String,
    },
    Literal(ScalarValue),
    BinaryOp {
        left: Box<BoundExpr>,
        op: crate::sql::parser::ast::BinOp,
        right: Box<BoundExpr>,
    },
    UnaryNot(Box<BoundExpr>),
    UnaryMinus(Box<BoundExpr>),
    IsNull(Box<BoundExpr>),
    IsNotNull(Box<BoundExpr>),
    Aggregate {
        func: crate::sql::planner::AggFunc,
        arg: Box<BoundExpr>,
        distinct: bool,
    },
    In {
        expr: Box<BoundExpr>,
        values: Vec<BoundExpr>,
        negated: bool,
    },
    Between {
        expr: Box<BoundExpr>,
        low: Box<BoundExpr>,
        high: Box<BoundExpr>,
        negated: bool,
    },
    Like {
        expr: Box<BoundExpr>,
        pattern: Box<BoundExpr>,
        negated: bool,
    },
    Wildcard,
}

use crate::protocol::types::DataType;

pub struct ExpressionBinder<'a> {
    ctx: &'a BindContext,
}

impl<'a> ExpressionBinder<'a> {
    pub fn new(ctx: &'a BindContext) -> Self {
        Self { ctx }
    }

    pub fn bind(&self, expr: &Expr) -> Result<BoundExpr> {
        match expr {
            Expr::Literal(v) => {
                let dt = v.data_type();
                Ok(BoundExpr {
                    kind: BoundExprKind::Literal(v.clone()),
                    data_type: dt,
                })
            }
            Expr::Column(name) => {
                let idx = self
                    .ctx
                    .resolve_column(name)
                    .ok_or_else(|| MonaxError::Sql(format!("column '{}' not found", name)))?;
                let dt = self.ctx.column_type(idx).unwrap_or(DataType::Int64);
                Ok(BoundExpr {
                    kind: BoundExprKind::ColumnRef {
                        index: idx,
                        name: name.clone(),
                    },
                    data_type: dt,
                })
            }
            Expr::QualifiedColumn { table, column } => {
                let idx = self
                    .ctx
                    .resolve_qualified_column(table, column)
                    .ok_or_else(|| {
                        MonaxError::Sql(format!(
                            "column '{}' not found in table '{}'",
                            column, table
                        ))
                    })?;
                let dt = self.ctx.column_type(idx).unwrap_or(DataType::Int64);
                Ok(BoundExpr {
                    kind: BoundExprKind::ColumnRef {
                        index: idx,
                        name: column.clone(),
                    },
                    data_type: dt,
                })
            }
            Expr::BinaryOp { left, op, right } => {
                let l = self.bind(left)?;
                let r = self.bind(right)?;
                let dt = crate::sql::binder::type_resolver::resolve_binary_type(
                    &l.data_type,
                    op,
                    &r.data_type,
                );
                Ok(BoundExpr {
                    kind: BoundExprKind::BinaryOp {
                        left: Box::new(l),
                        op: *op,
                        right: Box::new(r),
                    },
                    data_type: dt,
                })
            }
            Expr::UnaryNot(inner) => {
                let e = self.bind(inner)?;
                Ok(BoundExpr {
                    kind: BoundExprKind::UnaryNot(Box::new(e)),
                    data_type: DataType::Bool,
                })
            }
            Expr::IsNull(inner) => {
                let e = self.bind(inner)?;
                Ok(BoundExpr {
                    kind: BoundExprKind::IsNull(Box::new(e)),
                    data_type: DataType::Bool,
                })
            }
            Expr::IsNotNull(inner) => {
                let e = self.bind(inner)?;
                Ok(BoundExpr {
                    kind: BoundExprKind::IsNotNull(Box::new(e)),
                    data_type: DataType::Bool,
                })
            }
            Expr::Function {
                name,
                args,
                distinct,
            } => {
                let func_name = name.to_uppercase();
                let func = match func_name.as_str() {
                    "COUNT" => crate::sql::planner::AggFunc::Count,
                    "SUM" => crate::sql::planner::AggFunc::Sum,
                    "AVG" => crate::sql::planner::AggFunc::Avg,
                    "MIN" => crate::sql::planner::AggFunc::Min,
                    "MAX" => crate::sql::planner::AggFunc::Max,
                    other => return Err(MonaxError::Sql(format!("unknown function '{}'", other))),
                };
                let arg = if args.is_empty()
                    || (args.len() == 1 && matches!(&args[0], Expr::Column(n) if n == "*"))
                {
                    BoundExpr {
                        kind: BoundExprKind::Wildcard,
                        data_type: DataType::Int64,
                    }
                } else {
                    self.bind(&args[0])?
                };
                let dt = match func {
                    crate::sql::planner::AggFunc::Count => DataType::Int64,
                    _ => DataType::Float64,
                };
                Ok(BoundExpr {
                    kind: BoundExprKind::Aggregate {
                        func,
                        arg: Box::new(arg),
                        distinct: *distinct,
                    },
                    data_type: dt,
                })
            }
            // Handle new expression types with default binding
            Expr::UnaryMinus(inner) => {
                let bound = self.bind(inner)?;
                Ok(BoundExpr {
                    kind: BoundExprKind::UnaryMinus(Box::new(bound)),
                    data_type: DataType::Int64,
                })
            }
            Expr::In {
                expr,
                values,
                negated,
            } => {
                let bound_expr = self.bind(expr)?;
                let bound_values: Result<Vec<_>> = values.iter().map(|v| self.bind(v)).collect();
                Ok(BoundExpr {
                    kind: BoundExprKind::In {
                        expr: Box::new(bound_expr),
                        values: bound_values?,
                        negated: *negated,
                    },
                    data_type: DataType::Bool,
                })
            }
            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => {
                let bound_expr = self.bind(expr)?;
                let bound_low = self.bind(low)?;
                let bound_high = self.bind(high)?;
                Ok(BoundExpr {
                    kind: BoundExprKind::Between {
                        expr: Box::new(bound_expr),
                        low: Box::new(bound_low),
                        high: Box::new(bound_high),
                        negated: *negated,
                    },
                    data_type: DataType::Bool,
                })
            }
            Expr::Like {
                expr,
                pattern,
                negated,
            } => {
                let bound_expr = self.bind(expr)?;
                let bound_pattern = self.bind(pattern)?;
                Ok(BoundExpr {
                    kind: BoundExprKind::Like {
                        expr: Box::new(bound_expr),
                        pattern: Box::new(bound_pattern),
                        negated: *negated,
                    },
                    data_type: DataType::Bool,
                })
            }
            Expr::Case {
                operand: _,
                when_clauses: _,
                else_clause: _,
            } => Ok(BoundExpr {
                kind: BoundExprKind::Literal(ScalarValue::Bool(None)),
                data_type: DataType::Bool,
            }),
            Expr::Exists(_)
            | Expr::Subquery(_)
            | Expr::WindowFunction { .. }
            | Expr::Placeholder(_) => Ok(BoundExpr {
                kind: BoundExprKind::Literal(ScalarValue::Bool(None)),
                data_type: DataType::Bool,
            }),
        }
    }
}
