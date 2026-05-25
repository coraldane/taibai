use crate::protocol::types::DataType;
use crate::sql::parser::ast::BinOp;

pub fn resolve_binary_type(left: &DataType, op: &BinOp, right: &DataType) -> DataType {
    match op {
        BinOp::Eq | BinOp::Neq | BinOp::Lt | BinOp::Gt | BinOp::Lte | BinOp::Gte => DataType::Bool,
        BinOp::And | BinOp::Or => DataType::Bool,
        BinOp::Like | BinOp::NotLike => DataType::Bool,
        BinOp::In | BinOp::NotIn => DataType::Bool,
        BinOp::Mod => match (left, right) {
            (DataType::Float64, _) | (_, DataType::Float64) => DataType::Float64,
            (DataType::Float32, _) | (_, DataType::Float32) => DataType::Float32,
            _ => DataType::Int64,
        },
        BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Div => match (left, right) {
            (DataType::Float64, _) | (_, DataType::Float64) => DataType::Float64,
            (DataType::Float32, _) | (_, DataType::Float32) => DataType::Float32,
            (DataType::Int64, _) | (_, DataType::Int64) => DataType::Int64,
            (DataType::Int32, _) | (_, DataType::Int32) => DataType::Int32,
            _ => DataType::Int64,
        },
    }
}

pub fn is_numeric(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal
    )
}

pub fn common_type(a: &DataType, b: &DataType) -> DataType {
    if a == b {
        return *a;
    }
    match (a, b) {
        (DataType::Float64, _) | (_, DataType::Float64) => DataType::Float64,
        (DataType::Float32, _) | (_, DataType::Float32) => DataType::Float32,
        (DataType::Int64, _) | (_, DataType::Int64) => DataType::Int64,
        (DataType::Int32, _) | (_, DataType::Int32) => DataType::Int32,
        _ => DataType::Int64,
    }
}
