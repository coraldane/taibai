use crate::protocol::types::DataType;

pub fn common_type(a: &DataType, b: &DataType) -> DataType {
    if a == b {
        return *a;
    }
    match (a, b) {
        (DataType::Decimal, _) | (_, DataType::Decimal) => DataType::Decimal,
        (DataType::VarChar, DataType::VarChar) => DataType::VarChar,
        (DataType::Float64, _) | (_, DataType::Float64) => DataType::Float64,
        (DataType::Float32, _) | (_, DataType::Float32) => DataType::Float32,
        (DataType::UInt64, _) | (_, DataType::UInt64) => DataType::UInt64,
        (DataType::Int64, _) | (_, DataType::Int64) => DataType::Int64,
        (DataType::UInt32, _) | (_, DataType::UInt32) => DataType::UInt32,
        (DataType::Int32, _) | (_, DataType::Int32) => DataType::Int32,
        (DataType::UInt16, _) | (_, DataType::UInt16) => DataType::UInt16,
        (DataType::Int16, _) | (_, DataType::Int16) => DataType::Int16,
        (DataType::UInt8, _) | (_, DataType::UInt8) => DataType::UInt8,
        (DataType::Int8, _) | (_, DataType::Int8) => DataType::Int8,
        _ => DataType::Int64,
    }
}
