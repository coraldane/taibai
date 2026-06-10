use serde::{Deserialize, Serialize};

use crate::common::error::{TaibaiError, Result};

/// Wire data type enum (19 types, aligned with taibai protocol).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum DataType {
    Bool = 1,
    Int8 = 2,
    Int16 = 3,
    Int32 = 4,
    Int64 = 5,
    UInt8 = 6,
    UInt16 = 7,
    UInt32 = 8,
    UInt64 = 9,
    Float32 = 10,
    Float64 = 11,
    Decimal = 12,
    Date = 13,
    Time = 14,
    Timestamp = 15,
    Interval = 16,
    VarChar = 17,
    Blob = 18,
    Vector = 19,
}

impl DataType {
    pub fn from_u8(v: u8) -> Result<Self> {
        match v {
            1 => Ok(DataType::Bool),
            2 => Ok(DataType::Int8),
            3 => Ok(DataType::Int16),
            4 => Ok(DataType::Int32),
            5 => Ok(DataType::Int64),
            6 => Ok(DataType::UInt8),
            7 => Ok(DataType::UInt16),
            8 => Ok(DataType::UInt32),
            9 => Ok(DataType::UInt64),
            10 => Ok(DataType::Float32),
            11 => Ok(DataType::Float64),
            12 => Ok(DataType::Decimal),
            13 => Ok(DataType::Date),
            14 => Ok(DataType::Time),
            15 => Ok(DataType::Timestamp),
            16 => Ok(DataType::Interval),
            17 => Ok(DataType::VarChar),
            18 => Ok(DataType::Blob),
            19 => Ok(DataType::Vector),
            b'0' => Ok(DataType::Bool),
            b'1' => Ok(DataType::Int8),
            b'2' => Ok(DataType::Int16),
            b'3' => Ok(DataType::Int32),
            b'4' => Ok(DataType::Int64),
            b'5' => Ok(DataType::UInt8),
            b'6' => Ok(DataType::UInt16),
            b'7' => Ok(DataType::UInt32),
            b'8' => Ok(DataType::UInt64),
            b'9' => Ok(DataType::Float32),
            b':' => Ok(DataType::Float64),
            b';' => Ok(DataType::Decimal),
            b'<' => Ok(DataType::Date),
            b'=' => Ok(DataType::Time),
            b'>' => Ok(DataType::Timestamp),
            b'?' => Ok(DataType::Interval),
            b'@' => Ok(DataType::VarChar),
            b'A' => Ok(DataType::Blob),
            b'B' => Ok(DataType::Vector),
            0 => Ok(DataType::Bool),
            _ => Err(TaibaiError::Protocol(format!("unknown DataType: {v}"))),
        }
    }

    /// Returns the fixed byte size for fixed-width types, or None for variable-length types.
    pub fn fixed_size(&self) -> Option<usize> {
        match self {
            DataType::Bool => Some(1),
            DataType::Int8 => Some(1),
            DataType::Int16 => Some(2),
            DataType::Int32 => Some(4),
            DataType::Int64 => Some(8),
            DataType::UInt8 => Some(1),
            DataType::UInt16 => Some(2),
            DataType::UInt32 => Some(4),
            DataType::UInt64 => Some(8),
            DataType::Float32 => Some(4),
            DataType::Float64 => Some(8),
            DataType::Decimal => Some(8),
            DataType::Date => Some(4),
            DataType::Time => Some(8),
            DataType::Timestamp => Some(8),
            DataType::Interval => Some(8),
            DataType::VarChar => None,
            DataType::Blob => None,
            DataType::Vector => None,
        }
    }
}

impl std::fmt::Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.display_name())
    }
}

impl DataType {
    pub fn display_name(&self) -> &'static str {
        match self {
            DataType::Bool => "BOOL",
            DataType::Int8 => "INT8",
            DataType::Int16 => "INT16",
            DataType::Int32 => "INT32",
            DataType::Int64 => "INT64",
            DataType::UInt8 => "UINT8",
            DataType::UInt16 => "UINT16",
            DataType::UInt32 => "UINT32",
            DataType::UInt64 => "UINT64",
            DataType::Float32 => "FLOAT32",
            DataType::Float64 => "FLOAT64",
            DataType::Decimal => "DECIMAL",
            DataType::Date => "DATE",
            DataType::Time => "TIME",
            DataType::Timestamp => "TIMESTAMP",
            DataType::Interval => "INTERVAL",
            DataType::VarChar => "VARCHAR",
            DataType::Blob => "BLOB",
            DataType::Vector => "VECTOR",
        }
    }
}

/// Storage type for binary/ZCPY result encoding (separate numbering from DataType).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum StorageType {
    Int64 = 0,
    Int32 = 1,
    Float64 = 2,
    Float32 = 3,
    String = 4,
    Bool = 5,
    Uint64 = 6,
    Uint32 = 7,
    Timestamp = 8,
    Decimal = 18,
}

impl StorageType {
    pub fn from_u8(v: u8) -> Result<Self> {
        match v {
            0 => Ok(StorageType::Int64),
            1 => Ok(StorageType::Int32),
            2 => Ok(StorageType::Float64),
            3 => Ok(StorageType::Float32),
            4 => Ok(StorageType::String),
            5 => Ok(StorageType::Bool),
            6 => Ok(StorageType::Uint64),
            7 => Ok(StorageType::Uint32),
            8 => Ok(StorageType::Timestamp),
            18 => Ok(StorageType::Decimal),
            _ => Err(TaibaiError::Protocol(format!("unknown StorageType: {v}"))),
        }
    }

    pub fn to_data_type(&self) -> DataType {
        match self {
            StorageType::Int64 => DataType::Int64,
            StorageType::Int32 => DataType::Int32,
            StorageType::Float64 => DataType::Float64,
            StorageType::Float32 => DataType::Float32,
            StorageType::String => DataType::VarChar,
            StorageType::Bool => DataType::Bool,
            StorageType::Uint64 => DataType::UInt64,
            StorageType::Uint32 => DataType::UInt32,
            StorageType::Timestamp => DataType::Timestamp,
            StorageType::Decimal => DataType::Decimal,
        }
    }
}
