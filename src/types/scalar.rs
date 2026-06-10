use std::fmt;
use std::hash::{Hash, Hasher};

use serde::{Deserialize, Serialize};

use crate::common::error::{TaibaiError, Result};
use crate::protocol::types::DataType;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ScalarValue {
    Bool(Option<bool>),
    Int8(Option<i8>),
    Int16(Option<i16>),
    Int32(Option<i32>),
    Int64(Option<i64>),
    UInt8(Option<u8>),
    UInt16(Option<u16>),
    UInt32(Option<u32>),
    UInt64(Option<u64>),
    Float32(Option<f32>),
    Float64(Option<f64>),
    Decimal(Option<i64>),
    Date(Option<i32>),
    Time(Option<i64>),
    Timestamp(Option<i64>),
    Interval(Option<i64>),
    VarChar(Option<String>),
    Blob(Option<Vec<u8>>),
    Vector(Option<Vec<f32>>),
}

impl Hash for ScalarValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            ScalarValue::Bool(v) => v.hash(state),
            ScalarValue::Int8(v) => v.hash(state),
            ScalarValue::Int16(v) => v.hash(state),
            ScalarValue::Int32(v) => v.hash(state),
            ScalarValue::Int64(v) => v.hash(state),
            ScalarValue::UInt8(v) => v.hash(state),
            ScalarValue::UInt16(v) => v.hash(state),
            ScalarValue::UInt32(v) => v.hash(state),
            ScalarValue::UInt64(v) => v.hash(state),
            ScalarValue::Float32(v) => {
                v.map(|f| f.to_bits()).hash(state);
            }
            ScalarValue::Float64(v) => {
                v.map(|f| f.to_bits()).hash(state);
            }
            ScalarValue::Decimal(v) => v.hash(state),
            ScalarValue::Date(v) => v.hash(state),
            ScalarValue::Time(v) => v.hash(state),
            ScalarValue::Timestamp(v) => v.hash(state),
            ScalarValue::Interval(v) => v.hash(state),
            ScalarValue::VarChar(v) => v.hash(state),
            ScalarValue::Blob(v) => v.hash(state),
            ScalarValue::Vector(v) => {
                if let Some(vec) = v {
                    for f in vec {
                        f.to_bits().hash(state);
                    }
                }
            }
        }
    }
}

impl Eq for ScalarValue {}

impl ScalarValue {
    pub fn integer_i128(&self) -> Option<i128> {
        match self {
            ScalarValue::Int8(Some(v)) => Some(*v as i128),
            ScalarValue::Int16(Some(v)) => Some(*v as i128),
            ScalarValue::Int32(Some(v)) => Some(*v as i128),
            ScalarValue::Int64(Some(v)) => Some(*v as i128),
            ScalarValue::UInt8(Some(v)) => Some(*v as i128),
            ScalarValue::UInt16(Some(v)) => Some(*v as i128),
            ScalarValue::UInt32(Some(v)) => Some(*v as i128),
            ScalarValue::UInt64(Some(v)) => Some(*v as i128),
            ScalarValue::Decimal(Some(v)) => Some(*v as i128),
            _ => None,
        }
    }

    pub fn numeric_f64(&self) -> Option<f64> {
        match self {
            ScalarValue::Int8(Some(v)) => Some(*v as f64),
            ScalarValue::Int16(Some(v)) => Some(*v as f64),
            ScalarValue::Int32(Some(v)) => Some(*v as f64),
            ScalarValue::Int64(Some(v)) => Some(*v as f64),
            ScalarValue::UInt8(Some(v)) => Some(*v as f64),
            ScalarValue::UInt16(Some(v)) => Some(*v as f64),
            ScalarValue::UInt32(Some(v)) => Some(*v as f64),
            ScalarValue::UInt64(Some(v)) => Some(*v as f64),
            ScalarValue::Float32(Some(v)) => Some(*v as f64),
            ScalarValue::Float64(Some(v)) => Some(*v),
            ScalarValue::Decimal(Some(v)) => Some(*v as f64),
            _ => None,
        }
    }

    pub fn numeric_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (ScalarValue::Float32(Some(l)), ScalarValue::Float32(Some(r))) => l.partial_cmp(r),
            (ScalarValue::Float64(Some(l)), ScalarValue::Float64(Some(r))) => l.partial_cmp(r),
            (ScalarValue::Float32(Some(l)), r) => (*l as f64).partial_cmp(&r.numeric_f64()?),
            (ScalarValue::Float64(Some(l)), r) => l.partial_cmp(&r.numeric_f64()?),
            (l, ScalarValue::Float32(Some(r))) => l.numeric_f64()?.partial_cmp(&(*r as f64)),
            (l, ScalarValue::Float64(Some(r))) => l.numeric_f64()?.partial_cmp(r),
            _ => Some(self.integer_i128()?.cmp(&other.integer_i128()?)),
        }
    }

    /// Returns true if this value is NULL.
    pub fn is_null(&self) -> bool {
        match self {
            ScalarValue::Bool(v) => v.is_none(),
            ScalarValue::Int8(v) => v.is_none(),
            ScalarValue::Int16(v) => v.is_none(),
            ScalarValue::Int32(v) => v.is_none(),
            ScalarValue::Int64(v) => v.is_none(),
            ScalarValue::UInt8(v) => v.is_none(),
            ScalarValue::UInt16(v) => v.is_none(),
            ScalarValue::UInt32(v) => v.is_none(),
            ScalarValue::UInt64(v) => v.is_none(),
            ScalarValue::Float32(v) => v.is_none(),
            ScalarValue::Float64(v) => v.is_none(),
            ScalarValue::Decimal(v) => v.is_none(),
            ScalarValue::Date(v) => v.is_none(),
            ScalarValue::Time(v) => v.is_none(),
            ScalarValue::Timestamp(v) => v.is_none(),
            ScalarValue::Interval(v) => v.is_none(),
            ScalarValue::VarChar(v) => v.is_none(),
            ScalarValue::Blob(v) => v.is_none(),
            ScalarValue::Vector(v) => v.is_none(),
        }
    }

    /// Returns the DataType for this scalar value.
    pub fn data_type(&self) -> DataType {
        match self {
            ScalarValue::Bool(_) => DataType::Bool,
            ScalarValue::Int8(_) => DataType::Int8,
            ScalarValue::Int16(_) => DataType::Int16,
            ScalarValue::Int32(_) => DataType::Int32,
            ScalarValue::Int64(_) => DataType::Int64,
            ScalarValue::UInt8(_) => DataType::UInt8,
            ScalarValue::UInt16(_) => DataType::UInt16,
            ScalarValue::UInt32(_) => DataType::UInt32,
            ScalarValue::UInt64(_) => DataType::UInt64,
            ScalarValue::Float32(_) => DataType::Float32,
            ScalarValue::Float64(_) => DataType::Float64,
            ScalarValue::Decimal(_) => DataType::Decimal,
            ScalarValue::Date(_) => DataType::Date,
            ScalarValue::Time(_) => DataType::Time,
            ScalarValue::Timestamp(_) => DataType::Timestamp,
            ScalarValue::Interval(_) => DataType::Interval,
            ScalarValue::VarChar(_) => DataType::VarChar,
            ScalarValue::Blob(_) => DataType::Blob,
            ScalarValue::Vector(_) => DataType::Vector,
        }
    }

    /// Encode this scalar value as text for wire serialization.
    pub fn to_text(&self) -> Option<String> {
        match self {
            ScalarValue::Bool(Some(v)) => Some(if *v { "true".into() } else { "false".into() }),
            ScalarValue::Int8(Some(v)) => Some(v.to_string()),
            ScalarValue::Int16(Some(v)) => Some(v.to_string()),
            ScalarValue::Int32(Some(v)) => Some(v.to_string()),
            ScalarValue::Int64(Some(v)) => Some(v.to_string()),
            ScalarValue::UInt8(Some(v)) => Some(v.to_string()),
            ScalarValue::UInt16(Some(v)) => Some(v.to_string()),
            ScalarValue::UInt32(Some(v)) => Some(v.to_string()),
            ScalarValue::UInt64(Some(v)) => Some(v.to_string()),
            ScalarValue::Float32(Some(v)) => Some(v.to_string()),
            ScalarValue::Float64(Some(v)) => Some(v.to_string()),
            ScalarValue::Decimal(Some(v)) => Some(v.to_string()),
            ScalarValue::Date(Some(v)) => Some(v.to_string()),
            ScalarValue::Time(Some(v)) => Some(v.to_string()),
            ScalarValue::Timestamp(Some(v)) => Some(v.to_string()),
            ScalarValue::Interval(Some(v)) => Some(v.to_string()),
            ScalarValue::VarChar(Some(v)) => Some(v.clone()),
            ScalarValue::Blob(Some(v)) => Some(format!("{:?}", v)),
            ScalarValue::Vector(Some(v)) => Some(format!("{:?}", v)),
            _ => None,
        }
    }

    /// Create a NULL value for the given DataType.
    pub fn null_for_type(dt: DataType) -> Self {
        match dt {
            DataType::Bool => ScalarValue::Bool(None),
            DataType::Int8 => ScalarValue::Int8(None),
            DataType::Int16 => ScalarValue::Int16(None),
            DataType::Int32 => ScalarValue::Int32(None),
            DataType::Int64 => ScalarValue::Int64(None),
            DataType::UInt8 => ScalarValue::UInt8(None),
            DataType::UInt16 => ScalarValue::UInt16(None),
            DataType::UInt32 => ScalarValue::UInt32(None),
            DataType::UInt64 => ScalarValue::UInt64(None),
            DataType::Float32 => ScalarValue::Float32(None),
            DataType::Float64 => ScalarValue::Float64(None),
            DataType::Decimal => ScalarValue::Decimal(None),
            DataType::Date => ScalarValue::Date(None),
            DataType::Time => ScalarValue::Time(None),
            DataType::Timestamp => ScalarValue::Timestamp(None),
            DataType::Interval => ScalarValue::Interval(None),
            DataType::VarChar => ScalarValue::VarChar(None),
            DataType::Blob => ScalarValue::Blob(None),
            DataType::Vector => ScalarValue::Vector(None),
        }
    }

    /// Parse a string value into a ScalarValue for the given DataType.
    pub fn parse_from_str(s: &str, dt: DataType) -> Result<Self> {
        if s.is_empty() {
            return Ok(Self::null_for_type(dt));
        }
        match dt {
            DataType::Bool => {
                Ok(ScalarValue::Bool(Some(s.parse::<bool>().map_err(|e| {
                    TaibaiError::Sql(format!("invalid bool '{}': {}", s, e))
                })?)))
            }
            DataType::Int8 => {
                Ok(ScalarValue::Int8(Some(s.parse::<i8>().map_err(|e| {
                    TaibaiError::Sql(format!("invalid int8 '{}': {}", s, e))
                })?)))
            }
            DataType::Int16 => {
                Ok(ScalarValue::Int16(Some(s.parse::<i16>().map_err(|e| {
                    TaibaiError::Sql(format!("invalid int16 '{}': {}", s, e))
                })?)))
            }
            DataType::Int32 => {
                Ok(ScalarValue::Int32(Some(s.parse::<i32>().map_err(|e| {
                    TaibaiError::Sql(format!("invalid int32 '{}': {}", s, e))
                })?)))
            }
            DataType::Int64 => {
                Ok(ScalarValue::Int64(Some(s.parse::<i64>().map_err(|e| {
                    TaibaiError::Sql(format!("invalid int64 '{}': {}", s, e))
                })?)))
            }
            DataType::UInt8 => {
                Ok(ScalarValue::UInt8(Some(s.parse::<u8>().map_err(|e| {
                    TaibaiError::Sql(format!("invalid uint8 '{}': {}", s, e))
                })?)))
            }
            DataType::UInt16 => Ok(ScalarValue::UInt16(Some(
                s.parse::<u16>()
                    .map_err(|e| TaibaiError::Sql(format!("invalid uint16 '{}': {}", s, e)))?,
            ))),
            DataType::UInt32 => Ok(ScalarValue::UInt32(Some(
                s.parse::<u32>()
                    .map_err(|e| TaibaiError::Sql(format!("invalid uint32 '{}': {}", s, e)))?,
            ))),
            DataType::UInt64 => Ok(ScalarValue::UInt64(Some(
                s.parse::<u64>()
                    .map_err(|e| TaibaiError::Sql(format!("invalid uint64 '{}': {}", s, e)))?,
            ))),
            DataType::Float32 => Ok(ScalarValue::Float32(Some(
                s.parse::<f32>()
                    .map_err(|e| TaibaiError::Sql(format!("invalid float32 '{}': {}", s, e)))?,
            ))),
            DataType::Float64 => Ok(ScalarValue::Float64(Some(
                s.parse::<f64>()
                    .map_err(|e| TaibaiError::Sql(format!("invalid float64 '{}': {}", s, e)))?,
            ))),
            DataType::Decimal => Ok(ScalarValue::Decimal(Some(
                s.parse::<i64>()
                    .map_err(|e| TaibaiError::Sql(format!("invalid decimal '{}': {}", s, e)))?,
            ))),
            DataType::Date => {
                Ok(ScalarValue::Date(Some(s.parse::<i32>().map_err(|e| {
                    TaibaiError::Sql(format!("invalid date '{}': {}", s, e))
                })?)))
            }
            DataType::VarChar => Ok(ScalarValue::VarChar(Some(s.to_string()))),
            DataType::Timestamp => Ok(ScalarValue::Timestamp(Some(
                s.parse::<i64>()
                    .map_err(|e| TaibaiError::Sql(format!("invalid timestamp '{}': {}", s, e)))?,
            ))),
            DataType::Time => {
                Ok(ScalarValue::Time(Some(s.parse::<i64>().map_err(|e| {
                    TaibaiError::Sql(format!("invalid time '{}': {}", s, e))
                })?)))
            }
            DataType::Interval => Ok(ScalarValue::Interval(Some(
                s.parse::<i64>()
                    .map_err(|e| TaibaiError::Sql(format!("invalid interval '{}': {}", s, e)))?,
            ))),
            DataType::Blob => Ok(ScalarValue::Blob(Some(s.as_bytes().to_vec()))),
            DataType::Vector => {
                let floats: Result<Vec<f32>> = s
                    .split(',')
                    .map(|p| {
                        p.trim().parse::<f32>().map_err(|e| {
                            TaibaiError::Sql(format!("invalid vector element '{}': {}", p, e))
                        })
                    })
                    .collect();
                Ok(ScalarValue::Vector(Some(floats?)))
            }
        }
    }
}

impl fmt::Display for ScalarValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ScalarValue::Bool(Some(v)) => write!(f, "{}", v),
            ScalarValue::Bool(None) => write!(f, "NULL"),
            ScalarValue::Int8(Some(v)) => write!(f, "{}", v),
            ScalarValue::Int8(None) => write!(f, "NULL"),
            ScalarValue::Int16(Some(v)) => write!(f, "{}", v),
            ScalarValue::Int16(None) => write!(f, "NULL"),
            ScalarValue::Int32(Some(v)) => write!(f, "{}", v),
            ScalarValue::Int32(None) => write!(f, "NULL"),
            ScalarValue::Int64(Some(v)) => write!(f, "{}", v),
            ScalarValue::Int64(None) => write!(f, "NULL"),
            ScalarValue::UInt8(Some(v)) => write!(f, "{}", v),
            ScalarValue::UInt8(None) => write!(f, "NULL"),
            ScalarValue::UInt16(Some(v)) => write!(f, "{}", v),
            ScalarValue::UInt16(None) => write!(f, "NULL"),
            ScalarValue::UInt32(Some(v)) => write!(f, "{}", v),
            ScalarValue::UInt32(None) => write!(f, "NULL"),
            ScalarValue::UInt64(Some(v)) => write!(f, "{}", v),
            ScalarValue::UInt64(None) => write!(f, "NULL"),
            ScalarValue::Float32(Some(v)) => write!(f, "{}", v),
            ScalarValue::Float32(None) => write!(f, "NULL"),
            ScalarValue::Float64(Some(v)) => write!(f, "{}", v),
            ScalarValue::Float64(None) => write!(f, "NULL"),
            ScalarValue::Decimal(Some(v)) => write!(f, "{}", v),
            ScalarValue::Decimal(None) => write!(f, "NULL"),
            ScalarValue::Date(Some(v)) => write!(f, "{}", v),
            ScalarValue::Date(None) => write!(f, "NULL"),
            ScalarValue::Time(Some(v)) => write!(f, "{}", v),
            ScalarValue::Time(None) => write!(f, "NULL"),
            ScalarValue::Timestamp(Some(v)) => write!(f, "{}", v),
            ScalarValue::Timestamp(None) => write!(f, "NULL"),
            ScalarValue::Interval(Some(v)) => write!(f, "{}", v),
            ScalarValue::Interval(None) => write!(f, "NULL"),
            ScalarValue::VarChar(Some(v)) => write!(f, "{}", v),
            ScalarValue::VarChar(None) => write!(f, "NULL"),
            ScalarValue::Blob(Some(v)) => write!(f, "{:?}", v),
            ScalarValue::Blob(None) => write!(f, "NULL"),
            ScalarValue::Vector(Some(v)) => write!(f, "{:?}", v),
            ScalarValue::Vector(None) => write!(f, "NULL"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_null_detection() {
        assert!(ScalarValue::Int32(None).is_null());
        assert!(!ScalarValue::Int32(Some(42)).is_null());
        assert!(ScalarValue::VarChar(None).is_null());
        assert!(!ScalarValue::VarChar(Some("hello".into())).is_null());
    }

    #[test]
    fn test_data_type_roundtrip() {
        assert_eq!(ScalarValue::Int64(Some(1)).data_type(), DataType::Int64);
        assert_eq!(
            ScalarValue::VarChar(Some("x".into())).data_type(),
            DataType::VarChar
        );
        assert_eq!(ScalarValue::Bool(Some(true)).data_type(), DataType::Bool);
    }

    #[test]
    fn test_parse_from_str() {
        let v = ScalarValue::parse_from_str("42", DataType::Int64).unwrap();
        assert_eq!(v, ScalarValue::Int64(Some(42)));

        let v = ScalarValue::parse_from_str("2.71", DataType::Float64).unwrap();
        assert_eq!(v, ScalarValue::Float64(Some(2.71)));

        let v = ScalarValue::parse_from_str("hello", DataType::VarChar).unwrap();
        assert_eq!(v, ScalarValue::VarChar(Some("hello".into())));

        let v = ScalarValue::parse_from_str("", DataType::Int32).unwrap();
        assert!(v.is_null());
    }

    #[test]
    fn test_null_for_type() {
        assert!(ScalarValue::null_for_type(DataType::Int64).is_null());
        assert!(ScalarValue::null_for_type(DataType::VarChar).is_null());
    }
}
