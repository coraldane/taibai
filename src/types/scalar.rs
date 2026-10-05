use std::fmt;
use std::hash::{Hash, Hasher};

use serde::{Deserialize, Serialize};

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

    /// Total order over scalars for sorting and merging: ORDER BY, TopN,
    /// spill-merge cursors, min/max folds, zone maps and sort keys.
    ///
    /// NULLs are placed by `null_order` (`Greater` = nulls last in ascending
    /// order, `Less` = nulls first). Numeric pairs — integers, decimals and
    /// floats in any mix — compare by value promotion. Remaining same-type
    /// pairs compare naturally; anything else falls back to comparing Display
    /// text, which keeps the result a deterministic total order (NaN vs any
    /// number also lands here and sorts after it, like `"NaN" > "1"`).
    ///
    /// This is the single implementation behind the former four drifting
    /// copies (`sort_key::compare_scalars_for_sort`, `sort::
    /// compare_scalars_null_last`, `aggregate::compare_scalars`,
    /// `query::union_scalar_cmp`).
    pub fn compare_ordered(
        &self,
        other: &Self,
        null_order: std::cmp::Ordering,
    ) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        match (self.is_null(), other.is_null()) {
            (true, true) => return Ordering::Equal,
            (true, false) => return null_order,
            (false, true) => return null_order.reverse(),
            (false, false) => {}
        }
        if let Some(ord) = self.numeric_cmp(other) {
            return ord;
        }
        match (self, other) {
            (ScalarValue::VarChar(Some(l)), ScalarValue::VarChar(Some(r))) => l.cmp(r),
            (ScalarValue::Bool(Some(l)), ScalarValue::Bool(Some(r))) => l.cmp(r),
            (ScalarValue::Timestamp(Some(l)), ScalarValue::Timestamp(Some(r))) => l.cmp(r),
            (ScalarValue::Date(Some(l)), ScalarValue::Date(Some(r))) => l.cmp(r),
            (ScalarValue::Time(Some(l)), ScalarValue::Time(Some(r))) => l.cmp(r),
            (ScalarValue::Interval(Some(l)), ScalarValue::Interval(Some(r))) => l.cmp(r),
            (ScalarValue::Blob(Some(l)), ScalarValue::Blob(Some(r))) => l.cmp(r),
            _ => self.to_string().cmp(&other.to_string()),
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

/// Canonical bit pattern for using a float as an equality/hash key (hot-store
/// point lookup, join and GROUP BY keys). SQL `=` treats `-0.0 == 0.0`, so the
/// sign bit of zero is folded away; every NaN payload folds to the canonical
/// quiet NaN so NaN rows group together instead of splitting per bit pattern.
/// Raw `to_bits()` keys made `WHERE k = 0.0` miss `-0.0` rows in the hot store.
#[inline]
pub fn float_key_bits(v: f64) -> u64 {
    if v.is_nan() {
        0x7ff8_0000_0000_0000
    } else {
        (v + 0.0).to_bits()
    }
}

/// f32-domain twin of [`float_key_bits`] for 4-byte key formats (the
/// `encode_key_at` payload width must stay f32-sized for Float32 columns).
#[inline]
pub fn float32_key_bits(v: f32) -> u32 {
    if v.is_nan() {
        0x7fc0_0000
    } else {
        (v + 0.0).to_bits()
    }
}

#[cfg(test)]
#[path = "scalar_test.rs"]
mod tests;
