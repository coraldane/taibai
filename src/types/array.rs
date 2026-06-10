use crate::common::error::{TaibaiError, Result};
use crate::protocol::types::DataType;
use crate::types::bitmap::NullBitmap;
use crate::types::scalar::ScalarValue;

/// A typed column array with null bitmap.
/// Data is stored in typed vectors for cache efficiency; null values are
/// marked in the bitmap but still occupy space in the data vector (simpler
/// than taibai's "skip nulls" approach for in-memory representation).
#[derive(Debug, Clone)]
pub enum ColumnArray {
    Bool {
        data: Vec<u8>,
        bitmap: NullBitmap,
    },
    Int8 {
        data: Vec<i8>,
        bitmap: NullBitmap,
    },
    Int16 {
        data: Vec<i16>,
        bitmap: NullBitmap,
    },
    Int32 {
        data: Vec<i32>,
        bitmap: NullBitmap,
    },
    Int64 {
        data: Vec<i64>,
        bitmap: NullBitmap,
    },
    UInt8 {
        data: Vec<u8>,
        bitmap: NullBitmap,
    },
    UInt16 {
        data: Vec<u16>,
        bitmap: NullBitmap,
    },
    UInt32 {
        data: Vec<u32>,
        bitmap: NullBitmap,
    },
    UInt64 {
        data: Vec<u64>,
        bitmap: NullBitmap,
    },
    Float32 {
        data: Vec<f32>,
        bitmap: NullBitmap,
    },
    Float64 {
        data: Vec<f64>,
        bitmap: NullBitmap,
    },
    Decimal {
        data: Vec<i64>,
        bitmap: NullBitmap,
    },
    Date {
        data: Vec<i32>,
        bitmap: NullBitmap,
    },
    Time {
        data: Vec<i64>,
        bitmap: NullBitmap,
    },
    Timestamp {
        data: Vec<i64>,
        bitmap: NullBitmap,
    },
    Interval {
        data: Vec<i64>,
        bitmap: NullBitmap,
    },
    VarChar {
        data: Vec<String>,
        bitmap: NullBitmap,
    },
    Blob {
        data: Vec<Vec<u8>>,
        bitmap: NullBitmap,
    },
    Vector {
        data: Vec<Vec<f32>>,
        bitmap: NullBitmap,
    },
}

impl ColumnArray {
    /// Create an empty ColumnArray for the given DataType.
    pub fn empty(dt: DataType) -> Self {
        match dt {
            DataType::Bool => ColumnArray::Bool {
                data: Vec::new(),
                bitmap: NullBitmap::new_valid(0),
            },
            DataType::Int8 => ColumnArray::Int8 {
                data: Vec::new(),
                bitmap: NullBitmap::new_valid(0),
            },
            DataType::Int16 => ColumnArray::Int16 {
                data: Vec::new(),
                bitmap: NullBitmap::new_valid(0),
            },
            DataType::Int32 => ColumnArray::Int32 {
                data: Vec::new(),
                bitmap: NullBitmap::new_valid(0),
            },
            DataType::Int64 => ColumnArray::Int64 {
                data: Vec::new(),
                bitmap: NullBitmap::new_valid(0),
            },
            DataType::UInt8 => ColumnArray::UInt8 {
                data: Vec::new(),
                bitmap: NullBitmap::new_valid(0),
            },
            DataType::UInt16 => ColumnArray::UInt16 {
                data: Vec::new(),
                bitmap: NullBitmap::new_valid(0),
            },
            DataType::UInt32 => ColumnArray::UInt32 {
                data: Vec::new(),
                bitmap: NullBitmap::new_valid(0),
            },
            DataType::UInt64 => ColumnArray::UInt64 {
                data: Vec::new(),
                bitmap: NullBitmap::new_valid(0),
            },
            DataType::Float32 => ColumnArray::Float32 {
                data: Vec::new(),
                bitmap: NullBitmap::new_valid(0),
            },
            DataType::Float64 => ColumnArray::Float64 {
                data: Vec::new(),
                bitmap: NullBitmap::new_valid(0),
            },
            DataType::Decimal => ColumnArray::Decimal {
                data: Vec::new(),
                bitmap: NullBitmap::new_valid(0),
            },
            DataType::Date => ColumnArray::Date {
                data: Vec::new(),
                bitmap: NullBitmap::new_valid(0),
            },
            DataType::Time => ColumnArray::Time {
                data: Vec::new(),
                bitmap: NullBitmap::new_valid(0),
            },
            DataType::Timestamp => ColumnArray::Timestamp {
                data: Vec::new(),
                bitmap: NullBitmap::new_valid(0),
            },
            DataType::Interval => ColumnArray::Interval {
                data: Vec::new(),
                bitmap: NullBitmap::new_valid(0),
            },
            DataType::VarChar => ColumnArray::VarChar {
                data: Vec::new(),
                bitmap: NullBitmap::new_valid(0),
            },
            DataType::Blob => ColumnArray::Blob {
                data: Vec::new(),
                bitmap: NullBitmap::new_valid(0),
            },
            DataType::Vector => ColumnArray::Vector {
                data: Vec::new(),
                bitmap: NullBitmap::new_valid(0),
            },
        }
    }

    pub fn with_capacity(dt: DataType, capacity: usize) -> Self {
        let bitmap = || NullBitmap::with_capacity(capacity);
        match dt {
            DataType::Bool => ColumnArray::Bool {
                data: Vec::with_capacity(capacity),
                bitmap: bitmap(),
            },
            DataType::Int8 => ColumnArray::Int8 {
                data: Vec::with_capacity(capacity),
                bitmap: bitmap(),
            },
            DataType::Int16 => ColumnArray::Int16 {
                data: Vec::with_capacity(capacity),
                bitmap: bitmap(),
            },
            DataType::Int32 => ColumnArray::Int32 {
                data: Vec::with_capacity(capacity),
                bitmap: bitmap(),
            },
            DataType::Int64 => ColumnArray::Int64 {
                data: Vec::with_capacity(capacity),
                bitmap: bitmap(),
            },
            DataType::UInt8 => ColumnArray::UInt8 {
                data: Vec::with_capacity(capacity),
                bitmap: bitmap(),
            },
            DataType::UInt16 => ColumnArray::UInt16 {
                data: Vec::with_capacity(capacity),
                bitmap: bitmap(),
            },
            DataType::UInt32 => ColumnArray::UInt32 {
                data: Vec::with_capacity(capacity),
                bitmap: bitmap(),
            },
            DataType::UInt64 => ColumnArray::UInt64 {
                data: Vec::with_capacity(capacity),
                bitmap: bitmap(),
            },
            DataType::Float32 => ColumnArray::Float32 {
                data: Vec::with_capacity(capacity),
                bitmap: bitmap(),
            },
            DataType::Float64 => ColumnArray::Float64 {
                data: Vec::with_capacity(capacity),
                bitmap: bitmap(),
            },
            DataType::Decimal => ColumnArray::Decimal {
                data: Vec::with_capacity(capacity),
                bitmap: bitmap(),
            },
            DataType::Date => ColumnArray::Date {
                data: Vec::with_capacity(capacity),
                bitmap: bitmap(),
            },
            DataType::Time => ColumnArray::Time {
                data: Vec::with_capacity(capacity),
                bitmap: bitmap(),
            },
            DataType::Timestamp => ColumnArray::Timestamp {
                data: Vec::with_capacity(capacity),
                bitmap: bitmap(),
            },
            DataType::Interval => ColumnArray::Interval {
                data: Vec::with_capacity(capacity),
                bitmap: bitmap(),
            },
            DataType::VarChar => ColumnArray::VarChar {
                data: Vec::with_capacity(capacity),
                bitmap: bitmap(),
            },
            DataType::Blob => ColumnArray::Blob {
                data: Vec::with_capacity(capacity),
                bitmap: bitmap(),
            },
            DataType::Vector => ColumnArray::Vector {
                data: Vec::with_capacity(capacity),
                bitmap: bitmap(),
            },
        }
    }

    /// Returns the DataType of this column.
    pub fn data_type(&self) -> DataType {
        match self {
            ColumnArray::Bool { .. } => DataType::Bool,
            ColumnArray::Int8 { .. } => DataType::Int8,
            ColumnArray::Int16 { .. } => DataType::Int16,
            ColumnArray::Int32 { .. } => DataType::Int32,
            ColumnArray::Int64 { .. } => DataType::Int64,
            ColumnArray::UInt8 { .. } => DataType::UInt8,
            ColumnArray::UInt16 { .. } => DataType::UInt16,
            ColumnArray::UInt32 { .. } => DataType::UInt32,
            ColumnArray::UInt64 { .. } => DataType::UInt64,
            ColumnArray::Float32 { .. } => DataType::Float32,
            ColumnArray::Float64 { .. } => DataType::Float64,
            ColumnArray::Decimal { .. } => DataType::Decimal,
            ColumnArray::Date { .. } => DataType::Date,
            ColumnArray::Time { .. } => DataType::Time,
            ColumnArray::Timestamp { .. } => DataType::Timestamp,
            ColumnArray::Interval { .. } => DataType::Interval,
            ColumnArray::VarChar { .. } => DataType::VarChar,
            ColumnArray::Blob { .. } => DataType::Blob,
            ColumnArray::Vector { .. } => DataType::Vector,
        }
    }

    /// Returns the number of rows in this column.
    pub fn len(&self) -> usize {
        match self {
            ColumnArray::Bool { data, .. } => data.len(),
            ColumnArray::Int8 { data, .. } => data.len(),
            ColumnArray::Int16 { data, .. } => data.len(),
            ColumnArray::Int32 { data, .. } => data.len(),
            ColumnArray::Int64 { data, .. } => data.len(),
            ColumnArray::UInt8 { data, .. } => data.len(),
            ColumnArray::UInt16 { data, .. } => data.len(),
            ColumnArray::UInt32 { data, .. } => data.len(),
            ColumnArray::UInt64 { data, .. } => data.len(),
            ColumnArray::Float32 { data, .. } => data.len(),
            ColumnArray::Float64 { data, .. } => data.len(),
            ColumnArray::Decimal { data, .. } => data.len(),
            ColumnArray::Date { data, .. } => data.len(),
            ColumnArray::Time { data, .. } => data.len(),
            ColumnArray::Timestamp { data, .. } => data.len(),
            ColumnArray::Interval { data, .. } => data.len(),
            ColumnArray::VarChar { data, .. } => data.len(),
            ColumnArray::Blob { data, .. } => data.len(),
            ColumnArray::Vector { data, .. } => data.len(),
        }
    }

    /// Returns true if the column has no rows.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns a reference to the null bitmap.
    pub fn bitmap(&self) -> &NullBitmap {
        match self {
            ColumnArray::Bool { bitmap, .. } => bitmap,
            ColumnArray::Int8 { bitmap, .. } => bitmap,
            ColumnArray::Int16 { bitmap, .. } => bitmap,
            ColumnArray::Int32 { bitmap, .. } => bitmap,
            ColumnArray::Int64 { bitmap, .. } => bitmap,
            ColumnArray::UInt8 { bitmap, .. } => bitmap,
            ColumnArray::UInt16 { bitmap, .. } => bitmap,
            ColumnArray::UInt32 { bitmap, .. } => bitmap,
            ColumnArray::UInt64 { bitmap, .. } => bitmap,
            ColumnArray::Float32 { bitmap, .. } => bitmap,
            ColumnArray::Float64 { bitmap, .. } => bitmap,
            ColumnArray::Decimal { bitmap, .. } => bitmap,
            ColumnArray::Date { bitmap, .. } => bitmap,
            ColumnArray::Time { bitmap, .. } => bitmap,
            ColumnArray::Timestamp { bitmap, .. } => bitmap,
            ColumnArray::Interval { bitmap, .. } => bitmap,
            ColumnArray::VarChar { bitmap, .. } => bitmap,
            ColumnArray::Blob { bitmap, .. } => bitmap,
            ColumnArray::Vector { bitmap, .. } => bitmap,
        }
    }

    /// Returns a mutable reference to the null bitmap.
    pub fn bitmap_mut(&mut self) -> &mut NullBitmap {
        match self {
            ColumnArray::Bool { bitmap, .. } => bitmap,
            ColumnArray::Int8 { bitmap, .. } => bitmap,
            ColumnArray::Int16 { bitmap, .. } => bitmap,
            ColumnArray::Int32 { bitmap, .. } => bitmap,
            ColumnArray::Int64 { bitmap, .. } => bitmap,
            ColumnArray::UInt8 { bitmap, .. } => bitmap,
            ColumnArray::UInt16 { bitmap, .. } => bitmap,
            ColumnArray::UInt32 { bitmap, .. } => bitmap,
            ColumnArray::UInt64 { bitmap, .. } => bitmap,
            ColumnArray::Float32 { bitmap, .. } => bitmap,
            ColumnArray::Float64 { bitmap, .. } => bitmap,
            ColumnArray::Decimal { bitmap, .. } => bitmap,
            ColumnArray::Date { bitmap, .. } => bitmap,
            ColumnArray::Time { bitmap, .. } => bitmap,
            ColumnArray::Timestamp { bitmap, .. } => bitmap,
            ColumnArray::Interval { bitmap, .. } => bitmap,
            ColumnArray::VarChar { bitmap, .. } => bitmap,
            ColumnArray::Blob { bitmap, .. } => bitmap,
            ColumnArray::Vector { bitmap, .. } => bitmap,
        }
    }

    /// Get the scalar value at the given index.
    pub fn scalar(&self, idx: usize) -> ScalarValue {
        match self {
            ColumnArray::Bool { data, bitmap } => {
                if bitmap.is_null(idx) {
                    ScalarValue::Bool(None)
                } else {
                    ScalarValue::Bool(Some(data[idx] != 0))
                }
            }
            ColumnArray::Int8 { data, bitmap } => {
                if bitmap.is_null(idx) {
                    ScalarValue::Int8(None)
                } else {
                    ScalarValue::Int8(Some(data[idx]))
                }
            }
            ColumnArray::Int16 { data, bitmap } => {
                if bitmap.is_null(idx) {
                    ScalarValue::Int16(None)
                } else {
                    ScalarValue::Int16(Some(data[idx]))
                }
            }
            ColumnArray::Int32 { data, bitmap } => {
                if bitmap.is_null(idx) {
                    ScalarValue::Int32(None)
                } else {
                    ScalarValue::Int32(Some(data[idx]))
                }
            }
            ColumnArray::Int64 { data, bitmap } => {
                if bitmap.is_null(idx) {
                    ScalarValue::Int64(None)
                } else {
                    ScalarValue::Int64(Some(data[idx]))
                }
            }
            ColumnArray::UInt8 { data, bitmap } => {
                if bitmap.is_null(idx) {
                    ScalarValue::UInt8(None)
                } else {
                    ScalarValue::UInt8(Some(data[idx]))
                }
            }
            ColumnArray::UInt16 { data, bitmap } => {
                if bitmap.is_null(idx) {
                    ScalarValue::UInt16(None)
                } else {
                    ScalarValue::UInt16(Some(data[idx]))
                }
            }
            ColumnArray::UInt32 { data, bitmap } => {
                if bitmap.is_null(idx) {
                    ScalarValue::UInt32(None)
                } else {
                    ScalarValue::UInt32(Some(data[idx]))
                }
            }
            ColumnArray::UInt64 { data, bitmap } => {
                if bitmap.is_null(idx) {
                    ScalarValue::UInt64(None)
                } else {
                    ScalarValue::UInt64(Some(data[idx]))
                }
            }
            ColumnArray::Float32 { data, bitmap } => {
                if bitmap.is_null(idx) {
                    ScalarValue::Float32(None)
                } else {
                    ScalarValue::Float32(Some(data[idx]))
                }
            }
            ColumnArray::Float64 { data, bitmap } => {
                if bitmap.is_null(idx) {
                    ScalarValue::Float64(None)
                } else {
                    ScalarValue::Float64(Some(data[idx]))
                }
            }
            ColumnArray::Decimal { data, bitmap } => {
                if bitmap.is_null(idx) {
                    ScalarValue::Decimal(None)
                } else {
                    ScalarValue::Decimal(Some(data[idx]))
                }
            }
            ColumnArray::Date { data, bitmap } => {
                if bitmap.is_null(idx) {
                    ScalarValue::Date(None)
                } else {
                    ScalarValue::Date(Some(data[idx]))
                }
            }
            ColumnArray::Time { data, bitmap } => {
                if bitmap.is_null(idx) {
                    ScalarValue::Time(None)
                } else {
                    ScalarValue::Time(Some(data[idx]))
                }
            }
            ColumnArray::Timestamp { data, bitmap } => {
                if bitmap.is_null(idx) {
                    ScalarValue::Timestamp(None)
                } else {
                    ScalarValue::Timestamp(Some(data[idx]))
                }
            }
            ColumnArray::Interval { data, bitmap } => {
                if bitmap.is_null(idx) {
                    ScalarValue::Interval(None)
                } else {
                    ScalarValue::Interval(Some(data[idx]))
                }
            }
            ColumnArray::VarChar { data, bitmap } => {
                if bitmap.is_null(idx) {
                    ScalarValue::VarChar(None)
                } else {
                    ScalarValue::VarChar(Some(data[idx].clone()))
                }
            }
            ColumnArray::Blob { data, bitmap } => {
                if bitmap.is_null(idx) {
                    ScalarValue::Blob(None)
                } else {
                    ScalarValue::Blob(Some(data[idx].clone()))
                }
            }
            ColumnArray::Vector { data, bitmap } => {
                if bitmap.is_null(idx) {
                    ScalarValue::Vector(None)
                } else {
                    ScalarValue::Vector(Some(data[idx].clone()))
                }
            }
        }
    }

    /// Push a scalar value into this column array.
    pub fn push(&mut self, value: &ScalarValue) -> Result<()> {
        let is_null = value.is_null();
        match (self, value) {
            (ColumnArray::Bool { data, bitmap }, ScalarValue::Bool(v)) => {
                data.push(if v.unwrap_or(false) { 1 } else { 0 });
                if is_null {
                    bitmap.push_null();
                } else {
                    bitmap.push_valid();
                }
            }
            (ColumnArray::Int8 { data, bitmap }, ScalarValue::Int8(v)) => {
                data.push(v.unwrap_or(0));
                if is_null {
                    bitmap.push_null();
                } else {
                    bitmap.push_valid();
                }
            }
            (ColumnArray::Int16 { data, bitmap }, ScalarValue::Int16(v)) => {
                data.push(v.unwrap_or(0));
                if is_null {
                    bitmap.push_null();
                } else {
                    bitmap.push_valid();
                }
            }
            (ColumnArray::Int32 { data, bitmap }, ScalarValue::Int32(v)) => {
                data.push(v.unwrap_or(0));
                if is_null {
                    bitmap.push_null();
                } else {
                    bitmap.push_valid();
                }
            }
            (ColumnArray::Int64 { data, bitmap }, ScalarValue::Int64(v)) => {
                data.push(v.unwrap_or(0));
                if is_null {
                    bitmap.push_null();
                } else {
                    bitmap.push_valid();
                }
            }
            (ColumnArray::UInt8 { data, bitmap }, ScalarValue::UInt8(v)) => {
                data.push(v.unwrap_or(0));
                if is_null {
                    bitmap.push_null();
                } else {
                    bitmap.push_valid();
                }
            }
            (ColumnArray::UInt16 { data, bitmap }, ScalarValue::UInt16(v)) => {
                data.push(v.unwrap_or(0));
                if is_null {
                    bitmap.push_null();
                } else {
                    bitmap.push_valid();
                }
            }
            (ColumnArray::UInt32 { data, bitmap }, ScalarValue::UInt32(v)) => {
                data.push(v.unwrap_or(0));
                if is_null {
                    bitmap.push_null();
                } else {
                    bitmap.push_valid();
                }
            }
            (ColumnArray::UInt64 { data, bitmap }, ScalarValue::UInt64(v)) => {
                data.push(v.unwrap_or(0));
                if is_null {
                    bitmap.push_null();
                } else {
                    bitmap.push_valid();
                }
            }
            (ColumnArray::Float32 { data, bitmap }, ScalarValue::Float32(v)) => {
                data.push(v.unwrap_or(0.0));
                if is_null {
                    bitmap.push_null();
                } else {
                    bitmap.push_valid();
                }
            }
            (ColumnArray::Float64 { data, bitmap }, ScalarValue::Float64(v)) => {
                data.push(v.unwrap_or(0.0));
                if is_null {
                    bitmap.push_null();
                } else {
                    bitmap.push_valid();
                }
            }
            (ColumnArray::Decimal { data, bitmap }, ScalarValue::Decimal(v)) => {
                data.push(v.unwrap_or(0));
                if is_null {
                    bitmap.push_null();
                } else {
                    bitmap.push_valid();
                }
            }
            (ColumnArray::Date { data, bitmap }, ScalarValue::Date(v)) => {
                data.push(v.unwrap_or(0));
                if is_null {
                    bitmap.push_null();
                } else {
                    bitmap.push_valid();
                }
            }
            (ColumnArray::Time { data, bitmap }, ScalarValue::Time(v)) => {
                data.push(v.unwrap_or(0));
                if is_null {
                    bitmap.push_null();
                } else {
                    bitmap.push_valid();
                }
            }
            (ColumnArray::Timestamp { data, bitmap }, ScalarValue::Timestamp(v)) => {
                data.push(v.unwrap_or(0));
                if is_null {
                    bitmap.push_null();
                } else {
                    bitmap.push_valid();
                }
            }
            (ColumnArray::Interval { data, bitmap }, ScalarValue::Interval(v)) => {
                data.push(v.unwrap_or(0));
                if is_null {
                    bitmap.push_null();
                } else {
                    bitmap.push_valid();
                }
            }
            (ColumnArray::VarChar { data, bitmap }, ScalarValue::VarChar(v)) => {
                data.push(v.clone().unwrap_or_default());
                if is_null {
                    bitmap.push_null();
                } else {
                    bitmap.push_valid();
                }
            }
            (ColumnArray::Blob { data, bitmap }, ScalarValue::Blob(v)) => {
                data.push(v.clone().unwrap_or_default());
                if is_null {
                    bitmap.push_null();
                } else {
                    bitmap.push_valid();
                }
            }
            (ColumnArray::Vector { data, bitmap }, ScalarValue::Vector(v)) => {
                data.push(v.clone().unwrap_or_default());
                if is_null {
                    bitmap.push_null();
                } else {
                    bitmap.push_valid();
                }
            }
            (arr, val) => {
                return Err(TaibaiError::Internal(format!(
                    "type mismatch: column is {} but value is {}",
                    arr.data_type().display_name(),
                    val.data_type().display_name(),
                )));
            }
        }
        Ok(())
    }

    /// Build a ColumnArray from a slice of ScalarValues.
    /// All values must match the requested DataType.
    pub fn from_scalar_values(dt: DataType, values: &[ScalarValue]) -> Result<Self> {
        let mut arr = Self::empty(dt);
        for v in values {
            arr.push(v)?;
        }
        Ok(arr)
    }

    /// Append another ColumnArray of the same type.
    pub fn extend(&mut self, other: &ColumnArray) -> Result<()> {
        if self.data_type() != other.data_type() {
            return Err(TaibaiError::Internal(format!(
                "cannot extend {} with {}",
                self.data_type().display_name(),
                other.data_type().display_name(),
            )));
        }

        macro_rules! extend_copy_column {
            ($data:expr, $bitmap:expr, $other_data:expr, $other_bitmap:expr) => {{
                $data.extend_from_slice($other_data);
                $bitmap.extend($other_bitmap);
                return Ok(());
            }};
        }

        match (self, other) {
            (
                ColumnArray::Bool { data, bitmap },
                ColumnArray::Bool {
                    data: other_data,
                    bitmap: other_bitmap,
                },
            ) => extend_copy_column!(data, bitmap, other_data, other_bitmap),
            (
                ColumnArray::Int8 { data, bitmap },
                ColumnArray::Int8 {
                    data: other_data,
                    bitmap: other_bitmap,
                },
            ) => extend_copy_column!(data, bitmap, other_data, other_bitmap),
            (
                ColumnArray::Int16 { data, bitmap },
                ColumnArray::Int16 {
                    data: other_data,
                    bitmap: other_bitmap,
                },
            ) => extend_copy_column!(data, bitmap, other_data, other_bitmap),
            (
                ColumnArray::Int32 { data, bitmap },
                ColumnArray::Int32 {
                    data: other_data,
                    bitmap: other_bitmap,
                },
            ) => extend_copy_column!(data, bitmap, other_data, other_bitmap),
            (
                ColumnArray::Int64 { data, bitmap },
                ColumnArray::Int64 {
                    data: other_data,
                    bitmap: other_bitmap,
                },
            ) => extend_copy_column!(data, bitmap, other_data, other_bitmap),
            (
                ColumnArray::UInt8 { data, bitmap },
                ColumnArray::UInt8 {
                    data: other_data,
                    bitmap: other_bitmap,
                },
            ) => extend_copy_column!(data, bitmap, other_data, other_bitmap),
            (
                ColumnArray::UInt16 { data, bitmap },
                ColumnArray::UInt16 {
                    data: other_data,
                    bitmap: other_bitmap,
                },
            ) => extend_copy_column!(data, bitmap, other_data, other_bitmap),
            (
                ColumnArray::UInt32 { data, bitmap },
                ColumnArray::UInt32 {
                    data: other_data,
                    bitmap: other_bitmap,
                },
            ) => extend_copy_column!(data, bitmap, other_data, other_bitmap),
            (
                ColumnArray::UInt64 { data, bitmap },
                ColumnArray::UInt64 {
                    data: other_data,
                    bitmap: other_bitmap,
                },
            ) => extend_copy_column!(data, bitmap, other_data, other_bitmap),
            (
                ColumnArray::Float32 { data, bitmap },
                ColumnArray::Float32 {
                    data: other_data,
                    bitmap: other_bitmap,
                },
            ) => extend_copy_column!(data, bitmap, other_data, other_bitmap),
            (
                ColumnArray::Float64 { data, bitmap },
                ColumnArray::Float64 {
                    data: other_data,
                    bitmap: other_bitmap,
                },
            ) => extend_copy_column!(data, bitmap, other_data, other_bitmap),
            (
                ColumnArray::Decimal { data, bitmap },
                ColumnArray::Decimal {
                    data: other_data,
                    bitmap: other_bitmap,
                },
            ) => extend_copy_column!(data, bitmap, other_data, other_bitmap),
            (
                ColumnArray::Date { data, bitmap },
                ColumnArray::Date {
                    data: other_data,
                    bitmap: other_bitmap,
                },
            ) => extend_copy_column!(data, bitmap, other_data, other_bitmap),
            (
                ColumnArray::Time { data, bitmap },
                ColumnArray::Time {
                    data: other_data,
                    bitmap: other_bitmap,
                },
            ) => extend_copy_column!(data, bitmap, other_data, other_bitmap),
            (
                ColumnArray::Timestamp { data, bitmap },
                ColumnArray::Timestamp {
                    data: other_data,
                    bitmap: other_bitmap,
                },
            ) => extend_copy_column!(data, bitmap, other_data, other_bitmap),
            (
                ColumnArray::Interval { data, bitmap },
                ColumnArray::Interval {
                    data: other_data,
                    bitmap: other_bitmap,
                },
            ) => extend_copy_column!(data, bitmap, other_data, other_bitmap),
            (
                ColumnArray::VarChar { data, bitmap },
                ColumnArray::VarChar {
                    data: other_data,
                    bitmap: other_bitmap,
                },
            ) => {
                data.extend(other_data.iter().cloned());
                bitmap.extend(other_bitmap);
                Ok(())
            }
            (
                ColumnArray::Blob { data, bitmap },
                ColumnArray::Blob {
                    data: other_data,
                    bitmap: other_bitmap,
                },
            ) => {
                data.extend(other_data.iter().cloned());
                bitmap.extend(other_bitmap);
                Ok(())
            }
            (
                ColumnArray::Vector { data, bitmap },
                ColumnArray::Vector {
                    data: other_data,
                    bitmap: other_bitmap,
                },
            ) => {
                data.extend(other_data.iter().cloned());
                bitmap.extend(other_bitmap);
                Ok(())
            }
            _ => unreachable!("column type checked before extend"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_column() {
        let col = ColumnArray::empty(DataType::Int64);
        assert_eq!(col.len(), 0);
        assert!(col.is_empty());
        assert_eq!(col.data_type(), DataType::Int64);
    }

    #[test]
    fn test_push_and_scalar() {
        let mut col = ColumnArray::empty(DataType::Int32);
        col.push(&ScalarValue::Int32(Some(42))).unwrap();
        col.push(&ScalarValue::Int32(None)).unwrap();
        col.push(&ScalarValue::Int32(Some(7))).unwrap();

        assert_eq!(col.len(), 3);
        assert_eq!(col.scalar(0), ScalarValue::Int32(Some(42)));
        assert_eq!(col.scalar(1), ScalarValue::Int32(None));
        assert_eq!(col.scalar(2), ScalarValue::Int32(Some(7)));
    }

    #[test]
    fn test_varchar_column() {
        let mut col = ColumnArray::empty(DataType::VarChar);
        col.push(&ScalarValue::VarChar(Some("hello".into())))
            .unwrap();
        col.push(&ScalarValue::VarChar(None)).unwrap();

        assert_eq!(col.scalar(0), ScalarValue::VarChar(Some("hello".into())));
        assert!(col.scalar(1).is_null());
    }

    #[test]
    fn test_type_mismatch() {
        let mut col = ColumnArray::empty(DataType::Int64);
        let result = col.push(&ScalarValue::VarChar(Some("oops".into())));
        assert!(result.is_err());
    }

    #[test]
    fn test_extend() {
        let mut col1 = ColumnArray::empty(DataType::Int64);
        col1.push(&ScalarValue::Int64(Some(1))).unwrap();
        col1.push(&ScalarValue::Int64(Some(2))).unwrap();

        let mut col2 = ColumnArray::empty(DataType::Int64);
        col2.push(&ScalarValue::Int64(Some(3))).unwrap();

        col1.extend(&col2).unwrap();
        assert_eq!(col1.len(), 3);
        assert_eq!(col1.scalar(2), ScalarValue::Int64(Some(3)));
    }
}

impl ColumnArray {
    pub fn count_eq(&self, value: &ScalarValue) -> u64 {
        let num_rows = self.len();
        let bitmap = self.bitmap();
        match (self, value) {
            (ColumnArray::Int64 { data, .. }, ScalarValue::Int64(Some(v))) => (0..num_rows)
                .filter(|&i| !bitmap.is_null(i) && data[i] == *v)
                .count()
                as u64,
            (ColumnArray::Int32 { data, .. }, ScalarValue::Int32(Some(v))) => (0..num_rows)
                .filter(|&i| !bitmap.is_null(i) && data[i] == *v)
                .count()
                as u64,
            (ColumnArray::Int16 { data, .. }, ScalarValue::Int16(Some(v))) => (0..num_rows)
                .filter(|&i| !bitmap.is_null(i) && data[i] == *v)
                .count()
                as u64,
            (ColumnArray::Int8 { data, .. }, ScalarValue::Int8(Some(v))) => (0..num_rows)
                .filter(|&i| !bitmap.is_null(i) && data[i] == *v)
                .count()
                as u64,
            (ColumnArray::UInt64 { data, .. }, ScalarValue::UInt64(Some(v))) => (0..num_rows)
                .filter(|&i| !bitmap.is_null(i) && data[i] == *v)
                .count()
                as u64,
            (ColumnArray::UInt32 { data, .. }, ScalarValue::UInt32(Some(v))) => (0..num_rows)
                .filter(|&i| !bitmap.is_null(i) && data[i] == *v)
                .count()
                as u64,
            (ColumnArray::UInt16 { data, .. }, ScalarValue::UInt16(Some(v))) => (0..num_rows)
                .filter(|&i| !bitmap.is_null(i) && data[i] == *v)
                .count()
                as u64,
            (ColumnArray::UInt8 { data, .. }, ScalarValue::UInt8(Some(v))) => (0..num_rows)
                .filter(|&i| !bitmap.is_null(i) && data[i] == *v)
                .count()
                as u64,
            (ColumnArray::VarChar { data, .. }, ScalarValue::VarChar(Some(v))) => (0..num_rows)
                .filter(|&i| !bitmap.is_null(i) && data[i] == *v)
                .count()
                as u64,
            (ColumnArray::Float64 { data, .. }, ScalarValue::Float64(Some(v))) => (0..num_rows)
                .filter(|&i| !bitmap.is_null(i) && data[i] == *v)
                .count()
                as u64,
            (ColumnArray::Float32 { data, .. }, ScalarValue::Float32(Some(v))) => (0..num_rows)
                .filter(|&i| !bitmap.is_null(i) && data[i] == *v)
                .count()
                as u64,
            (ColumnArray::Decimal { data, .. }, ScalarValue::Decimal(Some(v))) => (0..num_rows)
                .filter(|&i| !bitmap.is_null(i) && data[i] == *v)
                .count()
                as u64,
            (ColumnArray::Date { data, .. }, ScalarValue::Date(Some(v))) => (0..num_rows)
                .filter(|&i| !bitmap.is_null(i) && data[i] == *v)
                .count()
                as u64,
            (ColumnArray::Timestamp { data, .. }, ScalarValue::Timestamp(Some(v))) => (0..num_rows)
                .filter(|&i| !bitmap.is_null(i) && data[i] == *v)
                .count()
                as u64,
            _ => 0,
        }
    }

    pub fn count_eq_deleted(
        &self,
        value: &ScalarValue,
        rg_start_row: u64,
        delete_bitmap: &[u8],
    ) -> u64 {
        let num_rows = self.len();
        let bitmap = self.bitmap();
        let mut deleted_count: u64 = 0;
        match (self, value) {
            (ColumnArray::Int64 { data, .. }, ScalarValue::Int64(Some(v))) => {
                for (i, item) in data.iter().enumerate().take(num_rows) {
                    if !bitmap.is_null(i) && *item == *v {
                        let row_id = rg_start_row + i as u64;
                        let byte = (row_id / 8) as usize;
                        let bit = (row_id % 8) as u8;
                        if delete_bitmap
                            .get(byte)
                            .is_some_and(|b| (b & (1u8 << bit)) != 0)
                        {
                            deleted_count += 1;
                        }
                    }
                }
            }
            (ColumnArray::Int32 { data, .. }, ScalarValue::Int32(Some(v))) => {
                for (i, item) in data.iter().enumerate().take(num_rows) {
                    if !bitmap.is_null(i) && *item == *v {
                        let row_id = rg_start_row + i as u64;
                        let byte = (row_id / 8) as usize;
                        let bit = (row_id % 8) as u8;
                        if delete_bitmap
                            .get(byte)
                            .is_some_and(|b| (b & (1u8 << bit)) != 0)
                        {
                            deleted_count += 1;
                        }
                    }
                }
            }
            (ColumnArray::VarChar { data, .. }, ScalarValue::VarChar(Some(v))) => {
                for (i, item) in data.iter().enumerate().take(num_rows) {
                    if !bitmap.is_null(i) && *item == *v {
                        let row_id = rg_start_row + i as u64;
                        let byte = (row_id / 8) as usize;
                        let bit = (row_id % 8) as u8;
                        if delete_bitmap
                            .get(byte)
                            .is_some_and(|b| (b & (1u8 << bit)) != 0)
                        {
                            deleted_count += 1;
                        }
                    }
                }
            }
            _ => {}
        }
        deleted_count
    }

    pub fn eq_vec(&self, value: &ScalarValue) -> Vec<bool> {
        let num_rows = self.len();
        let bitmap = self.bitmap();
        match (self, value) {
            (ColumnArray::Bool { data, .. }, ScalarValue::Bool(Some(v))) => (0..num_rows)
                .map(|i| !bitmap.is_null(i) && (data[i] != 0) == *v)
                .collect(),
            (ColumnArray::Int64 { data, .. }, ScalarValue::Int64(Some(v))) => (0..num_rows)
                .map(|i| !bitmap.is_null(i) && data[i] == *v)
                .collect(),
            (ColumnArray::Int32 { data, .. }, ScalarValue::Int32(Some(v))) => (0..num_rows)
                .map(|i| !bitmap.is_null(i) && data[i] == *v)
                .collect(),
            (ColumnArray::VarChar { data, .. }, ScalarValue::VarChar(Some(v))) => (0..num_rows)
                .map(|i| !bitmap.is_null(i) && &data[i] == v)
                .collect(),
            _ if value.numeric_f64().is_some() => (0..num_rows)
                .map(|i| {
                    !bitmap.is_null(i)
                        && (self.scalar(i).numeric_cmp(value) == Some(std::cmp::Ordering::Equal))
                })
                .collect(),
            _ => vec![false; num_rows],
        }
    }

    pub fn lt_vec(&self, value: &ScalarValue) -> Vec<bool> {
        let num_rows = self.len();
        let bitmap = self.bitmap();
        match (self, value) {
            (ColumnArray::Int64 { data, .. }, ScalarValue::Int64(Some(v))) => (0..num_rows)
                .map(|i| !bitmap.is_null(i) && data[i] < *v)
                .collect(),
            (ColumnArray::Int32 { data, .. }, ScalarValue::Int32(Some(v))) => (0..num_rows)
                .map(|i| !bitmap.is_null(i) && data[i] < *v)
                .collect(),
            (ColumnArray::VarChar { data, .. }, ScalarValue::VarChar(Some(v))) => (0..num_rows)
                .map(|i| !bitmap.is_null(i) && &data[i] < v)
                .collect(),
            _ if value.numeric_f64().is_some() => (0..num_rows)
                .map(|i| {
                    !bitmap.is_null(i)
                        && (self.scalar(i).numeric_cmp(value) == Some(std::cmp::Ordering::Less))
                })
                .collect(),
            _ => vec![false; num_rows],
        }
    }

    pub fn le_vec(&self, value: &ScalarValue) -> Vec<bool> {
        let num_rows = self.len();
        let bitmap = self.bitmap();
        match (self, value) {
            (ColumnArray::Int64 { data, .. }, ScalarValue::Int64(Some(v))) => (0..num_rows)
                .map(|i| !bitmap.is_null(i) && data[i] <= *v)
                .collect(),
            (ColumnArray::Int32 { data, .. }, ScalarValue::Int32(Some(v))) => (0..num_rows)
                .map(|i| !bitmap.is_null(i) && data[i] <= *v)
                .collect(),
            (ColumnArray::VarChar { data, .. }, ScalarValue::VarChar(Some(v))) => (0..num_rows)
                .map(|i| !bitmap.is_null(i) && &data[i] <= v)
                .collect(),
            _ if value.numeric_f64().is_some() => (0..num_rows)
                .map(|i| {
                    !bitmap.is_null(i)
                        && self
                            .scalar(i)
                            .numeric_cmp(value)
                            .is_some_and(|ord| ord != std::cmp::Ordering::Greater)
                })
                .collect(),
            _ => vec![false; num_rows],
        }
    }

    pub fn gt_vec(&self, value: &ScalarValue) -> Vec<bool> {
        let num_rows = self.len();
        let bitmap = self.bitmap();
        match (self, value) {
            (ColumnArray::Int64 { data, .. }, ScalarValue::Int64(Some(v))) => (0..num_rows)
                .map(|i| !bitmap.is_null(i) && data[i] > *v)
                .collect(),
            (ColumnArray::Int32 { data, .. }, ScalarValue::Int32(Some(v))) => (0..num_rows)
                .map(|i| !bitmap.is_null(i) && data[i] > *v)
                .collect(),
            (ColumnArray::VarChar { data, .. }, ScalarValue::VarChar(Some(v))) => (0..num_rows)
                .map(|i| !bitmap.is_null(i) && &data[i] > v)
                .collect(),
            _ if value.numeric_f64().is_some() => (0..num_rows)
                .map(|i| {
                    !bitmap.is_null(i)
                        && (self.scalar(i).numeric_cmp(value) == Some(std::cmp::Ordering::Greater))
                })
                .collect(),
            _ => vec![false; num_rows],
        }
    }

    pub fn ge_vec(&self, value: &ScalarValue) -> Vec<bool> {
        let num_rows = self.len();
        let bitmap = self.bitmap();
        match (self, value) {
            (ColumnArray::Int64 { data, .. }, ScalarValue::Int64(Some(v))) => (0..num_rows)
                .map(|i| !bitmap.is_null(i) && data[i] >= *v)
                .collect(),
            (ColumnArray::Int32 { data, .. }, ScalarValue::Int32(Some(v))) => (0..num_rows)
                .map(|i| !bitmap.is_null(i) && data[i] >= *v)
                .collect(),
            (ColumnArray::VarChar { data, .. }, ScalarValue::VarChar(Some(v))) => (0..num_rows)
                .map(|i| !bitmap.is_null(i) && &data[i] >= v)
                .collect(),
            _ if value.numeric_f64().is_some() => (0..num_rows)
                .map(|i| {
                    !bitmap.is_null(i)
                        && self
                            .scalar(i)
                            .numeric_cmp(value)
                            .is_some_and(|ord| ord != std::cmp::Ordering::Less)
                })
                .collect(),
            _ => vec![false; num_rows],
        }
    }

    pub fn is_null_vec(&self) -> Vec<bool> {
        let num_rows = self.len();
        let bitmap = self.bitmap();
        (0..num_rows).map(|i| bitmap.is_null(i)).collect()
    }

    pub fn is_not_null_vec(&self) -> Vec<bool> {
        let num_rows = self.len();
        let bitmap = self.bitmap();
        (0..num_rows).map(|i| !bitmap.is_null(i)).collect()
    }

    pub fn filter_by_mask(&self, mask: &[bool]) -> ColumnArray {
        let mut new_col = ColumnArray::empty(self.data_type());
        for (i, &keep) in mask.iter().enumerate() {
            if keep {
                new_col.push(&self.scalar(i)).unwrap();
            }
        }
        new_col
    }

    pub fn filter_by_mask_fast(&self, mask: &[bool], match_count: usize) -> ColumnArray {
        macro_rules! filter_copy_column {
            ($variant:ident, $data:expr, $bitmap:expr) => {{
                let mut new_data = Vec::with_capacity(match_count);
                let mut new_bitmap = crate::types::bitmap::NullBitmap::new_valid(0);
                for (i, &keep) in mask.iter().enumerate() {
                    if keep {
                        new_data.push($data[i]);
                        if $bitmap.is_null(i) {
                            new_bitmap.push_null();
                        } else {
                            new_bitmap.push_valid();
                        }
                    }
                }
                ColumnArray::$variant {
                    data: new_data,
                    bitmap: new_bitmap,
                }
            }};
        }

        match self {
            ColumnArray::Bool { data, bitmap } => filter_copy_column!(Bool, data, bitmap),
            ColumnArray::Int8 { data, bitmap } => filter_copy_column!(Int8, data, bitmap),
            ColumnArray::Int16 { data, bitmap } => filter_copy_column!(Int16, data, bitmap),
            ColumnArray::Int32 { data, bitmap } => filter_copy_column!(Int32, data, bitmap),
            ColumnArray::Int64 { data, bitmap } => filter_copy_column!(Int64, data, bitmap),
            ColumnArray::UInt8 { data, bitmap } => filter_copy_column!(UInt8, data, bitmap),
            ColumnArray::UInt16 { data, bitmap } => filter_copy_column!(UInt16, data, bitmap),
            ColumnArray::UInt32 { data, bitmap } => filter_copy_column!(UInt32, data, bitmap),
            ColumnArray::UInt64 { data, bitmap } => filter_copy_column!(UInt64, data, bitmap),
            ColumnArray::Float32 { data, bitmap } => filter_copy_column!(Float32, data, bitmap),
            ColumnArray::Float64 { data, bitmap } => filter_copy_column!(Float64, data, bitmap),
            ColumnArray::Decimal { data, bitmap } => filter_copy_column!(Decimal, data, bitmap),
            ColumnArray::Date { data, bitmap } => filter_copy_column!(Date, data, bitmap),
            ColumnArray::Time { data, bitmap } => filter_copy_column!(Time, data, bitmap),
            ColumnArray::Timestamp { data, bitmap } => filter_copy_column!(Timestamp, data, bitmap),
            ColumnArray::Interval { data, bitmap } => filter_copy_column!(Interval, data, bitmap),
            ColumnArray::VarChar { data, bitmap } => {
                let mut new_data = Vec::with_capacity(match_count);
                let mut new_bitmap = crate::types::bitmap::NullBitmap::new_valid(0);
                for (i, &keep) in mask.iter().enumerate() {
                    if keep {
                        new_data.push(data[i].clone());
                        if bitmap.is_null(i) {
                            new_bitmap.push_null();
                        } else {
                            new_bitmap.push_valid();
                        }
                    }
                }
                ColumnArray::VarChar {
                    data: new_data,
                    bitmap: new_bitmap,
                }
            }
            _ => self.filter_by_mask(mask),
        }
    }

    pub fn slice_fast(&self, start: usize, end: usize) -> ColumnArray {
        macro_rules! slice_copy_column {
            ($variant:ident, $data:expr, $bitmap:expr) => {{
                let new_data = $data[start..end].to_vec();
                let mut new_bitmap = crate::types::bitmap::NullBitmap::new_valid(0);
                for i in start..end {
                    if $bitmap.is_null(i) {
                        new_bitmap.push_null();
                    } else {
                        new_bitmap.push_valid();
                    }
                }
                ColumnArray::$variant {
                    data: new_data,
                    bitmap: new_bitmap,
                }
            }};
        }

        match self {
            ColumnArray::Bool { data, bitmap } => slice_copy_column!(Bool, data, bitmap),
            ColumnArray::Int8 { data, bitmap } => slice_copy_column!(Int8, data, bitmap),
            ColumnArray::Int16 { data, bitmap } => slice_copy_column!(Int16, data, bitmap),
            ColumnArray::Int32 { data, bitmap } => slice_copy_column!(Int32, data, bitmap),
            ColumnArray::Int64 { data, bitmap } => slice_copy_column!(Int64, data, bitmap),
            ColumnArray::UInt8 { data, bitmap } => slice_copy_column!(UInt8, data, bitmap),
            ColumnArray::UInt16 { data, bitmap } => slice_copy_column!(UInt16, data, bitmap),
            ColumnArray::UInt32 { data, bitmap } => slice_copy_column!(UInt32, data, bitmap),
            ColumnArray::UInt64 { data, bitmap } => slice_copy_column!(UInt64, data, bitmap),
            ColumnArray::Float32 { data, bitmap } => slice_copy_column!(Float32, data, bitmap),
            ColumnArray::Float64 { data, bitmap } => slice_copy_column!(Float64, data, bitmap),
            ColumnArray::Decimal { data, bitmap } => slice_copy_column!(Decimal, data, bitmap),
            ColumnArray::Date { data, bitmap } => slice_copy_column!(Date, data, bitmap),
            ColumnArray::Time { data, bitmap } => slice_copy_column!(Time, data, bitmap),
            ColumnArray::Timestamp { data, bitmap } => slice_copy_column!(Timestamp, data, bitmap),
            ColumnArray::Interval { data, bitmap } => slice_copy_column!(Interval, data, bitmap),
            ColumnArray::VarChar { data, bitmap } => slice_copy_column!(VarChar, data, bitmap),
            _ => {
                let mut new_col = ColumnArray::empty(self.data_type());
                for i in start..end {
                    new_col.push(&self.scalar(i)).unwrap();
                }
                new_col
            }
        }
    }
}
