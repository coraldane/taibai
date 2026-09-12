use crate::common::error::{Result, TaibaiError};
use crate::protocol::types::DataType;
use crate::types::bitmap::NullBitmap;
use crate::types::scalar::ScalarValue;

/// Arrow-compatible variable-width UTF-8 storage. `offsets` always contains
/// one more entry than there are values and `data` owns all string bytes in a
/// single allocation.
#[derive(Debug, Clone, Default)]
pub struct Utf8Buffer {
    offsets: Vec<u32>,
    data: Vec<u8>,
}

impl Utf8Buffer {
    pub fn new() -> Self {
        Self {
            offsets: vec![0],
            data: Vec::new(),
        }
    }

    pub fn with_capacity(values: usize) -> Self {
        let mut offsets = Vec::with_capacity(values.saturating_add(1));
        offsets.push(0);
        Self {
            offsets,
            data: Vec::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.offsets.len().saturating_sub(1)
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn capacity(&self) -> usize {
        self.offsets.capacity().saturating_sub(1)
    }
    pub fn data_capacity(&self) -> usize {
        self.data.capacity()
    }
    pub fn offsets(&self) -> &[u32] {
        &self.offsets
    }
    pub fn values(&self) -> &[u8] {
        &self.data
    }

    pub fn value(&self, index: usize) -> &str {
        let start = self.offsets[index] as usize;
        let end = self.offsets[index + 1] as usize;
        // Values enter through String/&str or the validated segment decoder.
        unsafe { std::str::from_utf8_unchecked(&self.data[start..end]) }
    }

    pub fn get(&self, index: usize) -> Option<&str> {
        (index < self.len()).then(|| self.value(index))
    }

    pub fn push(&mut self, value: String) {
        self.push_str(&value);
    }

    pub fn push_str(&mut self, value: &str) {
        self.data.extend_from_slice(value.as_bytes());
        self.offsets.push(self.data.len() as u32);
    }

    pub fn append(&mut self, other: &mut Self) {
        let base = self.data.len() as u32;
        self.data.append(&mut other.data);
        self.offsets
            .extend(other.offsets.iter().skip(1).map(|offset| base + *offset));
        other.offsets.clear();
        other.offsets.push(0);
    }

    pub fn extend_from(&mut self, other: &Self) {
        for value in other.iter() {
            self.push_str(value);
        }
    }

    /// Used only by compaction's position gatherer. Query execution builds
    /// buffers append-only and never pays this relocation cost.
    pub fn set(&mut self, index: usize, value: &str) {
        let start = self.offsets[index] as usize;
        let end = self.offsets[index + 1] as usize;
        let old_len = end - start;
        self.data.splice(start..end, value.bytes());
        let delta = value.len() as isize - old_len as isize;
        if delta != 0 {
            for offset in self.offsets.iter_mut().skip(index + 1) {
                *offset = (*offset as isize + delta) as u32;
            }
        }
    }

    pub fn iter(&self) -> Utf8Iter<'_> {
        Utf8Iter {
            buffer: self,
            index: 0,
        }
    }
}

impl From<Vec<String>> for Utf8Buffer {
    fn from(values: Vec<String>) -> Self {
        let mut out = Self::with_capacity(values.len());
        for value in values {
            out.push(value);
        }
        out
    }
}

impl std::ops::Index<usize> for Utf8Buffer {
    type Output = str;
    fn index(&self, index: usize) -> &Self::Output {
        self.value(index)
    }
}

pub struct Utf8Iter<'a> {
    buffer: &'a Utf8Buffer,
    index: usize,
}
impl<'a> Iterator for Utf8Iter<'a> {
    type Item = &'a str;
    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.buffer.len() {
            return None;
        }
        let value = self.buffer.value(self.index);
        self.index += 1;
        Some(value)
    }
}

impl<'a> IntoIterator for &'a Utf8Buffer {
    type Item = &'a str;
    type IntoIter = Utf8Iter<'a>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl PartialEq<Vec<String>> for Utf8Buffer {
    fn eq(&self, other: &Vec<String>) -> bool {
        self.len() == other.len() && self.iter().zip(other).all(|(a, b)| a == b)
    }
}

/// Arrow-compatible variable-width binary storage.
#[derive(Debug, Clone, Default)]
pub struct BinaryBuffer {
    offsets: Vec<u32>,
    data: Vec<u8>,
}

impl BinaryBuffer {
    pub fn new() -> Self {
        Self {
            offsets: vec![0],
            data: Vec::new(),
        }
    }
    pub fn with_capacity(values: usize) -> Self {
        let mut offsets = Vec::with_capacity(values.saturating_add(1));
        offsets.push(0);
        Self {
            offsets,
            data: Vec::new(),
        }
    }
    pub fn len(&self) -> usize {
        self.offsets.len().saturating_sub(1)
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn capacity(&self) -> usize {
        self.offsets.capacity().saturating_sub(1)
    }
    pub fn data_capacity(&self) -> usize {
        self.data.capacity()
    }
    pub fn offsets(&self) -> &[u32] {
        &self.offsets
    }
    pub fn values(&self) -> &[u8] {
        &self.data
    }
    pub fn value(&self, index: usize) -> &[u8] {
        &self.data[self.offsets[index] as usize..self.offsets[index + 1] as usize]
    }
    pub fn get(&self, index: usize) -> Option<&[u8]> {
        (index < self.len()).then(|| self.value(index))
    }
    pub fn push(&mut self, value: Vec<u8>) {
        self.push_slice(&value);
    }
    pub fn push_slice(&mut self, value: &[u8]) {
        self.data.extend_from_slice(value);
        self.offsets.push(self.data.len() as u32);
    }
    pub fn append(&mut self, other: &mut Self) {
        let base = self.data.len() as u32;
        self.data.append(&mut other.data);
        self.offsets
            .extend(other.offsets.iter().skip(1).map(|offset| base + *offset));
        other.offsets.clear();
        other.offsets.push(0);
    }
    pub fn extend_from(&mut self, other: &Self) {
        for value in other.iter() {
            self.push_slice(value);
        }
    }
    pub fn set(&mut self, index: usize, value: &[u8]) {
        let start = self.offsets[index] as usize;
        let end = self.offsets[index + 1] as usize;
        let old_len = end - start;
        self.data.splice(start..end, value.iter().copied());
        let delta = value.len() as isize - old_len as isize;
        if delta != 0 {
            for offset in self.offsets.iter_mut().skip(index + 1) {
                *offset = (*offset as isize + delta) as u32;
            }
        }
    }
    pub fn iter(&self) -> BinaryIter<'_> {
        BinaryIter {
            buffer: self,
            index: 0,
        }
    }
}

impl From<Vec<Vec<u8>>> for BinaryBuffer {
    fn from(values: Vec<Vec<u8>>) -> Self {
        let mut out = Self::with_capacity(values.len());
        for value in values {
            out.push(value);
        }
        out
    }
}

impl std::ops::Index<usize> for BinaryBuffer {
    type Output = [u8];
    fn index(&self, index: usize) -> &Self::Output {
        self.value(index)
    }
}

pub struct BinaryIter<'a> {
    buffer: &'a BinaryBuffer,
    index: usize,
}
impl<'a> Iterator for BinaryIter<'a> {
    type Item = &'a [u8];
    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.buffer.len() {
            return None;
        }
        let value = self.buffer.value(self.index);
        self.index += 1;
        Some(value)
    }
}

impl<'a> IntoIterator for &'a BinaryBuffer {
    type Item = &'a [u8];
    type IntoIter = BinaryIter<'a>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

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
        data: Utf8Buffer,
        bitmap: NullBitmap,
    },
    Blob {
        data: BinaryBuffer,
        bitmap: NullBitmap,
    },
    Vector {
        data: Vec<Vec<f32>>,
        bitmap: NullBitmap,
    },
}

impl ColumnArray {
    /// Compare two positions without constructing `ScalarValue`s. NULL sorts
    /// last, matching the SQL sort operator's existing semantics.
    pub fn compare_at(&self, left: usize, right: usize) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        let left_null = self.bitmap().is_null(left);
        let right_null = self.bitmap().is_null(right);
        match (left_null, right_null) {
            (true, true) => return Ordering::Equal,
            (true, false) => return Ordering::Greater,
            (false, true) => return Ordering::Less,
            _ => {}
        }
        macro_rules! cmp_ord {
            ($data:expr) => {
                $data[left].cmp(&$data[right])
            };
        }
        macro_rules! cmp_float {
            ($data:expr) => {
                $data[left]
                    .partial_cmp(&$data[right])
                    .unwrap_or(Ordering::Equal)
            };
        }
        match self {
            ColumnArray::Bool { data, .. } | ColumnArray::UInt8 { data, .. } => cmp_ord!(data),
            ColumnArray::Int8 { data, .. } => cmp_ord!(data),
            ColumnArray::Int16 { data, .. } => cmp_ord!(data),
            ColumnArray::Int32 { data, .. } | ColumnArray::Date { data, .. } => cmp_ord!(data),
            ColumnArray::Int64 { data, .. }
            | ColumnArray::Decimal { data, .. }
            | ColumnArray::Time { data, .. }
            | ColumnArray::Timestamp { data, .. }
            | ColumnArray::Interval { data, .. } => cmp_ord!(data),
            ColumnArray::UInt16 { data, .. } => cmp_ord!(data),
            ColumnArray::UInt32 { data, .. } => cmp_ord!(data),
            ColumnArray::UInt64 { data, .. } => cmp_ord!(data),
            ColumnArray::Float32 { data, .. } => cmp_float!(data),
            ColumnArray::Float64 { data, .. } => cmp_float!(data),
            ColumnArray::VarChar { data, .. } => data.value(left).cmp(data.value(right)),
            ColumnArray::Blob { data, .. } => data.value(left).cmp(data.value(right)),
            ColumnArray::Vector { data, .. } => data[left]
                .partial_cmp(&data[right])
                .unwrap_or(Ordering::Equal),
        }
    }

    /// Typed comparison against a literal, used by filter/range kernels.
    pub fn compare_scalar_at(&self, row: usize, value: &ScalarValue) -> Option<std::cmp::Ordering> {
        if self.bitmap().is_null(row) || value.is_null() {
            return None;
        }
        macro_rules! cmp {
            ($data:expr, $v:expr) => {
                Some($data[row].cmp($v))
            };
        }
        macro_rules! cmp_float {
            ($data:expr, $v:expr) => {
                $data[row].partial_cmp($v)
            };
        }
        match (self, value) {
            (ColumnArray::Bool { data, .. }, ScalarValue::Bool(Some(v))) => {
                Some((data[row] != 0).cmp(v))
            }
            (ColumnArray::Int8 { data, .. }, ScalarValue::Int8(Some(v))) => cmp!(data, v),
            (ColumnArray::Int16 { data, .. }, ScalarValue::Int16(Some(v))) => cmp!(data, v),
            (ColumnArray::Int32 { data, .. }, ScalarValue::Int32(Some(v))) => cmp!(data, v),
            (ColumnArray::Int64 { data, .. }, ScalarValue::Int64(Some(v))) => cmp!(data, v),
            (ColumnArray::UInt8 { data, .. }, ScalarValue::UInt8(Some(v))) => cmp!(data, v),
            (ColumnArray::UInt16 { data, .. }, ScalarValue::UInt16(Some(v))) => cmp!(data, v),
            (ColumnArray::UInt32 { data, .. }, ScalarValue::UInt32(Some(v))) => cmp!(data, v),
            (ColumnArray::UInt64 { data, .. }, ScalarValue::UInt64(Some(v))) => cmp!(data, v),
            (ColumnArray::Float32 { data, .. }, ScalarValue::Float32(Some(v))) => {
                cmp_float!(data, v)
            }
            (ColumnArray::Float64 { data, .. }, ScalarValue::Float64(Some(v))) => {
                cmp_float!(data, v)
            }
            (ColumnArray::Decimal { data, .. }, ScalarValue::Decimal(Some(v))) => cmp!(data, v),
            (ColumnArray::Date { data, .. }, ScalarValue::Date(Some(v))) => cmp!(data, v),
            (ColumnArray::Time { data, .. }, ScalarValue::Time(Some(v))) => cmp!(data, v),
            (ColumnArray::Timestamp { data, .. }, ScalarValue::Timestamp(Some(v))) => cmp!(data, v),
            (ColumnArray::Interval { data, .. }, ScalarValue::Interval(Some(v))) => cmp!(data, v),
            (ColumnArray::VarChar { data, .. }, ScalarValue::VarChar(Some(v))) => {
                Some(data.value(row).cmp(v))
            }
            (ColumnArray::Blob { data, .. }, ScalarValue::Blob(Some(v))) => {
                Some(data.value(row).cmp(v))
            }
            _ => self.numeric_f64_at(row)?.partial_cmp(&value.numeric_f64()?),
        }
    }

    /// Append a stable, typed row-key representation directly from column
    /// storage. Used by GROUP BY and hash join to avoid per-row boxing.
    pub fn encode_key_at(&self, row: usize, out: &mut Vec<u8>) {
        out.push(self.data_type() as u8);
        if self.bitmap().is_null(row) {
            out.push(0);
            return;
        }
        out.push(1);
        macro_rules! bytes {
            ($data:expr) => {
                out.extend_from_slice(&$data[row].to_le_bytes())
            };
        }
        match self {
            ColumnArray::Bool { data, .. } | ColumnArray::UInt8 { data, .. } => out.push(data[row]),
            ColumnArray::Int8 { data, .. } => out.push(data[row] as u8),
            ColumnArray::Int16 { data, .. } => bytes!(data),
            ColumnArray::Int32 { data, .. } | ColumnArray::Date { data, .. } => bytes!(data),
            ColumnArray::Int64 { data, .. }
            | ColumnArray::Decimal { data, .. }
            | ColumnArray::Time { data, .. }
            | ColumnArray::Timestamp { data, .. }
            | ColumnArray::Interval { data, .. } => bytes!(data),
            ColumnArray::UInt16 { data, .. } => bytes!(data),
            ColumnArray::UInt32 { data, .. } => bytes!(data),
            ColumnArray::UInt64 { data, .. } => bytes!(data),
            ColumnArray::Float32 { data, .. } => {
                out.extend_from_slice(&data[row].to_bits().to_le_bytes())
            }
            ColumnArray::Float64 { data, .. } => {
                out.extend_from_slice(&data[row].to_bits().to_le_bytes())
            }
            ColumnArray::VarChar { data, .. } => {
                let value = data.value(row);
                out.extend_from_slice(&(value.len() as u32).to_le_bytes());
                out.extend_from_slice(value.as_bytes());
            }
            ColumnArray::Blob { data, .. } => {
                let value = data.value(row);
                out.extend_from_slice(&(value.len() as u32).to_le_bytes());
                out.extend_from_slice(value);
            }
            ColumnArray::Vector { data, .. } => {
                out.extend_from_slice(&(data[row].len() as u32).to_le_bytes());
                for value in &data[row] {
                    out.extend_from_slice(&value.to_bits().to_le_bytes());
                }
            }
        }
    }

    pub fn numeric_i64_at(&self, row: usize) -> Option<i64> {
        if self.bitmap().is_null(row) {
            return None;
        }
        match self {
            ColumnArray::Int8 { data, .. } => Some(data[row] as i64),
            ColumnArray::Int16 { data, .. } => Some(data[row] as i64),
            ColumnArray::Int32 { data, .. } | ColumnArray::Date { data, .. } => {
                Some(data[row] as i64)
            }
            ColumnArray::Int64 { data, .. }
            | ColumnArray::Decimal { data, .. }
            | ColumnArray::Time { data, .. }
            | ColumnArray::Timestamp { data, .. }
            | ColumnArray::Interval { data, .. } => Some(data[row]),
            ColumnArray::UInt8 { data, .. } => Some(data[row] as i64),
            ColumnArray::UInt16 { data, .. } => Some(data[row] as i64),
            ColumnArray::UInt32 { data, .. } => Some(data[row] as i64),
            ColumnArray::UInt64 { data, .. } => i64::try_from(data[row]).ok(),
            _ => None,
        }
    }

    pub fn numeric_f64_at(&self, row: usize) -> Option<f64> {
        if self.bitmap().is_null(row) {
            return None;
        }
        match self {
            ColumnArray::Float32 { data, .. } => Some(data[row] as f64),
            ColumnArray::Float64 { data, .. } => Some(data[row]),
            _ => self.numeric_i64_at(row).map(|value| value as f64),
        }
    }

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
                data: Utf8Buffer::new(),
                bitmap: NullBitmap::new_valid(0),
            },
            DataType::Blob => ColumnArray::Blob {
                data: BinaryBuffer::new(),
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
                data: Utf8Buffer::with_capacity(capacity),
                bitmap: bitmap(),
            },
            DataType::Blob => ColumnArray::Blob {
                data: BinaryBuffer::with_capacity(capacity),
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

    /// Estimated resident bytes, including owned variable-width payloads.
    /// This deliberately favours a cheap, deterministic estimate over an
    /// allocator-specific exact value and is used for query spill decisions.
    pub fn estimated_size_bytes(&self) -> usize {
        let bitmap = self.bitmap().estimated_size_bytes();
        match self {
            ColumnArray::Bool { data, .. } | ColumnArray::UInt8 { data, .. } => {
                bitmap + data.capacity()
            }
            ColumnArray::Int8 { data, .. } => bitmap + data.capacity(),
            ColumnArray::Int16 { data, .. } => {
                bitmap + data.capacity() * std::mem::size_of::<i16>()
            }
            ColumnArray::Int32 { data, .. } | ColumnArray::Date { data, .. } => {
                bitmap + data.capacity() * std::mem::size_of::<i32>()
            }
            ColumnArray::Int64 { data, .. }
            | ColumnArray::Decimal { data, .. }
            | ColumnArray::Time { data, .. }
            | ColumnArray::Timestamp { data, .. }
            | ColumnArray::Interval { data, .. } => {
                bitmap + data.capacity() * std::mem::size_of::<i64>()
            }
            ColumnArray::UInt16 { data, .. } => {
                bitmap + data.capacity() * std::mem::size_of::<u16>()
            }
            ColumnArray::UInt32 { data, .. } => {
                bitmap + data.capacity() * std::mem::size_of::<u32>()
            }
            ColumnArray::UInt64 { data, .. } => {
                bitmap + data.capacity() * std::mem::size_of::<u64>()
            }
            ColumnArray::Float32 { data, .. } => {
                bitmap + data.capacity() * std::mem::size_of::<f32>()
            }
            ColumnArray::Float64 { data, .. } => {
                bitmap + data.capacity() * std::mem::size_of::<f64>()
            }
            ColumnArray::VarChar { data, .. } => {
                bitmap + data.offsets.capacity() * std::mem::size_of::<u32>() + data.data_capacity()
            }
            ColumnArray::Blob { data, .. } => {
                bitmap + data.offsets.capacity() * std::mem::size_of::<u32>() + data.data_capacity()
            }
            ColumnArray::Vector { data, .. } => {
                bitmap
                    + data.capacity() * std::mem::size_of::<Vec<f32>>()
                    + data
                        .iter()
                        .map(|value| value.capacity() * std::mem::size_of::<f32>())
                        .sum::<usize>()
            }
        }
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
                    ScalarValue::VarChar(Some(data.value(idx).to_owned()))
                }
            }
            ColumnArray::Blob { data, bitmap } => {
                if bitmap.is_null(idx) {
                    ScalarValue::Blob(None)
                } else {
                    ScalarValue::Blob(Some(data.value(idx).to_vec()))
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

    pub fn push_null_value(&mut self) {
        match self {
            ColumnArray::Bool { data, bitmap } => {
                data.push(0);
                bitmap.push_null();
            }
            ColumnArray::Int8 { data, bitmap } => {
                data.push(0);
                bitmap.push_null();
            }
            ColumnArray::Int16 { data, bitmap } => {
                data.push(0);
                bitmap.push_null();
            }
            ColumnArray::Int32 { data, bitmap } => {
                data.push(0);
                bitmap.push_null();
            }
            ColumnArray::Int64 { data, bitmap } => {
                data.push(0);
                bitmap.push_null();
            }
            ColumnArray::UInt8 { data, bitmap } => {
                data.push(0);
                bitmap.push_null();
            }
            ColumnArray::UInt16 { data, bitmap } => {
                data.push(0);
                bitmap.push_null();
            }
            ColumnArray::UInt32 { data, bitmap } => {
                data.push(0);
                bitmap.push_null();
            }
            ColumnArray::UInt64 { data, bitmap } => {
                data.push(0);
                bitmap.push_null();
            }
            ColumnArray::Float32 { data, bitmap } => {
                data.push(0.0);
                bitmap.push_null();
            }
            ColumnArray::Float64 { data, bitmap } => {
                data.push(0.0);
                bitmap.push_null();
            }
            ColumnArray::Decimal { data, bitmap } => {
                data.push(0);
                bitmap.push_null();
            }
            ColumnArray::Date { data, bitmap } => {
                data.push(0);
                bitmap.push_null();
            }
            ColumnArray::Time { data, bitmap } => {
                data.push(0);
                bitmap.push_null();
            }
            ColumnArray::Timestamp { data, bitmap } => {
                data.push(0);
                bitmap.push_null();
            }
            ColumnArray::Interval { data, bitmap } => {
                data.push(0);
                bitmap.push_null();
            }
            ColumnArray::VarChar { data, bitmap } => {
                data.push(String::new());
                bitmap.push_null();
            }
            ColumnArray::Blob { data, bitmap } => {
                data.push(Vec::new());
                bitmap.push_null();
            }
            ColumnArray::Vector { data, bitmap } => {
                data.push(Vec::new());
                bitmap.push_null();
            }
        }
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
                data.extend_from(other_data);
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
                data.extend_from(other_data);
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

    /// Append an owned column. Variable-width buffers are moved instead of
    /// cloned, which is important when compaction concatenates many decoded
    /// row groups into one global column.
    pub fn append_owned(&mut self, other: ColumnArray) -> Result<()> {
        match other {
            ColumnArray::VarChar {
                mut data,
                bitmap: other_bitmap,
            } => match self {
                ColumnArray::VarChar { data: dst, bitmap } => {
                    dst.append(&mut data);
                    bitmap.extend(&other_bitmap);
                    Ok(())
                }
                _ => Err(TaibaiError::Internal(
                    "cannot append VARCHAR to a different column type".to_string(),
                )),
            },
            ColumnArray::Blob {
                mut data,
                bitmap: other_bitmap,
            } => match self {
                ColumnArray::Blob { data: dst, bitmap } => {
                    dst.append(&mut data);
                    bitmap.extend(&other_bitmap);
                    Ok(())
                }
                _ => Err(TaibaiError::Internal(
                    "cannot append BLOB to a different column type".to_string(),
                )),
            },
            ColumnArray::Vector {
                mut data,
                bitmap: other_bitmap,
            } => match self {
                ColumnArray::Vector { data: dst, bitmap } => {
                    dst.append(&mut data);
                    bitmap.extend(&other_bitmap);
                    Ok(())
                }
                _ => Err(TaibaiError::Internal(
                    "cannot append VECTOR to a different column type".to_string(),
                )),
            },
            other => self.extend(&other),
        }
    }
}

#[cfg(test)]
#[path = "array_test.rs"]
mod tests;

#[cfg(test)]
#[path = "array/vec_kernel_test.rs"]
mod vec_kernel_tests;

include!("array/analytics.rs");
