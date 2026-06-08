use serde::{Deserialize, Serialize};

use crate::common::error::{MonaxError, Result};
use crate::protocol::types::DataType;
use crate::types::array::ColumnArray;
use crate::types::scalar::ScalarValue;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ColumnDefaultValue {
    CurrentTimestamp,
    Literal(ScalarValue),
}

impl Default for ColumnDefaultValue {
    fn default() -> Self {
        ColumnDefaultValue::Literal(ScalarValue::Int64(None))
    }
}

/// Column definition within a table schema.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub scale: i32,
    pub precision: i32,
    pub auto_increment: bool,
    #[serde(default)]
    pub primary_key: bool,
    #[serde(default)]
    pub default_value: Option<ColumnDefaultValue>,
}

impl ColumnDef {
    pub fn new(name: &str, data_type: DataType) -> Self {
        Self {
            name: name.to_string(),
            data_type,
            nullable: true,
            scale: 0,
            precision: 0,
            auto_increment: false,
            primary_key: false,
            default_value: None,
        }
    }

    pub fn with_nullable(mut self, nullable: bool) -> Self {
        self.nullable = nullable;
        self
    }

    pub fn with_scale(mut self, scale: i32) -> Self {
        self.scale = scale;
        self
    }

    pub fn with_precision(mut self, precision: i32) -> Self {
        self.precision = precision;
        self
    }

    pub fn with_auto_increment(mut self, auto_increment: bool) -> Self {
        self.auto_increment = auto_increment;
        self
    }

    pub fn with_primary_key(mut self, primary_key: bool) -> Self {
        self.primary_key = primary_key;
        self
    }

    pub fn with_default_value(mut self, default_value: Option<ColumnDefaultValue>) -> Self {
        self.default_value = default_value;
        self
    }
}

/// Schema for a table: column names + types.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Schema {
    pub columns: Vec<ColumnDef>,
}

impl Schema {
    pub fn new(columns: Vec<ColumnDef>) -> Self {
        Self { columns }
    }

    pub fn empty() -> Self {
        Self {
            columns: Vec::new(),
        }
    }

    /// Returns the number of columns.
    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    /// Find the index of a column by name (case-insensitive).
    pub fn index_of(&self, name: &str) -> Option<usize> {
        self.columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(name))
    }

    /// Get a column definition by index.
    pub fn column(&self, idx: usize) -> Option<&ColumnDef> {
        self.columns.get(idx)
    }

    /// Returns an iterator over column names.
    pub fn column_names(&self) -> impl Iterator<Item = &str> {
        self.columns.iter().map(|c| c.name.as_str())
    }

    /// Returns an iterator over data types.
    pub fn data_types(&self) -> impl Iterator<Item = DataType> + '_ {
        self.columns.iter().map(|c| c.data_type)
    }
}

/// A record batch: a horizontal slice of rows with columns matching a schema.
#[derive(Debug, Clone)]
pub struct RecordBatch {
    pub schema: Schema,
    pub columns: Vec<ColumnArray>,
}

impl RecordBatch {
    /// Create a new empty RecordBatch with the given schema.
    pub fn new(schema: Schema) -> Self {
        let columns = schema
            .columns
            .iter()
            .map(|c| ColumnArray::empty(c.data_type))
            .collect();
        Self { schema, columns }
    }

    pub fn with_capacity(schema: Schema, capacity: usize) -> Self {
        let columns = schema
            .columns
            .iter()
            .map(|c| ColumnArray::with_capacity(c.data_type, capacity))
            .collect();
        Self { schema, columns }
    }

    /// Create a RecordBatch from pre-built column arrays.
    /// Validates that the column count matches the schema.
    #[track_caller]
    pub fn try_new(schema: Schema, columns: Vec<ColumnArray>) -> Result<Self> {
        if columns.len() != schema.num_columns() {
            return Err(MonaxError::Internal(format!(
                "column count mismatch: schema has {} columns but got {} arrays",
                schema.num_columns(),
                columns.len(),
            )));
        }
        // Validate types match
        for (i, (col_def, col_arr)) in schema.columns.iter().zip(columns.iter()).enumerate() {
            if col_def.data_type != col_arr.data_type() {
                return Err(MonaxError::Internal(format!(
                    "type mismatch at column {}: schema says {} but array is {}",
                    i,
                    col_def.data_type.display_name(),
                    col_arr.data_type().display_name(),
                )));
            }
        }
        if let Some(first) = columns.first() {
            let expected_len = first.len();
            for (i, col_arr) in columns.iter().enumerate().skip(1) {
                if col_arr.len() != expected_len {
                    let caller = std::panic::Location::caller();
                    return Err(MonaxError::Internal(format!(
                        "column length mismatch at {}:{}: column 0 has {} rows but column {} has {} rows",
                        caller.file(),
                        caller.line(),
                        expected_len,
                        i,
                        col_arr.len()
                    )));
                }
            }
        }
        Ok(Self { schema, columns })
    }

    /// Returns the number of rows (assumes all columns have the same length).
    pub fn num_rows(&self) -> usize {
        self.columns.first().map(|c| c.len()).unwrap_or(0)
    }

    /// Returns the number of columns.
    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    /// Returns true if there are no rows.
    pub fn is_empty(&self) -> bool {
        self.num_rows() == 0
    }

    /// Get the column array at the given index.
    pub fn column(&self, idx: usize) -> Option<&ColumnArray> {
        self.columns.get(idx)
    }

    /// Get a mutable reference to the column array at the given index.
    pub fn column_mut(&mut self, idx: usize) -> Option<&mut ColumnArray> {
        self.columns.get_mut(idx)
    }

    /// Get the scalar value at (row, col).
    pub fn scalar(&self, row: usize, col: usize) -> Option<crate::types::scalar::ScalarValue> {
        self.columns.get(col).map(|c| c.scalar(row))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema() {
        let schema = Schema::new(vec![
            ColumnDef::new("id", DataType::Int64),
            ColumnDef::new("name", DataType::VarChar),
        ]);
        assert_eq!(schema.num_columns(), 2);
        assert_eq!(schema.index_of("id"), Some(0));
        assert_eq!(schema.index_of("name"), Some(1));
        assert_eq!(schema.index_of("missing"), None);
    }

    #[test]
    fn test_empty_batch() {
        let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
        let batch = RecordBatch::new(schema);
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 1);
        assert!(batch.is_empty());
    }

    #[test]
    fn test_batch_type_mismatch() {
        let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
        let columns = vec![ColumnArray::empty(DataType::VarChar)];
        let result = RecordBatch::try_new(schema, columns);
        assert!(result.is_err());
    }
}
