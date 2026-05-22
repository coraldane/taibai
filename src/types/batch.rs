use serde::{Deserialize, Serialize};

use crate::common::error::{MonaxError, Result};
use crate::protocol::types::DataType;
use crate::types::array::ColumnArray;

/// Column definition within a table schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub scale: i32,
    pub precision: i32,
}

impl ColumnDef {
    pub fn new(name: &str, data_type: DataType) -> Self {
        Self {
            name: name.to_string(),
            data_type,
            nullable: true,
            scale: 0,
            precision: 0,
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
}

/// Schema for a table: column names + types.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    pub columns: Vec<ColumnDef>,
}

impl Schema {
    pub fn new(columns: Vec<ColumnDef>) -> Self {
        Self { columns }
    }

    pub fn empty() -> Self {
        Self { columns: Vec::new() }
    }

    /// Returns the number of columns.
    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    /// Find the index of a column by name (case-insensitive).
    pub fn index_of(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name.eq_ignore_ascii_case(name))
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
        let columns = schema.columns.iter()
            .map(|c| ColumnArray::empty(c.data_type))
            .collect();
        Self { schema, columns }
    }

    /// Create a RecordBatch from pre-built column arrays.
    /// Validates that the column count matches the schema.
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
        let schema = Schema::new(vec![
            ColumnDef::new("id", DataType::Int64),
        ]);
        let batch = RecordBatch::new(schema);
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 1);
        assert!(batch.is_empty());
    }

    #[test]
    fn test_batch_type_mismatch() {
        let schema = Schema::new(vec![
            ColumnDef::new("id", DataType::Int64),
        ]);
        let columns = vec![ColumnArray::empty(DataType::VarChar)];
        let result = RecordBatch::try_new(schema, columns);
        assert!(result.is_err());
    }
}
