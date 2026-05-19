use std::collections::HashMap;

use crate::protocol::types::DataType;
use crate::types::batch::Schema;

#[derive(Debug, Clone)]
pub struct BindContext {
    pub database: String,
    pub table_schemas: HashMap<String, Schema>,
    pub column_index_map: HashMap<String, usize>,
    pub current_table: Option<String>,
    pub output_schema: Option<Schema>,
}

impl BindContext {
    pub fn new(database: String) -> Self {
        Self {
            database,
            table_schemas: HashMap::new(),
            column_index_map: HashMap::new(),
            current_table: None,
            output_schema: None,
        }
    }

    pub fn with_table(table_name: &str, schema: Schema) -> Self {
        let mut column_index_map = HashMap::new();
        for (i, col) in schema.columns.iter().enumerate() {
            column_index_map.insert(col.name.to_lowercase(), i);
        }
        Self {
            database: String::new(),
            table_schemas: {
                let mut m = HashMap::new();
                m.insert(table_name.to_string(), schema);
                m
            },
            column_index_map,
            current_table: Some(table_name.to_string()),
            output_schema: None,
        }
    }

    pub fn resolve_column(&self, name: &str) -> Option<usize> {
        self.column_index_map.get(&name.to_lowercase()).copied()
    }

    pub fn resolve_qualified_column(&self, _table: &str, column: &str) -> Option<usize> {
        self.column_index_map.get(&column.to_lowercase()).copied()
    }

    pub fn column_type(&self, idx: usize) -> Option<DataType> {
        if let Some(schema) = &self.output_schema {
            schema.columns.get(idx).map(|c| c.data_type)
        } else if let Some(table) = &self.current_table {
            self.table_schemas.get(table)
                .and_then(|s| s.columns.get(idx))
                .map(|c| c.data_type)
        } else {
            None
        }
    }

    pub fn schema(&self) -> Option<&Schema> {
        if let Some(ref s) = self.output_schema {
            Some(s)
        } else if let Some(ref table) = self.current_table {
            self.table_schemas.get(table)
        } else {
            None
        }
    }
}
