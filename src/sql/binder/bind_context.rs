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
    pub table_alias_map: HashMap<String, String>,
}

impl BindContext {
    pub fn new(database: String) -> Self {
        Self {
            database,
            table_schemas: HashMap::new(),
            column_index_map: HashMap::new(),
            current_table: None,
            output_schema: None,
            table_alias_map: HashMap::new(),
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
            table_alias_map: HashMap::new(),
        }
    }

    pub fn register_table_alias(&mut self, alias: &str, table_name: &str) {
        self.table_alias_map.insert(alias.to_lowercase(), table_name.to_string());
    }

    pub fn resolve_table_name(&self, name: &str) -> Option<String> {
        if let Some(real_name) = self.table_alias_map.get(&name.to_lowercase()) {
            Some(real_name.clone())
        } else if self.table_schemas.contains_key(name) {
            Some(name.to_string())
        } else {
            None
        }
    }

    pub fn resolve_column(&self, name: &str) -> Option<usize> {
        self.column_index_map.get(&name.to_lowercase()).copied()
    }

    pub fn resolve_qualified_column(&self, table: &str, column: &str) -> Option<usize> {
        let resolved_table = self.resolve_table_name(table)?;

        let schema = self.table_schemas.get(&resolved_table)?;
        schema.index_of(column)
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
