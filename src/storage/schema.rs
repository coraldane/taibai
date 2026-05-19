use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::common::error::{MonaxError, Result};
use crate::types::batch::ColumnDef;

/// Partition type for a table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PartitionType {
    None = 0,
    Value = 1,
    Function = 2,
}

impl Default for PartitionType {
    fn default() -> Self {
        PartitionType::None
    }
}

/// Partition info for a table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionInfo {
    pub key: String,
    pub row_count: i64,
    pub segment_count: i32,
    pub created_at: i64,
    pub modified_at: i64,
}

/// Table metadata stored in the catalog.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMeta {
    pub name: String,
    pub database: String,
    pub columns: Vec<ColumnDef>,
    pub row_count: i64,
    pub segment_count: i32,
    pub created_at: i64,
    pub cluster_key: Vec<String>,
    pub row_group_size: i32,
    pub partition_key: String,
    pub partition_type: PartitionType,
    pub partitions: Vec<PartitionInfo>,
}

impl TableMeta {
    pub fn new(database: &str, name: &str, columns: Vec<ColumnDef>) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        Self {
            name: name.to_string(),
            database: database.to_string(),
            columns,
            row_count: 0,
            segment_count: 0,
            created_at: now,
            cluster_key: Vec::new(),
            row_group_size: 8192,
            partition_key: String::new(),
            partition_type: PartitionType::None,
            partitions: Vec::new(),
        }
    }

    /// Returns the data directory path for this table's segments.
    pub fn data_path(&self, base_dir: &str) -> PathBuf {
        PathBuf::from(base_dir).join(&self.database).join(&self.name)
    }
}

/// Database catalog: manages table schemas via JSON files.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseCatalog {
    pub name: String,
    pub tables: HashMap<String, TableMeta>,
    pub created_at: i64,
}

impl DatabaseCatalog {
    /// Create a new empty database catalog.
    pub fn new(name: &str) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        Self {
            name: name.to_string(),
            tables: HashMap::new(),
            created_at: now,
        }
    }

    /// Load or create the catalog for a database from the meta directory.
    pub fn load_or_create(meta_dir: &Path, db_name: &str) -> Result<Self> {
        let schema_path = meta_dir.join("schema.json");
        if schema_path.exists() {
            let data = fs::read_to_string(&schema_path)
                .map_err(|e| MonaxError::Storage(format!("failed to read schema.json: {}", e)))?;
            let catalog: DatabaseCatalog = serde_json::from_str(&data)
                .map_err(|e| MonaxError::Storage(format!("failed to parse schema.json: {}", e)))?;
            Ok(catalog)
        } else {
            let catalog = DatabaseCatalog::new(db_name);
            fs::create_dir_all(meta_dir)
                .map_err(|e| MonaxError::Storage(format!("failed to create meta dir: {}", e)))?;
            catalog.save(meta_dir)?;
            Ok(catalog)
        }
    }

    /// Save the catalog to the meta directory.
    pub fn save(&self, meta_dir: &Path) -> Result<()> {
        fs::create_dir_all(meta_dir)
            .map_err(|e| MonaxError::Storage(format!("failed to create meta dir: {}", e)))?;
        let schema_path = meta_dir.join("schema.json");
        let data = serde_json::to_string_pretty(self)
            .map_err(|e| MonaxError::Storage(format!("failed to serialize catalog: {}", e)))?;
        // Write atomically via temp file
        let tmp_path = schema_path.with_extension("json.tmp");
        fs::write(&tmp_path, &data)
            .map_err(|e| MonaxError::Storage(format!("failed to write schema.json.tmp: {}", e)))?;
        fs::rename(&tmp_path, &schema_path)
            .map_err(|e| MonaxError::Storage(format!("failed to rename schema.json: {}", e)))?;
        Ok(())
    }

    /// Create a new table in the catalog.
    pub fn create_table(&mut self, name: &str, columns: Vec<ColumnDef>) -> Result<()> {
        if self.tables.contains_key(name) {
            return Err(MonaxError::Sql(format!("table '{}' already exists", name)));
        }
        let meta = TableMeta::new(&self.name, name, columns);
        self.tables.insert(name.to_string(), meta);
        Ok(())
    }

    /// Drop a table from the catalog.
    pub fn drop_table(&mut self, name: &str) -> Result<()> {
        if self.tables.remove(name).is_none() {
            return Err(MonaxError::Sql(format!("table '{}' does not exist", name)));
        }
        Ok(())
    }

    /// Get a table's metadata.
    pub fn get_table(&self, name: &str) -> Option<&TableMeta> {
        self.tables.get(name)
    }

    /// Get a mutable reference to a table's metadata.
    pub fn get_table_mut(&mut self, name: &str) -> Option<&mut TableMeta> {
        self.tables.get_mut(name)
    }

    /// List all table names.
    pub fn table_names(&self) -> Vec<&str> {
        self.tables.keys().map(|s| s.as_str()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::types::DataType;
    use tempfile::TempDir;

    #[test]
    fn test_create_and_get_table() {
        let mut catalog = DatabaseCatalog::new("testdb");
        catalog.create_table("users", vec![
            ColumnDef::new("id", DataType::Int64),
            ColumnDef::new("name", DataType::VarChar),
        ]).unwrap();

        let table = catalog.get_table("users").unwrap();
        assert_eq!(table.name, "users");
        assert_eq!(table.columns.len(), 2);
        assert_eq!(table.row_count, 0);
    }

    #[test]
    fn test_duplicate_table() {
        let mut catalog = DatabaseCatalog::new("testdb");
        catalog.create_table("t", vec![
            ColumnDef::new("id", DataType::Int64),
        ]).unwrap();

        let result = catalog.create_table("t", vec![
            ColumnDef::new("id", DataType::Int64),
        ]);
        assert!(result.is_err());
    }

    #[test]
    fn test_drop_table() {
        let mut catalog = DatabaseCatalog::new("testdb");
        catalog.create_table("t", vec![
            ColumnDef::new("id", DataType::Int64),
        ]).unwrap();

        catalog.drop_table("t").unwrap();
        assert!(catalog.get_table("t").is_none());
    }

    #[test]
    fn test_drop_nonexistent() {
        let mut catalog = DatabaseCatalog::new("testdb");
        let result = catalog.drop_table("nope");
        assert!(result.is_err());
    }

    #[test]
    fn test_save_and_load() {
        let dir = TempDir::new().unwrap();
        let meta_dir = dir.path().join("meta");

        let mut catalog = DatabaseCatalog::new("testdb");
        catalog.create_table("users", vec![
            ColumnDef::new("id", DataType::Int64),
            ColumnDef::new("name", DataType::VarChar),
        ]).unwrap();
        catalog.save(&meta_dir).unwrap();

        let loaded = DatabaseCatalog::load_or_create(&meta_dir, "testdb").unwrap();
        assert_eq!(loaded.name, "testdb");
        assert_eq!(loaded.tables.len(), 1);
        let table = loaded.get_table("users").unwrap();
        assert_eq!(table.columns.len(), 2);
    }

    #[test]
    fn test_load_or_create_new() {
        let dir = TempDir::new().unwrap();
        let meta_dir = dir.path().join("new_meta");

        let catalog = DatabaseCatalog::load_or_create(&meta_dir, "newdb").unwrap();
        assert_eq!(catalog.name, "newdb");
        assert!(catalog.tables.is_empty());

        // Should have created the file
        assert!(meta_dir.join("schema.json").exists());
    }

    #[test]
    fn test_table_names() {
        let mut catalog = DatabaseCatalog::new("testdb");
        catalog.create_table("a", vec![ColumnDef::new("id", DataType::Int64)]).unwrap();
        catalog.create_table("b", vec![ColumnDef::new("id", DataType::Int64)]).unwrap();

        let mut names = catalog.table_names();
        names.sort();
        assert_eq!(names, vec!["a", "b"]);
    }
}
