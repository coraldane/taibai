#![allow(dead_code)]
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
    Day = 2,
    Year = 3,
    Month = 4,
    Hash = 5,
    Mod = 6,
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

fn default_row_group_size() -> i32 {
    8192
}
fn default_partition_param() -> i32 {
    0
}
fn default_partition_key_idx() -> i32 {
    -1
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
    #[serde(default)]
    pub cluster_key: Vec<String>,
    #[serde(default = "default_row_group_size")]
    pub row_group_size: i32,
    #[serde(default)]
    pub partition_key: String,
    #[serde(default)]
    pub partition_type: PartitionType,
    #[serde(default = "default_partition_param")]
    pub partition_param: i32,
    #[serde(default)]
    pub partitions: Vec<PartitionInfo>,
    #[serde(default = "default_partition_key_idx")]
    pub partition_key_idx: i32,
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
            partition_param: 0,
            partitions: Vec::new(),
            partition_key_idx: -1,
        }
    }

    pub fn update_partition_key_idx(&mut self) {
        if self.partition_key.is_empty() {
            self.partition_key_idx = -1;
            return;
        }
        self.partition_key_idx = self
            .columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(&self.partition_key))
            .map(|i| i as i32)
            .unwrap_or(-1);
    }

    pub fn get_partition_column_index_cached(&self) -> Option<usize> {
        if self.partition_key_idx < 0 {
            return None;
        }
        Some(self.partition_key_idx as usize)
    }

    pub fn data_path(&self, base_dir: &str) -> PathBuf {
        PathBuf::from(base_dir)
            .join(&self.database)
            .join(&self.name)
    }

    pub fn partition_strategy(
        &self,
    ) -> crate::storage::partition::partition_function::PartitionFunction {
        use crate::storage::partition::partition_function::PartitionFunction;
        match self.partition_type {
            PartitionType::None => PartitionFunction::Value,
            PartitionType::Value => PartitionFunction::Value,
            PartitionType::Day => PartitionFunction::Day,
            PartitionType::Year => PartitionFunction::Year,
            PartitionType::Month => PartitionFunction::Month,
            PartitionType::Hash => PartitionFunction::Hash(self.partition_param),
            PartitionType::Mod => PartitionFunction::Mod(self.partition_param),
        }
    }

    pub fn is_partitioned(&self) -> bool {
        self.partition_type != PartitionType::None
    }

    pub fn get_partition_column_index(&self) -> Option<usize> {
        if self.partition_key.is_empty() {
            return None;
        }
        self.columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(&self.partition_key))
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

    pub fn load_or_create(meta_dir: &Path, db_name: &str) -> Result<Self> {
        let schema_path = Self::catalog_path(meta_dir, db_name);
        if schema_path.exists() {
            let data = fs::read_to_string(&schema_path)
                .map_err(|e| MonaxError::Storage(format!("failed to read catalog: {}", e)))?;
            let mut catalog: DatabaseCatalog = serde_json::from_str(&data)
                .map_err(|e| MonaxError::Storage(format!("failed to parse catalog: {}", e)))?;
            catalog.name = db_name.to_string();
            for table in catalog.tables.values_mut() {
                table.database = db_name.to_string();
                table.update_partition_key_idx();
            }
            Ok(catalog)
        } else if let Some(catalog) = Self::load_legacy_catalog(meta_dir, db_name)? {
            catalog.save(meta_dir)?;
            Ok(catalog)
        } else {
            let catalog = DatabaseCatalog::new(db_name);
            catalog.save(meta_dir)?;
            Ok(catalog)
        }
    }

    pub fn save(&self, meta_dir: &Path) -> Result<()> {
        let schema_path = Self::catalog_path(meta_dir, &self.name);
        if let Some(parent) = schema_path.parent() {
            fs::create_dir_all(parent)
                .map_err(|e| MonaxError::Storage(format!("failed to create catalog dir: {}", e)))?;
        }
        let data = serde_json::to_string_pretty(self)
            .map_err(|e| MonaxError::Storage(format!("failed to serialize catalog: {}", e)))?;
        let tmp_path = schema_path.with_extension("json.tmp");
        fs::write(&tmp_path, &data)
            .map_err(|e| MonaxError::Storage(format!("failed to write catalog tmp: {}", e)))?;
        fs::rename(&tmp_path, &schema_path)
            .map_err(|e| MonaxError::Storage(format!("failed to rename catalog: {}", e)))?;
        Ok(())
    }

    pub fn catalog_path(meta_dir: &Path, db_name: &str) -> PathBuf {
        meta_dir.join("catalogs").join(format!("{}.json", db_name))
    }

    pub fn catalog_dir(meta_dir: &Path) -> PathBuf {
        meta_dir.join("catalogs")
    }

    fn load_legacy_catalog(meta_dir: &Path, db_name: &str) -> Result<Option<Self>> {
        let legacy_path = meta_dir.join("schema.json");
        if !legacy_path.exists() {
            return Ok(None);
        }
        let data = fs::read_to_string(&legacy_path).map_err(|e| {
            MonaxError::Storage(format!("failed to read legacy schema.json: {}", e))
        })?;
        let mut catalog: DatabaseCatalog = serde_json::from_str(&data).map_err(|e| {
            MonaxError::Storage(format!("failed to parse legacy schema.json: {}", e))
        })?;
        if catalog.name != db_name {
            return Ok(None);
        }
        catalog.name = db_name.to_string();
        for table in catalog.tables.values_mut() {
            table.database = db_name.to_string();
            table.update_partition_key_idx();
        }
        Ok(Some(catalog))
    }

    pub fn create_table(&mut self, name: &str, columns: Vec<ColumnDef>) -> Result<()> {
        if self.tables.contains_key(name) {
            return Err(MonaxError::Sql(format!("table '{}' already exists", name)));
        }
        let meta = TableMeta::new(&self.name, name, columns);
        self.tables.insert(name.to_string(), meta);
        Ok(())
    }

    pub fn drop_table(&mut self, name: &str) -> Result<()> {
        if self.tables.remove(name).is_none() {
            return Err(MonaxError::Sql(format!("table '{}' does not exist", name)));
        }
        Ok(())
    }

    pub fn get_table(&self, name: &str) -> Option<&TableMeta> {
        self.tables.get(name)
    }

    pub fn get_table_mut(&mut self, name: &str) -> Option<&mut TableMeta> {
        self.tables.get_mut(name)
    }

    pub fn table_names(&self) -> Vec<&str> {
        self.tables.keys().map(|s| s.as_str()).collect()
    }

    pub fn rename_table(&mut self, old_name: &str, new_name: &str) -> Result<()> {
        if !self.tables.contains_key(old_name) {
            return Err(MonaxError::Sql(format!(
                "table '{}' does not exist",
                old_name
            )));
        }
        if self.tables.contains_key(new_name) {
            return Err(MonaxError::Sql(format!(
                "table '{}' already exists",
                new_name
            )));
        }
        let mut meta = self.tables.remove(old_name).unwrap();
        meta.name = new_name.to_string();
        self.tables.insert(new_name.to_string(), meta);
        Ok(())
    }

    pub fn add_column(&mut self, table_name: &str, column: ColumnDef) -> Result<()> {
        let table = self
            .tables
            .get_mut(table_name)
            .ok_or_else(|| MonaxError::Sql(format!("table '{}' does not exist", table_name)))?;
        if table.columns.iter().any(|c| c.name == column.name) {
            return Err(MonaxError::Sql(format!(
                "column '{}' already exists in table '{}'",
                column.name, table_name
            )));
        }
        table.columns.push(column);
        Ok(())
    }

    pub fn drop_column(&mut self, table_name: &str, column_name: &str) -> Result<()> {
        let table = self
            .tables
            .get_mut(table_name)
            .ok_or_else(|| MonaxError::Sql(format!("table '{}' does not exist", table_name)))?;
        let idx = table
            .columns
            .iter()
            .position(|c| c.name == column_name)
            .ok_or_else(|| {
                MonaxError::Sql(format!(
                    "column '{}' does not exist in table '{}'",
                    column_name, table_name
                ))
            })?;
        table.columns.remove(idx);
        Ok(())
    }

    pub fn rename_column(
        &mut self,
        table_name: &str,
        old_name: &str,
        new_name: &str,
    ) -> Result<()> {
        let table = self
            .tables
            .get_mut(table_name)
            .ok_or_else(|| MonaxError::Sql(format!("table '{}' does not exist", table_name)))?;
        if table.columns.iter().any(|c| c.name == new_name) {
            return Err(MonaxError::Sql(format!(
                "column '{}' already exists in table '{}'",
                new_name, table_name
            )));
        }
        let col = table
            .columns
            .iter_mut()
            .find(|c| c.name == old_name)
            .ok_or_else(|| {
                MonaxError::Sql(format!(
                    "column '{}' does not exist in table '{}'",
                    old_name, table_name
                ))
            })?;
        col.name = new_name.to_string();
        Ok(())
    }

    pub fn modify_column(
        &mut self,
        table_name: &str,
        column_name: &str,
        data_type: crate::protocol::types::DataType,
        scale: i32,
        precision: i32,
    ) -> Result<()> {
        let table = self
            .tables
            .get_mut(table_name)
            .ok_or_else(|| MonaxError::Sql(format!("table '{}' does not exist", table_name)))?;
        let col = table
            .columns
            .iter_mut()
            .find(|c| c.name == column_name)
            .ok_or_else(|| {
                MonaxError::Sql(format!(
                    "column '{}' does not exist in table '{}'",
                    column_name, table_name
                ))
            })?;
        col.data_type = data_type;
        col.scale = scale;
        col.precision = precision;
        Ok(())
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
        catalog
            .create_table(
                "users",
                vec![
                    ColumnDef::new("id", DataType::Int64),
                    ColumnDef::new("name", DataType::VarChar),
                ],
            )
            .unwrap();

        let table = catalog.get_table("users").unwrap();
        assert_eq!(table.name, "users");
        assert_eq!(table.columns.len(), 2);
        assert_eq!(table.row_count, 0);
    }

    #[test]
    fn test_duplicate_table() {
        let mut catalog = DatabaseCatalog::new("testdb");
        catalog
            .create_table("t", vec![ColumnDef::new("id", DataType::Int64)])
            .unwrap();

        let result = catalog.create_table("t", vec![ColumnDef::new("id", DataType::Int64)]);
        assert!(result.is_err());
    }

    #[test]
    fn test_drop_table() {
        let mut catalog = DatabaseCatalog::new("testdb");
        catalog
            .create_table("t", vec![ColumnDef::new("id", DataType::Int64)])
            .unwrap();

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
        catalog
            .create_table(
                "users",
                vec![
                    ColumnDef::new("id", DataType::Int64),
                    ColumnDef::new("name", DataType::VarChar),
                ],
            )
            .unwrap();
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

        assert!(DatabaseCatalog::catalog_path(&meta_dir, "newdb").exists());
    }

    #[test]
    fn test_catalogs_are_isolated_per_database() {
        let dir = TempDir::new().unwrap();
        let meta_dir = dir.path().join("meta");

        let mut db1 = DatabaseCatalog::new("db1");
        db1.create_table("t1", vec![ColumnDef::new("id", DataType::Int64)])
            .unwrap();
        db1.save(&meta_dir).unwrap();

        let mut db2 = DatabaseCatalog::new("db2");
        db2.create_table("t2", vec![ColumnDef::new("id", DataType::Int64)])
            .unwrap();
        db2.save(&meta_dir).unwrap();

        let loaded_db1 = DatabaseCatalog::load_or_create(&meta_dir, "db1").unwrap();
        let loaded_db2 = DatabaseCatalog::load_or_create(&meta_dir, "db2").unwrap();

        assert!(loaded_db1.get_table("t1").is_some());
        assert!(loaded_db1.get_table("t2").is_none());
        assert!(loaded_db2.get_table("t1").is_none());
        assert!(loaded_db2.get_table("t2").is_some());
    }

    #[test]
    fn test_table_names() {
        let mut catalog = DatabaseCatalog::new("testdb");
        catalog
            .create_table("a", vec![ColumnDef::new("id", DataType::Int64)])
            .unwrap();
        catalog
            .create_table("b", vec![ColumnDef::new("id", DataType::Int64)])
            .unwrap();

        let mut names = catalog.table_names();
        names.sort();
        assert_eq!(names, vec!["a", "b"]);
    }

    #[test]
    fn test_rename_table() {
        let mut catalog = DatabaseCatalog::new("testdb");
        catalog
            .create_table("old", vec![ColumnDef::new("id", DataType::Int64)])
            .unwrap();

        catalog.rename_table("old", "new").unwrap();
        assert!(catalog.get_table("old").is_none());
        assert!(catalog.get_table("new").is_some());
    }

    #[test]
    fn test_partition_type_enum() {
        let mut meta = TableMeta::new(
            "testdb",
            "test_table",
            vec![
                ColumnDef::new("id", DataType::Int64),
                ColumnDef::new("ts", DataType::Timestamp),
            ],
        );

        meta.partition_type = PartitionType::Day;
        meta.partition_key = "ts".to_string();
        assert_eq!(
            meta.partition_strategy(),
            crate::storage::partition::partition_function::PartitionFunction::Day
        );

        meta.partition_type = PartitionType::Year;
        assert_eq!(
            meta.partition_strategy(),
            crate::storage::partition::partition_function::PartitionFunction::Year
        );

        meta.partition_type = PartitionType::Month;
        assert_eq!(
            meta.partition_strategy(),
            crate::storage::partition::partition_function::PartitionFunction::Month
        );

        meta.partition_type = PartitionType::Hash;
        meta.partition_param = 32;
        assert_eq!(
            meta.partition_strategy(),
            crate::storage::partition::partition_function::PartitionFunction::Hash(32)
        );

        meta.partition_type = PartitionType::Mod;
        meta.partition_param = 4;
        assert_eq!(
            meta.partition_strategy(),
            crate::storage::partition::partition_function::PartitionFunction::Mod(4)
        );
    }

    #[test]
    fn test_is_partitioned() {
        let mut meta = TableMeta::new(
            "testdb",
            "test_table",
            vec![ColumnDef::new("id", DataType::Int64)],
        );

        assert!(!meta.is_partitioned());

        meta.partition_type = PartitionType::Day;
        meta.partition_key = "ts".to_string();
        assert!(meta.is_partitioned());

        meta.partition_type = PartitionType::None;
        assert!(!meta.is_partitioned());
    }

    #[test]
    fn test_cluster_key() {
        let mut meta = TableMeta::new(
            "testdb",
            "test_table",
            vec![
                ColumnDef::new("id", DataType::Int64),
                ColumnDef::new("name", DataType::VarChar),
            ],
        );

        assert!(meta.cluster_key.is_empty());

        meta.cluster_key = vec!["id".to_string(), "name".to_string()];
        assert_eq!(meta.cluster_key.len(), 2);
        assert_eq!(meta.cluster_key[0], "id");
        assert_eq!(meta.cluster_key[1], "name");
    }

    #[test]
    fn test_partition_column_index() {
        let mut meta = TableMeta::new(
            "testdb",
            "test_table",
            vec![
                ColumnDef::new("id", DataType::Int64),
                ColumnDef::new("ts", DataType::Timestamp),
                ColumnDef::new("name", DataType::VarChar),
            ],
        );

        assert!(meta.get_partition_column_index().is_none());

        meta.partition_key = "ts".to_string();
        let idx = meta.get_partition_column_index();
        assert!(idx.is_some());
        assert_eq!(idx.unwrap(), 1);

        meta.partition_key = "TS".to_string();
        let idx = meta.get_partition_column_index();
        assert!(idx.is_some());
        assert_eq!(idx.unwrap(), 1);

        meta.partition_key = "nonexistent".to_string();
        assert!(meta.get_partition_column_index().is_none());
    }
}
