use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::common::error::{Result, TaibaiError};
use crate::types::batch::ColumnDef;
use crate::types::scalar::ScalarValue;

/// Partition type for a table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum PartitionType {
    #[default]
    None = 0,
    Value = 1,
    Day = 2,
    Year = 3,
    Month = 4,
    Hash = 5,
    Mod = 6,
}

/// Durability contract for a table's ordinary INSERT path.
///
/// `Strict` (default) keeps the memtable + durable-tail pipeline: every
/// acknowledged auto-commit INSERT is fsynced to the tail before the ack and
/// survives a crash. `Relaxed` routes qualifying large multi-row INSERTs
/// directly into the segment row-group buffer, skipping the durable tail
/// entirely: acknowledged rows may be lost when the process crashes before
/// the buffered row group is flushed, which is the documented trade for
/// halving write amplification on bulk-style data feeds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum TableDurability {
    #[default]
    Strict,
    Relaxed,
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
fn default_auto_increment_next_value() -> u64 {
    1
}
fn default_next_column_id() -> u32 {
    1
}

/// Time-to-live policy for a table. Rows are visible while
/// `column + duration_secs` is greater than the current time.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TtlInfo {
    pub column: String,
    pub duration_secs: i64,
    #[serde(default = "default_ttl_column_idx")]
    pub column_idx: i32,
}

fn default_ttl_column_idx() -> i32 {
    -1
}

/// Table metadata stored in the catalog.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UniqueIndexMeta {
    pub name: String,
    pub columns: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum SecondaryIndexType {
    #[default]
    Bloom,
    MinMax,
    Set,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecondaryIndexMeta {
    pub name: String,
    pub columns: Vec<String>,
    #[serde(default)]
    pub index_type: SecondaryIndexType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistogramBucketMeta {
    pub lower_bound: ScalarValue,
    pub upper_bound: ScalarValue,
    pub sample_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnStatisticsMeta {
    pub distinct_count: i64,
    pub null_count: i64,
    pub min_value: Option<ScalarValue>,
    pub max_value: Option<ScalarValue>,
    #[serde(default)]
    pub histogram: Vec<HistogramBucketMeta>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompositeStatisticsMeta {
    pub columns: Vec<String>,
    pub distinct_count: i64,
    pub null_count: i64,
    #[serde(default)]
    pub unique: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableStatisticsMeta {
    pub row_count: i64,
    pub schema_version: u64,
    pub collected_at: i64,
    pub columns: HashMap<String, ColumnStatisticsMeta>,
    #[serde(default)]
    pub composite_columns: Vec<CompositeStatisticsMeta>,
}

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
    #[serde(default)]
    pub hot_time_column: Option<String>,
    #[serde(default)]
    pub hot_close_time: Option<String>,
    #[serde(default)]
    pub hot_last_close_date: Option<String>,
    /// Epoch millis of the moment the last close-of-day flush completed
    /// (after seal, resident eviction and optimize). Startup warmup uses it
    /// as the recency cutoff: only segments modified after this instant are
    /// reloaded into the hot store, because older ones were archived by that
    /// close and a continuously-running server would not hold them resident.
    #[serde(default)]
    pub hot_last_close_at: Option<i64>,
    /// Partition-column raw value bound establishing the partial-residency
    /// invariant: the hot mirror holds every row whose partition-column value
    /// is strictly greater than this bound. Established at close-of-day as the
    /// max over archived segment zone maps (the mirror is empty at that
    /// instant, so only post-close writes — all mirrored — can exceed it) and
    /// re-established by restart warmup when every post-close segment was
    /// fully loaded. Any mirror bypass (`hot_unfed_rows`) invalidates the
    /// claim at query time via `hot_partition_mirrored`.
    #[serde(default)]
    pub hot_mirror_covers_from: Option<i64>,
    #[serde(default)]
    pub hot_index_column: Option<String>,
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
    #[serde(default)]
    pub ttl: Option<TtlInfo>,
    #[serde(default)]
    pub unique_indexes: Vec<UniqueIndexMeta>,
    #[serde(default)]
    pub secondary_indexes: Vec<SecondaryIndexMeta>,
    #[serde(default = "default_auto_increment_next_value")]
    pub auto_increment_next_value: u64,
    #[serde(default = "default_next_column_id")]
    pub next_column_id: u32,
    #[serde(default)]
    pub schema_version: u64,
    #[serde(default)]
    pub dropped_columns: Vec<ColumnDef>,
    #[serde(default)]
    pub statistics: Option<TableStatisticsMeta>,
    #[serde(default)]
    pub durability: TableDurability,
    /// Per-table zstd compression level override (1..=22) applied to segment
    /// writes; `None` follows the global `compression.bulk_zstd_level` config.
    #[serde(default)]
    pub compression_level: Option<i32>,
}

impl TableMeta {
    pub fn new(database: &str, name: &str, columns: Vec<ColumnDef>) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        let mut meta = Self {
            name: name.to_string(),
            database: database.to_string(),
            columns,
            row_count: 0,
            segment_count: 0,
            created_at: now,
            cluster_key: Vec::new(),
            hot_time_column: None,
            hot_close_time: None,
            hot_last_close_date: None,
            hot_last_close_at: None,
            hot_mirror_covers_from: None,
            hot_index_column: None,
            row_group_size: 8192,
            partition_key: String::new(),
            partition_type: PartitionType::None,
            partition_param: 0,
            partitions: Vec::new(),
            partition_key_idx: -1,
            ttl: None,
            unique_indexes: Vec::new(),
            secondary_indexes: Vec::new(),
            auto_increment_next_value: 1,
            next_column_id: 1,
            schema_version: 1,
            dropped_columns: Vec::new(),
            statistics: None,
            durability: TableDurability::Strict,
            compression_level: None,
        };
        meta.ensure_column_ids();
        meta
    }

    pub fn valid_statistics(&self) -> Option<&TableStatisticsMeta> {
        self.statistics.as_ref().filter(|statistics| {
            statistics.row_count == self.row_count
                && statistics.schema_version == self.schema_version
        })
    }

    pub fn ensure_column_ids(&mut self) {
        let mut max_id = self.next_column_id.saturating_sub(1);
        for column in self.columns.iter().chain(self.dropped_columns.iter()) {
            max_id = max_id.max(column.column_id);
        }
        for column in &mut self.columns {
            if column.column_id == 0 {
                max_id = max_id.saturating_add(1);
                column.column_id = max_id;
            }
        }
        self.next_column_id = max_id.saturating_add(1).max(1);
    }

    fn assign_column_id(&mut self, column: &mut ColumnDef) {
        if column.column_id == 0 {
            column.column_id = self.next_column_id.max(1);
            self.next_column_id = column.column_id.saturating_add(1);
        } else {
            self.next_column_id = self.next_column_id.max(column.column_id.saturating_add(1));
        }
    }

    fn bump_schema_version(&mut self) {
        self.schema_version = self.schema_version.saturating_add(1).max(1);
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

    pub fn update_ttl_column_idx(&mut self) {
        let Some(ttl) = self.ttl.as_mut() else {
            return;
        };
        ttl.column_idx = self
            .columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(&ttl.column))
            .map(|i| i as i32)
            .unwrap_or(-1);
    }

    pub fn auto_increment_column(&self) -> Option<&ColumnDef> {
        self.columns.iter().find(|c| c.auto_increment)
    }

    pub fn has_auto_increment(&self) -> bool {
        self.auto_increment_column().is_some()
    }

    pub fn ensure_auto_increment_next_value(&mut self) {
        if self.has_auto_increment() && self.auto_increment_next_value == 0 {
            self.auto_increment_next_value = self.row_count.max(0) as u64 + 1;
        }
    }

    pub fn reset_auto_increment_next_value(&mut self) {
        if self.has_auto_increment() {
            self.auto_increment_next_value = 1;
        }
    }

    pub fn set_ttl(&mut self, column: &str, duration_secs: i64) {
        self.ttl = Some(TtlInfo {
            column: column.to_string(),
            duration_secs,
            column_idx: -1,
        });
        self.update_ttl_column_idx();
    }

    pub fn clear_ttl(&mut self) {
        self.ttl = None;
    }

    pub fn get_ttl_column_index_cached(&self) -> Option<usize> {
        let ttl = self.ttl.as_ref()?;
        if ttl.column_idx < 0 {
            return None;
        }
        Some(ttl.column_idx as usize)
    }

    pub fn get_partition_column_index_cached(&self) -> Option<usize> {
        if self.partition_key_idx < 0 {
            return None;
        }
        Some(self.partition_key_idx as usize)
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

    pub fn drop_unique_index(&mut self, name: &str) -> Result<()> {
        let before = self.unique_indexes.len();
        self.unique_indexes
            .retain(|idx| !idx.name.eq_ignore_ascii_case(name));
        if self.unique_indexes.len() == before {
            return Err(TaibaiError::Sql(format!("index '{}' does not exist", name)));
        }
        Ok(())
    }

    pub fn drop_secondary_index(&mut self, name: &str) -> Result<()> {
        let before = self.secondary_indexes.len();
        self.secondary_indexes
            .retain(|idx| !idx.name.eq_ignore_ascii_case(name));
        if self.secondary_indexes.len() == before {
            return Err(TaibaiError::Sql(format!("index '{}' does not exist", name)));
        }
        Ok(())
    }

    pub fn has_index(&self, name: &str) -> bool {
        self.unique_indexes
            .iter()
            .any(|idx| idx.name.eq_ignore_ascii_case(name))
            || self
                .secondary_indexes
                .iter()
                .any(|idx| idx.name.eq_ignore_ascii_case(name))
    }

    pub fn indexed_columns_by_type(&self, index_type: SecondaryIndexType) -> Vec<String> {
        let mut columns = Vec::new();
        for index in &self.secondary_indexes {
            if index.index_type != index_type {
                continue;
            }
            for column in &index.columns {
                if !columns
                    .iter()
                    .any(|c: &String| c.eq_ignore_ascii_case(column))
                {
                    columns.push(column.clone());
                }
            }
        }
        columns
    }

    pub fn add_unique_index(&mut self, name: &str, columns: Vec<String>) -> Result<()> {
        if self
            .unique_indexes
            .iter()
            .any(|idx| idx.name.eq_ignore_ascii_case(name))
            || self
                .secondary_indexes
                .iter()
                .any(|idx| idx.name.eq_ignore_ascii_case(name))
        {
            return Err(TaibaiError::Sql(format!("index '{}' already exists", name)));
        }
        for column in &columns {
            if !self
                .columns
                .iter()
                .any(|c| c.name.eq_ignore_ascii_case(column))
            {
                return Err(TaibaiError::Sql(format!(
                    "unique index column '{}' not found",
                    column
                )));
            }
        }
        self.unique_indexes.push(UniqueIndexMeta {
            name: name.to_string(),
            columns,
        });
        Ok(())
    }

    pub fn add_secondary_index(
        &mut self,
        name: &str,
        columns: Vec<String>,
        index_type: SecondaryIndexType,
    ) -> Result<()> {
        if self
            .secondary_indexes
            .iter()
            .any(|idx| idx.name.eq_ignore_ascii_case(name))
            || self
                .unique_indexes
                .iter()
                .any(|idx| idx.name.eq_ignore_ascii_case(name))
        {
            return Err(TaibaiError::Sql(format!("index '{}' already exists", name)));
        }
        if columns.is_empty() {
            return Err(TaibaiError::Sql(
                "index must contain at least one column".into(),
            ));
        }
        let mut seen = std::collections::HashSet::new();
        for column in &columns {
            let Some((idx, _)) = self
                .columns
                .iter()
                .enumerate()
                .find(|(_, c)| c.name.eq_ignore_ascii_case(column))
            else {
                return Err(TaibaiError::Sql(format!(
                    "index column '{}' not found",
                    column
                )));
            };
            if !seen.insert(idx) {
                return Err(TaibaiError::Sql(format!(
                    "duplicate index column '{}'",
                    column
                )));
            }
        }
        self.secondary_indexes.push(SecondaryIndexMeta {
            name: name.to_string(),
            columns,
            index_type,
        });
        Ok(())
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
    pub tables: HashMap<String, std::sync::Arc<TableMeta>>,
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
                .map_err(|e| TaibaiError::Storage(format!("failed to read catalog: {}", e)))?;
            let mut catalog: DatabaseCatalog = serde_json::from_str(&data)
                .map_err(|e| TaibaiError::Storage(format!("failed to parse catalog: {}", e)))?;
            catalog.name = db_name.to_string();
            for table in catalog.tables.values_mut() {
                // Freshly loaded maps hold unique Arcs, so make_mut is a
                // no-copy unwrap here.
                let table = std::sync::Arc::make_mut(table);
                table.database = db_name.to_string();
                table.update_partition_key_idx();
                table.update_ttl_column_idx();
                table.ensure_auto_increment_next_value();
                table.ensure_column_ids();
            }
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
            fs::create_dir_all(parent).map_err(|e| {
                TaibaiError::Storage(format!("failed to create catalog dir: {}", e))
            })?;
        }
        let data = serde_json::to_string_pretty(self)
            .map_err(|e| TaibaiError::Storage(format!("failed to serialize catalog: {}", e)))?;
        // Write + fsync on one handle: the fsync must happen before the
        // rename or a power loss can persist the rename ahead of the data
        // blocks, leaving a torn/empty catalog that fails to deserialize at
        // startup — every table definition in the database becomes
        // unloadable. (Reopening the file read-only for sync_all fails on
        // Windows: FlushFileBuffers needs a write handle.)
        let tmp_path = schema_path.with_extension("json.tmp");
        use std::io::Write as _;
        let mut tmp = fs::File::create(&tmp_path)
            .map_err(|e| TaibaiError::Storage(format!("failed to create catalog tmp: {}", e)))?;
        tmp.write_all(data.as_bytes())
            .map_err(|e| TaibaiError::Storage(format!("failed to write catalog tmp: {}", e)))?;
        tmp.sync_all()
            .map_err(|e| TaibaiError::Storage(format!("failed to fsync catalog tmp: {}", e)))?;
        drop(tmp);
        fs::rename(&tmp_path, &schema_path)
            .map_err(|e| TaibaiError::Storage(format!("failed to rename catalog: {}", e)))?;
        Ok(())
    }

    pub fn catalog_path(meta_dir: &Path, db_name: &str) -> PathBuf {
        meta_dir.join("catalogs").join(format!("{}.json", db_name))
    }

    pub fn catalog_dir(meta_dir: &Path) -> PathBuf {
        meta_dir.join("catalogs")
    }

    pub fn create_table(&mut self, name: &str, columns: Vec<ColumnDef>) -> Result<()> {
        if self.tables.contains_key(name) {
            return Err(TaibaiError::Sql(format!("table '{}' already exists", name)));
        }
        let meta = TableMeta::new(&self.name, name, columns);
        self.tables
            .insert(name.to_string(), std::sync::Arc::new(meta));
        Ok(())
    }

    pub fn drop_table(&mut self, name: &str) -> Result<()> {
        if self.tables.remove(name).is_none() {
            return Err(TaibaiError::Sql(format!("table '{}' does not exist", name)));
        }
        Ok(())
    }

    pub fn get_table(&self, name: &str) -> Option<&TableMeta> {
        self.tables.get(name).map(|table| table.as_ref())
    }

    /// Shared handle for hot paths that only read metadata: cloning the Arc
    /// escapes the catalog lock without copying the table definition.
    pub fn get_table_shared(&self, name: &str) -> Option<&std::sync::Arc<TableMeta>> {
        self.tables.get(name)
    }

    pub fn get_table_mut(&mut self, name: &str) -> Option<&mut TableMeta> {
        // Copy-on-write: DDL under the catalog write lock pays a TableMeta
        // copy only when an in-flight reader still shares the old snapshot.
        let table = self.tables.get_mut(name)?;
        Some(std::sync::Arc::make_mut(table))
    }

    pub fn table_names(&self) -> Vec<&str> {
        self.tables.keys().map(|s| s.as_str()).collect()
    }

    pub fn rename_table(&mut self, old_name: &str, new_name: &str) -> Result<()> {
        if !self.tables.contains_key(old_name) {
            return Err(TaibaiError::Sql(format!(
                "table '{}' does not exist",
                old_name
            )));
        }
        if self.tables.contains_key(new_name) {
            return Err(TaibaiError::Sql(format!(
                "table '{}' already exists",
                new_name
            )));
        }
        let mut meta = self.tables.remove(old_name).unwrap();
        let meta = std::sync::Arc::make_mut(&mut meta);
        meta.name = new_name.to_string();
        self.tables
            .insert(new_name.to_string(), std::sync::Arc::new(meta.clone()));
        Ok(())
    }

    pub fn add_column(&mut self, table_name: &str, mut column: ColumnDef) -> Result<()> {
        let table = self
            .get_table_mut(table_name)
            .ok_or_else(|| TaibaiError::Sql(format!("table '{}' does not exist", table_name)))?;
        if table.columns.iter().any(|c| c.name == column.name) {
            return Err(TaibaiError::Sql(format!(
                "column '{}' already exists in table '{}'",
                column.name, table_name
            )));
        }
        table.assign_column_id(&mut column);
        table.columns.push(column);
        table.bump_schema_version();
        Ok(())
    }

    pub fn drop_column(&mut self, table_name: &str, column_name: &str) -> Result<()> {
        let table = self
            .get_table_mut(table_name)
            .ok_or_else(|| TaibaiError::Sql(format!("table '{}' does not exist", table_name)))?;
        let idx = table
            .columns
            .iter()
            .position(|c| c.name == column_name)
            .ok_or_else(|| {
                TaibaiError::Sql(format!(
                    "column '{}' does not exist in table '{}'",
                    column_name, table_name
                ))
            })?;
        let mut dropped = table.columns.remove(idx);
        dropped.previous_names.clear();
        table.dropped_columns.push(dropped);
        table.bump_schema_version();
        if table.columns.iter().all(|c| !c.auto_increment) {
            table.auto_increment_next_value = 1;
        }
        if table
            .ttl
            .as_ref()
            .is_some_and(|ttl| ttl.column.eq_ignore_ascii_case(column_name))
        {
            table.ttl = None;
        } else {
            table.update_ttl_column_idx();
            table.ensure_column_ids();
        }
        table.update_partition_key_idx();
        Ok(())
    }

    pub fn rename_column(
        &mut self,
        table_name: &str,
        old_name: &str,
        new_name: &str,
    ) -> Result<()> {
        let table = self
            .get_table_mut(table_name)
            .ok_or_else(|| TaibaiError::Sql(format!("table '{}' does not exist", table_name)))?;
        if table.columns.iter().any(|c| c.name == new_name) {
            return Err(TaibaiError::Sql(format!(
                "column '{}' already exists in table '{}'",
                new_name, table_name
            )));
        }
        let col = table
            .columns
            .iter_mut()
            .find(|c| c.name == old_name)
            .ok_or_else(|| {
                TaibaiError::Sql(format!(
                    "column '{}' does not exist in table '{}'",
                    old_name, table_name
                ))
            })?;
        if !col
            .previous_names
            .iter()
            .any(|name| name.eq_ignore_ascii_case(&col.name))
        {
            col.previous_names.push(col.name.clone());
        }
        col.name = new_name.to_string();
        table.bump_schema_version();
        table.update_ttl_column_idx();
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
            .get_table_mut(table_name)
            .ok_or_else(|| TaibaiError::Sql(format!("table '{}' does not exist", table_name)))?;
        let col = table
            .columns
            .iter_mut()
            .find(|c| c.name == column_name)
            .ok_or_else(|| {
                TaibaiError::Sql(format!(
                    "column '{}' does not exist in table '{}'",
                    column_name, table_name
                ))
            })?;
        col.data_type = data_type;
        col.scale = scale;
        col.precision = precision;
        table.bump_schema_version();
        Ok(())
    }
}

#[cfg(test)]
#[path = "schema_test.rs"]
mod tests;
