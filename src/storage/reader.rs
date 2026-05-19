use std::path::{Path, PathBuf};

use crate::common::error::Result;
use crate::storage::segment::SegmentReader;
use crate::types::batch::RecordBatch;

/// TableReader: discovers .dat files and yields RecordBatches.
/// Supports reading both regular and partitioned segment files.
pub struct TableReader {
    table_dir: PathBuf,
}

impl TableReader {
    /// Create a new TableReader for the given table directory.
    pub fn new(table_dir: &Path) -> Result<Self> {
        Ok(Self {
            table_dir: table_dir.to_path_buf(),
        })
    }

    /// Scan all segments in the table directory and return all batches.
    pub fn scan_all(&self, batch_size: usize) -> Result<Vec<RecordBatch>> {
        let segment_files = self.discover_segments()?;
        let mut all_batches = Vec::new();

        for seg_path in segment_files {
            let reader = SegmentReader::open(&seg_path)?;
            let batches = reader.scan(batch_size)?;
            all_batches.extend(batches);
        }

        Ok(all_batches)
    }

    /// Scan only segments matching the given partition keys.
    pub fn scan_partitions(&self, batch_size: usize, partition_keys: &[String]) -> Result<Vec<RecordBatch>> {
        let segment_files = self.discover_partition_segments(partition_keys)?;
        let mut all_batches = Vec::new();

        for seg_path in segment_files {
            let reader = SegmentReader::open(&seg_path)?;
            let batches = reader.scan(batch_size)?;
            all_batches.extend(batches);
        }

        Ok(all_batches)
    }

    /// Discover all segment files (regular + partitioned) in the table directory.
    pub fn discover_segments(&self) -> Result<Vec<PathBuf>> {
        if !self.table_dir.exists() {
            return Ok(Vec::new());
        }
        let mut files = Vec::new();
        for entry in std::fs::read_dir(&self.table_dir)? {
            let entry = entry?;
            let path = entry.path();
            if let Some(name) = path.file_name() {
                let name = name.to_string_lossy();
                // Regular segments: seg_XXXXX.dat
                // Partitioned segments: p{key}_XXXX.dat
                if (name.starts_with("seg_") || (name.starts_with('p') && name.contains('_')))
                    && name.ends_with(".dat")
                    && !name.starts_with("wal_")
                {
                    files.push(path);
                }
            }
        }
        files.sort();
        Ok(files)
    }

    /// Discover segment files matching specific partition keys.
    fn discover_partition_segments(&self, partition_keys: &[String]) -> Result<Vec<PathBuf>> {
        if !self.table_dir.exists() {
            return Ok(Vec::new());
        }
        let mut files = Vec::new();
        for entry in std::fs::read_dir(&self.table_dir)? {
            let entry = entry?;
            let path = entry.path();
            if let Some(name) = path.file_name() {
                let name = name.to_string_lossy();
                if let Some(key) = crate::storage::partition::parse_partition_key(&name) {
                    if partition_keys.contains(&key) {
                        files.push(path);
                    }
                }
            }
        }
        files.sort();
        Ok(files)
    }

    /// Get the total row count across all segments.
    pub fn total_rows(&self) -> Result<u64> {
        let segments = self.discover_segments()?;
        let mut total = 0u64;
        for seg_path in segments {
            let reader = SegmentReader::open(&seg_path)?;
            total += reader.row_count();
        }
        Ok(total)
    }

    /// Get the table directory path.
    pub fn table_dir(&self) -> &Path {
        &self.table_dir
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::types::DataType;
    use crate::storage::segment::SegmentWriter;
    use crate::types::batch::{ColumnDef, Schema};
    use crate::types::scalar::ScalarValue;
    use tempfile::TempDir;

    #[test]
    fn test_table_reader_empty_dir() {
        let dir = TempDir::new().unwrap();
        let table_dir = dir.path().join("empty_table");
        std::fs::create_dir_all(&table_dir).unwrap();

        let reader = TableReader::new(&table_dir).unwrap();
        let batches = reader.scan_all(4096).unwrap();
        assert!(batches.is_empty());
    }

    #[test]
    fn test_table_reader_with_data() {
        let dir = TempDir::new().unwrap();
        let table_dir = dir.path().join("test_table");
        std::fs::create_dir_all(&table_dir).unwrap();

        let schema = Schema::new(vec![
            ColumnDef::new("id", DataType::Int64),
        ]);

        // Write a segment
        let seg_path = table_dir.join("seg_00001.dat");
        let mut writer = SegmentWriter::create(&seg_path, schema.clone(), 8192).unwrap();
        let mut batch = RecordBatch::new(schema);
        batch.column_mut(0).unwrap().push(&ScalarValue::Int64(Some(42))).unwrap();
        writer.write_batch(&batch).unwrap();
        writer.seal().unwrap();

        // Read back
        let reader = TableReader::new(&table_dir).unwrap();
        let batches = reader.scan_all(4096).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);

        let total = reader.total_rows().unwrap();
        assert_eq!(total, 1);
    }
}
