use std::path::{Path, PathBuf};
use std::sync::Mutex;

use crate::common::error::Result;
use crate::storage::segment::SegmentWriter;
use crate::storage::wal::{WalWriter, RecordType};
use crate::types::batch::{RecordBatch, Schema};

/// TableWriter: coordinates WAL + memory buffer + segment sealing.
/// Manages writes to a single table.
#[allow(dead_code)]
pub struct TableWriter {
    table_dir: PathBuf,
    schema: Schema,
    row_group_size: usize,
    wal: Mutex<WalWriter>,
    next_txn_id: Mutex<u64>,
    segment_seq: Mutex<u32>,
}

impl TableWriter {
    /// Create a new TableWriter for the given table directory.
    pub fn new(table_dir: &Path) -> Result<Self> {
        let schema = Schema::empty();
        let wal_dir = table_dir.join("wal");
        let wal = WalWriter::open(&wal_dir, 64 * 1024 * 1024)?;
        Ok(Self {
            table_dir: table_dir.to_path_buf(),
            schema,
            row_group_size: 8192,
            wal: Mutex::new(wal),
            next_txn_id: Mutex::new(1),
            segment_seq: Mutex::new(0),
        })
    }

    /// Create a TableWriter with a known schema.
    pub fn with_schema(table_dir: &Path, schema: Schema) -> Result<Self> {
        let wal_dir = table_dir.join("wal");
        let wal = WalWriter::open(&wal_dir, 64 * 1024 * 1024)?;
        Ok(Self {
            table_dir: table_dir.to_path_buf(),
            schema,
            row_group_size: 8192,
            wal: Mutex::new(wal),
            next_txn_id: Mutex::new(1),
            segment_seq: Mutex::new(0),
        })
    }

    /// Insert a batch: write WAL entry, then write to segment.
    pub fn insert(&self, batch: &RecordBatch) -> Result<u64> {
        let row_count = batch.num_rows() as u64;
        if row_count == 0 {
            return Ok(0);
        }

        // Serialize batch for WAL
        let wal_data = self.serialize_for_wal(batch);

        // Write to WAL (write-ahead)
        let txn_id = {
            let mut txn = self.next_txn_id.lock().unwrap();
            *txn += 1;
            *txn - 1
        };
        {
            let mut wal = self.wal.lock().unwrap();
            wal.write_batch(txn_id, &[(RecordType::Insert, wal_data.as_slice())])?;
        }

        // Write to segment
        let seg_path = self.next_segment_path()?;
        let mut writer = SegmentWriter::create(&seg_path, batch.schema.clone(), self.row_group_size)?;
        writer.write_batch(batch)?;
        writer.seal()?;

        Ok(row_count)
    }

    /// Get the next segment file path.
    fn next_segment_path(&self) -> Result<PathBuf> {
        let mut seq = self.segment_seq.lock().unwrap();
        *seq += 1;
        let filename = format!("seg_{:05}.dat", *seq);
        Ok(self.table_dir.join(filename))
    }

    /// Serialize a batch to bytes for WAL storage.
    fn serialize_for_wal(&self, batch: &RecordBatch) -> Vec<u8> {
        let mut buf = Vec::new();
        let num_cols = batch.num_columns() as u32;
        let num_rows = batch.num_rows() as u32;

        buf.extend_from_slice(&num_cols.to_le_bytes());
        buf.extend_from_slice(&num_rows.to_le_bytes());

        for col_idx in 0..batch.num_columns() {
            let col = batch.column(col_idx).unwrap();
            buf.push(col.data_type() as u8);

            let mut data = Vec::new();
            for i in 0..batch.num_rows() {
                let val = col.scalar(i);
                if val.is_null() {
                    data.push(0u8);
                } else {
                    data.push(1u8);
                    match &val {
                        crate::types::scalar::ScalarValue::Int64(Some(v)) =>
                            data.extend_from_slice(&v.to_le_bytes()),
                        crate::types::scalar::ScalarValue::Int32(Some(v)) =>
                            data.extend_from_slice(&v.to_le_bytes()),
                        crate::types::scalar::ScalarValue::Float64(Some(v)) =>
                            data.extend_from_slice(&v.to_le_bytes()),
                        crate::types::scalar::ScalarValue::VarChar(Some(v)) => {
                            data.extend_from_slice(&(v.len() as u32).to_le_bytes());
                            data.extend_from_slice(v.as_bytes());
                        }
                        _ => {}
                    }
                }
            }
            buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
            buf.extend_from_slice(&data);
        }

        buf
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::types::DataType;
    use crate::types::batch::ColumnDef;
    use crate::types::scalar::ScalarValue;
    use tempfile::TempDir;

    #[test]
    fn test_table_writer_insert() {
        let dir = TempDir::new().unwrap();
        let table_dir = dir.path().join("test_table");
        std::fs::create_dir_all(&table_dir).unwrap();

        let schema = Schema::new(vec![
            ColumnDef::new("id", DataType::Int64),
            ColumnDef::new("name", DataType::VarChar),
        ]);
        let writer = TableWriter::with_schema(&table_dir, schema.clone()).unwrap();

        let mut batch = RecordBatch::new(schema);
        batch.column_mut(0).unwrap().push(&ScalarValue::Int64(Some(1))).unwrap();
        batch.column_mut(1).unwrap().push(&ScalarValue::VarChar(Some("test".into()))).unwrap();

        let rows = writer.insert(&batch).unwrap();
        assert_eq!(rows, 1);
    }
}
