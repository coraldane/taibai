use std::path::Path;

use rayon::prelude::*;

use crate::common::error::{MonaxError, Result};
use crate::storage::column_encoder::{encode_column_fast, EncodedColumn};
use crate::types::batch::{RecordBatch, Schema};

pub const SEGMENT_MAGIC: u32 = 0x5441_4948;
pub const SEGMENT_VERSION: u32 = 3;
pub const FOOTER_SIZE: usize = 4096;
pub const MAX_SEGMENT_SIZE: u64 = 2 * 1024 * 1024 * 1024;
pub const DEFAULT_ROW_GROUP_SIZE: usize = 50000;

#[derive(Debug, Clone)]
pub struct RowGroupMeta {
    pub offset: u64,
    pub length: u64,
    pub row_count: u64,
    pub column_sizes: Vec<u64>,
    pub compressed_sizes: Vec<u64>,
    pub uncompressed_sizes: Vec<u64>,
}

pub struct HighThroughputIngestor {
    schema: Schema,
    row_group_size: usize,
    compression_level: i32,
    pending_rows: usize,
    pending_columns: Vec<Vec<EncodedColumn>>,
    total_rows: u64,
    row_group_metas: Vec<RowGroupMeta>,
    write_buffer: Vec<u8>,
    current_offset: u64,
}

impl HighThroughputIngestor {
    pub fn new(schema: Schema) -> Self {
        Self {
            schema,
            row_group_size: DEFAULT_ROW_GROUP_SIZE,
            compression_level: 3,
            pending_rows: 0,
            pending_columns: Vec::new(),
            total_rows: 0,
            row_group_metas: Vec::new(),
            write_buffer: Vec::with_capacity(16 * 1024 * 1024),
            current_offset: 0,
        }
    }

    pub fn with_row_group_size(mut self, size: usize) -> Self {
        self.row_group_size = size;
        self
    }

    pub fn with_compression_level(mut self, level: i32) -> Self {
        self.compression_level = level;
        self
    }

    pub fn push_batch(&mut self, batch: &RecordBatch) -> Result<bool> {
        if batch.is_empty() {
            return Ok(false);
        }

        let num_cols = batch.num_columns();
        let rows = batch.num_rows();

        let encoded: Vec<EncodedColumn> = (0..num_cols)
            .into_par_iter()
            .map(|col_idx| {
                batch.column(col_idx)
                    .ok_or_else(|| MonaxError::Storage(format!("Column {} not found", col_idx)))
                    .and_then(|col| encode_column_fast(col))
            })
            .collect::<Result<Vec<_>>>()?;

        self.pending_columns.push(encoded);
        self.pending_rows += rows;

        Ok(self.pending_rows >= self.row_group_size)
    }

    pub fn compress_and_buffer(&mut self) -> Result<()> {
        if self.pending_columns.is_empty() {
            return Ok(());
        }

        let num_cols = self.schema.num_columns();
        let row_count = self.pending_rows;

        let mut column_sizes = vec![0u64; num_cols];
        let mut compressed_sizes = vec![0u64; num_cols];
        let mut uncompressed_sizes = vec![0u64; num_cols];

        for col_idx in 0..num_cols {
            let mut col_data = Vec::new();
            let mut uncompressed_total = 0usize;

            for encoded_cols in &self.pending_columns {
                if col_idx < encoded_cols.len() {
                    col_data.extend_from_slice(&encoded_cols[col_idx].data);
                    uncompressed_total += encoded_cols[col_idx].uncompressed_size;
                }
            }

            let compressed = zstd::bulk::compress(&col_data, self.compression_level)
                .map_err(|e| MonaxError::Storage(format!("Compression error: {}", e)))?;

            column_sizes[col_idx] = compressed.len() as u64;
            compressed_sizes[col_idx] = compressed.len() as u64;
            uncompressed_sizes[col_idx] = uncompressed_total as u64;

            self.write_buffer.extend_from_slice(&compressed);
        }

        let length: u64 = column_sizes.iter().sum();

        self.row_group_metas.push(RowGroupMeta {
            offset: self.current_offset,
            length,
            row_count: row_count as u64,
            column_sizes,
            compressed_sizes,
            uncompressed_sizes,
        });

        self.current_offset += length;
        self.total_rows += row_count as u64;
        self.pending_columns.clear();
        self.pending_rows = 0;

        Ok(())
    }

    pub fn write_buffer(&self) -> &[u8] {
        &self.write_buffer
    }

    pub fn write_buffer_len(&self) -> usize {
        self.write_buffer.len()
    }

    pub fn take_buffer(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.write_buffer)
    }

    pub fn total_rows(&self) -> u64 {
        self.total_rows
    }

    pub fn row_groups(&self) -> &[RowGroupMeta] {
        &self.row_group_metas
    }

    pub fn pending_rows(&self) -> usize {
        self.pending_rows
    }

    pub fn seal_to_file(self, path: &Path) -> Result<()> {
        use std::fs::File;
        use std::io::Write;

        let mut file = File::create(path)?;

        file.write_all(&self.write_buffer)?;

        let footer = self.encode_footer()?;
        file.write_all(&footer)?;

        let footer_offset = self.write_buffer.len() as u64;
        file.write_all(&footer_offset.to_le_bytes())?;

        file.sync_all()?;

        Ok(())
    }

    fn encode_footer(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::with_capacity(FOOTER_SIZE);

        buf.extend_from_slice(&SEGMENT_MAGIC.to_le_bytes());
        buf.extend_from_slice(&SEGMENT_VERSION.to_le_bytes());
        buf.extend_from_slice(&0u64.to_le_bytes());

        buf.extend_from_slice(&self.total_rows.to_le_bytes());
        buf.extend_from_slice(&(self.row_group_metas.len() as u32).to_le_bytes());

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        buf.extend_from_slice(&now.to_le_bytes());
        buf.extend_from_slice(&now.to_le_bytes());

        buf.extend_from_slice(&0u32.to_le_bytes());
        buf.extend_from_slice(&(self.schema.columns.len() as u32).to_le_bytes());
        for col in &self.schema.columns {
            buf.extend_from_slice(&(col.name.len() as u32).to_le_bytes());
            buf.extend_from_slice(col.name.as_bytes());
            buf.push(col.data_type as u8);
            buf.push(if col.nullable { 1 } else { 0 });
            buf.extend_from_slice(&(col.scale as u32).to_le_bytes());
            buf.extend_from_slice(&(col.precision as u32).to_le_bytes());
        }

        for rg in &self.row_group_metas {
            buf.extend_from_slice(&rg.offset.to_le_bytes());
            buf.extend_from_slice(&rg.length.to_le_bytes());
            buf.extend_from_slice(&rg.row_count.to_le_bytes());

            for &s in &rg.column_sizes {
                buf.extend_from_slice(&s.to_le_bytes());
            }
            for &s in &rg.compressed_sizes {
                buf.extend_from_slice(&s.to_le_bytes());
            }
            for &s in &rg.uncompressed_sizes {
                buf.extend_from_slice(&s.to_le_bytes());
            }
        }

        if buf.len() < FOOTER_SIZE {
            buf.resize(FOOTER_SIZE, 0);
        }

        Ok(buf)
    }
}