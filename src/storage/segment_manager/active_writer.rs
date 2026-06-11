use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

use crate::common::error::{Result, TaibaiError};
use crate::storage::bulk_payload::DirectBulkColumn;
use crate::storage::column_encoding::encode_column_direct_range;
use crate::storage::partition::pruning::compute_segment_zone_maps_from_row_groups;
use crate::storage::segment::meta::{
    append_sidecar_footer, append_sidecar_row_groups, compact_sidecar_footer_if_needed,
    remove_sidecar_meta,
};
use crate::storage::segment::{encode_segment_footer, RowGroupMeta};
use crate::storage::segment_manager::config::{
    bulk_compression_level, FLUSH_META_AFTER_ROW_GROUP, SIDECAR_COMPACT_MIN_BYTES,
    SIDECAR_COMPACT_MIN_RECORDS, SQL_INSERT_ROW_GROUP_TARGET_ROWS,
};
use crate::storage::segment_manager::helpers::{
    compute_zone_maps_from_batch, compute_zone_maps_from_bulk_columns,
};
use crate::types::batch::{RecordBatch, Schema};

pub(super) struct ActiveSegmentWriter {
    pub(super) file: std::io::BufWriter<std::fs::File>,
    pub(super) path: PathBuf,
    pub(super) schema: Schema,
    pub(super) row_groups: Vec<RowGroupMeta>,
    pub(super) current_size: u64,
    pub(super) total_rows: u64,
    pub(super) buffered_batch: Option<RecordBatch>,
    pub(super) buffered_rows: usize,
    pub(super) dirty_meta: bool,
    pub(super) data_end: u64,
    pub(super) sidecar_row_groups_flushed: usize,
    pub(super) last_write_at: Instant,
}

impl ActiveSegmentWriter {
    pub(super) fn append_batch(
        &mut self,
        batch: &RecordBatch,
        invalidate_reader: &dyn Fn(&Path),
    ) -> Result<Vec<String>> {
        let mut wal_keys_to_clear = Vec::new();
        if let Some(ref mut buffered) = self.buffered_batch {
            for col_idx in 0..batch.num_columns() {
                let src = batch.column(col_idx).unwrap();
                let dst = buffered.column_mut(col_idx).unwrap();
                dst.extend(src)?;
            }
            self.buffered_rows += batch.num_rows();
        } else {
            self.buffered_batch = Some(batch.clone());
            self.buffered_rows = batch.num_rows();
        }

        while self.buffered_rows >= SQL_INSERT_ROW_GROUP_TARGET_ROWS {
            if let Some(wk) = self.flush_buffer(invalidate_reader)? {
                wal_keys_to_clear.push(wk);
            }
        }
        Ok(wal_keys_to_clear)
    }

    pub(super) fn append_direct_row_group(
        &mut self,
        columns: &[DirectBulkColumn<'_>],
        compressed_columns: &[(Vec<u8>, u64)],
        row_count: u64,
    ) -> Result<()> {
        self.ensure_data_write_position()?;

        let offset = self.current_size;
        let mut column_compressed_sizes = Vec::with_capacity(compressed_columns.len());
        let mut compressed_sizes = Vec::with_capacity(compressed_columns.len());
        let mut uncompressed_sizes = Vec::with_capacity(compressed_columns.len());
        let mut total_length = 0u64;
        let mut write_buf = Vec::new();

        for (compressed, uncompressed_size) in compressed_columns {
            let n = compressed.len() as u64;
            column_compressed_sizes.push(n);
            compressed_sizes.push(n);
            uncompressed_sizes.push(*uncompressed_size);
            total_length += n;
            write_buf.extend_from_slice(compressed);
        }
        self.file.write_all(&write_buf)?;

        self.row_groups.push(RowGroupMeta {
            offset,
            length: total_length,
            row_count,
            zone_maps: compute_zone_maps_from_bulk_columns(columns, &self.schema, row_count),
            column_compressed_sizes,
            compressed_sizes,
            uncompressed_sizes,
            bloom_filters: vec![None; self.schema.num_columns()],
        });
        self.current_size += total_length;
        self.total_rows += row_count;
        self.dirty_meta = true;
        self.data_end = self.current_size;
        self.last_write_at = Instant::now();

        Ok(())
    }

    pub(super) fn flush_buffer(
        &mut self,
        invalidate_reader: &dyn Fn(&Path),
    ) -> Result<Option<String>> {
        use rayon::prelude::*;

        let buffered = match self.buffered_batch.take() {
            Some(b) => b,
            None => return Ok(None),
        };
        let total_buffered = self.buffered_rows;
        self.buffered_rows = 0;

        let compression_level = bulk_compression_level();
        let num_cols = buffered.num_columns();

        let rows_to_flush = SQL_INSERT_ROW_GROUP_TARGET_ROWS.min(total_buffered);

        let compressed_columns: Vec<(Vec<u8>, u64)> = (0..num_cols)
            .into_par_iter()
            .map(|col_idx| {
                let col = buffered.column(col_idx).unwrap();
                let encoded = encode_column_direct_range(col, 0, rows_to_flush);
                let orig_len = encoded.len() as u64;
                let compressed = zstd::bulk::compress(&encoded, compression_level)
                    .map_err(|e| TaibaiError::Storage(format!("zstd error: {}", e)))?;
                Ok((compressed, orig_len))
            })
            .collect::<Result<Vec<_>>>()?;

        self.ensure_data_write_position()?;

        let offset = self.current_size;
        let mut column_compressed_sizes = Vec::with_capacity(num_cols);
        let mut compressed_sizes = Vec::with_capacity(num_cols);
        let mut uncompressed_sizes = Vec::with_capacity(num_cols);
        let mut total_length = 0u64;
        let mut write_buf = Vec::new();
        for (compressed, uncompressed_size) in &compressed_columns {
            let n = compressed.len() as u64;
            column_compressed_sizes.push(n);
            compressed_sizes.push(n);
            uncompressed_sizes.push(*uncompressed_size);
            total_length += n;
            write_buf.extend_from_slice(compressed);
        }
        self.file.write_all(&write_buf)?;
        self.row_groups.push(RowGroupMeta {
            offset,
            length: total_length,
            row_count: rows_to_flush as u64,
            zone_maps: compute_zone_maps_from_batch(&buffered, 0, rows_to_flush),
            column_compressed_sizes,
            compressed_sizes,
            uncompressed_sizes,
            bloom_filters: vec![None; self.schema.num_columns()],
        });
        self.current_size += total_length;
        self.total_rows += rows_to_flush as u64;
        self.dirty_meta = true;
        self.data_end = self.current_size;
        self.last_write_at = Instant::now();

        if FLUSH_META_AFTER_ROW_GROUP {
            self.flush_meta(invalidate_reader)?;
        }

        let remaining = total_buffered - rows_to_flush;
        if remaining > 0 {
            let mut remainder = RecordBatch::new(buffered.schema.clone());
            for col_idx in 0..num_cols {
                let col = buffered.column(col_idx).unwrap();
                let dst = remainder.column_mut(col_idx).unwrap();
                for row_idx in rows_to_flush..total_buffered {
                    let val = col.scalar(row_idx);
                    dst.push(&val)?;
                }
            }
            self.buffered_batch = Some(remainder);
            self.buffered_rows = remaining;
        }

        Ok(Some(self.path.to_string_lossy().to_string()))
    }

    pub(super) fn flush_meta(&mut self, invalidate_reader: &dyn Fn(&Path)) -> Result<()> {
        if !self.dirty_meta {
            return Ok(());
        }

        self.file.flush()?;
        self.file.get_ref().set_len(self.data_end)?;
        self.file.seek(SeekFrom::Start(self.data_end))?;
        self.file.get_ref().sync_all()?;

        let footer = self.encode_footer()?;
        if self.sidecar_row_groups_flushed == 0
            || self.sidecar_row_groups_flushed > self.row_groups.len()
        {
            append_sidecar_footer(&self.path, self.data_end, &footer)?;
        } else if self.sidecar_row_groups_flushed < self.row_groups.len() {
            let table_name = self.table_name();
            append_sidecar_row_groups(
                &self.path,
                self.data_end,
                &self.schema,
                self.total_rows,
                &self.row_groups[self.sidecar_row_groups_flushed..],
                &table_name,
            )?;
        } else {
            append_sidecar_footer(&self.path, self.data_end, &footer)?;
        }
        if compact_sidecar_footer_if_needed(
            &self.path,
            self.data_end,
            &footer,
            SIDECAR_COMPACT_MIN_RECORDS,
            SIDECAR_COMPACT_MIN_BYTES,
        )? {
            tracing::debug!(
                path = %self.path.display(),
                rows = self.total_rows,
                row_groups = self.row_groups.len(),
                "segment sidecar meta compacted"
            );
        }

        self.current_size = self.data_end;
        self.sidecar_row_groups_flushed = self.row_groups.len();
        self.dirty_meta = false;
        invalidate_reader(&self.path);

        tracing::debug!(
            path = %self.path.display(),
            rows = self.total_rows,
            row_groups = self.row_groups.len(),
            "segment meta flushed"
        );

        Ok(())
    }

    pub(super) fn seal(&mut self, invalidate_reader: &dyn Fn(&Path)) -> Result<()> {
        self.flush_buffer(invalidate_reader)?;
        self.write_final_footer()?;
        self.finalize()?;
        remove_sidecar_meta(&self.path)?;
        invalidate_reader(&self.path);

        tracing::info!(
            path = %self.path.display(),
            size_mb = self.current_size / (1024 * 1024),
            rows = self.total_rows,
            row_groups = self.row_groups.len(),
            "segment sealed"
        );

        Ok(())
    }

    pub(super) fn write_final_footer(&mut self) -> Result<()> {
        self.file.flush()?;
        self.file.get_ref().set_len(self.data_end)?;
        self.file.seek(SeekFrom::Start(self.data_end))?;

        let footer = self.encode_footer()?;
        self.file.write_all(&footer)?;
        self.file.write_all(&self.data_end.to_le_bytes())?;
        self.file.flush()?;
        self.file.get_ref().sync_all()?;

        self.current_size = self.data_end + footer.len() as u64 + 8;
        self.sidecar_row_groups_flushed = self.row_groups.len();
        self.dirty_meta = false;
        Ok(())
    }

    pub(super) fn finalize(&mut self) -> Result<()> {
        self.file.flush()?;
        self.file.get_ref().sync_all()?;

        tracing::info!(
            path = %self.path.display(),
            size_mb = self.current_size / (1024 * 1024),
            rows = self.total_rows,
            row_groups = self.row_groups.len(),
            "segment finalized and closed"
        );

        Ok(())
    }

    fn ensure_data_write_position(&mut self) -> Result<()> {
        if self.current_size != self.data_end {
            self.file.get_ref().set_len(self.data_end)?;
            self.file.seek(SeekFrom::Start(self.data_end))?;
            self.current_size = self.data_end;
        }
        Ok(())
    }

    fn encode_footer(&self) -> Result<Vec<u8>> {
        let table_name = self.table_name();
        let segment_zone_maps =
            compute_segment_zone_maps_from_row_groups(&self.row_groups, self.schema.num_columns());
        Ok(encode_segment_footer(
            &self.schema,
            self.total_rows,
            &self.row_groups,
            &[],
            0,
            &table_name,
            &segment_zone_maps,
        ))
    }

    fn table_name(&self) -> String {
        self.path
            .parent()
            .and_then(|p| p.file_name())
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_default()
    }
}
