use std::collections::HashSet;
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

use crate::common::error::{Result, TaibaiError};
use crate::storage::bulk_payload::DirectBulkColumn;
use crate::storage::column_encoding::encode_column_direct_range;
use crate::storage::partition::pruning::{compute_segment_zone_maps_from_row_groups, ZoneMap};
use crate::storage::segment::meta::{
    append_sidecar_footer, append_sidecar_row_groups, compact_sidecar_footer_if_needed,
    remove_sidecar_meta,
};
use crate::storage::segment::writer::VALUE_SET_MAX_DISTINCT;
use crate::storage::segment::{encode_segment_footer, RowGroupMeta};
use crate::storage::segment_manager::config::{
    bulk_compression_level, FLUSH_META_AFTER_ROW_GROUP, SIDECAR_COMPACT_MIN_BYTES,
    SIDECAR_COMPACT_MIN_RECORDS,
};
use crate::storage::segment_manager::helpers::{
    compute_value_sets_from_bulk_columns, compute_zone_map_from_column,
    compute_zone_maps_from_bulk_columns,
};
use crate::types::array::ColumnArray;
use crate::types::batch::{RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

pub(super) struct ActiveSegmentWriter {
    pub(super) file: std::io::BufWriter<std::fs::File>,
    pub(super) path: PathBuf,
    pub(super) schema: Schema,
    pub(super) row_groups: Vec<RowGroupMeta>,
    pub(super) current_size: u64,
    pub(super) total_rows: u64,
    pub(super) delete_bitmap: Vec<u8>,
    pub(super) deleted_row_count: u64,
    pub(super) buffered_batch: Option<RecordBatch>,
    pub(super) buffered_rows: usize,
    pub(super) dirty_meta: bool,
    pub(super) data_end: u64,
    pub(super) sidecar_row_groups_flushed: usize,
    pub(super) last_write_at: Instant,
    pub(super) row_group_target_rows: usize,
    /// Change-feed handle for deferred bulk/RELAXED writes; `None` for
    /// standalone/temporary writers that never feed notifications.
    pub(super) pubsub: Option<std::sync::Arc<crate::pubsub::PubSubHub>>,
    /// Row groups persisted to the data file but not yet visible (their
    /// footer has not been written). Published when the footer lands.
    pub(super) pending_publish: Vec<RecordBatch>,
}

impl ActiveSegmentWriter {
    /// Rows appended to this segment that a fresh reader cannot see yet: row
    /// groups written to the data file after the last footer publish plus the
    /// not-yet-row-grouped buffer. Reclaim paths must treat these as not
    /// persisted — dropping their hot-store mirror before the footer publish
    /// makes them invisible to every query until the next publish.
    pub(super) fn unpublished_row_count(&self) -> u64 {
        let published: u64 = self
            .row_groups
            .iter()
            .take(self.sidecar_row_groups_flushed)
            .map(|rg| rg.row_count)
            .sum();
        self.total_rows
            .saturating_sub(published)
            .saturating_add(self.buffered_rows as u64)
    }

    pub(super) fn append_batch(
        &mut self,
        batch: &RecordBatch,
        invalidate_reader: &dyn Fn(&Path),
    ) -> Result<()> {
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

        while self.buffered_rows >= self.row_group_target_rows {
            self.flush_buffer(invalidate_reader)?;
        }
        Ok(())
    }

    /// Append a batch whose buffers are no longer needed by the caller.
    ///
    /// The common SQL/bulk path hands ownership to the segment manager.  Keep
    /// that ownership all the way into the row-group buffer instead of cloning
    /// every fixed- and variable-width column first.
    pub(super) fn append_batch_owned(
        &mut self,
        batch: RecordBatch,
        invalidate_reader: &dyn Fn(&Path),
    ) -> Result<()> {
        let added = batch.num_rows();
        if batch.num_columns() != self.schema.num_columns() {
            return Err(TaibaiError::Internal(format!(
                "active segment append column count mismatch: {} != {}",
                batch.num_columns(),
                self.schema.num_columns()
            )));
        }
        if batch
            .columns
            .iter()
            .zip(&self.schema.columns)
            .any(|(column, definition)| column.data_type() != definition.data_type)
        {
            return Err(TaibaiError::Internal(
                "active segment append column type mismatch".into(),
            ));
        }
        if let Some(ref mut buffered) = self.buffered_batch {
            for (dst, src) in buffered.columns.iter_mut().zip(batch.columns) {
                dst.append_owned(src)?;
            }
            self.buffered_rows += added;
        } else {
            self.buffered_batch = Some(batch);
            self.buffered_rows = added;
        }

        while self.buffered_rows >= self.row_group_target_rows {
            self.flush_buffer(invalidate_reader)?;
        }
        Ok(())
    }

    pub(super) fn append_direct_row_group(
        &mut self,
        columns: &[DirectBulkColumn<'_>],
        compressed_columns: &[(Vec<u8>, u64)],
        row_count: u64,
        build_value_sets: bool,
        zone_map_columns: Option<&[usize]>,
    ) -> Result<()> {
        self.ensure_data_write_position()?;

        let offset = self.current_size;
        let mut column_compressed_sizes = Vec::with_capacity(compressed_columns.len());
        let mut compressed_sizes = Vec::with_capacity(compressed_columns.len());
        let mut uncompressed_sizes = Vec::with_capacity(compressed_columns.len());
        let mut total_length = 0u64;

        for (compressed, uncompressed_size) in compressed_columns {
            let n = compressed.len() as u64;
            column_compressed_sizes.push(n);
            compressed_sizes.push(n);
            uncompressed_sizes.push(*uncompressed_size);
            total_length += n;
            // `file` is already buffered. Writing each compressed column
            // directly avoids constructing and copying one row-group-sized
            // concatenation buffer.
            self.file.write_all(compressed)?;
        }

        let value_sets = if build_value_sets {
            compute_value_sets_from_bulk_columns(columns, row_count)?
        } else {
            vec![None; self.schema.num_columns()]
        };

        self.row_groups.push(RowGroupMeta {
            offset,
            length: total_length,
            row_count,
            zone_maps: self.compute_direct_row_group_zone_maps(
                columns,
                row_count,
                zone_map_columns,
            ),
            column_compressed_sizes,
            compressed_sizes,
            uncompressed_sizes,
            bloom_filters: vec![None; self.schema.num_columns()],
            value_sets,
        });
        self.current_size += total_length;
        self.total_rows += row_count;
        self.dirty_meta = true;
        self.data_end = self.current_size;
        self.last_write_at = Instant::now();

        Ok(())
    }

    fn compute_direct_row_group_zone_maps(
        &self,
        columns: &[DirectBulkColumn<'_>],
        row_count: u64,
        zone_map_columns: Option<&[usize]>,
    ) -> Vec<ZoneMap> {
        match zone_map_columns {
            None => compute_zone_maps_from_bulk_columns(columns, &self.schema, row_count),
            Some(indices) => {
                let mut zone_maps = vec![ZoneMap::default(); self.schema.num_columns()];
                if indices.is_empty() {
                    for zm in &mut zone_maps {
                        zm.row_count = row_count;
                    }
                    return zone_maps;
                }
                let selected_columns: Vec<DirectBulkColumn<'_>> = indices
                    .iter()
                    .filter_map(|&idx| columns.get(idx))
                    .map(|col| DirectBulkColumn {
                        name: col.name,
                        data_type: col.data_type,
                        scale: col.scale,
                        precision: col.precision,
                        data: col.data,
                        nulls: col.nulls,
                        variable_ranges: col.variable_ranges.clone(),
                    })
                    .collect();
                let selected_maps =
                    compute_zone_maps_from_bulk_columns(&selected_columns, &self.schema, row_count);
                for (&idx, zm) in indices.iter().zip(selected_maps) {
                    if idx < zone_maps.len() {
                        zone_maps[idx] = zm;
                    }
                }
                for zm in &mut zone_maps {
                    if zm.row_count == 0 {
                        zm.row_count = row_count;
                    }
                }
                zone_maps
            }
        }
    }

    pub(super) fn flush_buffer(&mut self, invalidate_reader: &dyn Fn(&Path)) -> Result<()> {
        use rayon::prelude::*;

        let buffered = match self.buffered_batch.take() {
            Some(b) => b,
            None => return Ok(()),
        };
        let total_buffered = self.buffered_rows;
        self.buffered_rows = 0;

        let compression_level = bulk_compression_level();
        let num_cols = buffered.num_columns();

        let rows_to_flush = self.row_group_target_rows.min(total_buffered);

        // One pass per column in parallel: encode+compress, zone map and
        // value set used to run value sets serially on the single flusher
        // thread, where the per-row ScalarValue allocations of low-cardinality
        // columns dominated the group write time.
        let outputs = (0..num_cols)
            .into_par_iter()
            .map(|col_idx| {
                let col = buffered.column(col_idx).unwrap();
                let encoded = encode_column_direct_range(col, 0, rows_to_flush);
                let orig_len = encoded.len() as u64;
                let compressed = zstd::bulk::compress(&encoded, compression_level)
                    .map_err(|e| TaibaiError::Storage(format!("zstd error: {}", e)))?;
                let zone_map = compute_zone_map_from_column(col, 0, rows_to_flush);
                let value_set = compute_value_set_from_column(col, 0, rows_to_flush);
                Ok((compressed, orig_len, zone_map, value_set))
            })
            .collect::<Result<Vec<_>>>()?;
        let mut compressed_columns = Vec::with_capacity(num_cols);
        let mut zone_maps = Vec::with_capacity(num_cols);
        let mut value_sets = Vec::with_capacity(num_cols);
        for (compressed, orig_len, zone_map, value_set) in outputs {
            compressed_columns.push((compressed, orig_len));
            zone_maps.push(zone_map);
            value_sets.push(value_set);
        }

        self.ensure_data_write_position()?;

        let offset = self.current_size;
        let mut column_compressed_sizes = Vec::with_capacity(num_cols);
        let mut compressed_sizes = Vec::with_capacity(num_cols);
        let mut uncompressed_sizes = Vec::with_capacity(num_cols);
        let mut total_length = 0u64;
        for (compressed, uncompressed_size) in &compressed_columns {
            let n = compressed.len() as u64;
            column_compressed_sizes.push(n);
            compressed_sizes.push(n);
            uncompressed_sizes.push(*uncompressed_size);
            total_length += n;
            self.file.write_all(compressed)?;
        }
        self.row_groups.push(RowGroupMeta {
            offset,
            length: total_length,
            row_count: rows_to_flush as u64,
            zone_maps,
            column_compressed_sizes,
            compressed_sizes,
            uncompressed_sizes,
            bloom_filters: vec![None; self.schema.num_columns()],
            value_sets,
        });
        self.current_size += total_length;
        self.total_rows += rows_to_flush as u64;
        self.dirty_meta = true;
        self.data_end = self.current_size;
        self.last_write_at = Instant::now();

        // Rows persisted but not yet visible: stash them until the footer
        // write makes them visible (see `publish_pending`).
        if rows_to_flush > 0 {
            if let Some(pubsub) = self.pubsub.as_ref() {
                if pubsub.has_subscribers() && self.topic().is_some() {
                    let indices: Vec<u32> = (0..rows_to_flush as u32).collect();
                    let mut cols = Vec::with_capacity(num_cols);
                    let mut ok = true;
                    for col_idx in 0..num_cols {
                        match buffered.column(col_idx) {
                            Some(col) => cols.push(col.take(&indices)),
                            None => {
                                ok = false;
                                break;
                            }
                        }
                    }
                    if ok {
                        if let Ok(batch) = RecordBatch::try_new(buffered.schema.clone(), cols) {
                            self.pending_publish.push(batch);
                        }
                    }
                }
            }
        }

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

        Ok(())
    }

    pub(super) fn flush_meta(&mut self, invalidate_reader: &dyn Fn(&Path)) -> Result<()> {
        if !self.dirty_meta {
            return Ok(());
        }

        self.file.flush()?;
        self.file.get_ref().set_len(self.data_end)?;
        self.file.seek(SeekFrom::Start(self.data_end))?;

        let footer = self.encode_footer()?;
        self.file.write_all(&footer)?;
        self.file.write_all(&self.data_end.to_le_bytes())?;
        self.file.flush()?;
        // A single sync_all is the durability barrier for both the row-group
        // bytes and the footer that publishes them; a second sync before the
        // footer doubled the per-row-group fsync cost without adding a
        // guarantee.
        self.file.get_ref().sync_all()?;

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

        self.current_size = self.data_end + footer.len() as u64 + 8;
        self.sidecar_row_groups_flushed = self.row_groups.len();
        self.dirty_meta = false;
        invalidate_reader(&self.path);

        tracing::debug!(
            path = %self.path.display(),
            rows = self.total_rows,
            row_groups = self.row_groups.len(),
            "segment meta flushed"
        );

        self.publish_pending();
        Ok(())
    }

    pub(super) fn seal(&mut self, invalidate_reader: &dyn Fn(&Path)) -> Result<()> {
        self.flush_buffer(invalidate_reader)?;
        self.flush_meta(invalidate_reader)?;
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

    pub(super) fn flush_visible_footer(&mut self, invalidate_reader: &dyn Fn(&Path)) -> Result<()> {
        if !self.dirty_meta {
            return Ok(());
        }

        self.flush_meta(invalidate_reader)
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
            &self.delete_bitmap,
            self.deleted_row_count,
            &table_name,
            &segment_zone_maps,
        ))
    }

    /// `(db, table)` derived from the table directory this segment lives in.
    fn topic(&self) -> Option<(String, String)> {
        let table_dir = self.path.parent()?;
        let db_dir = table_dir.parent()?;
        Some((
            db_dir.file_name()?.to_string_lossy().into_owned(),
            table_dir.file_name()?.to_string_lossy().into_owned(),
        ))
    }

    /// The footer above just made every pending row group visible: hand each
    /// of them to the change feed as one delta notification.
    fn publish_pending(&mut self) {
        if self.pending_publish.is_empty() {
            return;
        }
        match (self.pubsub.as_ref(), self.topic()) {
            (Some(pubsub), Some((db, table))) => {
                for batch in self.pending_publish.drain(..) {
                    pubsub.publish_shared(&db, &table, std::sync::Arc::new(batch));
                }
            }
            _ => self.pending_publish.clear(),
        }
    }

    fn table_name(&self) -> String {
        self.path
            .parent()
            .and_then(|p| p.file_name())
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_default()
    }
}

/// Distinct values of one column up to `VALUE_SET_MAX_DISTINCT`, tracked on
/// raw typed keys instead of a `ScalarValue` per row: the allocation-free
/// probe keeps exact semantics (same null skip, same distinctness, same cap,
/// same final sort) at a fraction of the cost for low-cardinality columns
/// such as `deal_date`/`deal_time`, which previously had to walk every row.
fn compute_value_set_from_column(
    column: &ColumnArray,
    start_row: usize,
    row_count: usize,
) -> Option<Vec<ScalarValue>> {
    let end_row = start_row + row_count;
    let bitmap = column.bitmap();

    macro_rules! typed_value_set {
        ($values:expr, $scalar:ident) => {{
            let mut keys = HashSet::new();
            for (row, &value) in $values.iter().enumerate().take(end_row).skip(start_row) {
                if bitmap.is_null(row) {
                    continue;
                }
                keys.insert(value);
                if keys.len() > VALUE_SET_MAX_DISTINCT {
                    return None;
                }
            }
            let mut scalars: Vec<ScalarValue> =
                keys.into_iter().map(|value| ScalarValue::$scalar(Some(value))).collect();
            scalars.sort_by(crate::storage::index::sort_key::compare_scalars_for_sort);
            return Some(scalars);
        }};
    }

    match column {
        ColumnArray::Bool { data, .. } => {
            let mut keys = HashSet::new();
            for (row, &raw) in data.iter().enumerate().take(end_row).skip(start_row) {
                if bitmap.is_null(row) {
                    continue;
                }
                keys.insert(raw != 0);
                if keys.len() > VALUE_SET_MAX_DISTINCT {
                    return None;
                }
            }
            let mut scalars: Vec<ScalarValue> =
                keys.into_iter().map(|value| ScalarValue::Bool(Some(value))).collect();
            scalars.sort_by(crate::storage::index::sort_key::compare_scalars_for_sort);
            Some(scalars)
        }
        ColumnArray::Int8 { data, .. } => typed_value_set!(data, Int8),
        ColumnArray::Int16 { data, .. } => typed_value_set!(data, Int16),
        ColumnArray::Int32 { data, .. } => typed_value_set!(data, Int32),
        ColumnArray::Int64 { data, .. } => typed_value_set!(data, Int64),
        ColumnArray::UInt8 { data, .. } => typed_value_set!(data, UInt8),
        ColumnArray::UInt16 { data, .. } => typed_value_set!(data, UInt16),
        ColumnArray::UInt32 { data, .. } => typed_value_set!(data, UInt32),
        ColumnArray::UInt64 { data, .. } => typed_value_set!(data, UInt64),
        // +0.0 folds -0.0 onto 0.0 so float keys match f64 equality (the
        // scalar-hash semantics they replace); every other value is unchanged.
        ColumnArray::Float32 { data, .. } => {
            // +0.0 folds -0.0 onto 0.0 (f64/f32 equality treats them as one
            // value); bits give the key Hash+Eq.
            let mut keys = HashSet::new();
            for (row, &value) in data.iter().enumerate().take(end_row).skip(start_row) {
                if bitmap.is_null(row) {
                    continue;
                }
                keys.insert((value + 0.0).to_bits());
                if keys.len() > VALUE_SET_MAX_DISTINCT {
                    return None;
                }
            }
            let mut scalars: Vec<ScalarValue> = keys
                .into_iter()
                .map(|bits| ScalarValue::Float32(Some(f32::from_bits(bits))))
                .collect();
            scalars.sort_by(crate::storage::index::sort_key::compare_scalars_for_sort);
            Some(scalars)
        }
        ColumnArray::Float64 { data, .. } => {
            let mut keys = HashSet::new();
            for (row, &value) in data.iter().enumerate().take(end_row).skip(start_row) {
                if bitmap.is_null(row) {
                    continue;
                }
                keys.insert((value + 0.0).to_bits());
                if keys.len() > VALUE_SET_MAX_DISTINCT {
                    return None;
                }
            }
            let mut scalars: Vec<ScalarValue> = keys
                .into_iter()
                .map(|bits| ScalarValue::Float64(Some(f64::from_bits(bits))))
                .collect();
            scalars.sort_by(crate::storage::index::sort_key::compare_scalars_for_sort);
            Some(scalars)
        }
        ColumnArray::Decimal { data, .. } => typed_value_set!(data, Decimal),
        ColumnArray::Date { data, .. } => typed_value_set!(data, Date),
        ColumnArray::Time { data, .. } => typed_value_set!(data, Time),
        ColumnArray::Timestamp { data, .. } => typed_value_set!(data, Timestamp),
        ColumnArray::Interval { data, .. } => typed_value_set!(data, Interval),
        ColumnArray::VarChar { data, .. } => {
            let mut keys = HashSet::new();
            for row in start_row..end_row {
                if bitmap.is_null(row) {
                    continue;
                }
                keys.insert(data.value(row));
                if keys.len() > VALUE_SET_MAX_DISTINCT {
                    return None;
                }
            }
            let mut scalars: Vec<ScalarValue> = keys
                .into_iter()
                .map(|value| ScalarValue::VarChar(Some(value.to_string())))
                .collect();
            scalars.sort_by(crate::storage::index::sort_key::compare_scalars_for_sort);
            Some(scalars)
        }
        _ => {
            let mut values = HashSet::new();
            let mut complete = true;
            for row_idx in start_row..end_row {
                let value = column.scalar(row_idx);
                if value.is_null() {
                    continue;
                }
                values.insert(value);
                if values.len() > VALUE_SET_MAX_DISTINCT {
                    complete = false;
                    break;
                }
            }
            if complete {
                let mut values: Vec<ScalarValue> = values.into_iter().collect();
                values.sort_by(crate::storage::index::sort_key::compare_scalars_for_sort);
                Some(values)
            } else {
                None
            }
        }
    }
}

#[cfg(test)]
#[path = "owned_append_test.rs"]
mod owned_append_tests;
