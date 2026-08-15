use std::collections::HashSet;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use rayon::prelude::*;

use crate::common::error::{Result, TaibaiError};
use crate::storage::codec::column_codec::encode_column;
use crate::storage::codec::io_util::encode_len_prefixed_str;
use crate::storage::codec::scalar_tag::{encode_scalar_with_tag, null_for_dtype};
use crate::storage::index::bloomfilter::BloomFilter;
use crate::storage::partition::pruning::ZoneMap;
use crate::types::array::ColumnArray;
use crate::types::batch::Schema;
use crate::types::scalar::ScalarValue;

use super::constants::{FOOTER_SIZE, SEGMENT_HEADER_SIZE, SEGMENT_MAGIC, SEGMENT_VERSION};
use super::meta::remove_sidecar_meta;

pub(crate) const VALUE_SET_EXTENSION_MAGIC: u32 = 0x5354_4958; // STIX
pub(crate) const VALUE_SET_MAX_DISTINCT: usize = 1024;

struct BuildingRowGroup {
    columns: Vec<Vec<u8>>,
    row_count: usize,
    uncompressed_sizes: Vec<u64>,
    zone_mins: Vec<Option<ScalarValue>>,
    zone_maxs: Vec<Option<ScalarValue>>,
    null_counts: Vec<u64>,
    bloom_filters: Vec<Option<BloomFilter>>,
    value_sets: Vec<Option<HashSet<ScalarValue>>>,
}

struct ColumnMetadata {
    min_value: Option<ScalarValue>,
    max_value: Option<ScalarValue>,
    null_count: u64,
    bloom_filter: Option<BloomFilter>,
    value_set: Option<Vec<ScalarValue>>,
}

impl BuildingRowGroup {
    fn new(num_columns: usize, bloom_columns: &[bool]) -> Self {
        Self {
            columns: vec![Vec::new(); num_columns],
            row_count: 0,
            uncompressed_sizes: vec![0; num_columns],
            zone_mins: vec![None; num_columns],
            zone_maxs: vec![None; num_columns],
            null_counts: vec![0; num_columns],
            bloom_filters: bloom_columns
                .iter()
                .map(|enabled| enabled.then(BloomFilter::with_defaults))
                .collect(),
            value_sets: vec![Some(HashSet::new()); num_columns],
        }
    }
}

pub struct SegmentWriter {
    path: PathBuf,
    file: fs::File,
    schema: Schema,
    row_group_size: usize,
    row_groups: Vec<crate::storage::segment::RowGroupMeta>,
    current_group: Option<BuildingRowGroup>,
    total_rows: u64,
    bloom_columns: Vec<bool>,
}

impl SegmentWriter {
    pub fn create(path: &Path, schema: Schema, row_group_size: usize) -> Result<Self> {
        let bloom_columns: Vec<usize> = (0..schema.num_columns()).collect();
        Self::create_with_bloom_columns(path, schema, row_group_size, &bloom_columns)
    }

    pub fn create_with_bloom_columns(
        path: &Path,
        schema: Schema,
        row_group_size: usize,
        bloom_column_indices: &[usize],
    ) -> Result<Self> {
        remove_sidecar_meta(path)?;
        let mut file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;

        let header = encode_segment_header();
        file.write_all(&header)?;
        let mut bloom_columns = vec![false; schema.num_columns()];
        for &idx in bloom_column_indices {
            if idx < bloom_columns.len() {
                bloom_columns[idx] = true;
            }
        }

        Ok(Self {
            path: path.to_path_buf(),
            file,
            schema,
            row_group_size: row_group_size
                .clamp(1, crate::storage::segment::constants::MAX_COLUMN_PAGE_ROWS),
            row_groups: Vec::new(),
            current_group: None,
            total_rows: 0,
            bloom_columns,
        })
    }

    pub fn write_batch(&mut self, batch: &crate::types::batch::RecordBatch) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        let num_cols = self.schema.num_columns();
        if self.current_group.is_none() {
            self.current_group = Some(BuildingRowGroup::new(num_cols, &self.bloom_columns));
        }

        let mut remaining = batch.num_rows();
        let mut offset = 0;

        while remaining > 0 {
            if self.current_group.is_none() {
                self.current_group = Some(BuildingRowGroup::new(num_cols, &self.bloom_columns));
            }
            let group = self.current_group.as_mut().unwrap();
            let can_fit = self.row_group_size - group.row_count;
            if can_fit == 0 {
                self.flush_row_group()?;
                self.current_group = Some(BuildingRowGroup::new(num_cols, &self.bloom_columns));
            }
            let group = self.current_group.as_mut().unwrap();
            let can_fit = self.row_group_size - group.row_count;
            let to_write = remaining.min(can_fit);

            for col_idx in 0..num_cols {
                let col = batch.column(col_idx).unwrap();
                let metadata =
                    build_column_metadata(col, offset, to_write, self.bloom_columns[col_idx]);
                merge_column_metadata(group, col_idx, metadata);

                let encoded = encode_column(col, offset, to_write)?;
                let uncompressed_len = encoded.len() as u64;

                let compressed = zstd::bulk::compress(
                    &encoded,
                    crate::storage::segment_manager::bulk_compression_level(),
                )
                .map_err(|e| TaibaiError::Storage(format!("zstd compress error: {}", e)))?;

                group.uncompressed_sizes[col_idx] += uncompressed_len;
                if group.columns[col_idx].is_empty() {
                    group.columns[col_idx] = compressed;
                } else {
                    group.columns[col_idx].extend_from_slice(&compressed);
                }
            }

            group.row_count += to_write;
            offset += to_write;
            remaining -= to_write;

            if group.row_count >= self.row_group_size {
                self.flush_row_group()?;
            }
        }

        self.total_rows += batch.num_rows() as u64;
        Ok(())
    }

    /// Write complete row groups while encoding and compressing their columns
    /// in parallel. Compaction produces row-group-aligned batches and can use
    /// this path without the incremental buffering required by normal inserts.
    pub fn write_batch_parallel(&mut self, batch: &crate::types::batch::RecordBatch) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        if self.current_group.is_some() {
            self.flush_row_group()?;
        }

        let mut offset = 0usize;
        while offset < batch.num_rows() {
            let row_count = (batch.num_rows() - offset).min(self.row_group_size);
            self.write_complete_row_group_parallel(batch, offset, row_count)?;
            offset += row_count;
        }
        self.total_rows += batch.num_rows() as u64;
        Ok(())
    }

    fn write_complete_row_group_parallel(
        &mut self,
        batch: &crate::types::batch::RecordBatch,
        row_offset: usize,
        row_count: usize,
    ) -> Result<()> {
        struct EncodedColumn {
            compressed: Vec<u8>,
            uncompressed_size: u64,
            zone_map: ZoneMap,
            bloom_filter: Option<BloomFilter>,
            value_set: Option<Vec<ScalarValue>>,
        }

        let encoded_columns = (0..self.schema.num_columns())
            .into_par_iter()
            .map(|col_idx| -> Result<EncodedColumn> {
                let col = batch.column(col_idx).ok_or_else(|| {
                    TaibaiError::Storage(format!("missing column {} in write batch", col_idx))
                })?;
                let dtype = self.schema.columns[col_idx].data_type;
                let metadata =
                    build_column_metadata(col, row_offset, row_count, self.bloom_columns[col_idx]);

                let encoded = encode_column(col, row_offset, row_count)?;
                let uncompressed_size = encoded.len() as u64;
                let compressed = zstd::bulk::compress(
                    &encoded,
                    crate::storage::segment_manager::bulk_compression_level(),
                )
                .map_err(|e| TaibaiError::Storage(format!("zstd compress error: {}", e)))?;
                let value_set = metadata.value_set.map(|mut values| {
                    values.sort_by(crate::storage::index::sort_key::compare_scalars_for_sort);
                    values
                });

                Ok(EncodedColumn {
                    compressed,
                    uncompressed_size,
                    zone_map: ZoneMap {
                        min_value: metadata.min_value.unwrap_or_else(|| null_for_dtype(dtype)),
                        max_value: metadata.max_value.unwrap_or_else(|| null_for_dtype(dtype)),
                        null_count: metadata.null_count,
                        row_count: row_count as u64,
                    },
                    bloom_filter: metadata.bloom_filter.filter(|filter| !filter.is_empty()),
                    value_set,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let file_offset = self.file.metadata()?.len();
        let mut compressed_sizes = Vec::with_capacity(encoded_columns.len());
        let mut uncompressed_sizes = Vec::with_capacity(encoded_columns.len());
        let mut zone_maps = Vec::with_capacity(encoded_columns.len());
        let mut bloom_filters = Vec::with_capacity(encoded_columns.len());
        let mut value_sets = Vec::with_capacity(encoded_columns.len());
        for encoded in encoded_columns {
            let compressed_size = encoded.compressed.len() as u64;
            self.file.write_all(&encoded.compressed)?;
            compressed_sizes.push(compressed_size);
            uncompressed_sizes.push(encoded.uncompressed_size);
            zone_maps.push(encoded.zone_map);
            bloom_filters.push(encoded.bloom_filter);
            value_sets.push(encoded.value_set);
        }

        self.row_groups.push(crate::storage::segment::RowGroupMeta {
            offset: file_offset,
            length: compressed_sizes.iter().sum(),
            row_count: row_count as u64,
            zone_maps,
            column_compressed_sizes: compressed_sizes.clone(),
            compressed_sizes,
            uncompressed_sizes,
            bloom_filters,
            value_sets,
        });
        Ok(())
    }

    pub fn seal(self) -> Result<()> {
        self.seal_with_deletes(&[], 0)
    }

    pub fn seal_with_deletes(mut self, delete_bitmap: &[u8], deleted_row_count: u64) -> Result<()> {
        if self.current_group.is_some() {
            self.flush_row_group()?;
        }

        self.file.flush()?;
        let data_end = self.file.metadata()?.len();
        let footer = self.encode_footer_with_deletes(delete_bitmap, deleted_row_count)?;
        self.file.write_all(&footer)?;
        self.file.write_all(&data_end.to_le_bytes())?;
        self.file.flush()?;
        self.file.sync_all()?;
        remove_sidecar_meta(&self.path)?;

        tracing::debug!(
            path = %self.path.display(),
            rows = self.total_rows,
            row_groups = self.row_groups.len(),
            deleted = deleted_row_count,
            "segment sealed"
        );

        Ok(())
    }

    fn flush_row_group(&mut self) -> Result<()> {
        let group = self.current_group.take().unwrap();
        let offset = self.file.metadata()?.len();

        let mut compressed_sizes = Vec::with_capacity(group.columns.len());
        let mut column_compressed_sizes = Vec::with_capacity(group.columns.len());

        for col_data in &group.columns {
            let n = col_data.len() as u64;
            compressed_sizes.push(n);
            column_compressed_sizes.push(n);
            self.file.write_all(col_data)?;
        }

        let num_cols = self.schema.num_columns();
        let mut zone_maps = Vec::with_capacity(num_cols);
        for col_idx in 0..num_cols {
            let dtype = self.schema.columns[col_idx].data_type;
            let min_v = group.zone_mins[col_idx]
                .clone()
                .unwrap_or_else(|| null_for_dtype(dtype));
            let max_v = group.zone_maxs[col_idx]
                .clone()
                .unwrap_or_else(|| null_for_dtype(dtype));
            zone_maps.push(ZoneMap {
                min_value: min_v,
                max_value: max_v,
                null_count: group.null_counts[col_idx],
                row_count: group.row_count as u64,
            });
        }

        let bloom_filters: Vec<Option<BloomFilter>> = group
            .bloom_filters
            .into_iter()
            .map(|bf| bf.filter(|b| !b.is_empty()))
            .collect();
        let value_sets: Vec<Option<Vec<ScalarValue>>> = group
            .value_sets
            .into_iter()
            .map(|values| {
                let mut values: Vec<ScalarValue> = values?.into_iter().collect();
                values.sort_by(crate::storage::index::sort_key::compare_scalars_for_sort);
                Some(values)
            })
            .collect();

        self.row_groups.push(crate::storage::segment::RowGroupMeta {
            offset,
            length: compressed_sizes.iter().sum(),
            row_count: group.row_count as u64,
            column_compressed_sizes,
            compressed_sizes,
            uncompressed_sizes: group.uncompressed_sizes,
            zone_maps,
            bloom_filters,
            value_sets,
        });

        Ok(())
    }

    fn encode_footer_with_deletes(
        &self,
        delete_bitmap: &[u8],
        deleted_row_count: u64,
    ) -> Result<Vec<u8>> {
        let segment_zone_maps =
            compute_segment_zone_maps(&self.row_groups, self.schema.num_columns());

        Ok(encode_segment_footer(
            &self.schema,
            self.total_rows,
            &self.row_groups,
            delete_bitmap,
            deleted_row_count,
            "",
            &segment_zone_maps,
        ))
    }
}

pub fn encode_segment_header() -> [u8; SEGMENT_HEADER_SIZE] {
    let mut buf = [0u8; SEGMENT_HEADER_SIZE];
    buf[0..4].copy_from_slice(&SEGMENT_MAGIC.to_le_bytes());
    buf[4..8].copy_from_slice(&SEGMENT_VERSION.to_le_bytes());
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    buf[8..16].copy_from_slice(&now.to_le_bytes());
    buf[16..24].copy_from_slice(&0u64.to_le_bytes());
    buf
}

pub fn encode_segment_footer(
    schema: &Schema,
    total_rows: u64,
    row_groups: &[crate::storage::segment::RowGroupMeta],
    delete_bitmap: &[u8],
    deleted_row_count: u64,
    table_name: &str,
    segment_zone_maps: &[ZoneMap],
) -> Vec<u8> {
    let mut buf = Vec::with_capacity(FOOTER_SIZE);

    buf.extend_from_slice(&SEGMENT_MAGIC.to_le_bytes());
    buf.extend_from_slice(&SEGMENT_VERSION.to_le_bytes());
    buf.extend_from_slice(&0u64.to_le_bytes());

    buf.extend_from_slice(&total_rows.to_le_bytes());

    buf.extend_from_slice(&(row_groups.len() as u32).to_le_bytes());

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    buf.extend_from_slice(&now.to_le_bytes());
    buf.extend_from_slice(&now.to_le_bytes());

    encode_len_prefixed_str(&mut buf, table_name);
    buf.extend_from_slice(&(schema.columns.len() as u32).to_le_bytes());
    for col in &schema.columns {
        encode_len_prefixed_str(&mut buf, &col.name);
        buf.push(col.data_type as u8);
        buf.push(if col.nullable { 1 } else { 0 });
        buf.push(col.scale as u8);
        buf.push(col.precision as u8);
        buf.extend_from_slice(&col.column_id.to_le_bytes());
    }

    if !segment_zone_maps.is_empty() {
        buf.push(1);
        buf.extend_from_slice(&(segment_zone_maps.len() as u32).to_le_bytes());
        for zm in segment_zone_maps {
            encode_scalar_with_tag(&mut buf, &zm.min_value);
            encode_scalar_with_tag(&mut buf, &zm.max_value);
            buf.extend_from_slice(&zm.null_count.to_le_bytes());
            buf.extend_from_slice(&zm.row_count.to_le_bytes());
        }
    } else {
        buf.push(0);
    }

    for rg in row_groups {
        buf.extend_from_slice(&rg.offset.to_le_bytes());
        buf.extend_from_slice(&rg.length.to_le_bytes());
        buf.extend_from_slice(&rg.row_count.to_le_bytes());

        for &s in &rg.column_compressed_sizes {
            buf.extend_from_slice(&s.to_le_bytes());
        }
        for &s in &rg.compressed_sizes {
            buf.extend_from_slice(&s.to_le_bytes());
        }
        for &s in &rg.uncompressed_sizes {
            buf.extend_from_slice(&s.to_le_bytes());
        }
        for zm in &rg.zone_maps {
            encode_scalar_with_tag(&mut buf, &zm.min_value);
            encode_scalar_with_tag(&mut buf, &zm.max_value);
            buf.extend_from_slice(&zm.null_count.to_le_bytes());
            buf.extend_from_slice(&zm.row_count.to_le_bytes());
        }

        for bf in &rg.bloom_filters {
            if let Some(b) = bf {
                buf.push(1);
                let bf_data = b.serialize();
                buf.extend_from_slice(&(bf_data.len() as u32).to_le_bytes());
                buf.extend_from_slice(&bf_data);
            } else {
                buf.push(0);
            }
        }

        buf.extend_from_slice(&VALUE_SET_EXTENSION_MAGIC.to_le_bytes());
        for value_set in &rg.value_sets {
            if let Some(values) = value_set {
                buf.push(1);
                buf.extend_from_slice(&(values.len() as u32).to_le_bytes());
                for value in values {
                    encode_scalar_with_tag(&mut buf, value);
                }
            } else {
                buf.push(0);
            }
        }
    }

    buf.extend_from_slice(&deleted_row_count.to_le_bytes());
    buf.extend_from_slice(&(delete_bitmap.len() as u32).to_le_bytes());
    if !delete_bitmap.is_empty() {
        buf.extend_from_slice(delete_bitmap);
    }

    buf
}

fn merge_column_metadata(group: &mut BuildingRowGroup, col_idx: usize, metadata: ColumnMetadata) {
    merge_zone_bound(&mut group.zone_mins[col_idx], metadata.min_value, true);
    merge_zone_bound(&mut group.zone_maxs[col_idx], metadata.max_value, false);
    group.null_counts[col_idx] += metadata.null_count;

    if let (Some(destination), Some(source)) = (
        &mut group.bloom_filters[col_idx],
        metadata.bloom_filter.as_ref(),
    ) {
        destination.merge(source);
    }

    if let Some(destination) = &mut group.value_sets[col_idx] {
        match metadata.value_set {
            Some(values) => {
                destination.extend(values);
                if destination.len() > VALUE_SET_MAX_DISTINCT {
                    group.value_sets[col_idx] = None;
                }
            }
            None => group.value_sets[col_idx] = None,
        }
    }
}

fn merge_zone_bound(
    destination: &mut Option<ScalarValue>,
    candidate: Option<ScalarValue>,
    keep_min: bool,
) {
    let Some(candidate) = candidate else {
        return;
    };
    match destination {
        None => *destination = Some(candidate),
        Some(current) => {
            let replace = if keep_min {
                crate::storage::index::sort_key::scalar_lt(&candidate, current)
            } else {
                crate::storage::index::sort_key::scalar_lt(current, &candidate)
            };
            if replace {
                *current = candidate;
            }
        }
    }
}

fn build_column_metadata(
    col: &ColumnArray,
    offset: usize,
    count: usize,
    bloom_enabled: bool,
) -> ColumnMetadata {
    match col {
        ColumnArray::Bool { data, bitmap } => {
            let mut min_value = None;
            let mut max_value = None;
            let mut null_count = 0u64;
            let mut bloom_filter = bloom_enabled.then(BloomFilter::with_defaults);
            let mut values = Some(HashSet::new());
            for (row, &raw) in data.iter().enumerate().skip(offset).take(count) {
                if bitmap.is_null(row) {
                    null_count += 1;
                    continue;
                }
                let value = raw != 0;
                min_value = Some(min_value.map_or(value, |current: bool| current.min(value)));
                max_value = Some(max_value.map_or(value, |current: bool| current.max(value)));
                if let Some(filter) = &mut bloom_filter {
                    filter.add_typed(0, &value);
                }
                insert_bounded(&mut values, value);
            }
            ColumnMetadata {
                min_value: min_value.map(|value| ScalarValue::Bool(Some(value))),
                max_value: max_value.map(|value| ScalarValue::Bool(Some(value))),
                null_count,
                bloom_filter,
                value_set: values.map(|values| {
                    values
                        .into_iter()
                        .map(|value| ScalarValue::Bool(Some(value)))
                        .collect()
                }),
            }
        }
        ColumnArray::Int8 { data, bitmap } => {
            metadata_ord(data, bitmap, offset, count, bloom_enabled, 1, |value| {
                ScalarValue::Int8(Some(value))
            })
        }
        ColumnArray::Int16 { data, bitmap } => {
            metadata_ord(data, bitmap, offset, count, bloom_enabled, 2, |value| {
                ScalarValue::Int16(Some(value))
            })
        }
        ColumnArray::Int32 { data, bitmap } => {
            metadata_ord(data, bitmap, offset, count, bloom_enabled, 3, |value| {
                ScalarValue::Int32(Some(value))
            })
        }
        ColumnArray::Int64 { data, bitmap } => {
            metadata_ord(data, bitmap, offset, count, bloom_enabled, 4, |value| {
                ScalarValue::Int64(Some(value))
            })
        }
        ColumnArray::UInt8 { data, bitmap } => {
            metadata_ord(data, bitmap, offset, count, bloom_enabled, 5, |value| {
                ScalarValue::UInt8(Some(value))
            })
        }
        ColumnArray::UInt16 { data, bitmap } => {
            metadata_ord(data, bitmap, offset, count, bloom_enabled, 6, |value| {
                ScalarValue::UInt16(Some(value))
            })
        }
        ColumnArray::UInt32 { data, bitmap } => {
            metadata_ord(data, bitmap, offset, count, bloom_enabled, 7, |value| {
                ScalarValue::UInt32(Some(value))
            })
        }
        ColumnArray::UInt64 { data, bitmap } => {
            metadata_ord(data, bitmap, offset, count, bloom_enabled, 8, |value| {
                ScalarValue::UInt64(Some(value))
            })
        }
        ColumnArray::Float32 { data, bitmap } => {
            metadata_float32(data, bitmap, offset, count, bloom_enabled)
        }
        ColumnArray::Float64 { data, bitmap } => {
            metadata_float64(data, bitmap, offset, count, bloom_enabled)
        }
        ColumnArray::Decimal { data, bitmap } => {
            metadata_ord(data, bitmap, offset, count, bloom_enabled, 11, |value| {
                ScalarValue::Decimal(Some(value))
            })
        }
        ColumnArray::Date { data, bitmap } => {
            metadata_ord(data, bitmap, offset, count, bloom_enabled, 12, |value| {
                ScalarValue::Date(Some(value))
            })
        }
        ColumnArray::Time { data, bitmap } => {
            metadata_ord(data, bitmap, offset, count, bloom_enabled, 13, |value| {
                ScalarValue::Time(Some(value))
            })
        }
        ColumnArray::Timestamp { data, bitmap } => {
            metadata_ord(data, bitmap, offset, count, bloom_enabled, 14, |value| {
                ScalarValue::Timestamp(Some(value))
            })
        }
        ColumnArray::Interval { data, bitmap } => {
            metadata_ord(data, bitmap, offset, count, bloom_enabled, 15, |value| {
                ScalarValue::Interval(Some(value))
            })
        }
        ColumnArray::VarChar { data, bitmap } => {
            let mut min_value: Option<&str> = None;
            let mut max_value: Option<&str> = None;
            let mut null_count = 0u64;
            let mut bloom_filter = bloom_enabled.then(BloomFilter::with_defaults);
            let mut values = Some(HashSet::new());
            for row in offset..offset + count {
                if bitmap.is_null(row) {
                    null_count += 1;
                    continue;
                }
                let value = data.value(row);
                min_value = Some(min_value.map_or(value, |current| current.min(value)));
                max_value = Some(max_value.map_or(value, |current| current.max(value)));
                if let Some(filter) = &mut bloom_filter {
                    filter.add_typed(16, value);
                }
                insert_bounded(&mut values, value);
            }
            ColumnMetadata {
                min_value: min_value.map(|value| ScalarValue::VarChar(Some(value.to_string()))),
                max_value: max_value.map(|value| ScalarValue::VarChar(Some(value.to_string()))),
                null_count,
                bloom_filter,
                value_set: values.map(|values| {
                    values
                        .into_iter()
                        .map(|value| ScalarValue::VarChar(Some(value.to_string())))
                        .collect()
                }),
            }
        }
        ColumnArray::Blob { data, bitmap } => {
            let mut min_value: Option<&[u8]> = None;
            let mut max_value: Option<&[u8]> = None;
            let mut null_count = 0u64;
            let mut bloom_filter = bloom_enabled.then(BloomFilter::with_defaults);
            let mut values = Some(HashSet::new());
            for row in offset..offset + count {
                if bitmap.is_null(row) {
                    null_count += 1;
                    continue;
                }
                let value = data.value(row);
                min_value = Some(min_value.map_or(value, |current| current.min(value)));
                max_value = Some(max_value.map_or(value, |current| current.max(value)));
                if let Some(filter) = &mut bloom_filter {
                    filter.add_typed(17, value);
                }
                insert_bounded(&mut values, value);
            }
            ColumnMetadata {
                min_value: min_value.map(|value| ScalarValue::Blob(Some(value.to_vec()))),
                max_value: max_value.map(|value| ScalarValue::Blob(Some(value.to_vec()))),
                null_count,
                bloom_filter,
                value_set: values.map(|values| {
                    values
                        .into_iter()
                        .map(|value| ScalarValue::Blob(Some(value.to_vec())))
                        .collect()
                }),
            }
        }
        ColumnArray::Vector { data, bitmap } => {
            metadata_vector(data, bitmap, offset, count, bloom_enabled)
        }
    }
}

fn metadata_ord<T, F>(
    data: &[T],
    bitmap: &crate::types::bitmap::NullBitmap,
    offset: usize,
    count: usize,
    bloom_enabled: bool,
    type_tag: u8,
    make_scalar: F,
) -> ColumnMetadata
where
    T: Copy + Ord + std::hash::Hash,
    F: Fn(T) -> ScalarValue,
{
    let mut min_value = None;
    let mut max_value = None;
    let mut null_count = 0u64;
    let mut bloom_filter = bloom_enabled.then(BloomFilter::with_defaults);
    let mut values = Some(HashSet::new());
    for (row, &value) in data.iter().enumerate().skip(offset).take(count) {
        if bitmap.is_null(row) {
            null_count += 1;
            continue;
        }
        min_value = Some(min_value.map_or(value, |current: T| current.min(value)));
        max_value = Some(max_value.map_or(value, |current: T| current.max(value)));
        if let Some(filter) = &mut bloom_filter {
            filter.add_typed(type_tag, &value);
        }
        insert_bounded(&mut values, value);
    }
    ColumnMetadata {
        min_value: min_value.map(&make_scalar),
        max_value: max_value.map(&make_scalar),
        null_count,
        bloom_filter,
        value_set: values.map(|values| values.into_iter().map(make_scalar).collect()),
    }
}

fn metadata_float32(
    data: &[f32],
    bitmap: &crate::types::bitmap::NullBitmap,
    offset: usize,
    count: usize,
    bloom_enabled: bool,
) -> ColumnMetadata {
    let mut min_value = None;
    let mut max_value = None;
    let mut null_count = 0u64;
    let mut bloom_filter = bloom_enabled.then(BloomFilter::with_defaults);
    let mut values = Some(HashSet::new());
    for (row, &value) in data.iter().enumerate().skip(offset).take(count) {
        if bitmap.is_null(row) {
            null_count += 1;
            continue;
        }
        if min_value.is_none_or(|current| value < current) {
            min_value = Some(value);
        }
        if max_value.is_none_or(|current| value > current) {
            max_value = Some(value);
        }
        let bits = value.to_bits();
        if let Some(filter) = &mut bloom_filter {
            filter.add_typed(9, &bits);
        }
        insert_bounded(&mut values, bits);
    }
    ColumnMetadata {
        min_value: min_value.map(|value| ScalarValue::Float32(Some(value))),
        max_value: max_value.map(|value| ScalarValue::Float32(Some(value))),
        null_count,
        bloom_filter,
        value_set: values.map(|values| {
            values
                .into_iter()
                .map(|bits| ScalarValue::Float32(Some(f32::from_bits(bits))))
                .collect()
        }),
    }
}

fn metadata_float64(
    data: &[f64],
    bitmap: &crate::types::bitmap::NullBitmap,
    offset: usize,
    count: usize,
    bloom_enabled: bool,
) -> ColumnMetadata {
    let mut min_value = None;
    let mut max_value = None;
    let mut null_count = 0u64;
    let mut bloom_filter = bloom_enabled.then(BloomFilter::with_defaults);
    let mut values = Some(HashSet::new());
    for (row, &value) in data.iter().enumerate().skip(offset).take(count) {
        if bitmap.is_null(row) {
            null_count += 1;
            continue;
        }
        if min_value.is_none_or(|current| value < current) {
            min_value = Some(value);
        }
        if max_value.is_none_or(|current| value > current) {
            max_value = Some(value);
        }
        let bits = value.to_bits();
        if let Some(filter) = &mut bloom_filter {
            filter.add_typed(10, &bits);
        }
        insert_bounded(&mut values, bits);
    }
    ColumnMetadata {
        min_value: min_value.map(|value| ScalarValue::Float64(Some(value))),
        max_value: max_value.map(|value| ScalarValue::Float64(Some(value))),
        null_count,
        bloom_filter,
        value_set: values.map(|values| {
            values
                .into_iter()
                .map(|bits| ScalarValue::Float64(Some(f64::from_bits(bits))))
                .collect()
        }),
    }
}

fn metadata_vector(
    data: &[Vec<f32>],
    bitmap: &crate::types::bitmap::NullBitmap,
    offset: usize,
    count: usize,
    bloom_enabled: bool,
) -> ColumnMetadata {
    let mut min_row = None;
    let mut max_row = None;
    let mut null_count = 0u64;
    let mut bloom_filter = bloom_enabled.then(BloomFilter::with_defaults);
    let mut values = Some(HashSet::new());
    for row in offset..offset + count {
        if bitmap.is_null(row) {
            null_count += 1;
            continue;
        }
        if min_row.is_none_or(|current| data[row] < data[current]) {
            min_row = Some(row);
        }
        if max_row.is_none_or(|current| data[row] > data[current]) {
            max_row = Some(row);
        }
        if let Some(filter) = &mut bloom_filter {
            filter.add_vector(&data[row]);
        }
        if values.is_some() {
            let bits = data[row].iter().map(|value| value.to_bits()).collect();
            insert_bounded(&mut values, bits);
        }
    }
    ColumnMetadata {
        min_value: min_row.map(|row| ScalarValue::Vector(Some(data[row].clone()))),
        max_value: max_row.map(|row| ScalarValue::Vector(Some(data[row].clone()))),
        null_count,
        bloom_filter,
        value_set: values.map(|values| {
            values
                .into_iter()
                .map(|bits: Vec<u32>| {
                    ScalarValue::Vector(Some(bits.into_iter().map(f32::from_bits).collect()))
                })
                .collect()
        }),
    }
}

#[inline]
fn insert_bounded<T: Eq + std::hash::Hash>(values: &mut Option<HashSet<T>>, value: T) {
    if let Some(set) = values {
        set.insert(value);
        if set.len() > VALUE_SET_MAX_DISTINCT {
            *values = None;
        }
    }
}

fn compute_segment_zone_maps(
    row_groups: &[crate::storage::segment::RowGroupMeta],
    num_columns: usize,
) -> Vec<ZoneMap> {
    if row_groups.is_empty() {
        return Vec::new();
    }

    let mut segment_maps = Vec::with_capacity(num_columns);
    for col_idx in 0..num_columns {
        let mut seg_min: Option<ScalarValue> = None;
        let mut seg_max: Option<ScalarValue> = None;
        let mut total_null_count: u64 = 0;
        let mut total_row_count: u64 = 0;

        for rg in row_groups {
            if let Some(zm) = rg.zone_maps.get(col_idx) {
                total_row_count += zm.row_count;
                total_null_count += zm.null_count;

                if !zm.min_value.is_null() {
                    match &seg_min {
                        None => seg_min = Some(zm.min_value.clone()),
                        Some(m) => {
                            if crate::storage::index::sort_key::scalar_lt(&zm.min_value, m) {
                                seg_min = Some(zm.min_value.clone());
                            }
                        }
                    }
                }
                if !zm.max_value.is_null() {
                    match &seg_max {
                        None => seg_max = Some(zm.max_value.clone()),
                        Some(m) => {
                            if crate::storage::index::sort_key::scalar_lt(m, &zm.max_value) {
                                seg_max = Some(zm.max_value.clone());
                            }
                        }
                    }
                }
            }
        }

        segment_maps.push(ZoneMap {
            min_value: seg_min.unwrap_or(ScalarValue::Int64(None)),
            max_value: seg_max.unwrap_or(ScalarValue::Int64(None)),
            null_count: total_null_count,
            row_count: total_row_count,
        });
    }

    segment_maps
}
