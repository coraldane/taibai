use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

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

struct BuildingRowGroup {
    columns: Vec<Vec<u8>>,
    row_count: usize,
    uncompressed_sizes: Vec<u64>,
    zone_mins: Vec<Option<ScalarValue>>,
    zone_maxs: Vec<Option<ScalarValue>>,
    null_counts: Vec<u64>,
    bloom_filters: Vec<Option<BloomFilter>>,
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
            row_group_size,
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
                update_zone_map(
                    col,
                    offset,
                    to_write,
                    &mut group.zone_mins[col_idx],
                    &mut group.zone_maxs[col_idx],
                    &mut group.null_counts[col_idx],
                );

                for row_idx in offset..offset + to_write {
                    let val = col.scalar(row_idx);
                    if let Some(bloom_filter) = &mut group.bloom_filters[col_idx] {
                        bloom_filter.add(&val);
                    }
                }

                let encoded = encode_column(col, offset, to_write)?;
                let uncompressed_len = encoded.len() as u64;

                let compressed = zstd::bulk::compress(&encoded, 3)
                    .map_err(|e| TaibaiError::Storage(format!("zstd compress error: {}", e)))?;

                group.uncompressed_sizes[col_idx] += uncompressed_len;
                group.columns[col_idx].extend_from_slice(&compressed);
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

    pub fn seal(self) -> Result<()> {
        self.seal_with_deletes(&[], 0)
    }

    pub fn seal_with_deletes(mut self, delete_bitmap: &[u8], deleted_row_count: u64) -> Result<()> {
        if self.current_group.is_some() {
            self.flush_row_group()?;
        }

        let footer_offset = self.file.metadata()?.len();

        let footer = self.encode_footer_with_deletes(delete_bitmap, deleted_row_count)?;
        self.file.write_all(&footer)?;

        self.file.write_all(&footer_offset.to_le_bytes())?;

        self.file.sync_all()?;

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

        self.row_groups.push(crate::storage::segment::RowGroupMeta {
            offset,
            length: compressed_sizes.iter().sum(),
            row_count: group.row_count as u64,
            column_compressed_sizes,
            compressed_sizes,
            uncompressed_sizes: group.uncompressed_sizes,
            zone_maps,
            bloom_filters,
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
    }

    buf.extend_from_slice(&deleted_row_count.to_le_bytes());
    buf.extend_from_slice(&(delete_bitmap.len() as u32).to_le_bytes());
    if !delete_bitmap.is_empty() {
        buf.extend_from_slice(delete_bitmap);
    }

    buf
}

fn update_zone_map(
    col: &ColumnArray,
    offset: usize,
    count: usize,
    cur_min: &mut Option<ScalarValue>,
    cur_max: &mut Option<ScalarValue>,
    null_count: &mut u64,
) {
    for i in offset..offset + count {
        if col.bitmap().is_null(i) {
            *null_count += 1;
            continue;
        }
        let v = col.scalar(i);
        match cur_min {
            None => *cur_min = Some(v.clone()),
            Some(m) => {
                if crate::storage::index::sort_key::scalar_lt(&v, m) {
                    *cur_min = Some(v.clone());
                }
            }
        }
        match cur_max {
            None => *cur_max = Some(v),
            Some(m) => {
                if crate::storage::index::sort_key::scalar_lt(m, &v) {
                    *cur_max = Some(v);
                }
            }
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
