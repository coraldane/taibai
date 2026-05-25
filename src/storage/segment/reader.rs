use std::io::{Read, Seek, SeekFrom, Write};

use std::fs;
use std::path::{Path, PathBuf};

use memmap2::Mmap;

use crate::common::error::{MonaxError, Result};
use crate::storage::codec::encoding::{decode_column, decode_column_selected};
use crate::storage::index::bloomfilter::BloomFilter;
use crate::storage::partition::pruning::{RowGroupPredicate, ScanStats, ZoneMap};
use crate::types::array::ColumnArray;
use crate::types::batch::Schema;
use crate::types::scalar::ScalarValue;

use super::constants::{SEGMENT_HEADER_SIZE, SEGMENT_MAGIC, SEGMENT_VERSION};
use super::footer::{parse_segment_footer, reserved_section_offset, ParsedSegmentFooter};
use super::RowGroupMeta;

struct SegmentFooter {
    version: u32,
    row_count: u64,
    row_groups: Vec<RowGroupMeta>,
    schema: Schema,
    deleted_row_count: u64,
    delete_bitmap: Vec<u8>,
    segment_zone_maps: Vec<ZoneMap>,
}

impl From<ParsedSegmentFooter> for SegmentFooter {
    fn from(f: ParsedSegmentFooter) -> Self {
        Self {
            version: f.version,
            row_count: f.row_count,
            row_groups: f.row_groups,
            schema: f.schema,
            deleted_row_count: f.deleted_row_count,
            delete_bitmap: f.delete_bitmap,
            segment_zone_maps: f.segment_zone_maps,
        }
    }
}

pub struct SegmentReader {
    path: PathBuf,
    mmap: Mmap,
    _file: fs::File,
    footer: SegmentFooter,
}

impl SegmentReader {
    pub fn open(path: &Path) -> Result<Self> {
        let file = fs::File::open(path)?;
        let file_len = file.metadata()?.len();

        if file_len < (SEGMENT_HEADER_SIZE as u64) + 8 {
            return Err(MonaxError::Storage(format!(
                "segment file too small: {} bytes (need >= {})",
                file_len,
                SEGMENT_HEADER_SIZE + 8
            )));
        }

        let mut tmp_file = file;
        tmp_file.seek(SeekFrom::Start(0))?;
        let mut header_buf = [0u8; SEGMENT_HEADER_SIZE];
        tmp_file.read_exact(&mut header_buf)?;
        let hdr_magic = u32::from_le_bytes(header_buf[0..4].try_into().unwrap());
        let hdr_version = u32::from_le_bytes(header_buf[4..8].try_into().unwrap());
        if hdr_magic != SEGMENT_MAGIC {
            return Err(MonaxError::Storage(format!(
                "invalid segment header magic: {:08X}, expected {:08X} ({})",
                hdr_magic,
                SEGMENT_MAGIC,
                path.display(),
            )));
        }
        if hdr_version != SEGMENT_VERSION {
            return Err(MonaxError::Storage(format!(
                "unsupported segment version: {} (expected {}, file: {})",
                hdr_version,
                SEGMENT_VERSION,
                path.display(),
            )));
        }

        let mut footer_offset_buf = [0u8; 8];
        tmp_file.seek(SeekFrom::End(-8))?;
        tmp_file.read_exact(&mut footer_offset_buf)?;
        let footer_offset = u64::from_le_bytes(footer_offset_buf) as usize;

        if footer_offset < SEGMENT_HEADER_SIZE || footer_offset > file_len as usize {
            return Err(MonaxError::Storage(format!(
                "invalid footer offset: {} (file size {}, header size {})",
                footer_offset, file_len, SEGMENT_HEADER_SIZE
            )));
        }

        let mmap = unsafe { Mmap::map(&tmp_file)? };

        let footer_end = file_len as usize - 8;
        let footer_data = &mmap[footer_offset..footer_end];
        let parsed = parse_segment_footer(footer_data)?;

        Ok(Self {
            path: path.to_path_buf(),
            mmap,
            _file: tmp_file,
            footer: parsed.into(),
        })
    }

    pub fn schema(&self) -> &Schema {
        &self.footer.schema
    }

    pub fn row_count(&self) -> u64 {
        self.footer.row_count
    }

    pub fn num_row_groups(&self) -> usize {
        self.footer.row_groups.len()
    }

    pub fn zone_maps(&self, row_group: usize) -> &[ZoneMap] {
        &self.footer.row_groups[row_group].zone_maps
    }

    pub fn bloom_filters(&self, row_group: usize) -> &[Option<BloomFilter>] {
        &self.footer.row_groups[row_group].bloom_filters
    }

    pub fn segment_zone_maps(&self) -> &[ZoneMap] {
        &self.footer.segment_zone_maps
    }

    pub fn footer_row_groups_info(&self) -> Vec<RowGroupMeta> {
        self.footer.row_groups.clone()
    }

    pub fn row_group_rows(&self, row_group: usize) -> u64 {
        self.footer.row_groups[row_group].row_count
    }

    pub fn deleted_row_count(&self) -> u64 {
        self.footer.deleted_row_count
    }

    pub fn delete_bitmap(&self) -> &[u8] {
        &self.footer.delete_bitmap
    }

    pub fn is_row_deleted(&self, row_id: u64) -> bool {
        let bm = &self.footer.delete_bitmap;
        if bm.is_empty() {
            return false;
        }
        let byte = (row_id / 8) as usize;
        let bit = (row_id % 8) as u8;
        match bm.get(byte) {
            Some(b) => (b & (1u8 << bit)) != 0,
            None => false,
        }
    }

    pub fn rewrite_footer_with_deletes(
        path: &Path,
        delete_bitmap: &[u8],
        deleted_row_count: u64,
    ) -> Result<()> {
        let footer_bytes = Self::read_footer_bytes(path)?;
        let res_offset = reserved_section_offset(&footer_bytes)?;

        let mut new_footer = Vec::with_capacity(res_offset + 12 + delete_bitmap.len());
        new_footer.extend_from_slice(&footer_bytes[..res_offset]);
        new_footer.extend_from_slice(&deleted_row_count.to_le_bytes());
        new_footer.extend_from_slice(&(delete_bitmap.len() as u32).to_le_bytes());
        if !delete_bitmap.is_empty() {
            new_footer.extend_from_slice(delete_bitmap);
        }

        let mut file = fs::OpenOptions::new().write(true).read(true).open(path)?;
        let file_len = file.metadata()?.len();
        let mut tail = [0u8; 8];
        file.seek(SeekFrom::End(-8))?;
        file.read_exact(&mut tail)?;
        let footer_offset = u64::from_le_bytes(tail);

        file.seek(SeekFrom::Start(footer_offset))?;
        file.write_all(&new_footer)?;
        file.write_all(&footer_offset.to_le_bytes())?;
        let new_len = footer_offset + new_footer.len() as u64 + 8;
        if new_len < file_len {
            file.set_len(new_len)?;
        }
        file.sync_all()?;
        Ok(())
    }

    fn read_footer_bytes(path: &Path) -> Result<Vec<u8>> {
        let mut file = fs::File::open(path)?;
        let file_len = file.metadata()?.len();
        if file_len < (SEGMENT_HEADER_SIZE as u64) + 8 {
            return Err(MonaxError::Storage(format!(
                "segment file too small: {}",
                file_len
            )));
        }
        let mut tail = [0u8; 8];
        file.seek(SeekFrom::End(-8))?;
        file.read_exact(&mut tail)?;
        let footer_offset = u64::from_le_bytes(tail);
        if footer_offset >= file_len - 8 {
            return Err(MonaxError::Storage(format!(
                "invalid footer offset {}",
                footer_offset
            )));
        }
        let footer_len = (file_len - 8 - footer_offset) as usize;
        let mut buf = vec![0u8; footer_len];
        file.seek(SeekFrom::Start(footer_offset))?;
        file.read_exact(&mut buf)?;
        Ok(buf)
    }

    pub fn read_column(&self, row_group: usize, column_idx: usize) -> Result<ColumnArray> {
        let rg = &self.footer.row_groups[row_group];
        let col_def = &self.footer.schema.columns[column_idx];

        let mut col_offset = rg.offset as usize;
        for i in 0..column_idx {
            col_offset += rg.column_compressed_sizes[i] as usize;
        }
        let col_len = rg.column_compressed_sizes[column_idx] as usize;

        let compressed = &self.mmap[col_offset..col_offset + col_len];

        let decompressed = zstd::bulk::decompress(compressed, 64 * 1024 * 1024)
            .map_err(|e| MonaxError::Storage(format!("zstd decompress error: {}", e)))?;

        decode_column(&decompressed, rg.row_count as usize, col_def)
    }

    pub fn read_row_group_batch(
        &self,
        row_group: usize,
        column_indices: &[usize],
    ) -> Result<Vec<(usize, ColumnArray)>> {
        if column_indices.is_empty() {
            return Ok(Vec::new());
        }

        let rg = &self.footer.row_groups[row_group];
        let total_size: usize = rg.column_compressed_sizes.iter().map(|s| *s as usize).sum();
        if total_size == 0 {
            return Ok(Vec::new());
        }

        let all_data = &self.mmap[rg.offset as usize..rg.offset as usize + total_size];

        let col_offsets: Vec<usize> = rg
            .column_compressed_sizes
            .iter()
            .scan(0, |acc, s| {
                let offset = *acc;
                *acc += *s as usize;
                Some(offset)
            })
            .collect();

        let pool = crate::storage::core::buffer_pool::global_batch_pool();
        let mut results = Vec::with_capacity(column_indices.len());

        for &col_idx in column_indices {
            if col_idx >= col_offsets.len() {
                continue;
            }

            let col_offset = col_offsets[col_idx];
            let col_len = rg.column_compressed_sizes[col_idx] as usize;
            let compressed = &all_data[col_offset..col_offset + col_len];

            let mut buf = pool.acquire();
            buf.reserve(col_len * 3);

            let decompressed = zstd::bulk::decompress(compressed, 64 * 1024 * 1024)
                .map_err(|e| MonaxError::Storage(format!("zstd decompress error: {}", e)))?;

            pool.release(buf);

            let col_def = &self.footer.schema.columns[col_idx];
            let col_array = decode_column(&decompressed, rg.row_count as usize, col_def)?;
            results.push((col_idx, col_array));
        }

        Ok(results)
    }

    pub fn read_columns_filtered(
        &self,
        row_group: usize,
        column_indices: &[usize],
        selected_rows: &[usize],
    ) -> Result<Vec<(usize, ColumnArray)>> {
        if column_indices.is_empty() || selected_rows.is_empty() {
            return Ok(Vec::new());
        }

        let rg = &self.footer.row_groups[row_group];
        let total_size: usize = rg.column_compressed_sizes.iter().map(|s| *s as usize).sum();
        if total_size == 0 {
            return Ok(Vec::new());
        }

        let all_data = &self.mmap[rg.offset as usize..rg.offset as usize + total_size];

        let col_offsets: Vec<usize> = rg
            .column_compressed_sizes
            .iter()
            .scan(0, |acc, s| {
                let offset = *acc;
                *acc += *s as usize;
                Some(offset)
            })
            .collect();

        let mut results = Vec::with_capacity(column_indices.len());

        for &col_idx in column_indices {
            if col_idx >= col_offsets.len() {
                continue;
            }

            let col_offset = col_offsets[col_idx];
            let col_len = rg.column_compressed_sizes[col_idx] as usize;
            let compressed = &all_data[col_offset..col_offset + col_len];
            let decompressed = zstd::bulk::decompress(compressed, 64 * 1024 * 1024)
                .map_err(|e| MonaxError::Storage(format!("zstd decompress error: {}", e)))?;

            let col_def = &self.footer.schema.columns[col_idx];
            let col_array = decode_column_selected(
                &decompressed,
                rg.row_count as usize,
                col_def,
                selected_rows,
            )?;
            results.push((col_idx, col_array));
        }

        Ok(results)
    }

    pub fn read_columns_fast(
        &self,
        row_group: usize,
        column_indices: &[usize],
    ) -> Result<crate::types::batch::RecordBatch> {
        let columns_data = self.read_row_group_batch(row_group, column_indices)?;

        let mut columns = Vec::with_capacity(column_indices.len());
        let mut col_defs = Vec::with_capacity(column_indices.len());

        for (col_idx, col_array) in columns_data {
            columns.push(col_array);
            col_defs.push(self.footer.schema.columns[col_idx].clone());
        }

        let schema = crate::types::batch::Schema::new(col_defs);
        crate::types::batch::RecordBatch::try_new(schema, columns)
    }

    pub fn read_row_by_index(
        &self,
        global_row_idx: usize,
        column_names: &[String],
    ) -> Result<crate::types::batch::RecordBatch> {
        if global_row_idx >= self.row_count() as usize {
            return Err(crate::common::error::MonaxError::Internal(format!(
                "row index {} out of bounds for segment with {} rows",
                global_row_idx,
                self.row_count()
            )));
        }

        let mut cumulative_rows = 0usize;
        let mut target_rg = 0;
        let mut local_row_idx = 0;

        for (rg_idx, rg) in self.footer.row_groups.iter().enumerate() {
            let rg_rows = rg.row_count as usize;
            if global_row_idx < cumulative_rows + rg_rows {
                target_rg = rg_idx;
                local_row_idx = global_row_idx - cumulative_rows;
                break;
            }
            cumulative_rows += rg_rows;
        }

        let mut column_indices = Vec::with_capacity(column_names.len());
        for name in column_names {
            if let Some(idx) = self.footer.schema.index_of(name) {
                column_indices.push(idx);
            }
        }

        let batch = self.read_columns_fast(target_rg, &column_indices)?;

        if batch.num_rows() == 0 {
            return Ok(crate::types::batch::RecordBatch::new(batch.schema.clone()));
        }

        let mut selected_columns = Vec::with_capacity(column_indices.len());
        for (i, &col_idx) in column_indices.iter().enumerate() {
            if let Some(col) = batch.column(i) {
                let scalar = col.scalar(local_row_idx);
                let mut single_col = ColumnArray::empty(col.data_type());
                single_col.push(&scalar)?;
                selected_columns.push(single_col);
            }
        }

        let result_schema = crate::types::batch::Schema::new(
            column_names
                .iter()
                .zip(column_indices.iter())
                .map(|(name, &idx)| crate::types::batch::ColumnDef {
                    name: name.clone(),
                    data_type: self.footer.schema.columns[idx].data_type.clone(),
                    nullable: self.footer.schema.columns[idx].nullable,
                    scale: self.footer.schema.columns[idx].scale,
                    precision: self.footer.schema.columns[idx].precision,
                    auto_increment: self.footer.schema.columns[idx].auto_increment,
                })
                .collect(),
        );

        Ok(crate::types::batch::RecordBatch::try_new(
            result_schema,
            selected_columns,
        )?)
    }

    pub fn read_row_group(&self, row_group: usize) -> Result<crate::types::batch::RecordBatch> {
        let num_cols = self.footer.schema.num_columns();
        let mut columns = Vec::with_capacity(num_cols);
        for col_idx in 0..num_cols {
            columns.push(self.read_column(row_group, col_idx)?);
        }
        crate::types::batch::RecordBatch::try_new(self.footer.schema.clone(), columns)
    }

    pub fn read_row_group_with_projection(
        &self,
        row_group: usize,
        projection: &[usize],
    ) -> Result<crate::types::batch::RecordBatch> {
        let mut columns = Vec::with_capacity(projection.len());
        let mut col_defs = Vec::with_capacity(projection.len());
        for &col_idx in projection {
            columns.push(self.read_column(row_group, col_idx)?);
            col_defs.push(self.footer.schema.columns[col_idx].clone());
        }
        let schema = crate::types::batch::Schema::new(col_defs);
        crate::types::batch::RecordBatch::try_new(schema, columns)
    }

    pub fn scan(&self, batch_size: usize) -> Result<Vec<crate::types::batch::RecordBatch>> {
        self.scan_with_predicate(batch_size, None)
    }

    pub fn scan_with_predicate(
        &self,
        batch_size: usize,
        predicate: Option<&RowGroupPredicate>,
    ) -> Result<Vec<crate::types::batch::RecordBatch>> {
        self.scan_with_predicate_and_stats(batch_size, predicate, None)
    }

    pub fn scan_with_predicate_and_stats(
        &self,
        batch_size: usize,
        predicate: Option<&RowGroupPredicate>,
        mut stats: Option<&mut ScanStats>,
    ) -> Result<Vec<crate::types::batch::RecordBatch>> {
        self.scan_internal(batch_size, predicate, None, stats)
    }

    pub fn scan_with_projection_and_stats(
        &self,
        batch_size: usize,
        predicate: Option<&RowGroupPredicate>,
        projection: Option<&[usize]>,
        mut stats: Option<&mut ScanStats>,
    ) -> Result<Vec<crate::types::batch::RecordBatch>> {
        self.scan_internal(batch_size, predicate, projection, stats)
    }

    fn scan_internal(
        &self,
        batch_size: usize,
        predicate: Option<&RowGroupPredicate>,
        projection: Option<&[usize]>,
        mut stats: Option<&mut ScanStats>,
    ) -> Result<Vec<crate::types::batch::RecordBatch>> {
        let mut batches = Vec::new();
        let total = self.footer.row_groups.len();
        if let Some(s) = stats.as_deref_mut() {
            s.segments_opened += 1;
            s.row_groups_total += total as u64;
        }
        let mut skipped = 0usize;
        let mut row_start: u64 = 0;
        let bitmap_active = !self.footer.delete_bitmap.is_empty();
        for rg_idx in 0..total {
            let rg_rows = self.footer.row_groups[rg_idx].row_count;
            let rg_first_row = row_start;
            row_start += rg_rows;
            if let Some(pred) = predicate {
                if !pred.matches_row_group(&self.footer.row_groups[rg_idx].zone_maps) {
                    skipped += 1;
                    if let Some(s) = stats.as_deref_mut() {
                        s.row_groups_pruned += 1;
                    }
                    continue;
                }
            }
            if let Some(s) = stats.as_deref_mut() {
                s.row_groups_read += 1;
                if let Some(proj) = projection {
                    for &idx in proj {
                        if let Some(&b) = self.footer.row_groups[rg_idx]
                            .column_compressed_sizes
                            .get(idx)
                        {
                            s.bytes_read_compressed += b;
                        }
                    }
                } else {
                    for &b in &self.footer.row_groups[rg_idx].column_compressed_sizes {
                        s.bytes_read_compressed += b;
                    }
                }
            }
            let mut batch = if let Some(proj) = projection {
                self.read_row_group_with_projection(rg_idx, proj)?
            } else {
                self.read_row_group(rg_idx)?
            };
            if bitmap_active {
                let before = batch.num_rows();
                batch =
                    filter_batch_by_delete_bitmap(&batch, rg_first_row, &self.footer.delete_bitmap);
                if let Some(s) = stats.as_deref_mut() {
                    s.rows_filtered_by_delete_bitmap += (before - batch.num_rows()) as u64;
                }
            }
            if let Some(s) = stats.as_deref_mut() {
                s.rows_scanned += batch.num_rows() as u64;
            }
            if batch.num_rows() == 0 {
                continue;
            }
            if batch.num_rows() <= batch_size {
                batches.push(batch);
            } else {
                for start in (0..batch.num_rows()).step_by(batch_size) {
                    let end = (start + batch_size).min(batch.num_rows());
                    batches.push(split_batch(&batch, start, end));
                }
            }
        }
        if predicate.is_some() && skipped > 0 {
            tracing::debug!(
                segment = %self.path.display(),
                row_groups_total = total,
                row_groups_skipped = skipped,
                "zone-map pruning skipped row groups"
            );
        }
        Ok(batches)
    }

    pub fn count_matching_rows_fast(&self, column_idx: usize, value: &ScalarValue) -> Result<u64> {
        let mut total_count: u64 = 0;
        let bitmap_active = !self.footer.delete_bitmap.is_empty();

        for (rg_idx, rg) in self.footer.row_groups.iter().enumerate() {
            if let Some(bf) = rg.bloom_filters.get(column_idx).and_then(|b| b.as_ref()) {
                if !bf.might_contain(value) {
                    continue;
                }
            }

            if let Some(zm) = rg.zone_maps.get(column_idx) {
                if zm.row_count > 0 && zm.null_count == zm.row_count {
                    continue;
                }
                if !zm.min_value.is_null() && !zm.max_value.is_null() {
                    if !zm.contains(value) {
                        continue;
                    }
                }
            }

            let col = self.read_column(rg_idx, column_idx)?;
            let mask = col.eq_vec(value);
            let mut match_count = mask.iter().filter(|&&m| m).count() as u64;

            if bitmap_active {
                let rg_start_row = self
                    .footer
                    .row_groups
                    .iter()
                    .take(rg_idx)
                    .map(|r| r.row_count)
                    .sum::<u64>();
                for (i, &matched) in mask.iter().enumerate() {
                    if matched && self.is_row_deleted(rg_start_row + i as u64) {
                        match_count -= 1;
                    }
                }
            }

            total_count += match_count;
        }

        Ok(total_count)
    }
}

fn split_batch(
    batch: &crate::types::batch::RecordBatch,
    start: usize,
    end: usize,
) -> crate::types::batch::RecordBatch {
    let mut columns = Vec::with_capacity(batch.num_columns());
    for i in 0..batch.num_columns() {
        columns.push(split_column(batch.column(i).unwrap(), start, end));
    }
    crate::types::batch::RecordBatch::try_new(batch.schema.clone(), columns).unwrap()
}

fn filter_batch_by_delete_bitmap(
    batch: &crate::types::batch::RecordBatch,
    first_row_id: u64,
    delete_bitmap: &[u8],
) -> crate::types::batch::RecordBatch {
    let mut keep = Vec::with_capacity(batch.num_rows());
    for i in 0..batch.num_rows() {
        let row_id = first_row_id + i as u64;
        let byte = (row_id / 8) as usize;
        let bit = (row_id % 8) as u8;
        let deleted = delete_bitmap
            .get(byte)
            .map_or(false, |b| (b & (1u8 << bit)) != 0);
        if !deleted {
            keep.push(i);
        }
    }
    if keep.len() == batch.num_rows() {
        return batch.clone();
    }
    let mut columns = Vec::with_capacity(batch.num_columns());
    for c in 0..batch.num_columns() {
        let src = batch.column(c).unwrap();
        let mut new_col = ColumnArray::empty(src.data_type());
        for &i in &keep {
            new_col.push(&src.scalar(i)).unwrap();
        }
        columns.push(new_col);
    }
    crate::types::batch::RecordBatch::try_new(batch.schema.clone(), columns).unwrap()
}

fn split_column(col: &ColumnArray, start: usize, end: usize) -> ColumnArray {
    let mut new_col = ColumnArray::empty(col.data_type());
    for i in start..end {
        new_col.push(&col.scalar(i)).unwrap();
    }
    new_col
}
