use std::io::{Read, Seek, SeekFrom, Write};

use std::fs;
use std::path::{Path, PathBuf};

use memmap2::Mmap;

use crate::common::error::{Result, TaibaiError};
use crate::storage::codec::column_codec::{decode_column_selected, decode_column_with_runs};
use crate::storage::index::bloomfilter::BloomFilter;
use crate::storage::partition::pruning::{RowGroupPredicate, ScanStats, ZoneMap};
use crate::storage::segment::delete_bitmap;
use crate::types::array::ColumnArray;
use crate::types::batch::Schema;
use crate::types::scalar::ScalarValue;

use super::constants::{is_supported_segment_version, SEGMENT_HEADER_SIZE, SEGMENT_MAGIC};
use super::footer::{parse_segment_footer, reserved_section_offset, ParsedSegmentFooter};
use super::footer_ext::{append_footer_crc, encode_footer_tail};
use super::meta::{read_embedded_footer, read_latest_sidecar_footer};
use super::RowGroupMeta;

const ZSTD_FRAME_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];

struct SegmentFooter {
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
    data: SegmentData,
    footer: SegmentFooter,
    data_end: u64,
    /// Prefix sums of row-group row counts, built once at open:
    /// `row_group_start_row` used to re-sum per call (O(row_groups^2) per
    /// scan segment during delete-bitmap filtering).
    row_group_starts: Vec<u64>,
    /// Uncommitted-transaction delete view, set only on scan-side reader
    /// instances opened for a transaction that ran deferred DML (never on
    /// the shared cached readers). When present, every delete lookup answers
    /// from this composed bitmap instead of the footer.
    txn_delete_overlay: Option<TxnDeleteOverlay>,
}

struct TxnDeleteOverlay {
    bitmap: std::sync::Arc<Vec<u8>>,
    deleted_count: u64,
}

struct FooterSource {
    data_end: u64,
    footer: Vec<u8>,
}

enum SegmentData {
    Mmap(Mmap),
    Owned(Vec<u8>),
}

impl SegmentData {
    fn as_slice(&self) -> &[u8] {
        match self {
            Self::Mmap(mmap) => mmap,
            Self::Owned(data) => data,
        }
    }
}

impl SegmentReader {
    pub fn open(path: &Path) -> Result<Self> {
        Self::open_inner(path, false)
    }

    pub fn open_owned(path: &Path) -> Result<Self> {
        Self::open_inner(path, true)
    }

    fn open_inner(path: &Path, owned: bool) -> Result<Self> {
        let file = fs::File::open(path)?;
        let file_len = file.metadata()?.len();

        if file_len < SEGMENT_HEADER_SIZE as u64 {
            return Err(TaibaiError::Storage(format!(
                "segment file too small: {} bytes (need >= {})",
                file_len, SEGMENT_HEADER_SIZE
            )));
        }

        let mut tmp_file = file;
        tmp_file.seek(SeekFrom::Start(0))?;
        let mut header_buf = [0u8; SEGMENT_HEADER_SIZE];
        tmp_file.read_exact(&mut header_buf)?;
        let hdr_magic = u32::from_le_bytes(header_buf[0..4].try_into().unwrap());
        let hdr_version = u32::from_le_bytes(header_buf[4..8].try_into().unwrap());
        if hdr_magic != SEGMENT_MAGIC {
            return Err(TaibaiError::Storage(format!(
                "invalid segment header magic: {:08X}, expected {:08X} ({})",
                hdr_magic,
                SEGMENT_MAGIC,
                path.display(),
            )));
        }
        if !is_supported_segment_version(hdr_version) {
            return Err(TaibaiError::Storage(format!(
                "unsupported segment version: {} (supported: {:?}, file: {}) - run the migrate tool after upgrading",
                hdr_version,
                super::constants::SUPPORTED_SEGMENT_VERSIONS,
                path.display(),
            )));
        }

        let footer_source = Self::read_footer_source(path, Some(file_len))?;
        if footer_source.data_end < SEGMENT_HEADER_SIZE as u64 || footer_source.data_end > file_len
        {
            return Err(TaibaiError::Storage(format!(
                "invalid footer data_end: {} (file size {}, header size {})",
                footer_source.data_end, file_len, SEGMENT_HEADER_SIZE
            )));
        }

        let data = if owned {
            let mut buf = vec![0u8; file_len as usize];
            tmp_file.seek(SeekFrom::Start(0))?;
            tmp_file.read_exact(&mut buf).map_err(|err| {
                TaibaiError::Storage(format!(
                    "segment file changed while opening: expected {} bytes ({}, {})",
                    file_len,
                    path.display(),
                    err
                ))
            })?;
            SegmentData::Owned(buf)
        } else {
            SegmentData::Mmap(unsafe { Mmap::map(&tmp_file)? })
        };

        let parsed = parse_segment_footer(&footer_source.footer)?;

        let footer: SegmentFooter = parsed.into();
        let mut row_group_starts = Vec::with_capacity(footer.row_groups.len() + 1);
        let mut running = 0u64;
        for rg in &footer.row_groups {
            row_group_starts.push(running);
            running = running.saturating_add(rg.row_count);
        }
        row_group_starts.push(running);
        Ok(Self {
            path: path.to_path_buf(),
            data,
            footer,
            data_end: footer_source.data_end,
            row_group_starts,
            txn_delete_overlay: None,
        })
    }

    pub fn schema(&self) -> &Schema {
        &self.footer.schema
    }

    pub fn row_count(&self) -> u64 {
        self.footer.row_count
    }

    pub fn data_end(&self) -> u64 {
        self.data_end
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

    pub fn value_sets(
        &self,
        row_group: usize,
    ) -> &[Option<Vec<crate::types::scalar::ScalarValue>>] {
        &self.footer.row_groups[row_group].value_sets
    }

    pub fn segment_zone_maps(&self) -> &[ZoneMap] {
        &self.footer.segment_zone_maps
    }

    pub fn footer_row_groups_info(&self) -> Vec<RowGroupMeta> {
        self.footer.row_groups.clone()
    }

    /// Borrowed footer row groups: metadata walks that only read zone maps
    /// and sums must not pay the deep clone in
    /// [`Self::footer_row_groups_info`] — value sets and bloom filters make
    /// that expensive on wide clustered segments.
    pub fn footer_row_groups(&self) -> &[RowGroupMeta] {
        &self.footer.row_groups
    }

    pub fn row_group_start_row(&self, row_group: usize) -> u64 {
        // O(1) from the prefix built at open; the old re-summing made
        // per-row-group delete filtering quadratic in the row-group count.
        self.row_group_starts
            .get(row_group)
            .copied()
            .unwrap_or_else(|| self.row_group_starts.last().copied().unwrap_or(0))
    }

    pub fn row_group_rows(&self, row_group: usize) -> u64 {
        self.footer.row_groups[row_group].row_count
    }

    /// Footer index for one catalog column, resolved by its current name or
    /// any recorded previous name (RENAME COLUMN). `None` when the segment
    /// predates the column (ADD COLUMN after the segment was written).
    pub fn footer_column_index_for_names(&self, names: &[String]) -> Option<usize> {
        names
            .iter()
            .find_map(|name| self.footer.schema.index_of(name))
    }

    /// Translate catalog column indices into footer column indices by name.
    ///
    /// Segments written before an ALTER keep the pre-DDL physical layout in
    /// their footer, and DROP/RENAME COLUMN shifts catalog positions without
    /// rewriting data: positional use of catalog indices silently read (and
    /// rewrote) the WRONG column. Every read that crosses the catalog→segment
    /// boundary must go through here.
    pub fn align_indices(
        &self,
        table_schema: &crate::types::batch::Schema,
        indices: &[usize],
    ) -> Result<Vec<usize>> {
        align_indices_against_schema(table_schema, &self.footer.schema, indices)
    }

    /// Re-anchor a catalog-space row-group predicate onto this segment's
    /// footer column order, so `matches_row_group(zone_maps)` indexes the
    /// zone maps of the columns the clauses actually reference instead of
    /// whatever happens to sit at the same positional index.
    pub fn align_row_group_predicate(
        &self,
        table_schema: &crate::types::batch::Schema,
        predicate: &crate::storage::partition::pruning::RowGroupPredicate,
    ) -> Result<crate::storage::partition::pruning::RowGroupPredicate> {
        if predicate.clauses.is_empty() {
            return Ok(predicate.clone());
        }
        let indices: Vec<usize> = predicate
            .clauses
            .iter()
            .map(|clause| clause.column_index)
            .collect();
        let aligned = self.align_indices(table_schema, &indices)?;
        let clauses = predicate
            .clauses
            .iter()
            .zip(aligned)
            .map(|(clause, column_index)| crate::storage::partition::pruning::RangeClause {
                column_index,
                op: clause.op,
                value: clause.value.clone(),
            })
            .collect();
        Ok(crate::storage::partition::pruning::RowGroupPredicate::new(
            clauses,
        ))
    }

    /// Read the requested catalog columns (by name, returned in request
    /// order) from one row group — the name-based counterpart of
    /// `read_row_group_with_projection` for callers holding catalog indices.
    ///
    /// The returned batch carries the CATALOG column defs of the requested
    /// columns, so downstream name-based evaluation (WHERE masks) and
    /// positional use of catalog indices both stay correct even when this
    /// segment's footer predates a DROP/RENAME COLUMN.
    pub fn read_row_group_columns(
        &self,
        row_group: usize,
        table_schema: &crate::types::batch::Schema,
        indices: &[usize],
    ) -> Result<crate::types::batch::RecordBatch> {
        let footer_indices = self.align_indices(table_schema, indices)?;
        let batch = self.read_row_group_with_projection(row_group, &footer_indices)?;
        batch_with_catalog_defs(batch, table_schema, indices)
    }

    /// Read ALL catalog columns of one row group, in catalog order — the
    /// read primitive for whole-row-group rewrites (UPDATE Pass B). Columns
    /// this segment predates (ADD COLUMN after the segment was written) are
    /// materialized as the column's DEFAULT value (or typed NULL when it has
    /// none), matching what a SELECT over the same segment returns, so
    /// batches from segments of different ages share one catalog-ordered
    /// shape and can be merged column-wise safely.
    pub fn read_row_group_columns_aligned(
        &self,
        row_group: usize,
        table_schema: &crate::types::batch::Schema,
    ) -> Result<crate::types::batch::RecordBatch> {
        let rg_rows = self.row_group_rows(row_group) as usize;
        let mut columns = Vec::with_capacity(table_schema.num_columns());
        for col_def in &table_schema.columns {
            let mut names = Vec::with_capacity(col_def.previous_names.len() + 1);
            names.push(col_def.name.clone());
            names.extend(col_def.previous_names.iter().cloned());
            match self.footer_column_index_for_names(&names) {
                Some(footer_idx) => columns.push(self.read_column(row_group, footer_idx)?),
                None => columns.push(default_value_column(col_def, rg_rows)?),
            }
        }
        crate::types::batch::RecordBatch::try_new(table_schema.clone(), columns)
    }

    /// Attach a transaction's composed delete bitmap (footer ∪ deferred
    /// tombstones of the owning transaction). Only fresh per-scan instances
    /// receive this — the cached readers are shared across sessions.
    pub fn set_txn_delete_overlay(&mut self, bitmap: std::sync::Arc<Vec<u8>>) {
        let deleted_count = bitmap.iter().map(|byte| byte.count_ones() as u64).sum();
        self.txn_delete_overlay = Some(TxnDeleteOverlay {
            bitmap,
            deleted_count,
        });
    }

    pub fn deleted_row_count(&self) -> u64 {
        match &self.txn_delete_overlay {
            Some(overlay) => overlay.deleted_count,
            None => self.footer.deleted_row_count,
        }
    }

    pub fn delete_bitmap(&self) -> &[u8] {
        match &self.txn_delete_overlay {
            Some(overlay) => &overlay.bitmap,
            None => &self.footer.delete_bitmap,
        }
    }

    pub fn is_row_deleted(&self, row_id: u64) -> bool {
        let bm = match &self.txn_delete_overlay {
            Some(overlay) => &overlay.bitmap,
            None => &self.footer.delete_bitmap,
        };
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
        // Delete rewrites use the embedded footer as the authoritative sealed
        // snapshot. SegmentManager refreshes the sidecar separately when this
        // is still an active segment.
        let source = Self::read_dat_footer_source(path, None)?;
        // Validate the source footer with the full parser (footer checksum,
        // extension structure) before copying any of it: a locally corrupt
        // footer used to be copied verbatim and "laundered" by the freshly
        // recomputed checksum.
        let parsed = parse_segment_footer(&source.footer)?;
        let res_offset = reserved_section_offset(&source.footer)?;

        // The rebuilt footer keeps the row-group prefix (including the
        // per-row-group CRC extensions) verbatim — the column blocks are
        // untouched — and replaces the delete section, so the whole-footer
        // checksum must be recomputed, never copied.
        let mut new_footer = Vec::with_capacity(res_offset + 20 + delete_bitmap.len());
        new_footer.extend_from_slice(&source.footer[..res_offset]);
        new_footer.extend_from_slice(&deleted_row_count.to_le_bytes());
        new_footer.extend_from_slice(&(delete_bitmap.len() as u32).to_le_bytes());
        if !delete_bitmap.is_empty() {
            new_footer.extend_from_slice(delete_bitmap);
        }
        // Preserve footer tail TLV entries across the rewrite, then
        // close with a fresh whole-footer checksum (always last).
        new_footer.extend_from_slice(&encode_footer_tail(&parsed.tail));
        append_footer_crc(&mut new_footer);

        // The in-place truncate-and-rewrite below is not atomic: a crash
        // between `set_len` and the new footer reaching disk would leave the
        // segment with neither embedded footer nor sidecar, silently dropping
        // its data from every scan. A journal file (`<segment>.del`) makes
        // the rewrite recoverable — see `repair_pending_footer_rewrite`,
        // which discovery invokes when a segment fails to open.
        let journal = pending_delete_journal_path(path);
        write_delete_journal(&journal, source.data_end, &new_footer)?;

        let write_result = Self::apply_footer_bytes_with_retry(path, source.data_end, &new_footer);
        if write_result.is_err() {
            // The journal stays behind: the next open of this segment
            // repairs from it instead of losing the data block.
            return write_result;
        }
        let _ = fs::remove_file(&journal);
        Ok(())
    }

    /// Truncate to `data_end` and append the new footer, retrying with
    /// backoff: on Windows, `set_len` fails while a reader holds the segment
    /// mmapped (ERROR_USER_MAPPED_FILE). The write is idempotent, so
    /// retrying the whole body after a partial attempt is safe. A long
    /// analytical scan easily holds its mmap for many seconds, so the window
    /// grows to ~10s (fixed 200ms steps); a delete failing here used to
    /// leave the statement partially applied.
    fn apply_footer_bytes_with_retry(path: &Path, data_end: u64, footer: &[u8]) -> Result<()> {
        const ATTEMPTS: usize = 50;
        const RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(200);
        let mut last_err = None;
        for attempt in 0..ATTEMPTS {
            let result = (|| -> Result<()> {
                let mut file = fs::OpenOptions::new().write(true).open(path)?;
                file.set_len(data_end)?;
                file.seek(SeekFrom::Start(data_end))?;
                file.write_all(footer)?;
                file.write_all(&data_end.to_le_bytes())?;
                file.flush()?;
                file.sync_all()?;
                Ok(())
            })();
            match result {
                Ok(()) => return Ok(()),
                Err(err) => {
                    // Non-retryable errors (permissions, missing file) fail
                    // fast instead of burning the full retry window.
                    if !error_is_retryable_file_conflict(&err) {
                        return Err(err);
                    }
                    last_err = Some(err);
                    if attempt + 1 < ATTEMPTS {
                        std::thread::sleep(RETRY_DELAY);
                    }
                }
            }
        }
        Err(last_err
            .unwrap_or_else(|| TaibaiError::Storage("segment footer rewrite failed".to_string())))
    }

    /// Complete or discard an interrupted delete-footer rewrite. Returns
    /// `true` when the embedded footer was repaired from the journal.
    pub fn repair_pending_footer_rewrite(path: &Path) -> Result<bool> {
        let journal = pending_delete_journal_path(path);
        if !journal.exists() {
            return Ok(false);
        }
        let Some((data_end, footer)) = read_delete_journal(&journal)? else {
            // Unreadable journal: nothing we can safely apply. Remove it so
            // it does not mask future rewrites of this segment.
            let _ = fs::remove_file(&journal);
            return Ok(false);
        };
        if Self::read_dat_footer_source(path, None).is_ok() {
            // The embedded footer parses: the rewrite had completed and only
            // the journal removal was lost.
            let _ = fs::remove_file(&journal);
            return Ok(false);
        }
        tracing::warn!(
            file = %path.display(),
            "repairing segment from interrupted delete-footer journal"
        );
        Self::apply_footer_bytes_with_retry(path, data_end, &footer)?;
        let _ = fs::remove_file(&journal);
        Ok(true)
    }

    fn read_footer_source(path: &Path, file_len_hint: Option<u64>) -> Result<FooterSource> {
        let file_len = match file_len_hint {
            Some(len) => len,
            None => fs::metadata(path)?.len(),
        };
        if let Some(sidecar) = read_latest_sidecar_footer(path)? {
            if sidecar.data_end >= SEGMENT_HEADER_SIZE as u64
                && sidecar.data_end <= file_len
                && parse_segment_footer(&sidecar.footer).is_ok()
            {
                return Ok(FooterSource {
                    data_end: sidecar.data_end,
                    footer: sidecar.footer,
                });
            }
            tracing::warn!(
                file = %path.display(),
                sidecar_data_end = sidecar.data_end,
                file_len,
                "ignoring invalid segment sidecar and falling back to embedded footer"
            );
        }

        Self::read_dat_footer_source(path, Some(file_len))
    }

    fn read_dat_footer_source(path: &Path, _file_len_hint: Option<u64>) -> Result<FooterSource> {
        let source = read_embedded_footer(path)?;
        Ok(FooterSource {
            data_end: source.data_end,
            footer: source.footer,
        })
    }

    fn decompress_column_block(block: &[u8]) -> Result<std::borrow::Cow<'_, [u8]>> {
        // Adaptive blocks that skip zstd begin with the null bitmap length
        // (u32 LE); bounded row groups keep it far below the zstd frame magic.
        if block.starts_with(&ZSTD_FRAME_MAGIC) {
            let decompressed = zstd::bulk::decompress(block, 64 * 1024 * 1024)
                .map_err(|e| TaibaiError::Storage(format!("zstd decompress error: {}", e)))?;
            Ok(std::borrow::Cow::Owned(decompressed))
        } else {
            Ok(std::borrow::Cow::Borrowed(block))
        }
    }

    pub fn read_column(&self, row_group: usize, column_idx: usize) -> Result<ColumnArray> {
        self.read_column_allow_dictionary(row_group, column_idx, false)
    }

    pub fn read_column_allow_dictionary(
        &self,
        row_group: usize,
        column_idx: usize,
        allow_dictionary: bool,
    ) -> Result<ColumnArray> {
        self.read_column_passthrough(row_group, column_idx, allow_dictionary, false)
    }

    /// `allow_runs` keeps Rle/DeltaRle columns in run form for run-aware
    /// consumers (everything else flattens lazily).
    pub fn read_column_passthrough(
        &self,
        row_group: usize,
        column_idx: usize,
        allow_dictionary: bool,
        allow_runs: bool,
    ) -> Result<ColumnArray> {
        let rg = &self.footer.row_groups[row_group];
        let col_def = &self.footer.schema.columns[column_idx];

        let decompressed = self.read_column_raw(row_group, column_idx)?;
        decode_column_with_runs(
            &decompressed,
            rg.row_count as usize,
            col_def,
            allow_dictionary,
            allow_runs,
        )
    }

    pub fn read_column_raw(&self, row_group: usize, column_idx: usize) -> Result<Vec<u8>> {
        let rg = &self.footer.row_groups[row_group];
        let mut col_offset = rg.offset as usize;
        for i in 0..column_idx {
            col_offset = col_offset.wrapping_add(rg.column_compressed_sizes[i] as usize);
        }
        let col_len = rg.column_compressed_sizes[column_idx] as usize;

        let data = self.data.as_slice();
        // Footer offsets come from disk: a corrupted footer must surface as
        // an error, not as an out-of-bounds panic.
        let col_end = col_offset
            .checked_add(col_len)
            .ok_or_else(|| TaibaiError::Storage("segment column offset overflow".to_string()))?;
        if col_offset > data.len() || col_end > data.len() {
            return Err(TaibaiError::Storage(format!(
                "segment column range {}..{} exceeds data length {}",
                col_offset,
                col_end,
                data.len()
            )));
        }
        let compressed = &data[col_offset..col_end];

        // Block checksum (present for segments written with the CRC
        // extension): bit rot must fail the read instead of decompressing
        // into garbage values.
        if let Some(&expected) = rg.column_crcs.get(column_idx) {
            if crc32fast::hash(compressed) != expected {
                return Err(TaibaiError::Storage(format!(
                    "segment column checksum mismatch at row group {} column {}",
                    row_group, column_idx
                )));
            }
        }

        Ok(Self::decompress_column_block(compressed)?.into_owned())
    }

    pub fn read_row_group_batch(
        &self,
        row_group: usize,
        column_indices: &[usize],
    ) -> Result<Vec<(usize, ColumnArray)>> {
        self.read_row_group_batch_allow_dictionary(row_group, column_indices, false)
    }

    pub fn read_row_group_batch_allow_dictionary(
        &self,
        row_group: usize,
        column_indices: &[usize],
        allow_dictionary: bool,
    ) -> Result<Vec<(usize, ColumnArray)>> {
        self.read_row_group_batch_passthrough(row_group, column_indices, allow_dictionary, false)
    }

    pub fn read_row_group_batch_passthrough(
        &self,
        row_group: usize,
        column_indices: &[usize],
        allow_dictionary: bool,
        allow_runs: bool,
    ) -> Result<Vec<(usize, ColumnArray)>> {
        if column_indices.is_empty() {
            return Ok(Vec::new());
        }

        let rg = &self.footer.row_groups[row_group];
        let total_size: usize = rg.column_compressed_sizes.iter().map(|s| *s as usize).sum();
        if total_size == 0 {
            return Ok(Vec::new());
        }

        let data = self.data.as_slice();
        // Footer offsets come from disk: a corrupted footer must surface as
        // an error, not as an out-of-bounds panic (same contract as
        // read_column_raw).
        let rg_end = (rg.offset as usize)
            .checked_add(total_size)
            .ok_or_else(|| TaibaiError::Storage("segment row group offset overflow".to_string()))?;
        if rg.offset as usize > data.len() || rg_end > data.len() {
            return Err(TaibaiError::Storage(format!(
                "segment row group range {}..{} exceeds data length {}",
                rg.offset,
                rg_end,
                data.len()
            )));
        }
        let all_data = &data[rg.offset as usize..rg_end];

        let col_offsets: Vec<usize> = rg
            .column_compressed_sizes
            .iter()
            .scan(0, |acc, s| {
                let offset = *acc;
                *acc += *s as usize;
                Some(offset)
            })
            .collect();

        use rayon::prelude::*;
        let results = column_indices
            .par_iter()
            .map(|&col_idx| -> Result<Option<(usize, ColumnArray)>> {
                if col_idx >= col_offsets.len() {
                    return Ok(None);
                }

                let col_offset = col_offsets[col_idx];
                let col_len = rg.column_compressed_sizes[col_idx] as usize;
                let compressed = &all_data[col_offset..col_offset + col_len];
                let decompressed = Self::decompress_column_block(compressed)?;
                let col_def = &self.footer.schema.columns[col_idx];
                let col_array = decode_column_with_runs(
                    &decompressed,
                    rg.row_count as usize,
                    col_def,
                    allow_dictionary,
                    allow_runs,
                )?;
                Ok(Some((col_idx, col_array)))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(results.into_iter().flatten().collect())
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

        let data = self.data.as_slice();
        // Footer offsets come from disk: a corrupted footer must surface as
        // an error, not as an out-of-bounds panic (same contract as
        // read_column_raw).
        let rg_end = (rg.offset as usize)
            .checked_add(total_size)
            .ok_or_else(|| TaibaiError::Storage("segment row group offset overflow".to_string()))?;
        if rg.offset as usize > data.len() || rg_end > data.len() {
            return Err(TaibaiError::Storage(format!(
                "segment row group range {}..{} exceeds data length {}",
                rg.offset,
                rg_end,
                data.len()
            )));
        }
        let all_data = &data[rg.offset as usize..rg_end];

        let col_offsets: Vec<usize> = rg
            .column_compressed_sizes
            .iter()
            .scan(0, |acc, s| {
                let offset = *acc;
                *acc += *s as usize;
                Some(offset)
            })
            .collect();

        use rayon::prelude::*;
        let results: Vec<Option<(usize, ColumnArray)>> = column_indices
            .par_iter()
            .map(|&col_idx| -> Result<Option<(usize, ColumnArray)>> {
                if col_idx >= col_offsets.len() {
                    return Ok(None);
                }

                let col_offset = col_offsets[col_idx];
                let col_len = rg.column_compressed_sizes[col_idx] as usize;
                let compressed = &all_data[col_offset..col_offset + col_len];
                let decompressed = Self::decompress_column_block(compressed)?;

                let col_def = &self.footer.schema.columns[col_idx];
                let col_array = decode_column_selected(
                    &decompressed,
                    rg.row_count as usize,
                    col_def,
                    selected_rows,
                )?;
                Ok(Some((col_idx, col_array)))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(results.into_iter().flatten().collect())
    }

    pub fn read_columns_fast(
        &self,
        row_group: usize,
        column_indices: &[usize],
    ) -> Result<crate::types::batch::RecordBatch> {
        self.read_columns_fast_allow_dictionary(row_group, column_indices, false)
    }

    pub fn read_columns_fast_allow_dictionary(
        &self,
        row_group: usize,
        column_indices: &[usize],
        allow_dictionary: bool,
    ) -> Result<crate::types::batch::RecordBatch> {
        self.read_columns_fast_passthrough(row_group, column_indices, allow_dictionary, false)
    }

    pub fn read_columns_fast_passthrough(
        &self,
        row_group: usize,
        column_indices: &[usize],
        allow_dictionary: bool,
        allow_runs: bool,
    ) -> Result<crate::types::batch::RecordBatch> {
        let columns_data = self.read_row_group_batch_passthrough(
            row_group,
            column_indices,
            allow_dictionary,
            allow_runs,
        )?;

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
            return Err(crate::common::error::TaibaiError::Internal(format!(
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
        for (i, &_col_idx) in column_indices.iter().enumerate() {
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
                    data_type: self.footer.schema.columns[idx].data_type,
                    nullable: self.footer.schema.columns[idx].nullable,
                    scale: self.footer.schema.columns[idx].scale,
                    precision: self.footer.schema.columns[idx].precision,
                    auto_increment: self.footer.schema.columns[idx].auto_increment,
                    primary_key: self.footer.schema.columns[idx].primary_key,
                    default_value: self.footer.schema.columns[idx].default_value.clone(),
                    column_id: self.footer.schema.columns[idx].column_id,
                    added_at_micros: None,
                    previous_names: self.footer.schema.columns[idx].previous_names.clone(),
                })
                .collect(),
        );

        crate::types::batch::RecordBatch::try_new(result_schema, selected_columns)
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
        self.read_row_group_with_projection_and_dictionary(row_group, projection, false)
    }

    pub fn read_row_group_with_projection_and_dictionary(
        &self,
        row_group: usize,
        projection: &[usize],
        allow_dictionary: bool,
    ) -> Result<crate::types::batch::RecordBatch> {
        self.read_row_group_with_projection_passthrough(
            row_group,
            projection,
            allow_dictionary,
            false,
        )
    }

    pub fn read_row_group_with_projection_passthrough(
        &self,
        row_group: usize,
        projection: &[usize],
        allow_dictionary: bool,
        allow_runs: bool,
    ) -> Result<crate::types::batch::RecordBatch> {
        let mut columns = Vec::with_capacity(projection.len());
        let mut col_defs = Vec::with_capacity(projection.len());
        for &col_idx in projection {
            columns.push(self.read_column_passthrough(
                row_group,
                col_idx,
                allow_dictionary,
                allow_runs,
            )?);
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
        stats: Option<&mut ScanStats>,
    ) -> Result<Vec<crate::types::batch::RecordBatch>> {
        self.scan_internal(batch_size, predicate, None, stats)
    }

    pub fn scan_with_projection_and_stats(
        &self,
        batch_size: usize,
        predicate: Option<&RowGroupPredicate>,
        projection: Option<&[usize]>,
        stats: Option<&mut ScanStats>,
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
        let delete_bitmap = self.delete_bitmap();
        let bitmap_active = !delete_bitmap.is_empty();
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
                batch = delete_bitmap::filter_batch(batch, rg_first_row, delete_bitmap)?;
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
        // Composed bitmap (footer + transaction overlay): the overlay exists
        // precisely so in-txn deferred deletes are visible to scans, and the
        // manager opens COUNT readers with the overlay installed.
        let delete_bitmap = self.delete_bitmap();
        let bitmap_active = !delete_bitmap.is_empty();

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
                    if zm.min_value == *value && zm.max_value == *value && !bitmap_active {
                        total_count += zm.row_count - zm.null_count;
                        continue;
                    }
                }
            }

            let match_count = if bitmap_active {
                let col = self.read_column(rg_idx, column_idx)?;
                let raw_count = col.count_eq(value);
                if raw_count == 0 {
                    continue;
                }
                let rg_start_row = self.row_group_start_row(rg_idx);
                let deleted = col.count_eq_deleted(value, rg_start_row, delete_bitmap);
                raw_count.saturating_sub(deleted)
            } else {
                self.count_eq_raw(rg_idx, column_idx, value)?
            };

            total_count += match_count;
        }

        Ok(total_count)
    }

    fn count_eq_raw(
        &self,
        row_group: usize,
        column_idx: usize,
        value: &ScalarValue,
    ) -> Result<u64> {
        let rg = &self.footer.row_groups[row_group];
        let col_def = &self.footer.schema.columns[column_idx];

        // Route through read_column_raw so the COUNT path gets the same
        // footer-offset bounds checking and block CRC verification as every
        // other read: the previous inline slice panicked on a corrupted
        // offset and skipped checksums entirely.
        let decompressed = self.read_column_raw(row_group, column_idx)?;

        crate::storage::codec::predicate_scan::count_eq_from_raw(
            &decompressed,
            rg.row_count as usize,
            col_def,
            value,
        )
    }
}

/// Translate catalog column indices into the column indices of the given
/// segment (footer) schema by name — the schema-parameterized core of
/// [`SegmentReader::align_indices`]. Callers that prune against sidecar
/// metadata (whose per-column arrays follow the same footer order but have
/// no `SegmentReader` at hand) use this directly.
pub fn align_indices_against_schema(
    table_schema: &crate::types::batch::Schema,
    footer_schema: &crate::types::batch::Schema,
    indices: &[usize],
) -> Result<Vec<usize>> {
    indices
        .iter()
        .map(|&catalog_idx| {
            let column = table_schema.columns.get(catalog_idx).ok_or_else(|| {
                TaibaiError::Internal(format!(
                    "catalog column index {catalog_idx} out of range"
                ))
            })?;
            let mut names = Vec::with_capacity(column.previous_names.len() + 1);
            names.push(column.name.clone());
            names.extend(column.previous_names.iter().cloned());
            names
                .iter()
                .find_map(|name| footer_schema.index_of(name))
                .ok_or_else(|| {
                    TaibaiError::Storage(format!(
                        "column '{}' not found in segment schema",
                        column.name
                    ))
                })
        })
        .collect()
}

/// Re-wrap a projected batch read from a segment with the CATALOG column
/// defs of the requested columns. Column order (and therefore every
/// positional mapping over the request list) is unchanged; only the schema
/// names/metadata are re-anchored, so downstream WHERE evaluation resolves
/// columns by their current catalog names even when the footer predates a
/// RENAME COLUMN.
fn batch_with_catalog_defs(
    batch: crate::types::batch::RecordBatch,
    table_schema: &crate::types::batch::Schema,
    indices: &[usize],
) -> Result<crate::types::batch::RecordBatch> {
    let same_defs = indices
        .iter()
        .zip(batch.schema.columns.iter())
        .all(|(&idx, def)| table_schema.columns.get(idx) == Some(def));
    if same_defs {
        return Ok(batch);
    }
    let col_defs = indices
        .iter()
        .map(|&idx| {
            table_schema.columns.get(idx).cloned().ok_or_else(|| {
                TaibaiError::Internal(format!(
                    "catalog column index {idx} out of range"
                ))
            })
        })
        .collect::<Result<Vec<_>>>()?;
    crate::types::batch::RecordBatch::try_new(
        crate::types::batch::Schema::new(col_defs),
        batch.columns,
    )
}

/// One column's worth of the value rows written before an ALTER ADD COLUMN
/// logically carry: the column's DEFAULT (CurrentTimestamp pinned to the
/// ALTER instant, like the SELECT-side default fill), or typed NULL when the
/// column declares no default. Used by the whole-row-group aligned read so a
/// segment rewrite does not silently turn defaulted columns into NULLs.
pub(crate) fn default_value_column(
    col_def: &crate::types::batch::ColumnDef,
    row_count: usize,
) -> Result<crate::types::array::ColumnArray> {
    let value = column_default_value(col_def);
    let mut column = crate::types::array::ColumnArray::empty(col_def.data_type);
    if value.is_null() {
        for _ in 0..row_count {
            column.push_null_value();
        }
    } else {
        for _ in 0..row_count {
            column.push(&value)?;
        }
    }
    Ok(column)
}

/// The scalar every row of a segment predating `col_def` logically carries:
/// the column's DEFAULT literal (CurrentTimestamp pinned to the ALTER
/// instant), or NULL when no default is declared. Shared by the aligned
/// default-fill read and the COUNT fast paths, so predicate evaluation can
/// never disagree with the materialized fill.
pub(crate) fn column_default_value(
    col_def: &crate::types::batch::ColumnDef,
) -> crate::types::scalar::ScalarValue {
    match &col_def.default_value {
        Some(crate::types::batch::ColumnDefaultValue::Literal(value)) => value.clone(),
        Some(crate::types::batch::ColumnDefaultValue::CurrentTimestamp) => {
            ScalarValue::Timestamp(Some(
                col_def
                    .added_at_micros
                    .unwrap_or_else(now_micros_for_default),
            ))
        }
        None => ScalarValue::null_for_type(col_def.data_type),
    }
}

/// Whether ANY of the catalog-space `indices` names a column this footer
/// schema predates (neither its current nor any previous name exists in the
/// footer). Such segments carry the column's DEFAULT on every row — the
/// caller decides the default-fill semantics; an out-of-range catalog index
/// reports `false` so the regular alignment surfaces its own error.
pub fn any_column_absent_in_schema(
    table_schema: &crate::types::batch::Schema,
    footer_schema: &crate::types::batch::Schema,
    indices: &[usize],
) -> bool {
    if table_schema.num_columns() == 0 {
        return false;
    }
    indices.iter().any(|&catalog_idx| {
        let Some(column) = table_schema.columns.get(catalog_idx) else {
            return false;
        };
        let mut names = Vec::with_capacity(column.previous_names.len() + 1);
        names.push(column.name.clone());
        names.extend(column.previous_names.iter().cloned());
        !names
            .iter()
            .any(|name| footer_schema.index_of(name).is_some())
    })
}

fn now_micros_for_default() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as i64
}

fn split_batch(
    batch: &crate::types::batch::RecordBatch,
    start: usize,
    end: usize,
) -> crate::types::batch::RecordBatch {
    let mut columns = Vec::with_capacity(batch.num_columns());
    for i in 0..batch.num_columns() {
        columns.push(split_column(
            batch.column(i).expect("column exists"),
            start,
            end,
        ));
    }
    crate::types::batch::RecordBatch::try_new(batch.schema.clone(), columns)
        .expect("split_batch: sliced columns match the batch schema")
}

fn split_column(col: &ColumnArray, start: usize, end: usize) -> ColumnArray {
    let mut new_col = ColumnArray::empty(col.data_type());
    for i in start..end {
        new_col.push(&col.scalar(i)).unwrap();
    }
    new_col
}

// ---------------------------------------------------------------------------
// Delete-footer rewrite journal
//
// `<segment-file>.del` records the exact bytes of a pending in-place footer
// rewrite before truncation starts, so a crash mid-rewrite can be repaired
// on the next open instead of silently losing the segment's data.
// Layout: magic(8) | data_end u64 | footer_len u32 | footer bytes | crc32 u32
// ---------------------------------------------------------------------------

const DELETE_JOURNAL_MAGIC: &[u8; 8] = b"TBDELJ01";

fn pending_delete_journal_path(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(".del");
    PathBuf::from(name)
}

fn write_delete_journal(journal: &Path, data_end: u64, footer: &[u8]) -> Result<()> {
    let mut body = Vec::with_capacity(footer.len() + 12);
    body.extend_from_slice(&data_end.to_le_bytes());
    body.extend_from_slice(&(footer.len() as u32).to_le_bytes());
    body.extend_from_slice(footer);
    let mut crc = crc32fast::Hasher::new();
    crc.update(&body);
    let crc = crc.finalize();

    let tmp = {
        let mut name = journal.as_os_str().to_os_string();
        name.push(".tmp");
        PathBuf::from(name)
    };
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&tmp)?;
    file.write_all(DELETE_JOURNAL_MAGIC)?;
    file.write_all(&body)?;
    file.write_all(&crc.to_le_bytes())?;
    file.sync_all()?;
    drop(file);
    // Journal visibility must be atomic: fs::rename replaces the destination
    // on both platforms (MoveFileEx(REPLACE_EXISTING) semantics on Windows).
    fs::rename(&tmp, journal)?;
    Ok(())
}

fn read_delete_journal(journal: &Path) -> Result<Option<(u64, Vec<u8>)>> {
    let Ok(bytes) = fs::read(journal) else {
        return Ok(None);
    };
    if bytes.len() < 8 + 12 + 4 || !bytes.starts_with(DELETE_JOURNAL_MAGIC) {
        return Ok(None);
    }
    let body = &bytes[8..bytes.len() - 4];
    let crc = u32::from_le_bytes(
        bytes[bytes.len() - 4..]
            .try_into()
            .expect("crc slice has fixed length"),
    );
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(body);
    if hasher.finalize() != crc {
        return Ok(None);
    }
    let data_end = u64::from_le_bytes(body[0..8].try_into().expect("u64 slice"));
    let footer_len = u32::from_le_bytes(body[8..12].try_into().expect("u32 slice")) as usize;
    if body.len() < 12 + footer_len {
        return Ok(None);
    }
    Ok(Some((data_end, body[12..12 + footer_len].to_vec())))
}

/// Whether a footer-write failure is worth retrying: transient sharing
/// violations (Windows mapped-file truncation) clear once in-flight readers
/// drop their mmap handles.
fn error_is_retryable_file_conflict(err: &TaibaiError) -> bool {
    let text = err.to_string();
    text.contains("Access is denied")
        || text.contains("used by another process")
        || text.contains("being used by another")
        || text.contains("Permission denied")
        || text.contains("share")
        || text.contains("resource temporarily unavailable")
}
