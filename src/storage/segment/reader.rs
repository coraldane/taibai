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
            .map(
                |(clause, column_index)| crate::storage::partition::pruning::RangeClause {
                    column_index,
                    op: clause.op,
                    value: clause.value.clone(),
                },
            )
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
        mode: FooterRewriteMode,
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

        // The rebuilt footer keeps the row-group prefix (including the
        // per-row-group CRC extensions) verbatim — the column blocks are
        // untouched — and replaces the delete section, so the whole-footer
        // checksum must be recomputed, never copied.
        let res_offset = reserved_section_offset(&source.footer)?;
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
        // the rewrite recoverable — see `repair_pending_footer_rewrite`
        // (V01) and `recover_statement_footer_journals` (V02).
        let journal = pending_delete_journal_path(path);
        let deferred = match mode {
            FooterRewriteMode::Immediate => {
                write_delete_journal(&journal, source.data_end, &new_footer)?;
                None
            }
            FooterRewriteMode::Deferred(image_checks) => {
                write_delete_journal_v2(
                    &journal,
                    source.data_end,
                    &new_footer,
                    // Undo: the exact pre-rewrite footer bytes and their
                    // truncation point, both read from the live segment.
                    source.data_end,
                    &source.footer,
                    &image_checks,
                )?;
                Some(())
            }
        };

        Self::apply_footer_bytes_with_retry(path, source.data_end, &new_footer)?;
        if deferred.is_none() {
            let _ = fs::remove_file(&journal);
        }
        // A deferred journal survives until the statement's other legs are
        // durable — the caller clears it (`clear_deferred_delete_journal`)
        // or startup recovery rolls the footer back.
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
        if journal_is_deferred(&journal) {
            // A deferred (V02) journal belongs to a possibly in-flight
            // multi-leg statement: only startup recovery may judge it, never
            // a concurrent discovery pass.
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
                TaibaiError::Internal(format!("catalog column index {catalog_idx} out of range"))
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
                TaibaiError::Internal(format!("catalog column index {idx} out of range"))
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
    constant_column(col_def.data_type, &value, row_count)
}

/// A full-length column whose every row holds the same scalar, built through
/// the run-length variant: one `(value, row_count)` run instead of a
/// row-by-row `push` loop. The Rle column is flattened before it leaves this
/// function — on purpose, not a lost optimization: several consumers of
/// default-filled columns do not understand `ColumnArray::Rle` and live in
/// modules this crate cannot change here (the predicate kernels
/// `eq_vec`/`compare_scalar_at` route through `numeric_i64_at`, which has no
/// Rle arm and answers `None`, turning predicates into all-false masks; the
/// delete-bitmap gather `take`/`gather_into` has no Rle arm and panics;
/// `count_eq` answers 0). `flatten_in_place` expands the single run with a
/// memset-speed `repeat_n` fill plus a bitmap memcpy, which is still far
/// cheaper than the old per-row push loop, and callers that never touch the
/// values pay only the O(1) run construction of the intermediate.
///
/// Types the Rle variant cannot represent (Bool, UInt64, floats, VarChar,
/// Blob, Vector — `expand_runs` covers only the exact-i64-view types) keep
/// the push-loop construction; a NULL default rides the Rle path with an
/// all-null bitmap, matching `push_null_value` output exactly after the
/// flatten.
pub(crate) fn constant_column(
    data_type: crate::protocol::types::DataType,
    value: &ScalarValue,
    row_count: usize,
) -> Result<crate::types::array::ColumnArray> {
    // The RLE fast path requires the TYPE to be expand_runs-representable —
    // including for NULL values: a NULL default on a VarChar/Float/Bool/…
    // column used to take the run path and hit expand_runs' unreachable!()
    // on flatten. scalar_i64_view is consulted on the TYPE pair (a NULL
    // scalar of a representable type returns Some(0) via the guard below).
    let run_value = if value.is_null() {
        null_run_representable(data_type).then_some(0i64)
    } else {
        scalar_i64_view(data_type, value)
    };
    if let Some(bits) = run_value {
        if let Ok(run_len) = u32::try_from(row_count) {
            let bitmap = if value.is_null() {
                crate::types::bitmap::NullBitmap::from_bools(&vec![true; row_count])
            } else {
                crate::types::bitmap::NullBitmap::new_valid(row_count)
            };
            let mut column = crate::types::array::ColumnArray::rle(
                vec![(bits, run_len)],
                data_type,
                bitmap,
                row_count,
            );
            column.flatten_in_place();
            return Ok(column);
        }
    }
    let mut column = crate::types::array::ColumnArray::empty(data_type);
    if value.is_null() {
        for _ in 0..row_count {
            column.push_null_value();
        }
    } else {
        for _ in 0..row_count {
            column.push(value)?;
        }
    }
    Ok(column)
}

/// The exact i64 view of a scalar for a run-decodable column type, or `None`
/// when the (type, scalar) pair has no Rle representation. Only pairs that
/// `ColumnArray::expand_runs` can rebuild are accepted — anything else would
/// hit its `unreachable!()` on flatten.
/// Whether a run of NULLs of this type can be flattened by `expand_runs`
/// (the payload bits are irrelevant for NULL; only the type matters).
fn null_run_representable(data_type: crate::protocol::types::DataType) -> bool {
    use crate::protocol::types::DataType as Dt;
    matches!(
        data_type,
        Dt::Int8
            | Dt::Int16
            | Dt::Int32
            | Dt::Int64
            | Dt::UInt8
            | Dt::UInt16
            | Dt::UInt32
            | Dt::Decimal
            | Dt::Date
            | Dt::Time
            | Dt::Timestamp
            | Dt::Interval
    )
}

fn scalar_i64_view(
    data_type: crate::protocol::types::DataType,
    value: &ScalarValue,
) -> Option<i64> {
    match (data_type, value) {
        (crate::protocol::types::DataType::Int8, ScalarValue::Int8(Some(v))) => Some(*v as i64),
        (crate::protocol::types::DataType::Int16, ScalarValue::Int16(Some(v))) => Some(*v as i64),
        (crate::protocol::types::DataType::Int32, ScalarValue::Int32(Some(v))) => Some(*v as i64),
        (crate::protocol::types::DataType::Int64, ScalarValue::Int64(Some(v))) => Some(*v),
        (crate::protocol::types::DataType::UInt8, ScalarValue::UInt8(Some(v))) => Some(*v as i64),
        (crate::protocol::types::DataType::UInt16, ScalarValue::UInt16(Some(v))) => Some(*v as i64),
        (crate::protocol::types::DataType::UInt32, ScalarValue::UInt32(Some(v))) => Some(*v as i64),
        (crate::protocol::types::DataType::Decimal, ScalarValue::Decimal(Some(v))) => Some(*v),
        (crate::protocol::types::DataType::Date, ScalarValue::Date(Some(v))) => Some(*v as i64),
        (crate::protocol::types::DataType::Time, ScalarValue::Time(Some(v))) => Some(*v),
        (crate::protocol::types::DataType::Timestamp, ScalarValue::Timestamp(Some(v))) => Some(*v),
        (crate::protocol::types::DataType::Interval, ScalarValue::Interval(Some(v))) => Some(*v),
        _ => None,
    }
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
// `<segment-file>.del` records the exact bytes of a pending in-place header
// rewrite before truncation starts, so a crash mid-rewrite can be repaired
// on the next open instead of silently losing the segment's data.
//
// V01 layout (single-leg statements, e.g. DELETE):
//   magic(8) | data_end u64 | footer_len u32 | footer bytes | crc32 u32
// Removed as soon as the new footer lands; discovery repairs forward-only.
//
// V02 layout (multi-leg statements, e.g. UPDATE's tombstone leg):
//   magic(8) | new_data_end u64 | new_footer_len u32 | new_footer
//          | old_data_end u64 | old_footer_len u32 | old_footer
//          | checks u32 | per check: uuid(16) | part_len u32 | part
//                                   | expected_rows u64 | crc32 u32
// The old footer is the UNDO; the image checks are the statement's
// durability watermark (effect verification, the flush-intent pattern).
// The journal survives until the caller clears it after the other legs are
// durable; `recover_statement_footer_journals` rolls the footer back unless
// the watermark is met. Online discovery never touches a V02 journal.
// ---------------------------------------------------------------------------

const DELETE_JOURNAL_MAGIC: &[u8; 8] = b"TBDELJ01";
const DELETE_JOURNAL_MAGIC_V2: &[u8; 8] = b"TBDELJ02";

/// One leg of a multi-leg statement's durability watermark: the reinserted
/// image rows of an UPDATE become durable in this memtable's tail at commit;
/// recovery compares frame-walked tail rows against `expected_rows` to
/// decide roll-forward vs roll-back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImageDurabilityCheck {
    pub memtable_id: uuid::Uuid,
    pub partition: String,
    pub expected_rows: u64,
}

/// Journal lifecycle of a footer rewrite.
#[derive(Debug, Clone, Default)]
pub enum FooterRewriteMode {
    /// Single-leg statement: the journal is removed as soon as the footer
    /// lands (V01); discovery's repair completes it forward-only.
    #[default]
    Immediate,
    /// Multi-leg statement: the journal also records the OLD footer (undo)
    /// and the image watermark (V02) and survives until the caller clears
    /// it (or startup recovery rolls it back).
    Deferred(Vec<ImageDurabilityCheck>),
}

fn pending_delete_journal_path(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(".del");
    PathBuf::from(name)
}

/// Remove a deferred journal after the statement's other legs are durable.
/// Best-effort: a leftover journal is re-judged (and removed) by startup
/// recovery, so a failed unlink cannot corrupt anything.
pub fn clear_deferred_delete_journal(segment_path: &Path) {
    let journal = pending_delete_journal_path(segment_path);
    let _ = fs::remove_file(&journal);
}

/// In-process twin of [`recover_statement_footer_journals`]'s roll-forward
/// judgement: remove the deferred journal at `segment_path` only when the
/// statement it records is provably complete — the embedded footer parses
/// and every image watermark is durable.
///
/// Callers that just made their own images durable (REPLACE after its tail
/// commit wait) pass through in the common uncontended case. When a LATER
/// statement on the same segment has already replaced the journal, the
/// judgement runs on THAT statement's watermark, so a racing statement's
/// undo is never discarded before its own images are durable — the same
/// criterion startup recovery would apply to the leftover.
///
/// Must be serialized against journal writers (the table mutation lock):
/// without it a check-then-remove could delete a journal that a concurrent
/// rewrite just swapped in. Startup recovery is naturally exclusive.
///
/// A journal with an EMPTY check list is never removed here: an empty
/// watermark cannot prove durability, so the decision is left to startup
/// recovery (whose conservative roll-back only trades a lost statement for
/// a consistent table).
pub fn clear_deferred_delete_journal_when_satisfied(table_dir: &Path, segment_path: &Path) {
    let journal_path = pending_delete_journal_path(segment_path);
    let Ok(Some(journal)) = read_delete_journal_v2(&journal_path) else {
        return;
    };
    let satisfied = !journal.image_checks.is_empty()
        && SegmentReader::read_dat_footer_source(segment_path, None).is_ok()
        && journal.image_checks.iter().all(|check| {
            crate::storage::core::durable_tail::durable_tail_rows_for_memtable(
                table_dir,
                check.memtable_id,
            ) >= check.expected_rows
        });
    if satisfied {
        let _ = fs::remove_file(&journal_path);
    }
}

/// Clear the deferred journal of a statement that just COMPLETED (its rows
/// are durable in a tail, or a failed tail commit was already rescued by a
/// segment flush, which the tail-only watermark cannot see). The journal is
/// removed when its redo is provably THIS statement's rewrite — the delete
/// section of the recorded new footer equals the `(bitmap, count)` the
/// caller wrote — and ownership is unforgeable: a later statement rewriting
/// the same segment always adds tombstones, so its absolute state differs.
/// A journal left by a LATER statement is judged on that statement's own
/// watermark (the roll-forward criterion).
///
/// Same serialization requirement as
/// [`clear_deferred_delete_journal_when_satisfied`]: callers hold the table
/// mutation lock.
pub fn clear_deferred_delete_journal_for_statement(
    table_dir: &Path,
    segment_path: &Path,
    new_bitmap: &[u8],
    new_deleted_count: u64,
) {
    let journal_path = pending_delete_journal_path(segment_path);
    let Ok(Some(journal)) = read_delete_journal_v2(&journal_path) else {
        return;
    };
    let ours = parse_segment_footer(&journal.new_footer)
        .map(|footer| {
            footer.deleted_row_count == new_deleted_count && footer.delete_bitmap == new_bitmap
        })
        .unwrap_or(false);
    if ours {
        let _ = fs::remove_file(&journal_path);
        return;
    }
    clear_deferred_delete_journal_when_satisfied(table_dir, segment_path);
}

fn write_delete_journal(journal: &Path, data_end: u64, footer: &[u8]) -> Result<()> {
    let mut body = Vec::with_capacity(footer.len() + 12);
    body.extend_from_slice(&data_end.to_le_bytes());
    body.extend_from_slice(&(footer.len() as u32).to_le_bytes());
    body.extend_from_slice(footer);
    write_journal_file(journal, DELETE_JOURNAL_MAGIC, &body)
}

fn write_delete_journal_v2(
    journal: &Path,
    new_data_end: u64,
    new_footer: &[u8],
    old_data_end: u64,
    old_footer: &[u8],
    image_checks: &[ImageDurabilityCheck],
) -> Result<()> {
    let mut body = Vec::with_capacity(new_footer.len() + old_footer.len() + 28);
    body.extend_from_slice(&new_data_end.to_le_bytes());
    body.extend_from_slice(&(new_footer.len() as u32).to_le_bytes());
    body.extend_from_slice(new_footer);
    body.extend_from_slice(&old_data_end.to_le_bytes());
    body.extend_from_slice(&(old_footer.len() as u32).to_le_bytes());
    body.extend_from_slice(old_footer);
    body.extend_from_slice(&(image_checks.len() as u32).to_le_bytes());
    for check in image_checks {
        body.extend_from_slice(check.memtable_id.as_bytes());
        body.extend_from_slice(&(check.partition.len() as u32).to_le_bytes());
        body.extend_from_slice(check.partition.as_bytes());
        body.extend_from_slice(&check.expected_rows.to_le_bytes());
    }
    write_journal_file(journal, DELETE_JOURNAL_MAGIC_V2, &body)
}

/// Shared tmp+fsync+rename durability: the journal must be fully durable and
/// atomically visible before the segment's own bytes are touched.
fn write_journal_file(journal: &Path, magic: &[u8; 8], body: &[u8]) -> Result<()> {
    let mut crc = crc32fast::Hasher::new();
    crc.update(body);
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
    file.write_all(magic)?;
    file.write_all(body)?;
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
    if bytes.starts_with(DELETE_JOURNAL_MAGIC_V2) {
        // Deferred journal: only startup recovery may judge it.
        return Ok(None);
    }
    if bytes.len() < 8 + 12 + 4 || !bytes.starts_with(DELETE_JOURNAL_MAGIC) {
        return Ok(None);
    }
    decode_delete_journal_v1(&bytes)
}

fn decode_delete_journal_v1(bytes: &[u8]) -> Result<Option<(u64, Vec<u8>)>> {
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

/// The decoded form of a deferred (V02) journal.
pub struct DeferredDeleteJournal {
    pub segment_path: PathBuf,
    pub new_data_end: u64,
    pub new_footer: Vec<u8>,
    pub old_data_end: u64,
    pub old_footer: Vec<u8>,
    pub image_checks: Vec<ImageDurabilityCheck>,
}

fn read_delete_journal_v2(journal: &Path) -> Result<Option<DeferredDeleteJournal>> {
    let Ok(bytes) = fs::read(journal) else {
        return Ok(None);
    };
    if !bytes.starts_with(DELETE_JOURNAL_MAGIC_V2) {
        return Ok(None);
    }
    // A journal always sits next to its segment: "<segment>.del".
    let segment_path = {
        let text = journal.to_string_lossy();
        let stripped = text.strip_suffix(".del").unwrap_or(&text);
        PathBuf::from(stripped)
    };
    if bytes.len() < 8 + 4 {
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
    let mut cursor = 0usize;
    let take_u64 = |cursor: &mut usize| -> Option<u64> {
        let next = cursor.checked_add(8)?;
        let slice = body.get(*cursor..next)?;
        *cursor = next;
        Some(u64::from_le_bytes(slice.try_into().expect("u64 slice")))
    };
    let take_u32 = |cursor: &mut usize| -> Option<u32> {
        let next = cursor.checked_add(4)?;
        let slice = body.get(*cursor..next)?;
        *cursor = next;
        Some(u32::from_le_bytes(slice.try_into().expect("u32 slice")))
    };
    let Some(new_data_end) = take_u64(&mut cursor) else {
        return Ok(None);
    };
    let Some(new_len) = take_u32(&mut cursor).map(|len| len as usize) else {
        return Ok(None);
    };
    let Some(new_footer) = body.get(cursor..cursor + new_len).map(|s| s.to_vec()) else {
        return Ok(None);
    };
    cursor += new_len;
    let Some(old_data_end) = take_u64(&mut cursor) else {
        return Ok(None);
    };
    let Some(old_len) = take_u32(&mut cursor).map(|len| len as usize) else {
        return Ok(None);
    };
    let Some(old_footer) = body.get(cursor..cursor + old_len).map(|s| s.to_vec()) else {
        return Ok(None);
    };
    cursor += old_len;
    let Some(check_count) = take_u32(&mut cursor).map(|len| len as usize) else {
        return Ok(None);
    };
    let mut image_checks = Vec::with_capacity(check_count);
    for _ in 0..check_count {
        let Some(uuid_bytes) = body.get(cursor..cursor + 16).map(|s| s.to_vec()) else {
            return Ok(None);
        };
        cursor += 16;
        let memtable_id = uuid::Uuid::from_slice(&uuid_bytes).map_err(|error| {
            TaibaiError::Storage(format!("bad memtable id in journal: {error}"))
        })?;
        let Some(part_len) = take_u32(&mut cursor).map(|len| len as usize) else {
            return Ok(None);
        };
        let Some(part_slice) = body.get(cursor..cursor + part_len) else {
            return Ok(None);
        };
        let partition = String::from_utf8_lossy(part_slice).into_owned();
        cursor += part_len;
        let Some(expected_rows) = take_u64(&mut cursor) else {
            return Ok(None);
        };
        image_checks.push(ImageDurabilityCheck {
            memtable_id,
            partition,
            expected_rows,
        });
    }
    Ok(Some(DeferredDeleteJournal {
        segment_path,
        new_data_end,
        new_footer,
        old_data_end,
        old_footer,
        image_checks,
    }))
}

/// Whether the journal at `journal` carries the deferred (V02) magic.
fn journal_is_deferred(journal: &Path) -> bool {
    let Ok(mut file) = fs::File::open(journal) else {
        return false;
    };
    let mut magic = [0u8; 8];
    use std::io::Read as _;
    file.read_exact(&mut magic)
        .is_ok_and(|_| magic == *DELETE_JOURNAL_MAGIC_V2)
}

/// Startup-time judgement of deferred (V02) delete-footer journals, one
/// table directory at a time. Single-writer per data dir means nothing is
/// in flight here, so the decision is safe to make now:
///
/// - embedded footer unreadable → the rewrite was interrupted mid-apply →
///   roll BACK to the journal's old footer (the statement cannot have
///   completed);
/// - footer readable and every image check's durable-tail watermark met →
///   the statement completed after the rewrite → roll FORWARD (journal is
///   stale, just remove it);
/// - footer readable but any watermark unmet → the crash landed between
///   the tombstones and the image commit → roll BACK, restoring the
///   matched rows (the statement is lost, the table is consistent).
///
/// Every performed rollback is recorded in `<data_dir>/.taibai-footer-
/// rollbacks` (durable before the journal is removed) so engine startup can
/// compensate standbys that already applied the pre-rollback
/// DeleteFooterRewrite event — see `drain_footer_rollback_compensations`.
///
/// Effect verification instead of step tracking mirrors the flush-intent
/// arbitration in the durable tail.
pub fn recover_statement_footer_journals(table_dir: &Path) {
    let Ok(entries) = fs::read_dir(table_dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if !name.ends_with(".del") {
            continue;
        }
        let journal = match read_delete_journal_v2(&path) {
            Ok(Some(journal)) => journal,
            Ok(None) => continue, // V01 (or absent): discovery's repair owns it
            Err(error) => {
                tracing::warn!(
                    journal = %path.display(),
                    error = %error,
                    "statement journal unreadable; leaving it for discovery repair"
                );
                continue;
            }
        };
        let segment = &journal.segment_path;
        let roll_forward = SegmentReader::read_dat_footer_source(segment, None).is_ok()
            && !journal.image_checks.is_empty()
            && journal.image_checks.iter().all(|check| {
                crate::storage::core::durable_tail::durable_tail_rows_for_memtable(
                    table_dir,
                    check.memtable_id,
                ) >= check.expected_rows
            });
        if roll_forward {
            tracing::info!(
                segment = %segment.display(),
                "statement journal: tombstone rewrite confirmed by durable images; rolling forward"
            );
        } else {
            tracing::warn!(
                segment = %segment.display(),
                checks = journal.image_checks.len(),
                "statement journal: images not durable; rolling footer back to pre-statement state"
            );
            if let Err(error) = SegmentReader::apply_footer_bytes_with_retry(
                segment,
                journal.old_data_end,
                &journal.old_footer,
            ) {
                tracing::error!(
                    segment = %segment.display(),
                    error = %error,
                    "statement journal rollback failed; journal kept for the next startup"
                );
                continue;
            }
            // Record the rollback BEFORE removing the journal, and durably:
            // a crash in between leaves both, and the next startup judges the
            // (idempotent) rollback again and re-records it.
            record_footer_rollback(table_dir, segment);
        }
        let _ = fs::remove_file(&path);
    }
}

// ---------------------------------------------------------------------------
// Standby compensation for rolled-back footer rewrites
//
// A primary may have shipped a DeleteFooterRewrite event for a rewrite that
// startup recovery subsequently rolled back; without compensation the
// standby keeps the tombstones forever. Recovery records each rollback in
// `<data_dir>/.taibai-footer-rollbacks` and the engine, once RepLog is
// wired, drains the log and re-emits the segment's current (restored)
// footer state as an idempotent absolute-bitmap event.
// ---------------------------------------------------------------------------

/// Path of the rollback compensation log under a data directory.
pub fn footer_rollback_log_path(data_dir: &Path) -> PathBuf {
    data_dir.join(".taibai-footer-rollbacks")
}

/// Append one durable `db\ttable\tsegment_path` record for a rollback the
/// recovery just performed. Best-effort beyond the rollback itself: a lost
/// record only delays standby convergence (the standby diverges until
/// re-bootstrapped), it cannot corrupt the primary.
fn record_footer_rollback(table_dir: &Path, segment_path: &Path) {
    let Some(table) = table_dir.file_name().and_then(|name| name.to_str()) else {
        return;
    };
    let Some(db_dir) = table_dir.parent() else {
        return;
    };
    let Some(db) = db_dir.file_name().and_then(|name| name.to_str()) else {
        return;
    };
    let Some(data_dir) = db_dir.parent() else {
        return;
    };
    let log = footer_rollback_log_path(data_dir);
    let line = format!("{db}\t{table}\t{}\n", segment_path.display());
    let write = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log)
        .and_then(|mut file| {
            use std::io::Write as _;
            file.write_all(line.as_bytes())?;
            file.flush()?;
            file.sync_data()
        });
    if let Err(error) = write {
        tracing::warn!(
            rollback_log = %log.display(),
            error = %error,
            "failed to record footer rollback for standby compensation"
        );
    }
}

/// Drain the rollback compensation log: for every recorded rollback, read
/// the segment's CURRENT (post-recovery) embedded footer state and hand it
/// to `emit` — the caller turns each `(segment, bitmap, deleted_count)`
/// triple into a DeleteFooterRewrite replication event, which is idempotent
/// on standbys because the bitmap is absolute. The log is truncated
/// afterwards. A missing log is a no-op; unreadable records (segment since
/// dropped, footer corrupt) are skipped with a warning.
pub fn drain_footer_rollback_compensations(
    data_dir: &Path,
    mut emit: impl FnMut(&Path, &[u8], u64),
) {
    let log = footer_rollback_log_path(data_dir);
    let Ok(content) = fs::read_to_string(&log) else {
        return;
    };
    for line in content.lines() {
        let mut fields = line.splitn(3, '\t');
        let (Some(db), Some(table), Some(segment)) = (fields.next(), fields.next(), fields.next())
        else {
            tracing::warn!(rollback_log = %log.display(), "skipping malformed rollback record");
            continue;
        };
        let segment_path = PathBuf::from(segment);
        match read_embedded_footer(&segment_path) {
            Ok(source) => match parse_segment_footer(&source.footer) {
                Ok(parsed) => {
                    tracing::info!(
                        db,
                        table,
                        segment = %segment_path.display(),
                        "emitting footer-rollback compensation event"
                    );
                    emit(
                        &segment_path,
                        &parsed.delete_bitmap,
                        parsed.deleted_row_count,
                    );
                }
                Err(error) => tracing::warn!(
                    db,
                    table,
                    segment = %segment_path.display(),
                    error = %error,
                    "rollback compensation: footer unparseable; skipping"
                ),
            },
            Err(error) => tracing::warn!(
                db,
                table,
                segment = %segment_path.display(),
                error = %error,
                "rollback compensation: segment unreadable; skipping"
            ),
        }
    }
    if let Err(error) = fs::write(&log, b"") {
        tracing::warn!(
            rollback_log = %log.display(),
            error = %error,
            "failed to truncate footer rollback log"
        );
    }
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
