use std::fs;
use std::io::{self, Read, Write, Seek, SeekFrom, Cursor};
use std::path::{Path, PathBuf};

use memmap2::Mmap;

use crate::common::error::{MonaxError, Result};
use crate::protocol::types::DataType;
use crate::types::array::ColumnArray;
use crate::types::batch::{ColumnDef, Schema};
use crate::types::bitmap::NullBitmap;
use crate::types::scalar::ScalarValue;

/// Segment magic number: ASCII "TAIX" = bytes 54 41 49 58.
/// Stored little-endian, so on disk the first 4 bytes are `58 49 41 54`.
/// Kept in sync with taibai (`pkg/storage/columnar/types.go: SegmentMagic = 0x54414958`).
pub const SEGMENT_MAGIC: u32 = 0x5441_4958;
pub const SEGMENT_VERSION: u32 = 4;
pub const DEFAULT_ROW_GROUP_SIZE: usize = 8192;
pub const FOOTER_SIZE: usize = 4096;
pub const SEGMENT_HEADER_SIZE: usize = 32;
pub const MAX_SEGMENT_SIZE: u64 = 2 * 1024 * 1024 * 1024; // 2 GB

/// Segment writer: builds .dat files on disk.
pub struct SegmentWriter {
    path: PathBuf,
    file: fs::File,
    schema: Schema,
    row_group_size: usize,
    row_groups: Vec<RowGroupMeta>,
    current_group: Option<BuildingRowGroup>,
    total_rows: u64,
}

/// Per-column statistics for a row group, used for zone-map pruning.
#[derive(Debug, Clone)]
pub struct ZoneMap {
    pub min_value: ScalarValue,
    pub max_value: ScalarValue,
    pub null_count: u64,
    pub row_count: u64,
}

impl ZoneMap {
    fn null_for(dtype: DataType) -> ScalarValue {
        match dtype {
            DataType::Bool => ScalarValue::Bool(None),
            DataType::Int8 => ScalarValue::Int8(None),
            DataType::Int16 => ScalarValue::Int16(None),
            DataType::Int32 => ScalarValue::Int32(None),
            DataType::Int64 => ScalarValue::Int64(None),
            DataType::UInt8 => ScalarValue::UInt8(None),
            DataType::UInt16 => ScalarValue::UInt16(None),
            DataType::UInt32 => ScalarValue::UInt32(None),
            DataType::UInt64 => ScalarValue::UInt64(None),
            DataType::Float32 => ScalarValue::Float32(None),
            DataType::Float64 => ScalarValue::Float64(None),
            DataType::Decimal => ScalarValue::Decimal(None),
            DataType::Date => ScalarValue::Date(None),
            DataType::Time => ScalarValue::Time(None),
            DataType::Timestamp => ScalarValue::Timestamp(None),
            DataType::Interval => ScalarValue::Interval(None),
            DataType::VarChar => ScalarValue::VarChar(None),
            DataType::Blob => ScalarValue::Blob(None),
            DataType::Vector => ScalarValue::Vector(None),
        }
    }
}

/// A comparison operator usable in row-group pruning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeOp {
    Eq,
    Lt,
    Le,
    Gt,
    Ge,
}

/// A single per-column range predicate, used to prune row groups via ZoneMaps.
/// Multiple clauses are combined with AND.
#[derive(Debug, Clone)]
pub struct RangeClause {
    pub column_index: usize,
    pub op: RangeOp,
    pub value: ScalarValue,
}

/// Runtime statistics captured while scanning a segment. Used by EXPLAIN ANALYZE.
#[derive(Debug, Default, Clone)]
pub struct ScanStats {
    pub segments_opened: u64,
    pub row_groups_total: u64,
    pub row_groups_read: u64,
    pub row_groups_pruned: u64,
    pub rows_scanned: u64,
    pub rows_filtered_by_delete_bitmap: u64,
    pub bytes_read_compressed: u64,
}

impl ScanStats {
    pub fn merge(&mut self, other: &ScanStats) {
        self.segments_opened += other.segments_opened;
        self.row_groups_total += other.row_groups_total;
        self.row_groups_read += other.row_groups_read;
        self.row_groups_pruned += other.row_groups_pruned;
        self.rows_scanned += other.rows_scanned;
        self.rows_filtered_by_delete_bitmap += other.rows_filtered_by_delete_bitmap;
        self.bytes_read_compressed += other.bytes_read_compressed;
    }
}

/// A row-group pruning predicate: a conjunction of [RangeClause].
/// Clients construct this from their logical WHERE expression.
#[derive(Debug, Clone, Default)]
pub struct RowGroupPredicate {
    pub clauses: Vec<RangeClause>,
}

impl RowGroupPredicate {
    pub fn new(clauses: Vec<RangeClause>) -> Self {
        Self { clauses }
    }

    pub fn is_empty(&self) -> bool {
        self.clauses.is_empty()
    }

    /// Returns false ONLY if the predicate proves no row in this row group can
    /// match. Returns true on any uncertainty (fail-safe).
    pub fn matches_row_group(&self, zone_maps: &[ZoneMap]) -> bool {
        for c in &self.clauses {
            let Some(zm) = zone_maps.get(c.column_index) else {
                continue;
            };
            // If column is all-NULL in this row group, no equality/range can match.
            if zm.row_count > 0 && zm.null_count == zm.row_count {
                return false;
            }
            if zm.min_value.is_null() || zm.max_value.is_null() {
                continue;
            }
            if !clause_overlaps_range(c, &zm.min_value, &zm.max_value) {
                return false;
            }
        }
        true
    }
}

fn clause_overlaps_range(c: &RangeClause, min_v: &ScalarValue, max_v: &ScalarValue) -> bool {
    match c.op {
        RangeOp::Eq => !scalar_lt(&c.value, min_v) && !scalar_lt(max_v, &c.value),
        RangeOp::Lt => scalar_lt(min_v, &c.value),
        RangeOp::Le => !scalar_lt(&c.value, min_v),
        RangeOp::Gt => scalar_lt(&c.value, max_v),
        RangeOp::Ge => !scalar_lt(max_v, &c.value),
    }
}

/// Metadata for a completed row group. Public so writer paths outside of
/// `SegmentWriter` (e.g. the SQL engine's streaming bulk-insert path in
/// `sql::execution::engine`) can build the same structure and feed it to
/// [`encode_segment_footer`].
#[derive(Debug, Clone)]
pub struct RowGroupMeta {
    pub offset: u64,
    pub length: u64,
    pub row_count: u64,
    pub column_sizes: Vec<u64>,
    pub compressed_sizes: Vec<u64>,
    pub uncompressed_sizes: Vec<u64>,
    pub zone_maps: Vec<ZoneMap>,
}

/// A row group being built in memory.
struct BuildingRowGroup {
    columns: Vec<Vec<u8>>,
    row_count: usize,
    uncompressed_sizes: Vec<u64>,
    zone_mins: Vec<Option<ScalarValue>>,
    zone_maxs: Vec<Option<ScalarValue>>,
    null_counts: Vec<u64>,
}

impl BuildingRowGroup {
    fn new(num_columns: usize) -> Self {
        Self {
            columns: vec![Vec::new(); num_columns],
            row_count: 0,
            uncompressed_sizes: vec![0; num_columns],
            zone_mins: vec![None; num_columns],
            zone_maxs: vec![None; num_columns],
            null_counts: vec![0; num_columns],
        }
    }
}

impl SegmentWriter {
    /// Create a new segment writer.
    pub fn create(path: &Path, schema: Schema, row_group_size: usize) -> Result<Self> {
        let mut file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;

        let header = encode_segment_header();
        file.write_all(&header)?;

        Ok(Self {
            path: path.to_path_buf(),
            file,
            schema,
            row_group_size,
            row_groups: Vec::new(),
            current_group: None,
            total_rows: 0,
        })
    }

    /// Write a record batch into the segment.
    pub fn write_batch(&mut self, batch: &crate::types::batch::RecordBatch) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        let num_cols = self.schema.num_columns();
        if self.current_group.is_none() {
            self.current_group = Some(BuildingRowGroup::new(num_cols));
        }

        let mut remaining = batch.num_rows();
        let mut offset = 0;

        while remaining > 0 {
            // Ensure we have an active row group
            if self.current_group.is_none() {
                self.current_group = Some(BuildingRowGroup::new(num_cols));
            }
            let group = self.current_group.as_mut().unwrap();
            let can_fit = self.row_group_size - group.row_count;
            if can_fit == 0 {
                self.flush_row_group()?;
                self.current_group = Some(BuildingRowGroup::new(num_cols));
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

                let encoded = encode_column(col, offset, to_write)?;
                let uncompressed_len = encoded.len() as u64;

                let compressed = zstd::bulk::compress(&encoded, 3)
                    .map_err(|e| MonaxError::Storage(format!("zstd compress error: {}", e)))?;

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

    /// Finalize the segment: write footer (with empty delete bitmap) and close.
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

        // Write footer offset (last 8 bytes of file)
        self.file.write_all(&(footer_offset as u64).to_le_bytes())?;

        self.file.sync_all()?;

        tracing::debug!(
            path = %self.path.display(),
            rows = self.total_rows,
            row_groups = self.row_groups.len(), deleted = deleted_row_count,
            "segment sealed"
        );

        Ok(())
    }

    fn flush_row_group(&mut self) -> Result<()> {
        let group = self.current_group.take().unwrap();
        let offset = self.file.metadata()?.len();

        let mut column_sizes = Vec::with_capacity(group.columns.len());
        let mut compressed_sizes = Vec::with_capacity(group.columns.len());

        for col_data in &group.columns {
            column_sizes.push(col_data.len() as u64);
            compressed_sizes.push(col_data.len() as u64);
            self.file.write_all(col_data)?;
        }

        let num_cols = self.schema.num_columns();
        let mut zone_maps = Vec::with_capacity(num_cols);
        for col_idx in 0..num_cols {
            let dtype = self.schema.columns[col_idx].data_type;
            let min_v = group.zone_mins[col_idx]
                .clone()
                .unwrap_or_else(|| ZoneMap::null_for(dtype));
            let max_v = group.zone_maxs[col_idx]
                .clone()
                .unwrap_or_else(|| ZoneMap::null_for(dtype));
            zone_maps.push(ZoneMap {
                min_value: min_v,
                max_value: max_v,
                null_count: group.null_counts[col_idx],
                row_count: group.row_count as u64,
            });
        }

        self.row_groups.push(RowGroupMeta {
            offset,
            length: column_sizes.iter().sum(),
            row_count: group.row_count as u64,
            column_sizes,
            compressed_sizes,
            uncompressed_sizes: group.uncompressed_sizes,
            zone_maps,
        });

        Ok(())
    }

    fn encode_footer_with_deletes(&self, delete_bitmap: &[u8], deleted_row_count: u64) -> Result<Vec<u8>> {
        Ok(encode_segment_footer(
            &self.schema,
            self.total_rows,
            &self.row_groups,
            delete_bitmap,
            deleted_row_count,
            "",
        ))
    }
}

/// Encode the segment footer, returning the byte stream that should be
/// appended to the data section and followed by an 8-byte little-endian
/// footer offset. Single source of truth for the on-disk layout, shared
/// between [`SegmentWriter`] and any external writer (e.g. the SQL engine's
/// streaming bulk-insert path).
///
/// `table_name` is informational only; pass `""` if unknown.
pub fn encode_segment_footer(
    schema: &Schema,
    total_rows: u64,
    row_groups: &[RowGroupMeta],
    delete_bitmap: &[u8],
    deleted_row_count: u64,
    table_name: &str,
) -> Vec<u8> {
    let mut buf = Vec::with_capacity(FOOTER_SIZE);

    // Magic + version + generation
    buf.extend_from_slice(&SEGMENT_MAGIC.to_le_bytes());
    buf.extend_from_slice(&SEGMENT_VERSION.to_le_bytes());
    buf.extend_from_slice(&0u64.to_le_bytes()); // generation

    // Row count
    buf.extend_from_slice(&total_rows.to_le_bytes());

    // Row group count
    buf.extend_from_slice(&(row_groups.len() as u32).to_le_bytes());

    // Timestamps
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    buf.extend_from_slice(&now.to_le_bytes()); // created_at
    buf.extend_from_slice(&now.to_le_bytes()); // modified_at

    // Schema
    encode_len_prefixed_str(&mut buf, table_name);
    buf.extend_from_slice(&(schema.columns.len() as u32).to_le_bytes());
    for col in &schema.columns {
        encode_len_prefixed_str(&mut buf, &col.name);
        buf.push(col.data_type as u8);
        buf.push(if col.nullable { 1 } else { 0 });
        buf.extend_from_slice(&(col.scale as u32).to_le_bytes());
        buf.extend_from_slice(&(col.precision as u32).to_le_bytes());
    }

    // Row group metadata
    for rg in row_groups {
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
        for zm in &rg.zone_maps {
            encode_scalar_with_tag(&mut buf, &zm.min_value);
            encode_scalar_with_tag(&mut buf, &zm.max_value);
            buf.extend_from_slice(&zm.null_count.to_le_bytes());
            buf.extend_from_slice(&zm.row_count.to_le_bytes());
        }
    }

    // Reserved: delete-bitmap section (kept for forward compat)
    buf.extend_from_slice(&deleted_row_count.to_le_bytes());
    buf.extend_from_slice(&(delete_bitmap.len() as u32).to_le_bytes());
    if !delete_bitmap.is_empty() {
        buf.extend_from_slice(delete_bitmap);
    }

    buf
}

/// Segment reader: reads .dat files via mmap.
#[allow(dead_code)]
pub struct SegmentReader {
    path: PathBuf,
    mmap: Mmap,
    _file: fs::File,
    footer: SegmentFooter,
}

/// Parsed segment footer.
#[allow(dead_code)]
struct SegmentFooter {
    version: u32,
    row_count: u64,
    row_groups: Vec<RowGroupInfo>,
    schema: Schema,
    deleted_row_count: u64,
    delete_bitmap: Vec<u8>,
}

#[allow(dead_code)]
struct RowGroupInfo {
    offset: u64,
    length: u64,
    row_count: u64,
    column_compressed_sizes: Vec<u64>,
    column_uncompressed_sizes: Vec<u64>,
    zone_maps: Vec<ZoneMap>,
}

impl SegmentReader {
    /// Open a segment file for reading via mmap.
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

        // Validate fixed-size file header (offset 0..32)
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

        // Read the last 8 bytes to get footer offset
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

        // mmap the file
        let mmap = unsafe { Mmap::map(&tmp_file)? };

        // Parse footer: from footer_offset to (file_len - 8)
        let footer_end = file_len as usize - 8;
        let footer_data = &mmap[footer_offset..footer_end];
        let footer = Self::parse_footer(footer_data)?;

        Ok(Self {
            path: path.to_path_buf(),
            mmap,
            _file: tmp_file,
            footer,
        })
    }

    /// Get the schema from the segment.
    pub fn schema(&self) -> &Schema {
        &self.footer.schema
    }

    /// Get the total row count.
    pub fn row_count(&self) -> u64 {
        self.footer.row_count
    }

    /// Get the number of row groups.
    pub fn num_row_groups(&self) -> usize {
        self.footer.row_groups.len()
    }

    /// Return per-column zone maps (min/max/null_count/row_count) for a row group.
    pub fn zone_maps(&self, row_group: usize) -> &[ZoneMap] {
        &self.footer.row_groups[row_group].zone_maps
    }

    /// Number of rows in a given row group.
    pub fn row_group_rows(&self, row_group: usize) -> u64 {
        self.footer.row_groups[row_group].row_count
    }

    /// Number of rows currently marked deleted in this segment (delete bitmap
    /// section is reserved; future delete feature will populate it).
    pub fn deleted_row_count(&self) -> u64 {
        self.footer.deleted_row_count
    }

    /// Raw delete-bitmap bytes; empty if no rows are deleted.
    pub fn delete_bitmap(&self) -> &[u8] {
        &self.footer.delete_bitmap
    }

    /// Returns true if row `row_id` (segment-wide row index, starting at 0) is
    /// marked deleted. Always false today, kept stable for future use.
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

    /// Rewrite only the footer of an existing segment file to install a new
    /// delete bitmap. Data blocks are untouched, so this is O(footer size)
    /// regardless of segment size — the fast path for DELETE.
    ///
    /// The caller MUST ensure no SegmentReader holds an mmap on `path` when
    /// invoking this (Windows will reject the write otherwise).
    pub fn rewrite_footer_with_deletes(
        path: &Path,
        delete_bitmap: &[u8],
        deleted_row_count: u64,
    ) -> Result<()> {
        let footer_bytes = Self::read_footer_bytes(path)?;
        let reserved_offset = Self::reserved_section_offset(&footer_bytes)?;

        let mut new_footer = Vec::with_capacity(reserved_offset + 12 + delete_bitmap.len());
        new_footer.extend_from_slice(&footer_bytes[..reserved_offset]);
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
            return Err(MonaxError::Storage(format!("segment file too small: {}", file_len)));
        }
        let mut tail = [0u8; 8];
        file.seek(SeekFrom::End(-8))?;
        file.read_exact(&mut tail)?;
        let footer_offset = u64::from_le_bytes(tail);
        if footer_offset >= file_len - 8 {
            return Err(MonaxError::Storage(format!("invalid footer offset {}", footer_offset)));
        }
        let footer_len = (file_len - 8 - footer_offset) as usize;
        let mut buf = vec![0u8; footer_len];
        file.seek(SeekFrom::Start(footer_offset))?;
        file.read_exact(&mut buf)?;
        Ok(buf)
    }

    /// Locate the byte offset within the footer at which the reserved
    /// delete-bitmap section begins (i.e. just past the row-group metadata).
    fn reserved_section_offset(buf: &[u8]) -> Result<usize> {
        let mut cursor = Cursor::new(buf);
        let _magic = read_u32_le(&mut cursor)?;
        let _version = read_u32_le(&mut cursor)?;
        let _generation = read_u64_le(&mut cursor)?;
        let _row_count = read_u64_le(&mut cursor)?;
        let row_group_count = read_u32_le(&mut cursor)? as usize;
        let _created_at = read_u64_le(&mut cursor)?;
        let _modified_at = read_u64_le(&mut cursor)?;
        let _table_name = read_len_prefixed_str(&mut cursor)?;
        let col_count = read_u32_le(&mut cursor)? as usize;
        for _ in 0..col_count {
            let _name = read_len_prefixed_str(&mut cursor)?;
            let _dt = read_u8(&mut cursor)?;
            let _nullable = read_u8(&mut cursor)?;
            let _scale = read_u32_le(&mut cursor)?;
            let _precision = read_u32_le(&mut cursor)?;
        }
        for _ in 0..row_group_count {
            let _offset = read_u64_le(&mut cursor)?;
            let _length = read_u64_le(&mut cursor)?;
            let _rc = read_u64_le(&mut cursor)?;
            for _ in 0..col_count { let _ = read_u64_le(&mut cursor)?; }
            for _ in 0..col_count { let _ = read_u64_le(&mut cursor)?; }
            for _ in 0..col_count { let _ = read_u64_le(&mut cursor)?; }
            for _ in 0..col_count {
                let _ = decode_scalar_with_tag(&mut cursor)?;
                let _ = decode_scalar_with_tag(&mut cursor)?;
                let _ = read_u64_le(&mut cursor)?;
                let _ = read_u64_le(&mut cursor)?;
            }
        }
        Ok(cursor.position() as usize)
    }

    /// Read a specific column from a row group.
    pub fn read_column(&self, row_group: usize, column_idx: usize) -> Result<ColumnArray> {
        let rg = &self.footer.row_groups[row_group];
        let col_def = &self.footer.schema.columns[column_idx];

        // Calculate offset within the row group
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

    /// Read an entire row group as a RecordBatch.
    pub fn read_row_group(&self, row_group: usize) -> Result<crate::types::batch::RecordBatch> {
        let num_cols = self.footer.schema.num_columns();
        let mut columns = Vec::with_capacity(num_cols);
        for col_idx in 0..num_cols {
            columns.push(self.read_column(row_group, col_idx)?);
        }
        crate::types::batch::RecordBatch::try_new(self.footer.schema.clone(), columns)
    }

    /// Scan all row groups and yield batches of up to `batch_size` rows.
    pub fn scan(&self, batch_size: usize) -> Result<Vec<crate::types::batch::RecordBatch>> {
        self.scan_with_predicate(batch_size, None)
    }

    /// Scan with optional row-group level pruning predicate. Row groups whose
    /// ZoneMap proves they cannot match `predicate` are skipped entirely.
    /// If predicate evaluation hits an unsupported type or comparison, the row
    /// group is kept (fail-safe: never under-return rows).
    pub fn scan_with_predicate(&self, batch_size: usize, predicate: Option<&RowGroupPredicate>,) -> Result<Vec<crate::types::batch::RecordBatch>> { self.scan_with_predicate_and_stats(batch_size, predicate, None) }

    pub fn scan_with_predicate_and_stats(
        &self,
        batch_size: usize,
        predicate: Option<&RowGroupPredicate>,
        mut stats: Option<&mut ScanStats>,
    ) -> Result<Vec<crate::types::batch::RecordBatch>> {
        let mut batches = Vec::new();
        let total = self.footer.row_groups.len();
        if let Some(s) = stats.as_deref_mut() { s.segments_opened += 1; s.row_groups_total += total as u64; }
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
                    if let Some(s) = stats.as_deref_mut() { s.row_groups_pruned += 1; }
                    continue;
                }
            }
            if let Some(s) = stats.as_deref_mut() { s.row_groups_read += 1; for &b in &self.footer.row_groups[rg_idx].column_compressed_sizes { s.bytes_read_compressed += b; } }
            let mut batch = self.read_row_group(rg_idx)?;
            if bitmap_active {
                let before = batch.num_rows();
                batch = filter_batch_by_delete_bitmap(&batch, rg_first_row, &self.footer.delete_bitmap);
                if let Some(s) = stats.as_deref_mut() { s.rows_filtered_by_delete_bitmap += (before - batch.num_rows()) as u64; }
            }
            if let Some(s) = stats.as_deref_mut() { s.rows_scanned += batch.num_rows() as u64; }
            if batch.num_rows() == 0 { continue; }
            if batch.num_rows() <= batch_size { batches.push(batch); }
            else { for start in (0..batch.num_rows()).step_by(batch_size) { let end = (start + batch_size).min(batch.num_rows()); batches.push(split_batch(&batch, start, end)); } }
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

    fn parse_footer(buf: &[u8]) -> Result<SegmentFooter> {
        let mut cursor = Cursor::new(buf);

        let magic = read_u32_le(&mut cursor)?;
        if magic != SEGMENT_MAGIC {
            return Err(MonaxError::Storage(format!(
                "invalid segment magic: {:08X}, expected {:08X}",
                magic, SEGMENT_MAGIC
            )));
        }

        let version = read_u32_le(&mut cursor)?;
        let _generation = read_u64_le(&mut cursor)?;
        let row_count = read_u64_le(&mut cursor)?;
        let row_group_count = read_u32_le(&mut cursor)?;
        let _created_at = read_u64_le(&mut cursor)?;
        let _modified_at = read_u64_le(&mut cursor)?;

        // Schema
        let _table_name = read_len_prefixed_str(&mut cursor)?;
        let col_count = read_u32_le(&mut cursor)? as usize;
        let mut columns = Vec::with_capacity(col_count);
        for _ in 0..col_count {
            let name = read_len_prefixed_str(&mut cursor)?;
            let dtype_byte = read_u8(&mut cursor)?;
            let nullable = read_u8(&mut cursor)? != 0;
            let scale = read_u32_le(&mut cursor)? as i32;
            let precision = read_u32_le(&mut cursor)? as i32;
            columns.push(ColumnDef {
                name,
                data_type: DataType::from_u8(dtype_byte)?,
                nullable,
                scale,
                precision,
                auto_increment: false,
            });
        }
        let schema = Schema::new(columns);

        // Row groups
        let mut row_groups = Vec::with_capacity(row_group_count as usize);
        for _ in 0..row_group_count {
            let offset = read_u64_le(&mut cursor)?;
            let length = read_u64_le(&mut cursor)?;
            let rg_row_count = read_u64_le(&mut cursor)?;

            // column_sizes (same as compressed sizes in our format)
            let mut _column_sizes = Vec::with_capacity(col_count);
            for _ in 0..col_count {
                _column_sizes.push(read_u64_le(&mut cursor)?);
            }
            // compressed_sizes
            let mut comp_sizes = Vec::with_capacity(col_count);
            for _ in 0..col_count {
                comp_sizes.push(read_u64_le(&mut cursor)?);
            }
            let mut uncompressed_sizes = Vec::with_capacity(col_count);
            for _ in 0..col_count {
                uncompressed_sizes.push(read_u64_le(&mut cursor)?);
            }

            let mut zone_maps = Vec::with_capacity(col_count);
            for _ in 0..col_count {
                let min_v = decode_scalar_with_tag(&mut cursor)?;
                let max_v = decode_scalar_with_tag(&mut cursor)?;
                let null_count = read_u64_le(&mut cursor)?;
                let zm_row_count = read_u64_le(&mut cursor)?;
                zone_maps.push(ZoneMap {
                    min_value: min_v,
                    max_value: max_v,
                    null_count,
                    row_count: zm_row_count,
                });
            }

            row_groups.push(RowGroupInfo {
                offset,
                length,
                row_count: rg_row_count,
                column_compressed_sizes: comp_sizes,
                column_uncompressed_sizes: uncompressed_sizes,
                zone_maps,
            });
        }

        // Delete-bitmap section. Currently always written as empty by writer,
        // but reader-side already supports loading a present bitmap so the
        // delete feature can be enabled without bumping the segment version.
        let deleted_row_count = read_u64_le(&mut cursor).unwrap_or(0);
        let bitmap_len = read_u32_le(&mut cursor).unwrap_or(0) as usize;
        let mut delete_bitmap = vec![0u8; bitmap_len];
        if bitmap_len > 0 {
            cursor.read_exact(&mut delete_bitmap)?;
        }

        Ok(SegmentFooter {
            version,
            row_count,
            row_groups,
            schema,
            deleted_row_count,
            delete_bitmap,
        })
    }
}

// ---------------------------------------------------------------------------
// Column encoding/decoding
// ---------------------------------------------------------------------------

#[allow(dead_code)]
/// Encode a column array slice [offset..offset+count) into bytes, selecting
/// the best encoding automatically. Returns (encoded_bytes, encoding_type).
/// Format: [u8 encoding_type][bitmap_len(4 LE)][bitmap_data][data].
fn encode_column_with_encoding(col: &ColumnArray, offset: usize, count: usize) -> Result<(Vec<u8>, crate::storage::encoding::EncodingType)> {
    use crate::types::scalar::ScalarValue;

    // Extract scalar values for encoding analysis
    let values: Vec<ScalarValue> = (offset..offset + count)
        .map(|i| col.scalar(i))
        .collect();

    let encoding_type = crate::storage::encoding::select_encoding(&values);

    let mut buf = Vec::new();
    buf.push(encoding_type.to_u8());

    let null_flags: Vec<bool> = (offset..offset + count)
        .map(|i| col.bitmap().is_null(i))
        .collect();
    let bitmap = NullBitmap::from_bools(&null_flags);
    let bitmap_bytes = bitmap.to_bytes();

    buf.extend_from_slice(&(bitmap_bytes.len() as u32).to_le_bytes());
    buf.extend_from_slice(&bitmap_bytes);

    // Apply encoding-specific serialization for the data portion
    match encoding_type {
        crate::storage::encoding::EncodingType::Rle => {
            let pairs = crate::storage::encoding::rle_encode(&values);
            buf.extend_from_slice(&(pairs.len() as u32).to_le_bytes());
            for (val, count) in &pairs {
                if !val.is_null() {
                    encode_scalar_value(&mut buf, val);
                }
                buf.extend_from_slice(&count.to_le_bytes());
            }
        }
        crate::storage::encoding::EncodingType::Dictionary => {
            let (dict, indices) = crate::storage::encoding::dictionary_encode(&values);
            buf.extend_from_slice(&(dict.len() as u32).to_le_bytes());
            for val in &dict {
                if !val.is_null() {
                    encode_scalar_value(&mut buf, val);
                }
            }
            buf.extend_from_slice(&(indices.len() as u32).to_le_bytes());
            for &idx in &indices {
                buf.extend_from_slice(&idx.to_le_bytes());
            }
        }
        _ => {
            // Plain encoding: write values directly
            for i in offset..offset + count {
                if col.bitmap().is_null(i) {
                    continue;
                }
                encode_value(&mut buf, col, i);
            }
        }
    }

    Ok((buf, encoding_type))
}

/// Encode a scalar value to bytes for dictionary encoding.
#[allow(dead_code)]
fn encode_scalar_value(buf: &mut Vec<u8>, val: &ScalarValue) {
    match val {
        ScalarValue::Int64(Some(v)) => buf.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::Int32(Some(v)) => buf.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::Int16(Some(v)) => buf.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::Float64(Some(v)) => buf.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::Float32(Some(v)) => buf.extend_from_slice(&v.to_le_bytes()),
        ScalarValue::VarChar(Some(v)) => {
            buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
            buf.extend_from_slice(v.as_bytes());
        }
        ScalarValue::Bool(Some(v)) => buf.push(if *v { 1 } else { 0 }),
        _ => {}
    }
}

/// Encode a column array slice [offset..offset+count) into bytes.
/// Format: bitmap_len(4 LE) + bitmap_data + data (non-null values only).
fn encode_column(col: &ColumnArray, offset: usize, count: usize) -> Result<Vec<u8>> {
    let mut buf = Vec::new();

    let null_flags: Vec<bool> = (offset..offset + count)
        .map(|i| col.bitmap().is_null(i))
        .collect();
    let bitmap = NullBitmap::from_bools(&null_flags);
    let bitmap_bytes = bitmap.to_bytes();

    buf.extend_from_slice(&(bitmap_bytes.len() as u32).to_le_bytes());
    buf.extend_from_slice(&bitmap_bytes);

    for i in offset..offset + count {
        if col.bitmap().is_null(i) {
            continue;
        }
        encode_value(&mut buf, col, i);
    }

    Ok(buf)
}

fn encode_value(buf: &mut Vec<u8>, col: &ColumnArray, idx: usize) {
    match col {
        ColumnArray::Bool { data, .. } => buf.push(data[idx]),
        ColumnArray::Int8 { data, .. } => buf.push(data[idx] as u8),
        ColumnArray::Int16 { data, .. } => buf.extend_from_slice(&data[idx].to_le_bytes()),
        ColumnArray::Int32 { data, .. } => buf.extend_from_slice(&data[idx].to_le_bytes()),
        ColumnArray::Int64 { data, .. } => buf.extend_from_slice(&data[idx].to_le_bytes()),
        ColumnArray::UInt8 { data, .. } => buf.push(data[idx]),
        ColumnArray::UInt16 { data, .. } => buf.extend_from_slice(&data[idx].to_le_bytes()),
        ColumnArray::UInt32 { data, .. } => buf.extend_from_slice(&data[idx].to_le_bytes()),
        ColumnArray::UInt64 { data, .. } => buf.extend_from_slice(&data[idx].to_le_bytes()),
        ColumnArray::Float32 { data, .. } => buf.extend_from_slice(&data[idx].to_le_bytes()),
        ColumnArray::Float64 { data, .. } => buf.extend_from_slice(&data[idx].to_le_bytes()),
        ColumnArray::Decimal { data, .. } => buf.extend_from_slice(&data[idx].to_le_bytes()),
        ColumnArray::Date { data, .. } => buf.extend_from_slice(&data[idx].to_le_bytes()),
        ColumnArray::Time { data, .. } => buf.extend_from_slice(&data[idx].to_le_bytes()),
        ColumnArray::Timestamp { data, .. } => buf.extend_from_slice(&data[idx].to_le_bytes()),
        ColumnArray::Interval { data, .. } => buf.extend_from_slice(&data[idx].to_le_bytes()),
        ColumnArray::VarChar { data, .. } => {
            buf.extend_from_slice(&(data[idx].len() as u32).to_le_bytes());
            buf.extend_from_slice(data[idx].as_bytes());
        }
        ColumnArray::Blob { data, .. } => {
            buf.extend_from_slice(&(data[idx].len() as u32).to_le_bytes());
            buf.extend_from_slice(&data[idx]);
        }
        ColumnArray::Vector { data, .. } => {
            let v = &data[idx];
            buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
            for f in v {
                buf.extend_from_slice(&f.to_le_bytes());
            }
        }
    }
}

/// Decode a column from bytes.
fn decode_column(data: &[u8], row_count: usize, col_def: &ColumnDef) -> Result<ColumnArray> {
    let mut cursor = Cursor::new(data);
    let bitmap_len = read_u32_le(&mut cursor)? as usize;
    let mut bitmap_data = vec![0u8; bitmap_len];
    cursor.read_exact(&mut bitmap_data)?;
    let bitmap = NullBitmap::from_bytes(&bitmap_data, row_count);

    let remaining_data = &data[4 + bitmap_len..];
    let mut dc = Cursor::new(remaining_data);

    macro_rules! read_fixed {
        ($ty:ty, $size:expr, $variant:ident) => {{
            let mut vals = Vec::with_capacity(row_count);
            for i in 0..row_count {
                if bitmap.is_null(i) {
                    vals.push(<$ty>::default());
                } else {
                    let mut b = [0u8; $size];
                    dc.read_exact(&mut b).map_err(|e| MonaxError::Storage(format!("read error: {}", e)))?;
                    vals.push(<$ty>::from_le_bytes(b));
                }
            }
            ColumnArray::$variant { data: vals, bitmap: bitmap.clone() }
        }};
    }

    let col = match col_def.data_type {
        DataType::Bool => {
            let mut vals = Vec::with_capacity(row_count);
            for i in 0..row_count {
                vals.push(if bitmap.is_null(i) {
                    0u8
                } else {
                    let mut b = [0u8; 1];
                    dc.read_exact(&mut b).map_err(|e| MonaxError::Storage(format!("read error: {}", e)))?;
                    b[0]
                });
            }
            ColumnArray::Bool { data: vals, bitmap: bitmap.clone() }
        }
        DataType::Int8 => {
            let mut vals = Vec::with_capacity(row_count);
            for i in 0..row_count {
                vals.push(if bitmap.is_null(i) {
                    0i8
                } else {
                    let mut b = [0u8; 1];
                    dc.read_exact(&mut b).map_err(|e| MonaxError::Storage(format!("read error: {}", e)))?;
                    b[0] as i8
                });
            }
            ColumnArray::Int8 { data: vals, bitmap: bitmap.clone() }
        }
        DataType::Int16 => read_fixed!(i16, 2, Int16),
        DataType::Int32 => read_fixed!(i32, 4, Int32),
        DataType::Int64 => read_fixed!(i64, 8, Int64),
        DataType::UInt8 => {
            let mut vals = Vec::with_capacity(row_count);
            for i in 0..row_count {
                vals.push(if bitmap.is_null(i) {
                    0u8
                } else {
                    let mut b = [0u8; 1];
                    dc.read_exact(&mut b).map_err(|e| MonaxError::Storage(format!("read error: {}", e)))?;
                    b[0]
                });
            }
            ColumnArray::UInt8 { data: vals, bitmap: bitmap.clone() }
        }
        DataType::UInt16 => read_fixed!(u16, 2, UInt16),
        DataType::UInt32 => read_fixed!(u32, 4, UInt32),
        DataType::UInt64 => read_fixed!(u64, 8, UInt64),
        DataType::Float32 => read_fixed!(f32, 4, Float32),
        DataType::Float64 => read_fixed!(f64, 8, Float64),
        DataType::Decimal => read_fixed!(i64, 8, Decimal),
        DataType::Date => read_fixed!(i32, 4, Date),
        DataType::Time => read_fixed!(i64, 8, Time),
        DataType::Timestamp => read_fixed!(i64, 8, Timestamp),
        DataType::Interval => read_fixed!(i64, 8, Interval),
        DataType::VarChar => {
            let mut vals = Vec::with_capacity(row_count);
            for i in 0..row_count {
                if bitmap.is_null(i) {
                    vals.push(String::new());
                } else {
                    let len = read_u32_le(&mut dc).unwrap_or(0) as usize;
                    let mut s = vec![0u8; len];
                    dc.read_exact(&mut s).unwrap_or(());
                    vals.push(String::from_utf8_lossy(&s).into_owned());
                }
            }
            ColumnArray::VarChar { data: vals, bitmap: bitmap.clone() }
        }
        DataType::Blob => {
            let mut vals = Vec::with_capacity(row_count);
            for i in 0..row_count {
                if bitmap.is_null(i) {
                    vals.push(Vec::new());
                } else {
                    let len = read_u32_le(&mut dc).unwrap_or(0) as usize;
                    let mut v = vec![0u8; len];
                    dc.read_exact(&mut v).unwrap_or(());
                    vals.push(v);
                }
            }
            ColumnArray::Blob { data: vals, bitmap: bitmap.clone() }
        }
        DataType::Vector => {
            let mut vals = Vec::with_capacity(row_count);
            for i in 0..row_count {
                if bitmap.is_null(i) {
                    vals.push(Vec::new());
                } else {
                    let count = read_u32_le(&mut dc).unwrap_or(0) as usize;
                    let mut v = Vec::with_capacity(count);
                    for _ in 0..count {
                        let mut b = [0u8; 4];
                        dc.read_exact(&mut b).unwrap_or(());
                        v.push(f32::from_le_bytes(b));
                    }
                    vals.push(v);
                }
            }
            ColumnArray::Vector { data: vals, bitmap: bitmap.clone() }
        }
    };

    Ok(col)
}

/// Split a batch into rows [start..end).
fn split_batch(batch: &crate::types::batch::RecordBatch, start: usize, end: usize) -> crate::types::batch::RecordBatch {
    let mut columns = Vec::with_capacity(batch.num_columns());
    for i in 0..batch.num_columns() {
        columns.push(split_column(batch.column(i).unwrap(), start, end));
    }
    crate::types::batch::RecordBatch::try_new(batch.schema.clone(), columns).unwrap()
}

/// Build a new batch containing only those rows not marked deleted in
/// `delete_bitmap`. `first_row_id` is the global row id (within the segment) of
/// `batch[0]`, so we can index the bitmap correctly.
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
        let deleted = delete_bitmap.get(byte).map_or(false, |b| (b & (1u8 << bit)) != 0);
        if !deleted { keep.push(i); }
    }
    if keep.len() == batch.num_rows() { return batch.clone(); }
    let mut columns = Vec::with_capacity(batch.num_columns());
    for c in 0..batch.num_columns() {
        let src = batch.column(c).unwrap();
        let mut new_col = ColumnArray::empty(src.data_type());
        for &i in &keep { new_col.push(&src.scalar(i)).unwrap(); }
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

// ---------------------------------------------------------------------------
// Header / footer / zone-map helpers
// ---------------------------------------------------------------------------

/// Encode the fixed 32-byte segment file header (magic + version + created_at + flags).
/// Must be the first bytes of every segment file. Exposed so writer paths
/// outside of [`SegmentWriter`] (e.g. the SQL engine's streaming bulk-insert
/// path) write the same header byte-for-byte.
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
}fn update_zone_map(
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
                if scalar_lt(&v, m) {
                    *cur_min = Some(v.clone());
                }
            }
        }
        match cur_max {
            None => *cur_max = Some(v),
            Some(m) => {
                if scalar_lt(m, &v) {
                    *cur_max = Some(v);
                }
            }
        }
    }
}

fn scalar_lt(a: &ScalarValue, b: &ScalarValue) -> bool {
    use ScalarValue::*;
    match (a, b) {
        (Bool(Some(x)), Bool(Some(y))) => !*x && *y,
        (Int8(Some(x)), Int8(Some(y))) => x < y,
        (Int16(Some(x)), Int16(Some(y))) => x < y,
        (Int32(Some(x)), Int32(Some(y))) => x < y,
        (Int64(Some(x)), Int64(Some(y))) => x < y,
        (UInt8(Some(x)), UInt8(Some(y))) => x < y,
        (UInt16(Some(x)), UInt16(Some(y))) => x < y,
        (UInt32(Some(x)), UInt32(Some(y))) => x < y,
        (UInt64(Some(x)), UInt64(Some(y))) => x < y,
        (Float32(Some(x)), Float32(Some(y))) => x < y,
        (Float64(Some(x)), Float64(Some(y))) => x < y,
        (Decimal(Some(x)), Decimal(Some(y))) => x < y,
        (Date(Some(x)), Date(Some(y))) => x < y,
        (Time(Some(x)), Time(Some(y))) => x < y,
        (Timestamp(Some(x)), Timestamp(Some(y))) => x < y,
        (Interval(Some(x)), Interval(Some(y))) => x < y,
        (VarChar(Some(x)), VarChar(Some(y))) => x < y,
        (Blob(Some(x)), Blob(Some(y))) => x < y,
        _ => false,
    }
}
fn encode_scalar_with_tag(buf: &mut Vec<u8>, v: &ScalarValue) {
    let tag = scalar_tag(v);
    buf.push(tag);
    if v.is_null() {
        return;
    }
    match v {
        ScalarValue::Bool(Some(x)) => buf.push(if *x { 1 } else { 0 }),
        ScalarValue::Int8(Some(x)) => buf.push(*x as u8),
        ScalarValue::Int16(Some(x)) => buf.extend_from_slice(&x.to_le_bytes()),
        ScalarValue::Int32(Some(x)) => buf.extend_from_slice(&x.to_le_bytes()),
        ScalarValue::Int64(Some(x)) => buf.extend_from_slice(&x.to_le_bytes()),
        ScalarValue::UInt8(Some(x)) => buf.push(*x),
        ScalarValue::UInt16(Some(x)) => buf.extend_from_slice(&x.to_le_bytes()),
        ScalarValue::UInt32(Some(x)) => buf.extend_from_slice(&x.to_le_bytes()),
        ScalarValue::UInt64(Some(x)) => buf.extend_from_slice(&x.to_le_bytes()),
        ScalarValue::Float32(Some(x)) => buf.extend_from_slice(&x.to_le_bytes()),
        ScalarValue::Float64(Some(x)) => buf.extend_from_slice(&x.to_le_bytes()),
        ScalarValue::Decimal(Some(x)) => buf.extend_from_slice(&x.to_le_bytes()),
        ScalarValue::Date(Some(x)) => buf.extend_from_slice(&x.to_le_bytes()),
        ScalarValue::Time(Some(x)) => buf.extend_from_slice(&x.to_le_bytes()),
        ScalarValue::Timestamp(Some(x)) => buf.extend_from_slice(&x.to_le_bytes()),
        ScalarValue::Interval(Some(x)) => buf.extend_from_slice(&x.to_le_bytes()),
        ScalarValue::VarChar(Some(x)) => {
            buf.extend_from_slice(&(x.len() as u32).to_le_bytes());
            buf.extend_from_slice(x.as_bytes());
        }
        ScalarValue::Blob(Some(x)) => {
            buf.extend_from_slice(&(x.len() as u32).to_le_bytes());
            buf.extend_from_slice(x);
        }
        ScalarValue::Vector(Some(x)) => {
            buf.extend_from_slice(&(x.len() as u32).to_le_bytes());
            for f in x {
                buf.extend_from_slice(&f.to_le_bytes());
            }
        }
        _ => {}
    }
}

fn scalar_tag(v: &ScalarValue) -> u8 {
    let dtype_tag = match v.data_type() {
        DataType::Bool => 1u8,
        DataType::Int8 => 2,
        DataType::Int16 => 3,
        DataType::Int32 => 4,
        DataType::Int64 => 5,
        DataType::UInt8 => 6,
        DataType::UInt16 => 7,
        DataType::UInt32 => 8,
        DataType::UInt64 => 9,
        DataType::Float32 => 10,
        DataType::Float64 => 11,
        DataType::Decimal => 12,
        DataType::Date => 13,
        DataType::Time => 14,
        DataType::Timestamp => 15,
        DataType::Interval => 16,
        DataType::VarChar => 17,
        DataType::Blob => 18,
        DataType::Vector => 19,
    };
    if v.is_null() {
        dtype_tag | 0x80
    } else {
        dtype_tag
    }
}
pub fn decode_scalar_with_tag(cursor: &mut Cursor<&[u8]>) -> Result<ScalarValue> {
    let tag = read_u8(cursor).map_err(|e| MonaxError::Storage(format!("read tag: {}", e)))?;
    let is_null = (tag & 0x80) != 0;
    let dtype_tag = tag & 0x7F;
    let dtype = match dtype_tag {
        1 => DataType::Bool,
        2 => DataType::Int8,
        3 => DataType::Int16,
        4 => DataType::Int32,
        5 => DataType::Int64,
        6 => DataType::UInt8,
        7 => DataType::UInt16,
        8 => DataType::UInt32,
        9 => DataType::UInt64,
        10 => DataType::Float32,
        11 => DataType::Float64,
        12 => DataType::Decimal,
        13 => DataType::Date,
        14 => DataType::Time,
        15 => DataType::Timestamp,
        16 => DataType::Interval,
        17 => DataType::VarChar,
        18 => DataType::Blob,
        19 => DataType::Vector,
        other => return Err(MonaxError::Storage(format!("bad scalar tag: {}", other))),
    };
    if is_null {
        return Ok(ZoneMap::null_for(dtype));
    }
    let v = match dtype {
        DataType::Bool => ScalarValue::Bool(Some(read_u8(cursor).unwrap_or(0) != 0)),
        DataType::Int8 => ScalarValue::Int8(Some(read_u8(cursor).unwrap_or(0) as i8)),
        DataType::Int16 => {
            let mut b = [0u8; 2]; cursor.read_exact(&mut b)?;
            ScalarValue::Int16(Some(i16::from_le_bytes(b)))
        }
        DataType::Int32 => {
            let mut b = [0u8; 4]; cursor.read_exact(&mut b)?;
            ScalarValue::Int32(Some(i32::from_le_bytes(b)))
        }
        DataType::Int64 => {
            let mut b = [0u8; 8]; cursor.read_exact(&mut b)?;
            ScalarValue::Int64(Some(i64::from_le_bytes(b)))
        }
        DataType::UInt8 => ScalarValue::UInt8(Some(read_u8(cursor).unwrap_or(0))),
        DataType::UInt16 => {
            let mut b = [0u8; 2]; cursor.read_exact(&mut b)?;
            ScalarValue::UInt16(Some(u16::from_le_bytes(b)))
        }
        DataType::UInt32 => {
            let mut b = [0u8; 4]; cursor.read_exact(&mut b)?;
            ScalarValue::UInt32(Some(u32::from_le_bytes(b)))
        }
        DataType::UInt64 => {
            let mut b = [0u8; 8]; cursor.read_exact(&mut b)?;
            ScalarValue::UInt64(Some(u64::from_le_bytes(b)))
        }
        DataType::Float32 => {
            let mut b = [0u8; 4]; cursor.read_exact(&mut b)?;
            ScalarValue::Float32(Some(f32::from_le_bytes(b)))
        }
        DataType::Float64 => {
            let mut b = [0u8; 8]; cursor.read_exact(&mut b)?;
            ScalarValue::Float64(Some(f64::from_le_bytes(b)))
        }
        DataType::Decimal => {
            let mut b = [0u8; 8]; cursor.read_exact(&mut b)?;
            ScalarValue::Decimal(Some(i64::from_le_bytes(b)))
        }
        DataType::Date => {
            let mut b = [0u8; 4]; cursor.read_exact(&mut b)?;
            ScalarValue::Date(Some(i32::from_le_bytes(b)))
        }
        DataType::Time => {
            let mut b = [0u8; 8]; cursor.read_exact(&mut b)?;
            ScalarValue::Time(Some(i64::from_le_bytes(b)))
        }
        DataType::Timestamp => {
            let mut b = [0u8; 8]; cursor.read_exact(&mut b)?;
            ScalarValue::Timestamp(Some(i64::from_le_bytes(b)))
        }
        DataType::Interval => {
            let mut b = [0u8; 8]; cursor.read_exact(&mut b)?;
            ScalarValue::Interval(Some(i64::from_le_bytes(b)))
        }
        DataType::VarChar => {
            let len = read_u32_le(cursor)? as usize;
            let mut b = vec![0u8; len]; cursor.read_exact(&mut b)?;
            ScalarValue::VarChar(Some(String::from_utf8_lossy(&b).into_owned()))
        }
        DataType::Blob => {
            let len = read_u32_le(cursor)? as usize;
            let mut b = vec![0u8; len]; cursor.read_exact(&mut b)?;
            ScalarValue::Blob(Some(b))
        }
        DataType::Vector => {
            let len = read_u32_le(cursor)? as usize;
            let mut v = Vec::with_capacity(len);
            for _ in 0..len {
                let mut b = [0u8; 4]; cursor.read_exact(&mut b)?;
                v.push(f32::from_le_bytes(b));
            }
            ScalarValue::Vector(Some(v))
        }
    };
    Ok(v)
}

fn read_u8(cursor: &mut Cursor<&[u8]>) -> std::result::Result<u8, io::Error> {
    let mut buf = [0u8; 1];
    cursor.read_exact(&mut buf)?;
    Ok(buf[0])
}

fn read_u32_le(cursor: &mut Cursor<&[u8]>) -> std::result::Result<u32, io::Error> {
    let mut buf = [0u8; 4];
    cursor.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

fn read_u64_le(cursor: &mut Cursor<&[u8]>) -> std::result::Result<u64, io::Error> {
    let mut buf = [0u8; 8];
    cursor.read_exact(&mut buf)?;
    Ok(u64::from_le_bytes(buf))
}

fn read_len_prefixed_str(cursor: &mut Cursor<&[u8]>) -> std::result::Result<String, io::Error> {
    let len = read_u32_le(cursor)? as usize;
    let mut buf = vec![0u8; len];
    cursor.read_exact(&mut buf)?;
    Ok(String::from_utf8_lossy(&buf).into_owned())
}

fn encode_len_prefixed_str(buf: &mut Vec<u8>, s: &str) {
    buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
    buf.extend_from_slice(s.as_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::batch::RecordBatch;
    use crate::types::scalar::ScalarValue;
    use tempfile::TempDir;

    #[test]
    fn test_segment_write_read_roundtrip() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("seg_00001.dat");

        let schema = Schema::new(vec![
            ColumnDef::new("id", DataType::Int64),
            ColumnDef::new("name", DataType::VarChar),
        ]);

        // Write
        {
            let mut writer = SegmentWriter::create(&path, schema.clone(), 8192).unwrap();
            let mut batch = RecordBatch::new(schema.clone());
            for i in 0..100i64 {
                batch.column_mut(0).unwrap().push(&ScalarValue::Int64(Some(i))).unwrap();
                batch.column_mut(1).unwrap().push(&ScalarValue::VarChar(Some(format!("user_{}", i)))).unwrap();
            }
            writer.write_batch(&batch).unwrap();
            writer.seal().unwrap();
        }

        // Read
        let reader = SegmentReader::open(&path).unwrap();
        assert_eq!(reader.row_count(), 100);
        assert_eq!(reader.schema().columns.len(), 2);

        let batches = reader.scan(1000).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 100);

        assert_eq!(batches[0].scalar(0, 0).unwrap(), ScalarValue::Int64(Some(0)));
        assert_eq!(batches[0].scalar(99, 0).unwrap(), ScalarValue::Int64(Some(99)));
        assert_eq!(batches[0].scalar(0, 1).unwrap(), ScalarValue::VarChar(Some("user_0".into())));
        assert_eq!(batches[0].scalar(99, 1).unwrap(), ScalarValue::VarChar(Some("user_99".into())));
    }

    #[test]
    fn test_segment_with_nulls() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("seg_00002.dat");

        let schema = Schema::new(vec![
            ColumnDef::new("val", DataType::Int32),
        ]);

        {
            let mut writer = SegmentWriter::create(&path, schema.clone(), 8192).unwrap();
            let mut batch = RecordBatch::new(schema.clone());
            batch.column_mut(0).unwrap().push(&ScalarValue::Int32(Some(1))).unwrap();
            batch.column_mut(0).unwrap().push(&ScalarValue::Int32(None)).unwrap();
            batch.column_mut(0).unwrap().push(&ScalarValue::Int32(Some(3))).unwrap();
            writer.write_batch(&batch).unwrap();
            writer.seal().unwrap();
        }

        let reader = SegmentReader::open(&path).unwrap();
        let batches = reader.scan(1000).unwrap();
        assert_eq!(batches[0].num_rows(), 3);
        assert_eq!(batches[0].scalar(0, 0).unwrap(), ScalarValue::Int32(Some(1)));
        assert!(batches[0].scalar(1, 0).unwrap().is_null());
        assert_eq!(batches[0].scalar(2, 0).unwrap(), ScalarValue::Int32(Some(3)));
    }

    #[test]
    fn test_empty_segment() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("seg_00003.dat");

        let schema = Schema::new(vec![
            ColumnDef::new("id", DataType::Int64),
        ]);

        {
            let writer = SegmentWriter::create(&path, schema.clone(), 8192).unwrap();
            writer.seal().unwrap();
        }

        let reader = SegmentReader::open(&path).unwrap();
        assert_eq!(reader.row_count(), 0);
    }

    #[test]
    fn test_multiple_row_groups() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("seg_00004.dat");

        let schema = Schema::new(vec![
            ColumnDef::new("id", DataType::Int64),
        ]);

        {
            let mut writer = SegmentWriter::create(&path, schema.clone(), 10).unwrap(); // small row group
            let mut batch = RecordBatch::new(schema.clone());
            for i in 0..25i64 {
                batch.column_mut(0).unwrap().push(&ScalarValue::Int64(Some(i))).unwrap();
            }
            writer.write_batch(&batch).unwrap();
            writer.seal().unwrap();
        }

        let reader = SegmentReader::open(&path).unwrap();
        assert_eq!(reader.row_count(), 25);
        assert_eq!(reader.num_row_groups(), 3); // 10 + 10 + 5

        let batches = reader.scan(1000).unwrap();
        assert_eq!(batches.len(), 3);
        assert_eq!(batches[0].num_rows(), 10);
        assert_eq!(batches[1].num_rows(), 10);
        assert_eq!(batches[2].num_rows(), 5);
    }

    #[test]
    fn test_zone_maps_populated() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("seg_zm.dat");

        let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);

        {
            let mut writer = SegmentWriter::create(&path, schema.clone(), 10).unwrap();
            let mut batch = RecordBatch::new(schema.clone());
            for i in 0..25i64 {
                batch.column_mut(0).unwrap().push(&ScalarValue::Int64(Some(i))).unwrap();
            }
            writer.write_batch(&batch).unwrap();
            writer.seal().unwrap();
        }

        let reader = SegmentReader::open(&path).unwrap();
        assert_eq!(reader.num_row_groups(), 3);

        // rg 0: ids 0..10 -> min=0, max=9
        let zm0 = &reader.zone_maps(0)[0];
        assert_eq!(zm0.min_value, ScalarValue::Int64(Some(0)));
        assert_eq!(zm0.max_value, ScalarValue::Int64(Some(9)));
        assert_eq!(zm0.null_count, 0);
        assert_eq!(zm0.row_count, 10);

        // rg 2: ids 20..25 -> min=20, max=24
        let zm2 = &reader.zone_maps(2)[0];
        assert_eq!(zm2.min_value, ScalarValue::Int64(Some(20)));
        assert_eq!(zm2.max_value, ScalarValue::Int64(Some(24)));
        assert_eq!(zm2.row_count, 5);
    }

    #[test]
    fn test_row_group_predicate_pruning() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("seg_prune.dat");

        let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);

        {
            let mut writer = SegmentWriter::create(&path, schema.clone(), 10).unwrap();
            let mut batch = RecordBatch::new(schema.clone());
            for i in 0..25i64 {
                batch.column_mut(0).unwrap().push(&ScalarValue::Int64(Some(i))).unwrap();
            }
            writer.write_batch(&batch).unwrap();
            writer.seal().unwrap();
        }

        let reader = SegmentReader::open(&path).unwrap();

        // id = 22 -> only rg 2 (20..25) can match
        let pred = RowGroupPredicate::new(vec![RangeClause {
            column_index: 0,
            op: RangeOp::Eq,
            value: ScalarValue::Int64(Some(22)),
        }]);
        let batches = reader.scan_with_predicate(1000, Some(&pred)).unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 5, "only row group 2 should be returned");

        // id > 100 -> all row groups pruned
        let pred = RowGroupPredicate::new(vec![RangeClause {
            column_index: 0,
            op: RangeOp::Gt,
            value: ScalarValue::Int64(Some(100)),
        }]);
        let batches = reader.scan_with_predicate(1000, Some(&pred)).unwrap();
        assert!(batches.is_empty(), "all row groups should be pruned");

        // id >= 15 -> only rg 1 (10..19) and rg 2 (20..24)
        let pred = RowGroupPredicate::new(vec![RangeClause {
            column_index: 0,
            op: RangeOp::Ge,
            value: ScalarValue::Int64(Some(15)),
        }]);
        let batches = reader.scan_with_predicate(1000, Some(&pred)).unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 15, "row groups 1+2 should be kept (10 + 5)");

        // empty predicate -> read everything
        let pred = RowGroupPredicate::new(vec![]);
        let batches = reader.scan_with_predicate(1000, Some(&pred)).unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 25);
    }

    #[test]
    fn test_row_group_predicate_with_nulls() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("seg_prune_null.dat");

        let schema = Schema::new(vec![ColumnDef::new("v", DataType::Int32)]);

        {
            let mut writer = SegmentWriter::create(&path, schema.clone(), 4).unwrap();
            let mut batch = RecordBatch::new(schema.clone());
            // 8 rows: 4 + 4, second rg is all NULL
            batch.column_mut(0).unwrap().push(&ScalarValue::Int32(Some(1))).unwrap();
            batch.column_mut(0).unwrap().push(&ScalarValue::Int32(Some(2))).unwrap();
            batch.column_mut(0).unwrap().push(&ScalarValue::Int32(Some(3))).unwrap();
            batch.column_mut(0).unwrap().push(&ScalarValue::Int32(Some(4))).unwrap();
            for _ in 0..4 {
                batch.column_mut(0).unwrap().push(&ScalarValue::Int32(None)).unwrap();
            }
            writer.write_batch(&batch).unwrap();
            writer.seal().unwrap();
        }

        let reader = SegmentReader::open(&path).unwrap();
        assert_eq!(reader.num_row_groups(), 2);

        // rg 1 is all NULL -> Eq predicate should prune it
        let pred = RowGroupPredicate::new(vec![RangeClause {
            column_index: 0,
            op: RangeOp::Eq,
            value: ScalarValue::Int32(Some(99)),
        }]);
        let batches = reader.scan_with_predicate(1000, Some(&pred)).unwrap();
        // rg 0 has min=1 max=4, so 99 prunes rg 0 too -> all pruned
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 0);

        // Eq 2 -> only rg 0 should match (rg 1 all-null is pruned)
        let pred = RowGroupPredicate::new(vec![RangeClause {
            column_index: 0,
            op: RangeOp::Eq,
            value: ScalarValue::Int32(Some(2)),
        }]);
        let batches = reader.scan_with_predicate(1000, Some(&pred)).unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 4, "rg 0 returned, rg 1 (all null) pruned");
    }

    #[test]
    fn test_segment_header_validation() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("seg_hdr.dat");
        let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);

        {
            let writer = SegmentWriter::create(&path, schema.clone(), 1024).unwrap();
            writer.seal().unwrap();
        }

        // Corrupt the magic
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[0] = 0xFF;
        std::fs::write(&path, &bytes).unwrap();

        let err = match SegmentReader::open(&path) { Ok(_) => panic!("opening corrupted segment should fail"), Err(e) => e };
        let msg = format!("{}", err);
        assert!(msg.contains("magic"), "error should mention magic: {}", msg);
    }

    #[test]
    fn test_seal_with_deletes_then_scan_filters_rows() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("seg_del.dat");
        let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);

        let mut writer = SegmentWriter::create(&path, schema.clone(), 32).unwrap();
        let mut batch = RecordBatch::new(schema.clone());
        for i in 0..100i64 {
            batch.column_mut(0).unwrap().push(&ScalarValue::Int64(Some(i))).unwrap();
        }
        writer.write_batch(&batch).unwrap();

        // Mark every odd-indexed row deleted.
        let mut bitmap = vec![0u8; 13];
        let mut deleted = 0u64;
        for i in 0..100u64 {
            if i % 2 == 1 {
                bitmap[(i / 8) as usize] |= 1u8 << (i % 8);
                deleted += 1;
            }
        }
        writer.seal_with_deletes(&bitmap, deleted).unwrap();

        let reader = SegmentReader::open(&path).unwrap();
        assert_eq!(reader.row_count(), 100);
        assert_eq!(reader.deleted_row_count(), 50);
        assert_eq!(reader.delete_bitmap().len(), 13);

        let mut stats = ScanStats::default();
        let batches = reader
            .scan_with_predicate_and_stats(4096, None, Some(&mut stats))
            .unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 50, "scan should skip deleted rows");
        for b in &batches {
            for r in 0..b.num_rows() {
                let v = b.scalar(r, 0).unwrap();
                if let ScalarValue::Int64(Some(n)) = v {
                    assert!(n % 2 == 0, "deleted (odd) row leaked through: {}", n);
                }
            }
        }
        assert_eq!(stats.rows_filtered_by_delete_bitmap, 50);
        assert_eq!(stats.rows_scanned, 50);
    }

    #[test]
    fn test_rewrite_footer_with_deletes_preserves_data() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("seg_rewrite.dat");
        let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);

        let mut writer = SegmentWriter::create(&path, schema.clone(), 50).unwrap();
        let mut batch = RecordBatch::new(schema.clone());
        for i in 0..200i64 {
            batch.column_mut(0).unwrap().push(&ScalarValue::Int64(Some(i))).unwrap();
        }
        writer.write_batch(&batch).unwrap();
        writer.seal().unwrap();

        // Capture data-block bytes (everything before the footer) for later check.
        let original = std::fs::read(&path).unwrap();
        let original_data_section = {
            let r = SegmentReader::open(&path).unwrap();
            let total_rows = r.row_count();
            drop(r);
            // Footer offset is the last 8 bytes; we don't need to parse it again.
            let footer_off = u64::from_le_bytes(original[original.len() - 8..].try_into().unwrap()) as usize;
            let _ = total_rows;
            original[..footer_off].to_vec()
        };

        // Build a bitmap that deletes rows [10..20).
        let mut bitmap = vec![0u8; 25];
        let mut deleted = 0u64;
        for i in 10u64..20 {
            bitmap[(i / 8) as usize] |= 1u8 << (i % 8);
            deleted += 1;
        }
        SegmentReader::rewrite_footer_with_deletes(&path, &bitmap, deleted).unwrap();

        let after = std::fs::read(&path).unwrap();
        let after_footer_off = u64::from_le_bytes(after[after.len() - 8..].try_into().unwrap()) as usize;
        assert_eq!(after[..after_footer_off], original_data_section[..],
            "rewrite_footer_with_deletes must not modify data blocks");

        let reader = SegmentReader::open(&path).unwrap();
        assert_eq!(reader.row_count(), 200);
        assert_eq!(reader.deleted_row_count(), 10);
        let batches = reader.scan(4096).unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 190);
        for b in &batches {
            for r in 0..b.num_rows() {
                if let Some(ScalarValue::Int64(Some(n))) = b.scalar(r, 0) {
                    assert!(!(10..20).contains(&n), "deleted row {} leaked", n);
                }
            }
        }
    }
}
