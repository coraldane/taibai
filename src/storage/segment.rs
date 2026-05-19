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

/// Segment magic number: "TAIX" = 0x54414948.
pub const SEGMENT_MAGIC: u32 = 0x5441_4948;
pub const SEGMENT_VERSION: u32 = 3;
pub const DEFAULT_ROW_GROUP_SIZE: usize = 8192;
pub const FOOTER_SIZE: usize = 4096;
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

/// Metadata for a completed row group.
#[derive(Debug, Clone)]
struct RowGroupMeta {
    offset: u64,
    length: u64,
    row_count: u64,
    column_sizes: Vec<u64>,
    compressed_sizes: Vec<u64>,
    uncompressed_sizes: Vec<u64>,
}

/// A row group being built in memory.
struct BuildingRowGroup {
    columns: Vec<Vec<u8>>,
    row_count: usize,
    uncompressed_sizes: Vec<u64>,
}

impl BuildingRowGroup {
    fn new(num_columns: usize) -> Self {
        Self {
            columns: vec![Vec::new(); num_columns],
            row_count: 0,
            uncompressed_sizes: vec![0; num_columns],
        }
    }
}

impl SegmentWriter {
    /// Create a new segment writer.
    pub fn create(path: &Path, schema: Schema, row_group_size: usize) -> Result<Self> {
        let file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;

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

    /// Finalize the segment: write footer and close.
    pub fn seal(mut self) -> Result<()> {
        if self.current_group.is_some() {
            self.flush_row_group()?;
        }

        let footer_offset = self.file.metadata()?.len();

        let footer = self.encode_footer()?;
        self.file.write_all(&footer)?;

        // Write footer offset (last 8 bytes of file)
        self.file.write_all(&(footer_offset as u64).to_le_bytes())?;

        self.file.sync_all()?;

        tracing::debug!(
            path = %self.path.display(),
            rows = self.total_rows,
            row_groups = self.row_groups.len(),
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

        self.row_groups.push(RowGroupMeta {
            offset,
            length: column_sizes.iter().sum(),
            row_count: group.row_count as u64,
            column_sizes,
            compressed_sizes,
            uncompressed_sizes: group.uncompressed_sizes,
        });

        Ok(())
    }

    fn encode_footer(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::with_capacity(FOOTER_SIZE);

        // Magic + version + generation
        buf.extend_from_slice(&SEGMENT_MAGIC.to_le_bytes());
        buf.extend_from_slice(&SEGMENT_VERSION.to_le_bytes());
        buf.extend_from_slice(&0u64.to_le_bytes()); // generation

        // Row count
        buf.extend_from_slice(&self.total_rows.to_le_bytes());

        // Row group count
        buf.extend_from_slice(&(self.row_groups.len() as u32).to_le_bytes());

        // Timestamps
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        buf.extend_from_slice(&now.to_le_bytes()); // created_at
        buf.extend_from_slice(&now.to_le_bytes()); // modified_at

        // Schema
        encode_len_prefixed_str(&mut buf, "");
        buf.extend_from_slice(&(self.schema.columns.len() as u32).to_le_bytes());
        for col in &self.schema.columns {
            encode_len_prefixed_str(&mut buf, &col.name);
            buf.push(col.data_type as u8);
            buf.push(if col.nullable { 1 } else { 0 });
            buf.extend_from_slice(&(col.scale as u32).to_le_bytes());
            buf.extend_from_slice(&(col.precision as u32).to_le_bytes());
        }

        // Row group metadata
        for rg in &self.row_groups {
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

        // Pad to FOOTER_SIZE
        if buf.len() < FOOTER_SIZE {
            buf.resize(FOOTER_SIZE, 0);
        }

        Ok(buf)
    }
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
}

#[allow(dead_code)]
struct RowGroupInfo {
    offset: u64,
    length: u64,
    row_count: u64,
    column_compressed_sizes: Vec<u64>,
    column_uncompressed_sizes: Vec<u64>,
}

impl SegmentReader {
    /// Open a segment file for reading via mmap.
    pub fn open(path: &Path) -> Result<Self> {
        let file = fs::File::open(path)?;
        let file_len = file.metadata()?.len();

        if file_len < 8 {
            return Err(MonaxError::Storage(format!("segment file too small: {} bytes", file_len)));
        }

        // Read the last 8 bytes to get footer offset
        let mut footer_offset_buf = [0u8; 8];
        let mut tmp_file = file;
        tmp_file.seek(SeekFrom::End(-8))?;
        tmp_file.read_exact(&mut footer_offset_buf)?;
        let footer_offset = u64::from_le_bytes(footer_offset_buf) as usize;

        if footer_offset > file_len as usize {
            return Err(MonaxError::Storage(format!(
                "invalid footer offset: {} > file size {}",
                footer_offset, file_len
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
        let mut batches = Vec::new();
        for rg_idx in 0..self.footer.row_groups.len() {
            let batch = self.read_row_group(rg_idx)?;
            if batch.num_rows() <= batch_size {
                batches.push(batch);
            } else {
                for start in (0..batch.num_rows()).step_by(batch_size) {
                    let end = (start + batch_size).min(batch.num_rows());
                    batches.push(split_batch(&batch, start, end));
                }
            }
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

            row_groups.push(RowGroupInfo {
                offset,
                length,
                row_count: rg_row_count,
                column_compressed_sizes: comp_sizes,
                column_uncompressed_sizes: uncompressed_sizes,
            });
        }

        Ok(SegmentFooter {
            version,
            row_count,
            row_groups,
            schema,
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

fn split_column(col: &ColumnArray, start: usize, end: usize) -> ColumnArray {
    let mut new_col = ColumnArray::empty(col.data_type());
    for i in start..end {
        new_col.push(&col.scalar(i)).unwrap();
    }
    new_col
}

// ---------------------------------------------------------------------------
// Footer helpers
// ---------------------------------------------------------------------------

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
}
