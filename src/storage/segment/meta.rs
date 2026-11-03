use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crc32fast::Hasher;

use crate::common::error::{Result, TaibaiError};
use crate::storage::partition::pruning::{compute_segment_zone_maps_from_row_groups, ZoneMap};
use crate::storage::segment::footer::{
    parse_segment_footer, parse_segment_footer_index, SegmentFooterIndex,
};
use crate::storage::segment::writer::encode_segment_footer;
use crate::storage::segment::{
    is_supported_segment_version, RowGroupMeta, SEGMENT_HEADER_SIZE, SEGMENT_MAGIC,
};
use crate::types::batch::Schema;

const SIDECAR_MAGIC: u32 = 0x4D58_4D54;
const SIDECAR_VERSION: u32 = 3;
const V3_RECORD_HEADER_SIZE: usize = 36;
const RECORD_CRC_SIZE: usize = 4;
const RECORD_KIND_FULL_SNAPSHOT: u32 = 1;
const RECORD_KIND_ROW_GROUPS: u32 = 2;

#[derive(Debug, Clone)]
pub struct SidecarFooter {
    pub data_end: u64,
    pub footer: Vec<u8>,
}

/// Parsed snapshot of a segment's metadata (sidecar or embedded footer).
///
/// # Column-index spaces
///
/// `schema`, `row_groups` and `segment_zone_maps` are FOOTER-space: indexed
/// by the segment's own column order, which drifts from the catalog's after
/// DROP/RENAME COLUMN while sealed segments keep their layout. The fields
/// are module-private on purpose — positional use with catalog-space
/// indices once pruned the wrong row groups and silently dropped rows.
/// Outside `crate::storage::segment` they are reachable only through the
/// `footer_*` accessors (whose names carry the space) or, for pruning,
/// through the catalog-aligned gateway `scan::row_filter::meta_prune_row_groups`.
#[derive(Debug, Clone)]
pub struct SegmentMetaSnapshot {
    pub data_end: u64,
    pub row_count: u64,
    pub delete_bitmap: Vec<u8>,
    pub deleted_row_count: u64,
    /// Resident index: schema, segment zone maps, and 12 bytes per row group
    /// (a `u32` footer offset and a `u64` row count). Row-group records —
    /// per-column sizes, zone maps, CRCs, sums — are decoded from `footer` on
    /// demand instead of being materialised here.
    index: SegmentFooterIndex,
    /// The footer bytes the index addresses, shared so cloning a snapshot
    /// (the metadata cache hands out `Arc`s) never copies the buffer.
    footer: std::sync::Arc<[u8]>,
}

impl SegmentMetaSnapshot {
    /// Resident heap footprint of this snapshot, in bytes.
    ///
    /// Measured on a real `md_trade` segment (2 GiB compressed, 210M rows,
    /// 28,315 row groups at the default 8192-row granule): fully parsing that
    /// footer produced ~80 MiB of heap — 0.3 bytes of metadata per row, which
    /// sounds like nothing until a segment holds 210M rows. The index holds
    /// the same segment in 0.33 MiB.
    ///
    /// This is what the metadata cache charges an entry, so the reported
    /// size is the real one.
    pub fn estimated_heap_bytes(&self) -> u64 {
        std::mem::size_of::<Self>() as u64
            + self.index.heap_bytes()
            + self.footer.len() as u64
    }

    /// The segment's own schema, in footer column order.
    pub fn footer_schema(&self) -> &Schema {
        &self.index.schema
    }

    /// Segment-level zone maps in footer column order.
    pub fn footer_zone_maps(&self) -> &[ZoneMap] {
        &self.index.segment_zone_maps
    }

    /// How many row groups the segment holds.
    pub fn row_group_count(&self) -> usize {
        self.index.num_row_groups()
    }

    /// Row count of one row group, straight from the resident index — no
    /// decode, no allocation.
    pub fn row_group_row_count(&self, row_group: usize) -> u64 {
        self.index.row_group_row_count(row_group)
    }

    /// Decode one row group's metadata on demand, in footer column order.
    pub fn row_group_meta(&self, row_group: usize) -> Result<RowGroupMeta> {
        self.index.decode_row_group(self.footer.as_ref(), row_group)
    }

    /// [`Self::row_group_meta`] into a caller-owned buffer, so walking every
    /// row group allocates nothing per row group.
    pub fn row_group_meta_into(&self, row_group: usize, out: &mut RowGroupMeta) -> Result<()> {
        self.index
            .decode_row_group_into(self.footer.as_ref(), row_group, out)
    }
}

pub fn sidecar_meta_path(segment_path: &Path) -> PathBuf {
    segment_path.with_extension("meta")
}

pub fn read_latest_sidecar_footer(segment_path: &Path) -> Result<Option<SidecarFooter>> {
    read_latest_sidecar_footer_from(&sidecar_meta_path(segment_path))
}

/// Read only the embedded footer at the end of a segment file.
///
/// This path deliberately avoids mmap and never reads column payload bytes. It
/// is used for sealed segments, whose embedded footer is the authoritative
/// metadata snapshot after their temporary sidecar has been removed.
pub fn read_embedded_footer(segment_path: &Path) -> Result<SidecarFooter> {
    let mut file = fs::File::open(segment_path)?;
    let file_len = file.metadata()?.len();
    if file_len < SEGMENT_HEADER_SIZE as u64 + 8 {
        return Err(TaibaiError::Storage(format!(
            "missing embedded segment footer for {}",
            segment_path.display()
        )));
    }

    let mut header = [0u8; 8];
    file.read_exact(&mut header)?;
    let magic = u32::from_le_bytes(header[0..4].try_into().unwrap());
    let version = u32::from_le_bytes(header[4..8].try_into().unwrap());
    if magic != SEGMENT_MAGIC || !is_supported_segment_version(version) {
        return Err(TaibaiError::Storage(format!(
            "invalid segment header for {}",
            segment_path.display()
        )));
    }

    file.seek(SeekFrom::End(-8))?;
    let mut offset = [0u8; 8];
    file.read_exact(&mut offset)?;
    let data_end = u64::from_le_bytes(offset);
    if data_end < SEGMENT_HEADER_SIZE as u64 || data_end > file_len - 8 {
        return Err(TaibaiError::Storage(format!(
            "invalid embedded footer offset {} (file size {}, file: {})",
            data_end,
            file_len,
            segment_path.display()
        )));
    }

    let footer_len = usize::try_from(file_len - 8 - data_end).map_err(|_| {
        TaibaiError::Storage(format!(
            "embedded footer is too large for {}",
            segment_path.display()
        ))
    })?;
    let mut footer = vec![0u8; footer_len];
    file.seek(SeekFrom::Start(data_end))?;
    file.read_exact(&mut footer)?;

    Ok(SidecarFooter { data_end, footer })
}

pub fn read_embedded_segment_meta(segment_path: &Path) -> Result<SegmentMetaSnapshot> {
    segment_meta_snapshot(read_embedded_footer(segment_path)?)
}

fn segment_meta_snapshot(source: SidecarFooter) -> Result<SegmentMetaSnapshot> {
    let mut index = parse_segment_footer_index(&source.footer)?;
    let row_count = index.row_count;
    let deleted_row_count = index.deleted_row_count;
    // Move the bitmap out of the index: a heavily deleted segment's bitmap
    // can reach megabytes, and holding it twice (once in the index, once on
    // the snapshot) would undo the point of the compact representation.
    let delete_bitmap = std::mem::take(&mut index.delete_bitmap);
    Ok(SegmentMetaSnapshot {
        data_end: source.data_end,
        row_count,
        delete_bitmap,
        deleted_row_count,
        index,
        footer: source.footer.into(),
    })
}

fn read_latest_sidecar_footer_from(meta_path: &Path) -> Result<Option<SidecarFooter>> {
    if !meta_path.exists() {
        return Ok(None);
    }

    let mut file = fs::File::open(meta_path)?;
    let mut buf = Vec::new();
    file.read_to_end(&mut buf)?;

    let mut offset = 0usize;
    let mut latest: Option<SidecarFooter> = None;
    let mut state: Option<SidecarState> = None;

    while offset + V3_RECORD_HEADER_SIZE + RECORD_CRC_SIZE <= buf.len() {
        let header = &buf[offset..offset + V3_RECORD_HEADER_SIZE];
        let magic = u32::from_le_bytes(header[0..4].try_into().unwrap());
        let version = u32::from_le_bytes(header[4..8].try_into().unwrap());

        if magic != SIDECAR_MAGIC {
            break;
        }

        let Some(header) = record_header(&buf, offset, version) else {
            break;
        };

        // All lengths come from disk; checked arithmetic prevents a
        // corrupted sidecar from wrapping around the bounds check and
        // panicking on the slice below.
        let Some(record_end) = offset
            .checked_add(header.header_len)
            .and_then(|v| v.checked_add(header.name_len))
            .and_then(|v| v.checked_add(header.footer_len))
            .and_then(|v| v.checked_add(RECORD_CRC_SIZE))
        else {
            break;
        };
        if record_end > buf.len() {
            break;
        }

        let Some(name_start) = offset.checked_add(header.header_len) else {
            break;
        };
        let Some(name_end) = name_start.checked_add(header.name_len) else {
            break;
        };
        let footer_start = name_end;
        let Some(footer_end) = footer_start.checked_add(header.footer_len) else {
            break;
        };
        let stored_crc = u32::from_le_bytes(buf[footer_end..record_end].try_into().unwrap());
        let computed_crc = compute_crc(&buf[offset..footer_end]);
        if stored_crc != computed_crc {
            break;
        }

        let footer = &buf[footer_start..footer_end];
        match header.kind {
            RECORD_KIND_FULL_SNAPSHOT => {
                state = Some(SidecarState::from_footer(header.data_end, footer)?);
            }
            RECORD_KIND_ROW_GROUPS => {
                let delta = parse_segment_footer(footer)?;
                if let Some(existing) = &mut state {
                    existing.data_end = header.data_end;
                    existing.row_count = delta.row_count;
                    existing.row_groups.extend(delta.row_groups);
                } else {
                    state = Some(SidecarState::from_parsed(header.data_end, delta));
                }
            }
            _ => break,
        }

        if let Some(existing) = &state {
            latest = Some(existing.to_footer());
        }
        offset = record_end;
    }

    Ok(latest)
}

pub fn append_sidecar_footer(segment_path: &Path, data_end: u64, footer: &[u8]) -> Result<()> {
    append_record(segment_path, RECORD_KIND_FULL_SNAPSHOT, data_end, footer)
}

pub fn append_sidecar_row_groups(
    segment_path: &Path,
    data_end: u64,
    schema: &Schema,
    total_rows: u64,
    row_groups: &[RowGroupMeta],
    table_name: &str,
) -> Result<()> {
    if row_groups.is_empty() {
        return Ok(());
    }
    let footer = encode_segment_footer(schema, total_rows, row_groups, &[], 0, table_name, &[]);
    append_record(segment_path, RECORD_KIND_ROW_GROUPS, data_end, &footer)
}

pub fn append_record(segment_path: &Path, kind: u32, data_end: u64, footer: &[u8]) -> Result<()> {
    let meta_path = sidecar_meta_path(segment_path);
    let name = segment_name(segment_path);
    let name_bytes = name.as_bytes();
    let mut record = Vec::with_capacity(
        V3_RECORD_HEADER_SIZE + name_bytes.len() + footer.len() + RECORD_CRC_SIZE,
    );
    record.extend_from_slice(&SIDECAR_MAGIC.to_le_bytes());
    record.extend_from_slice(&SIDECAR_VERSION.to_le_bytes());
    record.extend_from_slice(&kind.to_le_bytes());
    record.extend_from_slice(&0u32.to_le_bytes());
    record.extend_from_slice(&data_end.to_le_bytes());
    record.extend_from_slice(&(footer.len() as u64).to_le_bytes());
    record.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
    record.extend_from_slice(name_bytes);
    record.extend_from_slice(footer);
    let crc = compute_crc(&record);
    record.extend_from_slice(&crc.to_le_bytes());

    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&meta_path)?;
    file.write_all(&record)?;
    file.sync_all()?;
    Ok(())
}

pub fn compact_sidecar_footer_if_needed(
    segment_path: &Path,
    data_end: u64,
    footer: &[u8],
    min_records: usize,
    min_bytes: u64,
) -> Result<bool> {
    let meta_path = sidecar_meta_path(segment_path);
    let Ok(meta) = fs::metadata(&meta_path) else {
        return Ok(false);
    };

    let record_count = valid_record_count(&meta_path)?;
    if record_count < min_records && meta.len() < min_bytes {
        return Ok(false);
    }

    let tmp_path = compact_tmp_path(segment_path);
    match fs::remove_file(&tmp_path) {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => return Err(TaibaiError::Io(err)),
    }

    let name = segment_name(segment_path);
    let name_bytes = name.as_bytes();
    let mut record = Vec::with_capacity(
        V3_RECORD_HEADER_SIZE + name_bytes.len() + footer.len() + RECORD_CRC_SIZE,
    );
    record.extend_from_slice(&SIDECAR_MAGIC.to_le_bytes());
    record.extend_from_slice(&SIDECAR_VERSION.to_le_bytes());
    record.extend_from_slice(&RECORD_KIND_FULL_SNAPSHOT.to_le_bytes());
    record.extend_from_slice(&0u32.to_le_bytes());
    record.extend_from_slice(&data_end.to_le_bytes());
    record.extend_from_slice(&(footer.len() as u64).to_le_bytes());
    record.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
    record.extend_from_slice(name_bytes);
    record.extend_from_slice(footer);
    let crc = compute_crc(&record);
    record.extend_from_slice(&crc.to_le_bytes());

    {
        let mut file = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&tmp_path)?;
        file.write_all(&record)?;
        file.sync_all()?;
    }

    atomic_replace(&tmp_path, &meta_path)?;
    sync_parent_dir(&meta_path)?;
    Ok(true)
}

pub fn remove_sidecar_meta(segment_path: &Path) -> Result<()> {
    remove_file_if_exists(&sidecar_meta_path(segment_path))
}

fn remove_file_if_exists(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(TaibaiError::Io(err)),
    }
}

fn compact_tmp_path(segment_path: &Path) -> PathBuf {
    sidecar_meta_path(segment_path).with_extension("meta.compact.tmp")
}

fn valid_record_count(path: &Path) -> Result<usize> {
    let mut file = fs::File::open(path)?;
    let mut buf = Vec::new();
    file.read_to_end(&mut buf)?;

    let mut offset = 0usize;
    let mut count = 0usize;
    while offset + V3_RECORD_HEADER_SIZE + RECORD_CRC_SIZE <= buf.len() {
        let header = &buf[offset..offset + V3_RECORD_HEADER_SIZE];
        let magic = u32::from_le_bytes(header[0..4].try_into().unwrap());
        let version = u32::from_le_bytes(header[4..8].try_into().unwrap());
        if magic != SIDECAR_MAGIC {
            break;
        }

        let Some(header) = record_header(&buf, offset, version) else {
            break;
        };
        let record_end =
            offset + header.header_len + header.name_len + header.footer_len + RECORD_CRC_SIZE;
        if record_end > buf.len() {
            break;
        }

        let footer_end = offset + header.header_len + header.name_len + header.footer_len;
        let stored_crc = u32::from_le_bytes(buf[footer_end..record_end].try_into().unwrap());
        if stored_crc != compute_crc(&buf[offset..footer_end]) {
            break;
        }

        count += 1;
        offset = record_end;
    }
    Ok(count)
}

#[cfg(windows)]
pub(crate) fn atomic_replace(src: &Path, dst: &Path) -> Result<()> {
    use std::os::windows::ffi::OsStrExt;

    const MOVEFILE_REPLACE_EXISTING: u32 = 0x1;
    const MOVEFILE_WRITE_THROUGH: u32 = 0x8;

    unsafe extern "system" {
        fn MoveFileExW(
            lpExistingFileName: *const u16,
            lpNewFileName: *const u16,
            dwFlags: u32,
        ) -> i32;
    }

    let src_wide: Vec<u16> = src.as_os_str().encode_wide().chain(Some(0)).collect();
    let dst_wide: Vec<u16> = dst.as_os_str().encode_wide().chain(Some(0)).collect();
    let ok = unsafe {
        MoveFileExW(
            src_wide.as_ptr(),
            dst_wide.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    };
    if ok == 0 {
        return Err(TaibaiError::Io(std::io::Error::last_os_error()));
    }
    Ok(())
}

#[cfg(not(windows))]
pub(crate) fn atomic_replace(src: &Path, dst: &Path) -> Result<()> {
    fs::rename(src, dst)?;
    Ok(())
}

#[cfg(windows)]
fn sync_parent_dir(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(not(windows))]
pub(crate) fn sync_parent_dir(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::File::open(parent)?.sync_all()?;
    }
    Ok(())
}

fn compute_crc(data: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(data);
    hasher.finalize()
}

fn segment_name(segment_path: &Path) -> String {
    segment_path
        .file_name()
        .map(|name| name.to_string_lossy().to_string())
        .unwrap_or_default()
}

struct RecordHeader {
    kind: u32,
    data_end: u64,
    footer_len: usize,
    header_len: usize,
    name_len: usize,
}

fn record_header(buf: &[u8], offset: usize, version: u32) -> Option<RecordHeader> {
    if version != SIDECAR_VERSION || offset + V3_RECORD_HEADER_SIZE > buf.len() {
        return None;
    }
    let header = &buf[offset..offset + V3_RECORD_HEADER_SIZE];
    Some(RecordHeader {
        kind: u32::from_le_bytes(header[8..12].try_into().ok()?),
        data_end: u64::from_le_bytes(header[16..24].try_into().ok()?),
        footer_len: u64::from_le_bytes(header[24..32].try_into().ok()?) as usize,
        name_len: u32::from_le_bytes(header[32..36].try_into().ok()?) as usize,
        header_len: V3_RECORD_HEADER_SIZE,
    })
}

struct SidecarState {
    data_end: u64,
    schema: Schema,
    row_count: u64,
    row_groups: Vec<RowGroupMeta>,
    delete_bitmap: Vec<u8>,
    deleted_row_count: u64,
}

impl SidecarState {
    fn from_footer(data_end: u64, footer: &[u8]) -> Result<Self> {
        let parsed = parse_segment_footer(footer)?;
        Ok(Self::from_parsed(data_end, parsed))
    }

    fn from_parsed(
        data_end: u64,
        parsed: crate::storage::segment::footer::ParsedSegmentFooter,
    ) -> Self {
        Self {
            data_end,
            schema: parsed.schema,
            row_count: parsed.row_count,
            row_groups: parsed.row_groups,
            delete_bitmap: parsed.delete_bitmap,
            deleted_row_count: parsed.deleted_row_count,
        }
    }

    fn to_footer(&self) -> SidecarFooter {
        let segment_zone_maps =
            compute_segment_zone_maps_from_row_groups(&self.row_groups, self.schema.num_columns());
        SidecarFooter {
            data_end: self.data_end,
            footer: encode_segment_footer(
                &self.schema,
                self.row_count,
                &self.row_groups,
                &self.delete_bitmap,
                self.deleted_row_count,
                "",
                &segment_zone_maps,
            ),
        }
    }
}
