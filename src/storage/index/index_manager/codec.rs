use std::collections::{HashMap, HashSet};
use std::io::{Cursor, Read, Write};
use std::path::Path;

use super::types::{
    PreparedUniqueIndexEntry, UniqueIndexBlock, UniqueIndexBlockEntries, UniqueIndexBlockRef,
    UniqueIndexData, UniqueIndexDeltaState, UniqueIndexEntry, UniqueIndexProgress,
    UniqueRowLocation,
};
use crate::common::error::{Result, TaibaiError};
use crate::types::scalar::ScalarValue;
pub(super) const UNIQUE_INDEX_MAGIC_V5: &[u8; 8] = b"TBIUKV5\0";
/// V6 rewrites the base layout for on-demand reads: entry runs sit between a
/// small header and a footer that carries the block directory (first key,
/// entry range, byte range per block). Lookups parse header + footer only and
/// read the bytes of blocks they touch; merge passes stream entries straight
/// into the file instead of materializing the whole snapshot in memory.
pub(super) const UNIQUE_INDEX_MAGIC_V6: &[u8; 8] = b"TBIUKV6\0";
pub(super) const UNIQUE_INDEX_DELTA_MAGIC_V2: &[u8; 8] = b"TBIUKD2\0";
pub(super) const UNIQUE_INDEX_PROGRESS_MAGIC: &[u8; 8] = b"TBIUKP1\0";
pub(super) const UNIQUE_INDEX_BLOCK_SIZE: usize = 256;
#[cfg(test)]
pub(super) const UNIQUE_INDEX_DELTA_COMPACT_MIN_ENTRIES: u64 = 32;
#[cfg(not(test))]
pub(super) const UNIQUE_INDEX_DELTA_COMPACT_MIN_ENTRIES: u64 = 262_144;
#[cfg(test)]
pub(super) const UNIQUE_INDEX_DELTA_COMPACT_MAX_ENTRIES: u64 = 4_096;
#[cfg(not(test))]
/// Absolute ceiling that keeps the in-memory delta map bounded even when the
/// base snapshot is far larger than the ratio threshold alone would imply.
pub(super) const UNIQUE_INDEX_DELTA_COMPACT_MAX_ENTRIES: u64 = 4_194_304;
pub(super) const UNIQUE_INDEX_DELTA_COMPACT_BASE_RATIO: u64 = 2;

pub(super) fn unique_index_delta_needs_compaction(delta_count: u64, base_count: usize) -> bool {
    let threshold = UNIQUE_INDEX_DELTA_COMPACT_MIN_ENTRIES
        .max(base_count as u64 / UNIQUE_INDEX_DELTA_COMPACT_BASE_RATIO)
        .min(UNIQUE_INDEX_DELTA_COMPACT_MAX_ENTRIES);
    delta_count >= threshold
}

pub(super) fn encode_unique_key(key: &[ScalarValue]) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    put_u32(&mut out, key.len() as u32);
    for value in key {
        encode_scalar_value(&mut out, value)?;
    }
    Ok(out)
}

/// Streams an on-demand (V6) base file: header, then entry runs grouped into
/// blocks of [`UNIQUE_INDEX_BLOCK_SIZE`], then a footer carrying the block
/// directory, the segment-path table and the progress watermark. Entry bytes
/// are written as they are pushed, so callers never hold the whole snapshot
/// in memory; the only resident state is the directory (one small record per
/// 256 entries) plus the open block's run buffer.
pub(super) struct UniqueIndexBaseWriter<W: Write> {
    writer: W,
    bytes_written: u64,
    db: String,
    table: String,
    index_name: String,
    columns: Vec<String>,
    next_entry_index: usize,
    run: Vec<u8>,
    run_entries: usize,
    run_first_key: Vec<u8>,
    blocks: Vec<UniqueIndexBlockRef>,
    paths: Vec<String>,
    path_ids: HashMap<String, u32>,
}

impl<W: Write> UniqueIndexBaseWriter<W> {
    pub(super) fn new(
        writer: W,
        db: &str,
        table: &str,
        index_name: &str,
        columns: Vec<String>,
    ) -> Result<Self> {
        let mut out = Self {
            writer,
            bytes_written: 0,
            db: db.to_string(),
            table: table.to_string(),
            index_name: index_name.to_string(),
            columns,
            next_entry_index: 0,
            run: Vec::with_capacity(UNIQUE_INDEX_BLOCK_SIZE * 32),
            run_entries: 0,
            run_first_key: Vec::new(),
            blocks: Vec::new(),
            paths: Vec::new(),
            path_ids: HashMap::new(),
        };
        let mut header = Vec::with_capacity(64);
        header.extend_from_slice(UNIQUE_INDEX_MAGIC_V6);
        put_string(&mut header, &out.db);
        put_string(&mut header, &out.table);
        put_string(&mut header, &out.index_name);
        put_u32(&mut header, out.columns.len() as u32);
        for column in &out.columns {
            put_string(&mut header, column);
        }
        out.write_all_bytes(&header)?;
        Ok(out)
    }

    fn write_all_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        self.writer.write_all(bytes)?;
        self.bytes_written += bytes.len() as u64;
        Ok(())
    }

    fn path_id(&mut self, segment_path: &str) -> u32 {
        if let Some(id) = self.path_ids.get(segment_path) {
            return *id;
        }
        let id = self.paths.len() as u32;
        self.paths.push(segment_path.to_string());
        self.path_ids.insert(segment_path.to_string(), id);
        id
    }

    /// Append one entry. Entries must be pushed in ascending encoded-key
    /// order; the sort that guarantees this happens on the caller's side.
    pub(super) fn push_entry(
        &mut self,
        encoded_key: &[u8],
        location: &UniqueRowLocation,
    ) -> Result<()> {
        if self.run_entries == 0 {
            self.run_first_key = encoded_key.to_vec();
        }
        let prefix_len = common_prefix_len(&self.run_first_key, encoded_key);
        put_varint(&mut self.run, prefix_len as u64);
        put_varint(&mut self.run, (encoded_key.len() - prefix_len) as u64);
        self.run.extend_from_slice(&encoded_key[prefix_len..]);
        let path_id = self.path_id(&location.segment_path);
        put_varint(&mut self.run, path_id as u64);
        put_varint(&mut self.run, location.row_id);
        self.run_entries += 1;
        self.next_entry_index += 1;
        if self.run_entries == UNIQUE_INDEX_BLOCK_SIZE {
            self.seal_block()?;
        }
        Ok(())
    }

    fn seal_block(&mut self) -> Result<()> {
        if self.run_entries == 0 {
            return Ok(());
        }
        let block = UniqueIndexBlockRef {
            first_key: std::mem::take(&mut self.run_first_key),
            start: self.next_entry_index - self.run_entries,
            len: self.run_entries,
            data_offset: self.bytes_written,
            data_len: self.run.len() as u32,
        };
        let run = std::mem::take(&mut self.run);
        self.write_all_bytes(&run)?;
        self.run_entries = 0;
        self.blocks.push(block);
        Ok(())
    }

    /// Seal the open block, append the footer and flush. Returns the inner
    /// writer so the caller can fsync before renaming the file into place.
    pub(super) fn finish(
        mut self,
        flush_epoch: Option<u64>,
        segment_watermarks: &HashMap<String, u64>,
    ) -> Result<W> {
        self.seal_block()?;
        let mut footer = Vec::with_capacity(64 + self.blocks.len() * 48);
        put_u64(&mut footer, self.next_entry_index as u64);
        put_u32(&mut footer, UNIQUE_INDEX_BLOCK_SIZE as u32);
        put_u32(&mut footer, self.blocks.len() as u32);
        for block in &self.blocks {
            put_bytes(&mut footer, &block.first_key);
            put_u64(&mut footer, block.start as u64);
            put_u32(&mut footer, block.len as u32);
            put_u64(&mut footer, block.data_offset);
            put_u32(&mut footer, block.data_len);
        }
        put_u32(&mut footer, self.paths.len() as u32);
        for path in &self.paths {
            put_string(&mut footer, path);
        }
        footer.push(if flush_epoch.is_some() { 1 } else { 0 });
        put_u64(&mut footer, flush_epoch.unwrap_or(0));
        put_u32(&mut footer, segment_watermarks.len() as u32);
        for (segment_path, row_count) in segment_watermarks {
            put_string(&mut footer, segment_path);
            put_u64(&mut footer, *row_count);
        }
        let footer_offset = self.bytes_written;
        self.write_all_bytes(&footer)?;
        self.write_all_bytes(&footer_offset.to_le_bytes())?;
        self.writer.flush()?;
        Ok(self.writer)
    }
}

/// Decode one block's entry run: per entry, a key stored as prefix lengths
/// against the block's first key plus a path-table reference and row id.
pub(super) fn decode_unique_index_block_run(
    bytes: &[u8],
    first_key: &[u8],
    paths: &[String],
) -> Result<UniqueIndexBlockEntries> {
    let mut cursor = Cursor::new(bytes);
    let mut block = UniqueIndexBlockEntries::default();
    while (cursor.position() as usize) < bytes.len() {
        let prefix_len = read_varint(&mut cursor)? as usize;
        let suffix_len = read_varint(&mut cursor)? as usize;
        if prefix_len > first_key.len() {
            return Err(TaibaiError::Storage(
                "corrupt unique index block key prefix".to_string(),
            ));
        }
        let mut encoded_key = Vec::with_capacity(prefix_len + suffix_len);
        encoded_key.extend_from_slice(&first_key[..prefix_len]);
        let mut suffix = vec![0u8; suffix_len];
        cursor.read_exact(&mut suffix)?;
        encoded_key.extend_from_slice(&suffix);
        let path_idx = read_varint(&mut cursor)? as usize;
        let row_id = read_varint(&mut cursor)?;
        let Some(segment_path) = paths.get(path_idx) else {
            return Err(TaibaiError::Storage(
                "corrupt unique index block path reference".to_string(),
            ));
        };
        block.keys.push(encoded_key);
        block.locations.push(UniqueRowLocation {
            segment_path: segment_path.clone(),
            row_id,
        });
    }
    Ok(block)
}

/// Parse-only readers used by the on-demand base loader, which reads fields
/// straight from the file instead of buffering it whole.
pub(super) fn read_file_u8(file: &mut std::fs::File) -> Result<u8> {
    use std::io::Read;
    let mut buf = [0u8; 1];
    file.read_exact(&mut buf)?;
    Ok(buf[0])
}

pub(super) fn read_file_u32(file: &mut std::fs::File) -> Result<u32> {
    use std::io::Read;
    let mut buf = [0u8; 4];
    file.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

pub(super) fn read_file_u64(file: &mut std::fs::File) -> Result<u64> {
    use std::io::Read;
    let mut buf = [0u8; 8];
    file.read_exact(&mut buf)?;
    Ok(u64::from_le_bytes(buf))
}

pub(super) fn read_file_bytes(file: &mut std::fs::File) -> Result<Vec<u8>> {
    let len = read_file_u32(file)? as usize;
    use std::io::Read;
    let mut bytes = vec![0u8; len];
    file.read_exact(&mut bytes)?;
    Ok(bytes)
}

pub(super) fn read_file_string(file: &mut std::fs::File) -> Result<String> {
    let bytes = read_file_bytes(file)?;
    String::from_utf8(bytes)
        .map_err(|err| TaibaiError::Storage(format!("decode unique index string: {}", err)))
}

pub(super) fn append_prepared_unique_index_delta(
    path: &Path,
    entries: &[PreparedUniqueIndexEntry],
) -> Result<()> {
    let is_new = !path.exists() || std::fs::metadata(path)?.len() == 0;
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    if is_new {
        file.write_all(UNIQUE_INDEX_DELTA_MAGIC_V2)?;
    }
    for addition in entries {
        let entry = &addition.entry;
        let mut record = Vec::new();
        record.push(1);
        put_bytes(&mut record, &addition.encoded_key);
        put_u32(&mut record, entry.key.len() as u32);
        for value in &entry.key {
            encode_scalar_value(&mut record, value)?;
        }
        put_string(&mut record, &entry.location.segment_path);
        put_u64(&mut record, entry.location.row_id);
        write_u32(&mut file, record.len() as u32)?;
        file.write_all(&record)?;
    }
    file.sync_all()?;
    Ok(())
}

pub(super) fn append_unique_index_tombstone_delta(
    path: &Path,
    tombstones: &[UniqueRowLocation],
) -> Result<()> {
    append_unique_index_delta_records(path, &[], tombstones, &[])
}

pub(super) fn append_unique_index_removed_key_delta(
    path: &Path,
    removed_keys: &[Vec<u8>],
) -> Result<()> {
    append_unique_index_delta_records(path, &[], &[], removed_keys)
}

pub(super) fn append_unique_index_delta_records(
    path: &Path,
    entries: &[UniqueIndexEntry],
    tombstones: &[UniqueRowLocation],
    removed_keys: &[Vec<u8>],
) -> Result<()> {
    let is_new = !path.exists() || std::fs::metadata(path)?.len() == 0;
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    if is_new {
        file.write_all(UNIQUE_INDEX_DELTA_MAGIC_V2)?;
    }
    for entry in entries {
        let mut record = Vec::new();
        record.push(1);
        put_bytes(&mut record, &encode_unique_key(&entry.key)?);
        put_u32(&mut record, entry.key.len() as u32);
        for value in &entry.key {
            encode_scalar_value(&mut record, value)?;
        }
        put_string(&mut record, &entry.location.segment_path);
        put_u64(&mut record, entry.location.row_id);
        write_u32(&mut file, record.len() as u32)?;
        file.write_all(&record)?;
    }
    for location in tombstones {
        let mut record = Vec::new();
        record.push(2);
        put_string(&mut record, &location.segment_path);
        put_u64(&mut record, location.row_id);
        write_u32(&mut file, record.len() as u32)?;
        file.write_all(&record)?;
    }
    for encoded in removed_keys {
        let mut record = Vec::new();
        record.push(3);
        put_bytes(&mut record, encoded);
        write_u32(&mut file, record.len() as u32)?;
        file.write_all(&record)?;
    }
    file.sync_all()?;
    Ok(())
}

pub(super) enum UniqueIndexDeltaRecord {
    Entry(PreparedUniqueIndexEntry),
    Tombstone(UniqueRowLocation),
    RemovedKey(Vec<u8>),
}

#[derive(Default)]
pub(super) struct UniqueIndexDelta {
    pub(super) records: Vec<UniqueIndexDeltaRecord>,
}

impl UniqueIndexDelta {
    pub(super) fn into_state(self) -> Result<UniqueIndexDeltaState> {
        let mut state = UniqueIndexDeltaState::default();
        for record in self.records {
            match record {
                UniqueIndexDeltaRecord::Entry(addition) => {
                    state
                        .key_locations
                        .entry(addition.encoded_key.clone())
                        .or_default()
                        .push(addition.entry.location);
                    state.entry_count += 1;
                }
                UniqueIndexDeltaRecord::Tombstone(location) => {
                    state.tombstones.insert(location);
                }
                UniqueIndexDeltaRecord::RemovedKey(encoded) => {
                    if let Some(locations) = state.key_locations.remove(&encoded) {
                        state.entry_count =
                            state.entry_count.saturating_sub(locations.len() as u64);
                    }
                    state.removed_keys.insert(encoded);
                }
            }
        }
        Ok(state)
    }
}

pub(super) fn encode_unique_index_progress(progress: &UniqueIndexProgress) -> Vec<u8> {
    let mut out = UNIQUE_INDEX_PROGRESS_MAGIC.to_vec();
    out.push(if progress.flush_epoch.is_some() { 1 } else { 0 });
    put_u64(&mut out, progress.flush_epoch.unwrap_or(0));
    put_u32(&mut out, progress.segment_watermarks.len() as u32);
    for (path, row_count) in &progress.segment_watermarks {
        put_string(&mut out, path);
        put_u64(&mut out, *row_count);
    }
    out
}

pub(super) fn decode_unique_index_progress(bytes: &[u8]) -> UniqueIndexProgress {
    if bytes.len() < 8 || &bytes[..8] != UNIQUE_INDEX_PROGRESS_MAGIC {
        return UniqueIndexProgress::default();
    }
    let mut cursor = Cursor::new(&bytes[8..]);
    let mut progress = UniqueIndexProgress::default();
    let (Ok(flag), Ok(epoch), Ok(count)) = (
        read_u8(&mut cursor),
        read_u64(&mut cursor),
        read_u32(&mut cursor),
    ) else {
        return UniqueIndexProgress::default();
    };
    progress.flush_epoch = (flag == 1).then_some(epoch);
    for _ in 0..count {
        let (Ok(path), Ok(row_count)) = (read_string(&mut cursor), read_u64(&mut cursor)) else {
            return UniqueIndexProgress::default();
        };
        progress.segment_watermarks.insert(path, row_count);
    }
    progress
}

pub(super) fn decode_unique_index_delta(bytes: &[u8]) -> Result<UniqueIndexDelta> {
    if bytes.is_empty() {
        return Ok(UniqueIndexDelta::default());
    }
    let mut cursor = Cursor::new(bytes);
    let mut magic = [0u8; 8];
    cursor.read_exact(&mut magic)?;
    if &magic != UNIQUE_INDEX_DELTA_MAGIC_V2 {
        return Err(TaibaiError::Storage(
            "invalid unique index delta file".to_string(),
        ));
    }
    decode_unique_index_delta_records(bytes, cursor.position() as usize)
}

/// Decodes length-framed delta records starting at `offset`. Offset 8 skips
/// the file magic; offset 0 decodes a suffix appended after a compaction
/// snapshot, which is how appends that raced an in-flight merge survive.
pub(super) fn decode_unique_index_delta_records(
    bytes: &[u8],
    offset: usize,
) -> Result<UniqueIndexDelta> {
    let tail = &bytes[offset.min(bytes.len())..];
    let mut delta = UniqueIndexDelta::default();
    let mut cursor = Cursor::new(tail);
    while (cursor.position() as usize) < tail.len() {
        let len = read_u32(&mut cursor)? as usize;
        let start = cursor.position() as usize;
        let end = start + len;
        if end > tail.len() {
            return Err(TaibaiError::Storage(
                "truncated unique index delta record".to_string(),
            ));
        }
        let record = &tail[start..end];
        let mut record_cursor = Cursor::new(record);
        let record_type = read_u8(&mut record_cursor)?;
        match record_type {
            1 => {
                let encoded_key = read_bytes(&mut record_cursor)?;
                let entry = read_unique_index_entry(&mut record_cursor)?;
                delta
                    .records
                    .push(UniqueIndexDeltaRecord::Entry(PreparedUniqueIndexEntry {
                        encoded_key,
                        entry,
                    }));
            }
            2 => {
                let segment_path = read_string(&mut record_cursor)?;
                let row_id = read_u64(&mut record_cursor)?;
                delta
                    .records
                    .push(UniqueIndexDeltaRecord::Tombstone(UniqueRowLocation {
                        segment_path,
                        row_id,
                    }));
            }
            3 => {
                let encoded_key = read_bytes(&mut record_cursor)?;
                delta
                    .records
                    .push(UniqueIndexDeltaRecord::RemovedKey(encoded_key));
            }
            other => {
                return Err(TaibaiError::Storage(format!(
                    "invalid unique index delta record type {}",
                    other
                )));
            }
        }
        cursor.set_position(end as u64);
    }
    Ok(delta)
}

pub(super) fn read_unique_index_entry(cursor: &mut Cursor<&[u8]>) -> Result<UniqueIndexEntry> {
    let key_len = read_u32(cursor)? as usize;
    let mut key = Vec::with_capacity(key_len);
    for _ in 0..key_len {
        key.push(decode_scalar_value(cursor)?);
    }
    let segment_path = read_string(cursor)?;
    let row_id = read_u64(cursor)?;
    Ok(UniqueIndexEntry {
        key,
        location: UniqueRowLocation {
            segment_path,
            row_id,
        },
    })
}

pub(super) fn write_u32<W: Write>(writer: &mut W, value: u32) -> Result<()> {
    writer.write_all(&value.to_le_bytes())?;
    Ok(())
}
pub(super) fn decode_unique_index_data(bytes: &[u8]) -> Result<Option<UniqueIndexData>> {
    let mut cursor = Cursor::new(bytes);
    let mut magic = [0u8; 8];
    cursor.read_exact(&mut magic)?;
    if &magic != UNIQUE_INDEX_MAGIC_V5 {
        return Err(TaibaiError::Storage(
            "unsupported unique index version".to_string(),
        ));
    }
    decode_unique_index_data_v5(&mut cursor)
}

pub(super) fn decode_unique_index_data_v5(
    cursor: &mut Cursor<&[u8]>,
) -> Result<Option<UniqueIndexData>> {
    let Some((mut data, paths)) = decode_unique_index_data_blocks_with_paths(cursor)? else {
        return Ok(None);
    };
    let mut flag = [0u8; 1];
    cursor.read_exact(&mut flag)?;
    let epoch = read_u64(cursor)?;
    data.flush_epoch = (flag[0] == 1).then_some(epoch);
    let watermark_count = read_u32(cursor)? as usize;
    for _ in 0..watermark_count {
        let path_idx = read_varint(cursor)? as usize;
        let row_count = read_u64(cursor)?;
        let Some(segment_path) = paths.get(path_idx) else {
            return Err(TaibaiError::Storage(
                "corrupt unique index watermark path reference".to_string(),
            ));
        };
        data.segment_watermarks
            .insert(segment_path.clone(), row_count);
    }
    Ok(Some(data))
}

pub(super) fn decode_unique_index_data_blocks_with_paths(
    cursor: &mut Cursor<&[u8]>,
) -> Result<Option<(UniqueIndexData, Vec<String>)>> {
    let db = read_string(cursor)?;
    let table = read_string(cursor)?;
    let index_name = read_string(cursor)?;
    let column_count = read_u32(cursor)? as usize;
    let mut columns = Vec::with_capacity(column_count);
    for _ in 0..column_count {
        columns.push(read_string(cursor)?);
    }
    let entry_count = read_u64(cursor)? as usize;
    let _block_size = read_u32(cursor)? as usize;
    let block_count = read_u32(cursor)? as usize;
    let mut blocks = Vec::with_capacity(block_count);
    for _ in 0..block_count {
        blocks.push(UniqueIndexBlock {
            start: read_u64(cursor)? as usize,
            len: read_u32(cursor)? as usize,
            first_key: read_bytes(cursor)?,
        });
    }
    let path_count = read_u32(cursor)? as usize;
    let mut paths = Vec::with_capacity(path_count);
    for _ in 0..path_count {
        paths.push(read_string(cursor)?);
    }
    let mut entries = Vec::with_capacity(entry_count);
    let mut encoded_keys = Vec::with_capacity(entry_count);
    for block in &blocks {
        for _ in 0..block.len {
            let prefix_len = read_varint(cursor)? as usize;
            let suffix_len = read_varint(cursor)? as usize;
            if prefix_len > block.first_key.len() {
                return Err(TaibaiError::Storage(
                    "corrupt unique index key prefix".to_string(),
                ));
            }
            let mut encoded_key = Vec::with_capacity(prefix_len + suffix_len);
            encoded_key.extend_from_slice(&block.first_key[..prefix_len]);
            let mut suffix = vec![0u8; suffix_len];
            cursor.read_exact(&mut suffix)?;
            encoded_key.extend_from_slice(&suffix);
            let path_idx = read_varint(cursor)? as usize;
            let row_id = read_varint(cursor)?;
            let Some(segment_path) = paths.get(path_idx) else {
                return Err(TaibaiError::Storage(
                    "corrupt unique index path reference".to_string(),
                ));
            };
            let key = decode_encoded_unique_key(&encoded_key)?;
            encoded_keys.push(encoded_key);
            entries.push(UniqueIndexEntry {
                key,
                location: UniqueRowLocation {
                    segment_path: segment_path.clone(),
                    row_id,
                },
            });
        }
    }
    if entries.len() != entry_count {
        return Err(TaibaiError::Storage(
            "corrupt unique index entry count".to_string(),
        ));
    }
    Ok(Some((
        UniqueIndexData {
            db,
            table,
            index_name,
            columns,
            entries,
            flush_epoch: None,
            segment_watermarks: HashMap::new(),
            tombstones: HashSet::new(),
            encoded_keys,
            blocks,
        },
        paths,
    )))
}

pub(super) fn decode_encoded_unique_key(encoded: &[u8]) -> Result<Vec<ScalarValue>> {
    let mut cursor = Cursor::new(encoded);
    let count = read_u32(&mut cursor)? as usize;
    let mut key = Vec::with_capacity(count);
    for _ in 0..count {
        key.push(decode_scalar_value(&mut cursor)?);
    }
    Ok(key)
}

pub(super) fn common_prefix_len(left: &[u8], right: &[u8]) -> usize {
    left.iter()
        .zip(right.iter())
        .take_while(|(left, right)| left == right)
        .count()
}

pub(super) fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

pub(super) fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

pub(super) fn put_varint(out: &mut Vec<u8>, mut value: u64) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

pub(super) fn put_i64(out: &mut Vec<u8>, value: i64) {
    out.extend_from_slice(&value.to_le_bytes());
}

pub(super) fn put_string(out: &mut Vec<u8>, value: &str) {
    let bytes = value.as_bytes();
    put_u32(out, bytes.len() as u32);
    out.extend_from_slice(bytes);
}

pub(super) fn put_bytes(out: &mut Vec<u8>, value: &[u8]) {
    put_u32(out, value.len() as u32);
    out.extend_from_slice(value);
}

pub(super) fn read_u8(cursor: &mut Cursor<&[u8]>) -> Result<u8> {
    let mut buf = [0u8; 1];
    cursor.read_exact(&mut buf)?;
    Ok(buf[0])
}

pub(super) fn read_varint(cursor: &mut Cursor<&[u8]>) -> Result<u64> {
    let mut value = 0u64;
    let mut shift = 0u32;
    loop {
        let byte = read_u8(cursor)?;
        value |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
        shift += 7;
        if shift >= 64 {
            return Err(TaibaiError::Storage(
                "invalid varint in unique index".to_string(),
            ));
        }
    }
}

pub(super) fn read_u32(cursor: &mut Cursor<&[u8]>) -> Result<u32> {
    let mut bytes = [0u8; 4];
    cursor.read_exact(&mut bytes)?;
    Ok(u32::from_le_bytes(bytes))
}

pub(super) fn read_u64(cursor: &mut Cursor<&[u8]>) -> Result<u64> {
    let mut bytes = [0u8; 8];
    cursor.read_exact(&mut bytes)?;
    Ok(u64::from_le_bytes(bytes))
}

pub(super) fn read_i64(cursor: &mut Cursor<&[u8]>) -> Result<i64> {
    let mut bytes = [0u8; 8];
    cursor.read_exact(&mut bytes)?;
    Ok(i64::from_le_bytes(bytes))
}

pub(super) fn read_string(cursor: &mut Cursor<&[u8]>) -> Result<String> {
    let bytes = read_bytes(cursor)?;
    String::from_utf8(bytes)
        .map_err(|err| TaibaiError::Storage(format!("decode unique index string: {}", err)))
}

pub(super) fn read_bytes(cursor: &mut Cursor<&[u8]>) -> Result<Vec<u8>> {
    let len = read_u32(cursor)? as usize;
    let mut bytes = vec![0u8; len];
    cursor.read_exact(&mut bytes)?;
    Ok(bytes)
}

pub(super) fn encode_scalar_value(out: &mut Vec<u8>, value: &ScalarValue) -> Result<()> {
    match value {
        ScalarValue::Bool(v) => encode_option_bool(out, 1, *v),
        ScalarValue::Int8(v) => encode_option_i64(out, 2, v.map(|v| v as i64)),
        ScalarValue::Int16(v) => encode_option_i64(out, 3, v.map(|v| v as i64)),
        ScalarValue::Int32(v) => encode_option_i64(out, 4, v.map(|v| v as i64)),
        ScalarValue::Int64(v) => encode_option_i64(out, 5, *v),
        ScalarValue::UInt8(v) => encode_option_u64(out, 6, v.map(|v| v as u64)),
        ScalarValue::UInt16(v) => encode_option_u64(out, 7, v.map(|v| v as u64)),
        ScalarValue::UInt32(v) => encode_option_u64(out, 8, v.map(|v| v as u64)),
        ScalarValue::UInt64(v) => encode_option_u64(out, 9, *v),
        ScalarValue::Float32(v) => encode_option_u64(out, 10, v.map(|v| v.to_bits() as u64)),
        ScalarValue::Float64(v) => encode_option_u64(out, 11, v.map(|v| v.to_bits())),
        ScalarValue::Decimal(v) => encode_option_i64(out, 12, *v),
        ScalarValue::Date(v) => encode_option_i64(out, 13, v.map(|v| v as i64)),
        ScalarValue::Time(v) => encode_option_i64(out, 14, *v),
        ScalarValue::Timestamp(v) => encode_option_i64(out, 15, *v),
        ScalarValue::Interval(v) => encode_option_i64(out, 16, *v),
        ScalarValue::VarChar(v) => encode_option_string(out, 17, v.as_deref()),
        ScalarValue::Blob(v) => encode_option_bytes(out, 18, v.as_deref()),
        ScalarValue::Vector(v) => {
            out.push(19);
            match v {
                Some(values) => {
                    out.push(1);
                    put_u32(out, values.len() as u32);
                    for value in values {
                        out.extend_from_slice(&value.to_bits().to_le_bytes());
                    }
                }
                None => out.push(0),
            }
        }
    }
    Ok(())
}

pub(super) fn encode_option_bool(out: &mut Vec<u8>, tag: u8, value: Option<bool>) {
    out.push(tag);
    match value {
        Some(value) => {
            out.push(1);
            out.push(u8::from(value));
        }
        None => out.push(0),
    }
}

pub(super) fn encode_option_i64(out: &mut Vec<u8>, tag: u8, value: Option<i64>) {
    out.push(tag);
    match value {
        Some(value) => {
            out.push(1);
            put_i64(out, value);
        }
        None => out.push(0),
    }
}

pub(super) fn encode_option_u64(out: &mut Vec<u8>, tag: u8, value: Option<u64>) {
    out.push(tag);
    match value {
        Some(value) => {
            out.push(1);
            put_u64(out, value);
        }
        None => out.push(0),
    }
}

pub(super) fn encode_option_string(out: &mut Vec<u8>, tag: u8, value: Option<&str>) {
    out.push(tag);
    match value {
        Some(value) => {
            out.push(1);
            put_string(out, value);
        }
        None => out.push(0),
    }
}

pub(super) fn encode_option_bytes(out: &mut Vec<u8>, tag: u8, value: Option<&[u8]>) {
    out.push(tag);
    match value {
        Some(value) => {
            out.push(1);
            put_bytes(out, value);
        }
        None => out.push(0),
    }
}

pub(super) fn decode_scalar_value(cursor: &mut Cursor<&[u8]>) -> Result<ScalarValue> {
    let mut tag = [0u8; 1];
    cursor.read_exact(&mut tag)?;
    let mut present = [0u8; 1];
    cursor.read_exact(&mut present)?;
    let present = present[0] != 0;
    Ok(match tag[0] {
        1 => ScalarValue::Bool(if present {
            Some(read_bool(cursor)?)
        } else {
            None
        }),
        2 => ScalarValue::Int8(if present {
            Some(read_i64(cursor)? as i8)
        } else {
            None
        }),
        3 => ScalarValue::Int16(if present {
            Some(read_i64(cursor)? as i16)
        } else {
            None
        }),
        4 => ScalarValue::Int32(if present {
            Some(read_i64(cursor)? as i32)
        } else {
            None
        }),
        5 => ScalarValue::Int64(if present {
            Some(read_i64(cursor)?)
        } else {
            None
        }),
        6 => ScalarValue::UInt8(if present {
            Some(read_u64(cursor)? as u8)
        } else {
            None
        }),
        7 => ScalarValue::UInt16(if present {
            Some(read_u64(cursor)? as u16)
        } else {
            None
        }),
        8 => ScalarValue::UInt32(if present {
            Some(read_u64(cursor)? as u32)
        } else {
            None
        }),
        9 => ScalarValue::UInt64(if present {
            Some(read_u64(cursor)?)
        } else {
            None
        }),
        10 => ScalarValue::Float32(if present {
            Some(f32::from_bits(read_u64(cursor)? as u32))
        } else {
            None
        }),
        11 => ScalarValue::Float64(if present {
            Some(f64::from_bits(read_u64(cursor)?))
        } else {
            None
        }),
        12 => ScalarValue::Decimal(if present {
            Some(read_i64(cursor)?)
        } else {
            None
        }),
        13 => ScalarValue::Date(if present {
            Some(read_i64(cursor)? as i32)
        } else {
            None
        }),
        14 => ScalarValue::Time(if present {
            Some(read_i64(cursor)?)
        } else {
            None
        }),
        15 => ScalarValue::Timestamp(if present {
            Some(read_i64(cursor)?)
        } else {
            None
        }),
        16 => ScalarValue::Interval(if present {
            Some(read_i64(cursor)?)
        } else {
            None
        }),
        17 => ScalarValue::VarChar(if present {
            Some(read_string(cursor)?)
        } else {
            None
        }),
        18 => ScalarValue::Blob(if present {
            Some(read_bytes(cursor)?)
        } else {
            None
        }),
        19 => {
            if present {
                let len = read_u32(cursor)? as usize;
                let mut values = Vec::with_capacity(len);
                for _ in 0..len {
                    let mut bytes = [0u8; 4];
                    cursor.read_exact(&mut bytes)?;
                    values.push(f32::from_bits(u32::from_le_bytes(bytes)));
                }
                ScalarValue::Vector(Some(values))
            } else {
                ScalarValue::Vector(None)
            }
        }
        tag => return Err(TaibaiError::Storage(format!("unknown scalar tag {}", tag))),
    })
}

pub(super) fn read_bool(cursor: &mut Cursor<&[u8]>) -> Result<bool> {
    let mut value = [0u8; 1];
    cursor.read_exact(&mut value)?;
    Ok(value[0] != 0)
}
