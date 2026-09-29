//! Single source of truth for segment footer extension blocks.
//!
//! Every optional block that can appear inside a segment footer is described
//! exactly once in this module: its magic, its decode walk and its
//! validating skip walk. `parse_segment_footer`, `reserved_section_offset`,
//! the segment checker and footer rewrite paths all go through these
//! functions, so adding an extension must not require touching five parse
//! sites again.
//!
//! ## Layout invariants
//!
//! Per row group the blocks appear in a fixed order after the bloom filters:
//! `value sets (STIX) -> column sums (CSUM) -> column CRCs (CCR3)`. At the
//! footer tail the order is `delete bitmap -> tail TLV (TLV1, optional) ->
//! whole-footer CRC (FCR3, always last)`. The TLV block is only written when
//! it has entries; readers treat its absence as "no entries". `FCR3` covers
//! every byte before it, so appending future structure must keep it last.
//!
//! Old binaries reading newer footers fail loudly: the header version gate
//! rejects unknown versions before any footer parsing happens.

use std::io::{Cursor, Read};

use crate::common::error::{Result, TaibaiError};
use crate::storage::codec::io_util::{read_u32_le, read_u8};
use crate::storage::codec::scalar_tag::decode_scalar_with_tag;
use crate::types::scalar::ScalarValue;

pub const VALUE_SET_EXTENSION_MAGIC: u32 = 0x5354_4958; // STIX
pub const COL_SUM_EXTENSION_MAGIC: u32 = 0x4353_554D; // CSUM
pub const COLUMN_CRC_EXTENSION_MAGIC: u32 = 0x4343_5233; // CCR3
pub const FOOTER_CRC_EXTENSION_MAGIC: u32 = 0x4643_5233; // FCR3
/// Self-describing tail block: `[magic][entry_count u32]
/// [{tag u16, len u32, payload}*]*`. Future footer extensions go in here
/// instead of growing the fixed field sequence.
pub const FOOTER_TAIL_TLV_MAGIC: u32 = 0x544C_5631; // TLV1

/// One entry of the footer tail TLV block.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FooterTailEntry {
    pub tag: u16,
    pub payload: Vec<u8>,
}

pub fn peek_u32(buf: &[u8], cursor: &Cursor<&[u8]>) -> Option<u32> {
    let pos = cursor.position() as usize;
    if pos + 4 > buf.len() {
        return None;
    }
    Some(u32::from_le_bytes(buf[pos..pos + 4].try_into().ok()?))
}

// ---------------------------------------------------------------------------
// per-row-group extension blocks
// ---------------------------------------------------------------------------

/// Decode the value-set block when its magic is next; absent magic (segments
/// written before the extension) yields all-`None`.
pub fn read_value_set_extension(
    buf: &[u8],
    cursor: &mut Cursor<&[u8]>,
    col_count: usize,
) -> Result<Vec<Option<Vec<ScalarValue>>>> {
    if peek_u32(buf, cursor) != Some(VALUE_SET_EXTENSION_MAGIC) {
        return Ok(vec![None; col_count]);
    }
    let _magic = read_u32_le(cursor)?;
    let mut value_sets = Vec::with_capacity(col_count);
    for _ in 0..col_count {
        let has_set = read_u8(cursor)?;
        if has_set != 0 {
            let value_count = read_u32_le(cursor)? as usize;
            let mut values = Vec::with_capacity(value_count);
            for _ in 0..value_count {
                values.push(decode_scalar_with_tag(cursor)?);
            }
            value_sets.push(Some(values));
        } else {
            value_sets.push(None);
        }
    }
    Ok(value_sets)
}

/// Decode the column-sum block; absent magic yields an empty vec, which
/// every consumer treats as "sums unavailable". The stored column count must
/// match the schema — a mismatch means the magic bytes were random data, so
/// fail loudly instead of misfolding sums.
pub fn read_col_sum_extension(
    buf: &[u8],
    cursor: &mut Cursor<&[u8]>,
    col_count: usize,
) -> Result<Vec<i64>> {
    if peek_u32(buf, cursor) != Some(COL_SUM_EXTENSION_MAGIC) {
        return Ok(Vec::new());
    }
    let _magic = read_u32_le(cursor)?;
    let stored_count = read_u32_le(cursor)? as usize;
    if stored_count != col_count {
        return Err(TaibaiError::Storage(format!(
            "column-sum extension count mismatch: {} columns stored, {} in schema",
            stored_count, col_count
        )));
    }
    let mut col_sums = Vec::with_capacity(col_count);
    for _ in 0..col_count {
        let mut bytes = [0u8; 8];
        cursor.read_exact(&mut bytes)?;
        col_sums.push(i64::from_le_bytes(bytes));
    }
    Ok(col_sums)
}

/// Decode the column-CRC block; absent magic yields an empty vec and readers
/// then skip block verification.
pub fn read_column_crc_extension(
    buf: &[u8],
    cursor: &mut Cursor<&[u8]>,
    col_count: usize,
) -> Result<Vec<u32>> {
    if peek_u32(buf, cursor) != Some(COLUMN_CRC_EXTENSION_MAGIC) {
        return Ok(Vec::new());
    }
    let _magic = read_u32_le(cursor)?;
    let stored_count = read_u32_le(cursor)? as usize;
    if stored_count != col_count {
        return Err(TaibaiError::Storage(format!(
            "column-crc extension count mismatch: {} columns stored, {} in schema",
            stored_count, col_count
        )));
    }
    let mut crcs = Vec::with_capacity(col_count);
    for _ in 0..col_count {
        let mut bytes = [0u8; 4];
        cursor.read_exact(&mut bytes)?;
        crcs.push(u32::from_le_bytes(bytes));
    }
    Ok(crcs)
}

/// Advance past the three per-row-group blocks without keeping payloads.
/// Validation matches the decode walks exactly, so a footer that passes the
/// skip also passes the full parse (the rewrite path relies on this to find
/// the correct truncation boundary).
pub fn skip_row_group_extensions(
    buf: &[u8],
    cursor: &mut Cursor<&[u8]>,
    col_count: usize,
) -> Result<()> {
    // Value sets must decode to advance (variable-length scalars); drop the
    // values.
    let _ = read_value_set_extension(buf, cursor, col_count)?;
    // Sums and CRCs are fixed-width: skip by position with the same count
    // validation.
    if peek_u32(buf, cursor) == Some(COL_SUM_EXTENSION_MAGIC) {
        let _magic = read_u32_le(cursor)?;
        let stored_count = read_u32_le(cursor)? as usize;
        if stored_count != col_count {
            return Err(TaibaiError::Storage(
                "column-sum extension count mismatch while scanning footer".to_string(),
            ));
        }
        cursor.set_position(cursor.position() + stored_count as u64 * 8);
    }
    if peek_u32(buf, cursor) == Some(COLUMN_CRC_EXTENSION_MAGIC) {
        let _magic = read_u32_le(cursor)?;
        let stored_count = read_u32_le(cursor)? as usize;
        if stored_count != col_count {
            return Err(TaibaiError::Storage(
                "column-crc extension count mismatch while scanning footer".to_string(),
            ));
        }
        cursor.set_position(cursor.position() + stored_count as u64 * 4);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// footer tail: TLV block + whole-footer checksum
// ---------------------------------------------------------------------------

/// Decode the tail TLV block when its magic is next; absent magic (v1
/// footers, or v2 footers with no entries) yields an empty vec.
pub fn read_footer_tail(buf: &[u8], cursor: &mut Cursor<&[u8]>) -> Result<Vec<FooterTailEntry>> {
    if peek_u32(buf, cursor) != Some(FOOTER_TAIL_TLV_MAGIC) {
        return Ok(Vec::new());
    }
    let _magic = read_u32_le(cursor)?;
    let entry_count = read_u32_le(cursor)? as usize;
    let mut entries = Vec::with_capacity(entry_count.min(1024));
    for _ in 0..entry_count {
        let mut tag_bytes = [0u8; 2];
        cursor.read_exact(&mut tag_bytes)?;
        let tag = u16::from_le_bytes(tag_bytes);
        let len = read_u32_le(cursor)? as usize;
        if cursor.position() as usize + len > buf.len() {
            return Err(TaibaiError::Storage(format!(
                "footer tail entry {} length {} exceeds footer size {}",
                tag,
                len,
                buf.len()
            )));
        }
        let start = cursor.position() as usize;
        entries.push(FooterTailEntry {
            tag,
            payload: buf[start..start + len].to_vec(),
        });
        cursor.set_position((start + len) as u64);
    }
    Ok(entries)
}

/// Skip the tail TLV block without materializing payloads.
pub fn skip_footer_tail(buf: &[u8], cursor: &mut Cursor<&[u8]>) -> Result<()> {
    let _ = read_footer_tail(buf, cursor)?;
    Ok(())
}

/// Encode the tail TLV block. Empty input encodes to nothing (the block is
/// only written when it has entries, keeping plain v2 footers byte-shape
/// identical to v1).
pub fn encode_footer_tail(entries: &[FooterTailEntry]) -> Vec<u8> {
    if entries.is_empty() {
        return Vec::new();
    }
    let mut buf = Vec::new();
    buf.extend_from_slice(&FOOTER_TAIL_TLV_MAGIC.to_le_bytes());
    buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for entry in entries {
        buf.extend_from_slice(&entry.tag.to_le_bytes());
        buf.extend_from_slice(&(entry.payload.len() as u32).to_le_bytes());
        buf.extend_from_slice(&entry.payload);
    }
    buf
}

/// Append the whole-footer checksum: CRC over everything already in `buf`,
/// then the FCR3 magic. Must stay the last thing written to a footer.
pub fn append_footer_crc(buf: &mut Vec<u8>) {
    let crc = crc32fast::hash(buf);
    buf.extend_from_slice(&FOOTER_CRC_EXTENSION_MAGIC.to_le_bytes());
    buf.extend_from_slice(&crc.to_le_bytes());
}

/// Verify the whole-footer checksum if the FCR3 block is present. `consumed`
/// is the cursor position after the checksum block; the CRC covers
/// `buf[..consumed - 8]`.
pub fn verify_footer_crc(buf: &[u8], cursor: &mut Cursor<&[u8]>) -> Result<()> {
    if peek_u32(buf, cursor) != Some(FOOTER_CRC_EXTENSION_MAGIC) {
        return Ok(());
    }
    let _magic = read_u32_le(cursor)?;
    let mut crc_bytes = [0u8; 4];
    cursor.read_exact(&mut crc_bytes)?;
    let stored = u32::from_le_bytes(crc_bytes);
    let computed = crc32fast::hash(&buf[..cursor.position() as usize - 8]);
    if stored != computed {
        return Err(TaibaiError::Storage(
            "segment footer checksum mismatch".to_string(),
        ));
    }
    Ok(())
}
