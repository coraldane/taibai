//! Advisory per-segment plan metadata and the persistent segment catalog.
//!
//! The catalog is a maintenance accelerator, never a source of truth: the
//! segment footer and the retirement journal remain authoritative. Every
//! catalog entry can be discarded and rebuilt from the footer, and every
//! planned candidate is re-verified (stat + maintenance revision) before any
//! commit, so a stale or missing catalog can only delay maintenance work —
//! it can never hide rows or resurrect a retired file.

use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use crc32fast::Hasher;

use crate::common::error::{Result, TaibaiError};
use crate::storage::segment::meta::{atomic_replace, sync_parent_dir, SegmentMetaSnapshot};
use crate::storage::segment::RowGroupMeta;
use crate::types::scalar::ScalarValue;

pub(super) const CATALOG_FILE_NAME: &str = "segment_catalog";
const CATALOG_MAGIC: u32 = 0x4353_4254; // "TBSC"
const CATALOG_VERSION: u32 = 1;

/// Planning-only metadata for an immutable segment, extracted once from its
/// footer. Deliberately excludes the schema, per-row-group bloom filters and
/// value sets, and the delete bitmap: planning needs row counts, row-group
/// sizes, and (for TTL tables) the TTL column's per-row-group zone stats.
/// The rewrite itself reads authoritative data through segment readers.
#[derive(Debug, Clone)]
pub struct SegmentPlanMeta {
    /// Segment file length when the plan was captured. An in-place footer
    /// rewrite (delete compaction) changes the length, so a mismatch against
    /// a fresh stat invalidates the plan.
    pub file_len: u64,
    pub row_count: u64,
    pub deleted_row_count: u64,
    pub row_group_rows: Vec<u64>,
    pub ttl: Option<PlanTtlStats>,
}

/// Per-row-group TTL zone statistics for the table's TTL column, resolved
/// against the segment's own footer schema at capture time.
#[derive(Debug, Clone)]
pub struct PlanTtlStats {
    /// Column the stats describe (the table's TTL column name at capture).
    pub column: String,
    /// One entry per footer row group, same order as `row_group_rows`.
    pub zones: Vec<PlanTtlZone>,
}

#[derive(Debug, Clone, Copy)]
pub struct PlanTtlZone {
    pub row_count: u64,
    pub null_count: u64,
    /// `zone.max_value` when it is a timestamp; `None` otherwise (missing
    /// zone, NULL max) so planning stays conservative.
    pub max_timestamp_us: Option<i64>,
}

/// The TTL column a capture should extract stats for: the current name plus
/// the names the column carried before renames, because sealed segments keep
/// their original footer layout.
#[derive(Clone, Debug)]
pub struct MaintenanceTtlColumn {
    pub name: String,
    pub previous_names: Vec<String>,
}

impl MaintenanceTtlColumn {
    fn names(&self) -> impl Iterator<Item = &str> {
        std::iter::once(self.name.as_str()).chain(self.previous_names.iter().map(String::as_str))
    }
}

/// Extract plan metadata from a parsed footer snapshot.
pub fn build_plan_meta(
    meta: &SegmentMetaSnapshot,
    file_len: u64,
    ttl_column: Option<&MaintenanceTtlColumn>,
) -> SegmentPlanMeta {
    let row_group_count = meta.row_group_count();
    // Row counts come straight from the resident index — no decode at all.
    let row_group_rows = (0..row_group_count)
        .map(|row_group| meta.row_group_row_count(row_group))
        .collect();
    let ttl = ttl_column.and_then(|ttl| {
        let index = ttl
            .names()
            .find_map(|name| meta.footer_schema().index_of(name))?;
        // TTL zones need one column's bounds per row group, so the records
        // are decoded one at a time into a reused buffer rather than the
        // whole footer being materialised.
        let mut zones = Vec::with_capacity(row_group_count);
        let mut scratch = RowGroupMeta::empty();
        for row_group in 0..row_group_count {
            let decoded = meta.row_group_meta_into(row_group, &mut scratch).is_ok();
            let zone = if decoded {
                scratch.zone_maps.get(index)
            } else {
                None
            };
            zones.push(match zone {
                Some(zone) => PlanTtlZone {
                    row_count: zone.row_count,
                    null_count: zone.null_count,
                    max_timestamp_us: match &zone.max_value {
                        ScalarValue::Timestamp(Some(ts)) => Some(*ts),
                        _ => None,
                    },
                },
                None => PlanTtlZone {
                    row_count: 0,
                    null_count: 0,
                    max_timestamp_us: None,
                },
            });
        }
        Some(PlanTtlStats {
            column: ttl.name.clone(),
            zones,
        })
    });
    SegmentPlanMeta {
        file_len,
        row_count: meta.row_count,
        deleted_row_count: meta.deleted_row_count,
        row_group_rows,
        ttl,
    }
}

/// TTL full-expiry check over plan stats. Mirrors the footer-based
/// `segment_ttl_fully_expired`: every row group must have a populated,
/// NULL-free zone whose max timestamp is past the horizon. Missing stats,
/// a column mismatch, or any unprovable group keeps the segment.
pub fn plan_ttl_fully_expired(
    plan: &SegmentPlanMeta,
    ttl_column: &str,
    duration_us: i64,
    now_us: i64,
) -> bool {
    let Some(ttl) = plan.ttl.as_ref() else {
        return false;
    };
    if ttl.column != ttl_column {
        return false;
    }
    ttl.zones.iter().all(|zone| {
        zone.row_count > 0
            && zone.null_count == 0
            && matches!(zone.max_timestamp_us, Some(ts) if ts.saturating_add(duration_us) <= now_us)
    })
}

/// One persisted catalog record: a segment file name plus its plan metadata.
#[derive(Debug, Clone)]
pub(super) struct CatalogEntryRecord {
    pub name: String,
    pub file_len: u64,
    pub row_count: u64,
    pub deleted_row_count: u64,
    pub row_group_rows: Vec<u64>,
    pub ttl: Option<PlanTtlStats>,
}

impl CatalogEntryRecord {
    pub(super) fn from_plan(name: String, plan: &SegmentPlanMeta) -> Self {
        Self {
            name,
            file_len: plan.file_len,
            row_count: plan.row_count,
            deleted_row_count: plan.deleted_row_count,
            row_group_rows: plan.row_group_rows.clone(),
            ttl: plan.ttl.clone(),
        }
    }

    pub(super) fn into_plan(self) -> Arc<SegmentPlanMeta> {
        Arc::new(SegmentPlanMeta {
            file_len: self.file_len,
            row_count: self.row_count,
            deleted_row_count: self.deleted_row_count,
            row_group_rows: self.row_group_rows,
            ttl: self.ttl,
        })
    }
}

/// Load the table's catalog file. `Ok(None)` means the file does not exist;
/// an existing but unreadable file is an error the caller treats as "start
/// empty and reconcile".
pub(super) fn load_catalog_file(table_dir: &Path) -> Result<Option<Vec<CatalogEntryRecord>>> {
    let path = table_dir.join(CATALOG_FILE_NAME);
    let bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(TaibaiError::Io(error)),
    };
    decode_catalog(&bytes).map(Some)
}

/// Atomically replace the catalog file. Best-effort callers warn on error and
/// stay dirty; the next maintenance round retries.
pub(super) fn store_catalog_file(table_dir: &Path, entries: &[CatalogEntryRecord]) -> Result<()> {
    let bytes = encode_catalog(entries);
    let tmp_path = table_dir.join(format!("{CATALOG_FILE_NAME}.tmp"));
    {
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
    }
    atomic_replace(&tmp_path, &table_dir.join(CATALOG_FILE_NAME))?;
    sync_parent_dir(&table_dir.join(CATALOG_FILE_NAME))
}

fn encode_catalog(entries: &[CatalogEntryRecord]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&CATALOG_MAGIC.to_le_bytes());
    buf.extend_from_slice(&CATALOG_VERSION.to_le_bytes());
    buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for entry in entries {
        encode_str(&mut buf, &entry.name);
        buf.extend_from_slice(&entry.file_len.to_le_bytes());
        buf.extend_from_slice(&entry.row_count.to_le_bytes());
        buf.extend_from_slice(&entry.deleted_row_count.to_le_bytes());
        buf.extend_from_slice(&(entry.row_group_rows.len() as u32).to_le_bytes());
        for &rows in &entry.row_group_rows {
            buf.extend_from_slice(&rows.to_le_bytes());
        }
        match &entry.ttl {
            None => buf.push(0),
            Some(ttl) => {
                buf.push(1);
                encode_str(&mut buf, &ttl.column);
                buf.extend_from_slice(&(ttl.zones.len() as u32).to_le_bytes());
                for zone in &ttl.zones {
                    buf.extend_from_slice(&zone.row_count.to_le_bytes());
                    buf.extend_from_slice(&zone.null_count.to_le_bytes());
                    match zone.max_timestamp_us {
                        Some(ts) => {
                            buf.push(1);
                            buf.extend_from_slice(&ts.to_le_bytes());
                        }
                        None => buf.push(0),
                    }
                }
            }
        }
    }
    let crc = crc32(&buf);
    buf.extend_from_slice(&crc.to_le_bytes());
    buf
}

fn decode_catalog(bytes: &[u8]) -> Result<Vec<CatalogEntryRecord>> {
    if bytes.len() < 16 {
        return Err(catalog_corrupt("truncated header"));
    }
    let (body, crc_bytes) = bytes.split_at(bytes.len() - 4);
    let stored_crc = u32::from_le_bytes(crc_bytes.try_into().unwrap());
    if stored_crc != crc32(body) {
        return Err(catalog_corrupt("checksum mismatch"));
    }
    let magic = u32::from_le_bytes(body[0..4].try_into().unwrap());
    let version = u32::from_le_bytes(body[4..8].try_into().unwrap());
    if magic != CATALOG_MAGIC {
        return Err(catalog_corrupt("bad magic"));
    }
    if version != CATALOG_VERSION {
        return Err(catalog_corrupt("unsupported version"));
    }
    let count = u32::from_le_bytes(body[8..12].try_into().unwrap()) as usize;
    let mut cursor = 12usize;
    let mut entries = Vec::with_capacity(count.min(1 << 20));
    for _ in 0..count {
        let name = decode_str(body, &mut cursor)?;
        let file_len = decode_u64(body, &mut cursor)?;
        let row_count = decode_u64(body, &mut cursor)?;
        let deleted_row_count = decode_u64(body, &mut cursor)?;
        let rg_count = decode_u32(body, &mut cursor)? as usize;
        if rg_count > body.len() {
            return Err(catalog_corrupt("row group count out of range"));
        }
        let mut row_group_rows = Vec::with_capacity(rg_count);
        for _ in 0..rg_count {
            row_group_rows.push(decode_u64(body, &mut cursor)?);
        }
        let ttl_present = decode_u8(body, &mut cursor)?;
        let ttl = if ttl_present == 0 {
            None
        } else {
            let column = decode_str(body, &mut cursor)?;
            let zone_count = decode_u32(body, &mut cursor)? as usize;
            if zone_count > body.len() {
                return Err(catalog_corrupt("ttl zone count out of range"));
            }
            let mut zones = Vec::with_capacity(zone_count);
            for _ in 0..zone_count {
                let row_count = decode_u64(body, &mut cursor)?;
                let null_count = decode_u64(body, &mut cursor)?;
                let max_timestamp_us = if decode_u8(body, &mut cursor)? == 0 {
                    None
                } else {
                    Some(decode_i64(body, &mut cursor)?)
                };
                zones.push(PlanTtlZone {
                    row_count,
                    null_count,
                    max_timestamp_us,
                });
            }
            Some(PlanTtlStats { column, zones })
        };
        entries.push(CatalogEntryRecord {
            name,
            file_len,
            row_count,
            deleted_row_count,
            row_group_rows,
            ttl,
        });
    }
    Ok(entries)
}

fn encode_str(buf: &mut Vec<u8>, value: &str) {
    let bytes = value.as_bytes();
    buf.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
    buf.extend_from_slice(bytes);
}

fn decode_str(body: &[u8], cursor: &mut usize) -> Result<String> {
    let len = decode_u16(body, cursor)? as usize;
    let end = cursor
        .checked_add(len)
        .filter(|end| *end <= body.len())
        .ok_or_else(|| catalog_corrupt("truncated string"))?;
    let value = std::str::from_utf8(&body[*cursor..end])
        .map_err(|_| catalog_corrupt("invalid string"))?
        .to_string();
    *cursor = end;
    Ok(value)
}

fn decode_u8(body: &[u8], cursor: &mut usize) -> Result<u8> {
    take(body, cursor, 1).map(|slice| slice[0])
}

fn decode_u16(body: &[u8], cursor: &mut usize) -> Result<u16> {
    take(body, cursor, 2).map(|slice| u16::from_le_bytes(slice.try_into().unwrap()))
}

fn decode_u32(body: &[u8], cursor: &mut usize) -> Result<u32> {
    take(body, cursor, 4).map(|slice| u32::from_le_bytes(slice.try_into().unwrap()))
}

fn decode_u64(body: &[u8], cursor: &mut usize) -> Result<u64> {
    take(body, cursor, 8).map(|slice| u64::from_le_bytes(slice.try_into().unwrap()))
}

fn decode_i64(body: &[u8], cursor: &mut usize) -> Result<i64> {
    take(body, cursor, 8).map(|slice| i64::from_le_bytes(slice.try_into().unwrap()))
}

fn take<'a>(body: &'a [u8], cursor: &mut usize, len: usize) -> Result<&'a [u8]> {
    let end = cursor
        .checked_add(len)
        .filter(|end| *end <= body.len())
        .ok_or_else(|| catalog_corrupt("truncated record"))?;
    let slice = &body[*cursor..end];
    *cursor = end;
    Ok(slice)
}

fn crc32(data: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(data);
    hasher.finalize()
}

fn catalog_corrupt(reason: &str) -> TaibaiError {
    TaibaiError::Storage(format!("segment catalog unreadable: {reason}"))
}
