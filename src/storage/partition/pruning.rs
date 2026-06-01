#![allow(dead_code)]
use std::io::Cursor;

use crate::storage::codec::scalar_tag::decode_scalar_with_tag;
use crate::types::scalar::ScalarValue;

#[derive(Debug, Clone)]
pub struct ZoneMap {
    pub min_value: ScalarValue,
    pub max_value: ScalarValue,
    pub null_count: u64,
    pub row_count: u64,
}

impl ZoneMap {
    pub fn null() -> Self {
        Self {
            min_value: ScalarValue::Int64(None),
            max_value: ScalarValue::Int64(None),
            null_count: 0,
            row_count: 0,
        }
    }

    pub fn contains(&self, value: &ScalarValue) -> bool {
        if self.min_value.is_null() || value.is_null() {
            return true;
        }
        crate::storage::index::sort_key::compare_scalars_for_sort(value, &self.min_value)
            >= std::cmp::Ordering::Equal
            && crate::storage::index::sort_key::compare_scalars_for_sort(value, &self.max_value)
                <= std::cmp::Ordering::Equal
    }

    pub fn update(&mut self, value: &ScalarValue) {
        if value.is_null() {
            self.null_count += 1;
            return;
        }
        if self.min_value.is_null() {
            self.min_value = value.clone();
            self.max_value = value.clone();
        } else {
            if crate::storage::index::sort_key::scalar_lt(value, &self.min_value) {
                self.min_value = value.clone();
            }
            if crate::storage::index::sort_key::scalar_lt(&self.max_value, value) {
                self.max_value = value.clone();
            }
        }
        self.row_count += 1;
    }

    pub fn overlaps(&self, min: &ScalarValue, max: &ScalarValue) -> bool {
        if self.min_value.is_null() || self.max_value.is_null() {
            return true;
        }
        if min.is_null() || max.is_null() {
            return true;
        }
        !crate::storage::index::sort_key::scalar_lt(max, &self.min_value)
            && !crate::storage::index::sort_key::scalar_lt(&self.max_value, min)
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        crate::storage::codec::scalar_tag::encode_scalar_with_tag(&mut buf, &self.min_value);
        crate::storage::codec::scalar_tag::encode_scalar_with_tag(&mut buf, &self.max_value);
        buf.extend_from_slice(&self.null_count.to_le_bytes());
        buf.extend_from_slice(&self.row_count.to_le_bytes());
        buf
    }

    pub fn deserialize(data: &[u8]) -> Option<(Self, usize)> {
        let mut cursor = Cursor::new(data);
        let min_value = decode_scalar_with_tag(&mut cursor).ok()?;
        let max_value = decode_scalar_with_tag(&mut cursor).ok()?;

        let pos = cursor.position() as usize;
        if data.len() < pos + 16 {
            return None;
        }

        let null_count = u64::from_le_bytes(data[pos..pos + 8].try_into().ok()?);
        let row_count = u64::from_le_bytes(data[pos + 8..pos + 16].try_into().ok()?);

        Some((
            Self {
                min_value,
                max_value,
                null_count,
                row_count,
            },
            pos + 16,
        ))
    }
}

impl Default for ZoneMap {
    fn default() -> Self {
        Self::null()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeOp {
    Eq,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Debug, Clone)]
pub struct RangeClause {
    pub column_index: usize,
    pub op: RangeOp,
    pub value: ScalarValue,
}

#[derive(Debug, Default, Clone)]
pub struct ScanStats {
    pub segments_opened: u64,
    pub segments_pruned_by_cluster_key: u64,
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
        self.segments_pruned_by_cluster_key += other.segments_pruned_by_cluster_key;
        self.row_groups_total += other.row_groups_total;
        self.row_groups_read += other.row_groups_read;
        self.row_groups_pruned += other.row_groups_pruned;
        self.rows_scanned += other.rows_scanned;
        self.rows_filtered_by_delete_bitmap += other.rows_filtered_by_delete_bitmap;
        self.bytes_read_compressed += other.bytes_read_compressed;
    }
}

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

    pub fn matches_row_group(&self, zone_maps: &[ZoneMap]) -> bool {
        for c in &self.clauses {
            let Some(zm) = zone_maps.get(c.column_index) else {
                continue;
            };
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
        RangeOp::Eq => {
            !crate::storage::index::sort_key::scalar_lt(&c.value, min_v)
                && !crate::storage::index::sort_key::scalar_lt(max_v, &c.value)
        }
        RangeOp::Lt => crate::storage::index::sort_key::scalar_lt(min_v, &c.value),
        RangeOp::Le => !crate::storage::index::sort_key::scalar_lt(&c.value, min_v),
        RangeOp::Gt => crate::storage::index::sort_key::scalar_lt(&c.value, max_v),
        RangeOp::Ge => !crate::storage::index::sort_key::scalar_lt(max_v, &c.value),
    }
}

pub fn compute_segment_zone_maps_from_row_groups(
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
            min_value: seg_min.unwrap_or_else(|| ScalarValue::Int64(None)),
            max_value: seg_max.unwrap_or_else(|| ScalarValue::Int64(None)),
            null_count: total_null_count,
            row_count: total_row_count,
        });
    }

    segment_maps
}
