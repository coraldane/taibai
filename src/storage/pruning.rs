use std::io::Cursor;

use crate::storage::encoding::decode_scalar_with_tag;
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
        crate::storage::sort_key::compare_scalars_for_sort(value, &self.min_value) >= std::cmp::Ordering::Equal
            && crate::storage::sort_key::compare_scalars_for_sort(value, &self.max_value) <= std::cmp::Ordering::Equal
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
            if crate::storage::sort_key::scalar_lt(value, &self.min_value) {
                self.min_value = value.clone();
            }
            if crate::storage::sort_key::scalar_lt(&self.max_value, value) {
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
        !crate::storage::sort_key::scalar_lt(max, &self.min_value)
            && !crate::storage::sort_key::scalar_lt(&self.max_value, min)
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        crate::storage::encoding::encode_scalar_with_tag(&mut buf, &self.min_value);
        crate::storage::encoding::encode_scalar_with_tag(&mut buf, &self.max_value);
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
            !crate::storage::sort_key::scalar_lt(&c.value, min_v)
                && !crate::storage::sort_key::scalar_lt(max_v, &c.value)
        }
        RangeOp::Lt => crate::storage::sort_key::scalar_lt(min_v, &c.value),
        RangeOp::Le => !crate::storage::sort_key::scalar_lt(&c.value, min_v),
        RangeOp::Gt => crate::storage::sort_key::scalar_lt(&c.value, max_v),
        RangeOp::Ge => !crate::storage::sort_key::scalar_lt(max_v, &c.value),
    }
}
