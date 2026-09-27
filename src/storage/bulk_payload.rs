use crate::common::error::{Result, TaibaiError};
use crate::protocol::types::DataType;

pub struct DirectBulkRequest<'a> {
    pub table_name: &'a str,
    pub row_count: usize,
    pub columns: Vec<DirectBulkColumn<'a>>,
}

pub struct DirectBulkColumn<'a> {
    pub name: &'a str,
    pub data_type: DataType,
    pub scale: i32,
    pub precision: i32,
    pub data: &'a [u8],
    pub nulls: &'a [u8],
    pub variable_ranges: Option<Vec<(usize, usize)>>,
}

impl DirectBulkColumn<'_> {
    #[inline]
    pub fn is_null(&self, row_idx: usize) -> bool {
        null_bit_is_set(self.nulls, row_idx)
    }

    pub fn has_nulls(&self, row_count: usize) -> bool {
        (0..row_count).any(|row_idx| self.is_null(row_idx))
    }
}

pub fn parse_direct_bulk_payload(payload: &[u8]) -> Result<DirectBulkRequest<'_>> {
    if payload.len() < 10 {
        return Err(TaibaiError::Protocol("bulk payload too short".into()));
    }

    let mut pos = 0usize;
    let table_len = read_u16_at(payload, &mut pos)? as usize;
    let table_name = read_utf8_at(payload, &mut pos, table_len)?;
    let row_count = read_u32_at(payload, &mut pos)? as usize;
    let column_count = read_u32_at(payload, &mut pos)? as usize;

    let mut columns = Vec::with_capacity(column_count);
    for col_idx in 0..column_count {
        let name_len = read_u16_at(payload, &mut pos)? as usize;
        let name = read_utf8_at(payload, &mut pos, name_len)?;
        let dtype = DataType::from_u8(read_u8_at(payload, &mut pos)?)?;
        let scale = read_u8_at(payload, &mut pos)? as i32;
        let precision = read_u8_at(payload, &mut pos)? as i32;
        let elem_count = read_u32_at(payload, &mut pos)? as usize;
        let nulls_size = read_u32_at(payload, &mut pos)? as usize;

        if elem_count != row_count {
            return Err(TaibaiError::Protocol(format!(
                "column {} row count mismatch: header={} column={}",
                col_idx, row_count, elem_count
            )));
        }

        let (data_len, variable_ranges) = direct_column_layout(dtype, elem_count, &payload[pos..])?;
        if pos + data_len > payload.len() {
            return Err(TaibaiError::Protocol(format!(
                "column {} data exceeds payload",
                col_idx
            )));
        }
        let data = &payload[pos..pos + data_len];
        pos += data_len;

        if pos + nulls_size > payload.len() {
            return Err(TaibaiError::Protocol(format!(
                "column {} null bitmap exceeds payload",
                col_idx
            )));
        }
        let nulls = &payload[pos..pos + nulls_size];
        pos += nulls_size;

        validate_null_bitmap(col_idx, row_count, nulls)?;

        columns.push(DirectBulkColumn {
            name,
            data_type: dtype,
            scale,
            precision,
            data,
            nulls,
            variable_ranges,
        });
    }

    if pos != payload.len() {
        return Err(TaibaiError::Protocol(format!(
            "trailing bytes after bulk payload: parsed {}, payload size {}",
            pos,
            payload.len()
        )));
    }

    Ok(DirectBulkRequest {
        table_name,
        row_count,
        columns,
    })
}

fn validate_null_bitmap(col_idx: usize, row_count: usize, nulls: &[u8]) -> Result<()> {
    if nulls.is_empty() {
        return Ok(());
    }

    // NullBitmap serializes complete little-endian u64 words.  Requiring the
    // canonical length prevents truncated bitmaps from silently treating rows
    // as valid in the direct path.
    let expected = row_count
        .div_ceil(64)
        .checked_mul(8)
        .ok_or_else(|| TaibaiError::Protocol("bulk null bitmap length overflow".into()))?;
    if nulls.len() != expected {
        return Err(TaibaiError::Protocol(format!(
            "column {} null bitmap size mismatch: got {}, expected {} for {} rows",
            col_idx,
            nulls.len(),
            expected,
            row_count
        )));
    }

    if !row_count.is_multiple_of(64) {
        let used_bits = row_count % 64;
        let last_word = u64::from_le_bytes(nulls[nulls.len() - 8..].try_into().unwrap());
        if last_word >> used_bits != 0 {
            return Err(TaibaiError::Protocol(format!(
                "column {} null bitmap has bits set beyond {} rows",
                col_idx, row_count
            )));
        }
    }
    Ok(())
}

#[inline]
pub(crate) fn null_bit_is_set(nulls: &[u8], row_idx: usize) -> bool {
    nulls
        .get(row_idx / 8)
        .is_some_and(|byte| byte & (1u8 << (row_idx % 8)) != 0)
}

type BulkVariableRanges = Vec<(usize, usize)>;

fn direct_column_layout(
    data_type: DataType,
    elem_count: usize,
    data: &[u8],
) -> Result<(usize, Option<BulkVariableRanges>)> {
    if let Some(width) = data_type.fixed_size() {
        let len = elem_count
            .checked_mul(width)
            .ok_or_else(|| TaibaiError::Protocol("fixed-width column length overflow".into()))?;
        return Ok((len, None));
    }

    let mut offset = 0usize;
    // Bound the initial capacity by what the remaining payload can possibly
    // contain: every element needs at least a 4-byte length prefix. The wire
    // elem_count is attacker-controlled and used to be trusted directly,
    // letting a ~20-byte malformed header request a ~68GB allocation (and
    // with it an allocation-failure abort of the whole process).
    let capacity_bound = (data.len() / 4).saturating_add(1);
    let mut ranges = Vec::with_capacity(elem_count.min(capacity_bound));
    for _ in 0..elem_count {
        if offset + 4 > data.len() {
            return Err(TaibaiError::Protocol(
                "variable column length prefix truncated".into(),
            ));
        }
        let len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
        let value_offset = offset
            .checked_add(4)
            .ok_or_else(|| TaibaiError::Protocol("variable column length overflow".into()))?;
        offset = value_offset
            .checked_add(len)
            .ok_or_else(|| TaibaiError::Protocol("variable column length overflow".into()))?;
        if offset > data.len() {
            return Err(TaibaiError::Protocol(
                "variable column data truncated".into(),
            ));
        }
        ranges.push((value_offset, len));
    }
    Ok((offset, Some(ranges)))
}

fn read_u8_at(payload: &[u8], pos: &mut usize) -> Result<u8> {
    if *pos >= payload.len() {
        return Err(TaibaiError::Protocol("payload truncated reading u8".into()));
    }
    let v = payload[*pos];
    *pos += 1;
    Ok(v)
}

fn read_u16_at(payload: &[u8], pos: &mut usize) -> Result<u16> {
    if *pos + 2 > payload.len() {
        return Err(TaibaiError::Protocol(
            "payload truncated reading u16".into(),
        ));
    }
    let v = u16::from_le_bytes(payload[*pos..*pos + 2].try_into().unwrap());
    *pos += 2;
    Ok(v)
}

fn read_u32_at(payload: &[u8], pos: &mut usize) -> Result<u32> {
    if *pos + 4 > payload.len() {
        return Err(TaibaiError::Protocol(
            "payload truncated reading u32".into(),
        ));
    }
    let v = u32::from_le_bytes(payload[*pos..*pos + 4].try_into().unwrap());
    *pos += 4;
    Ok(v)
}

fn read_utf8_at<'a>(payload: &'a [u8], pos: &mut usize, len: usize) -> Result<&'a str> {
    if *pos + len > payload.len() {
        return Err(TaibaiError::Protocol(
            "payload truncated reading string".into(),
        ));
    }
    let s = std::str::from_utf8(&payload[*pos..*pos + len])
        .map_err(|e| TaibaiError::Protocol(format!("invalid utf8 in bulk payload: {}", e)))?;
    *pos += len;
    Ok(s)
}

#[cfg(test)]
#[path = "bulk_payload_test.rs"]
mod tests;
