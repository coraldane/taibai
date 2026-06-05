use crate::common::error::{MonaxError, Result};
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

pub fn parse_direct_bulk_payload(payload: &[u8]) -> Result<DirectBulkRequest<'_>> {
    if payload.len() < 10 {
        return Err(MonaxError::Protocol("bulk payload too short".into()));
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
        let scale = read_u32_at(payload, &mut pos)? as i32;
        let precision = read_u32_at(payload, &mut pos)? as i32;
        let elem_count = read_u32_at(payload, &mut pos)? as usize;
        let nulls_size = read_u32_at(payload, &mut pos)? as usize;

        if elem_count != row_count {
            return Err(MonaxError::Protocol(format!(
                "column {} row count mismatch: header={} column={}",
                col_idx, row_count, elem_count
            )));
        }

        let (data_len, variable_ranges) = direct_column_layout(dtype, elem_count, &payload[pos..])?;
        if pos + data_len > payload.len() {
            return Err(MonaxError::Protocol(format!(
                "column {} data exceeds payload",
                col_idx
            )));
        }
        let data = &payload[pos..pos + data_len];
        pos += data_len;

        if pos + nulls_size > payload.len() {
            return Err(MonaxError::Protocol(format!(
                "column {} null bitmap exceeds payload",
                col_idx
            )));
        }
        let nulls = &payload[pos..pos + nulls_size];
        pos += nulls_size;

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

    Ok(DirectBulkRequest {
        table_name,
        row_count,
        columns,
    })
}

fn direct_column_layout(
    data_type: DataType,
    elem_count: usize,
    data: &[u8],
) -> Result<(usize, Option<Vec<(usize, usize)>>)> {
    if let Some(width) = data_type.fixed_size() {
        let len = elem_count
            .checked_mul(width)
            .ok_or_else(|| MonaxError::Protocol("fixed-width column length overflow".into()))?;
        return Ok((len, None));
    }

    let mut offset = 0usize;
    let mut ranges = Vec::with_capacity(elem_count);
    for _ in 0..elem_count {
        if offset + 4 > data.len() {
            return Err(MonaxError::Protocol(
                "variable column length prefix truncated".into(),
            ));
        }
        let len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
        let value_offset = offset
            .checked_add(4)
            .ok_or_else(|| MonaxError::Protocol("variable column length overflow".into()))?;
        offset = value_offset
            .checked_add(len)
            .ok_or_else(|| MonaxError::Protocol("variable column length overflow".into()))?;
        if offset > data.len() {
            return Err(MonaxError::Protocol(
                "variable column data truncated".into(),
            ));
        }
        ranges.push((value_offset, len));
    }
    Ok((offset, Some(ranges)))
}

fn read_u8_at(payload: &[u8], pos: &mut usize) -> Result<u8> {
    if *pos >= payload.len() {
        return Err(MonaxError::Protocol("payload truncated reading u8".into()));
    }
    let v = payload[*pos];
    *pos += 1;
    Ok(v)
}

fn read_u16_at(payload: &[u8], pos: &mut usize) -> Result<u16> {
    if *pos + 2 > payload.len() {
        return Err(MonaxError::Protocol("payload truncated reading u16".into()));
    }
    let v = u16::from_le_bytes(payload[*pos..*pos + 2].try_into().unwrap());
    *pos += 2;
    Ok(v)
}

fn read_u32_at(payload: &[u8], pos: &mut usize) -> Result<u32> {
    if *pos + 4 > payload.len() {
        return Err(MonaxError::Protocol("payload truncated reading u32".into()));
    }
    let v = u32::from_le_bytes(payload[*pos..*pos + 4].try_into().unwrap());
    *pos += 4;
    Ok(v)
}

fn read_utf8_at<'a>(payload: &'a [u8], pos: &mut usize, len: usize) -> Result<&'a str> {
    if *pos + len > payload.len() {
        return Err(MonaxError::Protocol(
            "payload truncated reading string".into(),
        ));
    }
    let s = std::str::from_utf8(&payload[*pos..*pos + len])
        .map_err(|e| MonaxError::Protocol(format!("invalid utf8 in bulk payload: {}", e)))?;
    *pos += len;
    Ok(s)
}
