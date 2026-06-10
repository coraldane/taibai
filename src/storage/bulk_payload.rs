use crate::common::error::{TaibaiError, Result};
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

fn direct_column_layout(
    data_type: DataType,
    elem_count: usize,
    data: &[u8],
) -> Result<(usize, Option<Vec<(usize, usize)>>)> {
    if let Some(width) = data_type.fixed_size() {
        let len = elem_count
            .checked_mul(width)
            .ok_or_else(|| TaibaiError::Protocol("fixed-width column length overflow".into()))?;
        return Ok((len, None));
    }

    let mut offset = 0usize;
    let mut ranges = Vec::with_capacity(elem_count);
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
        return Err(TaibaiError::Protocol("payload truncated reading u16".into()));
    }
    let v = u16::from_le_bytes(payload[*pos..*pos + 2].try_into().unwrap());
    *pos += 2;
    Ok(v)
}

fn read_u32_at(payload: &[u8], pos: &mut usize) -> Result<u32> {
    if *pos + 4 > payload.len() {
        return Err(TaibaiError::Protocol("payload truncated reading u32".into()));
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
mod tests {
    use super::*;

    #[test]
    fn parses_u8_decimal_meta_payload() {
        let mut payload = Vec::new();
        write_header(&mut payload, "ticks", 2, 1);
        write_column_header(&mut payload, "price", DataType::Decimal, 2, 9, 2, 0);
        payload.extend_from_slice(&1234i64.to_le_bytes());
        payload.extend_from_slice(&5678i64.to_le_bytes());

        let req = parse_direct_bulk_payload(&payload).unwrap();

        assert_eq!(req.table_name, "ticks");
        assert_eq!(req.row_count, 2);
        assert_eq!(req.columns.len(), 1);
        assert_eq!(req.columns[0].scale, 2);
        assert_eq!(req.columns[0].precision, 9);
        assert_eq!(req.columns[0].data.len(), 16);
    }

    #[test]
    fn rejects_trailing_bytes() {
        let mut payload = Vec::new();
        write_header(&mut payload, "ticks", 0, 0);
        payload.push(0);

        assert!(parse_direct_bulk_payload(&payload).is_err());
    }

    fn write_header(buf: &mut Vec<u8>, table: &str, rows: u32, cols: u32) {
        buf.extend_from_slice(&(table.len() as u16).to_le_bytes());
        buf.extend_from_slice(table.as_bytes());
        buf.extend_from_slice(&rows.to_le_bytes());
        buf.extend_from_slice(&cols.to_le_bytes());
    }

    fn write_column_header(
        buf: &mut Vec<u8>,
        name: &str,
        data_type: DataType,
        scale: u8,
        precision: u8,
        elem_count: u32,
        nulls_size: u32,
    ) {
        buf.extend_from_slice(&(name.len() as u16).to_le_bytes());
        buf.extend_from_slice(name.as_bytes());
        buf.push(data_type as u8);
        buf.push(scale);
        buf.push(precision);
        buf.extend_from_slice(&elem_count.to_le_bytes());
        buf.extend_from_slice(&nulls_size.to_le_bytes());
    }
}
