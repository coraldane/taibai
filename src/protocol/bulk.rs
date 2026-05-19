use std::io::Cursor;

use crate::common::error::{MonaxError, Result};
use crate::protocol::types::DataType;
use crate::types::array::ColumnArray;
use crate::types::batch::{ColumnDef, RecordBatch, Schema};
use crate::types::bitmap::NullBitmap;

/// Parsed columnar bulk insert request.
pub struct BulkInsertRequest {
    pub table_name: String,
    pub batch: RecordBatch,
}

/// Decode a columnar bulk insert payload (message type 0x23).
///
/// Wire format:
///   u16 table_name_len + table_name
///   u32 row_count
///   u32 column_count
///   per column:
///     u16 name_len + name
///     u8  type (DataType)
///     u32 scale
///     u32 precision
///     u32 data_len (number of elements)
///     u32 nulls_size (bytes of null bitmap)
///     data values (data_len * element_size)
///     null bitmap (nulls_size bytes)
pub fn decode_columnar_bulk_insert(payload: &[u8]) -> Result<BulkInsertRequest> {
    let mut cursor = Cursor::new(payload);

    // Table name
    let table_name_len = read_u16_le(&mut cursor)? as usize;
    let mut table_name_buf = vec![0u8; table_name_len];
    cursor.read_exact(&mut table_name_buf)?;
    let table_name = String::from_utf8_lossy(&table_name_buf).into_owned();

    let _row_count = read_u32_le(&mut cursor)? as usize;
    let column_count = read_u32_le(&mut cursor)? as usize;

    let mut columns_defs = Vec::with_capacity(column_count);
    let mut columns_data = Vec::with_capacity(column_count);

    for _ in 0..column_count {
        // Column header
        let name_len = read_u16_le(&mut cursor)? as usize;
        let mut name_buf = vec![0u8; name_len];
        cursor.read_exact(&mut name_buf)?;
        let name = String::from_utf8_lossy(&name_buf).into_owned();

        let dtype_byte = read_u8(&mut cursor)?;
        let data_type = DataType::from_u8(dtype_byte)?;
        let scale = read_u32_le(&mut cursor)? as i32;
        let precision = read_u32_le(&mut cursor)? as i32;
        let data_len = read_u32_le(&mut cursor)? as usize;
        let nulls_size = read_u32_le(&mut cursor)? as usize;

        columns_defs.push(ColumnDef {
            name,
            data_type,
            nullable: true,
            scale,
            precision,
        });

        // Read data then nulls (encoder writes data first, then nulls)
        let data_bytes_start = cursor.position() as usize;
        let data_bytes_len = calculate_data_size(data_type, data_len, &payload[data_bytes_start..]);

        // Read null bitmap first (after data)
        let nulls_start = data_bytes_start + data_bytes_len;
        let null_bitmap = if nulls_size > 0 && nulls_start + nulls_size <= payload.len() {
            let nulls_data = &payload[nulls_start..nulls_start + nulls_size];
            NullBitmap::from_bytes(nulls_data, data_len)
        } else {
            NullBitmap::new_valid(data_len)
        };

        // Read data values
        let col_data = decode_column_data(&payload[data_bytes_start..], data_type, data_len, &null_bitmap)?;
        columns_data.push(col_data);

        // Advance cursor past data + nulls
        cursor.set_position((nulls_start + nulls_size) as u64);
    }

    let schema = Schema::new(columns_defs);
    let batch = RecordBatch::try_new(schema, columns_data)?;

    Ok(BulkInsertRequest { table_name, batch })
}

/// Calculate the byte size of column data based on type and element count.
/// For variable-length types, we need to scan the actual data.
fn calculate_data_size(data_type: DataType, count: usize, data: &[u8]) -> usize {
    match data_type.fixed_size() {
        Some(fixed) => count * fixed,
        None => {
            // Variable-length: scan through to find total size
            let mut offset = 0usize;
            for _ in 0..count {
                if offset + 4 > data.len() { break; }
                let len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap_or([0u8; 4])) as usize;
                offset += 4 + len;
            }
            offset
        }
    }
}

/// Decode column data from raw bytes into a ColumnArray.
/// In bulk insert wire format, ALL rows have data bytes on the wire (even nulls have placeholders).
fn decode_column_data(data: &[u8], data_type: DataType, count: usize, bitmap: &NullBitmap) -> Result<ColumnArray> {
    let mut cursor = Cursor::new(data);

    macro_rules! read_fixed_all {
        ($ty:ty, $size:expr, $variant:ident) => {{
            let mut vals = Vec::with_capacity(count);
            for _ in 0..count {
                let mut b = [0u8; $size];
                cursor.read_exact(&mut b).map_err(|e| MonaxError::Storage(format!("bulk read error: {}", e)))?;
                vals.push(<$ty>::from_le_bytes(b));
            }
            ColumnArray::$variant { data: vals, bitmap: bitmap.clone() }
        }};
    }

    let col = match data_type {
        DataType::Bool => {
            let mut vals = Vec::with_capacity(count);
            for _ in 0..count {
                let mut b = [0u8; 1];
                cursor.read_exact(&mut b).map_err(|e| MonaxError::Storage(format!("bulk read error: {}", e)))?;
                vals.push(b[0]);
            }
            ColumnArray::Bool { data: vals, bitmap: bitmap.clone() }
        }
        DataType::Int8 => {
            let mut vals = Vec::with_capacity(count);
            for _ in 0..count {
                let mut b = [0u8; 1];
                cursor.read_exact(&mut b).map_err(|e| MonaxError::Storage(format!("bulk read error: {}", e)))?;
                vals.push(b[0] as i8);
            }
            ColumnArray::Int8 { data: vals, bitmap: bitmap.clone() }
        }
        DataType::Int16 => read_fixed_all!(i16, 2, Int16),
        DataType::Int32 => read_fixed_all!(i32, 4, Int32),
        DataType::Int64 => read_fixed_all!(i64, 8, Int64),
        DataType::UInt8 => {
            let mut vals = Vec::with_capacity(count);
            for _ in 0..count {
                let mut b = [0u8; 1];
                cursor.read_exact(&mut b).map_err(|e| MonaxError::Storage(format!("bulk read error: {}", e)))?;
                vals.push(b[0]);
            }
            ColumnArray::UInt8 { data: vals, bitmap: bitmap.clone() }
        }
        DataType::UInt16 => read_fixed_all!(u16, 2, UInt16),
        DataType::UInt32 => read_fixed_all!(u32, 4, UInt32),
        DataType::UInt64 => read_fixed_all!(u64, 8, UInt64),
        DataType::Float32 => read_fixed_all!(f32, 4, Float32),
        DataType::Float64 => read_fixed_all!(f64, 8, Float64),
        DataType::Decimal => read_fixed_all!(i64, 8, Decimal),
        DataType::Date => read_fixed_all!(i32, 4, Date),
        DataType::Time => read_fixed_all!(i64, 8, Time),
        DataType::Timestamp => read_fixed_all!(i64, 8, Timestamp),
        DataType::Interval => read_fixed_all!(i64, 8, Interval),
        DataType::VarChar => {
            let mut vals = Vec::with_capacity(count);
            for _ in 0..count {
                let len = read_u32_le(&mut cursor).map_err(|e| MonaxError::Storage(format!("bulk read error: {}", e)))? as usize;
                let mut s = vec![0u8; len];
                cursor.read_exact(&mut s).map_err(|e| MonaxError::Storage(format!("bulk read error: {}", e)))?;
                vals.push(String::from_utf8_lossy(&s).into_owned());
            }
            ColumnArray::VarChar { data: vals, bitmap: bitmap.clone() }
        }
        DataType::Blob => {
            let mut vals = Vec::with_capacity(count);
            for _ in 0..count {
                let len = read_u32_le(&mut cursor).map_err(|e| MonaxError::Storage(format!("bulk read error: {}", e)))? as usize;
                let mut v = vec![0u8; len];
                cursor.read_exact(&mut v).map_err(|e| MonaxError::Storage(format!("bulk read error: {}", e)))?;
                vals.push(v);
            }
            ColumnArray::Blob { data: vals, bitmap: bitmap.clone() }
        }
        DataType::Vector => {
            let mut vals = Vec::with_capacity(count);
            for _ in 0..count {
                let dim = read_u32_le(&mut cursor).map_err(|e| MonaxError::Storage(format!("bulk read error: {}", e)))? as usize;
                let mut v = Vec::with_capacity(dim);
                for _ in 0..dim {
                    let mut b = [0u8; 4];
                    cursor.read_exact(&mut b).map_err(|e| MonaxError::Storage(format!("bulk read error: {}", e)))?;
                    v.push(f32::from_le_bytes(b));
                }
                vals.push(v);
            }
            ColumnArray::Vector { data: vals, bitmap: bitmap.clone() }
        }
    };

    Ok(col)
}

fn read_u8(cursor: &mut Cursor<&[u8]>) -> std::result::Result<u8, std::io::Error> {
    let mut buf = [0u8; 1];
    cursor.read_exact(&mut buf)?;
    Ok(buf[0])
}

fn read_u16_le(cursor: &mut Cursor<&[u8]>) -> std::result::Result<u16, std::io::Error> {
    let mut buf = [0u8; 2];
    cursor.read_exact(&mut buf)?;
    Ok(u16::from_le_bytes(buf))
}

fn read_u32_le(cursor: &mut Cursor<&[u8]>) -> std::result::Result<u32, std::io::Error> {
    let mut buf = [0u8; 4];
    cursor.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

trait CursorExt {
    fn read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()>;
}

impl CursorExt for Cursor<&[u8]> {
    fn read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
        std::io::Read::read_exact(self, buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_bulk_payload(table: &str, columns: &[(&str, DataType, u32, u32, usize, &[u8], &[u8])]) -> Vec<u8> {
        let mut buf = Vec::new();
        // Table name
        buf.extend_from_slice(&(table.len() as u16).to_le_bytes());
        buf.extend_from_slice(table.as_bytes());

        // Row count (infer from first column)
        let row_count = if columns.is_empty() { 0u32 } else { columns[0].4 as u32 };
        buf.extend_from_slice(&row_count.to_le_bytes());
        buf.extend_from_slice(&(columns.len() as u32).to_le_bytes());

        for &(name, dtype, scale, precision, data_len, data, nulls) in columns {
            // Column header
            buf.extend_from_slice(&(name.len() as u16).to_le_bytes());
            buf.extend_from_slice(name.as_bytes());
            buf.push(dtype as u8);
            buf.extend_from_slice(&scale.to_le_bytes());
            buf.extend_from_slice(&precision.to_le_bytes());
            buf.extend_from_slice(&(data_len as u32).to_le_bytes());
            buf.extend_from_slice(&(nulls.len() as u32).to_le_bytes());
            // Data then nulls
            buf.extend_from_slice(data);
            buf.extend_from_slice(nulls);
        }

        buf
    }

    #[test]
    fn test_decode_simple_int64_column() {
        // 3 rows of Int64: [10, 20, 30]
        let mut data = Vec::new();
        for v in [10i64, 20, 30] {
            data.extend_from_slice(&v.to_le_bytes());
        }

        let payload = build_bulk_payload("test_table", &[
            ("id", DataType::Int64, 0, 0, 3, &data, &[]),
        ]);

        let req = decode_columnar_bulk_insert(&payload).unwrap();
        assert_eq!(req.table_name, "test_table");
        assert_eq!(req.batch.num_rows(), 3);
        assert_eq!(req.batch.num_columns(), 1);
        assert_eq!(req.batch.scalar(0, 0).unwrap(), ScalarValue::Int64(Some(10)));
        assert_eq!(req.batch.scalar(1, 0).unwrap(), ScalarValue::Int64(Some(20)));
        assert_eq!(req.batch.scalar(2, 0).unwrap(), ScalarValue::Int64(Some(30)));
    }

    #[test]
    fn test_decode_mixed_columns() {
        // 2 rows: id(Int64), name(VarChar)
        let mut id_data = Vec::new();
        id_data.extend_from_slice(&1i64.to_le_bytes());
        id_data.extend_from_slice(&2i64.to_le_bytes());

        let mut name_data = Vec::new();
        // "Alice"
        name_data.extend_from_slice(&(5u32).to_le_bytes());
        name_data.extend_from_slice(b"Alice");
        // "Bob"
        name_data.extend_from_slice(&(3u32).to_le_bytes());
        name_data.extend_from_slice(b"Bob");

        let payload = build_bulk_payload("users", &[
            ("id", DataType::Int64, 0, 0, 2, &id_data, &[]),
            ("name", DataType::VarChar, 0, 0, 2, &name_data, &[]),
        ]);

        let req = decode_columnar_bulk_insert(&payload).unwrap();
        assert_eq!(req.table_name, "users");
        assert_eq!(req.batch.num_rows(), 2);
        assert_eq!(req.batch.scalar(0, 0).unwrap(), ScalarValue::Int64(Some(1)));
        assert_eq!(req.batch.scalar(0, 1).unwrap(), ScalarValue::VarChar(Some("Alice".into())));
        assert_eq!(req.batch.scalar(1, 0).unwrap(), ScalarValue::Int64(Some(2)));
        assert_eq!(req.batch.scalar(1, 1).unwrap(), ScalarValue::VarChar(Some("Bob".into())));
    }

    #[test]
    fn test_decode_with_nulls() {
        // 3 rows: [1, NULL, 3] with null bitmap
        let mut data = Vec::new();
        data.extend_from_slice(&1i64.to_le_bytes());
        data.extend_from_slice(&0i64.to_le_bytes()); // placeholder for null
        data.extend_from_slice(&3i64.to_le_bytes());

        // Null bitmap: bit 1 = NULL => uint64 with bit 1 set
        let null_bitmap = (1u64 << 1).to_le_bytes();
        let nulls = &null_bitmap[..];

        let payload = build_bulk_payload("t", &[
            ("val", DataType::Int64, 0, 0, 3, &data, nulls),
        ]);

        let req = decode_columnar_bulk_insert(&payload).unwrap();
        assert_eq!(req.batch.num_rows(), 3);
        assert_eq!(req.batch.scalar(0, 0).unwrap(), ScalarValue::Int64(Some(1)));
        assert!(req.batch.scalar(1, 0).unwrap().is_null());
        assert_eq!(req.batch.scalar(2, 0).unwrap(), ScalarValue::Int64(Some(3)));
    }
}
