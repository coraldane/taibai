use bytes::Bytes;

use crate::common::error::{Result, TaibaiError};
use crate::types::array::ColumnArray;
use crate::types::batch::RecordBatch;

pub fn encode_columnar_bulk_insert(table_name: &str, batch: &RecordBatch) -> Result<Bytes> {
    let mut buf = Vec::new();

    buf.extend_from_slice(&(table_name.len() as u16).to_le_bytes());
    buf.extend_from_slice(table_name.as_bytes());

    buf.extend_from_slice(&(batch.num_rows() as u32).to_le_bytes());
    buf.extend_from_slice(&(batch.num_columns() as u32).to_le_bytes());

    for col_idx in 0..batch.num_columns() {
        let col = batch.column(col_idx).unwrap();
        let col_def = &batch.schema.columns[col_idx];

        buf.extend_from_slice(&(col_def.name.len() as u16).to_le_bytes());
        buf.extend_from_slice(col_def.name.as_bytes());
        buf.push(col_def.data_type as u8);
        let scale = u8::try_from(col_def.scale).map_err(|_| {
            TaibaiError::Protocol(format!(
                "column '{}' scale {} exceeds u8 bulk metadata range",
                col_def.name, col_def.scale
            ))
        })?;
        let precision = u8::try_from(col_def.precision).map_err(|_| {
            TaibaiError::Protocol(format!(
                "column '{}' precision {} exceeds u8 bulk metadata range",
                col_def.name, col_def.precision
            ))
        })?;
        buf.push(scale);
        buf.push(precision);

        let (data, nulls) = encode_column_data(col)?;

        buf.extend_from_slice(&(batch.num_rows() as u32).to_le_bytes());
        buf.extend_from_slice(&(nulls.len() as u32).to_le_bytes());
        buf.extend_from_slice(&data);
        buf.extend_from_slice(&nulls);
    }

    Ok(Bytes::from(buf))
}

fn encode_column_data(col: &ColumnArray) -> Result<(Vec<u8>, Vec<u8>)> {
    let mut data = Vec::new();
    let mut nulls = Vec::new();
    let _row_count = col.len();

    let null_bitmap = col.bitmap();
    if !null_bitmap.is_all_valid() {
        nulls = null_bitmap.to_bytes();
    }

    match col {
        ColumnArray::Selected { .. } => return encode_column_data(col.flattened()),
        ColumnArray::Bool { data: vals, .. } => {
            for v in vals {
                data.push(*v);
            }
        }
        ColumnArray::Int8 { data: vals, .. } => {
            for v in vals {
                data.push(*v as u8);
            }
        }
        ColumnArray::Int16 { data: vals, .. } => {
            for v in vals {
                data.extend_from_slice(&v.to_le_bytes());
            }
        }
        ColumnArray::Int32 { data: vals, .. } => {
            for v in vals {
                data.extend_from_slice(&v.to_le_bytes());
            }
        }
        ColumnArray::Int64 { data: vals, .. } => {
            for v in vals {
                data.extend_from_slice(&v.to_le_bytes());
            }
        }
        ColumnArray::UInt8 { data: vals, .. } => {
            for v in vals {
                data.push(*v);
            }
        }
        ColumnArray::UInt16 { data: vals, .. } => {
            for v in vals {
                data.extend_from_slice(&v.to_le_bytes());
            }
        }
        ColumnArray::UInt32 { data: vals, .. } => {
            for v in vals {
                data.extend_from_slice(&v.to_le_bytes());
            }
        }
        ColumnArray::UInt64 { data: vals, .. } => {
            for v in vals {
                data.extend_from_slice(&v.to_le_bytes());
            }
        }
        ColumnArray::Float32 { data: vals, .. } => {
            for v in vals {
                data.extend_from_slice(&v.to_le_bytes());
            }
        }
        ColumnArray::Float64 { data: vals, .. } => {
            for v in vals {
                data.extend_from_slice(&v.to_le_bytes());
            }
        }
        ColumnArray::Decimal { data: vals, .. } => {
            for v in vals {
                data.extend_from_slice(&v.to_le_bytes());
            }
        }
        ColumnArray::Date { data: vals, .. } => {
            for v in vals {
                data.extend_from_slice(&v.to_le_bytes());
            }
        }
        ColumnArray::Time { data: vals, .. } => {
            for v in vals {
                data.extend_from_slice(&v.to_le_bytes());
            }
        }
        ColumnArray::Timestamp { data: vals, .. } => {
            for v in vals {
                data.extend_from_slice(&v.to_le_bytes());
            }
        }
        ColumnArray::Interval { data: vals, .. } => {
            for v in vals {
                data.extend_from_slice(&v.to_le_bytes());
            }
        }
        ColumnArray::VarChar { data: vals, .. } => {
            for v in vals {
                data.extend_from_slice(&(v.len() as u32).to_le_bytes());
                data.extend_from_slice(v.as_bytes());
            }
        }
        ColumnArray::Blob { data: vals, .. } => {
            for v in vals {
                data.extend_from_slice(&(v.len() as u32).to_le_bytes());
                data.extend_from_slice(v);
            }
        }
        ColumnArray::Vector { data: vals, .. } => {
            for v in vals {
                data.extend_from_slice(&(v.len() as u32).to_le_bytes());
                for f in v {
                    data.extend_from_slice(&f.to_le_bytes());
                }
            }
        }
    }

    Ok((data, nulls))
}

#[cfg(test)]
#[path = "bulk_importer_test.rs"]
mod tests;
