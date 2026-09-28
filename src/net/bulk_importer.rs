use bytes::Bytes;

use crate::common::error::{Result, TaibaiError};
use crate::types::array::ColumnArray;
use crate::types::batch::{ColumnDef, RecordBatch, Schema};

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

/// Bulk wire layout: column data bytes first (same little-endian /
/// length-prefixed layout as the result encoder), then the null bitmap.
fn encode_column_data(col: &ColumnArray) -> Result<(Vec<u8>, Vec<u8>)> {
    let mut data = Vec::new();
    crate::protocol::result::append_column_data(col, &mut data);
    let nulls = if col.bitmap().is_all_valid() {
        Vec::new()
    } else {
        col.bitmap().to_bytes()
    };
    Ok((data, nulls))
}

/// Decode a payload produced by [`encode_columnar_bulk_insert`] back into a
/// batch. Used by reliable-subscription clients to materialize pushed data
/// frames; the returned schema is derived from the wire column metadata, so
/// it does not require catalog access.
pub fn decode_columnar_bulk_insert(payload: &[u8]) -> Result<RecordBatch> {
    let req = crate::storage::bulk_payload::parse_direct_bulk_payload(payload)?;
    let columns: Vec<ColumnDef> = req
        .columns
        .iter()
        .map(|col| {
            let mut def = ColumnDef::new(col.name, col.data_type);
            def.scale = col.scale;
            def.precision = col.precision;
            def
        })
        .collect();
    let arrays: Vec<ColumnArray> = req
        .columns
        .iter()
        .map(|col| {
            crate::storage::segment_manager::helpers::direct_bulk_column_to_array(
                col,
                req.row_count,
            )
        })
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(Schema::new(columns), arrays)
}

#[cfg(test)]
#[path = "bulk_importer_test.rs"]
mod tests;
