//! Diagnostic comparison of the per-row ScalarValue decode (legacy
//! algorithm) against the direct typed decode, on a synthetic tick
//! payload. Run with:
//! `cargo test --lib direct_bulk -- --ignored --nocapture`
use super::*;
use crate::protocol::types::DataType;
use crate::storage::bulk_payload::DirectBulkColumn;
use std::time::Instant;

fn tick_request(rows: usize) -> (DirectBulkRequest<'static>, Vec<Vec<u8>>) {
    // Leak payloads to keep 'static references for DirectBulkRequest.
    let mut symbol_bytes: Vec<u8> = Vec::new();
    let mut symbol_ranges: Vec<(usize, usize)> = Vec::with_capacity(rows);
    let mut fixed: [Vec<u8>; 4] = [const { Vec::new() }; 4];
    for seq in 0..rows {
        let symbol = format!("SH{:06}", seq % 5000);
        symbol_ranges.push((symbol_bytes.len(), symbol.len()));
        symbol_bytes.extend_from_slice(symbol.as_bytes());
        fixed[0].extend_from_slice(&20260911i32.to_le_bytes());
        fixed[1].extend_from_slice(&(1_000 + (seq % 90_000) as i64).to_le_bytes());
        fixed[2].extend_from_slice(&100i64.to_le_bytes());
        fixed[3].extend_from_slice(&(1_783_700_000_000_000u64 as i64).to_le_bytes());
    }
    let symbol_bytes: &'static [u8] = Box::leak(symbol_bytes.into_boxed_slice());
    let symbol_ranges: &'static [(usize, usize)] = Box::leak(symbol_ranges.into_boxed_slice());
    let fixed_static: Vec<&'static [u8]> = fixed
        .into_iter()
        .map(|v| Box::leak(v.into_boxed_slice()) as &'static [u8])
        .collect();
    let mk = |name: &'static str,
              dtype: DataType,
              data: &'static [u8],
              ranges: Option<&'static [(usize, usize)]>| DirectBulkColumn {
        name,
        data_type: dtype,
        scale: 0,
        precision: 0,
        data,
        nulls: std::borrow::Cow::Borrowed(&[]),
        variable_ranges: ranges.map(|r| r.to_vec()),
    };
    let columns = vec![
        mk(
            "security_id",
            DataType::VarChar,
            symbol_bytes,
            Some(symbol_ranges),
        ),
        mk("deal_date", DataType::Int32, fixed_static[0], None),
        mk("price", DataType::Decimal, fixed_static[1], None),
        mk("trans_volume", DataType::Int64, fixed_static[2], None),
        mk("local_time", DataType::Timestamp, fixed_static[3], None),
    ];
    (
        DirectBulkRequest {
            table_name: "t",
            row_count: rows,
            columns,
        },
        Vec::new(),
    )
}

#[test]
#[ignore = "diagnostic benchmark: run with --ignored --nocapture"]
fn direct_decode_vs_per_row_scalar() {
    let rows = 100_000;
    let (req, _) = tick_request(rows);

    let start = Instant::now();
    let mut legacy_rows = 0usize;
    for col in &req.columns {
        let mut array = ColumnArray::empty(col.data_type);
        for row_idx in 0..req.row_count {
            let scalar = direct_bulk_column_scalar_at(col, row_idx).unwrap();
            array.push(&scalar).unwrap();
            legacy_rows += 1;
        }
    }
    let legacy = start.elapsed();

    let start = Instant::now();
    let mut direct_rows = 0usize;
    for col in &req.columns {
        let array = direct_bulk_column_to_array(col, req.row_count).unwrap();
        direct_rows += array.len();
    }
    let direct = start.elapsed();

    assert_eq!(legacy_rows, direct_rows);
    println!(
            "decode {rows} rows x {} cols: legacy per-row Scalar = {legacy:?}, direct typed = {direct:?} ({:.1}x faster)",
            req.columns.len(),
            legacy.as_secs_f64() / direct.as_secs_f64()
        );
}
