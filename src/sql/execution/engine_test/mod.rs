use super::*;
use crate::net::session::Session;
use crate::protocol::types::DataType;
use crate::storage::segment_manager::SegmentManager;
use crate::types::scalar::ScalarValue;

mod aggregate_cache;
mod amplification_regressions;
mod audit_regression;
mod bulk_payload;
mod catalog_stats;
mod cancellation;
mod crud_root_regressions;
mod dml;
mod durability;
mod lifecycle;
mod p0_regressions;
mod replace_semantics;
mod unique_index;
mod window_regressions;

fn session() -> Session {
    Session::new("root".to_string(), "main".to_string())
}

fn bulk_payload(table: &str, row_count: u32, columns: Vec<(&str, DataType, Vec<u8>)>) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&(table.len() as u16).to_le_bytes());
    payload.extend_from_slice(table.as_bytes());
    payload.extend_from_slice(&row_count.to_le_bytes());
    payload.extend_from_slice(&(columns.len() as u32).to_le_bytes());
    for (name, dtype, data) in columns {
        payload.extend_from_slice(&(name.len() as u16).to_le_bytes());
        payload.extend_from_slice(name.as_bytes());
        payload.push(dtype as u8);
        payload.push(0);
        payload.push(0);
        payload.extend_from_slice(&row_count.to_le_bytes());
        payload.extend_from_slice(&0u32.to_le_bytes());
        payload.extend_from_slice(&data);
    }
    payload
}

fn int64_col(values: &[i64]) -> Vec<u8> {
    let mut data = Vec::new();
    for value in values {
        data.extend_from_slice(&value.to_le_bytes());
    }
    data
}

fn first_text(result: ExecResult, row: usize, col: usize) -> String {
    let ExecResult::Query { batches, .. } = result else {
        panic!("expected query result");
    };
    batches[0]
        .scalar(row, col)
        .and_then(|v| v.to_text())
        .unwrap()
}

fn first_i64(result: ExecResult, row: usize, col: usize) -> i64 {
    let ExecResult::Query { batches, .. } = result else {
        panic!("expected query result");
    };
    match batches[0].scalar(row, col).unwrap() {
        ScalarValue::Int64(Some(v)) => v,
        ScalarValue::Int32(Some(v)) => v as i64,
        value => panic!("unexpected value {:?}", value),
    }
}

fn inject_unvalidated_bulk_row(
    engine: &Engine,
    db: &str,
    table: &str,
    columns: Vec<(&str, DataType, Vec<u8>)>,
) -> Result<()> {
    let payload = bulk_payload(table, 1, columns);
    let catalog = {
        let catalogs = engine.catalogs().read().unwrap();
        catalogs.get(db).cloned().expect("database exists")
    };
    engine
        .segment_mgr()
        .bulk_insert_payload_with_catalog(db, &payload, &catalog, true)?;
    Ok(())
}
