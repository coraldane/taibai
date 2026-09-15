use std::time::Instant;

use tempfile::TempDir;

use taibai::net::session::Session;
use taibai::protocol::types::DataType;
use taibai::sql::execution::engine::Engine;

const ROWS_PER_BATCH: u32 = 5_000;
const BATCHES: u32 = 60;
const SYMBOLS: usize = 5_000;

struct TickRow {
    symbol_seq: usize,
    deal_date: i32,
    deal_time: i32,
    price_unscaled: i64,
    volume: i64,
    local_time: i64,
}

fn tick_row(seq: usize) -> TickRow {
    TickRow {
        symbol_seq: seq % SYMBOLS,
        deal_date: 20260911,
        deal_time: 34_200_000 + (seq % 14_400) as i32,
        price_unscaled: 1_000 + (seq % 90_000) as i64,
        volume: 100 + (seq % 900) as i64,
        local_time: 1_783_700_000_000_000 + seq as i64 * 1_000,
    }
}

fn write_column_header(
    payload: &mut Vec<u8>,
    name: &str,
    dtype: DataType,
    scale: u8,
    precision: u8,
    rows: u32,
) {
    payload.extend_from_slice(&(name.len() as u16).to_le_bytes());
    payload.extend_from_slice(name.as_bytes());
    payload.push(dtype as u8);
    payload.push(scale);
    payload.push(precision);
    payload.extend_from_slice(&rows.to_le_bytes());
    payload.extend_from_slice(&0u32.to_le_bytes());
}

fn build_v1_payload(first_seq: usize) -> Vec<u8> {
    let rows = ROWS_PER_BATCH as usize;
    let mut symbol_data = Vec::with_capacity(rows * 10);
    let mut fixed: [Vec<u8>; 5] = [const { Vec::new() }; 5];
    for idx in 0..rows {
        let row = tick_row(first_seq + idx);
        let symbol = format!("SH{:06}", row.symbol_seq);
        symbol_data.extend_from_slice(&(symbol.len() as u32).to_le_bytes());
        symbol_data.extend_from_slice(symbol.as_bytes());
        fixed[0].extend_from_slice(&row.deal_date.to_le_bytes());
        fixed[1].extend_from_slice(&row.deal_time.to_le_bytes());
        fixed[2].extend_from_slice(&row.price_unscaled.to_le_bytes());
        fixed[3].extend_from_slice(&row.volume.to_le_bytes());
        fixed[4].extend_from_slice(&row.local_time.to_le_bytes());
    }
    let mut payload = Vec::with_capacity(64 + symbol_data.len() + rows * 36);
    payload.extend_from_slice(&("rt_ticks".len() as u16).to_le_bytes());
    payload.extend_from_slice(b"rt_ticks");
    payload.extend_from_slice(&ROWS_PER_BATCH.to_le_bytes());
    payload.extend_from_slice(&6u32.to_le_bytes());
    write_column_header(
        &mut payload,
        "security_id",
        DataType::VarChar,
        0,
        0,
        ROWS_PER_BATCH,
    );
    payload.extend_from_slice(&symbol_data);
    write_column_header(
        &mut payload,
        "deal_date",
        DataType::Int32,
        0,
        0,
        ROWS_PER_BATCH,
    );
    payload.extend_from_slice(&fixed[0]);
    write_column_header(
        &mut payload,
        "deal_time",
        DataType::Int32,
        0,
        0,
        ROWS_PER_BATCH,
    );
    payload.extend_from_slice(&fixed[1]);
    write_column_header(
        &mut payload,
        "price",
        DataType::Decimal,
        2,
        9,
        ROWS_PER_BATCH,
    );
    payload.extend_from_slice(&fixed[2]);
    write_column_header(
        &mut payload,
        "trans_volume",
        DataType::Int64,
        0,
        0,
        ROWS_PER_BATCH,
    );
    payload.extend_from_slice(&fixed[3]);
    write_column_header(
        &mut payload,
        "local_time",
        DataType::Timestamp,
        0,
        0,
        ROWS_PER_BATCH,
    );
    payload.extend_from_slice(&fixed[4]);
    payload
}

fn build_v2_payload(first_seq: usize) -> Vec<u8> {
    let rows = ROWS_PER_BATCH as usize;
    let mut symbol_data = Vec::with_capacity(rows * 8);
    let mut symbol_offsets = Vec::with_capacity(rows + 1);
    symbol_offsets.push(0u32);
    let mut fixed: [Vec<u8>; 5] = [const { Vec::new() }; 5];
    for idx in 0..rows {
        let row = tick_row(first_seq + idx);
        let symbol = format!("SH{:06}", row.symbol_seq);
        symbol_data.extend_from_slice(symbol.as_bytes());
        symbol_offsets.push(symbol_data.len() as u32);
        fixed[0].extend_from_slice(&row.deal_date.to_le_bytes());
        fixed[1].extend_from_slice(&row.deal_time.to_le_bytes());
        fixed[2].extend_from_slice(&row.price_unscaled.to_le_bytes());
        fixed[3].extend_from_slice(&row.volume.to_le_bytes());
        fixed[4].extend_from_slice(&row.local_time.to_le_bytes());
    }
    let mut payload = Vec::with_capacity(64 + symbol_data.len() + rows * 40);
    payload.extend_from_slice(&0xFFFFu16.to_le_bytes());
    payload.push(2);
    payload.extend_from_slice(&("rt_ticks".len() as u16).to_le_bytes());
    payload.extend_from_slice(b"rt_ticks");
    payload.extend_from_slice(&ROWS_PER_BATCH.to_le_bytes());
    payload.extend_from_slice(&6u32.to_le_bytes());
    write_column_header(
        &mut payload,
        "security_id",
        DataType::VarChar,
        0,
        0,
        ROWS_PER_BATCH,
    );
    payload.extend_from_slice(&(symbol_data.len() as u32).to_le_bytes());
    payload.extend_from_slice(&((symbol_offsets.len() * 4) as u32).to_le_bytes());
    payload.extend_from_slice(&symbol_data);
    for offset in &symbol_offsets {
        payload.extend_from_slice(&offset.to_le_bytes());
    }
    let fixed_columns = [
        ("deal_date", DataType::Int32, 0u8, 0u8, &fixed[0]),
        ("deal_time", DataType::Int32, 0, 0, &fixed[1]),
        ("price", DataType::Decimal, 2, 9, &fixed[2]),
        ("trans_volume", DataType::Int64, 0, 0, &fixed[3]),
        ("local_time", DataType::Timestamp, 0, 0, &fixed[4]),
    ];
    for (name, dtype, scale, precision, data) in fixed_columns {
        write_column_header(&mut payload, name, dtype, scale, precision, ROWS_PER_BATCH);
        payload.extend_from_slice(&(data.len() as u32).to_le_bytes());
        payload.extend_from_slice(&0u32.to_le_bytes());
        payload.extend_from_slice(data);
    }
    payload
}

fn run_phase(engine: &Engine, _session: &mut Session, label: &str, binary_v2: bool) {
    let start = Instant::now();
    let mut total_rows = 0u64;
    for batch_idx in 0..BATCHES {
        let first_seq = batch_idx as usize * ROWS_PER_BATCH as usize;
        let payload = if binary_v2 {
            build_v2_payload(first_seq)
        } else {
            build_v1_payload(first_seq)
        };
        let written = engine
            .exec_bulk_insert_payload_with_options("main", &payload, false)
            .expect("bulk insert");
        total_rows += written;
    }
    let elapsed = start.elapsed();
    let rows_per_sec = total_rows as f64 / elapsed.as_secs_f64();
    eprintln!(
        "[bulk-encoding-bench] encoding={} batches={} total_rows={} wall_ms={} rows/s={:.0}",
        label,
        BATCHES,
        total_rows,
        elapsed.as_millis(),
        rows_per_sec
    );
}

#[test]
#[ignore = "diagnostic benchmark: run with --ignored --nocapture"]
fn bulk_payload_throughput() {
    let dir = TempDir::new().expect("tempdir");
    let engine = Engine::new_with_storage_config_and_auto_compact(
        dir.path(),
        "main",
        taibai::storage::segment_manager::SegmentConfig::default(),
        taibai::sql::execution::AutoCompactConfig::default(),
        64 * 1024 * 1024,
        true, // async fsync: exposes the CPU-side decode cost
        taibai::common::config::PubSubSettings::default(),
        // bench engines do not exercise restart warmup
        false,
    )
    .expect("engine");
    let mut session = Session::new("root".to_string(), "main".to_string());
    engine
        .execute(
            &mut session,
            "CREATE TABLE rt_ticks (\
                security_id VARCHAR, deal_date INT32, deal_time INT32, \
                price DECIMAL(9,2), trans_volume INT64, local_time TIMESTAMP\
            ) PARTITION BY deal_date CLUSTER BY (security_id, deal_date) \
            HOT BY (local_time) INDEX (security_id)",
        )
        .expect("create table");

    // Warm up so page caches, hot store, and segment state are hot.
    run_phase(&engine, &mut session, "warmup", false);
    run_phase(&engine, &mut session, "v1_length_prefixed", false);
    run_phase(&engine, &mut session, "v1_repeat", false);
}
