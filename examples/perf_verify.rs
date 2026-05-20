use std::sync::Arc;
use std::time::Instant;

use monax::protocol::types::DataType;
use monax::sql::execution::Engine;
use monax::storage::wal::{WalWriter, RecordType, WalReader, WalRecovery};
use monax::types::batch::{ColumnDef, RecordBatch, Schema};
use monax::types::scalar::ScalarValue;
use monax::sql::execution::wal_helper;

const TICK_COLUMNS: usize = 34;
const NUM_SECURITIES: usize = 10;

fn tick_schema() -> Schema {
    Schema::new(vec![
        ColumnDef::new("security_id", DataType::VarChar),
        ColumnDef::new("deal_date", DataType::Int32),
        ColumnDef::new("deal_time", DataType::Int32),
        ColumnDef::new("pre_close", DataType::Decimal),
        ColumnDef::new("open_px", DataType::Decimal),
        ColumnDef::new("high_px", DataType::Decimal),
        ColumnDef::new("low_px", DataType::Decimal),
        ColumnDef::new("close_px", DataType::Decimal),
        ColumnDef::new("high_limit", DataType::Decimal),
        ColumnDef::new("low_limit", DataType::Decimal),
        ColumnDef::new("total_num_trade", DataType::Int32),
        ColumnDef::new("trans_volume", DataType::Int64),
        ColumnDef::new("trans_value", DataType::Decimal),
        ColumnDef::new("ask_px1", DataType::Decimal),
        ColumnDef::new("ask_px2", DataType::Decimal),
        ColumnDef::new("ask_px3", DataType::Decimal),
        ColumnDef::new("ask_px4", DataType::Decimal),
        ColumnDef::new("ask_px5", DataType::Decimal),
        ColumnDef::new("ask_vol1", DataType::Int32),
        ColumnDef::new("ask_vol2", DataType::Int32),
        ColumnDef::new("ask_vol3", DataType::Int32),
        ColumnDef::new("ask_vol4", DataType::Int32),
        ColumnDef::new("ask_vol5", DataType::Int32),
        ColumnDef::new("bid_px1", DataType::Decimal),
        ColumnDef::new("bid_px2", DataType::Decimal),
        ColumnDef::new("bid_px3", DataType::Decimal),
        ColumnDef::new("bid_px4", DataType::Decimal),
        ColumnDef::new("bid_px5", DataType::Decimal),
        ColumnDef::new("bid_vol1", DataType::Int32),
        ColumnDef::new("bid_vol2", DataType::Int32),
        ColumnDef::new("bid_vol3", DataType::Int32),
        ColumnDef::new("bid_vol4", DataType::Int32),
        ColumnDef::new("bid_vol5", DataType::Int32),
        ColumnDef::new("local_time", DataType::Timestamp),
    ])
}

fn generate_tick_batch(schema: &Schema, start_id: usize, count: usize) -> RecordBatch {
    let securities = ["600000", "000001", "600036", "000651", "601318",
                      "000858", "600519", "000333", "601166", "000568"];

    let mut batch = RecordBatch::new(schema.clone());
    for i in 0..count {
        let idx = start_id + i;
        let sec = securities[idx % NUM_SECURITIES];

        batch.column_mut(0).unwrap().push(&ScalarValue::VarChar(Some(sec.to_string()))).unwrap();
        batch.column_mut(1).unwrap().push(&ScalarValue::Int32(Some(20240101 + (idx % 365) as i32))).unwrap();
        batch.column_mut(2).unwrap().push(&ScalarValue::Int32(Some(93000000 + (idx % 14400) * 1000))).unwrap();
        batch.column_mut(3).unwrap().push(&ScalarValue::Decimal(Some(10000 + (idx % 5000) as i64))).unwrap();
        batch.column_mut(4).unwrap().push(&ScalarValue::Decimal(Some(10050 + (idx % 5000) as i64))).unwrap();
        batch.column_mut(5).unwrap().push(&ScalarValue::Decimal(Some(10100 + (idx % 5000) as i64))).unwrap();
        batch.column_mut(6).unwrap().push(&ScalarValue::Decimal(Some(10000 + (idx % 5000) as i64))).unwrap();
        batch.column_mut(7).unwrap().push(&ScalarValue::Decimal(Some(10080 + (idx % 5000) as i64))).unwrap();
        batch.column_mut(8).unwrap().push(&ScalarValue::Decimal(Some(11000 + (idx % 5000) as i64))).unwrap();
        batch.column_mut(9).unwrap().push(&ScalarValue::Decimal(Some(9000 + (idx % 5000) as i64))).unwrap();
        batch.column_mut(10).unwrap().push(&ScalarValue::Int32(Some(100 + (idx % 1000) as i32))).unwrap();
        batch.column_mut(11).unwrap().push(&ScalarValue::Int64(Some(1000000 + idx as i64))).unwrap();
        batch.column_mut(12).unwrap().push(&ScalarValue::Decimal(Some(50000000 + (idx % 1000000) as i64))).unwrap();
        for col in 13..18 {
            batch.column_mut(col).unwrap().push(&ScalarValue::Decimal(Some(10000 + (idx % 500 + col as i64 * 10) as i64))).unwrap();
        }
        for col in 18..23 {
            batch.column_mut(col).unwrap().push(&ScalarValue::Int32(Some(100 + (idx % 500 + col as i32 * 10) as i32))).unwrap();
        }
        for col in 23..28 {
            batch.column_mut(col).unwrap().push(&ScalarValue::Decimal(Some(9900 + (idx % 500 + col as i64 * 10) as i64))).unwrap();
        }
        for col in 28..33 {
            batch.column_mut(col).unwrap().push(&ScalarValue::Int32(Some(100 + (idx % 500 + col as i32 * 10) as i32))).unwrap();
        }
        batch.column_mut(33).unwrap().push(&ScalarValue::Timestamp(Some(1704067200000 + idx as i64 * 1000))).unwrap();
    }
    batch
}

fn bench_segment_write() {
    println!("\n=== Segment Write Benchmark ===");
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("data").join("main").join("tick");
    std::fs::create_dir_all(&data_dir).unwrap();

    let engine = Arc::new(Engine::new(&dir.path().join("data")).unwrap());
    engine.set_bulk_mode(true);

    let schema = tick_schema();
    let batch_size = 100_000usize;
    let total_rows = 1_000_000usize;

    println!("Batch size: {}, Total rows: {}", batch_size, total_rows);

    let start = Instant::now();
    let mut rows_written = 0usize;
    let mut batch_count = 0usize;

    while rows_written < total_rows {
        let batch = generate_tick_batch(&schema, rows_written, batch_size.min(total_rows - rows_written));
        batch_count += 1;

        engine.exec_bulk_insert_with_wal_control("main", "tick", batch, true).unwrap();
        rows_written += batch_size.min(total_rows - rows_written);
    }

    let flushed = engine.flush_all_buffers().unwrap();
    let elapsed = start.elapsed();
    let rate = rows_written as f64 / elapsed.as_secs_f64();

    println!("Segment write: {} rows in {:.3}s ({:.0} rows/sec)", rows_written, elapsed.as_secs_f64(), rate);
    println!("Flushed {} rows, {} batches", flushed, batch_count);

    let mut seg_size = 0u64;
    if let Ok(entries) = std::fs::read_dir(&data_dir) {
        for entry in entries.flatten() {
            if let Ok(meta) = entry.metadata() {
                seg_size += meta.len();
            }
        }
    }
    println!("Segment files total: {:.2} MB", seg_size as f64 / 1_000_000.0);
    println!("Bytes per row: {:.2}", seg_size as f64 / rows_written as f64);

    assert!(rate >= 100_000.0, "Segment write rate too low: {:.0} rows/sec (min 100k)", rate);
}

fn bench_wal_serialization() {
    println!("\n=== WAL Serialization Benchmark ===");
    let schema = tick_schema();
    let batch = generate_tick_batch(&schema, 0, 50_000);

    let start = Instant::now();
    let iterations = 10;
    let mut total_bytes = 0usize;

    for _ in 0..iterations {
        let data = wal_helper::serialize_batch_for_wal(&batch);
        total_bytes += data.len();
    }

    let elapsed = start.elapsed();
    let total_rows = 50_000 * iterations;
    let rate = total_rows as f64 / elapsed.as_secs_f64();

    println!("WAL serialization: {} rows in {:.3}s ({:.0} rows/sec)", total_rows, elapsed.as_secs_f64(), rate);
    println!("WAL data per batch (50k rows): {:.2} MB", (total_bytes / iterations) as f64 / 1_000_000.0);
    println!("WAL bytes per row: {:.2}", (total_bytes / iterations) as f64 / 50_000.0);
}

fn bench_wal_write() {
    println!("\n=== WAL Write Benchmark ===");
    let dir = tempfile::tempdir().unwrap();
    let wal_dir = dir.path().join("wal");
    let schema = tick_schema();
    let batch = generate_tick_batch(&schema, 0, 50_000);

    let wal_data = wal_helper::serialize_batch_for_wal(&batch);
    println!("WAL data size for 50k rows: {:.2} MB", wal_data.len() as f64 / 1_000_000.0);

    let mut writer = WalWriter::open(&wal_dir, 64 * 1024 * 1024).unwrap();

    let start = Instant::now();
    let total_rows = 500_000usize;
    let batch_size = 50_000usize;
    let mut rows_written = 0usize;

    for txn_id in 0..(total_rows / batch_size) {
        writer.write_batch(txn_id as u64, &[(RecordType::Insert, wal_data.as_slice())]).unwrap();
        rows_written += batch_size;
    }

    let elapsed = start.elapsed();
    let rate_with_sync = rows_written as f64 / elapsed.as_secs_f64();
    println!("WAL write (with sync): {} rows in {:.3}s ({:.0} rows/sec)", rows_written, elapsed.as_secs_f64(), rate_with_sync);

    let wal_size: u64 = std::fs::read_dir(&wal_dir).unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.metadata().map(|m| m.len()).unwrap_or(0))
        .sum();
    println!("WAL total size: {:.2} MB ({:.2} MB per 50k batch)", wal_size as f64 / 1_000_000.0, wal_size as f64 / 1_000_000.0 / (total_rows / batch_size) as f64);

    drop(writer);

    let records = WalReader::new(&wal_dir).read_all().unwrap();
    println!("WAL records read back: {}", records.len());

    assert!(records.len() > 0, "No WAL records found");

    let committed = WalRecovery::replay(&wal_dir).unwrap();
    println!("Committed WAL records: {}", committed.len());
    assert_eq!(committed.len(), total_rows / batch_size, "Recovery record count mismatch");
}

fn bench_wal_write_no_sync() {
    println!("\n=== WAL Write No-Sync Benchmark ===");
    let dir = tempfile::tempdir().unwrap();
    let wal_dir = dir.path().join("wal");
    let schema = tick_schema();
    let batch = generate_tick_batch(&schema, 0, 50_000);

    let wal_data = wal_helper::serialize_batch_for_wal(&batch);

    let mut writer = WalWriter::open(&wal_dir, 64 * 1024 * 1024).unwrap();

    let start = Instant::now();
    let total_rows = 1_000_000usize;
    let batch_size = 50_000usize;
    let mut rows_written = 0usize;

    for txn_id in 0..(total_rows / batch_size) {
        writer.write_batch_no_sync(txn_id as u64, &[(RecordType::Insert, wal_data.as_slice())]).unwrap();
        rows_written += batch_size;
    }

    let elapsed = start.elapsed();
    let rate_no_sync = rows_written as f64 / elapsed.as_secs_f64();
    println!("WAL write (no sync): {} rows in {:.3}s ({:.0} rows/sec)", rows_written, elapsed.as_secs_f64(), rate_no_sync);

    let wal_size: u64 = std::fs::read_dir(&wal_dir).unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.metadata().map(|m| m.len()).unwrap_or(0))
        .sum();
    println!("WAL total size (no sync): {:.2} MB", wal_size as f64 / 1_000_000.0);
}

fn bench_column_encoding() {
    println!("\n=== Column Encoding Benchmark ===");
    let schema = tick_schema();
    let batch = generate_tick_batch(&schema, 0, 100_000);

    let start = Instant::now();
    let iterations = 5;
    for _ in 0..iterations {
        let total_rows = batch.num_rows();
        assert_eq!(total_rows, 100_000);
        for col_idx in 0..batch.num_columns() {
            let col = batch.column(col_idx).unwrap();
            let encoded = monax::storage::column_encoder::encode_column_fast(col).unwrap();
            assert!(encoded.data.len() > 0);
        }
    }
    let elapsed = start.elapsed();
    let total_encoded_rows = 100_000 * iterations;
    let rate = total_encoded_rows as f64 / elapsed.as_secs_f64();

    println!("Column encoding: {} rows in {:.3}s ({:.0} rows/sec)", total_encoded_rows, elapsed.as_secs_f64(), rate);
}

fn bench_zstd_compression() {
    println!("\n=== ZSTD Compression Benchmark ===");
    let schema = tick_schema();
    let batch = generate_tick_batch(&schema, 0, 100_000);

    let mut total_uncompressed = 0usize;
    let mut total_compressed = 0usize;

    let start = Instant::now();
    for col_idx in 0..batch.num_columns() {
        let col = batch.column(col_idx).unwrap();
        let encoded = monax::storage::column_encoder::encode_column_fast(col).unwrap();
        total_uncompressed += encoded.data.len();

        let compressed = zstd::bulk::compress(&encoded.data, 3).unwrap();
        total_compressed += compressed.len();
    }
    let elapsed = start.elapsed();

    println!("Compression: {:.2} MB -> {:.2} MB ({:.1}x ratio) in {:.3}s",
        total_uncompressed as f64 / 1_000_000.0,
        total_compressed as f64 / 1_000_000.0,
        total_uncompressed as f64 / total_compressed as f64,
        elapsed.as_secs_f64());
    println!("Compression speed: {:.0} rows/sec", 100_000.0 / elapsed.as_secs_f64());
}

fn verify_data_integrity() {
    println!("\n=== Data Integrity Verification ===");
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("data");
    let engine = Arc::new(Engine::new(&data_dir).unwrap());

    let schema = tick_schema();

    {
        let mut catalogs = engine.catalogs().write().unwrap();
        let mut catalog = monax::storage::schema::DatabaseCatalog::load_or_create(&data_dir.join("meta"), "main").unwrap();
        catalog.create_table("tick", schema.columns.clone()).unwrap();
        catalogs.insert("main".to_string(), catalog);
    }

    engine.set_bulk_mode(true);

    let batch_size = 100_000usize;
    let total_rows = 300_000usize;
    let expected_rows = total_rows;

    let mut rows_written = 0usize;
    while rows_written < total_rows {
        let batch = generate_tick_batch(&schema, rows_written, batch_size.min(total_rows - rows_written));
        engine.exec_bulk_insert_with_wal_control("main", "tick", batch, true).unwrap();
        rows_written += batch_size.min(total_rows - rows_written);
    }

    let flushed = engine.flush_all_buffers().unwrap();
    println!("Written {} rows, flushed {} rows", rows_written, flushed);
    assert_eq!(flushed, expected_rows as u64, "Flushed row count mismatch: expected {}, got {}", expected_rows, flushed);

    let table_dir = data_dir.join("main").join("tick");
    assert!(table_dir.exists(), "Table directory should exist");

    let segments: Vec<_> = std::fs::read_dir(&table_dir).unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map(|ext| ext == "dat").unwrap_or(false))
        .collect();

    println!("Segment files: {}", segments.len());
    assert!(!segments.is_empty(), "No segment files found");

    let mut total_seg_rows = 0u64;
    for seg_entry in &segments {
        let path = seg_entry.path();
        let reader = monax::storage::segment::SegmentReader::open(&path).unwrap();
        total_seg_rows += reader.row_count();
        println!("  {} rows={}", path.display(), reader.row_count());
    }

    assert_eq!(total_seg_rows, expected_rows as u64,
        "Total segment rows mismatch: expected {}, got {}", expected_rows, total_seg_rows);
    println!("✓ Data integrity verified: {} rows in segments", total_seg_rows);
}

fn verify_wal_integrity() {
    println!("\n=== WAL Integrity Verification ===");
    let dir = tempfile::tempdir().unwrap();
    let wal_dir = dir.path().join("wal");

    let schema = tick_schema();
    let batch = generate_tick_batch(&schema, 0, 10_000);
    let wal_data = wal_helper::serialize_batch_for_wal(&batch);

    let mut writer = WalWriter::open(&wal_dir, 64 * 1024 * 1024).unwrap();

    writer.write_batch(1, &[(RecordType::Insert, wal_data.as_slice())]).unwrap();
    writer.write_batch(2, &[(RecordType::Insert, wal_data.as_slice())]).unwrap();

    drop(writer);

    let records = WalReader::new(&wal_dir).read_all().unwrap();
    assert!(records.len() >= 6, "Expected at least 6 records (3 per batch: Begin+Insert+Commit)");

    let committed = WalRecovery::replay(&wal_dir).unwrap();
    assert_eq!(committed.len(), 2, "Expected 2 committed Insert records");

    for rec in &committed {
        assert_eq!(rec.record_type, RecordType::Insert);
        let recovered_batch = wal_helper::deserialize_batch_from_wal(&rec.data, &schema).unwrap();
        assert_eq!(recovered_batch.num_rows(), 10_000);
        assert_eq!(recovered_batch.num_columns(), TICK_COLUMNS);
    }

    println!("✓ WAL integrity verified: {} committed records, each with {} rows x {} columns",
        committed.len(), 10_000, TICK_COLUMNS);

    let wal_size: u64 = std::fs::read_dir(&wal_dir).unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.metadata().map(|m| m.len()).unwrap_or(0))
        .sum();
    println!("WAL size for 20k rows (2 batches): {:.2} MB", wal_size as f64 / 1_000_000.0);

    let wal_bytes_per_row = wal_size as f64 / 20_000.0;
    let seg_bytes_per_row_estimate = 80.0;
    let ratio = wal_bytes_per_row / seg_bytes_per_row_estimate;
    println!("WAL bytes per row: {:.2}, estimated segment bytes per row: {:.1}, ratio: {:.2}x",
        wal_bytes_per_row, seg_bytes_per_row_estimate, ratio);
}

fn main() {
    println!("========================================");
    println!("  Monax Performance Verification Suite  ");
    println!("========================================");
    println!("Schema: {} columns (tick data)", TICK_COLUMNS);
    println!();

    bench_column_encoding();
    bench_zstd_compression();
    bench_wal_serialization();
    bench_wal_write();
    bench_wal_write_no_sync();
    bench_segment_write();

    verify_data_integrity();
    verify_wal_integrity();

    println!("\n========================================");
    println!("  Performance Verification Complete     ");
    println!("========================================");
}
