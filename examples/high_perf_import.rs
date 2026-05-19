use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use monax::protocol::fast_bulk::{BulkEncoderV2, decode_bulk_fast};
use monax::storage::async_segment::{HighThroughputIngestor, DEFAULT_ROW_GROUP_SIZE};
use monax::types::batch::{ColumnDef, RecordBatch, Schema};
use monax::types::scalar::ScalarValue;
use monax::protocol::types::DataType;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let total_rows: usize = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(1_000_000);
    let batch_size: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(100_000);
    let output_path = args.get(3).cloned().unwrap_or_else(|| "test_output.dat".to_string());

    println!("High-performance bulk import test");
    println!("Total rows: {}", total_rows);
    println!("Batch size: {}", batch_size);
    println!("Output path: {}", output_path);

    let schema = Schema::new(vec![
        ColumnDef::new("id", DataType::Int64),
        ColumnDef::new("symbol", DataType::VarChar),
        ColumnDef::new("price", DataType::Float64),
        ColumnDef::new("volume", DataType::Int64),
        ColumnDef::new("timestamp", DataType::Timestamp),
    ]);

    let symbols = ["AAPL", "GOOG", "MSFT", "AMZN", "META", "NVDA", "TSLA", "JPM", "V", "WMT"];

    let mut ingestor = HighThroughputIngestor::new(schema.clone())
        .with_row_group_size(DEFAULT_ROW_GROUP_SIZE)
        .with_compression_level(3);

    let mut encoder = BulkEncoderV2::new();

    let start = Instant::now();
    let mut rows_written = 0usize;
    let mut batches_encoded = 0usize;

    while rows_written < total_rows {
        let to_write = batch_size.min(total_rows - rows_written);
        let batch = generate_batch(&schema, rows_written, to_write, &symbols);

        batches_encoded += 1;

        let needs_flush = ingestor.push_batch(&batch).unwrap();
        if needs_flush {
            ingestor.compress_and_buffer().unwrap();
        }

        rows_written += to_write;
    }

    if ingestor.pending_rows > 0 {
        ingestor.compress_and_buffer().unwrap();
    }

    let encoding_time = start.elapsed();
    let rows_per_sec_encoding = rows_written as f64 / encoding_time.as_secs_f64();

    println!("\nEncoding results:");
    println!("  Rows encoded: {}", rows_written);
    println!("  Batches encoded: {}", batches_encoded);
    println!("  Encoding time: {:.2}s", encoding_time.as_secs_f64());
    println!("  Encoding rate: {:.0} rows/sec", rows_per_sec_encoding);
    println!("  Buffer size: {} MB", ingestor.write_buffer_len() / 1_000_000);

    let write_start = Instant::now();
    ingestor.seal_to_file(&PathBuf::from(&output_path)).unwrap();
    let write_time = write_start.elapsed();

    let total_time = start.elapsed();
    let rows_per_sec_total = rows_written as f64 / total_time.as_secs_f64();

    println!("\nWrite results:");
    println!("  Write time: {:.2}s", write_time.as_secs_f64());

    println!("\nOverall results:");
    println!("  Total rows: {}", rows_written);
    println!("  Total time: {:.2}s", total_time.as_secs_f64());
    println!("  Overall rate: {:.0} rows/sec", rows_per_sec_total);

    let file_size = std::fs::metadata(&output_path).unwrap().len();
    println!("  Output file size: {} MB", file_size / 1_000_000);
    println!("  Bytes per row: {:.2}", file_size as f64 / rows_written as f64);

    println!("\n=== TARGET: 1,000,000 rows/sec ===");
    if rows_per_sec_total >= 1_000_000.0 {
        println!("✓ TARGET ACHIEVED!");
    } else {
        let improvement_needed = 1_000_000.0 / rows_per_sec_total;
        println!("✗ Need {:.1}x improvement to reach target", improvement_needed);
    }
}

fn generate_batch(schema: &Schema, start_id: usize, count: usize, symbols: &[&str]) -> RecordBatch {
    let mut batch = RecordBatch::new(schema.clone());

    let symbol_count = symbols.len();

    for i in 0..count {
        let id = (start_id + i) as i64;
        let symbol_idx = i % symbol_count;
        let price = 100.0 + (i as f64 % 1000.0);
        let volume = (i % 10000 + 100) as i64;
        let ts = 1700000000000i64 + (i as i64 * 1000);

        batch.column_mut(0).unwrap().push(&ScalarValue::Int64(Some(id))).unwrap();
        batch.column_mut(1).unwrap().push(&ScalarValue::VarChar(Some(symbols[symbol_idx].to_string()))).unwrap();
        batch.column_mut(2).unwrap().push(&ScalarValue::Float64(Some(price))).unwrap();
        batch.column_mut(3).unwrap().push(&ScalarValue::Int64(Some(volume))).unwrap();
        batch.column_mut(4).unwrap().push(&ScalarValue::Timestamp(Some(ts))).unwrap();
    }

    batch
}