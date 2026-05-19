use std::path::PathBuf;
use std::time::Instant;

use monax::storage::async_segment::HighThroughputIngestor;
use monax::types::batch::{ColumnDef, RecordBatch, Schema};
use monax::types::scalar::ScalarValue;
use monax::protocol::types::DataType;

fn main() {
    eprintln!("=== High Performance Import Benchmark ===");
    
    let total_rows: usize = 200_000;
    let batch_size: usize = 100_000;
    
    eprintln!("Target: {} rows, batch size: {}", total_rows, batch_size);
    
    let schema = Schema::new(vec![
        ColumnDef::new("id", DataType::Int64),
        ColumnDef::new("value", DataType::Float64),
        ColumnDef::new("ts", DataType::Timestamp),
    ]);

    let mut ingestor = HighThroughputIngestor::new(schema.clone())
        .with_row_group_size(50_000)
        .with_compression_level(3);

    let start = Instant::now();
    let mut rows_written = 0usize;

    while rows_written < total_rows {
        let to_write = batch_size.min(total_rows - rows_written);
        let batch = create_batch(&schema, rows_written, to_write);
        
        let needs_flush = ingestor.push_batch(&batch).unwrap();
        if needs_flush {
            ingestor.compress_and_buffer().unwrap();
        }
        rows_written += to_write;
        println!("Written {} rows...", rows_written);
    }

    if ingestor.pending_rows() > 0 {
        ingestor.compress_and_buffer().unwrap();
    }

    let encode_time = start.elapsed();

    println!("Encoding: {} rows in {:.3}s = {:.0} rows/sec", 
        rows_written, encode_time.as_secs_f64(), rows_written as f64 / encode_time.as_secs_f64());

    let write_start = Instant::now();
    let output_path = PathBuf::from("benchmark_test.dat");
    ingestor.seal_to_file(&output_path).unwrap();
    let write_time = write_start.elapsed();

    let total_time = start.elapsed();
    let total_rate = rows_written as f64 / total_time.as_secs_f64();

    println!("Write: {:.3}s", write_time.as_secs_f64());
    println!("Total: {:.3}s = {:.0} rows/sec", total_time.as_secs_f64(), total_rate);

    if total_rate >= 1_000_000.0 {
        println!("SUCCESS: Target achieved!");
    } else {
        println!("Need {:.1}x improvement for 1M rows/sec target", 1_000_000.0 / total_rate);
    }
}

fn create_batch(schema: &Schema, start_id: usize, count: usize) -> RecordBatch {
    let mut batch = RecordBatch::new(schema.clone());
    for i in 0..count {
        batch.column_mut(0).unwrap().push(&ScalarValue::Int64(Some((start_id + i) as i64))).unwrap();
        batch.column_mut(1).unwrap().push(&ScalarValue::Float64(Some((i as f64) * 0.1))).unwrap();
        batch.column_mut(2).unwrap().push(&ScalarValue::Timestamp(Some(1700000000000i64 + i as i64))).unwrap();
    }
    batch
}