use std::fs::File;
use std::io::{BufRead, BufReader};
use std::time::Instant;

#[path = "../tick.rs"]
mod tick;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let path = &args[1];
    let max_rows: u64 = args
        .iter()
        .find(|a| a.starts_with("--max="))
        .and_then(|a| a.split('=').nth(1))
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(8_000_000);
    let batch_size: usize = args
        .iter()
        .find(|a| a.starts_with("--batch-size="))
        .and_then(|a| a.split('=').nth(1))
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(5000);

    let file = File::open(path).unwrap();
    let reader = BufReader::with_capacity(8 * 1024 * 1024, file);

    let mut sql = String::with_capacity(batch_size * 300);
    let mut batch_rows: usize = 0;
    let mut parsed: u64 = 0;
    let mut bytes_written: usize = 0;
    let start = Instant::now();

    for line in reader.lines() {
        let line = line.unwrap();
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if batch_rows > 0 {
            sql.push_str(", ");
        }
        if !tick::append_tick_csv_values(trimmed, &mut sql) {
            if batch_rows > 0 {
                sql.truncate(sql.len() - 2);
            }
            continue;
        }
        batch_rows += 1;
        parsed += 1;

        if batch_rows >= batch_size {
            bytes_written += sql.len();
            sql.clear();
            tick::append_insert_prefix("stock_tick_multi", &mut sql);
            batch_rows = 0;
            if parsed >= max_rows {
                break;
            }
        }
    }
    if batch_rows > 0 {
        bytes_written += sql.len();
    }

    let elapsed = start.elapsed().as_secs_f64();
    eprintln!(
        "LOCAL FORMAT: {} rows in {:.2}s ({:.0} rows/sec), {:.0} MB sql",
        parsed,
        elapsed,
        parsed as f64 / elapsed,
        bytes_written as f64 / 1_048_576.0
    );
}
