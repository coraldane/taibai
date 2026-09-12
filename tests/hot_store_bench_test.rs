//! Diagnostic benchmark for the in-memory hot store (kdb-style chunked
//! redesign).
//!
//! Run with:
//! ```text
//! cargo test --release --test hot_store_bench_test -- --ignored --nocapture
//! ```
//!
//! Models the tick-ingestion workload the store is built for: a varchar
//! symbol key (interned), timestamp-ordered appends in medium batches, and
//! "latest N ticks for one symbol" point lookups (reverse gather with a row
//! limit), plus posting-list-only counts. Numbers are comparable across
//! refactors on the same machine.

use std::time::Instant;

use taibai::storage::catalog::schema::TableMeta;
use taibai::storage::core::hot_store::HotStoreManager;
use taibai::types::batch::{ColumnDef, RecordBatch, Schema};
use taibai::types::scalar::ScalarValue;

const BATCH_ROWS: usize = 4_096;
const APPEND_ROUNDS: usize = 245; // ~1M rows in whole batches
const TOTAL_ROWS: usize = APPEND_ROUNDS * BATCH_ROWS;

/// Multiplier for profiling runs (`HOT_BENCH_SCALE=20 cargo test ...`).
fn bench_scale() -> usize {
    std::env::var("HOT_BENCH_SCALE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1)
}
const SYMBOLS: usize = 1_000;
const LOOKUPS: usize = 20_000;
const LOOKUP_LIMIT: usize = 10;

fn tick_schema() -> Schema {
    Schema::new(vec![
        ColumnDef::new("symbol", taibai::protocol::types::DataType::VarChar),
        ColumnDef::new("ts", taibai::protocol::types::DataType::Timestamp),
        ColumnDef::new("bid", taibai::protocol::types::DataType::Float64),
        ColumnDef::new("volume", taibai::protocol::types::DataType::Int64),
    ])
}

fn tick_meta() -> TableMeta {
    let mut meta = TableMeta::new("main", "ticks", tick_schema().columns.clone());
    meta.cluster_key = vec!["symbol".to_string(), "ts".to_string()];
    meta.hot_time_column = Some("ts".to_string());
    meta.hot_index_column = Some("symbol".to_string());
    meta
}

fn tick_batch(base_ts: i64, rows: usize) -> RecordBatch {
    let mut batch = RecordBatch::new(tick_schema());
    for i in 0..rows {
        let seq = base_ts as usize + i;
        let symbol = format!("S{:04}", seq % SYMBOLS);
        batch
            .column_mut(0)
            .unwrap()
            .push(&ScalarValue::VarChar(Some(symbol)))
            .unwrap();
        batch
            .column_mut(1)
            .unwrap()
            .push(&ScalarValue::Timestamp(Some(seq as i64)))
            .unwrap();
        batch
            .column_mut(2)
            .unwrap()
            .push(&ScalarValue::Float64(Some(100.0 + (seq % 997) as f64 / 10.0)))
            .unwrap();
        batch
            .column_mut(3)
            .unwrap()
            .push(&ScalarValue::Int64(Some((seq % 17) as i64 * 100)))
            .unwrap();
    }
    batch
}

#[test]
#[ignore]
fn bench_hot_store_append_and_point_lookup() {
    let mgr = HotStoreManager::new();
    let meta = tick_meta();

    // Phase 0: build the batches outside the timed region so the append
    // number isolates the store's own cost.
    let scale = bench_scale();
    let rounds = APPEND_ROUNDS * scale;
    let batches: Vec<RecordBatch> = (0..rounds)
        .map(|r| tick_batch((r * BATCH_ROWS) as i64, BATCH_ROWS))
        .collect();

    let start = Instant::now();
    for batch in &batches {
        mgr.try_append("main", "ticks", &meta, batch).unwrap();
    }
    let append_elapsed = start.elapsed();
    let append_rows = rounds * BATCH_ROWS;
    let rows_per_sec = append_rows as f64 / append_elapsed.as_secs_f64();

    let store = mgr.get("main", "ticks").unwrap();
    assert_eq!(store.row_count(), append_rows as u64);

    // "Latest N ticks for symbol X" - the resident fast-path gather the scan
    // layer issues (reverse posting walk + row limit, full projection).
    let lookups = LOOKUPS * scale;
    let mut hits = 0u64;
    let lookup_start = Instant::now();
    for i in 0..lookups {
        let symbol = format!("S{:04}", i % SYMBOLS);
        if let Some(batch) = store.lookup_filtered(
            &ScalarValue::VarChar(Some(symbol)),
            None,
            None,
            0,
            true,
            Some(LOOKUP_LIMIT),
            &[],
            &[],
        ) {
            hits += batch.num_rows() as u64;
        }
    }
    let lookup_elapsed = lookup_start.elapsed();
    assert!(hits > 0);
    let us_per_lookup = lookup_elapsed.as_secs_f64() * 1e6 / lookups as f64;

    // Full-key gather: every row of one symbol, ascending. This is the
    // per-symbol analytics scan (large candidates, no row limit).
    let gathers = SYMBOLS * scale; // every symbol exactly `scale` times
    let gather_start = Instant::now();
    let mut gathered = 0u64;
    for i in 0..gathers {
        let symbol = format!("S{:04}", i % SYMBOLS);
        if let Some(batch) = store.lookup_filtered(
            &ScalarValue::VarChar(Some(symbol)),
            None,
            None,
            0,
            false,
            None,
            &[],
            &[],
        ) {
            gathered += batch.num_rows() as u64;
        }
    }
    let gather_elapsed = gather_start.elapsed();
    assert_eq!(gathered, append_rows as u64 * scale as u64);
    let gather_rows_per_sec = gathered as f64 / gather_elapsed.as_secs_f64();

    // Posting-list-only count (composite index not used: no partition column).
    let count_start = Instant::now();
    let mut total = 0u64;
    for i in 0..SYMBOLS {
        let symbol = format!("S{:04}", i);
        total += store
            .count_by_key_partition_from(&ScalarValue::VarChar(Some(symbol)), None, 0)
            .unwrap_or(0);
    }
    let count_elapsed = count_start.elapsed();
    assert_eq!(total as usize, append_rows);

    println!(
        "append:  {append_rows} rows in {:.2?} -> {:.0} rows/s",
        append_elapsed, rows_per_sec
    );
    println!(
        "lookup:  {lookups} latest-{} gathers in {:.2?} -> {:.2} us/op ({} rows hit)",
        LOOKUP_LIMIT, lookup_elapsed, us_per_lookup, hits
    );
    println!(
        "gather:  {gathers} full-key scans, {gathered} rows in {:.2?} -> {:.0} rows/s",
        gather_elapsed, gather_rows_per_sec
    );
    println!(
        "count:   {SYMBOLS} key counts in {:.2?} -> total {total}",
        count_elapsed
    );
    println!("resident estimate: {} bytes", store.estimated_memory_bytes());
}
