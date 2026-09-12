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

    let start = Instant::now();
    let mut base = 0i64;
    for _ in 0..APPEND_ROUNDS {
        let batch = tick_batch(base, BATCH_ROWS);
        mgr.try_append("main", "ticks", &meta, &batch).unwrap();
        base += BATCH_ROWS as i64;
    }
    let append_elapsed = start.elapsed();
    let rows_per_sec = TOTAL_ROWS as f64 / append_elapsed.as_secs_f64();

    let store = mgr.get("main", "ticks").unwrap();
    assert_eq!(store.row_count(), TOTAL_ROWS as u64);

    // "Latest N ticks for symbol X" - the resident fast-path gather the scan
    // layer issues (reverse posting walk + row limit, full projection).
    let mut hits = 0u64;
    let lookup_start = Instant::now();
    for i in 0..LOOKUPS {
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
    let us_per_lookup = lookup_elapsed.as_secs_f64() * 1e6 / LOOKUPS as f64;

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
    assert_eq!(total as usize, TOTAL_ROWS);

    println!("append:  {TOTAL_ROWS} rows in {:.2?} -> {:.0} rows/s", append_elapsed, rows_per_sec);
    println!(
        "lookup:  {LOOKUPS} latest-{} gathers in {:.2?} -> {:.2} us/op ({} rows hit)",
        LOOKUP_LIMIT, lookup_elapsed, us_per_lookup, hits
    );
    println!(
        "count:   {SYMBOLS} key counts in {:.2?} -> total {total}",
        count_elapsed
    );
    println!("resident estimate: {} bytes", store.estimated_memory_bytes());
}
