mod tick;

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::time::{Duration, Instant};

use monax::net::client::{BulkImportConfig, Client};
use tick::{build_multi_insert_sql, parse_tick_csv_line, TICK_CREATE_TABLE_SQL};

const TICK_TABLE: &str = "stock_tick_multi";
const DEFAULT_BATCH_SIZE: usize = 5000;
const PIPELINE_DEPTH: usize = 4;

macro_rules! log_msg {
    ($($arg:tt)*) => {
        {
            let now = chrono::Local::now();
            print!("[{}] ", now.format("%Y-%m-%d %H:%M:%S"));
            println!($($arg)*);
        }
    };
}

fn print_usage() {
    eprintln!("Usage:");
    eprintln!(
        "  multi_insert <host> <csv_path> [--batch-size=<n>] [--no-header] [--database=<db>]"
    );
    eprintln!();
    eprintln!("Arguments:");
    eprintln!("  host              Server address (e.g. 127.0.0.1:5151)");
    eprintln!("  csv_path          Path to CSV file with stock tick data");
    eprintln!();
    eprintln!("Options:");
    eprintln!(
        "  --batch-size=<n>  Number of rows per INSERT batch (default: {})",
        DEFAULT_BATCH_SIZE
    );
    eprintln!("  --no-header       Skip first line as header");
    eprintln!("  --database=<db>   Database name (default: main)");
    eprintln!("  --drop            Drop table before import (default: true)");
    eprintln!("  --no-drop         Do not drop table before import");
    eprintln!("  --max=<n>         Max rows to import (default: 0 = unlimited)");
    eprintln!(
        "  --pipeline=<n>    Pipeline depth (default: {})",
        PIPELINE_DEPTH
    );
    eprintln!("  --bulk            Use START/STOP BULK IMPORT mode");
    eprintln!();
    eprintln!(
        "This tool tests regular SQL INSERT performance (INSERT ... VALUES (...), (...), ...)."
    );
    eprintln!("Use --bulk to enable bulk import mode for better performance.");
    eprintln!("For bulk binary import, use batch_import instead.");
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 3 {
        print_usage();
        std::process::exit(1);
    }

    let host = &args[1];
    let csv_path = &args[2];

    let batch_size = args
        .iter()
        .find(|a| a.starts_with("--batch-size="))
        .and_then(|a| a.split('=').nth(1))
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(DEFAULT_BATCH_SIZE);

    let skip_header = args.iter().any(|a| a == "--no-header");

    let database = args
        .iter()
        .find(|a| a.starts_with("--database="))
        .and_then(|a| a.split('=').nth(1))
        .map(|s| s.to_string())
        .unwrap_or_else(|| "main".to_string());

    let no_drop = args.iter().any(|a| a == "--no-drop");
    let max_rows = args
        .iter()
        .find(|a| a.starts_with("--max="))
        .and_then(|a| a.split('=').nth(1))
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);

    let pipeline_depth = args
        .iter()
        .find(|a| a.starts_with("--pipeline="))
        .and_then(|a| a.split('=').nth(1))
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(PIPELINE_DEPTH);

    let use_bulk = args.iter().any(|a| a == "--bulk");

    log_msg!("Connecting to {} ...", host);
    let mut client = Client::connect(host).await.expect("connect failed");

    client
        .login("root", "taibai", &database)
        .await
        .expect("login failed");
    log_msg!("Logged in to database '{}'.", database);

    if !no_drop {
        let drop_sql = format!("DROP TABLE {}", TICK_TABLE);
        match client.execute(&drop_sql).await {
            Ok(_) => log_msg!("Dropped table {}.", TICK_TABLE),
            Err(_) => {}
        }
    }

    let create_sql = TICK_CREATE_TABLE_SQL.replace("{table}", TICK_TABLE);
    client
        .execute(&create_sql)
        .await
        .expect("create table failed");
    log_msg!("Created table {}.", TICK_TABLE);

    if use_bulk {
        log_msg!("Starting bulk import mode (WAL deferred sync, delayed catalog save)...");
        client
            .start_bulk_import(&BulkImportConfig { disable_wal: false })
            .await
            .expect("start bulk import failed");
    }

    log_msg!("Starting pipelined multi-value INSERT mode...");
    log_msg!(
        "Importing from '{}' (batch_size={}, pipeline_depth={})",
        csv_path,
        batch_size,
        pipeline_depth
    );

    let start_time = Instant::now();

    let result = import_ticks_pipelined(
        csv_path,
        batch_size,
        skip_header,
        max_rows,
        pipeline_depth,
        &mut client,
    )
    .await;

    let elapsed = start_time.elapsed();

    match result {
        Ok(total_rows) => {
            log_msg!(
                "Import completed: {} rows in {:.2}s ({:.0} rows/sec)",
                total_rows,
                elapsed.as_secs_f64(),
                total_rows as f64 / elapsed.as_secs_f64()
            );
        }
        Err(e) => {
            eprintln!("Import failed: {}", e);
            if use_bulk {
                let _ = client.stop_bulk_import().await;
            }
            let _ = client.logout().await;
            let _ = client.close().await;
            std::process::exit(1);
        }
    }

    if use_bulk {
        log_msg!("Stopping bulk import (flushing buffers + WAL sync + catalog save)...");
        match client.stop_bulk_import().await {
            Ok(rows) => log_msg!("Flushed {} buffered rows.", rows),
            Err(e) => eprintln!("Stop bulk import warning: {}", e),
        }
    }

    let _ = client.logout().await;
    let _ = client.close().await;
    log_msg!("Done.");
}

async fn import_ticks_pipelined(
    csv_path: &str,
    batch_size: usize,
    skip_header: bool,
    max_rows: u64,
    pipeline_depth: usize,
    client: &mut Client,
) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
    let file = File::open(csv_path)?;
    let reader = BufReader::with_capacity(8 * 1024 * 1024, file);

    let mut batch: Vec<tick::TickElement> = Vec::with_capacity(batch_size);
    let mut read_count: u64 = 0;
    let mut parsed_count: u64 = 0;
    let mut sent_count: u64 = 0;
    let mut line_num: u64 = 0;
    let mut in_flight: usize = 0;

    let start_time = Instant::now();
    let mut last_progress_time = Instant::now();

    for line in reader.lines() {
        let line = line?;
        line_num += 1;

        if skip_header && line_num == 1 {
            continue;
        }

        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        read_count += 1;

        if max_rows > 0 && parsed_count >= max_rows {
            break;
        }

        let elem = match parse_tick_csv_line(trimmed) {
            Some(e) => e,
            None => continue,
        };

        parsed_count += 1;
        batch.push(elem);

        if batch.len() >= batch_size {
            let sql = build_multi_insert_sql(TICK_TABLE, &batch);
            client.send_execute(&sql).await?;
            sent_count += batch.len() as u64;
            in_flight += 1;
            batch.clear();

            if in_flight >= pipeline_depth {
                client.recv_execute_result().await?;
                in_flight -= 1;
            }

            let now = Instant::now();
            if now.duration_since(last_progress_time) >= Duration::from_secs(5) {
                let elapsed = start_time.elapsed().as_secs_f64();
                log_msg!(
                    "Progress: read={} parsed={} sent={} ({:.0} rows/sec)",
                    read_count,
                    parsed_count,
                    sent_count,
                    sent_count as f64 / elapsed
                );
                last_progress_time = now;
            }
        }
    }

    if !batch.is_empty() {
        let sql = build_multi_insert_sql(TICK_TABLE, &batch);
        client.send_execute(&sql).await?;
        sent_count += batch.len() as u64;
        in_flight += 1;
    }

    for _ in 0..in_flight {
        client.recv_execute_result().await?;
    }

    Ok(parsed_count)
}
