mod tick;

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::time::{Duration, Instant};

use taibai::net::client::Client;
use tick::{append_insert_prefix, append_tick_csv_values};

const HOT_TABLE: &str = "hot_tick";
const DEFAULT_BATCH_SIZE: usize = 5000;
const PIPELINE_DEPTH: usize = 8;
const DEFAULT_CLIENTS: usize = 4;
const DEFAULT_CLOSE_AT: &str = "15:30";
const HOT_CREATE_TABLE_SQL: &str = "CREATE TABLE {table} ( \
    security_id VARCHAR, \
    deal_date INT32, \
    deal_time INT32, \
    pre_close DECIMAL(9,2), \
    open_px DECIMAL(9,2), \
    high_px DECIMAL(9,2), \
    low_px DECIMAL(9,2), \
    close_px DECIMAL(9,2), \
    high_limit DECIMAL(9,2), \
    low_limit DECIMAL(9,2), \
    total_num_trade INT32, \
    trans_volume INT64, \
    trans_value DECIMAL(18,2), \
    ask_px1 DECIMAL(9,2), \
    ask_px2 DECIMAL(9,2), \
    ask_px3 DECIMAL(9,2), \
    ask_px4 DECIMAL(9,2), \
    ask_px5 DECIMAL(9,2), \
    ask_vol1 INT32, \
    ask_vol2 INT32, \
    ask_vol3 INT32, \
    ask_vol4 INT32, \
    ask_vol5 INT32, \
    bid_px1 DECIMAL(9,2), \
    bid_px2 DECIMAL(9,2), \
    bid_px3 DECIMAL(9,2), \
    bid_px4 DECIMAL(9,2), \
    bid_px5 DECIMAL(9,2), \
    bid_vol1 INT32, \
    bid_vol2 INT32, \
    bid_vol3 INT32, \
    bid_vol4 INT32, \
    bid_vol5 INT32, \
    local_time TIMESTAMP \
) PARTITION BY deal_date CLUSTER BY (security_id, deal_date, deal_time) HOT BY (local_time) INDEX (security_id) CLOSE AT '{close_at}'";

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
    eprintln!("  hot_insert <host> <csv_path> [--batch-size=<n>] [--no-header] [--database=<db>] [--close-at=HH:MM]");
    eprintln!();
    eprintln!("Arguments:");
    eprintln!("  host              Server address (e.g. 127.0.0.1:5151)");
    eprintln!("  csv_path          Path to CSV file with stock tick data");
    eprintln!();
    eprintln!("Options:");
    eprintln!(
        "  --batch-size=<n>  Number of rows per ordinary INSERT batch (default: {})",
        DEFAULT_BATCH_SIZE
    );
    eprintln!("  --no-header       Skip first line as header");
    eprintln!("  --database=<db>   Database name (default: main)");
    eprintln!(
        "  --pipeline=<n>    Pipeline depth per client (default: {})",
        PIPELINE_DEPTH
    );
    eprintln!(
        "  --clients=<n>     Parallel client connections (default: {})",
        DEFAULT_CLIENTS
    );
    eprintln!(
        "  --close-at=HH:MM  HOT CLOSE AT time (default: {})",
        DEFAULT_CLOSE_AT
    );
    eprintln!("  --drop            Drop hot table before import (default: false)");
    eprintln!("  --max=<n>         Max rows to import (default: 0 = unlimited)");
    eprintln!();
    eprintln!("This tool imports the same CSV format as multi_insert into HOT table using ordinary INSERT statements.");
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

    let close_at = args
        .iter()
        .find(|a| a.starts_with("--close-at="))
        .and_then(|a| a.split('=').nth(1))
        .unwrap_or(DEFAULT_CLOSE_AT);

    let pipeline_depth = args
        .iter()
        .find(|a| a.starts_with("--pipeline="))
        .and_then(|a| a.split('=').nth(1))
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(PIPELINE_DEPTH);

    let client_count = args
        .iter()
        .find(|a| a.starts_with("--clients="))
        .and_then(|a| a.split('=').nth(1))
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(DEFAULT_CLIENTS)
        .clamp(1, 16);

    let drop_table = args.iter().any(|a| a == "--drop");
    let max_rows = args
        .iter()
        .find(|a| a.starts_with("--max="))
        .and_then(|a| a.split('=').nth(1))
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);

    log_msg!("Connecting to {} ...", host);
    let mut client = Client::connect(host).await.expect("connect failed");

    client
        .login("root", "taibai", &database)
        .await
        .expect("login failed");
    log_msg!("Logged in to database '{}'.", database);

    if drop_table {
        let drop_sql = format!("DROP TABLE {}", HOT_TABLE);
        match client.execute(&drop_sql).await {
            Ok(_) => log_msg!("Dropped table {}.", HOT_TABLE),
            Err(_) => {}
        }
    }

    let create_sql = HOT_CREATE_TABLE_SQL
        .replace("{table}", HOT_TABLE)
        .replace("{close_at}", close_at);
    log_msg!("Create SQL: {}", create_sql);
    client
        .execute(&create_sql)
        .await
        .expect("create table failed");
    log_msg!(
        "Ensured HOT table {} exists (close_at={}).",
        HOT_TABLE,
        close_at
    );

    let mut clients = vec![client];
    for _ in 1..client_count {
        let mut extra = Client::connect(host).await.expect("connect failed");
        extra
            .login("root", "taibai", &database)
            .await
            .expect("login failed");
        clients.push(extra);
    }

    log_msg!("Starting pipelined ordinary INSERT import...");
    log_msg!(
        "Importing from '{}' into '{}' (batch_size={}, pipeline_depth={}, clients={})",
        csv_path,
        HOT_TABLE,
        batch_size,
        pipeline_depth,
        clients.len()
    );

    let start_time = Instant::now();

    let result = import_hot_ticks_pipelined(
        csv_path,
        batch_size,
        skip_header,
        max_rows,
        pipeline_depth,
        &mut clients,
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
            for mut client in clients {
                let _ = client.logout().await;
                let _ = client.close().await;
            }
            std::process::exit(1);
        }
    }

    for mut client in clients {
        let _ = client.logout().await;
        let _ = client.close().await;
    }
    log_msg!("Done.");
}

async fn import_hot_ticks_pipelined(
    csv_path: &str,
    batch_size: usize,
    skip_header: bool,
    max_rows: u64,
    pipeline_depth: usize,
    clients: &mut [Client],
) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
    let file = File::open(csv_path)?;
    let reader = BufReader::with_capacity(8 * 1024 * 1024, file);

    let mut sql = String::with_capacity(batch_size * 300);
    append_insert_prefix(HOT_TABLE, &mut sql);
    let mut batch_rows: usize = 0;
    let mut read_count: u64 = 0;
    let mut parsed_count: u64 = 0;
    let mut sent_count: u64 = 0;
    let mut line_num: u64 = 0;
    let mut in_flight = vec![0usize; clients.len()];
    let mut next_client = 0usize;

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

        if batch_rows > 0 {
            sql.push_str(", ");
        }
        if !append_tick_csv_values(trimmed, &mut sql) {
            if batch_rows > 0 {
                sql.truncate(sql.len() - 2);
            }
            continue;
        }

        parsed_count += 1;
        batch_rows += 1;

        if batch_rows >= batch_size {
            let client_idx = next_client;
            next_client = (next_client + 1) % clients.len();
            clients[client_idx].send_execute(&sql).await?;
            sent_count += batch_rows as u64;
            in_flight[client_idx] += 1;
            sql.clear();
            append_insert_prefix(HOT_TABLE, &mut sql);
            batch_rows = 0;

            if in_flight[client_idx] >= pipeline_depth {
                clients[client_idx].recv_execute_result().await?;
                in_flight[client_idx] -= 1;
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

    if batch_rows > 0 {
        let client_idx = next_client;
        clients[client_idx].send_execute(&sql).await?;
        sent_count += batch_rows as u64;
        in_flight[client_idx] += 1;
    }

    for client_idx in 0..clients.len() {
        for _ in 0..in_flight[client_idx] {
            clients[client_idx].recv_execute_result().await?;
        }
    }

    Ok(sent_count)
}
