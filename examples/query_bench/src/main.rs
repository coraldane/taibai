use std::time::Instant;

use taibai::net::client::{Client, QueryResult};

fn print_rows(result: &QueryResult) -> usize {
    match result {
        QueryResult::Rows {
            schema, batches, ..
        } => {
            let names: Vec<String> = schema.column_names().map(|s| s.to_string()).collect();
            println!("columns: {}", names.join(" | "));
            let mut total = 0usize;
            for batch in batches {
                total += batch.num_rows();
                for row in 0..batch.num_rows() {
                    let mut cells = Vec::with_capacity(batch.num_columns());
                    for col in 0..batch.num_columns() {
                        let v = batch.scalar(row, col);
                        cells.push(format!("{:?}", v));
                    }
                    println!("{}", cells.join(" | "));
                }
            }
            total
        }
        QueryResult::Ok { rows_affected, .. } => {
            println!("OK rows_affected={}", rows_affected);
            0
        }
        QueryResult::Error(e) => {
            println!("ERROR: {}", e);
            0
        }
    }
}

fn timings(
    result: &QueryResult,
) -> (
    Option<std::time::Duration>,
    Option<taibai::net::client::ServerTimings>,
) {
    match result {
        QueryResult::Rows {
            server_elapsed,
            server_timings,
            ..
        } => (*server_elapsed, server_timings.clone()),
        QueryResult::Ok {
            server_elapsed,
            server_timings,
            ..
        } => (*server_elapsed, server_timings.clone()),
        QueryResult::Error(_) => (None, None),
    }
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let host = args
        .get(1)
        .cloned()
        .unwrap_or_else(|| "127.0.0.1:5152".to_string());
    let database = args
        .get(2)
        .cloned()
        .unwrap_or_else(|| "quant_base".to_string());
    let sql = match args.get(3) {
        Some(s) if s.starts_with('@') => {
            std::fs::read_to_string(&s[1..]).expect("read sql file failed").trim().to_string()
        }
        Some(s) => s.clone(),
        None => "SELECT security_id, deal_date, deal_time, open_px, high_px, low_px, close_px, trade_volume, trade_value FROM t_stock_minute WHERE security_id = '000529.SZ' AND deal_date >= 20260701 AND deal_date <= 20260731 ORDER BY deal_date, deal_time".to_string(),
    };
    let repeat: usize = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(2);

    let mut client = Client::connect(&host).await.expect("connect failed");
    client.set_timeout(std::time::Duration::from_secs(1800));
    client
        .login("root", "taibai", &database)
        .await
        .expect("login failed");

    let explain_sql = format!("EXPLAIN {}", sql);
    match client.query(&explain_sql).await {
        Ok(res) => {
            println!("=== EXPLAIN ===");
            print_rows(&res);
        }
        Err(e) => println!("EXPLAIN failed: {}", e),
    }

    for i in 0..repeat {
        let start = Instant::now();
        match client.query(&sql).await {
            Ok(res) => {
                let wall = start.elapsed();
                if std::env::var("QB_PRINT_ROWS").is_ok() {
                    print_rows(&res);
                }
                let (server_elapsed, server_timings) = timings(&res);
                let rows = match &res {
                    QueryResult::Rows { batches, .. } => {
                        batches.iter().map(|b| b.num_rows()).sum::<usize>()
                    }
                    QueryResult::Ok { rows_affected, .. } => *rows_affected as usize,
                    QueryResult::Error(e) => {
                        println!("server error: {}", e);
                        0
                    }
                };
                println!(
                    "run {}: rows={} wall={:?} server_elapsed={:?} server_timings={:?}",
                    i + 1,
                    rows,
                    wall,
                    server_elapsed,
                    server_timings
                );
            }
            Err(e) => {
                println!("run {} failed: {}", i + 1, e);
            }
        }
    }

    let _ = client.logout().await;
    let _ = client.close().await;
}
