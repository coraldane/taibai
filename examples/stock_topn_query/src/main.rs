use std::time::Instant;

use monax::net::client::{Client, QueryResult};

const TARGET_SQL: &str = "SELECT d.security_id, s.display_name,
    d.open_px, d.high_px, d.low_px, d.close_px, d.pre_close,
    d.trade_value,
    v.turnover_ratio, v.pe_ratio, v.pb_ratio, v.market_cap, v.circulating_market_cap
FROM t_stock_daily d
LEFT JOIN t_security_info s ON d.security_id = s.security_id
LEFT JOIN t_valuation v ON v.deal_date = 20260604 AND d.security_id = v.security_id
WHERE d.deal_date = 20260604
ORDER BY (d.close_px - d.pre_close) / d.pre_close DESC
LIMIT 20 OFFSET 0";

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let host = args.get(1).map(String::as_str).unwrap_or("127.0.0.1:5151");
    let database = args.get(2).map(String::as_str).unwrap_or("main");
    let iterations = args
        .get(3)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(5);

    let mut client = Client::connect(host).await.expect("connect failed");
    client
        .login("root", "taibai", database)
        .await
        .expect("login failed");

    println!("EXPLAIN target query:");
    print_result(client.query(&format!("EXPLAIN {}", TARGET_SQL)).await);

    let mut total_ms = 0.0;
    for i in 0..iterations {
        let start = Instant::now();
        let result = client.query(TARGET_SQL).await;
        let elapsed = start.elapsed();
        total_ms += elapsed.as_secs_f64() * 1000.0;
        print!("run {}: {:.2} ms", i + 1, elapsed.as_secs_f64() * 1000.0);
        match &result {
            Ok(QueryResult::Rows {
                batches,
                server_elapsed,
                ..
            }) => {
                let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                println!(", rows={}, server_elapsed={:?}", rows, server_elapsed);
            }
            Ok(other) => println!(", result={:?}", other),
            Err(e) => println!(", error={}", e),
        }
        if i == 0 {
            print_result(result);
        }
    }

    if iterations > 0 {
        println!("avg: {:.2} ms", total_ms / iterations as f64);
    }

    let _ = client.logout().await;
    let _ = client.close().await;
}

fn print_result(result: monax::common::error::Result<QueryResult>) {
    match result {
        Ok(QueryResult::Rows {
            schema, batches, ..
        }) => {
            println!(
                "schema: {:?}",
                schema.columns.iter().map(|c| &c.name).collect::<Vec<_>>()
            );
            for batch in batches.iter().take(1) {
                for row in 0..batch.num_rows().min(20) {
                    let values: Vec<_> = (0..batch.num_columns())
                        .map(|col| format!("{:?}", batch.scalar(row, col)))
                        .collect();
                    println!("{}", values.join(", "));
                }
            }
        }
        Ok(QueryResult::Ok { rows_affected, .. }) => println!("ok rows_affected={}", rows_affected),
        Ok(QueryResult::Error(e)) => println!("server error: {}", e),
        Err(e) => println!("client error: {}", e),
    }
}
