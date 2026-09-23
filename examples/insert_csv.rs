use std::io::BufRead;

use taibai::net::client::Client;

/// Load a CSV file (exported by export_csv) via ordinary pipelined INSERT
/// statements — the same write path as hot_insert, exercising the
/// memtable + hot store mirror instead of the bulk-to-segment path.
///
/// Usage: insert_csv <host> <database> <table> <csv_path> [batch_rows]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let host = args
        .get(1)
        .cloned()
        .unwrap_or_else(|| "127.0.0.1:5252".into());
    let database = args.get(2).cloned().unwrap_or_else(|| "main".into());
    let table = args.get(3).cloned().expect("table name required");
    let csv_path = args.get(4).cloned().expect("csv path required");
    let batch_rows: usize = args.get(5).and_then(|s| s.parse().ok()).unwrap_or(2_000);
    let max_rows: usize = args
        .get(6)
        .and_then(|s| s.parse().ok())
        .unwrap_or(usize::MAX);
    let no_header = args.iter().any(|a| a == "--no-header");

    let mut client = Client::connect(host.as_str()).await?;
    client.set_timeout(std::time::Duration::from_secs(3600));
    client.login("root", "taibai", &database).await?;

    let start = std::time::Instant::now();
    let file = std::fs::File::open(&csv_path)?;
    let mut reader = std::io::BufReader::with_capacity(1 << 22, file);
    let mut header = String::new();
    reader.read_line(&mut header)?;
    let cols: Vec<String> = if no_header {
        vec![
            "id".into(),
            "gmt_create".into(),
            "security_id".into(),
            "deal_date".into(),
            "deal_time".into(),
            "time_key".into(),
            "open_px".into(),
            "high_px".into(),
            "low_px".into(),
            "close_px".into(),
            "trans_volume".into(),
            "trans_value".into(),
            "mean".into(),
        ]
    } else {
        header
            .trim_end()
            .split(',')
            .map(|s| s.trim_matches('"').to_string())
            .collect()
    };
    let col_list = cols.join(", ");

    let mut total = 0usize;
    let mut line = String::new();
    let mut sql = String::with_capacity(batch_rows * 200);
    while total < max_rows {
        line.clear();
        let n = reader.read_line(&mut line)?;
        if n == 0 {
            break;
        }
        let trimmed = line.trim_end();
        if trimmed.is_empty() {
            continue;
        }
        if sql.is_empty() {
            sql.push_str(&format!("INSERT INTO {} ({}) VALUES ", table, col_list));
        } else {
            sql.push_str(", ");
        }
        sql.push('(');
        for (i, field) in trimmed.split(',').enumerate() {
            if i > 0 {
                sql.push_str(", ");
            }
            append_sql_value(field.trim(), &cols[i], &mut sql);
        }
        sql.push(')');
        total += 1;
        if total % batch_rows == 0 {
            client.send_execute(&sql).await?;
            client.recv_execute_result().await?;
            sql.clear();
            if total % 100_000 == 0 {
                client.recv_execute_result().await?;
                eprintln!("inserted {} rows, elapsed {:?}", total, start.elapsed());
            }
        }
    }
    if !sql.is_empty() {
        client.send_execute(&sql).await?;
        client.recv_execute_result().await?;
    }
    eprintln!(
        "done: {} rows in {:?}, server said OK for every batch",
        total,
        start.elapsed()
    );
    client.close().await?;
    Ok(())
}

fn append_sql_value(field: &str, col: &str, out: &mut String) {
    if field.is_empty() || field == "NULL" {
        out.push_str("NULL");
        return;
    }
    if col == "security_id" {
        out.push('\'');
        out.push_str(field);
        out.push('\'');
    } else {
        out.push_str(field);
    }
}
