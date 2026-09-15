use std::io::Write;

use taibai::net::client::{Client, QueryResult};

/// Export a query result to CSV.
///
/// Usage: export_csv <host> <database> <sql> <output_path> [flush_every_rows]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let host = args.get(1).cloned().unwrap_or_else(|| "127.0.0.1:5151".into());
    let database = args.get(2).cloned().unwrap_or_else(|| "base_realtime".into());
    let sql = args
        .get(3)
        .cloned()
        .expect("usage: export_csv <host> <db> <sql> <out.csv> [flush_every]");
    let out_path = args.get(4).cloned().expect("missing output path");
    let flush_every: usize = args.get(5).and_then(|s| s.parse().ok()).unwrap_or(200_000);

    let mut client = Client::connect(host.as_str()).await?;
    client.set_timeout(std::time::Duration::from_secs(3600));
    client.login("root", "taibai", &database).await?;

    let start = std::time::Instant::now();
    let result = client.query(&sql).await?;
    let rows = match result {
        QueryResult::Rows { schema, batches, .. } => {
            let mut out = std::io::BufWriter::with_capacity(1 << 20, std::fs::File::create(&out_path)?);
            let names: Vec<String> = schema
                .column_names()
                .map(|s| csv_escape(&s))
                .collect();
            writeln!(out, "{}", names.join(","))?;
            let mut total = 0usize;
            for batch in &batches {
                let mut buf = String::with_capacity(1 << 16);
                for row in 0..batch.num_rows() {
                    for col in 0..batch.num_columns() {
                        if col > 0 {
                            buf.push(',');
                        }
                        let v = batch.scalar(row, col);
                        if let Some(s) = v.map(|s| s.to_string()) {
                            if s.contains(',') || s.contains('"') || s.contains('\n') {
                                buf.push_str(&csv_escape(&s));
                            } else {
                                buf.push_str(&s);
                            }
                        }
                    }
                    buf.push('\n');
                    total += 1;
                    if total % flush_every == 0 {
                        out.write_all(buf.as_bytes())?;
                        buf.clear();
                    }
                }
                out.write_all(buf.as_bytes())?;
            }
            out.flush()?;
            total
        }
        QueryResult::Ok { rows_affected, .. } => {
            eprintln!("no rows, affected={}", rows_affected);
            0
        }
        QueryResult::Error(e) => return Err(format!("server error: {}", e).into()),
    };
    eprintln!(
        "exported {} rows to {} in {:?}",
        rows,
        out_path,
        start.elapsed()
    );
    client.close().await?;
    Ok(())
}

fn csv_escape(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}
