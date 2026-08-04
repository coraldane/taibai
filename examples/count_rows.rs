use taibai::net::client::{Client, QueryResult};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:5152".into());
    let sql = std::env::args()
        .nth(2)
        .map(|raw| raw.replace('+', " "))
        .unwrap_or_else(|| "SELECT count(*) FROM stock_tick_multi".into());
    let mut client = Client::connect(addr.as_str()).await?;
    client.login("root", "taibai", "main").await?;
    match client.query(&sql).await? {
        QueryResult::Rows { batches, .. } => {
            for batch in &batches {
                for row in 0..batch.num_rows() {
                    let mut values = Vec::new();
                    for col in 0..batch.num_columns() {
                        values.push(format!("{:?}", batch.scalar(row, col)));
                    }
                    println!("{}", values.join(" | "));
                }
            }
        }
        other => println!("{other:?}"),
    }
    client.close().await?;
    Ok(())
}
