//! Change subscription (pubsub) demo.
//!
//! Two connections share the workload:
//! * a subscriber that registers a change subscription for `main.demo_ticks`
//!   and waits for pushed notifications;
//! * a writer that inserts rows in batches.
//!
//! Every committed insert batch arrives at the subscriber as one
//! notification with a per-topic sequence number. A gap in `seq` means the
//! server dropped events for this subscription (slow consumer) and the
//! client should re-read the table to catch up.
//!
//! Start a server first:
//!   cargo run --release -- --config conf/example.yaml
//! Then run this demo:
//!   cargo run --release -p pubsub_demo
//!   cargo run --release -p pubsub_demo -- 127.0.0.1:5252

use std::time::Duration;

use taibai::net::client::Client;

const DEFAULT_ADDR: &str = "127.0.0.1:5151";
const USER: &str = "root";
const PASSWORD: &str = "taibai";
const DATABASE: &str = "main";
const TABLE: &str = "demo_ticks";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Optional address override: pubsub_demo [host:port]
    let addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| DEFAULT_ADDR.to_string());

    let mut subscriber = Client::connect(&addr).await?;
    subscriber.login(USER, PASSWORD, DATABASE).await?;
    println!("[subscriber] connected to {addr}");

    let mut writer = Client::connect(&addr).await?;
    writer.login(USER, PASSWORD, DATABASE).await?;
    println!("[writer] connected");

    writer
        .query(&format!(
            "CREATE TABLE IF NOT EXISTS {TABLE} (symbol VARCHAR, price FLOAT64, volume INT64)"
        ))
        .await?;

    let sub_id = subscriber.subscribe(TABLE).await?;
    println!("[subscriber] subscribed to '{TABLE}' (subscription id {sub_id})");

    // Writer loop: five batches of market-tick style inserts.
    let writer_task = tokio::spawn(async move {
        for batch in 0..5u64 {
            let price = 100.0 + batch as f64;
            let sql = format!(
                "INSERT INTO {TABLE} VALUES ('AAPL', {price}, {batch}00), ('MSFT', {price}, {batch}00)"
            );
            if let Err(e) = writer.query(&sql).await {
                eprintln!("[writer] insert failed: {e}");
                return;
            }
            println!("[writer] committed batch {} (2 rows)", batch + 1);
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        println!("[writer] done");
    });

    // Subscriber loop: wait for the five pushed notifications.
    for _ in 0..5 {
        match subscriber
            .recv_notification_timeout(Duration::from_secs(5))
            .await
        {
            Ok(Some(notification)) => {
                println!(
                    "[subscriber] seq={} topic={} rows={} (sub {})",
                    notification.seq,
                    notification.topic,
                    notification.num_rows(),
                    notification.sub_id
                );
                // The payload decodes into ordinary columnar batches; read
                // the first row to prove the data is there.
                if let Some(batch) = notification.batches.first() {
                    if let Some(taibai::types::scalar::ScalarValue::VarChar(Some(symbol))) =
                        batch.scalar(0, 0)
                    {
                        println!("[subscriber]   first row symbol = {symbol}");
                    }
                }
            }
            Ok(None) => {
                println!("[subscriber] no notification within 5s; giving up");
                break;
            }
            Err(e) => {
                eprintln!("[subscriber] receive failed: {e}");
                break;
            }
        }
    }

    writer_task.await?;

    // Tidy up: stop the subscription and clean the demo table.
    subscriber.unsubscribe(sub_id).await?;
    println!("[subscriber] unsubscribed");

    let mut cleanup = Client::connect(&addr).await?;
    cleanup.login(USER, PASSWORD, DATABASE).await?;
    cleanup.query(&format!("DROP TABLE {TABLE}")).await?;

    Ok(())
}
