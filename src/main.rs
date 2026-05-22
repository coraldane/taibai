mod common;
mod types;
mod protocol;
mod storage;
mod sql;
mod net;

use std::sync::Arc;

use common::config::{RunMode, ServerConfig};
use net::server::Server;
use sql::execution::Engine;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    match RunMode::from_args() {
        RunMode::Server(config) => run_server(config).await,
        RunMode::Client {
            host,
            user,
            password,
            database,
            query,
        } => run_client(host, user, password, database, query).await,
    }
}

async fn run_server(config: ServerConfig) {
    tracing::info!(
        addr = %config.listen_addr,
        data_dir = %config.data_dir,
        database = %config.default_database,
        tls_enabled = config.tls.enabled,
        compression = config.enable_compression,
        "starting monax server"
    );

    if let Err(e) = std::fs::create_dir_all(&config.data_dir) {
        tracing::error!(error = %e, "failed to create data directory");
        std::process::exit(1);
    }

    let engine = match Engine::new(std::path::Path::new(&config.data_dir)) {
        Ok(e) => e,
        Err(e) => {
            tracing::error!(error = %e, "failed to initialize engine");
            std::process::exit(1);
        }
    };

    let config = Arc::new(config);
    let engine = Arc::new(engine);
    Engine::start_flush_thread(&engine);
    let server = Server::new(config, engine.clone());

    let server_handle = tokio::spawn(async move {
        if let Err(e) = server.run().await {
            tracing::error!(error = %e, "server error");
        }
    });

    tokio::select! {
        _ = server_handle => {
            tracing::info!("server loop exited");
        }
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("received Ctrl+C, flushing active segments...");
            match engine.flush_all_buffers() {
                Ok(n) => tracing::info!(rows_flushed = n, "all active segments sealed"),
                Err(e) => tracing::error!(error = %e, "failed to flush segments on shutdown"),
            }
        }
    }
}

async fn run_client(
    host: String,
    user: String,
    password: String,
    database: String,
    query: Option<String>,
) {
    use net::client::{Client, QueryResult};

    let mut client = match Client::connect(&host).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Failed to connect to {}: {}", host, e);
            std::process::exit(1);
        }
    };

    if let Err(e) = client.login(&user, &password, &database).await {
        eprintln!("Login failed: {}", e);
        std::process::exit(1);
    }
    println!("Connected to {} as {}", host, user);

    match client.ping().await {
        Ok(rtt) => println!("Ping: {:?}", rtt),
        Err(e) => eprintln!("Ping failed: {}", e),
    }

    if let Some(sql) = query {
        match client.query(&sql).await {
            Ok(QueryResult::Ok { rows_affected }) => {
                println!("OK, {} row(s) affected", rows_affected);
            }
            Ok(QueryResult::Rows { schema, batches }) => {
                let names: Vec<&str> = schema.column_names().collect();
                println!("{}", names.join("\t"));

                for batch in &batches {
                    for row in 0..batch.num_rows() {
                        let cells: Vec<String> = (0..schema.num_columns())
                            .map(|col| {
                                batch.scalar(row, col)
                                    .map(|v| v.to_text().unwrap_or_else(|| "NULL".to_string()))
                                    .unwrap_or_else(|| "NULL".to_string())
                            })
                            .collect();
                        println!("{}", cells.join("\t"));
                    }
                }
            }
            Ok(QueryResult::Error(msg)) => {
                eprintln!("Server error: {}", msg);
                std::process::exit(1);
            }
            Err(e) => {
                eprintln!("Query failed: {}", e);
                std::process::exit(1);
            }
        }
    } else {
        println!("monax client interactive mode. Type SQL queries or 'quit' to exit.");
        let stdin = std::io::stdin();
        let mut buf = String::new();
        loop {
            buf.clear();
            print!("monax> ");
            if let Err(e) = std::io::Write::flush(&mut std::io::stdout()) {
                eprintln!("IO error: {}", e);
                break;
            }
            if stdin.read_line(&mut buf).is_err() {
                break;
            }
            let sql = buf.trim();
            if sql.eq_ignore_ascii_case("quit")
                || sql.eq_ignore_ascii_case("exit")
                || sql.eq_ignore_ascii_case("\\q")
            {
                break;
            }
            if sql.is_empty() {
                continue;
            }

            match client.query(sql).await {
                Ok(QueryResult::Ok { rows_affected }) => {
                    println!("OK, {} row(s) affected", rows_affected);
                }
                Ok(QueryResult::Rows { schema, batches }) => {
                    let names: Vec<&str> = schema.column_names().collect();
                    println!("{}", names.join("\t"));
                    for batch in &batches {
                        for row in 0..batch.num_rows() {
                            let cells: Vec<String> = (0..schema.num_columns())
                                .map(|col| {
                                    batch.scalar(row, col)
                                        .map(|v| v.to_text().unwrap_or_else(|| "NULL".to_string()))
                                        .unwrap_or_else(|| "NULL".to_string())
                                })
                                .collect();
                            println!("{}", cells.join("\t"));
                        }
                    }
                }
                Ok(QueryResult::Error(msg)) => {
                    eprintln!("Server error: {}", msg);
                }
                Err(e) => {
                    eprintln!("Query failed: {}", e);
                }
            }
        }
    }

    let _ = client.logout().await;
    let _ = client.close().await;
    println!("Disconnected.");
}
