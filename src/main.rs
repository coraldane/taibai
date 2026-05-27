mod common;
mod net;
mod protocol;
mod sql;
mod storage;
mod types;

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
        addr = %config.listen_addr(),
        data_dir = %config.storage.data_dir,
        database = %config.storage.default_database,
        tls_enabled = config.tls.enabled,
        compression = config.compression.enabled,
        "starting monax server"
    );

    if let Err(e) = std::fs::create_dir_all(&config.storage.data_dir) {
        tracing::error!(error = %e, "failed to create data directory");
        std::process::exit(1);
    }

    let engine = match Engine::new(
        std::path::Path::new(&config.storage.data_dir),
        &config.storage.default_database,
    ) {
        Ok(e) => e,
        Err(e) => {
            tracing::error!(error = %e, "failed to initialize engine");
            std::process::exit(1);
        }
    };

    let config = Arc::new(config);
    let engine = Arc::new(engine);
    let _ = rayon::ThreadPoolBuilder::new().build_global();
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

    let start = std::time::Instant::now();
    if let Some(sql) = query {
        match client.query(&sql).await {
            Ok(QueryResult::Ok {
                rows_affected,
                server_elapsed,
                server_timings,
                client_timings,
            }) => {
                let output_start = std::time::Instant::now();
                println!("OK, {} row(s) affected", rows_affected);
                let output_elapsed = output_start.elapsed();
                print_elapsed(
                    start.elapsed(),
                    server_elapsed,
                    server_timings.as_ref(),
                    client_timings.as_ref(),
                    output_elapsed,
                    None,
                );
            }
            Ok(QueryResult::Rows {
                schema,
                batches,
                server_elapsed,
                server_timings,
                client_timings,
            }) => {
                let output_start = std::time::Instant::now();
                let stdout = std::io::stdout();
                let mut out = std::io::BufWriter::new(stdout.lock());
                if let Err(e) = write_query_rows(&mut out, &schema, &batches) {
                    eprintln!("IO error: {}", e);
                    std::process::exit(1);
                }
                if let Err(e) = std::io::Write::flush(&mut out) {
                    eprintln!("IO error: {}", e);
                    std::process::exit(1);
                }
                let output_elapsed = output_start.elapsed();
                let row_count = total_batch_rows(&batches);
                print_elapsed(
                    start.elapsed(),
                    server_elapsed,
                    server_timings.as_ref(),
                    client_timings.as_ref(),
                    output_elapsed,
                    Some(row_count),
                );
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

            let start = std::time::Instant::now();
            match client.query(sql).await {
                Ok(QueryResult::Ok {
                    rows_affected,
                    server_elapsed,
                    server_timings,
                    client_timings,
                }) => {
                    let output_start = std::time::Instant::now();
                    println!("OK, {} row(s) affected", rows_affected);
                    let output_elapsed = output_start.elapsed();
                    print_elapsed(
                        start.elapsed(),
                        server_elapsed,
                        server_timings.as_ref(),
                        client_timings.as_ref(),
                        output_elapsed,
                        None,
                    );
                }
                Ok(QueryResult::Rows {
                    schema,
                    batches,
                    server_elapsed,
                    server_timings,
                    client_timings,
                }) => {
                    let output_start = std::time::Instant::now();
                    let stdout = std::io::stdout();
                    let mut out = std::io::BufWriter::new(stdout.lock());
                    if let Err(e) = write_query_rows(&mut out, &schema, &batches) {
                        eprintln!("IO error: {}", e);
                        break;
                    }
                    if let Err(e) = std::io::Write::flush(&mut out) {
                        eprintln!("IO error: {}", e);
                        break;
                    }
                    let output_elapsed = output_start.elapsed();
                    let row_count = total_batch_rows(&batches);
                    print_elapsed(
                        start.elapsed(),
                        server_elapsed,
                        server_timings.as_ref(),
                        client_timings.as_ref(),
                        output_elapsed,
                        Some(row_count),
                    );
                }
                Ok(QueryResult::Error(msg)) => {
                    eprintln!("Server error: {} ({:?})", msg, start.elapsed());
                }
                Err(e) => {
                    eprintln!("Query failed: {} ({:?})", e, start.elapsed());
                }
            }
        }
    }

    let _ = client.logout().await;
    let _ = client.close().await;
    println!("Disconnected.");
}

fn print_elapsed(
    total: std::time::Duration,
    server: Option<std::time::Duration>,
    _timings: Option<&net::client::ServerTimings>,
    client: Option<&net::client::ClientTimings>,
    output: std::time::Duration,
    rows: Option<usize>,
) {
    let rows_text = rows
        .map(|rows| format!(", rows: {}", rows))
        .unwrap_or_default();
    match (server, client) {
        (Some(server), Some(client)) => println!(
            "(total: {:?}, server: {:?}, client_recv: {:?}, output: {:?}{})",
            total, server, client.recv, output, rows_text
        ),
        (Some(server), None) => println!(
            "(total: {:?}, server: {:?}, client_recv: n/a, output: {:?}{})",
            total, server, output, rows_text
        ),
        (None, Some(client)) => println!(
            "(total: {:?}, server: n/a, client_recv: {:?}, output: {:?}{})",
            total, client.recv, output, rows_text
        ),
        (None, None) => println!(
            "(total: {:?}, server: n/a, client_recv: n/a, output: {:?}{})",
            total, output, rows_text
        ),
    }
}

fn total_batch_rows(batches: &[types::batch::RecordBatch]) -> usize {
    batches.iter().map(|batch| batch.num_rows()).sum()
}

fn write_query_rows<W: std::io::Write>(
    out: &mut W,
    schema: &types::batch::Schema,
    batches: &[types::batch::RecordBatch],
) -> std::io::Result<()> {
    let mut line = String::with_capacity(schema.num_columns().saturating_mul(24));

    for (idx, col) in schema.columns.iter().enumerate() {
        if idx > 0 {
            line.push('\t');
        }
        line.push_str(&col.name);
    }
    line.push('\n');
    out.write_all(line.as_bytes())?;

    for batch in batches {
        for row in 0..batch.num_rows() {
            line.clear();
            for col_idx in 0..schema.num_columns() {
                if col_idx > 0 {
                    line.push('\t');
                }
                match batch.column(col_idx) {
                    Some(col) => {
                        append_column_cell(&mut line, col, row, schema.columns[col_idx].scale)
                    }
                    None => line.push_str("NULL"),
                }
            }
            line.push('\n');
            out.write_all(line.as_bytes())?;
        }
    }
    out.flush()
}

fn append_column_cell(line: &mut String, col: &types::array::ColumnArray, row: usize, scale: i32) {
    use std::fmt::Write as _;

    if col.bitmap().is_null(row) {
        line.push_str("NULL");
        return;
    }

    match col {
        types::array::ColumnArray::Bool { data, .. } => {
            line.push_str(if data[row] != 0 { "true" } else { "false" });
        }
        types::array::ColumnArray::Int8 { data, .. } => {
            let _ = write!(line, "{}", data[row]);
        }
        types::array::ColumnArray::Int16 { data, .. } => {
            let _ = write!(line, "{}", data[row]);
        }
        types::array::ColumnArray::Int32 { data, .. } => {
            let _ = write!(line, "{}", data[row]);
        }
        types::array::ColumnArray::Int64 { data, .. } => {
            let _ = write!(line, "{}", data[row]);
        }
        types::array::ColumnArray::UInt8 { data, .. } => {
            let _ = write!(line, "{}", data[row]);
        }
        types::array::ColumnArray::UInt16 { data, .. } => {
            let _ = write!(line, "{}", data[row]);
        }
        types::array::ColumnArray::UInt32 { data, .. } => {
            let _ = write!(line, "{}", data[row]);
        }
        types::array::ColumnArray::UInt64 { data, .. } => {
            let _ = write!(line, "{}", data[row]);
        }
        types::array::ColumnArray::Float32 { data, .. } => {
            let _ = write!(line, "{}", data[row]);
        }
        types::array::ColumnArray::Float64 { data, .. } => {
            let _ = write!(line, "{}", data[row]);
        }
        types::array::ColumnArray::Decimal { data, .. } if scale > 0 => {
            append_decimal_scaled(line, data[row], scale);
        }
        types::array::ColumnArray::Decimal { data, .. } => {
            let _ = write!(line, "{}", data[row]);
        }
        types::array::ColumnArray::Date { data, .. } => {
            let _ = write!(line, "{}", data[row]);
        }
        types::array::ColumnArray::Time { data, .. } => {
            let _ = write!(line, "{}", data[row]);
        }
        types::array::ColumnArray::Timestamp { data, .. } => {
            let _ = write!(line, "{}", data[row]);
        }
        types::array::ColumnArray::Interval { data, .. } => {
            let _ = write!(line, "{}", data[row]);
        }
        types::array::ColumnArray::VarChar { data, .. } => line.push_str(&data[row]),
        types::array::ColumnArray::Blob { data, .. } => {
            let _ = write!(line, "{:?}", data[row]);
        }
        types::array::ColumnArray::Vector { data, .. } => {
            let _ = write!(line, "{:?}", data[row]);
        }
    }
}

fn append_decimal_scaled(line: &mut String, value: i64, scale: i32) {
    use std::fmt::Write as _;

    let divisor = 10_i64.pow(scale as u32);
    let sign = if value < 0 { "-" } else { "" };
    let abs = value.abs();
    let _ = write!(
        line,
        "{}{}.{:0width$}",
        sign,
        abs / divisor,
        abs % divisor,
        width = scale as usize
    );
}
