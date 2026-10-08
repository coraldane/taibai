use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Read};
use std::time::{Duration, Instant};

use bytes::Bytes;
use taibai::net::client::{BulkImportConfig, Client};
use tokio::sync::mpsc;

#[allow(dead_code)]
mod csv_parser;
use csv_parser::ColumnarTickBatch;

const TICK_TABLE: &str = "stock_tick_batch";
const DEFAULT_BATCH_SIZE: usize = 200_000;
const PIPELINE_DEPTH: usize = 8;
const READ_BLOCK_SIZE: usize = 8 * 1024 * 1024;
const CHANNEL_CAPACITY: usize = 32;

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
        "  stock_import <host> <csv_path> [--batch-size=<n>] [--no-header] [--database=<db>]"
    );
    eprintln!();
    eprintln!("Arguments:");
    eprintln!("  host          Server address (e.g. 127.0.0.1:5151)");
    eprintln!("  csv_path      Path to CSV file with stock tick data");
    eprintln!();
    eprintln!("Options:");
    eprintln!(
        "  --batch-size=<n>  Number of rows per batch (default: {})",
        DEFAULT_BATCH_SIZE
    );
    eprintln!("  --no-header       Skip first line as header");
    eprintln!("  --database=<db>   Database name (default: main)");
    eprintln!();
    eprintln!("Optimizations:");
    eprintln!(
        "  - Pipeline mode: {} concurrent in-flight batches",
        PIPELINE_DEPTH
    );
    eprintln!("  - Producer-Consumer dual-task architecture");
    eprintln!("  - String-interned security_id (zero per-row heap allocation)");
    eprintln!("  - Block-level file I/O with manual line splitting");
    eprintln!("  - Precise payload pre-sizing");
    eprintln!("  - Real-time progress display every 5 seconds");
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

    log_msg!("Connecting to {} ...", host);
    let mut client = Client::connect(host).await.expect("connect failed");

    client
        .login("root", "taibai", &database)
        .await
        .expect("login failed");
    log_msg!("Logged in to database '{}'.", database);

    ensure_tick_table(&mut client).await;

    log_msg!("Starting bulk import mode...");
    client
        .start_bulk_import(&BulkImportConfig)
        .await
        .expect("start bulk import failed");

    log_msg!("Starting pipeline with depth={}...", PIPELINE_DEPTH);
    client
        .start_pipeline(PIPELINE_DEPTH)
        .await
        .expect("start pipeline failed");

    log_msg!(
        "Importing from '{}' (batch_size={}, pipeline_depth={})",
        csv_path,
        batch_size,
        PIPELINE_DEPTH
    );
    log_msg!("Progress will be displayed every 5 seconds...");

    let start_time = Instant::now();
    let result = import_tick_pipeline(csv_path, batch_size, skip_header, client).await;

    match result {
        Ok((total_rows, producer_stats, consumer_stats)) => {
            log_msg!(
                "Client stages: parse={:.2}s encode={:.2}s send_wait={:.2}s consumer_send={:.2}s consumer_drain_wait={:.2}s (batches={})",
                producer_stats.parse_us() as f64 / 1e6,
                producer_stats.encode_us() as f64 / 1e6,
                producer_stats.send_wait_us() as f64 / 1e6,
                consumer_stats.send_us() as f64 / 1e6,
                consumer_stats.drain_wait_us() as f64 / 1e6,
                producer_stats.batches()
            );

            let end_to_end = start_time.elapsed();
            log_msg!(
                "END-TO-END (server drain incl.): {} rows in {:.2}s ({:.0} rows/sec)",
                total_rows,
                end_to_end.as_secs_f64(),
                total_rows as f64 / end_to_end.as_secs_f64()
            );
        }
        Err(e) => {
            eprintln!("Import failed: {}", e);
            std::process::exit(1);
        }
    }
    log_msg!("Done.");
}

async fn ensure_tick_table(client: &mut Client) {
    let sql = format!(
        "CREATE TABLE IF NOT EXISTS {} ( \
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
        ) partition by deal_date cluster by (security_id, deal_date, deal_time)",
        TICK_TABLE
    );

    match client.query(&sql).await {
        Ok(taibai::net::client::QueryResult::Ok { .. }) => {
            println!("Table '{}' ensured.", TICK_TABLE);
        }
        Ok(taibai::net::client::QueryResult::Error(e)) => {
            println!("Table ensure warning: {}", e);
        }
        Ok(_) => {}
        Err(e) => println!("Table ensure warning: {}", e),
    }
}

struct ParsedBatch {
    payload: Vec<u8>,
    row_count: usize,
}

#[derive(Default)]
struct ProducerStats {
    /// File read + line scan + field parse (everything except encoding),
    /// accumulated across all parser threads.
    parse_us: std::sync::atomic::AtomicU64,
    /// Columnar payload encoding for completed batches.
    encode_us: std::sync::atomic::AtomicU64,
    /// Time threads spent blocked pushing batches downstream.
    send_wait_us: std::sync::atomic::AtomicU64,
    batches: std::sync::atomic::AtomicU64,
}

impl ProducerStats {
    fn parse_us(&self) -> u128 {
        self.parse_us.load(std::sync::atomic::Ordering::Relaxed) as u128
    }
    fn encode_us(&self) -> u128 {
        self.encode_us.load(std::sync::atomic::Ordering::Relaxed) as u128
    }
    fn send_wait_us(&self) -> u128 {
        self.send_wait_us.load(std::sync::atomic::Ordering::Relaxed) as u128
    }
    fn batches(&self) -> u64 {
        self.batches.load(std::sync::atomic::Ordering::Relaxed)
    }
}

#[derive(Default)]
struct ConsumerStats {
    /// Time spent waiting for pipeline send capacity (drain_one_pipeline_response).
    drain_wait_us: std::sync::atomic::AtomicU64,
    /// Time spent inside the send call itself.
    send_us: std::sync::atomic::AtomicU64,
}

impl ConsumerStats {
    fn from_arc(stats: &std::sync::Arc<ConsumerStats>) -> Self {
        Self {
            drain_wait_us: std::sync::atomic::AtomicU64::new(
                stats
                    .drain_wait_us
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            send_us: std::sync::atomic::AtomicU64::new(
                stats.send_us.load(std::sync::atomic::Ordering::Relaxed),
            ),
        }
    }
    fn drain_wait_us(&self) -> u128 {
        self.drain_wait_us
            .load(std::sync::atomic::Ordering::Relaxed) as u128
    }
    fn send_us(&self) -> u128 {
        self.send_us.load(std::sync::atomic::Ordering::Relaxed) as u128
    }
}

/// One line-aligned chunk of the CSV handed to a parser thread. Lines are
/// complete by construction; `skip_first_line` is set on segment 0 only.
struct CsvSegment {
    id: u64,
    data: Vec<u8>,
    skip_first_line: bool,
}

/// A parsed batch tagged with its (segment, batch-within-segment) position.
/// A single worker processes a whole segment, so per-segment arrivals are
/// strictly ordered; the sequencer restores the cross-segment order using the
/// explicit `SegmentDone` marker (which also skips segments that produced
/// zero batches).
enum IngestMsg {
    Batch {
        seg: u64,
        idx: u64,
        batch: ParsedBatch,
    },
    SegmentDone {
        seg: u64,
    },
}
/// Parser threads (including this machine's usual over-subscription headroom).
const PARSE_THREADS_MAX: usize = 8;
/// Target bytes per line-aligned segment (~2 batches at default settings).
const SEGMENT_TARGET_BYTES: usize = 64 * 1024 * 1024;

/// One ingest fan-out: batches arrive in file order and are split by parity
/// across two connections. Each connection gets a strictly ordered sub-stream
/// and its own consumer task, so both sockets drain concurrently while the
/// server processes one request per connection at a time. Bulk rows have no
/// arrival-order contract across batches (OPTIMIZE reclusters), so the
/// interleave is acceptable for import throughput.
async fn import_tick_pipeline(
    csv_path: &str,
    batch_size: usize,
    skip_header: bool,
    mut client: Client,
) -> Result<
    (usize, std::sync::Arc<ProducerStats>, ConsumerStats),
    Box<dyn std::error::Error + Send + Sync>,
> {
    let (tx, mut rx) = mpsc::channel::<ParsedBatch>(CHANNEL_CAPACITY);

    let csv_path_owned = csv_path.to_string();
    let stats = std::sync::Arc::new(ProducerStats::default());
    let producer = {
        let stats = std::sync::Arc::clone(&stats);
        tokio::task::spawn_blocking(move || {
            run_parallel_ingest(&csv_path_owned, batch_size, skip_header, tx, &stats)
        })
    };

    let start_time = Instant::now();
    let mut total = 0usize;
    let mut last_progress_time = Instant::now();
    let stats_c = std::sync::Arc::new(ConsumerStats::default());
    {
        let c = &mut client;
        while let Some(batch) = rx.recv().await {
            let drain_start = Instant::now();
            while c.pipeline_in_flight() >= PIPELINE_DEPTH as i64 {
                c.drain_one_pipeline_response().await?;
            }
            stats_c.drain_wait_us.fetch_add(
                drain_start.elapsed().as_micros() as u64,
                std::sync::atomic::Ordering::Relaxed,
            );
            let send_start = Instant::now();
            c.pipeline_bulk_insert_columnar(Bytes::from(batch.payload))
                .await?;
            stats_c.send_us.fetch_add(
                send_start.elapsed().as_micros() as u64,
                std::sync::atomic::Ordering::Relaxed,
            );

            total += batch.row_count;

            let now = Instant::now();
            if now.duration_since(last_progress_time) >= Duration::from_secs(5) {
                let elapsed = start_time.elapsed().as_secs_f64();
                log_msg!(
                    "Progress: {} rows sent ({:.0} rows/sec)",
                    total,
                    total as f64 / elapsed
                );
                last_progress_time = now;
            }
        }
    }

    // Surface producer failures instead of silently importing fewer rows.
    producer
        .await
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)??;

    let send_elapsed = start_time.elapsed();
    log_msg!(
        "Send loop done: {} rows in {:.2}s ({:.0} rows/sec)",
        total,
        send_elapsed.as_secs_f64(),
        total as f64 / send_elapsed.as_secs_f64()
    );

    {
        let c = &mut client;
        log_msg!("Draining pipeline responses...");
        if let Err(e) = c.drain_pipeline().await {
            eprintln!("Drain warning: {}", e);
        }
        log_msg!("Stopping pipeline...");
        let _ = c.stop_pipeline().await;
        log_msg!("Stopping bulk import...");
        if let Ok(rows) = c.stop_bulk_import().await {
            log_msg!("Flushed {} buffered rows.", rows);
        }
    }

    Ok((total, stats, ConsumerStats::from_arc(&stats_c)))
}

type IngestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

/// Parallel ingest: one reader thread splits the file into line-aligned
/// segments, N parser threads turn segments into encoded batches, and a
/// sequencer forwards batches to the network pipeline strictly in file order
/// (the server sees the same byte stream as the sequential importer).
fn run_parallel_ingest(
    csv_path: &str,
    batch_size: usize,
    skip_header: bool,
    tx: mpsc::Sender<ParsedBatch>,
    stats: &ProducerStats,
) -> IngestResult {
    let workers = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
        .clamp(1, PARSE_THREADS_MAX);

    let (seg_tx, seg_rx) = std::sync::mpsc::sync_channel::<CsvSegment>(4);
    let (batch_tx, batch_rx) = std::sync::mpsc::sync_channel::<IngestMsg>(32);
    let seg_rx = std::sync::Arc::new(std::sync::Mutex::new(seg_rx));
    let failed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));

    std::thread::scope(|scope| -> IngestResult {
        // Reader: single thread, block reads + line-boundary splitting only.
        let skip_header_reader = skip_header;
        {
            let failed = std::sync::Arc::clone(&failed);
            scope.spawn(move || {
                if let Err(e) = reader_loop(csv_path, skip_header_reader, seg_tx) {
                    eprintln!("Reader error: {}", e);
                    failed.store(true, std::sync::atomic::Ordering::Relaxed);
                }
            });
        }

        for _ in 0..workers {
            let seg_rx = std::sync::Arc::clone(&seg_rx);
            let batch_tx = batch_tx.clone();
            let failed = std::sync::Arc::clone(&failed);
            scope.spawn(move || {
                if let Err(e) = worker_loop(seg_rx, batch_tx, batch_size, stats) {
                    eprintln!("Parser error: {}", e);
                    failed.store(true, std::sync::atomic::Ordering::Relaxed);
                }
            });
        }
        drop(batch_tx);

        // Sequencer: restore strict file order before handing batches over.
        let mut pending: std::collections::BTreeMap<(u64, u64), ParsedBatch> =
            std::collections::BTreeMap::new();
        let mut done_segs: std::collections::BTreeSet<u64> = std::collections::BTreeSet::new();
        let mut cursor = (0u64, 0u64);
        for msg in batch_rx {
            match msg {
                IngestMsg::Batch { seg, idx, batch } => {
                    pending.insert((seg, idx), batch);
                }
                IngestMsg::SegmentDone { seg } => {
                    done_segs.insert(seg);
                }
            }
            // Forward everything contiguous at the cursor; a completed segment
            // with no batch at the cursor is fully forwarded, so skip past it.
            loop {
                while let Some(batch) = pending.remove(&cursor) {
                    let send_start = Instant::now();
                    if tx.blocking_send(batch).is_err() {
                        return Ok(());
                    }
                    stats.send_wait_us.fetch_add(
                        send_start.elapsed().as_micros() as u64,
                        std::sync::atomic::Ordering::Relaxed,
                    );
                    cursor.1 += 1;
                }
                if done_segs.contains(&cursor.0) {
                    done_segs.remove(&cursor.0);
                    cursor.0 += 1;
                    cursor.1 = 0;
                    continue;
                }
                break;
            }
        }
        if !pending.is_empty() || !done_segs.is_empty() {
            return Err(format!(
                "batch sequence gap: {} batch(es) and {} segment(s) never arrived",
                pending.len(),
                done_segs.len()
            )
            .into());
        }
        if failed.load(std::sync::atomic::Ordering::Relaxed) {
            return Err("ingest thread failed".into());
        }
        Ok(())
    })
}

fn reader_loop(
    csv_path: &str,
    skip_header: bool,
    seg_tx: std::sync::mpsc::SyncSender<CsvSegment>,
) -> std::io::Result<()> {
    let file = File::open(csv_path)?;
    let mut reader = BufReader::with_capacity(READ_BLOCK_SIZE, file);

    let mut pending: Vec<u8> = Vec::with_capacity(SEGMENT_TARGET_BYTES + READ_BLOCK_SIZE);
    let mut read_buf = vec![0u8; READ_BLOCK_SIZE];
    let mut segment_id = 0u64;
    let mut first_segment = true;

    loop {
        let bytes_read = reader.read(&mut read_buf)?;
        if bytes_read == 0 {
            break;
        }
        let block_start = pending.len();
        pending.extend_from_slice(&read_buf[..bytes_read]);

        // Find the last complete line; everything up to it is segment
        // material. Scanning only the new block keeps this linear — a full
        // pending rescan per 8MB block made the reader quadratic.
        let last_nl = read_buf[..bytes_read]
            .iter()
            .rposition(|&b| b == b'\n')
            .map(|rel| block_start + rel)
            .or_else(|| pending.iter().rposition(|&b| b == b'\n'));
        if let Some(last_nl) = last_nl {
            if last_nl + 1 >= SEGMENT_TARGET_BYTES {
                let data = pending.split_off(last_nl + 1);
                let segment = std::mem::replace(&mut pending, data);
                seg_tx
                    .send(CsvSegment {
                        id: segment_id,
                        data: segment,
                        skip_first_line: first_segment && skip_header,
                    })
                    .map_err(|_| {
                        std::io::Error::new(std::io::ErrorKind::BrokenPipe, "ingest shut down")
                    })?;
                segment_id += 1;
                first_segment = false;
            }
        }
    }

    if !pending.is_empty() {
        seg_tx
            .send(CsvSegment {
                id: segment_id,
                data: pending,
                skip_first_line: first_segment && skip_header,
            })
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "ingest shut down"))?;
    }
    Ok(())
}

fn worker_loop(
    seg_rx: std::sync::Arc<std::sync::Mutex<std::sync::mpsc::Receiver<CsvSegment>>>,
    batch_tx: std::sync::mpsc::SyncSender<IngestMsg>,
    batch_size: usize,
    stats: &ProducerStats,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    loop {
        let segment = {
            let rx = seg_rx.lock().map_err(|_| "reader thread panicked")?;
            rx.recv()
        };
        let segment = match segment {
            Ok(segment) => segment,
            Err(_) => return Ok(()), // channel closed: all segments processed
        };
        parse_segment(segment, batch_size, &batch_tx, stats)?;
    }
}

fn parse_segment(
    segment: CsvSegment,
    batch_size: usize,
    batch_tx: &std::sync::mpsc::SyncSender<IngestMsg>,
    stats: &ProducerStats,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let seg_start = Instant::now();
    let mut string_pool: HashMap<Vec<u8>, u32> = HashMap::new();
    let mut pool_strings: Vec<String> = Vec::new();
    let mut current_batch = ColumnarTickBatch::new(batch_size);
    let mut batch_index = 0u64;

    let mut first_line = segment.skip_first_line;
    for line_bytes in segment.data.split(|&b| b == b'\n') {
        let trimmed = trim_ascii(line_bytes);
        if trimmed.is_empty() {
            continue;
        }
        if first_line {
            first_line = false;
            continue;
        }
        if parse_tick_csv_line_interned(
            trimmed,
            &mut current_batch,
            &mut string_pool,
            &mut pool_strings,
        ) && current_batch.len() >= batch_size
        {
            flush_batch(
                &current_batch,
                &pool_strings,
                segment.id,
                batch_index,
                batch_tx,
                stats,
            )?;
            batch_index += 1;
            current_batch.reset();
        }
    }
    stats.parse_us.fetch_add(
        seg_start.elapsed().as_micros() as u64,
        std::sync::atomic::Ordering::Relaxed,
    );

    if !current_batch.is_empty() {
        flush_batch(
            &current_batch,
            &pool_strings,
            segment.id,
            batch_index,
            batch_tx,
            stats,
        )?;
    }
    let _ = batch_tx.send(IngestMsg::SegmentDone { seg: segment.id });
    Ok(())
}

fn flush_batch(
    batch: &ColumnarTickBatch,
    pool_strings: &[String],
    seg: u64,
    idx: u64,
    batch_tx: &std::sync::mpsc::SyncSender<IngestMsg>,
    stats: &ProducerStats,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let row_count = batch.len();
    let encode_start = Instant::now();
    let payload = encode_columnar_tick_batch_precise(TICK_TABLE, batch, pool_strings);
    stats.encode_us.fetch_add(
        encode_start.elapsed().as_micros() as u64,
        std::sync::atomic::Ordering::Relaxed,
    );
    stats
        .batches
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    if batch_tx
        .send(IngestMsg::Batch {
            seg,
            idx,
            batch: ParsedBatch { payload, row_count },
        })
        .is_err()
    {
        return Err("ingest shut down".into());
    }
    Ok(())
}

fn parse_tick_csv_line_interned(
    bytes: &[u8],
    batch: &mut ColumnarTickBatch,
    string_pool: &mut HashMap<Vec<u8>, u32>,
    pool_strings: &mut Vec<String>,
) -> bool {
    let mut fields: [&[u8]; 34] = [&[]; 34];
    let mut field_count = 0usize;
    let mut start = 0usize;

    for (i, &b) in bytes.iter().enumerate() {
        if b == b',' {
            if field_count < 34 {
                fields[field_count] = trim_ascii(&bytes[start..i]);
            }
            field_count += 1;
            start = i + 1;
        }
    }
    if start < bytes.len() && field_count < 34 {
        fields[field_count] = trim_ascii(&bytes[start..]);
    }
    field_count += 1;

    if field_count < 13 {
        return false;
    }

    let deal_date = csv_parser::parse_int32_bytes(fields[1]);
    if deal_date == 0 {
        return false;
    }

    let sec_id_raw = fields[0];
    let pool_idx = if let Some(&idx) = string_pool.get(sec_id_raw) {
        idx
    } else {
        let idx = pool_strings.len() as u32;
        string_pool.insert(sec_id_raw.to_vec(), idx);
        pool_strings.push(unsafe { std::str::from_utf8_unchecked(sec_id_raw) }.to_string());
        idx
    };
    batch.security_id_indices.push(pool_idx);

    batch.deal_dates.push(deal_date);
    batch
        .deal_times
        .push(csv_parser::parse_int32_bytes(fields[2]));

    batch
        .pre_closes
        .push(csv_parser::parse_decimal_bytes(fields[3], 2));
    batch
        .open_pxs
        .push(csv_parser::parse_decimal_bytes(fields[4], 2));
    batch
        .high_pxs
        .push(csv_parser::parse_decimal_bytes(fields[5], 2));
    batch
        .low_pxs
        .push(csv_parser::parse_decimal_bytes(fields[6], 2));
    batch
        .close_pxs
        .push(csv_parser::parse_decimal_bytes(fields[7], 2));
    batch
        .high_limits
        .push(csv_parser::parse_decimal_bytes(fields[8], 2));
    batch
        .low_limits
        .push(csv_parser::parse_decimal_bytes(fields[9], 2));

    batch
        .total_num_trades
        .push(csv_parser::parse_int32_bytes(fields[10]));
    batch
        .trans_volumes
        .push(csv_parser::parse_int64_bytes(fields[11]));
    batch
        .trans_values
        .push(csv_parser::parse_decimal_bytes(fields[12], 2));

    if field_count <= 13 {
        fill_zeros_fast(batch);
        return true;
    }

    batch
        .ask_px1s
        .push(csv_parser::parse_decimal_bytes(fields[13], 2));
    batch
        .ask_px2s
        .push(csv_parser::parse_decimal_bytes(fields[14], 2));
    batch
        .ask_px3s
        .push(csv_parser::parse_decimal_bytes(fields[15], 2));
    batch
        .ask_px4s
        .push(csv_parser::parse_decimal_bytes(fields[16], 2));
    batch
        .ask_px5s
        .push(csv_parser::parse_decimal_bytes(fields[17], 2));

    batch
        .ask_vol1s
        .push(csv_parser::parse_int32_bytes(fields[18]));
    batch
        .ask_vol2s
        .push(csv_parser::parse_int32_bytes(fields[19]));
    batch
        .ask_vol3s
        .push(csv_parser::parse_int32_bytes(fields[20]));
    batch
        .ask_vol4s
        .push(csv_parser::parse_int32_bytes(fields[21]));
    batch
        .ask_vol5s
        .push(csv_parser::parse_int32_bytes(fields[22]));

    batch
        .bid_px1s
        .push(csv_parser::parse_decimal_bytes(fields[23], 2));
    batch
        .bid_px2s
        .push(csv_parser::parse_decimal_bytes(fields[24], 2));
    batch
        .bid_px3s
        .push(csv_parser::parse_decimal_bytes(fields[25], 2));
    batch
        .bid_px4s
        .push(csv_parser::parse_decimal_bytes(fields[26], 2));
    batch
        .bid_px5s
        .push(csv_parser::parse_decimal_bytes(fields[27], 2));

    batch
        .bid_vol1s
        .push(csv_parser::parse_int32_bytes(fields[28]));
    batch
        .bid_vol2s
        .push(csv_parser::parse_int32_bytes(fields[29]));
    batch
        .bid_vol3s
        .push(csv_parser::parse_int32_bytes(fields[30]));
    batch
        .bid_vol4s
        .push(csv_parser::parse_int32_bytes(fields[31]));
    batch
        .bid_vol5s
        .push(csv_parser::parse_int32_bytes(fields[32]));

    let local_time = if field_count > 33 && !fields[33].is_empty() {
        csv_parser::parse_timestamp_bytes(fields[33])
    } else {
        0
    };
    batch.local_times.push(local_time);

    true
}

#[inline]
fn trim_ascii(bytes: &[u8]) -> &[u8] {
    let mut start = 0usize;
    let mut end = bytes.len();
    while start < end && bytes[start].is_ascii_whitespace() {
        start += 1;
    }
    while end > start && bytes[end - 1].is_ascii_whitespace() {
        end -= 1;
    }
    &bytes[start..end]
}

#[inline]
fn fill_zeros_fast(batch: &mut ColumnarTickBatch) {
    batch.ask_px1s.push(0);
    batch.ask_px2s.push(0);
    batch.ask_px3s.push(0);
    batch.ask_px4s.push(0);
    batch.ask_px5s.push(0);
    batch.ask_vol1s.push(0);
    batch.ask_vol2s.push(0);
    batch.ask_vol3s.push(0);
    batch.ask_vol4s.push(0);
    batch.ask_vol5s.push(0);
    batch.bid_px1s.push(0);
    batch.bid_px2s.push(0);
    batch.bid_px3s.push(0);
    batch.bid_px4s.push(0);
    batch.bid_px5s.push(0);
    batch.bid_vol1s.push(0);
    batch.bid_vol2s.push(0);
    batch.bid_vol3s.push(0);
    batch.bid_vol4s.push(0);
    batch.bid_vol5s.push(0);
    batch.local_times.push(0);
}

const NUM_COLUMNS: usize = 34;

fn estimate_payload_size(n: usize, avg_sec_id_len: usize) -> usize {
    let table_header = 2 + TICK_TABLE.len();
    let batch_header = 4 + 4;

    let varchar_data = n * (4 + avg_sec_id_len);
    let varchar_col = 2 + 11 + 1 + 1 + 1 + 4 + 4 + varchar_data;

    let int32_cols = 11usize;
    let int32_data = n * 4;
    let int32_col = int32_cols * (2 + 17 + 1 + 1 + 1 + 4 + 4) + int32_cols * int32_data;

    let int64_cols = 1usize;
    let int64_data = n * 8;
    let int64_col = int64_cols * (2 + 14 + 1 + 1 + 1 + 4 + 4) + int64_cols * int64_data;

    let decimal_cols = 7usize + 10usize;
    let decimal_data = n * 8;
    let decimal_col = decimal_cols * (2 + 10 + 1 + 1 + 1 + 4 + 4) + decimal_cols * decimal_data;

    let timestamp_cols = 1usize;
    let timestamp_data = n * 8;
    let timestamp_col =
        timestamp_cols * (2 + 11 + 1 + 1 + 1 + 4 + 4) + timestamp_cols * timestamp_data;

    table_header + batch_header + varchar_col + int32_col + int64_col + decimal_col + timestamp_col
}

fn encode_columnar_tick_batch_precise(
    table_name: &str,
    batch: &ColumnarTickBatch,
    pool_strings: &[String],
) -> Vec<u8> {
    let n = batch.len();
    if n == 0 {
        return Vec::new();
    }

    let avg_sec_id_len = if pool_strings.is_empty() {
        10
    } else {
        let total: usize = pool_strings.iter().map(|s| s.len()).sum();
        (total / pool_strings.len()).max(4)
    };
    let cap = estimate_payload_size(n, avg_sec_id_len);
    let mut buf = Vec::with_capacity(cap);

    buf.extend_from_slice(&(table_name.len() as u16).to_le_bytes());
    buf.extend_from_slice(table_name.as_bytes());
    buf.extend_from_slice(&(n as u32).to_le_bytes());
    buf.extend_from_slice(&(NUM_COLUMNS as u32).to_le_bytes());

    encode_interned_varchar_column(
        &mut buf,
        "security_id",
        &batch.security_id_indices,
        pool_strings,
    );
    encode_int32_column_fast(&mut buf, "deal_date", &batch.deal_dates);
    encode_int32_column_fast(&mut buf, "deal_time", &batch.deal_times);
    encode_decimal_column_fast(&mut buf, "pre_close", &batch.pre_closes, 2, 9);
    encode_decimal_column_fast(&mut buf, "open_px", &batch.open_pxs, 2, 9);
    encode_decimal_column_fast(&mut buf, "high_px", &batch.high_pxs, 2, 9);
    encode_decimal_column_fast(&mut buf, "low_px", &batch.low_pxs, 2, 9);
    encode_decimal_column_fast(&mut buf, "close_px", &batch.close_pxs, 2, 9);
    encode_decimal_column_fast(&mut buf, "high_limit", &batch.high_limits, 2, 9);
    encode_decimal_column_fast(&mut buf, "low_limit", &batch.low_limits, 2, 9);
    encode_int32_column_fast(&mut buf, "total_num_trade", &batch.total_num_trades);
    encode_int64_column_fast(&mut buf, "trans_volume", &batch.trans_volumes);
    encode_decimal_column_fast(&mut buf, "trans_value", &batch.trans_values, 2, 18);
    encode_decimal_column_fast(&mut buf, "ask_px1", &batch.ask_px1s, 2, 9);
    encode_decimal_column_fast(&mut buf, "ask_px2", &batch.ask_px2s, 2, 9);
    encode_decimal_column_fast(&mut buf, "ask_px3", &batch.ask_px3s, 2, 9);
    encode_decimal_column_fast(&mut buf, "ask_px4", &batch.ask_px4s, 2, 9);
    encode_decimal_column_fast(&mut buf, "ask_px5", &batch.ask_px5s, 2, 9);
    encode_int32_column_fast(&mut buf, "ask_vol1", &batch.ask_vol1s);
    encode_int32_column_fast(&mut buf, "ask_vol2", &batch.ask_vol2s);
    encode_int32_column_fast(&mut buf, "ask_vol3", &batch.ask_vol3s);
    encode_int32_column_fast(&mut buf, "ask_vol4", &batch.ask_vol4s);
    encode_int32_column_fast(&mut buf, "ask_vol5", &batch.ask_vol5s);
    encode_decimal_column_fast(&mut buf, "bid_px1", &batch.bid_px1s, 2, 9);
    encode_decimal_column_fast(&mut buf, "bid_px2", &batch.bid_px2s, 2, 9);
    encode_decimal_column_fast(&mut buf, "bid_px3", &batch.bid_px3s, 2, 9);
    encode_decimal_column_fast(&mut buf, "bid_px4", &batch.bid_px4s, 2, 9);
    encode_decimal_column_fast(&mut buf, "bid_px5", &batch.bid_px5s, 2, 9);
    encode_int32_column_fast(&mut buf, "bid_vol1", &batch.bid_vol1s);
    encode_int32_column_fast(&mut buf, "bid_vol2", &batch.bid_vol2s);
    encode_int32_column_fast(&mut buf, "bid_vol3", &batch.bid_vol3s);
    encode_int32_column_fast(&mut buf, "bid_vol4", &batch.bid_vol4s);
    encode_int32_column_fast(&mut buf, "bid_vol5", &batch.bid_vol5s);
    encode_timestamp_column_fast(&mut buf, "local_time", &batch.local_times);

    buf
}

fn write_column_header(
    buf: &mut Vec<u8>,
    name: &str,
    dtype: u8,
    scale: i32,
    precision: i32,
    data_len: u32,
) {
    buf.extend_from_slice(&(name.len() as u16).to_le_bytes());
    buf.extend_from_slice(name.as_bytes());
    buf.push(dtype);
    buf.push(scale as u8);
    buf.push(precision as u8);
    buf.extend_from_slice(&data_len.to_le_bytes());
    buf.extend_from_slice(&0u32.to_le_bytes());
}

fn encode_interned_varchar_column(buf: &mut Vec<u8>, name: &str, indices: &[u32], pool: &[String]) {
    write_column_header(
        buf,
        name,
        taibai::protocol::types::DataType::VarChar as u8,
        0,
        0,
        indices.len() as u32,
    );
    for &idx in indices {
        let s = &pool[idx as usize];
        let bytes = s.as_bytes();
        buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(bytes);
    }
}

fn encode_int32_column_fast(buf: &mut Vec<u8>, name: &str, data: &[i32]) {
    write_column_header(
        buf,
        name,
        taibai::protocol::types::DataType::Int32 as u8,
        0,
        0,
        data.len() as u32,
    );
    let slice = unsafe { std::slice::from_raw_parts(data.as_ptr() as *const u8, data.len() * 4) };
    buf.extend_from_slice(slice);
}

fn encode_int64_column_fast(buf: &mut Vec<u8>, name: &str, data: &[i64]) {
    write_column_header(
        buf,
        name,
        taibai::protocol::types::DataType::Int64 as u8,
        0,
        0,
        data.len() as u32,
    );
    let slice = unsafe { std::slice::from_raw_parts(data.as_ptr() as *const u8, data.len() * 8) };
    buf.extend_from_slice(slice);
}

fn encode_decimal_column_fast(
    buf: &mut Vec<u8>,
    name: &str,
    data: &[i64],
    scale: i32,
    precision: i32,
) {
    write_column_header(
        buf,
        name,
        taibai::protocol::types::DataType::Decimal as u8,
        scale,
        precision,
        data.len() as u32,
    );
    let slice = unsafe { std::slice::from_raw_parts(data.as_ptr() as *const u8, data.len() * 8) };
    buf.extend_from_slice(slice);
}

fn encode_timestamp_column_fast(buf: &mut Vec<u8>, name: &str, data: &[i64]) {
    write_column_header(
        buf,
        name,
        taibai::protocol::types::DataType::Timestamp as u8,
        0,
        0,
        data.len() as u32,
    );
    let slice = unsafe { std::slice::from_raw_parts(data.as_ptr() as *const u8, data.len() * 8) };
    buf.extend_from_slice(slice);
}
