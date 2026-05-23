use std::path::PathBuf;
use std::process;

use clap::Parser;
use monax::storage::segment::SegmentReader;
use monax::tools::segment::discover_segments;

#[derive(Parser, Debug)]
#[command(
    name = "stock_filter",
    about = "Filter stock tick data from segment files by security_id",
    version
)]
struct Args {
    #[arg(long, default_value = "/Users/coral/Downloads/data/monax/main/stock_tick_multi")]
    data_dir: PathBuf,

    #[arg(long, default_value = "688256.SH")]
    security_id: String,

    #[arg(short, long)]
    verbose: bool,

    #[arg(long)]
    limit: Option<usize>,
}

fn main() {
    let args = Args::parse();

    if !args.data_dir.exists() {
        eprintln!("error: directory not found: {}", args.data_dir.display());
        process::exit(1);
    }

    println!("scanning directory: {}", args.data_dir.display());
    println!("filtering security_id: {}\n", args.security_id);

    let files = match discover_segments(&args.data_dir) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("error: failed to scan segment files: {}", e);
            process::exit(1);
        }
    };

    if files.is_empty() {
        println!("no segment files found");
        return;
    }

    println!("found {} segment files\n", files.len());

    let mut total_matching: u64 = 0;
    let limit = args.limit.unwrap_or(usize::MAX);

    for (i, file) in files.iter().enumerate() {
        let name = file.file_name().and_then(|n| n.to_str()).unwrap_or("?");

        match filter_segment(file, &args.security_id, args.verbose, limit - total_matching as usize) {
            Ok(count) => {
                total_matching += count;
                if args.verbose {
                    println!("[{}] {} matching rows: {}", i + 1, name, count);
                } else {
                    println!("[{}] {}: {} matching rows", i + 1, name, count);
                }
            }
            Err(e) => {
                eprintln!("[{}] {}: error: {}", i + 1, name, e);
            }
        }

        if total_matching as usize >= limit {
            break;
        }
    }

    println!("\n=================================");
    println!("total matching rows: {}", total_matching);
}

fn filter_segment(
    path: &std::path::Path,
    security_id: &str,
    verbose: bool,
    remaining: usize,
) -> monax::common::error::Result<u64> {
    let reader = SegmentReader::open(path)?;
    let schema = reader.schema();

    let col_idx = match schema.index_of("security_id") {
        Some(idx) => idx,
        None => {
            println!("  warning: security_id column not found, available columns:");
            for col in &schema.columns {
                println!("    - {}", col.name);
            }
            return Ok(0);
        }
    };

    let mut match_count: u64 = 0;
    let num_rgs = reader.num_row_groups();

    for rg_idx in 0..num_rgs {
        if remaining > 0 && match_count as usize >= remaining {
            break;
        }

        let batch = reader.read_row_group(rg_idx)?;
        let col = batch.column(col_idx).unwrap();

        for row in 0..batch.num_rows() {
            if remaining > 0 && match_count as usize >= remaining {
                break;
            }

            let val = col.scalar(row);
            if scalar_matches_security_id(&val, security_id) {
                match_count += 1;
                if verbose || match_count <= 20 {
                    print_row(&batch, row, &schema);
                }
            }
        }
    }

    Ok(match_count)
}

fn scalar_matches_security_id(val: &monax::types::scalar::ScalarValue, target: &str) -> bool {
    use monax::types::scalar::ScalarValue;
    match val {
        ScalarValue::VarChar(Some(s)) => s == target,
        _ => false,
    }
}

fn format_scalar(val: &monax::types::scalar::ScalarValue) -> String {
    use monax::types::scalar::ScalarValue;
    match val {
        ScalarValue::VarChar(Some(s)) => s.clone(),
        ScalarValue::VarChar(None) => "NULL".to_string(),
        ScalarValue::Int32(Some(n)) => n.to_string(),
        ScalarValue::Int32(None) => "NULL".to_string(),
        ScalarValue::Int64(Some(n)) => n.to_string(),
        ScalarValue::Int64(None) => "NULL".to_string(),
        ScalarValue::Decimal(Some(n)) => format_decimal(*n),
        ScalarValue::Decimal(None) => "NULL".to_string(),
        ScalarValue::Timestamp(Some(t)) => format_timestamp(*t),
        ScalarValue::Timestamp(None) => "NULL".to_string(),
        ScalarValue::Float64(Some(f)) => format!("{:.4}", f),
        ScalarValue::Float64(None) => "NULL".to_string(),
        _ => format!("{:?}", val),
    }
}

fn format_decimal(n: i64) -> String {
    if n == 0 {
        "0".to_string()
    } else {
        let abs = n.abs();
        let integer_part = abs / 10000;
        let decimal_part = abs % 10000;
        let sign = if n < 0 { "-" } else { "" };
        format!("{}{}.{}", sign, integer_part, format!("{:04}", decimal_part))
    }
}

fn format_timestamp(ts: i64) -> String {
    let micros = ts % 1_000_000;
    let secs = ts / 1_000_000;
    let mins = secs / 60;
    let hours = mins / 60;
    let days = hours / 24;

    let hour = hours % 24;
    let minute = mins % 60;
    let second = secs % 60;

    let mut year = 1970;
    let mut days_left = days;
    loop {
        let days_in_year = if is_leap_year(year) { 366 } else { 365 };
        if days_left >= days_in_year {
            days_left -= days_in_year;
            year += 1;
        } else {
            break;
        }
    }

    let month_days = if is_leap_year(year) {
        [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    } else {
        [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    };

    let mut month = 1;
    let mut day = days_left + 1;
    for &days_in_month in &month_days {
        if day <= days_in_month {
            break;
        }
        day -= days_in_month;
        month += 1;
    }

    format!("{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}", year, month, day, hour, minute, second, micros)
}

fn is_leap_year(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

fn print_row(batch: &monax::types::batch::RecordBatch, row_idx: usize, schema: &monax::types::batch::Schema) {
    let mut parts = Vec::new();
    for col_idx in 0..batch.num_columns() {
        let col = batch.column(col_idx).unwrap();
        let val = col.scalar(row_idx);
        let col_name = &schema.columns[col_idx].name;
        parts.push(format!("{}={}", col_name, format_scalar(&val)));
    }
    println!("  {}", parts.join(", "));
}