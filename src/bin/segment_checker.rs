use std::path::PathBuf;
use std::process;

use clap::Parser;
use monax::tools::segment::{self, SegmentInspectInfo};

#[derive(Parser, Debug)]
#[command(
    name = "segment",
    about = "Inspect segment (.dat) files for monax/taibai columnar database",
    version
)]
struct Args {
    #[arg(long, default_value = "./data")]
    data: PathBuf,

    #[arg(long, default_value = "main")]
    db: String,

    #[arg(short, long)]
    table: String,

    #[arg(short = 'v', long)]
    verbose: bool,

    #[arg(long)]
    where_column: Option<String>,

    #[arg(long)]
    where_value: Option<String>,

    /// Print 32-byte file header (magic, version, created_at, flags).
    #[arg(long)]
    show_header: bool,

    /// Print per-column ZoneMap (min/max/null_count) for each row group.
    #[arg(long)]
    show_zonemaps: bool,
}

fn main() {
    let args = Args::parse();

    let table_dir = args.data.join(&args.db).join(&args.table);

    if !table_dir.exists() {
        eprintln!("error: table directory not found: {}", table_dir.display());
        process::exit(1);
    }

    println!(
        "scanning table: {} (database: {})",
        args.table, args.db
    );
    println!("directory: {}\n", table_dir.display());

    let files = match segment::discover_segments(&table_dir) {
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

    let has_where_filter = args.where_column.is_some() && args.where_value.is_some();

    let seg_count = files
        .iter()
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .map(|n| n.starts_with("seg_"))
                .unwrap_or(false)
        })
        .count();
    let part_count = files.len() - seg_count;
    let is_partitioned = part_count > 0;

    if is_partitioned {
        println!(
            "found {} segment files ({} regular + {} partitioned):\n",
            files.len(),
            seg_count,
            part_count
        );
    } else {
        println!("found {} segment files:\n", files.len());
    }

    let mut total_rows: u64 = 0;
    let mut total_size: u64 = 0;
    let mut total_compressed: u64 = 0;
    let mut total_uncompressed: u64 = 0;
    let mut total_row_groups: u32 = 0;

    for (i, file) in files.iter().enumerate() {
        if has_where_filter {
            let filter_col = args.where_column.as_ref().unwrap();
            let filter_val = args.where_value.as_ref().unwrap();
            match inspect_with_filter(file, filter_col, filter_val) {
                Ok((info, match_count)) => {
                    total_rows += info.row_count;
                    total_size += info.file_size;
                    total_compressed += info.compressed_size;
                    total_uncompressed += info.uncompressed_size;
                    total_row_groups += info.row_group_count;

                    let name = file.file_name().and_then(|n| n.to_str()).unwrap_or("?");
                    println!("[{}] file: {}", i + 1, name);
                    if !info.partition_key.is_empty() {
                        println!(
                            "    partition: {}, sequence: {:04}",
                            info.partition_key, info.sequence
                        );
                    }
                    println!("    total rows: {}", info.row_count);
                    println!("    matching rows: {}\n", match_count);
                }
                Err(e) => {
                    println!(
                        "[{}] file: {}",
                        i + 1,
                        file.file_name().and_then(|n| n.to_str()).unwrap_or("?")
                    );
                    println!("    error: {}\n", e);
                }
            }
        } else {
            match segment::inspect_segment(file) {
                Ok(info) => {
                    total_rows += info.row_count;
                    total_size += info.file_size;
                    total_compressed += info.compressed_size;
                    total_uncompressed += info.uncompressed_size;
                    total_row_groups += info.row_group_count;

                    print_info(i + 1, &info, args.verbose, args.show_header, args.show_zonemaps);

                    if args.verbose {
                        let schema = &info.schema;
                        println!("    schema:");
                        for col in &schema.columns {
                            if col.data_type == monax::protocol::types::DataType::Decimal
                                && col.precision > 0
                            {
                                println!(
                                    "      - {} ({}({},{}))",
                                    col.name,
                                    col.data_type.display_name(),
                                    col.precision,
                                    col.scale
                                );
                            } else {
                                println!(
                                    "      - {} ({})",
                                    col.name,
                                    col.data_type.display_name()
                                );
                            }
                        }
                        println!();
                    }
                }
                Err(e) => {
                    println!(
                        "[{}] file: {}",
                        i + 1,
                        file.file_name().and_then(|n| n.to_str()).unwrap_or("?")
                    );
                    println!("    error: {}\n", e);
                }
            }
        }
    }

    println!("=================================");
    println!("summary:");
    println!("  total segments: {}", files.len());
    println!("  total rows: {}", total_rows);
    println!("  total row groups: {}", total_row_groups);
    if total_row_groups > 0 {
        println!(
            "  avg row group size: {} rows",
            total_rows / total_row_groups as u64
        );
    }
    println!("  total file size: {}", segment::format_size(total_size));
    println!("  total compressed: {}", segment::format_size(total_compressed));
    println!(
        "  total uncompressed: {}",
        segment::format_size(total_uncompressed)
    );
    if total_uncompressed > 0 {
        let ratio = total_compressed as f64 / total_uncompressed as f64 * 100.0;
        println!(
            "  overall compression ratio: {:.1}% (saved {:.1}%)",
            ratio,
            100.0 - ratio
        );
    }
    if !files.is_empty() {
        println!(
            "  avg rows per segment: {}",
            total_rows / files.len() as u64
        );
    }
}

fn print_info(idx: usize, info: &SegmentInspectInfo, verbose: bool, show_header: bool, show_zonemaps: bool) {
    let name = info
        .path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("?");
    println!("[{}] file: {}", idx, name);
    if !info.partition_key.is_empty() {
        println!(
            "    partition: {}, sequence: {:04}",
            info.partition_key, info.sequence
        );
    }
    if show_header {
        println!("    file header (32 bytes):");
        println!(
            "      magic: 0x{:08X} (\"{}\"), version: {}",
            info.header_magic,
            magic_as_ascii(info.header_magic),
            info.header_version
        );
        println!(
            "      created_at: {} ({}), flags: 0x{:016X}",
            info.header_created_at,
            segment::format_timestamp(info.header_created_at),
            info.header_flags
        );
    }
    println!("    version: {}, generation: {}", info.version, info.generation);
    if info.deleted_row_count > 0 || info.delete_bitmap_len > 0 {
        println!(
            "    deleted rows: {} (bitmap bytes: {})",
            info.deleted_row_count, info.delete_bitmap_len
        );
    }
    println!(
        "    row count: {} (header: {}, footer actual: {})",
        info.row_count, info.header_row_count, info.footer_row_count
    );
    if info.header_row_count != info.footer_row_count {
        println!("    warning: header/footer row count mismatch!");
    }
    println!(
        "    row groups: {}, avg row group size: {} rows",
        info.row_group_count, info.avg_row_group_size
    );
    println!("    file size: {}", segment::format_size(info.file_size));
    println!(
        "    compressed size: {}",
        segment::format_size(info.compressed_size)
    );
    println!(
        "    uncompressed size: {}",
        segment::format_size(info.uncompressed_size)
    );
    if info.uncompressed_size > 0 {
        let ratio = info.compressed_size as f64 / info.uncompressed_size as f64 * 100.0;
        println!(
            "    compression ratio: {:.1}% (saved {:.1}%)",
            ratio,
            100.0 - ratio
        );
    }
    if info.created_at > 0 {
        println!(
            "    created: {}",
            segment::format_timestamp(info.created_at)
        );
    } else if let Some(t) = info.file_created {
        println!(
            "    created: {} (from file system)",
            segment::format_system_time(Some(t))
        );
    } else {
        println!("    created: N/A");
    }
    if info.modified_at > 0 {
        println!(
            "    modified: {}",
            segment::format_timestamp(info.modified_at)
        );
    } else if let Some(t) = info.file_modified {
        println!(
            "    modified: {} (from file system)",
            segment::format_system_time(Some(t))
        );
    } else {
        println!("    modified: N/A");
    }

    if verbose && !info.row_groups.is_empty() {
        println!("    row groups detail:");
        for (j, rg) in info.row_groups.iter().enumerate() {
            let avg_size = if rg.row_count > 0 {
                rg.length / rg.row_count
            } else {
                0
            };
            println!(
                "      [{}] offset={}, rows={}, size={}, avg_row_size={}",
                j,
                rg.offset,
                rg.row_count,
                segment::format_size(rg.length),
                segment::format_size(avg_size)
            );
            if show_zonemaps {
                for (c, zm) in rg.zone_maps.iter().enumerate() {
                    let col_name = info
                        .schema
                        .columns
                        .get(c)
                        .map(|cd| cd.name.as_str())
                        .unwrap_or("?");
                    println!(
                        "          col[{}] {:<16} min={:<24} max={:<24} nulls={}/{}",
                        c,
                        col_name,
                        format!("{:?}", zm.min_value),
                        format!("{:?}", zm.max_value),
                        zm.null_count,
                        zm.row_count
                    );
                }
            }
        }
    } else if show_zonemaps && !info.row_groups.is_empty() {
        println!("    zone maps:");
        for (j, rg) in info.row_groups.iter().enumerate() {
            for (c, zm) in rg.zone_maps.iter().enumerate() {
                let col_name = info
                    .schema
                    .columns
                    .get(c)
                    .map(|cd| cd.name.as_str())
                    .unwrap_or("?");
                println!(
                    "      rg[{}] col[{}] {:<16} min={:<24} max={:<24} nulls={}/{}",
                    j,
                    c,
                    col_name,
                    format!("{:?}", zm.min_value),
                    format!("{:?}", zm.max_value),
                    zm.null_count,
                    zm.row_count
                );
            }
        }
    }
    println!();
}

fn magic_as_ascii(m: u32) -> String {
    let b = m.to_le_bytes();
    b.iter()
        .map(|c| if c.is_ascii_graphic() { *c as char } else { '?' })
        .collect()
}

fn inspect_with_filter(
    path: &std::path::Path,
    filter_column: &str,
    filter_value: &str,
) -> monax::common::error::Result<(SegmentInspectInfo, u64)> {
    use monax::storage::segment::SegmentReader;

    let info = segment::inspect_segment(path)?;

    let reader = SegmentReader::open(path)?;
    let schema = reader.schema();

    let col_idx = match schema.index_of(filter_column) {
        Some(idx) => idx,
        None => {
            return Ok((info, 0));
        }
    };

    let col_def = schema.column(col_idx).unwrap();
    let target_val = parse_scalar_value(filter_value, col_def.data_type);

    let mut match_count: u64 = 0;
    let num_rgs = reader.num_row_groups();

    for rg_idx in 0..num_rgs {
        let batch = reader.read_row_group(rg_idx)?;
        let col = batch.column(col_idx).unwrap();
        for row in 0..batch.num_rows() {
            let val = col.scalar(row);
            if scalar_matches(&val, &target_val) {
                match_count += 1;
            }
        }
    }

    Ok((info, match_count))
}

fn parse_scalar_value(s: &str, dt: monax::protocol::types::DataType) -> monax::types::scalar::ScalarValue {
    use monax::types::scalar::ScalarValue;

    match dt {
        monax::protocol::types::DataType::Bool => {
            ScalarValue::Bool(Some(s.eq_ignore_ascii_case("true") || s == "1"))
        }
        monax::protocol::types::DataType::Int8 => {
            ScalarValue::Int8(Some(s.parse::<i8>().unwrap_or(0)))
        }
        monax::protocol::types::DataType::Int16 => {
            ScalarValue::Int16(Some(s.parse::<i16>().unwrap_or(0)))
        }
        monax::protocol::types::DataType::Int32 => {
            ScalarValue::Int32(Some(s.parse::<i32>().unwrap_or(0)))
        }
        monax::protocol::types::DataType::Int64 => {
            ScalarValue::Int64(Some(s.parse::<i64>().unwrap_or(0)))
        }
        monax::protocol::types::DataType::UInt8 => {
            ScalarValue::UInt8(Some(s.parse::<u8>().unwrap_or(0)))
        }
        monax::protocol::types::DataType::UInt16 => {
            ScalarValue::UInt16(Some(s.parse::<u16>().unwrap_or(0)))
        }
        monax::protocol::types::DataType::UInt32 => {
            ScalarValue::UInt32(Some(s.parse::<u32>().unwrap_or(0)))
        }
        monax::protocol::types::DataType::UInt64 => {
            ScalarValue::UInt64(Some(s.parse::<u64>().unwrap_or(0)))
        }
        monax::protocol::types::DataType::Float32 => {
            ScalarValue::Float32(Some(s.parse::<f32>().unwrap_or(0.0)))
        }
        monax::protocol::types::DataType::Float64 => {
            ScalarValue::Float64(Some(s.parse::<f64>().unwrap_or(0.0)))
        }
        monax::protocol::types::DataType::VarChar => {
            ScalarValue::VarChar(Some(s.to_string()))
        }
        monax::protocol::types::DataType::Timestamp => {
            ScalarValue::Timestamp(Some(s.parse::<i64>().unwrap_or(0)))
        }
        monax::protocol::types::DataType::Date => {
            ScalarValue::Date(Some(s.parse::<i32>().unwrap_or(0)))
        }
        monax::protocol::types::DataType::Decimal => {
            ScalarValue::Decimal(Some(s.parse::<i64>().unwrap_or(0)))
        }
        _ => ScalarValue::VarChar(Some(s.to_string())),
    }
}

fn scalar_matches(
    actual: &monax::types::scalar::ScalarValue,
    expected: &monax::types::scalar::ScalarValue,
) -> bool {
    use monax::types::scalar::ScalarValue;

    match (actual, expected) {
        (ScalarValue::Bool(Some(a)), ScalarValue::Bool(Some(b))) => a == b,
        (ScalarValue::Int8(Some(a)), ScalarValue::Int8(Some(b))) => a == b,
        (ScalarValue::Int16(Some(a)), ScalarValue::Int16(Some(b))) => a == b,
        (ScalarValue::Int32(Some(a)), ScalarValue::Int32(Some(b))) => a == b,
        (ScalarValue::Int64(Some(a)), ScalarValue::Int64(Some(b))) => a == b,
        (ScalarValue::UInt8(Some(a)), ScalarValue::UInt8(Some(b))) => a == b,
        (ScalarValue::UInt16(Some(a)), ScalarValue::UInt16(Some(b))) => a == b,
        (ScalarValue::UInt32(Some(a)), ScalarValue::UInt32(Some(b))) => a == b,
        (ScalarValue::UInt64(Some(a)), ScalarValue::UInt64(Some(b))) => a == b,
        (ScalarValue::Float32(Some(a)), ScalarValue::Float32(Some(b))) => (a - b).abs() < f32::EPSILON,
        (ScalarValue::Float64(Some(a)), ScalarValue::Float64(Some(b))) => (a - b).abs() < f64::EPSILON,
        (ScalarValue::VarChar(Some(a)), ScalarValue::VarChar(Some(b))) => a == b,
        (ScalarValue::Timestamp(Some(a)), ScalarValue::Timestamp(Some(b))) => a == b,
        (ScalarValue::Date(Some(a)), ScalarValue::Date(Some(b))) => a == b,
        (ScalarValue::Decimal(Some(a)), ScalarValue::Decimal(Some(b))) => a == b,
        _ => false,
    }
}
