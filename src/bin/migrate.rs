//! Segment format migration and verification tool for taibai.
//!
//! Verifies every segment under a data directory (magic, header version,
//! footer checksum, full parse) and, with `--write`, upgrades segments
//! written by an older supported binary to the current format version.
//!
//! Typical upgrade flow:
//! 1. stop the server (a clean shutdown seals every active writer);
//! 2. back up the data directory;
//! 3. `migrate --data <dir> [--db <db> [--table <table>]]` — dry run;
//! 4. `migrate --data <dir> --write` — apply;
//! 5. start the new binary.

use std::path::{Path, PathBuf};
use std::process;

use clap::Parser;
use taibai::storage::segment::migrate::{
    needs_upgrade, segment_header_version, upgrade_segment_file,
};
use taibai::storage::segment::SegmentReader;
use taibai::tools::segment;

#[derive(Parser, Debug)]
#[command(
    name = "migrate",
    about = "Verify and upgrade taibai segment files to the current format version",
    version
)]
struct Args {
    #[arg(long, default_value = "./data")]
    data: PathBuf,

    /// Only process this database (default: all databases).
    #[arg(long)]
    db: Option<String>,

    /// Only process this table (requires --db).
    #[arg(long)]
    table: Option<String>,

    /// Apply the migration (default is a dry run: verify and report only).
    #[arg(long)]
    write: bool,

    /// Verify data reads as well as structure (opens every segment).
    #[arg(long)]
    deep: bool,
}

fn main() {
    let args = Args::parse();
    if args.table.is_some() && args.db.is_none() {
        eprintln!("--table requires --db");
        process::exit(2);
    }

    let data_dir = args.data.as_path();
    if !data_dir.exists() {
        eprintln!("data directory does not exist: {}", data_dir.display());
        process::exit(2);
    }

    let targets = segment::collect_table_dirs(data_dir, args.db.as_deref(), args.table.as_deref());
    if targets.is_empty() {
        eprintln!("no tables found under {}", data_dir.display());
        process::exit(2);
    }

    let mut scanned = 0u64;
    let mut current = 0u64;
    let mut migrated = 0u64;
    let mut skipped = 0u64;
    let mut failed = 0u64;

    for (db, table, table_dir) in &targets {
        let segments = match segment::discover_segments(table_dir) {
            Ok(files) => files,
            Err(error) => {
                eprintln!("FAIL  {db}.{table}: cannot list segments: {error}");
                failed += 1;
                continue;
            }
        };
        for path in &segments {
            scanned += 1;
            let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("?");
            match verify_segment(path, args.deep) {
                Err(error) => {
                    failed += 1;
                    eprintln!("FAIL  {db}.{table}/{name}: {error}");
                    continue;
                }
                Ok(version) => {
                    let needs = match needs_upgrade(path) {
                        Ok(needs) => needs,
                        Err(error) => {
                            failed += 1;
                            eprintln!("FAIL  {db}.{table}/{name}: {error}");
                            continue;
                        }
                    };
                    if !needs {
                        current += 1;
                        continue;
                    }
                    if !args.write {
                        skipped += 1;
                        println!("NEEDS MIGRATION  v{version} -> current: {db}.{table}/{name}");
                        continue;
                    }
                    match upgrade_segment_file(path) {
                        Ok(Some(outcome)) => {
                            migrated += 1;
                            println!(
                                "MIGRATED  v{} -> v{}: {db}.{table}/{name}",
                                outcome.from_version, outcome.to_version
                            );
                        }
                        Ok(None) => {
                            current += 1;
                        }
                        Err(error) => {
                            failed += 1;
                            eprintln!("FAIL  {db}.{table}/{name}: {error}");
                        }
                    }
                }
            }
        }
    }

    let mode = if args.write { "write" } else { "dry-run" };
    println!(
        "\n{mode}: scanned {scanned}, current {current}, migrated {migrated}, \
         needing migration {skipped}, failed {failed}"
    );
    if failed > 0 {
        process::exit(1);
    }
    if !args.write && skipped > 0 {
        println!("re-run with --write to apply the migration");
    }
}

/// Full structural verification (header, sidecar/embedded footer, checksum,
/// complete parse); with `deep`, also opens the segment the way the server
/// would. Returns the header version on success.
fn verify_segment(path: &Path, deep: bool) -> taibai::common::error::Result<u32> {
    let version = segment_header_version(path)?;
    let info = segment::inspect_segment(path)?;
    if info.header_row_count != info.footer_row_count {
        return Err(taibai::common::error::TaibaiError::Storage(format!(
            "header row count {} != footer row count {}",
            info.header_row_count, info.footer_row_count
        )));
    }
    if deep {
        SegmentReader::open(path)?;
    }
    Ok(version)
}
