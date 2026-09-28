//! Offline backup tool for taibai data directories.
//!
//! Copies `meta/` (catalogs, auth, baselines) and every segment `.dat` file
//! of the selected databases/tables into a destination directory, writes a
//! JSON manifest, and optionally verifies every copied segment by opening it
//! the way the server would.
//!
//! Run against a STOPPED server: a clean shutdown seals all active writers
//! and removes sidecar/journal files, so the directory is a consistent
//! snapshot. The tool refuses obvious dirt (`*.meta` sidecars, `*.del`
//! journals, `*.tmp` temporaries) with a warning instead of copying them.
//!
//! Restore = copy the backup directory back (or point `storage.data_dir` at
//! it). To roll forward to the latest committed state, replay archived tail
//! logs (`<data>/wal/<db>/<table>/memtable_tail/`) from a `tail_archive`-enabled
//! primary - the replication/PITR phase builds on this.

use std::fs;
use std::path::{Path, PathBuf};
use std::process;

use clap::Parser;
use taibai::storage::segment::SegmentReader;
use taibai::tools::segment;

#[derive(Parser, Debug)]
#[command(
    name = "backup",
    about = "Create a consistent offline backup of a taibai data directory",
    version
)]
struct Args {
    /// Source data directory (server must be stopped).
    #[arg(long, default_value = "./data")]
    data: PathBuf,

    /// Destination directory (created; must not exist or must be empty).
    #[arg(long)]
    dest: PathBuf,

    /// Only back up this database (default: all).
    #[arg(long)]
    db: Option<String>,

    /// Only back up this table (requires --db).
    #[arg(long)]
    table: Option<String>,

    /// Open every copied segment to verify it parses.
    #[arg(long)]
    verify: bool,
}

fn main() {
    let args = Args::parse();
    if args.table.is_some() && args.db.is_none() {
        eprintln!("--table requires --db");
        process::exit(2);
    }
    if !args.data.exists() {
        eprintln!("data directory does not exist: {}", args.data.display());
        process::exit(2);
    }
    if args.dest.exists() {
        let empty = fs::read_dir(&args.dest)
            .map(|entries| entries.count() == 0)
            .unwrap_or(false);
        if !empty {
            eprintln!(
                "destination already exists and is not empty: {}",
                args.dest.display()
            );
            process::exit(2);
        }
    }

    let started = std::time::SystemTime::now();
    let mut manifest_files: Vec<ManifestEntry> = Vec::new();
    let mut warnings = 0u64;
    let mut failures = 0u64;

    // 1. meta/ (catalogs, auth, baselines) - required for a usable backup.
    let meta_src = args.data.join("meta");
    if meta_src.exists() {
        match copy_tree(&meta_src, &args.dest.join("meta"), &mut manifest_files) {
            Ok(count) => println!("meta: {count} files copied"),
            Err(error) => {
                eprintln!("FAIL copying meta/: {error}");
                failures += 1;
            }
        }
    } else {
        eprintln!("WARN no meta/ directory found; backup has no catalog");
        warnings += 1;
    }

    // 2. table data segments.
    for (db, table, table_dir) in collect_table_dirs(&args.data, args.db.as_deref(), args.table.as_deref()) {
        let segments = match segment::discover_segments(&table_dir) {
            Ok(files) => files,
            Err(error) => {
                eprintln!("FAIL {db}.{table}: cannot list segments: {error}");
                failures += 1;
                continue;
            }
        };
        let dest_table = args.dest.join(&db).join(&table);
        let mut copied = 0u64;
        for path in &segments {
            match copy_segment(path, &dest_table, path.file_name().expect("file name").to_os_string(), args.verify) {
                Ok(bytes) => {
                    copied += 1;
                    manifest_files.push(ManifestEntry {
                        path: format!("{db}/{table}/{}", file_name(path)),
                        size: bytes,
                    });
                }
                Err(error) => {
                    eprintln!("FAIL {db}.{table}/{}: {error}", file_name(path));
                    failures += 1;
                }
            }
        }
        // Refuse obvious dirt: a stopped server leaves none of these.
        for entry in fs::read_dir(&table_dir).into_iter().flatten().flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            if name.ends_with(".meta") || name.ends_with(".del") || name.ends_with(".tmp") {
                eprintln!(
                    "WARN {db}.{table}/{name}: active-segment artifact present; \
                     was the server shut down cleanly? Skipping it"
                );
                warnings += 1;
            }
        }
        // Archived tail logs are part of a backup when tail_archive is on.
        let archive_dir = table_dir
            .parent()
            .and_then(|db_dir| db_dir.parent())
            .map(|data_dir| data_dir.join("wal").join(&db).join(&table).join("memtable_tail"));
        if let Some(archive_dir) = archive_dir.filter(|dir| dir.exists()) {
            let dest_archive = args.dest.join("wal").join(&db).join(&table).join("memtable_tail");
            match copy_tree(&archive_dir, &dest_archive, &mut manifest_files) {
                Ok(count) if count > 0 => println!("{db}.{table}: archived {count} tail log files"),
                Ok(_) => {}
                Err(error) => {
                    eprintln!("FAIL {db}.{table}: copying archived tails: {error}");
                    failures += 1;
                }
            }
        }
        println!("{db}.{table}: {copied} segments copied");
    }

    // 3. manifest.
    let manifest = Manifest {
        created_at_unix: started
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0),
        source: args.data.display().to_string(),
        files: manifest_files,
    };
    let manifest_path = args.dest.join("backup_manifest.json");
    if let Err(error) = fs::write(
        &manifest_path,
        serde_json::to_vec_pretty(&manifest).expect("manifest serializes"),
    ) {
        eprintln!("FAIL writing manifest: {error}");
        failures += 1;
    }

    println!(
        "\nbackup complete: {} files, {warnings} warnings, {failures} failures -> {}",
        manifest.files.len() + 1,
        args.dest.display()
    );
    if failures > 0 {
        process::exit(1);
    }
}

fn file_name(path: &Path) -> String {
    path.file_name()
        .map(|name| name.to_string_lossy().to_string())
        .unwrap_or_default()
}

fn collect_table_dirs(
    data_dir: &Path,
    only_db: Option<&str>,
    only_table: Option<&str>,
) -> Vec<(String, String, PathBuf)> {
    let mut out = Vec::new();
    let Ok(dbs) = fs::read_dir(data_dir) else {
        return out;
    };
    for db_entry in dbs.flatten() {
        if !db_entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
            continue;
        }
        let db_name = db_entry.file_name().to_string_lossy().to_string();
        if db_name.starts_with('.') || db_name == "meta" || db_name == "wal" {
            continue;
        }
        if let Some(only_db) = only_db {
            if db_name != only_db {
                continue;
            }
        }
        let Ok(tables) = fs::read_dir(db_entry.path()) else {
            continue;
        };
        for table_entry in tables.flatten() {
            if !table_entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                continue;
            }
            let table_name = table_entry.file_name().to_string_lossy().to_string();
            if table_name.starts_with(".taibai-") {
                continue;
            }
            if let Some(only_table) = only_table {
                if table_name != only_table {
                    continue;
                }
            }
            out.push((db_name.clone(), table_name.clone(), table_entry.path()));
        }
    }
    out.sort();
    out
}

fn copy_tree(
    src: &Path,
    dest: &Path,
    manifest: &mut Vec<ManifestEntry>,
) -> Result<usize, std::io::Error> {
    let mut count = 0usize;
    fs::create_dir_all(dest)?;
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            let name = file_name(&path);
            count += copy_tree(&path, &dest.join(name), manifest)?;
        } else {
            let name = file_name(&path);
            let target = dest.join(&name);
            fs::copy(&path, &target)?;
            let size = entry.metadata().map(|m| m.len()).unwrap_or(0);
            let relative = target
                .strip_prefix(&dest)
                .map(|p| p.display().to_string())
                .unwrap_or_else(|_| name);
            manifest.push(ManifestEntry {
                path: relative,
                size,
            });
            count += 1;
        }
    }
    Ok(count)
}

fn copy_segment(src: &Path, dest_dir: &Path, name: std::ffi::OsString, verify: bool) -> Result<u64, String> {
    fs::create_dir_all(dest_dir).map_err(|e| e.to_string())?;
    let dest = dest_dir.join(&name);
    let bytes = fs::copy(src, &dest).map_err(|e| e.to_string())?;
    if verify {
        SegmentReader::open(&dest).map_err(|e| e.to_string())?;
    }
    Ok(bytes)
}

#[derive(serde::Serialize)]
struct Manifest {
    created_at_unix: u64,
    source: String,
    files: Vec<ManifestEntry>,
}

#[derive(serde::Serialize)]
struct ManifestEntry {
    path: String,
    size: u64,
}
