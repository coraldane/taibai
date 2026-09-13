mod active_writer;
mod cluster_meta_cache;
mod config;
mod helpers;
mod reader_cache;
mod segment_meta_cache;
mod state;

use std::collections::{HashMap, HashSet};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use crate::common::error::{Result, TaibaiError};
use crate::storage::bulk_payload::{
    parse_direct_bulk_payload, DirectBulkColumn, DirectBulkRequest,
};
use crate::storage::partition::partition_function::compute_partition_key_from_scalar;
use crate::storage::segment::meta::{
    append_sidecar_footer, read_embedded_footer, read_embedded_segment_meta, remove_sidecar_meta,
    sidecar_meta_path, SegmentMetaSnapshot,
};
use crate::storage::segment::{
    encode_segment_header, store, SegmentReader, SegmentWriter, SEGMENT_HEADER_SIZE,
};
use crate::types::batch::{RecordBatch, Schema};

pub(crate) use config::bulk_compression_level;
pub use config::SegmentConfig;
pub use helpers::table_key;
pub(crate) use helpers::{direct_bulk_column_scalar_at, direct_bulk_request_to_batch};

use active_writer::ActiveSegmentWriter;
use cluster_meta_cache::ClusterMetaCache;
use config::{
    reader_cache_capacity, FLUSH_CHECK_INTERVAL_SECS, FLUSH_IDLE_THRESHOLD_SECS,
    SEGMENT_BUFWRITER_CAP,
};
use helpers::{
    discover_max_partition_seq, discover_max_seq, ensure_state_not_retired, filter_batch_by_mask,
    is_transient_footer_error, slice_batch_fast,
};
use reader_cache::ReaderCache;
use segment_meta_cache::SegmentMetaCache;
use state::TableSegmentState;

fn same_segment_path(a: &Path, b: &Path) -> bool {
    a == b || a.to_string_lossy().replace('\\', "/") == b.to_string_lossy().replace('\\', "/")
}

fn sanitize_component(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '.' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

fn read_flush_epoch_file(path: &Path) -> u64 {
    std::fs::read_to_string(path)
        .ok()
        .and_then(|content| content.trim().parse::<u64>().ok())
        .unwrap_or(0)
}

enum PreparedDirectColumns<'a> {
    Borrowed(Vec<DirectBulkColumn<'a>>),
    Owned(Vec<OwnedDirectBulkColumn<'a>>),
}

struct PreparedDirectBulkGroup<'a> {
    partition_key: String,
    row_count: usize,
    columns: PreparedDirectColumns<'a>,
    compressed_columns: Vec<(Vec<u8>, u64)>,
}

struct OwnedDirectBulkColumn<'a> {
    name: &'a str,
    data_type: crate::protocol::types::DataType,
    scale: i32,
    precision: i32,
    data: Vec<u8>,
    nulls: Vec<u8>,
    variable_ranges: Option<Vec<(usize, usize)>>,
}

pub struct SegmentManager {
    data_dir: PathBuf,
    tables: RwLock<HashMap<String, Arc<TableSegmentState>>>,
    reader_cache: Mutex<ReaderCache>,
    segment_meta_cache: Mutex<SegmentMetaCache>,
    cluster_meta_cache: Mutex<ClusterMetaCache>,
    /// Per-table count of successful memtable flushes. Unique-index snapshots stamp
    /// this epoch at build time; a later mismatch means segment rows landed after
    /// the snapshot was built, so the snapshot must be rebuilt before probing.
    flush_epochs: RwLock<HashMap<String, u64>>,
    flush_stop: Arc<AtomicBool>,
    flush_paused: Arc<AtomicBool>,
    next_txn_id: AtomicU64,
    config: SegmentConfig,
}

include!("segment_manager/manager_core.rs");
include!("segment_manager/buffered_write.rs");
include!("segment_manager/direct_bulk.rs");
include!("segment_manager/lifecycle.rs");

fn cleanup_incomplete_compaction_files(table_dir: &Path) {
    let Ok(entries) = std::fs::read_dir(table_dir) else {
        return;
    };

    for entry in entries.flatten() {
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if !is_incomplete_compaction_file(name) {
            continue;
        }
        match std::fs::remove_file(&path) {
            Ok(()) => tracing::warn!(
                path = %path.display(),
                "removed incomplete compact temporary file during startup"
            ),
            Err(err) => tracing::warn!(
                path = %path.display(),
                error = %err,
                "failed to remove incomplete compact temporary file during startup"
            ),
        }
    }
}

fn is_incomplete_compaction_file(name: &str) -> bool {
    name.ends_with(".compact_tmp") || name.ends_with(".meta.compact.tmp")
}

#[cfg(test)]
#[path = "lock_test.rs"]
mod lock_tests;
