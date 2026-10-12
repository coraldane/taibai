mod active_writer;
mod cluster_meta_cache;
mod config;
pub(crate) mod helpers;
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
pub(crate) use config::resolve_compression_level;
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
    discover_max_partition_seq, discover_max_seq, ensure_state_not_retired,
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

struct PreparedDirectBulkGroup<'a> {
    partition_key: String,
    row_count: usize,
    columns: Vec<DirectBulkColumn<'a>>,
    compressed_columns: Vec<(Vec<u8>, u64)>,
}

pub struct SegmentManager {
    data_dir: PathBuf,
    tables: RwLock<HashMap<String, Arc<TableSegmentState>>>,
    reader_cache: Mutex<ReaderCache>,
    segment_meta_cache: Mutex<SegmentMetaCache>,
    cluster_meta_cache: Mutex<ClusterMetaCache>,
    /// Readers for active (mutable) segments keyed by path. A mutable reader
    /// only ever exposes the *published* footer state, so it stays valid while
    /// the writer has nothing pending; `invalidate_reader` (every visibility
    /// change and seal) evicts it. Caching them keeps read-heavy tables from
    /// re-opening and re-parsing the segment footer on every query.
    mutable_reader_cache: Mutex<HashMap<PathBuf, std::sync::Arc<SegmentReader>>>,
    /// Per-table count of successful memtable flushes. Unique-index snapshots stamp
    /// this epoch at build time; a later mismatch means segment rows landed after
    /// the snapshot was built, so the snapshot must be rebuilt before probing.
    flush_epochs: RwLock<HashMap<String, u64>>,
    flush_stop: Arc<AtomicBool>,
    flush_paused: Arc<AtomicBool>,
    next_txn_id: AtomicU64,
    config: SegmentConfig,
    /// Hub for deferred-write change notifications. Set once at engine
    /// startup after construction; `None` (unset) keeps the write path free
    /// of pub/sub work.
    pubsub: std::sync::OnceLock<std::sync::Arc<crate::pubsub::PubSubHub>>,
    /// Deferred-DML registry. Set once at engine startup; scan-side reader
    /// opens consult it to overlay a transaction's uncommitted tombstones,
    /// and compaction/TTL consult it to leave pinned segments alone.
    txn_mutations: std::sync::OnceLock<
        std::sync::Arc<crate::storage::core::txn_mutations::TxnMutationRegistry>,
    >,
    /// Replication log on primaries: flush/seal emission sites enqueue
    /// `SegmentUpdate` events through it. Set once at engine startup.
    rep_log: std::sync::OnceLock<std::sync::Arc<crate::storage::replication::RepLog>>,
}

mod buffered_write;
mod direct_bulk;
mod lifecycle;
mod manager_core;

// Glob re-exports keep every `segment_manager::…` path (and the test-support
// module) that the flat include! layout used to expose; visibility per item
// is preserved (min of pub and the item's own).
use direct_bulk::*;
pub(crate) use manager_core::*;

fn cleanup_incomplete_compaction_files(table_dir: &Path) {
    let Ok(entries) = std::fs::read_dir(table_dir) else {
        return;
    };

    for entry in entries.flatten() {
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        // A delete-footer journal whose data file no longer exists (the
        // segment was replaced by compaction before the journal was cleaned
        // up) is garbage; a live one must stay for `discover_segments` to
        // repair from.
        if let Some(stem) = name.strip_suffix(".del") {
            if !table_dir.join(stem).exists() {
                let _ = std::fs::remove_file(&path);
            }
            continue;
        }
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
    name.ends_with(".compact_tmp")
        || name.ends_with(".meta.compact.tmp")
        || name.ends_with(".update_tmp")
        || name.ends_with(".epoch.tmp")
        // Torn delete-footer / segment-replacement journal writes.
        || name.ends_with(".del.tmp")
        || name.ends_with(".replace.tmp")
        || name.ends_with(".retire.tmp")
        || (name.starts_with(".tail_flush_") && name.ends_with(".tmp"))
        // Clustered-compaction external merge sort runs (crash leftovers).
        || name.contains(".merge.tmp.")
}

#[cfg(test)]
#[path = "lock_test.rs"]
mod lock_tests;

#[cfg(test)]
#[path = "partition_lock_test.rs"]
mod partition_lock_tests;

#[cfg(test)]
#[path = "compression_level_test.rs"]
mod compression_level_tests;

#[cfg(test)]
#[path = "segment_manager/seal_outside_lock_test.rs"]
mod seal_outside_lock_tests;

#[cfg(test)]
#[path = "segment_manager/seq_allocation_test.rs"]
mod seq_allocation_tests;

#[cfg(test)]
#[path = "segment_manager/mutation_lock_cancel_test.rs"]
mod mutation_lock_cancel_tests;
