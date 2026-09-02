use std::collections::{HashMap, HashSet};
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex, RwLock};

use serde::{Deserialize, Serialize};

use crate::types::scalar::ScalarValue;
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UniqueIndexSnapshot {
    pub db: String,
    pub table: String,
    pub index_name: String,
    pub columns: Vec<String>,
    pub key_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct UniqueRowLocation {
    pub segment_path: String,
    pub row_id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UniqueIndexEntry {
    pub key: Vec<ScalarValue>,
    pub location: UniqueRowLocation,
}

#[derive(Clone)]
pub struct PreparedUniqueIndexEntry {
    pub encoded_key: Vec<u8>,
    pub entry: UniqueIndexEntry,
}

pub(super) struct UniqueIndexState {
    pub(super) base: Arc<UniqueIndexData>,
    pub(super) delta: RwLock<UniqueIndexDeltaState>,
    pub(super) progress: RwLock<UniqueIndexProgress>,
    pub(super) mutation_lock: Mutex<()>,
    pub(super) compaction_in_flight: AtomicBool,
}

#[derive(Default, Clone)]
pub(super) struct UniqueIndexProgress {
    pub(super) flush_epoch: Option<u64>,
    pub(super) segment_watermarks: HashMap<String, u64>,
}

#[derive(Default)]
pub(super) struct UniqueIndexDeltaState {
    pub(super) entry_count: u64,
    pub(super) key_locations: HashMap<Vec<u8>, Vec<UniqueRowLocation>>,
    pub(super) tombstones: HashSet<UniqueRowLocation>,
    /// Encoded keys whose entries were removed by deletes. Replays in record
    /// order during materialization so a later re-insert of the same key
    /// survives; also blocks placeholder resurrection on reload.
    pub(super) removed_keys: HashSet<Vec<u8>>,
}

impl UniqueIndexState {
    pub(super) fn new(base: UniqueIndexData, delta: UniqueIndexDeltaState) -> Self {
        let progress = UniqueIndexProgress {
            flush_epoch: base.flush_epoch,
            segment_watermarks: base.segment_watermarks.clone(),
        };
        Self::with_progress(base, delta, progress)
    }

    pub(super) fn with_progress(
        base: UniqueIndexData,
        delta: UniqueIndexDeltaState,
        progress: UniqueIndexProgress,
    ) -> Self {
        Self {
            base: Arc::new(base),
            delta: RwLock::new(delta),
            progress: RwLock::new(progress),
            mutation_lock: Mutex::new(()),
            compaction_in_flight: AtomicBool::new(false),
        }
    }

    pub(super) fn progress_epoch(&self) -> Option<u64> {
        self.progress.read().unwrap().flush_epoch
    }

    pub(super) fn progress_watermarks(&self) -> HashMap<String, u64> {
        self.progress.read().unwrap().segment_watermarks.clone()
    }

    pub(super) fn set_progress(
        &self,
        flush_epoch: Option<u64>,
        segment_watermarks: HashMap<String, u64>,
    ) {
        let mut progress = self.progress.write().unwrap();
        progress.flush_epoch = flush_epoch;
        progress.segment_watermarks = segment_watermarks;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UniqueIndexData {
    pub db: String,
    pub table: String,
    pub index_name: String,
    pub columns: Vec<String>,
    pub entries: Vec<UniqueIndexEntry>,
    /// Memtable-flush epoch stamped when this snapshot was built. A missing
    /// epoch is treated as stale and causes the first probe to rebuild it.
    #[serde(default)]
    pub flush_epoch: Option<u64>,
    /// Per-segment row-count watermark captured when this snapshot's entries
    /// were collected. Enables bounded incremental refresh after memtable
    /// flushes instead of a full segment rescan.
    #[serde(default)]
    pub segment_watermarks: HashMap<String, u64>,
    #[serde(skip)]
    pub tombstones: HashSet<UniqueRowLocation>,
    #[serde(skip)]
    pub encoded_keys: Vec<Vec<u8>>,
    #[serde(skip)]
    pub blocks: Vec<UniqueIndexBlock>,
}

#[derive(Debug, Clone)]
pub struct UniqueIndexBlock {
    pub first_key: Vec<u8>,
    pub start: usize,
    pub len: usize,
}
