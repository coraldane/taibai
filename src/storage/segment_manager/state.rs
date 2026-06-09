use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32};
use std::sync::Mutex;

use crate::storage::core::wal::WalWriter;
use crate::storage::segment_manager::active_writer::ActiveSegmentWriter;

pub(super) struct TableSegmentState {
    pub(super) seq: AtomicU32,
    pub(super) active_segments: Mutex<HashMap<String, ActiveSegmentWriter>>,
    pub(super) wal_writer: Mutex<Option<WalWriter>>,
    pub(super) mutation_lock: Mutex<()>,
    pub(super) retired: AtomicBool,
}

impl TableSegmentState {
    pub(super) fn new(initial_seq: u32) -> Self {
        Self {
            seq: AtomicU32::new(initial_seq),
            active_segments: Mutex::new(HashMap::new()),
            wal_writer: Mutex::new(None),
            mutation_lock: Mutex::new(()),
            retired: AtomicBool::new(false),
        }
    }
}
