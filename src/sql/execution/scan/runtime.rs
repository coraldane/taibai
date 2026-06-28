use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::storage::core::memtable::MemTableManager;
use crate::storage::segment_manager::SegmentManager;

use crate::sql::execution::engine::Engine;

#[derive(Clone)]
pub struct ScanRuntime {
    data_dir: PathBuf,
    segment_mgr: Arc<SegmentManager>,
    memtable_manager: Arc<MemTableManager>,
}

impl ScanRuntime {
    pub fn from_engine(engine: &Engine) -> Self {
        Self {
            data_dir: engine.data_dir().to_path_buf(),
            segment_mgr: Arc::clone(engine.segment_mgr()),
            memtable_manager: Arc::clone(engine.memtable_manager()),
        }
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    pub fn segment_mgr(&self) -> &Arc<SegmentManager> {
        &self.segment_mgr
    }

    pub fn memtable_manager(&self) -> &Arc<MemTableManager> {
        &self.memtable_manager
    }
}
