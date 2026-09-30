use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex, RwLock};

use serde::{Deserialize, Serialize};

use super::codec::{
    decode_unique_index_block_run, read_file_bytes, read_file_string, read_file_u32, read_file_u64,
    read_file_u8, UNIQUE_INDEX_MAGIC_V6,
};
use crate::common::error::{Result, TaibaiError};
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
    pub(super) base: Arc<UniqueIndexBase>,
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
    pub(super) fn new(base: UniqueIndexBase, delta: UniqueIndexDeltaState) -> Self {
        let progress = UniqueIndexProgress {
            flush_epoch: base.flush_epoch,
            segment_watermarks: base.segment_watermarks.clone(),
        };
        Self::with_progress(base, delta, progress)
    }

    pub(super) fn with_progress(
        base: UniqueIndexBase,
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

/// Legacy in-memory block layout kept for decoding files written before the
/// on-demand V6 format; new snapshots stream through [`UniqueIndexBaseWriter`].
#[derive(Debug, Clone)]
pub struct UniqueIndexBlock {
    pub first_key: Vec<u8>,
    pub start: usize,
    pub len: usize,
}

/// Directory entry of one base block in the on-demand format. `start`/`len`
/// index the global sorted entry sequence; `data_offset`/`data_len` locate
/// the block's serialized entry run inside the file so a lookup reads only
/// the bytes of blocks it touches.
pub(super) struct UniqueIndexBlockRef {
    pub(super) first_key: Vec<u8>,
    pub(super) start: usize,
    pub(super) len: usize,
    pub(super) data_offset: u64,
    pub(super) data_len: u32,
}

/// One decoded block: full encoded keys aligned with their row locations.
#[derive(Default)]
pub(super) struct UniqueIndexBlockEntries {
    pub(super) keys: Vec<Vec<u8>>,
    pub(super) locations: Vec<UniqueRowLocation>,
}

enum BlockBacking {
    /// Entry runs live in the file named by `path`; blocks are read on demand.
    File { path: PathBuf },
    /// Every block is already resident in the cache (legacy V5 files are
    /// fully decoded on load until the next compaction rewrites them V6).
    Memory,
}

/// LRU over decoded blocks, bounded by block count so a scan-heavy burst
/// cannot pin the whole index in memory.
struct BlockCache {
    capacity: usize,
    map: HashMap<usize, Arc<UniqueIndexBlockEntries>>,
    order: VecDeque<usize>,
}

impl BlockCache {
    fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            map: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    fn get(&mut self, idx: usize) -> Option<Arc<UniqueIndexBlockEntries>> {
        let block = self.map.get(&idx).cloned()?;
        self.touch(idx);
        Some(block)
    }

    fn put(&mut self, idx: usize, block: Arc<UniqueIndexBlockEntries>) {
        if self.map.insert(idx, block).is_none() {
            self.order.push_back(idx);
        }
        while self.map.len() > self.capacity {
            match self.order.pop_front() {
                Some(oldest) => {
                    self.map.remove(&oldest);
                }
                None => break,
            }
        }
    }

    fn touch(&mut self, idx: usize) {
        if let Some(pos) = self.order.iter().position(|candidate| *candidate == idx) {
            self.order.remove(pos);
        }
        self.order.push_back(idx);
    }
}

/// Read-only base snapshot of a unique index: sorted encoded keys with one
/// row location each, held in fixed-size blocks. The block directory (the
/// sparse index) stays resident while entry bytes load from disk on demand,
/// so resident memory is O(distinct lookups) instead of O(total keys).
pub(super) struct UniqueIndexBase {
    pub(super) db: String,
    pub(super) table: String,
    pub(super) index_name: String,
    pub(super) columns: Vec<String>,
    pub(super) entry_count: usize,
    pub(super) blocks: Vec<UniqueIndexBlockRef>,
    pub(super) flush_epoch: Option<u64>,
    pub(super) segment_watermarks: HashMap<String, u64>,
    /// Segment-path table shared by every block's location entries.
    paths: Vec<String>,
    backing: BlockBacking,
    cache: Mutex<BlockCache>,
}

const UNIQUE_INDEX_BLOCK_CACHE_BLOCKS: usize = 1024;

impl UniqueIndexBase {
    /// Open an on-demand (V6) base file: parse the header and footer block
    /// directory only; entry bytes load lazily through [`Self::block`].
    pub(super) fn open(path: &Path) -> Result<Self> {
        use std::io::{Read, Seek, SeekFrom};
        let mut file = std::fs::File::open(path)?;
        let file_len = file.metadata()?.len();
        if file_len < 16 {
            return Err(TaibaiError::Storage(
                "unique index base file too short".to_string(),
            ));
        }
        let mut magic = [0u8; 8];
        file.read_exact(&mut magic)?;
        if magic != *UNIQUE_INDEX_MAGIC_V6 {
            return Err(TaibaiError::Storage(
                "unsupported unique index version".to_string(),
            ));
        }
        let db = read_file_string(&mut file)?;
        let table = read_file_string(&mut file)?;
        let index_name = read_file_string(&mut file)?;
        let column_count = read_file_u32(&mut file)? as usize;
        let mut columns = Vec::with_capacity(column_count);
        for _ in 0..column_count {
            columns.push(read_file_string(&mut file)?);
        }
        let entries_start = file.stream_position()?;

        // The footer pointer sits in the last 8 bytes of the file.
        file.seek(SeekFrom::Start(file_len - 8))?;
        let footer_offset = read_file_u64(&mut file)?;
        if footer_offset < entries_start || footer_offset >= file_len - 8 {
            return Err(TaibaiError::Storage(
                "corrupt unique index footer offset".to_string(),
            ));
        }
        file.seek(SeekFrom::Start(footer_offset))?;
        let entry_count = read_file_u64(&mut file)? as usize;
        let block_size = read_file_u32(&mut file)? as usize;
        let block_count = read_file_u32(&mut file)? as usize;
        if block_size == 0 {
            return Err(TaibaiError::Storage(
                "corrupt unique index block size".to_string(),
            ));
        }
        let mut blocks = Vec::with_capacity(block_count);
        let mut expected_start = 0usize;
        for _ in 0..block_count {
            let first_key = read_file_bytes(&mut file)?;
            let start = read_file_u64(&mut file)? as usize;
            let len = read_file_u32(&mut file)? as usize;
            let data_offset = read_file_u64(&mut file)?;
            let data_len = read_file_u32(&mut file)?;
            if start != expected_start || len == 0 {
                return Err(TaibaiError::Storage(
                    "corrupt unique index block directory".to_string(),
                ));
            }
            expected_start = start + len;
            blocks.push(UniqueIndexBlockRef {
                first_key,
                start,
                len,
                data_offset,
                data_len,
            });
        }
        let path_count = read_file_u32(&mut file)? as usize;
        let mut paths = Vec::with_capacity(path_count);
        for _ in 0..path_count {
            paths.push(read_file_string(&mut file)?);
        }
        let epoch_flag = read_file_u8(&mut file)?;
        let epoch_value = read_file_u64(&mut file)?;
        let flush_epoch = (epoch_flag == 1).then_some(epoch_value);
        let watermark_count = read_file_u32(&mut file)? as usize;
        let mut segment_watermarks = HashMap::with_capacity(watermark_count);
        for _ in 0..watermark_count {
            let segment_path = read_file_string(&mut file)?;
            let row_count = read_file_u64(&mut file)?;
            segment_watermarks.insert(segment_path, row_count);
        }
        let total_entries: usize = blocks.iter().map(|block| block.len).sum();
        if total_entries != entry_count {
            return Err(TaibaiError::Storage(
                "corrupt unique index entry count".to_string(),
            ));
        }
        Ok(Self {
            db,
            table,
            index_name,
            columns,
            entry_count,
            blocks,
            flush_epoch,
            segment_watermarks,
            paths,
            backing: BlockBacking::File {
                path: path.to_path_buf(),
            },
            cache: Mutex::new(BlockCache::new(UNIQUE_INDEX_BLOCK_CACHE_BLOCKS)),
        })
    }

    /// Wrap an already-decoded legacy snapshot: blocks are pre-built in the
    /// cache so lookups never touch disk. The memory cost matches the old
    /// fully-resident design and goes away once a compaction rewrites the
    /// file in the on-demand format.
    pub(super) fn from_decoded(data: UniqueIndexData) -> Self {
        let mut cache = BlockCache::new(UNIQUE_INDEX_BLOCK_CACHE_BLOCKS.max(data.blocks.len()));
        for (block_idx, block) in data.blocks.iter().enumerate() {
            let end = (block.start + block.len).min(data.encoded_keys.len());
            let entries = UniqueIndexBlockEntries {
                keys: data.encoded_keys[block.start..end].to_vec(),
                locations: data.entries[block.start..end]
                    .iter()
                    .map(|entry| entry.location.clone())
                    .collect(),
            };
            cache.put(block_idx, Arc::new(entries));
        }
        Self {
            db: data.db,
            table: data.table,
            index_name: data.index_name,
            columns: data.columns,
            entry_count: data.encoded_keys.len(),
            blocks: data
                .blocks
                .iter()
                .map(|block| UniqueIndexBlockRef {
                    first_key: block.first_key.clone(),
                    start: block.start,
                    len: block.len,
                    data_offset: 0,
                    data_len: 0,
                })
                .collect(),
            flush_epoch: data.flush_epoch,
            segment_watermarks: data.segment_watermarks,
            paths: Vec::new(),
            backing: BlockBacking::Memory,
            cache: Mutex::new(cache),
        }
    }

    /// Decoded entries of one block, loading it from disk on a cache miss.
    pub(super) fn block(&self, idx: usize) -> Result<Arc<UniqueIndexBlockEntries>> {
        if let Some(block) = self.cache.lock().unwrap().get(idx) {
            return Ok(block);
        }
        let block = self.load_block(idx)?;
        let mut cache = self.cache.lock().unwrap();
        // A concurrent load may have filled the slot; prefer the winner.
        if let Some(existing) = cache.get(idx) {
            return Ok(existing);
        }
        cache.put(idx, Arc::clone(&block));
        Ok(block)
    }

    fn load_block(&self, idx: usize) -> Result<Arc<UniqueIndexBlockEntries>> {
        let block_ref = self
            .blocks
            .get(idx)
            .ok_or_else(|| TaibaiError::Storage("unique index block out of range".to_string()))?;
        let BlockBacking::File { path } = &self.backing else {
            return Err(TaibaiError::Storage(
                "unique index memory-backed block missing from cache".to_string(),
            ));
        };
        use std::io::{Read, Seek, SeekFrom};
        let mut file = std::fs::File::open(path)?;
        file.seek(SeekFrom::Start(block_ref.data_offset))?;
        let mut bytes = vec![0u8; block_ref.data_len as usize];
        file.read_exact(&mut bytes)?;
        Ok(Arc::new(decode_unique_index_block_run(
            &bytes,
            &block_ref.first_key,
            &self.paths,
        )?))
    }

    /// Row locations of every base entry whose encoded key equals `encoded`,
    /// walking forward across block boundaries while the run continues.
    /// Blocks are cut at fixed entry counts, so a run starts inside the last
    /// block whose first key is strictly below `encoded` (or at the head of
    /// the first block equal to it); one leading binary search plus the
    /// trailing equal-key blocks are all a lookup ever touches.
    pub(super) fn find_locations(&self, encoded: &[u8]) -> Result<Vec<UniqueRowLocation>> {
        let mut out = Vec::new();
        if self.blocks.is_empty() {
            return Ok(out);
        }
        // First block whose first key is >= encoded; the previous one may
        // still hold the run's head, so start there.
        let mut block_idx = self
            .blocks
            .partition_point(|block| block.first_key.as_slice() < encoded);
        if block_idx > 0 {
            block_idx -= 1;
        } else if self.blocks[0].first_key.as_slice() > encoded {
            // Every block starts strictly above the key.
            return Ok(out);
        }
        loop {
            let block = self.block(block_idx)?;
            let mut pos = block.keys.partition_point(|key| key.as_slice() < encoded);
            while pos < block.keys.len() && block.keys[pos].as_slice() == encoded {
                out.push(block.locations[pos].clone());
                pos += 1;
            }
            let next = block_idx + 1;
            if next < self.blocks.len() && self.blocks[next].first_key.as_slice() == encoded {
                block_idx = next;
            } else {
                return Ok(out);
            }
        }
    }

    #[cfg(test)]
    pub(super) fn cached_block_count(&self) -> usize {
        self.cache.lock().unwrap().map.len()
    }
}
