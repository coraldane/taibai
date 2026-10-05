use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::OnceLock;

use crate::common::error::Result;
use crate::storage::segment::meta::{remove_sidecar_meta, sidecar_meta_path};
use crate::storage::segment::{SegmentReader, SegmentWriter};
use crate::types::batch::{RecordBatch, Schema};

/// RAII-owned temporary segment used by external operators. The normal
/// segment codec gives spill files checksummed metadata, compression and
/// schema preservation without inventing a second row serialization format.
pub struct SpillFile {
    path: PathBuf,
}

/// Incremental spill writer for operators that partition an input stream.
pub struct SpillWriter {
    writer: Option<SegmentWriter>,
    spill: Option<SpillFile>,
    rows_written: u64,
}

/// Process-wide memory arbiter shared by all blocking operators and queries.
/// Per-operator limits are still honored, while this additional ceiling stops
/// concurrent Sort/GROUP BY/Hash Join instances from each consuming the full
/// configured budget.
pub struct MemoryManager {
    limit: usize,
    used: AtomicUsize,
}

pub struct MemoryReservation {
    manager: &'static MemoryManager,
    bytes: usize,
}

/// Shared accounting for operators that cannot spill their retained state.
/// Every allocation phase checks its peak, not just the final result size.
pub(crate) struct OperatorMemory {
    reservation: MemoryReservation,
    limit: usize,
    name: &'static str,
}
impl OperatorMemory {
    pub(crate) fn new(name: &'static str, limit: usize) -> Self {
        Self {
            reservation: MemoryManager::reservation(),
            limit: limit.max(1),
            name,
        }
    }
    pub(crate) fn reserve(&mut self, bytes: usize) -> Result<()> {
        if bytes > self.limit || !self.reservation.try_resize(bytes) {
            return Err(crate::common::error::TaibaiError::ResourceBusy(format!(
                "{} exceeded the query memory budget ({} bytes, local limit {} bytes)",
                self.name, bytes, self.limit
            )));
        }
        super::operators::profile::record_current_peak_memory(bytes);
        Ok(())
    }
}
pub(crate) fn operator_memory_limit() -> usize {
    crate::common::config::query_limits()
        .query_memory_limit_mb
        .saturating_mul(1024 * 1024)
        .min(usize::MAX as u64) as usize
}
pub(crate) fn scalar_heap_bytes(value: &crate::types::scalar::ScalarValue) -> usize {
    use crate::types::scalar::ScalarValue;
    match value {
        ScalarValue::VarChar(Some(v)) => v.capacity(),
        ScalarValue::Blob(Some(v)) => v.capacity(),
        ScalarValue::Vector(Some(v)) => v.capacity() * 4,
        _ => 0,
    }
}

impl MemoryManager {
    fn global() -> &'static Self {
        static MANAGER: OnceLock<MemoryManager> = OnceLock::new();
        MANAGER.get_or_init(|| {
            let limit = crate::common::config::query_limits()
                .query_memory_limit_mb
                .saturating_mul(1024 * 1024)
                .min(usize::MAX as u64) as usize;
            MemoryManager {
                limit: limit.max(1),
                used: AtomicUsize::new(0),
            }
        })
    }

    pub fn reservation() -> MemoryReservation {
        MemoryReservation {
            manager: Self::global(),
            bytes: 0,
        }
    }
}

impl MemoryReservation {
    /// Resize this operator's charge. Returns false when the shared budget is
    /// under pressure; callers respond by spilling their current run.
    pub fn try_resize(&mut self, target: usize) -> bool {
        if target <= self.bytes {
            self.manager
                .used
                .fetch_sub(self.bytes - target, Ordering::AcqRel);
            self.bytes = target;
            return true;
        }
        let increase = target - self.bytes;
        let mut used = self.manager.used.load(Ordering::Acquire);
        loop {
            if used.saturating_add(increase) > self.manager.limit {
                return false;
            }
            match self.manager.used.compare_exchange_weak(
                used,
                used + increase,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.bytes = target;
                    return true;
                }
                Err(actual) => used = actual,
            }
        }
    }

    pub fn release(&mut self) {
        let _ = self.try_resize(0);
    }
}

impl Drop for MemoryReservation {
    fn drop(&mut self) {
        self.release();
    }
}

impl SpillFile {
    pub fn write_batches(
        schema: Schema,
        batches: &[RecordBatch],
        row_group_rows: usize,
    ) -> Result<Self> {
        Self::write_batches_from_iter(schema, batches.iter().cloned().map(Ok), row_group_rows)
    }

    /// Stream batches into a spill segment without retaining a second copy of
    /// the whole run in memory.
    pub fn write_batches_from_iter<I>(
        schema: Schema,
        batches: I,
        row_group_rows: usize,
    ) -> Result<Self>
    where
        I: IntoIterator<Item = Result<RecordBatch>>,
    {
        let dir = std::env::temp_dir().join("taibai-spill");
        std::fs::create_dir_all(&dir)?;
        cleanup_stale_spill_files(&dir);
        let path = dir.join(format!("spill-{}.dat", uuid::Uuid::new_v4()));
        let spill = Self { path };
        let mut rows_written = 0u64;
        let mut writer = SegmentWriter::create_with_bloom_columns(
            &spill.path,
            schema,
            row_group_rows.max(1),
            &[],
            crate::storage::segment_manager::bulk_compression_level(),
        )?;
        for batch in batches {
            let batch = batch?;
            rows_written = rows_written.saturating_add(batch.num_rows() as u64);
            writer.write_batch_parallel(&batch)?;
        }
        writer.seal()?;
        record_spill(&spill.path, rows_written);
        Ok(spill)
    }

    pub fn open_reader(&self) -> Result<SegmentReader> {
        SegmentReader::open(&self.path)
    }
}

impl SpillWriter {
    pub fn create(schema: Schema, row_group_rows: usize) -> Result<Self> {
        let dir = std::env::temp_dir().join("taibai-spill");
        std::fs::create_dir_all(&dir)?;
        cleanup_stale_spill_files(&dir);
        let path = dir.join(format!("spill-{}.dat", uuid::Uuid::new_v4()));
        let spill = SpillFile { path };
        let writer = SegmentWriter::create_with_bloom_columns(
            &spill.path,
            schema,
            row_group_rows.max(1),
            &[],
            crate::storage::segment_manager::bulk_compression_level(),
        )?;
        Ok(Self {
            writer: Some(writer),
            spill: Some(spill),
            rows_written: 0,
        })
    }

    pub fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.rows_written = self.rows_written.saturating_add(batch.num_rows() as u64);
        self.writer
            .as_mut()
            .expect("spill writer already finished")
            .write_batch_parallel(batch)
    }

    pub fn finish(mut self) -> Result<SpillFile> {
        self.writer
            .take()
            .expect("spill writer already finished")
            .seal()?;
        let spill = self.spill.take().expect("spill file missing");
        record_spill(&spill.path, self.rows_written);
        Ok(spill)
    }
}

fn record_spill(path: &std::path::Path, rows: u64) {
    let bytes = std::fs::metadata(path).map(|meta| meta.len()).unwrap_or(0);
    crate::sql::execution::operators::profile::record_current_spill(rows, bytes);
}

impl Drop for SpillWriter {
    fn drop(&mut self) {
        self.writer.take();
        self.spill.take();
    }
}

impl Drop for SpillFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
        let _ = remove_sidecar_meta(&self.path);
        let sidecar = sidecar_meta_path(&self.path);
        let _ = std::fs::remove_file(sidecar);
    }
}

#[cfg(test)]
#[path = "spill_test.rs"]
mod tests;

/// Best-effort removal of spill files left behind by crashed processes.
/// Only files older than a day are touched: the directory is shared by every
/// concurrent process/query, and their active spills must survive.
fn cleanup_stale_spill_files(dir: &std::path::Path) {
    const MAX_AGE: std::time::Duration = std::time::Duration::from_secs(24 * 60 * 60);
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let Ok(meta) = entry.metadata() else {
            continue;
        };
        let Ok(modified) = meta.modified() else {
            continue;
        };
        if meta.is_file()
            && entry.file_name().to_string_lossy().starts_with("spill-")
            && modified.elapsed().is_ok_and(|age| age > MAX_AGE)
        {
            let _ = std::fs::remove_file(entry.path());
        }
    }
}
