//! Bounded external construction of sorted location-index bases.
use super::{codec::*, state_ops::*, types::*, IndexManager};
use crate::common::error::{Result, TaibaiError};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

type Entry = (Vec<u8>, UniqueRowLocation);
struct Runs {
    dir: PathBuf,
    next: usize,
    levels: Vec<Option<PathBuf>>,
}
impl Drop for Runs {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}
impl Runs {
    fn path(&mut self) -> PathBuf {
        let path = self.dir.join(format!("{}.run", self.next));
        self.next += 1;
        path
    }
    fn add(&mut self, mut path: PathBuf) -> Result<()> {
        let mut level = 0;
        loop {
            if level == self.levels.len() {
                self.levels.push(None);
            }
            if let Some(previous) = self.levels[level].take() {
                let output = self.path();
                merge(&previous, &path, &output)?;
                std::fs::remove_file(previous)?;
                std::fs::remove_file(path)?;
                path = output;
                level += 1;
            } else {
                self.levels[level] = Some(path);
                return Ok(());
            }
        }
    }
    fn finish(&mut self) -> Result<Option<PathBuf>> {
        let mut final_run: Option<PathBuf> = None;
        for path in std::mem::take(&mut self.levels).into_iter().flatten() {
            final_run = Some(match final_run {
                Some(old) => {
                    let output = self.path();
                    merge(&old, &path, &output)?;
                    std::fs::remove_file(old)?;
                    std::fs::remove_file(path)?;
                    output
                }
                None => path,
            });
        }
        Ok(final_run)
    }
}
fn write_entry(writer: &mut impl Write, entry: &Entry) -> Result<()> {
    writer.write_all(&(entry.0.len() as u32).to_le_bytes())?;
    writer.write_all(&entry.0)?;
    writer.write_all(&(entry.1.segment_path.len() as u32).to_le_bytes())?;
    writer.write_all(entry.1.segment_path.as_bytes())?;
    writer.write_all(&entry.1.row_id.to_le_bytes())?;
    Ok(())
}
fn read_entry(reader: &mut impl Read) -> Result<Option<Entry>> {
    let mut length = [0u8; 4];
    if reader.read(&mut length[..1])? == 0 {
        return Ok(None);
    }
    reader.read_exact(&mut length[1..])?;
    let mut key = vec![0; u32::from_le_bytes(length) as usize];
    reader.read_exact(&mut key)?;
    reader.read_exact(&mut length)?;
    let mut path = vec![0; u32::from_le_bytes(length) as usize];
    reader.read_exact(&mut path)?;
    let mut row = [0; 8];
    reader.read_exact(&mut row)?;
    Ok(Some((
        key,
        UniqueRowLocation {
            segment_path: String::from_utf8(path)
                .map_err(|e| TaibaiError::Storage(e.to_string()))?,
            row_id: u64::from_le_bytes(row),
        },
    )))
}
fn merge(left: &Path, right: &Path, output: &Path) -> Result<()> {
    let mut left = BufReader::new(std::fs::File::open(left)?);
    let mut right = BufReader::new(std::fs::File::open(right)?);
    let mut writer = BufWriter::new(std::fs::File::create(output)?);
    let mut a = read_entry(&mut left)?;
    let mut b = read_entry(&mut right)?;
    let mut count = 0usize;
    while a.is_some() || b.is_some() {
        if count % 1024 == 0 {
            crate::common::query_cancel::check()?;
        }
        count += 1;
        let take_left = match (&a, &b) {
            (Some(a), Some(b)) => a.0 <= b.0,
            (Some(_), None) => true,
            _ => false,
        };
        if take_left {
            write_entry(&mut writer, a.as_ref().unwrap())?;
            a = read_entry(&mut left)?;
        } else {
            write_entry(&mut writer, b.as_ref().unwrap())?;
            b = read_entry(&mut right)?;
        }
    }
    writer.flush()?;
    Ok(())
}
fn flush_chunk(chunk: &mut Vec<Entry>, runs: &mut Runs) -> Result<()> {
    if chunk.is_empty() {
        return Ok(());
    }
    chunk.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    let path = runs.path();
    let mut writer = BufWriter::new(std::fs::File::create(&path)?);
    for entry in chunk.drain(..) {
        write_entry(&mut writer, &entry)?;
    }
    writer.flush()?;
    runs.add(path)
}
impl IndexManager {
    /// Begin a bounded external construction of a location-index base:
    /// allocate the scratch run directory and memory budget. Entries are
    /// fed with [`Self::unique_index_stream_push`], possibly in several
    /// phases, and the base is installed with
    /// [`Self::unique_index_stream_publish`].
    pub(crate) fn unique_index_stream_begin(
        &self,
        db: &str,
        table: &str,
        _index_name: &str,
    ) -> Result<UniqueIndexStreamBuild> {
        let dir = self.unique_index_dir(db, table);
        std::fs::create_dir_all(&dir)?;
        let scratch = dir.join(format!(".build-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&scratch)?;
        let runs = Runs {
            dir: scratch,
            next: 0,
            levels: Vec::new(),
        };
        let limit = crate::common::config::query_limits()
            .query_memory_limit_mb
            .saturating_mul(1024 * 1024)
            .min(usize::MAX as u64) as usize;
        let chunk_limit = (limit / 4).max(1).min(4 * 1024 * 1024);
        let memory = crate::sql::execution::spill::OperatorMemory::new("index build", limit);
        Ok(UniqueIndexStreamBuild {
            runs,
            memory,
            chunk_limit,
            chunk: Vec::new(),
            bytes: 0,
            flush_epoch: self.segment_mgr.memtable_flush_epoch(db, table),
        })
    }

    /// Feed one entry into the pending stream. Entries over the per-entry
    /// memory budget abort the build; overflowing chunks spill to sorted
    /// runs on disk.
    pub(crate) fn unique_index_stream_push(
        &self,
        build: &mut UniqueIndexStreamBuild,
        entry: UniqueIndexEntry,
    ) -> Result<()> {
        let key = encode_unique_key(&entry.key)?;
        let size = key
            .len()
            .saturating_mul(2)
            .saturating_add(entry.location.segment_path.len())
            .saturating_add(128);
        if size > build.chunk_limit {
            return Err(TaibaiError::ResourceBusy(
                "one unique index entry exceeds the index build memory budget".into(),
            ));
        }
        if build.bytes.saturating_add(size) > build.chunk_limit {
            flush_chunk(&mut build.chunk, &mut build.runs)?;
            build.bytes = 0;
        }
        build.bytes = build.bytes.saturating_add(size);
        build.memory.reserve(build.bytes.saturating_mul(2))?;
        build.chunk.push((key, entry.location));
        Ok(())
    }

    /// Merge the pending runs, fsync a complete temporary base, and open its
    /// snapshot outside the table mutation lock. Duplicate keys across runs
    /// keep the last occurrence: a full rebuild can transiently hold a row at
    /// both its old and new location, and the later scan is authoritative.
    ///
    /// The epoch is captured when the build begins. A producer that raced a
    /// segment change fails here or at the final publish instead of stamping
    /// later watermarks on entries from an earlier table state.
    pub(crate) fn unique_index_stream_prepare(
        &self,
        db: &str,
        table: &str,
        index_name: &str,
        columns: &[String],
        mut build: UniqueIndexStreamBuild,
    ) -> Result<PreparedUniqueIndexStream> {
        if self.segment_mgr.memtable_flush_epoch(db, table) != build.flush_epoch {
            return Err(stale_stream_build(db, table, index_name));
        }
        flush_chunk(&mut build.chunk, &mut build.runs)?;
        let run = build.runs.finish()?;
        let tmp = build.runs.dir.join("base.tmp");
        let mut writer = UniqueIndexBaseWriter::new(
            BufWriter::new(std::fs::File::create(&tmp)?),
            db,
            table,
            index_name,
            columns.to_vec(),
        )?;
        let mut held: Option<Entry> = None;
        if let Some(run) = run {
            let mut reader = BufReader::new(std::fs::File::open(run)?);
            let mut count = 0usize;
            loop {
                let next = read_entry(&mut reader)?;
                let had_next = next.is_some();
                let same_as_held = matches!(
                    (&held, &next),
                    (Some((hkey, _)), Some((nkey, _))) if hkey == nkey
                );
                match held.take() {
                    None => held = next,
                    Some((pkey, ploc)) => {
                        if same_as_held {
                            held = next;
                        } else {
                            reserve_base_entry(
                                &mut build.memory,
                                &build.chunk,
                                &writer,
                                &pkey,
                                &ploc,
                            )?;
                            writer.push_entry(&pkey, &ploc)?;
                            held = next;
                        }
                    }
                }
                if !had_next {
                    break;
                }
                count += 1;
                if count % 1024 == 0 {
                    crate::common::query_cancel::check()?;
                }
            }
        }
        if let Some((key, location)) = held {
            reserve_base_entry(&mut build.memory, &build.chunk, &writer, &key, &location)?;
            writer.push_entry(&key, &location)?;
        }
        #[cfg(test)]
        if FAIL_STREAM_PREPARE.with(|fail| fail.replace(false)) {
            return Err(TaibaiError::Storage(
                "forced unique index stream prepare failure".into(),
            ));
        }
        let watermarks = self.current_segment_watermarks(db, table)?;
        let file = writer
            .finish(Some(build.flush_epoch), &watermarks)?
            .into_inner()
            .map_err(|e| TaibaiError::Storage(e.to_string()))?;
        file.sync_all()?;
        drop(file);
        // Opening here both validates the completed file before touching the
        // old path and keeps all directory/footer parsing out of the short
        // publication lock. The handle follows the inode across the rename.
        let base = UniqueIndexBase::open(&tmp)?;
        Ok(PreparedUniqueIndexStream {
            runs: build.runs,
            tmp,
            base,
            flush_epoch: build.flush_epoch,
        })
    }

    /// Atomically install a prepared base. The caller holds the table
    /// mutation lock; this method performs only bounded epoch validation, the
    /// rename, sidecar cleanup, and the in-memory swap. On drift the prepared
    /// files are discarded and the previous base and delta remain usable.
    pub(crate) fn unique_index_stream_publish(
        &self,
        db: &str,
        table: &str,
        index_name: &str,
        prepared: PreparedUniqueIndexStream,
    ) -> Result<()> {
        if self.segment_mgr.memtable_flush_epoch(db, table) != prepared.flush_epoch {
            return Err(stale_stream_build(db, table, index_name));
        }
        let dir = self.unique_index_dir(db, table);
        let data_path = dir.join(format!("{}.keys.tbi", sanitize_file_component(index_name)));
        std::fs::rename(&prepared.tmp, &data_path)?;
        let base = sanitize_file_component(index_name);
        for suffix in ["delta.tbi", "progress.tbi", "invalid"] {
            let _ = std::fs::remove_file(dir.join(format!("{}.{}", base, suffix)));
        }
        self.unique_indexes.write().unwrap().insert(
            unique_index_key(db, table, index_name),
            Arc::new(UniqueIndexState::new(
                prepared.base,
                UniqueIndexDeltaState::default(),
            )),
        );
        // Keep the scratch owner alive until after the swap. Its Drop removes
        // the now-empty run directory and never touches the renamed base.
        drop(prepared.runs);
        Ok(())
    }

    /// Scan entries once into bounded sorted runs, merge with bounded fan-in,
    /// and publish the normal on-demand base. Producer errors/collisions leave
    /// the old index intact; the RAII run directory removes all scratch files.
    #[allow(dead_code)] // retained for component users and external-build regression tests
    pub(crate) fn persist_unique_index_stream<F>(
        &self,
        db: &str,
        table: &str,
        index_name: &str,
        columns: &[String],
        producer: F,
    ) -> Result<()>
    where
        F: FnOnce(&mut dyn FnMut(UniqueIndexEntry) -> Result<()>) -> Result<()>,
    {
        let mut build = self.unique_index_stream_begin(db, table, index_name)?;
        producer(&mut |entry| self.unique_index_stream_push(&mut build, entry))?;
        let prepared = self.unique_index_stream_prepare(db, table, index_name, columns, build)?;
        self.segment_mgr.with_table_mutation_lock(db, table, || {
            self.unique_index_stream_publish(db, table, index_name, prepared)
        })
    }
}

fn stale_stream_build(db: &str, table: &str, index_name: &str) -> TaibaiError {
    TaibaiError::ResourceBusy(format!(
        "stale unique index build for {}.{}.{index_name}; retry outside the table mutation lock",
        db, table
    ))
}

#[cfg(test)]
thread_local! {
    static FAIL_STREAM_PREPARE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(test)]
pub(super) fn fail_next_stream_prepare() {
    FAIL_STREAM_PREPARE.with(|fail| fail.set(true));
}

fn reserve_base_entry<W: std::io::Write>(
    memory: &mut crate::sql::execution::spill::OperatorMemory,
    chunk: &Vec<Entry>,
    writer: &UniqueIndexBaseWriter<W>,
    key: &[u8],
    location: &UniqueRowLocation,
) -> Result<()> {
    // Reserve directory, current block, serialized footer and entry growth
    // before the base writer allocates them.
    memory.reserve(
        chunk
            .capacity()
            .saturating_mul(std::mem::size_of::<Entry>())
            .saturating_add(writer.resident_build_bytes().saturating_mul(3))
            .saturating_add(
                key.len()
                    .saturating_add(location.segment_path.len())
                    .saturating_add(256)
                    .saturating_mul(4),
            ),
    )
}

pub(crate) struct UniqueIndexStreamBuild {
    runs: Runs,
    memory: crate::sql::execution::spill::OperatorMemory,
    chunk_limit: usize,
    chunk: Vec<Entry>,
    bytes: usize,
    flush_epoch: u64,
}

pub(crate) struct PreparedUniqueIndexStream {
    runs: Runs,
    tmp: PathBuf,
    base: UniqueIndexBase,
    flush_epoch: u64,
}
