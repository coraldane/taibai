use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::{Arc, RwLock};

use super::codec::*;
use super::state_ops::*;
use super::types::*;
use super::IndexManager;
use crate::common::error::Result;
impl IndexManager {
    pub fn pause_unique_index_compaction(&self) {
        let mut schedule = self.unique_index_compaction.lock().unwrap();
        schedule.pause_count = schedule.pause_count.saturating_add(1);
    }

    /// Whether physical maintenance that rewrites location-index base
    /// snapshots must currently stay deferred (an active bulk import holds
    /// the compaction pause).
    pub fn unique_index_compaction_paused(&self) -> bool {
        self.unique_index_compaction
            .lock()
            .unwrap()
            .pause_count
            > 0
    }

    pub fn resume_unique_index_compaction(&self) {
        let pending = {
            let mut schedule = self.unique_index_compaction.lock().unwrap();
            schedule.pause_count = schedule.pause_count.saturating_sub(1);
            if schedule.pause_count > 0 {
                return;
            }
            std::mem::take(&mut schedule.pending)
        };
        for (db, table, index_name) in pending {
            let cache_key = unique_index_key(&db, &table, &index_name);
            let state = self.unique_indexes.read().unwrap().get(&cache_key).cloned();
            let Some(state) = state else {
                continue;
            };
            let delta_count = {
                let delta = state.delta.read().unwrap();
                delta
                    .entry_count
                    .saturating_add(delta.tombstones.len() as u64)
            };
            if unique_index_delta_needs_compaction(delta_count, state.base.entries.len()) {
                self.dispatch_unique_index_compaction(&db, &table, &index_name, &state);
            }
        }
    }

    pub(super) fn try_dispatch_unique_index_compaction(
        &self,
        db: &str,
        table: &str,
        index_name: &str,
        state: &Arc<UniqueIndexState>,
    ) {
        let mut schedule = self.unique_index_compaction.lock().unwrap();
        if schedule.pause_count > 0 {
            schedule
                .pending
                .insert((db.to_string(), table.to_string(), index_name.to_string()));
            return;
        }
        drop(schedule);
        self.dispatch_unique_index_compaction(db, table, index_name, state);
    }

    fn dispatch_unique_index_compaction(
        &self,
        db: &str,
        table: &str,
        index_name: &str,
        state: &Arc<UniqueIndexState>,
    ) {
        // Final re-check under the schedule lock: a dispatch racing a fresh
        // `begin_bulk_import` (e.g. a resume draining its pending set while a
        // new import pauses compaction) must re-queue instead of rewriting the
        // base snapshot mid-import. Every dispatch path funnels through here,
        // so this is the single choke point that keeps the pause airtight.
        {
            let mut schedule = self.unique_index_compaction.lock().unwrap();
            if schedule.pause_count > 0 {
                schedule
                    .pending
                    .insert((db.to_string(), table.to_string(), index_name.to_string()));
                return;
            }
        }
        if state.compaction_in_flight.swap(true, Ordering::AcqRel) {
            return;
        }
        let job = UniqueIndexCompactionJob {
            db: db.to_string(),
            table: table.to_string(),
            index_name: index_name.to_string(),
            state: Arc::clone(state),
            unique_indexes: Arc::clone(&self.unique_indexes),
            data_dir: self.data_dir.clone(),
        };
        #[cfg(test)]
        {
            job.run();
        }
        #[cfg(not(test))]
        {
            if std::thread::Builder::new()
                .name("taibai-unique-index-compaction".to_string())
                .spawn(move || job.run())
                .is_err()
            {
                state.compaction_in_flight.store(false, Ordering::Release);
            }
        }
    }
}

pub(super) struct UniqueIndexCompactionSnapshot {
    pub(super) delta_bytes: Vec<u8>,
    tombstones: HashSet<UniqueRowLocation>,
    removed_keys: HashSet<Vec<u8>>,
}

/// Single-flight background compaction of the delta back into the base
/// snapshot. Split into snapshot -> unlocked merge -> publish so appends
/// that race the merge (records appended past the snapshot length) are
/// preserved: publish rewrites the delta file to exactly that suffix.
pub(super) struct UniqueIndexCompactionJob {
    pub(super) db: String,
    pub(super) table: String,
    pub(super) index_name: String,
    pub(super) state: Arc<UniqueIndexState>,
    pub(super) unique_indexes: Arc<RwLock<HashMap<String, Arc<UniqueIndexState>>>>,
    pub(super) data_dir: PathBuf,
}

impl UniqueIndexCompactionJob {
    /// Best effort: any failure leaves the pre-compaction base and delta
    /// untouched (still a valid, merely larger, index) and the next
    /// threshold crossing retries.
    fn run(self) {
        if let Err(_error) = self.execute() {}
        self.state
            .compaction_in_flight
            .store(false, Ordering::Release);
    }

    fn execute(&self) -> Result<()> {
        let Some(snapshot) = self.snapshot()? else {
            return Ok(());
        };
        let (tmp_path, data) = self.build_compacted_base(&snapshot)?;
        if !self.publish(snapshot.delta_bytes.len(), &tmp_path, data)? {
            let _ = std::fs::remove_file(&tmp_path);
        }
        Ok(())
    }

    pub(super) fn snapshot(&self) -> Result<Option<UniqueIndexCompactionSnapshot>> {
        let _guard = self.state.mutation_lock.lock().unwrap();
        if !self.state_is_current() {
            return Ok(None);
        }
        let delta_path = self.delta_path();
        let delta_bytes = if delta_path.exists() {
            std::fs::read(&delta_path)?
        } else {
            Vec::new()
        };
        let (tombstones, removed_keys) = {
            let delta = self.state.delta.read().unwrap();
            (delta.tombstones.clone(), delta.removed_keys.clone())
        };
        Ok(Some(UniqueIndexCompactionSnapshot {
            delta_bytes,
            tombstones,
            removed_keys,
        }))
    }

    fn state_is_current(&self) -> bool {
        self.unique_indexes
            .read()
            .unwrap()
            .get(&unique_index_key(&self.db, &self.table, &self.index_name))
            .is_some_and(|current| Arc::ptr_eq(current, &self.state))
    }

    fn index_dir(&self) -> PathBuf {
        self.data_dir
            .join(&self.db)
            .join(&self.table)
            .join("indexes")
            .join("unique")
    }

    pub(super) fn delta_path(&self) -> PathBuf {
        self.index_dir().join(format!(
            "{}.delta.tbi",
            sanitize_file_component(&self.index_name)
        ))
    }

    pub(super) fn base_path(&self) -> PathBuf {
        self.index_dir().join(format!(
            "{}.keys.tbi",
            sanitize_file_component(&self.index_name)
        ))
    }

    pub(super) fn build_compacted_base(
        &self,
        snapshot: &UniqueIndexCompactionSnapshot,
    ) -> Result<(PathBuf, UniqueIndexData)> {
        let mut data = materialize_unique_index_state_parts(
            &self.state.base,
            &snapshot.delta_bytes,
            &snapshot.tombstones,
            &snapshot.removed_keys,
        )?;
        // Embed the snapshot progress: the progress sidecar file may
        // advance during the merge and outranks this on restart.
        data.flush_epoch = self.state.progress_epoch();
        data.segment_watermarks = self.state.progress_watermarks();
        compact_prepared_unique_index_entries(&mut data);
        let tmp = self.base_path().with_extension("keys.tbi.tmp");
        std::fs::create_dir_all(self.index_dir())?;
        std::fs::write(&tmp, encode_unique_index_data(&data)?)?;
        Ok((tmp, data))
    }

    /// Publishes the compacted base. Appends that raced the merge (records
    /// past `snapshot_len`) survive as the rewritten delta file's only
    /// content. The base rename happens before the delta rewrite: a crash
    /// in between leaves duplicate-but-benign entries, never lost ones.
    pub(super) fn publish(
        &self,
        snapshot_len: usize,
        tmp_path: &Path,
        data: UniqueIndexData,
    ) -> Result<bool> {
        let _guard = self.state.mutation_lock.lock().unwrap();
        if !self.state_is_current() {
            return Ok(false);
        }
        let delta_path = self.delta_path();
        let current = if delta_path.exists() {
            std::fs::read(&delta_path)?
        } else {
            Vec::new()
        };
        let suffix = current[snapshot_len.min(current.len())..].to_vec();
        let suffix_state = decode_unique_index_delta_records(&suffix, 0)?.into_state()?;
        std::fs::rename(tmp_path, self.base_path())?;
        if suffix.is_empty() {
            let _ = std::fs::remove_file(&delta_path);
        } else {
            let mut suffix_file = UNIQUE_INDEX_DELTA_MAGIC_V2.to_vec();
            suffix_file.extend_from_slice(&suffix);
            let delta_tmp = delta_path.with_extension("delta.tbi.tmp");
            std::fs::write(&delta_tmp, suffix_file)?;
            std::fs::rename(delta_tmp, &delta_path)?;
        }
        let progress = self.state.progress.read().unwrap().clone();
        self.unique_indexes.write().unwrap().insert(
            unique_index_key(&self.db, &self.table, &self.index_name),
            Arc::new(UniqueIndexState::with_progress(
                data,
                suffix_state,
                progress,
            )),
        );
        Ok(true)
    }
}
