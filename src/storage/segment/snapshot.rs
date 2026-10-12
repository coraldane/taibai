//! Segment publication and reader leases. Directory discovery and replacement
//! publication share a short mutex; a lease keeps retired files readable until
//! the last scan using this directory finishes.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use crate::common::error::Result;

#[derive(Default)]
pub(crate) struct DirectoryState {
    readers: usize,
    retirements: Vec<(PathBuf, Vec<PathBuf>)>,
}

type Directory = Mutex<DirectoryState>;

fn directory(path: &Path) -> Arc<Directory> {
    static DIRECTORIES: OnceLock<Mutex<HashMap<PathBuf, Weak<Directory>>>> = OnceLock::new();
    // Canonicalization also makes slash/case aliases share the publication gate.
    let mut ancestor = path;
    let mut missing = Vec::new();
    let key = loop {
        if let Ok(mut resolved) = std::fs::canonicalize(ancestor) {
            for component in missing.iter().rev() { resolved.push(component); }
            break resolved;
        }
        let (Some(parent), Some(name)) = (ancestor.parent(), ancestor.file_name()) else {
            break path.to_path_buf();
        };
        missing.push(name.to_os_string());
        ancestor = parent;
    };
    let mut directories = DIRECTORIES.get_or_init(Default::default).lock().unwrap();
    if let Some(state) = directories.get(&key).and_then(Weak::upgrade) {
        return state;
    }
    directories.retain(|_, state| state.strong_count() > 0);
    let state = Arc::new(Mutex::new(DirectoryState::default()));
    directories.insert(key, Arc::downgrade(&state));
    state
}

pub(crate) fn with_directory<T>(
    path: &Path,
    f: impl FnOnce(&mut DirectoryState) -> Result<T>,
) -> Result<T> {
    let state = directory(path);
    let mut guard = state.lock().unwrap();
    let result = f(&mut guard);
    let retired = guard.take_retirements();
    drop(guard);
    collect(&state, retired);
    result
}

pub struct SegmentSnapshot {
    directory: Arc<Directory>,
    pub paths: Vec<PathBuf>,
}

pub fn snapshot_segments(path: &Path) -> Result<Arc<SegmentSnapshot>> {
    let directory = directory(path);
    let mut state = directory.lock().unwrap();
    let paths = super::store::discover_segments_unlocked(path)?;
    state.readers += 1;
    drop(state);
    Ok(Arc::new(SegmentSnapshot { directory, paths }))
}

/// Pin the directory before a caller performs its own discovery. This avoids
/// an extra directory walk for metadata aggregates and hot-store-only scans.
pub(crate) fn pin_directory(path: &Path) -> Arc<SegmentSnapshot> {
    let directory = directory(path);
    directory.lock().unwrap().readers += 1;
    Arc::new(SegmentSnapshot {
        directory,
        paths: Vec::new(),
    })
}

impl DirectoryState {
    pub(crate) fn retire(&mut self, journal: PathBuf, paths: Vec<PathBuf>) {
        let committed = journal.with_extension("retire");
        let journal = if journal.extension().is_some_and(|e| e == "replace")
            && std::fs::rename(&journal, &committed).is_ok()
        {
            committed
        } else {
            journal
        };
        self.retirements.push((journal, paths));
    }

    fn take_retirements(&mut self) -> Vec<(PathBuf, Vec<PathBuf>)> {
        if self.readers != 0 {
            return Vec::new();
        }
        std::mem::take(&mut self.retirements)
    }
}

fn collect(directory: &Arc<Directory>, retirements: Vec<(PathBuf, Vec<PathBuf>)>) {
    for (journal, paths) in &retirements {
        // Committed journals do not depend on the replacement still
        // existing: it may itself be replaced before retirement succeeds.
        let committed = journal.with_extension("retire");
        let journal = if journal.extension().is_some_and(|e| e == "replace") {
            let _publication = directory.lock().unwrap();
            if std::fs::rename(journal, &committed).is_err() {
                continue;
            }
            &committed
        } else {
            journal
        };
        let mut pending = false;
        for path in paths {
            // Preserve the commit marker of a legacy replacement whose
            // promotion failed. Recovery will promote it before deletion.
            if path.with_extension("dat.replace").exists() {
                pending = true;
                continue;
            }
            if let Err(error) = std::fs::remove_file(path) {
                if error.kind() != std::io::ErrorKind::NotFound {
                    tracing::warn!(path = %path.display(), %error, "segment retirement deferred");
                    pending = true;
                    continue;
                }
            }
            if super::meta::remove_sidecar_meta(path).is_err() {
                pending = true;
            }
        }
        if !pending {
            // Discovery must not see a journal directory entry and then lose
            // its contents before it has excluded the retired paths.
            let _publication = directory.lock().unwrap();
            let _ = std::fs::remove_file(journal);
        }
        // Failed deletions remain represented by a durable .retire file.
    }
}

impl Drop for SegmentSnapshot {
    fn drop(&mut self) {
        let mut state = self.directory.lock().unwrap();
        state.readers -= 1;
        let retired = state.take_retirements();
        drop(state);
        collect(&self.directory, retired);
    }
}
