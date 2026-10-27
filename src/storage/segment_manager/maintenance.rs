use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::common::error::Result;
use crate::storage::segment::meta::SegmentMetaSnapshot;
use crate::storage::segment::snapshot::SegmentSnapshot;
use crate::storage::segment_manager::catalog::{
    build_plan_meta, load_catalog_file, store_catalog_file, MaintenanceTtlColumn, SegmentPlanMeta,
};
use crate::storage::segment_manager::{
    same_segment_path, table_key, SegmentManager, TableSegmentState,
};

const RECLAIM_COMMIT_BATCH: usize = 64;

/// Full directory discovery (read_dir + retirement-journal scan) runs every
/// Nth maintenance round; the rounds in between drive the candidate list from
/// the segment catalog. Reconcile bounds how long out-of-band segment changes
/// (replication apply, external tooling) can stay invisible to maintenance.
const CATALOG_RECONCILE_ROUNDS: u32 = 8;

struct MaintenancePlanLease {
    state: Arc<TableSegmentState>,
    _segments: Arc<SegmentSnapshot>,
}

impl Drop for MaintenancePlanLease {
    fn drop(&mut self) {
        self.state.prune_retired_maintenance_segments();
    }
}

impl std::fmt::Debug for MaintenancePlanLease {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("MaintenancePlanLease").finish()
    }
}

#[derive(Clone, Debug)]
pub struct PlannedSegment {
    pub path: PathBuf,
    pub file_len: u64,
    pub revision: u64,
    pub meta: Arc<SegmentPlanMeta>,
    _lease: Arc<MaintenancePlanLease>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MaintenanceSegmentState {
    Immutable,
    Active,
    Sealing,
}

#[derive(Clone, Debug)]
pub struct SegmentObservation {
    pub path: PathBuf,
    pub state: MaintenanceSegmentState,
    pub file_len: u64,
    pub row_count: u64,
    pub deleted_row_count: u64,
    pub row_group_rows: Vec<u64>,
    plan: Option<Arc<SegmentPlanMeta>>,
    revision: Option<u64>,
}

impl SegmentObservation {
    fn planned(&self, lease: &Arc<MaintenancePlanLease>) -> Option<PlannedSegment> {
        if self.state != MaintenanceSegmentState::Immutable {
            return None;
        }
        Some(PlannedSegment {
            path: self.path.clone(),
            file_len: self.file_len,
            revision: self.revision?,
            meta: Arc::clone(self.plan.as_ref()?),
            _lease: Arc::clone(lease),
        })
    }
}

#[derive(Clone, Debug)]
pub struct MaintenanceSegmentError {
    pub path: PathBuf,
    pub partition_key: Option<String>,
    pub error: String,
}

impl MaintenanceSegmentError {
    fn new(path: &Path, error: impl std::fmt::Display) -> Self {
        Self {
            path: path.to_path_buf(),
            partition_key: path.file_name().and_then(|name| {
                crate::storage::partition::partition::parse_partition_key(&name.to_string_lossy())
            }),
            error: error.to_string(),
        }
    }
}

pub struct MaintenanceSnapshot {
    lease: Arc<MaintenancePlanLease>,
    pub segments: Vec<SegmentObservation>,
    errors: Vec<MaintenanceSegmentError>,
}

impl MaintenanceSnapshot {
    pub fn planned_segments(&self) -> Vec<PlannedSegment> {
        self.segments
            .iter()
            .filter_map(|segment| segment.planned(&self.lease))
            .collect()
    }

    pub fn fully_deleted_segments(&self) -> Vec<PlannedSegment> {
        self.segments
            .iter()
            .filter(|segment| {
                segment.row_count > 0 && segment.deleted_row_count >= segment.row_count
            })
            .filter_map(|segment| segment.planned(&self.lease))
            .collect()
    }

    pub fn segment_errors(&self) -> &[MaintenanceSegmentError] {
        &self.errors
    }

    pub fn refresh_mutable_segments(
        &mut self,
        manager: &SegmentManager,
        ttl_column: Option<&MaintenanceTtlColumn>,
    ) {
        for index in 0..self.segments.len() {
            if self.segments[index].state == MaintenanceSegmentState::Immutable {
                continue;
            }
            let path = self.segments[index].path.clone();
            let refreshed = (|| -> Result<Option<SegmentObservation>> {
                let file_len = std::fs::metadata(&path)?.len();
                let before = manager.maintenance_revision(&path);
                let Some(meta) = manager.segment_meta(&path)? else {
                    return Ok(None);
                };
                let after = manager.maintenance_revision(&path);
                if before != after || before.1 || after.1 {
                    return Ok(None);
                }
                let plan = Arc::new(build_plan_meta(&meta, file_len, ttl_column));
                if let Some(state) = manager.state_for_segment_path(&path) {
                    state.set_maintenance_plan(&path, Arc::clone(&plan));
                }
                Ok(Some(SegmentObservation {
                    path: path.clone(),
                    state: MaintenanceSegmentState::Immutable,
                    file_len,
                    row_count: plan.row_count,
                    deleted_row_count: plan.deleted_row_count,
                    row_group_rows: plan.row_group_rows.clone(),
                    plan: Some(plan),
                    revision: Some(before.0),
                }))
            })();
            match refreshed {
                Ok(Some(observation)) => self.segments[index] = observation,
                Ok(None) => {}
                Err(error) => self.errors.push(MaintenanceSegmentError::new(&path, error)),
            }
        }
    }
}

impl SegmentManager {
    pub fn capture_maintenance_snapshot(
        &self,
        db: &str,
        table: &str,
        table_dir: &Path,
        ttl_column: Option<&MaintenanceTtlColumn>,
    ) -> Result<MaintenanceSnapshot> {
        let state = self.get_or_create_state(&table_key(db, table));
        if state.catalog_load_guard() {
            load_catalog_into_state(&state, table_dir);
        }
        let force_reconcile = state.take_catalog_reconcile_request();
        let round = state.next_catalog_round();
        let reconcile = force_reconcile || round % CATALOG_RECONCILE_ROUNDS == 0;
        let disk_snapshot = if reconcile {
            Some(crate::storage::segment::snapshot::snapshot_segments(
                table_dir,
            )?)
        } else {
            None
        };
        let lease_segments = match &disk_snapshot {
            Some(snapshot) => Arc::clone(snapshot),
            None => crate::storage::segment::snapshot::pin_directory(table_dir),
        };
        let plan_lease = Arc::new(MaintenancePlanLease {
            state: Arc::clone(&state),
            _segments: lease_segments,
        });
        let (active_paths, sealing_paths) = self.maintenance_path_states(db, table);
        if let Some(snapshot) = &disk_snapshot {
            let mut keep: HashSet<PathBuf> = snapshot.paths.iter().cloned().collect();
            keep.extend(active_paths.iter().cloned());
            keep.extend(sealing_paths.iter().cloned());
            state.retain_maintenance_paths(&keep);
        }
        let mut paths = match &disk_snapshot {
            Some(snapshot) => snapshot.paths.clone(),
            None => state.maintenance_paths(),
        };
        let mut seen: HashSet<PathBuf> = paths.iter().cloned().collect();
        for extra in active_paths.iter().chain(sealing_paths.iter()) {
            if seen.insert(extra.clone()) {
                paths.push(extra.clone());
            }
        }
        paths.sort();

        let mut segments = Vec::with_capacity(paths.len());
        let mut errors = Vec::new();
        for path in &paths {
            let observed = (|| -> Result<Option<SegmentObservation>> {
                let segment_state = if active_paths.contains(path) {
                    MaintenanceSegmentState::Active
                } else if sealing_paths.contains(path) {
                    MaintenanceSegmentState::Sealing
                } else {
                    MaintenanceSegmentState::Immutable
                };
                let Ok(file_meta) = std::fs::metadata(path) else {
                    // The file vanished out-of-band (replication apply,
                    // external tooling): drop the advisory entry and skip.
                    state.remove_maintenance_path(path);
                    tracing::debug!(
                        path = %path.display(),
                        "segment gone during maintenance snapshot"
                    );
                    return Ok(None);
                };
                let file_len = file_meta.len();
                if segment_state == MaintenanceSegmentState::Active {
                    if let Some(observation) = self.active_maintenance_observation(path, file_len) {
                        return Ok(Some(observation));
                    }
                }
                if segment_state == MaintenanceSegmentState::Sealing {
                    return Ok(Some(SegmentObservation {
                        path: path.clone(),
                        state: segment_state,
                        file_len,
                        row_count: 0,
                        deleted_row_count: 0,
                        row_group_rows: Vec::new(),
                        plan: None,
                        revision: None,
                    }));
                }
                Ok(Some(self.observe_immutable_segment(
                    &state, path, file_len, ttl_column,
                )?))
            })();
            match observed {
                Ok(Some(segment)) => segments.push(segment),
                Ok(None) => {}
                Err(error) => errors.push(MaintenanceSegmentError::new(path, error)),
            }
        }

        if let Some(generation) = state.catalog_persist_due() {
            persist_catalog(&state, table_dir, generation);
        }
        Ok(MaintenanceSnapshot {
            lease: plan_lease,
            segments,
            errors,
        })
    }

    /// Observe an immutable segment from the catalog plan when possible,
    /// falling back to one footer read that rebuilds the plan.
    fn observe_immutable_segment(
        &self,
        state: &Arc<TableSegmentState>,
        path: &Path,
        file_len: u64,
        ttl_column: Option<&MaintenanceTtlColumn>,
    ) -> Result<SegmentObservation> {
        let before = self.maintenance_revision(path);
        if !before.1 {
            if let Some(plan) = state.maintenance_plan(path) {
                let plan_fresh = plan.file_len == file_len
                    && ttl_plan_matches(
                        plan.ttl.as_ref().map(|stats| stats.column.as_str()),
                        ttl_column,
                    );
                if plan_fresh {
                    let after = self.maintenance_revision(path);
                    if !after.1 && after.0 == before.0 {
                        return Ok(observation_from_plan(
                            path,
                            MaintenanceSegmentState::Immutable,
                            file_len,
                            plan,
                            Some(before.0),
                        ));
                    }
                }
            }
        }

        let meta = self.immutable_segment_meta_with_repair(path)?;
        if let Some(meta) = meta {
            let after = self.maintenance_revision(path);
            let stable_revision =
                (!before.1 && !after.1 && before.0 == after.0).then_some(before.0);
            let plan = Arc::new(build_plan_meta(&meta, file_len, ttl_column));
            state.set_maintenance_plan(path, Arc::clone(&plan));
            return Ok(observation_from_plan(
                path,
                MaintenanceSegmentState::Immutable,
                file_len,
                plan,
                stable_revision,
            ));
        }

        let reader = crate::storage::segment::SegmentReader::open(path)?;
        Ok(SegmentObservation {
            path: path.to_path_buf(),
            state: MaintenanceSegmentState::Active,
            file_len,
            row_count: reader.row_count(),
            deleted_row_count: reader.deleted_row_count(),
            row_group_rows: (0..reader.num_row_groups())
                .map(|index| reader.row_group_rows(index))
                .collect(),
            plan: None,
            revision: None,
        })
    }

    fn immutable_segment_meta_with_repair(
        &self,
        path: &Path,
    ) -> Result<Option<Arc<SegmentMetaSnapshot>>> {
        if let Some(meta) = self.segment_meta(path)? {
            return Ok(Some(meta));
        }
        // Directory discovery used to complete interrupted delete-footer
        // rewrites on every round; with catalog rounds it runs here, on the
        // one path that actually needs the footer.
        let _ = crate::storage::segment::SegmentReader::repair_pending_footer_rewrite(path);
        self.segment_meta(path)
    }

    pub fn planned_segments_unchanged(&self, candidates: &[PlannedSegment]) -> bool {
        candidates
            .iter()
            .all(|candidate| self.maintenance_candidate_current(candidate, None))
    }

    pub fn cleanup_fully_deleted_segments(
        &self,
        db: &str,
        table: &str,
        table_dir: &Path,
        snapshot: &MaintenanceSnapshot,
    ) -> Result<u64> {
        let candidates = snapshot.fully_deleted_segments();
        let mut removed = 0u64;
        for batch in candidates.chunks(RECLAIM_COMMIT_BATCH) {
            let mut partitions = batch
                .iter()
                .map(|candidate| {
                    candidate
                        .path
                        .file_name()
                        .and_then(|name| {
                            crate::storage::partition::partition::parse_partition_key(
                                &name.to_string_lossy(),
                            )
                        })
                        .unwrap_or_default()
                })
                .collect::<Vec<_>>();
            partitions.sort();
            partitions.dedup();
            let partition_refs = partitions.iter().map(String::as_str).collect::<Vec<_>>();
            let committed =
                self.with_partition_set_mutation_lock(db, table, &partition_refs, || {
                    let paths = batch
                        .iter()
                        .filter(|candidate| {
                            self.maintenance_candidate_current(candidate, Some(true))
                                && !self.txn_mutations().is_some_and(|registry| {
                                    registry.segment_pin_conflicts(db, table, &candidate.path)
                                })
                        })
                        .map(|candidate| candidate.path.clone())
                        .collect::<Vec<_>>();
                    if paths.is_empty() {
                        return Ok(Vec::new());
                    }
                    self.retire_segments(table_dir, &paths)?;
                    Ok(paths)
                })?;
            if committed.is_empty() {
                continue;
            }
            self.note_segments_changed(db, table);
            let names = committed
                .iter()
                .filter_map(|path| path.file_name())
                .map(|name| name.to_string_lossy().into_owned())
                .collect::<Vec<_>>();
            self.emit_segments_removed(table_dir, &names);
            removed += committed.len() as u64;
        }
        Ok(removed)
    }

    fn maintenance_candidate_current(
        &self,
        candidate: &PlannedSegment,
        require_fully_deleted: Option<bool>,
    ) -> bool {
        let Some(state) = self.state_for_segment_path(&candidate.path) else {
            return false;
        };
        let active = state
            .active_segments
            .lock()
            .unwrap()
            .values()
            .any(|segment| same_segment_path(&segment.path, &candidate.path));
        if active || state.sealing_paths().contains(&candidate.path) {
            return false;
        }
        if std::fs::metadata(&candidate.path)
            .map(|metadata| metadata.len())
            .ok()
            != Some(candidate.file_len)
        {
            return false;
        }
        let (revision, retired) = self.maintenance_revision(&candidate.path);
        if retired || revision != candidate.revision {
            return false;
        }
        require_fully_deleted != Some(true)
            || (candidate.meta.row_count > 0
                && candidate.meta.deleted_row_count >= candidate.meta.row_count)
    }

    fn maintenance_revision(&self, path: &Path) -> (u64, bool) {
        self.state_for_segment_path(path)
            .map(|state| state.maintenance_revision(path))
            .unwrap_or((0, true))
    }

    fn active_maintenance_observation(
        &self,
        path: &Path,
        file_len: u64,
    ) -> Option<SegmentObservation> {
        let state = self.state_for_segment_path(path)?;
        let active = state.active_segments.lock().unwrap();
        let segment = active
            .values()
            .find(|segment| same_segment_path(&segment.path, path))?;
        Some(SegmentObservation {
            path: path.to_path_buf(),
            state: MaintenanceSegmentState::Active,
            file_len,
            row_count: segment.total_rows,
            deleted_row_count: segment.deleted_row_count,
            row_group_rows: segment
                .row_groups
                .iter()
                .map(|group| group.row_count)
                .collect(),
            plan: None,
            revision: None,
        })
    }

    fn maintenance_path_states(
        &self,
        db: &str,
        table: &str,
    ) -> (HashSet<PathBuf>, HashSet<PathBuf>) {
        let tables = self.tables.read().unwrap();
        tables
            .get(&table_key(db, table))
            .map(|state| {
                let active = state
                    .active_segments
                    .lock()
                    .unwrap()
                    .values()
                    .map(|segment| segment.path.clone())
                    .collect();
                (active, state.sealing_paths())
            })
            .unwrap_or_default()
    }
}

fn observation_from_plan(
    path: &Path,
    state: MaintenanceSegmentState,
    file_len: u64,
    plan: Arc<SegmentPlanMeta>,
    revision: Option<u64>,
) -> SegmentObservation {
    SegmentObservation {
        path: path.to_path_buf(),
        state,
        file_len,
        row_count: plan.row_count,
        deleted_row_count: plan.deleted_row_count,
        row_group_rows: plan.row_group_rows.clone(),
        plan: Some(plan),
        revision,
    }
}

/// A plan without TTL stats is fine for a table without a TTL; a table WITH a
/// TTL rebuilds any plan lacking stats for its current TTL column so expiry
/// decisions never silently degrade to "not expired".
fn ttl_plan_matches(plan_column: Option<&str>, ttl_column: Option<&MaintenanceTtlColumn>) -> bool {
    match (plan_column, ttl_column) {
        (None, None) => true,
        (None, Some(_)) => false,
        (Some(_), None) => true,
        (Some(plan), Some(ttl)) => plan == ttl.name,
    }
}

fn load_catalog_into_state(state: &Arc<TableSegmentState>, table_dir: &Path) {
    match load_catalog_file(table_dir) {
        Ok(Some(records)) => {
            for record in records {
                if record.name.is_empty() {
                    continue;
                }
                let path = table_dir.join(&record.name);
                state.seed_maintenance_plan(path, record.into_plan());
            }
        }
        Ok(None) => {}
        Err(error) => {
            tracing::warn!(
                table_dir = %table_dir.display(),
                %error,
                "segment catalog unreadable; rebuilding from directory"
            );
        }
    }
}

fn persist_catalog(state: &Arc<TableSegmentState>, table_dir: &Path, generation: u64) {
    let records = state.catalog_records();
    match store_catalog_file(table_dir, &records) {
        Ok(()) => state.mark_catalog_persisted(generation),
        Err(error) => {
            tracing::warn!(
                table_dir = %table_dir.display(),
                %error,
                "segment catalog persist failed"
            );
        }
    }
}
