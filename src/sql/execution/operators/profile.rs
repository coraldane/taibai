use std::cell::RefCell;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::common::error::Result;
use crate::types::batch::RecordBatch;

use super::{ExecutionOp, RuntimeInFilter};

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ScanProfileSeed {
    pub elapsed: Duration,
    pub peak_memory_bytes: usize,
}

#[derive(Clone, Debug)]
pub(crate) struct OperatorProfileSnapshot {
    pub id: usize,
    pub depth: usize,
    pub name: String,
    pub estimated_rows: u64,
    pub actual_rows: u64,
    pub output_batches: u64,
    pub elapsed: Duration,
    pub peak_memory_bytes: u64,
    pub spill_files: u64,
    pub spill_rows: u64,
    pub spill_bytes: u64,
}

#[derive(Default)]
struct OperatorProfileState {
    actual_rows: u64,
    output_batches: u64,
    elapsed: Duration,
    peak_memory_bytes: u64,
    spill_files: u64,
    spill_rows: u64,
    spill_bytes: u64,
}

#[derive(Clone)]
pub(crate) struct OperatorProfile {
    id: usize,
    depth: usize,
    name: String,
    estimated_rows: u64,
    state: Arc<Mutex<OperatorProfileState>>,
}

impl OperatorProfile {
    fn snapshot(&self) -> OperatorProfileSnapshot {
        let state = self.state.lock().unwrap();
        OperatorProfileSnapshot {
            id: self.id,
            depth: self.depth,
            name: self.name.clone(),
            estimated_rows: self.estimated_rows,
            actual_rows: state.actual_rows,
            output_batches: state.output_batches,
            elapsed: state.elapsed,
            peak_memory_bytes: state.peak_memory_bytes,
            spill_files: state.spill_files,
            spill_rows: state.spill_rows,
            spill_bytes: state.spill_bytes,
        }
    }

    pub(crate) fn add_elapsed(&self, elapsed: Duration) {
        let mut state = self.state.lock().unwrap();
        state.elapsed = state.elapsed.saturating_add(elapsed);
    }

    pub(crate) fn observe_memory(&self, bytes: usize) {
        let mut state = self.state.lock().unwrap();
        state.peak_memory_bytes = state.peak_memory_bytes.max(bytes as u64);
    }

    fn observe_batch(&self, batch: &RecordBatch) {
        let mut state = self.state.lock().unwrap();
        state.actual_rows = state.actual_rows.saturating_add(batch.num_rows() as u64);
        state.output_batches = state.output_batches.saturating_add(1);
        state.peak_memory_bytes = state
            .peak_memory_bytes
            .max(batch.estimated_size_bytes() as u64);
    }

    fn add_spill(&self, rows: u64, bytes: u64) {
        let mut state = self.state.lock().unwrap();
        state.spill_files = state.spill_files.saturating_add(1);
        state.spill_rows = state.spill_rows.saturating_add(rows);
        state.spill_bytes = state.spill_bytes.saturating_add(bytes);
    }
}

#[derive(Clone, Default)]
pub(crate) struct QueryProfile {
    operators: Arc<Mutex<Vec<OperatorProfile>>>,
}

impl QueryProfile {
    pub(crate) fn register(
        &self,
        depth: usize,
        name: impl Into<String>,
        estimated_rows: u64,
    ) -> OperatorProfile {
        let mut operators = self.operators.lock().unwrap();
        let profile = OperatorProfile {
            id: operators.len(),
            depth,
            name: name.into(),
            estimated_rows,
            state: Arc::new(Mutex::new(OperatorProfileState::default())),
        };
        operators.push(profile.clone());
        profile
    }

    pub(crate) fn snapshots(&self) -> Vec<OperatorProfileSnapshot> {
        self.operators
            .lock()
            .unwrap()
            .iter()
            .map(OperatorProfile::snapshot)
            .collect()
    }
}

thread_local! {
    static ACTIVE_PROFILES: RefCell<Vec<OperatorProfile>> = const { RefCell::new(Vec::new()) };
}

struct ActiveProfileGuard;

impl ActiveProfileGuard {
    fn enter(profile: OperatorProfile) -> Self {
        ACTIVE_PROFILES.with(|profiles| profiles.borrow_mut().push(profile));
        Self
    }
}

impl Drop for ActiveProfileGuard {
    fn drop(&mut self) {
        ACTIVE_PROFILES.with(|profiles| {
            profiles.borrow_mut().pop();
        });
    }
}

pub(crate) fn record_current_peak_memory(bytes: usize) {
    ACTIVE_PROFILES.with(|profiles| {
        if let Some(profile) = profiles.borrow().last() {
            profile.observe_memory(bytes);
        }
    });
}

pub(crate) fn record_current_spill(rows: u64, bytes: u64) {
    ACTIVE_PROFILES.with(|profiles| {
        if let Some(profile) = profiles.borrow().last() {
            profile.add_spill(rows, bytes);
        }
    });
}

pub(crate) struct ProfiledExec {
    input: Box<dyn ExecutionOp>,
    profile: OperatorProfile,
}

impl ProfiledExec {
    pub(crate) fn wrap(
        input: Box<dyn ExecutionOp>,
        profile: OperatorProfile,
    ) -> Box<dyn ExecutionOp> {
        Box::new(Self { input, profile })
    }
}

impl ExecutionOp for ProfiledExec {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        let _active = ActiveProfileGuard::enter(self.profile.clone());
        let start = Instant::now();
        let result = self.input.next_batch();
        self.profile.add_elapsed(start.elapsed());
        if let Ok(Some(batch)) = &result {
            self.profile.observe_batch(batch);
        }
        result
    }

    fn add_runtime_in_filter(&mut self, filter: RuntimeInFilter) -> bool {
        self.input.add_runtime_in_filter(filter)
    }
}

#[cfg(test)]
mod tests {
    use super::{ProfiledExec, QueryProfile};
    use crate::protocol::types::DataType;
    use crate::sql::execution::operators::scan::ScanExec;
    use crate::sql::execution::operators::sort::SortExec;
    use crate::sql::planner::{LogicalExpr, SortKey};
    use crate::types::array::ColumnArray;
    use crate::types::batch::{ColumnDef, RecordBatch, Schema};
    use crate::types::scalar::ScalarValue;

    #[test]
    fn profiled_sort_reports_rows_memory_and_spill_io() {
        let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
        let mut values = ColumnArray::empty(DataType::Int64);
        for value in (0..128i64).rev() {
            values.push(&ScalarValue::Int64(Some(value))).unwrap();
        }
        let batch = RecordBatch::try_new(schema.clone(), vec![values]).unwrap();
        let input = Box::new(ScanExec::new(vec![batch], None));
        let sort = SortExec::new(
            vec![SortKey {
                expr: LogicalExpr::Column(0),
                desc: false,
            }],
            input,
            schema,
        )
        .with_memory_limit_bytes(1);

        let query = QueryProfile::default();
        let operator = query.register(0, "Sort", 128);
        let mut profiled = ProfiledExec::wrap(Box::new(sort), operator);
        let mut rows = 0usize;
        while let Some(batch) = profiled.next_batch().unwrap() {
            rows += batch.num_rows();
        }
        assert_eq!(rows, 128);

        let metrics = query.snapshots();
        assert_eq!(metrics.len(), 1);
        assert_eq!(metrics[0].actual_rows, 128);
        assert!(metrics[0].peak_memory_bytes > 0);
        assert!(metrics[0].spill_files > 0);
        assert!(metrics[0].spill_rows >= 128);
        assert!(metrics[0].spill_bytes > 0);
    }
}
