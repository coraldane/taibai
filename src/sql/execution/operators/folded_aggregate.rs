//! Metadata-folded aggregate operator.
//!
//! Sits exactly where `HashAggregateExec` sits for `Aggregate(single Scan)`
//! shapes. On the first pull it runs the folding engine
//! (`scan::aggregate_meta::compute_folded_aggregate`), which answers from
//! segment metadata (zone maps / footers / per-row-group sums) per fold unit
//! and per group — decoding only the units the metadata cannot fold. When the
//! engine declines (unsupported shape, TTL, drifted predicate column, ...),
//! the operator lazily builds the regular `HashAggregateExec` over a deferred
//! scan and delegates, so the fallback costs nothing until it is needed.

use crate::common::error::Result;
use crate::sql::execution::operators::aggregate::HashAggregateExec;
use crate::sql::execution::operators::scan::ScanExec;
use crate::sql::execution::operators::ExecutionOp;
use crate::sql::execution::scan::aggregate_meta;
use crate::sql::execution::scan::runtime::ScanRuntime;
use crate::sql::execution::scan::spec::ScanSpec;
use crate::sql::planner::{AggregateExpr, LogicalExpr};
use crate::storage::catalog::schema::TableMeta;
use crate::types::batch::{RecordBatch, Schema};

enum FoldedState {
    /// The first `next_batch` has not resolved the engine yet.
    Pending,
    /// The engine answered: emit its batch, then end the stream.
    Answered(Option<RecordBatch>),
    /// The engine declined: delegate to the regular aggregate pipeline.
    Fallback(HashAggregateExec),
}

pub struct FoldedAggregateExec {
    group_by: Vec<LogicalExpr>,
    aggregates: Vec<AggregateExpr>,
    spec: ScanSpec,
    runtime: ScanRuntime,
    table_schema: Schema,
    table_meta: TableMeta,
    db: String,
    state: FoldedState,
}

impl FoldedAggregateExec {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        group_by: Vec<LogicalExpr>,
        aggregates: Vec<AggregateExpr>,
        spec: ScanSpec,
        runtime: ScanRuntime,
        table_schema: Schema,
        table_meta: TableMeta,
        db: String,
    ) -> Self {
        Self {
            group_by,
            aggregates,
            spec,
            runtime,
            table_schema,
            table_meta,
            db,
            state: FoldedState::Pending,
        }
    }

    /// Run the folding engine once, under the same table flush read lock the
    /// scan executor uses, so segment discovery and footer reads see one
    /// consistent snapshot.
    fn resolve(&mut self) -> Result<()> {
        let segment_mgr = self.runtime.segment_mgr().clone();
        let db = self.db.clone();
        let table = self.spec.table_name.clone();
        let batch = segment_mgr.with_table_flush_read_lock(&db, &table, || {
            let table_dir = self
                .runtime
                .data_dir()
                .join(&self.db)
                .join(&self.spec.table_name);
            let table_dir = if table_dir.exists() {
                table_dir
            } else {
                // No persisted segments; only memtable / hot store rows.
                std::path::PathBuf::new()
            };
            aggregate_meta::compute_folded_aggregate(
                &self.spec,
                &self.group_by,
                &self.aggregates,
                &self.table_meta,
                &self.table_schema,
                &table_dir,
                self.runtime.segment_mgr(),
                self.runtime.memtable_manager(),
                self.runtime.hot_store_manager(),
                &self.db,
            )
        })?;
        self.state = match batch {
            Some(batch) => FoldedState::Answered(Some(batch)),
            None => {
                // Declined: build the regular pipeline over the same scan.
                // ScanExec::deferred is lazy, so nothing was opened for the
                // folded attempt.
                let scan = ScanExec::deferred(
                    self.spec.clone(),
                    self.runtime.clone(),
                    self.table_schema.clone(),
                    self.table_meta.clone(),
                    self.db.clone(),
                );
                FoldedState::Fallback(HashAggregateExec::new(
                    self.group_by.clone(),
                    self.aggregates.clone(),
                    Box::new(scan),
                    self.table_schema.clone(),
                ))
            }
        };
        Ok(())
    }
}

impl ExecutionOp for FoldedAggregateExec {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        crate::common::query_cancel::check()?;
        if matches!(self.state, FoldedState::Pending) {
            self.resolve()?;
        }
        match &mut self.state {
            FoldedState::Answered(batch) => Ok(batch.take()),
            FoldedState::Fallback(exec) => exec.next_batch(),
            FoldedState::Pending => unreachable!("resolve ran above"),
        }
    }
}
