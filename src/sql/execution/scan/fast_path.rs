//! Scan fast-path chain: the Strategy/chain-of-responsibility skeleton that
//! gates and dispatches the scan-side shortcut paths.
//!
//! Every fast path historically re-implemented the same three-part shape —
//! eligibility gates (TTL, LIMIT pushdown safety, schema drift), the cheap
//! read itself, and a decline-to-fallback — and each site wired its own gates,
//! which is how truncation bugs happened (a new shortcut forgetting the TTL
//! gate returned fewer rows than exist). This module separates the two roles:
//!
//! * [`ScanFastPath`] implementations own only "the cheap read" plus their
//!   path-specific applicability. They delegate to the same free functions
//!   that existed before the chain (in `memtable` / `aggregate_meta`), which
//!   keep their internal gates as defense in depth — the in-module tests call
//!   those functions directly and must keep passing unchanged.
//! * The caller (the scan executor / `FoldedAggregateExec`) owns the chain:
//!   the universal TTL gate runs once before any node, decline falls through
//!   to the next node or the general path, and every outcome is recorded for
//!   EXPLAIN ANALYZE.
//!
//! Two outcome levels exist because the shortcuts answer at two different
//! granularities: [`FastPathOutcome::FinalAnswer`] is the finished query
//! result (metadata folds skip the operator pipeline), while
//! [`FastPathOutcome::ScanRows`] is a full scan-leg replacement that still
//! flows through the scan sink (limit + memory budget) and the operators
//! above it.

use std::path::{Path, PathBuf};

use crate::common::error::Result;
use crate::sql::planner::{AggregateExpr, LogicalExpr};
use crate::storage::catalog::schema::TableMeta;
use crate::storage::core::hot_store::HotStoreManager;
use crate::storage::core::memtable::MemTableManager;
use crate::storage::segment_manager::SegmentManager;
use crate::types::batch::{RecordBatch, Schema};

use super::runtime::ScanRuntime;
use super::spec::{MetadataFoldRequest, ScanSpec};

/// Skeleton-level gate entry recorded when TTL blocks the whole chain before
/// any node runs.
pub(crate) const TTL_GATE_NODE: &str = "ttl_gate";

/// Why a fast path did not answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DeclineReason {
    /// The chain ran for a shape this node does not apply to (e.g. the spec
    /// carries no `distinct_column`).
    NotApplicable,
    /// The node's own gates declined: residency, schema drift, unsupported
    /// aggregate shapes, TTL, txn overlays, ... (the free functions aggregate
    /// these; per-condition reasons would require threading reason strings
    /// through every decline site). TTL specifically is also enforced once
    /// for the whole chain by [`ttl_blocks`] before any node runs.
    NodeInternal,
}

/// One node's decline, carrying its chain position for observability.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FastPathDecline {
    pub node: &'static str,
    pub reason: DeclineReason,
}

#[derive(Debug)]
pub(crate) enum FastPathOutcome {
    /// The finished query result (folded aggregate / DISTINCT): the caller
    /// must not feed these rows through the aggregate pipeline again.
    FinalAnswer(RecordBatch),
    /// A complete replacement for the scan legs (resident hot store): pushed
    /// through the scan sink with the plan limit; downstream operators still
    /// run over it.
    ScanRows(RecordBatch),
    Declined(FastPathDecline),
}

/// Everything a fast-path node may read. Plain borrowed references so tests
/// can hand-build managers exactly like the free functions expect; use
/// [`FastPathContext::from_runtime`] at production call sites.
pub(crate) struct FastPathContext<'a> {
    pub spec: &'a ScanSpec,
    pub table_meta: &'a TableMeta,
    pub table_schema: &'a Schema,
    pub db: &'a str,
    /// Precomputed `data_dir / db / table` — the folded nodes read footers
    /// from it, and its `exists()` check absorbs the "no persisted segments"
    /// workaround the executor and `FoldedAggregateExec` used to duplicate.
    pub table_dir: PathBuf,
    pub segment_mgr: &'a SegmentManager,
    pub memtable_mgr: &'a MemTableManager,
    pub hot_mgr: &'a HotStoreManager,
}

impl<'a> FastPathContext<'a> {
    pub(crate) fn from_runtime(
        spec: &'a ScanSpec,
        table_meta: &'a TableMeta,
        table_schema: &'a Schema,
        db: &'a str,
        runtime: &'a ScanRuntime,
    ) -> Self {
        Self {
            table_dir: runtime.data_dir().join(db).join(&spec.table_name),
            segment_mgr: runtime.segment_mgr(),
            memtable_mgr: runtime.memtable_manager(),
            hot_mgr: runtime.hot_store_manager(),
            spec,
            table_meta,
            table_schema,
            db,
        }
    }
}

/// The single TTL decision point (`spec.rs` documents the contract): the spec
/// flag is set from the same source at spec-compile time, and the table-meta
/// check stops hand-built specs that forgot the flag. Truncation-style fast
/// paths gather at the pushed LIMIT before TTL filtering runs, so with
/// expired rows present they would return fewer live rows than exist.
pub(crate) fn ttl_blocks(spec: &ScanSpec, table_meta: &TableMeta) -> bool {
    spec.ttl_active || table_meta.ttl.is_some()
}

/// One shortcut in the scan fast-path chain. `try_answer` either answers or
/// declines — errors are I/O / decode failures, never "cannot answer".
pub(crate) trait ScanFastPath {
    fn name(&self) -> &'static str;

    fn try_answer(&self, ctx: &FastPathContext<'_>) -> Result<FastPathOutcome>;
}

/// Single-column, predicate-free `SELECT DISTINCT` answered from row-group
/// zone maps. The executor's routing TTL gate used to live at the call site;
/// the chain skeleton owns it now.
pub(crate) struct DistinctMetadataFastPath;

impl ScanFastPath for DistinctMetadataFastPath {
    fn name(&self) -> &'static str {
        "distinct_metadata"
    }

    fn try_answer(&self, ctx: &FastPathContext<'_>) -> Result<FastPathOutcome> {
        let Some(column) = ctx.spec.distinct_column.as_deref() else {
            return Ok(FastPathOutcome::Declined(FastPathDecline {
                node: self.name(),
                reason: DeclineReason::NotApplicable,
            }));
        };
        match super::aggregate_meta::compute_distinct_column(
            ctx.spec,
            ctx.table_meta,
            ctx.table_schema,
            &ctx.table_dir,
            ctx.segment_mgr,
            ctx.memtable_mgr,
            ctx.hot_mgr,
            ctx.db,
            column,
        )? {
            Some(batch) => Ok(FastPathOutcome::FinalAnswer(batch)),
            None => Ok(FastPathOutcome::Declined(FastPathDecline {
                node: self.name(),
                reason: DeclineReason::NodeInternal,
            })),
        }
    }
}

/// Metadata-folded aggregate: answered from row-group zone maps / footers per
/// fold unit and group, decoding only the units metadata cannot fold.
///
/// The fold request is carried explicitly (not read from
/// `spec.metadata_fold`) because the physical planner compiles
/// `FoldedAggregateExec`'s spec without populating that field, and the
/// deferred fallback must never inherit it — the deferred executor ignores
/// `metadata_answered`, so a fold re-answer there would feed an aggregate
/// result batch into the aggregate pipeline as raw rows.
pub(crate) struct FoldedAggregateFastPath<'a> {
    group_by: &'a [LogicalExpr],
    aggregates: &'a [AggregateExpr],
}

impl<'a> FoldedAggregateFastPath<'a> {
    pub(crate) fn from_fold(fold: &'a MetadataFoldRequest) -> Self {
        Self {
            group_by: &fold.group_by,
            aggregates: &fold.aggregates,
        }
    }

    /// Direct construction for callers that hold the group/aggregate slices
    /// outside a `MetadataFoldRequest` (the physical planner's
    /// `FoldedAggregateExec` compiles its spec without populating the field).
    pub(crate) fn new(group_by: &'a [LogicalExpr], aggregates: &'a [AggregateExpr]) -> Self {
        Self {
            group_by,
            aggregates,
        }
    }
}

impl ScanFastPath for FoldedAggregateFastPath<'_> {
    fn name(&self) -> &'static str {
        "folded_aggregate"
    }

    fn try_answer(&self, ctx: &FastPathContext<'_>) -> Result<FastPathOutcome> {
        // No persisted segments; only memtable / hot store rows.
        let table_dir = if ctx.table_dir.exists() {
            ctx.table_dir.as_path()
        } else {
            Path::new("")
        };
        match super::aggregate_meta::compute_folded_aggregate(
            ctx.spec,
            self.group_by,
            self.aggregates,
            ctx.table_meta,
            ctx.table_schema,
            table_dir,
            ctx.segment_mgr,
            ctx.memtable_mgr,
            ctx.hot_mgr,
            ctx.db,
        )? {
            Some(batch) => Ok(FastPathOutcome::FinalAnswer(batch)),
            None => Ok(FastPathOutcome::Declined(FastPathDecline {
                node: self.name(),
                reason: DeclineReason::NodeInternal,
            })),
        }
    }
}

/// Resident hot-store shortcut: when the store provably holds every live row
/// of the table (full residency, autocommit, no TTL) it answers the scan
/// without opening segments or memtables.
///
/// The projection slices come from the caller's built [`super::strategy::ScanPlan`]
/// — this node runs after the plan exists, and narrowing the projection is
/// what the old `try_resident_hot_scan` call site did.
pub(crate) struct ResidentHotFastPath<'a> {
    pub projection_cols: &'a [usize],
    pub strip_ttl_column: bool,
}

impl ScanFastPath for ResidentHotFastPath<'_> {
    fn name(&self) -> &'static str {
        "resident_hot"
    }

    fn try_answer(&self, ctx: &FastPathContext<'_>) -> Result<FastPathOutcome> {
        let mut hot_spec = ctx.spec.clone();
        hot_spec.projection = Some(self.projection_cols.to_vec());
        let Some(batch) = super::memtable::scan_resident_hot_store(
            ctx.hot_mgr,
            ctx.memtable_mgr,
            ctx.db,
            ctx.table_meta,
            &hot_spec,
        ) else {
            return Ok(FastPathOutcome::Declined(FastPathDecline {
                node: self.name(),
                reason: DeclineReason::NodeInternal,
            }));
        };
        // Same defensive post-step the old call site applied: the node
        // declines every TTL table so the filter is a no-op here, but every
        // scan-row source goes through it uniformly.
        let batch = super::strategy::apply_ttl_filter_and_projection(
            batch,
            ctx.table_meta,
            self.projection_cols,
            self.strip_ttl_column,
        )?;
        Ok(FastPathOutcome::ScanRows(batch))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::types::DataType;
    use crate::storage::catalog::schema::TtlInfo;
    use crate::types::batch::ColumnDef;
    use crate::types::scalar::ScalarValue;

    fn test_meta(ttl: Option<TtlInfo>) -> TableMeta {
        let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
        let mut meta = TableMeta::new("main", "events", schema.columns.clone());
        meta.ttl = ttl;
        meta
    }

    fn test_ctx<'a>(
        spec: &'a ScanSpec,
        meta: &'a TableMeta,
        schema: &'a Schema,
        segment_mgr: &'a SegmentManager,
        memtable_mgr: &'a MemTableManager,
        hot_mgr: &'a HotStoreManager,
    ) -> FastPathContext<'a> {
        FastPathContext {
            table_dir: PathBuf::from("nonexistent-test-table-dir"),
            spec,
            table_meta: meta,
            table_schema: schema,
            db: "main",
            segment_mgr,
            memtable_mgr,
            hot_mgr,
        }
    }

    /// The universal gate: either the spec flag (set at spec-compile time) or
    /// the table metadata blocks — a hand-built spec that forgot the flag is
    /// still stopped by the meta check.
    #[test]
    fn ttl_blocks_on_either_signal() {
        let ttl = TtlInfo {
            column: "ts".to_string(),
            duration_secs: 3600,
            column_idx: 1,
        };
        let meta_with_ttl = test_meta(Some(ttl));
        let meta_without = test_meta(None);

        let mut spec = ScanSpec::new("events");
        spec.ttl_active = true;
        assert!(ttl_blocks(&spec, &meta_without));

        let clean_spec = ScanSpec::new("events");
        assert!(ttl_blocks(&clean_spec, &meta_with_ttl));

        assert!(ttl_blocks(&spec, &meta_with_ttl));
        assert!(!ttl_blocks(&clean_spec, &meta_without));
    }

    /// Without a `distinct_column` in the spec the node does not apply at
    /// all — it declines before touching any manager.
    #[test]
    fn distinct_node_declines_not_applicable_without_target_column() {
        let meta = test_meta(None);
        let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
        let spec = ScanSpec::new("events");

        let dir = std::env::temp_dir().join(format!("taibai_fp_test_{}", std::process::id()));
        let segment_mgr = SegmentManager::new(&dir).unwrap();
        let memtable_mgr = MemTableManager::new(10_000, 60, 2, 3600);
        let hot_mgr = HotStoreManager::new();
        let ctx = test_ctx(&spec, &meta, &schema, &segment_mgr, &memtable_mgr, &hot_mgr);

        let outcome = DistinctMetadataFastPath.try_answer(&ctx).unwrap();
        match outcome {
            FastPathOutcome::Declined(decline) => {
                assert_eq!(decline.node, "distinct_metadata");
                assert_eq!(decline.reason, DeclineReason::NotApplicable);
            }
            other => panic!("expected decline, got {other:?}"),
        }
    }

    /// With no hot store for the table the resident node declines through the
    /// free function's own gate (no store registered in the manager).
    #[test]
    fn resident_node_declines_without_hot_store() {
        let meta = test_meta(None);
        let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
        let mut spec = ScanSpec::new("events");
        spec.eq_predicates.push((0, ScalarValue::Int64(Some(1))));

        let dir = std::env::temp_dir().join(format!("taibai_fp_test_res_{}", std::process::id()));
        let segment_mgr = SegmentManager::new(&dir).unwrap();
        let memtable_mgr = MemTableManager::new(10_000, 60, 2, 3600);
        let hot_mgr = HotStoreManager::new();
        let ctx = test_ctx(&spec, &meta, &schema, &segment_mgr, &memtable_mgr, &hot_mgr);

        let node = ResidentHotFastPath {
            projection_cols: &[0],
            strip_ttl_column: false,
        };
        let outcome = node.try_answer(&ctx).unwrap();
        match outcome {
            FastPathOutcome::Declined(decline) => {
                assert_eq!(decline.node, "resident_hot");
                assert_eq!(decline.reason, DeclineReason::NodeInternal);
            }
            other => panic!("expected decline, got {other:?}"),
        }
    }

    /// The folded node delegates the TTL decline: with table TTL set the fold
    /// engine returns `Ok(None)` and the node reports a decline, never an
    /// error and never a fabricated answer.
    #[test]
    fn folded_node_declines_ttl_via_delegation() {
        let ttl = TtlInfo {
            column: "ts".to_string(),
            duration_secs: 3600,
            column_idx: 0,
        };
        let meta = test_meta(Some(ttl));
        let schema = Schema::new(vec![ColumnDef::new("ts", DataType::Timestamp)]);
        let mut spec = ScanSpec::new("events");
        spec.table_schema = schema.clone();

        let dir = std::env::temp_dir().join(format!("taibai_fp_test_fold_{}", std::process::id()));
        let segment_mgr = SegmentManager::new(&dir).unwrap();
        let memtable_mgr = MemTableManager::new(10_000, 60, 2, 3600);
        let hot_mgr = HotStoreManager::new();
        let ctx = test_ctx(&spec, &meta, &schema, &segment_mgr, &memtable_mgr, &hot_mgr);

        let node = FoldedAggregateFastPath {
            group_by: &[],
            aggregates: &[],
        };
        let outcome = node.try_answer(&ctx).unwrap();
        match outcome {
            FastPathOutcome::Declined(decline) => {
                assert_eq!(decline.node, "folded_aggregate");
                assert_eq!(decline.reason, DeclineReason::NodeInternal);
            }
            other => panic!("expected decline, got {other:?}"),
        }
    }
}
