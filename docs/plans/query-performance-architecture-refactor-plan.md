# Query Performance Architecture Refactor Plan

## Background

The import path is already performing well, so query optimization must be handled as a read-side architecture refactor instead of adding more case-specific checks into the current execution path. The current query flow contains several optimizations, but many of them are expressed as ad-hoc branches in `query.rs` and `ScanExecutor::execute_scan`. This makes behavior hard to reason about, hard to extend, and risky for maintenance.

The core goal is to improve query performance without adding overhead to the import path.

## DuckDB Architecture Takeaways

DuckDB 0.2.0 separates query execution into clear phases:

1. SQL/binder/planner builds a logical operator tree.
2. Optimizer rewrites logical operators using rules.
3. Physical plan generator turns logical operators into physical operators.
4. Each physical operator owns its runtime state and exposes a uniform chunk-producing interface.
5. Scan receives pushed-down column IDs and table filters, but does not require the top-level query executor to know scan special cases.
6. Filter is a normal operator that evaluates vectorized expressions using a selection vector.
7. Scan-level pruning, table filters, and index scans are selected by planning, not by top-level query orchestration.

Relevant local DuckDB references:

- `E:/data/duckdb-0.2.0/src/execution/physical_plan_generator.cpp`
- `E:/data/duckdb-0.2.0/src/execution/operator/scan/physical_table_scan.cpp`
- `E:/data/duckdb-0.2.0/src/execution/operator/filter/physical_filter.cpp`

The important lesson is not to copy DuckDB internals directly, but to copy the separation of responsibility: logical optimization decides intent, physical operators implement one consistent execution contract, and storage-specific fast paths are selected through scan strategy objects instead of top-level special cases.

## Current Taibai Findings

Current query path:

1. `src/sql/execution/query.rs` resolves statement, loads table metadata, plans and optimizes.
2. It compiles `ScanSpec` per table.
3. It executes `ScanExecutor` immediately and materializes all table batches in memory.
4. It then builds a pull pipeline from already-materialized `table_batches` through `src/sql/execution/operators/mod.rs`.
5. It performs extra plan rewrites such as `remove_redundant_pushed_filter` and `try_skip_sort_for_cluster_key` at the query entry layer.

Current scan path:

- `src/sql/execution/scan/spec.rs` already centralizes scan intent: predicates, projection, limit, cluster key, partition info, and index columns.
- `src/sql/execution/scan/executor.rs` performs partition pruning, row-group pruning, bloom/set checks, cluster-key range reads, predicate-first reads, delete bitmap filtering, TTL filtering, projection, memtable scan, limit handling, count-star handling, and stats updates.
- `src/sql/execution/scan/row_filter.rs` contains many row-level and fast-path filtering routines.

Main architectural problem:

- The query entry function knows too much about physical execution.
- Scan is batch-materializing instead of being a first-class pipeline source.
- Scan strategy selection is encoded as nested conditionals instead of a small strategy planner.
- Fast paths are mixed with correctness behavior, making it difficult to add optimizations without risking regressions.

## Target Architecture

### 1. Query front-end only orchestrates phases

`exec_table_select` should eventually become:

1. Resolve subqueries.
2. Load table context.
3. Build logical plan.
4. Optimize logical plan.
5. Build physical pipeline.
6. Drain result batches.

It should not know details such as cluster-key sort skipping, pushed-filter removal, scan materialization, or scan fast-path choices.

### 2. Add a physical planning layer

Introduce a small physical plan generator inspired by DuckDB:

- Input: optimized `LogicalPlan` plus table metadata context.
- Output: `Box<dyn ExecutionOp>` tree.
- It replaces `build_pipeline(plan, &table_batches)` with `build_physical_pipeline(plan, &ExecutionContext)`.

Initial physical nodes can reuse current operators:

- `PhysicalScanExec`
- `FilterExec`
- `ProjectExec`
- `SortExec`
- `LimitExec`
- `HashAggregateExec`
- `HashJoinExec`
- `TopN` as sort-with-limit or a dedicated operator later

### 3. Make scan a streaming source operator

Replace the query-level pre-scan map with a scan operator that calls storage lazily:

- `ScanExec` should own `ScanSpec`, table schema, table meta, db name, engine reference, and optional stats.
- It should implement `ExecutionOp::next_batch`.
- It should maintain scan state: segment cursor, row-group cursor, opened reader, pending memtable batches, emitted row count.

This prevents large table scans from being fully materialized before downstream operators can apply limit/top-n/join behavior.

### 4. Split scan planning from scan execution

Inside `src/sql/execution/scan`, introduce a strategy layer:

- `ScanPlanner`: derives a `ScanPlan` from `ScanSpec`, `TableMeta`, and available segment metadata.
- `ScanPlan`: immutable description of candidate segments, predicate columns, projection columns, row-group predicates, checks, TTL projection extension, and limit behavior.
- `RowGroupReadStrategy`: chooses one of a small set of read methods.

Suggested strategies:

| Strategy | Use case | Reads |
| --- | --- | --- |
| `PlainRead` | no predicate-first benefit | projection columns then filter |
| `PredicateFirstRead` | predicates touch narrow columns and projection is wider | predicate columns first, then projected columns for selected rows |
| `ClusterKeyRangeRead` | cluster-key ordered row group with usable bounds | binary/range read on cluster key |
| `CountStarRead` | exact count without row materialization | metadata count, with TTL fallback |
| `EmptyRead` | pruning eliminates all data | no storage read |

The important rule: `ScanExecutor` should dispatch to a selected strategy, not contain nested business-specific conditionals.

### 5. Move special cases into rules or strategies

Current query-layer special cases should move downward or upward into proper layers:

| Current behavior | Target location |
| --- | --- |
| redundant pushed filter removal | optimizer rule or physical planner property |
| skip sort for cluster key | optimizer/physical planner using ordering property |
| count-star shortcut | physical scan strategy |
| cluster key binary/range read | scan strategy |
| predicate-first read | scan strategy |
| limit early stop | scan operator state |
| TTL filtering | scan post-read policy |
| delete bitmap | storage visibility policy inside scan |
| memtable scan | scan source composition |

### 6. Track physical properties

Add lightweight physical properties so optimizations are not hard-coded:

- `output_ordering`: columns/order guaranteed by scan or sort.
- `fully_applied_filter`: whether pushed predicates are complete.
- `estimated_rows`: optional, used for strategy decisions.
- `required_columns`: projection plus predicate columns.

This enables generic decisions such as removing sort when input ordering satisfies `ORDER BY`, instead of hard-coding cluster-key checks in `query.rs`.

## Import Performance Isolation

The refactor must not add work to the import path.

Rules:

1. Do not change segment writer hot paths for query-only optimization.
2. Do not compute new heavy metadata during import unless it already exists or is guarded by table/index configuration.
3. Query-side planning may lazily inspect segment footers and cache derived read-side metadata.
4. If a new metadata structure is needed, prefer backward-compatible optional metadata generated during compaction or background maintenance, not synchronous bulk import.
5. Keep `dml_insert.rs`, bulk insert protocol, WAL append, segment writer, and memtable write path out of the first query refactor phase.
6. Add benchmarks that run import throughput before and after any storage metadata change.

## Phased Implementation

### Phase 0: Guardrails

- Add benchmark or integration checks for representative queries:
  - point lookup on cluster key
  - date/security range query
  - top-n with order/limit
  - count star
  - filter on non-cluster column
- Add import throughput baseline command and keep it unchanged through query refactor phases.

### Phase 1: Remove query-entry special cases

- Create a physical planning module under `src/sql/execution/physical` or `src/sql/execution/planner`.
- Move pipeline construction responsibilities out of `query.rs`.
- Keep current materialized scan behavior initially to reduce risk.
- Move `try_skip_sort_for_cluster_key` and redundant pushed-filter handling into planner/optimizer properties.

Expected result: no major performance change, but architecture becomes safer.

### Phase 2: Streaming scan operator

- Introduce a new scan operator that owns `ScanExecutor` state and returns one batch at a time.
- Stop building `HashMap<String, Vec<RecordBatch>>` for simple single-table plans.
- Preserve the current materialized path for joins if needed, then migrate joins later.

Expected result: better limit/top-n behavior and lower memory usage.

### Phase 3: Scan strategy planner

- Extract strategy selection from `ScanExecutor::execute_scan` into explicit strategy structs/functions.
- Keep the same underlying row_filter functions at first.
- Ensure each strategy has clear preconditions and fallback to `PlainRead`.

Expected result: fewer special cases, safer future optimizations.

### Phase 4: Vectorized predicate execution cleanup

- Consolidate predicate evaluation into one vectorized expression/mask path.
- Keep binary/range pruning as metadata-level pruning, not expression special casing.
- Use a selection-mask approach similar to DuckDB filter operators.

Expected result: fewer duplicated predicate paths and more predictable correctness.

### Phase 5: Read-side metadata cache

- Add optional query-side cache for segment footer summaries, cluster-key bounds, row-group statistics, and predicate support structures.
- Cache must be invalidated by segment replacement/compaction and must not slow imports.

Expected result: faster planning/pruning without import overhead.

## Immediate Next Steps

1. Implement physical planning shell while preserving existing behavior.
2. Add an execution context object carrying engine, db, table metadata, schemas, and stats.
3. Change `query.rs` to call the physical planner instead of pre-scanning every table.
4. Introduce a streaming scan source for single-table scan/filter/project/limit plans.
5. Extract `ScanExecutor` strategy selection after streaming scan is stable.

## Implementation Notes

### 2026-06-28 Phase 1 Progress

Completed:

- Added `src/sql/execution/physical` as a physical execution planning layer.
- Added `PhysicalContext` to carry engine, resolved table refs, schemas, table metadata, and count-star intent.
- Changed `query.rs` so safe single-source SELECT plans can enter the physical planner instead of keeping all execution-specific rewrites in the query entry point.
- Kept complex plans on the old materialized fallback path:
  - joins
  - semi joins
  - aggregates and `COUNT(*)`
  - distinct
  - window plans
  - stats-enabled explain/query paths
- Moved redundant pushed-filter removal and cluster-key sort skipping into the physical planner.
- Made redundant pushed-filter removal conservative: only simple scan-rechecked predicates are removed; function predicates remain protected by `FilterExec`.
- Kept import/write paths untouched.

Important constraint found during implementation:

- The first implementation showed that a truly lazy `ScanExec` should not own `&Engine`, because current operator trees mostly use `Box<dyn ExecutionOp>` with an implicit static object lifetime.
- This was addressed by adding an owned read-side `ScanRuntime` snapshot containing only the read dependencies needed by `ScanExecutor`.
- Physical scan now defers `ScanExecutor::execute()` until `ScanExec::next_batch()` is first pulled, so planning no longer performs scan I/O.
- Safe `LIMIT/OFFSET` hints are propagated through the physical builder into `ScanSpec` for direct scan/project paths, allowing `ScanExecutor` to stop early when there is no order/aggregate/join/remaining-filter dependency.
- Deferred scan buffers are consumed through a queue, so returned batches are released as downstream operators pull them instead of being retained for the whole scan lifetime.
- `ScanExecutor` now routes scan output through a `BatchSink` abstraction, with `LimitedBatchSink` centralizing batch emission, limit trimming, and early-stop behavior. `execute_scan()` is now a compatibility wrapper over `execute_scan_into`, separating row-group production from result collection and providing the handoff point for a future streaming `RowGroupCursor`.
- Added `ScanCursor` for the first truly pull-based physical scan path. It streams plain row-group scans one batch at a time for safe shapes: no count-star, no predicates, no aggregate/order/group/join, and no TTL. Cursor construction is lazy and does not perform segment discovery or memtable reads until the first `next_batch()` call. It preserves projection, schema-evolution null fill, delete bitmap filtering, partition pruning, memtable tail batches, and scan limit trimming.
- Complex scan shapes still fall back to the existing deferred `ScanExecutor::execute()` path. The next phase should expand `ScanCursor` to support row-group pruning and simple predicate application, then predicate-first and cluster-key range reads.

Validation:

- `cargo check` passes.
- `cargo test` passes.

### 2026-06-28 Phase 2 / 3 / 4 Progress

Completed:

- Added `src/sql/execution/scan/strategy.rs` as the scan strategy planning layer:
  - `RowGroupReadStrategy` enum (`PlainRead`, `PredicateFirstRead`, `ClusterKeyRangeRead`).
  - `ScanPlan`, an immutable description carrying predicate columns, projection columns, TTL projection extension/strip flag, cluster-key bounds, the selected strategy, and the effective scan limit.
  - `ScanPlanner`, which derives a `ScanPlan` from `ScanSpec`, table schema, and `TableMeta` (cluster-key bound derivation, predicate-first decision, TTL projection extension, early-stop limit eligibility).
  - Shared read helpers: `prune_segment`, `read_row_group_with_strategy`, `read_plain_row_group`, `read_projected_row_group` (with schema-evolution null fill), and `apply_ttl_filter_and_projection`.
- Refactored `ScanExecutor::execute_scan_into` to build one `ScanPlan` and dispatch every row group through `read_row_group_with_strategy`, removing the previous nested cluster-key / predicate-first / plain branches. The executor now only owns segment discovery, partition pruning, cross-segment cluster-key segment pruning, stats accounting, parallel row-group collection, and the limited sink. All per-strategy business logic moved into the strategy layer.
- Deleted now-dead executor helpers (`read_and_filter_row_group`, `should_use_predicate_first`, `compute_predicate_columns`, `compute_projection_columns`, `extend_projection_for_ttl`, `apply_ttl_filter_and_projection`, `cluster_key_bounds`, and the local `strip_projection_column` / `now_micros` / `set_tighter_*` helpers).
- Expanded `ScanCursor` to stream through the same `ScanPlan`. It now supports predicates, segment partition pruning, per-segment row-group pruning, predicate-first reads, cluster-key range reads, delete-bitmap filtering, schema-evolution null fill, and TTL post-read filtering, all emitted one row group at a time with lazy segment/memtable discovery and scan-limit early stop.
- Widened streaming eligibility (`is_streamable`) so the cursor now serves any single-source scan/filter/project/limit shape except count-star, aggregate, order-by, group-by, and join. TTL tables now stream because TTL is handled as a post-read policy in the shared strategy layer instead of forcing the deferred materialized path.
- Both `ScanExecutor` and `ScanCursor` share a single read-and-filter path, consolidating the previously duplicated predicate/projection/TTL handling (Phase 4 cleanup goal for the scan source).
- Import/write paths remain untouched.

Validation:

- `cargo check` passes with no warnings introduced by the refactor.
- `cargo test` passes (all suites, 0 failures).

### 2026-06-28 Phase 2 Join Migration + Phase 5 Read-Side Cache

Completed (Phase 2 remainder — join/aggregate de-materialization):

- Extended the physical planner (`src/sql/execution/physical/planner.rs`) to build `Aggregate`, `Join`, `SemiJoin`, `Distinct`, and `Window` operators directly, reusing the existing `HashAggregateExec`, `HashJoinExec`, and `SemiJoinExec` operators.
- Each scan leaf under a join/aggregate is now built through `ScanExec::deferred`, which transparently selects the streaming `ScanCursor` (or deferred `ScanExecutor`). This removes the `HashMap<String, Vec<RecordBatch>>` pre-scan materialization for join/aggregate/distinct/window plans.
- Widened `can_use_physical_pipeline` (`is_supported_pipeline`) to accept multi-source plans (joins, semi joins) and aggregate/distinct/window shapes.
- The materialized fallback (`exec_table_select_materialized`) is now reached only for stats-enabled explain/query paths and the count-star shortcut, matching the documented isolation boundary.
- Redundant pushed-filter removal in the physical planner stays conservative and does not descend into joins, preserving the prior single-table-only behavior (multi-table filters remain protected by `FilterExec`).

Completed (Phase 5 — read-side metadata cache):

- Added `src/storage/segment_manager/cluster_meta_cache.rs` (`ClusterMetaCache`), a read-side cache of per-segment cluster-key column zone maps keyed by `(segment_path, column_index)`.
- Added `SegmentManager::cluster_zone_map_cached`, which derives and caches the cluster-key zone map for a sealed segment on first access and serves it from memory afterward. Mutable (unsealed) segments are never cached because their footer can still change.
- The cache shares the existing reader-cache invalidation lifecycle: `invalidate_reader` and `invalidate_readers_under` now also clear cluster-meta entries, so seal, compaction, delete, and DDL automatically drop stale entries. No new invalidation wiring was added to write paths.
- Refactored `ScanExecutor::segment_cluster_zone_map` to use the cached accessor, so cross-segment cluster-key segment pruning (monotonicity check + binary search) no longer re-derives per-segment bounds on every query.
- Import/write paths remain untouched; the cache is populated lazily by query-side planning only.

Validation:

- `cargo check` passes with no new warnings.
- `cargo test` passes (all suites, 435 tests, 0 failures).

#### 2026-09-11 Dedicated TopN Operator

Completed:

- Added `src/sql/execution/operators/topn.rs` (`TopNExec`), a streaming bounded-heap Top-N operator:
  - Input batches are drained once; only the best `limit + offset` rows are retained, so memory is O(K) instead of the previous O(N) full materialization in `SortExec::with_limit` (which merged all rows into one `RecordBatch` before `select_nth_unstable_by`).
  - Accepted rows are stored in bounded chunks; evicted slots become dead and trigger periodic compaction via `ColumnArray::take`, keeping storage within a small multiple of K.
  - Heap entries carry `Vec<ScalarValue>` keys compared through the shared `compare_scalars`, so ordering (including NULLs-last and per-key descending) matches `SortExec`; equal keys break by input order (deterministic).
  - Expression sort keys are remapped to the first physical batch schema, reusing `remap_expr_for_batch`/`eval_expr_column`; non-expression columns keep the typed fast path in the final gather.
- Wired `TopNExec` into both Top-N build sites: the physical planner (`LogicalPlan::TopN`) and the materialized fallback (`build_pipeline_with_subqueries`). `LimitExec` still trims offset when present. `SortExec` remains for unbounded `LogicalPlan::Sort`.
- Added `topn_test.rs`: asc/desc selection, deterministic ties, worst-case eviction stream with compaction, reference parity against full sort, expression keys, empty/zero-capacity, and NULL ordering.

Validation:

- `cargo check` passes; `rustfmt --check` clean on touched files.
- `cargo clippy --lib` introduces no new warnings.
- `cargo test --workspace` passes with 0 failures (403 lib tests plus all integration suites).

#### 2026-09-11 Cluster-Key Ordered TopN Early Stop

Completed:

- Extended `ScanPlanner::ordered_cluster_order` (`src/sql/execution/scan/strategy.rs`) to accept an
  ORDER BY column that IS the first cluster-key column. Previously only the equality-pinned prefix
  case (`WHERE k = ? ORDER BY next_key LIMIT n`) enabled the ordered-limit scan; ordering directly by
  the first cluster key returned `None`, so `ORDER BY ck LIMIT n` and every Top-N query on the
  cluster key drained the whole table into the Top-N heap. Plan-level engagement is still gated by
  the existing runtime guards: the scan disables the ordered read (and the early-stop limit) when
  memtable rows are pending or when sealed segments fail the cluster-key monotonicity check.
- Fixed a latent ordering bug in `ScanExecutor` (`src/sql/execution/scan/executor.rs`): with an
  ordered ASC limit plus pushed predicates, `rank_row_groups_for_limit` could reorder candidate row
  groups by estimated predicate density before the ordered read, breaking cluster-key order for the
  limit. Row-group ranking is now skipped whenever `ordered_limit` is active (ASC or DESC).
- With these changes, `ORDER BY <first cluster key> [DESC] LIMIT n [OFFSET m]` reads only the
  leading (or trailing, for DESC) cluster-ordered rows — O(limit + offset) I/O instead of a full
  scan — while non-monotonic storage and memtable tails keep the previous full-read correctness.
- Added regression tests:
  - `strategy_test.rs`: position-0 ordered limit allowed (asc and desc); ordering by a later cluster
    key without an equality prefix is still rejected.
  - `topn_offset_test.rs`: end-to-end asc/desc/offset and range-predicated Top-N over two monotonic
    flushed segments; fallback parity when segments are non-monotonic in discovery order; memtable
    tail holding a smaller cluster key than sealed segments is still returned under `LIMIT`.

Validation:

- `cargo fmt --check` clean.
- `cargo clippy --workspace --all-targets --all-features` clean.
- `cargo test --workspace` passes with 0 failures (403 lib tests plus all integration suites).

## Status Summary

All planned phases are now implemented:

- Phase 0 guardrails: regression test suite green.
- Phase 1: query-entry special cases moved into the physical planner.
- Phase 2: streaming scan source plus join/aggregate/distinct/window de-materialization through the physical pipeline.
- Phase 3: scan strategy planner (`ScanPlan` / `RowGroupReadStrategy` / `ScanPlanner`).
- Phase 4: single shared vectorized read-and-filter path for executor and cursor.
- Phase 5: read-side cluster-key metadata cache with reader-cache-aligned invalidation.


## Success Criteria

- Query entry code becomes shorter and contains no scan-specific special cases.
- Query correctness tests continue to pass.
- Import benchmark remains within noise of current performance.
- Point/range/top-n/count queries improve or remain stable.
- Adding a new scan optimization only requires adding a strategy/rule, not editing top-level query orchestration.
