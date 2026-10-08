use crate::sql::optimizer::partition_pruning::PartitionFilter;
use crate::sql::planner::{AggregateExpr, LogicalExpr, LogicalPlan, TimeRange};
use crate::storage::catalog::schema::{SecondaryIndexType, TableMeta};
use crate::storage::partition::pruning::RangeClause;
use crate::types::batch::Schema;
use crate::types::scalar::ScalarValue;

#[derive(Debug, Clone)]
pub struct ScanSpec {
    pub table_name: String,

    pub eq_predicates: Vec<(usize, ScalarValue)>,
    pub range_predicates: Vec<RangeClause>,
    pub in_predicates: Vec<InPredicate>,
    pub remaining_predicate: Option<LogicalExpr>,

    pub projection: Option<Vec<usize>>,

    pub has_aggregate: bool,
    pub has_order_by: bool,
    pub has_group_by: bool,
    pub has_join: bool,
    /// DISTINCT or window functions wrap the scan: a pushed-down LIMIT must
    /// not truncate the scan, because the wrapper needs every row to dedup
    /// or to compute frames before applying its own limit.
    pub has_distinct_or_window: bool,
    pub limit: Option<usize>,
    pub order_by: Vec<ScanSortKey>,

    pub cluster_key: Option<Vec<String>>,
    pub partition_info: Option<PartitionScanInfo>,
    pub bloom_index_columns: Vec<usize>,
    pub minmax_index_columns: Vec<usize>,
    pub set_index_columns: Vec<usize>,
    pub reader_txn_id: u64,
    /// TTL read-filtering is active for this table (`table_meta.ttl` set at
    /// spec-compile time). Hot-store/memtable fast paths that truncate
    /// gather early (LIMIT pushdown, key lookups) must be disabled when this
    /// is set: TTL filters rows AFTER collection, so a pre-filter truncation
    /// would return fewer rows than exist. This flag is the single decision
    /// point — fast paths check it instead of re-deriving TTL state, so a
    /// new fast path cannot forget the gate.
    pub ttl_active: bool,
    /// Partial-residency bound when the hybrid hot-mirror leg is active:
    /// `(partition_column_index, covers_from_value)`. The mirror answers rows
    /// with `column > value` (its leg adds a `Gt` clause); every non-mirror
    /// leg gains a `Le` clause on the same column so the two sides partition
    /// the table exactly. Set only when the mirror provably covers all rows
    /// above the bound (`TableMeta::hot_mirror_covers_from`).
    pub hot_mirror_bound: Option<(usize, ScalarValue)>,

    /// Metadata-folded aggregate fast path: the scan executor answers the
    /// aggregate from row-group zone maps / footers (per fold unit, per
    /// group) instead of streaming the data. Set from the query plan via
    /// [`extract_metadata_fold`].
    pub metadata_fold: Option<MetadataFoldRequest>,

    /// DISTINCT fast path target column (name in the table schema). Set only
    /// for single-column, predicate-free `SELECT DISTINCT` scans.
    pub distinct_column: Option<String>,

    /// The catalog schema this spec was compiled against. Every column index
    /// in the spec (predicates, projection, index columns) is an index into
    /// THIS schema, while per-segment structures (footer zone maps, bloom
    /// filters, physical column blocks) follow each segment's own footer
    /// order — after a DROP/RENAME COLUMN the two drift apart. Readers
    /// translate between the two spaces per segment via
    /// `SegmentReader::align_indices` using this schema. Empty for
    /// hand-built specs (tests): consumers then assume identity mapping.
    pub table_schema: Schema,
}

#[derive(Debug, Clone)]
pub struct InPredicate {
    pub column_index: usize,
    pub values: Vec<ScalarValue>,
}

/// A foldable `Aggregate(single Scan)` request: bare-column GROUP BY keys
/// and non-DISTINCT COUNT/SUM/MIN/MAX/AVG over bare columns / wildcard, with
/// no HAVING. The folding engine (`scan::aggregate_meta`) answers it from
/// segment metadata, declining at runtime to the regular pipeline.
#[derive(Debug, Clone)]
pub struct MetadataFoldRequest {
    pub group_by: Vec<LogicalExpr>,
    pub aggregates: Vec<AggregateExpr>,
}

/// Extract the fold request for `table_name` from a query plan. The plan
/// must be `Project?(Aggregate(Scan of table_name))` where the projection is
/// positional identity over the aggregate output (`Column(i)` at position
/// `i`) — the executor returns the aggregate rows directly, so any reorder,
/// computed column or wrapper above declines here and keeps the pipeline.
/// WHERE clauses the optimizer pushed into `ScanPlan::pushed_filter` are
/// covered (they land in the compiled spec); residual `Filter` nodes carry
/// predicates the spec never sees, so those decline too.
pub fn extract_metadata_fold(plan: &LogicalPlan, table_name: &str) -> Option<MetadataFoldRequest> {
    let inner = match plan {
        LogicalPlan::Project(project) => &*project.input,
        other => other,
    };
    let LogicalPlan::Aggregate(agg) = inner else {
        return None;
    };
    if agg.having.is_some() || agg.distinct {
        return None;
    }
    if agg
        .group_by
        .iter()
        .any(|expr| !matches!(expr, LogicalExpr::Column(_)))
    {
        return None;
    }
    for aggregate in &agg.aggregates {
        if aggregate.distinct {
            return None;
        }
        match &*aggregate.arg {
            LogicalExpr::Column(_) | LogicalExpr::Wildcard => {}
            _ => return None,
        }
    }
    let LogicalPlan::Scan(scan) = &*agg.input else {
        return None;
    };
    if !scan.table_name.eq_ignore_ascii_case(table_name) {
        return None;
    }
    // The identity-projection guard: when a Project sits above the
    // Aggregate, its expressions must map the aggregate output
    // position-for-position, otherwise the rows this path returns would have
    // the wrong column order (or drop computed select items).
    if let LogicalPlan::Project(project) = plan {
        let identity = project.expressions.iter().enumerate().all(
            |(position, expr)| matches!(expr, LogicalExpr::Column(index) if *index == position),
        );
        if !identity {
            return None;
        }
    }
    Some(MetadataFoldRequest {
        group_by: agg.group_by.clone(),
        aggregates: agg.aggregates.clone(),
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanSortKey {
    pub column_index: usize,
    pub desc: bool,
}

#[derive(Debug, Clone)]
pub struct PartitionScanInfo {
    pub time_range: Option<TimeRange>,
    pub partition_filter: Option<PartitionFilter>,
    pub partition_key_col_idx: Option<usize>,
    pub is_partitioned: bool,
}

impl ScanSpec {
    pub fn new(table_name: &str) -> Self {
        Self {
            table_name: table_name.to_string(),
            eq_predicates: Vec::new(),
            range_predicates: Vec::new(),
            in_predicates: Vec::new(),
            remaining_predicate: None,
            projection: None,
            has_aggregate: false,
            has_distinct_or_window: false,
            has_order_by: false,
            has_group_by: false,
            has_join: false,
            limit: None,
            order_by: Vec::new(),
            cluster_key: None,
            partition_info: None,
            bloom_index_columns: Vec::new(),
            minmax_index_columns: Vec::new(),
            set_index_columns: Vec::new(),
            reader_txn_id: 0,
            ttl_active: false,
            hot_mirror_bound: None,
            metadata_fold: None,
            distinct_column: None,
            table_schema: Schema::empty(),
        }
    }

    pub fn has_predicates(&self) -> bool {
        !self.eq_predicates.is_empty()
            || !self.range_predicates.is_empty()
            || !self.in_predicates.is_empty()
            || self.remaining_predicate.is_some()
    }

    pub fn all_predicate_columns(&self) -> Vec<usize> {
        let mut cols = Vec::new();
        for (idx, _) in &self.eq_predicates {
            if !cols.contains(idx) {
                cols.push(*idx);
            }
        }
        for clause in &self.range_predicates {
            if !cols.contains(&clause.column_index) {
                cols.push(clause.column_index);
            }
        }
        for pred in &self.in_predicates {
            if !cols.contains(&pred.column_index) {
                cols.push(pred.column_index);
            }
        }
        if let Some(expr) = &self.remaining_predicate {
            collect_expr_columns(expr, &mut cols);
        }
        cols
    }

    pub fn eq_value_for_column(&self, col_idx: usize) -> Option<&ScalarValue> {
        self.eq_predicates
            .iter()
            .find(|(idx, _)| *idx == col_idx)
            .map(|(_, v)| v)
    }

    pub fn to_row_group_predicate(&self) -> crate::storage::partition::pruning::RowGroupPredicate {
        let mut clauses = Vec::new();
        for (idx, val) in &self.eq_predicates {
            clauses.push(RangeClause {
                column_index: *idx,
                op: crate::storage::partition::pruning::RangeOp::Eq,
                value: val.clone(),
            });
        }
        clauses.extend(self.range_predicates.iter().cloned());
        crate::storage::partition::pruning::RowGroupPredicate::new(clauses)
    }
}

pub fn compile_scan_spec(
    plan: &LogicalPlan,
    table_name: &str,
    table_schema: &Schema,
    table_meta: &TableMeta,
) -> ScanSpec {
    let mut spec = ScanSpec::new(table_name);
    spec.table_schema = table_schema.clone();
    spec.ttl_active = table_meta.ttl.is_some();

    if let LogicalPlan::Scan(scan) = plan {
        spec.distinct_column = scan.distinct_column.clone();
    }

    if !table_meta.cluster_key.is_empty() {
        spec.cluster_key = Some(table_meta.cluster_key.clone());
    }

    spec.bloom_index_columns = bloom_index_columns(table_meta, table_schema);
    spec.minmax_index_columns =
        secondary_index_columns(table_meta, table_schema, SecondaryIndexType::MinMax);
    spec.set_index_columns =
        secondary_index_columns(table_meta, table_schema, SecondaryIndexType::Set);

    if table_meta.is_partitioned() {
        let pk_idx = table_meta.get_partition_column_index();
        spec.partition_info = Some(PartitionScanInfo {
            time_range: None,
            partition_filter: None,
            partition_key_col_idx: pk_idx,
            is_partitioned: true,
        });
    }

    collect_from_plan(plan, table_name, table_schema, &mut spec);

    if spec.has_join {
        // The projection survives: join-condition side columns are merged
        // into it during traversal, and the physical join re-pads its
        // narrowed children to the full planned side layout
        // (`ExpandToSchemaExec`), so consumers keep full-width semantics.
        // A LIMIT above a join still must not push into one side.
        spec.limit = None;
    }

    spec
}

fn bloom_index_columns(table_meta: &TableMeta, table_schema: &Schema) -> Vec<usize> {
    let mut cols = secondary_index_columns(table_meta, table_schema, SecondaryIndexType::Bloom);
    for index in &table_meta.unique_indexes {
        for column in &index.columns {
            if let Some(idx) = table_schema.index_of(column) {
                if !cols.contains(&idx) {
                    cols.push(idx);
                }
            }
        }
    }
    cols
}

fn secondary_index_columns(
    table_meta: &TableMeta,
    table_schema: &Schema,
    index_type: SecondaryIndexType,
) -> Vec<usize> {
    let mut cols = Vec::new();
    for index in &table_meta.secondary_indexes {
        if index.index_type != index_type {
            continue;
        }
        for column in &index.columns {
            if let Some(idx) = table_schema.index_of(column) {
                if !cols.contains(&idx) {
                    cols.push(idx);
                }
            }
        }
    }
    cols
}

fn collect_expr_columns(expr: &LogicalExpr, cols: &mut Vec<usize>) {
    match expr {
        LogicalExpr::CorrelatedSubquery(corr) => {
            for (_, outer_idx) in &corr.params {
                if !cols.contains(outer_idx) {
                    cols.push(*outer_idx);
                }
            }
        }
        LogicalExpr::Column(idx) => {
            if !cols.contains(idx) {
                cols.push(*idx);
            }
        }
        LogicalExpr::BinaryOp { left, right, .. } => {
            collect_expr_columns(left, cols);
            collect_expr_columns(right, cols);
        }
        LogicalExpr::Not(inner) | LogicalExpr::IsNull(inner) | LogicalExpr::IsNotNull(inner) => {
            collect_expr_columns(inner, cols);
        }
        LogicalExpr::In { expr, .. } | LogicalExpr::Like { expr, .. } => {
            collect_expr_columns(expr, cols);
        }
        LogicalExpr::Between {
            expr, low, high, ..
        } => {
            collect_expr_columns(expr, cols);
            collect_expr_columns(low, cols);
            collect_expr_columns(high, cols);
        }
        LogicalExpr::Case {
            operand,
            when_clauses,
            else_clause,
        } => {
            if let Some(operand) = operand {
                collect_expr_columns(operand, cols);
            }
            for (when_expr, then_expr) in when_clauses {
                collect_expr_columns(when_expr, cols);
                collect_expr_columns(then_expr, cols);
            }
            if let Some(else_expr) = else_clause {
                collect_expr_columns(else_expr, cols);
            }
        }
        LogicalExpr::Function { args, .. } => {
            for arg in args {
                collect_expr_columns(arg, cols);
            }
        }
        LogicalExpr::Aggregate(agg) => {
            collect_expr_columns(&agg.arg, cols);
        }
        LogicalExpr::ScalarSubquery(_)
        | LogicalExpr::ExistsSubquery(_)
        | LogicalExpr::InSubquery { .. }
        | LogicalExpr::Literal(_)
        | LogicalExpr::Wildcard => {}
    }
}

fn collect_from_plan(plan: &LogicalPlan, table_name: &str, schema: &Schema, spec: &mut ScanSpec) {
    collect_from_plan_ctx(plan, table_name, schema, spec, &[]);
}

/// `join_side_cols`: side-space column indices the enclosing joins'
/// conditions read on THIS table's side — they must survive any projection
/// pushdown, because the join's condition and row assembly address the full
/// side layout.
fn collect_from_plan_ctx(
    plan: &LogicalPlan,
    table_name: &str,
    schema: &Schema,
    spec: &mut ScanSpec,
    join_side_cols: &[usize],
) {
    match plan {
        LogicalPlan::Scan(scan) => {
            if !scan.table_name.eq_ignore_ascii_case(table_name) {
                return;
            }
            if let Some(limit) = scan.pushed_limit {
                set_scan_limit(spec, limit);
            }
            if let Some(order) = &scan.pushed_order {
                spec.has_order_by = true;
                collect_sort_keys(order, schema, spec);
            }
            if let Some(filter) = &scan.pushed_filter {
                extract_predicates(filter, schema, spec);
            }
            if let Some(proj) = &scan.projection {
                spec.projection = Some(proj.clone());
            }
            if !join_side_cols.is_empty() {
                // Union the join's side columns into an EXISTING narrowing
                // only: `None` means "every column" and already includes
                // them — turning it into a join-columns-only projection
                // starved the join's other consumers (a GROUP BY above lost
                // its key column and read padded NULLs).
                if let Some(mut cols) = spec.projection.take() {
                    for col in join_side_cols {
                        if !cols.contains(col) {
                            cols.push(*col);
                        }
                    }
                    cols.sort_unstable();
                    cols.dedup();
                    spec.projection = Some(cols);
                }
            }
            if scan.time_range.is_some() || scan.partition_filter.is_some() {
                if let Some(ref mut pi) = spec.partition_info {
                    pi.time_range = scan.time_range.clone();
                    pi.partition_filter = scan.partition_filter.clone();
                }
            }
        }
        LogicalPlan::Filter(f) => {
            collect_from_plan_ctx(&f.input, table_name, schema, spec, join_side_cols)
        }
        LogicalPlan::Project(p) => {
            collect_from_plan_ctx(&p.input, table_name, schema, spec, join_side_cols);
            // Above an Aggregate, projection expressions index the aggregate
            // OUTPUT, not this table's columns; slicing the scan by them
            // would drop group-key columns (materialized pipeline only).
            if spec.remaining_predicate.is_none() && !spec.has_join && !spec.has_aggregate {
                if let Some(mut cols) = plain_projection_columns(&p.expressions) {
                    // Filters sitting between this projection and the scan
                    // still evaluate over the scan output; predicates that
                    // were not pushed into the scan (e.g. correlated
                    // subqueries) must keep their columns alive.
                    let mut filter_cols = Vec::new();
                    collect_intermediate_filter_cols(&p.input, &mut filter_cols);
                    for idx in filter_cols {
                        if !cols.contains(&idx) {
                            cols.push(idx);
                        }
                    }
                    cols.sort_unstable();
                    spec.projection = Some(cols);
                }
            }
        }
        LogicalPlan::Aggregate(a) => {
            spec.has_aggregate = true;
            spec.has_group_by = !a.group_by.is_empty();
            // Aggregate projection pushdown (materialized path): reading
            // directly off this table's scan, restrict the read to GROUP BY
            // keys + aggregate arguments — the same rule the physical
            // pipeline applies via `pushdown_aggregate_projection`. Without
            // it, the materialized path decoded the whole row width for
            // aggregate queries. Only a bare Scan input qualifies: a residual
            // Filter below the aggregate evaluates over the scan output and
            // needs its own columns.
            if let LogicalPlan::Scan(scan) = &*a.input {
                if scan.table_name.eq_ignore_ascii_case(table_name) {
                    let mut cols: Vec<usize> = Vec::new();
                    let mut bare = true;
                    for expr in &a.group_by {
                        if let LogicalExpr::Column(index) = expr {
                            cols.push(*index);
                        } else {
                            bare = false;
                            break;
                        }
                    }
                    if bare {
                        for aggregate in &a.aggregates {
                            match &*aggregate.arg {
                                LogicalExpr::Column(index) => cols.push(*index),
                                LogicalExpr::Wildcard => {}
                                _ => {
                                    bare = false;
                                    break;
                                }
                            }
                        }
                    }
                    if bare {
                        cols.sort_unstable();
                        cols.dedup();
                        if !cols.is_empty() && cols.len() < scan.schema.columns.len() {
                            spec.projection = Some(cols);
                        }
                    }
                }
            }
            collect_from_plan_ctx(&a.input, table_name, schema, spec, join_side_cols);
        }
        LogicalPlan::Sort(s) => {
            spec.has_order_by = true;
            collect_sort_keys(&s.sort_keys, schema, spec);
            collect_from_plan_ctx(&s.input, table_name, schema, spec, join_side_cols);
        }
        LogicalPlan::Limit(l) => {
            set_scan_limit(spec, l.limit.saturating_add(l.offset));
            collect_from_plan_ctx(&l.input, table_name, schema, spec, join_side_cols);
        }
        LogicalPlan::TopN(t) => {
            spec.has_order_by = true;
            collect_sort_keys(&t.sort_keys, schema, spec);
            set_scan_limit(spec, t.limit.saturating_add(t.offset));
            collect_from_plan_ctx(&t.input, table_name, schema, spec, join_side_cols);
        }
        LogicalPlan::Distinct(d) => {
            spec.has_distinct_or_window = true;
            collect_from_plan_ctx(&d.input, table_name, schema, spec, join_side_cols);
        }
        LogicalPlan::Window(w) => {
            spec.has_distinct_or_window = true;
            collect_from_plan_ctx(&w.input, table_name, schema, spec, join_side_cols);
        }
        LogicalPlan::Join(j) => {
            spec.has_join = true;
            let mut condition_cols = Vec::new();
            crate::sql::planner::expr::collect_cols_from_expr(&j.condition, &mut condition_cols);
            let left_width = j.left_schema.columns.len();
            let mut own = join_side_cols.to_vec();
            own.retain(|col| *col < schema.num_columns());
            let mut left_cols = own.clone();
            left_cols.extend(condition_cols.iter().filter(|c| **c < left_width).copied());
            let mut right_cols = own;
            right_cols.extend(
                condition_cols
                    .iter()
                    .filter(|c| **c >= left_width)
                    .map(|c| *c - left_width),
            );
            collect_from_plan_ctx(&j.left, table_name, schema, spec, &left_cols);
            collect_from_plan_ctx(&j.right, table_name, schema, spec, &right_cols);
        }
        LogicalPlan::SemiJoin(s) => {
            spec.has_join = true;
            let mut condition_cols = Vec::new();
            crate::sql::planner::expr::collect_cols_from_expr(&s.condition, &mut condition_cols);
            let left_width = s.left_schema.columns.len();
            let mut own = join_side_cols.to_vec();
            own.retain(|col| *col < schema.num_columns());
            let mut left_cols = own.clone();
            left_cols.extend(condition_cols.iter().filter(|c| **c < left_width).copied());
            let mut right_cols = own;
            right_cols.extend(
                condition_cols
                    .iter()
                    .filter(|c| **c >= left_width)
                    .map(|c| *c - left_width),
            );
            collect_from_plan_ctx(&s.left, table_name, schema, spec, &left_cols);
            collect_from_plan_ctx(&s.right, table_name, schema, spec, &right_cols);
        }
        LogicalPlan::Empty => {}
    }
}

fn set_scan_limit(spec: &mut ScanSpec, limit: usize) {
    spec.limit = Some(spec.limit.map_or(limit, |current| current.min(limit)));
}

fn collect_sort_keys(
    sort_keys: &[crate::sql::planner::SortKey],
    schema: &Schema,
    spec: &mut ScanSpec,
) {
    for key in sort_keys {
        let LogicalExpr::Column(idx) = key.expr else {
            continue;
        };
        if idx >= schema.num_columns() {
            continue;
        }
        let sort_key = ScanSortKey {
            column_index: idx,
            desc: key.desc,
        };
        if !spec.order_by.contains(&sort_key) {
            spec.order_by.push(sort_key);
        }
    }
}

/// Columns referenced by predicates that remain as explicit Filter
/// operators between `plan` and its scan (not pushed into the scan spec).
fn collect_intermediate_filter_cols(plan: &LogicalPlan, cols: &mut Vec<usize>) {
    match plan {
        LogicalPlan::Filter(f) => {
            crate::sql::planner::collect_cols_from_expr(&f.predicate, cols);
            collect_intermediate_filter_cols(&f.input, cols);
        }
        LogicalPlan::Sort(s) => {
            for key in &s.sort_keys {
                crate::sql::planner::collect_cols_from_expr(&key.expr, cols);
            }
            collect_intermediate_filter_cols(&s.input, cols);
        }
        LogicalPlan::Limit(l) => collect_intermediate_filter_cols(&l.input, cols),
        LogicalPlan::Distinct(d) => collect_intermediate_filter_cols(&d.input, cols),
        LogicalPlan::TopN(t) => {
            for key in &t.sort_keys {
                crate::sql::planner::collect_cols_from_expr(&key.expr, cols);
            }
            collect_intermediate_filter_cols(&t.input, cols);
        }
        _ => {}
    }
}

fn plain_projection_columns(expressions: &[LogicalExpr]) -> Option<Vec<usize>> {
    let mut cols = Vec::with_capacity(expressions.len());
    for expr in expressions {
        let LogicalExpr::Column(idx) = expr else {
            return None;
        };
        cols.push(*idx);
    }
    Some(cols)
}

fn extract_predicates(expr: &LogicalExpr, schema: &Schema, spec: &mut ScanSpec) {
    match expr {
        LogicalExpr::BinaryOp { left, op, right } => {
            use crate::sql::parser::ast::BinOp;
            match op {
                BinOp::And => {
                    extract_predicates(left, schema, spec);
                    extract_predicates(right, schema, spec);
                }
                BinOp::Eq => {
                    if let Some((idx, val)) = try_extract_col_literal_eq(left, right, schema) {
                        spec.eq_predicates.push((idx, val));
                    } else if let Some((idx, val)) = try_extract_col_literal_eq(right, left, schema)
                    {
                        spec.eq_predicates.push((idx, val));
                    } else {
                        set_remaining(expr.clone(), spec);
                    }
                }
                BinOp::Gt | BinOp::Gte | BinOp::Lt | BinOp::Lte => {
                    if let Some(clause) = try_extract_range_clause(left, *op, right, schema) {
                        spec.range_predicates.push(clause);
                    } else if let Some(clause) =
                        try_extract_range_clause_mirrored(left, *op, right, schema)
                    {
                        spec.range_predicates.push(clause);
                    } else {
                        set_remaining(expr.clone(), spec);
                    }
                }
                _ => {
                    set_remaining(expr.clone(), spec);
                }
            }
        }
        LogicalExpr::Between {
            expr: inner,
            low,
            high,
            negated: false,
        } => {
            if let (LogicalExpr::Column(idx), LogicalExpr::Literal(lo), LogicalExpr::Literal(hi)) =
                (&**inner, &**low, &**high)
            {
                // Coerce like eq/range literals so downstream zone/bloom/set
                // checks compare values in the column's own type and scale.
                let column = &schema.columns[*idx];
                match (
                    coerce_scalar(lo, &column.data_type, column.scale),
                    coerce_scalar(hi, &column.data_type, column.scale),
                ) {
                    (Some(lo), Some(hi)) => {
                        spec.range_predicates.push(RangeClause {
                            column_index: *idx,
                            op: crate::storage::partition::pruning::RangeOp::Ge,
                            value: lo,
                        });
                        spec.range_predicates.push(RangeClause {
                            column_index: *idx,
                            op: crate::storage::partition::pruning::RangeOp::Le,
                            value: hi,
                        });
                    }
                    _ => set_remaining(expr.clone(), spec),
                }
            } else {
                set_remaining(expr.clone(), spec);
            }
        }
        LogicalExpr::In {
            expr: inner,
            values,
            negated: false,
        } => {
            if let LogicalExpr::Column(idx) = &**inner {
                if !values.is_empty() {
                    // IN values feed bloom/set index pruning, which are exact
                    // hash/equality checks with no cross-type fallback: an
                    // Int64 literal against an Int32-built index hashes
                    // differently and prunes row groups that actually match.
                    // Coerce to the column type; if any literal cannot be
                    // coerced, keep the predicate generic instead.
                    let column = &schema.columns[*idx];
                    let mut coerced = Vec::with_capacity(values.len());
                    let mut all_coerced = true;
                    for value in values {
                        match coerce_scalar(value, &column.data_type, column.scale) {
                            Some(v) => coerced.push(v),
                            None => {
                                all_coerced = false;
                                break;
                            }
                        }
                    }
                    if all_coerced {
                        spec.in_predicates.push(InPredicate {
                            column_index: *idx,
                            values: coerced,
                        });
                    } else {
                        set_remaining(expr.clone(), spec);
                    }
                }
            } else {
                set_remaining(expr.clone(), spec);
            }
        }
        _ => {
            set_remaining(expr.clone(), spec);
        }
    }
}

fn set_remaining(expr: LogicalExpr, spec: &mut ScanSpec) {
    match &spec.remaining_predicate {
        None => spec.remaining_predicate = Some(expr),
        Some(existing) => {
            spec.remaining_predicate = Some(LogicalExpr::BinaryOp {
                left: Box::new(existing.clone()),
                op: crate::sql::parser::ast::BinOp::And,
                right: Box::new(expr),
            });
        }
    }
}

fn try_extract_col_literal_eq(
    left: &LogicalExpr,
    right: &LogicalExpr,
    schema: &Schema,
) -> Option<(usize, ScalarValue)> {
    if let LogicalExpr::Column(idx) = left {
        if let LogicalExpr::Literal(val) = right {
            let coerced = coerce_scalar(
                val,
                &schema.columns.get(*idx)?.data_type,
                schema.columns.get(*idx)?.scale,
            )?;
            return Some((*idx, coerced));
        }
    }
    None
}

fn try_extract_range_clause(
    left: &LogicalExpr,
    op: crate::sql::parser::ast::BinOp,
    right: &LogicalExpr,
    schema: &Schema,
) -> Option<RangeClause> {
    use crate::storage::partition::pruning::RangeOp;
    if let LogicalExpr::Column(idx) = left {
        if let LogicalExpr::Literal(val) = right {
            let coerced = coerce_scalar(
                val,
                &schema.columns.get(*idx)?.data_type,
                schema.columns.get(*idx)?.scale,
            )?;
            let rop = match op {
                crate::sql::parser::ast::BinOp::Eq => RangeOp::Eq,
                crate::sql::parser::ast::BinOp::Lt => RangeOp::Lt,
                crate::sql::parser::ast::BinOp::Lte => RangeOp::Le,
                crate::sql::parser::ast::BinOp::Gt => RangeOp::Gt,
                crate::sql::parser::ast::BinOp::Gte => RangeOp::Ge,
                _ => return None,
            };
            return Some(RangeClause {
                column_index: *idx,
                op: rop,
                value: coerced,
            });
        }
    }
    None
}

fn try_extract_range_clause_mirrored(
    left: &LogicalExpr,
    op: crate::sql::parser::ast::BinOp,
    right: &LogicalExpr,
    schema: &Schema,
) -> Option<RangeClause> {
    use crate::storage::partition::pruning::RangeOp;
    if let LogicalExpr::Literal(val) = left {
        if let LogicalExpr::Column(idx) = right {
            let coerced = coerce_scalar(
                val,
                &schema.columns.get(*idx)?.data_type,
                schema.columns.get(*idx)?.scale,
            )?;
            let rop = match op {
                crate::sql::parser::ast::BinOp::Eq => RangeOp::Eq,
                crate::sql::parser::ast::BinOp::Lt => RangeOp::Gt,
                crate::sql::parser::ast::BinOp::Lte => RangeOp::Ge,
                crate::sql::parser::ast::BinOp::Gt => RangeOp::Lt,
                crate::sql::parser::ast::BinOp::Gte => RangeOp::Le,
                _ => return None,
            };
            return Some(RangeClause {
                column_index: *idx,
                op: rop,
                value: coerced,
            });
        }
    }
    None
}

fn coerce_scalar(
    value: &ScalarValue,
    target_type: &crate::protocol::types::DataType,
    scale: i32,
) -> Option<ScalarValue> {
    use crate::protocol::types::DataType;
    if value.is_null() {
        return Some(ScalarValue::null_for_type(*target_type));
    }
    match (target_type, value) {
        // Bool, integer, and float coercions delegate to the single ladder
        // in `execution::helpers` that SET/INSERT and the DML predicate
        // masks share, so `WHERE flag = 'yes'` accepts exactly the literals
        // `SET flag = 'yes'` accepts. The Option contract is preserved:
        // a rejected (or unknown) combination returns None and the
        // predicate stays in the generic row-level filter instead of
        // erroring or feeding an index probe a mistyped value.
        (DataType::Bool, ScalarValue::VarChar(Some(_)))
        | (DataType::Bool, ScalarValue::Int64(Some(_)))
        | (DataType::Bool, ScalarValue::Int32(Some(_))) => {
            crate::sql::execution::helpers::coerce_scalar_for_dtype(value, DataType::Bool).ok()
        }
        (
            target @ (DataType::Int64
            | DataType::Int32
            | DataType::Int16
            | DataType::Int8
            | DataType::UInt64
            | DataType::UInt32
            | DataType::UInt16
            | DataType::UInt8),
            value,
        ) => crate::sql::execution::helpers::coerce_integer_scalar(value, *target).ok(),
        // Integer literals against float/unsigned-64 columns: without these
        // arms the literal kept its Int64 tag, the bloom/set-index hash
        // mismatched, and row groups containing matches were pruned away.
        (DataType::Float64, _) | (DataType::Float32, _) => {
            crate::sql::execution::helpers::coerce_scalar_for_dtype(value, *target_type).ok()
        }
        (DataType::Decimal, ScalarValue::Float64(Some(v))) => {
            let m = 10_i64.checked_pow(scale as u32)?;
            Some(ScalarValue::Decimal(Some((*v * m as f64).round() as i64)))
        }
        (DataType::Decimal, ScalarValue::Int64(Some(v))) => {
            let m = 10_i64.checked_pow(scale as u32)?;
            v.checked_mul(m).map(|v| ScalarValue::Decimal(Some(v)))
        }
        (dt, v) if *dt == v.data_type() => Some(v.clone()),
        // Unhandled type combinations return None: the predicate then stays
        // in the generic (row-level) filter instead of feeding an index
        // probe with a mistyped value that would prune matching row groups.
        _ => None,
    }
}

/// A scan-side row cap that survived the pushdown-safety gate. Constructed
/// only by [`pushdown_safe_limit`] — fast paths that truncate reads at a
/// LIMIT (segment scan trim, memtable shortcut, hot-store key lookup, mirror
/// legs) must take their cap from there. A hand-rolled `spec.limit` filter
/// forgets a shape (aggregate, GROUP BY, DISTINCT, window, residual
/// predicate) and silently truncates correct results.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PushdownLimit(usize);

impl PushdownLimit {
    pub fn get(self) -> usize {
        self.0
    }
}

/// The single gate for turning a plan LIMIT into a scan-side row cap.
/// `ordered_lookup` says whether THIS path can serve the ORDER BY faithfully
/// (ordered cluster scan / ordered hot lookup); when it cannot, an ORDER BY
/// LIMIT must see every row to pick the right ones.
pub(crate) fn pushdown_safe_limit(spec: &ScanSpec, ordered_lookup: bool) -> Option<PushdownLimit> {
    let limit = spec.limit?;
    if spec.has_aggregate
        || spec.has_group_by
        || spec.has_join
        || spec.has_distinct_or_window
        || spec.remaining_predicate.is_some()
    {
        return None;
    }
    if spec.has_order_by && !ordered_lookup {
        return None;
    }
    Some(PushdownLimit(limit))
}

#[cfg(test)]
#[path = "spec_test.rs"]
mod tests;
