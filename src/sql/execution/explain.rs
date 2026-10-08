use std::collections::HashMap;

use crate::common::error::{Result, TaibaiError};
use crate::common::performance_baseline::PerformanceSample;
use crate::protocol::types::DataType;
use crate::sql::parser::ast::Statement;
use crate::sql::planner::*;
use crate::storage::catalog::schema::{SecondaryIndexType, TableMeta};
use crate::storage::segment::ScanStats;
use crate::types::batch::{ColumnDef, RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

use super::engine::{Engine, ExecResult};
use super::operators::profile::{OperatorProfileSnapshot, QueryProfile};
use super::physical::planner::prepare_physical_plan;
use super::physical::PhysicalContext;
use super::query::extract_table_refs;
use crate::sql::optimizer::join_order::estimate_plan_rows;

pub fn exec_explain(
    engine: &Engine,
    db: &str,
    inner: &Statement,
    analyze: bool,
) -> Result<ExecResult> {
    let select_from = match inner {
        Statement::Select { .. } => inner,
        other => {
            return Err(TaibaiError::Sql(format!(
                "EXPLAIN currently supports SELECT only, got: {:?}",
                std::mem::discriminant(other)
            )));
        }
    };

    // Resolve subqueries / derived tables so their plans can be rendered;
    // ANALYZE would need the per-query subquery runtime during execution and
    // is not wired through here yet.
    let mut resolved: Option<super::query::ResolvedQuery> = None;
    let (select_from, derived_tables) = if super::query::stmt_has_subquery_or_derived(select_from) {
        resolved = Some(super::query::resolve_query(engine, db, select_from)?);
        let query = resolved.as_ref().expect("just assigned");
        (&query.stmt as &Statement, Some(&query.derived))
    } else {
        (select_from, None)
    };

    let from_clause = match select_from {
        Statement::Select { from, .. } => from.as_ref(),
        _ => None,
    };
    let table_refs = from_clause
        .map(|from| extract_table_refs(from, db))
        .unwrap_or_default();
    let mut table_schemas = HashMap::new();
    let mut table_metas = HashMap::new();
    {
        let catalogs = engine.catalogs().read().unwrap();
        for table_ref in &table_refs {
            if table_ref.database == super::query::DERIVED_DB {
                let derived = derived_tables.expect("derived tables present");
                let (schema, _) = derived.get(&table_ref.key).ok_or_else(|| {
                    TaibaiError::Internal(format!(
                        "missing materialized derived table '{}'",
                        table_ref.key
                    ))
                })?;
                table_schemas.insert(table_ref.key.clone(), schema.clone());
                continue;
            }
            let catalog = catalogs.get(&table_ref.database).ok_or_else(|| {
                TaibaiError::Sql(format!("database '{}' does not exist", table_ref.database))
            })?;
            let meta = catalog.get_table(&table_ref.table).ok_or_else(|| {
                TaibaiError::Sql(format!(
                    "table '{}.{}' does not exist",
                    table_ref.database, table_ref.table
                ))
            })?;
            table_schemas.insert(table_ref.key.clone(), Schema::new(meta.columns.clone()));
            table_metas.insert(table_ref.key.clone(), meta.clone());
        }
    }

    let plan = plan_select(select_from, &table_schemas)?;
    let plan = crate::sql::optimizer::optimize(plan);
    let mut extra_batches: HashMap<String, Vec<std::sync::Arc<RecordBatch>>> = HashMap::new();
    let plan = if let Some(query) = &resolved {
        let outcome = super::decorrelate::decorrelate(plan, &query.registry);
        extra_batches = outcome.extra_batches;
        outcome.plan
    } else {
        plan
    };
    let plan = reconcile_plan_indexes(plan, &table_metas);
    let query_shape = plan_shape(&plan);
    let fingerprint = stable_fingerprint(&query_shape);
    let physical_ctx = PhysicalContext::new(engine, &table_refs, &table_schemas, &table_metas);
    let plan = prepare_physical_plan(plan, &physical_ctx);

    let mut out = String::new();
    render_plan(&plan, 0, &mut out, &physical_ctx);

    if analyze {
        let mut stats: HashMap<String, ScanStats> = HashMap::new();
        let profile = QueryProfile::default();
        let start = std::time::Instant::now();
        let pre = resolved
            .as_ref()
            .map(|query| super::query::PreResolvedExec {
                resolved: query,
                extra_batches: &extra_batches,
            });
        let result = super::query::exec_select_with_profile(
            engine,
            db,
            select_from,
            &mut stats,
            plan.clone(),
            &profile,
            pre,
        )?;
        let elapsed = start.elapsed();
        let rows_returned = match &result {
            ExecResult::Query { batches, .. } => {
                batches.iter().map(|b| b.num_rows()).sum::<usize>()
            }
            _ => 0,
        };
        out.push_str("\n=== ANALYZE ===\n");
        let mut tables: Vec<&String> = stats.keys().collect();
        tables.sort();
        for t in tables {
            let s = &stats[t];
            // A fast-path-only entry (resident shortcut / metadata fold
            // answered, nothing scanned) must not render the counter block:
            // the resident-shortcut contract is asserted by the ABSENCE of
            // the segments line — no segment scan happened, so no scan
            // counters exist for this table.
            let has_activity = s.segments_opened > 0
                || s.segments_pruned_by_cluster_key > 0
                || s.row_groups_total > 0
                || s.row_groups_read > 0
                || s.row_groups_pruned > 0
                || s.rows_scanned > 0
                || s.rows_filtered_by_delete_bitmap > 0
                || s.bytes_read_compressed > 0
                || s.hot_mirror_rows > 0;
            if !has_activity && s.fast_path_answered.is_none() && s.fast_path_declined.is_empty() {
                continue;
            }
            out.push_str(&format!("Scan {}:\n", t));
            if let Some(node) = s.fast_path_answered {
                out.push_str(&format!("  fast_path     : answered by {node}\n"));
            }
            if !s.fast_path_declined.is_empty() {
                out.push_str(&format!(
                    "  fast_path     : declined by {}\n",
                    s.fast_path_declined.join(", ")
                ));
            }
            if !has_activity {
                continue;
            }
            out.push_str(&format!("  segments      : {}\n", s.segments_opened));
            if s.hot_mirror_rows > 0 {
                out.push_str(&format!(
                    "  hot_mirror    : {} rows served by the hybrid resident mirror\n",
                    s.hot_mirror_rows
                ));
            }
            if s.segments_pruned_by_cluster_key > 0 {
                out.push_str(&format!(
                    "  segment_prune : {} by cluster key\n",
                    s.segments_pruned_by_cluster_key
                ));
            }
            let pruned_pct = if s.row_groups_total > 0 {
                (s.row_groups_pruned as f64 / s.row_groups_total as f64) * 100.0
            } else {
                0.0
            };
            out.push_str(&format!(
                "  row_groups    : {} total, {} read, {} pruned ({:.1}%)\n",
                s.row_groups_total, s.row_groups_read, s.row_groups_pruned, pruned_pct
            ));
            out.push_str(&format!(
                "  rows          : {} scanned, {} filtered by delete bitmap\n",
                s.rows_scanned, s.rows_filtered_by_delete_bitmap
            ));
            out.push_str(&format!(
                "  bytes_read    : {}\n",
                format_bytes(s.bytes_read_compressed)
            ));
        }
        let profiles = profile.snapshots();
        render_operator_profiles(&profiles, &mut out);
        let baseline = engine.performance_baselines().record(
            db,
            &fingerprint,
            &truncate_shape(&query_shape),
            performance_sample(elapsed, &profiles),
        );
        render_performance_baseline(&baseline, &mut out);
        out.push_str(&format!("rows_returned    : {}\n", rows_returned));
        out.push_str(&format!(
            "execution_time   : {:.3} ms\n",
            elapsed.as_secs_f64() * 1000.0
        ));
    }

    let schema = Schema::new(vec![ColumnDef::new("plan", DataType::VarChar)]);
    let mut batch = RecordBatch::new(schema.clone());
    for line in out.lines() {
        batch
            .column_mut(0)
            .unwrap()
            .push(&ScalarValue::VarChar(Some(line.to_string())))?;
    }

    Ok(ExecResult::Query {
        schema,
        batches: vec![batch],
    })
}

fn performance_sample(
    elapsed: std::time::Duration,
    profiles: &[OperatorProfileSnapshot],
) -> PerformanceSample {
    PerformanceSample {
        elapsed_ms: elapsed.as_secs_f64() * 1000.0,
        peak_memory_bytes: profiles
            .iter()
            .map(|profile| profile.peak_memory_bytes)
            .max()
            .unwrap_or(0),
        spill_bytes: profiles.iter().map(|profile| profile.spill_bytes).sum(),
        row_error: profiles.iter().map(cardinality_error).fold(1.0, f64::max),
    }
}

fn cardinality_error(profile: &OperatorProfileSnapshot) -> f64 {
    match (profile.estimated_rows, profile.actual_rows) {
        (0, 0) => 1.0,
        (0, actual) => actual.saturating_add(1) as f64,
        (estimated, 0) => estimated.saturating_add(1) as f64,
        (estimated, actual) => {
            let ratio = actual as f64 / estimated as f64;
            ratio.max(1.0 / ratio)
        }
    }
}

fn render_performance_baseline(
    baseline: &crate::common::performance_baseline::PerformanceBaselineEntry,
    out: &mut String,
) {
    out.push_str("\n=== PERFORMANCE BASELINE ===\n");
    out.push_str(&format!("fingerprint      : {}\n", baseline.fingerprint));
    out.push_str(&format!("environment      : {}\n", baseline.environment));
    out.push_str(&format!("version          : {}\n", baseline.version));
    out.push_str(&format!("samples          : {}\n", baseline.samples));
    out.push_str(&format!("status           : {}\n", baseline.status));
    out.push_str(&format!(
        "elapsed          : {:.3} ms baseline, {:.3} ms current\n",
        baseline.baseline_elapsed_ms, baseline.last_elapsed_ms
    ));
    out.push_str(&format!(
        "peak_memory      : {} baseline, {} current\n",
        format_bytes(baseline.baseline_peak_memory_bytes.round().max(0.0) as u64),
        format_bytes(baseline.last_peak_memory_bytes)
    ));
    out.push_str(&format!(
        "spill_bytes      : {} baseline, {} current\n",
        format_bytes(baseline.baseline_spill_bytes.round().max(0.0) as u64),
        format_bytes(baseline.last_spill_bytes)
    ));
    out.push_str(&format!(
        "row_error        : {:.2}x baseline, {:.2}x current\n",
        baseline.baseline_row_error, baseline.last_row_error
    ));
    if !baseline.regression_reasons.is_empty() {
        out.push_str(&format!(
            "regressions      : {}\n",
            baseline.regression_reasons.join(",")
        ));
    }
}

fn stable_fingerprint(shape: &str) -> String {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in shape.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
}

fn truncate_shape(shape: &str) -> String {
    const MAX_CHARS: usize = 512;
    let mut output = shape.chars().take(MAX_CHARS).collect::<String>();
    if shape.chars().count() > MAX_CHARS {
        output.push_str("...");
    }
    output
}

fn plan_shape(plan: &LogicalPlan) -> String {
    match plan {
        LogicalPlan::Scan(scan) => format!(
            "Scan({};projection={:?};filter={})",
            scan.table_name,
            scan.projection,
            scan.pushed_filter
                .as_ref()
                .map(expr_shape)
                .unwrap_or_else(|| "none".to_string())
        ),
        LogicalPlan::Filter(filter) => format!(
            "Filter({};{})",
            expr_shape(&filter.predicate),
            plan_shape(&filter.input)
        ),
        LogicalPlan::Project(project) => format!(
            "Project([{}];{})",
            project
                .expressions
                .iter()
                .map(expr_shape)
                .collect::<Vec<_>>()
                .join(","),
            plan_shape(&project.input)
        ),
        LogicalPlan::Aggregate(aggregate) => format!(
            "Aggregate(groups=[{}];aggs={};{})",
            aggregate
                .group_by
                .iter()
                .map(expr_shape)
                .collect::<Vec<_>>()
                .join(","),
            aggregate.aggregates.len(),
            plan_shape(&aggregate.input)
        ),
        LogicalPlan::Sort(sort) => {
            format!("Sort({};{})", sort.sort_keys.len(), plan_shape(&sort.input))
        }
        LogicalPlan::Limit(limit) => format!("Limit({};{})", limit.limit, plan_shape(&limit.input)),
        LogicalPlan::TopN(topn) => format!("TopN({};{})", topn.limit, plan_shape(&topn.input)),
        LogicalPlan::Distinct(distinct) => format!("Distinct({})", plan_shape(&distinct.input)),
        LogicalPlan::Window(window) => format!(
            "Window({};{})",
            window.funcs.len(),
            plan_shape(&window.input)
        ),
        LogicalPlan::Join(join) => format!(
            "Join({:?};{};{};{})",
            join.join_type,
            expr_shape(&join.condition),
            plan_shape(&join.left),
            plan_shape(&join.right)
        ),
        LogicalPlan::SemiJoin(join) => format!(
            "SemiJoin({};{};{})",
            join.is_anti,
            plan_shape(&join.left),
            plan_shape(&join.right)
        ),
        LogicalPlan::Empty => "Empty".to_string(),
    }
}

fn expr_shape(expr: &LogicalExpr) -> String {
    match expr {
        LogicalExpr::Column(column) => format!("c{column}"),
        LogicalExpr::Literal(_) => "?".to_string(),
        LogicalExpr::BinaryOp { left, op, right } => {
            format!("({}{op:?}{})", expr_shape(left), expr_shape(right))
        }
        LogicalExpr::Not(expr) => format!("Not({})", expr_shape(expr)),
        LogicalExpr::IsNull(expr) => format!("IsNull({})", expr_shape(expr)),
        LogicalExpr::IsNotNull(expr) => format!("IsNotNull({})", expr_shape(expr)),
        LogicalExpr::CorrelatedSubquery(_) => "correlated_subquery".to_string(),
        LogicalExpr::In { expr, negated, .. } => {
            format!("In({};negated={negated})", expr_shape(expr))
        }
        LogicalExpr::Between { expr, negated, .. } => {
            format!("Between({};negated={negated})", expr_shape(expr))
        }
        LogicalExpr::Like { expr, negated, .. } => {
            format!("Like({};negated={negated})", expr_shape(expr))
        }
        LogicalExpr::Case { .. } => "Case".to_string(),
        LogicalExpr::Function { name, args } => format!(
            "Function({};[{}])",
            name.to_ascii_lowercase(),
            args.iter().map(expr_shape).collect::<Vec<_>>().join(",")
        ),
        LogicalExpr::Aggregate(aggregate) => {
            format!(
                "AggregateExpr({:?};{})",
                aggregate.func,
                expr_shape(&aggregate.arg)
            )
        }
        LogicalExpr::Wildcard => "*".to_string(),
        LogicalExpr::ScalarSubquery(_) => "ScalarSubquery".to_string(),
        LogicalExpr::ExistsSubquery(_) => "ExistsSubquery".to_string(),
        LogicalExpr::InSubquery { expr, negated, .. } => {
            format!("InSubquery({};negated={negated})", expr_shape(expr))
        }
    }
}

fn render_operator_profiles(profiles: &[OperatorProfileSnapshot], out: &mut String) {
    out.push_str("\n=== OPERATOR METRICS ===\n");
    out.push_str(
        "elapsed_ms includes child calls; Scan also includes storage read time; \
         peak_memory is an operator estimate\n",
    );
    for profile in profiles {
        let pad = "  ".repeat(profile.depth);
        let row_ratio = if profile.estimated_rows == 0 {
            if profile.actual_rows == 0 {
                "1.00x".to_string()
            } else {
                "inf".to_string()
            }
        } else {
            format!(
                "{:.2}x",
                profile.actual_rows as f64 / profile.estimated_rows as f64
            )
        };
        out.push_str(&format!(
            "{pad}#{} {}: estimated_rows={}, actual_rows={}, row_ratio={}, batches={}, \
             elapsed_ms={:.3}, peak_memory={}, spill_files={}, spill_rows={}, spill_bytes={}\n",
            profile.id,
            profile.name,
            profile.estimated_rows,
            profile.actual_rows,
            row_ratio,
            profile.output_batches,
            profile.elapsed.as_secs_f64() * 1000.0,
            format_bytes(profile.peak_memory_bytes),
            profile.spill_files,
            profile.spill_rows,
            format_bytes(profile.spill_bytes),
        ));
    }
}

fn format_bytes(b: u64) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = KB * 1024.0;
    const GB: f64 = MB * 1024.0;
    let bf = b as f64;
    if bf >= GB {
        format!("{:.2} GB", bf / GB)
    } else if bf >= MB {
        format!("{:.2} MB", bf / MB)
    } else if bf >= KB {
        format!("{:.2} KB", bf / KB)
    } else {
        format!("{} B", b)
    }
}

fn render_plan(plan: &LogicalPlan, depth: usize, out: &mut String, ctx: &PhysicalContext<'_>) {
    let pad: String = "  ".repeat(depth);
    let estimated_rows = estimate_plan_rows(plan, ctx);
    match plan {
        LogicalPlan::Scan(scan) => {
            out.push_str(&format!("{}Scan: table={}", pad, scan.table_name));
            if let Some(proj) = &scan.projection {
                out.push_str(&format!(", projection={:?}", proj));
            }
            if let Some(limit) = scan.pushed_limit {
                out.push_str(&format!(", pushed_limit={}", limit));
            }
            out.push_str(&format!(", estimated_rows={estimated_rows}"));
            out.push('\n');
            if let Some(f) = &scan.pushed_filter {
                out.push_str(&format!(
                    "{}  pushed_filter: {}\n",
                    pad,
                    render_expr(f, &scan.schema)
                ));
            }
            if !scan.zone_map_filters.is_empty() {
                out.push_str(&format!("{}  zone_map_filters:\n", pad));
                for zmf in &scan.zone_map_filters {
                    out.push_str(&format!(
                        "{}    - {} in [{:?}, {:?}]\n",
                        pad, zmf.column, zmf.min_value, zmf.max_value
                    ));
                }
            }
            if !scan.index_columns.is_empty() {
                out.push_str(&format!(
                    "{}  index_columns: {:?}\n",
                    pad, scan.index_columns
                ));
            }
            if !scan.bloom_filter_columns.is_empty() {
                out.push_str(&format!(
                    "{}  bloom_filter_columns: {:?}\n",
                    pad, scan.bloom_filter_columns
                ));
            }
            if !scan.minmax_index_columns.is_empty() {
                out.push_str(&format!(
                    "{}  minmax_index_columns: {:?}\n",
                    pad, scan.minmax_index_columns
                ));
            }
            if !scan.set_index_columns.is_empty() {
                out.push_str(&format!(
                    "{}  set_index_columns: {:?}\n",
                    pad, scan.set_index_columns
                ));
            }
        }
        LogicalPlan::Filter(f) => {
            out.push_str(&format!(
                "{}Filter: {}, estimated_rows={}\n",
                pad,
                render_expr(&f.predicate, &get_input_schema(&f.input)),
                estimated_rows
            ));
            render_plan(&f.input, depth + 1, out, ctx);
        }
        LogicalPlan::Project(p) => {
            out.push_str(&format!(
                "{}Project: {} exprs, estimated_rows={}\n",
                pad,
                p.expressions.len(),
                estimated_rows
            ));
            render_plan(&p.input, depth + 1, out, ctx);
        }
        LogicalPlan::Aggregate(a) => {
            out.push_str(&format!(
                "{}Aggregate: group_by={}, aggs={}{} estimated_rows={}\n",
                pad,
                a.group_by.len(),
                a.aggregates.len(),
                if a.distinct { " DISTINCT," } else { "," },
                estimated_rows
            ));
            if a.having.is_some() {
                out.push_str(&format!("{}  having: yes\n", pad));
            }
            render_plan(&a.input, depth + 1, out, ctx);
        }
        LogicalPlan::Sort(s) => {
            out.push_str(&format!(
                "{}Sort: {} keys, estimated_rows={}\n",
                pad,
                s.sort_keys.len(),
                estimated_rows
            ));
            render_plan(&s.input, depth + 1, out, ctx);
        }
        LogicalPlan::Limit(l) => {
            out.push_str(&format!(
                "{}Limit: {}, offset={}, estimated_rows={}\n",
                pad, l.limit, l.offset, estimated_rows
            ));
            render_plan(&l.input, depth + 1, out, ctx);
        }
        LogicalPlan::TopN(t) => {
            out.push_str(&format!(
                "{}TopN: limit={}, offset={}, sort_keys={}, estimated_rows={}\n",
                pad,
                t.limit,
                t.offset,
                t.sort_keys.len(),
                estimated_rows
            ));
            render_plan(&t.input, depth + 1, out, ctx);
        }
        LogicalPlan::Distinct(d) => {
            out.push_str(&format!(
                "{}Distinct: estimated_rows={}\n",
                pad, estimated_rows
            ));
            render_plan(&d.input, depth + 1, out, ctx);
        }
        LogicalPlan::Window(w) => {
            out.push_str(&format!(
                "{}Window: funcs={}, estimated_rows={}\n",
                pad,
                w.funcs.len(),
                estimated_rows
            ));
            render_plan(&w.input, depth + 1, out, ctx);
        }
        LogicalPlan::Join(j) => {
            out.push_str(&format!(
                "{}Join: {:?}, estimated_rows={}\n",
                pad, j.join_type, estimated_rows
            ));
            out.push_str(&format!("{}  left:\n", pad));
            render_plan(&j.left, depth + 2, out, ctx);
            out.push_str(&format!("{}  right:\n", pad));
            render_plan(&j.right, depth + 2, out, ctx);
        }
        LogicalPlan::SemiJoin(s) => {
            out.push_str(&format!(
                "{}SemiJoin: anti={}, estimated_rows={}\n",
                pad, s.is_anti, estimated_rows
            ));
            out.push_str(&format!("{}  left:\n", pad));
            render_plan(&s.left, depth + 2, out, ctx);
            out.push_str(&format!("{}  right:\n", pad));
            render_plan(&s.right, depth + 2, out, ctx);
        }
        LogicalPlan::Empty => {
            out.push_str(&format!(
                "{}Empty: estimated_rows={}\n",
                pad, estimated_rows
            ));
        }
    }
}

fn reconcile_plan_indexes(
    plan: LogicalPlan,
    table_metas: &HashMap<String, TableMeta>,
) -> LogicalPlan {
    match plan {
        LogicalPlan::Scan(mut scan) => {
            if let Some(meta) = table_metas.get(&scan.table_name) {
                let all_indexed = all_indexed_columns(meta);
                let bloom_indexed = bloom_indexed_columns(meta);
                let minmax_indexed = meta.indexed_columns_by_type(SecondaryIndexType::MinMax);
                let set_indexed = meta.indexed_columns_by_type(SecondaryIndexType::Set);
                scan.index_columns
                    .retain(|col| contains_column(&all_indexed, col));
                scan.bloom_filter_columns
                    .retain(|col| contains_column(&bloom_indexed, col));
                scan.minmax_index_columns = scan
                    .index_columns
                    .iter()
                    .filter(|col| contains_column(&minmax_indexed, col))
                    .cloned()
                    .collect();
                scan.set_index_columns = scan
                    .index_columns
                    .iter()
                    .filter(|col| contains_column(&set_indexed, col))
                    .cloned()
                    .collect();
            } else {
                scan.index_columns.clear();
                scan.bloom_filter_columns.clear();
                scan.minmax_index_columns.clear();
                scan.set_index_columns.clear();
            }
            LogicalPlan::Scan(scan)
        }
        LogicalPlan::Filter(mut filter) => {
            filter.input = Box::new(reconcile_plan_indexes(*filter.input, table_metas));
            LogicalPlan::Filter(filter)
        }
        LogicalPlan::Project(mut project) => {
            project.input = Box::new(reconcile_plan_indexes(*project.input, table_metas));
            LogicalPlan::Project(project)
        }
        LogicalPlan::Aggregate(mut aggregate) => {
            aggregate.input = Box::new(reconcile_plan_indexes(*aggregate.input, table_metas));
            LogicalPlan::Aggregate(aggregate)
        }
        LogicalPlan::Sort(mut sort) => {
            sort.input = Box::new(reconcile_plan_indexes(*sort.input, table_metas));
            LogicalPlan::Sort(sort)
        }
        LogicalPlan::Limit(mut limit) => {
            limit.input = Box::new(reconcile_plan_indexes(*limit.input, table_metas));
            LogicalPlan::Limit(limit)
        }
        LogicalPlan::TopN(mut topn) => {
            topn.input = Box::new(reconcile_plan_indexes(*topn.input, table_metas));
            LogicalPlan::TopN(topn)
        }
        LogicalPlan::Distinct(mut distinct) => {
            distinct.input = Box::new(reconcile_plan_indexes(*distinct.input, table_metas));
            LogicalPlan::Distinct(distinct)
        }
        LogicalPlan::Window(mut window) => {
            window.input = Box::new(reconcile_plan_indexes(*window.input, table_metas));
            LogicalPlan::Window(window)
        }
        LogicalPlan::Join(mut join) => {
            join.left = Box::new(reconcile_plan_indexes(*join.left, table_metas));
            join.right = Box::new(reconcile_plan_indexes(*join.right, table_metas));
            LogicalPlan::Join(join)
        }
        LogicalPlan::SemiJoin(mut join) => {
            join.left = Box::new(reconcile_plan_indexes(*join.left, table_metas));
            join.right = Box::new(reconcile_plan_indexes(*join.right, table_metas));
            LogicalPlan::SemiJoin(join)
        }
        LogicalPlan::Empty => LogicalPlan::Empty,
    }
}

fn all_indexed_columns(meta: &TableMeta) -> Vec<String> {
    let mut columns = bloom_indexed_columns(meta);
    for column in meta.indexed_columns_by_type(SecondaryIndexType::MinMax) {
        if !contains_column(&columns, &column) {
            columns.push(column);
        }
    }
    for column in meta.indexed_columns_by_type(SecondaryIndexType::Set) {
        if !contains_column(&columns, &column) {
            columns.push(column);
        }
    }
    columns
}

fn bloom_indexed_columns(meta: &TableMeta) -> Vec<String> {
    let mut columns = meta.indexed_columns_by_type(SecondaryIndexType::Bloom);
    for index in &meta.unique_indexes {
        for column in &index.columns {
            if !contains_column(&columns, column) {
                columns.push(column.clone());
            }
        }
    }
    columns
}

fn contains_column(columns: &[String], column: &str) -> bool {
    columns.iter().any(|c| c.eq_ignore_ascii_case(column))
}

fn get_input_schema(plan: &LogicalPlan) -> Schema {
    match plan {
        LogicalPlan::Scan(s) => s.schema.clone(),
        LogicalPlan::Filter(f) => get_input_schema(&f.input),
        LogicalPlan::Project(p) => get_input_schema(&p.input),
        LogicalPlan::Aggregate(a) => get_input_schema(&a.input),
        LogicalPlan::Sort(s) => get_input_schema(&s.input),
        LogicalPlan::Limit(l) => get_input_schema(&l.input),
        LogicalPlan::TopN(t) => get_input_schema(&t.input),
        LogicalPlan::Distinct(d) => get_input_schema(&d.input),
        LogicalPlan::Window(w) => get_input_schema(&w.input),
        LogicalPlan::Join(j) => j.output_schema.clone(),
        LogicalPlan::SemiJoin(s) => s.left_schema.clone(),
        LogicalPlan::Empty => Schema::empty(),
    }
}

fn render_expr(expr: &LogicalExpr, schema: &Schema) -> String {
    use crate::sql::parser::ast::BinOp;
    match expr {
        LogicalExpr::Column(idx) => schema
            .columns
            .get(*idx)
            .map(|c| c.name.clone())
            .unwrap_or_else(|| format!("col#{}", idx)),
        LogicalExpr::CorrelatedSubquery(_) => "correlated subquery".to_string(),
        LogicalExpr::Literal(v) => format!("{:?}", v),
        LogicalExpr::BinaryOp { left, op, right } => {
            let opstr = match op {
                BinOp::Eq => "=",
                BinOp::Neq => "!=",
                BinOp::Lt => "<",
                BinOp::Lte => "<=",
                BinOp::Gt => ">",
                BinOp::Gte => ">=",
                BinOp::And => "AND",
                BinOp::Or => "OR",
                BinOp::Add => "+",
                BinOp::Sub => "-",
                BinOp::Mul => "*",
                BinOp::Div => "/",
                BinOp::Mod => "%",
                BinOp::Like => "LIKE",
                BinOp::NotLike => "NOT LIKE",
                BinOp::In => "IN",
                BinOp::NotIn => "NOT IN",
            };
            format!(
                "({} {} {})",
                render_expr(left, schema),
                opstr,
                render_expr(right, schema)
            )
        }
        LogicalExpr::Not(e) => format!("NOT ({})", render_expr(e, schema)),
        LogicalExpr::IsNull(e) => format!("{} IS NULL", render_expr(e, schema)),
        LogicalExpr::IsNotNull(e) => format!("{} IS NOT NULL", render_expr(e, schema)),
        LogicalExpr::In {
            expr,
            values,
            negated,
        } => format!(
            "{} {}IN ({} values)",
            render_expr(expr, schema),
            if *negated { "NOT " } else { "" },
            values.len()
        ),
        LogicalExpr::Between {
            expr,
            low,
            high,
            negated,
        } => format!(
            "{} {}BETWEEN {} AND {}",
            render_expr(expr, schema),
            if *negated { "NOT " } else { "" },
            render_expr(low, schema),
            render_expr(high, schema)
        ),
        LogicalExpr::Like {
            expr,
            pattern,
            negated,
        } => format!(
            "{} {}LIKE {:?}",
            render_expr(expr, schema),
            if *negated { "NOT " } else { "" },
            pattern
        ),
        LogicalExpr::Case { .. } => "CASE ...".to_string(),
        LogicalExpr::Function { name, args } => format!("{}({} args)", name, args.len()),
        LogicalExpr::Aggregate(a) => format!("{:?}({})", a.func, render_expr(&a.arg, schema)),
        LogicalExpr::Wildcard => "*".to_string(),
        LogicalExpr::ScalarSubquery(_) => "(subquery)".to_string(),
        LogicalExpr::ExistsSubquery(_) => "EXISTS(subquery)".to_string(),
        LogicalExpr::InSubquery { expr, negated, .. } => format!(
            "{} {}IN (subquery)",
            render_expr(expr, schema),
            if *negated { "NOT " } else { "" }
        ),
    }
}
