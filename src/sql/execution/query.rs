use std::collections::HashMap;

use crate::common::error::{MonaxError, Result};
use crate::protocol::types::DataType;
use crate::storage::memtable::MemTableState;
use crate::storage::segment::{RangeClause, RangeOp, RowGroupPredicate, ScanStats};
use crate::storage::segment_manager::SegmentManager;
use crate::types::batch::{ColumnDef, RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

use super::engine::{Engine, ExecResult};
use crate::sql::parser::ast::{BinOp, Expr, FromClause, SelectItem, Statement};
use crate::sql::planner::{LogicalExpr, LogicalPlan, ScanPlan};

pub fn exec_select(engine: &Engine, db: &str, stmt: &Statement) -> Result<ExecResult> {
    exec_select_inner(engine, db, stmt, None, None)
}

/// Execute a SELECT using a cached logical plan, skipping re-planning.
pub fn exec_select_with_plan(
    engine: &Engine,
    db: &str,
    stmt: &Statement,
    cached_plan: LogicalPlan,
) -> Result<ExecResult> {
    exec_select_inner(engine, db, stmt, None, Some(cached_plan))
}

/// Same as [exec_select], but populates per-table [ScanStats] in `stats`.
/// Used by EXPLAIN ANALYZE.
pub fn exec_select_with_stats(
    engine: &Engine,
    db: &str,
    stmt: &Statement,
    stats: &mut HashMap<String, ScanStats>,
) -> Result<ExecResult> {
    exec_select_inner(engine, db, stmt, Some(stats), None)
}

fn exec_select_inner(
    engine: &Engine,
    db: &str,
    stmt: &Statement,
    mut stats: Option<&mut HashMap<String, ScanStats>>,
    cached_plan: Option<LogicalPlan>,
) -> Result<ExecResult> {
    let from = match stmt {
        Statement::Select { from, .. } => from,
        _ => return Err(MonaxError::Internal("exec_select called with non-SELECT".into())),
    };

    match from {
        None => exec_literal_select(stmt),
        Some(from_clause) => exec_table_select(engine, db, stmt, from_clause, stats.as_deref_mut(), cached_plan),
    }
}

fn exec_literal_select(stmt: &Statement) -> Result<ExecResult> {
    let select_list = match stmt {
        Statement::Select { select_list, .. } => select_list,
        _ => unreachable!(),
    };
    let schema = Schema::new(vec![ColumnDef::new("1", DataType::Int64)]);
    let mut batch = RecordBatch::new(schema.clone());
    match &select_list[0] {
        SelectItem::Expr(expr, _) => {
            let val = super::helpers::eval_expr_to_scalar(expr, &DataType::Int64)?;
            batch.column_mut(0).unwrap().push(&val)?;
        }
        SelectItem::Star => {
            batch.column_mut(0).unwrap().push(&ScalarValue::Int64(Some(1)))?;
        }
    }
    Ok(ExecResult::Query { schema, batches: vec![batch] })
}

fn exec_table_select(
    engine: &Engine,
    db: &str,
    stmt: &Statement,
    from_clause: &FromClause,
    mut stats: Option<&mut HashMap<String, ScanStats>>,
    cached_plan: Option<LogicalPlan>,
) -> Result<ExecResult> {
    let table_names = extract_table_names(from_clause);

    if cached_plan.is_none() {
        if let Some(result) = try_count_star_fast_path(engine, db, stmt, &table_names) {
            return result;
        }
        if let Some(result) = try_fast_scan_eq_unified(engine, db, stmt, &table_names) {
            return result;
        }
    }

    let mut table_schemas = HashMap::new();
    let mut table_cluster_keys: HashMap<String, Vec<String>> = HashMap::new();
    {
        let catalogs = engine.catalogs().read().unwrap();
        let catalog = catalogs.get(db)
            .ok_or_else(|| MonaxError::Sql(format!("database '{}' does not exist", db)))?;

        for table_name in &table_names {
            let meta = catalog.get_table(table_name)
                .ok_or_else(|| MonaxError::Sql(format!("table '{}.{}' does not exist", db, table_name)))?
                .clone();
            table_schemas.insert(table_name.clone(), Schema::new(meta.columns.clone()));
            if !meta.cluster_key.is_empty() {
                table_cluster_keys.insert(table_name.clone(), meta.cluster_key.clone());
            }
        }
    }

    let plan = match cached_plan {
        Some(p) => p,
        None => {
            let plan = crate::sql::planner::plan_select(stmt, &table_schemas)?;
            crate::sql::optimizer::optimize(plan)
        }
    };

    let mut scan_predicates: HashMap<String, RowGroupPredicate> = HashMap::new();
    collect_scan_predicates(&plan, &mut scan_predicates);

    let mut scan_projections: HashMap<String, Vec<usize>> = HashMap::new();
    collect_scan_projections(&plan, &mut scan_projections);

    let mut table_batches: HashMap<String, Vec<RecordBatch>> = HashMap::new();
    let mgr = engine.segment_mgr();
    for table_name in &table_names {
        let table_dir = engine.data_dir().join(db).join(table_name);
        let mut batches = Vec::new();
        let predicate = scan_predicates.get(table_name);
        let pred_ref = predicate.filter(|p| !p.is_empty());
        let projection = scan_projections.get(table_name);
        if table_dir.exists() {
            let segment_files = SegmentManager::discover_segments(&table_dir)?;
            if let Some(p) = pred_ref {
                tracing::debug!(
                    table = %table_name,
                    clauses = p.clauses.len(),
                    "applying zone-map pruning to table scan"
                );
            }
            for seg_path in segment_files {
                let reader = mgr.open_reader(&seg_path)?;
                let table_stats = stats.as_deref_mut()
                    .map(|s| s.entry(table_name.clone()).or_default());
                batches.extend(reader.scan_with_projection_and_stats(4096, pred_ref, projection.as_deref().map(|v| &**v), table_stats)?);
            }
        }

        let memtables = engine.memtable_manager().get_by_table(db, table_name);
        for mt in memtables {
            if let Some(mt_batch) = mt.read() {
                if mt_batch.num_rows() > 0 {
                    let mt_batch = if let Some(proj) = projection {
                        apply_projection_to_batch(&mt_batch, proj)
                    } else {
                        mt_batch
                    };
                    if let Some(pred) = pred_ref {
                        let filtered = super::helpers::apply_row_group_predicate(&mt_batch, pred);
                        if !filtered.is_empty() {
                            batches.extend(filtered);
                        }
                    } else {
                        batches.push(mt_batch);
                    }
                }
            }
        }

        if let Some(cluster_key) = table_cluster_keys.get(table_name) {
            if let Some(key_col) = cluster_key.first() {
                if let Some(schema) = table_schemas.get(table_name) {
                    if let Some(col_idx) = schema.index_of(key_col) {
                        if let Some(pred) = scan_predicates.get(table_name) {
                            if let Some(clause) = pred.clauses.iter().find(|c| c.column_index == col_idx && c.op == RangeOp::Eq) {
                                let fast_scan = crate::storage::fastscan::FastScanEQ::new(col_idx, clause.value.clone());
                                let mut filtered = Vec::with_capacity(batches.len());
                                for batch in batches {
                                    if let Some(result) = fast_scan.scan_batch_sorted(&batch, &[col_idx]) {
                                        if result.num_rows() > 0 {
                                            filtered.push(result);
                                        }
                                    }
                                }
                                batches = filtered;
                            }
                        }
                    }
                }
            }
        }

        table_batches.insert(table_name.clone(), batches);
    }

    let plan = try_skip_sort_for_cluster_key(plan, &table_batches, &table_cluster_keys, &table_schemas);

    let mut pipeline = super::operators::build_pipeline(plan, &table_batches)?;
    let mut result_batches = Vec::new();
    let mut result_schema = None;
    while let Some(batch) = pipeline.next_batch()? {
        if result_schema.is_none() {
            result_schema = Some(batch.schema.clone());
        }
        result_batches.push(batch);
    }

    let schema = result_schema.unwrap_or_else(|| Schema::empty());
    Ok(ExecResult::Query { schema, batches: result_batches })
}

fn try_skip_sort_for_cluster_key(
    plan: LogicalPlan,
    table_batches: &HashMap<String, Vec<RecordBatch>>,
    table_cluster_keys: &HashMap<String, Vec<String>>,
    table_schemas: &HashMap<String, Schema>,
) -> LogicalPlan {
    let LogicalPlan::Sort(sort) = &plan else { return plan; };
    if table_batches.len() != 1 {
        return plan;
    }
    let (table_name, batches) = table_batches.iter().next().unwrap();
    if batches.len() != 1 {
        return plan;
    }
    let cluster_key = match table_cluster_keys.get(table_name) {
        Some(k) => k,
        None => return plan,
    };
    if cluster_key.is_empty() {
        return plan;
    }
    let schema = match table_schemas.get(table_name) {
        Some(s) => s,
        None => return plan,
    };
    let all_asc = sort.sort_keys.iter().all(|sk| !sk.desc);
    if !all_asc {
        return plan;
    }
    let matches = sort.sort_keys.len() == cluster_key.len()
        && sort.sort_keys.iter().zip(cluster_key.iter()).all(|(sk, ck)| {
            match &sk.expr {
                LogicalExpr::Column(idx) => {
                    schema.columns.get(*idx)
                        .map(|c| c.name == *ck)
                        .unwrap_or(false)
                }
                _ => false,
            }
        });
    if matches {
        *sort.input.clone()
    } else {
        plan
    }
}

pub fn extract_table_names(from: &FromClause) -> Vec<String> {
    match from {
        FromClause::Table(t) => vec![t.name.clone()],
        FromClause::Join(join_expr) => {
            let mut names = Vec::new();
            collect_table_names_from_join(join_expr, &mut names);
            names
        }
    }
}

fn collect_table_names_from_join(join_expr: &crate::sql::parser::ast::JoinExpr, names: &mut Vec<String>) {
    match &*join_expr.left {
        FromClause::Table(t) => names.push(t.name.clone()),
        FromClause::Join(inner) => collect_table_names_from_join(inner, names),
    }
    for jc in &join_expr.joins {
        names.push(jc.right.name.clone());
    }
}

fn collect_scan_predicates(
    plan: &LogicalPlan,
    out: &mut HashMap<String, RowGroupPredicate>,
) {
    match plan {
        LogicalPlan::Scan(scan) => {
            if let Some(filter) = &scan.pushed_filter {
                let mut clauses = Vec::new();
                extract_range_clauses(filter, scan, &mut clauses);
                if !clauses.is_empty() {
                    out.entry(scan.table_name.clone())
                        .and_modify(|p| p.clauses.extend(clauses.clone()))
                        .or_insert_with(|| RowGroupPredicate::new(clauses));
                }
            }
        }
        LogicalPlan::Filter(f) => collect_scan_predicates(&f.input, out),
        LogicalPlan::Project(p) => collect_scan_predicates(&p.input, out),
        LogicalPlan::Aggregate(a) => collect_scan_predicates(&a.input, out),
        LogicalPlan::Sort(s) => collect_scan_predicates(&s.input, out),
        LogicalPlan::Limit(l) => collect_scan_predicates(&l.input, out),
        LogicalPlan::TopN(t) => collect_scan_predicates(&t.input, out),
        LogicalPlan::Distinct(d) => collect_scan_predicates(&d.input, out),
        LogicalPlan::Window(w) => collect_scan_predicates(&w.input, out),
        LogicalPlan::Join(j) => {
            collect_scan_predicates(&j.left, out);
            collect_scan_predicates(&j.right, out);
        }
        LogicalPlan::SemiJoin(s) => {
            collect_scan_predicates(&s.left, out);
            collect_scan_predicates(&s.right, out);
        }
        LogicalPlan::Empty => {}
    }
}

fn collect_scan_projections(
    plan: &LogicalPlan,
    out: &mut HashMap<String, Vec<usize>>,
) {
    match plan {
        LogicalPlan::Scan(scan) => {
            if let Some(proj) = &scan.projection {
                out.insert(scan.table_name.clone(), proj.clone());
            }
        }
        LogicalPlan::Filter(f) => collect_scan_projections(&f.input, out),
        LogicalPlan::Project(p) => collect_scan_projections(&p.input, out),
        LogicalPlan::Aggregate(a) => collect_scan_projections(&a.input, out),
        LogicalPlan::Sort(s) => collect_scan_projections(&s.input, out),
        LogicalPlan::Limit(l) => collect_scan_projections(&l.input, out),
        LogicalPlan::TopN(t) => collect_scan_projections(&t.input, out),
        LogicalPlan::Distinct(d) => collect_scan_projections(&d.input, out),
        LogicalPlan::Window(w) => collect_scan_projections(&w.input, out),
        LogicalPlan::Join(j) => {
            collect_scan_projections(&j.left, out);
            collect_scan_projections(&j.right, out);
        }
        LogicalPlan::SemiJoin(s) => {
            collect_scan_projections(&s.left, out);
            collect_scan_projections(&s.right, out);
        }
        LogicalPlan::Empty => {}
    }
}

fn extract_range_clauses(
    expr: &LogicalExpr,
    scan: &ScanPlan,
    out: &mut Vec<RangeClause>,
) {
    match expr {
        LogicalExpr::BinaryOp { left, op, right } => match op {
            BinOp::And => {
                extract_range_clauses(left, scan, out);
                extract_range_clauses(right, scan, out);
            }
            BinOp::Eq | BinOp::Gt | BinOp::Gte | BinOp::Lt | BinOp::Lte => {
                if let Some(c) = try_build_clause(left, *op, right) {
                    out.push(c);
                } else if let Some(c) = try_build_clause_mirrored(left, *op, right) {
                    out.push(c);
                }
            }
            _ => {}
        },
        LogicalExpr::Between { expr, low, high, negated: false } => {
            if let (LogicalExpr::Column(idx), LogicalExpr::Literal(lo), LogicalExpr::Literal(hi)) =
                (&**expr, &**low, &**high)
            {
                out.push(RangeClause { column_index: *idx, op: RangeOp::Ge, value: lo.clone() });
                out.push(RangeClause { column_index: *idx, op: RangeOp::Le, value: hi.clone() });
            }
        }
        _ => {}
    }
}

fn try_build_clause(
    left: &LogicalExpr,
    op: BinOp,
    right: &LogicalExpr,
) -> Option<RangeClause> {
    if let (LogicalExpr::Column(idx), LogicalExpr::Literal(val)) = (left, right) {
        let rop = match op {
            BinOp::Eq => RangeOp::Eq,
            BinOp::Lt => RangeOp::Lt,
            BinOp::Lte => RangeOp::Le,
            BinOp::Gt => RangeOp::Gt,
            BinOp::Gte => RangeOp::Ge,
            _ => return None,
        };
        return Some(RangeClause { column_index: *idx, op: rop, value: val.clone() });
    }
    None
}

fn try_build_clause_mirrored(
    left: &LogicalExpr,
    op: BinOp,
    right: &LogicalExpr,
) -> Option<RangeClause> {
    if let (LogicalExpr::Literal(val), LogicalExpr::Column(idx)) = (left, right) {
        // Literal <op> Column  ===  Column <mirrored_op> Literal
        let rop = match op {
            BinOp::Eq => RangeOp::Eq,
            BinOp::Lt => RangeOp::Gt,
            BinOp::Lte => RangeOp::Ge,
            BinOp::Gt => RangeOp::Lt,
            BinOp::Gte => RangeOp::Le,
            _ => return None,
        };
        return Some(RangeClause { column_index: *idx, op: rop, value: val.clone() });
    }
    None
}
fn try_count_star_fast_path(
    engine: &Engine,
    db: &str,
    stmt: &Statement,
    table_names: &[String],
) -> Option<Result<ExecResult>> {
    let (select_list, where_clause, group_by) = match stmt {
        Statement::Select { select_list, where_clause, group_by, .. } => {
            (select_list, where_clause, group_by)
        }
        _ => return None,
    };

    if !group_by.is_empty() || table_names.len() != 1 {
        return None;
    }

    if select_list.len() != 1 {
        return None;
    }

    let is_count_star = match &select_list[0] {
        SelectItem::Expr(Expr::Function { name, args, distinct: false }, _) => {
            name.eq_ignore_ascii_case("count")
                && args.len() == 1
                && matches!(&args[0], Expr::Column(col) if col == "*")
        }
        _ => false,
    };

    if !is_count_star {
        return None;
    }

    let table_name = &table_names[0];
    let table_dir = engine.data_dir().join(db).join(table_name);

    if !table_dir.exists() {
        return None;
    }

    let table_schema = {
        let catalogs = engine.catalogs().read().unwrap();
        let catalog = catalogs.get(db)?;
        let meta = catalog.get_table(table_name)?;
        Schema::new(meta.columns.clone())
    };

    let segment_files = match SegmentManager::discover_segments(&table_dir) {
        Ok(files) => files,
        Err(e) => return Some(Err(e)),
    };

    let mgr = engine.segment_mgr();
    let predicate = super::helpers::build_row_group_predicate(where_clause, &table_schema);
    let mut total_rows: u64 = 0;

    for seg_path in &segment_files {
        match mgr.open_reader(seg_path) {
            Ok(reader) => {
                if where_clause.is_some() {
                    let batches = match reader.scan_with_predicate(4096, predicate.as_ref()) {
                        Ok(b) => b,
                        Err(e) => return Some(Err(e)),
                    };
                    for batch in batches {
                        if let Some(ref expr) = where_clause {
                            match super::helpers::eval_where_clause_batch(expr, &batch, &table_schema) {
                                Ok(mask) => {
                                    total_rows += mask.iter().filter(|&m| *m).count() as u64;
                                }
                                Err(_) => {
                                    total_rows += batch.num_rows() as u64;
                                }
                            }
                        }
                    }
                } else {
                    let count = reader.row_count();
                    let deleted = reader.deleted_row_count();
                    total_rows += count.saturating_sub(deleted);
                }
            }
            Err(e) => return Some(Err(e)),
        }
    }

    let memtables = engine.memtable_manager().get_by_table(db, table_name);
    for mt in &memtables {
        if let Some(batch) = mt.read() {
            if let Some(ref expr) = where_clause {
                match super::helpers::eval_where_clause_batch(expr, &batch, &table_schema) {
                    Ok(mask) => {
                        total_rows += mask.iter().filter(|&m| *m).count() as u64;
                    }
                    Err(_) => {
                        total_rows += batch.num_rows() as u64;
                    }
                }
            } else {
                total_rows += batch.num_rows() as u64;
            }
        }
    }

    let schema = Schema::new(vec![ColumnDef::new("count(*)", DataType::Int64)]);
    let mut batch = RecordBatch::new(schema.clone());
    if batch.column_mut(0).unwrap().push(&ScalarValue::Int64(Some(total_rows as i64))).is_err() {
        return None;
    }

    Some(Ok(ExecResult::Query { schema, batches: vec![batch] }))
}

fn apply_projection_to_batch(batch: &RecordBatch, projection: &[usize]) -> RecordBatch {
    let mut col_defs = Vec::with_capacity(projection.len());
    let mut columns = Vec::with_capacity(projection.len());
    for &idx in projection {
        if let Some(col) = batch.column(idx) {
            columns.push(col.clone());
            if let Some(col_def) = batch.schema.column(idx) {
                col_defs.push(col_def.clone());
            }
        }
    }
    let schema = Schema::new(col_defs);
    RecordBatch::try_new(schema, columns).unwrap_or_else(|_| batch.clone())
}

fn try_fast_scan_eq_unified(
    engine: &Engine,
    db: &str,
    stmt: &Statement,
    table_names: &[String],
) -> Option<Result<ExecResult>> {
    if table_names.len() != 1 {
        return None;
    }

    let (select_list, where_clause, group_by, order_by) = match stmt {
        Statement::Select { select_list, where_clause, group_by, order_by, .. } => {
            (select_list, where_clause, group_by, order_by)
        }
        _ => return None,
    };

    if !group_by.is_empty() || order_by.is_some() {
        return None;
    }

    let eq_clause = extract_single_eq_clause(where_clause)?;
    let table_name = &table_names[0];

    let table_schema = {
        let catalogs = engine.catalogs().read().unwrap();
        let catalog = catalogs.get(db)?;
        let meta = catalog.get_table(table_name)?;
        Schema::new(meta.columns.clone())
    };

    let col_idx = eq_clause.column_index;
    let col_name = table_schema.columns.get(col_idx)?.name.clone();
    let eq_value = eq_clause.value.clone();

    let projection = extract_projection_indices(select_list, &table_schema)?;
    let projection_names: Vec<String> = projection.iter()
        .map(|&idx| table_schema.columns[idx].name.clone())
        .collect();

    let mut all_rows: Vec<RecordBatch> = Vec::new();

    let table_dir = engine.data_dir().join(db).join(table_name);
    if table_dir.exists() {
        let segment_files = match SegmentManager::discover_segments(&table_dir) {
            Ok(files) => files,
            Err(e) => return Some(Err(e)),
        };

        let mgr = engine.segment_mgr();

        if let Some(index_mgr) = engine.index_manager() {
            if !index_mgr.is_index_stale(db, table_name) && index_mgr.index_block_count(db, table_name) > 0 {
                if let Some(row_ids) = index_mgr.search(db, table_name, &eq_value) {
                    if !row_ids.is_empty() {
                        for seg_path in &segment_files {
                            let reader = match mgr.open_reader(seg_path) {
                                Ok(r) => r,
                                Err(_) => continue,
                            };

                            for row_id in &row_ids {
                                if let Ok(batch) = reader.read_row_by_index(*row_id as usize, &projection_names) {
                                    if batch.num_rows() > 0 {
                                        let projected = apply_projection_to_batch(&batch, &projection);
                                        all_rows.push(projected);
                                    }
                                }
                            }
                        }

                        if !all_rows.is_empty() {
                            let memtables = engine.memtable_manager().get_by_table(db, table_name);
                            for mt in &memtables {
                                if let Some(batch) = mt.read() {
                                    if batch.num_rows() > 0 {
                                        let fast_scan = crate::storage::fastscan::FastScanEQ::new(col_idx, eq_value.clone());
                                        let filtered = fast_scan.scan_batch_fast(&batch);
                                        if filtered.num_rows() > 0 {
                                            let projected = apply_projection_to_batch(&filtered, &projection);
                                            all_rows.push(projected);
                                        }
                                    }
                                }
                            }

                            let result_schema = all_rows[0].schema.clone();
                            return Some(Ok(ExecResult::Query { schema: result_schema, batches: all_rows }));
                        }
                    }
                }
            }
        }

        let fast_scan = crate::storage::fastscan::FastScanEQ::new(col_idx, eq_value.clone());
        let pred = RowGroupPredicate::new(vec![RangeClause {
            column_index: col_idx,
            op: RangeOp::Eq,
            value: eq_value.clone(),
        }]);

        for seg_path in segment_files {
            let reader = match mgr.open_reader(&seg_path) {
                Ok(r) => r,
                Err(_) => continue,
            };

            let candidate_rgs = reader.filter_row_groups_two_level(&pred, Some(col_idx), Some(&eq_value));

            for rg_idx in candidate_rgs {
                let batch = match reader.read_columns_fast(rg_idx, &projection) {
                    Ok(b) => b,
                    Err(_) => continue,
                };

                if batch.num_rows() > 0 {
                    let filtered = fast_scan.scan_batch_fast(&batch);
                    if filtered.num_rows() > 0 {
                        all_rows.push(filtered);
                    }
                }
            }
        }
    }

    let memtables = engine.memtable_manager().get_by_table(db, table_name);
    for mt in &memtables {
        if let Some(batch) = mt.read() {
            if batch.num_rows() > 0 {
                let fast_scan = crate::storage::fastscan::FastScanEQ::new(col_idx, eq_value.clone());
                let filtered = fast_scan.scan_batch_fast(&batch);
                if filtered.num_rows() > 0 {
                    let projected = apply_projection_to_batch(&filtered, &projection);
                    all_rows.push(projected);
                }
            }
        }
    }

    if all_rows.is_empty() {
        return None;
    }

    let result_schema = all_rows[0].schema.clone();
    Some(Ok(ExecResult::Query { schema: result_schema, batches: all_rows }))
}

fn extract_single_eq_clause(where_clause: &Option<Expr>) -> Option<RangeClause> {
    let expr = where_clause.as_ref()?;
    
    match expr {
        Expr::BinaryOp { left, op, right } => {
            if *op != BinOp::Eq {
                return None;
            }
            
            if let (Expr::Column(col_name), Expr::Literal(val)) = (left.as_ref(), right.as_ref()) {
                let idx = 0;
                return Some(RangeClause { column_index: idx, op: RangeOp::Eq, value: val.clone() });
            }
            
            if let (Expr::Literal(val), Expr::Column(col_name)) = (left.as_ref(), right.as_ref()) {
                let idx = 0;
                return Some(RangeClause { column_index: idx, op: RangeOp::Eq, value: val.clone() });
            }
            
            None
        }
        _ => None,
    }
}

fn extract_projection_indices(select_list: &[SelectItem], schema: &Schema) -> Option<Vec<usize>> {
    let mut indices = Vec::new();
    for item in select_list {
        match item {
            SelectItem::Star => {
                indices = (0..schema.num_columns()).collect();
                break;
            }
            SelectItem::Expr(Expr::Column(col_name), _) => {
                let idx = schema.index_of(col_name)?;
                indices.push(idx);
            }
            _ => return None,
        }
    }
    Some(indices)
}

use crate::sql::parser::ast::{OrderByItem, Statement as AstStatement};

pub fn exec_union(
    engine: &Engine,
    db: &str,
    left: &Statement,
    right: &Statement,
    all: bool,
    order_by: &[OrderByItem],
    limit: &Option<Expr>,
    offset: &Option<Expr>,
) -> Result<ExecResult> {
    let left_result = exec_union_recursive(engine, db, left)?;
    let right_result = exec_union_recursive(engine, db, right)?;

    let left_batches = match left_result {
        ExecResult::Query { batches, .. } => batches,
        ExecResult::Ok { .. } => return Err(MonaxError::Sql("UNION left side returned no data".into())),
    };
    let right_batches = match right_result {
        ExecResult::Query { batches, .. } => batches,
        ExecResult::Ok { .. } => return Err(MonaxError::Sql("UNION right side returned no data".into())),
    };

    let schema = if !left_batches.is_empty() {
        left_batches[0].schema.clone()
    } else if !right_batches.is_empty() {
        right_batches[0].schema.clone()
    } else {
        return Err(MonaxError::Sql("UNION with empty result sets".into()));
    };

    let mut combined_batches = left_batches;
    combined_batches.extend(right_batches);

    if !all {
        combined_batches = deduplicate_batches(&combined_batches);
    }

    if !order_by.is_empty() {
        combined_batches = sort_batches(&combined_batches, order_by);
    }

    if let Some(offset_expr) = offset {
        let offset_val = super::helpers::eval_expr_to_int(offset_expr)?;
        if offset_val > 0 && offset_val < combined_batches.len() {
            combined_batches = combined_batches.into_iter().skip(offset_val as usize).collect();
        } else if offset_val >= combined_batches.len() as i64 {
            combined_batches = Vec::new();
        }
    }

    if let Some(limit_expr) = limit {
        let limit_val = super::helpers::eval_expr_to_int(limit_expr)?;
        if limit_val > 0 && limit_val < combined_batches.len() as i64 {
            combined_batches = combined_batches.into_iter().take(limit_val as usize).collect();
        }
    }

    Ok(ExecResult::Query { schema, batches: combined_batches })
}

fn exec_union_recursive(engine: &Engine, db: &str, stmt: &Statement) -> Result<ExecResult> {
    match stmt {
        Statement::Union { left, right, all, order_by, limit, offset } => {
            exec_union(engine, db, left, right, *all, order_by, limit, offset)
        }
        Statement::Select { .. } => {
            exec_select(engine, db, stmt)
        }
        _ => Err(MonaxError::Sql(format!("UNION component must be SELECT, got {:?}", stmt)))
    }
}

fn deduplicate_batches(batches: &[RecordBatch]) -> Vec<RecordBatch> {
    if batches.is_empty() {
        return Vec::new();
    }
    
    let mut seen_rows: std::collections::HashSet<Vec<String>> = std::collections::HashSet::new();
    let mut result_batches = Vec::new();
    let schema = batches[0].schema.clone();
    let mut current_batch = RecordBatch::new(schema.clone());
    
    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            let row_key = build_row_key(batch, row_idx);
            if seen_rows.insert(row_key) {
                for col_idx in 0..batch.num_columns() {
                    if let Some(col) = batch.column(col_idx) {
                        if let Some(dest_col) = current_batch.column_mut(col_idx) {
                            if let Ok(val) = col.get(row_idx) {
                                let _ = dest_col.push(&val);
                            }
                        }
                    }
                }
                if current_batch.num_rows() >= 4096 {
                    result_batches.push(current_batch);
                    current_batch = RecordBatch::new(schema.clone());
                }
            }
        }
    }
    
    if current_batch.num_rows() > 0 {
        result_batches.push(current_batch);
    }
    
    result_batches
}

fn build_row_key(batch: &RecordBatch, row_idx: usize) -> Vec<String> {
    let mut key = Vec::with_capacity(batch.num_columns());
    for col_idx in 0..batch.num_columns() {
        if let Some(col) = batch.column(col_idx) {
            if let Ok(val) = col.get(row_idx) {
                key.push(format!("{:?}", val));
            } else {
                key.push("NULL".to_string());
            }
        }
    }
    key
}

fn sort_batches(batches: &[RecordBatch], order_by: &[OrderByItem]) -> Vec<RecordBatch> {
    if batches.is_empty() || order_by.is_empty() {
        return batches.to_vec();
    }
    
    let schema = batches[0].schema.clone();
    let mut all_rows: Vec<(usize, usize)> = Vec::new();
    
    for (batch_idx, batch) in batches.iter().enumerate() {
        for row_idx in 0..batch.num_rows() {
            all_rows.push((batch_idx, row_idx));
        }
    }
    
    all_rows.sort_by(|a, b| {
        for ob in order_by {
            let col_idx = match &ob.expr {
                Expr::Column(col_name) => schema.index_of(col_name).unwrap_or(0),
                Expr::QualifiedColumn { column, .. } => schema.index_of(column).unwrap_or(0),
                Expr::Literal(ScalarValue::Int64(Some(idx))) => *idx as usize,
                _ => 0,
            };
            
            let val_a = batches[a.0].column(col_idx)
                .and_then(|c| c.get(a.1))
                .unwrap_or(ScalarValue::Null);
            let val_b = batches[b.0].column(col_idx)
                .and_then(|c| c.get(b.1))
                .unwrap_or(ScalarValue::Null);
            
            let cmp = compare_scalar_values(&val_a, &val_b);
            if cmp != std::cmp::Ordering::Equal {
                return if ob.desc { cmp.reverse() } else { cmp };
            }
        }
        std::cmp::Ordering::Equal
    });
    
    let mut result_batch = RecordBatch::new(schema.clone());
    for (batch_idx, row_idx) in all_rows {
        for col_idx in 0..schema.num_columns() {
            if let Some(col) = batches[batch_idx].column(col_idx) {
                if let Some(dest_col) = result_batch.column_mut(col_idx) {
                    if let Ok(val) = col.get(row_idx) {
                        let _ = dest_col.push(&val);
                    }
                }
            }
        }
        if result_batch.num_rows() >= 4096 {
            break;
        }
    }
    
    vec![result_batch]
}

fn compare_scalar_values(a: &ScalarValue, b: &ScalarValue) -> std::cmp::Ordering {
    match (a, b) {
        (ScalarValue::Int64(Some(va)), ScalarValue::Int64(Some(vb))) => va.cmp(&vb),
        (ScalarValue::Float64(Some(va)), ScalarValue::Float64(Some(vb))) => va.partial_cmp(&vb).unwrap_or(std::cmp::Ordering::Equal),
        (ScalarValue::VarChar(Some(va)), ScalarValue::VarChar(Some(vb))) => va.cmp(&vb),
        (ScalarValue::TimestampNanosecond(Some(va), _), ScalarValue::TimestampNanosecond(Some(vb), _)) => va.cmp(&vb),
        (ScalarValue::Null, _) => std::cmp::Ordering::Less,
        (_, ScalarValue::Null) => std::cmp::Ordering::Greater,
        _ => std::cmp::Ordering::Equal,
    }
}
