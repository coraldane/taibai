use std::collections::HashSet;
use std::path::PathBuf;

use crate::common::error::{Result, TaibaiError};
use crate::storage::catalog::schema::{PartitionType, TableMeta};
use crate::storage::index::index_manager::UniqueRowLocation;
use crate::storage::index::sort_key::compare_scalars_for_sort;
use crate::storage::partition::pruning::ZoneMap;
use crate::storage::partition::{
    parse_partition_key, partition_function::compute_partition_key_from_scalar,
};
use crate::storage::segment::delete_bitmap;
use crate::storage::segment::{RangeOp, SegmentReader};
use crate::storage::segment_manager::SegmentManager;
use crate::types::batch::Schema;
use crate::types::scalar::ScalarValue;

use super::engine::{Engine, ExecResult};
use super::helpers::{
    build_row_group_predicate, collect_expr_column_indices, eval_expr_to_scalar,
    eval_where_clause_batch,
};
use crate::sql::parser::ast::{BinOp, Expr};

pub(crate) struct ReplaceDeleteUndo {
    segment_footers: Vec<(PathBuf, Vec<u8>, u64)>,
    tombstones: Vec<UniqueRowLocation>,
    deleted_rows: u64,
}

impl ReplaceDeleteUndo {
    pub(crate) fn deleted_rows(&self) -> u64 {
        self.deleted_rows
    }
}

pub(crate) fn delete_replace_conflicts_under_table_lock(
    engine: &Engine,
    db: &str,
    table: &str,
    key_sets: &[(Vec<usize>, HashSet<Vec<ScalarValue>>)],
) -> Result<ReplaceDeleteUndo> {
    let table_dir = engine.data_dir().join(db).join(table);
    let mut undo = ReplaceDeleteUndo {
        segment_footers: Vec::new(),
        tombstones: Vec::new(),
        deleted_rows: 0,
    };
    if !table_dir.exists() || key_sets.is_empty() {
        return Ok(undo);
    }

    let mgr = engine.segment_mgr();
    for seg_path in SegmentManager::discover_segments(&table_dir)? {
        let reader = mgr.open_reader_cached(&seg_path)?;
        let total_rows = reader.row_count();
        if total_rows == 0 || reader.deleted_row_count() >= total_rows {
            continue;
        }
        let old_bitmap = reader.delete_bitmap().to_vec();
        let old_deleted = reader.deleted_row_count();
        let mut new_bitmap = old_bitmap.clone();
        new_bitmap.resize(total_rows.div_ceil(8) as usize, 0);
        let mut row_id = 0u64;
        let mut newly_deleted = 0u64;

        for rg_idx in 0..reader.num_row_groups() {
            let batch = reader.read_row_group(rg_idx)?;
            for row_idx in 0..batch.num_rows() {
                let absolute_row = row_id + row_idx as u64;
                if delete_bitmap::is_deleted(&new_bitmap, absolute_row) {
                    continue;
                }
                let conflicts = key_sets.iter().any(|(indices, keys)| {
                    let mut key = Vec::with_capacity(indices.len());
                    for &col_idx in indices {
                        let Some(value) = batch.scalar(row_idx, col_idx) else {
                            return false;
                        };
                        if value.is_null() {
                            return false;
                        }
                        key.push(value);
                    }
                    keys.contains(&key)
                });
                if conflicts && delete_bitmap::mark_deleted(&mut new_bitmap, absolute_row) {
                    undo.tombstones.push(UniqueRowLocation {
                        segment_path: seg_path.to_string_lossy().into_owned(),
                        row_id: absolute_row,
                    });
                    newly_deleted += 1;
                }
            }
            row_id += batch.num_rows() as u64;
        }

        if newly_deleted == 0 {
            continue;
        }
        undo.segment_footers
            .push((seg_path.clone(), old_bitmap, old_deleted));
        if let Err(error) =
            mgr.rewrite_delete_footer(&seg_path, &new_bitmap, old_deleted + newly_deleted)
        {
            let _ = restore_replace_delete_under_table_lock(engine, &undo);
            return Err(error);
        }
        undo.deleted_rows += newly_deleted;
    }
    Ok(undo)
}

pub(crate) fn restore_replace_delete_under_table_lock(
    engine: &Engine,
    undo: &ReplaceDeleteUndo,
) -> Result<()> {
    let mut first_error = None;
    for (path, bitmap, deleted_count) in undo.segment_footers.iter().rev() {
        if let Err(error) = engine
            .segment_mgr()
            .rewrite_delete_footer(path, bitmap, *deleted_count)
        {
            if first_error.is_none() {
                first_error = Some(error);
            }
        }
    }
    match first_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

pub(crate) fn finalize_replace_delete_under_table_lock(
    engine: &Engine,
    db: &str,
    table: &str,
    table_meta: &TableMeta,
    undo: &ReplaceDeleteUndo,
) -> Result<()> {
    if undo.deleted_rows == 0 {
        return Ok(());
    }
    for index in &table_meta.unique_indexes {
        engine.index_manager().append_unique_index_tombstones(
            db,
            table,
            &index.name,
            undo.tombstones.clone(),
        )?;
    }

    let mut catalogs = engine.catalogs().write().unwrap();
    if let Some(catalog) = catalogs.get_mut(db) {
        if let Some(meta) = catalog.get_table_mut(table) {
            meta.row_count = (meta.row_count - undo.deleted_rows as i64).max(0);
            let meta_dir = engine.data_dir().join("meta");
            catalog.save(&meta_dir)?;
        }
    }
    Ok(())
}

pub fn exec_delete(
    engine: &Engine,
    db: &str,
    table: &str,
    where_clause: &Option<Expr>,
) -> Result<ExecResult> {
    let (table_schema, table_meta) = {
        let catalogs = engine.catalogs().read().unwrap();
        let catalog = catalogs
            .get(db)
            .ok_or_else(|| TaibaiError::Sql(format!("database '{}' does not exist", db)))?;
        let meta = catalog
            .get_table(table)
            .ok_or_else(|| TaibaiError::Sql(format!("table '{}.{}' does not exist", db, table)))?
            .clone();
        (Schema::new(meta.columns.clone()), meta)
    };

    let resolved_where_clause = resolve_in_subqueries(engine, db, where_clause)?;

    engine.flush_table_buffers(db, table)?;

    let table_dir = engine.data_dir().join(db).join(table);
    if !table_dir.exists() {
        return Ok(ExecResult::Ok { rows_affected: 0 });
    }

    let mgr = engine.segment_mgr();
    let mut total_deleted: u64 = 0;
    let mut unique_index_tombstones: Vec<UniqueRowLocation> = Vec::new();
    let predicate = build_row_group_predicate(&resolved_where_clause, &table_schema);
    let predicate_projection = resolved_where_clause
        .as_ref()
        .map(|expr| collect_expr_column_indices(expr, &table_schema))
        .unwrap_or_default();

    mgr.with_table_mutation_lock(db, table, || {
        let mut segment_files = prune_segments_by_partition(
            SegmentManager::discover_segments(&table_dir)?,
            &resolved_where_clause,
            &table_schema,
            &table_meta,
        );
        if let Some(ref pred) = predicate {
            segment_files = prune_segments_by_cluster_key(
                segment_files,
                &mgr,
                &table_schema,
                &table_meta,
                pred,
            )?;
        }
        for seg_path in segment_files {
            let (new_bitmap, new_deleted_count, prev_deleted) = {
                let reader = mgr.open_reader_cached(&seg_path)?;
                let total_rows = reader.row_count();
                if total_rows == 0 {
                    continue;
                }
                if let Some(ref pred) = predicate {
                    if !segment_may_match(&reader, pred) {
                        continue;
                    }
                }
                let prev_bitmap = reader.delete_bitmap().to_vec();
                let prev_deleted = reader.deleted_row_count();
                let bitmap_bytes = total_rows.div_ceil(8) as usize;
                let mut new_bitmap = vec![0u8; bitmap_bytes];
                if !prev_bitmap.is_empty() {
                    let copy_len = prev_bitmap.len().min(bitmap_bytes);
                    new_bitmap[..copy_len].copy_from_slice(&prev_bitmap[..copy_len]);
                }

                let mut newly_deleted: u64 = 0;
                let mut row_id: u64 = 0;
                for rg_idx in 0..reader.num_row_groups() {
                    let rg_rows = reader.row_group_rows(rg_idx);
                    if delete_bitmap::row_range_all_deleted(&new_bitmap, row_id, rg_rows) {
                        row_id += rg_rows;
                        continue;
                    }

                    if let Some(ref pred) = predicate {
                        if !pred.matches_row_group(reader.zone_maps(rg_idx)) {
                            row_id += rg_rows;
                            continue;
                        }
                    }

                    if let Some(ref where_expr) = resolved_where_clause {
                        let batch = if predicate_projection.is_empty() {
                            reader.read_row_group(rg_idx)?
                        } else {
                            reader.read_row_group_with_projection(rg_idx, &predicate_projection)?
                        };
                        let mask = eval_where_clause_batch(where_expr, &batch, &batch.schema)?;
                        for (i, matches) in mask.into_iter().enumerate() {
                            if matches
                                && delete_bitmap::mark_deleted(&mut new_bitmap, row_id + i as u64)
                            {
                                unique_index_tombstones.push(UniqueRowLocation {
                                    segment_path: seg_path.to_string_lossy().into_owned(),
                                    row_id: row_id + i as u64,
                                });
                                newly_deleted += 1;
                            }
                        }
                        row_id += batch.num_rows() as u64;
                    } else {
                        for i in 0..rg_rows {
                            if delete_bitmap::mark_deleted(&mut new_bitmap, row_id + i) {
                                unique_index_tombstones.push(UniqueRowLocation {
                                    segment_path: seg_path.to_string_lossy().into_owned(),
                                    row_id: row_id + i,
                                });
                                newly_deleted += 1;
                            }
                        }
                        row_id += rg_rows;
                    }
                }

                let new_deleted_count = prev_deleted + newly_deleted;
                total_deleted += newly_deleted;
                (new_bitmap, new_deleted_count, prev_deleted)
            };

            if new_deleted_count == prev_deleted {
                continue;
            }

            mgr.rewrite_delete_footer(&seg_path, &new_bitmap, new_deleted_count)?;
        }
        Ok(())
    })?;

    if total_deleted > 0 {
        let unique_index_tombstones = collect_unique_index_tombstones(&mgr, &table_dir)?;
        for index in &table_meta.unique_indexes {
            engine.index_manager().append_unique_index_tombstones(
                db,
                table,
                &index.name,
                unique_index_tombstones.clone(),
            )?;
        }

        let mut catalogs = engine.catalogs().write().unwrap();
        if let Some(catalog) = catalogs.get_mut(db) {
            if let Some(meta) = catalog.get_table_mut(table) {
                meta.row_count = (meta.row_count - total_deleted as i64).max(0);
                let meta_dir = engine.data_dir().join("meta");
                catalog.save(&meta_dir)?;
            }
        }
    }

    Ok(ExecResult::Ok {
        rows_affected: total_deleted,
    })
}

fn collect_unique_index_tombstones(
    mgr: &SegmentManager,
    table_dir: &std::path::Path,
) -> Result<Vec<UniqueRowLocation>> {
    let mut tombstones = Vec::new();
    for seg_path in SegmentManager::discover_segments(table_dir)? {
        let reader = mgr.open_reader(&seg_path)?;
        let bitmap = reader.delete_bitmap();
        if bitmap.is_empty() {
            continue;
        }
        for row_id in 0..reader.row_count() {
            if delete_bitmap::is_deleted(bitmap, row_id) {
                tombstones.push(UniqueRowLocation {
                    segment_path: seg_path.to_string_lossy().into_owned(),
                    row_id,
                });
            }
        }
    }
    Ok(tombstones)
}

fn resolve_in_subqueries(
    engine: &Engine,
    db: &str,
    where_clause: &Option<Expr>,
) -> Result<Option<Expr>> {
    where_clause
        .as_ref()
        .map(|expr| resolve_expr_in_subqueries(engine, db, expr))
        .transpose()
}

fn resolve_expr_in_subqueries(engine: &Engine, db: &str, expr: &Expr) -> Result<Expr> {
    Ok(match expr {
        Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
            left: Box::new(resolve_expr_in_subqueries(engine, db, left)?),
            op: *op,
            right: Box::new(resolve_expr_in_subqueries(engine, db, right)?),
        },
        Expr::UnaryNot(inner) => {
            Expr::UnaryNot(Box::new(resolve_expr_in_subqueries(engine, db, inner)?))
        }
        Expr::UnaryMinus(inner) => {
            Expr::UnaryMinus(Box::new(resolve_expr_in_subqueries(engine, db, inner)?))
        }
        Expr::IsNull(inner) => {
            Expr::IsNull(Box::new(resolve_expr_in_subqueries(engine, db, inner)?))
        }
        Expr::IsNotNull(inner) => {
            Expr::IsNotNull(Box::new(resolve_expr_in_subqueries(engine, db, inner)?))
        }
        Expr::Exists(inner) => {
            Expr::Exists(Box::new(resolve_expr_in_subqueries(engine, db, inner)?))
        }
        Expr::In {
            expr: inner,
            values,
            negated,
        } => {
            let resolved_inner = resolve_expr_in_subqueries(engine, db, inner)?;
            let resolved_values = if let [Expr::Subquery(subquery)] = values.as_slice() {
                execute_in_subquery(engine, db, subquery)?
                    .into_iter()
                    .map(Expr::Literal)
                    .collect()
            } else {
                values
                    .iter()
                    .map(|value| resolve_expr_in_subqueries(engine, db, value))
                    .collect::<Result<Vec<_>>>()?
            };
            Expr::In {
                expr: Box::new(resolved_inner),
                values: resolved_values,
                negated: *negated,
            }
        }
        Expr::Between {
            expr: inner,
            low,
            high,
            negated,
        } => Expr::Between {
            expr: Box::new(resolve_expr_in_subqueries(engine, db, inner)?),
            low: Box::new(resolve_expr_in_subqueries(engine, db, low)?),
            high: Box::new(resolve_expr_in_subqueries(engine, db, high)?),
            negated: *negated,
        },
        Expr::Case {
            operand,
            when_clauses,
            else_clause,
        } => Expr::Case {
            operand: operand
                .as_ref()
                .map(|expr| resolve_expr_in_subqueries(engine, db, expr).map(Box::new))
                .transpose()?,
            when_clauses: when_clauses
                .iter()
                .map(|(when_expr, then_expr)| {
                    Ok((
                        resolve_expr_in_subqueries(engine, db, when_expr)?,
                        resolve_expr_in_subqueries(engine, db, then_expr)?,
                    ))
                })
                .collect::<Result<Vec<_>>>()?,
            else_clause: else_clause
                .as_ref()
                .map(|expr| resolve_expr_in_subqueries(engine, db, expr).map(Box::new))
                .transpose()?,
        },
        Expr::Like {
            expr: inner,
            pattern,
            negated,
        } => Expr::Like {
            expr: Box::new(resolve_expr_in_subqueries(engine, db, inner)?),
            pattern: Box::new(resolve_expr_in_subqueries(engine, db, pattern)?),
            negated: *negated,
        },
        Expr::Function {
            name,
            args,
            distinct,
        } => Expr::Function {
            name: name.clone(),
            args: args
                .iter()
                .map(|arg| resolve_expr_in_subqueries(engine, db, arg))
                .collect::<Result<Vec<_>>>()?,
            distinct: *distinct,
        },
        Expr::Subquery(subquery) => {
            Expr::Subquery(Box::new(resolve_stmt_in_subqueries(engine, db, subquery)?))
        }
        Expr::WindowFunction {
            func,
            partition_by,
            order_by,
            frame,
        } => Expr::WindowFunction {
            func: func.clone(),
            partition_by: partition_by
                .iter()
                .map(|arg| resolve_expr_in_subqueries(engine, db, arg))
                .collect::<Result<Vec<_>>>()?,
            order_by: order_by
                .iter()
                .map(|item| {
                    Ok(crate::sql::parser::ast::OrderByItem {
                        expr: resolve_expr_in_subqueries(engine, db, &item.expr)?,
                        desc: item.desc,
                    })
                })
                .collect::<Result<Vec<_>>>()?,
            frame: frame.clone(),
        },
        Expr::Literal(_)
        | Expr::Placeholder(_)
        | Expr::Column(_)
        | Expr::QualifiedColumn { .. } => expr.clone(),
    })
}

fn resolve_stmt_in_subqueries(
    engine: &Engine,
    db: &str,
    stmt: &crate::sql::parser::ast::Statement,
) -> Result<crate::sql::parser::ast::Statement> {
    let mut stmt = stmt.clone();
    if let crate::sql::parser::ast::Statement::Select {
        select_list,
        where_clause,
        group_by,
        having,
        order_by,
        limit,
        offset,
        ..
    } = &mut stmt
    {
        for item in select_list {
            if let crate::sql::parser::ast::SelectItem::Expr(expr, _) = item {
                *expr = resolve_expr_in_subqueries(engine, db, expr)?;
            }
        }
        if let Some(expr) = where_clause {
            *expr = resolve_expr_in_subqueries(engine, db, expr)?;
        }
        for expr in group_by {
            *expr = resolve_expr_in_subqueries(engine, db, expr)?;
        }
        if let Some(expr) = having {
            *expr = resolve_expr_in_subqueries(engine, db, expr)?;
        }
        for item in order_by {
            item.expr = resolve_expr_in_subqueries(engine, db, &item.expr)?;
        }
        if let Some(expr) = limit {
            *expr = resolve_expr_in_subqueries(engine, db, expr)?;
        }
        if let Some(expr) = offset {
            *expr = resolve_expr_in_subqueries(engine, db, expr)?;
        }
    }
    Ok(stmt)
}

fn execute_in_subquery(
    engine: &Engine,
    db: &str,
    subquery: &crate::sql::parser::ast::Statement,
) -> Result<Vec<ScalarValue>> {
    let subquery = resolve_stmt_in_subqueries(engine, db, subquery)?;
    if let Some(values) = execute_simple_derived_subquery(engine, db, &subquery)? {
        return Ok(values);
    }
    let ExecResult::Query { batches, .. } = super::query::exec_select(engine, db, &subquery)?
    else {
        return Err(TaibaiError::Sql("IN subquery must be a SELECT".into()));
    };
    Ok(first_column_values(&batches, 0))
}

fn execute_simple_derived_subquery(
    engine: &Engine,
    db: &str,
    stmt: &crate::sql::parser::ast::Statement,
) -> Result<Option<Vec<ScalarValue>>> {
    use crate::sql::parser::ast::{FromClause, SelectItem, Statement, TableRef};

    let Statement::Select {
        select_list,
        from: Some(FromClause::Table(TableRef::Subquery { query, alias })),
        where_clause: None,
        group_by,
        having: None,
        order_by: _,
        limit: None,
        offset: None,
        ..
    } = stmt
    else {
        return Ok(None);
    };

    if !group_by.is_empty() || select_list.len() != 1 {
        return Ok(None);
    }

    let inner = resolve_stmt_in_subqueries(engine, db, query)?;
    let ExecResult::Query { schema, batches } = super::query::exec_select(engine, db, &inner)?
    else {
        return Err(TaibaiError::Sql(
            "derived IN subquery must be a SELECT".into(),
        ));
    };

    let col_idx = match &select_list[0] {
        SelectItem::Star => 0,
        SelectItem::Expr(Expr::Column(name), _) => schema.index_of(name).unwrap_or(0),
        SelectItem::Expr(Expr::QualifiedColumn { table, column }, _)
            if table.eq_ignore_ascii_case(alias) =>
        {
            schema.index_of(column).unwrap_or(0)
        }
        SelectItem::Expr(_, _) => 0,
    };
    Ok(Some(first_column_values(&batches, col_idx)))
}

fn first_column_values(
    batches: &[crate::types::batch::RecordBatch],
    col_idx: usize,
) -> Vec<ScalarValue> {
    let mut values = Vec::new();
    for batch in batches {
        if batch.num_columns() <= col_idx {
            continue;
        }
        for row_idx in 0..batch.num_rows() {
            values.push(
                batch
                    .scalar(row_idx, col_idx)
                    .unwrap_or(ScalarValue::Int64(None)),
            );
        }
    }
    values
}

fn segment_may_match(
    reader: &SegmentReader,
    predicate: &crate::storage::segment::RowGroupPredicate,
) -> bool {
    let segment_zone_maps = reader.segment_zone_maps();
    if !segment_zone_maps.is_empty() {
        return predicate.matches_row_group(segment_zone_maps);
    }
    (0..reader.num_row_groups()).any(|rg_idx| predicate.matches_row_group(reader.zone_maps(rg_idx)))
}

fn prune_segments_by_partition(
    segment_files: Vec<PathBuf>,
    where_clause: &Option<Expr>,
    schema: &Schema,
    table_meta: &TableMeta,
) -> Vec<PathBuf> {
    if !table_meta.is_partitioned() {
        return segment_files;
    }
    let Some(target_key) = delete_partition_eq_key(where_clause, schema, table_meta) else {
        return segment_files;
    };
    if target_key.is_empty() {
        return segment_files;
    }

    segment_files
        .into_iter()
        .filter(|path| {
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                return true;
            };
            match parse_partition_key(name) {
                Some(key) => key == target_key,
                None => true,
            }
        })
        .collect()
}

#[derive(Debug, Clone)]
struct ClusterKeyBounds {
    column_index: usize,
    lower: Option<(ScalarValue, bool)>,
    upper: Option<(ScalarValue, bool)>,
}

fn prune_segments_by_cluster_key(
    segment_files: Vec<PathBuf>,
    mgr: &SegmentManager,
    schema: &Schema,
    table_meta: &TableMeta,
    predicate: &crate::storage::segment::RowGroupPredicate,
) -> Result<Vec<PathBuf>> {
    if segment_files.len() <= 1 {
        return Ok(segment_files);
    }
    let Some(bounds) = cluster_key_bounds(schema, table_meta, predicate) else {
        return Ok(segment_files);
    };
    let Some((start, end)) = cluster_segment_range(&segment_files, mgr, &bounds)? else {
        return Ok(segment_files);
    };
    if start > end {
        return Ok(Vec::new());
    }
    Ok(segment_files[start..=end].to_vec())
}

fn cluster_key_bounds(
    schema: &Schema,
    table_meta: &TableMeta,
    predicate: &crate::storage::segment::RowGroupPredicate,
) -> Option<ClusterKeyBounds> {
    let key_col = table_meta.cluster_key.first()?;
    let key_col_idx = schema.index_of(key_col)?;

    let mut lower: Option<(ScalarValue, bool)> = None;
    let mut upper: Option<(ScalarValue, bool)> = None;
    for clause in &predicate.clauses {
        if clause.column_index != key_col_idx {
            continue;
        }
        match clause.op {
            RangeOp::Eq => {
                return Some(ClusterKeyBounds {
                    column_index: key_col_idx,
                    lower: Some((clause.value.clone(), true)),
                    upper: Some((clause.value.clone(), true)),
                });
            }
            RangeOp::Gt => set_tighter_lower(&mut lower, clause.value.clone(), false),
            RangeOp::Ge => set_tighter_lower(&mut lower, clause.value.clone(), true),
            RangeOp::Lt => set_tighter_upper(&mut upper, clause.value.clone(), false),
            RangeOp::Le => set_tighter_upper(&mut upper, clause.value.clone(), true),
        }
    }

    if lower.is_some() || upper.is_some() {
        Some(ClusterKeyBounds {
            column_index: key_col_idx,
            lower,
            upper,
        })
    } else {
        None
    }
}

fn cluster_segment_range(
    segments: &[PathBuf],
    mgr: &SegmentManager,
    bounds: &ClusterKeyBounds,
) -> Result<Option<(usize, usize)>> {
    if !cluster_segment_zone_maps_are_ordered(segments, mgr, bounds.column_index)? {
        return Ok(None);
    }
    let start = match &bounds.lower {
        Some((value, inclusive)) => {
            let Some(start) = first_segment_with_max_at_least(
                segments,
                mgr,
                bounds.column_index,
                value,
                *inclusive,
            )?
            else {
                return Ok(None);
            };
            start
        }
        None => 0,
    };
    let end = match &bounds.upper {
        Some((value, inclusive)) => {
            let Some(end) = last_segment_with_min_at_most(
                segments,
                mgr,
                bounds.column_index,
                value,
                *inclusive,
            )?
            else {
                return Ok(None);
            };
            if end < 0 {
                return Ok(Some((1, 0)));
            }
            end as usize
        }
        None => segments.len().saturating_sub(1),
    };
    Ok(Some((start, end)))
}

fn cluster_segment_zone_maps_are_ordered(
    segments: &[PathBuf],
    mgr: &SegmentManager,
    col_idx: usize,
) -> Result<bool> {
    let mut previous_max: Option<ScalarValue> = None;
    for segment in segments {
        let Some(zm) = segment_cluster_zone_map(mgr, segment, col_idx)? else {
            return Ok(false);
        };
        if compare_scalars_for_sort(&zm.min_value, &zm.max_value) == std::cmp::Ordering::Greater {
            return Ok(false);
        }
        if let Some(prev) = &previous_max {
            if compare_scalars_for_sort(prev, &zm.min_value) == std::cmp::Ordering::Greater {
                return Ok(false);
            }
        }
        previous_max = Some(zm.max_value);
    }
    Ok(true)
}

fn first_segment_with_max_at_least(
    segments: &[PathBuf],
    mgr: &SegmentManager,
    col_idx: usize,
    value: &ScalarValue,
    inclusive: bool,
) -> Result<Option<usize>> {
    let mut lo = 0usize;
    let mut hi = segments.len();
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let Some(zm) = segment_cluster_zone_map(mgr, &segments[mid], col_idx)? else {
            return Ok(None);
        };
        let cmp = compare_scalars_for_sort(&zm.max_value, value);
        let before = if inclusive {
            cmp == std::cmp::Ordering::Less
        } else {
            cmp != std::cmp::Ordering::Greater
        };
        if before {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    Ok(Some(lo))
}

fn last_segment_with_min_at_most(
    segments: &[PathBuf],
    mgr: &SegmentManager,
    col_idx: usize,
    value: &ScalarValue,
    inclusive: bool,
) -> Result<Option<isize>> {
    let mut lo = 0usize;
    let mut hi = segments.len();
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let Some(zm) = segment_cluster_zone_map(mgr, &segments[mid], col_idx)? else {
            return Ok(None);
        };
        let cmp = compare_scalars_for_sort(&zm.min_value, value);
        let at_or_before = if inclusive {
            cmp != std::cmp::Ordering::Greater
        } else {
            cmp == std::cmp::Ordering::Less
        };
        if at_or_before {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    Ok(Some(lo as isize - 1))
}

fn segment_cluster_zone_map(
    mgr: &SegmentManager,
    segment: &std::path::Path,
    col_idx: usize,
) -> Result<Option<ZoneMap>> {
    let reader = mgr.open_reader_cached(segment)?;
    let Some(zm) = reader.segment_zone_maps().get(col_idx) else {
        return Ok(None);
    };
    if zm.row_count == 0 || zm.min_value.is_null() || zm.max_value.is_null() {
        return Ok(None);
    }
    Ok(Some(zm.clone()))
}

fn set_tighter_lower(
    current: &mut Option<(ScalarValue, bool)>,
    candidate: ScalarValue,
    inclusive: bool,
) {
    let replace = match current {
        Some((value, current_inclusive)) => match compare_scalars_for_sort(&candidate, value) {
            std::cmp::Ordering::Greater => true,
            std::cmp::Ordering::Equal => !inclusive && *current_inclusive,
            std::cmp::Ordering::Less => false,
        },
        None => true,
    };
    if replace {
        *current = Some((candidate, inclusive));
    }
}

fn set_tighter_upper(
    current: &mut Option<(ScalarValue, bool)>,
    candidate: ScalarValue,
    inclusive: bool,
) {
    let replace = match current {
        Some((value, current_inclusive)) => match compare_scalars_for_sort(&candidate, value) {
            std::cmp::Ordering::Less => true,
            std::cmp::Ordering::Equal => !inclusive && *current_inclusive,
            std::cmp::Ordering::Greater => false,
        },
        None => true,
    };
    if replace {
        *current = Some((candidate, inclusive));
    }
}

fn delete_partition_eq_key(
    where_clause: &Option<Expr>,
    schema: &Schema,
    table_meta: &TableMeta,
) -> Option<String> {
    let expr = where_clause.as_ref()?;
    let pk_idx = table_meta.get_partition_column_index()?;
    let pk_col = &schema.columns.get(pk_idx)?.name;
    let literal_expr = find_eq_literal_for_column(expr, pk_col)?;
    let scalar = eval_expr_to_scalar(literal_expr, &schema.columns[pk_idx].data_type).ok()?;

    match table_meta.partition_type {
        PartitionType::Day
        | PartitionType::Month
        | PartitionType::Year
        | PartitionType::Hash
        | PartitionType::Mod => Some(compute_partition_key_from_scalar(
            &table_meta.partition_strategy(),
            &scalar,
        )),
        PartitionType::Value => scalar.to_text(),
        PartitionType::None => None,
    }
}

fn find_eq_literal_for_column<'a>(expr: &'a Expr, column: &str) -> Option<&'a Expr> {
    match expr {
        Expr::BinaryOp { left, op, right } if *op == BinOp::And => {
            find_eq_literal_for_column(left, column)
                .or_else(|| find_eq_literal_for_column(right, column))
        }
        Expr::BinaryOp { left, op, right } if *op == BinOp::Eq => {
            if expr_matches_column(left, column) && matches!(right.as_ref(), Expr::Literal(_)) {
                return Some(right);
            }
            if expr_matches_column(right, column) && matches!(left.as_ref(), Expr::Literal(_)) {
                return Some(left);
            }
            None
        }
        _ => None,
    }
}

fn expr_matches_column(expr: &Expr, column: &str) -> bool {
    match expr {
        Expr::Column(name) => name.eq_ignore_ascii_case(column),
        Expr::QualifiedColumn { column: name, .. } => name.eq_ignore_ascii_case(column),
        _ => false,
    }
}

#[cfg(test)]
#[path = "dml_delete_tests.rs"]
mod tests;
