use std::path::PathBuf;

use crate::common::error::{TaibaiError, Result};
use crate::storage::catalog::schema::{PartitionType, TableMeta};
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

use super::engine::{change_row_from_images, Engine, ExecResult, MAX_CHANGE_ROWS_PER_EVENT};
use super::helpers::{
    build_row_group_predicate, collect_expr_column_indices, eval_expr_to_scalar,
    eval_where_clause_batch,
};
use crate::sql::parser::ast::{BinOp, Expr};

pub fn exec_delete(
    engine: &Engine,
    db: &str,
    table: &str,
    where_clause: &Option<Expr>,
    change_rows: &mut Vec<crate::storage::change_stream::ChangeRow>,
    row_images_truncated: &mut bool,
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

    engine.flush_all_buffers()?;

    let table_dir = engine.data_dir().join(db).join(table);
    if !table_dir.exists() {
        return Ok(ExecResult::Ok { rows_affected: 0 });
    }

    let mgr = engine.segment_mgr();
    let mut total_deleted: u64 = 0;
    let predicate = build_row_group_predicate(where_clause, &table_schema);
    let predicate_projection = where_clause
        .as_ref()
        .map(|expr| collect_expr_column_indices(expr, &table_schema))
        .unwrap_or_default();

    mgr.with_table_mutation_lock(db, table, || {
        let mut segment_files = prune_segments_by_partition(
            SegmentManager::discover_segments(&table_dir)?,
            where_clause,
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

                    if let Some(ref where_expr) = where_clause {
                        let batch = if predicate_projection.is_empty() {
                            reader.read_row_group(rg_idx)?
                        } else {
                            reader.read_row_group_with_projection(rg_idx, &predicate_projection)?
                        };
                        let mask = eval_where_clause_batch(where_expr, &batch, &batch.schema)?;
                        let full_batch = if predicate_projection.is_empty() {
                            batch.clone()
                        } else {
                            reader.read_row_group(rg_idx)?
                        };
                        for (i, matches) in mask.into_iter().enumerate() {
                            if matches
                                && delete_bitmap::mark_deleted(&mut new_bitmap, row_id + i as u64)
                            {
                                newly_deleted += 1;
                                if change_rows.len() < MAX_CHANGE_ROWS_PER_EVENT {
                                    change_rows.push(change_row_from_images(
                                        &table_schema,
                                        Some(&full_batch),
                                        i,
                                        None,
                                        0,
                                    ));
                                } else {
                                    *row_images_truncated = true;
                                }
                            }
                        }
                        row_id += batch.num_rows() as u64;
                    } else {
                        let batch = if change_rows.len() < MAX_CHANGE_ROWS_PER_EVENT {
                            Some(reader.read_row_group(rg_idx)?)
                        } else {
                            None
                        };
                        for i in 0..rg_rows {
                            if delete_bitmap::mark_deleted(&mut new_bitmap, row_id + i) {
                                newly_deleted += 1;
                                if let Some(batch) = &batch {
                                    if change_rows.len() >= MAX_CHANGE_ROWS_PER_EVENT {
                                        *row_images_truncated = true;
                                        continue;
                                    }
                                    change_rows.push(change_row_from_images(
                                        &table_schema,
                                        Some(batch),
                                        i as usize,
                                        None,
                                        0,
                                    ));
                                } else {
                                    *row_images_truncated = true;
                                }
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
mod tests {
    use crate::protocol::types::DataType;
    use crate::storage::catalog::schema::{PartitionType, TableMeta};
    use crate::types::batch::ColumnDef;
    use crate::types::scalar::ScalarValue;

    use super::*;

    fn partitioned_meta() -> (Schema, TableMeta) {
        let columns = vec![
            ColumnDef::new("id", DataType::Int64),
            ColumnDef::new("deal_date", DataType::Int32),
        ];
        let mut meta = TableMeta::new("main", "ticks", columns.clone());
        meta.partition_key = "deal_date".to_string();
        meta.partition_type = PartitionType::Year;
        meta.update_partition_key_idx();
        (Schema::new(columns), meta)
    }

    #[test]
    fn delete_partition_eq_key_uses_partition_function() {
        let (schema, meta) = partitioned_meta();
        let where_clause = Some(Expr::BinaryOp {
            left: Box::new(Expr::Column("deal_date".to_string())),
            op: BinOp::Eq,
            right: Box::new(Expr::Literal(ScalarValue::Int64(Some(20260108)))),
        });

        assert_eq!(
            delete_partition_eq_key(&where_clause, &schema, &meta),
            Some("2026".to_string())
        );
    }

    #[test]
    fn prune_segments_by_partition_keeps_only_matching_partition_files() {
        let (schema, meta) = partitioned_meta();
        let where_clause = Some(Expr::BinaryOp {
            left: Box::new(Expr::Column("deal_date".to_string())),
            op: BinOp::Eq,
            right: Box::new(Expr::Literal(ScalarValue::Int64(Some(20260108)))),
        });
        let files = vec![
            PathBuf::from("p2025_0001.dat"),
            PathBuf::from("p2026_0002.dat"),
            PathBuf::from("seg_00003.dat"),
        ];

        let pruned = prune_segments_by_partition(files, &where_clause, &schema, &meta);

        assert_eq!(
            pruned,
            vec![
                PathBuf::from("p2026_0002.dat"),
                PathBuf::from("seg_00003.dat")
            ]
        );
    }

    #[test]
    fn cluster_key_eq_predicate_builds_exact_bounds() {
        let (schema, mut meta) = partitioned_meta();
        meta.cluster_key = vec!["deal_date".to_string(), "id".to_string()];
        let predicate = crate::storage::segment::RowGroupPredicate::new(vec![
            crate::storage::segment::RangeClause {
                column_index: schema.index_of("deal_date").unwrap(),
                op: RangeOp::Eq,
                value: ScalarValue::Int64(Some(20260108)),
            },
        ]);

        let bounds = cluster_key_bounds(&schema, &meta, &predicate).expect("bounds");

        assert_eq!(bounds.column_index, schema.index_of("deal_date").unwrap());
        assert_eq!(
            bounds.lower,
            Some((ScalarValue::Int64(Some(20260108)), true))
        );
        assert_eq!(
            bounds.upper,
            Some((ScalarValue::Int64(Some(20260108)), true))
        );
    }
}
