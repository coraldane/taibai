//! Row-group predicate construction and segment zone-map pre-checks used by
//! the scan/DML planners.

use crate::types::batch::Schema;
use crate::types::scalar::ScalarValue;

use crate::sql::parser::ast::{BinOp, Expr};

pub fn build_row_group_predicate(
    where_clause: &Option<Expr>,
    schema: &Schema,
) -> Option<crate::storage::segment::RowGroupPredicate> {
    let expr = where_clause.as_ref()?;
    let mut clauses = Vec::new();
    extract_range_clauses(expr, schema, &mut clauses);
    if clauses.is_empty() {
        None
    } else {
        Some(crate::storage::segment::RowGroupPredicate::new(clauses))
    }
}

pub fn collect_expr_column_indices(expr: &Expr, schema: &Schema) -> Vec<usize> {
    let mut indices = Vec::new();
    collect_expr_column_indices_inner(expr, schema, &mut indices);
    indices.sort_unstable();
    indices.dedup();
    indices
}

fn collect_expr_column_indices_inner(expr: &Expr, schema: &Schema, indices: &mut Vec<usize>) {
    match expr {
        Expr::Column(name) => {
            if let Some(idx) = schema.index_of(name) {
                indices.push(idx);
            }
        }
        Expr::QualifiedColumn { column, .. } => {
            if let Some(idx) = schema.index_of(column) {
                indices.push(idx);
            }
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_expr_column_indices_inner(left, schema, indices);
            collect_expr_column_indices_inner(right, schema, indices);
        }
        Expr::UnaryMinus(inner)
        | Expr::UnaryNot(inner)
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner)
        | Expr::Exists(inner) => collect_expr_column_indices_inner(inner, schema, indices),
        Expr::Correlated(_) => {}
        Expr::In { expr, values, .. } => {
            collect_expr_column_indices_inner(expr, schema, indices);
            for item in values {
                collect_expr_column_indices_inner(item, schema, indices);
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_expr_column_indices_inner(expr, schema, indices);
            collect_expr_column_indices_inner(low, schema, indices);
            collect_expr_column_indices_inner(high, schema, indices);
        }
        Expr::Case {
            operand,
            when_clauses,
            else_clause,
        } => {
            if let Some(operand) = operand {
                collect_expr_column_indices_inner(operand, schema, indices);
            }
            for (when_expr, then_expr) in when_clauses {
                collect_expr_column_indices_inner(when_expr, schema, indices);
                collect_expr_column_indices_inner(then_expr, schema, indices);
            }
            if let Some(else_clause) = else_clause {
                collect_expr_column_indices_inner(else_clause, schema, indices);
            }
        }
        Expr::Like { expr, pattern, .. } => {
            collect_expr_column_indices_inner(expr, schema, indices);
            collect_expr_column_indices_inner(pattern, schema, indices);
        }
        Expr::Function { args, .. } => {
            for arg in args {
                collect_expr_column_indices_inner(arg, schema, indices);
            }
        }
        Expr::WindowFunction {
            partition_by,
            order_by,
            ..
        } => {
            for expr in partition_by {
                collect_expr_column_indices_inner(expr, schema, indices);
            }
            for item in order_by {
                collect_expr_column_indices_inner(&item.expr, schema, indices);
            }
        }
        Expr::Literal(_) | Expr::Placeholder(_) | Expr::Subquery(_) => {}
    }
}

fn extract_range_clauses(
    expr: &Expr,
    schema: &Schema,
    clauses: &mut Vec<crate::storage::segment::RangeClause>,
) {
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            BinOp::Eq | BinOp::Lt | BinOp::Lte | BinOp::Gt | BinOp::Gte => {
                if let Expr::Column(col_name) = left.as_ref() {
                    if let Expr::Literal(ref val) = right.as_ref() {
                        if let Some(idx) = schema.index_of(col_name) {
                            let op = match op {
                                BinOp::Eq => crate::storage::segment::RangeOp::Eq,
                                BinOp::Lt => crate::storage::segment::RangeOp::Lt,
                                BinOp::Lte => crate::storage::segment::RangeOp::Le,
                                BinOp::Gt => crate::storage::segment::RangeOp::Gt,
                                BinOp::Gte => crate::storage::segment::RangeOp::Ge,
                                _ => unreachable!(),
                            };
                            clauses.push(crate::storage::segment::RangeClause {
                                column_index: idx,
                                op,
                                value: val.clone(),
                            });
                        }
                    }
                }
            }
            BinOp::And => {
                extract_range_clauses(left, schema, clauses);
                extract_range_clauses(right, schema, clauses);
            }
            _ => {}
        },
        Expr::UnaryNot(inner) => {
            if let Expr::IsNull(col_expr) = inner.as_ref() {
                if let Expr::Column(col_name) = col_expr.as_ref() {
                    if let Some(idx) = schema.index_of(col_name) {
                        clauses.push(crate::storage::segment::RangeClause {
                            column_index: idx,
                            op: crate::storage::segment::RangeOp::Eq,
                            value: ScalarValue::null_for_type(schema.columns[idx].data_type),
                        });
                    }
                }
            }
        }
        _ => {}
    }
}

/// Byte budget for buffered change-event row images (UPDATE post-images /
/// DELETE pre-images). These batches are held until every segment rewrite
pub(crate) fn segment_may_match(
    reader: &crate::storage::segment::SegmentReader,
    table_schema: &Schema,
    predicate: Option<&crate::storage::segment::RowGroupPredicate>,
) -> bool {
    let Some(predicate) = predicate else {
        return true;
    };
    // The predicate's column indices are catalog-space; zone maps live in the
    // segment's footer order. After a DROP/RENAME COLUMN the two drift, and
    // testing catalog indices against footer zone maps prunes (or keeps) the
    // WRONG segments — re-anchor the predicate onto this segment first.
    //
    // Clauses on columns the footer predates cannot consult a zone map, but
    // under the default-fill semantics every row carries the column's
    // DEFAULT: the clause reduces to one constant comparison, which prunes
    // precisely. The old blanket "absent column cannot match" silently
    // skipped rows a DELETE was required to remove while UPDATE already
    // matched them. Undecidable constants keep the segment; the row-wise
    // evaluation on default-filled batches decides.
    let mut present_clauses = Vec::new();
    for clause in &predicate.clauses {
        if crate::storage::segment::reader::any_column_absent_in_schema(
            table_schema,
            reader.schema(),
            &[clause.column_index],
        ) {
            let Some(column) = table_schema.columns.get(clause.column_index) else {
                return true;
            };
            match default_clause_decision(column, &clause.op, &clause.value) {
                Some(true) => {}
                Some(false) => return false,
                None => {}
            }
        } else {
            present_clauses.push(clause.clone());
        }
    }
    if present_clauses.is_empty() {
        return true;
    }
    let reduced = crate::storage::partition::pruning::RowGroupPredicate::new(present_clauses);
    // Every remaining clause's column exists in the footer, so alignment can
    // only fail on an out-of-range catalog index: keep the segment and let
    // the row-wise evaluation surface the error.
    let Ok(aligned) = reader.align_row_group_predicate(table_schema, &reduced) else {
        return true;
    };
    let segment_zone_maps = reader.segment_zone_maps();
    if !segment_zone_maps.is_empty() {
        return aligned.matches_row_group(segment_zone_maps);
    }
    (0..reader.num_row_groups()).any(|rg_idx| aligned.matches_row_group(&reader.zone_maps(rg_idx)))
}

/// Decide one range clause against the constant value a pre-ALTER segment's
/// rows carry for `column`. `None` keeps the segment (undecidable).
fn default_clause_decision(
    column: &crate::types::batch::ColumnDef,
    op: &crate::storage::partition::pruning::RangeOp,
    value: &crate::types::scalar::ScalarValue,
) -> Option<bool> {
    use crate::storage::index::sort_key::compare_scalars_for_sort;
    use crate::storage::partition::pruning::RangeOp;
    let default = crate::storage::segment::reader::column_default_value(column);
    if default.is_null() {
        // NULL compares UNKNOWN against everything: no row matches.
        return Some(false);
    }
    let ordering = compare_scalars_for_sort(&default, value);
    Some(match op {
        RangeOp::Eq => ordering == std::cmp::Ordering::Equal,
        RangeOp::Lt => ordering == std::cmp::Ordering::Less,
        RangeOp::Le => ordering != std::cmp::Ordering::Greater,
        RangeOp::Gt => ordering == std::cmp::Ordering::Greater,
        RangeOp::Ge => ordering != std::cmp::Ordering::Less,
    })
}
