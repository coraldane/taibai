#![allow(dead_code)]

use crate::sql::optimizer::partition_pruning::PartitionFilter;
use crate::sql::planner::{LogicalExpr, LogicalPlan, TimeRange};
use crate::storage::catalog::schema::TableMeta;
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

    pub is_count_star: bool,
    pub has_aggregate: bool,
    pub has_order_by: bool,
    pub has_group_by: bool,
    pub has_join: bool,
    pub limit: Option<usize>,

    pub cluster_key: Option<Vec<String>>,
    pub partition_info: Option<PartitionScanInfo>,
}

#[derive(Debug, Clone)]
pub struct InPredicate {
    pub column_index: usize,
    pub values: Vec<ScalarValue>,
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
            is_count_star: false,
            has_aggregate: false,
            has_order_by: false,
            has_group_by: false,
            has_join: false,
            limit: None,
            cluster_key: None,
            partition_info: None,
        }
    }

    pub fn has_predicates(&self) -> bool {
        !self.eq_predicates.is_empty()
            || !self.range_predicates.is_empty()
            || !self.in_predicates.is_empty()
            || self.remaining_predicate.is_some()
    }

    pub fn has_eq_predicate(&self) -> bool {
        !self.eq_predicates.is_empty()
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

    pub fn bloom_check_columns(&self) -> Vec<(usize, &ScalarValue)> {
        self.eq_predicates
            .iter()
            .map(|(idx, v)| (*idx, v))
            .collect()
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

    if !table_meta.cluster_key.is_empty() {
        spec.cluster_key = Some(table_meta.cluster_key.clone());
    }

    if table_meta.is_partitioned() {
        let pk_idx = table_meta.get_partition_column_index();
        spec.partition_info = Some(PartitionScanInfo {
            time_range: None,
            partition_filter: None,
            partition_key_col_idx: pk_idx,
            is_partitioned: true,
        });
    }

    collect_from_plan(plan, table_schema, &mut spec);

    spec
}

fn collect_expr_columns(expr: &LogicalExpr, cols: &mut Vec<usize>) {
    match expr {
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

fn collect_from_plan(plan: &LogicalPlan, schema: &Schema, spec: &mut ScanSpec) {
    match plan {
        LogicalPlan::Scan(scan) => {
            if let Some(filter) = &scan.pushed_filter {
                extract_predicates(filter, schema, spec);
            }
            if let Some(proj) = &scan.projection {
                spec.projection = Some(proj.clone());
            }
            if scan.time_range.is_some() || scan.partition_filter.is_some() {
                if let Some(ref mut pi) = spec.partition_info {
                    pi.time_range = scan.time_range.clone();
                    pi.partition_filter = scan.partition_filter.clone();
                }
            }
        }
        LogicalPlan::Filter(f) => collect_from_plan(&f.input, schema, spec),
        LogicalPlan::Project(p) => {
            collect_from_plan(&p.input, schema, spec);
            if spec.remaining_predicate.is_none() && !spec.has_join {
                if let Some(cols) = plain_projection_columns(&p.expressions) {
                    spec.projection = Some(cols);
                }
            }
        }
        LogicalPlan::Aggregate(a) => {
            spec.has_aggregate = true;
            spec.has_group_by = !a.group_by.is_empty();
            collect_from_plan(&a.input, schema, spec);
        }
        LogicalPlan::Sort(s) => {
            spec.has_order_by = true;
            collect_from_plan(&s.input, schema, spec);
        }
        LogicalPlan::Limit(l) => {
            spec.limit = Some(l.limit);
            collect_from_plan(&l.input, schema, spec);
        }
        LogicalPlan::TopN(t) => {
            spec.has_order_by = true;
            spec.limit = Some(t.limit);
            collect_from_plan(&t.input, schema, spec);
        }
        LogicalPlan::Distinct(d) => collect_from_plan(&d.input, schema, spec),
        LogicalPlan::Window(w) => collect_from_plan(&w.input, schema, spec),
        LogicalPlan::Join(j) => {
            spec.has_join = true;
            collect_from_plan(&j.left, schema, spec);
            collect_from_plan(&j.right, schema, spec);
        }
        LogicalPlan::SemiJoin(s) => {
            spec.has_join = true;
            collect_from_plan(&s.left, schema, spec);
            collect_from_plan(&s.right, schema, spec);
        }
        LogicalPlan::Empty => {}
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
                spec.range_predicates.push(RangeClause {
                    column_index: *idx,
                    op: crate::storage::partition::pruning::RangeOp::Ge,
                    value: lo.clone(),
                });
                spec.range_predicates.push(RangeClause {
                    column_index: *idx,
                    op: crate::storage::partition::pruning::RangeOp::Le,
                    value: hi.clone(),
                });
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
                    spec.in_predicates.push(InPredicate {
                        column_index: *idx,
                        values: values.clone(),
                    });
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
        (DataType::Int32, ScalarValue::Int64(Some(v))) => {
            i32::try_from(*v).ok().map(|v| ScalarValue::Int32(Some(v)))
        }
        (DataType::Int16, ScalarValue::Int64(Some(v))) => {
            i16::try_from(*v).ok().map(|v| ScalarValue::Int16(Some(v)))
        }
        (DataType::Int8, ScalarValue::Int64(Some(v))) => {
            i8::try_from(*v).ok().map(|v| ScalarValue::Int8(Some(v)))
        }
        (DataType::UInt32, ScalarValue::Int64(Some(v))) => {
            u32::try_from(*v).ok().map(|v| ScalarValue::UInt32(Some(v)))
        }
        (DataType::UInt16, ScalarValue::Int64(Some(v))) => {
            u16::try_from(*v).ok().map(|v| ScalarValue::UInt16(Some(v)))
        }
        (DataType::UInt8, ScalarValue::Int64(Some(v))) => {
            u8::try_from(*v).ok().map(|v| ScalarValue::UInt8(Some(v)))
        }
        (DataType::Float32, ScalarValue::Float64(Some(v))) => {
            Some(ScalarValue::Float32(Some(*v as f32)))
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
        _ => Some(value.clone()),
    }
}
