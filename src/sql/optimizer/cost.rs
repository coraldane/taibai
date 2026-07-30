use std::collections::HashMap;

use crate::sql::parser::ast::BinOp;
use crate::sql::planner::LogicalExpr;
use crate::storage::catalog::schema::HistogramBucketMeta;
use crate::types::batch::Schema;
use crate::types::scalar::ScalarValue;

#[derive(Debug, Clone)]
pub struct TableStats {
    pub row_count: i64,
    pub schema: Schema,
    pub column_stats: HashMap<String, ColumnStats>,
}

#[derive(Debug, Clone)]
pub struct ColumnStats {
    pub distinct_count: i64,
    pub null_count: i64,
    pub min_value: Option<ScalarValue>,
    pub max_value: Option<ScalarValue>,
    pub histogram: Vec<HistogramBucketMeta>,
}

pub struct CostEstimator {
    pub stats: TableStats,
}

impl CostEstimator {
    pub fn new(stats: TableStats) -> Self {
        Self { stats }
    }

    pub fn estimate_scan_cost(
        &self,
        filter: Option<&LogicalExpr>,
        prunable_row_groups: usize,
        total_row_groups: usize,
        limit: Option<usize>,
    ) -> f64 {
        let selectivity = filter
            .map(|expr| self.estimate_selectivity(expr))
            .unwrap_or(1.0);
        let prune_ratio = if total_row_groups == 0 {
            1.0
        } else {
            (prunable_row_groups as f64 / total_row_groups as f64).clamp(0.0, 1.0)
        };
        let rows_after_pruning = self.stats.row_count.max(0) as f64 * (1.0 - prune_ratio);
        let mut cost = rows_after_pruning * selectivity;
        if let Some(limit) = limit {
            cost = cost.min(limit as f64);
        }
        cost.max(0.0)
    }

    pub fn estimate_rows(&self, filter: Option<&LogicalExpr>) -> u64 {
        let selectivity = filter
            .map(|expr| self.estimate_selectivity(expr))
            .unwrap_or(1.0);
        (self.stats.row_count.max(0) as f64 * selectivity)
            .ceil()
            .max(0.0) as u64
    }

    pub fn estimate_selectivity(&self, expr: &LogicalExpr) -> f64 {
        match expr {
            LogicalExpr::BinaryOp { left, op, right } => match op {
                BinOp::Eq => self
                    .column_literal(left, right)
                    .map(|(idx, value)| self.equality_selectivity(idx, value))
                    .unwrap_or(0.1),
                BinOp::Neq => self
                    .column_literal(left, right)
                    .map(|(idx, value)| 1.0 - self.equality_selectivity(idx, value))
                    .unwrap_or(0.9),
                BinOp::Lt | BinOp::Lte | BinOp::Gt | BinOp::Gte => self
                    .column_literal(left, right)
                    .map(|(idx, value)| self.range_selectivity(idx, value, op))
                    .unwrap_or(0.33),
                BinOp::And => (self.estimate_selectivity(left) * self.estimate_selectivity(right))
                    .clamp(0.0, 1.0),
                BinOp::Or => {
                    let left = self.estimate_selectivity(left);
                    let right = self.estimate_selectivity(right);
                    (left + right - left * right).clamp(0.0, 1.0)
                }
                _ => 1.0,
            },
            LogicalExpr::IsNull(inner) => self
                .column_index(inner)
                .and_then(|idx| self.column_stats(idx))
                .map(|stats| stats.null_count.max(0) as f64 / self.stats.row_count.max(1) as f64)
                .unwrap_or(0.05),
            LogicalExpr::IsNotNull(inner) => (1.0
                - self.estimate_selectivity(&LogicalExpr::IsNull(inner.clone())))
            .clamp(0.0, 1.0),
            LogicalExpr::In {
                expr,
                values,
                negated,
            } => {
                let selectivity = self
                    .column_index(expr)
                    .and_then(|idx| self.column_stats(idx))
                    .filter(|stats| stats.distinct_count > 0)
                    .map(|stats| values.len() as f64 / stats.distinct_count as f64)
                    .unwrap_or((values.len() as f64 * 0.1).min(1.0))
                    .clamp(0.0, 1.0);
                if *negated {
                    1.0 - selectivity
                } else {
                    selectivity
                }
            }
            _ => 1.0,
        }
    }

    fn equality_selectivity(&self, idx: usize, value: &ScalarValue) -> f64 {
        let Some(stats) = self.column_stats(idx) else {
            return 0.1;
        };
        if value_outside_bounds(value, stats) {
            return 0.0;
        }
        if let Some(selectivity) =
            histogram_equality_selectivity(stats, value, self.stats.row_count)
        {
            return selectivity;
        }
        if stats.distinct_count <= 0 {
            return 0.1;
        }
        let non_null_ratio =
            1.0 - stats.null_count.max(0) as f64 / self.stats.row_count.max(1) as f64;
        (non_null_ratio / stats.distinct_count as f64).clamp(0.0, 1.0)
    }

    fn range_selectivity(&self, idx: usize, value: &ScalarValue, op: &BinOp) -> f64 {
        let Some(stats) = self.column_stats(idx) else {
            return 0.33;
        };
        if let Some(selectivity) =
            histogram_range_selectivity(stats, value, op, self.stats.row_count)
        {
            return selectivity;
        }
        let (Some(min), Some(max), Some(value)) = (
            stats.min_value.as_ref().and_then(ScalarValue::numeric_f64),
            stats.max_value.as_ref().and_then(ScalarValue::numeric_f64),
            value.numeric_f64(),
        ) else {
            return 0.33;
        };
        if max <= min {
            return match op {
                BinOp::Lt | BinOp::Lte => f64::from(value >= max),
                BinOp::Gt | BinOp::Gte => f64::from(value <= min),
                _ => 0.33,
            };
        }
        let fraction = ((value - min) / (max - min)).clamp(0.0, 1.0);
        match op {
            BinOp::Lt | BinOp::Lte => fraction,
            BinOp::Gt | BinOp::Gte => 1.0 - fraction,
            _ => 0.33,
        }
    }

    fn column_literal<'a>(
        &self,
        left: &'a LogicalExpr,
        right: &'a LogicalExpr,
    ) -> Option<(usize, &'a ScalarValue)> {
        match (left, right) {
            (LogicalExpr::Column(idx), LogicalExpr::Literal(value))
            | (LogicalExpr::Literal(value), LogicalExpr::Column(idx)) => Some((*idx, value)),
            _ => None,
        }
    }

    fn column_index(&self, expr: &LogicalExpr) -> Option<usize> {
        match expr {
            LogicalExpr::Column(idx) => Some(*idx),
            _ => None,
        }
    }

    fn column_stats(&self, idx: usize) -> Option<&ColumnStats> {
        let name = &self.stats.schema.columns.get(idx)?.name;
        self.stats.column_stats.get(name)
    }
}

fn histogram_equality_selectivity(
    stats: &ColumnStats,
    value: &ScalarValue,
    row_count: i64,
) -> Option<f64> {
    let total = stats
        .histogram
        .iter()
        .map(|bucket| bucket.sample_count as u64)
        .sum::<u64>();
    if total == 0 {
        return None;
    }
    let bucket = stats.histogram.iter().find(|bucket| {
        crate::sql::execution::operators::sort::compare_scalars(value, &bucket.lower_bound).is_ge()
            && crate::sql::execution::operators::sort::compare_scalars(value, &bucket.upper_bound)
                .is_le()
    })?;
    if !crate::sql::execution::operators::sort::compare_scalars(
        &bucket.lower_bound,
        &bucket.upper_bound,
    )
    .is_eq()
    {
        return None;
    }
    let non_null_ratio = 1.0 - stats.null_count.max(0) as f64 / row_count.max(1) as f64;
    Some((bucket.sample_count as f64 / total as f64 * non_null_ratio).clamp(0.0, 1.0))
}

fn histogram_range_selectivity(
    stats: &ColumnStats,
    value: &ScalarValue,
    op: &BinOp,
    row_count: i64,
) -> Option<f64> {
    if stats.histogram.is_empty() {
        return None;
    }
    let cdf_inclusive = matches!(op, BinOp::Lte | BinOp::Gt);
    let cdf = histogram_cdf(&stats.histogram, value, cdf_inclusive)?;
    let non_null_ratio = 1.0 - stats.null_count.max(0) as f64 / row_count.max(1) as f64;
    let selectivity = match op {
        BinOp::Lt | BinOp::Lte => cdf,
        BinOp::Gt | BinOp::Gte => 1.0 - cdf,
        _ => return None,
    };
    Some((selectivity * non_null_ratio).clamp(0.0, 1.0))
}

fn histogram_cdf(
    histogram: &[HistogramBucketMeta],
    value: &ScalarValue,
    inclusive: bool,
) -> Option<f64> {
    let total = histogram
        .iter()
        .map(|bucket| bucket.sample_count as u64)
        .sum::<u64>();
    if total == 0 {
        return None;
    }
    let mut preceding = 0u64;
    for bucket in histogram {
        let lower_cmp =
            crate::sql::execution::operators::sort::compare_scalars(value, &bucket.lower_bound);
        if lower_cmp.is_lt() {
            return Some(preceding as f64 / total as f64);
        }
        let upper_cmp =
            crate::sql::execution::operators::sort::compare_scalars(value, &bucket.upper_bound);
        if upper_cmp.is_gt() {
            preceding = preceding.saturating_add(bucket.sample_count as u64);
            continue;
        }

        let fraction = if crate::sql::execution::operators::sort::compare_scalars(
            &bucket.lower_bound,
            &bucket.upper_bound,
        )
        .is_eq()
        {
            f64::from(inclusive && lower_cmp.is_eq())
        } else if let (Some(lower), Some(upper), Some(value)) = (
            bucket.lower_bound.numeric_f64(),
            bucket.upper_bound.numeric_f64(),
            value.numeric_f64(),
        ) {
            if upper <= lower {
                f64::from(inclusive)
            } else {
                ((value - lower) / (upper - lower)).clamp(0.0, 1.0)
            }
        } else if lower_cmp.is_eq() {
            f64::from(inclusive)
        } else if upper_cmp.is_eq() {
            if inclusive {
                1.0
            } else {
                0.5
            }
        } else {
            0.5
        };
        return Some((preceding as f64 + bucket.sample_count as f64 * fraction) / total as f64);
    }
    Some(1.0)
}

fn value_outside_bounds(value: &ScalarValue, stats: &ColumnStats) -> bool {
    let below = stats
        .min_value
        .as_ref()
        .and_then(|min| value.numeric_cmp(min))
        .is_some_and(|ordering| ordering == std::cmp::Ordering::Less);
    let above = stats
        .max_value
        .as_ref()
        .and_then(|max| value.numeric_cmp(max))
        .is_some_and(|ordering| ordering == std::cmp::Ordering::Greater);
    below || above
}

#[cfg(test)]
mod tests {
    use super::{ColumnStats, CostEstimator, TableStats};
    use crate::protocol::types::DataType;
    use crate::sql::parser::ast::BinOp;
    use crate::sql::planner::LogicalExpr;
    use crate::storage::catalog::schema::HistogramBucketMeta;
    use crate::types::batch::{ColumnDef, Schema};
    use crate::types::scalar::ScalarValue;
    use std::collections::HashMap;

    fn estimator() -> CostEstimator {
        let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
        let mut columns = HashMap::new();
        columns.insert(
            "id".to_string(),
            ColumnStats {
                distinct_count: 100,
                null_count: 0,
                min_value: Some(ScalarValue::Int64(Some(0))),
                max_value: Some(ScalarValue::Int64(Some(99))),
                histogram: Vec::new(),
            },
        );
        CostEstimator::new(TableStats {
            row_count: 1_000,
            schema,
            column_stats: columns,
        })
    }

    #[test]
    fn equality_uses_ndv_and_schema_mapping() {
        let predicate = LogicalExpr::BinaryOp {
            left: Box::new(LogicalExpr::Column(0)),
            op: BinOp::Eq,
            right: Box::new(LogicalExpr::Literal(ScalarValue::Int64(Some(42)))),
        };
        assert_eq!(estimator().estimate_rows(Some(&predicate)), 10);
    }

    #[test]
    fn impossible_equality_estimates_zero_rows() {
        let predicate = LogicalExpr::BinaryOp {
            left: Box::new(LogicalExpr::Column(0)),
            op: BinOp::Eq,
            right: Box::new(LogicalExpr::Literal(ScalarValue::Int64(Some(200)))),
        };
        assert_eq!(estimator().estimate_rows(Some(&predicate)), 0);
    }

    #[test]
    fn range_selectivity_uses_histogram_for_skewed_values() {
        let schema = Schema::new(vec![ColumnDef::new("value", DataType::Int64)]);
        let stats = ColumnStats {
            distinct_count: 11,
            null_count: 0,
            min_value: Some(ScalarValue::Int64(Some(0))),
            max_value: Some(ScalarValue::Int64(Some(10))),
            histogram: vec![
                HistogramBucketMeta {
                    lower_bound: ScalarValue::Int64(Some(0)),
                    upper_bound: ScalarValue::Int64(Some(0)),
                    sample_count: 90,
                },
                HistogramBucketMeta {
                    lower_bound: ScalarValue::Int64(Some(1)),
                    upper_bound: ScalarValue::Int64(Some(10)),
                    sample_count: 10,
                },
            ],
        };
        let estimator = CostEstimator::new(TableStats {
            row_count: 100,
            schema,
            column_stats: HashMap::from([("value".to_string(), stats)]),
        });
        let less_than_one = LogicalExpr::BinaryOp {
            left: Box::new(LogicalExpr::Column(0)),
            op: BinOp::Lt,
            right: Box::new(LogicalExpr::Literal(ScalarValue::Int64(Some(1)))),
        };
        let at_least_one = LogicalExpr::BinaryOp {
            left: Box::new(LogicalExpr::Column(0)),
            op: BinOp::Gte,
            right: Box::new(LogicalExpr::Literal(ScalarValue::Int64(Some(1)))),
        };
        let equal_zero = LogicalExpr::BinaryOp {
            left: Box::new(LogicalExpr::Column(0)),
            op: BinOp::Eq,
            right: Box::new(LogicalExpr::Literal(ScalarValue::Int64(Some(0)))),
        };

        assert_eq!(estimator.estimate_rows(Some(&less_than_one)), 90);
        assert_eq!(estimator.estimate_rows(Some(&at_least_one)), 10);
        assert_eq!(estimator.estimate_rows(Some(&equal_zero)), 90);
    }
}
