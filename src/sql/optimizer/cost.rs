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
        crate::sql::execution::operators::sort::compare_scalars_null_last(
            value,
            &bucket.lower_bound,
        )
        .is_ge()
            && crate::sql::execution::operators::sort::compare_scalars_null_last(
                value,
                &bucket.upper_bound,
            )
            .is_le()
    })?;
    if !crate::sql::execution::operators::sort::compare_scalars_null_last(
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
        let lower_cmp = crate::sql::execution::operators::sort::compare_scalars_null_last(
            value,
            &bucket.lower_bound,
        );
        if lower_cmp.is_lt() {
            return Some(preceding as f64 / total as f64);
        }
        let upper_cmp = crate::sql::execution::operators::sort::compare_scalars_null_last(
            value,
            &bucket.upper_bound,
        );
        if upper_cmp.is_gt() {
            preceding = preceding.saturating_add(bucket.sample_count as u64);
            continue;
        }

        let fraction = if crate::sql::execution::operators::sort::compare_scalars_null_last(
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
#[path = "cost_test.rs"]
mod tests;
