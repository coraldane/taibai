use crate::sql::parser::ast::BinOp;
use crate::sql::planner::LogicalExpr;
use crate::types::scalar::ScalarValue;

pub struct TableStats {
    pub row_count: i64,
    pub column_stats: std::collections::HashMap<String, ColumnStats>,
}

pub struct ColumnStats {
    pub distinct_count: i64,
    pub null_count: i64,
    pub min_value: Option<ScalarValue>,
    pub max_value: Option<ScalarValue>,
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
        zone_map_count: usize,
        limit: Option<usize>,
    ) -> f64 {
        let mut cost = self.stats.row_count as f64;

        if let Some(f) = filter {
            cost *= self.estimate_selectivity(f);
        }

        if zone_map_count > 0 {
            cost *= 0.1;
        }

        if let Some(lim) = limit {
            cost = cost.min(lim as f64);
        }

        cost
    }

    fn estimate_selectivity(&self, expr: &LogicalExpr) -> f64 {
        match expr {
            LogicalExpr::BinaryOp { left, op, right } => {
                match op {
                    BinOp::Eq => {
                        if let LogicalExpr::Column(idx) = **left {
                            if let LogicalExpr::Literal(_) = **right {
                                if let Some(col_name) = self.column_name_by_index(idx) {
                                    if let Some(stats) = self.stats.column_stats.get(&col_name) {
                                        if stats.distinct_count > 0 {
                                            return 1.0 / stats.distinct_count as f64;
                                        }
                                    }
                                }
                            }
                        }
                        0.1
                    }
                    BinOp::Lt | BinOp::Lte | BinOp::Gt | BinOp::Gte => 0.33,
                    BinOp::And => {
                        let l = self.estimate_selectivity(left);
                        let r = self.estimate_selectivity(right);
                        l * r
                    }
                    BinOp::Or => {
                        let l = self.estimate_selectivity(left);
                        let r = self.estimate_selectivity(right);
                        l + r - l * r
                    }
                    _ => 1.0,
                }
            }
            _ => 1.0,
        }
    }

    fn column_name_by_index(&self, _idx: usize) -> Option<String> {
        None
    }
}
