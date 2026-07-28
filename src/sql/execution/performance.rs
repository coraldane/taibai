use crate::common::error::Result;
use crate::protocol::types::DataType;
use crate::types::batch::{ColumnDef, RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

use super::engine::{Engine, ExecResult};

pub(crate) fn exec_show_performance_baselines(
    engine: &Engine,
    database: &str,
) -> Result<ExecResult> {
    let schema = Schema::new(vec![
        ColumnDef::new("Fingerprint", DataType::VarChar),
        ColumnDef::new("Environment", DataType::VarChar),
        ColumnDef::new("Version", DataType::VarChar),
        ColumnDef::new("QueryShape", DataType::VarChar),
        ColumnDef::new("Samples", DataType::Int64),
        ColumnDef::new("Status", DataType::VarChar),
        ColumnDef::new("RegressionReasons", DataType::VarChar),
        ColumnDef::new("BaselineElapsedMs", DataType::Float64),
        ColumnDef::new("LastElapsedMs", DataType::Float64),
        ColumnDef::new("ElapsedRatio", DataType::Float64),
        ColumnDef::new("BaselinePeakMemory", DataType::Int64),
        ColumnDef::new("LastPeakMemory", DataType::Int64),
        ColumnDef::new("BaselineSpillBytes", DataType::Int64),
        ColumnDef::new("LastSpillBytes", DataType::Int64),
        ColumnDef::new("BaselineRowError", DataType::Float64),
        ColumnDef::new("LastRowError", DataType::Float64),
        ColumnDef::new("UpdatedAt", DataType::Timestamp),
    ]);
    let mut batch = RecordBatch::new(schema.clone());
    for entry in engine.performance_baselines().entries(database) {
        let values = [
            ScalarValue::VarChar(Some(entry.fingerprint)),
            ScalarValue::VarChar(Some(entry.environment)),
            ScalarValue::VarChar(Some(entry.version)),
            ScalarValue::VarChar(Some(entry.query_shape)),
            ScalarValue::Int64(Some(saturating_i64(entry.samples))),
            ScalarValue::VarChar(Some(entry.status)),
            ScalarValue::VarChar(Some(entry.regression_reasons.join(","))),
            ScalarValue::Float64(Some(entry.baseline_elapsed_ms)),
            ScalarValue::Float64(Some(entry.last_elapsed_ms)),
            ScalarValue::Float64(Some(ratio(
                entry.last_elapsed_ms,
                entry.baseline_elapsed_ms,
            ))),
            ScalarValue::Int64(Some(saturating_i64(
                entry.baseline_peak_memory_bytes.round().max(0.0) as u64,
            ))),
            ScalarValue::Int64(Some(saturating_i64(entry.last_peak_memory_bytes))),
            ScalarValue::Int64(Some(saturating_i64(
                entry.baseline_spill_bytes.round().max(0.0) as u64,
            ))),
            ScalarValue::Int64(Some(saturating_i64(entry.last_spill_bytes))),
            ScalarValue::Float64(Some(entry.baseline_row_error)),
            ScalarValue::Float64(Some(entry.last_row_error)),
            ScalarValue::Timestamp(Some(entry.updated_at)),
        ];
        for (column, value) in values.iter().enumerate() {
            batch.column_mut(column).unwrap().push(value)?;
        }
    }
    Ok(ExecResult::Query {
        schema,
        batches: vec![batch],
    })
}

pub(crate) fn exec_reset_performance_baselines(
    engine: &Engine,
    database: &str,
) -> Result<ExecResult> {
    Ok(ExecResult::Ok {
        rows_affected: engine.performance_baselines().reset(database)?,
    })
}

fn saturating_i64(value: u64) -> i64 {
    value.min(i64::MAX as u64) as i64
}

fn ratio(current: f64, baseline: f64) -> f64 {
    if baseline <= 0.0 {
        if current <= 0.0 {
            1.0
        } else {
            f64::MAX
        }
    } else {
        current / baseline
    }
}
