/// Aggregate function implementations for use by the function registry.
/// The actual aggregation logic is in the HashAggregateExec operator.
/// This module provides accumulator implementations.
use crate::common::error::Result;
use crate::protocol::types::DataType;
use crate::types::scalar::ScalarValue;

use super::registry::{Accumulator, AggregateFunction};

// --- COUNT ---
pub struct CountAggregate;
pub struct CountAccumulator { count: i64 }

impl AggregateFunction for CountAggregate {
    fn name(&self) -> &str { "count" }
    fn new_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(CountAccumulator { count: 0 })
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DataType { DataType::Int64 }
}

impl Accumulator for CountAccumulator {
    fn update(&mut self, value: &ScalarValue) -> Result<()> {
        if !value.is_null() { self.count += 1; }
        Ok(())
    }
    fn finalize(&self) -> Result<ScalarValue> {
        Ok(ScalarValue::Int64(Some(self.count)))
    }
    fn reset(&mut self) { self.count = 0; }
}

// --- SUM ---
pub struct SumAggregate;
pub struct SumAccumulator { sum: f64, has_value: bool }

impl AggregateFunction for SumAggregate {
    fn name(&self) -> &str { "sum" }
    fn new_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(SumAccumulator { sum: 0.0, has_value: false })
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DataType { DataType::Float64 }
}

impl Accumulator for SumAccumulator {
    fn update(&mut self, value: &ScalarValue) -> Result<()> {
        if let Some(v) = scalar_to_f64(value) {
            self.sum += v;
            self.has_value = true;
        }
        Ok(())
    }
    fn finalize(&self) -> Result<ScalarValue> {
        if self.has_value { Ok(ScalarValue::Float64(Some(self.sum))) }
        else { Ok(ScalarValue::Float64(None)) }
    }
    fn reset(&mut self) { self.sum = 0.0; self.has_value = false; }
}

// --- AVG ---
pub struct AvgAggregate;
pub struct AvgAccumulator { sum: f64, count: i64 }

impl AggregateFunction for AvgAggregate {
    fn name(&self) -> &str { "avg" }
    fn new_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(AvgAccumulator { sum: 0.0, count: 0 })
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DataType { DataType::Float64 }
}

impl Accumulator for AvgAccumulator {
    fn update(&mut self, value: &ScalarValue) -> Result<()> {
        if let Some(v) = scalar_to_f64(value) {
            self.sum += v;
            self.count += 1;
        }
        Ok(())
    }
    fn finalize(&self) -> Result<ScalarValue> {
        if self.count > 0 { Ok(ScalarValue::Float64(Some(self.sum / self.count as f64))) }
        else { Ok(ScalarValue::Float64(None)) }
    }
    fn reset(&mut self) { self.sum = 0.0; self.count = 0; }
}

// --- MIN ---
pub struct MinAggregate;
pub struct MinAccumulator { min: Option<ScalarValue> }

impl AggregateFunction for MinAggregate {
    fn name(&self) -> &str { "min" }
    fn new_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(MinAccumulator { min: None })
    }
    fn return_type(&self, arg_types: &[DataType]) -> DataType {
        arg_types.first().copied().unwrap_or(DataType::Int64)
    }
}

impl Accumulator for MinAccumulator {
    fn update(&mut self, value: &ScalarValue) -> Result<()> {
        if value.is_null() { return Ok(()); }
        match &self.min {
            None => self.min = Some(value.clone()),
            Some(current) => {
                if compare_scalars(value, current) == std::cmp::Ordering::Less {
                    self.min = Some(value.clone());
                }
            }
        }
        Ok(())
    }
    fn finalize(&self) -> Result<ScalarValue> {
        Ok(self.min.clone().unwrap_or(ScalarValue::Int64(None)))
    }
    fn reset(&mut self) { self.min = None; }
}

// --- MAX ---
pub struct MaxAggregate;
pub struct MaxAccumulator { max: Option<ScalarValue> }

impl AggregateFunction for MaxAggregate {
    fn name(&self) -> &str { "max" }
    fn new_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(MaxAccumulator { max: None })
    }
    fn return_type(&self, arg_types: &[DataType]) -> DataType {
        arg_types.first().copied().unwrap_or(DataType::Int64)
    }
}

impl Accumulator for MaxAccumulator {
    fn update(&mut self, value: &ScalarValue) -> Result<()> {
        if value.is_null() { return Ok(()); }
        match &self.max {
            None => self.max = Some(value.clone()),
            Some(current) => {
                if compare_scalars(value, current) == std::cmp::Ordering::Greater {
                    self.max = Some(value.clone());
                }
            }
        }
        Ok(())
    }
    fn finalize(&self) -> Result<ScalarValue> {
        Ok(self.max.clone().unwrap_or(ScalarValue::Int64(None)))
    }
    fn reset(&mut self) { self.max = None; }
}

fn scalar_to_f64(v: &ScalarValue) -> Option<f64> {
    match v {
        ScalarValue::Int64(Some(v)) => Some(*v as f64),
        ScalarValue::Int32(Some(v)) => Some(*v as f64),
        ScalarValue::Float64(Some(v)) => Some(*v),
        ScalarValue::Float32(Some(v)) => Some(*v as f64),
        _ => None,
    }
}

fn compare_scalars(a: &ScalarValue, b: &ScalarValue) -> std::cmp::Ordering {
    match (a, b) {
        (ScalarValue::Int64(Some(l)), ScalarValue::Int64(Some(r))) => l.cmp(r),
        (ScalarValue::Int32(Some(l)), ScalarValue::Int32(Some(r))) => l.cmp(r),
        (ScalarValue::Float64(Some(l)), ScalarValue::Float64(Some(r))) => {
            l.partial_cmp(r).unwrap_or(std::cmp::Ordering::Equal)
        }
        (ScalarValue::VarChar(Some(l)), ScalarValue::VarChar(Some(r))) => l.cmp(r),
        _ => std::cmp::Ordering::Equal,
    }
}
