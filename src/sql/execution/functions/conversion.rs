/// Type conversion SQL functions.
use crate::common::error::Result;
use crate::types::scalar::ScalarValue;

pub fn to_int(args: &[ScalarValue]) -> Result<ScalarValue> {
    match args.first() {
        Some(ScalarValue::Int64(Some(v))) => Ok(ScalarValue::Int64(Some(*v))),
        Some(ScalarValue::Int32(Some(v))) => Ok(ScalarValue::Int64(Some(*v as i64))),
        Some(ScalarValue::Float64(Some(v))) => Ok(ScalarValue::Int64(Some(*v as i64))),
        Some(ScalarValue::Float32(Some(v))) => Ok(ScalarValue::Int64(Some(*v as i64))),
        Some(ScalarValue::VarChar(Some(s))) => match s.parse::<i64>() {
            Ok(v) => Ok(ScalarValue::Int64(Some(v))),
            Err(_) => Ok(ScalarValue::Int64(None)),
        },
        Some(ScalarValue::Bool(Some(v))) => Ok(ScalarValue::Int64(Some(if *v { 1 } else { 0 }))),
        _ => Ok(ScalarValue::Int64(None)),
    }
}

pub fn to_float(args: &[ScalarValue]) -> Result<ScalarValue> {
    match args.first() {
        Some(ScalarValue::Float64(Some(v))) => Ok(ScalarValue::Float64(Some(*v))),
        Some(ScalarValue::Float32(Some(v))) => Ok(ScalarValue::Float64(Some(*v as f64))),
        Some(ScalarValue::Int64(Some(v))) => Ok(ScalarValue::Float64(Some(*v as f64))),
        Some(ScalarValue::Int32(Some(v))) => Ok(ScalarValue::Float64(Some(*v as f64))),
        Some(ScalarValue::VarChar(Some(s))) => match s.parse::<f64>() {
            Ok(v) => Ok(ScalarValue::Float64(Some(v))),
            Err(_) => Ok(ScalarValue::Float64(None)),
        },
        _ => Ok(ScalarValue::Float64(None)),
    }
}

pub fn to_string(args: &[ScalarValue]) -> Result<ScalarValue> {
    match args.first() {
        Some(v) if !v.is_null() => Ok(ScalarValue::VarChar(Some(v.to_string()))),
        _ => Ok(ScalarValue::VarChar(None)),
    }
}

pub fn to_bool(args: &[ScalarValue]) -> Result<ScalarValue> {
    match args.first() {
        Some(ScalarValue::Bool(Some(v))) => Ok(ScalarValue::Bool(Some(*v))),
        Some(ScalarValue::Int64(Some(v))) => Ok(ScalarValue::Bool(Some(*v != 0))),
        Some(ScalarValue::Int32(Some(v))) => Ok(ScalarValue::Bool(Some(*v != 0))),
        Some(ScalarValue::Float64(Some(v))) => Ok(ScalarValue::Bool(Some(*v != 0.0))),
        Some(ScalarValue::VarChar(Some(s))) => {
            let lower = s.to_lowercase();
            Ok(ScalarValue::Bool(Some(
                lower == "true" || lower == "1" || lower == "yes",
            )))
        }
        _ => Ok(ScalarValue::Bool(None)),
    }
}
