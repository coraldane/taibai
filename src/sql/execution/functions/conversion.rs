/// Type conversion SQL functions.
use super::registry::args_to_i64;
use crate::common::error::{Result, TaibaiError};
use crate::protocol::types::DataType;
use crate::sql::execution::helpers::coerce_scalar_for_dtype;
use crate::types::scalar::ScalarValue;

/// `CAST(expr AS type)` — desugared by the parser to
/// `cast(value, NULL::<target>, scale)`: the second argument is a NULL
/// literal whose own DataType is the conversion target.
pub fn cast(args: &[ScalarValue]) -> Result<ScalarValue> {
    let Some(target) = args.get(1).map(|v| v.data_type()) else {
        return Err(TaibaiError::Sql(
            "cast() requires a target type argument".into(),
        ));
    };
    let data_type = target;
    let scale = args_to_i64(args, 2).unwrap_or(0);

    let mut value = args.first().cloned().unwrap_or(ScalarValue::Int64(None));
    if value.is_null() {
        return Ok(ScalarValue::null_for_type(data_type));
    }

    // Strings reach numbers by parsing. Floats delegate to to_float; ints
    // accept "12" and truncate "12.9" toward zero (the MySQL CAST convention),
    // with unparseable text yielding NULL like to_int does.
    let from_text = if let ScalarValue::VarChar(Some(s)) = &value {
        match data_type {
            DataType::Float32 | DataType::Float64 => {
                Some(to_float(std::slice::from_ref(&value))?)
            }
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => Some(ScalarValue::Int64(parse_numeric_string(s))),
            _ => None,
        }
    } else {
        None
    };
    if let Some(parsed) = from_text {
        value = parsed;
    }

    match data_type {
        DataType::Timestamp => match value {
            ScalarValue::Timestamp(_) => Ok(value),
            ScalarValue::VarChar(Some(s)) => {
                let ts = crate::sql::execution::functions::date::parse_timestamp_smart(&s, 0);
                Ok(ScalarValue::Timestamp(ts))
            }
            ScalarValue::Int64(Some(n)) => Ok(ScalarValue::Timestamp(Some(
                crate::sql::execution::functions::date::parse_timestamp_from_int(n),
            ))),
            _ => Err(cast_unsupported(&value, data_type)),
        },
        DataType::Date => match value {
            ScalarValue::Date(_) => Ok(value),
            ScalarValue::VarChar(Some(s)) => {
                let days = crate::sql::execution::functions::date::parse_datetime_string(&s, 0)
                    .map(|ts| (ts / 86400) as i32);
                Ok(ScalarValue::Date(days))
            }
            ScalarValue::Int64(Some(n)) => Ok(ScalarValue::Date(Some(n as i32))),
            _ => Err(cast_unsupported(&value, data_type)),
        },
        DataType::Decimal => {
            if !(0..=18).contains(&scale) {
                return Err(TaibaiError::Sql(format!(
                    "invalid decimal scale {} in CAST",
                    scale
                )));
            }
            let multiplier = 10_i64.pow(scale as u32);
            let as_f64 = if let ScalarValue::VarChar(Some(s)) = &value {
                s.trim().parse::<f64>().ok()
            } else {
                value.numeric_f64()
            };
            if let Some(i) = value.integer_i128() {
                let scaled = i
                    .checked_mul(multiplier as i128)
                    .and_then(|v| i64::try_from(v).ok())
                    .ok_or_else(|| {
                        TaibaiError::Sql(format!(
                            "decimal value '{}' overflows scale {}",
                            i, scale
                        ))
                    })?;
                Ok(ScalarValue::Decimal(Some(scaled)))
            } else if let Some(f) = as_f64 {
                Ok(ScalarValue::Decimal(Some(
                    (f * multiplier as f64).round() as i64
                )))
            } else {
                Err(cast_unsupported(&value, data_type))
            }
        }
        // Time / Interval / Blob / Vector have no cross-type conversion; the
        // generic coercion below already passes same-type values through.
        _ => coerce_scalar_for_dtype(&value, data_type),
    }
}

fn cast_unsupported(value: &ScalarValue, data_type: DataType) -> TaibaiError {
    TaibaiError::Sql(format!(
        "cannot CAST {:?} to {:?}",
        value.data_type(),
        data_type
    ))
}

/// Parse text as an integer, truncating a fractional part toward zero.
fn parse_numeric_string(s: &str) -> Option<i64> {
    let s = s.trim();
    s.parse::<i64>()
        .ok()
        .or_else(|| s.parse::<f64>().ok().and_then(|f| f.is_finite().then(|| f.trunc() as i64)))
}

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
