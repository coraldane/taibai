/// Mathematical SQL functions.
use crate::common::error::Result;
use crate::types::scalar::ScalarValue;

use super::registry::args_to_f64;

pub fn abs(args: &[ScalarValue]) -> Result<ScalarValue> {
    match args_to_f64(args, 0) {
        Some(v) => Ok(ScalarValue::Float64(Some(v.abs()))),
        None => Ok(ScalarValue::Float64(None)),
    }
}

pub fn ceil(args: &[ScalarValue]) -> Result<ScalarValue> {
    match args_to_f64(args, 0) {
        Some(v) => Ok(ScalarValue::Float64(Some(v.ceil()))),
        None => Ok(ScalarValue::Float64(None)),
    }
}

pub fn floor(args: &[ScalarValue]) -> Result<ScalarValue> {
    match args_to_f64(args, 0) {
        Some(v) => Ok(ScalarValue::Float64(Some(v.floor()))),
        None => Ok(ScalarValue::Float64(None)),
    }
}

pub fn round(args: &[ScalarValue]) -> Result<ScalarValue> {
    let val = args_to_f64(args, 0);
    let precision = args_to_f64(args, 1).unwrap_or(0.0) as i32;
    match val {
        Some(v) => {
            let factor = 10f64.powi(precision);
            Ok(ScalarValue::Float64(Some((v * factor).round() / factor)))
        }
        None => Ok(ScalarValue::Float64(None)),
    }
}

pub fn sqrt(args: &[ScalarValue]) -> Result<ScalarValue> {
    match args_to_f64(args, 0) {
        Some(v) if v >= 0.0 => Ok(ScalarValue::Float64(Some(v.sqrt()))),
        _ => Ok(ScalarValue::Float64(None)),
    }
}

pub fn pow(args: &[ScalarValue]) -> Result<ScalarValue> {
    let base = args_to_f64(args, 0);
    let exp = args_to_f64(args, 1);
    match (base, exp) {
        (Some(b), Some(e)) => Ok(ScalarValue::Float64(Some(b.powf(e)))),
        _ => Ok(ScalarValue::Float64(None)),
    }
}

pub fn log(args: &[ScalarValue]) -> Result<ScalarValue> {
    // LOG(base, value) or LOG(value) = natural log
    match args.len() {
        1 => ln(args),
        _ => {
            let base = args_to_f64(args, 0);
            let value = args_to_f64(args, 1);
            match (base, value) {
                (Some(b), Some(v)) if b > 0.0 && v > 0.0 => {
                    Ok(ScalarValue::Float64(Some(v.ln() / b.ln())))
                }
                _ => Ok(ScalarValue::Float64(None)),
            }
        }
    }
}

pub fn ln(args: &[ScalarValue]) -> Result<ScalarValue> {
    match args_to_f64(args, 0) {
        Some(v) if v > 0.0 => Ok(ScalarValue::Float64(Some(v.ln()))),
        _ => Ok(ScalarValue::Float64(None)),
    }
}

pub fn log10(args: &[ScalarValue]) -> Result<ScalarValue> {
    match args_to_f64(args, 0) {
        Some(v) if v > 0.0 => Ok(ScalarValue::Float64(Some(v.log10()))),
        _ => Ok(ScalarValue::Float64(None)),
    }
}

pub fn log2(args: &[ScalarValue]) -> Result<ScalarValue> {
    match args_to_f64(args, 0) {
        Some(v) if v > 0.0 => Ok(ScalarValue::Float64(Some(v.log2()))),
        _ => Ok(ScalarValue::Float64(None)),
    }
}

pub fn exp(args: &[ScalarValue]) -> Result<ScalarValue> {
    match args_to_f64(args, 0) {
        Some(v) => Ok(ScalarValue::Float64(Some(v.exp()))),
        None => Ok(ScalarValue::Float64(None)),
    }
}

pub fn sin(args: &[ScalarValue]) -> Result<ScalarValue> {
    match args_to_f64(args, 0) {
        Some(v) => Ok(ScalarValue::Float64(Some(v.sin()))),
        None => Ok(ScalarValue::Float64(None)),
    }
}

pub fn cos(args: &[ScalarValue]) -> Result<ScalarValue> {
    match args_to_f64(args, 0) {
        Some(v) => Ok(ScalarValue::Float64(Some(v.cos()))),
        None => Ok(ScalarValue::Float64(None)),
    }
}

pub fn tan(args: &[ScalarValue]) -> Result<ScalarValue> {
    match args_to_f64(args, 0) {
        Some(v) => Ok(ScalarValue::Float64(Some(v.tan()))),
        None => Ok(ScalarValue::Float64(None)),
    }
}

pub fn asin(args: &[ScalarValue]) -> Result<ScalarValue> {
    match args_to_f64(args, 0) {
        Some(v) if v >= -1.0 && v <= 1.0 => Ok(ScalarValue::Float64(Some(v.asin()))),
        _ => Ok(ScalarValue::Float64(None)),
    }
}

pub fn acos(args: &[ScalarValue]) -> Result<ScalarValue> {
    match args_to_f64(args, 0) {
        Some(v) if v >= -1.0 && v <= 1.0 => Ok(ScalarValue::Float64(Some(v.acos()))),
        _ => Ok(ScalarValue::Float64(None)),
    }
}

pub fn atan(args: &[ScalarValue]) -> Result<ScalarValue> {
    match args_to_f64(args, 0) {
        Some(v) => Ok(ScalarValue::Float64(Some(v.atan()))),
        None => Ok(ScalarValue::Float64(None)),
    }
}

pub fn atan2(args: &[ScalarValue]) -> Result<ScalarValue> {
    let y = args_to_f64(args, 0);
    let x = args_to_f64(args, 1);
    match (y, x) {
        (Some(y), Some(x)) => Ok(ScalarValue::Float64(Some(y.atan2(x)))),
        _ => Ok(ScalarValue::Float64(None)),
    }
}

pub fn sign(args: &[ScalarValue]) -> Result<ScalarValue> {
    match args_to_f64(args, 0) {
        Some(v) => Ok(ScalarValue::Float64(Some(v.signum()))),
        None => Ok(ScalarValue::Float64(None)),
    }
}

pub fn modulo(args: &[ScalarValue]) -> Result<ScalarValue> {
    let a = args_to_f64(args, 0);
    let b = args_to_f64(args, 1);
    match (a, b) {
        (Some(a), Some(b)) if b != 0.0 => Ok(ScalarValue::Float64(Some(a % b))),
        _ => Ok(ScalarValue::Float64(None)),
    }
}

pub fn truncate_func(args: &[ScalarValue]) -> Result<ScalarValue> {
    let val = args_to_f64(args, 0);
    let precision = args_to_f64(args, 1).unwrap_or(0.0) as i32;
    match val {
        Some(v) => {
            let factor = 10f64.powi(precision);
            Ok(ScalarValue::Float64(Some((v * factor).trunc() / factor)))
        }
        None => Ok(ScalarValue::Float64(None)),
    }
}

pub fn pi(_args: &[ScalarValue]) -> Result<ScalarValue> {
    Ok(ScalarValue::Float64(Some(std::f64::consts::PI)))
}
