/// String SQL functions.
use crate::common::error::Result;
use crate::types::scalar::ScalarValue;

use super::registry::{args_to_i64, args_to_string};

pub fn length(args: &[ScalarValue]) -> Result<ScalarValue> {
    match args_to_string(args, 0) {
        Some(s) => Ok(ScalarValue::Int64(Some(s.len() as i64))),
        None => Ok(ScalarValue::Int64(None)),
    }
}

pub fn upper(args: &[ScalarValue]) -> Result<ScalarValue> {
    match args_to_string(args, 0) {
        Some(s) => Ok(ScalarValue::VarChar(Some(s.to_uppercase()))),
        None => Ok(ScalarValue::VarChar(None)),
    }
}

pub fn lower(args: &[ScalarValue]) -> Result<ScalarValue> {
    match args_to_string(args, 0) {
        Some(s) => Ok(ScalarValue::VarChar(Some(s.to_lowercase()))),
        None => Ok(ScalarValue::VarChar(None)),
    }
}

pub fn trim(args: &[ScalarValue]) -> Result<ScalarValue> {
    match args_to_string(args, 0) {
        Some(s) => Ok(ScalarValue::VarChar(Some(s.trim().to_string()))),
        None => Ok(ScalarValue::VarChar(None)),
    }
}

pub fn ltrim(args: &[ScalarValue]) -> Result<ScalarValue> {
    match args_to_string(args, 0) {
        Some(s) => Ok(ScalarValue::VarChar(Some(s.trim_start().to_string()))),
        None => Ok(ScalarValue::VarChar(None)),
    }
}

pub fn rtrim(args: &[ScalarValue]) -> Result<ScalarValue> {
    match args_to_string(args, 0) {
        Some(s) => Ok(ScalarValue::VarChar(Some(s.trim_end().to_string()))),
        None => Ok(ScalarValue::VarChar(None)),
    }
}

pub fn substring(args: &[ScalarValue]) -> Result<ScalarValue> {
    let s = args_to_string(args, 0);
    let start = args_to_i64(args, 1);
    match (s, start) {
        (Some(s), Some(start)) if start > 0 => {
            let len = args_to_i64(args, 2).unwrap_or(s.len() as i64);
            let start_idx = (start - 1) as usize;
            if start_idx >= s.len() {
                return Ok(ScalarValue::VarChar(Some(String::new())));
            }
            let end_idx = (start_idx + len as usize).min(s.len());
            Ok(ScalarValue::VarChar(Some(
                s[start_idx..end_idx].to_string(),
            )))
        }
        _ => Ok(ScalarValue::VarChar(None)),
    }
}

pub fn concat(args: &[ScalarValue]) -> Result<ScalarValue> {
    let mut result = String::new();
    for arg in args {
        match arg {
            ScalarValue::VarChar(Some(s)) => result.push_str(s),
            ScalarValue::Int64(Some(v)) => result.push_str(&v.to_string()),
            ScalarValue::Int32(Some(v)) => result.push_str(&v.to_string()),
            ScalarValue::Float64(Some(v)) => result.push_str(&v.to_string()),
            ScalarValue::Float32(Some(v)) => result.push_str(&v.to_string()),
            ScalarValue::Bool(Some(v)) => result.push_str(if *v { "true" } else { "false" }),
            _ => {} // skip NULLs in CONCAT
        }
    }
    Ok(ScalarValue::VarChar(Some(result)))
}

pub fn concat_ws(args: &[ScalarValue]) -> Result<ScalarValue> {
    let sep = match args_to_string(args, 0) {
        Some(s) => s,
        None => return Ok(ScalarValue::VarChar(None)),
    };
    let mut parts = Vec::new();
    for arg in &args[1..] {
        match arg {
            ScalarValue::VarChar(Some(s)) => parts.push(s.clone()),
            ScalarValue::Int64(Some(v)) => parts.push(v.to_string()),
            ScalarValue::Int32(Some(v)) => parts.push(v.to_string()),
            ScalarValue::Float64(Some(v)) => parts.push(v.to_string()),
            _ => {} // skip NULLs
        }
    }
    Ok(ScalarValue::VarChar(Some(parts.join(&sep))))
}

pub fn replace(args: &[ScalarValue]) -> Result<ScalarValue> {
    let s = args_to_string(args, 0);
    let from = args_to_string(args, 1);
    let to = args_to_string(args, 2);
    match (s, from, to) {
        (Some(s), Some(from), Some(to)) => Ok(ScalarValue::VarChar(Some(s.replace(&from, &to)))),
        _ => Ok(ScalarValue::VarChar(None)),
    }
}

pub fn reverse(args: &[ScalarValue]) -> Result<ScalarValue> {
    match args_to_string(args, 0) {
        Some(s) => Ok(ScalarValue::VarChar(Some(s.chars().rev().collect()))),
        None => Ok(ScalarValue::VarChar(None)),
    }
}

pub fn left(args: &[ScalarValue]) -> Result<ScalarValue> {
    let s = args_to_string(args, 0);
    let n = args_to_i64(args, 1);
    match (s, n) {
        (Some(s), Some(n)) if n >= 0 => {
            let end = (n as usize).min(s.len());
            Ok(ScalarValue::VarChar(Some(s[..end].to_string())))
        }
        _ => Ok(ScalarValue::VarChar(None)),
    }
}

pub fn right(args: &[ScalarValue]) -> Result<ScalarValue> {
    let s = args_to_string(args, 0);
    let n = args_to_i64(args, 1);
    match (s, n) {
        (Some(s), Some(n)) if n >= 0 => {
            let start = if (n as usize) >= s.len() {
                0
            } else {
                s.len() - n as usize
            };
            Ok(ScalarValue::VarChar(Some(s[start..].to_string())))
        }
        _ => Ok(ScalarValue::VarChar(None)),
    }
}

pub fn lpad(args: &[ScalarValue]) -> Result<ScalarValue> {
    let s = args_to_string(args, 0);
    let len = args_to_i64(args, 1);
    let pad = args_to_string(args, 2).unwrap_or_else(|| " ".to_string());
    match (s, len) {
        (Some(s), Some(len)) if len >= 0 => {
            let target = len as usize;
            if s.len() >= target {
                Ok(ScalarValue::VarChar(Some(s[..target].to_string())))
            } else {
                let pad_needed = target - s.len();
                let padding: String = pad.chars().cycle().take(pad_needed).collect();
                Ok(ScalarValue::VarChar(Some(format!("{}{}", padding, s))))
            }
        }
        _ => Ok(ScalarValue::VarChar(None)),
    }
}

pub fn rpad(args: &[ScalarValue]) -> Result<ScalarValue> {
    let s = args_to_string(args, 0);
    let len = args_to_i64(args, 1);
    let pad = args_to_string(args, 2).unwrap_or_else(|| " ".to_string());
    match (s, len) {
        (Some(s), Some(len)) if len >= 0 => {
            let target = len as usize;
            if s.len() >= target {
                Ok(ScalarValue::VarChar(Some(s[..target].to_string())))
            } else {
                let pad_needed = target - s.len();
                let padding: String = pad.chars().cycle().take(pad_needed).collect();
                Ok(ScalarValue::VarChar(Some(format!("{}{}", s, padding))))
            }
        }
        _ => Ok(ScalarValue::VarChar(None)),
    }
}

pub fn instr(args: &[ScalarValue]) -> Result<ScalarValue> {
    let s = args_to_string(args, 0);
    let substr = args_to_string(args, 1);
    match (s, substr) {
        (Some(s), Some(sub)) => {
            match s.find(&sub) {
                Some(idx) => Ok(ScalarValue::Int64(Some((idx + 1) as i64))), // 1-based
                None => Ok(ScalarValue::Int64(Some(0))),
            }
        }
        _ => Ok(ScalarValue::Int64(None)),
    }
}

pub fn repeat(args: &[ScalarValue]) -> Result<ScalarValue> {
    let s = args_to_string(args, 0);
    let n = args_to_i64(args, 1);
    match (s, n) {
        (Some(s), Some(n)) if n >= 0 => Ok(ScalarValue::VarChar(Some(s.repeat(n as usize)))),
        _ => Ok(ScalarValue::VarChar(None)),
    }
}

pub fn space(args: &[ScalarValue]) -> Result<ScalarValue> {
    match args_to_i64(args, 0) {
        Some(n) if n >= 0 => Ok(ScalarValue::VarChar(Some(" ".repeat(n as usize)))),
        _ => Ok(ScalarValue::VarChar(None)),
    }
}
