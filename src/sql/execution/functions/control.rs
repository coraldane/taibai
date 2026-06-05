/// Control flow SQL functions (COALESCE, NULLIF, IFNULL, etc.)
use crate::common::error::Result;
use crate::types::scalar::ScalarValue;

pub fn coalesce(args: &[ScalarValue]) -> Result<ScalarValue> {
    for arg in args {
        if !arg.is_null() {
            return Ok(arg.clone());
        }
    }
    Ok(args.first().cloned().unwrap_or(ScalarValue::Int64(None)))
}

pub fn nullif(args: &[ScalarValue]) -> Result<ScalarValue> {
    if args.len() < 2 {
        return Ok(ScalarValue::Int64(None));
    }
    if args[0] == args[1] {
        Ok(ScalarValue::Int64(None))
    } else {
        Ok(args[0].clone())
    }
}

pub fn ifnull(args: &[ScalarValue]) -> Result<ScalarValue> {
    if args.len() < 2 {
        return Ok(ScalarValue::Int64(None));
    }
    if args[0].is_null() {
        Ok(args[1].clone())
    } else {
        Ok(args[0].clone())
    }
}

pub fn isnull_func(args: &[ScalarValue]) -> Result<ScalarValue> {
    if args.len() < 2 {
        return Ok(ScalarValue::Bool(Some(
            args.first().map(|v| v.is_null()).unwrap_or(true),
        )));
    }
    // ISNULL(a, b) = IFNULL(a, b)
    if args[0].is_null() {
        Ok(args[1].clone())
    } else {
        Ok(args[0].clone())
    }
}
