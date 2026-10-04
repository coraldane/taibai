//! Scalar coercion, literal parsing (decimal/timezone aware), constant
//! folding and the SQL-predicate comparison (`Option<Ordering>` semantics:
//! `None` means not comparable, so the predicate is false).

use crate::common::error::{Result, TaibaiError};
use crate::protocol::types::DataType;
use crate::types::batch::RecordBatch;
use crate::types::scalar::ScalarValue;

use crate::sql::parser::ast::{BinOp, Expr};

pub fn compare_scalars(a: &ScalarValue, b: &ScalarValue) -> Option<std::cmp::Ordering> {
    match (a, b) {
        _ if a.numeric_cmp(b).is_some() => a.numeric_cmp(b),
        (ScalarValue::Int32(Some(a)), ScalarValue::Int64(Some(b))) => (*a as i64).partial_cmp(b),
        (ScalarValue::Int64(Some(a)), ScalarValue::Int32(Some(b))) => a.partial_cmp(&(*b as i64)),
        (ScalarValue::Int64(Some(a)), ScalarValue::Int64(Some(b))) => Some(a.cmp(b)),
        (ScalarValue::Int32(Some(a)), ScalarValue::Int32(Some(b))) => Some(a.cmp(b)),
        (ScalarValue::Float64(Some(a)), ScalarValue::Float64(Some(b))) => a.partial_cmp(b),
        (ScalarValue::VarChar(Some(a)), ScalarValue::VarChar(Some(b))) => Some(a.cmp(b)),
        (ScalarValue::Timestamp(Some(a)), ScalarValue::Timestamp(Some(b))) => Some(a.cmp(b)),
        _ => None,
    }
}

pub(super) fn scalar_values_equal(a: &ScalarValue, b: &ScalarValue) -> bool {
    match (a, b) {
        (ScalarValue::Int32(Some(a)), ScalarValue::Int64(Some(b))) => *a as i64 == *b,
        (ScalarValue::Int64(Some(a)), ScalarValue::Int32(Some(b))) => *a == *b as i64,
        _ => a == b,
    }
}

/// SQL LIKE pattern match with `%` (any run) and `_` (single char) wildcards.
/// A backslash escapes `%`, `_` and itself. Case-sensitive, char-based.
pub fn sql_like_match(value: &str, pattern: &str) -> bool {
    enum Token {
        Char(char),
        Percent,
        Underscore,
    }
    let mut tokens = Vec::with_capacity(pattern.len());
    let mut chars = pattern.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\\' => match chars.next() {
                Some(escaped @ ('%' | '_' | '\\')) => tokens.push(Token::Char(escaped)),
                Some(other) => {
                    tokens.push(Token::Char('\\'));
                    tokens.push(Token::Char(other));
                }
                None => tokens.push(Token::Char('\\')),
            },
            '%' => tokens.push(Token::Percent),
            '_' => tokens.push(Token::Underscore),
            other => tokens.push(Token::Char(other)),
        }
    }
    let value: Vec<char> = value.chars().collect();
    let (mut vi, mut ti) = (0usize, 0usize);
    let (mut star_token, mut star_value) = (usize::MAX, 0usize);
    while vi < value.len() {
        match tokens.get(ti) {
            Some(Token::Underscore) => {
                vi += 1;
                ti += 1;
            }
            Some(Token::Char(c)) if *c == value[vi] => {
                vi += 1;
                ti += 1;
            }
            Some(Token::Percent) => {
                star_token = ti;
                star_value = vi;
                ti += 1;
            }
            _ => {
                if star_token == usize::MAX {
                    return false;
                }
                star_value += 1;
                vi = star_value;
                ti = star_token + 1;
            }
        }
    }
    while matches!(tokens.get(ti), Some(Token::Percent)) {
        ti += 1;
    }
    ti == tokens.len()
}

/// LIKE semantics for one scalar: NULL never matches (also under NOT LIKE).
/// Non-string values are coerced through their text form.
pub fn scalar_like_matches(value: &ScalarValue, pattern: &str) -> bool {
    if value.is_null() {
        return false;
    }
    match value {
        ScalarValue::VarChar(Some(text)) => sql_like_match(text, pattern),
        other => other
            .to_text()
            .is_some_and(|text| sql_like_match(&text, pattern)),
    }
}

pub fn coerce_scalar_for_dtype(value: &ScalarValue, dtype: DataType) -> Result<ScalarValue> {
    if value.is_null() {
        return Ok(ScalarValue::null_for_type(dtype));
    }
    match (dtype, value) {
        (target, value) if target == value.data_type() => Ok(value.clone()),
        (DataType::Bool, ScalarValue::Int64(Some(v))) => Ok(ScalarValue::Bool(Some(*v != 0))),
        (DataType::Bool, ScalarValue::Int32(Some(v))) => Ok(ScalarValue::Bool(Some(*v != 0))),
        (DataType::Bool, ScalarValue::VarChar(Some(v))) => match v.to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" => Ok(ScalarValue::Bool(Some(true))),
            "false" | "0" | "no" => Ok(ScalarValue::Bool(Some(false))),
            _ => Err(TaibaiError::Sql(format!("invalid bool '{}'", v))),
        },
        (
            target @ (DataType::Int64
            | DataType::Int32
            | DataType::Int16
            | DataType::Int8
            | DataType::UInt64
            | DataType::UInt32
            | DataType::UInt16
            | DataType::UInt8),
            value,
        ) => coerce_integer_scalar(value, target),
        (DataType::Float64, value) => value
            .numeric_f64()
            .map(|v| ScalarValue::Float64(Some(v)))
            .ok_or_else(|| TaibaiError::Sql(format!("cannot cast {:?} to FLOAT64", value))),
        (DataType::Float32, value) => value
            .numeric_f64()
            .map(|v| ScalarValue::Float32(Some(v as f32)))
            .ok_or_else(|| TaibaiError::Sql(format!("cannot cast {:?} to FLOAT32", value))),
        (DataType::VarChar, value) => value
            .to_text()
            .map(|v| ScalarValue::VarChar(Some(v)))
            .ok_or_else(|| TaibaiError::Sql(format!("cannot cast {:?} to VARCHAR", value))),
        _ => Ok(value.clone()),
    }
}

/// The single integer coercion ladder, shared by the predicate fast path and
/// the constant-expression evaluator (`eval_expr_to_scalar*`): widening is
/// exact via i128, narrowing and negative-UINT rejections carry explicit
/// errors. Both call sites must stay on this ladder so `WHERE flag = 'yes'`
/// and `SET flag = 'yes'` — and every integer conversion — behave
/// identically no matter which path evaluates them.
pub(crate) fn coerce_integer_scalar(value: &ScalarValue, target: DataType) -> Result<ScalarValue> {
    let Some(raw) = value.integer_i128() else {
        return Err(TaibaiError::Sql(format!(
            "cannot cast {:?} to {}",
            value,
            target.display_name()
        )));
    };
    macro_rules! narrowed {
        ($t:ty, $variant:ident, $name:expr) => {
            match <$t>::try_from(raw) {
                Ok(v) => Ok(ScalarValue::$variant(Some(v))),
                Err(_) if raw < 0 => Err(TaibaiError::Sql(format!(
                    "negative value '{}' for {}",
                    raw, $name
                ))),
                Err(_) => Err(TaibaiError::Sql(format!(
                    "value '{}' overflows {}",
                    raw, $name
                ))),
            }
        };
    }
    match target {
        DataType::Int64 => narrowed!(i64, Int64, "INT64"),
        DataType::Int32 => narrowed!(i32, Int32, "INT32"),
        DataType::Int16 => narrowed!(i16, Int16, "INT16"),
        DataType::Int8 => narrowed!(i8, Int8, "INT8"),
        DataType::UInt64 => narrowed!(u64, UInt64, "UINT64"),
        DataType::UInt32 => narrowed!(u32, UInt32, "UINT32"),
        DataType::UInt16 => narrowed!(u16, UInt16, "UINT16"),
        DataType::UInt8 => narrowed!(u8, UInt8, "UINT8"),
        _ => unreachable!("coerce_integer_scalar called with non-integer target"),
    }
}

pub fn eval_constant_expr(expr: &Expr) -> Result<ScalarValue> {
    match expr {
        Expr::Literal(v) => Ok(v.clone()),
        Expr::UnaryMinus(inner) => match eval_constant_expr(inner)? {
            ScalarValue::Int64(Some(v)) => Ok(ScalarValue::Int64(Some(-v))),
            ScalarValue::Int32(Some(v)) => Ok(ScalarValue::Int32(Some(-v))),
            ScalarValue::Float64(Some(v)) => Ok(ScalarValue::Float64(Some(-v))),
            ScalarValue::Float32(Some(v)) => Ok(ScalarValue::Float32(Some(-v))),
            value if value.is_null() => Ok(ScalarValue::Int64(None)),
            value => Err(TaibaiError::Sql(format!(
                "cannot negate non-numeric constant {:?}",
                value
            ))),
        },
        Expr::BinaryOp { left, op, right } => {
            let left = eval_constant_expr(left)?;
            let right = eval_constant_expr(right)?;
            eval_constant_binary(&left, *op, &right)
        }
        Expr::Function { name, args, .. } => {
            let arg_values = args
                .iter()
                .map(eval_constant_expr)
                .collect::<Result<Vec<_>>>()?;
            crate::sql::execution::functions::registry::FunctionRegistry::default()
                .execute(name, &arg_values)
        }
        _ => Err(TaibaiError::Sql(
            "INSERT values must be constant expressions".to_string(),
        )),
    }
}

fn eval_constant_binary(left: &ScalarValue, op: BinOp, right: &ScalarValue) -> Result<ScalarValue> {
    if left.is_null() || right.is_null() {
        return Ok(ScalarValue::Int64(None));
    }

    if let (Some(left), Some(right)) = (left.integer_i128(), right.integer_i128()) {
        if !matches!(op, BinOp::Div) {
            let value = match op {
                BinOp::Add => left.checked_add(right),
                BinOp::Sub => left.checked_sub(right),
                BinOp::Mul => left.checked_mul(right),
                BinOp::Mod => {
                    if right == 0 {
                        return Ok(ScalarValue::Int64(None));
                    }
                    left.checked_rem(right)
                }
                _ => None,
            }
            .ok_or_else(|| TaibaiError::Sql("integer constant expression overflow".to_string()))?;
            return i64::try_from(value)
                .map(|value| ScalarValue::Int64(Some(value)))
                .map_err(|_| TaibaiError::Sql("integer constant expression overflow".to_string()));
        }
    }

    let left = left.numeric_f64().ok_or_else(|| {
        TaibaiError::Sql("constant arithmetic requires numeric operands".to_string())
    })?;
    let right = right.numeric_f64().ok_or_else(|| {
        TaibaiError::Sql("constant arithmetic requires numeric operands".to_string())
    })?;
    let value = match op {
        BinOp::Add => left + right,
        BinOp::Sub => left - right,
        BinOp::Mul => left * right,
        BinOp::Div => {
            if right == 0.0 {
                return Err(TaibaiError::Sql("division by zero".to_string()));
            }
            left / right
        }
        BinOp::Mod => {
            if right == 0.0 {
                return Ok(ScalarValue::Float64(None));
            }
            left % right
        }
        _ => {
            return Err(TaibaiError::Sql(
                "unsupported operator in constant expression".to_string(),
            ))
        }
    };
    Ok(ScalarValue::Float64(Some(value)))
}

pub fn eval_expr_to_scalar(expr: &Expr, dt: &DataType) -> Result<ScalarValue> {
    eval_expr_to_scalar_with_scale_timezone(expr, dt, 0, 0)
}

pub fn eval_expr_to_scalar_with_scale_timezone(
    expr: &Expr,
    dt: &DataType,
    scale: i32,
    timezone_offset_hours: i32,
) -> Result<ScalarValue> {
    match expr {
        Expr::Function { .. } | Expr::BinaryOp { .. } | Expr::UnaryMinus(_) => {
            let value = eval_constant_expr(expr)?;
            eval_expr_to_scalar_with_scale_timezone(
                &Expr::Literal(value),
                dt,
                scale,
                timezone_offset_hours,
            )
        }
        Expr::Literal(v) if v.is_null() => Ok(ScalarValue::null_for_type(*dt)),
        Expr::Literal(v) => match (dt, v) {
            (target, value) if *target == value.data_type() => Ok(value.clone()),
            // Bool-from-string/int and every integer/float conversion are
            // intentionally NOT re-implemented here: the fallback below
            // delegates to `coerce_scalar_for_dtype`, the single ladder both
            // evaluators share (`WHERE flag = 'yes'` and
            // `SET flag = 'yes'` must accept the same literals).
            (DataType::Decimal, ScalarValue::Float64(Some(v))) => {
                if let Some(scaled) = float_literal_to_decimal_i64(*v, scale) {
                    return Ok(ScalarValue::Decimal(Some(scaled)));
                }
                let multiplier = decimal_multiplier(scale)?;
                Ok(ScalarValue::Decimal(Some(
                    (*v * multiplier as f64).round() as i64
                )))
            }
            (DataType::Decimal, ScalarValue::Int64(Some(v))) => {
                let multiplier = decimal_multiplier(scale)?;
                Ok(ScalarValue::Decimal(Some(
                    v.checked_mul(multiplier).ok_or_else(|| {
                        TaibaiError::Sql(format!("decimal value '{}' overflows scale {}", v, scale))
                    })?,
                )))
            }
            (DataType::Timestamp, ScalarValue::VarChar(Some(s))) => {
                use crate::sql::execution::functions::date::parse_timestamp_smart;
                match parse_timestamp_smart(s, timezone_offset_hours) {
                    Some(ts) => Ok(ScalarValue::Timestamp(Some(ts))),
                    None => Ok(ScalarValue::Timestamp(None)),
                }
            }
            (DataType::Timestamp, ScalarValue::Int64(Some(n))) => {
                use crate::sql::execution::functions::date::parse_timestamp_from_int;
                Ok(ScalarValue::Timestamp(Some(parse_timestamp_from_int(*n))))
            }
            (DataType::Date, ScalarValue::VarChar(Some(s))) => {
                // Single string→DATE conversion point (micros → days).
                use crate::sql::execution::functions::date::datetime_string_to_days;
                Ok(ScalarValue::Date(datetime_string_to_days(
                    s,
                    timezone_offset_hours,
                )))
            }
            (DataType::Date, ScalarValue::Int64(Some(n))) => {
                // Interpret as days directly
                Ok(ScalarValue::Date(Some(*n as i32)))
            }
            (DataType::VarChar, value) => match value.to_text() {
                Some(text) => Ok(ScalarValue::VarChar(Some(text))),
                None => Ok(ScalarValue::VarChar(None)),
            },
            (DataType::Decimal, value) => {
                let multiplier = decimal_multiplier(scale)?;
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
                } else if let Some(f) = value.numeric_f64() {
                    Ok(ScalarValue::Decimal(Some(
                        (f * multiplier as f64).round() as i64
                    )))
                } else {
                    coerce_scalar_for_dtype(v, *dt)
                }
            }
            _ => coerce_scalar_for_dtype(v, *dt),
        },
        // Column references and row-dependent expressions are not constant
        // per statement. Erroring here is fail-safe: the previous silent
        // NULL turned `UPDATE t SET a = b` into "set every a to NULL".
        Expr::Column(col) => Err(TaibaiError::Sql(format!(
            "column reference '{}' is not allowed here; only constant expressions are supported",
            col
        ))),
        _ => Err(TaibaiError::Sql(
            "unsupported expression: only constant expressions are allowed in this context"
                .to_string(),
        )),
    }
}

/// Exact-decimal conversion of a float LITERAL to its scaled integer.
/// Literal floats arrive from the lexer as f64, but Rust's shortest
/// round-trip Display reconstructs the original decimal text, so the value
/// can be re-parsed with exact integer arithmetic (half-up on the first
/// dropped digit, sign applied last — half away from zero, matching the
/// fast INSERT path's parser) instead of f64 multiplication: `2.675 * 100`
/// is 267.499... in binary floating point and used to store 267 while the
/// fast path's exact parse stored 268, silently diverging on syntax alone.
/// Text in scientific notation (beyond realistic literals) keeps the f64
/// fallback. `None` means the text was not a plain decimal.
pub(crate) fn float_literal_to_decimal_i64(value: f64, scale: i32) -> Option<i64> {
    let text = value.to_string();
    if text.contains(['e', 'E']) {
        let multiplier = decimal_multiplier(scale).ok()?;
        return Some((value * multiplier as f64).round() as i64);
    }
    parse_exact_decimal_text(&text, scale)
}

fn parse_exact_decimal_text(text: &str, scale: i32) -> Option<i64> {
    let scale = usize::try_from(scale).ok()?;
    let bytes = text.as_bytes();
    let mut idx = 0usize;
    let negative = match bytes.first() {
        Some(b'-') => {
            idx = 1;
            true
        }
        Some(b'+') => {
            idx = 1;
            false
        }
        _ => false,
    };
    let mut value: i64 = 0;
    let mut seen_digit = false;
    while idx < bytes.len() && bytes[idx] != b'.' {
        let b = bytes[idx];
        if !b.is_ascii_digit() {
            return None;
        }
        seen_digit = true;
        value = value.checked_mul(10)?.checked_add((b - b'0') as i64)?;
        idx += 1;
    }
    if idx < bytes.len() {
        idx += 1; // consume '.'
    }
    let mut frac_digits = 0usize;
    while frac_digits < scale && idx < bytes.len() {
        let b = bytes[idx];
        if !b.is_ascii_digit() {
            return None;
        }
        seen_digit = true;
        value = value.checked_mul(10)?.checked_add((b - b'0') as i64)?;
        frac_digits += 1;
        idx += 1;
    }
    while frac_digits < scale {
        value = value.checked_mul(10)?;
        frac_digits += 1;
    }
    // Half-up on the first dropped digit; the rest of the tail only needs
    // to stay digits.
    if let Some(&b) = bytes.get(idx) {
        if !b.is_ascii_digit() {
            return None;
        }
        if b >= b'5' {
            value = value.checked_add(1)?;
        }
    }
    if !seen_digit {
        return None;
    }
    Some(if negative { -value } else { value })
}

pub(crate) fn decimal_multiplier(scale: i32) -> Result<i64> {
    if !(0..=18).contains(&scale) {
        return Err(TaibaiError::Sql(format!("invalid decimal scale {}", scale)));
    }
    10_i64
        .checked_pow(scale as u32)
        .ok_or_else(|| TaibaiError::Sql(format!("invalid decimal scale {}", scale)))
}

pub fn sort_batch_by_key(
    batch: &RecordBatch,
    inserter: &crate::storage::index::sort_key::SortKeyInserter,
) -> Result<RecordBatch> {
    let num_rows = batch.num_rows();
    if num_rows <= 1 {
        return Ok(batch.clone());
    }

    // Sort lightweight row indices against the typed column buffers. The old
    // implementation materialized every key as ScalarValue and then rebuilt
    // all cells one by one, which dominated multi-value INSERT workloads.
    let mut rows = (0..num_rows as u32).collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        for (ki, &col_idx) in inserter.key_indices.iter().enumerate() {
            let cmp = batch.columns[col_idx].compare_at(*left as usize, *right as usize);
            let cmp = if inserter.desc.get(ki).copied().unwrap_or(false) {
                cmp.reverse()
            } else {
                cmp
            };
            if cmp != std::cmp::Ordering::Equal {
                return cmp;
            }
        }
        std::cmp::Ordering::Equal
    });

    let columns = batch
        .columns
        .iter()
        .map(|column| column.take(&rows))
        .collect();
    RecordBatch::try_new(batch.schema.clone(), columns)
}
