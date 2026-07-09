use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::common::error::{Result, TaibaiError};
use crate::protocol::types::DataType;
use crate::storage::catalog::auth::{PermissionObjectType, PermissionType};
use crate::storage::catalog::auto_increment::AutoIncrementCounter;
use crate::storage::index::index_manager::{UniqueIndexEntry, UniqueRowLocation};
use crate::storage::index::sort_key::SortKeyInserter;
use crate::storage::segment_manager::SegmentManager;
use crate::types::array::ColumnArray;
use crate::types::batch::{ColumnDefaultValue, RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

use super::engine::{Engine, ExecResult};
use super::helpers::{eval_expr_to_scalar_with_scale_timezone, sort_batch_by_key};
use crate::sql::parser::ast::Expr;

lazy_static::lazy_static! {
    static ref PRIMARY_KEY_INSERT_LOCK: Mutex<()> = Mutex::new(());
    static ref FAST_INSERT_PLAN_CACHE: Mutex<HashMap<String, Arc<FastInsertPlan>>> = Mutex::new(HashMap::new());
}

struct FastInsertPlan {
    schema_signature: String,
    col_indices: Vec<usize>,
    provided_by_col: Vec<Option<usize>>,
    missing_auto_inc: Vec<usize>,
}

fn insert_profile_enabled() -> bool {
    matches!(std::env::var("TAIBAI_INSERT_PROFILE"), Ok(v) if v == "1" || v.eq_ignore_ascii_case("true"))
}

fn log_insert_profile(stage: &str, rows: usize, elapsed: std::time::Duration) {
    if insert_profile_enabled() {
        eprintln!(
            "[insert-profile] stage={} rows={} elapsed_us={}",
            stage,
            rows,
            elapsed.as_micros()
        );
    }
}

pub fn try_exec_fast_insert_sql(
    engine: &Engine,
    default_db: &str,
    sql: &str,
    timezone_offset_hours: i32,
    txn_id: u64,
    bulk_import_mode: bool,
    username: &str,
) -> Option<Result<ExecResult>> {
    match try_exec_fast_insert_sql_inner(
        engine,
        default_db,
        sql,
        timezone_offset_hours,
        txn_id,
        bulk_import_mode,
        username,
    ) {
        FastInsertOutcome::Executed(result) => Some(result),
        FastInsertOutcome::Unsupported => None,
    }
}

enum FastInsertOutcome {
    Executed(Result<ExecResult>),
    Unsupported,
}

#[derive(Clone, Copy)]
enum FastValue<'a> {
    Null,
    String(&'a str),
    Bare(&'a str),
}

fn try_exec_fast_insert_sql_inner(
    engine: &Engine,
    default_db: &str,
    sql: &str,
    timezone_offset_hours: i32,
    txn_id: u64,
    bulk_import_mode: bool,
    username: &str,
) -> FastInsertOutcome {
    let profile_start = Instant::now();
    let bytes = sql.as_bytes();
    let mut pos = 0usize;
    if !consume_keyword(bytes, &mut pos, b"INSERT") || !consume_keyword(bytes, &mut pos, b"INTO") {
        return FastInsertOutcome::Unsupported;
    }
    skip_spaces(bytes, &mut pos);
    let table_start = pos;
    while pos < bytes.len() && !matches!(bytes[pos], b' ' | b'\t' | b'\r' | b'\n' | b'(') {
        pos += 1;
    }
    if pos == table_start {
        return FastInsertOutcome::Unsupported;
    }
    let table_raw = &sql[table_start..pos];
    let (db, table) = if let Some((db, table)) = table_raw.split_once('.') {
        (db, table)
    } else {
        (default_db, table_raw)
    };
    if !engine.auth_manager().check_permission(
        username,
        PermissionType::Insert,
        PermissionObjectType::Table,
        &format!("{}.{}", db, table),
    ) {
        return FastInsertOutcome::Executed(Err(TaibaiError::Sql(format!(
            "access denied for user '{}' to {:?} on {:?} '{}'",
            username,
            PermissionType::Insert,
            PermissionObjectType::Table,
            format!("{}.{}", db, table)
        ))));
    }

    let catalogs = engine.catalogs().read().unwrap();
    let catalog = match catalogs.get(db) {
        Some(catalog) => catalog,
        None => {
            return FastInsertOutcome::Executed(Err(TaibaiError::Sql(format!(
                "database '{}' does not exist",
                db
            ))))
        }
    };
    let table_meta = match catalog.get_table(table) {
        Some(meta) => meta.clone(),
        None => {
            return FastInsertOutcome::Executed(Err(TaibaiError::Sql(format!(
                "table '{}.{}' does not exist",
                db, table
            ))))
        }
    };
    drop(catalogs);

    let schema = Schema::new(table_meta.columns.clone());
    let columns = match parse_insert_column_list(sql, bytes, &mut pos) {
        Some(columns) => columns,
        None => return FastInsertOutcome::Unsupported,
    };
    if !consume_keyword(bytes, &mut pos, b"VALUES") {
        return FastInsertOutcome::Unsupported;
    }
    let plan_key = sql[..pos].to_ascii_lowercase();
    let schema_signature = fast_insert_schema_signature(&schema);
    let plan = match get_or_build_fast_insert_plan(&plan_key, &schema_signature, &schema, &columns)
    {
        Ok(plan) => plan,
        Err(e) => return FastInsertOutcome::Executed(Err(e)),
    };
    let col_indices = &plan.col_indices;
    let provided_by_col = &plan.provided_by_col;
    let missing_auto_inc = plan.missing_auto_inc.clone();
    let auto_inc_counter = if !missing_auto_inc.is_empty() {
        engine.auto_increment_manager().get(db, table)
    } else {
        None
    };

    let capacity = estimate_values_rows(bytes, pos).min(100_000).max(1);
    let mut batch = RecordBatch::with_capacity(schema.clone(), capacity);
    let mut rows = 0usize;
    let direct_full_row = missing_auto_inc.is_empty() && col_indices.len() == schema.columns.len();
    let mut row_values = if direct_full_row {
        Vec::new()
    } else {
        Vec::with_capacity(col_indices.len())
    };

    loop {
        skip_spaces(bytes, &mut pos);
        if pos >= bytes.len() {
            break;
        }
        if bytes[pos] != b'(' {
            return FastInsertOutcome::Unsupported;
        }
        pos += 1;
        if direct_full_row {
            for value_idx in 0..col_indices.len() {
                skip_spaces(bytes, &mut pos);
                let value = match parse_fast_value(sql, bytes, &mut pos) {
                    Some(value) => value,
                    None => return FastInsertOutcome::Unsupported,
                };
                let col_idx = col_indices[value_idx];
                if let Err(e) = push_fast_insert_provided_value(&mut batch, &schema, col_idx, value)
                {
                    return FastInsertOutcome::Executed(Err(e));
                }
                skip_spaces(bytes, &mut pos);
                if value_idx + 1 < col_indices.len() {
                    if pos >= bytes.len() || bytes[pos] != b',' {
                        return FastInsertOutcome::Unsupported;
                    }
                    pos += 1;
                }
            }
        } else {
            row_values.clear();
            for value_idx in 0..col_indices.len() {
                skip_spaces(bytes, &mut pos);
                let value = match parse_fast_value(sql, bytes, &mut pos) {
                    Some(value) => value,
                    None => return FastInsertOutcome::Unsupported,
                };
                row_values.push(value);
                skip_spaces(bytes, &mut pos);
                if value_idx + 1 < col_indices.len() {
                    if pos >= bytes.len() || bytes[pos] != b',' {
                        return FastInsertOutcome::Unsupported;
                    }
                    pos += 1;
                }
            }
            if let Err(e) = materialize_fast_insert_row(
                engine,
                db,
                table,
                &auto_inc_counter,
                &mut batch,
                &schema,
                &provided_by_col,
                timezone_offset_hours,
                &row_values,
            ) {
                return FastInsertOutcome::Executed(Err(e));
            }
        }
        skip_spaces(bytes, &mut pos);
        if pos >= bytes.len() || bytes[pos] != b')' {
            return FastInsertOutcome::Unsupported;
        }
        pos += 1;
        rows += 1;
        skip_spaces(bytes, &mut pos);
        if pos >= bytes.len() {
            break;
        }
        if bytes[pos] == b',' {
            pos += 1;
            continue;
        }
        return FastInsertOutcome::Unsupported;
    }

    if rows == 0 {
        return FastInsertOutcome::Unsupported;
    }

    log_insert_profile("fast_materialize", rows, profile_start.elapsed());

    FastInsertOutcome::Executed(exec_materialized_insert_batch(
        engine,
        db,
        table,
        &table_meta,
        schema,
        batch,
        txn_id,
        bulk_import_mode,
        true,
        missing_auto_inc,
    ))
}

#[inline]
fn skip_spaces(bytes: &[u8], pos: &mut usize) {
    while *pos < bytes.len() && matches!(bytes[*pos], b' ' | b'\t' | b'\r' | b'\n') {
        *pos += 1;
    }
}

fn consume_keyword(bytes: &[u8], pos: &mut usize, keyword: &[u8]) -> bool {
    skip_spaces(bytes, pos);
    if *pos + keyword.len() > bytes.len() {
        return false;
    }
    for (idx, expected) in keyword.iter().enumerate() {
        if bytes[*pos + idx].to_ascii_uppercase() != *expected {
            return false;
        }
    }
    let end = *pos + keyword.len();
    if end < bytes.len() && (bytes[end].is_ascii_alphanumeric() || bytes[end] == b'_') {
        return false;
    }
    *pos = end;
    true
}

fn parse_insert_column_list(sql: &str, bytes: &[u8], pos: &mut usize) -> Option<Vec<String>> {
    skip_spaces(bytes, pos);
    if *pos >= bytes.len() || bytes[*pos] != b'(' {
        return Some(Vec::new());
    }
    *pos += 1;
    let mut columns = Vec::new();
    loop {
        skip_spaces(bytes, pos);
        let start = *pos;
        while *pos < bytes.len()
            && !matches!(bytes[*pos], b',' | b')' | b' ' | b'\t' | b'\r' | b'\n')
        {
            *pos += 1;
        }
        if *pos == start {
            return None;
        }
        columns.push(sql[start..*pos].to_string());
        skip_spaces(bytes, pos);
        if *pos >= bytes.len() {
            return None;
        }
        match bytes[*pos] {
            b',' => *pos += 1,
            b')' => {
                *pos += 1;
                return Some(columns);
            }
            _ => return None,
        }
    }
}

fn resolve_fast_insert_columns(schema: &Schema, columns: &[String]) -> Result<Vec<usize>> {
    if columns.is_empty() {
        return Ok((0..schema.columns.len()).collect());
    }
    let mut seen = HashSet::new();
    let mut indices = Vec::with_capacity(columns.len());
    for column in columns {
        let idx = schema
            .index_of(column)
            .ok_or_else(|| TaibaiError::Sql(format!("column '{}' not found", column)))?;
        if !seen.insert(idx) {
            return Err(TaibaiError::Sql(format!(
                "column '{}' specified more than once",
                column
            )));
        }
        indices.push(idx);
    }
    Ok(indices)
}

fn fast_insert_schema_signature(schema: &Schema) -> String {
    let mut signature = String::new();
    for col in &schema.columns {
        signature.push_str(&col.name.to_ascii_lowercase());
        signature.push(':');
        signature.push_str(col.data_type.display_name());
        signature.push(':');
        signature.push_str(&col.scale.to_string());
        signature.push(':');
        signature.push(if col.auto_increment { '1' } else { '0' });
        signature.push(';');
    }
    signature
}

fn get_or_build_fast_insert_plan(
    plan_key: &str,
    schema_signature: &str,
    schema: &Schema,
    columns: &[String],
) -> Result<Arc<FastInsertPlan>> {
    if let Some(plan) = FAST_INSERT_PLAN_CACHE
        .lock()
        .unwrap()
        .get(plan_key)
        .cloned()
    {
        if plan.schema_signature == schema_signature {
            return Ok(plan);
        }
    }

    let col_indices = resolve_fast_insert_columns(schema, columns)?;
    let mut provided_by_col = vec![None; schema.columns.len()];
    for (value_idx, &col_idx) in col_indices.iter().enumerate() {
        provided_by_col[col_idx] = Some(value_idx);
    }
    let missing_auto_inc: Vec<usize> = schema
        .columns
        .iter()
        .enumerate()
        .filter(|(idx, col)| col.auto_increment && provided_by_col[*idx].is_none())
        .map(|(idx, _)| idx)
        .collect();
    let plan = Arc::new(FastInsertPlan {
        schema_signature: schema_signature.to_string(),
        col_indices,
        provided_by_col,
        missing_auto_inc,
    });
    let mut cache = FAST_INSERT_PLAN_CACHE.lock().unwrap();
    if cache.len() > 1024 {
        cache.clear();
    }
    cache.insert(plan_key.to_string(), Arc::clone(&plan));
    Ok(plan)
}

#[inline]
fn parse_fast_value<'a>(sql: &'a str, bytes: &[u8], pos: &mut usize) -> Option<FastValue<'a>> {
    if *pos >= bytes.len() {
        return None;
    }
    if bytes[*pos] == b'\'' {
        *pos += 1;
        let start = *pos;
        while *pos < bytes.len() {
            if bytes[*pos] == b'\'' {
                if *pos + 1 < bytes.len() && bytes[*pos + 1] == b'\'' {
                    *pos += 2;
                    continue;
                }
                let end = *pos;
                *pos += 1;
                return Some(FastValue::String(&sql[start..end]));
            }
            *pos += 1;
        }
        return None;
    }
    let start = *pos;
    while *pos < bytes.len() && !matches!(bytes[*pos], b',' | b')' | b' ' | b'\t' | b'\r' | b'\n') {
        *pos += 1;
    }
    if *pos == start {
        return None;
    }
    let text = &sql[start..*pos];
    if text.contains('(') || text.contains(')') {
        return None;
    }
    if text.eq_ignore_ascii_case("NULL") {
        Some(FastValue::Null)
    } else {
        Some(FastValue::Bare(text))
    }
}

#[inline]
fn push_fast_insert_provided_value(
    batch: &mut RecordBatch,
    schema: &Schema,
    col_idx: usize,
    value: FastValue<'_>,
) -> Result<()> {
    let col = &schema.columns[col_idx];
    if matches!(value, FastValue::Null) {
        if col.primary_key || !col.nullable {
            return Err(TaibaiError::Sql(format!(
                "column '{}' cannot be NULL",
                col.name
            )));
        }
        batch.column_mut(col_idx).unwrap().push_null_value();
    } else {
        push_fast_value_direct(batch.column_mut(col_idx).unwrap(), value, col.scale)?;
    }
    Ok(())
}

fn materialize_fast_insert_row(
    engine: &Engine,
    db: &str,
    table: &str,
    auto_inc_counter: &Option<Arc<AutoIncrementCounter>>,
    batch: &mut RecordBatch,
    schema: &Schema,
    provided_by_col: &[Option<usize>],
    timezone_offset_hours: i32,
    row_values: &[FastValue<'_>],
) -> Result<()> {
    for (col_idx, provided) in provided_by_col.iter().enumerate() {
        let col = &schema.columns[col_idx];
        if let Some(value_idx) = provided {
            let value = row_values[*value_idx];
            if matches!(value, FastValue::Null) {
                if col.primary_key || !col.nullable {
                    return Err(TaibaiError::Sql(format!(
                        "column '{}' cannot be NULL",
                        col.name
                    )));
                }
                batch.column_mut(col_idx).unwrap().push_null_value();
            } else {
                push_fast_value_direct(batch.column_mut(col_idx).unwrap(), value, col.scale)?;
            }
        } else {
            push_missing_insert_value(
                engine,
                db,
                table,
                auto_inc_counter,
                batch.column_mut(col_idx).unwrap(),
                schema,
                col_idx,
                timezone_offset_hours,
            )?;
        }
    }
    Ok(())
}

#[inline]
fn push_fast_value_direct(col: &mut ColumnArray, value: FastValue<'_>, scale: i32) -> Result<()> {
    match col {
        ColumnArray::Bool { data, bitmap } => {
            data.push(if parse_bool_fast(value)? { 1 } else { 0 });
            bitmap.push_valid();
        }
        ColumnArray::Int8 { data, bitmap } => {
            let v = parse_i64_fast(value)?;
            data.push(i8::try_from(v).map_err(|_| overflow_error(v, "INT8"))?);
            bitmap.push_valid();
        }
        ColumnArray::Int16 { data, bitmap } => {
            let v = parse_i64_fast(value)?;
            data.push(i16::try_from(v).map_err(|_| overflow_error(v, "INT16"))?);
            bitmap.push_valid();
        }
        ColumnArray::Int32 { data, bitmap } => {
            let v = parse_i64_fast(value)?;
            data.push(i32::try_from(v).map_err(|_| overflow_error(v, "INT32"))?);
            bitmap.push_valid();
        }
        ColumnArray::Int64 { data, bitmap } => {
            data.push(parse_i64_fast(value)?);
            bitmap.push_valid();
        }
        ColumnArray::UInt8 { data, bitmap } => {
            let v = parse_u64_fast(value)?;
            data.push(u8::try_from(v).map_err(|_| overflow_error_u(v, "UINT8"))?);
            bitmap.push_valid();
        }
        ColumnArray::UInt16 { data, bitmap } => {
            let v = parse_u64_fast(value)?;
            data.push(u16::try_from(v).map_err(|_| overflow_error_u(v, "UINT16"))?);
            bitmap.push_valid();
        }
        ColumnArray::UInt32 { data, bitmap } => {
            let v = parse_u64_fast(value)?;
            data.push(u32::try_from(v).map_err(|_| overflow_error_u(v, "UINT32"))?);
            bitmap.push_valid();
        }
        ColumnArray::UInt64 { data, bitmap } => {
            data.push(parse_u64_fast(value)?);
            bitmap.push_valid();
        }
        ColumnArray::Float32 { data, bitmap } => {
            data.push(parse_f64_fast(value)? as f32);
            bitmap.push_valid();
        }
        ColumnArray::Float64 { data, bitmap } => {
            data.push(parse_f64_fast(value)?);
            bitmap.push_valid();
        }
        ColumnArray::Decimal { data, bitmap } => {
            data.push(parse_decimal_fast(value, scale)?);
            bitmap.push_valid();
        }
        ColumnArray::Date { data, bitmap } => {
            let v = parse_i64_fast(value)?;
            data.push(i32::try_from(v).map_err(|_| overflow_error(v, "DATE"))?);
            bitmap.push_valid();
        }
        ColumnArray::Time { data, bitmap } => {
            data.push(parse_i64_fast(value)?);
            bitmap.push_valid();
        }
        ColumnArray::Timestamp { data, bitmap } => {
            data.push(parse_i64_fast(value)?);
            bitmap.push_valid();
        }
        ColumnArray::Interval { data, bitmap } => {
            data.push(parse_i64_fast(value)?);
            bitmap.push_valid();
        }
        ColumnArray::VarChar { data, bitmap } => {
            data.push(parse_owned_string_fast(value)?);
            bitmap.push_valid();
        }
        ColumnArray::Blob { .. } | ColumnArray::Vector { .. } => {
            return Err(TaibaiError::Sql(format!(
                "fast INSERT does not support {} literal",
                col.data_type().display_name()
            )));
        }
    }
    Ok(())
}

fn materialize_insert_row<F>(
    engine: &Engine,
    db: &str,
    table: &str,
    auto_inc_counter: &Option<Arc<AutoIncrementCounter>>,
    batch: &mut RecordBatch,
    schema: &Schema,
    provided_by_col: &[Option<usize>],
    timezone_offset_hours: i32,
    mut materialize_provided: F,
) -> Result<()>
where
    F: FnMut(usize, usize) -> Result<ScalarValue>,
{
    for (col_idx, provided) in provided_by_col.iter().enumerate() {
        if let Some(value_idx) = provided {
            let scalar = materialize_provided(col_idx, *value_idx)?;
            validate_nullable(schema, col_idx, &scalar)?;
            batch.column_mut(col_idx).unwrap().push(&scalar)?;
        } else {
            push_missing_insert_value(
                engine,
                db,
                table,
                auto_inc_counter,
                batch.column_mut(col_idx).unwrap(),
                schema,
                col_idx,
                timezone_offset_hours,
            )?;
        }
    }
    Ok(())
}

fn push_missing_insert_value(
    engine: &Engine,
    db: &str,
    table: &str,
    auto_inc_counter: &Option<Arc<AutoIncrementCounter>>,
    col: &mut ColumnArray,
    schema: &Schema,
    col_idx: usize,
    timezone_offset_hours: i32,
) -> Result<()> {
    let scalar = if schema.columns[col_idx].auto_increment {
        let next_val = match auto_inc_counter {
            Some(counter) => counter.next_value(),
            None => engine.auto_increment_manager().next_value(db, table)?,
        };
        materialize_auto_increment_value(schema, col_idx, next_val)
    } else if let Some(default) = &schema.columns[col_idx].default_value {
        materialize_default_value(default, schema, col_idx, timezone_offset_hours)?
    } else {
        ScalarValue::null_for_type(schema.columns[col_idx].data_type)
    };
    validate_nullable(schema, col_idx, &scalar)?;
    col.push(&scalar)
}

fn materialize_auto_increment_value(schema: &Schema, col_idx: usize, next_val: u64) -> ScalarValue {
    match schema.columns[col_idx].data_type {
        DataType::Int32 => ScalarValue::Int32(Some(next_val as i32)),
        DataType::Int64 => ScalarValue::Int64(Some(next_val as i64)),
        DataType::UInt32 => ScalarValue::UInt32(Some(next_val as u32)),
        DataType::UInt64 => ScalarValue::UInt64(Some(next_val)),
        _ => ScalarValue::Int64(Some(next_val as i64)),
    }
}

#[inline]
fn parse_text_fast(value: FastValue<'_>) -> Result<&str> {
    match value {
        FastValue::String(v) | FastValue::Bare(v) => Ok(v),
        FastValue::Null => Err(TaibaiError::Sql("unexpected NULL".into())),
    }
}

fn parse_owned_string_fast(value: FastValue<'_>) -> Result<String> {
    match value {
        FastValue::String(v) => {
            if v.contains("''") {
                Ok(v.replace("''", "'"))
            } else {
                Ok(v.to_string())
            }
        }
        FastValue::Bare(v) => Ok(v.to_string()),
        FastValue::Null => Err(TaibaiError::Sql("unexpected NULL".into())),
    }
}

#[inline]
fn parse_i64_fast(value: FastValue<'_>) -> Result<i64> {
    let text = parse_text_fast(value)?;
    parse_i64_bytes(text, text.as_bytes())
}

#[inline]
fn parse_i64_bytes(text: &str, bytes: &[u8]) -> Result<i64> {
    if bytes.is_empty() {
        return Err(TaibaiError::Sql(format!(
            "invalid integer literal '{}'",
            text
        )));
    }
    let mut idx = 0usize;
    let negative = match bytes[0] {
        b'-' => {
            idx = 1;
            true
        }
        b'+' => {
            idx = 1;
            false
        }
        _ => false,
    };
    if idx >= bytes.len() {
        return Err(TaibaiError::Sql(format!(
            "invalid integer literal '{}'",
            text
        )));
    }
    let mut result: i64 = 0;
    while idx < bytes.len() {
        let b = bytes[idx];
        if b < b'0' || b > b'9' {
            return Err(TaibaiError::Sql(format!(
                "invalid integer literal '{}'",
                text
            )));
        }
        result = match result
            .checked_mul(10)
            .and_then(|v| v.checked_add((b - b'0') as i64))
        {
            Some(v) => v,
            None => return Err(TaibaiError::Sql(format!("value '{}' overflows", text))),
        };
        idx += 1;
    }
    if negative {
        Ok(-result)
    } else {
        Ok(result)
    }
}

#[inline]
fn parse_u64_fast(value: FastValue<'_>) -> Result<u64> {
    let text = parse_text_fast(value)?;
    let bytes = text.as_bytes();
    if bytes.is_empty() {
        return Err(TaibaiError::Sql(format!(
            "invalid integer literal '{}'",
            text
        )));
    }
    let mut idx = 0usize;
    if bytes[0] == b'+' {
        idx = 1;
        if idx >= bytes.len() {
            return Err(TaibaiError::Sql(format!(
                "invalid integer literal '{}'",
                text
            )));
        }
    }
    let mut result: u64 = 0;
    while idx < bytes.len() {
        let b = bytes[idx];
        if b < b'0' || b > b'9' {
            return Err(TaibaiError::Sql(format!(
                "invalid integer literal '{}'",
                text
            )));
        }
        result = match result
            .checked_mul(10)
            .and_then(|v| v.checked_add((b - b'0') as u64))
        {
            Some(v) => v,
            None => return Err(TaibaiError::Sql(format!("value '{}' overflows", text))),
        };
        idx += 1;
    }
    Ok(result)
}

fn parse_f64_fast(value: FastValue<'_>) -> Result<f64> {
    parse_text_fast(value)?
        .parse::<f64>()
        .map_err(|e| TaibaiError::Sql(e.to_string()))
}

fn parse_bool_fast(value: FastValue<'_>) -> Result<bool> {
    let text = parse_text_fast(value)?;
    if text.eq_ignore_ascii_case("true") || text == "1" {
        Ok(true)
    } else if text.eq_ignore_ascii_case("false") || text == "0" {
        Ok(false)
    } else {
        Err(TaibaiError::Sql(format!("invalid BOOL literal '{}'", text)))
    }
}

#[inline]
fn parse_decimal_fast(value: FastValue<'_>, scale: i32) -> Result<i64> {
    let text = parse_text_fast(value)?;
    let bytes = text.as_bytes();
    parse_decimal_bytes(text, bytes, scale)
}

#[inline]
fn parse_decimal_bytes(text: &str, bytes: &[u8], scale: i32) -> Result<i64> {
    let scale_usize = usize::try_from(scale)
        .map_err(|_| TaibaiError::Sql(format!("invalid decimal scale {}", scale)))?;
    let len = bytes.len();
    let mut idx = 0usize;
    let negative = if len > 0 && bytes[0] == b'-' {
        idx = 1;
        true
    } else if len > 0 && bytes[0] == b'+' {
        idx = 1;
        false
    } else {
        false
    };
    let mut value: i64 = 0;
    let mut seen_digit = false;
    while idx < len {
        let b = bytes[idx];
        if b == b'.' {
            idx += 1;
            break;
        }
        if b < b'0' || b > b'9' {
            return Err(TaibaiError::Sql(format!(
                "invalid DECIMAL literal '{}'",
                text
            )));
        }
        seen_digit = true;
        value = match value
            .checked_mul(10)
            .and_then(|v| v.checked_add((b - b'0') as i64))
        {
            Some(v) => v,
            None => {
                return Err(TaibaiError::Sql(format!(
                    "decimal value '{}' overflows",
                    text
                )))
            }
        };
        idx += 1;
    }
    let mut frac_digits = 0usize;
    while frac_digits < scale_usize && idx < len {
        let b = bytes[idx];
        if b < b'0' || b > b'9' {
            break;
        }
        seen_digit = true;
        value = match value
            .checked_mul(10)
            .and_then(|v| v.checked_add((b - b'0') as i64))
        {
            Some(v) => v,
            None => {
                return Err(TaibaiError::Sql(format!(
                    "decimal value '{}' overflows",
                    text
                )))
            }
        };
        frac_digits += 1;
        idx += 1;
    }
    while frac_digits < scale_usize {
        value = match value.checked_mul(10) {
            Some(v) => v,
            None => {
                return Err(TaibaiError::Sql(format!(
                    "decimal value '{}' overflows",
                    text
                )))
            }
        };
        frac_digits += 1;
    }
    if idx < len {
        let b = bytes[idx];
        if b >= b'0' && b <= b'9' {
            if b >= b'5' {
                value = match value.checked_add(1) {
                    Some(v) => v,
                    None => {
                        return Err(TaibaiError::Sql(format!(
                            "decimal value '{}' overflows",
                            text
                        )))
                    }
                };
            }
            idx += 1;
            while idx < len {
                let r = bytes[idx];
                if r < b'0' || r > b'9' {
                    return Err(TaibaiError::Sql(format!(
                        "invalid DECIMAL literal '{}'",
                        text
                    )));
                }
                idx += 1;
            }
        } else {
            return Err(TaibaiError::Sql(format!(
                "invalid DECIMAL literal '{}'",
                text
            )));
        }
    }
    if !seen_digit {
        return Err(TaibaiError::Sql(format!(
            "invalid DECIMAL literal '{}'",
            text
        )));
    }
    if negative {
        Ok(-value)
    } else {
        Ok(value)
    }
}

fn overflow_error(value: i64, ty: &str) -> TaibaiError {
    TaibaiError::Sql(format!("value '{}' overflows {}", value, ty))
}

fn overflow_error_u(value: u64, ty: &str) -> TaibaiError {
    TaibaiError::Sql(format!("value '{}' overflows {}", value, ty))
}

fn estimate_values_rows(bytes: &[u8], start: usize) -> usize {
    bytes[start..].iter().filter(|b| **b == b'(').count()
}

pub fn exec_replace(
    engine: &Engine,
    db: &str,
    table: &str,
    columns: &[String],
    values: &[Vec<Expr>],
    timezone_offset_hours: i32,
    txn_id: u64,
    bulk_import_mode: bool,
) -> Result<ExecResult> {
    let _unique_guard = PRIMARY_KEY_INSERT_LOCK.lock().unwrap();
    let where_clause = build_replace_conflict_filter(engine, db, table, columns, values)?;
    let deleted = if where_clause.is_some() {
        match super::dml_delete::exec_delete(engine, db, table, &where_clause)? {
            ExecResult::Ok { rows_affected } => rows_affected,
            _ => 0,
        }
    } else {
        0
    };
    let inserted = match exec_insert_inner(
        engine,
        db,
        table,
        columns,
        values,
        timezone_offset_hours,
        txn_id,
        bulk_import_mode,
        false,
    )? {
        ExecResult::Ok { rows_affected } => rows_affected,
        other => return Ok(other),
    };
    Ok(ExecResult::Ok {
        rows_affected: deleted + inserted,
    })
}

pub fn exec_insert(
    engine: &Engine,
    db: &str,
    table: &str,
    columns: &[String],
    values: &[Vec<Expr>],
    timezone_offset_hours: i32,
    txn_id: u64,
    bulk_import_mode: bool,
) -> Result<ExecResult> {
    exec_insert_inner(
        engine,
        db,
        table,
        columns,
        values,
        timezone_offset_hours,
        txn_id,
        bulk_import_mode,
        true,
    )
}

fn exec_insert_inner(
    engine: &Engine,
    db: &str,
    table: &str,
    columns: &[String],
    values: &[Vec<Expr>],
    timezone_offset_hours: i32,
    txn_id: u64,
    bulk_import_mode: bool,
    lock_unique_constraints: bool,
) -> Result<ExecResult> {
    let catalogs = engine.catalogs().read().unwrap();
    let catalog = catalogs
        .get(db)
        .ok_or_else(|| TaibaiError::Sql(format!("database '{}' does not exist", db)))?;

    let table_meta = catalog
        .get_table(table)
        .ok_or_else(|| TaibaiError::Sql(format!("table '{}.{}' does not exist", db, table)))?
        .clone();

    let schema = Schema::new(table_meta.columns.clone());

    let auto_inc_cols: Vec<usize> = schema
        .columns
        .iter()
        .enumerate()
        .filter(|(_, c)| c.auto_increment)
        .map(|(i, _)| i)
        .collect();

    let col_indices = resolve_insert_columns(&schema, columns, values)?;

    let mut provided_by_col = vec![None; schema.columns.len()];
    for (value_idx, &col_idx) in col_indices.iter().enumerate() {
        provided_by_col[col_idx] = Some(value_idx);
    }

    let missing_auto_inc: Vec<usize> = auto_inc_cols
        .iter()
        .filter(|&idx| provided_by_col[*idx].is_none())
        .cloned()
        .collect();
    let mut is_missing_auto_inc = vec![false; schema.columns.len()];
    for &idx in &missing_auto_inc {
        is_missing_auto_inc[idx] = true;
    }

    let auto_inc_counter = if !missing_auto_inc.is_empty() {
        engine.auto_increment_manager().get(db, table)
    } else {
        None
    };

    let mut batch = RecordBatch::with_capacity(schema.clone(), values.len());

    if can_direct_materialize(&schema, &provided_by_col, &is_missing_auto_inc) {
        for row in values {
            for (col_idx, provided) in provided_by_col.iter().enumerate() {
                let val_idx = provided.unwrap();
                push_literal_fast(
                    batch.column_mut(col_idx).unwrap(),
                    &row[val_idx],
                    schema.columns[col_idx].scale,
                    timezone_offset_hours,
                )?;
            }
        }
    } else {
        for row in values {
            materialize_insert_row(
                engine,
                db,
                table,
                &auto_inc_counter,
                &mut batch,
                &schema,
                &provided_by_col,
                timezone_offset_hours,
                |col_idx, value_idx| {
                    eval_expr_to_scalar_with_scale_timezone(
                        &row[value_idx],
                        &schema.columns[col_idx].data_type,
                        schema.columns[col_idx].scale,
                        timezone_offset_hours,
                    )
                },
            )?;
        }
    }

    drop(catalogs);

    exec_materialized_insert_batch(
        engine,
        db,
        table,
        &table_meta,
        schema,
        batch,
        txn_id,
        bulk_import_mode,
        lock_unique_constraints,
        missing_auto_inc,
    )
}

fn exec_materialized_insert_batch(
    engine: &Engine,
    db: &str,
    table: &str,
    table_meta: &crate::storage::catalog::schema::TableMeta,
    schema: Schema,
    batch: RecordBatch,
    txn_id: u64,
    bulk_import_mode: bool,
    lock_unique_constraints: bool,
    missing_auto_inc: Vec<usize>,
) -> Result<ExecResult> {
    let auto_inc_cols: Vec<usize> = schema
        .columns
        .iter()
        .enumerate()
        .filter(|(_, c)| c.auto_increment)
        .map(|(i, _)| i)
        .collect();
    let mut is_missing_auto_inc = vec![false; schema.columns.len()];
    for &idx in &missing_auto_inc {
        if idx < is_missing_auto_inc.len() {
            is_missing_auto_inc[idx] = true;
        }
    }

    let total_start = Instant::now();
    let row_count = batch.num_rows() as u64;
    log_insert_profile(
        "cluster_sort",
        row_count as usize,
        std::time::Duration::ZERO,
    );

    let constraint_start = Instant::now();
    let unique_constraints = unique_constraints(table_meta, &schema)?;
    let has_unique_checks = !unique_constraints.is_empty();
    let primary_key_indices = primary_key_indices(&schema);
    let _pk_guard =
        if !lock_unique_constraints || (primary_key_indices.is_empty() && !has_unique_checks) {
            None
        } else {
            Some(PRIMARY_KEY_INSERT_LOCK.lock().unwrap())
        };

    validate_unique_constraints(engine, db, table, &batch, &schema, &unique_constraints)?;

    if !primary_key_indices.is_empty() {
        if generated_auto_increment_key_guarantees_uniqueness(
            &primary_key_indices,
            &is_missing_auto_inc,
        ) {
            validate_primary_key_values_present(&batch, &schema, &primary_key_indices)?;
        } else {
            validate_primary_key_uniqueness(
                engine,
                db,
                table,
                &batch,
                &schema,
                &primary_key_indices,
            )?;
        }
    }
    advance_explicit_auto_increment_counters(
        engine,
        db,
        table,
        &batch,
        &auto_inc_cols,
        &missing_auto_inc,
    )?;

    let auto_increment_touched = !auto_inc_cols.is_empty();
    let unique_index_entries =
        build_unique_index_entries_for_batch(table_meta, &schema, &batch, &unique_constraints)?;
    log_insert_profile(
        "constraints",
        row_count as usize,
        constraint_start.elapsed(),
    );

    if txn_id == 0 {
        match engine.feed_hot_store(db, table, table_meta, &batch) {
            Ok(()) => {
                if engine
                    .hot_store_manager()
                    .soft_limit_exceeded_for(db, table)
                {
                    let _ = engine.flush_table_buffers(db, table);
                }
            }
            Err(crate::common::error::TaibaiError::ResourceBusy(_)) => {
                engine.reclaim_hot_store_pressure(db, table);
                if let Err(e) = engine.feed_hot_store(db, table, table_meta, &batch) {
                    return Err(e);
                }
            }
            Err(e) => return Err(e),
        }
    }
    let write_start = Instant::now();
    if bulk_import_mode {
        engine
            .exec_bulk_insert_partitioned_with_wal_control(db, table, table_meta, batch, false)?;
    } else {
        write_batch_by_partition(engine, db, table, table_meta, batch, schema, txn_id)?;
    }
    log_insert_profile("write", row_count as usize, write_start.elapsed());

    let catalog_start = Instant::now();
    append_unique_index_entries_after_insert(engine, db, table, unique_index_entries)?;

    engine.record_inserted_rows(
        db,
        table,
        row_count as i64,
        !bulk_import_mode && txn_id > 0,
        !bulk_import_mode && txn_id == 0,
    )?;
    if auto_increment_touched {
        engine.persist_auto_increment_next_value(db, table, !bulk_import_mode && txn_id > 0)?;
    }
    log_insert_profile("catalog", row_count as usize, catalog_start.elapsed());
    log_insert_profile("total_exec", row_count as usize, total_start.elapsed());

    Ok(ExecResult::Ok {
        rows_affected: row_count,
    })
}

fn build_unique_index_entries_for_batch(
    table_meta: &crate::storage::catalog::schema::TableMeta,
    schema: &Schema,
    batch: &RecordBatch,
    constraints: &[(String, Vec<usize>)],
) -> Result<Vec<(String, Vec<UniqueIndexEntry>)>> {
    let mut result = Vec::new();
    for (index_name, indices) in constraints {
        let columns = table_meta
            .unique_indexes
            .iter()
            .find(|index| index.name == *index_name)
            .map(|index| index.columns.clone())
            .unwrap_or_default();
        let mut entries = Vec::new();
        for row_idx in 0..batch.num_rows() {
            let Some(key) = unique_key_for_row(batch, row_idx, indices, schema) else {
                continue;
            };
            entries.push(UniqueIndexEntry {
                key,
                location: UniqueRowLocation {
                    segment_path: String::new(),
                    row_id: row_idx as u64,
                },
            });
        }
        if !columns.is_empty() && !entries.is_empty() {
            result.push((index_name.clone(), entries));
        }
    }
    Ok(result)
}

fn append_unique_index_entries_after_insert(
    engine: &Engine,
    db: &str,
    table: &str,
    entries_by_index: Vec<(String, Vec<UniqueIndexEntry>)>,
) -> Result<()> {
    for (index_name, entries) in entries_by_index {
        let _ =
            engine
                .index_manager()
                .append_unique_index_entries(db, table, &index_name, entries)?;
    }
    Ok(())
}

fn build_replace_conflict_filter(
    engine: &Engine,
    db: &str,
    table: &str,
    columns: &[String],
    values: &[Vec<Expr>],
) -> Result<Option<Expr>> {
    let catalogs = engine.catalogs().read().unwrap();
    let catalog = catalogs
        .get(db)
        .ok_or_else(|| TaibaiError::Sql(format!("database '{}' does not exist", db)))?;
    let table_meta = catalog
        .get_table(table)
        .ok_or_else(|| TaibaiError::Sql(format!("table '{}.{}' does not exist", db, table)))?
        .clone();
    drop(catalogs);

    let schema = Schema::new(table_meta.columns.clone());
    let insert_indices = resolve_insert_columns(&schema, columns, values)?;
    let unique_constraints = unique_constraints(&table_meta, &schema)?;
    let mut predicates = Vec::new();
    for (_, index_indices) in unique_constraints {
        let mut value_positions = Vec::with_capacity(index_indices.len());
        let mut complete = true;
        for index_col in &index_indices {
            match insert_indices.iter().position(|col| col == index_col) {
                Some(pos) => value_positions.push((*index_col, pos)),
                None => {
                    complete = false;
                    break;
                }
            }
        }
        if !complete {
            continue;
        }
        for row in values {
            let mut row_predicate = None;
            for (col_idx, value_pos) in &value_positions {
                let Expr::Literal(value) = &row[*value_pos] else {
                    row_predicate = None;
                    break;
                };
                if value.is_null() {
                    row_predicate = None;
                    break;
                }
                let eq = Expr::BinaryOp {
                    left: Box::new(Expr::Column(schema.columns[*col_idx].name.clone())),
                    op: crate::sql::parser::ast::BinOp::Eq,
                    right: Box::new(Expr::Literal(value.clone())),
                };
                row_predicate = Some(match row_predicate {
                    Some(existing) => Expr::BinaryOp {
                        left: Box::new(existing),
                        op: crate::sql::parser::ast::BinOp::And,
                        right: Box::new(eq),
                    },
                    None => eq,
                });
            }
            if let Some(predicate) = row_predicate {
                predicates.push(predicate);
            }
        }
    }

    Ok(predicates.into_iter().reduce(|left, right| Expr::BinaryOp {
        left: Box::new(left),
        op: crate::sql::parser::ast::BinOp::Or,
        right: Box::new(right),
    }))
}

fn write_batch_by_partition(
    engine: &Engine,
    db: &str,
    table: &str,
    table_meta: &crate::storage::catalog::schema::TableMeta,
    batch: RecordBatch,
    schema: Schema,
    txn_id: u64,
) -> Result<()> {
    if table_meta.partition_key.is_empty() || batch.num_rows() == 0 {
        write_partition_batch(engine, db, table, table_meta, "", batch, schema, txn_id)?;
        return Ok(());
    }

    let partition_start = Instant::now();
    let single_partition = single_partition_key(table_meta, &batch);
    log_insert_profile(
        "partition_route",
        batch.num_rows(),
        partition_start.elapsed(),
    );
    if let Some(partition_key) = single_partition {
        write_partition_batch(
            engine,
            db,
            table,
            table_meta,
            &partition_key,
            batch,
            schema,
            txn_id,
        )?;
        return Ok(());
    }

    if let Some(ranges) = partition_ranges_if_contiguous(table_meta, &batch) {
        for (partition_key, start, end) in ranges {
            let part_batch = slice_batch_fast(&batch, start, end)?;
            write_partition_batch(
                engine,
                db,
                table,
                table_meta,
                &partition_key,
                part_batch,
                schema.clone(),
                txn_id,
            )?;
        }
        return Ok(());
    }

    let groups = partition_masks(table_meta, &batch);
    if groups.len() <= 1 {
        let partition_key = groups
            .first()
            .map(|(key, _)| key.clone())
            .unwrap_or_default();
        write_partition_batch(
            engine,
            db,
            table,
            table_meta,
            &partition_key,
            batch,
            schema,
            txn_id,
        )?;
        return Ok(());
    }

    for (partition_key, mask) in groups {
        let match_count = mask.iter().filter(|keep| **keep).count();
        let part_batch = filter_batch_by_mask(&batch, &mask, match_count)?;
        write_partition_batch(
            engine,
            db,
            table,
            table_meta,
            &partition_key,
            part_batch,
            schema.clone(),
            txn_id,
        )?;
    }
    Ok(())
}

fn write_partition_batch(
    engine: &Engine,
    db: &str,
    table: &str,
    table_meta: &crate::storage::catalog::schema::TableMeta,
    partition_key: &str,
    batch: RecordBatch,
    schema: Schema,
    txn_id: u64,
) -> Result<()> {
    let batch = sort_batch_for_cluster_key(table_meta, batch)?;
    engine.batch_writer().write_columns_with_txn(
        db,
        table,
        partition_key,
        batch,
        schema,
        txn_id,
    )?;
    Ok(())
}

fn sort_batch_for_cluster_key(
    table_meta: &crate::storage::catalog::schema::TableMeta,
    batch: RecordBatch,
) -> Result<RecordBatch> {
    if table_meta.cluster_key.is_empty() || batch.num_rows() <= 1 {
        return Ok(batch);
    }

    let mut key_indices = Vec::with_capacity(table_meta.cluster_key.len());
    for key_col in &table_meta.cluster_key {
        let idx = batch.schema.index_of(key_col).ok_or_else(|| {
            TaibaiError::Sql(format!("cluster key column '{}' not found", key_col))
        })?;
        key_indices.push(idx);
    }
    let desc = vec![false; key_indices.len()];
    let inserter = SortKeyInserter::new(key_indices, desc);
    if inserter.is_sorted(&batch) {
        return Ok(batch);
    }
    sort_batch_by_key(&batch, &inserter)
}

fn single_partition_key(
    table_meta: &crate::storage::catalog::schema::TableMeta,
    batch: &RecordBatch,
) -> Option<String> {
    let col_idx = table_meta.get_partition_column_index_cached()?;
    let first = batch
        .scalar(0, col_idx)
        .unwrap_or(crate::types::scalar::ScalarValue::Int64(None));
    let first_key = compute_partition_key_for_value(table_meta, &first);

    for row_idx in 1..batch.num_rows() {
        let val = batch
            .scalar(row_idx, col_idx)
            .unwrap_or(crate::types::scalar::ScalarValue::Int64(None));
        if compute_partition_key_for_value(table_meta, &val) != first_key {
            return None;
        }
    }

    Some(first_key)
}

fn partition_ranges_if_contiguous(
    table_meta: &crate::storage::catalog::schema::TableMeta,
    batch: &RecordBatch,
) -> Option<Vec<(String, usize, usize)>> {
    if table_meta.partition_key.is_empty() || batch.num_rows() == 0 {
        return Some(vec![(String::new(), 0, batch.num_rows())]);
    }
    let col_idx = table_meta.get_partition_column_index_cached()?;
    let first = batch
        .scalar(0, col_idx)
        .unwrap_or(crate::types::scalar::ScalarValue::Int64(None));
    let mut current_key = compute_partition_key_for_value(table_meta, &first);
    let mut current_start = 0usize;
    let mut closed = HashSet::new();
    let mut ranges = Vec::new();

    for row_idx in 1..batch.num_rows() {
        let val = batch
            .scalar(row_idx, col_idx)
            .unwrap_or(crate::types::scalar::ScalarValue::Int64(None));
        let key = compute_partition_key_for_value(table_meta, &val);
        if key != current_key {
            if !closed.insert(current_key.clone()) {
                return None;
            }
            ranges.push((current_key, current_start, row_idx));
            if closed.contains(&key) {
                return None;
            }
            current_key = key;
            current_start = row_idx;
        }
    }
    ranges.push((current_key, current_start, batch.num_rows()));
    Some(ranges)
}

fn slice_batch_fast(batch: &RecordBatch, start: usize, end: usize) -> Result<RecordBatch> {
    if start == 0 && end == batch.num_rows() {
        return Ok(batch.clone());
    }
    let columns = batch
        .columns
        .iter()
        .map(|col| col.slice_fast(start, end))
        .collect();
    RecordBatch::try_new(batch.schema.clone(), columns)
}

fn partition_masks(
    table_meta: &crate::storage::catalog::schema::TableMeta,
    batch: &RecordBatch,
) -> Vec<(String, Vec<bool>)> {
    if table_meta.partition_key.is_empty() || batch.num_rows() == 0 {
        return vec![(String::new(), vec![true; batch.num_rows()])];
    }
    let col_idx = match table_meta.get_partition_column_index_cached() {
        Some(idx) => idx,
        None => return vec![(String::new(), vec![true; batch.num_rows()])],
    };
    let mut key_order = Vec::new();
    let mut groups: HashMap<String, Vec<bool>> = HashMap::new();
    for row_idx in 0..batch.num_rows() {
        let val = batch
            .scalar(row_idx, col_idx)
            .unwrap_or(crate::types::scalar::ScalarValue::Int64(None));
        let key = compute_partition_key_for_value(table_meta, &val);
        if !groups.contains_key(&key) {
            key_order.push(key.clone());
            groups.insert(key.clone(), vec![false; batch.num_rows()]);
        }
        if let Some(mask) = groups.get_mut(&key) {
            mask[row_idx] = true;
        }
    }
    key_order
        .into_iter()
        .filter_map(|key| groups.remove(&key).map(|mask| (key, mask)))
        .collect()
}

fn filter_batch_by_mask(
    batch: &RecordBatch,
    mask: &[bool],
    match_count: usize,
) -> Result<RecordBatch> {
    let columns = batch
        .columns
        .iter()
        .map(|col| col.filter_by_mask_fast(mask, match_count))
        .collect();
    RecordBatch::try_new(batch.schema.clone(), columns)
}

fn can_direct_materialize(
    schema: &Schema,
    provided_by_col: &[Option<usize>],
    is_missing_auto_inc: &[bool],
) -> bool {
    schema.columns.iter().enumerate().all(|(idx, col)| {
        provided_by_col[idx].is_some()
            && !is_missing_auto_inc[idx]
            && col.default_value.is_none()
            && col.nullable
            && !col.primary_key
    })
}

fn push_literal_fast(
    col: &mut ColumnArray,
    expr: &Expr,
    scale: i32,
    timezone_offset_hours: i32,
) -> Result<()> {
    let Expr::Literal(value) = expr else {
        return col.push(&eval_expr_to_scalar_with_scale_timezone(
            expr,
            &col.data_type(),
            scale,
            timezone_offset_hours,
        )?);
    };

    if value.is_null() {
        return col.push(&ScalarValue::null_for_type(col.data_type()));
    }

    match (col, value) {
        (ColumnArray::Int32 { data, bitmap }, ScalarValue::Int64(Some(v))) => {
            data.push(
                i32::try_from(*v)
                    .map_err(|_| TaibaiError::Sql(format!("value '{}' overflows INT32", v)))?,
            );
            bitmap.push_valid();
            Ok(())
        }
        (ColumnArray::Int64 { data, bitmap }, ScalarValue::Int64(Some(v))) => {
            data.push(*v);
            bitmap.push_valid();
            Ok(())
        }
        (ColumnArray::Timestamp { data, bitmap }, ScalarValue::Int64(Some(v))) => {
            data.push(*v);
            bitmap.push_valid();
            Ok(())
        }
        (ColumnArray::Decimal { data, bitmap }, ScalarValue::Float64(Some(v))) => {
            let multiplier = decimal_multiplier(scale)?;
            data.push((*v * multiplier as f64).round() as i64);
            bitmap.push_valid();
            Ok(())
        }
        (ColumnArray::Decimal { data, bitmap }, ScalarValue::Int64(Some(v))) => {
            let multiplier = decimal_multiplier(scale)?;
            data.push(v.checked_mul(multiplier).ok_or_else(|| {
                TaibaiError::Sql(format!("decimal value '{}' overflows scale {}", v, scale))
            })?);
            bitmap.push_valid();
            Ok(())
        }
        (ColumnArray::VarChar { data, bitmap }, ScalarValue::VarChar(Some(v))) => {
            data.push(v.clone());
            bitmap.push_valid();
            Ok(())
        }
        (col, value) if col.data_type() == value.data_type() => col.push(value),
        (col, value) => col.push(&eval_expr_to_scalar_with_scale_timezone(
            &Expr::Literal(value.clone()),
            &col.data_type(),
            scale,
            timezone_offset_hours,
        )?),
    }
}

fn decimal_multiplier(scale: i32) -> Result<i64> {
    if !(0..=18).contains(&scale) {
        return Err(TaibaiError::Sql(format!("invalid decimal scale {}", scale)));
    }
    10_i64
        .checked_pow(scale as u32)
        .ok_or_else(|| TaibaiError::Sql(format!("invalid decimal scale {}", scale)))
}

fn resolve_insert_columns(
    schema: &Schema,
    columns: &[String],
    values: &[Vec<Expr>],
) -> Result<Vec<usize>> {
    if columns.is_empty() {
        for row in values {
            if row.len() != schema.columns.len() {
                return Err(TaibaiError::Sql(format!(
                    "insert has {} values but table has {} columns",
                    row.len(),
                    schema.columns.len()
                )));
            }
        }
        return Ok((0..schema.columns.len()).collect());
    }

    let mut seen = HashSet::new();
    let mut indices = Vec::with_capacity(columns.len());
    for col_name in columns {
        let idx = schema
            .index_of(col_name)
            .ok_or_else(|| TaibaiError::Sql(format!("column '{}' not found", col_name)))?;
        if !seen.insert(idx) {
            return Err(TaibaiError::Sql(format!(
                "column '{}' specified more than once",
                col_name
            )));
        }
        indices.push(idx);
    }

    for row in values {
        if row.len() != columns.len() {
            return Err(TaibaiError::Sql(format!(
                "insert has {} values but {} columns were specified",
                row.len(),
                columns.len()
            )));
        }
    }

    Ok(indices)
}

fn materialize_default_value(
    default: &ColumnDefaultValue,
    schema: &Schema,
    col_idx: usize,
    timezone_offset_hours: i32,
) -> Result<ScalarValue> {
    match default {
        ColumnDefaultValue::CurrentTimestamp => {
            if schema.columns[col_idx].data_type != DataType::Timestamp {
                return Err(TaibaiError::Sql(format!(
                    "CURRENT_TIMESTAMP default requires TIMESTAMP column '{}'",
                    schema.columns[col_idx].name
                )));
            }
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_micros() as i64;
            Ok(ScalarValue::Timestamp(Some(now)))
        }
        ColumnDefaultValue::Literal(value) => {
            let expr = Expr::Literal(value.clone());
            eval_expr_to_scalar_with_scale_timezone(
                &expr,
                &schema.columns[col_idx].data_type,
                schema.columns[col_idx].scale,
                timezone_offset_hours,
            )
        }
    }
}

fn validate_nullable(schema: &Schema, col_idx: usize, scalar: &ScalarValue) -> Result<()> {
    let col = &schema.columns[col_idx];
    if (col.primary_key || !col.nullable) && scalar.is_null() {
        return Err(TaibaiError::Sql(format!(
            "column '{}' cannot be NULL",
            col.name
        )));
    }
    Ok(())
}

fn unique_constraints(
    table_meta: &crate::storage::catalog::schema::TableMeta,
    schema: &Schema,
) -> Result<Vec<(String, Vec<usize>)>> {
    let mut constraints = Vec::new();
    for index in &table_meta.unique_indexes {
        let mut indices = Vec::with_capacity(index.columns.len());
        for column in &index.columns {
            indices.push(schema.index_of(column).ok_or_else(|| {
                TaibaiError::Sql(format!("unique index column '{}' not found", column))
            })?);
        }
        constraints.push((index.name.clone(), indices));
    }
    Ok(constraints)
}

fn validate_unique_constraints(
    engine: &Engine,
    db: &str,
    table: &str,
    batch: &RecordBatch,
    schema: &Schema,
    constraints: &[(String, Vec<usize>)],
) -> Result<()> {
    for (name, indices) in constraints {
        validate_unique_key_uniqueness(engine, db, table, schema, indices, name, Some(batch))?;
    }
    Ok(())
}

pub fn validate_unique_key_uniqueness(
    engine: &Engine,
    db: &str,
    table: &str,
    schema: &Schema,
    indices: &[usize],
    index_name: &str,
    new_batch: Option<&RecordBatch>,
) -> Result<()> {
    let mut keys = HashSet::new();
    if let Some(batch) = new_batch {
        for row_idx in 0..batch.num_rows() {
            let Some(key) = unique_key_for_row(batch, row_idx, indices, schema) else {
                continue;
            };
            if !keys.insert(key.clone()) {
                return Err(duplicate_unique_key_error(table, index_name, &key));
            }
        }
    }
    if new_batch.is_some() {
        if let Some(data) = engine
            .index_manager()
            .load_unique_index_data(db, table, index_name)?
        {
            let index_columns = indices
                .iter()
                .map(|idx| schema.columns[*idx].name.clone())
                .collect::<Vec<_>>();
            if data.columns == index_columns {
                for key in &keys {
                    if engine
                        .index_manager()
                        .unique_key_exists(db, table, index_name, key)?
                        .unwrap_or(false)
                    {
                        return Err(duplicate_unique_key_error(table, index_name, key));
                    }
                }
                return Ok(());
            }
        }
    }
    load_existing_unique_keys(engine, db, table, indices, index_name, &mut keys)
}

pub fn collect_unique_index_entries(
    engine: &Engine,
    db: &str,
    table: &str,
    indices: &[usize],
    index_name: &str,
) -> Result<Vec<UniqueIndexEntry>> {
    let mut keys = HashSet::new();
    let mut entries = Vec::new();
    let table_dir = engine.data_dir().join(db).join(table);
    if table_dir.exists() {
        for seg_path in SegmentManager::discover_segments(&table_dir)? {
            let reader = engine.segment_mgr().open_reader_cached(&seg_path)?;
            for rg_idx in 0..reader.num_row_groups() {
                let mut batch = reader.read_row_group_with_projection(rg_idx, indices)?;
                let rg_start = reader.row_group_start_row(rg_idx);
                if !reader.delete_bitmap().is_empty() {
                    batch = crate::storage::segment::delete_bitmap::filter_batch(
                        &batch,
                        rg_start,
                        reader.delete_bitmap(),
                    )?;
                }
                let projected_indices: Vec<usize> = (0..batch.num_columns()).collect();
                for row_idx in 0..batch.num_rows() {
                    let Some(key) =
                        unique_key_for_row(&batch, row_idx, &projected_indices, &batch.schema)
                    else {
                        continue;
                    };
                    if !keys.insert(key.clone()) {
                        return Err(duplicate_unique_key_error(table, index_name, &key));
                    }
                    entries.push(UniqueIndexEntry {
                        key,
                        location: UniqueRowLocation {
                            segment_path: seg_path.to_string_lossy().to_string(),
                            row_id: rg_start + row_idx as u64,
                        },
                    });
                }
            }
        }
    }
    for memtable in engine.memtable_manager().get_by_table(db, table) {
        if let Some(batch) = memtable.read() {
            for row_idx in 0..batch.num_rows() {
                let Some(key) = unique_key_for_row(&batch, row_idx, indices, &batch.schema) else {
                    continue;
                };
                if !keys.insert(key.clone()) {
                    return Err(duplicate_unique_key_error(table, index_name, &key));
                }
                entries.push(UniqueIndexEntry {
                    key,
                    location: UniqueRowLocation {
                        segment_path: String::new(),
                        row_id: row_idx as u64,
                    },
                });
            }
        }
    }
    Ok(entries)
}

fn load_existing_unique_keys(
    engine: &Engine,
    db: &str,
    table: &str,
    indices: &[usize],
    index_name: &str,
    inserted_keys: &mut HashSet<Vec<ScalarValue>>,
) -> Result<()> {
    let table_dir = engine.data_dir().join(db).join(table);
    if table_dir.exists() {
        for seg_path in SegmentManager::discover_segments(&table_dir)? {
            let reader = engine.segment_mgr().open_reader_cached(&seg_path)?;
            for rg_idx in 0..reader.num_row_groups() {
                let mut batch = reader.read_row_group_with_projection(rg_idx, indices)?;
                if !reader.delete_bitmap().is_empty() {
                    batch = crate::storage::segment::delete_bitmap::filter_batch(
                        &batch,
                        reader.row_group_start_row(rg_idx),
                        reader.delete_bitmap(),
                    )?;
                }
                check_existing_unique_batch(table, index_name, &batch, inserted_keys)?;
            }
        }
    }
    for memtable in engine.memtable_manager().get_by_table(db, table) {
        if let Some(batch) = memtable.read() {
            check_existing_unique_batch_with_indices(
                table,
                index_name,
                &batch,
                indices,
                inserted_keys,
            )?;
        }
    }
    Ok(())
}

fn check_existing_unique_batch(
    table: &str,
    index_name: &str,
    batch: &RecordBatch,
    inserted_keys: &mut HashSet<Vec<ScalarValue>>,
) -> Result<()> {
    let projected_indices: Vec<usize> = (0..batch.num_columns()).collect();
    check_existing_unique_batch_with_indices(
        table,
        index_name,
        batch,
        &projected_indices,
        inserted_keys,
    )
}

fn check_existing_unique_batch_with_indices(
    table: &str,
    index_name: &str,
    batch: &RecordBatch,
    indices: &[usize],
    inserted_keys: &mut HashSet<Vec<ScalarValue>>,
) -> Result<()> {
    for row_idx in 0..batch.num_rows() {
        let Some(key) = unique_key_for_row(batch, row_idx, indices, &batch.schema) else {
            continue;
        };
        if !inserted_keys.insert(key.clone()) {
            return Err(duplicate_unique_key_error(table, index_name, &key));
        }
    }
    Ok(())
}

fn unique_key_for_row(
    batch: &RecordBatch,
    row_idx: usize,
    indices: &[usize],
    schema: &Schema,
) -> Option<Vec<ScalarValue>> {
    let mut key = Vec::with_capacity(indices.len());
    for &col_idx in indices {
        let value = batch
            .scalar(row_idx, col_idx)
            .unwrap_or(ScalarValue::null_for_type(
                schema.columns[col_idx].data_type,
            ));
        if value.is_null() {
            return None;
        }
        key.push(value);
    }
    Some(key)
}

pub fn validate_unique_indexes_in_batches(
    table: &str,
    schema: &Schema,
    unique_indexes: &[crate::storage::catalog::schema::UniqueIndexMeta],
    batches: &[RecordBatch],
) -> Result<()> {
    for index in unique_indexes {
        let mut indices = Vec::with_capacity(index.columns.len());
        for column in &index.columns {
            indices.push(schema.index_of(column).ok_or_else(|| {
                TaibaiError::Sql(format!("unique index column '{}' not found", column))
            })?);
        }
        let mut keys = HashSet::new();
        for batch in batches {
            for row_idx in 0..batch.num_rows() {
                let Some(key) = unique_key_for_row(batch, row_idx, &indices, schema) else {
                    continue;
                };
                if !keys.insert(key.clone()) {
                    return Err(duplicate_unique_key_error(table, &index.name, &key));
                }
            }
        }
    }
    Ok(())
}

fn duplicate_unique_key_error(table: &str, index_name: &str, key: &[ScalarValue]) -> TaibaiError {
    let key_text = key
        .iter()
        .map(|v| v.to_text().unwrap_or_else(|| "NULL".to_string()))
        .collect::<Vec<_>>()
        .join(", ");
    TaibaiError::Sql(format!(
        "duplicate unique key '{}' for table '{}': ({})",
        index_name, table, key_text
    ))
}

fn primary_key_indices(schema: &Schema) -> Vec<usize> {
    schema
        .columns
        .iter()
        .enumerate()
        .filter(|(_, col)| col.primary_key)
        .map(|(idx, _)| idx)
        .collect()
}

fn generated_auto_increment_key_guarantees_uniqueness(
    primary_key_indices: &[usize],
    is_missing_auto_inc: &[bool],
) -> bool {
    primary_key_indices
        .iter()
        .any(|&idx| is_missing_auto_inc.get(idx).copied().unwrap_or(false))
}

fn validate_primary_key_values_present(
    batch: &RecordBatch,
    schema: &Schema,
    primary_key_indices: &[usize],
) -> Result<()> {
    for row_idx in 0..batch.num_rows() {
        primary_key_for_row(batch, row_idx, primary_key_indices, schema)?;
    }
    Ok(())
}

fn validate_primary_key_uniqueness(
    engine: &Engine,
    db: &str,
    table: &str,
    batch: &RecordBatch,
    schema: &Schema,
    primary_key_indices: &[usize],
) -> Result<()> {
    let mut keys = HashSet::new();
    for row_idx in 0..batch.num_rows() {
        let key = primary_key_for_row(batch, row_idx, primary_key_indices, schema)?;
        if !keys.insert(key.clone()) {
            return Err(duplicate_primary_key_error(table, &key));
        }
    }

    load_existing_primary_keys(engine, db, table, primary_key_indices, &mut keys)?;
    Ok(())
}

fn advance_explicit_auto_increment_counters(
    engine: &Engine,
    db: &str,
    table: &str,
    batch: &RecordBatch,
    auto_inc_cols: &[usize],
    missing_auto_inc: &[usize],
) -> Result<()> {
    for &col_idx in auto_inc_cols {
        if missing_auto_inc.contains(&col_idx) {
            continue;
        }

        let mut max_value = None;
        for row_idx in 0..batch.num_rows() {
            let Some(value) = batch.scalar(row_idx, col_idx) else {
                continue;
            };
            let Some(int_value) = value.integer_i128() else {
                continue;
            };
            let Ok(unsigned) = u64::try_from(int_value) else {
                continue;
            };
            max_value = Some(max_value.map_or(unsigned, |current: u64| current.max(unsigned)));
        }

        if let Some(value) = max_value {
            engine
                .auto_increment_manager()
                .advance_past(db, table, value)?;
        }
    }
    Ok(())
}

fn load_existing_primary_keys(
    engine: &Engine,
    db: &str,
    table: &str,
    primary_key_indices: &[usize],
    inserted_keys: &mut HashSet<Vec<ScalarValue>>,
) -> Result<()> {
    let table_dir = engine.data_dir().join(db).join(table);
    if table_dir.exists() {
        for seg_path in SegmentManager::discover_segments(&table_dir)? {
            let reader = engine.segment_mgr().open_reader_cached(&seg_path)?;
            for rg_idx in 0..reader.num_row_groups() {
                let batch = reader.read_row_group_with_projection(rg_idx, primary_key_indices)?;
                check_existing_batch(table, &batch, inserted_keys)?;
            }
        }
    }

    for memtable in engine.memtable_manager().get_by_table(db, table) {
        if let Some(batch) = memtable.read() {
            check_existing_batch_with_indices(table, &batch, primary_key_indices, inserted_keys)?;
        }
    }

    Ok(())
}

fn check_existing_batch(
    table: &str,
    batch: &RecordBatch,
    inserted_keys: &HashSet<Vec<ScalarValue>>,
) -> Result<()> {
    let projected_indices: Vec<usize> = (0..batch.num_columns()).collect();
    check_existing_batch_with_indices(table, batch, &projected_indices, inserted_keys)
}

fn check_existing_batch_with_indices(
    table: &str,
    batch: &RecordBatch,
    primary_key_indices: &[usize],
    inserted_keys: &HashSet<Vec<ScalarValue>>,
) -> Result<()> {
    for row_idx in 0..batch.num_rows() {
        let key = primary_key_for_row(batch, row_idx, primary_key_indices, &batch.schema)?;
        if inserted_keys.contains(&key) {
            return Err(duplicate_primary_key_error(table, &key));
        }
    }
    Ok(())
}

fn primary_key_for_row(
    batch: &RecordBatch,
    row_idx: usize,
    primary_key_indices: &[usize],
    schema: &Schema,
) -> Result<Vec<ScalarValue>> {
    let mut key = Vec::with_capacity(primary_key_indices.len());
    for &col_idx in primary_key_indices {
        let value = batch
            .scalar(row_idx, col_idx)
            .unwrap_or(ScalarValue::null_for_type(
                schema.columns[col_idx].data_type,
            ));
        if value.is_null() {
            return Err(TaibaiError::Sql(format!(
                "primary key column '{}' cannot be NULL",
                schema.columns[col_idx].name
            )));
        }
        key.push(value);
    }
    Ok(key)
}

fn duplicate_primary_key_error(table: &str, key: &[ScalarValue]) -> TaibaiError {
    let key_text = key
        .iter()
        .map(|v| v.to_text().unwrap_or_else(|| "NULL".to_string()))
        .collect::<Vec<_>>()
        .join(", ");
    TaibaiError::Sql(format!(
        "duplicate primary key for table '{}': ({})",
        table, key_text
    ))
}

fn compute_partition_key_for_value(
    table_meta: &crate::storage::catalog::schema::TableMeta,
    val: &ScalarValue,
) -> String {
    if table_meta.partition_key.is_empty() {
        return String::new();
    }
    use crate::storage::catalog::schema::PartitionType;
    match table_meta.partition_type {
        PartitionType::Day
        | PartitionType::Year
        | PartitionType::Month
        | PartitionType::Hash
        | PartitionType::Mod => {
            crate::storage::partition::partition_function::compute_partition_key_from_scalar(
                &table_meta.partition_strategy(),
                val,
            )
        }
        PartitionType::Value => val.to_text().unwrap_or_default(),
        PartitionType::None => String::new(),
    }
}
