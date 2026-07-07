use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

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
    let col_indices = match resolve_fast_insert_columns(&schema, &columns) {
        Ok(indices) => indices,
        Err(e) => return FastInsertOutcome::Executed(Err(e)),
    };
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
    let auto_inc_counter = if !missing_auto_inc.is_empty() {
        engine.auto_increment_manager().get(db, table)
    } else {
        None
    };

    let capacity = estimate_values_rows(bytes, pos).min(100_000).max(1);
    let mut batch = RecordBatch::with_capacity(schema.clone(), capacity);
    let mut rows = 0usize;

    loop {
        skip_spaces(bytes, &mut pos);
        if pos >= bytes.len() {
            break;
        }
        if bytes[pos] != b'(' {
            return FastInsertOutcome::Unsupported;
        }
        pos += 1;
        let mut row_values = Vec::with_capacity(col_indices.len());
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
        skip_spaces(bytes, &mut pos);
        if pos >= bytes.len() || bytes[pos] != b')' {
            return FastInsertOutcome::Unsupported;
        }
        if let Err(e) = materialize_insert_row(
            engine,
            db,
            table,
            &auto_inc_counter,
            &mut batch,
            &schema,
            &provided_by_col,
            timezone_offset_hours,
            |col_idx, value_idx| materialize_fast_value(&schema, col_idx, row_values[value_idx]),
        ) {
            return FastInsertOutcome::Executed(Err(e));
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

fn materialize_fast_value(
    schema: &Schema,
    col_idx: usize,
    value: FastValue<'_>,
) -> Result<ScalarValue> {
    if matches!(value, FastValue::Null) {
        return Ok(ScalarValue::null_for_type(
            schema.columns[col_idx].data_type,
        ));
    }
    Ok(match schema.columns[col_idx].data_type {
        DataType::Bool => ScalarValue::Bool(Some(parse_bool_fast(value)?)),
        DataType::Int8 => ScalarValue::Int8(Some(
            parse_i64_fast(value)
                .and_then(|v| i8::try_from(v).map_err(|_| overflow_error(v, "INT8")))?,
        )),
        DataType::Int16 => ScalarValue::Int16(Some(
            parse_i64_fast(value)
                .and_then(|v| i16::try_from(v).map_err(|_| overflow_error(v, "INT16")))?,
        )),
        DataType::Int32 => ScalarValue::Int32(Some(
            parse_i64_fast(value)
                .and_then(|v| i32::try_from(v).map_err(|_| overflow_error(v, "INT32")))?,
        )),
        DataType::Int64 => ScalarValue::Int64(Some(parse_i64_fast(value)?)),
        DataType::UInt8 => ScalarValue::UInt8(Some(
            parse_u64_fast(value)
                .and_then(|v| u8::try_from(v).map_err(|_| overflow_error_u(v, "UINT8")))?,
        )),
        DataType::UInt16 => {
            ScalarValue::UInt16(Some(parse_u64_fast(value).and_then(|v| {
                u16::try_from(v).map_err(|_| overflow_error_u(v, "UINT16"))
            })?))
        }
        DataType::UInt32 => {
            ScalarValue::UInt32(Some(parse_u64_fast(value).and_then(|v| {
                u32::try_from(v).map_err(|_| overflow_error_u(v, "UINT32"))
            })?))
        }
        DataType::UInt64 => ScalarValue::UInt64(Some(parse_u64_fast(value)?)),
        DataType::Float32 => ScalarValue::Float32(Some(parse_f64_fast(value)? as f32)),
        DataType::Float64 => ScalarValue::Float64(Some(parse_f64_fast(value)?)),
        DataType::Decimal => ScalarValue::Decimal(Some(parse_decimal_fast(
            value,
            schema.columns[col_idx].scale,
        )?)),
        DataType::Date => ScalarValue::Date(Some(
            parse_i64_fast(value)
                .and_then(|v| i32::try_from(v).map_err(|_| overflow_error(v, "DATE")))?,
        )),
        DataType::Time => ScalarValue::Time(Some(parse_i64_fast(value)?)),
        DataType::Timestamp => ScalarValue::Timestamp(Some(parse_i64_fast(value)?)),
        DataType::Interval => ScalarValue::Interval(Some(parse_i64_fast(value)?)),
        DataType::VarChar => ScalarValue::VarChar(Some(parse_owned_string_fast(value)?)),
        DataType::Blob | DataType::Vector => {
            return Err(TaibaiError::Sql(format!(
                "fast INSERT does not support {} literal",
                schema.columns[col_idx].data_type.display_name()
            )))
        }
    })
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

fn parse_i64_fast(value: FastValue<'_>) -> Result<i64> {
    parse_text_fast(value)?
        .parse::<i64>()
        .map_err(|e| TaibaiError::Sql(e.to_string()))
}

fn parse_u64_fast(value: FastValue<'_>) -> Result<u64> {
    parse_text_fast(value)?
        .parse::<u64>()
        .map_err(|e| TaibaiError::Sql(e.to_string()))
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

fn parse_decimal_fast(value: FastValue<'_>, scale: i32) -> Result<i64> {
    let text = parse_text_fast(value)?.trim();
    let scale_usize = usize::try_from(scale)
        .map_err(|_| TaibaiError::Sql(format!("invalid decimal scale {}", scale)))?;
    let mut chars = text.as_bytes().iter().copied().peekable();
    let negative = match chars.peek() {
        Some(b'-') => {
            chars.next();
            true
        }
        Some(b'+') => {
            chars.next();
            false
        }
        _ => false,
    };
    let mut value: i64 = 0;
    let mut seen_digit = false;
    while let Some(ch) = chars.peek().copied() {
        if ch == b'.' {
            chars.next();
            break;
        }
        if !ch.is_ascii_digit() {
            return Err(TaibaiError::Sql(format!(
                "invalid DECIMAL literal '{}'",
                text
            )));
        }
        seen_digit = true;
        value = value
            .checked_mul(10)
            .and_then(|v| v.checked_add((ch - b'0') as i64))
            .ok_or_else(|| TaibaiError::Sql(format!("decimal value '{}' overflows", text)))?;
        chars.next();
    }
    let mut frac_digits = 0usize;
    while frac_digits < scale_usize {
        match chars.peek().copied() {
            Some(ch) if ch.is_ascii_digit() => {
                seen_digit = true;
                value = value
                    .checked_mul(10)
                    .and_then(|v| v.checked_add((ch - b'0') as i64))
                    .ok_or_else(|| {
                        TaibaiError::Sql(format!("decimal value '{}' overflows", text))
                    })?;
                chars.next();
                frac_digits += 1;
            }
            _ => break,
        }
    }
    while frac_digits < scale_usize {
        value = value
            .checked_mul(10)
            .ok_or_else(|| TaibaiError::Sql(format!("decimal value '{}' overflows", text)))?;
        frac_digits += 1;
    }
    if let Some(ch) = chars.peek().copied() {
        if ch.is_ascii_digit() {
            if ch >= b'5' {
                value = value.checked_add(1).ok_or_else(|| {
                    TaibaiError::Sql(format!("decimal value '{}' overflows", text))
                })?;
            }
            chars.next();
            while let Some(rest) = chars.next() {
                if !rest.is_ascii_digit() {
                    return Err(TaibaiError::Sql(format!(
                        "invalid DECIMAL literal '{}'",
                        text
                    )));
                }
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
    mut batch: RecordBatch,
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

    let has_sort_key = !table_meta.cluster_key.is_empty();
    if has_sort_key {
        let key_indices: Vec<usize> = table_meta
            .cluster_key
            .iter()
            .filter_map(|k| schema.index_of(k))
            .collect();
        if !key_indices.is_empty() {
            let inserter = SortKeyInserter::new(key_indices, vec![false]);
            if !inserter.is_sorted(&batch) {
                batch = sort_batch_by_key(&batch, &inserter)?;
            }
        }
    }

    let row_count = batch.num_rows() as u64;

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

    if bulk_import_mode {
        engine
            .exec_bulk_insert_partitioned_with_wal_control(db, table, table_meta, batch, false)?;
    } else {
        write_batch_by_partition(engine, db, table, table_meta, batch, schema, txn_id)?;
    }

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
        engine
            .batch_writer()
            .write_columns_with_txn(db, table, "", batch, schema, txn_id)?;
        return Ok(());
    }

    if let Some(partition_key) = single_partition_key(table_meta, &batch) {
        engine.batch_writer().write_columns_with_txn(
            db,
            table,
            &partition_key,
            batch,
            schema,
            txn_id,
        )?;
        return Ok(());
    }

    let groups = partition_masks(table_meta, &batch);
    if groups.len() <= 1 {
        let partition_key = groups
            .first()
            .map(|(key, _)| key.clone())
            .unwrap_or_default();
        engine.batch_writer().write_columns_with_txn(
            db,
            table,
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
        engine.batch_writer().write_columns_with_txn(
            db,
            table,
            &partition_key,
            part_batch,
            schema.clone(),
            txn_id,
        )?;
    }
    Ok(())
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
