fn insert_profile_enabled() -> bool {
    crate::common::config::insert_profile_enabled()
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

/// Test seam: force INSERTs down the standard parse/materialize path so
/// differential tests can compare both routes' outcomes on the same SQL.
/// Production value stays false.
static FAST_INSERT_DISABLED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

pub fn set_fast_insert_disabled_for_tests(disabled: bool) {
    FAST_INSERT_DISABLED.store(disabled, std::sync::atomic::Ordering::Release);
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
    if FAST_INSERT_DISABLED.load(std::sync::atomic::Ordering::Acquire) {
        return None;
    }
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
            "access denied for user '{}' to {:?} on {:?} '{}.{}'",
            username,
            PermissionType::Insert,
            PermissionObjectType::Table,
            db,
            table
        ))));
    }

    let catalogs = engine.catalogs().read().unwrap();
    let table_meta = match catalogs
        .get(db)
        .and_then(|catalog| catalog.get_table_shared(table))
        .cloned()
    {
        Some(meta) => meta,
        None => {
            let missing = if catalogs.get(db).is_none() {
                format!("database '{}' does not exist", db)
            } else {
                format!("table '{}.{}' does not exist", db, table)
            };
            return FastInsertOutcome::Executed(Err(TaibaiError::Sql(missing)));
        }
    };
    drop(catalogs);

    // Arc-shared metadata: the standard path uses `get_table_shared`; cloning
    // the whole TableMeta per statement showed up on high-frequency
    // single-row INSERTs. One schema build remains (Schema owns its columns).
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

    // First pass — supportability check only (C20). parse_fast_value and
    // the separator grammar below are pure functions of the SQL bytes (they
    // only advance a cursor over borrowed slices), so this walk materializes
    // no rows and consumes no auto-increment values. Any row the fast path
    // cannot fully parse bails out to the standard parser BEFORE the first
    // row is materialized, so a mid-batch fallback can no longer strand
    // auto-increment values the fast path already consumed.
    {
        let mut check_pos = pos;
        loop {
            skip_spaces(bytes, &mut check_pos);
            if check_pos >= bytes.len() {
                break;
            }
            if bytes[check_pos] != b'(' {
                return FastInsertOutcome::Unsupported;
            }
            check_pos += 1;
            for value_idx in 0..col_indices.len() {
                skip_spaces(bytes, &mut check_pos);
                if parse_fast_value(sql, bytes, &mut check_pos).is_none() {
                    return FastInsertOutcome::Unsupported;
                }
                skip_spaces(bytes, &mut check_pos);
                if value_idx + 1 < col_indices.len() {
                    if check_pos >= bytes.len() || bytes[check_pos] != b',' {
                        return FastInsertOutcome::Unsupported;
                    }
                    check_pos += 1;
                }
            }
            skip_spaces(bytes, &mut check_pos);
            if check_pos >= bytes.len() || bytes[check_pos] != b')' {
                return FastInsertOutcome::Unsupported;
            }
            check_pos += 1;
            skip_spaces(bytes, &mut check_pos);
            if check_pos >= bytes.len() {
                break;
            }
            if bytes[check_pos] == b',' {
                check_pos += 1;
                continue;
            }
            return FastInsertOutcome::Unsupported;
        }
    }

    // Second pass — materialization. The first pass proved the whole VALUES
    // list parses, so this loop can no longer hit an Unsupported condition
    // partway through; coercion errors surface here exactly as before.
    let capacity = estimate_values_rows(bytes, pos).clamp(1, 100_000);
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
                if let Err(e) = push_fast_insert_provided_value(
                    &mut batch,
                    &schema,
                    col_idx,
                    value,
                    timezone_offset_hours,
                ) {
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
                provided_by_col,
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
        true,
        false,
        missing_auto_inc,
        true,
        true,
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
        if *pos >= bytes.len() {
            return None;
        }
        // Quoted identifier ("col" or `col`): the real lexer strips the
        // quotes, so the fast path must too — otherwise the plan resolves
        // against a name that still carries them.
        let quote = bytes[*pos];
        if quote == b'"' || quote == b'`' {
            *pos += 1;
            let mut name = String::new();
            let mut seg_start = *pos;
            loop {
                let Some(&c) = bytes.get(*pos) else {
                    return None;
                };
                if c == quote {
                    if bytes.get(*pos + 1) == Some(&quote) {
                        name.push_str(&sql[seg_start..*pos]);
                        name.push(quote as char);
                        *pos += 2;
                        seg_start = *pos;
                    } else {
                        name.push_str(&sql[seg_start..*pos]);
                        *pos += 1;
                        break;
                    }
                } else {
                    *pos += 1;
                }
            }
            columns.push(name);
        } else {
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
        }
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
    if cache.len() >= 1024 {
        // Evict a few oldest entries instead of clearing the whole table:
        // clear-all threw every hot plan out at once, collapsing the hit
        // rate whenever 1024 cold keys had cycled through. Vec insertion
        // order = HashMap iteration is unspecified, but evicting arbitrary
        // entries still keeps the working set warm, which clear() did not.
        let victims: Vec<String> = cache.keys().take(64).cloned().collect();
        for key in victims {
            cache.remove(&key);
        }
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
    } else if bare_token_is_literal(text) {
        Some(FastValue::Bare(text))
    } else {
        // Bare identifiers (`VALUES (abc)`) are not literals; hand the
        // statement back to the standard parser, which reports them as
        // unknown columns instead of silently storing "abc" as a string.
        None
    }
}

/// A bare (unquoted) token the fast path may treat as a literal: numbers and
/// boolean words. Anything else is an identifier.
#[inline]
fn bare_token_is_literal(text: &str) -> bool {
    if text.eq_ignore_ascii_case("true") || text.eq_ignore_ascii_case("false") {
        return true;
    }
    let bytes = text.as_bytes();
    let digits = match bytes.first() {
        Some(b'+' | b'-') => &bytes[1..],
        Some(b'0'..=b'9') => bytes,
        _ => return false,
    };
    !digits.is_empty()
        && digits
            .iter()
            .all(|b| matches!(b, b'0'..=b'9' | b'.' | b'e' | b'E' | b'+' | b'-'))
}

#[inline]
fn push_fast_insert_provided_value(
    batch: &mut RecordBatch,
    schema: &Schema,
    col_idx: usize,
    value: FastValue<'_>,
    timezone_offset_hours: i32,
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
        push_fast_value_direct(
            batch.column_mut(col_idx).unwrap(),
            value,
            col.scale,
            timezone_offset_hours,
        )?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
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
                push_fast_value_direct(
                    batch.column_mut(col_idx).unwrap(),
                    value,
                    col.scale,
                    timezone_offset_hours,
                )?;
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
fn push_fast_value_direct(
    col: &mut ColumnArray,
    value: FastValue<'_>,
    scale: i32,
    timezone_offset_hours: i32,
) -> Result<()> {
    col.flatten_in_place();
    match col {
        ColumnArray::Selected { .. } | ColumnArray::Dictionary { .. } | ColumnArray::Rle { .. } => {
            unreachable!("flattened above")
        }
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
            let v = match parse_i64_fast(value) {
                Ok(v) => v,
                Err(int_err) => match parse_date_string_fast(value, timezone_offset_hours) {
                    Some(days) => days as i64,
                    None => return Err(int_err),
                },
            };
            data.push(i32::try_from(v).map_err(|_| overflow_error(v, "DATE"))?);
            bitmap.push_valid();
        }
        ColumnArray::Time { data, bitmap } => {
            data.push(parse_i64_fast(value)?);
            bitmap.push_valid();
        }
        ColumnArray::Timestamp { data, bitmap } => {
            let v = match parse_i64_fast(value) {
                Ok(v) => v,
                Err(int_err) => match parse_timestamp_string_fast(value, timezone_offset_hours) {
                    Some(ts) => ts,
                    None => return Err(int_err),
                },
            };
            data.push(v);
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

#[allow(clippy::too_many_arguments)]
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

#[allow(clippy::too_many_arguments)]
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
    let scalar = materialize_missing_insert_scalar(
        engine,
        db,
        table,
        auto_inc_counter,
        schema,
        col_idx,
        timezone_offset_hours,
    )?;
    col.push(&scalar)
}

fn materialize_missing_insert_scalar(
    engine: &Engine,
    db: &str,
    table: &str,
    auto_inc_counter: &Option<Arc<AutoIncrementCounter>>,
    schema: &Schema,
    col_idx: usize,
    timezone_offset_hours: i32,
) -> Result<ScalarValue> {
    let scalar = if schema.columns[col_idx].auto_increment {
        let next_val = match auto_inc_counter {
            Some(counter) => counter.next_value(),
            None => engine.auto_increment_manager().next_value(db, table)?,
        };
        materialize_auto_increment_value(schema, col_idx, next_val)?
    } else if let Some(default) = &schema.columns[col_idx].default_value {
        materialize_default_value(default, schema, col_idx, timezone_offset_hours)?
    } else {
        ScalarValue::null_for_type(schema.columns[col_idx].data_type)
    };
    validate_nullable(schema, col_idx, &scalar)?;
    Ok(scalar)
}

fn materialize_auto_increment_value(
    schema: &Schema,
    col_idx: usize,
    next_val: u64,
) -> Result<ScalarValue> {
    let overflow = || {
        TaibaiError::Sql(format!(
            "auto-increment value {} overflows column type",
            next_val
        ))
    };
    Ok(match schema.columns[col_idx].data_type {
        DataType::Int32 => ScalarValue::Int32(Some(i32::try_from(next_val).map_err(|_| overflow())?)),
        DataType::Int64 => ScalarValue::Int64(Some(i64::try_from(next_val).map_err(|_| overflow())?)),
        DataType::UInt32 => ScalarValue::UInt32(Some(u32::try_from(next_val).map_err(|_| overflow())?)),
        DataType::UInt64 => ScalarValue::UInt64(Some(next_val)),
        _ => ScalarValue::Int64(Some(i64::try_from(next_val).map_err(|_| overflow())?)),
    })
}

#[inline]
fn parse_text_fast(value: FastValue<'_>) -> Result<&str> {
    match value {
        FastValue::String(v) | FastValue::Bare(v) => Ok(v),
        FastValue::Null => Err(TaibaiError::Sql("unexpected NULL".into())),
    }
}

/// `'2024-01-01 10:00:00'`-style TIMESTAMP literals, mirroring the standard
/// path's `parse_timestamp_smart`. Integer literals keep the raw fast-path
/// passthrough.
fn parse_timestamp_string_fast(value: FastValue<'_>, timezone_offset_hours: i32) -> Option<i64> {
    let text = parse_text_fast(value).ok()?;
    crate::sql::execution::functions::date::parse_timestamp_smart(text, timezone_offset_hours)
}

/// `'2024-01-01'`-style DATE literals as days since epoch, mirroring the
/// standard path's conversion (single conversion point in
/// `functions::date::datetime_string_to_days`).
fn parse_date_string_fast(value: FastValue<'_>, timezone_offset_hours: i32) -> Option<i32> {
    let text = parse_text_fast(value).ok()?;
    crate::sql::execution::functions::date::datetime_string_to_days(text, timezone_offset_hours)
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
        if !b.is_ascii_digit() {
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
        if !b.is_ascii_digit() {
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
        if !b.is_ascii_digit() {
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
        if !b.is_ascii_digit() {
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
        if b.is_ascii_digit() {
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
                if !r.is_ascii_digit() {
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
