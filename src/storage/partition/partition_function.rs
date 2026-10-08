use crate::types::array::ColumnArray;
use crate::types::scalar::ScalarValue;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PartitionFunction {
    Day,
    Year,
    Month,
    Value,
    Hash(i32),
    Mod(i32),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DateParts {
    year: i32,
    month: u32,
    day: u32,
}

impl PartitionFunction {
    pub fn apply(&self, value: i64) -> i64 {
        match self {
            PartitionFunction::Day => value / 86400,
            PartitionFunction::Year => value / (86400 * 365),
            PartitionFunction::Month => value / (86400 * 30),
            PartitionFunction::Value => value,
            PartitionFunction::Mod(divisor) => {
                if *divisor > 0 {
                    value.rem_euclid(*divisor as i64)
                } else {
                    value
                }
            }
            PartitionFunction::Hash(buckets) => {
                if *buckets > 0 {
                    value.abs() % (*buckets as i64)
                } else {
                    value
                }
            }
        }
    }

    pub fn apply_str(&self, s: &str) -> i64 {
        match self {
            PartitionFunction::Hash(buckets) if *buckets > 0 => {
                let hash = compute_string_hash(s);
                hash % (*buckets as i64)
            }
            _ => 0,
        }
    }
}

fn compute_string_hash(s: &str) -> i64 {
    let mut hash: i64 = 0;
    for c in s.chars() {
        hash = hash.wrapping_mul(31).wrapping_add(c as i64);
    }
    hash.abs()
}

pub fn compute_partition_key_from_scalar(func: &PartitionFunction, val: &ScalarValue) -> String {
    match func {
        PartitionFunction::Hash(_) => hash_partition_value(
            val,
            match func {
                PartitionFunction::Hash(buckets) => *buckets as i64,
                _ => 0,
            },
        )
        .to_string(),
        PartitionFunction::Mod(divisor) => mod_partition_value(val, *divisor as i64).to_string(),
        PartitionFunction::Value => val.to_text().unwrap_or_default(),
        PartitionFunction::Day | PartitionFunction::Month | PartitionFunction::Year => {
            temporal_partition_key(func, val).unwrap_or_default()
        }
    }
}

/// The temporal partition key of a scalar value, or `None` when the value
/// carries no calendar meaning (NULL, a non-calendar integer — see
/// `date_parts_from_scalar`, a non-date string). Range-pruning callers
/// treat `None` as "no bound on this side" so an unparseable literal can
/// never eliminate partitions; write routing and eq pruning route such rows
/// to the unpartitioned bucket.
pub fn temporal_partition_key(func: &PartitionFunction, val: &ScalarValue) -> Option<String> {
    let parts = date_parts_from_scalar(val)?;
    match func {
        PartitionFunction::Day => Some(format!(
            "{:04}{:02}{:02}",
            parts.year, parts.month, parts.day
        )),
        PartitionFunction::Month => Some(format!("{:04}{:02}", parts.year, parts.month)),
        PartitionFunction::Year => Some(format!("{:04}", parts.year)),
        _ => None,
    }
}

pub fn hash_partition_value(val: &ScalarValue, num_buckets: i64) -> i64 {
    let func = PartitionFunction::Hash(num_buckets as i32);
    match val {
        ScalarValue::Int64(Some(v)) => func.apply(*v),
        ScalarValue::Int32(Some(v)) => func.apply(*v as i64),
        ScalarValue::Timestamp(Some(v)) => func.apply(*v),
        ScalarValue::Date(Some(v)) => func.apply(*v as i64),
        ScalarValue::Time(Some(v)) => func.apply(*v),
        ScalarValue::UInt64(Some(v)) if num_buckets > 0 => (*v % num_buckets as u64) as i64,
        ScalarValue::UInt32(Some(v)) if num_buckets > 0 => (*v % num_buckets as u32) as i64,
        ScalarValue::VarChar(Some(s)) => func.apply_str(s),
        _ => 0,
    }
}

pub fn mod_partition_value(val: &ScalarValue, divisor: i64) -> i64 {
    if divisor <= 0 {
        return 0;
    }
    match val {
        ScalarValue::Int64(Some(v)) => v.rem_euclid(divisor),
        ScalarValue::Int32(Some(v)) => (*v as i64).rem_euclid(divisor),
        ScalarValue::Timestamp(Some(v)) => v.rem_euclid(divisor),
        ScalarValue::Date(Some(v)) => (*v as i64).rem_euclid(divisor),
        ScalarValue::Time(Some(v)) => v.rem_euclid(divisor),
        ScalarValue::UInt64(Some(v)) => (*v % divisor as u64) as i64,
        ScalarValue::UInt32(Some(v)) => (*v % divisor as u32) as i64,
        ScalarValue::VarChar(Some(s)) => s
            .trim()
            .parse::<i64>()
            .map(|v| v.rem_euclid(divisor))
            .unwrap_or(0),
        _ => 0,
    }
}

/// One value, one interpretation, over disjoint magnitude domains: a valid
/// calendar number (`YYYYMMDD` / `YYYYMM`) reads as a calendar date; a bare
/// integer in the epoch windows (see [`date_parts_from_epoch_number`])
/// reads as an epoch timestamp; everything else yields `None`. Every
/// caller (write routing, eq and range pruning, DELETE/UPDATE partition
/// narrowing) shares this one function, so a `None` row lands in — and is
/// read from — the unpartitioned bucket exactly like NULL. The old
/// unconditional seconds fallback used to reinterpret every non-calendar
/// integer (an invalid date number like `20240132`, a wall-clock `930`) as
/// Unix seconds, silently collapsing whole date ranges into
/// `p19700823`-style partitions that the TTL purger then treated as
/// half-century-old data and dropped.
fn date_parts_from_scalar(val: &ScalarValue) -> Option<DateParts> {
    match val {
        ScalarValue::Timestamp(Some(v)) => Some(date_parts_from_epoch_micros(*v)),
        ScalarValue::Date(Some(days)) => Some(date_parts_from_epoch_days(*days as i64)),
        ScalarValue::Int32(Some(v)) => date_parts_from_date_number(*v as i64)
            .or_else(|| date_parts_from_epoch_number(*v as i64)),
        ScalarValue::Int64(Some(v)) => {
            date_parts_from_date_number(*v).or_else(|| date_parts_from_epoch_number(*v))
        }
        ScalarValue::UInt32(Some(v)) => date_parts_from_date_number(*v as i64)
            .or_else(|| date_parts_from_epoch_number(*v as i64)),
        ScalarValue::UInt64(Some(v)) if *v <= i64::MAX as u64 => {
            let v = *v as i64;
            date_parts_from_date_number(v).or_else(|| date_parts_from_epoch_number(v))
        }
        ScalarValue::VarChar(Some(s)) => date_parts_from_str(s),
        _ => None,
    }
}

fn date_parts_from_str(s: &str) -> Option<DateParts> {
    let trimmed = s.trim();
    if trimmed.len() >= 10 && trimmed.as_bytes().get(4) == Some(&b'-') {
        let year = trimmed.get(0..4)?.parse::<i32>().ok()?;
        let month = trimmed.get(5..7)?.parse::<u32>().ok()?;
        let day = trimmed.get(8..10)?.parse::<u32>().ok()?;
        return valid_date_parts(year, month, day);
    }
    trimmed
        .parse::<i64>()
        .ok()
        .and_then(date_parts_from_date_number)
}

fn date_parts_from_date_number(v: i64) -> Option<DateParts> {
    let abs = v.abs();
    if (1_000_101..=99_991_231).contains(&abs) {
        let year = (abs / 10_000) as i32;
        let month = ((abs / 100) % 100) as u32;
        let day = (abs % 100) as u32;
        return valid_date_parts(year, month, day);
    }
    if (100_001..=999_912).contains(&abs) {
        let year = (abs / 100) as i32;
        let month = (abs % 100) as u32;
        return valid_date_parts(year, month, 1);
    }
    None
}

fn valid_date_parts(year: i32, month: u32, day: u32) -> Option<DateParts> {
    if year <= 0 || !(1..=12).contains(&month) {
        return None;
    }
    let max_day = days_in_month(year, month);
    if day == 0 || day > max_day {
        return None;
    }
    Some(DateParts { year, month, day })
}

fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => 0,
    }
}

fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

/// Epoch interpretation of a bare integer, restricted to magnitude windows
/// that cannot collide with calendar numbers (`YYYYMMDD` tops out at
/// 99_991_231, `YYYYMM` at 999_912): seconds `[10^9, 10^10)`, millis
/// `[10^10, 10^13)`, micros `>= 10^13`. Any smaller non-calendar integer
/// (a wall-clock value like `930`, an invalid date number like `20240132`)
/// has no temporal meaning and returns `None` — such rows partition into
/// the unpartitioned bucket exactly like NULL, instead of silently
/// collapsing into `p1970xxxx` partitions that the TTL purger would then
/// treat as half-century-old data and drop.
fn date_parts_from_epoch_number(v: i64) -> Option<DateParts> {
    let abs = v.abs();
    let micros = if (1_000_000_000..10_000_000_000).contains(&abs) {
        v.saturating_mul(1_000_000)
    } else if (10_000_000_000..10_000_000_000_000).contains(&abs) {
        v.saturating_mul(1_000)
    } else if abs >= 10_000_000_000_000 {
        v
    } else {
        return None;
    };
    Some(date_parts_from_epoch_micros(micros))
}

fn date_parts_from_epoch_micros(micros: i64) -> DateParts {
    date_parts_from_epoch_days(micros.div_euclid(86_400_000_000))
}

fn date_parts_from_epoch_days(days: i64) -> DateParts {
    let z = days + 719468;
    let era = if z >= 0 {
        z / 146097
    } else {
        (z - 146096) / 146097
    };
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    DateParts {
        year: y as i32,
        month: m as u32,
        day: d as u32,
    }
}

/// Read the raw numeric value of a timestamp/date/time/integer column as
/// i64, for memoization. `None` for NULL rows and for non-numeric column
/// types (which never take the memo path). `UInt64` is excluded: wrapping
/// `as i64` could make two distinct values share a memo entry.
pub fn numeric_memo_value_at(column: &ColumnArray, row: usize) -> Option<i64> {
    if column.bitmap().is_null(row) {
        return None;
    }
    match column {
        ColumnArray::Timestamp { data, .. }
        | ColumnArray::Time { data, .. }
        | ColumnArray::Int64 { data, .. } => data.get(row).copied(),
        ColumnArray::Date { data, .. } | ColumnArray::Int32 { data, .. } => {
            data.get(row).map(|v| *v as i64)
        }
        ColumnArray::UInt32 { data, .. } => data.get(row).map(|v| *v as i64),
        _ => None,
    }
}

fn temporal_key_from_parts(func: &PartitionFunction, parts: Option<DateParts>) -> String {
    let Some(parts) = parts else {
        return String::new();
    };
    match func {
        PartitionFunction::Day => format!("{:04}{:02}{:02}", parts.year, parts.month, parts.day),
        PartitionFunction::Month => format!("{:04}{:02}", parts.year, parts.month),
        PartitionFunction::Year => format!("{:04}", parts.year),
        _ => String::new(),
    }
}

fn mod_key(v: i64, divisor: i32) -> String {
    if divisor > 0 {
        v.rem_euclid(divisor as i64).to_string()
    } else {
        "0".to_string()
    }
}

/// Compute the partition key directly from typed column storage, producing
/// exactly the same string as `compute_partition_key_from_scalar(func,
/// &column.scalar(row))` without constructing the intermediate ScalarValue
/// (a per-row enum plus a String clone inside `to_text`). Column types
/// without a typed arm fall back to the scalar function; the
/// `partition_key_at_matches_scalar` test pins the equivalence.
pub fn compute_partition_key_at(
    func: &PartitionFunction,
    column: &ColumnArray,
    row: usize,
) -> String {
    if column.bitmap().is_null(row) {
        // Mirrors the scalar `None` arms: Hash/Mod yield 0, temporal and
        // Value yield an empty key.
        return match func {
            PartitionFunction::Hash(_) | PartitionFunction::Mod(_) => "0".to_string(),
            _ => String::new(),
        };
    }
    match column {
        ColumnArray::Timestamp { data, .. } => {
            let v = data[row];
            match func {
                PartitionFunction::Day | PartitionFunction::Month | PartitionFunction::Year => {
                    temporal_key_from_parts(func, Some(date_parts_from_epoch_micros(v)))
                }
                PartitionFunction::Hash(_) => func.apply(v).to_string(),
                PartitionFunction::Mod(divisor) => mod_key(v, *divisor),
                PartitionFunction::Value => v.to_string(),
            }
        }
        ColumnArray::Date { data, .. } => {
            let v = data[row] as i64;
            match func {
                PartitionFunction::Day | PartitionFunction::Month | PartitionFunction::Year => {
                    temporal_key_from_parts(func, Some(date_parts_from_epoch_days(v)))
                }
                PartitionFunction::Hash(_) => func.apply(v).to_string(),
                PartitionFunction::Mod(divisor) => mod_key(v, *divisor),
                PartitionFunction::Value => v.to_string(),
            }
        }
        ColumnArray::Time { data, .. } => {
            let v = data[row];
            match func {
                PartitionFunction::Day | PartitionFunction::Month | PartitionFunction::Year => {
                    String::new()
                }
                PartitionFunction::Hash(_) => func.apply(v).to_string(),
                PartitionFunction::Mod(divisor) => mod_key(v, *divisor),
                PartitionFunction::Value => v.to_string(),
            }
        }
        ColumnArray::Int64 { data, .. } => {
            let v = data[row];
            match func {
                PartitionFunction::Day | PartitionFunction::Month | PartitionFunction::Year => {
                    temporal_key_from_parts(
                        func,
                        date_parts_from_date_number(v).or_else(|| date_parts_from_epoch_number(v)),
                    )
                }
                PartitionFunction::Hash(_) => func.apply(v).to_string(),
                PartitionFunction::Mod(divisor) => mod_key(v, *divisor),
                PartitionFunction::Value => v.to_string(),
            }
        }
        ColumnArray::Int32 { data, .. } => {
            let v = data[row] as i64;
            match func {
                PartitionFunction::Day | PartitionFunction::Month | PartitionFunction::Year => {
                    temporal_key_from_parts(
                        func,
                        date_parts_from_date_number(v).or_else(|| date_parts_from_epoch_number(v)),
                    )
                }
                PartitionFunction::Hash(_) => func.apply(v).to_string(),
                PartitionFunction::Mod(divisor) => mod_key(v, *divisor),
                PartitionFunction::Value => v.to_string(),
            }
        }
        ColumnArray::UInt32 { data, .. } => {
            let v = data[row];
            match func {
                PartitionFunction::Day | PartitionFunction::Month | PartitionFunction::Year => {
                    temporal_key_from_parts(
                        func,
                        date_parts_from_date_number(v as i64)
                            .or_else(|| date_parts_from_epoch_number(v as i64)),
                    )
                }
                PartitionFunction::Hash(buckets) => if *buckets > 0 {
                    (v % *buckets as u32) as i64
                } else {
                    0
                }
                .to_string(),
                PartitionFunction::Mod(divisor) => if *divisor > 0 {
                    (v % *divisor as u32) as i64
                } else {
                    0
                }
                .to_string(),
                PartitionFunction::Value => v.to_string(),
            }
        }
        ColumnArray::UInt64 { data, .. } => {
            let v = data[row];
            match func {
                PartitionFunction::Day | PartitionFunction::Month | PartitionFunction::Year => {
                    temporal_key_from_parts(
                        func,
                        (v <= i64::MAX as u64)
                            .then(|| date_parts_from_date_number(v as i64))
                            .flatten()
                            .or_else(|| {
                                (v <= i64::MAX as u64)
                                    .then(|| date_parts_from_epoch_number(v as i64))
                                    .flatten()
                            }),
                    )
                }
                PartitionFunction::Hash(buckets) => if *buckets > 0 {
                    (v % *buckets as u64) as i64
                } else {
                    0
                }
                .to_string(),
                PartitionFunction::Mod(divisor) => if *divisor > 0 {
                    (v % *divisor as u64) as i64
                } else {
                    0
                }
                .to_string(),
                PartitionFunction::Value => v.to_string(),
            }
        }
        ColumnArray::VarChar { data, .. } => {
            let value = data.value(row);
            match func {
                PartitionFunction::Day | PartitionFunction::Month | PartitionFunction::Year => {
                    temporal_key_from_parts(func, date_parts_from_str(value))
                }
                PartitionFunction::Hash(_) => func.apply_str(value).to_string(),
                PartitionFunction::Mod(divisor) => {
                    if *divisor > 0 {
                        value
                            .trim()
                            .parse::<i64>()
                            .map(|v| v.rem_euclid(*divisor as i64))
                            .unwrap_or(0)
                            .to_string()
                    } else {
                        "0".to_string()
                    }
                }
                PartitionFunction::Value => value.to_string(),
            }
        }
        _ => compute_partition_key_from_scalar(func, &column.scalar(row)),
    }
}

/// Mirror of `ScalarValue::to_text` for the value at `row` (NULL becomes an
/// empty string, matching `to_text().unwrap_or_default()`).
pub fn value_text_at(column: &ColumnArray, row: usize) -> String {
    if column.bitmap().is_null(row) {
        return String::new();
    }
    match column {
        ColumnArray::Selected { .. } | ColumnArray::Rle { .. } => {
            value_text_at(column.flattened(), row)
        }
        ColumnArray::Dictionary {
            ids, dict, bitmap, ..
        } => {
            if bitmap.is_null(row) {
                String::new()
            } else {
                dict.get(ids[row] as usize).unwrap_or("").to_string()
            }
        }
        ColumnArray::Bool { data, .. } => {
            if data[row] != 0 {
                "true".to_string()
            } else {
                "false".to_string()
            }
        }
        ColumnArray::Int8 { data, .. } => data[row].to_string(),
        ColumnArray::Int16 { data, .. } => data[row].to_string(),
        ColumnArray::Int32 { data, .. } | ColumnArray::Date { data, .. } => data[row].to_string(),
        ColumnArray::Int64 { data, .. }
        | ColumnArray::Decimal { data, .. }
        | ColumnArray::Time { data, .. }
        | ColumnArray::Timestamp { data, .. }
        | ColumnArray::Interval { data, .. } => data[row].to_string(),
        ColumnArray::UInt8 { data, .. } => data[row].to_string(),
        ColumnArray::UInt16 { data, .. } => data[row].to_string(),
        ColumnArray::UInt32 { data, .. } => data[row].to_string(),
        ColumnArray::UInt64 { data, .. } => data[row].to_string(),
        ColumnArray::Float32 { data, .. } => data[row].to_string(),
        ColumnArray::Float64 { data, .. } => data[row].to_string(),
        ColumnArray::VarChar { data, .. } => data.value(row).to_string(),
        ColumnArray::Blob { data, .. } => format!("{:?}", &data[row]),
        ColumnArray::Vector { data, .. } => format!("{:?}", &data[row]),
    }
}

/// Which key derivation [`PartitionKeysAt`] applies.
#[derive(Debug, Clone, Copy)]
pub enum PartitionKeyKind {
    /// A full `PartitionFunction` (Day/Year/Month/Hash/Mod/Value).
    Strategy(PartitionFunction),
    /// `PartitionType::Value`: the display text of the raw value.
    ValueText,
}

/// Memoized partition-key computation over one column. Runs of rows sharing
/// the same raw partition value (the common time-series case) reuse the last
/// key instead of formatting a new `String` per row.
pub struct PartitionKeysAt<'a> {
    kind: PartitionKeyKind,
    column: &'a ColumnArray,
    memo_eligible: bool,
    primed: bool,
    cache_raw: Option<i64>,
    cache_key: String,
    /// Holds keys for rows that cannot refresh the memo (NULL rows), so the
    /// memo-hit `cache_key` never gets overwritten with a foreign row's key.
    scratch_key: String,
}

impl<'a> PartitionKeysAt<'a> {
    pub fn new(kind: PartitionKeyKind, column: &'a ColumnArray) -> Self {
        let memo_eligible = matches!(
            column,
            ColumnArray::Timestamp { .. }
                | ColumnArray::Time { .. }
                | ColumnArray::Int64 { .. }
                | ColumnArray::Int32 { .. }
                | ColumnArray::Date { .. }
                | ColumnArray::UInt32 { .. }
        );
        Self {
            kind,
            column,
            memo_eligible,
            primed: false,
            cache_raw: None,
            cache_key: String::new(),
            scratch_key: String::new(),
        }
    }

    /// Key for `row`; the returned slice is valid until the next call.
    pub fn key_at(&mut self, row: usize) -> &str {
        if self.memo_eligible {
            let raw = numeric_memo_value_at(self.column, row);
            if self.primed && raw.is_some() && raw == self.cache_raw {
                return &self.cache_key;
            }
            if raw.is_some() {
                self.cache_raw = raw;
                self.primed = true;
                self.cache_key = self.compute_key(row);
                return &self.cache_key;
            }
            // NULL row: the memo stays primed for the previous raw value, so
            // the key must not land in `cache_key` (a later memo hit would
            // return this NULL row's key).
            self.scratch_key = self.compute_key(row);
            return &self.scratch_key;
        }
        self.cache_key = self.compute_key(row);
        &self.cache_key
    }

    fn compute_key(&self, row: usize) -> String {
        match self.kind {
            PartitionKeyKind::Strategy(func) => compute_partition_key_at(&func, self.column, row),
            PartitionKeyKind::ValueText => value_text_at(self.column, row),
        }
    }
}

#[cfg(test)]
#[path = "partition_function_test.rs"]
mod tests;
