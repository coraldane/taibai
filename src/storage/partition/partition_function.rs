use crate::types::scalar::ScalarValue;

#[derive(Debug, PartialEq)]
pub enum PartitionFunction {
    Day,
    Year,
    Month,
    Value,
    Hash(i32),
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
            PartitionFunction::Hash(buckets) => {
                if *buckets > 0 {
                    (value.abs() % (*buckets as i64)) as i64
                } else {
                    value
                }
            }
        }
    }

    pub fn apply_str(&self, s: &str) -> i64 {
        match self {
            PartitionFunction::Hash(buckets) => {
                if *buckets > 0 {
                    let hash = compute_string_hash(s);
                    hash % (*buckets as i64)
                } else {
                    0
                }
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
        PartitionFunction::Value => val.to_text().unwrap_or_default(),
        PartitionFunction::Day | PartitionFunction::Month | PartitionFunction::Year => {
            temporal_partition_key(func, val).unwrap_or_default()
        }
    }
}

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
        ScalarValue::UInt64(Some(v)) => {
            if num_buckets > 0 {
                (*v % num_buckets as u64) as i64
            } else {
                0
            }
        }
        ScalarValue::UInt32(Some(v)) => {
            if num_buckets > 0 {
                (*v % num_buckets as u32) as i64
            } else {
                0
            }
        }
        ScalarValue::VarChar(Some(s)) => func.apply_str(s),
        _ => 0,
    }
}

fn date_parts_from_scalar(val: &ScalarValue) -> Option<DateParts> {
    match val {
        ScalarValue::Timestamp(Some(v)) => Some(date_parts_from_epoch_micros(*v)),
        ScalarValue::Date(Some(days)) => Some(date_parts_from_epoch_days(*days as i64)),
        ScalarValue::Int32(Some(v)) => date_parts_from_date_number(*v as i64).or_else(|| {
            Some(date_parts_from_epoch_micros(normalize_epoch_to_micros(
                *v as i64,
            )))
        }),
        ScalarValue::Int64(Some(v)) => date_parts_from_date_number(*v)
            .or_else(|| Some(date_parts_from_epoch_micros(normalize_epoch_to_micros(*v)))),
        ScalarValue::UInt32(Some(v)) => date_parts_from_date_number(*v as i64).or_else(|| {
            Some(date_parts_from_epoch_micros(normalize_epoch_to_micros(
                *v as i64,
            )))
        }),
        ScalarValue::UInt64(Some(v)) if *v <= i64::MAX as u64 => {
            let v = *v as i64;
            date_parts_from_date_number(v)
                .or_else(|| Some(date_parts_from_epoch_micros(normalize_epoch_to_micros(v))))
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
    if (10_001_01..=99_991_231).contains(&abs) {
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

fn normalize_epoch_to_micros(v: i64) -> i64 {
    let abs = v.abs();
    if abs < 10_000_000_000 {
        v.saturating_mul(1_000_000)
    } else if abs < 10_000_000_000_000 {
        v.saturating_mul(1_000)
    } else {
        v
    }
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
