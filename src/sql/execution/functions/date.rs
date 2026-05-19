/// Date/Time SQL functions using chrono crate.
use chrono::{Datelike, Timelike, Local, NaiveDate, NaiveDateTime, NaiveTime};

use crate::common::error::Result;
use crate::types::scalar::ScalarValue;

use super::registry::{args_to_i64, args_to_string};

pub fn now(_args: &[ScalarValue]) -> Result<ScalarValue> {
    let ts = Local::now().timestamp_micros();
    Ok(ScalarValue::Timestamp(Some(ts)))
}

pub fn current_date(_args: &[ScalarValue]) -> Result<ScalarValue> {
    let today = Local::now().date_naive();
    // Store as days since epoch
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let days = (today - epoch).num_days() as i32;
    Ok(ScalarValue::Date(Some(days)))
}

pub fn current_time(_args: &[ScalarValue]) -> Result<ScalarValue> {
    let now = Local::now().time();
    let micros = (now.hour() as i64 * 3600 + now.minute() as i64 * 60 + now.second() as i64) * 1_000_000
        + now.nanosecond() as i64 / 1000;
    Ok(ScalarValue::Time(Some(micros)))
}

pub fn current_timestamp(_args: &[ScalarValue]) -> Result<ScalarValue> {
    let ts = Local::now().timestamp_micros();
    Ok(ScalarValue::Timestamp(Some(ts)))
}

pub fn year(args: &[ScalarValue]) -> Result<ScalarValue> {
    match extract_date(args) {
        Some(dt) => Ok(ScalarValue::Int64(Some(dt.year() as i64))),
        None => Ok(ScalarValue::Int64(None)),
    }
}

pub fn month(args: &[ScalarValue]) -> Result<ScalarValue> {
    match extract_date(args) {
        Some(dt) => Ok(ScalarValue::Int64(Some(dt.month() as i64))),
        None => Ok(ScalarValue::Int64(None)),
    }
}

pub fn day(args: &[ScalarValue]) -> Result<ScalarValue> {
    match extract_date(args) {
        Some(dt) => Ok(ScalarValue::Int64(Some(dt.day() as i64))),
        None => Ok(ScalarValue::Int64(None)),
    }
}

pub fn hour(args: &[ScalarValue]) -> Result<ScalarValue> {
    match extract_datetime(args) {
        Some(dt) => Ok(ScalarValue::Int64(Some(dt.hour() as i64))),
        None => Ok(ScalarValue::Int64(None)),
    }
}

pub fn minute(args: &[ScalarValue]) -> Result<ScalarValue> {
    match extract_datetime(args) {
        Some(dt) => Ok(ScalarValue::Int64(Some(dt.minute() as i64))),
        None => Ok(ScalarValue::Int64(None)),
    }
}

pub fn second(args: &[ScalarValue]) -> Result<ScalarValue> {
    match extract_datetime(args) {
        Some(dt) => Ok(ScalarValue::Int64(Some(dt.second() as i64))),
        None => Ok(ScalarValue::Int64(None)),
    }
}

pub fn dayofweek(args: &[ScalarValue]) -> Result<ScalarValue> {
    match extract_date(args) {
        Some(dt) => Ok(ScalarValue::Int64(Some(dt.weekday().num_days_from_sunday() as i64 + 1))),
        None => Ok(ScalarValue::Int64(None)),
    }
}

pub fn dayofyear(args: &[ScalarValue]) -> Result<ScalarValue> {
    match extract_date(args) {
        Some(dt) => Ok(ScalarValue::Int64(Some(dt.ordinal() as i64))),
        None => Ok(ScalarValue::Int64(None)),
    }
}

pub fn week(args: &[ScalarValue]) -> Result<ScalarValue> {
    match extract_date(args) {
        Some(dt) => Ok(ScalarValue::Int64(Some(dt.iso_week().week() as i64))),
        None => Ok(ScalarValue::Int64(None)),
    }
}

pub fn quarter(args: &[ScalarValue]) -> Result<ScalarValue> {
    match extract_date(args) {
        Some(dt) => Ok(ScalarValue::Int64(Some(((dt.month() - 1) / 3 + 1) as i64))),
        None => Ok(ScalarValue::Int64(None)),
    }
}

pub fn date_format(args: &[ScalarValue]) -> Result<ScalarValue> {
    let dt = extract_datetime(args);
    let fmt = args_to_string(args, 1);
    match (dt, fmt) {
        (Some(dt), Some(fmt)) => {
            // Convert SQL-style format to chrono format
            let chrono_fmt = sql_to_chrono_format(&fmt);
            Ok(ScalarValue::VarChar(Some(dt.format(&chrono_fmt).to_string())))
        }
        _ => Ok(ScalarValue::VarChar(None)),
    }
}

pub fn last_day(args: &[ScalarValue]) -> Result<ScalarValue> {
    match extract_date(args) {
        Some(dt) => {
            let (year, month) = (dt.year(), dt.month());
            if month == 12 {
                let last = NaiveDate::from_ymd_opt(year, 12, 31).unwrap();
                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                Ok(ScalarValue::Date(Some((last - epoch).num_days() as i32)))
            } else {
                let first_next = NaiveDate::from_ymd_opt(year, month + 1, 1).unwrap();
                let last = first_next.pred_opt().unwrap();
                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                Ok(ScalarValue::Date(Some((last - epoch).num_days() as i32)))
            }
        }
        None => Ok(ScalarValue::Date(None)),
    }
}

// --- Helpers ---

fn extract_date(args: &[ScalarValue]) -> Option<NaiveDate> {
    match args.first() {
        Some(ScalarValue::Date(Some(days))) => {
            NaiveDate::from_num_days_from_ce_opt(*days + 719163) // Adjust from Unix epoch
        }
        Some(ScalarValue::VarChar(Some(s))) => {
            NaiveDate::parse_from_str(s, "%Y-%m-%d").ok()
        }
        _ => None,
    }
}

fn extract_datetime(args: &[ScalarValue]) -> Option<NaiveDateTime> {
    match args.first() {
        Some(ScalarValue::Timestamp(Some(micros))) => {
            NaiveDateTime::from_timestamp_opt(*micros / 1_000_000, (*micros % 1_000_000) as u32 * 1000)
        }
        Some(ScalarValue::VarChar(Some(s))) => {
            // Try common formats
            NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").ok()
                .or_else(|| NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S").ok())
                .or_else(|| {
                    NaiveDate::parse_from_str(s, "%Y-%m-%d").ok()
                        .map(|d| d.and_hms_opt(0, 0, 0).unwrap())
                })
        }
        Some(ScalarValue::Date(Some(days))) => {
            extract_date(args).map(|d| d.and_hms_opt(0, 0, 0).unwrap())
        }
        _ => None,
    }
}

/// Convert SQL-style date format to chrono format.
fn sql_to_chrono_format(fmt: &str) -> String {
    fmt.replace("%Y", "%Y")
        .replace("%m", "%m")
        .replace("%d", "%d")
        .replace("%H", "%H")
        .replace("%i", "%M")
        .replace("%S", "%S")
        .replace("%s", "%S")
        .replace("%Y", "%Y")
}
