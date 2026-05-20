use std::fs::File;
use std::io::{BufRead, BufReader};

pub struct ColumnarTickBatch {
    pub security_ids: Vec<String>,
    pub security_id_indices: Vec<u32>,
    pub deal_dates: Vec<i32>,
    pub deal_times: Vec<i32>,
    pub pre_closes: Vec<i64>,
    pub open_pxs: Vec<i64>,
    pub high_pxs: Vec<i64>,
    pub low_pxs: Vec<i64>,
    pub close_pxs: Vec<i64>,
    pub high_limits: Vec<i64>,
    pub low_limits: Vec<i64>,
    pub total_num_trades: Vec<i32>,
    pub trans_volumes: Vec<i64>,
    pub trans_values: Vec<i64>,
    pub ask_px1s: Vec<i64>,
    pub ask_px2s: Vec<i64>,
    pub ask_px3s: Vec<i64>,
    pub ask_px4s: Vec<i64>,
    pub ask_px5s: Vec<i64>,
    pub ask_vol1s: Vec<i32>,
    pub ask_vol2s: Vec<i32>,
    pub ask_vol3s: Vec<i32>,
    pub ask_vol4s: Vec<i32>,
    pub ask_vol5s: Vec<i32>,
    pub bid_px1s: Vec<i64>,
    pub bid_px2s: Vec<i64>,
    pub bid_px3s: Vec<i64>,
    pub bid_px4s: Vec<i64>,
    pub bid_px5s: Vec<i64>,
    pub bid_vol1s: Vec<i32>,
    pub bid_vol2s: Vec<i32>,
    pub bid_vol3s: Vec<i32>,
    pub bid_vol4s: Vec<i32>,
    pub bid_vol5s: Vec<i32>,
    pub local_times: Vec<i64>,
}

impl ColumnarTickBatch {
    pub fn new(capacity: usize) -> Self {
        Self {
            security_ids: Vec::with_capacity(capacity),
            security_id_indices: Vec::with_capacity(capacity),
            deal_dates: Vec::with_capacity(capacity),
            deal_times: Vec::with_capacity(capacity),
            pre_closes: Vec::with_capacity(capacity),
            open_pxs: Vec::with_capacity(capacity),
            high_pxs: Vec::with_capacity(capacity),
            low_pxs: Vec::with_capacity(capacity),
            close_pxs: Vec::with_capacity(capacity),
            high_limits: Vec::with_capacity(capacity),
            low_limits: Vec::with_capacity(capacity),
            total_num_trades: Vec::with_capacity(capacity),
            trans_volumes: Vec::with_capacity(capacity),
            trans_values: Vec::with_capacity(capacity),
            ask_px1s: Vec::with_capacity(capacity),
            ask_px2s: Vec::with_capacity(capacity),
            ask_px3s: Vec::with_capacity(capacity),
            ask_px4s: Vec::with_capacity(capacity),
            ask_px5s: Vec::with_capacity(capacity),
            ask_vol1s: Vec::with_capacity(capacity),
            ask_vol2s: Vec::with_capacity(capacity),
            ask_vol3s: Vec::with_capacity(capacity),
            ask_vol4s: Vec::with_capacity(capacity),
            ask_vol5s: Vec::with_capacity(capacity),
            bid_px1s: Vec::with_capacity(capacity),
            bid_px2s: Vec::with_capacity(capacity),
            bid_px3s: Vec::with_capacity(capacity),
            bid_px4s: Vec::with_capacity(capacity),
            bid_px5s: Vec::with_capacity(capacity),
            bid_vol1s: Vec::with_capacity(capacity),
            bid_vol2s: Vec::with_capacity(capacity),
            bid_vol3s: Vec::with_capacity(capacity),
            bid_vol4s: Vec::with_capacity(capacity),
            bid_vol5s: Vec::with_capacity(capacity),
            local_times: Vec::with_capacity(capacity),
        }
    }

    pub fn len(&self) -> usize {
        self.security_id_indices.len()
    }

    pub fn is_empty(&self) -> bool {
        self.security_id_indices.is_empty()
    }

    pub fn reset(&mut self) {
        self.security_ids.clear();
        self.security_id_indices.clear();
        self.deal_dates.clear();
        self.deal_times.clear();
        self.pre_closes.clear();
        self.open_pxs.clear();
        self.high_pxs.clear();
        self.low_pxs.clear();
        self.close_pxs.clear();
        self.high_limits.clear();
        self.low_limits.clear();
        self.total_num_trades.clear();
        self.trans_volumes.clear();
        self.trans_values.clear();
        self.ask_px1s.clear();
        self.ask_px2s.clear();
        self.ask_px3s.clear();
        self.ask_px4s.clear();
        self.ask_px5s.clear();
        self.ask_vol1s.clear();
        self.ask_vol2s.clear();
        self.ask_vol3s.clear();
        self.ask_vol4s.clear();
        self.ask_vol5s.clear();
        self.bid_px1s.clear();
        self.bid_px2s.clear();
        self.bid_px3s.clear();
        self.bid_px4s.clear();
        self.bid_px5s.clear();
        self.bid_vol1s.clear();
        self.bid_vol2s.clear();
        self.bid_vol3s.clear();
        self.bid_vol4s.clear();
        self.bid_vol5s.clear();
        self.local_times.clear();
    }
}

pub fn parse_tick_csv(file_path: &str, batch_size: usize, skip_header: bool) -> std::io::Result<(Vec<ColumnarTickBatch>, usize)> {
    let file = File::open(file_path)?;
    let reader = BufReader::new(file);

    let mut batches = Vec::new();
    let mut current_batch = ColumnarTickBatch::new(batch_size);
    let mut total_rows = 0usize;
    let mut line_num = 0usize;

    for line in reader.lines() {
        let line = line?;
        line_num += 1;

        if skip_header && line_num == 1 {
            continue;
        }

        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        if parse_tick_csv_line_columnar(trimmed, &mut current_batch) {
            total_rows += 1;

            if current_batch.len() >= batch_size {
                batches.push(current_batch);
                current_batch = ColumnarTickBatch::new(batch_size);
            }
        }
    }

    if !current_batch.is_empty() {
        batches.push(current_batch);
    }

    Ok((batches, total_rows))
}

fn parse_tick_csv_line_columnar(line: &str, batch: &mut ColumnarTickBatch) -> bool {
    let args: Vec<&str> = line.split(',').collect();
    if args.len() < 13 {
        return false;
    }

    let deal_date = parse_int32_fast(args[1]);
    if deal_date == 0 {
        return false;
    }

    batch.security_ids.push(args[0].to_string());

    batch.deal_dates.push(deal_date);
    batch.deal_times.push(parse_int32_fast(args[2]));

    batch.pre_closes.push(parse_decimal_fast(args[3], 2));
    batch.open_pxs.push(parse_decimal_fast(args[4], 2));
    batch.high_pxs.push(parse_decimal_fast(args[5], 2));
    batch.low_pxs.push(parse_decimal_fast(args[6], 2));
    batch.close_pxs.push(parse_decimal_fast(args[7], 2));
    batch.high_limits.push(parse_decimal_fast(args[8], 2));
    batch.low_limits.push(parse_decimal_fast(args[9], 2));

    batch.total_num_trades.push(parse_int32_fast(args[10]));
    batch.trans_volumes.push(parse_int64_fast(args[11]));
    batch.trans_values.push(parse_decimal_fast(args[12], 2));

    if args.len() <= 13 {
        batch.ask_px1s.push(0);
        batch.ask_px2s.push(0);
        batch.ask_px3s.push(0);
        batch.ask_px4s.push(0);
        batch.ask_px5s.push(0);
        batch.ask_vol1s.push(0);
        batch.ask_vol2s.push(0);
        batch.ask_vol3s.push(0);
        batch.ask_vol4s.push(0);
        batch.ask_vol5s.push(0);
        batch.bid_px1s.push(0);
        batch.bid_px2s.push(0);
        batch.bid_px3s.push(0);
        batch.bid_px4s.push(0);
        batch.bid_px5s.push(0);
        batch.bid_vol1s.push(0);
        batch.bid_vol2s.push(0);
        batch.bid_vol3s.push(0);
        batch.bid_vol4s.push(0);
        batch.bid_vol5s.push(0);
        batch.local_times.push(0);
        return true;
    }

    batch.ask_px1s.push(parse_decimal_fast(args[13], 2));
    batch.ask_px2s.push(parse_decimal_fast(args[14], 2));
    batch.ask_px3s.push(parse_decimal_fast(args[15], 2));
    batch.ask_px4s.push(parse_decimal_fast(args[16], 2));
    batch.ask_px5s.push(parse_decimal_fast(args[17], 2));

    batch.ask_vol1s.push(parse_int32_fast(args[18]));
    batch.ask_vol2s.push(parse_int32_fast(args[19]));
    batch.ask_vol3s.push(parse_int32_fast(args[20]));
    batch.ask_vol4s.push(parse_int32_fast(args[21]));
    batch.ask_vol5s.push(parse_int32_fast(args[22]));

    batch.bid_px1s.push(parse_decimal_fast(args[23], 2));
    batch.bid_px2s.push(parse_decimal_fast(args[24], 2));
    batch.bid_px3s.push(parse_decimal_fast(args[25], 2));
    batch.bid_px4s.push(parse_decimal_fast(args[26], 2));
    batch.bid_px5s.push(parse_decimal_fast(args[27], 2));

    batch.bid_vol1s.push(parse_int32_fast(args[28]));
    batch.bid_vol2s.push(parse_int32_fast(args[29]));
    batch.bid_vol3s.push(parse_int32_fast(args[30]));
    batch.bid_vol4s.push(parse_int32_fast(args[31]));
    batch.bid_vol5s.push(parse_int32_fast(args[32]));

    let local_time = if args.len() > 33 && !args[33].is_empty() {
        parse_timestamp_fast(args[33])
    } else {
        0
    };
    batch.local_times.push(local_time);

    true
}

#[inline]
pub fn parse_decimal_fast(s: &str, scale: u32) -> i64 {
    parse_decimal_bytes(s.as_bytes(), scale)
}

#[inline]
pub fn parse_decimal_bytes(bytes: &[u8], scale: u32) -> i64 {
    if bytes.is_empty() {
        return 0;
    }

    let mut result: i64 = 0;
    let mut neg = false;
    let mut idx = 0;
    let mut decimals = 0u32;
    let mut has_decimal = false;

    if bytes[0] == b'-' {
        neg = true;
        idx = 1;
    } else if bytes[0] == b'+' {
        idx = 1;
    }

    while idx < bytes.len() {
        let b = bytes[idx];
        if b == b'.' {
            has_decimal = true;
            idx += 1;
            continue;
        }
        if b < b'0' || b > b'9' {
            break;
        }
        result = result * 10 + (b - b'0') as i64;
        if has_decimal {
            decimals += 1;
        }
        idx += 1;
    }

    while decimals < scale {
        result *= 10;
        decimals += 1;
    }

    if neg {
        -result
    } else {
        result
    }
}

#[inline]
pub fn parse_int32_fast(s: &str) -> i32 {
    parse_int32_bytes(s.as_bytes())
}

#[inline]
pub fn parse_int32_bytes(bytes: &[u8]) -> i32 {
    if bytes.is_empty() {
        return 0;
    }

    let mut result: i32 = 0;
    let mut neg = false;
    let mut idx = 0;

    if bytes[0] == b'-' {
        neg = true;
        idx = 1;
    } else if bytes[0] == b'+' {
        idx = 1;
    }

    while idx < bytes.len() {
        let b = bytes[idx];
        if b < b'0' || b > b'9' {
            break;
        }
        result = result.wrapping_mul(10).wrapping_add((b - b'0') as i32);
        idx += 1;
    }

    if neg {
        -result
    } else {
        result
    }
}

#[inline]
pub fn parse_int64_fast(s: &str) -> i64 {
    parse_int64_bytes(s.as_bytes())
}

#[inline]
pub fn parse_int64_bytes(bytes: &[u8]) -> i64 {
    if bytes.is_empty() {
        return 0;
    }

    let mut result: i64 = 0;
    let mut neg = false;
    let mut idx = 0;
    let mut has_decimal = false;
    let mut first_after_dot = 0u8;

    if bytes[0] == b'-' {
        neg = true;
        idx = 1;
    } else if bytes[0] == b'+' {
        idx = 1;
    }

    while idx < bytes.len() {
        let b = bytes[idx];
        if b == b'.' {
            has_decimal = true;
            idx += 1;
            continue;
        }
        if b < b'0' || b > b'9' {
            break;
        }
        if !has_decimal {
            result = result * 10 + (b - b'0') as i64;
        } else if first_after_dot == 0 {
            first_after_dot = b - b'0';
        }
        idx += 1;
    }

    if has_decimal && first_after_dot >= 5 {
        result += 1;
    }

    if neg {
        -result
    } else {
        result
    }
}

#[inline]
pub fn parse_timestamp_fast(s: &str) -> i64 {
    parse_timestamp_bytes(s.as_bytes())
}

#[inline]
pub fn parse_timestamp_bytes(bytes: &[u8]) -> i64 {
    if bytes.len() < 19 {
        return 0;
    }

    let year = parse_4digits(bytes, 0) as i32;
    let month = parse_2digits(bytes, 5) as i32;
    let day = parse_2digits(bytes, 8) as i32;
    let hour = parse_2digits(bytes, 11) as i32;
    let minute = parse_2digits(bytes, 14) as i32;
    let second = parse_2digits(bytes, 17) as i32;

    if year < 1970 || month < 1 || month > 12 || day < 1 || day > 31 {
        return 0;
    }

    let days_before_month: [i32; 12] = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334];
    let mut days = (year - 1970) as i64 * 365;
    days += ((year - 1969) / 4) as i64;
    days -= ((year - 1901) / 100) as i64;
    days += ((year - 1601) / 400) as i64;
    days += days_before_month[(month - 1) as usize] as i64;
    if month > 2 && is_leap(year) {
        days += 1;
    }
    days += (day - 1) as i64;

    let mut micros = days * 86400_000_000i64;
    micros += hour as i64 * 3600_000_000i64;
    micros += minute as i64 * 60_000_000i64;
    micros += second as i64 * 1_000_000i64;

    if bytes.len() > 19 && bytes[19] == b'.' {
        let mut frac: i64 = 0;
        let mut frac_digits = 0;
        let mut pos = 20;
        while pos < bytes.len() && frac_digits < 6 {
            let b = bytes[pos];
            if b < b'0' || b > b'9' {
                break;
            }
            frac = frac * 10 + (b - b'0') as i64;
            frac_digits += 1;
            pos += 1;
        }
        while frac_digits < 6 {
            frac *= 10;
            frac_digits += 1;
        }
        micros += frac;
    }

    micros
}

#[inline]
fn parse_4digits(bytes: &[u8], start: usize) -> u16 {
    if start + 4 > bytes.len() {
        return 0;
    }
    (bytes[start] - b'0') as u16 * 1000
        + (bytes[start + 1] - b'0') as u16 * 100
        + (bytes[start + 2] - b'0') as u16 * 10
        + (bytes[start + 3] - b'0') as u16
}

#[inline]
fn parse_2digits(bytes: &[u8], start: usize) -> u8 {
    if start + 2 > bytes.len() {
        return 0;
    }
    (bytes[start] - b'0') * 10 + (bytes[start + 1] - b'0')
}

#[inline]
fn is_leap(y: i32) -> bool {
    (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0)
}
