pub const TICK_CREATE_TABLE_SQL: &str = "CREATE TABLE IF NOT EXISTS {table} ( \
    security_id VARCHAR, \
    deal_date INT32, \
    deal_time INT32, \
    pre_close DECIMAL(9,2), \
    open_px DECIMAL(9,2), \
    high_px DECIMAL(9,2), \
    low_px DECIMAL(9,2), \
    close_px DECIMAL(9,2), \
    high_limit DECIMAL(9,2), \
    low_limit DECIMAL(9,2), \
    total_num_trade INT32, \
    trans_volume INT64, \
    trans_value DECIMAL(18,2), \
    ask_px1 DECIMAL(9,2), \
    ask_px2 DECIMAL(9,2), \
    ask_px3 DECIMAL(9,2), \
    ask_px4 DECIMAL(9,2), \
    ask_px5 DECIMAL(9,2), \
    ask_vol1 INT32, \
    ask_vol2 INT32, \
    ask_vol3 INT32, \
    ask_vol4 INT32, \
    ask_vol5 INT32, \
    bid_px1 DECIMAL(9,2), \
    bid_px2 DECIMAL(9,2), \
    bid_px3 DECIMAL(9,2), \
    bid_px4 DECIMAL(9,2), \
    bid_px5 DECIMAL(9,2), \
    bid_vol1 INT32, \
    bid_vol2 INT32, \
    bid_vol3 INT32, \
    bid_vol4 INT32, \
    bid_vol5 INT32, \
    local_time TIMESTAMP \
)";

pub struct TickElement {
    pub security_id: String,
    pub deal_date: i32,
    pub deal_time: i32,
    pub pre_close: i64,
    pub open_px: i64,
    pub high_px: i64,
    pub low_px: i64,
    pub close_px: i64,
    pub high_limit: i64,
    pub low_limit: i64,
    pub total_num_trade: i32,
    pub trans_volume: i64,
    pub trans_value: i64,
    pub ask_px1: i64,
    pub ask_px2: i64,
    pub ask_px3: i64,
    pub ask_px4: i64,
    pub ask_px5: i64,
    pub ask_vol1: i32,
    pub ask_vol2: i32,
    pub ask_vol3: i32,
    pub ask_vol4: i32,
    pub ask_vol5: i32,
    pub bid_px1: i64,
    pub bid_px2: i64,
    pub bid_px3: i64,
    pub bid_px4: i64,
    pub bid_px5: i64,
    pub bid_vol1: i32,
    pub bid_vol2: i32,
    pub bid_vol3: i32,
    pub bid_vol4: i32,
    pub bid_vol5: i32,
    pub local_time: i64,
}

pub fn parse_tick_csv_line(line: &str) -> Option<TickElement> {
    let args: Vec<&str> = line.split(',').collect();
    if args.len() < 13 {
        return None;
    }

    let deal_date = parse_int32(args[1]);
    if deal_date == 0 {
        return None;
    }

    let local_time = if args.len() > 33 && !args[33].trim().is_empty() {
        parse_timestamp(args[33].trim())
    } else {
        0
    };

    Some(TickElement {
        security_id: args[0].trim().to_string(),
        deal_date,
        deal_time: parse_int32(args[2]),
        pre_close: parse_decimal(args[3], 2),
        open_px: parse_decimal(args[4], 2),
        high_px: parse_decimal(args[5], 2),
        low_px: parse_decimal(args[6], 2),
        close_px: parse_decimal(args[7], 2),
        high_limit: parse_decimal(args[8], 2),
        low_limit: parse_decimal(args[9], 2),
        total_num_trade: parse_int32(args[10]),
        trans_volume: parse_int64(args[11]),
        trans_value: parse_decimal(args[12], 2),
        ask_px1: if args.len() > 13 { parse_decimal(args[13], 2) } else { 0 },
        ask_px2: if args.len() > 14 { parse_decimal(args[14], 2) } else { 0 },
        ask_px3: if args.len() > 15 { parse_decimal(args[15], 2) } else { 0 },
        ask_px4: if args.len() > 16 { parse_decimal(args[16], 2) } else { 0 },
        ask_px5: if args.len() > 17 { parse_decimal(args[17], 2) } else { 0 },
        ask_vol1: if args.len() > 18 { parse_int32(args[18]) } else { 0 },
        ask_vol2: if args.len() > 19 { parse_int32(args[19]) } else { 0 },
        ask_vol3: if args.len() > 20 { parse_int32(args[20]) } else { 0 },
        ask_vol4: if args.len() > 21 { parse_int32(args[21]) } else { 0 },
        ask_vol5: if args.len() > 22 { parse_int32(args[22]) } else { 0 },
        bid_px1: if args.len() > 23 { parse_decimal(args[23], 2) } else { 0 },
        bid_px2: if args.len() > 24 { parse_decimal(args[24], 2) } else { 0 },
        bid_px3: if args.len() > 25 { parse_decimal(args[25], 2) } else { 0 },
        bid_px4: if args.len() > 26 { parse_decimal(args[26], 2) } else { 0 },
        bid_px5: if args.len() > 27 { parse_decimal(args[27], 2) } else { 0 },
        bid_vol1: if args.len() > 28 { parse_int32(args[28]) } else { 0 },
        bid_vol2: if args.len() > 29 { parse_int32(args[29]) } else { 0 },
        bid_vol3: if args.len() > 30 { parse_int32(args[30]) } else { 0 },
        bid_vol4: if args.len() > 31 { parse_int32(args[31]) } else { 0 },
        bid_vol5: if args.len() > 32 { parse_int32(args[32]) } else { 0 },
        local_time,
    })
}

pub fn build_multi_insert_sql(table: &str, batch: &[TickElement]) -> String {
    let mut sb = String::with_capacity(batch.len() * 350);
    sb.push_str("INSERT INTO ");
    sb.push_str(table);
    sb.push_str(" (security_id, deal_date, deal_time, pre_close, open_px, high_px, low_px, close_px, high_limit, low_limit, total_num_trade, trans_volume, trans_value, ask_px1, ask_px2, ask_px3, ask_px4, ask_px5, ask_vol1, ask_vol2, ask_vol3, ask_vol4, ask_vol5, bid_px1, bid_px2, bid_px3, bid_px4, bid_px5, bid_vol1, bid_vol2, bid_vol3, bid_vol4, bid_vol5, local_time) VALUES ");

    for (i, e) in batch.iter().enumerate() {
        if i > 0 {
            sb.push_str(", ");
        }
        sb.push('(');
        sb.push('\'');
        escape_string_into(&e.security_id, &mut sb);
        sb.push('\'');
        sb.push(',');
        sb.push_str(&e.deal_date.to_string());
        sb.push(',');
        sb.push_str(&e.deal_time.to_string());
        sb.push(',');
        sb.push_str(&format_decimal(e.pre_close));
        sb.push(',');
        sb.push_str(&format_decimal(e.open_px));
        sb.push(',');
        sb.push_str(&format_decimal(e.high_px));
        sb.push(',');
        sb.push_str(&format_decimal(e.low_px));
        sb.push(',');
        sb.push_str(&format_decimal(e.close_px));
        sb.push(',');
        sb.push_str(&format_decimal(e.high_limit));
        sb.push(',');
        sb.push_str(&format_decimal(e.low_limit));
        sb.push(',');
        sb.push_str(&e.total_num_trade.to_string());
        sb.push(',');
        sb.push_str(&e.trans_volume.to_string());
        sb.push(',');
        sb.push_str(&format_decimal(e.trans_value));
        sb.push(',');
        sb.push_str(&format_decimal(e.ask_px1));
        sb.push(',');
        sb.push_str(&format_decimal(e.ask_px2));
        sb.push(',');
        sb.push_str(&format_decimal(e.ask_px3));
        sb.push(',');
        sb.push_str(&format_decimal(e.ask_px4));
        sb.push(',');
        sb.push_str(&format_decimal(e.ask_px5));
        sb.push(',');
        sb.push_str(&e.ask_vol1.to_string());
        sb.push(',');
        sb.push_str(&e.ask_vol2.to_string());
        sb.push(',');
        sb.push_str(&e.ask_vol3.to_string());
        sb.push(',');
        sb.push_str(&e.ask_vol4.to_string());
        sb.push(',');
        sb.push_str(&e.ask_vol5.to_string());
        sb.push(',');
        sb.push_str(&format_decimal(e.bid_px1));
        sb.push(',');
        sb.push_str(&format_decimal(e.bid_px2));
        sb.push(',');
        sb.push_str(&format_decimal(e.bid_px3));
        sb.push(',');
        sb.push_str(&format_decimal(e.bid_px4));
        sb.push(',');
        sb.push_str(&format_decimal(e.bid_px5));
        sb.push(',');
        sb.push_str(&e.bid_vol1.to_string());
        sb.push(',');
        sb.push_str(&e.bid_vol2.to_string());
        sb.push(',');
        sb.push_str(&e.bid_vol3.to_string());
        sb.push(',');
        sb.push_str(&e.bid_vol4.to_string());
        sb.push(',');
        sb.push_str(&e.bid_vol5.to_string());
        sb.push(',');
        if e.local_time != 0 {
            sb.push('\'');
            sb.push_str(&format_timestamp(e.local_time));
            sb.push('\'');
        } else {
            sb.push_str("NULL");
        }
        sb.push(')');
    }

    sb
}

fn escape_string_into(s: &str, sb: &mut String) {
    for c in s.chars() {
        if c == '\'' {
            sb.push_str("''");
        } else {
            sb.push(c);
        }
    }
}

fn format_decimal(raw: i64) -> String {
    let neg = raw < 0;
    let abs = if neg { -raw } else { raw };
    let int_part = abs / 100;
    let frac_part = abs % 100;
    let mut s = if neg { String::from("-") } else { String::new() };
    s.push_str(&int_part.to_string());
    s.push('.');
    if frac_part < 10 {
        s.push('0');
    }
    s.push_str(&frac_part.to_string());
    s
}

fn format_timestamp(micros: i64) -> String {
    let secs = micros / 1_000_000;
    let remainder = micros % 1_000_000;
    let frac_millis = remainder / 1000;

    let mut remaining_days = secs / 86400;
    let time_secs = (secs % 86400) as u32;
    let hour = time_secs / 3600;
    let minute = (time_secs % 3600) / 60;
    let second = time_secs % 60;

    let mut year: i32 = 1970;
    loop {
        let days_in_year = if is_leap(year) { 366 } else { 365 };
        if remaining_days < days_in_year as i64 {
            break;
        }
        remaining_days -= days_in_year as i64;
        year += 1;
    }

    let days_in_month = |m: u32, y: i32| -> i64 {
        match m {
            1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
            4 | 6 | 9 | 11 => 30,
            2 => if is_leap(y) { 29 } else { 28 },
            _ => 0,
        }
    };

    let mut month = 1u32;
    loop {
        let dim = days_in_month(month, year);
        if remaining_days < dim {
            break;
        }
        remaining_days -= dim;
        month += 1;
    }
    let day = (remaining_days + 1) as u32;

    if frac_millis > 0 {
        format!("{}-{:02}-{:02} {:02}:{:02}:{:02}.{:03}", year, month, day, hour, minute, second, frac_millis)
    } else {
        format!("{}-{:02}-{:02} {:02}:{:02}:{:02}", year, month, day, hour, minute, second)
    }
}

#[inline]
fn parse_int32(s: &str) -> i32 {
    let s = s.trim();
    let bytes = s.as_bytes();
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
    if neg { -result } else { result }
}

#[inline]
fn parse_int64(s: &str) -> i64 {
    let s = s.trim();
    let bytes = s.as_bytes();
    if bytes.is_empty() {
        return 0;
    }
    let mut result: i64 = 0;
    let mut neg = false;
    let mut idx = 0;
    let mut has_dot = false;
    let mut first_after_dot: i64 = 0;

    if bytes[0] == b'-' {
        neg = true;
        idx = 1;
    } else if bytes[0] == b'+' {
        idx = 1;
    }
    while idx < bytes.len() {
        let b = bytes[idx];
        if b == b'.' {
            has_dot = true;
            idx += 1;
            continue;
        }
        if b < b'0' || b > b'9' {
            break;
        }
        if !has_dot {
            result = result * 10 + (b - b'0') as i64;
        } else if first_after_dot == 0 {
            first_after_dot = (b - b'0') as i64;
        }
        idx += 1;
    }
    if has_dot && first_after_dot >= 5 {
        result += 1;
    }
    if neg { -result } else { result }
}

#[inline]
fn parse_decimal(s: &str, scale: u32) -> i64 {
    let s = s.trim();
    let bytes = s.as_bytes();
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
    if neg { -result } else { result }
}

#[inline]
fn parse_timestamp(s: &str) -> i64 {
    let bytes = s.as_bytes();
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
