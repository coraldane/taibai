#![allow(dead_code)]

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
) PARTITION BY deal_date CLUSTER BY (security_id, deal_date, deal_time)";

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
        ask_px1: if args.len() > 13 {
            parse_decimal(args[13], 2)
        } else {
            0
        },
        ask_px2: if args.len() > 14 {
            parse_decimal(args[14], 2)
        } else {
            0
        },
        ask_px3: if args.len() > 15 {
            parse_decimal(args[15], 2)
        } else {
            0
        },
        ask_px4: if args.len() > 16 {
            parse_decimal(args[16], 2)
        } else {
            0
        },
        ask_px5: if args.len() > 17 {
            parse_decimal(args[17], 2)
        } else {
            0
        },
        ask_vol1: if args.len() > 18 {
            parse_int32(args[18])
        } else {
            0
        },
        ask_vol2: if args.len() > 19 {
            parse_int32(args[19])
        } else {
            0
        },
        ask_vol3: if args.len() > 20 {
            parse_int32(args[20])
        } else {
            0
        },
        ask_vol4: if args.len() > 21 {
            parse_int32(args[21])
        } else {
            0
        },
        ask_vol5: if args.len() > 22 {
            parse_int32(args[22])
        } else {
            0
        },
        bid_px1: if args.len() > 23 {
            parse_decimal(args[23], 2)
        } else {
            0
        },
        bid_px2: if args.len() > 24 {
            parse_decimal(args[24], 2)
        } else {
            0
        },
        bid_px3: if args.len() > 25 {
            parse_decimal(args[25], 2)
        } else {
            0
        },
        bid_px4: if args.len() > 26 {
            parse_decimal(args[26], 2)
        } else {
            0
        },
        bid_px5: if args.len() > 27 {
            parse_decimal(args[27], 2)
        } else {
            0
        },
        bid_vol1: if args.len() > 28 {
            parse_int32(args[28])
        } else {
            0
        },
        bid_vol2: if args.len() > 29 {
            parse_int32(args[29])
        } else {
            0
        },
        bid_vol3: if args.len() > 30 {
            parse_int32(args[30])
        } else {
            0
        },
        bid_vol4: if args.len() > 31 {
            parse_int32(args[31])
        } else {
            0
        },
        bid_vol5: if args.len() > 32 {
            parse_int32(args[32])
        } else {
            0
        },
        local_time,
    })
}

pub fn build_multi_insert_sql(table: &str, batch: &[TickElement]) -> String {
    let mut sb = String::with_capacity(batch.len() * 300);
    append_insert_prefix(table, &mut sb);

    let mut dec_buf = DecBuf::new();
    let mut int_buf = IntBuf::new();

    for (i, e) in batch.iter().enumerate() {
        if i > 0 {
            sb.push_str(", ");
        }
        sb.push('(');
        sb.push('\'');
        escape_string_into(&e.security_id, &mut sb);
        sb.push('\'');
        sb.push(',');
        write_i32(&mut int_buf, &mut sb, e.deal_date);
        sb.push(',');
        write_i32(&mut int_buf, &mut sb, e.deal_time);
        sb.push(',');
        write_decimal(&mut dec_buf, &mut sb, e.pre_close);
        sb.push(',');
        write_decimal(&mut dec_buf, &mut sb, e.open_px);
        sb.push(',');
        write_decimal(&mut dec_buf, &mut sb, e.high_px);
        sb.push(',');
        write_decimal(&mut dec_buf, &mut sb, e.low_px);
        sb.push(',');
        write_decimal(&mut dec_buf, &mut sb, e.close_px);
        sb.push(',');
        write_decimal(&mut dec_buf, &mut sb, e.high_limit);
        sb.push(',');
        write_decimal(&mut dec_buf, &mut sb, e.low_limit);
        sb.push(',');
        write_i32(&mut int_buf, &mut sb, e.total_num_trade);
        sb.push(',');
        write_i64(&mut int_buf, &mut sb, e.trans_volume);
        sb.push(',');
        write_decimal(&mut dec_buf, &mut sb, e.trans_value);
        sb.push(',');
        write_decimal(&mut dec_buf, &mut sb, e.ask_px1);
        sb.push(',');
        write_decimal(&mut dec_buf, &mut sb, e.ask_px2);
        sb.push(',');
        write_decimal(&mut dec_buf, &mut sb, e.ask_px3);
        sb.push(',');
        write_decimal(&mut dec_buf, &mut sb, e.ask_px4);
        sb.push(',');
        write_decimal(&mut dec_buf, &mut sb, e.ask_px5);
        sb.push(',');
        write_i32(&mut int_buf, &mut sb, e.ask_vol1);
        sb.push(',');
        write_i32(&mut int_buf, &mut sb, e.ask_vol2);
        sb.push(',');
        write_i32(&mut int_buf, &mut sb, e.ask_vol3);
        sb.push(',');
        write_i32(&mut int_buf, &mut sb, e.ask_vol4);
        sb.push(',');
        write_i32(&mut int_buf, &mut sb, e.ask_vol5);
        sb.push(',');
        write_decimal(&mut dec_buf, &mut sb, e.bid_px1);
        sb.push(',');
        write_decimal(&mut dec_buf, &mut sb, e.bid_px2);
        sb.push(',');
        write_decimal(&mut dec_buf, &mut sb, e.bid_px3);
        sb.push(',');
        write_decimal(&mut dec_buf, &mut sb, e.bid_px4);
        sb.push(',');
        write_decimal(&mut dec_buf, &mut sb, e.bid_px5);
        sb.push(',');
        write_i32(&mut int_buf, &mut sb, e.bid_vol1);
        sb.push(',');
        write_i32(&mut int_buf, &mut sb, e.bid_vol2);
        sb.push(',');
        write_i32(&mut int_buf, &mut sb, e.bid_vol3);
        sb.push(',');
        write_i32(&mut int_buf, &mut sb, e.bid_vol4);
        sb.push(',');
        write_i32(&mut int_buf, &mut sb, e.bid_vol5);
        sb.push(',');
        if e.local_time != 0 {
            write_i64(&mut int_buf, &mut sb, e.local_time);
        } else {
            sb.push_str("NULL");
        }
        sb.push(')');
    }

    sb
}

pub fn append_insert_prefix(table: &str, sb: &mut String) {
    sb.push_str("INSERT INTO ");
    sb.push_str(table);
    sb.push_str(" (security_id, deal_date, deal_time, pre_close, open_px, high_px, low_px, close_px, high_limit, low_limit, total_num_trade, trans_volume, trans_value, ask_px1, ask_px2, ask_px3, ask_px4, ask_px5, ask_vol1, ask_vol2, ask_vol3, ask_vol4, ask_vol5, bid_px1, bid_px2, bid_px3, bid_px4, bid_px5, bid_vol1, bid_vol2, bid_vol3, bid_vol4, bid_vol5, local_time) VALUES ");
}

pub fn append_tick_csv_values(line: &str, sb: &mut String) -> bool {
    let args: Vec<&str> = line.split(',').collect();
    if args.len() < 13 {
        return false;
    }

    let deal_date = parse_int32(args[1]);
    if deal_date == 0 {
        return false;
    }

    let mut int_buf = IntBuf::new();
    sb.push('(');
    sb.push('\'');
    escape_string_into(args[0].trim(), sb);
    sb.push('\'');
    sb.push(',');
    write_i32(&mut int_buf, sb, deal_date);
    sb.push(',');
    append_i32_field(&args, 2, &mut int_buf, sb);
    sb.push(',');
    append_decimal_field(&args, 3, sb);
    sb.push(',');
    append_decimal_field(&args, 4, sb);
    sb.push(',');
    append_decimal_field(&args, 5, sb);
    sb.push(',');
    append_decimal_field(&args, 6, sb);
    sb.push(',');
    append_decimal_field(&args, 7, sb);
    sb.push(',');
    append_decimal_field(&args, 8, sb);
    sb.push(',');
    append_decimal_field(&args, 9, sb);
    sb.push(',');
    append_i32_field(&args, 10, &mut int_buf, sb);
    sb.push(',');
    append_i64_field(&args, 11, &mut int_buf, sb);
    sb.push(',');
    append_decimal_field(&args, 12, sb);
    sb.push(',');
    append_decimal_field(&args, 13, sb);
    sb.push(',');
    append_decimal_field(&args, 14, sb);
    sb.push(',');
    append_decimal_field(&args, 15, sb);
    sb.push(',');
    append_decimal_field(&args, 16, sb);
    sb.push(',');
    append_decimal_field(&args, 17, sb);
    sb.push(',');
    append_i32_field(&args, 18, &mut int_buf, sb);
    sb.push(',');
    append_i32_field(&args, 19, &mut int_buf, sb);
    sb.push(',');
    append_i32_field(&args, 20, &mut int_buf, sb);
    sb.push(',');
    append_i32_field(&args, 21, &mut int_buf, sb);
    sb.push(',');
    append_i32_field(&args, 22, &mut int_buf, sb);
    sb.push(',');
    append_decimal_field(&args, 23, sb);
    sb.push(',');
    append_decimal_field(&args, 24, sb);
    sb.push(',');
    append_decimal_field(&args, 25, sb);
    sb.push(',');
    append_decimal_field(&args, 26, sb);
    sb.push(',');
    append_decimal_field(&args, 27, sb);
    sb.push(',');
    append_i32_field(&args, 28, &mut int_buf, sb);
    sb.push(',');
    append_i32_field(&args, 29, &mut int_buf, sb);
    sb.push(',');
    append_i32_field(&args, 30, &mut int_buf, sb);
    sb.push(',');
    append_i32_field(&args, 31, &mut int_buf, sb);
    sb.push(',');
    append_i32_field(&args, 32, &mut int_buf, sb);
    sb.push(',');
    if args.len() > 33 && !args[33].trim().is_empty() {
        write_i64(&mut int_buf, sb, parse_timestamp(args[33].trim()));
    } else {
        sb.push_str("NULL");
    }
    sb.push(')');
    true
}

fn append_decimal_field(args: &[&str], idx: usize, sb: &mut String) {
    let value = args.get(idx).map(|s| s.trim()).unwrap_or("");
    if value.is_empty() {
        sb.push('0');
    } else {
        sb.push_str(value);
    }
}

fn append_i32_field(args: &[&str], idx: usize, int_buf: &mut IntBuf, sb: &mut String) {
    write_i32(
        int_buf,
        sb,
        args.get(idx).map(|s| parse_int32(s)).unwrap_or(0),
    );
}

fn append_i64_field(args: &[&str], idx: usize, int_buf: &mut IntBuf, sb: &mut String) {
    write_i64(
        int_buf,
        sb,
        args.get(idx).map(|s| parse_int64(s)).unwrap_or(0),
    );
}

struct DecBuf {
    buf: [u8; 32],
    len: usize,
}

impl DecBuf {
    fn new() -> Self {
        DecBuf {
            buf: [0u8; 32],
            len: 0,
        }
    }
}

struct IntBuf {
    buf: [u8; 20],
    len: usize,
}

impl IntBuf {
    fn new() -> Self {
        IntBuf {
            buf: [0u8; 20],
            len: 0,
        }
    }
}

fn write_decimal(buf: &mut DecBuf, sb: &mut String, raw: i64) {
    let neg = raw < 0;
    let abs = if neg { -raw } else { raw };
    let int_part = abs / 100;
    let frac_part = abs % 100;

    buf.len = 0;
    if neg {
        buf.buf[buf.len] = b'-';
        buf.len += 1;
    }

    if int_part == 0 {
        buf.buf[buf.len] = b'0';
        buf.len += 1;
    } else {
        let mut tmp = [0u8; 20];
        let mut tmp_len = 0;
        let mut n = int_part;
        while n > 0 {
            tmp[tmp_len] = b'0' + (n % 10) as u8;
            tmp_len += 1;
            n /= 10;
        }
        for i in (0..tmp_len).rev() {
            buf.buf[buf.len] = tmp[i];
            buf.len += 1;
        }
    }

    buf.buf[buf.len] = b'.';
    buf.len += 1;

    if frac_part < 10 {
        buf.buf[buf.len] = b'0';
        buf.len += 1;
    }

    if frac_part == 0 {
        buf.buf[buf.len] = b'0';
        buf.len += 1;
    } else {
        let mut tmp = [0u8; 2];
        let mut tmp_len = 0;
        let mut n = frac_part;
        while n > 0 {
            tmp[tmp_len] = b'0' + (n % 10) as u8;
            tmp_len += 1;
            n /= 10;
        }
        for i in (0..tmp_len).rev() {
            buf.buf[buf.len] = tmp[i];
            buf.len += 1;
        }
    }

    sb.push_str(unsafe { std::str::from_utf8_unchecked(&buf.buf[..buf.len]) });
}

fn write_i32(buf: &mut IntBuf, sb: &mut String, val: i32) {
    buf.len = 0;
    let mut n = val;
    if n < 0 {
        buf.buf[buf.len] = b'-';
        buf.len += 1;
        n = -n;
    }
    if n == 0 {
        buf.buf[buf.len] = b'0';
        buf.len += 1;
    } else {
        let mut tmp = [0u8; 10];
        let mut tmp_len = 0;
        while n > 0 {
            tmp[tmp_len] = b'0' + (n % 10) as u8;
            tmp_len += 1;
            n /= 10;
        }
        for i in (0..tmp_len).rev() {
            buf.buf[buf.len] = tmp[i];
            buf.len += 1;
        }
    }
    sb.push_str(unsafe { std::str::from_utf8_unchecked(&buf.buf[..buf.len]) });
}

fn write_i64(buf: &mut IntBuf, sb: &mut String, val: i64) {
    buf.len = 0;
    let mut n = val;
    if n < 0 {
        buf.buf[buf.len] = b'-';
        buf.len += 1;
        n = -n;
    }
    if n == 0 {
        buf.buf[buf.len] = b'0';
        buf.len += 1;
    } else {
        let mut tmp = [0u8; 20];
        let mut tmp_len = 0;
        while n > 0 {
            tmp[tmp_len] = b'0' + (n % 10) as u8;
            tmp_len += 1;
            n /= 10;
        }
        for i in (0..tmp_len).rev() {
            buf.buf[buf.len] = tmp[i];
            buf.len += 1;
        }
    }
    sb.push_str(unsafe { std::str::from_utf8_unchecked(&buf.buf[..buf.len]) });
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
        if !b.is_ascii_digit() {
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
        if !b.is_ascii_digit() {
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
    if neg {
        -result
    } else {
        result
    }
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
        if !b.is_ascii_digit() {
            break;
        }
        if !has_decimal || decimals < scale {
            result = result * 10 + (b - b'0') as i64;
            if has_decimal {
                decimals += 1;
            }
        } else if decimals == scale {
            let digit = (b - b'0') as i64;
            if digit >= 5 {
                result += 1;
            }
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

    if year < 1970 || !(1..=12).contains(&month) || !(1..=31).contains(&day) {
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

    let mut micros = days * 86_400_000_000i64;
    micros += hour as i64 * 3_600_000_000i64;
    micros += minute as i64 * 60_000_000i64;
    micros += second as i64 * 1_000_000i64;

    if bytes.len() > 19 && bytes[19] == b'.' {
        let mut frac: i64 = 0;
        let mut frac_digits = 0;
        let mut pos = 20;
        while pos < bytes.len() && frac_digits < 6 {
            let b = bytes[pos];
            if !b.is_ascii_digit() {
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
