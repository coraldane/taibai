use super::{concat, length, substring};
use crate::types::scalar::ScalarValue;

fn varchar(s: &str) -> ScalarValue {
    ScalarValue::VarChar(Some(s.to_string()))
}

fn int(v: i64) -> ScalarValue {
    ScalarValue::Int64(Some(v))
}

fn as_str(value: &ScalarValue) -> String {
    match value {
        ScalarValue::VarChar(Some(s)) => s.clone(),
        other => panic!("expected VarChar, got {other:?}"),
    }
}

fn assert_null(value: &ScalarValue) {
    assert!(
        matches!(value, ScalarValue::VarChar(None)),
        "expected NULL, got {value:?}"
    );
}

#[test]
fn substring_ascii_matches_byte_semantics() {
    let s = varchar("abcdefg");
    assert_eq!(
        as_str(&substring(&[s.clone(), int(2), int(3)]).unwrap()),
        "bcd"
    );
    assert_eq!(as_str(&substring(&[s.clone(), int(3)]).unwrap()), "cdefg");
    assert_eq!(as_str(&substring(&[s, int(1), int(0)]).unwrap()), "");
}

#[test]
fn substring_slices_on_char_boundaries_for_multibyte() {
    let s = varchar("深证证券交易所");
    assert_eq!(
        as_str(&substring(&[s.clone(), int(3), int(2)]).unwrap()),
        "证券"
    );
    assert_eq!(as_str(&substring(&[s, int(6)]).unwrap()), "易所");
}

#[test]
fn substring_longer_len_than_rest_is_safe_for_multibyte() {
    // 2026-10-13 生产 panic 回归：SUBSTR(main_business,1,80) 切进多字节字符。
    let s = varchar("深圳市朗科智能电气股份有限公司及子公司主营业务为智能电气的研发");
    assert_eq!(
        as_str(&substring(&[s, int(1), int(80)]).unwrap()),
        "深圳市朗科智能电气股份有限公司及子公司主营业务为智能电气的研发"
    );
}

#[test]
fn substring_start_beyond_char_count_returns_empty() {
    let s = varchar("中文");
    assert_eq!(as_str(&substring(&[s, int(10), int(3)]).unwrap()), "");
}

#[test]
fn substring_negative_len_returns_empty() {
    let s = varchar("abcdef");
    assert_eq!(as_str(&substring(&[s, int(2), int(-1)]).unwrap()), "");
}

#[test]
fn substring_null_and_nonpositive_start() {
    assert_null(&substring(&[ScalarValue::VarChar(None), int(1), int(2)]).unwrap());
    assert_null(&substring(&[varchar("abc"), int(0), int(2)]).unwrap());
    assert_null(&substring(&[varchar("abc"), int(-2), int(2)]).unwrap());
}

#[test]
fn length_counts_bytes_like_mysql() {
    assert_eq!(
        length(&[varchar("abc")]).unwrap(),
        ScalarValue::Int64(Some(3))
    );
    assert_eq!(
        length(&[varchar("中文")]).unwrap(),
        ScalarValue::Int64(Some(6))
    );
}

#[test]
fn concat_skips_nulls() {
    let got = concat(&[varchar("a"), ScalarValue::VarChar(None), varchar("b")]).unwrap();
    assert_eq!(as_str(&got), "ab");
}
