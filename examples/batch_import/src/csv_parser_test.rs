use super::parse_decimal_bytes;

#[test]
fn decimal_parser_respects_target_scale() {
    assert_eq!(parse_decimal_bytes(b"16235.9018", 2), 1_623_590);
    assert_eq!(parse_decimal_bytes(b"1.6500", 2), 165);
    assert_eq!(parse_decimal_bytes(b"123", 2), 12_300);
}

#[test]
fn decimal_parser_rounds_first_discarded_digit() {
    assert_eq!(parse_decimal_bytes(b"1.235", 2), 124);
    assert_eq!(parse_decimal_bytes(b"-1.235", 2), -124);
    assert_eq!(parse_decimal_bytes(b"9.999", 2), 1_000);
}
