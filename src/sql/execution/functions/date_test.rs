use super::*;

#[test]
fn string_to_days_uses_micros_divisor() {
    // Regression: the string→DATE conversion divided the microsecond
    // timestamp by seconds-per-day (86_400), producing values 10^6 too
    // large that wrapped i32 — every string date insert stored garbage.
    let days = datetime_string_to_days("2024-01-01", 0).expect("parse");
    assert_eq!(days, 19_723);
    assert_eq!(datetime_string_to_days("1970-01-02", 0), Some(1));
    // Datetime strings floor to the day boundary.
    assert_eq!(
        datetime_string_to_days("2024-01-01 23:59:59", 0),
        Some(19_723)
    );
}
