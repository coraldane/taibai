use super::*;

/// Only leading-keyword SELECT/SHOW/EXPLAIN auto-retry: reads are
/// idempotent, writes are not (a retried INSERT may commit twice).
#[test]
fn read_only_classification_follows_the_leading_keyword() {
    for sql in [
        "SELECT * FROM t",
        "select 1",
        "  SHOW TABLES",
        "show databases",
        "EXPLAIN SELECT COUNT(*) FROM t",
        "explain analyze",
    ] {
        assert!(statement_is_read_only(sql), "{sql:?} should retry");
    }
    for sql in [
        "INSERT INTO t VALUES (1)",
        "insert into t values (1)",
        "UPDATE t SET a = 1",
        "DELETE FROM t",
        "REPLACE INTO t VALUES (1)",
        "CREATE TABLE u (a INT)",
        "BEGIN",
        "WITH c AS (SELECT 1) INSERT INTO t SELECT * FROM c",
    ] {
        assert!(
            !statement_is_read_only(sql),
            "{sql:?} must be left to the application"
        );
    }
}

#[test]
fn retryable_marker_is_detected_by_stable_prefix() {
    assert!(is_retryable_error_message(
        "[RETRYABLE] resource busy: hot store over limit"
    ));
    assert!(is_retryable_error_message("  [RETRYABLE] resource busy: x"));
    assert!(!is_retryable_error_message(
        "resource busy: [RETRYABLE] embedded"
    ));
    assert!(!is_retryable_error_message("SQL error: bad syntax"));
    assert!(!is_retryable_error_message(""));
}

/// 10ms / 40ms / 160ms with up to +50% jitter, capped at 160ms.
#[test]
fn backoff_schedule_is_ten_forty_one_sixty_plus_jitter() {
    let first = retry_backoff_delay(0).as_millis();
    assert!((10..=15).contains(&first), "first backoff {first}ms");
    let second = retry_backoff_delay(1).as_millis();
    assert!((40..=60).contains(&second), "second backoff {second}ms");
    let third = retry_backoff_delay(2).as_millis();
    assert!((160..=240).contains(&third), "third backoff {third}ms");
    let capped = retry_backoff_delay(9).as_millis();
    assert!((160..=240).contains(&capped), "capped backoff {capped}ms");
}
