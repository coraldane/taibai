use super::*;

#[test]
fn resource_busy_maps_to_retryable_marker() {
    let msg = build_query_error(&TaibaiError::ResourceBusy("hot store over limit".into()));
    assert_eq!(msg.msg_type, MSG_TYPE_ERROR);
    let text = String::from_utf8_lossy(&msg.payload);
    assert!(
        text.starts_with(crate::net::client::RETRYABLE_ERROR_PREFIX),
        "marker must lead the message, got {text:?}"
    );
    assert!(text.contains("hot store over limit"));
}

#[test]
fn non_busy_errors_keep_plain_text() {
    let msg = build_query_error(&TaibaiError::Sql("syntax error".into()));
    let text = String::from_utf8_lossy(&msg.payload);
    assert!(!text.contains(crate::net::client::RETRYABLE_ERROR_PREFIX));
    assert!(text.contains("syntax error"));
}

#[test]
fn result_overrun_is_deterministic_without_retry_marker() {
    let msg = build_query_error(&TaibaiError::ResourceBusy(
        "query result exceeded memory limit of 32 MB (bounded by server.max_message_size = 64 MB)"
            .into(),
    ));
    let text = String::from_utf8_lossy(&msg.payload);
    assert!(
        !text.contains(crate::net::client::RETRYABLE_ERROR_PREFIX),
        "deterministic result overruns must not advertise retry, got {text:?}"
    );
    assert!(text.starts_with("resource busy: "), "{text}");
    assert!(text.contains("exceeded memory limit"), "{text}");
}
