use super::*;

#[test]
fn test_encode_decode_small_payload() {
    let mut codec = MessageCodec::new(1024 * 1024);
    let msg = Message::new(0x01, Bytes::from("hello"));
    let mut buf = BytesMut::new();
    codec.encode(msg, &mut buf).unwrap();

    let decoded = codec.decode(&mut buf).unwrap().unwrap();
    assert_eq!(decoded.msg_type, 0x01);
    assert_eq!(&decoded.payload[..], b"hello");
}

#[test]
fn test_encode_decode_large_payload_compressed() {
    let mut codec = MessageCodec::new(1024 * 1024).with_compression(100);
    let data: Vec<u8> = (0..10000).map(|i| (i % 256) as u8).collect();
    let msg = Message::new(0x02, Bytes::from(data));
    let mut buf = BytesMut::new();
    codec.encode(msg, &mut buf).unwrap();

    let decoded = codec.decode(&mut buf).unwrap().unwrap();
    assert_eq!(decoded.msg_type, 0x02);
    assert_eq!(decoded.payload.len(), 10000);
}

#[test]
fn test_prepare_response_is_not_compressed() {
    let mut codec = MessageCodec::new(1024 * 1024).with_compression(8);
    let mut payload = Vec::new();
    payload.extend_from_slice(&1u32.to_le_bytes());
    payload.extend_from_slice(&4096u32.to_le_bytes());
    payload.extend(std::iter::repeat_n(7u8, 4096));
    let msg = Message::new(MSG_TYPE_PREPARE, Bytes::from(payload.clone()));
    let mut buf = BytesMut::new();
    codec.encode(msg, &mut buf).unwrap();

    assert_eq!(buf.len(), HEADER_SIZE + payload.len());
    assert_eq!(&buf[HEADER_SIZE..HEADER_SIZE + 4], &1u32.to_le_bytes());
}

#[test]
fn test_decode_incomplete() {
    let mut codec = MessageCodec::new(1024 * 1024);
    let mut buf = BytesMut::from(&[0x01, 0xDB, 0x01, 0x00, 0x00, 0x00][..]);
    let result = codec.decode(&mut buf).unwrap();
    assert!(result.is_none());
}

#[test]
fn test_decode_close_stmt_payload_not_treated_as_compressed() {
    let mut codec = MessageCodec::new(1024 * 1024);
    let stmt_id: u32 = 376;
    let payload = stmt_id.to_le_bytes();
    assert_eq!(payload, [0x78, 0x01, 0x00, 0x00]);
    let header = MessageHeader {
        msg_type: 0x52,
        payload_len: payload.len() as u32,
    };
    let mut buf = BytesMut::new();
    buf.extend_from_slice(&header.encode());
    buf.extend_from_slice(&payload);
    let decoded = codec.decode(&mut buf).unwrap().unwrap();
    assert_eq!(decoded.msg_type, 0x52);
    assert_eq!(&decoded.payload[..], &payload[..]);
}

#[test]
fn test_decode_invalid_zlib_prefix_payload_left_untouched() {
    let mut codec = MessageCodec::new(1024 * 1024);
    let payload: Vec<u8> = vec![0x78, 0x9C, 0xAA, 0xBB, 0x00, 0x11, 0x22, 0x33, 0x44];
    let header = MessageHeader {
        msg_type: 0x53,
        payload_len: payload.len() as u32,
    };
    let mut buf = BytesMut::new();
    buf.extend_from_slice(&header.encode());
    buf.extend_from_slice(&payload);
    let decoded = codec.decode(&mut buf).unwrap().unwrap();
    assert_eq!(&decoded.payload[..], &payload[..]);
}

#[test]
fn test_change_event_notifications_are_not_recompressed() {
    // The fan-out thread compresses delete/update notification payloads once
    // before pushing; the codec must not compress them again or a client's
    // single decompress pass leaves zlib bytes that parse as garbage.
    for msg_type in [
        MSG_TYPE_NOTIFICATION,
        MSG_TYPE_NOTIFICATION_INSERT,
        MSG_TYPE_NOTIFICATION_DELETE,
        MSG_TYPE_NOTIFICATION_UPDATE,
    ] {
        let mut codec = MessageCodec::new(1024 * 1024).with_compression(64);
        // Compress once, as the fan-out thread does. The inner payload is
        // pseudo-random so the compressed form still exceeds the threshold.
        let inner: Vec<u8> = (0..8192u64)
            .map(|i| ((i.wrapping_mul(6364136223846793005).wrapping_shr(33)) & 0xFF) as u8)
            .collect();
        let once = crate::protocol::compress::zlib_compress(&inner).expect("compress");
        assert!(once.len() > 64, "test payload must exceed the threshold");

        let msg = Message::new(msg_type, Bytes::from(once.clone()));
        let mut buf = BytesMut::new();
        codec.encode(msg, &mut buf).unwrap();
        assert_eq!(
            buf.len(),
            HEADER_SIZE + once.len(),
            "payload was recompressed for {msg_type:#04X}"
        );

        // The wire carries exactly the fan-out bytes (checked above), so one
        // client-side decode pass restores the original payload.
        let decoded = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.msg_type, msg_type);
        assert_eq!(decoded.payload, Bytes::from(inner));
    }
}
