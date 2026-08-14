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
