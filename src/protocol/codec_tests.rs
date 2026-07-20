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
