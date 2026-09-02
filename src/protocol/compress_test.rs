use super::*;

#[test]
fn test_zlib_decompress_complete_roundtrip() {
    let data: Vec<u8> = (0..10000).map(|i| (i % 256) as u8).collect();
    let compressed = zlib_compress(&data).unwrap();
    assert_eq!(zlib_decompress_complete(&compressed).unwrap(), data);
}

#[test]
fn test_zlib_decompress_complete_rejects_truncated_stream() {
    assert!(zlib_decompress_complete(&[0x78, 0x01, 0x00, 0x00]).is_none());
    let compressed = zlib_compress(b"hello taibai").unwrap();
    let truncated = &compressed[..compressed.len() - 2];
    assert!(zlib_decompress_complete(truncated).is_none());
}

#[test]
fn test_zlib_decompress_complete_rejects_trailing_bytes() {
    let mut compressed = zlib_compress(b"payload").unwrap();
    compressed.extend_from_slice(&[0x00, 0x01]);
    assert!(zlib_decompress_complete(&compressed).is_none());
}

#[test]
fn test_zlib_decompress_complete_rejects_garbage() {
    assert!(
        zlib_decompress_complete(&[0x78, 0x9C, 0xAA, 0xBB, 0x00, 0x11, 0x22, 0x33, 0x44]).is_none()
    );
    assert!(zlib_decompress_complete(b"not compressed at all").is_none());
    assert!(zlib_decompress_complete(&[]).is_none());
}
