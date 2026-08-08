use super::*;

#[test]
fn test_gzip_roundtrip() {
    let data = b"Hello, World! This is a test of gzip compression.";
    let compressed = gzip_compress(data).unwrap();
    // Small payloads may have overhead; just verify roundtrip works
    let decompressed = gzip_decompress(&compressed).unwrap();
    assert_eq!(decompressed, data);
}

#[test]
fn test_zlib_roundtrip() {
    let data = b"Hello, World! This is a test of zlib compression.";
    let compressed = zlib_compress(data).unwrap();
    let decompressed = zlib_decompress(&compressed).unwrap();
    assert_eq!(decompressed, data);
}

#[test]
fn test_gzip_large_data() {
    let data: Vec<u8> = (0..10000).map(|i| (i % 256) as u8).collect();
    let compressed = gzip_compress(&data).unwrap();
    // Repetitive data should compress well
    assert!(compressed.len() < data.len());
    let decompressed = gzip_decompress(&compressed).unwrap();
    assert_eq!(decompressed, data);
}

#[test]
fn test_zlib_empty() {
    let compressed = zlib_compress(&[]).unwrap();
    let decompressed = zlib_decompress(&compressed).unwrap();
    assert!(decompressed.is_empty());
}

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
    assert!(zlib_decompress_complete(&[0x78, 0x9C, 0xAA, 0xBB, 0x00, 0x11, 0x22, 0x33, 0x44]).is_none());
    assert!(zlib_decompress_complete(b"not compressed at all").is_none());
    assert!(zlib_decompress_complete(&[]).is_none());
}
