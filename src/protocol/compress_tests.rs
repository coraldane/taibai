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
