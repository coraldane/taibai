use crate::common::error::{Result, TaibaiError};

/// Compress data using gzip.
pub fn gzip_compress(data: &[u8]) -> Result<Vec<u8>> {
    use std::io::Write;
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    encoder
        .write_all(data)
        .map_err(|e| TaibaiError::Protocol(format!("gzip compress error: {}", e)))?;
    encoder
        .finish()
        .map_err(|e| TaibaiError::Protocol(format!("gzip finalize error: {}", e)))
}

/// Decompress gzip data.
pub fn gzip_decompress(data: &[u8]) -> Result<Vec<u8>> {
    use std::io::Read;
    let mut decoder = flate2::read::GzDecoder::new(data);
    let mut result = Vec::new();
    decoder
        .read_to_end(&mut result)
        .map_err(|e| TaibaiError::Protocol(format!("gzip decompress error: {}", e)))?;
    Ok(result)
}

/// Compress data using zlib.
pub fn zlib_compress(data: &[u8]) -> Result<Vec<u8>> {
    use std::io::Write;
    let mut encoder = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::fast());
    encoder
        .write_all(data)
        .map_err(|e| TaibaiError::Protocol(format!("zlib compress error: {}", e)))?;
    encoder
        .finish()
        .map_err(|e| TaibaiError::Protocol(format!("zlib finalize error: {}", e)))
}

/// Decompress zlib data.
pub fn zlib_decompress(data: &[u8]) -> Result<Vec<u8>> {
    use std::io::Read;
    let mut decoder = flate2::read::ZlibDecoder::new(data);
    let mut result = Vec::new();
    decoder
        .read_to_end(&mut result)
        .map_err(|e| TaibaiError::Protocol(format!("zlib decompress error: {}", e)))?;
    Ok(result)
}

#[cfg(test)]
mod tests {
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
}
