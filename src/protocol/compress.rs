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
#[path = "compress_tests.rs"]
mod tests;
