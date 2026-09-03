use crate::common::error::{Result, TaibaiError};

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

/// Decompress only if `data` is a complete, well-formed zlib stream that
/// consumes every input byte (header, deflate body, and adler32 trailer).
/// Returns `None` for anything else so callers keep the original payload.
pub fn zlib_decompress_complete(data: &[u8]) -> Option<Vec<u8>> {
    use flate2::{Decompress, FlushDecompress, Status};
    if data.len() < 8 {
        return None;
    }
    let mut decompress = Decompress::new(true);
    let mut out = Vec::with_capacity(data.len().saturating_mul(2));
    let mut chunk = [0u8; 16384];
    loop {
        let input = &data[decompress.total_in() as usize..];
        let before_out = decompress.total_out();
        let status = decompress
            .decompress(input, &mut chunk, FlushDecompress::None)
            .ok()?;
        let produced = (decompress.total_out() - before_out) as usize;
        out.extend_from_slice(&chunk[..produced]);
        match status {
            Status::StreamEnd => break,
            Status::Ok | Status::BufError => {
                if produced == 0 || out.len() > crate::protocol::message::MAX_MESSAGE_SIZE {
                    return None;
                }
            }
        }
    }
    if decompress.total_in() as usize != data.len() {
        return None;
    }
    Some(out)
}

#[cfg(test)]
#[path = "compress_test.rs"]
mod tests;
