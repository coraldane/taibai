use std::io::{Cursor, Read};

use crate::common::error::{Result, TaibaiError};
use crate::protocol::types::DataType;
use crate::storage::codec::bit_util::{BitReader, BitWriter};
use crate::storage::codec::encoding::EncodingType;
use crate::storage::codec::io_util::{
    read_u32_le, read_varint, write_varint, zigzag_decode, zigzag_encode,
};
use crate::types::array::{ColumnArray, Utf8Buffer};
use crate::types::batch::ColumnDef;
use crate::types::bitmap::NullBitmap;

pub(crate) const COLUMN_ENCODING_MAGIC: &[u8; 4] = b"MXE1";

pub fn encode_column_ex(col: &ColumnArray, offset: usize, count: usize) -> Result<(Vec<u8>, bool)> {
    let mut buf = column_bitmap_prefix(col, offset, count);

    if let Some(encoded) = encode_column_adaptive(col, offset, count) {
        buf.extend_from_slice(COLUMN_ENCODING_MAGIC);
        buf.push(encoded.0.to_u8());
        buf.extend_from_slice(&encoded.1);
        let specialized = matches!(
            encoded.0,
            EncodingType::GorillaXor
                | EncodingType::DeltaOfDelta
                | EncodingType::BitPacked
                | EncodingType::Rle
                | EncodingType::DeltaRle
        );
        return Ok((buf, specialized));
    }

    append_plain_values(&mut buf, col, offset, count);

    Ok((buf, false))
}

pub fn encode_column_plain(col: &ColumnArray, offset: usize, count: usize) -> Result<Vec<u8>> {
    let mut buf = column_bitmap_prefix(col, offset, count);
    append_plain_values(&mut buf, col, offset, count);
    Ok(buf)
}

fn column_bitmap_prefix(col: &ColumnArray, offset: usize, count: usize) -> Vec<u8> {
    let null_flags: Vec<bool> = (offset..offset + count)
        .map(|i| col.bitmap().is_null(i))
        .collect();
    let bitmap = NullBitmap::from_bools(&null_flags);
    let bitmap_bytes = bitmap.to_bytes();

    let mut buf = Vec::new();
    buf.extend_from_slice(&(bitmap_bytes.len() as u32).to_le_bytes());
    buf.extend_from_slice(&bitmap_bytes);
    buf
}

fn append_plain_values(buf: &mut Vec<u8>, col: &ColumnArray, offset: usize, count: usize) {
    for i in offset..offset + count {
        if col.bitmap().is_null(i) {
            continue;
        }
        encode_value(buf, col, i);
    }
}

/// Pick the smaller of two per-block storage candidates: a specialized
/// adaptive encoding stored verbatim, or the plain encoding under zstd.
/// Chaining zstd on top of a bit-packed XOR/DeltaOfDelta stream wastes CPU
/// for little gain, so the two candidates compete instead of stacking.
pub struct StoredColumnEncoding {
    pub data: Vec<u8>,
    pub uncompressed_size: u64,
}

/// Read the adaptive encoding tag from a full encoded column buffer
/// (u32 bitmap_len + bitmap + magic + tag), if present.
pub(crate) fn stored_encoding_tag(buf: &[u8]) -> Option<EncodingType> {
    if buf.len() < 4 + COLUMN_ENCODING_MAGIC.len() + 1 {
        return None;
    }
    let bitmap_len = u32::from_le_bytes(buf[..4].try_into().ok()?) as usize;
    let tag_pos = 4 + bitmap_len + COLUMN_ENCODING_MAGIC.len();
    if tag_pos >= buf.len() {
        return None;
    }
    EncodingType::from_u8(buf[tag_pos]).ok()
}

pub fn choose_stored_column_encoding(
    col: &ColumnArray,
    offset: usize,
    count: usize,
    zstd_level: i32,
    allow_verbatim: bool,
) -> Result<StoredColumnEncoding> {
    let (encoded, specialized) = encode_column_ex(col, offset, count)?;
    if !specialized || !allow_verbatim {
        let uncompressed_size = encoded.len() as u64;
        let data = zstd::bulk::compress(&encoded, zstd_level)
            .map_err(|e| TaibaiError::Storage(format!("zstd compress error: {}", e)))?;
        return Ok(StoredColumnEncoding {
            data,
            uncompressed_size,
        });
    }

    let plain = encode_column_plain(col, offset, count)?;
    let compressed_plain = zstd::bulk::compress(&plain, zstd_level)
        .map_err(|e| TaibaiError::Storage(format!("zstd compress error: {}", e)))?;
    // Fast-decode adaptive layouts (bit unpacking) run an order of magnitude
    // faster than a zstd frame, so they only yield to the zstd(plain)
    // candidate when that one is decisively smaller — a marginal size edge is
    // not worth a ~10x decode regression on scan-heavy reads.
    if stored_encoding_tag(&encoded) == Some(EncodingType::BitPacked) {
        let keep_verbatim = compressed_plain.len() * 5 > encoded.len() * 4;
        return if keep_verbatim {
            Ok(StoredColumnEncoding {
                uncompressed_size: encoded.len() as u64,
                data: encoded,
            })
        } else {
            Ok(StoredColumnEncoding {
                data: compressed_plain,
                uncompressed_size: plain.len() as u64,
            })
        };
    }

    // RLE competes three ways: verbatim (run-fill decode, no zstd pass —
    // wins on long runs where the payload is already tiny), zstd(RLE) (the
    // densest form for short runs), and zstd(plain). Strictly by size.
    // DeltaRle decodes per run (varint parse + fill), fast enough to deserve
    // the same treatment as bit packing: verbatim unless the zstd frame of
    // its own payload is decisively (>=20%) smaller, in which case take the
    // denser of zstd(payload) / zstd(plain).
    if stored_encoding_tag(&encoded) == Some(EncodingType::DeltaRle) {
        let compressed_deltarle = zstd::bulk::compress(&encoded, zstd_level)
            .map_err(|e| TaibaiError::Storage(format!("zstd compress error: {}", e)))?;
        let keep_verbatim = compressed_deltarle.len() * 5 > encoded.len() * 4;
        return if keep_verbatim {
            Ok(StoredColumnEncoding {
                uncompressed_size: encoded.len() as u64,
                data: encoded,
            })
        } else if compressed_deltarle.len() <= compressed_plain.len() {
            Ok(StoredColumnEncoding {
                data: compressed_deltarle,
                uncompressed_size: encoded.len() as u64,
            })
        } else {
            Ok(StoredColumnEncoding {
                data: compressed_plain,
                uncompressed_size: plain.len() as u64,
            })
        };
    }

    if stored_encoding_tag(&encoded) == Some(EncodingType::Rle) {
        let compressed_rle = zstd::bulk::compress(&encoded, zstd_level)
            .map_err(|e| TaibaiError::Storage(format!("zstd compress error: {}", e)))?;
        let encoded_len = encoded.len() as u64;
        let mut best_len = encoded_len as usize;
        let mut best = (encoded, encoded_len);
        if compressed_rle.len() < best_len {
            best_len = compressed_rle.len();
            best = (compressed_rle, encoded_len);
        }
        if compressed_plain.len() < best_len {
            best = (compressed_plain, plain.len() as u64);
        }
        return Ok(StoredColumnEncoding {
            data: best.0,
            uncompressed_size: best.1,
        });
    }

    if compressed_plain.len() >= encoded.len() {
        Ok(StoredColumnEncoding {
            uncompressed_size: encoded.len() as u64,
            data: encoded,
        })
    } else {
        Ok(StoredColumnEncoding {
            data: compressed_plain,
            uncompressed_size: plain.len() as u64,
        })
    }
}

fn encode_column_adaptive(
    col: &ColumnArray,
    offset: usize,
    count: usize,
) -> Option<(EncodingType, Vec<u8>)> {
    if count < 128 {
        return None;
    }

    match col {
        ColumnArray::Int8 { data, bitmap } => {
            encode_signed_numeric_adaptive(data, bitmap, offset, count, |v| *v as i64, 1)
        }
        ColumnArray::Int16 { data, bitmap } => {
            encode_signed_numeric_adaptive(data, bitmap, offset, count, |v| *v as i64, 2)
        }
        ColumnArray::Int32 { data, bitmap } => {
            encode_signed_numeric_adaptive(data, bitmap, offset, count, |v| *v as i64, 4)
        }
        ColumnArray::Int64 { data, bitmap } => {
            encode_signed_numeric_adaptive(data, bitmap, offset, count, |v| *v, 8)
        }
        ColumnArray::Decimal { data, bitmap } => {
            encode_decimal_adaptive(data, bitmap, offset, count)
        }
        ColumnArray::Date { data, bitmap } => {
            encode_signed_numeric_adaptive(data, bitmap, offset, count, |v| *v as i64, 4)
        }
        ColumnArray::Time { data, bitmap } => {
            encode_signed_numeric_adaptive(data, bitmap, offset, count, |v| *v, 8)
        }
        ColumnArray::Timestamp { data, bitmap } => {
            encode_signed_numeric_adaptive(data, bitmap, offset, count, |v| *v, 8)
        }
        ColumnArray::Interval { data, bitmap } => {
            encode_signed_numeric_adaptive(data, bitmap, offset, count, |v| *v, 8)
        }
        ColumnArray::UInt8 { data, bitmap } => {
            encode_unsigned_numeric_adaptive(data, bitmap, offset, count, |v| *v as u64, 1)
        }
        ColumnArray::UInt16 { data, bitmap } => {
            encode_unsigned_numeric_adaptive(data, bitmap, offset, count, |v| *v as u64, 2)
        }
        ColumnArray::UInt32 { data, bitmap } => {
            encode_unsigned_numeric_adaptive(data, bitmap, offset, count, |v| *v as u64, 4)
        }
        ColumnArray::UInt64 { data, bitmap } => {
            encode_unsigned_numeric_adaptive(data, bitmap, offset, count, |v| *v, 8)
        }
        ColumnArray::Float32 { data, bitmap } => {
            encode_float_adaptive(data, bitmap, offset, count, 32, |v| v.to_bits() as u64)
        }
        ColumnArray::Float64 { data, bitmap } => {
            encode_float_adaptive(data, bitmap, offset, count, 64, |v| v.to_bits())
        }
        ColumnArray::VarChar { data, bitmap } => {
            encode_varchar_dictionary_adaptive(data, bitmap, offset, count)
        }
        _ => None,
    }
}

fn encode_signed_numeric_adaptive<T, F>(
    data: &[T],
    bitmap: &NullBitmap,
    offset: usize,
    count: usize,
    to_i64: F,
    plain_width: usize,
) -> Option<(EncodingType, Vec<u8>)>
where
    F: Fn(&T) -> i64,
{
    let values: Vec<i64> = (offset..offset + count)
        .filter(|&i| !bitmap.is_null(i))
        .map(|i| to_i64(&data[i]))
        .collect();
    choose_numeric_encoding(&values, plain_width)
}

fn encode_unsigned_numeric_adaptive<T, F>(
    data: &[T],
    bitmap: &NullBitmap,
    offset: usize,
    count: usize,
    to_u64: F,
    plain_width: usize,
) -> Option<(EncodingType, Vec<u8>)>
where
    F: Fn(&T) -> u64,
{
    let mut values = Vec::new();
    for (i, item) in data.iter().enumerate().skip(offset).take(count) {
        if bitmap.is_null(i) {
            continue;
        }
        let v = to_u64(item);
        if v > i64::MAX as u64 {
            return None;
        }
        values.push(v as i64);
    }
    choose_numeric_encoding(&values, plain_width)
}

fn encode_float_adaptive<T, F>(
    data: &[T],
    bitmap: &NullBitmap,
    offset: usize,
    count: usize,
    width: u32,
    to_bits: F,
) -> Option<(EncodingType, Vec<u8>)>
where
    F: Fn(&T) -> u64,
{
    let bits: Vec<u64> = (offset..offset + count)
        .filter(|&i| !bitmap.is_null(i))
        .map(|i| to_bits(&data[i]))
        .collect();
    if bits.len() < 128 {
        return None;
    }
    let plain_size = bits.len() * (width as usize / 8);
    let payload = encode_gorilla_xor_payload(&bits, width);
    if payload.len() + 5 < plain_size {
        Some((EncodingType::GorillaXor, payload))
    } else {
        None
    }
}

fn encode_decimal_adaptive(
    data: &[i64],
    bitmap: &NullBitmap,
    offset: usize,
    count: usize,
) -> Option<(EncodingType, Vec<u8>)> {
    let mut best = encode_signed_numeric_adaptive(data, bitmap, offset, count, |v| *v, 8);
    let bits: Vec<u64> = (offset..offset + count)
        .filter(|&i| !bitmap.is_null(i))
        .map(|i| data[i] as u64)
        .collect();
    if bits.len() >= 128 {
        let plain_size = bits.len() * 8;
        let payload = encode_gorilla_xor_payload(&bits, 64);
        if payload.len() + 5 < plain_size
            && best
                .as_ref()
                .map(|(_, existing)| payload.len() < existing.len())
                .unwrap_or(true)
        {
            best = Some((EncodingType::GorillaXor, payload));
        }
    }
    best
}

fn choose_numeric_encoding(values: &[i64], plain_width: usize) -> Option<(EncodingType, Vec<u8>)> {
    if values.len() < 128 {
        return None;
    }
    let plain_size = values.len().saturating_mul(plain_width);

    let mut best: Option<(EncodingType, Vec<u8>)> = None;
    let rle = encode_i64_rle_payload(values);
    if rle.len() + 5 < plain_size {
        best = Some((EncodingType::Rle, rle));
    }

    // Bit packing is not gated on sortedness: unlike varint Delta/DoD it
    // pairs the deltas with a per-block minimum, so locally-steady columns
    // (tick prices, volumes) compact well even when they oscillate.
    if let Some(packed) = encode_i64_bitpacked_payload(values) {
        if (packed.len() + 5 < plain_size || plain_width == 8 && packed.len() < plain_size)
            && best
                .as_ref()
                .map(|(_, payload)| packed.len() < payload.len())
                .unwrap_or(true)
        {
            best = Some((EncodingType::BitPacked, packed));
        }
    }

    let delta_rle = encode_i64_delta_rle_payload(values);
    if (delta_rle.len() + 5 < plain_size || plain_width == 8 && delta_rle.len() < plain_size)
        && best
            .as_ref()
            .map(|(_, payload)| delta_rle.len() < payload.len())
            .unwrap_or(true)
    {
        best = Some((EncodingType::DeltaRle, delta_rle));
    }

    if values.windows(2).all(|w| w[0] <= w[1]) {
        let delta = encode_i64_delta_payload(values);
        if (delta.len() + 5 < plain_size || plain_width == 8 && delta.len() < plain_size)
            && best
                .as_ref()
                .map(|(_, payload)| delta.len() < payload.len())
                .unwrap_or(true)
        {
            best = Some((EncodingType::Delta, delta));
        }

        let dod = encode_i64_dod_payload(values);
        if (dod.len() + 5 < plain_size || plain_width == 8 && dod.len() < plain_size)
            && best
                .as_ref()
                .map(|(_, payload)| dod.len() < payload.len())
                .unwrap_or(true)
        {
            best = Some((EncodingType::DeltaOfDelta, dod));
        }
    }

    best
}

fn encode_i64_rle_payload(values: &[i64]) -> Vec<u8> {
    let mut runs = Vec::new();
    let mut current = values[0];
    let mut len = 1u32;
    for &value in &values[1..] {
        if value == current && len < u32::MAX {
            len += 1;
        } else {
            runs.push((current, len));
            current = value;
            len = 1;
        }
    }
    runs.push((current, len));

    let mut out = Vec::with_capacity(4 + runs.len() * 12);
    out.extend_from_slice(&(runs.len() as u32).to_le_bytes());
    for (value, len) in runs {
        out.extend_from_slice(&value.to_le_bytes());
        out.extend_from_slice(&len.to_le_bytes());
    }
    out
}

/// RLE over run-to-run deltas: each run stores `varint(zigzag(value -
/// prev_value))` plus `varint(len)` — 2~4 bytes per run on run-heavy data
/// (clustered tick prices) versus the fixed 12 of the plain RLE above, dense
/// enough to win the stored choice verbatim and skip the zstd pass entirely.
/// The per-run (not per-value) varint parse keeps decode cheap even when runs
/// are short. Worst case (random 64-bit jumps) is 15 bytes/run, so the plain
/// RLE candidate stays in the competition for that shape.
pub(crate) fn encode_i64_delta_rle_payload(values: &[i64]) -> Vec<u8> {
    let mut runs = Vec::new();
    let mut current = values[0];
    let mut len = 1u64;
    for &value in &values[1..] {
        if value == current {
            len += 1;
        } else {
            runs.push((current, len));
            current = value;
            len = 1;
        }
    }
    runs.push((current, len));

    let mut out = Vec::with_capacity(4 + runs.len() * 3);
    out.extend_from_slice(&(runs.len() as u32).to_le_bytes());
    let mut prev = 0i64;
    for (value, len) in runs {
        write_varint(&mut out, zigzag_encode(value.wrapping_sub(prev)));
        write_varint(&mut out, len);
        prev = value;
    }
    out
}

pub(crate) fn decode_i64_delta_rle_payload(
    payload: &[u8],
    expected_rows: usize,
) -> Result<Vec<i64>> {
    let mut cursor = Cursor::new(payload);
    let run_count = read_u32_le(&mut cursor)? as usize;
    let mut values = Vec::new();
    let mut prev = 0i64;
    for _ in 0..run_count {
        let delta = zigzag_decode(read_varint(&mut cursor)?);
        prev = prev.wrapping_add(delta);
        let len = read_varint(&mut cursor)? as usize;
        // Same pre-expansion bound as the plain RLE decoder.
        if values.len().saturating_add(len) > expected_rows {
            return Err(TaibaiError::Storage(format!(
                "delta-RLE run expansion reaches {} values, exceeding the row group's {}",
                values.len().saturating_add(len),
                expected_rows
            )));
        }
        values.extend(std::iter::repeat_n(prev, len));
    }
    Ok(values)
}

fn encode_i64_delta_payload(values: &[i64]) -> Vec<u8> {
    let mut out = Vec::with_capacity(12 + values.len() * 2);
    out.extend_from_slice(&(values.len() as u32).to_le_bytes());
    out.extend_from_slice(&values[0].to_le_bytes());
    let mut prev = values[0];
    for &value in &values[1..] {
        write_varint(&mut out, zigzag_encode(value - prev));
        prev = value;
    }
    out
}

fn encode_i64_dod_payload(values: &[i64]) -> Vec<u8> {
    let mut out = Vec::with_capacity(12 + values.len() * 2);
    out.extend_from_slice(&(values.len() as u32).to_le_bytes());
    out.extend_from_slice(&values[0].to_le_bytes());
    let mut prev = values[0];
    let mut delta = 0i64;
    for (i, &value) in values.iter().enumerate().skip(1) {
        let next_delta = value.wrapping_sub(prev);
        let encoded = if i == 1 {
            next_delta
        } else {
            next_delta.wrapping_sub(delta)
        };
        write_varint(&mut out, zigzag_encode(encoded));
        delta = next_delta;
        prev = value;
    }
    out
}

pub(crate) const BITPACK_BLOCK_SIZE: usize = 128;

/// Delta + frame-of-reference + bit packing (Parquet DELTA_BINARY_PACKED
/// shape): each block of consecutive deltas is offset by its minimum and
/// packed at a per-block fixed bit width. The decode side is a straight
/// unpack-accumulate loop — no varint branching and, because the block is
/// stored verbatim, no zstd pass — which is the point of competing this
/// candidate against the zstd(plain) layout on size.
fn encode_i64_bitpacked_payload(values: &[i64]) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(16 + values.len());
    out.extend_from_slice(&(values.len() as u32).to_le_bytes());
    out.extend_from_slice(&values[0].to_le_bytes());
    out.extend_from_slice(&(BITPACK_BLOCK_SIZE as u32).to_le_bytes());

    let mut block_start = 1usize;
    while block_start < values.len() {
        let block_end = (block_start + BITPACK_BLOCK_SIZE).min(values.len());
        let mut prev = values[block_start - 1];
        let mut min = i64::MAX;
        let mut max = i64::MIN;
        for &value in &values[block_start..block_end] {
            let delta = value.wrapping_sub(prev);
            prev = value;
            min = min.min(delta);
            max = max.max(delta);
        }
        let range = max.wrapping_sub(min) as u64;
        let width = 64 - range.leading_zeros(); // 0 when every delta equals min
        if width > 56 {
            // Wider blocks cannot use the 64-bit refill accumulator below and
            // at >=7.125 bytes/value they cannot beat the 8-byte plain layout.
            return None;
        }
        out.push(width as u8);
        out.extend_from_slice(&min.to_le_bytes());

        let packed_len = ((block_end - block_start) * width as usize + 7) / 8;
        let packed_start = out.len();
        out.resize(packed_start + packed_len, 0);
        let mut acc = 0u64;
        let mut bits = 0u32;
        let mut pos = packed_start;
        prev = values[block_start - 1];
        for &value in &values[block_start..block_end] {
            let delta = value.wrapping_sub(prev).wrapping_sub(min);
            prev = value;
            // `bits` stays below 8 between values (the flush loop drains full
            // bytes), so the shifted delta never exceeds 64 bits for width<=56.
            acc |= (delta as u64) << bits;
            bits += width;
            while bits >= 8 {
                out[pos] = acc as u8;
                pos += 1;
                acc >>= 8;
                bits -= 8;
            }
        }
        if bits > 0 {
            out[pos] = acc as u8;
        }
        block_start = block_end;
    }
    Some(out)
}

pub(crate) fn decode_i64_bitpacked_payload(payload: &[u8]) -> Result<Vec<i64>> {
    let mut cursor = Cursor::new(payload);
    let count = read_u32_le(&mut cursor)? as usize;
    if count == 0 {
        return Ok(Vec::new());
    }
    let mut base_buf = [0u8; 8];
    cursor.read_exact(&mut base_buf)?;
    let mut prev = i64::from_le_bytes(base_buf);
    let mut block_buf = [0u8; 4];
    cursor.read_exact(&mut block_buf)?;
    let block_size = u32::from_le_bytes(block_buf) as usize;
    if block_size == 0 {
        return Err(TaibaiError::Storage(
            "bitpacked block size must be positive".into(),
        ));
    }

    let mut values = Vec::with_capacity(count);
    values.push(prev);
    let bitstream = &payload[cursor.position() as usize..];
    let mut pos = 0usize;
    let mut remaining = count - 1;
    while remaining > 0 {
        let len = remaining.min(block_size);
        if pos + 9 > bitstream.len() {
            return Err(TaibaiError::Storage(
                "bitpacked block header truncated".into(),
            ));
        }
        let width = bitstream[pos] as u32;
        pos += 1;
        let min = i64::from_le_bytes(bitstream[pos..pos + 8].try_into().unwrap());
        pos += 8;
        if width > 56 {
            return Err(TaibaiError::Storage(
                "bitpacked width exceeds decoder capability".into(),
            ));
        }
        if width == 0 {
            // Constant-delta block: an arithmetic sequence, no bitstream.
            for _ in 0..len {
                prev = prev.wrapping_add(min);
                values.push(prev);
            }
        } else {
            let packed_len = (len * width as usize + 7) / 8;
            if pos + packed_len > bitstream.len() {
                return Err(TaibaiError::Storage("bitpacked block truncated".into()));
            }
            unpack_bitpacked_block(
                &mut values,
                &mut prev,
                &bitstream[pos..pos + packed_len],
                len,
                width,
                min,
            );
            pos += packed_len;
        }
        remaining -= len;
    }
    Ok(values)
}

/// Unpack `len` fixed-width offsets (LSB-first byte stream) and append
/// `prev + min + offset` for each.
///
/// Widths that divide a byte (8/16/32) read straight from the stream. Other
/// widths pull groups of values out of one 64-bit funnel-shift window (two
/// unaligned loads), which removes the per-value refill branches and bounds
/// checks; a group of g values spans g*width <= 64 bits, so g is 4 up to
/// width 16, 2 up to 32, and 1 beyond (the stream ends in the 64-bit window
/// only when width itself does). Anything a window cannot cover — the tail
/// values and windows that would overrun the slice — falls back to the
/// byte-granularity refill loop, whose shifts stay below 56 bits.
fn unpack_bitpacked_block(
    values: &mut Vec<i64>,
    prev: &mut i64,
    packed: &[u8],
    len: usize,
    width: u32,
    min: i64,
) {
    if width == 8 {
        for &byte in &packed[..len] {
            *prev = prev.wrapping_add(min).wrapping_add(byte as i64);
            values.push(*prev);
        }
        return;
    }
    if width == 16 {
        for chunk in packed[..len * 2].chunks_exact(2) {
            let offset = u16::from_le_bytes(chunk.try_into().unwrap()) as i64;
            *prev = prev.wrapping_add(min).wrapping_add(offset);
            values.push(*prev);
        }
        return;
    }
    if width == 32 {
        for chunk in packed[..len * 4].chunks_exact(4) {
            let offset = u32::from_le_bytes(chunk.try_into().unwrap()) as i64;
            *prev = prev.wrapping_add(min).wrapping_add(offset);
            values.push(*prev);
        }
        return;
    }

    let group: usize = if width <= 16 {
        4
    } else if width <= 32 {
        2
    } else {
        1
    };
    let mask = (1u64 << width) - 1;
    let group_bits = group * width as usize;
    let mut bit_pos = 0usize;
    let mut done = 0usize;
    while done + group <= len {
        let Some(window) = bit_window64(packed, bit_pos / 8, (bit_pos % 8) as u32) else {
            break;
        };
        match group {
            4 => {
                let d0 = min.wrapping_add((window & mask) as i64);
                let d1 = min.wrapping_add(((window >> width) & mask) as i64);
                let d2 = min.wrapping_add(((window >> (width * 2)) & mask) as i64);
                let d3 = min.wrapping_add(((window >> (width * 3)) & mask) as i64);
                *prev = prev.wrapping_add(d0);
                values.push(*prev);
                *prev = prev.wrapping_add(d1);
                values.push(*prev);
                *prev = prev.wrapping_add(d2);
                values.push(*prev);
                *prev = prev.wrapping_add(d3);
                values.push(*prev);
            }
            2 => {
                let d0 = min.wrapping_add((window & mask) as i64);
                let d1 = min.wrapping_add(((window >> width) & mask) as i64);
                *prev = prev.wrapping_add(d0);
                values.push(*prev);
                *prev = prev.wrapping_add(d1);
                values.push(*prev);
            }
            _ => {
                let d0 = min.wrapping_add((window & mask) as i64);
                *prev = prev.wrapping_add(d0);
                values.push(*prev);
            }
        }
        done += group;
        bit_pos += group_bits;
    }
    if done == len {
        return;
    }

    // Tail: resume mid-stream with the partial byte folded into the
    // accumulator, then refill a byte at a time as before.
    let mut acc = (packed[bit_pos / 8] >> (bit_pos % 8) as u32) as u64;
    let mut bits = 8 - (bit_pos % 8) as u32;
    let mut pos = bit_pos / 8 + 1;
    for _ in done..len {
        while bits < width {
            acc |= (packed[pos] as u64) << bits;
            pos += 1;
            bits += 8;
        }
        let offset = acc & mask;
        acc >>= width;
        bits -= width;
        *prev = prev.wrapping_add(min).wrapping_add(offset as i64);
        values.push(*prev);
    }
}

/// 64 bits of the stream starting at bit offset `byte_pos*8 + shift`, funnel-
/// shifted from two unaligned loads. Returns None when the (rare) tail window
/// would read past the packed slice.
#[inline]
fn bit_window64(packed: &[u8], byte_pos: usize, shift: u32) -> Option<u64> {
    if byte_pos + 8 > packed.len() {
        return None;
    }
    let lo = u64::from_le_bytes(packed[byte_pos..byte_pos + 8].try_into().unwrap());
    if shift == 0 {
        return Some(lo);
    }
    if byte_pos + 16 > packed.len() {
        return None;
    }
    let hi = u64::from_le_bytes(packed[byte_pos + 8..byte_pos + 16].try_into().unwrap());
    Some((lo >> shift) | (hi << (64 - shift)))
}

pub(crate) fn decode_i64_dod_payload(payload: &[u8]) -> Result<Vec<i64>> {
    let mut cursor = Cursor::new(payload);
    let count = read_u32_le(&mut cursor)? as usize;
    if count == 0 {
        return Ok(Vec::new());
    }
    let mut base_buf = [0u8; 8];
    cursor.read_exact(&mut base_buf)?;
    let mut prev = i64::from_le_bytes(base_buf);
    let mut values = Vec::with_capacity(count);
    values.push(prev);
    if count == 1 {
        return Ok(values);
    }
    let mut delta = zigzag_decode(read_varint(&mut cursor)?);
    prev = prev.wrapping_add(delta);
    values.push(prev);
    for _ in 2..count {
        let dod = zigzag_decode(read_varint(&mut cursor)?);
        delta = delta.wrapping_add(dod);
        prev = prev.wrapping_add(delta);
        values.push(prev);
    }
    Ok(values)
}

fn encode_gorilla_xor_payload(bits: &[u64], width: u32) -> Vec<u8> {
    let mut out = Vec::with_capacity(8 + bits.len() * 2);
    out.extend_from_slice(&(bits.len() as u32).to_le_bytes());
    if width == 32 {
        out.extend_from_slice(&(bits[0] as u32).to_le_bytes());
    } else {
        out.extend_from_slice(&bits[0].to_le_bytes());
    }

    let mut writer = BitWriter::with_capacity(bits.len() * 2);
    let mut prev = bits[0];
    let mut prev_leading = width;
    let mut prev_trailing = width;
    for &cur in &bits[1..] {
        let xor = cur ^ prev;
        if xor == 0 {
            writer.write_bit(false);
            continue;
        }
        writer.write_bit(true);
        let leading = (xor.leading_zeros() - (64 - width)).min(31);
        let trailing = xor.trailing_zeros().min(width - leading);
        let meaningful_len = width - leading - trailing;
        if leading >= prev_leading && trailing >= prev_trailing {
            writer.write_bit(false);
            writer.write_bits(xor >> prev_trailing, width - prev_leading - prev_trailing);
        } else {
            writer.write_bit(true);
            writer.write_bits(leading as u64, 5);
            writer.write_bits((meaningful_len - 1) as u64, 6);
            writer.write_bits(xor >> trailing, meaningful_len);
            prev_leading = leading;
            prev_trailing = trailing;
        }
        prev = cur;
    }
    out.extend_from_slice(&writer.finish());
    out
}

pub(crate) fn decode_gorilla_xor_payload(payload: &[u8], width: u32) -> Result<Vec<u64>> {
    let mut cursor = Cursor::new(payload);
    let count = read_u32_le(&mut cursor)? as usize;
    if count == 0 {
        return Ok(Vec::new());
    }
    let first = if width == 32 {
        let mut b = [0u8; 4];
        cursor.read_exact(&mut b)?;
        u32::from_le_bytes(b) as u64
    } else {
        let mut b = [0u8; 8];
        cursor.read_exact(&mut b)?;
        u64::from_le_bytes(b)
    };
    let bitstream = &payload[cursor.position() as usize..];
    let mut reader = BitReader::new(bitstream);
    let mut values = Vec::with_capacity(count);
    let mut prev = first;
    values.push(prev);
    let mut prev_leading = width;
    let mut prev_trailing = width;
    for _ in 1..count {
        let xor = if !reader.read_bit()? {
            0
        } else if !reader.read_bit()? {
            let len = width
                .saturating_sub(prev_leading)
                .saturating_sub(prev_trailing);
            reader.read_bits(len)? << prev_trailing
        } else {
            let leading = reader.read_bits(5)? as u32;
            let meaningful_len = (reader.read_bits(6)? as u32) + 1;
            let trailing = width.saturating_sub(leading).saturating_sub(meaningful_len);
            prev_leading = leading;
            prev_trailing = trailing;
            reader.read_bits(meaningful_len)? << trailing
        };
        prev ^= xor;
        values.push(prev);
    }
    Ok(values)
}

fn encode_varchar_dictionary_adaptive(
    data: &Utf8Buffer,
    bitmap: &NullBitmap,
    offset: usize,
    count: usize,
) -> Option<(EncodingType, Vec<u8>)> {
    let mut dict = Vec::<&str>::new();
    let mut index_map = std::collections::HashMap::<&str, u32>::new();
    let mut indices = Vec::new();
    let mut plain_size = 0usize;

    for (i, item) in data.iter().enumerate().skip(offset).take(count) {
        if bitmap.is_null(i) {
            continue;
        }
        let value = item;
        plain_size += 4 + value.len();
        let idx = if let Some(&idx) = index_map.get(value) {
            idx
        } else {
            if dict.len() >= u32::MAX as usize {
                return None;
            }
            let idx = dict.len() as u32;
            dict.push(value);
            index_map.insert(value, idx);
            idx
        };
        indices.push(idx);
    }

    if indices.len() < 128 || dict.len() >= indices.len() / 2 {
        return None;
    }

    let mut out = Vec::new();
    out.extend_from_slice(&(dict.len() as u32).to_le_bytes());
    for value in dict {
        out.extend_from_slice(&(value.len() as u32).to_le_bytes());
        out.extend_from_slice(value.as_bytes());
    }
    out.extend_from_slice(&(indices.len() as u32).to_le_bytes());
    for idx in indices {
        out.extend_from_slice(&idx.to_le_bytes());
    }

    if out.len() + 5 < plain_size {
        Some((EncodingType::Dictionary, out))
    } else {
        None
    }
}

pub fn encode_value(buf: &mut Vec<u8>, col: &ColumnArray, idx: usize) {
    match col {
        ColumnArray::Selected { .. } | ColumnArray::Rle { .. } => {
            encode_value(buf, col.flattened(), idx)
        }
        ColumnArray::Dictionary { .. } => encode_value(buf, &col.materialize_varchar(), idx),
        ColumnArray::Bool { data, .. } => buf.push(data[idx]),
        ColumnArray::Int8 { data, .. } => buf.push(data[idx] as u8),
        ColumnArray::Int16 { data, .. } => buf.extend_from_slice(&data[idx].to_le_bytes()),
        ColumnArray::Int32 { data, .. } => buf.extend_from_slice(&data[idx].to_le_bytes()),
        ColumnArray::Int64 { data, .. } => buf.extend_from_slice(&data[idx].to_le_bytes()),
        ColumnArray::UInt8 { data, .. } => buf.push(data[idx]),
        ColumnArray::UInt16 { data, .. } => buf.extend_from_slice(&data[idx].to_le_bytes()),
        ColumnArray::UInt32 { data, .. } => buf.extend_from_slice(&data[idx].to_le_bytes()),
        ColumnArray::UInt64 { data, .. } => buf.extend_from_slice(&data[idx].to_le_bytes()),
        ColumnArray::Float32 { data, .. } => buf.extend_from_slice(&data[idx].to_le_bytes()),
        ColumnArray::Float64 { data, .. } => buf.extend_from_slice(&data[idx].to_le_bytes()),
        ColumnArray::Decimal { data, .. } => buf.extend_from_slice(&data[idx].to_le_bytes()),
        ColumnArray::Date { data, .. } => buf.extend_from_slice(&data[idx].to_le_bytes()),
        ColumnArray::Time { data, .. } => buf.extend_from_slice(&data[idx].to_le_bytes()),
        ColumnArray::Timestamp { data, .. } => buf.extend_from_slice(&data[idx].to_le_bytes()),
        ColumnArray::Interval { data, .. } => buf.extend_from_slice(&data[idx].to_le_bytes()),
        ColumnArray::VarChar { data, .. } => {
            buf.extend_from_slice(&(data[idx].len() as u32).to_le_bytes());
            buf.extend_from_slice(data[idx].as_bytes());
        }
        ColumnArray::Blob { data, .. } => {
            buf.extend_from_slice(&(data[idx].len() as u32).to_le_bytes());
            buf.extend_from_slice(&data[idx]);
        }
        ColumnArray::Vector { data, .. } => {
            let v = &data[idx];
            buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
            for f in v {
                buf.extend_from_slice(&f.to_le_bytes());
            }
        }
    }
}

pub fn decode_column(
    data: &[u8],
    row_count: usize,
    col_def: &ColumnDef,
    allow_dictionary: bool,
) -> Result<ColumnArray> {
    decode_column_with_runs(data, row_count, col_def, allow_dictionary, false)
}

/// `allow_runs` keeps Rle/DeltaRle columns in their run form
/// ([`ColumnArray::Rle`]) instead of expanding them — the run-aware
/// aggregate consumes runs directly, and every other consumer flattens
/// lazily.
pub fn decode_column_with_runs(
    data: &[u8],
    row_count: usize,
    col_def: &ColumnDef,
    allow_dictionary: bool,
    allow_runs: bool,
) -> Result<ColumnArray> {
    let mut cursor = Cursor::new(data);
    let bitmap_len = read_u32_le(&mut cursor)? as usize;
    let mut bitmap_data = vec![0u8; bitmap_len];
    cursor.read_exact(&mut bitmap_data)?;
    let bitmap = NullBitmap::from_bytes(&bitmap_data, row_count);

    let remaining_data = &data[4 + bitmap_len..];
    if remaining_data.starts_with(COLUMN_ENCODING_MAGIC) {
        return decode_adaptive_column_with_runs(
            remaining_data,
            row_count,
            col_def,
            bitmap,
            allow_dictionary,
            allow_runs,
        );
    }
    let mut dc = Cursor::new(remaining_data);

    macro_rules! read_fixed {
        ($ty:ty, $size:expr, $variant:ident) => {{
            // Null-free fast path: the payload is exactly row_count * $size
            // little-endian values, so decode degrades to one bulk conversion
            // (a per-word load on little-endian hosts) instead of a
            // per-value Cursor::read_exact with bounds checks.
            if bitmap.null_count() == 0
                && remaining_data.len() - dc.position() as usize >= row_count * $size
            {
                let start = dc.position() as usize;
                let raw = &remaining_data[start..start + row_count * $size];
                let mut vals = Vec::with_capacity(row_count);
                vals.extend(
                    raw.chunks_exact($size)
                        .map(|b| <$ty>::from_le_bytes(b.try_into().unwrap())),
                );
                dc.set_position((start + row_count * $size) as u64);
                ColumnArray::$variant {
                    data: vals.into(),
                    bitmap: bitmap.clone(),
                }
            } else {
                let mut vals = Vec::with_capacity(row_count);
                for i in 0..row_count {
                    if bitmap.is_null(i) {
                        vals.push(<$ty>::default());
                    } else {
                        let mut b = [0u8; $size];
                        dc.read_exact(&mut b)
                            .map_err(|e| TaibaiError::Storage(format!("read error: {}", e)))?;
                        vals.push(<$ty>::from_le_bytes(b));
                    }
                }
                ColumnArray::$variant {
                    data: vals.into(),
                    bitmap: bitmap.clone(),
                }
            }
        }};
    }

    let col = match col_def.data_type {
        DataType::Bool => read_fixed!(u8, 1, Bool),
        DataType::Int8 => read_fixed!(i8, 1, Int8),
        DataType::Int16 => read_fixed!(i16, 2, Int16),
        DataType::Int32 => read_fixed!(i32, 4, Int32),
        DataType::Int64 => read_fixed!(i64, 8, Int64),
        DataType::UInt8 => read_fixed!(u8, 1, UInt8),
        DataType::UInt16 => read_fixed!(u16, 2, UInt16),
        DataType::UInt32 => read_fixed!(u32, 4, UInt32),
        DataType::UInt64 => read_fixed!(u64, 8, UInt64),
        DataType::Float32 => read_fixed!(f32, 4, Float32),
        DataType::Float64 => read_fixed!(f64, 8, Float64),
        DataType::Decimal => read_fixed!(i64, 8, Decimal),
        DataType::Date => read_fixed!(i32, 4, Date),
        DataType::Time => read_fixed!(i64, 8, Time),
        DataType::Timestamp => read_fixed!(i64, 8, Timestamp),
        DataType::Interval => read_fixed!(i64, 8, Interval),
        DataType::VarChar => {
            // Decode straight into the column's UTF-8 arena: one contiguous
            // buffer + offsets, zero per-row String allocations.
            let mut buffer = Utf8Buffer::with_capacity(row_count);
            let mut pos = dc.position() as usize;
            for i in 0..row_count {
                if bitmap.is_null(i) {
                    buffer.push_str("");
                    continue;
                }
                if pos + 4 > remaining_data.len() {
                    return Err(TaibaiError::Storage(
                        "varchar decode: truncated length".into(),
                    ));
                }
                let len =
                    u32::from_le_bytes(remaining_data[pos..pos + 4].try_into().unwrap()) as usize;
                pos += 4;
                if pos + len > remaining_data.len() {
                    return Err(TaibaiError::Storage(
                        "varchar decode: truncated value".into(),
                    ));
                }
                let raw = &remaining_data[pos..pos + len];
                match std::str::from_utf8(raw) {
                    Ok(value) => buffer.push_str(value),
                    Err(_) => {
                        let owned = String::from_utf8_lossy(raw).into_owned();
                        buffer.push_str(&owned);
                    }
                }
                pos += len;
            }
            dc.set_position(pos as u64);
            ColumnArray::VarChar {
                data: buffer,
                bitmap: bitmap.clone(),
            }
        }
        DataType::Blob => {
            let mut vals = Vec::with_capacity(row_count);
            for i in 0..row_count {
                if bitmap.is_null(i) {
                    vals.push(Vec::new());
                } else {
                    let len = read_u32_le(&mut dc).unwrap_or(0) as usize;
                    let mut v = vec![0u8; len];
                    dc.read_exact(&mut v).unwrap_or(());
                    vals.push(v);
                }
            }
            ColumnArray::Blob {
                data: vals.into(),
                bitmap: bitmap.clone(),
            }
        }
        DataType::Vector => {
            let mut vals = Vec::with_capacity(row_count);
            for i in 0..row_count {
                if bitmap.is_null(i) {
                    vals.push(Vec::new());
                } else {
                    let count = read_u32_le(&mut dc).unwrap_or(0) as usize;
                    let mut v = Vec::with_capacity(count);
                    for _ in 0..count {
                        let mut b = [0u8; 4];
                        dc.read_exact(&mut b).unwrap_or(());
                        v.push(f32::from_le_bytes(b));
                    }
                    vals.push(v);
                }
            }
            ColumnArray::Vector {
                data: vals,
                bitmap: bitmap.clone(),
            }
        }
    };

    Ok(col)
}

pub(crate) fn decode_adaptive_column(
    data: &[u8],
    row_count: usize,
    col_def: &ColumnDef,
    bitmap: NullBitmap,
    allow_dictionary: bool,
) -> Result<ColumnArray> {
    decode_adaptive_column_with_runs(data, row_count, col_def, bitmap, allow_dictionary, false)
}

pub(crate) fn decode_adaptive_column_with_runs(
    data: &[u8],
    row_count: usize,
    col_def: &ColumnDef,
    bitmap: NullBitmap,
    allow_dictionary: bool,
    allow_runs: bool,
) -> Result<ColumnArray> {
    if data.len() < COLUMN_ENCODING_MAGIC.len() + 1 {
        return Err(TaibaiError::Storage(
            "adaptive column header truncated".into(),
        ));
    }
    let encoding = EncodingType::from_u8(data[COLUMN_ENCODING_MAGIC.len()])?;
    let payload = &data[COLUMN_ENCODING_MAGIC.len() + 1..];
    if std::env::var("TAIBAI_ENC_TRACE").as_deref() == Ok("1") {
        eprintln!(
            "DBGENC col={} enc={:?} rows={} payload_len={}",
            col_def.name,
            encoding,
            row_count,
            payload.len()
        );
    }

    match encoding {
        EncodingType::Rle => {
            if allow_runs && crate::storage::segment::column_sum_supported(col_def.data_type) {
                let runs = parse_i64_runs_plain_rle(payload)?;
                return build_run_column(runs, row_count, col_def, bitmap);
            }
            let values = decode_i64_rle_payload(payload, row_count)?;
            build_numeric_column_from_i64(values, row_count, col_def, bitmap)
        }
        EncodingType::DeltaRle => {
            if allow_runs && crate::storage::segment::column_sum_supported(col_def.data_type) {
                let runs = parse_i64_runs_delta_rle(payload)?;
                return build_run_column(runs, row_count, col_def, bitmap);
            }
            let values = decode_i64_delta_rle_payload(payload, row_count)?;
            build_numeric_column_from_i64(values, row_count, col_def, bitmap)
        }
        EncodingType::Delta => {
            let values = decode_i64_delta_payload(payload)?;
            build_numeric_column_from_i64(values, row_count, col_def, bitmap)
        }
        EncodingType::DeltaOfDelta => {
            let values = decode_i64_dod_payload(payload)?;
            build_numeric_column_from_i64(values, row_count, col_def, bitmap)
        }
        EncodingType::BitPacked => {
            let values = decode_i64_bitpacked_payload(payload)?;
            build_numeric_column_from_i64(values, row_count, col_def, bitmap)
        }
        EncodingType::GorillaXor => {
            let width = match col_def.data_type {
                DataType::Float32 => 32,
                DataType::Float64 | DataType::Decimal => 64,
                other => {
                    return Err(TaibaiError::Storage(format!(
                        "gorilla xor encoding cannot decode {}",
                        other.display_name()
                    )))
                }
            };
            let bits = decode_gorilla_xor_payload(payload, width)?;
            build_column_from_bits(bits, row_count, col_def, bitmap)
        }
        EncodingType::Dictionary if col_def.data_type == DataType::VarChar => {
            expand_varchar_dictionary(payload, row_count, &bitmap, allow_dictionary)
        }
        other => Err(TaibaiError::Storage(format!(
            "unsupported adaptive column encoding {:?} for {}",
            other,
            col_def.data_type.display_name()
        ))),
    }
}

/// Parse plain-Rle runs without expanding them: `(value, len)` pairs.
fn parse_i64_runs_plain_rle(payload: &[u8]) -> Result<Vec<(i64, u32)>> {
    let mut cursor = Cursor::new(payload);
    let run_count = read_u32_le(&mut cursor)? as usize;
    let mut runs = Vec::with_capacity(run_count);
    for _ in 0..run_count {
        let mut value_buf = [0u8; 8];
        cursor.read_exact(&mut value_buf)?;
        let value = i64::from_le_bytes(value_buf);
        let len = read_u32_le(&mut cursor)?;
        runs.push((value, len));
    }
    Ok(runs)
}

/// Parse DeltaRle runs without expanding them: varint(zigzag delta) values.
fn parse_i64_runs_delta_rle(payload: &[u8]) -> Result<Vec<(i64, u32)>> {
    let mut cursor = Cursor::new(payload);
    let run_count = read_u32_le(&mut cursor)? as usize;
    let mut runs = Vec::with_capacity(run_count);
    let mut prev = 0i64;
    for _ in 0..run_count {
        let delta = crate::storage::codec::io_util::zigzag_decode(read_varint(&mut cursor)?);
        prev = prev.wrapping_add(delta);
        let len = read_varint(&mut cursor)? as u32;
        runs.push((prev, len));
    }
    Ok(runs)
}

/// Validate run lengths against the row/null counts and hand the runs to
/// the lazy [`ColumnArray::Rle`] variant.
fn build_run_column(
    runs: Vec<(i64, u32)>,
    row_count: usize,
    col_def: &ColumnDef,
    bitmap: NullBitmap,
) -> Result<ColumnArray> {
    let covered: u64 = runs.iter().map(|&(_, len)| len as u64).sum();
    let expected = row_count.saturating_sub(bitmap.null_count());
    if covered as usize != expected {
        return Err(TaibaiError::Storage(format!(
            "run-length value count mismatch: got {}, expected {}",
            covered, expected
        )));
    }
    Ok(ColumnArray::rle(runs, col_def.data_type, bitmap, row_count))
}

pub(crate) fn decode_i64_rle_payload(payload: &[u8], expected_rows: usize) -> Result<Vec<i64>> {
    let mut cursor = Cursor::new(payload);
    let run_count = read_u32_le(&mut cursor)? as usize;
    let mut values = Vec::new();
    for _ in 0..run_count {
        let mut value_buf = [0u8; 8];
        cursor.read_exact(&mut value_buf)?;
        let value = i64::from_le_bytes(value_buf);
        let len = read_u32_le(&mut cursor)? as usize;
        // Validate BEFORE expanding: a corrupt run length used to drive a
        // repeat_n allocation of up to 4 billion values.
        if values.len().saturating_add(len) > expected_rows {
            return Err(TaibaiError::Storage(format!(
                "RLE run expansion reaches {} values, exceeding the row group's {}",
                values.len().saturating_add(len),
                expected_rows
            )));
        }
        values.extend(std::iter::repeat_n(value, len));
    }
    Ok(values)
}

pub(crate) fn decode_i64_delta_payload(payload: &[u8]) -> Result<Vec<i64>> {
    let mut cursor = Cursor::new(payload);
    let count = read_u32_le(&mut cursor)? as usize;
    if count == 0 {
        return Ok(Vec::new());
    }
    let mut base_buf = [0u8; 8];
    cursor.read_exact(&mut base_buf)?;
    let mut values = Vec::with_capacity(count);
    let mut prev = i64::from_le_bytes(base_buf);
    values.push(prev);
    for _ in 1..count {
        let encoded = read_varint(&mut cursor)?;
        let delta = zigzag_decode(encoded);
        let next = prev
            .checked_add(delta)
            .ok_or_else(|| TaibaiError::Storage("delta decoded value overflowed i64".into()))?;
        values.push(next);
        prev = next;
    }
    Ok(values)
}

/// Expand a dictionary-encoded varchar column directly into the column's
/// UTF-8 arena: dictionary entries borrow from the payload and each row is a
/// single memcpy — no per-row String allocation or clone.
fn expand_varchar_dictionary(
    payload: &[u8],
    row_count: usize,
    bitmap: &NullBitmap,
    allow_dictionary: bool,
) -> Result<ColumnArray> {
    let mut cursor = Cursor::new(payload);
    let dict_count = read_u32_le(&mut cursor)? as usize;
    let mut dict: Vec<&str> = Vec::with_capacity(dict_count);
    for _ in 0..dict_count {
        let len = read_u32_le(&mut cursor)? as usize;
        let start = cursor.position() as usize;
        if start + len > payload.len() {
            return Err(TaibaiError::Storage(
                "dictionary varchar decode: truncated entry".into(),
            ));
        }
        let bytes = &payload[start..start + len];
        let value = match std::str::from_utf8(bytes) {
            Ok(v) => v,
            Err(_) => {
                // Corrupt-data fallback (should not happen: writers validate
                // UTF-8). Leak one boxed string rather than complicate the
                // borrow structure for an unreachable path.
                let leaked: &'static str =
                    Box::leak(String::from_utf8_lossy(bytes).into_owned().into_boxed_str());
                leaked
            }
        };
        dict.push(value);
        cursor.set_position((start + len) as u64);
    }
    let index_count = read_u32_le(&mut cursor)? as usize;
    if index_count != row_count.saturating_sub(bitmap.null_count()) {
        return Err(TaibaiError::Storage(format!(
            "adaptive varchar value count mismatch: got {}, expected {}",
            index_count,
            row_count.saturating_sub(bitmap.null_count())
        )));
    }
    // Dictionary passthrough: keep (ids, dict) unexpanded so aggregation can
    // group on u32 ids without any per-row string work. Gated: the
    // ClusterKeyRangeRead strategy's binary search does not understand the
    // dictionary variant yet, so this is opt-in until that path is adapted.
    if !allow_dictionary {
        let mut buffer = Utf8Buffer::with_capacity(row_count);
        let mut pos = cursor.position() as usize;
        for row in 0..row_count {
            if bitmap.is_null(row) {
                buffer.push_str("");
                continue;
            }
            if pos + 4 > payload.len() {
                return Err(TaibaiError::Storage(
                    "dictionary varchar decode: truncated index".into(),
                ));
            }
            let idx = u32::from_le_bytes(payload[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;
            let value = dict.get(idx).ok_or_else(|| {
                TaibaiError::Storage(format!("dictionary index {} out of bounds", idx))
            })?;
            buffer.push_str(value);
        }
        return Ok(ColumnArray::VarChar {
            data: buffer,
            bitmap: bitmap.clone(),
        });
    }
    let start = cursor.position() as usize;
    if start + index_count * 4 > payload.len() {
        return Err(TaibaiError::Storage(
            "dictionary varchar decode: truncated index".into(),
        ));
    }
    let ids: Vec<u32> = payload[start..start + index_count * 4]
        .chunks_exact(4)
        .map(|b| u32::from_le_bytes(b.try_into().unwrap()))
        .collect();
    // The passthrough keeps raw ids: an out-of-range id would silently
    // decode to "" downstream (scalar/encode_key_at fall back to ""), so
    // validate against the dictionary here instead.
    if ids.iter().any(|&idx| idx as usize >= dict.len()) {
        return Err(TaibaiError::Storage(
            "dictionary index out of bounds".into(),
        ));
    }
    let mut ids_full = vec![0u32; row_count];
    let mut value_pos = 0usize;
    for (row, slot) in ids_full.iter_mut().enumerate() {
        if bitmap.is_null(row) {
            continue;
        }
        *slot = ids[value_pos];
        value_pos += 1;
    }
    let mut dict_buf = Utf8Buffer::with_capacity(dict.len());
    for value in &dict {
        dict_buf.push_str(value);
    }
    Ok(ColumnArray::Dictionary {
        ids: ids_full,
        dict: dict_buf,
        bitmap: bitmap.clone(),
        flat: std::sync::OnceLock::new(),
    })
}

fn build_numeric_column_from_i64(
    values: Vec<i64>,
    row_count: usize,
    col_def: &ColumnDef,
    bitmap: NullBitmap,
) -> Result<ColumnArray> {
    let expected = row_count.saturating_sub(bitmap.null_count());
    if values.len() != expected {
        return Err(TaibaiError::Storage(format!(
            "adaptive numeric value count mismatch: got {}, expected {}",
            values.len(),
            expected
        )));
    }

    let mut value_pos = 0usize;
    // Null-free fast path: values map 1:1 onto rows, so the build degrades to
    // one vectorizable cast per element instead of a per-row bitmap probe
    // (the same shape as the plain decoder's null-free bulk path). For the
    // i64-backed types the decoded buffer itself becomes the column — no
    // second pass, no copy.
    if bitmap.null_count() == 0 {
        match col_def.data_type {
            DataType::Int64 => {
                return Ok(ColumnArray::Int64 {
                    data: values,
                    bitmap,
                })
            }
            DataType::Decimal => {
                return Ok(ColumnArray::Decimal {
                    data: values,
                    bitmap,
                })
            }
            DataType::Time => {
                return Ok(ColumnArray::Time {
                    data: values,
                    bitmap,
                })
            }
            DataType::Timestamp => {
                return Ok(ColumnArray::Timestamp {
                    data: values,
                    bitmap,
                })
            }
            DataType::Interval => {
                return Ok(ColumnArray::Interval {
                    data: values,
                    bitmap,
                })
            }
            _ => {}
        }
    }
    // One macro serves signed and unsigned (and Date/Time/…) fixed-width
    // targets alike: `as` casts handle both.
    macro_rules! build_fixed {
        ($variant:ident, $ty:ty) => {{
            let out: Vec<$ty> = if bitmap.null_count() == 0 {
                values.iter().map(|&v| v as $ty).collect()
            } else {
                let mut out = Vec::with_capacity(row_count);
                for row in 0..row_count {
                    if bitmap.is_null(row) {
                        out.push(<$ty>::default());
                    } else {
                        out.push(values[value_pos] as $ty);
                        value_pos += 1;
                    }
                }
                out
            };
            ColumnArray::$variant { data: out, bitmap }
        }};
    }

    Ok(match col_def.data_type {
        DataType::Int8 => build_fixed!(Int8, i8),
        DataType::Int16 => build_fixed!(Int16, i16),
        DataType::Int32 => build_fixed!(Int32, i32),
        DataType::Int64 => build_fixed!(Int64, i64),
        DataType::Decimal => build_fixed!(Decimal, i64),
        DataType::Date => build_fixed!(Date, i32),
        DataType::Time => build_fixed!(Time, i64),
        DataType::Timestamp => build_fixed!(Timestamp, i64),
        DataType::Interval => build_fixed!(Interval, i64),
        DataType::UInt8 => build_fixed!(UInt8, u8),
        DataType::UInt16 => build_fixed!(UInt16, u16),
        DataType::UInt32 => build_fixed!(UInt32, u32),
        DataType::UInt64 => build_fixed!(UInt64, u64),
        other => {
            return Err(TaibaiError::Storage(format!(
                "adaptive numeric encoding cannot decode {}",
                other.display_name()
            )))
        }
    })
}

fn build_column_from_bits(
    values: Vec<u64>,
    row_count: usize,
    col_def: &ColumnDef,
    bitmap: NullBitmap,
) -> Result<ColumnArray> {
    let expected = row_count.saturating_sub(bitmap.null_count());
    if values.len() != expected {
        return Err(TaibaiError::Storage(format!(
            "gorilla xor value count mismatch: got {}, expected {}",
            values.len(),
            expected
        )));
    }

    let mut value_pos = 0usize;
    let column = match col_def.data_type {
        DataType::Float32 => {
            let mut out = Vec::with_capacity(row_count);
            for row in 0..row_count {
                if bitmap.is_null(row) {
                    out.push(f32::default());
                } else {
                    out.push(f32::from_bits(values[value_pos] as u32));
                    value_pos += 1;
                }
            }
            ColumnArray::Float32 { data: out, bitmap }
        }
        DataType::Float64 => {
            let mut out = Vec::with_capacity(row_count);
            for row in 0..row_count {
                if bitmap.is_null(row) {
                    out.push(f64::default());
                } else {
                    out.push(f64::from_bits(values[value_pos]));
                    value_pos += 1;
                }
            }
            ColumnArray::Float64 { data: out, bitmap }
        }
        DataType::Decimal => {
            let mut out = Vec::with_capacity(row_count);
            for row in 0..row_count {
                if bitmap.is_null(row) {
                    out.push(i64::default());
                } else {
                    out.push(values[value_pos] as i64);
                    value_pos += 1;
                }
            }
            ColumnArray::Decimal { data: out, bitmap }
        }
        other => {
            return Err(TaibaiError::Storage(format!(
                "gorilla xor encoding cannot decode {}",
                other.display_name()
            )))
        }
    };
    Ok(column)
}

pub fn decode_column_selected(
    data: &[u8],
    row_count: usize,
    col_def: &ColumnDef,
    selected_rows: &[usize],
) -> Result<ColumnArray> {
    let mut cursor = Cursor::new(data);
    let bitmap_len = read_u32_le(&mut cursor)? as usize;
    let mut bitmap_data = vec![0u8; bitmap_len];
    cursor.read_exact(&mut bitmap_data)?;
    let bitmap = NullBitmap::from_bytes(&bitmap_data, row_count);
    let values = &data[4 + bitmap_len..];

    if values.starts_with(COLUMN_ENCODING_MAGIC) {
        let decoded = decode_adaptive_column(values, row_count, col_def, bitmap, false)?;
        if let Some(&row) = selected_rows.iter().max() {
            if row >= decoded.len() {
                return Err(TaibaiError::Storage(format!(
                    "selected row {} out of bounds {}",
                    row,
                    decoded.len()
                )));
            }
        }
        // The adaptive decode produced a fully materialized column, so the
        // gather is a typed indexed copy — no per-row `scalar()` boxing.
        return Ok(decoded.gather_rows(selected_rows));
    }

    macro_rules! decode_fixed_variant {
        ($ty:ty, $width:expr, $variant:ident) => {
            decode_selected_fixed::<$ty, $width, _>(
                values,
                &bitmap,
                row_count,
                selected_rows,
                |data, bitmap| ColumnArray::$variant { data, bitmap },
            )
        };
    }

    match col_def.data_type {
        DataType::Bool => decode_fixed_variant!(u8, 1, Bool),
        DataType::Int8 => decode_fixed_variant!(i8, 1, Int8),
        DataType::Int16 => decode_fixed_variant!(i16, 2, Int16),
        DataType::Int32 => decode_fixed_variant!(i32, 4, Int32),
        DataType::Int64 => decode_fixed_variant!(i64, 8, Int64),
        DataType::UInt8 => decode_fixed_variant!(u8, 1, UInt8),
        DataType::UInt16 => decode_fixed_variant!(u16, 2, UInt16),
        DataType::UInt32 => decode_fixed_variant!(u32, 4, UInt32),
        DataType::UInt64 => decode_fixed_variant!(u64, 8, UInt64),
        DataType::Float32 => decode_fixed_variant!(f32, 4, Float32),
        DataType::Float64 => decode_fixed_variant!(f64, 8, Float64),
        DataType::Decimal => decode_fixed_variant!(i64, 8, Decimal),
        DataType::Date => decode_fixed_variant!(i32, 4, Date),
        DataType::Time => decode_fixed_variant!(i64, 8, Time),
        DataType::Timestamp => decode_fixed_variant!(i64, 8, Timestamp),
        DataType::Interval => decode_fixed_variant!(i64, 8, Interval),
        DataType::VarChar => decode_selected_varchar(values, &bitmap, row_count, selected_rows),
        DataType::Blob => decode_selected_blob(values, &bitmap, row_count, selected_rows),
        DataType::Vector => decode_selected_vector(values, &bitmap, row_count, selected_rows),
    }
}

trait FixedLe: Copy + Default {
    fn from_le_bytes(bytes: &[u8]) -> Self;
}

macro_rules! impl_fixed_le {
    ($ty:ty, $size:expr) => {
        impl FixedLe for $ty {
            fn from_le_bytes(bytes: &[u8]) -> Self {
                <$ty>::from_le_bytes(bytes[..$size].try_into().unwrap())
            }
        }
    };
}

impl_fixed_le!(u8, 1);
impl_fixed_le!(i8, 1);
impl_fixed_le!(i16, 2);
impl_fixed_le!(i32, 4);
impl_fixed_le!(i64, 8);
impl_fixed_le!(u16, 2);
impl_fixed_le!(u32, 4);
impl_fixed_le!(u64, 8);
impl_fixed_le!(f32, 4);
impl_fixed_le!(f64, 8);

fn selected_bitmap(bitmap: &NullBitmap, selected_rows: &[usize]) -> NullBitmap {
    let null_flags: Vec<bool> = selected_rows
        .iter()
        .map(|&row| bitmap.is_null(row))
        .collect();
    NullBitmap::from_bools(&null_flags)
}

fn decode_selected_fixed<T, const WIDTH: usize, F>(
    values: &[u8],
    bitmap: &NullBitmap,
    row_count: usize,
    selected_rows: &[usize],
    build: F,
) -> Result<ColumnArray>
where
    T: FixedLe,
    F: FnOnce(Vec<T>, NullBitmap) -> ColumnArray,
{
    let mut out = Vec::with_capacity(selected_rows.len());
    let out_bitmap = selected_bitmap(bitmap, selected_rows);

    if bitmap.null_count() == 0 {
        for &row in selected_rows {
            if row >= row_count {
                return Err(TaibaiError::Storage(format!(
                    "selected row {} out of bounds {}",
                    row, row_count
                )));
            }
            let offset = row
                .checked_mul(WIDTH)
                .ok_or_else(|| TaibaiError::Storage("fixed column offset overflow".into()))?;
            let end = offset + WIDTH;
            if end > values.len() {
                return Err(TaibaiError::Storage(
                    "fixed column selected offset exceeds data".into(),
                ));
            }
            out.push(T::from_le_bytes(&values[offset..end]));
        }
        return Ok(build(out, out_bitmap));
    }

    let mut selected_pos = 0usize;
    let mut value_offset = 0usize;
    for row in 0..row_count {
        let is_selected = selected_pos < selected_rows.len() && selected_rows[selected_pos] == row;
        if bitmap.is_null(row) {
            if is_selected {
                out.push(T::default());
                selected_pos += 1;
            }
            continue;
        }

        let end = value_offset + WIDTH;
        if end > values.len() {
            return Err(TaibaiError::Storage(
                "fixed column selected scan exceeds data".into(),
            ));
        }
        if is_selected {
            out.push(T::from_le_bytes(&values[value_offset..end]));
            selected_pos += 1;
            if selected_pos == selected_rows.len() {
                break;
            }
        }
        value_offset = end;
    }

    if out.len() != selected_rows.len() {
        // selected_rows must be ascending and in bounds (callers guarantee
        // it); falling short means the invariant broke and out would not
        // line up with out_bitmap — fail loudly instead of silently
        // returning a shorter column.
        return Err(TaibaiError::Storage(
            "selected decode did not cover every selected row".into(),
        ));
    }

    Ok(build(out, out_bitmap))
}

fn decode_selected_varchar(
    values: &[u8],
    bitmap: &NullBitmap,
    row_count: usize,
    selected_rows: &[usize],
) -> Result<ColumnArray> {
    let mut out = Vec::with_capacity(selected_rows.len());
    let out_bitmap = selected_bitmap(bitmap, selected_rows);
    let mut selected_pos = 0usize;
    let mut value_offset = 0usize;

    for row in 0..row_count {
        let is_selected = selected_pos < selected_rows.len() && selected_rows[selected_pos] == row;
        if bitmap.is_null(row) {
            if is_selected {
                out.push(String::new());
                selected_pos += 1;
            }
            continue;
        }
        if value_offset + 4 > values.len() {
            return Err(TaibaiError::Storage(
                "variable column length prefix truncated".into(),
            ));
        }
        let len =
            u32::from_le_bytes(values[value_offset..value_offset + 4].try_into().unwrap()) as usize;
        let start = value_offset + 4;
        let end = start + len;
        if end > values.len() {
            return Err(TaibaiError::Storage(
                "variable column selected value truncated".into(),
            ));
        }
        if is_selected {
            out.push(String::from_utf8_lossy(&values[start..end]).into_owned());
            selected_pos += 1;
            if selected_pos == selected_rows.len() {
                break;
            }
        }
        value_offset = end;
    }

    Ok(ColumnArray::VarChar {
        data: out.into(),
        bitmap: out_bitmap,
    })
}

fn decode_selected_blob(
    values: &[u8],
    bitmap: &NullBitmap,
    row_count: usize,
    selected_rows: &[usize],
) -> Result<ColumnArray> {
    let mut out = Vec::with_capacity(selected_rows.len());
    let out_bitmap = selected_bitmap(bitmap, selected_rows);
    let mut selected_pos = 0usize;
    let mut value_offset = 0usize;

    for row in 0..row_count {
        let is_selected = selected_pos < selected_rows.len() && selected_rows[selected_pos] == row;
        if bitmap.is_null(row) {
            if is_selected {
                out.push(Vec::new());
                selected_pos += 1;
            }
            continue;
        }
        if value_offset + 4 > values.len() {
            return Err(TaibaiError::Storage(
                "blob column length prefix truncated".into(),
            ));
        }
        let len =
            u32::from_le_bytes(values[value_offset..value_offset + 4].try_into().unwrap()) as usize;
        let start = value_offset + 4;
        let end = start + len;
        if end > values.len() {
            return Err(TaibaiError::Storage(
                "blob column selected value truncated".into(),
            ));
        }
        if is_selected {
            out.push(values[start..end].to_vec());
            selected_pos += 1;
            if selected_pos == selected_rows.len() {
                break;
            }
        }
        value_offset = end;
    }

    Ok(ColumnArray::Blob {
        data: out.into(),
        bitmap: out_bitmap,
    })
}

fn decode_selected_vector(
    values: &[u8],
    bitmap: &NullBitmap,
    row_count: usize,
    selected_rows: &[usize],
) -> Result<ColumnArray> {
    let mut out = Vec::with_capacity(selected_rows.len());
    let out_bitmap = selected_bitmap(bitmap, selected_rows);
    let mut selected_pos = 0usize;
    let mut value_offset = 0usize;

    for row in 0..row_count {
        let is_selected = selected_pos < selected_rows.len() && selected_rows[selected_pos] == row;
        if bitmap.is_null(row) {
            if is_selected {
                out.push(Vec::new());
                selected_pos += 1;
            }
            continue;
        }
        if value_offset + 4 > values.len() {
            return Err(TaibaiError::Storage(
                "vector column length prefix truncated".into(),
            ));
        }
        let count =
            u32::from_le_bytes(values[value_offset..value_offset + 4].try_into().unwrap()) as usize;
        let start = value_offset + 4;
        let end = start + count * 4;
        if end > values.len() {
            return Err(TaibaiError::Storage(
                "vector column selected value truncated".into(),
            ));
        }
        if is_selected {
            let mut v = Vec::with_capacity(count);
            for chunk in values[start..end].chunks_exact(4) {
                v.push(f32::from_le_bytes(chunk.try_into().unwrap()));
            }
            out.push(v);
            selected_pos += 1;
            if selected_pos == selected_rows.len() {
                break;
            }
        }
        value_offset = end;
    }

    Ok(ColumnArray::Vector {
        data: out,
        bitmap: out_bitmap,
    })
}
#[cfg(test)]
#[path = "column_codec_test.rs"]
mod tests;
