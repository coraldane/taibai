use crate::protocol::types::DataType;
use crate::storage::codec::column_codec::{decode_column, encode_column_ex, COLUMN_ENCODING_MAGIC};
use crate::storage::codec::encoding::EncodingType;
use crate::types::array::ColumnArray;
use crate::types::batch::ColumnDef;
use crate::types::bitmap::NullBitmap;

fn col_def(data_type: DataType) -> ColumnDef {
    ColumnDef::new("c", data_type)
}

fn smooth_prices_f64(n: usize) -> Vec<f64> {
    let mut ticks = Vec::with_capacity(n);
    let mut cum = 0i64;
    for i in 0..n {
        if i % 16 == 0 {
            cum += 1;
        }
        ticks.push(cum);
    }
    ticks.iter().map(|t| (10_000 + t) as f64 / 100.0).collect()
}

fn smooth_prices_f32(n: usize) -> Vec<f32> {
    smooth_prices_f64(n).iter().map(|v| *v as f32).collect()
}

fn smooth_decimals(n: usize) -> Vec<i64> {
    let mut v = Vec::with_capacity(n);
    let mut d = 1_000_000i64;
    for i in 0..n {
        d += ((i % 5) as i64 - 2) * 100;
        v.push(d);
    }
    v
}

fn fixed_interval_timestamps(n: usize) -> Vec<i64> {
    let start = 1_700_000_000_000i64;
    (0..n).map(|i| start + (i as i64) * 1_000).collect()
}

fn lcg_bits(n: usize) -> Vec<u64> {
    let mut state = 0x2545F4914F6CDD1Du64;
    (0..n)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        })
        .collect()
}

#[test]
fn gorilla_xor_roundtrip_float64() {
    let n = 512;
    let values = smooth_prices_f64(n);
    let col = ColumnArray::Float64 {
        data: values.clone(),
        bitmap: NullBitmap::new_valid(n),
    };
    let (encoded, skip_zstd) = encode_column_ex(&col, 0, n).unwrap();
    assert!(skip_zstd, "smooth float64 column should skip zstd");
    assert!(
        encoded.len() < values.len() * 8 / 2,
        "encoded {} bytes vs plain {}",
        encoded.len(),
        values.len() * 8
    );

    let decoded = decode_column(&encoded, n, &col_def(DataType::Float64), false).unwrap();
    match decoded {
        ColumnArray::Float64 { data, .. } => assert_eq!(data, values),
        other => panic!("unexpected variant: {:?}", other.data_type()),
    }
}

#[test]
fn gorilla_xor_roundtrip_float32() {
    let n = 512;
    let values = smooth_prices_f32(n);
    let col = ColumnArray::Float32 {
        data: values.clone(),
        bitmap: NullBitmap::new_valid(n),
    };
    let (encoded, skip_zstd) = encode_column_ex(&col, 0, n).unwrap();
    assert!(skip_zstd);
    let decoded = decode_column(&encoded, n, &col_def(DataType::Float32), false).unwrap();
    match decoded {
        ColumnArray::Float32 { data, .. } => assert_eq!(data, values),
        other => panic!("unexpected variant: {:?}", other.data_type()),
    }
}

#[test]
fn gorilla_xor_roundtrip_decimal() {
    let n = 512;
    let values = smooth_decimals(n);
    let col = ColumnArray::Decimal {
        data: values.clone(),
        bitmap: NullBitmap::new_valid(n),
    };
    let (encoded, skip_zstd) = encode_column_ex(&col, 0, n).unwrap();
    assert!(skip_zstd, "smooth decimal column should skip zstd");
    let decoded = decode_column(&encoded, n, &col_def(DataType::Decimal), false).unwrap();
    match decoded {
        ColumnArray::Decimal { data, .. } => assert_eq!(data, values),
        other => panic!("unexpected variant: {:?}", other.data_type()),
    }
}

#[test]
fn gorilla_xor_roundtrip_with_nulls() {
    let n = 512;
    let values = smooth_prices_f64(n);
    let flags: Vec<bool> = (0..n).map(|i| i % 50 == 17).collect();
    let col = ColumnArray::Float64 {
        data: values.clone(),
        bitmap: NullBitmap::from_bools(&flags),
    };
    let (encoded, _) = encode_column_ex(&col, 0, n).unwrap();
    let decoded = decode_column(&encoded, n, &col_def(DataType::Float64), false).unwrap();
    match decoded {
        ColumnArray::Float64 { data, bitmap } => {
            for i in 0..n {
                assert_eq!(bitmap.is_null(i), flags[i], "row {} null flag", i);
                if !flags[i] {
                    assert_eq!(data[i].to_bits(), values[i].to_bits(), "row {} value", i);
                }
            }
        }
        other => panic!("unexpected variant: {:?}", other.data_type()),
    }
}

#[test]
fn high_entropy_floats_fall_back_to_plain() {
    let n = 512;
    let values: Vec<f64> = lcg_bits(n).iter().map(|b| f64::from_bits(*b)).collect();
    let col = ColumnArray::Float64 {
        data: values,
        bitmap: NullBitmap::new_valid(n),
    };
    let (_, skip_zstd) = encode_column_ex(&col, 0, n).unwrap();
    assert!(!skip_zstd, "random floats must not claim xor compressed");
}

#[test]
fn stored_encoding_choice_is_empirical_and_roundtrips() {
    use crate::storage::codec::column_codec::choose_stored_column_encoding;

    let n = 4_096;
    let level = crate::storage::segment_manager::bulk_compression_level();
    let def = col_def(DataType::Float64);

    let noisy: Vec<f64> = {
        let mut state = 0x9E3779B97F4A7C15u64;
        let mut cum = 0i64;
        (0..n)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                if state.is_multiple_of(8) {
                    cum += 1;
                }
                (1_000_000 + cum) as f64 / 100.0
            })
            .collect()
    };
    let col_noisy = ColumnArray::Float64 {
        data: noisy.clone(),
        bitmap: NullBitmap::new_valid(n),
    };
    let stored = choose_stored_column_encoding(&col_noisy, 0, n, level, true).unwrap();
    let stored_data = &stored.data;
    assert!(
        stored_data.starts_with(&[0x28, 0xB5, 0x2F, 0xFD]),
        "two-decimal tick prices must keep the zstd(plain) candidate"
    );

    let sticky: Vec<f64> = {
        let mut cum = 0i64;
        (0..n)
            .map(|i| {
                if i % 64 == 0 {
                    cum += 1;
                }
                (1_000_000 + cum) as f64 / 100.0
            })
            .collect()
    };
    let col_sticky = ColumnArray::Float64 {
        data: sticky.clone(),
        bitmap: NullBitmap::new_valid(n),
    };
    let stored_sticky = choose_stored_column_encoding(&col_sticky, 0, n, level, true).unwrap();
    let stored_sticky_data = &stored_sticky.data;

    let plain =
        crate::storage::codec::column_codec::encode_column_plain(&col_sticky, 0, n).unwrap();
    let zstd_plain = zstd::bulk::compress(&plain, level).unwrap();
    if stored_sticky_data.starts_with(&[0x28, 0xB5, 0x2F, 0xFD]) {
        assert!(stored_sticky_data.len() <= zstd_plain.len() + 1);
    }

    let decompressed_noisy = zstd::bulk::decompress(stored_data, plain_bytes_cap(n)).unwrap();
    assert_eq!(stored.uncompressed_size, decompressed_noisy.len() as u64);
    let decoded_noisy = decode_column(&decompressed_noisy, n, &def, false).unwrap();
    match decoded_noisy {
        ColumnArray::Float64 { data, .. } => assert_eq!(data, noisy),
        other => panic!("unexpected variant: {:?}", other.data_type()),
    }
}

fn plain_bytes_cap(rows: usize) -> usize {
    16 + rows * 64
}

#[test]
fn fixed_interval_timestamps_prefer_bitpacked() {
    let n = 512;
    let values = fixed_interval_timestamps(n);
    let col = ColumnArray::Timestamp {
        data: values.clone(),
        bitmap: NullBitmap::new_valid(n),
    };
    let (encoded, skip_zstd) = encode_column_ex(&col, 0, n).unwrap();
    assert!(skip_zstd, "constant-delta timestamps should skip zstd");
    assert_eq!(adaptive_encoding_tag(&encoded), EncodingType::BitPacked);
    assert!(
        encoded.len() < n / 2,
        "bitpacked {} bytes for {} constant-delta values must beat ~1B/value varint",
        encoded.len(),
        n
    );

    let decoded = decode_column(&encoded, n, &col_def(DataType::Timestamp), false).unwrap();
    match decoded {
        ColumnArray::Timestamp { data, .. } => assert_eq!(data, values),
        other => panic!("unexpected variant: {:?}", other.data_type()),
    }
}

#[test]
fn gorilla_blocks_are_independent() {
    let n = 256;
    let values_a = smooth_prices_f64(n);
    let mut values_b = smooth_prices_f64(n);
    for v in values_b.iter_mut() {
        *v += 500.0;
    }
    let col_a = ColumnArray::Float64 {
        data: values_a.clone(),
        bitmap: NullBitmap::new_valid(n),
    };
    let col_b = ColumnArray::Float64 {
        data: values_b.clone(),
        bitmap: NullBitmap::new_valid(n),
    };
    let (enc_a, _) = encode_column_ex(&col_a, 0, n).unwrap();
    let (enc_b, _) = encode_column_ex(&col_b, 0, n).unwrap();
    let def = col_def(DataType::Float64);
    let dec_a = decode_column(&enc_a, n, &def, false).unwrap();
    let dec_b = decode_column(&enc_b, n, &def, false).unwrap();
    match (dec_a, dec_b) {
        (ColumnArray::Float64 { data: da, .. }, ColumnArray::Float64 { data: db, .. }) => {
            assert_eq!(da, values_a);
            assert_eq!(db, values_b);
        }
        _ => panic!("unexpected variants"),
    }
}

#[test]
#[ignore = "size/speed benchmark, run with: cargo test --release --lib codec -- --ignored --nocapture"]
fn adaptive_encoding_compression_bench() {
    use std::time::Instant;

    let n = 100_000usize;
    let level = crate::storage::segment_manager::bulk_compression_level();

    let tick_prices_f64: Vec<f64> = {
        let mut state = 0x9E3779B97F4A7C15u64;
        let mut cum = 0i64;
        (0..n)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                if state.is_multiple_of(32) {
                    cum += (state % 7) as i64 - 3;
                }
                (1_000_000 + cum) as f64 / 100.0
            })
            .collect()
    };
    let tick_prices_decimal: Vec<i64> =
        tick_prices_f64.iter().map(|v| (v * 100.0) as i64).collect();
    let ts: Vec<i64> = (0..n)
        .map(|i| 1_700_000_000_000 + (i as i64) * 100)
        .collect();

    let cases: Vec<(&str, ColumnArray, DataType)> = vec![
        (
            "float64 tick price",
            ColumnArray::Float64 {
                data: tick_prices_f64,
                bitmap: NullBitmap::new_valid(n),
            },
            DataType::Float64,
        ),
        (
            "decimal tick price",
            ColumnArray::Decimal {
                data: tick_prices_decimal,
                bitmap: NullBitmap::new_valid(n),
            },
            DataType::Decimal,
        ),
        (
            "timestamp 100ms interval",
            ColumnArray::Timestamp {
                data: ts,
                bitmap: NullBitmap::new_valid(n),
            },
            DataType::Timestamp,
        ),
    ];

    for (name, col, dtype) in cases {
        let plain_bytes = n * 8;
        let encoded_plain = {
            let mut buf = Vec::with_capacity(4 + plain_bytes);
            buf.extend_from_slice(&0u32.to_le_bytes());
            let raw = crate::storage::column_encoding::encode_column_direct_range(&col, 0, n);
            buf.extend_from_slice(&raw[4..]);
            buf
        };
        let zstd_plain = zstd::bulk::compress(&encoded_plain, level).unwrap();

        let t0 = Instant::now();
        let (adaptive, skip_zstd) = encode_column_ex(&col, 0, n).unwrap();
        let encode_us = t0.elapsed().as_micros();

        let t3 = Instant::now();
        let stored = crate::storage::codec::column_codec::choose_stored_column_encoding(
            &col, 0, n, level, true,
        )
        .unwrap();
        let choose_us = t3.elapsed().as_micros();
        let stored_verbatim = !stored.data.starts_with(&[0x28, 0xB5, 0x2F, 0xFD]);

        let def = col_def(dtype);
        let t1 = Instant::now();
        let decoded = decode_column(&adaptive, n, &def, false).unwrap();
        let decode_us = t1.elapsed().as_micros();
        assert_eq!(decoded.len(), n);

        let t2 = Instant::now();
        let zstd_decoded = zstd::bulk::decompress(&zstd_plain, plain_bytes + 4096).unwrap();
        let zstd_decode_us = t2.elapsed().as_micros();
        assert_eq!(zstd_decoded.len(), encoded_plain.len());

        println!(
            "{:28} n={:6} plain={:8}B  zstd(plain)={:7}B ({:4.1}x)  adaptive={:7}B ({:4.1}x)  chosen={} ({:4.1}x)  enc={:6}us choose={:6}us dec={:6}us (zstd dec={:6}us)",
            name,
            n,
            plain_bytes,
            zstd_plain.len(),
            plain_bytes as f64 / zstd_plain.len() as f64,
            adaptive.len(),
            plain_bytes as f64 / adaptive.len() as f64,
            if stored_verbatim { "adaptive-verbatim" } else { "zstd(plain)" },
            plain_bytes as f64 / stored.data.len() as f64,
            encode_us,
            choose_us,
            decode_us,
            zstd_decode_us,
        );
        let _ = skip_zstd;
    }
}

/// Read the adaptive encoding tag from an encode_column_ex buffer:
/// u32 bitmap_len + bitmap + "MXE1" magic + tag byte.
fn adaptive_encoding_tag(encoded: &[u8]) -> EncodingType {
    let bitmap_len = u32::from_le_bytes(encoded[..4].try_into().unwrap()) as usize;
    let tag_pos = 4 + bitmap_len + COLUMN_ENCODING_MAGIC.len();
    EncodingType::from_u8(encoded[tag_pos]).unwrap()
}

fn bitpacked_roundtrip(values: &[i64]) {
    let payload =
        crate::storage::codec::column_codec::encode_i64_bitpacked_payload(values).unwrap();
    let decoded =
        crate::storage::codec::column_codec::decode_i64_bitpacked_payload(&payload).unwrap();
    assert_eq!(decoded, values);
}

#[test]
fn bitpacked_payload_roundtrip_block_boundaries() {
    // Block size 128: probe sizes around every block boundary, including the
    // single-value and exact-multiple cases.
    for len in [1usize, 2, 127, 128, 129, 255, 256, 384, 511] {
        let values: Vec<i64> = (0..len).map(|i| 10_000 + i as i64 * 3).collect();
        bitpacked_roundtrip(&values);
        // All-equal deltas: width-0 blocks take the arithmetic-fill path.
        let constant: Vec<i64> = (0..len).map(|i| -7 + i as i64 * 1000).collect();
        bitpacked_roundtrip(&constant);
        let flat: Vec<i64> = vec![42; len];
        bitpacked_roundtrip(&flat);
    }
}

#[test]
fn bitpacked_payload_roundtrip_fluctuating_and_negative() {
    // Tick-price shape: oscillating deltas around a trend, close to but not
    // exactly monotonic — the close_px/trans_volume profile this encoding
    // targets.
    let mut values = Vec::new();
    let mut base = -5_000_000i64;
    let mut state = 0x2545F4914F6CDD1Du64;
    for i in 0..1_000 {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        base += (state % 400) as i64 - 200;
        if i % 97 == 0 {
            base += 50_000;
        }
        values.push(base);
    }
    bitpacked_roundtrip(&values);
}

#[test]
fn bitpacked_payload_survives_maximal_deltas() {
    // Deltas spanning ~2^55 land at the 56-bit width cap; the accumulator
    // must still reconstruct wrapping negative deltas exactly.
    let mut values = Vec::new();
    let mut base = -(1i64 << 54);
    for i in 0..300 {
        let delta = if i % 2 == 0 {
            1i64 << 54
        } else {
            -(1i64 << 54)
        };
        base = base.wrapping_add(delta);
        values.push(base);
    }
    bitpacked_roundtrip(&values);
}

#[test]
fn bitpacked_payload_rejects_unpackable_widths() {
    // Full-range random deltas need ~64 bits: the encoder must decline
    // instead of producing a stream the decoder cannot unpack.
    let mut state = 0x9E3779B97F4A7C15u64;
    let values: Vec<i64> = (0..512)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as i64
        })
        .collect();
    assert_eq!(
        crate::storage::codec::column_codec::encode_i64_bitpacked_payload(&values),
        None
    );
}

#[test]
fn rle_stored_choice_never_loses_to_the_old_stacked_form() {
    use crate::storage::codec::column_codec::{choose_stored_column_encoding, stored_encoding_tag};

    let level = crate::storage::segment_manager::bulk_compression_level();
    let def = col_def(DataType::Int64);
    let zstd_magic = [0x28, 0xB5, 0x2F, 0xFD];

    let cases: Vec<(String, Vec<i64>)> = vec![
        // One giant run: a 6-byte DeltaRle payload (or 16-byte RLE) under a
        // null-free bitmap. Huge inter-run jumps keep bit packing wide, so a
        // run form wins the adaptive competition.
        ("flat".into(), vec![7i64; 4_096]),
        // Long-ish runs with moderate jumps.
        (
            "long runs".into(),
            (0..4_096)
                .map(|i| 1_000_000 + (i / 512) as i64 * 100)
                .collect(),
        ),
    ];

    for (name, values) in cases {
        let col = ColumnArray::Int64 {
            data: values.clone(),
            bitmap: NullBitmap::new_valid(4_096),
        };
        let (adaptive, _) = encode_column_ex(&col, 0, 4_096).unwrap();
        let tag = stored_encoding_tag(&adaptive);
        assert!(
            matches!(tag, Some(EncodingType::Rle) | Some(EncodingType::DeltaRle)),
            "[{}] a run form should win the adaptive competition, got {:?}",
            name,
            tag
        );
        let compressed_stacked = zstd::bulk::compress(&adaptive, level).unwrap();

        let stored = choose_stored_column_encoding(&col, 0, 4_096, level, true).unwrap();
        if tag == Some(EncodingType::Rle) {
            // Three-way strict size: never worse than the zstd(payload) stack.
            assert!(
                stored.data.len() <= compressed_stacked.len(),
                "[{}] stored {} bytes must not exceed the zstd(RLE) form {} bytes",
                name,
                stored.data.len(),
                compressed_stacked.len()
            );
        } else {
            // DeltaRle tilt: verbatim may only cost up to 25% over the
            // zstd(payload) candidate.
            assert!(
                stored.data.len() * 5 <= compressed_stacked.len() * 4 + compressed_stacked.len(),
                "[{}] stored {} bytes vs zstd(payload) {} bytes exceeds the tilt budget",
                name,
                stored.data.len(),
                compressed_stacked.len()
            );
        }

        let raw = if stored.data.starts_with(&zstd_magic) {
            zstd::bulk::decompress(&stored.data, plain_bytes_cap(4_096)).unwrap()
        } else {
            assert_eq!(
                stored_encoding_tag(&stored.data),
                tag,
                "[{}] verbatim form must keep the winning tag",
                name
            );
            stored.data.clone()
        };
        match decode_column(&raw, 4_096, &def, false).unwrap() {
            ColumnArray::Int64 { data, .. } => assert_eq!(data, values, "[{}]", name),
            other => panic!("[{}] unexpected variant: {:?}", name, other.data_type()),
        }
    }
}

#[test]
fn bitpacked_payload_roundtrip_all_widths() {
    // Hammer every representable bit width (1..=56) x assorted lengths x
    // random deltas: exercises the windowed group paths (4/2/1 values per
    // window) and the mid-stream tail resume against the plain refill loop.
    let mut state = 0xDEADBEEFCAFEBABEu64;
    let mut next = move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    for width in 1u32..=56 {
        // Deltas drawn from the width's full range keep the encoder from
        // bailing; center them so signed deltas wrap the frame of reference.
        let delta_range: i64 = if width >= 63 { 0 } else { 1i64 << (width - 1) };
        for len in [129usize, 130, 132, 255, 257, 383] {
            let mut values = Vec::with_capacity(len);
            let mut base = (next() % 2_000_000) as i64 - 1_000_000;
            for _ in 0..len {
                let delta = (next() % (2 * delta_range as u64 + 1)) as i64 - delta_range;
                base = base.wrapping_add(delta);
                values.push(base);
            }
            let payload =
                match crate::storage::codec::column_codec::encode_i64_bitpacked_payload(&values) {
                    Some(payload) => payload,
                    // Blocks too wide for the 56-bit cap are declined wholesale.
                    None => continue,
                };
            let decoded =
                crate::storage::codec::column_codec::decode_i64_bitpacked_payload(&payload)
                    .unwrap();
            assert_eq!(decoded, values, "width {} len {}", width, len);
        }
    }
}

#[test]
fn bitpacked_payload_truncated_stream_is_error_not_panic() {
    let values: Vec<i64> = (0..300).map(|i| i as i64 * 7 + (i % 13) as i64).collect();
    let payload =
        crate::storage::codec::column_codec::encode_i64_bitpacked_payload(&values).unwrap();
    for cut in [0, 5, 17, payload.len() / 2, payload.len() - 1] {
        assert!(
            crate::storage::codec::column_codec::decode_i64_bitpacked_payload(&payload[..cut])
                .is_err(),
            "truncation at {} must be rejected",
            cut
        );
    }
}

#[test]
fn bitpacked_column_roundtrip_int64_with_nulls() {
    let n = 1_000;
    let mut values = Vec::with_capacity(n);
    let mut base = 100i64;
    for i in 0..n {
        base += if i % 64 == 0 { 50 } else { 1 };
        values.push(base);
    }
    let flags: Vec<bool> = (0..n).map(|i| i % 31 == 5).collect();
    let col = ColumnArray::Int64 {
        data: values.clone(),
        bitmap: NullBitmap::from_bools(&flags),
    };
    let (encoded, skip_zstd) = encode_column_ex(&col, 0, n).unwrap();
    assert!(skip_zstd, "steadily growing int64 should skip zstd");
    let decoded = decode_column(&encoded, n, &col_def(DataType::Int64), false).unwrap();
    match decoded {
        ColumnArray::Int64 { data, bitmap } => {
            for i in 0..n {
                assert_eq!(bitmap.is_null(i), flags[i], "row {} null flag", i);
                if !flags[i] {
                    assert_eq!(data[i], values[i], "row {} value", i);
                }
            }
        }
        other => panic!("unexpected variant: {:?}", other.data_type()),
    }
}

#[test]
fn bitpacked_decimal_tick_prices_roundtrip_via_stored_choice() {
    use crate::storage::codec::column_codec::{choose_stored_column_encoding, stored_encoding_tag};

    let n = 4_096;
    let level = crate::storage::segment_manager::bulk_compression_level();
    let zstd_magic = [0x28, 0xB5, 0x2F, 0xFD];

    let cases: Vec<(&str, Vec<i64>, bool)> = vec![
        // Bounded oscillation around a fixed level: ultra-low entropy, zstd
        // may legitimately keep the stored bytes.
        ("ultra-smooth", smooth_decimals(n), false),
        // Moderate-entropy small-range deltas (volume-like ticks): the case
        // bit packing exists for — too irregular for zstd to crush, far too
        // narrow to justify 8 bytes/value.
        (
            "moderate entropy",
            {
                let mut values = Vec::with_capacity(n);
                let mut base = 500i64;
                let mut state = 0x2545F4914F6CDD1Du64;
                for _ in 0..n {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    base += (state % 4_096) as i64 - 2_048;
                    values.push(base);
                }
                values
            },
            true,
        ),
    ];

    for (name, values, expect_bitpacked) in cases {
        let col = ColumnArray::Decimal {
            data: values.clone(),
            bitmap: NullBitmap::new_valid(n),
        };
        let stored = choose_stored_column_encoding(&col, 0, n, level, true).unwrap();

        // Whatever candidate won, the roundtrip through the stored bytes must
        // reproduce the column exactly.
        let raw = if expect_bitpacked {
            assert!(
                !stored.data.starts_with(&zstd_magic),
                "[{}] moderate-entropy deltas must favor the verbatim bit-packed stream",
                name
            );
            assert_eq!(
                stored_encoding_tag(&stored.data),
                Some(EncodingType::BitPacked),
                "[{}] verbatim candidate should be the bit-packed stream",
                name
            );
            assert!(
                stored.data.len() * 4 < n * 8,
                "[{}] bitpacked {} bytes must at least quarter plain {} bytes",
                name,
                stored.data.len(),
                n * 8
            );
            stored.data.clone()
        } else if stored.data.starts_with(&zstd_magic) {
            zstd::bulk::decompress(&stored.data, plain_bytes_cap(n)).unwrap()
        } else {
            stored.data.clone()
        };

        let decoded = decode_column(&raw, n, &col_def(DataType::Decimal), false).unwrap();
        match decoded {
            ColumnArray::Decimal { data, .. } => assert_eq!(data, values, "[{}]", name),
            other => panic!("[{}] unexpected variant: {:?}", name, other.data_type()),
        }
    }
}

fn delta_rle_roundtrip(values: &[i64]) {
    let payload = crate::storage::codec::column_codec::encode_i64_delta_rle_payload(values);
    let decoded =
        crate::storage::codec::column_codec::decode_i64_delta_rle_payload(&payload, values.len()).unwrap();
    assert_eq!(decoded, values);
}

#[test]
fn delta_rle_payload_roundtrip_run_shapes() {
    // Single run, runs of one with huge jumps, varint length boundaries,
    // negative values and wrapping deltas.
    delta_rle_roundtrip(&[42i64; 1]);
    delta_rle_roundtrip(&[42i64; 4_096]);
    delta_rle_roundtrip(&[-1i64; 300]);
    let alternating: Vec<i64> = (0..500)
        .flat_map(|i| [i64::MIN + i, i64::MAX - i].repeat(3))
        .collect();
    delta_rle_roundtrip(&alternating);
    let mut tick_like = Vec::new();
    let mut base = 1_490_900i64;
    let mut state = 0x2545F4914F6CDD1Du64;
    for i in 0..2_000 {
        if i % 9 == 0 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            base += (state % 300) as i64 - 150;
        }
        tick_like.push(base);
    }
    delta_rle_roundtrip(&tick_like);
    let mut long_lens = Vec::new();
    let mut v = -9_000_000_000i64;
    for len in [1usize, 126, 127, 128, 16_383, 16_384, 16_385] {
        long_lens.extend(std::iter::repeat_n(v, len));
        v += 123_456_789;
    }
    delta_rle_roundtrip(&long_lens);
}

#[test]
fn delta_rle_column_roundtrip_with_nulls_and_verbatim() {
    use crate::storage::codec::column_codec::{choose_stored_column_encoding, stored_encoding_tag};

    let n = 4_096;
    let mut values = Vec::with_capacity(n);
    let mut base = 1_000_000i64;
    for i in 0..n {
        if i % 12 == 0 {
            base += 300;
        }
        values.push(base);
    }
    let flags: Vec<bool> = (0..n).map(|i| i % 61 == 7).collect();
    let col = ColumnArray::Decimal {
        data: values.clone(),
        bitmap: NullBitmap::from_bools(&flags),
    };

    let (encoded, skip_zstd) = encode_column_ex(&col, 0, n).unwrap();
    assert_eq!(
        adaptive_encoding_tag(&encoded),
        EncodingType::DeltaRle,
        "run-heavy decimal prices should pick delta RLE"
    );
    assert!(skip_zstd, "DeltaRle is a verbatim-eligible layout");

    let level = crate::storage::segment_manager::bulk_compression_level();
    let stored = choose_stored_column_encoding(&col, 0, n, level, true).unwrap();
    let zstd_magic = [0x28, 0xB5, 0x2F, 0xFD];
    let raw = if stored.data.starts_with(&zstd_magic) {
        zstd::bulk::decompress(&stored.data, plain_bytes_cap(n)).unwrap()
    } else {
        assert_eq!(
            stored_encoding_tag(&stored.data),
            Some(EncodingType::DeltaRle)
        );
        stored.data.clone()
    };

    let decoded = decode_column(&raw, n, &col_def(DataType::Decimal), false).unwrap();
    match decoded {
        ColumnArray::Decimal { data, bitmap } => {
            for i in 0..n {
                assert_eq!(bitmap.is_null(i), flags[i], "row {} null flag", i);
                if !flags[i] {
                    assert_eq!(data[i], values[i], "row {} value", i);
                }
            }
        }
        other => panic!("unexpected variant: {:?}", other.data_type()),
    }
}

#[test]
#[ignore = "decode throughput benchmark, run with: cargo test --release --lib codec -- --ignored --nocapture"]
fn bitpacked_decode_throughput_bench() {
    use std::time::Instant;

    let n = 1_000_000usize;
    let level = crate::storage::segment_manager::bulk_compression_level();
    let def = col_def(DataType::Decimal);

    // close_px-like: price ticks of a few cents around 1e6 (scaled i64).
    // trans_volume-like: wider pseudo-random walk.
    let mut price_values = Vec::with_capacity(n);
    let mut volume_values = Vec::with_capacity(n);
    let mut p = 1_000_000i64;
    let mut v = 500i64;
    let mut state = 0x2545F4914F6CDD1Du64;
    for _ in 0..n {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        p += (state % 200) as i64 - 100;
        v += (state % 4_096) as i64 - 2_048;
        price_values.push(p);
        volume_values.push(v);
    }

    for (name, values) in [
        ("price-like", &price_values),
        ("volume-like", &volume_values),
    ] {
        let col = ColumnArray::Decimal {
            data: values.clone(),
            bitmap: NullBitmap::new_valid(n),
        };

        // zstd(plain) storage candidate: the pre-bitpacking layout for these
        // columns, decoded at zstd-frame speed plus a value parse pass.
        let plain = crate::storage::codec::column_codec::encode_column_plain(&col, 0, n).unwrap();
        let zstd_block = zstd::bulk::compress(&plain, level).unwrap();
        let t0 = Instant::now();
        let from_zstd = decode_column(
            &zstd::bulk::decompress(&zstd_block, plain_bytes_cap(n)).unwrap(),
            n,
            &def,
            false,
        )
        .unwrap();
        let zstd_us = t0.elapsed().as_micros().max(1);

        // verbatim bit-packed storage candidate: no zstd frame, straight to
        // the unpack loop.
        let (adaptive, _) = encode_column_ex(&col, 0, n).unwrap();
        let t1 = Instant::now();
        let from_packed = decode_column(&adaptive, n, &def, false).unwrap();
        let packed_us = t1.elapsed().as_micros().max(1);

        // Break down the bit-packed path: raw payload unpack vs the full
        // decode_column (bitmap prefix + payload + column materialization).
        let tag_pos = 4
            + u32::from_le_bytes(adaptive[..4].try_into().unwrap()) as usize
            + COLUMN_ENCODING_MAGIC.len()
            + 1;
        let payload = &adaptive[tag_pos..];
        let t2 = Instant::now();
        let unpacked =
            crate::storage::codec::column_codec::decode_i64_bitpacked_payload(payload).unwrap();
        let unpack_us = t2.elapsed().as_micros().max(1);
        assert_eq!(unpacked.len(), n);

        match (from_zstd, from_packed) {
            (ColumnArray::Decimal { data: a, .. }, ColumnArray::Decimal { data: b, .. }) => {
                assert_eq!(a, *values);
                assert_eq!(b, *values);
            }
            _ => panic!("unexpected variants"),
        }

        let out_bytes = n * 8;
        println!(
            "{:12} n={} zstd_block={:7}B dec={:6}us ({:5.2} GB/s)  bitpacked={:7}B dec={:6}us ({:5.2} GB/s, unpack={:6}us)  speedup={:4.1}x",
            name,
            n,
            zstd_block.len(),
            zstd_us,
            out_bytes as f64 / zstd_us as f64 / 1e3,
            adaptive.len(),
            packed_us,
            out_bytes as f64 / packed_us as f64 / 1e3,
            unpack_us,
            zstd_us as f64 / packed_us as f64,
        );
    }
}

/// `decode_column_selected` on adaptive columns gathers typed values from
/// the fully decoded column; pin it against the plain decode + per-row
/// scalar reference for a run-heavy (Rle/DeltaRle) and a dictionary shape.
#[test]
fn decode_selected_adaptive_gather_matches_reference() {
    use crate::storage::codec::column_codec::decode_column_selected;

    fn check(
        col: &ColumnArray,
        dt: DataType,
        encoded: &[u8],
        row_count: usize,
        selected: &[usize],
    ) {
        let gathered = decode_column_selected(encoded, row_count, &col_def(dt), selected).unwrap();
        assert_eq!(gathered.len(), selected.len());
        assert_eq!(gathered.data_type(), dt);
        for (out_row, &src_row) in selected.iter().enumerate() {
            assert_eq!(
                gathered.scalar(out_row),
                col.scalar(src_row),
                "gathered row {} must equal source row {}",
                out_row,
                src_row
            );
        }
    }

    // DeltaRle shape: long repeated runs (the Q6 close_px form).
    let mut values = Vec::new();
    for &(v, len) in &[(7i64, 300usize), (42, 400), (-9, 250), (1000, 50)] {
        values.extend(std::iter::repeat_n(v, len));
    }
    let decimal: ColumnArray = ColumnArray::Decimal {
        data: values.clone(),
        bitmap: NullBitmap::new_valid(values.len()),
    };
    let (encoded, _) = encode_column_ex(&decimal, 0, values.len()).unwrap();
    assert_eq!(
        crate::storage::codec::column_codec::stored_encoding_tag(&encoded),
        Some(EncodingType::DeltaRle),
        "fixture must be DeltaRle-encoded"
    );
    let selected: Vec<usize> = (0..values.len()).step_by(7).collect();
    check(&decimal, DataType::Decimal, &encoded, values.len(), &selected);

    // Dictionary varchar shape: dictionary passthrough decodes to plain.
    let mut strings: Vec<&str> = Vec::new();
    for i in 0..500 {
        strings.push(if i % 5 == 0 { "aaa" } else { "bbbb" });
    }
    let owned: Vec<String> = strings.iter().map(|s| s.to_string()).collect();
    let varchar: ColumnArray = ColumnArray::VarChar {
        data: owned.clone().into(),
        bitmap: NullBitmap::new_valid(owned.len()),
    };
    let (encoded, _) = encode_column_ex(&varchar, 0, owned.len()).unwrap();
    let selected: Vec<usize> = (0..owned.len()).rev().step_by(3).collect();
    let gathered = decode_column_selected(&encoded, owned.len(), &col_def(DataType::VarChar), &selected).unwrap();
    assert_eq!(gathered.len(), selected.len());
    for (out_row, &src_row) in selected.iter().enumerate() {
        assert_eq!(gathered.scalar(out_row), varchar.scalar(src_row));
    }
}
