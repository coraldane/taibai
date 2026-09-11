use crate::protocol::types::DataType;
use crate::storage::codec::column_codec::{decode_column, encode_column_ex};
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

    let decoded = decode_column(&encoded, n, &col_def(DataType::Float64)).unwrap();
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
    let decoded = decode_column(&encoded, n, &col_def(DataType::Float32)).unwrap();
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
    let decoded = decode_column(&encoded, n, &col_def(DataType::Decimal)).unwrap();
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
    let decoded = decode_column(&encoded, n, &col_def(DataType::Float64)).unwrap();
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
    let decoded_noisy = decode_column(&decompressed_noisy, n, &def).unwrap();
    match decoded_noisy {
        ColumnArray::Float64 { data, .. } => assert_eq!(data, noisy),
        other => panic!("unexpected variant: {:?}", other.data_type()),
    }
}

fn plain_bytes_cap(rows: usize) -> usize {
    16 + rows * 64
}

#[test]
fn fixed_interval_timestamps_prefer_delta_of_delta() {
    let n = 512;
    let values = fixed_interval_timestamps(n);
    let col = ColumnArray::Timestamp {
        data: values.clone(),
        bitmap: NullBitmap::new_valid(n),
    };
    let (encoded, skip_zstd) = encode_column_ex(&col, 0, n).unwrap();
    assert!(skip_zstd, "constant-delta timestamps should skip zstd");
    assert!(
        encoded.len() < n + 128,
        "dod encoded {} bytes for {} constant-delta values (bitmap+header budget 128)",
        encoded.len(),
        n
    );

    let decoded = decode_column(&encoded, n, &col_def(DataType::Timestamp)).unwrap();
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
    let dec_a = decode_column(&enc_a, n, &def).unwrap();
    let dec_b = decode_column(&enc_b, n, &def).unwrap();
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
        let decoded = decode_column(&adaptive, n, &def).unwrap();
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
