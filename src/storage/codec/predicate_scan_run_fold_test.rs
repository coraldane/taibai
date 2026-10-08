use super::*;
use crate::storage::codec::column_codec::{
    decode_i64_delta_rle_payload, decode_i64_rle_payload, encode_i64_delta_rle_payload,
};

/// Reference fold: expand the payload, filter per value, aggregate.
fn reference_fold(values: &[i64], predicates: &[RunPredicate]) -> RunFold {
    let mut out = RunFold::default();
    for &value in values {
        if predicates.iter().all(|p| p.matches(value)) {
            if !out.any {
                out.min = value;
                out.max = value;
                out.any = true;
            } else {
                out.min = out.min.min(value);
                out.max = out.max.max(value);
            }
            out.matched_rows += 1;
            out.sum_i = out.sum_i.wrapping_add(value);
        }
    }
    out
}

fn raw_column(payload: &[u8], tag: u8, null_rows: usize, non_null: usize) -> Vec<u8> {
    // bitmap_len = 0 + [magic][tag][payload] — a null-free column (the
    // fold never consults the bitmap; runs cover only non-null values).
    let bitmap = crate::types::bitmap::NullBitmap::new_valid(non_null + null_rows);
    let bitmap_bytes = bitmap.to_bytes();
    let mut raw = Vec::with_capacity(4 + bitmap_bytes.len() + 6 + payload.len());
    raw.extend_from_slice(&(bitmap_bytes.len() as u32).to_le_bytes());
    raw.extend_from_slice(&bitmap_bytes);
    raw.extend_from_slice(COLUMN_ENCODING_MAGIC);
    raw.push(tag);
    raw.extend_from_slice(payload);
    raw
}

fn sample_values() -> Vec<i64> {
    // Long runs, negatives, zeros, repeated extremes.
    let mut values = Vec::new();
    for &(value, len) in &[
        (10i64, 40usize),
        (-5, 30),
        (0, 25),
        (100, 20),
        (7, 35),
        (-100, 10),
        (10, 20),
    ] {
        values.extend(std::iter::repeat_n(value, len));
    }
    values
}

fn int_pred(op: RangeOp, value: i128) -> RunPredicate {
    RunPredicate::Int { op, value }
}

#[test]
fn delta_rle_run_fold_matches_reference_across_predicates() {
    let values = sample_values();
    let payload = encode_i64_delta_rle_payload(&values);
    assert_eq!(
        decode_i64_delta_rle_payload(&payload, values.len()).unwrap(),
        values,
        "fixture must round-trip"
    );
    let raw = raw_column(
        &payload,
        crate::storage::codec::encoding::EncodingType::DeltaRle.to_u8(),
        0,
        values.len(),
    );

    let cases: Vec<Vec<RunPredicate>> = vec![
        vec![],
        vec![int_pred(RangeOp::Gt, 7)],
        vec![int_pred(RangeOp::Ge, 0)],
        vec![int_pred(RangeOp::Lt, 50)],
        vec![int_pred(RangeOp::Le, 10)],
        vec![int_pred(RangeOp::Eq, 10)],
        vec![int_pred(RangeOp::Gt, -100), int_pred(RangeOp::Lt, 100)],
        vec![int_pred(RangeOp::Gt, 1000)],
    ];
    for predicates in cases {
        let folded = fold_runs_from_raw(&raw, &predicates)
            .expect("adaptive column")
            .expect("fold ok");
        let reference = reference_fold(&values, &predicates);
        assert_eq!(
            folded, reference,
            "mismatch for predicates {:?}",
            predicates
        );
    }
}

#[test]
fn plain_rle_run_fold_matches_reference() {
    let values = sample_values();
    // Re-encode as plain Rle (value + u32 len per run).
    let mut runs: Vec<(i64, u32)> = Vec::new();
    for &value in &values {
        match runs.last_mut() {
            Some((last, len)) if *last == value => *len += 1,
            _ => runs.push((value, 1)),
        }
    }
    let mut payload = Vec::new();
    payload.extend_from_slice(&(runs.len() as u32).to_le_bytes());
    for (value, len) in runs {
        payload.extend_from_slice(&value.to_le_bytes());
        payload.extend_from_slice(&len.to_le_bytes());
    }
    assert_eq!(
        decode_i64_rle_payload(&payload, values.len()).unwrap(),
        values
    );
    let raw = raw_column(
        &payload,
        crate::storage::codec::encoding::EncodingType::Rle.to_u8(),
        0,
        values.len(),
    );

    let predicates = vec![int_pred(RangeOp::Ge, 7)];
    let folded = fold_runs_from_raw(&raw, &predicates)
        .expect("adaptive column")
        .expect("fold ok");
    assert_eq!(folded, reference_fold(&values, &predicates));
}

#[test]
fn non_run_encodings_decline_and_never_predicates_fold_nothing() {
    // BitPacked tag: fold must decline (None).
    let raw = raw_column(
        &[],
        crate::storage::codec::encoding::EncodingType::BitPacked.to_u8(),
        0,
        0,
    );
    assert!(fold_runs_from_raw(&raw, &[]).is_none());
    // No magic at all (plain column): decline.
    assert!(fold_runs_from_raw(&[0, 0, 0, 0, 1, 2, 3], &[]).is_none());

    // A NULL / varchar threshold never matches, mirroring the row
    // kernels' all-false fallback.
    let values = vec![1i64, 2, 3];
    let payload = encode_i64_delta_rle_payload(&values);
    let raw = raw_column(
        &payload,
        crate::storage::codec::encoding::EncodingType::DeltaRle.to_u8(),
        0,
        values.len(),
    );
    let never = RunPredicate::new(RangeOp::Gt, &ScalarValue::VarChar(Some("x".into())));
    assert_eq!(never, RunPredicate::Never);
    let folded = fold_runs_from_raw(&raw, &[never])
        .expect("adaptive column")
        .expect("fold ok");
    assert_eq!(folded.matched_rows, 0);
    assert!(!folded.any);
}

#[test]
fn run_predicate_float_threshold_uses_f64_domain() {
    // Float thresholds compare in the raw f64 domain (numeric_cmp
    // semantics), so 10.5 excludes the run of 10 and includes 100.
    let values = sample_values();
    let payload = encode_i64_delta_rle_payload(&values);
    let raw = raw_column(
        &payload,
        crate::storage::codec::encoding::EncodingType::DeltaRle.to_u8(),
        0,
        values.len(),
    );
    let pred = RunPredicate::Float {
        op: RangeOp::Gt,
        value: 10.5,
    };
    let folded = fold_runs_from_raw(&raw, &[pred])
        .expect("adaptive column")
        .expect("fold ok");
    let reference = reference_fold(&values, &[pred]);
    assert_eq!(folded, reference);
}
