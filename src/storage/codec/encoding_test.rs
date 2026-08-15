use super::*;

#[test]
fn test_rle_encode_decode() {
    let values: Vec<ScalarValue> = vec![
        ScalarValue::Int64(Some(1)),
        ScalarValue::Int64(Some(1)),
        ScalarValue::Int64(Some(1)),
        ScalarValue::Int64(Some(2)),
        ScalarValue::Int64(Some(2)),
        ScalarValue::Int64(Some(3)),
    ];
    let rle = rle_encode(&values);
    assert_eq!(rle.len(), 3);
    assert_eq!(rle[0], (ScalarValue::Int64(Some(1)), 3));
    assert_eq!(rle[1], (ScalarValue::Int64(Some(2)), 2));
    assert_eq!(rle[2], (ScalarValue::Int64(Some(3)), 1));

    let decoded = rle_decode(&rle);
    assert_eq!(decoded, values);
}

#[test]
fn test_dictionary_encode_decode() {
    let values: Vec<ScalarValue> = vec![
        ScalarValue::VarChar(Some("a".into())),
        ScalarValue::VarChar(Some("b".into())),
        ScalarValue::VarChar(Some("a".into())),
        ScalarValue::VarChar(Some("c".into())),
        ScalarValue::VarChar(Some("b".into())),
    ];
    let (dict, indices) = dictionary_encode(&values);
    assert_eq!(dict.len(), 3);
    assert_eq!(indices, vec![0, 1, 0, 2, 1]);

    let decoded = dictionary_decode(&dict, &indices);
    assert_eq!(decoded, values);
}

#[test]
fn test_delta_encode_decode() {
    let values = vec![10i64, 20, 30, 35, 40];
    let deltas = delta_encode(&values);
    assert_eq!(deltas, vec![10, 10, 10, 5, 5]);

    let decoded = delta_decode(&deltas);
    assert_eq!(decoded, values);
}

#[test]
fn test_select_encoding_rle() {
    let values: Vec<ScalarValue> = std::iter::repeat_n(ScalarValue::Int64(Some(42)), 32).collect();
    assert_eq!(select_encoding(&values), EncodingType::Rle);
}

#[test]
fn test_select_encoding_plain() {
    let values: Vec<ScalarValue> = (1..=100)
        .map(|i| ScalarValue::Int64(Some((i as i64 * 997 + 13) % 1000 + i as i64 * 5000)))
        .collect();
    let enc = select_encoding(&values);
    assert!(
        enc == EncodingType::Plain
            || enc == EncodingType::Delta
            || enc == EncodingType::DeltaOfDelta,
        "expected non-RLE non-Dictionary, got {:?}",
        enc
    );
}

#[test]
fn test_select_encoding_delta() {
    let values: Vec<ScalarValue> = (1..=20)
        .map(|i| ScalarValue::Int64(Some(100 + i * 3)))
        .collect();
    let enc = select_encoding(&values);
    assert!(enc == EncodingType::Delta || enc == EncodingType::DeltaOfDelta);
}

#[test]
fn test_select_encoding_delta_of_delta() {
    let values: Vec<ScalarValue> = (1..=20).map(|i| ScalarValue::Int64(Some(i * i))).collect();
    assert_eq!(select_encoding(&values), EncodingType::DeltaOfDelta);
}
