use crate::common::error::{MonaxError, Result};
use crate::types::scalar::ScalarValue;

/// Encoding type for column data within a segment block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncodingType {
    /// Raw uncompressed fixed-width values.
    Plain,
    /// Run-length encoding: (value, count) pairs.
    Rle,
    /// Dictionary encoding: indices into a dictionary of unique values.
    Dictionary,
    /// Delta encoding: store first value + differences.
    Delta,
    /// Delta-of-delta encoding: store differences of differences (for monotonic sequences).
    DeltaOfDelta,
    /// Bit-packed: pack values using fewer bits than their full width.
    BitPacked,
}

impl EncodingType {
    pub fn from_u8(v: u8) -> Result<Self> {
        match v {
            0 => Ok(EncodingType::Plain),
            1 => Ok(EncodingType::Rle),
            2 => Ok(EncodingType::Dictionary),
            3 => Ok(EncodingType::Delta),
            4 => Ok(EncodingType::DeltaOfDelta),
            5 => Ok(EncodingType::BitPacked),
            _ => Err(MonaxError::Storage(format!("unknown encoding type: {}", v))),
        }
    }

    pub fn to_u8(self) -> u8 {
        match self {
            EncodingType::Plain => 0,
            EncodingType::Rle => 1,
            EncodingType::Dictionary => 2,
            EncodingType::Delta => 3,
            EncodingType::DeltaOfDelta => 4,
            EncodingType::BitPacked => 5,
        }
    }
}

/// Select the best encoding for a column of scalar values.
/// Uses simple heuristics to pick the encoding with the best expected compression.
pub fn select_encoding(values: &[ScalarValue]) -> EncodingType {
    if values.is_empty() {
        return EncodingType::Plain;
    }

    let n = values.len();

    // Check for RLE: if there are few unique values relative to total
    let unique_count = count_unique(values);
    if unique_count <= 4 && n > 16 {
        return EncodingType::Rle;
    }

    // Check for dictionary encoding: good when cardinality is much less than n
    if unique_count < n / 2 && n > 32 {
        return EncodingType::Dictionary;
    }

    // Check for monotonic integer sequences (delta-of-delta)
    if is_monotonic_int(values) && n > 8 {
        let deltas_of_deltas = compute_deltas_of_deltas(values);
        if let Some(dod) = deltas_of_deltas {
            let unique_dod = count_unique_i64(&dod);
            if unique_dod <= 3 {
                return EncodingType::DeltaOfDelta;
            }
        }
        return EncodingType::Delta;
    }

    EncodingType::Plain
}

/// RLE encode a slice of scalar values into (value, count) pairs.
pub fn rle_encode(values: &[ScalarValue]) -> Vec<(ScalarValue, u32)> {
    if values.is_empty() {
        return Vec::new();
    }

    let mut result = Vec::new();
    let mut current = values[0].clone();
    let mut count = 1u32;

    for i in 1..values.len() {
        if values_equal(&current, &values[i]) {
            count += 1;
        } else {
            result.push((current, count));
            current = values[i].clone();
            count = 1;
        }
    }
    result.push((current, count));
    result
}

/// RLE decode (value, count) pairs back into scalar values.
pub fn rle_decode(pairs: &[(ScalarValue, u32)]) -> Vec<ScalarValue> {
    let mut result = Vec::new();
    for (val, count) in pairs {
        for _ in 0..*count {
            result.push(val.clone());
        }
    }
    result
}

/// Dictionary encode a slice: returns (dictionary, indices).
pub fn dictionary_encode(values: &[ScalarValue]) -> (Vec<ScalarValue>, Vec<u32>) {
    let mut dict: Vec<ScalarValue> = Vec::new();
    let mut indices = Vec::with_capacity(values.len());

    for val in values {
        if let Some(pos) = dict.iter().position(|d| values_equal(d, val)) {
            indices.push(pos as u32);
        } else {
            indices.push(dict.len() as u32);
            dict.push(val.clone());
        }
    }

    (dict, indices)
}

/// Dictionary decode: given dictionary and indices, reconstruct values.
pub fn dictionary_decode(dict: &[ScalarValue], indices: &[u32]) -> Vec<ScalarValue> {
    indices.iter()
        .map(|&idx| dict.get(idx as usize).cloned().unwrap_or(ScalarValue::Int64(None)))
        .collect()
}

/// Delta encode a slice of i64 values: first value + differences.
pub fn delta_encode(values: &[i64]) -> Vec<i64> {
    if values.is_empty() {
        return Vec::new();
    }
    let mut result = Vec::with_capacity(values.len());
    result.push(values[0]);
    for i in 1..values.len() {
        result.push(values[i] - values[i - 1]);
    }
    result
}

/// Delta decode: reconstruct original from first value + differences.
pub fn delta_decode(deltas: &[i64]) -> Vec<i64> {
    if deltas.is_empty() {
        return Vec::new();
    }
    let mut result = Vec::with_capacity(deltas.len());
    result.push(deltas[0]);
    for i in 1..deltas.len() {
        result.push(result[i - 1] + deltas[i]);
    }
    result
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn count_unique(values: &[ScalarValue]) -> usize {
    let mut seen: Vec<&ScalarValue> = Vec::new();
    for v in values {
        if !seen.iter().any(|s| values_equal(s, v)) {
            seen.push(v);
        }
    }
    seen.len()
}

fn count_unique_i64(values: &[i64]) -> usize {
    let mut seen = std::collections::HashSet::new();
    for &v in values {
        seen.insert(v);
    }
    seen.len()
}

fn values_equal(a: &ScalarValue, b: &ScalarValue) -> bool {
    match (a, b) {
        (ScalarValue::Int64(Some(l)), ScalarValue::Int64(Some(r))) => l == r,
        (ScalarValue::Int32(Some(l)), ScalarValue::Int32(Some(r))) => l == r,
        (ScalarValue::Float64(Some(l)), ScalarValue::Float64(Some(r))) => l == r,
        (ScalarValue::VarChar(Some(l)), ScalarValue::VarChar(Some(r))) => l == r,
        (ScalarValue::Bool(Some(l)), ScalarValue::Bool(Some(r))) => l == r,
        (ScalarValue::Int64(None), ScalarValue::Int64(None)) => true,
        (ScalarValue::VarChar(None), ScalarValue::VarChar(None)) => true,
        _ => false,
    }
}

fn is_monotonic_int(values: &[ScalarValue]) -> bool {
    if values.len() < 2 {
        return false;
    }
    let ints: Vec<i64> = values.iter()
        .filter_map(|v| match v {
            ScalarValue::Int64(Some(v)) => Some(*v),
            ScalarValue::Int32(Some(v)) => Some(*v as i64),
            _ => None,
        })
        .collect();
    if ints.len() != values.len() {
        return false;
    }
    // Check monotonic non-decreasing
    ints.windows(2).all(|w| w[0] <= w[1])
}

fn compute_deltas_of_deltas(values: &[ScalarValue]) -> Option<Vec<i64>> {
    let ints: Vec<i64> = values.iter()
        .filter_map(|v| match v {
            ScalarValue::Int64(Some(v)) => Some(*v),
            ScalarValue::Int32(Some(v)) => Some(*v as i64),
            _ => None,
        })
        .collect();
    if ints.len() != values.len() || ints.len() < 3 {
        return None;
    }
    let deltas: Vec<i64> = ints.windows(2).map(|w| w[1] - w[0]).collect();
    let dod: Vec<i64> = deltas.windows(2).map(|w| w[1] - w[0]).collect();
    Some(dod)
}

#[cfg(test)]
mod tests {
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
        assert_eq!(dict.len(), 3); // 3 unique values
        assert_eq!(indices[0], indices[2]); // "a" at same position

        let decoded = dictionary_decode(&dict, &indices);
        assert_eq!(decoded, values);
    }

    #[test]
    fn test_delta_encode_decode() {
        let values = vec![100i64, 102, 105, 110, 118];
        let encoded = delta_encode(&values);
        assert_eq!(encoded[0], 100);
        assert_eq!(encoded[1], 2);
        assert_eq!(encoded[2], 3);

        let decoded = delta_decode(&encoded);
        assert_eq!(decoded, values);
    }

    #[test]
    fn test_select_encoding_rle() {
        let values: Vec<ScalarValue> = (0..100).map(|_| ScalarValue::Int64(Some(42))).collect();
        assert_eq!(select_encoding(&values), EncodingType::Rle);
    }

    #[test]
    fn test_select_encoding_plain() {
        // Non-monotonic values with high cardinality => Plain
        let values: Vec<ScalarValue> = (0..50).map(|i| {
            ScalarValue::Int64(Some(((i * 97 + 13) * 2654435761) % 1000))
        }).collect();
        assert_eq!(select_encoding(&values), EncodingType::Plain);
    }

    #[test]
    fn test_encoding_type_roundtrip() {
        for code in 0u8..=5 {
            let t = EncodingType::from_u8(code).unwrap();
            assert_eq!(t.to_u8(), code);
        }
    }

    #[test]
    fn test_empty_inputs() {
        assert!(rle_encode(&[]).is_empty());
        assert!(delta_encode(&[]).is_empty());
        let (dict, idx) = dictionary_encode(&[]);
        assert!(dict.is_empty());
        assert!(idx.is_empty());
    }
}
