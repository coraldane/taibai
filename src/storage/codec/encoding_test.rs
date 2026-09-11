use super::*;

#[test]
fn test_encoding_type_roundtrip() {
    let all = [
        EncodingType::Plain,
        EncodingType::Rle,
        EncodingType::Dictionary,
        EncodingType::Delta,
        EncodingType::DeltaOfDelta,
        EncodingType::BitPacked,
        EncodingType::GorillaXor,
    ];
    for enc in all {
        let back = EncodingType::from_u8(enc.to_u8()).unwrap();
        assert_eq!(enc, back);
    }
    assert!(EncodingType::from_u8(7).is_err());
}
