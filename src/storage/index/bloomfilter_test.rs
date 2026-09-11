use super::*;

#[test]
fn test_bloomfilter_basic() {
    let mut bf = BloomFilter::with_defaults();

    bf.add(&ScalarValue::Int64(Some(42)));
    bf.add(&ScalarValue::VarChar(Some("test".into())));

    assert!(bf.might_contain(&ScalarValue::Int64(Some(42))));
    assert!(bf.might_contain(&ScalarValue::VarChar(Some("test".into()))));
    assert!(!bf.might_contain(&ScalarValue::Int64(Some(999))));
}

#[test]
fn test_bloomfilter_null() {
    let mut bf = BloomFilter::with_defaults();
    bf.add(&ScalarValue::Int64(None));

    assert!(bf.might_contain(&ScalarValue::Int64(None)));
}

#[test]
fn test_bloomfilter_merge() {
    let mut bf1 = BloomFilter::new(100, 0.01);
    let mut bf2 = BloomFilter::new(100, 0.01);

    bf1.add(&ScalarValue::Int64(Some(1)));
    bf1.add(&ScalarValue::Int64(Some(2)));

    bf2.add(&ScalarValue::Int64(Some(3)));
    bf2.add(&ScalarValue::Int64(Some(4)));

    bf1.merge(&bf2);

    assert!(bf1.might_contain(&ScalarValue::Int64(Some(1))));
    assert!(bf1.might_contain(&ScalarValue::Int64(Some(3))));
}

#[test]
fn test_bloomfilter_serialize_deserialize() {
    let mut bf = BloomFilter::with_defaults();
    bf.add(&ScalarValue::Int64(Some(42)));
    bf.add(&ScalarValue::VarChar(Some("hello".into())));

    let serialized = bf.serialize();
    let deserialized = BloomFilter::deserialize(&serialized).unwrap();

    assert!(deserialized.might_contain(&ScalarValue::Int64(Some(42))));
    assert!(deserialized.might_contain(&ScalarValue::VarChar(Some("hello".into()))));
}

#[test]
fn typed_add_matches_scalar_lookup_hashes() {
    let mut bf = BloomFilter::with_defaults();
    bf.add_typed(0, &true);
    bf.add_typed(3, &42i32);
    bf.add_typed(10, &3.5f64.to_bits());
    bf.add_typed(16, "688256.SH");
    bf.add_typed(17, &[1u8, 2, 3][..]);
    bf.add_vector(&[1.25, 2.5]);

    for value in [
        ScalarValue::Bool(Some(true)),
        ScalarValue::Int32(Some(42)),
        ScalarValue::Float64(Some(3.5)),
        ScalarValue::VarChar(Some("688256.SH".to_string())),
        ScalarValue::Blob(Some(vec![1, 2, 3])),
        ScalarValue::Vector(Some(vec![1.25, 2.5])),
    ] {
        assert!(bf.might_contain(&value), "typed bloom missed {value:?}");
    }
}
