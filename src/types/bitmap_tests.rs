use super::*;

#[test]
fn test_no_nulls() {
    let bm = NullBitmap::new_valid(100);
    for i in 0..100 {
        assert!(!bm.is_null(i));
        assert!(bm.is_valid(i));
    }
    assert_eq!(bm.null_count(), 0);
    assert_eq!(bm.valid_count(), 100);
    assert!(bm.is_all_valid());
}

#[test]
fn test_all_nulls() {
    let bm = NullBitmap::new_all_null(100);
    for i in 0..100 {
        assert!(bm.is_null(i));
        assert!(!bm.is_valid(i));
    }
    assert_eq!(bm.null_count(), 100);
    assert!(!bm.is_all_valid());
}

#[test]
fn test_mixed_nulls() {
    let flags = vec![false, true, false, false, true, true, false, true];
    let bm = NullBitmap::from_bools(&flags);
    assert!(!bm.is_null(0));
    assert!(bm.is_null(1));
    assert!(!bm.is_null(2));
    assert!(bm.is_null(4));
    assert_eq!(bm.null_count(), 4);
    assert_eq!(bm.valid_count(), 4);
    assert!(!bm.is_all_valid());
}

#[test]
fn test_set_clear() {
    let mut bm = NullBitmap::new_valid(10);
    assert!(!bm.is_null(5));
    bm.set_null(5);
    assert!(bm.is_null(5));
    bm.set_valid(5);
    assert!(!bm.is_null(5));
}

#[test]
fn test_roundtrip_bytes() {
    let flags: Vec<bool> = (0..200).map(|i| i % 3 == 0).collect();
    let bm = NullBitmap::from_bools(&flags);
    let bytes = bm.to_bytes();
    let bm2 = NullBitmap::from_bytes(&bytes, 200);
    for i in 0..200 {
        assert_eq!(bm.is_null(i), bm2.is_null(i), "mismatch at {}", i);
    }
}

#[test]
fn test_extend() {
    let mut bm1 = NullBitmap::from_bools(&[true, false, true]);
    let bm2 = NullBitmap::from_bools(&[false, true]);
    bm1.extend(&bm2);
    assert_eq!(bm1.len(), 5);
    assert!(bm1.is_null(0));
    assert!(!bm1.is_null(1));
    assert!(bm1.is_null(2));
    assert!(!bm1.is_null(3));
    assert!(bm1.is_null(4));
}
