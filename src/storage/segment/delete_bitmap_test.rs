use super::*;

#[test]
fn range_checks_match_per_bit_semantics() {
    // bits: rows 0..=8 deleted, 9.. not
    let mut bitmap = vec![0u8; 2];
    for row in 0..=8u64 {
        mark_deleted(&mut bitmap, row);
    }
    assert!(row_range_all_deleted(&bitmap, 0, 9));
    assert!(!row_range_all_deleted(&bitmap, 0, 10));
    assert!(row_range_has_deleted(&bitmap, 7, 5));
    assert!(!row_range_has_deleted(&bitmap, 9, 7));
    // Bits past the end count as not deleted.
    assert!(!row_range_all_deleted(&bitmap, 0, 17));
    assert!(!row_range_has_deleted(&bitmap, 16, 8));
    // Unaligned 64-bit window crossing two words.
    let mut wide = vec![0u8; 16];
    mark_deleted(&mut wide, 70);
    assert!(row_range_has_deleted(&wide, 64, 16));
    assert!(!row_range_has_deleted(&wide, 64, 6));
}
