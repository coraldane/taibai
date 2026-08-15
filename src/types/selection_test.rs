use super::SelectionVector;

#[test]
fn mask_is_converted_to_stable_row_ids() {
    assert_eq!(
        SelectionVector::from_mask(&[true, false, true, true]).indices(),
        &[0, 2, 3]
    );
}
