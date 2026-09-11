use super::*;

#[test]
fn parses_regular_segment_names() {
    assert!(is_segment_filename("seg_00042.dat"));
    assert_eq!(parse_segment_seq("seg_00042.dat"), Some(42));
    assert!(!is_segment_filename("seg_00042.tmp"));
}

#[test]
fn parses_partition_segment_names() {
    assert!(is_segment_filename("p20260601_0007.dat"));
    assert_eq!(parse_segment_seq("p20260601_0007.dat"), Some(7));
    assert_eq!(
        parse_partition_seq("p20260601_0007.dat", "20260601"),
        Some(7)
    );
    assert_eq!(parse_partition_seq("p20260601_0007.dat", "20260602"), None);
}
