use super::*;
use crate::storage::codec::io_util::encode_len_prefixed_str;
use crate::storage::codec::scalar_tag::encode_scalar_with_tag;
use crate::storage::segment::writer::encode_segment_footer;

#[test]
fn parses_footer_with_u8_decimal_meta() {
    let mut footer = Vec::new();
    footer.extend_from_slice(&SEGMENT_MAGIC.to_le_bytes());
    footer.extend_from_slice(&crate::storage::segment::SEGMENT_VERSION.to_le_bytes());
    footer.extend_from_slice(&0u64.to_le_bytes());
    footer.extend_from_slice(&1u64.to_le_bytes());
    footer.extend_from_slice(&1u32.to_le_bytes());
    footer.extend_from_slice(&0u64.to_le_bytes());
    footer.extend_from_slice(&0u64.to_le_bytes());

    encode_len_prefixed_str(&mut footer, "t");
    footer.extend_from_slice(&1u32.to_le_bytes());
    encode_len_prefixed_str(&mut footer, "id");
    footer.push(DataType::Int64 as u8);
    footer.push(1);
    footer.push(0);
    footer.push(0);
    footer.push(0);
    footer.extend_from_slice(&1u32.to_le_bytes());

    footer.extend_from_slice(&32u64.to_le_bytes());
    footer.extend_from_slice(&8u64.to_le_bytes());
    footer.extend_from_slice(&1u64.to_le_bytes());
    footer.extend_from_slice(&8u64.to_le_bytes());
    footer.extend_from_slice(&8u64.to_le_bytes());
    footer.extend_from_slice(&8u64.to_le_bytes());
    encode_scalar_with_tag(&mut footer, &ScalarValue::Int64(Some(1)));
    encode_scalar_with_tag(&mut footer, &ScalarValue::Int64(Some(1)));
    footer.extend_from_slice(&0u64.to_le_bytes());
    footer.extend_from_slice(&1u64.to_le_bytes());
    footer.push(0);

    footer.extend_from_slice(&0u64.to_le_bytes());
    footer.extend_from_slice(&0u32.to_le_bytes());

    let parsed = parse_segment_footer(&footer).unwrap();

    assert_eq!(parsed.row_count, 1);
    assert_eq!(parsed.row_groups.len(), 1);
    assert_eq!(parsed.schema.columns[0].name, "id");
    assert_eq!(parsed.schema.columns[0].scale, 0);
    assert_eq!(
        reserved_section_offset(&footer).unwrap(),
        footer.len() - std::mem::size_of::<u64>() - std::mem::size_of::<u32>()
    );
}

#[test]
fn reserved_section_offset_skips_value_sets_for_all_row_groups() {
    let schema = Schema::new(vec![ColumnDef {
        name: "id".to_string(),
        data_type: DataType::Int64,
        nullable: true,
        scale: 0,
        precision: 0,
        auto_increment: false,
        primary_key: false,
        default_value: None,
        column_id: 1,
        previous_names: Vec::new(),
            added_at_micros: None,
    }]);
    let row_groups = vec![
        RowGroupMeta {
            offset: 32,
            length: 8,
            row_count: 1,
            column_compressed_sizes: vec![8],
            compressed_sizes: vec![8],
            uncompressed_sizes: vec![8],
            zone_maps: vec![ZoneMap {
                min_value: ScalarValue::Int64(Some(1)),
                max_value: ScalarValue::Int64(Some(1)),
                null_count: 0,
                row_count: 1,
            }],
            bloom_filters: vec![None],
            value_sets: vec![Some(vec![ScalarValue::Int64(Some(1))])],
            col_sums: vec![12345],
            column_crcs: Vec::new(),
        },
        RowGroupMeta {
            offset: 40,
            length: 8,
            row_count: 1,
            column_compressed_sizes: vec![8],
            compressed_sizes: vec![8],
            uncompressed_sizes: vec![8],
            zone_maps: vec![ZoneMap {
                min_value: ScalarValue::Int64(Some(2)),
                max_value: ScalarValue::Int64(Some(2)),
                null_count: 0,
                row_count: 1,
            }],
            bloom_filters: vec![None],
            value_sets: vec![Some(vec![ScalarValue::Int64(Some(2))])],
            // No sums: footers re-encoded from pre-extension segments keep
            // omitting the block, and the parser must leave them empty.
            col_sums: Vec::new(),
            column_crcs: Vec::new(),
        },
    ];
    let footer = encode_segment_footer(&schema, 2, &row_groups, &[0], 1, "t", &[]);

    assert_eq!(
        reserved_section_offset(&footer).unwrap(),
        // trailing reserved section = deleted_row_count(8) + bitmap_len(4) +
        // bitmap(1) + footer-crc extension(8)
        footer.len() - 8 - 4 - 1 - 8
    );
    let parsed = parse_segment_footer(&footer).unwrap();
    assert_eq!(parsed.row_groups[0].col_sums, vec![12345]);
    assert!(parsed.row_groups[1].col_sums.is_empty());
}

// ---------------------------------------------------------------------------
// Checksum extensions
// ---------------------------------------------------------------------------

fn checksum_test_schema() -> crate::types::batch::Schema {
    crate::types::batch::Schema::new(vec![crate::types::batch::ColumnDef::new(
        "id",
        DataType::Int64,
    )])
}

fn checksum_test_row_group() -> crate::storage::segment::RowGroupMeta {
    use crate::storage::segment::RowGroupMeta;
    RowGroupMeta {
        offset: 32,
        length: 8,
        row_count: 1,
        column_compressed_sizes: vec![8],
        compressed_sizes: vec![8],
        uncompressed_sizes: vec![8],
        zone_maps: vec![crate::storage::partition::pruning::ZoneMap {
            min_value: ScalarValue::Int64(Some(1)),
            max_value: ScalarValue::Int64(Some(1)),
            null_count: 0,
            row_count: 1,
        }],
        bloom_filters: vec![None],
        value_sets: vec![None],
        col_sums: Vec::new(),
        column_crcs: Vec::new(),
    }
}

#[test]
fn footer_checksum_detects_corruption() {
    let footer = encode_segment_footer(
        &checksum_test_schema(),
        1,
        &[checksum_test_row_group()],
        &[],
        0,
        "t",
        &[],
    );
    // New footers carry the CRC extension and parse cleanly.
    assert!(parse_segment_footer(&footer).is_ok());

    // Flip one byte in the middle: the whole-footer checksum must reject it.
    let mut corrupted = footer.clone();
    let mid = corrupted.len() / 2;
    corrupted[mid] ^= 0xFF;
    let err = match parse_segment_footer(&corrupted) {
        Err(error) => error.to_string(),
        Ok(_) => panic!("corrupted footer must not parse"),
    };
    assert!(
        err.contains("checksum"),
        "expected checksum failure, got: {err}"
    );
}

#[test]
fn column_block_checksums_round_trip_through_footer() {
    let mut row_group = checksum_test_row_group();
    row_group.column_crcs = vec![0x1234_5678];
    let footer = encode_segment_footer(&checksum_test_schema(), 1, &[row_group], &[], 0, "t", &[]);
    let parsed = parse_segment_footer(&footer).unwrap();
    assert_eq!(parsed.row_groups[0].column_crcs, vec![0x1234_5678]);
}

#[test]
fn old_footers_without_checksum_extension_still_parse() {
    // Footers written before the extension have no trailing CRC block; the
    // parser must accept them (back-compat).
    let footer = encode_segment_footer(
        &checksum_test_schema(),
        2,
        &[checksum_test_row_group(), checksum_test_row_group()],
        &[0],
        1,
        "t",
        &[],
    );
    let legacy = footer[..footer.len() - 8].to_vec();
    let parsed = parse_segment_footer(&legacy).unwrap();
    assert_eq!(parsed.row_groups.len(), 2);
}

#[test]
fn footer_tail_tlv_round_trips_and_keeps_reserved_offset() {
    use crate::storage::segment::footer_ext::{
        append_footer_crc, encode_footer_tail, FooterTailEntry,
    };
    let footer = encode_segment_footer(
        &checksum_test_schema(),
        1,
        &[checksum_test_row_group()],
        &[],
        0,
        "t",
        &[],
    );
    // Rebuild with a TLV block between the delete bitmap and the checksum.
    let body = footer[..footer.len() - 8].to_vec();
    let entries = vec![
        FooterTailEntry {
            tag: 7,
            payload: vec![1, 2, 3],
        },
        FooterTailEntry {
            tag: 9,
            payload: b"feature".to_vec(),
        },
    ];
    let mut rebuilt = body.clone();
    rebuilt.extend_from_slice(&encode_footer_tail(&entries));
    append_footer_crc(&mut rebuilt);

    let parsed = parse_segment_footer(&rebuilt).unwrap();
    assert_eq!(parsed.tail, entries);
    // The reserved section ends before the delete bitmap, so appending tail
    // entries must not move it (footer rewrites keep their boundary).
    assert_eq!(
        reserved_section_offset(&rebuilt).unwrap(),
        reserved_section_offset(&body).unwrap()
    );
}

#[test]
fn empty_footer_tail_encodes_to_nothing() {
    use crate::storage::segment::footer_ext::encode_footer_tail;
    assert!(encode_footer_tail(&[]).is_empty());
}
