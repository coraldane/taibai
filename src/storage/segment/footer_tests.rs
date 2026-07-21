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
        },
    ];
    let footer = encode_segment_footer(&schema, 2, &row_groups, &[0], 1, "t", &[]);

    assert_eq!(
        reserved_section_offset(&footer).unwrap(),
        footer.len() - std::mem::size_of::<u64>() - std::mem::size_of::<u32>() - 1
    );
    parse_segment_footer(&footer).unwrap();
}
