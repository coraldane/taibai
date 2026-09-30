use super::*;
use crate::types::batch::ColumnDef;

#[test]
fn typed_zone_maps_preserve_ranges_and_null_counts() {
    let schema = Schema::new(vec![
        ColumnDef::new("number", DataType::Int32),
        ColumnDef::new("text", DataType::VarChar),
    ]);
    let mut batch = RecordBatch::new(schema);
    for (number, text) in [
        (
            ScalarValue::Int32(Some(9)),
            ScalarValue::VarChar(Some("z".into())),
        ),
        (
            ScalarValue::Int32(None),
            ScalarValue::VarChar(Some("b".into())),
        ),
        (ScalarValue::Int32(Some(-4)), ScalarValue::VarChar(None)),
        (
            ScalarValue::Int32(Some(3)),
            ScalarValue::VarChar(Some("m".into())),
        ),
    ] {
        batch.column_mut(0).unwrap().push(&number).unwrap();
        batch.column_mut(1).unwrap().push(&text).unwrap();
    }

    let maps: Vec<ZoneMap> = (0..batch.num_columns())
        .map(|col_idx| compute_zone_map_from_column(batch.column(col_idx).unwrap(), 0, 4))
        .collect();
    assert_eq!(maps[0].min_value, ScalarValue::Int32(Some(-4)));
    assert_eq!(maps[0].max_value, ScalarValue::Int32(Some(9)));
    assert_eq!(maps[0].null_count, 1);
    assert_eq!(maps[1].min_value, ScalarValue::VarChar(Some("b".into())));
    assert_eq!(maps[1].max_value, ScalarValue::VarChar(Some("z".into())));
    assert_eq!(maps[1].null_count, 1);
    assert!(maps.iter().all(|map| map.row_count == 4));
}
