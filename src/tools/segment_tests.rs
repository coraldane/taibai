use super::*;
use crate::protocol::types::DataType;
use crate::storage::segment::SegmentWriter;
use crate::types::batch::{ColumnDef, RecordBatch, Schema};
use crate::types::scalar::ScalarValue;
use tempfile::TempDir;

#[test]
fn inspect_segment_skips_segment_zone_maps() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("seg_00001.dat");
    let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);

    {
        let mut writer = SegmentWriter::create(&path, schema.clone(), 4).unwrap();
        let mut batch = RecordBatch::new(schema);
        for value in 0..10i64 {
            batch
                .column_mut(0)
                .unwrap()
                .push(&ScalarValue::Int64(Some(value)))
                .unwrap();
        }
        writer.write_batch(&batch).unwrap();
        writer.seal().unwrap();
    }

    let info = inspect_segment(&path).unwrap();
    assert_eq!(info.row_group_count, 3);
    assert_eq!(info.footer_row_count, 10);
    assert_eq!(
        info.row_groups
            .iter()
            .map(|rg| rg.row_count)
            .collect::<Vec<_>>(),
        vec![4, 4, 2]
    );
    assert_eq!(info.row_groups[0].offset, SEGMENT_HEADER_SIZE as u64);
}
