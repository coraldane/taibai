use super::*;
use crate::protocol::types::DataType;
use crate::storage::segment::constants::SEGMENT_HEADER_SIZE;
use crate::types::array::ColumnArray;
use crate::types::batch::ColumnDef;
use crate::types::scalar::ScalarValue;

#[test]
fn first_owned_batch_keeps_variable_buffer_allocation() {
    let schema = Schema::new(vec![ColumnDef::new("value", DataType::VarChar)]);
    let mut batch = RecordBatch::with_capacity(schema.clone(), 4);
    batch
        .column_mut(0)
        .unwrap()
        .push(&ScalarValue::VarChar(Some("owned-buffer".repeat(64))))
        .unwrap();
    let source_ptr = match batch.column(0).unwrap() {
        ColumnArray::VarChar { data, .. } => data.values().as_ptr(),
        _ => unreachable!(),
    };

    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("owned.dat");
    let file = std::fs::File::create(&path).unwrap();
    let mut writer = ActiveSegmentWriter {
        file: std::io::BufWriter::new(file),
        path,
        schema,
        row_groups: Vec::new(),
        current_size: SEGMENT_HEADER_SIZE as u64,
        total_rows: 0,
        delete_bitmap: Vec::new(),
        deleted_row_count: 0,
        buffered_batch: None,
        buffered_rows: 0,
        dirty_meta: false,
        data_end: SEGMENT_HEADER_SIZE as u64,
        sidecar_row_groups_flushed: 0,
        last_write_at: Instant::now(),
        row_group_target_rows: 1024,
        pubsub: None,
        pending_publish: Vec::new(),
        pubsub_deferred_ranges: Vec::new(),
        compression_level: 3,
    };

    writer.append_batch_owned(batch, false, &|_| {}).unwrap();
    let buffered_ptr = match writer.buffered_batch.as_ref().unwrap().column(0).unwrap() {
        ColumnArray::VarChar { data, .. } => data.values().as_ptr(),
        _ => unreachable!(),
    };
    assert_eq!(buffered_ptr, source_ptr, "owned append must not clone data");
}
