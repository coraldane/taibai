use super::SpillFile;
use crate::protocol::types::DataType;
use crate::types::array::ColumnArray;
use crate::types::batch::{ColumnDef, RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

#[test]
fn spill_round_trip_and_cleanup() {
    let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
    let mut column = ColumnArray::empty(DataType::Int64);
    column.push(&ScalarValue::Int64(Some(7))).unwrap();
    let batch = RecordBatch::try_new(schema.clone(), vec![column]).unwrap();
    let path;
    {
        let spill = SpillFile::write_batches(schema, &[batch], 16).unwrap();
        path = spill.path().to_path_buf();
        assert_eq!(spill.open_reader().unwrap().row_count(), 1);
    }
    assert!(!path.exists());
}

impl SpillFile {
    pub fn path(&self) -> &std::path::Path {
        &self.path
    }
}
