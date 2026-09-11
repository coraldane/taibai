use super::{ProfiledExec, QueryProfile};
use crate::protocol::types::DataType;
use crate::sql::execution::operators::scan::ScanExec;
use crate::sql::execution::operators::sort::SortExec;
use crate::sql::planner::{LogicalExpr, SortKey};
use crate::types::array::ColumnArray;
use crate::types::batch::{ColumnDef, RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

#[test]
fn profiled_sort_reports_rows_memory_and_spill_io() {
    let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
    let mut values = ColumnArray::empty(DataType::Int64);
    for value in (0..128i64).rev() {
        values.push(&ScalarValue::Int64(Some(value))).unwrap();
    }
    let batch = RecordBatch::try_new(schema.clone(), vec![values]).unwrap();
    let input = Box::new(ScanExec::new(vec![batch], None));
    let sort = SortExec::new(
        vec![SortKey {
            expr: LogicalExpr::Column(0),
            desc: false,
        }],
        input,
        schema,
    )
    .with_memory_limit_bytes(1);

    let query = QueryProfile::default();
    let operator = query.register(0, "Sort", 128);
    let mut profiled = ProfiledExec::wrap(Box::new(sort), operator);
    let mut rows = 0usize;
    while let Some(batch) = profiled.next_batch().unwrap() {
        rows += batch.num_rows();
    }
    assert_eq!(rows, 128);

    let metrics = query.snapshots();
    assert_eq!(metrics.len(), 1);
    assert_eq!(metrics[0].actual_rows, 128);
    assert!(metrics[0].peak_memory_bytes > 0);
    assert!(metrics[0].spill_files > 0);
    assert!(metrics[0].spill_rows >= 128);
    assert!(metrics[0].spill_bytes > 0);
}
