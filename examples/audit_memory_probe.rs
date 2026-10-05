//! Diagnostic assertions demonstrating current memory-budget omissions.
//! After fixes, invert these expectations and move them into regression tests.

use taibai::common::config::{set_query_limits, QueryLimits};
use taibai::protocol::types::DataType;
use taibai::sql::execution::operators::{
    scan::ScanExec, topn::TopNExec, window::WindowExec, ExecutionOp,
};
use taibai::sql::planner::{LogicalExpr, SortKey, WindowFuncKind, WindowFuncPlan};
use taibai::types::{
    array::ColumnArray,
    batch::{ColumnDef, RecordBatch, Schema},
    scalar::ScalarValue,
};
fn main() {
    set_query_limits(QueryLimits {
        query_memory_limit_mb: 1,
        ..QueryLimits::default()
    })
    .unwrap();
    let schema = Schema::new(vec![ColumnDef::new("v", DataType::Int64)]);
    let mut col = ColumnArray::empty(DataType::Int64);
    for i in 0..200_000 {
        col.push(&ScalarValue::Int64(Some(i))).unwrap();
    }
    let batch = RecordBatch::try_new(schema.clone(), vec![col]).unwrap();
    let make_input = || Box::new(ScanExec::new(vec![batch.clone()], None)) as Box<dyn ExecutionOp>;
    let mut window = WindowExec::new(
        make_input(),
        vec![WindowFuncPlan {
            kind: WindowFuncKind::RowNumber,
            partition_by: vec![],
            order_by: vec![],
            frame: None,
            output_name: "rn".into(),
        }],
        schema.clone(),
    );
    let out = window.next_batch().unwrap().unwrap();
    println!(
        "Window with 1 MiB budget retained {} bytes, {} rows",
        out.estimated_size_bytes(),
        out.num_rows()
    );
    assert!(out.estimated_size_bytes() > 1024 * 1024);
    let mut top = TopNExec::new(
        vec![SortKey {
            expr: LogicalExpr::Column(0),
            desc: false,
        }],
        make_input(),
        schema,
    )
    .with_capacity(190_000);
    let out = top.next_batch().unwrap().unwrap();
    println!(
        "TopN with 1 MiB budget retained {} bytes, {} rows",
        out.estimated_size_bytes(),
        out.num_rows()
    );
    assert!(out.estimated_size_bytes() > 1024 * 1024);

    std::env::set_var("TAIBAI_TXN_MUTATION_BUDGET_BYTES", "1024");
    let dir = tempfile::tempdir().unwrap();
    let engine = taibai::sql::execution::engine::Engine::new(dir.path(), "main").unwrap();
    let mut session = taibai::net::session::Session::new("root".into(), "main".into());
    engine
        .execute(&mut session, "CREATE TABLE mutations (id INT64, v INT64)")
        .unwrap();
    let values = (0..1000)
        .map(|i| format!("({i},10)"))
        .collect::<Vec<_>>()
        .join(",");
    engine
        .execute(
            &mut session,
            &format!("INSERT INTO mutations VALUES {values}"),
        )
        .unwrap();
    let resident = engine.execute(&mut session, "UPDATE mutations SET v = 20");
    println!("Resident UPDATE with 1 KiB mutation budget => {resident:?}");
    assert!(resident.is_ok());
    engine
        .execute(&mut session, "FLUSH TABLE mutations")
        .unwrap();
    let segment = engine.execute(&mut session, "UPDATE mutations SET v = 30");
    println!("Segment UPDATE with 1 KiB mutation budget => {segment:?}");
    assert!(segment.is_err());
    engine.segment_mgr().shutdown();
}
