use super::*;
use crate::protocol::types::DataType;
use crate::types::batch::ColumnDef;

#[test]
fn partition_range_is_discovered_below_topn() {
    let schema = Schema::new(vec![ColumnDef::new("deal_date", DataType::Int32)]);
    let scan = LogicalPlan::Scan(ScanPlan::new("daily".to_string(), schema));
    let filter = LogicalPlan::Filter(FilterPlan {
        predicate: LogicalExpr::BinaryOp {
            left: Box::new(LogicalExpr::Column(0)),
            op: BinOp::Lt,
            right: Box::new(LogicalExpr::Literal(ScalarValue::Int64(Some(20251231)))),
        },
        input: Box::new(scan),
    });
    let plan = LogicalPlan::TopN(TopNPlan {
        sort_keys: vec![SortKey {
            expr: LogicalExpr::Column(0),
            desc: true,
        }],
        limit: 121,
        offset: 0,
        input: Box::new(filter),
    });

    let (optimized, changed) = apply_partition_pruning(plan);
    assert!(changed);

    let LogicalPlan::TopN(topn) = optimized else {
        panic!("expected topn");
    };
    let LogicalPlan::Filter(filter) = *topn.input else {
        panic!("expected filter");
    };
    let LogicalPlan::Scan(scan) = *filter.input else {
        panic!("expected scan");
    };
    assert_eq!(
        scan.time_range,
        Some(TimeRange {
            column: "deal_date".to_string(),
            start: None,
            end: Some(ScalarValue::Int64(Some(20251231))),
        })
    );
}
