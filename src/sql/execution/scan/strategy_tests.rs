use super::*;
use crate::protocol::types::DataType;
use crate::sql::execution::scan::spec::{ScanSortKey, ScanSpec};
use crate::storage::catalog::schema::TableMeta;
use crate::types::batch::{ColumnDef, Schema};
use crate::types::scalar::ScalarValue;

fn clustered_schema() -> Schema {
    Schema::new(vec![
        ColumnDef::new("k", DataType::VarChar),
        ColumnDef::new("p", DataType::Int32),
        ColumnDef::new("v", DataType::Float64),
    ])
}

fn clustered_meta(schema: &Schema) -> TableMeta {
    let mut meta = TableMeta::new("main", "test_table", schema.columns.clone());
    meta.cluster_key = vec!["k".to_string(), "p".to_string()];
    meta
}

#[test]
fn ordered_cluster_limit_allows_scan_limit_for_eq_prefix_and_desc_next_key() {
    let schema = clustered_schema();
    let meta = clustered_meta(&schema);
    let mut spec = ScanSpec::new("test_table");
    spec.cluster_key = Some(meta.cluster_key.clone());
    spec.eq_predicates
        .push((0, ScalarValue::VarChar(Some("k1".to_string()))));
    spec.has_order_by = true;
    spec.limit = Some(121);
    spec.order_by.push(ScanSortKey {
        column_index: 1,
        desc: true,
    });

    let plan = ScanPlanner::new(&spec, &schema, &meta).build();

    assert_eq!(plan.scan_limit, Some(121));
    assert_eq!(
        plan.ordered_limit,
        Some(OrderedClusterLimit {
            order_column_index: 1,
            desc: true
        })
    );
}

#[test]
fn ordered_cluster_order_does_not_require_limit() {
    let schema = clustered_schema();
    let meta = clustered_meta(&schema);
    let mut spec = ScanSpec::new("test_table");
    spec.cluster_key = Some(meta.cluster_key.clone());
    spec.eq_predicates
        .push((0, ScalarValue::VarChar(Some("k1".to_string()))));
    spec.has_order_by = true;
    spec.order_by.push(ScanSortKey {
        column_index: 1,
        desc: true,
    });

    let plan = ScanPlanner::new(&spec, &schema, &meta).build();

    assert_eq!(plan.scan_limit, None);
    assert_eq!(
        plan.ordered_limit,
        Some(OrderedClusterLimit {
            order_column_index: 1,
            desc: true
        })
    );
}

#[test]
fn ordered_cluster_limit_requires_eq_prefix() {
    let schema = clustered_schema();
    let meta = clustered_meta(&schema);
    let mut spec = ScanSpec::new("test_table");
    spec.cluster_key = Some(meta.cluster_key.clone());
    spec.has_order_by = true;
    spec.limit = Some(121);
    spec.order_by.push(ScanSortKey {
        column_index: 1,
        desc: true,
    });

    let plan = ScanPlanner::new(&spec, &schema, &meta).build();

    assert_eq!(plan.scan_limit, None);
    assert_eq!(plan.ordered_limit, None);
}
