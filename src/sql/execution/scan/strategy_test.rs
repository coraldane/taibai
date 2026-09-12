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

#[test]
fn ordered_cluster_limit_allows_first_cluster_key_without_eq_prefix() {
    let schema = clustered_schema();
    let meta = clustered_meta(&schema);
    let mut spec = ScanSpec::new("test_table");
    spec.cluster_key = Some(meta.cluster_key.clone());
    spec.has_order_by = true;
    spec.limit = Some(121);
    spec.order_by.push(ScanSortKey {
        column_index: 0,
        desc: false,
    });

    let plan = ScanPlanner::new(&spec, &schema, &meta).build();

    assert_eq!(plan.scan_limit, Some(121));
    assert_eq!(
        plan.ordered_limit,
        Some(OrderedClusterLimit {
            order_column_index: 0,
            desc: false
        })
    );
}

#[test]
fn ordered_cluster_limit_allows_first_cluster_key_desc() {
    let schema = clustered_schema();
    let meta = clustered_meta(&schema);
    let mut spec = ScanSpec::new("test_table");
    spec.cluster_key = Some(meta.cluster_key.clone());
    spec.has_order_by = true;
    spec.limit = Some(121);
    spec.order_by.push(ScanSortKey {
        column_index: 0,
        desc: true,
    });

    let plan = ScanPlanner::new(&spec, &schema, &meta).build();

    assert_eq!(plan.scan_limit, Some(121));
    assert_eq!(
        plan.ordered_limit,
        Some(OrderedClusterLimit {
            order_column_index: 0,
            desc: true
        })
    );
}

#[test]
fn cluster_key_bounds_build_composite_prefix_eq_plus_range() {
    let schema = clustered_schema();
    let meta = clustered_meta(&schema);
    let mut spec = ScanSpec::new("test_table");
    spec.cluster_key = Some(meta.cluster_key.clone());
    spec.eq_predicates
        .push((0, ScalarValue::VarChar(Some("k1".to_string()))));
    spec.range_predicates
        .push(crate::storage::partition::pruning::RangeClause {
            column_index: 1,
            op: crate::storage::partition::pruning::RangeOp::Ge,
            value: ScalarValue::Int32(Some(100)),
        });
    spec.range_predicates
        .push(crate::storage::partition::pruning::RangeClause {
            column_index: 1,
            op: crate::storage::partition::pruning::RangeOp::Lt,
            value: ScalarValue::Int32(Some(200)),
        });

    let plan = ScanPlanner::new(&spec, &schema, &meta).build();
    let bounds = plan.cluster_key_bounds.expect("composite bounds");
    assert_eq!(
        bounds.prefix_eq,
        vec![(0, ScalarValue::VarChar(Some("k1".to_string())))]
    );
    assert_eq!(bounds.range_column_index, 1);
    assert_eq!(bounds.lower, Some((ScalarValue::Int32(Some(100)), true)));
    assert_eq!(bounds.upper, Some((ScalarValue::Int32(Some(200)), false)));
    assert_eq!(plan.strategy, RowGroupReadStrategy::ClusterKeyRangeRead);
}

#[test]
fn cluster_key_bounds_prefix_only_equality_stays_on_sorted_search() {
    let schema = clustered_schema();
    let meta = clustered_meta(&schema);
    let mut spec = ScanSpec::new("test_table");
    spec.cluster_key = Some(meta.cluster_key.clone());
    spec.eq_predicates
        .push((0, ScalarValue::VarChar(Some("k1".to_string()))));

    let plan = ScanPlanner::new(&spec, &schema, &meta).build();
    let bounds = plan.cluster_key_bounds.expect("prefix bounds");
    assert!(bounds.prefix_eq.is_empty());
    assert_eq!(bounds.range_column_index, 0);
    assert_eq!(
        bounds.lower,
        Some((ScalarValue::VarChar(Some("k1".to_string())), true))
    );
    assert_eq!(
        bounds.upper,
        Some((ScalarValue::VarChar(Some("k1".to_string())), true))
    );
}

#[test]
fn ordered_cluster_order_matches_multi_column_cluster_prefix() {
    let schema = clustered_schema();
    let meta = clustered_meta(&schema);
    let mut spec = ScanSpec::new("test_table");
    spec.cluster_key = Some(meta.cluster_key.clone());
    spec.has_order_by = true;
    spec.order_by.push(ScanSortKey {
        column_index: 0,
        desc: false,
    });
    spec.order_by.push(ScanSortKey {
        column_index: 1,
        desc: false,
    });

    let plan = ScanPlanner::new(&spec, &schema, &meta).build();
    assert_eq!(
        plan.ordered_limit,
        Some(OrderedClusterLimit {
            order_column_index: 0,
            desc: false
        })
    );
}

#[test]
fn ordered_cluster_order_rejects_mixed_directions_or_gaps() {
    let schema = clustered_schema();
    let meta = clustered_meta(&schema);
    let mut spec = ScanSpec::new("test_table");
    spec.cluster_key = Some(meta.cluster_key.clone());
    spec.has_order_by = true;
    spec.order_by.push(ScanSortKey {
        column_index: 0,
        desc: false,
    });
    spec.order_by.push(ScanSortKey {
        column_index: 1,
        desc: true,
    });
    let plan = ScanPlanner::new(&spec, &schema, &meta).build();
    assert_eq!(
        plan.ordered_limit, None,
        "mixed directions are not reversible"
    );

    let mut spec = ScanSpec::new("test_table");
    spec.cluster_key = Some(meta.cluster_key.clone());
    spec.has_order_by = true;
    // v then k skips the leading cluster column without an eq pin.
    spec.order_by.push(ScanSortKey {
        column_index: 1,
        desc: false,
    });
    spec.order_by.push(ScanSortKey {
        column_index: 2,
        desc: false,
    });
    let plan = ScanPlanner::new(&spec, &schema, &meta).build();
    assert_eq!(plan.ordered_limit, None, "cluster-key gaps are not ordered");
}
