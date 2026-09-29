use crate::protocol::types::DataType;
use crate::storage::catalog::schema::{PartitionType, TableMeta};
use crate::types::batch::ColumnDef;
use crate::types::scalar::ScalarValue;

use super::*;

fn partitioned_meta() -> (Schema, TableMeta) {
    let columns = vec![
        ColumnDef::new("id", DataType::Int64),
        ColumnDef::new("partition_key", DataType::Int32),
    ];
    let mut meta = TableMeta::new("main", "test_table", columns.clone());
    meta.partition_key = "partition_key".to_string();
    meta.partition_type = PartitionType::Year;
    meta.update_partition_key_idx();
    (Schema::new(columns), meta)
}

#[test]
fn delete_partition_eq_key_uses_partition_function() {
    let (schema, meta) = partitioned_meta();
    let where_clause = Some(Expr::BinaryOp {
        left: Box::new(Expr::Column("partition_key".to_string())),
        op: BinOp::Eq,
        right: Box::new(Expr::Literal(ScalarValue::Int64(Some(20260108)))),
    });

    assert_eq!(
        delete_partition_eq_key(&where_clause, &schema, &meta),
        Some("2026".to_string())
    );
}

#[test]
fn prune_segments_by_partition_keeps_only_matching_partition_files() {
    let (schema, meta) = partitioned_meta();
    let where_clause = Some(Expr::BinaryOp {
        left: Box::new(Expr::Column("partition_key".to_string())),
        op: BinOp::Eq,
        right: Box::new(Expr::Literal(ScalarValue::Int64(Some(20260108)))),
    });
    let files = vec![
        PathBuf::from("p2025_0001.dat"),
        PathBuf::from("p2026_0002.dat"),
        PathBuf::from("seg_00003.dat"),
    ];

    let pruned = prune_segments_by_partition(files, &where_clause, &schema, &meta);

    assert_eq!(
        pruned,
        vec![
            PathBuf::from("p2026_0002.dat"),
            PathBuf::from("seg_00003.dat")
        ]
    );
}

#[test]
fn cluster_key_eq_predicate_builds_exact_bounds() {
    let (schema, mut meta) = partitioned_meta();
    meta.cluster_key = vec!["partition_key".to_string(), "id".to_string()];
    let predicate = crate::storage::segment::RowGroupPredicate::new(vec![
        crate::storage::segment::RangeClause {
            column_index: schema.index_of("partition_key").unwrap(),
            op: RangeOp::Eq,
            value: ScalarValue::Int64(Some(20260108)),
        },
    ]);

    let bounds = cluster_key_bounds(&schema, &meta, &predicate).expect("bounds");

    assert_eq!(
        bounds.range_column_index,
        schema.index_of("partition_key").unwrap()
    );
    assert_eq!(
        bounds.lower,
        Some((ScalarValue::Int64(Some(20260108)), true))
    );
    assert_eq!(
        bounds.upper,
        Some((ScalarValue::Int64(Some(20260108)), true))
    );
}
