use super::*;
use crate::types::array::ColumnArray;
use crate::types::scalar::ScalarValue;

#[test]
fn project_preserves_decimal_scale_for_plain_columns() {
    let schema = Schema::new(vec![
        ColumnDef::new("name", DataType::VarChar),
        ColumnDef::new("score", DataType::Decimal)
            .with_scale(2)
            .with_precision(9),
    ]);
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            ColumnArray::from_scalar_values(
                DataType::VarChar,
                &[ScalarValue::VarChar(Some("alice".to_string()))],
            )
            .unwrap(),
            ColumnArray::from_scalar_values(DataType::Decimal, &[ScalarValue::Decimal(Some(1234))])
                .unwrap(),
        ],
    )
    .unwrap();

    let mut exec = ProjectExec {
        expressions: vec![LogicalExpr::Column(1)],
        aliases: vec![None],
        input: Box::new(crate::sql::execution::operators::scan::ScanExec::new(
            vec![batch],
            None,
        )),
        input_schema: schema,
        subqueries: None,
        remapped: None,
    };

    let result = exec.next_batch().unwrap().unwrap();

    assert_eq!(result.schema.columns[0].name, "score");
    assert_eq!(result.schema.columns[0].data_type, DataType::Decimal);
    assert_eq!(result.schema.columns[0].scale, 2);
    assert_eq!(result.schema.columns[0].precision, 9);
    assert_eq!(
        result.scalar(0, 0).unwrap(),
        ScalarValue::Decimal(Some(1234))
    );
}

#[test]
fn project_reorders_plain_columns_without_rebuilding_scalars() {
    let schema = Schema::new(vec![
        ColumnDef::new("name", DataType::VarChar),
        ColumnDef::new("amount", DataType::Int64),
        ColumnDef::new("created_at", DataType::Timestamp),
    ]);
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            ColumnArray::from_scalar_values(
                DataType::VarChar,
                &[
                    ScalarValue::VarChar(Some("alice".to_string())),
                    ScalarValue::VarChar(Some("alice".to_string())),
                ],
            )
            .unwrap(),
            ColumnArray::from_scalar_values(
                DataType::Int64,
                &[ScalarValue::Int64(Some(10)), ScalarValue::Int64(Some(20))],
            )
            .unwrap(),
            ColumnArray::from_scalar_values(
                DataType::Timestamp,
                &[
                    ScalarValue::Timestamp(Some(1000)),
                    ScalarValue::Timestamp(Some(2000)),
                ],
            )
            .unwrap(),
        ],
    )
    .unwrap();

    let mut exec = ProjectExec {
        expressions: vec![LogicalExpr::Column(2), LogicalExpr::Column(0)],
        aliases: vec![Some("lt".to_string()), None],
        input: Box::new(crate::sql::execution::operators::scan::ScanExec::new(
            vec![batch],
            None,
        )),
        input_schema: schema,
        subqueries: None,
        remapped: None,
    };

    let result = exec.next_batch().unwrap().unwrap();

    assert_eq!(result.schema.columns[0].name, "lt");
    assert_eq!(result.schema.columns[1].name, "name");
    assert_eq!(
        result.scalar(0, 0).unwrap(),
        ScalarValue::Timestamp(Some(1000))
    );
    assert_eq!(
        result.scalar(1, 1).unwrap(),
        ScalarValue::VarChar(Some("alice".to_string()))
    );
}
