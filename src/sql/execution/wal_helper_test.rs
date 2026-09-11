use crate::common::error::Result;
use crate::protocol::types::DataType;
use crate::sql::execution::wal_helper::{deserialize_batch_from_wal, serialize_batch_for_wal};
use crate::types::batch::{ColumnDef, RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

fn assert_round_trip(batch: &RecordBatch) -> Result<()> {
    let bytes = serialize_batch_for_wal(batch);
    let restored = deserialize_batch_from_wal(&bytes, &batch.schema)?;
    assert_eq!(restored.num_rows(), batch.num_rows());
    assert_eq!(restored.num_columns(), batch.num_columns());
    for col_idx in 0..batch.num_columns() {
        for row in 0..batch.num_rows() {
            let expected = batch.scalar(row, col_idx);
            let actual = restored.scalar(row, col_idx);
            assert_eq!(
                actual, expected,
                "mismatch at column {} row {}",
                col_idx, row
            );
        }
    }
    Ok(())
}

#[test]
fn round_trip_fixed_width_column_with_nulls() -> Result<()> {
    let schema = Schema::new(vec![
        ColumnDef::new("id", DataType::Int64),
        ColumnDef::new("price", DataType::Float64),
    ]);
    let mut batch = RecordBatch::new(schema);
    let values: [Option<i64>; 5] = [Some(1), None, Some(3), None, Some(5)];
    for v in values {
        batch
            .column_mut(0)
            .unwrap()
            .push(&match v {
                Some(v) => ScalarValue::Int64(Some(v)),
                None => ScalarValue::Int64(None),
            })
            .unwrap();
        batch
            .column_mut(1)
            .unwrap()
            .push(&match v {
                Some(v) => ScalarValue::Float64(Some(v as f64 * 1.5)),
                None => ScalarValue::Float64(None),
            })
            .unwrap();
    }
    assert_round_trip(&batch)
}

#[test]
fn round_trip_varchar_column_with_nulls() -> Result<()> {
    let schema = Schema::new(vec![ColumnDef::new("name", DataType::VarChar)]);
    let mut batch = RecordBatch::new(schema);
    let values = ["a", "", "longer string here", "", "tail"];
    for (idx, v) in values.iter().enumerate() {
        let scalar = if idx == 1 || idx == 3 {
            ScalarValue::VarChar(None)
        } else {
            ScalarValue::VarChar(Some((*v).to_string()))
        };
        batch.column_mut(0).unwrap().push(&scalar).unwrap();
    }
    assert_round_trip(&batch)
}

#[test]
fn round_trip_mixed_columns_with_leading_nulls() -> Result<()> {
    let schema = Schema::new(vec![
        ColumnDef::new("sym", DataType::VarChar),
        ColumnDef::new("qty", DataType::Int32),
        ColumnDef::new("flag", DataType::Bool),
    ]);
    let mut batch = RecordBatch::new(schema);
    for row in 0..6i32 {
        let sym = if row % 2 == 0 {
            ScalarValue::VarChar(None)
        } else {
            ScalarValue::VarChar(Some(format!("S{}", row)))
        };
        let qty = if row == 0 {
            ScalarValue::Int32(None)
        } else {
            ScalarValue::Int32(Some(row * 100))
        };
        let flag = ScalarValue::Bool(Some(row % 3 == 0));
        batch.column_mut(0).unwrap().push(&sym).unwrap();
        batch.column_mut(1).unwrap().push(&qty).unwrap();
        batch.column_mut(2).unwrap().push(&flag).unwrap();
    }
    assert_round_trip(&batch)
}

#[test]
fn round_trip_all_null_column() -> Result<()> {
    let schema = Schema::new(vec![ColumnDef::new("note", DataType::VarChar)]);
    let mut batch = RecordBatch::new(schema);
    for _ in 0..4 {
        batch
            .column_mut(0)
            .unwrap()
            .push(&ScalarValue::VarChar(None))
            .unwrap();
    }
    assert_round_trip(&batch)
}

#[test]
fn round_trip_blob_and_vector_with_nulls() -> Result<()> {
    let schema = Schema::new(vec![
        ColumnDef::new("blob", DataType::Blob),
        ColumnDef::new("vec", DataType::Vector),
    ]);
    let mut batch = RecordBatch::new(schema);
    for row in 0..4i32 {
        let blob = if row == 1 {
            ScalarValue::Blob(None)
        } else {
            ScalarValue::Blob(Some(vec![row as u8, 0xAA]))
        };
        let vec = if row == 2 {
            ScalarValue::Vector(None)
        } else {
            ScalarValue::Vector(Some(vec![1.5f32, -2.5]))
        };
        batch.column_mut(0).unwrap().push(&blob).unwrap();
        batch.column_mut(1).unwrap().push(&vec).unwrap();
    }
    assert_round_trip(&batch)
}
