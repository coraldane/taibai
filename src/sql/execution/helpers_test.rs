use crate::common::error::{Result, TaibaiError};
use crate::protocol::types::DataType;
use crate::sql::parser::ast::Expr;
use crate::types::batch::Schema;
use crate::types::scalar::ScalarValue;

use super::*;

#[test]
fn sql_like_match_handles_wildcards_and_escapes() {
    assert!(sql_like_match("apple", "%appl%"));
    assert!(sql_like_match("apple", "app%"));
    assert!(sql_like_match("apple", "appl_"));
    assert!(sql_like_match("apple", "apple"));
    assert!(sql_like_match("apple", "%"));
    assert!(sql_like_match("", "%"));
    assert!(!sql_like_match("", "_"));
    assert!(!sql_like_match("apple", "Apple%"));
    assert!(!sql_like_match("banana", "%appl%"));
    assert!(sql_like_match("100%", "100\\%"));
    assert!(!sql_like_match("100x", "100\\%"));
    assert!(sql_like_match("a_b", "a\\_b"));
    assert!(!sql_like_match("axb", "a\\_b"));
    assert!(sql_like_match("axb", "a_b"));
    assert!(sql_like_match("abcabc", "%abc"));
    assert!(!sql_like_match("abcabd", "%abc"));
    assert!(sql_like_match("日本語", "日_語"));
}

#[test]
fn scalar_like_matches_never_matches_null() {
    assert!(!scalar_like_matches(&ScalarValue::VarChar(None), "%"));
    assert!(!scalar_like_matches(&ScalarValue::VarChar(None), "a%"));
    assert!(scalar_like_matches(
        &ScalarValue::VarChar(Some("apple".to_string())),
        "%pp%"
    ));
    assert!(!scalar_like_matches(
        &ScalarValue::VarChar(Some("apple".to_string())),
        "%zz%"
    ));
}

fn eval(value: ScalarValue, dt: DataType, scale: i32) -> Result<ScalarValue> {
    eval_expr_to_scalar_with_scale_timezone(&Expr::Literal(value), &dt, scale, 0)
}

#[test]
fn coerces_int32_param_into_varchar_column() {
    let got = eval(ScalarValue::Int32(Some(42)), DataType::VarChar, 0).unwrap();
    assert_eq!(got, ScalarValue::VarChar(Some("42".to_string())));
}

#[test]
fn coerces_numeric_and_bool_params_into_varchar_column() {
    let cases = vec![
        (ScalarValue::Int64(Some(-7)), "-7"),
        (ScalarValue::Int8(Some(8)), "8"),
        (ScalarValue::UInt64(Some(9)), "9"),
        (ScalarValue::Float64(Some(1.5)), "1.5"),
        (ScalarValue::Float32(Some(2.5)), "2.5"),
        (ScalarValue::Bool(Some(true)), "true"),
    ];
    for (input, want) in cases {
        let got = eval(input, DataType::VarChar, 0).unwrap();
        assert_eq!(got, ScalarValue::VarChar(Some(want.to_string())));
    }
}

#[test]
fn coerces_int32_param_into_decimal_column_with_scale() {
    let got = eval(ScalarValue::Int32(Some(42)), DataType::Decimal, 4).unwrap();
    assert_eq!(got, ScalarValue::Decimal(Some(420000)));
}

#[test]
fn coerces_cross_integer_params_via_fallback() {
    let got = eval(ScalarValue::Int32(Some(7)), DataType::Int64, 0).unwrap();
    assert_eq!(got, ScalarValue::Int64(Some(7)));

    let got = eval(ScalarValue::Int64(Some(300)), DataType::Int16, 0).unwrap();
    assert_eq!(got, ScalarValue::Int16(Some(300)));
}

#[test]
fn incompatible_param_type_returns_sql_error_not_internal_mismatch() {
    let err = eval(ScalarValue::VarChar(Some("abc".into())), DataType::Int32, 0).unwrap_err();
    assert!(
        matches!(err, TaibaiError::Sql(_)),
        "expected Sql error, got {:?}",
        err
    );
}

#[test]
fn null_param_stays_null_for_target_type() {
    let got = eval(ScalarValue::Int32(None), DataType::VarChar, 0).unwrap();
    assert_eq!(got, ScalarValue::VarChar(None));
}

/// The hashed `column IN (literals)` fast path keeps the generic loop's
/// mask exactly: only listed rows match, everything else (including NULL
/// column values) is excluded.
#[test]
fn in_literal_hash_mask_keeps_only_listed_rows() {
    use crate::sql::parser::ast::Expr;
    use crate::types::array::ColumnArray;
    use crate::types::batch::{ColumnDef, RecordBatch};

    let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int32)]);
    let mut ids = ColumnArray::empty(DataType::Int32);
    for i in 0..40 {
        ids.push(&ScalarValue::Int32(Some(i))).unwrap();
    }
    let batch = RecordBatch::try_new(schema.clone(), vec![ids]).unwrap();

    let values: Vec<Expr> = (0..5)
        .map(|i| Expr::Literal(ScalarValue::Int32(Some(i))))
        .collect();
    let expr = Expr::In {
        expr: Box::new(Expr::Column("id".to_string())),
        values,
        negated: false,
    };
    let mask = eval_where_clause_batch(&expr, &batch, &schema).unwrap();
    let kept: Vec<i32> = (0..40).filter(|i| mask[*i]).map(|i| i as i32).collect();
    assert_eq!(kept, vec![0, 1, 2, 3, 4], "mask: {mask:?}");
}
