use super::*;

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
