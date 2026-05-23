use crate::types::scalar::ScalarValue;

pub enum PartitionFunction {
    Day,
    Year,
    Value,
}

impl PartitionFunction {
    pub fn apply(&self, value: i64) -> i64 {
        match self {
            PartitionFunction::Day => value / 86400,
            PartitionFunction::Year => value / (86400 * 365),
            PartitionFunction::Value => value,
        }
    }
}

pub fn compute_partition_key_from_scalar(
    func: &PartitionFunction,
    val: &ScalarValue,
) -> String {
    match val {
        ScalarValue::Int64(Some(v)) => func.apply(*v).to_string(),
        ScalarValue::Int32(Some(v)) => func.apply(*v as i64).to_string(),
        ScalarValue::Timestamp(Some(v)) => func.apply(*v).to_string(),
        ScalarValue::VarChar(Some(s)) => s.clone(),
        _ => String::new(),
    }
}