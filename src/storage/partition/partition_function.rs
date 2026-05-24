use crate::types::scalar::ScalarValue;

#[derive(Debug, PartialEq)]
pub enum PartitionFunction {
    Day,
    Year,
    Month,
    Value,
    Hash(i32),
}

impl PartitionFunction {
    pub fn apply(&self, value: i64) -> i64 {
        match self {
            PartitionFunction::Day => value / 86400,
            PartitionFunction::Year => value / (86400 * 365),
            PartitionFunction::Month => value / (86400 * 30),
            PartitionFunction::Value => value,
            PartitionFunction::Hash(buckets) => {
                if *buckets > 0 {
                    (value.abs() % (*buckets as i64)) as i64
                } else {
                    value
                }
            }
        }
    }
    
    pub fn apply_str(&self, s: &str) -> i64 {
        match self {
            PartitionFunction::Hash(buckets) => {
                if *buckets > 0 {
                    let hash = compute_string_hash(s);
                    hash % (*buckets as i64)
                } else {
                    0
                }
            }
            _ => 0,
        }
    }
}

fn compute_string_hash(s: &str) -> i64 {
    let mut hash: i64 = 0;
    for c in s.chars() {
        hash = hash.wrapping_mul(31).wrapping_add(c as i64);
    }
    hash.abs()
}

pub fn compute_partition_key_from_scalar(
    func: &PartitionFunction,
    val: &ScalarValue,
) -> String {
    match val {
        ScalarValue::Int64(Some(v)) => func.apply(*v).to_string(),
        ScalarValue::Int32(Some(v)) => func.apply(*v as i64).to_string(),
        ScalarValue::Timestamp(Some(v)) => func.apply(*v).to_string(),
        ScalarValue::Date(Some(v)) => func.apply(*v as i64).to_string(),
        ScalarValue::VarChar(Some(s)) => {
            match func {
                PartitionFunction::Hash(_) => func.apply_str(s).to_string(),
                _ => s.clone(),
            }
        }
        _ => String::new(),
    }
}

pub fn hash_partition_value(val: &ScalarValue, num_buckets: i64) -> i64 {
    let func = PartitionFunction::Hash(num_buckets as i32);
    match val {
        ScalarValue::Int64(Some(v)) => func.apply(*v),
        ScalarValue::Int32(Some(v)) => func.apply(*v as i64),
        ScalarValue::UInt64(Some(v)) => {
            if num_buckets > 0 {
                (*v % num_buckets as u64) as i64
            } else {
                0
            }
        }
        ScalarValue::UInt32(Some(v)) => {
            if num_buckets > 0 {
                (*v % num_buckets as u32) as i64
            } else {
                0
            }
        }
        ScalarValue::VarChar(Some(s)) => func.apply_str(s),
        _ => 0,
    }
}