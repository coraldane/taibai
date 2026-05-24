use crate::common::error::{MonaxError, Result};
use crate::protocol::types::DataType;
use crate::storage::bulk_payload::DirectBulkRequest;
use crate::storage::catalog::schema::TableMeta;
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

pub fn compute_partition_key_from_bulk_payload(
    req: &DirectBulkRequest<'_>,
    table_meta: &TableMeta,
) -> Result<String> {
    let col_idx = match table_meta.get_partition_column_index_cached() {
        Some(idx) => idx,
        None => return Ok(String::new()),
    };

    let bulk_col = req.columns.iter().find(|c| c.name.eq_ignore_ascii_case(&table_meta.partition_key));
    let bulk_col = match bulk_col {
        Some(c) => c,
        None => return Ok(String::new()),
    };

    if bulk_col.data.is_empty() || req.row_count == 0 {
        return Ok(String::new());
    }

    let first_val = match bulk_col.data_type {
        DataType::Int32 => {
            let v = i32::from_le_bytes(bulk_col.data[0..4].try_into().unwrap());
            ScalarValue::Int32(Some(v))
        }
        DataType::Int64 => {
            let v = i64::from_le_bytes(bulk_col.data[0..8].try_into().unwrap());
            ScalarValue::Int64(Some(v))
        }
        DataType::Timestamp => {
            let v = i64::from_le_bytes(bulk_col.data[0..8].try_into().unwrap());
            ScalarValue::Timestamp(Some(v))
        }
        DataType::Date => {
            let v = i32::from_le_bytes(bulk_col.data[0..4].try_into().unwrap());
            ScalarValue::Date(Some(v))
        }
        DataType::UInt32 => {
            let v = u32::from_le_bytes(bulk_col.data[0..4].try_into().unwrap());
            ScalarValue::UInt32(Some(v))
        }
        DataType::UInt64 => {
            let v = u64::from_le_bytes(bulk_col.data[0..8].try_into().unwrap());
            ScalarValue::UInt64(Some(v))
        }
        DataType::VarChar => {
            let len = u32::from_le_bytes(bulk_col.data[0..4].try_into().unwrap()) as usize;
            if len + 4 > bulk_col.data.len() {
                return Ok(String::new());
            }
            let s = std::str::from_utf8(&bulk_col.data[4..4 + len])
                .map_err(|_| MonaxError::Protocol("invalid utf8 in partition column".into()))?;
            ScalarValue::VarChar(Some(s.to_string()))
        }
        _ => return Ok(String::new()),
    };

    Ok(compute_partition_key_from_scalar(&table_meta.partition_strategy(), &first_val))
}