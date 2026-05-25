use std::path::PathBuf;

use crate::storage::catalog::schema::TableMeta;
use crate::storage::partition::{
    is_partition_segment, parse_partition_key, PartitionPruner, PartitionStrategy,
};
use crate::types::scalar::ScalarValue;

use super::spec::{PartitionScanInfo, ScanSpec};

pub fn prune_segments_by_partition(
    segment_files: Vec<PathBuf>,
    spec: &ScanSpec,
    table_meta: Option<&TableMeta>,
) -> Vec<PathBuf> {
    let Some(pi) = &spec.partition_info else {
        return segment_files;
    };
    if !pi.is_partitioned {
        return segment_files;
    }
    let Some(meta) = table_meta else {
        return segment_files;
    };
    if !meta.is_partitioned() {
        return segment_files;
    }

    let eq_partition_value = extract_partition_eq_value(spec, pi, meta);

    if let Some(target_key) = eq_partition_value {
        return filter_by_partition_key(segment_files, &target_key);
    }

    if let Some(pf) = &pi.partition_filter {
        if pf.column.eq_ignore_ascii_case(&meta.partition_key) {
            return filter_by_partition_values(segment_files, pf);
        }
    }

    let strategy = match meta.partition_type {
        crate::storage::catalog::schema::PartitionType::Day => PartitionStrategy::Day,
        crate::storage::catalog::schema::PartitionType::Year => PartitionStrategy::Year,
        crate::storage::catalog::schema::PartitionType::Month => PartitionStrategy::Month,
        crate::storage::catalog::schema::PartitionType::Value => PartitionStrategy::None,
        crate::storage::catalog::schema::PartitionType::Hash => return segment_files,
        crate::storage::catalog::schema::PartitionType::None => return segment_files,
    };

    if strategy == PartitionStrategy::None {
        return segment_files;
    }

    let (lower_bound, upper_bound) = if let Some(tr) = &pi.time_range {
        (tr.start.as_ref(), tr.end.as_ref())
    } else {
        (None, None)
    };

    if lower_bound.is_none() && upper_bound.is_none() {
        return segment_files;
    }

    let pruner = PartitionPruner::new(strategy);

    let available_partitions: Vec<String> = segment_files
        .iter()
        .filter_map(|path| {
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                parse_partition_key(name)
            } else {
                None
            }
        })
        .collect();

    let pruned_partitions = pruner.prune(&available_partitions, lower_bound, upper_bound);

    segment_files
        .into_iter()
        .filter(|path| {
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if is_partition_segment(name) {
                    if let Some(key) = parse_partition_key(name) {
                        return pruned_partitions.contains(&key);
                    }
                }
            }
            true
        })
        .collect()
}

fn extract_partition_eq_value(
    spec: &ScanSpec,
    pi: &PartitionScanInfo,
    meta: &TableMeta,
) -> Option<String> {
    let pk_idx = pi.partition_key_col_idx?;
    let val = spec.eq_value_for_column(pk_idx)?;
    Some(scalar_to_partition_key(val))
}

fn scalar_to_partition_key(value: &ScalarValue) -> String {
    match value {
        ScalarValue::VarChar(Some(s)) => s.clone(),
        ScalarValue::Int32(Some(v)) => v.to_string(),
        ScalarValue::Int64(Some(v)) => v.to_string(),
        ScalarValue::Date(Some(v)) => v.to_string(),
        _ => String::new(),
    }
}

fn filter_by_partition_key(segment_files: Vec<PathBuf>, target_key: &str) -> Vec<PathBuf> {
    if target_key.is_empty() {
        return segment_files;
    }
    segment_files
        .into_iter()
        .filter(|path| {
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if let Some(key) = parse_partition_key(name) {
                    return key == target_key;
                }
            }
            true
        })
        .collect()
}

fn filter_by_partition_values(
    segment_files: Vec<PathBuf>,
    pf: &crate::sql::optimizer::partition_pruning::PartitionFilter,
) -> Vec<PathBuf> {
    let target_keys: Vec<String> = pf
        .values
        .iter()
        .filter_map(|v| match v {
            ScalarValue::VarChar(Some(s)) => Some(s.clone()),
            _ => None,
        })
        .collect();

    if target_keys.is_empty() {
        return segment_files;
    }

    segment_files
        .into_iter()
        .filter(|path| {
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if let Some(key) = parse_partition_key(name) {
                    return target_keys.contains(&key);
                }
            }
            true
        })
        .collect()
}

pub fn extract_partition_key_from_in_predicate(
    spec: &ScanSpec,
    pi: &PartitionScanInfo,
) -> Vec<String> {
    let Some(pk_idx) = pi.partition_key_col_idx else {
        return Vec::new();
    };
    for pred in &spec.in_predicates {
        if pred.column_index == pk_idx {
            return pred
                .values
                .iter()
                .map(scalar_to_partition_key)
                .filter(|k| !k.is_empty())
                .collect();
        }
    }
    Vec::new()
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::protocol::types::DataType;
    use crate::sql::optimizer::partition_pruning::PartitionFilter;
    use crate::storage::catalog::schema::{PartitionType, TableMeta};
    use crate::types::batch::ColumnDef;
    use crate::types::scalar::ScalarValue;

    use super::*;

    #[test]
    fn ignores_partition_filter_for_non_partition_column() {
        let files = vec![
            PathBuf::from("p20251111_0001.dat"),
            PathBuf::from("p20251112_0001.dat"),
        ];
        let mut meta = TableMeta::new(
            "main",
            "test_table",
            vec![
                ColumnDef::new("name", DataType::VarChar),
                ColumnDef::new("region_id", DataType::Int32),
            ],
        );
        meta.partition_type = PartitionType::Value;
        meta.partition_key = "DEAL_DATE".to_string();

        let mut spec = ScanSpec::new("test_table");
        spec.partition_info = Some(PartitionScanInfo {
            time_range: None,
            partition_filter: Some(PartitionFilter {
                column: "name".to_string(),
                values: vec![ScalarValue::VarChar(Some("alice".to_string()))],
            }),
            partition_key_col_idx: meta.get_partition_column_index(),
            is_partitioned: true,
        });

        let pruned = prune_segments_by_partition(files.clone(), &spec, Some(&meta));

        assert_eq!(pruned, files);
    }
}
