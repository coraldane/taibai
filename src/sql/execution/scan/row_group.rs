use crate::storage::index::bloomfilter::BloomFilter;
use crate::storage::partition::pruning::{RowGroupPredicate, ZoneMap};
use crate::storage::segment::SegmentReader;
use crate::types::scalar::ScalarValue;

use super::spec::InPredicate;

#[derive(Debug, Clone)]
pub struct RowGroupPruneResult {
    pub candidate_indices: Vec<usize>,
}

pub fn prune_row_groups(
    reader: &SegmentReader,
    predicate: &RowGroupPredicate,
    eq_checks: &[(usize, ScalarValue)],
    in_checks: &[&InPredicate],
    set_eq_checks: &[(usize, ScalarValue)],
    set_in_checks: &[&InPredicate],
    _segment_zone_maps: &[ZoneMap],
) -> RowGroupPruneResult {
    let num_rgs = reader.num_row_groups();
    let mut candidates = Vec::new();

    for rg_idx in 0..num_rgs {
        let rg_zone_maps = reader.zone_maps(rg_idx);
        let rg_bloom_filters = reader.bloom_filters(rg_idx);
        let rg_value_sets = reader.value_sets(rg_idx);

        if !predicate.matches_row_group(rg_zone_maps) {
            continue;
        }

        let mut set_pass = true;
        for (col_idx, val) in set_eq_checks {
            if let Some(Some(values)) = rg_value_sets.get(*col_idx) {
                if !values.contains(val) {
                    set_pass = false;
                    break;
                }
            }
        }
        if !set_pass {
            continue;
        }

        for pred in set_in_checks {
            if let Some(Some(values)) = rg_value_sets.get(pred.column_index) {
                if !pred.values.iter().any(|value| values.contains(value)) {
                    set_pass = false;
                    break;
                }
            }
        }
        if !set_pass {
            continue;
        }

        let mut bloom_pass = true;
        for (col_idx, val) in eq_checks {
            if let Some(bf) = rg_bloom_filters.get(*col_idx).and_then(|b| b.as_ref()) {
                if !bf.might_contain(val) {
                    bloom_pass = false;
                    break;
                }
            }
            if let Some(zm) = rg_zone_maps.get(*col_idx) {
                if !zm.contains(val) {
                    bloom_pass = false;
                    break;
                }
            }
        }
        if !bloom_pass {
            continue;
        }

        let mut in_pass = true;
        for pred in in_checks {
            if !check_in_predicate_row_group(pred, rg_zone_maps, rg_bloom_filters) {
                in_pass = false;
                break;
            }
        }
        if !in_pass {
            continue;
        }

        candidates.push(rg_idx);
    }

    RowGroupPruneResult {
        candidate_indices: candidates,
    }
}

pub fn prune_segment_by_zone_map(
    predicate: &RowGroupPredicate,
    segment_zone_maps: &[ZoneMap],
) -> bool {
    if segment_zone_maps.is_empty() {
        return true;
    }
    predicate.matches_row_group(segment_zone_maps)
}

fn check_in_predicate_row_group(
    pred: &InPredicate,
    zone_maps: &[ZoneMap],
    bloom_filters: &[Option<BloomFilter>],
) -> bool {
    let Some(zm) = zone_maps.get(pred.column_index) else {
        return true;
    };
    if zm.row_count > 0 && zm.null_count == zm.row_count {
        return false;
    }
    if zm.min_value.is_null() || zm.max_value.is_null() {
        return true;
    }

    if let Some(bf) = bloom_filters
        .get(pred.column_index)
        .and_then(|b| b.as_ref())
    {
        let any_might = pred.values.iter().any(|v| bf.might_contain(v));
        if !any_might {
            return false;
        }
    }

    for v in &pred.values {
        if zm.contains(v) {
            return true;
        }
    }
    false
}

pub fn build_bloom_check_list(spec: &super::spec::ScanSpec) -> Vec<(usize, ScalarValue)> {
    spec.eq_predicates
        .iter()
        .filter(|(idx, _)| spec.bloom_index_columns.contains(idx))
        .cloned()
        .collect()
}

pub fn build_in_check_list(spec: &super::spec::ScanSpec) -> Vec<&InPredicate> {
    spec.in_predicates
        .iter()
        .filter(|pred| spec.bloom_index_columns.contains(&pred.column_index))
        .collect()
}

pub fn build_set_check_list(spec: &super::spec::ScanSpec) -> Vec<(usize, ScalarValue)> {
    spec.eq_predicates
        .iter()
        .filter(|(idx, _)| spec.set_index_columns.contains(idx))
        .cloned()
        .collect()
}

pub fn build_set_in_check_list(spec: &super::spec::ScanSpec) -> Vec<&InPredicate> {
    spec.in_predicates
        .iter()
        .filter(|pred| spec.set_index_columns.contains(&pred.column_index))
        .collect()
}
