use crate::common::error::Result;
use crate::storage::segment_manager::SegmentManager;
use crate::storage::segment::ScanStats;
use crate::types::batch::{ColumnDef, RecordBatch, Schema};
use crate::protocol::types::DataType;
use crate::types::scalar::ScalarValue;

use super::spec::ScanSpec;
use super::row_group;
use super::row_filter;

use std::collections::HashMap;
use std::path::Path;

pub fn count_star(
    spec: &ScanSpec,
    table_dir: &Path,
    segment_mgr: &SegmentManager,
    memtable_mgr: &crate::storage::memtable::MemTableManager,
    db: &str,
    stats: Option<&mut HashMap<String, ScanStats>>,
) -> Result<u64> {
    let segment_files = SegmentManager::discover_segments(table_dir)?;
    let pred = spec.to_row_group_predicate();
    let bloom_checks = row_group::build_bloom_check_list(spec);
    let in_checks = row_group::build_in_check_list(spec);

    let mut total_rows: u64 = 0;

    if spec.eq_predicates.len() == 1
        && spec.range_predicates.is_empty()
        && spec.in_predicates.is_empty()
        && spec.remaining_predicate.is_none()
    {
        let (col_idx, value) = &spec.eq_predicates[0];
        count_with_single_eq(spec, &segment_files, segment_mgr, *col_idx, value, &mut total_rows, stats)?;
    } else if spec.has_predicates() {
        count_with_predicates(spec, &segment_files, segment_mgr, &pred, &bloom_checks, &in_checks, &mut total_rows, stats)?;
    } else {
        count_no_predicate(&segment_files, segment_mgr, &mut total_rows, stats)?;
    }

    total_rows += super::memtable::count_memtable_rows(memtable_mgr, db, &spec.table_name, spec);

    Ok(total_rows)
}

fn count_no_predicate(
    segment_files: &[std::path::PathBuf],
    segment_mgr: &SegmentManager,
    total_rows: &mut u64,
    mut stats: Option<&mut HashMap<String, ScanStats>>,
) -> Result<()> {
    for seg_path in segment_files {
        match segment_mgr.open_reader(seg_path) {
            Ok(reader) => {
                let count = reader.row_count();
                let deleted = reader.deleted_row_count();
                *total_rows += count.saturating_sub(deleted);
                if let Some(ref mut s) = stats {
                    s.entry("count".to_string()).or_default().segments_opened += 1;
                }
            }
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

fn count_with_single_eq(
    spec: &ScanSpec,
    segment_files: &[std::path::PathBuf],
    segment_mgr: &SegmentManager,
    col_idx: usize,
    value: &ScalarValue,
    total_rows: &mut u64,
    mut stats: Option<&mut HashMap<String, ScanStats>>,
) -> Result<()> {
    let pred = spec.to_row_group_predicate();

    for seg_path in segment_files {
        let reader = match segment_mgr.open_reader(seg_path) {
            Ok(r) => r,
            Err(e) => return Err(e),
        };

        if !row_group::prune_segment_by_zone_map(&pred, reader.segment_zone_maps()) {
            continue;
        }

        match reader.count_matching_rows_fast(col_idx, value) {
            Ok(count) => *total_rows += count,
            Err(e) => return Err(e),
        }

        if let Some(ref mut s) = stats {
            s.entry("count".to_string()).or_default().segments_opened += 1;
        }
    }
    Ok(())
}

fn count_with_predicates(
    spec: &ScanSpec,
    segment_files: &[std::path::PathBuf],
    segment_mgr: &SegmentManager,
    pred: &crate::storage::pruning::RowGroupPredicate,
    bloom_checks: &[(usize, ScalarValue)],
    in_checks: &[&super::spec::InPredicate],
    total_rows: &mut u64,
    mut stats: Option<&mut HashMap<String, ScanStats>>,
) -> Result<()> {
    let filter_columns = spec.all_predicate_columns();
    let projection = if filter_columns.is_empty() {
        None
    } else {
        Some(filter_columns.as_slice())
    };

    for seg_path in segment_files {
        let reader = match segment_mgr.open_reader(seg_path) {
            Ok(r) => r,
            Err(e) => return Err(e),
        };

        if !row_group::prune_segment_by_zone_map(pred, reader.segment_zone_maps()) {
            continue;
        }

        let prune_result = row_group::prune_row_groups(&reader, pred, bloom_checks, in_checks, reader.segment_zone_maps());

        for rg_idx in &prune_result.candidate_indices {
            let mut batch = reader.read_row_group_with_projection(*rg_idx, &filter_columns)?;
            let bitmap_active = !reader.delete_bitmap().is_empty();
            if bitmap_active {
                let rg_start_row: u64 = reader.footer_row_groups_info()
                    .iter()
                    .take(*rg_idx)
                    .map(|r| r.row_count)
                    .sum();
                batch = filter_batch_by_delete_bitmap(&batch, rg_start_row, reader.delete_bitmap());
            }

            let filtered = row_filter::apply_predicates(
                &batch,
                &spec.eq_predicates,
                &spec.range_predicates,
                &spec.in_predicates,
                spec.remaining_predicate.as_ref(),
            );
            *total_rows += filtered.num_rows() as u64;
        }

        if let Some(ref mut s) = stats {
            s.entry("count".to_string()).or_default().segments_opened += 1;
        }
    }
    Ok(())
}

fn filter_batch_by_delete_bitmap(
    batch: &RecordBatch,
    first_row_id: u64,
    delete_bitmap: &[u8],
) -> RecordBatch {
    let num_rows = batch.num_rows();
    let mut keep = Vec::with_capacity(num_rows);
    for i in 0..num_rows {
        let row_id = first_row_id + i as u64;
        let byte = (row_id / 8) as usize;
        let bit = (row_id % 8) as u8;
        let deleted = delete_bitmap
            .get(byte)
            .map_or(false, |b| (b & (1u8 << bit)) != 0);
        if !deleted {
            keep.push(i);
        }
    }
    if keep.len() == num_rows {
        return batch.clone();
    }
    let mut columns = Vec::with_capacity(batch.num_columns());
    for c in 0..batch.num_columns() {
        let src = batch.column(c).unwrap();
        let mut new_col = crate::types::array::ColumnArray::empty(src.data_type());
        for &i in &keep {
            new_col.push(&src.scalar(i)).unwrap();
        }
        columns.push(new_col);
    }
    RecordBatch::try_new(batch.schema.clone(), columns).unwrap()
}

pub fn build_count_star_result(count: u64) -> super::super::engine::ExecResult {
    let schema = Schema::new(vec![ColumnDef::new("count(*)", DataType::Int64)]);
    let mut batch = RecordBatch::new(schema.clone());
    batch
        .column_mut(0)
        .unwrap()
        .push(&ScalarValue::Int64(Some(count as i64)))
        .unwrap();
    super::super::engine::ExecResult::Query {
        schema,
        batches: vec![batch],
    }
}
