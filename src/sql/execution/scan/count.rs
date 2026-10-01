use crate::common::error::Result;
use crate::protocol::types::DataType;
use crate::storage::segment::{delete_bitmap, ScanStats};
use crate::storage::segment_manager::SegmentManager;
use crate::types::batch::{ColumnDef, RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

use super::partition;
use super::row_filter;
use super::row_group;
use super::spec::ScanSpec;

use std::collections::HashMap;
use std::path::Path;

pub fn count_star(
    spec: &ScanSpec,
    table_meta: &crate::storage::catalog::schema::TableMeta,
    table_dir: &Path,
    segment_mgr: &SegmentManager,
    memtable_mgr: &crate::storage::core::memtable::MemTableManager,
    db: &str,
    stats: Option<&mut HashMap<String, ScanStats>>,
) -> Result<u64> {
    let segment_files = partition::prune_segments_by_partition(
        SegmentManager::discover_segments(table_dir)?,
        spec,
        Some(table_meta),
    );
    let pred = spec.to_row_group_predicate();
    let bloom_checks = row_group::build_bloom_check_list(spec);
    let in_checks = row_group::build_in_check_list(spec);
    let set_checks = row_group::build_set_check_list(spec);
    let set_in_checks = row_group::build_set_in_check_list(spec);

    let mut total_rows: u64 = 0;

    if spec.eq_predicates.len() == 1
        && spec.range_predicates.is_empty()
        && spec.in_predicates.is_empty()
        && spec.remaining_predicate.is_none()
    {
        let (col_idx, value) = &spec.eq_predicates[0];
        count_with_single_eq(
            spec,
            &segment_files,
            segment_mgr,
            *col_idx,
            value,
            &mut total_rows,
            stats,
        )?;
    } else if spec.has_predicates() {
        count_with_predicates(
            spec,
            &segment_files,
            segment_mgr,
            &pred,
            &bloom_checks,
            &in_checks,
            &set_checks,
            &set_in_checks,
            &mut total_rows,
            stats,
        )?;
    } else {
        count_no_predicate(spec, &segment_files, segment_mgr, &mut total_rows, stats)?;
    }

    total_rows += super::memtable::count_memtable_rows(memtable_mgr, db, &spec.table_name, spec);

    Ok(total_rows)
}

fn count_no_predicate(
    spec: &ScanSpec,
    segment_files: &[std::path::PathBuf],
    segment_mgr: &SegmentManager,
    total_rows: &mut u64,
    mut stats: Option<&mut HashMap<String, ScanStats>>,
) -> Result<()> {
    for seg_path in segment_files {
        // Footer metadata cannot see this transaction's deferred tombstones;
        // fall through to the overlay-aware reader for such segments.
        let meta = if segment_mgr.txn_has_segment_overlay(seg_path, spec.reader_txn_id) {
            None
        } else {
            segment_mgr.segment_meta(seg_path)?
        };
        match meta {
            Some(meta) => {
                *total_rows += meta.row_count.saturating_sub(meta.deleted_row_count);
            }
            None => {
                let reader = segment_mgr.open_scan_reader(seg_path, spec.reader_txn_id)?;
                *total_rows += reader
                    .row_count()
                    .saturating_sub(reader.deleted_row_count());
                if let Some(ref mut s) = stats {
                    s.entry("count".to_string()).or_default().segments_opened += 1;
                }
            }
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
        let meta_snapshot = segment_mgr.segment_meta(seg_path)?;
        if let Some(meta) = meta_snapshot.as_ref() {
            // A segment whose footer predates the predicate column
            // logically carries the column's DEFAULT (or NULL) on every
            // row — the same default-fill the SELECT scan and value
            // aggregates already apply. The footer has no entry to align
            // or prune against, so `=` folds to one constant comparison
            // over the live rows.
            if row_filter::any_predicate_column_absent(&spec.table_schema, &meta.schema, &[col_idx])
            {
                let reader = segment_mgr.open_scan_reader(seg_path, spec.reader_txn_id)?;
                *total_rows += count_eq_on_default_filled_segment(
                    &spec.table_schema,
                    &reader,
                    col_idx,
                    value,
                );
                if let Some(ref mut s) = stats {
                    s.entry("count".to_string()).or_default().segments_opened += 1;
                }
                continue;
            }
            // Sidecar zone maps follow the segment's footer column order;
            // re-index the predicate onto it before comparing.
            let aligned_pred =
                row_filter::align_predicate_to_footer(&spec.table_schema, &meta.schema, &pred)?;
            if !row_group::prune_segment_by_zone_map(&aligned_pred, &meta.segment_zone_maps) {
                continue;
            }
        }
        let reader = segment_mgr.open_scan_reader(seg_path, spec.reader_txn_id)?;
        if row_filter::any_predicate_column_absent(&spec.table_schema, reader.schema(), &[col_idx])
        {
            // No sidecar meta to consult — the footer itself predates the
            // column.
            *total_rows +=
                count_eq_on_default_filled_segment(&spec.table_schema, &reader, col_idx, value);
            if let Some(ref mut s) = stats {
                s.entry("count".to_string()).or_default().segments_opened += 1;
            }
            continue;
        }

        // count_matching_rows_fast probes bloom filters / zone maps / raw
        // blocks by footer column number: col_idx is a catalog index and
        // must be translated per segment (DROP/RENAME COLUMN drift).
        let footer_idx =
            row_filter::align_indices_to_footer(&spec.table_schema, reader.schema(), &[col_idx])?
                [0];
        match reader.count_matching_rows_fast(footer_idx, value) {
            Ok(count) => *total_rows += count,
            Err(e) => return Err(e),
        }

        if let Some(ref mut s) = stats {
            s.entry("count".to_string()).or_default().segments_opened += 1;
        }
    }
    Ok(())
}

/// COUNT one segment that predates the predicate column: every row
/// logically carries the column's DEFAULT (`column_default_value`), so the
/// eq predicate folds to a single constant comparison over all live rows.
/// NULL never satisfies `=`, and the compared value is coerced to the
/// column type at spec build time, so plain scalar equality is exact.
fn count_eq_on_default_filled_segment(
    table_schema: &Schema,
    reader: &crate::storage::segment::SegmentReader,
    col_idx: usize,
    value: &ScalarValue,
) -> u64 {
    let default = crate::storage::segment::reader::column_default_value(
        &table_schema.columns[col_idx],
    );
    if default.is_null() || default != *value {
        return 0;
    }
    reader.row_count().saturating_sub(reader.deleted_row_count())
}

#[allow(clippy::too_many_arguments)]
fn count_with_predicates(
    spec: &ScanSpec,
    segment_files: &[std::path::PathBuf],
    segment_mgr: &SegmentManager,
    pred: &crate::storage::partition::pruning::RowGroupPredicate,
    bloom_checks: &[(usize, ScalarValue)],
    in_checks: &[&super::spec::InPredicate],
    set_checks: &[(usize, ScalarValue)],
    set_in_checks: &[&super::spec::InPredicate],
    total_rows: &mut u64,
    mut stats: Option<&mut HashMap<String, ScanStats>>,
) -> Result<()> {
    let filter_columns = spec.all_predicate_columns();

    for seg_path in segment_files {
        let meta_snapshot = segment_mgr.segment_meta(seg_path).ok().flatten();
        if let Some(meta) = meta_snapshot.as_ref() {
            // A segment whose footer predates a predicate column carries the
            // column's DEFAULT on every row (SELECT/aggregate default-fill
            // semantics): zone maps and bloom probes have no entry to prune
            // with and projection alignment would fail, so evaluate the
            // predicates over default-filled whole row groups instead.
            if row_filter::any_predicate_column_absent(
                &spec.table_schema,
                &meta.schema,
                &filter_columns,
            ) {
                let reader = segment_mgr.open_scan_reader(seg_path, spec.reader_txn_id)?;
                count_predicates_on_default_filled_segment(
                    spec,
                    &reader,
                    total_rows,
                    stats.as_deref_mut(),
                )?;
                continue;
            }
        }

        // Meta-based row-group pruning is authoritative when the sidecar meta
        // exists; re-running the same pruning through the reader footer (the
        // old behavior) doubled the metadata walk for every segment. Keep the
        // reader fallback only for segments without meta.
        //
        // Both the sidecar meta and the reader footer index their zone maps /
        // bloom filters / value sets by the segment's own footer column
        // order, while the predicate and check lists carry catalog indices:
        // translate them onto each segment before pruning.
        let had_meta_snapshot = meta_snapshot.is_some();
        let meta_candidates = match meta_snapshot {
            Some(meta) => {
                let aligned = row_filter::align_prune_checks_to_footer(
                    &spec.table_schema,
                    &meta.schema,
                    pred,
                    bloom_checks,
                    in_checks,
                    set_checks,
                    set_in_checks,
                )?;
                if !row_group::prune_segment_by_zone_map(
                    &aligned.predicate,
                    &meta.segment_zone_maps,
                ) {
                    Some(Vec::new())
                } else {
                    let in_refs = aligned.in_check_refs();
                    let set_in_refs = aligned.set_in_check_refs();
                    Some(
                        row_group::prune_row_group_meta(
                            &meta.row_groups,
                            &aligned.predicate,
                            &aligned.bloom_checks,
                            &in_refs,
                            &aligned.set_checks,
                            &set_in_refs,
                        )
                        .candidate_indices,
                    )
                }
            }
            None => None,
        };

        if let Some(candidates) = &meta_candidates {
            if candidates.is_empty() {
                continue;
            }
        }

        let reader = segment_mgr.open_scan_reader(seg_path, spec.reader_txn_id)?;

        // No sidecar meta to consult — decide default-fill applicability on
        // the footer itself before the alignment-based pruning below.
        if !had_meta_snapshot
            && row_filter::any_predicate_column_absent(
                &spec.table_schema,
                reader.schema(),
                &filter_columns,
            )
        {
            count_predicates_on_default_filled_segment(
                spec,
                &reader,
                total_rows,
                stats.as_deref_mut(),
            )?;
            continue;
        }

        let prune_result = match meta_candidates {
            Some(candidates) => super::row_group::RowGroupPruneResult {
                candidate_indices: candidates,
            },
            None => {
                let aligned = row_filter::align_prune_checks_to_footer(
                    &spec.table_schema,
                    reader.schema(),
                    pred,
                    bloom_checks,
                    in_checks,
                    set_checks,
                    set_in_checks,
                )?;
                let in_refs = aligned.in_check_refs();
                let set_in_refs = aligned.set_in_check_refs();
                row_group::prune_row_groups(
                    &reader,
                    &aligned.predicate,
                    &aligned.bloom_checks,
                    &in_refs,
                    &aligned.set_checks,
                    &set_in_refs,
                    reader.segment_zone_maps(),
                )
            }
        };

        for rg_idx in &prune_result.candidate_indices {
            let mut batch = if filter_columns.is_empty() {
                if super::strategy::reader_schema_differs(reader.schema(), &spec.table_schema)
                    && spec.table_schema.num_columns() > 0
                {
                    // Drifted footer: read every catalog column in catalog
                    // order (defaults for columns this segment predates) so
                    // the catalog-space predicates below evaluate the right
                    // columns.
                    reader.read_row_group_columns_aligned(*rg_idx, &spec.table_schema)?
                } else {
                    reader.read_row_group(*rg_idx)?
                }
            } else {
                let aligned_filter = row_filter::align_indices_to_footer(
                    &spec.table_schema,
                    reader.schema(),
                    &filter_columns,
                )?;
                reader.read_row_group_with_projection(*rg_idx, &aligned_filter)?
            };
            let bitmap_active = !reader.delete_bitmap().is_empty();
            if bitmap_active {
                // O(1) prefix sum; the old clone-the-whole-footer sum was
                // quadratic in the row-group count for bitmap segments.
                let rg_start_row: u64 = reader.row_group_start_row(*rg_idx);
                batch = delete_bitmap::filter_batch(batch, rg_start_row, reader.delete_bitmap())?;
            }

            let filtered = if filter_columns.is_empty() {
                row_filter::apply_predicates(
                    &batch,
                    &spec.eq_predicates,
                    &spec.range_predicates,
                    &spec.in_predicates,
                    spec.remaining_predicate.as_ref(),
                )?
            } else {
                row_filter::apply_predicates_with_column_map(
                    &batch,
                    &filter_columns,
                    &spec.eq_predicates,
                    &spec.range_predicates,
                    &spec.in_predicates,
                    spec.remaining_predicate.as_ref(),
                )?
            };
            *total_rows += filtered.num_rows() as u64;
        }

        if let Some(ref mut s) = stats {
            s.entry("count".to_string()).or_default().segments_opened += 1;
        }
    }
    Ok(())
}

/// COUNT row groups of a segment that predates a predicate column: read
/// whole row groups in catalog order (defaults filled) and evaluate the
/// predicates the way the SELECT scan applies them. No zone-map/bloom
/// pruning — the footer has no entries for the missing column, and the
/// defaults make the missing-column clauses decidable per row anyway.
fn count_predicates_on_default_filled_segment(
    spec: &ScanSpec,
    reader: &crate::storage::segment::SegmentReader,
    total_rows: &mut u64,
    mut stats: Option<&mut HashMap<String, ScanStats>>,
) -> Result<()> {
    for rg_idx in 0..reader.num_row_groups() {
        let mut batch = reader.read_row_group_columns_aligned(rg_idx, &spec.table_schema)?;
        if !reader.delete_bitmap().is_empty() {
            let rg_start_row = reader.row_group_start_row(rg_idx);
            batch = delete_bitmap::filter_batch(batch, rg_start_row, reader.delete_bitmap())?;
        }
        let filtered = row_filter::apply_predicates(
            &batch,
            &spec.eq_predicates,
            &spec.range_predicates,
            &spec.in_predicates,
            spec.remaining_predicate.as_ref(),
        )?;
        *total_rows += filtered.num_rows() as u64;
    }
    if let Some(ref mut s) = stats {
        s.entry("count".to_string()).or_default().segments_opened += 1;
    }
    Ok(())
}

pub fn build_count_star_result(count: u64) -> super::super::engine::ExecResult {
    let schema = Schema::new(vec![ColumnDef::new("count(*)", DataType::Int64)]);
    let mut batch = RecordBatch::new(schema.clone());
    batch
        .column_mut(0)
        .expect("count(*) schema has exactly one column")
        .push(&ScalarValue::Int64(Some(count as i64)))
        .expect("Int64 push into a fresh column cannot fail");
    super::super::engine::ExecResult::Query {
        schema,
        batches: vec![batch],
    }
}
