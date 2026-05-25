use crate::common::error::Result;
use crate::storage::segment::ScanStats;
use crate::storage::segment_manager::SegmentManager;
use crate::types::batch::RecordBatch;
use std::collections::HashMap;

use super::count;
use super::memtable;
use super::partition;
use super::row_filter;
use super::row_group;
use super::spec::ScanSpec;

pub struct ScanExecutor<'a> {
    spec: &'a ScanSpec,
    engine: &'a crate::sql::execution::engine::Engine,
    table_schema: crate::types::batch::Schema,
    table_meta: crate::storage::catalog::schema::TableMeta,
    db: &'a str,
    stats: Option<&'a mut HashMap<String, ScanStats>>,
}

impl<'a> ScanExecutor<'a> {
    pub fn new(
        spec: &'a ScanSpec,
        engine: &'a crate::sql::execution::engine::Engine,
        table_schema: crate::types::batch::Schema,
        table_meta: crate::storage::catalog::schema::TableMeta,
        db: &'a str,
    ) -> Self {
        Self {
            spec,
            engine,
            table_schema,
            table_meta,
            db,
            stats: None,
        }
    }

    pub fn with_stats(mut self, stats: &'a mut HashMap<String, ScanStats>) -> Self {
        self.stats = Some(stats);
        self
    }

    pub fn execute(&mut self) -> Result<Vec<RecordBatch>> {
        if self.spec.is_count_star {
            return self.execute_count_star();
        }
        self.execute_scan()
    }

    fn execute_count_star(&mut self) -> Result<Vec<RecordBatch>> {
        let table_dir = self
            .engine
            .data_dir()
            .join(self.db)
            .join(&self.spec.table_name);
        if !table_dir.exists() {
            let result = count::build_count_star_result(0);
            return match result {
                crate::sql::execution::engine::ExecResult::Query { batches, .. } => Ok(batches),
                _ => Ok(Vec::new()),
            };
        }
        let count = count::count_star(
            self.spec,
            &table_dir,
            self.engine.segment_mgr(),
            self.engine.memtable_manager(),
            self.db,
            self.stats.as_deref_mut(),
        )?;
        let result = count::build_count_star_result(count);
        match result {
            crate::sql::execution::engine::ExecResult::Query { batches, .. } => Ok(batches),
            _ => Ok(Vec::new()),
        }
    }

    fn execute_scan(&mut self) -> Result<Vec<RecordBatch>> {
        let table_dir = self
            .engine
            .data_dir()
            .join(self.db)
            .join(&self.spec.table_name);
        let mut batches = Vec::new();

        if table_dir.exists() {
            let all_segments = SegmentManager::discover_segments(&table_dir)?;

            let filtered_segments = partition::prune_segments_by_partition(
                all_segments,
                self.spec,
                Some(&self.table_meta),
            );

            let pred = self.spec.to_row_group_predicate();
            let bloom_checks = row_group::build_bloom_check_list(self.spec);
            let in_checks = row_group::build_in_check_list(self.spec);

            let mgr = self.engine.segment_mgr();

            let use_predicate_first = self.should_use_predicate_first();
            let predicate_cols = self.compute_predicate_columns();
            let projection_cols = self.compute_projection_columns();

            for seg_path in &filtered_segments {
                let reader = match mgr.open_reader(seg_path) {
                    Ok(r) => r,
                    Err(_) => continue,
                };

                if let Some(s) = self.stats.as_deref_mut() {
                    s.entry(self.spec.table_name.clone())
                        .or_default()
                        .segments_opened += 1;
                }

                let segment_zone_maps = reader.segment_zone_maps();

                if !row_group::prune_segment_by_zone_map(&pred, &segment_zone_maps) {
                    continue;
                }

                let prune_result = row_group::prune_row_groups(
                    &reader,
                    &pred,
                    &bloom_checks,
                    &in_checks,
                    &segment_zone_maps,
                );

                if let Some(s) = self.stats.as_deref_mut() {
                    let stats_entry = s.entry(self.spec.table_name.clone()).or_default();
                    stats_entry.row_groups_total += reader.num_row_groups() as u64;
                    stats_entry.row_groups_pruned +=
                        (reader.num_row_groups() - prune_result.candidate_indices.len()) as u64;
                    stats_entry.row_groups_read += prune_result.candidate_indices.len() as u64;
                }

                if use_predicate_first && !predicate_cols.is_empty() && !projection_cols.is_empty()
                {
                    use rayon::prelude::*;

                    let mut rg_batches: Vec<(usize, RecordBatch)> = prune_result
                        .candidate_indices
                        .par_iter()
                        .enumerate()
                        .filter_map(|(pos, rg_idx)| {
                            row_filter::predicate_first_read(
                                &reader,
                                *rg_idx,
                                &predicate_cols,
                                &projection_cols,
                                &self.spec.eq_predicates,
                                &self.spec.range_predicates,
                                &self.spec.in_predicates,
                                self.spec.remaining_predicate.as_ref(),
                            )
                            .ok()
                            .filter(|batch| batch.num_rows() > 0)
                            .map(|batch| (pos, batch))
                        })
                        .collect();
                    rg_batches.sort_by_key(|(pos, _)| *pos);

                    let rows_scanned = rg_batches
                        .iter()
                        .map(|(_, batch)| batch.num_rows() as u64)
                        .sum::<u64>();
                    if let Some(s) = self.stats.as_deref_mut() {
                        s.entry(self.spec.table_name.clone())
                            .or_default()
                            .rows_scanned += rows_scanned;
                    }
                    batches.extend(rg_batches.into_iter().map(|(_, batch)| batch));
                } else {
                    for rg_idx in &prune_result.candidate_indices {
                        let batch = match self.read_and_filter_row_group(
                            &reader,
                            *rg_idx,
                            &projection_cols,
                        ) {
                            Ok(b) => b,
                            Err(_) => continue,
                        };
                        if batch.num_rows() > 0 {
                            batches.push(batch);
                        }
                    }
                }
            }
        }

        batches.extend(memtable::scan_memtables(
            self.engine.memtable_manager(),
            self.db,
            &self.spec.table_name,
            self.spec,
        ));

        Ok(batches)
    }

    fn read_and_filter_row_group(
        &mut self,
        reader: &crate::storage::segment::SegmentReader,
        rg_idx: usize,
        projection_cols: &[usize],
    ) -> Result<RecordBatch> {
        let mut batch = if projection_cols.is_empty() {
            reader.read_row_group(rg_idx)?
        } else {
            reader.read_row_group_with_projection(rg_idx, projection_cols)?
        };

        let bitmap_active = !reader.delete_bitmap().is_empty();
        if bitmap_active {
            let rg_start_row: u64 = reader
                .footer_row_groups_info()
                .iter()
                .take(rg_idx)
                .map(|r| r.row_count)
                .sum();
            let before = batch.num_rows();
            batch = filter_batch_by_delete_bitmap(&batch, rg_start_row, reader.delete_bitmap());
            if let Some(s) = self.stats.as_deref_mut() {
                s.entry(self.spec.table_name.clone())
                    .or_default()
                    .rows_filtered_by_delete_bitmap += (before - batch.num_rows()) as u64;
            }
        }

        if let Some(s) = self.stats.as_deref_mut() {
            s.entry(self.spec.table_name.clone())
                .or_default()
                .rows_scanned += batch.num_rows() as u64;
        }

        if batch.num_rows() == 0 {
            return Ok(batch);
        }

        if self.spec.has_predicates() {
            let filtered = row_filter::apply_predicates(
                &batch,
                &self.spec.eq_predicates,
                &self.spec.range_predicates,
                &self.spec.in_predicates,
                self.spec.remaining_predicate.as_ref(),
            );
            Ok(filtered)
        } else {
            Ok(batch)
        }
    }

    fn should_use_predicate_first(&self) -> bool {
        if !self.spec.has_predicates() {
            return false;
        }
        if self.spec.projection.is_none() {
            return false;
        }
        let pred_cols = self.spec.all_predicate_columns();
        if pred_cols.is_empty() {
            return false;
        }
        true
    }

    fn compute_predicate_columns(&self) -> Vec<usize> {
        self.spec.all_predicate_columns()
    }

    fn compute_projection_columns(&self) -> Vec<usize> {
        match &self.spec.projection {
            Some(p) => p.clone(),
            None => (0..self.table_schema.num_columns()).collect(),
        }
    }
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
