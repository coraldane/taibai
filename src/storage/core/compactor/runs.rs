use super::*;

#[derive(Default)]
pub(super) struct TempRuns(pub Vec<PathBuf>);
impl Drop for TempRuns {
    fn drop(&mut self) {
        for path in &self.0 {
            let _ = std::fs::remove_file(path);
            let _ = crate::storage::segment::meta::remove_sidecar_meta(path);
        }
    }
}

impl Compactor {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn build_clustered_runs(
        &self,
        db: &str,
        table: &str,
        label: &str,
        tmp: &Path,
        schema: &Schema,
        keys: &[usize],
        files: &[PathBuf],
        ttl: Option<(usize, i64)>,
        output_rows: usize,
        scratch: &mut TempRuns,
    ) -> Result<(Vec<PathBuf>, u64, u64)> {
        let budget = clustered_merge_budget_bytes();
        let run_budget = (budget / 8).max(1);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as i64;
        let mut pending = RecordBatch::new(schema.clone());
        let mut pending_bytes = 0usize;
        let mut paths = Vec::new();
        let mut dropped = 0;
        let mut expired = 0;
        let flush = |batch: &mut RecordBatch,
                     paths: &mut Vec<PathBuf>,
                     scratch: &mut TempRuns|
         -> Result<()> {
            if batch.is_empty() {
                return Ok(());
            }
            let mut indices: Vec<u32> = (0..batch.num_rows() as u32).collect();
            sort_clustered_row_indices(&mut indices, batch, keys);
            let path = merge_run_path(tmp, paths.len());
            scratch.0.push(path.clone());
            // No second run-sized sorted copy: gather only the output block.
            self.write_clustered_segment(
                db,
                table,
                label,
                &path,
                schema,
                &[],
                batch,
                &indices,
                output_rows,
                0,
            )?;
            paths.push(path);
            *batch = RecordBatch::new(schema.clone());
            Ok(())
        };
        for path in files {
            let reader = self.segment_mgr.open_reader(path)?;
            for rg in 0..reader.num_row_groups() {
                let batch = reader.read_row_group_columns_aligned(rg, schema)?;
                ensure_compaction_memory(
                    batch.estimated_size_bytes(),
                    budget / 4,
                    "source row group",
                )?;
                let base = reader.row_group_start_row(rg);
                for row in 0..batch.num_rows() {
                    if reader.is_row_deleted(base + row as u64) {
                        dropped += 1;
                        continue;
                    }
                    if let Some((col, secs)) = ttl {
                        if !ttl_row_visible(
                            &batch.column(col).unwrap().scalar(row),
                            secs.saturating_mul(1_000_000),
                            now,
                        ) {
                            dropped += 1;
                            expired += 1;
                            continue;
                        }
                    }
                    for col in 0..schema.num_columns() {
                        pending.column_mut(col).unwrap().extend_range(
                            batch.column(col).unwrap(),
                            row,
                            row + 1,
                        )?;
                    }
                    pending_bytes = pending_bytes.saturating_add(row_allocation_bytes(&batch, row));
                    if pending_bytes >= run_budget || pending.num_rows() >= u32::MAX as usize {
                        flush(&mut pending, &mut paths, scratch)?;
                        pending_bytes = 0;
                    }
                }
            }
        }
        flush(&mut pending, &mut paths, scratch)?;
        Ok((paths, dropped, expired))
    }

    /// Select fan-in from the actual largest decoded input block, including
    /// variable-width payloads. Only one block is decoded at a time here.
    pub(super) fn merge_fan_in(&self, paths: &[PathBuf], schema: &Schema) -> Result<usize> {
        let mut largest = 1usize;
        for path in paths {
            let reader = self.segment_mgr.open_reader(path)?;
            for rg in 0..reader.num_row_groups() {
                let batch = reader.read_row_group_columns_aligned(rg, schema)?;
                largest = largest.max(batch.estimated_size_bytes());
            }
        }
        ensure_compaction_memory(
            largest,
            clustered_merge_budget_bytes() / 4,
            "merge row group",
        )?;
        Ok((clustered_merge_budget_bytes() / 2 / largest).clamp(2, CLUSTERED_MERGE_MAX_RUNS))
    }
}

/// Account for copied payload and geometric buffer growth in O(columns),
/// without rescanning an ever-growing VECTOR column for every appended row.
pub(super) fn row_allocation_bytes(batch: &RecordBatch, row: usize) -> usize {
    batch
        .columns
        .iter()
        .map(|col| {
            let bytes = match col {
                ColumnArray::VarChar { data, .. } => data[row].len() + 8,
                ColumnArray::Blob { data, .. } => data[row].len() + 8,
                ColumnArray::Dictionary { ids, dict, .. } => {
                    dict.get(ids[row] as usize).map_or(0, str::len) + 8
                }
                ColumnArray::Vector { data, .. } => data[row].len().saturating_mul(4) + 24,
                _ => col.data_type().fixed_size().unwrap_or(96) + 1,
            };
            bytes.saturating_mul(2)
        })
        .sum()
}
