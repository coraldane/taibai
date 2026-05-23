use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use crate::common::error::{MonaxError, Result};
use crate::storage::schema::TableMeta;
use crate::storage::segment_manager::SegmentManager;
use crate::types::batch::{RecordBatch, Schema};
use crate::types::scalar::ScalarValue;
use crate::protocol::types::DataType;

const ROW_GROUP_SIZE: usize = 8192;

pub struct Compactor {
    segment_mgr: Arc<SegmentManager>,
    data_dir: PathBuf,
}

impl Compactor {
    pub fn new(segment_mgr: Arc<SegmentManager>, data_dir: PathBuf) -> Self {
        Self {
            segment_mgr,
            data_dir,
        }
    }

    pub fn optimize_table(&self, db: &str, table: &str, table_meta: &TableMeta) -> Result<()> {
        if table_meta.cluster_key.is_empty() {
            tracing::info!(db, table, "table has no CLUSTER BY key, skipping optimization");
            return Ok(());
        }

        let start_time = Instant::now();
        let table_dir = self.data_dir.join(db).join(table);
        
        if !table_dir.exists() {
            tracing::info!(db, table, "table directory does not exist, skipping");
            return Ok(());
        }

        let schema = Schema::new(table_meta.columns.clone());
        let cluster_key_indices = self.get_cluster_key_indices(table_meta, &schema)?;

        tracing::info!(db, table, cluster_key = ?table_meta.cluster_key, "starting table optimization");

        let segment_files = SegmentManager::discover_segments(&table_dir)?;
        if segment_files.is_empty() {
            return Ok(());
        }

        let mut all_batches: Vec<RecordBatch> = Vec::new();
        let mut total_rows: usize = 0;

        for seg_path in &segment_files {
            match self.segment_mgr.open_reader(seg_path) {
                Ok(reader) => {
                    let num_row_groups = reader.num_row_groups();
                    for rg_idx in 0..num_row_groups {
                        let column_indices: Vec<usize> = (0..schema.columns.len()).collect();
                        match reader.read_columns_fast(rg_idx, &column_indices) {
                            Ok(batch) => {
                                total_rows += batch.num_rows();
                                all_batches.push(batch);
                            }
                            Err(e) => {
                                tracing::warn!(db, table, rg_idx, error = ?e, "failed to read row group");
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(db, table, path = ?seg_path, error = ?e, "failed to open segment");
                }
            }
        }

        if total_rows == 0 {
            return Ok(());
        }

        tracing::info!(db, table, total_rows, segments = segment_files.len(), "read all data");

        let sorted_indices = self.sort_indices(&all_batches, &cluster_key_indices, &schema);

        tracing::info!(db, table, "data sorted by cluster key");

        let new_segment_path = self.generate_new_segment_path(&table_dir, None, &segment_files);
        
        let mut writer = crate::storage::segment::SegmentWriter::create(&new_segment_path, schema.clone(), ROW_GROUP_SIZE)?;
        
        let mut current_batch = RecordBatch::new(schema.clone());

        for (batch_idx, batch) in all_batches.iter().enumerate() {
            for row_idx in 0..batch.num_rows() {
                let sorted_idx = sorted_indices[batch_idx * batch.num_rows() + row_idx];
                let (src_batch_idx, src_row_idx) = self.decode_sorted_index(sorted_idx, &all_batches);
                
                let src_batch = &all_batches[src_batch_idx];
                
                for col_idx in 0..schema.columns.len() {
                    let val = src_batch.scalar(src_row_idx, col_idx)
                        .ok_or_else(|| MonaxError::Storage(format!("failed to get scalar at row {} col {}", src_row_idx, col_idx)))?;
                    if let Some(col) = current_batch.column_mut(col_idx) {
                        col.push(&val)?;
                    }
                }

                if current_batch.num_rows() >= ROW_GROUP_SIZE {
                    writer.write_batch(&current_batch)?;
                    current_batch = RecordBatch::new(schema.clone());
                }
            }
        }

        if current_batch.num_rows() > 0 {
            writer.write_batch(&current_batch)?;
        }

        writer.seal()?;

        for old_seg in &segment_files {
            if old_seg != &new_segment_path {
                let _ = std::fs::remove_file(old_seg);
            }
        }

        tracing::info!(db, table, elapsed_ms = start_time.elapsed().as_millis(), "optimization completed");

        Ok(())
    }

    fn get_cluster_key_indices(&self, table_meta: &TableMeta, schema: &Schema) -> Result<Vec<usize>> {
        let mut indices = Vec::new();
        for key_col in &table_meta.cluster_key {
            let idx = schema.index_of(key_col)
                .ok_or_else(|| MonaxError::Sql(format!("cluster key column '{}' not found", key_col)))?;
            indices.push(idx);
        }
        Ok(indices)
    }

    fn sort_indices(&self, batches: &[RecordBatch], key_indices: &[usize], schema: &Schema) -> Vec<u32> {
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        let mut indices: Vec<u32> = Vec::with_capacity(total_rows);
        
        let mut offset = 0u32;
        for batch in batches {
            for i in 0..batch.num_rows() {
                indices.push(offset + i as u32);
            }
            offset += batch.num_rows() as u32;
        }

        indices.sort_by(|&a, &b| {
            let (batch_a, row_a) = self.decode_sorted_index(a, batches);
            let (batch_b, row_b) = self.decode_sorted_index(b, batches);
            
            for &key_idx in key_indices {
                let val_a = batches[batch_a].scalar(row_a, key_idx).unwrap_or(ScalarValue::Int64(None));
                let val_b = batches[batch_b].scalar(row_b, key_idx).unwrap_or(ScalarValue::Int64(None));
                
                let cmp = self.compare_scalars(&val_a, &val_b, &schema.columns[key_idx].data_type);
                if cmp != 0 {
                    return cmp.cmp(&0);
                }
            }
            std::cmp::Ordering::Equal
        });

        indices
    }

    fn decode_sorted_index(&self, idx: u32, batches: &[RecordBatch]) -> (usize, usize) {
        let mut offset = 0u32;
        for (batch_idx, batch) in batches.iter().enumerate() {
            let batch_rows = batch.num_rows() as u32;
            if idx < offset + batch_rows {
                return (batch_idx, (idx - offset) as usize);
            }
            offset += batch_rows;
        }
        (0, 0)
    }

    fn compare_scalars(&self, a: &ScalarValue, b: &ScalarValue, dtype: &DataType) -> i32 {
        match dtype {
            DataType::Int32 | DataType::Int64 | DataType::Date | DataType::Timestamp => {
                match (a, b) {
                    (ScalarValue::Int64(Some(va)), ScalarValue::Int64(Some(vb))) => (*va).cmp(&(*vb)) as i32 - 2,
                    (ScalarValue::Int64(Some(_)), ScalarValue::Int64(None)) => 1,
                    (ScalarValue::Int64(None), ScalarValue::Int64(Some(_))) => -1,
                    _ => 0,
                }
            }
            DataType::UInt32 | DataType::UInt64 => {
                match (a, b) {
                    (ScalarValue::UInt64(Some(va)), ScalarValue::UInt64(Some(vb))) => (*va).cmp(&(*vb)) as i32 - 2,
                    (ScalarValue::UInt64(Some(_)), ScalarValue::UInt64(None)) => 1,
                    (ScalarValue::UInt64(None), ScalarValue::UInt64(Some(_))) => -1,
                    _ => 0,
                }
            }
            DataType::Float32 | DataType::Float64 => {
                match (a, b) {
                    (ScalarValue::Float64(Some(va)), ScalarValue::Float64(Some(vb))) => {
                        if *va < *vb { -1 } else if *va > *vb { 1 } else { 0 }
                    }
                    (ScalarValue::Float64(Some(_)), ScalarValue::Float64(None)) => 1,
                    (ScalarValue::Float64(None), ScalarValue::Float64(Some(_))) => -1,
                    _ => 0,
                }
            }
            DataType::VarChar => {
                match (a, b) {
                    (ScalarValue::VarChar(Some(va)), ScalarValue::VarChar(Some(vb))) => (*va).cmp(&(*vb)) as i32 - 2,
                    (ScalarValue::VarChar(Some(_)), ScalarValue::VarChar(None)) => 1,
                    (ScalarValue::VarChar(None), ScalarValue::VarChar(Some(_))) => -1,
                    _ => 0,
                }
            }
            _ => 0,
        }
    }

    fn generate_new_segment_path(&self, table_dir: &Path, partition_key: Option<&str>, existing_segments: &[PathBuf]) -> PathBuf {
        let max_seq = existing_segments.iter()
            .filter_map(|p| {
                let filename = p.file_name()?.to_string_lossy();
                if partition_key.is_some() {
                    if filename.starts_with("p") && filename.contains("_") {
                        let parts: Vec<&str> = filename.split("_").collect();
                        if parts.len() >= 2 {
                            parts[1].replace(".seg", "").parse::<u32>().ok()
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    if filename.starts_with("seg_") {
                        filename.replace("seg_", "").replace(".seg", "").parse::<u32>().ok()
                    } else {
                        None
                    }
                }
            })
            .max()
            .unwrap_or(0);

        let new_seq = max_seq + 1;

        if let Some(pk) = partition_key {
            table_dir.join(format!("p{}_{}.seg", pk, new_seq))
        } else {
            table_dir.join(format!("seg_{}.seg", new_seq))
        }
    }
}