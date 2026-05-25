use std::collections::{BTreeMap, HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use rayon::prelude::*;

use crate::common::error::{MonaxError, Result};
use crate::storage::catalog::schema::TableMeta;
use crate::storage::index::sort_key::compare_scalars_for_sort;
use crate::storage::partition::partition::parse_partition_key;
use crate::storage::segment::SegmentWriter;
use crate::storage::segment_manager::SegmentManager;
use crate::types::batch::{RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

const COMPACT_ROW_GROUP_SIZE: usize = 262_144;
const ROW_GROUP_CACHE_CAPACITY: usize = 8;
const PARALLEL_SORT_THRESHOLD: usize = 1_000_000;

#[derive(Debug, Clone, Copy)]
struct RowRef {
    batch_idx: usize,
    row_idx: usize,
    ordinal: u64,
}

#[derive(Debug, Clone)]
struct SourceRowGroup {
    segment_path: PathBuf,
    row_group_idx: usize,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct CompactStats {
    pub segments_read: usize,
    pub segments_written: usize,
    pub rows_written: u64,
    pub rows_dropped: u64,
}

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

    pub fn optimize_table(
        &self,
        db: &str,
        table: &str,
        table_meta: &TableMeta,
    ) -> Result<CompactStats> {
        if table_meta.cluster_key.is_empty() {
            tracing::info!(
                db,
                table,
                "table has no CLUSTER BY key, skipping optimization"
            );
            return Ok(CompactStats::default());
        }

        let start_time = Instant::now();
        let table_dir = self.data_dir.join(db).join(table);

        if !table_dir.exists() {
            tracing::info!(db, table, "table directory does not exist, skipping");
            return Ok(CompactStats::default());
        }

        let schema = Schema::new(table_meta.columns.clone());
        let cluster_key_indices = self.get_cluster_key_indices(table_meta, &schema)?;
        let segment_files = SegmentManager::discover_segments(&table_dir)?;
        if segment_files.is_empty() {
            return Ok(CompactStats::default());
        }

        tracing::info!(
            db,
            table,
            cluster_key = ?table_meta.cluster_key,
            segments = segment_files.len(),
            "starting table optimization"
        );

        let mut stats = CompactStats {
            segments_read: segment_files.len(),
            ..CompactStats::default()
        };

        for (partition_key, files) in group_segments_by_partition(segment_files) {
            let part_stats = self.compact_segment_group(
                &table_dir,
                partition_key.as_deref(),
                &files,
                &schema,
                &cluster_key_indices,
            )?;
            stats.segments_written += part_stats.segments_written;
            stats.rows_written += part_stats.rows_written;
            stats.rows_dropped += part_stats.rows_dropped;
        }

        tracing::info!(
            db,
            table,
            rows_written = stats.rows_written,
            rows_dropped = stats.rows_dropped,
            segments_read = stats.segments_read,
            segments_written = stats.segments_written,
            elapsed_ms = start_time.elapsed().as_millis(),
            "optimization completed"
        );

        Ok(stats)
    }

    fn compact_segment_group(
        &self,
        table_dir: &Path,
        partition_key: Option<&str>,
        segment_files: &[PathBuf],
        schema: &Schema,
        cluster_key_indices: &[usize],
    ) -> Result<CompactStats> {
        let mut key_batches = Vec::new();
        let mut source_row_groups = Vec::new();
        let mut row_refs = Vec::new();
        let mut ordinal = 0u64;
        let mut deleted_rows = 0u64;

        for seg_path in segment_files {
            let reader = self.segment_mgr.open_reader(seg_path)?;
            let mut segment_row_base = 0u64;
            for rg_idx in 0..reader.num_row_groups() {
                let key_batch = reader.read_columns_fast(rg_idx, cluster_key_indices)?;
                let batch_idx = key_batches.len();
                for row_idx in 0..key_batch.num_rows() {
                    let row_id = segment_row_base + row_idx as u64;
                    if reader.is_row_deleted(row_id) {
                        deleted_rows += 1;
                        continue;
                    }
                    row_refs.push(RowRef {
                        batch_idx,
                        row_idx,
                        ordinal,
                    });
                    ordinal += 1;
                }
                segment_row_base += key_batch.num_rows() as u64;
                key_batches.push(key_batch);
                source_row_groups.push(SourceRowGroup {
                    segment_path: seg_path.clone(),
                    row_group_idx: rg_idx,
                });
            }
        }

        if row_refs.is_empty() {
            for old_seg in segment_files {
                std::fs::remove_file(old_seg)?;
            }
            return Ok(CompactStats {
                segments_read: segment_files.len(),
                rows_dropped: deleted_rows,
                ..CompactStats::default()
            });
        }

        sort_row_refs(&mut row_refs, &key_batches);

        let final_path = next_compact_segment_path(table_dir, partition_key)?;
        let tmp_path = compact_tmp_path(&final_path);

        let write_result =
            self.write_sorted_segment(&tmp_path, schema, &source_row_groups, &row_refs);
        if let Err(err) = write_result {
            let _ = std::fs::remove_file(&tmp_path);
            return Err(err);
        }

        if final_path.exists() {
            return Err(MonaxError::Storage(format!(
                "compact target segment already exists: {}",
                final_path.display()
            )));
        }
        std::fs::rename(&tmp_path, &final_path)?;

        for old_seg in segment_files {
            if old_seg != &final_path {
                std::fs::remove_file(old_seg)?;
            }
        }

        Ok(CompactStats {
            segments_read: segment_files.len(),
            segments_written: 1,
            rows_written: row_refs.len() as u64,
            rows_dropped: deleted_rows,
        })
    }

    fn write_sorted_segment(
        &self,
        path: &Path,
        schema: &Schema,
        source_row_groups: &[SourceRowGroup],
        row_refs: &[RowRef],
    ) -> Result<()> {
        let mut writer = SegmentWriter::create(path, schema.clone(), COMPACT_ROW_GROUP_SIZE)?;
        let mut row_group_cache = RowGroupCache::new(ROW_GROUP_CACHE_CAPACITY);
        let mut current_batch = RecordBatch::new(schema.clone());

        for row_ref in row_refs {
            let source = source_row_groups.get(row_ref.batch_idx).ok_or_else(|| {
                MonaxError::Storage(format!(
                    "missing source row group for batch {}",
                    row_ref.batch_idx
                ))
            })?;
            let src_batch = row_group_cache.get(source, &self.segment_mgr)?;
            for col_idx in 0..schema.columns.len() {
                let val = src_batch.scalar(row_ref.row_idx, col_idx).ok_or_else(|| {
                    MonaxError::Storage(format!(
                        "failed to get scalar at batch {} row {} col {}",
                        row_ref.batch_idx, row_ref.row_idx, col_idx
                    ))
                })?;
                if let Some(col) = current_batch.column_mut(col_idx) {
                    col.push(&val)?;
                }
            }

            if current_batch.num_rows() >= COMPACT_ROW_GROUP_SIZE {
                writer.write_batch(&current_batch)?;
                current_batch = RecordBatch::new(schema.clone());
            }
        }

        if current_batch.num_rows() > 0 {
            writer.write_batch(&current_batch)?;
        }

        writer.seal()
    }

    fn get_cluster_key_indices(
        &self,
        table_meta: &TableMeta,
        schema: &Schema,
    ) -> Result<Vec<usize>> {
        let mut indices = Vec::new();
        for key_col in &table_meta.cluster_key {
            let idx = schema.index_of(key_col).ok_or_else(|| {
                MonaxError::Sql(format!("cluster key column '{}' not found", key_col))
            })?;
            indices.push(idx);
        }
        Ok(indices)
    }
}

struct RowGroupCache {
    capacity: usize,
    batches: HashMap<(PathBuf, usize), RecordBatch>,
    order: VecDeque<(PathBuf, usize)>,
}

impl RowGroupCache {
    fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            batches: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    fn get(
        &mut self,
        source: &SourceRowGroup,
        segment_mgr: &SegmentManager,
    ) -> Result<&RecordBatch> {
        let key = (source.segment_path.clone(), source.row_group_idx);
        if self.batches.contains_key(&key) {
            self.touch(&key);
            return Ok(self.batches.get(&key).unwrap());
        }

        let reader = segment_mgr.open_reader(&source.segment_path)?;
        let batch = reader.read_row_group(source.row_group_idx)?;
        self.batches.insert(key.clone(), batch);
        self.order.push_back(key.clone());
        self.evict_if_needed();
        Ok(self.batches.get(&key).unwrap())
    }

    fn touch(&mut self, key: &(PathBuf, usize)) {
        if let Some(pos) = self.order.iter().position(|existing| existing == key) {
            self.order.remove(pos);
        }
        self.order.push_back(key.clone());
    }

    fn evict_if_needed(&mut self) {
        while self.batches.len() > self.capacity {
            if let Some(old_key) = self.order.pop_front() {
                self.batches.remove(&old_key);
            } else {
                break;
            }
        }
    }
}

fn sort_row_refs(row_refs: &mut [RowRef], key_batches: &[RecordBatch]) {
    if row_refs.len() >= PARALLEL_SORT_THRESHOLD {
        row_refs.par_sort_unstable_by(|left, right| compare_row_refs(left, right, key_batches));
    } else {
        row_refs.sort_unstable_by(|left, right| compare_row_refs(left, right, key_batches));
    }
}

fn compare_row_refs(
    left: &RowRef,
    right: &RowRef,
    key_batches: &[RecordBatch],
) -> std::cmp::Ordering {
    let key_count = key_batches
        .get(left.batch_idx)
        .map(|batch| batch.num_columns())
        .unwrap_or(0);
    for key_pos in 0..key_count {
        let left_val = scalar_at(key_batches, *left, key_pos);
        let right_val = scalar_at(key_batches, *right, key_pos);
        let cmp = compare_scalars_for_sort(&left_val, &right_val);
        if cmp != std::cmp::Ordering::Equal {
            return cmp;
        }
    }
    left.ordinal.cmp(&right.ordinal)
}

fn scalar_at(batches: &[RecordBatch], row_ref: RowRef, col_idx: usize) -> ScalarValue {
    batches[row_ref.batch_idx]
        .scalar(row_ref.row_idx, col_idx)
        .unwrap_or_else(|| {
            ScalarValue::null_for_type(batches[row_ref.batch_idx].schema.columns[col_idx].data_type)
        })
}

fn group_segments_by_partition(
    segment_files: Vec<PathBuf>,
) -> BTreeMap<Option<String>, Vec<PathBuf>> {
    let mut groups = BTreeMap::new();
    for path in segment_files {
        let partition_key = path
            .file_name()
            .and_then(|name| parse_partition_key(&name.to_string_lossy()));
        groups
            .entry(partition_key)
            .or_insert_with(Vec::new)
            .push(path);
    }
    groups
}

fn compact_tmp_path(final_path: &Path) -> PathBuf {
    let file_name = final_path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| "segment.dat".to_string());
    final_path.with_file_name(format!("{}.compact_tmp", file_name))
}

fn next_compact_segment_path(table_dir: &Path, partition_key: Option<&str>) -> Result<PathBuf> {
    let mut max_seq = 0u32;
    if table_dir.exists() {
        for entry in std::fs::read_dir(table_dir)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            let seq = match partition_key {
                Some(pk) => parse_partition_seq(&name, pk),
                None => parse_regular_seq(&name),
            };
            if let Some(seq) = seq {
                max_seq = max_seq.max(seq);
            }
        }
    }

    let next_seq = max_seq + 1;
    Ok(match partition_key {
        Some(pk) => table_dir.join(format!("p{}_{:04}.dat", pk, next_seq)),
        None => table_dir.join(format!("seg_{:05}.dat", next_seq)),
    })
}

fn parse_regular_seq(name: &str) -> Option<u32> {
    let seq = name.strip_prefix("seg_")?.strip_suffix(".dat")?;
    seq.parse().ok()
}

fn parse_partition_seq(name: &str, partition_key: &str) -> Option<u32> {
    let prefix = format!("p{}_", partition_key);
    let seq = name.strip_prefix(&prefix)?.strip_suffix(".dat")?;
    seq.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::types::DataType;
    use crate::storage::segment::SegmentReader;
    use crate::types::batch::ColumnDef;

    const TEST_ROW_GROUP_SIZE: usize = 8192;

    fn test_schema() -> Schema {
        Schema::new(vec![
            ColumnDef::new("id", DataType::Int64),
            ColumnDef::new("name", DataType::VarChar),
        ])
    }

    fn test_meta(schema: &Schema) -> TableMeta {
        let mut meta = TableMeta::new("default", "t", schema.columns.clone());
        meta.cluster_key = vec!["id".to_string()];
        meta
    }

    fn make_batch(schema: &Schema, rows: &[(i64, &str)]) -> Result<RecordBatch> {
        let mut batch = RecordBatch::new(schema.clone());
        for (id, name) in rows {
            batch
                .column_mut(0)
                .unwrap()
                .push(&ScalarValue::Int64(Some(*id)))?;
            batch
                .column_mut(1)
                .unwrap()
                .push(&ScalarValue::VarChar(Some((*name).to_string())))?;
        }
        Ok(batch)
    }

    fn write_segment(path: &Path, schema: &Schema, rows: &[(i64, &str)]) -> Result<()> {
        let batch = make_batch(schema, rows)?;
        let mut writer = SegmentWriter::create(path, schema.clone(), TEST_ROW_GROUP_SIZE)?;
        writer.write_batch(&batch)?;
        writer.seal()
    }

    fn read_ids(path: &Path) -> Result<Vec<i64>> {
        let reader = SegmentReader::open(path)?;
        let mut ids = Vec::new();
        for rg_idx in 0..reader.num_row_groups() {
            let batch = reader.read_row_group(rg_idx)?;
            for row_idx in 0..batch.num_rows() {
                if let ScalarValue::Int64(Some(id)) = batch.scalar(row_idx, 0).unwrap() {
                    ids.push(id);
                }
            }
        }
        Ok(ids)
    }

    #[test]
    fn optimize_table_merges_and_sorts_segments() -> Result<()> {
        let tmp = tempfile::tempdir().unwrap();
        let table_dir = tmp.path().join("default").join("t");
        std::fs::create_dir_all(&table_dir)?;

        let schema = test_schema();
        write_segment(
            &table_dir.join("seg_00001.dat"),
            &schema,
            &[(3, "c"), (1, "a")],
        )?;
        write_segment(
            &table_dir.join("seg_00002.dat"),
            &schema,
            &[(4, "d"), (2, "b")],
        )?;

        let segment_mgr = Arc::new(SegmentManager::new(tmp.path())?);
        let compactor = Compactor::new(segment_mgr, tmp.path().to_path_buf());
        let stats = compactor.optimize_table("default", "t", &test_meta(&schema))?;

        assert_eq!(stats.segments_read, 2);
        assert_eq!(stats.segments_written, 1);
        assert_eq!(stats.rows_written, 4);

        let segments = SegmentManager::discover_segments(&table_dir)?;
        assert_eq!(segments.len(), 1);
        assert_eq!(read_ids(&segments[0])?, vec![1, 2, 3, 4]);
        Ok(())
    }

    #[test]
    fn optimize_table_drops_deleted_rows() -> Result<()> {
        let tmp = tempfile::tempdir().unwrap();
        let table_dir = tmp.path().join("default").join("t");
        std::fs::create_dir_all(&table_dir)?;

        let schema = test_schema();
        let seg_path = table_dir.join("seg_00001.dat");
        write_segment(&seg_path, &schema, &[(2, "b"), (1, "a"), (3, "c")])?;

        let segment_mgr = Arc::new(SegmentManager::new(tmp.path())?);
        segment_mgr.rewrite_delete_footer(&seg_path, &[0b0000_0010], 1)?;

        let compactor = Compactor::new(segment_mgr, tmp.path().to_path_buf());
        let stats = compactor.optimize_table("default", "t", &test_meta(&schema))?;

        assert_eq!(stats.rows_written, 2);
        assert_eq!(stats.rows_dropped, 1);

        let segments = SegmentManager::discover_segments(&table_dir)?;
        assert_eq!(segments.len(), 1);
        assert_eq!(read_ids(&segments[0])?, vec![2, 3]);
        let reader = SegmentReader::open(&segments[0])?;
        assert_eq!(reader.deleted_row_count(), 0);
        Ok(())
    }
}
