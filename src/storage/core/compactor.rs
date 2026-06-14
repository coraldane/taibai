use std::collections::{BTreeMap, HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use rayon::prelude::*;

use crate::common::error::{Result, TaibaiError};
use crate::protocol::types::DataType;
use crate::storage::catalog::schema::TableMeta;
use crate::storage::index::sort_key::compare_scalars_for_sort;
use crate::storage::partition::partition::parse_partition_key;
use crate::storage::segment::{store, SegmentWriter};
use crate::storage::segment_manager::SegmentManager;
use crate::types::array::ColumnArray;
use crate::types::batch::{RecordBatch, Schema};
use crate::types::bitmap::NullBitmap;
use crate::types::scalar::ScalarValue;

const COMPACT_ROW_GROUP_SIZE: usize = 8192;
// Gather more rows than one output row group at a time. SegmentWriter still
// splits the batch into COMPACT_ROW_GROUP_SIZE row groups, but this reduces
// per-chunk source grouping and per-column dispatch overhead during optimize.
const WRITE_GATHER_CHUNK_ROWS: usize = COMPACT_ROW_GROUP_SIZE * 8;
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
        let start_time = Instant::now();
        let table_dir = self.data_dir.join(db).join(table);

        if !table_dir.exists() {
            tracing::info!(db, table, "table directory does not exist, skipping");
            return Ok(CompactStats::default());
        }

        let schema = Schema::new(table_meta.columns.clone());
        let cluster_key_indices = self.get_cluster_key_indices(table_meta, &schema)?;
        let stats = self.segment_mgr.with_table_mutation_lock(db, table, || {
            let segment_files = self
                .segment_mgr
                .discover_inactive_segments(db, table, &table_dir)?;
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
                    db,
                    table,
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

            Ok(stats)
        })?;

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

    pub fn optimize_one_partition(
        &self,
        db: &str,
        table: &str,
        table_meta: &TableMeta,
    ) -> Result<CompactStats> {
        let start_time = Instant::now();
        let table_dir = self.data_dir.join(db).join(table);

        if !table_dir.exists() {
            tracing::info!(db, table, table_full_name = %format!("{}.{}", db, table), "table directory does not exist, skipping");
            return Ok(CompactStats::default());
        }

        let schema = Schema::new(table_meta.columns.clone());
        let cluster_key_indices = self.get_cluster_key_indices(table_meta, &schema)?;
        let stats = self.segment_mgr.with_table_mutation_lock(db, table, || {
            let segment_files = self
                .segment_mgr
                .discover_inactive_segments(db, table, &table_dir)?;
            if segment_files.is_empty() {
                return Ok(CompactStats::default());
            }

            let mut groups = group_segments_by_partition(segment_files)
                .into_iter()
                .filter(|(_, files)| files.len() >= 2)
                .collect::<Vec<_>>();
            groups.sort_by(|left, right| right.1.len().cmp(&left.1.len()));

            let Some((partition_key, files)) = groups.into_iter().next() else {
                return Ok(CompactStats::default());
            };

            tracing::info!(
                db,
                table,
                table_full_name = %format!("{}.{}", db, table),
                partition_key = partition_key.as_deref().unwrap_or("<unpartitioned>"),
                cluster_key = ?table_meta.cluster_key,
                segments = files.len(),
                "starting one-partition table optimization"
            );

            self.compact_segment_group(
                db,
                table,
                &table_dir,
                partition_key.as_deref(),
                &files,
                &schema,
                &cluster_key_indices,
            )
        })?;

        tracing::info!(
            db,
            table,
            table_full_name = %format!("{}.{}", db, table),
            rows_written = stats.rows_written,
            rows_dropped = stats.rows_dropped,
            segments_read = stats.segments_read,
            segments_written = stats.segments_written,
            elapsed_ms = start_time.elapsed().as_millis(),
            "one-partition optimization completed"
        );

        Ok(stats)
    }

    pub fn optimize_partition(
        &self,
        db: &str,
        table: &str,
        table_meta: &TableMeta,
        partition_key: &str,
    ) -> Result<CompactStats> {
        if !table_meta.is_partitioned() {
            return Err(TaibaiError::Sql(format!(
                "table '{}.{}' is not partitioned",
                db, table
            )));
        }

        let start_time = Instant::now();
        let table_dir = self.data_dir.join(db).join(table);
        let candidates = partition_key_candidates(partition_key);

        if !table_dir.exists() {
            tracing::info!(
                db,
                table,
                table_full_name = %format!("{}.{}", db, table),
                partition_key,
                "table directory does not exist, skipping partition optimization"
            );
            return Ok(CompactStats::default());
        }

        let schema = Schema::new(table_meta.columns.clone());
        let cluster_key_indices = self.get_cluster_key_indices(table_meta, &schema)?;
        let stats = self.segment_mgr.with_table_mutation_lock(db, table, || {
            let segment_files = self
                .segment_mgr
                .discover_inactive_segments(db, table, &table_dir)?;
            let mut groups = group_segments_by_partition(segment_files);
            let matched_key = candidates
                .iter()
                .find(|candidate| groups.contains_key(&Some((*candidate).clone())))
                .cloned();

            let Some(matched_key) = matched_key else {
                let available = groups
                    .keys()
                    .filter_map(|key| key.as_deref())
                    .collect::<Vec<_>>()
                    .join(", ");
                return Err(TaibaiError::Sql(format!(
                    "partition '{}' not found for table '{}.{}'{}",
                    partition_key,
                    db,
                    table,
                    if available.is_empty() {
                        String::new()
                    } else {
                        format!("; available partitions: {}", available)
                    }
                )));
            };

            let files = groups
                .remove(&Some(matched_key.clone()))
                .unwrap_or_default();
            if files.is_empty() {
                return Ok(CompactStats::default());
            }

            tracing::info!(
                db,
                table,
                table_full_name = %format!("{}.{}", db, table),
                partition_key = %matched_key,
                requested_partition = partition_key,
                cluster_key = ?table_meta.cluster_key,
                segments = files.len(),
                "starting requested partition optimization"
            );

            self.compact_segment_group(
                db,
                table,
                &table_dir,
                Some(&matched_key),
                &files,
                &schema,
                &cluster_key_indices,
            )
        })?;

        tracing::info!(
            db,
            table,
            table_full_name = %format!("{}.{}", db, table),
            requested_partition = partition_key,
            rows_written = stats.rows_written,
            rows_dropped = stats.rows_dropped,
            segments_read = stats.segments_read,
            segments_written = stats.segments_written,
            elapsed_ms = start_time.elapsed().as_millis(),
            "requested partition optimization completed"
        );

        Ok(stats)
    }

    fn compact_segment_group(
        &self,
        db: &str,
        table: &str,
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
        let partition_label = partition_key.unwrap_or("<unpartitioned>");

        tracing::info!(
            db,
            table,
            table_full_name = %format!("{}.{}", db, table),
            partition_key = partition_label,
            segments = segment_files.len(),
            "compact partition started"
        );

        for seg_path in segment_files {
            let reader = self.segment_mgr.open_reader(seg_path)?;
            let mut segment_row_base = 0u64;
            for rg_idx in 0..reader.num_row_groups() {
                let batch_idx = source_row_groups.len();
                let row_count = if cluster_key_indices.is_empty() {
                    reader.row_group_rows(rg_idx) as usize
                } else {
                    let key_batch = reader.read_columns_fast(rg_idx, cluster_key_indices)?;
                    let row_count = key_batch.num_rows();
                    key_batches.push(key_batch);
                    row_count
                };
                for row_idx in 0..row_count {
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
                segment_row_base += row_count as u64;
                source_row_groups.push(SourceRowGroup {
                    segment_path: seg_path.clone(),
                    row_group_idx: rg_idx,
                });
            }
        }

        if row_refs.is_empty() {
            for old_seg in segment_files {
                self.segment_mgr.remove_segment_file(old_seg)?;
            }
            tracing::info!(
                db,
                table,
                table_full_name = %format!("{}.{}", db, table),
                partition_key = partition_label,
                segments_read = segment_files.len(),
                rows_dropped = deleted_rows,
                "compact partition removed empty/deleted segments"
            );
            return Ok(CompactStats {
                segments_read: segment_files.len(),
                rows_dropped: deleted_rows,
                ..CompactStats::default()
            });
        }

        sort_row_refs(&mut row_refs, &key_batches);

        let final_path = self
            .segment_mgr
            .next_replacement_segment_path(table_dir, partition_key)?;
        let tmp_path = store::replacement_tmp_path(&final_path, "compact_tmp");

        tracing::info!(
            db,
            table,
            table_full_name = %format!("{}.{}", db, table),
            partition_key = partition_label,
            source_row_groups = source_row_groups.len(),
            rows_to_write = row_refs.len(),
            tmp_path = %tmp_path.display(),
            final_path = %final_path.display(),
            "compact partition writing replacement segment"
        );

        let write_result = self.write_sorted_segment(
            &tmp_path,
            schema,
            cluster_key_indices,
            &key_batches,
            &source_row_groups,
            &row_refs,
        );
        if let Err(err) = write_result {
            let _ = std::fs::remove_file(&tmp_path);
            tracing::warn!(
                db,
                table,
                table_full_name = %format!("{}.{}", db, table),
                partition_key = partition_label,
                tmp_path = %tmp_path.display(),
                error = %err,
                "compact partition write failed"
            );
            return Err(err);
        }

        self.segment_mgr
            .replace_segments_with_tmp(&tmp_path, &final_path, segment_files)?;

        tracing::info!(
            db,
            table,
            table_full_name = %format!("{}.{}", db, table),
            partition_key = partition_label,
            final_path = %final_path.display(),
            segments_read = segment_files.len(),
            rows_written = row_refs.len(),
            rows_dropped = deleted_rows,
            "compact partition completed"
        );

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
        cluster_key_indices: &[usize],
        key_batches: &[RecordBatch],
        source_row_groups: &[SourceRowGroup],
        row_refs: &[RowRef],
    ) -> Result<()> {
        let mut writer = SegmentWriter::create_with_bloom_columns(
            path,
            schema.clone(),
            COMPACT_ROW_GROUP_SIZE,
            cluster_key_indices,
        )?;
        let mut rows_written = 0usize;
        let non_key_projection = non_key_projection(schema.num_columns(), cluster_key_indices);
        let projected_positions =
            projected_column_positions(schema.num_columns(), &non_key_projection);
        let key_positions = key_column_positions(schema.num_columns(), cluster_key_indices);
        let mut row_group_cache = RowGroupCache::new(source_row_groups.len(), non_key_projection);

        tracing::info!(
            source_row_groups = source_row_groups.len(),
            cache_capacity = row_group_cache.capacity(),
            chunk_rows = WRITE_GATHER_CHUNK_ROWS,
            "compact write using decoded row group cache"
        );

        for chunk in row_refs.chunks(WRITE_GATHER_CHUNK_ROWS) {
            let batch = self.gather_sorted_chunk(
                schema,
                key_batches,
                &key_positions,
                &projected_positions,
                source_row_groups,
                chunk,
                &mut row_group_cache,
            )?;
            rows_written += batch.num_rows();
            writer.write_batch(&batch)?;
            if rows_written.is_multiple_of(WRITE_GATHER_CHUNK_ROWS * 16) {
                tracing::info!(
                    rows_written,
                    total_rows = row_refs.len(),
                    "compact write progress"
                );
            }
        }

        tracing::info!(
            rows_written,
            cache_hits = row_group_cache.hits(),
            cache_misses = row_group_cache.misses(),
            cached_row_groups = row_group_cache.len(),
            "compact write gathered sorted rows"
        );

        writer.seal()
    }

    fn gather_sorted_chunk(
        &self,
        schema: &Schema,
        key_batches: &[RecordBatch],
        key_positions: &[Option<usize>],
        projected_positions: &[Option<usize>],
        source_row_groups: &[SourceRowGroup],
        row_refs: &[RowRef],
        row_group_cache: &mut RowGroupCache,
    ) -> Result<RecordBatch> {
        let mut positions_by_source: BTreeMap<usize, Vec<(usize, usize)>> = BTreeMap::new();
        for (output_pos, row_ref) in row_refs.iter().enumerate() {
            positions_by_source
                .entry(row_ref.batch_idx)
                .or_default()
                .push((output_pos, row_ref.row_idx));
        }

        let mut columns = schema
            .columns
            .iter()
            .map(|col| empty_column_for_gather(col.data_type, row_refs.len()))
            .collect::<Vec<_>>();

        for (source_idx, positions) in positions_by_source {
            let source = source_row_groups.get(source_idx).ok_or_else(|| {
                TaibaiError::Storage(format!("missing source row group {}", source_idx))
            })?;
            let src_batch = row_group_cache.get(source, &self.segment_mgr)?;

            for col_idx in 0..schema.columns.len() {
                let src_col = if let Some(key_pos) = key_positions[col_idx] {
                    key_batches
                        .get(source_idx)
                        .and_then(|batch| batch.column(key_pos))
                        .ok_or_else(|| {
                            TaibaiError::Storage(format!(
                                "missing key column {} in row group {}",
                                col_idx, source_idx
                            ))
                        })?
                } else {
                    let projected_pos = projected_positions[col_idx].ok_or_else(|| {
                        TaibaiError::Storage(format!(
                            "missing projection for source column {}",
                            col_idx
                        ))
                    })?;
                    src_batch.column(projected_pos).ok_or_else(|| {
                        TaibaiError::Storage(format!(
                            "missing source column {} in row group {}",
                            col_idx, source_idx
                        ))
                    })?
                };
                gather_column_positions(&mut columns[col_idx], src_col, &positions)?;
            }
        }

        RecordBatch::try_new(schema.clone(), columns)
    }

    fn get_cluster_key_indices(
        &self,
        table_meta: &TableMeta,
        schema: &Schema,
    ) -> Result<Vec<usize>> {
        let mut indices = Vec::new();
        for key_col in &table_meta.cluster_key {
            let idx = schema.index_of(key_col).ok_or_else(|| {
                TaibaiError::Sql(format!("cluster key column '{}' not found", key_col))
            })?;
            indices.push(idx);
        }
        Ok(indices)
    }
}

struct RowGroupCache {
    capacity: usize,
    projection: Vec<usize>,
    batches: HashMap<(PathBuf, usize), RecordBatch>,
    order: VecDeque<(PathBuf, usize)>,
    hits: usize,
    misses: usize,
}

impl RowGroupCache {
    fn new(capacity: usize, projection: Vec<usize>) -> Self {
        Self {
            capacity: capacity.max(1),
            projection,
            batches: HashMap::new(),
            order: VecDeque::new(),
            hits: 0,
            misses: 0,
        }
    }

    fn capacity(&self) -> usize {
        self.capacity
    }

    fn len(&self) -> usize {
        self.batches.len()
    }

    fn hits(&self) -> usize {
        self.hits
    }

    fn misses(&self) -> usize {
        self.misses
    }

    fn get(
        &mut self,
        source: &SourceRowGroup,
        segment_mgr: &SegmentManager,
    ) -> Result<&RecordBatch> {
        let key = (source.segment_path.clone(), source.row_group_idx);
        if self.batches.contains_key(&key) {
            self.hits += 1;
            self.touch(&key);
            return Ok(self.batches.get(&key).unwrap());
        }

        self.misses += 1;
        let reader = segment_mgr.open_reader(&source.segment_path)?;
        let batch =
            reader.read_row_group_with_projection(source.row_group_idx, &self.projection)?;
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

fn non_key_projection(num_columns: usize, cluster_key_indices: &[usize]) -> Vec<usize> {
    (0..num_columns)
        .filter(|idx| !cluster_key_indices.contains(idx))
        .collect()
}

fn projected_column_positions(num_columns: usize, projection: &[usize]) -> Vec<Option<usize>> {
    let mut positions = vec![None; num_columns];
    for (projected_pos, &col_idx) in projection.iter().enumerate() {
        if col_idx < num_columns {
            positions[col_idx] = Some(projected_pos);
        }
    }
    positions
}

fn key_column_positions(num_columns: usize, cluster_key_indices: &[usize]) -> Vec<Option<usize>> {
    let mut positions = vec![None; num_columns];
    for (key_pos, &col_idx) in cluster_key_indices.iter().enumerate() {
        if col_idx < num_columns {
            positions[col_idx] = Some(key_pos);
        }
    }
    positions
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

fn empty_column_for_gather(data_type: DataType, len: usize) -> ColumnArray {
    let bitmap = NullBitmap::new_valid(len);
    match data_type {
        DataType::Bool => ColumnArray::Bool {
            data: vec![0; len],
            bitmap,
        },
        DataType::Int8 => ColumnArray::Int8 {
            data: vec![0; len],
            bitmap,
        },
        DataType::Int16 => ColumnArray::Int16 {
            data: vec![0; len],
            bitmap,
        },
        DataType::Int32 => ColumnArray::Int32 {
            data: vec![0; len],
            bitmap,
        },
        DataType::Int64 => ColumnArray::Int64 {
            data: vec![0; len],
            bitmap,
        },
        DataType::UInt8 => ColumnArray::UInt8 {
            data: vec![0; len],
            bitmap,
        },
        DataType::UInt16 => ColumnArray::UInt16 {
            data: vec![0; len],
            bitmap,
        },
        DataType::UInt32 => ColumnArray::UInt32 {
            data: vec![0; len],
            bitmap,
        },
        DataType::UInt64 => ColumnArray::UInt64 {
            data: vec![0; len],
            bitmap,
        },
        DataType::Float32 => ColumnArray::Float32 {
            data: vec![0.0; len],
            bitmap,
        },
        DataType::Float64 => ColumnArray::Float64 {
            data: vec![0.0; len],
            bitmap,
        },
        DataType::Decimal => ColumnArray::Decimal {
            data: vec![0; len],
            bitmap,
        },
        DataType::Date => ColumnArray::Date {
            data: vec![0; len],
            bitmap,
        },
        DataType::Time => ColumnArray::Time {
            data: vec![0; len],
            bitmap,
        },
        DataType::Timestamp => ColumnArray::Timestamp {
            data: vec![0; len],
            bitmap,
        },
        DataType::Interval => ColumnArray::Interval {
            data: vec![0; len],
            bitmap,
        },
        DataType::VarChar => ColumnArray::VarChar {
            data: vec![String::new(); len],
            bitmap,
        },
        DataType::Blob => ColumnArray::Blob {
            data: vec![Vec::new(); len],
            bitmap,
        },
        DataType::Vector => ColumnArray::Vector {
            data: vec![Vec::new(); len],
            bitmap,
        },
    }
}

fn gather_column_positions(
    dst: &mut ColumnArray,
    src: &ColumnArray,
    positions: &[(usize, usize)],
) -> Result<()> {
    macro_rules! copy_fixed {
        ($dst_data:expr, $dst_bitmap:expr, $src_data:expr, $src_bitmap:expr) => {{
            for &(output_pos, source_row_idx) in positions {
                $dst_data[output_pos] = $src_data[source_row_idx];
                if $src_bitmap.is_null(source_row_idx) {
                    $dst_bitmap.set_null(output_pos);
                }
            }
        }};
    }

    macro_rules! copy_clone {
        ($dst_data:expr, $dst_bitmap:expr, $src_data:expr, $src_bitmap:expr) => {{
            for &(output_pos, source_row_idx) in positions {
                $dst_data[output_pos] = $src_data[source_row_idx].clone();
                if $src_bitmap.is_null(source_row_idx) {
                    $dst_bitmap.set_null(output_pos);
                }
            }
        }};
    }

    match (dst, src) {
        (
            ColumnArray::Bool {
                data: dst_data,
                bitmap: dst_bitmap,
            },
            ColumnArray::Bool {
                data: src_data,
                bitmap: src_bitmap,
            },
        ) => copy_fixed!(dst_data, dst_bitmap, src_data, src_bitmap),
        (
            ColumnArray::Int8 {
                data: dst_data,
                bitmap: dst_bitmap,
            },
            ColumnArray::Int8 {
                data: src_data,
                bitmap: src_bitmap,
            },
        ) => copy_fixed!(dst_data, dst_bitmap, src_data, src_bitmap),
        (
            ColumnArray::Int16 {
                data: dst_data,
                bitmap: dst_bitmap,
            },
            ColumnArray::Int16 {
                data: src_data,
                bitmap: src_bitmap,
            },
        ) => copy_fixed!(dst_data, dst_bitmap, src_data, src_bitmap),
        (
            ColumnArray::Int32 {
                data: dst_data,
                bitmap: dst_bitmap,
            },
            ColumnArray::Int32 {
                data: src_data,
                bitmap: src_bitmap,
            },
        ) => copy_fixed!(dst_data, dst_bitmap, src_data, src_bitmap),
        (
            ColumnArray::Int64 {
                data: dst_data,
                bitmap: dst_bitmap,
            },
            ColumnArray::Int64 {
                data: src_data,
                bitmap: src_bitmap,
            },
        ) => copy_fixed!(dst_data, dst_bitmap, src_data, src_bitmap),
        (
            ColumnArray::UInt8 {
                data: dst_data,
                bitmap: dst_bitmap,
            },
            ColumnArray::UInt8 {
                data: src_data,
                bitmap: src_bitmap,
            },
        ) => copy_fixed!(dst_data, dst_bitmap, src_data, src_bitmap),
        (
            ColumnArray::UInt16 {
                data: dst_data,
                bitmap: dst_bitmap,
            },
            ColumnArray::UInt16 {
                data: src_data,
                bitmap: src_bitmap,
            },
        ) => copy_fixed!(dst_data, dst_bitmap, src_data, src_bitmap),
        (
            ColumnArray::UInt32 {
                data: dst_data,
                bitmap: dst_bitmap,
            },
            ColumnArray::UInt32 {
                data: src_data,
                bitmap: src_bitmap,
            },
        ) => copy_fixed!(dst_data, dst_bitmap, src_data, src_bitmap),
        (
            ColumnArray::UInt64 {
                data: dst_data,
                bitmap: dst_bitmap,
            },
            ColumnArray::UInt64 {
                data: src_data,
                bitmap: src_bitmap,
            },
        ) => copy_fixed!(dst_data, dst_bitmap, src_data, src_bitmap),
        (
            ColumnArray::Float32 {
                data: dst_data,
                bitmap: dst_bitmap,
            },
            ColumnArray::Float32 {
                data: src_data,
                bitmap: src_bitmap,
            },
        ) => copy_fixed!(dst_data, dst_bitmap, src_data, src_bitmap),
        (
            ColumnArray::Float64 {
                data: dst_data,
                bitmap: dst_bitmap,
            },
            ColumnArray::Float64 {
                data: src_data,
                bitmap: src_bitmap,
            },
        ) => copy_fixed!(dst_data, dst_bitmap, src_data, src_bitmap),
        (
            ColumnArray::Decimal {
                data: dst_data,
                bitmap: dst_bitmap,
            },
            ColumnArray::Decimal {
                data: src_data,
                bitmap: src_bitmap,
            },
        ) => copy_fixed!(dst_data, dst_bitmap, src_data, src_bitmap),
        (
            ColumnArray::Date {
                data: dst_data,
                bitmap: dst_bitmap,
            },
            ColumnArray::Date {
                data: src_data,
                bitmap: src_bitmap,
            },
        ) => copy_fixed!(dst_data, dst_bitmap, src_data, src_bitmap),
        (
            ColumnArray::Time {
                data: dst_data,
                bitmap: dst_bitmap,
            },
            ColumnArray::Time {
                data: src_data,
                bitmap: src_bitmap,
            },
        ) => copy_fixed!(dst_data, dst_bitmap, src_data, src_bitmap),
        (
            ColumnArray::Timestamp {
                data: dst_data,
                bitmap: dst_bitmap,
            },
            ColumnArray::Timestamp {
                data: src_data,
                bitmap: src_bitmap,
            },
        ) => copy_fixed!(dst_data, dst_bitmap, src_data, src_bitmap),
        (
            ColumnArray::Interval {
                data: dst_data,
                bitmap: dst_bitmap,
            },
            ColumnArray::Interval {
                data: src_data,
                bitmap: src_bitmap,
            },
        ) => copy_fixed!(dst_data, dst_bitmap, src_data, src_bitmap),
        (
            ColumnArray::VarChar {
                data: dst_data,
                bitmap: dst_bitmap,
            },
            ColumnArray::VarChar {
                data: src_data,
                bitmap: src_bitmap,
            },
        ) => copy_clone!(dst_data, dst_bitmap, src_data, src_bitmap),
        (
            ColumnArray::Blob {
                data: dst_data,
                bitmap: dst_bitmap,
            },
            ColumnArray::Blob {
                data: src_data,
                bitmap: src_bitmap,
            },
        ) => copy_clone!(dst_data, dst_bitmap, src_data, src_bitmap),
        (
            ColumnArray::Vector {
                data: dst_data,
                bitmap: dst_bitmap,
            },
            ColumnArray::Vector {
                data: src_data,
                bitmap: src_bitmap,
            },
        ) => copy_clone!(dst_data, dst_bitmap, src_data, src_bitmap),
        (dst, src) => {
            return Err(TaibaiError::Storage(format!(
                "column type mismatch while compacting: dst={} src={}",
                dst.data_type().display_name(),
                src.data_type().display_name()
            )));
        }
    }
    Ok(())
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

fn partition_key_candidates(raw: &str) -> Vec<String> {
    let key = raw.trim();
    let mut candidates = vec![key.to_string()];
    if key.len() > 1
        && key.as_bytes()[0].eq_ignore_ascii_case(&b'p')
        && key[1..].chars().next().is_some_and(|c| c.is_ascii_digit())
    {
        candidates.push(key[1..].to_string());
    }
    candidates
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::types::DataType;
    use crate::storage::catalog::schema::PartitionType;
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

    fn partitioned_test_meta(schema: &Schema) -> TableMeta {
        let mut meta = test_meta(schema);
        meta.partition_type = PartitionType::Year;
        meta.partition_key = "id".to_string();
        meta.update_partition_key_idx();
        meta
    }

    fn test_meta_without_cluster(schema: &Schema) -> TableMeta {
        TableMeta::new("default", "t", schema.columns.clone())
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

    #[test]
    fn optimize_table_without_cluster_key_preserves_row_order() -> Result<()> {
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
        let stats =
            compactor.optimize_table("default", "t", &test_meta_without_cluster(&schema))?;

        assert_eq!(stats.segments_read, 2);
        assert_eq!(stats.segments_written, 1);
        assert_eq!(stats.rows_written, 4);

        let segments = SegmentManager::discover_segments(&table_dir)?;
        assert_eq!(segments.len(), 1);
        assert_eq!(read_ids(&segments[0])?, vec![3, 1, 4, 2]);
        Ok(())
    }

    #[test]
    fn optimize_partition_only_rewrites_requested_partition() -> Result<()> {
        let tmp = tempfile::tempdir().unwrap();
        let table_dir = tmp.path().join("default").join("t");
        std::fs::create_dir_all(&table_dir)?;

        let schema = test_schema();
        write_segment(
            &table_dir.join("p2024_0001.dat"),
            &schema,
            &[(3, "c"), (1, "a")],
        )?;
        write_segment(
            &table_dir.join("p2024_0002.dat"),
            &schema,
            &[(4, "d"), (2, "b")],
        )?;
        write_segment(&table_dir.join("p2025_0001.dat"), &schema, &[(30, "z")])?;
        write_segment(&table_dir.join("p2025_0002.dat"), &schema, &[(20, "y")])?;

        let segment_mgr = Arc::new(SegmentManager::new(tmp.path())?);
        let compactor = Compactor::new(segment_mgr, tmp.path().to_path_buf());
        let stats = compactor.optimize_partition(
            "default",
            "t",
            &partitioned_test_meta(&schema),
            "P2024",
        )?;

        assert_eq!(stats.segments_read, 2);
        assert_eq!(stats.segments_written, 1);
        assert_eq!(stats.rows_written, 4);

        let mut files = SegmentManager::discover_segments(&table_dir)?
            .into_iter()
            .filter_map(|path| {
                path.file_name()
                    .map(|name| name.to_string_lossy().to_string())
            })
            .collect::<Vec<_>>();
        files.sort();

        assert_eq!(
            files,
            vec![
                "p2024_0003.dat".to_string(),
                "p2025_0001.dat".to_string(),
                "p2025_0002.dat".to_string(),
            ]
        );
        assert_eq!(
            read_ids(&table_dir.join("p2024_0003.dat"))?,
            vec![1, 2, 3, 4]
        );
        Ok(())
    }
}
