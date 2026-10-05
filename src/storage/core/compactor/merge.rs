//! Bounded run merge, row-group caching and output gathering.

use super::*;

impl Compactor {
    /// K-way merge of sorted run files into `writer`, gathering rows through
    /// a bounded row-group cache.
    pub(super) fn merge_runs_into_writer(
        &self,
        run_paths: &[PathBuf],
        schema: &Schema,
        sort_key_indices: &[usize],
        output_row_group_rows: usize,
        writer: &mut SegmentWriter,
    ) -> Result<usize> {
        // Streams are files; each file's row groups occupy contiguous slots
        // in source_row_groups, so (source_idx, row_idx) ordering matches the
        // original global row order for stable tie-breaks.
        let mut source_row_groups: Vec<SourceRowGroup> = Vec::new();
        let mut rows_per_source: Vec<usize> = Vec::new();
        let mut file_bounds: Vec<(usize, usize)> = Vec::new(); // [start, end) source slots
        for path in run_paths {
            let reader = self.segment_mgr.open_reader(path)?;
            let start = source_row_groups.len();
            for rg_idx in 0..reader.num_row_groups() {
                source_row_groups.push(SourceRowGroup {
                    segment_path: path.clone(),
                    row_group_idx: rg_idx,
                });
                rows_per_source.push(reader.row_group_rows(rg_idx) as usize);
            }
            file_bounds.push((start, source_row_groups.len()));
        }

        let mut cache = RowGroupCache::new(32, schema.clone());

        struct MergeHead<'k> {
            keys: Vec<crate::types::scalar::ScalarValue>,
            source_idx: usize,
            row_idx: usize,
            sort_keys: &'k [usize],
        }
        impl PartialEq for MergeHead<'_> {
            fn eq(&self, other: &Self) -> bool {
                self.cmp(other) == std::cmp::Ordering::Equal
            }
        }
        impl Eq for MergeHead<'_> {}
        impl PartialOrd for MergeHead<'_> {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                Some(self.cmp(other))
            }
        }
        impl Ord for MergeHead<'_> {
            fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                for (col, _key_idx) in self.sort_keys.iter().enumerate() {
                    let Some(left) = self.keys.get(col) else {
                        continue;
                    };
                    let Some(right) = other.keys.get(col) else {
                        continue;
                    };
                    let ord =
                        crate::storage::index::sort_key::compare_scalars_for_sort(left, right);
                    if ord != std::cmp::Ordering::Equal {
                        return ord
                            // Stable, deterministic tie-break: original row
                            // order.
                            .then(self.source_idx.cmp(&other.source_idx))
                            .then(self.row_idx.cmp(&other.row_idx));
                    }
                }
                self.source_idx
                    .cmp(&other.source_idx)
                    .then(self.row_idx.cmp(&other.row_idx))
            }
        }

        // BinaryHeap is a max-heap; reverse for smallest-first.
        use std::cmp::Reverse;
        use std::collections::BinaryHeap;
        let mut heap: BinaryHeap<Reverse<MergeHead>> = BinaryHeap::new();

        let materialize_keys = |cache: &mut RowGroupCache,
                                source_idx: usize,
                                row_idx: usize|
         -> Result<Vec<crate::types::scalar::ScalarValue>> {
            let source = &source_row_groups[source_idx];
            let batch = cache.get(source, &self.segment_mgr)?;
            Ok(sort_key_indices
                .iter()
                .map(|&col| {
                    batch
                        .column(col)
                        .map(|column| column.scalar(row_idx))
                        .unwrap_or(crate::types::scalar::ScalarValue::Int64(None))
                })
                .collect())
        };

        for &(start, end) in &file_bounds {
            // First NON-EMPTY row group of the file (a leading empty row
            // group must not hide the rest of the file).
            let mut first_source = start;
            while first_source < end && rows_per_source[first_source] == 0 {
                first_source += 1;
            }
            if first_source >= end {
                continue;
            }
            let keys = materialize_keys(&mut cache, first_source, 0)?;
            heap.push(Reverse(MergeHead {
                keys,
                source_idx: first_source,
                row_idx: 0,
                sort_keys: sort_key_indices,
            }));
        }

        let output_row_group_rows = output_row_group_rows.max(1);
        let mut chunk: Vec<RowRef> = Vec::with_capacity(output_row_group_rows);
        let mut rows_written = 0usize;
        while let Some(Reverse(head)) = heap.pop() {
            chunk.push(RowRef {
                batch_idx: head.source_idx,
                row_idx: head.row_idx,
            });
            rows_written += 1;
            if chunk.len() >= output_row_group_rows {
                let batch =
                    self.gather_cached_chunk(schema, &source_row_groups, &chunk, &mut cache)?;
                writer.write_batch_parallel(&batch)?;
                chunk.clear();
            }
            // Advance this stream: next row in the same row group, else the
            // next NON-EMPTY row group of the same file (empty row groups
            // must be skipped forward, not abandon the file).
            let mut next_source = head.source_idx;
            let mut next_row = head.row_idx + 1;
            if next_row >= rows_per_source[next_source] {
                next_row = 0;
                let file = file_bounds
                    .iter()
                    .find(|&&(start, end)| head.source_idx >= start && head.source_idx < end);
                let end = match file {
                    Some(&(_start, end)) => end,
                    None => source_row_groups.len(),
                };
                next_source += 1;
                while next_source < end && rows_per_source[next_source] == 0 {
                    next_source += 1;
                }
                if next_source >= end {
                    continue; // file exhausted
                }
            }
            let keys = materialize_keys(&mut cache, next_source, next_row)?;
            heap.push(Reverse(MergeHead {
                keys,
                source_idx: next_source,
                row_idx: next_row,
                sort_keys: sort_key_indices,
            }));
        }
        if !chunk.is_empty() {
            let batch = self.gather_cached_chunk(schema, &source_row_groups, &chunk, &mut cache)?;
            writer.write_batch_parallel(&batch)?;
        }
        Ok(rows_written)
    }

    pub(super) fn gather_cached_chunk(
        &self,
        schema: &Schema,
        source_row_groups: &[SourceRowGroup],
        row_refs: &[RowRef],
        row_group_cache: &mut RowGroupCache,
    ) -> Result<RecordBatch> {
        // Only touched sources consume gather bookkeeping, even when a
        // partition contains millions of historical row groups.
        let mut positions_by_source = BTreeMap::<usize, Vec<(usize, usize)>>::new();
        for (output_pos, row_ref) in row_refs.iter().enumerate() {
            if row_ref.batch_idx >= source_row_groups.len() {
                return Err(TaibaiError::Storage(format!(
                    "missing source row group {} during gather",
                    row_ref.batch_idx
                )));
            }
            let positions = positions_by_source.entry(row_ref.batch_idx).or_default();
            positions.push((output_pos, row_ref.row_idx));
        }

        let mut columns = schema
            .columns
            .iter()
            .map(|col| GatherColumn::new(col.data_type, row_refs.len()))
            .collect::<Vec<_>>();

        for (source_idx, positions) in positions_by_source {
            if positions.is_empty() {
                continue;
            }
            let source = source_row_groups.get(source_idx).ok_or_else(|| {
                TaibaiError::Storage(format!("missing source row group {}", source_idx))
            })?;
            let src_batch = row_group_cache.get(source, &self.segment_mgr)?;

            for col_idx in 0..schema.columns.len() {
                let src_col = src_batch.column(col_idx).ok_or_else(|| {
                    TaibaiError::Storage(format!(
                        "missing source column {} in row group {}",
                        col_idx, source_idx
                    ))
                })?;
                columns[col_idx].scatter(src_col, &positions)?;
            }
        }

        RecordBatch::try_new(
            schema.clone(),
            columns.into_iter().map(GatherColumn::finish).collect(),
        )
    }
}

pub(super) struct RowGroupCache {
    pub(super) capacity: usize,
    pub(super) schema: Schema,
    pub(super) batches: HashMap<(PathBuf, usize), RecordBatch>,
    pub(super) order: VecDeque<(PathBuf, usize)>,
    pub(super) hits: usize,
    pub(super) misses: usize,
}

impl RowGroupCache {
    pub(super) fn new(capacity: usize, schema: Schema) -> Self {
        Self {
            capacity: capacity.max(1),
            schema,
            batches: HashMap::new(),
            order: VecDeque::new(),
            hits: 0,
            misses: 0,
        }
    }

    pub(super) fn hits(&self) -> usize {
        self.hits
    }

    pub(super) fn misses(&self) -> usize {
        self.misses
    }

    pub(super) fn get(
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
        // Reuse the parsed footer and mmap for every row group in the same
        // segment. Reopening here is especially expensive for fragmented
        // segments because their footer can be almost as large as the data.
        let reader = segment_mgr.open_reader_cached(&source.segment_path)?;
        let batch = reader.read_row_group_columns_aligned(source.row_group_idx, &self.schema)?;
        self.batches.insert(key.clone(), batch);
        self.order.push_back(key.clone());
        self.evict_if_needed();
        Ok(self.batches.get(&key).unwrap())
    }

    pub(super) fn touch(&mut self, key: &(PathBuf, usize)) {
        if let Some(pos) = self.order.iter().position(|existing| existing == key) {
            self.order.remove(pos);
        }
        self.order.push_back(key.clone());
    }

    pub(super) fn evict_if_needed(&mut self) {
        while self.batches.len() > self.capacity {
            if let Some(old_key) = self.order.pop_front() {
                self.batches.remove(&old_key);
            } else {
                break;
            }
        }
    }
}

pub(super) fn compare_row_indices(
    left: u32,
    right: u32,
    source_batch: &RecordBatch,
    sort_key_indices: &[usize],
) -> Ordering {
    for &col_idx in sort_key_indices {
        let Some(column) = source_batch.column(col_idx) else {
            continue;
        };
        let cmp = compare_column_rows(column, left as usize, column, right as usize);
        if cmp != Ordering::Equal {
            return cmp;
        }
    }
    Ordering::Equal
}

// Initialization and scatter copying share this type table, so adding a
// fixed-width type cannot update one operation while leaving the other out.
macro_rules! fixed_column_types {
    ($callback:ident) => {
        $callback!(Bool => 0, Int8 => 0, Int16 => 0, Int32 => 0, Int64 => 0,
            UInt8 => 0, UInt16 => 0, UInt32 => 0, UInt64 => 0,
            Float32 => 0.0, Float64 => 0.0, Decimal => 0,
            Date => 0, Time => 0, Timestamp => 0, Interval => 0)
    };
}

pub(super) fn empty_column_for_gather(data_type: DataType, len: usize) -> ColumnArray {
    let bitmap = NullBitmap::new_valid(len);
    macro_rules! fixed_columns {
        ($($variant:ident => $zero:expr),+ $(,)?) => {
            match data_type {
                $(DataType::$variant => ColumnArray::$variant { data: vec![$zero; len], bitmap },)+
                DataType::VarChar => ColumnArray::VarChar { data: vec![String::new(); len].into(), bitmap },
                DataType::Blob => ColumnArray::Blob { data: vec![Vec::new(); len].into(), bitmap },
                DataType::Vector => ColumnArray::Vector { data: vec![Vec::new(); len], bitmap },
            }
        };
    }
    fixed_column_types!(fixed_columns)
}

pub(super) fn gather_column_positions(
    dst: &mut ColumnArray,
    src: &ColumnArray,
    positions: &[(usize, usize)],
) -> Result<()> {
    macro_rules! fixed_columns {
        ($($variant:ident => $zero:expr),+ $(,)?) => {
            match (dst, src) {
                $((ColumnArray::$variant { data: dst, bitmap: dst_bitmap },
                   ColumnArray::$variant { data: src, bitmap: src_bitmap }) => {
                    for &(output, row) in positions {
                        dst[output] = src[row];
                        if src_bitmap.is_null(row) { dst_bitmap.set_null(output); }
                    }
                },)+
                (ColumnArray::Vector { data: dst, bitmap: dst_bitmap },
                 ColumnArray::Vector { data: src, bitmap: src_bitmap }) => {
                    for &(output, row) in positions {
                        dst[output] = src[row].clone();
                        if src_bitmap.is_null(row) { dst_bitmap.set_null(output); }
                    }
                }
                (dst, src) => return Err(TaibaiError::Storage(format!(
                    "column type mismatch while compacting: dst={} src={}",
                    dst.data_type().display_name(), src.data_type().display_name()
                ))),
            }
        };
    }
    fixed_column_types!(fixed_columns);
    Ok(())
}

// Variable-width rows are staged by position, then packed exactly once in
// output order. Mutating packed buffers per row would repeatedly shift their
// payload and rebase all subsequent offsets (quadratic work).
enum GatherColumn {
    Fixed(ColumnArray),
    Text {
        data: Vec<String>,
        bitmap: NullBitmap,
    },
    Binary {
        data: Vec<Vec<u8>>,
        bitmap: NullBitmap,
    },
}

impl GatherColumn {
    fn new(data_type: DataType, len: usize) -> Self {
        match data_type {
            DataType::VarChar => Self::Text {
                data: vec![String::new(); len],
                bitmap: NullBitmap::new_valid(len),
            },
            DataType::Blob => Self::Binary {
                data: vec![Vec::new(); len],
                bitmap: NullBitmap::new_valid(len),
            },
            _ => Self::Fixed(empty_column_for_gather(data_type, len)),
        }
    }

    fn scatter(&mut self, src: &ColumnArray, positions: &[(usize, usize)]) -> Result<()> {
        match (self, src) {
            (Self::Fixed(dst), src) => gather_column_positions(dst, src, positions),
            (
                Self::Text { data, bitmap },
                ColumnArray::VarChar {
                    data: src,
                    bitmap: src_bitmap,
                },
            ) => {
                for &(out, row) in positions {
                    data[out] = src.value(row).to_owned();
                    if src_bitmap.is_null(row) {
                        bitmap.set_null(out);
                    }
                }
                Ok(())
            }
            (
                Self::Binary { data, bitmap },
                ColumnArray::Blob {
                    data: src,
                    bitmap: src_bitmap,
                },
            ) => {
                for &(out, row) in positions {
                    data[out] = src.value(row).to_owned();
                    if src_bitmap.is_null(row) {
                        bitmap.set_null(out);
                    }
                }
                Ok(())
            }
            _ => Err(TaibaiError::Storage(
                "variable-width column type mismatch while compacting".into(),
            )),
        }
    }

    fn finish(self) -> ColumnArray {
        match self {
            Self::Fixed(column) => column,
            Self::Text { data, bitmap } => ColumnArray::VarChar {
                data: data.into(),
                bitmap,
            },
            Self::Binary { data, bitmap } => ColumnArray::Blob {
                data: data.into(),
                bitmap,
            },
        }
    }
}
