//! Bounded run merge, row-group caching and output gathering.

use super::*;

impl Compactor {
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
        let mut gathered_bytes = 0usize;

        for (source_idx, positions) in positions_by_source {
            if positions.is_empty() {
                continue;
            }
            let source = source_row_groups.get(source_idx).ok_or_else(|| {
                TaibaiError::Storage(format!("missing source row group {}", source_idx))
            })?;
            let src_batch = row_group_cache.get(source, &self.segment_mgr)?;
            for &(_, row) in &positions {
                gathered_bytes = gathered_bytes
                    .saturating_add(super::runs::row_allocation_bytes(src_batch, row));
            }
            ensure_compaction_memory(
                gathered_bytes,
                (clustered_merge_budget_bytes() / 4).max(4096),
                "output gather",
            )?;

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
    bytes: usize,
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
            bytes: 0,
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
        let bytes = batch.estimated_size_bytes();
        ensure_compaction_memory(
            bytes,
            (clustered_merge_budget_bytes() / 4).max(4096),
            "cached row group",
        )?;
        self.bytes = self.bytes.saturating_add(bytes);
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
        while self.batches.len() > self.capacity
            || (self.bytes > (clustered_merge_budget_bytes() / 8).max(4096)
                && self.batches.len() > 1)
        {
            if let Some(old_key) = self.order.pop_front() {
                if let Some(batch) = self.batches.remove(&old_key) {
                    self.bytes -= batch.estimated_size_bytes();
                }
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
