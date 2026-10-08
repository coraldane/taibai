//! Scalar keys and fused batch grouping.

use super::*;

/// Resolve the index column (leading cluster-key column) for a table. Returns
/// `None` for tables without a cluster key, or whose leading key column is a
/// binary/vector type that cannot be meaningfully indexed.
pub fn resolve_key_column(table_meta: &TableMeta) -> Option<usize> {
    table_meta.hot_time_column.as_ref()?;
    let key = table_meta
        .hot_index_column
        .as_deref()
        .or_else(|| table_meta.cluster_key.first().map(String::as_str))?;
    let idx = (0..table_meta.columns.len())
        .find(|i| table_meta.columns[*i].name.eq_ignore_ascii_case(key))?;
    match table_meta.columns[idx].data_type {
        DataType::Blob | DataType::Vector => None,
        _ => Some(idx),
    }
}

/// Convert a predicate scalar to an index key on the read path. Variable-length
/// strings are resolved against the pool; an unseen string yields `Ok(None)`
/// (no matching rows). Binary/vector values are unsupported.
pub(super) fn scalar_to_key(sv: &ScalarValue, pool: &StringPool) -> Result<Option<ValueKey>> {
    use ScalarValue::*;
    let key = match sv {
        Blob(_) | Vector(_) => {
            return Err(TaibaiError::Internal(
                "hot store: Blob/Vector are not supported as a cluster-key column".into(),
            ));
        }
        VarChar(Some(s)) => match pool.lookup(s) {
            Some(id) => ValueKey::Str(id),
            None => return Ok(None),
        },
        Bool(Some(b)) => ValueKey::Bool(*b),
        Int8(Some(v)) => ValueKey::Int(*v as i64),
        Int16(Some(v)) => ValueKey::Int(*v as i64),
        Int32(Some(v)) => ValueKey::Int(*v as i64),
        Int64(Some(v)) => ValueKey::Int(*v),
        UInt8(Some(v)) => ValueKey::Int(*v as i64),
        UInt16(Some(v)) => ValueKey::Int(*v as i64),
        UInt32(Some(v)) => ValueKey::Int(*v as i64),
        UInt64(Some(v)) => ValueKey::UInt(*v),
        Float32(Some(v)) => ValueKey::Float(crate::types::scalar::float_key_bits(*v as f64)),
        Float64(Some(v)) => ValueKey::Float(crate::types::scalar::float_key_bits(*v)),
        Decimal(Some(v)) => ValueKey::Int(*v),
        Date(Some(v)) => ValueKey::Int(*v as i64),
        Time(Some(v)) | Timestamp(Some(v)) | Interval(Some(v)) => ValueKey::Int(*v),
        // Any remaining variant carries `None` -> null key (matches NULL rows).
        _ => ValueKey::Null,
    };
    Ok(Some(key))
}

/// Row accessor that yields the inverted-index `ValueKey` for a column without
/// materializing a `Vec<ValueKey>`: the append path groups keys straight from
/// the column range in one pass. Variable-width values dedup through a
/// batch-local map, so the pool interns each distinct string once per slice
/// (the pool's own 1024-slot direct-mapped cache thrashes beyond ~1000
/// distinct symbols).
pub(super) enum KeySource<'a> {
    Bool {
        data: &'a [u8],
        bm: &'a NullBitmap,
    },
    Int8 {
        data: &'a [i8],
        bm: &'a NullBitmap,
    },
    Int16 {
        data: &'a [i16],
        bm: &'a NullBitmap,
    },
    Int32 {
        data: &'a [i32],
        bm: &'a NullBitmap,
    },
    Int64 {
        data: &'a [i64],
        bm: &'a NullBitmap,
    },
    UInt8 {
        data: &'a [u8],
        bm: &'a NullBitmap,
    },
    UInt16 {
        data: &'a [u16],
        bm: &'a NullBitmap,
    },
    UInt32 {
        data: &'a [u32],
        bm: &'a NullBitmap,
    },
    UInt64 {
        data: &'a [u64],
        bm: &'a NullBitmap,
    },
    Float32 {
        data: &'a [f32],
        bm: &'a NullBitmap,
    },
    Float64 {
        data: &'a [f64],
        bm: &'a NullBitmap,
    },
    Decimal {
        data: &'a [i64],
        bm: &'a NullBitmap,
    },
    Date {
        data: &'a [i32],
        bm: &'a NullBitmap,
    },
    Time {
        data: &'a [i64],
        bm: &'a NullBitmap,
    },
    Timestamp {
        data: &'a [i64],
        bm: &'a NullBitmap,
    },
    Interval {
        data: &'a [i64],
        bm: &'a NullBitmap,
    },
    VarChar {
        data: &'a Utf8Buffer,
        bm: &'a NullBitmap,
        dedup: HashMap<&'a str, ValueKey, FastBuild>,
    },
}

impl<'a> KeySource<'a> {
    pub(super) fn new(col: &'a ColumnArray) -> Result<Self> {
        Ok(match col {
            ColumnArray::Bool { data, bitmap } => Self::Bool { data, bm: bitmap },
            ColumnArray::Int8 { data, bitmap } => Self::Int8 { data, bm: bitmap },
            ColumnArray::Int16 { data, bitmap } => Self::Int16 { data, bm: bitmap },
            ColumnArray::Int32 { data, bitmap } => Self::Int32 { data, bm: bitmap },
            ColumnArray::Int64 { data, bitmap } => Self::Int64 { data, bm: bitmap },
            ColumnArray::UInt8 { data, bitmap } => Self::UInt8 { data, bm: bitmap },
            ColumnArray::UInt16 { data, bitmap } => Self::UInt16 { data, bm: bitmap },
            ColumnArray::UInt32 { data, bitmap } => Self::UInt32 { data, bm: bitmap },
            ColumnArray::UInt64 { data, bitmap } => Self::UInt64 { data, bm: bitmap },
            ColumnArray::Float32 { data, bitmap } => Self::Float32 { data, bm: bitmap },
            ColumnArray::Float64 { data, bitmap } => Self::Float64 { data, bm: bitmap },
            ColumnArray::Decimal { data, bitmap } => Self::Decimal { data, bm: bitmap },
            ColumnArray::Date { data, bitmap } => Self::Date { data, bm: bitmap },
            ColumnArray::Time { data, bitmap } => Self::Time { data, bm: bitmap },
            ColumnArray::Timestamp { data, bitmap } => Self::Timestamp { data, bm: bitmap },
            ColumnArray::Interval { data, bitmap } => Self::Interval { data, bm: bitmap },
            ColumnArray::VarChar { data, bitmap } => Self::VarChar {
                data,
                bm: bitmap,
                dedup: HashMap::with_capacity_and_hasher(256, FastBuild::default()),
            },
            other => {
                return Err(TaibaiError::Internal(format!(
                    "hot store: column type {} is not supported as a cluster-key column",
                    other.data_type().display_name()
                )));
            }
        })
    }

    #[inline]
    pub(super) fn key_at(&mut self, i: usize, pool: &mut StringPool) -> ValueKey {
        match self {
            Self::Bool { data, bm } => key_or_null(bm, i, || ValueKey::Bool(data[i] != 0)),
            Self::Int8 { data, bm } => key_or_null(bm, i, || ValueKey::Int(data[i] as i64)),
            Self::Int16 { data, bm } => key_or_null(bm, i, || ValueKey::Int(data[i] as i64)),
            Self::Int32 { data, bm } => key_or_null(bm, i, || ValueKey::Int(data[i] as i64)),
            Self::Int64 { data, bm } => key_or_null(bm, i, || ValueKey::Int(data[i])),
            Self::UInt8 { data, bm } => key_or_null(bm, i, || ValueKey::Int(data[i] as i64)),
            Self::UInt16 { data, bm } => key_or_null(bm, i, || ValueKey::Int(data[i] as i64)),
            Self::UInt32 { data, bm } => key_or_null(bm, i, || ValueKey::Int(data[i] as i64)),
            Self::UInt64 { data, bm } => key_or_null(bm, i, || ValueKey::UInt(data[i])),
            Self::Float32 { data, bm } => key_or_null(bm, i, || {
                ValueKey::Float(crate::types::scalar::float_key_bits(data[i] as f64))
            }),
            Self::Float64 { data, bm } => key_or_null(bm, i, || {
                ValueKey::Float(crate::types::scalar::float_key_bits(data[i]))
            }),
            Self::Decimal { data, bm } => key_or_null(bm, i, || ValueKey::Int(data[i])),
            Self::Date { data, bm } => key_or_null(bm, i, || ValueKey::Int(data[i] as i64)),
            Self::Time { data, bm } => key_or_null(bm, i, || ValueKey::Int(data[i])),
            Self::Timestamp { data, bm } => key_or_null(bm, i, || ValueKey::Int(data[i])),
            Self::Interval { data, bm } => key_or_null(bm, i, || ValueKey::Int(data[i])),
            Self::VarChar { data, bm, dedup } => {
                if bm.is_null(i) {
                    return ValueKey::Null;
                }
                let s: &'a str = &data[i];
                if let Some(key) = dedup.get(s) {
                    return *key;
                }
                let key = ValueKey::Str(pool.intern(s));
                dedup.insert(s, key);
                key
            }
        }
    }
}

/// Shared null-check for the fixed-width `key_at` arms: every one of them
/// reads `ValueKey::Null` for a null row and a type-specific key otherwise.
#[inline]
pub(super) fn key_or_null(bm: &NullBitmap, i: usize, value: impl FnOnce() -> ValueKey) -> ValueKey {
    if bm.is_null(i) {
        ValueKey::Null
    } else {
        value()
    }
}

// ---------------------------------------------------------------------------
// Batch grouping.
// ---------------------------------------------------------------------------

/// Intern-id sentinel marking a NULL key row in the dense fast path.
pub(super) const NULL_RUN_ID: u32 = u32::MAX;

/// Groups one batch's rows into a chunk's [`GroupIndex`], slice by slice
/// (chunk boundaries split a batch). The strategy is picked once per batch
/// from the key/partition column shapes, and all scratch buffers live here
/// so they are reused across slices instead of reallocated per chunk.
pub(super) enum BatchGrouper<'a> {
    /// VarChar key column without partition grouping: intern ids make per-row
    /// work one pool probe plus a flat pair push, and one sort lays the slice
    /// out as per-key runs — the chunk index sees one entry + one contiguous
    /// extend per distinct symbol, with no per-row hash-table probe.
    DenseIds {
        data: &'a Utf8Buffer,
        bm: &'a NullBitmap,
        pending: Vec<(u32, u32)>,
    },
    /// Every other shape: per-row key extraction into batch-local scratch
    /// maps, then one contiguous extend per distinct key. The per-key vectors
    /// are cleared (buffers kept) between slices — group shapes are stable
    /// across batches of a table, so the capacity survives.
    Keys {
        key: KeySource<'a>,
        partition: Option<KeySource<'a>>,
        key_scratch: HashMap<ValueKey, Vec<u32>, FastBuild>,
        pkey_scratch: HashMap<(ValueKey, ValueKey), Vec<u32>, FastBuild>,
    },
}

impl<'a> BatchGrouper<'a> {
    /// Validate that the key (and partition) columns are groupable and pick
    /// the strategy for this batch. `rows` reserves scratch up front.
    pub(super) fn new(
        key_col: &'a ColumnArray,
        partition_col: Option<&'a ColumnArray>,
        rows: usize,
    ) -> Result<Self> {
        if partition_col.is_none() {
            if let ColumnArray::VarChar { data, bitmap } = key_col {
                return Ok(Self::DenseIds {
                    data,
                    bm: bitmap,
                    pending: Vec::with_capacity(rows),
                });
            }
        }
        Ok(Self::Keys {
            key: KeySource::new(key_col)?,
            partition: match partition_col {
                Some(col) => Some(KeySource::new(col)?),
                None => None,
            },
            key_scratch: HashMap::with_capacity_and_hasher(256, FastBuild::default()),
            pkey_scratch: HashMap::with_capacity_and_hasher(256, FastBuild::default()),
        })
    }

    /// Group rows `[start, end)` of the batch into `index` at local offsets
    /// `base + (i - start)`. Offsets within one key's postings stay ascending
    /// by construction.
    pub(super) fn group(
        &mut self,
        pool: &mut StringPool,
        index: &mut GroupIndex,
        start: usize,
        end: usize,
        base: u32,
    ) {
        match self {
            Self::DenseIds { data, bm, pending } => {
                pending.clear();
                for i in start..end {
                    let offset = base + (i - start) as u32;
                    if bm.is_null(i) {
                        pending.push((NULL_RUN_ID, offset));
                    } else {
                        pending.push((pool.intern(&data[i]), offset));
                    }
                }
                // Sort by (id, offset): per-key runs become contiguous with
                // ascending offsets, so each distinct key extends its
                // postings exactly once. Symbol-grouped ingestion is nearly
                // pre-sorted, which the unstable sort detects.
                pending.sort_unstable();
                let mut i = 0usize;
                while i < pending.len() {
                    let id = pending[i].0;
                    let mut j = i + 1;
                    while j < pending.len() && pending[j].0 == id {
                        j += 1;
                    }
                    let key = if id == NULL_RUN_ID {
                        ValueKey::Null
                    } else {
                        ValueKey::Str(id)
                    };
                    index
                        .key
                        .entry(key)
                        .or_default()
                        .extend(pending[i..j].iter().map(|p| p.1));
                    i = j;
                }
            }
            Self::Keys {
                key,
                partition,
                key_scratch,
                pkey_scratch,
            } => {
                // Clear the per-key vectors but keep their buffers: reusing
                // them avoids ~one Vec allocation per distinct key per batch.
                if key_scratch.len() > 8192 {
                    key_scratch.clear();
                } else {
                    for offsets in key_scratch.values_mut() {
                        offsets.clear();
                    }
                }
                match partition.as_mut() {
                    Some(psrc) => {
                        if pkey_scratch.len() > 8192 {
                            pkey_scratch.clear();
                        } else {
                            for offsets in pkey_scratch.values_mut() {
                                offsets.clear();
                            }
                        }
                        for i in start..end {
                            let k = key.key_at(i, pool);
                            let pk = psrc.key_at(i, pool);
                            let offset = base + (i - start) as u32;
                            key_scratch.entry(k).or_default().push(offset);
                            pkey_scratch.entry((k, pk)).or_default().push(offset);
                        }
                    }
                    None => {
                        for i in start..end {
                            let k = key.key_at(i, pool);
                            key_scratch
                                .entry(k)
                                .or_default()
                                .push(base + (i - start) as u32);
                        }
                    }
                }
                for (key, offsets) in key_scratch.iter() {
                    index
                        .key
                        .entry(*key)
                        .or_default()
                        .extend_from_slice(offsets);
                }
                if let Some(kp) = index.key_partition.as_mut() {
                    for ((key, pkey), offsets) in pkey_scratch.iter() {
                        kp.entry((*key, *pkey))
                            .or_default()
                            .extend_from_slice(offsets);
                    }
                }
            }
        }
    }
}
