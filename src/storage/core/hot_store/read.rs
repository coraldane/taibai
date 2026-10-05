//! Point lookups, filtered reads and count queries.

use super::*;

impl HotStore {
    /// Gather all rows whose index-column value equals `key_value`, optionally
    /// projected to a subset of columns. Returns `None` only when the value has
    /// never been seen (caller may treat that as an empty result).
    ///
    /// `min_offset` excludes rows whose append offset is below it. The scan
    /// layer derives `min_offset` as a watermark (= total appended rows minus
    /// rows still resident in live memtables) so that rows already persisted to
    /// segments are not gathered here (the segment scan covers those), avoiding
    /// double-counting. This relies on the single-active-partition invariant:
    /// persisted rows form a contiguous append-prefix of the store.
    pub fn lookup(
        &self,
        key_value: &ScalarValue,
        partition_value: Option<&ScalarValue>,
        projection: Option<&[usize]>,
        min_offset: u64,
        reverse: bool,
    ) -> Option<RecordBatch> {
        self.lookup_filtered(
            key_value,
            partition_value,
            projection,
            min_offset,
            reverse,
            None,
            &[],
            &[],
        )
    }

    /// Index lookup with predicate/limit pushdown. Candidate row offsets are
    /// filtered against resident typed columns before projected values are
    /// gathered, so a selective time range does not copy every table column.
    #[allow(clippy::too_many_arguments)]
    pub fn lookup_filtered(
        &self,
        key_value: &ScalarValue,
        partition_value: Option<&ScalarValue>,
        projection: Option<&[usize]>,
        min_offset: u64,
        reverse: bool,
        row_limit: Option<usize>,
        eq_predicates: &[(usize, ScalarValue)],
        range_predicates: &[RangeClause],
    ) -> Option<RecordBatch> {
        let (key, partition_key) = self.resolve_query_keys(key_value, partition_value)?;
        // Hold the snapshot for the whole walk so a query never mixes chunks
        // from before and after a prefix reclamation.
        let st = self.state.read().unwrap();
        let open_guard = st.open.read().unwrap();
        let front = st.front;

        // Visit chunks in append order (ascending ids), or newest-first for
        // reverse lookups, so LIMIT can terminate early.
        let mut visit: Vec<&dyn ChunkView> = Vec::with_capacity(st.sealed.len() + 1);
        let open_visit_pos = if reverse {
            visit.push(&*open_guard);
            visit.extend(st.sealed.iter().rev().map(|c| c.as_ref() as &dyn ChunkView));
            0usize
        } else {
            visit.extend(st.sealed.iter().map(|c| c.as_ref() as &dyn ChunkView));
            let pos = visit.len();
            visit.push(&*open_guard);
            pos
        };

        let proj: Vec<usize> = match projection {
            Some(p) => p.to_vec(),
            None => (0..self.schema.num_columns()).collect(),
        };
        let target = row_limit.unwrap_or(usize::MAX);

        // Large gathers (analytics over one key's full history) parallelize
        // across chunks on the rayon pool; point lookups and limited reads
        // stay on the serial walk with its early exit.
        if row_limit.is_none()
            && visit.len() > 1
            && estimate_candidates(&visit, &key, partition_key.as_ref(), min_offset, front)
                >= PARALLEL_GATHER_MIN_ROWS
        {
            use rayon::prelude::*;
            // (probed, skipped below watermark, gathered part)
            type Part = (bool, bool, Option<(Vec<ColumnDef>, Vec<ColumnArray>)>);
            let parts: Vec<Part> = visit
                .par_iter()
                .map(|&chunk| {
                    let base = chunk.base() - front;
                    if base + chunk.num_rows() <= min_offset {
                        return (false, true, None);
                    }
                    let postings = match partition_key {
                        Some(pk) => chunk.key_partition_postings(&key, &pk),
                        None => chunk.key_postings(&key),
                    };
                    let Some(postings) = postings else {
                        return (false, false, None);
                    };
                    let cut = watermark_cut(min_offset, base);
                    let slice = resident_postings(postings, cut);
                    if slice.is_empty() {
                        return (true, false, None);
                    }
                    let covered_partition = partition_key.and(self.partition_column_index);
                    let preds = compile_predicates(
                        chunk.columns(),
                        eq_predicates,
                        range_predicates,
                        self.key_column_index,
                        covered_partition,
                    );
                    let mut rows = Vec::with_capacity(slice.len());
                    if reverse {
                        for &row in slice.iter().rev() {
                            if preds.iter().all(|p| p.matches(row as usize)) {
                                rows.push(row);
                            }
                        }
                    } else {
                        for &row in slice {
                            if preds.iter().all(|p| p.matches(row as usize)) {
                                rows.push(row);
                            }
                        }
                    }
                    if rows.is_empty() {
                        return (true, false, None);
                    }
                    let columns: Vec<ColumnArray> = proj
                        .iter()
                        .map(|&ci| chunk.columns()[ci].take(&rows))
                        .collect();
                    let defs = proj
                        .iter()
                        .map(|&i| self.schema.columns[i].clone())
                        .collect();
                    (true, false, Some((defs, columns)))
                })
                .collect();
            let mut gathered: Option<(Vec<ColumnDef>, Vec<ColumnArray>)> = None;
            let mut seen = false;
            let mut skipped_by_watermark = false;
            for (probed, was_skipped, part) in parts {
                seen |= probed;
                skipped_by_watermark |= was_skipped;
                if let Some((defs, columns)) = part {
                    accumulate_part(&mut gathered, defs, columns);
                }
            }
            if !seen && skipped_by_watermark {
                // open_guard is held for the whole walk — consult the halves
                // directly instead of re-acquiring the read lock.
                if !(Self::key_in_sealed(&st, &key, partition_key.as_ref())
                    || Self::key_in_open(&open_guard, &key, partition_key.as_ref()))
                {
                    return None;
                }
            } else if !seen {
                return None;
            }
            let Some((defs, columns)) = gathered else {
                let col_defs: Vec<ColumnDef> = proj
                    .iter()
                    .map(|&i| self.schema.columns[i].clone())
                    .collect();
                return Some(RecordBatch::new(Schema::new(col_defs)));
            };
            return RecordBatch::try_new(Schema::new(defs), columns).ok();
        }

        let mut found: usize = 0;
        let mut seen = false;
        let mut skipped_by_watermark = false;
        let mut rows_buf: Vec<u32> = Vec::new();
        // Per contributing chunk: the row selection plus, for each projected
        // column, either the shared sealed source (`View`) or an already
        // gathered owned column for the open chunk (`Owned`).
        let mut chunk_selections: Vec<(Vec<u32>, Vec<SelectionSource>)> = Vec::new();

        for (visit_idx, chunk) in visit.iter().enumerate() {
            let chunk: &dyn ChunkView = *chunk;
            let is_open = visit_idx == open_visit_pos;
            let base = chunk.base() - front;
            let chunk_end = base + chunk.num_rows();
            if chunk_end <= min_offset {
                // Fully below the watermark. Coverage is an append-order
                // prefix, so a newest-first (reverse) walk can stop here.
                skipped_by_watermark = true;
                if reverse {
                    break;
                }
                continue;
            }
            if found >= target {
                break;
            }
            let postings = match partition_key {
                Some(pk) => chunk.key_partition_postings(&key, &pk),
                None => chunk.key_postings(&key),
            };
            let Some(postings) = postings else {
                continue;
            };
            seen = true;
            let cut = watermark_cut(min_offset, base);
            let slice = resident_postings(postings, cut);
            if slice.is_empty() {
                continue;
            }
            let covered_partition = partition_key.and(self.partition_column_index);
            let preds = compile_predicates(
                chunk.columns(),
                eq_predicates,
                range_predicates,
                self.key_column_index,
                covered_partition,
            );
            rows_buf.clear();
            let remaining = target - found;
            if reverse {
                for &row in slice.iter().rev() {
                    if preds.iter().all(|p| p.matches(row as usize)) {
                        rows_buf.push(row);
                        if rows_buf.len() >= remaining {
                            break;
                        }
                    }
                }
            } else {
                for &row in slice {
                    if preds.iter().all(|p| p.matches(row as usize)) {
                        rows_buf.push(row);
                        if rows_buf.len() >= remaining {
                            break;
                        }
                    }
                }
            }
            if rows_buf.is_empty() {
                continue;
            }
            found += rows_buf.len();
            let rows = std::mem::take(&mut rows_buf);
            let sources: Vec<SelectionSource> = proj
                .iter()
                .map(|&ci| {
                    if is_open {
                        // Open-chunk columns mutate in place; views cannot
                        // share them, so take only the selected rows.
                        SelectionSource::Owned(chunk.columns()[ci].take(&rows))
                    } else {
                        SelectionSource::Shared(chunk.columns()[ci].clone())
                    }
                })
                .collect();
            chunk_selections.push((rows, sources));
        }
        drop(open_guard);

        // Small results containing sealed-chunk rows become zero-copy views;
        // everything else keeps the fused memcpy gather, which beats
        // indirection for consumers.
        let has_shared_part = chunk_selections.iter().any(|(_, sources)| {
            sources
                .iter()
                .any(|s| matches!(s, SelectionSource::Shared(_)))
        });
        if seen && has_shared_part && found <= SELECTED_MAX_ROWS {
            let defs: Vec<ColumnDef> = proj
                .iter()
                .map(|&i| self.schema.columns[i].clone())
                .collect();
            let columns: Vec<ColumnArray> = proj
                .iter()
                .enumerate()
                .map(|(col_pos, &ci)| {
                    let data_type = self.schema.columns[ci].data_type;
                    let parts: Vec<(std::sync::Arc<ColumnArray>, std::sync::Arc<[u32]>)> =
                        chunk_selections
                            .iter()
                            .map(|(rows, sources)| match &sources[col_pos] {
                                SelectionSource::Shared(source) => {
                                    (source.clone(), std::sync::Arc::from(rows.clone()))
                                }
                                SelectionSource::Owned(owned) => {
                                    let identity: Vec<u32> = (0..owned.len() as u32).collect();
                                    (
                                        std::sync::Arc::new(owned.clone()),
                                        std::sync::Arc::from(identity),
                                    )
                                }
                            })
                            .collect();
                    ColumnArray::selected(std::sync::Arc::from(parts), data_type, found)
                })
                .collect();
            return RecordBatch::try_new(Schema::new(defs), columns).ok();
        }
        if seen && !has_shared_part && found <= SELECTED_MAX_ROWS && chunk_selections.len() == 1 {
            // Single open-chunk hit: the taken columns ARE the result; move
            // them without a copy.
            let (_, sources) = chunk_selections.remove(0);
            let defs: Vec<ColumnDef> = proj
                .iter()
                .map(|&i| self.schema.columns[i].clone())
                .collect();
            let columns: Vec<ColumnArray> = sources
                .into_iter()
                .map(|s| match s {
                    SelectionSource::Owned(col) => col,
                    SelectionSource::Shared(_) => unreachable!("checked above"),
                })
                .collect();
            return RecordBatch::try_new(Schema::new(defs), columns).ok();
        }
        let mut gathered: Option<(Vec<ColumnDef>, Vec<ColumnArray>)> = None;
        for (rows, sources) in chunk_selections {
            if gathered.is_none() {
                let defs = proj
                    .iter()
                    .map(|&i| self.schema.columns[i].clone())
                    .collect();
                let columns = proj
                    .iter()
                    .map(|&i| ColumnArray::empty(self.schema.columns[i].data_type))
                    .collect();
                gathered = Some((defs, columns));
            }
            let mut sources = sources.into_iter();
            if let Some((_, acc)) = gathered.as_mut() {
                for col in acc.iter_mut() {
                    match sources.next() {
                        Some(SelectionSource::Shared(source)) => {
                            // A gather failure abandons the whole answer:
                            // partial rows would under-count the group.
                            source.gather_into(&rows, col).ok()?;
                        }
                        Some(SelectionSource::Owned(owned)) => {
                            // Rows were already gathered from the open chunk;
                            // append them (buffer move for variable-width).
                            col.append_owned(owned).ok();
                        }
                        None => {}
                    }
                }
            }
        }

        if !seen {
            if skipped_by_watermark {
                // The key was never probed above the watermark. Keep the
                // None-vs-empty contract: a key that exists only under the
                // watermark yields Some(empty), never None. The open read
                // guard was dropped before the gather above, so this
                // re-acquisition is not recursive.
                if !self.key_exists_anywhere(&st, &key, partition_key.as_ref()) {
                    return None;
                }
            } else {
                return None;
            }
        }
        let Some((defs, columns)) = gathered else {
            let col_defs: Vec<ColumnDef> = proj
                .iter()
                .map(|&i| self.schema.columns[i].clone())
                .collect();
            return Some(RecordBatch::new(Schema::new(col_defs)));
        };
        RecordBatch::try_new(Schema::new(defs), columns).ok()
    }

    /// Existence probe used to distinguish "never seen" (`None`) from "seen
    /// but everything filtered" (`Some(empty)`) when the watermark skipped
    /// chunks without probing their postings.
    pub(super) fn key_exists_anywhere(
        &self,
        st: &StoreState,
        key: &ValueKey,
        partition_key: Option<&ValueKey>,
    ) -> bool {
        if Self::key_in_sealed(st, key, partition_key) {
            return true;
        }
        let open = st.open.read().unwrap();
        Self::key_in_open(&open, key, partition_key)
    }

    pub(super) fn key_in_sealed(
        st: &StoreState,
        key: &ValueKey,
        partition_key: Option<&ValueKey>,
    ) -> bool {
        st.sealed.iter().any(|c| match partition_key {
            Some(pk) => c.key_partition_postings(key, pk).is_some(),
            None => c.key_postings(key).is_some(),
        })
    }

    pub(super) fn key_in_open(
        open: &std::sync::RwLockReadGuard<'_, OpenChunk>,
        key: &ValueKey,
        partition_key: Option<&ValueKey>,
    ) -> bool {
        match partition_key {
            Some(pk) => open.key_partition_postings(key, pk).is_some(),
            None => open.key_postings(key).is_some(),
        }
    }

    /// Count rows whose index-column value equals `key_value` without
    /// materialising the batch.
    pub fn count_by_key_partition_from(
        &self,
        key_value: &ScalarValue,
        partition_value: Option<&ScalarValue>,
        min_offset: u64,
    ) -> Option<u64> {
        let (key, partition_key) = self.resolve_query_keys(key_value, partition_value)?;
        let st = self.state.read().unwrap();
        let open = st.open.read().unwrap();

        if min_offset == 0 {
            // Resident fast path: every sealed row is visible, so the
            // seal-time summary answers for all chunks in one probe.
            let sealed_total = match partition_key {
                None => st.summary.key_rows.get(&key).copied().unwrap_or(0),
                Some(pk) => st
                    .summary
                    .key_partition_rows
                    .get(&(key, pk))
                    .copied()
                    .unwrap_or(0),
            };
            let mut total = sealed_total;
            let mut seen = sealed_total > 0;
            let open_postings = match partition_key {
                Some(pk) => open.key_partition_postings(&key, &pk),
                None => open.key_postings(&key),
            };
            if let Some(offsets) = open_postings {
                seen = true;
                total += offsets.len() as u64;
            }
            return seen.then_some(total);
        }

        let mut total = 0u64;
        let (probed, skipped) = walk_resident_postings(
            &st,
            &open,
            &key,
            partition_key.as_ref(),
            min_offset,
            |slice, _| total += slice.len() as u64,
        );
        if !probed && skipped {
            // The `open` read guard is held — avoid re-acquiring it.
            if !(Self::key_in_sealed(&st, &key, partition_key.as_ref())
                || Self::key_in_open(&open, &key, partition_key.as_ref()))
            {
                return None;
            }
        }
        (probed || skipped).then_some(total)
    }

    /// Count matching index candidates without gathering a RecordBatch.
    pub fn count_filtered(
        &self,
        key_value: &ScalarValue,
        partition_value: Option<&ScalarValue>,
        min_offset: u64,
        eq_predicates: &[(usize, ScalarValue)],
        range_predicates: &[RangeClause],
    ) -> Option<u64> {
        let (key, partition_key) = self.resolve_query_keys(key_value, partition_value)?;
        let st = self.state.read().unwrap();
        let open = st.open.read().unwrap();
        let covered_partition = partition_key.and(self.partition_column_index);
        let mut total = 0u64;
        let (probed, skipped) = walk_resident_postings(
            &st,
            &open,
            &key,
            partition_key.as_ref(),
            min_offset,
            |slice, chunk| {
                let preds = compile_predicates(
                    chunk.columns(),
                    eq_predicates,
                    range_predicates,
                    self.key_column_index,
                    covered_partition,
                );
                total += slice
                    .iter()
                    .filter(|&&row| preds.iter().all(|p| p.matches(row as usize)))
                    .count() as u64;
            },
        );
        if !probed && skipped {
            // The `open` read guard is held — avoid re-acquiring it.
            if !(Self::key_in_sealed(&st, &key, partition_key.as_ref())
                || Self::key_in_open(&open, &key, partition_key.as_ref()))
            {
                return None;
            }
        }
        (probed || skipped).then_some(total)
    }

    /// Resolve the query key (and optional partition key) through the intern
    /// pool. `None` means the value has never been appended.
    pub(super) fn resolve_query_keys(
        &self,
        key_value: &ScalarValue,
        partition_value: Option<&ScalarValue>,
    ) -> Option<(ValueKey, Option<ValueKey>)> {
        let pool = self.string_pool.read().unwrap();
        let key = scalar_to_key(key_value, &pool).ok()??;
        let partition_key = match partition_value {
            Some(value) => Some(scalar_to_key(value, &pool).ok()??),
            None => None,
        };
        Some((key, partition_key))
    }
}

/// Visible posting entries across all chunks, used to size up a gather before
/// deciding between the serial and parallel walks. Early-outs at the parallel
/// threshold so point lookups pay for at most one extra chunk probe.
pub(super) fn estimate_candidates(
    visit: &[&dyn ChunkView],
    key: &ValueKey,
    partition_key: Option<&ValueKey>,
    min_offset: u64,
    front: u64,
) -> u64 {
    let mut total = 0u64;
    for &chunk in visit {
        let base = chunk.base() - front;
        if base + chunk.num_rows() <= min_offset {
            continue;
        }
        let postings = match partition_key {
            Some(pk) => chunk.key_partition_postings(key, pk),
            None => chunk.key_postings(key),
        };
        if let Some(postings) = postings {
            let cut = watermark_cut(min_offset, base);
            total += resident_postings(postings, cut).len() as u64;
            if total >= PARALLEL_GATHER_MIN_ROWS {
                break;
            }
        }
    }
    total
}

/// Fold one chunk's gathered columns into the output accumulator, preserving
/// visit order. `append_owned` moves variable-width buffers instead of
/// re-copying them row by row.
pub(super) fn accumulate_part(
    gathered: &mut Option<(Vec<ColumnDef>, Vec<ColumnArray>)>,
    defs: Vec<ColumnDef>,
    columns: Vec<ColumnArray>,
) {
    match gathered {
        Some((_, acc)) => {
            for (dst, src) in acc.iter_mut().zip(columns) {
                dst.append_owned(src).ok();
            }
        }
        slot @ None => {
            *slot = Some((defs, columns));
        }
    }
}

/// One chunk's contribution to a lookup result, per projected column: either
/// the sealed chunk's shared column (rows selected by index) or an owned
/// gather of the open chunk's selected rows.
pub(super) enum SelectionSource {
    Shared(std::sync::Arc<ColumnArray>),
    Owned(ColumnArray),
}
