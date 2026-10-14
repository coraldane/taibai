use super::*;

pub(crate) fn unique_constraints(
    table_meta: &crate::storage::catalog::schema::TableMeta,
    schema: &Schema,
) -> Result<Vec<(String, Vec<usize>)>> {
    let mut constraints = Vec::new();
    for index in &table_meta.unique_indexes {
        let mut indices = Vec::with_capacity(index.columns.len());
        for column in &index.columns {
            indices.push(schema.index_of(column).ok_or_else(|| {
                TaibaiError::Sql(format!("unique index column '{}' not found", column))
            })?);
        }
        constraints.push((index.name.clone(), indices));
    }
    Ok(constraints)
}

pub(super) fn validate_unique_constraints(
    engine: &Engine,
    db: &str,
    table: &str,
    batch: &RecordBatch,
    schema: &Schema,
    constraints: &[(String, Vec<usize>)],
) -> Result<()> {
    for (name, indices) in constraints {
        validate_unique_key_uniqueness(engine, db, table, schema, indices, name, Some(batch))?;
    }
    Ok(())
}

pub(super) fn validate_unique_constraints_in_batch(
    table: &str,
    batch: &RecordBatch,
    schema: &Schema,
    constraints: &[(String, Vec<usize>)],
) -> Result<()> {
    for (name, indices) in constraints {
        let mut keys = HashSet::new();
        for row_idx in 0..batch.num_rows() {
            let Some(key) = unique_key_for_row(batch, row_idx, indices, schema) else {
                continue;
            };
            // Probe first and move on success: hashing twice is cheaper than
            // cloning every key cell (a String copy for VarChar) per row.
            if keys.contains(&key) {
                return Err(duplicate_unique_key_error(table, name, &key));
            }
            keys.insert(key);
        }
    }
    Ok(())
}

pub fn validate_unique_key_uniqueness(
    engine: &Engine,
    db: &str,
    table: &str,
    schema: &Schema,
    indices: &[usize],
    index_name: &str,
    new_batch: Option<&RecordBatch>,
) -> Result<()> {
    let mut keys = HashSet::new();
    if let Some(batch) = new_batch {
        for row_idx in 0..batch.num_rows() {
            let Some(key) = unique_key_for_row(batch, row_idx, indices, schema) else {
                continue;
            };
            if keys.contains(&key) {
                return Err(duplicate_unique_key_error(table, index_name, &key));
            }
            keys.insert(key);
        }
    }
    if new_batch.is_some() {
        if let Some(columns) = engine
            .index_manager()
            .unique_index_columns(db, table, index_name)?
        {
            let index_columns = indices
                .iter()
                .map(|idx| schema.columns[*idx].name.clone())
                .collect::<Vec<_>>();
            if columns == index_columns {
                // Pass the set directly: the lookup is generic over the key
                // iteration, so the whole key set need not be re-cloned per
                // INSERT.
                match engine
                    .index_manager()
                    .unique_keys_exist(db, table, index_name, &keys)?
                {
                    Some(false) => return Ok(()),
                    Some(true) => {
                        for key in &keys {
                            if engine
                                .index_manager()
                                .unique_key_exists(db, table, index_name, key)?
                                .unwrap_or(false)
                            {
                                return Err(duplicate_unique_key_error(table, index_name, key));
                            }
                        }
                        return Ok(());
                    }
                    None => {}
                }
            }
        }
    }
    load_existing_unique_keys(engine, db, table, schema, indices, index_name, &keys)
}

/// Collect one entry per live row for the given key columns. Rows sharing a
/// duplicate key from an unvalidated write is preserved: REPLACE treats every location as a
/// replaceable conflict, so callers must see each one instead of failing on
/// pre-existing dirty data.
pub fn collect_unique_index_entries(
    engine: &Engine,
    db: &str,
    table: &str,
    indices: &[usize],
    reason: &str,
) -> Result<Vec<UniqueIndexEntry>> {
    let schema = engine
        .catalogs()
        .read()
        .unwrap()
        .get(db)
        .and_then(|catalog| catalog.get_table_shared(table))
        .map(|meta| crate::types::batch::Schema::new(meta.columns.clone()))
        .ok_or_else(|| TaibaiError::Sql(format!("table '{}.{}' does not exist", db, table)))?;
    collect_unique_index_entries_from(
        engine.segment_mgr(),
        &engine.memtable_manager().get_by_table(db, table),
        &engine.data_dir().join(db).join(table),
        table,
        &schema,
        indices,
        reason,
    )
}

pub(crate) fn collect_unique_index_entries_from(
    segment_mgr: &SegmentManager,
    memtables: &[std::sync::Arc<crate::storage::core::memtable::MemTable>],
    table_dir: &std::path::Path,
    table: &str,
    table_schema: &crate::types::batch::Schema,
    indices: &[usize],
    reason: &str,
) -> Result<Vec<UniqueIndexEntry>> {
    let mut entries = Vec::new();
    visit_unique_index_entries_from(
        segment_mgr,
        memtables,
        table_dir,
        table,
        table_schema,
        indices,
        reason,
        &mut |entry| {
            entries.push(entry);
            Ok(())
        },
    )?;
    Ok(entries)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn visit_unique_index_entries_from(
    segment_mgr: &SegmentManager,
    memtables: &[std::sync::Arc<crate::storage::core::memtable::MemTable>],
    table_dir: &std::path::Path,
    table: &str,
    table_schema: &crate::types::batch::Schema,
    indices: &[usize],
    reason: &str,
    visit: &mut dyn FnMut(UniqueIndexEntry) -> Result<()>,
) -> Result<()> {
    let started = std::time::Instant::now();
    let mut count = 0usize;
    if table_dir.exists() {
        let segments = SegmentManager::discover_segments(table_dir)?;
        count += visit_unique_index_entries_in_segments(
            segment_mgr,
            &segments,
            table_schema,
            indices,
            visit,
        )?;
    }
    count += visit_unique_index_entries_in_memtables(memtables, table_schema, indices, visit)?;
    tracing::warn!(
        table,
        key_columns = indices.len(),
        entries = count,
        elapsed_ms = started.elapsed().as_millis() as u64,
        reason,
        "unique index snapshot rebuild from full-table scan"
    );
    Ok(())
}

/// Stream unique-index entries from an explicit segment list. Callers decide
/// which segments are safe to read outside the table mutation lock (the
/// two-phase rebuild passes immutable segments only); the readers themselves
/// are stable snapshots because delete rewrites invalidate and reopen the
/// cache instead of mutating it in place.
pub(crate) fn visit_unique_index_entries_in_segments(
    segment_mgr: &SegmentManager,
    segments: &[std::path::PathBuf],
    table_schema: &crate::types::batch::Schema,
    indices: &[usize],
    visit: &mut dyn FnMut(UniqueIndexEntry) -> Result<()>,
) -> Result<usize> {
    let mut count = 0usize;
    for seg_path in segments {
        let reader = segment_mgr.open_reader_cached(seg_path)?;
        // Name-based read: catalog indices hit the wrong physical column
        // on segments whose footer predates a DROP/RENAME COLUMN.
        let batch_per_rg = |rg_idx: usize| -> Result<crate::types::batch::RecordBatch> {
            reader.read_row_group_columns(rg_idx, table_schema, indices)
        };
        for rg_idx in 0..reader.num_row_groups() {
            let batch = batch_per_rg(rg_idx)?;
            let rg_start = reader.row_group_start_row(rg_idx);
            let delete_bitmap = reader.delete_bitmap();
            let projected_indices: Vec<usize> = (0..batch.num_columns()).collect();
            for row_idx in 0..batch.num_rows() {
                if row_idx % 1024 == 0 {
                    crate::common::query_cancel::check()?;
                }
                let row_id = rg_start + row_idx as u64;
                if crate::storage::segment::delete_bitmap::is_deleted(delete_bitmap, row_id) {
                    continue;
                }
                let Some(key) =
                    unique_key_for_row(&batch, row_idx, &projected_indices, &batch.schema)
                else {
                    continue;
                };
                visit(UniqueIndexEntry {
                    key,
                    location: UniqueRowLocation {
                        segment_path: seg_path.to_string_lossy().to_string(),
                        row_id,
                    },
                })?;
                count += 1;
            }
        }
    }
    Ok(count)
}

/// Stream unique-index entries from the given live memtables. Memtable
/// batches keep their insert-time layout, so key columns resolve by name
/// against each batch's own schema.
pub(crate) fn visit_unique_index_entries_in_memtables(
    memtables: &[std::sync::Arc<crate::storage::core::memtable::MemTable>],
    table_schema: &crate::types::batch::Schema,
    indices: &[usize],
    visit: &mut dyn FnMut(UniqueIndexEntry) -> Result<()>,
) -> Result<usize> {
    let mut count = 0usize;
    for memtable in memtables {
        if let Some(batch) = memtable.read_snapshot_arc() {
            let local_indices: Vec<usize> = indices
                .iter()
                .filter_map(|&catalog_idx| {
                    table_schema
                        .columns
                        .get(catalog_idx)
                        .and_then(|column| batch.schema.index_of(&column.name))
                })
                .collect();
            if local_indices.len() != indices.len() {
                continue;
            }
            for row_idx in 0..batch.num_rows() {
                if row_idx % 1024 == 0 {
                    crate::common::query_cancel::check()?;
                }
                let Some(key) = unique_key_for_row(&batch, row_idx, &local_indices, &batch.schema)
                else {
                    continue;
                };
                visit(UniqueIndexEntry {
                    key,
                    location: UniqueRowLocation {
                        segment_path: String::new(),
                        row_id: row_idx as u64,
                    },
                })?;
                count += 1;
            }
        }
    }
    Ok(count)
}

/// Historical duplicate keys among themselves do not violate the statement
/// being validated: only a collision with a key the current batch is writing
/// is an error. This mirrors the snapshot-based path (`unique_key_exists`),
/// which never fails on unrelated pre-existing duplicates.
///
/// The fallback scans once into bounded sorted runs and publishes a file-
/// backed snapshot. Only incoming keys are retained in memory, so losing a
/// snapshot cannot turn a small INSERT into a table-sized key allocation.
fn load_existing_unique_keys(
    engine: &Engine,
    db: &str,
    table: &str,
    schema: &Schema,
    indices: &[usize],
    index_name: &str,
    new_keys: &HashSet<Vec<ScalarValue>>,
) -> Result<()> {
    let columns = indices
        .iter()
        .map(|&idx| schema.columns[idx].name.clone())
        .collect::<Vec<_>>();
    engine
        .index_manager()
        .persist_unique_index_stream(db, table, index_name, &columns, |emit| {
            visit_unique_index_entries_from(
                engine.segment_mgr(),
                &engine.memtable_manager().get_by_table(db, table),
                &engine.data_dir().join(db).join(table),
                table,
                schema,
                indices,
                "unique key validation fallback",
                &mut |entry| {
                    if new_keys.contains(&entry.key) {
                        return Err(duplicate_unique_key_error(table, index_name, &entry.key));
                    }
                    emit(entry)
                },
            )
        })
}

pub(super) fn unique_key_for_row(
    batch: &RecordBatch,
    row_idx: usize,
    indices: &[usize],
    schema: &Schema,
) -> Option<Vec<ScalarValue>> {
    let mut key = Vec::with_capacity(indices.len());
    for &col_idx in indices {
        let value = batch
            .scalar(row_idx, col_idx)
            .unwrap_or(ScalarValue::null_for_type(
                schema.columns[col_idx].data_type,
            ));
        if value.is_null() {
            return None;
        }
        key.push(value);
    }
    Some(key)
}

/// Bounded-memory uniqueness validation for row-rewriting statements
/// (UPDATE): the keys the statement INTRODUCES are materialized once (they
/// are already bounded by the statement's mutation budget); every other
/// live row is streamed past that set. Collisions purely among streamed
/// survivors are pre-existing state the statement did not create — the
/// same philosophy the batch validator documents — so the introduced sets
/// are only ever probed from the survivor side, never grown by it.
///
/// All batches are addressed through `projected_indices[p] = batch column
/// index of validation position p`; batches that already carry the
/// validation projection pass the identity.
/// Per index to validate: (`UniqueIndexMeta`, key positions inside the
/// validation projection, materialized introduced-key set).
type UniqueIndexProjection<'a> = (
    &'a crate::storage::catalog::schema::UniqueIndexMeta,
    Vec<usize>,
    std::collections::HashSet<Vec<ScalarValue>>,
);

pub struct IntroducedKeyValidator<'a> {
    table: &'a str,
    /// Per index to validate: the positions of its key columns inside the
    /// VALIDATION projection plus its materialized introduced-key set.
    indexes: Vec<UniqueIndexProjection<'a>>,
    budget: super::super::dml_staging::MutationBudget,
    shared_budget: Option<&'a mut super::super::dml_staging::MutationBudget>,
    charged: usize,
}

impl<'a> IntroducedKeyValidator<'a> {
    /// `validation_schema` describes the projection every batch is
    /// addressed in; indexes whose columns are missing from it (dropped
    /// between planning and validation) are skipped rather than panicking.
    pub fn new(
        table: &'a str,
        validation_schema: &Schema,
        indexes_to_validate: &'a [crate::storage::catalog::schema::UniqueIndexMeta],
    ) -> Self {
        let mut validators = Vec::with_capacity(indexes_to_validate.len());
        for index in indexes_to_validate {
            let positions: Option<Vec<usize>> = index
                .columns
                .iter()
                .map(|column| validation_schema.index_of(column))
                .collect();
            if let Some(positions) = positions {
                validators.push((index, positions, std::collections::HashSet::new()));
            }
        }
        Self {
            table,
            indexes: validators,
            budget: super::super::dml_staging::MutationBudget::new(),
            shared_budget: None,
            charged: 0,
        }
    }

    pub(crate) fn new_with_budget(
        table: &'a str,
        validation_schema: &Schema,
        indexes: &'a [crate::storage::catalog::schema::UniqueIndexMeta],
        budget: &'a mut super::super::dml_staging::MutationBudget,
    ) -> Self {
        let mut validator = Self::new(table, validation_schema, indexes);
        validator.shared_budget = Some(budget);
        validator
    }

    /// Register the keys of every row of a batch. Two introduced rows
    /// sharing a key is an immediate violation — both will be live after
    /// the statement.
    pub fn introduce_full_batch(
        &mut self,
        batch: &RecordBatch,
        projected_indices: &[usize],
    ) -> Result<()> {
        self.introduce_selected(batch, projected_indices, 0, batch.num_rows() as u32, &[])
    }

    /// Register the keys of the MATCHED positions of a batch (rows this
    /// statement rewrote in memory; their new keys go live).
    pub fn introduce_matched(
        &mut self,
        batch: &RecordBatch,
        projected_indices: &[usize],
        matched: &[u32],
    ) -> Result<()> {
        for &row_idx in matched {
            self.introduce_selected(batch, projected_indices, row_idx, row_idx + 1, &[])?;
        }
        Ok(())
    }

    fn introduce_selected(
        &mut self,
        batch: &RecordBatch,
        projected_indices: &[usize],
        from: u32,
        to: u32,
        _excluded: &[u32],
    ) -> Result<()> {
        for row_idx in from..to {
            for (index, positions, keys) in &mut self.indexes {
                let Some(key) = key_at(batch, projected_indices, positions, row_idx as usize)
                else {
                    continue;
                };
                let bytes = key.capacity() * std::mem::size_of::<ScalarValue>()
                    + key
                        .iter()
                        .map(super::super::spill::scalar_heap_bytes)
                        .sum::<usize>()
                    + 64;
                self.shared_budget
                    .as_deref_mut()
                    .unwrap_or(&mut self.budget)
                    .charge(bytes)?;
                self.charged = self.charged.saturating_add(bytes);
                if !keys.insert(key.clone()) {
                    return Err(duplicate_unique_key_error(self.table, &index.name, &key));
                }
            }
        }
        Ok(())
    }

    /// Stream a survivor batch past the introduced sets: any shared key is
    /// a violation. `excluded` row positions are skipped (rows this
    /// statement rewrote — their old keys retire with them).
    pub fn check_rows_excluding(
        &mut self,
        batch: &RecordBatch,
        projected_indices: &[usize],
        excluded: &[u32],
    ) -> Result<()> {
        let excluded: std::collections::HashSet<u32> = excluded.iter().copied().collect();
        for row_idx in 0..batch.num_rows() as u32 {
            if excluded.contains(&row_idx) {
                continue;
            }
            for (index, positions, keys) in &self.indexes {
                let Some(key) = key_at(batch, projected_indices, positions, row_idx as usize)
                else {
                    continue;
                };
                if keys.contains(&key) {
                    return Err(duplicate_unique_key_error(self.table, &index.name, &key));
                }
            }
        }
        Ok(())
    }

    /// Introduced keys of one index, for the location-index fast path: the
    /// segment-survivor check can probe the maintained snapshot with them
    /// instead of streaming every row group of the table's key columns.
    pub fn introduced_keys(&self, index_name: &str) -> Option<&HashSet<Vec<ScalarValue>>> {
        self.indexes
            .iter()
            .find(|(index, _, _)| index.name == index_name)
            .map(|(_, _, keys)| keys)
    }

    /// Collision error for the location-index fast path: an introduced key
    /// already exists at a segment location the statement does not
    /// tombstone — the same error the streaming check would report.
    pub fn probe_collision_error(
        &self,
        index_name: &str,
        key: &[ScalarValue],
    ) -> crate::common::error::TaibaiError {
        duplicate_unique_key_error(self.table, index_name, key)
    }

    /// Currently always Ok; the hook where any deferred finalization
    /// (counting, overflow accounting) would live.
    pub fn finish(self) -> Result<()> {
        Ok(())
    }
}

/// Assemble one index key for `row_idx`: `positions` names validation-space
/// columns, `projected_indices` maps validation space onto the batch.
fn key_at(
    batch: &RecordBatch,
    projected_indices: &[usize],
    positions: &[usize],
    row_idx: usize,
) -> Option<Vec<ScalarValue>> {
    let mut key = Vec::with_capacity(positions.len());
    for &position in positions {
        let batch_idx = projected_indices.get(position).copied()?;
        let column = batch.column(batch_idx)?;
        if column.bitmap().is_null(row_idx) {
            return None;
        }
        key.push(column.scalar(row_idx));
    }
    Some(key)
}

pub fn validate_unique_indexes_in_batches(
    table: &str,
    schema: &Schema,
    unique_indexes: &[crate::storage::catalog::schema::UniqueIndexMeta],
    batches: &[std::sync::Arc<RecordBatch>],
) -> Result<()> {
    for index in unique_indexes {
        let mut indices = Vec::with_capacity(index.columns.len());
        for column in &index.columns {
            indices.push(schema.index_of(column).ok_or_else(|| {
                TaibaiError::Sql(format!("unique index column '{}' not found", column))
            })?);
        }
        let mut keys = HashSet::new();
        for batch in batches {
            for row_idx in 0..batch.num_rows() {
                let Some(key) = unique_key_for_row(batch, row_idx, &indices, schema) else {
                    continue;
                };
                if keys.contains(&key) {
                    return Err(duplicate_unique_key_error(table, &index.name, &key));
                }
                keys.insert(key);
            }
        }
    }
    Ok(())
}

fn duplicate_unique_key_error(table: &str, index_name: &str, key: &[ScalarValue]) -> TaibaiError {
    let key_text = key
        .iter()
        .map(|v| v.to_text().unwrap_or_else(|| "NULL".to_string()))
        .collect::<Vec<_>>()
        .join(", ");
    TaibaiError::Sql(format!(
        "duplicate unique key '{}' for table '{}': ({})",
        index_name, table, key_text
    ))
}

pub(crate) fn primary_key_indices(schema: &Schema) -> Vec<usize> {
    schema
        .columns
        .iter()
        .enumerate()
        .filter(|(_, col)| col.primary_key)
        .map(|(idx, _)| idx)
        .collect()
}

pub(super) fn generated_auto_increment_key_guarantees_uniqueness(
    primary_key_indices: &[usize],
    is_missing_auto_inc: &[bool],
) -> bool {
    primary_key_indices
        .iter()
        .any(|&idx| is_missing_auto_inc.get(idx).copied().unwrap_or(false))
}

pub(super) fn validate_primary_key_values_present(
    batch: &RecordBatch,
    schema: &Schema,
    primary_key_indices: &[usize],
) -> Result<()> {
    for row_idx in 0..batch.num_rows() {
        primary_key_for_row(batch, row_idx, primary_key_indices, schema)?;
    }
    Ok(())
}

pub(crate) fn primary_keys_for_batch(
    batch: &RecordBatch,
    schema: &Schema,
    primary_key_indices: &[usize],
) -> Result<Vec<Vec<ScalarValue>>> {
    (0..batch.num_rows())
        .map(|row_idx| primary_key_for_row(batch, row_idx, primary_key_indices, schema))
        .collect()
}

pub fn validate_primary_key_uniqueness(
    engine: &Engine,
    db: &str,
    table: &str,
    batch: &RecordBatch,
    schema: &Schema,
    primary_key_indices: &[usize],
) -> Result<()> {
    let keys = primary_keys_for_batch(batch, schema, primary_key_indices)?;
    let mut unique = HashSet::with_capacity(keys.len());
    for key in &keys {
        if !unique.insert(key.clone()) {
            return Err(duplicate_primary_key_error(table, key));
        }
    }

    if let Some(exists) = engine.index_manager().unique_keys_exist(
        db,
        table,
        crate::storage::index::index_manager::PRIMARY_LOCATION_INDEX_NAME,
        &keys,
    )? {
        if exists {
            return Err(TaibaiError::Sql(format!(
                "duplicate primary key for table '{}'",
                table
            )));
        }
        return Ok(());
    }

    if let Some(exists) = engine
        .index_manager()
        .primary_keys_exist(db, table, &keys)?
    {
        if exists {
            return Err(TaibaiError::Sql(format!(
                "duplicate primary key for table '{}'",
                table
            )));
        }
        return Ok(());
    }

    load_existing_unique_keys(
        engine,
        db,
        table,
        schema,
        primary_key_indices,
        crate::storage::index::index_manager::PRIMARY_LOCATION_INDEX_NAME,
        &unique,
    )
}

pub(super) fn validate_primary_key_uniqueness_in_batch(
    table: &str,
    batch: &RecordBatch,
    schema: &Schema,
    primary_key_indices: &[usize],
) -> Result<()> {
    let mut keys = HashSet::new();
    for row_idx in 0..batch.num_rows() {
        let key = primary_key_for_row(batch, row_idx, primary_key_indices, schema)?;
        if keys.contains(&key) {
            return Err(duplicate_primary_key_error(table, &key));
        }
        keys.insert(key);
    }
    Ok(())
}

pub(super) fn advance_explicit_auto_increment_counters(
    engine: &Engine,
    db: &str,
    table: &str,
    batch: &RecordBatch,
    auto_inc_cols: &[usize],
    missing_auto_inc: &[usize],
) -> Result<()> {
    for &col_idx in auto_inc_cols {
        if missing_auto_inc.contains(&col_idx) {
            continue;
        }

        let mut max_value = None;
        for row_idx in 0..batch.num_rows() {
            let Some(value) = batch.scalar(row_idx, col_idx) else {
                continue;
            };
            let Some(int_value) = value.integer_i128() else {
                continue;
            };
            let Ok(unsigned) = u64::try_from(int_value) else {
                continue;
            };
            max_value = Some(max_value.map_or(unsigned, |current: u64| current.max(unsigned)));
        }

        if let Some(value) = max_value {
            engine
                .auto_increment_manager()
                .advance_past(db, table, value)?;
        }
    }
    Ok(())
}

fn primary_key_for_row(
    batch: &RecordBatch,
    row_idx: usize,
    primary_key_indices: &[usize],
    schema: &Schema,
) -> Result<Vec<ScalarValue>> {
    let mut key = Vec::with_capacity(primary_key_indices.len());
    for &col_idx in primary_key_indices {
        let value = batch
            .scalar(row_idx, col_idx)
            .unwrap_or(ScalarValue::null_for_type(
                schema.columns[col_idx].data_type,
            ));
        if value.is_null() {
            return Err(TaibaiError::Sql(format!(
                "primary key column '{}' cannot be NULL",
                schema.columns[col_idx].name
            )));
        }
        key.push(value);
    }
    Ok(key)
}

fn duplicate_primary_key_error(table: &str, key: &[ScalarValue]) -> TaibaiError {
    let key_text = key
        .iter()
        .map(|v| v.to_text().unwrap_or_else(|| "NULL".to_string()))
        .collect::<Vec<_>>()
        .join(", ");
    TaibaiError::Sql(format!(
        "duplicate primary key for table '{}': ({})",
        table, key_text
    ))
}

impl Drop for IntroducedKeyValidator<'_> {
    fn drop(&mut self) {
        if let Some(budget) = &mut self.shared_budget {
            budget.release(self.charged);
        }
    }
}
