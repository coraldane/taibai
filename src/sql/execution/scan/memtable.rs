use crate::storage::core::memtable::MemTableManager;
use crate::types::batch::RecordBatch;

use super::spec::ScanSpec;

pub fn scan_memtables(
    memtable_mgr: &MemTableManager,
    db: &str,
    table_name: &str,
    spec: &ScanSpec,
) -> Vec<RecordBatch> {
    let memtables = memtable_mgr.get_by_table(db, table_name);
    let mut batches = Vec::new();

    for mt in &memtables {
        if let Some(mt_batch) = mt.read() {
            if mt_batch.num_rows() == 0 {
                continue;
            }

            let mt_batch = if spec.has_predicates() {
                super::row_filter::apply_predicates(
                    &mt_batch,
                    &spec.eq_predicates,
                    &spec.range_predicates,
                    &spec.in_predicates,
                    spec.remaining_predicate.as_ref(),
                )
            } else {
                mt_batch.clone()
            };
            if mt_batch.num_rows() == 0 {
                continue;
            }

            if let Some(proj) = &spec.projection {
                batches.push(apply_projection(&mt_batch, proj));
            } else {
                batches.push(mt_batch);
            }
        }
    }

    batches
}

pub fn count_memtable_rows(
    memtable_mgr: &MemTableManager,
    db: &str,
    table_name: &str,
    spec: &ScanSpec,
) -> u64 {
    let memtables = memtable_mgr.get_by_table(db, table_name);
    let mut total: u64 = 0;

    for mt in &memtables {
        if let Some(mt_batch) = mt.read() {
            if mt_batch.num_rows() == 0 {
                continue;
            }

            if spec.eq_predicates.len() == 1
                && spec.range_predicates.is_empty()
                && spec.in_predicates.is_empty()
                && spec.remaining_predicate.is_none()
            {
                let (col_idx, value) = &spec.eq_predicates[0];
                if let Some(col) = mt_batch.column(*col_idx) {
                    let mask = col.eq_vec(value);
                    total += mask.iter().filter(|&&m| m).count() as u64;
                }
            } else if spec.has_predicates() {
                let filtered = super::row_filter::apply_predicates(
                    &mt_batch,
                    &spec.eq_predicates,
                    &spec.range_predicates,
                    &spec.in_predicates,
                    spec.remaining_predicate.as_ref(),
                );
                total += filtered.num_rows() as u64;
            } else {
                total += mt_batch.num_rows() as u64;
            }
        }
    }

    total
}

fn apply_projection(batch: &RecordBatch, projection: &[usize]) -> RecordBatch {
    let mut col_defs = Vec::with_capacity(projection.len());
    let mut columns = Vec::with_capacity(projection.len());
    for &idx in projection {
        if let Some(col) = batch.column(idx) {
            columns.push(col.clone());
            if let Some(col_def) = batch.schema.column(idx) {
                col_defs.push(col_def.clone());
            }
        }
    }
    let schema = crate::types::batch::Schema::new(col_defs);
    RecordBatch::try_new(schema, columns).unwrap_or_else(|_| batch.clone())
}
