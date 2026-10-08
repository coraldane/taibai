#[cfg(test)]
use std::cell::Cell;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::common::error::{Result, TaibaiError};
use crate::protocol::types::DataType;
use crate::storage::catalog::auth::{PermissionObjectType, PermissionType};
use crate::storage::catalog::auto_increment::AutoIncrementCounter;
use crate::storage::catalog::schema::TableDurability;
use crate::storage::index::index_manager::{UniqueIndexEntry, UniqueRowLocation};
use crate::storage::index::sort_key::SortKeyInserter;
use crate::storage::segment_manager::SegmentManager;
use crate::types::array::ColumnArray;
use crate::types::batch::{ColumnDefaultValue, RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

use super::engine::{Engine, ExecResult};
use super::helpers::{eval_expr_to_scalar_with_scale_timezone, sort_batch_by_key};
use crate::sql::parser::ast::Expr;

const CONSTRAINT_INSERT_LOCK_STRIPES: usize = 256;

/// Minimum rows per auto-commit INSERT statement for a DURABILITY = RELAXED
/// table to take the direct segment write path. Small statements keep the
/// strict memtable pipeline so mixed OLTP traffic retains immediate
/// visibility and per-statement durability.
const RELAXED_DIRECT_INSERT_MIN_ROWS: usize = 1024;

lazy_static::lazy_static! {
    static ref CONSTRAINT_INSERT_LOCKS: Vec<Mutex<()>> = (0..CONSTRAINT_INSERT_LOCK_STRIPES)
        .map(|_| Mutex::new(()))
        .collect();
    static ref FAST_INSERT_PLAN_CACHE: Mutex<HashMap<String, Arc<FastInsertPlan>>> = Mutex::new(HashMap::new());
}

pub(super) fn constraint_insert_lock(db: &str, table: &str) -> &'static Mutex<()> {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    db.hash(&mut hasher);
    table.hash(&mut hasher);
    &CONSTRAINT_INSERT_LOCKS[hasher.finish() as usize % CONSTRAINT_INSERT_LOCK_STRIPES]
}

#[cfg(test)]
thread_local! {
    static FORCE_REPLACE_INSERT_FAILURE: Cell<bool> = const { Cell::new(false) };
}

#[cfg(test)]
pub(crate) fn force_next_replace_insert_failure() {
    FORCE_REPLACE_INSERT_FAILURE.with(|force| force.set(true));
}

struct FastInsertPlan {
    schema_signature: String,
    col_indices: Vec<usize>,
    provided_by_col: Vec<Option<usize>>,
    missing_auto_inc: Vec<usize>,
}

struct MaterializedInsert {
    table_meta: std::sync::Arc<crate::storage::catalog::schema::TableMeta>,
    schema: Schema,
    batch: RecordBatch,
    missing_auto_inc: Vec<usize>,
}

mod constraints;
mod fast;
mod replace;
mod write;

pub use constraints::{
    collect_unique_index_entries, validate_primary_key_uniqueness,
    validate_unique_indexes_in_batches, validate_unique_key_uniqueness, IntroducedKeyValidator,
};
pub(crate) use constraints::{
    primary_key_indices, primary_keys_for_batch, unique_constraints,
    visit_unique_index_entries_from,
};
pub use fast::{set_fast_insert_disabled_for_tests, try_exec_fast_insert_sql};
pub(crate) use write::{
    append_unique_index_entries_after_insert, build_unique_index_entries_for_batch,
    primary_location_entries_for_batch, rebuild_table_location_indexes,
    rebuild_table_location_indexes_from, write_batch_by_partition,
};
pub use write::{exec_insert, exec_replace};

#[cfg(test)]
#[path = "dml_insert/lock_order_test.rs"]
mod lock_order_tests;
