pub mod ddl;
pub mod decorrelate;
pub mod dml_delete;
pub mod dml_insert;
pub mod dml_staging;
pub mod dml_update;
pub mod engine;
pub mod explain;
pub mod expr_vec;
pub mod functions;
pub mod helpers;
pub mod join_hashtable;
pub mod operators;
mod performance;
pub mod physical;
pub mod prepared;
pub mod query;
pub mod query_registry;
pub mod read_only_gate;
pub mod scan;
pub mod spill;
pub mod subquery_runtime;
pub mod ttl_purge;
pub mod txn_dml;
pub mod wal_helper;

pub use engine::{AutoCompactConfig, Engine, ExecResult};

#[cfg(test)]
#[path = "expr_vec_test.rs"]
mod expr_vec_tests;
