use std::collections::HashMap;

use crate::storage::catalog::schema::TableMeta;
use crate::types::batch::Schema;

use crate::sql::execution::engine::Engine;
use crate::sql::execution::query::ResolvedTableRef;
use crate::sql::execution::scan::runtime::ScanRuntime;
use crate::sql::execution::subquery_runtime::SubqueryRegistry;

pub struct PhysicalContext<'a> {
    pub scan_runtime: ScanRuntime,
    pub table_refs: &'a [ResolvedTableRef],
    pub table_schemas: &'a HashMap<String, Schema>,
    pub table_metas: &'a HashMap<String, TableMeta>,
    pub is_count_star: bool,
    /// Snapshot registry for correlated subqueries, used by the profiled
    /// materialized pipeline (EXPLAIN ANALYZE) to evaluate markers that
    /// could not be decorrelated.
    pub subqueries: Option<SubqueryRegistry>,
}

impl<'a> PhysicalContext<'a> {
    pub fn new(
        engine: &'a Engine,
        table_refs: &'a [ResolvedTableRef],
        table_schemas: &'a HashMap<String, Schema>,
        table_metas: &'a HashMap<String, TableMeta>,
        is_count_star: bool,
    ) -> Self {
        Self {
            scan_runtime: ScanRuntime::from_engine(engine),
            table_refs,
            table_schemas,
            table_metas,
            is_count_star,
            subqueries: None,
        }
    }

    pub fn with_subqueries(mut self, subqueries: Option<SubqueryRegistry>) -> Self {
        self.subqueries = subqueries;
        self
    }

    pub fn table_ref(&self, table_name: &str) -> Option<&ResolvedTableRef> {
        self.table_refs
            .iter()
            .find(|table_ref| table_ref.key.eq_ignore_ascii_case(table_name))
    }

    pub fn schema(&self, table_name: &str) -> Option<&Schema> {
        self.table_schemas.get(table_name)
    }

    pub fn meta(&self, table_name: &str) -> Option<&TableMeta> {
        self.table_metas.get(table_name)
    }
}
