use std::collections::HashMap;
use std::path::PathBuf;

use crate::common::error::{MonaxError, Result};
use crate::protocol::types::DataType;
use crate::storage::segment::SegmentReader;
use crate::types::batch::{ColumnDef, RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

use super::engine::{Engine, ExecResult};
use super::helpers::discover_segments;
use crate::sql::parser::ast::{FromClause, SelectItem, Statement};

pub fn exec_select(engine: &Engine, db: &str, stmt: &Statement) -> Result<ExecResult> {
    let from = match stmt {
        Statement::Select { from, .. } => from,
        _ => return Err(MonaxError::Internal("exec_select called with non-SELECT".into())),
    };

    match from {
        None => exec_literal_select(stmt),
        Some(from_clause) => exec_table_select(engine, db, stmt, from_clause),
    }
}

fn exec_literal_select(stmt: &Statement) -> Result<ExecResult> {
    let select_list = match stmt {
        Statement::Select { select_list, .. } => select_list,
        _ => unreachable!(),
    };
    let schema = Schema::new(vec![ColumnDef::new("1", DataType::Int64)]);
    let mut batch = RecordBatch::new(schema.clone());
    match &select_list[0] {
        SelectItem::Expr(expr, _) => {
            let val = super::helpers::eval_expr_to_scalar(expr, &DataType::Int64)?;
            batch.column_mut(0).unwrap().push(&val)?;
        }
        SelectItem::Star => {
            batch.column_mut(0).unwrap().push(&ScalarValue::Int64(Some(1)))?;
        }
    }
    Ok(ExecResult::Query { schema, batches: vec![batch] })
}

fn exec_table_select(
    engine: &Engine,
    db: &str,
    stmt: &Statement,
    from_clause: &FromClause,
) -> Result<ExecResult> {
    let table_names = extract_table_names(from_clause);

    let mut table_schemas = HashMap::new();
    {
        let catalogs = engine.catalogs().read().unwrap();
        let catalog = catalogs.get(db)
            .ok_or_else(|| MonaxError::Sql(format!("database '{}' does not exist", db)))?;

        for table_name in &table_names {
            let meta = catalog.get_table(table_name)
                .ok_or_else(|| MonaxError::Sql(format!("table '{}.{}' does not exist", db, table_name)))?
                .clone();
            table_schemas.insert(table_name.clone(), Schema::new(meta.columns.clone()));
        }
    }

    let plan = crate::sql::planner::plan_select(stmt, &table_schemas)?;
    let plan = crate::sql::optimizer::optimize(plan);

    let mut table_batches: HashMap<String, Vec<RecordBatch>> = HashMap::new();
    for table_name in &table_names {
        let table_dir = engine.data_dir().join(db).join(table_name);
        let mut batches = Vec::new();
        if table_dir.exists() {
            let segment_files = discover_segments(&table_dir)?;
            for seg_path in segment_files {
                let reader = SegmentReader::open(&seg_path)?;
                batches.extend(reader.scan(4096)?);
            }
        }
        table_batches.insert(table_name.clone(), batches);
    }

    let mut pipeline = super::operators::build_pipeline(plan, &table_batches)?;
    let mut result_batches = Vec::new();
    let mut result_schema = None;
    while let Some(batch) = pipeline.next_batch()? {
        if result_schema.is_none() {
            result_schema = Some(batch.schema.clone());
        }
        result_batches.push(batch);
    }

    let schema = result_schema.unwrap_or_else(|| Schema::empty());
    Ok(ExecResult::Query { schema, batches: result_batches })
}

fn extract_table_names(from: &FromClause) -> Vec<String> {
    match from {
        FromClause::Table(t) => vec![t.name.clone()],
        FromClause::Join(join_expr) => {
            let mut names = Vec::new();
            collect_table_names_from_join(join_expr, &mut names);
            names
        }
    }
}

fn collect_table_names_from_join(join_expr: &crate::sql::parser::ast::JoinExpr, names: &mut Vec<String>) {
    match &*join_expr.left {
        FromClause::Table(t) => names.push(t.name.clone()),
        FromClause::Join(inner) => collect_table_names_from_join(inner, names),
    }
    for jc in &join_expr.joins {
        names.push(jc.right.name.clone());
    }
}
