use std::collections::HashMap;

use crate::common::error::{MonaxError, Result};
use crate::protocol::types::DataType;
use crate::sql::parser::ast::Statement;
use crate::sql::planner::*;
use crate::storage::segment::ScanStats;
use crate::types::batch::{ColumnDef, RecordBatch, Schema};
use crate::types::scalar::ScalarValue;

use super::engine::{Engine, ExecResult};

pub fn exec_explain(
    engine: &Engine,
    db: &str,
    inner: &Statement,
    analyze: bool,
) -> Result<ExecResult> {
    let select_from = match inner {
        Statement::Select { .. } => inner,
        other => {
            return Err(MonaxError::Sql(format!(
                "EXPLAIN currently supports SELECT only, got: {:?}",
                std::mem::discriminant(other)
            )));
        }
    };

    let mut table_schemas = HashMap::new();
    {
        let catalogs = engine.catalogs().read().unwrap();
        let catalog = catalogs
            .get(db)
            .ok_or_else(|| MonaxError::Sql(format!("database '{}' does not exist", db)))?;
        collect_tables_from_stmt(select_from, |t| {
            if let Some(meta) = catalog.get_table(t) {
                table_schemas.insert(t.to_string(), Schema::new(meta.columns.clone()));
            }
        });
    }

    let plan = plan_select(select_from, &table_schemas)?;
    let plan = crate::sql::optimizer::optimize(plan);

    let mut out = String::new();
    render_plan(&plan, 0, &mut out);

    if analyze {
        let mut stats: HashMap<String, ScanStats> = HashMap::new();
        let start = std::time::Instant::now();
        let result = super::query::exec_select_with_stats(engine, db, select_from, &mut stats)?;
        let elapsed = start.elapsed();
        let rows_returned = match &result {
            ExecResult::Query { batches, .. } => {
                batches.iter().map(|b| b.num_rows()).sum::<usize>()
            }
            _ => 0,
        };
        out.push_str("\n=== ANALYZE ===\n");
        let mut tables: Vec<&String> = stats.keys().collect();
        tables.sort();
        for t in tables {
            let s = &stats[t];
            out.push_str(&format!("Scan {}:\n", t));
            out.push_str(&format!("  segments      : {}\n", s.segments_opened));
            let pruned_pct = if s.row_groups_total > 0 {
                (s.row_groups_pruned as f64 / s.row_groups_total as f64) * 100.0
            } else {
                0.0
            };
            out.push_str(&format!(
                "  row_groups    : {} total, {} read, {} pruned ({:.1}%)\n",
                s.row_groups_total, s.row_groups_read, s.row_groups_pruned, pruned_pct
            ));
            out.push_str(&format!(
                "  rows          : {} scanned, {} filtered by delete bitmap\n",
                s.rows_scanned, s.rows_filtered_by_delete_bitmap
            ));
            out.push_str(&format!(
                "  bytes_read    : {}\n",
                format_bytes(s.bytes_read_compressed)
            ));
        }
        out.push_str(&format!("rows_returned    : {}\n", rows_returned));
        out.push_str(&format!(
            "execution_time   : {:.3} ms\n",
            elapsed.as_secs_f64() * 1000.0
        ));
    }

    let schema = Schema::new(vec![ColumnDef::new("plan", DataType::VarChar)]);
    let mut batch = RecordBatch::new(schema.clone());
    for line in out.lines() {
        batch
            .column_mut(0)
            .unwrap()
            .push(&ScalarValue::VarChar(Some(line.to_string())))?;
    }

    Ok(ExecResult::Query {
        schema,
        batches: vec![batch],
    })
}

fn format_bytes(b: u64) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = KB * 1024.0;
    const GB: f64 = MB * 1024.0;
    let bf = b as f64;
    if bf >= GB {
        format!("{:.2} GB", bf / GB)
    } else if bf >= MB {
        format!("{:.2} MB", bf / MB)
    } else if bf >= KB {
        format!("{:.2} KB", bf / KB)
    } else {
        format!("{} B", b)
    }
}

fn collect_tables_from_stmt<F: FnMut(&str)>(stmt: &Statement, mut f: F) {
    use crate::sql::parser::ast::FromClause;
    if let Statement::Select { from: Some(fc), .. } = stmt {
        walk_from(fc, &mut f);
    }
    fn walk_from<F: FnMut(&str)>(fc: &FromClause, f: &mut F) {
        match fc {
            FromClause::Table(t) => f(&t.name),
            FromClause::Join(je) => {
                walk_from(&je.left, f);
                for jc in &je.joins {
                    f(&jc.right.name);
                }
            }
        }
    }
}

fn render_plan(plan: &LogicalPlan, depth: usize, out: &mut String) {
    let pad: String = "  ".repeat(depth);
    match plan {
        LogicalPlan::Scan(scan) => {
            out.push_str(&format!("{}Scan: table={}", pad, scan.table_name));
            if let Some(proj) = &scan.projection {
                out.push_str(&format!(", projection={:?}", proj));
            }
            if let Some(limit) = scan.pushed_limit {
                out.push_str(&format!(", pushed_limit={}", limit));
            }
            out.push('\n');
            if let Some(f) = &scan.pushed_filter {
                out.push_str(&format!(
                    "{}  pushed_filter: {}\n",
                    pad,
                    render_expr(f, &scan.schema)
                ));
            }
            if !scan.zone_map_filters.is_empty() {
                out.push_str(&format!("{}  zone_map_filters:\n", pad));
                for zmf in &scan.zone_map_filters {
                    out.push_str(&format!(
                        "{}    - {} in [{:?}, {:?}]\n",
                        pad, zmf.column, zmf.min_value, zmf.max_value
                    ));
                }
            }
            if !scan.index_columns.is_empty() {
                out.push_str(&format!(
                    "{}  index_columns: {:?}\n",
                    pad, scan.index_columns
                ));
            }
            if !scan.bloom_filter_columns.is_empty() {
                out.push_str(&format!(
                    "{}  bloom_filter_columns: {:?}\n",
                    pad, scan.bloom_filter_columns
                ));
            }
        }
        LogicalPlan::Filter(f) => {
            out.push_str(&format!(
                "{}Filter: {}\n",
                pad,
                render_expr(&f.predicate, &get_input_schema(&f.input))
            ));
            render_plan(&f.input, depth + 1, out);
        }
        LogicalPlan::Project(p) => {
            out.push_str(&format!("{}Project: {} exprs\n", pad, p.expressions.len()));
            render_plan(&p.input, depth + 1, out);
        }
        LogicalPlan::Aggregate(a) => {
            out.push_str(&format!(
                "{}Aggregate: group_by={}, aggs={}{}\n",
                pad,
                a.group_by.len(),
                a.aggregates.len(),
                if a.distinct { " DISTINCT" } else { "" }
            ));
            if a.having.is_some() {
                out.push_str(&format!("{}  having: yes\n", pad));
            }
            render_plan(&a.input, depth + 1, out);
        }
        LogicalPlan::Sort(s) => {
            out.push_str(&format!("{}Sort: {} keys\n", pad, s.sort_keys.len()));
            render_plan(&s.input, depth + 1, out);
        }
        LogicalPlan::Limit(l) => {
            out.push_str(&format!("{}Limit: {}, offset={}\n", pad, l.limit, l.offset));
            render_plan(&l.input, depth + 1, out);
        }
        LogicalPlan::TopN(t) => {
            out.push_str(&format!(
                "{}TopN: limit={}, offset={}, sort_keys={}\n",
                pad,
                t.limit,
                t.offset,
                t.sort_keys.len()
            ));
            render_plan(&t.input, depth + 1, out);
        }
        LogicalPlan::Distinct(d) => {
            out.push_str(&format!("{}Distinct\n", pad));
            render_plan(&d.input, depth + 1, out);
        }
        LogicalPlan::Window(w) => {
            out.push_str(&format!(
                "{}Window: funcs={}, partition_by={}, order_by={}\n",
                pad,
                w.window_funcs.len(),
                w.partition_by.len(),
                w.order_by.len()
            ));
            render_plan(&w.input, depth + 1, out);
        }
        LogicalPlan::Join(j) => {
            out.push_str(&format!("{}Join: {:?}\n", pad, j.join_type));
            out.push_str(&format!("{}  left:\n", pad));
            render_plan(&j.left, depth + 2, out);
            out.push_str(&format!("{}  right:\n", pad));
            render_plan(&j.right, depth + 2, out);
        }
        LogicalPlan::SemiJoin(s) => {
            out.push_str(&format!("{}SemiJoin: anti={}\n", pad, s.is_anti));
            out.push_str(&format!("{}  left:\n", pad));
            render_plan(&s.left, depth + 2, out);
            out.push_str(&format!("{}  right:\n", pad));
            render_plan(&s.right, depth + 2, out);
        }
        LogicalPlan::Empty => {
            out.push_str(&format!("{}Empty\n", pad));
        }
    }
}

fn get_input_schema(plan: &LogicalPlan) -> Schema {
    match plan {
        LogicalPlan::Scan(s) => s.schema.clone(),
        LogicalPlan::Filter(f) => get_input_schema(&f.input),
        LogicalPlan::Project(p) => get_input_schema(&p.input),
        LogicalPlan::Aggregate(a) => get_input_schema(&a.input),
        LogicalPlan::Sort(s) => get_input_schema(&s.input),
        LogicalPlan::Limit(l) => get_input_schema(&l.input),
        LogicalPlan::TopN(t) => get_input_schema(&t.input),
        LogicalPlan::Distinct(d) => get_input_schema(&d.input),
        LogicalPlan::Window(w) => get_input_schema(&w.input),
        LogicalPlan::Join(j) => j.output_schema.clone(),
        LogicalPlan::SemiJoin(s) => s.left_schema.clone(),
        LogicalPlan::Empty => Schema::empty(),
    }
}

fn render_expr(expr: &LogicalExpr, schema: &Schema) -> String {
    use crate::sql::parser::ast::BinOp;
    match expr {
        LogicalExpr::Column(idx) => schema
            .columns
            .get(*idx)
            .map(|c| c.name.clone())
            .unwrap_or_else(|| format!("col#{}", idx)),
        LogicalExpr::Literal(v) => format!("{:?}", v),
        LogicalExpr::BinaryOp { left, op, right } => {
            let opstr = match op {
                BinOp::Eq => "=",
                BinOp::Neq => "!=",
                BinOp::Lt => "<",
                BinOp::Lte => "<=",
                BinOp::Gt => ">",
                BinOp::Gte => ">=",
                BinOp::And => "AND",
                BinOp::Or => "OR",
                BinOp::Add => "+",
                BinOp::Sub => "-",
                BinOp::Mul => "*",
                BinOp::Div => "/",
                BinOp::Mod => "%",
                BinOp::Like => "LIKE",
                BinOp::NotLike => "NOT LIKE",
                BinOp::In => "IN",
                BinOp::NotIn => "NOT IN",
            };
            format!(
                "({} {} {})",
                render_expr(left, schema),
                opstr,
                render_expr(right, schema)
            )
        }
        LogicalExpr::Not(e) => format!("NOT ({})", render_expr(e, schema)),
        LogicalExpr::IsNull(e) => format!("{} IS NULL", render_expr(e, schema)),
        LogicalExpr::IsNotNull(e) => format!("{} IS NOT NULL", render_expr(e, schema)),
        LogicalExpr::In {
            expr,
            values,
            negated,
        } => format!(
            "{} {}IN ({} values)",
            render_expr(expr, schema),
            if *negated { "NOT " } else { "" },
            values.len()
        ),
        LogicalExpr::Between {
            expr,
            low,
            high,
            negated,
        } => format!(
            "{} {}BETWEEN {} AND {}",
            render_expr(expr, schema),
            if *negated { "NOT " } else { "" },
            render_expr(low, schema),
            render_expr(high, schema)
        ),
        LogicalExpr::Like {
            expr,
            pattern,
            negated,
        } => format!(
            "{} {}LIKE {:?}",
            render_expr(expr, schema),
            if *negated { "NOT " } else { "" },
            pattern
        ),
        LogicalExpr::Case { .. } => "CASE ...".to_string(),
        LogicalExpr::Function { name, args } => format!("{}({} args)", name, args.len()),
        LogicalExpr::Aggregate(a) => format!("{:?}({})", a.func, render_expr(&a.arg, schema)),
        LogicalExpr::Wildcard => "*".to_string(),
        LogicalExpr::ScalarSubquery(_) => "(subquery)".to_string(),
        LogicalExpr::ExistsSubquery(_) => "EXISTS(subquery)".to_string(),
        LogicalExpr::InSubquery { expr, negated, .. } => format!(
            "{} {}IN (subquery)",
            render_expr(expr, schema),
            if *negated { "NOT " } else { "" }
        ),
    }
}
