use std::io::BufRead;

use taibai::net::bulk_importer::encode_columnar_bulk_insert;
use taibai::net::client::{Client, QueryResult};
use taibai::protocol::types::DataType;
use taibai::types::batch::{ColumnDef, RecordBatch, Schema};
use taibai::types::scalar::ScalarValue;

/// Bulk-load a CSV file (exported by export_csv: header row + plain values)
/// into a table via the columnar bulk insert pipeline.
///
/// Usage: load_csv <host> <database> <table> <csv_path> [batch_rows]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let host = args
        .get(1)
        .cloned()
        .unwrap_or_else(|| "127.0.0.1:5151".into());
    let database = args.get(2).cloned().unwrap_or_else(|| "main".into());
    let table = args.get(3).cloned().expect("table name required");
    let csv_path = args.get(4).cloned().expect("csv path required");
    let batch_rows: usize = args.get(5).and_then(|s| s.parse().ok()).unwrap_or(200_000);

    let mut client = Client::connect(host.as_str()).await?;
    client.set_timeout(std::time::Duration::from_secs(3600));
    client.login("root", "taibai", &database).await?;

    // Fetch target schema.
    let mut columns: Vec<(String, DataType)> = Vec::new();
    match client
        .query(&format!("SHOW COLUMNS FROM {}", table))
        .await?
    {
        QueryResult::Rows { batches, .. } => {
            for batch in &batches {
                for row in 0..batch.num_rows() {
                    let name = batch.scalar(row, 0).unwrap().to_string();
                    let ty = batch.scalar(row, 1).unwrap().to_string();
                    columns.push((name, parse_type(&ty)?));
                }
            }
        }
        _ => return Err("SHOW COLUMNS failed".into()),
    }
    eprintln!(
        "table {} has {} columns: {:?}",
        table,
        columns.len(),
        columns
            .iter()
            .map(|(n, t)| format!("{n}:{t:?}"))
            .collect::<Vec<_>>()
    );

    let schema = Schema::new(
        columns
            .iter()
            .map(|(name, dt)| ColumnDef::new(name, *dt))
            .collect(),
    );

    client.start_pipeline(8).await?;

    let start = std::time::Instant::now();
    let file = std::fs::File::open(&csv_path)?;
    let mut reader = std::io::BufReader::with_capacity(1 << 22, file);
    let mut header = String::new();
    reader.read_line(&mut header)?;
    let file_columns: Vec<String> = header
        .trim_end()
        .split(',')
        .map(|s| s.trim_matches('"').to_string())
        .collect();

    // Map file columns to table columns by name.
    let mut col_map: Vec<Option<usize>> = Vec::with_capacity(file_columns.len());
    for fc in &file_columns {
        col_map.push(columns.iter().position(|(n, _)| n.eq_ignore_ascii_case(fc)));
    }

    let mut total = 0usize;
    let mut line = String::new();
    let mut batch = RecordBatch::with_capacity(schema.clone(), batch_rows);
    loop {
        line.clear();
        let n = reader.read_line(&mut line)?;
        if n == 0 {
            break;
        }
        let trimmed = line.trim_end();
        if trimmed.is_empty() {
            continue;
        }
        for (file_idx, target_idx) in col_map.iter().enumerate() {
            let Some(target_idx) = target_idx else {
                continue;
            };
            let dt = columns[*target_idx].1;
            let field = trimmed.split(',').nth(file_idx).unwrap_or("");
            let value = parse_scalar(field.trim(), dt)?;
            batch.column_mut(*target_idx).unwrap().push(&value)?;
        }
        total += 1;
        if total.is_multiple_of(batch_rows) {
            let payload = encode_columnar_bulk_insert(&table, &batch)?;
            client.pipeline_bulk_insert_columnar(payload).await?;
            batch = RecordBatch::with_capacity(schema.clone(), batch_rows);
            if total.is_multiple_of(batch_rows * 5) {
                eprintln!(
                    "loaded {} rows, elapsed {:?}, in-flight {}",
                    total,
                    start.elapsed(),
                    client.pipeline_in_flight()
                );
            }
        }
    }
    if batch.num_rows() > 0 {
        let payload = encode_columnar_bulk_insert(&table, &batch)?;
        client.pipeline_bulk_insert_columnar(payload).await?;
    }
    client.drain_pipeline().await?;
    client.stop_pipeline().await?;
    eprintln!("done: {} rows in {:?}", total, start.elapsed());
    client.close().await?;
    Ok(())
}

fn parse_type(ty: &str) -> Result<DataType, Box<dyn std::error::Error>> {
    let upper = ty.to_uppercase();
    let dt = match upper.as_str() {
        "INT8" | "TINYINT" => DataType::Int8,
        "INT16" | "SMALLINT" => DataType::Int16,
        "INT32" | "INT" | "INTEGER" => DataType::Int32,
        "INT64" | "BIGINT" => DataType::Int64,
        "FLOAT" | "FLOAT32" => DataType::Float32,
        "DOUBLE" | "FLOAT64" => DataType::Float64,
        "VARCHAR" | "STRING" => DataType::VarChar,
        "BOOLEAN" | "BOOL" => DataType::Bool,
        "TIMESTAMP" => DataType::Timestamp,
        "DATE" => DataType::Date,
        "DECIMAL" => DataType::Decimal,
        other => {
            if other.starts_with("DECIMAL") {
                DataType::Decimal
            } else {
                return Err(format!("unsupported type {}", other).into());
            }
        }
    };
    Ok(dt)
}

fn parse_scalar(field: &str, dt: DataType) -> Result<ScalarValue, Box<dyn std::error::Error>> {
    if field.is_empty() || field == "NULL" {
        return Ok(match dt {
            DataType::Int8 => ScalarValue::Int8(None),
            DataType::Int16 => ScalarValue::Int16(None),
            DataType::Int32 => ScalarValue::Int32(None),
            DataType::Int64 => ScalarValue::Int64(None),
            DataType::Float32 => ScalarValue::Float32(None),
            DataType::Float64 => ScalarValue::Float64(None),
            DataType::Decimal => ScalarValue::Decimal(None),
            DataType::Timestamp => ScalarValue::Timestamp(None),
            DataType::Date => ScalarValue::Date(None),
            DataType::VarChar => ScalarValue::VarChar(None),
            DataType::Bool => ScalarValue::Bool(None),
            other => return Err(format!("no null for {:?}", other).into()),
        });
    }
    let v = match dt {
        DataType::Int8 => ScalarValue::Int8(Some(field.parse()?)),
        DataType::Int16 => ScalarValue::Int16(Some(field.parse()?)),
        DataType::Int32 => ScalarValue::Int32(Some(field.parse()?)),
        DataType::Int64 => ScalarValue::Int64(Some(field.parse()?)),
        DataType::Float32 => ScalarValue::Float32(Some(field.parse()?)),
        DataType::Float64 => ScalarValue::Float64(Some(field.parse()?)),
        DataType::Decimal => ScalarValue::Decimal(Some(field.parse()?)),
        DataType::Timestamp => ScalarValue::Timestamp(Some(field.parse()?)),
        DataType::Date => ScalarValue::Date(Some(field.parse()?)),
        DataType::VarChar => ScalarValue::VarChar(Some(field.to_string())),
        DataType::Bool => {
            ScalarValue::Bool(Some(field.eq_ignore_ascii_case("true") || field == "1"))
        }
        other => return Err(format!("unsupported parse type {:?}", other).into()),
    };
    Ok(v)
}
