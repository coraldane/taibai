use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::process;
use std::time::Instant;

use clap::Parser;
use serde::Serialize;
use taibai::storage::segment::SegmentReader;
use taibai::types::batch::{RecordBatch, Schema};
use taibai::types::scalar::ScalarValue;

const CSV_COLUMNS: [&str; 13] = [
    "ChannelNo",
    "ApplSeqNum",
    "MDStreamID",
    "BidApplSeqNum",
    "OfferApplSeqNum",
    "SecurityID",
    "SecurityIDSource",
    "LastPx",
    "LastQty",
    "ExecType",
    "TransactTime",
    "LocalTime",
    "SeqNo",
];

#[derive(Parser)]
struct Args {
    #[arg(long)]
    csv: PathBuf,
    #[arg(long)]
    segment: PathBuf,
    #[arg(long)]
    trade_date: i32,
    #[arg(long, default_value_t = 100)]
    max_differences: usize,
    #[arg(long, default_value_t = 0)]
    limit: u64,
    #[arg(long)]
    output: Option<PathBuf>,
    #[arg(long, default_value_t = 1_000_000)]
    progress_rows: u64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Difference {
    csv_line: Option<u64>,
    segment_row: Option<u64>,
    column: String,
    csv_value: Option<String>,
    segment_value: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Report {
    matched: bool,
    csv_rows: u64,
    segment_rows: u64,
    compared_rows: u64,
    differences: u64,
    elapsed_seconds: f64,
    samples: Vec<Difference>,
}

struct SegmentCursor {
    reader: SegmentReader,
    row_group: usize,
    row: usize,
    global_row: u64,
    batch: Option<RecordBatch>,
}

impl SegmentCursor {
    fn new(reader: SegmentReader) -> Self {
        Self {
            reader,
            row_group: 0,
            row: 0,
            global_row: 0,
            batch: None,
        }
    }

    fn next_row(&mut self) -> Result<Option<Vec<ScalarValue>>, String> {
        loop {
            if let Some(batch) = &self.batch {
                if self.row < batch.num_rows() {
                    let values = batch
                        .columns
                        .iter()
                        .map(|column| column.scalar(self.row))
                        .collect();
                    self.row += 1;
                    self.global_row += 1;
                    return Ok(Some(values));
                }
            }
            if self.row_group >= self.reader.num_row_groups() {
                return Ok(None);
            }
            let batch = self
                .reader
                .read_row_group(self.row_group)
                .map_err(|e| format!("read row group {}: {}", self.row_group, e))?;
            self.row_group += 1;
            self.row = 0;
            self.batch = Some(batch);
        }
    }
}

fn main() {
    let args = Args::parse();
    match run(&args) {
        Ok(report) => {
            let json = serde_json::to_string_pretty(&report).unwrap();
            if let Some(path) = &args.output {
                let file = File::create(path).unwrap_or_else(|e| {
                    eprintln!("create report {}: {}", path.display(), e);
                    process::exit(2);
                });
                let mut writer = BufWriter::new(file);
                writer.write_all(json.as_bytes()).unwrap();
                writer.write_all(b"\n").unwrap();
            } else {
                println!("{}", json);
            }
            process::exit(if report.matched { 0 } else { 1 });
        }
        Err(error) => {
            eprintln!("{}", error);
            process::exit(2);
        }
    }
}

fn run(args: &Args) -> Result<Report, String> {
    let started = Instant::now();
    let reader = SegmentReader::open(&args.segment)
        .map_err(|e| format!("open segment {}: {}", args.segment.display(), e))?;
    validate_schema(reader.schema())?;
    let physical_rows = reader
        .row_count()
        .saturating_sub(reader.deleted_row_count());
    eprintln!(
        "segment={} rows={} row_groups={}",
        args.segment.display(),
        physical_rows,
        reader.num_row_groups()
    );

    let file =
        File::open(&args.csv).map_err(|e| format!("open csv {}: {}", args.csv.display(), e))?;
    let mut csv = csv::ReaderBuilder::new().flexible(true).from_reader(file);
    let headers = csv
        .headers()
        .map_err(|e| format!("read csv header: {}", e))?
        .clone();
    let indexes = csv_indexes(&headers)?;
    let mut segment = SegmentCursor::new(reader);
    let mut report = Report {
        matched: false,
        csv_rows: 0,
        segment_rows: 0,
        compared_rows: 0,
        differences: 0,
        elapsed_seconds: 0.0,
        samples: Vec::new(),
    };
    let mut previous_csv_seq = None;
    let mut previous_segment_seq = None;

    for record in csv.records() {
        if args.limit > 0 && report.csv_rows >= args.limit {
            break;
        }
        let record = record.map_err(|e| format!("read csv line {}: {}", report.csv_rows + 2, e))?;
        report.csv_rows += 1;
        let segment_row = segment.next_row()?;
        let Some(segment_values) = segment_row else {
            let csv_line = report.csv_rows + 1;
            add_difference(
                &mut report,
                args.max_differences,
                Difference {
                    csv_line: Some(csv_line),
                    segment_row: None,
                    column: "<MISSING_IN_SEGMENT>".to_string(),
                    csv_value: indexes
                        .get("SeqNo")
                        .and_then(|index| record.get(*index))
                        .map(str::to_string),
                    segment_value: None,
                },
            );
            continue;
        };
        report.segment_rows += 1;
        report.compared_rows += 1;

        let csv_seq = parse_i64(csv_field(&record, &indexes, "SeqNo")?, "SeqNo")?;
        let segment_seq = scalar_i64(&segment_values[13], "SeqNo")?;
        if let Some(previous) = previous_csv_seq {
            if csv_seq <= previous {
                return Err(format!(
                    "csv SeqNo is not strictly increasing at line {}: previous={} current={}",
                    report.csv_rows + 1,
                    previous,
                    csv_seq
                ));
            }
        }
        if let Some(previous) = previous_segment_seq {
            if segment_seq <= previous {
                return Err(format!(
                    "segment SeqNo is not strictly increasing at row {}: previous={} current={}",
                    report.segment_rows, previous, segment_seq
                ));
            }
        }
        previous_csv_seq = Some(csv_seq);
        previous_segment_seq = Some(segment_seq);

        compare_row(
            &record,
            &indexes,
            &segment_values,
            args.trade_date,
            report.csv_rows + 1,
            report.segment_rows,
            args.max_differences,
            &mut report,
        )?;

        if args.progress_rows > 0 && report.csv_rows % args.progress_rows == 0 {
            eprintln!(
                "progress rows={} differences={} elapsed={:.1}s",
                report.csv_rows,
                report.differences,
                started.elapsed().as_secs_f64()
            );
        }
    }

    if args.limit == 0 {
        while segment.next_row()?.is_some() {
            report.segment_rows += 1;
            let segment_row = report.segment_rows;
            add_difference(
                &mut report,
                args.max_differences,
                Difference {
                    csv_line: None,
                    segment_row: Some(segment_row),
                    column: "<EXTRA_IN_SEGMENT>".to_string(),
                    csv_value: None,
                    segment_value: None,
                },
            );
        }
    }
    report.elapsed_seconds = started.elapsed().as_secs_f64();
    report.matched = report.differences == 0
        && (args.limit > 0 || report.csv_rows == report.segment_rows)
        && (args.limit > 0 || report.segment_rows == physical_rows);
    Ok(report)
}

fn validate_schema(schema: &Schema) -> Result<(), String> {
    let expected = [
        "TradeDate",
        "ChannelNo",
        "ApplSeqNum",
        "MDStreamID",
        "BidApplSeqNum",
        "OfferApplSeqNum",
        "SecurityID",
        "SecurityIDSource",
        "LastPx",
        "LastQty",
        "ExecType",
        "TransactTime",
        "LocalTime",
        "SeqNo",
    ];
    if schema.columns.len() != expected.len() {
        return Err(format!(
            "segment schema has {} columns, expected {}",
            schema.columns.len(),
            expected.len()
        ));
    }
    for (index, name) in expected.iter().enumerate() {
        if !schema.columns[index].name.eq_ignore_ascii_case(name) {
            return Err(format!(
                "segment column {} is {}, expected {}",
                index, schema.columns[index].name, name
            ));
        }
    }
    Ok(())
}

fn csv_indexes(headers: &csv::StringRecord) -> Result<HashMap<String, usize>, String> {
    let mut available = HashMap::new();
    for (index, header) in headers.iter().enumerate() {
        available.insert(
            header.trim().trim_start_matches('\u{feff}').to_string(),
            index,
        );
    }
    let mut indexes = HashMap::new();
    for name in CSV_COLUMNS {
        let index = available
            .get(name)
            .copied()
            .ok_or_else(|| format!("csv header is missing {}", name))?;
        indexes.insert(name.to_string(), index);
    }
    Ok(indexes)
}

fn compare_row(
    csv: &csv::StringRecord,
    indexes: &HashMap<String, usize>,
    segment: &[ScalarValue],
    trade_date: i32,
    csv_line: u64,
    segment_row: u64,
    max_differences: usize,
    report: &mut Report,
) -> Result<(), String> {
    compare_value(
        report,
        max_differences,
        csv_line,
        segment_row,
        "TradeDate",
        trade_date.to_string(),
        scalar_i64(&segment[0], "TradeDate")?.to_string(),
    );
    for (csv_index, name) in CSV_COLUMNS.iter().enumerate() {
        let csv_value = csv_field(csv, indexes, name)?.trim();
        let segment_value = &segment[csv_index + 1];
        match *name {
            "LastPx" => {
                let left = csv_value
                    .parse::<f64>()
                    .map_err(|e| format!("parse {} at line {}: {}", name, csv_line, e))?;
                let right = scalar_f64(segment_value, name)?;
                if left.to_bits() != right.to_bits() {
                    add_field_difference(
                        report,
                        max_differences,
                        csv_line,
                        segment_row,
                        name,
                        csv_value,
                        &right.to_string(),
                    );
                }
            }
            "MDStreamID" | "SecurityID" | "SecurityIDSource" | "TransactTime" | "LocalTime" => {
                let right = scalar_string(segment_value, name)?;
                compare_value(
                    report,
                    max_differences,
                    csv_line,
                    segment_row,
                    name,
                    csv_value.to_string(),
                    right.to_string(),
                );
            }
            _ => {
                let left = parse_i64(csv_value, name)?;
                let right = scalar_i64(segment_value, name)?;
                if left != right {
                    add_field_difference(
                        report,
                        max_differences,
                        csv_line,
                        segment_row,
                        name,
                        csv_value,
                        &right.to_string(),
                    );
                }
            }
        }
    }
    Ok(())
}

fn compare_value(
    report: &mut Report,
    max_differences: usize,
    csv_line: u64,
    segment_row: u64,
    column: &str,
    csv_value: String,
    segment_value: String,
) {
    if csv_value != segment_value {
        add_field_difference(
            report,
            max_differences,
            csv_line,
            segment_row,
            column,
            &csv_value,
            &segment_value,
        );
    }
}

fn add_field_difference(
    report: &mut Report,
    max_differences: usize,
    csv_line: u64,
    segment_row: u64,
    column: &str,
    csv_value: &str,
    segment_value: &str,
) {
    add_difference(
        report,
        max_differences,
        Difference {
            csv_line: Some(csv_line),
            segment_row: Some(segment_row),
            column: column.to_string(),
            csv_value: Some(csv_value.to_string()),
            segment_value: Some(segment_value.to_string()),
        },
    );
}

fn add_difference(report: &mut Report, max_differences: usize, difference: Difference) {
    report.differences += 1;
    if report.samples.len() < max_differences {
        report.samples.push(difference);
    }
}

fn csv_field<'a>(
    record: &'a csv::StringRecord,
    indexes: &HashMap<String, usize>,
    name: &str,
) -> Result<&'a str, String> {
    let index = indexes
        .get(name)
        .ok_or_else(|| format!("csv index is missing {}", name))?;
    record
        .get(*index)
        .ok_or_else(|| format!("csv row is missing {}", name))
}

fn parse_i64(value: &str, name: &str) -> Result<i64, String> {
    value
        .trim()
        .parse::<i64>()
        .map_err(|e| format!("parse {} value {:?}: {}", name, value, e))
}

fn scalar_i64(value: &ScalarValue, name: &str) -> Result<i64, String> {
    value
        .integer_i128()
        .and_then(|value| i64::try_from(value).ok())
        .ok_or_else(|| format!("segment {} is not a non-null integer: {:?}", name, value))
}

fn scalar_f64(value: &ScalarValue, name: &str) -> Result<f64, String> {
    match value {
        ScalarValue::Float64(Some(value)) => Ok(*value),
        ScalarValue::Float32(Some(value)) => Ok(*value as f64),
        _ => Err(format!(
            "segment {} is not a non-null float: {:?}",
            name, value
        )),
    }
}

fn scalar_string<'a>(value: &'a ScalarValue, name: &str) -> Result<&'a str, String> {
    match value {
        ScalarValue::VarChar(Some(value)) => Ok(value),
        _ => Err(format!(
            "segment {} is not a non-null string: {:?}",
            name, value
        )),
    }
}
