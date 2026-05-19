use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId};
use monax::types::batch::{ColumnDef, RecordBatch, Schema};
use monax::types::scalar::ScalarValue;
use monax::protocol::types::DataType;
use monax::storage::segment::{SegmentWriter, SegmentReader};
use monax::storage::wal::{WalWriter, RecordType};
use monax::sql::operators::*;
use monax::sql::parser::ast::BinOp;
use monax::sql::planner::*;

fn make_batch(n_rows: usize) -> RecordBatch {
    let schema = Schema::new(vec![
        ColumnDef::new("id", DataType::Int64),
        ColumnDef::new("name", DataType::VarChar),
        ColumnDef::new("score", DataType::Float64),
    ]);
    let mut batch = RecordBatch::new(schema);
    for i in 0..n_rows {
        batch.column_mut(0).unwrap().push(&ScalarValue::Int64(Some(i as i64))).unwrap();
        batch.column_mut(1).unwrap().push(&ScalarValue::VarChar(Some(format!("user_{}", i)))).unwrap();
        batch.column_mut(2).unwrap().push(&ScalarValue::Float64(Some(i as f64 * 1.5))).unwrap();
    }
    batch
}

fn bench_scan(c: &mut Criterion) {
    let batch = make_batch(10_000);
    let mut group = c.benchmark_group("scan");
    group.bench_function("10k_rows", |b| {
        b.iter(|| {
            let mut exec = ScanExec::new(vec![batch.clone()], None);
            while let Ok(Some(_)) = exec.next_batch() {}
        })
    });
    group.finish();
}

fn bench_filter(c: &mut Criterion) {
    let batch = make_batch(10_000);
    let schema = batch.schema.clone();
    let predicate = LogicalExpr::BinaryOp {
        left: Box::new(LogicalExpr::Column(0)),
        op: BinOp::Gt,
        right: Box::new(LogicalExpr::Literal(ScalarValue::Int64(Some(5000)))),
    };
    let mut group = c.benchmark_group("filter");
    group.bench_function("10k_rows_gt_5k", |b| {
        b.iter(|| {
            let mut exec = FilterExec {
                predicate: predicate.clone(),
                input: Box::new(ScanExec::new(vec![batch.clone()], None)),
                schema: schema.clone(),
            };
            while let Ok(Some(_)) = exec.next_batch() {}
        })
    });
    group.finish();
}

fn bench_aggregate(c: &mut Criterion) {
    let batch = make_batch(10_000);
    let input_schema = batch.schema.clone();
    let agg = AggregateExpr {
        func: AggFunc::Count,
        arg: Box::new(LogicalExpr::Wildcard),
        distinct: false,
    };
    let mut group = c.benchmark_group("aggregate");
    group.bench_function("count_10k", |b| {
        b.iter(|| {
            let mut exec = HashAggregateExec::new(
                vec![],
                vec![agg.clone()],
                Box::new(ScanExec::new(vec![batch.clone()], None)),
                input_schema.clone(),
            );
            while let Ok(Some(_)) = exec.next_batch() {}
        })
    });
    group.finish();
}

fn bench_segment_write_read(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    let mut group = c.benchmark_group("segment");
    for size in [100, 1000, 10000] {
        let batch = make_batch(size);
        let schema = batch.schema.clone();
        group.bench_with_input(BenchmarkId::new("write_read", size), &size, |b, &_size| {
            b.iter(|| {
                let path = dir.path().join(format!("bench_{}.dat", std::process::id()));
                let mut writer = SegmentWriter::create(&path, schema.clone(), 8192).unwrap();
                writer.write_batch(&batch).unwrap();
                writer.seal().unwrap();
                let reader = SegmentReader::open(&path).unwrap();
                let _batches = reader.scan(4096).unwrap();
            })
        });
    }
    group.finish();
}

fn bench_wal_write(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    let mut group = c.benchmark_group("wal");
    let data = vec![0u8; 256];
    group.bench_function("write_100_records", |b| {
        b.iter(|| {
            let mut writer = WalWriter::open(dir.path(), 1024 * 1024).unwrap();
            for i in 0..100u64 {
                writer.write(RecordType::Begin, i, &data).unwrap();
            }
        })
    });
    group.finish();
}

fn bench_bulk_insert_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("bulk_insert");
    let col_count = 3u32;
    let row_count = 1000u32;
    let mut payload = Vec::new();
    let table_name = b"bench_table";
    payload.extend_from_slice(&(table_name.len() as u16).to_le_bytes());
    payload.extend_from_slice(table_name);
    payload.extend_from_slice(&row_count.to_le_bytes());
    payload.extend_from_slice(&col_count.to_le_bytes());
    let col_names: [&[u8]; 3] = [b"id", b"name", b"score"];
    let col_types: [u8; 3] = [4, 10, 8];
    for i in 0..3 {
        payload.extend_from_slice(&(col_names[i].len() as u16).to_le_bytes());
        payload.extend_from_slice(col_names[i]);
        payload.push(col_types[i]);
        payload.extend_from_slice(&0u32.to_le_bytes());
        payload.extend_from_slice(&0u32.to_le_bytes());
        let data_len = if i == 1 { row_count * 20 } else { row_count * 8 };
        payload.extend_from_slice(&data_len.to_le_bytes());
        payload.extend_from_slice(&0u32.to_le_bytes());
        for r in 0..row_count {
            match i {
                0 => payload.extend_from_slice(&(r as i64).to_le_bytes()),
                1 => {
                    let s = format!("user_{:018}", r);
                    payload.extend_from_slice(s.as_bytes());
                }
                2 => payload.extend_from_slice(&(r as f64).to_le_bytes()),
                _ => {}
            }
        }
    }
    group.bench_function("decode_1k_rows_3_cols", |b| {
        b.iter(|| {
            let _ = monax::protocol::bulk::decode_bulk_insert(&payload);
        })
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_scan,
    bench_filter,
    bench_aggregate,
    bench_segment_write_read,
    bench_wal_write,
    bench_bulk_insert_decode,
);
criterion_main!(benches);
