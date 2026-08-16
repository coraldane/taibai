use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::Instant;

use bytes::Bytes;
use crossbeam_queue::ArrayQueue;

use crate::common::error::{Result, TaibaiError};
use crate::net::client::{BulkImportConfig, Client};
use crate::types::array::ColumnArray;
use crate::types::batch::{RecordBatch, Schema};

const DEFAULT_PIPELINE_DEPTH: usize = 8;
const DEFAULT_BATCH_SIZE: usize = 100000;

pub struct BulkImporterConfig {
    pub pipeline_depth: usize,
    pub batch_size: usize,
    pub parallel_encode: bool,
}

impl Default for BulkImporterConfig {
    fn default() -> Self {
        Self {
            pipeline_depth: DEFAULT_PIPELINE_DEPTH,
            batch_size: DEFAULT_BATCH_SIZE,
            parallel_encode: true,
        }
    }
}

pub struct BulkImporterStats {
    pub total_rows: u64,
    pub total_batches: u64,
    pub rows_per_sec: f64,
    pub batches_per_sec: f64,
    pub elapsed_secs: f64,
    pub pending_count: i64,
}

pub struct BulkImporter {
    client: Client,
    config: BulkImporterConfig,
    table_name: String,
    schema: Schema,
    sent_count: AtomicI64,
    completed_count: AtomicI64,
    total_rows: AtomicU64,
    total_batches: AtomicU64,
    start_time: Instant,
    batch_queue: ArrayQueue<Bytes>,
}

impl BulkImporter {
    pub async fn new(
        client: Client,
        table_name: &str,
        schema: Schema,
        config: BulkImporterConfig,
    ) -> Result<Self> {
        let batch_queue = ArrayQueue::new(config.pipeline_depth * 2);
        for _ in 0..config.pipeline_depth {
            let _ = batch_queue.push(Bytes::new());
        }

        Ok(Self {
            client,
            config,
            table_name: table_name.to_string(),
            schema,
            sent_count: AtomicI64::new(0),
            completed_count: AtomicI64::new(0),
            total_rows: AtomicU64::new(0),
            total_batches: AtomicU64::new(0),
            start_time: Instant::now(),
            batch_queue,
        })
    }

    pub async fn start(&mut self) -> Result<()> {
        self.client.start_bulk_import(&BulkImportConfig).await?;

        self.client
            .start_pipeline(self.config.pipeline_depth)
            .await?;

        self.start_time = Instant::now();

        Ok(())
    }

    pub async fn insert_batch(&mut self, batch: RecordBatch) -> Result<()> {
        let row_count = batch.num_rows();
        if row_count == 0 {
            return Ok(());
        }

        let payload = self.encode_batch(&batch)?;

        while self.sent_count.load(Ordering::Relaxed) - self.completed_count.load(Ordering::Relaxed)
            >= self.config.pipeline_depth as i64
        {
            self.drain_one_response().await?;
        }

        self.client.pipeline_bulk_insert_columnar(payload).await?;

        self.sent_count.fetch_add(1, Ordering::Relaxed);
        self.total_rows
            .fetch_add(row_count as u64, Ordering::Relaxed);
        self.total_batches.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    fn encode_batch(&self, batch: &RecordBatch) -> Result<Bytes> {
        encode_columnar_bulk_insert(&self.table_name, batch)
    }

    pub async fn drain_responses(&mut self) -> Result<()> {
        while self.completed_count.load(Ordering::Relaxed) < self.sent_count.load(Ordering::Relaxed)
        {
            self.drain_one_response().await?;
        }

        Ok(())
    }

    async fn drain_one_response(&mut self) -> Result<()> {
        let resp = self.client.recv_response().await?;
        self.completed_count.fetch_add(1, Ordering::Relaxed);

        match resp.msg_type {
            crate::protocol::message::MSG_TYPE_OK => Ok(()),
            crate::protocol::message::MSG_TYPE_ERROR => {
                let msg = String::from_utf8_lossy(&resp.payload).to_string();
                Err(TaibaiError::Storage(msg))
            }
            other => Err(TaibaiError::Protocol(format!(
                "unexpected bulk import response type: {other:#04X}"
            ))),
        }
    }

    pub async fn finish(&mut self) -> Result<BulkImporterStats> {
        self.drain_responses().await?;

        self.client.stop_pipeline().await?;
        self.client.stop_bulk_import().await?;

        let elapsed = self.start_time.elapsed();
        let elapsed_secs = elapsed.as_secs_f64();
        let total_rows = self.total_rows.load(Ordering::Relaxed);
        let total_batches = self.total_batches.load(Ordering::Relaxed);

        Ok(BulkImporterStats {
            total_rows,
            total_batches,
            rows_per_sec: total_rows as f64 / elapsed_secs,
            batches_per_sec: total_batches as f64 / elapsed_secs,
            elapsed_secs,
            pending_count: self.sent_count.load(Ordering::Relaxed)
                - self.completed_count.load(Ordering::Relaxed),
        })
    }

    pub fn get_stats(&self) -> BulkImporterStats {
        let elapsed = self.start_time.elapsed();
        let elapsed_secs = elapsed.as_secs_f64();
        let total_rows = self.total_rows.load(Ordering::Relaxed);
        let total_batches = self.total_batches.load(Ordering::Relaxed);

        BulkImporterStats {
            total_rows,
            total_batches,
            rows_per_sec: total_rows as f64 / elapsed_secs,
            batches_per_sec: total_batches as f64 / elapsed_secs,
            elapsed_secs,
            pending_count: self.sent_count.load(Ordering::Relaxed)
                - self.completed_count.load(Ordering::Relaxed),
        }
    }
}

pub fn encode_columnar_bulk_insert(table_name: &str, batch: &RecordBatch) -> Result<Bytes> {
    let mut buf = Vec::new();

    buf.extend_from_slice(&(table_name.len() as u16).to_le_bytes());
    buf.extend_from_slice(table_name.as_bytes());

    buf.extend_from_slice(&(batch.num_rows() as u32).to_le_bytes());
    buf.extend_from_slice(&(batch.num_columns() as u32).to_le_bytes());

    for col_idx in 0..batch.num_columns() {
        let col = batch.column(col_idx).unwrap();
        let col_def = &batch.schema.columns[col_idx];

        buf.extend_from_slice(&(col_def.name.len() as u16).to_le_bytes());
        buf.extend_from_slice(col_def.name.as_bytes());
        buf.push(col_def.data_type as u8);
        let scale = u8::try_from(col_def.scale).map_err(|_| {
            TaibaiError::Protocol(format!(
                "column '{}' scale {} exceeds u8 bulk metadata range",
                col_def.name, col_def.scale
            ))
        })?;
        let precision = u8::try_from(col_def.precision).map_err(|_| {
            TaibaiError::Protocol(format!(
                "column '{}' precision {} exceeds u8 bulk metadata range",
                col_def.name, col_def.precision
            ))
        })?;
        buf.push(scale);
        buf.push(precision);

        let (data, nulls) = encode_column_data(col)?;

        buf.extend_from_slice(&(batch.num_rows() as u32).to_le_bytes());
        buf.extend_from_slice(&(nulls.len() as u32).to_le_bytes());
        buf.extend_from_slice(&data);
        buf.extend_from_slice(&nulls);
    }

    Ok(Bytes::from(buf))
}

fn encode_column_data(col: &ColumnArray) -> Result<(Vec<u8>, Vec<u8>)> {
    let mut data = Vec::new();
    let mut nulls = Vec::new();
    let _row_count = col.len();

    let null_bitmap = col.bitmap();
    if !null_bitmap.is_all_valid() {
        nulls = null_bitmap.to_bytes();
    }

    match col {
        ColumnArray::Bool { data: vals, .. } => {
            for v in vals {
                data.push(*v);
            }
        }
        ColumnArray::Int8 { data: vals, .. } => {
            for v in vals {
                data.push(*v as u8);
            }
        }
        ColumnArray::Int16 { data: vals, .. } => {
            for v in vals {
                data.extend_from_slice(&v.to_le_bytes());
            }
        }
        ColumnArray::Int32 { data: vals, .. } => {
            for v in vals {
                data.extend_from_slice(&v.to_le_bytes());
            }
        }
        ColumnArray::Int64 { data: vals, .. } => {
            for v in vals {
                data.extend_from_slice(&v.to_le_bytes());
            }
        }
        ColumnArray::UInt8 { data: vals, .. } => {
            for v in vals {
                data.push(*v);
            }
        }
        ColumnArray::UInt16 { data: vals, .. } => {
            for v in vals {
                data.extend_from_slice(&v.to_le_bytes());
            }
        }
        ColumnArray::UInt32 { data: vals, .. } => {
            for v in vals {
                data.extend_from_slice(&v.to_le_bytes());
            }
        }
        ColumnArray::UInt64 { data: vals, .. } => {
            for v in vals {
                data.extend_from_slice(&v.to_le_bytes());
            }
        }
        ColumnArray::Float32 { data: vals, .. } => {
            for v in vals {
                data.extend_from_slice(&v.to_le_bytes());
            }
        }
        ColumnArray::Float64 { data: vals, .. } => {
            for v in vals {
                data.extend_from_slice(&v.to_le_bytes());
            }
        }
        ColumnArray::Decimal { data: vals, .. } => {
            for v in vals {
                data.extend_from_slice(&v.to_le_bytes());
            }
        }
        ColumnArray::Date { data: vals, .. } => {
            for v in vals {
                data.extend_from_slice(&v.to_le_bytes());
            }
        }
        ColumnArray::Time { data: vals, .. } => {
            for v in vals {
                data.extend_from_slice(&v.to_le_bytes());
            }
        }
        ColumnArray::Timestamp { data: vals, .. } => {
            for v in vals {
                data.extend_from_slice(&v.to_le_bytes());
            }
        }
        ColumnArray::Interval { data: vals, .. } => {
            for v in vals {
                data.extend_from_slice(&v.to_le_bytes());
            }
        }
        ColumnArray::VarChar { data: vals, .. } => {
            for v in vals {
                data.extend_from_slice(&(v.len() as u32).to_le_bytes());
                data.extend_from_slice(v.as_bytes());
            }
        }
        ColumnArray::Blob { data: vals, .. } => {
            for v in vals {
                data.extend_from_slice(&(v.len() as u32).to_le_bytes());
                data.extend_from_slice(v);
            }
        }
        ColumnArray::Vector { data: vals, .. } => {
            for v in vals {
                data.extend_from_slice(&(v.len() as u32).to_le_bytes());
                for f in v {
                    data.extend_from_slice(&f.to_le_bytes());
                }
            }
        }
    }

    Ok((data, nulls))
}

#[cfg(test)]
#[path = "bulk_importer_test.rs"]
mod tests;
