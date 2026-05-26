#![allow(dead_code)]
use bytes::{Bytes, BytesMut};
use crossbeam_queue::ArrayQueue;
use std::sync::Arc;

pub struct BufferPool {
    buffers: ArrayQueue<BytesMut>,
    default_size: usize,
    created: std::sync::atomic::AtomicU64,
}

impl BufferPool {
    pub fn new(pool_size: usize, default_size: usize) -> Self {
        let buffers = ArrayQueue::new(pool_size);
        for _ in 0..pool_size {
            let _ = buffers.push(BytesMut::with_capacity(default_size));
        }
        Self {
            buffers,
            default_size,
            created: std::sync::atomic::AtomicU64::new(0),
        }
    }

    pub fn acquire(&self) -> BytesMut {
        self.buffers.pop().unwrap_or_else(|| {
            self.created
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            BytesMut::with_capacity(self.default_size)
        })
    }

    pub fn release(&self, buf: BytesMut) {
        let mut buf = buf;
        buf.clear();
        let _ = self.buffers.push(buf);
    }

    pub fn acquire_bytes(&self) -> Bytes {
        let buf = self.acquire();
        buf.freeze()
    }

    pub fn stats(&self) -> (usize, u64) {
        (
            self.buffers.len(),
            self.created.load(std::sync::atomic::Ordering::Relaxed),
        )
    }
}

impl Default for BufferPool {
    fn default() -> Self {
        Self::new(256, 64 * 1024)
    }
}

pub struct EncodeBufferPool {
    pools: Vec<Arc<BufferPool>>,
}

impl EncodeBufferPool {
    pub fn new() -> Self {
        let sizes = [4 * 1024, 16 * 1024, 64 * 1024, 256 * 1024, 1024 * 1024];
        let pools = sizes
            .iter()
            .map(|size| Arc::new(BufferPool::new(64, *size)))
            .collect();
        Self { pools }
    }

    pub fn acquire_for_size(&self, needed: usize) -> BytesMut {
        for pool in &self.pools {
            if pool.default_size >= needed {
                return pool.acquire();
            }
        }
        BytesMut::with_capacity(needed)
    }

    pub fn release(&self, buf: BytesMut) {
        let capacity = buf.capacity();
        for pool in &self.pools {
            if pool.default_size == capacity {
                pool.release(buf);
                return;
            }
        }
    }
}

impl Default for EncodeBufferPool {
    fn default() -> Self {
        Self::new()
    }
}

pub struct BatchBufferPool {
    pool: ArrayQueue<Vec<u8>>,
    default_size: usize,
}

impl BatchBufferPool {
    pub fn new(pool_size: usize, default_size: usize) -> Self {
        let pool = ArrayQueue::new(pool_size);
        for _ in 0..pool_size {
            let _ = pool.push(Vec::with_capacity(default_size));
        }
        Self { pool, default_size }
    }

    pub fn acquire(&self) -> Vec<u8> {
        self.pool
            .pop()
            .unwrap_or_else(|| Vec::with_capacity(self.default_size))
    }

    pub fn release(&self, buf: Vec<u8>) {
        let mut buf = buf;
        buf.clear();
        let _ = self.pool.push(buf);
    }
}

impl Default for BatchBufferPool {
    fn default() -> Self {
        Self::new(128, 64 * 1024)
    }
}

pub static GLOBAL_BUFFER_POOL: std::sync::OnceLock<Arc<BufferPool>> = std::sync::OnceLock::new();
pub static GLOBAL_ENCODE_POOL: std::sync::OnceLock<Arc<EncodeBufferPool>> =
    std::sync::OnceLock::new();
pub static GLOBAL_BATCH_POOL: std::sync::OnceLock<Arc<BatchBufferPool>> =
    std::sync::OnceLock::new();

pub fn global_buffer_pool() -> Arc<BufferPool> {
    GLOBAL_BUFFER_POOL
        .get_or_init(|| Arc::new(BufferPool::new(512, 64 * 1024)))
        .clone()
}

pub fn global_encode_pool() -> Arc<EncodeBufferPool> {
    GLOBAL_ENCODE_POOL
        .get_or_init(|| Arc::new(EncodeBufferPool::new()))
        .clone()
}

pub fn global_batch_pool() -> Arc<BatchBufferPool> {
    GLOBAL_BATCH_POOL
        .get_or_init(|| Arc::new(BatchBufferPool::new(128, 64 * 1024)))
        .clone()
}
