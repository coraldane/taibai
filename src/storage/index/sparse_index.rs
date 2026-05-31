#![allow(dead_code)]
use std::sync::{Mutex, RwLock};

use crate::storage::index::bloomfilter::BloomFilter;
use crate::storage::segment::ZoneMap;
use crate::types::scalar::ScalarValue;

#[derive(Debug, Clone)]
pub struct SparseBlock {
    pub block_id: usize,
    pub zone_map: Option<ZoneMap>,
    pub bloom_filter: Option<BloomFilter>,
    pub start_row_id: i64,
    pub row_count: i64,
}

impl SparseBlock {
    pub fn new(block_id: usize, start_row_id: i64, row_count: i64) -> Self {
        Self {
            block_id,
            zone_map: Some(ZoneMap::null()),
            bloom_filter: Some(BloomFilter::with_defaults()),
            start_row_id,
            row_count,
        }
    }

    pub fn contains_row(&self, row_id: i64) -> bool {
        row_id >= self.start_row_id && row_id < self.start_row_id + self.row_count
    }
}

#[derive(Debug, Clone)]
pub struct SparseBlockData {
    pub block_id: usize,
    pub zone_map: Option<ZoneMap>,
    pub bloom_filter: Option<BloomFilter>,
    pub start_row_id: i64,
    pub row_count: i64,
}

pub struct SparseIndexImpl {
    blocks: RwLock<Vec<SparseBlock>>,
    size: Mutex<i64>,
    stale: Mutex<bool>,
}

impl SparseIndexImpl {
    pub fn new() -> Self {
        Self {
            blocks: RwLock::new(Vec::new()),
            size: Mutex::new(0),
            stale: Mutex::new(false),
        }
    }

    pub fn insert(&self, key: &ScalarValue, row_id: i64) -> bool {
        if key.is_null() {
            return false;
        }

        let blocks = self.blocks.read().unwrap();
        let block_idx = self.find_block_for_row_id(&blocks, row_id);

        if block_idx < 0 || block_idx >= blocks.len() as i32 {
            *self.stale.lock().unwrap() = true;
            return false;
        }

        drop(blocks);
        let mut blocks = self.blocks.write().unwrap();
        let block = &mut blocks[block_idx as usize];

        if let Some(zm) = &mut block.zone_map {
            zm.update(key);
        }

        if let Some(bf) = &mut block.bloom_filter {
            bf.add(key);
        }

        *self.size.lock().unwrap() += 1;
        true
    }

    pub fn search(&self, key: &ScalarValue) -> Vec<i64> {
        if key.is_null() {
            return Vec::new();
        }

        let blocks = self.blocks.read().unwrap();
        let block_ids = self.search_blocks(&blocks, key);
        self.collect_row_ids(&blocks, &block_ids)
    }

    pub fn search_blocks(&self, blocks: &[SparseBlock], key: &ScalarValue) -> Vec<usize> {
        let mut candidates = Vec::new();

        for block in blocks {
            if let Some(bf) = &block.bloom_filter {
                if !bf.might_contain(key) {
                    continue;
                }
            }

            if let Some(zm) = &block.zone_map {
                if !zm.contains(key) {
                    continue;
                }
            }

            candidates.push(block.block_id);
        }

        candidates
    }

    pub fn range_search(&self, min: &ScalarValue, max: &ScalarValue) -> Vec<i64> {
        let blocks = self.blocks.read().unwrap();
        let block_ids = self.range_search_blocks(&blocks, min, max);
        self.collect_row_ids(&blocks, &block_ids)
    }

    pub fn range_search_blocks(
        &self,
        blocks: &[SparseBlock],
        min: &ScalarValue,
        max: &ScalarValue,
    ) -> Vec<usize> {
        let mut candidates = Vec::new();

        for block in blocks {
            if let Some(zm) = &block.zone_map {
                if zm.overlaps(min, max) {
                    candidates.push(block.block_id);
                }
            } else {
                candidates.push(block.block_id);
            }
        }

        candidates
    }

    fn find_block_for_row_id(&self, blocks: &[SparseBlock], row_id: i64) -> i32 {
        for (i, block) in blocks.iter().enumerate() {
            if block.contains_row(row_id) {
                return i as i32;
            }
        }
        -1
    }

    fn collect_row_ids(&self, blocks: &[SparseBlock], block_ids: &[usize]) -> Vec<i64> {
        let mut row_ids = Vec::new();

        for block_id in block_ids {
            if *block_id < blocks.len() {
                let block = &blocks[*block_id];
                for i in 0..block.row_count {
                    row_ids.push(block.start_row_id + i);
                }
            }
        }

        row_ids
    }

    pub fn add_block(&self, block: SparseBlock) {
        let mut blocks = self.blocks.write().unwrap();

        let block = if block.zone_map.is_none() {
            SparseBlock {
                zone_map: Some(ZoneMap::null()),
                bloom_filter: Some(BloomFilter::with_defaults()),
                ..block
            }
        } else if block.bloom_filter.is_none() {
            SparseBlock {
                bloom_filter: Some(BloomFilter::with_defaults()),
                ..block
            }
        } else {
            block
        };

        let row_count = block.row_count;
        blocks.push(block);
        *self.size.lock().unwrap() += row_count;
    }

    pub fn rebuild_from_blocks(&self, block_data: Vec<SparseBlockData>) {
        let mut blocks = self.blocks.write().unwrap();
        blocks.clear();
        *self.size.lock().unwrap() = 0;
        *self.stale.lock().unwrap() = false;

        for data in block_data {
            let block = SparseBlock {
                block_id: data.block_id,
                zone_map: data.zone_map.or_else(|| Some(ZoneMap::null())),
                bloom_filter: data
                    .bloom_filter
                    .or_else(|| Some(BloomFilter::with_defaults())),
                start_row_id: data.start_row_id,
                row_count: data.row_count,
            };

            blocks.push(block);
            *self.size.lock().unwrap() += data.row_count;
        }
    }

    pub fn mark_stale(&self) {
        *self.stale.lock().unwrap() = true;
    }

    pub fn is_stale(&self) -> bool {
        *self.stale.lock().unwrap()
    }

    pub fn size(&self) -> i64 {
        *self.size.lock().unwrap()
    }

    pub fn block_count(&self) -> usize {
        self.blocks.read().unwrap().len()
    }

    pub fn get_block(&self, block_id: usize) -> Option<SparseBlock> {
        let blocks = self.blocks.read().unwrap();
        if block_id < blocks.len() {
            Some(blocks[block_id].clone())
        } else {
            None
        }
    }

    pub fn get_all_blocks(&self) -> Vec<SparseBlock> {
        self.blocks.read().unwrap().clone()
    }

    pub fn clear(&self) {
        let mut blocks = self.blocks.write().unwrap();
        blocks.clear();
        *self.size.lock().unwrap() = 0;
        *self.stale.lock().unwrap() = false;
    }

    pub fn serialize(&self) -> Vec<u8> {
        let blocks = self.blocks.read().unwrap();
        let mut buf = Vec::new();

        buf.extend_from_slice(&(blocks.len() as u64).to_le_bytes());

        for block in blocks.iter() {
            buf.extend_from_slice(&(block.block_id as u64).to_le_bytes());
            buf.extend_from_slice(&block.start_row_id.to_le_bytes());
            buf.extend_from_slice(&block.row_count.to_le_bytes());

            let has_zm = block.zone_map.is_some();
            buf.push(if has_zm { 1 } else { 0 });
            if let Some(zm) = &block.zone_map {
                buf.extend_from_slice(&zm.serialize());
            }

            let has_bf = block.bloom_filter.is_some();
            buf.push(if has_bf { 1 } else { 0 });
            if let Some(bf) = &block.bloom_filter {
                buf.extend_from_slice(&bf.serialize());
            }
        }

        buf
    }

    pub fn deserialize(data: &[u8]) -> Option<Self> {
        if data.len() < 8 {
            return None;
        }

        let num_blocks = u64::from_le_bytes(data[0..8].try_into().ok()?) as usize;
        let mut blocks = Vec::with_capacity(num_blocks);
        let mut offset = 8;

        for _ in 0..num_blocks {
            if offset + 24 > data.len() {
                return None;
            }

            let block_id = u64::from_le_bytes(data[offset..offset + 8].try_into().ok()?) as usize;
            offset += 8;
            let start_row_id = i64::from_le_bytes(data[offset..offset + 8].try_into().ok()?);
            offset += 8;
            let row_count = i64::from_le_bytes(data[offset..offset + 8].try_into().ok()?);
            offset += 8;

            let has_zm = data[offset] != 0;
            offset += 1;
            let zone_map = if has_zm {
                ZoneMap::deserialize(&data[offset..]).map(|(zm, len)| {
                    offset += len;
                    zm
                })
            } else {
                None
            };

            let has_bf = if offset < data.len() {
                data[offset] != 0
            } else {
                false
            };
            offset += 1;
            let bloom_filter = if has_bf && offset < data.len() {
                BloomFilter::deserialize(&data[offset..]).map(|bf| {
                    offset += bf.size_in_bytes() + 16;
                    bf
                })
            } else {
                None
            };

            blocks.push(SparseBlock {
                block_id,
                zone_map,
                bloom_filter,
                start_row_id,
                row_count,
            });
        }

        let size = blocks.iter().map(|b| b.row_count).sum();

        Some(Self {
            blocks: RwLock::new(blocks),
            size: Mutex::new(size),
            stale: Mutex::new(false),
        })
    }
}

impl Default for SparseIndexImpl {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sparse_index_basic() {
        let index = SparseIndexImpl::new();

        index.add_block(SparseBlock::new(0, 0, 8192));
        index.add_block(SparseBlock::new(1, 8192, 8192));

        index.insert(&ScalarValue::Int64(Some(42)), 100);
        index.insert(&ScalarValue::Int64(Some(100)), 9000);

        let results = index.search(&ScalarValue::Int64(Some(42)));
        assert!(!results.is_empty());
        assert!(results.contains(&100));
    }

    #[test]
    fn test_sparse_index_range_search() {
        let index = SparseIndexImpl::new();

        index.add_block(SparseBlock::new(0, 0, 8192));

        let results = index.range_search(
            &ScalarValue::Int64(Some(0)),
            &ScalarValue::Int64(Some(10000)),
        );
        assert!(!results.is_empty());
    }

    #[test]
    fn test_sparse_index_serialize_deserialize() {
        let index = SparseIndexImpl::new();

        index.add_block(SparseBlock::new(0, 0, 100));
        index.insert(&ScalarValue::Int64(Some(42)), 10);

        let serialized = index.serialize();
        let deserialized = SparseIndexImpl::deserialize(&serialized).unwrap();

        assert_eq!(deserialized.block_count(), 1);
        let results = deserialized.search(&ScalarValue::Int64(Some(42)));
        assert!(!results.is_empty());
    }
}
