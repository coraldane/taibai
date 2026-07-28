#![allow(dead_code)]
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use crate::types::scalar::ScalarValue;

const DEFAULT_FALSE_POSITIVE_RATE: f64 = 0.01;
const DEFAULT_EXPECTED_ITEMS: usize = 8192;

#[derive(Debug, Clone)]
pub struct BloomFilter {
    bits: Vec<u8>,
    num_bits: usize,
    num_hashes: usize,
}

impl BloomFilter {
    pub fn new(expected_items: usize, false_positive_rate: f64) -> Self {
        let num_bits = Self::optimal_num_bits(expected_items, false_positive_rate);
        let num_hashes = Self::optimal_num_hashes(expected_items, num_bits);

        Self {
            bits: vec![0u8; num_bits.div_ceil(8)],
            num_bits,
            num_hashes,
        }
    }

    pub fn with_defaults() -> Self {
        Self::new(DEFAULT_EXPECTED_ITEMS, DEFAULT_FALSE_POSITIVE_RATE)
    }

    fn optimal_num_bits(n: usize, p: f64) -> usize {
        if n == 0 || p <= 0.0 || p >= 1.0 {
            return 1024;
        }
        let m = -((n as f64) * p.ln()) / (2.0_f64.ln().powi(2));
        m.ceil() as usize
    }

    fn optimal_num_hashes(n: usize, m: usize) -> usize {
        if n == 0 || m == 0 {
            return 3;
        }
        let k = ((m as f64) / (n as f64)) * 2.0_f64.ln();
        k.round() as usize
    }

    pub fn add(&mut self, value: &ScalarValue) {
        if value.is_null() {
            return;
        }

        let hash1 = self.hash_value(value, 0);
        let hash2 = self.hash_value(value, hash1);

        self.add_hashes(hash1, hash2);
    }

    /// Add a native typed value without first boxing it into `ScalarValue`.
    /// `type_tag` must match `hash_scalar`; the segment metadata kernels use
    /// this on the write path while lookups remain wire-compatible with
    /// `might_contain(ScalarValue)`.
    pub(crate) fn add_typed<T: Hash + ?Sized>(&mut self, type_tag: u8, value: &T) {
        let hash1 = self.hash_typed(type_tag, value, 0);
        let hash2 = self.hash_typed(type_tag, value, hash1);
        self.add_hashes(hash1, hash2);
    }

    pub(crate) fn add_vector(&mut self, value: &[f32]) {
        let hash_value = |seed: u64| {
            let mut hasher = DefaultHasher::new();
            seed.hash(&mut hasher);
            18u8.hash(&mut hasher);
            value.len().hash(&mut hasher);
            for item in value {
                item.to_bits().hash(&mut hasher);
            }
            hasher.finish()
        };
        let hash1 = hash_value(0);
        let hash2 = hash_value(hash1);
        self.add_hashes(hash1, hash2);
    }

    #[inline]
    fn add_hashes(&mut self, hash1: u64, hash2: u64) {
        for i in 0..self.num_hashes {
            let combined_hash = hash1.wrapping_add((i as u64).wrapping_mul(hash2));
            let bit_index = combined_hash % (self.num_bits as u64);
            self.set_bit(bit_index as usize);
        }
    }

    #[inline]
    fn hash_typed<T: Hash + ?Sized>(&self, type_tag: u8, value: &T, seed: u64) -> u64 {
        let mut hasher = DefaultHasher::new();
        seed.hash(&mut hasher);
        type_tag.hash(&mut hasher);
        value.hash(&mut hasher);
        hasher.finish()
    }

    pub fn might_contain(&self, value: &ScalarValue) -> bool {
        if value.is_null() {
            return true;
        }

        let hash1 = self.hash_value(value, 0);
        let hash2 = self.hash_value(value, hash1);

        for i in 0..self.num_hashes {
            let combined_hash = hash1.wrapping_add((i as u64).wrapping_mul(hash2));
            let bit_index = combined_hash % (self.num_bits as u64);
            if !self.get_bit(bit_index as usize) {
                return false;
            }
        }
        true
    }

    pub fn merge(&mut self, other: &BloomFilter) {
        if self.num_bits != other.num_bits {
            return;
        }

        for (i, &byte) in other.bits.iter().enumerate() {
            if i < self.bits.len() {
                self.bits[i] |= byte;
            }
        }
    }

    fn hash_value(&self, value: &ScalarValue, seed: u64) -> u64 {
        let mut hasher = DefaultHasher::new();
        seed.hash(&mut hasher);
        self.hash_scalar(value, &mut hasher);
        hasher.finish()
    }

    fn hash_scalar(&self, value: &ScalarValue, hasher: &mut DefaultHasher) {
        match value {
            ScalarValue::Bool(Some(v)) => {
                0u8.hash(hasher);
                v.hash(hasher);
            }
            ScalarValue::Int8(Some(v)) => {
                1u8.hash(hasher);
                v.hash(hasher);
            }
            ScalarValue::Int16(Some(v)) => {
                2u8.hash(hasher);
                v.hash(hasher);
            }
            ScalarValue::Int32(Some(v)) => {
                3u8.hash(hasher);
                v.hash(hasher);
            }
            ScalarValue::Int64(Some(v)) => {
                4u8.hash(hasher);
                v.hash(hasher);
            }
            ScalarValue::UInt8(Some(v)) => {
                5u8.hash(hasher);
                v.hash(hasher);
            }
            ScalarValue::UInt16(Some(v)) => {
                6u8.hash(hasher);
                v.hash(hasher);
            }
            ScalarValue::UInt32(Some(v)) => {
                7u8.hash(hasher);
                v.hash(hasher);
            }
            ScalarValue::UInt64(Some(v)) => {
                8u8.hash(hasher);
                v.hash(hasher);
            }
            ScalarValue::Float32(Some(v)) => {
                9u8.hash(hasher);
                v.to_bits().hash(hasher);
            }
            ScalarValue::Float64(Some(v)) => {
                10u8.hash(hasher);
                v.to_bits().hash(hasher);
            }
            ScalarValue::Decimal(Some(v)) => {
                11u8.hash(hasher);
                v.hash(hasher);
            }
            ScalarValue::Date(Some(v)) => {
                12u8.hash(hasher);
                v.hash(hasher);
            }
            ScalarValue::Time(Some(v)) => {
                13u8.hash(hasher);
                v.hash(hasher);
            }
            ScalarValue::Timestamp(Some(v)) => {
                14u8.hash(hasher);
                v.hash(hasher);
            }
            ScalarValue::Interval(Some(v)) => {
                15u8.hash(hasher);
                v.hash(hasher);
            }
            ScalarValue::VarChar(Some(v)) => {
                16u8.hash(hasher);
                v.hash(hasher);
            }
            ScalarValue::Blob(Some(v)) => {
                17u8.hash(hasher);
                v.hash(hasher);
            }
            ScalarValue::Vector(Some(v)) => {
                18u8.hash(hasher);
                v.len().hash(hasher);
                for f in v {
                    f.to_bits().hash(hasher);
                }
            }
            _ => {}
        }
    }

    fn set_bit(&mut self, index: usize) {
        let byte_index = index / 8;
        let bit_offset = index % 8;
        if byte_index < self.bits.len() {
            self.bits[byte_index] |= 1 << bit_offset;
        }
    }

    fn get_bit(&self, index: usize) -> bool {
        let byte_index = index / 8;
        let bit_offset = index % 8;
        if byte_index < self.bits.len() {
            (self.bits[byte_index] & (1 << bit_offset)) != 0
        } else {
            false
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(8 + self.bits.len());
        buf.extend_from_slice(&(self.num_bits as u64).to_le_bytes());
        buf.extend_from_slice(&(self.num_hashes as u64).to_le_bytes());
        buf.extend_from_slice(&self.bits);
        buf
    }

    pub fn deserialize(data: &[u8]) -> Option<Self> {
        if data.len() < 16 {
            return None;
        }

        let num_bits = u64::from_le_bytes(data[0..8].try_into().ok()?) as usize;
        let num_hashes = u64::from_le_bytes(data[8..16].try_into().ok()?) as usize;
        let bits_len = num_bits.div_ceil(8);

        if data.len() < 16 + bits_len {
            return None;
        }

        Some(Self {
            bits: data[16..16 + bits_len].to_vec(),
            num_bits,
            num_hashes,
        })
    }

    pub fn clear(&mut self) {
        for byte in self.bits.iter_mut() {
            *byte = 0;
        }
    }

    pub fn is_empty(&self) -> bool {
        self.bits.iter().all(|&b| b == 0)
    }

    pub fn size_in_bytes(&self) -> usize {
        self.bits.len()
    }
}

#[cfg(test)]
#[path = "bloomfilter_tests.rs"]
mod tests;
