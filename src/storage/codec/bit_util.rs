use crate::common::error::{Result, TaibaiError};

pub struct BitWriter {
    buf: Vec<u8>,
    cur: u8,
    cur_bits: u32,
}

impl BitWriter {
    pub fn with_capacity(bytes: usize) -> Self {
        Self {
            buf: Vec::with_capacity(bytes),
            cur: 0,
            cur_bits: 0,
        }
    }

    pub fn write_bit(&mut self, bit: bool) {
        if bit {
            self.cur |= 1 << self.cur_bits;
        }
        self.cur_bits += 1;
        if self.cur_bits == 8 {
            self.buf.push(self.cur);
            self.cur = 0;
            self.cur_bits = 0;
        }
    }

    pub fn write_bits(&mut self, value: u64, count: u32) {
        for i in 0..count {
            self.write_bit((value >> i) & 1 == 1);
        }
    }

    pub fn finish(mut self) -> Vec<u8> {
        if self.cur_bits > 0 {
            self.buf.push(self.cur);
        }
        self.buf
    }
}

pub struct BitReader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> BitReader<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    pub fn read_bit(&mut self) -> Result<bool> {
        let byte_idx = self.pos / 8;
        if byte_idx >= self.data.len() {
            return Err(TaibaiError::Storage("bit reader exhausted".into()));
        }
        let bit = (self.data[byte_idx] >> (self.pos % 8)) & 1 == 1;
        self.pos += 1;
        Ok(bit)
    }

    pub fn read_bits(&mut self, count: u32) -> Result<u64> {
        let mut value = 0u64;
        for i in 0..count {
            if self.read_bit()? {
                value |= 1 << i;
            }
        }
        Ok(value)
    }
}

#[cfg(test)]
#[path = "bit_util_test.rs"]
mod tests;
