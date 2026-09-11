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
mod tests {
    use super::*;

    #[test]
    fn bit_roundtrip_mixed() {
        let mut w = BitWriter::with_capacity(4);
        w.write_bit(true);
        w.write_bit(false);
        w.write_bits(0b1011_0110, 8);
        w.write_bits(u64::MAX, 64);
        w.write_bits(1, 3);
        let bytes = w.finish();

        let mut r = BitReader::new(&bytes);
        assert!(r.read_bit().unwrap());
        assert!(!r.read_bit().unwrap());
        assert_eq!(r.read_bits(8).unwrap(), 0b1011_0110);
        assert_eq!(r.read_bits(64).unwrap(), u64::MAX);
        assert_eq!(r.read_bits(3).unwrap(), 1);
    }

    #[test]
    fn bit_reader_reports_exhaustion() {
        let mut w = BitWriter::with_capacity(1);
        w.write_bits(0b1010_1010, 8);
        let bytes = w.finish();
        assert_eq!(bytes.len(), 1);
        let mut r = BitReader::new(&bytes);
        for _ in 0..8 {
            assert!(r.read_bit().is_ok());
        }
        assert!(r.read_bit().is_err());
    }

    #[test]
    fn partial_byte_is_flushed() {
        let mut w = BitWriter::with_capacity(1);
        w.write_bits(0b1, 1);
        let bytes = w.finish();
        assert_eq!(bytes.len(), 1);
        assert_eq!(bytes[0], 1);
    }
}
