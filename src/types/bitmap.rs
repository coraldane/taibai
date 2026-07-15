/// Arrow-style null bitmap: bit K = 1 means row K is NULL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NullBitmap {
    data: Vec<u64>,
    len: usize,
}

impl NullBitmap {
    pub fn new_valid(len: usize) -> Self {
        Self {
            data: vec![0u64; Self::u64_count(len)],
            len,
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(Self::u64_count(capacity)),
            len: 0,
        }
    }

    pub fn new_all_null(len: usize) -> Self {
        let u64_count = Self::u64_count(len);
        let mut data = vec![u64::MAX; u64_count];
        if len > 0 {
            let bits_in_last = len - (u64_count - 1) * 64;
            if bits_in_last < 64 {
                let last = u64_count - 1;
                data[last] = (1u64 << bits_in_last) - 1;
            }
        }
        Self { data, len }
    }

    pub fn from_bytes(bitmap_bytes: &[u8], len: usize) -> Self {
        let u64_count = Self::u64_count(len);
        let mut data = vec![0u64; u64_count];
        let chunks = bitmap_bytes.chunks_exact(8);
        for (i, chunk) in chunks.enumerate() {
            if i < u64_count {
                data[i] = u64::from_le_bytes(chunk.try_into().unwrap_or([0u8; 8]));
            }
        }
        Self { data, len }
    }

    pub fn from_bools(null_flags: &[bool]) -> Self {
        let len = null_flags.len();
        let mut data = vec![0u64; Self::u64_count(len)];
        for (i, &is_null) in null_flags.iter().enumerate() {
            if is_null {
                data[i / 64] |= 1u64 << (i % 64);
            }
        }
        Self { data, len }
    }

    #[inline]
    pub fn is_null(&self, idx: usize) -> bool {
        if self.len == 0 || idx >= self.len || self.data.is_empty() {
            return false;
        }
        let data_idx = idx / 64;
        if data_idx >= self.data.len() {
            return false;
        }
        self.data[data_idx] & (1u64 << (idx % 64)) != 0
    }

    #[inline]
    pub fn is_valid(&self, idx: usize) -> bool {
        !self.is_null(idx)
    }

    #[inline]
    pub fn set_null(&mut self, idx: usize) {
        if idx >= self.len || self.data.is_empty() {
            return;
        }
        let data_idx = idx / 64;
        if data_idx < self.data.len() {
            self.data[data_idx] |= 1u64 << (idx % 64);
        }
    }

    #[inline]
    pub fn set_valid(&mut self, idx: usize) {
        if idx >= self.len || self.data.is_empty() {
            return;
        }
        let data_idx = idx / 64;
        if data_idx < self.data.len() {
            self.data[data_idx] &= !(1u64 << (idx % 64));
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn null_count(&self) -> usize {
        self.data.iter().map(|w| w.count_ones() as usize).sum()
    }

    pub fn valid_count(&self) -> usize {
        self.len - self.null_count()
    }

    pub fn is_all_valid(&self) -> bool {
        for &word in &self.data {
            if word != 0 {
                return false;
            }
        }
        true
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.data.len() * 8);
        for &word in &self.data {
            buf.extend_from_slice(&word.to_le_bytes());
        }
        buf
    }

    /// Expand the bit-packed null map into one byte per row, where each byte is
    /// `1` when the row is NULL and `0` otherwise. The all-valid case (common
    /// for `NOT NULL` tick columns) is a single bulk memset instead of a per-row
    /// branch, which is what makes 5000-row result serialization cheap.
    pub fn null_flag_bytes(&self) -> Vec<u8> {
        let mut out = vec![0u8; self.len];
        if self.is_all_valid() {
            return out;
        }
        for i in 0..self.len {
            if self.is_null(i) {
                out[i] = 1;
            }
        }
        out
    }

    pub fn extend(&mut self, other: &NullBitmap) {
        if other.is_all_valid() {
            let needed = Self::u64_count(self.len + other.len);
            self.len += other.len;
            while self.data.len() < needed {
                self.data.push(0);
            }
            return;
        }
        let old_len = self.len;
        self.len += other.len;
        let needed = Self::u64_count(self.len);
        while self.data.len() < needed {
            self.data.push(0);
        }
        let other_offset = old_len % 64;
        if other_offset == 0 {
            let dst_start = old_len / 64;
            for (i, &word) in other.data.iter().enumerate() {
                if dst_start + i < self.data.len() {
                    self.data[dst_start + i] |= word;
                }
            }
        } else {
            for i in 0..other.len {
                if other.is_null(i) {
                    self.set_null(old_len + i);
                }
            }
        }
    }

    pub fn push_valid(&mut self) {
        self.len += 1;
        let needed = Self::u64_count(self.len);
        if self.data.len() < needed {
            self.data.push(0);
        }
    }

    pub fn push_null(&mut self) {
        self.len += 1;
        let needed = Self::u64_count(self.len);
        if self.data.len() < needed {
            self.data.push(0);
        }
        let idx = self.len - 1;
        self.data[idx / 64] |= 1u64 << (idx % 64);
    }

    #[inline]
    fn u64_count(len: usize) -> usize {
        len.div_ceil(64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_nulls() {
        let bm = NullBitmap::new_valid(100);
        for i in 0..100 {
            assert!(!bm.is_null(i));
            assert!(bm.is_valid(i));
        }
        assert_eq!(bm.null_count(), 0);
        assert_eq!(bm.valid_count(), 100);
        assert!(bm.is_all_valid());
    }

    #[test]
    fn test_all_nulls() {
        let bm = NullBitmap::new_all_null(100);
        for i in 0..100 {
            assert!(bm.is_null(i));
            assert!(!bm.is_valid(i));
        }
        assert_eq!(bm.null_count(), 100);
        assert!(!bm.is_all_valid());
    }

    #[test]
    fn test_mixed_nulls() {
        let flags = vec![false, true, false, false, true, true, false, true];
        let bm = NullBitmap::from_bools(&flags);
        assert!(!bm.is_null(0));
        assert!(bm.is_null(1));
        assert!(!bm.is_null(2));
        assert!(bm.is_null(4));
        assert_eq!(bm.null_count(), 4);
        assert_eq!(bm.valid_count(), 4);
        assert!(!bm.is_all_valid());
    }

    #[test]
    fn test_set_clear() {
        let mut bm = NullBitmap::new_valid(10);
        assert!(!bm.is_null(5));
        bm.set_null(5);
        assert!(bm.is_null(5));
        bm.set_valid(5);
        assert!(!bm.is_null(5));
    }

    #[test]
    fn test_roundtrip_bytes() {
        let flags: Vec<bool> = (0..200).map(|i| i % 3 == 0).collect();
        let bm = NullBitmap::from_bools(&flags);
        let bytes = bm.to_bytes();
        let bm2 = NullBitmap::from_bytes(&bytes, 200);
        for i in 0..200 {
            assert_eq!(bm.is_null(i), bm2.is_null(i), "mismatch at {}", i);
        }
    }

    #[test]
    fn test_extend() {
        let mut bm1 = NullBitmap::from_bools(&[true, false, true]);
        let bm2 = NullBitmap::from_bools(&[false, true]);
        bm1.extend(&bm2);
        assert_eq!(bm1.len(), 5);
        assert!(bm1.is_null(0));
        assert!(!bm1.is_null(1));
        assert!(bm1.is_null(2));
        assert!(!bm1.is_null(3));
        assert!(bm1.is_null(4));
    }
}
