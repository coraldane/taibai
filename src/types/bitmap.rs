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

    pub fn estimated_size_bytes(&self) -> usize {
        self.data.capacity() * std::mem::size_of::<u64>()
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
        for (i, flag) in out.iter_mut().enumerate() {
            if self.is_null(i) {
                *flag = 1;
            }
        }
        out
    }

    pub fn extend(&mut self, other: &NullBitmap) {
        if other.len == 0 {
            return;
        }
        if other.is_all_valid() {
            let needed = Self::u64_count(self.len + other.len);
            self.len += other.len;
            self.data.resize(needed, 0);
            return;
        }
        let old_len = self.len;
        self.len += other.len;
        let needed = Self::u64_count(self.len);
        self.data.resize(needed, 0);

        // Merge whole machine words. The previous implementation fell back to
        // one is_null/set_null pair per row whenever the destination was not
        // 64-bit aligned, which made many small nullable INSERTs needlessly
        // branch-heavy.
        let dst_start = old_len / 64;
        let shift = old_len % 64;
        if shift != 0 {
            // Bits above the old logical length are padding and must not turn
            // into real NULLs after the append (notably for from_bytes input).
            self.data[dst_start] &= (1u64 << shift) - 1;
        }
        for (i, &word) in other.data.iter().enumerate() {
            let dst = dst_start + i;
            self.data[dst] |= word << shift;
            if shift != 0 && dst + 1 < self.data.len() {
                self.data[dst + 1] |= word >> (64 - shift);
            }
        }

        // Preserve the canonical invariant that unused tail bits are zero.
        let tail_bits = self.len % 64;
        if tail_bits != 0 {
            let mask = (1u64 << tail_bits) - 1;
            if let Some(last) = self.data.last_mut() {
                *last &= mask;
            }
        }
    }

    /// Append `n` valid rows in one step. Gathers from null-free source
    /// columns use this to keep the bitmap work O(n/64) instead of a branch
    /// per row.
    pub fn extend_valid(&mut self, n: usize) {
        if n == 0 {
            return;
        }
        self.len += n;
        let needed = Self::u64_count(self.len);
        if self.data.len() < needed {
            self.data.resize(needed, 0);
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
#[path = "bitmap_test.rs"]
mod tests;
