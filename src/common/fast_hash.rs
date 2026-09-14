//! FxHash-family hasher shared by the hot store's string/intern pools and the
//! aggregate fast path's group interning (same construction, one impl, so
//! the two cannot drift apart).

use std::hash::Hasher;

/// Convenience alias for maps/sets built on [`FastHasher`].
pub(crate) type FastBuild = std::hash::BuildHasherDefault<FastHasher>;

#[derive(Default)]
pub(crate) struct FastHasher {
    hash: u64,
}

impl FastHasher {
    const SEED: u64 = 0x517c_c1b7_2722_0a95;

    #[inline]
    fn add(&mut self, word: u64) {
        self.hash = (self.hash.rotate_left(5) ^ word).wrapping_mul(Self::SEED);
    }

    /// Standalone string hash for the intern cache (nonzero by construction).
    #[inline]
    pub(crate) fn hash_str(s: &str) -> u64 {
        let mut h = FastHasher::default();
        h.write(s.as_bytes());
        h.write_u8(0xff);
        h.finish() | 1
    }
}

impl Hasher for FastHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.hash
    }

    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        let mut chunks = bytes.chunks_exact(8);
        for chunk in &mut chunks {
            self.add(u64::from_le_bytes(chunk.try_into().unwrap()));
        }
        let rem = chunks.remainder();
        if !rem.is_empty() {
            let mut buf = [0u8; 8];
            buf[..rem.len()].copy_from_slice(rem);
            // Mix the tail length so short tails cannot alias padded words.
            self.add(u64::from_le_bytes(buf) ^ ((rem.len() as u64) << 56));
        }
    }

    #[inline]
    fn write_u8(&mut self, i: u8) {
        self.add(i as u64);
    }

    #[inline]
    fn write_u16(&mut self, i: u16) {
        self.add(i as u64);
    }

    #[inline]
    fn write_u32(&mut self, i: u32) {
        self.add(i as u64);
    }

    #[inline]
    fn write_u64(&mut self, i: u64) {
        self.add(i);
    }

    #[inline]
    fn write_i8(&mut self, i: i8) {
        self.add(i as u64);
    }

    #[inline]
    fn write_i16(&mut self, i: i16) {
        self.add(i as u64);
    }

    #[inline]
    fn write_i32(&mut self, i: i32) {
        self.add(i as u64);
    }

    #[inline]
    fn write_i64(&mut self, i: i64) {
        self.add(i as u64);
    }

    #[inline]
    fn write_isize(&mut self, i: isize) {
        self.add(i as u64);
    }

    #[inline]
    fn write_usize(&mut self, i: usize) {
        self.add(i as u64);
    }
}
