#![allow(dead_code)]
pub const SEGMENT_MAGIC: u32 = 0x5441_4958;
pub const SEGMENT_VERSION: u32 = 1;
pub const DEFAULT_ROW_GROUP_SIZE: usize = 8192;
pub const FOOTER_SIZE: usize = 4096;
pub const SEGMENT_HEADER_SIZE: usize = 32;
pub const MAX_SEGMENT_SIZE: u64 = 2 * 1024 * 1024 * 1024;
