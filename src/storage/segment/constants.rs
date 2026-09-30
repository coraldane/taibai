pub const SEGMENT_MAGIC: u32 = 0x5441_4958;

/// On-disk segment format version written by this binary. The reader
/// accepts every version in `SUPPORTED_SEGMENT_VERSIONS`, so upgrades
/// keep reading data written by older binaries; the reverse direction
/// (old binary, new segments) fails loudly at the header gate. Run the
/// `migrate` tool when rolling forward, and back up the data directory
/// before upgrading.
pub const SEGMENT_VERSION: u32 = 2;

/// Header versions this binary can read.
pub const SUPPORTED_SEGMENT_VERSIONS: [u32; 2] = [1, 2];

pub fn is_supported_segment_version(version: u32) -> bool {
    SUPPORTED_SEGMENT_VERSIONS.contains(&version)
}
pub const DEFAULT_ROW_GROUP_SIZE: usize = 8192;
/// Maximum number of values in one independently compressed column page.
/// The on-disk v1 reader already addresses these units as row groups, so
/// capping the physical group size gives page-level decompression and pruning
/// without a format break for existing segments.
pub const MAX_COLUMN_PAGE_ROWS: usize = 16_384;
pub const FOOTER_SIZE: usize = 4096;
pub const SEGMENT_HEADER_SIZE: usize = 32;
