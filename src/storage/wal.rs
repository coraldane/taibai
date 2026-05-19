use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use crc32fast::Hasher;

use crate::common::error::{MonaxError, Result};

/// WAL file prefix and suffix.
pub const FILE_PREFIX: &str = "wal_";
pub const FILE_SUFFIX: &str = ".log";

/// WAL record header size: seq(8) + type(1) + txn_id(8) + data_len(4) + crc(4) = 25.
pub const HEADER_SIZE: usize = 25;

/// WAL record types, aligned with taibai.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RecordType {
    Begin = 1,
    Commit = 2,
    Abort = 3,
    Insert = 4,
    Delete = 5,
    Checkpoint = 6,
    CompactSwap = 7,
}

impl RecordType {
    pub fn from_u8(v: u8) -> Result<Self> {
        match v {
            1 => Ok(RecordType::Begin),
            2 => Ok(RecordType::Commit),
            3 => Ok(RecordType::Abort),
            4 => Ok(RecordType::Insert),
            5 => Ok(RecordType::Delete),
            6 => Ok(RecordType::Checkpoint),
            7 => Ok(RecordType::CompactSwap),
            _ => Err(MonaxError::Storage(format!("unknown WAL record type: {}", v))),
        }
    }
}

/// A single WAL record.
#[derive(Debug, Clone)]
pub struct WalRecord {
    pub sequence: u64,
    pub record_type: RecordType,
    pub txn_id: u64,
    pub data: Vec<u8>,
}

impl WalRecord {
    /// Encode this record into bytes: header (25 bytes) + data.
    /// Header: seq(8 LE) + type(1) + txn_id(8 LE) + data_len(4 LE) + crc32(4 LE).
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(HEADER_SIZE + self.data.len());
        buf.extend_from_slice(&self.sequence.to_le_bytes());
        buf.push(self.record_type as u8);
        buf.extend_from_slice(&self.txn_id.to_le_bytes());
        buf.extend_from_slice(&(self.data.len() as u32).to_le_bytes());
        // CRC placeholder
        buf.extend_from_slice(&[0u8; 4]);
        buf.extend_from_slice(&self.data);

        // Compute CRC32 over header[0..21] + data
        let crc = compute_crc(&buf[..21], &self.data);
        buf[21..25].copy_from_slice(&crc.to_le_bytes());

        buf
    }

    /// Decode a record from bytes. Returns the record and the number of bytes consumed.
    pub fn decode(buf: &[u8]) -> Result<(Self, usize)> {
        if buf.len() < HEADER_SIZE {
            return Err(MonaxError::Storage("WAL record too short".into()));
        }

        let sequence = u64::from_le_bytes(buf[0..8].try_into().unwrap());
        let record_type = RecordType::from_u8(buf[8])?;
        let txn_id = u64::from_le_bytes(buf[9..17].try_into().unwrap());
        let data_len = u32::from_le_bytes(buf[17..21].try_into().unwrap()) as usize;
        let stored_crc = u32::from_le_bytes(buf[21..25].try_into().unwrap());

        if buf.len() < HEADER_SIZE + data_len {
            return Err(MonaxError::Storage("WAL record truncated".into()));
        }

        let data = buf[HEADER_SIZE..HEADER_SIZE + data_len].to_vec();

        // Verify CRC
        let computed_crc = compute_crc(&buf[..21], &data);
        if stored_crc != computed_crc {
            return Err(MonaxError::Storage(format!(
                "WAL CRC mismatch: stored={}, computed={}",
                stored_crc, computed_crc
            )));
        }

        let total_len = HEADER_SIZE + data_len;
        Ok((WalRecord { sequence, record_type, txn_id, data }, total_len))
    }
}

/// Compute CRC32 over header prefix + data.
fn compute_crc(header_prefix: &[u8], data: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(header_prefix);
    hasher.update(data);
    hasher.finalize()
}

/// WAL writer: appends records to the current WAL file.
pub struct WalWriter {
    dir: PathBuf,
    file_index: u32,
    file: Option<fs::File>,
    sequence: u64,
    current_size: u64,
    max_file_size: u64,
}

impl WalWriter {
    /// Create or open a WAL writer in the given directory.
    /// If the directory exists and has WAL files, continues from the latest.
    pub fn open(dir: &Path, max_file_size: u64) -> Result<Self> {
        fs::create_dir_all(dir)?;

        let (file_index, sequence) = find_latest_wal(dir)?;

        let mut writer = Self {
            dir: dir.to_path_buf(),
            file_index,
            file: None,
            sequence,
            current_size: 0,
            max_file_size,
        };

        if file_index > 0 || !dir_has_wal_files(dir) {
            writer.rotate_file()?;
        } else {
            // Found existing files, open the latest for append
            let path = wal_path(dir, file_index);
            if path.exists() {
                let file = fs::OpenOptions::new().append(true).open(&path)?;
                let meta = file.metadata()?;
                writer.current_size = meta.len();
                writer.file = Some(file);
            } else {
                writer.rotate_file()?;
            }
        }

        Ok(writer)
    }

    /// Write a single record and optionally sync.
    pub fn write(&mut self, record_type: RecordType, txn_id: u64, data: &[u8]) -> Result<u64> {
        self.sequence += 1;
        let record = WalRecord {
            sequence: self.sequence,
            record_type,
            txn_id,
            data: data.to_vec(),
        };

        let encoded = record.encode();
        self.write_raw(&encoded)?;
        Ok(self.sequence)
    }

    /// Write a batch of records under a single transaction.
    /// Wraps with Begin + Commit, returns the commit sequence.
    pub fn write_batch(&mut self, txn_id: u64, records: &[(RecordType, &[u8])]) -> Result<u64> {
        self.sequence += 1;
        let begin = WalRecord {
            sequence: self.sequence,
            record_type: RecordType::Begin,
            txn_id,
            data: Vec::new(),
        };
        self.write_raw(&begin.encode())?;

        for &(rt, data) in records {
            self.sequence += 1;
            let rec = WalRecord {
                sequence: self.sequence,
                record_type: rt,
                txn_id,
                data: data.to_vec(),
            };
            self.write_raw(&rec.encode())?;
        }

        self.sequence += 1;
        let commit = WalRecord {
            sequence: self.sequence,
            record_type: RecordType::Commit,
            txn_id,
            data: Vec::new(),
        };
        self.write_raw(&commit.encode())?;

        self.sync()?;
        Ok(self.sequence)
    }

    /// Get the current sequence number.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Sync the current file to disk.
    pub fn sync(&mut self) -> Result<()> {
        if let Some(ref file) = self.file {
            file.sync_all()?;
        }
        Ok(())
    }

    /// Clear the WAL file after data has been safely persisted to segments.
    /// This prevents duplicate data on recovery.
    pub fn clear(&mut self) -> Result<()> {
        // Sync and close current file
        if let Some(ref file) = self.file {
            file.sync_all()?;
        }
        self.file = None;
        self.current_size = 0;
        self.sequence = 0;

        // Delete all WAL files in the directory
        let indices = list_wal_files(&self.dir)?;
        for idx in indices {
            let path = wal_path(&self.dir, idx);
            if let Err(e) = fs::remove_file(&path) {
                tracing::warn!(path = %path.display(), error = %e, "failed to remove WAL file");
            }
        }

        // Start a fresh WAL file
        self.file_index = 0;
        self.rotate_file()?;

        tracing::debug!(dir = %self.dir.display(), "WAL cleared after segment persistence");
        Ok(())
    }

    fn write_raw(&mut self, data: &[u8]) -> Result<()> {
        // Rotate if needed
        if self.current_size + data.len() as u64 > self.max_file_size && self.current_size > 0 {
            self.rotate_file()?;
        }

        if let Some(ref mut file) = self.file {
            file.write_all(data)?;
            self.current_size += data.len() as u64;
        }
        Ok(())
    }

    fn rotate_file(&mut self) -> Result<()> {
        // Sync and close current file
        if let Some(ref file) = self.file {
            file.sync_all()?;
        }
        self.file = None;
        self.current_size = 0;

        // Increment file index
        self.file_index += 1;
        let path = wal_path(&self.dir, self.file_index);

        let file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(&path)?;
        self.file = Some(file);

        tracing::debug!(path = %path.display(), "rotated WAL file");
        Ok(())
    }
}

/// WAL reader: reads all records from WAL files for recovery.
pub struct WalReader {
    dir: PathBuf,
}

impl WalReader {
    pub fn new(dir: &Path) -> Self {
        Self { dir: dir.to_path_buf() }
    }

    /// Read all valid records from all WAL files in order.
    pub fn read_all(&self) -> Result<Vec<WalRecord>> {
        let mut all_records = Vec::new();
        let indices = list_wal_files(&self.dir)?;

        for idx in indices {
            let path = wal_path(&self.dir, idx);
            let records = self.read_file(&path)?;
            all_records.extend(records);
        }

        Ok(all_records)
    }

    /// Read all records from a single WAL file.
    fn read_file(&self, path: &Path) -> Result<Vec<WalRecord>> {
        let data = fs::read(path)?;
        let mut records = Vec::new();
        let mut offset = 0;

        while offset < data.len() {
            match WalRecord::decode(&data[offset..]) {
                Ok((record, consumed)) => {
                    offset += consumed;
                    records.push(record);
                }
                Err(MonaxError::Storage(msg)) if msg.contains("truncated") => {
                    // Truncated record at end of file — ignore
                    break;
                }
                Err(e) => {
                    tracing::warn!(path = %path.display(), error = %e, "WAL read error, stopping");
                    break;
                }
            }
        }

        Ok(records)
    }
}

/// WAL recovery: replay committed transactions.
pub struct WalRecovery;

impl WalRecovery {
    /// Recover committed records from the WAL directory.
    /// Returns only the data records from committed transactions.
    pub fn replay(dir: &Path) -> Result<Vec<WalRecord>> {
        let reader = WalReader::new(dir);
        let all = reader.read_all()?;

        // Classify transactions
        let mut committed: std::collections::HashSet<u64> = std::collections::HashSet::new();
        let mut aborted: std::collections::HashSet<u64> = std::collections::HashSet::new();

        for rec in &all {
            match rec.record_type {
                RecordType::Commit => {
                    committed.insert(rec.txn_id);
                }
                RecordType::Abort => {
                    aborted.insert(rec.txn_id);
                }
                _ => {}
            }
        }

        // Collect data records from committed transactions
        let mut result = Vec::new();
        let mut in_committed = false;

        for rec in all {
            match rec.record_type {
                RecordType::Begin => {
                    in_committed = committed.contains(&rec.txn_id);
                }
                RecordType::Commit | RecordType::Abort => {
                    in_committed = false;
                }
                RecordType::Insert | RecordType::Delete | RecordType::Checkpoint => {
                    if in_committed {
                        result.push(rec);
                    }
                }
                RecordType::CompactSwap => {
                    if in_committed {
                        result.push(rec);
                    }
                }
            }
        }

        Ok(result)
    }
}

fn wal_path(dir: &Path, index: u32) -> PathBuf {
    dir.join(format!("{}{:08}{}", FILE_PREFIX, index, FILE_SUFFIX))
}

fn find_latest_wal(dir: &Path) -> Result<(u32, u64)> {
    let indices = list_wal_files(dir)?;
    if indices.is_empty() {
        return Ok((0, 0));
    }

    let latest = *indices.last().unwrap();
    let path = wal_path(dir, latest);
    let max_seq = find_max_sequence(&path)?;
    Ok((latest, max_seq))
}

fn list_wal_files(dir: &Path) -> Result<Vec<u32>> {
    let entries = fs::read_dir(dir)?;
    let mut indices = Vec::new();
    for entry in entries {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with(FILE_PREFIX) && name.ends_with(FILE_SUFFIX) {
            let num_str = &name[FILE_PREFIX.len()..name.len() - FILE_SUFFIX.len()];
            if let Ok(idx) = num_str.parse::<u32>() {
                indices.push(idx);
            }
        }
    }
    indices.sort();
    Ok(indices)
}

fn dir_has_wal_files(dir: &Path) -> bool {
    list_wal_files(dir).map(|v| !v.is_empty()).unwrap_or(false)
}

fn find_max_sequence(path: &Path) -> Result<u64> {
    if !path.exists() {
        return Ok(0);
    }
    let data = fs::read(path)?;
    let mut max_seq = 0u64;
    let mut offset = 0;
    while offset + HEADER_SIZE <= data.len() {
        match WalRecord::decode(&data[offset..]) {
            Ok((rec, consumed)) => {
                max_seq = max_seq.max(rec.sequence);
                offset += consumed;
            }
            Err(_) => break,
        }
    }
    Ok(max_seq)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_record_encode_decode() {
        let rec = WalRecord {
            sequence: 42,
            record_type: RecordType::Insert,
            txn_id: 1,
            data: vec![1, 2, 3, 4, 5],
        };
        let encoded = rec.encode();
        assert_eq!(encoded.len(), HEADER_SIZE + 5);

        let (decoded, consumed) = WalRecord::decode(&encoded).unwrap();
        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded.sequence, 42);
        assert_eq!(decoded.record_type, RecordType::Insert);
        assert_eq!(decoded.txn_id, 1);
        assert_eq!(decoded.data, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_crc_validation() {
        let rec = WalRecord {
            sequence: 1,
            record_type: RecordType::Commit,
            txn_id: 0,
            data: vec![],
        };
        let mut encoded = rec.encode();
        // Corrupt the CRC
        encoded[22] ^= 0xFF;
        let result = WalRecord::decode(&encoded);
        assert!(result.is_err());
    }

    #[test]
    fn test_truncated_record() {
        let buf = vec![0u8; 10]; // Less than HEADER_SIZE
        let result = WalRecord::decode(&buf);
        assert!(result.is_err());
    }

    #[test]
    fn test_writer_and_reader() {
        let dir = TempDir::new().unwrap();
        let wal_dir = dir.path().join("wal");

        let mut writer = WalWriter::open(&wal_dir, 1024 * 1024).unwrap();

        // Write a batch
        let insert_data = vec![1, 2, 3];
        writer.write_batch(1, &[(RecordType::Insert, insert_data.as_slice())]).unwrap();
        assert!(writer.sequence() > 0);

        // Write another record
        writer.write(RecordType::Insert, 2, &[4, 5, 6]).unwrap();

        // Drop the writer to flush
        drop(writer);

        // Read back
        let reader = WalReader::new(&wal_dir);
        let records = reader.read_all().unwrap();
        // Begin + Insert + Commit + Insert = 4 records
        assert_eq!(records.len(), 4);
        assert_eq!(records[0].record_type, RecordType::Begin);
        assert_eq!(records[1].record_type, RecordType::Insert);
        assert_eq!(records[2].record_type, RecordType::Commit);
        assert_eq!(records[3].record_type, RecordType::Insert);
    }

    #[test]
    fn test_recovery_only_committed() {
        let dir = TempDir::new().unwrap();
        let wal_dir = dir.path().join("wal");

        let mut writer = WalWriter::open(&wal_dir, 1024 * 1024).unwrap();

        // Committed transaction
        writer.write_batch(1, &[(RecordType::Insert, &[1, 2, 3])]).unwrap();

        // Uncommitted transaction (write begin + insert, but no commit)
        writer.write(RecordType::Begin, 2, &[]).unwrap();
        writer.write(RecordType::Insert, 2, &[4, 5, 6]).unwrap();

        // Aborted transaction
        writer.write(RecordType::Begin, 3, &[]).unwrap();
        writer.write(RecordType::Insert, 3, &[7, 8, 9]).unwrap();
        writer.write(RecordType::Abort, 3, &[]).unwrap();

        drop(writer);

        let committed = WalRecovery::replay(&wal_dir).unwrap();
        // Only the Insert from txn 1 should be recovered
        assert_eq!(committed.len(), 1);
        assert_eq!(committed[0].txn_id, 1);
        assert_eq!(committed[0].data, vec![1, 2, 3]);
    }
}
