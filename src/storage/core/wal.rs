use std::fs;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use crc32fast::Hasher;

use crate::common::error::{MonaxError, Result};

pub const FILE_PREFIX: &str = "wal_";
pub const FILE_SUFFIX: &str = ".log";

pub const HEADER_SIZE: usize = 25;

const WAL_BUF_CAPACITY: usize = 256 * 1024;

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
    BatchInsert = 8,
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
            8 => Ok(RecordType::BatchInsert),
            _ => Err(MonaxError::Storage(format!(
                "unknown WAL record type: {}",
                v
            ))),
        }
    }
}

#[derive(Debug, Clone)]
pub struct WalRecord {
    pub sequence: u64,
    pub record_type: RecordType,
    pub txn_id: u64,
    pub data: Vec<u8>,
}

impl WalRecord {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(HEADER_SIZE + self.data.len());
        encode_record_to(
            &mut buf,
            self.sequence,
            self.record_type,
            self.txn_id,
            &self.data,
        );
        buf
    }

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

        let computed_crc = compute_crc(&buf[..21], &data);
        if stored_crc != computed_crc {
            return Err(MonaxError::Storage(format!(
                "WAL CRC mismatch: stored={}, computed={}",
                stored_crc, computed_crc
            )));
        }

        let total_len = HEADER_SIZE + data_len;
        Ok((
            WalRecord {
                sequence,
                record_type,
                txn_id,
                data,
            },
            total_len,
        ))
    }
}

fn encode_record_to(buf: &mut Vec<u8>, seq: u64, rt: RecordType, txn_id: u64, data: &[u8]) {
    let start = buf.len();
    buf.extend_from_slice(&seq.to_le_bytes());
    buf.push(rt as u8);
    buf.extend_from_slice(&txn_id.to_le_bytes());
    buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
    buf.extend_from_slice(&[0u8; 4]);
    buf.extend_from_slice(data);

    let crc = compute_crc(&buf[start..start + 21], data);
    buf[start + 21..start + 25].copy_from_slice(&crc.to_le_bytes());
}

fn compute_crc(header_prefix: &[u8], data: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(header_prefix);
    hasher.update(data);
    hasher.finalize()
}

pub struct WalWriter {
    dir: PathBuf,
    file_index: u32,
    file: Option<BufWriter<fs::File>>,
    sequence: u64,
    current_size: u64,
    max_file_size: u64,
    batch_buf: Vec<u8>,
}

impl WalWriter {
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
            batch_buf: Vec::with_capacity(64 * 1024),
        };

        if file_index > 0 || !dir_has_wal_files(dir) {
            writer.rotate_file()?;
        } else {
            let path = wal_path(dir, file_index);
            if path.exists() {
                let file = fs::OpenOptions::new().append(true).open(&path)?;
                let meta = file.metadata()?;
                writer.current_size = meta.len();
                writer.file = Some(BufWriter::with_capacity(WAL_BUF_CAPACITY, file));
            } else {
                writer.rotate_file()?;
            }
        }

        Ok(writer)
    }

    pub fn write(&mut self, record_type: RecordType, txn_id: u64, data: &[u8]) -> Result<u64> {
        self.sequence += 1;
        self.batch_buf.clear();
        encode_record_to(
            &mut self.batch_buf,
            self.sequence,
            record_type,
            txn_id,
            data,
        );
        let buf = self.batch_buf.clone();
        self.write_raw(&buf)?;
        Ok(self.sequence)
    }

    pub fn write_batch(&mut self, txn_id: u64, records: &[(RecordType, &[u8])]) -> Result<u64> {
        self.batch_buf.clear();

        self.sequence += 1;
        encode_record_to(
            &mut self.batch_buf,
            self.sequence,
            RecordType::Begin,
            txn_id,
            &[],
        );

        for &(rt, data) in records {
            self.sequence += 1;
            encode_record_to(&mut self.batch_buf, self.sequence, rt, txn_id, data);
        }

        self.sequence += 1;
        encode_record_to(
            &mut self.batch_buf,
            self.sequence,
            RecordType::Commit,
            txn_id,
            &[],
        );

        let buf = self.batch_buf.clone();
        self.write_raw(&buf)?;
        self.sync()?;
        Ok(self.sequence)
    }

    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    pub fn sync(&mut self) -> Result<()> {
        if let Some(ref mut file) = self.file {
            file.flush()?;
            file.get_ref().sync_all()?;
        }
        Ok(())
    }

    pub fn write_batch_no_sync(
        &mut self,
        txn_id: u64,
        records: &[(RecordType, &[u8])],
    ) -> Result<u64> {
        self.batch_buf.clear();

        self.sequence += 1;
        encode_record_to(
            &mut self.batch_buf,
            self.sequence,
            RecordType::Begin,
            txn_id,
            &[],
        );

        for &(rt, data) in records {
            self.sequence += 1;
            encode_record_to(&mut self.batch_buf, self.sequence, rt, txn_id, data);
        }

        self.sequence += 1;
        encode_record_to(
            &mut self.batch_buf,
            self.sequence,
            RecordType::Commit,
            txn_id,
            &[],
        );

        let buf = self.batch_buf.clone();
        self.write_raw(&buf)?;
        Ok(self.sequence)
    }

    pub fn write_compact(&mut self, txn_id: u64, data: &[u8], sync: bool) -> Result<u64> {
        self.sequence += 1;
        self.batch_buf.clear();
        encode_record_to(
            &mut self.batch_buf,
            self.sequence,
            RecordType::BatchInsert,
            txn_id,
            data,
        );
        let buf = self.batch_buf.clone();
        self.write_raw(&buf)?;

        if sync {
            self.sync()?;
        }
        Ok(self.sequence)
    }

    pub fn clear(&mut self) -> Result<()> {
        if let Some(ref mut file) = self.file {
            let _ = file.flush();
            let _ = file.get_ref().sync_all();
        }
        self.file = None;
        self.current_size = 0;
        self.sequence = 0;

        let indices = list_wal_files(&self.dir)?;
        for idx in indices {
            let path = wal_path(&self.dir, idx);
            if let Err(e) = fs::remove_file(&path) {
                tracing::warn!(path = %path.display(), error = %e, "failed to remove WAL file");
            }
        }

        self.file_index = 0;
        self.rotate_file()?;

        tracing::debug!(dir = %self.dir.display(), "WAL cleared after segment persistence");
        Ok(())
    }

    pub fn clear_entries_for_txn(&mut self, txn_id: u64) -> Result<()> {
        self.write(RecordType::Abort, txn_id, &[])?;
        self.sync()?;
        tracing::debug!(txn_id, "WAL abort marker written");
        Ok(())
    }

    pub fn write_txn_marker(&mut self, txn_id: u64, record_type: RecordType) -> Result<()> {
        self.write(record_type, txn_id, &[])?;
        self.sync()?;
        Ok(())
    }

    fn write_raw(&mut self, data: &[u8]) -> Result<()> {
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
        if let Some(ref mut file) = self.file {
            let _ = file.flush();
            let _ = file.get_ref().sync_all();
        }
        self.file = None;
        self.current_size = 0;

        self.file_index += 1;
        let path = wal_path(&self.dir, self.file_index);

        let file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(&path)?;
        self.file = Some(BufWriter::with_capacity(WAL_BUF_CAPACITY, file));

        tracing::debug!(path = %path.display(), "rotated WAL file");
        Ok(())
    }
}

pub struct WalReader {
    dir: PathBuf,
}

impl WalReader {
    pub fn new(dir: &Path) -> Self {
        Self {
            dir: dir.to_path_buf(),
        }
    }

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

pub struct WalRecovery;

impl WalRecovery {
    pub fn replay(dir: &Path) -> Result<Vec<WalRecord>> {
        let reader = WalReader::new(dir);
        let all = reader.read_all()?;

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
                RecordType::BatchInsert => {
                    result.push(rec);
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
        encoded[22] ^= 0xFF;
        let result = WalRecord::decode(&encoded);
        assert!(result.is_err());
    }

    #[test]
    fn test_truncated_record() {
        let buf = vec![0u8; 10];
        let result = WalRecord::decode(&buf);
        assert!(result.is_err());
    }

    #[test]
    fn test_writer_and_reader() {
        let dir = TempDir::new().unwrap();
        let wal_dir = dir.path().join("wal");

        let mut writer = WalWriter::open(&wal_dir, 1024 * 1024).unwrap();

        let insert_data = vec![1, 2, 3];
        writer
            .write_batch(1, &[(RecordType::Insert, insert_data.as_slice())])
            .unwrap();
        assert!(writer.sequence() > 0);

        writer.write(RecordType::Insert, 2, &[4, 5, 6]).unwrap();

        drop(writer);

        let reader = WalReader::new(&wal_dir);
        let records = reader.read_all().unwrap();
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

        writer
            .write_batch(1, &[(RecordType::Insert, &[1, 2, 3])])
            .unwrap();

        writer.write(RecordType::Begin, 2, &[]).unwrap();
        writer.write(RecordType::Insert, 2, &[4, 5, 6]).unwrap();

        writer.write(RecordType::Begin, 3, &[]).unwrap();
        writer.write(RecordType::Insert, 3, &[7, 8, 9]).unwrap();
        writer.write(RecordType::Abort, 3, &[]).unwrap();

        drop(writer);

        let committed = WalRecovery::replay(&wal_dir).unwrap();
        assert_eq!(committed.len(), 1);
        assert_eq!(committed[0].txn_id, 1);
        assert_eq!(committed[0].data, vec![1, 2, 3]);
    }

    #[test]
    fn test_batch_insert_recovery() {
        let dir = TempDir::new().unwrap();
        let wal_dir = dir.path().join("wal");

        let mut writer = WalWriter::open(&wal_dir, 1024 * 1024).unwrap();

        writer.write_compact(1, &[10, 20, 30], false).unwrap();
        writer.write_compact(2, &[40, 50, 60], true).unwrap();

        drop(writer);

        let committed = WalRecovery::replay(&wal_dir).unwrap();
        assert_eq!(committed.len(), 2);
        assert_eq!(committed[0].record_type, RecordType::BatchInsert);
        assert_eq!(committed[0].data, vec![10, 20, 30]);
        assert_eq!(committed[1].data, vec![40, 50, 60]);
    }
}
