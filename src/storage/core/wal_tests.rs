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
