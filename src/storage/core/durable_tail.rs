use std::collections::HashMap;
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use crc32fast::Hasher;
use uuid::Uuid;

use crate::common::error::{Result, TaibaiError};
use crate::sql::execution::wal_helper::{deserialize_batch_from_wal, serialize_batch_for_wal};
use crate::storage::segment::SegmentReader;
use crate::types::batch::{RecordBatch, Schema};

const TAIL_MAGIC: u32 = 0x5441_494C;
const TAIL_LOG_MAGIC: u32 = 0x544C_4F47;
const FLUSH_MAGIC: u32 = 0x464C_5553;
const TAIL_DIR: &str = "memtable_tail";
static TAIL_SEQUENCE: AtomicU64 = AtomicU64::new(0);

pub struct RecoveredTail {
    pub id: Uuid,
    pub partition_key: String,
    pub batch: RecordBatch,
}

pub struct FlushIntent {
    id: Uuid,
    marker_path: PathBuf,
}

#[derive(Clone)]
enum GenerationKind {
    Tail,
    Clear,
    Flush,
}

#[derive(Clone)]
struct Generation {
    sequence: u64,
    path: PathBuf,
    kind: GenerationKind,
}

pub fn persist(table_dir: &Path, id: Uuid, partition_key: &str, batch: &RecordBatch) -> Result<()> {
    let tail_dir = table_dir.join(TAIL_DIR);
    fs::create_dir_all(&tail_dir)?;
    let sequence = next_sequence();
    let payload = encode_tail_log(partition_key, batch);
    let stem = format!("{}.{}", id.simple(), sequence);
    let tmp_path = tail_dir.join(format!(".{stem}.tmp"));
    let final_path = tail_dir.join(format!("{stem}.tail"));

    write_new_synced(&tmp_path, &payload)?;
    fs::rename(&tmp_path, &final_path)?;
    remove_other_generations(&tail_dir, id, &final_path)?;
    Ok(())
}

/// Durably append only the newly inserted rows to an existing memtable tail.
///
/// The previous implementation rewrote the complete accumulated memtable for
/// every INSERT. Repeated single-row INSERTs therefore produced quadratic I/O
/// and one fsync/rename/delete cycle per ever-growing snapshot. A tail log keeps
/// the same acknowledgement durability while writing only the statement delta.
pub fn append(table_dir: &Path, id: Uuid, partition_key: &str, delta: &RecordBatch) -> Result<()> {
    if delta.num_rows() == 0 {
        return Ok(());
    }
    let tail_dir = table_dir.join(TAIL_DIR);
    fs::create_dir_all(&tail_dir)?;
    let existing = newest_live_tail_path(&tail_dir, id)?;
    let frame = encode_tail_frame(delta);

    if let Some(path) = existing {
        if read_tail_log_partition_key(&path)?.as_deref() == Some(partition_key) {
            let mut file = fs::OpenOptions::new().append(true).open(&path)?;
            file.write_all(&frame)?;
            file.sync_all()?;
            return Ok(());
        }

        // One-time migration for tails written by versions that stored a full
        // snapshot. Keep the old generation until the new log is durable.
        let bytes = fs::read(&path)?;
        let (old_partition_key, old_batch) = decode_tail(&bytes, &delta.schema)?;
        if old_partition_key != partition_key {
            return Err(TaibaiError::Storage(
                "durable tail partition key changed while appending".into(),
            ));
        }
        let sequence = next_sequence();
        let stem = format!("{}.{}", id.simple(), sequence);
        let tmp_path = tail_dir.join(format!(".{stem}.tmp"));
        let final_path = tail_dir.join(format!("{stem}.tail"));
        let mut payload = encode_tail_log(partition_key, &old_batch);
        payload.extend_from_slice(&frame);
        write_new_synced(&tmp_path, &payload)?;
        fs::rename(&tmp_path, &final_path)?;
        remove_other_generations(&tail_dir, id, &final_path)?;
        return Ok(());
    }

    let sequence = next_sequence();
    let stem = format!("{}.{}", id.simple(), sequence);
    let tmp_path = tail_dir.join(format!(".{stem}.tmp"));
    let final_path = tail_dir.join(format!("{stem}.tail"));
    write_new_synced(&tmp_path, &encode_tail_log(partition_key, delta))?;
    fs::rename(&tmp_path, &final_path)?;
    Ok(())
}

/// Persist a tombstone before removing a tail. Keeping the tombstone makes a
/// failed cleanup fail closed: an older tail cannot be resurrected on restart.
pub fn clear(table_dir: &Path, id: Uuid) -> Result<()> {
    let tail_dir = table_dir.join(TAIL_DIR);
    if !tail_dir.exists() {
        return Ok(());
    }
    let sequence = next_sequence();
    let marker = tail_dir.join(format!("{}.{}.clear", id.simple(), sequence));
    write_new_synced(&marker, &TAIL_MAGIC.to_le_bytes())?;
    remove_other_generations(&tail_dir, id, &marker)?;
    Ok(())
}

pub fn has_tail(table_dir: &Path, id: Uuid) -> bool {
    let Ok(mut generations) = generations_for_id(&table_dir.join(TAIL_DIR), id) else {
        return false;
    };
    generations.sort_by_key(|generation| std::cmp::Reverse(generation.sequence));
    for generation in generations {
        match generation.kind {
            GenerationKind::Tail => return true,
            GenerationKind::Clear => return false,
            GenerationKind::Flush => {
                let Ok(bytes) = fs::read(&generation.path) else {
                    continue;
                };
                let Ok(segment_name) = decode_flush_target(&bytes) else {
                    continue;
                };
                let segment_path = table_dir.join(segment_name);
                if segment_path.is_file() && SegmentReader::open(&segment_path).is_ok() {
                    return false;
                }
            }
        }
    }
    false
}

/// Record the destination before a durable tail is copied into a sealed
/// segment. Recovery can then distinguish "segment rename completed" from
/// "flush never reached the atomic rename", avoiding duplicate rows.
pub fn begin_flush(table_dir: &Path, id: Uuid, segment_file_name: &str) -> Result<FlushIntent> {
    if Path::new(segment_file_name)
        .file_name()
        .and_then(|name| name.to_str())
        != Some(segment_file_name)
    {
        return Err(TaibaiError::Storage(
            "durable tail flush target must be a file name".into(),
        ));
    }
    let tail_dir = table_dir.join(TAIL_DIR);
    fs::create_dir_all(&tail_dir)?;
    let marker_path = tail_dir.join(format!("{}.{}.flush", id.simple(), next_sequence()));
    write_new_synced(&marker_path, &encode_flush_target(segment_file_name))?;
    Ok(FlushIntent { id, marker_path })
}

/// Remove the source tail first and the intent last. If cleanup is interrupted,
/// the surviving intent still points recovery at the already durable segment.
pub fn finish_flush(table_dir: &Path, intent: FlushIntent) -> Result<()> {
    let tail_dir = table_dir.join(TAIL_DIR);
    remove_other_generations(&tail_dir, intent.id, &intent.marker_path)?;
    remove_file_if_exists(&intent.marker_path)?;
    remove_empty_dir(&tail_dir);
    Ok(())
}

pub fn cancel_flush(intent: FlushIntent) -> Result<()> {
    remove_file_if_exists(&intent.marker_path)
}

pub fn recover(table_dir: &Path, schema: &Schema) -> Result<Vec<RecoveredTail>> {
    let tail_dir = table_dir.join(TAIL_DIR);
    if !tail_dir.exists() {
        return Ok(Vec::new());
    }
    let mut by_id = HashMap::<Uuid, Vec<Generation>>::new();
    for entry in fs::read_dir(&tail_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) == Some("tmp") {
            let _ = fs::remove_file(path);
            continue;
        }
        let Some((id, generation)) = parse_generation(&path) else {
            continue;
        };
        by_id.entry(id).or_default().push(generation);
    }

    let mut recovered = Vec::new();
    for (id, mut generations) in by_id {
        generations.sort_by_key(|generation| std::cmp::Reverse(generation.sequence));
        let mut recovered_tail = None;
        for generation in &generations {
            match generation.kind {
                GenerationKind::Tail => {
                    let bytes = fs::read(&generation.path)?;
                    let (partition_key, batch) = decode_tail(&bytes, schema)?;
                    recovered_tail = Some((generation.path.clone(), partition_key, batch));
                    break;
                }
                GenerationKind::Clear => break,
                GenerationKind::Flush => {
                    let segment_name = decode_flush_target(&fs::read(&generation.path)?)?;
                    let segment_path = table_dir.join(segment_name);
                    if segment_path.is_file() && SegmentReader::open(&segment_path).is_ok() {
                        remove_other_generations(&tail_dir, id, &generation.path)?;
                        remove_file_if_exists(&generation.path)?;
                        break;
                    }
                    // The atomic segment rename did not complete. Ignore this
                    // intent and continue to the preceding durable tail.
                    let _ = remove_file_if_exists(&generation.path);
                }
            }
        }
        if let Some((path, partition_key, batch)) = recovered_tail {
            remove_other_generations(&tail_dir, id, &path)?;
            recovered.push(RecoveredTail {
                id,
                partition_key,
                batch,
            });
        }
    }
    remove_empty_dir(&tail_dir);
    Ok(recovered)
}

fn write_new_synced(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

fn encode_tail_log(partition_key: &str, batch: &RecordBatch) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(12 + partition_key.len());
    bytes.extend_from_slice(&TAIL_LOG_MAGIC.to_le_bytes());
    bytes.extend_from_slice(&(partition_key.len() as u32).to_le_bytes());
    bytes.extend_from_slice(partition_key.as_bytes());
    bytes.extend_from_slice(&encode_tail_frame(batch));
    bytes
}

fn encode_tail_frame(batch: &RecordBatch) -> Vec<u8> {
    let payload = serialize_batch_for_wal(batch);
    let mut frame = Vec::with_capacity(12 + payload.len());
    frame.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    frame.extend_from_slice(&payload);
    let mut hasher = Hasher::new();
    hasher.update(&frame);
    frame.extend_from_slice(&hasher.finalize().to_le_bytes());
    frame
}

fn encode_flush_target(segment_file_name: &str) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(12 + segment_file_name.len());
    bytes.extend_from_slice(&FLUSH_MAGIC.to_le_bytes());
    bytes.extend_from_slice(&(segment_file_name.len() as u32).to_le_bytes());
    bytes.extend_from_slice(segment_file_name.as_bytes());
    append_crc(bytes)
}

fn append_crc(mut bytes: Vec<u8>) -> Vec<u8> {
    let mut hasher = Hasher::new();
    hasher.update(&bytes);
    bytes.extend_from_slice(&hasher.finalize().to_le_bytes());
    bytes
}

fn verify_crc(bytes: &[u8]) -> Result<&[u8]> {
    if bytes.len() < 4 {
        return Err(TaibaiError::Storage(
            "durable tail record is truncated".into(),
        ));
    }
    let body_end = bytes.len() - 4;
    let stored_crc = u32::from_le_bytes(bytes[body_end..].try_into().unwrap());
    let mut hasher = Hasher::new();
    hasher.update(&bytes[..body_end]);
    if stored_crc != hasher.finalize() {
        return Err(TaibaiError::Storage(
            "durable tail checksum mismatch".into(),
        ));
    }
    Ok(&bytes[..body_end])
}

fn decode_tail(bytes: &[u8], schema: &Schema) -> Result<(String, RecordBatch)> {
    if bytes.len() >= 4 && u32::from_le_bytes(bytes[0..4].try_into().unwrap()) == TAIL_LOG_MAGIC {
        return decode_tail_log(bytes, schema);
    }
    let body = verify_crc(bytes)?;
    if body.len() < 16 {
        return Err(TaibaiError::Storage("durable tail is truncated".into()));
    }
    let magic = u32::from_le_bytes(body[0..4].try_into().unwrap());
    if magic != TAIL_MAGIC {
        return Err(TaibaiError::Storage("invalid durable tail magic".into()));
    }
    let partition_len = u32::from_le_bytes(body[4..8].try_into().unwrap()) as usize;
    let partition_end = 8usize.saturating_add(partition_len);
    if partition_end.saturating_add(8) > body.len() {
        return Err(TaibaiError::Storage("invalid durable tail header".into()));
    }
    let partition_key = std::str::from_utf8(&body[8..partition_end])
        .map_err(|_| TaibaiError::Storage("invalid durable tail partition key".into()))?
        .to_string();
    let batch_len =
        u64::from_le_bytes(body[partition_end..partition_end + 8].try_into().unwrap()) as usize;
    let batch_start = partition_end + 8;
    let batch_end = batch_start.saturating_add(batch_len);
    if batch_end != body.len() {
        return Err(TaibaiError::Storage(
            "invalid durable tail payload length".into(),
        ));
    }
    let batch = deserialize_batch_from_wal(&body[batch_start..batch_end], schema)?;
    Ok((partition_key, batch))
}

fn decode_tail_log(bytes: &[u8], schema: &Schema) -> Result<(String, RecordBatch)> {
    if bytes.len() < 8 {
        return Err(TaibaiError::Storage("durable tail log is truncated".into()));
    }
    let partition_len = u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize;
    let mut offset = 8usize.saturating_add(partition_len);
    if offset > bytes.len() {
        return Err(TaibaiError::Storage(
            "invalid durable tail log header".into(),
        ));
    }
    let partition_key = std::str::from_utf8(&bytes[8..offset])
        .map_err(|_| TaibaiError::Storage("invalid durable tail partition key".into()))?
        .to_string();
    let mut recovered = RecordBatch::new(schema.clone());
    let mut complete_frames = 0usize;
    while offset < bytes.len() {
        // A crash may leave the final append incomplete. All preceding frames
        // were individually synced and remain a valid recovery prefix.
        if bytes.len().saturating_sub(offset) < 12 {
            break;
        }
        let payload_len =
            u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap()) as usize;
        let payload_start = offset + 8;
        let payload_end = payload_start.saturating_add(payload_len);
        let frame_end = payload_end.saturating_add(4);
        if frame_end > bytes.len() {
            break;
        }
        let mut hasher = Hasher::new();
        hasher.update(&bytes[offset..payload_end]);
        let stored_crc = u32::from_le_bytes(bytes[payload_end..frame_end].try_into().unwrap());
        if stored_crc != hasher.finalize() {
            return Err(TaibaiError::Storage(
                "durable tail log frame checksum mismatch".into(),
            ));
        }
        let batch = deserialize_batch_from_wal(&bytes[payload_start..payload_end], schema)?;
        append_batch(&mut recovered, &batch)?;
        complete_frames += 1;
        offset = frame_end;
    }
    if complete_frames == 0 {
        return Err(TaibaiError::Storage(
            "durable tail log contains no complete frame".into(),
        ));
    }
    Ok((partition_key, recovered))
}

fn append_batch(target: &mut RecordBatch, batch: &RecordBatch) -> Result<()> {
    for col_idx in 0..batch.num_columns() {
        let source = batch
            .column(col_idx)
            .ok_or_else(|| TaibaiError::Storage("durable tail delta is missing a column".into()))?;
        let destination = target
            .column_mut(col_idx)
            .ok_or_else(|| TaibaiError::Storage("durable tail schema column mismatch".into()))?;
        destination.extend(source)?;
    }
    Ok(())
}

fn read_tail_log_partition_key(path: &Path) -> Result<Option<String>> {
    let mut file = fs::File::open(path)?;
    let mut header = [0u8; 8];
    if let Err(error) = file.read_exact(&mut header) {
        if error.kind() == std::io::ErrorKind::UnexpectedEof {
            return Ok(None);
        }
        return Err(error.into());
    }
    if u32::from_le_bytes(header[0..4].try_into().unwrap()) != TAIL_LOG_MAGIC {
        return Ok(None);
    }
    let partition_len = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;
    let mut partition = vec![0u8; partition_len];
    file.read_exact(&mut partition)?;
    String::from_utf8(partition)
        .map(Some)
        .map_err(|_| TaibaiError::Storage("invalid durable tail partition key".into()))
}

fn decode_flush_target(bytes: &[u8]) -> Result<String> {
    let body = verify_crc(bytes)?;
    if body.len() < 8 {
        return Err(TaibaiError::Storage("flush intent is truncated".into()));
    }
    if u32::from_le_bytes(body[0..4].try_into().unwrap()) != FLUSH_MAGIC {
        return Err(TaibaiError::Storage("invalid flush intent magic".into()));
    }
    let name_len = u32::from_le_bytes(body[4..8].try_into().unwrap()) as usize;
    if 8usize.saturating_add(name_len) != body.len() {
        return Err(TaibaiError::Storage("invalid flush intent length".into()));
    }
    let name = std::str::from_utf8(&body[8..])
        .map_err(|_| TaibaiError::Storage("invalid flush intent target".into()))?
        .to_string();
    if Path::new(&name)
        .file_name()
        .and_then(|value| value.to_str())
        != Some(name.as_str())
    {
        return Err(TaibaiError::Storage("unsafe flush intent target".into()));
    }
    Ok(name)
}

fn next_sequence() -> u64 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(u64::MAX as u128) as u64;
    let counter = TAIL_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    nanos.saturating_add(counter & 0xFFFF)
}

fn parse_generation(path: &Path) -> Option<(Uuid, Generation)> {
    let name = path.file_name()?.to_str()?;
    let mut parts = name.split('.');
    let id = Uuid::parse_str(parts.next()?).ok()?;
    let sequence = parts.next()?.parse::<u64>().ok()?;
    let kind = match parts.next()? {
        "tail" => GenerationKind::Tail,
        "clear" => GenerationKind::Clear,
        "flush" => GenerationKind::Flush,
        _ => return None,
    };
    if parts.next().is_some() {
        return None;
    }
    Some((
        id,
        Generation {
            sequence,
            path: path.to_path_buf(),
            kind,
        },
    ))
}

fn generations_for_id(tail_dir: &Path, id: Uuid) -> Result<Vec<Generation>> {
    if !tail_dir.exists() {
        return Ok(Vec::new());
    }
    let mut generations = Vec::new();
    for entry in fs::read_dir(tail_dir)? {
        let path = entry?.path();
        if let Some((entry_id, generation)) = parse_generation(&path) {
            if entry_id == id {
                generations.push(generation);
            }
        }
    }
    Ok(generations)
}

fn newest_live_tail_path(tail_dir: &Path, id: Uuid) -> Result<Option<PathBuf>> {
    let mut generations = generations_for_id(tail_dir, id)?;
    generations.sort_by_key(|generation| std::cmp::Reverse(generation.sequence));
    for generation in generations {
        match generation.kind {
            GenerationKind::Tail => return Ok(Some(generation.path)),
            GenerationKind::Clear => return Ok(None),
            GenerationKind::Flush => {
                let Ok(bytes) = fs::read(&generation.path) else {
                    continue;
                };
                let Ok(segment_name) = decode_flush_target(&bytes) else {
                    continue;
                };
                let segment_path = tail_dir.parent().unwrap_or(tail_dir).join(segment_name);
                if segment_path.is_file() && SegmentReader::open(&segment_path).is_ok() {
                    return Ok(None);
                }
            }
        }
    }
    Ok(None)
}

fn remove_other_generations(tail_dir: &Path, id: Uuid, keep_path: &Path) -> Result<()> {
    for generation in generations_for_id(tail_dir, id)? {
        if generation.path != keep_path {
            remove_file_if_exists(&generation.path)?;
        }
    }
    Ok(())
}

fn remove_file_if_exists(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn remove_empty_dir(path: &Path) {
    if fs::read_dir(path)
        .ok()
        .is_some_and(|mut entries| entries.next().is_none())
    {
        let _ = fs::remove_dir(path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::types::DataType;
    use crate::storage::segment::constants::DEFAULT_ROW_GROUP_SIZE;
    use crate::storage::segment::SegmentWriter;
    use crate::types::batch::ColumnDef;
    use crate::types::scalar::ScalarValue;

    fn test_batch(schema: &Schema, values: &[i64]) -> Result<RecordBatch> {
        let mut batch = RecordBatch::new(schema.clone());
        for value in values {
            batch
                .column_mut(0)
                .unwrap()
                .push(&ScalarValue::Int64(Some(*value)))?;
        }
        Ok(batch)
    }

    #[test]
    fn latest_tail_generation_round_trips_and_clear_tombstones_it() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
        let id = Uuid::new_v4();
        persist(temp.path(), id, "2026", &test_batch(&schema, &[1])?)?;
        persist(temp.path(), id, "2026", &test_batch(&schema, &[1, 2])?)?;
        let recovered = recover(temp.path(), &schema)?;
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].id, id);
        assert_eq!(recovered[0].partition_key, "2026");
        assert_eq!(recovered[0].batch.num_rows(), 2);

        clear(temp.path(), id)?;
        assert!(recover(temp.path(), &schema)?.is_empty());
        assert!(!has_tail(temp.path(), id));
        Ok(())
    }

    #[test]
    fn appended_tail_deltas_recover_as_one_batch() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
        let id = Uuid::new_v4();
        append(temp.path(), id, "2026", &test_batch(&schema, &[1])?)?;
        append(temp.path(), id, "2026", &test_batch(&schema, &[2, 3])?)?;

        let recovered = recover(temp.path(), &schema)?;
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].partition_key, "2026");
        assert_eq!(recovered[0].batch.num_rows(), 3);
        assert_eq!(
            recovered[0].batch.scalar(2, 0),
            Some(ScalarValue::Int64(Some(3)))
        );
        Ok(())
    }

    #[test]
    fn appending_migrates_legacy_snapshot_tail() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
        let id = Uuid::new_v4();
        let partition_key = "legacy";
        let legacy_batch = test_batch(&schema, &[1])?;
        let batch_payload = serialize_batch_for_wal(&legacy_batch);
        let mut legacy = Vec::new();
        legacy.extend_from_slice(&TAIL_MAGIC.to_le_bytes());
        legacy.extend_from_slice(&(partition_key.len() as u32).to_le_bytes());
        legacy.extend_from_slice(partition_key.as_bytes());
        legacy.extend_from_slice(&(batch_payload.len() as u64).to_le_bytes());
        legacy.extend_from_slice(&batch_payload);
        let legacy = append_crc(legacy);
        let tail_dir = temp.path().join(TAIL_DIR);
        fs::create_dir_all(&tail_dir)?;
        fs::write(tail_dir.join(format!("{}.1.tail", id.simple())), legacy)?;

        append(temp.path(), id, partition_key, &test_batch(&schema, &[2])?)?;
        let recovered = recover(temp.path(), &schema)?;
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].batch.num_rows(), 2);
        assert_eq!(
            recovered[0].batch.scalar(1, 0),
            Some(ScalarValue::Int64(Some(2)))
        );
        Ok(())
    }

    #[test]
    fn recovery_ignores_incomplete_final_tail_frame() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
        let id = Uuid::new_v4();
        append(temp.path(), id, "", &test_batch(&schema, &[1])?)?;
        let tail_path =
            newest_live_tail_path(&temp.path().join(TAIL_DIR), id)?.expect("tail should exist");
        let mut file = fs::OpenOptions::new().append(true).open(tail_path)?;
        file.write_all(&[0x08, 0x00, 0x00, 0x00])?;
        drop(file);

        let recovered = recover(temp.path(), &schema)?;
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].batch.num_rows(), 1);
        Ok(())
    }

    #[test]
    fn incomplete_flush_recovers_tail_and_completed_flush_suppresses_it() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
        let batch = test_batch(&schema, &[1, 2])?;
        let id = Uuid::new_v4();
        persist(temp.path(), id, "", &batch)?;

        let incomplete = begin_flush(temp.path(), id, "seg_00001.dat")?;
        assert_eq!(recover(temp.path(), &schema)?.len(), 1);
        let _ = cancel_flush(incomplete);

        let _completed = begin_flush(temp.path(), id, "seg_00001.dat")?;
        let mut writer = SegmentWriter::create(
            &temp.path().join("seg_00001.dat"),
            schema.clone(),
            DEFAULT_ROW_GROUP_SIZE,
        )?;
        writer.write_batch(&batch)?;
        writer.seal()?;
        assert!(recover(temp.path(), &schema)?.is_empty());
        assert!(!has_tail(temp.path(), id));
        Ok(())
    }
}
