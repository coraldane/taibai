use super::*;
use crate::protocol::types::DataType;
use crate::storage::segment::writer::SegmentWriter;
use crate::types::batch::{ColumnDef, Schema};

fn entry(value: i64, row_id: u64) -> UniqueIndexEntry {
    UniqueIndexEntry {
        key: vec![ScalarValue::Int64(Some(value))],
        location: UniqueRowLocation {
            segment_path: String::new(),
            row_id,
        },
    }
}

#[test]
fn incremental_unique_index_merge_keeps_entries_sorted_and_aligned() -> Result<()> {
    let mut data = UniqueIndexData {
        db: "db".to_string(),
        table: "ticks".to_string(),
        index_name: "uk_value".to_string(),
        columns: vec!["value".to_string()],
        entries: vec![entry(2, 2), entry(4, 4)],
        flush_epoch: None,
        segment_watermarks: HashMap::new(),
        tombstones: HashSet::new(),
        encoded_keys: Vec::new(),
        blocks: Vec::new(),
    };
    prepare_unique_index_data(&mut data)?;

    merge_unique_index_entries(
        &mut data,
        vec![entry(5, 5), entry(1, 1), entry(4, 40), entry(3, 3)],
    )?;

    assert_eq!(data.entries.len(), 6);
    assert!(data.encoded_keys.windows(2).all(|keys| keys[0] <= keys[1]));
    for (encoded, entry) in data.encoded_keys.iter().zip(&data.entries) {
        assert_eq!(*encoded, encode_unique_key(&entry.key)?);
    }
    assert_eq!(
        data.entries
            .iter()
            .filter(|entry| entry.key == vec![ScalarValue::Int64(Some(4))])
            .count(),
        2
    );
    assert_eq!(data.blocks.len(), 1);
    Ok(())
}

#[test]
fn direct_location_lookup_skips_transient_locations() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let manager = IndexManager::new_with_data_dir(
        Arc::new(SegmentManager::new(dir.path())?),
        dir.path().to_path_buf(),
    );
    manager.persist_unique_index_entries(
        "db",
        "ticks",
        "uk_value",
        &["value".to_string()],
        vec![entry(1, 0)],
        Some(0),
        None,
    )?;
    // Memtable-resident pseudo locations (empty segment_path) are skipped:
    // REPLACE deletes conflicting memtable rows in place, so a location
    // probe only needs segment-side results.
    assert_eq!(
        manager.unique_key_locations(
            "db",
            "ticks",
            "uk_value",
            &[vec![ScalarValue::Int64(Some(1))]],
        )?,
        Some(Vec::new())
    );
    Ok(())
}

#[test]
fn flush_epoch_mismatch_refreshes_snapshot_incrementally() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let segment_mgr = Arc::new(SegmentManager::new(dir.path())?);
    let manager =
        IndexManager::new_with_data_dir(Arc::clone(&segment_mgr), dir.path().to_path_buf());
    manager.persist_unique_index_entries(
        "db",
        "ticks",
        "uk_value",
        &["value".to_string()],
        vec![entry(1, 0)],
        Some(segment_mgr.memtable_flush_epoch("db", "ticks")),
        None,
    )?;
    // A memtable flush after the snapshot was built bumps the epoch. With
    // no segment rows beyond the watermarks, the refresh is a no-op scan
    // and the probe succeeds without a full rebuild.
    segment_mgr.note_memtable_flush("db", "ticks");
    assert_eq!(
        manager.unique_key_locations(
            "db",
            "ticks",
            "uk_value",
            &[vec![ScalarValue::Int64(Some(1))]],
        )?,
        Some(Vec::new())
    );
    // The refreshed snapshot (epoch stamped) survives a restart through
    // the on-disk files; the epoch marker survives as well.
    let segment_mgr = Arc::new(SegmentManager::new(dir.path())?);
    let manager =
        IndexManager::new_with_data_dir(Arc::clone(&segment_mgr), dir.path().to_path_buf());
    assert_eq!(segment_mgr.memtable_flush_epoch("db", "ticks"), 1);
    assert_eq!(
        manager.unique_key_locations(
            "db",
            "ticks",
            "uk_value",
            &[vec![ScalarValue::Int64(Some(1))]],
        )?,
        Some(Vec::new())
    );
    Ok(())
}

#[test]
fn compact_prepared_entries_drops_tombstones_and_dedups_placeholders() -> Result<()> {
    let seg_path = "/tmp/taibai-test/seg_00000.dat".to_string();
    let mut data = UniqueIndexData {
        db: "db".to_string(),
        table: "ticks".to_string(),
        index_name: "uk_value".to_string(),
        columns: vec!["value".to_string()],
        entries: vec![
            entry(1, 0),
            entry(1, 5),
            entry(2, 0),
            UniqueIndexEntry {
                key: vec![ScalarValue::Int64(Some(3))],
                location: UniqueRowLocation {
                    segment_path: seg_path.clone(),
                    row_id: 0,
                },
            },
            UniqueIndexEntry {
                key: vec![ScalarValue::Int64(Some(4))],
                location: UniqueRowLocation {
                    segment_path: seg_path.clone(),
                    row_id: 1,
                },
            },
        ],
        flush_epoch: Some(1),
        segment_watermarks: HashMap::from([(seg_path.clone(), 2)]),
        tombstones: HashSet::new(),
        encoded_keys: Vec::new(),
        blocks: Vec::new(),
    };
    prepare_unique_index_data(&mut data)?;
    data.tombstones.insert(UniqueRowLocation {
        segment_path: seg_path.clone(),
        row_id: 1,
    });
    compact_prepared_unique_index_entries(&mut data);

    let keys = data
        .entries
        .iter()
        .map(|entry| entry.key[0].clone())
        .collect::<Vec<_>>();
    assert_eq!(
        keys,
        vec![
            ScalarValue::Int64(Some(1)),
            ScalarValue::Int64(Some(2)),
            ScalarValue::Int64(Some(3)),
        ],
        "duplicate placeholders collapse to one and tombstoned locations drop"
    );
    for (encoded, entry) in data.encoded_keys.iter().zip(&data.entries) {
        assert_eq!(*encoded, encode_unique_key(&entry.key)?);
    }
    assert_eq!(
        data.blocks.len(),
        data.entries.len().div_ceil(UNIQUE_INDEX_BLOCK_SIZE)
    );
    assert_eq!(data.segment_watermarks.get(&seg_path), Some(&2));
    Ok(())
}

#[test]
fn refresh_collapses_duplicate_memtable_placeholders() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let table_dir = dir.path().join("db").join("ticks");
    std::fs::create_dir_all(&table_dir)?;
    write_value_segment(&table_dir.join("seg_00000.dat"), &[1, 2, 3])?;

    let segment_mgr = Arc::new(SegmentManager::new(dir.path())?);
    let manager =
        IndexManager::new_with_data_dir(Arc::clone(&segment_mgr), dir.path().to_path_buf());
    let seg0 = table_dir
        .join("seg_00000.dat")
        .to_string_lossy()
        .to_string();
    // Two placeholders for key 2 simulate two upserts of the same key
    // while the row was still memtable-resident.
    manager.persist_unique_index_entries(
        "db",
        "ticks",
        "uk_value",
        &["value".to_string()],
        vec![
            UniqueIndexEntry {
                key: vec![ScalarValue::Int64(Some(1))],
                location: UniqueRowLocation {
                    segment_path: seg0.clone(),
                    row_id: 0,
                },
            },
            entry(2, 1),
            entry(2, 2),
        ],
        Some(segment_mgr.memtable_flush_epoch("db", "ticks")),
        Some(HashMap::from([(seg0.clone(), 3)])),
    )?;

    write_value_segment(&table_dir.join("seg_00001.dat"), &[4, 5])?;
    segment_mgr.note_memtable_flush("db", "ticks");
    assert!(manager
        .unique_key_locations(
            "db",
            "ticks",
            "uk_value",
            &[vec![ScalarValue::Int64(Some(5))]],
        )?
        .is_some());

    let indexes = manager.unique_indexes.read().unwrap();
    let state = indexes
        .get(&unique_index_key("db", "ticks", "uk_value"))
        .expect("refreshed state must be cached");
    let placeholder_count = state
        .base
        .entries
        .iter()
        .filter(|entry| {
            entry.location.segment_path.is_empty() && entry.key == vec![ScalarValue::Int64(Some(2))]
        })
        .count();
    assert_eq!(
        placeholder_count, 1,
        "refresh must collapse duplicate memtable placeholders per key"
    );
    let total = state.base.entries.len();
    assert_eq!(total, 4, "keys 1, 2, 4, 5 each keep exactly one entry");
    Ok(())
}

fn write_value_segment(path: &Path, values: &[i64]) -> Result<()> {
    let schema = Schema::new(vec![ColumnDef::new("value", DataType::Int64)]);
    let mut batch = RecordBatch::new(schema.clone());
    for value in values {
        batch
            .column_mut(0)
            .unwrap()
            .push(&ScalarValue::Int64(Some(*value)))?;
    }
    let mut writer = SegmentWriter::create(path, schema, 8192)?;
    writer.write_batch(&batch)?;
    writer.seal()?;
    Ok(())
}

#[test]
fn stale_epoch_refresh_scans_only_new_segment_rows() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let table_dir = dir.path().join("db").join("ticks");
    std::fs::create_dir_all(&table_dir)?;
    write_value_segment(&table_dir.join("seg_00000.dat"), &[1, 2, 3])?;

    let segment_mgr = Arc::new(SegmentManager::new(dir.path())?);
    let manager =
        IndexManager::new_with_data_dir(Arc::clone(&segment_mgr), dir.path().to_path_buf());
    // Snapshot covering the first segment: value=1 has a real location,
    // values 2 and 3 are memtable-resident placeholders.
    let seg0 = table_dir
        .join("seg_00000.dat")
        .to_string_lossy()
        .to_string();
    manager.persist_unique_index_entries(
        "db",
        "ticks",
        "uk_value",
        &["value".to_string()],
        vec![
            UniqueIndexEntry {
                key: vec![ScalarValue::Int64(Some(1))],
                location: UniqueRowLocation {
                    segment_path: seg0.clone(),
                    row_id: 0,
                },
            },
            entry(2, 1),
            entry(3, 2),
        ],
        Some(segment_mgr.memtable_flush_epoch("db", "ticks")),
        None,
    )?;

    // A later flush appends a new segment with values 4 and 5 and bumps
    // the epoch; the next probe must pick up their locations via an
    // incremental refresh.
    write_value_segment(&table_dir.join("seg_00001.dat"), &[4, 5])?;
    segment_mgr.note_memtable_flush("db", "ticks");

    let seg1 = table_dir
        .join("seg_00001.dat")
        .to_string_lossy()
        .to_string();
    assert_eq!(
        manager.unique_key_locations(
            "db",
            "ticks",
            "uk_value",
            &[
                vec![ScalarValue::Int64(Some(4))],
                vec![ScalarValue::Int64(Some(5))]
            ],
        )?,
        Some(vec![
            UniqueRowLocation {
                segment_path: seg1.clone(),
                row_id: 0,
            },
            UniqueRowLocation {
                segment_path: seg1.clone(),
                row_id: 1,
            },
        ])
    );
    // Rows already covered by the snapshot keep their recorded location
    // (watermark skips the first segment) and memtable placeholders stay
    // location-free.
    assert_eq!(
        manager.unique_key_locations(
            "db",
            "ticks",
            "uk_value",
            &[
                vec![ScalarValue::Int64(Some(1))],
                vec![ScalarValue::Int64(Some(2))]
            ],
        )?,
        Some(vec![UniqueRowLocation {
            segment_path: seg0,
            row_id: 0,
        }])
    );
    // The refreshed snapshot is stamped with the new epoch and persists
    // across a restart.
    let segment_mgr = Arc::new(SegmentManager::new(dir.path())?);
    let manager =
        IndexManager::new_with_data_dir(Arc::clone(&segment_mgr), dir.path().to_path_buf());
    assert_eq!(
        manager.unique_key_locations(
            "db",
            "ticks",
            "uk_value",
            &[vec![ScalarValue::Int64(Some(5))]],
        )?,
        Some(vec![UniqueRowLocation {
            segment_path: seg1,
            row_id: 1,
        }])
    );
    Ok(())
}

#[test]
fn incremental_unique_index_merge_handles_many_batches() -> Result<()> {
    let mut data = UniqueIndexData {
        db: "db".to_string(),
        table: "ticks".to_string(),
        index_name: "uk_value".to_string(),
        columns: vec!["value".to_string()],
        entries: Vec::new(),
        flush_epoch: None,
        segment_watermarks: HashMap::new(),
        tombstones: HashSet::new(),
        encoded_keys: Vec::new(),
        blocks: Vec::new(),
    };

    for batch in 0..100 {
        let start = batch * 100;
        let entries = (0..100)
            .rev()
            .map(|offset| entry((start + offset) as i64, (start + offset) as u64))
            .collect();
        merge_unique_index_entries(&mut data, entries)?;
    }

    assert_eq!(data.entries.len(), 10_000);
    assert!(data.encoded_keys.windows(2).all(|keys| keys[0] <= keys[1]));
    assert_eq!(
        data.blocks.len(),
        data.entries.len().div_ceil(UNIQUE_INDEX_BLOCK_SIZE)
    );
    Ok(())
}

fn stock_entry(id: &str, date: i32, time: i32, row_id: u64, path: &str) -> UniqueIndexEntry {
    UniqueIndexEntry {
        key: vec![
            ScalarValue::VarChar(Some(id.to_string())),
            ScalarValue::Int32(Some(date)),
            ScalarValue::Int32(Some(time)),
        ],
        location: UniqueRowLocation {
            segment_path: path.to_string(),
            row_id,
        },
    }
}

fn stock_minute_index_data(entry_count: u32, path: &str) -> UniqueIndexData {
    let entries = (0..entry_count)
        .map(|idx| {
            stock_entry(
                &format!("{:06}.SZ", idx / 240),
                20260701,
                93000 + (idx % 240) as i32,
                idx as u64,
                path,
            )
        })
        .collect();
    UniqueIndexData {
        db: "quant_base".to_string(),
        table: "t_stock_minute".to_string(),
        index_name: PRIMARY_LOCATION_INDEX_NAME.to_string(),
        columns: vec![
            "security_id".to_string(),
            "deal_date".to_string(),
            "deal_time".to_string(),
        ],
        entries,
        flush_epoch: None,
        segment_watermarks: HashMap::new(),
        tombstones: HashSet::new(),
        encoded_keys: Vec::new(),
        blocks: Vec::new(),
    }
}

fn encode_legacy_v2_unique_index_data(data: &UniqueIndexData) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(UNIQUE_INDEX_MAGIC_V2);
    put_string(&mut out, &data.db);
    put_string(&mut out, &data.table);
    put_string(&mut out, &data.index_name);
    put_u32(&mut out, data.columns.len() as u32);
    for column in &data.columns {
        put_string(&mut out, column);
    }
    put_u64(&mut out, data.entries.len() as u64);
    put_u32(&mut out, UNIQUE_INDEX_BLOCK_SIZE as u32);
    put_u32(&mut out, data.blocks.len() as u32);
    for block in &data.blocks {
        put_u64(&mut out, block.start as u64);
        put_u32(&mut out, block.len as u32);
        put_bytes(&mut out, &block.first_key);
    }
    for (idx, entry) in data.entries.iter().enumerate() {
        put_bytes(&mut out, &data.encoded_keys[idx]);
        put_u32(&mut out, entry.key.len() as u32);
        for value in &entry.key {
            encode_scalar_value(&mut out, value)?;
        }
        put_string(&mut out, &entry.location.segment_path);
        put_u64(&mut out, entry.location.row_id);
    }
    Ok(out)
}

#[test]
fn unique_index_data_v5_roundtrip_preserves_entries_and_watermarks() -> Result<()> {
    let path = "/data/monitor-data/taibai_data/quant_base/t_stock_minute/p2026_0007.dat";
    let mut data = stock_minute_index_data(3000, path);
    data.flush_epoch = Some(7);
    data.segment_watermarks.insert(path.to_string(), 424_242);
    prepare_unique_index_data(&mut data)?;
    let bytes = encode_unique_index_data(&data)?;
    assert_eq!(&bytes[..8], UNIQUE_INDEX_MAGIC_V5);
    let decoded = decode_unique_index_data(&bytes)?.unwrap();
    assert_eq!(
        decoded.segment_watermarks.get(path),
        Some(&424_242),
        "segment watermarks must survive the round trip"
    );
    assert_eq!(decoded.db, data.db);
    assert_eq!(decoded.table, data.table);
    assert_eq!(decoded.index_name, data.index_name);
    assert_eq!(decoded.columns, data.columns);
    assert_eq!(decoded.flush_epoch, Some(7));
    assert_eq!(decoded.entries.len(), data.entries.len());
    assert_eq!(decoded.encoded_keys, data.encoded_keys);
    for (decoded_entry, expected_entry) in decoded.entries.iter().zip(&data.entries) {
        assert_eq!(decoded_entry.key, expected_entry.key);
        assert_eq!(decoded_entry.location, expected_entry.location);
    }
    assert_eq!(decoded.blocks.len(), data.blocks.len());
    for (decoded_block, expected_block) in decoded.blocks.iter().zip(&data.blocks) {
        assert_eq!(decoded_block.start, expected_block.start);
        assert_eq!(decoded_block.len, expected_block.len);
        assert_eq!(decoded_block.first_key, expected_block.first_key);
    }
    Ok(())
}

#[test]
fn unique_index_data_v4_without_watermarks_still_decodes() -> Result<()> {
    let path = "/data/monitor-data/taibai_data/quant_base/t_stock_minute/p2026_0007.dat";
    let mut data = stock_minute_index_data(3000, path);
    data.flush_epoch = Some(7);
    prepare_unique_index_data(&mut data)?;
    let v5_bytes = encode_unique_index_data(&data)?;
    // With no watermarks the v5 layout is exactly v4 plus a trailing
    // zero-count u32; stripping it and downgrading the magic
    // reconstructs a legacy v4 payload.
    let mut v4_bytes = v5_bytes[..v5_bytes.len() - 4].to_vec();
    v4_bytes[..8].copy_from_slice(UNIQUE_INDEX_MAGIC_V4);
    assert_eq!(&v4_bytes[..8], UNIQUE_INDEX_MAGIC_V4);
    let decoded = decode_unique_index_data(&v4_bytes)?.unwrap();
    assert_eq!(decoded.flush_epoch, Some(7));
    assert_eq!(decoded.entries.len(), data.entries.len());
    assert!(decoded.segment_watermarks.is_empty());
    Ok(())
}

#[test]
fn unique_index_data_v3_shrinks_shared_path_keyset() -> Result<()> {
    let path = "/data/monitor-data/taibai_data/quant_base/t_stock_minute/p2026_0007.dat";
    let mut data = stock_minute_index_data(2000, path);
    prepare_unique_index_data(&mut data)?;
    let v2_bytes = encode_legacy_v2_unique_index_data(&data)?;
    let v3_bytes = encode_unique_index_data(&data)?;
    assert!(v3_bytes.len() * 3 < v2_bytes.len());
    assert!(v3_bytes.len() < data.entries.len() * 40);
    Ok(())
}

#[test]
fn unique_index_data_v2_snapshot_still_decodes() -> Result<()> {
    let path = "/data/monitor-data/taibai_data/quant_base/t_stock_minute/p2026_0007.dat";
    let mut data = stock_minute_index_data(600, path);
    prepare_unique_index_data(&mut data)?;
    let v2_bytes = encode_legacy_v2_unique_index_data(&data)?;
    let decoded = decode_unique_index_data(&v2_bytes)?.unwrap();
    assert_eq!(decoded.entries.len(), data.entries.len());
    assert_eq!(decoded.encoded_keys, data.encoded_keys);
    for (decoded_entry, expected_entry) in decoded.entries.iter().zip(&data.entries) {
        assert_eq!(decoded_entry.key, expected_entry.key);
        assert_eq!(decoded_entry.location, expected_entry.location);
    }
    Ok(())
}
