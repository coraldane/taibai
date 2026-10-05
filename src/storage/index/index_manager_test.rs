use super::*;
use crate::protocol::types::DataType;
use crate::storage::segment::writer::SegmentWriter;
use crate::types::batch::{ColumnDef, RecordBatch, Schema};
use std::sync::atomic::Ordering;

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
fn removed_key_hides_base_placeholder_entries_until_reinsert() -> Result<()> {
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
    assert_eq!(
        manager.unique_key_exists("db", "ticks", "uk_value", &[ScalarValue::Int64(Some(1))],)?,
        Some(true)
    );
    assert!(manager.remove_unique_index_keys(
        "db",
        "ticks",
        "uk_value",
        &[vec![ScalarValue::Int64(Some(1))]]
    )?);
    assert_eq!(
        manager.unique_key_exists("db", "ticks", "uk_value", &[ScalarValue::Int64(Some(1))],)?,
        Some(false)
    );
    assert_eq!(
        manager.unique_keys_exist(
            "db",
            "ticks",
            "uk_value",
            &[vec![ScalarValue::Int64(Some(1))]]
        )?,
        Some(false)
    );
    manager.append_unique_index_entries("db", "ticks", "uk_value", vec![entry(1, 0)])?;
    assert_eq!(
        manager.unique_key_exists("db", "ticks", "uk_value", &[ScalarValue::Int64(Some(1))],)?,
        Some(true)
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
    segment_mgr.note_segments_changed("db", "ticks");
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
fn stream_merge_drops_tombstones_and_dedups_placeholders() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let seg_path = "/tmp/taibai-test/seg_00000.dat".to_string();

    // Base: key 1 has one live and one tombstoned location, key 2 has two
    // duplicate placeholders, key 5 will be removed by the delta.
    let base_path = dir.path().join("uk.keys.tbi");
    let file = std::fs::File::create(&base_path)?;
    let mut writer = UniqueIndexBaseWriter::new(
        std::io::BufWriter::new(file),
        "db",
        "ticks",
        "uk_value",
        vec!["value".to_string()],
    )?;
    let base_entries = vec![(1u64, 0u64), (1, 1), (2, 5), (2, 6), (3, 2), (5, 3)];
    for (value, row_id) in base_entries {
        let entry = if value == 2 {
            entry(value as i64, row_id)
        } else {
            UniqueIndexEntry {
                key: vec![ScalarValue::Int64(Some(value as i64))],
                location: UniqueRowLocation {
                    segment_path: seg_path.clone(),
                    row_id,
                },
            }
        };
        writer.push_entry(&encode_unique_key(&entry.key)?, &entry.location)?;
    }
    writer.finish(Some(1), &HashMap::new())?;

    let replayed = ReplayedDelta {
        additions: HashMap::from([(
            encode_unique_key(&[ScalarValue::Int64(Some(4))])?,
            vec![UniqueRowLocation {
                segment_path: String::new(),
                row_id: 9,
            }],
        )]),
        removed_keys: HashSet::from([encode_unique_key(&[ScalarValue::Int64(Some(5))])?]),
        tombstones: HashSet::from([UniqueRowLocation {
            segment_path: seg_path.clone(),
            row_id: 1,
        }]),
    };

    let out_path = dir.path().join("uk.keys.tbi.merged");
    let out_file = std::fs::File::create(&out_path)?;
    let mut writer = UniqueIndexBaseWriter::new(
        std::io::BufWriter::new(out_file),
        "db",
        "ticks",
        "uk_value",
        vec!["value".to_string()],
    )?;
    let base = UniqueIndexBase::open(&base_path)?;
    stream_merge_unique_index_base(&base, replayed, &mut writer)?;
    writer.finish(Some(1), &HashMap::new())?;

    let merged = UniqueIndexBase::open(&out_path)?;
    assert_eq!(merged.entry_count, 4, "tombstone drops, placeholder dedup");
    let locations_of = |value: i64| -> Result<Vec<UniqueRowLocation>> {
        merged.find_locations(&encode_unique_key(&[ScalarValue::Int64(Some(value))])?)
    };
    assert_eq!(locations_of(1)?.len(), 1, "tombstoned location dropped");
    assert_eq!(locations_of(2)?.len(), 1, "duplicate placeholders collapse");
    assert_eq!(locations_of(3)?.len(), 1);
    assert_eq!(locations_of(4)?.len(), 1, "delta addition merges in");
    assert_eq!(locations_of(5)?.len(), 0, "removed key drops base rows");
    assert_eq!(locations_of(6)?.len(), 0);
    Ok(())
}

#[test]
fn delta_compaction_trigger_amortizes_by_entry_count() {
    assert!(!unique_index_delta_needs_compaction(0, 0));
    assert!(unique_index_delta_needs_compaction(
        UNIQUE_INDEX_DELTA_COMPACT_MIN_ENTRIES,
        0
    ));
    let base = UNIQUE_INDEX_DELTA_COMPACT_MIN_ENTRIES * UNIQUE_INDEX_DELTA_COMPACT_BASE_RATIO;
    assert!(!unique_index_delta_needs_compaction(
        UNIQUE_INDEX_DELTA_COMPACT_MIN_ENTRIES - 1,
        base as usize
    ));
    assert!(unique_index_delta_needs_compaction(
        UNIQUE_INDEX_DELTA_COMPACT_MIN_ENTRIES,
        base as usize
    ));
    // The absolute ceiling bounds delta memory once the base grows huge.
    let huge_base =
        (UNIQUE_INDEX_DELTA_COMPACT_MAX_ENTRIES * UNIQUE_INDEX_DELTA_COMPACT_BASE_RATIO) as usize;
    assert!(!unique_index_delta_needs_compaction(
        UNIQUE_INDEX_DELTA_COMPACT_MAX_ENTRIES - 1,
        huge_base
    ));
    assert!(unique_index_delta_needs_compaction(
        UNIQUE_INDEX_DELTA_COMPACT_MAX_ENTRIES,
        huge_base
    ));
}

#[test]
fn contains_any_key_uses_exact_binary_search_across_base_range() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let segment_mgr = SegmentManager::new(dir.path())?;
    let mut data = UniqueIndexData {
        db: "db".to_string(),
        table: "ticks".to_string(),
        index_name: "uk_value".to_string(),
        columns: vec!["value".to_string()],
        entries: (0..3000i64).map(|v| entry(v, v as u64)).collect(),
        flush_epoch: None,
        segment_watermarks: HashMap::new(),
        tombstones: HashSet::new(),
        encoded_keys: Vec::new(),
        blocks: Vec::new(),
    };
    prepare_unique_index_data(&mut data)?;
    assert!(data.blocks.len() >= 12, "seed must span multiple blocks");

    fn encoded_sorted(values: &[i64]) -> Result<Vec<Vec<u8>>> {
        let mut keys: Vec<Vec<u8>> = values
            .iter()
            .map(|v| encode_unique_key(&[ScalarValue::Int64(Some(*v))]))
            .collect::<Result<_>>()?;
        keys.sort();
        keys.dedup();
        Ok(keys)
    }

    let state = UniqueIndexState::new(
        UniqueIndexBase::from_decoded(data),
        UniqueIndexDeltaState::default(),
    );
    // Needles confined to the last block must still be found.
    assert!(unique_index_state_contains_any_key(
        &segment_mgr,
        &state,
        &encoded_sorted(&[2990, 2991, 2999])?
    )?);
    // Needles inside a middle block.
    assert!(unique_index_state_contains_any_key(
        &segment_mgr,
        &state,
        &encoded_sorted(&[1024, 1025])?
    )?);
    // All needles beyond the key range.
    assert!(!unique_index_state_contains_any_key(
        &segment_mgr,
        &state,
        &encoded_sorted(&[3000, 3001, 5000])?
    )?);
    // Missing needles mixed with a present one.
    assert!(unique_index_state_contains_any_key(
        &segment_mgr,
        &state,
        &encoded_sorted(&[4000, 5, 4001])?
    )?);

    let mut sparse_data = UniqueIndexData {
        db: "db".to_string(),
        table: "ticks".to_string(),
        index_name: "uk_value".to_string(),
        columns: vec!["value".to_string()],
        entries: (0..6000i64)
            .step_by(2)
            .map(|v| entry(v + 1, v as u64))
            .collect(),
        flush_epoch: None,
        segment_watermarks: HashMap::new(),
        tombstones: HashSet::new(),
        encoded_keys: Vec::new(),
        blocks: Vec::new(),
    };
    prepare_unique_index_data(&mut sparse_data)?;
    let sparse_state = UniqueIndexState::new(
        UniqueIndexBase::from_decoded(sparse_data),
        UniqueIndexDeltaState::default(),
    );
    let missing_across_range: Vec<i64> = (0..6000i64).step_by(2).collect();
    assert!(!unique_index_state_contains_any_key(
        &segment_mgr,
        &sparse_state,
        &encoded_sorted(&missing_across_range)?
    )?);
    Ok(())
}

fn int64_keys(values: &[i64]) -> Vec<Vec<ScalarValue>> {
    values
        .iter()
        .map(|value| vec![ScalarValue::Int64(Some(*value))])
        .collect()
}

fn seed_unique_index(manager: &IndexManager, values: std::ops::Range<i64>) -> Result<()> {
    manager.persist_unique_index_entries(
        "db",
        "t",
        "uk",
        &["value".to_string()],
        values.map(|v| entry(v, v as u64)).collect(),
        Some(0),
        Some(HashMap::new()),
    )
}

#[test]
fn background_compaction_preserves_appends_that_raced_the_merge() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let segment_mgr = Arc::new(SegmentManager::new(dir.path())?);
    let manager =
        IndexManager::new_with_data_dir(Arc::clone(&segment_mgr), dir.path().to_path_buf());
    seed_unique_index(&manager, 0..10)?;
    let batch: Vec<UniqueIndexEntry> = (10..40).map(|v| entry(v, v as u64)).collect();
    assert!(manager.append_unique_index_entries("db", "t", "uk", batch)?);

    let state = manager.load_unique_index_state("db", "t", "uk")?.unwrap();
    let job = UniqueIndexCompactionJob {
        db: "db".to_string(),
        table: "t".to_string(),
        index_name: "uk".to_string(),
        state: Arc::clone(&state),
        unique_indexes: Arc::clone(&manager.unique_indexes),
        data_dir: dir.path().to_path_buf(),
    };
    let snapshot = job.snapshot()?.unwrap();

    let window: Vec<UniqueIndexEntry> = (40..50).map(|v| entry(v, v as u64)).collect();
    let prepared = manager.prepare_unique_index_entries(window)?;
    append_prepared_unique_index_delta(&job.delta_path(), &prepared)?;
    {
        let mut delta = state.delta.write().unwrap();
        for addition in prepared {
            delta
                .key_locations
                .entry(addition.encoded_key.clone())
                .or_default()
                .push(addition.entry.location.clone());
            delta.entry_count += 1;
        }
    }

    let tmp_path = job.build_compacted_base(&snapshot)?;
    assert!(job.publish(snapshot.delta_bytes.len(), &tmp_path)?);

    assert_eq!(
        manager.unique_keys_exist("db", "t", "uk", int64_keys(&[5, 25, 45]))?,
        Some(true)
    );
    assert_eq!(
        manager.unique_keys_exist("db", "t", "uk", int64_keys(&[99]))?,
        Some(false)
    );

    let delta_bytes = std::fs::read(job.delta_path())?;
    assert_eq!(
        decode_unique_index_delta(&delta_bytes)?
            .records
            .iter()
            .filter(|record| { matches!(record, UniqueIndexDeltaRecord::Entry(_)) })
            .count(),
        10
    );

    let merged = manager.load_unique_index_state("db", "t", "uk")?.unwrap();
    assert_eq!(merged.base.entry_count, 40);
    assert_eq!(merged.delta.read().unwrap().entry_count, 10);
    assert!(!merged.compaction_in_flight.load(Ordering::Acquire));

    let manager2 = IndexManager::new_with_data_dir(
        Arc::new(SegmentManager::new(dir.path())?),
        dir.path().to_path_buf(),
    );
    assert_eq!(
        manager2.unique_keys_exist("db", "t", "uk", int64_keys(&[5, 25, 45]))?,
        Some(true)
    );
    assert_eq!(
        manager2.unique_keys_exist("db", "t", "uk", int64_keys(&[99]))?,
        Some(false)
    );
    Ok(())
}

#[test]
fn superseded_compaction_job_leaves_files_untouched() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let segment_mgr = Arc::new(SegmentManager::new(dir.path())?);
    let manager =
        IndexManager::new_with_data_dir(Arc::clone(&segment_mgr), dir.path().to_path_buf());
    seed_unique_index(&manager, 0..10)?;
    let batch: Vec<UniqueIndexEntry> = (10..20).map(|v| entry(v, v as u64)).collect();
    assert!(manager.append_unique_index_entries("db", "t", "uk", batch)?);

    let state = manager.load_unique_index_state("db", "t", "uk")?.unwrap();
    let job = UniqueIndexCompactionJob {
        db: "db".to_string(),
        table: "t".to_string(),
        index_name: "uk".to_string(),
        state: Arc::clone(&state),
        unique_indexes: Arc::clone(&manager.unique_indexes),
        data_dir: dir.path().to_path_buf(),
    };
    let snapshot = job.snapshot()?.unwrap();
    let delta_before = std::fs::read(job.delta_path())?;
    let base_before = std::fs::read(job.base_path())?;
    let tmp_path = job.build_compacted_base(&snapshot)?;

    manager.invalidate_unique_index_snapshot("db", "t", "uk");
    assert!(!job.publish(snapshot.delta_bytes.len(), &tmp_path)?);
    let _ = std::fs::remove_file(&tmp_path);

    assert_eq!(std::fs::read(job.delta_path())?, delta_before);
    assert_eq!(std::fs::read(job.base_path())?, base_before);
    assert!(manager
        .unique_keys_exist("db", "t", "uk", int64_keys(&[5]))?
        .is_none());
    Ok(())
}

#[test]
fn compaction_waits_until_all_pauses_are_resumed() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let segment_mgr = Arc::new(SegmentManager::new(dir.path())?);
    let manager =
        IndexManager::new_with_data_dir(Arc::clone(&segment_mgr), dir.path().to_path_buf());
    seed_unique_index(&manager, 0..10)?;
    manager.pause_unique_index_compaction();
    manager.pause_unique_index_compaction();

    let batch: Vec<UniqueIndexEntry> = (10..60).map(|v| entry(v, v as u64)).collect();
    assert!(manager.append_unique_index_entries("db", "t", "uk", batch)?);
    let state = manager.load_unique_index_state("db", "t", "uk")?.unwrap();
    assert_eq!(state.base.entry_count, 10);
    assert_eq!(state.delta.read().unwrap().entry_count, 50);

    manager.resume_unique_index_compaction();
    let state = manager.load_unique_index_state("db", "t", "uk")?.unwrap();
    assert_eq!(state.base.entry_count, 10);
    assert_eq!(state.delta.read().unwrap().entry_count, 50);

    manager.resume_unique_index_compaction();
    let state = manager.load_unique_index_state("db", "t", "uk")?.unwrap();
    assert_eq!(state.base.entry_count, 60);
    assert_eq!(state.delta.read().unwrap().entry_count, 0);
    Ok(())
}

#[test]
fn dispatched_delta_compaction_publishes_new_base_and_resets_flag() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let segment_mgr = Arc::new(SegmentManager::new(dir.path())?);
    let manager =
        IndexManager::new_with_data_dir(Arc::clone(&segment_mgr), dir.path().to_path_buf());
    seed_unique_index(&manager, 0..10)?;
    let batch: Vec<UniqueIndexEntry> = (10..60).map(|v| entry(v, v as u64)).collect();
    assert!(manager.append_unique_index_entries("db", "t", "uk", batch)?);

    let state = manager.load_unique_index_state("db", "t", "uk")?.unwrap();
    assert_eq!(state.base.entry_count, 60);
    assert_eq!(state.delta.read().unwrap().entry_count, 0);
    assert!(!state.compaction_in_flight.load(Ordering::Acquire));
    assert!(!manager
        .unique_index_dir("db", "t")
        .join("uk.delta.tbi")
        .exists());

    assert_eq!(
        manager.unique_keys_exist("db", "t", "uk", int64_keys(&[5, 55]))?,
        Some(true)
    );
    assert_eq!(
        manager.unique_keys_exist("db", "t", "uk", int64_keys(&[99]))?,
        Some(false)
    );

    let manager2 = IndexManager::new_with_data_dir(
        Arc::new(SegmentManager::new(dir.path())?),
        dir.path().to_path_buf(),
    );
    assert_eq!(
        manager2.unique_keys_exist("db", "t", "uk", int64_keys(&[5, 55]))?,
        Some(true)
    );
    Ok(())
}

#[test]
fn refresh_collapses_duplicate_memtable_placeholders_at_compaction() -> Result<()> {
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

    // A later flush appends a tail large enough to cross the amortized
    // compaction threshold once it has been indexed into the delta.
    let tail: Vec<i64> = (4..54).collect();
    write_value_segment(&table_dir.join("seg_00001.dat"), &tail)?;
    segment_mgr.note_segments_changed("db", "ticks");
    // The first refresh appends the tail to the delta without rewriting base.
    assert!(manager
        .unique_key_locations(
            "db",
            "ticks",
            "uk_value",
            &[vec![ScalarValue::Int64(Some(53))]],
        )?
        .is_some());
    // The next refresh past the threshold compacts the delta back into the
    // base snapshot, collapsing duplicate memtable placeholders per key.
    segment_mgr.note_segments_changed("db", "ticks");
    assert!(manager
        .unique_key_locations(
            "db",
            "ticks",
            "uk_value",
            &[vec![ScalarValue::Int64(Some(53))]],
        )?
        .is_some());

    let indexes = manager.unique_indexes.read().unwrap();
    let state = indexes
        .get(&unique_index_key("db", "ticks", "uk_value"))
        .expect("refreshed state must be cached");
    // Key 2 carried two placeholders before compaction; the merged base must
    // hold exactly one entry for it (one for key 1 plus one per tail key).
    let total = state.base.entry_count;
    assert_eq!(
        total,
        2 + tail.len(),
        "compaction must collapse duplicate memtable placeholders per key"
    );
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
    let mut writer = SegmentWriter::create(path, schema, 8192, 3)?;
    writer.write_batch(&batch)?;
    writer.seal()?;
    Ok(())
}

#[test]
fn stale_epoch_refresh_keeps_physical_rows_with_delete_bitmap() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let table_dir = dir.path().join("db").join("ticks");
    std::fs::create_dir_all(&table_dir)?;
    write_value_segment(
        &table_dir.join("seg_00000.dat"),
        &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    )?;

    let segment_mgr = Arc::new(SegmentManager::new(dir.path())?);
    let manager =
        IndexManager::new_with_data_dir(Arc::clone(&segment_mgr), dir.path().to_path_buf());
    let seg0_path = table_dir.join("seg_00000.dat");
    let seg0 = seg0_path.to_string_lossy().to_string();

    // Snapshot whose per-segment watermark covers only the first five rows.
    let mut watermarks = HashMap::new();
    watermarks.insert(seg0.clone(), 5u64);
    let snapshot_entries = (0..5u64)
        .map(|row_id| UniqueIndexEntry {
            key: vec![ScalarValue::Int64(Some(row_id as i64 + 1))],
            location: UniqueRowLocation {
                segment_path: seg0.clone(),
                row_id,
            },
        })
        .collect::<Vec<_>>();
    manager.persist_unique_index_entries(
        "db",
        "ticks",
        "uk_value",
        &["value".to_string()],
        snapshot_entries,
        Some(segment_mgr.memtable_flush_epoch("db", "ticks")),
        Some(watermarks),
    )?;

    // Delete rows 2 and 3 in place. The next flush-epoch bump makes the
    // refresh re-scan the row group crossing the watermark; recorded
    // locations must stay aligned with physical rows instead of filtered
    // positions.
    let mut bitmap = vec![0u8; 10usize.div_ceil(8)];
    crate::storage::segment::delete_bitmap::mark_deleted(&mut bitmap, 2);
    crate::storage::segment::delete_bitmap::mark_deleted(&mut bitmap, 3);
    segment_mgr.rewrite_delete_footer(&seg0_path, &bitmap, 2)?;
    segment_mgr.note_segments_changed("db", "ticks");

    let locations = manager.unique_key_locations(
        "db",
        "ticks",
        "uk_value",
        &[
            vec![ScalarValue::Int64(Some(6))],
            vec![ScalarValue::Int64(Some(10))],
        ],
    )?;
    assert_eq!(
        locations,
        Some(vec![
            UniqueRowLocation {
                segment_path: seg0.clone(),
                row_id: 5,
            },
            UniqueRowLocation {
                segment_path: seg0,
                row_id: 9,
            },
        ])
    );
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
    segment_mgr.note_segments_changed("db", "ticks");

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

#[test]
fn unique_index_base_v6_roundtrip_preserves_entries_and_watermarks() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("uk.keys.tbi");
    let segment = "/data/monitor-data/taibai_data/quant_base/t_stock_minute/p2026_0007.dat";
    let mut data = stock_minute_index_data(3000, segment);
    // Entries must reach the writer sorted by encoded bytes, exactly like
    // the persisted-snapshot path feeds it.
    prepare_unique_index_data(&mut data)?;

    let file = std::fs::File::create(&path)?;
    let mut writer = UniqueIndexBaseWriter::new(
        std::io::BufWriter::new(file),
        &data.db,
        &data.table,
        &data.index_name,
        data.columns.clone(),
    )?;
    for (encoded, entry) in data.encoded_keys.iter().zip(&data.entries) {
        writer.push_entry(encoded, &entry.location)?;
    }
    let watermarks = HashMap::from([(segment.to_string(), 424_242u64)]);
    writer.finish(Some(7), &watermarks)?;

    let base = UniqueIndexBase::open(&path)?;
    assert_eq!(base.entry_count, 3000);
    assert_eq!(base.db, data.db);
    assert_eq!(base.table, data.table);
    assert_eq!(base.index_name, data.index_name);
    assert_eq!(base.columns, data.columns);
    assert_eq!(base.flush_epoch, Some(7));
    assert_eq!(base.segment_watermarks.get(segment), Some(&424_242));
    assert!(
        base.blocks.len() >= 12,
        "seed must span multiple blocks for on-demand reads"
    );

    // A single lookup parses header + footer and touches only its own
    // blocks: nothing close to the whole index may be resident.
    let hit = base.find_locations(&data.encoded_keys[2999])?;
    assert_eq!(hit.len(), 1);
    assert_eq!(hit[0], data.entries[2999].location);
    assert!(
        base.cached_block_count() <= 2,
        "lookups must load blocks on demand, not eagerly"
    );

    for idx in (0..3000usize).step_by(97) {
        let locations = base.find_locations(&data.encoded_keys[idx])?;
        assert_eq!(locations.len(), 1, "key at {idx} must be found");
        assert_eq!(locations[0], data.entries[idx].location);
    }
    let missing = encode_unique_key(&[
        ScalarValue::VarChar(Some("999999.SZ".to_string())),
        ScalarValue::Int32(Some(20260701)),
        ScalarValue::Int32(Some(93000)),
    ])?;
    assert!(base.find_locations(&missing)?.is_empty());
    Ok(())
}

#[test]
fn find_locations_walks_equal_key_runs_across_block_boundaries() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("uk.keys.tbi");
    let segment = "/tmp/taibai-test/seg_00000.dat";
    let file = std::fs::File::create(&path)?;
    let mut writer = UniqueIndexBaseWriter::new(
        std::io::BufWriter::new(file),
        "db",
        "ticks",
        "uk_value",
        vec!["value".to_string()],
    )?;

    // 700 locations of one key span three blocks; single-location keys sit
    // immediately before and after the run.
    let total = 700usize;
    let make = |value: i64, row_id: u64| UniqueIndexEntry {
        key: vec![ScalarValue::Int64(Some(value))],
        location: UniqueRowLocation {
            segment_path: segment.to_string(),
            row_id,
        },
    };
    writer.push_entry(&encode_unique_key(&make(0, 0).key)?, &make(0, 0).location)?;
    for row_id in 0..total as u64 {
        let entry = make(1, row_id);
        writer.push_entry(&encode_unique_key(&entry.key)?, &entry.location)?;
    }
    let tail = make(2, total as u64);
    writer.push_entry(&encode_unique_key(&tail.key)?, &tail.location)?;
    writer.finish(Some(1), &HashMap::new())?;

    let base = UniqueIndexBase::open(&path)?;
    assert_eq!(base.entry_count, total + 2);
    let run = base.find_locations(&encode_unique_key(&[ScalarValue::Int64(Some(1))])?)?;
    assert_eq!(run.len(), total, "equal-key run must survive block edges");
    let mut row_ids: Vec<u64> = run.iter().map(|location| location.row_id).collect();
    row_ids.sort();
    assert_eq!(row_ids.first(), Some(&0));
    assert_eq!(row_ids.last(), Some(&(total as u64 - 1)));
    assert_eq!(
        base.find_locations(&encode_unique_key(&[ScalarValue::Int64(Some(0))])?)?
            .len(),
        1
    );
    assert_eq!(
        base.find_locations(&encode_unique_key(&[ScalarValue::Int64(Some(2))])?)?
            .len(),
        1
    );
    Ok(())
}

#[test]
fn legacy_decoded_snapshot_serves_lookups_from_memory_blocks() -> Result<()> {
    let segment = "/data/monitor-data/taibai_data/quant_base/t_stock_minute/p2026_0007.dat";
    let mut data = stock_minute_index_data(600, segment);
    data.flush_epoch = Some(3);
    data.segment_watermarks.insert(segment.to_string(), 600);
    prepare_unique_index_data(&mut data)?;

    let base = UniqueIndexBase::from_decoded(data);
    assert_eq!(base.entry_count, 600);
    // idx 480 maps to security 000002.SZ at deal_time 93000 (480 % 240 == 0).
    let probe = encode_unique_key(&[
        ScalarValue::VarChar(Some("000002.SZ".to_string())),
        ScalarValue::Int32(Some(20260701)),
        ScalarValue::Int32(Some(93000)),
    ])?;
    let locations = base.find_locations(&probe)?;
    assert_eq!(locations.len(), 1);
    assert_eq!(locations[0].row_id, 480);
    assert!(
        base.cached_block_count() >= 3,
        "legacy snapshots pre-build every block in the cache"
    );
    Ok(())
}

/// Regression: an in-flight base snapshot must keep reading its OWN file
/// bytes after a compaction publish renames a new base over the same path.
/// The base pins an open handle at `open` time for exactly this reason;
/// reading by path paired the old block directory (offsets, first keys,
/// path table) with the new file's bytes and surfaced
/// "corrupt unique index block key prefix / path reference" or an
/// UnexpectedEof on concurrent INSERT probes.
#[test]
fn base_block_loads_survive_publish_rename_over_the_path() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let base_path = dir.path().join("uk.keys.tbi");
    // Keys are fixed-width zero-padded strings: encoded-byte order matches
    // generation order, satisfying the writer's encoded-sorted contract
    // (the on-disk order is by encoded bytes, not by integer value).
    let write_base = |path: &std::path::Path, seg_path: &str, count: usize| -> Result<()> {
        let file = std::fs::File::create(path)?;
        let mut writer = UniqueIndexBaseWriter::new(
            std::io::BufWriter::new(file),
            "db",
            "ticks",
            "uk_value",
            vec!["value".to_string()],
        )?;
        for value in 0..count {
            writer.push_entry(
                &encode_unique_key(&[ScalarValue::VarChar(Some(format!("k{value:06}")))])?,
                &UniqueRowLocation {
                    segment_path: seg_path.to_string(),
                    row_id: value as u64,
                },
            )?;
        }
        writer.finish(None, &HashMap::new())?;
        Ok(())
    };

    // Snapshot A: 600 sorted keys across three 256-entry blocks.
    write_base(&base_path, "/tmp/a/seg_00000.dat", 600)?;
    let state_a = UniqueIndexBase::open(&base_path)?;

    // A compaction publish rewrites the path with a shorter, differently
    // laid-out file B via tmp+rename — a NEW file identity replacing the
    // directory entry, never a truncate of the open file (on Windows a
    // truncate would shrink the very bytes the pinned handle reads).
    let tmp_path = dir.path().join("uk.keys.tbi.tmp");
    write_base(&tmp_path, "/tmp/b/seg_00000.dat", 10)?;
    std::fs::rename(&tmp_path, &base_path)?;

    // The in-flight state must resolve its own keys from its pinned handle:
    // every lookup below loads blocks on demand AFTER the overwrite.
    for value in [0usize, 255, 256, 512, 599] {
        let locations = state_a.find_locations(&encode_unique_key(&[ScalarValue::VarChar(
            Some(format!("k{value:06}")),
        )])?)?;
        assert_eq!(
            locations,
            vec![UniqueRowLocation {
                segment_path: "/tmp/a/seg_00000.dat".to_string(),
                row_id: value as u64,
            }],
            "value {value} must resolve against snapshot A's own bytes"
        );
    }
    assert_eq!(state_a.entry_count, 600);
    Ok(())
}

#[test]
fn external_index_build_merges_runs_and_keeps_previous_base_on_scan_error() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let segment_mgr = Arc::new(SegmentManager::new(dir.path())?);
    let manager =
        IndexManager::new_with_data_dir(Arc::clone(&segment_mgr), dir.path().to_path_buf());
    manager.persist_unique_index_stream("db", "t", "uq", &["v".into()], |emit| {
        for value in (0..40_000).rev() {
            emit(entry(value, value as u64))?;
        }
        Ok(())
    })?;
    for value in [0, 1, 20_000, 39_999] {
        assert_eq!(
            manager.unique_key_exists("db", "t", "uq", &[ScalarValue::Int64(Some(value))])?,
            Some(true)
        );
    }
    assert_eq!(
        manager.unique_key_exists("db", "t", "uq", &[ScalarValue::Int64(Some(40_000))])?,
        Some(false)
    );
    let failed = manager.persist_unique_index_stream("db", "t", "uq", &["v".into()], |emit| {
        for value in 50_000..90_000 {
            emit(entry(value, value as u64))?;
        }
        Err(TaibaiError::Storage("forced producer read failure".into()))
    });
    assert!(failed.is_err());
    assert_eq!(
        manager.unique_key_exists("db", "t", "uq", &[ScalarValue::Int64(Some(39_999))])?,
        Some(true)
    );
    assert_eq!(
        manager.unique_key_exists("db", "t", "uq", &[ScalarValue::Int64(Some(50_000))])?,
        Some(false)
    );
    assert!(
        std::fs::read_dir(manager.unique_index_dir("db", "t"))?.all(|entry| !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with(".build-"))
    );
    drop(manager);
    let manager =
        IndexManager::new_with_data_dir(Arc::clone(&segment_mgr), dir.path().to_path_buf());
    assert_eq!(
        manager.unique_key_exists("db", "t", "uq", &[ScalarValue::Int64(Some(39_999))])?,
        Some(true)
    );
    Ok(())
}
