use super::*;

fn build_replace_key_envelopes(
    key_sets: &[(Vec<usize>, HashSet<Vec<ScalarValue>>)],
) -> Vec<ReplaceKeyEnvelope> {
    build_replace_key_envelopes_from_refs(&key_sets.iter().collect::<Vec<_>>())
}

fn replace_keys_may_match_row_group(
    zone_maps: &[ZoneMap],
    key_sets: &[(Vec<usize>, HashSet<Vec<ScalarValue>>)],
    envelopes: &[ReplaceKeyEnvelope],
) -> bool {
    replace_keys_may_match_row_group_from_refs(
        zone_maps,
        &key_sets.iter().collect::<Vec<_>>(),
        envelopes,
    )
}

fn zone(min: i64, max: i64) -> ZoneMap {
    ZoneMap {
        min_value: ScalarValue::Int64(Some(min)),
        max_value: ScalarValue::Int64(Some(max)),
        null_count: 0,
        row_count: 10,
    }
}

#[test]
fn replace_prunes_row_group_when_no_key_can_match() {
    let key_sets = vec![(
        vec![0, 1],
        [
            vec![ScalarValue::Int64(Some(1)), ScalarValue::Int64(Some(100))],
            vec![ScalarValue::Int64(Some(20)), ScalarValue::Int64(Some(5))],
        ]
        .into_iter()
        .collect(),
    )];
    assert!(!replace_keys_may_match_row_group(
        &[zone(1, 10), zone(1, 10)],
        &key_sets,
        &build_replace_key_envelopes(&key_sets),
    ));
}

#[test]
fn replace_keeps_row_group_when_composite_key_may_match() {
    let key_sets = vec![(
        vec![0, 1],
        [vec![
            ScalarValue::Int64(Some(5)),
            ScalarValue::Int64(Some(8)),
        ]]
        .into_iter()
        .collect(),
    )];
    assert!(replace_keys_may_match_row_group(
        &[zone(1, 10), zone(1, 10)],
        &key_sets,
        &build_replace_key_envelopes(&key_sets),
    ));
}

#[test]
fn replace_large_batch_uses_envelope_pruning() {
    let keys = (1_000..6_000)
        .map(|value| vec![ScalarValue::Int64(Some(value))])
        .collect();
    let key_sets = vec![(vec![0], keys)];
    assert!(!replace_keys_may_match_row_group(
        &[zone(1, 10)],
        &key_sets,
        &build_replace_key_envelopes(&key_sets),
    ));
    assert!(replace_keys_may_match_row_group(
        &[zone(2_000, 2_100)],
        &key_sets,
        &build_replace_key_envelopes(&key_sets),
    ));
}

#[test]
fn replace_keeps_row_group_when_zone_map_is_missing() {
    let key_sets = vec![(
        vec![1],
        [vec![ScalarValue::Int64(Some(100))]].into_iter().collect(),
    )];
    assert!(replace_keys_may_match_row_group(
        &[zone(1, 10)],
        &key_sets,
        &build_replace_key_envelopes(&key_sets),
    ));
}

#[test]
fn replace_candidates_respect_row_group_boundaries() {
    let candidates = [0, 7, 8, 15, 16, 39];
    assert_eq!(replace_candidates_in_range(&candidates, 0, 8), &[0, 7]);
    assert_eq!(replace_candidates_in_range(&candidates, 8, 8), &[8, 15]);
    assert_eq!(replace_candidates_in_range(&candidates, 16, 8), &[16]);
    assert!(replace_candidates_in_range(&candidates, 24, 8).is_empty());
    assert_eq!(replace_candidates_in_range(&candidates, 32, 8), &[39]);
    assert!(replace_candidates_in_range(&[], 0, 8).is_empty());
}

#[test]
fn replace_mixed_fallback_verifies_direct_locations_with_all_key_columns() -> Result<()> {
    use crate::net::session::Session;
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = Session::new("root".into(), "main".into());
    engine.execute(
        &mut session,
        "CREATE TABLE t (id INT64 PRIMARY KEY, payload VARCHAR, code INT64, UNIQUE KEY uk (code))",
    )?;
    engine.execute(
        &mut session,
        "INSERT INTO t VALUES (1, 'a', 100), (2, 'b', 200), (3, 'c', 300)",
    )?;
    engine.execute(&mut session, "FLUSH TABLE t")?;
    let table_meta = engine.get_table_meta("main", "t").unwrap();
    let paths = SegmentManager::discover_segments(&dir.path().join("main/t"))?;
    assert_eq!(paths.len(), 1);
    let reader = engine.segment_mgr().open_reader_cached(&paths[0])?;
    assert_eq!(reader.row_count(), 3);
    let key_sets = vec![
        (
            vec![0],
            [vec![ScalarValue::Int64(Some(1))]].into_iter().collect(),
        ),
        (
            vec![2],
            [vec![ScalarValue::Int64(Some(200))]].into_iter().collect(),
        ),
    ];
    // Row 0 matches only the direct PK. Row 1 matches only the fallback
    // unique key. Row 2 is a stale location and must survive verification.
    let direct = [0, 2]
        .into_iter()
        .map(|row_id| UniqueRowLocation {
            segment_path: paths[0].to_string_lossy().into_owned(),
            row_id,
        })
        .collect();
    let undo = engine
        .segment_mgr()
        .with_table_mutation_lock("main", "t", || {
            delete_replace_conflicts_under_table_lock(
                &engine,
                "main",
                "t",
                &table_meta,
                &key_sets,
                ReplaceConflictLocations {
                    direct,
                    fallback: vec![false, true],
                },
                &[],
            )
        })?;
    assert_eq!(undo.deleted_rows, 2);
    let reader = engine.segment_mgr().open_reader_cached(&paths[0])?;
    assert!(delete_bitmap::is_deleted(reader.delete_bitmap(), 0));
    assert!(delete_bitmap::is_deleted(reader.delete_bitmap(), 1));
    assert!(!delete_bitmap::is_deleted(reader.delete_bitmap(), 2));
    engine
        .segment_mgr()
        .with_table_mutation_lock("main", "t", || {
            restore_replace_delete_under_table_lock(&engine, &undo)
        })?;
    assert_eq!(
        engine
            .segment_mgr()
            .open_reader_cached(&paths[0])?
            .deleted_row_count(),
        0
    );
    Ok(())
}

#[test]
fn replace_sparse_candidates_across_row_groups_preserves_other_rows() -> Result<()> {
    use crate::net::session::Session;
    let dir = tempfile::tempdir().unwrap();
    let engine = Engine::new(dir.path(), "main")?;
    let mut session = Session::new("root".into(), "main".into());
    engine.execute(
        &mut session,
        "CREATE TABLE t (id INT64 PRIMARY KEY, v INT64)",
    )?;
    let values = (0..20_000)
        .map(|id| format!("({id}, 1)"))
        .collect::<Vec<_>>()
        .join(",");
    engine.execute(&mut session, &format!("INSERT INTO t VALUES {values}"))?;
    engine.execute(&mut session, "FLUSH TABLE t")?;
    let paths = SegmentManager::discover_segments(&dir.path().join("main/t"))?;
    let groups: usize = paths
        .iter()
        .map(|p| {
            engine
                .segment_mgr()
                .open_reader_cached(p)
                .unwrap()
                .num_row_groups()
        })
        .sum();
    assert!(groups > 1);
    let replaced = [0, 8191, 8192, 16383, 16384, 19999];
    let values = replaced
        .iter()
        .map(|id| format!("({id}, 9)"))
        .collect::<Vec<_>>()
        .join(",");
    let result = engine.execute(&mut session, &format!("REPLACE INTO t VALUES {values}"))?;
    assert!(matches!(result, ExecResult::Ok { rows_affected: 12 }));
    let ExecResult::Query { batches, .. } = engine.execute(&mut session, "SELECT id, v FROM t")?
    else {
        panic!("expected query")
    };
    let mut seen = HashSet::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            let Some(ScalarValue::Int64(Some(id))) = batch.scalar(row, 0) else {
                panic!("expected id")
            };
            assert!(seen.insert(id));
            assert_eq!(
                batch.scalar(row, 1),
                Some(ScalarValue::Int64(Some(if replaced.contains(&id) {
                    9
                } else {
                    1
                })))
            );
        }
    }
    assert_eq!(seen.len(), 20_000);
    Ok(())
}
