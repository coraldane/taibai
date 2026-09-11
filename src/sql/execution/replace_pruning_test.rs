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
