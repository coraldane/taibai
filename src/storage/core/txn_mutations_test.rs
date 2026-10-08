use super::*;

/// Transactional INSERT memtable bytes count against the same per-
/// transaction in-memory budget as deferred DML, and the charged entry
/// disappears on the usual rollback cleanup.
#[test]
fn txn_insert_bytes_count_against_mutation_budget() {
    let registry = TxnMutationRegistry::new();
    let budget = txn_mutation_budget_bytes();

    // Exactly at budget is fine; one more byte trips it.
    registry
        .charge_txn_insert_bytes(9, "db", "t", budget)
        .expect("at-budget charge must pass");
    let err = registry
        .charge_txn_insert_bytes(9, "db", "t", 1)
        .expect_err("over-budget charge must fail");
    assert!(err.to_string().contains("budget exceeded"), "{err}");

    // Other transactions are unaffected.
    registry
        .charge_txn_insert_bytes(10, "db", "t", 1024)
        .expect("other txn unaffected");

    // Rollback cleanup drops the charged entry; the next transaction
    // with the same id starts from a fresh budget.
    registry.remove(9);
    registry
        .charge_txn_insert_bytes(9, "db", "t", 1)
        .expect("cleaned entry starts fresh");
}

#[test]
fn segment_tombstones_merge_and_compose() {
    let registry = TxnMutationRegistry::new();
    registry
        .register_segment_tombstones(7, "db", "t", None, Path::new("s1"), &[], [3, 9])
        .unwrap();
    registry
        .register_segment_tombstones(7, "db", "t", None, Path::new("s1"), &[], [3, 20])
        .unwrap();
    let composed = registry.segment_overlay(7, Path::new("s1")).unwrap();
    assert!(composed.len() >= 3);
    let is_set = |row: u64| composed[(row / 8) as usize] & (1u8 << (row % 8)) != 0;
    for row in [3u64, 9, 20] {
        assert!(is_set(row), "row {row} must be set");
    }
    assert!(!is_set(4));
    assert_eq!(registry.segment_row_ids(7, Path::new("s1")), vec![3, 9, 20]);
}

#[test]
fn take_remove_and_pins() {
    let registry = TxnMutationRegistry::new();
    registry
        .register_segment_tombstones(1, "db", "t", None, Path::new("s"), &[], [0])
        .unwrap();
    assert!(registry.table_has_pins("db", "t"));
    assert!(!registry.table_has_pins("db", "other"));
    let taken = registry.take(1).unwrap();
    assert!(!registry.table_has_pins("db", "t"));
    registry.put_back(1, taken);
    assert!(registry.table_has_pins("db", "t"));
    registry
        .register_segment_tombstones(2, "db", "t", None, Path::new("s2"), &[], [1])
        .unwrap();
    assert_eq!(registry.remove(1), 0);
    assert!(registry.table_has_pins("db", "t"), "txn 2 still pins");
    registry.remove(2);
    assert!(!registry.table_has_pins("db", "t"));
}

/// A later table-wide statement must widen the COMMIT lock scope: a
/// narrow-partition DELETE followed by a table-wide DELETE would
/// otherwise leave the recorded scope narrow while tombstones cover
/// other partitions, letting COMMIT rewrite those segments against a
/// concurrent flush.
#[test]
fn table_scope_widens_to_none_on_table_wide_statement() {
    let registry = TxnMutationRegistry::new();
    registry
        .register_segment_tombstones(
            1,
            "db",
            "t",
            Some(&["a".to_string()]),
            Path::new("s1"),
            &[],
            [0],
        )
        .unwrap();
    // Table-wide statement (partitions = None).
    registry
        .register_segment_tombstones(1, "db", "t", None, Path::new("s2"), &[], [0])
        .unwrap();
    // Scope is private; verify via take: the merged entry drives the
    // COMMIT lock breadth in materialize_tables.
    let mutations = registry.take(1).unwrap();
    let entry = &mutations.tables[0];
    assert!(
        entry.partitions.is_none(),
        "table-wide statement must widen the recorded scope to None"
    );
}

/// Two different narrow partitions union their keys; a repeat of the
/// same partition does not duplicate the entry.
#[test]
fn narrow_scopes_union_across_statements() {
    let registry = TxnMutationRegistry::new();
    registry
        .register_segment_tombstones(
            1,
            "db",
            "t",
            Some(&["a".to_string()]),
            Path::new("s1"),
            &[],
            [0],
        )
        .unwrap();
    registry
        .register_segment_tombstones(
            1,
            "db",
            "t",
            Some(&["b".to_string()]),
            Path::new("s2"),
            &[],
            [0],
        )
        .unwrap();
    registry
        .register_segment_tombstones(
            1,
            "db",
            "t",
            Some(&["a".to_string()]),
            Path::new("s1"),
            &[],
            [1],
        )
        .unwrap();
    let mutations = registry.take(1).unwrap();
    let entry = &mutations.tables[0];
    assert_eq!(
        entry.partitions.as_deref(),
        Some(&["a".to_string(), "b".to_string()][..]),
        "narrow scopes must union without duplicates"
    );
    assert_eq!(entry.segments.len(), 2);
}

/// The incremental composition must stay equivalent to the old full
/// replay: after every statement, the composed bitmap equals
/// `footer ∪ all recorded row ids` recomputed from scratch. Footers grow
/// monotonically like a pinned segment's really would (deletes only set
/// bits, appends only extend the length), which is the invariant the
/// increment relies on: the pinned segment cannot gain bits
/// mid-transaction, so folding the first footer equals the replay's
/// per-statement `current footer ∪ all ids`.
#[test]
fn incremental_segment_composition_matches_full_replay() {
    let registry = TxnMutationRegistry::new();
    let path = Path::new("s_incremental");

    // Deterministic xorshift so failures reproduce.
    let mut seed = 0x2545_F491_4F6C_DD1Du64;
    let mut next = move || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    };

    let mut reference_ids: BTreeSet<u64> = BTreeSet::new();
    let mut footer: Vec<u8> = Vec::new();
    for _statement in 0..40 {
        // The segment's footer bitmap only ever grows (like a pinned
        // segment's): the length may extend and bits may be set, never
        // cleared.
        footer.resize(footer.len() + (next() as usize) % 3, 0);
        for byte in footer.iter_mut() {
            *byte |= (next() & 0x03) as u8;
        }
        let new_ids: Vec<u64> = (0..(next() as usize) % 12).map(|_| next() % 96).collect();

        registry
            .register_segment_tombstones(
                11,
                "db",
                "t",
                None,
                path,
                &footer,
                new_ids.iter().copied(),
            )
            .unwrap();
        for id in &new_ids {
            reference_ids.insert(*id);
        }

        // Reference: full recomputation the replay used to do.
        let mut expected = footer.clone();
        for &row_id in &reference_ids {
            let byte = (row_id / 8) as usize;
            let bit = (row_id % 8) as u8;
            if byte >= expected.len() {
                expected.resize(byte + 1, 0);
            }
            expected[byte] |= 1u8 << bit;
        }
        let composed = registry.segment_overlay(11, path).unwrap();
        assert_eq!(
            composed.as_ref(),
            &expected,
            "incremental composition diverged from full replay"
        );
        assert_eq!(
            registry.segment_row_ids(11, path),
            reference_ids.iter().copied().collect::<Vec<_>>()
        );
    }
}

/// Memtable tombstone merging is differential now; the visible position
/// set must still be the exact union across statements, with repeated
/// positions counted (and charged) only once.
#[test]
fn memtable_position_merge_is_the_exact_union() {
    let registry = TxnMutationRegistry::new();
    let schema = crate::types::batch::Schema::new(vec![crate::types::batch::ColumnDef::new(
        "id",
        crate::protocol::types::DataType::Int64,
    )]);
    let memtable = std::sync::Arc::new(MemTable::new("db".into(), "t".into(), "p".into(), schema));

    registry
        .register_memtable_tombstones(
            21,
            "db",
            "t",
            None,
            std::sync::Arc::clone(&memtable),
            [5, 9],
        )
        .unwrap();
    registry
        .register_memtable_tombstones(
            21,
            "db",
            "t",
            None,
            std::sync::Arc::clone(&memtable),
            [9, 1, 3],
        )
        .unwrap();
    registry
        .register_memtable_tombstones(
            21,
            "db",
            "t",
            None,
            std::sync::Arc::clone(&memtable),
            [1, 5, 9],
        )
        .unwrap();

    let positions = registry
        .memtable_positions(21, memtable.id())
        .expect("positions recorded");
    let merged: Vec<u32> = positions.iter().copied().collect();
    assert_eq!(merged, vec![1, 3, 5, 9]);
}
