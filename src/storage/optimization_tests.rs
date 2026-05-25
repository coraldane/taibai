#[cfg(test)]
mod tests {
    use crate::storage::index::bloomfilter::BloomFilter;
    use crate::storage::index::sparse_index::{SparseBlock, SparseBlockData, SparseIndexImpl};
    use crate::storage::segment::{RangeClause, RangeOp, RowGroupPredicate, ZoneMap};
    use crate::types::scalar::ScalarValue;

    #[test]
    fn test_bloomfilter_basic() {
        let mut bf = BloomFilter::new(1000, 0.01);

        bf.add(&ScalarValue::Int32(Some(42)));
        bf.add(&ScalarValue::Int32(Some(100)));
        bf.add(&ScalarValue::Int64(Some(999)));
        bf.add(&ScalarValue::VarChar(Some("hello".to_string())));

        // Should contain inserted values
        assert!(bf.might_contain(&ScalarValue::Int32(Some(42))));
        assert!(bf.might_contain(&ScalarValue::Int32(Some(100))));
        assert!(bf.might_contain(&ScalarValue::Int64(Some(999))));
        assert!(bf.might_contain(&ScalarValue::VarChar(Some("hello".to_string()))));

        // Definitely not contain these
        // Note: BloomFilter may have false positives, so we test for definite negatives
        // only for values that are unlikely to collide
        assert!(!bf.might_contain(&ScalarValue::Int32(Some(-999999))));
        assert!(!bf.might_contain(&ScalarValue::VarChar(Some("__nonexistent__".to_string()))));
    }

    #[test]
    fn test_bloomfilter_batch_insert() {
        let mut bf = BloomFilter::new(1000, 0.01);

        let values: Vec<ScalarValue> = (0..100).map(|i| ScalarValue::Int32(Some(i))).collect();

        for v in &values {
            bf.add(v);
        }

        // All inserted values should be found
        for v in &values {
            assert!(bf.might_contain(v), "Value {:?} should be in filter", v);
        }
    }

    #[test]
    fn test_bloomfilter_serialization() {
        let mut bf = BloomFilter::new(1000, 0.01);

        bf.add(&ScalarValue::Int32(Some(1)));
        bf.add(&ScalarValue::Int32(Some(2)));
        bf.add(&ScalarValue::VarChar(Some("test".to_string())));

        let encoded = bf.serialize();

        let decoded = BloomFilter::deserialize(&encoded).unwrap();

        assert!(decoded.might_contain(&ScalarValue::Int32(Some(1))));
        assert!(decoded.might_contain(&ScalarValue::Int32(Some(2))));
        assert!(decoded.might_contain(&ScalarValue::VarChar(Some("test".to_string()))));
    }

    #[test]
    fn test_zone_map_contains() {
        let zm = ZoneMap {
            min_value: ScalarValue::Int32(Some(10)),
            max_value: ScalarValue::Int32(Some(100)),
            null_count: 0,
            row_count: 100,
        };

        // Contains tests
        assert!(zm.contains(&ScalarValue::Int32(Some(50))));
        assert!(zm.contains(&ScalarValue::Int32(Some(10))));
        assert!(zm.contains(&ScalarValue::Int32(Some(100))));

        // Not contains tests
        assert!(!zm.contains(&ScalarValue::Int32(Some(5))));
        assert!(!zm.contains(&ScalarValue::Int32(Some(101))));
    }

    #[test]
    fn test_zone_map_overlaps() {
        let zm = ZoneMap {
            min_value: ScalarValue::Int32(Some(10)),
            max_value: ScalarValue::Int32(Some(100)),
            null_count: 0,
            row_count: 100,
        };

        // Overlaps with range
        assert!(zm.overlaps(&ScalarValue::Int32(Some(20)), &ScalarValue::Int32(Some(50))));
        assert!(zm.overlaps(&ScalarValue::Int32(Some(5)), &ScalarValue::Int32(Some(15))));
        assert!(zm.overlaps(
            &ScalarValue::Int32(Some(50)),
            &ScalarValue::Int32(Some(150))
        ));

        // No overlap
        assert!(!zm.overlaps(&ScalarValue::Int32(Some(1)), &ScalarValue::Int32(Some(5))));
        assert!(!zm.overlaps(
            &ScalarValue::Int32(Some(101)),
            &ScalarValue::Int32(Some(200))
        ));
    }

    #[test]
    fn test_sparse_index_basic() {
        let index = SparseIndexImpl::new();
        index.add_block(SparseBlock::new(0, 0, 300));

        // Insert some values
        index.insert(&ScalarValue::Int32(Some(1)), 0);
        index.insert(&ScalarValue::Int32(Some(5)), 100);
        index.insert(&ScalarValue::Int32(Some(10)), 200);

        // Search for exact match
        let found = index.search(&ScalarValue::Int32(Some(5)));
        assert!(found.contains(&100));

        // Search for non-existent value
        let not_found = index.search(&ScalarValue::Int32(Some(999)));
        assert!(not_found.is_empty());
    }

    #[test]
    fn test_sparse_index_range_search() {
        let index = SparseIndexImpl::new();
        index.add_block(SparseBlock::new(0, 0, 100));

        for i in 0..100 {
            index.insert(&ScalarValue::Int32(Some(i * 10)), i as i64);
        }

        // Range search [30, 70]
        let found =
            index.range_search(&ScalarValue::Int32(Some(30)), &ScalarValue::Int32(Some(70)));

        // Should contain values 30, 40, 50, 60, 70
        assert!(found.contains(&3)); // value 30
        assert!(found.contains(&4)); // value 40
        assert!(found.contains(&5)); // value 50
        assert!(found.contains(&6)); // value 60
        assert!(found.contains(&7)); // value 70
    }

    #[test]
    fn test_sparse_index_rebuild() {
        let index = SparseIndexImpl::new();

        // Initial data
        let blocks = vec![
            SparseBlockData {
                block_id: 0,
                zone_map: None,
                bloom_filter: None,
                start_row_id: 0,
                row_count: 1000,
            },
            SparseBlockData {
                block_id: 1,
                zone_map: None,
                bloom_filter: None,
                start_row_id: 1000,
                row_count: 1000,
            },
        ];

        index.rebuild_from_blocks(blocks);

        assert_eq!(index.block_count(), 2);
        assert_eq!(index.size(), 2000);
    }

    #[test]
    fn test_row_group_predicate_with_bloomfilter() {
        let pred = RowGroupPredicate::new(vec![RangeClause {
            column_index: 0,
            op: RangeOp::Eq,
            value: ScalarValue::Int32(Some(42)),
        }]);

        // Create zone map that contains the value
        let zone_maps = vec![ZoneMap {
            min_value: ScalarValue::Int32(Some(0)),
            max_value: ScalarValue::Int32(Some(100)),
            null_count: 0,
            row_count: 100,
        }];

        // ZoneMap should match
        assert!(pred.matches_row_group(&zone_maps));
    }

    #[test]
    fn test_two_level_filtering() {
        // Simulate two-level filtering: BloomFilter -> ZoneMap

        // Step 1: BloomFilter rejects quickly
        let mut bf = BloomFilter::new(1000, 0.01);
        for i in 0..100 {
            bf.add(&ScalarValue::Int32(Some(i)));
        }

        // Value 50 should pass BloomFilter
        assert!(bf.might_contain(&ScalarValue::Int32(Some(50))));

        // Value 999 should be rejected by BloomFilter
        assert!(!bf.might_contain(&ScalarValue::Int32(Some(999))));

        // Step 2: ZoneMap further narrows down
        let zm = ZoneMap {
            min_value: ScalarValue::Int32(Some(40)),
            max_value: ScalarValue::Int32(Some(60)),
            null_count: 0,
            row_count: 21,
        };

        // After passing BloomFilter, check ZoneMap
        assert!(zm.contains(&ScalarValue::Int32(Some(50))));
        assert!(!zm.contains(&ScalarValue::Int32(Some(70))));
    }

    #[test]
    fn test_bloomfilter_false_positive_rate() {
        // Test that false positive rate is within expected bounds
        let n = 10000;
        let fp_rate = 0.01;

        let mut bf = BloomFilter::new(n, fp_rate);

        // Insert n items
        for i in 0..n {
            bf.add(&ScalarValue::Int32(Some(i as i32)));
        }

        // Test for items not in the filter
        let mut false_positives = 0;
        let test_count = 1000;
        for i in n..(n + test_count) {
            if bf.might_contain(&ScalarValue::Int32(Some(i as i32))) {
                false_positives += 1;
            }
        }

        let actual_fp_rate = false_positives as f64 / test_count as f64;

        // Should be approximately the target rate (allowing some variance)
        assert!(
            actual_fp_rate < fp_rate * 3.0,
            "False positive rate {} exceeds tolerance (target: {})",
            actual_fp_rate,
            fp_rate
        );
    }

    #[test]
    fn test_various_scalar_types() {
        let mut bf = BloomFilter::new(100, 0.01);

        // Test various types
        bf.add(&ScalarValue::Int8(Some(42)));
        bf.add(&ScalarValue::Int16(Some(1000)));
        bf.add(&ScalarValue::Int32(Some(100000)));
        bf.add(&ScalarValue::Int64(Some(10000000000)));
        bf.add(&ScalarValue::Float32(Some(3.14)));
        bf.add(&ScalarValue::Float64(Some(3.14159265358979)));
        bf.add(&ScalarValue::VarChar(Some("hello world".to_string())));
        bf.add(&ScalarValue::Date(Some(18628))); // 2021-01-01

        // All types should be found
        assert!(bf.might_contain(&ScalarValue::Int8(Some(42))));
        assert!(bf.might_contain(&ScalarValue::Int16(Some(1000))));
        assert!(bf.might_contain(&ScalarValue::Int32(Some(100000))));
        assert!(bf.might_contain(&ScalarValue::Int64(Some(10000000000))));
        assert!(bf.might_contain(&ScalarValue::Float32(Some(3.14))));
        assert!(bf.might_contain(&ScalarValue::Float64(Some(3.14159265358979))));
        assert!(bf.might_contain(&ScalarValue::VarChar(Some("hello world".to_string()))));
        assert!(bf.might_contain(&ScalarValue::Date(Some(18628))));
    }
}
