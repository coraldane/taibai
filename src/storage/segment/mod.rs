use crate::storage::index::bloomfilter::BloomFilter;

pub mod constants;
pub mod footer;
pub mod reader;
pub mod writer;

#[derive(Debug, Clone)]
pub struct RowGroupMeta {
    pub offset: u64,
    pub length: u64,
    pub row_count: u64,
    pub column_compressed_sizes: Vec<u64>,
    pub compressed_sizes: Vec<u64>,
    pub uncompressed_sizes: Vec<u64>,
    pub zone_maps: Vec<crate::storage::partition::pruning::ZoneMap>,
    pub bloom_filters: Vec<Option<BloomFilter>>,
}

pub use constants::{
    DEFAULT_ROW_GROUP_SIZE, FOOTER_SIZE, MAX_SEGMENT_SIZE, SEGMENT_HEADER_SIZE, SEGMENT_MAGIC,
    SEGMENT_VERSION,
};
pub use reader::SegmentReader;
pub use writer::{encode_segment_footer, encode_segment_header, SegmentWriter};

pub use crate::storage::codec::encoding::decode_scalar_with_tag;
pub use crate::storage::partition::pruning::{RangeClause, RangeOp, RowGroupPredicate, ScanStats, ZoneMap};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::types::DataType;
    use crate::types::batch::{ColumnDef, RecordBatch, Schema};
    use crate::types::scalar::ScalarValue;
    use tempfile::TempDir;

    #[test]
    fn test_segment_write_read_roundtrip() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("seg_00001.dat");

        let schema = Schema::new(vec![
            ColumnDef::new("id", DataType::Int64),
            ColumnDef::new("name", DataType::VarChar),
        ]);

        {
            let mut writer = SegmentWriter::create(&path, schema.clone(), 8192).unwrap();
            let mut batch = RecordBatch::new(schema.clone());
            for i in 0..100i64 {
                batch
                    .column_mut(0)
                    .unwrap()
                    .push(&ScalarValue::Int64(Some(i)))
                    .unwrap();
                batch
                    .column_mut(1)
                    .unwrap()
                    .push(&ScalarValue::VarChar(Some(format!("user_{}", i))))
                    .unwrap();
            }
            writer.write_batch(&batch).unwrap();
            writer.seal().unwrap();
        }

        let reader = SegmentReader::open(&path).unwrap();
        assert_eq!(reader.row_count(), 100);
        assert_eq!(reader.schema().columns.len(), 2);

        let batches = reader.scan(1000).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 100);

        assert_eq!(
            batches[0].scalar(0, 0).unwrap(),
            ScalarValue::Int64(Some(0))
        );
        assert_eq!(
            batches[0].scalar(99, 0).unwrap(),
            ScalarValue::Int64(Some(99))
        );
        assert_eq!(
            batches[0].scalar(0, 1).unwrap(),
            ScalarValue::VarChar(Some("user_0".into()))
        );
        assert_eq!(
            batches[0].scalar(99, 1).unwrap(),
            ScalarValue::VarChar(Some("user_99".into()))
        );
    }

    #[test]
    fn test_segment_with_nulls() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("seg_00002.dat");

        let schema = Schema::new(vec![ColumnDef::new("val", DataType::Int32)]);

        {
            let mut writer = SegmentWriter::create(&path, schema.clone(), 8192).unwrap();
            let mut batch = RecordBatch::new(schema.clone());
            batch
                .column_mut(0)
                .unwrap()
                .push(&ScalarValue::Int32(Some(1)))
                .unwrap();
            batch
                .column_mut(0)
                .unwrap()
                .push(&ScalarValue::Int32(None))
                .unwrap();
            batch
                .column_mut(0)
                .unwrap()
                .push(&ScalarValue::Int32(Some(3)))
                .unwrap();
            writer.write_batch(&batch).unwrap();
            writer.seal().unwrap();
        }

        let reader = SegmentReader::open(&path).unwrap();
        let batches = reader.scan(1000).unwrap();
        assert_eq!(batches[0].num_rows(), 3);
        assert_eq!(
            batches[0].scalar(0, 0).unwrap(),
            ScalarValue::Int32(Some(1))
        );
        assert!(batches[0].scalar(1, 0).unwrap().is_null());
        assert_eq!(
            batches[0].scalar(2, 0).unwrap(),
            ScalarValue::Int32(Some(3))
        );
    }

    #[test]
    fn test_empty_segment() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("seg_00003.dat");

        let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);

        {
            let writer = SegmentWriter::create(&path, schema.clone(), 8192).unwrap();
            writer.seal().unwrap();
        }

        let reader = SegmentReader::open(&path).unwrap();
        assert_eq!(reader.row_count(), 0);
    }

    #[test]
    fn test_multiple_row_groups() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("seg_00004.dat");

        let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);

        {
            let mut writer = SegmentWriter::create(&path, schema.clone(), 10).unwrap();
            let mut batch = RecordBatch::new(schema.clone());
            for i in 0..25i64 {
                batch
                    .column_mut(0)
                    .unwrap()
                    .push(&ScalarValue::Int64(Some(i)))
                    .unwrap();
            }
            writer.write_batch(&batch).unwrap();
            writer.seal().unwrap();
        }

        let reader = SegmentReader::open(&path).unwrap();
        assert_eq!(reader.row_count(), 25);
        assert_eq!(reader.num_row_groups(), 3);

        let batches = reader.scan(1000).unwrap();
        assert_eq!(batches.len(), 3);
        assert_eq!(batches[0].num_rows(), 10);
        assert_eq!(batches[1].num_rows(), 10);
        assert_eq!(batches[2].num_rows(), 5);
    }

    #[test]
    fn test_zone_maps_populated() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("seg_zm.dat");

        let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);

        {
            let mut writer = SegmentWriter::create(&path, schema.clone(), 10).unwrap();
            let mut batch = RecordBatch::new(schema.clone());
            for i in 0..25i64 {
                batch
                    .column_mut(0)
                    .unwrap()
                    .push(&ScalarValue::Int64(Some(i)))
                    .unwrap();
            }
            writer.write_batch(&batch).unwrap();
            writer.seal().unwrap();
        }

        let reader = SegmentReader::open(&path).unwrap();
        assert_eq!(reader.num_row_groups(), 3);

        let zm0 = &reader.zone_maps(0)[0];
        assert_eq!(zm0.min_value, ScalarValue::Int64(Some(0)));
        assert_eq!(zm0.max_value, ScalarValue::Int64(Some(9)));
        assert_eq!(zm0.null_count, 0);
        assert_eq!(zm0.row_count, 10);

        let zm2 = &reader.zone_maps(2)[0];
        assert_eq!(zm2.min_value, ScalarValue::Int64(Some(20)));
        assert_eq!(zm2.max_value, ScalarValue::Int64(Some(24)));
        assert_eq!(zm2.row_count, 5);
    }

    #[test]
    fn test_row_group_predicate_pruning() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("seg_prune.dat");

        let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);

        {
            let mut writer = SegmentWriter::create(&path, schema.clone(), 10).unwrap();
            let mut batch = RecordBatch::new(schema.clone());
            for i in 0..25i64 {
                batch
                    .column_mut(0)
                    .unwrap()
                    .push(&ScalarValue::Int64(Some(i)))
                    .unwrap();
            }
            writer.write_batch(&batch).unwrap();
            writer.seal().unwrap();
        }

        let reader = SegmentReader::open(&path).unwrap();

        let pred = RowGroupPredicate::new(vec![RangeClause {
            column_index: 0,
            op: RangeOp::Eq,
            value: ScalarValue::Int64(Some(22)),
        }]);
        let batches = reader.scan_with_predicate(1000, Some(&pred)).unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 5, "only row group 2 should be returned");

        let pred = RowGroupPredicate::new(vec![RangeClause {
            column_index: 0,
            op: RangeOp::Gt,
            value: ScalarValue::Int64(Some(100)),
        }]);
        let batches = reader.scan_with_predicate(1000, Some(&pred)).unwrap();
        assert!(batches.is_empty(), "all row groups should be pruned");

        let pred = RowGroupPredicate::new(vec![RangeClause {
            column_index: 0,
            op: RangeOp::Ge,
            value: ScalarValue::Int64(Some(15)),
        }]);
        let batches = reader.scan_with_predicate(1000, Some(&pred)).unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 15, "row groups 1+2 should be kept (10 + 5)");

        let pred = RowGroupPredicate::new(vec![]);
        let batches = reader.scan_with_predicate(1000, Some(&pred)).unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 25);
    }

    #[test]
    fn test_row_group_predicate_with_nulls() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("seg_prune_null.dat");

        let schema = Schema::new(vec![ColumnDef::new("v", DataType::Int32)]);

        {
            let mut writer = SegmentWriter::create(&path, schema.clone(), 4).unwrap();
            let mut batch = RecordBatch::new(schema.clone());
            batch
                .column_mut(0)
                .unwrap()
                .push(&ScalarValue::Int32(Some(1)))
                .unwrap();
            batch
                .column_mut(0)
                .unwrap()
                .push(&ScalarValue::Int32(Some(2)))
                .unwrap();
            batch
                .column_mut(0)
                .unwrap()
                .push(&ScalarValue::Int32(Some(3)))
                .unwrap();
            batch
                .column_mut(0)
                .unwrap()
                .push(&ScalarValue::Int32(Some(4)))
                .unwrap();
            for _ in 0..4 {
                batch
                    .column_mut(0)
                    .unwrap()
                    .push(&ScalarValue::Int32(None))
                    .unwrap();
            }
            writer.write_batch(&batch).unwrap();
            writer.seal().unwrap();
        }

        let reader = SegmentReader::open(&path).unwrap();
        assert_eq!(reader.num_row_groups(), 2);

        let pred = RowGroupPredicate::new(vec![RangeClause {
            column_index: 0,
            op: RangeOp::Eq,
            value: ScalarValue::Int32(Some(99)),
        }]);
        let batches = reader.scan_with_predicate(1000, Some(&pred)).unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 0);

        let pred = RowGroupPredicate::new(vec![RangeClause {
            column_index: 0,
            op: RangeOp::Eq,
            value: ScalarValue::Int32(Some(2)),
        }]);
        let batches = reader.scan_with_predicate(1000, Some(&pred)).unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 4, "rg 0 returned, rg 1 (all null) pruned");
    }

    #[test]
    fn test_segment_header_validation() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("seg_hdr.dat");
        let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);

        {
            let writer = SegmentWriter::create(&path, schema.clone(), 1024).unwrap();
            writer.seal().unwrap();
        }

        let mut bytes = std::fs::read(&path).unwrap();
        bytes[0] = 0xFF;
        std::fs::write(&path, &bytes).unwrap();

        let err = match SegmentReader::open(&path) {
            Ok(_) => panic!("opening corrupted segment should fail"),
            Err(e) => e,
        };
        let msg = format!("{}", err);
        assert!(msg.contains("magic"), "error should mention magic: {}", msg);
    }

    #[test]
    fn test_seal_with_deletes_then_scan_filters_rows() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("seg_del.dat");
        let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);

        let mut writer = SegmentWriter::create(&path, schema.clone(), 32).unwrap();
        let mut batch = RecordBatch::new(schema.clone());
        for i in 0..100i64 {
            batch
                .column_mut(0)
                .unwrap()
                .push(&ScalarValue::Int64(Some(i)))
                .unwrap();
        }
        writer.write_batch(&batch).unwrap();

        let mut bitmap = vec![0u8; 13];
        let mut deleted = 0u64;
        for i in 0..100u64 {
            if i % 2 == 1 {
                bitmap[(i / 8) as usize] |= 1u8 << (i % 8);
                deleted += 1;
            }
        }
        writer.seal_with_deletes(&bitmap, deleted).unwrap();

        let reader = SegmentReader::open(&path).unwrap();
        assert_eq!(reader.row_count(), 100);
        assert_eq!(reader.deleted_row_count(), 50);
        assert_eq!(reader.delete_bitmap().len(), 13);

        let mut stats = ScanStats::default();
        let batches = reader
            .scan_with_predicate_and_stats(4096, None, Some(&mut stats))
            .unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 50, "scan should skip deleted rows");
        for b in &batches {
            for r in 0..b.num_rows() {
                let v = b.scalar(r, 0).unwrap();
                if let ScalarValue::Int64(Some(n)) = v {
                    assert!(n % 2 == 0, "deleted (odd) row leaked through: {}", n);
                }
            }
        }
        assert_eq!(stats.rows_filtered_by_delete_bitmap, 50);
        assert_eq!(stats.rows_scanned, 50);
    }

    #[test]
    fn test_rewrite_footer_with_deletes_preserves_data() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("seg_rewrite.dat");
        let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);

        let mut writer = SegmentWriter::create(&path, schema.clone(), 50).unwrap();
        let mut batch = RecordBatch::new(schema.clone());
        for i in 0..200i64 {
            batch
                .column_mut(0)
                .unwrap()
                .push(&ScalarValue::Int64(Some(i)))
                .unwrap();
        }
        writer.write_batch(&batch).unwrap();
        writer.seal().unwrap();

        let original = std::fs::read(&path).unwrap();
        let original_data_section = {
            let r = SegmentReader::open(&path).unwrap();
            let _total_rows = r.row_count();
            drop(r);
            let footer_off =
                u64::from_le_bytes(original[original.len() - 8..].try_into().unwrap()) as usize;
            original[..footer_off].to_vec()
        };

        let mut bitmap = vec![0u8; 25];
        let mut deleted = 0u64;
        for i in 10u64..20 {
            bitmap[(i / 8) as usize] |= 1u8 << (i % 8);
            deleted += 1;
        }
        SegmentReader::rewrite_footer_with_deletes(&path, &bitmap, deleted).unwrap();

        let after = std::fs::read(&path).unwrap();
        let after_footer_off =
            u64::from_le_bytes(after[after.len() - 8..].try_into().unwrap()) as usize;
        assert_eq!(
            after[..after_footer_off],
            original_data_section[..],
            "rewrite_footer_with_deletes must not modify data blocks"
        );

        let reader = SegmentReader::open(&path).unwrap();
        assert_eq!(reader.row_count(), 200);
        assert_eq!(reader.deleted_row_count(), 10);
        let batches = reader.scan(4096).unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 190);
        for b in &batches {
            for r in 0..b.num_rows() {
                if let Some(ScalarValue::Int64(Some(n))) = b.scalar(r, 0) {
                    assert!(!(10..20).contains(&n), "deleted row {} leaked", n);
                }
            }
        }
    }
}
