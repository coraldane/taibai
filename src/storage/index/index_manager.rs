#![allow(dead_code)]
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, RwLock};

use crate::common::error::Result;
use crate::storage::index::sparse_index::SparseIndexImpl;
use crate::storage::segment_manager::SegmentManager;
use crate::types::scalar::ScalarValue;

pub struct IndexManager {
    indexes: RwLock<HashMap<String, HashMap<String, Arc<SparseIndexImpl>>>>,
    segment_mgr: Arc<SegmentManager>,
}

impl IndexManager {
    pub fn new(segment_mgr: Arc<SegmentManager>) -> Self {
        Self {
            indexes: RwLock::new(HashMap::new()),
            segment_mgr,
        }
    }

    pub fn get_or_create_index(&self, db: &str, table: &str) -> Arc<SparseIndexImpl> {
        let indexes = self.indexes.read().unwrap();

        if let Some(db_indexes) = indexes.get(db) {
            if let Some(idx) = db_indexes.get(table) {
                return idx.clone();
            }
        }

        drop(indexes);

        let new_index = Arc::new(SparseIndexImpl::new());
        let mut indexes = self.indexes.write().unwrap();

        indexes
            .entry(db.to_string())
            .or_insert_with(HashMap::new)
            .insert(table.to_string(), new_index.clone());

        new_index
    }

    pub fn rebuild_index_from_segments(
        &self,
        db: &str,
        table: &str,
        segments: &[String],
    ) -> Result<()> {
        let index = self.get_or_create_index(db, table);
        index.clear();

        let mut current_row_id: i64 = 0;
        let mut block_data = Vec::new();

        for (_seg_idx, seg_path) in segments.iter().enumerate() {
            let reader = self.segment_mgr.open_reader(Path::new(seg_path))?;

            let num_row_groups = reader.num_row_groups();
            for rg_idx in 0..num_row_groups {
                let rg_row_count = reader.row_group_rows(rg_idx) as i64;

                if rg_row_count == 0 {
                    continue;
                }

                block_data.push(crate::storage::index::sparse_index::SparseBlockData {
                    block_id: block_data.len(),
                    zone_map: None,
                    bloom_filter: None,
                    start_row_id: current_row_id,
                    row_count: rg_row_count,
                });

                current_row_id += rg_row_count;
            }
        }

        index.rebuild_from_blocks(block_data);
        Ok(())
    }

    pub fn search(&self, db: &str, table: &str, value: &ScalarValue) -> Option<Vec<i64>> {
        let indexes = self.indexes.read().unwrap();

        let idx = indexes.get(db).and_then(|db_idx| db_idx.get(table))?;

        Some(idx.search(value))
    }

    pub fn range_search(
        &self,
        db: &str,
        table: &str,
        min: &ScalarValue,
        max: &ScalarValue,
    ) -> Option<Vec<i64>> {
        let indexes = self.indexes.read().unwrap();

        let idx = indexes.get(db).and_then(|db_idx| db_idx.get(table))?;

        Some(idx.range_search(min, max))
    }

    pub fn get_index(&self, db: &str, table: &str) -> Option<Arc<SparseIndexImpl>> {
        let indexes = self.indexes.read().unwrap();
        indexes
            .get(db)
            .and_then(|db_idx| db_idx.get(table))
            .cloned()
    }

    pub fn update_index(&self, db: &str, table: &str, key: &ScalarValue, row_id: i64) {
        if let Some(idx) = self.get_index(db, table) {
            idx.insert(key, row_id);
        }
    }

    pub fn mark_index_stale(&self, db: &str, table: &str) {
        if let Some(idx) = self.get_index(db, table) {
            idx.mark_stale();
        }
    }

    pub fn is_index_stale(&self, db: &str, table: &str) -> bool {
        if let Some(idx) = self.get_index(db, table) {
            idx.is_stale()
        } else {
            false
        }
    }

    pub fn clear_index(&self, db: &str, table: &str) {
        if let Some(idx) = self.get_index(db, table) {
            idx.clear();
        }
    }

    pub fn index_size(&self, db: &str, table: &str) -> i64 {
        if let Some(idx) = self.get_index(db, table) {
            idx.size()
        } else {
            0
        }
    }

    pub fn index_block_count(&self, db: &str, table: &str) -> usize {
        if let Some(idx) = self.get_index(db, table) {
            idx.block_count()
        } else {
            0
        }
    }
}

impl Default for IndexManager {
    fn default() -> Self {
        let segment_mgr = SegmentManager::new(std::path::Path::new("/tmp/monax_default"))
            .expect("failed to create default SegmentManager");
        Self::new(Arc::new(segment_mgr))
    }
}
