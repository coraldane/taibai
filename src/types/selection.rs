use crate::types::batch::RecordBatch;

/// Compact row selection shared by vectorized operators. Keeping row ids
/// separate from values avoids materialising `ScalarValue` objects merely to
/// move surviving rows between operators.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SelectionVector {
    indices: Vec<u32>,
}

impl SelectionVector {
    pub fn from_mask(mask: &[bool]) -> Self {
        let indices = mask
            .iter()
            .enumerate()
            .filter_map(|(idx, keep)| keep.then_some(idx as u32))
            .collect();
        Self { indices }
    }

    pub fn all(len: usize) -> Self {
        Self {
            indices: (0..len as u32).collect(),
        }
    }

    pub fn indices(&self) -> &[u32] {
        &self.indices
    }

    pub fn len(&self) -> usize {
        self.indices.len()
    }

    pub fn is_empty(&self) -> bool {
        self.indices.is_empty()
    }

    pub fn apply(&self, batch: &RecordBatch) -> RecordBatch {
        if self.len() == batch.num_rows()
            && self
                .indices
                .iter()
                .enumerate()
                .all(|(idx, selected)| *selected as usize == idx)
        {
            return batch.clone();
        }
        let columns = batch
            .columns
            .iter()
            .map(|column| column.take(&self.indices))
            .collect();
        RecordBatch::try_new(batch.schema.clone(), columns)
            .expect("selection preserves record-batch schema")
    }
}

#[cfg(test)]
#[path = "selection_test.rs"]
mod tests;
