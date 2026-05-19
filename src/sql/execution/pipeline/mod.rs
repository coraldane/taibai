pub mod push_filter;
pub mod push_project;
pub mod push_aggregate;
pub mod push_limit;
pub mod mono_pipeline;

use crate::common::error::Result;
use crate::types::batch::RecordBatch;

pub trait PushOp {
    fn push(&mut self, batch: RecordBatch) -> Result<()>;
    fn finalize(self: Box<Self>) -> Result<Option<RecordBatch>>;
}
