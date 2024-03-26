use crate::types::EmbeddingRecord;

pub(super) trait SegmentWriter {
    fn begin_transaction(&mut self);
    fn write_records(&mut self, records: Vec<Box<EmbeddingRecord>>, offset_ids: Vec<Option<u32>>);
    fn commit_transaction(&mut self);
    fn rollback_transaction(&self);
}

pub(super) trait OffsetIdAssigner: SegmentWriter {
    fn assign_offset_ids(&self, records: Vec<Box<EmbeddingRecord>>) -> Vec<Option<u32>>;
}
