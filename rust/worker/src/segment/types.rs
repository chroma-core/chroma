use crate::types::LogRecord;

pub(super) trait SegmentWriter {
    fn begin_transaction(&mut self);
    fn write_records(&mut self, records: Vec<Box<LogRecord>>, offset_ids: Vec<Option<u32>>);
    fn commit_transaction(&mut self);
    fn rollback_transaction(&mut self);
}

pub(super) trait OffsetIdAssigner: SegmentWriter {
    fn assign_offset_ids(&mut self, records: Vec<Box<LogRecord>>) -> Vec<Option<u32>>;
}
