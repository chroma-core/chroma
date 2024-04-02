use crate::execution::data::data_chunk::Chunk;
use crate::types::LogRecord;

struct MaterializedLogRecord<'a> {
    segment_offset_id: u32,
    log_record: &'a LogRecord,
}

trait SegmentWriter {
    fn begin_transaction(&self);
    fn apply_materialized_log_chunk(&self, records: Chunk<MaterializedLogRecord>);
    fn apply_log_chunk(&self, records: Chunk<LogRecord>);
    fn commit_transaction(&self);
    fn rollback_transaction(&self);
}

trait LogMaterializer: SegmentWriter {
    fn materialize(&self, records: Chunk<LogRecord>) -> Chunk<MaterializedLogRecord>;
}
