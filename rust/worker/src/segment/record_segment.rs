use super::types::{LogMaterializer, MaterializedLogRecord, SegmentWriter};
use crate::blockstore::Blockfile;
use crate::execution::data::data_chunk::Chunk;
use crate::types::LogRecord;

struct RecordSegment {
    records: Box<dyn Blockfile>,
}

impl SegmentWriter for RecordSegment {
    fn begin_transaction(&self) {
        todo!()
    }

    fn apply_materialized_log_chunk(&self, records: Chunk<MaterializedLogRecord>) {
        todo!()
    }

    fn apply_log_chunk(&self, records: Chunk<LogRecord>) {
        todo!()
    }

    fn commit_transaction(&self) {
        todo!()
    }

    fn rollback_transaction(&self) {
        todo!()
    }
}

impl LogMaterializer for RecordSegment {
    fn materialize(&self, records: Chunk<LogRecord>) -> Chunk<MaterializedLogRecord> {
        todo!()
    }
}
