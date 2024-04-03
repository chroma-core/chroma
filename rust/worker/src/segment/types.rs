use crate::execution::data::data_chunk::Chunk;
use crate::types::{LogRecord, Metadata};

pub(super) struct MaterializedLogRecord<'a> {
    segment_offset_id: u32,
    log_record: &'a LogRecord,
    materialized_record: DataRecord<'a>,
}

pub(super) struct DataRecord<'a> {
    id: &'a str,
    embedding: &'a [f32],
    metadata: &'a Option<Metadata>,
    document: &'a Option<String>,
}

pub(super) trait SegmentWriter {
    fn begin_transaction(&self);
    fn apply_materialized_log_chunk(&self, records: Chunk<MaterializedLogRecord>);
    fn apply_log_chunk(&self, records: Chunk<LogRecord>);
    fn commit_transaction(&self);
    fn rollback_transaction(&self);
}

pub(crate) trait LogMaterializer: SegmentWriter {
    fn materialize(&self, records: Chunk<LogRecord>) -> Chunk<MaterializedLogRecord>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{MetadataValue, Operation, OperationRecord};
    use std::collections::HashMap;

    // This is just a POC test to show how the materialize method could be tested, we can
    // remove it later
    #[test]
    fn test_materialize() {
        let mut metadata_1 = HashMap::new();
        metadata_1.insert("key".to_string(), MetadataValue::Str("value".to_string()));
        let metadata_1 = Some(metadata_1);

        let data = vec![
            LogRecord {
                log_offset: 1,
                record: OperationRecord {
                    id: "embedding_id_1".to_string(),
                    embedding: None,
                    encoding: None,
                    metadata: None,
                    operation: Operation::Add,
                },
            },
            LogRecord {
                log_offset: 2,
                record: OperationRecord {
                    id: "embedding_id_2".to_string(),
                    embedding: None,
                    encoding: None,
                    metadata: None,
                    operation: Operation::Add,
                },
            },
            LogRecord {
                log_offset: 3,
                record: OperationRecord {
                    id: "embedding_id_1".to_string(),
                    embedding: None,
                    encoding: None,
                    metadata: None,
                    operation: Operation::Add,
                },
            },
        ];
        let data: Chunk<LogRecord> = Chunk::new(data.into());

        let materialized_data = data
            .iter()
            .map(|record| MaterializedLogRecord {
                segment_offset_id: 0,
                log_record: record.0,
                materialized_record: DataRecord {
                    id: &record.0.record.id,
                    embedding: &[],
                    metadata: &metadata_1,
                    document: &None,
                },
            })
            .collect::<Vec<_>>();

        let materialized_chunk = Chunk::new(materialized_data.into());
        drop(materialized_chunk);
        drop(data);
    }
}
