use crate::execution::data::data_chunk::Chunk;
use crate::types::{LogRecord, Metadata};

pub(crate) struct MaterializedLogRecord<'a> {
    pub(super) segment_offset_id: u32,
    pub(super) log_record: &'a LogRecord,
    pub(super) materialized_record: DataRecord<'a>,
}

impl<'a> MaterializedLogRecord<'a> {
    pub(crate) fn new(
        segment_offset_id: u32,
        log_record: &'a LogRecord,
        materialized_record: DataRecord<'a>,
    ) -> Self {
        Self {
            segment_offset_id,
            log_record,
            materialized_record,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct DataRecord<'a> {
    pub(crate) id: &'a str,
    pub(crate) embedding: &'a [f32],
    pub(crate) metadata: Option<Metadata>,
    pub(crate) document: Option<&'a str>,
}

impl DataRecord<'_> {
    pub(crate) fn get_size(&self) -> usize {
        let id_size = self.id.len();
        let embedding_size = self.embedding.len() * std::mem::size_of::<f32>();
        // TODO: use serialized_metadata size to calculate the size
        let metadata_size = 0;
        let document_size = match self.document {
            Some(document) => document.len(),
            None => 0,
        };
        id_size + embedding_size + metadata_size + document_size
    }
}

pub(super) trait SegmentWriter {
    fn apply_materialized_log_chunk(&self, records: Chunk<MaterializedLogRecord>);
    fn apply_log_chunk(&self, records: Chunk<LogRecord>);
    fn commit(&self);
}

pub(crate) trait LogMaterializer: SegmentWriter {
    fn materialize<'chunk>(
        &self,
        records: &'chunk Chunk<LogRecord>,
    ) -> Chunk<MaterializedLogRecord<'chunk>>;
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
                    metadata: metadata_1.clone(),
                    document: None,
                },
            })
            .collect::<Vec<_>>();

        let materialized_chunk = Chunk::new(materialized_data.into());
        drop(materialized_chunk);
        drop(data);
    }
}
