use std::collections::HashSet;

use thiserror::Error;

use tonic::async_trait;

use crate::{
    blockstore::provider::BlockfileProvider,
    errors::{ChromaError, ErrorCodes},
    execution::{data::data_chunk::Chunk, operator::Operator},
    segment::record_segment::{RecordSegmentReader, RecordSegmentReaderCreationError},
    types::{LogRecord, Operation, Segment},
};

#[derive(Debug)]
pub(crate) struct CountRecordsOperator {}

impl CountRecordsOperator {
    pub(crate) fn new() -> Box<Self> {
        Box::new(CountRecordsOperator {})
    }
}

#[derive(Debug)]
pub(crate) struct CountRecordsInput {
    record_segment_definition: Segment,
    blockfile_provider: BlockfileProvider,
    log_records: Chunk<LogRecord>,
}

impl CountRecordsInput {
    pub(crate) fn new(
        record_segment_definition: Segment,
        blockfile_provider: BlockfileProvider,
        log_records: Chunk<LogRecord>,
    ) -> Self {
        Self {
            record_segment_definition,
            blockfile_provider,
            log_records,
        }
    }
}

#[derive(Debug)]
pub(crate) struct CountRecordsOutput {
    pub(crate) count: usize,
}

#[derive(Error, Debug)]
pub(crate) enum CountRecordsError {
    #[error("Error reading record segment reader")]
    RecordSegmentReadError,
    #[error("Error creating record segment reader")]
    RecordSegmentError(#[from] RecordSegmentReaderCreationError),
}

impl ChromaError for CountRecordsError {
    fn code(&self) -> ErrorCodes {
        match self {
            CountRecordsError::RecordSegmentError(e) => e.code(),
            CountRecordsError::RecordSegmentReadError => ErrorCodes::Internal,
        }
    }
}

#[async_trait]
impl Operator<CountRecordsInput, CountRecordsOutput> for CountRecordsOperator {
    type Error = CountRecordsError;
    async fn run(
        &self,
        input: &CountRecordsInput,
    ) -> Result<CountRecordsOutput, CountRecordsError> {
        let segment_reader = RecordSegmentReader::from_segment(
            &input.record_segment_definition,
            &input.blockfile_provider,
        )
        .await;
        let reader = match segment_reader {
            Ok(r) => r,
            Err(e) => {
                println!("Error opening record segment");
                return Err(CountRecordsError::RecordSegmentError(*e));
            }
        };
        // Reconcile adds, updates and deletes.
        // Ids that exist in both the log and the segment (can be
        // in both deleted and not deleted state).
        let mut deleted_and_non_deleted_present_in_segment: HashSet<String> = HashSet::new();
        let mut res_count: i32 = 0;
        // In theory, we can sort all the ids here
        // and send them to the reader so that the reader
        // can process all in one iteration of the sparse index.
        // In practice, the blocks
        // will get cached so overall performance benefits
        // should not be significant.
        for (log_record, _) in input.log_records.iter() {
            match reader
                .data_exists_for_user_id(log_record.record.id.as_str())
                .await
            {
                Ok(exists) => {
                    if exists {
                        deleted_and_non_deleted_present_in_segment
                            .insert(log_record.record.id.clone());
                    }
                }
                Err(_) => {
                    println!("Error reading record segment");
                    return Err(CountRecordsError::RecordSegmentReadError);
                }
            }
        }
        // Ids that are present in the log and segment and their end state is not deleted.
        let mut non_deleted_present_in_segment: HashSet<String> =
            deleted_and_non_deleted_present_in_segment.clone();
        // Ids that are absent in the segment but present in log in non deleted state.
        let mut non_deleted_absent_in_segment: HashSet<String> = HashSet::new();
        for (log_record, _) in input.log_records.iter() {
            if deleted_and_non_deleted_present_in_segment.contains(log_record.record.id.as_str()) {
                match log_record.record.operation {
                    Operation::Add | Operation::Upsert => {
                        non_deleted_present_in_segment.insert(log_record.record.id.clone());
                    }
                    Operation::Delete => {
                        non_deleted_present_in_segment.remove(log_record.record.id.as_str());
                    }
                    Operation::Update => {}
                }
            } else {
                match log_record.record.operation {
                    Operation::Add | Operation::Upsert => {
                        non_deleted_absent_in_segment.insert(log_record.record.id.clone());
                    }
                    Operation::Delete => {
                        non_deleted_absent_in_segment.remove(log_record.record.id.as_str());
                    }
                    Operation::Update => {}
                }
            }
        }
        // Discount the records that are present in the record segment but have
        // been deleted more recently in the log.
        res_count -= (deleted_and_non_deleted_present_in_segment.len()
            - non_deleted_present_in_segment.len()) as i32;
        // Add the records that are absent in the record segment but
        // have been inserted more recently in the log.
        res_count += non_deleted_absent_in_segment.len() as i32;
        // Finally, add the count from the record segment.
        match reader.count().await {
            Ok(val) => {
                res_count += val as i32;
            }
            Err(_) => {
                println!("Error reading record segment");
                return Err(CountRecordsError::RecordSegmentReadError);
            }
        };
        Ok(CountRecordsOutput {
            count: res_count as usize,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, str::FromStr};

    use uuid::Uuid;

    use crate::{
        blockstore::provider::BlockfileProvider,
        execution::{
            data::data_chunk::Chunk,
            operator::Operator,
            operators::count_records::{CountRecordsInput, CountRecordsOperator},
        },
        segment::{record_segment::RecordSegmentWriter, LogMaterializer, SegmentWriter},
        types::{LogRecord, Operation, OperationRecord},
    };

    use crate::segment::types::SegmentFlusher;

    #[tokio::test]
    async fn test_merge_log_and_storage() {
        let in_memory_provider = BlockfileProvider::new_memory();
        let mut record_segment = crate::types::Segment {
            id: Uuid::from_str("00000000-0000-0000-0000-000000000000").expect("parse error"),
            r#type: crate::types::SegmentType::Record,
            scope: crate::types::SegmentScope::RECORD,
            collection: Some(
                Uuid::from_str("00000000-0000-0000-0000-000000000000").expect("parse error"),
            ),
            metadata: None,
            file_path: HashMap::new(),
        };
        {
            let segment_writer =
                RecordSegmentWriter::from_segment(&record_segment, &in_memory_provider)
                    .await
                    .expect("Error creating segment writer");
            let data = vec![
                LogRecord {
                    log_offset: 1,
                    record: OperationRecord {
                        id: "embedding_id_1".to_string(),
                        embedding: Some(vec![1.0, 2.0, 3.0]),
                        encoding: None,
                        metadata: None,
                        document: None,
                        operation: Operation::Add,
                    },
                },
                LogRecord {
                    log_offset: 2,
                    record: OperationRecord {
                        id: "embedding_id_2".to_string(),
                        embedding: Some(vec![4.0, 5.0, 6.0]),
                        encoding: None,
                        metadata: None,
                        document: None,
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
                        document: None,
                        operation: Operation::Delete,
                    },
                },
            ];
            let data: Chunk<LogRecord> = Chunk::new(data.into());
            segment_writer.materialize(&data).await;
            let flusher = segment_writer
                .commit()
                .expect("Commit for segment writer failed");
            record_segment.file_path = flusher.flush().await.expect("Flush segment writer failed");
        }
        let data = vec![
            LogRecord {
                log_offset: 4,
                record: OperationRecord {
                    id: "embedding_id_1".to_string(),
                    embedding: Some(vec![1.0, 2.0, 3.0]),
                    encoding: None,
                    metadata: None,
                    document: None,
                    operation: Operation::Add,
                },
            },
            LogRecord {
                log_offset: 5,
                record: OperationRecord {
                    id: "embedding_id_4".to_string(),
                    embedding: Some(vec![4.0, 5.0, 6.0]),
                    encoding: None,
                    metadata: None,
                    document: None,
                    operation: Operation::Add,
                },
            },
            LogRecord {
                log_offset: 6,
                record: OperationRecord {
                    id: "embedding_id_2".to_string(),
                    embedding: None,
                    encoding: None,
                    metadata: None,
                    document: None,
                    operation: Operation::Update,
                },
            },
        ];
        let data: Chunk<LogRecord> = Chunk::new(data.into());
        let input = CountRecordsInput {
            record_segment_definition: record_segment,
            blockfile_provider: in_memory_provider,
            log_records: data,
        };
        let operator = CountRecordsOperator {};
        let count = operator
            .run(&input)
            .await
            .expect("Count operator run failed");
        assert_eq!(3, count.count);
    }
}
