use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::ChromaError;
use chroma_system::{Operator, OperatorType};
use chroma_types::{Segment, SegmentType};
use futures::{stream::FuturesUnordered, StreamExt};
use thiserror::Error;
use tonic::async_trait;
use uuid::Uuid;

#[derive(Debug, Default)]
pub struct PrefetchSegmentOperator {}

impl PrefetchSegmentOperator {
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Debug)]
pub struct PrefetchSegmentInput {
    segment: Segment,
    blockfile_provider: BlockfileProvider,
}

impl PrefetchSegmentInput {
    pub fn new(segment: Segment, blockfile_provider: BlockfileProvider) -> Self {
        Self {
            segment,
            blockfile_provider,
        }
    }
}

#[derive(Debug)]
pub struct PrefetchSegmentOutput {
    #[allow(dead_code)]
    num_blocks_fetched: usize,
}

#[derive(Debug, Error)]
pub enum PrefetchSegmentError {
    #[error("Could not parse blockfile ID string: {0}")]
    ParseBlockfileId(#[from] uuid::Error),
    #[error("Error prefetching: {0}")]
    Prefetch(#[from] Box<dyn ChromaError>),
    #[error("Unsupported segment type: {:?}", .0)]
    UnsupportedSegmentType(SegmentType),
}

impl ChromaError for PrefetchSegmentError {
    fn code(&self) -> chroma_error::ErrorCodes {
        match self {
            PrefetchSegmentError::ParseBlockfileId(_) => chroma_error::ErrorCodes::InvalidArgument,
            PrefetchSegmentError::Prefetch(err) => err.code(),
            PrefetchSegmentError::UnsupportedSegmentType(_) => {
                chroma_error::ErrorCodes::InvalidArgument
            }
        }
    }
}

#[async_trait]
impl Operator<PrefetchSegmentInput, PrefetchSegmentOutput> for PrefetchSegmentOperator {
    type Error = PrefetchSegmentError;

    async fn run(
        &self,
        input: &PrefetchSegmentInput,
    ) -> Result<PrefetchSegmentOutput, PrefetchSegmentError> {
        if !input.segment.prefetch_supported() {
            return Err(PrefetchSegmentError::UnsupportedSegmentType(
                input.segment.r#type,
            ));
        }

        let mut futures = input
            .segment
            .filepaths_to_prefetch()
            .into_iter()
            .map(|blockfile_id| async move {
                let blockfile_id = Uuid::parse_str(&blockfile_id)?;
                let count = input.blockfile_provider.prefetch(&blockfile_id).await?;
                Ok::<_, PrefetchSegmentError>(count)
            })
            .collect::<FuturesUnordered<_>>();

        let mut total_blocks_fetched = 0;
        while let Some(result) = futures.next().await {
            total_blocks_fetched += result?;
        }

        Ok(PrefetchSegmentOutput {
            num_blocks_fetched: total_blocks_fetched,
        })
    }

    fn get_type(&self) -> OperatorType {
        OperatorType::IO
    }

    // We don't care if the sender is dropped since this is a prefetch
    fn errors_when_sender_dropped(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chroma_cache::new_cache_for_test;
    use chroma_segment::blockfile_record::{RecordSegmentReader, RecordSegmentWriter};
    use chroma_segment::types::materialize_logs;
    use chroma_storage::test_storage;
    use chroma_types::{Chunk, CollectionUuid, LogRecord, Operation, OperationRecord, SegmentUuid};
    use std::collections::HashMap;
    use std::str::FromStr;

    #[tokio::test]
    async fn test_loads_blocks_into_cache() {
        let cache = new_cache_for_test();
        let blockfile_provider =
            BlockfileProvider::new_arrow(test_storage(), 1000, cache, new_cache_for_test());

        let mut record_segment = chroma_types::Segment {
            id: SegmentUuid::from_str("00000000-0000-0000-0000-000000000000").expect("parse error"),
            r#type: chroma_types::SegmentType::BlockfileRecord,
            scope: chroma_types::SegmentScope::RECORD,
            collection: CollectionUuid::from_str("00000000-0000-0000-0000-000000000000")
                .expect("parse error"),
            metadata: None,
            file_path: HashMap::new(),
        };
        {
            let segment_writer =
                RecordSegmentWriter::from_segment(&record_segment, &blockfile_provider)
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
            let record_segment_reader: Option<RecordSegmentReader> = None;

            let mat_records = materialize_logs(&record_segment_reader, data, None)
                .await
                .expect("Log materialization failed");
            segment_writer
                .apply_materialized_log_chunk(&record_segment_reader, &mat_records)
                .await
                .expect("Apply materialized log failed");
            let flusher = segment_writer
                .commit()
                .await
                .expect("Commit for segment writer failed");
            record_segment.file_path = flusher.flush().await.expect("Flush segment writer failed");
        }

        // Since our cache is write-through, this should have no effect
        let input = PrefetchSegmentInput::new(record_segment.clone(), blockfile_provider.clone());
        let operator = PrefetchSegmentOperator::new();

        let result = operator
            .run(&input)
            .await
            .expect("Prefetch operator run failed");

        assert_eq!(result.num_blocks_fetched, 0);

        // Clear the cache...
        blockfile_provider.clear().await.unwrap();

        // ...and now blocks should be fetched
        let input = PrefetchSegmentInput::new(record_segment, blockfile_provider);
        let operator = PrefetchSegmentOperator::new();

        let result = operator
            .run(&input)
            .await
            .expect("Prefetch operator run failed");

        assert!(result.num_blocks_fetched > 0);
    }
}
