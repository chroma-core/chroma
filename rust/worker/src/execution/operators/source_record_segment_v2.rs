use crate::execution::operators::materialize_logs::MaterializeLogOutput;
use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_segment::blockfile_record::RecordSegmentReaderShard;
use chroma_segment::types::{
    materialize_logs_for_rebuild, MaterializeLogsResult, PartitionedMaterializeLogsResult,
};
use chroma_system::Operator;
use chroma_types::{Chunk, LogRecord, Operation, OperationRecord};
use futures::StreamExt;
use thiserror::Error;

/// The `SourceRecordSegmentV2Operator` streams through the record segment and produces
/// partitioned materialized log records for rebuild operations.
/// This combines the functionality of SourceRecordSegment, Partition, and MaterializeLog operators.
///
/// # Parameters
/// - `max_partition_size`: Maximum size of each partition
///
/// # Inputs
/// - `record_reader`: The record segment reader, if the collection is initialized
///
/// # Outputs
/// - Vec of MaterializeLogsResult (one per partition)
///
/// TODO(tanujnay112): This will replace SourceRecordSegmentOperator for full rebuilds once
/// this code bakes.
#[derive(Clone, Debug)]
pub struct SourceRecordSegmentV2Operator {
    max_partition_size: usize,
    shard_count: usize,
    shard_index: u32,
}

impl SourceRecordSegmentV2Operator {
    pub fn new(max_partition_size: usize, shard_count: usize, shard_index: u32) -> Self {
        Self {
            max_partition_size,
            shard_count,
            shard_index,
        }
    }
}

#[derive(Clone, Debug)]
pub struct SourceRecordSegmentV2Input {
    pub record_segment_reader: Option<RecordSegmentReaderShard<'static>>,
}

#[derive(Debug, Clone)]
pub struct SourceRecordSegmentV2Output {
    pub partitions: Vec<MaterializeLogOutput>,
    pub total_records: usize,
}

#[derive(Debug, Error)]
pub enum SourceRecordSegmentV2Error {
    #[error("Error reading record segment: {0}")]
    RecordSegment(#[from] Box<dyn ChromaError>),
    #[error("Error materializing logs: {0}")]
    MaterializeLogs(#[from] chroma_segment::types::LogMaterializerError),
}

impl ChromaError for SourceRecordSegmentV2Error {
    fn code(&self) -> ErrorCodes {
        match self {
            SourceRecordSegmentV2Error::RecordSegment(e) => e.code(),
            SourceRecordSegmentV2Error::MaterializeLogs(e) => e.code(),
        }
    }
}

#[async_trait]
impl Operator<SourceRecordSegmentV2Input, SourceRecordSegmentV2Output>
    for SourceRecordSegmentV2Operator
{
    type Error = SourceRecordSegmentV2Error;

    async fn run(
        &self,
        input: &SourceRecordSegmentV2Input,
    ) -> Result<SourceRecordSegmentV2Output, SourceRecordSegmentV2Error> {
        tracing::trace!("[{}]: {:?}", self.get_name(), input);

        let empty_shard = || MaterializeLogsResult {
            logs: Chunk::new(Vec::new().into()),
            materialized: Chunk::new(Vec::new().into()),
            has_backfill: false,
        };
        let build_partition = |materialized: MaterializeLogsResult| {
            let shards = (0..self.shard_count)
                .map(|i| {
                    if i == self.shard_index as usize {
                        materialized.clone()
                    } else {
                        empty_shard()
                    }
                })
                .collect();
            MaterializeLogOutput {
                result: PartitionedMaterializeLogsResult { shards },
                collection_logical_size_delta: 0,
            }
        };

        let reader = match input.record_segment_reader.as_ref() {
            Some(reader) => reader,
            None => {
                // Even with no reader, we need to return empty shards for all positions
                let shards = (0..self.shard_count).map(|_| empty_shard()).collect();
                let output = MaterializeLogOutput {
                    result: PartitionedMaterializeLogsResult { shards },
                    collection_logical_size_delta: 0,
                };
                return Ok(SourceRecordSegmentV2Output {
                    partitions: vec![output],
                    total_records: 0,
                });
            }
        };

        let mut partitions = Vec::new();
        let mut current_partition_logs = Vec::new();
        let mut current_partition_offsets = Vec::new();
        let mut total_records = 0;
        let mut log_offset = 1;

        let mut stream = reader.get_data_stream(..).await;

        while let Some(result) = stream.next().await {
            let (offset_id, record) = result?;
            let log_record = LogRecord {
                log_offset,
                record: OperationRecord {
                    id: record.id.to_string(),
                    embedding: Some(record.embedding.to_vec()),
                    encoding: Some(chroma_types::ScalarEncoding::FLOAT32),
                    metadata: record
                        .metadata
                        .map(|meta| meta.into_iter().map(|(k, v)| (k, v.into())).collect()),
                    document: record.document.map(ToString::to_string),
                    operation: Operation::Add,
                },
            };
            // Store offset ID in the same order as logs
            current_partition_offsets.push(offset_id);
            current_partition_logs.push(log_record);
            total_records += 1;
            log_offset += 1;

            if current_partition_logs.len() >= self.max_partition_size {
                let logs_chunk = Chunk::new(current_partition_logs.into());
                let materialized =
                    materialize_logs_for_rebuild(logs_chunk, current_partition_offsets).await?;
                partitions.push(build_partition(materialized));
                current_partition_logs = Vec::new();
                current_partition_offsets = Vec::new();
            }
        }

        if !current_partition_logs.is_empty() {
            let logs_chunk = Chunk::new(current_partition_logs.into());
            let materialized =
                materialize_logs_for_rebuild(logs_chunk, current_partition_offsets).await?;
            partitions.push(build_partition(materialized));
        }

        Ok(SourceRecordSegmentV2Output {
            partitions,
            total_records,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chroma_log::test::{upsert_generator, LoadFromGenerator};
    use chroma_segment::test::TestDistributedSegment;
    use chroma_types::{MaterializedLogOperation, SegmentShard};

    async fn setup_test_reader(num_records: usize) -> RecordSegmentReaderShard<'static> {
        let mut test_segment = TestDistributedSegment::new().await;
        test_segment
            .populate_with_generator(num_records, upsert_generator)
            .await;
        let record_segment_shard =
            SegmentShard::try_from((&test_segment.record_segment, 0)).expect("valid shard index");
        Box::pin(RecordSegmentReaderShard::from_segment(
            &record_segment_shard,
            &test_segment.blockfile_provider,
            None,
        ))
        .await
        .expect("Record segment reader should be initialized")
    }

    #[tokio::test]
    async fn test_source_v2_basic() {
        let reader = setup_test_reader(100).await;
        let input = SourceRecordSegmentV2Input {
            record_segment_reader: Some(reader),
        };

        let operator = SourceRecordSegmentV2Operator::new(30, 1, 0);
        let output = operator.run(&input).await.expect("Operator should succeed");

        assert_eq!(output.total_records, 100);
        assert_eq!(output.partitions.len(), 4); // 30, 30, 30, 10

        // Verify that each partition has 1 shard
        for partition in &output.partitions {
            assert_eq!(partition.result.shards.len(), 1);
        }

        // Verify operations are correct
        for partition in &output.partitions {
            // partition.result is PartitionedMaterializeLogsResult, iterate over shards
            for shard in partition.result.iter() {
                // Now iterate over records in each shard
                for record in shard {
                    // For rebuild, we expect AddNew operation
                    assert_eq!(record.get_operation(), MaterializedLogOperation::AddNew);
                }
            }
        }
    }

    #[tokio::test]
    async fn test_source_v2_empty() {
        let input = SourceRecordSegmentV2Input {
            record_segment_reader: None,
        };

        let operator = SourceRecordSegmentV2Operator::new(30, 1, 0);
        let output = operator.run(&input).await.expect("Operator should succeed");

        assert_eq!(output.total_records, 0);
        assert_eq!(output.partitions.len(), 1);
    }

    #[tokio::test]
    async fn test_source_v2_preserves_offset_ids() {
        let reader = setup_test_reader(10).await;
        let input = SourceRecordSegmentV2Input {
            record_segment_reader: Some(reader),
        };

        let operator = SourceRecordSegmentV2Operator::new(5, 1, 0);
        let output = operator.run(&input).await.expect("Operator should succeed");

        assert_eq!(output.partitions.len(), 2);

        // Verify offset IDs are preserved (0-based from test data generation)
        let mut expected_offset_id = 1u32;
        for partition in &output.partitions {
            // partition.result is PartitionedMaterializeLogsResult, iterate over shards
            for shard in partition.result.iter() {
                // Now iterate over records in each shard
                for record in shard {
                    assert_eq!(record.get_offset_id(), expected_offset_id);
                    expected_offset_id += 1;
                }
            }
        }
    }
}
