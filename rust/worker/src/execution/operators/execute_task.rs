use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::ChromaError;
use chroma_log::Log;
use chroma_segment::types::HydratedMaterializedLogRecord;
use chroma_segment::{
    blockfile_record::{
        RecordSegmentReader, RecordSegmentReaderOptions, RecordSegmentReaderShard,
        RecordSegmentReaderShardCreationError,
    },
    bloom_filter::BloomFilterManager,
};
use chroma_system::{Operator, OperatorType};
use chroma_types::{
    AttachedFunction, Chunk, CollectionUuid, LogRecord, MaterializedLogOperation, Operation,
    OperationRecord, Segment, SegmentShard, SegmentShardError, UpdateMetadataValue,
    FUNCTION_COUNT_TO_FILE_ASYNC_ID, FUNCTION_DUMMY_ASYNC_ID, FUNCTION_HTTP_GENERATE_ID,
    FUNCTION_RECORD_COUNTER_ID, FUNCTION_REVISION_HISTORY_ID, FUNCTION_STATISTICS_ID,
};
use std::sync::Arc;
use thiserror::Error;

use crate::execution::functions::{
    CountToFileAsyncExecutor, CounterFunctionFactory, HttpGenerateExecutor,
    RevisionHistoryExecutor,
    StatisticsFunctionExecutor,
};
use crate::execution::operators::materialize_logs::MaterializeLogOutput;

// Constants for CountAttachedFunction
const COUNT_FUNCTION_OUTPUT_ID: &str = "function_output";
const COUNT_METADATA_KEY: &str = "total_count";

/// Trait for attached function executors that process input records and produce output records.
/// Implementors can read from the output collection to maintain state across executions.
#[async_trait]
pub trait AttachedFunctionExecutor: Send + Sync + std::fmt::Debug {
    /// Execute the attached function logic on input records.
    ///
    /// # Arguments
    /// * `input_records` - The hydrated materialized log records to process
    /// * `output_reader` - Optional reader for the output collection's compacted data
    ///
    /// # Returns
    /// The output records to be written to the output collection
    async fn execute(
        &self,
        input_records: Vec<Chunk<HydratedMaterializedLogRecord<'_, '_>>>,
        output_reader: Option<&RecordSegmentReaderShard<'_>>,
    ) -> Result<Chunk<LogRecord>, Box<dyn ChromaError>>;
}

/// A simple counting attached function that maintains a running total of records processed.
/// Stores the count in a metadata field called "total_count".
#[derive(Debug)]
pub struct CountAttachedFunction;

impl CountAttachedFunction {
    /// Reads the existing count from the output reader.
    /// Returns 0 if no existing count is found.
    async fn get_existing_count(output_reader: Option<&RecordSegmentReaderShard<'_>>) -> i64 {
        let Some(reader) = output_reader else {
            return 0;
        };

        // Try to get the existing record with the function output ID
        let offset_id = match reader
            .get_offset_id_for_user_id(
                COUNT_FUNCTION_OUTPUT_ID,
                &RecordSegmentReaderOptions::default(),
            )
            .await
        {
            Ok(Some(offset_id)) => offset_id,
            _ => return 0,
        };

        // Get the data record for this offset id
        let data_record = match reader.get_data_for_offset_id(offset_id).await {
            Ok(Some(data_record)) => data_record,
            _ => return 0,
        };

        // Extract total_count from metadata
        if let Some(metadata) = &data_record.metadata {
            if let Some(chroma_types::MetadataValue::Int(count)) = metadata.get(COUNT_METADATA_KEY)
            {
                return *count;
            }
        }

        0
    }
}

#[async_trait]
impl AttachedFunctionExecutor for CountAttachedFunction {
    async fn execute(
        &self,
        input_records: Vec<Chunk<HydratedMaterializedLogRecord<'_, '_>>>,
        output_reader: Option<&RecordSegmentReaderShard<'_>>,
    ) -> Result<Chunk<LogRecord>, Box<dyn ChromaError>> {
        let records_count = input_records.iter().map(Chunk::len).sum::<usize>() as i64;

        // NOTE(tanujnay112): Can get all these in one pass but this function is just for
        // testing.
        let delete_count = input_records
            .iter()
            .flat_map(|batch| batch.iter())
            .filter(|(record, _)| {
                record.get_operation() == MaterializedLogOperation::DeleteExisting
            })
            .count() as i64;

        let insert_count = input_records
            .iter()
            .flat_map(|batch| batch.iter())
            .filter(|(record, _)| record.get_operation() == MaterializedLogOperation::AddNew)
            .count() as i64;

        // Read existing count from output_reader if available
        let existing_count = Self::get_existing_count(output_reader).await;
        let new_total_count = existing_count + insert_count - delete_count;
        println!(
            "Existing count: {}, Insert count: {}, Delete count: {}, New total count: {}",
            existing_count, insert_count, delete_count, new_total_count
        );

        // Create output record with updated count
        let mut metadata = std::collections::HashMap::new();
        metadata.insert(
            COUNT_METADATA_KEY.to_string(),
            UpdateMetadataValue::Int(new_total_count),
        );

        let output_record = LogRecord {
            log_offset: 0,
            record: OperationRecord {
                id: COUNT_FUNCTION_OUTPUT_ID.to_string(),
                embedding: Some(vec![0.0]),
                encoding: None,
                metadata: Some(metadata),
                document: Some(format!(
                    "Last processed {} records (total: {})",
                    records_count, new_total_count
                )),
                operation: Operation::Upsert,
            },
        };

        Ok(Chunk::new(Arc::from(vec![output_record])))
    }
}

/// A dummy attached function for testing that logs a message and returns empty output.
#[derive(Debug)]
pub struct DummyAttachedFunction;

#[async_trait]
impl AttachedFunctionExecutor for DummyAttachedFunction {
    async fn execute(
        &self,
        input_records: Vec<Chunk<HydratedMaterializedLogRecord<'_, '_>>>,
        _output_reader: Option<&RecordSegmentReaderShard<'_>>,
    ) -> Result<Chunk<LogRecord>, Box<dyn ChromaError>> {
        tracing::info!(
            "DummyAttachedFunction executing with {} input records",
            input_records.iter().map(Chunk::len).sum::<usize>()
        );

        // Return empty output records
        Ok(Chunk::new(vec![].into()))
    }
}

/// The ExecuteAttachedFunction operator executes attached function logic based on fetched logs.
/// Uses an AttachedFunctionExecutor trait to allow different attached function implementations.
#[derive(Debug)]
pub struct ExecuteAttachedFunctionOperator {
    pub log_client: Log,
    pub attached_function_executor: Arc<dyn AttachedFunctionExecutor>,
}

impl ExecuteAttachedFunctionOperator {
    /// Create a new ExecuteAttachedFunctionOperator from an AttachedFunction.
    /// The executor is selected based on the function_id in the attached function.
    pub(crate) fn from_attached_function(
        attached_function: &AttachedFunction,
        log_client: Log,
        storage: Option<chroma_storage::Storage>,
    ) -> Result<Self, ExecuteAttachedFunctionError> {
        let function_id = attached_function.function_id;
        let executor: Arc<dyn AttachedFunctionExecutor> = match function_id {
            FUNCTION_RECORD_COUNTER_ID => Arc::new(CountAttachedFunction),
            FUNCTION_STATISTICS_ID => {
                Arc::new(StatisticsFunctionExecutor(Box::new(CounterFunctionFactory)))
            }
            FUNCTION_DUMMY_ASYNC_ID => Arc::new(DummyAttachedFunction),
            FUNCTION_COUNT_TO_FILE_ASYNC_ID => {
                let executor =
                    CountToFileAsyncExecutor::from_attached_function(attached_function, storage)
                        .map_err(|e| {
                            ExecuteAttachedFunctionError::ExecutorConfig(format!(
                                "CountToFileAsyncExecutor: {e}"
                            ))
                        })?;
                Arc::new(executor)
            }
            FUNCTION_HTTP_GENERATE_ID => {
                let executor = HttpGenerateExecutor::from_attached_function(attached_function)
                    .map_err(|e| {
                        ExecuteAttachedFunctionError::ExecutorConfig(format!(
                            "HttpGenerateExecutor: {e}"
                        ))
                    })?;
                Arc::new(executor)
            }
            FUNCTION_REVISION_HISTORY_ID => {
                let executor = RevisionHistoryExecutor::from_attached_function(attached_function)
                    .map_err(|e| {
                    ExecuteAttachedFunctionError::ExecutorConfig(format!(
                        "RevisionHistoryExecutor: {e}"
                    ))
                })?;
                Arc::new(executor)
            }
            _ => {
                tracing::error!("Unknown function_id UUID: {}", function_id);
                return Err(ExecuteAttachedFunctionError::InvalidUuid(format!(
                    "Unknown function_id UUID: {}",
                    function_id
                )));
            }
        };

        Ok(ExecuteAttachedFunctionOperator {
            log_client,
            attached_function_executor: executor,
        })
    }
}

/// Input for the ExecuteAttachedFunction operator
#[derive(Debug)]
pub struct ExecuteAttachedFunctionBatchInput {
    /// The materialized logs for one input collection.
    pub materialized_logs: Vec<MaterializeLogOutput>,
    /// The input collection's record segment to hydrate against.
    pub input_record_segment: Option<RecordSegmentReader<'static>>,
}

#[derive(Debug)]
pub struct ExecuteAttachedFunctionInput {
    /// The materialized log outputs to process, grouped by input collection.
    pub input_batches: Vec<ExecuteAttachedFunctionBatchInput>,
    /// The tenant ID
    pub tenant_id: String,
    /// The output collection ID where results are written
    pub output_collection_id: CollectionUuid,
    /// The output collection's record segment to read existing data
    pub output_record_segment: Segment,
    /// Blockfile provider for reading segments
    pub blockfile_provider: BlockfileProvider,

    pub is_rebuild: bool,
    pub is_for_backfill: bool,
    pub bloom_filter_manager: Option<BloomFilterManager>,
}

/// Output from the ExecuteAttachedFunction operator
#[derive(Debug)]
pub struct ExecuteAttachedFunctionOutput {
    /// The number of records processed in this execution
    pub records_processed: u64,
    /// The output log records to be partitioned and compacted
    pub output_records: Chunk<LogRecord>,
}

#[derive(Debug, Error)]
pub enum ExecuteAttachedFunctionError {
    #[error("Failed to read from segment: {0}")]
    SegmentRead(#[from] Box<dyn ChromaError>),
    #[error("Failed to create record segment reader: {0}")]
    RecordReader(#[from] RecordSegmentReaderShardCreationError),
    #[error("Invalid collection UUID: {0}")]
    InvalidUuid(String),
    #[error("Executor configuration error: {0}")]
    ExecutorConfig(String),
    #[error("Log offset arithmetic overflow: base_offset={0}, record_index={1}")]
    LogOffsetOverflow(i64, usize),
    #[error("Log offset overflow: base_offset={0}, record_index={1}")]
    LogOffsetOverflowUnsignedToSigned(u64, usize),
    #[error(transparent)]
    SegmentShard(#[from] SegmentShardError),
}

impl ChromaError for ExecuteAttachedFunctionError {
    fn code(&self) -> chroma_error::ErrorCodes {
        match self {
            ExecuteAttachedFunctionError::SegmentRead(e) => e.code(),
            ExecuteAttachedFunctionError::RecordReader(e) => e.code(),
            ExecuteAttachedFunctionError::InvalidUuid(_) => {
                chroma_error::ErrorCodes::InvalidArgument
            }
            ExecuteAttachedFunctionError::ExecutorConfig(_) => {
                chroma_error::ErrorCodes::InvalidArgument
            }
            ExecuteAttachedFunctionError::LogOffsetOverflow(_, _) => {
                chroma_error::ErrorCodes::Internal
            }
            ExecuteAttachedFunctionError::LogOffsetOverflowUnsignedToSigned(_, _) => {
                chroma_error::ErrorCodes::Internal
            }
            ExecuteAttachedFunctionError::SegmentShard(e) => e.code(),
        }
    }
}

#[async_trait]
impl Operator<ExecuteAttachedFunctionInput, ExecuteAttachedFunctionOutput>
    for ExecuteAttachedFunctionOperator
{
    type Error = ExecuteAttachedFunctionError;

    fn get_type(&self) -> OperatorType {
        OperatorType::IO
    }

    async fn run(
        &self,
        input: &ExecuteAttachedFunctionInput,
    ) -> Result<ExecuteAttachedFunctionOutput, ExecuteAttachedFunctionError> {
        tracing::info!(
            "[ExecuteAttachedFunction]: Processing {} input collections for output collection {}",
            input.input_batches.len(),
            input.output_collection_id
        );

        // Create record segment reader from the output collection's record segment
        let output_record_segment_reader = if input.is_rebuild || input.is_for_backfill {
            // For rebuild and backfill, we don't read any existing data in output collection
            None
        } else {
            let record_segment_shard = SegmentShard::try_from((&input.output_record_segment, 0))?;
            match Box::pin(RecordSegmentReaderShard::from_segment(
                &record_segment_shard,
                &input.blockfile_provider,
                input.bloom_filter_manager.clone(),
            ))
            .await
            {
                Ok(reader) => Some(reader),
                Err(e)
                    if matches!(
                        *e,
                        RecordSegmentReaderShardCreationError::UninitializedSegment
                    ) =>
                {
                    // Output collection has no data yet - this is the first run
                    tracing::info!("[ExecuteAttachedFunction]: Output segment uninitialized - first attached function run");
                    None
                }
                Err(e) => return Err((*e).into()),
            }
        };

        // Process all materialized logs and hydrate the records
        let mut all_hydrated_records = Vec::new();
        let mut total_records_processed = 0u64;

        for batch in &input.input_batches {
            let mut hydrated_records = Vec::new();

            // For backfill, all existing compacted data from the input collection should be
            // in our input materialized logs. So we don't need to read any existing data from
            // the input collection segments.
            let input_record_segment = if input.is_for_backfill {
                None
            } else {
                batch.input_record_segment.as_ref()
            };

            for materialized_log in &batch.materialized_logs {
                for (shard_idx, shard_result) in materialized_log.result.shards.iter().enumerate() {
                    let shard_reader = input_record_segment
                        .and_then(|reader| reader.get_shards().get(shard_idx))
                        .and_then(|shard_opt| shard_opt.as_ref());

                    for borrowed_record in shard_result.iter() {
                        let hydrated_record = borrowed_record
                            .hydrate(shard_reader)
                            .await
                            .map_err(|e| ExecuteAttachedFunctionError::SegmentRead(Box::new(e)))?;

                        hydrated_records.push(hydrated_record);
                    }
                }

                total_records_processed += materialized_log.result.len() as u64;
            }

            all_hydrated_records.push(Chunk::new(std::sync::Arc::from(hydrated_records)));
        }

        // Execute the attached function using the provided executor
        let output_records = self
            .attached_function_executor
            .execute(all_hydrated_records, output_record_segment_reader.as_ref())
            .await
            .map_err(ExecuteAttachedFunctionError::SegmentRead)?;

        let output_records_with_offsets: Vec<LogRecord> = output_records
            .iter()
            .map(|(log_record, _)| {
                Ok(LogRecord {
                    log_offset: -1, // Nobody should be using these anyway.
                    record: log_record.record.clone(),
                })
            })
            .collect::<Result<Vec<_>, ExecuteAttachedFunctionError>>()?;

        tracing::info!(
            "[ExecuteAttachedFunction]: Attached function executed successfully, produced {} output records",
            output_records_with_offsets.len()
        );

        // Return the output records to be partitioned
        Ok(ExecuteAttachedFunctionOutput {
            records_processed: total_records_processed,
            output_records: Chunk::new(std::sync::Arc::from(output_records_with_offsets)),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::operators::materialize_logs::{
        MaterializeLogInput, MaterializeLogOperator,
    };
    use chroma_log::in_memory_log::InMemoryLog;
    use chroma_segment::test::TestDistributedSegment;
    use chroma_system::Operator;
    use std::collections::HashMap;

    #[derive(Debug)]
    struct EchoHydratedDocumentsExecutor;

    #[async_trait]
    impl AttachedFunctionExecutor for EchoHydratedDocumentsExecutor {
        async fn execute(
            &self,
            input_records: Vec<Chunk<HydratedMaterializedLogRecord<'_, '_>>>,
            _output_reader: Option<&RecordSegmentReaderShard<'_>>,
        ) -> Result<Chunk<LogRecord>, Box<dyn ChromaError>> {
            let mut output_records = Vec::new();
            for (batch_idx, batch) in input_records.iter().enumerate() {
                for (record, _) in batch.iter() {
                    output_records.push(LogRecord {
                        log_offset: -1,
                        record: OperationRecord {
                            id: format!("batch-{batch_idx}-{}", record.get_user_id()),
                            embedding: Some(vec![0.0]),
                            encoding: None,
                            metadata: None,
                            document: record.merged_document_ref().map(str::to_string),
                            operation: Operation::Upsert,
                        },
                    });
                }
            }
            Ok(Chunk::new(Arc::from(output_records)))
        }
    }

    fn existing_record(id: &str, document: &str, dimension: usize) -> LogRecord {
        LogRecord {
            log_offset: 0,
            record: OperationRecord {
                id: id.to_string(),
                embedding: Some(vec![0.0; dimension]),
                encoding: None,
                metadata: Some(HashMap::new()),
                document: Some(document.to_string()),
                operation: Operation::Add,
            },
        }
    }

    fn delete_record(id: &str) -> LogRecord {
        LogRecord {
            log_offset: 1,
            record: OperationRecord {
                id: id.to_string(),
                embedding: None,
                encoding: None,
                metadata: None,
                document: None,
                operation: Operation::Delete,
            },
        }
    }

    async fn reader_and_delete_materialized_output(
        document: &str,
    ) -> (
        TestDistributedSegment,
        RecordSegmentReader<'static>,
        MaterializeLogOutput,
    ) {
        let mut segment = TestDistributedSegment::new().await;
        let dimension = segment
            .collection
            .dimension
            .expect("test collection has dimension") as usize;
        Box::pin(segment.compact_log(
            Chunk::new(Arc::from(vec![existing_record(
                "shared-id",
                document,
                dimension,
            )])),
            1,
        ))
        .await;

        let reader = Box::pin(RecordSegmentReader::from_segment(
            &segment.record_segment,
            &segment.blockfile_provider,
            None,
        ))
        .await
        .expect("record reader should be created");

        let materialized = MaterializeLogOperator::new()
            .run(&MaterializeLogInput::new(
                Chunk::new(Arc::from(vec![delete_record("shared-id")])),
                Some(reader.clone()),
                vec![],
                RecordSegmentReaderOptions::default(),
            ))
            .await
            .expect("delete should materialize against the input reader");

        (segment, reader, materialized)
    }

    #[tokio::test]
    async fn execute_uses_each_input_batch_record_reader_for_hydration() {
        let (_input_segment_a, reader_a, materialized_a) =
            reader_and_delete_materialized_output("document-from-input-a").await;
        let (_input_segment_b, reader_b, materialized_b) =
            reader_and_delete_materialized_output("document-from-input-b").await;
        let output_segment = TestDistributedSegment::new().await;

        let operator = ExecuteAttachedFunctionOperator {
            log_client: Log::InMemory(InMemoryLog::new()),
            attached_function_executor: Arc::new(EchoHydratedDocumentsExecutor),
        };

        let output = operator
            .run(&ExecuteAttachedFunctionInput {
                input_batches: vec![
                    ExecuteAttachedFunctionBatchInput {
                        materialized_logs: vec![materialized_a],
                        input_record_segment: Some(reader_a),
                    },
                    ExecuteAttachedFunctionBatchInput {
                        materialized_logs: vec![materialized_b],
                        input_record_segment: Some(reader_b),
                    },
                ],
                tenant_id: output_segment.collection.tenant.clone(),
                output_collection_id: output_segment.collection.collection_id,
                output_record_segment: output_segment.record_segment.clone(),
                blockfile_provider: output_segment.blockfile_provider.clone(),
                is_rebuild: false,
                is_for_backfill: false,
                bloom_filter_manager: None,
            })
            .await
            .expect("execution should succeed");

        let documents = output
            .output_records
            .iter()
            .map(|(record, _)| {
                (
                    record.record.id.clone(),
                    record
                        .record
                        .document
                        .clone()
                        .expect("executor should emit hydrated document"),
                )
            })
            .collect::<HashMap<_, _>>();

        assert_eq!(
            documents.get("batch-0-shared-id").map(String::as_str),
            Some("document-from-input-a")
        );
        assert_eq!(
            documents.get("batch-1-shared-id").map(String::as_str),
            Some("document-from-input-b")
        );
    }
}
