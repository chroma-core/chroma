use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::ChromaError;
use chroma_log::Log;
use chroma_segment::blockfile_record::{RecordSegmentReader, RecordSegmentReaderCreationError};
use chroma_segment::types::HydratedMaterializedLogRecord;
use chroma_system::{Operator, OperatorType};
use chroma_types::{
    Chunk, CollectionUuid, LogRecord, MaterializedLogOperation, Operation, OperationRecord,
    Segment, UpdateMetadataValue, FUNCTION_RECORD_COUNTER_ID, FUNCTION_STATISTICS_ID,
};
use std::sync::Arc;
use thiserror::Error;
use uuid::Uuid;

use crate::execution::functions::{CounterFunctionFactory, StatisticsFunctionExecutor};
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
        input_records: Chunk<HydratedMaterializedLogRecord<'_, '_>>,
        output_reader: Option<&RecordSegmentReader<'_>>,
    ) -> Result<Chunk<LogRecord>, Box<dyn ChromaError>>;
}

/// A simple counting attached function that maintains a running total of records processed.
/// Stores the count in a metadata field called "total_count".
#[derive(Debug)]
pub struct CountAttachedFunction;

impl CountAttachedFunction {
    /// Reads the existing count from the output reader.
    /// Returns 0 if no existing count is found.
    async fn get_existing_count(output_reader: Option<&RecordSegmentReader<'_>>) -> i64 {
        let Some(reader) = output_reader else {
            return 0;
        };

        // Try to get the existing record with the function output ID
        let offset_id = match reader
            .get_offset_id_for_user_id(COUNT_FUNCTION_OUTPUT_ID)
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
        input_records: Chunk<HydratedMaterializedLogRecord<'_, '_>>,
        output_reader: Option<&RecordSegmentReader<'_>>,
    ) -> Result<Chunk<LogRecord>, Box<dyn ChromaError>> {
        let records_count = input_records.len() as i64;

        // NOTE(tanujnay112): Can get all these in one pass but this function is just for
        // testing.
        let delete_count = input_records
            .iter()
            .filter(|(record, _)| {
                record.get_operation() == MaterializedLogOperation::DeleteExisting
            })
            .count() as i64;

        let insert_count = input_records
            .iter()
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
        function_id: Uuid,
        log_client: Log,
    ) -> Result<Self, ExecuteAttachedFunctionError> {
        let executor: Arc<dyn AttachedFunctionExecutor> = match function_id {
            // For the record counter, use CountAttachedFunction
            FUNCTION_RECORD_COUNTER_ID => Arc::new(CountAttachedFunction),
            // For statistics, use StatisticsFunctionExecutor with CounterFunctionFactory
            FUNCTION_STATISTICS_ID => {
                Arc::new(StatisticsFunctionExecutor(Box::new(CounterFunctionFactory)))
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
pub struct ExecuteAttachedFunctionInput {
    /// The materialized log outputs to process
    pub materialized_logs: Vec<MaterializeLogOutput>,
    /// The tenant ID
    pub tenant_id: String,
    /// The input collection's record segment to read existing data
    pub input_record_segment: Option<RecordSegmentReader<'static>>,
    /// The output collection ID where results are written
    pub output_collection_id: CollectionUuid,
    /// The current completion offset
    pub completion_offset: u64,
    /// The output collection's record segment to read existing data
    pub output_record_segment: Segment,
    /// Blockfile provider for reading segments
    pub blockfile_provider: BlockfileProvider,

    pub is_rebuild: bool,
    pub is_for_backfill: bool,
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
    RecordReader(#[from] RecordSegmentReaderCreationError),
    #[error("Invalid collection UUID: {0}")]
    InvalidUuid(String),
    #[error("Log offset arithmetic overflow: base_offset={0}, record_index={1}")]
    LogOffsetOverflow(i64, usize),
    #[error("Log offset overflow: base_offset={0}, record_index={1}")]
    LogOffsetOverflowUnsignedToSigned(u64, usize),
}

impl ChromaError for ExecuteAttachedFunctionError {
    fn code(&self) -> chroma_error::ErrorCodes {
        match self {
            ExecuteAttachedFunctionError::SegmentRead(e) => e.code(),
            ExecuteAttachedFunctionError::RecordReader(e) => e.code(),
            ExecuteAttachedFunctionError::InvalidUuid(_) => {
                chroma_error::ErrorCodes::InvalidArgument
            }
            ExecuteAttachedFunctionError::LogOffsetOverflow(_, _) => {
                chroma_error::ErrorCodes::Internal
            }
            ExecuteAttachedFunctionError::LogOffsetOverflowUnsignedToSigned(_, _) => {
                chroma_error::ErrorCodes::Internal
            }
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
            "[ExecuteAttachedFunction]: Processing {} materialized log outputs for output collection {}",
            input.materialized_logs.len(),
            input.output_collection_id
        );

        // Create record segment reader from the output collection's record segment
        let output_record_segment_reader = if input.is_rebuild || input.is_for_backfill {
            // For rebuild and backfill, we don't read any existing data in output collection
            None
        } else {
            match Box::pin(RecordSegmentReader::from_segment(
                &input.output_record_segment,
                &input.blockfile_provider,
            ))
            .await
            {
                Ok(reader) => Some(reader),
                Err(e) if matches!(*e, RecordSegmentReaderCreationError::UninitializedSegment) => {
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

        // For backfill, all existing compacted data from the input collection should be
        // in our input materialized logs. So we don't need to read any existing data from
        // the input collection segments.
        let input_record_segment = if input.is_for_backfill {
            None
        } else {
            input.input_record_segment.as_ref()
        };

        for materialized_log in input.materialized_logs.iter() {
            // Use the iterator to process each materialized record
            for borrowed_record in materialized_log.result.iter() {
                // Hydrate the record using the same pattern as materialize_logs operator
                let hydrated_record = borrowed_record
                    .hydrate(input_record_segment)
                    .await
                    .map_err(|e| ExecuteAttachedFunctionError::SegmentRead(Box::new(e)))?;

                all_hydrated_records.push(hydrated_record);
            }

            total_records_processed += materialized_log.result.len() as u64;
        }

        // Execute the attached function using the provided executor
        let output_records = self
            .attached_function_executor
            .execute(
                Chunk::new(std::sync::Arc::from(all_hydrated_records)),
                output_record_segment_reader.as_ref(),
            )
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
