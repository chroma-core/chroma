use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::ChromaError;
use chroma_log::Log;
use chroma_segment::blockfile_record::{RecordSegmentReader, RecordSegmentReaderCreationError};
use chroma_system::{Operator, OperatorType};
use chroma_types::{
    Chunk, CollectionUuid, LogRecord, Operation, OperationRecord, Segment, UpdateMetadataValue,
    FUNCTION_RECORD_COUNTER_ID, FUNCTION_STATISTICS_ID,
};
use std::sync::Arc;
use thiserror::Error;

use crate::execution::functions::{CounterFunctionFactory, StatisticsFunctionExecutor};

/// Trait for attached function executors that process input records and produce output records.
/// Implementors can read from the output collection to maintain state across executions.
#[async_trait]
pub trait AttachedFunctionExecutor: Send + Sync + std::fmt::Debug {
    /// Execute the attached function logic on input records.
    ///
    /// # Arguments
    /// * `input_records` - The log records to process
    /// * `output_reader` - Optional reader for the output collection's compacted data
    ///
    /// # Returns
    /// The output records to be written to the output collection
    async fn execute(
        &self,
        input_records: Chunk<LogRecord>,
        output_reader: Option<&RecordSegmentReader<'_>>,
    ) -> Result<Chunk<LogRecord>, Box<dyn ChromaError>>;
}

/// A simple counting attached function that maintains a running total of records processed.
/// Stores the count in a metadata field called "total_count".
#[derive(Debug)]
pub struct CountAttachedFunction;

#[async_trait]
impl AttachedFunctionExecutor for CountAttachedFunction {
    async fn execute(
        &self,
        input_records: Chunk<LogRecord>,
        _output_reader: Option<&RecordSegmentReader<'_>>,
    ) -> Result<Chunk<LogRecord>, Box<dyn ChromaError>> {
        let records_count = input_records.len() as i64;

        let new_total_count = records_count;

        // Create output record with updated count
        let mut metadata = std::collections::HashMap::new();
        metadata.insert(
            "total_count".to_string(),
            UpdateMetadataValue::Int(new_total_count),
        );

        let operation_record = OperationRecord {
            id: "attached_function_result".to_string(),
            embedding: Some(vec![0.0]),
            encoding: None,
            metadata: Some(metadata),
            document: None,
            operation: Operation::Upsert,
        };

        let log_record = LogRecord {
            log_offset: 0, // Will be set by caller
            record: operation_record,
        };

        Ok(Chunk::new(Arc::new([log_record])))
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
        attached_function: &chroma_types::AttachedFunction,
        log_client: Log,
    ) -> Result<Self, ExecuteAttachedFunctionError> {
        let executor: Arc<dyn AttachedFunctionExecutor> = match attached_function.function_id {
            // For the record counter, use CountAttachedFunction
            FUNCTION_RECORD_COUNTER_ID => Arc::new(CountAttachedFunction),
            // For statistics, use StatisticsFunctionExecutor with CounterFunctionFactory
            FUNCTION_STATISTICS_ID => {
                Arc::new(StatisticsFunctionExecutor(Box::new(CounterFunctionFactory)))
            }
            _ => {
                tracing::error!(
                    "Unknown function_id UUID: {}",
                    attached_function.function_id
                );
                return Err(ExecuteAttachedFunctionError::InvalidUuid(format!(
                    "Unknown function_id UUID: {}",
                    attached_function.function_id
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
    /// The fetched log records to process
    pub log_records: Chunk<LogRecord>,
    /// The tenant ID
    pub tenant_id: String,
    /// The output collection ID where results are written
    pub output_collection_id: CollectionUuid,
    /// The current completion offset
    pub completion_offset: u64,
    /// The output collection's record segment to read existing data
    pub output_record_segment: Segment,
    /// Blockfile provider for reading segments
    pub blockfile_provider: BlockfileProvider,
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
            "[ExecuteAttachedFunction]: Processing {} records for output collection {}",
            input.log_records.len(),
            input.output_collection_id
        );

        let records_count = input.log_records.len() as u64;

        // Create record segment reader from the output collection's record segment
        let record_segment_reader = match Box::pin(RecordSegmentReader::from_segment(
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
        };

        // Execute the attached function using the provided executor
        let output_records = self
            .attached_function_executor
            .execute(input.log_records.clone(), record_segment_reader.as_ref())
            .await
            .map_err(ExecuteAttachedFunctionError::SegmentRead)?;

        // Update log offsets for output records
        // Convert u64 completion_offset to i64 for LogRecord (which uses i64)
        let base_offset: i64 = input.completion_offset.try_into().map_err(|_| {
            ExecuteAttachedFunctionError::LogOffsetOverflowUnsignedToSigned(
                input.completion_offset,
                0,
            )
        })?;

        let output_records_with_offsets: Vec<LogRecord> = output_records
            .iter()
            .enumerate()
            .map(|(i, (log_record, _))| {
                let i_i64 = i64::try_from(i)
                    .map_err(|_| ExecuteAttachedFunctionError::LogOffsetOverflow(base_offset, i))?;
                let offset = base_offset.checked_add(i_i64).ok_or_else(|| {
                    ExecuteAttachedFunctionError::LogOffsetOverflow(base_offset, i)
                })?;
                Ok(LogRecord {
                    log_offset: offset,
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
            records_processed: records_count,
            output_records: Chunk::new(Arc::from(output_records_with_offsets)),
        })
    }
}
