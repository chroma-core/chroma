use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::ChromaError;
use chroma_log::Log;
use chroma_segment::blockfile_record::{RecordSegmentReader, RecordSegmentReaderCreationError};
use chroma_system::{Operator, OperatorType};
use chroma_types::{Chunk, LogRecord, Operation, OperationRecord, Segment, UpdateMetadataValue};
use std::sync::Arc;
use thiserror::Error;

/// Trait for task executors that process input records and produce output records.
/// Implementors can read from the output collection to maintain state across executions.
#[async_trait]
pub trait TaskExecutor: Send + Sync + std::fmt::Debug {
    /// Execute the task logic on input records.
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

/// A simple counting task that maintains a running total of records processed.
/// Stores the count in a metadata field called "total_count".
#[derive(Debug)]
pub struct CountTask;

#[async_trait]
impl TaskExecutor for CountTask {
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
            id: "task_result".to_string(),
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

/// The ExecuteTask operator executes task logic based on fetched logs.
/// Uses a TaskExecutor trait to allow different task implementations.
#[derive(Debug)]
pub struct ExecuteTaskOperator {
    pub log_client: Log,
    pub task_executor: Arc<dyn TaskExecutor>,
}

/// Input for the ExecuteTask operator
#[derive(Debug)]
pub struct ExecuteTaskInput {
    /// The fetched log records to process
    pub log_records: Chunk<LogRecord>,
    /// The tenant ID
    pub tenant_id: String,
    /// The output collection ID where results are written
    pub output_collection_id: String,
    /// The current completion offset
    pub completion_offset: u64,
    /// The output collection's record segment to read existing data
    pub output_record_segment: Segment,
    /// Blockfile provider for reading segments
    pub blockfile_provider: BlockfileProvider,
}

/// Output from the ExecuteTask operator
#[derive(Debug)]
pub struct ExecuteTaskOutput {
    /// The number of records processed in this execution
    pub records_processed: u64,
    /// The output log records to be partitioned and compacted
    pub output_records: Chunk<LogRecord>,
}

#[derive(Debug, Error)]
pub enum ExecuteTaskError {
    #[error("Failed to read from segment: {0}")]
    SegmentRead(#[from] Box<dyn ChromaError>),
    #[error("Failed to create record segment reader: {0}")]
    RecordReader(#[from] RecordSegmentReaderCreationError),
    #[error("Invalid collection UUID: {0}")]
    InvalidUuid(String),
}

impl ChromaError for ExecuteTaskError {
    fn code(&self) -> chroma_error::ErrorCodes {
        match self {
            ExecuteTaskError::SegmentRead(e) => e.code(),
            ExecuteTaskError::RecordReader(e) => e.code(),
            ExecuteTaskError::InvalidUuid(_) => chroma_error::ErrorCodes::InvalidArgument,
        }
    }
}

#[async_trait]
impl Operator<ExecuteTaskInput, ExecuteTaskOutput> for ExecuteTaskOperator {
    type Error = ExecuteTaskError;

    fn get_type(&self) -> OperatorType {
        OperatorType::IO
    }

    async fn run(&self, input: &ExecuteTaskInput) -> Result<ExecuteTaskOutput, ExecuteTaskError> {
        tracing::info!(
            "[ExecuteTask]: Processing {} records for output collection {}",
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
                tracing::info!("[ExecuteTask]: Output segment uninitialized - first task run");
                None
            }
            Err(e) => return Err((*e).into()),
        };

        // Execute the task using the provided executor
        let output_records = self
            .task_executor
            .execute(input.log_records.clone(), record_segment_reader.as_ref())
            .await
            .map_err(ExecuteTaskError::SegmentRead)?;

        // Update log offsets for output records
        // completion_offset = -1 means "no records processed yet", treat as 0
        let base_offset = if input.completion_offset == u64::MAX {
            0i64
        } else {
            input.completion_offset as i64
        };
        let output_records_with_offsets: Vec<LogRecord> = output_records
            .iter()
            .enumerate()
            .map(|(i, (log_record, _))| LogRecord {
                log_offset: base_offset + i as i64,
                record: log_record.record.clone(),
            })
            .collect();

        tracing::info!(
            "[ExecuteTask]: Task executed successfully, produced {} output records",
            output_records_with_offsets.len()
        );

        // Return the output records to be partitioned
        Ok(ExecuteTaskOutput {
            records_processed: records_count,
            output_records: Chunk::new(Arc::from(output_records_with_offsets)),
        })
    }
}
