use std::{
    cell::OnceCell,
    sync::{atomic::AtomicU32, Arc},
};

use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_segment::{
    blockfile_metadata::{MetadataSegmentError, MetadataSegmentWriter},
    blockfile_record::{
        RecordSegmentReader, RecordSegmentReaderCreationError, RecordSegmentWriter,
        RecordSegmentWriterCreationError,
    },
    distributed_hnsw::{DistributedHNSWSegmentFromSegmentError, DistributedHNSWSegmentWriter},
    distributed_spann::SpannSegmentWriterError,
    types::VectorSegmentWriter,
};
use chroma_system::{
    wrap, ChannelError, ComponentContext, ComponentHandle, Dispatcher, Handler, Orchestrator,
    OrchestratorContext, PanicError, TaskError, TaskMessage, TaskResult,
};
use chroma_types::{
    AttachedFunctionUuid, Chunk, CollectionAndSegments, CollectionUuid, JobId, LogRecord,
    SegmentType,
};
use thiserror::Error;
use tokio::sync::oneshot::{error::RecvError, Sender};
use tracing::Span;
use uuid::Uuid;

use crate::execution::{
    operators::{
        execute_task::{
            ExecuteAttachedFunctionError, ExecuteAttachedFunctionInput,
            ExecuteAttachedFunctionOperator, ExecuteAttachedFunctionOutput,
        },
        get_attached_function::{
            GetAttachedFunctionInput, GetAttachedFunctionOperator,
            GetAttachedFunctionOperatorError, GetAttachedFunctionOutput,
        },
        get_collection_and_segments::{
            GetCollectionAndSegmentsError, GetCollectionAndSegmentsOperator,
        },
        materialize_logs::{
            MaterializeLogInput, MaterializeLogOperator, MaterializeLogOperatorError,
            MaterializeLogOutput,
        },
    },
    orchestration::compact::{CompactionContextError, ExecutionState},
};

use super::compact::{CollectionCompactInfo, CompactWriters, CompactionContext};
use chroma_types::AdvanceAttachedFunctionError;

#[derive(Debug, Clone)]
pub struct FunctionContext {
    pub attached_function_id: AttachedFunctionUuid,
    pub function_id: Uuid,
    pub updated_completion_offset: u64,
}

#[derive(Debug)]
pub struct AttachedFunctionOrchestrator {
    input_collection_info: CollectionCompactInfo,
    output_context: CompactionContext,
    result_channel: Option<
        Sender<Result<AttachedFunctionOrchestratorResponse, AttachedFunctionOrchestratorError>>,
    >,

    // Store the materialized outputs from DataFetchOrchestrator
    materialized_log_data: Vec<MaterializeLogOutput>,

    // Function context
    function_context: OnceCell<FunctionContext>,

    // Execution state
    state: ExecutionState,

    orchestrator_context: OrchestratorContext,

    dispatcher: ComponentHandle<Dispatcher>,
}

#[derive(Error, Debug)]
pub enum AttachedFunctionOrchestratorError {
    #[error("Operation aborted because resources exhausted")]
    Aborted,
    #[error("Failed to get attached function: {0}")]
    GetAttachedFunction(#[from] GetAttachedFunctionOperatorError),
    #[error("Failed to get collection and segments: {0}")]
    GetCollectionAndSegments(#[from] GetCollectionAndSegmentsError),
    #[error("No attached function found")]
    NoAttachedFunction,
    #[error("Failed to execute attached function: {0}")]
    ExecuteAttachedFunction(#[from] ExecuteAttachedFunctionError),
    #[error("Failed to advance attached function: {0}")]
    AdvanceAttachedFunction(#[from] AdvanceAttachedFunctionError),
    #[error("Function context not set")]
    FunctionContextNotSet,
    #[error("Invariant violation: {0}")]
    InvariantViolation(String),
    #[error("Failed to materialize log: {0}")]
    MaterializeLog(#[from] MaterializeLogOperatorError),
    #[error("Compaction context error: {0}")]
    CompactionContext(#[from] CompactionContextError),
    #[error("Output collection ID not set")]
    OutputCollectionIdNotSet,
    #[error("Channel error: {0}")]
    Channel(#[from] ChannelError),
    #[error("Could not count current segment: {0}")]
    CountError(Box<dyn chroma_error::ChromaError>),
    #[error("Receiver error: {0}")]
    RecvError(#[from] RecvError),
    #[error("Panic error: {0}")]
    PanicError(#[from] PanicError),
    #[error("Error creating metadata writer: {0}")]
    MetadataSegment(#[from] MetadataSegmentError),
    #[error("Error creating record segment writer: {0}")]
    RecordSegmentWriter(#[from] RecordSegmentWriterCreationError),
    #[error("Error creating record segment reader: {0}")]
    RecordSegmentReader(#[from] RecordSegmentReaderCreationError),
    #[error("Error creating hnsw writer: {0}")]
    HnswSegment(#[from] DistributedHNSWSegmentFromSegmentError),
    #[error("Error creating spann writer: {0}")]
    SpannSegment(#[from] SpannSegmentWriterError),
}

impl ChromaError for AttachedFunctionOrchestratorError {
    fn code(&self) -> ErrorCodes {
        match self {
            AttachedFunctionOrchestratorError::Aborted => ErrorCodes::Aborted,
            AttachedFunctionOrchestratorError::GetAttachedFunction(e) => e.code(),
            AttachedFunctionOrchestratorError::GetCollectionAndSegments(e) => e.code(),
            AttachedFunctionOrchestratorError::NoAttachedFunction => ErrorCodes::NotFound,
            AttachedFunctionOrchestratorError::ExecuteAttachedFunction(e) => e.code(),
            AttachedFunctionOrchestratorError::AdvanceAttachedFunction(e) => e.code(),
            AttachedFunctionOrchestratorError::MaterializeLog(e) => e.code(),
            AttachedFunctionOrchestratorError::FunctionContextNotSet => ErrorCodes::Internal,
            AttachedFunctionOrchestratorError::InvariantViolation(_) => ErrorCodes::Internal,
            AttachedFunctionOrchestratorError::CompactionContext(e) => e.code(),
            AttachedFunctionOrchestratorError::OutputCollectionIdNotSet => ErrorCodes::Internal,
            AttachedFunctionOrchestratorError::Channel(e) => e.code(),
            AttachedFunctionOrchestratorError::RecvError(_) => ErrorCodes::Internal,
            AttachedFunctionOrchestratorError::CountError(e) => e.code(),
            AttachedFunctionOrchestratorError::PanicError(e) => e.code(),
            AttachedFunctionOrchestratorError::MetadataSegment(e) => e.code(),
            AttachedFunctionOrchestratorError::RecordSegmentWriter(e) => e.code(),
            AttachedFunctionOrchestratorError::RecordSegmentReader(e) => e.code(),
            AttachedFunctionOrchestratorError::HnswSegment(e) => e.code(),
            AttachedFunctionOrchestratorError::SpannSegment(e) => e.code(),
        }
    }

    fn should_trace_error(&self) -> bool {
        match self {
            AttachedFunctionOrchestratorError::Aborted => true,
            AttachedFunctionOrchestratorError::GetAttachedFunction(e) => e.should_trace_error(),
            AttachedFunctionOrchestratorError::GetCollectionAndSegments(e) => {
                e.should_trace_error()
            }
            AttachedFunctionOrchestratorError::NoAttachedFunction => false,
            AttachedFunctionOrchestratorError::ExecuteAttachedFunction(e) => e.should_trace_error(),
            AttachedFunctionOrchestratorError::AdvanceAttachedFunction(e) => e.should_trace_error(),
            AttachedFunctionOrchestratorError::MaterializeLog(e) => e.should_trace_error(),
            AttachedFunctionOrchestratorError::FunctionContextNotSet => true,
            AttachedFunctionOrchestratorError::InvariantViolation(_) => true,
            AttachedFunctionOrchestratorError::CompactionContext(e) => e.should_trace_error(),
            AttachedFunctionOrchestratorError::OutputCollectionIdNotSet => true,
            AttachedFunctionOrchestratorError::Channel(e) => e.should_trace_error(),
            AttachedFunctionOrchestratorError::RecvError(_) => true,
            AttachedFunctionOrchestratorError::CountError(e) => e.should_trace_error(),
            AttachedFunctionOrchestratorError::PanicError(e) => e.should_trace_error(),
            AttachedFunctionOrchestratorError::MetadataSegment(e) => e.should_trace_error(),
            AttachedFunctionOrchestratorError::RecordSegmentWriter(e) => e.should_trace_error(),
            AttachedFunctionOrchestratorError::RecordSegmentReader(e) => e.should_trace_error(),
            AttachedFunctionOrchestratorError::HnswSegment(e) => e.should_trace_error(),
            AttachedFunctionOrchestratorError::SpannSegment(e) => e.should_trace_error(),
        }
    }
}

impl<E> From<TaskError<E>> for AttachedFunctionOrchestratorError
where
    E: Into<AttachedFunctionOrchestratorError>,
{
    fn from(value: TaskError<E>) -> Self {
        match value {
            TaskError::Aborted => AttachedFunctionOrchestratorError::Aborted,
            TaskError::Panic(e) => e.into(),
            TaskError::TaskFailed(e) => e.into(),
        }
    }
}

#[derive(Debug)]
pub enum AttachedFunctionOrchestratorResponse {
    /// No attached function was found, so nothing was executed
    NoAttachedFunction { job_id: JobId },
    /// Success - attached function was executed successfully
    Success {
        job_id: JobId,
        materialized_output: Vec<MaterializeLogOutput>,
        output_collection_info: CollectionCompactInfo,
        attached_function_id: AttachedFunctionUuid,
        completion_offset: u64,
    },
}

impl AttachedFunctionOrchestrator {
    pub fn new(
        input_collection_info: CollectionCompactInfo,
        output_context: CompactionContext,
        dispatcher: ComponentHandle<Dispatcher>,
        data_fetch_records: Vec<MaterializeLogOutput>,
    ) -> Self {
        let orchestrator_context = OrchestratorContext::new(dispatcher.clone());

        AttachedFunctionOrchestrator {
            input_collection_info,
            output_context,
            result_channel: None,
            materialized_log_data: data_fetch_records,
            function_context: OnceCell::new(),
            state: ExecutionState::MaterializeApplyCommitFlush,
            orchestrator_context,
            dispatcher,
        }
    }

    /// Get the input collection info, following the same pattern as CompactionContext
    pub fn get_input_collection_info(&self) -> &CollectionCompactInfo {
        &self.input_collection_info
    }

    /// Get the output collection info if it has been set
    pub fn get_output_collection_info(
        &self,
    ) -> Result<&CollectionCompactInfo, AttachedFunctionOrchestratorError> {
        self.output_context
            .get_collection_info()
            .map_err(AttachedFunctionOrchestratorError::CompactionContext)
    }

    /// Get the output collection ID if it has been set
    pub fn get_output_collection_id(
        &self,
    ) -> Result<CollectionUuid, AttachedFunctionOrchestratorError> {
        self.output_context
            .get_collection_info()
            .map(|info| info.collection_id)
            .map_err(AttachedFunctionOrchestratorError::CompactionContext)
    }

    /// Set the output collection info
    pub fn set_output_collection_info(
        &mut self,
        collection_info: CollectionCompactInfo,
    ) -> Result<(), CollectionCompactInfo> {
        self.output_context.collection_info.set(collection_info)
    }

    /// Get the function context if it has been set
    pub fn get_function_context(&self) -> Option<&FunctionContext> {
        self.function_context.get()
    }

    /// Set the function context
    pub fn set_function_context(
        &self,
        function_context: FunctionContext,
    ) -> Result<(), FunctionContext> {
        self.function_context.set(function_context)
    }

    async fn finish_no_attached_function(&mut self, ctx: &ComponentContext<Self>) {
        let collection_info = self.get_input_collection_info();
        let job_id = collection_info.collection_id.into();
        self.terminate_with_result(
            Ok(AttachedFunctionOrchestratorResponse::NoAttachedFunction { job_id }),
            ctx,
        )
        .await;
    }

    async fn finish_success(
        &mut self,
        materialized_output: Vec<MaterializeLogOutput>,
        ctx: &ComponentContext<Self>,
    ) {
        let collection_info = self.get_input_collection_info();

        // Get output collection info - should always exist in success case
        let output_collection_info = match self.get_output_collection_info() {
            Ok(info) => info.clone(),
            Err(e) => {
                self.terminate_with_result(Err(e), ctx).await;
                return;
            }
        };

        // Get attached function ID - should always exist in success case
        let attached_function = match self.get_function_context() {
            Some(func) => func,
            None => {
                self.terminate_with_result(
                    Err(AttachedFunctionOrchestratorError::FunctionContextNotSet),
                    ctx,
                )
                .await;
                return;
            }
        };
        let attached_function_id = attached_function.attached_function_id;

        // Get the completion offset from the input collection's pulled log offset
        let completion_offset = collection_info.pulled_log_offset as u64;

        let materialized_output = materialized_output
            .into_iter()
            .filter(|output| !output.result.is_empty())
            .collect::<Vec<_>>();

        tracing::info!(
            "Attached function finished successfully with {} records",
            materialized_output.len()
        );

        let job_id = collection_info.collection_id.into();
        self.terminate_with_result(
            Ok(AttachedFunctionOrchestratorResponse::Success {
                job_id,
                materialized_output,
                output_collection_info,
                attached_function_id,
                completion_offset,
            }),
            ctx,
        )
        .await;
    }

    async fn materialize_log(
        &mut self,
        partitions: Vec<Chunk<LogRecord>>,
        ctx: &ComponentContext<Self>,
    ) {
        self.state = ExecutionState::MaterializeApplyCommitFlush;

        // NOTE: We allow writers to be uninitialized for the case when the materialized logs are empty
        let record_reader = self
            .output_context
            .get_segment_writers()
            .ok()
            .and_then(|writers| writers.record_reader);
        tracing::info!(
            "Materializing to collection: {:?}",
            self.output_context
                .get_collection_info()
                .unwrap()
                .collection_id
        );

        let next_max_offset_id = Arc::new(
            record_reader
                .as_ref()
                .map(|reader| AtomicU32::new(reader.get_max_offset_id() + 1))
                .unwrap_or_default(),
        );

        if let Some(rr) = record_reader.as_ref() {
            let count = match rr.count().await {
                Ok(count) => count as u64,
                Err(err) => {
                    return self
                        .terminate_with_result(
                            Err(AttachedFunctionOrchestratorError::CountError(err)),
                            ctx,
                        )
                        .await;
                }
            };

            let collection_info = match self.output_context.get_collection_info_mut() {
                Ok(info) => info,
                Err(err) => {
                    return self.terminate_with_result(Err(err.into()), ctx).await;
                }
            };
            collection_info.collection.total_records_post_compaction = count;
        }

        for partition in partitions.iter() {
            let operator = MaterializeLogOperator::new();
            let input = MaterializeLogInput::new(
                partition.clone(),
                record_reader.clone(),
                next_max_offset_id.clone(),
            );
            let task = wrap(
                operator,
                input,
                ctx.receiver(),
                self.output_context
                    .orchestrator_context
                    .task_cancellation_token
                    .clone(),
            );
            self.send(task, ctx, Some(Span::current())).await;
        }
    }
}

#[async_trait]
impl Orchestrator for AttachedFunctionOrchestrator {
    type Output = AttachedFunctionOrchestratorResponse;
    type Error = AttachedFunctionOrchestratorError;

    fn dispatcher(&self) -> ComponentHandle<Dispatcher> {
        self.dispatcher.clone()
    }

    fn context(&self) -> &OrchestratorContext {
        &self.orchestrator_context
    }

    async fn initial_tasks(
        &mut self,
        ctx: &ComponentContext<Self>,
    ) -> Vec<(TaskMessage, Option<Span>)> {
        // Start by getting the attached function for this collection
        let collection_info = self.get_input_collection_info();
        let operator = Box::new(GetAttachedFunctionOperator::new(
            self.output_context.sysdb.clone(),
            collection_info.collection_id,
        ));
        let input = GetAttachedFunctionInput {
            collection_id: collection_info.collection_id,
        };
        let task = wrap(
            operator,
            input,
            ctx.receiver(),
            self.context().task_cancellation_token.clone(),
        );
        vec![(task, Some(Span::current()))]
    }

    fn set_result_channel(
        &mut self,
        sender: Sender<
            Result<AttachedFunctionOrchestratorResponse, AttachedFunctionOrchestratorError>,
        >,
    ) {
        self.result_channel = Some(sender)
    }

    fn take_result_channel(
        &mut self,
    ) -> Option<
        Sender<Result<AttachedFunctionOrchestratorResponse, AttachedFunctionOrchestratorError>>,
    > {
        self.result_channel.take()
    }
}

#[async_trait]
impl Handler<TaskResult<MaterializeLogOutput, MaterializeLogOperatorError>>
    for AttachedFunctionOrchestrator
{
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<MaterializeLogOutput, MaterializeLogOperatorError>,
        ctx: &ComponentContext<Self>,
    ) {
        let message = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(message) => message,
            None => return,
        };

        self.finish_success(vec![message], ctx).await;
    }
}

#[async_trait]
impl Handler<TaskResult<GetAttachedFunctionOutput, GetAttachedFunctionOperatorError>>
    for AttachedFunctionOrchestrator
{
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<GetAttachedFunctionOutput, GetAttachedFunctionOperatorError>,
        ctx: &ComponentContext<Self>,
    ) {
        let message = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(message) => message,
            None => return,
        };

        match message.attached_function {
            Some(attached_function) => {
                tracing::info!(
                    "[AttachedFunctionOrchestrator]: Found attached function '{}' for collection",
                    attached_function.name
                );

                if self
                    .set_function_context(FunctionContext {
                        attached_function_id: attached_function.id,
                        function_id: attached_function.function_id,
                        updated_completion_offset: attached_function.completion_offset,
                    })
                    .is_err()
                {
                    self.terminate_with_result(
                        Err(AttachedFunctionOrchestratorError::InvariantViolation(
                            "Failed to set function context for attached function".to_string(),
                        )),
                        ctx,
                    )
                    .await;
                    return;
                }

                // Get the output collection ID from the attached function
                let output_collection_id = match attached_function.output_collection_id {
                    Some(id) => id,
                    None => {
                        tracing::error!(
                            "[AttachedFunctionOrchestrator]: Output collection ID not set for attached function '{}'",
                            attached_function.name
                        );
                        self.terminate_with_result(
                            Err(AttachedFunctionOrchestratorError::OutputCollectionIdNotSet),
                            ctx,
                        )
                        .await;
                        return;
                    }
                };

                // Next step: get the output collection segments using the existing GetCollectionAndSegmentsOperator
                let operator = Box::new(GetCollectionAndSegmentsOperator::new(
                    self.output_context.sysdb.clone(),
                    output_collection_id,
                ));
                let input = ();
                let task = wrap(
                    operator,
                    input,
                    ctx.receiver(),
                    self.context().task_cancellation_token.clone(),
                );
                let res = self.dispatcher().send(task, None).await;
                self.ok_or_terminate(res, ctx).await;
            }
            None => {
                tracing::info!("[AttachedFunctionOrchestrator]: No attached function found");
                self.finish_no_attached_function(ctx).await;
            }
        }
    }
}

#[async_trait]
impl Handler<TaskResult<CollectionAndSegments, GetCollectionAndSegmentsError>>
    for AttachedFunctionOrchestrator
{
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<CollectionAndSegments, GetCollectionAndSegmentsError>,
        ctx: &ComponentContext<Self>,
    ) {
        let message = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(message) => message,
            None => return,
        };

        tracing::debug!(
            "[AttachedFunctionOrchestrator]: Found output collection segments - metadata: {:?}, record: {:?}, vector: {:?}",
            message.metadata_segment.id,
            message.record_segment.id,
            message.vector_segment.id
        );

        // Create segment writers for the output collection
        let collection = &message.collection;
        let dimension = match collection.dimension {
            Some(dim) => dim as usize,
            None => {
                // Output collection is not initialized, cannot create writers
                self.terminate_with_result(
                    Err(AttachedFunctionOrchestratorError::InvariantViolation(
                        "Output collection dimension is not set".to_string(),
                    )),
                    ctx,
                )
                .await;
                return;
            }
        };

        let record_writer = match self
            .ok_or_terminate(
                RecordSegmentWriter::from_segment(
                    &collection.tenant,
                    &collection.database_id,
                    &message.record_segment,
                    &self.output_context.blockfile_provider,
                )
                .await,
                ctx,
            )
            .await
        {
            Some(writer) => writer,
            None => return,
        };

        let metadata_writer = match self
            .ok_or_terminate(
                MetadataSegmentWriter::from_segment(
                    &collection.tenant,
                    &collection.database_id,
                    &message.metadata_segment,
                    &self.output_context.blockfile_provider,
                )
                .await,
                ctx,
            )
            .await
        {
            Some(writer) => writer,
            None => return,
        };

        let (hnsw_index_uuid, vector_writer) = match message.vector_segment.r#type {
            SegmentType::Spann => match self
                .ok_or_terminate(
                    self.output_context
                        .spann_provider
                        .write(collection, &message.vector_segment, dimension)
                        .await,
                    ctx,
                )
                .await
            {
                Some(writer) => (writer.hnsw_index_uuid(), VectorSegmentWriter::Spann(writer)),
                None => return,
            },
            _ => match self
                .ok_or_terminate(
                    DistributedHNSWSegmentWriter::from_segment(
                        collection,
                        &message.vector_segment,
                        dimension,
                        self.output_context.hnsw_provider.clone(),
                    )
                    .await
                    .map_err(|err| *err),
                    ctx,
                )
                .await
            {
                Some(writer) => (writer.index_uuid(), VectorSegmentWriter::Hnsw(writer)),
                None => return,
            },
        };

        // Create record reader for the output collection to load existing statistics
        let record_reader = match self
            .ok_or_terminate(
                match Box::pin(RecordSegmentReader::from_segment(
                    &message.record_segment,
                    &self.output_context.blockfile_provider,
                ))
                .await
                {
                    Ok(reader) => Ok(Some(reader)),
                    Err(err) => match *err {
                        RecordSegmentReaderCreationError::UninitializedSegment => Ok(None),
                        _ => Err(*err),
                    },
                },
                ctx,
            )
            .await
        {
            Some(reader) => reader,
            None => return,
        };

        let writers = CompactWriters {
            record_reader: record_reader.filter(|_| !self.output_context.is_rebuild),
            metadata_writer,
            record_writer,
            vector_writer,
        };

        // Store the output collection info with writers
        let output_collection_info = CollectionCompactInfo {
            collection_id: message.collection.collection_id,
            collection: message.collection.clone(),
            writers: Some(writers),
            pulled_log_offset: message.collection.log_position,
            hnsw_index_uuid: Some(hnsw_index_uuid),
            schema: message.collection.schema.clone(),
        };

        if self
            .set_output_collection_info(output_collection_info)
            .is_err()
        {
            self.terminate_with_result(
                Err(AttachedFunctionOrchestratorError::InvariantViolation(
                    "Failed to set output collection info".to_string(),
                )),
                ctx,
            )
            .await;
            return;
        }

        let function_context = self.function_context.get();

        let attached_function = match function_context {
            Some(func) => func,
            None => {
                self.terminate_with_result(
                    Err(AttachedFunctionOrchestratorError::NoAttachedFunction),
                    ctx,
                )
                .await;
                return;
            }
        };

        let function_id = attached_function.function_id;
        // Execute the attached function
        let operator = match ExecuteAttachedFunctionOperator::from_attached_function(
            function_id,
            self.output_context.log.clone(),
        ) {
            Ok(op) => Box::new(op),
            Err(e) => {
                self.terminate_with_result(
                    Err(AttachedFunctionOrchestratorError::ExecuteAttachedFunction(
                        e,
                    )),
                    ctx,
                )
                .await;
                return;
            }
        };

        // Get the input collection info to access pulled_log_offset
        let collection_info = self.get_input_collection_info();

        // Get the input collection's record segment reader
        // This can be None if the input collection is uninitialized or in rebuild mode
        let input_record_segment = self
            .input_collection_info
            .writers
            .as_ref()
            .and_then(|writers| writers.record_reader.clone());

        let input = ExecuteAttachedFunctionInput {
            materialized_logs: self.materialized_log_data.clone(), // Use the actual materialized logs from data fetch
            tenant_id: "default".to_string(), // TODOItanujnay112): Get actual tenant ID
            input_record_segment,
            output_collection_id: message.collection.collection_id,
            completion_offset: collection_info.pulled_log_offset as u64, // Use the completion offset from input collection
            output_record_segment: message.record_segment.clone(),
            blockfile_provider: self.output_context.blockfile_provider.clone(),
            is_rebuild: self.output_context.is_rebuild,
        };

        let task = wrap(
            operator,
            input,
            ctx.receiver(),
            self.context().task_cancellation_token.clone(),
        );
        let res = self.dispatcher().send(task, None).await;
        self.ok_or_terminate(res, ctx).await;
    }
}

#[async_trait]
impl Handler<TaskResult<ExecuteAttachedFunctionOutput, ExecuteAttachedFunctionError>>
    for AttachedFunctionOrchestrator
{
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<ExecuteAttachedFunctionOutput, ExecuteAttachedFunctionError>,
        ctx: &ComponentContext<Self>,
    ) {
        let message = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(message) => message,
            None => return,
        };

        tracing::info!(
            "[AttachedFunctionOrchestrator]: Attached function executed successfully, processed {} records",
            message.records_processed
        );
        self.materialize_log(vec![message.output_records], ctx)
            .await;
    }
}
