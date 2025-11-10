use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_segment::{
    blockfile_metadata::MetadataSegmentError,
    blockfile_record::{
        ApplyMaterializedLogError, RecordSegmentReaderCreationError,
        RecordSegmentWriterCreationError,
    },
    distributed_hnsw::DistributedHNSWSegmentFromSegmentError,
    distributed_spann::SpannSegmentWriterError,
    types::{ChromaSegmentFlusher, ChromaSegmentWriter, MaterializeLogsResult},
};
use chroma_system::{
    wrap, ChannelError, ComponentContext, ComponentHandle, Dispatcher, Handler, Orchestrator,
    OrchestratorContext, PanicError, TaskError, TaskMessage, TaskResult,
};
use chroma_types::{JobId, Schema, SchemaError, SegmentFlushInfo, SegmentUuid};
use thiserror::Error;
use tokio::sync::oneshot::{error::RecvError, Sender};
use tracing::Span;

use crate::execution::{
    operators::{
        apply_log_to_segment_writer::{
            ApplyLogToSegmentWriterInput, ApplyLogToSegmentWriterOperator,
            ApplyLogToSegmentWriterOperatorError, ApplyLogToSegmentWriterOutput,
        },
        commit_segment_writer::{
            CommitSegmentWriterInput, CommitSegmentWriterOperator,
            CommitSegmentWriterOperatorError, CommitSegmentWriterOutput,
        },
        flush_segment_writer::{
            FlushSegmentWriterInput, FlushSegmentWriterOperator, FlushSegmentWriterOperatorError,
            FlushSegmentWriterOutput,
        },
        materialize_logs::MaterializeLogOutput,
    },
    orchestration::compact::{CollectionCompactInfo, CompactionContextError},
};

use super::compact::{CompactionContext, CompactionMetrics, ExecutionState};

#[derive(Debug)]
pub struct ApplyLogsOrchestrator {
    context: CompactionContext,
    flush_results: Vec<SegmentFlushInfo>,
    result_channel:
        Option<Sender<Result<ApplyLogsOrchestratorResponse, ApplyLogsOrchestratorError>>>,
    num_uncompleted_materialization_tasks: usize,
    num_uncompleted_tasks_by_segment: HashMap<SegmentUuid, usize>,
    collection_logical_size_delta_bytes: i64,
    state: ExecutionState,

    // Total number of materialized logs
    num_materialized_logs: u64,

    // We track a parent span for each segment type so we can group all the spans for a given segment type (makes the resulting trace much easier to read)
    segment_spans: HashMap<SegmentUuid, Span>,

    // Store the materialized outputs from LogFetchOrchestrator
    materialized_log_data: Option<Arc<Vec<MaterializeLogOutput>>>,

    metrics: CompactionMetrics,
}

#[derive(Error, Debug)]
pub enum ApplyLogsOrchestratorError {
    #[error("Operation aborted because resources exhausted")]
    Aborted,
    #[error("Error applying logs to segment writers: {0}")]
    ApplyLog(#[from] ApplyLogToSegmentWriterOperatorError),
    #[error("Error sending message through channel: {0}")]
    Channel(#[from] ChannelError),
    #[error("Error commiting segment writers: {0}")]
    Commit(#[from] CommitSegmentWriterOperatorError),
    #[error("Error getting from compaction context: {0}")]
    CompactionContext(#[from] CompactionContextError),
    #[error("Error flushing segment writers: {0}")]
    Flush(#[from] FlushSegmentWriterOperatorError),
    #[error("Error creating hnsw writer: {0}")]
    HnswSegment(#[from] DistributedHNSWSegmentFromSegmentError),
    #[error("Invariant violation: {}", .0)]
    InvariantViolation(&'static str),
    #[error("Error creating metadata writer: {0}")]
    MetadataSegment(#[from] MetadataSegmentError),
    #[error("Panic during compaction: {0}")]
    Panic(#[from] PanicError),
    #[error("Error creating record segment reader: {0}")]
    RecordSegmentReader(#[from] RecordSegmentReaderCreationError),
    #[error("Error creating record segment writer: {0}")]
    RecordSegmentWriter(#[from] RecordSegmentWriterCreationError),
    #[error("Error receiving final result: {0}")]
    Result(#[from] RecvError),
    #[error("Error creating spann writer: {0}")]
    SpannSegment(#[from] SpannSegmentWriterError),
    #[error("Could not count current segment: {0}")]
    CountError(Box<dyn chroma_error::ChromaError>),
}

impl ChromaError for ApplyLogsOrchestratorError {
    fn code(&self) -> ErrorCodes {
        match self {
            ApplyLogsOrchestratorError::Aborted => ErrorCodes::Aborted,
            _ => ErrorCodes::Internal,
        }
    }

    fn should_trace_error(&self) -> bool {
        match self {
            Self::Aborted => true,
            Self::ApplyLog(e) => e.should_trace_error(),
            Self::Channel(e) => e.should_trace_error(),
            Self::Commit(e) => e.should_trace_error(),
            Self::CompactionContext(e) => e.should_trace_error(),
            Self::Flush(e) => e.should_trace_error(),
            Self::HnswSegment(e) => e.should_trace_error(),
            Self::InvariantViolation(_) => true,
            Self::MetadataSegment(e) => e.should_trace_error(),
            Self::Panic(e) => e.should_trace_error(),
            Self::RecordSegmentReader(e) => e.should_trace_error(),
            Self::RecordSegmentWriter(e) => e.should_trace_error(),
            Self::Result(_) => true,
            Self::SpannSegment(e) => e.should_trace_error(),
            Self::CountError(e) => e.should_trace_error(),
        }
    }
}

impl<E> From<TaskError<E>> for ApplyLogsOrchestratorError
where
    E: Into<ApplyLogsOrchestratorError>,
{
    fn from(value: TaskError<E>) -> Self {
        match value {
            TaskError::Aborted => ApplyLogsOrchestratorError::Aborted,
            TaskError::Panic(e) => e.into(),
            TaskError::TaskFailed(e) => e.into(),
        }
    }
}

#[derive(Debug)]
pub struct ApplyLogsOrchestratorResponse {
    pub job_id: JobId,
    pub total_records_post_compaction: u64,
    pub flush_results: Vec<SegmentFlushInfo>,
    pub collection_logical_size_bytes: u64,
    pub schema: Option<Schema>,
}

impl ApplyLogsOrchestratorResponse {
    pub fn new(
        job_id: JobId,
        total_records_post_compaction: u64,
        flush_results: Vec<SegmentFlushInfo>,
        collection_logical_size_bytes: u64,
        schema: Option<Schema>,
    ) -> Self {
        ApplyLogsOrchestratorResponse {
            job_id,
            total_records_post_compaction,
            flush_results,
            collection_logical_size_bytes,
            schema,
        }
    }

    pub fn new_with_empty_results(job_id: JobId, collection_info: &CollectionCompactInfo) -> Self {
        ApplyLogsOrchestratorResponse {
            job_id,
            total_records_post_compaction: collection_info.collection.total_records_post_compaction,
            flush_results: Vec::new(),
            collection_logical_size_bytes: collection_info.collection.size_bytes_post_compaction,
            schema: collection_info.collection.schema.clone(),
        }
    }
}

impl ApplyLogsOrchestrator {
    pub fn new(
        context: &CompactionContext,
        materialized_log_data: Option<Arc<Vec<MaterializeLogOutput>>>,
    ) -> Self {
        ApplyLogsOrchestrator {
            context: context.clone(),
            flush_results: Vec::new(),
            result_channel: None,
            num_uncompleted_materialization_tasks: 0,
            num_uncompleted_tasks_by_segment: HashMap::new(),
            collection_logical_size_delta_bytes: 0,
            state: ExecutionState::MaterializeApplyCommitFlush,
            num_materialized_logs: 0,
            segment_spans: HashMap::new(),
            materialized_log_data,
            metrics: CompactionMetrics::default(),
        }
    }

    async fn create_apply_log_to_segment_writer_tasks(
        &mut self,
        materialized_logs: MaterializeLogsResult,
        ctx: &ComponentContext<Self>,
    ) -> Result<Vec<(TaskMessage, Option<Span>)>, CompactionContextError> {
        let mut tasks_to_run = Vec::new();
        self.num_materialized_logs += materialized_logs.len() as u64;

        let writers = self.context.get_output_segment_writers()?;

        {
            self.num_uncompleted_tasks_by_segment
                .entry(writers.record_writer.id)
                .and_modify(|v| {
                    *v += 1;
                })
                .or_insert(1);

            let writer = ChromaSegmentWriter::RecordSegment(writers.record_writer);
            let span = self.get_segment_writer_span(&writer);
            let operator = ApplyLogToSegmentWriterOperator::new();
            let input = ApplyLogToSegmentWriterInput::new(
                writer,
                materialized_logs.clone(),
                writers.record_reader.clone(),
                None,
                #[cfg(test)]
                self.context.poison_offset,
            );
            let task = wrap(
                operator,
                input,
                ctx.receiver(),
                self.context
                    .orchestrator_context
                    .task_cancellation_token
                    .clone(),
            );
            tasks_to_run.push((task, Some(span)));
        }

        {
            self.num_uncompleted_tasks_by_segment
                .entry(writers.metadata_writer.id)
                .and_modify(|v| {
                    *v += 1;
                })
                .or_insert(1);

            let writer = ChromaSegmentWriter::MetadataSegment(writers.metadata_writer);
            let span = self.get_segment_writer_span(&writer);
            let operator = ApplyLogToSegmentWriterOperator::new();
            let input = ApplyLogToSegmentWriterInput::new(
                writer,
                materialized_logs.clone(),
                writers.record_reader.clone(),
                self.context
                    .get_output_collection_info()?
                    .collection
                    .schema
                    .clone(),
                #[cfg(test)]
                self.context.poison_offset,
            );
            let task = wrap(
                operator,
                input,
                ctx.receiver(),
                self.context
                    .orchestrator_context
                    .task_cancellation_token
                    .clone(),
            );
            tasks_to_run.push((task, Some(span)));
        }

        {
            self.num_uncompleted_tasks_by_segment
                .entry(writers.vector_writer.get_id())
                .and_modify(|v| {
                    *v += 1;
                })
                .or_insert(1);

            let writer = ChromaSegmentWriter::VectorSegment(writers.vector_writer);
            let span = self.get_segment_writer_span(&writer);
            let operator = ApplyLogToSegmentWriterOperator::new();
            let input = ApplyLogToSegmentWriterInput::new(
                writer,
                materialized_logs,
                writers.record_reader,
                None,
                #[cfg(test)]
                self.context.poison_offset,
            );
            let task = wrap(
                operator,
                input,
                ctx.receiver(),
                self.context
                    .orchestrator_context
                    .task_cancellation_token
                    .clone(),
            );
            tasks_to_run.push((task, Some(span)));
        }

        Ok(tasks_to_run)
    }

    async fn dispatch_segment_writer_commit(
        &mut self,
        segment_writer: ChromaSegmentWriter<'static>,
        ctx: &ComponentContext<Self>,
    ) {
        let span = self.get_segment_writer_span(&segment_writer);
        let operator = CommitSegmentWriterOperator::new();
        let input = CommitSegmentWriterInput::new(segment_writer);
        let task = wrap(
            operator,
            input,
            ctx.receiver(),
            self.context
                .orchestrator_context
                .task_cancellation_token
                .clone(),
        );
        let res = self.dispatcher().send(task, Some(span)).await;
        self.ok_or_terminate(res, ctx).await;
    }

    async fn dispatch_segment_flush(
        &mut self,
        segment_flusher: ChromaSegmentFlusher,
        ctx: &ComponentContext<Self>,
    ) {
        let span = self.get_segment_flusher_span(&segment_flusher);
        let operator = FlushSegmentWriterOperator::new();
        let input = FlushSegmentWriterInput::new(segment_flusher);
        let task = wrap(
            operator,
            input,
            ctx.receiver(),
            self.context
                .orchestrator_context
                .task_cancellation_token
                .clone(),
        );
        let res = self.dispatcher().send(task, Some(span)).await;
        self.ok_or_terminate(res, ctx).await;
    }

    async fn finish_materialized_output(&mut self, ctx: &ComponentContext<Self>) {
        self.metrics
            .total_logs_applied_flushed
            .add(self.num_materialized_logs, &[]);

        self.state = ExecutionState::Register;
        let collection_info = match self.context.get_output_collection_info() {
            Ok(collection_info) => collection_info,
            Err(err) => {
                self.terminate_with_result(Err(err.into()), ctx).await;
                return;
            }
        };
        let collection = collection_info.collection.clone();
        let collection_logical_size_bytes = if self.context.is_rebuild {
            match u64::try_from(self.collection_logical_size_delta_bytes) {
                Ok(size_bytes) => size_bytes,
                _ => {
                    self.terminate_with_result(
                        Err(ApplyLogsOrchestratorError::InvariantViolation(
                            "The collection size delta after rebuild should be non-negative",
                        )),
                        ctx,
                    )
                    .await;
                    return;
                }
            }
        } else {
            collection
                .size_bytes_post_compaction
                .saturating_add_signed(self.collection_logical_size_delta_bytes)
        };

        let flush_results = std::mem::take(&mut self.flush_results);
        let total_records_post_compaction = collection.total_records_post_compaction;
        let job_id = collection.collection_id.into();
        // let collection_logical_size_delta_bytes = self.collection_logical_size_delta_bytes;
        self.terminate_with_result(
            Ok(ApplyLogsOrchestratorResponse::new(
                job_id,
                total_records_post_compaction,
                flush_results,
                collection_logical_size_bytes,
                collection_info.schema.clone(),
            )),
            ctx,
        )
        .await;
    }

    fn get_segment_writer_span(&mut self, writer: &ChromaSegmentWriter) -> Span {
        let span = self
            .segment_spans
            .entry(writer.get_id())
            .or_insert_with(|| {
                tracing::span!(
                    tracing::Level::INFO,
                    "Segment",
                    otel.name = format!("Segment: {:?}", writer.get_name())
                )
            });
        span.clone()
    }

    fn get_segment_flusher_span(&mut self, flusher: &ChromaSegmentFlusher) -> Span {
        match self.segment_spans.get(&flusher.get_id()) {
            Some(span) => span.clone(),
            None => {
                tracing::error!(
                    "No span found for segment: {:?}. This should never happen because get_segment_writer_span() should have previously created a span.",
                    flusher.get_name()
                );
                Span::current()
            }
        }
    }
}

#[async_trait]
impl Orchestrator for ApplyLogsOrchestrator {
    type Output = ApplyLogsOrchestratorResponse;
    type Error = ApplyLogsOrchestratorError;

    fn dispatcher(&self) -> ComponentHandle<Dispatcher> {
        self.context.dispatcher.clone()
    }

    fn context(&self) -> &OrchestratorContext {
        &self.context.orchestrator_context
    }

    async fn initial_tasks(
        &mut self,
        ctx: &ComponentContext<Self>,
    ) -> Vec<(TaskMessage, Option<Span>)> {
        let mut tasks = Vec::new();
        let materialized_outputs = match self.materialized_log_data.take() {
            Some(outputs) => outputs,
            None => {
                self.terminate_with_result(
                    Err(ApplyLogsOrchestratorError::InvariantViolation(
                        "Materialized log data should have been set",
                    )),
                    ctx,
                )
                .await;
                return Vec::new();
            }
        };

        for materialized_output in materialized_outputs.iter() {
            if materialized_output.result.is_empty() {
                self.terminate_with_result(
                    Err(ApplyLogsOrchestratorError::InvariantViolation(
                        "Attempting to apply an empty materialized output",
                    )),
                    ctx,
                )
                .await;
                return Vec::new();
            }
            self.collection_logical_size_delta_bytes +=
                materialized_output.collection_logical_size_delta;

            // Create tasks for each materialized output
            let result = self
                .create_apply_log_to_segment_writer_tasks(materialized_output.result.clone(), ctx)
                .await;

            let mut new_tasks = match result {
                Ok(tasks) => tasks,
                Err(err) => {
                    self.terminate_with_result(Err(err.into()), ctx).await;
                    return Vec::new();
                }
            };
            tasks.append(&mut new_tasks);
        }

        tasks
    }

    fn set_result_channel(
        &mut self,
        sender: Sender<Result<ApplyLogsOrchestratorResponse, ApplyLogsOrchestratorError>>,
    ) {
        self.result_channel = Some(sender)
    }

    fn take_result_channel(
        &mut self,
    ) -> Option<Sender<Result<ApplyLogsOrchestratorResponse, ApplyLogsOrchestratorError>>> {
        self.result_channel.take()
    }
}

#[async_trait]
impl Handler<TaskResult<ApplyLogToSegmentWriterOutput, ApplyLogToSegmentWriterOperatorError>>
    for ApplyLogsOrchestrator
{
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<ApplyLogToSegmentWriterOutput, ApplyLogToSegmentWriterOperatorError>,
        ctx: &ComponentContext<Self>,
    ) {
        let message = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(message) => message,
            None => return,
        };

        if message.segment_type == "MetadataSegmentWriter" {
            if let Some(update) = message.schema_update {
                let collection_info = match self.context.get_output_collection_info_mut() {
                    Ok(info) => info,
                    Err(err) => {
                        return self.terminate_with_result(Err(err.into()), ctx).await;
                    }
                };

                match collection_info.schema.take() {
                    Some(existing) => match existing.merge(&update) {
                        Ok(merged) => {
                            collection_info.schema = Some(merged);
                        }
                        Err(err) => {
                            let err = ApplyLogsOrchestratorError::ApplyLog(
                                ApplyLogToSegmentWriterOperatorError::ApplyMaterializedLogsError(
                                    ApplyMaterializedLogError::Schema(err),
                                ),
                            );
                            self.terminate_with_result(Err(err), ctx).await;
                            return;
                        }
                    },
                    None => {
                        let err = ApplyLogsOrchestratorError::ApplyLog(
                            ApplyLogToSegmentWriterOperatorError::ApplyMaterializedLogsError(
                                ApplyMaterializedLogError::Schema(SchemaError::InvalidSchema {
                                    reason: "schema not found".to_string(),
                                }),
                            ),
                        );
                        self.terminate_with_result(Err(err), ctx).await;
                        return;
                    }
                }
            }
        }
        self.num_uncompleted_tasks_by_segment
            .entry(message.segment_id)
            .and_modify(|v| {
                *v -= 1;
            });

        let num_tasks_left = {
            let num_tasks_left = self
                .num_uncompleted_tasks_by_segment
                .get(&message.segment_id)
                .ok_or(ApplyLogsOrchestratorError::InvariantViolation(
                    "Invariant violation: segment writer task count not found",
                ))
                .cloned();
            match self.ok_or_terminate(num_tasks_left, ctx).await {
                Some(num_tasks_left) => num_tasks_left,
                None => return,
            }
        };

        if num_tasks_left == 0 && self.num_uncompleted_materialization_tasks == 0 {
            let segment_writer = self
                .context
                .get_output_segment_writer_by_id(message.segment_id);
            let segment_writer = match self.ok_or_terminate(segment_writer, ctx).await {
                Some(writer) => writer,
                None => return,
            };

            self.dispatch_segment_writer_commit(segment_writer, ctx)
                .await;
        }
    }
}

#[async_trait]
impl Handler<TaskResult<CommitSegmentWriterOutput, CommitSegmentWriterOperatorError>>
    for ApplyLogsOrchestrator
{
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<CommitSegmentWriterOutput, CommitSegmentWriterOperatorError>,
        ctx: &ComponentContext<Self>,
    ) {
        let message = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(message) => message,
            None => return,
        };

        // If the flusher received is a record segment flusher, get the number of keys for the blockfile and set it on the orchestrator
        if let ChromaSegmentFlusher::RecordSegment(record_segment_flusher) = &message.flusher {
            let collection_info = match self.context.get_output_collection_info_mut() {
                Ok(info) => info,
                Err(err) => {
                    self.terminate_with_result(Err(err.into()), ctx).await;
                    return;
                }
            };
            collection_info.collection.total_records_post_compaction =
                record_segment_flusher.count();
        }

        self.dispatch_segment_flush(message.flusher, ctx).await;
    }
}

#[async_trait]
impl Handler<TaskResult<FlushSegmentWriterOutput, FlushSegmentWriterOperatorError>>
    for ApplyLogsOrchestrator
{
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<FlushSegmentWriterOutput, FlushSegmentWriterOperatorError>,
        ctx: &ComponentContext<Self>,
    ) {
        let message = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(message) => message,
            None => return,
        };

        let segment_id = message.flush_info.segment_id;

        // Drops the span so that the end timestamp is accurate
        let _ = self.segment_spans.remove(&segment_id);

        self.flush_results.push(message.flush_info);
        self.num_uncompleted_tasks_by_segment.remove(&segment_id);

        if self.num_uncompleted_tasks_by_segment.is_empty() {
            self.finish_materialized_output(ctx).await;
        }
    }
}
