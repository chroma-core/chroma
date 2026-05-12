use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_log::Log;
use chroma_system::{
    wrap, ChannelError, ComponentContext, ComponentHandle, Dispatcher, Handler, Orchestrator,
    OrchestratorContext, PanicError, TaskError, TaskMessage, TaskResult,
};
use chroma_types::{AttachedFunctionUuid, CollectionUuid, DatabaseName, LogRecord};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::oneshot::{error::RecvError, Sender};
use tracing::Span;

use crate::execution::operators::fetch_log::{FetchLogError, FetchLogOperator, FetchLogOutput};

/// Identifier handed to the sink with every push and finish. The
/// `new_completion_offset` is the high-water mark up to which records have
/// been streamed; the downstream echoes the same tuple back on `FinishWork`
/// so the fn_consumer can forward it 1:1 to the work queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WorkId {
    pub fn_id: AttachedFunctionUuid,
    pub input_coll_id: CollectionUuid,
    pub new_completion_offset: i64,
}

/// Sink to which the orchestrator pushes WAL records pulled for a work item.
/// v1 implementation is a no-op; the real transport is TBD.
#[async_trait]
pub trait RecordSink: Send + Sync + std::fmt::Debug + 'static {
    async fn push(&self, work_id: WorkId, records: Vec<LogRecord>) -> Result<(), SinkError>;
    async fn finish(&self, work_id: WorkId) -> Result<(), SinkError>;
}

#[derive(Debug, Error)]
pub enum SinkError {
    #[error("Sink push failed: {0}")]
    Push(String),
}

impl ChromaError for SinkError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::Internal
    }
}

#[derive(Debug, Default)]
pub struct NoopSink;

#[async_trait]
impl RecordSink for NoopSink {
    async fn push(&self, _work_id: WorkId, _records: Vec<LogRecord>) -> Result<(), SinkError> {
        Ok(())
    }
    async fn finish(&self, _work_id: WorkId) -> Result<(), SinkError> {
        Ok(())
    }
}

#[derive(Debug)]
pub struct FnConsumerOrchestratorResponse {
    pub fn_id: AttachedFunctionUuid,
    pub input_coll_id: CollectionUuid,
    pub new_completion_offset: i64,
}

#[derive(Debug, Error)]
pub enum FnConsumerOrchestratorError {
    #[error("Sink error: {0}")]
    Sink(#[from] SinkError),
    #[error("Failed to fetch log records: {0}")]
    FetchLog(#[from] FetchLogError),
    #[error("Operation aborted because resources exhausted")]
    Aborted,
    #[error("Channel error: {0}")]
    Channel(#[from] ChannelError),
    #[error("Recv error: {0}")]
    Recv(#[from] RecvError),
    #[error("Panic: {0}")]
    Panic(#[from] PanicError),
}

impl<E> From<TaskError<E>> for FnConsumerOrchestratorError
where
    E: Into<FnConsumerOrchestratorError>,
{
    fn from(value: TaskError<E>) -> Self {
        match value {
            TaskError::Panic(e) => e.into(),
            TaskError::TaskFailed(e) => e.into(),
            TaskError::Aborted => FnConsumerOrchestratorError::Aborted,
        }
    }
}

impl ChromaError for FnConsumerOrchestratorError {
    fn code(&self) -> ErrorCodes {
        match self {
            FnConsumerOrchestratorError::Sink(e) => e.code(),
            FnConsumerOrchestratorError::FetchLog(e) => e.code(),
            FnConsumerOrchestratorError::Aborted => ErrorCodes::ResourceExhausted,
            FnConsumerOrchestratorError::Channel(_) => ErrorCodes::Internal,
            FnConsumerOrchestratorError::Recv(_) => ErrorCodes::Internal,
            FnConsumerOrchestratorError::Panic(_) => ErrorCodes::Internal,
        }
    }
}

#[derive(Debug)]
pub struct FnConsumerOrchestrator {
    fn_id: AttachedFunctionUuid,
    input_coll_id: CollectionUuid,
    start_offset: i64,
    sink: Arc<dyn RecordSink>,
    log: Log,
    tenant: String,
    database_name: DatabaseName,
    fetch_log_batch_size: u32,
    fetch_log_concurrency: usize,
    fetch_log_max_count: u32,
    context: OrchestratorContext,
    result_channel:
        Option<Sender<Result<FnConsumerOrchestratorResponse, FnConsumerOrchestratorError>>>,
}

impl FnConsumerOrchestrator {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        fn_id: AttachedFunctionUuid,
        input_coll_id: CollectionUuid,
        start_offset: i64,
        sink: Arc<dyn RecordSink>,
        log: Log,
        tenant: String,
        database_name: DatabaseName,
        fetch_log_batch_size: u32,
        fetch_log_concurrency: usize,
        fetch_log_max_count: u32,
        dispatcher: ComponentHandle<Dispatcher>,
    ) -> Self {
        Self {
            fn_id,
            input_coll_id,
            start_offset,
            sink,
            log,
            tenant,
            database_name,
            fetch_log_batch_size,
            fetch_log_concurrency,
            fetch_log_max_count,
            context: OrchestratorContext::new(dispatcher),
            result_channel: None,
        }
    }
}

#[async_trait]
impl Orchestrator for FnConsumerOrchestrator {
    type Output = FnConsumerOrchestratorResponse;
    type Error = FnConsumerOrchestratorError;

    fn dispatcher(&self) -> ComponentHandle<Dispatcher> {
        self.context.dispatcher.clone()
    }

    fn context(&self) -> &OrchestratorContext {
        &self.context
    }

    async fn initial_tasks(
        &mut self,
        ctx: &ComponentContext<Self>,
    ) -> Vec<(TaskMessage, Option<Span>)> {
        let fetch_log_task = wrap(
            Box::new(FetchLogOperator {
                log_client: self.log.clone(),
                batch_size: self.fetch_log_batch_size,
                start_log_offset_id: self.start_offset.max(0) as u64,
                maximum_fetch_count: Some(self.fetch_log_max_count),
                collection_uuid: self.input_coll_id,
                tenant: self.tenant.clone(),
                database_name: self.database_name.clone(),
                fetch_log_concurrency: self.fetch_log_concurrency,
                fragment_fetcher: None,
                log_upper_bound_offset: 0,
            }),
            (),
            ctx.receiver(),
            self.context.task_cancellation_token.clone(),
        );
        vec![(fetch_log_task, Some(Span::current()))]
    }

    fn set_result_channel(&mut self, sender: Sender<Result<Self::Output, Self::Error>>) {
        self.result_channel = Some(sender);
    }

    fn take_result_channel(&mut self) -> Option<Sender<Result<Self::Output, Self::Error>>> {
        self.result_channel.take()
    }
}

#[async_trait]
impl Handler<TaskResult<FetchLogOutput, FetchLogError>> for FnConsumerOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<FetchLogOutput, FetchLogError>,
        ctx: &ComponentContext<Self>,
    ) {
        let chunk = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(chunk) => chunk,
            None => return,
        };

        let new_completion_offset = chunk
            .iter()
            .map(|(record, _)| record.log_offset)
            .max()
            .map(|m| m + 1)
            .unwrap_or(self.start_offset);

        let work_id = WorkId {
            fn_id: self.fn_id,
            input_coll_id: self.input_coll_id,
            new_completion_offset,
        };

        let records: Vec<LogRecord> = chunk.iter().map(|(record, _)| record.clone()).collect();
        if let Some(()) = self.ok_or_terminate(self.sink.push(work_id, records).await, ctx).await {
            if let Some(()) = self.ok_or_terminate(self.sink.finish(work_id).await, ctx).await {
                self.terminate_with_result(
                    Ok(FnConsumerOrchestratorResponse {
                        fn_id: self.fn_id,
                        input_coll_id: self.input_coll_id,
                        new_completion_offset,
                    }),
                    ctx,
                )
                .await;
            }
        }
    }
}
