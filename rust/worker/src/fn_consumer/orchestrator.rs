use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_system::{
    ChannelError, ComponentContext, ComponentHandle, Dispatcher, Orchestrator,
    OrchestratorContext, PanicError,
};
use chroma_types::{AttachedFunctionUuid, CollectionUuid, LogRecord};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::oneshot::{error::RecvError, Sender};

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
    #[error("Channel error: {0}")]
    Channel(#[from] ChannelError),
    #[error("Recv error: {0}")]
    Recv(#[from] RecvError),
    #[error("Panic: {0}")]
    Panic(#[from] PanicError),
}

impl ChromaError for FnConsumerOrchestratorError {
    fn code(&self) -> ErrorCodes {
        match self {
            FnConsumerOrchestratorError::Sink(e) => e.code(),
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
    context: OrchestratorContext,
    result_channel:
        Option<Sender<Result<FnConsumerOrchestratorResponse, FnConsumerOrchestratorError>>>,
}

impl FnConsumerOrchestrator {
    pub fn new(
        fn_id: AttachedFunctionUuid,
        input_coll_id: CollectionUuid,
        start_offset: i64,
        sink: Arc<dyn RecordSink>,
        dispatcher: ComponentHandle<Dispatcher>,
    ) -> Self {
        Self {
            fn_id,
            input_coll_id,
            start_offset,
            sink,
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

    async fn on_start(&mut self, ctx: &ComponentContext<Self>) {
        // TODO(fn_consumer): read WAL[input_coll_id] starting at
        // start_offset, push batches through self.sink while advancing the
        // high-water mark, and report the final offset back via the
        // response. PR1 stubs this out: report start == end so the manager
        // wiring is fully exercisable.
        let new_completion_offset = self.start_offset;
        let work_id = WorkId {
            fn_id: self.fn_id,
            input_coll_id: self.input_coll_id,
            new_completion_offset,
        };
        let result = match self.sink.finish(work_id).await {
            Ok(()) => Ok(FnConsumerOrchestratorResponse {
                fn_id: self.fn_id,
                input_coll_id: self.input_coll_id,
                new_completion_offset,
            }),
            Err(e) => Err(e.into()),
        };
        self.terminate_with_result(result, ctx).await;
    }

    fn set_result_channel(&mut self, sender: Sender<Result<Self::Output, Self::Error>>) {
        self.result_channel = Some(sender);
    }

    fn take_result_channel(&mut self) -> Option<Sender<Result<Self::Output, Self::Error>>> {
        self.result_channel.take()
    }
}
