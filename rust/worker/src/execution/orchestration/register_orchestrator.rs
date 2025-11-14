use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_system::{
    wrap, ChannelError, ComponentContext, ComponentHandle, Dispatcher, Handler, Orchestrator,
    OrchestratorContext, TaskMessage, TaskResult,
};
use chroma_system::{PanicError, TaskError};
use chroma_types::{JobId, SegmentFlushInfo};
use thiserror::Error;
use tokio::sync::oneshot::error::RecvError;
use tokio::sync::oneshot::Sender;
use tracing::Span;

use crate::execution::operators::register::{
    RegisterError, RegisterInput, RegisterOperator, RegisterOutput,
};
use crate::execution::orchestration::compact::CompactionContextError;

use super::compact::{CompactionContext, ExecutionState};

#[derive(Debug)]
pub struct RegisterOrchestrator {
    pub context: CompactionContext,
    dispatcher: ComponentHandle<Dispatcher>,
    result_channel: Option<Sender<Result<RegisterOrchestratorResponse, RegisterOrchestratorError>>>,
    _state: ExecutionState,
    flush_results: Vec<SegmentFlushInfo>,
    collection_logical_size_bytes: u64,
}

#[derive(Debug)]
pub struct RegisterOrchestratorResponse {
    pub job_id: JobId,
}

impl RegisterOrchestratorResponse {
    pub fn new(job_id: JobId) -> Self {
        Self { job_id }
    }
}

#[derive(Error, Debug)]
pub enum RegisterOrchestratorError {
    #[error("Operation aborted because resources exhausted")]
    Aborted,
    #[error("Error sending message through channel: {0}")]
    Channel(#[from] ChannelError),
    #[error("Error in compaction context: {0}")]
    CompactionContext(#[from] CompactionContextError),
    #[error("Invariant violation: {}", .0)]
    InvariantViolation(&'static str),
    #[error("Panic during compaction: {0}")]
    Panic(#[from] PanicError),
    #[error("Error receiving: {0}")]
    RecvError(#[from] RecvError),
    #[error("Error registering compaction result: {0}")]
    Register(#[from] RegisterError),
}

impl ChromaError for RegisterOrchestratorError {
    fn code(&self) -> ErrorCodes {
        match self {
            RegisterOrchestratorError::Aborted => ErrorCodes::Aborted,
            _ => ErrorCodes::Internal,
        }
    }

    fn should_trace_error(&self) -> bool {
        match self {
            RegisterOrchestratorError::Aborted => true,
            RegisterOrchestratorError::Channel(e) => e.should_trace_error(),
            RegisterOrchestratorError::CompactionContext(e) => e.should_trace_error(),
            RegisterOrchestratorError::InvariantViolation(_) => true,
            RegisterOrchestratorError::Panic(e) => e.should_trace_error(),
            RegisterOrchestratorError::Register(e) => e.should_trace_error(),
            RegisterOrchestratorError::RecvError(_) => true,
        }
    }
}

impl<E> From<TaskError<E>> for RegisterOrchestratorError
where
    E: Into<RegisterOrchestratorError>,
{
    fn from(value: TaskError<E>) -> Self {
        match value {
            TaskError::Aborted => RegisterOrchestratorError::Aborted,
            TaskError::Panic(e) => e.into(),
            TaskError::TaskFailed(e) => e.into(),
        }
    }
}

impl RegisterOrchestrator {
    pub fn new(
        context: &CompactionContext,
        dispatcher: ComponentHandle<Dispatcher>,
        flush_results: Vec<SegmentFlushInfo>,
        collection_logical_size_bytes: u64,
    ) -> Self {
        RegisterOrchestrator {
            context: context.clone(),
            dispatcher,
            result_channel: None,
            _state: ExecutionState::Register,
            flush_results,
            collection_logical_size_bytes,
        }
    }
}

#[async_trait]
impl Orchestrator for RegisterOrchestrator {
    type Output = RegisterOrchestratorResponse;
    type Error = RegisterOrchestratorError;

    fn dispatcher(&self) -> ComponentHandle<Dispatcher> {
        self.dispatcher.clone()
    }

    fn context(&self) -> &OrchestratorContext {
        &self.context.orchestrator_context
    }

    fn set_result_channel(&mut self, sender: Sender<Result<Self::Output, Self::Error>>) {
        self.result_channel = Some(sender)
    }

    fn take_result_channel(&mut self) -> Option<Sender<Result<Self::Output, Self::Error>>> {
        self.result_channel.take()
    }

    async fn initial_tasks(
        &mut self,
        ctx: &ComponentContext<Self>,
    ) -> Vec<(TaskMessage, Option<Span>)> {
        // Check if collection is set before proceeding
        let collection_info = match self.context.get_collection_info() {
            Ok(collection_info) => collection_info,
            Err(e) => {
                self.terminate_with_result(Err(e.into()), ctx).await;
                return vec![];
            }
        };

        vec![(
            wrap(
                RegisterOperator::new(),
                RegisterInput::new(
                    collection_info.collection.tenant.clone(),
                    collection_info.collection_id,
                    collection_info.pulled_log_offset,
                    collection_info.collection.version,
                    self.flush_results.clone().into(),
                    collection_info.collection.total_records_post_compaction,
                    self.collection_logical_size_bytes,
                    self.context.sysdb.clone(),
                    self.context.log.clone(),
                    collection_info.schema.clone(),
                ),
                ctx.receiver(),
                self.context
                    .orchestrator_context
                    .task_cancellation_token
                    .clone(),
            ),
            Some(Span::current()),
        )]
    }
}

#[async_trait]
impl Handler<TaskResult<RegisterOutput, RegisterError>> for RegisterOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<RegisterOutput, RegisterError>,
        ctx: &ComponentContext<Self>,
    ) {
        let collection_info = match self.context.get_collection_info() {
            Ok(collection_info) => collection_info,
            Err(e) => {
                self.terminate_with_result(Err(e.into()), ctx).await;
                return;
            }
        };

        self.terminate_with_result(
            message
                .into_inner()
                .map_err(|e| e.into())
                .map(|_| RegisterOrchestratorResponse::new(collection_info.collection_id.into())),
            ctx,
        )
        .await;
    }
}
