use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_system::{
    wrap, ChannelError, ComponentContext, ComponentHandle, Dispatcher, Handler, Orchestrator,
    PanicError, TaskError, TaskMessage, TaskResult,
};
use chroma_types::operator::{GetResult, Limit, Projection, ProjectionOutput};
use thiserror::Error;
use tokio::sync::oneshot::{error::RecvError, Sender};
use tracing::Span;

use crate::execution::operators::{
    fetch_log::FetchLogError,
    filter::FilterError,
    limit::{LimitError, LimitInput, LimitOutput},
    prefetch_record::{PrefetchRecordError, PrefetchRecordOperator, PrefetchRecordOutput},
    projection::{ProjectionError, ProjectionInput},
};

use super::filter::FilterOrchestratorOutput;

#[derive(Error, Debug)]
pub enum GetError {
    #[error("Error sending message through channel: {0}")]
    Channel(#[from] ChannelError),
    #[error("Error running Fetch Log Operator: {0}")]
    FetchLog(#[from] FetchLogError),
    #[error("Error running Filter Operator: {0}")]
    Filter(#[from] FilterError),
    #[error("Error running Limit Operator: {0}")]
    Limit(#[from] LimitError),
    #[error("Panic: {0}")]
    Panic(#[from] PanicError),
    #[error("Error running Projection Operator: {0}")]
    Projection(#[from] ProjectionError),
    #[error("Error receiving final result: {0}")]
    Result(#[from] RecvError),
    #[error("Operation aborted because resources exhausted")]
    Aborted,
}

impl ChromaError for GetError {
    fn code(&self) -> ErrorCodes {
        match self {
            GetError::Channel(e) => e.code(),
            GetError::FetchLog(e) => e.code(),
            GetError::Filter(e) => e.code(),
            GetError::Limit(e) => e.code(),
            GetError::Panic(_) => ErrorCodes::Aborted,
            GetError::Projection(e) => e.code(),
            GetError::Result(_) => ErrorCodes::Internal,
            GetError::Aborted => ErrorCodes::ResourceExhausted,
        }
    }
}

impl<E> From<TaskError<E>> for GetError
where
    E: Into<GetError>,
{
    fn from(value: TaskError<E>) -> Self {
        match value {
            TaskError::Panic(e) => e.into(),
            TaskError::TaskFailed(e) => e.into(),
            TaskError::Aborted => GetError::Aborted,
        }
    }
}

/// The `GetOrchestrator` chains a sequence of operators in sequence to get data for user.
/// When used together with `FilterOrchestrator`, they evaluate a `<collection>.get(...)` query
///
/// # Pipeline
/// ```text
///              │
///              │
///              ▼
///  ┌───────────────────────┐
///  │                       │
///  │   FilterOrchestrator  │
///  │                       │
///  └───────────┬───────────┘
///              │
/// ┌──────────  │  ────────────┐
/// │            │     Get      │
/// │            │ Orchestrator │
/// │            ▼              │
/// │   ┌─────────────────┐     │
/// │   │                 │     │
/// │   │  LimitOperator  │     │
/// │   │                 │     │
/// │   └────────┬────────┘     │
/// │            │              │
/// │            ▼              │
/// │ ┌──────────────────────┐  │
/// │ │                      │  │
/// │ │  ProjectionOperator  │  │
/// │ │                      │  │
/// │ └──────────┬───────────┘  │
/// │            │              │
/// │            ▼              │
/// │   ┌──────────────────┐    │
/// │   │                  │    │
/// │   │  result_channel  │    │
/// │   │                  │    │
/// │   └──────────────────┘    │
/// │            │              │
/// └──────────  │  ────────────┘
///              ▼
///
/// ```
#[derive(Debug)]
pub struct GetOrchestrator {
    // Orchestrator parameters
    blockfile_provider: BlockfileProvider,
    dispatcher: ComponentHandle<Dispatcher>,
    queue: usize,

    // Output from FilterOrchestrator
    filter_output: FilterOrchestratorOutput,

    // Pipelined operators
    limit: Limit,
    projection: Projection,

    // Result channel
    result_channel: Option<Sender<Result<GetResult, GetError>>>,
}

impl GetOrchestrator {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        blockfile_provider: BlockfileProvider,
        dispatcher: ComponentHandle<Dispatcher>,
        queue: usize,
        filter_output: FilterOrchestratorOutput,
        limit: Limit,
        projection: Projection,
    ) -> Self {
        Self {
            blockfile_provider,
            dispatcher,
            queue,
            filter_output,
            limit,
            projection,
            result_channel: None,
        }
    }
}

#[async_trait]
impl Orchestrator for GetOrchestrator {
    type Output = GetResult;
    type Error = GetError;

    fn dispatcher(&self) -> ComponentHandle<Dispatcher> {
        self.dispatcher.clone()
    }

    async fn initial_tasks(
        &mut self,
        ctx: &ComponentContext<Self>,
    ) -> Vec<(TaskMessage, Option<Span>)> {
        vec![(
            wrap(
                Box::new(self.limit.clone()),
                LimitInput {
                    logs: self.filter_output.logs.clone(),
                    blockfile_provider: self.blockfile_provider.clone(),
                    record_segment: self.filter_output.record_segment.clone(),
                    log_offset_ids: self.filter_output.filter_output.log_offset_ids.clone(),
                    compact_offset_ids: self.filter_output.filter_output.compact_offset_ids.clone(),
                },
                ctx.receiver(),
            ),
            Some(Span::current()),
        )]
    }

    fn queue_size(&self) -> usize {
        self.queue
    }

    fn set_result_channel(&mut self, sender: Sender<Result<GetResult, GetError>>) {
        self.result_channel = Some(sender)
    }

    fn take_result_channel(&mut self) -> Sender<Result<GetResult, GetError>> {
        self.result_channel
            .take()
            .expect("The result channel should be set before take")
    }
}

#[async_trait]
impl Handler<TaskResult<LimitOutput, LimitError>> for GetOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<LimitOutput, LimitError>,
        ctx: &ComponentContext<Self>,
    ) {
        let output = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(output) => output,
            None => return,
        };

        let input = ProjectionInput {
            logs: self.filter_output.logs.clone(),
            blockfile_provider: self.blockfile_provider.clone(),
            record_segment: self.filter_output.record_segment.clone(),
            offset_ids: output.offset_ids.iter().collect(),
        };

        // Prefetch records before projection
        let prefetch_task = wrap(
            Box::new(PrefetchRecordOperator {}),
            input.clone(),
            ctx.receiver(),
        );

        if !self.send(prefetch_task, ctx, Some(Span::current())).await {
            return;
        }

        let task = wrap(Box::new(self.projection.clone()), input, ctx.receiver());
        self.send(task, ctx, Some(Span::current())).await;
    }
}

#[async_trait]
impl Handler<TaskResult<PrefetchRecordOutput, PrefetchRecordError>> for GetOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        _message: TaskResult<PrefetchRecordOutput, PrefetchRecordError>,
        _ctx: &ComponentContext<Self>,
    ) {
        // The output and error from `PrefetchRecordOperator` are ignored
    }
}

#[async_trait]
impl Handler<TaskResult<ProjectionOutput, ProjectionError>> for GetOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<ProjectionOutput, ProjectionError>,
        ctx: &ComponentContext<Self>,
    ) {
        let output = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(output) => output,
            None => return,
        };

        let pulled_log_bytes = self
            .filter_output
            .logs
            .iter()
            .map(|(l, _)| l.size_bytes())
            .sum();

        self.terminate_with_result(
            Ok(GetResult {
                pulled_log_bytes,
                result: output,
            }),
            ctx,
        )
        .await;
    }
}
