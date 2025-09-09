use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_system::{
    wrap, ChannelError, ComponentContext, ComponentHandle, Dispatcher, Handler, Orchestrator,
    OrchestratorContext, PanicError, TaskError, TaskMessage, TaskResult,
};
use chroma_types::{
    operator::{Limit, Rank, RecordMeasure, SearchPayloadResult, Select},
    CollectionAndSegments,
};
use thiserror::Error;
use tokio::sync::oneshot::{error::RecvError, Sender};
use tracing::Span;

use crate::execution::operators::{
    fetch_log::FetchLogOutput,
    rank::{RankError, RankInput, RankOutput},
    select::{SelectError, SelectInput, SelectOutput},
};

#[derive(Error, Debug)]
pub enum RankOrchestratorError {
    #[error("Operation aborted because resources exhausted")]
    Aborted,
    #[error("Error sending message through channel: {0}")]
    Channel(#[from] ChannelError),
    #[error("Panic: {0}")]
    Panic(#[from] PanicError),
    #[error("Error receiving final result: {0}")]
    Result(#[from] RecvError),
    #[error("Error running Rank operator: {0}")]
    Rank(#[from] RankError),
    #[error("Error running Select operator: {0}")]
    Select(#[from] SelectError),
}

impl ChromaError for RankOrchestratorError {
    fn code(&self) -> ErrorCodes {
        match self {
            RankOrchestratorError::Aborted => ErrorCodes::ResourceExhausted,
            RankOrchestratorError::Channel(err) => err.code(),
            RankOrchestratorError::Panic(_) => ErrorCodes::Aborted,
            RankOrchestratorError::Result(_) => ErrorCodes::Internal,
            RankOrchestratorError::Rank(err) => err.code(),
            RankOrchestratorError::Select(err) => err.code(),
        }
    }
}

impl<E> From<TaskError<E>> for RankOrchestratorError
where
    E: Into<RankOrchestratorError>,
{
    fn from(value: TaskError<E>) -> Self {
        match value {
            TaskError::Aborted => RankOrchestratorError::Aborted,
            TaskError::Panic(e) => e.into(),
            TaskError::TaskFailed(e) => e.into(),
        }
    }
}

#[derive(Debug)]
pub struct RankOrchestratorOutput {
    pub result: SearchPayloadResult,
    pub pulled_log_bytes: u64,
}

/// The `RankOrchestrator` chains operators to evaluate ranks, apply limits, and select fields
/// for search results from multiple KNN orchestrators.
///
/// # Pipeline
/// ```text
///   HashMap<Rank, Vec<RecordMeasure>>
///                  │
///                  ▼
///         ┌──────────────────┐
///         │  Rank Operator   │
///         └────────┬─────────┘
///                  │
///                  ▼
///         ┌──────────────────┐
///         │  Slice by Limit  │
///         └────────┬─────────┘
///                  │
///                  ▼
///         ┌──────────────────┐
///         │ Select Operator  │
///         └────────┬─────────┘
///                  │
///                  ▼
///        RankOrchestratorOutput
/// ```
#[derive(Debug)]
pub struct RankOrchestrator {
    // Orchestrator parameters
    context: OrchestratorContext,
    blockfile_provider: BlockfileProvider,
    queue: usize,

    // Input data
    knn_results: Vec<Vec<RecordMeasure>>,
    rank: Rank,
    limit: Limit,
    select: Select,

    // Collection information
    collection_and_segments: CollectionAndSegments,
    logs: FetchLogOutput,

    // Result channel
    result_channel: Option<Sender<Result<RankOrchestratorOutput, RankOrchestratorError>>>,
}

impl RankOrchestrator {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        blockfile_provider: BlockfileProvider,
        dispatcher: ComponentHandle<Dispatcher>,
        queue: usize,
        knn_results: Vec<Vec<RecordMeasure>>,
        rank: Rank,
        limit: Limit,
        select: Select,
        collection_and_segments: CollectionAndSegments,
        logs: FetchLogOutput,
    ) -> Self {
        let context = OrchestratorContext::new(dispatcher);
        Self {
            context,
            blockfile_provider,
            queue,
            knn_results,
            rank,
            limit,
            select,
            collection_and_segments,
            logs,
            result_channel: None,
        }
    }
}

#[async_trait]
impl Orchestrator for RankOrchestrator {
    type Output = RankOrchestratorOutput;
    type Error = RankOrchestratorError;

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
        // Start with Rank operator
        let rank_task = wrap(
            Box::new(self.rank.clone()),
            RankInput {
                knn_results: self.knn_results.clone(),
            },
            ctx.receiver(),
            self.context.task_cancellation_token.clone(),
        );
        vec![(rank_task, Some(Span::current()))]
    }

    fn queue_size(&self) -> usize {
        self.queue
    }

    fn set_result_channel(
        &mut self,
        sender: Sender<Result<RankOrchestratorOutput, RankOrchestratorError>>,
    ) {
        self.result_channel = Some(sender);
    }

    fn take_result_channel(
        &mut self,
    ) -> Option<Sender<Result<RankOrchestratorOutput, RankOrchestratorError>>> {
        self.result_channel.take()
    }
}

#[async_trait]
impl Handler<TaskResult<RankOutput, RankError>> for RankOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<RankOutput, RankError>,
        ctx: &ComponentContext<Self>,
    ) {
        let output = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(output) => output,
            None => return,
        };

        // Apply limit directly on the sorted ranks
        let offset = self.limit.offset as usize;
        let limit = self.limit.limit.unwrap_or(u32::MAX) as usize;

        let sliced_records = output
            .ranks
            .into_iter()
            .skip(offset)
            .take(limit)
            .collect::<Vec<_>>();

        // Create and dispatch Select operator
        let select_task = wrap(
            Box::new(self.select.clone()),
            SelectInput {
                records: sliced_records,
                logs: self.logs.clone(),
                blockfile_provider: self.blockfile_provider.clone(),
                record_segment: self.collection_and_segments.record_segment.clone(),
            },
            ctx.receiver(),
            self.context.task_cancellation_token.clone(),
        );

        self.send(select_task, ctx, Some(Span::current())).await;
    }
}

#[async_trait]
impl Handler<TaskResult<SelectOutput, SelectError>> for RankOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<SelectOutput, SelectError>,
        ctx: &ComponentContext<Self>,
    ) {
        let output = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(output) => output,
            None => return,
        };

        // Terminate with final result
        let pulled_log_bytes = self.logs.iter().map(|(l, _)| l.size_bytes()).sum();

        let result = RankOrchestratorOutput {
            result: output,
            pulled_log_bytes,
        };

        self.terminate_with_result(Ok(result), ctx).await;
    }
}
