use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_system::{
    wrap, ChannelError, ComponentContext, ComponentHandle, Dispatcher, Handler, Orchestrator,
    OrchestratorContext, PanicError, TaskError, TaskMessage, TaskResult,
};
use chroma_types::{
    operator::{Limit, Select, Rank, RecordMeasure, Score, SearchPayloadResult},
    CollectionAndSegments,
};
use std::collections::HashMap;
use thiserror::Error;
use tokio::sync::oneshot::{error::RecvError, Sender};
use tracing::Span;

use crate::execution::operators::{
    fetch_log::FetchLogOutput,
    select::{SelectError, SelectInput, SelectOutput},
    score::{ScoreError, ScoreInput, ScoreOutput},
};

#[derive(Error, Debug)]
pub enum ScoreOrchestratorError {
    #[error("Operation aborted because resources exhausted")]
    Aborted,
    #[error("Error sending message through channel: {0}")]
    Channel(#[from] ChannelError),
    #[error("Panic: {0}")]
    Panic(#[from] PanicError),
    #[error("Error receiving final result: {0}")]
    Result(#[from] RecvError),
    #[error("Error running Score operator: {0}")]
    Score(#[from] ScoreError),
    #[error("Error running Select operator: {0}")]
    Select(#[from] SelectError),
}

impl ChromaError for ScoreOrchestratorError {
    fn code(&self) -> ErrorCodes {
        match self {
            ScoreOrchestratorError::Aborted => ErrorCodes::ResourceExhausted,
            ScoreOrchestratorError::Channel(err) => err.code(),
            ScoreOrchestratorError::Panic(_) => ErrorCodes::Aborted,
            ScoreOrchestratorError::Result(_) => ErrorCodes::Internal,
            ScoreOrchestratorError::Score(err) => err.code(),
            ScoreOrchestratorError::Select(err) => err.code(),
        }
    }
}

impl<E> From<TaskError<E>> for ScoreOrchestratorError
where
    E: Into<ScoreOrchestratorError>,
{
    fn from(value: TaskError<E>) -> Self {
        match value {
            TaskError::Aborted => ScoreOrchestratorError::Aborted,
            TaskError::Panic(e) => e.into(),
            TaskError::TaskFailed(e) => e.into(),
        }
    }
}

#[derive(Debug)]
pub struct ScoreOrchestratorOutput {
    pub result: SearchPayloadResult,
    pub pulled_log_bytes: u64,
}

/// The `ScoreOrchestrator` chains operators to evaluate scores, apply limits, and select fields
/// for search results from multiple KNN orchestrators.
///
/// # Pipeline
/// ```text
///   HashMap<Rank, Vec<RecordMeasure>>
///                  │
///                  ▼
///         ┌──────────────────┐
///         │  Score Operator  │
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
///        ScoreOrchestratorOutput
/// ```
#[derive(Debug)]
pub struct ScoreOrchestrator {
    // Orchestrator parameters
    context: OrchestratorContext,
    blockfile_provider: BlockfileProvider,
    queue: usize,

    // Input data
    ranks: HashMap<Rank, Vec<RecordMeasure>>,
    score: Score,
    limit: Limit,
    select: Select,

    // Collection information
    collection_and_segments: CollectionAndSegments,
    logs: FetchLogOutput,

    // Result channel
    result_channel: Option<Sender<Result<ScoreOrchestratorOutput, ScoreOrchestratorError>>>,
}

impl ScoreOrchestrator {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        blockfile_provider: BlockfileProvider,
        dispatcher: ComponentHandle<Dispatcher>,
        queue: usize,
        ranks: HashMap<Rank, Vec<RecordMeasure>>,
        score: Score,
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
            ranks,
            score,
            limit,
            select,
            collection_and_segments,
            logs,
            result_channel: None,
        }
    }
}

#[async_trait]
impl Orchestrator for ScoreOrchestrator {
    type Output = ScoreOrchestratorOutput;
    type Error = ScoreOrchestratorError;

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
        // Start with Score operator
        let score_task = wrap(
            Box::new(self.score.clone()),
            ScoreInput {
                blockfile_provider: self.blockfile_provider.clone(),
                ranks: self.ranks.clone(),
            },
            ctx.receiver(),
            self.context.task_cancellation_token.clone(),
        );
        vec![(score_task, Some(Span::current()))]
    }

    fn queue_size(&self) -> usize {
        self.queue
    }

    fn set_result_channel(
        &mut self,
        sender: Sender<Result<ScoreOrchestratorOutput, ScoreOrchestratorError>>,
    ) {
        self.result_channel = Some(sender);
    }

    fn take_result_channel(
        &mut self,
    ) -> Option<Sender<Result<ScoreOrchestratorOutput, ScoreOrchestratorError>>> {
        self.result_channel.take()
    }
}

#[async_trait]
impl Handler<TaskResult<ScoreOutput, ScoreError>> for ScoreOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<ScoreOutput, ScoreError>,
        ctx: &ComponentContext<Self>,
    ) {
        let output = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(output) => output,
            None => return,
        };

        // Apply limit directly on the sorted scores
        let skip = self.limit.skip as usize;
        let fetch = self.limit.fetch.unwrap_or(u32::MAX) as usize;

        let sliced_records = output
            .scores
            .into_iter()
            .skip(skip)
            .take(fetch)
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
impl Handler<TaskResult<SelectOutput, SelectError>> for ScoreOrchestrator {
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

        let result = ScoreOrchestratorOutput {
            result: output,
            pulled_log_bytes,
        };

        self.terminate_with_result(Ok(result), ctx).await;
    }
}
