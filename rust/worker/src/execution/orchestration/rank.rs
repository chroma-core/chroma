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

use crate::execution::{
    operators::{
        limit::{LimitError, LimitInput, LimitOutput},
        rank::{RankError, RankInput, RankOutput},
        select::{SelectError, SelectInput, SelectOutput},
    },
    orchestration::knn_filter::KnnFilterOutput,
};

#[derive(Error, Debug)]
pub enum RankOrchestratorError {
    #[error("Operation aborted because resources exhausted")]
    Aborted,
    #[error("Error sending message through channel: {0}")]
    Channel(#[from] ChannelError),
    #[error("Error running Limit Operator: {0}")]
    Limit(#[from] LimitError),
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
            RankOrchestratorError::Limit(e) => e.code(),
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

#[derive(Debug, Default)]
pub struct RankOrchestratorOutput {
    pub result: SearchPayloadResult,
    pub pulled_log_bytes: u64,
}

/// The `RankOrchestrator` chains operators to evaluate ranks, apply limits, and select keys
/// for search results from multiple KNN orchestrators.
///
/// # Pipeline
///
/// When rank expression is provided:
/// ```text
///   Vec<Vec<RecordMeasure>>
///            │
///            ▼
///   ┌──────────────────┐
///   │  Rank Operator   │
///   └────────┬─────────┘
///            │
///            ▼
///   ┌──────────────────┐
///   │  Slice by Limit  │
///   └────────┬─────────┘
///            │
///            ▼
///   ┌──────────────────┐
///   │ Select Operator  │
///   └────────┬─────────┘
///            │
///            ▼
///   RankOrchestratorOutput
/// ```
///
/// When rank expression is None:
/// ```text
///     KnnFilterOutput
///            │
///            ▼
///   ┌──────────────────┐
///   │  Limit Operator  │
///   └────────┬─────────┘
///            │
///            ▼
///   ┌──────────────────┐
///   │ Select Operator  │
///   └────────┬─────────┘
///            │
///            ▼
///   RankOrchestratorOutput
/// ```
#[derive(Debug)]
pub struct RankOrchestrator {
    // Orchestrator parameters
    context: OrchestratorContext,
    blockfile_provider: BlockfileProvider,
    queue: usize,

    // Input data
    knn_filter_output: KnnFilterOutput,
    knn_results: Vec<Vec<RecordMeasure>>,
    rank: Rank,
    limit: Limit,
    select: Select,

    // Collection information
    collection_and_segments: CollectionAndSegments,

    // Result channel
    result_channel: Option<Sender<Result<RankOrchestratorOutput, RankOrchestratorError>>>,
}

impl RankOrchestrator {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        blockfile_provider: BlockfileProvider,
        dispatcher: ComponentHandle<Dispatcher>,
        queue: usize,
        knn_filter_output: KnnFilterOutput,
        knn_results: Vec<Vec<RecordMeasure>>,
        rank: Rank,
        limit: Limit,
        select: Select,
        collection_and_segments: CollectionAndSegments,
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
            knn_filter_output,
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
        // If a rank expression is provided, start with the Rank operator
        // Otherwise, start the Limit operator which implicitly rank by internal offset
        let task = match self.rank.clone().expr {
            Some(expr) => wrap(
                Box::new(expr),
                RankInput {
                    knn_results: self.knn_results.clone(),
                },
                ctx.receiver(),
                self.context.task_cancellation_token.clone(),
            ),
            None => wrap(
                Box::new(self.limit.clone()),
                LimitInput {
                    logs: self.knn_filter_output.logs.clone(),
                    blockfile_provider: self.blockfile_provider.clone(),
                    record_segment: self.collection_and_segments.record_segment.clone(),
                    log_offset_ids: self.knn_filter_output.filter_output.log_offset_ids.clone(),
                    compact_offset_ids: self
                        .knn_filter_output
                        .filter_output
                        .compact_offset_ids
                        .clone(),
                },
                ctx.receiver(),
                self.context.task_cancellation_token.clone(),
            ),
        };
        vec![(task, Some(Span::current()))]
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
impl Handler<TaskResult<LimitOutput, LimitError>> for RankOrchestrator {
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

        let task = wrap(
            Box::new(self.select.clone()),
            SelectInput {
                records: output
                    .offset_ids
                    .iter()
                    .enumerate()
                    .map(|(rank_position, offset_id)| RecordMeasure {
                        offset_id,
                        measure: rank_position as f32,
                    })
                    .collect(),
                logs: self.knn_filter_output.logs.clone(),
                blockfile_provider: self.blockfile_provider.clone(),
                record_segment: self.collection_and_segments.record_segment.clone(),
            },
            ctx.receiver(),
            self.context.task_cancellation_token.clone(),
        );

        self.send(task, ctx, Some(Span::current())).await;
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

        // Apply limit (offset and limit) directly on the ranked results
        // This slices the ranked records instead of using the Limit operator
        let offset = self.limit.offset as usize;
        let limit = self.limit.limit.unwrap_or(u32::MAX) as usize;

        let sliced_records = output
            .ranks
            .into_iter()
            .skip(offset)
            .take(limit)
            .collect::<Vec<_>>();

        let task = wrap(
            Box::new(self.select.clone()),
            SelectInput {
                records: sliced_records,
                logs: self.knn_filter_output.logs.clone(),
                blockfile_provider: self.blockfile_provider.clone(),
                record_segment: self.collection_and_segments.record_segment.clone(),
            },
            ctx.receiver(),
            self.context.task_cancellation_token.clone(),
        );

        self.send(task, ctx, Some(Span::current())).await;
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
        let pulled_log_bytes = self
            .knn_filter_output
            .logs
            .iter()
            .map(|(l, _)| l.size_bytes())
            .sum();

        let result = RankOrchestratorOutput {
            result: output,
            pulled_log_bytes,
        };

        self.terminate_with_result(Ok(result), ctx).await;
    }
}
