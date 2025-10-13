use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_system::{
    wrap, ChannelError, ComponentContext, ComponentHandle, Dispatcher, Handler, Orchestrator,
    OrchestratorContext, PanicError, TaskError, TaskMessage, TaskResult,
};
use chroma_types::{
    operator::{Filter, GetResult, Limit, Projection, ProjectionOutput},
    CollectionAndSegments,
};
use opentelemetry::trace::TraceContextExt;
use thiserror::Error;
use tokio::sync::oneshot::{error::RecvError, Sender};
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::execution::operators::{
    fetch_log::{FetchLogError, FetchLogOperator, FetchLogOutput},
    filter::{FilterError, FilterInput, FilterOutput},
    limit::{LimitError, LimitInput, LimitOutput},
    prefetch_segment::{
        PrefetchSegmentError, PrefetchSegmentInput, PrefetchSegmentOperator, PrefetchSegmentOutput,
    },
    projection::{ProjectionError, ProjectionInput},
};

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

/// The `GetOrchestrator` chains a sequence of operators in sequence to evaluate
/// a `<collection>.get(...)` query from the user
///
/// # Pipeline
/// ```text
///       ┌────────────┐
///       │            │
///       │  on_start  │
///       │            │
///       └──────┬─────┘
///              │
///              ▼
///    ┌────────────────────┐
///    │                    │
///    │  FetchLogOperator  │
///    │                    │
///    └─────────┬──────────┘
///              │
///              ▼
///    ┌───────────────────┐
///    │                   │
///    │   FilterOperator  │
///    │                   │
///    └─────────┬─────────┘
///              │
///              ▼
///     ┌─────────────────┐
///     │                 │
///     │  LimitOperator  │
///     │                 │
///     └────────┬────────┘
///              │
///              ▼
///   ┌──────────────────────┐
///   │                      │
///   │  ProjectionOperator  │
///   │                      │
///   └──────────┬───────────┘
///              │
///              ▼
///     ┌──────────────────┐
///     │                  │
///     │  result_channel  │
///     │                  │
///     └──────────────────┘
/// ```
#[derive(Debug)]
pub struct GetOrchestrator {
    // Orchestrator parameters
    context: OrchestratorContext,
    queue: usize,
    blockfile_provider: BlockfileProvider,

    // Collection segments
    collection_and_segments: CollectionAndSegments,

    // Fetch logs
    fetch_log: FetchLogOperator,

    // Fetched logs
    fetched_logs: Option<FetchLogOutput>,

    // Pipelined operators
    filter: Filter,
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
        collection_and_segments: CollectionAndSegments,
        fetch_log: FetchLogOperator,
        filter: Filter,
        limit: Limit,
        projection: Projection,
    ) -> Self {
        let context = OrchestratorContext::new(dispatcher);
        Self {
            context,
            queue,
            blockfile_provider,
            collection_and_segments,
            fetch_log,
            fetched_logs: None,
            filter,
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
        self.context.dispatcher.clone()
    }

    fn context(&self) -> &OrchestratorContext {
        &self.context
    }

    async fn initial_tasks(
        &mut self,
        ctx: &ComponentContext<Self>,
    ) -> Vec<(TaskMessage, Option<Span>)> {
        let mut tasks = vec![];
        // prefetch record segment
        let prefetch_record_segment_task = wrap(
            Box::new(PrefetchSegmentOperator::new()),
            PrefetchSegmentInput::new(
                self.collection_and_segments.record_segment.clone(),
                self.blockfile_provider.clone(),
            ),
            ctx.receiver(),
            self.context.task_cancellation_token.clone(),
        );
        // Prefetch task is detached from the orchestrator
        let prefetch_span = tracing::info_span!(parent: None, "Prefetch record segment", segment_id = %self.collection_and_segments.record_segment.id);
        Span::current().add_link(prefetch_span.context().span().span_context().clone());
        tasks.push((prefetch_record_segment_task, Some(prefetch_span)));

        // Prefetch metadata segment.
        let prefetch_metadata_task = wrap(
            Box::new(PrefetchSegmentOperator::new()),
            PrefetchSegmentInput::new(
                self.collection_and_segments.metadata_segment.clone(),
                self.blockfile_provider.clone(),
            ),
            ctx.receiver(),
            self.context.task_cancellation_token.clone(),
        );
        let prefetch_span = tracing::info_span!(parent: None, "Prefetch metadata segment", segment_id = %self.collection_and_segments.metadata_segment.id);
        Span::current().add_link(prefetch_span.context().span().span_context().clone());
        tasks.push((prefetch_metadata_task, Some(prefetch_span.clone())));

        // Fetch log task.
        let fetch_log_task = wrap(
            Box::new(self.fetch_log.clone()),
            (),
            ctx.receiver(),
            self.context.task_cancellation_token.clone(),
        );
        tasks.push((fetch_log_task, Some(Span::current())));

        tasks
    }

    fn queue_size(&self) -> usize {
        self.queue
    }

    fn set_result_channel(&mut self, sender: Sender<Result<GetResult, GetError>>) {
        self.result_channel = Some(sender)
    }

    fn take_result_channel(&mut self) -> Option<Sender<Result<GetResult, GetError>>> {
        self.result_channel.take()
    }
}

#[async_trait]
impl Handler<TaskResult<PrefetchSegmentOutput, PrefetchSegmentError>> for GetOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        _: TaskResult<PrefetchSegmentOutput, PrefetchSegmentError>,
        _: &ComponentContext<GetOrchestrator>,
    ) {
        // Nothing to do.
    }
}

#[async_trait]
impl Handler<TaskResult<FetchLogOutput, FetchLogError>> for GetOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<FetchLogOutput, FetchLogError>,
        ctx: &ComponentContext<Self>,
    ) {
        let output = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(output) => output,
            None => return,
        };

        self.fetched_logs = Some(output.clone());

        let task = wrap(
            Box::new(self.filter.clone()),
            FilterInput {
                logs: output,
                blockfile_provider: self.blockfile_provider.clone(),
                metadata_segment: self.collection_and_segments.metadata_segment.clone(),
                record_segment: self.collection_and_segments.record_segment.clone(),
            },
            ctx.receiver(),
            self.context.task_cancellation_token.clone(),
        );
        self.send(task, ctx, Some(Span::current())).await;
    }
}

#[async_trait]
impl Handler<TaskResult<FilterOutput, FilterError>> for GetOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<FilterOutput, FilterError>,
        ctx: &ComponentContext<Self>,
    ) {
        let output = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(output) => output,
            None => return,
        };
        let task = wrap(
            Box::new(self.limit.clone()),
            LimitInput {
                logs: self
                    .fetched_logs
                    .as_ref()
                    .expect("FetchLogOperator should have finished already")
                    .clone(),
                blockfile_provider: self.blockfile_provider.clone(),
                record_segment: self.collection_and_segments.record_segment.clone(),
                log_offset_ids: output.log_offset_ids,
                compact_offset_ids: output.compact_offset_ids,
            },
            ctx.receiver(),
            self.context.task_cancellation_token.clone(),
        );
        self.send(task, ctx, Some(Span::current())).await;
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
            logs: self
                .fetched_logs
                .as_ref()
                .expect("FetchLogOperator should have finished already")
                .clone(),
            blockfile_provider: self.blockfile_provider.clone(),
            record_segment: self.collection_and_segments.record_segment.clone(),
            offset_ids: output.offset_ids.iter().collect(),
        };

        let task = wrap(
            Box::new(self.projection.clone()),
            input,
            ctx.receiver(),
            self.context.task_cancellation_token.clone(),
        );
        self.send(task, ctx, Some(Span::current())).await;
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
            .fetched_logs
            .as_ref()
            .expect("FetchLogOperator should have finished already")
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
