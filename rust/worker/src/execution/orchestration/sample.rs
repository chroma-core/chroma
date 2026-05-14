use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_segment::{bloom_filter::BloomFilterManager, spann_provider::SpannProvider};
use chroma_system::{
    wrap, ChannelError, ComponentContext, ComponentHandle, Dispatcher, Handler, Orchestrator,
    OrchestratorContext, PanicError, TaskError, TaskMessage, TaskResult,
};
use chroma_types::{
    operator::{Filter, Projection, ProjectionOutput, Sample, SampleResult},
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
    filter_logs_for_shard::{
        FilterLogsForShardError, FilterLogsForShardOperator, FilterLogsForShardOutput,
    },
    prefetch_segment::{
        PrefetchSegmentError, PrefetchSegmentInput, PrefetchSegmentOperator, PrefetchSegmentOutput,
    },
    projection::{ProjectionError, ProjectionInput},
    sample::{SampleError, SampleInput, SampleOutput},
};

#[derive(Error, Debug)]
pub enum SampleOrchestratorError {
    #[error("Error sending message through channel: {0}")]
    Channel(#[from] ChannelError),
    #[error("Error running Fetch Log Operator: {0}")]
    FetchLog(#[from] FetchLogError),
    #[error("Error running Filter Operator: {0}")]
    Filter(#[from] FilterError),
    #[error("Error partitioning logs to shard: {0}")]
    FilterLogsForShard(#[from] FilterLogsForShardError),
    #[error("Panic: {0}")]
    Panic(#[from] PanicError),
    #[error("Error running Projection Operator: {0}")]
    Projection(#[from] ProjectionError),
    #[error("Error receiving final result: {0}")]
    Result(#[from] RecvError),
    #[error("Error running Sample Operator: {0}")]
    Sample(#[from] SampleError),
    #[error("Operation aborted because resources exhausted")]
    Aborted,
}

impl ChromaError for SampleOrchestratorError {
    fn code(&self) -> ErrorCodes {
        match self {
            SampleOrchestratorError::Channel(e) => e.code(),
            SampleOrchestratorError::FetchLog(e) => e.code(),
            SampleOrchestratorError::Filter(e) => e.code(),
            SampleOrchestratorError::FilterLogsForShard(e) => e.code(),
            SampleOrchestratorError::Panic(_) => ErrorCodes::Aborted,
            SampleOrchestratorError::Projection(e) => e.code(),
            SampleOrchestratorError::Result(_) => ErrorCodes::Internal,
            SampleOrchestratorError::Sample(e) => e.code(),
            SampleOrchestratorError::Aborted => ErrorCodes::ResourceExhausted,
        }
    }
}

impl<E> From<TaskError<E>> for SampleOrchestratorError
where
    E: Into<SampleOrchestratorError>,
{
    fn from(value: TaskError<E>) -> Self {
        match value {
            TaskError::Panic(e) => e.into(),
            TaskError::TaskFailed(e) => e.into(),
            TaskError::Aborted => SampleOrchestratorError::Aborted,
        }
    }
}

#[derive(Debug)]
pub struct SampleOrchestrator {
    context: OrchestratorContext,
    queue: usize,
    blockfile_provider: BlockfileProvider,
    spann_provider: SpannProvider,
    collection_and_segments: CollectionAndSegments,
    fetch_log: FetchLogOperator,
    fetched_logs: Option<FetchLogOutput>,
    filter: Filter,
    sample: Sample,
    projection: Projection,
    bloom_filter_manager: Option<BloomFilterManager>,
    shard_index: u32,
    num_shards: u32,
    strata_seen: u64,
    result_channel: Option<Sender<Result<SampleResult, SampleOrchestratorError>>>,
}

impl SampleOrchestrator {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        blockfile_provider: BlockfileProvider,
        spann_provider: SpannProvider,
        dispatcher: ComponentHandle<Dispatcher>,
        queue: usize,
        collection_and_segments: CollectionAndSegments,
        fetch_log: FetchLogOperator,
        filter: Filter,
        sample: Sample,
        projection: Projection,
        bloom_filter_manager: Option<BloomFilterManager>,
        shard_index: u32,
        num_shards: u32,
    ) -> Self {
        let context = OrchestratorContext::new(dispatcher);
        Self {
            context,
            queue,
            blockfile_provider,
            spann_provider,
            collection_and_segments,
            fetch_log,
            fetched_logs: None,
            filter,
            sample,
            projection,
            bloom_filter_manager,
            shard_index,
            num_shards,
            strata_seen: 0,
            result_channel: None,
        }
    }
}

#[async_trait]
impl Orchestrator for SampleOrchestrator {
    type Output = SampleResult;
    type Error = SampleOrchestratorError;

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
        let prefetch_record_segment_task = wrap(
            Box::new(PrefetchSegmentOperator::new()),
            PrefetchSegmentInput::new_with_shard(
                self.collection_and_segments.record_segment.clone(),
                self.blockfile_provider.clone(),
                Some(self.shard_index),
            ),
            ctx.receiver(),
            self.context.task_cancellation_token.clone(),
        );
        let prefetch_span = tracing::info_span!(parent: None, "Prefetch record segment", segment_id = %self.collection_and_segments.record_segment.id);
        Span::current().add_link(prefetch_span.context().span().span_context().clone());
        tasks.push((prefetch_record_segment_task, Some(prefetch_span)));

        let prefetch_metadata_task = wrap(
            Box::new(PrefetchSegmentOperator::new()),
            PrefetchSegmentInput::new_with_shard(
                self.collection_and_segments.metadata_segment.clone(),
                self.blockfile_provider.clone(),
                Some(self.shard_index),
            ),
            ctx.receiver(),
            self.context.task_cancellation_token.clone(),
        );
        let prefetch_span = tracing::info_span!(parent: None, "Prefetch metadata segment", segment_id = %self.collection_and_segments.metadata_segment.id);
        Span::current().add_link(prefetch_span.context().span().span_context().clone());
        tasks.push((prefetch_metadata_task, Some(prefetch_span)));

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

    fn set_result_channel(
        &mut self,
        sender: Sender<Result<SampleResult, SampleOrchestratorError>>,
    ) {
        self.result_channel = Some(sender)
    }

    fn take_result_channel(
        &mut self,
    ) -> Option<Sender<Result<SampleResult, SampleOrchestratorError>>> {
        self.result_channel.take()
    }
}

#[async_trait]
impl Handler<TaskResult<PrefetchSegmentOutput, PrefetchSegmentError>> for SampleOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        _: TaskResult<PrefetchSegmentOutput, PrefetchSegmentError>,
        _: &ComponentContext<SampleOrchestrator>,
    ) {
    }
}

#[async_trait]
impl Handler<TaskResult<FetchLogOutput, FetchLogError>> for SampleOrchestrator {
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

        let task = wrap(
            Box::new(FilterLogsForShardOperator {
                shard_index: self.shard_index,
                num_shards: self.num_shards,
                record_segment: self.collection_and_segments.record_segment.clone(),
                blockfile_provider: self.blockfile_provider.clone(),
                bloom_filter_manager: self.bloom_filter_manager.clone(),
            }),
            output,
            ctx.receiver(),
            self.context.task_cancellation_token.clone(),
        );
        self.send(task, ctx, Some(Span::current())).await;
    }
}

#[async_trait]
impl Handler<TaskResult<FilterLogsForShardOutput, FilterLogsForShardError>> for SampleOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<FilterLogsForShardOutput, FilterLogsForShardError>,
        ctx: &ComponentContext<Self>,
    ) {
        let partitioned = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(output) => output,
            None => return,
        };

        self.fetched_logs = Some(partitioned.clone());

        let task = wrap(
            Box::new(self.filter.clone()),
            FilterInput {
                logs: partitioned,
                blockfile_provider: self.blockfile_provider.clone(),
                metadata_segment: self.collection_and_segments.metadata_segment.clone(),
                record_segment: self.collection_and_segments.record_segment.clone(),
                bloom_filter_manager: self.bloom_filter_manager.clone(),
                shard_index: self.shard_index,
            },
            ctx.receiver(),
            self.context.task_cancellation_token.clone(),
        );
        self.send(task, ctx, Some(Span::current())).await;
    }
}

#[async_trait]
impl Handler<TaskResult<FilterOutput, FilterError>> for SampleOrchestrator {
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
            Box::new(self.sample.clone()),
            SampleInput {
                logs: self
                    .fetched_logs
                    .as_ref()
                    .expect("FetchLogOperator should have finished already")
                    .clone(),
                blockfile_provider: self.blockfile_provider.clone(),
                collection: self.collection_and_segments.collection.clone(),
                record_segment: self.collection_and_segments.record_segment.clone(),
                vector_segment: self.collection_and_segments.vector_segment.clone(),
                log_offset_ids: output.log_offset_ids,
                compact_offset_ids: output.compact_offset_ids,
                bloom_filter_manager: self.bloom_filter_manager.clone(),
                spann_provider: self.spann_provider.clone(),
                shard_index: self.shard_index,
            },
            ctx.receiver(),
            self.context.task_cancellation_token.clone(),
        );
        self.send(task, ctx, Some(Span::current())).await;
    }
}

#[async_trait]
impl Handler<TaskResult<SampleOutput, SampleError>> for SampleOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<SampleOutput, SampleError>,
        ctx: &ComponentContext<Self>,
    ) {
        let output = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(output) => output,
            None => return,
        };
        self.strata_seen = output.strata_seen;

        let input = ProjectionInput {
            logs: self
                .fetched_logs
                .as_ref()
                .expect("FetchLogOperator should have finished already")
                .clone(),
            blockfile_provider: self.blockfile_provider.clone(),
            record_segment: self.collection_and_segments.record_segment.clone(),
            offset_ids: output.offset_ids,
            bloom_filter_manager: self.bloom_filter_manager.clone(),
            shard_index: self.shard_index,
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
impl Handler<TaskResult<ProjectionOutput, ProjectionError>> for SampleOrchestrator {
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
            Ok(SampleResult {
                pulled_log_bytes,
                strata_seen: self.strata_seen,
                result: output,
            }),
            ctx,
        )
        .await;
    }
}
