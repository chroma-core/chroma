use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_distance::DistanceFunction;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_index::hnsw_provider::HnswIndexProvider;
use chroma_segment::{
    distributed_hnsw::{DistributedHNSWSegmentFromSegmentError, DistributedHNSWSegmentReader},
    distributed_spann::SpannSegmentReaderError,
};
use chroma_system::{
    wrap, ChannelError, ComponentContext, ComponentHandle, Dispatcher, Handler, Orchestrator,
    OrchestratorContext, PanicError, TaskError, TaskMessage, TaskResult,
};
use chroma_types::{
    operator::Filter, CollectionAndSegments, HnswParametersFromSegmentError, SchemaError,
    SegmentType,
};
use opentelemetry::trace::TraceContextExt;
use thiserror::Error;
use tokio::sync::oneshot::{error::RecvError, Sender};
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::execution::operators::{
    fetch_log::{FetchLogError, FetchLogOperator, FetchLogOutput},
    filter::{FilterError, FilterInput, FilterOutput},
    knn_hnsw::KnnHnswError,
    knn_log::KnnLogError,
    knn_merge::KnnMergeError,
    knn_projection::KnnProjectionError,
    prefetch_segment::{
        PrefetchSegmentError, PrefetchSegmentInput, PrefetchSegmentOperator, PrefetchSegmentOutput,
    },
    spann_bf_pl::SpannBfPlError,
    spann_centers_search::SpannCentersSearchError,
    spann_fetch_pl::SpannFetchPlError,
};

#[derive(Error, Debug)]
pub enum KnnError {
    #[error("Error sending message through channel: {0}")]
    Channel(#[from] ChannelError),
    #[error("Error running Fetch Log Operator: {0}")]
    FetchLog(#[from] FetchLogError),
    #[error("Error running Filter Operator: {0}")]
    Filter(#[from] FilterError),
    #[error("Error creating hnsw segment reader: {0}")]
    HnswReader(#[from] DistributedHNSWSegmentFromSegmentError),
    #[error("Error parsing collection config: {0}")]
    Config(#[from] HnswParametersFromSegmentError),
    #[error("Error running Knn Log Operator: {0}")]
    KnnLog(#[from] KnnLogError),
    #[error("Error running Knn Hnsw Operator: {0}")]
    KnnHnsw(#[from] KnnHnswError),
    #[error("Error running Knn Merge Operator")]
    KnnMerge(#[from] KnnMergeError),
    #[error("Error running Knn Projection Operator: {0}")]
    KnnProjection(#[from] KnnProjectionError),
    #[error("Error inspecting collection dimension")]
    NoCollectionDimension,
    #[error("Panic: {0}")]
    Panic(#[from] PanicError),
    #[error("Error receiving final result: {0}")]
    Result(#[from] RecvError),
    #[error("Error running Spann Bruteforce Postinglist Operator: {0}")]
    SpannBfPl(#[from] SpannBfPlError),
    #[error("Error running Spann Fetch Postinglist Operator: {0}")]
    SpannFetchPl(#[from] SpannFetchPlError),
    #[error("Error running Spann Head Search Operator: {0}")]
    SpannHeadSearch(#[from] SpannCentersSearchError),
    #[error("Error creating spann segment reader: {0}")]
    SpannSegmentReaderCreationError(#[from] SpannSegmentReaderError),
    #[error("Invalid distance function")]
    InvalidDistanceFunction,
    #[error("Operation aborted because resources exhausted")]
    Aborted,
    #[error("Invalid schema: {0}")]
    InvalidSchema(#[from] SchemaError),
}

impl ChromaError for KnnError {
    fn code(&self) -> ErrorCodes {
        match self {
            KnnError::Channel(e) => e.code(),
            KnnError::FetchLog(e) => e.code(),
            KnnError::Filter(e) => e.code(),
            KnnError::HnswReader(e) => e.code(),
            KnnError::Config(e) => e.code(),
            KnnError::KnnLog(e) => e.code(),
            KnnError::KnnHnsw(e) => e.code(),
            KnnError::KnnMerge(_) => ErrorCodes::Internal,
            KnnError::KnnProjection(e) => e.code(),
            KnnError::NoCollectionDimension => ErrorCodes::InvalidArgument,
            KnnError::Panic(_) => ErrorCodes::Aborted,
            KnnError::Result(_) => ErrorCodes::Internal,
            KnnError::SpannBfPl(e) => e.code(),
            KnnError::SpannFetchPl(e) => e.code(),
            KnnError::SpannHeadSearch(e) => e.code(),
            KnnError::InvalidDistanceFunction => ErrorCodes::InvalidArgument,
            KnnError::Aborted => ErrorCodes::ResourceExhausted,
            KnnError::SpannSegmentReaderCreationError(e) => e.code(),
            KnnError::InvalidSchema(e) => e.code(),
        }
    }
}

impl<E> From<TaskError<E>> for KnnError
where
    E: Into<KnnError>,
{
    fn from(value: TaskError<E>) -> Self {
        match value {
            TaskError::Panic(e) => e.into(),
            TaskError::TaskFailed(e) => e.into(),
            TaskError::Aborted => KnnError::Aborted,
        }
    }
}

#[derive(Clone, Debug)]
pub struct KnnFilterOutput {
    pub logs: FetchLogOutput,
    pub fetch_log_bytes: u64,
    pub filter_output: FilterOutput,
    pub dimension: usize,
    pub distance_function: DistanceFunction,
    pub hnsw_reader: Option<Box<DistributedHNSWSegmentReader>>,
}

type KnnFilterResult = Result<KnnFilterOutput, KnnError>;

/// The `KnnFilterOrchestrator` chains a sequence of operators in sequence to evaluate
/// the first half of a `<collection>.query(...)` query from the user
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
///     ┌──────────────────┐
///     │                  │
///     │  result_channel  │
///     │                  │
///     └──────────────────┘
/// ```
#[derive(Debug)]
pub struct KnnFilterOrchestrator {
    // Orchestrator parameters
    context: OrchestratorContext,
    blockfile_provider: BlockfileProvider,
    hnsw_provider: HnswIndexProvider,
    queue: usize,

    // Collection segments
    collection_and_segments: CollectionAndSegments,

    // Fetch logs
    fetch_log: FetchLogOperator,

    // Fetched logs
    fetched_logs: Option<FetchLogOutput>,

    // Pipelined operators
    filter: Filter,

    // Result channel
    result_channel: Option<Sender<KnnFilterResult>>,
}

impl KnnFilterOrchestrator {
    pub fn new(
        blockfile_provider: BlockfileProvider,
        dispatcher: ComponentHandle<Dispatcher>,
        hnsw_provider: HnswIndexProvider,
        queue: usize,
        collection_and_segments: CollectionAndSegments,
        fetch_log: FetchLogOperator,
        filter: Filter,
    ) -> Self {
        let context = OrchestratorContext::new(dispatcher);
        Self {
            context,
            blockfile_provider,
            hnsw_provider,
            queue,
            collection_and_segments,
            fetch_log,
            fetched_logs: None,
            filter,
            result_channel: None,
        }
    }
}

#[async_trait]
impl Orchestrator for KnnFilterOrchestrator {
    type Output = KnnFilterOutput;
    type Error = KnnError;

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
        // prefetch spann segment
        let prefetch_task = wrap(
            Box::new(PrefetchSegmentOperator::new()),
            PrefetchSegmentInput::new(
                self.collection_and_segments.vector_segment.clone(),
                self.blockfile_provider.clone(),
            ),
            ctx.receiver(),
            self.context.task_cancellation_token.clone(),
        );
        // Prefetch task is detached from the orchestrator
        let prefetch_span = tracing::info_span!(parent: None, "Prefetch spann segment", segment_id = %self.collection_and_segments.vector_segment.id);
        Span::current().add_link(prefetch_span.context().span().span_context().clone());
        tasks.push((prefetch_task, Some(prefetch_span)));

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
        tasks.push((prefetch_metadata_task, Some(prefetch_span)));

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

    fn set_result_channel(&mut self, sender: Sender<KnnFilterResult>) {
        self.result_channel = Some(sender)
    }

    fn take_result_channel(&mut self) -> Option<Sender<KnnFilterResult>> {
        self.result_channel.take()
    }
}

#[async_trait]
impl Handler<TaskResult<PrefetchSegmentOutput, PrefetchSegmentError>> for KnnFilterOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        _: TaskResult<PrefetchSegmentOutput, PrefetchSegmentError>,
        _: &ComponentContext<KnnFilterOrchestrator>,
    ) {
        // Nothing to do.
    }
}

#[async_trait]
impl Handler<TaskResult<FetchLogOutput, FetchLogError>> for KnnFilterOrchestrator {
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
impl Handler<TaskResult<FilterOutput, FilterError>> for KnnFilterOrchestrator {
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
        let collection_dimension = match self
            .ok_or_terminate(
                self.collection_and_segments
                    .collection
                    .dimension
                    .ok_or(KnnError::NoCollectionDimension),
                ctx,
            )
            .await
        {
            Some(dim) => dim as u32,
            None => return,
        };

        let (hnsw_reader, distance_function) = if self.collection_and_segments.vector_segment.r#type
            == SegmentType::HnswDistributed
        {
            let hnsw_configuration = match self
                .ok_or_terminate(
                    self.collection_and_segments
                        .collection
                        .schema
                        .as_ref()
                        .ok_or(KnnError::InvalidSchema(SchemaError::InvalidSchema {
                            reason: "Schema is None".to_string(),
                        }))
                        .and_then(|schema| {
                            schema
                                .get_internal_hnsw_config_with_legacy_fallback(
                                    &self.collection_and_segments.vector_segment,
                                )
                                .map_err(KnnError::from)
                        }),
                    ctx,
                )
                .await
                .flatten()
            {
                Some(hnsw_configuration) => hnsw_configuration,
                None => return,
            };
            match DistributedHNSWSegmentReader::from_segment(
                &self.collection_and_segments.collection,
                &self.collection_and_segments.vector_segment,
                collection_dimension as usize,
                self.hnsw_provider.clone(),
            )
            .await
            {
                Ok(hnsw_reader) => (Some(hnsw_reader), hnsw_configuration.space.into()),
                Err(err)
                    if matches!(*err, DistributedHNSWSegmentFromSegmentError::Uninitialized) =>
                {
                    (None, hnsw_configuration.space.into())
                }

                Err(err) => {
                    self.terminate_with_result(Err((*err).into()), ctx).await;
                    return;
                }
            }
        } else {
            let params = match self
                .ok_or_terminate(
                    self.collection_and_segments
                        .collection
                        .schema
                        .as_ref()
                        .ok_or(KnnError::InvalidSchema(SchemaError::InvalidSchema {
                            reason: "Schema is None".to_string(),
                        }))
                        .and_then(|s| {
                            s.get_internal_spann_config()
                                .ok_or(KnnError::InvalidDistanceFunction)
                        }),
                    ctx,
                )
                .await
            {
                Some(params) => params,
                None => return,
            };
            (None, params.space.into())
        };

        let logs = self
            .fetched_logs
            .take()
            .expect("FetchLogOperator should have finished already");

        let fetch_log_bytes = logs.iter().map(|(l, _)| l.size_bytes()).sum();

        let output = KnnFilterOutput {
            logs,
            fetch_log_bytes,
            filter_output: output,
            dimension: collection_dimension as usize,
            distance_function,
            hnsw_reader,
        };
        self.terminate_with_result(Ok(output), ctx).await;
    }
}
