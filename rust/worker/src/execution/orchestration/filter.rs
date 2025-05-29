use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_distance::DistanceFunction;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_index::hnsw_provider::HnswIndexProvider;
use chroma_segment::distributed_hnsw::{
    DistributedHNSWSegmentFromSegmentError, DistributedHNSWSegmentReader,
};
use chroma_system::{
    wrap, ChannelError, ComponentContext, ComponentHandle, Dispatcher, Handler, Orchestrator,
    PanicError, TaskError, TaskMessage, TaskResult,
};
use chroma_types::{
    operator::Filter, CollectionAndSegments, HnswParametersFromSegmentError, Segment, SegmentType,
};
use thiserror::Error;
use tokio::sync::oneshot::{error::RecvError, Sender};
use tracing::Span;

use crate::execution::operators::{
    fetch_log::{FetchLogError, FetchLogOperator, FetchLogOutput},
    filter::{FilterError, FilterInput, FilterOutput},
};

#[derive(Clone, Debug)]
pub struct FilterOrchestratorOutput {
    pub logs: FetchLogOutput,
    pub distance_function: DistanceFunction,
    pub filter_output: FilterOutput,
    pub hnsw_reader: Option<Box<DistributedHNSWSegmentReader>>,
    pub record_segment: Segment,
    pub vector_segment: Segment,
    pub dimension: usize,
    pub fetch_log_bytes: u64,
}

#[derive(Error, Debug)]
pub enum FilterOrchestratorError {
    #[error("Operation aborted because resources exhausted")]
    Aborted,
    #[error("Error sending message through channel: {0}")]
    Channel(#[from] ChannelError),
    #[error("Error parsing collection config: {0}")]
    Config(#[from] HnswParametersFromSegmentError),
    #[error("Error running Fetch Log Operator: {0}")]
    FetchLog(#[from] FetchLogError),
    #[error("Error running Filter Operator: {0}")]
    Filter(#[from] FilterError),
    #[error("Error creating hnsw segment reader: {0}")]
    HnswReader(#[from] DistributedHNSWSegmentFromSegmentError),
    #[error("Invalid distance function")]
    InvalidDistanceFunction,
    #[error("Error inspecting collection dimension")]
    NoCollectionDimension,
    #[error("Panic: {0}")]
    Panic(#[from] PanicError),
    #[error("Error receiving operator result: {0}")]
    Recv(#[from] RecvError),
}

impl ChromaError for FilterOrchestratorError {
    fn code(&self) -> ErrorCodes {
        match self {
            FilterOrchestratorError::Aborted => ErrorCodes::ResourceExhausted,
            FilterOrchestratorError::Channel(e) => e.code(),
            FilterOrchestratorError::Config(e) => e.code(),
            FilterOrchestratorError::FetchLog(e) => e.code(),
            FilterOrchestratorError::Filter(e) => e.code(),
            FilterOrchestratorError::HnswReader(e) => e.code(),
            FilterOrchestratorError::InvalidDistanceFunction => ErrorCodes::InvalidArgument,
            FilterOrchestratorError::NoCollectionDimension => ErrorCodes::InvalidArgument,
            FilterOrchestratorError::Panic(e) => e.code(),
            FilterOrchestratorError::Recv(_) => ErrorCodes::Internal,
        }
    }
}

impl<E> From<TaskError<E>> for FilterOrchestratorError
where
    E: Into<FilterOrchestratorError>,
{
    fn from(value: TaskError<E>) -> Self {
        match value {
            TaskError::Aborted => FilterOrchestratorError::Aborted,
            TaskError::Panic(e) => e.into(),
            TaskError::TaskFailed(e) => e.into(),
        }
    }
}

type FilterOrchestratorResult = Result<FilterOrchestratorOutput, FilterOrchestratorError>;

/// The `FilterOrchestrator` chains a sequence of operators in sequence to compute the set
/// of collection records for further queries
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
pub struct FilterOrchestrator {
    // Orchestrator parameters
    blockfile_provider: BlockfileProvider,
    dispatcher: ComponentHandle<Dispatcher>,
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
    result_channel: Option<Sender<FilterOrchestratorResult>>,
}

impl FilterOrchestrator {
    pub fn new(
        blockfile_provider: BlockfileProvider,
        dispatcher: ComponentHandle<Dispatcher>,
        hnsw_provider: HnswIndexProvider,
        queue: usize,
        collection_and_segments: CollectionAndSegments,
        fetch_log: FetchLogOperator,
        filter: Filter,
    ) -> Self {
        Self {
            blockfile_provider,
            dispatcher,
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
impl Orchestrator for FilterOrchestrator {
    type Output = FilterOrchestratorOutput;
    type Error = FilterOrchestratorError;

    fn dispatcher(&self) -> ComponentHandle<Dispatcher> {
        self.dispatcher.clone()
    }

    async fn initial_tasks(
        &mut self,
        ctx: &ComponentContext<Self>,
    ) -> Vec<(TaskMessage, Option<Span>)> {
        vec![(
            wrap(Box::new(self.fetch_log.clone()), (), ctx.receiver()),
            Some(Span::current()),
        )]
    }

    fn queue_size(&self) -> usize {
        self.queue
    }

    fn set_result_channel(&mut self, sender: Sender<FilterOrchestratorResult>) {
        self.result_channel = Some(sender)
    }

    fn take_result_channel(&mut self) -> Sender<FilterOrchestratorResult> {
        self.result_channel
            .take()
            .expect("The result channel should be set before take")
    }
}

#[async_trait]
impl Handler<TaskResult<FetchLogOutput, FetchLogError>> for FilterOrchestrator {
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
        );
        self.send(task, ctx, Some(Span::current())).await;
    }
}

#[async_trait]
impl Handler<TaskResult<FilterOutput, FilterError>> for FilterOrchestrator {
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
                    .ok_or(FilterOrchestratorError::NoCollectionDimension),
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
                        .config
                        .get_hnsw_config_with_legacy_fallback(
                            &self.collection_and_segments.vector_segment,
                        ),
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
                        .config
                        .get_spann_config()
                        .ok_or(FilterOrchestratorError::InvalidDistanceFunction),
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

        let output = FilterOrchestratorOutput {
            logs,
            distance_function,
            filter_output: output,
            hnsw_reader,
            record_segment: self.collection_and_segments.record_segment.clone(),
            vector_segment: self.collection_and_segments.vector_segment.clone(),
            dimension: collection_dimension as usize,
            fetch_log_bytes,
        };
        self.terminate_with_result(Ok(output), ctx).await;
    }
}
