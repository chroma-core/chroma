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
    PanicError, TaskError, TaskMessage, TaskResult,
};
use chroma_types::{CollectionAndSegments, HnswParametersFromSegmentError, Segment, SegmentType};
use thiserror::Error;
use tokio::sync::oneshot::{error::RecvError, Sender};
use tracing::Span;

use crate::execution::operators::{
    fetch_log::{FetchLogError, FetchLogOperator, FetchLogOutput},
    filter::{FilterError, FilterInput, FilterOperator, FilterOutput},
    knn_hnsw::KnnHnswError,
    knn_log::KnnLogError,
    knn_merge::KnnMergeError,
    knn_projection::{KnnProjectionError, KnnProjectionOutput},
    spann_bf_pl::SpannBfPlError,
    spann_centers_search::SpannCentersSearchError,
    spann_fetch_pl::SpannFetchPlError,
    spann_knn_merge::SpannKnnMergeError,
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
    #[error("Error running Spann Knn Merge Operator")]
    SpannKnnMerge(#[from] SpannKnnMergeError),
    #[error("Error creating spann segment reader: {0}")]
    SpannSegmentReaderCreationError(#[from] SpannSegmentReaderError),
    #[error("Invalid distance function")]
    InvalidDistanceFunction,
    #[error("Operation aborted because resources exhausted")]
    Aborted,
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
            KnnError::SpannKnnMerge(_) => ErrorCodes::Internal,
            KnnError::InvalidDistanceFunction => ErrorCodes::InvalidArgument,
            KnnError::Aborted => ErrorCodes::ResourceExhausted,
            KnnError::SpannSegmentReaderCreationError(e) => e.code(),
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
    pub distance_function: DistanceFunction,
    pub filter_output: FilterOutput,
    pub hnsw_reader: Option<Box<DistributedHNSWSegmentReader>>,
    pub record_segment: Segment,
    pub vector_segment: Segment,
    pub dimension: usize,
    pub fetch_log_bytes: u64,
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
    filter: FilterOperator,

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
        filter: FilterOperator,
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
impl Orchestrator for KnnFilterOrchestrator {
    type Output = KnnFilterOutput;
    type Error = KnnError;

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

    fn set_result_channel(&mut self, sender: Sender<KnnFilterResult>) {
        self.result_channel = Some(sender)
    }

    fn take_result_channel(&mut self) -> Sender<KnnFilterResult> {
        self.result_channel
            .take()
            .expect("The result channel should be set before take")
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
                        .ok_or(KnnError::InvalidDistanceFunction),
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

pub(super) type KnnOutput = KnnProjectionOutput;
pub(super) type KnnResult = Result<KnnOutput, KnnError>;
