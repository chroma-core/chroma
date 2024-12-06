use chroma_blockstore::provider::BlockfileProvider;
use chroma_distance::DistanceFunction;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_index::hnsw_provider::HnswIndexProvider;
use chroma_types::{CollectionSegments, Segment};
use thiserror::Error;
use tokio::sync::oneshot::{self, error::RecvError, Sender};
use tonic::async_trait;
use tracing::Span;

use crate::{
    execution::{
        dispatcher::Dispatcher,
        operator::{wrap, TaskError, TaskResult},
        operators::{
            fetch_log::{FetchLogError, FetchLogOperator, FetchLogOutput},
            filter::{FilterError, FilterInput, FilterOperator, FilterOutput},
            knn_hnsw::KnnHnswError,
            knn_log::KnnLogError,
            knn_projection::{KnnProjectionError, KnnProjectionOutput},
            spann_bf_pl::SpannBfPlError,
            spann_centers_search::SpannCentersSearchError,
            spann_fetch_pl::SpannFetchPlError,
        },
        orchestration::common::terminate_with_error,
    },
    segment::{
        distributed_hnsw_segment::{
            DistributedHNSWSegmentFromSegmentError, DistributedHNSWSegmentReader,
        },
        utils::distance_function_from_segment,
    },
    system::{ChannelError, Component, ComponentContext, ComponentHandle, Handler, System},
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
    #[error("Error running Knn Log Operator: {0}")]
    KnnLog(#[from] KnnLogError),
    #[error("Error running Knn Hnsw Operator: {0}")]
    KnnHnsw(#[from] KnnHnswError),
    #[error("Error running Spann Head search Operator: {0}")]
    SpannHeadSearch(#[from] SpannCentersSearchError),
    #[error("Error running Spann fetch posting list Operator: {0}")]
    SpannFetchPl(#[from] SpannFetchPlError),
    #[error("Error running Spann brute force posting list Operator: {0}")]
    SpannBfPl(#[from] SpannBfPlError),
    #[error("Error running Knn Projection Operator: {0}")]
    KnnProjection(#[from] KnnProjectionError),
    #[error("Error inspecting collection dimension")]
    NoCollectionDimension,
    #[error("Panic running task: {0}")]
    Panic(String),
    #[error("Error receiving final result: {0}")]
    Result(#[from] RecvError),
    #[error("Invalid distance function")]
    InvalidDistanceFunction,
}

impl ChromaError for KnnError {
    fn code(&self) -> ErrorCodes {
        match self {
            KnnError::Channel(e) => e.code(),
            KnnError::FetchLog(e) => e.code(),
            KnnError::Filter(e) => e.code(),
            KnnError::HnswReader(e) => e.code(),
            KnnError::KnnLog(e) => e.code(),
            KnnError::KnnHnsw(e) => e.code(),
            KnnError::SpannHeadSearch(e) => e.code(),
            KnnError::SpannFetchPl(e) => e.code(),
            KnnError::SpannBfPl(e) => e.code(),
            KnnError::KnnProjection(e) => e.code(),
            KnnError::NoCollectionDimension => ErrorCodes::InvalidArgument,
            KnnError::Panic(_) => ErrorCodes::Aborted,
            KnnError::Result(_) => ErrorCodes::Internal,
            KnnError::InvalidDistanceFunction => ErrorCodes::InvalidArgument,
        }
    }
}

impl<E> From<TaskError<E>> for KnnError
where
    E: Into<KnnError>,
{
    fn from(value: TaskError<E>) -> Self {
        match value {
            TaskError::Panic(e) => KnnError::Panic(e.unwrap_or_default()),
            TaskError::TaskFailed(e) => e.into(),
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
    collection_segments: CollectionSegments,

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
        collection_segments: CollectionSegments,
        fetch_log: FetchLogOperator,
        filter: FilterOperator,
    ) -> Self {
        Self {
            blockfile_provider,
            dispatcher,
            hnsw_provider,
            queue,
            collection_segments,
            fetch_log,
            fetched_logs: None,
            filter,
            result_channel: None,
        }
    }

    pub async fn run(mut self, system: System) -> KnnFilterResult {
        let (tx, rx) = oneshot::channel();
        self.result_channel = Some(tx);
        let mut handle = system.start_component(self);
        let result = rx.await;
        handle.stop();
        result?
    }

    fn terminate_with_error<E>(&mut self, ctx: &ComponentContext<Self>, err: E)
    where
        E: Into<KnnError>,
    {
        let knn_err = err.into();
        tracing::error!("Error running orchestrator: {}", &knn_err);
        terminate_with_error(self.result_channel.take(), knn_err, ctx);
    }
}

#[async_trait]
impl Component for KnnFilterOrchestrator {
    fn get_name() -> &'static str {
        "Knn Filter Orchestrator"
    }

    fn queue_size(&self) -> usize {
        self.queue
    }

    async fn on_start(&mut self, ctx: &ComponentContext<Self>) {
        let task = wrap(Box::new(self.fetch_log.clone()), (), ctx.receiver());
        if let Err(err) = self.dispatcher.send(task, Some(Span::current())).await {
            self.terminate_with_error(ctx, err);
            return;
        }
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
        let output = match message.into_inner() {
            Ok(output) => output,
            Err(err) => {
                self.terminate_with_error(ctx, err);
                return;
            }
        };

        self.fetched_logs = Some(output.clone());

        let task = wrap(
            Box::new(self.filter.clone()),
            FilterInput {
                logs: output,
                blockfile_provider: self.blockfile_provider.clone(),
                metadata_segment: self.collection_segments.metadata_segment.clone(),
                record_segment: self.collection_segments.record_segment.clone(),
            },
            ctx.receiver(),
        );
        if let Err(err) = self.dispatcher.send(task, Some(Span::current())).await {
            self.terminate_with_error(ctx, err);
        }
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
        let output = match message.into_inner() {
            Ok(output) => output,
            Err(err) => {
                self.terminate_with_error(ctx, err);
                return;
            }
        };
        let collection_dimension = match self.collection_segments.collection.dimension {
            Some(dimension) => dimension as u32,
            None => {
                self.terminate_with_error(ctx, KnnError::NoCollectionDimension);
                return;
            }
        };
        let distance_function =
            match distance_function_from_segment(&self.collection_segments.vector_segment) {
                Ok(distance_function) => distance_function,
                Err(_) => {
                    self.terminate_with_error(ctx, KnnError::InvalidDistanceFunction);
                    return;
                }
            };
        let hnsw_reader = match DistributedHNSWSegmentReader::from_segment(
            &self.collection_segments.vector_segment,
            collection_dimension as usize,
            self.hnsw_provider.clone(),
        )
        .await
        {
            Ok(hnsw_reader) => Some(hnsw_reader),
            Err(err) if matches!(*err, DistributedHNSWSegmentFromSegmentError::Uninitialized) => {
                None
            }

            Err(err) => {
                self.terminate_with_error(ctx, *err);
                return;
            }
        };
        if let Some(chan) = self.result_channel.take() {
            if chan
                .send(Ok(KnnFilterOutput {
                    logs: self
                        .fetched_logs
                        .take()
                        .expect("FetchLogOperator should have finished already"),
                    distance_function,
                    filter_output: output,
                    hnsw_reader,
                    record_segment: self.collection_segments.record_segment.clone(),
                    vector_segment: self.collection_segments.vector_segment.clone(),
                    dimension: collection_dimension as usize,
                }))
                .is_err()
            {
                tracing::error!("Error sending final result");
            };
        }
    }
}

pub(super) type KnnOutput = KnnProjectionOutput;
pub(super) type KnnResult = Result<KnnOutput, KnnError>;
