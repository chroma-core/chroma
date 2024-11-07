use chroma_blockstore::provider::BlockfileProvider;
use chroma_distance::{DistanceFunction, DistanceFunctionError};
use chroma_error::{ChromaError, ErrorCodes};
use chroma_index::hnsw_provider::HnswIndexProvider;
use chroma_types::MetadataValue;
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
            fetch_segment::{FetchSegmentError, FetchSegmentOperator, FetchSegmentOutput},
            filter::{FilterError, FilterInput, FilterOperator, FilterOutput},
            knn::{KnnOperator, RecordDistance},
            knn_hnsw::{KnnHnswError, KnnHnswInput, KnnHnswOutput},
            knn_log::{KnnLogError, KnnLogInput, KnnLogOutput},
            knn_merge::{KnnMergeError, KnnMergeInput, KnnMergeOperator, KnnMergeOutput},
            knn_projection::{
                KnnProjectionError, KnnProjectionInput, KnnProjectionOperator, KnnProjectionOutput,
            },
            prefetch_record::{
                PrefetchRecordError, PrefetchRecordInput, PrefetchRecordOperator,
                PrefetchRecordOutput,
            },
        },
        orchestration::common::terminate_with_error,
    },
    system::{ChannelError, Component, ComponentContext, ComponentHandle, Handler, System},
};

/// The `knn` module contains two orchestrator: `KnnFilterOrchestrator` and `KnnOrchestrator`.
/// When used together, they carry out the evaluation of a `<collection>.query(...)` query
/// for the user. We breakdown the evaluation into two parts because a `<collection>.query(...)`
/// is inherently multiple queries sharing the same filter criteria. Thus we first evaluate
/// the filter criteria with `KnnFilterOrchestrator`. Then we spawn a `KnnOrchestrator` for each
/// of the embedding together with a copy of the result from `KnnFilterOrchestrator`, run these
/// orchestrators in parallel, and join them in the end.
///
///
/// # Pipeline
/// ```text
///                                                           │
///                                                           │
///                                                           │
///                            ┌────────────────────────────  │  ───────────────────────────────┐
///                            │                              ▼                                 │
///                            │                       ┌────────────┐    KnnFilterOrchestrator  │
///                            │                       │            │                           │
///                            │           ┌───────────┤  on_start  ├────────────────┐          │
///                            │           │           │            │                │          │
///                            │           │           └────────────┘                │          │
///                            │           │                                         │          │
///                            │           ▼                                         ▼          │
///                            │  ┌────────────────────┐            ┌────────────────────────┐  │
///                            │  │                    │            │                        │  │
///                            │  │  FetchLogOperator  │            │  FetchSegmentOperator  │  │
///                            │  │                    │            │                        │  │
///                            │  └────────┬───────────┘            └────────────────┬───────┘  │
///                            │           │                                         │          │
///                            │           │                                         │          │
///                            │           │     ┌─────────────────────────────┐     │          │
///                            │           │     │                             │     │          │
///                            │           └────►│  try_start_filter_operator  │◄────┘          │
///                            │                 │                             │                │
///                            │                 └────────────┬────────────────┘                │
///                            │                              │                                 │
///                            │                              ▼                                 │
///                            │                    ┌───────────────────┐                       │
///                            │                    │                   │                       │
///                            │                    │   FilterOperator  │                       │
///                            │                    │                   │                       │
///                            │                    └─────────┬─────────┘                       │
///                            │                              │                                 │
///                            │                              ▼                                 │
///                            │                     ┌──────────────────┐                       │
///                            │                     │                  │                       │
///                            │                     │  result_channel  │                       │
///                            │                     │                  │                       │
///                            │                     └────────┬─────────┘                       │
///                            │                              │                                 │
///                            └────────────────────────────  │  ───────────────────────────────┘
///                                                           │
///                                                           │
///                                                           │
///                        ┌──────────────────────────────────┴─────────────────────────────────────┐
///                        │                                                                        │
///                        │                    ... One branch per embedding ...                    │
///                        │                                                                        │
/// ┌────────────────────  │  ─────────────────────┐                         ┌────────────────────  │  ─────────────────────┐
/// │                      ▼                       │                         │                      ▼                       │
/// │               ┌────────────┐ KnnOrchestrator │                         │               ┌────────────┐ KnnOrchestrator │
/// │               │            │                 │                         │               │            │                 │
/// │           ┌───┤  on_start  ├────┐            │           ...           │           ┌───┤  on_start  ├────┐            │
/// │           │   │            │    │            │                         │           │   │            │    │            │
/// │           │   └────────────┘    │            │                         │           │   └────────────┘    │            │
/// │           │                     │            │                         │           │                     │            │
/// │           ▼                     ▼            │                         │           ▼                     ▼            │
/// │  ┌──────────────────┐ ┌───────────────────┐  │                         │  ┌──────────────────┐ ┌───────────────────┐  │
/// │  │                  │ │                   │  │                         │  │                  │ │                   │  │
/// │  │  KnnLogOperator  │ │  KnnHnswOperator  │  │           ...           │  │  KnnLogOperator  │ │  KnnHnswOperator  │  │
/// │  │                  │ │                   │  │                         │  │                  │ │                   │  │
/// │  └────────┬─────────┘ └─────────┬─────────┘  │                         │  └────────┬─────────┘ └─────────┬─────────┘  │
/// │           │                     │            │                         │           │                     │            │
/// │           ▼                     ▼            │                         │           ▼                     ▼            │
/// │      ┌────────────────────────────────┐      │                         │      ┌────────────────────────────────┐      │
/// │      │                                │      │                         │      │                                │      │
/// │      │  try_start_knn_merge_operator  │      │           ...           │      │  try_start_knn_merge_operator  │      │
/// │      │                                │      │                         │      │                                │      │
/// │      └───────────────┬────────────────┘      │                         │      └───────────────┬────────────────┘      │
/// │                      │                       │                         │                      │                       │
/// │                      ▼                       │                         │                      ▼                       │
/// │           ┌─────────────────────┐            │                         │           ┌─────────────────────┐            │
/// │           │                     │            │                         │           │                     │            │
/// │           │  KnnMergeOperator   │            │           ...           │           │  KnnMergeOperator   │            │
/// │           │                     │            │                         │           │                     │            │
/// │           └──────────┬──────────┘            │                         │           └──────────┬──────────┘            │
/// │                      │                       │                         │                      │                       │
/// │                      ▼                       │                         │                      ▼                       │
/// │         ┌─────────────────────────┐          │                         │         ┌─────────────────────────┐          │
/// │         │                         │          │                         │         │                         │          │
/// │         │  KnnProjectionOperator  │          │           ...           │         │  KnnProjectionOperator  │          │
/// │         │                         │          │                         │         │                         │          │
/// │         └────────────┬────────────┘          │                         │         └────────────┬────────────┘          │
/// │                      │                       │                         │                      │                       │
/// │                      ▼                       │                         │                      ▼                       │
/// │             ┌──────────────────┐             │                         │             ┌──────────────────┐             │
/// │             │                  │             │                         │             │                  │             │
/// │             │  result_channel  │             │           ...           │             │  result_channel  │             │
/// │             │                  │             │                         │             │                  │             │
/// │             └────────┬─────────┘             │                         │             └────────┬─────────┘             │
/// │                      │                       │                         │                      │                       │
/// └────────────────────  │  ─────────────────────┘                         └────────────────────  │  ─────────────────────┘
///                        │                                                                        │
///                        │                                                                        │
///                        │                                                                        │
///                        │                           ┌────────────────┐                           │
///                        │                           │                │                           │
///                        └──────────────────────────►│  try_join_all  │◄──────────────────────────┘
///                                                    │                │
///                                                    └───────┬────────┘
///                                                            │
///                                                            │
///                                                            ▼
/// ```
///
/// # State tracking
/// Similar to the `GetOrchestrator`, the `KnnFilterOrchestrator` need to keep track of the outputs from
/// `FetchLogOperator` and `FetchSegmentOperator`. For `KnnOrchestrator`, it needs to track the outputs from
/// `KnnLogOperator` and `KnnHnswOperator`. It invokes `try_start_knn_merge_operator` when it receives outputs
/// from either operators, and if both outputs are present it composes the input for `KnnMergeOperator` and
/// proceeds with execution. The outputs of other operators are directly forwarded without being tracked
/// by the orchestrator.

#[derive(Error, Debug)]
pub enum KnnError {
    #[error("Error sending message through channel: {0}")]
    Channel(#[from] ChannelError),
    #[error("Error instantiating distance function: {0}")]
    DistanceFunction(#[from] DistanceFunctionError),
    #[error("Empty collection")]
    EmptyCollection,
    #[error("Error running Fetch Log Operator: {0}")]
    FetchLog(#[from] FetchLogError),
    #[error("Error running Fetch Segment Operator: {0}")]
    FetchSegment(#[from] FetchSegmentError),
    #[error("Error running Filter Operator: {0}")]
    Filter(#[from] FilterError),
    #[error("Error running Knn Log Operator: {0}")]
    KnnLog(#[from] KnnLogError),
    #[error("Error running Knn Hnsw Operator: {0}")]
    KnnHnsw(#[from] KnnHnswError),
    #[error("Error running Knn Projection Operator: {0}")]
    KnnProjection(#[from] KnnProjectionError),
    #[error("Error inspecting collection dimension")]
    NoCollectionDimension,
    #[error("Panic running task: {0}")]
    Panic(String),
    #[error("Error receiving final result: {0}")]
    Result(#[from] RecvError),
}

impl ChromaError for KnnError {
    fn code(&self) -> ErrorCodes {
        match self {
            KnnError::Channel(e) => e.code(),
            KnnError::DistanceFunction(e) => e.code(),
            KnnError::EmptyCollection => ErrorCodes::Internal,
            KnnError::FetchLog(e) => e.code(),
            KnnError::FetchSegment(e) => e.code(),
            KnnError::Filter(e) => e.code(),
            KnnError::KnnLog(e) => e.code(),
            KnnError::KnnHnsw(e) => e.code(),
            KnnError::KnnProjection(e) => e.code(),
            KnnError::NoCollectionDimension => ErrorCodes::InvalidArgument,
            KnnError::Panic(_) => ErrorCodes::Aborted,
            KnnError::Result(_) => ErrorCodes::Internal,
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
    pub segments: FetchSegmentOutput,
    pub filter_output: FilterOutput,
}

type KnnFilterResult = Result<KnnFilterOutput, KnnError>;

#[derive(Debug)]
pub struct KnnFilterOrchestrator {
    // Orchestrator parameters
    blockfile_provider: BlockfileProvider,
    dispatcher: ComponentHandle<Dispatcher>,
    queue: usize,

    // Fetch logs and segments
    fetch_log: FetchLogOperator,
    fetch_segment: FetchSegmentOperator,

    // Fetch output
    fetch_log_output: Option<FetchLogOutput>,
    fetch_segment_output: Option<FetchSegmentOutput>,

    // Pipelined operators
    filter: FilterOperator,

    // Result channel
    result_channel: Option<Sender<KnnFilterResult>>,
}

impl KnnFilterOrchestrator {
    pub fn new(
        blockfile_provider: BlockfileProvider,
        dispatcher: ComponentHandle<Dispatcher>,
        queue: usize,
        fetch_log: FetchLogOperator,
        fetch_segment: FetchSegmentOperator,
        filter: FilterOperator,
    ) -> Self {
        Self {
            blockfile_provider,
            dispatcher,
            queue,
            fetch_log,
            fetch_segment,
            fetch_log_output: None,
            fetch_segment_output: None,
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

    async fn try_start_filter_operator(&mut self, ctx: &ComponentContext<Self>) {
        if let (Some(logs), Some(segments)) = (
            self.fetch_log_output.as_ref(),
            self.fetch_segment_output.as_ref(),
        ) {
            let task = wrap(
                Box::new(self.filter.clone()),
                FilterInput {
                    logs: logs.clone(),
                    blockfile_provider: self.blockfile_provider.clone(),
                    metadata_segment: segments.metadata_segment.clone(),
                    record_segment: segments.record_segment.clone(),
                },
                ctx.receiver(),
            );
            if let Err(err) = self.dispatcher.send(task, Some(Span::current())).await {
                self.terminate_with_error(ctx, err);
            }
        }
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
        let log_task = wrap(Box::new(self.fetch_log.clone()), (), ctx.receiver());
        let segment_task = wrap(Box::new(self.fetch_segment.clone()), (), ctx.receiver());
        if let Err(err) = self.dispatcher.send(log_task, Some(Span::current())).await {
            self.terminate_with_error(ctx, err);
        } else if let Err(err) = self
            .dispatcher
            .send(segment_task, Some(Span::current()))
            .await
        {
            self.terminate_with_error(ctx, err);
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
        self.fetch_log_output = Some(output);
        self.try_start_filter_operator(ctx).await;
    }
}

#[async_trait]
impl Handler<TaskResult<FetchSegmentOutput, FetchSegmentError>> for KnnFilterOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<FetchSegmentOutput, FetchSegmentError>,
        ctx: &ComponentContext<Self>,
    ) {
        let output = match message.into_inner() {
            Ok(output) => output,
            Err(err) => {
                self.terminate_with_error(ctx, err);
                return;
            }
        };

        // If dimension is not set and segment is uninitialized,  we assume
        // this is a query on empty collection, so we return early here
        if output.collection.dimension.is_none() && output.vector_segment.file_path.is_empty() {
            self.terminate_with_error(ctx, KnnError::EmptyCollection);
            return;
        }

        self.fetch_segment_output = Some(output);
        self.try_start_filter_operator(ctx).await;
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
        if let Some(chan) = self.result_channel.take() {
            if chan
                .send(Ok(KnnFilterOutput {
                    logs: self
                        .fetch_log_output
                        .take()
                        .expect("FetchLogOperator should have finished already"),
                    segments: self
                        .fetch_segment_output
                        .take()
                        .expect("FetchSegmentOperator should have finished already"),
                    filter_output: output,
                }))
                .is_err()
            {
                tracing::error!("Error sending final result");
            };
        }
    }
}

type KnnOutput = KnnProjectionOutput;
type KnnResult = Result<KnnOutput, KnnError>;

#[derive(Debug)]
pub struct KnnOrchestrator {
    // Orchestrator parameters
    blockfile_provider: BlockfileProvider,
    dispatcher: ComponentHandle<Dispatcher>,
    hnsw_provider: HnswIndexProvider,
    queue: usize,

    // Output from KnnFilterOrchestrator
    knn_filter_output: KnnFilterOutput,

    // Knn operator shared between log and segments
    knn: KnnOperator,

    // Knn output
    knn_log_distances: Option<Vec<RecordDistance>>,
    knn_segment_distances: Option<Vec<RecordDistance>>,

    // Merge and project
    merge: KnnMergeOperator,
    knn_projection: KnnProjectionOperator,

    // Result channel
    result_channel: Option<Sender<KnnResult>>,
}

impl KnnOrchestrator {
    pub fn new(
        blockfile_provider: BlockfileProvider,
        dispatcher: ComponentHandle<Dispatcher>,
        hnsw_provider: HnswIndexProvider,
        queue: usize,
        knn_filter_output: KnnFilterOutput,
        knn: KnnOperator,
        knn_projection: KnnProjectionOperator,
    ) -> Self {
        let fetch = knn.fetch;
        Self {
            blockfile_provider,
            dispatcher,
            hnsw_provider,
            queue,
            knn_filter_output,
            knn,
            knn_log_distances: None,
            knn_segment_distances: None,
            merge: KnnMergeOperator { fetch },
            knn_projection,
            result_channel: None,
        }
    }

    pub async fn run(mut self, system: System) -> KnnResult {
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

    async fn try_start_knn_merge_operator(&mut self, ctx: &ComponentContext<Self>) {
        if let (Some(log_distances), Some(segment_distances)) = (
            self.knn_log_distances.as_ref(),
            self.knn_segment_distances.as_ref(),
        ) {
            let task = wrap(
                Box::new(self.merge.clone()),
                KnnMergeInput {
                    first_distances: log_distances.clone(),
                    second_distances: segment_distances.clone(),
                },
                ctx.receiver(),
            );
            if let Err(err) = self.dispatcher.send(task, Some(Span::current())).await {
                self.terminate_with_error(ctx, err);
            }
        }
    }
}

#[async_trait]
impl Component for KnnOrchestrator {
    fn get_name() -> &'static str {
        "Knn Orchestrator"
    }

    fn queue_size(&self) -> usize {
        self.queue
    }

    async fn on_start(&mut self, ctx: &ComponentContext<Self>) {
        let space = match self
            .knn_filter_output
            .segments
            .vector_segment
            .metadata
            .as_ref()
        {
            Some(metadata) => match metadata.get("hnsw:space") {
                Some(MetadataValue::Str(space)) => space,
                _ => "l2",
            },
            None => "l2",
        };
        let distance_function = match DistanceFunction::try_from(space) {
            Ok(func) => func,
            Err(err) => {
                self.terminate_with_error(ctx, err);
                return;
            }
        };

        let knn_log_task = wrap(
            Box::new(self.knn.clone()),
            KnnLogInput {
                logs: self.knn_filter_output.logs.clone(),
                blockfile_provider: self.blockfile_provider.clone(),
                record_segment: self.knn_filter_output.segments.record_segment.clone(),
                log_offset_ids: self.knn_filter_output.filter_output.log_offset_ids.clone(),
                distance_function: distance_function.clone(),
            },
            ctx.receiver(),
        );
        if let Err(err) = self
            .dispatcher
            .send(knn_log_task, Some(Span::current()))
            .await
        {
            self.terminate_with_error(ctx, err);
            return;
        }

        let collection_dimension = match self.knn_filter_output.segments.collection.dimension {
            Some(dimension) => dimension as u32,
            None => {
                self.terminate_with_error(ctx, KnnError::NoCollectionDimension);
                return;
            }
        };

        let knn_segment_task = wrap(
            Box::new(self.knn.clone()),
            KnnHnswInput {
                hnsw_provider: self.hnsw_provider.clone(),
                hnsw_segment: self.knn_filter_output.segments.vector_segment.clone(),
                collection_dimension,
                compact_offset_ids: self
                    .knn_filter_output
                    .filter_output
                    .compact_offset_ids
                    .clone(),
                distance_function,
            },
            ctx.receiver(),
        );

        if let Err(err) = self
            .dispatcher
            .send(knn_segment_task, Some(Span::current()))
            .await
        {
            self.terminate_with_error(ctx, err);
        }
    }
}

#[async_trait]
impl Handler<TaskResult<KnnLogOutput, KnnLogError>> for KnnOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<KnnLogOutput, KnnLogError>,
        ctx: &ComponentContext<Self>,
    ) {
        let output = match message.into_inner() {
            Ok(output) => output,
            Err(err) => {
                self.terminate_with_error(ctx, err);
                return;
            }
        };
        self.knn_log_distances = Some(output.record_distances);
        self.try_start_knn_merge_operator(ctx).await;
    }
}

#[async_trait]
impl Handler<TaskResult<KnnHnswOutput, KnnHnswError>> for KnnOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<KnnHnswOutput, KnnHnswError>,
        ctx: &ComponentContext<Self>,
    ) {
        let output = match message.into_inner() {
            Ok(output) => output,
            Err(err) => {
                self.terminate_with_error(ctx, err);
                return;
            }
        };
        self.knn_segment_distances = Some(output.record_distances);
        self.try_start_knn_merge_operator(ctx).await;
    }
}

#[async_trait]
impl Handler<TaskResult<KnnMergeOutput, KnnMergeError>> for KnnOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<KnnMergeOutput, KnnMergeError>,
        ctx: &ComponentContext<Self>,
    ) {
        let output = message
            .into_inner()
            .expect("KnnMergeOperator should not fail");

        // Prefetch records before projection
        let prefetch_task = wrap(
            Box::new(PrefetchRecordOperator {}),
            PrefetchRecordInput {
                logs: self.knn_filter_output.logs.clone(),
                blockfile_provider: self.blockfile_provider.clone(),
                record_segment: self.knn_filter_output.segments.record_segment.clone(),
                offset_ids: output
                    .record_distances
                    .iter()
                    .map(|record| record.offset_id)
                    .collect(),
            },
            ctx.receiver(),
        );
        if let Err(err) = self
            .dispatcher
            .send(prefetch_task, Some(Span::current()))
            .await
        {
            self.terminate_with_error(ctx, err);
        }

        let projection_task = wrap(
            Box::new(self.knn_projection.clone()),
            KnnProjectionInput {
                logs: self.knn_filter_output.logs.clone(),
                blockfile_provider: self.blockfile_provider.clone(),
                record_segment: self.knn_filter_output.segments.record_segment.clone(),
                record_distances: output.record_distances,
            },
            ctx.receiver(),
        );
        if let Err(err) = self
            .dispatcher
            .send(projection_task, Some(Span::current()))
            .await
        {
            self.terminate_with_error(ctx, err);
        }
    }
}

#[async_trait]
impl Handler<TaskResult<PrefetchRecordOutput, PrefetchRecordError>> for KnnOrchestrator {
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
impl Handler<TaskResult<KnnProjectionOutput, KnnProjectionError>> for KnnOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<KnnProjectionOutput, KnnProjectionError>,
        ctx: &ComponentContext<Self>,
    ) {
        let output = match message.into_inner() {
            Ok(output) => output,
            Err(err) => {
                self.terminate_with_error(ctx, err);
                return;
            }
        };
        if let Some(chan) = self.result_channel.take() {
            if chan.send(Ok(output)).is_err() {
                tracing::error!("Error sending final result");
            };
        }
    }
}
