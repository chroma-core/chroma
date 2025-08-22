use std::collections::HashMap;

use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_index::hnsw_provider::HnswIndexProvider;
use chroma_segment::{
    distributed_hnsw::{DistributedHNSWSegmentFromSegmentError, DistributedHNSWSegmentReader},
    spann_provider::SpannProvider,
};
use chroma_system::{
    wrap, ChannelError, ComponentContext, ComponentHandle, Dispatcher, Handler, Orchestrator,
    OrchestratorContext, PanicError, System, TaskError, TaskMessage, TaskResult,
};
use chroma_types::{
    operator::{Knn, KnnProjection, Projection, Rank, RecordDistance},
    plan::RetrievePayload,
    CollectionAndSegments, HnswParametersFromSegmentError, SegmentType,
};
use futures::future::try_join_all;
use thiserror::Error;
use tokio::sync::oneshot::{error::RecvError, Sender};
use tracing::Span;

use crate::execution::{
    operators::{
        fetch_log::{FetchLogError, FetchLogOperator, FetchLogOutput},
        filter::{FilterError, FilterInput, FilterOutput},
        materialize_logs::MaterializeLogOperatorError,
        prefetch_segment::{
            PrefetchSegmentError, PrefetchSegmentInput, PrefetchSegmentOperator,
            PrefetchSegmentOutput,
        },
        projection::ProjectionError,
        reverse_project::{ReverseProjection, ReverseProjectionInput, ReverseProjectionOutput},
        score::{ScoreError, ScoreInput, ScoreOutput},
        sparse_knn::{SparseKnn, SparseKnnError, SparseKnnInput, SparseKnnOutput},
    },
    orchestration::{
        knn::KnnOrchestrator,
        knn_filter::{KnnError, KnnFilterOutput},
        spann_knn::SpannKnnOrchestrator,
    },
};

#[derive(Debug)]
pub struct RetrieveOutput {
    pub debug: String,
}

#[derive(Debug, Error)]
pub enum RetrieveError {
    #[error("Operation aborted because resources exhausted")]
    Aborted,
    #[error("Error sending message through channel: {0}")]
    Channel(#[from] ChannelError),
    #[error("Error running FetchLog operator: {0}")]
    FetchLog(#[from] FetchLogError),
    #[error("Error running Filter operator: {0}")]
    Filter(#[from] FilterError),
    #[error("Error parsing Hnsw config: {0}")]
    HnswConfig(#[from] HnswParametersFromSegmentError),
    #[error("Error creating hnsw segment reader: {0}")]
    HnswReader(#[from] DistributedHNSWSegmentFromSegmentError),
    #[error("Invalid vector segment")]
    InvalidVectorSegment,
    #[error("Error running Knn operator: {0}")]
    Knn(#[from] KnnError),
    #[error("Error running MaterializeLog operator: {0}")]
    MaterializeLog(#[from] MaterializeLogOperatorError),
    #[error("Error inspecting collection dimension")]
    NoCollectionDimension,
    #[error("Panic: {0}")]
    Panic(#[from] PanicError),
    #[error("Error running Project operator: {0}")]
    Project(#[from] ProjectionError),
    #[error("Error receiving final result: {0}")]
    Result(#[from] RecvError),
    #[error("Error running Score operator: {0}")]
    Score(#[from] ScoreError),
    #[error("Error running SparseKnn operator: {0}")]
    SparseKnn(#[from] SparseKnnError),
}

impl ChromaError for RetrieveError {
    fn code(&self) -> ErrorCodes {
        match self {
            RetrieveError::Aborted => ErrorCodes::Aborted,
            RetrieveError::Channel(err) => err.code(),
            RetrieveError::FetchLog(err) => err.code(),
            RetrieveError::Filter(err) => err.code(),
            RetrieveError::HnswConfig(err) => err.code(),
            RetrieveError::HnswReader(err) => err.code(),
            RetrieveError::InvalidVectorSegment => ErrorCodes::InvalidArgument,
            RetrieveError::Knn(err) => err.code(),
            RetrieveError::MaterializeLog(err) => err.code(),
            RetrieveError::NoCollectionDimension => ErrorCodes::InvalidArgument,
            RetrieveError::Panic(err) => err.code(),
            RetrieveError::Project(err) => err.code(),
            RetrieveError::Result(_) => ErrorCodes::Internal,
            RetrieveError::Score(_) => ErrorCodes::Internal,
            RetrieveError::SparseKnn(err) => err.code(),
        }
    }
}

impl<E> From<TaskError<E>> for RetrieveError
where
    E: Into<RetrieveError>,
{
    fn from(value: TaskError<E>) -> Self {
        match value {
            TaskError::Panic(e) => e.into(),
            TaskError::TaskFailed(e) => e.into(),
            TaskError::Aborted => RetrieveError::Aborted,
        }
    }
}

type RetrieveResult = Result<RetrieveOutput, RetrieveError>;

#[derive(Debug)]
pub struct RetrieveOrchestrator {
    context: OrchestratorContext,
    queue: usize,
    spann_provider: SpannProvider,
    system: System,

    collection_and_segments: CollectionAndSegments,
    fetch_log: FetchLogOperator,
    retrieve: RetrievePayload,

    // TODO: Materialize once and share with all operators
    fetched_logs: Option<FetchLogOutput>,

    // TODO: Better state management, and flatten orchestrator
    rank_result: HashMap<Rank, Vec<RecordDistance>>,
    rank_task_count: usize,

    result_channel: Option<Sender<RetrieveResult>>,
}

impl RetrieveOrchestrator {
    pub fn new(
        dispatcher: ComponentHandle<Dispatcher>,
        queue: usize,
        spann_provider: SpannProvider,
        system: System,
        collection_and_segments: CollectionAndSegments,
        fetch_log: FetchLogOperator,
        retrieve: RetrievePayload,
    ) -> Self {
        Self {
            context: OrchestratorContext::new(dispatcher),
            queue,
            spann_provider,
            system,
            collection_and_segments,
            fetch_log,
            retrieve,
            fetched_logs: None,
            rank_result: HashMap::new(),
            rank_task_count: 0,
            result_channel: None,
        }
    }

    pub fn blockfile_provider(&self) -> BlockfileProvider {
        self.spann_provider.blockfile_provider.clone()
    }

    pub fn hnsw_provider(&self) -> HnswIndexProvider {
        self.spann_provider.hnsw_provider.clone()
    }

    pub async fn try_score(&mut self, ctx: &ComponentContext<Self>) {
        if self.rank_result.len() < self.rank_task_count {
            return;
        }
        let score = Box::new(self.retrieve.score.clone());
        let score_input = ScoreInput {
            blockfile_provider: self.blockfile_provider(),
            ranks: self.rank_result.clone(),
        };
        let task = wrap(
            score,
            score_input,
            ctx.receiver(),
            self.context.task_cancellation_token.clone(),
        );
        self.send(task, ctx, Some(Span::current())).await;
    }
}

#[async_trait]
impl Orchestrator for RetrieveOrchestrator {
    type Output = RetrieveOutput;
    type Error = RetrieveError;

    fn context(&self) -> &OrchestratorContext {
        &self.context
    }

    fn dispatcher(&self) -> ComponentHandle<Dispatcher> {
        self.context.dispatcher.clone()
    }

    async fn initial_tasks(
        &mut self,
        ctx: &ComponentContext<Self>,
    ) -> Vec<(TaskMessage, Option<Span>)> {
        let prefetch_metadata_task = wrap(
            Box::new(PrefetchSegmentOperator::new()),
            PrefetchSegmentInput::new(
                self.collection_and_segments.metadata_segment.clone(),
                self.blockfile_provider(),
            ),
            ctx.receiver(),
            self.context.task_cancellation_token.clone(),
        );

        let prefetch_vector_task = wrap(
            Box::new(PrefetchSegmentOperator::new()),
            PrefetchSegmentInput::new(
                self.collection_and_segments.vector_segment.clone(),
                self.blockfile_provider(),
            ),
            ctx.receiver(),
            self.context.task_cancellation_token.clone(),
        );

        let prefetch_record_task = wrap(
            Box::new(PrefetchSegmentOperator::new()),
            PrefetchSegmentInput::new(
                self.collection_and_segments.record_segment.clone(),
                self.blockfile_provider(),
            ),
            ctx.receiver(),
            self.context.task_cancellation_token.clone(),
        );

        let fetch_log_task = wrap(
            Box::new(self.fetch_log.clone()),
            (),
            ctx.receiver(),
            self.context.task_cancellation_token.clone(),
        );

        vec![
            (
                prefetch_metadata_task,
                Some(
                    tracing::info_span!(parent: None, "Prefetch metadata segment", segment_id = %self.collection_and_segments.metadata_segment.id),
                ),
            ),
            (
                prefetch_vector_task,
                Some(
                    tracing::info_span!(parent: None, "Prefetch vector segment", segment_id = %self.collection_and_segments.vector_segment.id),
                ),
            ),
            (
                prefetch_record_task,
                Some(
                    tracing::info_span!(parent: None, "Prefetch record segment", segment_id = %self.collection_and_segments.record_segment.id),
                ),
            ),
            (fetch_log_task, Some(Span::current())),
        ]
    }

    fn queue_size(&self) -> usize {
        self.queue
    }

    fn set_result_channel(&mut self, sender: Sender<RetrieveResult>) {
        self.result_channel = Some(sender)
    }

    fn take_result_channel(&mut self) -> Option<Sender<RetrieveResult>> {
        self.result_channel.take()
    }
}

#[async_trait]
impl Handler<TaskResult<PrefetchSegmentOutput, PrefetchSegmentError>> for RetrieveOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        _: TaskResult<PrefetchSegmentOutput, PrefetchSegmentError>,
        _: &ComponentContext<Self>,
    ) {
        // NOTE: Prefetch operator is considered terminal and there is nothing to do
    }
}

#[async_trait]
impl Handler<TaskResult<FetchLogOutput, FetchLogError>> for RetrieveOrchestrator {
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
            Box::new(self.retrieve.filter.clone()),
            FilterInput {
                logs: output,
                blockfile_provider: self.blockfile_provider().clone(),
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
impl Handler<TaskResult<FilterOutput, FilterError>> for RetrieveOrchestrator {
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

        // NOTE: As a temporary hack, we construct sub-orchestrators to find the KNN
        // according to the scoring expression. In the future we could flatten this.

        let collection_dimension = match self
            .ok_or_terminate(
                self.collection_and_segments
                    .collection
                    .dimension
                    .ok_or(RetrieveError::NoCollectionDimension),
                ctx,
            )
            .await
        {
            Some(dim) => dim as u32,
            None => return,
        };

        let (space, hnsw_reader) = match self.collection_and_segments.vector_segment.r#type {
            SegmentType::HnswDistributed => {
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
                let hnsw_reader = match DistributedHNSWSegmentReader::from_segment(
                    &self.collection_and_segments.collection,
                    &self.collection_and_segments.vector_segment,
                    collection_dimension as usize,
                    self.hnsw_provider(),
                )
                .await
                {
                    Ok(hnsw_reader) => Some(*hnsw_reader),
                    Err(err)
                        if matches!(
                            *err,
                            DistributedHNSWSegmentFromSegmentError::Uninitialized
                        ) =>
                    {
                        None
                    }

                    Err(err) => {
                        self.terminate_with_result(Err((*err).into()), ctx).await;
                        return;
                    }
                };
                (hnsw_configuration.space, hnsw_reader)
            }
            SegmentType::Spann => {
                match self
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
                    Some(params) => (params.space, None),
                    None => return,
                }
            }
            _ => {
                self.terminate_with_result(Err(RetrieveError::InvalidVectorSegment), ctx)
                    .await;
                return;
            }
        };

        let logs = self
            .fetched_logs
            .as_ref()
            .cloned()
            .expect("FetchLogOperator should have finished already");

        let fetch_log_bytes = logs.iter().map(|(l, _)| l.size_bytes()).sum();

        let knn_filter_output = KnnFilterOutput {
            logs: logs.clone(),
            distance_function: space.into(),
            filter_output: output,
            record_segment: self.collection_and_segments.record_segment.clone(),
            vector_segment: self.collection_and_segments.vector_segment.clone(),
            dimension: collection_dimension as usize,
            fetch_log_bytes,
        };
        let ranks = self.retrieve.score.ranks();
        self.rank_task_count = ranks.len();

        let mut dense_ranks = Vec::with_capacity(self.rank_task_count);
        let mut dense_knn_futures = Vec::with_capacity(self.rank_task_count);
        for rank in ranks {
            let rank_clone = rank.clone();
            match rank {
                Rank::DenseKnn {
                    embedding,
                    key: _,
                    limit,
                } => {
                    let knn_projection = KnnProjection {
                        projection: Projection::default(),
                        distance: true,
                    };
                    let dense_knn_future = match self.collection_and_segments.vector_segment.r#type
                    {
                        SegmentType::HnswDistributed => KnnOrchestrator::new(
                            self.blockfile_provider(),
                            self.dispatcher(),
                            self.queue,
                            hnsw_reader.clone(),
                            knn_filter_output.clone(),
                            Knn {
                                embedding,
                                fetch: limit,
                            },
                            knn_projection.clone(),
                        )
                        .run(self.system.clone()),
                        SegmentType::Spann => SpannKnnOrchestrator::new(
                            self.spann_provider.clone(),
                            self.dispatcher(),
                            self.queue,
                            self.collection_and_segments.collection.clone(),
                            knn_filter_output.clone(),
                            limit as usize,
                            embedding,
                            knn_projection.clone(),
                        )
                        .run(self.system.clone()),
                        _ => {
                            self.terminate_with_result(
                                Err(RetrieveError::InvalidVectorSegment),
                                ctx,
                            )
                            .await;
                            return;
                        }
                    };
                    dense_ranks.push(rank_clone);
                    dense_knn_futures.push(dense_knn_future);
                }
                Rank::SparseKnn {
                    embedding,
                    key,
                    limit,
                } => {
                    let sparse_knn = Box::new(SparseKnn {
                        embedding,
                        key,
                        limit,
                    });
                    let sparse_knn_input = SparseKnnInput {
                        blockfile_provider: self.blockfile_provider(),
                        distance_function: knn_filter_output.distance_function.clone(),
                        logs: knn_filter_output.logs.clone(),
                        mask: knn_filter_output.filter_output.clone(),
                        metadata_segment: self.collection_and_segments.metadata_segment.clone(),
                        record_segment: self.collection_and_segments.record_segment.clone(),
                    };
                    let task = wrap(
                        sparse_knn,
                        sparse_knn_input,
                        ctx.receiver(),
                        self.context.task_cancellation_token.clone(),
                    );

                    self.send(task, ctx, Some(Span::current())).await;
                }
            }
        }
        if !dense_knn_futures.is_empty() {
            let dense_knn_results = match self
                .ok_or_terminate(try_join_all(dense_knn_futures).await, ctx)
                .await
            {
                Some(results) => results,
                None => return,
            };
            let reverse_project_input = ReverseProjectionInput {
                logs: logs,
                blockfile_provider: self.blockfile_provider(),
                record_segment: self.collection_and_segments.record_segment.clone(),
                projection_outputs: dense_ranks.into_iter().zip(dense_knn_results).collect(),
            };
            let reverse_project = Box::new(ReverseProjection {});
            let task = wrap(
                reverse_project,
                reverse_project_input,
                ctx.receiver(),
                self.context.task_cancellation_token.clone(),
            );
            self.send(task, ctx, Some(Span::current())).await;
        }
        self.try_score(ctx).await;
    }
}

#[async_trait]
impl Handler<TaskResult<SparseKnnOutput, SparseKnnError>> for RetrieveOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<SparseKnnOutput, SparseKnnError>,
        ctx: &ComponentContext<Self>,
    ) {
        let output = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(output) => output,
            None => return,
        };

        self.rank_result.insert(output.rank, output.records);
        self.try_score(ctx).await;
    }
}

#[async_trait]
impl Handler<TaskResult<ReverseProjectionOutput, ProjectionError>> for RetrieveOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<ReverseProjectionOutput, ProjectionError>,
        ctx: &ComponentContext<Self>,
    ) {
        let output = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(output) => output,
            None => return,
        };

        self.rank_result.extend(output.rank_records);
        self.try_score(ctx).await;
    }
}

#[async_trait]
impl Handler<TaskResult<ScoreOutput, ScoreError>> for RetrieveOrchestrator {
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
        self.terminate_with_result(
            Ok(RetrieveOutput {
                debug: format!("{output:#?}"),
            }),
            ctx,
        )
        .await;
    }
}
