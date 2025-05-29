use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_system::{
    wrap, ChannelError, ComponentContext, ComponentHandle, Dispatcher, Handler, Orchestrator,
    PanicError, TaskError, TaskMessage, TaskResult,
};
use chroma_types::operator::{
    Knn, KnnMerge, KnnOutput, KnnProjection, KnnProjectionOutput, RecordDistance,
};
use thiserror::Error;
use tokio::sync::oneshot::{error::RecvError, Sender};
use tracing::Span;

use crate::execution::operators::{
    knn_hnsw::{KnnHnswError, KnnHnswInput},
    knn_log::{KnnLogError, KnnLogInput},
    knn_merge::{KnnMergeError, KnnMergeInput, KnnMergeOutput},
    knn_projection::{KnnProjectionError, KnnProjectionInput},
    prefetch_record::{
        PrefetchRecordError, PrefetchRecordInput, PrefetchRecordOperator, PrefetchRecordOutput,
    },
};

use super::filter::FilterOrchestratorOutput;

#[derive(Error, Debug)]
pub enum KnnHnswOrchestratorError {
    #[error("Operation aborted because resources exhausted")]
    Aborted,
    #[error("Error sending message through channel: {0}")]
    Channel(#[from] ChannelError),
    #[error("Error running Knn Hnsw Operator: {0}")]
    KnnHnsw(#[from] KnnHnswError),
    #[error("Error running Knn Log Operator: {0}")]
    KnnLog(#[from] KnnLogError),
    #[error("Error running Knn Merge Operator")]
    KnnMerge(#[from] KnnMergeError),
    #[error("Error running Knn Projection Operator: {0}")]
    KnnProjection(#[from] KnnProjectionError),
    #[error("Panic: {0}")]
    Panic(#[from] PanicError),
    #[error("Error receiving final result: {0}")]
    Result(#[from] RecvError),
}

impl ChromaError for KnnHnswOrchestratorError {
    fn code(&self) -> ErrorCodes {
        match self {
            KnnHnswOrchestratorError::Aborted => ErrorCodes::ResourceExhausted,
            KnnHnswOrchestratorError::Channel(e) => e.code(),
            KnnHnswOrchestratorError::KnnHnsw(e) => e.code(),
            KnnHnswOrchestratorError::KnnLog(e) => e.code(),
            KnnHnswOrchestratorError::KnnMerge(_) => ErrorCodes::Internal,
            KnnHnswOrchestratorError::KnnProjection(e) => e.code(),
            KnnHnswOrchestratorError::Panic(_) => ErrorCodes::Aborted,
            KnnHnswOrchestratorError::Result(_) => ErrorCodes::Internal,
        }
    }
}

impl<E> From<TaskError<E>> for KnnHnswOrchestratorError
where
    E: Into<KnnHnswOrchestratorError>,
{
    fn from(value: TaskError<E>) -> Self {
        match value {
            TaskError::Aborted => KnnHnswOrchestratorError::Aborted,
            TaskError::Panic(e) => e.into(),
            TaskError::TaskFailed(e) => e.into(),
        }
    }
}

/// The `KnnHnswOrchestrator` finds the nearest neighbor of a target embedding given the search domain.
/// When used together with `FilterOrchestrator`, they evaluate a `<collection>.query(...)` query
/// for the user. We breakdown the evaluation into two parts because a `<collection>.query(...)`
/// is inherently multiple queries sharing the same filter criteria. Thus we first evaluate
/// the filter criteria with `FilterOrchestrator`. Then we spawn a `KnnOrchestrator` for each
/// of the embedding together with a copy of the result from `FilterOrchestrator`, run these
/// orchestrators in parallel, and join them in the end.
///
///
/// # Pipeline
/// ```text
///                                                           │
///                                                           │
///                                                           │
///                                                           │
///                                                           ▼
///                                               ┌───────────────────────┐
///                                               │                       │
///                                               │   FilterOrchestrator  │
///                                               │                       │
///                                               └───────────┬───────────┘
///                                                           │
///                                                           │
///                                                           │
///                        ┌──────────────────────────────────┴─────────────────────────────────────┐
///                        │                                                                        │
///                        │                    ... One branch per embedding ...                    │
///                        │                                                                        │
/// ┌────────────────────  │  ─────────────────────┐                         ┌────────────────────  │  ─────────────────────┐
/// │                      ▼                       │                         │                      ▼                       │
/// │               ┌────────────┐     KnnHnsw     │                         │               ┌────────────┐     KnnHnsw     │
/// │               │            │   Orchestrator  │                         │               │            │   Orchestrator  │
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
#[derive(Debug)]
pub struct KnnHnswOrchestrator {
    // Orchestrator parameters
    blockfile_provider: BlockfileProvider,
    dispatcher: ComponentHandle<Dispatcher>,
    queue: usize,

    // Output from FilterOrchestrator
    filter_output: FilterOrchestratorOutput,

    // Knn operator shared between log and segments
    knn: Knn,

    // Knn output
    batch_distances: Vec<Vec<RecordDistance>>,

    // Merge and project
    merge: KnnMerge,
    knn_projection: KnnProjection,

    // Result channel
    result_channel: Option<Sender<Result<KnnProjectionOutput, KnnHnswOrchestratorError>>>,
}

impl KnnHnswOrchestrator {
    pub fn new(
        blockfile_provider: BlockfileProvider,
        dispatcher: ComponentHandle<Dispatcher>,
        queue: usize,
        filter_output: FilterOrchestratorOutput,
        knn: Knn,
        knn_projection: KnnProjection,
    ) -> Self {
        let fetch = knn.fetch;
        let batch_distances = if filter_output.hnsw_reader.is_none() {
            vec![Vec::new()]
        } else {
            Vec::new()
        };
        Self {
            blockfile_provider,
            dispatcher,
            queue,
            filter_output,
            knn,
            batch_distances,
            merge: KnnMerge { fetch },
            knn_projection,
            result_channel: None,
        }
    }

    async fn try_start_knn_merge_operator(&mut self, ctx: &ComponentContext<Self>) {
        if self.batch_distances.len() == 2 {
            let task = wrap(
                Box::new(self.merge.clone()),
                KnnMergeInput {
                    batch_distances: self.batch_distances.drain(..).collect(),
                },
                ctx.receiver(),
            );
            self.send(task, ctx, Some(Span::current())).await;
        }
    }
}

#[async_trait]
impl Orchestrator for KnnHnswOrchestrator {
    type Output = KnnProjectionOutput;
    type Error = KnnHnswOrchestratorError;

    fn dispatcher(&self) -> ComponentHandle<Dispatcher> {
        self.dispatcher.clone()
    }

    async fn initial_tasks(
        &mut self,
        ctx: &ComponentContext<Self>,
    ) -> Vec<(TaskMessage, Option<Span>)> {
        let mut tasks = Vec::new();

        let knn_log_task = wrap(
            Box::new(self.knn.clone()),
            KnnLogInput {
                logs: self.filter_output.logs.clone(),
                blockfile_provider: self.blockfile_provider.clone(),
                record_segment: self.filter_output.record_segment.clone(),
                log_offset_ids: self.filter_output.filter_output.log_offset_ids.clone(),
                distance_function: self.filter_output.distance_function.clone(),
            },
            ctx.receiver(),
        );
        tasks.push((knn_log_task, Some(Span::current())));

        if let Some(hnsw_reader) = self.filter_output.hnsw_reader.as_ref().cloned() {
            let knn_segment_task = wrap(
                Box::new(self.knn.clone()),
                KnnHnswInput {
                    hnsw_reader,
                    compact_offset_ids: self.filter_output.filter_output.compact_offset_ids.clone(),
                    distance_function: self.filter_output.distance_function.clone(),
                },
                ctx.receiver(),
            );
            tasks.push((knn_segment_task, Some(Span::current())));
        }

        tasks
    }

    fn queue_size(&self) -> usize {
        self.queue
    }

    fn set_result_channel(
        &mut self,
        sender: Sender<Result<KnnProjectionOutput, KnnHnswOrchestratorError>>,
    ) {
        self.result_channel = Some(sender)
    }

    fn take_result_channel(
        &mut self,
    ) -> Sender<Result<KnnProjectionOutput, KnnHnswOrchestratorError>> {
        self.result_channel
            .take()
            .expect("The result channel should be set before take")
    }
}

#[async_trait]
impl Handler<TaskResult<KnnOutput, KnnLogError>> for KnnHnswOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<KnnOutput, KnnLogError>,
        ctx: &ComponentContext<Self>,
    ) {
        let output = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(output) => output,
            None => return,
        };
        self.batch_distances.push(output.distances);
        self.try_start_knn_merge_operator(ctx).await;
    }
}

#[async_trait]
impl Handler<TaskResult<KnnOutput, KnnHnswError>> for KnnHnswOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<KnnOutput, KnnHnswError>,
        ctx: &ComponentContext<Self>,
    ) {
        let output = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(output) => output,
            None => return,
        };
        self.batch_distances.push(output.distances);
        self.try_start_knn_merge_operator(ctx).await;
    }
}

#[async_trait]
impl Handler<TaskResult<KnnMergeOutput, KnnMergeError>> for KnnHnswOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<KnnMergeOutput, KnnMergeError>,
        ctx: &ComponentContext<Self>,
    ) {
        let output = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(output) => output,
            None => return,
        };

        // Prefetch records before projection
        let prefetch_task = wrap(
            Box::new(PrefetchRecordOperator {}),
            PrefetchRecordInput {
                logs: self.filter_output.logs.clone(),
                blockfile_provider: self.blockfile_provider.clone(),
                record_segment: self.filter_output.record_segment.clone(),
                offset_ids: output
                    .distances
                    .iter()
                    .map(|record| record.offset_id)
                    .collect(),
            },
            ctx.receiver(),
        );
        // Prefetch span is detached from the orchestrator.
        let prefetch_span = tracing::info_span!(parent: None, "Prefetch_record", num_records = output.distances.len());
        self.send(prefetch_task, ctx, Some(prefetch_span)).await;

        let projection_task = wrap(
            Box::new(self.knn_projection.clone()),
            KnnProjectionInput {
                logs: self.filter_output.logs.clone(),
                blockfile_provider: self.blockfile_provider.clone(),
                record_segment: self.filter_output.record_segment.clone(),
                record_distances: output.distances,
            },
            ctx.receiver(),
        );
        self.send(projection_task, ctx, Some(Span::current())).await;
    }
}

#[async_trait]
impl Handler<TaskResult<PrefetchRecordOutput, PrefetchRecordError>> for KnnHnswOrchestrator {
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
impl Handler<TaskResult<KnnProjectionOutput, KnnProjectionError>> for KnnHnswOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<KnnProjectionOutput, KnnProjectionError>,
        ctx: &ComponentContext<Self>,
    ) {
        self.terminate_with_result(message.into_inner().map_err(|e| e.into()), ctx)
            .await;
    }
}
