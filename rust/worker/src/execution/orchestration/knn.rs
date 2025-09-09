use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_system::{
    wrap, ComponentContext, ComponentHandle, Dispatcher, Handler, Orchestrator,
    OrchestratorContext, TaskMessage, TaskResult,
};
use chroma_types::{
    operator::{Knn, KnnOutput, Merge, RecordMeasure},
    CollectionAndSegments,
};
use tokio::sync::oneshot::Sender;
use tracing::Span;

use crate::execution::operators::{
    knn_hnsw::{KnnHnswError, KnnHnswInput},
    knn_log::{KnnLogError, KnnLogInput},
    knn_merge::{KnnMergeError, KnnMergeInput, KnnMergeOutput},
};

use super::knn_filter::{KnnError, KnnFilterOutput};

/// The `KnnOrchestrator` finds the nearest neighbor of a target embedding given the search domain.
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
pub struct KnnOrchestrator {
    // Orchestrator parameters
    context: OrchestratorContext,
    blockfile_provider: BlockfileProvider,
    queue: usize,

    // Collection information
    collection_and_segments: CollectionAndSegments,

    // Output from KnnFilterOrchestrator
    knn_filter_output: KnnFilterOutput,

    // Knn operator shared between log and segments
    knn: Knn,

    // Knn output
    batch_distances: Vec<Vec<RecordMeasure>>,

    // Merge
    merge: Merge,

    // Result channel
    result_channel: Option<Sender<Result<Vec<RecordMeasure>, KnnError>>>,
}

impl KnnOrchestrator {
    pub fn new(
        blockfile_provider: BlockfileProvider,
        dispatcher: ComponentHandle<Dispatcher>,
        queue: usize,
        collection_and_segments: CollectionAndSegments,
        knn_filter_output: KnnFilterOutput,
        knn: Knn,
    ) -> Self {
        let fetch = knn.fetch;
        let batch_distances = if knn_filter_output.hnsw_reader.is_none() {
            vec![Vec::new()]
        } else {
            Vec::new()
        };
        let context = OrchestratorContext::new(dispatcher);
        Self {
            context,
            blockfile_provider,
            queue,
            collection_and_segments,
            knn_filter_output,
            knn,
            batch_distances,
            merge: Merge { k: fetch },
            result_channel: None,
        }
    }

    async fn try_start_knn_merge_operator(&mut self, ctx: &ComponentContext<Self>) {
        if self.batch_distances.len() == 2 {
            let task = wrap(
                Box::new(self.merge.clone()),
                KnnMergeInput {
                    batch_measures: self.batch_distances.drain(..).collect(),
                },
                ctx.receiver(),
                self.context.task_cancellation_token.clone(),
            );
            self.send(task, ctx, Some(Span::current())).await;
        }
    }
}

#[async_trait]
impl Orchestrator for KnnOrchestrator {
    type Output = Vec<RecordMeasure>;
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
        let mut tasks = Vec::new();

        let knn_log_task = wrap(
            Box::new(self.knn.clone()),
            KnnLogInput {
                logs: self.knn_filter_output.logs.clone(),
                blockfile_provider: self.blockfile_provider.clone(),
                record_segment: self.collection_and_segments.record_segment.clone(),
                log_offset_ids: self.knn_filter_output.filter_output.log_offset_ids.clone(),
                distance_function: self.knn_filter_output.distance_function.clone(),
            },
            ctx.receiver(),
            self.context.task_cancellation_token.clone(),
        );
        tasks.push((knn_log_task, Some(Span::current())));

        if let Some(hnsw_reader) = self.knn_filter_output.hnsw_reader.as_ref().cloned() {
            let knn_segment_task = wrap(
                Box::new(self.knn.clone()),
                KnnHnswInput {
                    hnsw_reader,
                    compact_offset_ids: self
                        .knn_filter_output
                        .filter_output
                        .compact_offset_ids
                        .clone(),
                    distance_function: self.knn_filter_output.distance_function.clone(),
                },
                ctx.receiver(),
                self.context.task_cancellation_token.clone(),
            );
            tasks.push((knn_segment_task, Some(Span::current())));
        }

        tasks
    }

    fn queue_size(&self) -> usize {
        self.queue
    }

    fn set_result_channel(&mut self, sender: Sender<Result<Vec<RecordMeasure>, KnnError>>) {
        self.result_channel = Some(sender)
    }

    fn take_result_channel(&mut self) -> Option<Sender<Result<Vec<RecordMeasure>, KnnError>>> {
        self.result_channel.take()
    }
}

#[async_trait]
impl Handler<TaskResult<KnnOutput, KnnLogError>> for KnnOrchestrator {
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
impl Handler<TaskResult<KnnOutput, KnnHnswError>> for KnnOrchestrator {
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
impl Handler<TaskResult<KnnMergeOutput, KnnMergeError>> for KnnOrchestrator {
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

        self.terminate_with_result(Ok(output.measures), ctx).await;
    }
}
