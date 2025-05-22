use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_system::{
    wrap, ComponentContext, ComponentHandle, Dispatcher, Handler, Orchestrator, TaskMessage,
    TaskResult,
};
use tokio::sync::oneshot::Sender;
use tracing::Span;

use crate::execution::operators::{
    knn::{KnnOperator, RecordDistance},
    knn_hnsw::{KnnHnswError, KnnHnswInput, KnnHnswOutput},
    knn_log::{KnnLogError, KnnLogInput, KnnLogOutput},
    knn_merge::{KnnMergeError, KnnMergeInput, KnnMergeOperator, KnnMergeOutput},
    knn_projection::{
        KnnProjectionError, KnnProjectionInput, KnnProjectionOperator, KnnProjectionOutput,
    },
    prefetch_record::{
        PrefetchRecordError, PrefetchRecordInput, PrefetchRecordOperator, PrefetchRecordOutput,
    },
};

use super::knn_filter::{KnnError, KnnFilterOutput, KnnOutput, KnnResult};

/// The `KnnOrchestrator` finds the nearest neighbor of a target embedding given the search domain.
/// When used together with `KnnFilterOrchestrator`, they evaluate a `<collection>.query(...)` query
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
///                                                           │
///                                                           ▼
///                                               ┌───────────────────────┐
///                                               │                       │
///                                               │ KnnFilterOrchestrator │
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
pub struct KnnOrchestrator {
    // Orchestrator parameters
    blockfile_provider: BlockfileProvider,
    dispatcher: ComponentHandle<Dispatcher>,
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
        queue: usize,
        knn_filter_output: KnnFilterOutput,
        knn: KnnOperator,
        knn_projection: KnnProjectionOperator,
    ) -> Self {
        let fetch = knn.fetch;
        let knn_segment_distances = if knn_filter_output.hnsw_reader.is_none() {
            Some(Vec::new())
        } else {
            None
        };
        Self {
            blockfile_provider,
            dispatcher,
            queue,
            knn_filter_output,
            knn,
            knn_log_distances: None,
            knn_segment_distances,
            merge: KnnMergeOperator { fetch },
            knn_projection,
            result_channel: None,
        }
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
            self.send(task, ctx, Some(Span::current())).await;
        }
    }
}

#[async_trait]
impl Orchestrator for KnnOrchestrator {
    type Output = KnnOutput;
    type Error = KnnError;

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
                logs: self.knn_filter_output.logs.clone(),
                blockfile_provider: self.blockfile_provider.clone(),
                record_segment: self.knn_filter_output.record_segment.clone(),
                log_offset_ids: self.knn_filter_output.filter_output.log_offset_ids.clone(),
                distance_function: self.knn_filter_output.distance_function.clone(),
            },
            ctx.receiver(),
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
            );
            tasks.push((knn_segment_task, Some(Span::current())));
        }

        tasks
    }

    fn queue_size(&self) -> usize {
        self.queue
    }

    fn set_result_channel(&mut self, sender: Sender<KnnResult>) {
        self.result_channel = Some(sender)
    }

    fn take_result_channel(&mut self) -> Sender<KnnResult> {
        self.result_channel
            .take()
            .expect("The result channel should be set before take")
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
        let output = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(output) => output,
            None => return,
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
        let output = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(output) => output,
            None => return,
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
        let output = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(output) => output,
            None => return,
        };

        // Prefetch records before projection
        let prefetch_task = wrap(
            Box::new(PrefetchRecordOperator {}),
            PrefetchRecordInput {
                logs: self.knn_filter_output.logs.clone(),
                blockfile_provider: self.blockfile_provider.clone(),
                record_segment: self.knn_filter_output.record_segment.clone(),
                offset_ids: output
                    .record_distances
                    .iter()
                    .map(|record| record.offset_id)
                    .collect(),
            },
            ctx.receiver(),
        );
        // Prefetch span is detached from the orchestrator.
        let prefetch_span = tracing::info_span!(parent: None, "Prefetch_record", num_records = output.record_distances.len());
        self.send(prefetch_task, ctx, Some(prefetch_span)).await;

        let projection_task = wrap(
            Box::new(self.knn_projection.clone()),
            KnnProjectionInput {
                logs: self.knn_filter_output.logs.clone(),
                blockfile_provider: self.blockfile_provider.clone(),
                record_segment: self.knn_filter_output.record_segment.clone(),
                record_distances: output.record_distances,
            },
            ctx.receiver(),
        );
        self.send(projection_task, ctx, Some(Span::current())).await;
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
        self.terminate_with_result(message.into_inner().map_err(|e| e.into()), ctx)
            .await;
    }
}
