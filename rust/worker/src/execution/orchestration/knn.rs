use chroma_blockstore::provider::BlockfileProvider;
use tokio::sync::oneshot::{self, Sender};
use tonic::async_trait;
use tracing::Span;

use crate::{
    execution::{
        dispatcher::Dispatcher,
        operator::{wrap, TaskResult},
        operators::{
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
    system::{Component, ComponentContext, ComponentHandle, Handler, System},
};

use super::knn_filter::{KnnError, KnnFilterOutput, KnnResult};

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
        Self {
            blockfile_provider,
            dispatcher,
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
        if let Err(err) = self
            .dispatcher
            .send(knn_log_task, Some(Span::current()))
            .await
        {
            self.terminate_with_error(ctx, err);
            return;
        }

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

            if let Err(err) = self
                .dispatcher
                .send(knn_segment_task, Some(Span::current()))
                .await
            {
                self.terminate_with_error(ctx, err);
            }
        } else {
            self.knn_segment_distances = Some(Vec::new())
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
                record_segment: self.knn_filter_output.record_segment.clone(),
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
                record_segment: self.knn_filter_output.record_segment.clone(),
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
