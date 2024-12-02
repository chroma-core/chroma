use chroma_blockstore::provider::BlockfileProvider;
use chroma_distance::{normalize, DistanceFunction};
use chroma_index::hnsw_provider::HnswIndexProvider;
use tokio::sync::oneshot::{self, Sender};
use tonic::async_trait;
use tracing::Span;

use crate::{
    execution::{
        dispatcher::Dispatcher,
        operator::{wrap, TaskResult},
        operators::{
            knn::{KnnOperator, RecordDistance},
            knn_log::{KnnLogError, KnnLogInput, KnnLogOutput},
            knn_projection::{
                KnnProjectionError, KnnProjectionInput, KnnProjectionOperator, KnnProjectionOutput,
            },
            prefetch_record::{
                PrefetchRecordError, PrefetchRecordInput, PrefetchRecordOperator,
                PrefetchRecordOutput,
            },
            spann_bf_pl::{SpannBfPlError, SpannBfPlInput, SpannBfPlOperator, SpannBfPlOutput},
            spann_centers_search::{
                SpannCentersSearchError, SpannCentersSearchInput, SpannCentersSearchOperator,
                SpannCentersSearchOutput,
            },
            spann_fetch_pl::{
                SpannFetchPlError, SpannFetchPlInput, SpannFetchPlOperator, SpannFetchPlOutput,
            },
            spann_knn_merge::{
                SpannKnnMergeError, SpannKnnMergeInput, SpannKnnMergeOperator, SpannKnnMergeOutput,
            },
        },
        orchestration::common::terminate_with_error,
    },
    segment::spann_segment::SpannSegmentReaderContext,
    system::{Component, ComponentContext, ComponentHandle, Handler, System},
};

use super::knn_filter::{KnnError, KnnFilterOutput, KnnResult};

// TODO(Sanket): Make these configurable.
const RNG_FACTOR: f32 = 1.0;
const QUERY_EPSILON: f32 = 10.0;
const NUM_PROBE: usize = 64;

#[derive(Debug)]
pub struct SpannKnnOrchestrator {
    // Orchestrator parameters
    blockfile_provider: BlockfileProvider,
    hnsw_provider: HnswIndexProvider,
    dispatcher: ComponentHandle<Dispatcher>,
    queue: usize,

    // Output from KnnFilterOrchestrator
    knn_filter_output: KnnFilterOutput,

    // Query params.
    k: usize,
    normalized_query_emb: Vec<f32>,

    // Knn operator for the log.
    log_knn: KnnOperator,
    // Spann segment knn operators.
    head_search: SpannCentersSearchOperator,
    fetch_pl: SpannFetchPlOperator,
    bf_pl: SpannBfPlOperator,

    // State tracking.
    heads_searched: bool,
    num_outstanding_bf_pl: usize,

    // Knn output
    records: Vec<Vec<RecordDistance>>,

    // Merge and project
    merge: SpannKnnMergeOperator,
    knn_projection: KnnProjectionOperator,

    // Result channel
    result_channel: Option<Sender<KnnResult>>,
    // TODO(Sanket): We can pass the spann segment reader
    // here instead of constructing it everywhere since it has an
    // overhead.
}

impl SpannKnnOrchestrator {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        blockfile_provider: BlockfileProvider,
        hnsw_provider: HnswIndexProvider,
        dispatcher: ComponentHandle<Dispatcher>,
        queue: usize,
        knn_filter_output: KnnFilterOutput,
        k: usize,
        query_embedding: Vec<f32>,
        knn_projection: KnnProjectionOperator,
    ) -> Self {
        let normalized_query_emb;
        if knn_filter_output.distance_function == DistanceFunction::Cosine {
            normalized_query_emb = normalize(&query_embedding);
        } else {
            normalized_query_emb = query_embedding;
        }
        Self {
            blockfile_provider,
            hnsw_provider,
            dispatcher,
            queue,
            knn_filter_output,
            k,
            normalized_query_emb: normalized_query_emb.clone(),
            log_knn: KnnOperator {
                embedding: normalized_query_emb,
                fetch: k as u32,
            },
            head_search: SpannCentersSearchOperator {},
            fetch_pl: SpannFetchPlOperator {},
            bf_pl: SpannBfPlOperator {},
            heads_searched: false,
            num_outstanding_bf_pl: 0,
            records: Vec::new(),
            merge: SpannKnnMergeOperator { k: k as u32 },
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
        if self.heads_searched && self.num_outstanding_bf_pl == 0 {
            // This is safe because self.records is only used once and that is during merge.
            // It is only pushed into until then and after merge never used.
            let records = std::mem::take(&mut self.records);
            let task = wrap(
                Box::new(self.merge.clone()),
                SpannKnnMergeInput { records },
                ctx.receiver(),
            );
            if let Err(err) = self.dispatcher.send(task, Some(Span::current())).await {
                self.terminate_with_error(ctx, err);
            }
        }
    }
}

#[async_trait]
impl Component for SpannKnnOrchestrator {
    fn get_name() -> &'static str {
        "Spann Knn Orchestrator"
    }

    fn queue_size(&self) -> usize {
        self.queue
    }

    async fn on_start(&mut self, ctx: &ComponentContext<Self>) {
        let knn_log_task = wrap(
            Box::new(self.log_knn.clone()),
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

        // Invoke Head search operator.
        let reader_context = SpannSegmentReaderContext {
            segment: self.knn_filter_output.vector_segment.clone(),
            blockfile_provider: self.blockfile_provider.clone(),
            hnsw_provider: self.hnsw_provider.clone(),
            dimension: self.knn_filter_output.dimension,
        };
        let head_search_task = wrap(
            Box::new(self.head_search.clone()),
            SpannCentersSearchInput {
                reader_context,
                normalized_query: self.normalized_query_emb.clone(),
                k: NUM_PROBE,
                rng_epsilon: QUERY_EPSILON,
                rng_factor: RNG_FACTOR,
                distance_function: self.knn_filter_output.distance_function.clone(),
            },
            ctx.receiver(),
        );

        if let Err(err) = self
            .dispatcher
            .send(head_search_task, Some(Span::current()))
            .await
        {
            self.terminate_with_error(ctx, err);
        }
    }
}

#[async_trait]
impl Handler<TaskResult<KnnLogOutput, KnnLogError>> for SpannKnnOrchestrator {
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
        self.records.push(output.record_distances);
        self.try_start_knn_merge_operator(ctx).await;
    }
}

#[async_trait]
impl Handler<TaskResult<SpannCentersSearchOutput, SpannCentersSearchError>>
    for SpannKnnOrchestrator
{
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<SpannCentersSearchOutput, SpannCentersSearchError>,
        ctx: &ComponentContext<Self>,
    ) {
        let output = match message.into_inner() {
            Ok(output) => output,
            Err(err) => {
                self.terminate_with_error(ctx, err);
                return;
            }
        };
        // Set state that is used for tracking when we are ready for merging.
        self.heads_searched = true;
        self.num_outstanding_bf_pl = output.center_ids.len();
        // Spawn fetch posting list tasks for the centers.
        for head_id in output.center_ids {
            // Invoke Head search operator.
            let reader_context = SpannSegmentReaderContext {
                segment: self.knn_filter_output.vector_segment.clone(),
                blockfile_provider: self.blockfile_provider.clone(),
                hnsw_provider: self.hnsw_provider.clone(),
                dimension: self.knn_filter_output.dimension,
            };
            let fetch_pl_task = wrap(
                Box::new(self.fetch_pl.clone()),
                SpannFetchPlInput {
                    reader_context,
                    head_id: head_id as u32,
                },
                ctx.receiver(),
            );

            if let Err(err) = self
                .dispatcher
                .send(fetch_pl_task, Some(Span::current()))
                .await
            {
                self.terminate_with_error(ctx, err);
            }
        }
    }
}

#[async_trait]
impl Handler<TaskResult<SpannFetchPlOutput, SpannFetchPlError>> for SpannKnnOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<SpannFetchPlOutput, SpannFetchPlError>,
        ctx: &ComponentContext<Self>,
    ) {
        let output = match message.into_inner() {
            Ok(output) => output,
            Err(err) => {
                self.terminate_with_error(ctx, err);
                return;
            }
        };
        // Spawn brute force posting list task.
        let bf_pl_task = wrap(
            Box::new(self.bf_pl.clone()),
            SpannBfPlInput {
                posting_list: output.posting_list,
                k: self.k,
                filter: self
                    .knn_filter_output
                    .filter_output
                    .compact_offset_ids
                    .clone(),
                distance_function: self.knn_filter_output.distance_function.clone(),
                query: self.normalized_query_emb.clone(),
            },
            ctx.receiver(),
        );

        if let Err(err) = self
            .dispatcher
            .send(bf_pl_task, Some(Span::current()))
            .await
        {
            self.terminate_with_error(ctx, err);
        }
    }
}

#[async_trait]
impl Handler<TaskResult<SpannBfPlOutput, SpannBfPlError>> for SpannKnnOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<SpannBfPlOutput, SpannBfPlError>,
        ctx: &ComponentContext<Self>,
    ) {
        let output = match message.into_inner() {
            Ok(output) => output,
            Err(err) => {
                self.terminate_with_error(ctx, err);
                return;
            }
        };
        // Update state tracking for merging.
        self.num_outstanding_bf_pl -= 1;
        self.records.push(output.records);
        // Spawn merge task if all done.
        self.try_start_knn_merge_operator(ctx).await;
    }
}

#[async_trait]
impl Handler<TaskResult<SpannKnnMergeOutput, SpannKnnMergeError>> for SpannKnnOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<SpannKnnMergeOutput, SpannKnnMergeError>,
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
                    .merged_records
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
                record_distances: output.merged_records,
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
impl Handler<TaskResult<PrefetchRecordOutput, PrefetchRecordError>> for SpannKnnOrchestrator {
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
impl Handler<TaskResult<KnnProjectionOutput, KnnProjectionError>> for SpannKnnOrchestrator {
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
