use std::collections::HashMap;

use async_trait::async_trait;
use chroma_distance::{normalize, DistanceFunction};
use chroma_segment::{distributed_spann::SpannSegmentReader, spann_provider::SpannProvider};
use chroma_system::{
    wrap, ComponentContext, ComponentHandle, Dispatcher, Handler, Orchestrator, TaskMessage,
    TaskResult,
};
use chroma_types::Collection;
use tokio::sync::oneshot::Sender;
use uuid::Uuid;

use crate::execution::operators::{
    knn::{KnnOperator, RecordDistance},
    knn_log::{KnnLogError, KnnLogInput, KnnLogOutput},
    knn_projection::{
        KnnProjectionError, KnnProjectionInput, KnnProjectionOperator, KnnProjectionOutput,
    },
    prefetch_record::{
        PrefetchRecordError, PrefetchRecordInput, PrefetchRecordOperator, PrefetchRecordOutput,
    },
    prefetch_segment::{
        PrefetchSegmentError, PrefetchSegmentInput, PrefetchSegmentOperator, PrefetchSegmentOutput,
    },
    spann_bf_pl::{SpannBfPlError, SpannBfPlInput, SpannBfPlOperator, SpannBfPlOutput},
    spann_centers_search::{
        SpannCentersSearchError, SpannCentersSearchInput, SpannCentersSearchOperator,
        SpannCentersSearchOutput,
    },
    spann_fetch_block::{
        SpannFetchBlockError, SpannFetchBlockInput, SpannFetchBlockOperator, SpannFetchBlockOutput,
    },
    spann_knn_merge::{
        SpannKnnMergeError, SpannKnnMergeInput, SpannKnnMergeOperator, SpannKnnMergeOutput,
    },
};

use super::knn_filter::{KnnError, KnnFilterOutput, KnnOutput, KnnResult};

#[derive(Debug)]
pub struct SpannKnnOrchestrator {
    // Orchestrator parameters
    spann_provider: SpannProvider,
    dispatcher: ComponentHandle<Dispatcher>,
    queue: usize,
    collection: Collection,

    // Output from KnnFilterOrchestrator
    knn_filter_output: KnnFilterOutput,

    // Query params.
    k: usize,
    normalized_query_emb: Vec<f32>,

    // Knn operator for the log.
    log_knn: KnnOperator,
    // Spann segment knn operators.
    head_search: SpannCentersSearchOperator,
    bf_pl: Box<SpannBfPlOperator>,

    // State tracking.
    heads_searched: bool,
    num_outstanding_bf_pl: usize,
    bruteforce_log_done: bool,

    // Knn output
    records: Vec<Vec<RecordDistance>>,
    block_ids_to_heads: HashMap<Uuid, Vec<u32>>,

    // Merge and project
    merge: SpannKnnMergeOperator,
    knn_projection: KnnProjectionOperator,

    // Result channel
    result_channel: Option<Sender<KnnResult>>,
    spann_reader: Option<SpannSegmentReader<'static>>,
}

impl SpannKnnOrchestrator {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        spann_provider: SpannProvider,
        dispatcher: ComponentHandle<Dispatcher>,
        queue: usize,
        collection: Collection,
        knn_filter_output: KnnFilterOutput,
        k: usize,
        query_embedding: Vec<f32>,
        knn_projection: KnnProjectionOperator,
    ) -> Self {
        let normalized_query_emb =
            if knn_filter_output.distance_function == DistanceFunction::Cosine {
                normalize(&query_embedding)
            } else {
                query_embedding.clone()
            };
        Self {
            spann_provider,
            dispatcher,
            queue,
            collection,
            knn_filter_output,
            k,
            normalized_query_emb,
            log_knn: KnnOperator {
                embedding: query_embedding,
                fetch: k as u32,
            },
            head_search: SpannCentersSearchOperator {},
            bf_pl: SpannBfPlOperator::new(),
            heads_searched: false,
            num_outstanding_bf_pl: 0,
            bruteforce_log_done: false,
            records: Vec::new(),
            merge: SpannKnnMergeOperator { k: k as u32 },
            knn_projection,
            result_channel: None,
            spann_reader: None,
            block_ids_to_heads: HashMap::new(),
        }
    }

    async fn try_start_knn_merge_operator(&mut self, ctx: &ComponentContext<Self>) {
        if self.heads_searched && self.num_outstanding_bf_pl == 0 && self.bruteforce_log_done {
            // This is safe because self.records is only used once and that is during merge.
            // It is only pushed into until then and after merge never used.
            let records = std::mem::take(&mut self.records);
            let task = wrap(
                Box::new(self.merge.clone()),
                SpannKnnMergeInput { records },
                ctx.receiver(),
            );
            self.send(task, ctx).await;
        }
    }

    async fn set_spann_reader(&mut self, ctx: &ComponentContext<Self>) {
        let reader_res = SpannSegmentReader::from_segment(
            &self.collection,
            &self.knn_filter_output.vector_segment,
            &self.spann_provider.blockfile_provider,
            &self.spann_provider.hnsw_provider,
            self.knn_filter_output.dimension,
        )
        .await;
        let reader = match self.ok_or_terminate(reader_res, ctx) {
            Some(reader) => reader,
            None => {
                tracing::error!("Failed to create SpannSegmentReader");
                return;
            }
        };
        self.spann_reader = Some(reader);
    }
}

#[async_trait]
impl Orchestrator for SpannKnnOrchestrator {
    type Output = KnnOutput;
    type Error = KnnError;

    fn dispatcher(&self) -> ComponentHandle<Dispatcher> {
        self.dispatcher.clone()
    }

    async fn initial_tasks(&mut self, ctx: &ComponentContext<Self>) -> Vec<TaskMessage> {
        let mut tasks = Vec::new();

        let knn_log_task = wrap(
            Box::new(self.log_knn.clone()),
            KnnLogInput {
                logs: self.knn_filter_output.logs.clone(),
                blockfile_provider: self.spann_provider.blockfile_provider.clone(),
                record_segment: self.knn_filter_output.record_segment.clone(),
                log_offset_ids: self.knn_filter_output.filter_output.log_offset_ids.clone(),
                distance_function: self.knn_filter_output.distance_function.clone(),
            },
            ctx.receiver(),
        );
        tasks.push(knn_log_task);
        self.set_spann_reader(ctx).await;
        let head_search_task = wrap(
            Box::new(self.head_search.clone()),
            SpannCentersSearchInput {
                reader: self.spann_reader.clone(),
                normalized_query: self.normalized_query_emb.clone(),
            },
            ctx.receiver(),
        );
        tasks.push(head_search_task);

        let prefetch_task = wrap(
            Box::new(PrefetchSegmentOperator::new()),
            PrefetchSegmentInput::new(
                self.knn_filter_output.vector_segment.clone(),
                self.spann_provider.blockfile_provider.clone(),
            ),
            ctx.receiver(),
        );
        tasks.push(prefetch_task);

        let prefetch_record_segment_task = wrap(
            Box::new(PrefetchSegmentOperator::new()),
            PrefetchSegmentInput::new(
                self.knn_filter_output.record_segment.clone(),
                self.spann_provider.blockfile_provider.clone(),
            ),
            ctx.receiver(),
        );
        tasks.push(prefetch_record_segment_task);

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
impl Handler<TaskResult<PrefetchSegmentOutput, PrefetchSegmentError>> for SpannKnnOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        _: TaskResult<PrefetchSegmentOutput, PrefetchSegmentError>,
        _: &ComponentContext<SpannKnnOrchestrator>,
    ) {
        // Nothing to do.
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
        let output = match self.ok_or_terminate(message.into_inner(), ctx) {
            Some(output) => output,
            None => return,
        };
        self.records.push(output.record_distances);
        self.bruteforce_log_done = true;
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
        let output = match self.ok_or_terminate(message.into_inner(), ctx) {
            Some(output) => output,
            None => return,
        };
        // Set state that is used for tracking when we are ready for merging.
        self.heads_searched = true;
        self.num_outstanding_bf_pl = output.center_ids.len();

        let centers = output
            .center_ids
            .iter()
            .map(|id| *id as u32)
            .collect::<Vec<_>>();
        let block_id_to_heads = match &self.spann_reader {
            Some(reader) => reader.group_heads_by_blocks(&centers).await,
            None => {
                tracing::error!("SpannSegmentReader is not set, cannot group heads by blocks");
                self.terminate_with_result(Err(KnnError::SpannSegmentReaderNotFound), ctx);
                return;
            }
        };
        self.block_ids_to_heads = block_id_to_heads;
        let block_ids = self.block_ids_to_heads.keys().cloned().collect::<Vec<_>>();
        // Spanw block get tasks one per block.
        for block_id in block_ids {
            let fetch_block_task = wrap(
                SpannFetchBlockOperator::new(),
                SpannFetchBlockInput {
                    provider: self.spann_provider.blockfile_provider.clone(),
                    block_id,
                },
                ctx.receiver(),
            );
            self.send(fetch_block_task, ctx).await;
        }
    }
}

#[async_trait]
impl Handler<TaskResult<SpannFetchBlockOutput, SpannFetchBlockError>> for SpannKnnOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<SpannFetchBlockOutput, SpannFetchBlockError>,
        ctx: &ComponentContext<Self>,
    ) {
        let output = match self.ok_or_terminate(message.into_inner(), ctx) {
            Some(output) => output,
            None => return,
        };
        let heads = match self.block_ids_to_heads.get(&output.block.id) {
            Some(heads) => heads.clone(),
            None => {
                return;
            }
        };
        for head_id in heads {
            // Spawn brute force posting list task.
            let bf_pl_task = wrap(
                self.bf_pl.clone(),
                SpannBfPlInput {
                    block: output.block.clone(),
                    k: self.k,
                    filter: self
                        .knn_filter_output
                        .filter_output
                        .compact_offset_ids
                        .clone(),
                    distance_function: self.knn_filter_output.distance_function.clone(),
                    query: self.normalized_query_emb.clone(),
                    head_id,
                    reader: self.spann_reader.clone(),
                    dimension: self.knn_filter_output.dimension,
                },
                ctx.receiver(),
            );

            self.send(bf_pl_task, ctx).await;
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
        let output = match self.ok_or_terminate(message.into_inner(), ctx) {
            Some(output) => output,
            None => return,
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
        let output = match self.ok_or_terminate(message.into_inner(), ctx) {
            Some(output) => output,
            None => return,
        };

        // Prefetch records before projection
        let prefetch_task = wrap(
            Box::new(PrefetchRecordOperator {}),
            PrefetchRecordInput {
                logs: self.knn_filter_output.logs.clone(),
                blockfile_provider: self.spann_provider.blockfile_provider.clone(),
                record_segment: self.knn_filter_output.record_segment.clone(),
                offset_ids: output
                    .merged_records
                    .iter()
                    .map(|record| record.offset_id)
                    .collect(),
            },
            ctx.receiver(),
        );
        self.send(prefetch_task, ctx).await;

        let projection_task = wrap(
            Box::new(self.knn_projection.clone()),
            KnnProjectionInput {
                logs: self.knn_filter_output.logs.clone(),
                blockfile_provider: self.spann_provider.blockfile_provider.clone(),
                record_segment: self.knn_filter_output.record_segment.clone(),
                record_distances: output.merged_records,
            },
            ctx.receiver(),
        );
        self.send(projection_task, ctx).await;
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
        self.terminate_with_result(message.into_inner().map_err(|e| e.into()), ctx);
    }
}
