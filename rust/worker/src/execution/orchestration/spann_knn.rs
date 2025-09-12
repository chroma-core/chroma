use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_distance::{normalize, DistanceFunction};
use chroma_segment::{
    distributed_spann::{SpannSegmentReader, SpannSegmentReaderError},
    spann_provider::SpannProvider,
};
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
    knn_log::{KnnLogError, KnnLogInput},
    knn_merge::{KnnMergeError, KnnMergeInput, KnnMergeOutput},
    spann_bf_pl::{SpannBfPlError, SpannBfPlInput, SpannBfPlOperator, SpannBfPlOutput},
    spann_centers_search::{
        SpannCentersSearchError, SpannCentersSearchInput, SpannCentersSearchOperator,
        SpannCentersSearchOutput,
    },
    spann_fetch_pl::{
        SpannFetchPlError, SpannFetchPlInput, SpannFetchPlOperator, SpannFetchPlOutput,
    },
};

use super::knn_filter::{KnnError, KnnFilterOutput};

#[derive(Debug)]
pub struct SpannKnnOrchestrator {
    // Orchestrator parameters
    context: OrchestratorContext,
    blockfile_provider: BlockfileProvider,
    spann_provider: SpannProvider,
    queue: usize,
    collection_and_segments: CollectionAndSegments,

    // Output from KnnFilterOrchestrator
    knn_filter_output: KnnFilterOutput,

    // Query params.
    k: usize,
    normalized_query_emb: Vec<f32>,

    // Knn operator for the log.
    log_knn: Knn,
    // Spann segment knn operators.
    head_search: SpannCentersSearchOperator,
    fetch_pl: SpannFetchPlOperator,
    bf_pl: SpannBfPlOperator,

    // State tracking.
    heads_searched: bool,
    num_outstanding_bf_pl: usize,
    bruteforce_log_done: bool,

    // Knn output
    records: Vec<Vec<RecordMeasure>>,

    // Merge
    merge: Merge,

    // Result channel
    result_channel: Option<Sender<Result<Vec<RecordMeasure>, KnnError>>>,
    spann_reader: Option<SpannSegmentReader<'static>>,
}

impl SpannKnnOrchestrator {
    pub fn new(
        spann_provider: SpannProvider,
        dispatcher: ComponentHandle<Dispatcher>,
        queue: usize,
        collection_and_segments: CollectionAndSegments,
        knn_filter_output: KnnFilterOutput,
        k: usize,
        query: Vec<f32>,
    ) -> Self {
        let normalized_query_emb =
            if knn_filter_output.distance_function == DistanceFunction::Cosine {
                normalize(&query)
            } else {
                query.clone()
            };
        let context = OrchestratorContext::new(dispatcher);
        let blockfile_provider = spann_provider.blockfile_provider.clone();
        Self {
            context,
            blockfile_provider,
            spann_provider,
            queue,
            collection_and_segments,
            knn_filter_output,
            k,
            normalized_query_emb,
            log_knn: Knn {
                embedding: query,
                fetch: k as u32,
            },
            head_search: SpannCentersSearchOperator {},
            fetch_pl: SpannFetchPlOperator {},
            bf_pl: SpannBfPlOperator {},
            heads_searched: false,
            num_outstanding_bf_pl: 0,
            bruteforce_log_done: false,
            records: Vec::new(),
            merge: Merge { k: k as u32 },
            result_channel: None,
            spann_reader: None,
        }
    }

    async fn try_start_knn_merge_operator(&mut self, ctx: &ComponentContext<Self>) {
        if self.heads_searched && self.num_outstanding_bf_pl == 0 && self.bruteforce_log_done {
            // This is safe because self.records is only used once and that is during merge.
            // It is only pushed into until then and after merge never used.
            let batch_distances = std::mem::take(&mut self.records);
            let task = wrap(
                Box::new(self.merge.clone()),
                KnnMergeInput {
                    batch_measures: batch_distances,
                },
                ctx.receiver(),
                self.context.task_cancellation_token.clone(),
            );
            self.send(task, ctx, Some(Span::current())).await;
        }
    }
}

#[async_trait]
impl Orchestrator for SpannKnnOrchestrator {
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
            Box::new(self.log_knn.clone()),
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
        let reader_res = Box::pin(SpannSegmentReader::from_segment(
            &self.collection_and_segments.collection,
            &self.collection_and_segments.vector_segment,
            &self.blockfile_provider,
            &self.spann_provider.hnsw_provider,
            self.knn_filter_output.dimension,
            self.spann_provider.adaptive_search_nprobe,
        ))
        .await;
        match reader_res {
            Ok(reader) => {
                self.spann_reader = Some(reader.clone());
                // Spawn the centers search task if reader is found.
                let head_search_task = wrap(
                    Box::new(self.head_search.clone()),
                    SpannCentersSearchInput {
                        reader: Some(reader),
                        normalized_query: self.normalized_query_emb.clone(),
                        collection_num_records_post_compaction: self
                            .collection_and_segments
                            .collection
                            .total_records_post_compaction
                            as usize,
                        k: self.k,
                    },
                    ctx.receiver(),
                    self.context.task_cancellation_token.clone(),
                );
                tasks.push((head_search_task, Some(Span::current())));
            }
            Err(e) => match e {
                // Segment uninited means no compaction yet.
                SpannSegmentReaderError::UninitializedSegment => {
                    // If the segment is uninitialized, we can skip the head search.
                    self.spann_reader = None;
                    self.heads_searched = true;
                }
                _ => {
                    let _: Option<()> = self
                        .ok_or_terminate(Err(KnnError::SpannSegmentReaderCreationError(e)), ctx)
                        .await;
                }
            },
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
impl Handler<TaskResult<KnnOutput, KnnLogError>> for SpannKnnOrchestrator {
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
        self.records.push(output.distances);
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
        let output = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(output) => output,
            None => return,
        };
        // Set state that is used for tracking when we are ready for merging.
        self.heads_searched = true;
        self.num_outstanding_bf_pl = output.center_ids.len();
        // Spawn fetch posting list tasks for the centers.
        for head_id in output.center_ids {
            // Invoke Head search operator.
            let fetch_pl_task = wrap(
                Box::new(self.fetch_pl.clone()),
                SpannFetchPlInput {
                    reader: self.spann_reader.clone(),
                    head_id: head_id as u32,
                },
                ctx.receiver(),
                self.context.task_cancellation_token.clone(),
            );

            self.send(fetch_pl_task, ctx, Some(Span::current())).await;
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
        let output = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(output) => output,
            None => return,
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
            self.context.task_cancellation_token.clone(),
        );

        self.send(bf_pl_task, ctx, Some(Span::current())).await;
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
        let output = match self.ok_or_terminate(message.into_inner(), ctx).await {
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
impl Handler<TaskResult<KnnMergeOutput, KnnMergeError>> for SpannKnnOrchestrator {
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
