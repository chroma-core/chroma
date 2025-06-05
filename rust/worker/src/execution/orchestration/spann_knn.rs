use std::collections::HashMap;

use async_trait::async_trait;
use chroma_distance::{normalize, DistanceFunction};
use chroma_segment::{
    distributed_spann::{SpannSegmentReader, SpannSegmentReaderError},
    spann_provider::SpannProvider,
};
use chroma_system::{
    wrap, ComponentContext, ComponentHandle, Dispatcher, Handler, Orchestrator, TaskMessage,
    TaskResult,
};
use chroma_types::Collection;
use tokio::sync::oneshot::Sender;
use tracing::Span;

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
    spann_fetch_pl::{
        SpannFetchPlError, SpannFetchPlInput, SpannFetchPlOperator, SpannFetchPlOutput,
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
    fetch_pl: SpannFetchPlOperator,
    bf_pl: SpannBfPlOperator,

    // State tracking.
    heads_searched: bool,
    num_outstanding_bf_pl: usize,
    bruteforce_log_done: bool,
    pl_spans: HashMap<u32, Span>,

    // Knn output
    records: Vec<Vec<RecordDistance>>,

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
            fetch_pl: SpannFetchPlOperator {},
            bf_pl: SpannBfPlOperator {},
            heads_searched: false,
            num_outstanding_bf_pl: 0,
            bruteforce_log_done: false,
            pl_spans: HashMap::new(),
            records: Vec::new(),
            merge: SpannKnnMergeOperator { k: k as u32 },
            knn_projection,
            result_channel: None,
            spann_reader: None,
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
            self.send(task, ctx, Some(Span::current())).await;
        }
    }
}

#[async_trait]
impl Orchestrator for SpannKnnOrchestrator {
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
        tasks.push((knn_log_task, Some(Span::current())));
        let reader_res = SpannSegmentReader::from_segment(
            &self.collection,
            &self.knn_filter_output.vector_segment,
            &self.spann_provider.blockfile_provider,
            &self.spann_provider.hnsw_provider,
            self.knn_filter_output.dimension,
        )
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
                    },
                    ctx.receiver(),
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

        // prefetch spann segment
        let prefetch_task = wrap(
            Box::new(PrefetchSegmentOperator::new()),
            PrefetchSegmentInput::new(
                self.knn_filter_output.vector_segment.clone(),
                self.spann_provider.blockfile_provider.clone(),
            ),
            ctx.receiver(),
        );
        // Prefetch task is detached from the orchestrator
        let prefetch_span = tracing::info_span!(parent: None, "Prefetch spann segment", segment_id = %self.knn_filter_output.vector_segment.id);
        tasks.push((prefetch_task, Some(prefetch_span)));

        // prefetch record segment
        let prefetch_record_segment_task = wrap(
            Box::new(PrefetchSegmentOperator::new()),
            PrefetchSegmentInput::new(
                self.knn_filter_output.record_segment.clone(),
                self.spann_provider.blockfile_provider.clone(),
            ),
            ctx.receiver(),
        );
        // Prefetch task is detached from the orchestrator
        let prefetch_span = tracing::info_span!(parent: None, "Prefetch record segment", segment_id = %self.knn_filter_output.record_segment.id);
        tasks.push((prefetch_record_segment_task, Some(prefetch_span)));

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
        let output = match self.ok_or_terminate(message.into_inner(), ctx).await {
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
        let output = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(output) => output,
            None => return,
        };
        // Set state that is used for tracking when we are ready for merging.
        self.heads_searched = true;
        self.num_outstanding_bf_pl = output.center_ids.len();
        // Spawn fetch posting list tasks for the centers.
        for head_id in output.center_ids {
            let pl_span = tracing::info_span!(
                parent: Span::current(),
                "Fetch posting list",
                head_id = head_id,
            );
            self.pl_spans.insert(head_id as u32, pl_span.clone());
            // Invoke Head search operator.
            let fetch_pl_task = wrap(
                Box::new(self.fetch_pl.clone()),
                SpannFetchPlInput {
                    reader: self.spann_reader.clone(),
                    head_id: head_id as u32,
                },
                ctx.receiver(),
            );

            self.send(fetch_pl_task, ctx, Some(pl_span)).await;
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
        let pl_span = self
            .pl_spans
            .remove(&output.head_id)
            .unwrap_or_else(Span::current);
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

        self.send(bf_pl_task, ctx, Some(pl_span)).await;
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
impl Handler<TaskResult<SpannKnnMergeOutput, SpannKnnMergeError>> for SpannKnnOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<SpannKnnMergeOutput, SpannKnnMergeError>,
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
        // Prefetch span is detached from the orchestrator.
        let prefetch_span = tracing::info_span!(parent: None, "Prefetch_record", num_records = output.merged_records.len());
        self.send(prefetch_task, ctx, Some(prefetch_span)).await;

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
        self.send(projection_task, ctx, Some(Span::current())).await;
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
        self.terminate_with_result(message.into_inner().map_err(|e| e.into()), ctx)
            .await;
    }
}
