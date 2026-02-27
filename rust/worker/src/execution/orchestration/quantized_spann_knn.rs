use std::sync::Arc;

use async_trait::async_trait;
use chroma_segment::{quantized_spann::QuantizedSpannSegmentReader, spann_provider::SpannProvider};
use chroma_system::{
    wrap, ComponentContext, ComponentHandle, Dispatcher, Handler, Orchestrator,
    OrchestratorContext, TaskMessage, TaskResult,
};
use chroma_types::{
    default_search_nprobe, InternalSpannConfiguration,
    operator::{Knn, KnnOutput, Merge, RecordMeasure},
    CollectionAndSegments,
};
use tokio::sync::oneshot::Sender;
use tracing::Span;

use crate::execution::operators::{
    knn_log::{KnnLogError, KnnLogInput},
    knn_merge::{KnnMergeError, KnnMergeInput, KnnMergeOutput},
    quantized_spann_bruteforce::{
        QuantizedSpannBruteforceError, QuantizedSpannBruteforceInput,
        QuantizedSpannBruteforceOperator, QuantizedSpannBruteforceOutput,
    },
    quantized_spann_center_search::{
        QuantizedSpannCenterSearchError, QuantizedSpannCenterSearchInput,
        QuantizedSpannCenterSearchOutput,
    },
    quantized_spann_load_center::{
        QuantizedSpannLoadCenterError, QuantizedSpannLoadCenterOperator,
        QuantizedSpannLoadCenterOutput,
    },
    quantized_spann_load_cluster::{
        QuantizedSpannLoadClusterError, QuantizedSpannLoadClusterInput,
        QuantizedSpannLoadClusterOperator, QuantizedSpannLoadClusterOutput,
    },
    quantized_spann_rerank::{
        QuantizedSpannRerankError, QuantizedSpannRerankInput, QuantizedSpannRerankOperator,
        QuantizedSpannRerankOutput,
    },
};

use super::knn_filter::{KnnError, KnnFilterOutput};

#[derive(Debug)]
pub struct QuantizedSpannKnnOrchestrator {
    collection_and_segments: CollectionAndSegments,
    context: OrchestratorContext,
    knn: Knn,
    knn_filter_output: KnnFilterOutput,
    queue: usize,
    reader: Option<QuantizedSpannSegmentReader>,
    rotated_query: Option<Arc<[f32]>>,
    spann_provider: SpannProvider,

    // Rerank factors read from collection config (1 = disabled).
    centroid_rerank_factor: usize,
    vector_rerank_factor: usize,

    // State tracking.
    // num_bruteforces is set when either there is no reader (0) or center search completes.
    num_bruteforces: Option<usize>,
    log_and_bruteforce_results: Vec<Vec<RecordMeasure>>,

    // Result channel.
    result_channel: Option<Sender<Result<Vec<RecordMeasure>, KnnError>>>,
}

impl QuantizedSpannKnnOrchestrator {
    pub fn new(
        spann_provider: SpannProvider,
        dispatcher: ComponentHandle<Dispatcher>,
        queue: usize,
        collection_and_segments: CollectionAndSegments,
        knn_filter_output: KnnFilterOutput,
        knn: Knn,
    ) -> Self {
        // Derive rerank factors from the collection's SPANN configuration.
        let spann_config: InternalSpannConfiguration = collection_and_segments
            .collection
            .schema
            .as_ref()
            .and_then(|s| s.get_spann_config())
            .map(|(config, space)| InternalSpannConfiguration::from((Some(&space), &config)))
            .unwrap_or_default();

        Self {
            collection_and_segments,
            context: OrchestratorContext::new(dispatcher),
            knn,
            knn_filter_output,
            queue,
            reader: None,
            rotated_query: None,
            spann_provider,
            centroid_rerank_factor: spann_config.centroid_rerank_factor as usize,
            vector_rerank_factor: spann_config.vector_rerank_factor as usize,
            num_bruteforces: None,
            log_and_bruteforce_results: Vec::new(),
            result_channel: None,
        }
    }

    async fn try_merge(&mut self, ctx: &ComponentContext<Self>) {
        // Merge once KnnLog + all bruteforces report in.
        if self
            .num_bruteforces
            .is_some_and(|num_bruteforces| self.log_and_bruteforce_results.len() > num_bruteforces)
        {
            // Merge up to k * vector_rerank_factor candidates; the rerank step (if enabled)
            // will then trim to k with exact distances.
            let merge_k = (self.knn.fetch as usize * self.vector_rerank_factor) as u32;
            let task = wrap(
                Box::new(Merge { k: merge_k }),
                KnnMergeInput {
                    batch_measures: std::mem::take(&mut self.log_and_bruteforce_results),
                },
                ctx.receiver(),
                self.context.task_cancellation_token.clone(),
            );
            self.send(task, ctx, Some(Span::current())).await;
        }
    }
}

#[async_trait]
impl Orchestrator for QuantizedSpannKnnOrchestrator {
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

        // 1. KnnLog — always dispatched.
        let knn_log_task = wrap(
            Box::new(self.knn.clone()),
            KnnLogInput {
                logs: self.knn_filter_output.logs.clone(),
                blockfile_provider: self.spann_provider.blockfile_provider.clone(),
                record_segment: self.collection_and_segments.record_segment.clone(),
                log_offset_ids: self.knn_filter_output.filter_output.log_offset_ids.clone(),
                distance_function: self.knn_filter_output.distance_function.clone(),
            },
            ctx.receiver(),
            self.context.task_cancellation_token.clone(),
        );
        tasks.push((knn_log_task, Some(Span::current())));

        // 2. LoadCenter — dispatched if segment is initialized.
        if self
            .collection_and_segments
            .vector_segment
            .file_path
            .is_empty()
        {
            self.num_bruteforces = Some(0);
        } else {
            let load_center_task = wrap(
                Box::new(QuantizedSpannLoadCenterOperator {
                    collection: self.collection_and_segments.collection.clone(),
                    spann_provider: self.spann_provider.clone(),
                    vector_segment: self.collection_and_segments.vector_segment.clone(),
                }),
                (),
                ctx.receiver(),
                self.context.task_cancellation_token.clone(),
            );
            tasks.push((load_center_task, Some(Span::current())));
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
impl Handler<TaskResult<KnnOutput, KnnLogError>> for QuantizedSpannKnnOrchestrator {
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
        self.log_and_bruteforce_results.push(output.distances);
        self.try_merge(ctx).await;
    }
}

#[async_trait]
impl Handler<TaskResult<QuantizedSpannLoadCenterOutput, QuantizedSpannLoadCenterError>>
    for QuantizedSpannKnnOrchestrator
{
    type Result = ();
    async fn handle(
        &mut self,
        message: TaskResult<QuantizedSpannLoadCenterOutput, QuantizedSpannLoadCenterError>,
        ctx: &ComponentContext<Self>,
    ) {
        let output = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(output) => output,
            None => return,
        };

        self.reader = Some(output.reader.clone());

        let search_nprobe = self
            .collection_and_segments
            .collection
            .schema
            .as_ref()
            .and_then(|s| s.get_spann_config())
            .and_then(|(config, _)| config.search_nprobe)
            .unwrap_or(default_search_nprobe()) as usize;

        let center_search_task = wrap(
            Box::new(self.knn.clone()),
            QuantizedSpannCenterSearchInput {
                count: search_nprobe,
                centroid_rerank_factor: self.centroid_rerank_factor,
                reader: output.reader,
            },
            ctx.receiver(),
            self.context.task_cancellation_token.clone(),
        );
        self.send(center_search_task, ctx, Some(Span::current()))
            .await;
    }
}

#[async_trait]
impl Handler<TaskResult<QuantizedSpannCenterSearchOutput, QuantizedSpannCenterSearchError>>
    for QuantizedSpannKnnOrchestrator
{
    type Result = ();
    async fn handle(
        &mut self,
        message: TaskResult<QuantizedSpannCenterSearchOutput, QuantizedSpannCenterSearchError>,
        ctx: &ComponentContext<Self>,
    ) {
        let output = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(output) => output,
            None => return,
        };

        self.num_bruteforces = Some(output.cluster_ids.len());
        self.rotated_query = Some(output.rotated_query.clone());

        if output.cluster_ids.is_empty() {
            self.try_merge(ctx).await;
            return;
        }

        let reader = self
            .reader
            .as_ref()
            .expect("reader must be set when center search succeeds")
            .clone();

        let load_cluster_operator = QuantizedSpannLoadClusterOperator {
            reader: reader.clone(),
        };

        for cluster_id in output.cluster_ids {
            let load_cluster_task = wrap(
                Box::new(load_cluster_operator.clone()),
                QuantizedSpannLoadClusterInput { cluster_id },
                ctx.receiver(),
                self.context.task_cancellation_token.clone(),
            );
            self.send(load_cluster_task, ctx, Some(Span::current()))
                .await;
        }
    }
}

#[async_trait]
impl Handler<TaskResult<QuantizedSpannLoadClusterOutput, QuantizedSpannLoadClusterError>>
    for QuantizedSpannKnnOrchestrator
{
    type Result = ();
    async fn handle(
        &mut self,
        message: TaskResult<QuantizedSpannLoadClusterOutput, QuantizedSpannLoadClusterError>,
        ctx: &ComponentContext<Self>,
    ) {
        let output = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(output) => output,
            None => return,
        };

        let distance_function = self
            .reader
            .as_ref()
            .expect("reader must be set when load cluster succeeds")
            .distance_function()
            .clone();

        let rotated_query = self
            .rotated_query
            .as_ref()
            .expect("rotated_query must be set when load cluster succeeds")
            .clone();

        // Fetch k * vector_rerank_factor candidates per cluster; the merge + rerank steps
        // will then trim to k with exact distances.
        let bf_count = self.knn.fetch as usize * self.vector_rerank_factor;

        let bf_operator = QuantizedSpannBruteforceOperator {
            count: bf_count,
            distance_function,
            filter: self
                .knn_filter_output
                .filter_output
                .compact_offset_ids
                .clone(),
            rotated_query,
        };

        let bf_task = wrap(
            Box::new(bf_operator),
            QuantizedSpannBruteforceInput {
                cluster: output.cluster,
                global_versions: output.global_versions,
            },
            ctx.receiver(),
            self.context.task_cancellation_token.clone(),
        );
        self.send(bf_task, ctx, Some(Span::current())).await;
    }
}

#[async_trait]
impl Handler<TaskResult<QuantizedSpannBruteforceOutput, QuantizedSpannBruteforceError>>
    for QuantizedSpannKnnOrchestrator
{
    type Result = ();
    async fn handle(
        &mut self,
        message: TaskResult<QuantizedSpannBruteforceOutput, QuantizedSpannBruteforceError>,
        ctx: &ComponentContext<Self>,
    ) {
        let output = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(output) => output,
            None => return,
        };
        self.log_and_bruteforce_results.push(output.records);
        self.try_merge(ctx).await;
    }
}

#[async_trait]
impl Handler<TaskResult<KnnMergeOutput, KnnMergeError>> for QuantizedSpannKnnOrchestrator {
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

        if self.vector_rerank_factor <= 1 {
            // No vector reranking: return the merged approximate results directly.
            self.terminate_with_result(Ok(output.measures), ctx).await;
            return;
        }

        // Dispatch the rerank operator to re-score candidates with exact distances.
        let rerank_operator = QuantizedSpannRerankOperator {
            k: self.knn.fetch as usize,
            embedding: self.knn.embedding.clone(),
            distance_function: self.knn_filter_output.distance_function.clone(),
            blockfile_provider: self.spann_provider.blockfile_provider.clone(),
            record_segment: self.collection_and_segments.record_segment.clone(),
        };

        let rerank_task = wrap(
            Box::new(rerank_operator),
            QuantizedSpannRerankInput {
                candidates: output.measures,
            },
            ctx.receiver(),
            self.context.task_cancellation_token.clone(),
        );
        self.send(rerank_task, ctx, Some(Span::current())).await;
    }
}

#[async_trait]
impl Handler<TaskResult<QuantizedSpannRerankOutput, QuantizedSpannRerankError>>
    for QuantizedSpannKnnOrchestrator
{
    type Result = ();
    async fn handle(
        &mut self,
        message: TaskResult<QuantizedSpannRerankOutput, QuantizedSpannRerankError>,
        ctx: &ComponentContext<Self>,
    ) {
        let output = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(output) => output,
            None => return,
        };
        self.terminate_with_result(Ok(output.measures), ctx).await;
    }
}
