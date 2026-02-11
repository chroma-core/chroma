use async_trait::async_trait;
use chroma_segment::{quantized_spann::QuantizedSpannSegmentReader, spann_provider::SpannProvider};
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
    quantized_spann_bruteforce::{
        QuantizedSpannBruteforceError, QuantizedSpannBruteforceInput,
        QuantizedSpannBruteforceOperator, QuantizedSpannBruteforceOutput,
    },
    quantized_spann_navigate::{
        QuantizedSpannNavigateError, QuantizedSpannNavigateInput, QuantizedSpannNavigateOutput,
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
    spann_provider: SpannProvider,

    // State tracking.
    num_bruteforces: Option<usize>,
    records: Vec<Vec<RecordMeasure>>,

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
        Self {
            collection_and_segments,
            context: OrchestratorContext::new(dispatcher),
            knn,
            knn_filter_output,
            queue,
            reader: None,
            spann_provider,
            num_bruteforces: None,
            records: Vec::new(),
            result_channel: None,
        }
    }

    async fn try_merge(&mut self, ctx: &ComponentContext<Self>) {
        if !self.records.is_empty() && self.reader.is_none() {
            let records = std::mem::take(&mut self.records);
            self.terminate_with_result(Ok(records.into_iter().flatten().collect()), ctx)
                .await;
            return;
        }

        if self
            .num_bruteforces
            .is_some_and(|num_bruteforces| self.records.len() > num_bruteforces)
        {
            let task = wrap(
                Box::new(Merge { k: self.knn.fetch }),
                KnnMergeInput {
                    batch_measures: std::mem::take(&mut self.records),
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

        // 2. Create reader and dispatch Navigate if segment is initialized.
        if !self
            .collection_and_segments
            .vector_segment
            .file_path
            .is_empty()
        {
            match self
                .spann_provider
                .read_quantized_usearch(
                    &self.collection_and_segments.collection,
                    &self.collection_and_segments.vector_segment,
                )
                .await
            {
                Ok(reader) => {
                    let search_nprobe = self
                        .collection_and_segments
                        .collection
                        .schema
                        .as_ref()
                        .and_then(|s| s.get_spann_config())
                        .and_then(|(config, _)| config.search_nprobe)
                        .unwrap_or(64) as usize;

                    let navigate_task = wrap(
                        Box::new(self.knn.clone()),
                        QuantizedSpannNavigateInput {
                            count: search_nprobe,
                            reader: reader.clone(),
                        },
                        ctx.receiver(),
                        self.context.task_cancellation_token.clone(),
                    );
                    tasks.push((navigate_task, Some(Span::current())));
                    self.reader = Some(reader);
                }
                Err(e) => {
                    self.terminate_with_result(Err(KnnError::QuantizedSpannReader(e)), ctx)
                        .await;
                }
            }
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
        self.records.push(output.distances);
        self.try_merge(ctx).await;
    }
}

#[async_trait]
impl Handler<TaskResult<QuantizedSpannNavigateOutput, QuantizedSpannNavigateError>>
    for QuantizedSpannKnnOrchestrator
{
    type Result = ();
    async fn handle(
        &mut self,
        message: TaskResult<QuantizedSpannNavigateOutput, QuantizedSpannNavigateError>,
        ctx: &ComponentContext<Self>,
    ) {
        let output = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(output) => output,
            None => return,
        };

        self.num_bruteforces = Some(output.cluster_ids.len());

        if output.cluster_ids.is_empty() {
            self.try_merge(ctx).await;
            return;
        }

        let reader = self
            .reader
            .as_ref()
            .expect("reader must be set when navigate succeeds")
            .clone();

        reader
            .load_clusters(output.cluster_ids.iter().copied())
            .await;

        let bf_operator = QuantizedSpannBruteforceOperator {
            count: self.knn.fetch as usize,
            filter: self
                .knn_filter_output
                .filter_output
                .compact_offset_ids
                .clone(),
            reader,
            rotated_query: output.rotated_query,
        };
        for cluster_id in output.cluster_ids {
            let bf_task = wrap(
                Box::new(bf_operator.clone()),
                QuantizedSpannBruteforceInput { cluster_id },
                ctx.receiver(),
                self.context.task_cancellation_token.clone(),
            );
            self.send(bf_task, ctx, Some(Span::current())).await;
        }
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
        self.records.push(output.records);
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
        self.terminate_with_result(Ok(output.measures), ctx).await;
    }
}
