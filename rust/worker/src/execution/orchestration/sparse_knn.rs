use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_system::{
    wrap, ChannelError, ComponentContext, ComponentHandle, Dispatcher, Handler, Orchestrator,
    OrchestratorContext, PanicError, TaskError, TaskMessage, TaskResult,
};
use chroma_types::{
    operator::{Merge, RecordMeasure},
    CollectionAndSegments, SparseVector,
};
use thiserror::Error;
use tokio::sync::oneshot::{error::RecvError, Sender};
use tracing::Span;

use crate::execution::{
    operators::{
        idf::{Idf, IdfError, IdfInput, IdfOutput},
        knn_merge::{KnnMergeError, KnnMergeInput, KnnMergeOutput},
        sparse_index_knn::{
            SparseIndexKnn, SparseIndexKnnError, SparseIndexKnnInput, SparseIndexKnnOutput,
        },
        sparse_log_knn::{SparseLogKnn, SparseLogKnnError, SparseLogKnnInput, SparseLogKnnOutput},
    },
    orchestration::knn_filter::KnnFilterOutput,
};

#[derive(Error, Debug)]
pub enum SparseKnnError {
    #[error("Operation aborted because resources exhausted")]
    Aborted,
    #[error("Error sending message through channel: {0}")]
    Channel(#[from] ChannelError),
    #[error("Error running Idf operator: {0}")]
    Idf(#[from] IdfError),
    #[error("Error running KnnMerge operator: {0}")]
    KnnMerge(#[from] KnnMergeError),
    #[error("Panic: {0}")]
    Panic(#[from] PanicError),
    #[error("Error receiving final result: {0}")]
    Result(#[from] RecvError),
    #[error("Error running SparseIndexKnn operator: {0}")]
    SparseIndexKnn(#[from] SparseIndexKnnError),
    #[error("Error running SparseLogKnn operator: {0}")]
    SparseLogKnn(#[from] SparseLogKnnError),
}

impl ChromaError for SparseKnnError {
    fn code(&self) -> ErrorCodes {
        match self {
            SparseKnnError::Aborted => ErrorCodes::ResourceExhausted,
            SparseKnnError::Channel(err) => err.code(),
            SparseKnnError::Idf(err) => err.code(),
            SparseKnnError::KnnMerge(err) => err.code(),
            SparseKnnError::Panic(_) => ErrorCodes::Aborted,
            SparseKnnError::Result(_) => ErrorCodes::Internal,
            SparseKnnError::SparseIndexKnn(err) => err.code(),
            SparseKnnError::SparseLogKnn(err) => err.code(),
        }
    }
}

impl<E> From<TaskError<E>> for SparseKnnError
where
    E: Into<SparseKnnError>,
{
    fn from(value: TaskError<E>) -> Self {
        match value {
            TaskError::Aborted => SparseKnnError::Aborted,
            TaskError::Panic(e) => e.into(),
            TaskError::TaskFailed(e) => e.into(),
        }
    }
}

#[derive(Debug)]
pub struct SparseKnnOrchestrator {
    // Orchestrator parameters
    context: OrchestratorContext,
    blockfile_provider: BlockfileProvider,
    queue: usize,

    // Collection information
    collection_and_segments: CollectionAndSegments,

    // TODO: This is a temporary config to enable bm25 for certain tenants.
    // This should be removed once we have collection schema ready.
    use_bm25: bool,

    // Output from KnnFilterOrchestrator
    knn_filter_output: KnnFilterOutput,

    // Sparse Knn params shared between log and segments
    query: SparseVector,
    key: String,
    limit: u32,

    // Knn output
    batch_measures: Vec<Vec<RecordMeasure>>,

    // Merge
    merge: Merge,

    // Result channel
    result_channel: Option<Sender<Result<Vec<RecordMeasure>, SparseKnnError>>>,
}

impl SparseKnnOrchestrator {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        blockfile_provider: BlockfileProvider,
        dispatcher: ComponentHandle<Dispatcher>,
        queue: usize,
        collection_and_segments: CollectionAndSegments,
        use_bm25: bool,
        knn_filter_output: KnnFilterOutput,
        query: SparseVector,
        key: String,
        limit: u32,
    ) -> Self {
        let context = OrchestratorContext::new(dispatcher);
        Self {
            context,
            blockfile_provider,
            queue,
            collection_and_segments,
            use_bm25,
            knn_filter_output,
            query,
            key,
            limit,
            batch_measures: Vec::with_capacity(2),
            merge: Merge { k: limit },
            result_channel: None,
        }
    }

    fn sparse_knn_tasks(
        &mut self,
        query: SparseVector,
        ctx: &ComponentContext<Self>,
    ) -> Vec<TaskMessage> {
        let sparse_log_knn_task = wrap(
            Box::new(SparseLogKnn {
                query: query.clone(),
                key: self.key.clone(),
                limit: self.limit,
            }),
            SparseLogKnnInput {
                blockfile_provider: self.blockfile_provider.clone(),
                logs: self.knn_filter_output.logs.clone(),
                mask: self.knn_filter_output.filter_output.log_offset_ids.clone(),
                record_segment: self.collection_and_segments.record_segment.clone(),
            },
            ctx.receiver(),
            self.context.task_cancellation_token.clone(),
        );

        let sparse_index_knn_task = wrap(
            Box::new(SparseIndexKnn {
                query,
                key: self.key.clone(),
                limit: self.limit,
            }),
            SparseIndexKnnInput {
                blockfile_provider: self.blockfile_provider.clone(),
                mask: self
                    .knn_filter_output
                    .filter_output
                    .compact_offset_ids
                    .clone(),
                metadata_segment: self.collection_and_segments.metadata_segment.clone(),
            },
            ctx.receiver(),
            self.context.task_cancellation_token.clone(),
        );
        vec![sparse_log_knn_task, sparse_index_knn_task]
    }

    async fn try_start_merge_operator(&mut self, ctx: &ComponentContext<Self>) {
        if self.batch_measures.len() == 2 {
            let task = wrap(
                Box::new(self.merge.clone()),
                KnnMergeInput {
                    batch_measures: self.batch_measures.drain(..).collect(),
                },
                ctx.receiver(),
                self.context.task_cancellation_token.clone(),
            );
            self.send(task, ctx, Some(Span::current())).await;
        }
    }
}

#[async_trait]
impl Orchestrator for SparseKnnOrchestrator {
    type Output = Vec<RecordMeasure>;
    type Error = SparseKnnError;

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
        let use_bm25 = self.use_bm25
            || self
                .collection_and_segments
                .collection
                .schema
                .as_ref()
                .is_some_and(|schema| {
                    if let Some(flag) = schema.keys.get(&self.key).and_then(|uvt| {
                        uvt.sparse_vector.as_ref().and_then(|vt| {
                            vt.sparse_vector_index
                                .as_ref()
                                .and_then(|it| it.config.bm25)
                        })
                    }) {
                        return flag;
                    }
                    schema.defaults.sparse_vector.as_ref().is_some_and(|vt| {
                        vt.sparse_vector_index
                            .as_ref()
                            .is_some_and(|it| it.config.bm25.unwrap_or_default())
                    })
                });
        if use_bm25 {
            let idf_task = wrap(
                Box::new(Idf {
                    query: self.query.clone(),
                    key: self.key.clone(),
                }),
                IdfInput {
                    blockfile_provider: self.blockfile_provider.clone(),
                    logs: self.knn_filter_output.logs.clone(),
                    mask: self.knn_filter_output.filter_output.log_offset_ids.clone(),
                    metadata_segment: self.collection_and_segments.metadata_segment.clone(),
                    record_segment: self.collection_and_segments.record_segment.clone(),
                },
                ctx.receiver(),
                self.context.task_cancellation_token.clone(),
            );
            vec![(idf_task, Some(Span::current()))]
        } else {
            self.sparse_knn_tasks(self.query.clone(), ctx)
                .into_iter()
                .map(|task| (task, Some(Span::current())))
                .collect()
        }
    }

    fn queue_size(&self) -> usize {
        self.queue
    }

    fn set_result_channel(&mut self, sender: Sender<Result<Vec<RecordMeasure>, SparseKnnError>>) {
        self.result_channel = Some(sender)
    }

    fn take_result_channel(
        &mut self,
    ) -> Option<Sender<Result<Vec<RecordMeasure>, SparseKnnError>>> {
        self.result_channel.take()
    }
}

#[async_trait]
impl Handler<TaskResult<IdfOutput, IdfError>> for SparseKnnOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<IdfOutput, IdfError>,
        ctx: &ComponentContext<Self>,
    ) {
        let output = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(output) => output,
            None => return,
        };
        for task in self.sparse_knn_tasks(output.scaled_query, ctx) {
            self.send(task, ctx, Some(Span::current())).await;
        }
    }
}

#[async_trait]
impl Handler<TaskResult<SparseLogKnnOutput, SparseLogKnnError>> for SparseKnnOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<SparseLogKnnOutput, SparseLogKnnError>,
        ctx: &ComponentContext<Self>,
    ) {
        let output = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(output) => output,
            None => return,
        };
        self.batch_measures.push(output.records);
        self.try_start_merge_operator(ctx).await;
    }
}

#[async_trait]
impl Handler<TaskResult<SparseIndexKnnOutput, SparseIndexKnnError>> for SparseKnnOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<SparseIndexKnnOutput, SparseIndexKnnError>,
        ctx: &ComponentContext<Self>,
    ) {
        let output = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(output) => output,
            None => return,
        };
        self.batch_measures.push(output.records);
        self.try_start_merge_operator(ctx).await;
    }
}

#[async_trait]
impl Handler<TaskResult<KnnMergeOutput, KnnMergeError>> for SparseKnnOrchestrator {
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
