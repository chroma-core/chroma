use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_system::{
    wrap, ChannelError, ComponentContext, ComponentHandle, Dispatcher, Handler, Orchestrator,
    OrchestratorContext, PanicError, TaskError, TaskMessage, TaskResult,
};
use chroma_types::{
    operator::{KnnProjection, KnnProjectionOutput, RecordMeasure},
    Segment,
};
use thiserror::Error;
use tokio::sync::oneshot::error::RecvError;
use tokio::sync::oneshot::Sender;
use tracing::Span;

use crate::execution::operators::{
    fetch_log::FetchLogOutput,
    knn_projection::{KnnProjectionError, KnnProjectionInput},
};

/// The `ProjectionOrchestrator` takes KNN results (Vec<RecordMeasure>) and projects them
/// to retrieve the actual record content with metadata and embeddings as requested.
///
/// This orchestrator is designed to be used after KNN orchestrators have completed their
/// search and merge operations, separating the concerns of finding nearest neighbors
/// from retrieving their full content.
///
/// # Pipeline
/// ```text
///                    Vec<RecordMeasure>
///                            │
///                            ▼
///                ┌────────────────────────┐
///                │                        │
///                │ ProjectionOrchestrator │
///                │                        │
///                └───────────┬────────────┘
///                            │
///                            ▼
///                ┌───────────────────────┐
///                │                       │
///                │ KnnProjectionOperator │
///                │                       │
///                └───────────┬───────────┘
///                            │
///                            ▼
///                   KnnProjectionOutput
/// ```
#[derive(Debug)]
pub struct ProjectionOrchestrator {
    // Orchestrator parameters
    context: OrchestratorContext,
    blockfile_provider: BlockfileProvider,
    queue: usize,

    // Input data
    logs: FetchLogOutput,
    record_segment: Segment,
    record_distances: Vec<RecordMeasure>,
    knn_projection: KnnProjection,

    // Result channel
    result_channel: Option<Sender<Result<KnnProjectionOutput, ProjectionError>>>,
}

#[derive(Error, Debug)]
pub enum ProjectionError {
    #[error("Operation aborted because resources exhausted")]
    Aborted,
    #[error("Channel error: {0}")]
    Channel(#[from] ChannelError),
    #[error("Panic occurred: {0}")]
    Panic(#[from] PanicError),
    #[error("Projection operation failed: {0}")]
    Projection(#[from] KnnProjectionError),
    #[error("Receive error: {0}")]
    Recv(#[from] RecvError),
}

impl ChromaError for ProjectionError {
    fn code(&self) -> ErrorCodes {
        match self {
            ProjectionError::Aborted => ErrorCodes::ResourceExhausted,
            ProjectionError::Channel(e) => e.code(),
            ProjectionError::Panic(e) => e.code(),
            ProjectionError::Recv(_) => ErrorCodes::Internal,
            ProjectionError::Projection(e) => e.code(),
        }
    }
}

impl<E> From<TaskError<E>> for ProjectionError
where
    E: Into<ProjectionError>,
{
    fn from(value: TaskError<E>) -> Self {
        match value {
            TaskError::Aborted => ProjectionError::Aborted,
            TaskError::Panic(e) => e.into(),
            TaskError::TaskFailed(e) => e.into(),
        }
    }
}

impl ProjectionOrchestrator {
    pub fn new(
        dispatcher: ComponentHandle<Dispatcher>,
        queue: usize,
        blockfile_provider: BlockfileProvider,
        logs: FetchLogOutput,
        record_segment: Segment,
        record_distances: Vec<RecordMeasure>,
        knn_projection: KnnProjection,
    ) -> Self {
        let context = OrchestratorContext::new(dispatcher);
        Self {
            context,
            blockfile_provider,
            queue,
            logs,
            record_segment,
            record_distances,
            knn_projection,
            result_channel: None,
        }
    }
}

#[async_trait]
impl Orchestrator for ProjectionOrchestrator {
    type Output = KnnProjectionOutput;
    type Error = ProjectionError;

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
        let projection_task = wrap(
            Box::new(self.knn_projection.clone()),
            KnnProjectionInput {
                logs: self.logs.clone(),
                blockfile_provider: self.blockfile_provider.clone(),
                record_segment: self.record_segment.clone(),
                record_distances: self.record_distances.clone(),
            },
            ctx.receiver(),
            self.context.task_cancellation_token.clone(),
        );

        vec![(projection_task, Some(Span::current()))]
    }

    fn queue_size(&self) -> usize {
        self.queue
    }

    fn set_result_channel(&mut self, sender: Sender<Result<KnnProjectionOutput, ProjectionError>>) {
        self.result_channel = Some(sender);
    }

    fn take_result_channel(
        &mut self,
    ) -> Option<Sender<Result<KnnProjectionOutput, ProjectionError>>> {
        self.result_channel.take()
    }
}

#[async_trait]
impl Handler<TaskResult<KnnProjectionOutput, KnnProjectionError>> for ProjectionOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<KnnProjectionOutput, KnnProjectionError>,
        ctx: &ComponentContext<Self>,
    ) {
        // Simply terminate with the result from the projection operator
        self.terminate_with_result(message.into_inner().map_err(|e| e.into()), ctx)
            .await;
    }
}
