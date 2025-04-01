use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::{ChromaError, ErrorCodes};
// use chroma_index::hnsw_provider::HnswIndexProvider;
// use chroma_segment::spann_provider::SpannProvider;
use chroma_sysdb::SysDb;
use chroma_system::{
    wrap, ChannelError, ComponentContext, ComponentHandle, Dispatcher, Handler, Orchestrator,
    PanicError, TaskError, TaskMessage, TaskResult,
};
use chroma_types::CollectionUuid;
use thiserror::Error;
use tokio::sync::oneshot::{error::RecvError, Sender};

use crate::execution::operators::{
    get_collection_and_segments::{
        GetCollectionAndSegmentsError, GetCollectionAndSegmentsOperator,
        GetCollectionAndSegmentsOutput,
    },
    prefetch_segment::{
        PrefetchSegmentError, PrefetchSegmentInput, PrefetchSegmentOperator, PrefetchSegmentOutput,
    },
};

#[derive(Debug, Error)]
pub enum RebuildError {
    #[error("Operation aborted")]
    Aborted,
    #[error("Error sending message through channel: {0}")]
    Channel(#[from] ChannelError),
    #[error("Error getting collection and segments: {0}")]
    GetCollectionAndSegments(#[from] GetCollectionAndSegmentsError),
    #[error("Panic: {0}")]
    Panic(#[from] PanicError),
    #[error("Error prefetching segment: {0}")]
    PrefetchSegment(#[from] PrefetchSegmentError),
    #[error("Error receiving final result: {0}")]
    Result(#[from] RecvError),
}

impl ChromaError for RebuildError {
    fn code(&self) -> ErrorCodes {
        match self {
            RebuildError::Aborted => ErrorCodes::Aborted,
            RebuildError::Channel(e) => e.code(),
            RebuildError::GetCollectionAndSegments(e) => e.code(),
            RebuildError::Panic(_) => ErrorCodes::Aborted,
            RebuildError::PrefetchSegment(e) => e.code(),
            RebuildError::Result(_) => ErrorCodes::Internal,
        }
    }
}

impl<E> From<TaskError<E>> for RebuildError
where
    E: Into<RebuildError>,
{
    fn from(value: TaskError<E>) -> Self {
        match value {
            TaskError::Aborted => RebuildError::Aborted,
            TaskError::Panic(e) => e.into(),
            TaskError::TaskFailed(e) => e.into(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct RebuildOutput {}

type RebuildResult = Result<RebuildOutput, RebuildError>;

#[derive(Debug)]
pub struct RebuildOrchestrator {
    // Orchestrator parameters
    sysdb: SysDb,
    blockfile_provider: BlockfileProvider,
    dispatcher: ComponentHandle<Dispatcher>,
    // hnsw_provider: HnswIndexProvider,
    // spann_provider: SpannProvider,
    queue: usize,

    // Collection ID
    collection_id: CollectionUuid,

    // Result channel
    result_channel: Option<Sender<RebuildResult>>,
}

impl RebuildOrchestrator {
    pub fn new(
        sysdb: SysDb,
        blockfile_provider: BlockfileProvider,
        dispatcher: ComponentHandle<Dispatcher>,
        // hnsw_provider: HnswIndexProvider,
        // spann_provider: SpannProvider,
        queue: usize,
        collection_id: CollectionUuid,
    ) -> Self {
        Self {
            sysdb,
            blockfile_provider,
            dispatcher,
            // hnsw_provider,
            // spann_provider,
            queue,
            collection_id,
            result_channel: None,
        }
    }
}

#[async_trait]
impl Orchestrator for RebuildOrchestrator {
    type Output = RebuildOutput;
    type Error = RebuildError;

    fn dispatcher(&self) -> ComponentHandle<Dispatcher> {
        self.dispatcher.clone()
    }

    fn initial_tasks(&self, ctx: &ComponentContext<Self>) -> Vec<TaskMessage> {
        vec![wrap(
            Box::new(GetCollectionAndSegmentsOperator {
                sysdb: self.sysdb.clone(),
                collection_id: self.collection_id,
            }),
            (),
            ctx.receiver(),
        )]
    }

    fn queue_size(&self) -> usize {
        self.queue
    }

    fn set_result_channel(&mut self, sender: Sender<Result<Self::Output, Self::Error>>) {
        self.result_channel = Some(sender)
    }

    fn take_result_channel(&mut self) -> Sender<Result<Self::Output, Self::Error>> {
        self.result_channel
            .take()
            .expect("The result channel should be set before take")
    }
}

#[async_trait]
impl Handler<TaskResult<GetCollectionAndSegmentsOutput, GetCollectionAndSegmentsError>>
    for RebuildOrchestrator
{
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<GetCollectionAndSegmentsOutput, GetCollectionAndSegmentsError>,
        ctx: &ComponentContext<Self>,
    ) {
        let output = match self.ok_or_terminate(message.into_inner(), ctx) {
            Some(output) => output,
            None => return,
        };

        // Prefetch record segment
        let prefetch_task = wrap(
            Box::new(PrefetchSegmentOperator::new()),
            PrefetchSegmentInput::new(output.record_segment, self.blockfile_provider.clone()),
            ctx.receiver(),
        );
        self.send(prefetch_task, ctx).await;
    }
}

#[async_trait]
impl Handler<TaskResult<PrefetchSegmentOutput, PrefetchSegmentError>> for RebuildOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<PrefetchSegmentOutput, PrefetchSegmentError>,
        ctx: &ComponentContext<RebuildOrchestrator>,
    ) {
        self.ok_or_terminate(message.into_inner(), ctx);
    }
}
