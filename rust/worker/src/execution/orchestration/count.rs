use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_system::{
    wrap, ChannelError, ComponentContext, ComponentHandle, Dispatcher, Handler, Orchestrator,
    OrchestratorContext, PanicError, TaskError, TaskMessage, TaskResult,
};
use chroma_types::CollectionAndSegments;
use thiserror::Error;
use tokio::sync::oneshot::{error::RecvError, Sender};
use tracing::Span;

use crate::execution::operators::{
    count_records::{
        CountRecordsError, CountRecordsInput, CountRecordsOperator, CountRecordsOutput,
    },
    fetch_log::{FetchLogError, FetchLogOperator, FetchLogOutput},
};

#[derive(Error, Debug)]
pub enum CountError {
    #[error("Error sending message through channel: {0}")]
    Channel(#[from] ChannelError),
    #[error("Error running Fetch Log Operator: {0}")]
    FetchLog(#[from] FetchLogError),
    #[error("Error running Count Record Operator: {0}")]
    CountRecord(#[from] CountRecordsError),
    #[error("Panic: {0}")]
    Panic(#[from] PanicError),
    #[error("Error receiving final result: {0}")]
    Result(#[from] RecvError),
    #[error("Operation aborted because resources exhausted")]
    Aborted,
}

impl ChromaError for CountError {
    fn code(&self) -> ErrorCodes {
        match self {
            CountError::Channel(e) => e.code(),
            CountError::FetchLog(e) => e.code(),
            CountError::CountRecord(e) => e.code(),
            CountError::Panic(_) => ErrorCodes::Aborted,
            CountError::Result(_) => ErrorCodes::Internal,
            CountError::Aborted => ErrorCodes::ResourceExhausted,
        }
    }
}

impl<E> From<TaskError<E>> for CountError
where
    E: Into<CountError>,
{
    fn from(value: TaskError<E>) -> Self {
        match value {
            TaskError::Panic(e) => CountError::Panic(e),
            TaskError::TaskFailed(e) => e.into(),
            TaskError::Aborted => CountError::Aborted,
        }
    }
}

type CountOutput = (u32, u64);
type CountResult = Result<CountOutput, CountError>;

#[derive(Debug)]
pub struct CountOrchestrator {
    // Orchestrator parameters
    context: OrchestratorContext,
    blockfile_provider: BlockfileProvider,
    queue: usize,
    // Collection and segments
    collection_and_segments: CollectionAndSegments,

    // Fetch logs
    fetch_log: FetchLogOperator,

    // Fetched log size
    fetch_log_bytes: Option<u64>,

    // Result channel
    result_channel: Option<Sender<CountResult>>,
}

impl CountOrchestrator {
    pub(crate) fn new(
        blockfile_provider: BlockfileProvider,
        dispatcher: chroma_system::ComponentHandle<Dispatcher>,
        queue: usize,
        collection_and_segments: CollectionAndSegments,
        fetch_log: FetchLogOperator,
    ) -> Self {
        let context = OrchestratorContext::new(dispatcher);
        Self {
            context,
            blockfile_provider,
            collection_and_segments,
            queue,
            fetch_log,
            fetch_log_bytes: None,
            result_channel: None,
        }
    }
}

#[async_trait]
impl Orchestrator for CountOrchestrator {
    type Output = CountOutput;
    type Error = CountError;

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
        vec![(
            wrap(
                Box::new(self.fetch_log.clone()),
                (),
                ctx.receiver(),
                self.context.task_cancellation_token.clone(),
            ),
            Some(Span::current()),
        )]
    }

    fn queue_size(&self) -> usize {
        self.queue
    }

    fn set_result_channel(&mut self, sender: Sender<CountResult>) {
        self.result_channel = Some(sender)
    }

    fn take_result_channel(&mut self) -> Option<Sender<CountResult>> {
        self.result_channel.take()
    }
}

#[async_trait]
impl Handler<TaskResult<FetchLogOutput, FetchLogError>> for CountOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<FetchLogOutput, FetchLogError>,
        ctx: &ComponentContext<Self>,
    ) {
        let output = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(output) => output,
            None => return,
        };
        self.fetch_log_bytes
            .replace(output.iter().map(|(l, _)| l.size_bytes()).sum());
        let task = wrap(
            CountRecordsOperator::new(),
            CountRecordsInput::new(
                self.collection_and_segments.record_segment.clone(),
                self.blockfile_provider.clone(),
                output,
            ),
            ctx.receiver(),
            self.context.task_cancellation_token.clone(),
        );
        self.send(task, ctx, Some(Span::current())).await;
    }
}

#[async_trait]
impl Handler<TaskResult<CountRecordsOutput, CountRecordsError>> for CountOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<CountRecordsOutput, CountRecordsError>,
        ctx: &ComponentContext<Self>,
    ) {
        self.terminate_with_result(
            message.into_inner().map_err(|e| e.into()).map(|output| {
                (
                    output.count as u32,
                    self.fetch_log_bytes
                        .expect("FetchLogOperator should have finished already"),
                )
            }),
            ctx,
        )
        .await;
    }
}
