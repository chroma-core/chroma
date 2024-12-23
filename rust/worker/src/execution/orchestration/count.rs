use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_types::CollectionAndSegments;
use thiserror::Error;
use tokio::sync::oneshot::{error::RecvError, Sender};

use crate::{
    execution::{
        dispatcher::Dispatcher,
        operator::{wrap, TaskError, TaskMessage, TaskResult},
        operators::{
            count_records::{
                CountRecordsError, CountRecordsInput, CountRecordsOperator, CountRecordsOutput,
            },
            fetch_log::{FetchLogError, FetchLogOperator, FetchLogOutput},
        },
    },
    system::{ChannelError, ComponentContext, ComponentHandle, Handler},
};

use super::orchestrator::Orchestrator;

#[derive(Error, Debug)]
pub enum CountError {
    #[error("Error sending message through channel: {0}")]
    Channel(#[from] ChannelError),
    #[error("Error running Fetch Log Operator: {0}")]
    FetchLog(#[from] FetchLogError),
    #[error("Error running Count Record Operator: {0}")]
    CountRecord(#[from] CountRecordsError),
    #[error("Panic running task: {0}")]
    Panic(String),
    #[error("Error receiving final result: {0}")]
    Result(#[from] RecvError),
}

impl ChromaError for CountError {
    fn code(&self) -> ErrorCodes {
        match self {
            CountError::Channel(e) => e.code(),
            CountError::FetchLog(e) => e.code(),
            CountError::CountRecord(e) => e.code(),
            CountError::Panic(_) => ErrorCodes::Aborted,
            CountError::Result(_) => ErrorCodes::Internal,
        }
    }
}

impl<E> From<TaskError<E>> for CountError
where
    E: Into<CountError>,
{
    fn from(value: TaskError<E>) -> Self {
        match value {
            TaskError::Panic(e) => CountError::Panic(e.unwrap_or_default()),
            TaskError::TaskFailed(e) => e.into(),
        }
    }
}

type CountOutput = usize;
type CountResult = Result<CountOutput, CountError>;

#[derive(Debug)]
pub struct CountOrchestrator {
    // Orchestrator parameters
    blockfile_provider: BlockfileProvider,
    dispatcher: ComponentHandle<Dispatcher>,
    queue: usize,

    // Collection and segments
    collection_and_segments: CollectionAndSegments,

    // Fetch logs
    fetch_log: FetchLogOperator,

    // Result channel
    result_channel: Option<Sender<Result<usize, CountError>>>,
}

impl CountOrchestrator {
    pub(crate) fn new(
        blockfile_provider: BlockfileProvider,
        dispatcher: ComponentHandle<Dispatcher>,
        queue: usize,
        collection_and_segments: CollectionAndSegments,
        fetch_log: FetchLogOperator,
    ) -> Self {
        Self {
            blockfile_provider,
            dispatcher,
            collection_and_segments,
            queue,
            fetch_log,
            result_channel: None,
        }
    }
}

#[async_trait]
impl Orchestrator for CountOrchestrator {
    type Output = CountOutput;
    type Error = CountError;

    fn dispatcher(&self) -> ComponentHandle<Dispatcher> {
        self.dispatcher.clone()
    }

    fn initial_tasks(&self, ctx: &ComponentContext<Self>) -> Vec<TaskMessage> {
        vec![wrap(Box::new(self.fetch_log.clone()), (), ctx.receiver())]
    }

    fn queue_size(&self) -> usize {
        self.queue
    }

    fn set_result_channel(&mut self, sender: Sender<CountResult>) {
        self.result_channel = Some(sender)
    }

    fn take_result_channel(&mut self) -> Sender<CountResult> {
        self.result_channel
            .take()
            .expect("The result channel should be set before take")
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
        let output = match self.ok_or_terminate(message.into_inner(), ctx) {
            Some(output) => output,
            None => return,
        };
        let task = wrap(
            CountRecordsOperator::new(),
            CountRecordsInput::new(
                self.collection_and_segments.record_segment.clone(),
                self.blockfile_provider.clone(),
                output,
            ),
            ctx.receiver(),
        );
        self.send(task, ctx).await;
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
            message
                .into_inner()
                .map_err(|e| e.into())
                .map(|output| output.count),
            ctx,
        );
    }
}
