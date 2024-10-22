use crate::{
    execution::{
        dispatcher::Dispatcher,
        operator::{wrap, TaskError, TaskMessage, TaskResult},
        operators::{
            filter::{FilterError, FilterInput, FilterOperator, FilterOutput},
            limit::{LimitError, LimitInput, LimitOperator, LimitOutput},
            projection::{ProjectionError, ProjectionInput, ProjectionOperator, ProjectionOutput},
            scan::{ScanError, ScanOperator, ScanOutput},
        },
        orchestration::common::terminate_with_error,
    },
    system::{ChannelError, Component, ComponentContext, ComponentHandle, Handler, System},
};
use chroma_error::{ChromaError, ErrorCodes};
use thiserror::Error;
use tokio::sync::oneshot::{self, error::RecvError, Sender};
use tonic::async_trait;
use tracing::Span;

#[derive(Debug)]
pub struct GetOrchestrator {
    pub dispatcher: ComponentHandle<Dispatcher>,
    pub queue: usize,
    // Query operators
    pub scan: ScanOperator,
    pub filter: FilterOperator,
    pub limit: LimitOperator,
    pub projection: ProjectionOperator,
    // Result channel
    pub result_channel: Option<Sender<GetResult>>,
}

type GetOutput = ProjectionOutput;

#[derive(Error, Debug)]
pub enum GetError {
    #[error("Error sending message through channel: {0}")]
    Channel(#[from] ChannelError),
    #[error("Error running Filter Operator: {0}")]
    Filter(#[from] FilterError),
    #[error("Error running Limit Operator: {0}")]
    Limit(#[from] LimitError),
    #[error("Panic running task: {0}")]
    Panic(String),
    #[error("Error running Projection Operator: {0}")]
    Projection(#[from] ProjectionError),
    #[error("Error receiving final result: {0}")]
    Result(#[from] RecvError),
    #[error("Error running Scan Operator: {0}")]
    Scan(#[from] ScanError),
}

impl ChromaError for GetError {
    fn code(&self) -> ErrorCodes {
        use GetError::*;
        match self {
            Channel(e) => e.code(),
            Filter(e) => e.code(),
            Limit(e) => e.code(),
            Panic(_) => ErrorCodes::Aborted,
            Projection(e) => e.code(),
            Result(_) => ErrorCodes::Internal,
            Scan(e) => e.code(),
        }
    }
}

impl<E> From<TaskError<E>> for GetError
where
    E: Into<GetError>,
{
    fn from(value: TaskError<E>) -> Self {
        use TaskError::*;
        match value {
            Panic(e) => GetError::Panic(e.unwrap_or_default()),
            TaskFailed(e) => e.into(),
        }
    }
}

type GetResult = Result<GetOutput, GetError>;

impl GetOrchestrator {
    async fn next_task<O, E, N>(
        &mut self,
        message: TaskResult<O, E>,
        ctx: &ComponentContext<Self>,
        next_task: N,
    ) where
        E: Into<GetError>,
        N: FnOnce(O) -> TaskMessage,
    {
        if let Some(err) = match message.into_inner() {
            Ok(output) => match self
                .dispatcher
                .send(next_task(output), Some(Span::current()))
                .await
            {
                Ok(_) => None,
                Err(e) => Some(e.into()),
            },
            Err(e) => Some(e.into()),
        } {
            tracing::error!("Error handling operator: {}", err);
            terminate_with_error(self.result_channel.take(), err, ctx);
        }
    }

    pub async fn register_and_run(mut self, system: System) -> GetResult {
        let (tx, rx) = oneshot::channel();
        self.result_channel = Some(tx);
        let mut handle = system.start_component(self);
        let result = rx.await;
        handle.stop();
        result?
    }
}

#[async_trait]
impl Component for GetOrchestrator {
    fn get_name() -> &'static str {
        "Get Orchestrator"
    }

    fn queue_size(&self) -> usize {
        self.queue
    }

    async fn on_start(&mut self, ctx: &ComponentContext<Self>) {
        let scan = self.scan.clone();
        let task = wrap(Box::new(scan), (), ctx.receiver());
        if let Err(err) = self.dispatcher.send(task, Some(Span::current())).await {
            tracing::error!("Error starting orchestrator: {}", err);
            terminate_with_error(self.result_channel.take(), err.into(), ctx);
        };
    }
}

#[async_trait]
impl Handler<TaskResult<ScanOutput, ScanError>> for GetOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<ScanOutput, ScanError>,
        ctx: &ComponentContext<Self>,
    ) {
        let filter = self.filter.clone();
        let task = |output| wrap(Box::new(filter), FilterInput::from(output), ctx.receiver());
        self.next_task(message, ctx, task).await;
    }
}

#[async_trait]
impl Handler<TaskResult<FilterOutput, FilterError>> for GetOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<FilterOutput, FilterError>,
        ctx: &ComponentContext<Self>,
    ) {
        let limit = self.limit.clone();
        let task = |output| wrap(Box::new(limit), LimitInput::from(output), ctx.receiver());
        self.next_task(message, ctx, task).await;
    }
}

#[async_trait]
impl Handler<TaskResult<LimitOutput, LimitError>> for GetOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<LimitOutput, LimitError>,
        ctx: &ComponentContext<Self>,
    ) {
        let projection = self.projection.clone();
        let task = |output| {
            wrap(
                Box::new(projection),
                ProjectionInput::from(output),
                ctx.receiver(),
            )
        };
        self.next_task(message, ctx, task).await;
    }
}

#[async_trait]
impl Handler<TaskResult<ProjectionOutput, ProjectionError>> for GetOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<ProjectionOutput, ProjectionError>,
        ctx: &ComponentContext<Self>,
    ) {
        match message.into_inner() {
            Ok(output) => {
                if let Some(chan) = self.result_channel.take() {
                    if chan.send(Ok(output)).is_err() {
                        tracing::error!("Error sending final result");
                    };
                }
            }
            Err(e) => {
                tracing::error!("Error handling operator: {}", e);
                terminate_with_error(self.result_channel.take(), e.into(), ctx);
            }
        };
    }
}
