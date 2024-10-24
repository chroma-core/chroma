use chroma_error::{ChromaError, ErrorCodes};
use thiserror::Error;
use tokio::sync::oneshot::{self, error::RecvError, Sender};
use tonic::async_trait;
use tracing::Span;

use crate::{
    execution::{
        dispatcher::Dispatcher,
        operator::{wrap, TaskError, TaskMessage, TaskResult},
        operators::{
            fetch_log::{FetchLogError, FetchLogOperator, FetchLogOutput},
            fetch_segment::{FetchSegmentError, FetchSegmentOperator, FetchSegmentOutput},
            filter::{FilterError, FilterInput, FilterOperator, FilterOutput, PreFilterState},
            limit::{LimitError, LimitOperator, LimitOutput},
            projection::{ProjectionError, ProjectionOperator, ProjectionOutput},
        },
        orchestration::common::terminate_with_error,
    },
    system::{ChannelError, Component, ComponentContext, ComponentHandle, Handler, System},
};

#[derive(Error, Debug)]
pub enum GetError {
    #[error("Error sending message through channel: {0}")]
    Channel(#[from] ChannelError),
    #[error("Error running Fetch Log Operator: {0}")]
    FetchLog(#[from] FetchLogError),
    #[error("Error running Fetch Segment Operator: {0}")]
    FetchSegment(#[from] FetchSegmentError),
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
}

impl ChromaError for GetError {
    fn code(&self) -> ErrorCodes {
        match self {
            GetError::Channel(e) => e.code(),
            GetError::FetchLog(e) => e.code(),
            GetError::FetchSegment(e) => e.code(),
            GetError::Filter(e) => e.code(),
            GetError::Limit(e) => e.code(),
            GetError::Panic(_) => ErrorCodes::Aborted,
            GetError::Projection(e) => e.code(),
            GetError::Result(_) => ErrorCodes::Internal,
        }
    }
}

impl<E> From<TaskError<E>> for GetError
where
    E: Into<GetError>,
{
    fn from(value: TaskError<E>) -> Self {
        match value {
            TaskError::Panic(e) => GetError::Panic(e.unwrap_or_default()),
            TaskError::TaskFailed(e) => e.into(),
        }
    }
}

type GetOutput = ProjectionOutput;

type GetResult = Result<GetOutput, GetError>;

#[derive(Debug)]
pub struct GetOrchestrator {
    // Orchestrator parameters
    dispatcher: ComponentHandle<Dispatcher>,
    queue: usize,

    // Fetch logs and segments
    fetch_log: FetchLogOperator,
    fetch_segment: FetchSegmentOperator,

    // Pre-filter state
    prefilter_state: PreFilterState,

    // Pipelined operators
    filter: FilterOperator,
    limit: LimitOperator,
    projection: ProjectionOperator,

    // Result channel
    result_channel: Option<Sender<GetResult>>,
}

impl GetOrchestrator {
    pub fn new(
        dispatcher: ComponentHandle<Dispatcher>,
        queue: usize,
        fetch_log: FetchLogOperator,
        fetch_segment: FetchSegmentOperator,
        filter: FilterOperator,
        limit: LimitOperator,
        projection: ProjectionOperator,
    ) -> Self {
        Self {
            dispatcher,
            queue,
            fetch_log,
            fetch_segment,
            prefilter_state: PreFilterState::default(),
            filter,
            limit,
            projection,
            result_channel: None,
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

    // Cleanup the task result and produce the output if any
    // Terminate the orchestrator if there is any error
    fn cleanup_response<O, E>(
        &mut self,
        ctx: &ComponentContext<Self>,
        message: TaskResult<O, E>,
    ) -> Option<O>
    where
        E: Into<GetError>,
    {
        match message.into_inner() {
            Ok(output) => Some(output),
            Err(err) => {
                self.terminate_with_error(ctx, err.into());
                None
            }
        }
    }

    fn terminate_with_error(&mut self, ctx: &ComponentContext<Self>, err: GetError) {
        tracing::error!("Error running orchestrator: {}", err);
        terminate_with_error(self.result_channel.take(), err, ctx);
    }

    // Sends the task to dispatcher and returns whether the action is successful
    // Terminate the orchestrator if there is any error
    async fn send_task(&mut self, ctx: &ComponentContext<Self>, task: TaskMessage) -> bool {
        if let Err(err) = self.dispatcher.send(task, Some(Span::current())).await {
            self.terminate_with_error(ctx, err.into());
            false
        } else {
            true
        }
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
        let log_task = wrap(Box::new(self.fetch_log.clone()), (), ctx.receiver());
        let segment_task = wrap(Box::new(self.fetch_segment.clone()), (), ctx.receiver());
        let log_task_success = self.send_task(ctx, log_task).await;
        if log_task_success {
            self.send_task(ctx, segment_task).await;
        }
    }
}

#[async_trait]
impl Handler<TaskResult<FetchLogOutput, FetchLogError>> for GetOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<FetchLogOutput, FetchLogError>,
        ctx: &ComponentContext<Self>,
    ) {
        let output = match self.cleanup_response(ctx, message) {
            Some(output) => output,
            None => return,
        };
        self.prefilter_state.logs = Some(output);
        let next_input = FilterInput::try_from(self.prefilter_state.clone());
        if let Ok(input) = next_input {
            let task = wrap(Box::new(self.filter.clone()), input, ctx.receiver());
            self.send_task(ctx, task).await;
        }
    }
}

#[async_trait]
impl Handler<TaskResult<FetchSegmentOutput, FetchSegmentError>> for GetOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<FetchSegmentOutput, FetchSegmentError>,
        ctx: &ComponentContext<Self>,
    ) {
        let output = match self.cleanup_response(ctx, message) {
            Some(output) => output,
            None => return,
        };
        self.prefilter_state.segments = Some(output);
        let next_input = FilterInput::try_from(self.prefilter_state.clone());
        if let Ok(input) = next_input {
            let task = wrap(Box::new(self.filter.clone()), input, ctx.receiver());
            self.send_task(ctx, task).await;
        }
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
        let output = match self.cleanup_response(ctx, message) {
            Some(output) => output,
            None => return,
        };
        let task = wrap(Box::new(self.limit.clone()), output.into(), ctx.receiver());
        self.send_task(ctx, task).await;
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
        let output = match self.cleanup_response(ctx, message) {
            Some(output) => output,
            None => return,
        };
        let task = wrap(
            Box::new(self.projection.clone()),
            output.into(),
            ctx.receiver(),
        );
        self.send_task(ctx, task).await;
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
        let output = match self.cleanup_response(ctx, message) {
            Some(output) => output,
            None => return,
        };
        if let Some(chan) = self.result_channel.take() {
            if chan.send(Ok(output)).is_err() {
                tracing::error!("Error sending final result");
            };
        }
    }
}
