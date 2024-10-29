use chroma_error::{ChromaError, ErrorCodes};
use thiserror::Error;
use tokio::sync::oneshot::{self, error::RecvError, Sender};
use tonic::async_trait;
use tracing::Span;

use crate::{
    execution::{
        dispatcher::Dispatcher,
        operator::{wrap, TaskError, TaskResult},
        operators::{
            fetch_log::{FetchLogError, FetchLogOperator, FetchLogOutput},
            fetch_segment::{FetchSegmentError, FetchSegmentOperator, FetchSegmentOutput},
            filter::{FilterError, FilterInput, FilterOperator, FilterOutput, PreFilterState},
            knn::KnnOperator,
            knn_hnsw::{KnnHnswError, KnnHnswInput, KnnHnswOutput},
            knn_log::{KnnLogError, KnnLogInput, KnnLogOutput},
            knn_merge::{
                KnnMergeError, KnnMergeInput, KnnMergeOperator, KnnMergeOutput, PreKnnMergeState,
            },
            knn_projection::{KnnProjectionError, KnnProjectionOperator, KnnProjectionOutput},
        },
        orchestration::common::terminate_with_error,
    },
    system::{ChannelError, Component, ComponentContext, ComponentHandle, Handler, System},
};

#[derive(Error, Debug)]
pub enum KnnError {
    #[error("Error sending message through channel: {0}")]
    Channel(#[from] ChannelError),
    #[error("Error running Fetch Log Operator: {0}")]
    FetchLog(#[from] FetchLogError),
    #[error("Error running Fetch Segment Operator: {0}")]
    FetchSegment(#[from] FetchSegmentError),
    #[error("Error running Filter Operator: {0}")]
    Filter(#[from] FilterError),
    #[error("Error running Knn Log Operator: {0}")]
    KnnLog(#[from] KnnLogError),
    #[error("Error running Knn Hnsw Operator: {0}")]
    KnnHnsw(#[from] KnnHnswError),
    #[error("Error running Knn Merge Operator: {0}")]
    KnnMerge(#[from] KnnMergeError),
    #[error("Error running Knn Projection Operator: {0}")]
    KnnProjection(#[from] KnnProjectionError),
    #[error("Panic running task: {0}")]
    Panic(String),
    #[error("Error receiving final result: {0}")]
    Result(#[from] RecvError),
}

impl ChromaError for KnnError {
    fn code(&self) -> ErrorCodes {
        match self {
            KnnError::Channel(e) => e.code(),
            KnnError::FetchLog(e) => e.code(),
            KnnError::FetchSegment(e) => e.code(),
            KnnError::Filter(e) => e.code(),
            KnnError::KnnLog(e) => e.code(),
            KnnError::KnnHnsw(e) => e.code(),
            KnnError::KnnMerge(e) => e.code(),
            KnnError::KnnProjection(e) => e.code(),
            KnnError::Panic(_) => ErrorCodes::Aborted,
            KnnError::Result(_) => ErrorCodes::Internal,
        }
    }
}

impl<E> From<TaskError<E>> for KnnError
where
    E: Into<KnnError>,
{
    fn from(value: TaskError<E>) -> Self {
        match value {
            TaskError::Panic(e) => KnnError::Panic(e.unwrap_or_default()),
            TaskError::TaskFailed(e) => e.into(),
        }
    }
}

type KnnFilterOutput = FilterOutput;

type KnnFilterResult = Result<KnnFilterOutput, KnnError>;

#[derive(Debug)]
pub struct KnnFilterOrchestrator {
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

    // Result channel
    result_channel: Option<Sender<KnnFilterResult>>,
}

impl KnnFilterOrchestrator {
    pub fn new(
        dispatcher: ComponentHandle<Dispatcher>,
        queue: usize,
        fetch_log: FetchLogOperator,
        fetch_segment: FetchSegmentOperator,
        filter: FilterOperator,
    ) -> Self {
        Self {
            dispatcher,
            queue,
            fetch_log,
            fetch_segment,
            prefilter_state: PreFilterState::default(),
            filter,
            result_channel: None,
        }
    }

    pub async fn run(mut self, system: System) -> KnnFilterResult {
        let (tx, rx) = oneshot::channel();
        self.result_channel = Some(tx);
        let mut handle = system.start_component(self);
        let result = rx.await;
        handle.stop();
        result?
    }

    fn terminate_with_error<E>(&mut self, ctx: &ComponentContext<Self>, err: E)
    where
        E: Into<KnnError>,
    {
        let knn_err = err.into();
        tracing::error!("Error running orchestrator: {}", &knn_err);
        terminate_with_error(self.result_channel.take(), knn_err, ctx);
    }
}

#[async_trait]
impl Component for KnnFilterOrchestrator {
    fn get_name() -> &'static str {
        "Knn Filter Orchestrator"
    }

    fn queue_size(&self) -> usize {
        self.queue
    }

    async fn on_start(&mut self, ctx: &ComponentContext<Self>) {
        let log_task = wrap(Box::new(self.fetch_log.clone()), (), ctx.receiver());
        let segment_task = wrap(Box::new(self.fetch_segment.clone()), (), ctx.receiver());
        if let Err(err) = self.dispatcher.send(log_task, Some(Span::current())).await {
            self.terminate_with_error(ctx, err);
        } else if let Err(err) = self
            .dispatcher
            .send(segment_task, Some(Span::current()))
            .await
        {
            self.terminate_with_error(ctx, err);
        }
    }
}

#[async_trait]
impl Handler<TaskResult<FetchLogOutput, FetchLogError>> for KnnFilterOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<FetchLogOutput, FetchLogError>,
        ctx: &ComponentContext<Self>,
    ) {
        let output = match message.into_inner() {
            Ok(output) => output,
            Err(err) => {
                self.terminate_with_error(ctx, err);
                return;
            }
        };
        self.prefilter_state.logs = Some(output);
        let next_input = FilterInput::try_from(self.prefilter_state.clone());
        if let Ok(input) = next_input {
            let task = wrap(Box::new(self.filter.clone()), input, ctx.receiver());
            if let Err(err) = self.dispatcher.send(task, Some(Span::current())).await {
                self.terminate_with_error(ctx, err);
            }
        }
    }
}

#[async_trait]
impl Handler<TaskResult<FetchSegmentOutput, FetchSegmentError>> for KnnFilterOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<FetchSegmentOutput, FetchSegmentError>,
        ctx: &ComponentContext<Self>,
    ) {
        let output = match message.into_inner() {
            Ok(output) => output,
            Err(err) => {
                self.terminate_with_error(ctx, err);
                return;
            }
        };
        self.prefilter_state.segments = Some(output);
        let next_input = FilterInput::try_from(self.prefilter_state.clone());
        if let Ok(input) = next_input {
            let task = wrap(Box::new(self.filter.clone()), input, ctx.receiver());
            if let Err(err) = self.dispatcher.send(task, Some(Span::current())).await {
                self.terminate_with_error(ctx, err);
            }
        }
    }
}

#[async_trait]
impl Handler<TaskResult<FilterOutput, FilterError>> for KnnFilterOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<FilterOutput, FilterError>,
        ctx: &ComponentContext<Self>,
    ) {
        let output = match message.into_inner() {
            Ok(output) => output,
            Err(err) => {
                self.terminate_with_error(ctx, err);
                return;
            }
        };
        if let Some(chan) = self.result_channel.take() {
            if chan.send(Ok(output)).is_err() {
                tracing::error!("Error sending final result");
            };
        }
    }
}

type KnnOutput = KnnProjectionOutput;
type KnnResult = Result<KnnOutput, KnnError>;

#[derive(Debug)]
pub struct KnnOrchestrator {
    // Orchestrator parameters
    dispatcher: ComponentHandle<Dispatcher>,
    queue: usize,

    // Output from KnnFilterOrchestrator
    knn_filter_output: KnnFilterOutput,

    // Knn operator shared between log and segments
    knn: KnnOperator,

    // Partial merge input
    preknnmerge_state: PreKnnMergeState,

    // Merge and project
    merge: KnnMergeOperator,
    knn_projection: KnnProjectionOperator,

    // Result channel
    result_channel: Option<Sender<KnnResult>>,
}

impl KnnOrchestrator {
    pub fn new(
        dispatcher: ComponentHandle<Dispatcher>,
        queue: usize,
        knn_filter_output: KnnFilterOutput,
        knn: KnnOperator,
        knn_projection: KnnProjectionOperator,
    ) -> Self {
        let fetch = knn.fetch;
        Self {
            dispatcher,
            queue,
            knn_filter_output,
            knn,
            preknnmerge_state: PreKnnMergeState::default(),
            merge: KnnMergeOperator { fetch },
            knn_projection,
            result_channel: None,
        }
    }

    fn terminate_with_error<E>(&mut self, ctx: &ComponentContext<Self>, err: E)
    where
        E: Into<KnnError>,
    {
        let knn_err = err.into();
        tracing::error!("Error running orchestrator: {}", &knn_err);
        terminate_with_error(self.result_channel.take(), knn_err, ctx);
    }

    pub async fn run(mut self, system: System) -> KnnResult {
        let (tx, rx) = oneshot::channel();
        self.result_channel = Some(tx);
        let mut handle = system.start_component(self);
        let result = rx.await;
        handle.stop();
        result?
    }
}

#[async_trait]
impl Component for KnnOrchestrator {
    fn get_name() -> &'static str {
        "Knn Orchestrator"
    }

    fn queue_size(&self) -> usize {
        self.queue
    }

    async fn on_start(&mut self, ctx: &ComponentContext<Self>) {
        let knn_log_task = wrap(
            Box::new(self.knn.clone()),
            KnnLogInput::from(self.knn_filter_output.clone()),
            ctx.receiver(),
        );
        let knn_segment_task = wrap(
            Box::new(self.knn.clone()),
            KnnHnswInput::from(self.knn_filter_output.clone()),
            ctx.receiver(),
        );
        if let Err(err) = self
            .dispatcher
            .send(knn_log_task, Some(Span::current()))
            .await
        {
            self.terminate_with_error(ctx, err);
        } else if let Err(err) = self
            .dispatcher
            .send(knn_segment_task, Some(Span::current()))
            .await
        {
            self.terminate_with_error(ctx, err);
        }
    }
}

#[async_trait]
impl Handler<TaskResult<KnnLogOutput, KnnLogError>> for KnnOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<KnnLogOutput, KnnLogError>,
        ctx: &ComponentContext<Self>,
    ) {
        let output = match message.into_inner() {
            Ok(output) => output,
            Err(err) => {
                self.terminate_with_error(ctx, err);
                return;
            }
        };
        self.preknnmerge_state.logs = Some(output.logs);
        self.preknnmerge_state.log_distance = Some(output.distances);
        let next_input = KnnMergeInput::try_from(self.preknnmerge_state.clone());
        if let Ok(input) = next_input {
            let task = wrap(Box::new(self.merge.clone()), input, ctx.receiver());
            if let Err(err) = self.dispatcher.send(task, Some(Span::current())).await {
                self.terminate_with_error(ctx, err);
            }
        }
    }
}

#[async_trait]
impl Handler<TaskResult<KnnHnswOutput, KnnHnswError>> for KnnOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<KnnHnswOutput, KnnHnswError>,
        ctx: &ComponentContext<Self>,
    ) {
        let output = match message.into_inner() {
            Ok(output) => output,
            Err(err) => {
                self.terminate_with_error(ctx, err);
                return;
            }
        };
        self.preknnmerge_state.segments = Some(output.segments);
        self.preknnmerge_state.segment_distance = Some(output.distances);
        let next_input = KnnMergeInput::try_from(self.preknnmerge_state.clone());
        if let Ok(input) = next_input {
            let task = wrap(Box::new(self.merge.clone()), input, ctx.receiver());
            if let Err(err) = self.dispatcher.send(task, Some(Span::current())).await {
                self.terminate_with_error(ctx, err);
            }
        }
    }
}

#[async_trait]
impl Handler<TaskResult<KnnMergeOutput, KnnMergeError>> for KnnOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<KnnMergeOutput, KnnMergeError>,
        ctx: &ComponentContext<Self>,
    ) {
        let output = match message.into_inner() {
            Ok(output) => output,
            Err(err) => {
                self.terminate_with_error(ctx, err);
                return;
            }
        };
        let task = wrap(
            Box::new(self.knn_projection.clone()),
            output.into(),
            ctx.receiver(),
        );
        if let Err(err) = self.dispatcher.send(task, Some(Span::current())).await {
            self.terminate_with_error(ctx, err);
        }
    }
}

#[async_trait]
impl Handler<TaskResult<KnnProjectionOutput, KnnProjectionError>> for KnnOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<KnnProjectionOutput, KnnProjectionError>,
        ctx: &ComponentContext<Self>,
    ) {
        let output = match message.into_inner() {
            Ok(output) => output,
            Err(err) => {
                self.terminate_with_error(ctx, err);
                return;
            }
        };
        if let Some(chan) = self.result_channel.take() {
            if chan.send(Ok(output)).is_err() {
                tracing::error!("Error sending final result");
            };
        }
    }
}
