use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_types::CollectionSegments;
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
            filter::{FilterError, FilterInput, FilterOperator, FilterOutput},
            limit::{LimitError, LimitInput, LimitOperator, LimitOutput},
            prefetch_record::{PrefetchRecordError, PrefetchRecordOperator, PrefetchRecordOutput},
            projection::{ProjectionError, ProjectionInput, ProjectionOperator, ProjectionOutput},
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

/// The `GetOrchestrator` chains a sequence of operators in sequence to evaluate
/// a `<collection>.get(...)` query from the user
///
/// # Pipeline
/// ```text
///       ┌────────────┐           
///       │            │           
///       │  on_start  │           
///       │            │           
///       └──────┬─────┘           
///              │                 
///              ▼                 
///    ┌────────────────────┐      
///    │                    │      
///    │  FetchLogOperator  │      
///    │                    │      
///    └─────────┬──────────┘      
///              │                 
///              ▼                 
///    ┌───────────────────┐       
///    │                   │       
///    │   FilterOperator  │       
///    │                   │       
///    └─────────┬─────────┘       
///              │                 
///              ▼                 
///     ┌─────────────────┐        
///     │                 │        
///     │  LimitOperator  │        
///     │                 │        
///     └────────┬────────┘        
///              │                 
///              ▼                 
///   ┌──────────────────────┐     
///   │                      │     
///   │  ProjectionOperator  │     
///   │                      │     
///   └──────────┬───────────┘     
///              │                 
///              ▼                 
///     ┌──────────────────┐       
///     │                  │       
///     │  result_channel  │       
///     │                  │       
///     └──────────────────┘       
/// ```
#[derive(Debug)]
pub struct GetOrchestrator {
    // Orchestrator parameters
    blockfile_provider: BlockfileProvider,
    dispatcher: ComponentHandle<Dispatcher>,
    queue: usize,

    // Collection segments
    collection_segments: CollectionSegments,

    // Fetch logs
    fetch_log: FetchLogOperator,

    // Fetched logs
    fetched_logs: Option<FetchLogOutput>,

    // Pipelined operators
    filter: FilterOperator,
    limit: LimitOperator,
    projection: ProjectionOperator,

    // Result channel
    result_channel: Option<Sender<GetResult>>,
}

impl GetOrchestrator {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        blockfile_provider: BlockfileProvider,
        dispatcher: ComponentHandle<Dispatcher>,
        queue: usize,
        segments: CollectionSegments,
        fetch_log: FetchLogOperator,
        filter: FilterOperator,
        limit: LimitOperator,
        projection: ProjectionOperator,
    ) -> Self {
        Self {
            blockfile_provider,
            dispatcher,
            queue,
            collection_segments: segments,
            fetch_log,
            fetched_logs: None,
            filter,
            limit,
            projection,
            result_channel: None,
        }
    }

    pub async fn run(mut self, system: System) -> GetResult {
        let (tx, rx) = oneshot::channel();
        self.result_channel = Some(tx);
        let mut handle = system.start_component(self);
        let result = rx.await;
        handle.stop();
        result?
    }

    fn terminate_with_error<E>(&mut self, ctx: &ComponentContext<Self>, err: E)
    where
        E: Into<GetError>,
    {
        let get_err = err.into();
        tracing::error!("Error running orchestrator: {}", &get_err);
        terminate_with_error(self.result_channel.take(), get_err, ctx);
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
        let task = wrap(Box::new(self.fetch_log.clone()), (), ctx.receiver());
        if let Err(err) = self.dispatcher.send(task, Some(Span::current())).await {
            self.terminate_with_error(ctx, err);
            return;
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
        let output = match message.into_inner() {
            Ok(output) => output,
            Err(err) => {
                self.terminate_with_error(ctx, err);
                return;
            }
        };

        self.fetched_logs = Some(output.clone());

        let task = wrap(
            Box::new(self.filter.clone()),
            FilterInput {
                logs: output,
                blockfile_provider: self.blockfile_provider.clone(),
                metadata_segment: self.collection_segments.metadata_segment.clone(),
                record_segment: self.collection_segments.record_segment.clone(),
            },
            ctx.receiver(),
        );
        if let Err(err) = self.dispatcher.send(task, Some(Span::current())).await {
            self.terminate_with_error(ctx, err);
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
        let output = match message.into_inner() {
            Ok(output) => output,
            Err(err) => {
                self.terminate_with_error(ctx, err);
                return;
            }
        };
        let task = wrap(
            Box::new(self.limit.clone()),
            LimitInput {
                logs: self
                    .fetched_logs
                    .as_ref()
                    .expect("FetchLogOperator should have finished already")
                    .clone(),
                blockfile_provider: self.blockfile_provider.clone(),
                record_segment: self.collection_segments.record_segment.clone(),
                log_offset_ids: output.log_offset_ids,
                compact_offset_ids: output.compact_offset_ids,
            },
            ctx.receiver(),
        );
        if let Err(err) = self.dispatcher.send(task, Some(Span::current())).await {
            self.terminate_with_error(ctx, err);
        }
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
        let output = match message.into_inner() {
            Ok(output) => output,
            Err(err) => {
                self.terminate_with_error(ctx, err);
                return;
            }
        };

        let input = ProjectionInput {
            logs: self
                .fetched_logs
                .as_ref()
                .expect("FetchLogOperator should have finished already")
                .clone(),
            blockfile_provider: self.blockfile_provider.clone(),
            record_segment: self.collection_segments.record_segment.clone(),
            offset_ids: output.offset_ids.iter().collect(),
        };

        // Prefetch records before projection
        let prefetch_task = wrap(
            Box::new(PrefetchRecordOperator {}),
            input.clone(),
            ctx.receiver(),
        );
        if let Err(err) = self
            .dispatcher
            .send(prefetch_task, Some(Span::current()))
            .await
        {
            self.terminate_with_error(ctx, err);
        }

        let task = wrap(Box::new(self.projection.clone()), input, ctx.receiver());
        if let Err(err) = self.dispatcher.send(task, Some(Span::current())).await {
            self.terminate_with_error(ctx, err);
        }
    }
}

#[async_trait]
impl Handler<TaskResult<PrefetchRecordOutput, PrefetchRecordError>> for GetOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        _message: TaskResult<PrefetchRecordOutput, PrefetchRecordError>,
        _ctx: &ComponentContext<Self>,
    ) {
        // The output and error from `PrefetchRecordOperator` are ignored
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
