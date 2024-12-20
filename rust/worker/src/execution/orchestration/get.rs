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
            fetch_log::{FetchLogError, FetchLogOperator, FetchLogOutput},
            filter::{FilterError, FilterInput, FilterOperator, FilterOutput},
            limit::{LimitError, LimitInput, LimitOperator, LimitOutput},
            prefetch_record::{PrefetchRecordError, PrefetchRecordOperator, PrefetchRecordOutput},
            projection::{ProjectionError, ProjectionInput, ProjectionOperator, ProjectionOutput},
        },
    },
    system::{ChannelError, ComponentContext, ComponentHandle, Handler},
};

use super::orchestrator::Orchestrator;

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
    collection_and_segments: CollectionAndSegments,

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
        collection_and_segments: CollectionAndSegments,
        fetch_log: FetchLogOperator,
        filter: FilterOperator,
        limit: LimitOperator,
        projection: ProjectionOperator,
    ) -> Self {
        Self {
            blockfile_provider,
            dispatcher,
            queue,
            collection_and_segments,
            fetch_log,
            fetched_logs: None,
            filter,
            limit,
            projection,
            result_channel: None,
        }
    }
}

#[async_trait]
impl Orchestrator for GetOrchestrator {
    type Output = GetOutput;
    type Error = GetError;

    fn dispatcher(&self) -> ComponentHandle<Dispatcher> {
        self.dispatcher.clone()
    }

    fn initial_tasks(&self, ctx: &ComponentContext<Self>) -> Vec<TaskMessage> {
        vec![wrap(Box::new(self.fetch_log.clone()), (), ctx.receiver())]
    }

    fn queue_size(&self) -> usize {
        self.queue
    }

    fn set_result_channel(&mut self, sender: Sender<GetResult>) {
        self.result_channel = Some(sender)
    }

    fn take_result_channel(&mut self) -> Sender<GetResult> {
        self.result_channel
            .take()
            .expect("The result channel should be set before take")
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
        let output = match self.ok_or_terminate(message.into_inner(), ctx) {
            Some(output) => output,
            None => return,
        };

        self.fetched_logs = Some(output.clone());

        let task = wrap(
            Box::new(self.filter.clone()),
            FilterInput {
                logs: output,
                blockfile_provider: self.blockfile_provider.clone(),
                metadata_segment: self.collection_and_segments.metadata_segment.clone(),
                record_segment: self.collection_and_segments.record_segment.clone(),
            },
            ctx.receiver(),
        );
        self.send(task, ctx).await;
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
        let output = match self.ok_or_terminate(message.into_inner(), ctx) {
            Some(output) => output,
            None => return,
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
                record_segment: self.collection_and_segments.record_segment.clone(),
                log_offset_ids: output.log_offset_ids,
                compact_offset_ids: output.compact_offset_ids,
            },
            ctx.receiver(),
        );
        self.send(task, ctx).await;
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
        let output = match self.ok_or_terminate(message.into_inner(), ctx) {
            Some(output) => output,
            None => return,
        };

        let input = ProjectionInput {
            logs: self
                .fetched_logs
                .as_ref()
                .expect("FetchLogOperator should have finished already")
                .clone(),
            blockfile_provider: self.blockfile_provider.clone(),
            record_segment: self.collection_and_segments.record_segment.clone(),
            offset_ids: output.offset_ids.iter().collect(),
        };

        // Prefetch records before projection
        let prefetch_task = wrap(
            Box::new(PrefetchRecordOperator {}),
            input.clone(),
            ctx.receiver(),
        );

        if !self.send(prefetch_task, ctx).await {
            return;
        }

        let task = wrap(Box::new(self.projection.clone()), input, ctx.receiver());
        self.send(task, ctx).await;
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
        self.terminate_with_result(message.into_inner().map_err(|e| e.into()), ctx);
    }
}
