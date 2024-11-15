use super::common::{
    get_collection_by_id, get_hnsw_segment_by_id, get_record_segment_by_collection_id,
};
use crate::{
    execution::{
        dispatcher::Dispatcher,
        operator::{wrap, TaskResult},
        operators::{
            get_vectors_operator::{
                GetVectorsOperator, GetVectorsOperatorError, GetVectorsOperatorInput,
                GetVectorsOperatorOutput,
            },
            pull_log::{PullLogsInput, PullLogsOperator, PullLogsOutput},
        },
        orchestration::common::terminate_with_error,
    },
    log::log::{Log, PullLogsError},
    sysdb::sysdb::SysDb,
    system::{
        ChannelError, Component, ComponentContext, ComponentHandle, Handler, ReceiverForMessage,
        System,
    },
};
use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_types::{Chunk, Collection, CollectionUuid, GetVectorsResult, LogRecord, Segment};
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tracing::{trace, Span};
use uuid::Uuid;

#[derive(Debug)]
#[allow(dead_code)]
enum ExecutionState {
    Pending,
    PullLogs,
    GetVectors,
}

#[derive(Debug, Error)]
enum GetVectorsError {
    #[error("Error sending task to dispatcher")]
    TaskSendError(#[from] ChannelError),
    #[error("System time error")]
    SystemTimeError(#[from] std::time::SystemTimeError),
    #[error("Collection version mismatch")]
    CollectionVersionMismatch,
}

impl ChromaError for GetVectorsError {
    fn code(&self) -> ErrorCodes {
        match self {
            GetVectorsError::TaskSendError(e) => e.code(),
            GetVectorsError::SystemTimeError(_) => ErrorCodes::Internal,
            GetVectorsError::CollectionVersionMismatch => ErrorCodes::VersionMismatch,
        }
    }
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct GetVectorsOrchestrator {
    state: ExecutionState,
    // Component Execution
    system: System,
    // Query state
    search_user_ids: Vec<String>,
    hnsw_segment_id: Uuid,
    collection_id: CollectionUuid,
    // State fetched or created for query execution
    record_segment: Option<Segment>,
    collection: Option<Collection>,
    // Services
    log: Box<Log>,
    sysdb: Box<SysDb>,
    dispatcher: ComponentHandle<Dispatcher>,
    blockfile_provider: BlockfileProvider,
    // Result channel
    result_channel:
        Option<tokio::sync::oneshot::Sender<Result<GetVectorsResult, Box<dyn ChromaError>>>>,
    collection_version: u32,
    log_position: u64,
}

#[allow(dead_code)]
impl GetVectorsOrchestrator {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        system: System,
        get_ids: Vec<String>,
        hnsw_segment_id: Uuid,
        collection_id: CollectionUuid,
        log: Box<Log>,
        sysdb: Box<SysDb>,
        dispatcher: ComponentHandle<Dispatcher>,
        blockfile_provider: BlockfileProvider,
        collection_version: u32,
        log_position: u64,
    ) -> Self {
        Self {
            state: ExecutionState::Pending,
            system,
            search_user_ids: get_ids,
            hnsw_segment_id,
            collection_id,
            log,
            sysdb,
            dispatcher,
            blockfile_provider,
            record_segment: None,
            collection: None,
            result_channel: None,
            collection_version,
            log_position,
        }
    }

    async fn pull_logs(
        &mut self,
        self_address: Box<dyn ReceiverForMessage<TaskResult<PullLogsOutput, PullLogsError>>>,
        ctx: &ComponentContext<Self>,
    ) {
        self.state = ExecutionState::PullLogs;
        let operator = PullLogsOperator::new(self.log.clone());
        let end_timestamp = SystemTime::now().duration_since(UNIX_EPOCH);
        let end_timestamp = match end_timestamp {
            // TODO: change protobuf definition to use u64 instead of i64
            Ok(end_timestamp) => end_timestamp.as_nanos() as i64,
            Err(e) => {
                terminate_with_error(
                    self.result_channel.take(),
                    Box::new(GetVectorsError::SystemTimeError(e)),
                    ctx,
                );
                return;
            }
        };

        let collection = self
            .collection
            .as_ref()
            .expect("State machine invariant violation. The collection is not set when pulling logs. This should never happen.");

        let input = PullLogsInput::new(
            collection.collection_id,
            // The collection log position is inclusive, and we want to start from the next log
            // Note that we query using the incoming log position this is critical for correctness
            // TODO: We should make all the log service code use u64 instead of i64
            (self.log_position as i64) + 1,
            100,
            None,
            Some(end_timestamp),
        );

        let task = wrap(operator, input, self_address);
        // Wrap the task with current span as the parent. The worker then executes it
        // inside a child span with this parent.
        match self.dispatcher.send(task, Some(Span::current())).await {
            Ok(_) => (),
            Err(e) => {
                terminate_with_error(
                    self.result_channel.take(),
                    Box::new(GetVectorsError::TaskSendError(e)),
                    ctx,
                );
            }
        }
    }

    async fn get_vectors(
        &mut self,
        self_address: Box<
            dyn ReceiverForMessage<TaskResult<GetVectorsOperatorOutput, GetVectorsOperatorError>>,
        >,
        log: Chunk<LogRecord>,
        ctx: &ComponentContext<Self>,
    ) {
        self.state = ExecutionState::GetVectors;
        let record_segment = self
            .record_segment
            .as_ref()
            .expect("Invariant violation. Record segment is not set.");
        let blockfile_provider = self.blockfile_provider.clone();
        let operator = GetVectorsOperator::new();
        tracing::info!("get_vectors with search ids {:?}", self.search_user_ids);
        let input = GetVectorsOperatorInput::new(
            record_segment.clone(),
            blockfile_provider,
            log,
            self.search_user_ids.clone(),
        );

        let task = wrap(operator, input, self_address);
        match self.dispatcher.send(task, Some(Span::current())).await {
            Ok(_) => (),
            Err(e) => {
                terminate_with_error(
                    self.result_channel.take(),
                    Box::new(GetVectorsError::TaskSendError(e)),
                    ctx,
                );
            }
        }
    }

    ///  Run the orchestrator and return the result.
    ///  # Note
    ///  Use this over spawning the component directly. This method will start the component and
    ///  wait for it to finish before returning the result.
    pub(crate) async fn run(mut self) -> Result<GetVectorsResult, Box<dyn ChromaError>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.result_channel = Some(tx);
        let mut handle = self.system.clone().start_component(self);
        let result = rx.await;
        handle.stop();
        result.unwrap()
    }
}

// ============== Component Implementation ==============

#[async_trait]
impl Component for GetVectorsOrchestrator {
    fn get_name() -> &'static str {
        "GetVectorsOrchestrator"
    }

    fn queue_size(&self) -> usize {
        1000
    }

    async fn on_start(&mut self, ctx: &ComponentContext<Self>) {
        // Populate the orchestrator with the initial state - The HNSW Segment, The Record Segment and the Collection
        let hnsw_segment = match get_hnsw_segment_by_id(
            self.sysdb.clone(),
            &self.hnsw_segment_id,
            &self.collection_id,
        )
        .await
        {
            Ok(segment) => segment,
            Err(e) => {
                terminate_with_error(self.result_channel.take(), e, ctx);
                return;
            }
        };

        let collection_id = &hnsw_segment.collection;

        let collection = match get_collection_by_id(self.sysdb.clone(), collection_id).await {
            Ok(collection) => collection,
            Err(e) => {
                terminate_with_error(self.result_channel.take(), e, ctx);
                return;
            }
        };

        // If the collection version does not match the request version then we terminate with an error
        if collection.version as u32 != self.collection_version {
            terminate_with_error(
                self.result_channel.take(),
                Box::new(GetVectorsError::CollectionVersionMismatch),
                ctx,
            );
            return;
        }

        let record_segment =
            match get_record_segment_by_collection_id(self.sysdb.clone(), collection_id).await {
                Ok(segment) => segment,
                Err(e) => {
                    terminate_with_error(self.result_channel.take(), e, ctx);
                    return;
                }
            };

        self.record_segment = Some(record_segment);
        self.collection = Some(collection);

        self.pull_logs(ctx.receiver(), ctx).await;
    }
}

// ============== Handlers ==============

#[async_trait]
impl Handler<TaskResult<PullLogsOutput, PullLogsError>> for GetVectorsOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<PullLogsOutput, PullLogsError>,
        ctx: &ComponentContext<Self>,
    ) {
        let message = message.into_inner();
        match message {
            Ok(output) => {
                let logs = output.logs();
                self.get_vectors(ctx.receiver(), logs, ctx).await;
            }
            Err(e) => {
                terminate_with_error(self.result_channel.take(), Box::new(e), ctx);
            }
        }
    }
}

#[async_trait]
impl Handler<TaskResult<GetVectorsOperatorOutput, GetVectorsOperatorError>>
    for GetVectorsOrchestrator
{
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<GetVectorsOperatorOutput, GetVectorsOperatorError>,
        ctx: &ComponentContext<Self>,
    ) {
        let message = message.into_inner();
        match message {
            Ok(output) => {
                let result = GetVectorsResult {
                    ids: output.ids,
                    vectors: output.vectors,
                };
                let result_channel = self
                    .result_channel
                    .take()
                    .expect("Invariant violation. Result channel is not set.");
                match result_channel.send(Ok(result)) {
                    Ok(_) => (),
                    Err(_e) => {
                        // Log an error - this implied the listener was dropped
                        trace!(
                            "[GetVectorsOrchestrators] Result channel dropped before sending result"
                        );
                    }
                }
                // Cancel the orchestrator so it stops processing
                ctx.cancellation_token.cancel();
            }
            Err(e) => {
                terminate_with_error(self.result_channel.take(), Box::new(e), ctx);
            }
        }
    }
}
