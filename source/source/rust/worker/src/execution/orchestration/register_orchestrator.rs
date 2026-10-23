use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_system::{
    wrap, ChannelError, ComponentContext, ComponentHandle, Dispatcher, Handler, Orchestrator,
    OrchestratorContext, TaskMessage, TaskResult,
};
use chroma_system::{PanicError, TaskError};
use chroma_types::{JobId, SegmentFlushInfo};
use thiserror::Error;
use tokio::sync::oneshot::error::RecvError;
use tokio::sync::oneshot::Sender;
use tracing::Span;

use crate::execution::operators::finish_attached_function::{
    FinishAttachedFunctionError, FinishAttachedFunctionInput, FinishAttachedFunctionOperator,
    FinishAttachedFunctionOutput,
};
use crate::execution::operators::register::{
    RegisterError, RegisterInput, RegisterOperator, RegisterOutput,
};
use crate::execution::orchestration::attached_function_orchestrator::FunctionContext;
use crate::execution::orchestration::compact::CollectionCompactInfo;
use crate::execution::orchestration::compact::CompactionContextError;

use super::compact::{CompactionContext, ExecutionState};

#[derive(Debug)]
pub struct RegisterOrchestrator {
    pub context: CompactionContext,
    dispatcher: ComponentHandle<Dispatcher>,
    result_channel: Option<Sender<Result<RegisterOrchestratorResponse, RegisterOrchestratorError>>>,
    _state: ExecutionState,
    // Attached function fields
    collection_register_infos: Vec<CollectionRegisterInfo>,
    function_context: Option<FunctionContext>,
}

#[derive(Debug)]
pub struct CollectionRegisterInfo {
    pub collection_info: CollectionCompactInfo,
    pub flush_results: Vec<SegmentFlushInfo>,
    pub collection_logical_size_bytes: u64,
}

/// Error when converting CollectionRegisterInfo to CollectionFlushInfo.
#[derive(Debug, Error)]
pub enum CollectionRegisterInfoConversionError {
    #[error("Invalid database name")]
    InvalidDatabaseName,
}

impl TryFrom<&CollectionRegisterInfo> for chroma_types::CollectionFlushInfo {
    type Error = CollectionRegisterInfoConversionError;

    fn try_from(info: &CollectionRegisterInfo) -> Result<Self, Self::Error> {
        let database_name =
            chroma_types::DatabaseName::new(info.collection_info.collection.database.clone())
                .ok_or(CollectionRegisterInfoConversionError::InvalidDatabaseName)?;
        Ok(chroma_types::CollectionFlushInfo {
            tenant_id: info.collection_info.collection.tenant.clone(),
            database_name,
            collection_id: info.collection_info.collection_id,
            log_position: info.collection_info.pulled_log_offset,
            collection_version: info.collection_info.collection.version,
            segment_flush_info: info.flush_results.clone().into(),
            total_records_post_compaction: info
                .collection_info
                .collection
                .total_records_post_compaction,
            size_bytes_post_compaction: info.collection_logical_size_bytes,
            schema: info.collection_info.schema.clone(),
        })
    }
}

#[derive(Debug)]
pub struct RegisterOrchestratorResponse {
    pub job_id: JobId,
}

impl RegisterOrchestratorResponse {
    pub fn new(job_id: JobId) -> Self {
        Self { job_id }
    }
}

#[derive(Error, Debug)]
pub enum RegisterOrchestratorError {
    #[error("Operation aborted because resources exhausted")]
    Aborted,
    #[error("Error sending message through channel: {0}")]
    Channel(#[from] ChannelError),
    #[error("Error in compaction context: {0}")]
    CompactionContext(#[from] CompactionContextError),
    #[error("Invariant violation: {}", .0)]
    InvariantViolation(&'static str),
    #[error("Panic during compaction: {0}")]
    Panic(#[from] PanicError),
    #[error("Error receiving: {0}")]
    RecvError(#[from] RecvError),
    #[error("Error registering compaction result: {0}")]
    Register(#[from] RegisterError),
}

impl ChromaError for RegisterOrchestratorError {
    fn code(&self) -> ErrorCodes {
        match self {
            RegisterOrchestratorError::Aborted => ErrorCodes::Aborted,
            _ => ErrorCodes::Internal,
        }
    }

    fn should_trace_error(&self) -> bool {
        match self {
            RegisterOrchestratorError::Aborted => true,
            RegisterOrchestratorError::Channel(e) => e.should_trace_error(),
            RegisterOrchestratorError::CompactionContext(e) => e.should_trace_error(),
            RegisterOrchestratorError::InvariantViolation(_) => true,
            RegisterOrchestratorError::Panic(e) => e.should_trace_error(),
            RegisterOrchestratorError::Register(e) => e.should_trace_error(),
            RegisterOrchestratorError::RecvError(_) => true,
        }
    }
}

impl<E> From<TaskError<E>> for RegisterOrchestratorError
where
    E: Into<RegisterOrchestratorError>,
{
    fn from(value: TaskError<E>) -> Self {
        match value {
            TaskError::Aborted => RegisterOrchestratorError::Aborted,
            TaskError::Panic(e) => e.into(),
            TaskError::TaskFailed(e) => e.into(),
        }
    }
}

impl From<FinishAttachedFunctionError> for RegisterOrchestratorError {
    fn from(value: FinishAttachedFunctionError) -> Self {
        RegisterOrchestratorError::Register(value.into())
    }
}

impl RegisterOrchestrator {
    pub fn new(
        context: &CompactionContext,
        dispatcher: ComponentHandle<Dispatcher>,
        collection_register_infos: Vec<CollectionRegisterInfo>,
        function_context: Option<FunctionContext>,
    ) -> Self {
        RegisterOrchestrator {
            context: context.clone(),
            dispatcher,
            result_channel: None,
            _state: ExecutionState::Register,
            collection_register_infos,
            function_context,
        }
    }
}

#[async_trait]
impl Orchestrator for RegisterOrchestrator {
    type Output = RegisterOrchestratorResponse;
    type Error = RegisterOrchestratorError;

    fn dispatcher(&self) -> ComponentHandle<Dispatcher> {
        self.dispatcher.clone()
    }

    fn context(&self) -> &OrchestratorContext {
        &self.context.orchestrator_context
    }

    fn set_result_channel(&mut self, sender: Sender<Result<Self::Output, Self::Error>>) {
        self.result_channel = Some(sender)
    }

    fn take_result_channel(&mut self) -> Option<Sender<Result<Self::Output, Self::Error>>> {
        self.result_channel.take()
    }

    async fn initial_tasks(
        &mut self,
        ctx: &ComponentContext<Self>,
    ) -> Vec<(TaskMessage, Option<Span>)> {
        // Check if we have attached function context
        let collection_flush_infos: Result<Vec<_>, _> = self
            .collection_register_infos
            .iter()
            .map(chroma_types::CollectionFlushInfo::try_from)
            .collect();
        let collection_flush_infos = match collection_flush_infos {
            Ok(infos) => infos,
            Err(_) => {
                self.terminate_with_result(
                    Err(RegisterOrchestratorError::InvariantViolation(
                        "Invalid database name in collection flush info",
                    )),
                    ctx,
                )
                .await;
                return vec![];
            }
        };
        if let Some(function_context) = &self.function_context {
            vec![(
                wrap(
                    FinishAttachedFunctionOperator::new(),
                    FinishAttachedFunctionInput::new(
                        collection_flush_infos,
                        function_context.attached_function_id,
                        function_context.updated_completion_offset,
                        self.context.sysdb.clone(),
                        self.context.log.clone(),
                    ),
                    ctx.receiver(),
                    self.context
                        .orchestrator_context
                        .task_cancellation_token
                        .clone(),
                ),
                Some(Span::current()),
            )]
        } else {
            // Use regular RegisterOperator for normal compaction
            // INVARIANT: We should have exactly one collection register info
            let output_collection_register_info = match self.collection_register_infos.first() {
                Some(info) => info,
                None => {
                    self.terminate_with_result(
                        Err(RegisterOrchestratorError::InvariantViolation(
                            "No collection register info found",
                        )),
                        ctx,
                    )
                    .await;
                    return vec![];
                }
            };

            let database_name = match chroma_types::DatabaseName::new(
                output_collection_register_info
                    .collection_info
                    .collection
                    .database
                    .clone(),
            ) {
                Some(name) => name,
                None => {
                    self.terminate_with_result(
                        Err(RegisterOrchestratorError::InvariantViolation(
                            "Invalid database name",
                        )),
                        ctx,
                    )
                    .await;
                    return vec![];
                }
            };

            vec![(
                wrap(
                    RegisterOperator::new(),
                    RegisterInput::new(
                        output_collection_register_info
                            .collection_info
                            .collection
                            .tenant
                            .clone(),
                        database_name,
                        output_collection_register_info
                            .collection_info
                            .collection_id,
                        output_collection_register_info
                            .collection_info
                            .pulled_log_offset,
                        output_collection_register_info
                            .collection_info
                            .collection
                            .version,
                        output_collection_register_info.flush_results.clone().into(),
                        output_collection_register_info
                            .collection_info
                            .collection
                            .total_records_post_compaction,
                        output_collection_register_info.collection_logical_size_bytes,
                        self.context.sysdb.clone(),
                        self.context.log.clone(),
                        output_collection_register_info
                            .collection_info
                            .schema
                            .clone(),
                    ),
                    ctx.receiver(),
                    self.context
                        .orchestrator_context
                        .task_cancellation_token
                        .clone(),
                ),
                Some(Span::current()),
            )]
        }
    }
}

#[async_trait]
impl Handler<TaskResult<RegisterOutput, RegisterError>> for RegisterOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<RegisterOutput, RegisterError>,
        ctx: &ComponentContext<Self>,
    ) {
        let collection_info = match self.context.get_collection_info() {
            Ok(collection_info) => collection_info,
            Err(e) => {
                self.terminate_with_result(Err(e.into()), ctx).await;
                return;
            }
        };

        self.terminate_with_result(
            message
                .into_inner()
                .map_err(|e| e.into())
                .map(|_| RegisterOrchestratorResponse::new(collection_info.collection_id.into())),
            ctx,
        )
        .await;
    }
}

#[async_trait]
impl Handler<TaskResult<FinishAttachedFunctionOutput, FinishAttachedFunctionError>>
    for RegisterOrchestrator
{
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<FinishAttachedFunctionOutput, FinishAttachedFunctionError>,
        ctx: &ComponentContext<Self>,
    ) {
        let collection_info = match self.context.get_collection_info() {
            Ok(collection_info) => collection_info,
            Err(e) => {
                self.terminate_with_result(Err(e.into()), ctx).await;
                return;
            }
        };

        self.terminate_with_result(
            message
                .into_inner()
                .map_err(|e| match e {
                    TaskError::TaskFailed(inner_error) => {
                        RegisterOrchestratorError::Register(inner_error.into())
                    }
                    other_error => other_error.into(),
                })
                .map(|_| RegisterOrchestratorResponse {
                    job_id: collection_info.collection_id.into(),
                }),
            ctx,
        )
        .await;
    }
}
