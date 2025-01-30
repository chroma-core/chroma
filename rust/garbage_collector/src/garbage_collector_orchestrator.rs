use std::fmt::{Debug, Formatter};

use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_storage::Storage;
use chroma_sysdb::SysDb;
use chroma_system::{
    wrap, ChannelError, ComponentContext, ComponentHandle, Dispatcher, Handler, Orchestrator,
    PanicError, TaskError, TaskMessage, TaskResult,
};
use chroma_types::CollectionUuid;
use thiserror::Error;
use tokio::sync::oneshot::{error::RecvError, Sender};

use crate::fetch_version_file::{
    FetchVersionFileError, FetchVersionFileInput, FetchVersionFileOperator, FetchVersionFileOutput,
};

#[allow(dead_code)]
pub struct GarbageCollectorOrchestrator {
    collection_id: CollectionUuid,
    version_file_path: String,
    cutoff_time_hours: u32,
    sysdb_client: Box<SysDb>,
    dispatcher: ComponentHandle<Dispatcher>,
    // Result Channel
    result_channel: Option<Sender<Result<GarbageCollectorResponse, GarbageCollectorError>>>,
    storage: Storage,
}

impl Debug for GarbageCollectorOrchestrator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GarbageCollector").finish()
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct GarbageCollectorResponse {
    collection_id: CollectionUuid,
    version_file_path: String,
}

impl GarbageCollectorOrchestrator {
    pub fn new(
        collection_id: CollectionUuid,
        version_file_path: String,
        cutoff_time_hours: u32,
        sysdb_client: Box<SysDb>,
        dispatcher: ComponentHandle<Dispatcher>,
        storage: Storage,
    ) -> Self {
        Self {
            collection_id,
            version_file_path,
            cutoff_time_hours,
            sysdb_client,
            dispatcher,
            result_channel: None,
            storage,
        }
    }
}

#[derive(Error, Debug)]
pub enum GarbageCollectorError {
    #[error("FetchVersionFile error: {0}")]
    FetchVersionFile(#[from] FetchVersionFileError),
    #[error("Panic during compaction: {0}")]
    Panic(#[from] PanicError),
    #[error("Error sending message through channel: {0}")]
    Channel(#[from] ChannelError),
    #[error("Error receiving final result: {0}")]
    Result(#[from] RecvError),
    #[error("{0}")]
    Generic(#[from] Box<dyn ChromaError>),
    #[error("The task was aborted because resources were exhausted")]
    Aborted,
}

impl ChromaError for GarbageCollectorError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::Internal
    }
}

impl<E> From<TaskError<E>> for GarbageCollectorError
where
    E: Into<GarbageCollectorError>,
{
    fn from(value: TaskError<E>) -> Self {
        match value {
            TaskError::Panic(e) => GarbageCollectorError::Panic(e),
            TaskError::TaskFailed(e) => e.into(),
            TaskError::Aborted => GarbageCollectorError::Aborted,
        }
    }
}

#[async_trait]
impl Orchestrator for GarbageCollectorOrchestrator {
    type Output = GarbageCollectorResponse;
    type Error = GarbageCollectorError;

    fn dispatcher(&self) -> ComponentHandle<Dispatcher> {
        self.dispatcher.clone()
    }

    fn initial_tasks(&self, ctx: &ComponentContext<Self>) -> Vec<TaskMessage> {
        vec![wrap(
            Box::new(FetchVersionFileOperator {}),
            FetchVersionFileInput {
                version_file_path: self.version_file_path.clone(),
                storage: self.storage.clone(),
            },
            ctx.receiver(),
        )]
    }

    fn set_result_channel(
        &mut self,
        sender: Sender<Result<GarbageCollectorResponse, GarbageCollectorError>>,
    ) {
        self.result_channel = Some(sender)
    }

    fn take_result_channel(
        &mut self,
    ) -> Sender<Result<GarbageCollectorResponse, GarbageCollectorError>> {
        self.result_channel
            .take()
            .expect("The result channel should be set before take")
    }
}

#[async_trait]
impl Handler<TaskResult<FetchVersionFileOutput, FetchVersionFileError>>
    for GarbageCollectorOrchestrator
{
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<FetchVersionFileOutput, FetchVersionFileError>,
        ctx: &ComponentContext<GarbageCollectorOrchestrator>,
    ) {
        let _ = match self.ok_or_terminate(message.into_inner(), ctx) {
            Some(recs) => recs,
            None => todo!(),
        };
        // TODO(Sanket): Dispatch a task to determine versions to prune.
    }
}
