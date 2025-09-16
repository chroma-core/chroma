use crate::garbage_collector_orchestrator_v2::GarbageCollectorError;
use crate::operators::delete_unused_logs::{
    DeleteUnusedLogsError, DeleteUnusedLogsInput, DeleteUnusedLogsOperator, DeleteUnusedLogsOutput,
};
use crate::types::{CleanupMode, GarbageCollectorResponse};
use async_trait::async_trait;
use chroma_log::Log;
use chroma_storage::Storage;
use chroma_system::{
    wrap, ComponentContext, ComponentHandle, Dispatcher, Handler, Orchestrator,
    OrchestratorContext, TaskResult,
};
use chroma_types::CollectionUuid;
use std::collections::{HashMap, HashSet};
use tokio::sync::oneshot::Sender;
use tracing::{Level, Span};

#[derive(Debug)]
pub struct HardDeleteLogOnlyGarbageCollectorOrchestrator {
    context: OrchestratorContext,
    storage: Storage,
    logs: Log,
    result_channel: Option<Sender<Result<GarbageCollectorResponse, GarbageCollectorError>>>,
    collection_to_destroy: CollectionUuid,
}

#[allow(clippy::too_many_arguments)]
impl HardDeleteLogOnlyGarbageCollectorOrchestrator {
    pub fn new(
        dispatcher: ComponentHandle<Dispatcher>,
        storage: Storage,
        logs: Log,
        collection_to_destroy: CollectionUuid,
    ) -> Self {
        Self {
            context: OrchestratorContext::new(dispatcher),
            storage,
            logs,
            result_channel: None,
            collection_to_destroy,
        }
    }
}

#[async_trait]
impl Orchestrator for HardDeleteLogOnlyGarbageCollectorOrchestrator {
    type Output = GarbageCollectorResponse;
    type Error = GarbageCollectorError;

    fn dispatcher(&self) -> ComponentHandle<Dispatcher> {
        self.context.dispatcher.clone()
    }

    fn context(&self) -> &OrchestratorContext {
        &self.context
    }

    async fn on_start(&mut self, ctx: &ComponentContext<Self>) {
        let _ = self
            .try_start_delete_unused_logs_operator(ctx)
            .await
            .inspect_err(|_| {
                tracing::event!(
                    Level::ERROR,
                    "could not start job to hard delete unused logs",
                )
            });
    }

    fn set_result_channel(
        &mut self,
        sender: Sender<Result<GarbageCollectorResponse, GarbageCollectorError>>,
    ) {
        self.result_channel = Some(sender);
    }

    fn take_result_channel(
        &mut self,
    ) -> Option<Sender<Result<GarbageCollectorResponse, GarbageCollectorError>>> {
        self.result_channel.take()
    }
}

impl HardDeleteLogOnlyGarbageCollectorOrchestrator {
    async fn try_start_delete_unused_logs_operator(
        &mut self,
        ctx: &ComponentContext<Self>,
    ) -> Result<(), GarbageCollectorError> {
        let collections_to_destroy =
            HashSet::from_iter(vec![self.collection_to_destroy].into_iter());
        let collections_to_garbage_collect = HashMap::new();
        let task = wrap(
            Box::new(DeleteUnusedLogsOperator {
                enabled: true,
                mode: CleanupMode::DeleteV2,
                storage: self.storage.clone(),
                logs: self.logs.clone(),
                enable_dangerous_option_to_ignore_min_versions_for_wal3: false,
            }),
            DeleteUnusedLogsInput {
                collections_to_destroy,
                collections_to_garbage_collect,
            },
            ctx.receiver(),
            self.context.task_cancellation_token.clone(),
        );
        self.dispatcher()
            .send(task, Some(Span::current()))
            .await
            .map_err(GarbageCollectorError::Channel)?;
        Ok(())
    }
}

#[async_trait]
impl Handler<TaskResult<DeleteUnusedLogsOutput, DeleteUnusedLogsError>>
    for HardDeleteLogOnlyGarbageCollectorOrchestrator
{
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<DeleteUnusedLogsOutput, DeleteUnusedLogsError>,
        ctx: &ComponentContext<HardDeleteLogOnlyGarbageCollectorOrchestrator>,
    ) {
        let _output = match self.ok_or_terminate(message.into_inner(), ctx).await {
            Some(output) => output,
            None => return,
        };
        self.terminate_with_result(
            Ok(GarbageCollectorResponse {
                collection_id: self.collection_to_destroy,
                num_versions_deleted: 0,
                num_files_deleted: 0,
                ..Default::default()
            }),
            ctx,
        )
        .await;
    }
}
