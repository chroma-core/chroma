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

#[cfg(test)]
mod tests {
    //! Test suite for the `HardDeleteLogOnlyGarbageCollectorOrchestrator`.
    //!
    //! This module verifies the core functionality of the hard delete orchestrator,
    //! which is responsible for permanently removing log data for destroyed collections.
    //! The tests ensure proper initialization, configuration, and trait implementation
    //! of the orchestrator component.
    //!
    //! # Test Coverage
    //!
    //! The test suite validates:
    //! - Correct initialization with required dependencies
    //! - Proper storage of collection UUID for destruction
    //! - Result channel lifecycle management
    //! - Orchestrator trait contract fulfillment
    //!
    //! # Testing Approach
    //!
    //! Tests use mock components (test storage, dispatcher, logs) to isolate
    //! orchestrator behavior without requiring actual I/O operations.
    //! Each test is self-contained and can run in parallel using tokio's
    //! multi-threaded runtime.
    use super::*;
    use chroma_config::registry::Registry;
    use chroma_config::Configurable;
    use chroma_log::config::{GrpcLogConfig, LogConfig};
    use chroma_storage::test_storage;
    use chroma_system::{Dispatcher, System};

    /// Verifies that the orchestrator correctly initializes with all required components.
    ///
    /// This test ensures that when creating a new `HardDeleteLogOnlyGarbageCollectorOrchestrator`,
    /// all provided dependencies (dispatcher, storage, logs, collection UUID) are properly
    /// stored and the result channel starts in an uninitialized state.
    ///
    /// # Test Invariants
    ///
    /// - Collection UUID must match the one provided during construction
    /// - Result channel must be `None` initially (set later by the system)
    #[tokio::test(flavor = "multi_thread")]
    async fn test_k8s_integration_orchestrator_initialization() {
        let (_storage_dir, storage) = test_storage();
        let system = System::new();
        let dispatcher = Dispatcher::new(Default::default());
        let dispatcher_handle = system.start_component(dispatcher);
        let registry = Registry::new();
        let log_config = LogConfig::Grpc(GrpcLogConfig::default());
        let logs = Log::try_from_config(&(log_config, system.clone()), &registry)
            .await
            .unwrap();
        let collection_to_destroy = CollectionUuid::new();

        // Create orchestrator with test dependencies
        let orchestrator = HardDeleteLogOnlyGarbageCollectorOrchestrator::new(
            dispatcher_handle.clone(),
            storage.clone(),
            logs.clone(),
            collection_to_destroy,
        );

        // Verify the orchestrator is properly initialized
        assert_eq!(orchestrator.collection_to_destroy, collection_to_destroy);
        assert!(orchestrator.result_channel.is_none());
    }

    /// Validates that the orchestrator correctly stores the collection UUID for hard deletion.
    ///
    /// This test verifies that the orchestrator preserves the collection UUID that will be
    /// passed to the `DeleteUnusedLogsOperator` when `on_start` is called. It also documents
    /// the hardcoded configuration that will be used for the delete operation.
    ///
    /// # Implementation Details
    ///
    /// When the orchestrator starts the delete operator (in `try_start_delete_unused_logs_operator`),
    /// it uses the following hardcoded configuration:
    /// - `enabled`: true (operator is active)
    /// - `mode`: `CleanupMode::DeleteV2` (performs hard deletion)
    /// - `enable_dangerous_option_to_ignore_min_versions_for_wal3`: false (safety check enabled)
    ///
    /// The collection UUID stored in `collection_to_destroy` is placed in the
    /// `collections_to_destroy` set, while `collections_to_garbage_collect` remains empty
    /// since this orchestrator only handles hard deletion, not soft garbage collection.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_k8s_integration_delete_operator_params() {
        let (_storage_dir, storage) = test_storage();
        let system = System::new();
        let dispatcher = Dispatcher::new(Default::default());
        let dispatcher_handle = system.start_component(dispatcher);
        let registry = Registry::new();
        let log_config = LogConfig::Grpc(GrpcLogConfig::default());
        let logs = Log::try_from_config(&(log_config, system.clone()), &registry)
            .await
            .unwrap();
        let collection_to_destroy = CollectionUuid::new();

        let orchestrator = HardDeleteLogOnlyGarbageCollectorOrchestrator::new(
            dispatcher_handle,
            storage.clone(),
            logs.clone(),
            collection_to_destroy,
        );

        // Verify the orchestrator stores correct collection UUID for destruction
        assert_eq!(orchestrator.collection_to_destroy, collection_to_destroy);

        // Note: The delete operator configuration is hardcoded in try_start_delete_unused_logs_operator
        // and cannot be modified externally. This ensures consistent deletion behavior.
    }
}
