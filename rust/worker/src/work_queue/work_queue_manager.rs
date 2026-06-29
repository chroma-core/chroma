// V1: WorkDistributor import commented out
// use crate::work_queue::distribution::WorkDistributor;
use crate::work_queue::state::QueueState;
use crate::work_queue::types::{FinishResult, WorkQueueError, WorkQueueRecord};

#[allow(dead_code)]
enum WorkResponse {
    Push(oneshot::Sender<Result<(), WorkQueueError>>),
    Repair(oneshot::Sender<Result<FinishResult, WorkQueueError>>),
}
use async_trait::async_trait;
use chroma_error::ChromaError;
use chroma_storage::{GetOptions, PutMode, PutOptions, Storage};
use chroma_sysdb::SysDb;
use chroma_system::{Component, ComponentContext, ComponentRuntime, Handler};
use chroma_types::chroma_proto::{
    CheckInvocationStatusRequest, InvocationCheckItem, InvocationStatus, InvocationStatusResult,
    TryFinishAsyncAttachedFunctionInvocationRequest,
};
use chroma_types::{AttachedFunctionUuid, CollectionUuid};
use std::time::Duration;
use tokio::sync::oneshot;

// Message types
#[derive(Debug)]
#[allow(dead_code)]
pub struct PushWorkMessage {
    pub fn_id: AttachedFunctionUuid,
    pub input_coll_id: CollectionUuid,
    pub completion_offset: i64,
    pub compaction_offset: Option<i64>,
    pub response_tx: oneshot::Sender<Result<(), WorkQueueError>>,
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct FinishWorkMessage {
    pub fn_id: AttachedFunctionUuid,
    pub input_coll_id: CollectionUuid,
    pub new_completion_offset: i64,
    pub response_tx: oneshot::Sender<Result<FinishResult, WorkQueueError>>,
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct GetWorkMessage {
    #[allow(dead_code)]
    pub shard_id: String,
    pub limit: usize,
    pub response_tx: oneshot::Sender<Result<Vec<WorkQueueRecord>, WorkQueueError>>,
}

#[derive(Debug)]
pub(crate) struct WorkQueueReadyMessage;

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct PeriodicPersistMessage;

// Component implementation
#[derive(Debug)]
pub(crate) struct WorkQueueManager {
    state: QueueState,
    storage: Storage,
    storage_path: String,
    sysdb: SysDb,
    config: crate::work_queue::config::WorkQueueConfig,
    // Pending responses waiting for persistence (push work responses)
    pending_push_responses: Vec<oneshot::Sender<Result<(), WorkQueueError>>>,
    // Pending responses for finish work
    pending_finish_responses: Vec<(
        FinishResult,
        oneshot::Sender<Result<FinishResult, WorkQueueError>>,
    )>,
}

#[derive(Debug)]
enum InvocationCompletionStatus {
    NotDone,
    Done,
    NeedsRepair(i64),
}

#[derive(Debug, thiserror::Error)]
enum InvocationCompletionStatusConversionError {
    #[error("invalid invocation status: {0}")]
    InvalidStatus(i32),
}

impl TryFrom<InvocationStatusResult> for InvocationCompletionStatus {
    type Error = InvocationCompletionStatusConversionError;

    fn try_from(result: InvocationStatusResult) -> Result<Self, Self::Error> {
        match InvocationStatus::try_from(result.status) {
            Ok(InvocationStatus::Done) => Ok(InvocationCompletionStatus::Done),
            Ok(InvocationStatus::NeedsRepair) => Ok(InvocationCompletionStatus::NeedsRepair(
                result.current_completion_offset,
            )),
            Ok(InvocationStatus::NotDone) => Ok(InvocationCompletionStatus::NotDone),
            Err(_) => Err(InvocationCompletionStatusConversionError::InvalidStatus(
                result.status,
            )),
        }
    }
}

impl WorkQueueManager {
    pub fn new(
        storage: Storage,
        config: crate::work_queue::config::WorkQueueConfig,
        sysdb: SysDb,
    ) -> Self {
        Self {
            state: QueueState::new(),
            storage,
            storage_path: config.storage_path.clone(),
            sysdb,
            config,
            pending_push_responses: Vec::new(),
            pending_finish_responses: Vec::new(),
        }
    }

    // V1: Memberlist methods commented out
    // pub fn set_memberlist(&mut self, members: Vec<chroma_memberlist::memberlist_provider::Member>) {
    //     self.distributor = Some(WorkDistributor::new(members));
    // }

    #[tracing::instrument(name = "WorkQueueManager::load_state", skip(self))]
    async fn load_state(&mut self) -> Result<(), WorkQueueError> {
        match self
            .storage
            .get_with_e_tag(&self.storage_path, GetOptions::default())
            .await
        {
            Ok((bytes, Some(etag))) => {
                self.state = QueueState::from_parquet_bytes(&bytes)?;
                self.state.current_etag = Some(etag);
                tracing::info!(
                    "Loaded work queue state with {} items",
                    self.state.pending_work.len()
                );
                Ok(())
            }
            Ok((bytes, None)) => {
                self.state = QueueState::from_parquet_bytes(&bytes)?;
                self.state.current_etag = None;
                tracing::info!(
                    "Loaded work queue state with {} items (no ETag support)",
                    self.state.pending_work.len()
                );
                Ok(())
            }
            Err(chroma_storage::StorageError::NotFound { .. }) => {
                tracing::info!("No existing work queue state found, starting fresh");
                Ok(())
            }
            Err(e) => Err(WorkQueueError::Storage(e.to_string())),
        }
    }

    #[tracing::instrument(name = "WorkQueueManager::persist", skip(self), level = "debug")]
    async fn persist(&mut self) -> Result<(), WorkQueueError> {
        if !self.state.dirty {
            self.notify_pending_responses();
            return Ok(());
        }

        let bytes = self.state.to_parquet_bytes()?;

        let put_options = if let Some(etag) = &self.state.current_etag {
            PutOptions::default().with_mode(PutMode::IfMatch(etag.clone()))
        } else {
            PutOptions::default().with_mode(PutMode::IfNotExist)
        };

        match self
            .storage
            .put_bytes(&self.storage_path, bytes.to_vec(), put_options)
            .await
        {
            Ok(etag_opt) => {
                let has_etag = etag_opt.is_some();
                self.state.current_etag = etag_opt;
                self.state.dirty = false;

                let etag_msg = if has_etag { "" } else { " (no ETag)" };
                let total_pending =
                    self.pending_push_responses.len() + self.pending_finish_responses.len();
                tracing::debug!(
                    "Persisted work queue state{}, responding to {} pending requests",
                    etag_msg,
                    total_pending
                );

                self.notify_pending_responses();
                Ok(())
            }
            Err(e) => match e {
                chroma_storage::StorageError::Precondition { .. } => {
                    tracing::error!("ETag mismatch - another instance is active");
                    panic!("Work queue ETag mismatch - shutting down");
                }
                _ => {
                    let err = WorkQueueError::Storage(e.to_string());
                    self.notify_pending_responses_error(&err);
                    Err(err)
                }
            },
        }
    }

    fn notify_pending_responses(&mut self) {
        for tx in self.pending_push_responses.drain(..) {
            if tx.send(Ok(())).is_err() {
                tracing::error!("Failed to send push work response - receiver dropped");
            }
        }

        for (result, tx) in self.pending_finish_responses.drain(..) {
            if tx.send(Ok(result)).is_err() {
                tracing::error!("Failed to send finish work response - receiver dropped");
            }
        }
    }

    fn notify_pending_responses_error(&mut self, error: &WorkQueueError) {
        for tx in self.pending_push_responses.drain(..) {
            if tx.send(Err(error.clone())).is_err() {
                tracing::error!("Failed to send push work error response - receiver dropped");
            }
        }

        for (_, tx) in self.pending_finish_responses.drain(..) {
            if tx.send(Err(error.clone())).is_err() {
                tracing::error!("Failed to send finish work error response - receiver dropped");
            }
        }
    }

    fn should_persist(&self) -> bool {
        if !self.state.dirty {
            return false;
        }

        let total_pending_responses =
            self.pending_push_responses.len() + self.pending_finish_responses.len();

        total_pending_responses >= self.config.persistence.pending_threshold
    }

    async fn push_work_and_queue_response(
        &mut self,
        fn_id: AttachedFunctionUuid,
        input_coll_id: CollectionUuid,
        completion_offset: i64,
        compaction_offset: Option<i64>,
        response: WorkResponse,
    ) {
        let _ = self
            .state
            .push_work(fn_id, input_coll_id, completion_offset, compaction_offset);

        // TODO(tanujnay112): Can optimize the case where we push work
        // that gets deduplicated. That would require epoch tracking
        // where the epoch is incremented per persistence event and
        // we associate each dedup map entry with an epoch.

        match response {
            WorkResponse::Push(tx) => self.pending_push_responses.push(tx),
            WorkResponse::Repair(tx) => self
                .pending_finish_responses
                .push((FinishResult::NeedsRepair, tx)),
        }

        // Check if persist needed
        if self.should_persist() {
            if let Err(e) = self.persist().await {
                tracing::error!("Failed to persist work queue: {}", e);
            }
        }
    }

    // Update sysdb's completion offset for an async attached function invocation.
    #[tracing::instrument(name = "WorkQueueManager::try_finish_invocation", skip(self))]
    async fn try_finish_invocation(
        &mut self,
        fn_id: &AttachedFunctionUuid,
        input_coll_id: &CollectionUuid,
        completion_offset: i64,
    ) -> Result<(), WorkQueueError> {
        let request = TryFinishAsyncAttachedFunctionInvocationRequest {
            attached_function_id: fn_id.to_string(),
            collection_id: input_coll_id.to_string(),
            new_completion_offset: completion_offset as u64,
        };

        self.sysdb
            .try_finish_async_attached_function_invocation(request)
            .await
            .map_err(|e| WorkQueueError::TryFinishFailed(e.message().to_string()))?;
        Ok(())
    }

    // Check invocation completion status (boolean version for compatibility)
    async fn are_invocations_done(
        &mut self,
        items: &[WorkQueueRecord],
    ) -> Result<Vec<bool>, WorkQueueError> {
        let statuses = self.check_invocations_status(items).await?;
        // Map DONE to true, everything else (NOT_DONE, NEEDS_REPAIR) to false
        Ok(statuses
            .into_iter()
            .map(|status| matches!(status, InvocationCompletionStatus::Done))
            .collect())
    }

    // Check invocation completion status with detailed status
    #[tracing::instrument(
        name = "WorkQueueManager::check_invocations_status",
        skip(self, items),
        level = "debug"
    )]
    async fn check_invocations_status(
        &mut self,
        items: &[WorkQueueRecord],
    ) -> Result<Vec<InvocationCompletionStatus>, WorkQueueError> {
        if items.is_empty() {
            return Ok(vec![]);
        }

        let invocation_items: Vec<InvocationCheckItem> = items
            .iter()
            .map(|item| InvocationCheckItem {
                function_id: item.fn_id.to_string(),
                input_collection_id: item.input_coll_id.to_string(),
                completion_offset: item.completion_offset,
            })
            .collect();

        let request = CheckInvocationStatusRequest {
            items: invocation_items,
        };

        let response = self
            .sysdb
            .check_invocation_status(request)
            .await
            .map_err(|e| WorkQueueError::CheckInvocationsFailed(e.message().to_string()))?;

        let statuses = response
            .into_inner()
            .results
            .into_iter()
            .map(InvocationCompletionStatus::try_from)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| WorkQueueError::CheckInvocationsFailed(e.to_string()))?;

        Ok(statuses)
    }

    // Check all pending items on startup and repair any that need it
    #[tracing::instrument(name = "WorkQueueManager::check_and_repair_pending_items", skip(self))]
    async fn check_and_repair_pending_items(&mut self) {
        if self.state.pending_work.is_empty() {
            tracing::info!("No pending work items to check for repair");
            return;
        }

        tracing::info!(
            "Checking {} pending work items for repair",
            self.state.pending_work.len()
        );

        // Get all pending items
        let items: Vec<_> = self.state.pending_work.iter().cloned().collect();

        // Check their statuses
        let statuses = match self.check_invocations_status(&items).await {
            Ok(statuses) => statuses,
            Err(e) => {
                tracing::error!("Failed to check invocation statuses for repair: {}", e);
                return;
            }
        };

        // Find items that need repair
        let items_needing_repair: Vec<_> = items
            .into_iter()
            .zip(statuses.iter())
            .filter_map(|(item, status)| match status {
                InvocationCompletionStatus::NeedsRepair(current_completion_offset) => {
                    Some((item, *current_completion_offset))
                }
                _ => None,
            })
            .collect();

        if items_needing_repair.is_empty() {
            tracing::info!("No items need repair");
            return;
        }

        tracing::warn!("Found {} items needing repair", items_needing_repair.len());

        for (item, current_completion_offset) in &items_needing_repair {
            tracing::info!(
                "Queueing repair for fn_id: {}, input_coll_id: {}, old_completion_offset: {}, current_completion_offset: {}",
                item.fn_id,
                item.input_coll_id,
                item.completion_offset,
                current_completion_offset
            );

            self.state.push_work(
                item.fn_id,
                item.input_coll_id,
                *current_completion_offset,
                None,
            );
        }

        if let Err(e) = self.persist().await {
            tracing::error!(
                "Failed to persist repaired work queue state; skipping sysdb repair finalization: {}",
                e
            );
            return;
        }

        for (item, _) in items_needing_repair {
            tracing::info!(
                "Finalizing repair for fn_id: {}, input_coll_id: {}",
                item.fn_id,
                item.input_coll_id
            );

            let repair_request =
                chroma_types::chroma_proto::FinalizeAsyncAttachedFunctionRepairRequest {
                    attached_function_id: item.fn_id.to_string(),
                    collection_id: item.input_coll_id.to_string(),
                };

            if let Err(e) = self
                .sysdb
                .finalize_async_attached_function_repair(repair_request)
                .await
            {
                tracing::error!(
                    "Failed to finalize repair for function {}: {}",
                    item.fn_id,
                    e
                );
            }
        }

        tracing::info!("Repair check completed");
    }
}

#[async_trait]
impl Component for WorkQueueManager {
    fn get_name() -> &'static str {
        "WorkQueueManager"
    }

    fn queue_size(&self) -> usize {
        1000
    }

    fn runtime() -> ComponentRuntime {
        ComponentRuntime::Inherit
    }

    async fn on_start(&mut self, ctx: &ComponentContext<Self>) -> () {
        tracing::info!("Starting WorkQueueManager");

        // Load existing state
        if let Err(e) = self.load_state().await {
            tracing::error!("Failed to load work queue state: {}", e);
            panic!("Cannot start without valid state");
        }

        // Check all heap items to see if any need repair
        self.check_and_repair_pending_items().await;

        // Schedule periodic persistence
        ctx.scheduler.schedule(
            PeriodicPersistMessage,
            Duration::from_secs(self.config.persistence.time_threshold_seconds),
            ctx,
            || None,
        );
    }

    async fn on_stop(&mut self) -> Result<(), Box<dyn ChromaError>> {
        tracing::info!("Stopping WorkQueueManager");

        // Final persist if dirty or if we have pending responses
        if self.state.dirty
            || !self.pending_push_responses.is_empty()
            || !self.pending_finish_responses.is_empty()
        {
            self.persist()
                .await
                .map_err(|e| Box::new(e) as Box<dyn ChromaError>)?;
        }

        Ok(())
    }
}

#[async_trait]
impl Handler<PushWorkMessage> for WorkQueueManager {
    type Result = ();

    async fn handle(&mut self, msg: PushWorkMessage, _ctx: &ComponentContext<WorkQueueManager>) {
        tracing::info!(
            "Received PushWorkMessage for fn_id: {}, input_coll_id: {}, completion_offset: {}, compaction_offset: {:?}",
            msg.fn_id,
            msg.input_coll_id,
            msg.completion_offset,
            msg.compaction_offset
        );
        self.push_work_and_queue_response(
            msg.fn_id,
            msg.input_coll_id,
            msg.completion_offset,
            msg.compaction_offset,
            WorkResponse::Push(msg.response_tx),
        )
        .await;
    }
}

#[async_trait]
impl Handler<FinishWorkMessage> for WorkQueueManager {
    type Result = ();

    async fn handle(&mut self, msg: FinishWorkMessage, _ctx: &ComponentContext<WorkQueueManager>) {
        // Call sysdb
        if let Err(e) = self
            .try_finish_invocation(&msg.fn_id, &msg.input_coll_id, msg.new_completion_offset)
            .await
        {
            if msg.response_tx.send(Err(e)).is_err() {
                tracing::error!("Failed to send error response");
            }
            return;
        }

        // Use the queued compaction frontier to decide whether to remove or advance the entry.
        self.state.finish_work_success(
            &msg.fn_id,
            &msg.input_coll_id,
            msg.new_completion_offset,
        );

        // Send immediate success response
        if msg.response_tx.send(Ok(FinishResult::Success)).is_err() {
            tracing::error!("Failed to send finish work success response - receiver dropped");
        }

        // Check if persist needed
        if self.should_persist() {
            let _ = self.persist().await;
        }
    }
}

#[async_trait]
impl Handler<GetWorkMessage> for WorkQueueManager {
    type Result = ();

    async fn handle(&mut self, msg: GetWorkMessage, _ctx: &ComponentContext<WorkQueueManager>) {
        // For now, return all items up to limit
        // TODO: In the future, this will be filtered based on work assignment

        let items: Vec<_> = self
            .state
            .pending_work
            .iter()
            .take(msg.limit)
            .cloned()
            .collect();

        // Check invocations done
        // TODO(tanujnay112): We won't need this if we make sure finish_work
        // deletes the work item from the queue and we look for repair on bootup.
        let done_flags = match self.are_invocations_done(&items).await {
            Ok(flags) => flags,
            Err(e) => {
                tracing::error!("Failed to check invocations done: {}", e);
                // If we fail to check, return empty results
                if msg.response_tx.send(Err(e)).is_err() {
                    tracing::warn!("Failed to send error response - receiver dropped");
                }
                return;
            }
        };

        let filtered: Vec<_> = items
            .into_iter()
            .zip(done_flags.iter())
            .filter(|(_, done)| !**done)
            .map(|(item, _)| item)
            .take(msg.limit)
            .collect();
        tracing::info!("Filtered {} items from get work response", filtered.len());

        if msg.response_tx.send(Ok(filtered)).is_err() {
            tracing::warn!("Failed to send get work response - receiver dropped");
        }
    }
}

#[async_trait]
impl Handler<WorkQueueReadyMessage> for WorkQueueManager {
    type Result = ();

    async fn handle(
        &mut self,
        _msg: WorkQueueReadyMessage,
        _ctx: &ComponentContext<WorkQueueManager>,
    ) {
    }
}

#[async_trait]
impl Handler<PeriodicPersistMessage> for WorkQueueManager {
    type Result = ();

    async fn handle(
        &mut self,
        _msg: PeriodicPersistMessage,
        ctx: &ComponentContext<WorkQueueManager>,
    ) {
        if let Err(e) = self.persist().await {
            tracing::error!("Periodic persist failed: {}", e);
        }

        ctx.scheduler.schedule(
            PeriodicPersistMessage,
            Duration::from_secs(self.config.persistence.time_threshold_seconds),
            ctx,
            || None,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::work_queue::state::QueueState;
    use chroma_storage::local::LocalStorage;
    use tempfile::TempDir;
    use uuid::Uuid;

    fn create_test_config() -> crate::work_queue::config::WorkQueueConfig {
        crate::work_queue::config::WorkQueueConfig {
            storage_path: "test-queue.parquet".to_string(),
            persistence: crate::work_queue::config::PersistenceConfig {
                time_threshold_seconds: 2,
                pending_threshold: 100, // Set high to avoid auto-persist in tests
            },
        }
    }

    fn create_test_sysdb() -> SysDb {
        // Create a test sysdb for unit tests that only test internal state.
        // This sysdb is not connected to any backend and is purely for testing.
        // If a test needs real sysdb interaction, it should be an integration test.
        SysDb::Test(chroma_sysdb::test_sysdb::TestSysDb::new())
    }

    async fn create_test_manager() -> (WorkQueueManager, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let storage = Storage::Local(LocalStorage::new(temp_dir.path().to_str().unwrap()));
        let mut config = create_test_config();
        config.storage_path = "queue.parquet".to_string(); // Use relative path within temp dir
        let sysdb = create_test_sysdb();
        (WorkQueueManager::new(storage, config, sysdb), temp_dir)
    }

    #[test]
    fn test_push_work_deduplication() {
        let mut state = QueueState::new();

        let fn_id = AttachedFunctionUuid(Uuid::new_v4());
        let coll_id = CollectionUuid(Uuid::new_v4());

        // Test direct state manipulation to verify deduplication logic
        state.push_work(fn_id, coll_id, 100, None);
        assert_eq!(state.pending_work.len(), 1);
        assert_eq!(state.pending_work[0].completion_offset, 100);

        // Push with lower offset should be ignored
        state.push_work(fn_id, coll_id, 50, None);
        assert_eq!(state.pending_work.len(), 1);
        assert_eq!(state.pending_work[0].completion_offset, 100);

        // Push with higher offset should replace
        state.push_work(fn_id, coll_id, 200, None);
        assert_eq!(state.pending_work.len(), 1);
        assert_eq!(state.pending_work[0].completion_offset, 200);

        assert!(state.dirty);
    }

    #[test]
    fn test_finish_work_success() {
        let mut state = QueueState::new();

        let fn_id = AttachedFunctionUuid(Uuid::new_v4());
        let coll_id = CollectionUuid(Uuid::new_v4());

        // Push work
        state.push_work(fn_id, coll_id, 100, None);
        assert_eq!(state.pending_work.len(), 1);

        // Finish work
        state.finish_work_success(&fn_id, &coll_id, 100);
        assert_eq!(state.pending_work.len(), 0);
        assert!(state.dirty);
    }

    #[test]
    fn test_get_work_filtering() {
        let mut state = QueueState::new();

        // Add multiple work items
        for i in 0..5 {
            state.push_work(
                AttachedFunctionUuid(Uuid::new_v4()),
                CollectionUuid(Uuid::new_v4()),
                i * 100,
                None,
            );
        }

        assert_eq!(state.pending_work.len(), 5);

        // Test filtering logic (simulating what get_work does)
        let limit = 3;
        let filtered: Vec<_> = state.pending_work.iter().take(limit).cloned().collect();

        assert_eq!(filtered.len(), 3);

        // Verify we got the first 3 items (FIFO order)
        for (i, item) in filtered.iter().enumerate().take(3) {
            assert_eq!(item.completion_offset, (i as i64) * 100);
        }
    }

    #[tokio::test]
    async fn test_load_state() {
        let temp_dir = TempDir::new().unwrap();
        let config = create_test_config();

        // Create and persist state
        {
            let storage = Storage::Local(LocalStorage::new(temp_dir.path().to_str().unwrap()));
            let sysdb = create_test_sysdb();
            let mut manager = WorkQueueManager::new(storage, config.clone(), sysdb);
            manager.state.push_work(
                AttachedFunctionUuid(Uuid::new_v4()),
                CollectionUuid(Uuid::new_v4()),
                100,
                None,
            );
            manager.state.push_work(
                AttachedFunctionUuid(Uuid::new_v4()),
                CollectionUuid(Uuid::new_v4()),
                200,
                None,
            );
            manager.persist().await.unwrap();
        }

        // Load state in new manager
        {
            let storage = Storage::Local(LocalStorage::new(temp_dir.path().to_str().unwrap()));
            let sysdb = create_test_sysdb();
            let mut manager = WorkQueueManager::new(storage, config, sysdb);
            manager.load_state().await.unwrap();
            assert_eq!(manager.state.pending_work.len(), 2);
            assert_eq!(manager.state.pending_work[0].completion_offset, 100);
            assert_eq!(manager.state.pending_work[1].completion_offset, 200);
        }
    }

    #[tokio::test]
    async fn test_notify_pending_responses() {
        let (mut manager, _temp_dir) = create_test_manager().await;

        // Add pending responses
        let (tx1, rx1) = oneshot::channel();
        let (tx2, rx2) = oneshot::channel();
        manager.pending_push_responses.push(tx1);
        manager
            .pending_finish_responses
            .push((FinishResult::NeedsRepair, tx2));

        // Notify success
        manager.notify_pending_responses();

        // Check responses
        assert!(rx1.await.unwrap().is_ok());
        let finish_result = rx2.await.unwrap().unwrap();
        assert!(matches!(finish_result, FinishResult::NeedsRepair));

        // Queues should be empty
        assert!(manager.pending_push_responses.is_empty());
        assert!(manager.pending_finish_responses.is_empty());
    }

    #[tokio::test]
    async fn test_notify_pending_responses_error() {
        let (mut manager, _temp_dir) = create_test_manager().await;

        // Add pending responses
        let (tx1, rx1) = oneshot::channel();
        let (tx2, rx2) = oneshot::channel();
        manager.pending_push_responses.push(tx1);
        manager
            .pending_finish_responses
            .push((FinishResult::NeedsRepair, tx2));

        // Notify error
        let error = WorkQueueError::Storage("test error".to_string());
        manager.notify_pending_responses_error(&error);

        // Check error responses
        assert!(matches!(
            rx1.await.unwrap(),
            Err(WorkQueueError::Storage(_))
        ));
        assert!(matches!(
            rx2.await.unwrap(),
            Err(WorkQueueError::Storage(_))
        ));

        // Queues should be empty
        assert!(manager.pending_push_responses.is_empty());
        assert!(manager.pending_finish_responses.is_empty());
    }

    // Note: ETag mismatch testing requires storage that supports conditional puts
    // LocalStorage may not enforce ETag conditions like S3 does
    // This test would be more appropriate with S3Storage or mocked storage
    // #[tokio::test]
    // #[should_panic(expected = "Work queue ETag mismatch")]
    // async fn test_etag_mismatch_panic() {
    //     // Test disabled for LocalStorage - would require S3 or mock storage
    // }

    #[test]
    fn test_finish_work_multiple_offsets() {
        let mut state = QueueState::new();

        let fn_id = AttachedFunctionUuid(Uuid::new_v4());
        let coll_id = CollectionUuid(Uuid::new_v4());

        // Push multiple work items with different offsets
        state.push_work(fn_id, coll_id, 100, None);
        state.push_work(fn_id, coll_id, 200, None);
        state.push_work(fn_id, coll_id, 300, None);
        assert_eq!(state.pending_work.len(), 1);
        assert_eq!(state.pending_work[0].completion_offset, 300);

        // Finish work up to offset 200
        state.finish_work_success(&fn_id, &coll_id, 200);

        // Should still have work with offset 300
        assert_eq!(state.pending_work.len(), 1);
        assert_eq!(state.pending_work[0].completion_offset, 300);

        // Finish remaining work
        state.finish_work_success(&fn_id, &coll_id, 300);
        assert_eq!(state.pending_work.len(), 0);
    }
}
