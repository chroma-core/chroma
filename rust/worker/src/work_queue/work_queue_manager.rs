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
use chroma_system::{Component, ComponentContext, ComponentRuntime, Handler};
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

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct PeriodicPersistMessage;

// Component implementation
#[derive(Debug)]
pub struct WorkQueueManager {
    state: QueueState,
    storage: Storage,
    storage_path: String,
    #[allow(dead_code)]
    sysdb: Option<chroma_sysdb::SysDb>,
    config: crate::work_queue::config::WorkQueueConfig,
    // Pending responses waiting for persistence (push work responses)
    pending_push_responses: Vec<oneshot::Sender<Result<(), WorkQueueError>>>,
    // Pending responses waiting for persistence (finish work responses)
    pending_finish_responses: Vec<oneshot::Sender<Result<FinishResult, WorkQueueError>>>,
}

impl WorkQueueManager {
    pub fn new(storage: Storage, config: crate::work_queue::config::WorkQueueConfig) -> Self {
        Self {
            state: QueueState::new(),
            storage,
            storage_path: config.storage_path.clone(),
            sysdb: None, // TODO: inject when sysdb integration ready
            config,
            pending_push_responses: Vec::new(),
            pending_finish_responses: Vec::new(),
        }
    }

    // V1: Memberlist methods commented out
    // pub fn set_memberlist(&mut self, members: Vec<chroma_memberlist::memberlist_provider::Member>) {
    //     self.distributor = Some(WorkDistributor::new(members));
    // }

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

        for tx in self.pending_finish_responses.drain(..) {
            if tx.send(Ok(FinishResult::NeedsRepair)).is_err() {
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

        for tx in self.pending_finish_responses.drain(..) {
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
        response: WorkResponse,
    ) {
        let _ = self
            .state
            .push_work(fn_id, input_coll_id, completion_offset);

        // TODO(tanujnay112): Can optimize the case where we push work
        // that gets deduplicated. That would require epoch tracking
        // where the epoch is incremented per persistence event and
        // we associate each dedup map entry with an epoch.

        match response {
            WorkResponse::Push(tx) => self.pending_push_responses.push(tx),
            WorkResponse::Repair(tx) => self.pending_finish_responses.push(tx),
        }

        // Check if persist needed
        if self.should_persist() {
            if let Err(e) = self.persist().await {
                tracing::error!("Failed to persist work queue: {}", e);
            }
        }
    }

    // STUB: Will call sysdb's TryFinishAsyncAttachedFunctionInvocation
    async fn try_finish_invocation_stub(
        &self,
        _fn_id: &AttachedFunctionUuid,
        _input_coll_id: &CollectionUuid,
        _completion_offset: i64,
    ) -> FinishResult {
        // TODO: When sysdb is available:
        // if let Some(sysdb) = &self.sysdb {
        //     match sysdb.try_finish_async_attached_function_invocation(
        //         fn_id, input_coll_id, completion_offset
        //     ).await {
        //         Ok(TryFinishResult::Success) => FinishResult::Success,
        //         Ok(TryFinishResult::NeedsRepair) => FinishResult::NeedsRepair,
        //         Err(e) => panic!("sysdb error: {}", e),
        //     }
        // }
        FinishResult::Success
    }

    // STUB: Will check invocation completion status
    async fn check_invocations_done_stub(&self, items: &[WorkQueueRecord]) -> Vec<bool> {
        // TODO: Call sysdb's AreInvocationsDone
        // For now, return all false (not done)
        vec![false; items.len()]
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

        // Schedule periodic persistence
        ctx.scheduler.schedule(
            PeriodicPersistMessage,
            Duration::from_secs(self.config.persistence.time_threshold_seconds),
            ctx,
            || None,
        );

        // TODO(tanujnay112): Check to see if any entry needs repair.
        // This is done by looking at all the heap items and seeing if
        // any of them need a repair according to sysdb.
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
        self.push_work_and_queue_response(
            msg.fn_id,
            msg.input_coll_id,
            msg.completion_offset,
            WorkResponse::Push(msg.response_tx),
        )
        .await;
    }
}

#[async_trait]
impl Handler<FinishWorkMessage> for WorkQueueManager {
    type Result = ();

    async fn handle(&mut self, msg: FinishWorkMessage, _ctx: &ComponentContext<WorkQueueManager>) {
        // STUB: Call sysdb
        let finish_result = self
            .try_finish_invocation_stub(&msg.fn_id, &msg.input_coll_id, msg.new_completion_offset)
            .await;

        match finish_result {
            FinishResult::Success => {
                // Use encapsulated finish_work_success method
                self.state.finish_work_success(
                    &msg.fn_id,
                    &msg.input_coll_id,
                    msg.new_completion_offset,
                );

                // Send immediate success response
                if msg.response_tx.send(Ok(FinishResult::Success)).is_err() {
                    tracing::error!(
                        "Failed to send finish work success response - receiver dropped"
                    );
                }
                return;
            }
            FinishResult::NeedsRepair => {
                // Re-push work and queue response atomically
                self.push_work_and_queue_response(
                    msg.fn_id,
                    msg.input_coll_id,
                    msg.new_completion_offset,
                    WorkResponse::Repair(msg.response_tx),
                )
                .await;
            }
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

        // STUB: Check invocations done
        let done_flags = self.check_invocations_done_stub(&items).await;
        let filtered: Vec<_> = items
            .into_iter()
            .zip(done_flags.iter())
            .filter(|(_, done)| !**done)
            .map(|(item, _)| item)
            .take(msg.limit)
            .collect();

        if msg.response_tx.send(Ok(filtered)).is_err() {
            tracing::warn!("Failed to send get work response - receiver dropped");
        }
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
<<<<<<< HEAD
=======
        // Persist if dirty or if we have pending responses waiting
>>>>>>> 36a59fe48 (get rid of weird port setting)
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

    fn create_test_manager() -> (WorkQueueManager, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let storage = Storage::Local(LocalStorage::new(temp_dir.path().to_str().unwrap()));
        let mut config = create_test_config();
        config.storage_path = "queue.parquet".to_string(); // Use relative path within temp dir
        (WorkQueueManager::new(storage, config), temp_dir)
    }

    #[tokio::test]
    async fn test_push_work_deduplication() {
        let (mut manager, _temp_dir) = create_test_manager();

        let fn_id = AttachedFunctionUuid(Uuid::new_v4());
        let coll_id = CollectionUuid(Uuid::new_v4());

        // Test direct state manipulation to verify deduplication logic
        manager.state.push_work(fn_id, coll_id, 100);
        assert_eq!(manager.state.pending_work.len(), 1);
        assert_eq!(manager.state.pending_work[0].completion_offset, 100);

        // Push with lower offset should be ignored
        manager.state.push_work(fn_id, coll_id, 50);
        assert_eq!(manager.state.pending_work.len(), 1);
        assert_eq!(manager.state.pending_work[0].completion_offset, 100);

        // Push with higher offset should replace
        manager.state.push_work(fn_id, coll_id, 200);
        assert_eq!(manager.state.pending_work.len(), 1);
        assert_eq!(manager.state.pending_work[0].completion_offset, 200);

        assert!(manager.state.dirty);
    }

    #[tokio::test]
    async fn test_finish_work_success() {
        let (mut manager, _temp_dir) = create_test_manager();

        let fn_id = AttachedFunctionUuid(Uuid::new_v4());
        let coll_id = CollectionUuid(Uuid::new_v4());

        // Push work
        manager.state.push_work(fn_id, coll_id, 100);
        assert_eq!(manager.state.pending_work.len(), 1);

        // Finish work
        manager.state.finish_work_success(&fn_id, &coll_id, 100);
        assert_eq!(manager.state.pending_work.len(), 0);
        assert!(manager.state.dirty);
    }

    #[tokio::test]
    async fn test_get_work_filtering() {
        let (mut manager, _temp_dir) = create_test_manager();

        // Add multiple work items
        for i in 0..5 {
            manager.state.push_work(
                AttachedFunctionUuid(Uuid::new_v4()),
                CollectionUuid(Uuid::new_v4()),
                i * 100,
            );
        }

        let (tx, rx) = oneshot::channel();
        let msg = GetWorkMessage {
            shard_id: "shard-1".to_string(),
            limit: 3,
            response_tx: tx,
        };

        // Handle without context since we can't create one in tests
        let filtered: Vec<_> = manager
            .state
            .pending_work
            .iter()
            .take(msg.limit)
            .cloned()
            .collect();
        let _ = msg.response_tx.send(Ok(filtered));

        let result = rx.await.unwrap().unwrap();
        assert_eq!(result.len(), 3);
    }

    #[tokio::test]
    async fn test_load_state() {
        let temp_dir = TempDir::new().unwrap();
        let config = create_test_config();

        // Create and persist state
        {
            let storage = Storage::Local(LocalStorage::new(temp_dir.path().to_str().unwrap()));
            let mut manager = WorkQueueManager::new(storage, config.clone());
            manager.state.push_work(
                AttachedFunctionUuid(Uuid::new_v4()),
                CollectionUuid(Uuid::new_v4()),
                100,
            );
            manager.state.push_work(
                AttachedFunctionUuid(Uuid::new_v4()),
                CollectionUuid(Uuid::new_v4()),
                200,
            );
            manager.persist().await.unwrap();
        }

        // Load state in new manager
        {
            let storage = Storage::Local(LocalStorage::new(temp_dir.path().to_str().unwrap()));
            let mut manager = WorkQueueManager::new(storage, config);
            manager.load_state().await.unwrap();
            assert_eq!(manager.state.pending_work.len(), 2);
            assert_eq!(manager.state.pending_work[0].completion_offset, 100);
            assert_eq!(manager.state.pending_work[1].completion_offset, 200);
        }
    }

    #[tokio::test]
    async fn test_notify_pending_responses() {
        let (mut manager, _temp_dir) = create_test_manager();

        // Add pending responses
        let (tx1, rx1) = oneshot::channel();
        let (tx2, rx2) = oneshot::channel();
        manager.pending_push_responses.push(tx1);
        manager.pending_finish_responses.push(tx2);

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
        let (mut manager, _temp_dir) = create_test_manager();

        // Add pending responses
        let (tx1, rx1) = oneshot::channel();
        let (tx2, rx2) = oneshot::channel();
        manager.pending_push_responses.push(tx1);
        manager.pending_finish_responses.push(tx2);

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

    #[tokio::test]
    async fn test_finish_work_multiple_offsets() {
        let (mut manager, _temp_dir) = create_test_manager();

        let fn_id = AttachedFunctionUuid(Uuid::new_v4());
        let coll_id = CollectionUuid(Uuid::new_v4());

        // Push multiple work items with different offsets
        manager.state.push_work(fn_id, coll_id, 100);
        manager.state.push_work(fn_id, coll_id, 200);
        manager.state.push_work(fn_id, coll_id, 300);
        assert_eq!(manager.state.pending_work.len(), 1);
        assert_eq!(manager.state.pending_work[0].completion_offset, 300);

        // Finish work up to offset 200
        manager.state.finish_work_success(&fn_id, &coll_id, 200);

        // Should still have work with offset 300
        assert_eq!(manager.state.pending_work.len(), 1);
        assert_eq!(manager.state.pending_work[0].completion_offset, 300);

        // Finish remaining work
        manager.state.finish_work_success(&fn_id, &coll_id, 300);
        assert_eq!(manager.state.pending_work.len(), 0);
    }
}
