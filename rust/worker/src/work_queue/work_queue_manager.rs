// V1: WorkDistributor import commented out
// use crate::work_queue::distribution::WorkDistributor;
use crate::work_queue::state::QueueState;
use crate::work_queue::types::{WorkQueueError, WorkQueueRecord};
use async_trait::async_trait;
use chroma_error::ChromaError;
use chroma_storage::{GetOptions, PutMode, PutOptions, Storage};
use chroma_sysdb::SysDb;
use chroma_system::{Component, ComponentContext, ComponentRuntime, Handler};
use chroma_types::chroma_proto::TryFinishAsyncAttachedFunctionInvocationRequest;
use chroma_types::{AttachedFunctionUuid, CollectionUuid};
use std::time::Duration;
use tokio::sync::oneshot;
use tonic::Code;

// Message types
#[derive(Debug)]
#[allow(dead_code)]
pub struct PushWorkMessage {
    pub fn_id: AttachedFunctionUuid,
    pub input_coll_id: CollectionUuid,
    pub completion_offset: i64,
    pub compaction_offset: i64,
    pub response_tx: oneshot::Sender<Result<(), WorkQueueError>>,
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct FinishWorkMessage {
    pub fn_id: AttachedFunctionUuid,
    pub input_coll_id: CollectionUuid,
    pub new_completion_offset: i64,
    pub response_tx: oneshot::Sender<Result<(), WorkQueueError>>,
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
                let total_pending = self.pending_push_responses.len();
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
    }

    fn notify_pending_responses_error(&mut self, error: &WorkQueueError) {
        for tx in self.pending_push_responses.drain(..) {
            if tx.send(Err(error.clone())).is_err() {
                tracing::error!("Failed to send push work error response - receiver dropped");
            }
        }
    }

    fn should_persist(&self) -> bool {
        if !self.state.dirty {
            return false;
        }

        let total_pending_responses = self.pending_push_responses.len();

        total_pending_responses >= self.config.persistence.pending_threshold
    }

    async fn push_work_and_queue_response(&mut self, msg: PushWorkMessage) {
        let PushWorkMessage {
            fn_id,
            input_coll_id,
            completion_offset,
            compaction_offset,
            response_tx,
        } = msg;

        let _ = self
            .state
            .push_work(fn_id, input_coll_id, completion_offset, compaction_offset);

        // TODO(tanujnay112): Can optimize the case where we push work
        // that gets deduplicated. That would require epoch tracking
        // where the epoch is incremented per persistence event and
        // we associate each dedup map entry with an epoch.

        self.pending_push_responses.push(response_tx);

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

        match self
            .sysdb
            .try_finish_async_attached_function_invocation(request)
            .await
        {
            Ok(_) => Ok(()),
            Err(status) if status.code() == Code::NotFound => {
                tracing::info!(
                    fn_id = %fn_id,
                    input_coll_id = %input_coll_id,
                    completion_offset,
                    "Ignoring missing async invocation during finish because work queue cleanup is idempotent"
                );
                Ok(())
            }
            Err(status) => Err(WorkQueueError::TryFinishFailed(
                status.message().to_string(),
            )),
        }
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
    }

    async fn on_stop(&mut self) -> Result<(), Box<dyn ChromaError>> {
        tracing::info!("Stopping WorkQueueManager");

        // Final persist if dirty or if we have pending responses
        if self.state.dirty || !self.pending_push_responses.is_empty() {
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
            "Received PushWorkMessage for fn_id: {}, input_coll_id: {}, completion_offset: {}, compaction_offset: {}",
            msg.fn_id,
            msg.input_coll_id,
            msg.completion_offset,
            msg.compaction_offset
        );
        self.push_work_and_queue_response(msg).await;
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
        self.state
            .finish_work_success(&msg.fn_id, &msg.input_coll_id, msg.new_completion_offset);

        // Send immediate success response
        if msg.response_tx.send(Ok(())).is_err() {
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
        // With eager stale-row removal on push, the queue's dedup index is the
        // source of truth for whether a row is still live.
        let filtered: Vec<_> = self
            .state
            .pending_work
            .iter()
            .filter(|item| self.state.contains_entry(&item.fn_id, &item.input_coll_id))
            .take(msg.limit)
            .cloned()
            .collect();
        tracing::info!("Returning {} items from get work response", filtered.len());

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
    use arrow::array::{Int64Array, StringArray, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use chroma_storage::local::LocalStorage;
    use parquet::arrow::ArrowWriter;
    use std::sync::Arc;
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
        state.push_work(fn_id, coll_id, 100, 100);
        assert_eq!(state.pending_work.len(), 1);
        assert_eq!(state.pending_work[0].completion_offset, 100);

        // Push with lower offset should be ignored
        state.push_work(fn_id, coll_id, 50, 50);
        assert_eq!(state.pending_work.len(), 1);
        assert_eq!(state.pending_work[0].completion_offset, 100);

        // Push with higher offset should replace
        state.push_work(fn_id, coll_id, 200, 200);
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
        state.push_work(fn_id, coll_id, 100, 100);
        assert_eq!(state.pending_work.len(), 1);

        // Matching the queued frontier keeps the entry queued until work
        // advances beyond it.
        state.finish_work_success(&fn_id, &coll_id, 100);
        assert_eq!(state.pending_work.len(), 1);

        state.finish_work_success(&fn_id, &coll_id, 101);
        assert_eq!(state.pending_work.len(), 0);
        assert!(state.dirty);
    }

    #[test]
    fn test_get_work_filtering() {
        let mut state = QueueState::new();
        let stale_fn_id = AttachedFunctionUuid(Uuid::new_v4());
        let stale_coll_id = CollectionUuid(Uuid::new_v4());

        // Add multiple work items
        for i in 0..5 {
            state.push_work(
                AttachedFunctionUuid(Uuid::new_v4()),
                CollectionUuid(Uuid::new_v4()),
                i * 100,
                i * 100,
            );
        }

        assert_eq!(state.pending_work.len(), 5);

        // Add an orphaned row to simulate a stale queue entry without a dedup record.
        state.pending_work.push_back(WorkQueueRecord {
            fn_id: stale_fn_id,
            input_coll_id: stale_coll_id,
            completion_offset: 999,
            compaction_offset: 999,
            insertion_order: 999,
        });

        // Test filtering logic (simulating what get_work does)
        let limit = 3;
        let filtered: Vec<_> = state
            .pending_work
            .iter()
            .filter(|item| state.contains_entry(&item.fn_id, &item.input_coll_id))
            .take(limit)
            .cloned()
            .collect();

        assert_eq!(filtered.len(), 3);

        // Verify we got the first 3 live items in FIFO order.
        for (i, item) in filtered.iter().enumerate().take(3) {
            assert_eq!(item.completion_offset, (i as i64) * 100);
        }
    }

    #[tokio::test]
    async fn test_load_state() {
        let temp_dir = TempDir::new().unwrap();
        let config = create_test_config();
        let fn_id_1 = AttachedFunctionUuid(Uuid::new_v4());
        let coll_id_1 = CollectionUuid(Uuid::new_v4());
        let fn_id_2 = AttachedFunctionUuid(Uuid::new_v4());
        let coll_id_2 = CollectionUuid(Uuid::new_v4());

        // Create and persist state
        {
            let storage = Storage::Local(LocalStorage::new(temp_dir.path().to_str().unwrap()));
            let sysdb = create_test_sysdb();
            let mut manager = WorkQueueManager::new(storage, config.clone(), sysdb);
            manager.state.push_work(fn_id_1, coll_id_1, 100, 100);
            manager.state.push_work(fn_id_2, coll_id_2, 200, 200);
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
    async fn test_load_state_hydrates_legacy_compaction_offsets() {
        let (mut manager, _temp_dir) = create_test_manager().await;
        let fn_id = AttachedFunctionUuid(Uuid::new_v4());
        let coll_id = CollectionUuid(Uuid::new_v4());

        let schema = Arc::new(Schema::new(vec![
            Field::new("fn_id", DataType::Utf8, false),
            Field::new("input_coll_id", DataType::Utf8, false),
            Field::new("completion_offset", DataType::Int64, false),
            Field::new("insertion_order", DataType::UInt64, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![fn_id.to_string()])),
                Arc::new(StringArray::from(vec![coll_id.to_string()])),
                Arc::new(Int64Array::from(vec![100])),
                Arc::new(UInt64Array::from(vec![0])),
            ],
        )
        .expect("Failed to build legacy batch");

        let mut buffer = Vec::new();
        let mut writer =
            ArrowWriter::try_new(&mut buffer, schema, None).expect("Failed to create writer");
        writer.write(&batch).expect("Failed to write batch");
        writer.close().expect("Failed to close writer");

        manager
            .storage
            .put_bytes(
                &manager.storage_path,
                buffer,
                PutOptions::default().with_mode(PutMode::Upsert),
            )
            .await
            .expect("Failed to persist legacy state");

        manager
            .load_state()
            .await
            .expect("Failed to load legacy state");

        assert_eq!(manager.state.pending_work.len(), 1);
        assert_eq!(manager.state.pending_work[0].completion_offset, 100);
        assert_eq!(manager.state.pending_work[0].compaction_offset, 100);
    }

    #[tokio::test]
    async fn test_try_finish_invocation_ignores_missing_attached_function() {
        let (mut manager, _temp_dir) = create_test_manager().await;
        let fn_id = AttachedFunctionUuid(Uuid::new_v4());
        let coll_id = CollectionUuid(Uuid::new_v4());

        manager
            .try_finish_invocation(&fn_id, &coll_id, 101)
            .await
            .expect("missing attached functions should not block queue cleanup");
    }

    #[tokio::test]
    async fn test_notify_pending_responses() {
        let (mut manager, _temp_dir) = create_test_manager().await;

        // Add pending responses
        let (tx1, rx1) = oneshot::channel();
        manager.pending_push_responses.push(tx1);

        // Notify success
        manager.notify_pending_responses();

        // Check responses
        assert!(rx1.await.unwrap().is_ok());

        // Queues should be empty
        assert!(manager.pending_push_responses.is_empty());
    }

    #[tokio::test]
    async fn test_notify_pending_responses_error() {
        let (mut manager, _temp_dir) = create_test_manager().await;

        // Add pending responses
        let (tx1, rx1) = oneshot::channel();
        manager.pending_push_responses.push(tx1);

        // Notify error
        let error = WorkQueueError::Storage("test error".to_string());
        manager.notify_pending_responses_error(&error);

        // Check error responses
        assert!(matches!(
            rx1.await.unwrap(),
            Err(WorkQueueError::Storage(_))
        ));

        // Queues should be empty
        assert!(manager.pending_push_responses.is_empty());
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
        state.push_work(fn_id, coll_id, 100, 100);
        state.push_work(fn_id, coll_id, 200, 200);
        state.push_work(fn_id, coll_id, 300, 300);
        assert_eq!(state.pending_work.len(), 1);
        assert_eq!(state.pending_work[0].completion_offset, 300);

        // Finish work up to offset 200
        state.finish_work_success(&fn_id, &coll_id, 200);

        // Should still have work with offset 300
        assert_eq!(state.pending_work.len(), 1);
        assert_eq!(state.pending_work[0].completion_offset, 300);

        // Matching the queued frontier keeps the entry queued.
        state.finish_work_success(&fn_id, &coll_id, 300);
        assert_eq!(state.pending_work.len(), 1);

        // Finish remaining work
        state.finish_work_success(&fn_id, &coll_id, 301);
        assert_eq!(state.pending_work.len(), 0);
    }
}
