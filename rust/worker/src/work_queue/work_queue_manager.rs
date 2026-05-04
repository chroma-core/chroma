use crate::work_queue::state::QueueState;
use crate::work_queue::types::{FinishResult, WorkQueueError, WorkQueueRecord};
use async_trait::async_trait;
use chroma_error::ChromaError;
use chroma_storage::Storage;
use chroma_system::{Component, ComponentContext, ComponentRuntime, Handler, System};
use chroma_types::{AttachedFunctionUuid, CollectionUuid};
use std::time::{Duration, Instant};
use tokio::sync::oneshot;
use tracing::{error, info};

// Message types
#[derive(Debug)]
pub struct PushWorkMessage {
    pub fn_id: AttachedFunctionUuid,
    pub input_coll_id: CollectionUuid,
    pub completion_offset: i64,
    pub response_tx: oneshot::Sender<Result<(), WorkQueueError>>,
}

#[derive(Debug)]
pub struct FinishWorkMessage {
    pub fn_id: AttachedFunctionUuid,
    pub input_coll_id: CollectionUuid,
    pub completion_offset: i64,
    pub response_tx: oneshot::Sender<Result<(), WorkQueueError>>,
}

#[derive(Debug)]
pub struct GetWorkMessage {
    pub shard_id: String,
    pub limit: usize,
    pub response_tx: oneshot::Sender<Result<Vec<WorkQueueRecord>, WorkQueueError>>,
}

#[derive(Debug)]
pub struct PeriodicPersistMessage;

// Component implementation
#[derive(Debug)]
pub struct WorkQueueManager {
    state: QueueState,
    storage: Storage,
    storage_path: String,
    sysdb: Option<chroma_sysdb::SysDb>,
    last_persist: Instant,
    operations_since_persist: u64,
    config: crate::work_queue::config::WorkQueueConfig,
}

impl WorkQueueManager {
    pub fn new(storage: Storage, config: crate::work_queue::config::WorkQueueConfig) -> Self {
        Self {
            state: QueueState::new(),
            storage,
            storage_path: config.storage_path.clone(),
            sysdb: None, // TODO: inject when sysdb integration ready
            last_persist: Instant::now(),
            operations_since_persist: 0,
            config,
        }
    }

    async fn load_state(&mut self) -> Result<(), WorkQueueError> {
        match self.storage.get_bytes(&self.storage_path).await {
            Ok((bytes, etag)) => {
                self.state = QueueState::from_parquet_bytes(&bytes)?;
                self.state.current_etag = Some(etag);
                info!(
                    "Loaded work queue state with {} items",
                    self.state.pending_work.len()
                );
                Ok(())
            }
            Err(e) if e.to_string().contains("not found") => {
                info!("No existing work queue state found, starting fresh");
                Ok(())
            }
            Err(e) => Err(WorkQueueError::Storage(e.to_string())),
        }
    }

    async fn persist(&mut self) -> Result<(), WorkQueueError> {
        if !self.state.dirty {
            return Ok(());
        }

        let bytes = self.state.to_parquet_bytes()?;

        match self
            .storage
            .put_bytes_with_etag(
                &self.storage_path,
                bytes,
                self.state.current_etag.as_deref(),
            )
            .await
        {
            Ok(new_etag) => {
                self.state.current_etag = Some(new_etag);
                self.state.dirty = false;
                self.operations_since_persist = 0;
                self.last_persist = Instant::now();
                info!("Persisted work queue state");
                Ok(())
            }
            Err(e) if e.to_string().contains("precondition") => {
                error!("ETag mismatch - another instance is active");
                panic!("Work queue ETag mismatch - shutting down");
            }
            Err(e) => Err(WorkQueueError::Storage(e.to_string())),
        }
    }

    fn should_persist(&self) -> bool {
        if !self.state.dirty {
            return false;
        }

        let time_elapsed = self.last_persist.elapsed()
            > Duration::from_secs(self.config.persistence.time_threshold_seconds);
        let ops_exceeded =
            self.operations_since_persist >= self.config.persistence.operation_threshold;
        let memory_exceeded =
            self.state.pending_work.len() >= self.config.persistence.memory_threshold;

        time_elapsed || ops_exceeded || memory_exceeded
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

    // STUB: Will handle repair flow with sysdb
    async fn handle_repair_stub(&mut self, _fn_id: &AttachedFunctionUuid) {
        // TODO: Call sysdb's get_attached_functions
        // TODO: Extract latest completion_offset
        // TODO: Re-push work with new offset
        // TODO: Call FinalizeAsyncAttachedFunctionRepair
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
        info!("Starting WorkQueueManager");

        // Load existing state
        if let Err(e) = self.load_state().await {
            error!("Failed to load work queue state: {}", e);
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
        info!("Stopping WorkQueueManager");

        // Final persist if dirty
        if self.state.dirty {
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
        let key = (msg.fn_id.clone(), msg.input_coll_id.clone());

        // Check dedup
        if let Some(&existing_offset) = self.state.dedup_index.get(&key) {
            if msg.completion_offset <= existing_offset {
                let _ = msg.response_tx.send(Ok(()));
                return;
            }
        }

        // Remove old entries
        self.state
            .pending_work
            .retain(|r| !(r.fn_id == msg.fn_id && r.input_coll_id == msg.input_coll_id));

        // Add new entry
        let record = WorkQueueRecord {
            fn_id: msg.fn_id,
            input_coll_id: msg.input_coll_id,
            completion_offset: msg.completion_offset,
            insertion_order: self.state.next_insertion_order,
        };

        self.state.next_insertion_order += 1;
        self.state.dedup_index.insert(key, msg.completion_offset);
        self.state.pending_work.push_back(record);
        self.state.dirty = true;
        self.operations_since_persist += 1;

        // Check if persist needed
        if self.should_persist() {
            let _ = self.persist().await;
        }

        let _ = msg.response_tx.send(Ok(()));
    }
}

#[async_trait]
impl Handler<FinishWorkMessage> for WorkQueueManager {
    type Result = ();

    async fn handle(&mut self, msg: FinishWorkMessage, _ctx: &ComponentContext<WorkQueueManager>) {
        // STUB: Call sysdb
        let finish_result = self
            .try_finish_invocation_stub(&msg.fn_id, &msg.input_coll_id, msg.completion_offset)
            .await;

        match finish_result {
            FinishResult::Success => {
                let key = (msg.fn_id.clone(), msg.input_coll_id.clone());
                self.state
                    .completed_work
                    .insert(key.clone(), msg.completion_offset);

                // Remove completed items
                self.state.pending_work.retain(|r| {
                    !(r.fn_id == msg.fn_id
                        && r.input_coll_id == msg.input_coll_id
                        && r.completion_offset <= msg.completion_offset)
                });

                // Update dedup index
                let max_remaining = self
                    .state
                    .pending_work
                    .iter()
                    .filter(|r| r.fn_id == msg.fn_id && r.input_coll_id == msg.input_coll_id)
                    .map(|r| r.completion_offset)
                    .max();

                if let Some(max) = max_remaining {
                    self.state.dedup_index.insert(key, max);
                } else {
                    self.state.dedup_index.remove(&key);
                }

                self.state.dirty = true;
                self.operations_since_persist += 1;
            }
            FinishResult::NeedsRepair => {
                // STUB: Handle repair
                self.handle_repair_stub(&msg.fn_id).await;
            }
        }

        // Check if persist needed
        if self.should_persist() {
            let _ = self.persist().await;
        }

        let _ = msg.response_tx.send(Ok(()));
    }
}

#[async_trait]
impl Handler<GetWorkMessage> for WorkQueueManager {
    type Result = ();

    async fn handle(&mut self, msg: GetWorkMessage, _ctx: &ComponentContext<WorkQueueManager>) {
        // TODO: Add memberlist and rendezvous hashing
        // For now, return all items up to limit

        let items: Vec<_> = self
            .state
            .pending_work
            .iter()
            .take(msg.limit * 2)
            .cloned()
            .collect();

        // STUB: Check invocations done
        if self.config.use_sysdb_filtering {
            let done_flags = self.check_invocations_done_stub(&items).await;
            let filtered: Vec<_> = items
                .into_iter()
                .zip(done_flags.iter())
                .filter(|(_, done)| !done)
                .map(|(item, _)| item)
                .take(msg.limit)
                .collect();

            let _ = msg.response_tx.send(Ok(filtered));
        } else {
            let result: Vec<_> = items.into_iter().take(msg.limit).collect();
            let _ = msg.response_tx.send(Ok(result));
        }
    }
}

#[async_trait]
impl Handler<PeriodicPersistMessage> for WorkQueueManager {
    type Result = ();

    async fn handle(
        &mut self,
        _msg: PeriodicPersistMessage,
        _ctx: &ComponentContext<WorkQueueManager>,
    ) {
        if self.state.dirty {
            if let Err(e) = self.persist().await {
                error!("Periodic persist failed: {}", e);
            }
        }
    }
}
