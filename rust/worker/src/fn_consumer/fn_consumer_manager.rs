use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_index::hnsw_provider::HnswIndexProvider;
use chroma_log::Log;
use chroma_segment::spann_provider::SpannProvider;
use chroma_sysdb::SysDb;
use chroma_system::{Component, ComponentContext, ComponentHandle, Dispatcher, Handler, System};
use chroma_types::{AttachedFunctionUuid, CollectionUuid};
use futures::future::join_all;
use std::collections::HashMap;
use std::time::{Duration, SystemTime};
use thiserror::Error;
use tracing::span;

use crate::execution::orchestration::compact::CompactionContext;
use crate::fn_consumer::config::FnConsumerConfig;
use crate::work_queue::work_queue_client::WorkQueueClient;

pub type FnJobKey = (AttachedFunctionUuid, CollectionUuid);

#[derive(Debug)]
pub struct InProgressFn {
    expires_at: SystemTime,
}

impl InProgressFn {
    pub fn new(job_expiry_seconds: u64) -> Self {
        Self {
            expires_at: SystemTime::now() + Duration::from_secs(job_expiry_seconds),
        }
    }

    pub fn is_expired(&self) -> bool {
        SystemTime::now() >= self.expires_at
    }
}

#[derive(Error, Debug)]
pub enum DispatchError {
    #[error("Dispatcher not initialized")]
    DispatcherNotInitialized,

    #[error("Compaction workflow failed: {0}")]
    CompactionFailed(#[from] crate::execution::orchestration::compact::CompactionError),
}

impl ChromaError for DispatchError {
    fn code(&self) -> ErrorCodes {
        match self {
            DispatchError::DispatcherNotInitialized => ErrorCodes::Internal,
            DispatchError::CompactionFailed(_) => ErrorCodes::Internal,
        }
    }
}

#[derive(Clone)]
pub struct FnConsumerContext {
    pub system: System,
    pub dispatcher: Option<ComponentHandle<Dispatcher>>,
    pub poll_interval: Duration,
    pub max_concurrent_workers: usize,
    pub get_work_batch_size: u32,
    pub job_expiry_seconds: u64,
    pub my_member_id: String,
    pub log: Log,
    pub sysdb: SysDb,
    pub blockfile_provider: BlockfileProvider,
    pub hnsw_provider: HnswIndexProvider,
    pub spann_provider: SpannProvider,
}

impl std::fmt::Debug for FnConsumerContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FnConsumerContext")
            .field("poll_interval", &self.poll_interval)
            .field("max_concurrent_workers", &self.max_concurrent_workers)
            .field("get_work_batch_size", &self.get_work_batch_size)
            .field("job_expiry_seconds", &self.job_expiry_seconds)
            .field("my_member_id", &self.my_member_id)
            .finish()
    }
}

pub struct FnConsumerManager {
    context: FnConsumerContext,
    in_progress: HashMap<FnJobKey, InProgressFn>,
    work_queue_client: WorkQueueClient,
}

impl std::fmt::Debug for FnConsumerManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FnConsumerManager")
            .field("context", &self.context)
            .field("in_progress_count", &self.in_progress.len())
            .finish()
    }
}

impl FnConsumerManager {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: FnConsumerConfig,
        my_member_id: String,
        system: System,
        work_queue_client: WorkQueueClient,
        log: Log,
        sysdb: SysDb,
        blockfile_provider: BlockfileProvider,
        hnsw_provider: HnswIndexProvider,
        spann_provider: SpannProvider,
    ) -> Self {
        let context = FnConsumerContext {
            system,
            dispatcher: None,
            poll_interval: Duration::from_secs(config.poll_interval_sec),
            max_concurrent_workers: config.max_concurrent_workers,
            get_work_batch_size: config.get_work_batch_size,
            job_expiry_seconds: config.job_expiry_seconds,
            my_member_id,
            log,
            sysdb,
            blockfile_provider,
            hnsw_provider,
            spann_provider,
        };
        Self {
            context,
            in_progress: HashMap::new(),
            work_queue_client,
        }
    }

    pub fn set_dispatcher(&mut self, dispatcher: ComponentHandle<Dispatcher>) {
        self.context.dispatcher = Some(dispatcher);
    }

    fn evict_expired(&mut self) {
        self.in_progress.retain(|_, j| !j.is_expired());
    }

    fn compute_remaining_capacity(&self) -> usize {
        self.context
            .max_concurrent_workers
            .saturating_sub(self.in_progress.len())
    }

    /// Runs the compaction workflow for the given function and collection.
    async fn dispatch_item(
        &self,
        fn_id: AttachedFunctionUuid,
        input_coll_id: CollectionUuid,
        completion_offset: i64,
    ) -> Result<(), DispatchError> {
        let Some(dispatcher) = self.context.dispatcher.clone() else {
            tracing::error!("Dispatcher not set on FnConsumerManager");
            return Err(DispatchError::DispatcherNotInitialized);
        };

        // Fetch collection information to get the database name
        let mut sysdb = self.context.sysdb.clone();
        let collection_info = match sysdb
            .get_collection_with_segments(None, input_coll_id)
            .await
        {
            Ok(info) => info,
            Err(e) => {
                tracing::error!(
                    fn_id = %fn_id,
                    input_coll_id = %input_coll_id,
                    "Failed to fetch collection information: {}",
                    e,
                );
                return Err(DispatchError::CompactionFailed(
                    crate::execution::orchestration::compact::CompactionError::InvariantViolation(
                        "Failed to fetch collection information",
                    ),
                ));
            }
        };

        let database_name =
            match chroma_types::DatabaseName::new(&collection_info.collection.database) {
                Some(name) => name,
                None => {
                    tracing::error!(
                        fn_id = %fn_id,
                        input_coll_id = %input_coll_id,
                        database = collection_info.collection.database,
                        "Invalid database name"
                    );
                    return Err(DispatchError::CompactionFailed(
                    crate::execution::orchestration::compact::CompactionError::InvariantViolation(
                        "Invalid database name",
                    ),
                ));
                }
            };

        // Create CompactionContext with is_fn_consumer = true
        let mut compaction_context = CompactionContext::new_with_log_offset(
            None,  // rebuild_info
            100,   // fetch_log_batch_size
            10,    // fetch_log_concurrency
            10000, // max_compaction_size
            1000,  // max_partition_size
            self.context.log.clone(),
            self.context.sysdb.clone(),
            self.context.blockfile_provider.clone(),
            self.context.hnsw_provider.clone(),
            self.context.spann_provider.clone(),
            dispatcher,
            false,                                // is_function_disabled
            true,                                 // is_fn_consumer
            None,                                 // fragment_fetcher
            None,                                 // bloom_filter_manager
            None,                                 // shard_size
            Some(self.work_queue_client.clone()), // work_queue_client
            completion_offset,                    // log_start_offset
        );

        // Run compaction workflow
        let result = Box::pin(compaction_context.run_compaction(
            input_coll_id,
            database_name,
            self.context.system.clone(),
        ))
        .await;

        match result {
            Ok(_response) => {
                tracing::info!(
                    fn_id = %fn_id,
                    input_coll_id = %input_coll_id,
                    "Function consumer workflow completed successfully"
                );
                Ok(())
            }
            Err(e) => {
                tracing::error!(
                    fn_id = %fn_id,
                    input_coll_id = %input_coll_id,
                    "Function consumer workflow failed: {}",
                    e,
                );
                Err(e.into())
            }
        }
    }

    async fn poll_and_dispatch(&mut self) {
        self.evict_expired();
        let rem = self.compute_remaining_capacity();
        if rem == 0 {
            tracing::debug!("fn_consumer at capacity, skipping poll");
            return;
        }
        let limit = rem.min(self.context.get_work_batch_size as usize) as u32;
        let resp = match self
            .work_queue_client
            .get_work(self.context.my_member_id.clone(), limit)
            .await
        {
            Ok(resp) => resp,
            Err(e) => {
                tracing::error!("GetWork failed: {}", e);
                return;
            }
        };
        // Collect valid work items first
        let mut work_items = Vec::new();
        for item in resp.items {
            let Ok(fn_id) = item.fn_id.parse::<AttachedFunctionUuid>() else {
                tracing::error!(fn_id = item.fn_id, "skipping work item: invalid fn_id");
                continue;
            };
            let Ok(input_coll_id) = item.input_coll_id.parse::<CollectionUuid>() else {
                tracing::error!(
                    input_coll_id = item.input_coll_id,
                    "skipping work item: invalid input_coll_id"
                );
                continue;
            };
            work_items.push((fn_id, input_coll_id, item.completion_offset));
        }

        let mut items_to_process = Vec::new();
        for (fn_id, input_coll_id, completion_offset) in work_items {
            let key = (fn_id, input_coll_id);

            if self.in_progress.contains_key(&key) {
                tracing::debug!(?key, "skipping: already in progress");
                continue;
            }

            self.in_progress
                .insert(key, InProgressFn::new(self.context.job_expiry_seconds));

            items_to_process.push((fn_id, input_coll_id, completion_offset));
        }

        let futures: Vec<_> = items_to_process
            .into_iter()
            .map(|(fn_id, input_coll_id, completion_offset)| {
                let fut = self.dispatch_item(fn_id, input_coll_id, completion_offset);
                Box::pin(async move {
                    let result = fut.await;
                    (fn_id, input_coll_id, result)
                })
            })
            .collect();

        let results = join_all(futures).await;

        for (fn_id, input_coll_id, result) in results {
            let key = (fn_id, input_coll_id);
            self.in_progress.remove(&key);

            match result {
                Ok(()) => {
                    tracing::debug!(
                        fn_id = %fn_id,
                        input_coll_id = %input_coll_id,
                        "Successfully completed work item"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        fn_id = %fn_id,
                        input_coll_id = %input_coll_id,
                        error = %e,
                        "Failed to process work item"
                    );
                }
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct ScheduledPollMessage;

#[async_trait]
impl Component for FnConsumerManager {
    fn get_name() -> &'static str {
        "Fn consumer manager"
    }

    fn queue_size(&self) -> usize {
        1000
    }

    async fn on_start(&mut self, ctx: &ComponentContext<Self>) {
        tracing::info!("Starting FnConsumerManager");
        ctx.scheduler.schedule(
            ScheduledPollMessage,
            self.context.poll_interval,
            ctx,
            || Some(span!(parent: None, tracing::Level::INFO, "Scheduled fn-consumer poll")),
        );
    }
}

#[async_trait]
impl Handler<ScheduledPollMessage> for FnConsumerManager {
    type Result = ();

    async fn handle(&mut self, _: ScheduledPollMessage, ctx: &ComponentContext<Self>) {
        Box::pin(self.poll_and_dispatch()).await;
        ctx.scheduler.schedule(
            ScheduledPollMessage,
            self.context.poll_interval,
            ctx,
            || Some(span!(parent: None, tracing::Level::INFO, "Scheduled fn-consumer poll")),
        );
    }
}
