use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_index::hnsw_provider::HnswIndexProvider;
use chroma_log::Log;
use chroma_segment::spann_provider::SpannProvider;
use chroma_sysdb::SysDb;
use chroma_system::{Component, ComponentContext, ComponentHandle, Dispatcher, Handler, System};
use chroma_types::{AttachedFunctionUuid, CollectionUuid};
use std::collections::HashMap;
use std::time::{Duration, SystemTime};
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

    /// Records dispatch and (will) run the orchestrator inline. v1 stubs
    /// the dispatch — only the in-flight bookkeeping is wired up. A later
    /// change replaces this with the real compaction workflow. Returns
    /// false if a matching (fn_id, input_coll_id) is already in flight or
    /// the dispatcher hasn't been wired yet.
    async fn dispatch_item(
        &mut self,
        fn_id: AttachedFunctionUuid,
        input_coll_id: CollectionUuid,
        _completion_offset: i64,
    ) -> bool {
        let key = (fn_id, input_coll_id);
        if self.in_progress.contains_key(&key) {
            tracing::debug!(?key, "skipping: in progress");
            return false;
        }
        self.in_progress
            .insert(key, InProgressFn::new(self.context.job_expiry_seconds));
        let Some(dispatcher) = self.context.dispatcher.clone() else {
            tracing::error!("Dispatcher not set on FnConsumerManager");
            return false;
        };
        // Create CompactionContext with is_fn_consumer = true
        let mut compaction_context = CompactionContext::new(
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
        );

        // Run compaction workflow
        match Box::pin(compaction_context.run_compaction(
            input_coll_id,
            chroma_types::DatabaseName::new("default_database").unwrap(), // TODO: Get database name from collection
            self.context.system.clone(),
        ))
        .await
        {
            Ok(_response) => {
                tracing::info!(
                    fn_id = %fn_id,
                    input_coll_id = %input_coll_id,
                    "Function consumer workflow completed successfully"
                );
            }
            Err(e) => {
                tracing::error!(
                    fn_id = %fn_id,
                    input_coll_id = %input_coll_id,
                    "Function consumer workflow failed: {}",
                    e,
                );
            }
        }
        true
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
            Box::pin(self.dispatch_item(fn_id, input_coll_id, item.completion_offset)).await;
        }
    }
}

#[derive(Clone, Debug)]
pub struct ScheduledPollMessage;

#[derive(Clone, Debug)]
pub struct RemoveInProgressMessage {
    pub fn_id: AttachedFunctionUuid,
    pub input_coll_id: CollectionUuid,
}

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

#[async_trait]
impl Handler<RemoveInProgressMessage> for FnConsumerManager {
    type Result = ();

    async fn handle(&mut self, msg: RemoveInProgressMessage, _ctx: &ComponentContext<Self>) {
        self.in_progress.remove(&(msg.fn_id, msg.input_coll_id));
    }
}
