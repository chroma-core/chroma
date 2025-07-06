use super::scheduler::Scheduler;
use super::scheduler_policy::LasCompactionTimeSchedulerPolicy;
use super::OneOffCompactMessage;
use super::RebuildMessage;
use crate::compactor::types::ScheduledCompactMessage;
use crate::config::CompactionServiceConfig;
use crate::execution::operators::purge_dirty_log::PurgeDirtyLog;
use crate::execution::operators::purge_dirty_log::PurgeDirtyLogError;
use crate::execution::operators::purge_dirty_log::PurgeDirtyLogInput;
use crate::execution::operators::purge_dirty_log::PurgeDirtyLogOutput;
use crate::execution::orchestration::CompactOrchestrator;
use crate::execution::orchestration::CompactionResponse;
use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_config::assignment::assignment_policy::AssignmentPolicy;
use chroma_config::registry::Registry;
use chroma_config::Configurable;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_index::hnsw_provider::HnswIndexProvider;
use chroma_log::Log;
use chroma_memberlist::memberlist_provider::Memberlist;
use chroma_segment::spann_provider::SpannProvider;
use chroma_storage::Storage;
use chroma_sysdb::SysDb;
use chroma_system::wrap;
use chroma_system::Dispatcher;
use chroma_system::Orchestrator;
use chroma_system::TaskResult;
use chroma_system::{Component, ComponentContext, ComponentHandle, Handler, System};
use chroma_types::CollectionUuid;
use futures::stream::FuturesUnordered;
use futures::FutureExt;
use futures::StreamExt;
use std::collections::HashSet;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::str::FromStr;
use std::time::Duration;
use thiserror::Error;
use tokio::select;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tracing::instrument;
use tracing::span;
use tracing::Instrument;
use tracing::Span;
use uuid::Uuid;

type BoxedFuture =
    Pin<Box<dyn Future<Output = Result<CompactionResponse, Box<dyn ChromaError>>> + Send>>;

struct CompactionTask {
    collection_id: CollectionUuid,
    future: BoxedFuture,
}

#[derive(Clone)]
pub(crate) struct CompactionManagerContext {
    system: System,
    // Dependencies
    log: Log,
    sysdb: SysDb,
    #[allow(dead_code)]
    storage: Storage,
    blockfile_provider: BlockfileProvider,
    hnsw_index_provider: HnswIndexProvider,
    spann_provider: SpannProvider,
    // Dispatcher
    dispatcher: Option<ComponentHandle<Dispatcher>>,
    // Config
    compaction_manager_queue_size: usize,
    compaction_interval: Duration,
    #[allow(dead_code)]
    min_compaction_size: usize,
    max_compaction_size: usize,
    max_partition_size: usize,
    fetch_log_batch_size: u32,
    purge_dirty_log_timeout_seconds: u64,
}

pub(crate) struct CompactionManager {
    scheduler: Scheduler,
    context: CompactionManagerContext,
    compact_awaiter_channel: mpsc::Sender<CompactionTask>,
    compact_awaiter_completion_channel: mpsc::UnboundedReceiver<CompactionResponse>,
    compact_awaiter: tokio::task::JoinHandle<()>,
    on_next_memberlist_signal: Option<oneshot::Sender<()>>,
}

#[derive(Error, Debug)]
pub(crate) enum CompactionError {
    #[error("Failed to compact")]
    FailedToCompact,
}

impl ChromaError for CompactionError {
    fn code(&self) -> ErrorCodes {
        match self {
            CompactionError::FailedToCompact => ErrorCodes::Internal,
        }
    }
}

impl CompactionManager {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        system: System,
        scheduler: Scheduler,
        log: Log,
        sysdb: SysDb,
        storage: Storage,
        blockfile_provider: BlockfileProvider,
        hnsw_index_provider: HnswIndexProvider,
        spann_provider: SpannProvider,
        compaction_manager_queue_size: usize,
        compaction_interval: Duration,
        min_compaction_size: usize,
        max_compaction_size: usize,
        max_partition_size: usize,
        fetch_log_batch_size: u32,
        purge_dirty_log_timeout_seconds: u64,
    ) -> Self {
        let (compact_awaiter_tx, compact_awaiter_rx) =
            mpsc::channel::<CompactionTask>(compaction_manager_queue_size);

        // Using unbounded channel for the completion channel as its size
        // is bounded by max_concurrent_jobs. It's far more important for the
        // completion channel to not block or drop messages.
        let (completion_tx, completion_rx) = mpsc::unbounded_channel::<CompactionResponse>();
        let compact_awaiter = tokio::spawn(async {
            compact_awaiter_loop(compact_awaiter_rx, completion_tx).await;
        });
        CompactionManager {
            scheduler,
            context: CompactionManagerContext {
                system,
                log,
                sysdb,
                storage,
                blockfile_provider,
                hnsw_index_provider,
                spann_provider,
                dispatcher: None,
                compaction_manager_queue_size,
                compaction_interval,
                min_compaction_size,
                max_compaction_size,
                max_partition_size,
                fetch_log_batch_size,
                purge_dirty_log_timeout_seconds,
            },
            on_next_memberlist_signal: None,
            compact_awaiter_channel: compact_awaiter_tx,
            compact_awaiter_completion_channel: completion_rx,
            compact_awaiter,
        }
    }

    #[instrument(name = "CompactionManager::start_compaction_batch", skip(self))]
    async fn start_compaction_batch(&mut self) {
        self.process_completions();
        let compact_awaiter_channel = &self.compact_awaiter_channel;
        self.scheduler.schedule().await;
        let jobs_iter = self.scheduler.get_jobs();
        for job in jobs_iter {
            let instrumented_span = span!(
                parent: None,
                tracing::Level::INFO,
                "Compacting job",
                collection_id = ?job.collection_id
            );
            instrumented_span.follows_from(Span::current());
            let future = self
                .context
                .clone()
                .compact(job.collection_id, false)
                .instrument(instrumented_span);
            compact_awaiter_channel
                .send(CompactionTask {
                    collection_id: job.collection_id,
                    future: Box::pin(future),
                })
                .await
                .unwrap();
        }
    }

    #[instrument(name = "CompactionManager::rebuild_batch", skip(self))]
    pub(crate) async fn rebuild_batch(&mut self, collection_ids: &[CollectionUuid]) {
        let _ = collection_ids
            .iter()
            .map(|id| self.context.clone().compact(*id, true))
            .collect::<FuturesUnordered<_>>()
            .collect::<Vec<_>>()
            .await;
    }

    #[instrument(name = "CompactionManager::purge_dirty_log", skip(ctx))]
    pub(crate) async fn purge_dirty_log(&mut self, ctx: &ComponentContext<Self>) {
        let deleted_collection_uuids = self.scheduler.drain_deleted_collections();
        if deleted_collection_uuids.is_empty() {
            tracing::info!("Skipping purge dirty log because there is no deleted collections");
            return;
        }
        let purge_dirty_log = PurgeDirtyLog {
            log_client: self.context.log.clone(),
            timeout: Duration::from_secs(self.context.purge_dirty_log_timeout_seconds),
        };
        let purge_dirty_log_input = PurgeDirtyLogInput {
            collection_uuids: deleted_collection_uuids.clone(),
        };
        let purge_dirty_log_task = wrap(
            Box::new(purge_dirty_log),
            purge_dirty_log_input,
            ctx.receiver(),
        );
        let Some(mut dispatcher) = self.context.dispatcher.clone() else {
            tracing::error!("Unable to create background task to purge dirty log: Dispatcher is not set for compaction manager");
            return;
        };
        if let Err(err) = dispatcher
            .send(purge_dirty_log_task, Some(Span::current()))
            .await
        {
            tracing::error!("Unable to create background task to purge dirty log: {err}");
            return;
        };
        tracing::info!(
            "Purging dirty logs for deleted collections: [{deleted_collection_uuids:?}]",
        );
    }

    pub(crate) fn set_dispatcher(&mut self, dispatcher: ComponentHandle<Dispatcher>) {
        self.context.dispatcher = Some(dispatcher);
    }

    fn process_completions(&mut self) -> Vec<CompactionResponse> {
        let compact_awaiter_completion_channel = &mut self.compact_awaiter_completion_channel;
        let mut completed_collections = Vec::new();
        while let Ok(resp) = compact_awaiter_completion_channel.try_recv() {
            self.scheduler.complete_collection(resp.collection_id);
            completed_collections.push(resp);
        }
        completed_collections
    }
}

impl Drop for CompactionManager {
    fn drop(&mut self) {
        self.compact_awaiter.abort();
    }
}

impl CompactionManagerContext {
    #[instrument(name = "CompactionManager::compact", skip(self))]
    async fn compact(
        self,
        collection_id: CollectionUuid,
        rebuild: bool,
    ) -> Result<CompactionResponse, Box<dyn ChromaError>> {
        tracing::info!("Compacting collection: {}", collection_id);
        let dispatcher = match self.dispatcher {
            Some(ref dispatcher) => dispatcher.clone(),
            None => {
                tracing::error!("No dispatcher found");
                return Err(Box::new(CompactionError::FailedToCompact));
            }
        };

        let orchestrator = CompactOrchestrator::new(
            collection_id,
            rebuild,
            self.fetch_log_batch_size,
            self.max_compaction_size,
            self.max_partition_size,
            self.log.clone(),
            self.sysdb.clone(),
            self.blockfile_provider.clone(),
            self.hnsw_index_provider.clone(),
            self.spann_provider.clone(),
            dispatcher,
            None,
        );

        match orchestrator.run(self.system.clone()).await {
            Ok(result) => {
                tracing::info!("Compaction Job completed: {:?}", result);
                return Ok(result);
            }
            Err(e) => {
                tracing::error!("Compaction Job failed: {:?}", e);
                return Err(Box::new(e));
            }
        }
    }
}

#[async_trait]
impl Configurable<(CompactionServiceConfig, System)> for CompactionManager {
    async fn try_from_config(
        config: &(crate::config::CompactionServiceConfig, System),
        registry: &Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        let (config, system) = config;
        let log_config = &config.log;
        let log = match Log::try_from_config(&(log_config.clone(), system.clone()), registry).await
        {
            Ok(log) => log,
            Err(err) => {
                return Err(err);
            }
        };
        let sysdb_config = &config.sysdb;
        let sysdb = match SysDb::try_from_config(sysdb_config, registry).await {
            Ok(sysdb) => sysdb,
            Err(err) => {
                return Err(err);
            }
        };

        let storage = match Storage::try_from_config(&config.storage, registry).await {
            Ok(storage) => storage,
            Err(err) => {
                return Err(err);
            }
        };

        let my_ip = config.my_member_id.clone();
        let policy = Box::new(LasCompactionTimeSchedulerPolicy {});
        let compaction_interval_sec = config.compactor.compaction_interval_sec;
        let max_concurrent_jobs = config.compactor.max_concurrent_jobs;
        let compaction_manager_queue_size = config.compactor.compaction_manager_queue_size;
        let min_compaction_size = config.compactor.min_compaction_size;
        let max_compaction_size = config.compactor.max_compaction_size;
        let max_partition_size = config.compactor.max_partition_size;
        let fetch_log_batch_size = config.compactor.fetch_log_batch_size;
        let purge_dirty_log_timeout_seconds = config.compactor.purge_dirty_log_timeout_seconds;
        let mut disabled_collections =
            HashSet::with_capacity(config.compactor.disabled_collections.len());
        for collection_id_str in &config.compactor.disabled_collections {
            disabled_collections.insert(CollectionUuid(Uuid::from_str(collection_id_str).unwrap()));
        }

        let assignment_policy_config = &config.assignment_policy;
        let assignment_policy =
            Box::<dyn AssignmentPolicy>::try_from_config(assignment_policy_config, registry)
                .await?;
        let job_expiry_seconds = config.compactor.job_expiry_seconds;
        let scheduler = Scheduler::new(
            my_ip,
            log.clone(),
            sysdb.clone(),
            policy,
            max_concurrent_jobs,
            min_compaction_size,
            assignment_policy,
            disabled_collections,
            job_expiry_seconds,
        );

        let blockfile_provider = BlockfileProvider::try_from_config(
            &(config.blockfile_provider.clone(), storage.clone()),
            registry,
        )
        .await?;

        let hnsw_index_provider = HnswIndexProvider::try_from_config(
            &(config.hnsw_provider.clone(), storage.clone()),
            registry,
        )
        .await?;

        let spann_provider = SpannProvider::try_from_config(
            &(
                hnsw_index_provider.clone(),
                blockfile_provider.clone(),
                config.spann_provider.clone(),
            ),
            registry,
        )
        .await?;

        Ok(CompactionManager::new(
            system.clone(),
            scheduler,
            log,
            sysdb,
            storage.clone(),
            blockfile_provider,
            hnsw_index_provider,
            spann_provider,
            compaction_manager_queue_size,
            Duration::from_secs(compaction_interval_sec),
            min_compaction_size,
            max_compaction_size,
            max_partition_size,
            fetch_log_batch_size,
            purge_dirty_log_timeout_seconds,
        ))
    }
}

async fn compact_awaiter_loop(
    mut job_rx: mpsc::Receiver<CompactionTask>,
    completion_tx: mpsc::UnboundedSender<CompactionResponse>,
) {
    let mut futures = FuturesUnordered::new();
    loop {
        select! {
            Some(job) = job_rx.recv() => {
                futures.push(async move {
                    let _ = AssertUnwindSafe(job.future).catch_unwind().await;
                    CompactionResponse {
                        collection_id: job.collection_id,
                    }
                });
            }
            Some(compaction_response) = futures.next() => {
                match completion_tx.send(compaction_response) {
                    Ok(_) => {},
                    Err(_) => {
                        tracing::error!("Failed to send compaction response");
                    }
                }
            }
            else => {
                break;
            }
        }
    }
}

// ============== Component Implementation ==============
#[async_trait]
impl Component for CompactionManager {
    fn get_name() -> &'static str {
        "Compaction manager"
    }

    fn queue_size(&self) -> usize {
        self.context.compaction_manager_queue_size
    }

    async fn on_start(&mut self, ctx: &ComponentContext<Self>) -> () {
        tracing::info!("Starting CompactionManager");
        ctx.scheduler.schedule(
            ScheduledCompactMessage {},
            self.context.compaction_interval,
            ctx,
            || Some(span!(parent: None, tracing::Level::INFO, "Scheduled compaction")),
        );
    }
}

impl Debug for CompactionManager {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompactionManager").finish()
    }
}

// ============== Handlers ==============
#[async_trait]
impl Handler<ScheduledCompactMessage> for CompactionManager {
    type Result = ();

    async fn handle(
        &mut self,
        _message: ScheduledCompactMessage,
        ctx: &ComponentContext<CompactionManager>,
    ) {
        tracing::info!("CompactionManager: Performing scheduled compaction");
        let _ = self.start_compaction_batch().await;
        self.purge_dirty_log(ctx).await;

        // Compactions are kicked off, schedule the next compaction
        ctx.scheduler.schedule(
            ScheduledCompactMessage {},
            self.context.compaction_interval,
            ctx,
            || Some(span!(parent: None, tracing::Level::INFO, "Scheduled compaction")),
        );
    }
}

#[async_trait]
impl Handler<OneOffCompactMessage> for CompactionManager {
    type Result = ();
    async fn handle(
        &mut self,
        message: OneOffCompactMessage,
        _ctx: &ComponentContext<CompactionManager>,
    ) {
        self.scheduler
            .add_oneoff_collections(message.collection_ids);
        tracing::info!(
            "One-off collections queued: {:?}",
            self.scheduler.get_oneoff_collections()
        );
    }
}

#[async_trait]
impl Handler<RebuildMessage> for CompactionManager {
    type Result = ();
    async fn handle(
        &mut self,
        message: RebuildMessage,
        _ctx: &ComponentContext<CompactionManager>,
    ) {
        tracing::info!(
            "Rebuild started for collections: {:?}",
            message.collection_ids
        );
        self.rebuild_batch(&message.collection_ids).await;
        tracing::info!(
            "Rebuild completed for collections: {:?}",
            message.collection_ids
        );
    }
}

#[async_trait]
impl Handler<Memberlist> for CompactionManager {
    type Result = ();

    async fn handle(&mut self, message: Memberlist, _ctx: &ComponentContext<CompactionManager>) {
        self.scheduler.set_memberlist(message);
        if let Some(on_next_memberlist_update) = self.on_next_memberlist_signal.take() {
            if let Err(e) = on_next_memberlist_update.send(()) {
                tracing::error!("Failed to send on_next_memberlist_update: {:?}", e);
            }
        }
    }
}

#[async_trait]
impl Handler<TaskResult<PurgeDirtyLogOutput, PurgeDirtyLogError>> for CompactionManager {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<PurgeDirtyLogOutput, PurgeDirtyLogError>,
        _ctx: &ComponentContext<CompactionManager>,
    ) {
        if let Err(err) = message.into_inner() {
            tracing::error!("Error when purging dirty log: {err}");
        }
    }
}

pub struct RegisterOnReadySignal {
    pub on_ready_tx: oneshot::Sender<()>,
}

impl Debug for RegisterOnReadySignal {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OnReadySubscriber").finish()
    }
}

#[async_trait]
impl Handler<RegisterOnReadySignal> for CompactionManager {
    type Result = ();

    async fn handle(
        &mut self,
        message: RegisterOnReadySignal,
        _ctx: &ComponentContext<CompactionManager>,
    ) {
        if self.scheduler.has_memberlist() {
            if let Some(on_next_memberlist_signal) = self.on_next_memberlist_signal.take() {
                if let Err(e) = on_next_memberlist_signal.send(()) {
                    tracing::error!("Failed to send on_next_memberlist_update: {:?}", e);
                }
            }
        } else {
            self.on_next_memberlist_signal = Some(message.on_ready_tx);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chroma_blockstore::arrow::config::TEST_MAX_BLOCK_SIZE_BYTES;
    use chroma_cache::{new_cache_for_test, new_non_persistent_cache_for_test};
    use chroma_config::assignment::assignment_policy::RendezvousHashingAssignmentPolicy;
    use chroma_index::config::{HnswGarbageCollectionConfig, PlGarbageCollectionConfig};
    use chroma_index::spann::types::{GarbageCollectionContext, SpannMetrics};
    use chroma_log::in_memory_log::{InMemoryLog, InternalLogRecord};
    use chroma_memberlist::memberlist_provider::Member;
    use chroma_storage::local::LocalStorage;
    use chroma_sysdb::TestSysDb;
    use chroma_system::{Dispatcher, DispatcherConfig};
    use chroma_types::SegmentUuid;
    use chroma_types::{Collection, LogRecord, Operation, OperationRecord, Segment};
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};
    use tokio::fs;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_compaction_manager() {
        let mut log = Log::InMemory(InMemoryLog::new());
        let in_memory_log = match log {
            Log::InMemory(ref mut log) => log,
            _ => panic!("Expected InMemoryLog"),
        };
        let tmpdir = tempfile::tempdir().unwrap();
        // Clear temp dir.
        tokio::fs::remove_dir_all(tmpdir.path())
            .await
            .expect("Failed to remove temp dir");
        let storage = Storage::Local(LocalStorage::new(tmpdir.path().to_str().unwrap()));

        let tenant_1 = "tenant_1".to_string();
        let collection_1 = Collection {
            name: "collection_1".to_string(),
            dimension: Some(1),
            tenant: tenant_1.clone(),
            database: "database_1".to_string(),
            log_position: -1,
            ..Default::default()
        };

        let collection_uuid_1 = collection_1.collection_id;

        in_memory_log.add_log(
            collection_uuid_1,
            InternalLogRecord {
                collection_id: collection_uuid_1,
                log_offset: 0,
                log_ts: 1,
                record: LogRecord {
                    log_offset: 0,
                    record: OperationRecord {
                        id: "embedding_id_1".to_string(),
                        embedding: Some(vec![1.0, 2.0, 3.0]),
                        encoding: None,
                        metadata: None,
                        document: None,
                        operation: Operation::Add,
                    },
                },
            },
        );

        let tenant_2 = "tenant_2".to_string();
        let collection_2 = Collection {
            name: "collection_2".to_string(),
            dimension: Some(1),
            tenant: tenant_2.clone(),
            database: "database_2".to_string(),
            log_position: -1,
            ..Default::default()
        };

        let collection_uuid_2 = collection_2.collection_id;
        in_memory_log.add_log(
            collection_uuid_2,
            InternalLogRecord {
                collection_id: collection_uuid_2,
                log_offset: 0,
                log_ts: 2,
                record: LogRecord {
                    log_offset: 0,
                    record: OperationRecord {
                        id: "embedding_id_2".to_string(),
                        embedding: Some(vec![4.0, 5.0, 6.0]),
                        encoding: None,
                        metadata: None,
                        document: None,
                        operation: Operation::Add,
                    },
                },
            },
        );

        let mut sysdb = SysDb::Test(TestSysDb::new());
        match sysdb {
            SysDb::Test(ref mut sysdb) => {
                sysdb.add_collection(collection_1);
                sysdb.add_collection(collection_2);
            }
            _ => panic!("Invalid sysdb type"),
        }

        let collection_1_record_segment = Segment {
            id: SegmentUuid::new(),
            r#type: chroma_types::SegmentType::BlockfileRecord,
            scope: chroma_types::SegmentScope::RECORD,
            collection: collection_uuid_1,
            metadata: None,
            file_path: HashMap::new(),
        };

        let collection_2_record_segment = Segment {
            id: SegmentUuid::new(),
            r#type: chroma_types::SegmentType::BlockfileRecord,
            scope: chroma_types::SegmentScope::RECORD,
            collection: collection_uuid_2,
            metadata: None,
            file_path: HashMap::new(),
        };

        let collection_1_hnsw_segment = Segment {
            id: SegmentUuid::new(),
            r#type: chroma_types::SegmentType::HnswDistributed,
            scope: chroma_types::SegmentScope::VECTOR,
            collection: collection_uuid_1,
            metadata: None,
            file_path: HashMap::new(),
        };

        let collection_2_hnsw_segment = Segment {
            id: SegmentUuid::new(),
            r#type: chroma_types::SegmentType::HnswDistributed,
            scope: chroma_types::SegmentScope::VECTOR,
            collection: collection_uuid_2,
            metadata: None,
            file_path: HashMap::new(),
        };

        let collection_1_metadata_segment = Segment {
            id: SegmentUuid::new(),
            r#type: chroma_types::SegmentType::BlockfileMetadata,
            scope: chroma_types::SegmentScope::METADATA,
            collection: collection_uuid_1,
            metadata: None,
            file_path: HashMap::new(),
        };

        let collection_2_metadata_segment = Segment {
            id: SegmentUuid::new(),
            r#type: chroma_types::SegmentType::BlockfileMetadata,
            scope: chroma_types::SegmentScope::METADATA,
            collection: collection_uuid_2,
            metadata: None,
            file_path: HashMap::new(),
        };

        match sysdb {
            SysDb::Test(ref mut sysdb) => {
                sysdb.add_segment(collection_1_record_segment);
                sysdb.add_segment(collection_2_record_segment);
                sysdb.add_segment(collection_1_hnsw_segment);
                sysdb.add_segment(collection_2_hnsw_segment);
                sysdb.add_segment(collection_1_metadata_segment);
                sysdb.add_segment(collection_2_metadata_segment);
                let last_compaction_time_1 = 2;
                sysdb.add_tenant_last_compaction_time(tenant_1, last_compaction_time_1);
                let last_compaction_time_2 = 1;
                sysdb.add_tenant_last_compaction_time(tenant_2, last_compaction_time_2);
            }
            _ => panic!("Invalid sysdb type"),
        }

        let my_member = Member {
            member_id: "member_1".to_string(),
            member_ip: "10.0.0.1".to_string(),
            member_node_name: "node_1".to_string(),
        };
        let compaction_manager_queue_size = 1000;
        let max_concurrent_jobs = 10;
        let compaction_interval = Duration::from_secs(1);
        let min_compaction_size = 0;
        let max_compaction_size = 1000;
        let max_partition_size = 1000;
        let fetch_log_batch_size = 100;
        let purge_dirty_log_timeout_seconds = 60;
        let job_expiry_seconds = 3600;

        // Set assignment policy
        let mut assignment_policy = Box::new(RendezvousHashingAssignmentPolicy::default());
        assignment_policy.set_members(vec![my_member.member_id.clone()]);

        let mut scheduler = Scheduler::new(
            my_member.member_id.clone(),
            log.clone(),
            sysdb.clone(),
            Box::new(LasCompactionTimeSchedulerPolicy {}),
            max_concurrent_jobs,
            min_compaction_size,
            assignment_policy,
            HashSet::new(),
            job_expiry_seconds,
        );
        // Set memberlist
        scheduler.set_memberlist(vec![my_member.clone()]);

        let block_cache = new_cache_for_test();
        let sparse_index_cache = new_cache_for_test();
        let hnsw_cache = new_non_persistent_cache_for_test();
        let gc_context = GarbageCollectionContext::try_from_config(
            &(
                PlGarbageCollectionConfig::default(),
                HnswGarbageCollectionConfig::default(),
            ),
            &Registry::default(),
        )
        .await
        .expect("Error converting config to gc context");
        let blockfile_provider = BlockfileProvider::new_arrow(
            storage.clone(),
            TEST_MAX_BLOCK_SIZE_BYTES,
            block_cache,
            sparse_index_cache,
        );
        let hnsw_provider = HnswIndexProvider::new(
            storage.clone(),
            PathBuf::from(tmpdir.path().to_str().unwrap()),
            hnsw_cache,
            16,
        );
        let spann_provider = SpannProvider {
            hnsw_provider: hnsw_provider.clone(),
            blockfile_provider: blockfile_provider.clone(),
            garbage_collection_context: Some(gc_context),
            metrics: SpannMetrics::default(),
            pl_block_size: Some(5 * 1024 * 1024),
        };
        let system = System::new();
        let mut manager = CompactionManager::new(
            system.clone(),
            scheduler,
            log,
            sysdb,
            storage.clone(),
            blockfile_provider,
            hnsw_provider,
            spann_provider,
            compaction_manager_queue_size,
            compaction_interval,
            min_compaction_size,
            max_compaction_size,
            max_partition_size,
            fetch_log_batch_size,
            purge_dirty_log_timeout_seconds,
        );

        let dispatcher = Dispatcher::new(DispatcherConfig {
            num_worker_threads: 10,
            task_queue_limit: 100,
            dispatcher_queue_size: 100,
            worker_queue_size: 100,
            active_io_tasks: 100,
        });
        let dispatcher_handle = system.start_component(dispatcher);
        manager.set_dispatcher(dispatcher_handle);
        manager.start_compaction_batch().await;

        let start = std::time::Instant::now();
        let timeout = std::time::Duration::from_secs(10);
        let expected_compactions = HashSet::from([collection_uuid_1, collection_uuid_2]);

        let mut completed_compactions = HashSet::new();

        while completed_compactions.len() < expected_compactions.len() && start.elapsed() < timeout
        {
            let completed = manager.process_completions();
            completed_compactions.extend(
                completed
                    .iter()
                    .map(|c| c.collection_id)
                    .collect::<Vec<CollectionUuid>>(),
            );
        }

        assert_eq!(completed_compactions, expected_compactions);

        check_purge_successful(tmpdir.path()).await;
    }

    pub async fn check_purge_successful(path: impl AsRef<Path>) {
        let mut entries = fs::read_dir(&path).await.expect("Failed to read dir");

        while let Some(entry) = entries.next_entry().await.expect("Failed to read next dir") {
            let path = entry.path();
            let metadata = entry.metadata().await.expect("Failed to read metadata");

            if metadata.is_dir() {
                assert!(path.ends_with("tenant"));
            } else {
                panic!("Expected hnsw purge to be successful")
            }
        }
    }
}
