use std::collections::HashSet;

use crate::operators::truncate_dirty_log::{
    TruncateDirtyLogError, TruncateDirtyLogOperator, TruncateDirtyLogOutput,
};
use crate::types::CleanupMode;
use crate::{config::GarbageCollectorConfig, types::GarbageCollectorResponse};
use async_trait::async_trait;
use chroma_blockstore::RootManager;
use chroma_config::{
    assignment::assignment_policy::AssignmentPolicy, registry::Registry, Configurable,
};
use chroma_error::ChromaError;
use chroma_log::Log;
use chroma_memberlist::memberlist_provider::Memberlist;
use chroma_storage::Storage;
use chroma_sysdb::{CollectionToGcInfo, GetCollectionsToGcError, SysDb, SysDbConfig};
use chroma_system::{
    wrap, Component, ComponentContext, ComponentHandle, Dispatcher, Handler, Orchestrator, System,
    TaskResult,
};
use chroma_types::CollectionUuid;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use opentelemetry::metrics::{Counter, Histogram};
use opentelemetry::trace::TraceContextExt;
use parking_lot::Mutex;
use s3heap_service::SysDbScheduler;
use std::{
    fmt::{Debug, Formatter},
    sync::Arc,
    time::{Duration, SystemTime},
};
use thiserror::Error;
use tracing::{span, Instrument, Level, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;

#[allow(dead_code)]
pub(crate) struct GarbageCollector {
    config: GarbageCollectorConfig,
    sysdb_client: SysDb,
    storage: Storage,
    logs: Log,
    dispatcher: Option<ComponentHandle<Dispatcher>>,
    system: Option<System>,
    assignment_policy: Box<dyn AssignmentPolicy>,
    memberlist: Memberlist,
    root_manager: RootManager,
    total_jobs_metric: Counter<u64>,
    job_duration_ms_metric: Histogram<u64>,
    total_files_deleted_metric: Counter<u64>,
    total_versions_deleted_metric: Counter<u64>,
    manual_collections: Mutex<HashSet<CollectionUuid>>,
}

impl Debug for GarbageCollector {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GarbageCollector").finish()
    }
}

#[derive(Debug, Error)]
enum GarbageCollectCollectionError {
    #[error("Uninitialized: missing dispatcher or system")]
    Uninitialized,
    #[error("Failed to run garbage collection orchestrator: {0}")]
    OrchestratorV2Error(#[from] crate::garbage_collector_orchestrator_v2::GarbageCollectorError),
}

#[allow(clippy::too_many_arguments)]
impl GarbageCollector {
    pub fn new(
        config: GarbageCollectorConfig,
        sysdb_client: SysDb,
        storage: Storage,
        logs: Log,
        assignment_policy: Box<dyn AssignmentPolicy>,
        root_manager: RootManager,
    ) -> Self {
        let meter = opentelemetry::global::meter("chroma");

        Self {
            config,
            sysdb_client,
            storage,
            logs,
            dispatcher: None,
            system: None,
            assignment_policy,
            memberlist: Memberlist::default(),
            root_manager,
            total_jobs_metric: meter
                .u64_counter("garbage_collector.total_jobs")
                .with_description("Total number of garbage collection jobs executed")
                .build(),
            job_duration_ms_metric: meter
                .u64_histogram("garbage_collector.job_duration_ms")
                .with_description("Duration of garbage collection jobs in milliseconds")
                .with_unit("ms")
                .build(),
            total_files_deleted_metric: meter
                .u64_counter("garbage_collector.total_files_deleted")
                .with_description("Total number of files deleted during garbage collection")
                .build(),
            total_versions_deleted_metric: meter
                .u64_counter("garbage_collector.total_versions_deleted")
                .with_description("Total number of versions deleted during garbage collection")
                .build(),
            manual_collections: Mutex::new(HashSet::default()),
        }
    }

    pub(crate) fn set_dispatcher(&mut self, dispatcher: ComponentHandle<Dispatcher>) {
        self.dispatcher = Some(dispatcher);
    }

    pub(crate) fn set_system(&mut self, system: chroma_system::System) {
        self.system = Some(system);
    }

    async fn garbage_collect_hard_delete_log(
        &self,
        collection_id: CollectionUuid,
    ) -> Result<GarbageCollectorResponse, GarbageCollectCollectionError> {
        let dispatcher = self
            .dispatcher
            .as_ref()
            .ok_or(GarbageCollectCollectionError::Uninitialized)?;
        let system = self
            .system
            .as_ref()
            .ok_or(GarbageCollectCollectionError::Uninitialized)?;

        let orchestrator =
            crate::log_only_orchestrator::HardDeleteLogOnlyGarbageCollectorOrchestrator::new(
                dispatcher.clone(),
                self.storage.clone(),
                self.logs.clone(),
                collection_id,
            );

        let result = match orchestrator.run(system.clone()).await {
            Ok(res) => res,
            Err(e) => {
                tracing::error!("Failed to run garbage collection orchestrator v2: {:?}", e);
                return Err(GarbageCollectCollectionError::OrchestratorV2Error(e));
            }
        };
        Ok(result)
    }

    async fn prune_heap_across_shards(&self, cutoff_time: chrono::DateTime<chrono::Utc>) {
        tracing::info!(
            "Pruning completed tasks from all heap shards (buckets_to_read={}, max_items={})",
            self.config.heap_prune_buckets_to_read,
            self.config.heap_prune_max_items
        );

        let prune_limits = s3heap::Limits::default()
            .with_buckets(self.config.heap_prune_buckets_to_read as usize)
            .with_items(self.config.heap_prune_max_items as usize)
            .with_time_cut_off(cutoff_time);

        let mut total_stats = s3heap::PruneStats::default();
        let mut service_index = 0;

        // Create scheduler for checking task completion status
        let scheduler: Arc<dyn s3heap::HeapScheduler> =
            Arc::new(SysDbScheduler::new(self.sysdb_client.clone()));

        // Iterate over all log service shards (rust-log-service-0, rust-log-service-1, ...)
        // Limit to 100 shards to prevent infinite loop if we get unexpected errors
        const MAX_SHARDS: u32 = 100;
        loop {
            if service_index >= MAX_SHARDS {
                tracing::warn!(
                    "Reached max shard limit of {} during heap pruning",
                    MAX_SHARDS
                );
                break;
            }
            let heap_prefix =
                s3heap::heap_path_from_hostname(&format!("rust-log-service-{}", service_index));

            let pruner = match s3heap::HeapPruner::new(
                self.storage.clone(),
                heap_prefix.clone(),
                Arc::clone(&scheduler),
            ) {
                Ok(pruner) => pruner,
                Err(s3heap::Error::UninitializedHeap(_)) => {
                    tracing::debug!(
                        "No heap found at shard {} - stopping iteration",
                        service_index
                    );
                    break;
                }
                Err(e) => {
                    tracing::warn!(
                        "Error creating heap pruner for shard {}: {:?} - continuing",
                        service_index,
                        e
                    );
                    service_index += 1;
                    continue;
                }
            };

            match pruner.prune(prune_limits.clone()).await {
                Ok(stats) => {
                    tracing::debug!(
                        "Pruned shard {}: {} items pruned, {} buckets deleted",
                        service_index,
                        stats.items_pruned,
                        stats.buckets_deleted
                    );
                    total_stats.merge(&stats);
                }
                Err(e) => {
                    tracing::error!("Failed to prune heap shard {}: {}", service_index, e);
                }
            }

            service_index += 1;
        }

        tracing::info!(
            "Heap pruning complete: {} items pruned, {} items retained, {} buckets deleted, {} buckets updated across {} shards",
            total_stats.items_pruned,
            total_stats.items_retained,
            total_stats.buckets_deleted,
            total_stats.buckets_updated,
            service_index
        );
    }

    async fn garbage_collect_attached_functions(
        &mut self,
        attached_function_soft_delete_absolute_cutoff_time: SystemTime,
    ) -> (u32, u32) {
        tracing::info!("Checking for soft-deleted attached functions to hard delete");
        match self
            .sysdb_client
            .get_soft_deleted_attached_functions(
                attached_function_soft_delete_absolute_cutoff_time,
                self.config.max_attached_functions_to_gc_per_run,
            )
            .await
        {
            Ok(attached_functions_to_delete) => {
                if !attached_functions_to_delete.is_empty() {
                    tracing::info!(
                        "Found {} soft-deleted attached functions to hard delete",
                        attached_functions_to_delete.len()
                    );

                    let deletion_jobs = attached_functions_to_delete.into_iter().map(|attached_function_id| {
                        tracing::info!(
                            "Hard deleting attached function: {}",
                            attached_function_id
                        );

                        let instrumented_span = span!(parent: None, tracing::Level::INFO, "Hard delete attached function", attached_function_id = %attached_function_id);
                        Span::current().add_link(instrumented_span.context().span().span_context().clone());

                        let mut sysdb = self.sysdb_client.clone();
                        Box::pin(async move {
                            // tanujnay112: Could batch this but just following the pattern of collection deletion below.
                            sysdb.finish_attached_function_deletion(attached_function_id).await
                                .map(|_| attached_function_id)
                        }.instrument(instrumented_span)) as std::pin::Pin<Box<dyn std::future::Future<Output = Result<chroma_types::AttachedFunctionUuid, chroma_sysdb::FinishAttachedFunctionDeletionError>> + Send + '_>>
                    });

                    let mut deletion_stream =
                        futures::stream::iter(deletion_jobs).buffer_unordered(10);

                    let mut num_deleted = 0;
                    let mut num_failed = 0;
                    while let Some(result) = deletion_stream.next().await {
                        match result {
                            Ok(attached_function_id) => {
                                tracing::info!(
                                    "Successfully hard deleted attached function: {}",
                                    attached_function_id
                                );
                                num_deleted += 1;
                            }
                            Err(e) => {
                                tracing::error!("Failed to hard delete attached function: {}", e);
                                num_failed += 1;
                            }
                        }
                    }

                    tracing::info!(
                        "Attached function deletion completed: {} deleted, {} failed",
                        num_deleted,
                        num_failed
                    );
                    (num_deleted, num_failed)
                } else {
                    tracing::debug!("No soft-deleted attached functions found to hard delete");
                    (0, 0)
                }
            }
            Err(e) => {
                tracing::error!("Failed to get soft-deleted attached functions: {}", e);
                (0, 0)
            }
        }
    }

    async fn garbage_collect_collection(
        &self,
        version_absolute_cutoff_time: DateTime<Utc>,
        collection_soft_delete_absolute_cutoff_time: DateTime<Utc>,
        collection: CollectionToGcInfo,
        cleanup_mode: CleanupMode,
        enable_dangerous_option_to_ignore_min_versions_for_wal3: bool,
    ) -> Result<GarbageCollectorResponse, GarbageCollectCollectionError> {
        let dispatcher = self
            .dispatcher
            .as_ref()
            .ok_or(GarbageCollectCollectionError::Uninitialized)?;
        let system = self
            .system
            .as_ref()
            .ok_or(GarbageCollectCollectionError::Uninitialized)?;

        let enable_log_gc = collection.tenant <= self.config.enable_log_gc_for_tenant_threshold
            || self
                .config
                .enable_log_gc_for_tenant
                .contains(&collection.tenant);

        let orchestrator =
            crate::garbage_collector_orchestrator_v2::GarbageCollectorOrchestrator::new(
                collection.id,
                collection.version_file_path,
                collection.lineage_file_path,
                version_absolute_cutoff_time,
                collection_soft_delete_absolute_cutoff_time,
                self.sysdb_client.clone(),
                dispatcher.clone(),
                system.clone(),
                self.storage.clone(),
                self.logs.clone(),
                self.root_manager.clone(),
                cleanup_mode,
                self.config.min_versions_to_keep,
                enable_log_gc,
                enable_dangerous_option_to_ignore_min_versions_for_wal3,
                self.config
                    .max_concurrent_list_files_operations_per_collection,
            );

        let started_at = SystemTime::now();
        let result = match orchestrator.run(system.clone()).await {
            Ok(res) => res,
            Err(e) => {
                tracing::error!("Failed to run garbage collection orchestrator v2: {:?}", e);
                return Err(GarbageCollectCollectionError::OrchestratorV2Error(e));
            }
        };
        let duration_ms = started_at
            .elapsed()
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        self.job_duration_ms_metric.record(duration_ms, &[]);
        self.total_files_deleted_metric.add(
            result.num_files_deleted as u64,
            &[opentelemetry::KeyValue::new(
                "cleanup_mode",
                format!("{:?}", cleanup_mode),
            )],
        );
        self.total_versions_deleted_metric.add(
            result.num_versions_deleted as u64,
            &[opentelemetry::KeyValue::new(
                "cleanup_mode",
                format!("{:?}", cleanup_mode),
            )],
        );

        Ok(result)
    }

    async fn truncate_dirty_log(&self, ctx: &ComponentContext<Self>) {
        let Some(mut dispatcher) = self.dispatcher.as_ref().cloned() else {
            tracing::error!("Uninitialized dispatcher for garbage collector");
            return;
        };
        let truncate_dirty_log_task = wrap(
            Box::new(TruncateDirtyLogOperator {
                storage: self.storage.clone(),
                logs: self.logs.clone(),
            }),
            (),
            ctx.receiver(),
            ctx.cancellation_token.clone(),
        );

        if let Err(err) = dispatcher
            .send(truncate_dirty_log_task, Some(Span::current()))
            .await
        {
            tracing::error!("Unable to dispatch truncate dirty log task: {err}");
        }
    }
}

#[async_trait]
impl Component for GarbageCollector {
    fn get_name() -> &'static str {
        "GarbageCollector"
    }

    fn queue_size(&self) -> usize {
        1000
    }

    async fn on_start(&mut self, ctx: &ComponentContext<Self>) {
        ctx.scheduler.schedule(
            GarbageCollectMessage { tenant: None },
            Duration::from_secs((self.config.gc_interval_mins * 60) as u64),
            ctx,
            || Some(span!(parent: None, tracing::Level::INFO, "Scheduled garbage collection")),
        );
    }

    fn on_stop_timeout(&self) -> Duration {
        // NOTE: Increased timeout for remaining jobs to finish
        Duration::from_secs(60)
    }
}

impl GarbageCollector {
    fn filter_collections(
        &mut self,
        collections: Vec<CollectionToGcInfo>,
    ) -> Vec<CollectionToGcInfo> {
        self.assignment_policy.set_members(
            self.memberlist
                .iter()
                .map(|member| member.member_id.clone())
                .collect(),
        );

        collections
            .into_iter()
            .filter(|collection| {
                // Filter out disabled collections
                if self.config.disallow_collections.contains(&collection.id) {
                    tracing::warn!(
                        "Skipping garbage collection for disabled collection: {}",
                        collection.id
                    );
                    return false;
                }

                // Only include collections assigned to this member
                match self
                    .assignment_policy
                    .assign_one(&collection.id.0.to_string())
                {
                    Ok(member) => member == self.config.my_member_id,
                    Err(err) => {
                        tracing::error!("Failed to assign collection {}: {}", collection.id, err);
                        false
                    }
                }
            })
            .collect()
    }

    fn manual_garbage_collection_request(
        &self,
        collection_id: CollectionUuid,
    ) -> Result<(), GarbageCollectCollectionError> {
        tracing::event!(Level::INFO, name = "manual garbage collection", collection_id =? collection_id);
        let mut manual_collections = self.manual_collections.lock();
        manual_collections.insert(collection_id);
        Ok(())
    }
}

#[async_trait]
impl Handler<Memberlist> for GarbageCollector {
    type Result = ();

    async fn handle(&mut self, message: Memberlist, _ctx: &ComponentContext<GarbageCollector>) {
        self.memberlist = message;
    }
}

#[derive(Debug)]
pub struct ManualGarbageCollectionRequest {
    pub collection_id: CollectionUuid,
}

#[async_trait]
impl Handler<ManualGarbageCollectionRequest> for GarbageCollector {
    type Result = ();

    async fn handle(
        &mut self,
        req: ManualGarbageCollectionRequest,
        _: &ComponentContext<GarbageCollector>,
    ) {
        if let Err(err) = self.manual_garbage_collection_request(req.collection_id) {
            tracing::event!(Level::ERROR, name = "manual collection failed", error =? err);
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
#[allow(dead_code)]
struct GarbageCollectResult {
    num_completed_jobs: u32,
    num_failed_jobs: u32,
    num_hard_deleted_databases: u32,
}

#[derive(Debug)]
struct GarbageCollectMessage {
    tenant: Option<String>,
}

#[async_trait]
impl Handler<GarbageCollectMessage> for GarbageCollector {
    type Result = GarbageCollectResult;

    async fn handle(
        &mut self,
        message: GarbageCollectMessage,
        ctx: &ComponentContext<Self>,
    ) -> Self::Result {
        let now = SystemTime::now();

        let version_absolute_cutoff_time =
            DateTime::<Utc>::from(now - self.config.version_cutoff_time);
        tracing::debug!(
            "Using absolute cutoff time: {} for versions (relative cutoff time: {:?})",
            version_absolute_cutoff_time,
            self.config.version_cutoff_time
        );

        let collection_soft_delete_absolute_cutoff_time =
            DateTime::<Utc>::from(now - self.config.collection_soft_delete_grace_period);
        tracing::debug!(
            "Using absolute cutoff time: {} for soft deleted collections (grace period: {:?})",
            collection_soft_delete_absolute_cutoff_time,
            self.config.collection_soft_delete_grace_period
        );

        let attached_function_soft_delete_absolute_cutoff_time =
            now - self.config.attached_function_soft_delete_grace_period;
        tracing::debug!(
            "Using absolute cutoff time: {:?} for soft deleted attached functions (grace period: {:?})",
            attached_function_soft_delete_absolute_cutoff_time,
            self.config.attached_function_soft_delete_grace_period
        );

        // Garbage collect soft-deleted attached functions that are past the grace period
        let (num_attached_functions_deleted, num_attached_functions_failed) = self
            .garbage_collect_attached_functions(attached_function_soft_delete_absolute_cutoff_time)
            .await;
        tracing::debug!(
            "Garbage collected {} soft-deleted attached functions, {} failed",
            num_attached_functions_deleted,
            num_attached_functions_failed
        );

        // Prune heap of completed tasks across all log service shards
        let cutoff_time = chrono::DateTime::<chrono::Utc>::from(
            attached_function_soft_delete_absolute_cutoff_time,
        );
        self.prune_heap_across_shards(cutoff_time).await;

        // Get all collections to gc and create gc orchestrator for each.
        tracing::info!("Getting collections to gc");
        let collections_to_gc = self
            .sysdb_client
            .get_collections_to_gc(
                Some(version_absolute_cutoff_time.into()),
                Some(
                    self.config
                        .max_collections_to_fetch
                        .unwrap_or(self.config.max_collections_to_gc)
                        .into(),
                ),
                message.tenant.clone(),
                self.config.filter_min_versions_if_alive,
            )
            .await
            .expect("Failed to get collections to gc");
        tracing::info!("Got {} total collections", collections_to_gc.len());
        let mut collections_to_gc = self.filter_collections(collections_to_gc);

        // Append to collections_to_gc any manual collections iff they aren't already in there.
        let mut manual = vec![];
        {
            let mut manual_collections = self.manual_collections.lock();
            // NOTE(rescrv):  We do this dance so that we can remove the collection here so it
            // isn't enqueued endlessly and so that it won't be thrown away immediately down below.
            while collections_to_gc.len() + manual.len()
                < self.config.max_collections_to_gc as usize
            {
                let popped = manual_collections.iter().next().cloned();
                if let Some(c) = popped {
                    manual.push(c);
                    manual_collections.remove(&c);
                } else {
                    break;
                }
            }
        }
        let mut collections_to_hard_delete_log = vec![];
        for collection_id in manual {
            if collections_to_gc.iter().any(|c| c.id == collection_id) {
                continue;
            }
            match self.sysdb_client.get_collection_to_gc(collection_id).await {
                Ok(collection_info) => {
                    tracing::event!(
                        Level::INFO,
                        name = "manually collecting",
                        collection_id = collection_id.to_string()
                    );
                    collections_to_gc.push(collection_info);
                }
                Err(GetCollectionsToGcError::NoSuchCollection) => {
                    collections_to_hard_delete_log.push(collection_id);
                }
                Err(err) => {
                    tracing::event!(
                        Level::ERROR,
                        name = "cannot perform manual collection",
                        collection_id = collection_id.to_string(),
                        error = err.to_string(),
                    );
                }
            }
        }

        let collections_to_gc = collections_to_gc
            .into_iter()
            .map(|collection| {
                let cleanup_mode =
                    if let Some(tenant_mode_overrides) = &self.config.tenant_mode_overrides {
                        tenant_mode_overrides
                            .get(&collection.tenant)
                            .cloned()
                            .unwrap_or(self.config.default_mode)
                    } else {
                        self.config.default_mode
                    };

                (cleanup_mode.to_owned(), collection)
            })
            .take(self.config.max_collections_to_gc as usize)
            .collect::<Vec<_>>();

        tracing::info!(
            "Filtered to {} collections to garbage collect",
            collections_to_gc.len()
        );

        let mut sysdb = self.sysdb_client.clone();

        let jobs_iter1 = collections_to_gc.into_iter()
            .map(|(cleanup_mode, collection)| {
                tracing::info!(
                    "Processing collection: {} (tenant: {}, version_file_path: {})",
                    collection.id,
                    collection.tenant,
                    collection.version_file_path
                );


                let instrumented_span = span!(parent: None, tracing::Level::INFO, "Garbage collection job", collection_id = ?collection.id, tenant_id = %collection.tenant, cleanup_mode = ?cleanup_mode);
                Span::current().add_link(instrumented_span.context().span().span_context().clone());

                Box::pin(self.garbage_collect_collection(
                    version_absolute_cutoff_time,
                    collection_soft_delete_absolute_cutoff_time,
                    collection,
                    cleanup_mode,
                    self.config
                        .enable_dangerous_option_to_ignore_min_versions_for_wal3,
                )
                .instrument(instrumented_span)) as std::pin::Pin<Box<dyn std::future::Future<Output = Result<GarbageCollectorResponse, GarbageCollectCollectionError>> + Send + '_>>
            });
        let jobs_iter2 = collections_to_hard_delete_log.into_iter().map(|collection_id| {
                tracing::event!(Level::INFO, "hard delete log-only");
                let instrumented_span = span!(parent: None, tracing::Level::INFO, "Garbage collection job (hard delete log)", collection_id =? collection_id);
                Span::current().add_link(instrumented_span.context().span().span_context().clone());
                Box::pin(self.garbage_collect_hard_delete_log(collection_id).instrument(instrumented_span)) as std::pin::Pin<Box<dyn std::future::Future<Output = Result<GarbageCollectorResponse, GarbageCollectCollectionError>> + Send + '_>>
        });
        let mut jobs_stream1 = futures::stream::iter(jobs_iter1).buffer_unordered(100);
        let mut jobs_stream2 = futures::stream::iter(jobs_iter2).buffer_unordered(100);

        let mut num_completed_jobs = 0;
        let mut num_failed_jobs = 0;
        while let Some(job_result) = jobs_stream1.next().await {
            match job_result {
                Ok(result) => {
                    {
                        let mut manual_collections = self.manual_collections.lock();
                        manual_collections.remove(&result.collection_id);
                    }
                    tracing::info!("Garbage collection completed. Deleted {} files over {} versions for collection {}.", result.num_files_deleted, result.num_versions_deleted, result.collection_id);
                    num_completed_jobs += 1;
                }
                Err(e) => {
                    tracing::error!("Garbage collection failed: {:?}", e);
                    num_failed_jobs += 1;
                }
            }
        }
        // NOTE(rescrv):  I'm not proud of this duplication, but I cannot coerce the
        // futures::stream::iter above to take a chain of two different futures.  It just won't
        // compile.
        while let Some(job_result) = jobs_stream2.next().await {
            match job_result {
                Ok(result) => {
                    {
                        let mut manual_collections = self.manual_collections.lock();
                        manual_collections.remove(&result.collection_id);
                    }
                    tracing::info!("Garbage collection hard delete completed. Deleted all log files collection {}.", result.collection_id);
                    num_completed_jobs += 1;
                }
                Err(e) => {
                    tracing::error!("Garbage collection failed: {:?}", e);
                    num_failed_jobs += 1;
                }
            }
        }
        tracing::info!(
            "Completed {} jobs, failed {} jobs",
            num_completed_jobs,
            num_failed_jobs
        );

        self.total_jobs_metric.add(
            num_completed_jobs as u64,
            &[opentelemetry::KeyValue::new("status", "success")],
        );
        self.total_jobs_metric.add(
            num_failed_jobs as u64,
            &[opentelemetry::KeyValue::new("status", "failure")],
        );

        let num_hard_deleted_databases = match sysdb
            .finish_database_deletion(version_absolute_cutoff_time.into())
            .await
        {
            Ok(num_deleted) => {
                tracing::debug!("Hard deleted {} databases", num_deleted);
                num_deleted
            }
            Err(err) => {
                tracing::error!("Call to FinishDatabaseDeletion failed: {:?}", err);
                0
            }
        };

        tracing::info!("Garbage collecting dirty log");
        self.truncate_dirty_log(ctx).await;

        // Schedule next run
        ctx.scheduler.schedule(
            GarbageCollectMessage {
                tenant: message.tenant.clone(),
            },
            Duration::from_secs((self.config.gc_interval_mins * 60) as u64),
            ctx,
            || Some(span!(parent: None, tracing::Level::INFO, "Scheduled garbage collection")),
        );

        return GarbageCollectResult {
            num_completed_jobs,
            num_failed_jobs,
            num_hard_deleted_databases: num_hard_deleted_databases as u32,
        };
    }
}

#[async_trait]
impl Handler<TaskResult<TruncateDirtyLogOutput, TruncateDirtyLogError>> for GarbageCollector {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<TruncateDirtyLogOutput, TruncateDirtyLogError>,
        _ctx: &ComponentContext<Self>,
    ) {
        if let Err(err) = message.into_inner() {
            tracing::error!("Failed to truncate dirty log: {err}");
        }
    }
}

#[async_trait]
impl Configurable<(GarbageCollectorConfig, System)> for GarbageCollector {
    async fn try_from_config(
        (config, system): &(GarbageCollectorConfig, System),
        registry: &Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        let sysdb_config = SysDbConfig::Grpc(config.sysdb_config.clone());
        let sysdb_client = SysDb::try_from_config(&sysdb_config, registry).await?;
        let storage = Storage::try_from_config(&config.storage_config, registry).await?;

        let assignment_policy =
            Box::<dyn AssignmentPolicy>::try_from_config(&config.assignment_policy, registry)
                .await?;

        let logs = Log::try_from_config(&(config.log.clone(), system.clone()), registry).await?;

        let root_manager_cache =
            chroma_cache::from_config_persistent(&config.root_cache_config).await?;
        let root_manager = RootManager::new(storage.clone(), root_manager_cache);

        Ok(GarbageCollector::new(
            config.clone(),
            sysdb_client,
            storage,
            logs,
            assignment_policy,
            root_manager,
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{HashMap, HashSet},
        str::FromStr,
    };

    use super::*;
    use crate::helper::ChromaGrpcClients;
    use chroma_log::config::{GrpcLogConfig, LogConfig};
    use chroma_memberlist::memberlist_provider::Member;
    use chroma_storage::s3_config_for_localhost_with_bucket_name;
    use chroma_sysdb::{GetCollectionsOptions, GrpcSysDb, GrpcSysDbConfig};
    use chroma_system::{DispatcherConfig, System};
    use chroma_tracing::{OtelFilter, OtelFilterLevel};
    use chroma_types::CollectionUuid;
    use tracing_test::traced_test;
    use uuid::Uuid;

    async fn wait_for_new_version(
        clients: &mut ChromaGrpcClients,
        collection_id: String,
        tenant_id: String,
        current_version_count: usize,
        max_attempts: usize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        for attempt in 1..=max_attempts {
            tracing::info!(
                attempt,
                max_attempts,
                collection_id,
                "Waiting for new version to be created..."
            );

            tokio::time::sleep(Duration::from_secs(2)).await;

            let versions = clients
                .list_collection_versions(
                    collection_id.clone(),
                    tenant_id.clone(),
                    Some(100),
                    None,
                    None,
                    None,
                )
                .await?;

            if versions.versions.len() > current_version_count {
                tracing::info!(
                    previous_count = current_version_count,
                    new_count = versions.versions.len(),
                    "New version detected"
                );
                return Ok(());
            }
        }

        Err("Timeout waiting for new version to be created".into())
    }

    const TEST_COLLECTIONS_SIZE: usize = 33;

    async fn create_test_collection(
        tenant_id: String,
        clients: &mut ChromaGrpcClients,
    ) -> (CollectionUuid, String) {
        let test_uuid = uuid::Uuid::new_v4();
        let database_name = format!("test_db_{}", test_uuid);
        let collection_name = format!("test_collection_{}", test_uuid);

        let collection_id = clients
            .create_database_and_collection(&tenant_id, &database_name, &collection_name, true)
            .await
            .unwrap();

        tracing::info!(collection_id = %collection_id, "Created collection");

        let mut embeddings = vec![];
        let mut ids = vec![];

        for i in 0..TEST_COLLECTIONS_SIZE {
            let mut embedding = vec![0.0; 3];
            embedding[i % 3] = 1.0;
            embeddings.push(embedding);
            ids.push(format!("id{}", i));
        }

        // Get initial version count
        let initial_versions = clients
            .list_collection_versions(
                collection_id.clone(),
                tenant_id.clone(),
                Some(100),
                None,
                None,
                None,
            )
            .await
            .unwrap();
        let initial_version_count = initial_versions.versions.len();

        tracing::info!(
            initial_count = initial_version_count,
            "Initial version count"
        );

        // Add first batch of 11 records
        tracing::info!("Adding first batch of embeddings");
        clients
            .add_embeddings(
                &collection_id,
                embeddings[..11].to_vec(),
                ids[..11].to_vec(),
            )
            .await
            .unwrap();

        // Wait for new version after first batch
        wait_for_new_version(
            clients,
            collection_id.clone(),
            tenant_id.clone(),
            initial_version_count,
            10,
        )
        .await
        .unwrap();

        // Add second batch of 11 records
        tracing::info!("Adding second batch of embeddings");
        clients
            .add_embeddings(
                &collection_id,
                embeddings[11..22].to_vec(),
                ids[11..22].to_vec(),
            )
            .await
            .unwrap();
        // Wait for new version after first batch
        wait_for_new_version(
            clients,
            collection_id.clone(),
            tenant_id.clone(),
            initial_version_count + 1,
            10,
        )
        .await
        .unwrap();

        // After adding second batch and waiting for version, add a third batch
        tracing::info!("Adding third batch of embeddings (modified records)");
        clients
            .add_embeddings(
                &collection_id,
                embeddings[22..].to_vec(),
                ids[22..].to_vec(),
            )
            .await
            .unwrap();

        wait_for_new_version(
            clients,
            collection_id.clone(),
            tenant_id.clone(),
            initial_version_count + 2,
            10,
        )
        .await
        .unwrap();

        let collection_id = CollectionUuid::from_str(&collection_id).unwrap();

        (collection_id, database_name)
    }

    #[tokio::test]
    #[traced_test]
    async fn test_k8s_integration_tenant_mode_override() {
        // Setup
        let tenant_id_for_delete_mode = format!("tenant-delete-mode-{}", Uuid::new_v4());
        let tenant_id_for_dry_run_mode = format!("tenant-dry-run-mode-{}", Uuid::new_v4());

        let mut tenant_mode_overrides = HashMap::new();
        tenant_mode_overrides.insert(tenant_id_for_delete_mode.clone(), CleanupMode::DeleteV2);

        let config = GarbageCollectorConfig {
            service_name: "gc".to_string(),
            otel_endpoint: "none".to_string(),
            otel_filters: vec![OtelFilter {
                crate_name: "garbage_collector".to_string(),
                filter_level: OtelFilterLevel::Debug,
            }],
            version_cutoff_time: Duration::from_secs(1),
            collection_soft_delete_grace_period: Duration::from_secs(1),
            attached_function_soft_delete_grace_period: Duration::from_secs(1),
            max_collections_to_gc: 100,
            max_collections_to_fetch: None,
            min_versions_to_keep: 2,
            filter_min_versions_if_alive: None,
            gc_interval_mins: 10,
            disallow_collections: HashSet::new(),
            sysdb_config: GrpcSysDbConfig {
                host: "localhost".to_string(),
                port: 50051,
                connect_timeout_ms: 5000,
                request_timeout_ms: 10000,
                num_channels: 1,
            },
            dispatcher_config: DispatcherConfig::default(),
            storage_config: s3_config_for_localhost_with_bucket_name("chroma-storage").await,
            default_mode: CleanupMode::DryRunV2,
            tenant_mode_overrides: Some(tenant_mode_overrides),
            assignment_policy: chroma_config::assignment::config::AssignmentPolicyConfig::default(),
            my_member_id: "test-gc".to_string(),
            memberlist_provider: chroma_memberlist::config::MemberlistProviderConfig::default(),
            port: 50055,
            root_cache_config: Default::default(),
            jemalloc_pprof_server_port: None,
            enable_log_gc_for_tenant: Vec::new(),
            enable_log_gc_for_tenant_threshold: "tenant-threshold".to_string(),
            log: LogConfig::Grpc(GrpcLogConfig::default()),
            enable_dangerous_option_to_ignore_min_versions_for_wal3: false,
            max_concurrent_list_files_operations_per_collection: 10,
            heap_prune_buckets_to_read: 10,
            heap_prune_max_items: 10000,
            max_attached_functions_to_gc_per_run: 100,
        };
        let registry = Registry::new();

        // Create collections
        let mut clients = ChromaGrpcClients::new().await.unwrap();
        let collection_in_dry_run_mode_handle = tokio::spawn({
            let mut clients = clients.clone();
            let tenant_id = tenant_id_for_dry_run_mode.clone();
            async move { create_test_collection(tenant_id, &mut clients).await }
        });
        let collection_in_delete_mode_handle = tokio::spawn({
            let mut clients = clients.clone();
            let tenant_id = tenant_id_for_delete_mode.clone();
            async move { create_test_collection(tenant_id, &mut clients).await }
        });
        let (collection_in_dry_run_mode, _) = collection_in_dry_run_mode_handle.await.unwrap();
        let (collection_in_delete_mode, _) = collection_in_delete_mode_handle.await.unwrap();

        // Wait 1 second for cutoff time
        tokio::time::sleep(Duration::from_secs(1)).await;

        let system = System::new();

        // Run garbage collection
        let mut garbage_collector_component =
            GarbageCollector::try_from_config(&(config.clone(), system.clone()), &registry)
                .await
                .unwrap();

        let dispatcher = Dispatcher::try_from_config(&config.dispatcher_config, &registry)
            .await
            .unwrap();

        let dispatcher_handle = system.start_component(dispatcher);

        garbage_collector_component.set_dispatcher(dispatcher_handle);
        garbage_collector_component.set_system(system.clone());
        let mut garbage_collector_handle = system.start_component(garbage_collector_component);

        garbage_collector_handle
            .send(
                vec![Member {
                    member_id: "test-gc".to_string(),
                    member_ip: "0.0.0.0".to_string(),
                    member_node_name: "test-gc-node".to_string(),
                }],
                None,
            )
            .await
            .unwrap();

        garbage_collector_handle
            .request(
                GarbageCollectMessage { tenant: None },
                Some(Span::current()),
            )
            .await
            .unwrap();

        // Get versions for dry run mode
        let dry_run_mode_versions = clients
            .list_collection_versions(
                collection_in_dry_run_mode.0.to_string(),
                tenant_id_for_dry_run_mode,
                None,
                None,
                None,
                Some(true),
            )
            .await
            .unwrap();

        // Dry run should have 4 versions, one marked for deletion
        assert_eq!(
            dry_run_mode_versions.versions.len(),
            4,
            "Expected 4 versions in dry run mode, found {}",
            dry_run_mode_versions.versions.len()
        );
        assert!(
            dry_run_mode_versions
                .versions
                .iter()
                .any(|v| v.marked_for_deletion),
            "Expected at least one version to be marked for deletion in dry run mode"
        );

        let delete_mode_versions = clients
            .list_collection_versions(
                collection_in_delete_mode.0.to_string(),
                tenant_id_for_delete_mode,
                None,
                None,
                None,
                Some(true),
            )
            .await
            .unwrap();

        // There should be 2 versions left in delete mode, since the versions 0 and 1 should have been deleted.
        assert_eq!(
            delete_mode_versions.versions.len(),
            2,
            "Expected 3 versions in delete mode, found {}",
            delete_mode_versions.versions.len()
        );
        assert!(
            delete_mode_versions
                .versions
                .iter()
                .all(|v| !v.marked_for_deletion),
            "Expected no versions to be marked for deletion in delete mode"
        );
    }

    async fn run_garbage_collection(
        config: &GarbageCollectorConfig,
        registry: &Registry,
        tenant_id: String,
    ) -> GarbageCollectResult {
        let system = System::new();
        let mut garbage_collector_component =
            GarbageCollector::try_from_config(&(config.clone(), system.clone()), registry)
                .await
                .unwrap();

        let dispatcher = Dispatcher::try_from_config(&config.dispatcher_config, registry)
            .await
            .unwrap();

        let mut dispatcher_handle = system.start_component(dispatcher);

        garbage_collector_component.set_dispatcher(dispatcher_handle.clone());
        garbage_collector_component.set_system(system.clone());
        let mut garbage_collector_handle = system.start_component(garbage_collector_component);

        garbage_collector_handle
            .send(
                vec![Member {
                    member_id: "test-gc".to_string(),
                    member_ip: "0.0.0.0".to_string(),
                    member_node_name: "test-gc-node".to_string(),
                }],
                None,
            )
            .await
            .unwrap();

        let result = garbage_collector_handle
            .request(
                GarbageCollectMessage {
                    tenant: Some(tenant_id),
                },
                Some(Span::current()),
            )
            .await
            .unwrap();

        garbage_collector_handle.stop();
        garbage_collector_handle.join().await.unwrap();
        dispatcher_handle.stop();
        dispatcher_handle.join().await.unwrap();

        result
    }

    #[tokio::test]
    #[traced_test]
    async fn test_k8s_integration_gc_v2_and_database_hard_delete() {
        // Setup
        let tenant_id = format!("tenant-delete-mode-{}", Uuid::new_v4());

        let config = GarbageCollectorConfig {
            service_name: "gc".to_string(),
            otel_endpoint: "none".to_string(),
            version_cutoff_time: Duration::from_secs(1),
            collection_soft_delete_grace_period: Duration::from_secs(1),
            attached_function_soft_delete_grace_period: Duration::from_secs(1),
            max_collections_to_gc: 100,
            min_versions_to_keep: 2,
            filter_min_versions_if_alive: None,
            gc_interval_mins: 10,
            disallow_collections: HashSet::new(),
            sysdb_config: GrpcSysDbConfig {
                host: "localhost".to_string(),
                port: 50051,
                connect_timeout_ms: 5000,
                request_timeout_ms: 10000,
                num_channels: 1,
            },
            dispatcher_config: DispatcherConfig::default(),
            storage_config: s3_config_for_localhost_with_bucket_name("chroma-storage").await,
            default_mode: CleanupMode::DeleteV2,
            tenant_mode_overrides: None,
            assignment_policy: chroma_config::assignment::config::AssignmentPolicyConfig::default(),
            my_member_id: "test-gc".to_string(),
            memberlist_provider: chroma_memberlist::config::MemberlistProviderConfig::default(),
            port: 50055,
            root_cache_config: Default::default(),
            jemalloc_pprof_server_port: None,
            enable_log_gc_for_tenant: Vec::new(),
            enable_log_gc_for_tenant_threshold: "ffffffff-ffff-ffff-ffff-ffffffffffff".to_string(),
            log: LogConfig::Grpc(GrpcLogConfig::default()),
            max_concurrent_list_files_operations_per_collection: 10,
            ..Default::default()
        };
        let registry = Registry::new();

        // Create collections
        let mut clients = ChromaGrpcClients::new().await.unwrap();
        let mut sysdb = SysDb::Grpc(
            GrpcSysDb::try_from_config(&config.sysdb_config, &registry)
                .await
                .unwrap(),
        );

        let collection_handle = tokio::spawn({
            let mut clients = clients.clone();
            let tenant_id = tenant_id.clone();
            async move { create_test_collection(tenant_id, &mut clients).await }
        });
        let (collection_id, database_name) = collection_handle.await.unwrap();

        // Fork collection to give it a lineage file (only GC v2 can handle fork trees)
        {
            let source_collection = sysdb
                .get_collections(GetCollectionsOptions {
                    collection_id: Some(collection_id),
                    ..Default::default()
                })
                .await
                .unwrap();
            let source_collection = source_collection.first().unwrap();

            sysdb
                .fork_collection(
                    collection_id,
                    source_collection.total_records_post_compaction,
                    source_collection.total_records_post_compaction,
                    CollectionUuid::new(),
                    "test-fork".to_string(),
                )
                .await
                .unwrap();
        }

        // Wait 1 second for cutoff time
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Run garbage collection
        run_garbage_collection(&config, &registry, tenant_id.clone()).await;

        let versions = clients
            .list_collection_versions(
                collection_id.0.to_string(),
                tenant_id.clone(),
                None,
                None,
                None,
                Some(true),
            )
            .await
            .unwrap();

        // There should be 2 versions left, since the versions 0 and 1 should have been deleted.
        assert_eq!(
            versions.versions.len(),
            2,
            "Expected 2 versions in delete mode, found {:#?}",
            versions.versions
        );
        assert!(
            versions.versions.iter().all(|v| !v.marked_for_deletion),
            "Expected no versions to be marked for deletion in delete mode"
        );

        // Delete database
        sysdb
            .delete_database(database_name, tenant_id.clone())
            .await
            .unwrap();

        // Wait 1s for cutoff time
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Soft deleted collection should not be hard deleted if still inside grace period
        let config_with_large_collection_grace_period = GarbageCollectorConfig {
            collection_soft_delete_grace_period: Duration::from_secs(60 * 60 * 24), // 1 day
            ..config.clone()
        };

        let result = run_garbage_collection(
            &config_with_large_collection_grace_period,
            &registry,
            tenant_id.clone(),
        )
        .await;
        assert_eq!(
            result,
            GarbageCollectResult {
                num_completed_jobs: 1,
                num_failed_jobs: 0,
                num_hard_deleted_databases: 0, // The database should not have been hard deleted yet
            }
        );

        // Double check that the collection is still soft deleted
        let statuses = sysdb
            .batch_get_collection_soft_delete_status(vec![collection_id])
            .await
            .unwrap();
        assert_eq!(
            statuses.get(&collection_id),
            Some(&true),
            "Collection should still be soft deleted"
        );

        // If outside the grace period, the collection should be hard deleted
        let result = run_garbage_collection(&config, &registry, tenant_id.clone()).await;
        assert_eq!(
            result,
            GarbageCollectResult {
                num_completed_jobs: 1,
                num_failed_jobs: 0,
                num_hard_deleted_databases: 1, // The database should have been hard deleted
            }
        );
    }

    #[tokio::test]
    async fn test_k8s_integration_gc_prunes_heap_after_attached_function_deletion() {
        use chroma_storage::Storage;
        use chroma_sysdb::GrpcSysDb;
        use s3heap::{HeapReader, HeapWriter, Limits, Schedule, Triggerable};
        use s3heap_service::SysDbScheduler;

        let tenant_id = format!("test-tenant-{}", Uuid::new_v4());

        // Use actual log service shard naming for the heap
        let log_service_index = 0;
        let heap_prefix =
            s3heap::heap_path_from_hostname(&format!("rust-log-service-{}", log_service_index));

        let config = GarbageCollectorConfig {
            service_name: "gc".to_string(),
            otel_endpoint: "none".to_string(),
            otel_filters: vec![],
            version_cutoff_time: Duration::from_secs(1),
            collection_soft_delete_grace_period: Duration::from_secs(1),
            attached_function_soft_delete_grace_period: Duration::from_secs(1),
            max_collections_to_gc: 0, // Don't GC collections in this test, only test heap pruning
            max_collections_to_fetch: None,
            min_versions_to_keep: 2,
            filter_min_versions_if_alive: None,
            gc_interval_mins: 10,
            disallow_collections: HashSet::new(),
            sysdb_config: GrpcSysDbConfig {
                host: "localhost".to_string(),
                port: 50051,
                connect_timeout_ms: 5000,
                request_timeout_ms: 10000,
                num_channels: 1,
            },
            dispatcher_config: DispatcherConfig::default(),
            storage_config: s3_config_for_localhost_with_bucket_name("chroma-storage").await,
            default_mode: CleanupMode::DeleteV2,
            tenant_mode_overrides: None,
            assignment_policy: chroma_config::assignment::config::AssignmentPolicyConfig::default(),
            my_member_id: "test-gc-heap-pruning".to_string(),
            memberlist_provider: chroma_memberlist::config::MemberlistProviderConfig::default(),
            port: 50056,
            root_cache_config: Default::default(),
            jemalloc_pprof_server_port: None,
            enable_log_gc_for_tenant: Vec::new(),
            enable_log_gc_for_tenant_threshold: "tenant-threshold".to_string(),
            log: LogConfig::Grpc(GrpcLogConfig::default()),
            enable_dangerous_option_to_ignore_min_versions_for_wal3: false,
            max_concurrent_list_files_operations_per_collection: 10,
            heap_prune_buckets_to_read: 10, // Reduce for faster test
            heap_prune_max_items: 100,      // Reduce for faster test
            max_attached_functions_to_gc_per_run: 100,
        };

        let registry = Registry::new();

        // Initialize storage and sysdb
        let storage = Storage::try_from_config(&config.storage_config, &registry)
            .await
            .unwrap();
        let mut sysdb = GrpcSysDb::try_from_config(&config.sysdb_config, &registry)
            .await
            .unwrap();

        // Create a test collection
        let mut clients = ChromaGrpcClients::new().await.unwrap();
        let collection_name = format!("test-collection-{}", Uuid::new_v4());
        let database_name = format!("test-db-{}", Uuid::new_v4());
        let collection_id_str = clients
            .create_database_and_collection(&tenant_id, &database_name, &collection_name, false)
            .await
            .unwrap();
        let collection_id = CollectionUuid::from_str(&collection_id_str).unwrap();

        tracing::info!(%collection_id, "Created test collection");

        // Create an attached function using the record_counter operator that exists
        let attached_function_id = sysdb
            .create_attached_function(
                "test_function".to_string(),
                "record_counter".to_string(), // Use existing operator
                collection_id,
                format!("test_output_{}", collection_id),
                serde_json::json!({}),
                tenant_id.clone(),
                database_name.clone(),
                1,
            )
            .await
            .unwrap();

        tracing::info!(
            ?attached_function_id,
            "Created attached function with record_counter operator"
        );

        // Write some scheduled tasks to the heap (using the actual log service heap path)
        let scheduler: Arc<dyn s3heap::HeapScheduler> =
            Arc::new(SysDbScheduler::new(SysDb::Grpc(sysdb.clone())));

        let writer = HeapWriter::new(storage.clone(), heap_prefix.clone(), Arc::clone(&scheduler))
            .await
            .unwrap();

        // Schedule 3 tasks
        let now = chrono::Utc::now();
        let schedules = vec![
            Schedule {
                triggerable: Triggerable {
                    partitioning: collection_id.0.into(),
                    scheduling: attached_function_id.0.into(),
                },
                nonce: uuid::Uuid::new_v4(),
                next_scheduled: now + chrono::Duration::seconds(10),
            },
            Schedule {
                triggerable: Triggerable {
                    partitioning: collection_id.0.into(),
                    scheduling: attached_function_id.0.into(),
                },
                nonce: uuid::Uuid::new_v4(),
                next_scheduled: now + chrono::Duration::seconds(20),
            },
            Schedule {
                triggerable: Triggerable {
                    partitioning: collection_id.0.into(),
                    scheduling: attached_function_id.0.into(),
                },
                nonce: uuid::Uuid::new_v4(),
                next_scheduled: now + chrono::Duration::seconds(30),
            },
        ];

        writer.push(&schedules).await.unwrap();
        tracing::info!("Pushed {} schedules to heap", schedules.len());

        // Verify tasks are in the heap before GC
        let reader = HeapReader::new(storage.clone(), heap_prefix.clone(), Arc::clone(&scheduler))
            .await
            .unwrap();

        let items_before = reader.peek(|_, _| true, Limits::default()).await.unwrap();
        let items_before_count = items_before.len();
        tracing::info!("Items in heap before GC: {}", items_before_count);
        assert!(
            items_before_count > 0,
            "Should have at least 1 item in heap before GC"
        );

        // Count items for our specific attached function
        let our_items_before = items_before
            .iter()
            .filter(|(_, item)| item.trigger.scheduling == attached_function_id.0.into())
            .count();
        tracing::info!(
            "Items for our attached function before GC: {}",
            our_items_before
        );
        assert!(
            our_items_before > 0,
            "Should have items for our attached function"
        );

        // Soft delete the attached function
        sysdb
            .soft_delete_attached_function(
                attached_function_id,
                false, // don't delete output
            )
            .await
            .unwrap();
        tracing::info!("Soft deleted attached function");

        // Wait for grace period to expire
        tokio::time::sleep(Duration::from_secs(2)).await;
        tracing::info!("Grace period expired, starting GC");

        // Now run the garbage collector - it should both hard-delete the function AND prune the heap
        let system = System::new();
        let mut garbage_collector =
            GarbageCollector::try_from_config(&(config.clone(), system.clone()), &registry)
                .await
                .unwrap();

        // Don't set dispatcher - this test only cares about heap pruning
        // Without a dispatcher, truncate_dirty_log will return early
        garbage_collector.set_system(system.clone());

        let mut gc_handle = system.start_component(garbage_collector);

        // Send memberlist update (required for GC to start processing)
        gc_handle
            .send(
                vec![Member {
                    member_id: config.my_member_id.clone(),
                    member_ip: "0.0.0.0".to_string(),
                    member_node_name: format!("{}-node", config.my_member_id),
                }],
                None,
            )
            .await
            .unwrap();

        tracing::info!("Sending GC request...");

        let result = gc_handle
            .request(
                GarbageCollectMessage { tenant: None },
                Some(Span::current()),
            )
            .await
            .unwrap();

        tracing::info!(?result, "Garbage collection completed");

        // The GC should have:
        // 1. Hard deleted the attached function (grace period expired)
        // 2. Pruned the heap items associated with it

        // Verify heap items for our attached function were pruned - this is the key assertion
        let items_after = reader.peek(|_, _| true, Limits::default()).await.unwrap();
        let items_after_count = items_after.len();
        tracing::info!("Items in heap after GC: {}", items_after_count);

        // Count items for our specific attached function after GC
        let our_items_after = items_after
            .iter()
            .filter(|(_, item)| item.trigger.scheduling == attached_function_id.0.into())
            .count();
        tracing::info!(
            "Items for our attached function after GC: {}",
            our_items_after
        );

        assert_eq!(
            our_items_after, 0,
            "GC should have pruned all heap items for the deleted attached function (before: {}, after: {})",
            our_items_before, our_items_after
        );
    }
}
