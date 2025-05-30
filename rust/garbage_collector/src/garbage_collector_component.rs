use crate::types::CleanupMode;
use crate::{config::GarbageCollectorConfig, types::GarbageCollectorResponse};
use async_trait::async_trait;
use chroma_blockstore::RootManager;
use chroma_config::{
    assignment::assignment_policy::AssignmentPolicy, registry::Registry, Configurable,
};
use chroma_error::ChromaError;
use chroma_memberlist::memberlist_provider::Memberlist;
use chroma_storage::Storage;
use chroma_sysdb::{CollectionToGcInfo, SysDb, SysDbConfig};
use chroma_system::{
    Component, ComponentContext, ComponentHandle, Dispatcher, Handler, Orchestrator,
};
use chroma_types::CollectionUuid;
use chrono::{DateTime, Utc};
use futures::{stream::FuturesUnordered, StreamExt};
use opentelemetry::metrics::{Counter, Histogram};
use std::{
    collections::{HashMap, HashSet},
    fmt::{Debug, Formatter},
    str::FromStr,
    time::{Duration, SystemTime},
};
use thiserror::Error;
use tracing::{instrument, span, Instrument, Span};
use uuid::Uuid;

#[allow(dead_code)]
pub(crate) struct GarbageCollector {
    gc_interval_mins: u64,
    relative_cutoff_time: Duration,
    max_collections_to_gc: u32,
    disabled_collections: HashSet<CollectionUuid>,
    sysdb_client: SysDb,
    storage: Storage,
    dispatcher: Option<ComponentHandle<Dispatcher>>,
    system: Option<chroma_system::System>,
    assignment_policy: Box<dyn AssignmentPolicy>,
    memberlist: Memberlist,
    my_member_id: String,
    default_cleanup_mode: CleanupMode,
    tenant_mode_overrides: Option<HashMap<String, CleanupMode>>,
    root_manager: RootManager,
    total_jobs_metric: Counter<u64>,
    job_duration_ms_metric: Histogram<u64>,
    total_files_deleted_metric: Counter<u64>,
    total_versions_deleted_metric: Counter<u64>,
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
    OrchestratorError(#[from] crate::garbage_collector_orchestrator::GarbageCollectorError),
    #[error("Failed to run garbage collection orchestrator: {0}")]
    OrchestratorV2Error(#[from] crate::garbage_collector_orchestrator_v2::GarbageCollectorError),
}

#[allow(clippy::too_many_arguments)]
impl GarbageCollector {
    pub fn new(
        gc_interval_mins: u64,
        relative_cutoff_time: Duration,
        max_collections_to_gc: u32,
        disabled_collections: HashSet<CollectionUuid>,
        sysdb_client: SysDb,
        storage: Storage,
        default_cleanup_mode: CleanupMode,
        tenant_mode_overrides: Option<HashMap<String, CleanupMode>>,
        assignment_policy: Box<dyn AssignmentPolicy>,
        my_member_id: String,
        root_manager: RootManager,
    ) -> Self {
        let meter = opentelemetry::global::meter("chroma");

        Self {
            gc_interval_mins,
            relative_cutoff_time,
            max_collections_to_gc,
            disabled_collections,
            sysdb_client,
            storage,
            dispatcher: None,
            system: None,
            default_cleanup_mode,
            tenant_mode_overrides,
            assignment_policy,
            memberlist: Memberlist::default(),
            my_member_id,
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
        }
    }

    pub(crate) fn set_dispatcher(&mut self, dispatcher: ComponentHandle<Dispatcher>) {
        self.dispatcher = Some(dispatcher);
    }

    pub(crate) fn set_system(&mut self, system: chroma_system::System) {
        self.system = Some(system);
    }

    #[instrument]
    async fn garbage_collect_collection(
        &self,
        absolute_cutoff_time: DateTime<Utc>,
        collection: CollectionToGcInfo,
        cleanup_mode: CleanupMode,
    ) -> Result<GarbageCollectorResponse, GarbageCollectCollectionError> {
        let dispatcher = self
            .dispatcher
            .as_ref()
            .ok_or(GarbageCollectCollectionError::Uninitialized)?;
        let system = self
            .system
            .as_ref()
            .ok_or(GarbageCollectCollectionError::Uninitialized)?;

        if cleanup_mode.is_v2() {
            let orchestrator =
                crate::garbage_collector_orchestrator_v2::GarbageCollectorOrchestrator::new(
                    collection.id,
                    collection.version_file_path,
                    collection.lineage_file_path,
                    absolute_cutoff_time,
                    self.sysdb_client.clone(),
                    dispatcher.clone(),
                    system.clone(),
                    self.storage.clone(),
                    self.root_manager.clone(),
                    cleanup_mode,
                    2,
                );

            let started_at = SystemTime::now();
            let result = orchestrator.run(system.clone()).await?;
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

            return Ok(result);
        }

        let orchestrator = crate::garbage_collector_orchestrator::GarbageCollectorOrchestrator::new(
            collection.id,
            collection.version_file_path,
            absolute_cutoff_time,
            self.sysdb_client.clone(),
            dispatcher.clone(),
            self.storage.clone(),
            cleanup_mode,
        );

        let started_at = SystemTime::now();
        let result = orchestrator.run(system.clone()).await?;
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
            Duration::from_secs(self.gc_interval_mins * 60),
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
                if self.disabled_collections.contains(&collection.id) {
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
                    Ok(member) => member == self.my_member_id,
                    Err(err) => {
                        tracing::error!("Failed to assign collection {}: {}", collection.id, err);
                        false
                    }
                }
            })
            .collect()
    }
}

#[async_trait]
impl Handler<Memberlist> for GarbageCollector {
    type Result = ();

    async fn handle(&mut self, message: Memberlist, _ctx: &ComponentContext<GarbageCollector>) {
        self.memberlist = message;
    }
}

#[derive(Debug, PartialEq, Eq)]
#[allow(dead_code)]
struct GarbageCollectResult {
    num_completed_jobs: u32,
    num_failed_jobs: u32,
    num_skipped_jobs: u32,
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
        let absolute_cutoff_time =
            DateTime::<Utc>::from(SystemTime::now() - self.relative_cutoff_time);
        tracing::info!(
            "Using absolute cutoff time: {} (relative cutoff time: {:?})",
            absolute_cutoff_time,
            self.relative_cutoff_time
        );

        // Get all collections to gc and create gc orchestrator for each.
        tracing::info!("Getting collections to gc");
        let collections_to_gc = self
            .sysdb_client
            .get_collections_to_gc(
                Some(absolute_cutoff_time.into()),
                Some(self.max_collections_to_gc.into()),
                message.tenant.clone(),
            )
            .await
            .expect("Failed to get collections to gc");
        tracing::info!("Got {} total collections", collections_to_gc.len());
        let collections_to_gc = self.filter_collections(collections_to_gc);
        tracing::info!(
            "Filtered to {} collections to garbage collect",
            collections_to_gc.len()
        );

        let mut sysdb = self.sysdb_client.clone();

        let mut jobs = FuturesUnordered::new();

        let mut num_skipped_jobs = 0;
        for collection in collections_to_gc {
            if self.disabled_collections.contains(&collection.id) {
                tracing::warn!(
                    "Skipping garbage collection for disabled collection: {}",
                    collection.id
                );
                num_skipped_jobs += 1;
                continue;
            }

            let cleanup_mode = if let Some(tenant_mode_overrides) = &self.tenant_mode_overrides {
                tenant_mode_overrides
                    .get(&collection.tenant)
                    .cloned()
                    .unwrap_or(self.default_cleanup_mode)
            } else {
                self.default_cleanup_mode
            };

            if collection.lineage_file_path.is_some() && !cleanup_mode.is_v2() {
                tracing::debug!(
                    "Skipping garbage collection for root of fork tree because GC v1 cannot handle fork trees: {}",
                    collection.id
                );
                num_skipped_jobs += 1;
                continue;
            }

            tracing::info!(
                "Processing collection: {} (tenant: {}, version_file_path: {})",
                collection.id,
                collection.tenant,
                collection.version_file_path
            );

            let instrumented_span = span!(parent: None, tracing::Level::INFO, "Garbage collection job", collection_id = ?collection.id, tenant_id = %collection.tenant, cleanup_mode = ?cleanup_mode);
            instrumented_span.follows_from(Span::current());

            jobs.push(
                self.garbage_collect_collection(absolute_cutoff_time, collection, cleanup_mode)
                    .instrument(instrumented_span),
            );
        }
        tracing::info!("GC {} jobs", jobs.len());
        let mut num_completed_jobs = 0;
        let mut num_failed_jobs = 0;
        while let Some(job) = jobs.next().await {
            match job {
                Ok(result) => {
                    tracing::info!("Garbage collection completed. Deleted {} files over {} versions for collection {}.", result.num_files_deleted, result.num_versions_deleted, result.collection_id);
                    num_completed_jobs += 1;
                }
                Err(e) => {
                    tracing::info!("Garbage collection failed: {:?}", e);
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
            .finish_database_deletion(absolute_cutoff_time.into())
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

        // Schedule next run
        ctx.scheduler.schedule(
            GarbageCollectMessage {
                tenant: message.tenant.clone(),
            },
            Duration::from_secs(self.gc_interval_mins * 60),
            ctx,
            || Some(span!(parent: None, tracing::Level::INFO, "Scheduled garbage collection")),
        );

        return GarbageCollectResult {
            num_completed_jobs,
            num_failed_jobs,
            num_skipped_jobs,
            num_hard_deleted_databases: num_hard_deleted_databases as u32,
        };
    }
}

#[async_trait]
impl Configurable<GarbageCollectorConfig> for GarbageCollector {
    async fn try_from_config(
        config: &GarbageCollectorConfig,
        registry: &Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        let sysdb_config = SysDbConfig::Grpc(config.sysdb_config.clone());
        let sysdb_client = SysDb::try_from_config(&sysdb_config, registry).await?;
        let storage = Storage::try_from_config(&config.storage_config, registry).await?;

        let mut disabled_collections = HashSet::new();
        for collection_id_str in config.disallow_collections.iter() {
            let collection_uuid = match Uuid::from_str(collection_id_str) {
                Ok(uuid) => uuid,
                Err(e) => {
                    // TODO(Sanket): Return a proper error here.
                    panic!("Invalid collection id: {}", e);
                }
            };
            let collection_id = CollectionUuid(collection_uuid);
            disabled_collections.insert(collection_id);
        }

        let assignment_policy =
            Box::<dyn AssignmentPolicy>::try_from_config(&config.assignment_policy, registry)
                .await?;

        let root_manager_cache =
            chroma_cache::from_config_persistent(&config.root_cache_config).await?;
        let root_manager = RootManager::new(storage.clone(), root_manager_cache);

        Ok(GarbageCollector::new(
            config.gc_interval_mins as u64,
            config.relative_cutoff_time,
            config.max_collections_to_gc,
            disabled_collections,
            sysdb_client,
            storage,
            config.default_mode,
            config.tenant_mode_overrides.clone(),
            assignment_policy,
            config.my_member_id.clone(),
            root_manager,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::helper::ChromaGrpcClients;
    use chroma_memberlist::memberlist_provider::Member;
    use chroma_storage::config::{
        ObjectStoreBucketConfig, ObjectStoreConfig, ObjectStoreType, StorageConfig,
    };
    use chroma_sysdb::{GrpcSysDb, GrpcSysDbConfig};
    use chroma_system::{DispatcherConfig, System};
    use tracing_test::traced_test;

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
    async fn test_k8s_integration_ignores_forked_collections() {
        let tenant_id = format!("tenant-{}", Uuid::new_v4());
        let tenant_mode_overrides = HashMap::from([(tenant_id.clone(), CleanupMode::Delete)]);

        let config = GarbageCollectorConfig {
            service_name: "gc".to_string(),
            otel_endpoint: "none".to_string(),
            relative_cutoff_time: Duration::from_secs(1),
            max_collections_to_gc: 100,
            gc_interval_mins: 10,
            disallow_collections: vec![],
            sysdb_config: GrpcSysDbConfig {
                host: "localhost".to_string(),
                port: 50051,
                connect_timeout_ms: 5000,
                request_timeout_ms: 10000,
                num_channels: 1,
            },
            dispatcher_config: DispatcherConfig::default(),
            storage_config: StorageConfig::ObjectStore(ObjectStoreConfig {
                bucket: ObjectStoreBucketConfig {
                    name: "chroma-storage".to_string(),
                    r#type: ObjectStoreType::Minio,
                },
                upload_part_size_bytes: 1024 * 1024,   // 1MB
                download_part_size_bytes: 1024 * 1024, // 1MB
                max_concurrent_requests: 10,
            }),
            default_mode: CleanupMode::DryRun,
            tenant_mode_overrides: Some(tenant_mode_overrides),
            assignment_policy: chroma_config::assignment::config::AssignmentPolicyConfig::default(),
            my_member_id: "test-gc".to_string(),
            memberlist_provider: chroma_memberlist::config::MemberlistProviderConfig::default(),
            port: 50055,
            root_cache_config: Default::default(),
        };
        let registry = Registry::new();

        // Create collection
        let mut clients = ChromaGrpcClients::new().await.unwrap();

        let (collection_id, _) = create_test_collection(tenant_id.clone(), &mut clients).await;
        let mut sysdb = SysDb::Grpc(
            GrpcSysDb::try_from_config(&config.sysdb_config, &registry)
                .await
                .unwrap(),
        );
        let collections = sysdb
            .get_collections(Some(collection_id), None, None, None, None, 0)
            .await
            .unwrap();
        let collection = collections.first().unwrap();
        // Fork collection
        sysdb
            .fork_collection(
                collection_id,
                collection.log_position as u64,
                collection.log_position as u64,
                CollectionUuid::new(),
                "test-fork".to_string(),
            )
            .await
            .unwrap();

        // Wait 1 second for cutoff time
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Run garbage collection
        let mut garbage_collector_component = GarbageCollector::try_from_config(&config, &registry)
            .await
            .unwrap();

        let dispatcher = Dispatcher::try_from_config(&config.dispatcher_config, &registry)
            .await
            .unwrap();

        let system = System::new();
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

        let result = garbage_collector_handle
            .request(
                GarbageCollectMessage {
                    tenant: Some(tenant_id.clone()),
                },
                Some(Span::current()),
            )
            .await
            .unwrap();

        // Should have skipped
        assert_eq!(
            result,
            GarbageCollectResult {
                num_completed_jobs: 0,
                num_failed_jobs: 0,
                num_skipped_jobs: 1,
                num_hard_deleted_databases: 0,
            }
        );
    }

    #[tokio::test]
    #[traced_test]
    async fn test_k8s_integration_tenant_mode_override() {
        // Setup
        let tenant_id_for_delete_mode = format!("tenant-delete-mode-{}", Uuid::new_v4());
        let tenant_id_for_dry_run_mode = format!("tenant-dry-run-mode-{}", Uuid::new_v4());

        let mut tenant_mode_overrides = HashMap::new();
        tenant_mode_overrides.insert(tenant_id_for_delete_mode.clone(), CleanupMode::Delete);

        let config = GarbageCollectorConfig {
            service_name: "gc".to_string(),
            otel_endpoint: "none".to_string(),
            relative_cutoff_time: Duration::from_secs(1),
            max_collections_to_gc: 100,
            gc_interval_mins: 10,
            disallow_collections: vec![],
            sysdb_config: GrpcSysDbConfig {
                host: "localhost".to_string(),
                port: 50051,
                connect_timeout_ms: 5000,
                request_timeout_ms: 10000,
                num_channels: 1,
            },
            dispatcher_config: DispatcherConfig::default(),
            storage_config: StorageConfig::ObjectStore(ObjectStoreConfig {
                bucket: ObjectStoreBucketConfig {
                    name: "chroma-storage".to_string(),
                    r#type: ObjectStoreType::Minio,
                },
                upload_part_size_bytes: 1024 * 1024,   // 1MB
                download_part_size_bytes: 1024 * 1024, // 1MB
                max_concurrent_requests: 10,
            }),
            default_mode: CleanupMode::DryRun,
            tenant_mode_overrides: Some(tenant_mode_overrides),
            assignment_policy: chroma_config::assignment::config::AssignmentPolicyConfig::default(),
            my_member_id: "test-gc".to_string(),
            memberlist_provider: chroma_memberlist::config::MemberlistProviderConfig::default(),
            port: 50055,
            root_cache_config: Default::default(),
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

        // Run garbage collection
        let mut garbage_collector_component = GarbageCollector::try_from_config(&config, &registry)
            .await
            .unwrap();

        let dispatcher = Dispatcher::try_from_config(&config.dispatcher_config, &registry)
            .await
            .unwrap();

        let system = System::new();
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

        // There should be 3 versions left in delete mode, since the version 1 should have been deleted.
        assert_eq!(
            delete_mode_versions.versions.len(),
            3,
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

    #[tokio::test]
    #[traced_test]
    async fn test_k8s_integration_gc_v2_and_database_hard_delete() {
        // Setup
        let tenant_id_for_delete_mode = format!("tenant-delete-mode-{}", Uuid::new_v4());
        let tenant_id_for_dry_run_mode = format!("tenant-dry-run-mode-{}", Uuid::new_v4());

        let mut tenant_mode_overrides = HashMap::new();
        tenant_mode_overrides.insert(tenant_id_for_delete_mode.clone(), CleanupMode::DeleteV2);

        let config = GarbageCollectorConfig {
            service_name: "gc".to_string(),
            otel_endpoint: "none".to_string(),
            relative_cutoff_time: Duration::from_secs(1),
            max_collections_to_gc: 100,
            gc_interval_mins: 10,
            disallow_collections: vec![],
            sysdb_config: GrpcSysDbConfig {
                host: "localhost".to_string(),
                port: 50051,
                connect_timeout_ms: 5000,
                request_timeout_ms: 10000,
                num_channels: 1,
            },
            dispatcher_config: DispatcherConfig::default(),
            storage_config: StorageConfig::ObjectStore(ObjectStoreConfig {
                bucket: ObjectStoreBucketConfig {
                    name: "chroma-storage".to_string(),
                    r#type: ObjectStoreType::Minio,
                },
                upload_part_size_bytes: 1024 * 1024,   // 1MB
                download_part_size_bytes: 1024 * 1024, // 1MB
                max_concurrent_requests: 10,
            }),
            default_mode: CleanupMode::DryRun,
            tenant_mode_overrides: Some(tenant_mode_overrides),
            assignment_policy: chroma_config::assignment::config::AssignmentPolicyConfig::default(),
            my_member_id: "test-gc".to_string(),
            memberlist_provider: chroma_memberlist::config::MemberlistProviderConfig::default(),
            port: 50055,
            root_cache_config: Default::default(),
        };
        let registry = Registry::new();

        // Create collections
        let mut clients = ChromaGrpcClients::new().await.unwrap();
        let mut sysdb = SysDb::Grpc(
            GrpcSysDb::try_from_config(&config.sysdb_config, &registry)
                .await
                .unwrap(),
        );

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
        let (collection_in_delete_mode, database_name_in_delete_mode) =
            collection_in_delete_mode_handle.await.unwrap();

        // Fork collection in delete mode to give it a lineage file (only GC v2 can handle fork trees)
        {
            let source_collection = sysdb
                .get_collections(Some(collection_in_delete_mode), None, None, None, None, 0)
                .await
                .unwrap();
            let source_collection = source_collection.first().unwrap();

            sysdb
                .fork_collection(
                    collection_in_delete_mode,
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
        let mut garbage_collector_component = GarbageCollector::try_from_config(&config, &registry)
            .await
            .unwrap();

        let dispatcher = Dispatcher::try_from_config(&config.dispatcher_config, &registry)
            .await
            .unwrap();

        let system = System::new();
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
                tenant_id_for_delete_mode.clone(),
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
            "Expected 2 versions in delete mode, found {:#?}",
            delete_mode_versions.versions
        );
        assert!(
            delete_mode_versions
                .versions
                .iter()
                .all(|v| !v.marked_for_deletion),
            "Expected no versions to be marked for deletion in delete mode"
        );

        // Delete database
        sysdb
            .delete_database(
                database_name_in_delete_mode,
                tenant_id_for_delete_mode.clone(),
            )
            .await
            .unwrap();

        // Wait 1s for cutoff time
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Run garbage collection again
        let result = garbage_collector_handle
            .request(
                GarbageCollectMessage {
                    tenant: Some(tenant_id_for_delete_mode),
                },
                Some(Span::current()),
            )
            .await
            .unwrap();
        assert_eq!(
            result,
            GarbageCollectResult {
                num_completed_jobs: 1,
                num_failed_jobs: 0,
                num_skipped_jobs: 0,
                num_hard_deleted_databases: 1, // The database should have been hard deleted
            }
        );
    }
}
